package main

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/karthikeyan5/c3/internal/c3types"
	"github.com/karthikeyan5/c3/internal/ipc"
)

// TestHelloWireFields drives the hello → hello_ack handshake against an
// in-memory broker peer (a net.Pipe) and asserts the load-bearing fields the
// Claude Desktop adapter must send: CLI "desktop", CannotRenderChannels true
// (Desktop cannot render live channel frames — poll-only), and the fetch_queue
// capability (its only inbound path).
func TestHelloWireFields(t *testing.T) {
	t.Setenv("C3_DESKTOP_CWD", "/tmp/desktop-test-cwd")

	clientEnd, serverEnd := net.Pipe()
	defer clientEnd.Close()
	defer serverEnd.Close()

	a := newAdapter()
	a.conn = ipc.NewConn(clientEnd)
	peer := ipc.NewConn(serverEnd)

	errCh := make(chan error, 1)
	go func() { errCh <- a.hello() }()

	// Read the hello frame the adapter wrote.
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("peer.ReadFrame: %v", err)
	}
	var hello ipc.HelloMsg
	if err := json.Unmarshal(raw, &hello); err != nil {
		t.Fatalf("unmarshal hello: %v\nwire: %s", err, raw)
	}
	if hello.Op != ipc.OpHello {
		t.Errorf("hello.Op = %q; want %q", hello.Op, ipc.OpHello)
	}
	if hello.CLI != "desktop" {
		t.Errorf("hello.CLI = %q; want %q", hello.CLI, "desktop")
	}
	if !hello.CannotRenderChannels {
		t.Error("hello.CannotRenderChannels = false; want true (Desktop is poll-only)")
	}
	if hello.CWD != "/tmp/desktop-test-cwd" {
		t.Errorf("hello.CWD = %q; want C3_DESKTOP_CWD override", hello.CWD)
	}
	if !containsStr(hello.Capabilities, "fetch_queue") {
		t.Errorf("hello.Capabilities = %v; want to contain fetch_queue", hello.Capabilities)
	}

	// Reply with a hello_ack so a.hello() returns.
	if err := peer.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck}); err != nil {
		t.Fatalf("peer.WriteJSON ack: %v", err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("a.hello() returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a.hello() did not return after hello_ack")
	}
	if a.helloAck.Op != ipc.OpHelloAck {
		t.Errorf("stored helloAck.Op = %q; want %q", a.helloAck.Op, ipc.OpHelloAck)
	}
}

// TestServerInfoAndTools exercises the same buildMCPServer path the live adapter
// uses and asserts, over real in-memory MCP transports: (a) serverInfo.name is
// "c3" (MUST equal the mcpServers.<key> in the Desktop config), (b) the server
// declares the Logging capability and NOT the Claude Code experimental
// claude/channel map (Desktop can't render channel frames), (c) instructions are
// non-empty, and (d) the exact adapter tool set is registered — with `ask`,
// `send_typing`, and any permission tool deliberately ABSENT.
func TestServerInfoAndTools(t *testing.T) {
	if adapterName != "c3" {
		t.Fatalf("adapterName must be %q to match the Desktop config mcpServers key; got %q", "c3", adapterName)
	}

	a := newAdapter()
	// Seed hello-ack caps so buildInstructions folds capability guidance in and
	// the reply media schema renders against a realistic manifest.
	a.helloAck.Capabilities = &c3types.Capabilities{
		Channel:         "telegram",
		RichText:        true,
		MaxMessageRunes: 4096,
		MediaKinds:      []c3types.MediaKind{c3types.MediaPhoto, c3types.MediaFile},
		CompressedPhoto: true,
		OriginalFile:    true,
		MaxSendBytes:    50 * 1024 * 1024,
		Polls:           true,
		Typing:          true,
	}
	srv := a.buildMCPServer()
	if srv == nil {
		t.Fatal("buildMCPServer returned nil")
	}

	clientT, serverT := mcp.NewInMemoryTransports()
	a.transport = newLogNotifyTransport(serverT)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = srv.Run(ctx, a.transport) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.0"}, nil)
	sess, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	defer sess.Close()

	params := sess.InitializeResult()
	if params == nil {
		t.Fatal("InitializeResult is nil")
	}
	if params.ServerInfo == nil || params.ServerInfo.Name != "c3" {
		var got string
		if params.ServerInfo != nil {
			got = params.ServerInfo.Name
		}
		t.Fatalf("serverInfo.name = %q; want %q", got, "c3")
	}
	if params.ServerInfo.Version != adapterVersion {
		t.Errorf("serverInfo.version = %q; want %q", params.ServerInfo.Version, adapterVersion)
	}
	if params.Capabilities == nil {
		t.Fatal("capabilities missing")
	}
	if params.Capabilities.Logging == nil {
		t.Error("capabilities.logging missing; Desktop adapter must declare Logging (agy parity)")
	}
	// Desktop must NOT advertise the Claude Code channel experimental capability.
	if params.Capabilities.Experimental != nil {
		if _, ok := params.Capabilities.Experimental["claude/channel"]; ok {
			t.Error("capabilities.experimental must NOT declare claude/channel (Desktop cannot render channels)")
		}
	}
	if params.Instructions == "" {
		t.Fatal("instructions empty in initialize response")
	}
	if !strings.Contains(params.Instructions, "POLL-ONLY") {
		t.Errorf("instructions must state Desktop is poll-only; got:\n%s", params.Instructions)
	}

	listResult, err := sess.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	got := map[string]bool{}
	for _, tool := range listResult.Tools {
		got[tool.Name] = true
	}
	wantTools := []string{
		"attach", "detach", "topics", "fetch_queue", "observe", "retranscribe",
		"reply", "react", "edit_message", "poll", "stop_poll", "download_attachment",
		"open_inbox",
	}
	for _, name := range wantTools {
		if !got[name] {
			t.Errorf("tools/list missing %q (got %v)", name, keys(got))
		}
	}
	// Exactly the expected set — no extras (guards against an accidental `ask`
	// or permission tool sneaking in).
	if len(got) != len(wantTools) {
		t.Errorf("tools/list has %d tools; want exactly %d (%v)", len(got), len(wantTools), keys(got))
	}
	for _, banned := range []string{"ask", "send_typing", "request_permission"} {
		if got[banned] {
			t.Errorf("tool %q must NOT be registered on the Desktop adapter", banned)
		}
	}

	// --- MCP Apps ("C3 Inbox") contract -------------------------------------
	// The adapter must advertise the io.modelcontextprotocol/ui extension, link
	// open_inbox to its ui:// resource via _meta.ui.resourceUri, and serve that
	// resource as text/html;profile=mcp-app.
	if params.Capabilities.Extensions == nil {
		t.Error("capabilities.extensions missing; MCP Apps hosts gate on it")
	} else if ext, ok := params.Capabilities.Extensions[uiExtensionID].(map[string]any); !ok {
		t.Errorf("capabilities.extensions[%q] missing or not an object; got %v", uiExtensionID, params.Capabilities.Extensions)
	} else if mimes, _ := ext["mimeTypes"].([]any); len(mimes) == 0 || mimes[0] != uiResourceMIME {
		t.Errorf("extension mimeTypes = %v; want [%q]", ext["mimeTypes"], uiResourceMIME)
	}

	var openInbox *mcp.Tool
	for _, tool := range listResult.Tools {
		if tool.Name == "open_inbox" {
			openInbox = tool
		}
	}
	if openInbox == nil {
		t.Fatal("open_inbox tool not found in tools/list")
	}
	if uiMeta, _ := openInbox.Meta["ui"].(map[string]any); uiMeta == nil || uiMeta["resourceUri"] != uiInboxURI {
		t.Errorf("open_inbox _meta.ui.resourceUri = %v; want %q", openInbox.Meta["ui"], uiInboxURI)
	}
	if openInbox.Meta["ui/resourceUri"] != uiInboxURI {
		t.Errorf("open_inbox deprecated _meta[ui/resourceUri] = %v; want %q (host back-compat)", openInbox.Meta["ui/resourceUri"], uiInboxURI)
	}

	// attach, topics, observe, and reply are marked app-callable (visibility
	// ["model","app"]) so the C3 Inbox panel can call them through the host bridge
	// (apps.mdx:399-401). observe is the panel's read-only Watch peek; reply is the
	// owner-only composer's Send.
	for _, name := range []string{"attach", "topics", "observe", "reply"} {
		var tool *mcp.Tool
		for _, tl := range listResult.Tools {
			if tl.Name == name {
				tool = tl
			}
		}
		if tool == nil {
			t.Fatalf("%s tool not found in tools/list", name)
		}
		vis := uiVisibility(tool)
		if !containsStr(vis, "model") || !containsStr(vis, "app") {
			t.Errorf("%s _meta.ui.visibility = %v; want to contain \"model\" and \"app\"", name, vis)
		}
	}

	rr, err := sess.ReadResource(ctx, &mcp.ReadResourceParams{URI: uiInboxURI})
	if err != nil {
		t.Fatalf("ReadResource(%s): %v", uiInboxURI, err)
	}
	if len(rr.Contents) == 0 {
		t.Fatalf("ReadResource(%s) returned no contents", uiInboxURI)
	}
	rc := rr.Contents[0]
	if rc.MIMEType != uiResourceMIME {
		t.Errorf("inbox resource mimeType = %q; want %q", rc.MIMEType, uiResourceMIME)
	}
	if !strings.Contains(rc.Text, "C3 Inbox connected") {
		t.Error("inbox HTML missing the '✅ C3 Inbox connected' banner")
	}
	if !strings.Contains(rc.Text, "ui/initialize") || !strings.Contains(rc.Text, "fetch_queue") {
		t.Error("inbox HTML missing the ui/initialize handshake or the fetch_queue call")
	}
	// Interactive inbox (2026-07-17 Watch/Take-over rework): the panel splits
	// read-only WATCH (an `observe` peek that never claims — parsed via the
	// ⟦c3 owner=…⟧ sentinel) from explicit TAKE-OVER (the only action that claims/
	// steals, via `attach`). Owner-only Hand to Claude + Auto + the ui/message
	// turn-start call remain; the take-over still carries args.steal/args.create
	// selected by the observed owner state. All must be present in the served HTML.
	for _, want := range []string{
		"ui/message", "Hand to Claude", "Auto-forward",
		"placeholder=\"topic to watch\"", "name: \"observe\"", "name: \"attach\"",
		"parseObserve", "⟦c3", "watchTopic", "doTakeOver",
		"Watch", "Take over here", "Create & take over",
		"args.steal = true", "args.create = true", "args.topic_id",
		"Pop out", "ui/request-display-mode", "press <b>Enter</b>",
	} {
		if !strings.Contains(rc.Text, want) {
			t.Errorf("inbox HTML missing %q (Watch/Take-over inbox)", want)
		}
	}
	// ui/message params contract (SEP-1865 McpUiMessageRequest): content MUST be
	// an ARRAY of content blocks. The single-object shape was rejected by Claude
	// Desktop as invalid params (~10ms), which surfaced as "Hand to Claude / Auto
	// does nothing" (2026-07-17). Guard both directions.
	if !strings.Contains(rc.Text, "content: [{ type: \"text\"") {
		t.Error("inbox HTML ui/message content is not a ContentBlock ARRAY (spec.types.ts McpUiMessageRequest)")
	}
	if strings.Contains(rc.Text, "content: { type: \"text\"") {
		t.Error("inbox HTML ui/message uses the single-object content shape the host rejects")
	}
	// McpUiMessageResult carries isError — a host can accept the RPC yet fail
	// delivery (e.g. the composer already holds an unsent draft). The panel MUST
	// check it before draining, or a failed hand would consume queued messages
	// (no-loss contract violation). The guard routes to handFailed(); match that.
	if !strings.Contains(rc.Text, "result && result.isError") || !strings.Contains(rc.Text, "handFailed(isAuto") {
		t.Error("inbox HTML hand flow does not guard on ui/message result.isError before draining")
	}
	// A failed hand must NOT disarm Auto on the first failure (that was the
	// 2026-07-17 bug: a composer-busy reject killed Auto). Auto only disarms after
	// a persistent streak (the circuit breaker), and a failed hand backs off.
	if !strings.Contains(rc.Text, "autoFailStreak") || !strings.Contains(rc.Text, "AUTO_FAIL_LIMIT") || !strings.Contains(rc.Text, "autoSkipCycles") {
		t.Error("inbox HTML hand flow missing the keep-Auto-armed backoff / circuit-breaker (composer-busy must not disarm Auto)")
	}
}

// TestInboxComposerScrollAndRename asserts the 2026-07-24 panel rework directly
// against the inboxHTML const (task #59):
//
//	(A) a reply composer — textarea + Send — that calls the `reply` tool, is
//	    owner-gated the same way Hand/Auto-forward are, and sends on Ctrl/Cmd+Enter.
//	(B) a bounded, self-scrolling message list plus a capped reportSize and a
//	    stick-to-bottom chat tail, so the iframe can't grow unbounded and the
//	    newest message can always come back into view.
//	(C) the auto-forward rename — the visible "Auto" control/help copy became
//	    "Auto-forward"; "Hand to Claude" is untouched.
func TestInboxComposerScrollAndRename(t *testing.T) {
	// (A) reply composer + Send wiring, owner-gated, direct reply tool call.
	for _, want := range []string{
		"id=\"composer\"", "id=\"replytext\"", "id=\"send\"", ">Send<",
		"function sendReply", "name: \"reply\"",
		"show(elComposer, owned)",   // owner-only, same gating as Hand/Auto-forward
		"e.ctrlKey || e.metaKey",    // Ctrl/Cmd+Enter sends
		"args.reply_to = lastMsgId", // threads reply_to when available
		"function newestMsgId",      // newest observed message id extraction
	} {
		if !strings.Contains(inboxHTML, want) {
			t.Errorf("inboxHTML missing reply-composer piece %q", want)
		}
	}

	// (B) bounded scroll container + capped reportSize + chat-tail.
	for _, want := range []string{
		"overflow-y: auto", // #list is a scroll container
		"max-height: 46vh", // and bounded
		"MAX_PANEL_H",      // reportSize caps the reported height
		"if (h > MAX_PANEL_H) { h = MAX_PANEL_H; }", // ...and enforces it
		"elList.scrollTop = elList.scrollHeight",    // auto-tail to newest
		"stickToBottom",                             // ...only when the user is at the bottom
	} {
		if !strings.Contains(inboxHTML, want) {
			t.Errorf("inboxHTML missing scroll-fix piece %q", want)
		}
	}

	// (C) Auto -> Auto-forward rename; "Hand to Claude" unchanged.
	if !strings.Contains(inboxHTML, "> Auto-forward</label>") {
		t.Error("inboxHTML auto-mode label was not renamed to \"Auto-forward\"")
	}
	if strings.Contains(inboxHTML, "> Auto</label>") {
		t.Error("inboxHTML still carries the bare \"Auto\" label (should be \"Auto-forward\")")
	}
	if !strings.Contains(inboxHTML, "Hand to Claude") {
		t.Error("inboxHTML \"Hand to Claude\" button must stay exactly as-is")
	}
}

// TestSessionID covers the C3_DESKTOP_SESSION contract: an explicit (trimmed)
// value is used verbatim, an unset/blank value falls back to a per-process id.
func TestSessionID(t *testing.T) {
	t.Setenv("C3_DESKTOP_SESSION", "my-stable-id")
	if got := sessionID(); got != "my-stable-id" {
		t.Errorf("sessionID() = %q; want %q", got, "my-stable-id")
	}

	t.Setenv("C3_DESKTOP_SESSION", "  spaced  ")
	if got := sessionID(); got != "spaced" {
		t.Errorf("sessionID() = %q; want trimmed %q", got, "spaced")
	}

	t.Setenv("C3_DESKTOP_SESSION", "")
	if got := sessionID(); !strings.HasPrefix(got, "desktop-") {
		t.Errorf("sessionID() = %q; want per-process desktop- prefix when unset", got)
	}
}

// TestDesktopCWD covers the C3_DESKTOP_CWD override used for testing / power
// users; unset falls back to os.Getwd (non-empty in the test runner).
func TestDesktopCWD(t *testing.T) {
	t.Setenv("C3_DESKTOP_CWD", "/custom/desktop/dir")
	if got := desktopCWD(); got != "/custom/desktop/dir" {
		t.Errorf("desktopCWD() = %q; want the C3_DESKTOP_CWD override", got)
	}
	t.Setenv("C3_DESKTOP_CWD", "")
	if got := desktopCWD(); got == "" {
		t.Error("desktopCWD() empty with no override; want os.Getwd fallback")
	}
}

func uiVisibility(tool *mcp.Tool) []string {
	if tool == nil || tool.Meta == nil {
		return nil
	}
	ui, _ := tool.Meta["ui"].(map[string]any)
	if ui == nil {
		return nil
	}
	raw, _ := ui["visibility"].([]any)
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestRenderQueuedInbound_CleanShape locks the human-facing render (2026-07-17,
// Karthi): the message TEXT stands alone at the top, then a blank line, then a
// compact "↳ from … · reply to … · msg N" trailer — NOT the old one-line
// `from=@u message_id=N text=%q` clutter. Attachment file_id/mime stay present
// (load-bearing for download_attachment) but on their own line under the trailer.
func TestRenderQueuedInbound_CleanShape(t *testing.T) {
	in := c3types.Inbound{
		MessageID: 5738,
		Sender:    c3types.Sender{Username: "skarthi"},
		Text:      "hey, check the queue",
		ReplyTo:   &c3types.ReplyContext{MessageID: 900, User: c3types.Sender{Username: "bot"}},
	}
	got := renderQueuedInbound(&in)

	// Text must be alone at the very top, followed by a blank line.
	if !strings.HasPrefix(got, "hey, check the queue\n\n") {
		t.Errorf("text is not alone-at-top with a blank line after; got:\n%s", got)
	}
	// Metadata rides in a compact trailer, not inline with the text.
	for _, want := range []string{"↳ ", "from @skarthi", "reply to @bot", "msg 5738"} {
		if !strings.Contains(got, want) {
			t.Errorf("render missing trailer part %q; got:\n%s", want, got)
		}
	}
	// The old noisy key=value shapes must be gone.
	for _, gone := range []string{"from=@", "message_id=", "text=\"", "reply_to="} {
		if strings.Contains(got, gone) {
			t.Errorf("render still contains old noisy token %q; got:\n%s", gone, got)
		}
	}

	// An attachment keeps its full metadata (file_id load-bearing) on its own line.
	in2 := c3types.Inbound{
		MessageID:   42,
		Sender:      c3types.Sender{Username: "skarthi"},
		Attachments: []c3types.Attachment{{Kind: "photo", FileID: "ABC123", MIME: "image/jpeg", Size: 88352}},
	}
	got2 := renderQueuedInbound(&in2)
	if !strings.Contains(got2, "file_id=\"ABC123\"") {
		t.Errorf("attachment render dropped the load-bearing file_id; got:\n%s", got2)
	}
	if !strings.Contains(got2, "from @skarthi") {
		t.Errorf("attachment-only render missing sender trailer; got:\n%s", got2)
	}
}
