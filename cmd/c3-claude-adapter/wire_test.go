package main

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// note: io.NopCloserReader/Writer wrappers are defined in testhelpers_test.go

// TestServerInfoName guards the lesson from 2026-05-09: serverInfo.name MUST
// match the .mcp.json key ("c3") so Claude Code's channel dispatch can route
// notifications/claude/channel frames from this server. Both reference
// implementations (~/.claude/plugins/.../fakechat/server.ts:60 and
// .../telegram/0.0.6/server.ts:371) follow this convention.
//
// Post-SDK-migration the assertion is structural — we exercise the same
// code path that constructs the live MCP server (buildMCPServer) and
// confirm Implementation.Name + experimental capability declaration.
func TestServerInfoName(t *testing.T) {
	if adapterName != "c3" {
		t.Fatalf("adapterName must be %q to match .mcp.json key; got %q", "c3", adapterName)
	}

	// buildMCPServer wires Implementation + Capabilities; if either drifts
	// from what Claude Code expects, channel dispatch silently breaks.
	// Construct the server and inspect what the SDK will emit on initialize
	// by exercising in-memory transports end-to-end.
	a := newAdapter()
	// Seed the hello-ack caps so buildInstructions folds capability.GuidanceFor
	// into the MCP `instructions` (P4). Mirrors the Telegram v1 manifest's
	// load-bearing values so the positive guidance branches render.
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
	a.notifyTx = newNotifyTransport(serverT)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = srv.Run(ctx, a.notifyTx) }()

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
	if params.Capabilities == nil || params.Capabilities.Experimental == nil {
		t.Fatal("capabilities.experimental missing")
	}
	if _, ok := params.Capabilities.Experimental["claude/channel"]; !ok {
		t.Fatal("capabilities.experimental missing claude/channel")
	}
	if params.Instructions == "" {
		t.Fatal("instructions empty in initialize response")
	}
	// P4: the capability guidance (capability.GuidanceFor, folded in via
	// mode.Combined(caps)) must be present in the MCP instructions so the
	// agent is capability-aware. Assert distinctive phrases — the header
	// (always present) AND a positive cap line (proves the seeded caps
	// actually flowed through, not just a zero-value default).
	for _, want := range []string{
		"CHANNEL CAPABILITIES",
		"Typing: shown automatically",
	} {
		if !strings.Contains(params.Instructions, want) {
			t.Errorf("instructions missing capability-guidance phrase %q:\n%s", want, params.Instructions)
		}
	}

	// Verify tools/list returns the adapter tools (attach, detach, topics,
	// reply, react, edit_message, poll, download_attachment) — Claude Code
	// displays tool presence per-server and a regression here turns a working
	// session into "tool not found" for every adapter operation. `send_typing`
	// is deliberately ABSENT (P5): the typing indicator is relayed
	// programmatically by the broker, not via an LLM tool.
	listResult, err := sess.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	wantTools := []string{
		"attach", "detach", "topics", "reply", "react",
		"edit_message", "poll", "ask", "download_attachment",
	}
	got := map[string]bool{}
	gotDesc := map[string]string{}
	for _, tool := range listResult.Tools {
		got[tool.Name] = true
		gotDesc[tool.Name] = tool.Description
	}
	for _, name := range wantTools {
		if !got[name] {
			t.Errorf("tools/list missing %q (got %v)", name, got)
		}
	}
	// The reply tool Description is the compose-time surface; it must carry the
	// format-for-readability nudge (formatting-policy 2026-06-20).
	if d := gotDesc["reply"]; !strings.Contains(d, "whenever it makes the reply easier to read") {
		t.Errorf("reply Description missing the formatting nudge; got %q", d)
	}
	// send_typing must NOT be agent-facing (P5): the broker relays typing
	// programmatically. A regression that re-registers it would let an LLM
	// pulse typing, defeating the deterministic relay.
	if got["send_typing"] {
		t.Errorf("send_typing must NOT be registered as an agent tool (P5: broker-relayed); got %v", got)
	}
}

// TestHandleInboundEndToEnd drives the full path inside the adapter:
// receive a serialized OpInbound frame from the (mocked) broker, run
// handleInbound, and verify the bytes written to the SDK's stdout match
// the channel-notification shape Claude Code expects.
func TestHandleInboundEndToEnd(t *testing.T) {
	tid := int64(914)
	in := c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -1001234567890,
		MessageID: 933,
		TopicID:   &tid,
		Text:      "hi inbound",
		Sender: c3types.Sender{
			UserID:   12345678,
			Username: "alice",
		},
		Timestamp: time.Date(2026, 5, 9, 9, 17, 55, 0, time.UTC),
	}
	raw, err := json.Marshal(ipc.InboundMsg{Op: ipc.OpInbound, Inbound: in})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Capture the wire bytes by wrapping an IOTransport with a buffer
	// writer. Connection.Write goes straight to this buffer.
	var buf safeBuffer
	a := newAdapter()
	a.notifyTx = newNotifyTransport(&mcp.IOTransport{
		Reader: nopCloseReader{strings.NewReader("")},
		Writer: nopCloseWriter{&buf},
	})
	// Establish the connection (drives Connect on the wrapped transport
	// and captures the live Connection inside notifyTx).
	if _, err := a.notifyTx.Connect(context.Background()); err != nil {
		t.Fatalf("notifyTx.Connect: %v", err)
	}

	a.handleInbound(context.Background(), raw)

	out := buf.Bytes()
	t.Logf("WIRE: %s", string(out))

	if !bytes.HasSuffix(out, []byte{'\n'}) {
		t.Fatal("frame must be newline-terminated")
	}
	var msg map[string]any
	if err := json.Unmarshal(bytes.TrimRight(out, "\n"), &msg); err != nil {
		t.Fatalf("unmarshal: %v\nwire: %s", err, out)
	}
	if msg["method"] != "notifications/claude/channel" {
		t.Errorf("method = %v; want notifications/claude/channel", msg["method"])
	}
	params := msg["params"].(map[string]any)
	if _, isString := params["content"].(string); !isString {
		t.Errorf("params.content = %T; want string", params["content"])
	}
	meta := params["meta"].(map[string]any)
	if _, isString := meta["chat_id"].(string); !isString {
		t.Errorf("meta.chat_id = %T; want string per docs", meta["chat_id"])
	}
}

// TestChannelFrameWireBytes exercises buildClaudeChannelFrame +
// notifyTransport.Notify the same way the live adapter does, captures the
// bytes written to "stdout", and asserts the wire format against the
// official Telegram plugin's frame shape.
//
// Reference frame (golden) — derived from
// ~/.claude/plugins/.../telegram/0.0.6/server.ts at line 978-1000:
//
//	{"jsonrpc":"2.0","method":"notifications/claude/channel","params":{
//	  "content":"hi inbound",
//	  "meta":{
//	    "chat_id": "-1001234567890",
//	    "message_thread_id": "914",
//	    "message_id": "933",
//	    "user": "alice",
//	    "user_id": "12345678",
//	    "ts": "2026-05-09T09:17:55.000Z"
//	  }
//	}}
//
// Map iteration order in Go is randomised so we compare by parsed struct, not
// raw bytes. But we DO assert: (a) `content` is a string not an array, (b)
// meta values are strings (per channels-reference.md), (c) no spurious fields,
// (d) one trailing newline (line-framed).
func TestChannelFrameWireBytes(t *testing.T) {
	tid := int64(914)
	in := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -1001234567890,
		MessageID: 933,
		TopicID:   &tid,
		Text:      "hi inbound",
		Sender: c3types.Sender{
			UserID:   12345678,
			Username: "alice",
		},
		Timestamp: time.Date(2026, 5, 9, 9, 17, 55, 0, time.UTC),
	}

	var buf safeBuffer
	tx := newNotifyTransport(&mcp.IOTransport{
		Reader: nopCloseReader{strings.NewReader("")},
		Writer: nopCloseWriter{&buf},
	})
	if _, err := tx.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	frame := buildClaudeChannelFrame(in)
	if err := tx.Notify(context.Background(), "notifications/claude/channel", frame); err != nil {
		t.Fatalf("Notify: %v", err)
	}

	raw := buf.Bytes()
	t.Logf("WIRE BYTES (%d): %s", len(raw), string(raw))

	// Line framing: exactly one trailing \n, no embedded.
	if !bytes.HasSuffix(raw, []byte{'\n'}) {
		t.Fatalf("frame must end with \\n, got: %q", raw[max(0, len(raw)-10):])
	}
	if bytes.Count(raw, []byte{'\n'}) != 1 {
		t.Fatalf("frame must contain exactly one \\n; found %d", bytes.Count(raw, []byte{'\n'}))
	}

	var msg struct {
		JSONRPC string         `json:"jsonrpc"`
		Method  string         `json:"method"`
		Params  map[string]any `json:"params"`
	}
	if err := json.Unmarshal(bytes.TrimRight(raw, "\n"), &msg); err != nil {
		t.Fatalf("unmarshal frame: %v\n%s", err, raw)
	}

	if msg.JSONRPC != "2.0" {
		t.Errorf("jsonrpc: want 2.0, got %q", msg.JSONRPC)
	}
	if msg.Method != "notifications/claude/channel" {
		t.Errorf("method: want notifications/claude/channel, got %q", msg.Method)
	}

	content, ok := msg.Params["content"].(string)
	if !ok {
		t.Fatalf("params.content: want string, got %T (value: %v)",
			msg.Params["content"], msg.Params["content"])
	}
	if content != "hi inbound" {
		t.Errorf("params.content: want %q, got %q", "hi inbound", content)
	}

	meta, ok := msg.Params["meta"].(map[string]any)
	if !ok {
		t.Fatalf("params.meta: want object, got %T", msg.Params["meta"])
	}

	chatIDStr, ok := meta["chat_id"].(string)
	if !ok {
		t.Fatalf("meta.chat_id: want string, got %T (value: %v)",
			meta["chat_id"], meta["chat_id"])
	}
	if chatIDStr != "-1001234567890" {
		t.Errorf("meta.chat_id: want %q, got %q", "-1001234567890", chatIDStr)
	}

	for _, k := range []string{"chat_id", "message_thread_id", "message_id", "user", "user_id", "ts"} {
		v, ok := meta[k]
		if !ok {
			t.Errorf("meta.%s: missing", k)
			continue
		}
		if _, ok := v.(string); !ok {
			t.Errorf("meta.%s: want string, got %T (value: %v)", k, v, v)
		}
	}

	if meta["message_thread_id"] != "914" {
		t.Errorf("meta.message_thread_id: want %q, got %v", "914", meta["message_thread_id"])
	}
	if meta["message_id"] != "933" {
		t.Errorf("meta.message_id: want %q, got %v", "933", meta["message_id"])
	}
	if meta["user"] != "alice" {
		t.Errorf("meta.user: want %q, got %v", "alice", meta["user"])
	}
	if meta["user_id"] != "12345678" {
		t.Errorf("meta.user_id: want %q, got %v", "12345678", meta["user_id"])
	}

	allowed := map[string]bool{
		"chat_id":             true,
		"message_thread_id":   true,
		"message_id":          true,
		"user":                true,
		"user_id":             true,
		"ts":                  true,
		"reply_to_message_id": true,
		"reply_to_user":       true,
		"reply_to_text":       true,
		"image_path":          true,
		"attachment_kind":     true,
		"attachment_file_id":  true,
		"attachment_size":     true,
		"attachment_mime":     true,
		"attachment_name":     true,
	}
	for k := range meta {
		if !allowed[k] {
			t.Errorf("meta.%s: SPURIOUS field — official plugin doesn't send it", k)
		}
	}
}

// TestChannelFrame_PollResultEvent asserts a poll_result event renders into a
// human-readable content string and string-only meta (the load-bearing contract:
// Claude Code silently drops a frame with non-string meta). P4.
func TestChannelFrame_PollResultEvent(t *testing.T) {
	in := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -100,
		MessageID: 42,
		Timestamp: time.Date(2026, 6, 16, 9, 0, 0, 0, time.UTC),
		Kind:      c3types.InboundPollResult,
		Event: &c3types.InboundEvent{PollResult: &c3types.PollResult{
			PollID: "p1", Question: "Lunch?", TotalVoters: 3, IsClosed: true,
			Options: []c3types.PollOptionTally{{Text: "Pizza", VoterCount: 2}, {Text: "Tacos", VoterCount: 1}},
		}},
	}
	frame := buildClaudeChannelFrame(in)

	content, ok := frame["content"].(string)
	if !ok {
		t.Fatalf("content must be a string; got %T", frame["content"])
	}
	for _, want := range []string{"Poll results:", "Lunch?", "3 votes", "Pizza:2", "Tacos:1", "(closed)"} {
		if !strings.Contains(content, want) {
			t.Errorf("content missing %q; got %q", want, content)
		}
	}
	meta, ok := frame["meta"].(map[string]any)
	if !ok {
		t.Fatalf("meta must be a map; got %T", frame["meta"])
	}
	// Every meta value MUST be a string (frame contract).
	for k, v := range meta {
		if _, ok := v.(string); !ok {
			t.Errorf("meta.%s must be a string; got %T (%v)", k, v, v)
		}
	}
	if meta["kind"] != "poll_result" {
		t.Errorf("meta.kind: want poll_result; got %v", meta["kind"])
	}
	if meta["poll_id"] != "p1" || meta["total_voters"] != "3" || meta["is_closed"] != "true" {
		t.Errorf("poll meta wrong: %+v", meta)
	}
}

// ─── 3-state attach formatter tests moved 2026-05-19 ───────────────────────
//
// formatAttached was extracted to internal/ipc/format.go as part of the
// audit-triage extraction (docs/plans/2026-05-19-audit-triage.md).
// All formatter assertions (3-state Status, proposal-parity, OK + Err
// fallbacks) live in internal/ipc/format_test.go now — single source of
// truth for the formatter, single source of truth for its tests. The
// adapter call-sites just call ipc.FormatAttached / ipc.FormatTopics.

// TestNotifyTransport_DisconnectClearsConn exercises the preventative
// Disconnect() method documented in notify_transport.go: after a
// successful Connect+Notify, calling Disconnect must clear the stored
// Connection so the NEXT Notify returns the "connection not yet
// established" sentinel error rather than silently writing to a stale
// reference. Closes report MINOR m2 (2026-05-19).
func TestNotifyTransport_DisconnectClearsConn(t *testing.T) {
	var buf safeBuffer
	tx := newNotifyTransport(&mcp.IOTransport{
		Reader: nopCloseReader{strings.NewReader("")},
		Writer: nopCloseWriter{&buf},
	})
	if _, err := tx.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// First Notify works.
	if err := tx.Notify(context.Background(), "test/method", map[string]string{"k": "v"}); err != nil {
		t.Fatalf("first Notify: %v", err)
	}

	// After Disconnect, the wrapper's stored conn is cleared.
	tx.Disconnect()

	// Subsequent Notify returns the sentinel error (caller must Connect
	// again before writing).
	err := tx.Notify(context.Background(), "test/method", map[string]string{"k": "v"})
	if err == nil {
		t.Fatal("Notify after Disconnect: want error, got nil")
	}
	if !strings.Contains(err.Error(), "not yet established") {
		t.Errorf("Notify after Disconnect: want 'not yet established' sentinel, got %v", err)
	}
}

// TestChannelFrame_MultipleAttachments asserts that an inbound carrying several
// attachments (album / media-group, or a rich message with several media blocks)
// surfaces EVERY attachment to the agent: the first on the canonical unsuffixed
// keys, extras under _N keys, plus attachment_count.
func TestChannelFrame_MultipleAttachments(t *testing.T) {
	tid := int64(914)
	in := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -1001234567890,
		MessageID: 2382,
		TopicID:   &tid,
		Text:      "two photos",
		Sender:    c3types.Sender{UserID: 42, Username: "alice"},
		Timestamp: time.Date(2026, 6, 20, 1, 6, 16, 0, time.UTC),
		Attachments: []c3types.Attachment{
			{Kind: "photo", FileID: "photo_one", Size: 195428},
			{Kind: "photo", FileID: "photo_two", Size: 104814},
		},
	}
	frame := buildClaudeChannelFrame(in)
	meta, ok := frame["meta"].(map[string]any)
	if !ok {
		t.Fatalf("meta: want map[string]any, got %T", frame["meta"])
	}
	// First attachment stays on the canonical unsuffixed keys.
	if meta["attachment_file_id"] != "photo_one" {
		t.Errorf("attachment_file_id: want photo_one, got %v", meta["attachment_file_id"])
	}
	if meta["attachment_kind"] != "photo" {
		t.Errorf("attachment_kind: want photo, got %v", meta["attachment_kind"])
	}
	// Count reflects all attachments.
	if meta["attachment_count"] != "2" {
		t.Errorf("attachment_count: want 2, got %v", meta["attachment_count"])
	}
	// Second attachment surfaced under _2 keys so the agent can download it.
	if meta["attachment_file_id_2"] != "photo_two" {
		t.Errorf("attachment_file_id_2: want photo_two, got %v", meta["attachment_file_id_2"])
	}
	if meta["attachment_kind_2"] != "photo" {
		t.Errorf("attachment_kind_2: want photo, got %v", meta["attachment_kind_2"])
	}
	if meta["attachment_size_2"] != "104814" {
		t.Errorf("attachment_size_2: want 104814, got %v", meta["attachment_size_2"])
	}
	// content is the message text when present.
	if frame["content"] != "two photos" {
		t.Errorf("content: want %q, got %v", "two photos", frame["content"])
	}
}

// TestChannelFrame_SingleAttachmentUnchanged guards backward compatibility: a
// single attachment must emit NO attachment_count and NO _N keys, so existing
// single-attachment frames are byte-identical to before the multi-attachment fix.
func TestChannelFrame_SingleAttachmentUnchanged(t *testing.T) {
	in := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -100,
		MessageID: 1,
		Text:      "one photo",
		Timestamp: time.Date(2026, 6, 20, 1, 0, 0, 0, time.UTC),
		Attachments: []c3types.Attachment{
			{Kind: "photo", FileID: "only", Size: 5},
		},
	}
	frame := buildClaudeChannelFrame(in)
	meta := frame["meta"].(map[string]any)
	if meta["attachment_file_id"] != "only" {
		t.Errorf("attachment_file_id: want only, got %v", meta["attachment_file_id"])
	}
	if _, ok := meta["attachment_count"]; ok {
		t.Error("attachment_count must be ABSENT for a single attachment")
	}
	if _, ok := meta["attachment_file_id_2"]; ok {
		t.Error("attachment_file_id_2 must be ABSENT for a single attachment")
	}
}

// TestChannelFrame_MultipleAttachmentsEmptyTextLabel asserts the empty-text
// fallback reports the count (so an uncaptioned album does not masquerade as a
// single "(photo message)").
func TestChannelFrame_MultipleAttachmentsEmptyTextLabel(t *testing.T) {
	in := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -100,
		MessageID: 1,
		Timestamp: time.Date(2026, 6, 20, 1, 0, 0, 0, time.UTC),
		Attachments: []c3types.Attachment{
			{Kind: "photo", FileID: "a"},
			{Kind: "photo", FileID: "b"},
			{Kind: "video", FileID: "c"},
		},
	}
	frame := buildClaudeChannelFrame(in)
	if frame["content"] != "(3 attachments)" {
		t.Errorf("content: want %q, got %v", "(3 attachments)", frame["content"])
	}
	meta := frame["meta"].(map[string]any)
	if meta["attachment_count"] != "3" {
		t.Errorf("attachment_count: want 3, got %v", meta["attachment_count"])
	}
	if meta["attachment_file_id_3"] != "c" {
		t.Errorf("attachment_file_id_3: want c, got %v", meta["attachment_file_id_3"])
	}
}

func TestChannelFrame_MergedBurstGolden(t *testing.T) {
	topicID := int64(914)
	in := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -100,
		TopicID:   &topicID,
		MessageID: 103,
		Sender:    c3types.Sender{UserID: 2, Username: "bob"},
		Text:      "caption\nfollowup",
		Timestamp: time.Date(2026, 8, 16, 10, 11, 12, 0, time.UTC),
		Merged: []c3types.MergedSource{
			{MessageID: 101, Sender: c3types.Sender{UserID: 1, Username: "alice"}, Text: "caption"},
			{MessageID: 102, Sender: c3types.Sender{UserID: 2, Username: "bob"}},
			{MessageID: 103, Sender: c3types.Sender{UserID: 2, Username: "bob"}, Text: "followup", ForwardOrigin: &c3types.ForwardOrigin{Kind: "user", Name: "Carol"}},
		},
		Attachments: []c3types.Attachment{
			{Kind: "photo", FileID: "photo-101", SourceMessageID: 101},
			{Kind: "document", FileID: "document-102", SourceMessageID: 102},
		},
	}
	want := map[string]any{
		"content": "[msg 101 @alice] caption\n[msg 103 fwd Carol] followup",
		"meta": map[string]any{
			"chat_id":                 "-100",
			"ts":                      "2026-08-16T10:11:12.000Z",
			"message_id":              "103",
			"merged_count":            "3",
			"merged_message_ids":      "101,102,103",
			"user":                    "bob",
			"user_id":                 "2",
			"message_thread_id":       "914",
			"attachment_kind":         "photo",
			"attachment_file_id":      "photo-101",
			"attachment_message_id":   "101",
			"attachment_count":        "2",
			"attachment_kind_2":       "document",
			"attachment_file_id_2":    "document-102",
			"attachment_message_id_2": "102",
		},
	}
	if got := buildClaudeChannelFrame(in); !reflect.DeepEqual(got, want) {
		t.Fatalf("merged frame:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestChannelFrame_MergedCaptionlessAlbumGolden(t *testing.T) {
	in := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -100,
		MessageID: 110,
		Sender:    c3types.Sender{UserID: 7, Username: "album-user"},
		Timestamp: time.Date(2026, 8, 16, 10, 11, 12, 0, time.UTC),
		Merged: []c3types.MergedSource{
			{MessageID: 101, Sender: c3types.Sender{UserID: 7, Username: "album-user"}},
			{MessageID: 102, Sender: c3types.Sender{UserID: 7, Username: "album-user"}},
			{MessageID: 103, Sender: c3types.Sender{UserID: 7, Username: "album-user"}},
			{MessageID: 104, Sender: c3types.Sender{UserID: 7, Username: "album-user"}},
			{MessageID: 105, Sender: c3types.Sender{UserID: 7, Username: "album-user"}},
			{MessageID: 106, Sender: c3types.Sender{UserID: 7, Username: "album-user"}},
			{MessageID: 107, Sender: c3types.Sender{UserID: 7, Username: "album-user"}},
			{MessageID: 108, Sender: c3types.Sender{UserID: 7, Username: "album-user"}},
			{MessageID: 109, Sender: c3types.Sender{UserID: 7, Username: "album-user"}},
			{MessageID: 110, Sender: c3types.Sender{UserID: 7, Username: "album-user"}},
		},
		Attachments: []c3types.Attachment{
			{Kind: "photo", FileID: "photo-101", SourceMessageID: 101},
			{Kind: "photo", FileID: "photo-102", SourceMessageID: 102},
			{Kind: "photo", FileID: "photo-103", SourceMessageID: 103},
			{Kind: "photo", FileID: "photo-104", SourceMessageID: 104},
			{Kind: "photo", FileID: "photo-105", SourceMessageID: 105},
			{Kind: "photo", FileID: "photo-106", SourceMessageID: 106},
			{Kind: "photo", FileID: "photo-107", SourceMessageID: 107},
			{Kind: "photo", FileID: "photo-108", SourceMessageID: 108},
			{Kind: "photo", FileID: "photo-109", SourceMessageID: 109},
			{Kind: "photo", FileID: "photo-110", SourceMessageID: 110},
		},
	}
	want := map[string]any{
		"content": "(10 attachments)",
		"meta": map[string]any{
			"chat_id":                  "-100",
			"ts":                       "2026-08-16T10:11:12.000Z",
			"message_id":               "110",
			"merged_count":             "10",
			"merged_message_ids":       "101,102,103,104,105,106,107,108,109,110",
			"user":                     "album-user",
			"user_id":                  "7",
			"attachment_kind":          "photo",
			"attachment_file_id":       "photo-101",
			"attachment_message_id":    "101",
			"attachment_count":         "10",
			"attachment_kind_2":        "photo",
			"attachment_file_id_2":     "photo-102",
			"attachment_message_id_2":  "102",
			"attachment_kind_3":        "photo",
			"attachment_file_id_3":     "photo-103",
			"attachment_message_id_3":  "103",
			"attachment_kind_4":        "photo",
			"attachment_file_id_4":     "photo-104",
			"attachment_message_id_4":  "104",
			"attachment_kind_5":        "photo",
			"attachment_file_id_5":     "photo-105",
			"attachment_message_id_5":  "105",
			"attachment_kind_6":        "photo",
			"attachment_file_id_6":     "photo-106",
			"attachment_message_id_6":  "106",
			"attachment_kind_7":        "photo",
			"attachment_file_id_7":     "photo-107",
			"attachment_message_id_7":  "107",
			"attachment_kind_8":        "photo",
			"attachment_file_id_8":     "photo-108",
			"attachment_message_id_8":  "108",
			"attachment_kind_9":        "photo",
			"attachment_file_id_9":     "photo-109",
			"attachment_message_id_9":  "109",
			"attachment_kind_10":       "photo",
			"attachment_file_id_10":    "photo-110",
			"attachment_message_id_10": "110",
		},
	}
	if got := buildClaudeChannelFrame(in); !reflect.DeepEqual(got, want) {
		t.Fatalf("captionless merged album frame:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestChannelFrame_SingleMessageGoldenUnchanged(t *testing.T) {
	topicID := int64(914)
	in := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -100,
		TopicID:   &topicID,
		MessageID: 201,
		Sender:    c3types.Sender{UserID: 7, Username: "alice"},
		Text:      "single",
		Timestamp: time.Date(2026, 8, 16, 10, 11, 12, 0, time.UTC),
		ReplyTo:   &c3types.ReplyContext{MessageID: 99, User: c3types.Sender{Username: "bob"}, Text: "prior"},
		Attachments: []c3types.Attachment{
			{Kind: "document", FileID: "document-201", Size: 42, MIME: "text/plain", Name: "note.txt"},
		},
	}
	want := map[string]any{
		"content": "single",
		"meta": map[string]any{
			"chat_id":             "-100",
			"ts":                  "2026-08-16T10:11:12.000Z",
			"message_id":          "201",
			"user":                "alice",
			"user_id":             "7",
			"message_thread_id":   "914",
			"reply_to_message_id": "99",
			"reply_to_user":       "bob",
			"reply_to_text":       "prior",
			"attachment_kind":     "document",
			"attachment_file_id":  "document-201",
			"attachment_size":     "42",
			"attachment_mime":     "text/plain",
			"attachment_name":     "note.txt",
		},
	}
	if got := buildClaudeChannelFrame(in); !reflect.DeepEqual(got, want) {
		t.Fatalf("single frame changed:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestChannelFrame_DiscreteForwardedGolden(t *testing.T) {
	in := &c3types.Inbound{
		ChatID:        -100,
		MessageID:     301,
		Sender:        c3types.Sender{Username: "alice"},
		Text:          "forwarded",
		Timestamp:     time.Date(2026, 8, 16, 10, 11, 12, 0, time.UTC),
		ForwardOrigin: &c3types.ForwardOrigin{Kind: "channel", Name: "News Desk"},
	}
	want := map[string]any{
		"content": "forwarded",
		"meta": map[string]any{
			"chat_id":        "-100",
			"ts":             "2026-08-16T10:11:12.000Z",
			"message_id":     "301",
			"user":           "alice",
			"forwarded_from": "News Desk",
		},
	}
	if got := buildClaudeChannelFrame(in); !reflect.DeepEqual(got, want) {
		t.Fatalf("forwarded frame:\n got: %#v\nwant: %#v", got, want)
	}

	plain := *in
	plain.ForwardOrigin = nil
	plainFrame := buildClaudeChannelFrame(&plain)
	forwardedFrame := buildClaudeChannelFrame(in)
	delete(forwardedFrame["meta"].(map[string]any), "forwarded_from")
	if !reflect.DeepEqual(forwardedFrame, plainFrame) {
		t.Fatalf("forward provenance changed fields beyond forwarded_from:\n forwarded: %#v\nplain: %#v", forwardedFrame, plainFrame)
	}
}
