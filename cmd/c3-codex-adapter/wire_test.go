package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/c3types"
)

// TestServerInfoName guards the contract that the Codex adapter advertises
// `c3_codex` as serverInfo.name (matches mcp_servers.<key> in Codex's
// config). Same lesson as the Claude adapter (see
// cmd/c3-claude-adapter/wire_test.go TestServerInfoName) — channel
// dispatch keys on this exact string.
func TestServerInfoName(t *testing.T) {
	if adapterName != "c3_codex" {
		t.Fatalf("adapterName must be %q to match mcp_servers key; got %q", "c3_codex", adapterName)
	}

	a := newAdapter()
	// Seed the hello-ack caps so buildInstructions folds capability.GuidanceFor
	// into the MCP `instructions` (P4) — same path + parity as the Claude
	// adapter. Mirrors the Telegram v1 manifest's load-bearing values.
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

	result := sess.InitializeResult()
	if result == nil {
		t.Fatal("InitializeResult is nil")
	}
	if result.ServerInfo == nil || result.ServerInfo.Name != "c3_codex" {
		var got string
		if result.ServerInfo != nil {
			got = result.ServerInfo.Name
		}
		t.Fatalf("serverInfo.name = %q; want %q", got, "c3_codex")
	}
	if result.Instructions == "" {
		t.Fatal("instructions empty in initialize response")
	}
	// P4: the capability guidance (capability.GuidanceFor, folded in via
	// mode.Combined(caps)) must be present in the MCP instructions so the
	// Codex agent is capability-aware — parity with the Claude adapter.
	// Assert the header (always present) AND a positive cap line (proves the
	// seeded caps flowed through, not just a zero-value default).
	for _, want := range []string{
		"CHANNEL CAPABILITIES",
		"Typing: shown automatically",
	} {
		if !strings.Contains(result.Instructions, want) {
			t.Errorf("instructions missing capability-guidance phrase %q:\n%s", want, result.Instructions)
		}
	}

	// Verify tools/list returns the expected Codex tool set
	// (attach, topics, fetch_queue, retranscribe, reply, react, edit_message,
	// poll, download_attachment, codex_forward) — a regression here breaks
	// every adapter operation for Codex. `send_typing` is deliberately ABSENT
	// (P5): the typing indicator is relayed programmatically by the broker, not
	// via an LLM tool. The in-memory `inbox` ring tool is RETIRED in favor of the
	// broker-backed `fetch_queue` (durable queue is the source of truth).
	listResult, err := sess.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	wantTools := []string{
		"attach", "topics", "fetch_queue", "retranscribe", "reply", "react",
		"edit_message", "poll", "download_attachment", "codex_forward",
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
	// format-for-readability nudge (formatting-policy 2026-06-20), in parity with
	// the Claude adapter.
	if d := gotDesc["reply"]; !strings.Contains(d, "whenever it makes the reply easier to read") {
		t.Errorf("reply Description missing the formatting nudge; got %q", d)
	}
	// send_typing must NOT be agent-facing (P5): the broker relays typing
	// programmatically. A regression that re-registers it would let an LLM
	// pulse typing, defeating the deterministic relay.
	if got["send_typing"] {
		t.Errorf("send_typing must NOT be registered as an agent tool (P5: broker-relayed); got %v", got)
	}
	// The in-memory `inbox` ring tool is RETIRED: a regression that re-registers
	// it would resurrect a cap-100 lossy buffer alongside the durable queue.
	if got["inbox"] {
		t.Errorf("inbox tool must be RETIRED in favor of broker-backed fetch_queue; got %v", got)
	}
}

// ─── formatAttached tests moved 2026-05-19 ─────────────────────────────────
//
// Both adapters' formatAttached + formatTopics were extracted to
// internal/ipc/format.go as part of the audit triage
// (docs/plans/2026-05-19-audit-triage.md). The previously-Codex-specific
// proposal-parity guard (created when the Codex adapter silently lacked
// disambiguate_dm + force_steal branches) is now centrally enforced in
// internal/ipc/format_test.go::TestFormatAttached_ProposalParity, so a
// single source of truth covers both adapters.

func TestCodexAttachTool_AcceptsPolicyRejectedArg(t *testing.T) {
	// Wire-shape guard: the agent must be able to pass
	// `policy_rejected=true` to the `attach` tool. If the input schema
	// blocks it, the agent will never be able to surface the host
	// rejection cleanly. InputSchema is registered as map[string]any.
	a := newAdapter()
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

	listResult, err := sess.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	var attachTool *mcp.Tool
	for _, tool := range listResult.Tools {
		if tool.Name == "attach" {
			attachTool = tool
			break
		}
	}
	if attachTool == nil {
		t.Fatal("attach tool not advertised")
	}
	// InputSchema is *jsonschema.Schema after deserialization through
	// the SDK; marshal back to JSON and substring-check so we don't
	// bind to internal SDK type names.
	raw, err := json.Marshal(attachTool.InputSchema)
	if err != nil {
		t.Fatalf("marshal attach InputSchema: %v", err)
	}
	if !strings.Contains(string(raw), `"policy_rejected"`) {
		t.Errorf("attach input schema missing 'policy_rejected' property; schema=%s", raw)
	}
}

// TestLogNotifyTransport_DisconnectClearsConn — analogous to the Claude
// adapter test (TestNotifyTransport_DisconnectClearsConn). Calling
// Disconnect after Connect+Notify clears the captured Connection so
// the next Notify returns the "connection not yet established"
// sentinel. Closes report MINOR m2 (2026-05-19).
func TestLogNotifyTransport_DisconnectClearsConn(t *testing.T) {
	clientT, serverT := mcp.NewInMemoryTransports()
	tx := newLogNotifyTransport(serverT)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := tx.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// Drain the in-memory transport on the client side so writes don't
	// block — we don't actually care about the bytes, just that Notify
	// returns nil on the first call.
	go func() {
		clientConn, err := clientT.Connect(ctx)
		if err != nil {
			return
		}
		for {
			if _, err := clientConn.Read(ctx); err != nil {
				return
			}
		}
	}()

	if err := tx.Notify(ctx, "notifications/message", map[string]string{"k": "v"}); err != nil {
		t.Fatalf("first Notify: %v", err)
	}

	tx.Disconnect()

	err := tx.Notify(ctx, "notifications/message", map[string]string{"k": "v"})
	if err == nil {
		t.Fatal("Notify after Disconnect: want error, got nil")
	}
	if !strings.Contains(err.Error(), "not yet established") {
		t.Errorf("Notify after Disconnect: want 'not yet established' sentinel, got %v", err)
	}
}
