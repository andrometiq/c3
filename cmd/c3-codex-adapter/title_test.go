package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/termtitle"
)

// titleScope: see Claude adapter equivalent. Swaps termtitle's package
// writer + isTTY probe for the lifetime of the test. Forces tty=true
// and clears C3_NO_TERMINAL_TITLE so production gating reduces to a
// no-op.
func titleScope(t *testing.T) *bytes.Buffer {
	t.Helper()
	t.Setenv("C3_NO_TERMINAL_TITLE", "")
	var buf bytes.Buffer
	prevW := termtitle.SetWriter(&buf)
	prevTTY := termtitle.SetIsTTY(func() bool { return true })
	t.Cleanup(func() {
		termtitle.SetWriter(prevW)
		termtitle.SetIsTTY(prevTTY)
	})
	return &buf
}

// drivePendingAttached mirrors dispatchAttached without a real broker.
// Polls for pending["attached"] registration to avoid a fixed sleep.
func drivePendingAttached(t *testing.T, a *adapter, msg ipc.AttachedMsg) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		a.pmu.Lock()
		_, ok := a.pending["attached"]
		a.pmu.Unlock()
		if ok {
			raw, err := json.Marshal(msg)
			if err != nil {
				t.Fatalf("marshal attached response: %v", err)
			}
			a.dispatchAttached(raw)
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("pending['attached'] never registered")
}

// newAdapterWithDummyConn wires the adapter to one end of net.Pipe and
// drains the other end so toolAttach's WriteJSON doesn't block.
func newAdapterWithDummyConn(t *testing.T) *adapter {
	t.Helper()
	a := newAdapter()
	pipeA, pipeB := net.Pipe()
	a.bmu.Lock()
	a.conn = ipc.NewConn(pipeA)
	a.bmu.Unlock()
	t.Cleanup(func() {
		_ = pipeA.Close()
		_ = pipeB.Close()
	})
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := pipeB.Read(buf); err != nil {
				return
			}
		}
	}()
	return a
}

// newAttachReq builds an mcp.CallToolRequest whose Params.Arguments is
// the JSON encoding of args. Matches the SDK raw-arg handler contract.
func newAttachReq(t *testing.T, args map[string]any) *mcp.CallToolRequest {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal args: %v", err)
	}
	return &mcp.CallToolRequest{
		Params: &mcp.CallToolParamsRaw{
			Name:      "attach",
			Arguments: raw,
		},
	}
}

func callToolAttachSync(t *testing.T, a *adapter, resp ipc.AttachedMsg) *mcp.CallToolResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req := newAttachReq(t, map[string]any{"name": "foo"})
	done := make(chan *mcp.CallToolResult, 1)
	go func() {
		result, _ := a.toolAttach(ctx, req)
		done <- result
	}()
	drivePendingAttached(t, a, resp)
	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		t.Fatal("toolAttach did not return in time")
	}
	return nil
}

func attachResultText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	if result == nil || len(result.Content) == 0 {
		t.Fatalf("toolAttach result = %+v", result)
	}
	content, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content[0] is %T, want *mcp.TextContent", result.Content[0])
	}
	return content.Text
}

// ── tests ───────────────────────────────────────────────────────────────────

func TestToolAttach_WebResultIncludesSpokenReplyGuidance(t *testing.T) {
	for _, test := range []struct {
		name     string
		initCaps *c3types.Capabilities
	}{
		{name: "after telegram initialize", initCaps: &c3types.Capabilities{Channel: "telegram", RichText: true}},
		{name: "after unknown initialize"},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := newAdapterWithDummyConn(t)
			a.helloAck.Capabilities = test.initCaps
			_ = a.buildInstructions()
			result := callToolAttachSync(t, a, ipc.AttachedMsg{
				Op:      ipc.OpAttached,
				OK:      true,
				Channel: "web",
				Name:    "web",
				Capabilities: &c3types.Capabilities{
					Channel: "web", RichText: true, SpokenReplies: true,
				},
			})
			if result.IsError {
				t.Fatalf("toolAttach result = %+v", result)
			}
			text := attachResultText(t, result)
			for _, want := range []string{"CHANNEL CAPABILITIES (web):", "Spoken replies: POSSIBLE", "WHILE VOICE IS ON"} {
				if !strings.Contains(text, want) {
					t.Errorf("attach web result missing %q:\n%s", want, text)
				}
			}
		})
	}
}

func TestToolAttach_TelegramResultMatchesPreModeText(t *testing.T) {
	a := newAdapterWithDummyConn(t)
	a.helloAck.Capabilities = &c3types.Capabilities{Channel: "telegram", RichText: true}
	_ = a.buildInstructions()
	topicID := int64(42)
	result := callToolAttachSync(t, a, ipc.AttachedMsg{
		Op:      ipc.OpAttached,
		OK:      true,
		Channel: "telegram",
		Name:    "project",
		ChatID:  -1001,
		TopicID: &topicID,
		Capabilities: &c3types.Capabilities{
			Channel: "telegram", RichText: true,
		},
	})
	if result.IsError {
		t.Fatalf("toolAttach result = %+v", result)
	}
	if got, want := attachResultText(t, result), `attached to "project" (chat -1001, thread 42)`; got != want {
		t.Fatalf("Telegram attach result changed:\n got: %q\nwant: %q", got, want)
	}
	if strings.Contains(attachResultText(t, result), "CHANNEL CAPABILITIES") {
		t.Fatalf("Telegram attach repeated initialize guidance: %q", attachResultText(t, result))
	}
}

func TestToolAttach_ForwardsWebExpr(t *testing.T) {
	t.Setenv("C3_CODEX_CWD", "/workspace/project")
	a := newAdapter()
	adapterSide, brokerSide := net.Pipe()
	a.bmu.Lock()
	a.conn = ipc.NewConn(adapterSide)
	a.bmu.Unlock()
	t.Cleanup(func() {
		_ = adapterSide.Close()
		_ = brokerSide.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan *mcp.CallToolResult, 1)
	go func() {
		result, _ := a.toolAttach(ctx, newAttachReq(t, map[string]any{"expr": "web"}))
		done <- result
	}()

	raw, err := ipc.NewConn(brokerSide).ReadFrame()
	if err != nil {
		t.Fatalf("read attach request: %v", err)
	}
	var request ipc.AttachReq
	if err := json.Unmarshal(raw, &request); err != nil {
		t.Fatalf("unmarshal attach request: %v", err)
	}
	if request.Expr != "web" {
		t.Fatalf("attach Expr=%q, want web", request.Expr)
	}
	drivePendingAttached(t, a, ipc.AttachedMsg{Op: ipc.OpAttached, Err: "test complete"})
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("toolAttach did not return in time")
	}
}

func TestToolAttach_EmitsTitleOnOK(t *testing.T) {
	buf := titleScope(t)
	a := newAdapterWithDummyConn(t)

	tid := int64(123)
	callToolAttachSync(t, a, ipc.AttachedMsg{
		Op:      ipc.OpAttached,
		OK:      true,
		Name:    "foo",
		Group:   "bar",
		TopicID: &tid,
		ChatID:  -1001,
		Status:  ipc.AttachStatusOK,
	})

	got := buf.String()
	want := "\x1b]0;c3: foo · bar\x07"
	if got != want {
		t.Errorf("title emit = %q; want %q", got, want)
	}
}

func TestToolAttach_EmitsTitleOnDM(t *testing.T) {
	buf := titleScope(t)
	a := newAdapterWithDummyConn(t)

	callToolAttachSync(t, a, ipc.AttachedMsg{
		Op:     ipc.OpAttached,
		OK:     true,
		Name:   "dm",
		ChatID: 4242,
		Status: ipc.AttachStatusOK,
	})

	got := buf.String()
	want := "\x1b]0;c3: dm\x07"
	if got != want {
		t.Errorf("title emit = %q; want %q", got, want)
	}
}

func TestToolAttach_NoEmitOnNoTopicsConfigured(t *testing.T) {
	buf := titleScope(t)
	a := newAdapterWithDummyConn(t)

	callToolAttachSync(t, a, ipc.AttachedMsg{
		Op:     ipc.OpAttached,
		OK:     false,
		Status: ipc.AttachStatusNoTopicsConfigured,
		Err:    "no topics",
	})

	if got := buf.String(); got != "" {
		t.Errorf("title emit = %q on no_topics_configured; want empty", got)
	}
}

func TestToolAttach_NoEmitOnPolicyRejected(t *testing.T) {
	buf := titleScope(t)
	a := newAdapterWithDummyConn(t)

	callToolAttachSync(t, a, ipc.AttachedMsg{
		Op:     ipc.OpAttached,
		OK:     false,
		Status: ipc.AttachStatusPolicyRejected,
		Err:    "policy",
	})

	if got := buf.String(); got != "" {
		t.Errorf("title emit = %q on policy_rejected; want empty", got)
	}
}

func TestToolAttach_NoEmitOnProposal(t *testing.T) {
	buf := titleScope(t)
	a := newAdapterWithDummyConn(t)

	callToolAttachSync(t, a, ipc.AttachedMsg{
		Op:                ipc.OpAttached,
		OK:                false,
		NeedsConfirmation: true,
		Proposal:          &ipc.Proposal{Action: "create", Name: "foo", Group: "default"},
	})

	if got := buf.String(); got != "" {
		t.Errorf("title emit = %q on proposal; want empty", got)
	}
}

func TestToolAttach_SuppressedByEnv(t *testing.T) {
	buf := titleScope(t)
	t.Setenv("C3_NO_TERMINAL_TITLE", "1")
	a := newAdapterWithDummyConn(t)

	tid := int64(7)
	callToolAttachSync(t, a, ipc.AttachedMsg{
		Op: ipc.OpAttached, OK: true, Name: "foo", Group: "bar",
		TopicID: &tid, Status: ipc.AttachStatusOK,
	})

	if got := buf.String(); got != "" {
		t.Errorf("title emit = %q with C3_NO_TERMINAL_TITLE=1; want empty", got)
	}
}

func TestToolAttach_NonTTY_NoEmit(t *testing.T) {
	t.Setenv("C3_NO_TERMINAL_TITLE", "")
	var buf bytes.Buffer
	prevW := termtitle.SetWriter(&buf)
	prevTTY := termtitle.SetIsTTY(func() bool { return false })
	t.Cleanup(func() {
		termtitle.SetWriter(prevW)
		termtitle.SetIsTTY(prevTTY)
	})

	a := newAdapterWithDummyConn(t)
	tid := int64(99)
	callToolAttachSync(t, a, ipc.AttachedMsg{
		Op: ipc.OpAttached, OK: true, Name: "foo", Group: "bar",
		TopicID: &tid, Status: ipc.AttachStatusOK,
	})

	if got := buf.String(); got != "" {
		t.Errorf("title emit = %q with isTTY=false; want empty", got)
	}
}
