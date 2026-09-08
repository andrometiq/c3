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

// titleScope swaps termtitle's package-level writer + isTTY probe to a
// captured buffer for the duration of the calling test. Restores the
// originals on cleanup. Forces isTTY=true so the gating decision in
// production reduces to the C3_NO_TERMINAL_TITLE check, which we also
// neutralize via t.Setenv.
func titleScope(t *testing.T) *bytes.Buffer {
	t.Helper()
	t.Setenv("C3_NO_TERMINAL_TITLE", "") // ensure not suppressed
	var buf bytes.Buffer
	prevW := termtitle.SetWriter(&buf)
	prevTTY := termtitle.SetIsTTY(func() bool { return true })
	t.Cleanup(func() {
		termtitle.SetWriter(prevW)
		termtitle.SetIsTTY(prevTTY)
	})
	return &buf
}

// drivePendingAttached pumps a synthetic OpAttached response into the
// adapter's pending["attached"] channel so toolAttach returns. Mirrors
// brokerReader's dispatchAttached helper without requiring a real
// broker conn. Polls for the registration to avoid a fixed sleep.
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

// newAdapterWithDummyConn returns an adapter wired to one end of a
// net.Pipe so toolAttach's WriteJSON to the broker succeeds. The other
// end is drained in a goroutine to keep the writer from blocking.
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

// newAttachReq builds a mcp.CallToolRequest with the supplied args
// encoded as Params.Arguments (json.RawMessage), matching the SDK's
// raw-arg handler contract that toolAttach already uses.
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

// callToolAttachSync invokes toolAttach in a goroutine, drives the
// pending channel with the supplied response, and returns when
// toolAttach returns. Ensures the title-emit (if any) has flushed
// before the test reads the buffer.
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
				t.Fatalf("toolAttach returned an error: %q", resultText(t, result))
			}
			text := resultText(t, result)
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
		t.Fatalf("toolAttach returned an error: %q", resultText(t, result))
	}
	if got, want := resultText(t, result), `attached to "project" (chat -1001, thread 42)`; !strings.HasPrefix(got, want+"\n\nLive route: queue-only") {
		t.Fatalf("Telegram attach result changed:\n got: %q\nwant: %q", got, want)
	}
	if strings.Contains(resultText(t, result), "CHANNEL CAPABILITIES") {
		t.Fatalf("Telegram attach repeated initialize guidance: %q", resultText(t, result))
	}
}

// TestToolAttach_EmitsTitleOnOK verifies the happy path: an OK attach
// response triggers exactly one title-bar escape with the
// "c3: <name> · <group>" body.
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

// TestToolAttach_EmitsTitleOnDM verifies DM attach (name="dm", no group)
// renders "c3: dm".
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

// TestToolAttach_NoEmitOnNoTopicsConfigured verifies the
// no_topics_configured failure path leaves the title untouched.
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

// TestToolAttach_NoEmitOnPolicyRejected verifies policy_rejected leaves
// the title untouched.
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

// TestToolAttach_NoEmitOnProposal verifies NeedsConfirmation proposals
// leave the title untouched — only the user-confirmed re-attach (which
// lands as OK=true) should flip the title.
func TestToolAttach_NoEmitOnProposal(t *testing.T) {
	buf := titleScope(t)
	a := newAdapterWithDummyConn(t)

	callToolAttachSync(t, a, ipc.AttachedMsg{
		Op:                ipc.OpAttached,
		OK:                false,
		NeedsConfirmation: true,
		Proposal: &ipc.Proposal{
			Action: "create", Name: "foo", Group: "default",
		},
	})

	if got := buf.String(); got != "" {
		t.Errorf("title emit = %q on proposal; want empty", got)
	}
}

// TestToolAttach_SuppressedByEnv verifies C3_NO_TERMINAL_TITLE=1 short-
// circuits emit even on a clean OK attach.
func TestToolAttach_SuppressedByEnv(t *testing.T) {
	buf := titleScope(t)
	t.Setenv("C3_NO_TERMINAL_TITLE", "1") // override titleScope's "" default
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

// TestToolAttach_NonTTY_NoEmit verifies isTTY=false short-circuits emit.
// Important so piped / log-captured stderr never sees escape garbage.
func TestToolAttach_NonTTY_NoEmit(t *testing.T) {
	// Don't use the shared scope helper — we want isTTY=false here.
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

// TestToolDetach_EmitsClearTitle verifies that detach restores the
// terminal default by emitting the empty-title escape.
func TestToolDetach_EmitsClearTitle(t *testing.T) {
	buf := titleScope(t)
	a := newAdapterWithDummyConn(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := a.toolDetach(ctx, nil); err != nil {
		t.Fatalf("toolDetach: %v", err)
	}

	got := buf.String()
	if !strings.Contains(got, "\x1b]0;\x07") {
		t.Errorf("toolDetach did not write empty-title escape; buf = %q", got)
	}
}
