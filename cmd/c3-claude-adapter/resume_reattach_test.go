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
)

// TestRenderResumeReattachFrame pins the task #47 first-turn wording: N>0 is an
// IMPERATIVE surface-now instruction (topic + count + fetch_queue); N==1 uses the
// singular noun; N==0 is a bare re-attach note with no fetch_queue nudge.
func TestRenderResumeReattachFrame(t *testing.T) {
	got := renderResumeReattachFrame("myproject", 3)
	for _, want := range []string{"myproject", "3", "held", "fetch_queue"} {
		if !strings.Contains(got, want) {
			t.Fatalf("N>0 frame missing %q; got %q", want, got)
		}
	}
	if got := renderResumeReattachFrame("t", 1); !strings.Contains(got, "1 held message") || strings.Contains(got, "messages") {
		t.Fatalf("N==1 must use singular 'message'; got %q", got)
	}
	got = renderResumeReattachFrame("myproject", 0)
	if strings.Contains(got, "fetch_queue") || !strings.Contains(got, "myproject") {
		t.Fatalf("N==0 must be a bare re-attach note (no fetch_queue); got %q", got)
	}
}

// newFlushTestAdapter wires an adapter whose notifyTx emits onto an in-memory
// buffer (so the emitted channel frame is inspectable) and whose broker conn is a
// net.Pipe (so the live re-peek round-trip can be driven from the peer). It
// returns the peer ipc.Conn, the raw peer net.Conn (for read deadlines), and
// emittedContent() which parses the "content" string of the emitted frame ("" if
// nothing was emitted).
func newFlushTestAdapter(t *testing.T) (a *adapter, peer *ipc.Conn, rawPeer net.Conn, emittedContent func() string) {
	t.Helper()
	a = newAdapter()

	buf := &safeBuffer{}
	a.notifyTx = newNotifyTransport(&mcp.IOTransport{
		Reader: nopCloseReader{strings.NewReader("")},
		Writer: nopCloseWriter{buf},
	})
	if _, err := a.notifyTx.Connect(context.Background()); err != nil {
		t.Fatalf("notifyTx.Connect: %v", err)
	}

	pipeA, pipeB := net.Pipe()
	t.Cleanup(func() { _ = pipeA.Close(); _ = pipeB.Close() })
	a.bmu.Lock()
	a.conn = ipc.NewConn(pipeA)
	a.bmu.Unlock()
	peer = ipc.NewConn(pipeB)
	rawPeer = pipeB

	emittedContent = func() string {
		out := buf.Bytes()
		if len(out) == 0 {
			return ""
		}
		var msg map[string]any
		if err := json.Unmarshal(bytes.TrimRight(out, "\n"), &msg); err != nil {
			t.Fatalf("unmarshal emitted frame (a double-emit would break this): %v\nwire: %s", err, out)
		}
		params, _ := msg["params"].(map[string]any)
		s, _ := params["content"].(string)
		return s
	}
	return a, peer, rawPeer, emittedContent
}

// answerPeek reads the FetchQueueReq the flush wrote, asserts it is a
// non-destructive peek (Ack=false), and dispatches a FetchQueueResp with the
// given returned-messages count and Remaining (so the live count = returned +
// remaining).
func answerPeek(t *testing.T, a *adapter, peer *ipc.Conn, returned, remaining int) {
	t.Helper()
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("read peek req: %v", err)
	}
	var req ipc.FetchQueueReq
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatalf("unmarshal peek req: %v", err)
	}
	if req.Ack {
		t.Fatalf("live re-peek must be NON-destructive (Ack=false); got Ack=true")
	}
	msgs := make([]c3types.Inbound, returned)
	for i := range msgs {
		msgs[i] = c3types.Inbound{Channel: "telegram", MessageID: int64(i + 1), Text: "held"}
	}
	respRaw, _ := json.Marshal(ipc.FetchQueueResp{
		Op: ipc.OpFetchQueueResult, ID: req.ID, Remaining: remaining, Messages: msgs,
	})
	a.dispatchFetchQueueResult(respRaw)
}

// TestFlushPendingRecoverNotice_LivePeekEmitsLiveCount: on the first tools/call
// the flush does a live re-peek and emits the CURRENT count (returned+remaining),
// not the stored at-recover text.
func TestFlushPendingRecoverNotice_LivePeekEmitsLiveCount(t *testing.T) {
	a, peer, _, emitted := newFlushTestAdapter(t)
	a.setAttachedTopic("myproject")
	a.setPendingRecoverNotice("STORED-FALLBACK should not be used")

	done := make(chan struct{})
	go func() { a.flushPendingRecoverNotice(); close(done) }()

	answerPeek(t, a, peer, 1, 4) // live count = 1 + 4 = 5

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not complete")
	}

	got := emitted()
	for _, want := range []string{"myproject", "5", "fetch_queue"} {
		if !strings.Contains(got, want) {
			t.Fatalf("live-count frame missing %q; got %q", want, got)
		}
	}
	if strings.Contains(got, "STORED-FALLBACK") {
		t.Fatalf("live re-peek succeeded but emitted the stored fallback: %q", got)
	}
}

// TestFlushPendingRecoverNotice_ZeroCountEmitsBareReattach: a live re-peek that
// returns 0 held messages emits the bare re-attach note (no fetch_queue nudge).
func TestFlushPendingRecoverNotice_ZeroCountEmitsBareReattach(t *testing.T) {
	a, peer, _, emitted := newFlushTestAdapter(t)
	a.setAttachedTopic("myproject")
	a.setPendingRecoverNotice("stored")

	done := make(chan struct{})
	go func() { a.flushPendingRecoverNotice(); close(done) }()

	answerPeek(t, a, peer, 0, 0) // live count = 0

	<-done
	got := emitted()
	if !strings.Contains(got, "myproject") || strings.Contains(got, "fetch_queue") {
		t.Fatalf("N==0 flush must emit a bare re-attach note (no fetch_queue); got %q", got)
	}
}

// TestFlushPendingRecoverNotice_FallsBackToStoredOnTimeout: an unresponsive
// broker must not hang the tools/call — the flush falls back to the stored notice
// within the (shortened) cap.
func TestFlushPendingRecoverNotice_FallsBackToStoredOnTimeout(t *testing.T) {
	old := livePeekTimeout
	livePeekTimeout = 50 * time.Millisecond
	defer func() { livePeekTimeout = old }()

	a, peer, _, emitted := newFlushTestAdapter(t)
	a.setAttachedTopic("myproject")
	const stored = "STORED-FALLBACK notice text"
	a.setPendingRecoverNotice(stored)

	done := make(chan struct{})
	start := time.Now()
	go func() { a.flushPendingRecoverNotice(); close(done) }()

	// Drain the peek req so the net.Pipe write unblocks, but never respond → the
	// re-peek times out and the flush falls back to the stored text.
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatalf("read peek req: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not complete within the cap")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("flush blocked too long (%s); the cap should bound it near livePeekTimeout", elapsed)
	}
	if got := emitted(); !strings.Contains(got, stored) {
		t.Fatalf("timeout fallback: emitted %q, want it to carry the stored text %q", got, stored)
	}
}

// TestFlushPendingRecoverNotice_OnceOnly: after the first flush the pending flag
// is cleared, so a second flush is a total no-op — it neither re-peeks nor emits.
func TestFlushPendingRecoverNotice_OnceOnly(t *testing.T) {
	old := livePeekTimeout
	livePeekTimeout = 50 * time.Millisecond
	defer func() { livePeekTimeout = old }()

	a, peer, rawPeer, emitted := newFlushTestAdapter(t)
	a.setAttachedTopic("myproject")
	a.setPendingRecoverNotice("stored")

	done := make(chan struct{})
	go func() { a.flushPendingRecoverNotice(); close(done) }()
	if _, err := peer.ReadFrame(); err != nil { // drain the first peek req; let it time out
		t.Fatalf("read first peek req: %v", err)
	}
	<-done
	if emitted() == "" {
		t.Fatal("first flush must emit the fallback notice")
	}

	// Second flush: nothing pending → must not write a peek req at all.
	go a.flushPendingRecoverNotice()
	_ = rawPeer.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if _, err := peer.ReadFrame(); err == nil {
		t.Fatal("second flush must not write a peek req (once-only: flag cleared)")
	}
}

// TestFlushPendingRecoverNotice_NoTTLDropAfterDelay: with the pendingRecoverTTL
// removed there is no age gate — a notice that has waited (well past the old
// 5-minute drop) must still flush and surface the live count. The field that
// tracked recover time is gone, so the sleep only documents that elapsed time no
// longer matters.
func TestFlushPendingRecoverNotice_NoTTLDropAfterDelay(t *testing.T) {
	a, peer, _, emitted := newFlushTestAdapter(t)
	a.setAttachedTopic("myproject")
	a.setPendingRecoverNotice("stored fallback")

	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() { a.flushPendingRecoverNotice(); close(done) }()
	answerPeek(t, a, peer, 0, 2) // live count = 2
	<-done

	if got := emitted(); !strings.Contains(got, "2") || !strings.Contains(got, "fetch_queue") {
		t.Fatalf("delayed flush must still fire with the live count; got %q", got)
	}
}
