package broker

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// liveHolder registers a LIVE, connected stub (real ipc.Conn) holding key, and
// returns the stub plus a channel that fires whenever the broker pushes an
// OpInbound to it. Used to prove push-vs-hold routing for the render-capability
// branch. Mirrors the harness in queue_integration_test.go.
func liveHolder(t *testing.T, b *Broker, key RouteKey) (*Stub, <-chan struct{}) {
	t.Helper()
	adapterEnd, holderEnd := net.Pipe()
	t.Cleanup(func() { adapterEnd.Close(); holderEnd.Close() })
	adapterConn := ipc.NewConn(adapterEnd)
	pushed := make(chan struct{}, 4)
	go func() {
		for {
			raw, err := adapterConn.ReadFrame()
			if err != nil {
				return
			}
			if op, _ := ipc.PeekOp(raw); op == ipc.OpInbound {
				select {
				case pushed <- struct{}{}:
				default:
				}
			}
		}
	}()
	stub := &Stub{CLI: "claude", PID: os.Getpid(), CWD: "/home/u/proj", ConnID: 1}
	stub.Reattach(ipc.NewConn(holderEnd), 1)
	b.Routes.Claim(key, stub)
	return stub, pushed
}

// A render-INCAPABLE holder (host silently drops channel pushes) must NOT be
// pushed a durable human message: it falls through to the queue + held-notice
// (recoverable via fetch_queue) while keeping its claim for outbound. This is the
// forked-session blackhole fix.
func TestForwardOrFallback_RenderIncapableHolder_HeldNotPushed(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	stub, pushed := liveHolder(t, b, key)
	stub.SetCannotRender(true)

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -1001234567890, TopicID: &tid,
		MessageID: 1, Text: "hi", Timestamp: time.Now(),
	}
	w.forwardOrFallback(context.Background(), in, 1)

	// Claim preserved — the session still holds the route for outbound.
	if _, held := b.Routes.Holder(key); !held {
		t.Error("render-incapable claim must be preserved (outbound still works)")
	}
	// Held-notice sent to the topic exactly once (the human learns messages queue).
	if got := len(fc.sendRepliesSnapshot()); got != 1 {
		t.Errorf("expected 1 held-notice SendReply, got %d", got)
	}
	// The message is durably queued — recoverable via fetch_queue.
	if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 1 {
		t.Errorf("expected 1 durably-queued message, got %d", n)
	}
	// And NO channel push reached the (blackhole) adapter.
	select {
	case <-pushed:
		t.Error("render-incapable holder must NOT be pushed an OpInbound")
	case <-time.After(150 * time.Millisecond):
	}
}

// Regression contrast: an IDENTICAL setup with a render-CAPABLE holder (the
// normal flagged session) takes the unchanged fast path — the message is pushed
// (delivered) and NO held-notice is sent.
func TestForwardOrFallback_RenderCapableHolder_DeliveredNoHold(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	_, pushed := liveHolder(t, b, key) // default render-capable (no SetCannotRender)

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -1001234567890, TopicID: &tid,
		MessageID: 2, Text: "hi", Timestamp: time.Now(),
	}
	w.forwardOrFallback(context.Background(), in, 1)

	// Delivered to the adapter.
	select {
	case <-pushed:
	case <-time.After(2 * time.Second):
		t.Fatal("render-capable holder must be delivered an OpInbound")
	}
	// No held-notice on the live delivery path.
	if got := len(fc.sendRepliesSnapshot()); got != 0 {
		t.Errorf("capable delivery must not send a held-notice; got %d", got)
	}
	if _, held := b.Routes.Holder(key); !held {
		t.Error("capable holder keeps its claim")
	}
}

// The blackhole fix is gated to human messages: a synthesized EVENT on a
// render-incapable holder is NOT diverted to the queue (events are never queued
// and carry no lost content). It stays on the current push path — proving the
// !in.IsEvent() guard.
func TestForwardOrFallback_RenderIncapableHolder_EventNotDiverted(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	stub, pushed := liveHolder(t, b, key)
	stub.SetCannotRender(true)

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	event := &c3types.Inbound{
		Channel: "telegram", ChatID: -1001234567890, TopicID: &tid,
		MessageID: 3, Kind: c3types.InboundPollResult,
		Event: &c3types.InboundEvent{PollResult: &c3types.PollResult{PollID: "p", IsClosed: true}},
	}
	w.forwardOrFallback(context.Background(), event, 0)

	// The event took the normal claimed-push path (not the held path).
	select {
	case <-pushed:
	case <-time.After(2 * time.Second):
		t.Fatal("an event on a render-incapable holder must still take the push path")
	}
	if got := len(fc.sendRepliesSnapshot()); got != 0 {
		t.Errorf("an event must not fire a held-notice; got %d", got)
	}
}

// Broker-side wire-compat: a hello WITHOUT the cannot_render_channels field (old
// adapter) leaves the stub render-capable, so its inbound is delivered normally.
func TestStub_CanRenderPush_DefaultsTrue(t *testing.T) {
	s := &Stub{CLI: "claude", PID: 1, CWD: "/x", ConnID: 1}
	if !s.CanRenderPush() {
		t.Error("a stub that never reported cannot-render must default to renderable")
	}
	s.SetCannotRender(true)
	if s.CanRenderPush() {
		t.Error("SetCannotRender(true) must make CanRenderPush false")
	}
}
