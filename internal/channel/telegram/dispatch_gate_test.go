package telegram

import (
	"fmt"
	"sync"
	"testing"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/karthikeyan5/c3/internal/c3types"
	"github.com/karthikeyan5/c3/internal/channel"
)

// fakeHost is a test double for channel.Host. Records every call so the
// dispatch-gate tests can assert "Emit was never called when gate dropped".
type fakeHost struct {
	mu       sync.Mutex
	emitted  []*c3types.Inbound
	decision channel.GateInboundDecision
	logs     []string
	health   []c3types.HealthEvent

	// cmdHandled lets a test opt this double into claiming "/status" (used by
	// the not-routed test in 5b); defaults to declining so the channel routes
	// normally.
	cmdHandled bool

	// cmdFn, when set, overrides HandleCommand's canned behavior entirely —
	// used by the broker-command intercept tests to simulate the silent-drop
	// ("", true) and async ("", true) replies without a real broker.
	cmdFn func(*c3types.Inbound) (string, bool)

	// emitDrops makes Emit return false (the worker-queue-full / stopped DROP
	// case) so the I4 stranded-update test can drive the drop branch. Default
	// false ⇒ Emit accepts (returns true), matching the steady-state path.
	emitDrops bool

	// cmdCalled records whether HandleCommand was invoked, so the I-SEC gate-order
	// tests can assert a stranger's /status NEVER reaches the command handler (the
	// gate must drop it first).
	cmdCalled bool
}

func (h *fakeHost) Config(name string, target any) error { return nil }

func (h *fakeHost) Emit(in *c3types.Inbound) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, in)
	return !h.emitDrops
}

func (h *fakeHost) Logf(format string, args ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logs = append(h.logs, fmt.Sprintf(format, args...))
}

func (h *fakeHost) Done() <-chan struct{} { return nil }

func (h *fakeHost) GateInbound(in *c3types.Inbound) channel.GateInboundDecision {
	return h.decision
}

// HandleCommand satisfies channel.Host. cmdHandled lets a test opt this double
// into claiming "/status" (used by the not-routed test in 5b); it defaults to
// declining so the channel routes normally. cmdCalled records invocation so the
// I-SEC tests can assert a stranger's /status never reaches it (gate-first).
func (h *fakeHost) HandleCommand(in *c3types.Inbound) (string, bool) {
	h.mu.Lock()
	h.cmdCalled = true
	fn := h.cmdFn
	h.mu.Unlock()
	if fn != nil {
		return fn(in)
	}
	if h.cmdHandled && in != nil && in.Text == "/status" {
		return "📊 ok", true
	}
	return "", false
}

func (h *fakeHost) handleCommandCalled() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cmdCalled
}

func (h *fakeHost) NotifyHealth(ev c3types.HealthEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.health = append(h.health, ev)
}

func (h *fakeHost) healthEvents() []c3types.HealthEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]c3types.HealthEvent, len(h.health))
	copy(out, h.health)
	return out
}

func (h *fakeHost) emitCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.emitted)
}

func makeChannel(host channel.Host) *Channel {
	return &Channel{host: host, cfg: Config{}}
}

func textMsg(text string, userID int64) *gotgbot.Message {
	return &gotgbot.Message{
		MessageId: 100,
		From:      &gotgbot.User{Id: userID},
		Chat:      gotgbot.Chat{Id: userID}, // positive = DM
		Date:      1715151931,
		Text:      text,
	}
}

func TestDispatchMessage_GateDrop_DoesNotEmit(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundDrop}
	c := makeChannel(h)
	c.dispatchMessage(1, textMsg("stranger", 99), false, nil)
	if got := h.emitCount(); got != 0 {
		t.Errorf("Emit called %d times for dropped inbound; want 0", got)
	}
}

func TestDispatchMessage_GatePairConsumed_DoesNotEmit(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundPairConsumed}
	c := makeChannel(h)
	c.dispatchMessage(1, textMsg("5829", 99), false, nil)
	if got := h.emitCount(); got != 0 {
		t.Errorf("Emit called %d times after pair-consumed; want 0 (control-plane signal, not content)", got)
	}
}

func TestDispatchMessage_GateAllow_Emits(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannel(h)
	c.dispatchMessage(1, textMsg("hi", 42), false, nil)
	if got := h.emitCount(); got != 1 {
		t.Errorf("Emit called %d times for allowed inbound; want 1", got)
	}
}
