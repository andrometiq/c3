package broker

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// fakeNotifier records desktop-notify calls instead of spawning a real popup,
// and reports a controllable delivered result.
type fakeNotifier struct {
	ch        chan c3types.HealthEvent
	delivered bool
	onNotify  func() // optional: invoked inside Notify before returning (simulate a concurrent edge)
}

func newFakeNotifier() *fakeNotifier {
	return &fakeNotifier{ch: make(chan c3types.HealthEvent, 4), delivered: true}
}

func (f *fakeNotifier) Notify(ev c3types.HealthEvent) bool {
	f.ch <- ev
	if f.onNotify != nil {
		f.onNotify()
	}
	return f.delivered
}

func newTestBroker() *Broker {
	return New(&mappings.MappingsFile{SchemaVersion: 1, Channels: map[string]mappings.ChannelConfig{}, Mappings: map[string]mappings.Mapping{}})
}

func brokerWithAgent(t *testing.T) (*Broker, *fakeNotifier, *ipc.Conn) {
	t.Helper()
	b := newTestBroker()
	fn := newFakeNotifier()
	b.desktopNotifier = fn
	agentSide, brokerSide := net.Pipe()
	t.Cleanup(func() { agentSide.Close(); brokerSide.Close() })
	agentConn := ipc.NewConn(agentSide)
	b.Stubs.Register("claude", 4242, "/work", ipc.NewConn(brokerSide))
	return b, fn, agentConn
}

// readBroadcastWithin returns (msg, true) if an InboundMsg arrives within d,
// else (zero, false). Used to assert both presence and ABSENCE of a broadcast.
func readBroadcastWithin(agentConn *ipc.Conn, d time.Duration) (ipc.InboundMsg, bool) {
	type rr struct {
		m   ipc.InboundMsg
		err error
	}
	ch := make(chan rr, 1)
	go func() {
		raw, err := agentConn.ReadFrame()
		if err != nil {
			ch <- rr{err: err}
			return
		}
		var m ipc.InboundMsg
		err = json.Unmarshal(raw, &m)
		ch <- rr{m: m, err: err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return ipc.InboundMsg{}, false
		}
		return r.m, true
	case <-time.After(d):
		return ipc.InboundMsg{}, false
	}
}

func assertHealthFileState(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read health file: %v", err)
	}
	var got healthFile
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("health file invalid JSON: %v (%s)", err, data)
	}
	if got.Channels["telegram"].State != want {
		t.Errorf("health file channels.telegram.state = %q, want %q", got.Channels["telegram"].State, want)
	}
}

// TestNotifyHealth_DownEdge_AmbientOnly asserts a DOWN edge surfaces ONLY on the
// ambient status line: it does NOT invoke the desktop notifier and does NOT
// broadcast a system event to CLI sessions, while still setting the status cache
// and writing the status-line health file. NotifyHealth is now fully synchronous
// (invasive tier removed 2026-07-07), so a non-blocking check after it returns is
// race-free — if it were going to invoke the notifier it already would have.
func TestNotifyHealth_DownEdge_AmbientOnly(t *testing.T) {
	hf := filepath.Join(t.TempDir(), "health.json")
	t.Setenv("C3_HEALTH_FILE", hf)
	b, fn, agentConn := brokerWithAgent(t)
	host := NewBrokerHost(b, "telegram")
	host.NotifyHealth(c3types.HealthEvent{Channel: "telegram", State: c3types.HealthStateDown, Since: time.Now(), Consec: 3, Reason: "dial failures"})

	select {
	case <-fn.ch:
		t.Fatal("desktop notifier invoked on DOWN edge (invasive tier should be gone)")
	default:
	}
	if _, got := readBroadcastWithin(agentConn, 300*time.Millisecond); got {
		t.Fatal("CLI broadcast fired on DOWN edge (invasive tier should be gone)")
	}
	if b.lastHealthSnapshot()["telegram"].State != c3types.HealthStateDown {
		t.Error("status cache not set")
	}
	assertHealthFileState(t, hf, "down")
}

// TestNotifyHealth_RecoveryEdge_AmbientOnly asserts a recovery (UP) edge writes
// the ambient status file (state "up") and likewise never invokes the desktop
// notifier or broadcasts to CLI sessions.
func TestNotifyHealth_RecoveryEdge_AmbientOnly(t *testing.T) {
	hf := filepath.Join(t.TempDir(), "health.json")
	t.Setenv("C3_HEALTH_FILE", hf)
	b, fn, agentConn := brokerWithAgent(t)
	host := NewBrokerHost(b, "telegram")
	host.NotifyHealth(c3types.HealthEvent{Channel: "telegram", State: c3types.HealthStateUp, DownFor: 3 * time.Minute})

	select {
	case <-fn.ch:
		t.Fatal("desktop notifier invoked on recovery edge (invasive tier should be gone)")
	default:
	}
	if _, got := readBroadcastWithin(agentConn, 300*time.Millisecond); got {
		t.Fatal("CLI broadcast fired on recovery edge (invasive tier should be gone)")
	}
	assertHealthFileState(t, hf, "up")
}

// TestBroadcastSystemEvent_GateBypassIsBrokerOriginated documents + asserts the
// security boundary: the broadcast is a broker-originated InboundSystem event
// that intentionally does NOT pass through the allowlist gate. We assert it is
// delivered even though no allowlist is configured (a user message would be
// dropped), and that it carries no user content (ChatID/Sender zero).
func TestBroadcastSystemEvent_GateBypassIsBrokerOriginated(t *testing.T) {
	b := newTestBroker()
	// No allowlist at all — a user inbound would be default-denied.

	agentSide, brokerSide := net.Pipe()
	defer agentSide.Close()
	defer brokerSide.Close()
	agentConn := ipc.NewConn(agentSide)
	b.Stubs.Register("claude", 1, "/w", ipc.NewConn(brokerSide))

	got := make(chan ipc.InboundMsg, 1)
	go func() {
		raw, err := agentConn.ReadFrame()
		if err != nil {
			return
		}
		var m ipc.InboundMsg
		_ = json.Unmarshal(raw, &m)
		got <- m
	}()

	b.broadcastSystemEvent(&c3types.SystemEvent{
		Source:  "telegram",
		Level:   "warn",
		Title:   "telegram fetch DOWN",
		Message: "Cannot reach telegram.",
	})

	select {
	case m := <-got:
		in := m.Inbound
		if in.Kind != c3types.InboundSystem {
			t.Fatalf("Kind = %q, want system", in.Kind)
		}
		// No user content rode along on this broker-originated event.
		if in.ChatID != 0 || in.Sender.UserID != 0 || in.Text != "" {
			t.Fatalf("broker-originated event must carry no user content, got chat=%d sender=%d text=%q",
				in.ChatID, in.Sender.UserID, in.Text)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker-originated system event was not delivered (gate bypass failed)")
	}
}

// TestSetLastHealth_OlderEdgeDoesNotOverwriteNewer (Fix A): the compare-and-skip
// keeps the newest edge in the cache even when an older edge is processed later
// (NotifyHealth runs lock-free across 3 goroutines, so processing can invert).
func TestSetLastHealth_OlderEdgeDoesNotOverwriteNewer(t *testing.T) {
	b := newTestBroker()
	t2 := time.Now()
	t1 := t2.Add(-1 * time.Minute)
	b.setLastHealth(c3types.HealthEvent{Channel: "telegram", State: c3types.HealthStateUp, Since: t2})
	b.setLastHealth(c3types.HealthEvent{Channel: "telegram", State: c3types.HealthStateDown, Since: t1}) // older — must be skipped
	if got := b.lastHealthSnapshot()["telegram"].State; got != c3types.HealthStateUp {
		t.Errorf("older down edge overwrote newer up: state=%q, want up", got)
	}
}

// TestBroadcastSystemEvent_SkipsTransientAndDisconnected covers broadcastSystemEvent's
// fan-out skip logic, which stays live via notifyUpdateRestart even though the health
// path no longer broadcasts (invasive tier removed 2026-07-07). Driven DIRECTLY through
// broadcastSystemEvent: the transient c3-broker-cli client and a disconnected long-lived
// session must both be skipped, so the one live session receives exactly one frame.
func TestBroadcastSystemEvent_SkipsTransientAndDisconnected(t *testing.T) {
	b, _, agentConn := brokerWithAgent(t) // registers a live "claude" stub on agentConn

	// A transient CLI client (status/topics) — must be skipped.
	b.Stubs.Register("c3-broker-cli", 9999, "/tmp", struct{}{})
	// A disconnected long-lived session — must be skipped.
	dead := b.Stubs.Register("codex", 7, "/dead", nil)
	dead.MarkDisconnected()

	// Broadcast off the main goroutine: the write to the live stub's net.Pipe conn
	// is unbuffered and blocks until readBroadcastWithin drains it below.
	go b.broadcastSystemEvent(&c3types.SystemEvent{
		Source: "telegram", Level: "warn",
		Title: "telegram fetch DOWN", Message: "Cannot reach telegram.",
	})

	// The live stub receives exactly one InboundSystem frame...
	msg, got := readBroadcastWithin(agentConn, 2*time.Second)
	if !got {
		t.Fatal("live CLI session did not receive the advisory")
	}
	if msg.Inbound.Kind != c3types.InboundSystem {
		t.Fatalf("Kind = %q, want system", msg.Inbound.Kind)
	}
	// ...and no second frame (transient + disconnected stubs contributed nothing).
	if _, got := readBroadcastWithin(agentConn, 300*time.Millisecond); got {
		t.Fatal("more than one frame delivered — a transient/disconnected stub was not skipped")
	}
}
