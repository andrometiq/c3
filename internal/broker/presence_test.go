package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
)

type presenceCall struct {
	chatID  int64
	topicID *int64
	holder  *channel.RouteHolder
}

type presenceChannel struct {
	*fakeChannel
	mu      sync.Mutex
	calls   []presenceCall
	panics  bool
	entered atomic.Bool
}

func (c *presenceChannel) SetRouteHolder(chatID int64, topicID *int64, holder *channel.RouteHolder) {
	c.entered.Store(true)
	if c.panics {
		panic("presence test panic")
	}
	call := presenceCall{chatID: chatID}
	if topicID != nil {
		copy := *topicID
		call.topicID = &copy
	}
	if holder != nil {
		copy := *holder
		call.holder = &copy
	}
	c.mu.Lock()
	c.calls = append(c.calls, call)
	c.mu.Unlock()
}

func (c *presenceChannel) snapshot() []presenceCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]presenceCall(nil), c.calls...)
}

func newPresenceBroker(t *testing.T) (*Broker, *presenceChannel) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	b := New(mfWithTelegram())
	notifier := &presenceChannel{fakeChannel: &fakeChannel{}}
	if err := b.RegisterChannel(notifier); err != nil {
		b.Shutdown()
		t.Fatal(err)
	}
	return b, notifier
}

func waitForPresenceCalls(t *testing.T, notifier *presenceChannel, count int) []presenceCall {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if calls := notifier.snapshot(); len(calls) >= count {
			return calls
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("presence calls=%d, want at least %d", len(notifier.snapshot()), count)
	return nil
}

func TestPresenceNotifierClaimReleaseAndBrokerSideRelease(t *testing.T) {
	b, notifier := newPresenceBroker(t)
	defer b.Shutdown()

	topicID := int64(281)
	key := MakeRouteKey("telegram", -100, &topicID)
	stub := &Stub{CLI: "claude", PID: 4242, CWD: "/workspace/project", ConnID: 7}
	stub.SetStableSessionID("session-123")
	before := time.Now().UTC()
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatal("claim failed")
	}
	claimed := waitForPresenceCalls(t, notifier, 1)[0]
	if claimed.chatID != -100 || claimed.topicID == nil || *claimed.topicID != topicID {
		t.Fatalf("claim route=%d/%v, want -100/%d", claimed.chatID, claimed.topicID, topicID)
	}
	if claimed.holder == nil || claimed.holder.CLI != "claude" || claimed.holder.PID != 4242 || claimed.holder.CWD != "/workspace/project" || claimed.holder.SessionID != "session-123" {
		t.Fatalf("claim holder=%+v", claimed.holder)
	}
	if claimed.holder.Since.Before(before) || claimed.holder.Since.After(time.Now().UTC()) {
		t.Fatalf("claim since=%s, want current claim time", claimed.holder.Since)
	}

	b.Routes.Release(key, stub.ConnID)
	released := waitForPresenceCalls(t, notifier, 2)[1]
	if released.holder != nil {
		t.Fatalf("release holder=%+v, want nil", released.holder)
	}

	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatal("second claim failed")
	}
	waitForPresenceCalls(t, notifier, 3)
	b.Routes.ReleaseAllByConnID(stub.ConnID)
	connDrop := waitForPresenceCalls(t, notifier, 4)[3]
	if connDrop.holder != nil {
		t.Fatalf("conn-drop holder=%+v, want nil", connDrop.holder)
	}
}

func TestPresenceNotifierConnDropPublishesRelease(t *testing.T) {
	b, notifier := newPresenceBroker(t)
	defer b.Shutdown()
	peer, closeConn := peerPair(t, b)
	defer closeConn()
	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: -1, CWD: "/workspace/project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, Target: "dm"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	waitForPresenceCalls(t, notifier, 1)
	closeConn()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		calls := notifier.snapshot()
		if len(calls) > 0 && calls[len(calls)-1].holder == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("conn-drop never published a final release: %+v", notifier.snapshot())
}

func TestPresenceNotifierRegistrationReplaysExistingClaim(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	b := New(mfWithTelegram())
	defer b.Shutdown()
	topicID := int64(281)
	stub := &Stub{CLI: "codex", PID: 73, CWD: "/workspace/existing", ConnID: 9}
	stub.SetStableSessionID("existing-session")
	if _, ok := b.Routes.Claim(MakeRouteKey("telegram", -100, &topicID), stub); !ok {
		t.Fatal("seed claim failed")
	}
	notifier := &presenceChannel{fakeChannel: &fakeChannel{}}
	if err := b.RegisterChannel(notifier); err != nil {
		t.Fatal(err)
	}
	calls := waitForPresenceCalls(t, notifier, 1)
	if len(calls) != 1 || calls[0].holder == nil || calls[0].holder.CLI != "codex" || calls[0].holder.SessionID != "existing-session" {
		t.Fatalf("registration replay=%+v", calls)
	}
}

func TestPresenceNotifierPanicIsContained(t *testing.T) {
	b, notifier := newPresenceBroker(t)
	defer b.Shutdown()
	notifier.panics = true
	first := MakeRouteKey("telegram", -100, ptrI64Val(281))
	if _, ok := b.Routes.Claim(first, &Stub{CLI: "claude", PID: 5, ConnID: 1}); !ok {
		t.Fatal("claim failed")
	}
	deadline := time.Now().Add(time.Second)
	for !notifier.entered.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !notifier.entered.Load() {
		t.Fatal("panicking notifier was not called")
	}
	second := MakeRouteKey("telegram", -200, ptrI64Val(412))
	if _, ok := b.Routes.Claim(second, &Stub{CLI: "codex", PID: 6, ConnID: 2}); !ok {
		t.Fatal("broker stopped processing claims after notifier panic")
	}
}
