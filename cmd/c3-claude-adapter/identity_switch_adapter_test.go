package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	c3broker "github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
)

func writeIdentityHandoff(t *testing.T, key, stableID string, unixNano int64) {
	t.Helper()
	if err := sessionhandoff.Write(key, sessionhandoff.Entry{
		StableSessionID: stableID,
		CWD:             "/projects/" + stableID,
		UnixNano:        unixNano,
	}); err != nil {
		t.Fatalf("write handoff %q -> %q: %v", key, stableID, err)
	}
}

// TestResolveTerminalHandoff is T8's resolver coverage. It pins the chain walk,
// strict monotonic guard, cycle stop, depth cap, and unchanged single-hop case.
func TestResolveTerminalHandoff(t *testing.T) {
	t.Run("walks to terminal entry", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		writeIdentityHandoff(t, "spawn", "session-a", 10)
		writeIdentityHandoff(t, "session-a", "session-b", 20)
		writeIdentityHandoff(t, "session-b", "session-c", 30)

		got, ok := resolveTerminalHandoff("spawn")
		if !ok || got.StableSessionID != "session-c" || got.UnixNano != 30 {
			t.Fatalf("resolver did not walk the complete handoff chain: ok=%v entry=%+v", ok, got)
		}
	})

	t.Run("rejects stale link", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		writeIdentityHandoff(t, "spawn", "session-current", 20)
		writeIdentityHandoff(t, "session-current", "session-stale", 20)
		writeIdentityHandoff(t, "session-stale", "session-wrong", 30)

		got, ok := resolveTerminalHandoff("spawn")
		if !ok || got.StableSessionID != "session-current" || got.UnixNano != 20 {
			t.Fatalf("resolver followed a non-newer stale handoff link: ok=%v entry=%+v", ok, got)
		}
	})

	t.Run("stops on cycle", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		writeIdentityHandoff(t, "spawn", "session-a", 10)
		writeIdentityHandoff(t, "session-a", "session-b", 20)
		writeIdentityHandoff(t, "session-b", "session-a", 30)

		got, ok := resolveTerminalHandoff("spawn")
		if !ok || got.StableSessionID != "session-a" || got.UnixNano != 30 {
			t.Fatalf("resolver did not stop at the newest accepted entry before a cycle: ok=%v entry=%+v", ok, got)
		}
	})

	t.Run("stops at depth cap", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		writeIdentityHandoff(t, "spawn", "session-00", 1)
		for i := 0; i <= terminalHandoffDepthCap; i++ {
			writeIdentityHandoff(t,
				fmt.Sprintf("session-%02d", i),
				fmt.Sprintf("session-%02d", i+1),
				int64(i+2),
			)
		}

		got, ok := resolveTerminalHandoff("spawn")
		want := fmt.Sprintf("session-%02d", terminalHandoffDepthCap)
		if !ok || got.StableSessionID != want {
			t.Fatalf("resolver exceeded depth cap %d: ok=%v stable=%q want=%q", terminalHandoffDepthCap, ok, got.StableSessionID, want)
		}
	})

	t.Run("single hop unchanged", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		writeIdentityHandoff(t, "spawn", "only-session", 10)

		got, ok := resolveTerminalHandoff("spawn")
		if !ok || got.StableSessionID != "only-session" || got.CWD != "/projects/only-session" {
			t.Fatalf("single-hop handoff behavior changed: ok=%v entry=%+v", ok, got)
		}
	})
}

func establishSettledIdentity(a *adapter, entry sessionhandoff.Entry) {
	a.recoverStarted.Store(true)
	a.recoverFired.Store(true)
	a.setCurrentStableIdentity(entry)
	a.markIdentitySettled()
}

func nextIdentitySwitchRecover(t *testing.T, broker *recoveryBroker) ipc.RecoverSessionReq {
	t.Helper()
	select {
	case raw := <-broker.frames:
		op, err := ipc.PeekOp(raw)
		if err != nil {
			t.Fatalf("switch emitted an unparseable broker frame: %v", err)
		}
		if op != ipc.OpRecoverSession {
			t.Fatalf("identity switch dispatched %q before re-resolution; want %q first", op, ipc.OpRecoverSession)
		}
		var req ipc.RecoverSessionReq
		if err := json.Unmarshal(raw, &req); err != nil {
			t.Fatalf("decode switch recover: %v", err)
		}
		return req
	case <-time.After(5 * time.Second):
		t.Fatal("identity switch did not refire recovery")
		return ipc.RecoverSessionReq{}
	}
}

func waitForIdentityGate(t *testing.T, gate chan struct{}, defect string) {
	t.Helper()
	select {
	case <-gate:
	case <-time.After(5 * time.Second):
		t.Fatal(defect)
	}
}

type reconnectSwitchChannel struct {
	mu        sync.Mutex
	validated []int64
	replies   []c3types.ReplyArgs
}

func (*reconnectSwitchChannel) Name() string { return "telegram" }
func (*reconnectSwitchChannel) Start(context.Context, channel.Host) error {
	return nil
}
func (*reconnectSwitchChannel) Stop() error { return nil }
func (*reconnectSwitchChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{Channel: "telegram"}
}
func (c *reconnectSwitchChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	c.mu.Lock()
	c.replies = append(c.replies, args)
	c.mu.Unlock()
	return 1, nil
}
func (*reconnectSwitchChannel) SendTyping(int64, *int64) error { return nil }
func (*reconnectSwitchChannel) EditMessage(args c3types.EditArgs) (*c3types.EditResult, error) {
	return &c3types.EditResult{MessageID: args.MessageID}, nil
}
func (*reconnectSwitchChannel) React(c3types.ReactArgs) error             { return nil }
func (*reconnectSwitchChannel) DownloadAttachment(string) (string, error) { return "", nil }
func (*reconnectSwitchChannel) StopPoll(int64, int64) (*c3types.PollResult, error) {
	return &c3types.PollResult{}, nil
}
func (*reconnectSwitchChannel) CreateTopic(int64, string) (int64, error) { return 0, nil }
func (c *reconnectSwitchChannel) ValidateTopic(_ int64, topicID int64) error {
	c.mu.Lock()
	c.validated = append(c.validated, topicID)
	c.mu.Unlock()
	return nil
}

func (c *reconnectSwitchChannel) validatedTopic(topicID int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, got := range c.validated {
		if got == topicID {
			return true
		}
	}
	return false
}

func (c *reconnectSwitchChannel) replyCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.replies)
}

func reconnectSwitchMappings() *mappings.MappingsFile {
	enabled := true
	mf := &mappings.MappingsFile{
		SchemaVersion:      1,
		AutoAttachOnResume: &enabled,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {
				DefaultGroup: "main",
				Groups: map[string]mappings.GroupConfig{
					"main": {ChatID: -100},
					"work": {ChatID: -200},
				},
				Topics: []mappings.Topic{
					{ChatID: -100, TopicID: 281, Name: "topic-a", Group: "main"},
					{ChatID: -200, TopicID: 412, Name: "topic-b", Group: "work"},
				},
			},
		},
		Mappings: map[string]mappings.Mapping{},
	}
	now := time.Now().UTC()
	topicA := int64(281)
	mf.UpsertSessionAttachment("conversation-a", mappings.SessionAttachment{
		Channel: "telegram", ChatID: -100, TopicID: &topicA,
		Name: "topic-a", Group: "main", LastAttachedAt: now,
	})
	topicB := int64(412)
	mf.UpsertSessionAttachment("conversation-b", mappings.SessionAttachment{
		Channel: "telegram", ChatID: -200, TopicID: &topicB,
		Name: "topic-b", Group: "work", LastAttachedAt: now,
	})
	return mf
}

func wireReconnectTestBroker(t *testing.T, a *adapter, b *c3broker.Broker) {
	t.Helper()
	a.connectBrokerFn = func() error {
		adapterSide, brokerSide := net.Pipe()
		a.bmu.Lock()
		a.conn = ipc.NewConn(adapterSide)
		a.bmu.Unlock()
		go b.HandleConn(brokerSide)
		return nil
	}
}

func waitForStableIdentity(t *testing.T, a *adapter, stableID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if current, settled := a.currentStableIdentity(); settled && current.StableSessionID == stableID {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	current, settled := a.currentStableIdentity()
	t.Fatalf("reconnect did not register terminal identity %q: settled=%v current=%+v", stableID, settled, current)
}

func requireReconnectSwitchIntegrity(t *testing.T, b *c3broker.Broker) {
	t.Helper()
	attachmentB, ok := b.Mappings().LookupSessionAttachment("conversation-b")
	if !ok || attachmentB.TopicID == nil || *attachmentB.TopicID != 412 {
		t.Fatalf("reconnect replay corrupted conversation B's attachment with conversation A's topic: got %+v ok=%v", attachmentB, ok)
	}
	attachmentA, ok := b.Mappings().LookupSessionAttachment("conversation-a")
	if !ok || attachmentA.TopicID == nil || *attachmentA.TopicID != 281 || attachmentA.Detached {
		t.Fatalf("reconnect switch damaged conversation A's independent attachment record: got %+v ok=%v", attachmentA, ok)
	}

	topicA := int64(281)
	keyA := c3broker.MakeRouteKey("telegram", -100, &topicA)
	topicB := int64(412)
	keyB := c3broker.MakeRouteKey("telegram", -200, &topicB)
	if _, held := b.Routes.Holder(keyA); held {
		t.Fatal("reconnect identity switch left conversation A's stale replay claim alive on the fresh broker")
	}
	if holder, held := b.Routes.Holder(keyB); !held || holder.StableSessionIDValue() != "conversation-b" {
		t.Fatalf("fresh broker did not recover conversation B's own topic: held=%v holder=%+v", held, holder)
	}
}

func waitForReconnectClaim(t *testing.T, b *c3broker.Broker, key c3broker.RouteKey, stableID, defect string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if holder, held := b.Routes.Holder(key); held && holder.StableSessionIDValue() == stableID {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	holder, held := b.Routes.Holder(key)
	t.Fatalf("%s: held=%v holder=%+v", defect, held, holder)
}

func requireNoReconnectResumeNotice(t *testing.T, a *adapter, ch *reconnectSwitchChannel, defect string) {
	t.Helper()
	// Both broker and adapter notices are dispatched asynchronously after the
	// recover response. Give a wrongly-taken recovery arm time to surface them.
	time.Sleep(100 * time.Millisecond)
	if text, ok := a.takePendingRecoverNotice(); ok {
		t.Fatalf("%s: adapter queued %q", defect, text)
	}
	if got := ch.replyCount(); got != 0 {
		t.Fatalf("%s: broker posted %d channel reply/replies", defect, got)
	}
}

// T15 runs ordinary broker restarts through recoverBroker and the real broker
// handler. Replay is the restoration operation; it must not be reclassified as
// an identity switch merely because hello created an identity-empty fresh stub.
func TestRecoverBroker_OrdinaryRestartReplayKeepsClaimWithoutResumeNotice(t *testing.T) {
	tests := []struct {
		name       string
		handoff    bool
		autoAttach bool
	}{
		{name: "no_handoff", autoAttach: true},
		{name: "same_identity_handoff_auto_attach_enabled", handoff: true, autoAttach: true},
		{name: "same_identity_handoff_auto_attach_disabled", handoff: true, autoAttach: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("XDG_STATE_HOME", t.TempDir())
			t.Setenv("XDG_CONFIG_HOME", t.TempDir())
			t.Setenv("C3_QUEUE_DIR", t.TempDir())
			t.Setenv("CLAUDE_CODE_SESSION_ID", "")
			if tc.handoff {
				t.Setenv("CLAUDE_CODE_SESSION_ID", "spawn")
				writeIdentityHandoff(t, "spawn", "conversation-a", 10)
			}

			mf := reconnectSwitchMappings()
			mf.AutoAttachOnResume = &tc.autoAttach
			freshBroker := c3broker.New(mf)
			t.Cleanup(freshBroker.Shutdown)
			testChannel := &reconnectSwitchChannel{}
			if err := freshBroker.RegisterChannel(testChannel); err != nil {
				t.Fatalf("register T15 channel: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			a := newAdapter()
			a.runCtx = ctx
			wireReconnectTestBroker(t, a, freshBroker)
			t.Cleanup(func() {
				cancel()
				if conn := a.currentConn(); conn != nil {
					_ = conn.Close()
				}
			})

			stamp := ""
			wantBrokerID := ""
			if tc.handoff {
				entry := sessionhandoff.Entry{
					StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
				}
				establishSettledIdentity(a, entry)
				stamp = entry.StableSessionID
				wantBrokerID = entry.StableSessionID
			}
			topicA := int64(281)
			a.rememberAttachForIdentity(rememberedIdentityReq("/projects/a", -100, &topicA, "main"), stamp)
			a.setAttachedTopic("topic-a")

			if !a.recoverBroker(ctx) {
				t.Fatal("T15 recoverBroker aborted before reconnecting to the fresh broker")
			}
			go a.brokerReader(ctx)

			keyA := c3broker.MakeRouteKey("telegram", -100, &topicA)
			waitForReconnectClaim(t, freshBroker, keyA, wantBrokerID,
				"T15 ordinary restart lost the replayed route claim or failed to register the same identity")
			if tc.handoff {
				// Seeing the broker-side stable id proves the async refire wrote
				// its request while holding recoverMu. Wait for that same lock
				// to be released so this subtest cannot leak a timeout read into
				// the next test's recoverRespTimeout override under -race.
				a.recoverMu.Lock()
				a.recoverMu.Unlock()
			}
			attachmentA, ok := freshBroker.Mappings().LookupSessionAttachment("conversation-a")
			if !ok || attachmentA.TopicID == nil || *attachmentA.TopicID != 281 || attachmentA.Detached {
				t.Fatalf("T15 ordinary restart damaged the session attachment record: got %+v ok=%v", attachmentA, ok)
			}
			requireNoReconnectResumeNotice(t, a, testChannel,
				"T15 ordinary restart took the identity-switch/recovery arm and emitted a spurious resumed notice")
		})
	}
}

// T16 reproduces D4 end to end: attach succeeded before identity, so its stamp
// is empty; the handoff then settles that same conversation before a broker
// restart. Reconnect must replay the route and re-register the stable identity.
func TestRecoverBroker_AttachBeforeIdentityReplayAndReregisters(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "spawn")

	disabled := false
	mf := reconnectSwitchMappings()
	mf.AutoAttachOnResume = &disabled
	delete(mf.SessionAttachments, "conversation-a")
	freshBroker := c3broker.New(mf)
	t.Cleanup(freshBroker.Shutdown)
	testChannel := &reconnectSwitchChannel{}
	if err := freshBroker.RegisterChannel(testChannel); err != nil {
		t.Fatalf("register T16 channel: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	a := newAdapter()
	a.runCtx = ctx
	wireReconnectTestBroker(t, a, freshBroker)
	t.Cleanup(func() {
		cancel()
		if conn := a.currentConn(); conn != nil {
			_ = conn.Close()
		}
	})

	topicA := int64(281)
	a.rememberAttachForIdentity(rememberedIdentityReq("/projects/a", -100, &topicA, "main"), "")
	a.setAttachedTopic("topic-a")
	writeIdentityHandoff(t, "spawn", "conversation-a", 10)
	establishSettledIdentity(a, sessionhandoff.Entry{
		StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
	})

	if !a.recoverBroker(ctx) {
		t.Fatal("T16 recoverBroker aborted before reconnecting to the fresh broker")
	}
	go a.brokerReader(ctx)

	keyA := c3broker.MakeRouteKey("telegram", -100, &topicA)
	waitForReconnectClaim(t, freshBroker, keyA, "conversation-a",
		"T16 attach-before-identity reconnect performed no usable replay/re-registration; the route or broker stable id is missing")
	a.recoverMu.Lock()
	a.recoverMu.Unlock()
	attachmentA, ok := freshBroker.Mappings().LookupSessionAttachment("conversation-a")
	if !ok || attachmentA.TopicID == nil || *attachmentA.TopicID != 281 || attachmentA.Detached {
		t.Fatalf("T16 fresh broker did not record the attach-before-identity route under the settled stable id: got %+v ok=%v", attachmentA, ok)
	}
	requireNoReconnectResumeNotice(t, a, testChannel,
		"T16 matching attach-before-identity replay was misclassified as a switch/recovery")
}

// T12 reproduces the broker-restart race from F1 end to end through the real
// recoverBroker entry point. This pins the production call-site ordering, not
// only restoreSessionAfterReconnect in isolation.
func TestRecoverBroker_IdentitySwitchSkipsStaleReplayAndPreservesAttachments(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "spawn")
	writeIdentityHandoff(t, "spawn", "conversation-a", 10)
	writeIdentityHandoff(t, "conversation-a", "conversation-b", 20)

	freshBroker := c3broker.New(reconnectSwitchMappings())
	t.Cleanup(freshBroker.Shutdown)
	testChannel := &reconnectSwitchChannel{}
	if err := freshBroker.RegisterChannel(testChannel); err != nil {
		t.Fatalf("register reconnect test channel: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a := newAdapter()
	a.runCtx = ctx
	wireReconnectTestBroker(t, a, freshBroker)
	t.Cleanup(func() {
		if conn := a.currentConn(); conn != nil {
			_ = conn.Close()
		}
	})

	establishSettledIdentity(a, sessionhandoff.Entry{
		StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
	})
	topicA := int64(281)
	a.rememberAttach(rememberedIdentityReq("/projects/a", -100, &topicA, "main"))
	a.setAttachedTopic("topic-a")

	if !a.recoverBroker(ctx) {
		t.Fatal("recoverBroker aborted before reconnecting to the fresh broker")
	}
	go a.brokerReader(ctx)
	waitForStableIdentity(t, a, "conversation-b")
	requireReconnectSwitchIntegrity(t, freshBroker)
	if testChannel.validatedTopic(281) {
		t.Fatal("recoverBroker replayed conversation A before resolving terminal identity B; production reconnect ordering regressed")
	}
}

// T13 drives the real fireRecoverLocked response-timeout exit, then attaches A
// while currentStableID is still empty. After the handoff advances to B and the
// broker reconnects, A's stamped replay must not be restored or recorded under
// B.
func TestRecoverBroker_UnsettledTimeoutDoesNotReplayPreviousConversation(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "spawn")
	writeIdentityHandoff(t, "spawn", "conversation-a", 10)

	oldTimeout := recoverRespTimeout
	recoverRespTimeout = 25 * time.Millisecond
	t.Cleanup(func() { recoverRespTimeout = oldTimeout })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a := newAdapter()
	a.runCtx = ctx
	stalledBroker := newRecoveryBroker(t, a)
	a.recoverStarted.Store(true)
	outcomeCh := make(chan recoverOutcome, 1)
	go func() {
		outcomeCh <- a.fireRecover(ctx, sessionhandoff.Entry{
			StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
		})
	}()
	if op, ok := stalledBroker.nextOp(t, 5*time.Second); !ok || op != ipc.OpRecoverSession {
		t.Fatalf("T13 precondition: timeout recovery never reached the broker: op=%q ok=%v", op, ok)
	}
	select {
	case outcome := <-outcomeCh:
		if !outcome.attempted || outcome.registered {
			t.Fatalf("T13 precondition: response-timeout path did not leave identity attempted-but-unregistered: %+v", outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("T13 precondition: fireRecoverLocked did not take the response-timeout exit")
	}
	if _, settled := a.currentStableIdentity(); settled {
		t.Fatal("T13 precondition: timeout unexpectedly registered currentStableID")
	}

	topicA := int64(281)
	a.rememberAttach(rememberedIdentityReq("/projects/a", -100, &topicA, "main"))
	a.setAttachedTopic("topic-a")
	a.amu.Lock()
	stamp := a.lastAttachStableID
	a.amu.Unlock()
	if stamp != "conversation-a" {
		t.Fatalf("T13 precondition: attach made after failed recovery was stamped %q, want conversation-a", stamp)
	}
	writeIdentityHandoff(t, "conversation-a", "conversation-b", 20)

	freshBroker := c3broker.New(reconnectSwitchMappings())
	t.Cleanup(freshBroker.Shutdown)
	testChannel := &reconnectSwitchChannel{}
	if err := freshBroker.RegisterChannel(testChannel); err != nil {
		t.Fatalf("register reconnect test channel: %v", err)
	}
	wireReconnectTestBroker(t, a, freshBroker)
	if !a.recoverBroker(ctx) {
		t.Fatal("T13 recoverBroker aborted before reconnecting to the fresh broker")
	}
	go a.brokerReader(ctx)
	waitForStableIdentity(t, a, "conversation-b")
	requireReconnectSwitchIntegrity(t, freshBroker)
	if testChannel.validatedTopic(281) {
		t.Fatal("T13 corruption guard failed: unsettled reconnect dispatched conversation A's stale replay while terminal identity was B")
	}
}

// N2: restoreSessionAfterReconnect is called on brokerReader. Even when an
// older recover owns recoverMu, restore must return so that same reader can
// drain and dispatch the response that releases the lock holder.
func TestReconnectIdentitySwitch_DoesNotBlockBrokerReaderDispatch(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "spawn")
	writeIdentityHandoff(t, "spawn", "conversation-a", 10)
	writeIdentityHandoff(t, "conversation-a", "conversation-b", 20)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a := newAdapter()
	a.runCtx = ctx
	establishSettledIdentity(a, sessionhandoff.Entry{
		StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
	})
	a.rememberAttach(ipc.AttachReq{Op: ipc.OpAttach, Name: "topic-a"})
	a.setAttachedTopic("topic-a")

	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() {
		_ = adapterSide.Close()
		_ = brokerSide.Close()
	})
	a.bmu.Lock()
	a.conn = ipc.NewConn(adapterSide)
	a.bmu.Unlock()
	peer := ipc.NewConn(brokerSide)

	response := make(chan ipc.RecoverSessionResp, 1)
	a.rsmu.Lock()
	a.rsPending = response
	a.rsmu.Unlock()

	a.recoverMu.Lock() // an older round-trip is waiting for brokerReader
	locked := true
	t.Cleanup(func() {
		if locked {
			a.recoverMu.Unlock()
		}
	})

	readerErr := make(chan error, 1)
	go func() {
		a.restoreSessionAfterReconnect(ctx)
		raw, err := a.currentConn().ReadFrame()
		if err == nil {
			a.dispatchRecoverSessionResult(raw)
		}
		readerErr <- err
	}()
	go func() {
		_ = peer.WriteJSON(ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult})
	}()

	select {
	case <-response:
		// Dispatch stayed live while the switch goroutine waited on recoverMu.
	case <-time.After(300 * time.Millisecond):
		t.Fatal("reconnect identity switch blocked brokerReader on recoverMu; it could not dispatch the response that releases the in-flight recovery")
	}
	select {
	case err := <-readerErr:
		if err != nil {
			t.Fatalf("brokerReader liveness probe failed to read the response: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("restoreSessionAfterReconnect did not return control to brokerReader while a switch was in flight")
	}

	a.recoverMu.Unlock()
	locked = false
}

// N4 pins §3d2: reconnect demotes recoverFired from once-per-process to
// once-per-connection. N14 pins both same-identity inputs: a resolved handoff
// takes the second restore arm, while a missing handoff plus settled identity
// takes the third. Either way, the fresh broker must learn the stable identity.
func TestReconnectSameIdentity_ResetsRecoverFiredAndReregisters(t *testing.T) {
	tests := []struct {
		name         string
		writeHandoff bool
	}{
		{name: "resolved_handoff", writeHandoff: true},
		{name: "settled_identity_without_handoff", writeHandoff: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("XDG_STATE_HOME", t.TempDir())
			t.Setenv("CLAUDE_CODE_SESSION_ID", "spawn")
			if tt.writeHandoff {
				writeIdentityHandoff(t, "spawn", "conversation-a", 10)
			}
			if _, found := reconnectTerminalHandoff(); found != tt.writeHandoff {
				t.Fatalf("N14 precondition: terminal handoff found=%v, want %v", found, tt.writeHandoff)
			}

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			a := newAdapter()
			a.runCtx = ctx
			broker := newRecoveryBroker(t, a)
			establishSettledIdentity(a, sessionhandoff.Entry{
				StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
			})

			a.restoreSessionAfterReconnect(ctx)
			select {
			case raw := <-broker.frames:
				var req ipc.RecoverSessionReq
				if err := json.Unmarshal(raw, &req); err != nil || req.Op != ipc.OpRecoverSession || req.StableSessionID != "conversation-a" {
					t.Fatalf("same-identity reconnect sent the wrong re-registration frame: req=%+v err=%v", req, err)
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("same-identity reconnect did not reset recoverFired; the fresh broker never received this session's stable identity")
			}
			answerRecover(t, a)
		})
	}
}

func TestReconnectRefire_ReresolvesBeforeAsyncDispatch(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "spawn")
	writeIdentityHandoff(t, "spawn", "conversation-b", 20)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a := newAdapter()
	a.runCtx = ctx
	broker := newRecoveryBroker(t, a)
	establishSettledIdentity(a, sessionhandoff.Entry{
		StableSessionID: "conversation-b", CWD: "/projects/b", UnixNano: 20,
	})

	// Simulate restoreSessionAfterReconnect having captured A immediately before
	// a newer SessionStart hook advanced the terminal identity to B.
	a.refireResolvedHandoffOnReconnect(ctx, sessionhandoff.Entry{
		StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
	})
	select {
	case raw := <-broker.frames:
		var req ipc.RecoverSessionReq
		if err := json.Unmarshal(raw, &req); err != nil {
			t.Fatalf("decode reconnect re-registration: %v", err)
		}
		if req.StableSessionID != "conversation-b" {
			t.Fatalf("async reconnect refire used stale pre-goroutine identity %q and would switch the session backward from B to A", req.StableSessionID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("async reconnect refire did not re-register the latest terminal identity")
	}
	answerRecover(t, a)
}

func TestReconnectSupersededSwitchFallsBackToReregistration(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "spawn")
	writeIdentityHandoff(t, "spawn", "conversation-b", 20)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a := newAdapter()
	a.runCtx = ctx
	broker := newRecoveryBroker(t, a)
	establishSettledIdentity(a, sessionhandoff.Entry{
		StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
	})
	a.rememberAttachForIdentity(ipc.AttachReq{Op: ipc.OpAttach, Name: "topic-a"}, "conversation-a")

	// Hold switch execution after restore has decided A→B, then model a
	// concurrent hook/recovery settling B before that scheduled switch runs.
	// The switch correctly bails, but reconnect must still register B with the
	// fresh broker instead of returning after doing neither operation.
	a.recoverMu.Lock()
	a.restoreSessionAfterReconnect(ctx)
	a.setCurrentStableIdentity(sessionhandoff.Entry{
		StableSessionID: "conversation-b", CWD: "/projects/b", UnixNano: 20,
	})
	a.recoverMu.Unlock()

	select {
	case raw := <-broker.frames:
		var req ipc.RecoverSessionReq
		if err := json.Unmarshal(raw, &req); err != nil {
			t.Fatalf("decode superseded-switch fallback registration: %v", err)
		}
		if req.Op != ipc.OpRecoverSession || req.StableSessionID != "conversation-b" {
			t.Fatalf("superseded reconnect switch re-registered the wrong identity: %+v", req)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("superseded reconnect switch performed neither replay nor re-registration; the fresh broker never learned conversation-b")
	}
	answerRecover(t, a)
}

// T9: the process-lifetime watch detects a later hook, opens a fresh identity
// epoch, and refires recovery with the terminal conversation id.
func TestSwitchWatch_ReopensGateAndRefires(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	writeIdentityHandoff(t, "conversation-a", "conversation-b", 20)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := newAdapter()
	a.runCtx = ctx
	broker := newRecoveryBroker(t, a)
	establishSettledIdentity(a, sessionhandoff.Entry{
		StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
	})
	a.setAttachedTopic("old-topic")
	a.rememberAttach(ipc.AttachReq{Op: ipc.OpAttach, Name: "old-topic"})

	oldGate := a.identityGate()
	oldEpoch := a.identityEpoch()
	go a.watchForIdentitySwitch(ctx, 5*time.Millisecond)

	req := nextIdentitySwitchRecover(t, broker)
	if req.StableSessionID != "conversation-b" {
		t.Fatalf("switch watch refired the non-terminal identity %q, want conversation-b", req.StableSessionID)
	}
	newGate := a.identityGate()
	if newGate == oldGate || a.identityEpoch() != oldEpoch+1 {
		t.Fatalf("identity switch did not reopen an epoch gate: gate_reused=%v epoch=%d want=%d", newGate == oldGate, a.identityEpoch(), oldEpoch+1)
	}
	select {
	case <-newGate:
		t.Fatal("identity switch's new gate was already settled before the second RecoverSession response")
	default:
	}

	answerRecover(t, a)
	waitForIdentityGate(t, newGate, "identity switch recovery finished but the reopened gate never settled")
	current, ok := a.currentStableIdentity()
	if !ok || current.StableSessionID != "conversation-b" {
		t.Fatalf("broker registered conversation-b but adapter current identity stayed stale: ok=%v entry=%+v", ok, current)
	}
}

// T10: a direct attach path performs the switch probe before awaiting identity.
// Detection reopens the gate; the attach cannot reach the broker until the new
// RecoverSession response settles it.
func TestToolsCall_SwitchCheckRunsBeforeIdentityGate(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	writeIdentityHandoff(t, "conversation-a", "conversation-b", 20)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := newAdapter()
	a.runCtx = ctx
	broker := newRecoveryBroker(t, a)
	establishSettledIdentity(a, sessionhandoff.Entry{
		StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
	})
	a.setAttachedTopic("old-topic")

	attachDone := make(chan struct{})
	attachReq := newAttachReq(t, map[string]any{"name": "new-topic"})
	go func() {
		defer close(attachDone)
		_, _ = a.toolAttach(ctx, attachReq)
	}()

	req := nextIdentitySwitchRecover(t, broker)
	if req.StableSessionID != "conversation-b" {
		t.Fatalf("attach-triggered switch recovered %q, want conversation-b", req.StableSessionID)
	}
	select {
	case raw := <-broker.frames:
		op, _ := ipc.PeekOp(raw)
		t.Fatalf("attach reached the broker as %q before switch recovery settled; the switch check ran after the identity gate", op)
	case <-time.After(250 * time.Millisecond):
	}

	answerRecover(t, a)
	if op, ok := broker.nextOp(t, 5*time.Second); !ok || op != ipc.OpAttach {
		t.Fatalf("attach did not dispatch after the new identity settled: op=%q ok=%v", op, ok)
	}
	drivePendingAttached(t, a, ipc.AttachedMsg{Op: ipc.OpAttached, OK: false, Err: "test"})
	select {
	case <-attachDone:
	case <-time.After(5 * time.Second):
		t.Fatal("attach never returned after its post-switch broker response")
	}
}

// T11: deferred banners are stamped with their identity epoch. Reopening drops
// the old "Re-attached" text, and an unattached switch installs the exact
// corrective notice in the new epoch.
func TestPendingRecoverNotice_DroppedOnEpochChange(t *testing.T) {
	t.Run("stale notice is rejected by epoch", func(t *testing.T) {
		a := newAdapter()
		a.setPendingRecoverNotice(`📨 Re-attached to "old-topic"`)
		oldEpoch := a.identityEpoch()
		a.reopenIdentity()

		if text, ok := a.takePendingRecoverNotice(); ok {
			t.Fatalf("stale recover notice crossed an identity epoch and would render in the new conversation: %q", text)
		}
		if a.identityEpoch() != oldEpoch+1 {
			t.Fatalf("identity gate remained settle-once: epoch=%d want=%d", a.identityEpoch(), oldEpoch+1)
		}
	})

	t.Run("unattached switch replaces stale notice", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		writeIdentityHandoff(t, "conversation-a", "conversation-b", 20)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		a := newAdapter()
		a.runCtx = ctx
		broker := newRecoveryBroker(t, a)
		establishSettledIdentity(a, sessionhandoff.Entry{
			StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
		})
		a.setAttachedTopic("old-topic")
		a.rememberAttach(ipc.AttachReq{Op: ipc.OpAttach, Name: "old-topic"})
		a.setPendingRecoverNotice(`📨 Re-attached to "old-topic"`)

		a.checkForIdentitySwitch(ctx)
		req := nextIdentitySwitchRecover(t, broker)
		if req.StableSessionID != "conversation-b" {
			t.Fatalf("notice test recovered %q, want conversation-b", req.StableSessionID)
		}
		newGate := a.identityGate()
		answerRecover(t, a)
		waitForIdentityGate(t, newGate, "unattached switch did not settle its identity epoch")

		text, ok := a.takePendingRecoverNotice()
		want := `session switched conversations — released topic "old-topic"; this conversation is not attached. Use attach to claim a topic.`
		if !ok || text != want {
			t.Fatalf("unattached switch did not replace the stale banner with the corrective notice: ok=%v text=%q", ok, text)
		}
		if strings.Contains(text, "Re-attached") {
			t.Fatalf("stale Re-attached banner survived into the new conversation: %q", text)
		}
		a.amu.Lock()
		lastAttach := a.lastAttach
		topic := a.attachedTopic
		a.amu.Unlock()
		if lastAttach != nil || topic != "" {
			t.Fatalf("identity switch retained old conversation state: lastAttach=%+v attachedTopic=%q", lastAttach, topic)
		}
	})

	t.Run("failed switch recovery still leaves corrective notice", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		writeIdentityHandoff(t, "conversation-a", "conversation-b", 20)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		a := newAdapter() // deliberately no broker connection: switch write fails
		a.runCtx = ctx
		establishSettledIdentity(a, sessionhandoff.Entry{
			StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
		})
		a.setAttachedTopic("old-topic")

		a.checkForIdentitySwitch(ctx)
		newGate := a.identityGate()
		waitForIdentityGate(t, newGate, "failed switch recovery wedged the reopened identity gate")

		text, ok := a.takePendingRecoverNotice()
		want := `session switched conversations — topic "old-topic" state is unknown; this session is not attached. Use attach to claim a topic.`
		if !ok || text != want {
			t.Fatalf("failed switch recovery left the now-unattached session without its corrective notice: ok=%v text=%q", ok, text)
		}
		if strings.Contains(text, "released") {
			t.Fatalf("failed switch recovery claimed a broker-side release that never happened: %q", text)
		}
	})

	t.Run("unknown old topic does not render empty quotes", func(t *testing.T) {
		text := renderIdentitySwitchUnattachedNotice("", true)
		if strings.Contains(text, `topic ""`) {
			t.Fatalf("corrective notice rendered an empty old-topic name: %q", text)
		}
		if !strings.Contains(text, "previous topic was released") {
			t.Fatalf("corrective notice did not explain the unnamed previous topic release: %q", text)
		}
	})
}
