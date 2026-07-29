package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
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

type reconnectSwitchChannel struct{}

func (*reconnectSwitchChannel) Name() string { return "telegram" }
func (*reconnectSwitchChannel) Start(context.Context, channel.Host) error {
	return nil
}
func (*reconnectSwitchChannel) Stop() error { return nil }
func (*reconnectSwitchChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{Channel: "telegram"}
}
func (*reconnectSwitchChannel) SendReply(c3types.ReplyArgs) (int64, error) { return 1, nil }
func (*reconnectSwitchChannel) SendTyping(int64, *int64) error             { return nil }
func (*reconnectSwitchChannel) EditMessage(args c3types.EditArgs) (*c3types.EditResult, error) {
	return &c3types.EditResult{MessageID: args.MessageID}, nil
}
func (*reconnectSwitchChannel) React(c3types.ReactArgs) error             { return nil }
func (*reconnectSwitchChannel) DownloadAttachment(string) (string, error) { return "", nil }
func (*reconnectSwitchChannel) StopPoll(int64, int64) (*c3types.PollResult, error) {
	return &c3types.PollResult{}, nil
}
func (*reconnectSwitchChannel) CreateTopic(int64, string) (int64, error) { return 0, nil }
func (*reconnectSwitchChannel) ValidateTopic(int64, int64) error         { return nil }

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

// T12 reproduces the broker-restart race from F1 end to end. The adapter holds
// A's replay request, but the handoff already resolves to B when it connects to
// a fresh broker. B must recover its own attachment without A ever being
// replayed onto the identity-empty fresh stub.
func TestRecoverBroker_IdentitySwitchSkipsStaleReplayAndPreservesAttachments(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "spawn")
	writeIdentityHandoff(t, "spawn", "conversation-a", 10)
	writeIdentityHandoff(t, "conversation-a", "conversation-b", 20)

	freshBroker := c3broker.New(reconnectSwitchMappings())
	t.Cleanup(freshBroker.Shutdown)
	if err := freshBroker.RegisterChannel(&reconnectSwitchChannel{}); err != nil {
		t.Fatalf("register reconnect test channel: %v", err)
	}

	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() {
		_ = adapterSide.Close()
		_ = brokerSide.Close()
	})
	go freshBroker.HandleConn(brokerSide)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a := newAdapter()
	a.runCtx = ctx
	a.bmu.Lock()
	a.conn = ipc.NewConn(adapterSide)
	a.bmu.Unlock()
	if err := a.hello(); err != nil {
		t.Fatalf("fresh broker hello: %v", err)
	}
	go a.brokerReader(ctx)

	establishSettledIdentity(a, sessionhandoff.Entry{
		StableSessionID: "conversation-a", CWD: "/projects/a", UnixNano: 10,
	})
	topicA := int64(281)
	a.rememberAttach(rememberedIdentityReq("/projects/a", -100, &topicA, "main"))
	a.setAttachedTopic("topic-a")

	a.restoreSessionAfterReconnect(ctx)
	newGate := a.identityGate()
	waitForIdentityGate(t, newGate, "reconnect switch recovery did not settle")

	attachmentB, ok := freshBroker.Mappings().LookupSessionAttachment("conversation-b")
	if !ok || attachmentB.TopicID == nil || *attachmentB.TopicID != 412 {
		t.Fatalf("reconnect replay corrupted conversation B's attachment with conversation A's topic: got %+v ok=%v", attachmentB, ok)
	}
	attachmentA, ok := freshBroker.Mappings().LookupSessionAttachment("conversation-a")
	if !ok || attachmentA.TopicID == nil || *attachmentA.TopicID != 281 || attachmentA.Detached {
		t.Fatalf("reconnect switch damaged conversation A's independent attachment record: got %+v ok=%v", attachmentA, ok)
	}

	keyA := c3broker.MakeRouteKey("telegram", -100, &topicA)
	topicB := int64(412)
	keyB := c3broker.MakeRouteKey("telegram", -200, &topicB)
	if _, held := freshBroker.Routes.Holder(keyA); held {
		t.Fatal("reconnect identity switch left conversation A's stale replay claim alive on the fresh broker")
	}
	if holder, held := freshBroker.Routes.Holder(keyB); !held || holder.StableSessionIDValue() != "conversation-b" {
		t.Fatalf("fresh broker did not recover conversation B's own topic: held=%v holder=%+v", held, holder)
	}
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
		want := `session switched conversations — released topic "old-topic" (it belonged to the previous conversation); this conversation is not attached. Use attach to claim a topic.`
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
		want := `session switched conversations — released topic "old-topic" (it belonged to the previous conversation); this conversation is not attached. Use attach to claim a topic.`
		if !ok || text != want {
			t.Fatalf("failed switch recovery left the now-unattached session without its corrective notice: ok=%v text=%q", ok, text)
		}
	})

	t.Run("unknown old topic does not render empty quotes", func(t *testing.T) {
		text := renderIdentitySwitchUnattachedNotice("")
		if strings.Contains(text, `topic ""`) {
			t.Fatalf("corrective notice rendered an empty old-topic name: %q", text)
		}
		if !strings.Contains(text, "previous conversation's topic was released") {
			t.Fatalf("corrective notice did not explain the unnamed previous topic release: %q", text)
		}
	})
}
