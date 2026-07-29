package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
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
}
