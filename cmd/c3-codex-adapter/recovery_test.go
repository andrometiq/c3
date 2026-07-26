package main

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// TestRecoverBroker_CtxCancelStopsLoop guards the D1 (adapter-ipc-2) reconnect-
// forever loop's exit condition: it must be ctx-aware and return promptly
// (false) when ctx is cancelled, instead of spinning forever. The previous
// one-shot brokerReader had no such loop at all; the new loop MUST honor
// cancellation so a clean shutdown isn't blocked by a perpetual reconnect.
//
// We pre-cancel ctx so the loop's first guard (ctx.Err()) trips before it ever
// dials — this never spawns or contacts a real broker.
func TestRecoverBroker_CtxCancelStopsLoop(t *testing.T) {
	a := newAdapter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	done := make(chan bool, 1)
	go func() { done <- a.recoverBroker(ctx) }()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("recoverBroker must return false when ctx is cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recoverBroker did not honor ctx cancellation (spun forever?)")
	}
}

// NOTE: an end-to-end "retry then cancel mid-backoff" test is deliberately
// omitted. recoverBroker → reconnectBroker → connectBroker dials the real
// broker socket (path probes /run/user/$UID independent of env) and would
// either connect to the LIVE broker or spawn a fresh one — both forbidden in a
// unit test (recovery-hardening constraint: don't touch the live broker). The
// ctx-aware loop exit is proven by TestRecoverBroker_CtxCancelStopsLoop; the
// retry/replay/advisory sub-behaviors are proven by the focused tests below.
// The reconnect machinery itself (reconnectBroker/connectBroker) is the same
// code the Claude adapter has run in production since the reconnect-forever
// upgrade.

// NOTE: the former TestAutoAttach_DoesNotRegisterPendingSlot was removed with
// the startup auto-attach itself (spec §2 Phase 1). A Codex session no longer
// silently binds a cwd-inferred topic name at startup — it attaches explicitly,
// and a bare attach surfaces a picker. rememberAttach/replayLastAttach stay for
// the `attach`-tool → broker-restart replay path (covered below).

// TestResolvedAttachReq_Parity pins the §3d1 + item C substitution on the Codex
// side (DORMANT — a Codex bare attach never returns OK post-Phase-1 — but wired
// for symmetry): an explicit request is remembered verbatim, a bare resolution
// substitutes the resolved identity id-addressed ({TopicID,Group} for a topic,
// {Target:"dm"} for the DM) — never {Name,Group}, which a fresh broker can't
// re-claim across groups.
func TestResolvedAttachReq_Parity(t *testing.T) {
	explicit := ipc.AttachReq{Op: ipc.OpAttach, CWD: "/p", Name: "c3"}
	if got := resolvedAttachReq(explicit, ipc.AttachedMsg{OK: true, Name: "c3"}); got != explicit {
		t.Fatalf("explicit must be verbatim: %+v", got)
	}
	tid := int64(281)
	bare := ipc.AttachReq{Op: ipc.OpAttach, CWD: "/p"}
	if got := resolvedAttachReq(bare, ipc.AttachedMsg{OK: true, Name: "c3", Group: "g", ChatID: -99, TopicID: &tid}); got.TopicID == nil || *got.TopicID != 281 || got.Group != "g" || got.ChatID != -99 || got.Name != "" || got.Target != "" {
		t.Fatalf("bare→topic: %+v (want id-addressed {TopicID:281 Group:g ChatID:-99})", got)
	}
	if got := resolvedAttachReq(bare, ipc.AttachedMsg{OK: true, Name: "dm"}); got.Target != "dm" || got.Name != "" {
		t.Fatalf("bare→DM: %+v", got)
	}
}

// TestRememberAttach_SanitizesSteal (item D): parity with the Claude adapter — a
// one-shot user-confirmed steal must be cleared in the remembered copy so a
// reconnect replay never silently force-evicts the current holder.
func TestRememberAttach_SanitizesSteal(t *testing.T) {
	a := newAdapter()
	tid := int64(281)
	a.rememberAttach(ipc.AttachReq{Op: ipc.OpAttach, Name: "c3", TopicID: &tid, Steal: true})

	a.amu.Lock()
	got := a.lastAttach
	a.amu.Unlock()
	if got == nil {
		t.Fatal("rememberAttach stored nothing")
	}
	if got.Steal {
		t.Fatalf("remembered attach must clear Steal (one-shot, not standing): %+v", got)
	}
	if got.Name != "c3" || got.TopicID == nil || *got.TopicID != 281 {
		t.Fatalf("rememberAttach dropped non-Steal fields: %+v", got)
	}
}

// TestReplayLastAttach_SendsReplayFrame covers the D3 (adapter-ipc-3) replay:
// after a reconnect, the remembered attach is re-sent with Replay=true so the
// route claim is re-established silently (no welcome message).
func TestReplayLastAttach_SendsReplayFrame(t *testing.T) {
	a := newAdapter()
	a.rememberAttach(ipc.AttachReq{Op: ipc.OpAttach, Name: "proj", CWD: "/x"})

	cliEnd, brokerEnd := net.Pipe()
	defer cliEnd.Close()
	defer brokerEnd.Close()
	a.bmu.Lock()
	a.conn = ipc.NewConn(cliEnd)
	a.bmu.Unlock()

	read := make(chan []byte, 1)
	go func() {
		bc := ipc.NewConn(brokerEnd)
		raw, _ := bc.ReadFrame()
		read <- raw
	}()

	a.replayLastAttach()

	select {
	case raw := <-read:
		var req ipc.AttachReq
		if err := json.Unmarshal(raw, &req); err != nil {
			t.Fatalf("replay frame unmarshal: %v", err)
		}
		if !req.Replay {
			t.Error("replayed attach must set Replay=true (suppress welcome)")
		}
		if req.Name != "proj" {
			t.Errorf("replayed attach lost fields: %+v", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replay frame")
	}
}

// TestReplayLastAttach_NoopWhenNeverAttached: a session that never attached must
// not send anything on reconnect.
func TestReplayLastAttach_NoopWhenNeverAttached(t *testing.T) {
	a := newAdapter()
	cliEnd, brokerEnd := net.Pipe()
	defer cliEnd.Close()
	defer brokerEnd.Close()
	a.bmu.Lock()
	a.conn = ipc.NewConn(cliEnd)
	a.bmu.Unlock()

	wrote := make(chan struct{}, 1)
	go func() {
		bc := ipc.NewConn(brokerEnd)
		if _, err := bc.ReadFrame(); err == nil {
			wrote <- struct{}{}
		}
	}()

	a.replayLastAttach()

	select {
	case <-wrote:
		t.Fatal("replayLastAttach must be a no-op when nothing was ever attached")
	case <-time.After(200 * time.Millisecond):
		// good — nothing written
	}
}

// TestInboundContentSummary_CapturesContent guards D4 (adapter-ipc-4) for the
// Codex adapter: the notify-FAIL log summary must include actual content so a
// dropped notify is recoverable from adapter.log. Mirrors the Claude adapter's
// test of the same name.
func TestInboundContentSummary_CapturesContent(t *testing.T) {
	in := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -100,
		MessageID: 7,
		Text:      "must not be lost",
		Sender:    c3types.Sender{Username: "alice", UserID: 42},
		Attachments: []c3types.Attachment{
			{Kind: "document", Size: 1234},
		},
	}
	got := inboundContentSummary(in)
	for _, want := range []string{"from=@alice", `text="must not be lost"`, "attach=document/1234"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary missing %q; got %q", want, got)
		}
	}
	if got := inboundContentSummary(&c3types.Inbound{}); got != "(no content)" {
		t.Errorf("empty inbound summary: want (no content); got %q", got)
	}
}

// TestAdviseBrokerDown_OneShotAndRearm guards the D5 (adapter-ipc-5) loud
// advisory: it surfaces exactly once per outage (no spam across retry cycles)
// and re-arms after a successful reconnect so a later outage advises again.
//
// The in-memory inbox ring is RETIRED (the broker's durable queue is the source
// of truth), and a broker-DOWN advisory can't be durably queued anyway — the
// broker is exactly what's unreachable — so it rides ONLY the best-effort notify
// frame. The one-shot/rearm contract is therefore asserted on the
// brokerDownAdvised flag (the mechanism that suppresses spam), the only
// observable state. An unconnected logNotify transport returns an error on
// Notify, which adviseBrokerDown logs and tolerates; the flag bookkeeping still
// runs, which is what we assert.
func TestAdviseBrokerDown_OneShotAndRearm(t *testing.T) {
	a := newAdapter()
	a.transport = newLogNotifyTransport(nil)

	if a.brokerDownAdvised.Load() {
		t.Fatal("brokerDownAdvised must start cleared")
	}

	a.adviseBrokerDown(6)
	if !a.brokerDownAdvised.Load() {
		t.Fatal("adviseBrokerDown must set the one-shot flag on first outage")
	}
	// Subsequent calls in the SAME outage must be suppressed (no re-arm, flag
	// stays set). The CompareAndSwap in adviseBrokerDown is the guard; these
	// calls must be no-ops that leave the flag set.
	a.adviseBrokerDown(7)
	a.adviseBrokerDown(8)
	if !a.brokerDownAdvised.Load() {
		t.Fatal("flag must remain set across repeated outage cycles (one-shot)")
	}

	// After a reconnect clears the flag, a new outage must be able to advise
	// again (re-arm).
	a.clearBrokerDownAdvisory()
	if a.brokerDownAdvised.Load() {
		t.Fatal("clearBrokerDownAdvisory must clear the one-shot flag on reconnect")
	}
	a.adviseBrokerDown(6)
	if !a.brokerDownAdvised.Load() {
		t.Fatal("a fresh outage after clear must re-advise (set the flag again)")
	}
}
