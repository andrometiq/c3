package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// pairingTestBroker returns a broker with an empty allowlist + an
// XDG_CONFIG_HOME pointing at a writable temp dir so SaveMappings (called
// on a successful pair) doesn't try to write into the user's real config.
func pairingTestBroker(t *testing.T) *Broker {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	// Pre-create the c3 subdir so mappings.Write's atomic-rename path
	// (which writes a tempfile in the parent directory) succeeds.
	if err := os.MkdirAll(filepath.Join(dir, "c3"), 0700); err != nil {
		t.Fatal(err)
	}
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]mappings.ChannelConfig{"telegram": {BotToken: "x"}},
		Mappings:      map[string]mappings.Mapping{},
	}
	return New(mf)
}

func TestPairing_GenerateCode_FourDigits(t *testing.T) {
	for i := 0; i < 50; i++ {
		c, err := generateCode()
		if err != nil {
			t.Fatal(err)
		}
		if len(c) != 4 {
			t.Errorf("generated code %q is not 4 chars", c)
		}
		for _, r := range c {
			if r < '0' || r > '9' {
				t.Errorf("code %q contains non-digit %q", c, r)
			}
		}
	}
}

func TestPairing_StartDM_SetsCodeAndTTL(t *testing.T) {
	b := pairingTestBroker(t)
	code, err := b.Pairing.StartDM()
	if err != nil {
		t.Fatal(err)
	}
	if len(code) != 4 {
		t.Errorf("code = %q", code)
	}
	w := b.Pairing.DMWindow()
	if w == nil {
		t.Fatal("DMWindow should be active right after StartDM")
	}
	if w.Code != code {
		t.Errorf("DMWindow.Code = %q, want %q", w.Code, code)
	}
	if w.Target != PairTargetDM {
		t.Errorf("DMWindow.Target = %v, want PairTargetDM", w.Target)
	}
	if !w.IsActive(time.Now()) {
		t.Errorf("window should be active")
	}
}

func TestPairing_TTLExpiry(t *testing.T) {
	b := pairingTestBroker(t)
	// Inject a clock so we don't sleep in tests.
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	b.Pairing.now = func() time.Time { return now }

	if _, err := b.Pairing.StartDM(); err != nil {
		t.Fatal(err)
	}
	if b.Pairing.DMWindow() == nil {
		t.Fatal("window should be active at t=0")
	}
	// Advance 9 minutes — still active.
	now = now.Add(9 * time.Minute)
	if b.Pairing.DMWindow() == nil {
		t.Fatal("window should still be active at t=9min")
	}
	// Advance to 10:01 — expired.
	now = now.Add(2 * time.Minute)
	if b.Pairing.DMWindow() != nil {
		t.Fatal("window should be expired after 10-min TTL")
	}
}

func TestPairing_GroupWindowsIndependent(t *testing.T) {
	b := pairingTestBroker(t)
	codeA, err := b.Pairing.StartGroup(-100)
	if err != nil {
		t.Fatal(err)
	}
	codeB, err := b.Pairing.StartGroup(-200)
	if err != nil {
		t.Fatal(err)
	}
	if codeA == codeB {
		// 1-in-10000 chance — re-roll once before failing to avoid flake.
		codeB, _ = b.Pairing.StartGroup(-200)
	}
	wA := b.Pairing.GroupWindow(-100)
	wB := b.Pairing.GroupWindow(-200)
	if wA == nil || wB == nil {
		t.Fatal("both group windows should be active")
	}
	if wA.ChatID != -100 || wB.ChatID != -200 {
		t.Errorf("group windows lost chat_id binding")
	}
	// Clearing one must not affect the other.
	b.Pairing.ClearGroup(-100)
	if b.Pairing.GroupWindow(-100) != nil {
		t.Errorf("group -100 should be cleared")
	}
	if b.Pairing.GroupWindow(-200) == nil {
		t.Errorf("group -200 should still be active")
	}
}

func TestPairing_DM_SeparateFromGroup(t *testing.T) {
	b := pairingTestBroker(t)
	if _, err := b.Pairing.StartDM(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Pairing.StartGroup(-100); err != nil {
		t.Fatal(err)
	}
	b.Pairing.ClearDM()
	if b.Pairing.DMWindow() != nil {
		t.Errorf("DM should be cleared")
	}
	if b.Pairing.GroupWindow(-100) == nil {
		t.Errorf("group window must survive DM clear (separate pairing per spec)")
	}
}

func TestGate_AllowsAllowlistedUser(t *testing.T) {
	b := pairingTestBroker(t)
	b.mutateMappings(func(mf *mappings.MappingsFile) { mf.AddAllowedUser(42) })

	in := &c3types.Inbound{
		Channel: "telegram",
		ChatID:  42, // positive = DM; Telegram private chat_id == user_id
		Sender:  c3types.Sender{UserID: 42},
		Text:    "hello",
	}
	if got := b.Gate(in); got != GateAllow {
		t.Errorf("allowlisted user: got %v, want GateAllow", got)
	}
}

func TestGate_AllowsAllowlistedGroup(t *testing.T) {
	b := pairingTestBroker(t)
	b.mutateMappings(func(mf *mappings.MappingsFile) { mf.AddAllowedGroup(-1009123456789) })

	in := &c3types.Inbound{
		Channel: "telegram",
		ChatID:  -1009123456789,             // negative = group/supergroup
		Sender:  c3types.Sender{UserID: 99}, // any member
		Text:    "hello team",
	}
	if got := b.Gate(in); got != GateAllow {
		t.Errorf("allowlisted group: got %v, want GateAllow", got)
	}
}

func TestGate_DropsStrangerByDefault(t *testing.T) {
	b := pairingTestBroker(t)
	in := &c3types.Inbound{
		Channel: "telegram",
		ChatID:  77,
		Sender:  c3types.Sender{UserID: 77},
		Text:    "hi",
	}
	if got := b.Gate(in); got != GateDrop {
		t.Errorf("non-allowlisted DM: got %v, want GateDrop", got)
	}
}

func TestGate_WrongCodeDuringPairing_DropsAndKeepsWindow(t *testing.T) {
	b := pairingTestBroker(t)
	code, err := b.Pairing.StartDM()
	if err != nil {
		t.Fatal(err)
	}
	wrong := "0000"
	if wrong == code {
		wrong = "1234" // 1-in-10000 flake guard
	}
	in := &c3types.Inbound{
		Channel: "telegram",
		ChatID:  42,
		Sender:  c3types.Sender{UserID: 42},
		Text:    wrong,
	}
	if got := b.Gate(in); got != GateDrop {
		t.Errorf("wrong code: got %v, want GateDrop", got)
	}
	// A single wrong code must not burn the window — the operator mistyping once
	// should not force a fresh /c3:pair. (Sustained guessing does close it; see
	// TestGate_WrongCodes_ExhaustWindow.)
	if b.Pairing.DMWindow() == nil {
		t.Errorf("window should still be active after a single wrong-code attempt")
	}
	// User must NOT have been added.
	if b.Mappings().IsUserAllowed(42) {
		t.Errorf("wrong code must not allowlist the sender")
	}
}

func TestGate_RightCodeDuringPairing_AllowlistsAndConsumes(t *testing.T) {
	b := pairingTestBroker(t)
	code, err := b.Pairing.StartDM()
	if err != nil {
		t.Fatal(err)
	}
	in := &c3types.Inbound{
		Channel: "telegram",
		ChatID:  42,
		Sender:  c3types.Sender{UserID: 42},
		Text:    code,
	}
	if got := b.Gate(in); got != GatePairConsumed {
		t.Errorf("right code: got %v, want GatePairConsumed", got)
	}
	if !b.Mappings().IsUserAllowed(42) {
		t.Errorf("right code must allowlist sender")
	}
	if b.Pairing.DMWindow() != nil {
		t.Errorf("pairing window must clear after successful match")
	}
	// Subsequent same-user messages now allowed.
	in2 := &c3types.Inbound{
		Channel: "telegram", ChatID: 42, Sender: c3types.Sender{UserID: 42}, Text: "real msg",
	}
	if got := b.Gate(in2); got != GateAllow {
		t.Errorf("post-pair: got %v, want GateAllow", got)
	}
}

func TestGate_GroupPairing_AllowlistsGroupNotUser(t *testing.T) {
	b := pairingTestBroker(t)
	chatID := int64(-1009123456789)
	code, err := b.Pairing.StartGroup(chatID)
	if err != nil {
		t.Fatal(err)
	}
	in := &c3types.Inbound{
		Channel: "telegram",
		ChatID:  chatID,
		Sender:  c3types.Sender{UserID: 12345}, // some member; should be incidental
		Text:    code,
	}
	if got := b.Gate(in); got != GatePairConsumed {
		t.Fatalf("right code in group: got %v, want GatePairConsumed", got)
	}
	if !b.Mappings().IsGroupAllowed(chatID) {
		t.Errorf("group chat_id must be allowlisted")
	}
	// Critical: the user_id who typed the code must NOT be allowlisted —
	// we trust the group, not the individual member.
	if b.Mappings().IsUserAllowed(12345) {
		t.Errorf("group pairing must NOT allowlist the typist's user_id (spec: trust the group)")
	}
	if b.Pairing.GroupWindow(chatID) != nil {
		t.Errorf("group window must clear after match")
	}
}

func TestGate_GroupPairingCodeFromWrongChat_DropsOnUnallowlistedChat(t *testing.T) {
	b := pairingTestBroker(t)
	// Pair window armed for chat A. Identical text body arrives in chat B.
	code, err := b.Pairing.StartGroup(-100)
	if err != nil {
		t.Fatal(err)
	}
	// Random other group, no pairing armed; even if body matches A's code
	// it must not promote chat B.
	in := &c3types.Inbound{
		Channel: "telegram",
		ChatID:  -200,
		Sender:  c3types.Sender{UserID: 1},
		Text:    code,
	}
	if got := b.Gate(in); got != GateDrop {
		t.Errorf("code in wrong group: got %v, want GateDrop", got)
	}
	if b.Mappings().IsGroupAllowed(-200) {
		t.Errorf("wrong-group must not be allowlisted")
	}
	// Original pairing window for chat A still active.
	if b.Pairing.GroupWindow(-100) == nil {
		t.Errorf("chat A's window must remain after a wrong-chat probe")
	}
}

func TestGate_DropsNonDigitBody(t *testing.T) {
	b := pairingTestBroker(t)
	// Pair armed; body is text not 4-digit.
	if _, err := b.Pairing.StartDM(); err != nil {
		t.Fatal(err)
	}
	in := &c3types.Inbound{
		Channel: "telegram", ChatID: 42,
		Sender: c3types.Sender{UserID: 42},
		Text:   "5829 please", // looks like a code but has extra text
	}
	if got := b.Gate(in); got != GateDrop {
		t.Errorf("non-strict body: got %v, want GateDrop (no whitespace, no extras)", got)
	}
}

func TestAutoStartDMPairingIfEmpty(t *testing.T) {
	b := pairingTestBroker(t)
	code := b.AutoStartDMPairingIfEmpty()
	if code == "" {
		t.Fatal("auto-start should fire when allowlist is empty")
	}
	if b.Pairing.DMWindow() == nil {
		t.Errorf("auto-start must arm a DM window")
	}
}

func TestAutoStartDMPairing_SkipsWhenUsersPresent(t *testing.T) {
	b := pairingTestBroker(t)
	b.mutateMappings(func(mf *mappings.MappingsFile) { mf.AddAllowedUser(1) })
	if code := b.AutoStartDMPairingIfEmpty(); code != "" {
		t.Errorf("should not auto-arm when users already allowlisted")
	}
	if b.Pairing.DMWindow() != nil {
		t.Errorf("no window should be armed")
	}
}

func TestHandlePairModeStart_DM(t *testing.T) {
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]mappings.ChannelConfig{},
		Mappings:      map[string]mappings.Mapping{},
	}
	peer, done := runHandlerWithPeer(t, mf)
	defer done()

	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "c3-broker-cli", PID: 1, CWD: "/x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}

	if err := peer.WriteJSON(ipc.PairModeStartReq{Op: ipc.OpPairModeStart, Target: "dm"}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.PairModeReplyMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("IPC pair_mode_start failed: %s", resp.Err)
	}
	if len(resp.Code) != 4 {
		t.Errorf("code = %q, want 4 digits", resp.Code)
	}
	if resp.Target != "dm" {
		t.Errorf("target echoed = %q", resp.Target)
	}
	if resp.TTLSec != int(PairTTL.Seconds()) {
		t.Errorf("TTL = %d, want %d", resp.TTLSec, int(PairTTL.Seconds()))
	}
}

func TestHandlePairModeStart_GroupRequiresChatID(t *testing.T) {
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]mappings.ChannelConfig{},
		Mappings:      map[string]mappings.Mapping{},
	}
	peer, done := runHandlerWithPeer(t, mf)
	defer done()

	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "c3-broker-cli", PID: 1, CWD: "/x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := peer.WriteJSON(ipc.PairModeStartReq{Op: ipc.OpPairModeStart, Target: "group"}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.PairModeReplyMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.OK {
		t.Errorf("group pair without chat_id should fail")
	}
	if resp.Err == "" {
		t.Errorf("expected error message")
	}
}

func TestGate_NoWorkerInvocation_WhenDropped(t *testing.T) {
	// Default-deny posture: a non-allowlisted inbound must NOT submit to
	// the worker pool. We assert via the host wrapper (BrokerHost.Emit
	// is what channels call to push inbound). After a GateInbound returns
	// Drop, the channel layer doesn't call Emit at all. Cross-check: the
	// pool has zero active workers after gating.
	b := pairingTestBroker(t)
	in := &c3types.Inbound{
		Channel: "telegram",
		ChatID:  99, // not allowlisted
		Sender:  c3types.Sender{UserID: 99},
		Text:    "stranger inbound",
	}
	if got := b.Gate(in); got != GateDrop {
		t.Fatalf("got %v, want GateDrop", got)
	}
	if active := b.Workers.Active(); active != 0 {
		t.Errorf("worker pool has %d active workers after dropped inbound — gate did not prevent dispatch", active)
	}
}

// v0.1.0 release audit: wrong codes used to be dropped silently while leaving the
// window live, so the 10^4 space was exhaustible inside one 10-minute window — and
// a successful guess writes the sender into allowlist.Users, which is exactly the
// operator set allowed to tap Allow on a relayed tool-use prompt. A window must
// now close after pairMaxWrongCodes attempts.
func TestGate_WrongCodes_ExhaustWindow(t *testing.T) {
	b := pairingTestBroker(t)
	code, err := b.Pairing.StartDM()
	if err != nil {
		t.Fatal(err)
	}
	wrong := "0000"
	if wrong == code {
		wrong = "1234"
	}
	guess := func() GateDecision {
		return b.Gate(&c3types.Inbound{
			Channel: "telegram", ChatID: 42,
			Sender: c3types.Sender{UserID: 42}, Text: wrong,
		})
	}

	for i := 1; i < pairMaxWrongCodes; i++ {
		if got := guess(); got != GateDrop {
			t.Fatalf("attempt %d: got %v, want GateDrop", i, got)
		}
		if b.Pairing.DMWindow() == nil {
			t.Fatalf("window closed early, after %d of %d attempts", i, pairMaxWrongCodes)
		}
	}

	// The pairMaxWrongCodes'th wrong code closes it.
	if got := guess(); got != GateDrop {
		t.Fatalf("final attempt: got %v, want GateDrop", got)
	}
	if b.Pairing.DMWindow() != nil {
		t.Errorf("BRUTE FORCE: the window survived %d wrong codes; the 10^4 code space is exhaustible within one %v window", pairMaxWrongCodes, PairTTL)
	}

	// And the now-closed window must not accept even the CORRECT code.
	got := b.Gate(&c3types.Inbound{
		Channel: "telegram", ChatID: 42,
		Sender: c3types.Sender{UserID: 42}, Text: code,
	})
	if got == GatePairConsumed {
		t.Errorf("a closed window must not pair; got %v", got)
	}
	if b.Mappings().IsUserAllowed(42) {
		t.Errorf("no sender may be allowlisted through an exhausted window")
	}
}

// A group window is limited independently of the DM window.
func TestGate_WrongCodes_ExhaustGroupWindowIndependently(t *testing.T) {
	b := pairingTestBroker(t)
	if _, err := b.Pairing.StartDM(); err != nil {
		t.Fatal(err)
	}
	const gid = int64(-100200300)
	gcode, err := b.Pairing.StartGroup(gid)
	if err != nil {
		t.Fatal(err)
	}
	wrong := "0000"
	if wrong == gcode {
		wrong = "1234"
	}
	for i := 0; i < pairMaxWrongCodes; i++ {
		b.Gate(&c3types.Inbound{
			Channel: "telegram", ChatID: gid,
			Sender: c3types.Sender{UserID: 77}, Text: wrong,
		})
	}
	if b.Pairing.GroupWindow(gid) != nil {
		t.Errorf("group window survived %d wrong codes", pairMaxWrongCodes)
	}
	if b.Pairing.DMWindow() == nil {
		t.Errorf("exhausting a GROUP window must not close the DM window")
	}
}

// A guess is evaluated against one specific pairing window. If an operator
// restarts pairing between the lookup and the attempt record/consume, traffic
// from the old window must not increment, close, or consume the replacement.
func TestPairing_OldWindowCannotMutateReplacement(t *testing.T) {
	p := newPairingState()
	if _, err := p.StartDM(); err != nil {
		t.Fatal(err)
	}
	oldDM := p.DMWindow()
	if _, err := p.StartDM(); err != nil {
		t.Fatal(err)
	}
	newDM := p.DMWindow()

	if _, exhausted := p.RecordWrongDM(oldDM); exhausted {
		t.Fatal("an attempt against the old DM window exhausted the replacement")
	}
	if newDM.Wrong != 0 {
		t.Fatalf("old-window attempt incremented replacement DM window: Wrong=%d", newDM.Wrong)
	}
	if p.ConsumeDM(oldDM) {
		t.Fatal("an old matching DM window consumed the replacement")
	}
	if p.DMWindow() != newDM {
		t.Fatal("replacement DM window was cleared")
	}

	const gid = int64(-100200300)
	if _, err := p.StartGroup(gid); err != nil {
		t.Fatal(err)
	}
	oldGroup := p.GroupWindow(gid)
	if _, err := p.StartGroup(gid); err != nil {
		t.Fatal(err)
	}
	newGroup := p.GroupWindow(gid)

	if _, exhausted := p.RecordWrongGroup(gid, oldGroup); exhausted {
		t.Fatal("an attempt against the old group window exhausted the replacement")
	}
	if newGroup.Wrong != 0 {
		t.Fatalf("old-window attempt incremented replacement group window: Wrong=%d", newGroup.Wrong)
	}
	if p.ConsumeGroup(gid, oldGroup) {
		t.Fatal("an old matching group window consumed the replacement")
	}
	if p.GroupWindow(gid) != newGroup {
		t.Fatal("replacement group window was cleared")
	}
}
