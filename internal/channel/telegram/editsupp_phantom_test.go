package telegram

import (
	"net/http"
	"testing"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// 2026-07-27 incident, Issue B
// (local-notes/INCIDENT-2026-07-27-resume-identity-phantom-edit.md). Voice 6661,
// sent 07-25 and held in the durable queue, was drained by fetch_queue on 07-27
// — correct — and then delivered AGAIN 40 seconds later, because the agent set a
// 👍 on it and Telegram emitted an edited_message for a 62-hour-old message. The
// broker had restarted since, so the suppressor held no baseline; STT re-ran
// (344 chars the first time, 385 the second) and the duplicate read like the
// user had corrected themselves.
//
// Two baseline-free rules close that: age, and C3's own react. What must NOT
// change is that a same-update_id redelivery still re-dispatches, and a real
// edit inside the window still flows.

// suppressed is the boolean form of suppressReason for the baseline-machinery
// unit tests, dated NOW so the age rule — which answers only when no baseline
// applies — never stands in for the behavior under test.
func suppressed(s *editSuppressor, chatID, msgID, updateID int64, fp string) bool {
	return s.suppressReason(chatID, msgID, updateID, fp, time.Now().Unix(), 0) != ""
}

// voiceMsgAged is voiceMsg with an explicit age, for the cases that turn on how
// old the ORIGINAL message is. msg.Date is the original send time and does not
// move when a message is edited, which is exactly what the age rule needs.
func voiceMsgAged(msgID int64, uniqueID string, age time.Duration) *gotgbot.Message {
	m := voiceMsg(msgID, uniqueID)
	m.Date = time.Now().Add(-age).Unix()
	return m
}

func suppressorChannel(h channel.Host) *Channel {
	c := makeChannel(h)
	c.offTrk = newOffsetTracker(800)
	c.msgToUpdate = map[seamKey][]int64{}
	c.editSupp = newEditSuppressor(8192, userEditWindow)
	return c
}

// The incident itself: a reaction on a 62-hour-old message, with no baseline
// (the broker restarted in between). A user cannot have edited it, so the
// edited_message is a phantom no matter what triggered it.
func TestDispatchMessage_EditOlderThanEditWindow_SuppressedWithoutBaseline(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := suppressorChannel(h)

	c.offTrk.Register(801)
	c.dispatchMessage(801, voiceMsgAged(6661, "uniq-6661", 62*time.Hour), true, nil)

	if got := h.emitCount(); got != 0 {
		t.Fatalf("an edited_message for a 62h-old message was delivered (Emit=%d, want 0) — no user edit is possible that far back, so this is a reaction phantom and re-delivering it re-runs STT and reads as a user correction", got)
	}
	// Suppressed = handled: the offset must advance, not wedge on it.
	if got := c.offTrk.Committed(); got != 801 {
		t.Fatalf("a suppressed edit must MarkDone its update; committed=%d, want 801", got)
	}
}

// The window boundary is what makes the rule safe: inside it a user genuinely
// can edit, so an edit with no baseline must still be delivered — the
// restart-amnesia behavior this suppressor has always had.
func TestDispatchMessage_EditInsideEditWindow_StillDeliveredWithoutBaseline(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := suppressorChannel(h)

	c.offTrk.Register(801)
	c.dispatchMessage(801, voiceMsgAged(6662, "uniq-6662", userEditWindow-time.Hour), true, nil)

	if got := h.emitCount(); got != 1 {
		t.Fatalf("an edit inside the %s edit window with no baseline was suppressed (Emit=%d, want 1) — that is a correction the user really could have made, and dropping it destroys it silently", userEditWindow, got)
	}
}

// A message with no stated date must never be aged out. Unknown is not ancient,
// and guessing would drop a real edit on the strength of a missing field.
func TestDispatchMessage_EditWithNoDate_StillDelivered(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := suppressorChannel(h)

	msg := voiceMsg(6663, "uniq-6663")
	msg.Date = 0
	c.offTrk.Register(801)
	c.dispatchMessage(801, msg, true, nil)

	if got := h.emitCount(); got != 1 {
		t.Fatalf("an edit on a message with no date was suppressed (Emit=%d, want 1) — an absent date is unknown age, not infinite age", got)
	}
}

// Even past the window, evidence beats the heuristic: if the deliverable content
// actually CHANGED against a recorded baseline, something really did move and
// the update is delivered. The age rule answers only where there is nothing
// better to go on.
func TestDispatchMessage_OldMessageWithChangedContent_StillDelivered(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := suppressorChannel(h)

	orig := voiceMsgAged(6664, "uniq-6664", 62*time.Hour)
	c.offTrk.Register(801)
	c.dispatchMessage(801, orig, false, nil)
	c.onPersisted(&c3types.Inbound{ChatID: 42, MessageID: 6664})

	changed := voiceMsgAged(6664, "uniq-6664", 62*time.Hour)
	changed.Caption = "now with a caption"
	c.offTrk.Register(802)
	c.dispatchMessage(802, changed, true, nil)

	if got := h.emitCount(); got != 2 {
		t.Fatalf("an edit whose content demonstrably changed was suppressed on age alone (Emit=%d, want 2) — a fingerprint that moved is positive evidence and outranks the age heuristic", got)
	}
}

// PRESERVED: the loss-free Append-retry path. Telegram re-sends the SAME
// update_id after onPersistFailed evicts its dedup entry, and that redelivery
// must genuinely re-dispatch — even though its fingerprint matches the baseline
// the first dispatch recorded.
func TestDispatchMessage_SameUpdateRedelivery_SurvivesTheAgeRule(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := suppressorChannel(h)

	// A real edit inside the window dispatches… and its durable Append FAILS.
	c.offTrk.Register(810)
	c.dispatchMessage(810, voiceMsgAged(6665, "uniq-6665", time.Hour), true, nil)
	if got := h.emitCount(); got != 1 {
		t.Fatalf("first dispatch must emit; got %d", got)
	}
	c.onPersistFailed(&c3types.Inbound{ChatID: 42, MessageID: 6665})

	// Telegram redelivers update 810 (offset held). It must re-dispatch.
	c.dispatchMessage(810, voiceMsgAged(6665, "uniq-6665", time.Hour), true, nil)
	if got := h.emitCount(); got != 2 {
		t.Fatalf("same-update_id redelivery was suppressed (Emit=%d, want 2) — the offset is held for it, so dropping it loses the message permanently", got)
	}
}

// PRESERVED: a real caption edit inside the window still reaches the session.
func TestDispatchMessage_RealEditInsideWindow_StillDelivered(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := suppressorChannel(h)

	c.offTrk.Register(801)
	c.dispatchMessage(801, voiceMsgAged(6666, "uniq-6666", 2*time.Hour), false, nil)
	c.onPersisted(&c3types.Inbound{ChatID: 42, MessageID: 6666})

	edited := voiceMsgAged(6666, "uniq-6666", 2*time.Hour)
	edited.Caption = "the user's correction"
	c.offTrk.Register(802)
	c.dispatchMessage(802, edited, true, nil)

	if got := h.emitCount(); got != 2 {
		t.Fatalf("a real edit inside the window was suppressed (Emit=%d, want 2)", got)
	}
}

// Fix 2 — self-arm on react. C3 reacts to a message whose baseline this process
// never recorded (it predates a restart); the edited_message Telegram sends back
// must be recognized as the reaction echoing, not as new content.
func TestDispatchMessage_ArmedByOwnReact_SuppressesTheEcho(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := suppressorChannel(h)

	c.editSupp.armReact(42, 6667) // what Channel.React does before setMessageReaction

	c.offTrk.Register(801)
	c.dispatchMessage(801, voiceMsgAged(6667, "uniq-6667", 2*time.Hour), true, nil)

	if got := h.emitCount(); got != 0 {
		t.Fatalf("the edited_message echoing C3's own react was delivered (Emit=%d, want 0) — it re-runs STT and surfaces a second, differently-worded transcript of a message the agent had already read", got)
	}
}

// The arm is ONE-SHOT. It explains the single echo the reaction produces; a
// second edit in the same window is the user, and must flow.
func TestDispatchMessage_ArmIsOneShot_SecondEditDelivered(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := suppressorChannel(h)

	c.editSupp.armReact(42, 6668)
	c.offTrk.Register(801)
	c.dispatchMessage(801, voiceMsgAged(6668, "uniq-6668", 2*time.Hour), true, nil)

	c.offTrk.Register(802)
	c.dispatchMessage(802, voiceMsgAged(6668, "uniq-6668", 2*time.Hour), true, nil)

	if got := h.emitCount(); got != 1 {
		t.Fatalf("Emit=%d, want 1: the arm must explain exactly one echo and then stop — a standing suppression would swallow every later edit of that message", got)
	}
}

// An arm goes stale. Past the window it must not explain an edit that arrives
// much later, or one react would mute a message for the rest of the TTL.
func TestEditSuppressor_StaleArmDoesNotSuppress(t *testing.T) {
	s := newEditSuppressor(10, userEditWindow)
	s.armReact(42, 300)

	// Age the arm past its window without sleeping through it.
	s.mu.Lock()
	b := s.index[editKey(42, 300)].Value.(*editBaseline)
	b.armedAt = time.Now().Add(-reactArmWindow - time.Second)
	s.mu.Unlock()

	if why := s.suppressReason(42, 300, 900, "fp-A", time.Now().Unix(), 0); why != "" {
		t.Fatalf("a stale react arm suppressed an edit (%s) — the arm covers the echo of one reaction, not everything that follows it", why)
	}
}

// Arming a message that already HAS a baseline must not throw the fingerprint
// away: it is strictly better evidence than the arm, and losing it would turn a
// precise content check into a blanket time window.
func TestEditSuppressor_ArmRefreshesRatherThanClobbersBaseline(t *testing.T) {
	s := newEditSuppressor(10, userEditWindow)
	s.record(42, 300, 801, "fp-A")
	s.armReact(42, 300)

	// Checked FIRST, while the arm is still live: an arm that discarded the
	// fingerprint would answer this one itself, and consuming itself in the
	// process would then let the assertion below pass on its own.
	if suppressed(s, 42, 300, 802, "fp-B") {
		t.Fatal("arming discarded the fingerprint: changed content is now suppressed by the arm, which would drop a real edit the baseline would have delivered")
	}
	if !suppressed(s, 42, 300, 803, "fp-A") {
		t.Fatal("identical content must still suppress after an arm — the fingerprint is the better evidence and arming must not cost it")
	}
}

// Arming must also keep the entry ALIVE. A baseline near the end of its TTL is
// exactly the one a reaction is about to need an answer for, and letting it
// expire between the react and the echo re-opens the hole this closes.
func TestEditSuppressor_ArmRefreshesTTL(t *testing.T) {
	const ttl = 200 * time.Millisecond
	s := newEditSuppressor(10, ttl)
	s.record(42, 300, 801, "fp-A")

	time.Sleep(150 * time.Millisecond) // most of the way to expiry
	s.armReact(42, 300)
	time.Sleep(150 * time.Millisecond) // past expiry measured from the RECORD

	if !suppressed(s, 42, 300, 802, "fp-A") {
		t.Fatalf("the baseline expired %s after being armed (TTL %s) — arming did not refresh it, so the phantom the react is about to cause arrives with nothing to match", 150*time.Millisecond, ttl)
	}
}

// …and keep it out of the capacity sweep, which evicts oldest-first. An armed
// entry that stays at the front is the next one thrown away.
func TestEditSuppressor_ArmMovesEntryToNewestForCapacityEviction(t *testing.T) {
	s := newEditSuppressor(2, userEditWindow)
	s.record(42, 1, 801, "fp-1")
	s.record(42, 2, 802, "fp-2")

	s.armReact(42, 1)            // msg 1 is now the one we care about
	s.record(42, 3, 803, "fp-3") // over capacity: one must go

	if !suppressed(s, 42, 1, 900, "fp-1") {
		t.Fatal("the armed entry was evicted by the capacity sweep — arming must make it the newest, or a react on a busy chat loses its own baseline immediately")
	}
	if suppressed(s, 42, 2, 900, "fp-2") {
		t.Fatal("capacity eviction dropped the wrong entry: msg 2 was the oldest and should have gone")
	}
}

// Fix 3 (already true, now pinned): the baseline is recorded at DISPATCH time,
// before Emit — so a message that reaches no live session still leaves one
// behind. The incident report guessed baselines were recorded only on live
// dispatch; they are not, and this test keeps it that way.
func TestDispatchMessage_BaselineRecordedEvenWhenNoSessionReceivesIt(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, emitDrops: true}
	c := suppressorChannel(h)

	c.offTrk.Register(801)
	c.dispatchMessage(801, voiceMsg(6669, "uniq-6669"), false, nil)

	h2 := &fakeHost{decision: channel.GateInboundAllow}
	c.host = h2
	c.offTrk.Register(802)
	c.dispatchMessage(802, voiceMsg(6669, "uniq-6669"), true, nil)

	if got := h2.emitCount(); got != 0 {
		t.Fatalf("phantom edit delivered (Emit=%d, want 0) — the original left no baseline because no session received it, so queued and held-reply messages would be unprotected", got)
	}
}

// E1 — the delayed-delivery regression. A user edits at 47h59m while C3 is down;
// Telegram holds the update (up to 24 hours,
// https://core.telegram.org/bots/api#getting-updates) and delivers it later.
// Aged against RECEIPT time the message looks 72 hours old and the correction is
// acknowledged and destroyed. Aged against the two SERVER timestamps it is what
// it actually is: an edit made inside the window.
func TestDispatchMessage_EditInsideWindowDeliveredLate_StillDelivered(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := suppressorChannel(h)

	sent := time.Now().Add(-72 * time.Hour)
	msg := voiceMsg(6680, "uniq-6680")
	msg.Date = sent.Unix()
	msg.EditDate = sent.Add(userEditWindow - time.Minute).Unix() // edited at 47h59m

	c.offTrk.Register(801)
	c.dispatchMessage(801, msg, true, nil)

	if got := h.emitCount(); got != 1 {
		t.Fatalf("an edit made INSIDE the window but delivered after it was suppressed (Emit=%d, want 1) — C3 being offline is not the user editing late, and acknowledging it destroys the correction permanently", got)
	}
}

// The same server-timestamp rule still catches the incident: a reaction phantom
// on a 62-hour-old message dated NOW is 62 hours past its send time.
func TestDispatchMessage_PhantomDatedNowOnAnOldMessage_Suppressed(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := suppressorChannel(h)

	sent := time.Now().Add(-62 * time.Hour)
	msg := voiceMsg(6681, "uniq-6681")
	msg.Date = sent.Unix()
	msg.EditDate = time.Now().Unix() // the reaction's own timestamp

	c.offTrk.Register(801)
	c.dispatchMessage(801, msg, true, nil)

	if got := h.emitCount(); got != 0 {
		t.Fatalf("a phantom edit dated 62h after the message was sent was delivered (Emit=%d, want 0)", got)
	}
}

// editAge unit: which clock answers, and when it refuses to answer.
func TestEditAge_PrefersServerTimestamps(t *testing.T) {
	now := time.Now()
	sent := now.Add(-72 * time.Hour)

	got, ok := editAge(sent.Unix(), sent.Add(time.Hour).Unix(), now)
	if !ok || got.Round(time.Minute) != time.Hour {
		t.Fatalf("editAge with an edit_date = %v (ok=%v), want ~1h measured server-side, not the 72h since it was sent", got, ok)
	}

	got, ok = editAge(sent.Unix(), 0, now)
	if !ok || got.Round(time.Hour) != 72*time.Hour {
		t.Fatalf("editAge with NO edit_date = %v (ok=%v), want the 72h receipt-time fallback", got, ok)
	}

	if _, ok = editAge(0, 0, now); ok {
		t.Fatal("a message with no send date has an UNKNOWN age; answering anyway ages out edits on the strength of a missing field")
	}
}

// T1 — the arm must exist while the outbound call is in flight, not merely by
// the time React returns. The fake Bot API asks the suppressor from inside the
// handler: if arming moved after SetMessageReaction, Telegram's echo could
// arrive before the arm and re-run STT.
func TestReact_ArmsSuppressorBeforeTheOutboundCall(t *testing.T) {
	var c *Channel
	ready := make(chan struct{})
	observed := make(chan string, 1)

	c = getFileChannelHandler(t, func(w http.ResponseWriter, _ *http.Request) {
		<-ready // c is fully assigned before the handler reads it
		observed <- c.editSupp.suppressReason(42, 6670, 900, "fp-A", time.Now().Unix(), 0)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	})
	c.editSupp = newEditSuppressor(8192, userEditWindow)
	c.rate = newRateLimiter()
	close(ready)

	if err := c.React(c3types.ReactArgs{ChatID: 42, MessageID: 6670, Emoji: "\U0001F44D"}); err != nil {
		t.Fatalf("React: %v", err)
	}
	if why := <-observed; why == "" {
		t.Fatal("the suppressor was NOT armed while setMessageReaction was in flight — Telegram's echo can be polled before the arm exists, and the message is re-delivered with a fresh transcript")
	}
}

// E2 — a reaction Telegram REJECTED produces no echo, so the arm it installed
// must be withdrawn. Left standing it is spent on the next genuine edit instead,
// dropping a real correction to pay for a reaction that never happened.
func TestReact_FailedReactionWithdrawsTheArm(t *testing.T) {
	c := getFileChannel(t, `{"ok":false,"error_code":400,"description":"Bad Request: REACTION_INVALID"}`)
	c.editSupp = newEditSuppressor(8192, userEditWindow)
	c.rate = newRateLimiter()

	if err := c.React(c3types.ReactArgs{ChatID: 42, MessageID: 6671, Emoji: "\U0001F44D"}); err == nil {
		t.Fatal("the fake API rejected the reaction; React reported success")
	}
	if why := c.editSupp.suppressReason(42, 6671, 900, "fp-A", time.Now().Unix(), 0); why != "" {
		t.Fatalf("a failed reaction left a blanket suppression arm (%s) — no echo is coming, so the arm can only be spent on a genuine edit", why)
	}
}

// The rollback is generation-scoped: a late withdrawal from a FAILED react must
// not cancel the arm a later, successful react installed.
func TestEditSuppressor_DisarmDoesNotCancelANewerArm(t *testing.T) {
	s := newEditSuppressor(10, userEditWindow)
	stale := s.armReact(42, 300)
	s.armReact(42, 300) // a second react, still in flight

	s.disarmReact(42, 300, stale) // the first one's failure rolls back late

	if why := s.suppressReason(42, 300, 900, "fp", time.Now().Unix(), 0); why == "" {
		t.Fatal("a late rollback from the first react cancelled the SECOND react's arm — its echo is now re-delivered with a fresh transcript")
	}
}

// …and it withdraws only the ARM. A fingerprint on the same entry was recorded
// by a real dispatch and says nothing about the reaction.
func TestEditSuppressor_DisarmKeepsTheFingerprint(t *testing.T) {
	s := newEditSuppressor(10, userEditWindow)
	s.record(42, 300, 801, "fp-A")
	gen := s.armReact(42, 300)

	s.disarmReact(42, 300, gen)

	if !suppressed(s, 42, 300, 802, "fp-A") {
		t.Fatal("rolling back a failed reaction destroyed the content baseline — every later phantom edit of that message is now re-delivered")
	}
}
