package telegram

import (
	"testing"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// These tests pin the phantom-edit suppression contract (2026-07-19 report:
// the agent's own 👍 react made Telegram emit a spurious edited_message for a
// reacted-to voice message seconds later — Bot API documents edited_message
// "may at times be triggered by changes to message fields that are either
// unavailable or not actively used by your bot" — and C3 re-ran STT and
// delivered the same transcription twice).

func voiceMsg(msgID int64, uniqueID string) *gotgbot.Message {
	return &gotgbot.Message{
		MessageId: msgID,
		From:      &gotgbot.User{Id: 42},
		Chat:      gotgbot.Chat{Id: 42},
		Date:      1715151931,
		Voice: &gotgbot.Voice{
			FileId:       "file-" + uniqueID,
			FileUniqueId: uniqueID,
			Duration:     3,
		},
	}
}

// A phantom edit — new update_id, deliverable content byte-identical — must be
// dropped (single Emit) and must mark its update done so the offset advances.
func TestDispatchMessage_PhantomEditSuppressed(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, emitDrops: false}
	c := makeChannel(h)
	c.offTrk = newOffsetTracker(800)
	c.msgToUpdate = map[int64][]int64{}
	c.editSupp = newEditSuppressor(8192, 48*time.Hour)

	// Original voice message, update 801: emitted, persisted.
	c.offTrk.Register(801)
	c.dispatchMessage(801, voiceMsg(300, "uniq-A"), false, nil)
	if got := h.emitCount(); got != 1 {
		t.Fatalf("original must emit once; got %d", got)
	}
	c.onPersisted(&c3types.Inbound{MessageID: 300})
	if got := c.offTrk.Committed(); got != 801 {
		t.Fatalf("original persisted; committed=%d, want 801", got)
	}

	// Reaction-triggered phantom edit: NEW update 802, same content.
	c.offTrk.Register(802)
	c.dispatchMessage(802, voiceMsg(300, "uniq-A"), true, nil)

	if got := h.emitCount(); got != 1 {
		t.Fatalf("phantom edit re-delivered: Emit called %d times, want 1 (suppressed)", got)
	}
	// Suppressed = handled: the offset must advance past 802, not wedge.
	if got := c.offTrk.Committed(); got != 802 {
		t.Fatalf("suppressed edit must MarkDone its update; committed=%d, want 802", got)
	}
	// No seam entry may be staged for a suppressed edit (nothing will persist it).
	c.mu.Lock()
	_, staged := c.msgToUpdate[300]
	c.mu.Unlock()
	if staged {
		t.Fatal("suppressed edit staged a msgToUpdate seam entry (would leak / wedge)")
	}
}

// A REAL edit — the deliverable content changed (here: caption added) — must
// flow exactly as before.
func TestDispatchMessage_RealEditStillDelivered(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, emitDrops: false}
	c := makeChannel(h)
	c.offTrk = newOffsetTracker(800)
	c.msgToUpdate = map[int64][]int64{}
	c.editSupp = newEditSuppressor(8192, 48*time.Hour)

	c.offTrk.Register(801)
	c.dispatchMessage(801, voiceMsg(300, "uniq-A"), false, nil)
	c.onPersisted(&c3types.Inbound{MessageID: 300})

	edited := voiceMsg(300, "uniq-A")
	edited.Caption = "now with a caption"
	c.offTrk.Register(802)
	c.dispatchMessage(802, edited, true, nil)

	if got := h.emitCount(); got != 2 {
		t.Fatalf("content-changed edit must be delivered; Emit called %d times, want 2", got)
	}
}

// Same-update_id redelivery (the loss-free Append-retry path: offset held,
// Telegram re-sent the update, dedup entry forgotten) must NOT be suppressed —
// suppression keys on a DIFFERENT update_id claiming the same content.
func TestDispatchMessage_SameUpdateRedeliveryNotSuppressed(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, emitDrops: false}
	c := makeChannel(h)
	c.offTrk = newOffsetTracker(800)
	c.msgToUpdate = map[int64][]int64{}
	c.editSupp = newEditSuppressor(8192, 48*time.Hour)

	// A real edit (update 810) dispatches… and its durable Append FAILS.
	c.offTrk.Register(810)
	c.dispatchMessage(810, voiceMsg(300, "uniq-A"), true, nil)
	if got := h.emitCount(); got != 1 {
		t.Fatalf("first dispatch must emit; got %d", got)
	}
	c.onPersistFailed(&c3types.Inbound{MessageID: 300})

	// Telegram redelivers update 810 (offset held). It must re-dispatch.
	c.dispatchMessage(810, voiceMsg(300, "uniq-A"), true, nil)
	if got := h.emitCount(); got != 2 {
		t.Fatalf("same-update_id redelivery was suppressed (loss-free retry broken); Emit=%d, want 2", got)
	}
}

// An edit for a message the suppressor has never seen (restart amnesia, or
// older than the TTL) must be delivered — old behavior, never a silent drop.
func TestDispatchMessage_UnknownMessageEditDelivered(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, emitDrops: false}
	c := makeChannel(h)
	c.offTrk = newOffsetTracker(800)
	c.msgToUpdate = map[int64][]int64{}
	c.editSupp = newEditSuppressor(8192, 48*time.Hour)

	c.offTrk.Register(801)
	c.dispatchMessage(801, voiceMsg(300, "uniq-A"), true, nil)
	if got := h.emitCount(); got != 1 {
		t.Fatalf("edit with no recorded baseline must deliver; Emit=%d, want 1", got)
	}
}

// Suppressor unit: TTL expiry forgets the baseline (edit then delivered).
func TestEditSuppressor_TTLExpiry(t *testing.T) {
	s := newEditSuppressor(10, 30*time.Millisecond)
	s.record(42, 300, 801, "fp-A")
	if !s.shouldSuppress(42, 300, 802, "fp-A") {
		t.Fatal("fresh identical fingerprint must suppress")
	}
	time.Sleep(60 * time.Millisecond)
	if s.shouldSuppress(42, 300, 803, "fp-A") {
		t.Fatal("expired baseline must not suppress")
	}
}

// Suppressor unit: capacity eviction is oldest-first and bounded.
func TestEditSuppressor_CapacityEviction(t *testing.T) {
	s := newEditSuppressor(2, time.Hour)
	s.record(42, 1, 801, "fp-1")
	s.record(42, 2, 802, "fp-2")
	s.record(42, 3, 803, "fp-3") // evicts msg 1
	if s.shouldSuppress(42, 1, 900, "fp-1") {
		t.Fatal("evicted baseline must not suppress")
	}
	if !s.shouldSuppress(42, 3, 900, "fp-3") {
		t.Fatal("retained baseline must suppress")
	}
}

// Suppressor unit: a different fingerprint updates nothing on lookup and a
// record after a real edit re-baselines to the NEW content.
func TestEditSuppressor_RebaselineOnRealEdit(t *testing.T) {
	s := newEditSuppressor(10, time.Hour)
	s.record(42, 300, 801, "fp-A")
	if s.shouldSuppress(42, 300, 802, "fp-B") {
		t.Fatal("changed fingerprint must not suppress")
	}
	s.record(42, 300, 802, "fp-B")
	if !s.shouldSuppress(42, 300, 803, "fp-B") {
		t.Fatal("re-baselined fingerprint must suppress the next phantom")
	}
	if s.shouldSuppress(42, 300, 803, "fp-A") {
		t.Fatal("stale fingerprint must not suppress after re-baseline")
	}
}

// Fingerprint unit: text, caption, entities, and media identity all
// distinguish; a byte-identical message fingerprints identically.
func TestEditFingerprint_Distinguishers(t *testing.T) {
	base := voiceMsg(300, "uniq-A")
	inBase := convertInbound("telegram", base, "", nil)
	fpBase := editFingerprint(inBase, base)

	same := voiceMsg(300, "uniq-A")
	if fp := editFingerprint(convertInbound("telegram", same, "", nil), same); fp != fpBase {
		t.Fatal("identical message must fingerprint identically")
	}

	capt := voiceMsg(300, "uniq-A")
	capt.Caption = "hello"
	if fp := editFingerprint(convertInbound("telegram", capt, "", nil), capt); fp == fpBase {
		t.Fatal("caption change must change the fingerprint")
	}

	media := voiceMsg(300, "uniq-B")
	if fp := editFingerprint(convertInbound("telegram", media, "", nil), media); fp == fpBase {
		t.Fatal("media identity change must change the fingerprint")
	}

	txtA := textMsg("hello world", 42)
	inA := convertInbound("telegram", txtA, "", nil)
	txtB := textMsg("hello world", 42)
	txtB.Entities = []gotgbot.MessageEntity{{Type: "bold", Offset: 0, Length: 5}}
	inB := convertInbound("telegram", txtB, "", nil)
	if editFingerprint(inA, txtA) == editFingerprint(inB, txtB) {
		t.Fatal("entity-only (formatting) change must change the fingerprint")
	}
}
