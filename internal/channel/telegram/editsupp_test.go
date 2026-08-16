package telegram

import (
	"reflect"
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

// voiceMsg is dated NOW, i.e. well inside the edit window, because that is the
// state every test in this file means: an edit that a user could actually have
// made. The age rule (2026-07-27) reads msg.Date, so a fixed date in the past
// would silently turn these into ancient-message tests. Ancient cases are
// constructed explicitly with voiceMsgAged.
func voiceMsg(msgID int64, uniqueID string) *gotgbot.Message {
	return &gotgbot.Message{
		MessageId: msgID,
		From:      &gotgbot.User{Id: 42},
		Chat:      gotgbot.Chat{Id: 42},
		Date:      time.Now().Unix(),
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
	c.msgToUpdate = map[seamKey][]int64{}
	c.editSupp = newEditSuppressor(8192, 48*time.Hour)

	// Original voice message, update 801: emitted, persisted.
	c.offTrk.Register(801)
	c.dispatchMessage(801, voiceMsg(300, "uniq-A"), false, nil)
	if got := h.emitCount(); got != 1 {
		t.Fatalf("original must emit once; got %d", got)
	}
	// voiceMsg lands in CHAT 42; the seam is keyed by (chat_id, message_id), so
	// the persist callback must carry that chat or it resolves nothing.
	c.onPersisted(&c3types.Inbound{ChatID: 42, MessageID: 300})
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
	// This is a NEGATIVE assertion, so the key must be the one dispatchMessage
	// would actually have staged: voiceMsg puts the message in CHAT 42. Looking up
	// seamKey{0, 300} would read an empty bucket and pass forever regardless of
	// what the suppressor did.
	c.mu.Lock()
	_, staged := c.msgToUpdate[seamKey{chatID: 42, msgID: 300}]
	c.mu.Unlock()
	if staged {
		t.Fatal("suppressed edit staged a msgToUpdate seam entry (would leak / wedge)")
	}
}

// The channel must STATE the amendment fact on the record it emits. An
// edited_message re-dispatches with the SAME MessageID, and the broker's
// delivered-dedup is keyed on that id alone — without in.Edited it classifies the
// correction as a crash-replay, skips the Append AND acks the update to Telegram,
// destroying the user's edit permanently (internal/broker/worker.go flushInbounds).
func TestDispatchMessage_SetsEditedMarker(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, emitDrops: false}
	c := makeChannel(h) // no editSupp, no offTrk: nothing suppresses or stages here

	// An edited_message (edited=true) must be marked.
	c.dispatchMessage(101, textMsg("corrected", 42), true, nil)
	// An ordinary message must NOT be marked — otherwise the flag is a constant
	// and the broker's replay dedup is disabled for every message, not just edits.
	c.dispatchMessage(100, textMsg("original", 42), false, nil)

	h.mu.Lock()
	got := append([]*c3types.Inbound(nil), h.emitted...)
	h.mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("both dispatches must Emit; got %d", len(got))
	}
	if !got[0].Edited {
		t.Fatal("an edited_message must be emitted with Edited=true, or the broker's message_id-keyed dedup destroys the correction and acks it to Telegram")
	}
	if got[1].Edited {
		t.Fatal("an ordinary message must be emitted with Edited=false (the flag must not be a constant)")
	}
}

func TestDispatchMessage_EditedCapturesMediaGroupAndForwardOrigin(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, emitDrops: false}
	c := makeChannel(h)
	msg := textMsg("corrected forward", 42)
	msg.MediaGroupId = "edited-album"
	msg.ForwardOrigin = gotgbot.MessageOriginChannel{Chat: gotgbot.Chat{Title: "News"}}

	c.dispatchMessage(101, msg, true, nil)

	h.mu.Lock()
	got := append([]*c3types.Inbound(nil), h.emitted...)
	h.mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("edited dispatches = %d, want 1", len(got))
	}
	if !got[0].Edited || got[0].MediaGroupID != "edited-album" {
		t.Fatalf("edited inbound = %+v, want Edited and MediaGroupID captured", got[0])
	}
	want := &c3types.ForwardOrigin{Kind: "channel", Name: "News"}
	if !reflect.DeepEqual(got[0].ForwardOrigin, want) {
		t.Fatalf("ForwardOrigin = %+v, want %+v", got[0].ForwardOrigin, want)
	}
}

// A REAL edit — the deliverable content changed (here: caption added) — must
// flow exactly as before.
func TestDispatchMessage_RealEditStillDelivered(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, emitDrops: false}
	c := makeChannel(h)
	c.offTrk = newOffsetTracker(800)
	c.msgToUpdate = map[seamKey][]int64{}
	c.editSupp = newEditSuppressor(8192, 48*time.Hour)

	c.offTrk.Register(801)
	c.dispatchMessage(801, voiceMsg(300, "uniq-A"), false, nil)
	c.onPersisted(&c3types.Inbound{ChatID: 42, MessageID: 300}) // voiceMsg → chat 42

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
	c.msgToUpdate = map[seamKey][]int64{}
	c.editSupp = newEditSuppressor(8192, 48*time.Hour)

	// A real edit (update 810) dispatches… and its durable Append FAILS.
	c.offTrk.Register(810)
	c.dispatchMessage(810, voiceMsg(300, "uniq-A"), true, nil)
	if got := h.emitCount(); got != 1 {
		t.Fatalf("first dispatch must emit; got %d", got)
	}
	c.onPersistFailed(&c3types.Inbound{ChatID: 42, MessageID: 300}) // voiceMsg → chat 42

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
	c.msgToUpdate = map[seamKey][]int64{}
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
	if !suppressed(s, 42, 300, 802, "fp-A") {
		t.Fatal("fresh identical fingerprint must suppress")
	}
	time.Sleep(60 * time.Millisecond)
	if suppressed(s, 42, 300, 803, "fp-A") {
		t.Fatal("expired baseline must not suppress")
	}
}

// Suppressor unit: capacity eviction is oldest-first and bounded.
func TestEditSuppressor_CapacityEviction(t *testing.T) {
	s := newEditSuppressor(2, time.Hour)
	s.record(42, 1, 801, "fp-1")
	s.record(42, 2, 802, "fp-2")
	s.record(42, 3, 803, "fp-3") // evicts msg 1
	if suppressed(s, 42, 1, 900, "fp-1") {
		t.Fatal("evicted baseline must not suppress")
	}
	if !suppressed(s, 42, 3, 900, "fp-3") {
		t.Fatal("retained baseline must suppress")
	}
}

// Suppressor unit: a different fingerprint updates nothing on lookup and a
// record after a real edit re-baselines to the NEW content.
func TestEditSuppressor_RebaselineOnRealEdit(t *testing.T) {
	s := newEditSuppressor(10, time.Hour)
	s.record(42, 300, 801, "fp-A")
	if suppressed(s, 42, 300, 802, "fp-B") {
		t.Fatal("changed fingerprint must not suppress")
	}
	s.record(42, 300, 802, "fp-B")
	if !suppressed(s, 42, 300, 803, "fp-B") {
		t.Fatal("re-baselined fingerprint must suppress the next phantom")
	}
	if suppressed(s, 42, 300, 803, "fp-A") {
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
