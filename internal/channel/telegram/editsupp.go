package telegram

import (
	"container/list"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
)

// editSuppressor remembers the last-dispatched content fingerprint per
// (chat_id, message_id) so a spurious edited_message — one whose deliverable
// content is byte-identical to what was already dispatched — is dropped
// instead of re-delivered. The Bot API documents that edited_message "may at
// times be triggered by changes to message fields that are either unavailable
// or not actively used by your bot"; the reliable live trigger is a REACTION
// on the message (including C3's own `react` tool), which otherwise re-runs
// STT and re-delivers a full voice transcription minutes or hours after the
// original (2026-07-19 duplicate-delivery report).
//
// A REAL edit changes the fingerprint (text/caption, entities, or media
// identity) and flows exactly as before. A redelivery of the SAME update_id
// is never suppressed — that is the loss-free Append-retry path (offset held,
// Telegram re-sent the update after onPersistFailed evicted its dedup entry),
// and it must genuinely re-dispatch.
//
// Capacity-bounded LRU + TTL, same shape as updateDedup. The TTL tracks the
// edit window below; past it an edit is delivered (the old behavior) rather
// than remembered forever.
//
// Baseline memory is in-process only, and a broker restart forgets it. That was
// documented as "an accepted, bounded degradation" — the 2026-07-27 incident
// showed it is not bounded in the way that sentence implies. A REACTION can
// land on a message of ANY age, and Telegram still emits edited_message for it;
// the TTL rationale only ever covered real edits. Voice 6661 was reacted to at
// 62 hours old, after a restart, and was re-transcribed (nondeterministically:
// 344 chars the first time, 385 the second) and delivered again, reading like a
// user correction. Two baseline-free rules now cover that gap — see
// suppressReason.
type editSuppressor struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	order    *list.List               // back = newest, front = oldest
	index    map[string]*list.Element // key → element holding *editBaseline
}

// userEditWindow is how far back a message can still be edited. The Bot API
// does not document a window for a USER editing their own message; the only
// edit horizon it states for messages not sent by the bot is 48 hours, in two
// independent places (editMessageText/Caption/Media/ReplyMarkup: "business
// messages that were not sent by the bot and do not contain an inline keyboard
// can only be edited within 48 hours from the time they were sent"; and
// deleteMessage: "A message can only be deleted if it was sent less than 48
// hours ago" — https://core.telegram.org/bots/api, verified 2026-07-29). So the
// number is the documented horizon rather than folklore, and it is the same 48
// hours this suppressor already used for its TTL.
const userEditWindow = 48 * time.Hour

// reactArmWindow is how long after C3's OWN react a baseline-free
// edited_message for that exact message is treated as the reaction echoing
// back. Telegram emitted the phantom ~5 seconds after setMessageReaction in the
// incident; the window is wider only to absorb a slow poll cycle.
//
// Kept deliberately SHORT and ONE-SHOT because it is the one rule here that can
// swallow a real edit: within the window, a genuine correction to a message
// with no baseline is dropped. Reaching that requires the user to edit a
// message whose baseline this process never recorded (i.e. one that predates a
// restart) in the same minute the agent reacted to it. Weighed against the
// alternative — re-delivering a freshly re-transcribed voice note as if the
// user had re-sent it — this is the smaller loss, and the window only ever
// opens because C3 itself just acted.
const reactArmWindow = 60 * time.Second

type editBaseline struct {
	key      string
	fp       string // fingerprint of the last-dispatched deliverable content
	updateID int64  // update that dispatched it (same-id redelivery exemption)
	insertAt time.Time
	// armedAt is when C3's own react tool touched this message, for entries that
	// carry no fingerprint yet. Zero once consumed or never armed.
	armedAt time.Time
}

func newEditSuppressor(capacity int, ttl time.Duration) *editSuppressor {
	return &editSuppressor{
		capacity: capacity,
		ttl:      ttl,
		order:    list.New(),
		index:    map[string]*list.Element{},
	}
}

func editKey(chatID, msgID int64) string {
	return fmt.Sprintf("c%d|m%d", chatID, msgID)
}

// suppressReason names why an edited_message dispatch must be dropped, or ""
// when it must be delivered. It returns a REASON rather than a bool because a
// silent drop's only trace is the log line, and three different causes reported
// with one wording would send an operator hunting the wrong one.
//
// The rules, in the order that matters:
//
//  1. A baseline exists (this process dispatched the message). Same update_id ⇒
//     NEVER suppressed: that is the loss-free Append-retry path (offset held,
//     Telegram re-sent the update after onPersistFailed evicted its dedup
//     entry) and it must genuinely re-dispatch. Otherwise an identical
//     fingerprint means the edit carries nothing new, and a CHANGED fingerprint
//     means real content changed — delivered, and deliberately checked before
//     the age rule below: positive evidence that content moved outranks any
//     heuristic about what "should" be possible.
//  2. C3's own react armed this message moments ago and no baseline exists.
//     One-shot: the arm is consumed whether or not more edits follow.
//  3. No baseline, and the message predates the edit window — a genuine user
//     edit is impossible, so the update is a phantom no matter what triggered
//     it. This is the rule that covers a reaction on an old message, which
//     nothing else could: reactions are not bounded by the edit window, and a
//     restarted broker has no baseline to compare against.
//
// msgUnixDate is Telegram's own Message.Date: the ORIGINAL send time in unix
// seconds, which does not move when a message is edited. A message with no
// stated date (0) is never aged out — unknown is not ancient, and guessing
// would drop a real edit on the strength of a missing field.
//
// A suppressed lookup does not touch the baseline: it stays keyed to the last
// DELIVERED content.
func (s *editSuppressor) suppressReason(chatID, msgID, updateID int64, fp string, msgUnixDate int64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpiredLocked()
	now := time.Now()
	if el, ok := s.index[editKey(chatID, msgID)]; ok {
		b := el.Value.(*editBaseline)
		switch {
		case b.fp != "":
			if b.updateID == updateID || b.fp != fp {
				return ""
			}
			return "deliverable content unchanged since the last dispatch"
		case !b.armedAt.IsZero() && now.Sub(b.armedAt) <= reactArmWindow:
			b.armedAt = time.Time{} // one-shot: never suppress a second edit on this arm
			return "C3's own react touched this message moments ago"
		}
	}
	if age := now.Sub(time.Unix(msgUnixDate, 0)); msgUnixDate > 0 && age > userEditWindow {
		return fmt.Sprintf("no baseline and the message is %s old, past the %s edit window — a user edit is impossible",
			age.Round(time.Hour), userEditWindow)
	}
	return ""
}

// armReact records that C3's own react tool is about to touch (chat, msg), so
// the edited_message Telegram emits in response is recognized without a
// baseline. An existing baseline is REFRESHED rather than overwritten — its
// fingerprint is strictly better evidence than the arm, and moving it to the
// back of the LRU keeps it from aging out from under the reaction it is about
// to have to explain.
func (s *editSuppressor) armReact(chatID, msgID int64) {
	key := editKey(chatID, msgID)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpiredLocked()
	if el, ok := s.index[key]; ok {
		b := el.Value.(*editBaseline)
		b.armedAt, b.insertAt = time.Now(), time.Now()
		s.order.MoveToBack(el)
		return
	}
	s.pushLocked(&editBaseline{key: key, insertAt: time.Now(), armedAt: time.Now()})
}

// record stores fp as the delivered-content baseline for (chat, msg) —
// called for every dispatched (non-suppressed) message, original or edit.
func (s *editSuppressor) record(chatID, msgID, updateID int64, fp string) {
	key := editKey(chatID, msgID)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpiredLocked()
	if el, ok := s.index[key]; ok {
		b := el.Value.(*editBaseline)
		b.fp, b.updateID, b.insertAt = fp, updateID, time.Now()
		s.order.MoveToBack(el)
		return
	}
	s.pushLocked(&editBaseline{key: key, fp: fp, updateID: updateID, insertAt: time.Now()})
}

// pushLocked appends a new baseline and trims to capacity, oldest-first.
// Caller must hold s.mu.
func (s *editSuppressor) pushLocked(b *editBaseline) {
	s.index[b.key] = s.order.PushBack(b)
	for s.order.Len() > s.capacity {
		front := s.order.Front()
		if front == nil {
			break
		}
		s.order.Remove(front)
		delete(s.index, front.Value.(*editBaseline).key)
	}
}

// evictExpiredLocked walks from the oldest until it finds a non-expired
// baseline. Caller must hold s.mu.
func (s *editSuppressor) evictExpiredLocked() {
	cutoff := time.Now().Add(-s.ttl)
	for {
		front := s.order.Front()
		if front == nil {
			return
		}
		b := front.Value.(*editBaseline)
		if b.insertAt.After(cutoff) {
			return
		}
		s.order.Remove(front)
		delete(s.index, b.key)
	}
}

// editFingerprint hashes exactly the content C3 delivers for a message: the
// extracted text/caption (in.Text — for rich messages that is the decoded
// markdown, so entity changes surface there), attachment kinds/names, the
// stable media identities (file_unique_id — file_id is NOT guaranteed stable
// across updates, and hashing it would make suppression silently unreliable),
// and the raw entity lists (a formatting-only edit on the non-rich path must
// still re-deliver). Fields outside this set cannot change what a session
// sees, so an "edit" that leaves the fingerprint identical is content-free.
func editFingerprint(in *c3types.Inbound, msg *gotgbot.Message) string {
	h := sha256.New()
	ws := func(s string) { h.Write([]byte(s)); h.Write([]byte{0}) }
	ws(in.Text)
	for _, a := range in.Attachments {
		ws(a.Kind)
		ws(a.Name)
	}
	for _, id := range mediaUniqueIDs(msg) {
		ws(id)
	}
	if len(msg.Entities) > 0 {
		if b, err := json.Marshal(msg.Entities); err == nil {
			h.Write(b)
		}
	}
	if len(msg.CaptionEntities) > 0 {
		if b, err := json.Marshal(msg.CaptionEntities); err == nil {
			h.Write(b)
		}
	}
	return string(h.Sum(nil))
}

// mediaUniqueIDs collects the stable (file_unique_id) identities of every
// media payload convertInbound can surface, kind-prefixed so a cross-kind
// collision is impossible. Replacing media in an edit changes the unique id,
// so a media-swap edit always re-delivers.
func mediaUniqueIDs(msg *gotgbot.Message) []string {
	var ids []string
	if msg.Voice != nil {
		ids = append(ids, "voice:"+msg.Voice.FileUniqueId)
	}
	if len(msg.Photo) > 0 {
		ids = append(ids, "photo:"+pickBestPhoto(msg.Photo).FileUniqueId)
	}
	if msg.Document != nil {
		ids = append(ids, "doc:"+msg.Document.FileUniqueId)
	}
	if msg.Audio != nil {
		ids = append(ids, "audio:"+msg.Audio.FileUniqueId)
	}
	if msg.Video != nil {
		ids = append(ids, "video:"+msg.Video.FileUniqueId)
	}
	if msg.VideoNote != nil {
		ids = append(ids, "vnote:"+msg.VideoNote.FileUniqueId)
	}
	if msg.Sticker != nil {
		ids = append(ids, "sticker:"+msg.Sticker.FileUniqueId)
	}
	if msg.Animation != nil {
		ids = append(ids, "anim:"+msg.Animation.FileUniqueId)
	}
	return ids
}
