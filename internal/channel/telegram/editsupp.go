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
// Capacity-bounded LRU + TTL, same shape as updateDedup. The TTL tracks
// Telegram's 48-hour user edit window; past it an edit is delivered (the old
// behavior) rather than remembered forever. The memory is in-process only: a
// broker restart forgets baselines, so the first phantom edit after a restart
// is delivered — an accepted, bounded degradation.
type editSuppressor struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	order    *list.List               // back = newest, front = oldest
	index    map[string]*list.Element // key → element holding *editBaseline
}

type editBaseline struct {
	key      string
	fp       string // fingerprint of the last-dispatched deliverable content
	updateID int64  // update that dispatched it (same-id redelivery exemption)
	insertAt time.Time
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

// shouldSuppress reports whether an edited_message dispatch is content-free:
// the fingerprint matches the recorded baseline for (chat, msg) AND the
// update_id differs (same id = a loss-free redelivery of the very dispatch
// that recorded the baseline — never suppressed). A suppressed lookup does
// not touch the baseline: it stays keyed to the last DELIVERED content.
func (s *editSuppressor) shouldSuppress(chatID, msgID, updateID int64, fp string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpiredLocked()
	el, ok := s.index[editKey(chatID, msgID)]
	if !ok {
		return false
	}
	b := el.Value.(*editBaseline)
	return b.fp == fp && b.updateID != updateID
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
	el := s.order.PushBack(&editBaseline{key: key, fp: fp, updateID: updateID, insertAt: time.Now()})
	s.index[key] = el
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
