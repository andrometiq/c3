// Package broker — pairing state machine.
//
// TODO #1 (locked 2026-05-18): the broker enforces a default-deny posture
// for inbound traffic. Only messages from an allowlisted user_id (DM) or
// allowlisted chat_id (group) reach the worker pool. To bootstrap an
// empty allowlist, the broker enters pairing mode — generates a
// cryptographically random 4-digit code with a 10-minute TTL and surfaces
// it on the CLI. Any inbound whose body matches the code exactly adds the
// appropriate identity to the allowlist and persists mappings.json.
//
// Pairing modes are SEPARATE per surface:
//   - DM-pair: matches in private chats; allowlists Sender.UserID.
//   - Group-pair-<chat_id>: matches only in that group; allowlists ChatID
//     (the user_id who typed the code is incidental — we trust the group,
//     not the individual member).
//
// Wrong-code, no-active-pairing, and ALL non-allowlisted traffic are
// silently dropped. Strangers see nothing.
package broker

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/karthikeyan5/c3/internal/c3types"
	"github.com/karthikeyan5/c3/internal/mappings"
)

// PairTargetType is which surface a pairing window applies to.
type PairTargetType int

const (
	// PairTargetDM — pairing for the bot's private (DM) surface. A match
	// adds the SENDER's user_id to allowlist.Users.
	PairTargetDM PairTargetType = iota
	// PairTargetGroup — pairing for one specific group (chat_id). A match
	// from within that chat adds the chat_id (not the user_id) to
	// allowlist.Groups.
	PairTargetGroup
)

// PairTTL is the lifetime of any one pairing window. After this elapses
// without a match, the window expires and pairing must be manually
// restarted via /c3:pair.
const PairTTL = 10 * time.Minute

// pairMaxWrongCodes is how many wrong 4-digit candidates a single pairing window
// tolerates before it closes and a fresh /c3:pair is required.
//
// Without a limit the 10^4 code space is exhaustible inside one 10-minute window:
// wrong guesses were silently dropped and left the window live, so an attacker
// could spray the whole space for free — and `/status` answers only allowlisted
// senders, giving a clean success oracle. The prize is not read access: a paired
// user is written into allowlist.Users, which is exactly the operator set that may
// tap Allow on a relayed tool-use prompt and make the coding agent execute tools on
// the operator's machine.
//
// This is the same defence the setup-side pairing poller already had
// (cmd/c3-broker/setup.go), which was never ported to the broker's own gate.
// Ten attempts leaves a 0.1% chance per window instead of a certainty.
// (v0.1.0 release audit, 2026-07-25.)
const pairMaxWrongCodes = 10

// PairWindow describes one active pairing session.
type PairWindow struct {
	Target   PairTargetType
	ChatID   int64 // populated for PairTargetGroup; zero for DM
	Code     string
	ExpireAt time.Time
	// Wrong counts rejected 4-digit candidates seen by this window. At
	// pairMaxWrongCodes the window is closed — see pairMaxWrongCodes.
	Wrong int
}

// IsActive reports whether the window has not yet expired.
func (w *PairWindow) IsActive(now time.Time) bool {
	return w != nil && now.Before(w.ExpireAt)
}

// pairingState holds the live pairing windows. Concurrency: a single
// mutex guards all maps; pairing is a low-volume control-plane concern,
// not a hot path.
type pairingState struct {
	mu sync.Mutex
	// dm is the live DM-pairing window (at most one).
	dm *PairWindow
	// groups maps chat_id → live group-pairing window for that chat.
	// Independent per chat so two simultaneous group pairings can run.
	groups map[int64]*PairWindow
	// now overrides time.Now for tests. Nil = use real clock.
	now func() time.Time
}

// newPairingState returns an empty state with the real clock.
func newPairingState() *pairingState {
	return &pairingState{
		groups: map[int64]*PairWindow{},
		now:    time.Now,
	}
}

// generateCode returns a fresh cryptographically random 4-digit code as
// a 4-character zero-padded string, e.g. "5829", "0042". Uses crypto/rand
// so an attacker can't predict the next code from the previous one (low
// risk given 10-minute TTL + silent-drop on wrong-code, but trivially
// stronger than time-seeded math/rand).
func generateCode() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(10000))
	if err != nil {
		return "", fmt.Errorf("pairing: rand.Int: %w", err)
	}
	return fmt.Sprintf("%04d", n.Int64()), nil
}

// StartDM starts (or restarts) the DM-pairing window with a fresh code
// and TTL. Returns the new code.
func (p *pairingState) StartDM() (string, error) {
	code, err := generateCode()
	if err != nil {
		return "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dm = &PairWindow{
		Target:   PairTargetDM,
		Code:     code,
		ExpireAt: p.now().Add(PairTTL),
	}
	return code, nil
}

// StartGroup starts (or restarts) pairing for one group chat_id.
func (p *pairingState) StartGroup(chatID int64) (string, error) {
	code, err := generateCode()
	if err != nil {
		return "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groups[chatID] = &PairWindow{
		Target:   PairTargetGroup,
		ChatID:   chatID,
		Code:     code,
		ExpireAt: p.now().Add(PairTTL),
	}
	return code, nil
}

// DMWindow returns the current DM pairing window, or nil if none is active.
// Active = unexpired. Expired windows are cleared on access.
func (p *pairingState) DMWindow() *PairWindow {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dm == nil {
		return nil
	}
	if !p.dm.IsActive(p.now()) {
		p.dm = nil
		return nil
	}
	return p.dm
}

// GroupWindow returns the active pairing window for chatID, or nil.
func (p *pairingState) GroupWindow(chatID int64) *PairWindow {
	p.mu.Lock()
	defer p.mu.Unlock()
	w, ok := p.groups[chatID]
	if !ok {
		return nil
	}
	if !w.IsActive(p.now()) {
		delete(p.groups, chatID)
		return nil
	}
	return w
}

// ClearDM ends the DM pairing window. Called on a successful match
// (no auto re-arm — per spec, manual /c3:pair required after).
func (p *pairingState) ClearDM() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dm = nil
}

// ClearGroup ends pairing for one chat_id.
func (p *pairingState) ClearGroup(chatID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.groups, chatID)
}

// RecordWrongDM counts one wrong code against the DM window and reports whether
// that exhausted it. An exhausted window is closed here, so the caller only has
// to decide what to log.
func (p *pairingState) RecordWrongDM() (attempts int, exhausted bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dm == nil || !p.dm.IsActive(p.now()) {
		return 0, false
	}
	p.dm.Wrong++
	attempts = p.dm.Wrong
	if attempts >= pairMaxWrongCodes {
		p.dm = nil
		return attempts, true
	}
	return attempts, false
}

// RecordWrongGroup is RecordWrongDM for one group's window.
func (p *pairingState) RecordWrongGroup(chatID int64) (attempts int, exhausted bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	w, ok := p.groups[chatID]
	if !ok || !w.IsActive(p.now()) {
		return 0, false
	}
	w.Wrong++
	attempts = w.Wrong
	if attempts >= pairMaxWrongCodes {
		delete(p.groups, chatID)
		return attempts, true
	}
	return attempts, false
}

// AnyActive reports whether any pairing window is currently live.
// Used by tests / diagnostics; not on the hot path.
func (p *pairingState) AnyActive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if p.dm != nil && p.dm.IsActive(now) {
		return true
	}
	for _, w := range p.groups {
		if w.IsActive(now) {
			return true
		}
	}
	return false
}

// GateDecision is the result of running an inbound through the
// allowlist + pairing gate.
type GateDecision int

const (
	// GateAllow — message passes; forward downstream.
	GateAllow GateDecision = iota
	// GateDrop — drop silently; do not forward to broker workers.
	GateDrop
	// GatePairConsumed — message body matched an active pairing code.
	// The allowlist was updated and persisted; pairing window cleared.
	// Per spec, the message itself is NOT forwarded (the user's "5829"
	// is a control-plane signal, not content).
	GatePairConsumed
)

// codeBody returns the trimmed code form of in.Text if it's a valid
// 4-digit pairing-code candidate; "" otherwise. Strict per spec — the
// match is `[0-9]{4}` with NO whitespace, NO extra characters. We allow
// surrounding newlines/whitespace ONLY via the strict equality check
// downstream (callers compare directly to PairWindow.Code).
func codeBody(text string) string {
	if len(text) != 4 {
		return ""
	}
	for _, r := range text {
		if r < '0' || r > '9' {
			return ""
		}
	}
	return text
}

// isPrivateChat heuristically determines whether an Inbound came from a
// Telegram private chat. Telegram's Bot API uses positive chat_ids for
// users/bots and negative chat_ids for groups/supergroups/channels.
// In a private chat, chat_id == sender's user_id. We use chat_id sign
// as the authoritative signal: positive = DM, negative = group.
//
// Source: https://core.telegram.org/bots/api#chat — Chat.id is signed,
// negative for groups, positive for private chats.
func isPrivateChat(in *c3types.Inbound) bool {
	return in != nil && in.ChatID > 0
}

// Gate runs an inbound through the allowlist + pairing gate. Returns
// the decision the channel layer should act on. When the decision is
// GatePairConsumed, the broker has already mutated and persisted the
// allowlist.
func (b *Broker) Gate(in *c3types.Inbound) GateDecision {
	if in == nil {
		return GateDrop
	}
	mf := b.Mappings()
	// Fast-path: allowlisted user (DM-cleared) or allowlisted group.
	if isPrivateChat(in) {
		if mf.IsUserAllowed(in.Sender.UserID) {
			return GateAllow
		}
	} else {
		if mf.IsGroupAllowed(in.ChatID) {
			return GateAllow
		}
	}

	// CB-2: an aggregate poll_result event is bot-initiated on an already-trusted
	// route — the bot only ever sends a poll onto a route it was attached to. Such
	// an aggregate update carries NO voter (Telegram's `poll` update has no user),
	// so the channel stamps the stored route-owner UserID before emit (see
	// internal/channel/telegram/poll.go dispatchPollUpdate + the sentPolls map).
	// The fast-path above normally clears it via IsUserAllowed(owner). This branch
	// is the explicit safety net: if the owner could not be determined (stamp==0),
	// a poll_result on a DM route the bot itself initiated must NOT be silently
	// gate-dropped — that is the exact regression that made the headline feature
	// dead in DMs. The allowlist stays authoritative for everything carrying a
	// real user (per-voter would-be answers, reactions, callbacks gate normally).
	if in.Kind == c3types.InboundPollResult {
		return GateAllow
	}

	// Allowlist miss. Check pairing.
	body := codeBody(in.Text)
	if body == "" {
		// Not even a 4-digit candidate; default-deny.
		return GateDrop
	}

	if isPrivateChat(in) {
		w := b.Pairing.DMWindow()
		if w == nil {
			return GateDrop
		}
		if body != w.Code {
			// Wrong code during active pairing → silent drop to the sender, but the
			// window is NOT free to guess at: count the attempt and close it once
			// exhausted (see pairMaxWrongCodes).
			if n, exhausted := b.Pairing.RecordWrongDM(); exhausted {
				log.Printf("pairing: DM window CLOSED after %d wrong codes — possible brute-force; run /c3:pair again to re-arm", n)
			}
			return GateDrop
		}
		// Match. Add user to allowlist, persist, clear window.
		b.acceptDMPair(in.Sender.UserID, w.Code)
		return GatePairConsumed
	}

	// Group inbound — check group pairing for THIS chat_id.
	w := b.Pairing.GroupWindow(in.ChatID)
	if w == nil {
		return GateDrop
	}
	if body != w.Code {
		if n, exhausted := b.Pairing.RecordWrongGroup(in.ChatID); exhausted {
			log.Printf("pairing: group %d window CLOSED after %d wrong codes — possible brute-force; run /c3:pair again to re-arm", in.ChatID, n)
		}
		return GateDrop
	}
	b.acceptGroupPair(in.ChatID, w.Code)
	return GatePairConsumed
}

// acceptDMPair finalizes a DM pairing match: clears the window, adds the
// user_id to allowlist.Users, persists mappings.json. Best-effort
// persistence — failures are logged but the in-memory allowlist update
// still takes effect (the gate will let the user through immediately).
func (b *Broker) acceptDMPair(userID int64, code string) {
	b.Pairing.ClearDM()
	b.mutateMappings(func(mf *mappings.MappingsFile) {
		mf.AddAllowedUser(userID)
	})
	if err := b.SaveMappings(); err != nil {
		log.Printf("pairing: DM match user=%d code=%s ACCEPTED (in-memory); persist failed: %v", userID, code, err)
		return
	}
	log.Printf("pairing: DM match user=%d code=%s ACCEPTED; allowlist persisted", userID, code)
}

// acceptGroupPair finalizes a group pairing match: clears that group's
// window, adds the chat_id to allowlist.Groups, persists.
func (b *Broker) acceptGroupPair(chatID int64, code string) {
	b.Pairing.ClearGroup(chatID)
	b.mutateMappings(func(mf *mappings.MappingsFile) {
		mf.AddAllowedGroup(chatID)
	})
	if err := b.SaveMappings(); err != nil {
		log.Printf("pairing: group match chat=%d code=%s ACCEPTED (in-memory); persist failed: %v", chatID, code, err)
		return
	}
	log.Printf("pairing: group match chat=%d code=%s ACCEPTED; allowlist persisted", chatID, code)
}

// AutoStartDMPairingIfEmpty starts a DM pairing window automatically when
// the broker boots with an empty user allowlist. Logs the code so the
// operator can see it in broker.log. Caller (main) should also surface
// the code on stderr / CLI for visibility.
//
// Returns the code on success, or "" if pairing was NOT started (already
// have allowlisted users, or code-generation failed — both logged).
func (b *Broker) AutoStartDMPairingIfEmpty() string {
	mf := b.Mappings()
	if len(mf.AllowlistOrEmpty().Users) > 0 {
		return ""
	}
	code, err := b.Pairing.StartDM()
	if err != nil {
		log.Printf("pairing: auto-start DM FAILED: %v", err)
		return ""
	}
	log.Printf("pairing: DM pairing AUTO-ARMED — send %q to the bot within %v to pair", code, PairTTL)
	return code
}
