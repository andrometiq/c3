package broker

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// Ask-registry lifecycle bounds (FIX-1: no-expiry/no-cap leak + stale live
// keyboard). An untapped ask (or a multi-select toggled-but-never-Done) used to
// stay registered forever, leaving its Telegram keyboard live long after the
// adapter's answer timeout returned — a late tap would then rewrite the message
// to "✅ <choice>" while the agent already moved on.
//
//   - askExpiryTTL: how old an ask may get before the reaper removes it and
//     best-effort clears its keyboard. MUST exceed the adapter's 600s answer
//     timeout plus margin so the broker never clears a keyboard the adapter is
//     still legitimately awaiting (adapter askAnswerTimeout = 600s ⇒ 11 min).
//   - askSweepInterval: reaper cadence.
//   - maxPendingAsks: registry size cap; register evicts the oldest entry (and
//     clears its keyboard) when full, so a flood can't grow the map unbounded.
const (
	askExpiryTTL     = 11 * time.Minute
	askSweepInterval = 1 * time.Minute
	maxPendingAsks   = 1000
)

// askCallbackPrefix is the opaque callback_data namespace for `ask` keyboards:
// "ask:<askID>:<optIdx>". The askID is 8-char base32 (no colon), so the LAST
// colon always separates the option index — robust against any future askID
// alphabet that the parser would otherwise mis-split.
const askCallbackPrefix = "ask:"

// pendingAsk is one registered, not-yet-answered `ask` awaiting a button tap. It
// is registered BEFORE the question is sent (the fast-tap race: a human could
// tap before the sendMessage round-trip returns), and removed atomically on
// resolution so a stale/double tap can't resolve it twice.
//
// options/question are kept so the broker can map the tapped index → option
// string and re-render the message ("✅ <chosen>") after clearing the keyboard.
//
// Phase 2 adds the multi-select / skip taxonomy:
//   - multi: each option tap TOGGLES selection (selected[idx]) and re-renders the
//     keyboard in place; a trailing "Done" button resolves with the selected list.
//   - allowSkip: a trailing "Skip" button resolves with AskAnswer{Skipped:true}.
//   - selected: per-option selection state, sized to len(options); only meaningful
//     for a multi ask. Mutated in place (under the registry mutex) on each toggle.
type pendingAsk struct {
	askID     string
	route     RouteKey
	question  string
	options   []string
	multi     bool
	allowSkip bool
	selected  []bool
	messageID int64

	// owner is the SESSION this question was asked FOR: the holder of `route` at
	// registration time (handleAskRegister routes via stub.CurrentRoute(), so that
	// holder IS the asking stub). deliverAskResult re-derives the recipient from
	// the routes table at TAP time, so without this an answer is delivered to
	// whoever holds the topic when the human taps — a different session than the
	// one blocked on the question — while the chat records "✅ <option>" as though
	// that session's question had been answered. Stamped by registerAsk; nil only
	// on a record built by hand (tests), which leaves the check inert.
	owner *Stub

	// createdAt is when the ask was registered, stamped by register (FIX-1). The
	// reaper expires asks older than askExpiryTTL, and the size cap evicts the
	// entry with the smallest createdAt. Uses time.Now() — the same clock the rest
	// of the broker uses (healthfile, pingText).
	createdAt time.Time
}

// askRegistry holds the broker's in-flight asks keyed by askID. Mutex-guarded:
// register runs on the connection handler goroutine, resolveAsk on a route
// worker goroutine.
type askRegistry struct {
	mu sync.Mutex
	m  map[string]*pendingAsk
}

func newAskRegistry() *askRegistry {
	return &askRegistry{m: map[string]*pendingAsk{}}
}

// register inserts p, stamping createdAt (if unset) so the reaper / size cap can
// age it. ok=false (evicted=nil) when askID is already registered (the rare
// adapter-side collision) so the caller can fail the registration fast rather
// than clobber a live ask. When the registry is at maxPendingAsks it first
// evicts the OLDEST entry (smallest createdAt) and returns it as `evicted` so the
// caller can best-effort clear its now-orphaned keyboard (the clear needs a
// channel edit, which the registry has no handle on — see Broker.registerAsk).
func (r *askRegistry) register(p *pendingAsk) (evicted *pendingAsk, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.m[p.askID]; exists {
		return nil, false
	}
	if p.createdAt.IsZero() {
		p.createdAt = time.Now()
	}
	if len(r.m) >= maxPendingAsks {
		evicted = r.evictOldestLocked()
	}
	r.m[p.askID] = p
	return evicted, true
}

// evictOldestLocked removes and returns the entry with the smallest createdAt.
// Caller holds r.mu. Returns nil only on an empty map (never the case at the cap).
func (r *askRegistry) evictOldestLocked() *pendingAsk {
	var oldest *pendingAsk
	for _, p := range r.m {
		if oldest == nil || p.createdAt.Before(oldest.createdAt) {
			oldest = p
		}
	}
	if oldest != nil {
		delete(r.m, oldest.askID)
	}
	return oldest
}

// sweepExpired removes and returns every ask older than ttl (createdAt + ttl <=
// now). Used by the reaper; the caller best-effort clears each returned ask's
// keyboard.
func (r *askRegistry) sweepExpired(now time.Time, ttl time.Duration) []*pendingAsk {
	r.mu.Lock()
	defer r.mu.Unlock()
	var expired []*pendingAsk
	for id, p := range r.m {
		if now.Sub(p.createdAt) >= ttl {
			expired = append(expired, p)
			delete(r.m, id)
		}
	}
	return expired
}

// setMessageID records the sent message's id so resolution can edit it.
func (r *askRegistry) setMessageID(askID string, id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.m[askID]; ok {
		p.messageID = id
	}
}

// delete removes an ask (e.g. when the send failed after registration).
func (r *askRegistry) delete(askID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, askID)
}

// has reports whether askID is currently registered (diagnostics/tests).
func (r *askRegistry) has(askID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.m[askID]
	return ok
}

// bindingOf returns the tap-binding facts for askID — the message its keyboard
// was rendered on and the session it was asked for — WITHOUT removing or
// mutating anything. ok=false when askID is not registered. Peeked before any
// resolve path runs so a mis-bound tap is refused before a multi-select toggle
// has already flipped selection state on its way to being rejected.
func (r *askRegistry) bindingOf(askID string) (owner *Stub, messageID int64, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.m[askID]
	if !ok {
		return nil, 0, false
	}
	return p.owner, p.messageID, true
}

// take atomically removes and returns the pendingAsk (resolve-once). The second
// concurrent tap for the same ask gets ok=false.
func (r *askRegistry) take(askID string) (*pendingAsk, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.m[askID]
	if ok {
		delete(r.m, askID)
	}
	return p, ok
}

// askTapResult is the atomic outcome of an option tap ("ask:<id>:<idx>"), decided
// under the registry mutex by tapIndex.
//
//   - match=false              → no live ask on this route with an in-range idx;
//     the caller falls through to the generic event path.
//   - match=true, resolved=true  → SINGLE-select ask: REMOVED from the registry.
//     chosen/question/messageID describe how to deliver + mark-answered the choice.
//   - match=true, resolved=false → MULTI-select ask: selected[idx] toggled IN PLACE
//     (the ask stays registered). kb/question/messageID describe the keyboard edit
//     (text stays the question; kb shows the updated ✓ prefixes + Done/Skip).
type askTapResult struct {
	match     bool
	resolved  bool
	chosen    string
	question  string
	messageID int64
	kb        [][]c3types.Button
}

// tapIndex applies an option tap atomically. For a SINGLE-select ask it removes
// and resolves; for a MULTI-select ask it toggles selected[idx] in place and
// returns the rebuilt keyboard — WITHOUT removing the ask (so later toggles /
// Done can still resolve it). All state reads/writes happen under the mutex so a
// concurrent setMessageID (fast-tap race) cannot data-race the keyboard build.
func (r *askRegistry) tapIndex(askID string, route RouteKey, idx int) askTapResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.m[askID]
	if !ok || p.route != route || idx < 0 || idx >= len(p.options) {
		return askTapResult{}
	}
	if p.multi {
		if idx < len(p.selected) {
			p.selected[idx] = !p.selected[idx]
		}
		return askTapResult{
			match: true, resolved: false,
			question: p.question, messageID: p.messageID, kb: askKeyboardFor(p),
		}
	}
	// Single-select: resolve-once — remove it.
	delete(r.m, askID)
	return askTapResult{
		match: true, resolved: true,
		chosen: p.options[idx], question: p.question, messageID: p.messageID,
	}
}

// askDoneSuffix / askSuffixSkip are the reserved non-numeric callback suffixes for
// the multi-select "Done" and the "Skip" buttons: "ask:<askID>:done" /
// "ask:<askID>:skip". They can never collide with an option index (which is always
// a decimal integer), and the askID alphabet (base32) contains no colon, so the
// LAST colon always separates the suffix.
const (
	askDoneSuffix = "done"
	askSkipSuffix = "skip"
)

// askKeyboard builds the single-select inline keyboard for an ask: one button
// per option, each on its own row, callback_data "ask:<askID>:<idx>". Kept as a
// thin convenience over askKeyboardFor (single-select = a pendingAsk with no
// multi/allowSkip) so the format has ONE source of truth.
func askKeyboard(askID string, options []string) [][]c3types.Button {
	return askKeyboardFor(&pendingAsk{askID: askID, options: options})
}

// askKeyboardFor builds the inline keyboard for a pendingAsk, generalizing
// askKeyboard across the Phase-2 taxonomy:
//   - one button per option, callback_data "ask:<askID>:<idx>". For a multi ask a
//     selected option is prefixed with "✓ " so the human sees current selection.
//   - a trailing "✅ Done" button (callback "ask:<askID>:done") when multi.
//   - a trailing "⏭ Skip" button (callback "ask:<askID>:skip") when allowSkip.
//
// The telegram channel enforces the 64-byte callback_data ceiling and the row
// count; with an 8-char askID the payload is "ask:"+8+":"+suffix — comfortably
// under 64 for any deliverable option count.
func askKeyboardFor(p *pendingAsk) [][]c3types.Button {
	rows := make([][]c3types.Button, 0, len(p.options)+2)
	for i, opt := range p.options {
		label := opt
		if p.multi && i < len(p.selected) && p.selected[i] {
			label = "✓ " + opt
		}
		rows = append(rows, []c3types.Button{{
			Text: label,
			Data: askCallbackData(p.askID, i),
		}})
	}
	if p.multi {
		rows = append(rows, []c3types.Button{{
			Text: "✅ Done",
			Data: askDoneData(p.askID),
		}})
	}
	if p.allowSkip {
		rows = append(rows, []c3types.Button{{
			Text: "⏭ Skip",
			Data: askSkipData(p.askID),
		}})
	}
	return rows
}

// askCallbackData formats the opaque callback payload for option idx.
func askCallbackData(askID string, idx int) string {
	return askCallbackPrefix + askID + ":" + strconv.Itoa(idx)
}

// askDoneData / askSkipData format the multi-select "Done" / "Skip" callbacks.
func askDoneData(askID string) string { return askCallbackPrefix + askID + ":" + askDoneSuffix }
func askSkipData(askID string) string { return askCallbackPrefix + askID + ":" + askSkipSuffix }

// askAction is the parsed kind of an "ask:" callback payload.
type askAction int

const (
	askActionNone  askAction = iota // not an ask callback (or malformed)
	askActionIndex                  // "ask:<id>:<idx>" — an option tap
	askActionDone                   // "ask:<id>:done"  — multi-select resolve
	askActionSkip                   // "ask:<id>:skip"  — skip resolve
)

// parseAskCallback parses an "ask:<askID>:<suffix>" callback payload into its
// (askID, action, idx). The LAST colon separates the suffix (the base32 askID has
// no colon), so it is robust against any future askID alphabet. action is
// askActionNone for a non-ask / malformed payload, so the generic event path
// proceeds untouched. idx is meaningful only for askActionIndex.
func parseAskCallback(data string) (askID string, action askAction, idx int) {
	if !strings.HasPrefix(data, askCallbackPrefix) {
		return "", askActionNone, 0
	}
	rest := data[len(askCallbackPrefix):]
	sep := strings.LastIndex(rest, ":")
	if sep <= 0 || sep == len(rest)-1 {
		return "", askActionNone, 0
	}
	id := rest[:sep]
	suffix := rest[sep+1:]
	switch suffix {
	case askDoneSuffix:
		return id, askActionDone, 0
	case askSkipSuffix:
		return id, askActionSkip, 0
	}
	n, err := strconv.Atoi(suffix)
	if err != nil || n < 0 {
		return "", askActionNone, 0
	}
	return id, askActionIndex, n
}

// parseAskData is the single-select-index view of parseAskCallback, retained for
// the keyboard round-trip test and any index-only caller. ok=false for a non-ask,
// malformed, or non-index (done/skip) payload.
func parseAskData(data string) (askID string, idx int, ok bool) {
	id, action, n := parseAskCallback(data)
	if action != askActionIndex {
		return "", 0, false
	}
	return id, n, true
}

// resolveAsk attempts to resolve (or, for a multi-select toggle, advance) a
// registered ask from an inline-keyboard callback whose Data is "ask:<askID>:…".
// It returns true when the callback matched a live ask on this route — in which
// case the caller (flushEvent) must SUPPRESS the generic event (the tap was the
// answer/toggle, not a fresh <channel> event). It returns false for a non-ask
// payload, an unknown/already-resolved askID, a route mismatch, an out-of-range
// index, or a tap that fails the binding gate (askTapBound: wrong message, or a
// route that changed hands since the question was asked), so the generic path
// proceeds; the telegram channel has already auto-acked the callback either way,
// so Telegram stops spinning regardless.
//
// Three callback forms (parseAskCallback):
//   - "ask:<id>:<idx>" → single-select resolves immediately; multi-select TOGGLES
//     selected[idx] and re-renders the keyboard in place (NOT resolved/removed).
//   - "ask:<id>:done"  → multi-select resolves with the selected option list.
//   - "ask:<id>:skip"  → resolves with AskAnswer{Skipped:true}.
//
// On a resolve it (a) pushes an OpAskResult to the route holder's conn — the SAME
// delivery path as OpInbound (survives a same-process broker reconnect via the
// transferred claim) — and (b) edits the message to record the outcome and CLEAR
// the keyboard, preventing double answers / stale taps.
func (b *Broker) resolveAsk(route RouteKey, cb *c3types.CallbackEvent) bool {
	if b == nil || b.Asks == nil || cb == nil {
		return false
	}
	askID, action, idx := parseAskCallback(cb.Data)
	if action != askActionNone && !b.askTapBound(route, askID, cb) {
		return false
	}
	switch action {
	case askActionIndex:
		return b.resolveAskIndex(route, askID, idx)
	case askActionDone:
		return b.resolveAskDone(route, askID)
	case askActionSkip:
		return b.resolveAskSkip(route, askID)
	default:
		return false
	}
}

// askTapBound gates ALL THREE resolve paths (index / done / skip) on the two
// bindings a bare askID does not carry. One gate, run before any of them, so a
// mis-bound tap cannot toggle a multi-select's selection state on the way to
// being refused.
//
//   - MESSAGE: the tap must come from the message the keyboard was RENDERED on.
//     An agent can author a reply button with arbitrary callback_data
//     (buttonsFromArgs applies no namespace filter, and worker.go notes that the
//     "ask:" prefix is one an agent may legitimately reuse), so a planted
//     "ask:<liveID>:<n>" elsewhere in the topic would otherwise rewrite the
//     question's durable body to "✅ <option>" — a fabricated consent record —
//     and hand the asking agent an answer no human chose. Positive mismatch ONLY:
//     messageID is 0 between register and setMessageID (the deliberate fast-tap
//     race) and cb.MessageID is 0 on a channel that carries none.
//   - OWNER: the answer must reach the session that ASKED (see pendingAsk.owner).
//
// Refusing returns false, which is exactly what any unmatched "ask:" tap does
// today — the generic event path proceeds (worker.go deliberately lets
// unresolved ask taps through, unlike "perm:"). The ask stays REGISTERED so the
// asking session is still answerable if it re-claims the route before
// askExpiryTTL, and the reaper clears the keyboard if it never does.
func (b *Broker) askTapBound(route RouteKey, askID string, cb *c3types.CallbackEvent) bool {
	owner, msgID, ok := b.Asks.bindingOf(askID)
	if !ok {
		return true // unknown id — the resolve path below reports it, not us
	}
	if msgID != 0 && cb.MessageID != 0 && msgID != cb.MessageID {
		log.Printf("ask MSG-MISMATCH chan=%s chat=%d topic=%s ask=%s want_msg=%d got_msg=%d: tap not bound to the question message",
			route.Channel, route.ChatID, TopicKeyStr(route), askID, msgID, cb.MessageID)
		return false
	}
	if !b.ownerStillHolds(owner, route) {
		log.Printf("ask STALE-OWNER chan=%s chat=%d topic=%s ask=%s: route changed hands since the question was asked — answer NOT delivered",
			route.Channel, route.ChatID, TopicKeyStr(route), askID)
		return false
	}
	return true
}

// resolveAskIndex handles an option tap. Single-select resolves with the chosen
// option; multi-select toggles in place and re-renders the keyboard, keeping the
// ask registered so later toggles / Done can resolve it.
func (b *Broker) resolveAskIndex(route RouteKey, askID string, idx int) bool {
	res := b.Asks.tapIndex(askID, route, idx)
	if !res.match {
		// Unknown / already-resolved / expired / wrong route / out-of-range.
		// Already auto-acked by the channel; fall through to the generic path.
		return false
	}
	if !res.resolved {
		// Multi-select toggle: re-render the keyboard (text stays the question); the
		// ask stays registered. The keyboard markup changed (✓ toggled) so Telegram
		// accepts the edit even though the text is unchanged.
		b.editAskMessage(route, askID, res.messageID, res.question, res.kb)
		log.Printf("ask TOGGLE chan=%s chat=%d topic=%s ask=%s idx=%d",
			route.Channel, route.ChatID, TopicKeyStr(route), askID, idx)
		return true
	}
	// Single-select resolve.
	b.deliverAskResult(route, askID, ipc.AskAnswer{Selected: []string{res.chosen}})
	b.editAskMessage(route, askID, res.messageID, askAnsweredText(res.question, res.chosen), [][]c3types.Button{})
	log.Printf("ask RESOLVED chan=%s chat=%d topic=%s ask=%s idx=%d",
		route.Channel, route.ChatID, TopicKeyStr(route), askID, idx)
	return true
}

// resolveAskDone resolves a multi-select ask with the selected option list. (The
// Done button only renders for a multi ask; for any other ask the selection is
// empty, which is the correct defensive outcome.)
func (b *Broker) resolveAskDone(route RouteKey, askID string) bool {
	p, ok := b.Asks.take(askID)
	if !ok {
		return false
	}
	if p.route != route {
		// registerAsk, not the raw registry call: at the size cap a re-register
		// evicts the oldest ask, and only registerAsk clears that entry's
		// now-orphaned keyboard.
		b.registerAsk(p)
		return false
	}
	selected := selectedOptions(p)
	b.deliverAskResult(route, askID, ipc.AskAnswer{Selected: selected})
	b.editAskMessage(route, askID, p.messageID, askDoneText(p.question, selected), [][]c3types.Button{})
	log.Printf("ask RESOLVED(done) chan=%s chat=%d topic=%s ask=%s selected=%d",
		route.Channel, route.ChatID, TopicKeyStr(route), askID, len(selected))
	return true
}

// resolveAskSkip resolves an ask with AskAnswer{Skipped:true}.
func (b *Broker) resolveAskSkip(route RouteKey, askID string) bool {
	p, ok := b.Asks.take(askID)
	if !ok {
		return false
	}
	if p.route != route {
		// registerAsk, not the raw registry call — see resolveAskDone.
		b.registerAsk(p)
		return false
	}
	b.deliverAskResult(route, askID, ipc.AskAnswer{Skipped: true})
	b.editAskMessage(route, askID, p.messageID, askSkippedText(p.question), [][]c3types.Button{})
	log.Printf("ask RESOLVED(skip) chan=%s chat=%d topic=%s ask=%s",
		route.Channel, route.ChatID, TopicKeyStr(route), askID)
	return true
}

// deliverAskResult pushes an OpAskResult to the route holder's conn — identical to
// OpInbound delivery (survives a same-process broker reconnect via the transferred
// claim). Best-effort: a disconnected/absent holder is logged, not fatal (the tool
// call will time out and recover).
func (b *Broker) deliverAskResult(route RouteKey, askID string, answer ipc.AskAnswer) {
	if holder, claimed := b.Routes.Holder(route); claimed {
		if conn, ok := holder.ConnValue().(*ipc.Conn); ok && conn != nil {
			if err := conn.WriteJSON(ipc.AskResultMsg{
				Op:     ipc.OpAskResult,
				AskID:  askID,
				Answer: answer,
			}); err != nil {
				log.Printf("ask deliver FAIL chan=%s chat=%d topic=%s ask=%s: %v",
					route.Channel, route.ChatID, TopicKeyStr(route), askID, err)
			}
		} else {
			log.Printf("ask deliver DROP chan=%s chat=%d topic=%s ask=%s: holder disconnected — answer not delivered",
				route.Channel, route.ChatID, TopicKeyStr(route), askID)
		}
	} else {
		log.Printf("ask deliver DROP chan=%s chat=%d topic=%s ask=%s: no live holder — answer not delivered",
			route.Channel, route.ChatID, TopicKeyStr(route), askID)
	}
}

// editKeyboardMessage edits a route's message text + inline keyboard via the
// channel. A non-nil EMPTY buttons slice clears the keyboard (see EditMessage); a
// non-empty slice re-renders it. found=false (err set) means the channel could not
// be resolved at all (the caller stays silent, matching the original
// editAskMessage behavior); found=true with a non-nil err is a real edit failure
// the caller should log with its own feature-flavored context. Shared by the ask
// and permission keyboard flows so the channel-edit plumbing has a single home.
// markup selects how the edited body is rendered. Callers whose body embeds
// attacker- or agent-supplied text that must be read literally (the permission
// prompt's command preview) pass c3types.MarkupNone; the empty value keeps the
// channel default (markdown).
func (b *Broker) editKeyboardMessage(route RouteKey, messageID int64, text string, buttons [][]c3types.Button, markup c3types.Markup) (found bool, err error) {
	ch, cerr := b.Channel(route.Channel)
	if cerr != nil {
		return false, cerr
	}
	_, err = ch.EditMessage(c3types.EditArgs{
		Channel:   route.Channel,
		ChatID:    route.ChatID,
		MessageID: messageID,
		Text:      text,
		Markup:    markup,
		Buttons:   buttons,
	})
	return true, err
}

// editAskMessage edits the ask's Telegram message to text + buttons. Best-effort:
// a failed edit must not block an answer that was already delivered. A non-nil
// EMPTY buttons slice clears the inline keyboard (see EditMessage); a non-empty
// slice re-renders it (multi-select toggle).
func (b *Broker) editAskMessage(route RouteKey, askID string, messageID int64, text string, buttons [][]c3types.Button) {
	if found, err := b.editKeyboardMessage(route, messageID, text, buttons, ""); found && err != nil {
		log.Printf("ask edit FAIL chan=%s chat=%d topic=%s ask=%s msg=%d: %v",
			route.Channel, route.ChatID, TopicKeyStr(route), askID, messageID, err)
	}
}

// registerAsk registers p in the ask registry and, when the size cap forces an
// eviction, best-effort clears the evicted (oldest) ask's now-orphaned keyboard
// (FIX-1). Returns false on an askID collision so the caller fails the
// registration. The eviction keyboard-clear lives here, not in askRegistry,
// because clearing requires a channel edit the registry has no handle on.
func (b *Broker) registerAsk(p *pendingAsk) bool {
	// Stamp the OWNER before the entry can be tapped: the session holding p.route
	// right now is the session this question is being asked for. Only when unset,
	// so the defensive re-registers above keep the ORIGINAL owner.
	if p.owner == nil {
		if holder, ok := b.Routes.Holder(p.route); ok {
			p.owner = holder
		}
	}
	evicted, ok := b.Asks.register(p)
	if evicted != nil {
		log.Printf("ask EVICTED chan=%s chat=%d topic=%s ask=%s reason=cap(max=%d) — clearing keyboard",
			evicted.route.Channel, evicted.route.ChatID, TopicKeyStr(evicted.route), evicted.askID, maxPendingAsks)
		b.editAskMessage(evicted.route, evicted.askID, evicted.messageID, askTimedOutText(evicted.question), [][]c3types.Button{})
	}
	return ok
}

// sweepExpiredAsks removes asks older than askExpiryTTL and best-effort clears
// each one's live keyboard (FIX-1). Called by the reaper (StartAskReaper). Logs
// the count + per-ask askID/reason, never the question/option text.
func (b *Broker) sweepExpiredAsks() {
	if b == nil || b.Asks == nil {
		return
	}
	expired := b.Asks.sweepExpired(time.Now(), askExpiryTTL)
	if len(expired) == 0 {
		return
	}
	log.Printf("ask sweep: expired %d ask(s) (ttl=%v) — clearing keyboards", len(expired), askExpiryTTL)
	for _, p := range expired {
		log.Printf("ask EXPIRED chan=%s chat=%d topic=%s ask=%s reason=ttl",
			p.route.Channel, p.route.ChatID, TopicKeyStr(p.route), p.askID)
		b.editAskMessage(p.route, p.askID, p.messageID, askTimedOutText(p.question), [][]c3types.Button{})
	}
}

// StartAskReaper launches the background goroutine that periodically expires
// stale asks AND stale permission prompts (every askSweepInterval) so an untapped
// ask / abandoned permission prompt can't leak or leave a live keyboard. Mirrors
// StartHealthRefresh: panic-supervised (recover→log→continue) and stops on the
// broker context cancel (Shutdown), so it never leaks or crashes the process.
// Call once, after the broker is constructed.
func (b *Broker) StartAskReaper() {
	go func() {
		t := time.NewTicker(askSweepInterval)
		defer t.Stop()
		for {
			select {
			case <-b.ctx.Done():
				return
			case <-t.C:
				func() {
					defer recoverGoroutine("askReaper")
					b.sweepExpiredAsks()
					b.sweepExpiredPerms()
				}()
			}
		}
	}()
}

// askTimedOutText renders the post-expiry/eviction body: the original question
// plus a no-answer notice, so the Telegram view records that nothing was
// captured after the keyboard clears.
func askTimedOutText(question string) string {
	const notice = "⌛ Timed out — no answer recorded"
	if question == "" {
		return notice
	}
	return question + "\n\n" + notice
}

// selectedOptions returns the option strings currently toggled on, in option
// order. Empty (nil) when nothing is selected — a valid multi-select Done result.
func selectedOptions(p *pendingAsk) []string {
	var out []string
	for i, sel := range p.selected {
		if sel && i < len(p.options) {
			out = append(out, p.options[i])
		}
	}
	return out
}

// askAnsweredText renders the post-answer message body for a single-select
// resolve: the original question followed by a checkmark line naming the chosen
// option, so the Telegram view records what was picked after the keyboard clears.
func askAnsweredText(question, chosen string) string {
	if question == "" {
		return "✅ " + chosen
	}
	return question + "\n\n✅ " + chosen
}

// askDoneText renders the post-answer body for a multi-select Done resolve: the
// question followed by a checkmark line listing the selected options (or a
// "(none selected)" note for an empty selection).
func askDoneText(question string, selected []string) string {
	line := "✅ (none selected)"
	if len(selected) > 0 {
		line = "✅ " + strings.Join(selected, ", ")
	}
	if question == "" {
		return line
	}
	return question + "\n\n" + line
}

// askSkippedText renders the post-answer body for a Skip resolve.
func askSkippedText(question string) string {
	if question == "" {
		return "⏭ Skipped"
	}
	return question + "\n\n⏭ Skipped"
}
