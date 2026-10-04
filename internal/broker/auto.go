package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Andrometiq/c3/internal/ipc"
)

// Auto-mode approval. When the auto-mode classifier denies a tool call, the
// PermissionDenied hook reports it (auto_denied, auto_hook.go) and waits while
// C3 posts an Allow/Deny card on the session's Telegram route (auto_card.go).
// Allow arms ONE grant for that exact call in that exact context; the
// PreToolUse hook consumes it (grant_check) when the model retries. Allow does
// not make the model retry and C3 never learns whether the call ran.
//
// Everything is in memory. A broker restart drops every request and grant, and
// a stale card's tap is answered as expired.

// autoCallbackPrefix is the callback_data namespace of approval cards:
// "c3:auto:<allow|deny>:<request id>". It sits inside the reserved "c3:"
// namespace, so an agent-authored button can never mint it (dispatch.go). The
// telegram channel mirrors it (autoTapDataPrefix, poll.go) to defer the tap's
// ack to resolveAutoTap; keep the two in lockstep.
const autoCallbackPrefix = "c3:auto:"

// Hard-deny rules are not overridable by user intent, so C3 must not offer an override.
var autoClassifierHardDenyRules = []string{"Data Exfiltration"}

const (
	// autoHookBudgetMax is the denial hook's own end-to-end bound. A hook never
	// gets a longer deadline than this, whatever budget it claims.
	autoHookBudgetMax = 320 * time.Second
	// autoBudgetMargin is left to the hook after the broker's deadline for its
	// output and exit.
	autoBudgetMargin = 5 * time.Second
	// autoReplyTimeout bounds a single frame written to a hook.
	autoReplyTimeout = time.Second
)

type autoState string

// pending → approved → armed → consumed | expired, or
// pending | approved → denied | timed_out | cancelled. A consumed grant
// becomes vetoed when Claude Code denies the very call it allowed, or
// allow-unconfirmed when C3 can't tell that the allow reached Claude Code.
const (
	autoPending   autoState = "pending"
	autoApproved  autoState = "approved" // Allow tapped; nothing armed until a hook acks
	autoArmed     autoState = "armed"
	autoConsumed  autoState = "consumed"
	autoExpired   autoState = "expired"
	autoDenied    autoState = "denied"
	autoTimedOut  autoState = "timed_out"
	autoCancelled autoState = "cancelled"
	autoVetoed    autoState = "vetoed-after-allow"
	// autoUnconfirmed: the grant was consumed but the hook's confirmation
	// that it printed the allow never came, and the same call was denied again.
	autoUnconfirmed autoState = "allow-unconfirmed"
)

func (s autoState) isLive() bool { return s == autoPending || s == autoApproved || s == autoArmed }

// autoKey is the exact call a grant matches: the hook-reported context plus
// the canonical input hash. There is at most one live request per key, so a
// retry never has to be matched to "its" request.
type autoKey struct {
	ipc.AutoCallContext
	inputHash string
}

// autoRequest is one denial's state machine. Every field except editMu is
// guarded by autoRegistry.mu.
type autoRequest struct {
	id  string // 128-bit random, never reused
	key autoKey

	// owner is the adapter incarnation the card was issued for, and
	// claimGeneration its claim on route at that moment. A reconnect, re-attach,
	// steal or release in between invalidates the request (isAutoOwnerLive).
	owner           *Stub
	route           RouteKey
	claimGeneration uint64

	state   autoState
	changed chan struct{} // closed and replaced on every state change

	// deadline ends the hook transaction (§5.4: queueing, upload, card send,
	// wait and handoff). ctx expires with it and is cancelled at any terminal
	// state, consumed included; every Telegram send and hook write for the
	// request uses it, so a grant that ends aborts an "armed" still in flight.
	deadline time.Time
	ctx      context.Context
	cancel   context.CancelFunc

	grantTTL time.Duration
	expires  time.Time // set when armed

	waiters map[*autoWaiter]struct{}
	// isAnnounced: some hook was sent "armed" successfully. From then on the
	// grant stays until it is used or expires, whatever other waiters do.
	isAnnounced bool

	messageID int64 // card message; taps are refused until it is recorded
	cardText  string

	editMu sync.Mutex // serializes card edits so the last one shows the latest state
}

// autoWaiter is one hook connection waiting on a request. Its frames are
// decided from request state alone (awaitAutoOutcome); isDone records that it
// has written its last frame or failed to, so it can no longer announce.
type autoWaiter struct {
	conn   *ipc.Conn
	isDone bool
}

type autoRegistry struct {
	mu    sync.Mutex
	byKey map[autoKey]*autoRequest
	byID  map[string]*autoRequest
	// consumed remembers, for one grant lifetime, which tool call (by
	// tool_use_id) consumed each grant, so a denial of that same call is
	// recognised as Claude Code vetoing an allowed retry.
	consumed map[autoConsumedCall]*autoConsumedRecord
}

// autoConsumedCall is a consumed grant's key plus the tool_use_id of the call
// that consumed it. tool_use_id is never part of the grant key: it only tells
// that later call apart from a new, identical one.
type autoConsumedCall struct {
	key       autoKey
	toolUseID string
}

type autoConsumedRecord struct {
	request *autoRequest
	expires time.Time
	// confirmation is closed when handleGrantCheck stops waiting for
	// grant_delivered, with isDelivered set if the frame came.
	confirmation chan struct{}
	isDelivered  bool
}

// newAutoRequestID returns a fresh 128-bit random request id (§5.2): never
// derived from the input, never reused across requests or broker restarts.
func newAutoRequestID() (string, error) {
	entropy := make([]byte, 16)
	if _, err := rand.Read(entropy); err != nil {
		return "", err
	}
	return hex.EncodeToString(entropy), nil
}

// decision is r's decision frame in state.
func (r *autoRequest) decision(state string) ipc.AutoDecisionMsg {
	return ipc.AutoDecisionMsg{Op: ipc.OpAutoDecision, RequestID: r.id, State: state}
}

// isClassifierBlock accepts "Blocked by classifier" or [Rule Name] with an optional
// explanation, excluding hard-deny rules. No-verdict/unavailable texts cannot match
// the bracketed shape because none start with '['.
func isClassifierBlock(cli, reason string) bool {
	if cli != "claude" {
		return false
	}
	if reason == "Blocked by classifier" {
		return true
	}
	if !strings.HasPrefix(reason, "[") {
		return false
	}
	end := strings.IndexByte(reason, ']')
	if end < 2 || end > 65 {
		return false
	}
	name := reason[1:end]
	if name[0] == ' ' || name[len(name)-1] == ' ' || strings.ContainsRune(name, '[') {
		return false
	}
	for _, char := range name {
		if unicode.IsControl(char) {
			return false
		}
	}
	if slices.Contains(autoClassifierHardDenyRules, name) {
		return false
	}
	suffix := reason[end+1:]
	if suffix == "" {
		return true
	}
	explanation, isSpaced := strings.CutPrefix(suffix, " ")
	first, _ := utf8.DecodeRuneInString(explanation)
	return isSpaced && first != utf8.RuneError && !unicode.IsSpace(first)
}

// sessionAdapter returns the one connected adapter whose stable session id
// (registered by handleRecoverSession) is sessionID under this CLI. None or
// several ⇒ nil: a grant must belong to exactly one live adapter, never to an
// attachment record or a handoff file.
func (b *Broker) sessionAdapter(cli, sessionID string) *Stub {
	var found *Stub
	for _, stub := range b.Stubs.Snapshot() {
		if stub.CLI != cli || stub.StableSessionIDValue() != sessionID || !stub.IsConnected() {
			continue
		}
		if found != nil {
			return nil
		}
		found = stub
	}
	return found
}

// confirmedClaimGeneration reports owner's claim generation on route, provided
// the routes table says owner holds route and the claim is confirmed.
func (b *Broker) confirmedClaimGeneration(owner *Stub, route RouteKey) (uint64, bool) {
	holder, isHeld := b.Routes.Holder(route)
	if !isHeld || holder != owner || !owner.RouteConfirmed(route) {
		return 0, false
	}
	return owner.claimGeneration(route), true
}

// resolveAutoRoute picks the route a card for owner goes to (§5.5): the output
// route when it is Telegram, else the only Telegram route held. Web-only and
// ambiguous sessions get no card.
func (b *Broker) resolveAutoRoute(owner *Stub) (RouteKey, uint64, bool) {
	routes, output := owner.RouteSnapshot()
	var selected *RouteKey
	if output != nil && output.Channel == "telegram" {
		selected = output
	} else {
		for _, route := range routes {
			if route.Channel != "telegram" {
				continue
			}
			if selected != nil {
				return RouteKey{}, 0, false
			}
			selected = &route
		}
	}
	if selected == nil {
		return RouteKey{}, 0, false
	}
	generation, ok := b.confirmedClaimGeneration(owner, *selected)
	return *selected, generation, ok
}

// isAutoOwnerLive is the one ownership gate after a request is created: the
// adapter the card was issued for is still the only live adapter for the
// session, and still holds the same confirmed claim on the card's route.
func (b *Broker) isAutoOwnerLive(r *autoRequest) bool {
	if b.sessionAdapter(r.key.CLI, r.key.SessionID) != r.owner {
		return false
	}
	generation, ok := b.confirmedClaimGeneration(r.owner, r.route)
	return ok && generation == r.claimGeneration
}

// openAutoRequest attaches waiter to the live request for this call, creating
// the request (isNew: the caller sends its card) when there is none. A nil
// request means no card: refusal says why for the audit log, and is empty when
// the feature is off.
func (b *Broker) openAutoRequest(req ipc.AutoDeniedReq, inputHash string, waiter *autoWaiter,
	received time.Time) (r *autoRequest, isNew bool, refusal string) {
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	settings := b.Mappings().AutoModeApprovalSettings()
	if !settings.Enabled {
		return nil, false, ""
	}
	key := autoKey{AutoCallContext: req.AutoCallContext, inputHash: inputHash}
	if b.vetoAutoLocked(autoConsumedCall{key: key, toolUseID: req.ToolUseID}) {
		return nil, false, "vetoed after allow"
	}
	if !isClassifierBlock(req.CLI, req.Reason) {
		return nil, false, "unknown reason"
	}
	owner := b.sessionAdapter(req.CLI, req.SessionID)
	if owner == nil {
		return nil, false, "no single live adapter for the session"
	}
	route, generation, ok := b.resolveAutoRoute(owner)
	if !ok {
		return nil, false, "no unique confirmed Telegram route"
	}
	if existing := b.auto.byKey[key]; existing != nil && b.refreshAutoLocked(existing) {
		if existing.state == autoArmed {
			// The retry it armed is still usable; a second one is not granted.
			return nil, false, "grant already armed for this call"
		}
		existing.waiters[waiter] = struct{}{}
		return existing, false, ""
	}
	undecided := 0
	for _, other := range b.auto.byKey {
		if other.route == route && b.refreshAutoLocked(other) && other.state != autoArmed {
			undecided++
		}
	}
	if undecided >= settings.MaxPending {
		return nil, false, "max_pending reached on route " + routeKeyStr(route)
	}
	budget := time.Duration(min(req.BudgetMS, autoHookBudgetMax.Milliseconds()))*time.Millisecond - autoBudgetMargin
	deadline := received.Add(min(time.Duration(settings.WaitSeconds)*time.Second, budget))
	if !time.Now().Before(deadline) {
		return nil, false, "hook budget exhausted"
	}
	id, err := newAutoRequestID()
	if err != nil {
		return nil, false, "request id: " + err.Error()
	}
	ctx, cancel := context.WithDeadline(b.ctx, deadline)
	r = &autoRequest{
		id:              id,
		key:             key,
		owner:           owner,
		route:           route,
		claimGeneration: generation,
		state:           autoPending,
		changed:         make(chan struct{}),
		deadline:        deadline,
		ctx:             ctx,
		cancel:          cancel,
		grantTTL:        time.Duration(settings.GrantTTLSeconds) * time.Second,
		waiters:         map[*autoWaiter]struct{}{waiter: {}},
	}
	if b.auto.byKey == nil {
		b.auto.byKey = map[autoKey]*autoRequest{}
		b.auto.byID = map[string]*autoRequest{}
	}
	b.auto.byKey[key] = r
	b.auto.byID[r.id] = r
	b.auditAuto(r, "created", 0, "")
	b.refreshAutoAfter(r, time.Until(deadline))
	return r, true, ""
}

// refreshAutoLocked re-checks a live request against the feature switch, its
// owner, its deadline and its grant expiry, recording any lapse. It reports
// whether the request is still live. Every use of a request goes through it,
// so expiry is enforced at use time, not only when a timer or sweep fires.
func (b *Broker) refreshAutoLocked(r *autoRequest) bool {
	if !r.state.isLive() {
		return false
	}
	now := time.Now()
	switch {
	case b.ctx.Err() != nil:
		b.setAutoStateLocked(r, autoCancelled, "broker stopping")
	case !b.Mappings().AutoModeApprovalSettings().Enabled:
		b.setAutoStateLocked(r, autoCancelled, "feature disabled")
	case !b.isAutoOwnerLive(r):
		b.setAutoStateLocked(r, autoCancelled, "session no longer holds the route")
	case r.state == autoArmed && !now.Before(r.expires):
		b.setAutoStateLocked(r, autoExpired, "")
	case r.state != autoArmed && !now.Before(r.deadline):
		b.setAutoStateLocked(r, autoTimedOut, "")
	default:
		return true
	}
	return false
}

// refreshAutoAfter re-checks r once delay has passed, so a deadline or grant
// expiry is recorded (and its card edited) even if nothing else touches r.
func (b *Broker) refreshAutoAfter(r *autoRequest, delay time.Duration) {
	time.AfterFunc(delay, func() {
		defer recoverGoroutine("autoRefresh")
		b.auto.mu.Lock()
		defer b.auto.mu.Unlock()
		b.refreshAutoLocked(r)
	})
}

// setAutoStateLocked moves a live request to state. A terminal state removes
// the request, so its id and key can never resolve again.
func (b *Broker) setAutoStateLocked(r *autoRequest, state autoState, detail string) {
	r.state = state
	close(r.changed)
	r.changed = make(chan struct{})
	b.auditAuto(r, string(state), 0, detail)
	if !state.isLive() {
		if b.auto.byKey[r.key] == r {
			delete(b.auto.byKey, r.key)
		}
		delete(b.auto.byID, r.id)
		r.cancel()
	}
	if state != autoPending && state != autoApproved {
		go b.editAutoCard(r)
	}
}

// cancelAuto cancels every live request owned by owner (nil: every request),
// limited to route when it is non-nil. Called when a session detaches or its
// adapter goes away.
func (b *Broker) cancelAuto(owner *Stub, route *RouteKey, detail string) {
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	b.cancelAutoLocked(owner, route, detail)
}

func (b *Broker) cancelAutoLocked(owner *Stub, route *RouteKey, detail string) {
	for _, r := range b.auto.byKey {
		if (owner == nil || r.owner == owner) && (route == nil || r.route == *route) {
			b.setAutoStateLocked(r, autoCancelled, detail)
		}
	}
}

// refreshAutoRoute re-checks the requests whose card is on route. Every route
// release (detach, steal, attach switch, identity switch, dead-holder
// displacement) reaches it through the presence queue, outside Routes.mu, so a
// request whose owner lost the route is cancelled promptly, not at the next
// sweep.
func (b *Broker) refreshAutoRoute(route RouteKey) {
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	for _, r := range b.auto.byKey {
		if r.route == route {
			b.refreshAutoLocked(r)
		}
	}
}

// sweepAuto records lapses nothing else has noticed yet. Run by the reaper.
func (b *Broker) sweepAuto() {
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	for _, r := range b.auto.byKey {
		b.refreshAutoLocked(r)
	}
	now := time.Now()
	for call, record := range b.auto.consumed {
		if !now.Before(record.expires) {
			delete(b.auto.consumed, call)
		}
	}
}

// dropAutoWaiter removes a hook connection that has finished or gone away. A
// request is cancelled when no waiter is left before it is armed (§5.4), or,
// once armed, when no hook was told and none is left to tell (settleAutoLocked).
func (b *Broker) dropAutoWaiter(r *autoRequest, waiter *autoWaiter) {
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	delete(r.waiters, waiter)
	if len(r.waiters) == 0 && (r.state == autoPending || r.state == autoApproved) {
		b.setAutoStateLocked(r, autoCancelled, "hook gone")
		return
	}
	b.settleAutoLocked(r, "")
}

// armAutoGrant arms the grant on a valid ack from a waiter that was sent
// "approved". It is only a state change: every waiter, the acking one
// included, then announces "armed" itself (awaitAutoOutcome). Arming comes
// first because the hook prints retry:true as soon as it reads "armed", and
// the retry's grant_check must already find the grant.
func (b *Broker) armAutoGrant(r *autoRequest, waiter *autoWaiter) {
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	if _, isWaiting := r.waiters[waiter]; !isWaiting || !b.refreshAutoLocked(r) || r.state != autoApproved {
		return
	}
	r.expires = time.Now().Add(r.grantTTL)
	b.setAutoStateLocked(r, autoArmed, "")
	b.refreshAutoAfter(r, r.grantTTL)
}

// finishAutoAnnouncement records one waiter's "armed" write. A success keeps
// the grant for good; a failure counts against it only if no other waiter
// announced it or still can.
func (b *Broker) finishAutoAnnouncement(r *autoRequest, waiter *autoWaiter, err error) {
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	waiter.isDone = true
	if err == nil {
		r.isAnnounced = true
		return
	}
	b.settleAutoLocked(r, err.Error())
}

// settleAutoLocked cancels an armed, unused grant that no hook was told about
// and no remaining waiter can still announce. One waiter's failure never
// cancels a grant another waiter announced or may yet announce.
func (b *Broker) settleAutoLocked(r *autoRequest, lastError string) {
	if r.state != autoArmed || r.isAnnounced {
		return
	}
	for waiter := range r.waiters {
		if !waiter.isDone {
			return
		}
	}
	detail := "no hook could be told"
	if lastError != "" {
		detail += ": " + lastError
	}
	b.setAutoStateLocked(r, autoCancelled, detail)
}

// consumeAutoGrant atomically consumes the armed grant matching this call. It
// is true at most once per grant, and only before the grant expires.
func (b *Broker) consumeAutoGrant(req ipc.GrantCheckReq) bool {
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	r := b.auto.byKey[autoKey{AutoCallContext: req.AutoCallContext, inputHash: req.InputHash}]
	if r == nil || !b.refreshAutoLocked(r) || r.state != autoArmed {
		return false
	}
	b.setAutoStateLocked(r, autoConsumed, "tool_use_id="+req.ToolUseID)
	if req.ToolUseID != "" {
		if b.auto.consumed == nil {
			b.auto.consumed = map[autoConsumedCall]*autoConsumedRecord{}
		}
		call := autoConsumedCall{key: r.key, toolUseID: req.ToolUseID}
		b.auto.consumed[call] = &autoConsumedRecord{request: r, expires: time.Now().Add(r.grantTTL),
			confirmation: make(chan struct{})}
	}
	return true
}

// finishAutoConfirmation ends the wait for call's grant_delivered, recording
// whether it came. handleGrantCheck calls it exactly once per consumed call.
func (b *Broker) finishAutoConfirmation(call autoConsumedCall, isDelivered bool) {
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	if record := b.auto.consumed[call]; record != nil {
		record.isDelivered = isDelivered
		close(record.confirmation)
	}
}

// awaitAutoConfirmation lets an open grant_delivered wait for call finish
// before a denial of call is classified, so a confirmation still in flight on
// the grant_check connection is not mistaken for a missing one. It waits at
// most grantDeliveredWait and never holds b.auto.mu while waiting.
func (b *Broker) awaitAutoConfirmation(call autoConsumedCall) {
	if call.toolUseID == "" {
		return
	}
	b.auto.mu.Lock()
	record := b.auto.consumed[call]
	b.auto.mu.Unlock()
	if record == nil {
		return
	}
	timer := time.NewTimer(grantDeliveredWait)
	defer timer.Stop()
	select {
	case <-record.confirmation:
	case <-timer.C:
	case <-b.ctx.Done():
	}
}

// vetoAutoLocked reports whether call is a tool call that consumed a grant
// within the last grant lifetime and whose allow reached Claude Code. If so,
// Claude Code allowed it through the hook and then denied it anyway (a hook
// allow need not bypass the classifier). The call did not run: the original
// card says so, and no new card is posted for a retry the operator already
// approved.
//
// If the hook's confirmation never came (it ran out of time, or predates
// grant_delivered), C3 can't tell that the allow reached Claude Code: the old
// request is closed as allow-unconfirmed and the denial gets a fresh card, so
// the operator can approve again. Callers run awaitAutoConfirmation first.
func (b *Broker) vetoAutoLocked(call autoConsumedCall) bool {
	record := b.auto.consumed[call]
	if call.toolUseID == "" || record == nil {
		return false
	}
	delete(b.auto.consumed, call)
	if !time.Now().Before(record.expires) {
		return false
	}
	if !record.isDelivered {
		b.setAutoStateLocked(record.request, autoUnconfirmed,
			"tool_use_id="+call.toolUseID+": no grant_delivered from the hook; asking again")
		return false
	}
	b.setAutoStateLocked(record.request, autoVetoed, "tool_use_id="+call.toolUseID)
	return true
}

// auditAuto logs one lifecycle event (§5.11). It never logs input text: the
// input is identified by its hash. detail carries refusal causes and channel
// errors, never input.
func (b *Broker) auditAuto(r *autoRequest, event string, actor int64, detail string) {
	log.Printf("auto-approval %s id=%s route=%s tool=%q hash=%s actor=%d detail=%q",
		event, r.id, routeKeyStr(r.route), r.key.ToolName, r.key.inputHash, actor, truncateRunes(detail, 300))
}

// truncateRunes caps s at limit runes for a log line.
func truncateRunes(s string, limit int) string {
	if runes := []rune(s); len(runes) > limit {
		return string(runes[:limit]) + "…"
	}
	return s
}
