package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"slices"
	"sync"
	"time"

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

// autoClassifierBlockReasons are the exact PermissionDenied reasons observed for
// auto-mode classifier blocks, per CLI. Only these get a card. Everything else
// (no-verdict and "unavailable" denials, any new form) gets no card and is
// audit-logged so the list can be extended deliberately from observed hook
// payloads. Never match by pattern.
var autoClassifierBlockReasons = map[string][]string{
	"claude": {"[Code from External]", "[Auto-Mode Bypass]", "[Credential Exploration]", "[Git Destructive]"},
}

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
// pending | approved → denied | timed_out | cancelled.
const (
	autoPending   autoState = "pending"
	autoApproved  autoState = "approved" // Allow tapped; nothing armed until a hook acks
	autoArmed     autoState = "armed"
	autoConsumed  autoState = "consumed"
	autoExpired   autoState = "expired"
	autoDenied    autoState = "denied"
	autoTimedOut  autoState = "timed_out"
	autoCancelled autoState = "cancelled"
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
	// state; every Telegram send and hook write for the request uses it.
	deadline time.Time
	ctx      context.Context
	cancel   context.CancelFunc

	grantTTL time.Duration
	expires  time.Time // set when armed

	waiters  map[*autoWaiter]struct{}
	isArming bool // one waiter is writing "armed"; no other may start

	messageID int64 // card message; taps are refused until it is recorded
	cardText  string

	editMu sync.Mutex // serializes card edits so the last one shows the latest state
}

// autoWaiter is one hook connection waiting on a request.
type autoWaiter struct {
	conn    *ipc.Conn
	isArmed bool // connected when the grant was armed, so it is told "armed"
}

type autoRegistry struct {
	mu    sync.Mutex
	byKey map[autoKey]*autoRequest
	byID  map[string]*autoRequest
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

func isClassifierBlock(cli, reason string) bool {
	return slices.Contains(autoClassifierBlockReasons[cli], reason)
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
	key := autoKey{AutoCallContext: req.AutoCallContext, inputHash: inputHash}
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
}

// dropAutoWaiter removes a hook connection that has finished or gone away. The
// request is cancelled only when no waiter is left before it is armed (§5.4).
func (b *Broker) dropAutoWaiter(r *autoRequest, waiter *autoWaiter) {
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	delete(r.waiters, waiter)
	if len(r.waiters) == 0 && (r.state == autoPending || r.state == autoApproved) {
		b.setAutoStateLocked(r, autoCancelled, "hook gone")
	}
}

// armAutoGrant runs step 3 of the handoff for the waiter that acked: write
// "armed", and only once that write has completed make the grant consumable. A
// failed write arms nothing and cancels the request. It reports whether this
// waiter's exchange is over (an armed frame was attempted); false means arming
// was not this waiter's to do, and it keeps waiting.
func (b *Broker) armAutoGrant(r *autoRequest, waiter *autoWaiter) bool {
	b.auto.mu.Lock()
	_, isWaiting := r.waiters[waiter]
	if !b.refreshAutoLocked(r) || r.state != autoApproved || r.isArming || !isWaiting {
		b.auto.mu.Unlock()
		return false
	}
	// Written outside the lock so a stuck hook socket can't stall other requests
	// or a config reload; isArming keeps a second waiter from arming meanwhile.
	r.isArming = true
	b.auto.mu.Unlock()

	ctx, cancel := context.WithTimeout(r.ctx, autoReplyTimeout)
	err := waiter.conn.WriteJSONContext(ctx, r.decision(ipc.AutoDecisionArmed))
	cancel()

	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	r.isArming = false
	if err != nil {
		if r.state.isLive() {
			b.setAutoStateLocked(r, autoCancelled, "armed write failed: "+err.Error())
		}
		return true
	}
	// Cancelled, disabled or past the deadline while writing: nothing is armed,
	// and the hook's retry gets normal permission processing.
	if !b.refreshAutoLocked(r) || r.state != autoApproved {
		return true
	}
	r.expires = time.Now().Add(r.grantTTL)
	for connected := range r.waiters {
		connected.isArmed = true
	}
	b.setAutoStateLocked(r, autoArmed, "")
	b.refreshAutoAfter(r, r.grantTTL)
	return true
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
	b.setAutoStateLocked(r, autoConsumed, "")
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
