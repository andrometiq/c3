package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

const recoverRespTimeout = 8 * time.Second

// recoverSettleBudget bounds how long a bare attach waits for the recovery
// round-trip to FINISH before refusing with a retryable answer. Deliberately
// longer than
// recoverRespTimeout so it is a genuine backstop rather than the normal exit:
// every ordinary failure inside fireRecover (write error, broker error, response
// timeout) settles the gate by itself well inside this window, so reaching this
// budget means the recovery goroutine is wedged somewhere it should not be. An
// explicit attach keeps waiting for that already-bounded attempt rather than
// racing it. A var (not a const) so tests can shorten it.
var recoverSettleBudget = recoverRespTimeout + 2*time.Second

var errIdentityStillResolving = errors.New("identity still resolving; retry attach")

// identityEpochSnapshot binds one recovery answer to the broker connection it
// describes. A caller that waited on an older gate may not re-read currentConn
// and accidentally write on a newer connection whose identity is still open.
type identityEpochSnapshot struct {
	conn *ipc.Conn
	gate chan struct{}
}

// trySessionRecover fires OpRecoverSession once after hello so a resumed Grok
// session silently re-claims its last topic (Claude parity, Grok-flavored:
// stable id is the Grok session UUID from env / active_sessions.json).
func (a *adapter) trySessionRecover(ctx context.Context) {
	sid := a.stableSessionID()
	if sid == "" {
		log.Printf("recover-session: no Grok session id yet — skip auto-attach (will register id on first attach)")
		return
	}
	// Mark the identity question OPEN before firing, so an attach arriving in the
	// same instant waits for the answer instead of racing it.
	a.recoverStarted.Store(true)
	go a.fireRecover(ctx, sid, a.cwd())
}

// stableSessionID returns the Grok session UUID this adapter is bound to — the
// leader-bound id when set, else the env / active_sessions.json resolution.
// leader.sessionID is written under leader.mu everywhere (connectLocked,
// fireRecover, bindSessionIDForAttach), so the read takes the same mutex: an
// unlocked read of a Go string racing those writers is a torn-read data race
// (pointer/length pair). leader.mu guards field state only (I/O runs under
// leader.ioMu), so this never waits behind an in-flight Inject's socket I/O;
// every caller runs on its own goroutine (trySessionRecover, the MCP attach
// handler, refireRecoverOnReconnect's goroutine) — never on brokerReader.
func (a *adapter) stableSessionID() string {
	if a.leader != nil {
		a.leader.mu.Lock()
		sid := a.leader.sessionID
		a.leader.mu.Unlock()
		if sid != "" {
			return sid
		}
	}
	return resolveGrokSessionID()
}

// cwd returns the working directory to report on recover ops — the
// leader-bound cwd when set (read under leader.mu, same contract as
// stableSessionID), else env / process cwd.
func (a *adapter) cwd() string {
	if a.leader != nil {
		a.leader.mu.Lock()
		cwd := a.leader.cwd
		a.leader.mu.Unlock()
		if cwd != "" {
			return cwd
		}
	}
	if v := os.Getenv("C3_GROK_CWD"); v != "" {
		return v
	}
	cwd, _ := os.Getwd()
	return cwd
}

func (a *adapter) fireRecover(ctx context.Context, stableID, cwd string) {
	a.fireRecoverEpoch(ctx, stableID, cwd, a.currentIdentityEpoch())
}

func (a *adapter) fireRecoverEpoch(ctx context.Context, stableID, cwd string, epoch identityEpochSnapshot) {
	if stableID == "" {
		return
	}
	// RecoverSessionResp has no request id. Serialize complete attempts so an
	// old connection's waiter can never overwrite or steal the fresh
	// connection's singleton response slot.
	a.recoverMu.Lock()
	defer a.recoverMu.Unlock()
	if !a.claimRecoveryEpoch(epoch) {
		return
	}
	// The epoch-claim winner OWNS the identity question, so it answers it on EVERY exit
	// path below — recovered, registered, refused, write-failed, timed out. A
	// session that could not be identified is a settled answer ("nobody"), not a
	// session that blocks attaches until a budget expires: a hung attach is a
	// worse failure than an unidentified one. (The claim loser deliberately does
	// NOT settle — the winner is still working, and letting the loser answer for
	// it is the exact entry-read-as-completion mistake this guards against.)
	defer a.settleIdentity(epoch.gate)

	// Ensure inject targets this session.
	if a.leader != nil {
		a.leader.mu.Lock()
		a.leader.sessionID = stableID
		if cwd != "" {
			a.leader.cwd = cwd
		}
		a.leader.mu.Unlock()
	}

	respCh := make(chan ipc.RecoverSessionResp, 1)
	a.rsmu.Lock()
	a.rsPending = respCh
	a.rsmu.Unlock()
	defer func() {
		a.rsmu.Lock()
		if a.rsPending == respCh {
			a.rsPending = nil
		}
		a.rsmu.Unlock()
	}()

	conn := epoch.conn
	if conn == nil {
		// NOTHING WAS SENT — so nothing was registered, and holding the
		// once-per-connection guard here wedges the session permanently: every
		// later ensureStableSessionRegistered short-circuits on a recovery that
		// never happened, the broker never learns this session's stable id, and
		// every subsequent attach is recorded against nothing. Release it.
		//
		// Releasing does not re-open the identity gate (deliberately — see
		// identitySettled): with no broker connection, toolAttach itself answers
		// "broker reconnecting — retry attach in a moment", so no attach can be
		// answered in this window anyway, and the reconnect path re-fires.
		a.releaseRecoveryEpoch(epoch)
		log.Printf("recover-session: no broker connection — nothing sent; releasing the once-per-connection guard so a later attempt can register this session")
		return
	}
	if err := conn.WriteJSON(ipc.RecoverSessionReq{
		Op: ipc.OpRecoverSession, StableSessionID: stableID, CWD: cwd,
	}); err != nil {
		// Same as conn == nil: the request never reached the broker, so the guard
		// must not stay set on a recovery that did not happen.
		a.releaseRecoveryEpoch(epoch)
		log.Printf("recover-session: write failed: %v (nothing sent — guard released for a later attempt)", err)
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-time.After(recoverRespTimeout):
		log.Printf("recover-session: no response within %v", recoverRespTimeout)
		return
	case resp := <-respCh:
		if resp.Err != "" {
			log.Printf("recover-session: broker err: %s", resp.Err)
			return
		}
		// Even when Recovered=false, broker has bound stable id on this stub —
		// future attaches will record session attachment for next resume.
		if !resp.Recovered {
			log.Printf("recover-session: session=%s registered (no prior attachment to re-claim)", stableID)
			return
		}
		a.rememberAttach(rememberedIdentityReq(cwd, resp.ChatID, resp.TopicID, resp.Group))
		a.setAttachedTopic(resp.Name)
		log.Printf("recover-session: auto-attached to %q (queued=%d)", resp.Name, resp.QueuedCount)
		if text := renderGrokRecoverNotice(resp); text != "" {
			a.emitRecoverNotice(text)
		}
	}
}

func (a *adapter) dispatchRecoverSessionResult(raw []byte) {
	var resp ipc.RecoverSessionResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		return
	}
	a.rsmu.Lock()
	ch := a.rsPending
	a.rsmu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- resp:
	default:
	}
}

func renderGrokRecoverNotice(resp ipc.RecoverSessionResp) string {
	name := resp.Name
	if name == "" {
		return ""
	}
	if resp.QueuedCount > 0 {
		noun := "message"
		if resp.QueuedCount != 1 {
			noun = "messages"
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "C3: auto-attached to %q (resumed session). ~%d %s held — call fetch_queue (limit:\"all\") to drain:",
			name, resp.QueuedCount, noun)
		for _, it := range resp.QueuedSummary {
			preview := it.Preview
			if preview == "" {
				preview = "(" + it.Kind + ")"
			}
			fmt.Fprintf(&sb, "\n  • [%d] %s %s: %s", it.MessageID, it.Sender, it.Kind, preview)
		}
		return sb.String()
	}
	return fmt.Sprintf("C3: auto-attached to %q (resumed session). Live Telegram inject is active.", name)
}

func (a *adapter) emitRecoverNotice(text string) {
	if a.transport == nil || text == "" {
		return
	}
	if err := a.transport.Notify(context.Background(), "notifications/message", map[string]any{
		"level":  "info",
		"logger": "c3",
		"data":   text,
	}); err != nil {
		log.Printf("recover notice notify failed: %v — %s", err, text)
	}
}

// refireRecoverOnReconnect re-registers this session's stable id on a FRESH
// broker connection (§3d2, ported from the Claude adapter). A broker RESTART
// (self-update / rebuild) yields a fresh stub with no stable id and no
// reconnect-transfer, so without this the new broker never learns the sid:
// recordSessionAttachment no-ops on an empty stable id, post-restart attach
// changes are never recorded, and a later Grok resume either silently
// re-attaches to the STALE pre-restart topic or finds nothing to recover. It
// demotes the recoverFired guard from once-per-process to once-per-connection
// (reset + re-fire) — in a goroutine, because fireRecover blocks on the
// RecoverSessionResp that brokerReader (whose recovery loop calls this) must
// be free to read, and stableSessionID() may briefly contend on leader.mu.
//
// Ordering is safe: replayLastAttach's synchronous write already put the
// replayed attach on the wire before this fires, and the broker's same-conn
// serial dispatch processes that attach FIRST — so the recover takes the
// record-only branch when the replay restored the route (and the gated
// own-route recover when the replay's proposal was discarded).
func (a *adapter) refireRecoverOnReconnect(ctx context.Context) {
	epoch := a.prepareReconnectRecoveryEpoch()
	sid := a.stableSessionID()
	if sid == "" {
		a.settleIdentity(epoch.gate)
		return
	}
	cwd := a.cwd()
	a.recoverStarted.Store(true)
	go func() {
		a.fireRecoverEpoch(ctx, sid, cwd, epoch)
	}()
}

// ensureStableSessionRegistered tells the broker this stub's stable session id
// (so attach records session attachment for resume) without claiming a route,
// and — because toolAttach calls it synchronously before it writes the attach —
// it is where the "identity before anything that depends on it" rule is enforced
// for Grok: every path out of here has either ANSWERED the identity question or
// waited for the answer.
func (a *adapter) ensureStableSessionRegistered(ctx context.Context, bare bool) (*ipc.Conn, error) {
	sid := a.stableSessionID()
	if sid == "" {
		// No id to register — but a recovery started earlier may still be in
		// flight, and the attach must not overtake it.
		return a.connAfterIdentitySettled(ctx, bare)
	}
	// fireRecover is once per BROKER CONNECTION (refireRecoverOnReconnect resets
	// the epoch guard after a reconnect), so calling it unconditionally is safe
	// and duplicate-free.
	a.recoverStarted.Store(true)
	a.fireRecover(ctx, sid, a.cwd())
	// A no-op RETURN is not an answer, though. The old code read recoverFired
	// (ENTRY into fireRecover) as "already registered" and returned straight into
	// the attach write — so an attach could be answered while the recover that
	// would have told the broker who this session is was still in flight, and the
	// broker answered attachBare from no identity. Wait for the answer.
	return a.connAfterIdentitySettled(ctx, bare)
}

// identityGate returns the channel that closes when this session's identity
// question has been answered, creating it on first use. Lazy so an adapter built
// as a bare struct literal (as several tests do) behaves like one from
// newAdapter instead of waiting on a nil channel forever.
func (a *adapter) identityGate() chan struct{} {
	a.idmu.Lock()
	defer a.idmu.Unlock()
	if a.identitySettled == nil {
		a.identitySettled = make(chan struct{})
	}
	return a.identitySettled
}

func (a *adapter) rearmIdentity() {
	a.idmu.Lock()
	old := a.identitySettled
	a.identitySettled = make(chan struct{})
	a.recoverFiredGate = nil
	a.recoverFired.Store(false)
	a.identityReconnect = false
	a.idmu.Unlock()
	if old != nil {
		a.settleIdentity(old)
	}
}

// beginReconnectIdentityEpoch makes the new unanswered identity visible before
// conn can point at the replacement broker. Closing the superseded gate merely
// wakes old waiters; connAfterIdentitySettled revalidates and chases the new
// epoch before returning any connection.
func (a *adapter) beginReconnectIdentityEpoch() {
	a.recoverStarted.Store(true)
	// Publish {no connection, new open gate} atomically under the same lock
	// order currentIdentityEpoch uses. Otherwise a caller can observe {old conn,
	// new gate}, let an old-stub recovery settle that gate, and later reuse its
	// answer against the replacement connection.
	a.idmu.Lock()
	a.bmu.Lock()
	old := a.conn
	oldGate := a.identitySettled
	a.identitySettled = make(chan struct{})
	a.recoverFiredGate = nil
	a.recoverFired.Store(false)
	a.identityReconnect = true
	a.brokerHelloPending = true
	a.conn = nil
	a.bmu.Unlock()
	a.idmu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	if oldGate != nil {
		a.settleIdentity(oldGate)
	}
}

// prepareReconnectRecoveryEpoch preserves the epoch opened by reconnectBroker.
// Direct tests and defensive callers that invoke refire without that transition
// still get a fresh epoch when the current gate is already settled. An open,
// claimed epoch is the recovery already running for this connection and must be
// reused; rearming it would create two uncorrelated recoveries on one conn.
func (a *adapter) prepareReconnectRecoveryEpoch() identityEpochSnapshot {
	a.idmu.Lock()
	gate := a.identitySettled
	a.identityReconnect = false // hello completed; this epoch may now register
	settled := gate == nil
	if gate != nil {
		select {
		case <-gate:
			settled = true
		default:
		}
	}
	a.idmu.Unlock()
	if settled {
		a.rearmIdentity()
	}
	return a.currentIdentityEpoch()
}

func (a *adapter) currentIdentityEpoch() identityEpochSnapshot {
	// One lock order everywhere that needs the pair: identity, then connection.
	a.idmu.Lock()
	a.bmu.Lock()
	epoch := identityEpochSnapshot{conn: a.conn, gate: a.identitySettled}
	if epoch.gate == nil {
		epoch.gate = make(chan struct{})
		a.identitySettled = epoch.gate
	}
	a.bmu.Unlock()
	a.idmu.Unlock()
	return epoch
}

func (a *adapter) identityEpochCurrent(epoch identityEpochSnapshot) bool {
	a.idmu.Lock()
	a.bmu.Lock()
	current := a.identitySettled == epoch.gate && a.conn == epoch.conn
	a.bmu.Unlock()
	a.idmu.Unlock()
	return current
}

func (a *adapter) claimRecoveryEpoch(epoch identityEpochSnapshot) bool {
	a.idmu.Lock()
	defer a.idmu.Unlock()
	a.bmu.Lock()
	currentPair := a.identitySettled == epoch.gate && a.conn == epoch.conn && !a.identityReconnect
	a.bmu.Unlock()
	if !currentPair {
		return false
	}
	// Tests and legacy setup sometimes seed only the atomic guard. Treat that as
	// a claim on the current epoch; normal production claims also record gate.
	if a.recoverFiredGate == epoch.gate || (a.recoverFired.Load() && a.recoverFiredGate == nil) {
		return false
	}
	a.recoverFiredGate = epoch.gate
	a.recoverFired.Store(true)
	return true
}

func (a *adapter) releaseRecoveryEpoch(epoch identityEpochSnapshot) {
	a.idmu.Lock()
	defer a.idmu.Unlock()
	if a.recoverFiredGate != epoch.gate {
		return
	}
	a.recoverFiredGate = nil
	a.recoverFired.Store(false)
}

func (a *adapter) markIdentitySettled() {
	a.settleIdentity(a.identityGate())
}

func (a *adapter) settleIdentity(gate chan struct{}) {
	a.idmu.Lock()
	defer a.idmu.Unlock()
	select {
	case <-gate:
	default:
		close(gate)
	}
}

// awaitIdentitySettled blocks until this session's identity question has been
// ANSWERED. Returns immediately when no recovery was ever started. A canceled
// caller aborts. At the settle budget a bare attach is refused because its
// answer depends on identity; an explicit target keeps waiting for the bounded
// recovery attempt and is never raced against a late recover frame.
func waitIdentityGate(ctx context.Context, gate chan struct{}, bare bool) error {
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(recoverSettleBudget):
		if bare {
			return errIdentityStillResolving
		}
		log.Printf("recover-session: identity still resolving after %v — explicit target will wait for recovery to settle before it is sent", recoverSettleBudget)
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// connAfterIdentitySettled returns the connection governed by the identity
// epoch that actually settled. A reconnect can replace the epoch while this
// call waits; in that case it loops. The caller must write only this returned
// connection, never re-read currentConn afterward.
func (a *adapter) connAfterIdentitySettled(ctx context.Context, bare bool) (*ipc.Conn, error) {
	return a.connAfterIdentityEpoch(ctx, bare, a.currentIdentityEpoch())
}

// connAfterIdentityEpoch is split out so tests can synchronously capture an old
// epoch and reproduce the reconnect interleaving without scheduler guesses.
func (a *adapter) connAfterIdentityEpoch(ctx context.Context, bare bool, epoch identityEpochSnapshot) (*ipc.Conn, error) {
	for {
		if a.recoverStarted.Load() {
			if err := waitIdentityGate(ctx, epoch.gate, bare); err != nil {
				return nil, err
			}
		}
		if a.identityEpochCurrent(epoch) {
			return epoch.conn, nil
		}
		epoch = a.currentIdentityEpoch()
	}
}

// awaitIdentitySettled remains the recovery-completion-only seam used by
// focused tests. Production attach uses connAfterIdentitySettled so the wait and
// subsequent write cannot be split across broker connections.
func (a *adapter) awaitIdentitySettled(ctx context.Context, bare bool) error {
	_, err := a.connAfterIdentitySettled(ctx, bare)
	return err
}

// bindSessionIDForAttach freezes inject + recover identity from cwd/env at
// attach time (multi-session: prefer the active_sessions entry matching cwd).
func (a *adapter) bindSessionIDForAttach(cwd string) {
	sid := resolveGrokSessionIDForCWD(cwd)
	if sid == "" {
		sid = resolveGrokSessionID()
	}
	if sid == "" {
		return
	}
	if a.leader != nil {
		a.leader.mu.Lock()
		a.leader.sessionID = sid
		a.leader.cwd = cwd
		a.leader.mu.Unlock()
	}
}
