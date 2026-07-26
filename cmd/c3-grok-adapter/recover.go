package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

const recoverRespTimeout = 8 * time.Second

// recoverSettleBudget bounds how long an identity-dependent call (attach) waits
// for the recovery round-trip to FINISH. Deliberately longer than
// recoverRespTimeout so it is a genuine backstop rather than the normal exit:
// every ordinary failure inside fireRecover (write error, broker error, response
// timeout) settles the gate by itself well inside this window, so reaching this
// budget means the recovery goroutine is wedged somewhere it should not be. A
// var (not a const) so tests can shorten it.
var recoverSettleBudget = recoverRespTimeout + 2*time.Second

// recover fields live on adapter (declared here via methods).

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
	a.fireRecover(ctx, sid, a.cwd())
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
	if stableID == "" {
		return
	}
	if !a.recoverFired.CompareAndSwap(false, true) {
		return
	}
	// The CAS winner OWNS the identity question, so it answers it on EVERY exit
	// path below — recovered, registered, refused, write-failed, timed out. A
	// session that could not be identified is a settled answer ("nobody"), not a
	// session that blocks attaches until a budget expires: a hung attach is a
	// worse failure than an unidentified one. (The CAS loser deliberately does
	// NOT settle — the winner is still working, and letting the loser answer for
	// it is the exact entry-read-as-completion mistake this guards against.)
	defer a.markIdentitySettled()

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

	conn := a.currentConn()
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
		a.recoverFired.Store(false)
		log.Printf("recover-session: no broker connection — nothing sent; releasing the once-per-connection guard so a later attempt can register this session")
		return
	}
	if err := conn.WriteJSON(ipc.RecoverSessionReq{
		Op: ipc.OpRecoverSession, StableSessionID: stableID, CWD: cwd,
	}); err != nil {
		// Same as conn == nil: the request never reached the broker, so the guard
		// must not stay set on a recovery that did not happen.
		a.recoverFired.Store(false)
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
	a.recoverFired.Store(false)
	go func() {
		sid := a.stableSessionID()
		if sid == "" {
			return // no session id resolvable — nothing to re-register yet;
			// the next attach's ensureStableSessionRegistered retries.
		}
		a.recoverStarted.Store(true)
		a.fireRecover(ctx, sid, a.cwd())
	}()
}

// ensureStableSessionRegistered tells the broker this stub's stable session id
// (so attach records session attachment for resume) without claiming a route,
// and — because toolAttach calls it synchronously before it writes the attach —
// it is where the "identity before anything that depends on it" rule is enforced
// for Grok: every path out of here has either ANSWERED the identity question or
// waited for the answer.
func (a *adapter) ensureStableSessionRegistered(ctx context.Context) {
	sid := a.stableSessionID()
	if sid == "" {
		// No id to register — but a recovery started earlier may still be in
		// flight, and the attach must not overtake it.
		a.awaitIdentitySettled(ctx)
		return
	}
	// fireRecover is once per BROKER CONNECTION (refireRecoverOnReconnect resets
	// the guard after a reconnect) and its CompareAndSwap makes this a no-op when
	// a recover already fired on this connection — so calling it unconditionally
	// is safe and duplicate-free.
	a.recoverStarted.Store(true)
	a.fireRecover(ctx, sid, a.cwd())
	// A no-op RETURN is not an answer, though. The old code read recoverFired
	// (ENTRY into fireRecover) as "already registered" and returned straight into
	// the attach write — so an attach could be answered while the recover that
	// would have told the broker who this session is was still in flight, and the
	// broker answered attachBare from no identity. Wait for the answer.
	a.awaitIdentitySettled(ctx)
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

// markIdentitySettled answers the identity question for this process. Idempotent
// — the first recovery attempt to finish settles it, and later re-registrations
// (reconnect refires) do not re-open it.
func (a *adapter) markIdentitySettled() {
	gate := a.identityGate()
	a.settleOnce.Do(func() { close(gate) })
}

// awaitIdentitySettled blocks until this session's identity question has been
// ANSWERED. Returns immediately when no recovery was ever started — nothing will
// ever settle it, and blocking on that would hang every attach in a session with
// no resolvable Grok session id.
func (a *adapter) awaitIdentitySettled(ctx context.Context) {
	if !a.recoverStarted.Load() {
		return
	}
	select {
	case <-a.identityGate():
	case <-ctx.Done():
	case <-time.After(recoverSettleBudget):
		// Say plainly what proceeding means: the session is dispatching this call
		// as an UNIDENTIFIED session — one the broker has no stable id for. An
		// `attach` answered here cannot silently re-claim this session's own last
		// topic; it falls to the picker / explicit-name path. Degraded, not
		// unsafe: a recover that lands later takes the broker's record-only branch
		// (internal/broker/handler.go handleRecoverSession, the already-attached
		// arm), so it cannot steal the route the explicit attach just took.
		log.Printf("recover-session: identity still unsettled after %v — dispatching this call as an UNIDENTIFIED session: an `attach` answered now will not silently re-claim this session's own topic (expect the picker) while the recover completes behind it", recoverSettleBudget)
		// Giving up IS the answer, so record it: one wedged recovery must cost
		// one budget for the process, not one budget per caller.
		a.markIdentitySettled()
	}
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
