package main

// Session recovery for Codex — the last adapter that had none.
//
// Every other adapter (claude, grok, desktop, agy) sends ipc.OpRecoverSession
// shortly after hello so a RESUMED conversation silently re-claims the topic it
// itself was last attached to. Codex could not, because it had no stable
// identity to send: its adapter's whole environment is
// {C3_CODEX_APP_SERVER_WS, C3_CODEX_CWD, C3_CODEX_REMOTE_BRIDGE} and none of
// those names a conversation. The launcher's cwd-inferred C3_ATTACH_NAME is NOT
// that identity and must never become it — it is a DESCRIPTION of how a session
// was started, so two sessions launched alike share it forever, which is exactly
// the mis-bind the 2026-07-10 redesign deleted (a cwd guess once bound the wrong
// topic and drained its queue).
//
// The identity used here is Codex's own: the THREAD id. It is a UUIDv7 that
// names the rollout file the conversation lives in, so it survives `codex
// resume` across days, and the app-server the adapter already speaks to reports
// which thread is loaded. Two rules govern its use:
//
//	(a) An unknown identity must never match another unknown identity. If the
//	    thread cannot be determined UNAMBIGUOUSLY, this file recovers NOTHING
//	    and says so in the log. There is no "best guess" branch.
//	(b) Identity is settled before anything that depends on it is answered —
//	    toolAttach waits on identitySettled before it sends an attach.
//
// Nothing on the wire changes: ipc.RecoverSessionReq{stable_session_id, cwd} is
// CLI-agnostic and the broker side (internal/broker/handler.go
// handleRecoverSession) already does all the work.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Andrometiq/c3/internal/ipc"
)

const (
	// recoverRespTimeout bounds the wait for the broker's RecoverSessionResp.
	// Same budget the other adapters use.
	recoverRespTimeout = 8 * time.Second

	// recoverIdentityTimeout bounds ONE app-server round trip while resolving the
	// thread id. Deliberately short: the app-server is a loopback process this
	// session's own launcher started moments ago, so a slow answer means
	// something is wrong — and an attach blocks on this resolution (rule (b)),
	// so it must not be able to hang a tool call.
	recoverIdentityTimeout = 3 * time.Second
)

var (
	// recoverSettleTimeout is the bare-attach patience budget. An explicit
	// attach may keep waiting for the already-bounded recovery attempt; a bare
	// attach refuses at this point because its answer depends on identity.
	// A var so mutation tests can shorten it.
	recoverSettleTimeout = 10 * time.Second

	errIdentityStillResolving = errors.New("identity still resolving; retry attach")

	// errCodexNoAppServer: no app-server to ask. Happens when the adapter runs
	// outside the C3 launcher (a hand-registered MCP server), which is a
	// supported way to run — it just cannot have a session identity.
	errCodexNoAppServer = errors.New("no Codex app-server configured (C3_CODEX_APP_SERVER_WS unset) — run Codex through the c3 launcher for session recovery")

	// errCodexNoLoadedThread: the app-server has no thread loaded yet. Nothing to
	// be identified as.
	errCodexNoLoadedThread = errors.New("the Codex app-server has no loaded thread yet")

	// errCodexThreadAmbiguous: more than one loaded thread and nothing tells us
	// which one is ours. Rule (a) — fail closed.
	errCodexThreadAmbiguous = errors.New("the Codex app-server has more than one loaded thread and nothing identifies this adapter's own")

	// errCodexLoadedListUnreadable: the loaded-thread list contained an entry
	// that is not a thread id the app-server named (a null, an object, a number,
	// a blank string). Rendering one anyway would mint an identity that every
	// session hitting the same malformation shares — see loadedThreadIDs.
	errCodexLoadedListUnreadable = errors.New("the Codex app-server's loaded-thread list has an entry that is not a readable thread id")
)

// resolveCodexThreadID answers "which Codex thread is this adapter's session?"
// and returns an error rather than a guess whenever it cannot.
//
// Source of truth is the app-server's `thread/loaded/list`, which is the set of
// threads live on THIS session's app-server. Since a launcher never adopts a
// running app-server (cmd/codex — every launch gets its own port), that set is
// the session's own thread, plus any transient sub-agent threads Codex has
// spawned under it. Verified against a live app-server: a session with one
// conversation reports exactly one id, and the id matches the rollout filename
// under the Codex home, so it is stable across a resume.
//
// Deliberately NOT reused here: forwarder.go's discoverThread, which narrows a
// multi-thread list by cwd and then falls back to `loaded[0]`. That fallback is
// tolerable for DELIVERY (a misdirected turn is visible and recoverable) and
// unacceptable for IDENTITY (a wrong id binds this session to another session's
// topic and drains its queue). Ambiguity here is refused, never narrowed —
// which is also what keeps a sub-agent thread's adapter from claiming the
// session's topic: while both threads are loaded it can resolve nothing.
//
// C3_CODEX_THREAD_ID (already the forwarder's pin) is honoured first: an
// operator naming the thread is a KNOWN identity, not a guess, and it is the
// escape hatch when the loaded list is genuinely ambiguous. A blank or
// whitespace-only value names nothing and is treated as unset — otherwise every
// session that exported it the same empty way would share one identity, which is
// the very thing rule (a) forbids.
func resolveCodexThreadID(ctx context.Context, cfg codexForwardConfig) (string, error) {
	if pinned := strings.TrimSpace(cfg.ThreadID); pinned != "" {
		return pinned, nil
	}
	if cfg.WSURL == "" {
		return "", errCodexNoAppServer
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = recoverIdentityTimeout
	}
	dialer := websocket.Dialer{HandshakeTimeout: cfg.Timeout}
	conn, _, err := dialer.DialContext(ctx, cfg.WSURL, nil)
	if err != nil {
		return "", fmt.Errorf("dial codex app-server: %w", err)
	}
	defer conn.Close()

	client := &codexWSClient{conn: conn, timeout: cfg.Timeout}
	if _, err := client.request(ctx, "initialize", codexInitializeParams()); err != nil {
		return "", err
	}
	if err := client.notify("initialized", nil); err != nil {
		return "", err
	}
	resp, err := client.request(ctx, "thread/loaded/list", map[string]any{"limit": 20})
	if err != nil {
		return "", err
	}
	loaded, err := loadedThreadIDs(resp["data"])
	if err != nil {
		return "", err
	}
	switch len(loaded) {
	case 1:
		return loaded[0], nil
	case 0:
		return "", errCodexNoLoadedThread
	default:
		return "", fmt.Errorf("%w (loaded: %s)", errCodexThreadAmbiguous, strings.Join(loaded, ", "))
	}
}

// loadedThreadIDs reads the identity candidates out of a `thread/loaded/list`
// response, accepting ONLY entries the app-server actually named as a string.
//
// Deliberately NOT forwarder.go's stringSlice, whose fmt.Sprint renders whatever
// it is handed: a JSON null becomes the literal id "<nil>" and an object becomes
// "map[]". Those are non-empty, so neither this file's empty-id guard nor the
// broker's (internal/broker/handler.go — an empty stable id records nothing)
// rejects them, and they are the SAME string in every session that hits the same
// malformed entry. That turns "I could not read this thread's id" into an
// identity two different sessions SHARE — and a shared identity is how one
// session recovers another's topic and drains its queue. fmt.Sprint's tolerance
// is fine on the delivery path, where a misdirected turn is visible and
// recoverable; on the identity path it launders an unknown into a match.
//
// So an unreadable entry fails the WHOLE list closed rather than being skipped:
// a list that is part unreadable might be hiding this session's own thread, and
// silently narrowing to the readable remainder is the same guess by another
// name (rule (a)).
func loadedThreadIDs(data any) ([]string, error) {
	if data == nil {
		return nil, nil // no `data` at all — nothing loaded, not something unreadable
	}
	items, ok := data.([]any)
	if !ok {
		return nil, errCodexLoadedListUnreadable
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		s, ok := item.(string)
		if !ok {
			return nil, errCodexLoadedListUnreadable
		}
		if s = strings.TrimSpace(s); s == "" {
			return nil, errCodexLoadedListUnreadable
		}
		ids = append(ids, s)
	}
	return ids, nil
}

// sessionRecoverEpoch binds every recovery side effect to the broker connection
// it belongs to. A superseded epoch is canceled and settled, but its completion
// can never close a replacement epoch's gate.
type sessionRecoverEpoch struct {
	conn    *ipc.Conn
	gate    chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	started bool // guarded by adapter.rcmu
	settled sync.Once
}

func (e *sessionRecoverEpoch) settle() {
	e.settled.Do(func() { close(e.gate) })
}

// prepareSessionRecoverForConn creates the open recovery epoch for conn. The
// production connect path calls this BEFORE publishing conn; ensure below also
// calls it so tests and manually-installed connections get the same invariant.
func (a *adapter) prepareSessionRecoverForConn(ctx context.Context, conn *ipc.Conn) *sessionRecoverEpoch {
	if conn == nil {
		return nil
	}
	a.rcmu.Lock()
	if current := a.recoverEpoch; current != nil && current.conn == conn {
		a.rcmu.Unlock()
		return current
	}
	epochCtx, cancel := context.WithCancel(ctx)
	epoch := &sessionRecoverEpoch{
		conn: conn, gate: make(chan struct{}), ctx: epochCtx, cancel: cancel,
	}
	old := a.recoverEpoch
	a.recoverEpoch = epoch
	a.rcmu.Unlock()

	if old != nil {
		old.cancel()
		old.settle()
	}
	return epoch
}

// retireSessionRecoverForConn removes and cancels conn's epoch during teardown.
// Waiters wake, observe that their epoch is no longer current, and retry against
// the replacement (or return "broker reconnecting" while no conn is published).
func (a *adapter) retireSessionRecoverForConn(conn *ipc.Conn) {
	if conn == nil {
		return
	}
	a.rcmu.Lock()
	epoch := a.recoverEpoch
	if epoch == nil || epoch.conn != conn {
		a.rcmu.Unlock()
		return
	}
	a.recoverEpoch = nil
	a.rcmu.Unlock()
	epoch.cancel()
	epoch.settle()
}

// ensureSessionRecoverForConn starts session recovery once per BROKER
// CONNECTION. brokerReader calls it on every loop turn; the epoch's started bit
// makes all but the first call on a given connection a no-op.
//
// Per-CONNECTION rather than per-process on purpose. A broker RESTART (a `c3
// update` or a rebuild — routine in this project) hands the adapter a fresh stub
// with no stable session id. Without a re-register, every attach made after that
// restart is recorded against nothing, and the NEXT resume silently recovers the
// pre-restart topic instead of the one the user actually chose.
func (a *adapter) ensureSessionRecoverForConn(ctx context.Context, conn *ipc.Conn) {
	epoch := a.prepareSessionRecoverForConn(ctx, conn)
	if epoch == nil {
		return
	}
	a.rcmu.Lock()
	if a.recoverEpoch != epoch || epoch.started {
		a.rcmu.Unlock()
		return
	}
	epoch.started = true
	a.rcmu.Unlock()
	go a.startSessionRecover(epoch)
}

// recoverStarted reports whether the current prepared epoch has been kicked off.
// Production attach gating starts at preparation; this narrower predicate is
// retained for tests that need to synchronize with the recovery goroutine.
func (a *adapter) recoverStarted() bool {
	a.rcmu.Lock()
	defer a.rcmu.Unlock()
	return a.recoverEpoch != nil && a.recoverEpoch.started
}

// startSessionRecover resolves this session's identity and, if it can, asks the
// broker to re-claim the topic that identity was last attached to. It ALWAYS
// settles the identity gate on the way out, including on failure — a session
// that cannot be identified is a settled answer ("unattached"), not a session
// that blocks attaches for ten seconds.
func (a *adapter) startSessionRecover(epoch *sessionRecoverEpoch) {
	defer epoch.settle()

	cfg := codexForwardConfigFromEnv()
	cfg.Timeout = recoverIdentityTimeout
	sid, err := a.stableSessionID(epoch.ctx, cfg)
	if err != nil {
		// Loud, and specific about the consequence: this is the branch where C3
		// chooses to do nothing rather than bind a topic it is not sure of.
		log.Printf("recover-session: NOT recovering — %v. This Codex session starts UNATTACHED; call `attach` to choose a topic (or set C3_CODEX_THREAD_ID to name this session's thread). C3 never guesses: an identity it cannot determine must not match another session's.", err)
		return
	}
	// Resolution can outlive a broker connection. Do not let a superseded
	// goroutine install a response waiter or write on behalf of the new epoch.
	a.rcmu.Lock()
	current := a.recoverEpoch == epoch
	a.rcmu.Unlock()
	if !current {
		return
	}
	a.fireRecover(epoch.ctx, epoch.conn, sid, cfg.CWD)
}

// stableSessionID returns this session's Codex thread id, resolving it from the
// app-server on first use and caching it. The cache is what makes a reconnect
// re-register cheap (one broker frame, no app-server round trip) and is also why
// a session's identity cannot change mid-process.
func (a *adapter) stableSessionID(ctx context.Context, cfg codexForwardConfig) (string, error) {
	a.tidmu.Lock()
	cached := a.threadID
	a.tidmu.Unlock()
	if cached != "" {
		return cached, nil
	}
	// Whole-resolution budget: dial + initialize + list, each already capped at
	// cfg.Timeout. An attach waits on this, so it is bounded rather than trusting
	// the per-call deadlines to compose.
	ictx, cancel := context.WithTimeout(ctx, 3*recoverIdentityTimeout)
	defer cancel()
	id, err := resolveCodexThreadID(ictx, cfg)
	if err != nil {
		return "", err
	}
	a.tidmu.Lock()
	a.threadID = id
	a.tidmu.Unlock()
	return id, nil
}

// fireRecover sends the recover request on conn and handles the response.
//
// conn is passed in (captured when the recovery was kicked off) rather than
// re-read here, so a reconnect racing this call cannot land the request on a
// connection it was not started for — the same capture-at-enqueue discipline the
// inbound forward path uses.
func (a *adapter) fireRecover(ctx context.Context, conn *ipc.Conn, stableID, cwd string) {
	if conn == nil || stableID == "" {
		return
	}
	respCh := make(chan ipc.RecoverSessionResp, 1)
	a.rsmu.Lock()
	if a.rsPending == nil {
		a.rsPending = make(map[*ipc.Conn]chan ipc.RecoverSessionResp)
	}
	a.rsPending[conn] = respCh
	a.rsmu.Unlock()
	defer func() {
		a.rsmu.Lock()
		if a.rsPending[conn] == respCh {
			delete(a.rsPending, conn)
		}
		a.rsmu.Unlock()
	}()

	if err := conn.WriteJSON(ipc.RecoverSessionReq{
		Op: ipc.OpRecoverSession, StableSessionID: stableID, CWD: cwd,
	}); err != nil {
		log.Printf("recover-session: write failed: %v", err)
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
		if !resp.Recovered {
			// Still worth having sent: the broker has now bound this stable id to
			// the stub, so the NEXT attach is recorded and a later resume has
			// something to recover.
			log.Printf("recover-session: thread=%s registered (no prior attachment to re-claim)", stableID)
			return
		}
		// Remember the recovered route ADDRESSED BY IDENTITY ({topic_id, group}
		// or {target:"dm"}), never by name — see rememberedIdentityReq — so a
		// later broker restart replays a claim that actually re-binds.
		a.rememberAttach(rememberedIdentityReq(cwd, resp.ChatID, resp.TopicID, resp.Group))
		a.setAttachedTopic(resp.Name)
		log.Printf("recover-session: auto-attached to %q (queued=%d)", resp.Name, resp.QueuedCount)
		a.emitRecoverNotice(renderCodexRecoverNotice(resp))
	}
}

// dispatchRecoverSessionResult routes the broker's response to the in-flight
// fireRecover for the SAME connection. Called from brokerReader; non-blocking,
// and a response with no waiter is dropped rather than parked.
func (a *adapter) dispatchRecoverSessionResult(conn *ipc.Conn, raw []byte) {
	var resp ipc.RecoverSessionResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		return
	}
	a.rsmu.Lock()
	ch := a.rsPending[conn]
	a.rsmu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- resp:
	default:
	}
}

func (a *adapter) identityGate() chan struct{} {
	a.rcmu.Lock()
	defer a.rcmu.Unlock()
	if a.recoverEpoch == nil {
		epochCtx, cancel := context.WithCancel(context.Background())
		a.recoverEpoch = &sessionRecoverEpoch{
			gate: make(chan struct{}), ctx: epochCtx, cancel: cancel,
		}
	}
	return a.recoverEpoch.gate
}

func (a *adapter) currentSessionRecoverEpoch() *sessionRecoverEpoch {
	a.rcmu.Lock()
	defer a.rcmu.Unlock()
	return a.recoverEpoch
}

func (a *adapter) currentSessionRecoverSnapshot() (*sessionRecoverEpoch, *ipc.Conn) {
	a.rcmu.Lock()
	defer a.rcmu.Unlock()
	a.bmu.Lock()
	defer a.bmu.Unlock()
	return a.recoverEpoch, a.conn
}

// markIdentitySettled releases anything waiting on the current broker
// connection's recovery registration.
func (a *adapter) markIdentitySettled() {
	a.rcmu.Lock()
	epoch := a.recoverEpoch
	if epoch == nil {
		epochCtx, cancel := context.WithCancel(context.Background())
		epoch = &sessionRecoverEpoch{
			gate: make(chan struct{}), ctx: epochCtx, cancel: cancel,
		}
		a.recoverEpoch = epoch
	}
	a.rcmu.Unlock()
	epoch.settle()
}

// awaitIdentitySettled blocks until this session's identity question has been
// answered — rule (b): nothing that depends on the identity may be answered
// before it is settled. A canceled caller aborts. At the settle budget a bare
// attach is refused because own-route recovery versus picker is identity-
// dependent. An explicit attach keeps waiting for the bounded recovery attempt;
// it is sent only after the broker has answered identity, never raced against a
// late recover frame. The returned conn belongs to the settled epoch; callers
// must use it rather than re-reading currentConn, which could pair a new conn
// with the old epoch's completion.
func (a *adapter) awaitIdentitySettled(ctx context.Context, bare bool) (*ipc.Conn, error) {
	return a.awaitIdentityEpoch(ctx, bare, a.currentSessionRecoverEpoch())
}

// awaitIdentityEpoch takes the first epoch as an argument so the snapshot and
// the wait are one explicit operation. On replacement it follows the current
// epoch; callers never treat an old gate's close as settlement for a new conn.
func (a *adapter) awaitIdentityEpoch(ctx context.Context, bare bool, epoch *sessionRecoverEpoch) (*ipc.Conn, error) {
	timer := time.NewTimer(recoverSettleTimeout)
	defer timer.Stop()
	var budget <-chan time.Time = timer.C

	for {
		if epoch == nil {
			// Read epoch+conn as one snapshot. Production publishes an epoch
			// before its conn, so {nil,newConn} is impossible here; a split read
			// could otherwise jump from the reconnect gap onto an unsettled conn.
			current, conn := a.currentSessionRecoverSnapshot()
			if current == nil {
				return conn, nil
			}
			epoch = current
			continue
		}
		select {
		case <-epoch.gate:
			// A replacement settles the old gate to wake waiters. Only return
			// when the epoch we waited on is still the current one.
			current, conn := a.currentSessionRecoverSnapshot()
			if current == epoch {
				return epoch.conn, nil
			}
			if current == nil {
				return conn, nil
			}
			epoch = current
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-budget:
			if bare {
				return nil, errIdentityStillResolving
			}
			log.Printf("attach: identity still resolving after %v — explicit target will wait for recovery to settle before it is sent", recoverSettleTimeout)
			// An explicit target has paid the one bounded patience budget. Keep
			// following replacement epochs until identity settles or ctx ends.
			budget = nil
		}
	}
}

// renderCodexRecoverNotice is the CLI-side echo of an auto-re-attach. Codex does
// not reliably surface unsolicited MCP notifications, so the broker's Telegram
// recover-welcome remains the guaranteed signal; this exists because a resumed
// Codex session that does not know it holds a topic will also not know to drain
// the backlog held on it.
func renderCodexRecoverNotice(resp ipc.RecoverSessionResp) string {
	if resp.Name == "" {
		return ""
	}
	notice := fmt.Sprintf("c3: resumed — re-attached to topic %q. Telegram messages for it arrive here again.", resp.Name)
	if summary := renderBacklogSummary(resp.QueuedCount, resp.QueuedSummary, resp.Name); summary != "" {
		notice += "\n\n" + summary
	}
	return notice
}

// emitRecoverNotice pushes the notice as a log notification. Best-effort: on
// failure the full text goes to adapter.log so the auto-attach is still
// discoverable, the same rule the other notify paths follow.
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
