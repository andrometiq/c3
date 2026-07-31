// c3-cursor-adapter is the Cursor Agent CLI MCP server that bridges Cursor's MCP stdio
// protocol to the C3 broker over $XDG_RUNTIME_DIR/c3.sock.
//
// Outbound tools (attach, reply, …) are broker-forwarded like the Codex/Grok
// adapters. Since Cursor does not support async push that starts a turn, inbound
// Telegram messages are retrieved via the `fetch_queue` tool.
//
// MCP wire layer: github.com/modelcontextprotocol/go-sdk.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mcptools"
	"github.com/Andrometiq/c3/internal/mode"
	"github.com/Andrometiq/c3/internal/osutil"
	"github.com/Andrometiq/c3/internal/spawn"
	"github.com/Andrometiq/c3/internal/termtitle"
)

const (
	// adapterName MUST match the MCP server key in the Cursor mcp.json
	// (plugin.json / mcp_config.json mcpServers.c3).
	adapterName    = "c3"
	adapterVersion = "0.1.0"

	idleStartupTimeout = 60 * time.Second // mirror cmd/c3-claude-adapter behavior
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "c3-cursor-adapter: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Persistent adapter log at $XDG_STATE_HOME/c3/adapter.log.
	if path, err := setupAdapterLog(); err == nil {
		fmt.Fprintf(os.Stderr, "c3-cursor-adapter: log file %s\n", path)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := newAdapter()
	a.runCtx = ctx
	installSignalHandlers(cancel)

	if err := a.connectBroker(); err != nil {
		log.Printf("adapter: exit pid=%d reason=connect-broker err=%v", os.Getpid(), err)
		return fmt.Errorf("connect broker: %w", err)
	}
	if err := a.hello(); err != nil {
		log.Printf("adapter: exit pid=%d reason=hello err=%v", os.Getpid(), err)
		return fmt.Errorf("hello: %w", err)
	}

	srv := a.buildMCPServer()
	a.transport = newLogNotifyTransport(&mcp.StdioTransport{})

	go a.brokerReader(ctx)
	go a.idleStartupWatchdog(ctx, cancel)
	// Resume auto-attach: register stable session id + re-claim last topic.
	// Prepare synchronously so the MCP server cannot expose attach while
	// recoverStarted still says there is no identity question.
	a.trySessionRecover(ctx)

	err := srv.Run(ctx, a.transport)
	switch {
	case err == nil:
		log.Printf("adapter: exit pid=%d reason=stdin-eof (clean)", os.Getpid())
		return nil
	case errors.Is(err, context.Canceled) || errors.Is(err, io.EOF):
		log.Printf("adapter: exit pid=%d reason=context-canceled-or-eof (signal or idle-startup) (clean)", os.Getpid())
		return nil
	default:
		log.Printf("adapter: exit pid=%d reason=mcp-error err=%v", os.Getpid(), err)
		return err
	}
}

func installSignalHandlers(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, append([]os.Signal{syscall.SIGTERM, syscall.SIGINT}, osutil.ReloadSignals()...)...)
	go func() {
		sig := <-ch
		log.Printf("adapter: received signal=%v pid=%d", sig, os.Getpid())
		cancel()
	}()
}

func setupAdapterLog() (string, error) {
	state := os.Getenv("XDG_STATE_HOME")
	if state == "" {
		home, _ := os.UserHomeDir()
		state = filepath.Join(home, ".local", "state")
	}
	dir := filepath.Join(state, "c3")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "adapter.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	log.SetOutput(io.MultiWriter(f, os.Stderr))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("adapter: started pid=%d cli=cursor", os.Getpid())
	return path, nil
}

type adapter struct {
	transport *logNotifyTransport

	bmu sync.Mutex
	// connHelloPending is the production-published connection whose HelloAck
	// has not yet been validated. currentConn hides it from ordinary tools while
	// hello uses rawCurrentConn for the handshake. Direct test-installed conns do
	// not set this field and remain usable by default.
	connHelloPending *ipc.Conn
	conn             *ipc.Conn
	epoch            *identityEpoch

	pmu     sync.Mutex
	pending map[string]chan ipc.ToolResultMsg
	nextID  atomic.Uint64

	fqmu      sync.Mutex
	fqPending map[string]chan ipc.FetchQueueResp
	rtmu      sync.Mutex
	rtPending map[string]chan ipc.RetranscribeResp

	recoverMu sync.Mutex
	rsmu      sync.Mutex
	rsPending chan ipc.RecoverSessionResp

	// recoverStarted records that recovery has begun. The current per-connection
	// identitySettled epoch says whether that registration has finished.
	recoverStarted atomic.Bool
	runCtx         context.Context

	helloAck      ipc.HelloAckMsg
	brokerVersion atomic.Int64

	amu           sync.Mutex
	lastAttach    *ipc.AttachReq
	attachedTopic string

	brokerDownAdvised atomic.Bool
	dispatched        atomic.Bool
}

// identityEpoch pairs one broker connection with the answer to the identity
// question asked on that connection. Neither field changes after construction:
// an old recovery can therefore settle only its own gate, never a gate opened
// by a reconnect.
type identityEpoch struct {
	conn *ipc.Conn
	gate chan struct{}

	recoverFired atomic.Bool
	ready        atomic.Bool
	settleOnce   sync.Once
}

func newAdapter() *adapter {
	return &adapter{
		pending:   map[string]chan ipc.ToolResultMsg{},
		fqPending: map[string]chan ipc.FetchQueueResp{},
		rtPending: map[string]chan ipc.RetranscribeResp{},
	}
}

func (a *adapter) connectBroker() error {
	sockPath, err := broker.SocketPath()
	if err != nil {
		return fmt.Errorf("resolve broker socket: %w", err)
	}
	for attempt := 0; attempt < 50; attempt++ {
		c, err := net.Dial("unix", sockPath)
		if err == nil {
			a.publishBrokerConnection(ipc.NewConn(c))
			return nil
		}
		if attempt == 0 {
			_ = spawnBroker()
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("could not reach broker at %s after 10s", sockPath)
}

func spawnBroker() error {
	return spawn.Detached(exec.Command("c3-broker"))
}

func (a *adapter) hello() error {
	conn := a.rawCurrentConn()
	if conn == nil {
		return errors.New("broker connection unavailable during hello")
	}
	cwd := os.Getenv("C3_CURSOR_CWD")
	if cwd == "" {
		cwd, _ = os.Getwd()
	}
	if err := conn.WriteJSON(ipc.HelloMsg{
		Op: ipc.OpHello, CLI: "cursor", PID: os.Getpid(), CWD: cwd,
		Capabilities:         []string{"fetch_queue"},
		CannotRenderChannels: true,
		ProtocolVersion:      ipc.ProtocolVersion,
	}); err != nil {
		return err
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		return err
	}
	var ack ipc.HelloAckMsg
	if err := json.Unmarshal(raw, &ack); err != nil {
		return err
	}
	if ack.Op != ipc.OpHelloAck {
		return fmt.Errorf("unexpected broker hello response op %q", ack.Op)
	}
	// Version disagreement is logged, never fatal — see ipc.ProtocolVersion.
	if w := ipc.AdapterProtocolWarning("cursor", ack.ProtocolVersion); w != "" {
		log.Print(w)
	}
	if !a.commitBrokerHello(conn, ack) {
		return errors.New("broker connection changed during hello")
	}
	return nil
}

func (a *adapter) currentConn() *ipc.Conn {
	a.bmu.Lock()
	defer a.bmu.Unlock()
	if a.conn != nil && a.connHelloPending == a.conn {
		return nil
	}
	return a.conn
}

// rawCurrentConn is reserved for the Hello handshake. Ordinary broker traffic
// must use currentConn so it cannot overtake HelloAck validation.
func (a *adapter) rawCurrentConn() *ipc.Conn {
	a.bmu.Lock()
	defer a.bmu.Unlock()
	return a.conn
}

// currentIdentityEpoch returns the epoch for the currently published broker
// connection. Bare test adapters sometimes install conn directly; treating that
// as a fresh epoch keeps those fixtures faithful to a real connection publish.
func (a *adapter) currentIdentityEpoch() *identityEpoch {
	a.bmu.Lock()
	defer a.bmu.Unlock()
	if a.epoch == nil || a.epoch.conn != a.conn {
		a.epoch = &identityEpoch{conn: a.conn, gate: make(chan struct{})}
		a.epoch.ready.Store(true) // direct/test-installed conn is already usable
	}
	return a.epoch
}

func (a *adapter) isCurrentIdentityEpoch(epoch *identityEpoch) bool {
	a.bmu.Lock()
	defer a.bmu.Unlock()
	if a.epoch == nil || a.epoch.conn != a.conn {
		a.epoch = &identityEpoch{conn: a.conn, gate: make(chan struct{})}
		a.epoch.ready.Store(true)
	}
	return a.epoch == epoch
}

// publishBrokerConnection opens its recovery epoch before making the connection
// visible. An attach that wakes from an older epoch can then only write to its
// captured old connection; it cannot silently adopt this one.
func (a *adapter) publishBrokerConnection(conn *ipc.Conn) {
	a.bmu.Lock()
	a.epoch = &identityEpoch{conn: conn, gate: make(chan struct{})}
	a.conn = conn
	a.connHelloPending = conn
	a.bmu.Unlock()
}

func (a *adapter) markCurrentIdentityEpochReady() {
	a.markBrokerHelloReady(a.rawCurrentConn())
}

func (a *adapter) markBrokerHelloReady(conn *ipc.Conn) bool {
	a.bmu.Lock()
	defer a.bmu.Unlock()
	if conn == nil || a.conn != conn {
		return false
	}
	a.markBrokerHelloReadyLocked(conn)
	return true
}

func (a *adapter) commitBrokerHello(conn *ipc.Conn, ack ipc.HelloAckMsg) bool {
	a.bmu.Lock()
	defer a.bmu.Unlock()
	if conn == nil || a.conn != conn {
		return false
	}
	a.helloAck = ack
	a.brokerVersion.Store(int64(ipc.PeerProtocolVersion(ack.ProtocolVersion)))
	a.markBrokerHelloReadyLocked(conn)
	return true
}

func (a *adapter) markBrokerHelloReadyLocked(conn *ipc.Conn) {
	if a.connHelloPending == conn {
		a.connHelloPending = nil
	}
	if a.epoch != nil && a.epoch.conn == conn {
		a.epoch.ready.Store(true)
	}
}

// retireBrokerConnection replaces a lost connection with an already-answered
// unavailable epoch. Wake old waiters promptly; their captured recovery owns
// only the old gate and cannot affect a later reconnect's epoch.
func (a *adapter) retireBrokerConnection() {
	a.bmu.Lock()
	oldConn := a.conn
	oldEpoch := a.epoch
	down := &identityEpoch{gate: make(chan struct{})}
	down.ready.Store(true)
	down.settleOnce.Do(func() { close(down.gate) })
	a.epoch = down
	a.conn = nil
	a.connHelloPending = nil
	a.bmu.Unlock()
	if oldConn != nil {
		_ = oldConn.Close()
	}
	if oldEpoch != nil {
		a.settleIdentity(oldEpoch)
	}
}

func (a *adapter) brokerReader(ctx context.Context) {
	for {
		conn := a.currentConn()
		if conn == nil {
			return
		}
		raw, err := conn.ReadFrame()
		if err != nil {
			log.Printf("broker read err: %v — recovering", err)
			if !a.recoverBroker(ctx) {
				log.Printf("broker recovery aborted")
				return
			}
			continue
		}
		op, err := ipc.PeekOp(raw)
		if err != nil {
			continue
		}
		switch op {
		case ipc.OpToolResult:
			a.dispatchToolResult(raw)
		case ipc.OpAttached:
			a.dispatchAttached(raw)
		case ipc.OpTopicsList:
			a.dispatchTopicsList(raw)
		case ipc.OpFetchQueueResult:
			a.dispatchFetchQueueResult(raw)
		case ipc.OpRetranscribeResult:
			a.dispatchRetranscribeResult(raw)
		case ipc.OpRecoverSessionResult:
			a.dispatchRecoverSessionResult(raw)
		case ipc.OpInbound:
			var msg ipc.InboundMsg
			if err := json.Unmarshal(raw, &msg); err == nil {
				if msg.Inbound.Kind == c3types.InboundSystem {
					a.handleSystemInbound(&msg.Inbound)
				} else {
					// Cursor is a pull-only host (CannotRenderChannels). The broker
					// still pushes synthesized channel EVENTS (poll_result /
					// reaction / callback) live to a claimed holder — see
					// internal/broker/worker.go, where the held-in-queue path is
					// gated to non-events — and events are NEVER queued, so they
					// are not recoverable via fetch_queue. We do not render events
					// here (this is not an event-rendering feature); log the drop
					// so it is visible in debugging. Rare, so unrate-limited is fine.
					log.Printf("cursor: dropped non-system inbound push (kind=%q message_id=%d) — this pull-only host cannot render channel-event pushes", msg.Inbound.Kind, msg.Inbound.MessageID)
				}
			}
		case ipc.OpError:
			var errMsg ipc.ErrorMsg
			_ = json.Unmarshal(raw, &errMsg)
			log.Printf("broker error: %s", errMsg.Err)
		default:
			// An op this build does not know — normally a NEWER broker (mixed
			// versions are routine after `c3 update`; see ipc.ProtocolVersion).
			// Skipping it is correct: unknown ops are additive by contract. Log
			// it so the skip is VISIBLE — a silent drop is the worst failure
			// mode there is.
			log.Printf("cursor: ignoring unknown op %q from broker (this adapter speaks protocol v%d — the broker may be a newer c3 build; restart this CLI to match)", op, ipc.ProtocolVersion)
		}
	}
}

func (a *adapter) handleSystemInbound(in *c3types.Inbound) {
	if in.Event == nil || in.Event.System == nil {
		return
	}
	sys := in.Event.System
	body := fmt.Sprintf("⚠️ [%s] %s: %s", sys.Level, sys.Title, sys.Message)
	if a.transport != nil {
		_ = a.transport.Notify(context.Background(), "notifications/message", map[string]any{
			"level":  "warning",
			"logger": "c3",
			"data":   body,
		})
	}
	log.Printf("system inbound: %s", body)
}

func (a *adapter) reconnectBroker() error {
	a.wakePendingWithErr("broker reconnect — request canceled")
	a.retireBrokerConnection()

	if err := a.connectBroker(); err != nil {
		return err
	}
	return a.hello()
}

const recoverBrokerAdviseAfter = 6

func (a *adapter) recoverBroker(ctx context.Context) bool {
	const (
		base       = 500 * time.Millisecond
		maxBackoff = 30 * time.Second
	)
	backoff := base
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			log.Printf("broker recovery canceled (ctx done): %v", err)
			return false
		}
		err := a.reconnectBroker()
		if err == nil {
			log.Printf("broker reconnected (attempt %d)", attempt)
			a.clearBrokerDownAdvisory()
			a.replayLastAttach()
			a.refireRecoverOnReconnect(ctx)
			return true
		}
		log.Printf("broker reconnect attempt %d failed: %v (retry in %v)", attempt, err, backoff)
		if attempt >= recoverBrokerAdviseAfter {
			a.adviseBrokerDown(attempt)
		}
		select {
		case <-ctx.Done():
			log.Printf("broker recovery canceled mid-backoff: %v", ctx.Err())
			return false
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (a *adapter) adviseBrokerDown(attempt int) {
	if !a.brokerDownAdvised.CompareAndSwap(false, true) {
		return
	}
	if a.transport == nil {
		return
	}
	sysev := &c3types.SystemEvent{
		Source:  "c3",
		Level:   "warn",
		Title:   "C3 broker unreachable",
		Message: fmt.Sprintf("C3 lost its connection to the broker and could not reconnect after %d attempts. Inbound Telegram messages will NOT arrive until this recovers. Your phone messages won't reach this session meanwhile.", attempt),
	}
	body := "⚠️ SYSTEM: " + sysev.Message
	if err := a.transport.Notify(context.Background(), "notifications/message", map[string]any{
		"level":  "warning",
		"logger": "c3",
		"data":   body,
	}); err != nil {
		log.Printf("broker-down advisory notify failed: %v — %s", err, body)
	}
	log.Printf("broker-down advisory surfaced (attempt %d)", attempt)
}

func (a *adapter) clearBrokerDownAdvisory() {
	a.brokerDownAdvised.Store(false)
}

const (
	// recoverRespTimeout bounds the wait for the broker's RecoverSessionResp
	// (was an inline literal at the one call site).
	recoverRespTimeout = 8 * time.Second
)

// recoverSettleBudget bounds how long a bare attach waits for the recovery
// round-trip to FINISH before refusing with a retryable answer. Deliberately
// longer than
// recoverRespTimeout so it is a genuine backstop rather than the normal exit:
// every ordinary failure inside fireRecover (write error, broker error, response
// timeout) settles the gate by itself well inside this window, so reaching this
// budget means the recovery goroutine is wedged. An explicit attach keeps
// waiting for that already-bounded attempt rather than racing it. A var (not a
// const) so tests can shorten it.
var recoverSettleBudget = recoverRespTimeout + 2*time.Second

var errIdentityStillResolving = errors.New("identity still resolving; retry attach")

// resolveCursorSessionID returns the first non-empty candidate env Cursor may
// expose for a stable conversation/chat id. Empty means skip auto-recover.
func resolveCursorSessionID() string {
	for _, key := range []string{
		"CURSOR_CONVERSATION_ID",
		"CURSOR_AGENT_CONVERSATION_ID",
		"CURSOR_CHAT_ID",
	} {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

func (a *adapter) trySessionRecover(ctx context.Context) {
	sid := resolveCursorSessionID()
	if sid == "" {
		log.Printf("recover-session: no Cursor session id yet — skip auto-attach (will register id on first attach)")
		return
	}
	cwd := os.Getenv("C3_CURSOR_CWD")
	if cwd == "" {
		cwd, _ = os.Getwd()
	}
	// Mark the identity question OPEN before firing, so an attach arriving in the
	// same instant waits for the answer instead of racing it.
	a.recoverStarted.Store(true)
	go a.fireRecover(ctx, sid, cwd)
}

func (a *adapter) refireRecoverOnReconnect(ctx context.Context) {
	sid := resolveCursorSessionID()
	if sid == "" {
		// This freshly connected broker has no identity to learn. Answer its
		// epoch rather than making an attach wait for a recovery nobody will fire.
		a.markIdentitySettled()
		return
	}
	cwd := os.Getenv("C3_CURSOR_CWD")
	if cwd == "" {
		cwd, _ = os.Getwd()
	}
	a.recoverStarted.Store(true)
	go func() {
		a.fireRecover(ctx, sid, cwd)
	}()
}

// identityGate returns the channel that closes when this session's identity
// question has been answered, creating it on first use. Lazy so an adapter built
// as a bare struct literal (as several tests do) behaves like one from
// newAdapter instead of waiting on a nil channel forever.
func (a *adapter) identityGate() chan struct{} {
	return a.currentIdentityEpoch().gate
}

func (a *adapter) markIdentitySettled() {
	a.settleIdentity(a.currentIdentityEpoch())
}

func (a *adapter) settleIdentity(epoch *identityEpoch) {
	if epoch != nil {
		epoch.settleOnce.Do(func() { close(epoch.gate) })
	}
}

// awaitIdentitySettled blocks until this session's identity question has been
// ANSWERED. Returns immediately when no recovery was ever started. A canceled
// caller aborts. At the settle budget a bare attach is refused because its
// answer depends on identity; an explicit target keeps waiting for the bounded
// recovery attempt and is never raced against a late recover frame.
func (a *adapter) awaitIdentitySettled(ctx context.Context, bare bool) error {
	_, err := a.awaitCurrentIdentityEpoch(ctx, bare)
	return err
}

// awaitCurrentIdentityEpoch waits for the current connection's recovery answer
// and returns that immutable conn+gate pair. If a reconnect replaces it while
// waiting, the old answer is deliberately not enough: chase the new epoch.
func (a *adapter) awaitCurrentIdentityEpoch(ctx context.Context, bare bool) (*identityEpoch, error) {
	epoch := a.currentIdentityEpoch()
	if !a.recoverStarted.Load() {
		if !epoch.ready.Load() {
			return nil, errIdentityStillResolving
		}
		return epoch, nil
	}
	return a.awaitIdentityEpoch(ctx, bare, epoch)
}

// awaitIdentityEpoch is split out so the epoch handoff is directly testable.
func (a *adapter) awaitIdentityEpoch(ctx context.Context, bare bool, epoch *identityEpoch) (*identityEpoch, error) {
	for {
		if err := waitForIdentityEpoch(ctx, bare, epoch.gate); err != nil {
			return nil, err
		}
		if a.isCurrentIdentityEpoch(epoch) {
			return epoch, nil
		}
		epoch = a.currentIdentityEpoch()
	}
}

func waitForIdentityEpoch(ctx context.Context, bare bool, gate <-chan struct{}) error {
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

func (a *adapter) fireRecover(ctx context.Context, stableID, cwd string) {
	a.recoverMu.Lock()
	defer a.recoverMu.Unlock()
	if stableID == "" {
		return
	}
	epoch := a.currentIdentityEpoch()
	if !epoch.ready.Load() {
		return
	}
	if !epoch.recoverFired.CompareAndSwap(false, true) {
		return
	}
	// The CAS winner OWNS the identity question, so it answers it on EVERY exit
	// path below — recovered, registered, refused, write-failed, timed out. A
	// session that could not be identified is a settled answer ("nobody"), not a
	// session that blocks attaches until a budget expires: a hung attach is a
	// worse failure than an unidentified one. (The CAS loser deliberately does
	// NOT settle — the winner is still working, and letting the loser answer for
	// it is the exact entry-read-as-completion mistake this guards against.)
	defer a.settleIdentity(epoch)

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
		// once-per-connection guard here wedges the session permanently: the
		// broker never learns this session's stable id, and every subsequent
		// attach is recorded against nothing. Release it so a later attempt can
		// actually fire.
		//
		// Releasing does not re-open the identity gate (deliberately — see
		// identitySettled): with no broker connection, toolAttach itself answers
		// "broker reconnecting — retry attach in a moment", so no attach can be
		// answered in this window anyway, and the reconnect path re-fires.
		epoch.recoverFired.Store(false)
		log.Printf("recover-session: no broker connection — nothing sent; releasing the once-per-connection guard so a later attempt can register this session")
		return
	}
	req := ipc.RecoverSessionReq{Op: ipc.OpRecoverSession, StableSessionID: stableID, CWD: cwd}
	if err := conn.WriteJSON(req); err != nil {
		// Same as conn == nil: the request never reached the broker, so the guard
		// must not stay set on a recovery that did not happen.
		epoch.recoverFired.Store(false)
		log.Printf("recover session write failed: %v (nothing sent — guard released for a later attempt)", err)
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(recoverRespTimeout):
		log.Printf("recover session timed out")
	case resp := <-respCh:
		if resp.Err != "" {
			log.Printf("recover session failed: %s", resp.Err)
			return
		}
		if !resp.Recovered {
			log.Printf("recover-session: session=%s registered (no prior attachment to re-claim)", stableID)
			return
		}
		a.rememberAttach(rememberedIdentityReq(cwd, resp.ChatID, resp.TopicID, resp.Group))
		a.setAttachedTopic(resp.Name)
		log.Printf("recover-session: auto-attached to %q (queued=%d)", resp.Name, resp.QueuedCount)
		if text := renderCursorRecoverNotice(resp); text != "" {
			a.emitRecoverNotice(text)
		}
	}
}

func renderCursorRecoverNotice(resp ipc.RecoverSessionResp) string {
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
	return fmt.Sprintf("C3: auto-attached to %q (resumed session). Inbound Telegram messages are held in C3's durable queue; call `fetch_queue` to read them.", name)
}

func (a *adapter) emitRecoverNotice(text string) {
	if a.transport == nil || text == "" {
		return
	}
	_ = a.transport.Notify(context.Background(), "notifications/message", map[string]any{
		"level":  "info",
		"logger": "c3",
		"data":   text,
	})
}

func (a *adapter) rememberAttach(req ipc.AttachReq) {
	a.amu.Lock()
	defer a.amu.Unlock()
	cp := req
	cp.Steal = false
	a.lastAttach = &cp
}

func (a *adapter) setAttachedTopic(name string) {
	a.amu.Lock()
	defer a.amu.Unlock()
	a.attachedTopic = name
}

func (a *adapter) currentTopicName() string {
	a.amu.Lock()
	defer a.amu.Unlock()
	return a.attachedTopic
}

func isBareAttachReq(req ipc.AttachReq) bool {
	return req.Expr == "" && req.Target == "" && req.Name == "" && req.TopicID == nil && !req.Create
}

func rememberedIdentityReq(cwd string, chatID int64, topicID *int64, group string) ipc.AttachReq {
	req := ipc.AttachReq{Op: ipc.OpAttach, CWD: cwd}
	if topicID == nil {
		req.Target = "dm"
		return req
	}
	tid := *topicID
	req.TopicID = &tid
	req.Group = group
	req.ChatID = chatID
	return req
}

func resolvedAttachReq(req ipc.AttachReq, attached ipc.AttachedMsg) ipc.AttachReq {
	if !isBareAttachReq(req) {
		return req
	}
	return rememberedIdentityReq(req.CWD, attached.ChatID, attached.TopicID, attached.Group)
}

func (a *adapter) replayLastAttach() {
	a.amu.Lock()
	req := a.lastAttach
	a.amu.Unlock()
	if req == nil {
		return
	}
	if conn := a.currentConn(); conn != nil {
		replay := *req
		replay.Replay = true
		if err := conn.WriteJSON(replay); err != nil {
			log.Printf("replay attach failed: %v", err)
			return
		}
		log.Printf("replayed attach (target=%q name=%q)", req.Target, req.Name)
	}
}

func (a *adapter) wakePendingWithErr(msg string) {
	a.pmu.Lock()
	pending := a.pending
	a.pending = map[string]chan ipc.ToolResultMsg{}
	a.pmu.Unlock()
	for _, ch := range pending {
		select {
		case ch <- ipc.ToolResultMsg{Error: &ipc.ErrorPayload{Code: -32000, Message: msg}}:
		default:
		}
	}
}

func (a *adapter) dispatchToolResult(raw []byte) {
	var msg ipc.ToolResultMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	a.pmu.Lock()
	ch, ok := a.pending[msg.ID]
	delete(a.pending, msg.ID)
	a.pmu.Unlock()
	if ok {
		ch <- msg
	}
}

func (a *adapter) dispatchAttached(raw []byte) {
	var msg ipc.AttachedMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	a.pmu.Lock()
	ch, ok := a.pending["attached"]
	delete(a.pending, "attached")
	a.pmu.Unlock()
	if ok {
		ch <- ipc.ToolResultMsg{Result: map[string]any{"_attached": msg}}
	}
}

func (a *adapter) dispatchTopicsList(raw []byte) {
	var msg ipc.TopicsListMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	a.pmu.Lock()
	ch, ok := a.pending["topics_list"]
	delete(a.pending, "topics_list")
	a.pmu.Unlock()
	if ok {
		ch <- ipc.ToolResultMsg{Result: map[string]any{"_topics_list": msg}}
	}
}

func (a *adapter) dispatchFetchQueueResult(raw []byte) {
	var resp ipc.FetchQueueResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		return
	}
	a.fqmu.Lock()
	ch, ok := a.fqPending[resp.ID]
	delete(a.fqPending, resp.ID)
	a.fqmu.Unlock()
	if ok {
		ch <- resp
	}
}

func (a *adapter) dispatchRetranscribeResult(raw []byte) {
	var resp ipc.RetranscribeResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		return
	}
	a.rtmu.Lock()
	ch, ok := a.rtPending[resp.ID]
	delete(a.rtPending, resp.ID)
	a.rtmu.Unlock()
	if ok {
		ch <- resp
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

func (a *adapter) idleStartupWatchdog(ctx context.Context, cancel context.CancelFunc) {
	timer := time.NewTimer(idleStartupTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		if !a.dispatched.Load() {
			log.Printf("adapter: idle-startup timeout pid=%d (no MCP frame in %v) — exiting so host can respawn",
				os.Getpid(), idleStartupTimeout)
			cancel()
		}
	}
}

func (a *adapter) buildMCPServer() *mcp.Server {
	opts := &mcp.ServerOptions{
		Instructions: a.buildInstructions(),
		Capabilities: &mcp.ServerCapabilities{
			Tools:   &mcp.ToolCapabilities{ListChanged: false},
			Logging: &mcp.LoggingCapabilities{},
		},
	}
	srv := mcp.NewServer(&mcp.Implementation{
		Name:    adapterName,
		Version: adapterVersion,
	}, opts)

	srv.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			a.dispatched.Store(true)
			return next(ctx, method, req)
		}
	})

	a.registerTools(srv)
	a.registerPrompts(srv)
	return srv
}

// registerPrompts declares C3's MCP prompts. Cursor Agent CLI (and the IDE)
// can surface MCP prompts as slash commands — `fetch-queue` drains the durable
// queue in one deterministic step and injects the messages into the turn, with
// no "please check my messages" sentence and no tool-call reasoning turn.
// Name is kebab-case and distinct from the underscore `fetch_queue` TOOL.
func (a *adapter) registerPrompts(srv *mcp.Server) {
	srv.AddPrompt(&mcp.Prompt{
		Name:        "fetch-queue",
		Title:       "Fetch C3 queue",
		Description: "Pull inbound Telegram messages held in C3's durable queue for the attached topic and drop them straight into the chat — a one-keystroke alternative to asking the agent to call fetch_queue. Drains everything by default; pass limit=N for the N oldest, or ack=false to peek without consuming.",
		Arguments: []*mcp.PromptArgument{
			{Name: "limit", Description: "How many oldest messages to pull: a number, or \"all\" (default)."},
			{Name: "ack", Description: "\"false\" to peek without consuming (leaves them queued). Default \"true\" — drain."},
		},
	}, a.promptFetchQueue)
}

// promptFetchQueue backs the fetch-queue MCP prompt / slash command. Defaults
// to draining the whole queue; limit narrows it and ack=false peeks.
func (a *adapter) promptFetchQueue(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
	limit, all := 0, true // default: drain all
	ack := true
	if req != nil && req.Params != nil {
		if v, ok := req.Params.Arguments["limit"]; ok {
			limit, all = parseFetchLimitStr(v)
		}
		if v, ok := req.Params.Arguments["ack"]; ok {
			v = strings.TrimSpace(v)
			ack = !(strings.EqualFold(v, "false") || v == "0" || strings.EqualFold(v, "no"))
		}
	}

	body, _ := a.doFetchQueue(ctx, ack, limit, all)
	text := "📨 C3 queue (via /fetch-queue):\n\n" + body
	if ack {
		text += "\n\nRead these and respond or act as needed — use the `reply` tool to answer on Telegram when the user asks (CLI mode keeps replies in the terminal by default)."
	} else {
		text += "\n\n(peeked — still queued; run without ack=false to consume.)"
	}

	return &mcp.GetPromptResult{
		Description: "C3 queued Telegram messages",
		Messages: []*mcp.PromptMessage{
			{Role: "user", Content: &mcp.TextContent{Text: text}},
		},
	}, nil
}

func (a *adapter) buildInstructions() string {
	var head string
	switch {
	case a.helloAck.NoConfig:
		head = "C3 not yet configured. Run `c3-broker setup` from a shell to provide your Telegram bot token, DM chat id, and at least one group chat id, then restart this Cursor session."
	case a.helloAck.NoMapping:
		cwd := os.Getenv("C3_CURSOR_CWD")
		if cwd == "" {
			cwd, _ = os.Getwd()
		}
		head = fmt.Sprintf("No saved C3 topic for this session (cwd %q). Call the `attach` tool with no argument: the broker returns a picker of suggested topics — list them for the user and let them choose (never guess), then re-invoke `attach` with the chosen `topic_id` or `name`. Or attach a specific topic directly with `attach(name=\"<name>\")`. Inbound Telegram messages are held in C3's durable queue; call `fetch_queue` (or the `/fetch-queue` / `/c3-fetch` slash command) to read them.", cwd)
	default:
		head = "C3 connected. Use `attach` to claim a Telegram topic, `fetch_queue` to read held/new inbound, `reply` to send. Cursor doesn't render unsolicited MCP notifications today — call `fetch_queue`, or use the `/fetch-queue` MCP prompt / `/c3-fetch` slash command to drop the queue into the turn."
	}
	return head + mode.Combined(a.capsOrDefault())
}

func (a *adapter) capsOrDefault() c3types.Capabilities {
	if a.helloAck.Capabilities != nil {
		return *a.helloAck.Capabilities
	}
	return c3types.Capabilities{}
}

func (a *adapter) toolForward(name string) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args, err := decodeArgs(req.Params.Arguments)
		if err != nil {
			return nil, fmt.Errorf("invalid params: %w", err)
		}
		id := strconv.FormatUint(a.nextID.Add(1), 10)
		ch := make(chan ipc.ToolResultMsg, 1)
		a.pmu.Lock()
		a.pending[id] = ch
		a.pmu.Unlock()
		defer func() { a.pmu.Lock(); delete(a.pending, id); a.pmu.Unlock() }()

		conn := a.currentConn()
		if conn == nil {
			return toolErrorResult("broker reconnecting — retry " + name + " in a moment"), nil
		}
		if err := conn.WriteJSON(ipc.ToolCallReq{Op: ipc.OpToolCall, ID: id, Name: name, Args: args}); err != nil {
			return toolErrorResult("broker write: " + err.Error()), nil
		}
		select {
		case <-ctx.Done():
			return toolErrorResult("canceled"), nil
		case <-time.After(120 * time.Second):
			return toolErrorResult(name + " timeout"), nil
		case res := <-ch:
			if res.Error != nil {
				return toolErrorResult(res.Error.Message), nil
			}
			return mapResult(res.Result), nil
		}
	}
}

func (a *adapter) toolAttach(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := decodeArgs(req.Params.Arguments)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	cwd := os.Getenv("C3_CURSOR_CWD")
	if cwd == "" {
		cwd, _ = os.Getwd()
	}
	attachReq := ipc.AttachReq{Op: ipc.OpAttach, CWD: cwd}
	if v, ok := args["target"].(string); ok {
		attachReq.Target = v
	}
	if v, ok := args["name"].(string); ok {
		attachReq.Name = v
	}
	if v, ok := args["group"].(string); ok {
		attachReq.Group = v
	}
	if v, ok := args["create"].(bool); ok {
		attachReq.Create = v
	}
	if v, ok := args["steal"].(bool); ok {
		attachReq.Steal = v
	}
	if v, ok := args["policy_rejected"].(bool); ok {
		attachReq.PolicyRejected = v
	}
	if v, ok := args["topic_id"]; ok {
		switch x := v.(type) {
		case float64:
			id := int64(x)
			attachReq.TopicID = &id
		case int64:
			attachReq.TopicID = &x
		}
	}

	// Register stable session id before attach. fireRecover is a no-op when a
	// recover already fired on this connection (its CompareAndSwap), so this is
	// safe to call unconditionally — but a no-op RETURN is not an answer: the
	// recover that DID fire may still be in flight, and the attach below is
	// ANSWERED from the identity it is carrying. Wait for the identity question
	// to be settled before the attach goes on the wire.
	sid := resolveCursorSessionID()
	if sid != "" {
		a.recoverStarted.Store(true)
		a.fireRecover(ctx, sid, cwd)
	}
	epoch, err := a.awaitCurrentIdentityEpoch(ctx, isBareAttachReq(attachReq))
	if err != nil {
		if errors.Is(err, errIdentityStillResolving) {
			return toolErrorResult(err.Error()), nil
		}
		return toolErrorResult("canceled"), nil
	}

	ch := make(chan ipc.ToolResultMsg, 1)
	a.pmu.Lock()
	a.pending["attached"] = ch
	a.pmu.Unlock()

	// Use the connection that answered the epoch we just waited on. Reading the
	// latest connection here would let an old waiter adopt a freshly reconnected
	// broker before that broker learned this session's stable identity.
	conn := epoch.conn
	if conn == nil {
		a.pmu.Lock()
		delete(a.pending, "attached")
		a.pmu.Unlock()
		return toolErrorResult("broker reconnecting — retry attach in a moment"), nil
	}

	if err := conn.WriteJSON(attachReq); err != nil {
		a.pmu.Lock()
		delete(a.pending, "attached")
		a.pmu.Unlock()
		return toolErrorResult("broker write: " + err.Error()), nil
	}
	select {
	case <-ctx.Done():
		return toolErrorResult("canceled"), nil
	case res := <-ch:
		attached, _ := res.Result["_attached"].(ipc.AttachedMsg)
		if attached.OK {
			a.rememberAttach(resolvedAttachReq(attachReq, attached))
			a.setAttachedTopic(attached.Name)
			termtitle.EmitAttach(&attached)
		}
		text := ipc.FormatAttached(&attached)
		if summary := renderBacklogSummary(attached.QueuedCount, attached.QueuedSummary, attached.Name); summary != "" {
			text += "\n\n" + summary
		}
		return toolTextResult(text), nil
	}
}

func (a *adapter) toolTopics(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ch := make(chan ipc.ToolResultMsg, 1)
	a.pmu.Lock()
	a.pending["topics_list"] = ch
	a.pmu.Unlock()
	conn := a.currentConn()
	if conn == nil {
		a.pmu.Lock()
		delete(a.pending, "topics_list")
		a.pmu.Unlock()
		return toolErrorResult("broker reconnecting — retry topics in a moment"), nil
	}
	if err := conn.WriteJSON(ipc.ListTopicsReq{Op: ipc.OpListTopics}); err != nil {
		a.pmu.Lock()
		delete(a.pending, "topics_list")
		a.pmu.Unlock()
		return toolErrorResult("broker write: " + err.Error()), nil
	}
	select {
	case <-ctx.Done():
		return toolErrorResult("canceled"), nil
	case res := <-ch:
		list, _ := res.Result["_topics_list"].(ipc.TopicsListMsg)
		return toolTextResult(ipc.FormatTopics(&list)), nil
	}
}

func (a *adapter) toolDetach(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn := a.currentConn()
	if conn == nil {
		return toolErrorResult("broker reconnecting — retry detach in a moment"), nil
	}
	if !ipc.ProtocolStateChangesCompatible(int(a.brokerVersion.Load())) {
		return toolErrorResult("detach refused: broker protocol is outside the state-change compatibility window; restart the CLI"), nil
	}
	req := ipc.ReleaseReq{Op: ipc.OpRelease}
	if err := conn.WriteJSON(req); err != nil {
		return toolErrorResult("broker write: " + err.Error()), nil
	}
	a.amu.Lock()
	a.lastAttach = nil
	a.attachedTopic = ""
	a.amu.Unlock()
	termtitle.Clear()
	return toolTextResult("detached successfully"), nil
}

func (a *adapter) toolFetchQueue(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := decodeArgs(req.Params.Arguments)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	ack := true
	if v, ok := args["ack"].(bool); ok {
		ack = v
	}
	limit, all := parseFetchLimit(args["limit"])
	text, isErr := a.doFetchQueue(ctx, ack, limit, all)
	if isErr {
		return toolErrorResult(text), nil
	}
	return toolTextResult(text), nil
}

// doFetchQueue runs one fetch_queue broker round-trip and returns the rendered
// text. Shared by the `fetch_queue` TOOL and the `fetch-queue` PROMPT so both
// consume the queue identically. isErr distinguishes a broker/transport failure
// from a successful fetch (which may legitimately be "queue is empty").
func (a *adapter) doFetchQueue(ctx context.Context, ack bool, limit int, all bool) (text string, isErr bool) {
	fq := ipc.FetchQueueReq{
		Op:    ipc.OpFetchQueue,
		ID:    strconv.FormatUint(a.nextID.Add(1), 10),
		Ack:   ack,
		Limit: limit,
		All:   all,
	}

	ch := make(chan ipc.FetchQueueResp, 1)
	a.fqmu.Lock()
	a.fqPending[fq.ID] = ch
	a.fqmu.Unlock()
	defer func() { a.fqmu.Lock(); delete(a.fqPending, fq.ID); a.fqmu.Unlock() }()

	conn := a.currentConn()
	if conn == nil {
		return "broker reconnecting — retry fetch_queue in a moment", true
	}
	if err := conn.WriteJSON(fq); err != nil {
		return "broker write: " + err.Error(), true
	}
	select {
	case <-ctx.Done():
		return "canceled", true
	case <-time.After(120 * time.Second):
		return "fetch_queue timeout", true
	case resp := <-ch:
		if resp.Err != "" {
			return resp.Err, true
		}
		if len(resp.Messages) == 0 && a.currentTopicName() == "" {
			return "⟦c3 not-attached⟧ This session isn't attached to any topic, so there's nowhere to look — an empty result here means \"not attached\", not \"no new mail\". Call `attach` (no args) to pick a topic this session used before.", false
		}
		return renderFetchedMessages(resp.Messages, resp.Remaining, a.currentTopicName()), false
	}
}

func (a *adapter) toolRetranscribe(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := decodeArgs(req.Params.Arguments)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	fileID, _ := args["file_id"].(string)
	if fileID == "" {
		return toolErrorResult("retranscribe: file_id is required"), nil
	}
	rt := ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: strconv.FormatUint(a.nextID.Add(1), 10), FileID: fileID}
	if v, ok := args["message_id"].(float64); ok {
		rt.MessageID = int64(v)
	}
	ch := make(chan ipc.RetranscribeResp, 1)
	a.rtmu.Lock()
	a.rtPending[rt.ID] = ch
	a.rtmu.Unlock()
	defer func() { a.rtmu.Lock(); delete(a.rtPending, rt.ID); a.rtmu.Unlock() }()

	conn := a.currentConn()
	if conn == nil {
		return toolErrorResult("broker reconnecting — retry retranscribe in a moment"), nil
	}
	if err := conn.WriteJSON(rt); err != nil {
		return toolErrorResult("broker write: " + err.Error()), nil
	}
	select {
	case <-ctx.Done():
		return toolErrorResult("canceled"), nil
	case <-time.After(120 * time.Second):
		return toolErrorResult("retranscribe timeout"), nil
	case resp := <-ch:
		if resp.Err != "" {
			return toolErrorResult(resp.Err), nil
		}
		return toolTextResult("re-transcribed: Fresh transcript: " + resp.Text), nil
	}
}

func (a *adapter) registerTools(srv *mcp.Server) {
	caps := a.capsOrDefault()
	tools := []struct {
		tool    *mcp.Tool
		handler mcp.ToolHandler
	}{
		{
			tool: &mcp.Tool{
				Name:        "attach",
				Description: "Attach this session to a Telegram topic. Empty = silently re-attach this session's own topic, or (first time) show a picker. `target='dm'` for DM. `name='X'` for a topic name. `topic_id=N` to claim a known thread. `create=true` to confirm creation. `steal=true` only after user-confirmed force_steal.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"target":          map[string]any{"type": "string"},
						"name":            map[string]any{"type": "string"},
						"topic_id":        map[string]any{"type": "integer"},
						"group":           map[string]any{"type": "string"},
						"create":          map[string]any{"type": "boolean"},
						"steal":           map[string]any{"type": "boolean"},
						"policy_rejected": map[string]any{"type": "boolean"},
					},
				},
			},
			handler: a.toolAttach,
		},
		{
			tool: &mcp.Tool{
				Name:        "topics",
				Description: "List known Telegram topics + claim state.",
				InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
			},
			handler: a.toolTopics,
		},
		{
			tool: &mcp.Tool{
				Name:        "fetch_queue",
				Description: "Retrieve inbound Telegram messages held in the durable queue for the attached topic (messages that arrived while no session was attached, or that a live push didn't confirm). `limit` is how many oldest messages to pull (default 3; or pass the string \"all\" to drain everything). `ack` (default true) consumes them (advances the cursor); ack=false peeks without consuming. Drain all at once for bulk catch-up, or pull in small batches (default 3) to process carefully one group at a time. Returns full content (text/transcript, sender, attachments with file_id) plus how many remain.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"limit": map[string]any{"description": "integer (default 3, max 50) or the string \"all\""},
						"ack":   map[string]any{"type": "boolean", "default": true},
					},
				},
			},
			handler: a.toolFetchQueue,
		},
		{
			tool: &mcp.Tool{
				Name:        "retranscribe",
				Description: "Re-run speech-to-text on a voice message by file_id (downloading the audio if not cached) and return the fresh transcript. Use this after a '[voice transcription failed]' message once the STT provider is healthy again — the audio is saved, so the user never has to resend. Optional `message_id` refreshes the stored transcript when that message is still queued.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"file_id":    map[string]any{"type": "string"},
						"message_id": map[string]any{"type": "integer"},
					},
					"required": []string{"file_id"},
				},
			},
			handler: a.toolRetranscribe,
		},
		{
			tool: &mcp.Tool{
				Name:        "reply",
				Description: "Send a Telegram reply to the currently-attached topic. The `text` is markdown — use formatting (lists, tables, code blocks, bold, block quotes) whenever it makes the reply easier to read; keep one-line answers plain. Attach media via the `media` array: kind=\"file\" delivers the ORIGINAL bytes (PDFs, logs); kind=\"photo\" is a COMPRESSED in-chat preview; also video/audio/voice/animation. Each item is sent as its own message after the text.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"text":     map[string]any{"type": "string"},
						"reply_to": map[string]any{"type": "integer"},
						"media":    mcptools.ReplyMediaSchema(caps),
						"buttons":  mcptools.ReplyButtonsSchema(),
					},
					"required": []string{"text"},
				},
			},
			handler: a.toolForward("reply"),
		},
		{
			tool: &mcp.Tool{
				Name:        "react",
				Description: "Set a single-emoji reaction on a Telegram message.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"message_id": map[string]any{"type": "integer"},
						"emoji":      map[string]any{"type": "string"},
					},
					"required": []string{"message_id", "emoji"},
				},
			},
			handler: a.toolForward("react"),
		},
		{
			tool: &mcp.Tool{
				Name:        "edit_message",
				Description: "Edit a previously-sent Telegram message.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"message_id": map[string]any{"type": "integer"},
						"text":       map[string]any{"type": "string"},
					},
					"required": []string{"message_id", "text"},
				},
			},
			handler: a.toolForward("edit_message"),
		},
		{
			tool: &mcp.Tool{
				Name:        "poll",
				Description: "Send a Telegram poll to the attached topic. Provide a `question` and 2+ `options`. `anonymous` (default true) and `multiple` (default false) tune the poll.",
				InputSchema: mcptools.PollToolSchema(),
			},
			handler: a.toolForward("poll"),
		},
		{
			tool: &mcp.Tool{
				Name:        "stop_poll",
				Description: "Force-close a poll you sent and read its final aggregate tally (counts per option + total voters). Pass the `message_id` returned when you sent the poll. This host is pull-only (it cannot render channel-event pushes), so the automatic poll-close event is NOT delivered here — poll results do NOT arrive on their own, and because channel events are never queued they are not recoverable via `fetch_queue` either. stop_poll is the reliable, deterministic way to read the tally.",
				InputSchema: mcptools.StopPollToolSchema(),
			},
			handler: a.toolForward("stop_poll"),
		},
		{
			tool: &mcp.Tool{
				Name:        "download_attachment",
				Description: "Download a Telegram file by file_id; returns the local path.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"file_id": map[string]any{"type": "string"},
					},
					"required": []string{"file_id"},
				},
			},
			handler: a.toolForward("download_attachment"),
		},
		{
			tool: &mcp.Tool{
				Name:        "detach",
				Description: "Release this session's current Telegram topic claim. After detach, inbound messages on that route fall through to the broker's fallback. No-op if not attached.",
				InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
			},
			handler: a.toolDetach,
		},
	}
	for _, t := range tools {
		srv.AddTool(t.tool, t.handler)
	}
}

// parseFetchLimit normalizes the `limit` tool argument into (limit, all). The
// agent may pass "all" (drain everything, case-insensitive), a JSON number, OR a
// numeric STRING like "5" (some MCP clients serialize an integer field as a
// string). A parseable numeric value is honored and clamped to [1,50]; "all"
// sets All; anything unparseable (or absent) yields the spec default of 3. It
// NEVER returns a negative Limit — the broker worker treats a negative limit as
// the consume-ALL sentinel (internal/broker/queue_dispatch.go), so clamping here
// is what protects the durable queue from a stray limit:-1 draining it. Pure +
// unit-tested. (Parity with the Grok/Claude adapters' parseFetchLimit.)
func parseFetchLimit(v any) (limit int, all bool) {
	switch t := v.(type) {
	case string:
		if strings.EqualFold(t, "all") {
			return 0, true
		}
		// A parseable numeric string ("5", "0", "999") is honored and clamped to
		// [1,50]; an unparseable string leaves limit 0 so it falls back to the
		// default below.
		if n, err := strconv.Atoi(strings.TrimSpace(t)); err == nil {
			if n < 1 {
				n = 1
			}
			if n > 50 {
				n = 50
			}
			return n, false
		}
	case float64:
		limit = int(t)
	}
	if limit <= 0 {
		limit = 3 // spec default
	}
	if limit > 50 {
		limit = 50
	}
	return limit, false
}

// parseFetchLimitStr parses the `limit` argument of the fetch-queue PROMPT,
// whose arguments arrive as strings. Empty or "all" ⇒ drain everything (the
// slash-command default); a positive integer ⇒ that many oldest; anything else
// falls back to draining all.
func parseFetchLimitStr(s string) (limit int, all bool) {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "all") {
		return 0, true
	}
	if n, err := strconv.Atoi(s); err == nil && n > 0 {
		if n > 50 {
			n = 50
		}
		return n, false
	}
	return 0, true
}

func renderBacklogSummary(count int, items []ipc.QueuedItem, route string) string {
	if count <= 0 {
		return ""
	}
	var sb strings.Builder
	if route != "" {
		fmt.Fprintf(&sb, "📨 %d message(s) for topic %q were held while no session was attached. Call `fetch_queue` (limit:3 or \"all\") to retrieve them.", count, route)
	} else {
		fmt.Fprintf(&sb, "📨 %d message(s) were held while no session was attached. Call `fetch_queue` (limit:3 or \"all\") to retrieve them.", count)
	}
	for _, it := range items {
		preview := it.Preview
		if preview == "" {
			preview = "(" + it.Kind + ")"
		}
		fmt.Fprintf(&sb, "\n  • [%d] %s %s: %s", it.MessageID, it.Sender, it.Kind, preview)
	}
	if count > len(items) {
		fmt.Fprintf(&sb, "\n  …and %d more", count-len(items))
	}
	return sb.String()
}

func pendingNudge(n int, route string) string {
	if route != "" {
		return fmt.Sprintf("(%d pending for topic %q — call `fetch_queue`)", n, route)
	}
	return fmt.Sprintf("(%d pending — call `fetch_queue`)", n)
}

func renderFetchedMessages(msgs []c3types.Inbound, remaining int, route string) string {
	if len(msgs) == 0 {
		return "c3 queue is empty"
	}
	blocks := make([]string, 0, len(msgs))
	for i := range msgs {
		blocks = append(blocks, renderQueuedInbound(&msgs[i]))
	}
	out := strings.Join(blocks, "\n\n")
	// #55 (2026-07-24): the per-message attachment block is trimmed to kind +
	// file_id; the "how to open it" instruction is shown ONCE per batch.
	if c3types.InboundsHaveAttachment(msgs) {
		out = c3types.AttachmentFetchHint + "\n\n" + out
	}
	if remaining > 0 {
		out += "\n\n" + pendingNudge(remaining, route)
	}
	return out
}

// renderQueuedInbound renders one queued message for fetch_queue output in the
// trimmed form approved 2026-07-24 (task #55): bare message text, then a compact
// metadata line (sender, message_id, reply context, kind+file_id attachment
// reference). The shared c3types renderer keeps this byte-identical across every
// adapter.
func renderQueuedInbound(in *c3types.Inbound) string {
	return c3types.RenderQueuedInbound(in)
}

func decodeArgs(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return map[string]any{}, nil
	}
	var args map[string]any
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, err
	}
	if args == nil {
		args = map[string]any{}
	}
	return args, nil
}

func toolErrorResult(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{
				Text: "Error: " + msg,
			},
		},
		IsError: true,
	}
}

func toolTextResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{
				Text: text,
			},
		},
	}
}

// mapResult converts a broker-returned result map into a CallToolResult. The
// broker always returns the standard MCP shape
// `{"content":[{"type":"text","text":…}]}` (internal/broker/dispatch.go mcpText)
// — never a top-level "text" key — so we translate each content element into an
// SDK text block. The JSON-encoded dump is kept ONLY as the true fallback for
// when "content" is absent or malformed. (Parity with the Grok/Claude adapters'
// toolResultFromMap.)
func mapResult(result map[string]any) *mcp.CallToolResult {
	if result == nil {
		return toolTextResult("")
	}
	if contentRaw, ok := result["content"]; ok {
		if items, ok := contentRaw.([]any); ok {
			var blocks []mcp.Content
			for _, item := range items {
				m, ok := item.(map[string]any)
				if !ok {
					continue
				}
				text, _ := m["text"].(string)
				blocks = append(blocks, &mcp.TextContent{Text: text})
			}
			if len(blocks) > 0 {
				return &mcp.CallToolResult{Content: blocks}
			}
		}
	}
	// Fallback: JSON-encode the whole result map.
	enc, err := json.Marshal(result)
	if err != nil {
		return toolErrorResult("marshal result: " + err.Error())
	}
	return toolTextResult(string(enc))
}
