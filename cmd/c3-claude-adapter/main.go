// c3-claude-adapter is the Claude Code MCP server that bridges Claude Code's
// MCP stdio protocol to the C3 broker over /tmp/c3.sock (or
// $XDG_RUNTIME_DIR/c3.sock).
//
// Spec §4.4. The adapter:
//
//  1. On stdin: accept JSON-RPC 2.0 requests from Claude Code (initialize,
//     tools/list, tools/call, ping, notifications/initialized).
//  2. On the broker socket: connect, send hello, listen for inbound /
//     tool_result / topics_list frames asynchronously.
//  3. For tools/call: route adapter-local tools (`attach`, `topics`, etc.)
//     directly; forward all other tools to the broker as ipc.OpToolCall and
//     return the result.
//  4. For broker-side ipc.OpInbound frames: emit `notifications/claude/channel`
//     via the custom notifyTransport (see notify_transport.go) so the
//     notification ends up on the same stdout the SDK uses, with a single
//     newline-terminated JSON frame matching the official Telegram plugin's
//     wire shape (CC's channel-dispatch validator silently drops malformed
//     frames; the wire shape must be exact).
//
// Reconnect-once policy from spec §4.4 has been upgraded to reconnect-forever
// with exponential backoff. The original 1-shot policy turned a 30s broker
// rebuild into a permanently dead adapter that required restarting the CLI
// session.
//
// MCP wire layer: github.com/modelcontextprotocol/go-sdk (v1.6.0+). All
// hand-rolled JSON-RPC framing has been migrated to the SDK; the only
// adapter-owned framing is the custom `notifications/claude/channel`
// notification, which the SDK's typed Notify API doesn't support (it locks
// outgoing methods to the spec's known set). For that single path we synthesise
// `*jsonrpc.Request` notifications via notifyTransport.Notify (see
// notify_transport.go for the rationale and wire-shape guarantees).
package main

import (
	"context"
	"crypto/rand"
	"encoding/base32"
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
	"github.com/Andrometiq/c3/internal/sessionhandoff"
	"github.com/Andrometiq/c3/internal/spawn"
	"github.com/Andrometiq/c3/internal/termtitle"
)

// idleStartupTimeout bounds how long the adapter waits for Claude Code to
// send its first MCP frame (initialize). If Claude Code spawns the adapter
// but then abandons it without driving stdin — observed during `--resume`
// flows where CC respawns MCP servers and the prior process is orphaned
// alive — the adapter exits cleanly after this window rather than living
// as a zombie holding a broker conn. 60s is well past the millisecond-scale
// MCP handshake budget; any real Claude Code session is past initialize
// long before this fires.
const idleStartupTimeout = 60 * time.Second

const (
	// adapterName MUST match the .mcp.json key so Claude Code's channel
	// dispatch recognises this server as the same one it spawned. Reference
	// implementations (~/.claude/plugins/.../fakechat/server.ts:60,
	// .../telegram/0.0.6/server.ts:371) all set serverInfo.name == .mcp.json
	// key. Using the binary name here was a guess that broke channel
	// notification surfacing in 2026-05-09 testing — broker delivered, this
	// frame went out correctly, but Claude Code never injected it as a
	// channel event.
	adapterName    = "c3"
	adapterVersion = "0.1.0"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "c3-claude-adapter: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Persistent adapter log at $XDG_STATE_HOME/c3/adapter.log. Adapter stderr
	// is socket-paired to Claude Code's plugin host and inaccessible from
	// outside; the file is the durable signal for "did the adapter send the
	// notification, and what did it look like." Same content policy as the
	// broker (DEBUGGING.md): metadata only on success, content on failure.
	if path, err := setupAdapterLog(); err == nil {
		fmt.Fprintf(os.Stderr, "c3-claude-adapter: log file %s\n", path)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	installSignalHandlers(cancel)

	a := newAdapter()
	a.runCtx = ctx
	// Detect once whether this host can render channel push notifications. A
	// flagless launch (typically a --fork-session background job) silently drops
	// them, so the broker must hold such a session's inbound in the durable queue
	// rather than let the adapter ack it as delivered. Cheap /proc walk; the
	// process tree is fixed for the session.
	a.hostRenderCapable = hostCanRenderChannels()
	if !a.hostRenderCapable {
		log.Printf("adapter: host CANNOT render channel pushes (launched without %s naming plugin c3) — inbound will be QUEUED for fetch_queue, not pushed", devChannelsFlag)
	}
	if err := a.connectBroker(); err != nil {
		log.Printf("adapter: exit pid=%d reason=connect-broker err=%v", os.Getpid(), err)
		return fmt.Errorf("connect broker: %w", err)
	}
	if err := a.hello(); err != nil {
		log.Printf("adapter: exit pid=%d reason=hello err=%v", os.Getpid(), err)
		return fmt.Errorf("hello: %w", err)
	}

	server := a.buildMCPServer()
	a.notifyTx = newNotifyTransport(&mcp.StdioTransport{})
	// Wire the receive interceptor: a diverted permission_request is relayed to the
	// broker (Allow/Deny keyboard) instead of being silently dropped by the SDK.
	// Must be set BEFORE server.Run drives notifyTx.Connect.
	a.notifyTx.SetPermissionHandler(a.handlePermissionRequest)

	go a.brokerReader(ctx)
	go a.recoverSessionOnResume(ctx)
	go a.idleStartupWatchdog(ctx, cancel)

	err := server.Run(ctx, a.notifyTx)
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

// installSignalHandlers cancels ctx on SIGTERM/SIGINT/SIGHUP. Without this,
// Go's default behavior is to terminate immediately, leaving no log line
// explaining why the adapter died. We need that breadcrumb to diagnose
// "MCP server disconnected" incidents — was it a signal from Claude Code,
// stdin EOF, or an internal error? Cancellation propagates through the
// MCP server loop so its Run() returns and main() logs the exit reason.
func installSignalHandlers(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, append([]os.Signal{syscall.SIGTERM, syscall.SIGINT}, osutil.ReloadSignals()...)...)
	go func() {
		sig := <-ch
		log.Printf("adapter: received signal=%v pid=%d", sig, os.Getpid())
		cancel()
	}()
}

// idleStartupWatchdog cancels ctx if Claude Code never sends an MCP frame
// within idleStartupTimeout. This handles the observed `--resume` failure
// mode where CC spawns an adapter, the adapter completes broker handshake,
// but CC never sends `initialize` (presumed: CC orphans the spawn during
// session resume teardown). Without this, the adapter sits reading stdin
// forever, holding a broker conn, and the user sees the plugin as
// "disconnected" until they manually `/mcp` reconnect.
func (a *adapter) idleStartupWatchdog(ctx context.Context, cancel context.CancelFunc) {
	timer := time.NewTimer(idleStartupTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		if !a.dispatched.Load() {
			log.Printf("adapter: idle-startup timeout pid=%d (no MCP frame in %v) — exiting so Claude Code can respawn",
				os.Getpid(), idleStartupTimeout)
			cancel()
		}
	}
}

// setupAdapterLog opens $XDG_STATE_HOME/c3/adapter.log (append) and tees
// stdlib log there + stderr.
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
	log.Printf("adapter: started pid=%d", os.Getpid())
	return path, nil
}

type recoverNotice struct {
	text  string
	epoch uint64
}

type recoverOutcome struct {
	attempted  bool
	registered bool
	recovered  bool
}

type adapter struct {
	// notifyTx wraps the stdio transport to permit emitting custom
	// `notifications/claude/channel` frames. Set in run() before Server.Run.
	notifyTx *notifyTransport

	// Broker connection state.
	bmu    sync.Mutex
	conn   *ipc.Conn
	connID uint64

	// Pending tool calls awaiting broker response, keyed by request id.
	pmu     sync.Mutex
	pending map[string]chan ipc.ToolResultMsg
	nextID  atomic.Uint64

	// fetch_queue / retranscribe have their own pending maps keyed by request
	// id so their typed responses don't overload the ToolResultMsg pending map.
	fqmu      sync.Mutex
	fqPending map[string]chan ipc.FetchQueueResp
	rtmu      sync.Mutex
	rtPending map[string]chan ipc.RetranscribeResp

	// recover-session permits one in-flight round-trip per adapter, so a single
	// buffered channel suffices instead of a pending map. Set by fireRecover
	// before it writes RecoverSessionReq; resolved by
	// dispatchRecoverSessionResult. recoverMu serializes complete recovery
	// attempts so rsPending's single slot can never be overwritten; rsmu guards
	// dispatch access to that slot.
	recoverMu sync.Mutex
	rsmu      sync.Mutex
	rsPending chan ipc.RecoverSessionResp

	// recoverFired guards the RecoverSessionReq to at most once per BROKER
	// CONNECTION. Both the background handoff watch (watchForHandoff) and the
	// first-tools/call belt-and-suspenders recheck call fireRecover; the
	// CompareAndSwap ensures the broker never sees a duplicate recover on one
	// connection (BUG #1). recoverBroker RESETS it after a reconnect (§3d2) and
	// re-fires, so a FRESH broker (self-update/rebuild restart, which has no sid
	// and no reconnect-transfer) re-learns this session's stable id — otherwise
	// every post-restart attach records nothing and a future --resume recovers a
	// stale own topic.
	recoverFired atomic.Bool
	// recoverStarted records that a recovery attempt has BEGUN — set
	// synchronously by every caller BEFORE it invokes fireRecover (including the
	// `go fireRecover` callers, so a caller that immediately waits on the identity
	// gate cannot race past a goroutine that has not run yet). Never reset: it
	// answers "is there an identity question in flight for this process?", which
	// is the only thing awaitIdentitySettled needs. recoverFired cannot serve that
	// role — it is reset by refireRecoverOnReconnect and released on the
	// nothing-was-sent paths, and it is set INSIDE fireRecover, one goroutine
	// hop too late.
	recoverStarted atomic.Bool
	// identitySettled is closed once the current recovery epoch has FINISHED —
	// recovered, registered, refused, failed, or timed out. It is the completion
	// signal recoverFired was being misread as: recoverFired records ENTRY into
	// fireRecover, and entry is not an answer. toolAttach waits on this so an
	// attach is never answered while this session still doesn't know who it is.
	//
	// Created lazily by identityGate (not in newAdapter) so an adapter built as a
	// bare struct literal behaves identically to one from the constructor.
	// An in-app conversation switch reopens it with reopenIdentity; same-id
	// reconnect registration does not.
	idmu                sync.Mutex
	identitySettled     chan struct{}
	idEpoch             uint64
	currentStableID     string
	currentHandoffEntry sessionhandoff.Entry
	// recoverRechecked makes the first-tools/call belt-and-suspenders recheck run
	// at most once, so a non-resume session doesn't re-stat the handoff file on
	// every subsequent tools/call.
	recoverRechecked atomic.Bool
	// switchWatchStarted guards the process-lifetime 5s watch to one goroutine,
	// including the case where the first successful registration happens only
	// after a broker reconnect.
	switchWatchStarted atomic.Bool
	// runCtx is the process-lifetime context (set in run() before the MCP server
	// starts). The first-activity recheck fires its recover round-trip against
	// this rather than the per-request context, which is cancelled when the
	// triggering tools/call returns.
	runCtx context.Context

	// ask round-trip pending maps, keyed by askID (8-char base32 generated in
	// toolAsk). askRegPending receives the broker's synchronous OpAskRegistered
	// ack; askPending receives the later unsolicited OpAskResult once the human
	// taps. Both guarded by askmu. Mirrors the fqPending fire-then-push pattern.
	askmu         sync.Mutex
	askRegPending map[string]chan ipc.AskRegisteredMsg
	askPending    map[string]chan ipc.AskResultMsg

	// hostRenderCapable is whether the launching Claude Code host can render
	// notifications/claude/channel pushes. Detected once from the /proc ancestor
	// chain in run() (hostCanRenderChannels). Default TRUE (set in newAdapter) so
	// an uncertain environment — and unrelated unit tests that never run detection
	// — keep the normal fast path. Reported to the broker at hello (inverted, as
	// CannotRenderChannels) and surfaced to the agent in buildInstructions when
	// false. See render_detect.go for the detection + the blackhole it defends.
	hostRenderCapable bool

	// Hello-ack response state, captured on connect.
	helloAck ipc.HelloAckMsg

	// Last successful attach request — replayed on broker reconnect so a
	// session that survives a broker restart auto-reclaims its route. Nil
	// if the user hasn't attached yet (or detached).
	amu        sync.Mutex
	lastAttach *ipc.AttachReq
	// attachedTopic is the human-readable name of the currently-attached topic
	// (attached.Name / resp.Name at attach time). Read when rendering the backlog
	// and pending-fetch nudges so a human can see WHICH topic a fetch_queue would
	// drain — a stale/wrong advertisement is then distinguishable rather than an
	// anonymous "N pending" (spec §5). Empty when unattached. Guarded by amu.
	attachedTopic string

	// firstInbound triggers a one-shot wire dump of the first
	// notifications/claude/channel frame for live debugging.
	firstInbound atomic.Bool

	// brokerDownAdvised guards the D5 one-shot "broker unreachable" advisory so
	// it surfaces once per outage, not on every recovery cycle. Cleared on a
	// successful reconnect so a later outage re-advises. Mirrors the Codex adapter.
	brokerDownAdvised atomic.Bool

	// dispatched is set the first time the SDK runs a method handler — i.e.
	// Claude Code has sent at least one MCP frame. The idle-startup watchdog
	// uses this to distinguish "live session" from "orphaned spawn that
	// Claude Code never drove". The receiving-middleware in buildMCPServer
	// flips this on first call.
	dispatched atomic.Bool

	// pendingRecoverNotice holds the auto-attach-on-resume CLI notice until the
	// session is active. recoverSessionOnResume sets it instead of emitting at
	// recover time, because a channel frame emitted in the resume idle gap
	// (right after the handshake, before the first user turn) is dropped by
	// Claude Code rather than buffered (2026-06-24). The receiving middleware
	// flushes it on the first tools/call — an active turn, where the frame
	// renders. At flush the adapter live re-peeks the held count (fetch_queue,
	// Ack=false) and emits an accurate frame; this stored text is the FALLBACK
	// used when that re-peek times out / errors. It persists until the first
	// tools/call however late (NO TTL): the live re-peek makes a stale stored
	// count impossible, so the old 5-minute drop — a silent-loss-of-awareness
	// bug on a resume idle >5min — is gone. The Telegram recover-welcome is the
	// GUARANTEED signal; this is the best-effort CLI echo. Each notice is stamped
	// with idEpoch so an old conversation's banner cannot cross a switch.
	// Guarded by pnmu.
	pnmu                 sync.Mutex
	pendingRecoverNotice recoverNotice
}

func newAdapter() *adapter {
	return &adapter{
		pending:       map[string]chan ipc.ToolResultMsg{},
		fqPending:     map[string]chan ipc.FetchQueueResp{},
		rtPending:     map[string]chan ipc.RetranscribeResp{},
		askRegPending: map[string]chan ipc.AskRegisteredMsg{},
		askPending:    map[string]chan ipc.AskResultMsg{},
		// Default capable; run() overwrites with the real /proc detection. Keeping
		// the default TRUE means every unit test that constructs an adapter without
		// running detection behaves as a normal (renderable) session.
		hostRenderCapable: true,
	}
}

// connectBroker dials the broker socket, spawning the broker if unreachable.
func (a *adapter) connectBroker() error {
	sockPath := broker.SocketPath()
	for attempt := 0; attempt < 50; attempt++ { // ~10s with 200ms sleep
		c, err := net.Dial("unix", sockPath)
		if err == nil {
			a.bmu.Lock()
			a.conn = ipc.NewConn(c)
			a.bmu.Unlock()
			return nil
		}
		if attempt == 0 {
			// First failure: spawn a broker, then retry.
			_ = spawnBroker()
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("could not reach broker at %s after 10s", sockPath)
}

// spawnBroker forks a `c3-broker` process detached from our process group so
// it survives our shutdown. The detached-launch semantics (setsid, no stdio,
// async reap) live in internal/spawn, shared with the Codex adapter (D7).
func spawnBroker() error {
	return spawn.Detached(exec.Command("c3-broker"))
}

// instanceIDFromEnv returns Claude Code's EPHEMERAL per-MCP-spawn id, exported
// to stdio MCP servers as CLAUDE_CODE_SESSION_ID. Despite the name, this is NOT
// the stable --resume id — it equals the UUID directory in the SessionStart
// hook's $CLAUDE_ENV_FILE path, so the adapter uses it to look up its own
// SessionStart-hook handoff (which carries the real stable id). Empty when unset
// (non-Claude-Code host / no hook) → recovery is skipped (fail-closed).
func instanceIDFromEnv() string { return os.Getenv("CLAUDE_CODE_SESSION_ID") }

func (a *adapter) hello() error {
	cwd, _ := os.Getwd()
	if err := a.conn.WriteJSON(ipc.HelloMsg{
		Op: ipc.OpHello, CLI: "claude", PID: os.Getpid(), CWD: cwd,
		Capabilities: []string{"claude/channel"},
		// Inverted: set only when we're confident the host drops channel pushes,
		// so the broker holds our inbound in the queue instead of acking it lost.
		CannotRenderChannels: !a.hostRenderCapable,
		ProtocolVersion:      ipc.ProtocolVersion,
	}); err != nil {
		return err
	}
	raw, err := a.conn.ReadFrame()
	if err != nil {
		return err
	}
	var ack ipc.HelloAckMsg
	if err := json.Unmarshal(raw, &ack); err != nil {
		return err
	}
	// Version disagreement is logged, never fatal — see ipc.ProtocolVersion.
	if w := ipc.AdapterProtocolWarning("claude", ack.ProtocolVersion); w != "" {
		log.Print(w)
	}
	a.helloAck = ack
	a.connID = ack.ConnID
	return nil
}

// brokerReader runs in a goroutine, draining frames from the broker. On any
// read error, runs the recovery loop (exponential backoff, no give-up) until
// either ctx is canceled or we re-establish a usable connection. After
// recovery, replays the last successful attach so the route claim is
// re-established without user intervention.
func (a *adapter) brokerReader(ctx context.Context) {
	for {
		conn := a.currentConn()
		if conn == nil {
			return
		}
		raw, err := conn.ReadFrame()
		if err != nil {
			// File-only log: CC's plugin host treats noisy stderr as
			// "distressed plugin → recycle me". Broker bounces are
			// expected and recoverable; don't telegraph them to CC.
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
		case ipc.OpInbound:
			a.handleInbound(ctx, raw)
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
		case ipc.OpAskRegistered:
			a.dispatchAskRegistered(raw)
		case ipc.OpAskResult:
			a.dispatchAskResult(raw)
		case ipc.OpPermissionVerdict:
			a.dispatchPermissionVerdict(raw)
		case ipc.OpError:
			var errMsg ipc.ErrorMsg
			_ = json.Unmarshal(raw, &errMsg)
			log.Printf("broker error: %s", errMsg.Err)
		default:
			// An op this build does not know — normally a NEWER broker (mixed
			// versions are routine after `c3 update`; see ipc.ProtocolVersion).
			// Skipping it is correct: unknown ops are additive by contract. Log
			// it so the skip is VISIBLE — a silent drop is the worst failure
			// mode there is. File-only log, like the rest of this reader.
			log.Printf("claude: ignoring unknown op %q from broker (this adapter speaks protocol v%d — the broker may be a newer c3 build; restart this CLI to match)", op, ipc.ProtocolVersion)
		}
	}
}

// reconnectBroker tears down the dead conn, dials a fresh one, sends hello.
// Pending tool calls are woken with an error so callers don't hang during
// the reconnect window. Single attempt; recoverBroker is the retry-loop
// wrapper.
func (a *adapter) reconnectBroker() error {
	a.wakePendingWithErr("broker reconnect — request canceled")

	a.bmu.Lock()
	if a.conn != nil {
		_ = a.conn.Close()
		a.conn = nil
	}
	a.bmu.Unlock()

	if err := a.connectBroker(); err != nil {
		return err
	}
	return a.hello()
}

// recoverBrokerAdviseAfter bounds how long recovery retries silently before the
// D5 loud advisory. ~30s at the 0.5→30s backoff (0.5+1+2+4+8+16 ≈ 31.5s), so
// attempt 6 trips it — long enough to ride out an ordinary broker rebuild, short
// enough that a real outage is surfaced before the user assumes inbound works.
const recoverBrokerAdviseAfter = 6

// recoverBroker loops with exponential backoff until reconnectBroker
// succeeds, or ctx cancellation aborts the loop (returns false).
// After a successful reconnect, restores the last successful attach only when
// the terminal identity is unchanged; an in-app switch skips that stale replay
// and recovers the new identity directly. If the broker is still unreachable
// after recoverBrokerAdviseAfter attempts (~30s), it surfaces a one-shot loud
// advisory so the user knows inbound is down (D5 / adapter-ipc-5) — the session
// otherwise looks alive.
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
			a.restoreSessionAfterReconnect(ctx)
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

// adviseBrokerDown surfaces a LOUD one-shot advisory to the Claude session when
// the broker can't be re-established after several recovery cycles (D5 /
// adapter-ipc-5). It synthesizes the SAME broker-originated InboundSystem event
// the broker's health broadcast uses (ChatID-less, rendered via
// buildClaudeChannelFrame's System case — the proven-visible path), but emits it
// adapter-side since the whole point is that the broker is unreachable.
// brokerDownAdvised keeps it from spamming on every retry; it is cleared on the
// next successful reconnect. (Batch E's status line also shows "C3 broker down"
// when the broker PROCESS is dead; this covers the case where THIS adapter can't
// reach an otherwise-live broker.)
func (a *adapter) adviseBrokerDown(attempt int) {
	if !a.brokerDownAdvised.CompareAndSwap(false, true) {
		return
	}
	if a.notifyTx == nil {
		return
	}
	sysev := &c3types.SystemEvent{
		Source:  "c3",
		Level:   "warn",
		Title:   "C3 broker unreachable",
		Message: fmt.Sprintf("C3 lost its connection to the broker and could not reconnect after %d attempts. Inbound Telegram messages will NOT arrive until this recovers (the adapter is still retrying in the background).", attempt),
	}
	in := &c3types.Inbound{
		Channel: "c3",
		Kind:    c3types.InboundSystem,
		Event:   &c3types.InboundEvent{System: sysev},
	}
	frame := buildClaudeChannelFrame(in)
	if err := a.notifyTx.Notify(context.Background(), "notifications/claude/channel", frame); err != nil {
		log.Printf("broker-down advisory notify failed: %v", err)
		return
	}
	log.Printf("broker-down advisory surfaced (attempt %d)", attempt)
}

// clearBrokerDownAdvisory re-arms the D5 advisory after a successful reconnect.
func (a *adapter) clearBrokerDownAdvisory() {
	a.brokerDownAdvised.Store(false)
}

// rememberAttach stores the last successful attach request for replay on
// reconnect. The pointer captures all dimensions (target/name/topic_id/
// group/create) the user originally chose.
func (a *adapter) rememberAttach(req ipc.AttachReq) {
	a.amu.Lock()
	defer a.amu.Unlock()
	cp := req
	// A steal is a ONE-SHOT human confirmation, not a standing property (item D).
	// Replaying steal=true verbatim after a broker bounce would silently
	// force-evict whoever currently holds the route (last-reconnector-wins). On a
	// fresh broker the route table is empty, so a plain replay claims fine; and a
	// route genuinely re-claimed by another session must surface a force_steal
	// proposal, not be silently evicted. So never remember the steal.
	cp.Steal = false
	a.lastAttach = &cp
}

// setAttachedTopic records the human-readable name of the currently-attached
// topic for the fetch_queue nudges (spec §5). Empty string clears it on detach.
func (a *adapter) setAttachedTopic(name string) {
	a.amu.Lock()
	defer a.amu.Unlock()
	a.attachedTopic = name
}

// currentTopicName returns the currently-attached topic name (empty when
// unattached), read on the push/fetch paths to name the fetch_queue nudge.
func (a *adapter) currentTopicName() string {
	a.amu.Lock()
	defer a.amu.Unlock()
	return a.attachedTopic
}

// isBareAttachReq reports whether an attach request carried no explicit target
// (no name-bearing Expr, Target, Name, TopicID, or Create). A bare attach that
// the broker resolves via its idempotent-already-attached branch or the
// session's own recover returns OK=true, but the raw request has nothing that a
// post-broker-restart replay could re-bind explicitly.
func isBareAttachReq(req ipc.AttachReq) bool {
	return req.Expr == "" && req.Target == "" && req.Name == "" && req.TopicID == nil && !req.Create
}

// rememberedIdentityReq builds the AttachReq to REMEMBER for a route that
// resolved to a concrete identity, addressing a topic by its stable id + group
// (NOT by name + group). A name+non-default-group replay can't re-claim across
// groups: attachByName's step-1 searches only the default group, and its
// otherGroupHits step excludes same-group hits — so a default-group-name replay
// with a non-default group arg discards the create proposal and the claim is
// silently dropped; and a registry-missing topic synthesizes an unreplayable
// "topic-N" name. attachByTopicID(topic_id, group) validates against the group's
// chat and re-claims cleanly across groups. A DM has no topic → replay by target.
// chatID is carried as an OPTIONAL cross-check (ipc.AttachReq.ChatID): a replay
// whose group resolves to a DIFFERENT chat than the topic actually lived in is
// then refused fail-closed instead of silently binding a same-id thread in the
// wrong chat. Shared by resolvedAttachReq (§3d1, item C) and fireRecover (item B).
func rememberedIdentityReq(cwd string, chatID int64, topicID *int64, group string) ipc.AttachReq {
	req := ipc.AttachReq{Op: ipc.OpAttach, CWD: cwd}
	if topicID == nil {
		req.Target = "dm" // DM has no topic; replay by target, not by a "dm" name.
		return req
	}
	tid := *topicID
	req.TopicID = &tid
	req.Group = group
	req.ChatID = chatID
	return req
}

// resolvedAttachReq picks the request to REMEMBER for reconnect replay (§3d1).
// An explicit request is remembered verbatim. A BARE request that resolved to a
// real route is remembered as the RESOLVED identity — {TopicID, Group} for a
// topic, {Target:"dm"} for the DM — so that a replay landing on a FRESH broker
// (after a self-update/rebuild restart, where there is no sid and no
// reconnect-transfer) re-binds the same topic EXPLICITLY instead of regressing to
// a picker and silently dropping the claim. This also stops a later idempotent
// bare OK from clobbering a previously-remembered explicit request with a bare
// one: the bare OK resolves back to the same identity, so the remembered request
// keeps pointing at the live route.
func resolvedAttachReq(req ipc.AttachReq, attached ipc.AttachedMsg) ipc.AttachReq {
	if !isBareAttachReq(req) {
		return req
	}
	return rememberedIdentityReq(req.CWD, attached.ChatID, attached.TopicID, attached.Group)
}

// replayLastAttach sends the saved attach request to the (just-reconnected)
// broker. Best-effort — failures are logged to stderr and not surfaced.
// The broker will respond with AttachedMsg which brokerReader processes
// (no pending channel registered, so the response is discarded). The point
// is to re-establish the route claim, not to confirm.
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

// reconnectTerminalHandoff resolves the current terminal identity once, before
// any reconnect replay is written. A missing handoff is the ordinary
// non-resumed-session case.
func reconnectTerminalHandoff() (sessionhandoff.Entry, bool) {
	inst := instanceIDFromEnv()
	if inst == "" {
		return sessionhandoff.Entry{}, false
	}
	return resolveTerminalHandoff(inst)
}

// restoreSessionAfterReconnect restores adapter state onto a FRESH broker
// connection. Same identity: preserve the deliberate replay-before-recover
// ordering so the fresh stub reclaims the live route, then records it under the
// known id. Switched identity: never replay the previous conversation's attach;
// start the switch sequence first, which synchronously clears stale conversation
// state and opens the new gate before its asynchronous recovery round-trip.
func (a *adapter) restoreSessionAfterReconnect(ctx context.Context) {
	entry, found := reconnectTerminalHandoff()
	if found {
		if current, settled := a.currentStableIdentity(); settled && current.StableSessionID != entry.StableSessionID {
			log.Printf("recover-session: reconnect resolved identity switch %s → %s — skipping stale attach replay", current.StableSessionID, entry.StableSessionID)
			a.refireResolvedHandoffOnReconnect(ctx, entry)
			return
		}
	}

	a.replayLastAttach()
	if found {
		a.refireResolvedHandoffOnReconnect(ctx, entry)
	}
}

// refireRecoverOnReconnect re-resolves and re-registers this session's stable id
// on a fresh broker connection. Kept as the direct reconnect entry point for
// tests and callers outside recoverBroker; recoverBroker itself resolves before
// replay via restoreSessionAfterReconnect.
func (a *adapter) refireRecoverOnReconnect(ctx context.Context) {
	entry, found := reconnectTerminalHandoff()
	if !found {
		return
	}
	a.refireResolvedHandoffOnReconnect(ctx, entry)
}

// refireResolvedHandoffOnReconnect re-registers one already-resolved terminal
// handoff. A switch begins synchronously (its broker wait remains asynchronous)
// so stale conversation state is gone before reconnect recovery returns to the
// broker reader. Same-id re-registration stays in a goroutine because that
// round-trip is answered by the broker reader itself.
func (a *adapter) refireResolvedHandoffOnReconnect(ctx context.Context, entry sessionhandoff.Entry) {
	if current, settled := a.currentStableIdentity(); settled && current.StableSessionID != entry.StableSessionID {
		a.beginIdentitySwitch(ctx, current.StableSessionID, entry)
		return
	}

	go func() {
		a.recoverMu.Lock()
		current, settled := a.currentStableIdentity()
		if settled && current.StableSessionID != entry.StableSessionID {
			a.recoverMu.Unlock()
			a.beginIdentitySwitch(ctx, current.StableSessionID, entry)
			return
		}
		a.recoverFired.Store(false)
		a.recoverStarted.Store(true)
		outcome := a.fireRecoverLocked(ctx, entry)
		if outcome.attempted {
			a.markIdentitySettled()
		}
		a.recoverMu.Unlock()
		if outcome.registered {
			a.startIdentitySwitchWatch(ctx)
		}
	}()
}

// wakePendingWithErr resolves every pending entry with an error.
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

func (a *adapter) currentConn() *ipc.Conn {
	a.bmu.Lock()
	defer a.bmu.Unlock()
	return a.conn
}

// handleInbound translates an ipc.OpInbound into notifications/claude/channel.
//
// Logging policy: log delivery metadata only (chan / chat / topic / msg /
// kind / outcome). NEVER content, NEVER sender username. See DEBUGGING.md.
func (a *adapter) handleInbound(ctx context.Context, raw []byte) {
	var in ipc.InboundMsg
	if err := json.Unmarshal(raw, &in); err != nil {
		log.Printf("handleInbound unmarshal: %v", err)
		return
	}
	kind := "text"
	if in.Inbound.IsEvent() {
		kind = string(in.Inbound.Kind) // poll_result / reaction / callback
	} else if len(in.Inbound.Attachments) > 0 && in.Inbound.Attachments[0].Kind != "" {
		kind = in.Inbound.Attachments[0].Kind
	}
	topic := "-"
	if in.Inbound.TopicID != nil {
		topic = strconv.FormatInt(*in.Inbound.TopicID, 10)
	}
	frame := buildClaudeChannelFrame(&in.Inbound)

	// Push-path recovery nudge (spec Component 3 — the push half of the recovery
	// net). When the broker reports remaining backlog (in.Pending > 0), append a
	// "(N pending — call fetch_queue)" suffix so a stuck backlog item surfaces on
	// THIS successful push, not only at the next re-attach.
	if s, ok := frame["content"].(string); ok {
		frame["content"] = decoratePushContent(s, in.Pending, a.currentTopicName())
	}

	// One-shot wire dump for diagnosing "broker delivers but CLI silent" —
	// captures the exact bytes we send so we can prove the shape from outside
	// the adapter. Logged on FIRST inbound only to avoid noise.
	if a.firstInbound.CompareAndSwap(false, true) {
		if raw, err := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"method":  "notifications/claude/channel",
			"params":  frame,
		}); err == nil {
			log.Printf("notify FIRST-WIRE-DUMP: %s", string(raw))
		}
	}

	if err := a.notifyTx.Notify(ctx, "notifications/claude/channel", frame); err != nil {
		// D4 (adapter-ipc-4): the broker already counted this inbound as
		// "delivered" the moment it wrote it to our IPC socket — if the
		// adapter→CLI notify now fails, the message is otherwise lost with no
		// record anywhere. Log the FULL content (not just metadata) so it's
		// recoverable from adapter.log. This is the same "don't lose
		// undelivered content" rule the broker's failure paths follow
		// (DEBUGGING.md / worker.go fallbackSummary). A broker-side nack op
		// to bounce to the Telegram fallback is out of scope here.
		log.Printf("notify FAIL chan=%s chat=%d topic=%s msg=%d kind=%s: %v — LOST CONTENT: %s",
			in.Inbound.Channel, in.Inbound.ChatID, topic, in.Inbound.MessageID, kind, err,
			inboundContentSummary(&in.Inbound))
		return
	}
	log.Printf("notified chan=%s chat=%d topic=%s msg=%d kind=%s",
		in.Inbound.Channel, in.Inbound.ChatID, topic, in.Inbound.MessageID, kind)

	// Tell the broker we accepted this push so it Consumes the queued copy/copies.
	// Echo Covered back as Count so a MERGED push of N stored lines consumes all N
	// (not just 1, which would orphan N-1 as phantom backlog). This is broker↔
	// adapter plumbing the agent never sees (lifecycle B). On the notify-FAIL
	// branch above we returned WITHOUT acking — the message stays queued as
	// backlog, exactly as the recovery-nudge design requires.
	//
	// C1: a synthesized EVENT (poll_result / reaction / callback) is NEVER queued,
	// so it covers zero stored lines — do NOT send a delivered-ack for one. The
	// broker stamps Covered=1 via covEffective on a push (overridden to 0 for
	// events broker-side too), and handleConsume would otherwise Consume a real
	// queued backlog message the event never delivered, silently dropping it.
	if conn := a.currentConn(); conn != nil && !in.Inbound.IsEvent() {
		_ = conn.WriteJSON(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: in.Inbound.MessageID, OK: true, Count: in.Covered})
	}
}

// inboundContentSummary renders a one-line, content-bearing summary of an
// inbound for the notify-FAIL log path (D4). UNLIKE the success path (which
// logs metadata only, per DEBUGGING.md), this is a failure path where the
// message is otherwise lost with no record — so we DO include content
// (sender, text, attachment summary) so it's recoverable from adapter.log.
func inboundContentSummary(in *c3types.Inbound) string {
	var parts []string
	switch {
	case in.Sender.Username != "":
		parts = append(parts, "from=@"+in.Sender.Username)
	case in.Sender.UserID != 0:
		parts = append(parts, fmt.Sprintf("from=uid=%d", in.Sender.UserID))
	}
	if in.Text != "" {
		parts = append(parts, fmt.Sprintf("text=%q", capRunes(in.Text, 200)))
	}
	if in.ReplyTo != nil {
		parts = append(parts, fmt.Sprintf("reply_to=%d", in.ReplyTo.MessageID))
	}
	for _, att := range in.Attachments {
		parts = append(parts, fmt.Sprintf("attach=%s/%d", att.Kind, att.Size))
	}
	if in.IsEvent() {
		parts = append(parts, fmt.Sprintf("event=%s", in.Kind))
	}
	if len(parts) == 0 {
		return "(no content)"
	}
	return strings.Join(parts, " ")
}

// renderBacklogSummary renders the on-attach backlog notification text. Empty
// string when nothing is queued. Instructs the agent to call fetch_queue.
//
// The broker may report count>0 with an EMPTY items slice (it degrades to
// count-only when Peek fails) — this still renders the count line + fetch_queue
// hint so the agent knows to drain, just without per-item previews.
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

// pendingNudge returns a "(N pending for topic "X" — call fetch_queue)" suffix,
// or "" when nothing is pending. Appended to pushes + the attach summary so Claude
// can always recover even after a failed push. Naming the topic (spec §5) makes a
// stale/wrong advertisement human-distinguishable; route=="" falls back to the
// name-less form for callers without a known topic.
func pendingNudge(n int, route string) string {
	if n <= 0 {
		return ""
	}
	if route != "" {
		return fmt.Sprintf("(%d pending for topic %q — call `fetch_queue`)", n, route)
	}
	return fmt.Sprintf("(%d pending — call `fetch_queue`)", n)
}

// decoratePushContent appends the recovery nudge to a push's content string when
// the broker reports remaining backlog. Pure + unit-testable. (spec Component 3
// — the push half of the recovery net: a stuck backlog item surfaces on the next
// successful push, not only at the next re-attach.)
func decoratePushContent(content string, pending int, route string) string {
	if nudge := pendingNudge(pending, route); nudge != "" {
		return content + "\n\n" + nudge
	}
	return content
}

// renderFetchedMessages turns pulled inbound into agent-readable text, one block
// per message with full content + each attachment's file_id/mime/size/name so
// the agent can act on backlog voice/media (download_attachment / retranscribe).
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
	// file_id; the "how to open it" instruction is shown ONCE per batch instead
	// of being implied on every line.
	if c3types.InboundsHaveAttachment(msgs) {
		out = c3types.AttachmentFetchHint + "\n\n" + out
	}
	if remaining > 0 {
		out += "\n\n" + pendingNudge(remaining, route)
	}
	return out
}

// renderQueuedInbound renders one queued message for fetch_queue output in the
// trimmed form approved 2026-07-24 (task #55): the message text stands bare,
// followed by a compact metadata line (sender, message_id, reply context, and a
// kind+file_id attachment reference — enough for download_attachment/retranscribe
// on a queued voice/media message, spec Component 4/6c). The shared c3types
// renderer keeps this byte-identical across every adapter and the codex
// live-forward turn.
func renderQueuedInbound(in *c3types.Inbound) string {
	return c3types.RenderQueuedInbound(in)
}

// capRunes truncates s to at most n runes (rune-safe, so the log can't emit a
// split UTF-8 sequence), appending an ellipsis when it trims. Matches the
// broker's 200-char content-log cap (worker.go) so the failure-path logs are
// consistent and a multi-KB paste can't dump in full into adapter.log.
func capRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// buildClaudeChannelFrame converts a c3types.Inbound into the params for
// notifications/claude/channel.
//
// Shape MUST match the official Telegram plugin's frame
// (~/.claude/plugins/cache/claude-plugins-official/telegram/0.0.6/server.ts
// around line 978) — Claude Code silently drops malformed channel
// notifications. Cross-checked 2026-05-09:
//   - `content` is a STRING, not an array of MCP-style content blocks. We
//     had it as the array shape originally and Claude Code dropped every
//     inbound on the floor (broker logged "delivered", user saw nothing).
//   - `chat_id` is stringified (matching channels-reference.md docs).
//   - `message_thread_id` only present when non-nil; same for the optional
//     attachment / reply_to fields.
func buildClaudeChannelFrame(in *c3types.Inbound) map[string]any {
	// P4: a synthesized channel event (poll_result / reaction / callback) renders
	// from its neutral Event payload rather than the message text/attachment path.
	// meta values stay strings (the frame contract requires Record<string,string>).
	if in.IsEvent() {
		content, evMeta := buildEventFrame(in)
		evMeta["chat_id"] = strconv.FormatInt(in.ChatID, 10)
		evMeta["ts"] = in.Timestamp.Format("2006-01-02T15:04:05.000Z")
		if in.MessageID != 0 {
			evMeta["message_id"] = strconv.FormatInt(in.MessageID, 10)
		}
		if in.TopicID != nil {
			evMeta["message_thread_id"] = strconv.FormatInt(*in.TopicID, 10)
		}
		return map[string]any{
			"content": content,
			"meta":    evMeta,
		}
	}

	meta := map[string]any{
		// Per channels-reference.md, `meta` is typed Record<string, string>.
		// All values must be strings; non-string values may cause Claude
		// Code to silently drop the field (or the whole notification).
		// Official Telegram plugin's TypeScript happens to serialize chat_id
		// as a number due to gotgbot's Long type, but the contract is string.
		// fakechat (the reference impl) sends "web" — clearly a string.
		"chat_id": strconv.FormatInt(in.ChatID, 10),
		"ts":      in.Timestamp.Format("2006-01-02T15:04:05.000Z"),
	}
	if in.MessageID != 0 {
		meta["message_id"] = strconv.FormatInt(in.MessageID, 10)
	}
	if in.Sender.Username != "" {
		meta["user"] = in.Sender.Username
	} else if in.Sender.UserID != 0 {
		meta["user"] = strconv.FormatInt(in.Sender.UserID, 10)
	}
	if in.Sender.UserID != 0 {
		meta["user_id"] = strconv.FormatInt(in.Sender.UserID, 10)
	}
	if in.TopicID != nil {
		meta["message_thread_id"] = strconv.FormatInt(*in.TopicID, 10)
	}
	if in.ReplyTo != nil {
		meta["reply_to_message_id"] = strconv.FormatInt(in.ReplyTo.MessageID, 10)
		if in.ReplyTo.User.Username != "" {
			meta["reply_to_user"] = in.ReplyTo.User.Username
		} else if in.ReplyTo.User.UserID != 0 {
			meta["reply_to_user"] = strconv.FormatInt(in.ReplyTo.User.UserID, 10)
		}
		if in.ReplyTo.Text != "" {
			meta["reply_to_text"] = in.ReplyTo.Text
		}
	}
	if len(in.Attachments) > 0 {
		// First attachment keeps the canonical unsuffixed keys (backward
		// compatible: single-attachment output is unchanged).
		att := in.Attachments[0]
		if att.Kind != "" {
			meta["attachment_kind"] = att.Kind
		}
		if att.FileID != "" {
			meta["attachment_file_id"] = att.FileID
		}
		if att.Size > 0 {
			meta["attachment_size"] = strconv.FormatInt(att.Size, 10)
		}
		if att.MIME != "" {
			meta["attachment_mime"] = att.MIME
		}
		if att.Name != "" {
			meta["attachment_name"] = att.Name
		}
		// Multiple attachments (album / media-group, or a rich message with
		// several media blocks): surface EVERY attachment so the agent can
		// download each one. The first stays on the unsuffixed keys above;
		// extras get an _N suffix (N starts at 2). attachment_count is emitted
		// ONLY when there is more than one, so single-attachment frames are
		// byte-identical to before.
		if len(in.Attachments) > 1 {
			meta["attachment_count"] = strconv.Itoa(len(in.Attachments))
			for i := 1; i < len(in.Attachments); i++ {
				a := in.Attachments[i]
				n := strconv.Itoa(i + 1)
				if a.Kind != "" {
					meta["attachment_kind_"+n] = a.Kind
				}
				if a.FileID != "" {
					meta["attachment_file_id_"+n] = a.FileID
				}
				if a.Size > 0 {
					meta["attachment_size_"+n] = strconv.FormatInt(a.Size, 10)
				}
				if a.MIME != "" {
					meta["attachment_mime_"+n] = a.MIME
				}
				if a.Name != "" {
					meta["attachment_name_"+n] = a.Name
				}
			}
		}
	}

	text := in.Text
	if text == "" && len(in.Attachments) > 0 {
		// Channel may have left text empty for voice (STT plugin not yet
		// substituting). Fall back to a label so the agent at least sees
		// something. With several attachments (album/media-group), report the
		// count so the agent knows more than one arrived.
		if len(in.Attachments) > 1 {
			text = fmt.Sprintf("(%d attachments)", len(in.Attachments))
		} else {
			text = fmt.Sprintf("(%s message)", in.Attachments[0].Kind)
		}
	}

	return map[string]any{
		"content": text, // STRING — matches official plugin shape
		"meta":    meta,
	}
}

// buildEventFrame renders a synthesized channel event (poll_result / reaction /
// callback) into the channel-frame content string + a string-only meta map (the
// caller adds the common chat_id/ts/message_id keys). Keeps payloads simple
// strings — no structured Telegram types reach Claude Code (it would drop the
// notification). Returns a safe fallback for an unknown/empty event shape.
func buildEventFrame(in *c3types.Inbound) (string, map[string]any) {
	meta := map[string]any{"kind": string(in.Kind)}
	ev := in.Event
	switch {
	case ev != nil && ev.PollResult != nil:
		pr := ev.PollResult
		var b strings.Builder
		fmt.Fprintf(&b, "Poll results: %q — %d vote", pr.Question, pr.TotalVoters)
		if pr.TotalVoters != 1 {
			b.WriteString("s")
		}
		parts := make([]string, 0, len(pr.Options))
		for _, o := range pr.Options {
			parts = append(parts, fmt.Sprintf("%s:%d", o.Text, o.VoterCount))
		}
		if len(parts) > 0 {
			b.WriteString(" — ")
			b.WriteString(strings.Join(parts, " "))
		}
		if pr.IsClosed {
			b.WriteString(" (closed)")
		}
		meta["poll_id"] = pr.PollID
		meta["total_voters"] = strconv.Itoa(pr.TotalVoters)
		meta["is_closed"] = strconv.FormatBool(pr.IsClosed)
		return b.String(), meta

	case ev != nil && ev.Reaction != nil:
		r := ev.Reaction
		actor := senderLabel(r.Actor)
		var b strings.Builder
		fmt.Fprintf(&b, "%s reacted on message %d", actor, r.MessageID)
		if len(r.Added) > 0 {
			fmt.Fprintf(&b, " — added %s", strings.Join(r.Added, " "))
		}
		if len(r.Removed) > 0 {
			fmt.Fprintf(&b, " — removed %s", strings.Join(r.Removed, " "))
		}
		meta["message_id"] = strconv.FormatInt(r.MessageID, 10)
		if len(r.Added) > 0 {
			meta["added"] = strings.Join(r.Added, " ")
		}
		if len(r.Removed) > 0 {
			meta["removed"] = strings.Join(r.Removed, " ")
		}
		return b.String(), meta

	case ev != nil && ev.Callback != nil:
		cb := ev.Callback
		actor := senderLabel(cb.Actor)
		content := fmt.Sprintf("%s pressed a button (data=%q) on message %d", actor, cb.Data, cb.MessageID)
		meta["callback_id"] = cb.CallbackID
		meta["message_id"] = strconv.FormatInt(cb.MessageID, 10)
		meta["data"] = cb.Data
		return content, meta

	case ev != nil && ev.System != nil:
		// Broker-originated system advisory (e.g. a channel-health alert
		// broadcast to every CLI session). Surfaced loud so the user sees that
		// their phone messages won't arrive while the channel is down. NOT user
		// content — purely operational.
		sysev := ev.System
		content := "⚠️ SYSTEM: " + sysev.Message
		if sysev.Source != "" {
			meta["source"] = sysev.Source
		}
		if sysev.Level != "" {
			meta["level"] = sysev.Level
		}
		if sysev.Title != "" {
			meta["title"] = sysev.Title
		}
		return content, meta

	default:
		return fmt.Sprintf("(%s event)", in.Kind), meta
	}
}

// senderLabel renders a Sender into a short display label for event content.
func senderLabel(s c3types.Sender) string {
	if s.Username != "" {
		return "@" + s.Username
	}
	if s.UserID != 0 {
		return "user " + strconv.FormatInt(s.UserID, 10)
	}
	return "someone"
}

// dispatchToolResult routes the result to the waiting caller.
func (a *adapter) dispatchToolResult(raw []byte) {
	var msg ipc.ToolResultMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	a.pmu.Lock()
	ch, ok := a.pending[msg.ID]
	if ok {
		delete(a.pending, msg.ID)
	}
	a.pmu.Unlock()
	if ok {
		ch <- msg
	}
}

// dispatchAttached / dispatchTopicsList are routed via the same pending map
// using fixed keys ("attached", "topics_list") since at most one of each is
// in flight at a time per adapter (attach is synchronous from the agent's
// perspective).
func (a *adapter) dispatchAttached(raw []byte) {
	a.pmu.Lock()
	ch, ok := a.pending["attached"]
	if ok {
		delete(a.pending, "attached")
	}
	a.pmu.Unlock()
	if ok {
		var attached ipc.AttachedMsg
		_ = json.Unmarshal(raw, &attached)
		// A successful attach may carry the just-claimed channel's manifest.
		// Store it as the latest caps so any subsequent instructions rebuild
		// (e.g. on a broker reconnect) reflects the attached channel. The MCP
		// `instructions` field itself is delivered only at initialize, so in
		// v1's single-channel world the hello_ack caps already cover the live
		// session; this keeps the adapter's stored caps fresh for the future
		// multi-channel turn-time-refresh seam (spec §L5).
		if attached.OK && attached.Capabilities != nil {
			a.helloAck.Capabilities = attached.Capabilities
		}
		ch <- ipc.ToolResultMsg{Result: map[string]any{"_attached": attached}}
	}
}

func (a *adapter) dispatchTopicsList(raw []byte) {
	a.pmu.Lock()
	ch, ok := a.pending["topics_list"]
	if ok {
		delete(a.pending, "topics_list")
	}
	a.pmu.Unlock()
	if ok {
		var list ipc.TopicsListMsg
		_ = json.Unmarshal(raw, &list)
		ch <- ipc.ToolResultMsg{Result: map[string]any{"_topics_list": list}}
	}
}

// ─── MCP server wiring (modelcontextprotocol/go-sdk) ────────────────────────

// buildMCPServer constructs the SDK-backed MCP server, wires the
// `instructions` / `experimental.claude/channel` initialize-response
// fields, registers all 8 tool handlers, and installs a receiving
// middleware that flips the idle-startup watchdog disarm flag on the
// first incoming MCP frame.
//
// helloAck must be populated before calling — the `instructions` text
// depends on whether the broker reports no-config / no-mapping / auto-
// attached state.
func (a *adapter) buildMCPServer() *mcp.Server {
	opts := &mcp.ServerOptions{
		Instructions: a.buildInstructions(),
		Capabilities: &mcp.ServerCapabilities{
			Tools: &mcp.ToolCapabilities{ListChanged: false},
			// Experimental.claude/channel matches the fakechat reference
			// (~/.claude/plugins/.../fakechat/server.ts) — and matches
			// what the hand-rolled MCP server declared pre-migration.
			// Without it, Claude Code's channel-dispatch validator may
			// silently drop our `notifications/claude/channel` frames.
			Experimental: map[string]any{
				"claude/channel": map[string]any{},
				// Declaring claude/channel/permission tells Claude Code this server
				// relays tool-use permission prompts. Per the reference plugin's
				// contract, declaring it ASSERTS that the channel authenticates the
				// replier — C3 sender-gates a verdict to an allowlisted operator
				// (resolvePerm), and an Allow tap over Telegram AUTHORIZES the tool use.
				"claude/channel/permission": map[string]any{},
			},
		},
	}
	srv := mcp.NewServer(&mcp.Implementation{
		Name:    adapterName,
		Version: adapterVersion,
	}, opts)

	// Receiving middleware flips `dispatched` on first frame. This is the
	// integration point for the idle-startup watchdog: once any MCP frame
	// arrives, the watchdog is disarmed for the lifetime of the session.
	// ping/initialize/tools/list/tools/call all flow through here.
	srv.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			a.dispatched.Store(true)
			if method != "ping" {
				log.Printf("mcp recv: method=%s", method)
			}
			// A tools/call means an active turn — the one moment Claude Code
			// reliably renders our channel frames (unlike the resume idle gap).
			// Flush the deferred auto-attach-on-resume CLI notice here; it
			// self-clears so this only emits once.
			if method == "tools/call" {
				// Detect an in-app conversation switch before consulting the
				// identity gate: detection reopens the gate, so this same call
				// waits for the new identity rather than passing the old,
				// already-settled epoch.
				a.checkForIdentitySwitch(ctx)
				a.flushPendingRecoverNotice()
				// Belt-and-suspenders for BUG #1: if the background watch hasn't
				// yet fired the recover (the handoff landed between watch polls as
				// the first turn arrived), re-check the handoff once here and fire.
				a.recheckRecoverOnFirstActivity(ctx)
			}
			return next(ctx, method, req)
		}
	})

	// Register the 8 tools. Schemas are passed as map[string]any (SDK
	// accepts any value that JSON-marshals to a valid object schema).
	a.registerTools(srv)
	return srv
}

// buildInstructions assembles the `instructions` text returned in the MCP
// initialize response. Composed of (a) a head line that reflects current
// helloAck state and (b) the shared mode.Combined() suffix (CLI/Telegram
// mode protocol + multi-part reply protocol). mode.Combined() is the
// single source of truth shared with the Codex adapter.
func (a *adapter) buildInstructions() string {
	var head string
	switch {
	case a.helloAck.NoConfig:
		head = "C3 not yet configured. Run `/c3:setup` (or `c3-broker setup`) to provide your Telegram bot token, DM chat id, and at least one group chat id, then restart this session."
	case a.helloAck.NoMapping:
		cwd, _ := os.Getwd()
		head = fmt.Sprintf("No saved C3 topic for this session (cwd %q). Call the `attach` tool with no argument: if this session has a topic it attached to before, it silently re-attaches that one; otherwise the broker returns a picker of suggested topics for you to choose from (it never guesses). Or attach a specific topic by name: `attach(name=\"<name>\")`.", cwd)
	case a.helloAck.AutoAttached && a.helloAck.Mapping != nil:
		// Vestigial: the broker no longer auto-attaches at hello (recovery moved
		// to the post-hello RecoverSessionReq path, surfaced via a notification).
		// Kept harmless in case a future hello-time auto-attach is reintroduced.
		m := a.helloAck.Mapping
		head = fmt.Sprintf("Auto-attached to %q (%s) — resumed session. Inbound messages render here as `<channel>` blocks.", m.Name, m.Channel)
	default:
		head = "C3 connected. Use the `attach` tool to claim a Telegram topic for this session."
	}
	// When the host cannot render channel pushes (flagless launch — the
	// forked-session blackhole), channel notifications never reach the agent, so
	// this init-time warning is the reliable way to tell it. The instructions are
	// returned in the MCP initialize RESULT (a normal JSON-RPC response the CLI
	// always processes), NOT a channel push, so they render even in the broken
	// session. The human separately sees the Telegram held-notice.
	return renderDegradedNote(a.hostRenderCapable) + head + permissionContractNote + mode.Combined(a.capsOrDefault())
}

// renderDegradedNote returns the leading init-instructions warning for a session
// whose host silently drops channel push notifications, or "" when the host can
// render normally (zero change to the capable fast path).
func renderDegradedNote(canRender bool) string {
	if canRender {
		return ""
	}
	return "⚠️ Inbound delivery is DEGRADED: this session was launched without the development-channels flag, so incoming Telegram messages CANNOT be pushed into this conversation (the host silently drops them). C3 is holding them in a durable queue instead — call the `fetch_queue` tool to retrieve queued messages (they arrive as a tool result, which renders here). For normal live delivery, relaunch with `--dangerously-load-development-channels plugin:c3@c3`.\n\n"
}

// permissionContractNote is the security contract carried in the MCP instructions
// for the claude/channel/permission capability (mirrors the reference Telegram
// plugin). Declaring the capability asserts C3 authenticates the replier — C3
// only honors an Allow/Deny verdict from an allowlisted operator, and tapping
// Allow over Telegram AUTHORIZES the pending tool use.
const permissionContractNote = "\n\nPermission relay: C3 declares the claude/channel/permission capability, which asserts the channel authenticates the replier. C3 surfaces a tool-use permission prompt as an Allow/Deny keyboard, honors a verdict only from an allowlisted operator, and an Allow tap over Telegram authorizes the pending tool use."

// capsOrDefault returns the channel capability manifest the broker delivered
// on hello_ack (or a fresh attach), falling back to a sensible default when
// the broker is older than the CMG build (Capabilities==nil) or no channel was
// resolvable for this connection. The default is a zero Capabilities, which
// GuidanceFor renders as honest all-NO guidance — never a panic, never a
// fabricated capability.
func (a *adapter) capsOrDefault() c3types.Capabilities {
	if a.helloAck.Capabilities != nil {
		return *a.helloAck.Capabilities
	}
	return c3types.Capabilities{}
}

// registerTools adds all adapter tools to srv. Each tool uses the SDK's
// raw ToolHandler (json.RawMessage args) so the schemas remain pure
// map[string]any — no struct-tag reflection. This matches the
// pre-migration hand-rolled wire shape.
func (a *adapter) registerTools(srv *mcp.Server) {
	caps := a.capsOrDefault()
	tools := []struct {
		tool    *mcp.Tool
		handler mcp.ToolHandler
	}{
		{
			tool: &mcp.Tool{
				Name:        "attach",
				Description: "Attach this session to a Telegram topic. Either pass `expr` (raw user-supplied string the broker parses: empty=cwd-default, 'dm'=DM, '<int>'=topic-id, 'create <name>' or '-y <name>'=create that name, '<other>'=name) OR structured args. `target='dm'` for the user's DM. `name='X'` for a specific name. `topic_id=N` to claim a known thread id. `create=true` to confirm a creation proposal. `steal=true` to displace an existing alive holder (only after user-confirmed force_steal proposal).",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"expr":     map[string]any{"type": "string"},
						"target":   map[string]any{"type": "string"},
						"name":     map[string]any{"type": "string"},
						"topic_id": map[string]any{"type": "integer"},
						"group":    map[string]any{"type": "string"},
						"create":   map[string]any{"type": "boolean"},
						"steal":    map[string]any{"type": "boolean"},
					},
				},
			},
			handler: a.toolAttach,
		},
		{
			tool: &mcp.Tool{
				Name:        "detach",
				Description: "Release this session's current Telegram topic claim. After detach, inbound messages on that route fall through to the broker's fallback. No-op if not attached.",
				InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
			},
			handler: a.toolDetach,
		},
		{
			tool: &mcp.Tool{
				Name:        "topics",
				Description: "List known Telegram topics across all groups, with claim state.",
				InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
			},
			handler: a.toolTopics,
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
		// NOTE: `send_typing` is intentionally NOT registered as an agent tool
		// (P5 / spec R3). The typing indicator is relayed PROGRAMMATICALLY by
		// the broker's per-route RouteWorker (a ticker armed on inbound delivery
		// once the session is in Telegram mode), never by an LLM tool call. The
		// broker dispatch still HANDLES a `send_typing` op (legacy-in-flight
		// callers + the validate_topic piggyback) and the channel keeps its
		// SendTyping method — only the agent-facing tool is gone.
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
				Name:        "ask",
				Description: "Ask the human a question with choices and BLOCK until they answer. Provide `question` plus a non-empty `options` array; C3 shows the options as Telegram buttons and waits — the chosen option string is returned as this tool's result, so your turn proceeds deterministically with the answer in hand. Use this (NOT the host's AskUserQuestion, NOT a fire-and-forget `reply` with buttons) whenever you need the human to pick before you continue. Single-select in this phase; times out after ~10 minutes (returns a timeout notice you can recover from). Note: an answer here is informational input to you — it does NOT authorize actions your harness blocks in auto-accept mode's safety review (e.g. high-severity persistence or ~/.claude self-modification); those still need a terminal confirmation, so don't retry expecting a tap to clear such a block.",
				InputSchema: mcptools.AskToolSchema(),
			},
			handler: a.toolAsk,
		},
		{
			tool: &mcp.Tool{
				Name:        "stop_poll",
				Description: "Force-close a poll you sent and read its final aggregate tally (counts per option + total voters). Pass the `message_id` returned when you sent the poll. Aggregate results also arrive automatically as a <channel> event when a poll closes; stop_poll is the deterministic early read.",
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
	}
	for _, t := range tools {
		srv.AddTool(t.tool, t.handler)
	}
}

// decodeArgs unmarshals the raw tool arguments into a generic map.
// Empty/null arguments are tolerated and yield an empty map.
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

// toolAttach implements the `attach` tool: send AttachReq to the broker
// and wait for an AttachedMsg. Mirrors the pre-migration handleAttachLocal.
func (a *adapter) toolAttach(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Identity before anything that depends on it: an attach is ANSWERED from
	// this session's identity, so it must not be answered while an in-flight
	// recovery is still deciding who this session is. No-op once settled, and a
	// no-op entirely when no recovery was ever started. The first-tools/call
	// recheck covers the first call whatever it is; this covers every LATER
	// attach, which that once-only recheck does not.
	a.checkForIdentitySwitch(ctx)
	a.awaitIdentitySettled(ctx)
	args, err := decodeArgs(req.Params.Arguments)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	cwd, _ := os.Getwd()
	attachReq := ipc.AttachReq{Op: ipc.OpAttach, CWD: cwd}
	if v, ok := args["expr"].(string); ok {
		attachReq.Expr = v
	}
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
	if v, ok := args["topic_id"]; ok {
		switch x := v.(type) {
		case float64:
			id := int64(x)
			attachReq.TopicID = &id
		case int64:
			attachReq.TopicID = &x
		}
	}

	ch := make(chan ipc.ToolResultMsg, 1)
	a.pmu.Lock()
	a.pending["attached"] = ch
	a.pmu.Unlock()

	conn := a.currentConn()
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
			// §3d1: remember the RESOLVED identity for a bare attach so a
			// post-broker-restart replay re-binds the same topic explicitly.
			a.rememberAttach(resolvedAttachReq(attachReq, attached))
			// Track the resolved topic name for the fetch_queue nudges (§5).
			a.setAttachedTopic(attached.Name)
			// Side-effect surface: write OSC-0 title-bar escape to
			// stderr so the user's terminal-emulator title reflects
			// the currently-attached topic. Closes TODO #19(a).
			// Gated on tty + C3_NO_TERMINAL_TITLE — see
			// internal/termtitle for the contract. Failure paths
			// (NeedsConfirmation, Status=no_topics_configured,
			// Status=policy_rejected, Err-set) never reach this
			// branch because they leave OK=false.
			termtitle.EmitAttach(&attached)
		}
		text := ipc.FormatAttached(&attached)
		// Backlog-on-attach: surface any inbound held while no session was
		// attached (spec backlog-on-attach). renderBacklogSummary degrades
		// gracefully when the broker reports QueuedCount>0 with an EMPTY
		// QueuedSummary (count-only fallback when Peek failed): the agent still
		// gets the count + a fetch_queue hint.
		if summary := renderBacklogSummary(attached.QueuedCount, attached.QueuedSummary, attached.Name); summary != "" {
			text += "\n\n" + summary
		}
		return toolTextResult(text), nil
	}
}

// toolDetach implements the `detach` tool: send OpRelease and forget the
// last-attach replay.
func (a *adapter) toolDetach(_ context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn := a.currentConn()
	if conn == nil {
		return toolErrorResult("broker not connected"), nil
	}
	if err := conn.WriteJSON(struct {
		Op ipc.Op `json:"op"`
	}{Op: ipc.OpRelease}); err != nil {
		return toolErrorResult("broker write: " + err.Error()), nil
	}
	a.amu.Lock()
	a.lastAttach = nil
	a.attachedTopic = ""
	a.amu.Unlock()
	// Restore the terminal-emulator's default title — see
	// EmitAttach call-site comment in toolAttach for context.
	termtitle.Clear()
	return toolTextResult("detached"), nil
}

// toolTopics implements the `topics` tool.
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

// toolForward returns a ToolHandler that forwards the named tool call to
// the broker via ipc.OpToolCall and surfaces the broker's
// ipc.OpToolResult to the caller. Used for the broker-side tools
// (reply / react / edit_message / poll / download_attachment). Note:
// `send_typing` is NOT among these — the typing indicator is relayed
// programmatically by the broker (P5), not via an LLM tool call.
func (a *adapter) toolForward(name string) mcp.ToolHandler {
	return func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args, err := decodeArgs(req.Params.Arguments)
		if err != nil {
			return nil, fmt.Errorf("invalid params: %w", err)
		}
		id := strconv.FormatUint(a.nextID.Add(1), 10)
		ch := make(chan ipc.ToolResultMsg, 1)
		a.pmu.Lock()
		a.pending[id] = ch
		a.pmu.Unlock()

		tcReq := ipc.ToolCallReq{Op: ipc.OpToolCall, ID: id, Name: name, Args: args}
		conn := a.currentConn()
		if conn == nil {
			a.pmu.Lock()
			delete(a.pending, id)
			a.pmu.Unlock()
			return toolErrorResult("broker reconnecting — retry tool call in a moment"), nil
		}
		if err := conn.WriteJSON(tcReq); err != nil {
			a.pmu.Lock()
			delete(a.pending, id)
			a.pmu.Unlock()
			return toolErrorResult("broker write: " + err.Error()), nil
		}

		select {
		case <-time.After(120 * time.Second):
			a.pmu.Lock()
			delete(a.pending, id)
			a.pmu.Unlock()
			return toolErrorResult("tool timeout"), nil
		case res := <-ch:
			if res.Error != nil {
				return toolErrorResult(res.Error.Message), nil
			}
			return toolResultFromMap(res.Result), nil
		}
	}
}

// parseFetchLimit normalizes the `limit` tool argument into (limit, all). The
// agent may pass "all" (drain everything), a JSON number, OR a numeric STRING
// like "5" (some MCP clients serialize an integer field as a string) — the last
// case previously matched neither the "all" nor the float64 arm and silently fell
// back to the default 3. A parseable numeric string is honored and clamped to
// [1,50]; "all" sets All; anything unparseable (or absent) yields the spec
// default of 3. Pure + unit-tested.
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

// toolFetchQueue forwards a fetch_queue pull to the broker and renders the
// returned messages. The agent sees full content; the broker advanced the
// cursor (ack=true) before replying.
func (a *adapter) toolFetchQueue(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := decodeArgs(req.Params.Arguments)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	fq := ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: strconv.FormatUint(a.nextID.Add(1), 10), Ack: true}
	if v, ok := args["ack"].(bool); ok {
		fq.Ack = v
	}
	fq.Limit, fq.All = parseFetchLimit(args["limit"])

	ch := make(chan ipc.FetchQueueResp, 1)
	a.fqmu.Lock()
	a.fqPending[fq.ID] = ch
	a.fqmu.Unlock()
	defer func() { a.fqmu.Lock(); delete(a.fqPending, fq.ID); a.fqmu.Unlock() }()

	conn := a.currentConn()
	if conn == nil {
		return toolErrorResult("broker reconnecting — retry fetch_queue in a moment"), nil
	}
	if err := conn.WriteJSON(fq); err != nil {
		return toolErrorResult("broker write: " + err.Error()), nil
	}
	select {
	case <-ctx.Done():
		return toolErrorResult("canceled"), nil
	case <-time.After(120 * time.Second):
		return toolErrorResult("fetch_queue timeout"), nil
	case resp := <-ch:
		if resp.Err != "" {
			return toolErrorResult(resp.Err), nil
		}
		return toolTextResult(renderFetchedMessages(resp.Messages, resp.Remaining, a.currentTopicName())), nil
	}
}

// toolRetranscribe forwards a retranscribe request and returns the transcript.
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
		return toolTextResult(resp.Text), nil
	}
}

// dispatchFetchQueueResult routes an OpFetchQueueResult frame to the waiting
// toolFetchQueue caller, keyed by the request ID.
func (a *adapter) dispatchFetchQueueResult(raw []byte) {
	var resp ipc.FetchQueueResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		return
	}
	a.fqmu.Lock()
	ch, ok := a.fqPending[resp.ID]
	if ok {
		delete(a.fqPending, resp.ID)
	}
	a.fqmu.Unlock()
	if ok {
		ch <- resp
	}
}

// dispatchRetranscribeResult routes an OpRetranscribeResult frame to the waiting
// toolRetranscribe caller, keyed by the request ID.
func (a *adapter) dispatchRetranscribeResult(raw []byte) {
	var resp ipc.RetranscribeResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		return
	}
	a.rtmu.Lock()
	ch, ok := a.rtPending[resp.ID]
	if ok {
		delete(a.rtPending, resp.ID)
	}
	a.rtmu.Unlock()
	if ok {
		ch <- resp
	}
}

// recoverSessionParams tune the post-hello handoff rendezvous. The SessionStart
// hook fires AFTER the adapter spawns, and its write latency is UNBOUNDED on a
// busy machine — measured +4.3s on one resume (recovered), but +11.1s and +91s
// on others, which the old FIXED ~10s blocking window silently missed: it gave
// up and auto-attach-on-resume no-op'd (BUG #1, 2026-06-24/3e4d45a). A fixed
// window can never win an unbounded race, so we WATCH IN THE BACKGROUND for a
// generous budget and fire the instant the handoff appears. All values stay
// bounded: the watch runs in its own goroutine and never blocks hello / server
// start, and a non-c3 / fresh-non-resumed session (no handoff ever) just polls
// cheap file stats to the budget, then expires silently — zero regression.
const (
	// recoverWatchBudget bounds the background handoff watch.
	//
	// It was 5 minutes, sized against a slow hook write. That measured the wrong
	// thing. The adapter spawns when the CLI PROCESS starts; the SessionStart hook
	// writes the handoff when a SESSION is CHOSEN. Under `claude --resume` the gap
	// between those two is however long the human sits at the resume picker, which
	// is unbounded by construction — one observed run was 15m20s (adapter 09:12:06,
	// watch expired 09:17:06, handoff written 09:27:26), so the watch had given up
	// ten minutes before the thing it was watching for existed.
	//
	// 30 minutes covers a realistic picker pause at 1 stat/second, which is
	// nothing. It is not the correctness guarantee, though: the authoritative path
	// is recheckRecoverOnFirstActivity, which reads the handoff synchronously on
	// the first tools/call no matter how long that takes, and the handoff file
	// survives for 24h (sessionhandoff.PruneStale). This watch only covers the
	// narrower case where inbound arrives BEFORE the session's first tool call.
	recoverWatchBudget = 30 * time.Minute
	// recoverWatchInterval is the background re-check cadence. Generous (the file
	// stat is cheap, but there's no need to spin) — a ~1s lag on a resume that
	// hasn't had its first turn yet is invisible.
	recoverWatchInterval = 1 * time.Second
	// recoverLateThreshold marks a handoff as "late" for logging — past the old
	// fixed window that used to silently lose it. Purely diagnostic.
	recoverLateThreshold = 10 * time.Second
	recoverRespTimeout   = 10 * time.Second
)

// recoverSettleBudget bounds how long an identity-dependent call (the first
// tools/call, and every `attach`) waits for the recovery round-trip to FINISH.
// Deliberately longer than recoverRespTimeout so it is a genuine backstop rather
// than the normal exit: every ordinary failure inside fireRecover (write error,
// broker error, response timeout) settles the gate by itself well inside this
// window, so reaching this budget means the recovery goroutine is wedged. It
// replaced a 3s budget that expired BEFORE fireRecover's own 10s response
// timeout could — which guaranteed the unidentified-dispatch outcome on any slow
// broker. A var (not a const) so tests can shorten it, matching livePeekTimeout.
var recoverSettleBudget = recoverRespTimeout + 2*time.Second

// livePeekTimeout bounds the flush-time live re-peek of the held count so the
// deferred notice never delays the first tools/call beyond a moment; on timeout
// the flush falls back to the stored at-recover text. A var (not a const) so
// tests can shorten/lengthen it.
var livePeekTimeout = 2 * time.Second

const (
	terminalHandoffDepthCap     = 16
	identitySwitchWatchInterval = 5 * time.Second
)

// resolveTerminalHandoff follows the SessionStart-hook handoff chain from inst
// to the newest provably-later conversation identity. A stable session id can
// itself become the instance key written by the next in-app SessionStart hook,
// so one read is not necessarily terminal. The strict timestamp increase
// rejects stale cross-generation files. Cycle detection and a depth cap keep a
// corrupt handoff graph bounded; both fail closed at the newest accepted entry.
func resolveTerminalHandoff(inst string) (sessionhandoff.Entry, bool) {
	entry, ok := sessionhandoff.Read(inst)
	if !ok {
		return sessionhandoff.Entry{}, false
	}

	visited := map[string]struct{}{inst: {}}
	for depth := 0; ; depth++ {
		nextKey := entry.StableSessionID
		if _, seen := visited[nextKey]; seen {
			log.Printf("recover-session: handoff chain cycle at %q — stopping at stable session %q", nextKey, entry.StableSessionID)
			return entry, true
		}
		if depth >= terminalHandoffDepthCap {
			log.Printf("recover-session: handoff chain depth cap %d reached at stable session %q — stopping", terminalHandoffDepthCap, entry.StableSessionID)
			return entry, true
		}
		next, found := sessionhandoff.Read(nextKey)
		if !found || next.UnixNano <= entry.UnixNano {
			return entry, true
		}
		visited[nextKey] = struct{}{}
		entry = next
	}
}

// currentStableIdentity returns the handoff entry whose stable id the broker
// most recently registered successfully. It stays on the previous identity
// while a switch round-trip is in flight.
func (a *adapter) currentStableIdentity() (sessionhandoff.Entry, bool) {
	a.idmu.Lock()
	defer a.idmu.Unlock()
	if a.currentStableID == "" {
		return sessionhandoff.Entry{}, false
	}
	entry := a.currentHandoffEntry
	entry.StableSessionID = a.currentStableID
	return entry, true
}

// setCurrentStableIdentity completes an identity transition after the broker
// has registered the RecoverSessionReq. Callers invoke it only on an error-free
// RecoverSessionResp, whether or not a route was recovered.
func (a *adapter) setCurrentStableIdentity(entry sessionhandoff.Entry) {
	a.idmu.Lock()
	a.currentStableID = entry.StableSessionID
	a.currentHandoffEntry = entry
	a.idmu.Unlock()
}

// checkForIdentitySwitch performs the cheap per-tools-call probe: exactly one
// stat of <currentStableID>.json. Only a hit is read, required to be newer than
// the entry that established the settled identity, and chain-walked to its
// terminal handoff.
func (a *adapter) checkForIdentitySwitch(ctx context.Context) {
	current, settled := a.currentStableIdentity()
	if !settled {
		return
	}
	path, err := sessionhandoff.Path(current.StableSessionID)
	if err != nil {
		return
	}
	if _, err := os.Stat(path); err != nil {
		return
	}
	first, ok := sessionhandoff.Read(current.StableSessionID)
	if !ok || first.UnixNano <= current.UnixNano {
		return
	}
	terminal, ok := resolveTerminalHandoff(current.StableSessionID)
	if !ok || terminal.StableSessionID == current.StableSessionID {
		return
	}
	a.beginIdentitySwitch(ctx, current.StableSessionID, terminal)
}

// watchForIdentitySwitch keeps the post-recovery handoff watch alive for the
// process lifetime. The no-switch cost is one absent-file stat every tick.
func (a *adapter) watchForIdentitySwitch(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.checkForIdentitySwitch(ctx)
		}
	}
}

func (a *adapter) startIdentitySwitchWatch(ctx context.Context) {
	if a.switchWatchStarted.CompareAndSwap(false, true) {
		go a.watchForIdentitySwitch(ctx, identitySwitchWatchInterval)
	}
}

// clearConversationStateForSwitch drops every adapter-side fact belonging to
// the previous conversation and returns its topic name for the corrective
// unattached notice.
func (a *adapter) clearConversationStateForSwitch() string {
	a.amu.Lock()
	defer a.amu.Unlock()
	oldTopic := a.attachedTopic
	a.lastAttach = nil
	a.attachedTopic = ""
	return oldTopic
}

func renderIdentitySwitchUnattachedNotice(oldTopic string) string {
	if oldTopic == "" {
		return "session switched conversations — the previous conversation's topic was released; this conversation is not attached. Use attach to claim a topic."
	}
	return fmt.Sprintf(
		"session switched conversations — released topic %q (it belonged to the previous conversation); this conversation is not attached. Use attach to claim a topic.",
		oldTopic,
	)
}

// beginIdentitySwitch waits for the old identity epoch, then synchronously
// establishes the new open epoch before returning. The broker round-trip runs
// in a goroutine so the caller can immediately wait on that fresh gate. The
// ready handshake is load-bearing: a tools/call must never observe the old,
// already-closed gate after switch detection returns.
func (a *adapter) beginIdentitySwitch(ctx context.Context, expectedCurrent string, terminal sessionhandoff.Entry) {
	a.awaitIdentitySettled(ctx)
	fireCtx := a.runCtx
	if fireCtx == nil {
		fireCtx = ctx
	}
	ready := make(chan struct{})
	go func() {
		a.recoverMu.Lock()
		defer a.recoverMu.Unlock()

		current, settled := a.currentStableIdentity()
		if !settled || current.StableSessionID != expectedCurrent || current.StableSessionID == terminal.StableSessionID {
			close(ready)
			return
		}

		a.reopenIdentity()
		oldTopic := a.clearConversationStateForSwitch()
		a.dropPendingRecoverNoticeOnEpochChange()
		a.recoverFired.Store(false)
		a.recoverStarted.Store(true)
		close(ready)

		outcome := a.fireRecoverLocked(fireCtx, terminal)
		if !outcome.recovered {
			a.setPendingRecoverNotice(renderIdentitySwitchUnattachedNotice(oldTopic))
		}
		if outcome.attempted {
			a.markIdentitySettled()
		}
	}()
	<-ready
}

// recoverSessionOnResume runs in a goroutine after hello. It WATCHES (in the
// background, for recoverWatchBudget) for this adapter's SessionStart-hook
// handoff (keyed on the ephemeral instance id), and the instant it appears asks
// the broker to re-attach the resumed session to its last topic (keyed on the
// STABLE session id from the handoff).
//
// Fail-closed throughout: no instance id (non-Claude-Code host), no handoff
// (non-c3 / fresh non-resumed session), broker write/timeout — all return
// silently → today's no-recovery behavior, zero regression. The watch lives
// entirely in this goroutine; it never delays hello or the MCP server.
func (a *adapter) recoverSessionOnResume(ctx context.Context) {
	inst := instanceIDFromEnv()
	if inst == "" {
		return
	}
	entry, found := a.watchForHandoff(ctx, inst, recoverWatchInterval, recoverWatchBudget)
	if !found {
		return // non-hook / non-c3 / non-resumed session — nothing to recover.
	}
	// Mark the identity question OPEN before firing, so an attach arriving in the
	// same instant waits for the answer instead of racing it.
	a.recoverStarted.Store(true)
	outcome := a.fireRecover(ctx, entry)
	if outcome.registered {
		a.startIdentitySwitchWatch(ctx)
	}
}

// watchForHandoff polls for the handoff entry for inst every interval, up to
// budget, returning it the instant it appears. It returns (zero, false) when the
// budget expires or ctx is cancelled — the non-resume case, which simply costs a
// few cheap file stats. Reads the handoff once up front (it may already exist
// for a fast hook) before the first sleep. Split out so the long-background-watch
// behavior is unit-testable without a broker.
func (a *adapter) watchForHandoff(ctx context.Context, inst string, interval, budget time.Duration) (sessionhandoff.Entry, bool) {
	start := time.Now()
	deadline := start.Add(budget)
	for {
		if e, ok := resolveTerminalHandoff(inst); ok {
			if elapsed := time.Since(start); elapsed > recoverLateThreshold {
				log.Printf("recover-session: handoff appeared late (+%s after hello, past the old %s window) — recovering now",
					elapsed.Round(time.Millisecond), recoverLateThreshold)
			}
			return e, true
		}
		if !time.Now().Before(deadline) {
			log.Printf("recover-session: no handoff within %s — not a resumed session (or the SessionStart hook never ran)", budget)
			return sessionhandoff.Entry{}, false
		}
		select {
		case <-ctx.Done():
			return sessionhandoff.Entry{}, false
		case <-time.After(interval):
		}
	}
}

// fireRecover serializes and sends a RecoverSessionReq EXACTLY ONCE per
// connection/identity epoch (guarded by recoverFired) and handles the response.
// Both the background watch
// and the first-tools/call belt-and-suspenders recheck call it; the
// CompareAndSwap ensures the broker never sees a duplicate recover for the same
// session. On a Recovered response it remembers the attach (for reconnect replay)
// and defers a CLI notice surfacing the held backlog. A late recover cannot
// steal: the broker takes the record-only branch if the stub has since attached,
// and skips a route held by another live session.
func (a *adapter) fireRecover(ctx context.Context, entry sessionhandoff.Entry) recoverOutcome {
	a.recoverMu.Lock()
	defer a.recoverMu.Unlock()
	outcome := a.fireRecoverLocked(ctx, entry)
	if outcome.attempted {
		a.markIdentitySettled()
	}
	return outcome
}

// fireRecoverLocked owns rsPending for one complete broker round-trip. Caller
// holds recoverMu and settles the current epoch after any switch-specific state
// (notably the corrective unattached notice) has been finalized.
func (a *adapter) fireRecoverLocked(ctx context.Context, entry sessionhandoff.Entry) recoverOutcome {
	var outcome recoverOutcome
	if !a.recoverFired.CompareAndSwap(false, true) {
		return outcome // already fired (watch + first-activity recheck race) — idempotent.
	}
	outcome.attempted = true
	// The CAS winner OWNS the identity question, so it answers it on EVERY exit
	// path below — recovered, registered, refused, write-failed, timed out. A
	// session that could not be identified is a settled answer ("nobody"), not a
	// session that blocks attaches until a budget expires: a hung attach is a
	// worse failure than an unidentified one. (The CAS loser deliberately does
	// NOT settle — the winner is still working, and letting the loser answer for
	// it is the exact entry-read-as-completion mistake this guards against.)

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
		// later attempt short-circuits on a recovery that never happened, and the
		// broker never learns this session's stable id, so every subsequent
		// attach is recorded against nothing. Release the guard so a later
		// attempt can actually fire.
		//
		// Releasing does not re-open the identity gate (deliberately — see
		// identitySettled): with no broker connection, toolAttach itself answers
		// "broker reconnecting — retry attach in a moment", so no attach can be
		// answered in this window anyway, and the reconnect path re-fires.
		a.recoverFired.Store(false)
		log.Printf("recover-session: no broker connection — nothing sent; releasing the once-per-connection guard so a later attempt can register this session")
		return outcome
	}
	if err := conn.WriteJSON(ipc.RecoverSessionReq{
		Op: ipc.OpRecoverSession, StableSessionID: entry.StableSessionID, CWD: entry.CWD,
	}); err != nil {
		// Same as conn == nil: the request never reached the broker, so the guard
		// must not stay set on a recovery that did not happen.
		a.recoverFired.Store(false)
		log.Printf("recover-session: write failed: %v (nothing sent — guard released for a later attempt)", err)
		return outcome
	}

	select {
	case <-ctx.Done():
		return outcome
	case <-time.After(recoverRespTimeout):
		log.Printf("recover-session: no response within %v", recoverRespTimeout)
		return outcome
	case resp := <-respCh:
		if resp.Err != "" {
			log.Printf("recover-session: broker err: %s", resp.Err)
			return outcome
		}
		a.setCurrentStableIdentity(entry)
		outcome.registered = true
		if !resp.Recovered {
			return outcome // already attached, or nothing recoverable — stay quiet.
		}
		outcome.recovered = true
		// Remember the recovered attach so a later broker reconnect replays it.
		// Address the topic by its stable id + group, NOT by {Name, Group}: for a
		// DM recover resp.Name=="dm", and a replayed attach(name="dm") can silently
		// bind a TOPIC literally named "dm" (the disambiguate_dm flow exists
		// precisely because such topics occur). rememberedIdentityReq maps
		// resp.TopicID==nil → {Target:"dm"} and a topic → {TopicID, Group}, which a
		// fresh broker re-claims via attachByTopicID (item B, shared with item C).
		a.rememberAttach(rememberedIdentityReq(entry.CWD, resp.ChatID, resp.TopicID, resp.Group))
		// Track the recovered topic name for the fetch_queue nudges (§5).
		a.setAttachedTopic(resp.Name)
		log.Printf("recover-session: auto-attached to %q (queued=%d)", resp.Name, resp.QueuedCount)
		if text := renderRecoverNotice(resp); text != "" {
			// Defer rather than emit now: this may run in the resume idle gap,
			// where Claude Code drops channel frames. The middleware flushes it
			// on the first tools/call. (The broker's Telegram recover-welcome is
			// the guaranteed signal; this is the best-effort CLI echo.)
			a.setPendingRecoverNotice(text)
		}
	}
	return outcome
}

// recheckRecoverOnFirstActivity is the belt-and-suspenders half of BUG #1: on the
// FIRST tools/call, if the background watch hasn't yet fired the recover (the
// handoff landed between watch polls just as the first turn arrived), re-check
// the handoff once and fire. Runs at most once (recoverRechecked) so a non-resume
// session doesn't re-stat the file on every later call, and the round-trip runs
// in a goroutine so it never blocks the tools/call. fireRecover's CompareAndSwap
// still guarantees a single RecoverSessionReq even if the watch fires concurrently.
func (a *adapter) recheckRecoverOnFirstActivity(ctx context.Context) {
	if !a.recoverRechecked.CompareAndSwap(false, true) {
		return // only on the very first tools/call.
	}
	if !a.recoverStarted.Load() {
		// No recovery under way — try to start one from the handoff.
		inst := instanceIDFromEnv()
		if inst == "" {
			return
		}
		e, ok := resolveTerminalHandoff(inst)
		if !ok {
			return
		}
		log.Printf("recover-session: handoff found on first tools/call (background watch had not yet fired) — recovering now, BEFORE this call reaches the broker")
		fireCtx := a.runCtx
		if fireCtx == nil {
			fireCtx = context.Background()
		}
		// The round trip runs on the PROCESS context (it must outlive this call),
		// while the wait below runs on the CALL's context. recoverStarted is set
		// before the goroutine so the wait cannot slip past a goroutine that has
		// not been scheduled yet.
		a.recoverStarted.Store(true)
		go func() {
			if outcome := a.fireRecover(fireCtx, e); outcome.registered {
				a.startIdentitySwitchWatch(fireCtx)
			}
		}()
	}
	// BLOCK HERE, deliberately. This used to return straight into next() as soon
	// as the recovery had been ENTERED, which raced the RecoverSessionReq against
	// the very tools/call that triggered it. When that call was `attach`, the
	// broker handled attach FIRST — with no stable session id yet, because it
	// learns the id only from RecoverSessionReq — so attachBare fell to its picker
	// branch. The recover then landed a moment later, the broker claimed the topic
	// and posted its "resumed" notice to the channel. Same instant, opposite
	// answers: the channel said attached, the CLI showed a topic picker.
	//
	// Ordering is the fix, not a longer timeout. The broker must know who this
	// session IS before it answers a question whose answer depends on that — and
	// "knows who it is" means the recovery FINISHED, not that it started.
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

// identityEpoch returns the epoch used to stamp deferred recovery notices.
func (a *adapter) identityEpoch() uint64 {
	a.idmu.Lock()
	defer a.idmu.Unlock()
	return a.idEpoch
}

// reopenIdentity starts a new unsettled epoch for an in-app conversation
// switch. Same-id reconnect registrations deliberately do not call it.
func (a *adapter) reopenIdentity() uint64 {
	a.idmu.Lock()
	defer a.idmu.Unlock()
	a.idEpoch++
	a.identitySettled = make(chan struct{})
	return a.idEpoch
}

// markIdentitySettled answers the CURRENT identity epoch. Idempotent within the
// epoch; a later switch swaps in a fresh channel before its recovery starts.
func (a *adapter) markIdentitySettled() {
	a.idmu.Lock()
	defer a.idmu.Unlock()
	if a.identitySettled == nil {
		a.identitySettled = make(chan struct{})
	}
	select {
	case <-a.identitySettled:
	default:
		close(a.identitySettled)
	}
}

// awaitIdentitySettled blocks until this session's identity question has been
// ANSWERED. Returns immediately when no recovery was ever started — nothing will
// ever settle it, and blocking on that would hang every attach in a non-resumed
// session.
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
		// topic; it falls to the picker / explicit-name path. That is a degraded
		// answer, not an unsafe one: a recover that lands later takes the broker's
		// record-only branch (internal/broker/handler.go handleRecoverSession,
		// the already-attached arm), so it cannot steal the route the explicit
		// attach just took.
		log.Printf("recover-session: identity still unsettled after %v — dispatching this call as an UNIDENTIFIED session: an `attach` answered now will not silently re-claim this session's own topic (expect the picker) while the recover completes behind it", recoverSettleBudget)
		// Giving up IS the answer, so record it. Without this the budget is paid
		// again by every later identity-dependent call — the first tools/call
		// waits it out, then the `attach` inside that same call waits it out
		// again. One wedged recovery must cost one budget, not one per caller.
		a.markIdentitySettled()
	}
}

// dispatchRecoverSessionResult resolves the one-shot recover-session pending
// channel. Safe to call when no recover is in flight (the channel is nil).
func (a *adapter) dispatchRecoverSessionResult(raw []byte) {
	var resp ipc.RecoverSessionResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		return
	}
	a.rsmu.Lock()
	ch := a.rsPending
	a.rsPending = nil
	a.rsmu.Unlock()
	if ch != nil {
		select {
		case ch <- resp:
		default:
		}
	}
}

// askAnswerTimeout bounds how long the `ask` tool blocks waiting for the human
// to tap a button after the question was successfully delivered. 600s (10 min)
// per the design; a var (not const) so tests can shorten it. On expiry the tool
// returns a recoverable timeout notice (not an error) so the agent can proceed.
var askAnswerTimeout = 600 * time.Second

// askRegisterTimeout bounds the wait for the broker's SYNCHRONOUS registration
// ack (OpAskRegistered). This only covers the local send round-trip, so it is
// short; a var so tests can shorten it.
var askRegisterTimeout = 30 * time.Second

// toolAsk implements the blocking, correlated `ask` tool. It generates an askID,
// registers two local pending channels (registration ack + answer), sends an
// AskRegisterReq, waits for the broker's synchronous OpAskRegistered (bailing fast
// on !OK so a send failure / ask-before-attach returns immediately), then blocks
// until the human's OpAskResult arrives, the answer times out, or the call is
// canceled. Mirrors toolFetchQueue's fire-then-push shape with a second wait.
func (a *adapter) toolAsk(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := decodeArgs(req.Params.Arguments)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	question, _ := args["question"].(string)
	if strings.TrimSpace(question) == "" {
		return toolErrorResult("ask: question is required"), nil
	}
	// free_text (no-options questions) and allow_other (an option that opens a
	// free-text answer) intercept the durable-queue text path and need a product
	// decision; they are not yet supported. Reject up front (before any broker
	// round-trip) so the agent learns to use single/multi-select instead.
	if argBoolField(args, "free_text") || argBoolField(args, "allow_other") {
		return toolErrorResult("ask: free_text / allow_other are not yet supported (single/multi-select only)"), nil
	}
	options := argStringList(args["options"])
	if len(options) == 0 {
		return toolErrorResult("ask: at least one option is required (single/multi-select; free-text questions are not yet available)"), nil
	}

	regCh := make(chan ipc.AskRegisteredMsg, 1)
	resCh := make(chan ipc.AskResultMsg, 1)
	a.askmu.Lock()
	// Pick an askID not already in-flight (FIX-5). crypto/rand collisions are
	// astronomically unlikely, but reusing a live id would silently clobber
	// another pending ask's channels — cheap insurance to regenerate on a clash.
	askID := newAskID()
	for i := 0; i < 8; i++ {
		_, regBusy := a.askRegPending[askID]
		_, resBusy := a.askPending[askID]
		if !regBusy && !resBusy {
			break
		}
		askID = newAskID()
	}
	a.askRegPending[askID] = regCh
	a.askPending[askID] = resCh
	a.askmu.Unlock()
	defer func() {
		a.askmu.Lock()
		delete(a.askRegPending, askID)
		delete(a.askPending, askID)
		a.askmu.Unlock()
	}()

	askReq := ipc.AskRegisterReq{
		Op:       ipc.OpAskRegister,
		AskID:    askID,
		Question: question,
		Options:  options,
		// multi / allow_skip are functional (Phase 2); allow_other / free_text are
		// rejected above and never reach here.
		Multi:     argBoolField(args, "multi"),
		AllowSkip: argBoolField(args, "allow_skip"),
	}

	conn := a.currentConn()
	if conn == nil {
		return toolErrorResult("broker reconnecting — retry ask in a moment"), nil
	}
	if err := conn.WriteJSON(askReq); err != nil {
		return toolErrorResult("broker write: " + err.Error()), nil
	}

	// Wait for the broker's synchronous registration ack so a fast failure
	// (ask-before-attach, oversized keyboard, channel/send error) returns the tool
	// immediately instead of blocking the full answer timeout.
	select {
	case <-ctx.Done():
		return toolErrorResult("canceled"), nil
	case <-time.After(askRegisterTimeout):
		return toolErrorResult("ask: timed out waiting for the broker to register the question"), nil
	case reg := <-regCh:
		if !reg.OK {
			return toolErrorResult("ask: " + reg.Err), nil
		}
	}

	// Registered + question delivered to Telegram; block until the human taps a
	// button or the ask times out.
	select {
	case <-ctx.Done():
		return toolErrorResult("canceled"), nil
	case <-time.After(askAnswerTimeout):
		return toolTextResult(renderAskAnswer(ipc.AskAnswer{TimedOut: true})), nil
	case res := <-resCh:
		if res.Err != "" {
			return toolErrorResult("ask: " + res.Err), nil
		}
		return toolTextResult(renderAskAnswer(res.Answer)), nil
	}
}

// dispatchAskRegistered routes an OpAskRegistered ack to the waiting toolAsk
// caller, keyed by askID.
func (a *adapter) dispatchAskRegistered(raw []byte) {
	var msg ipc.AskRegisteredMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	a.askmu.Lock()
	ch, ok := a.askRegPending[msg.AskID]
	a.askmu.Unlock()
	if ok {
		select {
		case ch <- msg:
		default:
		}
	}
}

// renderRecoverNotice builds the one-shot auto-attach notice. With held backlog
// it ACTIVELY SURFACES the messages — the oldest few (sender + kind + preview)
// plus the total — and tells the agent to process them, then drain any remainder
// via fetch_queue (BUG #2: turn the passive "N held, call fetch_queue" count into
// an actual surfacing so a bare resume with no user turn still gets worked). With
// no backlog it's a minimal "auto-attached" line. Returns "" only when nothing
// useful can be said (no name).
func renderRecoverNotice(resp ipc.RecoverSessionResp) string {
	name := resp.Name
	if name == "" {
		return ""
	}
	if resp.QueuedCount > 0 {
		noun := "message"
		if resp.QueuedCount > 1 {
			noun = "messages"
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "📨 Auto-attached to %q (resumed session). ~%d %s held at resume — process them now, then call `fetch_queue` (limit:\"all\") to drain any remainder (its Remaining is the live count):",
			name, resp.QueuedCount, noun)
		for _, it := range resp.QueuedSummary {
			preview := it.Preview
			if preview == "" {
				preview = "(" + it.Kind + ")"
			}
			fmt.Fprintf(&sb, "\n  • [%d] %s %s: %s", it.MessageID, it.Sender, it.Kind, preview)
		}
		if resp.QueuedCount > len(resp.QueuedSummary) {
			fmt.Fprintf(&sb, "\n  …and %d more", resp.QueuedCount-len(resp.QueuedSummary))
		}
		return sb.String()
	}
	return fmt.Sprintf("📨 Auto-attached to %q (resumed session). Inbound messages render here as `<channel>` blocks.", name)
}

// emitRecoverNotice surfaces the auto-attach notice to the Claude session via
// the same broker-originated system-event path the broker-down advisory uses
// (the proven-visible channel frame). No-op when the notify transport isn't up.
func (a *adapter) emitRecoverNotice(text string) {
	if a.notifyTx == nil {
		return
	}
	in := &c3types.Inbound{
		Channel: "c3",
		Kind:    c3types.InboundSystem,
		Event: &c3types.InboundEvent{System: &c3types.SystemEvent{
			Source: "c3", Level: "info", Title: "C3 auto-attached", Message: text,
		}},
	}
	frame := buildClaudeChannelFrame(in)
	if err := a.notifyTx.Notify(context.Background(), "notifications/claude/channel", frame); err != nil {
		log.Printf("recover-session: notify failed: %v", err)
	}
}

// setPendingRecoverNotice stores the auto-attach-on-resume CLI notice (the
// fallback text) to be flushed on the first active turn (see the
// adapter.pendingRecoverNotice doc).
func (a *adapter) setPendingRecoverNotice(text string) {
	epoch := a.identityEpoch()
	a.pnmu.Lock()
	a.pendingRecoverNotice = recoverNotice{text: text, epoch: epoch}
	a.pnmu.Unlock()
}

// takePendingRecoverNotice atomically clears and returns the pending notice when
// one is set. Returns ("", false) when none is pending. Once-only: a second take
// after a successful first returns ("", false), so the notice never re-emits.
// A notice persists however late within one identity epoch, but is dropped when
// an in-app switch has reopened the gate: text about the old conversation must
// never render into the new one.
// Split out from flush so the once-only logic is unit-testable without a live
// notify transport.
func (a *adapter) takePendingRecoverNotice() (string, bool) {
	a.pnmu.Lock()
	notice := a.pendingRecoverNotice
	a.pendingRecoverNotice = recoverNotice{}
	a.pnmu.Unlock()
	if notice.text == "" {
		return "", false
	}
	currentEpoch := a.identityEpoch()
	if notice.epoch != currentEpoch {
		log.Printf("recover-session: dropped stale pending recover notice from identity epoch %d (current epoch %d)", notice.epoch, currentEpoch)
		return "", false
	}
	return notice.text, true
}

// dropPendingRecoverNoticeOnEpochChange clears the previous conversation's
// deferred banner immediately after reopenIdentity. takePendingRecoverNotice
// performs and logs the epoch rejection.
func (a *adapter) dropPendingRecoverNoticeOnEpochChange() {
	_, _ = a.takePendingRecoverNotice()
}

// peekPendingCount does a NON-DESTRUCTIVE (Ack=false) fetch_queue round-trip to
// learn the live held count on the recovered route, reusing the fqPending
// dispatch plumbing (same fire-then-push pattern as toolFetchQueue). Returns
// (n, true) on success — n = returned + Remaining, robust for any limit — and
// (0, false) on a reconnecting/absent conn, write error, or timeout. Ack=false
// consumes nothing, so this preserves the pull-not-push invariant.
func (a *adapter) peekPendingCount(timeout time.Duration) (int, bool) {
	fq := ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: strconv.FormatUint(a.nextID.Add(1), 10), Ack: false}
	ch := make(chan ipc.FetchQueueResp, 1)
	a.fqmu.Lock()
	a.fqPending[fq.ID] = ch
	a.fqmu.Unlock()
	defer func() { a.fqmu.Lock(); delete(a.fqPending, fq.ID); a.fqmu.Unlock() }()

	conn := a.currentConn()
	if conn == nil {
		return 0, false
	}
	if err := conn.WriteJSON(fq); err != nil {
		return 0, false
	}
	select {
	case <-time.After(timeout):
		return 0, false
	case resp := <-ch:
		if resp.Err != "" {
			return 0, false
		}
		return len(resp.Messages) + resp.Remaining, true
	}
}

// renderResumeReattachFrame builds the live-accurate first-turn notice from a
// fresh count. N>0 is IMPERATIVE to the agent (surface now, call fetch_queue
// before continuing); N==0 is the bare re-attach note.
func renderResumeReattachFrame(name string, n int) string {
	if n <= 0 {
		return fmt.Sprintf("📨 Re-attached to %q (resumed session). Inbound messages render here as `<channel>` blocks.", name)
	}
	noun := "message"
	if n > 1 {
		noun = "messages"
	}
	return fmt.Sprintf("📨 Re-attached to %q (resumed session) — %d held %s. Surface them to the user now — call `fetch_queue` — before continuing with the user's request.", name, n, noun)
}

// flushPendingRecoverNotice emits the deferred auto-attach notice once, on the
// first tools/call (an active turn — the one moment Claude Code renders channel
// frames). It LIVE re-peeks the held count so the surfaced number is accurate at
// flush time, not frozen at recover time (which can be minutes stale). On a
// conn/timeout/error it falls back to the stored at-recover text so the flush
// never blocks the tools/call beyond livePeekTimeout. Called from the receiving
// middleware; self-clears so it only emits once.
func (a *adapter) flushPendingRecoverNotice() {
	stored, ok := a.takePendingRecoverNotice()
	if !ok {
		return
	}
	name := a.currentTopicName()
	if name != "" {
		if n, ok := a.peekPendingCount(livePeekTimeout); ok {
			a.emitRecoverNotice(renderResumeReattachFrame(name, n))
			return
		}
	}
	a.emitRecoverNotice(stored)
}

// handlePermissionRequest is the receive interceptor's callback (notify_transport
// SetPermissionHandler): it relays a diverted Claude Code permission_request to
// the broker as an OpPermissionRequest so the broker can surface an Allow/Deny
// keyboard on the claimed route. Fire-and-forget — there is no caller to unblock;
// a broker write failure (or a reconnecting broker) is logged and CC simply keeps
// waiting in its TUI. NEVER logs the preview body (it can carry tool input).
func (a *adapter) handlePermissionRequest(requestID, toolName, preview string) {
	if requestID == "" {
		return
	}
	conn := a.currentConn()
	if conn == nil {
		log.Printf("perm: broker reconnecting — dropping permission_request id=%s tool=%s (CC keeps waiting)", requestID, toolName)
		return
	}
	if err := conn.WriteJSON(ipc.PermissionReq{
		Op:        ipc.OpPermissionRequest,
		RequestID: requestID,
		ToolName:  toolName,
		Preview:   preview,
	}); err != nil {
		log.Printf("perm: broker write failed id=%s tool=%s: %v", requestID, toolName, err)
	}
}

// dispatchPermissionVerdict routes a broker OpPermissionVerdict into Claude Code:
// it emits a notifications/claude/channel/permission frame {request_id, behavior}
// (fire-and-forget — no pending-tool map, the harness consumes it directly). An
// unknown/late verdict is harmless: CC silently drops it.
func (a *adapter) dispatchPermissionVerdict(raw []byte) {
	var msg ipc.PermissionVerdictMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if msg.RequestID == "" || a.notifyTx == nil {
		return
	}
	if err := a.notifyTx.Notify(context.Background(), "notifications/claude/channel/permission", map[string]any{
		"request_id": msg.RequestID,
		"behavior":   msg.Behavior,
	}); err != nil {
		log.Printf("perm: verdict notify failed id=%s behavior=%s: %v", msg.RequestID, msg.Behavior, err)
	}
}

// dispatchAskResult routes an OpAskResult push to the waiting toolAsk caller,
// keyed by askID. A result for an unknown/expired askID (the tool already
// returned/timed out) is dropped.
func (a *adapter) dispatchAskResult(raw []byte) {
	var msg ipc.AskResultMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	a.askmu.Lock()
	ch, ok := a.askPending[msg.AskID]
	a.askmu.Unlock()
	if ok {
		select {
		case ch <- msg:
		default:
		}
	}
}

// renderAskAnswer renders an AskAnswer into the tool's text result so the agent
// gets a clean value to branch on:
//   - single-select (exactly one) → the chosen option verbatim.
//   - multi-select (>1)           → a bulleted list, one option per line, so an
//     option that itself contains a comma stays unambiguously separable (FIX-2:
//     a plain ", " join could not be re-split by the agent).
//   - multi-select (none toggled) → "Selected: (none)".
//   - skip                        → a "(skipped)" notice.
//   - timeout                     → a recoverable notice (re-ask or proceed).
func renderAskAnswer(ans ipc.AskAnswer) string {
	switch {
	case ans.TimedOut:
		return "No answer — the question timed out (no one tapped a button within the time limit). You can ask again or proceed without it."
	case ans.Skipped:
		return "(skipped)"
	case len(ans.Selected) == 1:
		// Single-select: the bare option string (unchanged contract).
		return ans.Selected[0]
	case len(ans.Selected) > 1:
		// Multi-select: bulleted, one per line — unambiguous even if an option
		// contains a comma.
		return "Selected:\n• " + strings.Join(ans.Selected, "\n• ")
	case ans.Text != "":
		return ans.Text
	default:
		// Multi-select Done with nothing toggled (empty Selected, no text).
		return "Selected: (none)"
	}
}

// newAskID returns an 8-char lowercase base32 ask identifier (40 bits of
// entropy). The base32 alphabet contains no colon, so it never collides with the
// "ask:<id>:<idx>" callback_data separator.
func newAskID() string {
	var b [5]byte // 5 bytes = 40 bits → exactly 8 base32 chars (no padding)
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is astronomically unlikely; fall back to a
		// time-derived id so the tool still functions (uniqueness within a route's
		// lifetime is all that's needed).
		return strconv.FormatInt(time.Now().UnixNano(), 32)
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:]))
}

// argStringList parses a JSON array-of-strings arg (the []any form
// json.Unmarshal produces) into []string, skipping empty/non-string elements.
func argStringList(v any) []string {
	list, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, e := range list {
		if s, ok := e.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

// argBoolField reads an optional bool arg (the Phase-2-ready flags), defaulting
// to false when absent or not a bool.
func argBoolField(args map[string]any, key string) bool {
	b, _ := args[key].(bool)
	return b
}

// toolTextResult wraps a string into the standard CallToolResult shape
// (one TextContent block). IsError stays false.
func toolTextResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: text}},
	}
}

// toolErrorResult wraps an error message as an in-band tool error (IsError
// true). Per the SDK's guidance, errors that originate inside the tool
// should be reported as IsError=true in the result, not as protocol-level
// errors, so the LLM can see and self-correct.
func toolErrorResult(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{&mcp.TextContent{Text: msg}},
	}
}

// toolResultFromMap converts a broker-returned result map into a
// CallToolResult. The broker may return either the standard MCP-style
// `{"content":[{"type":"text","text":…}]}` shape, in which case we
// translate it back into SDK content blocks; or a plain object, in which
// case we emit a single JSON-encoded text block.
func toolResultFromMap(result map[string]any) *mcp.CallToolResult {
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
