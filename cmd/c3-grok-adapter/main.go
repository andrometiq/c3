// c3-grok-adapter is the Grok Build MCP server that bridges Grok's MCP stdio
// protocol to the C3 broker over $XDG_RUNTIME_DIR/c3.sock.
//
// Live Telegram inject REQUIRES Grok leader mode ([cli] use_leader = true).
// On broker inbound the adapter registers as a leader ACP client and issues
// session/prompt against the TUI's session id (see docs/GROK-INJECT.md).
//
// Outbound tools (attach, reply, …) are broker-forwarded like the Claude/Grok
// adapters. Grok parity on the tool surface; detach included.
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
	// adapterName MUST match the MCP server key in the Grok plugin
	// (.mcp.json / config.toml mcp_servers.c3).
	adapterName    = "c3"
	adapterVersion = "0.1.0"

	idleStartupTimeout = 60 * time.Second // mirror cmd/c3-claude-adapter behavior
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "c3-grok-adapter: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Persistent adapter log at $XDG_STATE_HOME/c3/adapter.log. The Grok MCP
	// host owns (and may discard) our stderr, and several failure-path
	// contracts below promise adapter.log recoverability (the broker-down
	// advisory body, the forwardBlocked nudge-fail line) — this tee is what
	// makes those hold. Same content policy as the broker (DEBUGGING.md):
	// metadata only on success, content on failure. Mirrors the Claude
	// adapter's setupAdapterLog.
	if path, err := setupAdapterLog(); err == nil {
		fmt.Fprintf(os.Stderr, "c3-grok-adapter: log file %s\n", path)
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
	go a.trySessionRecover(ctx)

	err := srv.Run(ctx, a.transport)
	// Process exit is NOT a detach — deliberately NO OpRelease here (or in the
	// signal handler). The broker treats OpRelease as an explicit user detach:
	// handleRelease releases the claim AND tombstones the session attachment so
	// a later resume of the same session stays unattached (its doc-comment says
	// the process-exit path must NOT call it). Sending it on every exit made the
	// resume-recovery feature self-defeating — each quit-without-detach wiped
	// the recorded attachment — and, via the single-live-session sid fallback in
	// resolveGrokSessionID, an abandoned adapter spawn's idle-watchdog exit could
	// tombstone a LIVE session's mapping. Claude/codex parity: OpRelease is sent
	// only by the explicit `detach` tool; the broker's conn-drop + PID-liveness
	// reaping releases dead holders on its own.
	if a.leader != nil {
		a.leader.Close()
	}
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

// installSignalHandlers cancels ctx on SIGTERM/SIGINT/SIGHUP so run() logs the
// exit reason (Claude-adapter parity). It deliberately does NOT send OpRelease:
// a signal is a process exit, not a user detach — see the comment in run().
// The broker's conn-drop + PID-liveness path reaps this holder's claims.
func installSignalHandlers(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, append([]os.Signal{syscall.SIGTERM, syscall.SIGINT}, osutil.ReloadSignals()...)...)
	go func() {
		sig := <-ch
		log.Printf("adapter: received signal=%v pid=%d", sig, os.Getpid())
		cancel()
	}()
}

// setupAdapterLog opens $XDG_STATE_HOME/c3/adapter.log (append, 0600) and tees
// stdlib log there + stderr. Same file as the Claude adapter (one shared
// adapter log); the started line carries cli=grok so interleaved lines stay
// attributable.
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
	log.Printf("adapter: started pid=%d cli=grok", os.Getpid())
	return path, nil
}

// idleStartupWatchdog cancels ctx if Grok never sends an MCP frame within
// idleStartupTimeout. Grok's MCP host may abandon a spawned adapter
// without driving stdin (similar to Claude Code on `--resume`); the
// watchdog lets us exit cleanly rather than hold a broker conn forever.
func (a *adapter) idleStartupWatchdog(ctx context.Context, cancel context.CancelFunc) {
	timer := time.NewTimer(idleStartupTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		if !a.dispatched.Load() {
			log.Printf("adapter: idle-startup timeout pid=%d (no MCP frame in %v) — exiting so Grok can respawn",
				os.Getpid(), idleStartupTimeout)
			cancel()
		}
	}
}

type adapter struct {
	// transport wraps the stdio transport to expose Notify for the
	// `notifications/message` log frame the SDK's Log API would normally
	// emit but with our own shape (no level filtering).
	transport *logNotifyTransport

	bmu  sync.Mutex
	conn *ipc.Conn

	pmu     sync.Mutex
	pending map[string]chan ipc.ToolResultMsg
	nextID  atomic.Uint64

	// fetch_queue / retranscribe have their own pending maps keyed by request
	// ID so the typed broker responses don't have to share the generic
	// pending[ToolResultMsg] slot. Mirrors the Claude adapter.
	fqmu      sync.Mutex
	fqPending map[string]chan ipc.FetchQueueResp
	rtmu      sync.Mutex
	rtPending map[string]chan ipc.RetranscribeResp

	helloAck ipc.HelloAckMsg

	// Last successful attach request — replayed on broker reconnect so a
	// session that survives a broker restart auto-reclaims its route (D3 /
	// adapter-ipc-3). Nil until the session attaches; the `detach` tool clears
	// it again so a deliberately-released route is not silently re-claimed by a
	// reconnect replay. Mirrors the Claude adapter's
	// rememberAttach/replayLastAttach machinery.
	amu        sync.Mutex
	lastAttach *ipc.AttachReq
	// attachedTopic is the human-readable name of the currently-attached topic
	// (attached.Name at attach time). Read when rendering the backlog / pending-fetch
	// nudges so a human can see WHICH topic a fetch_queue would drain — a stale/wrong
	// advertisement is then distinguishable rather than an anonymous "N pending"
	// (spec §5). Empty when unattached. Guarded by amu. Mirrors the Claude adapter.
	attachedTopic string

	// dispatched is set the first time the SDK routes a method through the
	// receiving middleware — i.e. Grok has sent at least one MCP frame.
	dispatched atomic.Bool

	// brokerDownAdvised guards the D5 loud advisory so the "inbound is down"
	// SystemEvent is surfaced once per outage, not on every recovery cycle.
	// Reset on a successful reconnect so a later outage re-advises.
	brokerDownAdvised atomic.Bool

	// forwardCh feeds the SINGLE serial Grok-forward goroutine (grokForwardLoop,
	// started in newAdapter). Enqueueing here instead of spawning a goroutine per
	// inbound is the M2 loss fix: the broker's OpInboundDelivered consume is
	// count-off-HEAD (worker.go handleConsume → Queue.Consume(n); MessageID only
	// logged), so an ack is only safe when acks arrive in queue order, head-first,
	// and never while an earlier message is undelivered. Serial processing gives
	// in-order; the forwardBlocked latch gives never-ack-past-a-gap.
	forwardCh chan grokForwardReq

	// forwardBlocked latches true the instant an undelivered gap opens at/near the
	// queue head — a leader inject FAILURE in grokForwardLoop OR a forwardCh
	// buffer-full DROP in handleInbound. While set, the loop stops acking so no
	// later count-off-head ack can Consume an undelivered head message. It is NOT
	// process-lifetime: a full fetch_queue(ack=true) drain (Remaining==0) re-syncs
	// the queue head and clearForwardBlocked re-arms per-message acking. The old
	// never-cleared latch froze acks forever after ONE inject hiccup, so every
	// later live-handled message stayed queued and re-delivered to the next
	// session (2026-07-10 double-delivery incident, task #43).
	forwardBlocked atomic.Bool

	// forwardEpoch is bumped by clearForwardBlocked on every full-drain re-sync.
	// Each grokForwardReq captures the epoch at ENQUEUE time (handleInbound); the
	// loop skips — no inject, no ack — any non-event req from an older epoch: its
	// durable queue line was already consumed AND delivered to the agent by the
	// very fetch_queue drain that bumped the epoch, so re-injecting would
	// duplicate content and acking would Consume a NEWER post-drain line off the
	// head (silent loss). The loop re-checks the epoch at ACK time too, closing
	// the window where a drain completes while an inject is in flight.
	forwardEpoch atomic.Uint64

	// leader is the long-lived Grok leader ACP client used for live inject.
	// REQUIRED — without leader mode, inbound stays in the durable queue.
	leader *leaderClient

	// Session-resume recover (OpRecoverSession).
	runCtx    context.Context
	rsmu      sync.Mutex
	rsPending chan ipc.RecoverSessionResp
	// recoverFired guards RecoverSessionReq to at most once per BROKER
	// CONNECTION (not per process): fireRecover CompareAndSwaps it, and
	// refireRecoverOnReconnect RESETS it after a successful reconnect so a
	// FRESH broker (self-update / rebuild restart, which has no stub and no
	// reconnect-transfer) re-learns this session's stable id — otherwise every
	// post-restart attach records nothing and a later Grok resume silently
	// re-attaches to a stale topic. Claude-adapter parity (§3d2).
	recoverFired atomic.Bool
	// recoverStarted records that a recovery attempt has BEGUN — set
	// synchronously by every caller BEFORE it invokes fireRecover. Never reset:
	// it answers "is there an identity question in flight for this process?".
	// recoverFired cannot answer that — it is reset by refireRecoverOnReconnect,
	// released on the nothing-was-sent paths, and set inside fireRecover.
	recoverStarted atomic.Bool
	// identitySettled is closed once the first recovery attempt has FINISHED —
	// recovered, registered, refused, failed, or timed out. It is the completion
	// signal recoverFired was being misread as (recoverFired records ENTRY, and
	// entry is not an answer), and ensureStableSessionRegistered waits on it so an
	// attach is never answered while this session still doesn't know who it is.
	// Created lazily by identityGate so an adapter built as a bare struct literal
	// (as the forward tests do) behaves like one from newAdapter. One-shot for the
	// process: a later re-registration improves the broker's records without
	// re-opening an answered question — re-arming would let an attach block on a
	// gate nobody is left to close.
	idmu            sync.Mutex
	identitySettled chan struct{}
	settleOnce      sync.Once
}

// grokForwardReq is one inbound queued for the serial Grok-forward goroutine.
// conn is captured at ENQUEUE time (handleInbound) — not resolved at completion —
// so a broker reconnect+reattach during the up-to-15s forward can't make the ack
// land on a route this stub no longer holds (M2 hazard b). epoch is likewise
// captured at enqueue time; a full-drain re-sync (clearForwardBlocked) bumps the
// adapter's forwardEpoch, marking every earlier req stale — its queue line was
// consumed + delivered by that drain, so the loop must neither re-inject nor ack
// it (see forwardEpoch).
type grokForwardReq struct {
	inbound c3types.Inbound
	covered int
	conn    *ipc.Conn
	epoch   uint64
}

func newAdapter() *adapter {
	cwd, _ := os.Getwd()
	if v := os.Getenv("C3_GROK_CWD"); v != "" {
		cwd = v
	}
	a := &adapter{
		pending:   map[string]chan ipc.ToolResultMsg{},
		fqPending: map[string]chan ipc.FetchQueueResp{},
		rtPending: map[string]chan ipc.RetranscribeResp{},
		forwardCh: make(chan grokForwardReq, 256),
		leader: &leaderClient{
			sessionID: resolveGrokSessionID(),
			cwd:       cwd,
			sockPath:  leaderSocketPath(),
		},
	}
	// Single serial inject consumer — order-preserving acks (M2).
	go a.grokForwardLoop()
	return a
}

func (a *adapter) connectBroker() error {
	sockPath := broker.SocketPath()
	for attempt := 0; attempt < 50; attempt++ {
		c, err := net.Dial("unix", sockPath)
		if err == nil {
			a.bmu.Lock()
			a.conn = ipc.NewConn(c)
			a.bmu.Unlock()
			return nil
		}
		if attempt == 0 {
			_ = spawnBroker()
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("could not reach broker at %s after 10s", sockPath)
}

// spawnBroker forks a `c3-broker` process detached from our process group
// so it survives our shutdown. The detached-launch semantics (setsid, no
// stdio, async reap) live in internal/spawn, shared with the Claude adapter
// (D7) per the maintainer's "every flow must work the same in Grok" principle.
//
// Closes report MINOR m3 (2026-05-19).
func spawnBroker() error {
	return spawn.Detached(exec.Command("c3-broker"))
}

func (a *adapter) hello() error {
	cwd := os.Getenv("C3_GROK_CWD")
	if cwd == "" {
		cwd, _ = os.Getwd()
	}
	if err := a.conn.WriteJSON(ipc.HelloMsg{
		Op: ipc.OpHello, CLI: "grok", PID: os.Getpid(), CWD: cwd,
		Capabilities:    []string{"leader-inject", "fetch_queue"},
		ProtocolVersion: ipc.ProtocolVersion,
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
	if w := ipc.AdapterProtocolWarning("grok", ack.ProtocolVersion); w != "" {
		log.Print(w)
	}
	a.helloAck = ack
	return nil
}

func (a *adapter) currentConn() *ipc.Conn {
	a.bmu.Lock()
	defer a.bmu.Unlock()
	return a.conn
}

// brokerReader runs in a goroutine, draining frames from the broker. On any
// read error it runs the recovery loop (exponential backoff, no give-up) until
// either ctx is cancelled or a usable connection is re-established (D1 /
// adapter-ipc-2). Previously this reconnected exactly ONCE (a one-shot flag),
// so the next broker bounce left the adapter silently dead. After recovery it
// replays the last successful attach so the route claim is restored without
// user intervention (D3 / adapter-ipc-3). Mirrors the Claude adapter's
// brokerReader/recoverBroker structure.
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
		case ipc.OpInbound:
			a.handleInbound(raw)
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
			log.Printf("grok: ignoring unknown op %q from broker (this adapter speaks protocol v%d — the broker may be a newer c3 build; restart this CLI to match)", op, ipc.ProtocolVersion)
		}
	}
}

// reconnectBroker tears down the dead conn, dials a fresh one, sends hello.
// Pending tool calls are woken with an error so callers don't hang during the
// reconnect window. Single attempt; recoverBroker is the retry-loop wrapper.
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

// recoverBrokerAdviseAfter bounds how long the recovery loop retries silently
// before surfacing the D5 loud advisory. ~30s at the 0.5→30s backoff schedule
// is roughly 6 attempts (0.5+1+2+4+8+16 ≈ 31.5s), so attempt 6 is the trip
// point — long enough to ride out an ordinary broker rebuild, short enough that
// a real outage is surfaced before the user assumes inbound is working.
const recoverBrokerAdviseAfter = 6

// recoverBroker loops with exponential backoff until reconnectBroker succeeds,
// or ctx cancellation aborts the loop (returns false). After a successful
// reconnect it replays the last successful attach (best-effort) so the route
// claim is restored (D3). If the broker is still unreachable after
// recoverBrokerAdviseAfter attempts (~30s), it surfaces a one-shot loud
// advisory so the user knows inbound is down (D5) — the session otherwise looks
// alive. Mirrors the Claude adapter's recoverBroker.
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

// adviseBrokerDown surfaces a LOUD one-shot advisory to the Grok session when
// the broker can't be re-established after several recovery cycles (D5 /
// adapter-ipc-5). It reuses the broker's SystemEvent shape (the same channel a
// broker-side health alert rides on) but synthesizes it adapter-side, since the
// whole point is that the broker is unreachable. brokerDownAdvised keeps it
// from spamming on every retry; it is cleared on the next successful reconnect
// so a later outage re-advises.
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
		Message: fmt.Sprintf("C3 lost its connection to the broker and could not reconnect after %d attempts. Inbound Telegram messages will NOT arrive until this recovers (the adapter is still retrying in the background). Your phone messages won't reach this session meanwhile.", attempt),
	}
	body := "⚠️ SYSTEM: " + sysev.Message
	// The ring is retired; the broker's durable queue is the source of truth.
	// But a broker-DOWN advisory can't be queued (the broker is exactly what's
	// unreachable), so it is surfaced ONLY via the best-effort notify frame. If
	// that notify fails we log the FULL advisory so it is recoverable from
	// adapter.log — the same "don't lose undelivered content" rule the broker
	// follows (DEBUGGING.md).
	if err := a.transport.Notify(context.Background(), "notifications/message", map[string]any{
		"level":  "warning",
		"logger": "c3",
		"data":   body,
	}); err != nil {
		log.Printf("broker-down advisory notify failed: %v — %s", err, body)
	}
	log.Printf("broker-down advisory surfaced (attempt %d)", attempt)
}

// clearBrokerDownAdvisory re-arms the D5 advisory after a successful reconnect
// so a future outage surfaces a fresh advisory.
func (a *adapter) clearBrokerDownAdvisory() {
	a.brokerDownAdvised.Store(false)
}

// rememberAttach stores the last successful attach request for replay on
// reconnect (D3). The pointer captures all dimensions (target/name/topic_id/
// group/create) the session originally chose. Mirrors the Claude adapter.
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
// topic for the fetch_queue nudges (spec §5). Mirrors the Claude adapter.
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
// (no Target, Name, TopicID, or Create — Grok has no Expr arg). Mirrors the
// Claude adapter.
func isBareAttachReq(req ipc.AttachReq) bool {
	return req.Expr == "" && req.Target == "" && req.Name == "" && req.TopicID == nil && !req.Create
}

// rememberedIdentityReq builds the AttachReq to REMEMBER for a route that
// resolved to a concrete identity, addressing a topic by its stable id + group
// (NOT by name + group). A name+non-default-group replay can't re-claim across
// groups: attachByName's step-1 searches only the default group, and its
// otherGroupHits step excludes same-group hits — so the create proposal is
// discarded and the claim silently dropped; a registry-missing topic also
// synthesizes an unreplayable "topic-N" name. attachByTopicID(topic_id, group)
// re-claims cleanly across groups. A DM has no topic → replay by target. chatID is
// an OPTIONAL cross-check (ipc.AttachReq.ChatID) so a replay whose group resolves
// to a different chat than the topic lived in is refused fail-closed rather than
// binding a same-id thread in the wrong chat. Parity with the Claude adapter
// (items C + id-addressed cross-check).
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

// resolvedAttachReq picks the request to REMEMBER for reconnect replay (§3d1) —
// an explicit request verbatim, a BARE-resolved one as its RESOLVED identity
// ({TopicID, Group} for a topic, {Target:"dm"} for the DM) so a fresh-broker
// replay re-binds explicitly instead of regressing to a picker. Parity with the
// Claude adapter; DORMANT here because a Grok bare attach never returns OK
// post-Phase-1 (it always yields a picker), but wired for symmetry.
func resolvedAttachReq(req ipc.AttachReq, attached ipc.AttachedMsg) ipc.AttachReq {
	if !isBareAttachReq(req) {
		return req
	}
	return rememberedIdentityReq(req.CWD, attached.ChatID, attached.TopicID, attached.Group)
}

// replayLastAttach re-sends the saved attach request to the (just-reconnected)
// broker so the route claim is re-established without user intervention (D3).
// Best-effort — failures are logged and not surfaced. The Replay flag tells the
// broker to suppress the on-attach welcome (a bounce isn't a user-initiated
// attach). The broker's AttachedMsg response flows back through brokerReader →
// dispatchAttached; no pending channel is registered for a replay, so it's
// simply absorbed. The point is to re-claim the route, not to confirm.
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

// handleInbound enqueues the inbound for serial leader inject (session/prompt).
// On success the inject loop acks OpInboundDelivered so the broker consumes the
// durable queue line. On failure the message stays queued for fetch_queue recovery.
func (a *adapter) handleInbound(raw []byte) {
	var msg ipc.InboundMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	req := grokForwardReq{inbound: msg.Inbound, covered: msg.Covered, conn: a.currentConn(), epoch: a.forwardEpoch.Load()}
	select {
	case a.forwardCh <- req:
	default:
		log.Printf("grok inject queue full — skipping live inject for inbound id=%d; latching forwardBlocked (fetch_queue recovery)", msg.Inbound.MessageID)
		a.latchForwardBlocked("inject queue full")
	}
}

// inboundContentSummary renders a one-line, content-bearing summary of an
// inbound for the notify-FAIL log path (D4). It includes content (sender,
// text, attachment summary) so a dropped notify is recoverable from
// adapter.log. Mirrors the Claude adapter's helper of the same name.
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

// capRunes truncates s to at most n runes (rune-safe), appending an ellipsis
// when it trims — matches the broker's 200-char content-log cap so a multi-KB
// paste can't dump in full into adapter.log on a notify-fail.
func capRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// eventInjectBudget bounds the WHOLE inject attempt (retries included) for a
// best-effort channel event. Events ride the same single serial consumer as
// durable messages, so the full injectWithRetry budget (~2min of mid-turn
// backoff) on a never-acked event would head-of-line-block real durable
// traffic behind a busy TUI. An event that can't land inside this budget is
// dropped — by design it has no durable copy broker-side (worker.go forces
// Covered=0 and never queues events), so there is nothing to recover.
const eventInjectBudget = 10 * time.Second

// grokForwardLoop is the SINGLE consumer of forwardCh. It injects each inbound
// as a Grok turn via the leader ACP client (session/prompt), strictly in enqueue
// order, and is the ONLY path that sends OpInboundDelivered.
//
// Mid-turn: session/prompt may fail while a turn is in flight. We retry with
// backoff (Grok will queue or accept once free). The forwardBlocked latch fires
// only after retries are exhausted, and holds until a full fetch_queue(ack=true)
// drain re-syncs the queue head (clearForwardBlocked) — NOT for the process
// lifetime, which froze acks forever after one hiccup (task #43).
//
// M2: count-off-HEAD acks require serial, head-first delivery and never-ack-past-gap.
func (a *adapter) grokForwardLoop() {
	for req := range a.forwardCh {
		// Events (poll_result/reaction/callback/system) are not durable queue
		// lines — inject best-effort under a short budget, never ack a consume.
		// Rendered with the event-aware formatter: formatInboundTurnText reads
		// only Text/Attachments (both empty on an event), so routing events
		// through it injected the literal "(empty Telegram message)" and
		// discarded the entire payload (poll tally / reaction diff / callback
		// data). Events skip the epoch check too: they were never queued, so a
		// drain neither delivers nor consumes them.
		if req.inbound.IsEvent() {
			if a.leader != nil {
				text := formatEventTurnText(&req.inbound)
				evCtx, cancel := context.WithTimeout(a.baseCtx(), eventInjectBudget)
				if err := a.injectWithRetry(evCtx, text, req.inbound.MessageID); err != nil {
					log.Printf("grok event inject dropped kind=%s id=%d: %v (best-effort — events are never queued broker-side)",
						req.inbound.Kind, req.inbound.MessageID, err)
				}
				cancel()
			}
			continue
		}

		if req.epoch != a.forwardEpoch.Load() {
			// Enqueued before a full-drain re-sync (clearForwardBlocked): the
			// fetch_queue drain already consumed this message's queue line AND
			// delivered its content to the agent. Re-injecting would duplicate
			// the content into the TUI; acking would Consume a NEWER post-drain
			// line off the head (silent loss). Skip entirely — held-not-lost.
			log.Printf("grok forward skip inbound id=%d: stale epoch (queue re-synced by fetch_queue drain)", req.inbound.MessageID)
			continue
		}

		text := formatInboundTurnText(&req.inbound)
		err := a.injectWithRetry(a.baseCtx(), text, req.inbound.MessageID)
		if err != nil {
			if errors.Is(err, errInjectUncertain) {
				// Landed-but-unconfirmed is NOT a plain failure: the prompt was
				// written and may have reached the agent (see the Inject
				// contract). Still never ack — held-as-backlog / a later
				// fetch_queue double-delivery is the accepted safe direction;
				// consuming a possibly-undelivered line is not.
				log.Printf("grok leader inject UNCERTAIN for inbound id=%d: %v — prompt may have landed; NOT acked (message stays queued: double-delivery possible, loss is not)", req.inbound.MessageID, err)
			} else {
				log.Printf("grok leader inject failed for inbound id=%d after retries: %v", req.inbound.MessageID, err)
			}
			a.latchForwardBlocked("leader inject failed: " + err.Error())
			continue // DO NOT ack — durable queue keeps the message
		}
		if a.forwardBlocked.Load() || req.epoch != a.forwardEpoch.Load() {
			// Delivered live, but either an earlier gap is still undelivered
			// (forwardBlocked) or a full drain re-synced the head while this
			// inject was in flight (epoch re-check) — an ack now would Consume
			// a line this inject did not deliver. Skip: the message stays
			// queued / was already drained — a benign duplicate, never loss.
			continue
		}
		// Success AND head-synced: safe to ack so the broker Consumes exactly
		// the lines this push covered. Count echoes Covered VERBATIM — no 0→1
		// bump: the broker forwards Count as-is and drops Count<1 precisely so
		// a covers-nothing ack can never consume a real backlog line the push
		// didn't deliver (queue_dispatch.go C1). A zero-covered push therefore
		// skips the ack (nothing to consume; the line stays queued for
		// fetch_queue — the safe side). Claude/codex parity.
		if req.conn != nil && req.covered >= 1 {
			_ = req.conn.WriteJSON(ipc.InboundDeliveredMsg{
				Op: ipc.OpInboundDelivered, UpdateID: req.inbound.MessageID, OK: true, Count: req.covered,
			})
		}
	}
}

// formatEventTurnText renders a synthesized channel event (poll_result /
// reaction / callback / system) as a Grok user turn. It ports the content half
// of the Claude adapter's buildEventFrame contract (cmd/c3-claude-adapter/
// main.go): poll tallies, reaction diffs, callback data, and system advisories
// become real text instead of the "(empty Telegram message)" that
// formatInboundTurnText produced for payload-less inbounds. Grok's leader
// inject is plain text, so the structured meta map is folded into a short
// trailer line (event kind + chat/topic), mirroring formatInboundTurnText's
// body-first/meta-last shape so TUI previews show the payload. Event payloads
// embed channel-controlled text (poll questions/options, callback data), so
// the content passes through the same forgery sanitizer as message bodies and
// the trailer carries the host-owned sentinel (see c3TrailerSentinel).
func formatEventTurnText(in *c3types.Inbound) string {
	content := sanitizeInjectBody(renderEventContent(in))
	trailer := fmt.Sprintf("%s — %s event", c3TrailerSentinel, in.Kind)
	// A broker-originated system advisory has no chat (ChatID 0) — omit the
	// channel suffix rather than render a meaningless "· 0".
	if in.ChatID != 0 {
		channel := strconv.FormatInt(in.ChatID, 10)
		if in.TopicID != nil {
			channel = fmt.Sprintf("%d/%d", in.ChatID, *in.TopicID)
		}
		trailer += " · " + channel
	}
	return content + "\n\n" + trailer
}

// renderEventContent renders an event Inbound's payload into the content
// string. Byte-parity with the Claude adapter's buildEventFrame content
// rendering (its meta map has no plain-text equivalent here); the default arm
// is the same safe fallback for an unknown/empty event shape.
func renderEventContent(in *c3types.Inbound) string {
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
		return b.String()

	case ev != nil && ev.Reaction != nil:
		r := ev.Reaction
		var b strings.Builder
		fmt.Fprintf(&b, "%s reacted on message %d", senderLabel(r.Actor), r.MessageID)
		if len(r.Added) > 0 {
			fmt.Fprintf(&b, " — added %s", strings.Join(r.Added, " "))
		}
		if len(r.Removed) > 0 {
			fmt.Fprintf(&b, " — removed %s", strings.Join(r.Removed, " "))
		}
		return b.String()

	case ev != nil && ev.Callback != nil:
		cb := ev.Callback
		return fmt.Sprintf("%s pressed a button (data=%q) on message %d", senderLabel(cb.Actor), cb.Data, cb.MessageID)

	case ev != nil && ev.System != nil:
		// Broker-originated system advisory (e.g. a channel-health alert
		// broadcast to every CLI session). Surfaced loud so the user sees that
		// their phone messages won't arrive while the channel is down. NOT user
		// content — purely operational.
		return "⚠️ SYSTEM: " + ev.System.Message

	default:
		return fmt.Sprintf("(%s event)", in.Kind)
	}
}

// senderLabel renders a Sender into a short display label for event content.
// Same helper as the Claude adapter's (Go has no cross-main-package sharing).
func senderLabel(s c3types.Sender) string {
	if s.Username != "" {
		return "@" + s.Username
	}
	if s.UserID != 0 {
		return "user " + strconv.FormatInt(s.UserID, 10)
	}
	return "someone"
}

// baseCtx is the parent context for leader injects: the adapter run context
// when wired, so shutdown CANCELS an in-flight inject instead of waiting out
// the 120s socket deadline (grokForwardLoop used to pass context.Background(),
// which nothing could interrupt); Background only when unwired (tests
// construct the adapter without run()). The unsynchronized runCtx read is
// safe: run() writes it once before starting brokerReader, and this is only
// called for reqs that arrived through forwardCh FROM brokerReader — the
// channel send/receive orders the read after the write.
func (a *adapter) baseCtx() context.Context {
	if a.runCtx != nil {
		return a.runCtx
	}
	return context.Background()
}

// injectRetryBaseWait / injectRetryMaxWait shape injectWithRetry's mid-turn
// backoff (2s doubling to 15s, ~2min worst case over 12 attempts). They are
// vars (not consts) only so tests can shorten the schedule; production never
// reassigns them (same convention as broker.workerJobTimeout).
var (
	injectRetryBaseWait = 2 * time.Second
	injectRetryMaxWait  = 15 * time.Second
)

// injectWithRetry retries session/prompt when Grok is mid-turn. Transient
// errors (turn in flight / busy) wait and retry; other errors fail fast —
// including errInjectUncertain, which must NEVER be retried: the prompt may
// already have landed, so a retry could double-deliver into the TUI (see the
// Inject contract in leader.go).
func (a *adapter) injectWithRetry(ctx context.Context, text string, msgID int64) error {
	if a.leader == nil {
		return errLeaderUnavailable
	}
	const maxAttempts = 12
	var last error
	wait := injectRetryBaseWait
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		last = a.leader.Inject(ctx, text)
		if last == nil {
			if attempt > 1 {
				log.Printf("grok inject id=%d succeeded on attempt %d", msgID, attempt)
			}
			return nil
		}
		if !isTransientInjectErr(last) || attempt == maxAttempts {
			return last
		}
		log.Printf("grok inject id=%d attempt %d: %v — retry in %v (mid-turn/busy)", msgID, attempt, last, wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		if wait < injectRetryMaxWait {
			wait *= 2
			if wait > injectRetryMaxWait {
				wait = injectRetryMaxWait
			}
		}
	}
	return last
}

func isTransientInjectErr(err error) bool {
	if err == nil {
		return false
	}
	// An UNCERTAIN inject may already have landed — retrying could
	// double-deliver into the TUI, so it is never transient (the durable
	// queue keeps the message instead).
	if errors.Is(err, errInjectUncertain) {
		return false
	}
	s := strings.ToLower(err.Error())
	// Grok host phrases for "can't start a new turn yet".
	for _, p := range []string{
		"turninflight",
		"turn in flight",
		"already running",
		"turn has not finished",
		"in progress",
		"busy",
		"try again",
		"not finished",
	} {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

// latchForwardBlocked marks the Grok forward path blocked when an undelivered
// gap opens at the queue head — either a forward FAILURE (grokForwardLoop) or a
// forwardCh buffer-full DROP (handleInbound). It is idempotent and
// concurrency-safe: both callers race through CompareAndSwap, and only the
// goroutine that wins the false→true transition proceeds. While latched,
// grokForwardLoop stops acking so no later count-off-head ack consumes the
// undelivered head message (the loss vector this closes). The latch holds until
// a full fetch_queue(ack=true) drain re-syncs the queue head — the exact action
// the nudge below prompts — at which point clearForwardBlocked re-arms acking.
//
// On the transition it fires exactly ONE fetch_queue recovery nudge so the agent
// drains the durable backlog even in bridge mode — where the steady-state nudge in
// handleInbound is intentionally suppressed (it would race the forward's ack). This
// restores the "fetch_queue as source of truth" fallback the design assumes without
// waiting for a session restart. The nudge is best-effort; the content is durably
// queued regardless, so a Notify failure is logged, not fatal.
func (a *adapter) latchForwardBlocked(reason string) {
	if !a.forwardBlocked.CompareAndSwap(false, true) {
		return // already latched this session — the one-shot nudge already fired
	}
	if a.transport == nil {
		return
	}
	// Name the topic (§5) so a stale/wrong nudge is human-distinguishable.
	target := "pending Telegram messages"
	if route := a.currentTopicName(); route != "" {
		target = fmt.Sprintf("pending Telegram messages for topic %q", route)
	}
	if err := a.transport.Notify(context.Background(), "notifications/message", map[string]any{
		"data": "c3: live Telegram inject interrupted (" + reason + ") — call `fetch_queue` to read " + target + ". C3 Grok requires leader mode (`[cli] use_leader = true`).",
	}); err != nil {
		log.Printf("grok forwardBlocked recovery nudge FAIL (%s): %v — content durably queued; call fetch_queue to drain", reason, err)
	}
}

// clearForwardBlocked re-arms per-message acking after the durable queue has
// been FULLY re-synced: a fetch_queue(ack=true) that left Remaining==0 consumed
// every queued line and delivered its content to the agent, so no undelivered
// gap can sit at/near the head anymore. This is the un-latch half of
// latchForwardBlocked — without it, ONE inject failure froze acking for the
// process lifetime, so every later message the session handled live stayed
// queued and re-delivered wholesale to the next session that claimed the topic
// (the 2026-07-10 double-delivery incident, task #43).
//
// The epoch bump invalidates reqs still sitting in forwardCh from before the
// drain: their lines were consumed + delivered by that very drain, so the loop
// must neither re-inject them (duplicate) nor ack them (the ack would Consume a
// NEWER post-drain line off the head — loss). Bump-then-clear ordering matters:
// an enqueue or in-flight inject racing the two writes degrades to
// held-as-backlog (skipped / unacked), never to an over-consume.
//
// Called on EVERY full ack-drain, latched or not — the epoch bump also narrows
// the pre-existing fetch-vs-forward race where a drain lands while a healthy
// forward is mid-inject.
func (a *adapter) clearForwardBlocked() {
	a.forwardEpoch.Add(1)
	if a.forwardBlocked.CompareAndSwap(true, false) {
		log.Printf("grok forwardBlocked cleared: durable queue fully drained (fetch_queue ack=true, remaining=0) — live inject acks re-enabled")
	}
}

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
		// reflects the attached channel (multi-channel turn-time-refresh seam,
		// spec §L5). v1 single-channel: the hello_ack caps already cover the
		// live session — kept for parity with the Claude adapter.
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

// buildMCPServer constructs the SDK-backed MCP server with Grok-specific
// initialize fields, registers all tools, and installs the receiving
// middleware that disarms the idle-startup watchdog on first frame.
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
	return srv
}

func (a *adapter) buildInstructions() string {
	leaderNote := "\n\nGrok C3 REQUIRES leader mode for live Telegram inject: set `[cli] use_leader = true` in ~/.grok/config.toml (or launch with `--leader`). Without a leader socket, inbound messages stay in the durable queue — call `fetch_queue` to drain them."
	sid := resolveGrokSessionID()
	if sid != "" {
		leaderNote += fmt.Sprintf(" Bound session id: %s.", sid)
	} else {
		leaderNote += " No session id resolved yet (C3_GROK_SESSION_ID / active_sessions.json); live inject will fail until the TUI session is visible."
	}
	var head string
	switch {
	case a.helloAck.NoConfig:
		head = "C3 not yet configured. Run `c3-broker setup` from a shell to provide your Telegram bot token, DM chat id, and at least one group chat id, then restart this Grok session."
	case a.helloAck.NoMapping:
		cwd := os.Getenv("C3_GROK_CWD")
		if cwd == "" {
			cwd, _ = os.Getwd()
		}
		head = fmt.Sprintf("No saved C3 topic for this session (cwd %q). Call the `attach` tool with no argument: the broker returns a picker of suggested topics — list them for the user and let them choose (never guess), then re-invoke `attach` with the chosen `topic_id` or `name`. Or attach a specific topic directly with `attach(name=\"<name>\")`.", cwd)
	default:
		head = "C3 connected. Use `attach` to claim a Telegram topic, `reply` to send to Telegram. Live inbound is injected as a normal user turn via the Grok leader (session/prompt). Use `fetch_queue` only for backlog recovery."
	}
	return head + leaderNote + mode.Combined(a.capsOrDefault())
}

// capsOrDefault returns the channel capability manifest the broker delivered
// on hello_ack (or a fresh attach), falling back to a zero Capabilities when
// the broker predates the CMG build (Capabilities==nil) or no channel was
// resolvable. GuidanceFor renders the zero value as honest all-NO guidance —
// no panic, no fabricated capability. Parity with the Claude adapter: caps
// ride the once-delivered init/setup instructions, NOT per-turn text.
func (a *adapter) capsOrDefault() c3types.Capabilities {
	if a.helloAck.Capabilities != nil {
		return *a.helloAck.Capabilities
	}
	return c3types.Capabilities{}
}

// registerTools adds all adapter tools to srv (user-facing tools + the
// `grok_forward` debug tool).
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

// decodeArgs unmarshals raw tool arguments into a generic map.
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

func (a *adapter) toolAttach(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := decodeArgs(req.Params.Arguments)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	cwd := os.Getenv("C3_GROK_CWD")
	if cwd == "" {
		cwd, _ = os.Getwd()
	}
	// Bind stable session id before attach so broker records session attachment
	// for silent resume, and inject targets the right Grok session.
	a.bindSessionIDForAttach(cwd)
	a.ensureStableSessionRegistered(ctx)
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
			// D3 (adapter-ipc-3): remember the successful attach so a broker
			// restart re-claims this route via replayLastAttach. §3d1: a
			// bare-resolved attach is remembered as its resolved identity so a
			// fresh-broker replay re-binds explicitly. Parity with the Claude
			// adapter's toolAttach.
			a.rememberAttach(resolvedAttachReq(attachReq, attached))
			// Track the resolved topic name for the fetch_queue nudges (§5).
			a.setAttachedTopic(attached.Name)
			// Side-effect surface: OSC-0 title-bar escape to stderr
			// for the currently-attached topic, so the terminal tab
			// names the topic this session is bound to.
			// Cross-CLI parity with the Claude adapter; same gates
			// (tty + C3_NO_TERMINAL_TITLE). See internal/termtitle.
			termtitle.EmitAttach(&attached)
		}
		text := ipc.FormatAttached(&attached)
		// Backlog summary on attach (spec Component 6): if the just-claimed route
		// has held inbound, tell the agent to call fetch_queue. Handles the
		// broker's degraded count-only case (QueuedCount>0 with empty
		// QueuedSummary) gracefully. Parity with the Claude adapter.
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

// toolFetchQueue forwards a fetch_queue pull to the broker and renders the
// returned messages. The agent sees full content; the broker advanced the
// cursor (ack=true) before replying. The in-memory ring is RETIRED — the
// durable broker queue is the single source of truth (parity with the Claude
// adapter's toolFetchQueue).
//
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
		// A FULL ack-drain (Remaining==0) re-syncs the durable queue head:
		// everything queued was just consumed and returned to the agent, so the
		// forwardBlocked undelivered-gap latch can be safely released and
		// per-message live-inject acks resume (see clearForwardBlocked).
		if fq.Ack && resp.Remaining == 0 {
			a.clearForwardBlocked()
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

// dispatchFetchQueueResult routes a broker FetchQueueResp back to the waiting
// toolFetchQueue call by request ID.
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

// dispatchRetranscribeResult routes a broker RetranscribeResp back to the
// waiting toolRetranscribe call by request ID.
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

// renderBacklogSummary renders the on-attach backlog notification text. Empty
// string when nothing is queued. Instructs the agent to call fetch_queue.
//
// The broker may report count>0 with an EMPTY items slice (it degrades to
// count-only when Peek fails) — this still renders the count line + fetch_queue
// hint so the agent knows to drain, just without per-item previews. Byte-
// identical to the Claude adapter's renderBacklogSummary (Go has no cross-
// main-package sharing).
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

// pendingNudge returns a "(N pending for topic "X" — call fetch_queue)" suffix, or
// "" when nothing is pending. Naming the topic (spec §5) makes a stale/wrong
// advertisement human-distinguishable; route=="" falls back to the name-less form.
// Byte-identical to the Claude adapter's pendingNudge.
func pendingNudge(n int, route string) string {
	if n <= 0 {
		return ""
	}
	if route != "" {
		return fmt.Sprintf("(%d pending for topic %q — call `fetch_queue`)", n, route)
	}
	return fmt.Sprintf("(%d pending — call `fetch_queue`)", n)
}

// renderFetchedMessages turns pulled inbound into agent-readable text, one block
// per message with full content + each attachment's file_id/mime/size/name so
// the agent can act on backlog voice/media (download_attachment / retranscribe).
// Byte-identical to the Claude adapter's renderFetchedMessages.
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
// reference — enough for download_attachment/retranscribe, spec Component 4/6c).
// The shared c3types renderer keeps this byte-identical across every adapter.
func renderQueuedInbound(in *c3types.Inbound) string {
	return c3types.RenderQueuedInbound(in)
}

func (a *adapter) toolDetach(_ context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn := a.currentConn()
	if conn == nil {
		return toolErrorResult("broker reconnecting — retry detach in a moment"), nil
	}
	// OpRelease drops the claim; tool is named "detach" (Claude parity).
	if err := conn.WriteJSON(struct {
		Op ipc.Op `json:"op"`
	}{Op: ipc.OpRelease}); err != nil {
		return toolErrorResult("broker write: " + err.Error()), nil
	}
	a.amu.Lock()
	a.lastAttach = nil
	a.attachedTopic = ""
	a.amu.Unlock()
	return toolTextResult("detached"), nil
}

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

func toolTextResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: text}},
	}
}

func toolErrorResult(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{&mcp.TextContent{Text: msg}},
	}
}

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
	enc, err := json.Marshal(result)
	if err != nil {
		return toolErrorResult("marshal result: " + err.Error())
	}
	return toolTextResult(string(enc))
}
