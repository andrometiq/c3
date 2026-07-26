// c3-codex-adapter is the Codex MCP server that bridges Codex's MCP stdio
// protocol to the C3 broker over /tmp/c3.sock (or $XDG_RUNTIME_DIR/c3.sock).
//
// Spec §4.4. The adapter:
//
//  1. On stdin: accept JSON-RPC 2.0 requests from Codex (initialize, tools/list,
//     tools/call, ping, notifications/initialized).
//  2. On the broker socket: connect, send hello, listen for inbound /
//     tool_result / topics_list frames. The session starts UNATTACHED —
//     attaching is explicit (a bare attach shows a picker).
//  3. For tools/call: route adapter-local tools (`attach`, `topics`,
//     `fetch_queue`, `retranscribe`, `codex_forward`) directly; forward all
//     other tools to the broker. Backlog is read on demand via `fetch_queue`
//     (the broker's durable per-route queue is the single source of truth) —
//     there is no in-memory inbound buffer in the adapter.
//  4. For broker-side ipc.OpInbound frames:
//     - Emit a `notifications/message` log notification (cheap; future-proofs
//     for when Codex starts surfacing them — issues #18056/#17543/#15299),
//     carrying a "N pending — call fetch_queue" nudge when the broker reports
//     remaining backlog, so a stuck item resurfaces without a re-attach.
//     - When C3_CODEX_REMOTE_BRIDGE=1 (set by the codex launcher), forward
//     to the Codex app-server via WebSocket so the inbound becomes a
//     real Codex turn.
//
// Spec §4.4.5 env contract from launcher → adapter:
//
//	C3_ATTACH_NAME              topic name inferred from cwd (vestigial — the
//	                            adapter no longer auto-attaches; bare attach
//	                            shows a picker)
//	C3_CODEX_REMOTE_BRIDGE      "1" iff launcher started us; gates forwarding
//	C3_CODEX_CWD                absolute cwd; used for thread/list filtering
//	C3_CODEX_APP_SERVER_WS      ws://host:port of the Codex app-server
//	C3_CODEX_ALLOW_MANUAL_FORWARD  debug bypass for split-brain guard
//
// MCP wire layer: github.com/modelcontextprotocol/go-sdk (v1.6.0+).
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
	// adapterName MUST match the mcp_servers.<key>.* registration the
	// launcher writes into Codex's config (cmd/codex/main.go uses
	// `c3_codex`). Codex's channel/notification dispatch keys on this
	// name; using a different one (e.g. the binary name) silently
	// drops channel frames the same way Claude Code does
	// (see cmd/c3-claude-adapter/main.go's same comment).
	adapterName    = "c3_codex"
	adapterVersion = "0.1.0"

	idleStartupTimeout = 60 * time.Second // mirror cmd/c3-claude-adapter behavior
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "c3-codex-adapter: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	installSignalHandlers(cancel)

	a := newAdapter()
	if err := a.connectBroker(); err != nil {
		log.Printf("adapter: exit pid=%d reason=connect-broker err=%v", os.Getpid(), err)
		return fmt.Errorf("connect broker: %w", err)
	}
	if err := a.hello(); err != nil {
		log.Printf("adapter: exit pid=%d reason=hello err=%v", os.Getpid(), err)
		return fmt.Errorf("hello: %w", err)
	}

	// No startup auto-attach: a Codex session starts UNATTACHED and attaches
	// explicitly (a bare attach shows a picker). The old cwd-inferred
	// C3_ATTACH_NAME auto-attach was a silent cwd-guess bind — the exact
	// mis-target class this redesign closes (spec §2). The launcher still sets
	// C3_ATTACH_NAME, but the adapter no longer consumes it (vestigial).

	srv := a.buildMCPServer()
	a.transport = newLogNotifyTransport(&mcp.StdioTransport{})

	go a.brokerReader(ctx)
	go a.idleStartupWatchdog(ctx, cancel)

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

// installSignalHandlers cancels ctx on SIGTERM/SIGINT/SIGHUP. Without this,
// Go's default behavior is to terminate immediately, leaving no log line
// explaining why the adapter died — exact same rationale as the Claude
// adapter (cmd/c3-claude-adapter/main.go installSignalHandlers).
func installSignalHandlers(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, append([]os.Signal{syscall.SIGTERM, syscall.SIGINT}, osutil.ReloadSignals()...)...)
	go func() {
		sig := <-ch
		log.Printf("adapter: received signal=%v pid=%d", sig, os.Getpid())
		cancel()
	}()
}

// idleStartupWatchdog cancels ctx if Codex never sends an MCP frame within
// idleStartupTimeout. Codex's MCP host may abandon a spawned adapter
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
			log.Printf("adapter: idle-startup timeout pid=%d (no MCP frame in %v) — exiting so Codex can respawn",
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
	// adapter-ipc-3). Nil until the session attaches. (Codex has no detach tool
	// today, so it is never cleared once set.) Mirrors the Claude adapter's
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
	// receiving middleware — i.e. Codex has sent at least one MCP frame.
	dispatched atomic.Bool

	// brokerDownAdvised guards the D5 loud advisory so the "inbound is down"
	// SystemEvent is surfaced once per outage, not on every recovery cycle.
	// Reset on a successful reconnect so a later outage re-advises.
	brokerDownAdvised atomic.Bool

	// forwardCh feeds the SINGLE serial Codex-forward goroutine (codexForwardLoop,
	// started in newAdapter). Enqueueing here instead of spawning a goroutine per
	// inbound is the M2 loss fix: the broker's OpInboundDelivered consume is
	// count-off-HEAD (worker.go handleConsume → Queue.Consume(n); MessageID only
	// logged), so an ack is only safe when acks arrive in queue order, head-first,
	// and never while an earlier message is undelivered. Serial processing gives
	// in-order; the forwardBlocked latch gives never-ack-past-a-gap.
	forwardCh chan codexForwardReq

	// forwardBlocked latches true the instant an undelivered gap opens at/near the
	// queue head — a forward FAILURE in codexForwardLoop OR a forwardCh buffer-full
	// DROP in handleInbound (which runs on the IPC-read goroutine and cannot reach
	// the loop's local scope). Once set, codexForwardLoop stops acking, so no later
	// successful forward's count-off-head ack can Consume the undelivered head
	// message off the queue → no silent loss. Shared across two goroutines, so it
	// is atomic. Latched for the session (a process restart resets it); the
	// false→true transition fires exactly one fetch_queue recovery nudge.
	forwardBlocked atomic.Bool
}

// codexForwardReq is one inbound queued for the serial Codex-forward goroutine.
// conn is captured at ENQUEUE time (handleInbound) — not resolved at completion —
// so a broker reconnect+reattach during the up-to-15s forward can't make the ack
// land on a route this stub no longer holds (M2 hazard b).
type codexForwardReq struct {
	inbound c3types.Inbound
	covered int
	conn    *ipc.Conn
}

func newAdapter() *adapter {
	a := &adapter{
		pending:   map[string]chan ipc.ToolResultMsg{},
		fqPending: map[string]chan ipc.FetchQueueResp{},
		rtPending: map[string]chan ipc.RetranscribeResp{},
		forwardCh: make(chan codexForwardReq, 256),
	}
	// Single serial Codex-forward consumer. Started here (not per-inbound) so all
	// forwards + delivery acks go through one in-order path — the invariant the
	// broker's count-off-head consume requires (M2). Parks on forwardCh until an
	// inbound is enqueued; lives for the process lifetime like the old detached
	// forward goroutines did (no separate shutdown).
	go a.codexForwardLoop()
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
// (D7) per the maintainer's "every flow must work the same in Codex" principle.
//
// Closes report MINOR m3 (2026-05-19).
func spawnBroker() error {
	return spawn.Detached(exec.Command("c3-broker"))
}

func (a *adapter) hello() error {
	cwd := os.Getenv("C3_CODEX_CWD")
	if cwd == "" {
		cwd, _ = os.Getwd()
	}
	if err := a.conn.WriteJSON(ipc.HelloMsg{
		Op: ipc.OpHello, CLI: "codex", PID: os.Getpid(), CWD: cwd,
		Capabilities:    []string{"log-notification", "fetch_queue", "ws-forwarder"},
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
	if w := ipc.AdapterProtocolWarning("codex", ack.ProtocolVersion); w != "" {
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
			log.Printf("codex: ignoring unknown op %q from broker (this adapter speaks protocol v%d — the broker may be a newer c3 build; restart this CLI to match)", op, ipc.ProtocolVersion)
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

// adviseBrokerDown surfaces a LOUD one-shot advisory to the Codex session when
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
// (no Target, Name, TopicID, or Create — Codex has no Expr arg). Mirrors the
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
// Claude adapter; DORMANT here because a Codex bare attach never returns OK
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

// handleInbound: emit a lightweight "new message — call fetch_queue" nudge +
// (if remote-bridge) WS-forward. The in-memory ring is RETIRED — the broker's
// durable queue is the single source of truth; Codex polls it via fetch_queue.
func (a *adapter) handleInbound(raw []byte) {
	var msg ipc.InboundMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}

	// Codex cannot render unsolicited content reliably, so it polls. Replace the
	// retired in-memory ring with a lightweight "N pending" nudge — the durable
	// queue in the broker is the source of truth; the agent calls fetch_queue.
	//
	// I6: the true pending count is msg.Pending + msg.Covered. The broker stamps
	// Pending = lines queued AFTER the covered ones, and Covered = the lines THIS
	// push added (still queued — Codex never sends OpInboundDelivered, so the
	// just-pushed line is NOT consumed). Hardcoding 1 undercounted a debounced
	// merge / existing backlog. Floor at 1 (this push delivered at least itself).
	pendingCount := msg.Pending + msg.Covered
	if pendingCount < 1 {
		pendingCount = 1
	}
	// Nudge = the notify-only delivery signal (agent → fetch_queue → consume). It
	// is SUPPRESSED in bridge mode (M2 hazard c): when forwarding is enabled the
	// serial WS forward below is the SOLE delivery+consume path, and a nudge telling
	// the agent to call fetch_queue(ack=true) would add a SECOND count-off-head
	// consumer racing the forward's ack → over-consume → silent loss. In notify-only
	// mode (forwarding disabled) the nudge is the only delivery signal, so it stays.
	if a.transport != nil && !codexForwardingAllowed() {
		// Name the topic (§5) so a stale/wrong nudge is human-distinguishable.
		route := a.currentTopicName()
		notice := "c3: new Telegram message"
		if route != "" {
			notice += fmt.Sprintf(" for topic %q", route)
		}
		notice += " — call `fetch_queue` to read it. " + pendingNudge(pendingCount, route)
		if err := a.transport.Notify(context.Background(), "notifications/message", map[string]any{
			"data": notice,
		}); err != nil {
			// D4 (adapter-ipc-4): the broker already DURABLY QUEUED this inbound
			// before pushing it to us, so the content is never lost — it stays in
			// the queue until fetch_queue(ack=true) consumes it. The nudge is
			// best-effort; a notify failure is logged (with a content summary) so a
			// delivery problem is visible and recoverable from adapter.log, and the
			// agent can still call fetch_queue to drain the durable copy.
			thread := "-"
			if msg.Inbound.TopicID != nil {
				thread = strconv.FormatInt(*msg.Inbound.TopicID, 10)
			}
			log.Printf("notify FAIL chan=%s chat=%d thread=%s msg=%d: %v — content durably queued; call fetch_queue. %s",
				msg.Inbound.Channel, msg.Inbound.ChatID, thread, msg.Inbound.MessageID, err,
				inboundContentSummary(&msg.Inbound))
		}
	}

	// WS forwarder (gated by env, see split-brain guard). Enqueue onto the SINGLE
	// serial forwarder (codexForwardLoop) instead of spawning a goroutine per
	// inbound — that per-inbound design was the M2 loss bug (an older message's
	// forward failing while a newer one acked Count=1 consumed the OLDER undelivered
	// message off the head). conn is captured NOW (a.currentConn()) so a reconnect
	// during the forward can't redirect the later ack (hazard b). On a SUCCESSFUL,
	// not-blocked forward the loop acks so the broker Consumes the queued copy
	// (D-RC2); on failure it never acks and the content stays queued for fetch_queue
	// recovery. Inbound is copied by value so the loop owns it.
	if codexForwardingAllowed() {
		req := codexForwardReq{inbound: msg.Inbound, covered: msg.Covered, conn: a.currentConn()}
		select {
		case a.forwardCh <- req:
		default:
			// forwardCh full (256 buffered): skip this live forward and do NOT ack.
			// The inbound is durably queued in the broker, but skipping the forward
			// leaves it UNDELIVERED at/near the head. The broker's ack is
			// count-off-HEAD, so a later successful forward acking Count>=1 would
			// Consume THIS undelivered line off the head → silent loss. So latch
			// forwardBlocked exactly as a forward FAILURE does: codexForwardLoop then
			// stops acking and the content survives for fetch_queue recovery, which
			// the one-shot nudge (latchForwardBlocked) prompts even in bridge mode.
			log.Printf("codex forward queue full — skipping live forward for inbound id=%d; latching forwardBlocked so no later ack consumes it (fetch_queue recovery)", msg.Inbound.MessageID)
			a.latchForwardBlocked("forward queue full")
		}
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

func codexForwardingAllowed() bool {
	return os.Getenv("C3_CODEX_REMOTE_BRIDGE") == "1" ||
		os.Getenv("C3_CODEX_ALLOW_MANUAL_FORWARD") == "1"
}

// codexForwardLoop is the SINGLE consumer of forwardCh. It pushes each inbound as
// a Codex turn via WebSocket, strictly in enqueue (= queue) order, and is the ONLY
// path that sends an OpInboundDelivered ack.
//
// WS protocol per spec §4.4 Codex section:
//
//	initialize → notifications/initialized → thread/loaded/list →
//	(thread/list filtered by cwd if multiple loaded) → thread/resume →
//	thread/turn/start
//
// Each inbound opens a fresh short-lived WebSocket (Codex app-server expects new
// turns this way; long-lived sessions would conflict with the visible TUI's
// connection).
//
// M2 correctness: the broker's OpInboundDelivered consume is count-off-HEAD
// (worker.go handleConsume → Queue.Consume(n); MessageID is only logged). That is
// safe ONLY if (1) acks arrive in queue order, head-first, and (2) we never ack
// while an earlier message is still undelivered. The retired per-inbound goroutine
// design broke both: an older message's forward could FAIL (correctly no ack) while
// a newer one SUCCEEDED and acked Count=1 → the broker dropped 1 off the head = the
// OLDER undelivered message → silent loss. Serial processing here gives (1); the
// shared forwardBlocked latch gives (2). forwardBlocked stays set for the session
// once any forward fails OR handleInbound drops a buffer-full inbound (both open an
// undelivered head gap) — Codex then falls back to fetch_queue-as-source-of-truth
// (the safe pre-RC2 behavior); a session restart resets it. It is atomic because
// handleInbound (the IPC-read goroutine) latches it too.
func (a *adapter) codexForwardLoop() {
	for req := range a.forwardCh {
		err := forwardInboundToCodexAppServer(context.Background(), &req.inbound, codexForwardConfigFromEnv())
		if err != nil {
			// errCodexForwardNoWS = forwarding enabled but unconfigured (no WS URL):
			// the forward delivered nothing. It's a benign config state, not a real
			// failure, so don't spam stderr per inbound. Any OTHER error is a real
			// forward failure: log it. Either way the forward delivered nothing, so
			// the head is now an undelivered message and any later ack would consume
			// IT off the head → set blocked and never ack.
			if !errors.Is(err, errCodexForwardNoWS) {
				fmt.Fprintf(os.Stderr, "c3-codex-adapter: WS forward failed for inbound id=%d: %v\n", req.inbound.MessageID, err)
			}
			a.latchForwardBlocked("WS forward failed")
			continue // DO NOT ack — content stays queued (recovery via fetch_queue).
		}
		if a.forwardBlocked.Load() {
			// Delivered live, but an earlier message is still the undelivered head.
			// Acking now would Consume that earlier message off the head → loss. So
			// skip the ack: this (delivered) message stays queued and fetch_queue
			// re-delivers it later — a benign duplicate, never loss.
			continue
		}
		// Success AND not blocked: this message IS the current head and was delivered
		// live → safe to ack so the broker Consumes exactly it. Gated exactly like the
		// Claude adapter (cmd/c3-claude-adapter/main.go ~:655):
		//   - codexForwardingAllowed() already held at the enqueue site, so the
		//     notify-only (forwarding-disabled) path never enqueues — we never ack
		//     content the agent did not receive live (Watch-out #6).
		//   - !IsEvent(): a synthesized event (poll_result/reaction/callback) is never
		//     queued, so it covers zero stored lines — acking one would over-consume
		//     real backlog the event never delivered (Watch-out #5).
		//   - covered >= 1: nothing to consume otherwise. The broker double-guards
		//     (handleInboundDelivered drops Count<1, handleConsume skips Count<1) —
		//     this adapter guard is the first line.
		// req.conn was captured at ENQUEUE time (hazard b), so a reconnect during the
		// forward can't make this land on a route the stub no longer holds.
		// ipc.Conn.WriteJSON is wmu-guarded, so this write is safe from the goroutine.
		if req.conn != nil && !req.inbound.IsEvent() && req.covered >= 1 {
			_ = req.conn.WriteJSON(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: req.inbound.MessageID, OK: true, Count: req.covered})
		}
	}
}

// latchForwardBlocked marks the Codex forward path blocked the first time an
// undelivered gap opens at the queue head — either a forward FAILURE
// (codexForwardLoop) or a forwardCh buffer-full DROP (handleInbound). It is
// idempotent and concurrency-safe: both callers race through CompareAndSwap, and
// only the goroutine that wins the false→true transition proceeds. Once latched,
// codexForwardLoop stops acking so no later count-off-head ack consumes the
// undelivered head message (the loss vector this closes).
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
		"data": "c3: live message forwarding interrupted (" + reason + ") — call `fetch_queue` to read " + target + ".",
	}); err != nil {
		log.Printf("codex forwardBlocked recovery nudge FAIL (%s): %v — content durably queued; call fetch_queue to drain", reason, err)
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

// buildMCPServer constructs the SDK-backed MCP server with Codex-specific
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
	var head string
	switch {
	case a.helloAck.NoConfig:
		head = "C3 not yet configured. Run `c3-broker setup` from a shell to provide your Telegram bot token, DM chat id, and at least one group chat id, then restart this Codex session."
	case a.helloAck.NoMapping:
		cwd := os.Getenv("C3_CODEX_CWD")
		if cwd == "" {
			cwd, _ = os.Getwd()
		}
		head = fmt.Sprintf("No saved C3 topic for this session (cwd %q). Call the `attach` tool with no argument: the broker returns a picker of suggested topics — list them for the user and let them choose (never guess), then re-invoke `attach` with the chosen `topic_id` or `name`. Or attach a specific topic directly with `attach(name=\"<name>\")`. Inbound Telegram messages are held in C3's durable queue; call `fetch_queue` to read them.", cwd)
	default:
		head = "C3 connected. Use `attach` to claim a Telegram topic, `fetch_queue` to read held/new inbound, `reply` to send. Codex doesn't render unsolicited MCP notifications today; call `fetch_queue` when you see a 'new Telegram message' nudge or periodically."
	}
	return head + mode.Combined(a.capsOrDefault())
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
// `codex_forward` debug tool).
func (a *adapter) registerTools(srv *mcp.Server) {
	caps := a.capsOrDefault()
	tools := []struct {
		tool    *mcp.Tool
		handler mcp.ToolHandler
	}{
		{
			tool: &mcp.Tool{
				Name:        "attach",
				Description: "Attach this Codex session to a Telegram topic. Same proposal-flow semantics as Claude Code's attach. If your CLI host's policy layer rejects this call (e.g. Codex approvals_reviewer=auto_review surfacing 'unacceptable risk rejection'), re-invoke with `policy_rejected=true` so the user sees the actionable next-step (tenant admin approval) rather than a silent failure.",
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
				Name:        "detach",
				Description: "Release this session's current Telegram topic claim. After detach, inbound messages on that route fall through to the broker's fallback. No-op if not attached.",
				InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
			},
			handler: a.toolDetach,
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
				Name:        "codex_forward",
				Description: "Debugging/manual override for the Codex app-server WebSocket forwarder. Refused unless C3_CODEX_REMOTE_BRIDGE=1 (set by the codex launcher) or C3_CODEX_ALLOW_MANUAL_FORWARD=1.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"app_server_ws": map[string]any{"type": "string"},
						"thread_id":     map[string]any{"type": "string"},
					},
					"required": []string{"app_server_ws"},
				},
			},
			handler: a.toolCodexForward,
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
	cwd := os.Getenv("C3_CODEX_CWD")
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
			// for the currently-attached topic. Closes TODO #19(a).
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

// toolDetach implements the `detach` tool: send OpRelease and forget the
// last-attach replay. Mirrors the Claude adapter's toolDetach.
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
	// Restore the terminal-emulator's default title — see the EmitAttach
	// call-site comment in toolAttach for context.
	termtitle.Clear()
	return toolTextResult("detached"), nil
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
// The shared c3types renderer keeps this byte-identical across every adapter and
// the codex live-forward turn (formatInboundTurnText delegates to the same fn).
func renderQueuedInbound(in *c3types.Inbound) string {
	return c3types.RenderQueuedInbound(in)
}

func (a *adapter) toolCodexForward(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := decodeArgs(req.Params.Arguments)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	if !codexForwardingAllowed() {
		return toolErrorResult(
			"codex_forward refused: requires C3_CODEX_REMOTE_BRIDGE=1 (set by the codex launcher) or C3_CODEX_ALLOW_MANUAL_FORWARD=1 (debug). Split-brain guard."), nil
	}
	wsURL, _ := args["app_server_ws"].(string)
	if wsURL == "" {
		wsURL = os.Getenv("C3_CODEX_APP_SERVER_WS")
	}
	if wsURL == "" {
		return toolErrorResult("app_server_ws is required (or set C3_CODEX_APP_SERVER_WS)"), nil
	}
	return toolTextResult(fmt.Sprintf("codex_forward registered ws=%s", wsURL)), nil
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
