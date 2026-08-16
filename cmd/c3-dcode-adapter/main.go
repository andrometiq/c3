// c3-dcode-adapter is the dcode (deepagents-code) MCP server that bridges
// dcode's MCP stdio protocol to the C3 broker over its per-user runtime
// socket.
//
// Outbound tools (attach, reply, …) are broker-forwarded exactly like the
// reference Claude Code adapter. Inbound has two paths:
//
//  1. LIVE PUSH (preferred): dcode's external-event Unix socket. When the TUI
//     is launched with DEEPAGENTS_CODE_EXTERNAL_EVENT_SOCKET=1 it listens on
//     <runtime>/deepagents/events-<tui-pid>.sock and accepts newline-JSON
//     events; {"kind":"prompt"} enters the conversation as literal user text
//     (never parsed as a slash/shell command — mode "normal", detect_mode
//     false), and the {"ok":true} reply line is the landing confirmation.
//     The adapter finds the socket by walking /proc ancestors of its own pid
//     (dcode spawns MCP servers with a sanitized env, so XDG_RUNTIME_DIR is
//     NOT inherited and the pid walk is the reliable bind).
//  2. PULL-ONLY fallback: no event socket (flag off) → hello carries
//     cannot_render_channels=true and inbound waits in the durable queue for
//     the fetch_queue tool, like the other pull-only adapters.
//
// No stable session id is exposed to MCP children by dcode, so recover_session
// is deliberately skipped — fail closed rather than guess (docs/ADAPTERS.md).
//
// MCP wire layer: github.com/modelcontextprotocol/go-sdk.
package main

import (
	"bufio"
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
	// adapterName MUST match the MCP server key in ~/.deepagents/.mcp.json
	// (mcpServers.c3) — not the binary name. See docs/ADAPTERS.md §"The
	// adapter owns its MCP surface".
	adapterName    = "c3"
	adapterVersion = "0.1.0"

	idleStartupTimeout = 60 * time.Second // mirror cmd/c3-claude-adapter behavior
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "c3-dcode-adapter: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Persistent adapter log at $XDG_STATE_HOME/c3/dcode-adapter.log.
	if path, err := setupAdapterLog(); err == nil {
		fmt.Fprintf(os.Stderr, "c3-dcode-adapter: log file %s\n", path)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := newAdapter()
	installSignalHandlers(cancel)

	a.eventPath = resolveEventSocketPath()
	if a.eventPath != "" {
		log.Printf("dcode: event socket found at %s — live push enabled", a.eventPath)
	} else {
		log.Printf("dcode: no event socket (launch dcode with DEEPAGENTS_CODE_EXTERNAL_EVENT_SOCKET=1 for live push; requires /proc, i.e. Linux) — pull-only via fetch_queue")
	}

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
	path := filepath.Join(dir, "dcode-adapter.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	log.SetOutput(io.MultiWriter(f, os.Stderr))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("adapter: started pid=%d cli=dcode", os.Getpid())
	return path, nil
}

// ---------------------------------------------------------------------------
// Event-socket bind (dcode TUI → adapter)
// ---------------------------------------------------------------------------

// resolveEventSocketPath locates the dcode TUI's external-event socket.
//
// dcode binds <runtime>/deepagents/events-<pid>.sock (pid = the TUI process)
// when DEEPAGENTS_CODE_EXTERNAL_EVENT_SOCKET is truthy. The adapter is a
// direct child of that process, but dcode spawns MCP servers with a
// SANITIZED environment (HOME/PATH/USER/SHELL/TERM/LOGNAME only — see mcp
// client stdio get_default_environment), so XDG_RUNTIME_DIR must be resolved
// independently (same deterministic probe order as the broker's own
// runtimeDir: XDG_RUNTIME_DIR if an existing dir, then /run/user/$UID,
// then /tmp/c3-$UID) and the owning TUI pid found by walking /proc
// ancestors. Returns "" when no ancestor's socket exists — the adapter then
// runs pull-only.
func resolveEventSocketPath() string {
	if p := strings.TrimSpace(os.Getenv("C3_DCODE_EVENT_SOCK")); p != "" {
		if fi, err := os.Stat(p); err == nil && fi.Mode()&os.ModeSocket != 0 {
			return p
		}
	}
	runtimeDir, err := dcodeRuntimeDir()
	if err != nil {
		return ""
	}
	dir := filepath.Join(runtimeDir, "deepagents")
	for _, pid := range ancestorPIDs(os.Getpid()) {
		p := filepath.Join(dir, fmt.Sprintf("events-%d.sock", pid))
		if fi, err := os.Stat(p); err == nil && fi.Mode()&os.ModeSocket != 0 {
			return p
		}
	}
	// Parallel-session fallback: exactly one event socket in the directory
	// and it belongs to a live process — bind it. Ambiguity refuses (""),
	// matching the Grok adapter's fail-closed session resolution: injecting
	// into the wrong TUI would ack away a durable queue line that was never
	// seen by the user.
	if p := uniqueLiveEventSocket(dir); p != "" {
		log.Printf("dcode: event socket bound by unique-live-socket fallback: %s", p)
		return p
	}
	return ""
}

// uniqueLiveEventSocket scans dir for live-owned events-<pid>.sock entries
// and returns the single live candidate, or "" when there are zero or
// several (refusal is fail-safe: the message stays in the durable queue).
func uniqueLiveEventSocket(dir string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	var candidates []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "events-") || !strings.HasSuffix(name, ".sock") {
			continue
		}
		pidStr := strings.TrimSuffix(strings.TrimPrefix(name, "events-"), ".sock")
		pid, err := strconv.Atoi(pidStr)
		if err != nil || pid <= 0 || !processAlive(pid) {
			continue
		}
		candidates = append(candidates, filepath.Join(dir, name))
	}
	if len(candidates) == 1 {
		return candidates[0]
	}
	return ""
}

// dcodeRuntimeDir mirrors broker.runtimeDir's deterministic probe order
// without importing the unexported symbol: XDG_RUNTIME_DIR when it names an
// existing directory, then an unconditional /run/user/$UID probe, then no
// fallback (dcode's own default_unix_socket_path uses the temp dir, not
// /tmp/c3-$UID, when XDG_RUNTIME_DIR is unset — that env is always set in a
// real desktop session, so the probe pair covers real hosts and tests).
func dcodeRuntimeDir() (string, error) {
	if x := os.Getenv("XDG_RUNTIME_DIR"); x != "" {
		if st, err := os.Stat(x); err == nil && st.IsDir() {
			return x, nil
		}
	}
	canonical := fmt.Sprintf("/run/user/%d", os.Getuid())
	if st, err := os.Stat(canonical); err == nil && st.IsDir() {
		return canonical, nil
	}
	return "", fmt.Errorf("no runtime directory (XDG_RUNTIME_DIR unset and %s missing)", canonical)
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	_, err := os.Stat(fmt.Sprintf("/proc/%d", pid))
	return err == nil
}

func ancestorPIDs(start int) []int {
	out := []int{}
	pid := start
	for i := 0; i < 32 && pid > 1; i++ {
		out = append(out, pid)
		ppid, err := parentPID(pid)
		if err != nil || ppid <= 0 || ppid == pid {
			break
		}
		pid = ppid
	}
	return out
}

func parentPID(pid int) (int, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	s := string(b)
	rparen := strings.LastIndex(s, ")")
	if rparen < 0 || rparen+2 >= len(s) {
		return 0, fmt.Errorf("parse stat")
	}
	fields := strings.Fields(s[rparen+2:])
	if len(fields) < 2 {
		return 0, fmt.Errorf("stat fields")
	}
	return strconv.Atoi(fields[1])
}

// ---------------------------------------------------------------------------
// Adapter state
// ---------------------------------------------------------------------------

type adapter struct {
	transport *logNotifyTransport

	// eventPath is the dcode TUI's external-event socket ("" = pull-only).
	eventPath string

	bmu sync.Mutex
	// connHelloPending is the production-published connection whose HelloAck
	// has not yet been validated. currentConn hides it from ordinary tools while
	// hello uses rawCurrentConn for the handshake.
	connHelloPending *ipc.Conn
	conn             *ipc.Conn

	pmu     sync.Mutex
	pending map[string]chan ipc.ToolResultMsg
	nextID  atomic.Uint64

	fqmu      sync.Mutex
	fqPending map[string]chan ipc.FetchQueueResp
	rtmu      sync.Mutex
	rtPending map[string]chan ipc.RetranscribeResp

	helloAck      ipc.HelloAckMsg
	brokerVersion atomic.Int64

	amu           sync.Mutex
	lastAttach    *ipc.AttachReq
	attachedTopic string

	brokerDownAdvised atomic.Bool
	dispatched        atomic.Bool

	// emu serializes event-socket connections (one in-flight inject at a time).
	emu sync.Mutex
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
			// Spawn failure is non-fatal: the retry loop keeps dialing, and a
			// broker started by any other means (systemd, another session) is
			// picked up on a later attempt either way.
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
	cwd := adapterCWD()
	if err := conn.WriteJSON(ipc.HelloMsg{
		Op: ipc.OpHello, CLI: "dcode", PID: os.Getpid(), CWD: cwd,
		Capabilities:         []string{"fetch_queue"},
		CannotRenderChannels: a.eventPath == "",
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
	if w := ipc.AdapterProtocolWarning("dcode", ack.ProtocolVersion); w != "" {
		log.Print(w)
	}
	a.bmu.Lock()
	a.helloAck = ack
	a.brokerVersion.Store(int64(ipc.PeerProtocolVersion(ack.ProtocolVersion)))
	if a.connHelloPending == conn {
		a.connHelloPending = nil
	}
	a.bmu.Unlock()
	return nil
}

func adapterCWD() string {
	if cwd := os.Getenv("C3_DCODE_CWD"); cwd != "" {
		return cwd
	}
	cwd, _ := os.Getwd()
	return cwd
}

func (a *adapter) currentConn() *ipc.Conn {
	a.bmu.Lock()
	defer a.bmu.Unlock()
	if a.conn != nil && a.connHelloPending == a.conn {
		return nil
	}
	return a.conn
}

// rawCurrentConn is reserved for the Hello handshake.
func (a *adapter) rawCurrentConn() *ipc.Conn {
	a.bmu.Lock()
	defer a.bmu.Unlock()
	return a.conn
}

func (a *adapter) publishBrokerConnection(conn *ipc.Conn) {
	a.bmu.Lock()
	a.conn = conn
	a.connHelloPending = conn
	a.bmu.Unlock()
}

func (a *adapter) retireBrokerConnection() {
	a.bmu.Lock()
	oldConn := a.conn
	a.conn = nil
	a.connHelloPending = nil
	a.bmu.Unlock()
	if oldConn != nil {
		_ = oldConn.Close()
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
		case ipc.OpInbound:
			var msg ipc.InboundMsg
			if err := json.Unmarshal(raw, &msg); err == nil {
				a.handleInbound(ctx, &msg)
			}
		case ipc.OpError:
			var errMsg ipc.ErrorMsg
			_ = json.Unmarshal(raw, &errMsg)
			log.Printf("broker error: %s", errMsg.Err)
		default:
			// An op this build does not know — normally a NEWER broker (mixed
			// versions are routine after `c3 update`; see ipc.ProtocolVersion).
			// Skipping is correct: unknown ops are additive by contract. Log it
			// so the skip is VISIBLE — a silent drop is the worst failure mode.
			log.Printf("dcode: ignoring unknown op %q from broker (this adapter speaks protocol v%d — the broker may be a newer c3 build; restart this CLI to match)", op, ipc.ProtocolVersion)
		}
	}
}

// handleInbound renders one broker push. With a live event socket the message
// is injected into the dcode TUI as a literal user prompt and acked only
// after the TUI confirms landing ({"ok":true}). Without one (pull-only) a
// system advisory still renders as an MCP log notification, ordinary pushes
// are logged-and-dropped (the broker already knows this holder cannot render
// — cannot_render_channels — so human messages stay queued), and events are
// logged per the agy precedent.
func (a *adapter) handleInbound(ctx context.Context, msg *ipc.InboundMsg) {
	kind := "text"
	if msg.Inbound.IsEvent() {
		kind = string(msg.Inbound.Kind)
	} else if len(msg.Inbound.Attachments) > 0 && msg.Inbound.Attachments[0].Kind != "" {
		kind = msg.Inbound.Attachments[0].Kind
	}

	if msg.Inbound.Kind == c3types.InboundSystem {
		a.handleSystemInbound(&msg.Inbound)
		return
	}

	if a.eventPath == "" {
		// Pull-only: the broker never marks this holder's human inbound as
		// delivered (cannot_render_channels), so ordinary messages stay in the
		// durable queue for fetch_queue. Synthesized events are NEVER queued
		// and not recoverable — log the drop visibly (agy precedent).
		log.Printf("dcode: dropped non-system inbound push (kind=%q message_id=%d) — pull-only host cannot render channel-event pushes; human messages remain in the durable queue for fetch_queue", kind, msg.Inbound.MessageID)
		return
	}

	text := renderInjectedPrompt(msg)
	if err := a.injectPrompt(ctx, text); err != nil {
		// Landing NOT confirmed → do NOT ack. The message stays queued as
		// backlog (recoverable via fetch_queue). Log the full content so it is
		// not invisible (Claude adapter D4 rule).
		log.Printf("inject FAIL kind=%s message_id=%d: %v — NOT acked, stays queued — CONTENT: %s",
			kind, msg.Inbound.MessageID, err, inboundContentSummary(&msg.Inbound))
		return
	}
	log.Printf("injected kind=%s message_id=%d covered=%d pending=%d",
		kind, msg.Inbound.MessageID, msg.Covered, msg.Pending)

	// Ack only after confirmed landing, never for a synthesized EVENT (an
	// event covers zero stored lines; acking one would consume a real queued
	// message the event never delivered). Count echoes Covered so a merged
	// batch of N lines consumes exactly N. Token echoed unchanged. The
	// covered>=1 guard is the adapter-side first line; the broker double-guards
	// (drops Count<1) — same gating as the codex/grok adapters.
	if msg.Inbound.IsEvent() || msg.Covered < 1 {
		return
	}
	// Inbound_delivered is a destructive op: an incompatible broker dialect
	// refuses it with an uncorrelated error frame — skip the write outside
	// the compatibility window so the refusal stays quiet and the line stays
	// queued (protocol-version section of docs/ADAPTERS.md).
	if !ipc.ProtocolStateChangesCompatible(int(a.brokerVersion.Load())) {
		log.Printf("inbound_delivered skipped: broker protocol outside the state-change compatibility window — line stays queued")
		return
	}
	if conn := a.currentConn(); conn != nil {
		_ = conn.WriteJSON(ipc.InboundDeliveredMsg{
			Op: ipc.OpInboundDelivered, UpdateID: msg.Inbound.MessageID, OK: true,
			Count: msg.Covered, DeliveryToken: msg.DeliveryToken,
		})
	}
}

// injectPrompt writes one {"kind":"prompt"} line to the dcode TUI's
// external-event socket and waits for the {"ok":true} ack — that ack IS the
// landing confirmation, so a nil return means the TUI accepted the event
// into its conversation queue. Any failure returns non-nil and the caller
// must NOT ack the broker: a later fetch_queue drain may double-deliver,
// which is the accepted safe direction (loss is not). The TUI closes an
// idle client connection after 60s, so a fresh connection per inject is
// also the lifetime model that avoids racing that timeout.
func (a *adapter) injectPrompt(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("empty inject text")
	}
	a.emu.Lock()
	defer a.emu.Unlock()

	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "unix", a.eventPath)
	if err != nil {
		return fmt.Errorf("dial event socket: %w", err)
	}
	defer conn.Close()

	// Bound the whole exchange; cancellation closes the conn via AfterFunc.
	promptCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stop := context.AfterFunc(promptCtx, func() { _ = conn.Close() })
	defer stop()

	payload, err := json.Marshal(map[string]any{
		"kind":           "prompt",
		"payload":        text,
		"correlation_id": fmt.Sprintf("c3-inject-%d", time.Now().UnixNano()),
	})
	if err != nil {
		return err
	}
	if _, err := conn.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("write event: %w", err)
	}

	// The ack line is definitive: {"ok":true} = landed, anything else (NACK,
	// timeout, conn loss) = not confirmed → caller does not ack the broker.
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read ack: %w", err)
	}
	var ack struct {
		OK    bool   `json:"ok"`
		Error string `json:"error,omitempty"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &ack); err != nil {
		return fmt.Errorf("parse ack %q: %w", line, err)
	}
	if !ack.OK {
		return fmt.Errorf("event rejected: %s", ack.Error)
	}
	return nil
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
	return srv
}

func (a *adapter) buildInstructions() string {
	var head string
	switch {
	case a.helloAck.NoConfig:
		head = "C3 not yet configured. Run `c3-broker setup` from a shell to provide your Telegram bot token, DM chat id, and at least one group chat id, then restart this dcode session."
	case a.helloAck.NoMapping:
		head = fmt.Sprintf("No saved C3 topic for this session (cwd %q). Call the `attach` tool with no argument: the broker returns a picker of suggested topics — list them for the user and let them choose (never guess), then re-invoke `attach` with the chosen `topic_id` or `name`. Or attach a specific topic directly with `attach(name=\"<name>\")`. Inbound Telegram messages are held in C3's durable queue; call `fetch_queue` to read them.", adapterCWD())
	default:
		if a.eventPath != "" {
			head = "C3 connected with LIVE inbound: Telegram messages arrive in this conversation automatically as user turns prefixed [Telegram]. Use `attach` to claim a topic, `reply` to send. Messages that arrive while detached wait in the durable queue — `fetch_queue` drains them."
		} else {
			head = "C3 connected (pull-only: launch dcode with DEEPAGENTS_CODE_EXTERNAL_EVENT_SOCKET=1 for live inbound). Use `attach` to claim a Telegram topic, `fetch_queue` to read held/new inbound, `reply` to send. Call `fetch_queue` when you see a 'new Telegram message' nudge or periodically."
		}
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
	cwd := adapterCWD()
	attachReq := ipc.AttachReq{Op: ipc.OpAttach, CWD: cwd}
	if !ipc.ProtocolStateChangesCompatible(int(a.brokerVersion.Load())) {
		return toolErrorResult("attach refused: broker protocol is outside the state-change compatibility window; restart the CLI"), nil
	}
	if v, ok := args["target"].(string); ok {
		attachReq.Target = v
	}
	if v, ok := args["expr"].(string); ok {
		attachReq.Expr = v
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
	case <-time.After(120 * time.Second):
		return toolErrorResult("attach timeout"), nil
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
	case <-time.After(120 * time.Second):
		return toolErrorResult("topics timeout"), nil
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
	fq := ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: strconv.FormatUint(a.nextID.Add(1), 10), Ack: true}
	if v, ok := args["ack"].(bool); ok {
		fq.Ack = v
	}
	// fetch_queue(ack=true) is destructive: refuse locally outside the
	// broker's compatibility window rather than round-tripping a refusal.
	if fq.Ack && !ipc.ProtocolStateChangesCompatible(int(a.brokerVersion.Load())) {
		return toolErrorResult("fetch_queue refused: broker protocol is outside the state-change compatibility window; restart the CLI (peek with ack=false still works)"), nil
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

// stopPollModeNote tailors the stop_poll description to the inbound mode.
// Live mode: poll-close events ARE injected into the conversation (payload
// included), so stop_poll is the deterministic read rather than the only one.
// Pull-only: events are dropped (never queued, not recoverable), so stop_poll
// is the ONLY way to read a tally.
func stopPollModeNote(eventPath string) string {
	if eventPath != "" {
		return " Poll-close events also arrive in the conversation automatically (best-effort); this tool is the deterministic way to read the tally on demand."
	}
	return " This host does not render channel-event pushes, so the automatic poll-close event is NOT delivered here — poll results do NOT arrive on their own, and because channel events are never queued they are not recoverable via `fetch_queue` either. stop_poll is the reliable, deterministic way to read the tally."
}

func (a *adapter) registerTools(srv *mcp.Server) {
	caps := a.capsOrDefault()
	liveNote := ""
	if a.eventPath == "" {
		liveNote = " This host is currently pull-only — call fetch_queue periodically."
	}
	tools := []struct {
		tool    *mcp.Tool
		handler mcp.ToolHandler
	}{
		{
			tool: &mcp.Tool{
				Name:        "attach",
				Description: "Attach this session to a Telegram topic. Either pass `expr` (raw user-supplied string the broker parses: empty=cwd-default, 'dm'=DM, '<int>'=topic-id, 'create <name>' or '-y <name>'=create that name, '<other>'=name) OR structured args. Empty = silently re-attach this session's own topic, or (first time) show a picker. `target='dm'` for the user's DM. `name='X'` for a topic name. `topic_id=N` to claim a known thread. `create=true` to confirm creation. `steal=true` only after user-confirmed force_steal.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"expr":            map[string]any{"type": "string"},
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
				Description: "Retrieve inbound Telegram messages held in the durable queue for the attached topic (messages that arrived while no session was attached, or that a live push didn't confirm). `limit` is how many oldest messages to pull (default 3; or pass the string \"all\" to drain everything). `ack` (default true) consumes them (advances the cursor); ack=false peeks without consuming. Drain all at once for bulk catch-up, or pull in small batches (default 3) to process carefully one group at a time. Returns full content (text/transcript, sender, attachments with file_id) plus how many remain." + liveNote,
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
				Description: "Force-close a poll you sent and read its final aggregate tally (counts per option + total voters). Pass the `message_id` returned when you sent the poll." + stopPollModeNote(a.eventPath),
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

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

// renderEventContent renders a synthesized channel event's payload (poll
// tally / reaction diff / button callback) as plain text — the string half of
// the Claude adapter's buildEventFrame (cmd/c3-claude-adapter/main.go). A
// live push of an event carries the payload in Event, not Text; routing it
// through RenderQueuedInbound alone would inject the literal
// "(poll_result event)" and drop the tally — the exact regression the Grok
// adapter fixed for its inject path. Unknown/empty shapes keep the
// "(<kind> event)" fallback.
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
		fmt.Fprintf(&b, "%s reacted on message %d", eventSenderLabel(r.Actor), r.MessageID)
		if len(r.Added) > 0 {
			fmt.Fprintf(&b, " — added %s", strings.Join(r.Added, " "))
		}
		if len(r.Removed) > 0 {
			fmt.Fprintf(&b, " — removed %s", strings.Join(r.Removed, " "))
		}
		return b.String()

	case ev != nil && ev.Callback != nil:
		cb := ev.Callback
		return fmt.Sprintf("%s pressed a button (data=%q) on message %d", eventSenderLabel(cb.Actor), cb.Data, cb.MessageID)

	default:
		return fmt.Sprintf("(%s event)", in.Kind)
	}
}

// eventSenderLabel renders a Sender into a short display label for event
// content (Claude adapter's senderLabel).
func eventSenderLabel(s c3types.Sender) string {
	if s.Username != "" {
		return "@" + s.Username
	}
	if s.UserID != 0 {
		return "user " + strconv.FormatInt(s.UserID, 10)
	}
	return "someone"
}

// renderInjectedPrompt formats one broker push as the literal user text
// injected into the dcode conversation. The body uses the shared queued
// renderer (sender, message_id, reply context, compact attachments) so live
// and fetch_queue readbacks render identically; the [Telegram] prefix lets
// the agent (and the human reading the TUI) tell pushed channel input from
// typed input. A pending-backlog nudge is appended when the broker reports
// more queued lines than this push covered.
func renderInjectedPrompt(msg *ipc.InboundMsg) string {
	var b strings.Builder
	if msg.Inbound.IsEvent() {
		// Events carry their payload in Event, not Text — render the payload
		// so the tally/reaction/callback reaches the agent, then the shared
		// metadata line (sender, message_id, event=kind).
		b.WriteString("[Telegram] " + renderEventContent(&msg.Inbound))
		if meta := eventMetaLine(&msg.Inbound); meta != "" {
			b.WriteString("\n" + meta)
		}
	} else {
		b.WriteString("[Telegram] " + c3types.RenderQueuedInbound(&msg.Inbound))
	}
	if msg.Pending > 0 {
		b.WriteString(fmt.Sprintf("\n(%d pending — call `fetch_queue`)", msg.Pending))
	}
	return b.String()
}

// eventMetaLine renders the sender/message_id/event-kind metadata suffix for
// an injected event (the fields of RenderQueuedInbound's meta line that apply).
func eventMetaLine(in *c3types.Inbound) string {
	var meta []string
	switch {
	case in.Sender.Username != "":
		meta = append(meta, "from=@"+in.Sender.Username)
	case in.Sender.UserID != 0:
		meta = append(meta, fmt.Sprintf("from=uid=%d", in.Sender.UserID))
	}
	if in.MessageID != 0 {
		meta = append(meta, fmt.Sprintf("message_id=%d", in.MessageID))
	}
	meta = append(meta, "event="+string(in.Kind))
	return strings.Join(meta, " ")
}

// inboundContentSummary renders a one-line, content-bearing summary for the
// inject-FAIL log path — the message is otherwise lost from view, so content
// (not just metadata) is logged for recoverability (Claude adapter D4 rule).
func inboundContentSummary(in *c3types.Inbound) string {
	var parts []string
	switch {
	case in.Sender.Username != "":
		parts = append(parts, "from=@"+in.Sender.Username)
	case in.Sender.UserID != 0:
		parts = append(parts, fmt.Sprintf("from=uid=%d", in.Sender.UserID))
	}
	if in.Text != "" {
		parts = append(parts, fmt.Sprintf("text=%q", in.Text))
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

// parseFetchLimit normalizes the `limit` tool argument into (limit, all).
// Numeric strings, JSON numbers honored and clamped to [1,50]; "all" sets
// All; anything unparseable yields the spec default of 3. NEVER returns a
// negative Limit — the broker worker treats n<0 as the consume-ALL sentinel,
// so clamping here protects the durable queue from a stray limit:-1.
// (Parity with the other adapters' parseFetchLimit.)
func parseFetchLimit(v any) (limit int, all bool) {
	switch t := v.(type) {
	case string:
		if strings.EqualFold(t, "all") {
			return 0, true
		}
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
		blocks = append(blocks, c3types.RenderQueuedInbound(&msgs[i]))
	}
	out := strings.Join(blocks, "\n\n")
	if c3types.InboundsHaveAttachment(msgs) {
		out = c3types.AttachmentFetchHint + "\n\n" + out
	}
	if remaining > 0 {
		out += "\n\n" + pendingNudge(remaining, route)
	}
	return out
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
// {"content":[{"type":"text","text":…}]} — extract the content[].text blocks
// and keep the JSON dump only as the true fallback. (Parity with the other
// adapters' mapResult.)
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
	enc, err := json.Marshal(result)
	if err != nil {
		return toolErrorResult("marshal result: " + err.Error())
	}
	return toolTextResult(string(enc))
}
