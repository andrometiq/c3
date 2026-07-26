// c3-desktop-adapter is the Claude Desktop MCP server that bridges Claude
// Desktop's MCP stdio protocol to the C3 broker over $XDG_RUNTIME_DIR/c3.sock.
//
// Claude Desktop is a GUI chat app that is POLL-ONLY: it cannot render
// unsolicited server notifications and cannot start a turn on its own, and it
// exposes NO per-conversation id, NO project cwd, and NO session-start hook to
// its MCP servers. So this adapter is a pull bridge — structurally the Antigravity
// (agy) adapter, NOT the Claude Code adapter (which relies on live channel push
// Desktop lacks). Since the host cannot render channel frames the adapter sends
// `cannot_render_channels: true` in hello; the broker holds inbound in its durable
// queue and the agent retrieves it with the `fetch_queue` tool.
//
// Outbound tools (attach, reply, …) are broker-forwarded like the Codex/Grok/agy
// adapters.
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
	// adapterName MUST match the MCP server key in the Claude Desktop config
	// (claude_desktop_config.json mcpServers.c3).
	adapterName    = "c3"
	adapterVersion = "0.1.0"

	idleStartupTimeout = 60 * time.Second // mirror cmd/c3-claude-adapter behavior
)

// MCP Apps (SEP-1865, modelcontextprotocol/ext-apps spec 2026-01-26) wiring for
// the inline "C3 Inbox" panel. See registerInboxApp / inboxHTML at the bottom of
// this file. Cited to ext-apps at specification/2026-01-26/apps.mdx.
const (
	uiExtensionID  = "io.modelcontextprotocol/ui" // extension identifier — apps.mdx:40
	uiResourceMIME = "text/html;profile=mcp-app"  // required UI resource mimeType — apps.mdx:268
	uiInboxURI     = "ui://c3/inbox.html"         // ui:// view URI (scheme required) — apps.mdx:267
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "c3-desktop-adapter: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Persistent adapter log at $XDG_STATE_HOME/c3/adapter.log.
	if path, err := setupAdapterLog(); err == nil {
		fmt.Fprintf(os.Stderr, "c3-desktop-adapter: log file %s\n", path)
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
	// No auto-attach: Claude Desktop provides no per-conversation id and no
	// session-start hook, so there is nothing to key an on-resume recovery on.
	// Attachment is explicit-only (the user calls the `attach` tool); the stable
	// session id is registered lazily at that first attach — see toolAttach.

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
	log.Printf("adapter: started pid=%d cli=desktop", os.Getpid())
	return path, nil
}

// desktopCWD resolves the cwd sent in hello and used for the picker seed. Claude
// Desktop does not hand its MCP servers a project cwd, so this is best-effort
// (often the app's working directory) and MUST NOT be relied on for a mapping.
// An explicit C3_DESKTOP_CWD override wins (used for testing / power users).
func desktopCWD() string {
	if cwd := strings.TrimSpace(os.Getenv("C3_DESKTOP_CWD")); cwd != "" {
		return cwd
	}
	cwd, _ := os.Getwd()
	return cwd
}

// sessionID is the stable id used for attach/claim continuity. Claude Desktop
// gives its MCP servers no per-conversation id, so we let the user pin one via
// C3_DESKTOP_SESSION (stable across restarts → the same claim can be reclaimed on
// an explicit re-attach). Unset → a per-process id, which is distinct every launch
// (no cross-process continuity, by design).
func sessionID() string {
	if sid := strings.TrimSpace(os.Getenv("C3_DESKTOP_SESSION")); sid != "" {
		return sid
	}
	return fmt.Sprintf("desktop-%d", os.Getpid())
}

type adapter struct {
	transport *logNotifyTransport

	bmu  sync.Mutex
	conn *ipc.Conn

	pmu     sync.Mutex
	pending map[string]chan ipc.ToolResultMsg
	nextID  atomic.Uint64

	fqmu      sync.Mutex
	fqPending map[string]chan ipc.FetchQueueResp
	obmu      sync.Mutex
	obPending map[string]chan ipc.ObserveResp
	rtmu      sync.Mutex
	rtPending map[string]chan ipc.RetranscribeResp

	rsmu      sync.Mutex
	rsPending chan ipc.RecoverSessionResp

	recoverFired atomic.Bool
	runCtx       context.Context

	helloAck ipc.HelloAckMsg

	amu           sync.Mutex
	lastAttach    *ipc.AttachReq
	attachedTopic string

	brokerDownAdvised atomic.Bool
	dispatched        atomic.Bool
}

func newAdapter() *adapter {
	return &adapter{
		pending:   map[string]chan ipc.ToolResultMsg{},
		fqPending: map[string]chan ipc.FetchQueueResp{},
		obPending: map[string]chan ipc.ObserveResp{},
		rtPending: map[string]chan ipc.RetranscribeResp{},
	}
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

func spawnBroker() error {
	return spawn.Detached(exec.Command("c3-broker"))
}

func (a *adapter) hello() error {
	if err := a.conn.WriteJSON(ipc.HelloMsg{
		Op: ipc.OpHello, CLI: "desktop", PID: os.Getpid(), CWD: desktopCWD(),
		Capabilities:         []string{"fetch_queue"},
		CannotRenderChannels: true,
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
	if w := ipc.AdapterProtocolWarning("desktop", ack.ProtocolVersion); w != "" {
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
		case ipc.OpObserveResult:
			a.dispatchObserveResult(raw)
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
					// Pull-only host: the broker still pushes synthesized channel
					// EVENTS (poll_result/reaction/callback) live to a claimed
					// holder, and events are never queued — so a dropped push is
					// gone. Log it so the drop is visible in debugging.
					log.Printf("desktop: dropped non-system inbound push (kind=%q message_id=%d) — this pull-only host cannot render channel-event pushes", msg.Inbound.Kind, msg.Inbound.MessageID)
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
			log.Printf("desktop: ignoring unknown op %q from broker (this adapter speaks protocol v%d — the broker may be a newer c3 build; restart this CLI to match)", op, ipc.ProtocolVersion)
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

// fireRecover registers the stable session id with the broker (the only IPC lever
// that binds a stub → stable id) and, if that session had a prior route, re-claims
// it. Called ONCE, lazily, from the first explicit attach — there is no startup or
// on-resume auto-fire, because Claude Desktop offers no session-start hook or
// conversation id to key one on. With C3_DESKTOP_SESSION unset the id is
// per-process, so recovery is always a no-op (nothing prior to reclaim) and this
// simply binds the claim to the id for the life of the process.
func (a *adapter) fireRecover(ctx context.Context, stableID, cwd string) {
	if stableID == "" {
		return
	}
	if !a.recoverFired.CompareAndSwap(false, true) {
		return
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
		return
	}
	req := ipc.RecoverSessionReq{Op: ipc.OpRecoverSession, StableSessionID: stableID, CWD: cwd}
	if err := conn.WriteJSON(req); err != nil {
		log.Printf("recover session write failed: %v", err)
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(8 * time.Second):
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
		log.Printf("recover-session: re-attached to %q (queued=%d)", resp.Name, resp.QueuedCount)
		if text := renderDesktopRecoverNotice(resp); text != "" {
			a.emitRecoverNotice(text)
		}
	}
}

func renderDesktopRecoverNotice(resp ipc.RecoverSessionResp) string {
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
		fmt.Fprintf(&sb, "C3: re-attached to %q (session continuity). ~%d %s held — call fetch_queue (limit:\"all\") to drain:",
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
	return fmt.Sprintf("C3: re-attached to %q (session continuity). Inbound Telegram messages are held in C3's durable queue; call `fetch_queue` to read them.", name)
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

func (a *adapter) dispatchObserveResult(raw []byte) {
	var resp ipc.ObserveResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		return
	}
	a.obmu.Lock()
	ch, ok := a.obPending[resp.ID]
	delete(a.obPending, resp.ID)
	a.obmu.Unlock()
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
	// Advertise the MCP Apps UI extension (SEP-1724 extensions mechanism) so hosts
	// that gate on server-advertised capabilities know we serve
	// text/html;profile=mcp-app views. The host declares the same key on its side
	// (apps.mdx:1500-1518); this is the symmetric server declaration. It surfaces
	// in the initialize response because s.capabilities() clones Extensions
	// (go-sdk mcp/server.go:565).
	opts.Capabilities.AddExtension(uiExtensionID, map[string]any{
		"mimeTypes": []string{uiResourceMIME},
	})
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
	a.registerInboxApp(srv)
	return srv
}

// registerPrompts declares C3's MCP prompts. Claude Desktop surfaces MCP prompts
// as slash commands, so `fetch-queue` gives the user a one-keystroke `/fetch-queue`
// that pulls the durable queue in a single deterministic step — no "please check my
// messages" sentence and no tool-call reasoning turn to trigger the fetch. The
// returned message is injected by the client for the model to read/act on.
// Registering a prompt auto-advertises the server `prompts` capability. The name is
// kebab-case (Claude's slash-command convention, e.g. /add-dir) and deliberately
// distinct from the underscore `fetch_queue` TOOL so the two don't collide.
func (a *adapter) registerPrompts(srv *mcp.Server) {
	srv.AddPrompt(&mcp.Prompt{
		Name:        "fetch-queue",
		Title:       "Fetch C3 queue",
		Description: "Pull inbound Telegram messages held in C3's durable queue for the attached topic and drop them straight into the chat — a one-keystroke alternative to asking Claude to call fetch_queue. Drains everything by default; pass limit=N for the N oldest, or ack=false to peek without consuming.",
		Arguments: []*mcp.PromptArgument{
			{Name: "limit", Description: "How many oldest messages to pull: a number, or \"all\" (default)."},
			{Name: "ack", Description: "\"false\" to peek without consuming (leaves them queued). Default \"true\" — drain."},
		},
	}, a.promptFetchQueue)
}

// promptFetchQueue backs the `/fetch-queue` slash command. It defaults to draining
// the whole queue (a slash command is an explicit "show me what's waiting"); `limit`
// narrows it and `ack=false` peeks. The queued messages are returned as a user-role
// prompt message so the client injects them for the model to read and act on.
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
		text += "\n\nRead these and respond or act as needed — use the `reply` tool to answer on Telegram."
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
		head = "C3 is not yet configured. Run `c3-broker setup` from a shell to provide your Telegram bot token, DM chat id, and at least one group chat id, then restart Claude Desktop."
	default:
		// Claude Desktop is poll-only (no per-conversation id, no session-start
		// hook, cannot render unsolicited pushes), so the NoMapping and mapped
		// cases collapse to one honest instruction: attach explicitly, then poll.
		head = "C3 connected. Claude Desktop is POLL-ONLY — it cannot render unsolicited Telegram pushes, so nothing arrives on its own. Attach to a topic explicitly with `attach(name=\"<topic>\")` (or `attach(target=\"dm\")` for your DM); `attach` with no argument returns a picker to surface for the user. Inbound Telegram messages are held in C3's durable queue — call `fetch_queue` to read new/held messages (Desktop will not pop them for you), or the user can run the `/fetch-queue` slash command to drop the queue straight into the chat without asking. Send replies and reactions with the `reply`/`react` tools when the user asks."
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
	cwd := desktopCWD()
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
	// Always clear the pending entry on return (incl. the ctx.Done() path), so a
	// canceled attach can't leave a stale channel that a later same-key attach's
	// broker response is misdelivered to. Matches toolForward/toolFetchQueue.
	defer func() {
		a.pmu.Lock()
		delete(a.pending, "attached")
		a.pmu.Unlock()
	}()

	conn := a.currentConn()
	if conn == nil {
		return toolErrorResult("broker reconnecting — retry attach in a moment"), nil
	}

	// Register the stable session id before attach so the broker binds this
	// session → route for attach/claim continuity. sessionID() is always
	// non-empty; fireRecover fires at most once per process.
	a.fireRecover(ctx, sessionID(), cwd)

	if err := conn.WriteJSON(attachReq); err != nil {
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
	defer func() {
		a.pmu.Lock()
		delete(a.pending, "topics_list")
		a.pmu.Unlock()
	}()
	conn := a.currentConn()
	if conn == nil {
		return toolErrorResult("broker reconnecting — retry topics in a moment"), nil
	}
	if err := conn.WriteJSON(ipc.ListTopicsReq{Op: ipc.OpListTopics}); err != nil {
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
// text. Shared by the `fetch_queue` TOOL (an LLM-driven call) and the `fetch-queue`
// PROMPT (a user-driven slash command) so both consume the queue identically.
// isErr distinguishes a broker/transport failure (surface as an error) from a
// successful fetch (which may legitimately be "queue is empty").
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
		return "broker reconnecting — retry in a moment", true
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
		// Disambiguate "attached, nothing queued" from "not attached to
		// anything": the broker returns an empty queue for BOTH, so a bare
		// "queue is empty" misleads. When this session holds no topic, say so
		// and point at attach.
		if len(resp.Messages) == 0 && a.currentTopicName() == "" {
			return "⟦c3 not-attached⟧ This session isn't attached to any topic, so there's nowhere to look — an empty result here means \"not attached\", not \"no new mail\". Call `attach` (no args) to pick a topic this session used before.", false
		}
		return renderFetchedMessages(resp.Messages, resp.Remaining, a.currentTopicName()), false
	}
}

// toolObserve peeks a topic's inbox READ-ONLY without claiming it — the panel's
// "Watch" primitive and an honest LLM peek ("what's waiting on topic X, and who
// owns it?"). It never consumes and never steals. The rendered text carries a
// machine-readable ⟦c3 …⟧ status line (owner=you|other|none / status=…) the
// panel parses to pick the right affordances (Take over vs Hand/Auto); the line
// is also human-readable if it ever surfaces to the model.
func (a *adapter) toolObserve(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args, err := decodeArgs(req.Params.Arguments)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	ob := ipc.ObserveReq{Op: ipc.OpObserve, ID: strconv.FormatUint(a.nextID.Add(1), 10)}
	if v, ok := args["name"].(string); ok {
		ob.Name = v
	}
	if v, ok := args["target"].(string); ok {
		ob.Target = v
	}
	if v, ok := args["group"].(string); ok {
		ob.Group = v
	}
	if v, ok := args["topic_id"].(float64); ok {
		id := int64(v)
		ob.TopicID = &id
	}
	// Limit: absent → 0 (the broker shows the whole inbox — a display peek wants
	// everything). "all" is explicit; a number windows it.
	if v, ok := args["limit"]; ok {
		if s, ok := v.(string); ok && s == "all" {
			ob.All = true
		} else if f, ok := v.(float64); ok {
			ob.Limit = int(f)
		}
	}

	text, isErr := a.doObserve(ctx, ob)
	if isErr {
		return toolErrorResult(text), nil
	}
	return toolTextResult(text), nil
}

// doObserve runs one observe broker round-trip and renders the result. isErr is
// true only for a transport/broker failure (not for a resolved-but-not_found
// topic, which renders as normal content the panel acts on).
func (a *adapter) doObserve(ctx context.Context, ob ipc.ObserveReq) (text string, isErr bool) {
	ch := make(chan ipc.ObserveResp, 1)
	a.obmu.Lock()
	a.obPending[ob.ID] = ch
	a.obmu.Unlock()
	defer func() { a.obmu.Lock(); delete(a.obPending, ob.ID); a.obmu.Unlock() }()

	conn := a.currentConn()
	if conn == nil {
		return "broker reconnecting — retry in a moment", true
	}
	if err := conn.WriteJSON(ob); err != nil {
		return "broker write: " + err.Error(), true
	}
	select {
	case <-ctx.Done():
		return "canceled", true
	case <-time.After(120 * time.Second):
		return "observe timeout", true
	case resp := <-ch:
		return renderObserve(&resp), false
	}
}

// renderObserve builds the panel/LLM text: a machine-readable ⟦c3 …⟧ status line
// (parsed by the panel to choose Watch vs Take-over vs Hand/Auto), a blank line,
// then either the queued messages (SAME clean render as fetch_queue, via
// renderFetchedMessages) or a human note for the non-ok resolution states.
func renderObserve(resp *ipc.ObserveResp) string {
	name := resp.Name
	switch resp.Status {
	case "not_found":
		return "⟦c3 status=not_found name=" + name + "⟧\n\nNo topic named «" + name + "» yet. Take over to create it."
	case "ambiguous":
		return "⟦c3 status=ambiguous name=" + name + "⟧\n\nMore than one topic is named «" + name + "» — open the one you want by its full name."
	case "dm_unconfigured":
		return "⟦c3 status=dm_unconfigured⟧\n\nThe DM route isn't configured (run c3-broker setup)."
	case "ok":
		// fallthrough to the owner sentinel below
	default: // no_channel / anything else
		return "⟦c3 status=" + resp.Status + " name=" + name + "⟧\n\nCan't watch that topic here."
	}
	// OK: build the owner sentinel. Carry topic_id + group so the panel's take-over
	// can claim BY ID — a plain name-attach only silently claims default-group
	// names (a non-default-group name returns a use_existing_other_group proposal
	// and never claims), so id-based take-over is what makes take-over work for
	// EVERY watchable topic, not just default-group ones.
	var sb strings.Builder
	sb.WriteString("⟦c3")
	switch {
	case resp.HeldByYou:
		sb.WriteString(" owner=you")
	case resp.Holder != nil:
		fmt.Fprintf(&sb, " owner=other cli=%s pid=%d", sanitizeToken(resp.Holder.CLI), resp.Holder.PID)
	default:
		sb.WriteString(" owner=none")
	}
	if resp.TopicID != nil {
		fmt.Fprintf(&sb, " topic_id=%d", *resp.TopicID)
	}
	if resp.Group != "" {
		sb.WriteString(" group=" + sanitizeToken(resp.Group))
	}
	sb.WriteString(" name=" + name + "⟧")
	body := renderFetchedMessages(resp.Messages, resp.Remaining, name)
	if resp.Err != "" {
		body = "(couldn't read the queue: " + resp.Err + ")"
	}
	return sb.String() + "\n\n" + body
}

// sanitizeToken strips whitespace and the sentinel brackets from a value going
// into a single-token ⟦c3 …⟧ field (cli, group), so the panel's space-delimited
// key=value parse stays unambiguous and a stray newline/bracket can't split or
// truncate the status line. The trailing name= field may contain spaces (it is
// read to end-of-line), so it is not sanitized here.
func sanitizeToken(s string) string {
	return strings.NewReplacer(" ", "_", "\t", "_", "\n", "_", "\r", "_", "⟦", "", "⟧", "").Replace(s)
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
				Description: "Attach this session to a Telegram topic. Empty = show a picker of suggested topics for the user to choose (never guess). `target='dm'` for DM. `name='X'` for a topic name. `topic_id=N` to claim a known thread. `create=true` to confirm creation. `steal=true` only after user-confirmed force_steal.",
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
				// App-callable so the C3 Inbox panel can attach in-panel via the
				// host bridge (a host MUST reject tools/call from an app for tools
				// whose visibility omits "app" — apps.mdx:399-401). No resourceUri:
				// attach renders as text, it is not itself a panel.
				Meta: mcp.Meta{"ui": map[string]any{"visibility": []string{"model", "app"}}},
			},
			handler: a.toolAttach,
		},
		{
			tool: &mcp.Tool{
				Name:        "topics",
				Description: "List known Telegram topics + claim state.",
				InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
				// App-callable groundwork: the panel marks topics visible now so a
				// later increment can list topics in-panel. `topics` returns formatted
				// text (not structured data), so this version renders no topic buttons.
				Meta: mcp.Meta{"ui": map[string]any{"visibility": []string{"model", "app"}}},
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
				// Make fetch_queue callable by the MCP App panel. A host MUST reject
				// tools/call from an app for tools whose visibility omits "app"
				// (apps.mdx:399-401). The default is ["model","app"] (apps.mdx:397),
				// but we set it explicitly to de-risk hosts that don't apply the
				// default. No resourceUri here — fetch_queue renders as text, not a
				// panel; only open_inbox carries a resourceUri.
				Meta: mcp.Meta{"ui": map[string]any{"visibility": []string{"model", "app"}}},
			},
			handler: a.toolFetchQueue,
		},
		{
			tool: &mcp.Tool{
				Name:        "observe",
				Description: "Peek a topic's live inbox READ-ONLY, WITHOUT claiming it (never steals). Returns the messages waiting in C3's durable queue plus who currently owns the topic. Use this to WATCH a topic another session (e.g. a Claude Code CLI in Telegram mode) is holding — it keeps its claim and keeps replying while you see the same inbox. To actually receive/reply/consume, use `attach` to take the topic over. Select the topic with `name`, `target=\"dm\"`, or `topic_id`.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"name":     map[string]any{"type": "string"},
						"target":   map[string]any{"type": "string"},
						"topic_id": map[string]any{"type": "integer"},
						"group":    map[string]any{"type": "string"},
						"limit":    map[string]any{"description": "integer window, or the string \"all\"; absent shows everything waiting"},
					},
				},
				// App-callable so the C3 Inbox panel can peek in-panel without owning
				// the topic (a host MUST reject tools/call from an app for tools whose
				// visibility omits "app" — apps.mdx:399-401). No resourceUri: observe
				// renders as text, it is not itself a panel.
				Meta: mcp.Meta{"ui": map[string]any{"visibility": []string{"model", "app"}}},
			},
			handler: a.toolObserve,
		},
		{
			tool: &mcp.Tool{
				Name:        "open_inbox",
				Description: "Open the C3 Inbox — an inline panel that renders the inbound Telegram messages held in C3's durable queue for the attached topic. The panel PEEKS the queue (does not consume); use fetch_queue to actually drain it. Requires an attached topic.",
				InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
				// Link this tool to its UI resource so a host that supports MCP Apps
				// fetches and renders the panel when the tool is called (apps.mdx:363,
				// 388). Nested `ui.resourceUri` is the current form (src/app-bridge.ts:
				// 125-133); the flat `ui/resourceUri` is the deprecated fallback some
				// hosts still read (examples/qr-server/server.py:104). visibility
				// ["model","app"] keeps it callable by both the model and the app.
				Meta: mcp.Meta{
					"ui": map[string]any{
						"resourceUri": uiInboxURI,
						"visibility":  []string{"model", "app"},
					},
					"ui/resourceUri": uiInboxURI,
				},
			},
			handler: a.toolOpenInbox,
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
				// App-callable so the C3 Inbox panel's reply composer can send in-panel
				// via the host bridge (a host MUST reject tools/call from an app for
				// tools whose visibility omits "app" — apps.mdx:399-401). The default is
				// ["model","app"] (apps.mdx:397), but we set it explicitly to de-risk
				// hosts that don't apply the default. Outbound reply is broker-forwarded
				// and NOT subject to the pull-only inbound limitation, so the panel calls
				// it directly. No resourceUri: reply renders as text, not a panel.
				Meta: mcp.Meta{"ui": map[string]any{"visibility": []string{"model", "app"}}},
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

func parseFetchLimit(val any) (int, bool) {
	if s, ok := val.(string); ok && s == "all" {
		return 0, true
	}
	if f, ok := val.(float64); ok {
		return int(f), false
	}
	return 3, false
}

// parseFetchLimitStr parses the `limit` argument of the fetch-queue PROMPT, whose
// arguments arrive as strings (MCP prompt args are always strings). Empty or
// "all" ⇒ drain everything (the slash-command default); a positive integer ⇒ that
// many oldest; anything else falls back to draining all.
func parseFetchLimitStr(s string) (limit int, all bool) {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "all") {
		return 0, true
	}
	if n, err := strconv.Atoi(s); err == nil && n > 0 {
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
	if remaining > 0 {
		out += "\n\n" + pendingNudge(remaining, route)
	}
	return out
}

// renderQueuedInbound renders one queued message as a human-facing block for the
// Desktop chat surface. The maintainer (2026-07-17): the old one-line
// `from=@u message_id=N text=%q reply_to=N` render buried the message in metadata
// and read as clutter when handed into the composer. New shape: the message TEXT
// stands alone and prominent, followed by a blank line and a compact
// "↳ from … · reply to … · msg N" trailer. The trailer keeps the attribution and
// ids (Claude still needs message_id/sender for reply/react targeting; the reader
// still sees who sent it) out of the way of the text. Attachments keep their FULL
// metadata (file_id/mime are load-bearing for download_attachment/retranscribe)
// on their own indented line beneath the trailer. This render feeds three places:
// the panel inbox display, the /fetch-queue slash dump, and the Hand-to-Claude
// injection — all human-facing on Desktop, so all benefit from the clean shape.
func renderQueuedInbound(in *c3types.Inbound) string {
	var meta []string
	switch {
	case in.Sender.Username != "":
		meta = append(meta, "from @"+in.Sender.Username)
	case in.Sender.UserID != 0:
		meta = append(meta, fmt.Sprintf("from uid %d", in.Sender.UserID))
	}
	if in.ReplyTo != nil {
		switch {
		case in.ReplyTo.User.Username != "":
			meta = append(meta, "reply to @"+in.ReplyTo.User.Username)
		case in.ReplyTo.User.UserID != 0:
			meta = append(meta, fmt.Sprintf("reply to uid %d", in.ReplyTo.User.UserID))
		default:
			meta = append(meta, fmt.Sprintf("reply to msg %d", in.ReplyTo.MessageID))
		}
	}
	if in.MessageID != 0 {
		meta = append(meta, fmt.Sprintf("msg %d", in.MessageID))
	}
	if in.IsEvent() {
		meta = append(meta, "event="+string(in.Kind))
	}

	body := in.Text
	if body == "" && in.IsEvent() {
		body = fmt.Sprintf("(%s event)", in.Kind)
	}

	var b strings.Builder
	if body != "" {
		b.WriteString(body)
		b.WriteString("\n\n")
	}
	b.WriteString("↳ ")
	if len(meta) == 0 {
		b.WriteString("from unknown")
	} else {
		b.WriteString(strings.Join(meta, " · "))
	}
	for _, att := range in.Attachments {
		b.WriteString("\n   ")
		b.WriteString(c3types.AttachmentField(att))
	}
	return b.String()
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

func mapResult(m map[string]any) *mcp.CallToolResult {
	content := []mcp.Content{}
	if text, ok := m["text"].(string); ok {
		content = append(content, &mcp.TextContent{Text: text})
	} else {
		data, _ := json.Marshal(m)
		content = append(content, &mcp.TextContent{Text: string(data)})
	}
	return &mcp.CallToolResult{Content: content}
}

// --- MCP Apps: minimal "C3 Inbox" inline panel -----------------------------
//
// SEP-1865 (MCP Apps, spec 2026-01-26 in modelcontextprotocol/ext-apps). The
// open_inbox tool carries _meta.ui.resourceUri pointing at the ui:// resource
// below; a host that supports MCP Apps fetches inboxHTML and renders it in a
// sandboxed iframe when the tool is called. The panel handshakes with the host
// (ui/initialize -> ui/notifications/initialized, per src/app.ts:1959-1982) and
// then calls the existing fetch_queue tool through the host bridge to render the
// durable queue inline. Deliberately minimal — a render probe for the
// stdio+iframe MCP-App shape (de-risks claude-ai-mcp#165), not a full inbox.

// registerInboxApp declares the ui:// HTML resource that backs open_inbox.
// Registering a resource auto-advertises the server `resources` capability
// (go-sdk mcp/server.go:588), so no ServerOptions change is needed for it.
func (a *adapter) registerInboxApp(srv *mcp.Server) {
	srv.AddResource(&mcp.Resource{
		URI:         uiInboxURI,
		Name:        "C3 Inbox",
		Description: "Inline panel that renders C3's durable queue for the attached topic.",
		MIMEType:    uiResourceMIME,
	}, a.resourceInbox)
}

// resourceInbox serves the self-contained inbox HTML. mimeType MUST be
// text/html;profile=mcp-app for the host to treat it as an MCP App view
// (apps.mdx:268, src/app.ts:158). The HTML is fully inline (no external network)
// so it renders under the host's restrictive default CSP (apps.mdx:275-284) —
// hence no _meta.ui.csp is declared.
func (a *adapter) resourceInbox(_ context.Context, _ *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{
			URI:      uiInboxURI,
			MIMEType: uiResourceMIME,
			Text:     inboxHTML,
		}},
	}, nil
}

// toolOpenInbox is the tool the host calls to open the panel; the panel itself
// does the real work. This text is the graceful-degradation fallback for hosts
// that do not render MCP Apps — a UI-linked tool MUST still return meaningful
// content (apps.mdx:1556-1559).
func (a *adapter) toolOpenInbox(_ context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return toolTextResult("Opening the C3 inbox panel — type a topic and Watch to see its inbox READ-ONLY (a no-claim peek that never steals it from a Claude Code session). Take over only if you want Desktop to receive/reply; use fetch_queue to drain."), nil
}

// inboxHTML is the self-contained MCP App view. All CSS+JS are inline (no
// external network) so it satisfies the host's default CSP. The JS speaks the
// raw postMessage JSON-RPC dialect directly (no SDK) so nothing is fetched:
//   - ui/initialize request -> await result -> ui/notifications/initialized
//     notification (src/app.ts:1959-1982); protocolVersion 2026-01-26
//     (src/spec.types.ts:29).
//   - tools/call { name:"observe", arguments:{name} } to WATCH — a READ-ONLY peek
//     that resolves the topic and returns its queue + current holder WITHOUT
//     claiming it. The result's leading ⟦c3 owner=…⟧ sentinel is parsed
//     (parseObserve) to pick affordances; this is the default 5s refresh, so
//     merely having the panel open never steals a topic from a Claude Code
//     session (apps.mdx:495, 1483; App.callServerTool src/app.ts:1246).
//   - tools/call { name:"attach", arguments:{name[,steal|create]} } ONLY on an
//     explicit "Take over" — the sole action that claims/steals; the mode follows
//     the observed owner state (apps.mdx:399-401 gates app-callable tools).
//   - tools/call { name:"fetch_queue", arguments:{ack:false→peek, ack:true→drain} }
//     for "Hand to Claude" / Auto — owner-only (a non-owner can't consume), so
//     these appear only once Desktop owns the topic.
//   - ui/message { role:"user", content:[{type:"text", text}] } to hand the
//     drained messages to Claude and start a turn (apps.mdx:998-1034; the host
//     MAY prompt for consent). content is a ContentBlock ARRAY per SEP-1865
//     (spec.types.ts McpUiMessageRequest) — Claude Desktop rejects the
//     single-object shape as invalid params, and its result may carry isError
//     (McpUiMessageResult), which MUST be treated as not-delivered.
//   - ui/notifications/size-changed so a flexible-height host sizes the iframe to
//     the content (apps.mdx:718; src/app.ts:1859-1907).
//
// The panel WATCHES read-only by default and consumes/hands only when it owns the
// topic — it still exposes NO reply/send affordance; composing replies stays the
// model's job.
//
// NOTE: this string is a Go raw literal (backticks) — it must contain no
// backtick characters, so the JS uses string concatenation, not template
// literals.
const inboxHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="color-scheme" content="light dark">
<title>C3 Inbox</title>
<style>
  :root {
    color-scheme: light dark;
    --c3-bg: var(--color-background-primary, light-dark(#ffffff, #1a1a1a));
    --c3-fg: var(--color-text-primary, light-dark(#1a1a1a, #f4f4f4));
    --c3-muted: var(--color-text-secondary, light-dark(#5c5c5c, #a6a6a6));
    --c3-border: var(--color-border-primary, light-dark(#e4e4e4, #333333));
    --c3-ok: var(--color-text-success, light-dark(#0f7a34, #4ade80));
    --c3-card: var(--color-background-secondary, light-dark(#f7f7f7, #232323));
    --c3-radius: var(--border-radius-md, 10px);
    --c3-font: var(--font-sans, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif);
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; background: var(--c3-bg); color: var(--c3-fg); font-family: var(--c3-font); }
  body { padding: 16px; -webkit-font-smoothing: antialiased; }
  .banner { display: flex; align-items: center; gap: 8px; font-size: 18px; font-weight: 700; color: var(--c3-ok); }
  .sub { color: var(--c3-muted); font-size: 12px; margin: 4px 0 14px; }
  .bar { display: flex; align-items: center; gap: 10px; margin-bottom: 14px; flex-wrap: wrap; }
  button { font: inherit; font-size: 13px; font-weight: 600; padding: 7px 14px; border-radius: var(--c3-radius); border: 1px solid var(--c3-border); background: var(--c3-card); color: var(--c3-fg); cursor: pointer; }
  button:disabled { opacity: 0.5; cursor: default; }
  .status { color: var(--c3-muted); font-size: 12px; }
  .msg { border: 1px solid var(--c3-border); border-radius: var(--c3-radius); background: var(--c3-card); padding: 12px 14px; margin-bottom: 10px; }
  pre.raw { white-space: pre-wrap; overflow-wrap: anywhere; word-break: break-word; font: inherit; font-size: 13px; line-height: 1.5; margin: 0; }
  .empty { color: var(--c3-muted); font-size: 13px; padding: 6px 0; }
  [hidden] { display: none !important; }
  .attachbar { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }
  input.topic { font: inherit; font-size: 13px; padding: 7px 10px; border-radius: var(--c3-radius); border: 1px solid var(--c3-border); background: var(--c3-bg); color: var(--c3-fg); flex: 1 1 160px; min-width: 120px; }
  .hint { color: var(--c3-muted); font-size: 12px; margin: -2px 0 12px; white-space: pre-wrap; overflow-wrap: anywhere; word-break: break-word; }
  .note { color: var(--c3-muted); font-size: 12px; margin: -2px 0 12px; font-style: italic; }
  label.auto { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; color: var(--c3-fg); cursor: pointer; user-select: none; }
  /* Bounded scroll container: without a cap the panel reports full max-content
     height and the host grows the iframe unbounded, so the newest message can
     never come back into view. max-height + overflow-y:auto make #list a
     self-scrolling chat pane (see reportSize + the stickToBottom tail). */
  #list { max-height: 46vh; overflow-y: auto; overflow-x: hidden; }
  .composer { display: flex; align-items: flex-end; gap: 8px; margin-top: 12px; }
  textarea.replytext { font: inherit; font-size: 13px; line-height: 1.4; padding: 8px 10px; border-radius: var(--c3-radius); border: 1px solid var(--c3-border); background: var(--c3-bg); color: var(--c3-fg); flex: 1 1 auto; min-width: 120px; min-height: 42px; max-height: 30vh; resize: vertical; }
</style>
</head>
<body>
  <div class="banner"><span>&#9989; C3 Inbox connected</span></div>
  <div class="sub" id="host">Initializing&hellip;</div>
  <div class="attachbar" id="attachbar">
    <input type="text" class="topic" id="topic" placeholder="topic to watch" autocomplete="off" spellcheck="false">
    <button id="watch">Watch</button>
    <button id="takeover" hidden>Take over here</button>
  </div>
  <div class="hint" id="attachhint" hidden></div>
  <div class="note" id="holderline" hidden></div>
  <div class="bar">
    <button id="refresh" disabled>Refresh</button>
    <button id="hand" hidden>Hand to Claude</button>
    <label class="auto" id="autolabel" hidden><input type="checkbox" id="auto"> Auto-forward</label>
    <button id="popout" hidden>Pop out</button>
    <span class="status" id="status">Connecting to host&hellip;</span>
  </div>
  <div class="note" id="composernote" hidden>Handed messages land in the composer &mdash; press <b>Enter</b> to send. On the Code tab that&rsquo;s a review step; the host drafts, it can&rsquo;t auto-send.</div>
  <div class="note" id="autonote" hidden>Auto-forward: each new Telegram message is fed into the composer as you clear the last &mdash; works only while this panel is open.</div>
  <div id="list"><div class="empty">Enter a topic name above and click <b>Watch</b> to see its inbox (read-only &mdash; no stealing).</div></div>
  <div class="composer" id="composer" hidden>
    <textarea id="replytext" class="replytext" rows="2" placeholder="Reply to Telegram&hellip; (Ctrl/Cmd+Enter to send)" autocomplete="off" spellcheck="false"></textarea>
    <button id="send">Send</button>
  </div>
  <div class="note" id="replynote" hidden></div>
<script>
(function () {
  "use strict";
  var PROTOCOL_VERSION = "2026-01-26";
  var parentWin = window.parent;
  var nextId = 1;
  var pending = {};
  var connected = false;
  var lastW = -1, lastH = -1;
  var refreshTimer = null;
  var inFlight = false;
  var inFlightP = null;
  var watchName = "";        // the topic being watched (peeked read-only)
  var ownerState = "";       // "you" | "other" | "none" | "not_found" | "unavailable"
  var holderLabel = "";      // e.g. "claude (pid 1234)" for the read-only note
  var takeoverTopicId = "";  // resolved topic id → take over BY ID (default & non-default groups)
  var takeoverGroup = "";    // resolved group → threaded into the id-based take-over
  var owned = false;         // ownerState === "you": this Desktop session holds the claim
  var acting = false;        // a watch / take-over round-trip is in flight
  var handing = false;
  var REFRESH_MS = 5000;
  var HAND_PREFIX = "New C3 Telegram message(s):\n\n";
  var autoSkipCycles = 0;   // skip this many auto-poll cycles after a failed hand
  var autoFailStreak = 0;   // consecutive auto-hand failures; trips the breaker
  var AUTO_BACKOFF = 2;     // polls to skip after a failed auto-hand (~10s)
  var AUTO_FAIL_LIMIT = 5;  // disarm Auto-forward after this many consecutive failures
  var MAX_PANEL_H = 680;    // ceiling for the reported iframe height (px); past this
                            // the list scrolls internally instead of stretching the
                            // panel (fixes "scroll never comes down")
  var stickToBottom = true; // chat-tail: auto-scroll #list to the newest row unless
                            // the user has scrolled up to read
  var lastMsgId = 0;        // newest observed Telegram message id, for reply_to threading
  var sending = false;      // a reply-send round-trip is in flight

  var elHost = document.getElementById("host");
  var elStatus = document.getElementById("status");
  var elRefresh = document.getElementById("refresh");
  var elList = document.getElementById("list");
  var elAttachBar = document.getElementById("attachbar");
  var elTopic = document.getElementById("topic");
  var elWatch = document.getElementById("watch");
  var elTakeover = document.getElementById("takeover");
  var elHolderLine = document.getElementById("holderline");
  var elAttachHint = document.getElementById("attachhint");
  var elHand = document.getElementById("hand");
  var elAutoLabel = document.getElementById("autolabel");
  var elAuto = document.getElementById("auto");
  var elAutoNote = document.getElementById("autonote");
  var elComposerNote = document.getElementById("composernote");
  var elPopout = document.getElementById("popout");
  var elComposer = document.getElementById("composer");
  var elReplyText = document.getElementById("replytext");
  var elSend = document.getElementById("send");
  var elReplyNote = document.getElementById("replynote");

  function send(obj) { parentWin.postMessage(obj, "*"); }

  function request(method, params) {
    var id = nextId++;
    var msg = { jsonrpc: "2.0", id: id, method: method };
    if (params !== undefined) { msg.params = params; }
    return new Promise(function (resolve, reject) {
      pending[id] = { resolve: resolve, reject: reject };
      send(msg);
    });
  }

  function notify(method, params) {
    var msg = { jsonrpc: "2.0", method: method };
    if (params !== undefined) { msg.params = params; }
    send(msg);
  }

  function respond(id, result) { send({ jsonrpc: "2.0", id: id, result: result }); }

  // Measure true content height with the max-content trick (mirrors the SDK,
  // src/app.ts:1883-1886); width is the viewport width. Only emit when it
  // actually changed to avoid a resize feedback loop.
  function reportSize() {
    var html = document.documentElement;
    var prev = html.style.height;
    html.style.height = "max-content";
    var h = Math.ceil(html.getBoundingClientRect().height);
    html.style.height = prev;
    // Cap the reported height so a flexible-height host can't grow the iframe
    // without bound. #list is its own bounded scroll container (max-height +
    // overflow-y:auto), so past this ceiling new messages scroll INSIDE the list
    // instead of stretching the panel — which is what lets the tail return to the
    // newest message (fixes "scroll never comes down").
    if (h > MAX_PANEL_H) { h = MAX_PANEL_H; }
    var w = Math.ceil(window.innerWidth);
    if (w === lastW && h === lastH) { return; }
    lastW = w; lastH = h;
    notify("ui/notifications/size-changed", { width: w, height: h });
  }

  function applyTheme(ctx) {
    if (!ctx) { return; }
    if (ctx.theme) { document.documentElement.setAttribute("data-theme", ctx.theme); }
    var vars = ctx.styles && ctx.styles.variables;
    if (vars) {
      for (var k in vars) {
        if (Object.prototype.hasOwnProperty.call(vars, k) && vars[k] != null) {
          document.documentElement.style.setProperty(k, vars[k]);
        }
      }
    }
  }

  window.addEventListener("message", function (event) {
    var data = event.data;
    if (!data || data.jsonrpc !== "2.0") { return; }
    // Response to one of our requests (has id, no method).
    if (data.id !== undefined && data.method === undefined) {
      var p = pending[data.id];
      if (!p) { return; }
      delete pending[data.id];
      if (data.error) { p.reject(new Error((data.error && data.error.message) || "host error")); }
      else { p.resolve(data.result); }
      return;
    }
    // Request from the host to us (has id and method). Answer politely so the
    // host never hangs; ping and teardown both take an empty result.
    if (data.id !== undefined && data.method) {
      respond(data.id, {});
      return;
    }
    // Host notifications (no id) are ignored in this minimal probe.
  });

  function extractText(result) {
    if (!result || !result.content || !result.content.length) { return ""; }
    var parts = [];
    for (var i = 0; i < result.content.length; i++) {
      var c = result.content[i];
      if (c && c.type === "text" && typeof c.text === "string") { parts.push(c.text); }
    }
    return parts.join("\n\n");
  }

  function show(el, on) { if (el) { el.hidden = !on; } }

  // updateControls reflects ownerState into the affordances. The watch bar is
  // ALWAYS available (watch / switch topics freely). "Take over here" appears only
  // when the topic is watchable-but-not-owned; Hand to Claude + Auto (which
  // CONSUME) appear only when THIS session owns the claim. A holder line names who
  // owns it so read-only mode is never a mystery. Called on every render.
  function updateControls() {
    owned = ownerState === "you";
    var canTakeOver = ownerState === "other" || ownerState === "none" || ownerState === "not_found";
    show(elAttachBar, true);
    show(elTakeover, canTakeOver);
    if (canTakeOver && elTakeover) {
      elTakeover.textContent = ownerState === "not_found" ? "Create & take over"
        : ownerState === "other" ? "Take over here"
        : "Attach here";
    }
    show(elHand, owned);
    show(elAutoLabel, owned);
    show(elComposerNote, owned);
    show(elComposer, owned);   // reply composer is owner-only (a non-owner can't send)
    show(elAutoNote, owned && elAuto.checked);
    var hl = "";
    if (watchName) {
      if (owned) { hl = "👀 Watching «" + watchName + "» · you own it — Hand/Auto-forward enabled"; }
      else if (ownerState === "other") { hl = "👀 Watching «" + watchName + "» · held by " + (holderLabel || "another session") + " — read-only"; }
      else if (ownerState === "none") { hl = "👀 Watching «" + watchName + "» · unclaimed — take over to receive/reply"; }
      else if (ownerState === "not_found") { hl = "«" + watchName + "» doesn't exist yet — take over to create it"; }
    }
    if (hl) { elHolderLine.textContent = hl; elHolderLine.hidden = false; }
    else { elHolderLine.hidden = true; elHolderLine.textContent = ""; }
  }

  // shouldHand decides whether a fetched blob is real Telegram content worth
  // handing to Claude — not an error, not the empty-queue line, not the
  // not-attached sentinel.
  function shouldHand(text, isError) {
    if (isError) { return false; }
    var trimmed = (text || "").replace(/^\s+|\s+$/g, "");
    if (!trimmed) { return false; }
    var lower = trimmed.toLowerCase();
    if (lower === "c3 queue is empty") { return false; }
    if (lower.indexOf("no route claimed") !== -1 || lower.indexOf("before attach") !== -1) { return false; }
    return true;
  }

  var pendingTakeoverHint = "";

  // watchTopic starts watching the typed topic — a READ-ONLY observe. It never
  // claims, so it can never steal the topic from a Claude Code session that owns
  // it; that session keeps its claim and keeps replying while this panel shows the
  // same inbox.
  function watchTopic() {
    var name = (elTopic.value || "").replace(/^\s+|\s+$/g, "");
    if (!name) { return; }
    watchName = name;
    elAttachHint.hidden = true; elAttachHint.textContent = "";
    loadQueue();
  }

  // doTakeOver is the ONLY action that touches the exclusive claim: it makes THIS
  // Desktop session the owner so Hand/Auto/reply become possible. The attach mode
  // follows the observed ownerState — held-by-another → steal, unclaimed → plain
  // attach, not_found → create. A steal deliberately evicts the current holder
  // (e.g. a Claude Code session), so it is an explicit, one-tap decision, never
  // automatic. After it lands the next observe reports owner=you and the owner-only
  // affordances appear.
  function doTakeOver() {
    if (acting || !watchName) { return; }
    // Prefer claiming BY ID (unambiguous, and the only form that claims a
    // non-default-group topic — a plain name-attach there returns a proposal and
    // never claims). Fall back to name for a brand-new topic (create) or when no
    // id resolved (e.g. a DM route).
    var args = {};
    if (ownerState === "not_found") {
      args.name = watchName;
      args.create = true;
    } else if (takeoverTopicId) {
      args.topic_id = parseInt(takeoverTopicId, 10);
      if (takeoverGroup) { args.group = takeoverGroup; }
      if (ownerState === "other") { args.steal = true; }
    } else {
      args.name = watchName;
      if (ownerState === "other") { args.steal = true; }
    }
    acting = true;
    elTakeover.disabled = true;
    elAttachHint.hidden = true; elAttachHint.textContent = "";
    request("tools/call", { name: "attach", arguments: args }).then(function (result) {
      // Surface the attach text only if we STILL don't own it afterwards (a
      // create that needs confirmation, a re-collision, or a plain failure). The
      // double observe guarantees a fresh post-attach peek even if a timer peek
      // was mid-flight.
      pendingTakeoverHint = extractText(result);
      return loadQueue().then(function () { return loadQueue(); });
    }).catch(function (err) {
      pendingTakeoverHint = "Take-over failed: " + (err && err.message ? err.message : "error");
    }).then(function () {
      acting = false;
      elTakeover.disabled = false;
      if (!owned && pendingTakeoverHint) {
        elAttachHint.textContent = pendingTakeoverHint;
        elAttachHint.hidden = false;
      }
      pendingTakeoverHint = "";
      reportSize();
    });
  }

  // handToClaude hands the waiting messages to Claude (ui/message) and only THEN
  // consumes them. Ordering is deliberate and load-bearing: PEEK (ack:false, no
  // consume) → ui/message → DRAIN (ack:true) only after the host accepts. A failed
  // hand therefore never drops messages from the durable queue — they stay queued
  // and re-appear on the next peek (C3's whole contract is a no-loss queue).
  //
  // FAILURE HANDLING (revised 2026-07-17, after the maintainer's on-device tests + a
  // Desktop-internals probe): on the Code tab ui/message DRAFTS into the
  // composer rather than sending — an empty composer accepts the draft, a
  // non-empty one (a prior unsent draft) makes the host REJECT the call. The old
  // code treated that reject as a user "decline" and disarmed Auto, so Auto died
  // the moment a second message arrived before the user pressed Enter. It is NOT a
  // decline. So: a failed hand now KEEPS Auto armed, leaves the messages queued,
  // and backs off a couple of poll cycles (autoSkipCycles) so we don't hammer the
  // composer-busy reject every 5s. Once the user sends the drafted message the
  // composer clears and the next cycle drafts the next one — Auto becomes
  // "feed the queue into the composer one at a time as you clear it". A genuine
  // persistent failure (AUTO_FAIL_LIMIT consecutive) trips a circuit breaker that
  // finally disarms Auto with an explicit reason, so a truly broken host can't
  // silently retry forever. (There is no send-vs-draft param and no gesture-free
  // turn-start primitive — verified against the ext-apps spec + shipped app SDK.)
  function handToClaude(isAuto) {
    if (handing || !owned) { return; }
    handing = true;
    elHand.disabled = true;
    request("tools/call", { name: "fetch_queue", arguments: { ack: false } }).then(function (result) {
      var text = extractText(result);
      if (!shouldHand(text, result && result.isError)) {
        elStatus.textContent = "Nothing new to hand · " + new Date().toLocaleTimeString();
        return;
      }
      // content MUST be a ContentBlock ARRAY (SEP-1865 McpUiMessageRequest) —
      // the single-object shape is rejected by Claude Desktop as invalid params.
      return request("ui/message", { role: "user", content: [{ type: "text", text: HAND_PREFIX + text }] }).then(function (result) {
        // McpUiMessageResult.isError: host accepted the RPC but did not deliver
        // (e.g. composer already holds an unsent draft). Leave queued and, if this
        // was an auto-hand, back off — but do NOT disarm Auto.
        if (result && result.isError) { handFailed(isAuto, "host busy (a draft may be waiting — press Enter to send it)"); return; }
        autoFailStreak = 0;
        elStatus.textContent = "📤 handed to Claude — press Enter in the composer to send · " + new Date().toLocaleTimeString();
        // Accepted → consume so it won't re-hand or re-show. Best-effort.
        return request("tools/call", { name: "fetch_queue", arguments: { ack: true } }).then(function () {}, function () {});
      }, function (err) {
        // Rejected/failed → leave queued; back off Auto (don't disarm). The reject
        // reason is surfaced so a genuine problem isn't mute (2026-07-17).
        handFailed(isAuto, (err && err.message) ? err.message : "rejected");
      });
    }).catch(function (err) {
      elStatus.textContent = "Hand failed: " + (err && err.message ? err.message : "error");
    }).then(function () {
      handing = false;
      elHand.disabled = false;
      loadQueue();
      reportSize();
    });
  }

  // handFailed records a failed hand without dropping messages. Auto stays armed
  // (the common cause is a composer already holding an unsent draft, not a
  // decline) and simply backs off a few cycles; only a long streak of consecutive
  // failures trips the breaker and disarms Auto with an explicit reason.
  function handFailed(isAuto, reason) {
    if (isAuto) {
      autoFailStreak++;
      autoSkipCycles = AUTO_BACKOFF;
      if (autoFailStreak >= AUTO_FAIL_LIMIT && elAuto.checked) {
        elAuto.checked = false;
        autoFailStreak = 0;
        updateControls();
        elStatus.textContent = "Auto-forward off — handing kept failing (" + reason + "). Clear the composer and re-arm.";
        return;
      }
      elStatus.textContent = "Left in queue (" + reason + ") — Auto-forward will retry when the composer clears";
      return;
    }
    elStatus.textContent = "Not handed (" + reason + "). Press Enter to send any waiting draft, then try again.";
  }

  // autoMaybe auto-hands when Auto is armed and a peek surfaced real content.
  // Loop-avoidance: handing drains (ack:true), so the next peek is empty and
  // shouldHand is false; the single handing flag blocks re-entry while a hand
  // (incl. its ui/message consent round-trip) is outstanding.
  function autoMaybe(text, isError) {
    if (!elAuto.checked || !owned || handing) { return; }
    if (autoSkipCycles > 0) { autoSkipCycles--; return; }
    if (!shouldHand(text, isError)) { return; }
    handToClaude(true);
  }

  // parseObserve splits an observe result into its ⟦c3 …⟧ status line and the
  // message body. Parsed from the ORIGINAL-CASE text (never a lowercased copy —
  // topic names and cli values are case-sensitive). The status line is the FIRST
  // line; name= is TERMINAL (it may contain spaces, so it is split off first);
  // cli is space-free by construction (the adapter sanitizes it). Returns
  // { ownerState, holderLabel, body }.
  function parseObserve(text) {
    var out = { ownerState: "unavailable", holderLabel: "", topicId: "", group: "", body: text || "" };
    var s = text || "";
    var nl = s.indexOf("\n");
    var first = (nl >= 0 ? s.slice(0, nl) : s).replace(/^\s+|\s+$/g, "");
    if (first.indexOf("⟦c3") !== 0) { return out; } // no sentinel → leave defaults
    var end = first.indexOf("⟧");
    if (end < 0) { return out; }
    var inner = first.slice(3, end).replace(/^\s+|\s+$/g, "");
    out.body = (nl >= 0 ? s.slice(nl + 1) : "").replace(/^\s+/, "");
    var ni = inner.indexOf("name=");
    var head = ni >= 0 ? inner.slice(0, ni) : inner;
    var cli = "", pid = "";
    var toks = head.split(/\s+/);
    for (var i = 0; i < toks.length; i++) {
      var t = toks[i];
      if (t.indexOf("owner=") === 0) { out.ownerState = t.slice(6); }
      else if (t.indexOf("status=") === 0) { out.ownerState = t.slice(7) === "not_found" ? "not_found" : "unavailable"; }
      else if (t.indexOf("cli=") === 0) { cli = t.slice(4); }
      else if (t.indexOf("pid=") === 0) { pid = t.slice(4); }
      else if (t.indexOf("topic_id=") === 0) { out.topicId = t.slice(9); }
      else if (t.indexOf("group=") === 0) { out.group = t.slice(6); }
    }
    if (cli) { out.holderLabel = pid ? (cli + " (pid " + pid + ")") : cli; }
    return out;
  }

  // newestMsgId scans a rendered observe body for the largest "msg N" id (the
  // trailer renderQueuedInbound emits) so a panel reply can thread reply_to to the
  // newest inbound message. Best-effort — no id found → 0 (reply goes unthreaded).
  function newestMsgId(body) {
    var re = /\bmsg (\d+)\b/g, m, max = 0;
    while ((m = re.exec(body || "")) !== null) {
      var n = parseInt(m[1], 10);
      if (n > max) { max = n; }
    }
    return max;
  }

  // renderBody renders the message body (the observe result with the sentinel
  // stripped) into the list. Owner state is applied by loadQueue via updateControls.
  function renderBody(body, isError) {
    elList.innerHTML = "";
    var trimmed = (body || "").replace(/^\s+|\s+$/g, "");
    var lower = trimmed.toLowerCase();
    var box = document.createElement("div");
    if (isError) {
      box.className = "msg";
      var e = document.createElement("pre"); e.className = "raw"; e.textContent = trimmed || "(error)";
      box.appendChild(e);
    } else if (!trimmed || lower === "c3 queue is empty") {
      box.className = "empty";
      box.textContent = "No queued messages — you're all caught up.";
    } else {
      box.className = "msg";
      var pre = document.createElement("pre"); pre.className = "raw"; pre.textContent = trimmed;
      box.appendChild(pre);
    }
    elList.appendChild(box);
    // Chat-tail: after re-rendering, scroll to the newest content — but ONLY when
    // the user is already at/near the bottom (stickToBottom). If they've scrolled
    // up to read older messages, leave their position alone.
    if (stickToBottom) { elList.scrollTop = elList.scrollHeight; }
  }

  // loadQueue OBSERVES the watched topic — a READ-ONLY peek (never claims, never
  // consumes, never steals) — parses the owner state, and renders. Coalesces
  // concurrent callers so the take-over reload can await a real result. Skipped
  // while a hand is in flight (Hand drains the OWNED route; a racing peek would
  // show rows being drained). No watched topic → the "pick a topic" placeholder.
  function loadQueue() {
    if (!connected) { return Promise.resolve(); }
    if (!watchName) {
      ownerState = ""; holderLabel = ""; owned = false; takeoverTopicId = ""; takeoverGroup = "";
      updateControls();
      elList.innerHTML = "";
      var b = document.createElement("div"); b.className = "empty";
      b.textContent = "Enter a topic name above and click Watch to see its inbox.";
      elList.appendChild(b);
      elStatus.textContent = "Idle — nothing watched";
      reportSize();
      return Promise.resolve();
    }
    if (inFlight) { return inFlightP || Promise.resolve(); }
    if (handing) { return Promise.resolve(); }
    inFlight = true;
    elRefresh.disabled = true;
    inFlightP = request("tools/call", { name: "observe", arguments: { name: watchName } }).then(function (result) {
      var isErr = result && result.isError;
      var parsed = parseObserve(extractText(result));
      ownerState = parsed.ownerState;
      holderLabel = parsed.holderLabel;
      takeoverTopicId = parsed.topicId;
      takeoverGroup = parsed.group;
      lastMsgId = newestMsgId(parsed.body);
      renderBody(parsed.body, isErr);
      updateControls();
      elStatus.textContent = "🟢 live · updated " + new Date().toLocaleTimeString();
      autoMaybe(parsed.body, isErr);
    }).catch(function (err) {
      elStatus.textContent = "Watch failed (retrying): " + err.message;
    }).then(function () {
      inFlight = false;
      inFlightP = null;
      elRefresh.disabled = false;
      reportSize();
    });
    return inFlightP;
  }

  // sendReply sends the composer text to the attached topic via the reply tool.
  // Outbound reply is broker-forwarded (NOT subject to the pull-only inbound limit),
  // so this is a plain tools/call — no special plumbing. Owner-only (the composer is
  // hidden unless owned; the guard is belt-and-suspenders). reply_to threads to the
  // newest observed message id when one is available. On success the textarea is
  // cleared; on failure the reason is surfaced inline via the reply note.
  function sendReply() {
    if (sending || !owned) { return; }
    var text = (elReplyText.value || "").replace(/^\s+|\s+$/g, "");
    if (!text) { return; }
    sending = true;
    elSend.disabled = true;
    elReplyText.disabled = true;
    elReplyNote.hidden = true; elReplyNote.textContent = "";
    var args = { text: text };
    if (lastMsgId) { args.reply_to = lastMsgId; }
    request("tools/call", { name: "reply", arguments: args }).then(function (result) {
      // A tool result may carry isError (host accepted the RPC but the send
      // failed) — treat that as not-sent and keep the text so nothing is lost.
      if (result && result.isError) {
        elReplyNote.textContent = "Reply failed: " + (extractText(result) || "host error");
        elReplyNote.hidden = false;
        return;
      }
      elReplyText.value = "";
      elStatus.textContent = "📨 replied to Telegram · " + new Date().toLocaleTimeString();
      loadQueue();
    }).catch(function (err) {
      elReplyNote.textContent = "Reply failed: " + (err && err.message ? err.message : "error");
      elReplyNote.hidden = false;
    }).then(function () {
      sending = false;
      elSend.disabled = false;
      elReplyText.disabled = false;
      reportSize();
    });
  }

  elRefresh.addEventListener("click", loadQueue);
  elWatch.addEventListener("click", watchTopic);
  elTopic.addEventListener("keydown", function (e) { if (e.key === "Enter") { watchTopic(); } });
  if (elTakeover) { elTakeover.addEventListener("click", doTakeOver); }
  elHand.addEventListener("click", function () { handToClaude(false); });
  elSend.addEventListener("click", sendReply);
  // Ctrl/Cmd+Enter sends (plain Enter inserts a newline in the textarea).
  elReplyText.addEventListener("keydown", function (e) {
    if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) { e.preventDefault(); sendReply(); }
  });
  // Chat-tail flag: only auto-scroll to the newest row when the user is already
  // at/near the bottom; a 24px slack absorbs sub-pixel rounding.
  elList.addEventListener("scroll", function () {
    var dist = elList.scrollHeight - elList.scrollTop - elList.clientHeight;
    stickToBottom = dist <= 24;
  });
  elAuto.addEventListener("change", function () {
    updateControls();
    reportSize();
    if (elAuto.checked) { autoFailStreak = 0; autoSkipCycles = 0; loadQueue(); }
  });
  // Pop out: best-effort request for a floating (picture-in-picture) overlay so
  // the panel stays visible as the chat scrolls. This is the ONLY spec'd
  // stays-visible placement — there is no docked/side pane in MCP Apps — and it
  // needs a user gesture (this click supplies it). Degrades honestly if the host
  // doesn't support pip; the reliable fallback is re-summoning via "open the c3
  // inbox".
  if (elPopout) {
    elPopout.addEventListener("click", function () {
      request("ui/request-display-mode", { mode: "pip" }).then(function () {
        elStatus.textContent = "↗ popped out — floating overlay";
      }, function () {
        elStatus.textContent = "Pop-out not supported here — ask Claude to \"open the c3 inbox\" to re-summon it";
      });
    });
  }

  if (typeof ResizeObserver !== "undefined") {
    new ResizeObserver(function () { reportSize(); }).observe(document.documentElement);
  }

  // Handshake, then first (peeking) fetch.
  request("ui/initialize", {
    appCapabilities: { availableDisplayModes: ["inline", "pip"] },
    appInfo: { name: "C3 Inbox", version: "0.1.0" },
    protocolVersion: PROTOCOL_VERSION
  }).then(function (result) {
    connected = true;
    var host = (result && result.hostInfo && result.hostInfo.name) ? result.hostInfo.name : "host";
    elHost.textContent = "Connected to " + host + " · peeking the durable queue (not consuming)";
    applyTheme(result && result.hostContext);
    notify("ui/notifications/initialized");
    elRefresh.disabled = false;
    show(elPopout, true);
    elStatus.textContent = "🟢 live";
    reportSize();
    loadQueue();
    if (!refreshTimer) { refreshTimer = setInterval(loadQueue, REFRESH_MS); }
  }).catch(function (err) {
    elHost.textContent = "Handshake failed";
    elStatus.textContent = "Could not reach host: " + err.message;
    reportSize();
  });

  setTimeout(function () {
    if (!connected) {
      elStatus.textContent = "No response from host after 8s — this view may not be running inside an MCP Apps host.";
      reportSize();
    }
  }, 8000);

  reportSize();
})();
</script>
</body>
</html>
`
