// Leader ACP client for live Telegram → Grok turn inject.
//
// Protocol (proven 2026-07-10, see docs/GROK-INJECT.md):
//
//	unix socket $GROK_HOME/leader.sock
//	frame = u32 BE length + JSON body
//	first message: {"type":"register","client_type":"stdio","mode":"stdio","capabilities":{}}
//	ACP wrap:      {"type":"acp","payload":"<stringified json-rpc>"}
//	inject:        session/prompt on the bound session id
//
// Leader mode is REQUIRED for C3 Grok support. Without a leader socket there
// is no external inject path into the TUI session.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

const (
	leaderProtocolVersion = 1
	defaultLeaderDialTO   = 5 * time.Second
	defaultPromptTO       = 120 * time.Second
	maxFrameBytes         = 64 << 20 // 64 MiB — matches Grok's advertised max
)

// errLeaderUnavailable means no leader socket / connect failed. Callers must
// NOT ack the inbound as delivered (content stays in the durable queue).
var errLeaderUnavailable = errors.New("grok leader unavailable (C3 requires [cli] use_leader = true)")

// errNoSessionID means we could not resolve which Grok session to inject into.
var errNoSessionID = errors.New("could not resolve Grok session id (set C3_GROK_SESSION_ID or ensure active_sessions.json lists this process)")

// errInjectUncertain means the session/prompt frame was WRITTEN to the leader
// but landing was never confirmed (timeout / conn error / shutdown before our
// own echo or the terminal result arrived). The prompt MAY have reached the
// agent. Callers must NOT ack the durable queue line (the message stays queued
// — possible double-delivery is the accepted safe direction; loss is not) and
// must NOT blind-retry the inject (a retry could double-deliver into the TUI);
// isTransientInjectErr classifies it non-transient for exactly that reason.
var errInjectUncertain = errors.New("grok inject uncertain: session/prompt written but landing unconfirmed")

type leaderClient struct {
	// ioMu serializes whole leader exchanges (connect handshake, pending-turn
	// drain, session/prompt) and is the ONLY mutex held across blocking socket
	// I/O. mu guards field state and is held for short snapshot/update critical
	// sections only — NEVER across I/O — so Close() and the attach/recover
	// accessors (stableSessionID / cwd / bindSessionIDForAttach in recover.go)
	// cannot stall behind an in-flight inject's up-to-~120s socket deadlines,
	// which they did when a single mutex covered both. Lock order: ioMu → mu;
	// nothing acquires ioMu while holding mu.
	ioMu sync.Mutex
	mu   sync.Mutex

	conn net.Conn
	// buf is the read re-assembly buffer for conn. It is touched only on the
	// I/O paths, which all run under ioMu.
	buf         []byte
	nextID      atomic.Uint64
	sessionID   string
	cwd         string
	sockPath    string
	initialized bool
	// pendingDrainID is a session/prompt request whose user text already
	// landed (we acked C3's queue) but whose agent turn has not finished.
	// The next Inject drains it first so ACP frames stay ordered.
	pendingDrainID uint64
}

func leaderSocketPath() string {
	if p := os.Getenv("C3_GROK_LEADER_SOCK"); p != "" {
		return p
	}
	if p := os.Getenv("GROK_LEADER_SOCKET"); p != "" {
		return p
	}
	home := os.Getenv("GROK_HOME")
	if home == "" {
		uh, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		home = filepath.Join(uh, ".grok")
	}
	return filepath.Join(home, "leader.sock")
}

func resolveGrokSessionID() string {
	if s := strings.TrimSpace(os.Getenv("C3_GROK_SESSION_ID")); s != "" {
		return s
	}
	if s := strings.TrimSpace(os.Getenv("GROK_SESSION_ID")); s != "" {
		return s
	}
	// active_sessions.json is written by the TUI; match our ancestor PIDs.
	if sid := sessionIDFromActiveSessions(os.Getpid()); sid != "" {
		return sid
	}
	return ""
}

// resolveGrokSessionIDForCWD resolves the inject-target session for an attach
// at cwd (multi-session: MCP under leader cannot rely on TUI PID ancestry).
// Resolution is STRICT — fail-closed on ambiguity: with several sessions in
// the same cwd, only an exact identity signal (a single live entry, or the one
// session whose TUI process is an ancestor of this adapter) may pick one.
// Anything less refuses ("") — the old first-alive/match[0] heuristic injected
// one topic's Telegram content into the WRONG live Grok session, and the
// subsequent ack consumed the durable queue line (silent misdelivery). A
// refusal is fail-safe: inject errors with errNoSessionID and the message
// stays queued for fetch_queue; pin C3_GROK_SESSION_ID to disambiguate.
func resolveGrokSessionIDForCWD(cwd string) string {
	if s := strings.TrimSpace(os.Getenv("C3_GROK_SESSION_ID")); s != "" {
		return s
	}
	if s := strings.TrimSpace(os.Getenv("GROK_SESSION_ID")); s != "" {
		return s
	}
	cwd = filepath.Clean(cwd)
	sessions := readActiveSessions()
	var match []activeSession
	for _, s := range sessions {
		if s.SessionID != "" && filepath.Clean(s.CWD) == cwd {
			match = append(match, s)
		}
	}
	if len(match) == 1 {
		return match[0].SessionID
	}
	if len(match) > 1 {
		var alive []activeSession
		for _, s := range match {
			if processAlive(s.PID) {
				alive = append(alive, s)
			}
		}
		// Exactly one live entry: the rest are stale file leftovers, not a
		// genuine multi-session ambiguity.
		if len(alive) == 1 {
			return alive[0].SessionID
		}
		candidates := alive
		if len(candidates) == 0 {
			candidates = match
		}
		// Several (or zero) live sessions in this cwd: only PID ancestry is an
		// exact "this adapter belongs to that session" signal. Under leader
		// mode the adapter is usually spawned by the leader, not the TUI, so
		// this often misses — then we refuse rather than guess.
		ancestors := ancestorPIDs(os.Getpid())
		var own []activeSession
		for _, s := range candidates {
			if s.PID > 0 && ancestors[s.PID] {
				own = append(own, s)
			}
		}
		if len(own) == 1 {
			return own[0].SessionID
		}
		log.Printf("grok session resolution: %d sessions in cwd %q (%d alive), no exact match — refusing to auto-bind so content can't inject into the wrong session (set C3_GROK_SESSION_ID; messages stay in the durable queue)",
			len(match), cwd, len(alive))
		return ""
	}
	// No cwd match — fall back to the process-identity resolution.
	return sessionIDFromActiveSessions(os.Getpid())
}

func readActiveSessions() []activeSession {
	home := os.Getenv("GROK_HOME")
	if home == "" {
		uh, err := os.UserHomeDir()
		if err != nil {
			return nil
		}
		home = filepath.Join(uh, ".grok")
	}
	data, err := os.ReadFile(filepath.Join(home, "active_sessions.json"))
	if err != nil {
		return nil
	}
	var sessions []activeSession
	if err := json.Unmarshal(data, &sessions); err != nil {
		return nil
	}
	return sessions
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	// Linux: /proc/<pid> is the reliable existence check.
	_, err := os.Stat(fmt.Sprintf("/proc/%d", pid))
	return err == nil
}

type activeSession struct {
	SessionID string `json:"session_id"`
	PID       int    `json:"pid"`
	CWD       string `json:"cwd"`
}

func sessionIDFromActiveSessions(selfPID int) string {
	sessions := readActiveSessions()
	ancestors := ancestorPIDs(selfPID)
	for _, s := range sessions {
		if s.SessionID == "" || s.PID == 0 {
			continue
		}
		if ancestors[s.PID] {
			return s.SessionID
		}
	}
	// Single live session in this GROK_HOME — common when only one TUI is
	// open. PID 0 (absent/unknown pid) counts as NOT alive: a stale entry with
	// no pid must never win this heuristic and bind a dead session — the
	// fail-safe side is refusing, which leaves messages in the durable queue.
	alive := 0
	var only string
	for _, s := range sessions {
		if s.SessionID == "" {
			continue
		}
		if processAlive(s.PID) {
			alive++
			only = s.SessionID
		}
	}
	if alive == 1 {
		return only
	}
	return ""
}

func ancestorPIDs(start int) map[int]bool {
	out := map[int]bool{}
	pid := start
	for i := 0; i < 32 && pid > 1; i++ {
		out[pid] = true
		ppid, err := parentPID(pid)
		if err != nil || ppid <= 0 || ppid == pid {
			break
		}
		pid = ppid
	}
	return out
}

func parentPID(pid int) (int, error) {
	// Linux /proc/<pid>/stat: field 4 is ppid.
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	// Format: pid (comm) state ppid ...
	s := string(b)
	rparen := strings.LastIndex(s, ")")
	if rparen < 0 || rparen+2 >= len(s) {
		return 0, fmt.Errorf("parse stat")
	}
	fields := strings.Fields(s[rparen+2:])
	if len(fields) < 2 {
		return 0, fmt.Errorf("stat fields")
	}
	// fields[0]=state, fields[1]=ppid
	return strconv.Atoi(fields[1])
}

// ensure returns nil when the client is connected, initialized, and bound to a
// session — connecting first when not. Requires c.ioMu.
func (c *leaderClient) ensure(ctx context.Context) error {
	c.mu.Lock()
	ready := c.conn != nil && c.initialized && c.sessionID != ""
	c.mu.Unlock()
	if ready {
		return nil
	}
	return c.connect(ctx)
}

// connect dials the leader socket and runs the register → initialize →
// session/load handshake. Requires c.ioMu; field state is updated under c.mu
// in short sections so Close() can interrupt a stuck handshake by closing the
// published conn (ctx cancellation does the same via the AfterFunc below).
func (c *leaderClient) connect(ctx context.Context) error {
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.initialized = false
	}
	c.pendingDrainID = 0 // a fresh conn has no pending turn to drain
	sock := c.sockPath
	if sock == "" {
		sock = leaderSocketPath()
		c.sockPath = sock
	}
	sid := c.sessionID
	cwd := c.cwd
	c.mu.Unlock()
	c.buf = nil

	if sock == "" {
		return errLeaderUnavailable
	}
	if _, err := os.Stat(sock); err != nil {
		return fmt.Errorf("%w: %s: %v", errLeaderUnavailable, sock, err)
	}

	d := net.Dialer{Timeout: defaultLeaderDialTO}
	conn, err := d.DialContext(ctx, "unix", sock)
	if err != nil {
		return fmt.Errorf("%w: dial %s: %v", errLeaderUnavailable, sock, err)
	}
	// Publish immediately so Close() can interrupt a stuck handshake; the
	// AfterFunc makes ctx cancellation interrupt it too (a socket deadline
	// alone re-arms on the next read). initialized stays false until the
	// handshake completes.
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	if err := c.writeJSON(conn, map[string]any{
		"type":         "register",
		"client_type":  "stdio",
		"mode":         "stdio",
		"capabilities": map[string]any{},
	}); err != nil {
		c.dropConn(conn)
		return err
	}

	// Wait for leader_ready (registered may arrive first with ready=false).
	deadline := time.Now().Add(15 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		msg, err := c.readJSON(conn, time.Until(deadline))
		if err != nil {
			c.dropConn(conn)
			return fmt.Errorf("leader register: %w", err)
		}
		switch msg["type"] {
		case "registered", "leader_ready":
			if msg["type"] == "leader_ready" || msg["ready"] == true {
				ready = true
			}
			if msg["type"] == "leader_ready" {
				ready = true
			}
		case "error":
			c.dropConn(conn)
			return fmt.Errorf("leader register error: %v", msg)
		}
		if ready {
			break
		}
	}
	if !ready {
		// Proceed after registered even if ready flag lagged — initialize will fail loudly if not ready.
	}

	// ACP initialize
	initResp, err := c.acpRequest(ctx, conn, "initialize", map[string]any{
		"protocolVersion": leaderProtocolVersion,
		"clientCapabilities": map[string]any{
			"fs":       map[string]any{"readTextFile": true, "writeTextFile": false},
			"terminal": false,
		},
		"clientInfo": map[string]any{
			"name":    "c3-grok-adapter",
			"version": adapterVersion,
		},
	})
	if err != nil {
		c.dropConn(conn)
		return fmt.Errorf("leader initialize: %w", err)
	}
	_ = initResp
	_ = c.acpNotify(conn, "notifications/initialized", map[string]any{})

	if sid == "" {
		sid = resolveGrokSessionID()
	}
	if sid == "" {
		c.dropConn(conn)
		return errNoSessionID
	}
	if cwd == "" {
		cwd, _ = os.Getwd()
	}

	// Prefer session/load (existing TUI session). Fall back to error — we must
	// not session/new a parallel conversation the user cannot see.
	if _, err := c.acpRequest(ctx, conn, "session/load", map[string]any{
		"cwd":        cwd,
		"sessionId":  sid,
		"mcpServers": []any{},
	}); err != nil {
		c.dropConn(conn)
		return fmt.Errorf("session/load %s: %w", sid, err)
	}

	c.mu.Lock()
	switch {
	case c.conn != conn:
		// Close() raced the handshake and already tore this conn down.
		c.mu.Unlock()
		return errLeaderUnavailable
	case c.sessionID != "" && c.sessionID != sid:
		// bindSessionIDForAttach re-bound the target mid-handshake; the session
		// we loaded is stale. Drop so the next inject reconnects to the new id.
		rebound := c.sessionID
		c.mu.Unlock()
		c.dropConn(conn)
		return fmt.Errorf("grok session re-bound during connect (%s → %s) — retrying on next inject", sid, rebound)
	}
	c.sessionID = sid
	c.initialized = true
	c.mu.Unlock()
	return nil
}

// Inject delivers text as a user turn (session/prompt) on the bound Grok
// session and returns once landing is CONFIRMED.
//
// Ack contract (grokForwardLoop's OpInboundDelivered depends on it):
//   - nil — the prompt landed in OUR session: we saw either our own echoed
//     user_message_chunk (same sessionId, text matching the injected text) or
//     the request's terminal result. Safe to ack the durable queue line.
//   - errors.Is(err, errInjectUncertain) — the session/prompt frame was
//     written but landing was never confirmed (post-write timeout / conn
//     error / shutdown). The prompt MAY have landed: do NOT ack (the line
//     stays queued; a later fetch_queue drain double-delivers at worst,
//     never loses) and do NOT blind-retry (a retry could double-inject).
//   - any other error — the prompt did not land (pre-write failure, or the
//     leader answered with a definite JSON-RPC error). Not acked; retried
//     only when isTransientInjectErr says the leader was merely busy.
//
// ctx bounds AND cancels the whole exchange: grokForwardLoop plumbs the
// adapter run context, so shutdown interrupts an in-flight inject instead of
// waiting out the 120s socket deadline.
func (c *leaderClient) Inject(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("empty inject text")
	}
	c.ioMu.Lock()
	defer c.ioMu.Unlock()
	if err := c.ensure(ctx); err != nil {
		return err
	}

	// Snapshot conn + session under mu, then do ALL blocking I/O on the
	// snapshot without holding mu: ioMu alone serializes exchanges, and
	// Close()/attach-time accessors keep working mid-inject (Close unblocks
	// the pending read by closing the snapshot conn).
	c.mu.Lock()
	conn := c.conn
	sid := c.sessionID
	c.mu.Unlock()
	if conn == nil {
		return errLeaderUnavailable // Close() raced ensure
	}

	to := defaultPromptTO
	if deadline, ok := ctx.Deadline(); ok {
		if rem := time.Until(deadline); rem > 0 && rem < to {
			to = rem
		}
	}
	promptCtx, cancel := context.WithTimeout(ctx, to)
	defer cancel()
	// Cancellation must interrupt an in-flight blocking read — the per-read
	// socket deadlines re-arm every loop iteration, so closing the conn is the
	// only definitive interrupt. The deferred stop() runs BEFORE the deferred
	// cancel(), so a successful return never closes a healthy conn; on a real
	// cancel/timeout every error path below drops the conn anyway.
	stop := context.AfterFunc(promptCtx, func() { _ = conn.Close() })
	defer stop()

	// Finish the prior turn's ACP response before starting a new prompt.
	if err := c.drainPending(promptCtx, conn); err != nil {
		c.dropConn(conn)
		return err
	}

	// Ack C3 as soon as the user text lands in the session — NOT when the
	// agent turn finishes. Waiting for end-of-turn left orphans in the durable
	// queue when the adapter was killed mid-turn after inject already showed
	// in the TUI (msg 4224, 2026-07-10).
	landed, err := c.promptUntilLanded(promptCtx, conn, sid, text)
	if err != nil {
		c.dropConn(conn)
		return err
	}
	if landed.pendingDrain {
		c.mu.Lock()
		if c.conn == conn { // revalidate: a racing Close() voids the pending turn
			c.pendingDrainID = landed.id
		}
		c.mu.Unlock()
	}
	return nil
}

// dropConn closes the conn snapshot and, when it is still the current conn,
// resets client state so the next Inject reconnects. Revalidating against the
// snapshot keeps a stale error path from wiping state that belongs to a newer
// connection.
func (c *leaderClient) dropConn(conn net.Conn) {
	if conn == nil {
		return
	}
	_ = conn.Close()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == conn {
		c.conn = nil
		c.initialized = false
		c.pendingDrainID = 0
	}
}

// drainPending waits for a previously-landed session/prompt result. Requires
// c.ioMu; conn is the Inject-time snapshot.
func (c *leaderClient) drainPending(ctx context.Context, conn net.Conn) error {
	c.mu.Lock()
	id := c.pendingDrainID
	c.mu.Unlock()
	if id == 0 {
		return nil
	}
	clearPending := func() {
		c.mu.Lock()
		if c.pendingDrainID == id {
			c.pendingDrainID = 0
		}
		c.mu.Unlock()
	}
	deadline := time.Now().Add(defaultPromptTO)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rem := time.Until(deadline)
		if rem <= 0 {
			// Don't fail inject forever — drop and re-register next time.
			clearPending()
			return nil
		}
		frame, err := c.readJSON(conn, rem)
		if err != nil {
			clearPending()
			return err
		}
		if frame["type"] != "acp" {
			continue
		}
		acp, ok := decodeACPPayload(frame["payload"])
		if !ok {
			continue
		}
		if got, ok := asUint64(acp["id"]); ok && got == id {
			clearPending()
			if errObj, ok := acp["error"]; ok && errObj != nil {
				// Turn already landed for the user; log-level only at caller.
				_ = errObj
			}
			return nil
		}
	}
}

type promptLand struct {
	id           uint64
	pendingDrain bool // true if we returned before the final result
}

// promptUntilLanded sends session/prompt for text on session sid and waits
// until the prompt is CONFIRMED landed: our own echoed user_message_chunk —
// same session, text matching what we injected — or the request's terminal
// result. A chunk from another session on the shared leader, or one whose
// text is not our prompt (a human typing in the TUI concurrently), is never
// accepted as proof: a false landing-confirm would let the caller ack (and
// the broker Consume) a queue line that was never delivered. Requires c.ioMu;
// conn is the Inject-time snapshot.
//
// Every post-write failure (timeout, read error, ctx cancel) returns
// errInjectUncertain — see the Inject contract.
func (c *leaderClient) promptUntilLanded(ctx context.Context, conn net.Conn, sid, text string) (promptLand, error) {
	id := c.nextID.Add(1)
	msg := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  "session/prompt",
		"params": map[string]any{
			"sessionId": sid,
			"prompt": []map[string]any{{
				"type": "text",
				"text": text,
			}},
		},
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return promptLand{}, err
	}
	if err := c.writeJSON(conn, map[string]any{"type": "acp", "payload": string(payload)}); err != nil {
		return promptLand{}, err
	}

	// The prompt frame is on the wire — from here on, a failure without a
	// definite answer for our id is UNCERTAIN: the agent may have accepted the
	// prompt even though we never saw the confirmation.
	deadline := time.Now().Add(defaultPromptTO)
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	}
	for {
		if err := ctx.Err(); err != nil {
			return promptLand{}, fmt.Errorf("%w: %v", errInjectUncertain, err)
		}
		rem := time.Until(deadline)
		if rem <= 0 {
			return promptLand{}, fmt.Errorf("%w: %v", errInjectUncertain, context.DeadlineExceeded)
		}
		frame, err := c.readJSON(conn, rem)
		if err != nil {
			return promptLand{}, fmt.Errorf("%w: %v", errInjectUncertain, err)
		}
		if frame["type"] != "acp" {
			continue
		}
		acp, ok := decodeACPPayload(frame["payload"])
		if !ok {
			continue
		}
		// Terminal result/error for our request.
		if got, ok := asUint64(acp["id"]); ok && got == id {
			if errObj, ok := acp["error"]; ok && errObj != nil {
				b, _ := json.Marshal(errObj)
				return promptLand{}, fmt.Errorf("session/prompt: %s", b)
			}
			return promptLand{id: id, pendingDrain: false}, nil
		}
		// Streamed updates: OUR user text echoed into OUR session ⇒ landed.
		if method, _ := acp["method"].(string); method == "session/update" {
			params, _ := acp["params"].(map[string]any)
			if promptEchoMatches(params, sid, text) {
				return promptLand{id: id, pendingDrain: true}, nil
			}
		}
	}
}

// promptEchoMatches reports whether a session/update's params are OUR prompt's
// echo: sessionUpdate == user_message_chunk, the update's sessionId equals the
// session we prompted, and the chunk's text is a non-empty prefix of the
// injected text (the leader may echo the message in pieces; the first piece
// always starts at the beginning — GROK-INJECT.md's probe saw our text echoed
// back verbatim). Chunks without extractable text are rejected: accepting them
// would re-open the false-confirm hole this closes (any concurrent keystroke
// echo would "confirm" our inject). If the leader ever echoes altered text,
// landing simply falls back to the terminal-result confirm — slower, never
// wrong.
func promptEchoMatches(params map[string]any, sid, text string) bool {
	update, _ := params["update"].(map[string]any)
	kind, _ := update["sessionUpdate"].(string)
	if kind == "" {
		kind, _ = update["session_update"].(string)
	}
	if kind != "user_message_chunk" {
		return false
	}
	got, _ := params["sessionId"].(string)
	if got == "" {
		got, _ = params["session_id"].(string)
	}
	if got != sid {
		return false
	}
	chunk := chunkText(update["content"])
	return chunk != "" && strings.HasPrefix(text, chunk)
}

// chunkText extracts the text of a user_message_chunk content payload, which
// the leader emits either as a single content block or a block array.
func chunkText(content any) string {
	switch v := content.(type) {
	case string:
		return v
	case map[string]any:
		t, _ := v["text"].(string)
		return t
	case []any:
		var b strings.Builder
		for _, blk := range v {
			if m, ok := blk.(map[string]any); ok {
				if t, ok := m["text"].(string); ok {
					b.WriteString(t)
				}
			}
		}
		return b.String()
	}
	return ""
}

func decodeACPPayload(raw any) (map[string]any, bool) {
	switch p := raw.(type) {
	case string:
		var acp map[string]any
		if err := json.Unmarshal([]byte(p), &acp); err != nil {
			return nil, false
		}
		return acp, true
	case map[string]any:
		return p, true
	default:
		return nil, false
	}
}

// Close tears down the leader conn. It takes only the state mutex — never
// ioMu — so shutdown is not queued behind an in-flight inject; closing the
// conn is exactly what unblocks that inject's pending read.
func (c *leaderClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.initialized = false
	c.pendingDrainID = 0
}

func (c *leaderClient) writeJSON(conn net.Conn, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(raw) > maxFrameBytes {
		return fmt.Errorf("frame too large: %d", len(raw))
	}
	hdr := []byte{
		byte(len(raw) >> 24),
		byte(len(raw) >> 16),
		byte(len(raw) >> 8),
		byte(len(raw)),
	}
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	_, err = conn.Write(raw)
	return err
}

// readJSON reads one length-prefixed JSON frame from conn. It uses c.buf for
// re-assembly, so it requires c.ioMu (all I/O paths hold it).
func (c *leaderClient) readJSON(conn net.Conn, timeout time.Duration) (map[string]any, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		if len(c.buf) >= 4 {
			n := int(c.buf[0])<<24 | int(c.buf[1])<<16 | int(c.buf[2])<<8 | int(c.buf[3])
			if n < 0 || n > maxFrameBytes {
				return nil, fmt.Errorf("bad frame length %d", n)
			}
			if len(c.buf) >= 4+n {
				body := c.buf[4 : 4+n]
				c.buf = c.buf[4+n:]
				var msg map[string]any
				if err := json.Unmarshal(body, &msg); err != nil {
					return nil, err
				}
				return msg, nil
			}
		}
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, err
		}
		tmp := make([]byte, 64*1024)
		n, err := conn.Read(tmp)
		if err != nil {
			return nil, err
		}
		c.buf = append(c.buf, tmp[:n]...)
	}
}

func (c *leaderClient) acpNotify(conn net.Conn, method string, params map[string]any) error {
	msg := map[string]any{"jsonrpc": "2.0", "method": method}
	if params != nil {
		msg["params"] = params
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return c.writeJSON(conn, map[string]any{"type": "acp", "payload": string(payload)})
}

func (c *leaderClient) acpRequest(ctx context.Context, conn net.Conn, method string, params map[string]any) (map[string]any, error) {
	id := c.nextID.Add(1)
	msg := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
	}
	if params != nil {
		msg["params"] = params
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	if err := c.writeJSON(conn, map[string]any{"type": "acp", "payload": string(payload)}); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(defaultPromptTO)
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rem := time.Until(deadline)
		if rem <= 0 {
			return nil, context.DeadlineExceeded
		}
		frame, err := c.readJSON(conn, rem)
		if err != nil {
			return nil, err
		}
		if frame["type"] != "acp" {
			// control / ping / leader_ready noise — ignore
			continue
		}
		rawPayload := frame["payload"]
		var acp map[string]any
		switch p := rawPayload.(type) {
		case string:
			if err := json.Unmarshal([]byte(p), &acp); err != nil {
				continue
			}
		case map[string]any:
			acp = p
		default:
			continue
		}
		if got, ok := asUint64(acp["id"]); ok && got == id {
			if errObj, ok := acp["error"]; ok && errObj != nil {
				b, _ := json.Marshal(errObj)
				return nil, fmt.Errorf("%s: %s", method, b)
			}
			if result, ok := acp["result"].(map[string]any); ok {
				return result, nil
			}
			return map[string]any{}, nil
		}
		// Drop unrelated ACP notifications/updates (session/update etc.).
	}
}

func asUint64(v any) (uint64, bool) {
	switch x := v.(type) {
	case float64:
		return uint64(x), true
	case int:
		return uint64(x), true
	case int64:
		return uint64(x), true
	case uint64:
		return x, true
	case json.Number:
		n, err := x.Int64()
		return uint64(n), err == nil
	default:
		return 0, false
	}
}

// c3TrailerSentinel starts every host-generated meta line appended to an
// injected turn. The body is UNTRUSTED channel text (any member of an
// allowlisted group reaches this path), so attribution must be structural,
// not positional: meta lines are derived ONLY from broker-supplied fields
// (never message content), and sanitizeInjectBody quotes any body line that
// tries to open with the sentinel — a forged trailer therefore renders
// visibly as quoted body text, never as metadata. This is the closest the
// plain-text session/prompt medium gets to the Claude adapter's structural
// content/meta split (buildClaudeChannelFrame's string content + typed meta
// map inside a host-rendered channel frame).
const c3TrailerSentinel = "[c3]"

// sanitizeInjectBody neutralizes attribution forgery in untrusted channel
// text: any line whose trimmed form starts with c3TrailerSentinel is quoted
// ("> "-prefixed), so it renders as visibly-quoted body content instead of a
// host meta line. Perfect unforgeability is impossible in a plain-text prompt
// (homoglyphs, zero-width characters), but a byte-exact forged trailer can
// never collide with the real one.
func sanitizeInjectBody(body string) string {
	if !strings.Contains(body, c3TrailerSentinel) {
		return body
	}
	lines := strings.Split(body, "\n")
	for i, ln := range lines {
		if strings.HasPrefix(strings.TrimSpace(ln), c3TrailerSentinel) {
			lines[i] = "> " + ln
		}
	}
	return strings.Join(lines, "\n")
}

// formatInboundTurnText renders one inbound as a Grok user turn.
// Body first so the TUI sticky/preview shows real text (not "message...").
// Meta last — short, for routing/download only — and STRUCTURALLY owned by
// the host: every meta line starts with c3TrailerSentinel and is derived only
// from broker-supplied fields, while the untrusted body is sanitized so it
// cannot fabricate such a line (see sanitizeInjectBody). Without that split,
// any allowlisted-group member could forge an attribution trailer inside
// their message body and speak to the agent with the operator's voice.
func formatInboundTurnText(in *c3types.Inbound) string {
	user := strconv.FormatInt(in.Sender.UserID, 10)
	if in.Sender.Username != "" {
		user = "@" + in.Sender.Username + " (" + user + ")"
	}
	channel := fmt.Sprintf("%d", in.ChatID)
	if in.TopicID != nil {
		channel = fmt.Sprintf("%d/%d", in.ChatID, *in.TopicID)
	}

	body := strings.TrimSpace(in.Text)
	// Prefer the human text as the first line so scrollback previews show it.
	if body == "" && len(in.Attachments) > 0 {
		body = "(" + string(in.Attachments[0].Kind) + " attachment)"
	}
	if body == "" {
		body = "(empty Telegram message)"
	}

	var b strings.Builder
	b.WriteString(sanitizeInjectBody(body))
	fmt.Fprintf(&b, "\n\n%s — %s · %s", c3TrailerSentinel, user, channel)
	for _, att := range in.Attachments {
		fmt.Fprintf(&b, "\n%s file: kind=%s file_id=%s", c3TrailerSentinel, att.Kind, att.FileID)
		if att.Name != "" {
			fmt.Fprintf(&b, " name=%q", att.Name)
		}
	}
	return b.String()
}
