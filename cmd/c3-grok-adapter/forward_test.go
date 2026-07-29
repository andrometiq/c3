// Tests for the grokForwardLoop consume/ack state machine — the safety-critical
// path behind the 2026-07-10 double-delivery incident (task #43). Hermetic: the
// fake Grok leader is a unix socket in t.TempDir() speaking the register/ACP
// protocol, and the "broker" is the far side of a net.Pipe — no network, no
// real broker, no Telegram. Patterns mirror cmd/c3-codex-adapter's
// inbound_ack_test.go / forward_blocked_test.go and this package's
// leader_test.go fake leader.
package main

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// fakeGrokLeader is a multi-connection fake leader: unlike leader_test.go's
// single-accept helper, it keeps accepting so a client that drops its conn
// after a failed session/prompt (leaderClient.Inject does dropConnLocked on any
// prompt error) can reconnect and be served again. failFirst session/prompt
// requests are answered with a JSON-RPC error carrying failMsg; later prompts
// succeed (user_message_chunk, then the terminal result). served receives the
// text of every SUCCESSFUL prompt, so tests can wait until a specific message
// was actually delivered live before asserting on the (absence of an) ack.
type fakeGrokLeader struct {
	sock string

	mu       sync.Mutex
	prompts  []string // successful prompt texts, in order
	attempts int      // ALL session/prompt requests seen (failed + succeeded)

	failFirst int
	failMsg   string
	served    chan string
}

func (l *fakeGrokLeader) promptAttempts() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.attempts
}

func startFakeGrokLeader(t *testing.T, failFirst int, failMsg string) *fakeGrokLeader {
	t.Helper()
	dir := t.TempDir()
	l := &fakeGrokLeader{
		sock:      filepath.Join(dir, "leader.sock"),
		failFirst: failFirst,
		failMsg:   failMsg,
		served:    make(chan string, 16),
	}
	ln, err := net.Listen("unix", l.sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go l.serveConn(conn)
		}
	}()
	return l
}

func (l *fakeGrokLeader) serveConn(conn net.Conn) {
	defer conn.Close()
	buf := []byte{}
	readMsg := func() (map[string]any, error) {
		for {
			if len(buf) >= 4 {
				n := int(buf[0])<<24 | int(buf[1])<<16 | int(buf[2])<<8 | int(buf[3])
				if len(buf) >= 4+n {
					body := buf[4 : 4+n]
					buf = buf[4+n:]
					var m map[string]any
					if err := json.Unmarshal(body, &m); err != nil {
						return nil, err
					}
					return m, nil
				}
			}
			tmp := make([]byte, 4096)
			// Generous idle deadline: the unlatch test parks the conn between
			// messages while it asserts no-ack windows; a short deadline here
			// would kill the conn and turn the next inject into a failure.
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			k, err := conn.Read(tmp)
			if err != nil {
				return nil, err
			}
			buf = append(buf, tmp[:k]...)
		}
	}
	writeMsg := func(v any) {
		raw, _ := json.Marshal(v)
		hdr := []byte{byte(len(raw) >> 24), byte(len(raw) >> 16), byte(len(raw) >> 8), byte(len(raw))}
		_, _ = conn.Write(hdr)
		_, _ = conn.Write(raw)
	}

	m, err := readMsg()
	if err != nil || m["type"] != "register" {
		return
	}
	writeMsg(map[string]any{"type": "registered", "client_id": 1, "ready": true,
		"leader_protocol_version": 1, "leader_binary_version": "test",
		"leader_capabilities": map[string]any{}})
	writeMsg(map[string]any{"type": "leader_ready"})

	for {
		m, err := readMsg()
		if err != nil {
			return
		}
		if m["type"] != "acp" {
			continue
		}
		payload, _ := m["payload"].(string)
		var acp map[string]any
		if err := json.Unmarshal([]byte(payload), &acp); err != nil {
			continue
		}
		id := acp["id"]
		method, _ := acp["method"].(string)
		switch method {
		case "initialize":
			writeMsg(map[string]any{"type": "acp", "payload": mustJSON(map[string]any{
				"jsonrpc": "2.0", "id": id, "result": map[string]any{"protocolVersion": 1},
			})})
		case "notifications/initialized":
			// no response
		case "session/load":
			writeMsg(map[string]any{"type": "acp", "payload": mustJSON(map[string]any{
				"jsonrpc": "2.0", "id": id, "result": map[string]any{"sessionId": "sess-test"},
			})})
		case "session/prompt":
			l.mu.Lock()
			l.attempts++
			fail := l.attempts <= l.failFirst
			l.mu.Unlock()
			if fail {
				writeMsg(map[string]any{"type": "acp", "payload": mustJSON(map[string]any{
					"jsonrpc": "2.0", "id": id,
					"error": map[string]any{"code": -32000, "message": l.failMsg},
				})})
				continue // client drops the conn on a prompt error; readMsg will EOF
			}
			text := ""
			if params, ok := acp["params"].(map[string]any); ok {
				if prompt, ok := params["prompt"].([]any); ok && len(prompt) > 0 {
					if block, ok := prompt[0].(map[string]any); ok {
						text, _ = block["text"].(string)
					}
				}
			}
			l.mu.Lock()
			l.prompts = append(l.prompts, text)
			l.mu.Unlock()
			// Land the user message first (the adapter treats this as the ack
			// point), then finish the turn (exercises the pendingDrain path).
			writeMsg(map[string]any{"type": "acp", "payload": mustJSON(map[string]any{
				"jsonrpc": "2.0", "method": "session/update",
				"params": map[string]any{
					"sessionId": "sess-test",
					"update": map[string]any{
						"sessionUpdate": "user_message_chunk",
						"content":       map[string]any{"type": "text", "text": "x"},
					},
				},
			})})
			time.Sleep(10 * time.Millisecond)
			writeMsg(map[string]any{"type": "acp", "payload": mustJSON(map[string]any{
				"jsonrpc": "2.0", "id": id, "result": map[string]any{"stopReason": "end_turn"},
			})})
			select {
			case l.served <- text:
			default:
			}
		default:
			writeMsg(map[string]any{"type": "acp", "payload": mustJSON(map[string]any{
				"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32601, "message": "nope"},
			})})
		}
	}
}

// newForwardTestAdapter builds an adapter bound to the given leader socket with
// a net.Pipe broker conn whose far side the test reads (the broker's view of
// the adapter's delivered-ack writes). Constructed directly (not via
// newAdapter) so the session id / cwd don't depend on env or
// active_sessions.json. transport stays nil — latchForwardBlocked latches but
// skips the nudge, exactly like the codex adapter's tests.
func newForwardTestAdapter(t *testing.T, sock string) (*adapter, *ipc.Conn) {
	t.Helper()
	a := &adapter{
		pending:   map[string]chan ipc.ToolResultMsg{},
		fqPending: map[string]chan ipc.FetchQueueResp{},
		rtPending: map[string]chan ipc.RetranscribeResp{},
		forwardCh: make(chan grokForwardReq, 256),
		leader: &leaderClient{
			sessionID: "sess-test",
			cwd:       t.TempDir(),
			sockPath:  sock,
		},
	}
	go a.grokForwardLoop()
	peerSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = peerSide.Close(); _ = brokerSide.Close() })
	a.bmu.Lock()
	a.conn = ipc.NewConn(brokerSide)
	a.bmu.Unlock()
	return a, ipc.NewConn(peerSide)
}

// startAckReader starts the SINGLE reader goroutine for a broker-side conn and
// streams every parsed InboundDeliveredMsg into the returned channel. Use this
// (not repeated readDeliveredAck calls) when a test asserts a NO-ack window and
// then a real ack on the same conn: each readDeliveredAck timeout leaves an
// orphaned ReadFrame goroutine that would race the next reader for — and
// swallow — the later frame. The goroutine exits when the pipe closes
// (t.Cleanup).
func startAckReader(c *ipc.Conn) <-chan ipc.InboundDeliveredMsg {
	ch := make(chan ipc.InboundDeliveredMsg, 16)
	go func() {
		defer close(ch)
		for {
			raw, err := c.ReadFrame()
			if err != nil {
				return
			}
			var m ipc.InboundDeliveredMsg
			if json.Unmarshal(raw, &m) == nil {
				ch <- m
			}
		}
	}()
	return ch
}

// readDeliveredAck reads one frame off the broker-side conn within a deadline
// and returns the parsed InboundDeliveredMsg + whether a frame arrived. A
// nil/closed read or timeout means "no ack was sent". Mirrors the codex
// adapter's helper of the same name. Safe only when it is the LAST read a test
// performs on the conn (see startAckReader for the mixed case).
func readDeliveredAck(t *testing.T, c *ipc.Conn, within time.Duration) (ipc.InboundDeliveredMsg, bool) {
	t.Helper()
	type res struct {
		raw []byte
		err error
	}
	ch := make(chan res, 1)
	go func() {
		raw, err := c.ReadFrame()
		ch <- res{raw, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return ipc.InboundDeliveredMsg{}, false
		}
		var m ipc.InboundDeliveredMsg
		if err := json.Unmarshal(r.raw, &m); err != nil {
			t.Fatalf("unmarshal delivered ack: %v", err)
		}
		return m, true
	case <-time.After(within):
		return ipc.InboundDeliveredMsg{}, false
	}
}

func inboundRaw(t *testing.T, msg ipc.InboundMsg) []byte {
	t.Helper()
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal inbound: %v", err)
	}
	return raw
}

func waitServed(t *testing.T, l *fakeGrokLeader, within time.Duration) string {
	t.Helper()
	select {
	case text := <-l.served:
		return text
	case <-time.After(within):
		t.Fatal("fake leader never served the prompt")
		return ""
	}
}

// (a) Ack confirmation: a successful live inject must send an
// OpInboundDelivered ack echoing Covered VERBATIM as Count, so the broker
// Consumes exactly the lines this (possibly merged) push covered. The 2026-07-10
// incident showed zero inbound_delivered lines all day — this pins the confirm.
func TestGrokForward_InjectSuccess_AcksCovered(t *testing.T) {
	l := startFakeGrokLeader(t, 0, "")
	a, peer := newForwardTestAdapter(t, l.sock)

	a.handleInbound(inboundRaw(t, ipc.InboundMsg{
		Op: ipc.OpInbound, Covered: 2, DeliveryToken: "broker-a-7",
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 7, Text: "hi from phone"},
	}))

	ack, got := readDeliveredAck(t, peer, 3*time.Second)
	if !got {
		t.Fatal("a successful live inject must send an OpInboundDelivered ack so the broker consumes the queued copy")
	}
	if !ack.OK || ack.UpdateID != 7 || ack.Count != 2 || ack.DeliveryToken != "broker-a-7" {
		t.Fatalf("ack = %+v, want {OK:true UpdateID:7 Count:2 DeliveryToken:broker-a-7}", ack)
	}
	if text := waitServed(t, l, time.Second); !strings.HasPrefix(text, "hi from phone") {
		t.Fatalf("injected turn should lead with the body, got %q", text)
	}
}

// Verbatim-Count contract (C1): a zero-covered push covers no stored lines, so
// the adapter must NOT fabricate a Count=1 ack — the broker forwards Count
// verbatim and a 0→1 bump would Consume a real backlog line the push never
// delivered. Nothing-to-consume ⇒ no ack at all (claude/codex parity).
func TestGrokForward_ZeroCovered_NoAck(t *testing.T) {
	l := startFakeGrokLeader(t, 0, "")
	a, peer := newForwardTestAdapter(t, l.sock)

	a.handleInbound(inboundRaw(t, ipc.InboundMsg{
		Op: ipc.OpInbound, Covered: 0,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 8, Text: "zero covered"},
	}))

	waitServed(t, l, 3*time.Second) // delivered live — absence of ack below isn't slowness
	if ack, got := readDeliveredAck(t, peer, 500*time.Millisecond); got {
		t.Fatalf("zero-covered push must not ack (got %+v): a bumped Count=1 would over-consume real backlog", ack)
	}
}

// (b) part 1: a failed inject must NOT ack — the durable queue keeps the
// message for fetch_queue recovery — and must latch forwardBlocked so no later
// count-off-head ack consumes the undelivered head.
func TestGrokForward_FailedInject_NoAck_LatchesBlocked(t *testing.T) {
	l := startFakeGrokLeader(t, 1, "forced inject failure") // non-transient → fails fast
	a, peer := newForwardTestAdapter(t, l.sock)

	a.handleInbound(inboundRaw(t, ipc.InboundMsg{
		Op: ipc.OpInbound, Covered: 1,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 9, Text: "will fail"},
	}))

	if ack, got := readDeliveredAck(t, peer, time.Second); got {
		t.Fatalf("a failed inject must NOT ack (got %+v) — content must stay queued", ack)
	}
	if !a.forwardBlocked.Load() {
		t.Fatal("a failed inject must latch forwardBlocked so a later ack can't consume the undelivered head")
	}
}

// M2 regression (codex parity): an OLDER message whose inject FAILS followed by
// a NEWER message whose inject SUCCEEDS must not produce an ack for the newer
// one — the broker's consume is count-off-HEAD, so acking the newer message
// would Consume the OLDER (undelivered) message off the head → silent loss.
func TestGrokForward_OlderFailLaterSuccess_DoesNotAck(t *testing.T) {
	l := startFakeGrokLeader(t, 1, "forced inject failure")
	a, peer := newForwardTestAdapter(t, l.sock)

	a.handleInbound(inboundRaw(t, ipc.InboundMsg{
		Op: ipc.OpInbound, Covered: 1,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 1, Text: "older"},
	}))
	a.handleInbound(inboundRaw(t, ipc.InboundMsg{
		Op: ipc.OpInbound, Covered: 1,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 2, Text: "newer"},
	}))

	// Wait until the newer message actually landed live, so the only reason for
	// no ack is the latch.
	if text := waitServed(t, l, 5*time.Second); !strings.HasPrefix(text, "newer") {
		t.Fatalf("expected the newer message to be served, got %q", text)
	}
	if ack, got := readDeliveredAck(t, peer, 500*time.Millisecond); got {
		t.Fatalf("no ack may be sent while the older (head) message is undelivered — got %+v", ack)
	}
}

// (b) part 2 — the incident fix: ack suppression must NOT latch for the process
// lifetime. After the agent re-syncs the queue head with a FULL
// fetch_queue(ack=true) drain (clearForwardBlocked), a subsequent successful
// inject must ack again. Under the old permanent latch, every message after one
// hiccup stayed queued and re-delivered wholesale to the next session.
func TestGrokForward_UnlatchAfterFullDrain_AcksResume(t *testing.T) {
	l := startFakeGrokLeader(t, 1, "forced inject failure")
	a, peer := newForwardTestAdapter(t, l.sock)
	acks := startAckReader(peer)

	a.handleInbound(inboundRaw(t, ipc.InboundMsg{
		Op: ipc.OpInbound, Covered: 1,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 1, Text: "older"},
	}))
	a.handleInbound(inboundRaw(t, ipc.InboundMsg{
		Op: ipc.OpInbound, Covered: 1,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 2, Text: "newer"},
	}))
	if text := waitServed(t, l, 5*time.Second); !strings.HasPrefix(text, "newer") {
		t.Fatalf("expected the newer message to be served, got %q", text)
	}
	select {
	case ack := <-acks:
		t.Fatalf("latched: no ack expected yet, got %+v", ack)
	case <-time.After(500 * time.Millisecond):
	}

	// The agent drains the durable queue completely (fetch_queue ack=true,
	// Remaining==0) — the head is re-synced, acks may resume.
	a.clearForwardBlocked()
	if a.forwardBlocked.Load() {
		t.Fatal("clearForwardBlocked must release the latch after a full drain")
	}

	a.handleInbound(inboundRaw(t, ipc.InboundMsg{
		Op: ipc.OpInbound, Covered: 1,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 3, Text: "after drain"},
	}))
	select {
	case ack := <-acks:
		if !ack.OK || ack.UpdateID != 3 || ack.Count != 1 {
			t.Fatalf("ack = %+v, want {OK:true UpdateID:3 Count:1}", ack)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("after a full-drain un-latch, a successful inject must ack again — permanent suppression is the double-delivery bug (task #43)")
	}
}

// (c) Event rendering: an event inbound (here a button callback) must inject its
// REAL payload — actor, callback data, message id — not the literal
// "(empty Telegram message)" that formatInboundTurnText produces for
// payload-less inbounds. Events are never queued broker-side, so no ack either.
func TestGrokForward_EventInbound_RendersEventContent(t *testing.T) {
	l := startFakeGrokLeader(t, 0, "")
	a, peer := newForwardTestAdapter(t, l.sock)

	a.handleInbound(inboundRaw(t, ipc.InboundMsg{
		Op: ipc.OpInbound, Covered: 0, // broker forces Covered=0 for events
		Inbound: c3types.Inbound{
			Channel: "telegram", ChatID: -100, MessageID: 42,
			Kind: c3types.InboundCallback,
			Event: &c3types.InboundEvent{Callback: &c3types.CallbackEvent{
				CallbackID: "cb1", MessageID: 42,
				Actor: c3types.Sender{Username: "maintainer"},
				Data:  "approve",
			}},
		},
	}))

	text := waitServed(t, l, 3*time.Second)
	if strings.Contains(text, "(empty Telegram message)") {
		t.Fatalf("event injected as empty message — payload discarded: %q", text)
	}
	if !strings.Contains(text, `@maintainer pressed a button (data="approve") on message 42`) {
		t.Fatalf("event content missing actor/data/message: %q", text)
	}
	if ack, got := readDeliveredAck(t, peer, 500*time.Millisecond); got {
		t.Fatalf("events cover zero stored lines and must never ack a consume, got %+v", ack)
	}
}

// Pure formatter coverage for the remaining event kinds (poll tally, reaction
// diff, system advisory, unknown fallback) — content parity with the Claude
// adapter's buildEventFrame.
func TestFormatEventTurnText_Kinds(t *testing.T) {
	tid := int64(914)

	poll := c3types.Inbound{
		ChatID: -100, TopicID: &tid, Kind: c3types.InboundPollResult,
		Event: &c3types.InboundEvent{PollResult: &c3types.PollResult{
			PollID: "p1", Question: "lunch?", TotalVoters: 3, IsClosed: true,
			Options: []c3types.PollOptionTally{{Text: "yes", VoterCount: 2}, {Text: "no", VoterCount: 1}},
		}},
	}
	got := formatEventTurnText(&poll)
	for _, want := range []string{`Poll results: "lunch?" — 3 votes`, "yes:2 no:1", "(closed)", "poll_result event", "-100/914"} {
		if !strings.Contains(got, want) {
			t.Fatalf("poll event missing %q in %q", want, got)
		}
	}

	reaction := c3types.Inbound{
		ChatID: -100, Kind: c3types.InboundReaction,
		Event: &c3types.InboundEvent{Reaction: &c3types.ReactionEvent{
			MessageID: 5, Actor: c3types.Sender{UserID: 99}, Added: []string{"👍"}, Removed: []string{"👎"},
		}},
	}
	got = formatEventTurnText(&reaction)
	for _, want := range []string{"user 99 reacted on message 5", "added 👍", "removed 👎"} {
		if !strings.Contains(got, want) {
			t.Fatalf("reaction event missing %q in %q", want, got)
		}
	}

	system := c3types.Inbound{
		Kind: c3types.InboundSystem, // broker-originated: no ChatID
		Event: &c3types.InboundEvent{System: &c3types.SystemEvent{
			Source: "telegram", Level: "warn", Title: "down", Message: "fetch is down",
		}},
	}
	got = formatEventTurnText(&system)
	if !strings.Contains(got, "⚠️ SYSTEM: fetch is down") {
		t.Fatalf("system event missing message: %q", got)
	}
	if strings.Contains(got, "· 0") {
		t.Fatalf("ChatID-less system event must omit the channel suffix: %q", got)
	}

	unknown := c3types.Inbound{ChatID: -100, Kind: "mystery"}
	if got = formatEventTurnText(&unknown); !strings.Contains(got, "(mystery event)") {
		t.Fatalf("unknown event should fall back safely, got %q", got)
	}
}

// clearForwardBlocked must release the latch AND bump the epoch so reqs still
// sitting in forwardCh from the latched era are invalidated (their lines were
// consumed + delivered by the very drain that cleared the latch).
func TestClearForwardBlocked_UnlatchesAndBumpsEpoch(t *testing.T) {
	a := &adapter{forwardCh: make(chan grokForwardReq, 1)}
	a.forwardBlocked.Store(true)
	before := a.forwardEpoch.Load()
	a.clearForwardBlocked()
	if a.forwardBlocked.Load() {
		t.Fatal("clearForwardBlocked must release the latch")
	}
	if a.forwardEpoch.Load() != before+1 {
		t.Fatal("clearForwardBlocked must bump forwardEpoch to invalidate pre-drain reqs")
	}
}

// Wiring: a full toolFetchQueue ack-drain (Ack=true, Remaining==0) must clear
// the latch; a peek (ack=false) consumes nothing and must NOT.
func TestToolFetchQueue_FullDrainClearsLatch_PeekDoesNot(t *testing.T) {
	a, peer := newForwardTestAdapter(t, filepath.Join(t.TempDir(), "missing.sock"))

	serveOne := func(remaining int) {
		raw, err := peer.ReadFrame()
		if err != nil {
			return
		}
		var req ipc.FetchQueueReq
		if json.Unmarshal(raw, &req) != nil {
			return
		}
		resp, _ := json.Marshal(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: req.ID, Remaining: remaining})
		a.dispatchFetchQueueResult(resp)
	}
	call := func(args map[string]any) {
		t.Helper()
		raw, _ := json.Marshal(args)
		res, err := a.toolFetchQueue(context.Background(), &mcp.CallToolRequest{
			Params: &mcp.CallToolParamsRaw{Arguments: raw},
		})
		if err != nil || res.IsError {
			t.Fatalf("toolFetchQueue: err=%v res=%+v", err, res)
		}
	}

	// Peek (ack=false) with an empty queue: must NOT clear the latch.
	a.forwardBlocked.Store(true)
	go serveOne(0)
	call(map[string]any{"limit": "all", "ack": false})
	if !a.forwardBlocked.Load() {
		t.Fatal("a peek (ack=false) consumes nothing and must not clear forwardBlocked")
	}

	// Partial drain (Remaining>0): the gap may still be queued — stay latched.
	go serveOne(4)
	call(map[string]any{"limit": float64(3)})
	if !a.forwardBlocked.Load() {
		t.Fatal("a partial drain (Remaining>0) must not clear forwardBlocked")
	}

	// Full drain (ack=true default, Remaining==0): head re-synced — un-latch.
	go serveOne(0)
	call(map[string]any{"limit": "all"})
	if a.forwardBlocked.Load() {
		t.Fatal("a full ack=true drain (Remaining==0) must clear forwardBlocked so live acks resume")
	}
}

// Transient-vs-fatal classification for the busy-retry loop: mid-turn phrases
// retry; anything else (including the incident's post-write failures) fails
// fast so the durable queue keeps the message.
func TestIsTransientInjectErr(t *testing.T) {
	transient := []string{
		"TurnInFlight: a turn is already running",
		"session/prompt: turn in flight",
		"agent busy, try again",
		"previous turn has not finished",
	}
	for _, s := range transient {
		if !isTransientInjectErr(errFromString(s)) {
			t.Fatalf("%q should classify transient (retry)", s)
		}
	}
	fatal := []string{
		"session/prompt: forced inject failure",
		"dial unix /tmp/leader.sock: no such file",
		"context deadline exceeded",
	}
	for _, s := range fatal {
		if isTransientInjectErr(errFromString(s)) {
			t.Fatalf("%q should classify fatal (fail fast)", s)
		}
	}
	if isTransientInjectErr(nil) {
		t.Fatal("nil error is not transient")
	}
}

func errFromString(s string) error { return &stringError{s} }

type stringError struct{ s string }

func (e *stringError) Error() string { return e.s }

// Busy-retry: a transient (mid-turn) failure followed by success must succeed
// overall — one retry, prompt delivered exactly once.
func TestInjectWithRetry_TransientThenSuccess(t *testing.T) {
	// Shorten the backoff schedule (vars exist for exactly this; production
	// never reassigns them). No other test's forward loop is mid-retry here —
	// non-transient failures in the other tests fail fast without reading these.
	oldBase, oldMax := injectRetryBaseWait, injectRetryMaxWait
	injectRetryBaseWait, injectRetryMaxWait = 10*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { injectRetryBaseWait, injectRetryMaxWait = oldBase, oldMax })

	l := startFakeGrokLeader(t, 1, "turn in flight — try again")
	a, _ := newForwardTestAdapter(t, l.sock)

	if err := a.injectWithRetry(context.Background(), "retry me", 11); err != nil {
		t.Fatalf("transient-then-success must succeed, got %v", err)
	}
	if got := l.promptAttempts(); got != 2 {
		t.Fatalf("prompt attempts = %d, want 2 (one transient failure + one success)", got)
	}
	if text := waitServed(t, l, time.Second); text != "retry me" {
		t.Fatalf("served prompt = %q, want %q", text, "retry me")
	}
}

// readRecoverReq reads one frame off the broker-side conn and parses it as a
// RecoverSessionReq (same timeout pattern as readDeliveredAck).
func readRecoverReq(t *testing.T, c *ipc.Conn, within time.Duration) (ipc.RecoverSessionReq, bool) {
	t.Helper()
	type res struct {
		raw []byte
		err error
	}
	ch := make(chan res, 1)
	go func() {
		raw, err := c.ReadFrame()
		ch <- res{raw, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return ipc.RecoverSessionReq{}, false
		}
		var m ipc.RecoverSessionReq
		if err := json.Unmarshal(r.raw, &m); err != nil {
			t.Fatalf("unmarshal recover req: %v", err)
		}
		return m, true
	case <-time.After(within):
		return ipc.RecoverSessionReq{}, false
	}
}

// Once-per-connection guard: with recoverFired already set, a direct fireRecover
// must not write a second RecoverSessionReq on the same connection.
func TestFireRecover_GuardBlocksSecondFireOnSameConnection(t *testing.T) {
	a, peer := newForwardTestAdapter(t, filepath.Join(t.TempDir(), "missing.sock"))
	a.recoverFired.Store(true)

	go a.fireRecover(context.Background(), "sess-test", t.TempDir())

	if req, got := readRecoverReq(t, peer, 300*time.Millisecond); got {
		t.Fatalf("recoverFired guard must suppress a duplicate fire on one connection, got %+v", req)
	}
}

// Broker-restart refire (§3d2): refireRecoverOnReconnect must demote the guard
// from once-per-process to once-per-connection — reset it and re-send
// RecoverSessionReq with the stable session id so a FRESH broker re-learns the
// sid and post-restart attaches are recorded for future resume.
func TestRefireRecoverOnReconnect_ResetsGuardAndRefires(t *testing.T) {
	a, peer := newForwardTestAdapter(t, filepath.Join(t.TempDir(), "missing.sock"))
	a.recoverFired.Store(true) // simulate: already fired on the OLD connection

	a.refireRecoverOnReconnect(context.Background())

	req, got := readRecoverReq(t, peer, 3*time.Second)
	if !got {
		t.Fatal("reconnect must re-fire RecoverSessionReq — without it a fresh broker never learns the stable id and resume silently breaks")
	}
	if req.Op != ipc.OpRecoverSession || req.StableSessionID != "sess-test" {
		t.Fatalf("recover req = %+v, want OpRecoverSession with stable id %q", req, "sess-test")
	}
	// Resolve the in-flight fireRecover (as brokerReader would) so its goroutine
	// doesn't idle out on the 8s response timeout.
	resp, _ := json.Marshal(ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult})
	a.dispatchRecoverSessionResult(resp)

	if !a.recoverFired.Load() {
		t.Fatal("the refire must re-arm the once-per-connection guard")
	}
}
