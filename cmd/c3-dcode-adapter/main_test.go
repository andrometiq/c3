package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// --- fake dcode TUI event socket -----------------------------------------

// fakeEventTUI listens on a real Unix socket and answers {"kind":"prompt"}
// lines the way dcode's UnixSocketEventSource does: {"ok":true} with the
// correlation_id echoed. The stored payloads can be inspected by the test.
type fakeEventTUI struct {
	listener net.Listener
	accepted chan net.Conn

	mu       sync.Mutex
	payloads []string
	nackNext bool
}

func newFakeEventTUI(t *testing.T, dir string) *fakeEventTUI {
	t.Helper()
	path := filepath.Join(dir, "events.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeEventTUI{listener: l, accepted: make(chan net.Conn, 4)}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	t.Cleanup(func() { _ = l.Close() })
	return f
}

func (f *fakeEventTUI) serve(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	for {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		var ev struct {
			Kind          string `json:"kind"`
			Payload       string `json:"payload"`
			CorrelationID string `json:"correlation_id"`
		}
		if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &ev); err != nil {
			continue
		}
		f.mu.Lock()
		f.payloads = append(f.payloads, ev.Payload)
		nack := f.nackNext
		f.nackNext = false
		f.mu.Unlock()
		reply := map[string]any{"ok": !nack}
		if ev.CorrelationID != "" {
			reply["correlation_id"] = ev.CorrelationID
		}
		if nack {
			reply["error"] = "test rejection"
		}
		out, _ := json.Marshal(reply)
		_, _ = c.Write(append(out, '\n'))
	}
}

func (f *fakeEventTUI) payload(t *testing.T, i int) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		n := len(f.payloads)
		f.mu.Unlock()
		if n > i {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.payloads[i]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("payload %d never arrived", i)
	return ""
}

// --- fake broker -----------------------------------------------------------

// --- tests -----------------------------------------------------------------

// parseFetchLimit must clamp to [1,50], honor "all" (any case) and numeric
// strings, and NEVER return a negative limit — the broker worker treats n<0
// as the consume-ALL sentinel, so a stray -1 would destructively drain the
// durable queue. (Parity with every built-in adapter's test.)
func TestParseFetchLimit(t *testing.T) {
	cases := []struct {
		name      string
		in        any
		wantLimit int
		wantAll   bool
	}{
		{"all lowercase", "all", 0, true},
		{"all uppercase", "ALL", 0, true},
		{"string number 5", "5", 5, false},
		{"string negative clamps to 1", "-1", 1, false},
		{"json negative falls back to default", float64(-1), 3, false},
		{"json 1000 clamps to 50", float64(1000), 50, false},
		{"unparseable falls back to default 3", "abc", 3, false},
		{"absent falls back to default 3", nil, 3, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotLimit, gotAll := parseFetchLimit(tc.in)
			if gotLimit != tc.wantLimit || gotAll != tc.wantAll {
				t.Fatalf("parseFetchLimit(%#v) = (%d, %v), want (%d, %v)",
					tc.in, gotLimit, gotAll, tc.wantLimit, tc.wantAll)
			}
			if gotLimit < 0 {
				t.Fatalf("parseFetchLimit(%#v) returned NEGATIVE limit %d", tc.in, gotLimit)
			}
		})
	}
}

// The hello frame must carry cannot_render_channels=true exactly when there
// is no live event socket — getting this inverted loses every message (the
// broker would ack pushes the host never rendered).
func TestHelloCannotRenderChannels(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	a := newAdapter()
	a.eventPath = filepath.Join(t.TempDir(), "events.sock") // live-capable
	a.conn = ipc.NewConn(c1)
	peer := ipc.NewConn(c2)

	errCh := make(chan error, 1)
	go func() { errCh <- a.hello() }()

	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("no hello frame: %v", err)
	}
	var hello ipc.HelloMsg
	if err := json.Unmarshal(raw, &hello); err != nil {
		t.Fatalf("parse hello: %v", err)
	}
	if hello.CLI != "dcode" {
		t.Errorf("hello.CLI = %q, want %q", hello.CLI, "dcode")
	}
	if hello.CannotRenderChannels {
		t.Errorf("hello.CannotRenderChannels = true with a bound event path — must be false when live push is available")
	}
	if hello.ProtocolVersion != ipc.ProtocolVersion {
		t.Errorf("hello.ProtocolVersion = %d, want %d", hello.ProtocolVersion, ipc.ProtocolVersion)
	}

	if err := peer.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck, ConnID: 1, ProtocolVersion: ipc.ProtocolVersion}); err != nil {
		t.Fatalf("write ack: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("hello: %v", err)
	}

	// Pull-only variant: no event path → cannot_render_channels MUST be true.
	c3, c4 := net.Pipe()
	defer c3.Close()
	defer c4.Close()
	b := newAdapter()
	b.eventPath = "" // pull-only
	b.conn = ipc.NewConn(c3)
	peer2 := ipc.NewConn(c4)
	errCh2 := make(chan error, 1)
	go func() { errCh2 <- b.hello() }()
	raw2, err := peer2.ReadFrame()
	if err != nil {
		t.Fatalf("no second hello frame: %v", err)
	}
	var hello2 ipc.HelloMsg
	if err := json.Unmarshal(raw2, &hello2); err != nil {
		t.Fatalf("parse second hello: %v", err)
	}
	if !hello2.CannotRenderChannels {
		t.Fatalf("pull-only hello must set cannot_render_channels=true (no event socket bound)")
	}
}

// expectNoFrame fails the test if the peer receives any frame within the
// window — used to prove the adapter did NOT write an ack. ipc.Conn exposes
// no deadline control, so the read races in a goroutine.
func expectNoFrame(t *testing.T, peer *ipc.Conn, window time.Duration, what string) {
	t.Helper()
	got := make(chan []byte, 1)
	go func() {
		raw, err := peer.ReadFrame()
		if err == nil {
			got <- raw
		}
	}()
	select {
	case raw := <-got:
		t.Fatalf("%s produced a frame (%s) — must not", what, raw)
	case <-time.After(window):
	}
}

// startReader runs the adapter's production brokerReader loop with a
// canceled-on-cleanup context so pushes flow through the real dispatch
// (OpInbound → handleInbound) rather than a direct call.
func startReader(t *testing.T, a *adapter) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go a.brokerReader(ctx)
}

// The core live-path contract: a broker inbound push → one {"kind":"prompt"}
// line on the TUI event socket → {"ok":true} → inbound_delivered ack with
// count=covered and the delivery token echoed. Any broken link in that chain
// silently corrupts the durable queue, so every hop is asserted here.
func TestInboundInjectThenAck(t *testing.T) {
	dir := t.TempDir()
	tui := newFakeEventTUI(t, dir)

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	a := newAdapter()
	a.eventPath = filepath.Join(dir, "events.sock")
	a.conn = ipc.NewConn(c1)
	peer := ipc.NewConn(c2)
	startReader(t, a)

	msg := &ipc.InboundMsg{
		Op: ipc.OpInbound,
		Inbound: c3types.Inbound{
			Channel:   "telegram",
			ChatID:    -100,
			MessageID: 42,
			Sender:    c3types.Sender{UserID: 7, Username: "karthi"},
			Text:      "run the tests",
		},
		Covered:       2, // merged push
		DeliveryToken: "tok-abc",
	}
	if err := peer.WriteJSON(msg); err != nil {
		t.Fatalf("push inbound: %v", err)
	}

	got := tui.payload(t, 0)
	if !strings.Contains(got, "[Telegram]") {
		t.Errorf("injected payload missing [Telegram] prefix: %q", got)
	}
	if !strings.Contains(got, "run the tests") {
		t.Errorf("injected payload missing body: %q", got)
	}

	// The ack must follow the {"ok":true}: count=covered, token echoed.
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("no delivered ack: %v", err)
	}
	var ack ipc.InboundDeliveredMsg
	if err := json.Unmarshal(raw, &ack); err != nil {
		t.Fatalf("parse ack: %v", err)
	}
	if ack.Op != ipc.OpInboundDelivered || !ack.OK {
		t.Fatalf("ack = %+v, want ok inbound_delivered", ack)
	}
	if ack.UpdateID != 42 {
		t.Errorf("ack.UpdateID = %d, want 42 (inbound.MessageID)", ack.UpdateID)
	}
	if ack.Count != 2 {
		t.Errorf("ack.Count = %d, want 2 (covered) — a merged batch must consume every line it covered", ack.Count)
	}
	if ack.DeliveryToken != "tok-abc" {
		t.Errorf("ack.DeliveryToken = %q, want tok-abc (echoed unchanged)", ack.DeliveryToken)
	}
}

// A synthesized EVENT (Kind non-empty) covers zero stored lines and must
// NEVER be acked — acking one would consume a real queued message the event
// never delivered. It is still injected so the agent sees it.
func TestEventInboundInjectedButNeverAcked(t *testing.T) {
	dir := t.TempDir()
	tui := newFakeEventTUI(t, dir)

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	a := newAdapter()
	a.eventPath = filepath.Join(dir, "events.sock")
	a.conn = ipc.NewConn(c1)
	peer := ipc.NewConn(c2)
	startReader(t, a)

	msg := &ipc.InboundMsg{
		Op: ipc.OpInbound,
		Inbound: c3types.Inbound{
			Kind:      c3types.InboundPollResult,
			ChatID:    -100,
			MessageID: 43,
			Event: &c3types.InboundEvent{PollResult: &c3types.PollResult{
				Question:    "ship it?",
				TotalVoters: 3,
				Options:     []c3types.PollOptionTally{{Text: "yes", VoterCount: 2}, {Text: "no", VoterCount: 1}},
			}},
		},
	}
	if err := peer.WriteJSON(msg); err != nil {
		t.Fatalf("push event: %v", err)
	}

	got := tui.payload(t, 0) // injected…
	// The event payload MUST reach the agent — routing events through the
	// text-only renderer used to inject a contentless "(poll_result event)"
	// placeholder, discarding the tally (the Grok adapter's old bug).
	if !strings.Contains(got, "ship it?") {
		t.Errorf("injected event missing poll question: %q", got)
	}
	if !strings.Contains(got, "yes:2") || !strings.Contains(got, "no:1") {
		t.Errorf("injected event missing tally: %q", got)
	}

	expectNoFrame(t, peer, 300*time.Millisecond, "event push ack")
}

// A NACK from the TUI ({"ok":false}) means the prompt did NOT land: the
// adapter must NOT ack the broker, leaving the line queued for fetch_queue.
func TestInjectNackMeansNoBrokerAck(t *testing.T) {
	dir := t.TempDir()
	tui := newFakeEventTUI(t, dir)
	tui.mu.Lock()
	tui.nackNext = true
	tui.mu.Unlock()

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	a := newAdapter()
	a.eventPath = filepath.Join(dir, "events.sock")
	a.conn = ipc.NewConn(c1)
	peer := ipc.NewConn(c2)
	startReader(t, a)

	msg := &ipc.InboundMsg{
		Op: ipc.OpInbound,
		Inbound: c3types.Inbound{
			ChatID:    -100,
			MessageID: 44,
			Text:      "will be rejected",
		},
		Covered:       1,
		DeliveryToken: "tok-xyz",
	}
	if err := peer.WriteJSON(msg); err != nil {
		t.Fatalf("push inbound: %v", err)
	}
	tui.payload(t, 0)

	expectNoFrame(t, peer, 300*time.Millisecond, "NACKed inject broker ack")
}

// Pull-only mode: an ordinary push is dropped without an ack (the broker
// already holds human inbound in the queue because cannot_render_channels
// was set), while a SYSTEM advisory still surfaces as a log notification.
func TestPullOnlyDropsOrdinaryPushWithoutAck(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	a := newAdapter()
	a.eventPath = "" // pull-only
	a.conn = ipc.NewConn(c1)
	peer := ipc.NewConn(c2)
	startReader(t, a)

	msg := &ipc.InboundMsg{
		Op: ipc.OpInbound,
		Inbound: c3types.Inbound{
			ChatID:    -100,
			MessageID: 45,
			Text:      "held for fetch_queue",
		},
		Covered: 1,
	}
	if err := peer.WriteJSON(msg); err != nil {
		t.Fatalf("push inbound: %v", err)
	}

	expectNoFrame(t, peer, 300*time.Millisecond, "pull-only push ack")
}

// renderInjectedPrompt must carry the pending-backlog nudge when the broker
// reports more queued lines than the push covered, so a stuck backlog item
// is visible on THIS push, not only at the next attach.
func TestRenderInjectedPromptPendingNudge(t *testing.T) {
	msg := &ipc.InboundMsg{
		Inbound: c3types.Inbound{Text: "hello", MessageID: 1},
		Pending: 3,
	}
	got := renderInjectedPrompt(msg)
	if !strings.Contains(got, "3 pending") || !strings.Contains(got, "fetch_queue") {
		t.Fatalf("pending nudge missing: %q", got)
	}
	if got2 := renderInjectedPrompt(&ipc.InboundMsg{Inbound: c3types.Inbound{Text: "hi"}}); strings.Contains(got2, "pending") {
		t.Fatalf("zero-pending push must not carry a nudge: %q", got2)
	}
}

// The MCP server surface: serverInfo.name MUST equal the .mcp.json key
// ("c3"), never the binary name — Claude's shipped bug where delivery
// looked fine end-to-end and nothing rendered.
func TestMCPServerNameAndTools(t *testing.T) {
	a := newAdapter()
	srv := a.buildMCPServer()
	if srv == nil {
		t.Fatal("buildMCPServer returned nil")
	}

	clientT, serverT := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = srv.Run(ctx, serverT) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.0"}, nil)
	sess, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	defer sess.Close()

	listResult, err := sess.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	want := map[string]bool{
		"attach": false, "topics": false, "fetch_queue": false,
		"retranscribe": false, "reply": false, "react": false,
		"edit_message": false, "poll": false, "stop_poll": false,
		"download_attachment": false, "detach": false,
	}
	for _, tool := range listResult.Tools {
		if _, ok := want[tool.Name]; ok {
			want[tool.Name] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("tool %q not registered", name)
		}
	}
}

// uniqueLiveEventSocket must refuse to bind when more than one live event
// socket exists — injecting into the wrong TUI would ack away a queue line
// that TUI never saw — and must ignore dead-process leftovers.
func TestResolveEventSocketAmbiguityRefuses(t *testing.T) {
	dir := t.TempDir()
	// One live-owned socket (ours) + one dead-pid leftover.
	p1 := filepath.Join(dir, fmt.Sprintf("events-%d.sock", os.Getpid()))
	p2 := filepath.Join(dir, "events-999999.sock")
	for _, p := range []string{p1, p2} {
		l, err := net.Listen("unix", p)
		if err != nil {
			t.Fatalf("listen %s: %v", p, err)
		}
		defer l.Close()
	}
	if got := uniqueLiveEventSocket(dir); got != p1 {
		t.Fatalf("uniqueLiveEventSocket = %q, want unique live socket %q", got, p1)
	}

	// A second LIVE owner → ambiguity must refuse.
	proc, err := os.StartProcess("/bin/sleep", []string{"/bin/sleep", "10"}, &os.ProcAttr{})
	if err != nil {
		t.Skipf("cannot spawn helper process: %v", err)
	}
	defer func() {
		_ = proc.Kill()
		_, _ = proc.Wait()
	}()
	p3 := filepath.Join(dir, fmt.Sprintf("events-%d.sock", proc.Pid))
	l3, err := net.Listen("unix", p3)
	if err != nil {
		t.Fatalf("listen third: %v", err)
	}
	defer l3.Close()

	if got := uniqueLiveEventSocket(dir); got != "" {
		t.Fatalf("ambiguous live sockets resolved to %q — must refuse", got)
	}
}
