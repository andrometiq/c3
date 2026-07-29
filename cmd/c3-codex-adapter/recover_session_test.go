package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/ipc"
)

// These tests guard the identity rules the Codex recover path exists to obey:
//
//	(a) an identity that cannot be determined must never match another one, so
//	    an ambiguous answer recovers NOTHING;
//	(b) identity is settled before anything depending on it is answered.
//
// They drive the real entry point — brokerReader → ensureSessionRecoverForConn →
// startSessionRecover — against a fake Codex app-server and a piped broker, so
// deleting any single line of the production path (the kickoff in brokerReader,
// the ambiguity refusal, the response dispatch, the attach gate) fails one of
// them.

// fakeCodexAppServer stands up an app-server that reports `loaded` from
// thread/loaded/list. When gate is non-nil the answer is withheld until gate is
// closed, which lets a test hold this session's identity unresolved on purpose.
//
// loaded is []any, not []string, so a test can report what a real app-server
// could actually put on the wire — a null, an object — and not just the
// well-formed case the adapter hopes for.
func fakeCodexAppServer(t *testing.T, gate <-chan struct{}, loaded []any) string {
	t.Helper()
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			var msg map[string]any
			if err := c.ReadJSON(&msg); err != nil {
				return
			}
			id, hasID := msg["id"]
			if !hasID {
				continue
			}
			result := map[string]any{"ok": true}
			if method, _ := msg["method"].(string); method == "thread/loaded/list" {
				if gate != nil {
					<-gate
				}
				result = map[string]any{"data": loaded}
			}
			if err := c.WriteJSON(map[string]any{"id": id, "result": result}); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + srv.URL[len("http"):]
}

// recoverTestAdapter wires an adapter to a fake app-server and a piped broker,
// and returns the BROKER-side conn (what the broker would read and write) plus
// the adapter's lifetime ctx.
//
// Cleanup cancels ctx BEFORE closing the pipe: brokerReader treats a read error
// as a broker bounce and would otherwise dial — or spawn — the real broker.
func recoverTestAdapter(t *testing.T, wsURL string) (*adapter, *brokerPeer, context.Context) {
	t.Helper()
	t.Setenv("C3_CODEX_APP_SERVER_WS", wsURL)
	t.Setenv("C3_CODEX_CWD", "/w/proj")
	t.Setenv("C3_CODEX_THREAD_ID", "") // no operator pin: resolution must come from the app-server

	a := newAdapter()
	adapterSide, brokerSide := net.Pipe()
	a.bmu.Lock()
	a.conn = ipc.NewConn(adapterSide)
	a.bmu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = adapterSide.Close()
		_ = brokerSide.Close()
	})
	return a, newBrokerPeer(brokerSide), ctx
}

// brokerPeer is the broker's side of the pipe. Frames are drained by ONE
// goroutine into a buffered channel: a per-read goroutine would leave orphan
// readers behind on every "assert nothing was sent" check, and those orphans
// would then steal the frames a later assertion is waiting for.
type brokerPeer struct {
	conn   *ipc.Conn
	frames chan []byte
}

func newBrokerPeer(rw net.Conn) *brokerPeer {
	p := &brokerPeer{conn: ipc.NewConn(rw), frames: make(chan []byte, 16)}
	go func() {
		for {
			raw, err := p.conn.ReadFrame()
			if err != nil {
				close(p.frames)
				return
			}
			p.frames <- raw
		}
	}()
	return p
}

func (p *brokerPeer) WriteJSON(v any) error { return p.conn.WriteJSON(v) }

// next returns the next frame the adapter sent, or reports that none came.
func (p *brokerPeer) next(t *testing.T, within time.Duration) ([]byte, bool) {
	t.Helper()
	select {
	case raw, ok := <-p.frames:
		return raw, ok
	case <-time.After(within):
		return nil, false
	}
}

func frameOp(t *testing.T, raw []byte) ipc.Op {
	t.Helper()
	op, err := ipc.PeekOp(raw)
	if err != nil {
		t.Fatalf("unreadable frame %q: %v", raw, err)
	}
	return op
}

// A resumed Codex conversation must identify itself to the broker by the thread
// id the app-server reports, so the broker can key its attachment record on
// something that survives a resume. Nothing else in the adapter's environment
// names a conversation, so if this frame is not sent — or is sent with the wrong
// value — Codex silently goes back to having no attach identity at all.
func TestSessionRecover_RegistersThisSessionsCodexThreadID(t *testing.T) {
	ws := fakeCodexAppServer(t, nil, []any{"019f9552-33a6-7ae2-a70e-f1fc7f5f3e39"})
	a, broker, ctx := recoverTestAdapter(t, ws)

	go a.brokerReader(ctx)

	raw, ok := broker.next(t, 5*time.Second)
	if !ok {
		t.Fatal("the adapter never sent a recover_session frame: a resumed Codex session has no identity to " +
			"re-attach with, so it comes back unattached and its held messages stay unread")
	}
	var req ipc.RecoverSessionReq
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatalf("unmarshal recover frame: %v", err)
	}
	if req.Op != ipc.OpRecoverSession {
		t.Fatalf("first frame op = %q, want %q — recovery must be the first thing this session does",
			req.Op, ipc.OpRecoverSession)
	}
	if req.StableSessionID != "019f9552-33a6-7ae2-a70e-f1fc7f5f3e39" {
		t.Fatalf("stable_session_id = %q, want the app-server's loaded thread id. An id that is not Codex's own "+
			"thread does not survive a resume, so the next launch recovers nothing", req.StableSessionID)
	}
	if req.CWD != "/w/proj" {
		t.Errorf("cwd = %q, want the session's cwd", req.CWD)
	}
}

// RULE (a). Codex runs one adapter per THREAD — including transient sub-agent
// threads — so an adapter can find several threads loaded on its app-server with
// nothing marking which one it was spawned for. Picking one (the forwarder's
// delivery-path fallback does exactly that: forwarder.go discoverThread returns
// loaded[0]) would bind this session to another session's topic and drain its
// queue. The only safe answer is to recover nothing.
func TestSessionRecover_RefusesToGuessWhenSeveralThreadsAreLoaded(t *testing.T) {
	ws := fakeCodexAppServer(t, nil, []any{"thread-a", "thread-b"})
	a, broker, ctx := recoverTestAdapter(t, ws)

	go a.brokerReader(ctx)

	if raw, ok := broker.next(t, 2*time.Second); ok {
		t.Fatalf("the adapter claimed an identity it could not determine (sent %s). Two loaded threads and no "+
			"discriminator means an unknown identity was matched to a session's topic — the mis-bind that "+
			"drains someone else's queue", raw)
	}
	a.tidmu.Lock()
	cached := a.threadID
	a.tidmu.Unlock()
	if cached != "" {
		t.Fatalf("an ambiguous resolution was cached as this session's identity (%q); every later reconnect "+
			"would re-assert the guess", cached)
	}
}

// RULE (a), the other unknown: no thread loaded is not "probably the last one".
func TestSessionRecover_RefusesWhenNoThreadIsLoaded(t *testing.T) {
	ws := fakeCodexAppServer(t, nil, nil)
	a, broker, ctx := recoverTestAdapter(t, ws)

	go a.brokerReader(ctx)

	if raw, ok := broker.next(t, 2*time.Second); ok {
		t.Fatalf("the adapter sent an identity with no loaded thread to back it (%s)", raw)
	}
}

// RULE (a), the unknown that LOOKS known. A loaded-list entry the app-server did
// not name as a string (a null, an object) must not be rendered into an id.
// Rendering one produces a CONSTANT — Go prints a nil as "<nil>", an object as
// "map[]" — and a constant is the same in every session that hits the same
// malformation. Both are non-empty, so they sail past this adapter's empty-id
// guard and the broker's (internal/broker/handler.go rejects only an EMPTY
// stable id), and the broker then keys an attachment record on an identity two
// unrelated sessions share: one resumes and re-claims the other's topic, taking
// its held backlog with it. Drives the real path end to end.
func TestSessionRecover_RefusesAnIdentityTheAppServerNeverNamed(t *testing.T) {
	ws := fakeCodexAppServer(t, nil, []any{nil})
	a, broker, ctx := recoverTestAdapter(t, ws)

	go a.brokerReader(ctx)

	if raw, ok := broker.next(t, 2*time.Second); ok {
		t.Fatalf("the adapter minted a session identity out of a loaded entry that is not a thread id (sent %s). "+
			"Formatting an unreadable entry yields the same constant in every session that hits it, so two "+
			"unknown identities compare EQUAL and one session recovers the other's topic", raw)
	}
	a.tidmu.Lock()
	cached := a.threadID
	a.tidmu.Unlock()
	if cached != "" {
		t.Fatalf("a formatted non-id (%q) was cached as this session's identity; every reconnect re-asserts it", cached)
	}
}

// The variants of the same defect, each of which yields a DIFFERENT plausible
// constant, so a fix that special-cases only one of them is still broken.
func TestResolveCodexThreadID_RejectsEntriesThatAreNotThreadIDs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry any
		mints string // what fmt.Sprint would have produced
	}{
		{"a null", nil, `"<nil>"`},
		{"an object", map[string]any{}, `"map[]"`},
		{"a number", float64(42), `"42"`},
		{"an empty string", "", `""`},
		{"a blank string", "   ", `"   "`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ws := fakeCodexAppServer(t, nil, []any{tc.entry})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			got, err := resolveCodexThreadID(ctx, codexForwardConfig{WSURL: ws, Timeout: 3 * time.Second})
			if err == nil {
				t.Fatalf("resolved %q as this session's identity from a loaded entry that is %s. It would have "+
					"been sent as stable_session_id and shared with every other session whose app-server "+
					"reported the same thing", got, tc.name)
			}
			if !errors.Is(err, errCodexLoadedListUnreadable) {
				t.Errorf("refused with %v, want errCodexLoadedListUnreadable so the log names the real cause", err)
			}
			if got != "" {
				t.Errorf("refused but still returned %q (fmt.Sprint would mint %s); a caller that ignores the "+
					"error would send it", got, tc.mints)
			}
		})
	}
}

// A list that is only PARTLY readable is still ambiguous: the entry that could
// not be read might be this session's own thread, so narrowing to the readable
// remainder is the same guess wearing a different hat.
func TestResolveCodexThreadID_APartlyUnreadableListIsNotNarrowedToTheReadablePart(t *testing.T) {
	ws := fakeCodexAppServer(t, nil, []any{"019f9552-33a6-7ae2-a70e-f1fc7f5f3e39", nil})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got, err := resolveCodexThreadID(ctx, codexForwardConfig{WSURL: ws, Timeout: 3 * time.Second})
	if err == nil {
		t.Fatalf("picked %q out of a list with an unreadable entry beside it; the unreadable one may be this "+
			"session's own thread, so this claims an identity that belongs to another session", got)
	}
}

// A `data` that is not a list at all is a MALFORMED answer, not an empty one.
// Both refuse to recover, so the safety outcome is identical — what differs is
// what the operator is sent to debug: "no loaded thread yet" points at a Codex
// that has not opened a conversation, when the real fault is an app-server
// answering with something this adapter cannot read. The two must stay
// distinguishable.
func TestLoadedThreadIDs_AGarbledListIsReportedAsGarbledNotAsEmpty(t *testing.T) {
	ids, err := loadedThreadIDs("not-a-list")
	if err == nil {
		t.Fatalf("a non-list `data` was accepted as %v, so a garbled app-server response is reported as "+
			"'no thread loaded yet' — the wrong problem to go and debug", ids)
	}
	if !errors.Is(err, errCodexLoadedListUnreadable) {
		t.Errorf("reported %v, want errCodexLoadedListUnreadable", err)
	}
	// ...and the genuinely-empty answer must NOT be dressed up as a malformed one.
	if ids, err := loadedThreadIDs(nil); err != nil || len(ids) != 0 {
		t.Errorf("a missing `data` gave (%v, %v), want no candidates and no error so a session that simply has "+
			"no conversation open is reported as exactly that", ids, err)
	}
}

// An operator pin that is only whitespace is not an identity — it is an unset
// variable with a typo, and every session that mistypes it the same way would
// share it.
func TestResolveCodexThreadID_ABlankOperatorPinIsNotAnIdentity(t *testing.T) {
	ws := fakeCodexAppServer(t, nil, []any{"thread-a", "thread-b"})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got, err := resolveCodexThreadID(ctx, codexForwardConfig{WSURL: ws, ThreadID: "   ", Timeout: 3 * time.Second})
	if err == nil {
		t.Fatalf("a whitespace-only C3_CODEX_THREAD_ID was accepted as the identity %q", got)
	}
}

// An adapter running outside the c3 launcher has no app-server to ask. It must
// stay quiet rather than invent an id — and, critically, must still settle the
// identity question so `attach` is not blocked behind a resolution that will
// never come.
func TestSessionRecover_WithoutAnAppServerStaysQuietAndStillSettles(t *testing.T) {
	a, broker, ctx := recoverTestAdapter(t, "")
	t.Setenv("C3_CODEX_APP_SERVER_WS", "")

	go a.brokerReader(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for !a.recoverStarted() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !a.recoverStarted() {
		t.Fatal("brokerReader never started session recovery")
	}
	gate := a.identityGate()
	if raw, ok := broker.next(t, 1*time.Second); ok {
		t.Fatalf("an adapter with no app-server still asserted an identity (%s)", raw)
	}
	select {
	case <-gate:
	case <-time.After(2 * time.Second):
		t.Fatal("identity never settled with no app-server configured — every attach would stall for the full " +
			"settle timeout before doing anything")
	}
}

// A recovered route must be remembered ADDRESSED BY IDENTITY (topic id + group),
// not by name: replaying an attach by name cannot re-claim across groups, so a
// broker restart after a resume would silently drop the claim. This also pins
// that the broker's response is routed back at all — the recover_session_result
// arm of brokerReader.
func TestSessionRecover_RecoveredRouteIsRememberedByIdentityNotName(t *testing.T) {
	ws := fakeCodexAppServer(t, nil, []any{"thread-1"})
	a, broker, ctx := recoverTestAdapter(t, ws)

	go a.brokerReader(ctx)

	if _, ok := broker.next(t, 5*time.Second); !ok {
		t.Fatal("no recover_session frame to answer")
	}
	topicID := int64(281)
	if err := broker.WriteJSON(ipc.RecoverSessionResp{
		Op: ipc.OpRecoverSessionResult, Recovered: true,
		Channel: "telegram", ChatID: -1001, TopicID: &topicID,
		Name: "proj", Group: "work", QueuedCount: 2,
	}); err != nil {
		t.Fatalf("write recover response: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for a.currentTopicName() == "" && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if got := a.currentTopicName(); got != "proj" {
		t.Fatalf("attached topic = %q, want %q — the recovered route was never taken up, so the session does "+
			"not know which topic its messages belong to", got, "proj")
	}
	a.amu.Lock()
	remembered := a.lastAttach
	a.amu.Unlock()
	if remembered == nil {
		t.Fatal("the recovered route was not remembered for reconnect replay: a broker restart would drop the claim")
	}
	if remembered.TopicID == nil || *remembered.TopicID != topicID || remembered.Group != "work" || remembered.ChatID != -1001 {
		t.Fatalf("remembered attach = %+v, want the route addressed by {topic_id, group, chat_id}", remembered)
	}
	if remembered.Name != "" {
		t.Fatalf("the recovered route was remembered by NAME (%q); a name replay cannot re-claim across groups "+
			"and silently loses the claim on a fresh broker", remembered.Name)
	}
}

// RULE (b). While identity is still being resolved, an attach must not be
// answered: the in-flight recovery may be about to re-claim this session's own
// topic, and an attach that races it decides the route on incomplete
// information. The recover frame has to reach the broker FIRST.
func TestToolAttach_WaitsUntilThisSessionsIdentityIsSettled(t *testing.T) {
	gate := make(chan struct{})
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(gate) }) }
	// Any failure path must release the app-server handler too, or httptest's
	// Close waits forever on it and the failure presents as a hung test.
	defer openGate()

	ws := fakeCodexAppServer(t, gate, []any{"thread-1"})
	a, broker, ctx := recoverTestAdapter(t, ws)

	go a.brokerReader(ctx)
	// Let brokerReader start the (now-blocked) resolution before the attach.
	deadline := time.Now().Add(2 * time.Second)
	for !a.recoverStarted() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !a.recoverStarted() {
		t.Fatal("brokerReader never started session recovery")
	}

	attachDone := make(chan struct{})
	go func() {
		defer close(attachDone)
		_, _ = a.toolAttach(ctx, newAttachReq(t, map[string]any{"name": "other-topic"}))
	}()

	if raw, ok := broker.next(t, 500*time.Millisecond); ok {
		t.Fatalf("a frame (%s) reached the broker while this session's identity was still unresolved; an attach "+
			"answered before identity is settled races the recovery that is about to re-claim this session's "+
			"own topic", raw)
	}

	openGate() // identity can now resolve
	raw, ok := broker.next(t, 5*time.Second)
	if !ok {
		t.Fatal("nothing reached the broker after identity became resolvable")
	}
	if op := frameOp(t, raw); op != ipc.OpRecoverSession {
		t.Fatalf("first frame op = %q, want %q: recovery must be settled before the attach goes out", op, ipc.OpRecoverSession)
	}
	if err := broker.WriteJSON(ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult}); err != nil {
		t.Fatalf("write recover response: %v", err)
	}
	raw, ok = broker.next(t, 5*time.Second)
	if !ok {
		t.Fatal("the attach never went out after identity settled — the gate is a deadlock, not an ordering")
	}
	if op := frameOp(t, raw); op != ipc.OpAttach {
		t.Fatalf("second frame op = %q, want %q", op, ipc.OpAttach)
	}
	// Answer it so the tool call unwinds rather than parking on a reply that
	// never comes.
	if err := broker.WriteJSON(ipc.AttachedMsg{Op: ipc.OpAttached, OK: false, Err: "test"}); err != nil {
		t.Fatalf("write attach response: %v", err)
	}
	select {
	case <-attachDone:
	case <-time.After(5 * time.Second):
		t.Fatal("toolAttach never returned after its response arrived")
	}
}

func unresolvedRecoverTestAdapter(t *testing.T) (*adapter, *brokerPeer, context.Context) {
	t.Helper()
	a, broker, ctx := recoverTestAdapter(t, "")
	a.prepareSessionRecoverForConn(ctx, a.currentConn())
	return a, broker, ctx
}

func TestToolAttach_CanceledIdentityWaitWritesNoAttach(t *testing.T) {
	a, broker, _ := unresolvedRecoverTestAdapter(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := a.toolAttach(ctx, newAttachReq(t, map[string]any{"name": "other-topic"}))
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("canceled identity wait returned success: %+v", result)
	}
	if raw, ok := broker.next(t, 100*time.Millisecond); ok {
		t.Fatalf("a canceled identity wait still wrote a broker frame: %s", raw)
	}
}

func TestToolAttach_BareIdentityTimeoutRefusesWithoutBrokerWrite(t *testing.T) {
	old := recoverSettleTimeout
	recoverSettleTimeout = 20 * time.Millisecond
	defer func() { recoverSettleTimeout = old }()

	a, broker, _ := unresolvedRecoverTestAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	result, err := a.toolAttach(ctx, newAttachReq(t, map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	if raw, ok := broker.next(t, 100*time.Millisecond); ok {
		t.Fatalf("a bare attach reached the broker before identity settled: %s", raw)
	}
	if !result.IsError || len(result.Content) == 0 {
		t.Fatalf("bare unsettled attach did not return a retryable error: %+v", result)
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok || !strings.Contains(text.Text, "identity still resolving; retry attach") {
		t.Fatalf("bare unsettled attach error did not name the remedy: %+v", result.Content)
	}
}

func TestToolAttach_ExplicitTargetWaitsPastBareBudgetUntilIdentitySettles(t *testing.T) {
	old := recoverSettleTimeout
	recoverSettleTimeout = 20 * time.Millisecond
	defer func() { recoverSettleTimeout = old }()

	a, broker, ctx := unresolvedRecoverTestAdapter(t)
	done := make(chan *mcp.CallToolResult, 1)
	go func() {
		result, _ := a.toolAttach(ctx, newAttachReq(t, map[string]any{"name": "other-topic"}))
		done <- result
	}()

	if raw, ok := broker.next(t, 100*time.Millisecond); ok {
		t.Fatalf("an explicit attach overtook unsettled identity after the bare budget: %s", raw)
	}
	a.markIdentitySettled()
	raw, ok := broker.next(t, 2*time.Second)
	if !ok || frameOp(t, raw) != ipc.OpAttach {
		t.Fatalf("explicit attach was not released after identity settled: %s", raw)
	}
	attachedRaw, err := json.Marshal(ipc.AttachedMsg{Op: ipc.OpAttached, OK: false, Err: "test"})
	if err != nil {
		t.Fatal(err)
	}
	a.dispatchAttached(attachedRaw)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("explicit attach did not return after the broker response")
	}
}

// A broker RESTART hands the adapter a fresh stub with no stable session id.
// Unless the session re-registers on the new connection, every attach made
// afterwards is recorded against nothing and the next resume recovers the
// PRE-restart topic — the user's later choice silently lost. Re-registration is
// per connection, and must not re-fire on the same one.
func TestSessionRecover_ReRegistersOnAFreshBrokerConnectionOnly(t *testing.T) {
	ws := fakeCodexAppServer(t, nil, []any{"thread-1"})
	a, broker, ctx := recoverTestAdapter(t, ws)

	go a.brokerReader(ctx)

	if _, ok := broker.next(t, 5*time.Second); !ok {
		t.Fatal("no recover_session on the first connection")
	}
	// Same connection: repeated calls (brokerReader makes one per frame) must not
	// re-assert the identity.
	a.ensureSessionRecoverForConn(ctx, a.currentConn())
	if raw, ok := broker.next(t, 300*time.Millisecond); ok {
		t.Fatalf("recovery re-fired on the SAME broker connection (%s); every inbound frame would re-assert "+
			"the claim", raw)
	}

	// A fresh connection stands in for a broker restart.
	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = adapterSide.Close(); _ = brokerSide.Close() })
	fresh := ipc.NewConn(adapterSide)
	a.bmu.Lock()
	a.conn = fresh
	a.bmu.Unlock()
	a.ensureSessionRecoverForConn(ctx, fresh)

	raw, ok := newBrokerPeer(brokerSide).next(t, 5*time.Second)
	if !ok {
		t.Fatal("the session did not re-register on a fresh broker connection: after a broker restart its " +
			"attaches stop being recorded, and the next resume re-attaches the wrong topic")
	}
	var req ipc.RecoverSessionReq
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatalf("unmarshal re-register frame: %v", err)
	}
	if req.StableSessionID != "thread-1" {
		t.Fatalf("re-registered as %q, want the same identity as before the restart", req.StableSessionID)
	}
}

// An operator-pinned thread id is a KNOWN identity, and is the only way out of a
// genuinely ambiguous loaded list. It must win over the app-server probe.
func TestSessionRecover_OperatorPinnedThreadIDWinsOverAnAmbiguousAppServer(t *testing.T) {
	ws := fakeCodexAppServer(t, nil, []any{"thread-a", "thread-b"})
	a, broker, ctx := recoverTestAdapter(t, ws)
	t.Setenv("C3_CODEX_THREAD_ID", "pinned-thread")

	go a.brokerReader(ctx)

	raw, ok := broker.next(t, 5*time.Second)
	if !ok {
		t.Fatal("an explicitly pinned thread id was ignored, leaving no escape hatch from an ambiguous app-server")
	}
	var req ipc.RecoverSessionReq
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatalf("unmarshal recover frame: %v", err)
	}
	if req.StableSessionID != "pinned-thread" {
		t.Fatalf("stable_session_id = %q, want the operator's pin", req.StableSessionID)
	}
}

// The recovered-session notice is the only thing that tells a resumed Codex
// session it holds a topic AND that messages are waiting on it. Codex polls; a
// notice that omits the backlog leaves those messages unread indefinitely.
func TestRenderCodexRecoverNotice_NamesTheTopicAndTheHeldBacklog(t *testing.T) {
	got := renderCodexRecoverNotice(ipc.RecoverSessionResp{
		Recovered: true, Name: "proj", QueuedCount: 3,
		QueuedSummary: []ipc.QueuedItem{{MessageID: 9, Sender: "alice", Kind: "text", Preview: "ping"}},
	})
	for _, want := range []string{"proj", "3 message", "fetch_queue", "alice"} {
		if !strings.Contains(got, want) {
			t.Errorf("recover notice is missing %q — a resumed session cannot tell what is waiting for it.\ngot: %s", want, got)
		}
	}
	if renderCodexRecoverNotice(ipc.RecoverSessionResp{}) != "" {
		t.Error("a non-recovery must not announce an attach")
	}
}

func TestReconnectRecoveryRearmsIdentitySettlement(t *testing.T) {
	a := newAdapter()
	old := a.identityGate()
	a.markIdentitySettled()
	a.tidmu.Lock()
	a.threadID = "thread-reconnect"
	a.tidmu.Unlock()
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requestSeen := make(chan struct{})
	go func() {
		_, _ = ipc.NewConn(c2).ReadFrame()
		close(requestSeen)
	}()
	a.ensureSessionRecoverForConn(ctx, ipc.NewConn(c1))
	current := a.identityGate()
	if old == current {
		t.Fatal("reconnect recovery reused the permanently closed identity gate")
	}
	select {
	case <-current:
		t.Fatal("reconnect identity epoch was already settled before re-registration")
	default:
	}
	select {
	case <-requestSeen:
	case <-time.After(time.Second):
		t.Fatal("reconnect recovery did not send registration")
	}
	cancel()
	select {
	case <-current:
	case <-time.After(time.Second):
		t.Fatal("current reconnect identity epoch did not settle")
	}
}

func TestSupersededRecoveryCannotSettleReplacementEpoch(t *testing.T) {
	a := newAdapter()
	a.tidmu.Lock()
	a.threadID = "thread-epoch"
	a.tidmu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldAdapter, oldBroker := net.Pipe()
	newAdapterConn, newBrokerConn := net.Pipe()
	t.Cleanup(func() {
		_ = oldAdapter.Close()
		_ = oldBroker.Close()
		_ = newAdapterConn.Close()
		_ = newBrokerConn.Close()
	})
	oldConn := ipc.NewConn(oldAdapter)
	newConn := ipc.NewConn(newAdapterConn)
	oldEpoch := a.prepareSessionRecoverForConn(ctx, oldConn)
	newEpoch := a.prepareSessionRecoverForConn(ctx, newConn)

	// This is the exact stale-goroutine interleaving: conn1 was superseded before
	// its recovery goroutine got CPU. It must settle only conn1's captured epoch,
	// never look up and close conn2's current gate.
	a.startSessionRecover(oldEpoch)
	select {
	case <-newEpoch.gate:
		t.Fatal("superseded conn1 recovery settled conn2's identity epoch")
	default:
	}

	peer := newBrokerPeer(newBrokerConn)
	a.ensureSessionRecoverForConn(ctx, newConn)
	if raw, ok := peer.next(t, time.Second); !ok || frameOp(t, raw) != ipc.OpRecoverSession {
		t.Fatalf("replacement recovery did not register on conn2: %s", raw)
	}
	resp, err := json.Marshal(ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult})
	if err != nil {
		t.Fatal(err)
	}
	a.dispatchRecoverSessionResult(newConn, resp)
	select {
	case <-newEpoch.gate:
	case <-time.After(time.Second):
		t.Fatal("conn2 epoch did not settle after its own recovery response")
	}
}

func TestRecoverResponseWaitersAreConnectionBound(t *testing.T) {
	a := newAdapter()
	a.tidmu.Lock()
	a.threadID = "thread-response-routing"
	a.tidmu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	newAdapterConn, newBrokerConn := net.Pipe()
	staleAdapterConn, staleBrokerConn := net.Pipe()
	t.Cleanup(func() {
		_ = newAdapterConn.Close()
		_ = newBrokerConn.Close()
		_ = staleAdapterConn.Close()
		_ = staleBrokerConn.Close()
	})
	newConn := ipc.NewConn(newAdapterConn)
	staleConn := ipc.NewConn(staleAdapterConn)
	newEpoch := a.prepareSessionRecoverForConn(ctx, newConn)
	peer := newBrokerPeer(newBrokerConn)
	a.ensureSessionRecoverForConn(ctx, newConn)
	if raw, ok := peer.next(t, time.Second); !ok || frameOp(t, raw) != ipc.OpRecoverSession {
		t.Fatalf("current recovery did not reach conn2: %s", raw)
	}

	// Install a stale connection's waiter AFTER conn2's waiter. Recover responses
	// have no request id, so an unkeyed singleton would now send conn2's response
	// to this stale waiter and leave conn2 parked until its eight-second timeout.
	staleCtx, cancelStale := context.WithCancel(context.Background())
	staleDone := make(chan struct{})
	go func() {
		defer close(staleDone)
		a.fireRecover(staleCtx, staleConn, "thread-stale", "/w/proj")
	}()
	deadline := time.Now().Add(time.Second)
	for {
		a.rsmu.Lock()
		staleInstalled := a.rsPending[staleConn] != nil
		currentInstalled := a.rsPending[newConn] != nil
		a.rsmu.Unlock()
		if staleInstalled && currentInstalled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("both connection-bound recover waiters were not installed")
		}
		time.Sleep(time.Millisecond)
	}

	resp, err := json.Marshal(ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult})
	if err != nil {
		t.Fatal(err)
	}
	a.dispatchRecoverSessionResult(newConn, resp)
	select {
	case <-newEpoch.gate:
	case <-time.After(time.Second):
		t.Fatal("conn2 response was stolen by a stale connection's waiter")
	}

	cancelStale()
	if _, err := ipc.NewConn(staleBrokerConn).ReadFrame(); err != nil {
		t.Fatalf("release stale recover write: %v", err)
	}
	select {
	case <-staleDone:
	case <-time.After(time.Second):
		t.Fatal("stale recovery did not unwind after cancellation")
	}
}

func TestAwaitIdentityEpochFollowsReplacementAndReturnsItsConn(t *testing.T) {
	a := newAdapter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldAdapter, oldBroker := net.Pipe()
	newAdapterConn, newBrokerConn := net.Pipe()
	t.Cleanup(func() {
		_ = oldAdapter.Close()
		_ = oldBroker.Close()
		_ = newAdapterConn.Close()
		_ = newBrokerConn.Close()
	})
	oldConn := ipc.NewConn(oldAdapter)
	newConn := ipc.NewConn(newAdapterConn)
	oldEpoch := a.prepareSessionRecoverForConn(ctx, oldConn)
	newEpoch := a.prepareSessionRecoverForConn(ctx, newConn)

	got := make(chan *ipc.Conn, 1)
	go func() {
		conn, _ := a.awaitIdentityEpoch(ctx, false, oldEpoch)
		got <- conn
	}()
	newEpoch.settle()
	select {
	case conn := <-got:
		if conn != newConn {
			t.Fatalf("waiter released by conn1 returned %p, want replacement conn2 %p", conn, newConn)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not follow the settled old epoch to conn2")
	}
}

func TestPreparedConnectionBlocksAttachBeforeRecoveryStarts(t *testing.T) {
	a, broker, ctx := recoverTestAdapter(t, "")
	conn := a.currentConn()
	epoch := a.prepareSessionRecoverForConn(ctx, conn)
	done := make(chan *mcp.CallToolResult, 1)
	go func() {
		result, _ := a.toolAttach(ctx, newAttachReq(t, map[string]any{"name": "other-topic"}))
		done <- result
	}()

	if raw, ok := broker.next(t, 100*time.Millisecond); ok {
		t.Fatalf("attach overtook a prepared-but-not-started connection epoch: %s", raw)
	}
	epoch.settle()
	raw, ok := broker.next(t, time.Second)
	if !ok || frameOp(t, raw) != ipc.OpAttach {
		t.Fatalf("attach was not released after the prepared epoch settled: %s", raw)
	}
	attachedRaw, err := json.Marshal(ipc.AttachedMsg{Op: ipc.OpAttached, OK: false, Err: "test"})
	if err != nil {
		t.Fatal(err)
	}
	a.dispatchAttached(attachedRaw)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("attach did not return after its broker response")
	}
}
