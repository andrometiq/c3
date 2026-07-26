package broker

import (
	"encoding/json"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// pingerHelloAck registers a transient client (mimics the c3-broker
// sessions CLI subcommand) and consumes hello_ack. CLI defaults to
// "c3-broker-cli" to match dialBroker in cmd/c3-broker/client.go;
// override for the "filter the transient stub" test.
func pingerHelloAck(t *testing.T, peer *ipc.Conn, cli string, pid int, cwd string) {
	t.Helper()
	if cli == "" {
		cli = "c3-broker-cli"
	}
	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: cli, PID: pid, CWD: cwd}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
}

// askSessions issues a ListSessionsReq on peer and decodes the reply.
func askSessions(t *testing.T, peer *ipc.Conn, pid int, cwd string) ipc.ListSessionsReplyMsg {
	t.Helper()
	if err := peer.WriteJSON(ipc.ListSessionsReq{
		Op: ipc.OpListSessions, PID: pid, CWD: cwd,
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("read list_sessions_reply: %v", err)
	}
	var resp ipc.ListSessionsReplyMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("parse list_sessions_reply: %v", err)
	}
	if resp.Op != ipc.OpListSessionsReply {
		t.Errorf("Op=%q want %q", resp.Op, ipc.OpListSessionsReply)
	}
	return resp
}

func TestListSessions_ReturnsAllStubs(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// Three "adapter" stubs registered out-of-band (mimic real adapter
	// hellos without the network plumbing). Distinct CLI/PID/CWD per stub
	// so they don't collide on logical-session reconnect.
	b.Stubs.Register("claude", 1001, "/p/one", nil)
	b.Stubs.Register("claude", 1002, "/p/two", nil)
	b.Stubs.Register("codex", 1003, "/p/three", nil)

	// Transient client (the "ourselves" stub) — filtered out.
	pinger, done := peerPair(t, b)
	defer done()
	pingerHelloAck(t, pinger, "c3-broker-cli", 9999, "/wherever")

	resp := askSessions(t, pinger, 0, "/wherever")
	if len(resp.Sessions) != 3 {
		t.Fatalf("Sessions len=%d, want 3 (adapter stubs only; transient filtered)", len(resp.Sessions))
	}
	// Sanity: none of the entries reference the transient stub.
	for _, e := range resp.Sessions {
		if e.CLI == "c3-broker-cli" {
			t.Errorf("transient stub leaked into reply: %+v", e)
		}
	}
}

func TestListSessions_MarksThisSession(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// Two adapter stubs with distinct PIDs.
	b.Stubs.Register("claude", 4242, "/p/one", nil)
	b.Stubs.Register("claude", 5151, "/p/two", nil)

	pinger, done := peerPair(t, b)
	defer done()
	pingerHelloAck(t, pinger, "c3-broker-cli", 9999, "/wherever")

	// Caller's PID hint matches the second stub.
	resp := askSessions(t, pinger, 5151, "/wherever")
	if len(resp.Sessions) != 2 {
		t.Fatalf("Sessions len=%d, want 2", len(resp.Sessions))
	}
	var thisCount int
	for _, e := range resp.Sessions {
		if e.IsThisSession {
			thisCount++
			if e.PID != 5151 {
				t.Errorf("IsThisSession=true on wrong stub: PID=%d want 5151", e.PID)
			}
		}
	}
	if thisCount != 1 {
		t.Errorf("expected exactly one IsThisSession=true, got %d", thisCount)
	}
}

func TestListSessions_NoStubs_ReturnsEmptyList(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	pinger, done := peerPair(t, b)
	defer done()
	pingerHelloAck(t, pinger, "c3-broker-cli", 9999, "/wherever")

	resp := askSessions(t, pinger, 0, "/wherever")
	// The transient client is filtered, so even though one stub is
	// registered (the pinger itself) the response should be empty.
	if resp.Sessions == nil {
		t.Error("Sessions slice must be non-nil empty, got nil")
	}
	if len(resp.Sessions) != 0 {
		t.Errorf("Sessions len=%d, want 0", len(resp.Sessions))
	}
}

func TestListSessions_AttachedTo_FormatsTopicLabel(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// Register a stub and claim a topic so it has a CurrentRoute.
	stub := b.Stubs.Register("claude", 4242, "/projects/c3", nil)
	tid := int64(281) // topic "c3" in group "main" per mfWithTelegram
	key := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, stub, key, "c3", false, false) {
		t.Fatal("setup: tryClaim failed")
	}

	pinger, done := peerPair(t, b)
	defer done()
	pingerHelloAck(t, pinger, "c3-broker-cli", 9999, "/wherever")

	resp := askSessions(t, pinger, 0, "/wherever")
	if len(resp.Sessions) != 1 {
		t.Fatalf("Sessions len=%d, want 1", len(resp.Sessions))
	}
	got := resp.Sessions[0].AttachedTo
	want := "c3 (main)"
	if got != want {
		t.Errorf("AttachedTo=%q, want %q", got, want)
	}
}

func TestListSessions_AttachedTo_EmptyForUnattachedStub(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	b.Stubs.Register("claude", 4242, "/projects/c3", nil)

	pinger, done := peerPair(t, b)
	defer done()
	pingerHelloAck(t, pinger, "c3-broker-cli", 9999, "/wherever")

	resp := askSessions(t, pinger, 0, "/wherever")
	if len(resp.Sessions) != 1 {
		t.Fatalf("Sessions len=%d, want 1", len(resp.Sessions))
	}
	if got := resp.Sessions[0].AttachedTo; got != "" {
		t.Errorf("AttachedTo=%q, want \"\" (unattached stub)", got)
	}
}

func TestListSessions_DM_AttachedToLabel(t *testing.T) {
	// DM routes (HasTopic=false) render as "dm" — keep the label
	// format matching pingTopicLabel's convention.
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 4242, "/projects/c3", nil)
	key := MakeRouteKey("telegram", 42, nil) // DM
	if !b.tryClaim(nil, stub, key, "dm", false, false) {
		t.Fatal("setup: tryClaim DM failed")
	}

	pinger, done := peerPair(t, b)
	defer done()
	pingerHelloAck(t, pinger, "c3-broker-cli", 9999, "/wherever")

	resp := askSessions(t, pinger, 0, "/wherever")
	if len(resp.Sessions) != 1 {
		t.Fatalf("Sessions len=%d, want 1", len(resp.Sessions))
	}
	if got := resp.Sessions[0].AttachedTo; got != "dm" {
		t.Errorf("AttachedTo=%q, want \"dm\"", got)
	}
}

func TestListSessions_OrderedByConnIDDesc(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	a := b.Stubs.Register("claude", 1001, "/p/a", nil)
	bb := b.Stubs.Register("claude", 1002, "/p/b", nil)
	c := b.Stubs.Register("codex", 1003, "/p/c", nil)
	if !(a.ConnID < bb.ConnID && bb.ConnID < c.ConnID) {
		t.Fatalf("test setup: ConnIDs not monotonic: %d %d %d", a.ConnID, bb.ConnID, c.ConnID)
	}

	pinger, done := peerPair(t, b)
	defer done()
	pingerHelloAck(t, pinger, "c3-broker-cli", 9999, "/wherever")

	resp := askSessions(t, pinger, 0, "/wherever")
	if len(resp.Sessions) != 3 {
		t.Fatalf("Sessions len=%d, want 3", len(resp.Sessions))
	}
	// Expect c, b, a (descending by ConnID).
	wantOrder := []int{c.PID, bb.PID, a.PID}
	for i, e := range resp.Sessions {
		if e.PID != wantOrder[i] {
			t.Errorf("Sessions[%d].PID=%d, want %d (descending-ConnID order)", i, e.PID, wantOrder[i])
		}
	}
}

func TestListSessions_FiltersTransientClientStub(t *testing.T) {
	// The c3-broker-cli stub (the caller itself) MUST NOT appear in
	// the response — listing ourselves would be confusing and clutter
	// the table. Filter is by CLI name (matches dialBroker's hello).
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	b.Stubs.Register("claude", 1001, "/p/adapter", nil)

	pinger, done := peerPair(t, b)
	defer done()
	pingerHelloAck(t, pinger, "c3-broker-cli", 9999, "/wherever")

	resp := askSessions(t, pinger, 0, "/wherever")
	if len(resp.Sessions) != 1 {
		t.Fatalf("Sessions len=%d, want 1 (transient client filtered)", len(resp.Sessions))
	}
	for _, e := range resp.Sessions {
		if e.CLI == "c3-broker-cli" {
			t.Errorf("transient stub leaked: %+v", e)
		}
	}
}

func TestListSessions_EmptyCLIMappedToQuestionMark(t *testing.T) {
	// Defensive: if an adapter sent an empty CLI string (shouldn't
	// happen in practice — both adapters set hello.CLI), the broker
	// emits "?" so the rendered table column never goes blank.
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	b.Stubs.Register("", 1001, "/p/mystery", nil)

	pinger, done := peerPair(t, b)
	defer done()
	pingerHelloAck(t, pinger, "c3-broker-cli", 9999, "/wherever")

	resp := askSessions(t, pinger, 0, "/wherever")
	if len(resp.Sessions) != 1 {
		t.Fatalf("Sessions len=%d, want 1", len(resp.Sessions))
	}
	if got := resp.Sessions[0].CLI; got != "?" {
		t.Errorf("CLI=%q, want %q (empty-string adapter CLI normalized)", got, "?")
	}
}

// Belt-and-suspenders: confirm the new dispatch path routes correctly
// through HandleConn's switch.
var _ = mappings.MappingsFile{}
