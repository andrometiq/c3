package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// fakeBoardChannel is the board channel as the broker sees it: a channel that
// keeps boards and records what is sent to its routes.
type fakeBoardChannel struct {
	*fakeChannel
	boardMu sync.Mutex
	boards  []channel.Board
	// isKeyboardCapable makes the board declare InlineKeyboards, which the
	// real one never does, so a test proves the broker skips boards itself.
	isKeyboardCapable bool
}

var _ channel.BoardKeeper = (*fakeBoardChannel)(nil)

const (
	testStarterHTML = `<!doctype html><title>starter</title><section data-c3-part="1"></section>`
	boardSession    = "board-session"
	boardCWD        = "/proj"
)

func (c *fakeBoardChannel) Name() string { return channel.BoardChannelName }
func (c *fakeBoardChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{Channel: channel.BoardChannelName, EditMessages: true, InlineKeyboards: c.isKeyboardCapable}
}

func (c *fakeBoardChannel) CreateBoard(name string, owner channel.BoardOwner) (channel.Board, bool, error) {
	c.boardMu.Lock()
	defer c.boardMu.Unlock()
	for _, board := range c.boards {
		if board.Name == name {
			return board, false, nil
		}
	}
	board := channel.Board{Number: int64(len(c.boards) + 1), Name: name, Owner: owner, Link: "https://boards.example/board/" + name}
	c.boards = append(c.boards, board)
	return board, true, nil
}

func (c *fakeBoardChannel) BoardByName(name string) (channel.Board, bool) {
	c.boardMu.Lock()
	defer c.boardMu.Unlock()
	for _, board := range c.boards {
		if board.Name == name {
			return board, true
		}
	}
	return channel.Board{}, false
}

func (c *fakeBoardChannel) BoardByNumber(number int64) (channel.Board, bool) {
	c.boardMu.Lock()
	defer c.boardMu.Unlock()
	for _, board := range c.boards {
		if board.Number == number {
			return board, true
		}
	}
	return channel.Board{}, false
}

func (c *fakeBoardChannel) Boards() []channel.Board {
	c.boardMu.Lock()
	defer c.boardMu.Unlock()
	return append([]channel.Board(nil), c.boards...)
}

func (c *fakeBoardChannel) StarterHTML() (string, error) { return testStarterHTML, nil }

func (c *fakeBoardChannel) boardCount() int { return len(c.Boards()) }

func (c *fakeBoardChannel) markClosed(name string) {
	c.boardMu.Lock()
	defer c.boardMu.Unlock()
	for i := range c.boards {
		if c.boards[i].Name == name {
			c.boards[i].IsClosed = true
		}
	}
}

func (c *fakeBoardChannel) mustCreate(t *testing.T, name string, owner channel.BoardOwner) channel.Board {
	t.Helper()
	board, _, err := c.CreateBoard(name, owner)
	if err != nil {
		t.Fatal(err)
	}
	return board
}

var sessionOwner = channel.BoardOwner{CLI: "claude", SessionID: boardSession, CWD: boardCWD}

// newBoardBroker registers telegram (with keyboards), the web chat and the
// board channel.
func newBoardBroker(t *testing.T) (*Broker, *fakeChannel, *webFakeChannel, *fakeBoardChannel) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := multiMappings()
	mf.Allowlist = &mappings.Allowlist{Users: []int64{42}}
	b := newTestBroker(t, mf)
	t.Cleanup(b.Shutdown)
	telegramCaps := c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}
	telegram := &fakeChannel{caps: &telegramCaps}
	web := &webFakeChannel{fakeChannel: &fakeChannel{}, link: "https://device.example/auth#test-token"}
	boards := &fakeBoardChannel{fakeChannel: &fakeChannel{}}
	registerTestChannel(b, telegram)
	registerTestChannel(b, web)
	registerTestChannel(b, boards)
	return b, telegram, web, boards
}

// connectSession says hello as a claude session and, given a stable id,
// registers it the way the adapter's recover op does. It returns the
// adapter-side conn, the broker stub and a func that drops the connection.
func connectSession(t *testing.T, b *Broker, pid int, cwd, sid string) (*ipc.Conn, *Stub, func()) {
	t.Helper()
	peer, done := peerPair(t, b)
	t.Cleanup(done)
	if err := peer.WriteJSON(ipc.HelloMsg{Build: "test", Op: ipc.OpHello, CLI: "claude", PID: pid, CWD: cwd}); err != nil {
		t.Fatal(err)
	}
	readOp(t, peer, ipc.OpHelloAck)
	if sid != "" {
		if err := peer.WriteJSON(ipc.RecoverSessionReq{Op: ipc.OpRecoverSession, StableSessionID: sid, CWD: cwd}); err != nil {
			t.Fatal(err)
		}
		readOp(t, peer, ipc.OpRecoverSessionResult)
	}
	for _, stub := range b.Stubs.Snapshot() {
		if stub.PID == pid {
			return peer, stub, done
		}
	}
	t.Fatalf("no stub for pid %d", pid)
	return nil, nil, nil
}

// readOp reads frames until one carries op, skipping pushes and notices, and
// fails after five seconds rather than blocking the suite.
func readOp(t *testing.T, peer *ipc.Conn, op ipc.Op) []byte {
	t.Helper()
	found := make(chan []byte, 1)
	go func() {
		for {
			raw, err := peer.ReadFrame()
			if err != nil {
				close(found)
				return
			}
			if got, _ := ipc.PeekOp(raw); got == op {
				found <- raw
				return
			}
		}
	}()
	select {
	case raw, ok := <-found:
		if !ok {
			t.Fatalf("connection closed waiting for %s", op)
		}
		return raw
	case <-time.After(5 * time.Second):
		t.Fatalf("no %s frame within 5s", op)
	}
	return nil
}

func attachVia(t *testing.T, peer *ipc.Conn, req ipc.AttachReq) ipc.AttachedMsg {
	t.Helper()
	req.Op = ipc.OpAttach
	if err := peer.WriteJSON(req); err != nil {
		t.Fatal(err)
	}
	var attached ipc.AttachedMsg
	if err := json.Unmarshal(readOp(t, peer, ipc.OpAttached), &attached); err != nil {
		t.Fatal(err)
	}
	return attached
}

func mustAttach(t *testing.T, peer *ipc.Conn, req ipc.AttachReq) ipc.AttachedMsg {
	t.Helper()
	attached := attachVia(t, peer, req)
	if !attached.OK {
		t.Fatalf("attach %+v refused: %+v", req, attached)
	}
	return attached
}

var (
	c3TopicKey  = MakeRouteKey("telegram", -100, ptrI64Val(281))
	webChatKey  = MakeRouteKey("web", 42, nil)
	firstBoard  = boardRouteKey(1)
	secondBoard = boardRouteKey(2)
)

func storedRoutes(t *testing.T, b *Broker, sid string) []RouteKey {
	t.Helper()
	sa, ok := b.Mappings().LookupSessionAttachment("claude", sid)
	if !ok {
		t.Fatalf("no record for %s", sid)
	}
	var keys []RouteKey
	for _, ref := range routeRefsFromSessionAttachment(sa) {
		keys = append(keys, routeKeyFromRef(ref))
	}
	return keys
}

func TestBoardAttachBindsTheBoardToTheCallingSession(t *testing.T) {
	b, telegram, _, boards := newBoardBroker(t)
	peer, stub, _ := connectSession(t, b, 101, boardCWD, boardSession)

	attached := mustAttach(t, peer, ipc.AttachReq{Expr: "+board:plan", CWD: boardCWD})
	board, ok := boards.BoardByName("plan")
	if !ok || board.Owner != sessionOwner {
		t.Fatalf("board=%+v, want owner %+v", board, sessionOwner)
	}
	if holder, held := b.Routes.Holder(boardRouteKey(board.Number)); !held || holder != stub {
		t.Fatalf("board route holder=%v held=%v, want the calling session", holder, held)
	}
	if attached.Channel != channel.BoardChannelName || attached.Name != "board:plan" || attached.Link != board.Link || len(attached.Routes) != 1 || attached.Routes[0].Name != "board:plan" {
		t.Fatalf("attach result=%+v", attached)
	}
	for _, want := range []string{board.Link, `reply channel="board:plan"`, "[board plan · <part> · msg N]", "Approved your message M", "never their word for a push", testStarterHTML,
		"and no text, a caption at most: a reply with text and a file splits, and reply_to rides only the first part",
		`reply_to set to the approve line's own id, the N in its "[board plan · <part> · msg N]" header, not the M it approves`} {
		if !strings.Contains(attached.Notice, want) {
			t.Errorf("attach notice lacks %q:\n%s", want, attached.Notice)
		}
	}
	if keys := storedRoutes(t, b, boardSession); len(keys) != 1 || keys[0] != firstBoard {
		t.Fatalf("recorded routes=%v, want the board", keys)
	}

	again := mustAttach(t, peer, ipc.AttachReq{Expr: "+board:plan", CWD: boardCWD})
	if strings.Contains(again.Notice, testStarterHTML) || !strings.Contains(again.Notice, board.Link) || again.Link != board.Link || boards.boardCount() != 1 {
		t.Fatalf("re-attach notice=%q boards=%d, want the link without a starter and no second board", again.Notice, boards.boardCount())
	}

	// A topic attach greets its topic and names no link; a board attach writes nothing into the board.
	if topic := mustAttach(t, peer, ipc.AttachReq{Expr: "+c3"}); topic.Link != "" {
		t.Fatalf("a topic attach named a link: %q", topic.Link)
	}
	waitForReply(t, telegram, "Attached")
	if replies := boards.sendRepliesSnapshot(); len(replies) != 0 {
		t.Fatalf("board attach wrote into the board: %+v", replies)
	}
}

func waitForReply(t *testing.T, ch *fakeChannel, prefix string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, reply := range ch.sendRepliesSnapshot() {
			if strings.Contains(reply.Text, prefix) {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no reply containing %q in %+v", prefix, ch.sendRepliesSnapshot())
}

func TestBoardOfAnotherSessionIsRefusedEvenWithSteal(t *testing.T) {
	b, _, _, boards := newBoardBroker(t)
	ownerPeer, owner, _ := connectSession(t, b, 101, boardCWD, boardSession)
	mustAttach(t, ownerPeer, ipc.AttachReq{Expr: "+board:plan"})

	for _, other := range []struct {
		name     string
		pid      int
		cwd, sid string
	}{
		{name: "another session", pid: 102, cwd: boardCWD, sid: "other-session"},
		{name: "the same id from another folder", pid: 103, cwd: "/elsewhere", sid: boardSession},
		{name: "no stable id", pid: 104, cwd: boardCWD},
	} {
		peer, _, _ := connectSession(t, b, other.pid, other.cwd, other.sid)
		for _, steal := range []bool{false, true} {
			refused := attachVia(t, peer, ipc.AttachReq{Expr: "+board:plan", Steal: steal})
			if refused.OK || !strings.Contains(refused.Err, "belongs to another session") {
				t.Fatalf("%s steal=%v: %+v, want refused", other.name, steal, refused)
			}
			if holder, _ := b.Routes.Holder(firstBoard); holder != owner {
				t.Fatalf("%s steal=%v moved the board to %v", other.name, steal, holder)
			}
		}
	}

	// Recovery is a claim too: a record naming a board restores it only for its
	// owner, and never once it is closed.
	solo := boards.mustCreate(t, "solo", sessionOwner)
	soloRef := mappings.RouteRef{Channel: channel.BoardChannelName, ChatID: solo.Number, Name: "board:solo"}
	for _, sid := range []string{"record-session", boardSession} {
		b.Mappings().UpsertSessionAttachment("claude", sid, mappings.SessionAttachment{
			Channel: soloRef.Channel, ChatID: soloRef.ChatID, Name: soloRef.Name, CWD: boardCWD,
			LastAttachedAt: time.Now().UTC(), Routes: []mappings.RouteRef{soloRef}, Output: &soloRef,
		})
	}
	if _, recovered, _ := connectSession(t, b, 105, boardCWD, "record-session"); recovered.HasRoute(boardRouteKey(solo.Number)) {
		t.Fatal("another session's record restored the board")
	}
	boards.markClosed("solo")
	if _, recovered, _ := connectSession(t, b, 106, boardCWD, boardSession); recovered.HasRoute(boardRouteKey(solo.Number)) {
		t.Fatal("the owner's record restored a closed board")
	}

	boards.markClosed("plan")
	if closed := attachVia(t, ownerPeer, ipc.AttachReq{Expr: "+board:plan"}); closed.OK || !strings.Contains(closed.Err, "closed") {
		t.Fatalf("owner claim of a closed board=%+v, want refused", closed)
	}
}

func TestNoStructuredAttachBuildsABoardRoute(t *testing.T) {
	b, _, _, boards := newBoardBroker(t)
	peer, stub, _ := connectSession(t, b, 101, boardCWD, boardSession)
	boards.mustCreate(t, "plan", sessionOwner)
	topicID := int64(1)
	for _, req := range []ipc.AttachReq{
		{Channel: channel.BoardChannelName},
		{Channel: channel.BoardChannelName, Target: "dm", Replay: true},
		{Channel: channel.BoardChannelName, Name: "plan", Add: true},
		{Channel: channel.BoardChannelName, TopicID: &topicID},
		{Name: "board:plan", Add: true},
		{Expr: "board:plan"},
		{Expr: "+board:Plan"},
	} {
		if attached := attachVia(t, peer, req); attached.OK {
			t.Fatalf("attach %+v succeeded: %+v", req, attached)
		}
		for _, held := range stub.Routes() {
			if isBoardRoute(held) {
				t.Fatalf("attach %+v built board route %v", req, held)
			}
		}
	}
	if plain := attachVia(t, peer, ipc.AttachReq{Expr: "board:plan"}); plain.NeedsConfirmation || !strings.Contains(plain.Err, "+board:<name>") {
		t.Fatalf("expr without + => %+v, want the +board:<name> hint, never a topic proposal", plain)
	}
	mustAttach(t, peer, ipc.AttachReq{Expr: "+ board:plan"})
	if !stub.HasRoute(firstBoard) || boards.boardCount() != 1 {
		t.Fatalf("expr attach: routes=%v boards=%d", stub.Routes(), boards.boardCount())
	}

	// A channels.board stanza opens no structured path either: steal from
	// another session never reaches the owner's board.
	b.mutateMappings(func(mf *mappings.MappingsFile) {
		mf.Channels[channel.BoardChannelName] = mappings.ChannelConfig{DMChatID: 1}
	})
	other, otherStub, _ := connectSession(t, b, 102, "/elsewhere", "other-session")
	for _, req := range []ipc.AttachReq{
		{Channel: channel.BoardChannelName, Target: "dm", Steal: true},
		{Channel: channel.BoardChannelName, Name: "plan", Steal: true},
		{Channel: channel.BoardChannelName, TopicID: &topicID, Steal: true},
	} {
		if attached := attachVia(t, other, req); attached.OK || !strings.Contains(attached.Err, "+board:<name>") {
			t.Fatalf("with a board stanza, attach %+v => %+v, want the +board:<name> refusal", req, attached)
		}
	}
	if holder, _ := b.Routes.Holder(firstBoard); holder != stub || len(otherStub.Routes()) != 0 {
		t.Fatalf("board holder=%v, other session routes=%v", holder, otherStub.Routes())
	}
}

func TestBoardSelectorNeverMeansATopic(t *testing.T) {
	b, telegram, _, boards := newBoardBroker(t)
	peer, stub, _ := connectSession(t, b, 101, boardCWD, boardSession)
	if plain := attachVia(t, peer, ipc.AttachReq{Expr: "Board:plan"}); plain.OK || plain.NeedsConfirmation || plain.Err != boardTopicNameRefusal {
		t.Fatalf("Board:plan without + => %+v, want the +board:<name> hint", plain)
	}
	if attached := attachVia(t, peer, ipc.AttachReq{Expr: "+Board:plan"}); !attached.OK || attached.Name != "board:plan" || !stub.HasRoute(firstBoard) {
		t.Fatalf("+Board:plan => %+v, routes %v; want board plan, never a topic proposal", attached, stub.Routes())
	}
	if refused := attachVia(t, peer, ipc.AttachReq{Expr: "+BOARD:Plan"}); refused.OK || !strings.Contains(refused.Err, channel.BoardNamePattern) || boards.boardCount() != 1 {
		t.Fatalf("+BOARD:Plan => %+v, want the board-name rule", refused)
	}
	for _, req := range []ipc.AttachReq{
		{Expr: "create board:x"}, {Expr: "+create Board:x"}, {Name: "board:x", Create: true}, {Name: " BOARD:x"}, {Name: "board:plan", Add: true},
	} {
		if attached := attachVia(t, peer, req); attached.OK || attached.NeedsConfirmation || attached.Err != boardTopicNameRefusal {
			t.Fatalf("attach %+v => %+v, want the topic name refused", req, attached)
		}
	}
	telegram.mu.Lock()
	created := telegram.createCalls
	telegram.mu.Unlock()
	if len(created) != 0 {
		t.Fatalf("a board:-named topic was created: %+v", created)
	}

	// A topic already named board:x (made outside C3) never answers that selector.
	named := MakeRouteKey("telegram", -100, ptrI64Val(900))
	b.mutateMappings(func(mf *mappings.MappingsFile) {
		telegram := mf.Channels["telegram"]
		telegram.Topics = append(telegram.Topics, mappings.Topic{ChatID: -100, TopicID: 900, Name: "board:x", Group: "main"})
		mf.Channels["telegram"] = telegram
	})
	holder := b.Stubs.Register("claude", 12, boardCWD, nil)
	claimConfirmed(t, b, holder, named)
	for _, selector := range []string{"board:x", "Board:X"} {
		if got, err := b.resolveHeldRoute(holder, selector); err == nil {
			t.Fatalf("%q => %v, want no route (only a board answers it)", selector, got)
		}
	}
	if got, err := b.resolveHeldRoute(holder, "telegram"); err != nil || got != named {
		t.Fatalf(`"telegram" => %v, %v; want the topic`, got, err)
	}
}

// A topic named "Board: …" before the rule is refused by name, by the slash
// command's expr as by the structured name, and the refusal names the way that
// still reaches it: its topic_id.
func TestBoardNamedTopicAttachesByTopicID(t *testing.T) {
	b, _, _, _ := newBoardBroker(t)
	b.mutateMappings(func(mf *mappings.MappingsFile) {
		telegram := mf.Channels["telegram"]
		telegram.Topics = append(telegram.Topics, mappings.Topic{ChatID: -100, TopicID: 901, Name: "Board: Q3", Group: "main"})
		mf.Channels["telegram"] = telegram
	})
	peer, stub, _ := connectSession(t, b, 101, boardCWD, boardSession)
	for _, req := range []ipc.AttachReq{{Expr: "Board: Q3"}, {Name: "Board: Q3"}} {
		if refused := attachVia(t, peer, req); refused.OK || refused.Err != boardTopicNameRefusal || !strings.Contains(refused.Err, "topic_id") {
			t.Fatalf("attach %+v => %+v, want the topic-name refusal naming topic_id", req, refused)
		}
	}
	topicID := int64(901)
	for _, req := range []ipc.AttachReq{{Expr: "901"}, {TopicID: &topicID}} {
		if attached := attachVia(t, peer, req); !attached.OK || attached.Name != "Board: Q3" || !stub.HasRoute(MakeRouteKey("telegram", -100, &topicID)) {
			t.Fatalf("attach %+v => %+v, routes %v; want the Board: Q3 topic", req, attached, stub.Routes())
		}
	}
}

func TestBoardAttachKeepsTheOutputRoute(t *testing.T) {
	b, _, _, _ := newBoardBroker(t)
	peer, stub, _ := connectSession(t, b, 101, boardCWD, boardSession)
	mustAttach(t, peer, ipc.AttachReq{Name: "c3", Replay: true})
	attached := mustAttach(t, peer, ipc.AttachReq{Expr: "+board:plan"})
	if output := stub.OutputRoute(); output == nil || *output != c3TopicKey {
		t.Fatalf("output after a board attach=%v, want the topic", output)
	}
	if attached.Output == nil || attached.Output.Name != "c3" || attached.Capabilities == nil || attached.Capabilities.Channel != "telegram" {
		t.Fatalf("attach result output=%+v caps=%+v, want the topic and its channel's capabilities", attached.Output, attached.Capabilities)
	}

	emptyPeer, empty, _ := connectSession(t, b, 102, boardCWD, "second-session")
	mustAttach(t, emptyPeer, ipc.AttachReq{Expr: "+board:notes"})
	if output := empty.OutputRoute(); output == nil || *output != secondBoard {
		t.Fatalf("output of a session holding nothing before=%v, want its board", output)
	}
	if bare := mustAttach(t, emptyPeer, ipc.AttachReq{}); bare.Channel != channel.BoardChannelName || bare.Name != "board:notes" {
		t.Fatalf("bare attach with a board output=%+v, want that board", bare)
	}
}

func TestSessionHoldsTopicWebChatAndSeveralBoards(t *testing.T) {
	b, _, _, _ := newBoardBroker(t)
	peer, stub, _ := connectSession(t, b, 101, boardCWD, boardSession)
	mustAttach(t, peer, ipc.AttachReq{Name: "c3", Replay: true})
	mustAttach(t, peer, ipc.AttachReq{Expr: "+web", Replay: true})
	mustAttach(t, peer, ipc.AttachReq{Expr: "+board:plan"})
	last := mustAttach(t, peer, ipc.AttachReq{Expr: "+board:notes"})
	if len(stub.Routes()) != 4 || len(last.Routes) != 4 {
		t.Fatalf("held=%v result=%+v, want topic, web chat and two boards", stub.Routes(), last.Routes)
	}
	if output := stub.OutputRoute(); output == nil || *output != webChatKey {
		t.Fatalf("output=%v, want the web chat", output)
	}
	for selector, want := range map[string]RouteKey{"c3": c3TopicKey, "web": webChatKey, "board:plan": firstBoard, "board:notes": secondBoard} {
		if got, err := b.resolveHeldRoute(stub, selector); err != nil || got != want {
			t.Errorf("selector %q => %v, %v; want %v", selector, got, err, want)
		}
	}
	if _, err := b.resolveHeldRoute(stub, channel.BoardChannelName); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("bare board selector with two boards: %v, want ambiguous", err)
	}
}

func TestSwitchAttachKeepsBoards(t *testing.T) {
	b, _, _, _ := newBoardBroker(t)
	peer, stub, _ := connectSession(t, b, 101, boardCWD, boardSession)
	mustAttach(t, peer, ipc.AttachReq{Name: "c3", Replay: true})
	mustAttach(t, peer, ipc.AttachReq{Expr: "+board:plan"})
	featureTopic := int64(412)
	mustAttach(t, peer, ipc.AttachReq{TopicID: &featureTopic, Group: "work", Replay: true})

	featureKey := MakeRouteKey("telegram", -200, &featureTopic)
	if routes := stub.Routes(); len(routes) != 2 || !stub.HasRoute(featureKey) || !stub.HasRoute(firstBoard) {
		t.Fatalf("held after switch=%v, want the new topic and the board", routes)
	}
	if _, held := b.Routes.Holder(c3TopicKey); held {
		t.Fatal("switch kept the old topic")
	}
	if holder, _ := b.Routes.Holder(firstBoard); holder != stub {
		t.Fatal("switch released the board")
	}
	if output := stub.OutputRoute(); output == nil || *output != featureKey {
		t.Fatalf("output=%v, want the switched-to topic", output)
	}
}

func TestBoardWaitsForItsSessionAndArrivesOnResume(t *testing.T) {
	b, _, _, boards := newBoardBroker(t)
	boards.mustCreate(t, "plan", sessionOwner)
	ref := mappings.RouteRef{Channel: channel.BoardChannelName, ChatID: 1, Name: "board:plan"}
	b.Mappings().UpsertSessionAttachment("claude", boardSession, mappings.SessionAttachment{
		Channel: ref.Channel, ChatID: ref.ChatID, Name: ref.Name, CWD: boardCWD,
		LastAttachedAt: time.Now().UTC(), Routes: []mappings.RouteRef{ref}, Output: &ref,
	})
	host := NewBrokerHost(b, channel.BoardChannelName)
	line := "[board plan · 1.5 · msg 412] make this shorter"
	if !host.Emit(&c3types.Inbound{Channel: channel.BoardChannelName, ChatID: 1, MessageID: 412, Text: line, Sender: c3types.Sender{UserID: 42}, Timestamp: time.Now()}) {
		t.Fatal("emit refused")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if count, err := host.HeldCount(1); err == nil && count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the board's action never waited in its queue")
		}
		time.Sleep(10 * time.Millisecond)
	}

	_, done, resp := recoverViaPeer(t, b, boardCWD, boardSession)
	defer done()
	if !resp.Recovered || resp.Channel != channel.BoardChannelName || resp.Name != "board:plan" || resp.QueuedCount != 1 {
		t.Fatalf("resume=%+v, want the board back with its waiting action", resp)
	}
	if len(resp.QueuedSummary) != 1 || !strings.Contains(resp.QueuedSummary[0].Preview, "[board plan · 1.5 · msg 412]") {
		t.Fatalf("resume preview=%+v", resp.QueuedSummary)
	}
	time.Sleep(100 * time.Millisecond)
	for _, reply := range boards.sendRepliesSnapshot() {
		if strings.HasPrefix(reply.Text, "🔄") {
			t.Fatalf("resume wrote its welcome into the board: %q", reply.Text)
		}
	}
}

// A conversation switch in place releases every route; the board then waits
// for the conversation that made it, never the new one.
func TestConversationSwitchLeavesTheBoardToItsConversation(t *testing.T) {
	b, _, _, _ := newBoardBroker(t)
	peer, stub, _ := connectSession(t, b, 101, boardCWD, boardSession)
	mustAttach(t, peer, ipc.AttachReq{Expr: "+board:plan"})
	if err := peer.WriteJSON(ipc.RecoverSessionReq{Op: ipc.OpRecoverSession, StableSessionID: "next-conversation", CWD: boardCWD}); err != nil {
		t.Fatal(err)
	}
	readOp(t, peer, ipc.OpRecoverSessionResult)
	if _, held := b.Routes.Holder(firstBoard); held || stub.HasRoute(firstBoard) {
		t.Fatal("the new conversation kept the old conversation's board")
	}
	if again := attachVia(t, peer, ipc.AttachReq{Expr: "+board:plan"}); again.OK {
		t.Fatalf("the new conversation claimed the old one's board: %+v", again)
	}
	_, resumed, _ := connectSession(t, b, 102, boardCWD, boardSession)
	if !resumed.HasRoute(firstBoard) {
		t.Fatal("the board's own conversation did not get it back on resume")
	}
}

func TestBrokerRestartRestoresTopicAndBoards(t *testing.T) {
	topicID := int64(281)
	for _, shape := range []struct {
		name    string
		replays []ipc.AttachReq
	}{
		{name: "one remembered topic attach (claude, agy, cursor, grok, desktop)", replays: []ipc.AttachReq{{TopicID: &topicID, Group: "main", ChatID: -100}}},
		{name: "each held route but boards, added (codex)", replays: []ipc.AttachReq{{Channel: "telegram", TopicID: &topicID, Group: "main", ChatID: -100, Add: true}}},
		{name: "an old adapter's remembered board attach", replays: []ipc.AttachReq{{Expr: "+board:plan"}}},
		{name: "nothing remembered"},
	} {
		t.Run(shape.name, func(t *testing.T) {
			b, _, _, boards := newBoardBroker(t)
			boards.mustCreate(t, "plan", sessionOwner)
			boards.mustCreate(t, "notes", sessionOwner)
			topic := mappings.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &topicID, Name: "c3", Group: "main"}
			b.Mappings().UpsertSessionAttachment("claude", boardSession, mappings.SessionAttachment{
				Channel: "telegram", ChatID: -100, TopicID: &topicID, Name: "c3", Group: "main", CWD: boardCWD,
				LastAttachedAt: time.Now().UTC(), Output: &topic, Routes: []mappings.RouteRef{
					topic,
					{Channel: channel.BoardChannelName, ChatID: 1, Name: "board:plan"},
					{Channel: channel.BoardChannelName, ChatID: 2, Name: "board:notes"},
				},
			})

			peer, stub, _ := connectSession(t, b, 101, boardCWD, "")
			for _, replay := range shape.replays {
				replay.Replay = true
				attachVia(t, peer, replay)
			}
			if err := peer.WriteJSON(ipc.RecoverSessionReq{Op: ipc.OpRecoverSession, StableSessionID: boardSession, CWD: boardCWD}); err != nil {
				t.Fatal(err)
			}
			readOp(t, peer, ipc.OpRecoverSessionResult)

			if !sameRouteSet(stub.Routes(), []RouteKey{c3TopicKey, firstBoard, secondBoard}) {
				t.Fatalf("held after restart=%v, want the topic and both boards", stub.Routes())
			}
			if output := stub.OutputRoute(); output == nil || *output != c3TopicKey {
				t.Fatalf("output after restart=%v, want the topic", output)
			}
		})
	}
}

func TestBoardWithoutStableIDEndsWithItsConnection(t *testing.T) {
	b, _, _, _ := newBoardBroker(t)
	// An exited process: its dropped connection releases its claims.
	peer, stub, drop := connectSession(t, b, deadPID(t), boardCWD, "")
	attached := mustAttach(t, peer, ipc.AttachReq{Expr: "+board:plan"})
	if !strings.Contains(attached.Notice, "ends with this connection") {
		t.Fatalf("attach notice does not say the board ends with the connection:\n%s", attached.Notice)
	}
	mustAttach(t, peer, ipc.AttachReq{Expr: "+board:plan"})

	drop()
	deadline := time.Now().Add(2 * time.Second)
	for stub.IsConnected() || func() bool { _, held := b.Routes.Holder(firstBoard); return held }() {
		if time.Now().After(deadline) {
			t.Fatal("the dropped connection kept the board")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, sid := range []string{"", boardSession} {
		next, _, _ := connectSession(t, b, 102, boardCWD, sid)
		if again := attachVia(t, next, ipc.AttachReq{Expr: "+board:plan", Steal: true}); again.OK {
			t.Fatalf("a later connection (id %q) re-held a board bound to an ended connection: %+v", sid, again)
		}
	}
}

func TestDetachTargetReleasesOneBoard(t *testing.T) {
	b, _, _, _ := newBoardBroker(t)
	peer, stub, _ := connectSession(t, b, 101, boardCWD, boardSession)
	mustAttach(t, peer, ipc.AttachReq{Name: "c3", Replay: true})
	mustAttach(t, peer, ipc.AttachReq{Expr: "+board:plan"})
	mustAttach(t, peer, ipc.AttachReq{Expr: "+board:notes"})

	resp, emptied := targetedReleaseForTest(t, b, stub, "board:plan")
	if emptied || !resp.OK || len(resp.Routes) != 2 {
		t.Fatalf("release=%+v emptied=%v", resp, emptied)
	}
	if _, held := b.Routes.Holder(firstBoard); held || stub.HasRoute(firstBoard) {
		t.Fatal("the detached board is still held")
	}
	if !stub.HasRoute(secondBoard) || !stub.HasRoute(c3TopicKey) {
		t.Fatalf("detach released more than one board: %v", stub.Routes())
	}
	if !sameRouteSet(storedRoutes(t, b, boardSession), []RouteKey{c3TopicKey, secondBoard}) {
		t.Fatalf("record=%v, want the topic and the other board", storedRoutes(t, b, boardSession))
	}
	mustAttach(t, peer, ipc.AttachReq{Expr: "+board:plan"})
	if holder, _ := b.Routes.Holder(firstBoard); holder != stub {
		t.Fatal("the owner could not hold the detached board again")
	}
}

func TestCloseReleasesTheBoardAndTellsTheSession(t *testing.T) {
	b, _, _, boards := newBoardBroker(t)
	peer, stub, _ := connectSession(t, b, 101, boardCWD, boardSession)
	mustAttach(t, peer, ipc.AttachReq{Name: "c3", Replay: true})
	mustAttach(t, peer, ipc.AttachReq{Expr: "+board:plan"})
	host := NewBrokerHost(b, channel.BoardChannelName)

	boards.markClosed("plan")
	if err := host.CloseBoardRoute(1); err != nil {
		t.Fatal(err)
	}
	var inbound ipc.InboundMsg
	if err := json.Unmarshal(readOp(t, peer, ipc.OpInbound), &inbound); err != nil {
		t.Fatal(err)
	}
	if event := inbound.Inbound.Event; event == nil || event.System == nil || event.System.Title != "Board closed" ||
		!strings.Contains(event.System.Message, "board:plan was closed") || !strings.Contains(event.System.Message, "replies go to telegram · c3") {
		t.Fatalf("session was told %+v", inbound.Inbound.Event)
	}
	if _, held := b.Routes.Holder(firstBoard); held || stub.HasRoute(firstBoard) {
		t.Fatal("closed board still held")
	}
	if keys := storedRoutes(t, b, boardSession); len(keys) != 1 || keys[0] != c3TopicKey {
		t.Fatalf("record after close=%v, want only the topic", keys)
	}
	if again := attachVia(t, peer, ipc.AttachReq{Expr: "+board:plan"}); again.OK {
		t.Fatalf("closed board claimed again: %+v", again)
	}

	t.Run("a board nobody holds leaves the record", func(t *testing.T) {
		boards.mustCreate(t, "notes", channel.BoardOwner{CLI: "claude", SessionID: "ended-session", CWD: boardCWD})
		ref := mappings.RouteRef{Channel: channel.BoardChannelName, ChatID: 2, Name: "board:notes"}
		b.Mappings().UpsertSessionAttachment("claude", "ended-session", mappings.SessionAttachment{
			Channel: ref.Channel, ChatID: ref.ChatID, Name: ref.Name, LastAttachedAt: time.Now().UTC(),
			Routes: []mappings.RouteRef{ref}, Output: &ref,
		})
		boards.markClosed("notes")
		if err := host.CloseBoardRoute(2); err != nil {
			t.Fatal(err)
		}
		if sa, _ := b.Mappings().LookupSessionAttachment("claude", "ended-session"); len(sa.Routes) != 0 {
			t.Fatalf("record still lists the closed board: %+v", sa.Routes)
		}
	})

	t.Run("a close never undoes a detach", func(t *testing.T) {
		b, _, _, boards := newBoardBroker(t)
		peer, stub, drop := connectSession(t, b, 101, boardCWD, boardSession)
		mustAttach(t, peer, ipc.AttachReq{Name: "c3", Replay: true})
		mustAttach(t, peer, ipc.AttachReq{Expr: "+board:plan"})
		_, brokerConn := newConnPair(t)
		if !b.handleRelease(brokerConn, stub, []byte(`{"op":"release"}`)) {
			t.Fatal("a bare detach left routes held")
		}
		drop()
		for deadline := time.Now().Add(5 * time.Second); len(b.Stubs.Snapshot()) != 0; time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the detached session's connection never ended")
			}
		}
		boards.markClosed("plan")
		if err := NewBrokerHost(b, channel.BoardChannelName).CloseBoardRoute(1); err != nil {
			t.Fatal(err)
		}
		if sa, _ := b.Mappings().LookupSessionAttachment("claude", boardSession); !sa.Detached || sa.Output == nil ||
			routeKeyFromRef(*sa.Output) != c3TopicKey || !sameRouteSet(storedRoutes(t, b, boardSession), []RouteKey{c3TopicKey}) {
			t.Fatalf("record after detach and close=%+v, want it still detached, its output kept, without the board", sa)
		}
		_, done, resp := recoverViaPeer(t, b, boardCWD, boardSession)
		defer done()
		if _, held := b.Routes.Holder(c3TopicKey); resp.Recovered || held {
			t.Fatalf("resume after detach and close=%+v (topic held %v), want nothing back", resp, held)
		}
	})
}

func TestWaitingCountComesFromTheQueue(t *testing.T) {
	b, _, _, _ := newBoardBroker(t)
	host := NewBrokerHost(b, channel.BoardChannelName)
	appendQueued(t, b, firstBoard, 1, "first")
	appendQueued(t, b, firstBoard, 2, "second")
	if count, err := host.HeldCount(1); err != nil || count != 2 {
		t.Fatalf("held count=%d, %v; want 2", count, err)
	}
	if count, err := host.HeldCount(2); err != nil || count != 0 {
		t.Fatalf("another board's held count=%d, %v; want 0", count, err)
	}
	if _, err := b.Queue.Consume(queueRouteKey(firstBoard), 2); err != nil {
		t.Fatal(err)
	}
	if count, err := host.HeldCount(1); err != nil || count != 0 {
		t.Fatalf("held count after delivery=%d, %v; want 0", count, err)
	}
	b.Queue = nil
	if _, err := host.HeldCount(1); err == nil {
		t.Fatal("a disabled queue reported a count")
	}
}

func TestWebSelectorNeverMeansABoard(t *testing.T) {
	b, _, _, boards := newBoardBroker(t)
	board := boards.mustCreate(t, "web", sessionOwner)
	boardKey := boardRouteKey(board.Number)
	both := b.Stubs.Register("claude", 11, boardCWD, nil)
	claimConfirmed(t, b, both, webChatKey)
	claimConfirmed(t, b, both, boardKey)
	if got, err := b.resolveHeldRoute(both, "web"); err != nil || got != webChatKey {
		t.Fatalf(`"web" => %v, %v; want the web chat`, got, err)
	}
	if got, err := b.resolveHeldRoute(both, "board:web"); err != nil || got != boardKey {
		t.Fatalf(`"board:web" => %v, %v; want the board`, got, err)
	}
	boardOnly := b.Stubs.Register("claude", 12, boardCWD, nil)
	b.Routes.ForceReleaseKey(boardKey)
	claimConfirmed(t, b, boardOnly, boardKey)
	if got, err := b.resolveHeldRoute(boardOnly, "web"); err == nil {
		t.Fatalf(`"web" on a session holding only a board named web => %v`, got)
	}
}

func permissionRequest(t *testing.T, b *Broker, stub *Stub, id string) {
	t.Helper()
	raw, _ := json.Marshal(ipc.PermissionReq{Op: ipc.OpPermissionRequest, RequestID: id, ToolName: "Bash", Preview: "go test ./..."})
	b.handlePermissionRequest(nil, stub, raw)
}

func TestPermissionPreviewNeverReachesABoard(t *testing.T) {
	t.Run("a keyboard route is held", func(t *testing.T) {
		b, telegram, _, boards := newBoardBroker(t)
		stub := b.Stubs.Register("claude", 11, boardCWD, nil)
		claimConfirmed(t, b, stub, c3TopicKey)
		claimConfirmed(t, b, stub, firstBoard)
		stub.SetOutputRoute(firstBoard)
		permissionRequest(t, b, stub, "perm-keyboard")
		if replies := telegram.sendRepliesSnapshot(); len(replies) != 1 || len(replies[0].Buttons) == 0 {
			t.Fatalf("telegram prompt=%+v", replies)
		}
		if replies := boards.sendRepliesSnapshot(); len(replies) != 0 {
			t.Fatalf("board got %+v", replies)
		}
	})
	t.Run("only the web chat and a board are held", func(t *testing.T) {
		b, _, web, boards := newBoardBroker(t)
		stub := b.Stubs.Register("claude", 11, boardCWD, nil)
		claimConfirmed(t, b, stub, webChatKey)
		claimConfirmed(t, b, stub, firstBoard)
		stub.SetOutputRoute(firstBoard)
		permissionRequest(t, b, stub, "perm-web")
		if replies := web.sendRepliesSnapshot(); len(replies) != 1 || !strings.Contains(replies[0].Text, "go test ./...") {
			t.Fatalf("web notice=%+v, want the preview", replies)
		}
		if replies := boards.sendRepliesSnapshot(); len(replies) != 0 {
			t.Fatalf("board got %+v", replies)
		}
	})
	for _, isKeyboardCapable := range []bool{false, true} {
		t.Run(fmt.Sprintf("only boards are held, keyboards declared %v", isKeyboardCapable), func(t *testing.T) {
			b, _, _, boards := newBoardBroker(t)
			boards.isKeyboardCapable = isKeyboardCapable
			stub := b.Stubs.Register("claude", 11, boardCWD, nil)
			claimConfirmed(t, b, stub, firstBoard)
			claimConfirmed(t, b, stub, secondBoard)
			stub.SetOutputRoute(firstBoard)
			permissionRequest(t, b, stub, "perm-boards")
			replies := boards.sendRepliesSnapshot()
			if len(replies) != 2 {
				t.Fatalf("board notices=%+v, want one per board", replies)
			}
			for _, reply := range replies {
				if reply.Text != boardPermissionNotice || len(reply.Buttons) != 0 {
					t.Fatalf("board notice=%+v, want %q without a preview or keyboard", reply, boardPermissionNotice)
				}
			}
		})
	}
	t.Run("a board that declares keyboards is never the prompt's route", func(t *testing.T) {
		b, telegram, _, boards := newBoardBroker(t)
		boards.isKeyboardCapable = true
		stub := b.Stubs.Register("claude", 11, boardCWD, nil)
		claimConfirmed(t, b, stub, c3TopicKey)
		claimConfirmed(t, b, stub, firstBoard)
		stub.SetOutputRoute(firstBoard)
		permissionRequest(t, b, stub, "perm-declared")
		if replies := telegram.sendRepliesSnapshot(); len(replies) != 1 || len(replies[0].Buttons) == 0 {
			t.Fatalf("telegram prompt=%+v, want the keyboard there", replies)
		}
		if replies := boards.sendRepliesSnapshot(); len(replies) != 0 {
			t.Fatalf("board got %+v", replies)
		}
	})
}

func TestQuestionsNeverReachABoard(t *testing.T) {
	ask := func(t *testing.T, b *Broker, peer *ipc.Conn, stub *Stub) ipc.AskRegisteredMsg {
		t.Helper()
		go b.handleAskRegister(stub.ConnValue().(*ipc.Conn), stub, []byte(`{"ask_id":"q1","question":"Ship it?","options":["yes","no"]}`))
		var registered ipc.AskRegisteredMsg
		if err := json.Unmarshal(readOp(t, peer, ipc.OpAskRegistered), &registered); err != nil {
			t.Fatal(err)
		}
		return registered
	}
	t.Run("a board output with a topic held", func(t *testing.T) {
		b, telegram, _, boards := newBoardBroker(t)
		boards.isKeyboardCapable = true
		peer, stub, _ := connectSession(t, b, 101, boardCWD, "")
		claimConfirmed(t, b, stub, c3TopicKey)
		claimConfirmed(t, b, stub, firstBoard)
		stub.SetOutputRoute(firstBoard)
		if registered := ask(t, b, peer, stub); !registered.OK {
			t.Fatalf("ask=%+v, want it on the topic", registered)
		}
		if replies := telegram.sendRepliesSnapshot(); len(replies) != 1 || replies[0].Text != "Ship it?" {
			t.Fatalf("telegram=%+v, want the question", replies)
		}
		if replies := boards.sendRepliesSnapshot(); len(replies) != 0 {
			t.Fatalf("board got %+v", replies)
		}
	})
	t.Run("only boards are held", func(t *testing.T) {
		b, _, _, boards := newBoardBroker(t)
		boards.isKeyboardCapable = true
		peer, stub, _ := connectSession(t, b, 101, boardCWD, "")
		claimConfirmed(t, b, stub, firstBoard)
		stub.SetOutputRoute(firstBoard)
		if registered := ask(t, b, peer, stub); registered.OK {
			t.Fatalf("ask=%+v, want it refused", registered)
		}
		if replies := boards.sendRepliesSnapshot(); len(replies) != 0 {
			t.Fatalf("board got %+v", replies)
		}
	})
}

// TestBrokerTextsNeverLandInABoardsLog: a text the broker sends a session's
// routes goes to its first non-board route; a board gets at most a strip-only
// text (isStatusText's prefixes), which never reaches its log or page.
func TestBrokerTextsNeverLandInABoardsLog(t *testing.T) {
	stripOnly := func(t *testing.T, boards *fakeBoardChannel, want int) {
		t.Helper()
		replies := boards.sendRepliesSnapshot()
		if len(replies) != want {
			t.Fatalf("board texts=%+v, want %d", replies, want)
		}
		for _, reply := range replies {
			if !strings.HasPrefix(reply.Text, "⏸ Permission") && !strings.HasPrefix(reply.Text, "⚠️") {
				t.Fatalf("board text %q would be a log line", reply.Text)
			}
		}
	}
	t.Run("a restart refusal goes to the web chat, not the board output", func(t *testing.T) {
		b, _, web, boards := newBoardBroker(t)
		boards.isKeyboardCapable = true
		stub := b.Stubs.Register("claude", 11, boardCWD, nil)
		claimConfirmed(t, b, stub, webChatKey)
		claimConfirmed(t, b, stub, firstBoard)
		stub.SetOutputRoute(firstBoard)
		b.BeginControlledRestart()
		permissionRequest(t, b, stub, "perm-restart")
		if replies := web.sendRepliesSnapshot(); len(replies) != 1 || replies[0].Text != restartPermRefused {
			t.Fatalf("web=%+v, want the refusal", replies)
		}
		stripOnly(t, boards, 0)
	})
	t.Run("a boards-only restart refusal is strip-only", func(t *testing.T) {
		b, _, _, boards := newBoardBroker(t)
		stub := b.Stubs.Register("claude", 11, boardCWD, nil)
		claimConfirmed(t, b, stub, firstBoard)
		claimConfirmed(t, b, stub, secondBoard)
		stub.SetOutputRoute(firstBoard)
		b.BeginControlledRestart()
		permissionRequest(t, b, stub, "perm-restart")
		stripOnly(t, boards, 2)
	})
	t.Run("the update notice reaches a board's strip only", func(t *testing.T) {
		b, telegram, _, boards := newBoardBroker(t)
		stub := b.Stubs.Register("claude", 11, boardCWD, nil)
		claimConfirmed(t, b, stub, c3TopicKey)
		claimConfirmed(t, b, stub, firstBoard)
		b.notifyUpdateRestart("v9.9.9")
		if replies := telegram.sendRepliesSnapshot(); len(replies) != 1 || !strings.HasPrefix(replies[0].Text, "c3 updated to v9.9.9") {
			t.Fatalf("telegram=%+v", replies)
		}
		stripOnly(t, boards, 1)
	})
	t.Run("a retranscribe with a board output records on the topic", func(t *testing.T) {
		b, _, _, _ := newBoardBroker(t)
		b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) { return "fresh transcript", nil })
		stub := b.Stubs.Register("claude", 11, boardCWD, nil)
		claimConfirmed(t, b, stub, c3TopicKey)
		claimConfirmed(t, b, stub, firstBoard)
		stub.SetOutputRoute(firstBoard)
		agentSide, brokerSide := newConnPair(t)
		raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "r", FileID: "vf", MessageID: 999})
		go b.handleRetranscribe(brokerSide, stub, raw)
		if resp := readRetranscribeResp(t, agentSide); resp.Err != "" || resp.Text != "fresh transcript" {
			t.Fatalf("retranscribe=%+v", resp)
		}
		if rows, err := b.Queue.Peek(queueRouteKey(firstBoard), -1); err != nil || len(rows) != 0 {
			t.Fatalf("board queue rows=%+v err=%v, want none", rows, err)
		}
		if rows, err := b.Queue.Peek(queueRouteKey(c3TopicKey), -1); err != nil || len(rows) != 1 || !strings.Contains(rows[0].Text, "voice message 999") {
			t.Fatalf("topic queue rows=%+v err=%v, want the transcript update", rows, err)
		}
	})
}

func TestBoardInputNeverSettlesAPermission(t *testing.T) {
	b, _, _, _ := newBoardBroker(t)
	sessionSide, adapterSide := net.Pipe()
	t.Cleanup(func() { _ = sessionSide.Close(); _ = adapterSide.Close() })
	go func() {
		adapter := ipc.NewConn(adapterSide)
		for {
			if _, err := adapter.ReadFrame(); err != nil {
				return
			}
		}
	}()
	stub := b.Stubs.Register("claude", 11, boardCWD, ipc.NewConn(sessionSide))
	claimConfirmed(t, b, stub, c3TopicKey)
	claimConfirmed(t, b, stub, firstBoard)
	stub.SetOutputRoute(firstBoard)
	permissionRequest(t, b, stub, "perm-board")
	isPending := func() bool {
		b.Perms.mu.Lock()
		defer b.Perms.mu.Unlock()
		return b.Perms.m["perm-board"] != nil
	}
	tap := &c3types.CallbackEvent{CallbackID: "cb", Actor: c3types.Sender{UserID: 42}, Data: permAllowData("perm-board")}
	b.resolvePerm(firstBoard, tap)
	if !isPending() {
		t.Fatal("an Allow arriving on the board settled the permission")
	}
	b.resolvePerm(c3TopicKey, tap)
	if isPending() {
		t.Fatal("the same Allow on the prompt's own keyboard route did not settle it")
	}
}
