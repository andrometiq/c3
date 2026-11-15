package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// fakeBoardHost is the broker side of a board: fakeHost's gate and Emit plus
// the BoardHost extension.
type fakeBoardHost struct {
	*fakeHost
	boardMu  sync.Mutex
	held     map[int64]int
	closed   []int64
	closeErr error
	onClose  func(number int64)
}

func (host *fakeBoardHost) CloseBoardRoute(number int64) error {
	host.boardMu.Lock()
	host.closed = append(host.closed, number)
	hook, err := host.onClose, host.closeErr
	host.boardMu.Unlock()
	if hook != nil {
		hook(number)
	}
	return err
}

func (host *fakeBoardHost) HeldCount(number int64) (int, error) {
	host.boardMu.Lock()
	defer host.boardMu.Unlock()
	return host.held[number], nil
}

func (host *fakeBoardHost) setHeld(number int64, count int) {
	host.boardMu.Lock()
	host.held[number] = count
	host.boardMu.Unlock()
}

var boardFixtures = fstest.MapFS{
	"board.html":         {Data: []byte(`<!doctype html><title>shell</title><iframe sandbox="allow-scripts"></iframe><script src="/board/assets/board.js"></script>`)},
	"board.js":           {Data: []byte("/* shell fixture */")},
	"board-sdk.js":       {Data: []byte("window.c3 = {fixture: 'sdk'};")},
	"board-starter.html": {Data: []byte(`<!doctype html><title>starter</title><div data-c3-part="1">one</div><script>c3.threads()</script>`)},
}

var testOwner = channel.BoardOwner{CLI: "claude", SessionID: "session-1", CWD: "/work/project"}

func newFakeBoardHost() *fakeBoardHost {
	return &fakeBoardHost{
		fakeHost: &fakeHost{telegram: telegramConfig{MasterUserID: 42}, registered: true, allowed: true},
		held:     make(map[int64]int),
	}
}

// startBoardChannel starts web's state and the board channel in the current
// XDG_STATE_HOME with fixture page files.
func startBoardChannel(t *testing.T, host *fakeBoardHost) *Channel {
	t.Helper()
	c := New()
	c.host = host
	c.operatorID = 42
	c.baseURL = "http://127.0.0.1:8371"
	c.allowedOrigin = map[string]bool{"http://127.0.0.1:8371": true, "http://localhost:8371": true}
	c.heartbeatInterval = time.Hour
	if err := c.loadState(host); err != nil {
		t.Fatal(err)
	}
	c.board.assets = boardFixtures
	if err := c.board.Start(context.Background(), host); err != nil {
		t.Fatal(err)
	}
	return c
}

// newBoardTest is a started board channel with one operator login.
func newBoardTest(t *testing.T) (*Channel, *fakeBoardHost, *http.Cookie) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	host := newFakeBoardHost()
	c := startBoardChannel(t, host)
	return c, host, loginCookie(t, c)
}

func loginCookie(t *testing.T, c *Channel) *http.Cookie {
	t.Helper()
	value, err := c.createSession(loginToken{userID: 42, username: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	return &http.Cookie{Name: sessionCookieName, Value: value}
}

func createBoard(t *testing.T, c *Channel, name string) channel.Board {
	t.Helper()
	board, isCreated, err := c.board.CreateBoard(name, testOwner)
	if err != nil || !isCreated {
		t.Fatalf("CreateBoard(%q)=%+v/%v/%v", name, board, isCreated, err)
	}
	return board
}

func writePageFile(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func sendPage(t *testing.T, c *Channel, number int64, contents string, replyTo int64) int64 {
	t.Helper()
	args := c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: number,
		Media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: writePageFile(t, "page.html", contents)}}}
	if replyTo > 0 {
		args.ReplyTo = &replyTo
	}
	id, err := c.board.SendReply(args)
	if err != nil {
		t.Fatalf("send page: %v", err)
	}
	return id
}

func boardLog(t *testing.T, c *Channel, name string) []boardLogEntry {
	t.Helper()
	var log []boardLogEntry
	if err := c.board.withBoardNamed(name, func(board *boardState) error {
		log = append(log, board.log...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return log
}

func boardStreamCount(c *Channel, name string) int {
	count := 0
	_ = c.board.withBoardNamed(name, func(board *boardState) error { count = len(board.streams); return nil })
	return count
}

func startBoardSSE(t *testing.T, c *Channel, cookie *http.Cookie, name, lastID string) (*streamRecorder, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := request(http.MethodGet, "/board/"+name+"/events", "", cookie, false).WithContext(ctx)
	if lastID != "" {
		r.Header.Set("Last-Event-ID", lastID)
	}
	before := boardStreamCount(c, name)
	recorder := newStreamRecorder()
	done := make(chan struct{})
	go func() {
		c.routes().ServeHTTP(recorder, r)
		close(done)
	}()
	waitFor(t, func() bool { return boardStreamCount(c, name) == before+1 })
	return recorder, func() { cancel(); <-done }
}

func TestBoardReplyReachesOnlyThatBoardsViewers(t *testing.T) {
	c, _, cookie := newBoardTest(t)
	plan, other := createBoard(t, c, "plan"), createBoard(t, c, "other")
	planStream, stopPlan := startBoardSSE(t, c, cookie, "plan", "")
	defer stopPlan()
	otherStream, stopOther := startBoardSSE(t, c, cookie, "other", "")
	defer stopOther()
	webStream, cancelWeb, _ := startSSE(c, cookie, "", "")
	defer cancelWeb()
	waitFor(t, func() bool { return streamCount(c) == 1 })

	id, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, Text: "answer for plan"})
	if err != nil || id != 1 {
		t.Fatalf("SendReply=%d/%v, want the board's first id", id, err)
	}
	waitFor(t, func() bool { _, body := planStream.snapshot(); return strings.Contains(body, "answer for plan") })
	if _, body := planStream.snapshot(); !strings.Contains(body, "id: 1\nevent: message\n") {
		t.Fatalf("plan stream lacks the logged message event: %q", body)
	}
	time.Sleep(20 * time.Millisecond)
	if _, body := otherStream.snapshot(); strings.Contains(body, "answer for plan") {
		t.Fatalf("other board's viewer got plan's reply: %q", body)
	}
	if _, body := webStream.snapshot(); strings.Contains(body, "answer for plan") || len(c.replay) != 0 {
		t.Fatalf("web chat got a board reply: stream %q replay %d", body, len(c.replay))
	}
	if log := boardLog(t, c, "other"); len(log) != 0 {
		t.Fatalf("other board logged %+v", log)
	}
	if _, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: other.Number + 10, Text: "x"}); !errors.Is(err, errNoBoard) {
		t.Fatalf("reply to an unknown board number: %v", err)
	}
	if _, err := c.board.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: plan.Number, Text: "x"}); err == nil {
		t.Fatal("a reply addressed to the web channel was taken by the board channel")
	}
	if _, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, Text: " \n"}); err == nil {
		t.Fatal("an empty reply was logged")
	}
	if log := boardLog(t, c, "plan"); len(log) != 1 {
		t.Fatalf("plan's log %+v, want only the one answer", log)
	}
}

func TestBoardRefusesNonHTMLMedia(t *testing.T) {
	c, _, _ := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	page := writePageFile(t, "page.html", "<p>ok</p>")
	directory := filepath.Join(t.TempDir(), "folder.html")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		args c3types.ReplyArgs
	}{
		{"photo", c3types.ReplyArgs{Media: []c3types.MediaItem{{Kind: c3types.MediaPhoto, Path: page}}}},
		{"png file", c3types.ReplyArgs{Media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: writePageFile(t, "image.png", "png")}}}},
		{"two files", c3types.ReplyArgs{Media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: page}, {Kind: c3types.MediaFile, Path: page}}}},
		{"url", c3types.ReplyArgs{Media: []c3types.MediaItem{{Kind: c3types.MediaFile, URL: "https://example.invalid/page.html"}}}},
		{"directory", c3types.ReplyArgs{Media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: directory}}}},
		{"missing", c3types.ReplyArgs{Media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: directory + "/none.html"}}}},
		{"too big", c3types.ReplyArgs{Media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: writePageFile(t, "big.html", strings.Repeat("x", channel.BoardPageMaxBytes+1))}}}},
		{"buttons", c3types.ReplyArgs{Text: "pick", Buttons: [][]c3types.Button{{{Text: "a", Data: "a"}}}}},
		{"poll", c3types.ReplyArgs{Poll: &c3types.PollSpec{Question: "q", Options: []string{"a", "b"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.args.Channel, test.args.ChatID = channel.BoardChannelName, plan.Number
			if _, err := c.board.SendReply(test.args); err == nil {
				t.Fatal("accepted")
			}
		})
	}
	if fifo := filepath.Join(t.TempDir(), "pipe.html"); exec.Command("mkfifo", fifo).Run() == nil {
		done := make(chan error, 1)
		go func() {
			_, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number,
				Media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: fifo}}})
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("a FIFO was taken as a page")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("sending a FIFO as the page blocked")
		}
	}
	if board, _ := c.board.BoardByName("plan"); board.Number != plan.Number || len(boardLog(t, c, "plan")) != 0 {
		t.Fatalf("a refused send left a trace: %+v", boardLog(t, c, "plan"))
	}
	sendPage(t, c, plan.Number, "<p>ok</p>", 0)
	if log := boardLog(t, c, "plan"); len(log) != 1 || log[0].Op != "page" || log[0].Rev != 1 {
		t.Fatalf("the one HTML file was not stored as rev 1: %+v", log)
	}
}

func TestNewRevisionIsStoredLoggedAndAnnounced(t *testing.T) {
	c, _, cookie := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	stream, stop := startBoardSSE(t, c, cookie, "plan", "")
	defer stop()
	answer, _ := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, Text: "draft"})

	for rev := 1; rev <= channel.BoardRevisionsKept+2; rev++ {
		sendPage(t, c, plan.Number, "<title>Plan v"+string(rune('0'+rev))+"</title><p>rev</p>", answer)
	}
	last := int64(channel.BoardRevisionsKept + 2)
	directory := filepath.Join(c.board.directory, "plan")
	for rev := int64(1); rev <= last; rev++ {
		info, err := os.Stat(boardRevisionPath(directory, rev))
		isKept := rev > last-channel.BoardRevisionsKept
		if isKept != (err == nil) {
			t.Fatalf("revision %d kept=%v, stat err %v", rev, isKept, err)
		}
		if isKept && info.Mode().Perm() != 0o600 {
			t.Fatalf("revision %d mode %v, want 0600", rev, info.Mode().Perm())
		}
	}
	record, err := os.ReadFile(filepath.Join(directory, boardRecordName))
	if err != nil || !strings.Contains(string(record), `"rev":7`) || !strings.Contains(string(record), `"title":"Plan v7"`) {
		t.Fatalf("board.json=%s/%v, want rev 7 and the page's title", record, err)
	}
	log := boardLog(t, c, "plan")
	page := log[len(log)-1]
	if page.Op != "page" || page.Rev != last || page.ReplyTo != answer || page.Dir != "out" {
		t.Fatalf("last log entry %+v, want the page at rev %d answering %d", page, last, answer)
	}
	waitFor(t, func() bool {
		_, body := stream.snapshot()
		return strings.Contains(body, `event: page`) && strings.Contains(body, `"rev":7`)
	})
	info, err := os.Stat(directory)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("board directory mode %v/%v, want 0700", info.Mode().Perm(), err)
	}
}

func TestLintWarnsButNeverRefuses(t *testing.T) {
	c, host, cookie := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	stream, stop := startBoardSSE(t, c, cookie, "plan", "")
	defer stop()

	sendPage(t, c, plan.Number, `<p data-c3-part="1">clean</p><script>document.body.dataset.x = 1</script>`, 0)
	if strings.Contains(host.logText(), "lint") {
		t.Fatalf("a clean page was warned about: %s", host.logText())
	}
	risky := `<form action="/x"></form><a href="https://example.invalid" target="_blank">x</a><script>localStorage.x = 1; fetch("/y")</script>`
	sendPage(t, c, plan.Number, risky, 0)
	board, _ := c.board.BoardByName("plan")
	served, err := c.board.revision(board.Name, "")
	if err != nil || string(served) != risky {
		t.Fatalf("the warned page was not stored as the current page: %q/%v", served, err)
	}
	logged := host.logText()
	for _, want := range []string{"storage", "network", "outside URL", "form", "new windows"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("lint log %q lacks %q", logged, want)
		}
	}
	waitFor(t, func() bool { _, body := stream.snapshot(); return strings.Contains(body, "event: notice") })
	if _, body := stream.snapshot(); strings.Count(body, "event: notice") != 1 {
		t.Fatalf("want one strip notice, for the risky page only: %q", body)
	}
}

func TestBoardSurvivesBrokerRestart(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	host := newFakeBoardHost()
	first := startBoardChannel(t, host)
	plan := createBoard(t, first, "plan")
	createBoard(t, first, "other")
	answer, _ := first.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, Text: "answer"})
	sendPage(t, first, plan.Number, "<title>Plan</title><p>v1</p>", answer)
	logPath := filepath.Join(first.board.directory, "plan", boardLogName)
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString(`{"seq":3,"at":"2026-`) // a crash mid-append
	_ = file.Close()

	host.queuedMax = 40 // an inbound the broker queued but the log never saw
	second := startBoardChannel(t, host)
	restored, ok := second.board.BoardByName("plan")
	if !ok || restored.Number != plan.Number || restored.Owner != testOwner || restored.IsClosed {
		t.Fatalf("restored board %+v/%v, want %+v", restored, ok, plan)
	}
	if log := boardLog(t, second, "plan"); len(log) != 2 || log[1].Rev != 1 {
		t.Fatalf("restored log %+v, want the message and the page", log)
	}
	if page, err := second.board.revision("plan", ""); err != nil || string(page) != "<title>Plan</title><p>v1</p>" {
		t.Fatalf("restored page %q/%v", page, err)
	}
	id, err := second.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, Text: "after restart"})
	if err != nil || id != 41 {
		t.Fatalf("first id after restart=%d/%v, want 41 (above the held queue)", id, err)
	}
	if log := boardLog(t, second, "plan"); log[len(log)-1].Seq != 3 {
		t.Fatalf("seq after the torn line=%d, want 3", log[len(log)-1].Seq)
	}
	if lines := strings.Count(readFile(t, logPath), "\n"); lines != 3 {
		t.Fatalf("log has %d lines after the torn one was cut, want 3", lines)
	}
	third, _, err := second.board.CreateBoard("third", testOwner)
	if err != nil || third.Number != 3 {
		t.Fatalf("next board number=%d/%v, want 3 (never reused)", third.Number, err)
	}
	if log := boardLog(t, startBoardChannel(t, host), "plan"); len(log) != 3 || log[2].Text != "after restart" {
		t.Fatalf("the entry appended after the torn line did not survive the next restart: %+v", log)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestUnreadableBoardRecordStopsTheBoardChannel(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	host := newFakeBoardHost()
	first := startBoardChannel(t, host)
	createBoard(t, first, "plan")
	if err := os.WriteFile(filepath.Join(first.board.directory, "plan", boardRecordName), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	second := New()
	second.host = host
	if err := second.loadState(host); err != nil {
		t.Fatal(err)
	}
	if err := second.board.Start(context.Background(), host); err == nil || !strings.Contains(err.Error(), "plan") {
		t.Fatalf("Start with an unreadable board.json: %v, want an error naming the board", err)
	}
	if _, _, err := second.board.CreateBoard("next", testOwner); !errors.Is(err, errBoardsStopped) {
		t.Fatalf("a board was created on a channel that failed to start: %v", err)
	}
}

func TestBoardKeeperRules(t *testing.T) {
	c, _, _ := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	if plan.Number != 1 || plan.Link != "http://127.0.0.1:8371/board/plan" || plan.Owner != testOwner {
		t.Fatalf("created board %+v", plan)
	}
	again, isCreated, err := c.board.CreateBoard("plan", channel.BoardOwner{CLI: "codex", SessionID: "other", CWD: "/elsewhere"})
	if err != nil || isCreated || again != plan {
		t.Fatalf("taken name returned %+v/%v/%v, want the stored board unchanged", again, isCreated, err)
	}
	for _, name := range []string{"assets", "Plan", "-plan", "../plan", ""} {
		if _, _, err := c.board.CreateBoard(name, testOwner); err == nil {
			t.Fatalf("CreateBoard(%q) accepted", name)
		}
	}
	if byNumber, ok := c.board.BoardByNumber(plan.Number); !ok || byNumber != plan {
		t.Fatalf("BoardByNumber=%+v/%v", byNumber, ok)
	}
	if boards := c.board.Boards(); len(boards) != 1 || boards[0] != plan {
		t.Fatalf("Boards=%+v", boards)
	}
	if starter, err := c.board.StarterHTML(); err != nil || !strings.Contains(starter, "c3.threads()") {
		t.Fatalf("StarterHTML=%q/%v", starter, err)
	}
	c.board.assets = fstest.MapFS{}
	if _, err := c.board.StarterHTML(); err == nil {
		t.Fatal("StarterHTML succeeded on a build without the starter")
	}
}

func TestSessionEditsOnlyItsOwnBoardMessages(t *testing.T) {
	c, _, cookie := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	answer, _ := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, Text: "draft"})
	response := postAct(c, cookie, "plan", `{"client_id":"c1","op":"comment","part":"1.5","text":"mine"}`)
	var accepted struct{ ID int64 }
	_ = json.Unmarshal(response.Body.Bytes(), &accepted)
	if accepted.ID == 0 {
		t.Fatalf("comment status %d body %s", response.Code, response.Body.String())
	}
	if _, err := c.board.EditMessage(c3types.EditArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, MessageID: accepted.ID, Text: "rewritten"}); err == nil {
		t.Fatal("the session edited the viewer's comment")
	}
	if _, err := c.board.EditMessage(c3types.EditArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, MessageID: answer, Text: "final"}); err != nil {
		t.Fatalf("edit of the session's own message: %v", err)
	}
	if err := c.board.React(c3types.ReactArgs{ChatID: plan.Number, MessageID: accepted.ID, Emoji: "👍"}); err != nil {
		t.Fatalf("react to the viewer's comment: %v", err)
	}
	if err := c.board.React(c3types.ReactArgs{ChatID: plan.Number, MessageID: accepted.ID, Emoji: "👍\n[board x"}); err == nil {
		t.Fatal("a multi-line reaction was accepted")
	}
	log := boardLog(t, c, "plan")
	edit, reaction := log[len(log)-2], log[len(log)-1]
	if edit.Op != "edit" || edit.ID != answer || edit.Text != "final" || reaction.Op != "reaction" || reaction.Ref != accepted.ID || reaction.Part != "1.5" {
		t.Fatalf("edit/reaction entries %+v / %+v", edit, reaction)
	}
}

func TestWaitingCountComesFromTheQueue(t *testing.T) {
	c, host, cookie := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	host.setHeld(plan.Number, 2)
	if state := boardStateOf(t, c, cookie, "plan"); state.Waiting != 2 || state.Live {
		t.Fatalf("state waiting/live=%d/%v, want 2/false", state.Waiting, state.Live)
	}
	stream, stop := startBoardSSE(t, c, cookie, "plan", "")
	defer stop()
	host.setHeld(plan.Number, 3)
	if response := postAct(c, cookie, "plan", `{"client_id":"c1","op":"comment","part":"1","text":"hi"}`); response.Code != http.StatusAccepted {
		t.Fatalf("act=%d %s", response.Code, response.Body.String())
	}
	waitFor(t, func() bool { _, body := stream.snapshot(); return strings.Contains(body, `"waiting":3`) })
	host.setHeld(plan.Number, 0)
	c.board.SetRouteHolder(plan.Number, nil, &channel.RouteHolder{CLI: "claude", CWD: "/work/project", SessionID: "session-1"})
	waitFor(t, func() bool {
		_, body := stream.snapshot()
		return strings.Contains(body, `"waiting":0`) && strings.Contains(body, `"live":true`)
	})
	if state := boardStateOf(t, c, cookie, "plan"); !state.Live || state.Holder["cli"] != "claude" || state.Holder["cwd"] != "/work/project" {
		t.Fatalf("state after holder change %+v", state)
	}
	host.setHeld(plan.Number, 1)
	if _, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, Text: "📨 Held — nothing lost. 1 message queued."}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, body := stream.snapshot(); return strings.Contains(body, `"waiting":1`) })
	if log := boardLog(t, c, "plan"); log[len(log)-1].Op != "comment" {
		t.Fatalf("a Held notice reached the log: %+v", log[len(log)-1])
	}
}

func TestCloseReleasesTheBoardAndTellsTheSession(t *testing.T) {
	c, host, cookie := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	answer, _ := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, Text: "draft"})
	closedFirst := false
	host.onClose = func(number int64) {
		board, _ := c.board.BoardByNumber(number)
		closedFirst = board.IsClosed
	}
	stream, stop := startBoardSSE(t, c, cookie, "plan", "")
	defer stop()
	w := httptest.NewRecorder()
	c.routes().ServeHTTP(w, request(http.MethodPost, "/board/plan/close", "", cookie, true))
	if w.Code != http.StatusNoContent || len(host.closed) != 1 || host.closed[0] != plan.Number || !closedFirst {
		t.Fatalf("close=%d released=%v closed-before-release=%v", w.Code, host.closed, closedFirst)
	}
	waitFor(t, func() bool { _, body := stream.snapshot(); return strings.Contains(body, `"closed":true`) })
	act := postAct(c, cookie, "plan", `{"client_id":"c2","op":"approve","ref":`+itoa(answer)+`}`)
	if act.Code != http.StatusConflict || !strings.Contains(act.Body.String(), `"closed"`) {
		t.Fatalf("act on a closed board=%d %s, want 409 closed", act.Code, act.Body.String())
	}
	if _, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, Text: "late"}); !errors.Is(err, errBoardClosed) {
		t.Fatalf("reply to a closed board: %v", err)
	}
	if !strings.Contains(readFile(t, filepath.Join(c.board.directory, "plan", boardRecordName)), `"closed":true`) {
		t.Fatal("closed was not stored")
	}
	host.closeErr = errors.New("route still held")
	w = httptest.NewRecorder()
	c.routes().ServeHTTP(w, request(http.MethodPost, "/board/plan/close", "", cookie, true))
	if w.Code != http.StatusInternalServerError || len(host.closed) != 2 {
		t.Fatalf("a repeated close after a failed release=%d released=%v, want a second release try and 500", w.Code, host.closed)
	}
}
