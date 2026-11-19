package web

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// postVoice posts a recording as a voice act; headers name its fields.
func postVoice(c *Channel, cookie *http.Cookie, name string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/board/"+name+"/act", bytes.NewReader(body))
	r.RemoteAddr = "192.0.2.1:1234"
	if cookie != nil {
		r.AddCookie(cookie)
	}
	r.Header.Set("Origin", "http://127.0.0.1:8371")
	r.Header.Set("Content-Type", "audio/wav")
	for key, value := range headers {
		if value == "" {
			r.Header.Del(key)
		} else {
			r.Header.Set(key, value)
		}
	}
	w := httptest.NewRecorder()
	c.routes().ServeHTTP(w, r)
	return w
}

// logVoiceAct puts a voice act in a board's log, as an accepted upload does.
func logVoiceAct(t *testing.T, c *Channel, name, part string) int64 {
	t.Helper()
	var id int64
	if err := c.board.withBoardNamed(name, func(board *boardState) error {
		id = board.nextID()
		return c.board.appendLocked(board, boardLogEntry{Dir: "in", ID: id, Op: "voice", Part: part, From: "operator"})
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

// The owner's recording reaches the session as an ordinary board line on the
// part the shell recorded for, carrying the stored audio for the broker's STT;
// a retry of its client id is the same act.
func TestBoardVoiceActReachesTheSessionAsABoardLine(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available; real voice transcode test skipped")
	}
	c, host, cookie := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	headers := map[string]string{"X-Client-Id": "voice-1", "X-Board-Part": "1.5"}
	wav := sineWAV(t, time.Second)
	first := postVoice(c, cookie, "plan", wav, headers)
	retry := postVoice(c, cookie, "plan", wav, headers)
	var accepted struct{ ID int64 }
	if first.Code != http.StatusAccepted || json.Unmarshal(first.Body.Bytes(), &accepted) != nil || retry.Body.String() != first.Body.String() {
		t.Fatalf("first=%d %s retry=%d %s", first.Code, first.Body, retry.Code, retry.Body)
	}
	emitted := host.emittedSnapshot()
	if len(emitted) != 1 {
		t.Fatalf("emitted %d inbounds, want one", len(emitted))
	}
	inbound := emitted[0]
	want := "[board plan · 1.5 · msg " + itoa(accepted.ID) + "] Voice note:"
	if inbound.Channel != channel.BoardChannelName || inbound.ChatID != plan.Number || inbound.Text != want || len(inbound.Attachments) != 1 || inbound.Attachments[0].Kind != "voice" {
		t.Fatalf("inbound=%+v, want %q with one voice attachment on board %d", inbound, want, plan.Number)
	}
	if path, err := c.board.LocalAudioPath(inbound.Attachments[0].FileID); err != nil || path == "" {
		t.Fatalf("board audio path=%q/%v, want the stored recording", path, err)
	}
	log := boardLog(t, c, "plan")
	if len(log) != 1 || log[0].Op != "voice" || log[0].Dir != "in" || log[0].ID != accepted.ID || log[0].Part != "1.5" || log[0].Text != "" {
		t.Fatalf("log=%+v, want one voice act on 1.5", log)
	}
	if len(c.clientMessages) != 0 {
		t.Fatal("a board voice act touched the web chat's records")
	}
}

// A voice act gets an act's checks: sign-in, same origin, a part, a client id,
// an open board and the size limit; its fields come only from the shell's
// headers, and JSON can't make one.
func TestBoardVoiceActTrustBoundary(t *testing.T) {
	c, host, cookie := newBoardTest(t)
	createBoard(t, c, "plan")
	shut := createBoard(t, c, "shut")
	if err := c.board.withBoard(shut.Number, func(board *boardState) error { board.record.Closed = true; return nil }); err != nil {
		t.Fatal(err)
	}
	valid := func(changes map[string]string) map[string]string {
		headers := map[string]string{"X-Client-Id": "voice-x", "X-Board-Part": "1"}
		for key, value := range changes {
			headers[key] = value
		}
		return headers
	}
	tests := []struct {
		name    string
		board   string
		cookie  *http.Cookie
		headers map[string]string
		body    []byte
		want    int
	}{
		{"signed out", "plan", nil, valid(nil), []byte("x"), http.StatusUnauthorized},
		{"cross site", "plan", cookie, valid(map[string]string{"Origin": "https://evil.example"}), []byte("x"), http.StatusForbidden},
		{"no part", "plan", cookie, valid(map[string]string{"X-Board-Part": ""}), []byte("x"), http.StatusBadRequest},
		{"bad part", "plan", cookie, valid(map[string]string{"X-Board-Part": "a b"}), []byte("x"), http.StatusBadRequest},
		{"no client id", "plan", cookie, valid(map[string]string{"X-Client-Id": ""}), []byte("x"), http.StatusBadRequest},
		{"segment zero", "plan", cookie, valid(map[string]string{"X-Voice-Segment": "0"}), []byte("x"), http.StatusBadRequest},
		{"segment padded", "plan", cookie, valid(map[string]string{"X-Voice-Segment": "01"}), []byte("x"), http.StatusBadRequest},
		{"more without segment", "plan", cookie, valid(map[string]string{"X-Voice-More": "1"}), []byte("x"), http.StatusBadRequest},
		{"more not one", "plan", cookie, valid(map[string]string{"X-Voice-Segment": "2", "X-Voice-More": "yes"}), []byte("x"), http.StatusBadRequest},
		{"recording id with a space", "plan", cookie, valid(map[string]string{"X-Voice-Recording": "a b"}), []byte("x"), http.StatusBadRequest},
		{"read-only board", "shut", cookie, valid(nil), []byte("x"), http.StatusConflict},
		{"too big", "plan", cookie, valid(nil), make([]byte, voiceBodyLimit+1), http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if response := postVoice(c, test.cookie, test.board, test.body, test.headers); response.Code != test.want {
				t.Fatalf("status=%d %s, want %d", response.Code, response.Body, test.want)
			}
		})
	}
	if response := postAct(c, cookie, "plan", `{"client_id":"json-voice","op":"voice","part":"1"}`); response.Code != http.StatusBadRequest {
		t.Fatalf("JSON voice act=%d %s, want 400", response.Code, response.Body)
	}
	if len(host.emittedSnapshot()) != 0 || len(boardLog(t, c, "plan")) != 0 {
		t.Fatal("a refused voice act reached the session or the log")
	}
}

// Each part of a long recording tells the session whether more follows.
func TestVoicePartsTellTheSessionWhetherMoreFollows(t *testing.T) {
	for _, test := range []struct {
		act  boardAct
		want string
	}{
		{boardAct{Op: "voice", Part: "2"}, "[board plan · 2 · msg 7] Voice note:"},
		{boardAct{Op: "voice", Part: "2", Segment: 1, HasMore: true}, "[board plan · 2 · msg 7] Voice note (part 1; more follows):"},
		{boardAct{Op: "voice", Part: "2", Segment: 3}, "[board plan · 2 · msg 7] Voice note (part 3, the last):"},
	} {
		if got := boardLine("plan", test.act, 7); got != test.want {
			t.Errorf("boardLine(%+v)=%q, want %q", test.act, got, test.want)
		}
	}
	act, isValid := voiceAct(http.Header{"X-Client-Id": {"c"}, "X-Board-Part": {"2"}, "X-Voice-Segment": {"4"}, "X-Voice-More": {"1"}})
	if !isValid || act.Segment != 4 || !act.HasMore {
		t.Fatalf("voiceAct=%+v/%v, want part 4 with more", act, isValid)
	}
}

// A voice act's transcript, and C3's notices about it, arrive as edits of the
// act with its live status, not as the session's messages.
func TestVoiceStatusArrivesAsAnEditOfTheAct(t *testing.T) {
	c, _, cookie := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	stream, stop := startBoardSSE(t, c, cookie, "plan", "")
	defer stop()
	heard, quiet, broken := logVoiceAct(t, c, "plan", "1.5"), logVoiceAct(t, c, "plan", "2"), logVoiceAct(t, c, "plan", "3")
	if _, err := c.board.SendReadback(c3types.ReadbackArgs{ChatID: plan.Number, ReplyTo: &heard, Transcript: "make it shorter"}); err != nil {
		t.Fatal(err)
	}
	for _, status := range []c3types.VoiceStatusArgs{
		{ChatID: plan.Number, MessageID: quiet, Status: c3types.VoiceStatusSlow, Text: "slow"},
		{ChatID: plan.Number, MessageID: quiet, Status: c3types.VoiceStatusNoSpeech, Text: "nothing"},
		{ChatID: plan.Number, MessageID: broken, Status: c3types.VoiceStatusFailed, Text: c3types.VoiceTranscriptionFailureNoticePrefix + " — see logs / try again."},
	} {
		if err := c.board.SendVoiceStatus(status); err != nil {
			t.Fatalf("%+v: %v", status, err)
		}
	}
	log := boardLog(t, c, "plan")
	got := make([]string, 0, len(log))
	for _, entry := range log[3:] {
		got = append(got, fmt.Sprintf("%s %s %d %s %s %q", entry.Dir, entry.Op, entry.ID, entry.Status, entry.Part, entry.Text))
	}
	want := []string{
		fmt.Sprintf("in edit %d heard 1.5 %q", heard, "make it shorter"),
		fmt.Sprintf("in edit %d slow 2 %q", quiet, c3types.VoiceSlowNotice),
		fmt.Sprintf("in edit %d no_speech 2 %q", quiet, c3types.VoiceNoSpeechNotice),
		fmt.Sprintf("in edit %d failed 3 %q", broken, c3types.VoiceTranscriptionFailureNoticePrefix+" — see logs / try again."),
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("log=%q, want %q", got, want)
	}
	events := func() string { _, body := stream.snapshot(); return body }
	waitFor(t, func() bool { return strings.Count(events(), "event: edit\n") == 4 })
	if strings.Count(events(), "event: action\n") != 3 {
		t.Fatalf("stream=%s, want the three voice acts as the only actions", events())
	}
	answer, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, ReplyTo: &heard, Text: "Shorter it is."})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.board.SendReadback(c3types.ReadbackArgs{ChatID: plan.Number, ReplyTo: &answer, Transcript: "forged"}); err == nil {
		t.Fatal("a readback edited the session's own message")
	}
	if err := c.board.SendVoiceStatus(c3types.VoiceStatusArgs{ChatID: plan.Number, MessageID: answer, Status: c3types.VoiceStatusFailed, Text: "forged"}); err == nil {
		t.Fatal("a voice status edited the session's own message")
	}
}

// Only the broker sets a voice act's status: whatever the session's reply
// says, it is the session's own message, and a read-only board takes none.
func TestSessionReplyNeverSetsAVoiceStatus(t *testing.T) {
	c, _, _ := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	voice := logVoiceAct(t, c, "plan", "1.5")
	notices := []string{c3types.VoiceSlowNotice, c3types.VoiceNoSpeechNotice, c3types.VoiceTranscriptionFailureNoticePrefix + " — see logs / try again."}
	for _, notice := range notices {
		if id, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, ReplyTo: &voice, Text: notice}); err != nil || id == voice {
			t.Fatalf("reply %q: id=%d err=%v, want a message of its own", notice, id, err)
		}
	}
	for _, entry := range boardLog(t, c, "plan")[1:] {
		if entry.Dir != "out" || entry.Op != "message" || entry.Status != "" || entry.ReplyTo != voice {
			t.Fatalf("entry=%+v, want the session's answer to the voice act", entry)
		}
	}
	if err := c.board.withBoard(plan.Number, func(board *boardState) error { board.record.Closed = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, ReplyTo: &voice, Text: notices[1]}); !errors.Is(err, errBoardClosed) {
		t.Fatalf("reply on a read-only board: err=%v, want closed", err)
	}
	if len(boardLog(t, c, "plan")) != 1+len(notices) {
		t.Fatal("a read-only board logged the session's reply")
	}
}

// A voice act takes one final status (heard, no speech or failed), and a slow
// one only before it; a read-only board still takes the owner's act's status.
func TestVoiceActTakesOneFinalStatus(t *testing.T) {
	c, _, _ := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	voice := logVoiceAct(t, c, "plan", "1.5")
	status := func(name string) error {
		return c.board.SendVoiceStatus(c3types.VoiceStatusArgs{ChatID: plan.Number, MessageID: voice, Status: name, Text: "C3's notice"})
	}
	if err := status("transcribed"); err == nil {
		t.Fatal("an unknown status was taken")
	}
	if err := status(c3types.VoiceStatusSlow); err != nil {
		t.Fatal(err)
	}
	if err := status(c3types.VoiceStatusSlow); !errors.Is(err, errVoiceSettled) {
		t.Fatalf("second slow: err=%v, want settled", err)
	}
	if err := c.board.withBoard(plan.Number, func(board *boardState) error { board.record.Closed = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := c.board.SendReadback(c3types.ReadbackArgs{ChatID: plan.Number, ReplyTo: &voice, Transcript: "make it shorter"}); err != nil {
		t.Fatalf("transcript after slow, on a read-only board: %v", err)
	}
	for _, name := range []string{c3types.VoiceStatusFailed, c3types.VoiceStatusNoSpeech, c3types.VoiceStatusSlow} {
		if err := status(name); !errors.Is(err, errVoiceSettled) {
			t.Fatalf("%s after heard: err=%v, want settled", name, err)
		}
	}
	if _, err := c.board.SendReadback(c3types.ReadbackArgs{ChatID: plan.Number, ReplyTo: &voice, Transcript: "something else"}); !errors.Is(err, errVoiceSettled) {
		t.Fatalf("second transcript: err=%v, want settled", err)
	}
	if log := boardLog(t, c, "plan"); len(log) != 3 || log[2].Status != c3types.VoiceStatusHeard || log[2].Text != "make it shorter" {
		t.Fatalf("log=%+v, want the act, slow, then the one transcript", log)
	}
}

// Every part of a long recording reaches the session, so it can tell when it
// has them all: a part whose audio never reached C3 comes as a line saying so,
// with its number and whether more follow.
func TestEveryPartOfARecordingReachesTheSession(t *testing.T) {
	c, host, cookie := newBoardTest(t)
	createBoard(t, c, "plan")
	unsent := func(client, segment, more, reason string) map[string]string {
		return map[string]string{"X-Client-Id": client, "X-Board-Part": "1.5", "X-Voice-Segment": segment, "X-Voice-More": more, "X-Voice-Unsent": reason}
	}
	for _, test := range []struct {
		name    string
		headers map[string]string
		body    []byte
	}{
		{"whole recording", unsent("bad-1", "", "", "cancelled"), nil},
		{"cancelled with more", unsent("bad-2", "2", "1", "cancelled"), nil},
		{"unknown reason", unsent("bad-3", "2", "", "lost"), nil},
		{"with audio", unsent("bad-4", "2", "", "refused"), []byte("audio")},
	} {
		if response := postVoice(c, cookie, "plan", test.body, test.headers); response.Code != http.StatusBadRequest {
			t.Fatalf("%s: status=%d %s, want 400", test.name, response.Code, response.Body)
		}
	}
	refused := postVoice(c, cookie, "plan", nil, unsent("part-2", "2", "1", "refused"))
	cancelled := postVoice(c, cookie, "plan", nil, unsent("part-3", "3", "", "cancelled"))
	retry := postVoice(c, cookie, "plan", nil, unsent("part-3", "3", "", "cancelled"))
	if refused.Code != http.StatusAccepted || cancelled.Code != http.StatusAccepted || retry.Body.String() != cancelled.Body.String() {
		t.Fatalf("refused=%d %s cancelled=%d %s retry=%s", refused.Code, refused.Body, cancelled.Code, cancelled.Body, retry.Body)
	}
	emitted := host.emittedSnapshot()
	if len(emitted) != 2 {
		t.Fatalf("emitted %d inbounds, want the two parts", len(emitted))
	}
	want := []string{
		"[board plan · 1.5 · msg " + itoa(emitted[0].MessageID) + "] Voice note (part 2; more follows): C3 couldn't take this part; it was not sent.",
		"[board plan · 1.5 · msg " + itoa(emitted[1].MessageID) + "] Voice note (part 3, the last): the recording was cancelled; this part was not sent.",
	}
	for i, inbound := range emitted {
		if inbound.Text != want[i] || len(inbound.Attachments) != 0 {
			t.Fatalf("inbound %d=%+v, want %q with no audio", i, inbound, want[i])
		}
	}
	log := boardLog(t, c, "plan")
	if len(log) != 2 || log[0].Op != "voice" || log[0].Segment != 2 || !log[0].HasMore || log[0].Status != "refused" ||
		log[1].Op != "voice" || log[1].Segment != 3 || log[1].HasMore || log[1].Status != "cancelled" {
		t.Fatalf("log=%+v, want parts 2 (refused, more) and 3 (cancelled, the last)", log)
	}
}

// The board skill tells the session that parts can arrive out of order, and
// to act only once it has every part up to the one marked the last.
func TestBoardSkillWaitsForEveryPartUpToTheLast(t *testing.T) {
	skill, err := os.ReadFile(filepath.Join("..", "..", "..", "plugins", "c3", "skills", "board", "SKILL.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []string{"out of order", "parts 1 to N are all in", "N is the part marked `the last`"} {
		if !strings.Contains(string(skill), rule) {
			t.Errorf("the board skill does not say %q", rule)
		}
	}
}

// fakeFFmpeg puts an ffmpeg first on PATH that writes a small file in place
// of the transcode. With a gate directory it first creates gate/started and
// waits for gate/release, so a test can act while a voice note transcodes.
func fakeFFmpeg(t *testing.T, gate string) {
	t.Helper()
	directory := t.TempDir()
	script := "#!/bin/sh\n"
	if gate != "" {
		script += fmt.Sprintf("touch '%s'\nwhile [ ! -e '%s' ]; do sleep 0.01; done\n", filepath.Join(gate, "started"), filepath.Join(gate, "release"))
		t.Cleanup(func() { _ = os.WriteFile(filepath.Join(gate, "release"), nil, 0o600) })
	}
	script += "for output; do :; done\nprintf 'OggS' > \"$output\"\n"
	if err := os.WriteFile(filepath.Join(directory, "ffmpeg"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// A voice act's client id names its audio with its part, segment and whether
// more follow: a retry that changes any of them is a conflict, as for a text act.
func TestVoiceRetryMustRepeatThePartFields(t *testing.T) {
	fakeFFmpeg(t, "")
	c, host, cookie := newBoardTest(t)
	createBoard(t, c, "plan")
	headers := func(changes map[string]string) map[string]string {
		fields := map[string]string{"X-Client-Id": "voice-1", "X-Board-Part": "1.5", "X-Voice-Segment": "1", "X-Voice-More": "1"}
		for key, value := range changes {
			fields[key] = value
		}
		return fields
	}
	audio := []byte("the same audio")
	first := postVoice(c, cookie, "plan", audio, headers(nil))
	if first.Code != http.StatusAccepted {
		t.Fatalf("first=%d %s", first.Code, first.Body)
	}
	for _, changes := range []map[string]string{{"X-Board-Part": "2"}, {"X-Voice-Segment": "2"}, {"X-Voice-More": ""}, {"X-Voice-Recording": "other"}} {
		if response := postVoice(c, cookie, "plan", audio, headers(changes)); response.Code != http.StatusConflict {
			t.Fatalf("retry with %v: status=%d %s, want 409", changes, response.Code, response.Body)
		}
	}
	if retry := postVoice(c, cookie, "plan", audio, headers(nil)); retry.Code != http.StatusAccepted || retry.Body.String() != first.Body.String() {
		t.Fatalf("retry=%d %s, want the first act", retry.Code, retry.Body)
	}
	if len(host.emittedSnapshot()) != 1 || len(boardLog(t, c, "plan")) != 1 {
		t.Fatal("a conflicting retry reached the session or the log")
	}
}

// A board made read-only while a voice act's audio transcodes takes it no
// more: it is refused, not sent, logged or kept.
func TestVoiceActOnABoardMadeReadOnlyDuringTranscodeIsRefused(t *testing.T) {
	gate := t.TempDir()
	fakeFFmpeg(t, gate)
	c, host, cookie := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	responses := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		responses <- postVoice(c, cookie, "plan", []byte("audio"), map[string]string{"X-Client-Id": "voice-1", "X-Board-Part": "1"})
	}()
	waitFor(t, func() bool { _, err := os.Stat(filepath.Join(gate, "started")); return err == nil })
	if err := c.board.withBoard(plan.Number, func(board *boardState) error { board.record.Closed = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gate, "release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if response := <-responses; response.Code != http.StatusConflict {
		t.Fatalf("status=%d %s, want 409", response.Code, response.Body)
	}
	if len(host.emittedSnapshot()) != 0 || len(boardLog(t, c, "plan")) != 0 {
		t.Fatal("a voice act for a read-only board reached the session or the log")
	}
	directory, err := c.voiceDirectoryPath()
	if err != nil {
		t.Fatal(err)
	}
	if files, _ := filepath.Glob(filepath.Join(directory, "*")); len(files) != 0 {
		t.Fatalf("the refused recording was kept: %v", files)
	}
}

// fakeTimers is the board channel's clock in a test: a timer fires only when
// the test moves time past it.
type fakeTimers struct {
	mu      sync.Mutex
	elapsed time.Duration
	timers  []*fakeTimer
}

type fakeTimer struct {
	at        time.Duration
	fire      func()
	isStopped bool
}

func (clock *fakeTimers) after(wait time.Duration, fire func()) func() bool {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &fakeTimer{at: clock.elapsed + wait, fire: fire}
	clock.timers = append(clock.timers, timer)
	return func() bool {
		clock.mu.Lock()
		defer clock.mu.Unlock()
		wasArmed := !timer.isStopped
		timer.isStopped = true
		return wasArmed
	}
}

// advance moves time on by wait and fires each timer that comes due.
func (clock *fakeTimers) advance(wait time.Duration) {
	clock.mu.Lock()
	clock.elapsed += wait
	var due []*fakeTimer
	for _, timer := range clock.timers {
		if !timer.isStopped && timer.at <= clock.elapsed {
			timer.isStopped = true
			due = append(due, timer)
		}
	}
	clock.mu.Unlock()
	for _, timer := range due {
		timer.fire()
	}
}

func (clock *fakeTimers) armed() int {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	count := 0
	for _, timer := range clock.timers {
		if !timer.isStopped {
			count++
		}
	}
	return count
}

// recordingTest is a started board channel on a fake clock with board plan,
// posting parts of recordings about its part 1.5.
type recordingTest struct {
	t      *testing.T
	c      *Channel
	host   *fakeBoardHost
	clock  *fakeTimers
	cookie *http.Cookie
	sent   int
}

func newRecordingTest(t *testing.T) *recordingTest {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	test := &recordingTest{t: t, host: newFakeBoardHost(), clock: &fakeTimers{}}
	test.c = startBoardChannel(t, test.host, func(c *Channel) { c.board.after = test.clock.after })
	createBoard(t, test.c, "plan")
	test.cookie = loginCookie(t, test.c)
	return test
}

// send posts part segment of recording ("" sends no recording id) as the line
// saying its audio was refused, which needs no transcode and counts as a part.
func (test *recordingTest) send(recording string, segment int, hasMore bool, unsent string) {
	test.t.Helper()
	test.sent++
	headers := map[string]string{"X-Client-Id": fmt.Sprintf("voice-%d", test.sent), "X-Board-Part": "1.5",
		"X-Voice-Segment": strconv.Itoa(segment), "X-Voice-Recording": recording, "X-Voice-Unsent": unsent}
	if hasMore {
		headers["X-Voice-More"] = "1"
	}
	if response := postVoice(test.c, test.cookie, "plan", nil, headers); response.Code != http.StatusAccepted {
		test.t.Fatalf("part %d of %q: status=%d %s", segment, recording, response.Code, response.Body)
	}
}

// A recording whose parts stop coming (its tab died, or C3 refused a part and
// the line saying so) is ended by C3 once, with its own line on the
// recording's part, logged for the page.
func TestARecordingWhosePartsStopIsEndedOnce(t *testing.T) {
	test := newRecordingTest(t)
	test.send("a", 1, true, "refused")
	test.clock.advance(boardVoiceWait)
	emitted := test.host.emittedSnapshot()
	if len(emitted) != 2 {
		t.Fatalf("emitted %d lines, want the part and C3's end", len(emitted))
	}
	ended := emitted[1]
	if want := "[board plan · 1.5 · msg " + itoa(ended.MessageID) + "] Voice note ended after part 1: no further part arrived."; ended.Text != want ||
		ended.Channel != channel.BoardChannelName || ended.ChatID != 1 || ended.Sender.UserID != 42 {
		t.Fatalf("ended line=%+v, want %q from the operator on the board's route", ended, want)
	}
	log := boardLog(t, test.c, "plan")
	if entry := log[len(log)-1]; entry.Op != "voice_ended" || entry.ID != ended.MessageID || entry.Part != "1.5" || entry.Segment != 1 || entry.From != "C3" {
		t.Fatalf("logged %+v, want C3's voice_ended line", entry)
	}
	test.clock.advance(3 * boardVoiceWait)
	if len(test.host.emittedSnapshot()) != 2 || test.clock.armed() != 0 {
		t.Fatalf("emitted %d lines with %d timers armed, want it ended once", len(test.host.emittedSnapshot()), test.clock.armed())
	}
}

// Each new part of a recording restarts C3's wait for the next one.
func TestANewPartRestartsTheWait(t *testing.T) {
	test := newRecordingTest(t)
	test.send("", 1, true, "refused")
	test.clock.advance(boardVoiceWait - time.Second)
	test.send("", 2, true, "refused")
	test.clock.advance(boardVoiceWait - time.Second)
	if emitted := test.host.emittedSnapshot(); len(emitted) != 2 {
		t.Fatalf("emitted %d lines, want no end before a whole wait after part 2", len(emitted))
	}
	test.clock.advance(time.Second)
	if emitted := test.host.emittedSnapshot(); len(emitted) != 3 || !strings.HasSuffix(emitted[2].Text, "] Voice note ended after part 2: no further part arrived.") {
		t.Fatalf("emitted %d lines, want the end a whole wait after part 2", len(emitted))
	}
}

// A recording that reaches its last part, or a line saying it was cancelled,
// is never ended by C3.
func TestACompleteRecordingIsNeverEnded(t *testing.T) {
	test := newRecordingTest(t)
	test.send("a", 1, true, "refused")
	test.send("a", 2, false, "refused")
	test.send("", 1, true, "refused")
	test.send("", 2, false, "cancelled")
	test.clock.advance(3 * boardVoiceWait)
	if emitted := test.host.emittedSnapshot(); len(emitted) != 4 || test.clock.armed() != 0 {
		t.Fatalf("emitted %d lines with %d timers armed, want the four parts only", len(emitted), test.clock.armed())
	}
}

// On a board made read-only the end goes to the log only: the board's route
// is gone, and the session was told the board closed.
func TestARecordingOnABoardMadeReadOnlyEndsInTheLogOnly(t *testing.T) {
	test := newRecordingTest(t)
	test.send("", 1, true, "refused")
	w := httptest.NewRecorder()
	test.c.routes().ServeHTTP(w, request(http.MethodPost, "/board/plan/close", "", test.cookie, true))
	if w.Code != http.StatusNoContent {
		t.Fatalf("close=%d %s", w.Code, w.Body)
	}
	test.clock.advance(boardVoiceWait)
	log := boardLog(t, test.c, "plan")
	if len(test.host.emittedSnapshot()) != 1 || log[len(log)-1].Op != "voice_ended" {
		t.Fatalf("emitted %d lines, log %+v; want the end logged and not sent", len(test.host.emittedSnapshot()), log)
	}
}
