package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
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
// act with its live status, not as the session's messages; the same notice on
// anything but a voice act stays an ordinary message.
func TestVoiceStatusArrivesAsAnEditOfTheAct(t *testing.T) {
	c, _, cookie := newBoardTest(t)
	plan := createBoard(t, c, "plan")
	stream, stop := startBoardSSE(t, c, cookie, "plan", "")
	defer stop()
	voice := logVoiceAct(t, c, "plan", "1.5")
	if _, err := c.board.SendReadback(c3types.ReadbackArgs{ChatID: plan.Number, ReplyTo: &voice, Transcript: "make it shorter"}); err != nil {
		t.Fatal(err)
	}
	for _, notice := range []string{c3types.VoiceSlowNotice, c3types.VoiceNoSpeechNotice, c3types.VoiceTranscriptionFailureNoticePrefix + " — see logs / try again."} {
		if id, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, ReplyTo: &voice, Text: notice}); err != nil || id != voice {
			t.Fatalf("notice %q: id=%d err=%v", notice, id, err)
		}
	}
	answer, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, ReplyTo: &voice, Text: "Shorter it is."})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, ReplyTo: &answer, Text: c3types.VoiceNoSpeechNotice}); err != nil {
		t.Fatal(err)
	}
	log := boardLog(t, c, "plan")
	got := make([]string, 0, len(log))
	for _, entry := range log[1:] {
		got = append(got, entry.Dir+" "+entry.Op+" "+entry.Status+" "+entry.Part)
	}
	want := []string{"in edit heard 1.5", "in edit slow 1.5", "in edit no_speech 1.5", "in edit failed 1.5", "out message  1.5", "out message  1.5"}
	if strings.Join(got, "|") != strings.Join(want, "|") || log[1].ID != voice || log[1].Text != "make it shorter" {
		t.Fatalf("log=%q (first edit %+v), want %q", got, log[1], want)
	}
	events := func() string { _, body := stream.snapshot(); return body }
	waitFor(t, func() bool { return strings.Count(events(), "event: edit\n") == 4 })
	if strings.Count(events(), "event: action\n") != 1 {
		t.Fatalf("stream=%s, want the voice act as the only action", events())
	}
	if _, err := c.board.SendReadback(c3types.ReadbackArgs{ChatID: plan.Number, ReplyTo: &answer, Transcript: "forged"}); err == nil {
		t.Fatal("a readback edited the session's own message")
	}
}
