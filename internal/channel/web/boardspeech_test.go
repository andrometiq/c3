package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// fakeSpeech makes host's synthesizer return "ID3-<text>" as MP3.
func fakeSpeech(host *fakeBoardHost) {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.synthesize = func(_ context.Context, request c3types.SpeechRequest) (c3types.SpeechResult, error) {
		return c3types.SpeechResult{Audio: []byte("ID3-" + request.Text), MIME: "audio/mpeg", Provider: "fake"}, nil
	}
}

func postSpeech(c *Channel, cookie *http.Cookie, name, body string) *httptest.ResponseRecorder {
	return serve(c, request(http.MethodPost, "/board/"+name+"/speak", body, cookie, true))
}

func getBoardAudio(c *Channel, cookie *http.Cookie, path string) *httptest.ResponseRecorder {
	return serve(c, request(http.MethodGet, path, "", cookie, false))
}

// speechToken is the token a 200 from /board/<name>/speak carries.
func speechToken(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct{ Token string }
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil || len(body.Token) != 32 {
		t.Fatalf("speak=%d %s, want 200 with a token", response.Code, response.Body.String())
	}
	return body.Token
}

func expectBoardError(t *testing.T, response *httptest.ResponseRecorder, status int, rule string) {
	t.Helper()
	if response.Code != status || response.Body.String() != `{"error":"`+rule+`"}`+"\n" {
		t.Fatalf("status=%d body=%q, want %d %s", response.Code, response.Body.String(), status, rule)
	}
}

func agentMessage(t *testing.T, c *Channel, number int64, text string) int64 {
	t.Helper()
	id, err := c.board.SendReply(c3types.ReplyArgs{Channel: channel.BoardChannelName, ChatID: number, Text: text})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// Only the session's own messages on that board are read aloud, as last
// edited: never the owner's acts, a page, an unknown id or another board's
// message. The audio is served under that board only.
func TestBoardReadsAloudOnlyTheSessionsMessagesOnThatBoard(t *testing.T) {
	c, host, cookie := newBoardTest(t)
	fakeSpeech(host)
	plan := createBoard(t, c, "plan")
	createBoard(t, c, "other")
	answer := agentMessage(t, c, plan.Number, "first draft")
	if _, err := c.board.EditMessage(c3types.EditArgs{Channel: channel.BoardChannelName, ChatID: plan.Number, MessageID: answer, Text: "edited draft"}); err != nil {
		t.Fatal(err)
	}
	page := sendPage(t, c, plan.Number, "<!doctype html><title>p</title>", 0)
	var own struct{ ID int64 }
	_ = json.Unmarshal(postAct(c, cookie, "plan", `{"client_id":"own","op":"comment","part":"1","text":"mine"}`).Body.Bytes(), &own)

	token := speechToken(t, postSpeech(c, cookie, "plan", `{"id":`+itoa(answer)+`}`))
	audio := getBoardAudio(c, cookie, "/board/plan/audio/"+token)
	if audio.Code != http.StatusOK || audio.Header().Get("Content-Type") != "audio/mpeg" || audio.Body.String() != "ID3-edited draft" {
		t.Fatalf("board audio=%d %q %q, want the edited text's MP3", audio.Code, audio.Header().Get("Content-Type"), audio.Body.String())
	}
	if again := speechToken(t, postSpeech(c, cookie, "plan", `{"id":`+itoa(answer)+`}`)); again != token || host.synthCallCount() != 1 {
		t.Fatalf("a second ask synthesized again: token %s vs %s, %d calls", again, token, host.synthCallCount())
	}
	for _, path := range []string{"/board/other/audio/" + token, "/audio/" + token, "/board/plan/audio/" + strings.Repeat("0", 32)} {
		if response := getBoardAudio(c, cookie, path); response.Code != http.StatusNotFound {
			t.Fatalf("GET %s=%d, want 404: a board's audio is served under that board only", path, response.Code)
		}
	}
	for name, id := range map[string]int64{"the owner's own comment": own.ID, "a page": page, "an unknown id": 999} {
		t.Run(name, func(t *testing.T) {
			expectBoardError(t, postSpeech(c, cookie, "plan", `{"id":`+itoa(id)+`}`), http.StatusNotFound, "not_found")
		})
	}
	expectBoardError(t, postSpeech(c, cookie, "other", `{"id":`+itoa(answer)+`}`), http.StatusNotFound, "not_found")
	for _, body := range []string{`{"id":0}`, `{"id":-1}`, `{"id":"1"}`, `{"id":1,"text":"say this"}`, `{"id":1} {}`, `{}`} {
		expectBoardError(t, postSpeech(c, cookie, "plan", body), http.StatusBadRequest, "invalid")
	}
	if calls := host.synthCallCount(); calls != 1 {
		t.Fatalf("refused asks reached the synthesizer: %d calls", calls)
	}
}

// Speaking is a board route like the others: signed in, same origin, a board
// that exists. A board made read-only can still be heard.
func TestBoardSpeechTrustBoundary(t *testing.T) {
	c, host, cookie := newBoardTest(t)
	fakeSpeech(host)
	plan := createBoard(t, c, "plan")
	answer := agentMessage(t, c, plan.Number, "draft")
	body := `{"id":` + itoa(answer) + `}`
	expectBoardError(t, postSpeech(c, nil, "plan", body), http.StatusUnauthorized, "unauthorized")
	for _, origin := range []string{"https://evil.example", "null", ""} {
		r := request(http.MethodPost, "/board/plan/speak", body, cookie, false)
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		expectBoardError(t, serve(c, r), http.StatusForbidden, "forbidden")
	}
	expectBoardError(t, postSpeech(c, cookie, "ghost", body), http.StatusNotFound, "not_found")
	expectBoardError(t, postSpeech(c, cookie, "Bad_Name", body), http.StatusNotFound, "not_found")
	token := speechToken(t, postSpeech(c, cookie, "plan", body))
	if response := getBoardAudio(c, nil, "/board/plan/audio/"+token); response.Code != http.StatusUnauthorized {
		t.Fatalf("audio without a login=%d, want 401", response.Code)
	}
	if response := getBoardAudio(c, nil, "/board/plan/audio/unlock"); response.Code != http.StatusUnauthorized {
		t.Fatalf("unlock clip without a login=%d, want 401", response.Code)
	}
	if response := getBoardAudio(c, cookie, "/board/plan/audio/unlock"); response.Code != http.StatusOK || response.Header().Get("Content-Type") != "audio/mpeg" {
		t.Fatalf("unlock clip=%d %q, want the MP3", response.Code, response.Header().Get("Content-Type"))
	}
	if close := serve(c, request(http.MethodPost, "/board/plan/close", "", cookie, true)); close.Code != http.StatusNoContent {
		t.Fatalf("close=%d", close.Code)
	}
	speechToken(t, postSpeech(c, cookie, "plan", body))
}

// A reply C3 must synthesize counts against the board's rate; one already in
// the audio cache is free, and the count clears after a minute.
func TestBoardSpeechRateCountsOnlyNewAudio(t *testing.T) {
	c, host, cookie := newBoardTest(t)
	fakeSpeech(host)
	plan := createBoard(t, c, "plan")
	var ids []int64
	for range boardSpokenPerMinute + 1 {
		ids = append(ids, agentMessage(t, c, plan.Number, "reply "+itoa(int64(len(ids)))))
	}
	for _, id := range ids[:boardSpokenPerMinute] {
		speechToken(t, postSpeech(c, cookie, "plan", `{"id":`+itoa(id)+`}`))
	}
	last := `{"id":` + itoa(ids[boardSpokenPerMinute]) + `}`
	limited := postSpeech(c, cookie, "plan", last)
	expectBoardError(t, limited, http.StatusTooManyRequests, "rate")
	if limited.Header().Get("Retry-After") == "" {
		t.Fatal("429 without Retry-After")
	}
	speechToken(t, postSpeech(c, cookie, "plan", `{"id":`+itoa(ids[0])+`}`))
	if calls := host.synthCallCount(); calls != boardSpokenPerMinute {
		t.Fatalf("synthesized %d, want %d", calls, boardSpokenPerMinute)
	}
	start := time.Now()
	c.now = func() time.Time { return start.Add(time.Minute) }
	speechToken(t, postSpeech(c, cookie, "plan", last))
}

// A board's message ids and the web chat's overlap, so their audio is kept
// apart: the same id on each is synthesized for each and served only on its
// own route.
func TestBoardAndWebChatAudioNeverMix(t *testing.T) {
	c, host, cookie := newBoardTest(t)
	fakeSpeech(host)
	plan := createBoard(t, c, "plan")
	chatID, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "web chat reply"})
	if err != nil {
		t.Fatal(err)
	}
	var boardID int64
	for boardID < chatID {
		boardID = agentMessage(t, c, plan.Number, "board reply")
	}
	if boardID != chatID {
		t.Fatalf("board id %d, web chat id %d: want the same id", boardID, chatID)
	}
	chatToken, err := c.cacheSpeech(0, chatID, []byte("ID3-web chat reply"))
	if err != nil {
		t.Fatal(err)
	}
	boardToken := speechToken(t, postSpeech(c, cookie, "plan", `{"id":`+itoa(boardID)+`}`))
	if boardToken == chatToken || host.synthCallCount() != 1 {
		t.Fatalf("the board got the web chat's audio for id %d (%d synth calls)", boardID, host.synthCallCount())
	}
	if audio := getBoardAudio(c, cookie, "/board/plan/audio/"+boardToken); audio.Body.String() != "ID3-board reply" {
		t.Fatalf("board audio %q", audio.Body.String())
	}
	for _, path := range []string{"/audio/" + boardToken, "/board/plan/audio/" + chatToken} {
		if response := getBoardAudio(c, cookie, path); response.Code != http.StatusNotFound {
			t.Fatalf("GET %s=%d, want 404", path, response.Code)
		}
	}
	if token, ok := c.audioTokenForMessage(0, chatID); !ok || token != chatToken {
		t.Fatalf("the web chat's cached audio for %d is gone or replaced: %q %v", chatID, token, ok)
	}
}

// With a local from the front reaches /board/* only, so a board's audio and
// the unlock clip live under the board's own path, for the front's operator.
func TestFrontHearsABoardUnderItsOwnPath(t *testing.T) {
	c, host, _ := newFrontTest(t)
	useLocalFrom(c)
	fakeSpeech(host)
	plan, _ := c.board.BoardByName("plan")
	body := `{"id":` + itoa(agentMessage(t, c, plan.Number, "draft")) + `}`
	speak := serve(c, frontRequest(http.MethodPost, "/board/plan/speak", body, "127.0.0.1", frontOperator))
	token := speechToken(t, speak)
	for _, path := range []string{"/board/plan/audio/" + token, "/board/plan/audio/unlock"} {
		if response := serve(c, frontRequest(http.MethodGet, path, "", "127.0.0.1", frontOperator)); response.Code != http.StatusOK || response.Header().Get("Content-Type") != "audio/mpeg" {
			t.Fatalf("front operator GET %s=%d, want the MP3", path, response.Code)
		}
	}
	if response := serve(c, frontRequest(http.MethodGet, "/audio/"+token, "", "127.0.0.1", frontOperator)); response.Code != http.StatusNotFound {
		t.Fatalf("front GET /audio/<token>=%d, want 404: a boards-only front never reaches the web chat's routes", response.Code)
	}
	for _, refused := range []*http.Request{
		frontRequest(http.MethodPost, "/board/plan/speak", body, "127.0.0.1", "guest@example.test"),
		frontRequest(http.MethodGet, "/board/plan/audio/"+token, "", "127.0.0.1", "guest@example.test"),
		frontRequest(http.MethodGet, "/board/plan/audio/unlock", "", "127.0.0.1"),
	} {
		if response := serve(c, refused); response.Code != http.StatusForbidden {
			t.Fatalf("front %s %s for another identity=%d, want 403", refused.Method, refused.URL.Path, response.Code)
		}
	}
}
