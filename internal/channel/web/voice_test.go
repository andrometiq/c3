package web

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func voiceRequest(c *Channel, cookie *http.Cookie, body []byte, contentType, clientID string, withOrigin bool) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/voice-note", bytes.NewReader(body))
	if cookie != nil {
		request.AddCookie(cookie)
	}
	if withOrigin {
		request.Header.Set("Origin", "http://127.0.0.1:8371")
	}
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("X-Client-Id", clientID)
	response := httptest.NewRecorder()
	c.routes().ServeHTTP(response, request)
	return response
}

func voiceTestChannel(t *testing.T) (*Channel, *fakeHost, *http.Cookie, string) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	c, host, cookie := newHandlerChannel()
	directory, err := stateDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	c.stateDirectory = directory
	return c, host, cookie, directory
}

func TestVoiceNoteTrustBoundaryRejections(t *testing.T) {
	c, host, cookie, _ := voiceTestChannel(t)
	tests := []struct {
		name        string
		cookie      *http.Cookie
		origin      bool
		contentType string
		body        []byte
		want        int
	}{
		{name: "missing origin", cookie: cookie, contentType: "audio/wav", body: []byte("x"), want: http.StatusForbidden},
		{name: "missing cookie", origin: true, contentType: "audio/wav", body: []byte("x"), want: http.StatusUnauthorized},
		{name: "non audio", cookie: cookie, origin: true, contentType: "application/octet-stream", body: []byte("x"), want: http.StatusUnsupportedMediaType},
		{name: "oversize", cookie: cookie, origin: true, contentType: "audio/wav", body: make([]byte, voiceBodyLimit+1), want: http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := voiceRequest(c, test.cookie, test.body, test.contentType, "reject-"+test.name, test.origin)
			if response.Code != test.want {
				t.Fatalf("status=%d body=%q, want %d", response.Code, response.Body.String(), test.want)
			}
		})
	}
	if len(host.emittedSnapshot()) != 0 {
		t.Fatal("rejected voice request emitted inbound")
	}
}

func TestVoiceNoteSharesClientIDConflictStoreWithText(t *testing.T) {
	c, host, cookie, directory := voiceTestChannel(t)
	text := sendRequest(c, cookie, `{"text":"text first","client_id":"shared-id"}`)
	if text.Code != http.StatusAccepted {
		t.Fatalf("text status=%d", text.Code)
	}
	voice := voiceRequest(c, cookie, []byte("not decoded because id conflicts"), "audio/wav", "shared-id", true)
	if voice.Code != http.StatusConflict {
		t.Fatalf("voice conflict status/body=%d/%q", voice.Code, voice.Body.String())
	}
	if len(host.emittedSnapshot()) != 1 {
		t.Fatalf("conflicting voice emitted; total=%d", len(host.emittedSnapshot()))
	}
	if _, err := os.Stat(filepath.Join(directory, "voice")); !os.IsNotExist(err) {
		t.Fatalf("conflicting voice created storage: %v", err)
	}
}

func TestVoiceNoteAcceptsWAVIsIdempotentAndPrunes(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available; real voice-note transcode test skipped")
	}
	c, host, cookie, _ := voiceTestChannel(t)
	c.voiceRetention = 2
	voiceDirectory, err := c.ensureVoiceDirectory()
	if err != nil {
		t.Fatal(err)
	}
	for index, id := range []string{
		"00000000000000000000000000000001",
		"00000000000000000000000000000002",
	} {
		path := filepath.Join(voiceDirectory, id+".oga")
		if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
		modified := time.Now().Add(time.Duration(index-3) * time.Hour)
		if err := os.Chtimes(path, modified, modified); err != nil {
			t.Fatal(err)
		}
	}

	wav := sineWAV(t, time.Second)
	first := voiceRequest(c, cookie, wav, "audio/wav", "wav-one", true)
	if first.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%q", first.Code, first.Body.String())
	}
	var firstResult struct {
		MessageID int64 `json:"message_id"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstResult); err != nil {
		t.Fatal(err)
	}
	rows := host.emittedSnapshot()
	if len(rows) != 1 || len(rows[0].Attachments) != 1 {
		t.Fatalf("emitted=%+v", rows)
	}
	attachment := rows[0].Attachments[0]
	if rows[0].Text != "" || attachment.Kind != "voice" || attachment.MIME != "audio/ogg" || attachment.Size <= 0 {
		t.Fatalf("voice inbound=%+v attachment=%+v", rows[0], attachment)
	}
	path, err := c.LocalAudioPath(attachment.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Ext(path) != ".oga" {
		t.Fatalf("local path=%q", path)
	}
	if mode := fileMode(t, path); mode.Perm() != 0o600 {
		t.Fatalf("voice mode=%o", mode.Perm())
	}
	if mode := fileMode(t, voiceDirectory); mode.Perm() != 0o700 {
		t.Fatalf("voice directory mode=%o", mode.Perm())
	}
	entries, err := os.ReadDir(voiceDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("retention left %d files, want 2", len(entries))
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".oga" {
			t.Fatalf("original upload survived transcode: %s", entry.Name())
		}
	}
	if len(c.replay) != 1 || c.replay[0].kind != "own" || c.replay[0].payload.Voice == nil || !*c.replay[0].payload.Voice || c.replay[0].payload.DurationSeconds <= 0 {
		t.Fatalf("own event=%+v", c.replay)
	}

	retry := voiceRequest(c, cookie, wav, "audio/wav", "wav-one", true)
	var retryResult struct {
		MessageID int64 `json:"message_id"`
	}
	if retry.Code != http.StatusAccepted || json.Unmarshal(retry.Body.Bytes(), &retryResult) != nil || retryResult.MessageID != firstResult.MessageID {
		t.Fatalf("retry status/body=%d/%q", retry.Code, retry.Body.String())
	}
	afterRetry, _ := os.ReadDir(voiceDirectory)
	if len(afterRetry) != len(entries) || len(host.emittedSnapshot()) != 1 || len(c.replay) != 1 {
		t.Fatalf("retry created work: files/emits/replay=%d/%d/%d", len(afterRetry), len(host.emittedSnapshot()), len(c.replay))
	}
}

func TestLocalAudioPathRejectsUnknownAndPathLikeIDs(t *testing.T) {
	c, _, _, _ := voiceTestChannel(t)
	for _, fileID := range []string{
		"00000000000000000000000000000000", "../x", "/tmp/x", "ABCDEF00000000000000000000000000",
	} {
		if path, err := c.LocalAudioPath(fileID); err == nil || path != "" {
			t.Fatalf("LocalAudioPath(%q)=%q/%v, want closed refusal", fileID, path, err)
		}
	}
}

func TestLocalAudioPathRejectsSymlink(t *testing.T) {
	c, _, _, _ := voiceTestChannel(t)
	directory, err := c.ensureVoiceDirectory()
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "outside.oga")
	if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	fileID := "0123456789abcdef0123456789abcdef"
	if err := os.Symlink(target, filepath.Join(directory, fileID+".oga")); err != nil {
		t.Fatal(err)
	}
	if path, err := c.LocalAudioPath(fileID); err == nil || path != "" {
		t.Fatalf("LocalAudioPath(symlink)=%q/%v, want closed refusal", path, err)
	}
}

func TestSendReadbackPersistsOwnVoiceEditForReplay(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	first, host, _ := newHandlerChannel()
	if err := first.loadState(host); err != nil {
		t.Fatal(err)
	}
	cookieValue, err := first.createSession(loginToken{userID: 42, username: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	cookie := &http.Cookie{Name: sessionCookieName, Value: cookieValue}
	sourceID := int64(17)
	if _, err := first.SendReadback(c3types.ReadbackArgs{
		ChatID: 42, ReplyTo: &sourceID, Transcript: "the transcript",
	}); err != nil {
		t.Fatal(err)
	}
	if len(first.replay) != 1 || first.replay[0].kind != "edit" || first.replay[0].payload.Voice == nil || !*first.replay[0].payload.Voice {
		t.Fatalf("readback replay=%+v", first.replay)
	}

	second := New()
	second.host = host
	second.operatorID = 42
	second.allowedOrigin = first.allowedOrigin
	if err := second.loadState(host); err != nil {
		t.Fatal(err)
	}
	recorder, cancel, done := startSSE(second, cookie, "", "")
	waitFor(t, func() bool {
		_, body := recorder.snapshot()
		return strings.Contains(body, "event: edit") && strings.Contains(body, "the transcript") && strings.Contains(body, "\"voice\":true")
	})
	cancel()
	<-done
}

// C3's notices about a voice note (no speech heard, couldn't transcribe, still
// transcribing) land on the note's own row, in C3's wording, and survive a
// restart; the transcript then takes the row back as heard.
func TestVoiceStatusEditsTheNoteAndPersists(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	first, host, _ := newHandlerChannel()
	if err := first.loadState(host); err != nil {
		t.Fatal(err)
	}
	sourceID := int64(17)
	for _, status := range []c3types.VoiceStatusArgs{
		{ChatID: 42, MessageID: sourceID, Status: c3types.VoiceStatusSlow, Text: "anything"},
		{ChatID: 42, MessageID: sourceID, Status: c3types.VoiceStatusNoSpeech, Text: "anything"},
		{ChatID: 42, MessageID: sourceID, Status: c3types.VoiceStatusFailed, Text: c3types.VoiceTranscriptionFailureNoticePrefix + " — see logs / try again."},
	} {
		if err := first.SendVoiceStatus(status); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := first.SendReadback(c3types.ReadbackArgs{ChatID: 42, ReplyTo: &sourceID, Transcript: "heard at last"}); err != nil {
		t.Fatal(err)
	}
	want := []struct{ status, text string }{
		{c3types.VoiceStatusSlow, c3types.VoiceSlowNotice},
		{c3types.VoiceStatusNoSpeech, c3types.VoiceNoSpeechNotice},
		{c3types.VoiceStatusFailed, c3types.VoiceTranscriptionFailureNoticePrefix + " — see logs / try again."},
		{c3types.VoiceStatusHeard, "heard at last"},
	}
	second := New()
	second.host = host
	second.operatorID = 42
	if err := second.loadState(host); err != nil {
		t.Fatal(err)
	}
	for _, replay := range [][]streamEvent{first.replay, second.replay} {
		if len(replay) != len(want) {
			t.Fatalf("replay=%+v, want %d voice edits", replay, len(want))
		}
		for index, event := range replay {
			payload := event.payload
			if event.kind != "edit" || payload.MessageID != sourceID || payload.Voice == nil || !*payload.Voice ||
				payload.VoiceStatus != want[index].status || payload.Text != want[index].text {
				t.Fatalf("event %d=%s %+v, want an edit of %d with %+v", index, event.kind, payload, sourceID, want[index])
			}
		}
	}
	for _, refused := range []c3types.VoiceStatusArgs{
		{ChatID: 42, MessageID: sourceID, Status: "bogus", Text: "x"},
		{ChatID: 42, MessageID: 0, Status: c3types.VoiceStatusNoSpeech},
		{ChatID: 7, MessageID: sourceID, Status: c3types.VoiceStatusNoSpeech},
	} {
		if err := first.SendVoiceStatus(refused); err == nil {
			t.Fatalf("SendVoiceStatus(%+v) was taken", refused)
		}
	}
}

// A session's reply never edits a voice note, even one quoting it in C3's own
// failure wording: only the broker's SendVoiceStatus does.
func TestSessionReplyNeverEditsAVoiceNote(t *testing.T) {
	c, _, _ := newHandlerChannel()
	sourceID := int64(17)
	for _, text := range []string{
		c3types.VoiceTranscriptionFailureNoticePrefix + " — see logs / try again.",
		c3types.VoiceDownloadFailureNoticePrefix + ", so it wasn't transcribed",
		c3types.VoiceNoSpeechNotice,
	} {
		messageID, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, ReplyTo: &sourceID, Text: text})
		if err != nil {
			t.Fatal(err)
		}
		if messageID == sourceID {
			t.Fatalf("reply %q took the voice note's id", text)
		}
	}
	for _, event := range c.replay {
		if event.kind == "edit" || event.payload.Voice != nil {
			t.Fatalf("a session reply edited a voice note: %+v", c.replay)
		}
	}
}

func voicePartRequest(c *Channel, cookie *http.Cookie, body []byte, clientID string, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/voice-note", bytes.NewReader(body))
	request.AddCookie(cookie)
	request.Header.Set("Origin", "http://127.0.0.1:8371")
	request.Header.Set("Content-Type", "audio/wav")
	request.Header.Set("X-Client-Id", clientID)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response := httptest.NewRecorder()
	c.routes().ServeHTTP(response, request)
	return response
}

// A part's place in a recording is checked as a board voice act's is: a
// canonical number, more only with a number, and a not-sent part only with a
// number, cancelled only as the last part, and with no audio.
func TestVoicePartHeadersAreCheckedLikeABoardAct(t *testing.T) {
	c, host, cookie, _ := voiceTestChannel(t)
	for name, headers := range map[string]map[string]string{
		"zero":               {"X-Voice-Segment": "0"},
		"leading zero":       {"X-Voice-Segment": "01"},
		"too many":           {"X-Voice-Segment": "1000"},
		"more alone":         {"X-Voice-More": "1"},
		"more not one":       {"X-Voice-Segment": "1", "X-Voice-More": "true"},
		"unsent alone":       {"X-Voice-Unsent": "cancelled"},
		"cancelled not last": {"X-Voice-Segment": "2", "X-Voice-More": "1", "X-Voice-Unsent": "cancelled"},
		"unknown unsent":     {"X-Voice-Segment": "2", "X-Voice-Unsent": "lost"},
	} {
		if response := voicePartRequest(c, cookie, nil, "bad-"+name, headers); response.Code != http.StatusBadRequest {
			t.Errorf("%s: status=%d body=%q, want 400", name, response.Code, response.Body.String())
		}
	}
	withAudio := voicePartRequest(c, cookie, []byte("audio"), "unsent-with-audio", map[string]string{"X-Voice-Segment": "2", "X-Voice-Unsent": "refused"})
	if withAudio.Code != http.StatusBadRequest {
		t.Errorf("a not-sent part with audio: status=%d, want 400", withAudio.Code)
	}
	if emitted := host.emittedSnapshot(); len(emitted) != 0 {
		t.Fatalf("a refused part reached the session: %+v", emitted)
	}
}

// A recording longer than a part reaches the session as numbered parts, each
// under its line, and ends with a last part or the line that it was not sent;
// the page gets each part's place for its row. A whole note keeps no line.
func TestLongWebChatRecordingReachesTheSessionAsNumberedParts(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available; real voice-note transcode test skipped")
	}
	c, host, cookie, _ := voiceTestChannel(t)
	wav := sineWAV(t, time.Second)
	requests := []struct {
		clientID string
		body     []byte
		headers  map[string]string
		line     string
		hasAudio bool
	}{
		{"whole", wav, nil, "", true},
		{"part-1", wav, map[string]string{"X-Voice-Segment": "1", "X-Voice-More": "1"}, "Voice note (part 1; more follows):", true},
		{"part-2", wav, map[string]string{"X-Voice-Segment": "2", "X-Voice-More": "1"}, "Voice note (part 2; more follows):", true},
		{"part-3", nil, map[string]string{"X-Voice-Segment": "3", "X-Voice-Unsent": "cancelled"},
			"Voice note (part 3, the last): the recording was cancelled; this part was not sent.", false},
		{"again-1", wav, map[string]string{"X-Voice-Segment": "1", "X-Voice-More": "1"}, "Voice note (part 1; more follows):", true},
		{"again-2", nil, map[string]string{"X-Voice-Segment": "2", "X-Voice-More": "1", "X-Voice-Unsent": "refused"},
			"Voice note (part 2; more follows): C3 couldn't take this part; it was not sent.", false},
		{"again-3", wav, map[string]string{"X-Voice-Segment": "3"}, "Voice note (part 3, the last):", true},
	}
	for _, request := range requests {
		if response := voicePartRequest(c, cookie, request.body, request.clientID, request.headers); response.Code != http.StatusAccepted {
			t.Fatalf("%s: status=%d body=%q", request.clientID, response.Code, response.Body.String())
		}
	}
	emitted := host.emittedSnapshot()
	if len(emitted) != len(requests) || len(c.replay) != len(requests) {
		t.Fatalf("emitted=%d replay=%d, want %d each", len(emitted), len(c.replay), len(requests))
	}
	for index, request := range requests {
		inbound, own := emitted[index], c.replay[index].payload
		hasAudio := len(inbound.Attachments) == 1 && inbound.Attachments[0].Kind == "voice"
		if inbound.Text != request.line || hasAudio != request.hasAudio {
			t.Errorf("%s: session read %q (audio %t), want %q (audio %t)", request.clientID, inbound.Text, hasAudio, request.line, request.hasAudio)
		}
		segment, _ := strconv.Atoi(request.headers["X-Voice-Segment"])
		if c.replay[index].kind != "own" || own.Voice == nil || !*own.Voice || own.ClientID != request.clientID || own.Segment != segment ||
			own.HasMore != (request.headers["X-Voice-More"] == "1") || own.Unsent != request.headers["X-Voice-Unsent"] || own.Text != "" {
			t.Errorf("%s: own row=%+v", request.clientID, own)
		}
	}
}

// A retry of a part's client id must repeat its place in the recording, not
// only its audio.
func TestVoicePartRetryMustRepeatItsPlace(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available; real voice-note transcode test skipped")
	}
	c, host, cookie, _ := voiceTestChannel(t)
	wav := sineWAV(t, time.Second)
	first := map[string]string{"X-Voice-Segment": "1", "X-Voice-More": "1"}
	if response := voicePartRequest(c, cookie, wav, "part", first); response.Code != http.StatusAccepted {
		t.Fatalf("first: status=%d body=%q", response.Code, response.Body.String())
	}
	if response := voicePartRequest(c, cookie, wav, "part", first); response.Code != http.StatusAccepted {
		t.Fatalf("same retry: status=%d body=%q", response.Code, response.Body.String())
	}
	for name, moved := range map[string]map[string]string{
		"whole note": nil,
		"other part": {"X-Voice-Segment": "2", "X-Voice-More": "1"},
		"now last":   {"X-Voice-Segment": "1"},
	} {
		if response := voicePartRequest(c, cookie, wav, "part", moved); response.Code != http.StatusConflict {
			t.Errorf("%s: status=%d, want 409", name, response.Code)
		}
	}
	if emitted := host.emittedSnapshot(); len(emitted) != 1 {
		t.Fatalf("emitted %d inbounds, want the first part once", len(emitted))
	}
}

func sineWAV(t *testing.T, duration time.Duration) []byte {
	t.Helper()
	const sampleRate = 16000
	const channels = 1
	const bits = 16
	samples := int(float64(sampleRate) * duration.Seconds())
	dataBytes := samples * channels * bits / 8
	var output bytes.Buffer
	output.WriteString("RIFF")
	_ = binary.Write(&output, binary.LittleEndian, uint32(36+dataBytes))
	output.WriteString("WAVEfmt ")
	_ = binary.Write(&output, binary.LittleEndian, uint32(16))
	_ = binary.Write(&output, binary.LittleEndian, uint16(1))
	_ = binary.Write(&output, binary.LittleEndian, uint16(channels))
	_ = binary.Write(&output, binary.LittleEndian, uint32(sampleRate))
	_ = binary.Write(&output, binary.LittleEndian, uint32(sampleRate*channels*bits/8))
	_ = binary.Write(&output, binary.LittleEndian, uint16(channels*bits/8))
	_ = binary.Write(&output, binary.LittleEndian, uint16(bits))
	output.WriteString("data")
	_ = binary.Write(&output, binary.LittleEndian, uint32(dataBytes))
	for index := 0; index < samples; index++ {
		sample := int16(math.Sin(2*math.Pi*440*float64(index)/sampleRate) * 10000)
		_ = binary.Write(&output, binary.LittleEndian, sample)
	}
	return output.Bytes()
}

func fileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode()
}
