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

func TestVoiceFailureReplyPersistsAsOwnRowEdit(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	first, host, _ := newHandlerChannel()
	if err := first.loadState(host); err != nil {
		t.Fatal(err)
	}
	voice := true
	sourceID := int64(17)
	first.publish(streamEvent{kind: "own", payload: streamPayload{
		MessageID: sourceID, Voice: &voice, Timestamp: streamTimestamp(first.now()),
	}})
	notice := c3types.VoiceTranscriptionFailureNoticePrefix + " — see logs / try again."
	messageID, err := first.SendReply(c3types.ReplyArgs{
		Channel: Name, ChatID: 42, ReplyTo: &sourceID, Text: notice,
	})
	if err != nil {
		t.Fatal(err)
	}
	if messageID != sourceID || len(first.replay) != 2 || first.replay[1].kind != "edit" || first.replay[1].payload.MessageID != sourceID || first.replay[1].payload.Text != notice || first.replay[1].payload.Voice == nil || !*first.replay[1].payload.Voice {
		t.Fatalf("failure reply id/replay=%d/%+v", messageID, first.replay)
	}
	for _, event := range first.replay {
		if event.kind == "message" {
			t.Fatalf("failure reply created an agent row: %+v", first.replay)
		}
	}
	if !isVoiceReadbackNotice(c3types.VoiceDownloadFailureNoticePrefix + ", so it wasn't transcribed") {
		t.Fatal("download-failure prefix is not recognized as voice readback")
	}

	second := New()
	second.host = host
	second.operatorID = 42
	if err := second.loadState(host); err != nil {
		t.Fatal(err)
	}
	if len(second.replay) != 2 || second.replay[1].kind != "edit" || second.replay[1].payload.MessageID != sourceID || second.replay[1].payload.Text != notice {
		t.Fatalf("persisted failure edit=%+v", second.replay)
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
