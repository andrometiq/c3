package web

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func setVoiceEnabled(t *testing.T, c *Channel, cookie *http.Cookie, enabled bool) {
	t.Helper()
	c.authMu.Lock()
	current := c.sessions[sessionKey(cookie.Value)]
	if current == nil {
		c.authMu.Unlock()
		t.Fatal("test session missing")
	}
	current.voice = enabled
	c.authMu.Unlock()
}

func TestVoicePreferencePersistsAndEmitsOneSystemEventPerChange(t *testing.T) {
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
	post := func(enabled bool) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		body := `{"enabled":false}`
		if enabled {
			body = `{"enabled":true}`
		}
		first.routes().ServeHTTP(response, request(http.MethodPost, "/voice", body, cookie, true))
		return response
	}
	if response := post(true); response.Code != http.StatusNoContent {
		t.Fatalf("enable status/body=%d/%q", response.Code, response.Body.String())
	}
	if response := post(true); response.Code != http.StatusNoContent {
		t.Fatalf("idempotent enable status=%d", response.Code)
	}
	emitted := host.emittedSnapshot()
	if len(emitted) != 1 {
		t.Fatalf("toggle emitted %d events, want one", len(emitted))
	}
	event := emitted[0]
	if event.Channel != Name || event.ChatID != 42 || event.Kind != c3types.InboundSystem || event.Event == nil || event.Event.System == nil {
		t.Fatalf("system inbound=%+v", event)
	}
	if system := event.Event.System; system.Source != "web" || system.Level != "info" || system.Title != "Spoken replies ON" || system.Message != spokenRepliesOnMessage {
		t.Fatalf("system event=%+v", system)
	}

	recorder, cancel, done := startSSE(first, cookie, "", "")
	waitFor(t, func() bool {
		_, body := recorder.snapshot()
		return strings.Contains(body, "event: prefs") && strings.Contains(body, `{"voice":true}`)
	})
	cancel()
	<-done
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	second := New()
	second.host = host
	second.operatorID = 42
	second.allowedOrigin = first.allowedOrigin
	if err := second.loadState(host); err != nil {
		t.Fatal(err)
	}
	if _, current, ok := second.authenticate(request(http.MethodGet, "/", "", cookie, false), false); !ok || !current.voice {
		t.Fatalf("persisted voice preference ok/current=%v/%+v", ok, current)
	}
	off := httptest.NewRecorder()
	second.routes().ServeHTTP(off, request(http.MethodPost, "/voice", `{"enabled":false}`, cookie, true))
	if off.Code != http.StatusNoContent {
		t.Fatalf("disable status/body=%d/%q", off.Code, off.Body.String())
	}
	emitted = host.emittedSnapshot()
	if len(emitted) != 2 || emitted[1].Event.System.Title != "Spoken replies OFF" || emitted[1].Event.System.Message != spokenRepliesOffMessage {
		t.Fatalf("off system event=%+v", emitted)
	}
}

func TestSpokenReplyAudioServingRangeExpiryAndVoiceOff(t *testing.T) {
	c, host, cookie := newHandlerChannel()
	audio := []byte("ID3-fake-mp3-bytes")
	host.synthesize = func(_ context.Context, request c3types.SpeechRequest) (c3types.SpeechResult, error) {
		if request.Text != "spoken answer" || request.Language != "" {
			t.Fatalf("speech request=%+v", request)
		}
		return c3types.SpeechResult{Audio: audio, MIME: "audio/mpeg", Provider: "fake"}, nil
	}
	if _, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "voice off"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if host.synthCallCount() != 0 {
		t.Fatal("voice-off reply incurred synthesis")
	}

	setVoiceEnabled(t, c, cookie, true)
	if _, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "voice on without stream"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if host.synthCallCount() != 0 {
		t.Fatal("voice-on session without an SSE stream incurred synthesis")
	}
	recorder, cancel, done := startSSE(c, cookie, "", "")
	defer func() { cancel(); <-done }()
	waitFor(t, func() bool { return streamCount(c) == 1 })
	messageID, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "spoken answer"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		_, body := recorder.snapshot()
		return strings.Contains(body, "event: audio") && strings.Contains(body, `"provider":"fake"`)
	})
	_, stream := recorder.snapshot()
	if strings.Count(stream, "event: audio") != 1 {
		t.Fatalf("audio event count in %q", stream)
	}
	match := regexp.MustCompile(`"url":"([^"]+)"`).FindStringSubmatch(stream)
	if len(match) != 2 {
		t.Fatalf("audio URL missing from %q", stream)
	}
	token := strings.TrimPrefix(match[1], "/audio/")
	c.audioMu.Lock()
	cached := c.audioCache[token]
	c.audioMu.Unlock()
	if cached.messageID != messageID {
		t.Fatalf("cached message id=%d, want %d", cached.messageID, messageID)
	}

	serve := func(path string, rangeHeader string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		request := request(http.MethodGet, path, "", cookie, false)
		if rangeHeader != "" {
			request.Header.Set("Range", rangeHeader)
		}
		c.routes().ServeHTTP(response, request)
		return response
	}
	full := serve(match[1], "")
	if full.Code != http.StatusOK || full.Body.String() != string(audio) || full.Header().Get("Content-Type") != "audio/mpeg" || full.Header().Get("Cache-Control") != "no-store" || full.Header().Get("Accept-Ranges") != "bytes" || full.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("full audio status/body/headers=%d/%q/%v", full.Code, full.Body.String(), full.Header())
	}
	partial := serve(match[1], "bytes=4-7")
	if partial.Code != http.StatusPartialContent || partial.Body.String() != string(audio[4:8]) {
		t.Fatalf("range status/body=%d/%q", partial.Code, partial.Body.String())
	}
	if unknown := serve("/audio/00000000000000000000000000000000", ""); unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown token status=%d", unknown.Code)
	}
	unlock := serve("/audio/unlock", "bytes=0-2")
	if unlock.Code != http.StatusPartialContent || unlock.Header().Get("Content-Type") != "audio/mpeg" || unlock.Header().Get("X-Content-Type-Options") != "nosniff" || unlock.Body.Len() != 3 {
		t.Fatalf("unlock status/headers/bytes=%d/%v/%d", unlock.Code, unlock.Header(), unlock.Body.Len())
	}
	base := c.now()
	c.now = func() time.Time { return base.Add(audioCacheTTL) }
	if expired := serve(match[1], ""); expired.Code != http.StatusNotFound {
		t.Fatalf("expired token status=%d", expired.Code)
	}
}

func TestAudioRoutesRequireSessionCookie(t *testing.T) {
	c, _, _ := newHandlerChannel()
	for _, path := range []string{"/audio/00000000000000000000000000000000", "/audio/unlock"} {
		response := httptest.NewRecorder()
		c.routes().ServeHTTP(response, request(http.MethodGet, path, "", nil, false))
		if response.Code != http.StatusUnauthorized || response.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("GET %s without cookie status/headers=%d/%v", path, response.Code, response.Header())
		}
	}
}

func TestUnlockMP3MatchesGenerator(t *testing.T) {
	audio, err := pages.ReadFile("unlock.mp3")
	if err != nil {
		t.Fatal(err)
	}
	if len(audio) < 900 || len(audio) > 1300 || len(audio) < 4 {
		t.Fatalf("unlock.mp3 size=%d, want approximately 1 KB", len(audio))
	}
	header := binary.BigEndian.Uint32(audio[:4])
	if header>>21 != 0x7ff || header>>19&3 != 2 || header>>17&3 != 1 || header>>12&15 != 3 || header>>10&3 != 1 {
		t.Fatalf("unlock.mp3 first frame=%08x, want MPEG-2 Layer III at 24 kbps/24 kHz", header)
	}
}

func TestShortSpeechErrorUsesFixedReasons(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{name: "provider", err: errors.New("handler stderr must stay private"), want: "provider error"},
		{name: "timeout", err: context.DeadlineExceeded, want: "timeout"},
		{name: "not configured", err: c3types.ErrNoSynthesizer, want: "not configured"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := shortSpeechError(test.err); got != test.want {
				t.Fatalf("shortSpeechError(%v)=%q, want %q", test.err, got, test.want)
			}
		})
	}
}

func TestSpeechNothingToSayAndErrorsAreLiveOnlyAndThrottled(t *testing.T) {
	t.Run("nothing to say", func(t *testing.T) {
		c, host, cookie := newHandlerChannel()
		setVoiceEnabled(t, c, cookie, true)
		host.synthesize = func(context.Context, c3types.SpeechRequest) (c3types.SpeechResult, error) {
			return c3types.SpeechResult{}, c3types.ErrNothingToSay
		}
		recorder, cancel, done := startSSE(c, cookie, "", "")
		defer func() { cancel(); <-done }()
		waitFor(t, func() bool { return streamCount(c) == 1 })
		_, _ = c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "```"})
		waitFor(t, func() bool { return host.synthCallCount() == 1 })
		time.Sleep(10 * time.Millisecond)
		_, body := recorder.snapshot()
		if strings.Contains(body, "event: audio") || strings.Contains(body, "voice unavailable") {
			t.Fatalf("nothing-to-say surfaced audio/error: %q", body)
		}
	})

	t.Run("provider error throttled", func(t *testing.T) {
		c, host, cookie := newHandlerChannel()
		setVoiceEnabled(t, c, cookie, true)
		host.synthesize = func(context.Context, c3types.SpeechRequest) (c3types.SpeechResult, error) {
			return c3types.SpeechResult{}, errors.New("provider stderr detail")
		}
		recorder, cancel, done := startSSE(c, cookie, "", "")
		defer func() { cancel(); <-done }()
		waitFor(t, func() bool { return streamCount(c) == 1 })
		_, _ = c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "one"})
		_, _ = c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "two"})
		waitFor(t, func() bool { return host.synthCallCount() == 2 })
		waitFor(t, func() bool {
			_, body := recorder.snapshot()
			return strings.Contains(body, "voice unavailable")
		})
		_, body := recorder.snapshot()
		if strings.Count(body, "voice unavailable") != 1 || !strings.Contains(body, "🔇 voice unavailable: provider error") || strings.Contains(body, "provider stderr detail") || strings.Contains(body, "event: audio") {
			t.Fatalf("provider error stream=%q", body)
		}
		if !strings.Contains(host.logText(), "provider stderr detail") {
			t.Fatalf("provider detail was not logged: %q", host.logText())
		}
	})
}

func TestSpeechConcurrencyAndQueueBound(t *testing.T) {
	c, host, cookie := newHandlerChannel()
	setVoiceEnabled(t, c, cookie, true)
	client, _, _ := c.connectStream(sessionKey(cookie.Value), "")
	defer c.removeStream(sessionKey(cookie.Value), client)
	release := make(chan struct{})
	started := make(chan struct{}, 16)
	var active atomic.Int64
	var maximum atomic.Int64
	host.synthesize = func(context.Context, c3types.SpeechRequest) (c3types.SpeechResult, error) {
		current := active.Add(1)
		for {
			prior := maximum.Load()
			if current <= prior || maximum.CompareAndSwap(prior, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return c3types.SpeechResult{Audio: []byte("ID3"), MIME: "audio/mpeg"}, nil
	}
	for index := 0; index < speechConcurrency+speechQueueLimit+2; index++ {
		_, _ = c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "reply"})
	}
	for range speechConcurrency {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("two synthesis calls did not start")
		}
	}
	if maximum.Load() > speechConcurrency {
		t.Fatalf("in flight=%d, want <=%d", maximum.Load(), speechConcurrency)
	}
	close(release)
	waitFor(t, func() bool { return host.synthCallCount() == speechConcurrency+speechQueueLimit })
	waitFor(t, func() bool {
		c.speechMu.Lock()
		defer c.speechMu.Unlock()
		return c.speechActive == 0 && len(c.speechQueue) == 0
	})
	if maximum.Load() > speechConcurrency || !strings.Contains(host.logText(), "speech queue full") {
		t.Fatalf("maximum/log=%d/%q", maximum.Load(), host.logText())
	}
}
