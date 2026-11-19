package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/intake"
	"github.com/Andrometiq/c3/internal/mappings"
)

// The page learns from every prefs frame whether the authenticated front
// signed it in: the front signs its viewer out too, so the page hides C3's
// logout, which isn't there. C3's own login keeps it.
func TestPrefsTellAFrontViewerItHasNoLogout(t *testing.T) {
	c, _, cookie := newFrontTest(t)
	prefsFrames := func(r *http.Request, then func()) []string {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		recorder := newStreamRecorder()
		done := make(chan struct{})
		go func() {
			c.routes().ServeHTTP(recorder, r.WithContext(ctx))
			close(done)
		}()
		waitFor(t, func() bool {
			_, body := recorder.snapshot()
			return strings.Contains(body, "event: presence")
		})
		then()
		waitFor(t, func() bool {
			_, body := recorder.snapshot()
			return strings.Count(body, "event: prefs") == 2
		})
		cancel()
		<-done
		_, body := recorder.snapshot()
		var frames []string
		for _, frame := range strings.Split(body, "\n\n") {
			if strings.HasPrefix(frame, "event: prefs") {
				frames = append(frames, frame)
			}
		}
		return frames
	}
	turnVoiceOn := func(r *http.Request) func() {
		return func() {
			if w := serve(c, r); w.Code != http.StatusNoContent {
				t.Fatalf("POST /voice=%d %s", w.Code, w.Body.String())
			}
		}
	}
	frontVoice := frontRequest(http.MethodPost, "/voice", `{"enabled":true}`, frontAddress, frontOperator)
	frontVoice.Header.Set("Content-Type", "application/json")
	front := prefsFrames(frontRequest(http.MethodGet, "/events", "", frontAddress, frontOperator), turnVoiceOn(frontVoice))
	own := prefsFrames(request(http.MethodGet, "/events", "", cookie, false), turnVoiceOn(request(http.MethodPost, "/voice", `{"enabled":true}`, cookie, true)))
	for _, frame := range front {
		if !strings.Contains(frame, `"front_viewer":true`) {
			t.Errorf("front prefs frame %q lacks front_viewer", frame)
		}
	}
	for _, frame := range own {
		if strings.Contains(frame, "front_viewer") {
			t.Errorf("C3 login prefs frame %q names a front viewer", frame)
		}
	}
	page, _ := pages.ReadFile("page.html")
	for _, rule := range []string{
		`<button id="logout" type="button" hidden>logout</button>`,
		"logoutButton.hidden = Boolean(data.front_viewer);",
	} {
		if !strings.Contains(string(page), rule) {
			t.Errorf("page is missing logout rule %q", rule)
		}
	}
}

// Live and Hands-free (voice-activity detection) are off until a streaming
// voice model replaces them: their controls are hidden, their code kept.
func TestLiveAndHandsFreeControlsAreHidden(t *testing.T) {
	page, _ := pages.ReadFile("page.html")
	for _, id := range []string{"drive-live", "hands-free-toggle", "hands-free-state"} {
		tag := regexp.MustCompile(`<[a-z]+ id="` + id + `"[^>]*>`).FindString(string(page))
		if !regexp.MustCompile(`\shidden[\s>]`).MatchString(tag) {
			t.Errorf("control %s is shown: %s", id, tag)
		}
	}
}

// A voice row says "Transcribing…" once C3 has the note, then its transcript
// or C3's status in its place; a part of a longer recording names its place;
// a late "slow" never hides a final status.
func TestVoiceRowShowsItsLiveStatusAndPlaceWithNode(t *testing.T) {
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not on PATH; the executable voice-row test requires Node.js")
	}
	page, _ := pages.ReadFile("page.html")
	duration := regexp.MustCompile(`(?s)  function formatDuration\(seconds\) \{.*?\n  \}\n`).Find(page)
	helpers := regexp.MustCompile(`(?s)  const VOICE_UNSENT_TEXT = .*?  function mergeVoiceData\(known, change\) \{.*?\n  \}\n`).Find(page)
	if duration == nil || helpers == nil {
		t.Fatal("could not extract the voice-row helpers from page.html")
	}
	const cases = `
process.stdout.write(JSON.stringify({
  whole: voiceLabel({duration_seconds: 42}),
  first: voiceLabel({segment: 1, has_more: true, duration_seconds: 300}),
  last: voiceLabel({segment: 3}),
  uploading: voiceShown({isSending: true}),
  taken: voiceShown({}),
  heard: voiceShown({text: 'hello', voice_status: 'heard'}),
  oldHeard: voiceShown({text: 'hello'}),
  quiet: voiceShown({text: 'no speech', voice_status: 'no_speech'}),
  cancelled: voiceShown({segment: 3, unsent: 'cancelled'}),
  refused: voiceShown({segment: 2, has_more: true, unsent: 'refused'}),
  lateSlow: mergeVoiceData({text: 'hello', voice_status: 'heard'}, {text: 'slow', voice_status: 'slow'}).text,
  slow: mergeVoiceData({}, {text: 'slow', voice_status: 'slow'}).voice_status,
  retranscribed: mergeVoiceData({text: 'failed', voice_status: 'failed'}, {text: 'hello', voice_status: 'heard'}).voice_status
}));`
	output, err := exec.Command(nodePath, "-e", string(duration)+string(helpers)+cases).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, output)
	}
	var got map[string]any
	if err := json.Unmarshal(output, &got); err != nil {
		t.Fatalf("decode %s: %v", output, err)
	}
	want := map[string]any{
		"whole": "🎤 Voice note · 0:42", "first": "🎤 Voice note · part 1 · 5:00", "last": "🎤 Voice note · part 3, the last",
		"uploading": nil, "taken": []any{"transcribing", "Transcribing…"}, "heard": []any{"heard", "hello"},
		"oldHeard": []any{"heard", "hello"}, "quiet": []any{"no_speech", "no speech"},
		"cancelled": []any{"unsent", "Cancelled, not sent."}, "refused": []any{"unsent", "Not sent: C3 refused it."},
		"lateSlow": "hello", "slow": "slow", "retranscribed": "heard",
	}
	for name, value := range want {
		if fmt.Sprint(got[name]) != fmt.Sprint(value) {
			t.Errorf("%s = %v, want %v", name, got[name], value)
		}
	}
}

// webChatHarnessHost plays the broker at Emit, the boundary the broker
// proves: each voice note gets the broker's STT readback (a transcript naming
// its size) or, once asked, its no-speech status, then every message but a
// part with more to follow an answer. It logs what the session read for the driver to check.
type webChatHarnessHost struct {
	*fakeHost
	c          *Channel
	t          *testing.T
	logPath    string
	mu         sync.Mutex
	isNoSpeech bool
}

func (host *webChatHarnessHost) Emit(inbound *c3types.Inbound, _ ...*intake.Source) bool {
	copy := *inbound
	if copy.Kind != c3types.InboundMessage {
		return true // C3's own system notices (spoken replies on/off) aren't the operator's
	}
	hasVoice := len(copy.Attachments) == 1 && copy.Attachments[0].Kind == "voice"
	line, _ := json.Marshal(map[string]any{"id": copy.MessageID, "text": copy.Text, "voice": hasVoice})
	host.mu.Lock()
	if file, err := os.OpenFile(host.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); err == nil {
		_, _ = file.Write(append(line, '\n'))
		_ = file.Close()
	}
	isNoSpeech := host.isNoSpeech && hasVoice
	if isNoSpeech {
		host.isNoSpeech = false
	}
	host.mu.Unlock()
	go host.answer(&copy, hasVoice, isNoSpeech)
	return true
}

func (host *webChatHarnessHost) answer(inbound *c3types.Inbound, hasVoice, isNoSpeech bool) {
	replyTo := inbound.MessageID
	if hasVoice {
		time.Sleep(800 * time.Millisecond) // the broker's STT, played: long enough to see "Transcribing…"
		if isNoSpeech {
			_ = host.c.SendVoiceStatus(c3types.VoiceStatusArgs{ChatID: inbound.ChatID, MessageID: replyTo,
				Status: c3types.VoiceStatusNoSpeech, Text: c3types.VoiceNoSpeechNotice})
		} else {
			_, _ = host.c.SendReadback(c3types.ReadbackArgs{ChatID: inbound.ChatID, ReplyTo: &replyTo,
				Transcript: fmt.Sprintf("(harness transcript of %d bytes)", inbound.Attachments[0].Size)})
		}
	}
	if strings.Contains(inbound.Text, "more follows") {
		return // a session waits for a recording's last part before it answers
	}
	time.Sleep(300 * time.Millisecond)
	_, _ = host.c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: inbound.ChatID, ReplyTo: &replyTo,
		Text: fmt.Sprintf("Answer to msg %d.", inbound.MessageID)})
}

// TestWebChatBrowserHarness serves the web chat for a browser driver
// (check-webchat.mjs); nobody is in the loop. Set C3_WEBCHAT_HARNESS to a file
// path: the test writes {url, cookie, front_url, front_header,
// front_identity, inbounds, control} there and serves until that file is
// removed or C3_WEBCHAT_HARNESS_SECONDS (default 300) pass. C3's own login
// is served on 127.0.0.1; the authenticated front on this host's first
// non-loopback address, which the harness treats as the front's.
// C3_WEBCHAT_HARNESS_PART_MS shortens the page's part length. A "no-speech"
// line in the control file makes the next voice note one STT heard nothing in.
func TestWebChatBrowserHarness(t *testing.T) {
	infoPath := os.Getenv("C3_WEBCHAT_HARNESS")
	if infoPath == "" {
		t.Skip("set C3_WEBCHAT_HARNESS to an info file path to serve the web chat for a browser driver")
	}
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	ownListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	frontIP := firstNonLoopbackIPv4(t)
	frontListener, err := net.Listen("tcp", net.JoinHostPort(frontIP, "0"))
	if err != nil {
		t.Fatal(err)
	}
	host := &webChatHarnessHost{fakeHost: &fakeHost{telegram: telegramConfig{MasterUserID: 42}, registered: true, allowed: true},
		t: t, logPath: infoPath + ".inbounds"}
	c := New()
	host.c = c
	frontOrigin := "http://" + frontListener.Addr().String()
	c.host, c.operatorID, c.baseURL = host, 42, "http://"+ownListener.Addr().String()
	c.allowedOrigin = map[string]bool{c.baseURL: true, frontOrigin: true}
	c.cfg.Front = mappings.WebFront{Origin: frontOrigin, From: frontIP, IdentityHeader: "X-Harness-Identity", Operator: "operator@harness.test"}
	c.interfaceAddresses = func() ([]net.Addr, error) { return nil, nil } // the front's address counts as another host's
	if err := c.loadState(host); err != nil {
		t.Fatal(err)
	}
	c.SetRouteHolder(42, nil, &channel.RouteHolder{CLI: "harness-agent", CWD: "/harness/webchat", SessionID: "harness", Since: time.Now()})
	handler := shortenVoiceParts(t, c.routes(), os.Getenv("C3_WEBCHAT_HARNESS_PART_MS"))
	for _, listener := range []net.Listener{ownListener, frontListener} {
		server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
		go server.Serve(listener)
		defer server.Close()
	}
	controlPath := infoPath + ".control"
	info, _ := json.Marshal(map[string]any{
		"url": c.baseURL, "cookie": map[string]string{"name": sessionCookieName, "value": loginCookie(t, c).Value},
		"front_url": frontOrigin, "front_header": c.cfg.Front.IdentityHeader, "front_identity": c.cfg.Front.Operator,
		"inbounds": host.logPath, "control": controlPath,
	})
	if err := os.WriteFile(infoPath, info, 0o600); err != nil {
		t.Fatal(err)
	}
	seconds, err := strconv.Atoi(os.Getenv("C3_WEBCHAT_HARNESS_SECONDS"))
	if err != nil || seconds <= 0 {
		seconds = 300
	}
	for deadline := time.Now().Add(time.Duration(seconds) * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if _, err := os.Stat(infoPath); err != nil {
			return
		}
		if commands, err := os.ReadFile(controlPath); err == nil {
			_ = os.Remove(controlPath)
			host.mu.Lock()
			host.isNoSpeech = host.isNoSpeech || strings.Contains(string(commands), "no-speech")
			host.mu.Unlock()
		}
	}
}

// shortenVoiceParts serves the page with its part length set to partMS (a
// number of milliseconds), so the numbered-parts path runs in seconds; ""
// serves it as is.
func shortenVoiceParts(t *testing.T, handler http.Handler, partMS string) http.Handler {
	if partMS == "" {
		return handler
	}
	if _, err := strconv.Atoi(partMS); err != nil {
		t.Fatalf("C3_WEBCHAT_HARNESS_PART_MS %q is not a number of milliseconds", partMS)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/" {
			handler.ServeHTTP(w, r)
			return
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, r)
		page := recorder.Body.String()
		if recorder.Code == http.StatusOK {
			shortened := strings.Replace(page, "const VOICE_PART_MS = 5 * 60 * 1000;", "const VOICE_PART_MS = "+partMS+";", 1)
			if shortened == page {
				t.Error("page.html has no five-minute VOICE_PART_MS to shorten")
			}
			page = shortened
		}
		for name, values := range recorder.Header() {
			w.Header()[name] = values
		}
		w.WriteHeader(recorder.Code)
		_, _ = io.WriteString(w, page)
	})
}

func firstNonLoopbackIPv4(t *testing.T) string {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range addresses {
		if ip := interfaceIP(address).To4(); ip != nil && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
			return ip.String()
		}
	}
	t.Skip("no non-loopback IPv4 address to serve the front on")
	return ""
}
