package web

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

type fakeHost struct {
	mu              sync.Mutex
	webConfig       Config
	telegram        telegramConfig
	registered      bool
	allowed         bool
	gate            channel.GateInboundDecision
	emitResults     []bool
	emitted         []*c3types.Inbound
	logs            []string
	queuedMax       int64
	queuedErr       error
	loginDeliveries int
	done            chan struct{}
}

func (host *fakeHost) Config(name string, target any) error {
	switch name {
	case Name:
		*(target.(*Config)) = host.webConfig
	case "telegram":
		*(target.(*telegramConfig)) = host.telegram
	default:
		return errors.New("missing config")
	}
	return nil
}

func (host *fakeHost) Emit(inbound *c3types.Inbound) bool {
	host.mu.Lock()
	defer host.mu.Unlock()
	copy := *inbound
	host.emitted = append(host.emitted, &copy)
	if len(host.emitResults) == 0 {
		return true
	}
	result := host.emitResults[0]
	host.emitResults = host.emitResults[1:]
	return result
}

func (host *fakeHost) Logf(format string, args ...any) {
	host.mu.Lock()
	host.logs = append(host.logs, fmt.Sprintf(format, args...))
	host.mu.Unlock()
}

func (host *fakeHost) Done() <-chan struct{} {
	if host.done == nil {
		host.done = make(chan struct{})
	}
	return host.done
}

func (*fakeHost) NotifyHealth(c3types.HealthEvent) {}

func (host *fakeHost) GateInbound(*c3types.Inbound) channel.GateInboundDecision {
	return host.gate
}

func (*fakeHost) HandleCommand(*c3types.Inbound) (string, bool) { return "", false }
func (host *fakeHost) ChannelRegistered(name string) bool {
	return name == "telegram" && host.registered
}
func (host *fakeHost) UserAllowed(userID int64) bool {
	return host.allowed && userID == host.telegram.MasterUserID
}
func (host *fakeHost) MaxQueuedMessageID(string, int64, *int64) (int64, error) {
	return host.queuedMax, host.queuedErr
}
func (host *fakeHost) SendWebLoginLink(requestedBy string) (bool, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	if requestedBy != "requested from the web login page" {
		return false, fmt.Errorf("unexpected reason %q", requestedBy)
	}
	host.loginDeliveries++
	return true, nil
}

func (host *fakeHost) emittedSnapshot() []*c3types.Inbound {
	host.mu.Lock()
	defer host.mu.Unlock()
	return append([]*c3types.Inbound(nil), host.emitted...)
}

func (host *fakeHost) logText() string {
	host.mu.Lock()
	defer host.mu.Unlock()
	return strings.Join(host.logs, "\n")
}

func newHandlerChannel() (*Channel, *fakeHost, *http.Cookie) {
	host := &fakeHost{telegram: telegramConfig{MasterUserID: 42}, registered: true, allowed: true}
	c := New()
	c.host = host
	c.operatorID = 42
	c.baseURL = "http://127.0.0.1:8371"
	c.allowedOrigin = map[string]bool{
		"http://127.0.0.1:8371": true,
		"http://localhost:8371": true,
	}
	now := c.now()
	cookieValue := "session-one"
	c.sessions[sessionKey(cookieValue)] = &session{userID: 42, username: "operator", created: now, lastSeen: now, savedLastSeen: now}
	return c, host, &http.Cookie{Name: sessionCookieName, Value: cookieValue}
}

func request(method, path, body string, cookie *http.Cookie, withOrigin bool) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.RemoteAddr = "192.0.2.1:1234"
	if cookie != nil {
		r.AddCookie(cookie)
	}
	if withOrigin {
		r.Header.Set("Origin", "http://127.0.0.1:8371")
	}
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	return r
}

func sendRequest(c *Channel, cookie *http.Cookie, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	c.routes().ServeHTTP(recorder, request(http.MethodPost, "/send", body, cookie, true))
	return recorder
}

func TestSendTrustBoundaryAndGate(t *testing.T) {
	t.Run("gate drop is forbidden and final", func(t *testing.T) {
		c, host, cookie := newHandlerChannel()
		host.gate = channel.GateInboundDrop
		for attempt := 0; attempt < 2; attempt++ {
			response := sendRequest(c, cookie, `{"text":"blocked","client_id":"drop-1"}`)
			if response.Code != http.StatusForbidden || response.Body.String() != "{\"error\":\"not allowed\"}\n" {
				t.Fatalf("attempt %d status/body=%d/%q, want uniform 403", attempt+1, response.Code, response.Body.String())
			}
		}
		if len(host.emittedSnapshot()) != 0 || len(c.replay) != 0 {
			t.Fatalf("gate drop emitted=%d replay=%d, want 0/0", len(host.emittedSnapshot()), len(c.replay))
		}
	})

	t.Run("server stamps every field", func(t *testing.T) {
		c, host, cookie := newHandlerChannel()
		before := c.now()
		response := sendRequest(c, cookie, `{"text":"hello","client_id":"stamp-1"}`)
		if response.Code != http.StatusAccepted {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
		rows := host.emittedSnapshot()
		if len(rows) != 1 {
			t.Fatalf("emits=%d, want 1", len(rows))
		}
		got := rows[0]
		if got.Channel != Name || got.ChatID != 42 || got.TopicID != nil || got.MessageID <= 0 || got.Sender.UserID != 42 || got.Sender.Username != "operator" || got.Text != "hello" || got.ConvKind != c3types.ConvKindDM || got.Kind != c3types.InboundMessage || got.V != c3types.InboundRecordVersion || got.Timestamp.Before(before) {
			t.Fatalf("stamped inbound=%+v", got)
		}
	})

	t.Run("overlong text is rejected", func(t *testing.T) {
		c, host, cookie := newHandlerChannel()
		text := strings.Repeat("🙂", maxMessageRunes+1)
		body, err := json.Marshal(map[string]string{"text": text, "client_id": "cap-1"})
		if err != nil {
			t.Fatal(err)
		}
		response := sendRequest(c, cookie, string(body))
		if response.Code != http.StatusRequestEntityTooLarge || response.Body.String() != "{\"error\":\"message too long\"}\n" {
			t.Fatalf("status/body=%d/%q", response.Code, response.Body.String())
		}
		if got := len(host.emittedSnapshot()); got != 0 {
			t.Fatalf("overlong request emitted %d messages", got)
		}
	})

	t.Run("exact rune limit is accepted", func(t *testing.T) {
		c, host, cookie := newHandlerChannel()
		body, err := json.Marshal(map[string]string{
			"text": strings.Repeat("🙂", maxMessageRunes), "client_id": "exact-cap",
		})
		if err != nil {
			t.Fatal(err)
		}
		response := sendRequest(c, cookie, string(body))
		if response.Code != http.StatusAccepted || len(host.emittedSnapshot()) != 1 {
			t.Fatalf("status/emitted=%d/%d", response.Code, len(host.emittedSnapshot()))
		}
	})

	for _, body := range []string{
		`{"text":"x","client_id":"extra","extra":true}`,
		`{"text":"x","client_id":"kind","Kind":"poll_result"}`,
		`{"text":"x","client_id":"user","UserID":99}`,
	} {
		t.Run("rejects untrusted JSON "+body, func(t *testing.T) {
			c, host, cookie := newHandlerChannel()
			response := sendRequest(c, cookie, body)
			if response.Code != http.StatusBadRequest || len(host.emittedSnapshot()) != 0 {
				t.Fatalf("status=%d emitted=%d", response.Code, len(host.emittedSnapshot()))
			}
		})
	}
}

func TestSendRetryIdempotency(t *testing.T) {
	c, host, cookie := newHandlerChannel()
	host.emitResults = []bool{false, true}
	body := `{"text":"retry me","client_id":"retry-1"}`
	first := sendRequest(c, cookie, body)
	second := sendRequest(c, cookie, body)
	third := sendRequest(c, cookie, body)
	if first.Code != http.StatusServiceUnavailable || first.Header().Get("Retry-After") != "2" {
		t.Fatalf("first status/Retry-After=%d/%q", first.Code, first.Header().Get("Retry-After"))
	}
	if second.Code != http.StatusAccepted || third.Code != http.StatusAccepted {
		t.Fatalf("retry statuses=%d/%d", second.Code, third.Code)
	}
	var replies [3]struct {
		MessageID int64 `json:"message_id"`
	}
	for index, response := range []*httptest.ResponseRecorder{first, second, third} {
		if err := json.Unmarshal(response.Body.Bytes(), &replies[index]); err != nil {
			t.Fatal(err)
		}
	}
	if replies[0].MessageID != replies[1].MessageID || replies[1].MessageID != replies[2].MessageID {
		t.Fatalf("ids changed across retries: %+v", replies)
	}
	rows := host.emittedSnapshot()
	if len(rows) != 2 || rows[0].MessageID != rows[1].MessageID {
		t.Fatalf("emits=%+v, want two attempts with same id", rows)
	}
}

func TestConcurrentSameClientIDIsSerialized(t *testing.T) {
	c, host, cookie := newHandlerChannel()
	const requests = 12
	var wait sync.WaitGroup
	wait.Add(requests)
	statuses := make(chan int, requests)
	for range requests {
		go func() {
			defer wait.Done()
			statuses <- sendRequest(c, cookie, `{"text":"once","client_id":"same"}`).Code
		}()
	}
	wait.Wait()
	close(statuses)
	for status := range statuses {
		if status != http.StatusAccepted {
			t.Fatalf("concurrent status=%d", status)
		}
	}
	if got := len(host.emittedSnapshot()); got != 1 {
		t.Fatalf("Emit called %d times, want 1", got)
	}
}

func tokenFromLink(t *testing.T, link string) string {
	t.Helper()
	_, token, ok := strings.Cut(link, "#")
	if !ok || token == "" {
		t.Fatalf("link has no fragment token: %q", link)
	}
	return token
}

func postAuth(c *Channel, token string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	c.routes().ServeHTTP(recorder, request(http.MethodPost, "/auth", fmt.Sprintf(`{"token":%q}`, token), nil, true))
	return recorder
}

func TestMagicLinkExchange(t *testing.T) {
	c, _, _ := newHandlerChannel()
	c.failedAttemptDelay = time.Millisecond
	link, err := c.MintLoginLink(42)
	if err != nil {
		t.Fatal(err)
	}
	token := tokenFromLink(t, link)
	noOrigin := httptest.NewRecorder()
	c.routes().ServeHTTP(noOrigin, request(http.MethodPost, "/auth", fmt.Sprintf(`{"token":%q}`, token), nil, false))
	if noOrigin.Code != http.StatusForbidden {
		t.Fatalf("POST /auth without origin=%d", noOrigin.Code)
	}

	get := httptest.NewRecorder()
	c.routes().ServeHTTP(get, request(http.MethodGet, "/auth", "", nil, false))
	if get.Code != http.StatusOK || get.Header().Get("Referrer-Policy") != "no-referrer" {
		t.Fatalf("GET /auth status/header=%d/%q", get.Code, get.Header().Get("Referrer-Policy"))
	}
	c.authMu.Lock()
	_, stillLive := c.tokens[token]
	c.authMu.Unlock()
	if !stillLive {
		t.Fatal("GET /auth consumed the token")
	}

	accepted := postAuth(c, token)
	if accepted.Code != http.StatusSeeOther || len(accepted.Result().Cookies()) != 1 {
		t.Fatalf("POST /auth status/cookies=%d/%v", accepted.Code, accepted.Result().Cookies())
	}
	for _, candidate := range []string{token, "unknown-token"} {
		denied := postAuth(c, candidate)
		if denied.Code != http.StatusUnauthorized || strings.TrimSpace(denied.Body.String()) != errInvalidLogin.Error() {
			t.Fatalf("denial for %q=%d/%q", candidate, denied.Code, denied.Body.String())
		}
	}
}

func TestAuthExpiredUnknownAndFailedAttemptAreUniform(t *testing.T) {
	c, _, _ := newHandlerChannel()
	c.failedAttemptDelay = time.Millisecond
	expiredLink, _ := c.MintLoginLink(42)
	expired := tokenFromLink(t, expiredLink)
	c.authMu.Lock()
	token := c.tokens[expired]
	token.expires = c.now().Add(-time.Second)
	c.tokens[expired] = token
	c.authMu.Unlock()

	bodies := make([]string, 0, 3)
	for _, candidate := range []string{expired, "unknown", expired} {
		response := postAuth(c, candidate)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("status=%d for %q", response.Code, candidate)
		}
		bodies = append(bodies, response.Body.String())
	}
	if bodies[0] != bodies[1] || bodies[1] != bodies[2] {
		t.Fatalf("non-uniform auth bodies: %q", bodies)
	}

	validLink, _ := c.MintLoginLink(42)
	valid := tokenFromLink(t, validLink)
	_ = postAuth(c, "wrong")
	if response := postAuth(c, valid); response.Code != http.StatusSeeOther {
		t.Fatalf("failed attempt invalidated valid token: %d", response.Code)
	}
}

func TestFailedAuthDelayIsCapped(t *testing.T) {
	c := New()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }
	c.failedAttemptDelay = 2 * time.Second
	c.failedAuth["192.0.2.1"] = now.Add(4 * time.Second)
	var slept time.Duration
	c.sleep = func(delay time.Duration) { slept = delay }

	c.delayFailedAuth("192.0.2.1:1234")
	if slept != maxFailedAuthDelay {
		t.Fatalf("failed auth slept %s, want cap %s", slept, maxFailedAuthDelay)
	}
	if got := c.failedAuth["192.0.2.1"].Sub(now); got != maxFailedAuthDelay {
		t.Fatalf("failed auth deadline=%s, want cap %s", got, maxFailedAuthDelay)
	}
}

func TestSessionCookieAttributes(t *testing.T) {
	c, _, _ := newHandlerChannel()
	for _, test := range []struct {
		name      string
		publicURL string
		tls       bool
		secure    bool
	}{
		{"plain loopback", "", false, false},
		{"tls request", "", true, true},
		{"https public url", "https://device.ts.net", false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c.cfg.PublicURL = test.publicURL
			r := request(http.MethodPost, "/auth", "", nil, true)
			if test.tls {
				r.TLS = &tls.ConnectionState{}
			}
			w := httptest.NewRecorder()
			c.setSessionCookie(w, r, "id")
			cookie := w.Result().Cookies()[0]
			if !cookie.HttpOnly || cookie.SameSite != http.SameSiteLaxMode || cookie.Path != "/" || cookie.Secure != test.secure {
				t.Fatalf("cookie=%+v", cookie)
			}
		})
	}
}

func TestSessionAuthOriginLogoutAndExpiry(t *testing.T) {
	c, _, cookie := newHandlerChannel()

	withoutOrigin := sendRequestWithoutOrigin(c, cookie, `{"text":"x","client_id":"x"}`)
	if withoutOrigin.Code != http.StatusForbidden {
		t.Fatalf("POST /send without origin=%d", withoutOrigin.Code)
	}
	fetchSite := request(http.MethodPost, "/send", `{"text":"x","client_id":"fetch-site"}`, cookie, false)
	fetchSite.Header.Set("Sec-Fetch-Site", "same-origin")
	fetchSiteResponse := httptest.NewRecorder()
	c.routes().ServeHTTP(fetchSiteResponse, fetchSite)
	if fetchSiteResponse.Code != http.StatusAccepted {
		t.Fatalf("same-origin Sec-Fetch-Site status=%d", fetchSiteResponse.Code)
	}

	tampered := httptest.NewRecorder()
	c.routes().ServeHTTP(tampered, request(http.MethodGet, "/events", "", &http.Cookie{Name: sessionCookieName, Value: "tampered"}, false))
	if tampered.Code != http.StatusUnauthorized {
		t.Fatalf("tampered cookie status=%d", tampered.Code)
	}

	c.authMu.Lock()
	c.sessions[sessionKey(cookie.Value)].lastSeen = c.now().Add(-sessionIdleTTL)
	c.authMu.Unlock()
	expired := httptest.NewRecorder()
	c.routes().ServeHTTP(expired, request(http.MethodGet, "/", "", cookie, false))
	if expired.Code != http.StatusSeeOther || expired.Header().Get("Location") != "/login" {
		t.Fatalf("idle-expired root=%d location=%q", expired.Code, expired.Header().Get("Location"))
	}
}

func sendRequestWithoutOrigin(c *Channel, cookie *http.Cookie, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	c.routes().ServeHTTP(recorder, request(http.MethodPost, "/send", body, cookie, false))
	return recorder
}

type streamRecorder struct {
	mu     sync.Mutex
	header http.Header
	code   int
	body   strings.Builder
	flush  chan struct{}
}

func newStreamRecorder() *streamRecorder {
	return &streamRecorder{header: make(http.Header), flush: make(chan struct{}, 32)}
}

func (recorder *streamRecorder) Header() http.Header { return recorder.header }
func (recorder *streamRecorder) WriteHeader(code int) {
	recorder.mu.Lock()
	if recorder.code == 0 {
		recorder.code = code
	}
	recorder.mu.Unlock()
}
func (recorder *streamRecorder) Write(data []byte) (int, error) {
	recorder.mu.Lock()
	if recorder.code == 0 {
		recorder.code = http.StatusOK
	}
	count, err := recorder.body.Write(data)
	recorder.mu.Unlock()
	return count, err
}
func (recorder *streamRecorder) Flush() {
	select {
	case recorder.flush <- struct{}{}:
	default:
	}
}
func (recorder *streamRecorder) snapshot() (int, string) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return recorder.code, recorder.body.String()
}

func startSSE(c *Channel, cookie *http.Cookie, lastID, origin string) (*streamRecorder, context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	r := request(http.MethodGet, "/events", "", cookie, false).WithContext(ctx)
	if lastID != "" {
		r.Header.Set("Last-Event-ID", lastID)
	}
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	recorder := newStreamRecorder()
	done := make(chan struct{})
	go func() {
		c.routes().ServeHTTP(recorder, r)
		close(done)
	}()
	return recorder, cancel, done
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not reached")
}

func streamCount(c *Channel) int {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	count := 0
	for _, clients := range c.streams {
		count += len(clients)
	}
	return count
}

func TestEventsAuthOriginAndLiveOutbound(t *testing.T) {
	c, _, cookie := newHandlerChannel()
	noCookie := httptest.NewRecorder()
	c.routes().ServeHTTP(noCookie, request(http.MethodGet, "/events", "", nil, false))
	if noCookie.Code != http.StatusUnauthorized {
		t.Fatalf("GET /events without cookie=%d", noCookie.Code)
	}
	explicitCross := httptest.NewRecorder()
	r := request(http.MethodGet, "/events", "", cookie, false)
	r.Header.Set("Origin", "https://evil.example")
	c.routes().ServeHTTP(explicitCross, r)
	if explicitCross.Code != http.StatusForbidden {
		t.Fatalf("cross-site events status=%d", explicitCross.Code)
	}

	recorder, cancel, done := startSSE(c, cookie, "", "")
	defer cancel()
	waitFor(t, func() bool { return streamCount(c) == 1 })
	if id, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "agent answer"}); err != nil || id <= 0 {
		t.Fatalf("SendReply=%d/%v", id, err)
	}
	waitFor(t, func() bool {
		code, body := recorder.snapshot()
		return code == http.StatusOK && strings.Contains(body, "agent answer")
	})
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SSE handler did not stop after context cancellation")
	}
}

func TestFreshEventsReplayOwnAndAgentHistory(t *testing.T) {
	c, _, cookie := newHandlerChannel()
	response := sendRequest(c, cookie, `{"text":"question from browser","client_id":"reload-own"}`)
	if response.Code != http.StatusAccepted {
		t.Fatalf("send status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "answer from agent"}); err != nil {
		t.Fatal(err)
	}

	recorder, cancel, done := startSSE(c, cookie, "", "")
	waitFor(t, func() bool {
		_, body := recorder.snapshot()
		return strings.Contains(body, "question from browser") && strings.Contains(body, "answer from agent")
	})
	_, body := recorder.snapshot()
	if !strings.Contains(body, "event: own") || !strings.Contains(body, "event: message") || strings.Contains(body, "history may be incomplete") {
		t.Fatalf("fresh replay=%q", body)
	}
	cancel()
	<-done

	resumed, resumedCancel, resumedDone := startSSE(c, cookie, "1", "")
	waitFor(t, func() bool {
		_, replay := resumed.snapshot()
		return strings.Contains(replay, "answer from agent")
	})
	_, replay := resumed.snapshot()
	if strings.Contains(replay, "question from browser") || strings.Contains(replay, "event: own") {
		t.Fatalf("Last-Event-ID replay changed: %q", replay)
	}
	resumedCancel()
	<-resumedDone
}

func TestInboundAndReplyIDsHaveSeparateCounters(t *testing.T) {
	c, _, cookie := newHandlerChannel()
	inbound := sendRequest(c, cookie, `{"text":"question","client_id":"counter-test"}`)
	if inbound.Code != http.StatusAccepted {
		t.Fatalf("send status=%d body=%s", inbound.Code, inbound.Body.String())
	}
	var result struct {
		MessageID int64 `json:"message_id"`
	}
	if err := json.Unmarshal(inbound.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	replyID, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "answer"})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != 1 || replyID != 1 {
		t.Fatalf("independent inbound/reply ids=%d/%d, want 1/1", result.MessageID, replyID)
	}
}

func TestEventsReplayStaleHeartbeatTypingAndEdit(t *testing.T) {
	c, _, cookie := newHandlerChannel()
	first, _ := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "first"})
	c.publish(streamEvent{kind: "status", payload: streamPayload{Text: "quiet"}})
	_ = c.SendTyping(42, nil)
	_, _ = c.EditMessage(c3types.EditArgs{Channel: Name, ChatID: 42, MessageID: first, Text: "first edited"})
	_, _ = c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "second"})

	recorder, cancel, done := startSSE(c, cookie, "1", "")
	waitFor(t, func() bool {
		_, body := recorder.snapshot()
		return strings.Contains(body, "first edited") && strings.Contains(body, "second")
	})
	_, body := recorder.snapshot()
	if strings.Contains(body, "quiet") || strings.Contains(body, "event: typing") || strings.Contains(body, "history may be incomplete") {
		t.Fatalf("replay included non-replay event or false gap: %q", body)
	}
	cancel()
	<-done

	longRing := New()
	longRing.host = c.host
	longRing.operatorID = 42
	longRing.allowedOrigin = c.allowedOrigin
	longRing.sessions[sessionKey(cookie.Value)] = &session{userID: 42, username: "operator", created: longRing.now(), lastSeen: longRing.now()}
	for index := 0; index < replayLimit+1; index++ {
		_, _ = longRing.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: fmt.Sprintf("ring-%d", index)})
	}
	oldRecorder, oldCancel, oldDone := startSSE(longRing, cookie, "1", "")
	waitFor(t, func() bool {
		_, body := oldRecorder.snapshot()
		return strings.Contains(body, "history may be incomplete")
	})
	oldCancel()
	<-oldDone
	freshRecorder, freshCancel, freshDone := startSSE(longRing, cookie, "", "")
	waitFor(t, func() bool {
		_, body := freshRecorder.snapshot()
		return strings.Contains(body, "history may be incomplete") && strings.Contains(body, "ring-200")
	})
	freshCancel()
	<-freshDone

	empty := New()
	empty.host = c.host
	empty.operatorID = 42
	empty.allowedOrigin = c.allowedOrigin
	empty.sessions[sessionKey(cookie.Value)] = &session{userID: 42, username: "operator", created: empty.now(), lastSeen: empty.now()}
	staleRecorder, staleCancel, staleDone := startSSE(empty, cookie, "7", "")
	waitFor(t, func() bool {
		_, body := staleRecorder.snapshot()
		return strings.Contains(body, "history may be incomplete")
	})
	staleCancel()
	<-staleDone

	live := New()
	live.host = c.host
	live.operatorID = 42
	live.allowedOrigin = c.allowedOrigin
	live.heartbeatInterval = 2 * time.Millisecond
	live.sessions[sessionKey(cookie.Value)] = &session{userID: 42, username: "operator", created: live.now(), lastSeen: live.now()}
	heartbeat, heartbeatCancel, heartbeatDone := startSSE(live, cookie, "", "")
	waitFor(t, func() bool {
		_, body := heartbeat.snapshot()
		return strings.Contains(body, ":hb\n\n")
	})
	_ = live.SendTyping(42, nil)
	_, _ = live.EditMessage(c3types.EditArgs{Channel: Name, ChatID: 42, MessageID: 9, Text: "live edit"})
	waitFor(t, func() bool {
		_, body := heartbeat.snapshot()
		return strings.Contains(body, "event: typing") && strings.Contains(body, "live edit")
	})
	heartbeatCancel()
	<-heartbeatDone
}

func TestLogoutDestroysSessionAndClosesSSE(t *testing.T) {
	c, _, cookie := newHandlerChannel()
	_, cancel, done := startSSE(c, cookie, "", "")
	defer cancel()
	waitFor(t, func() bool { return streamCount(c) == 1 })
	logout := httptest.NewRecorder()
	c.routes().ServeHTTP(logout, request(http.MethodPost, "/logout", "", cookie, true))
	if logout.Code != http.StatusNoContent {
		t.Fatalf("logout status=%d", logout.Code)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("logout did not close SSE")
	}
	if c.HasLiveSession(42) {
		t.Fatal("logout left a live session")
	}
}

func TestUnsupportedMethods(t *testing.T) {
	c, _, _ := newHandlerChannel()
	if _, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Poll: &c3types.PollSpec{Question: "x"}}); err == nil {
		t.Fatal("poll reply unexpectedly supported")
	}
	if err := c.React(c3types.ReactArgs{}); err == nil {
		t.Fatal("React unexpectedly supported")
	}
	if _, err := c.DownloadAttachment("x"); err == nil {
		t.Fatal("DownloadAttachment unexpectedly supported")
	}
	if _, err := c.StopPoll(42, 1); err == nil {
		t.Fatal("StopPoll unexpectedly supported")
	}
	if _, err := c.CreateTopic(42, "x"); err == nil {
		t.Fatal("CreateTopic unexpectedly supported")
	}
	if err := c.ValidateTopic(42, 1); err == nil {
		t.Fatal("ValidateTopic unexpectedly supported")
	}
}

type fakeListener struct {
	address net.Addr
	done    chan struct{}
	once    sync.Once
}

func newFakeListener() *fakeListener {
	return &fakeListener{address: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 43871}, done: make(chan struct{})}
}
func (listener *fakeListener) Accept() (net.Conn, error) {
	<-listener.done
	return nil, net.ErrClosed
}
func (listener *fakeListener) Close() error {
	listener.once.Do(func() { close(listener.done) })
	return nil
}
func (listener *fakeListener) Addr() net.Addr { return listener.address }

func startWithFakeListener(t *testing.T, config Config, mutateHost func(*fakeHost)) (*Channel, *fakeHost, string) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	host := &fakeHost{
		webConfig: config, telegram: telegramConfig{MasterUserID: 42, DMChatID: 42},
		registered: true, allowed: true,
	}
	if mutateHost != nil {
		mutateHost(host)
	}
	c := New()
	var listened string
	c.listenFunc = func(_, address string) (net.Listener, error) {
		listened = address
		return newFakeListener(), nil
	}
	if err := c.Start(context.Background(), host); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop() })
	return c, host, listened
}

func TestStartListenerWarningsValidationAndSeed(t *testing.T) {
	t.Run("default loopback and held seed", func(t *testing.T) {
		c, host, listened := startWithFakeListener(t, Config{}, func(host *fakeHost) { host.queuedMax = 77 })
		if listened != defaultListen || strings.Contains(host.logText(), "non-loopback") {
			t.Fatalf("listen/log=%q/%q", listened, host.logText())
		}
		if got := c.nextInboundMessageID(); got != 78 {
			t.Fatalf("seeded next id=%d, want 78", got)
		}
	})

	t.Run("non-loopback and public host warn", func(t *testing.T) {
		_, host, listened := startWithFakeListener(t, Config{Listen: "192.0.2.10:9000", PublicURL: "https://chat.example.com"}, nil)
		logs := host.logText()
		if listened != "192.0.2.10:9000" || !strings.Contains(logs, "non-loopback") || !strings.Contains(logs, "neither loopback nor *.ts.net") {
			t.Fatalf("listen/logs=%q/%q", listened, logs)
		}
	})

	t.Run("missing telegram DM warns but keeps serving", func(t *testing.T) {
		_, host, _ := startWithFakeListener(t, Config{}, func(host *fakeHost) {
			host.telegram.DMChatID = 0
		})
		if !strings.Contains(host.logText(), "WARNING channels.telegram.dm_chat_id is 0; login links cannot be delivered") {
			t.Fatalf("missing startup warning: %q", host.logText())
		}
	})

	t.Run("public URL origin matching ignores scheme and host case", func(t *testing.T) {
		c, _, _ := startWithFakeListener(t, Config{PublicURL: "https://CHAT.EXAMPLE:8443"}, nil)
		r := request(http.MethodPost, "/send", "", nil, false)
		r.Header.Set("Origin", "HTTPS://chat.example:8443")
		if !c.sameOrigin(r) {
			t.Fatal("uppercase public_url and request origin did not match")
		}
		if got := normalizeOrigin("HTTPS://CHAT.EXAMPLE:8443/Path"); got != "https://chat.example:8443/Path" {
			t.Fatalf("normalized origin=%q", got)
		}
	})

	for _, config := range []Config{
		{PublicURL: "http://device.example"},
		{PublicURL: "https://device.example/path"},
		{PublicURL: "https://device.example:70000"},
	} {
		t.Run("invalid public URL "+config.PublicURL, func(t *testing.T) {
			host := &fakeHost{webConfig: config, telegram: telegramConfig{MasterUserID: 42, DMChatID: 42}, registered: true, allowed: true}
			c := New()
			c.listenFunc = func(_, _ string) (net.Listener, error) { t.Fatal("invalid config reached listen"); return nil, nil }
			if err := c.Start(context.Background(), host); err == nil {
				t.Fatal("invalid public URL was accepted")
			}
		})
	}
}

func TestStartRefusesMissingTrustPrerequisites(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fakeHost)
		want   string
	}{
		{"telegram not registered", func(host *fakeHost) { host.registered = false }, "telegram must be registered"},
		{"operator zero", func(host *fakeHost) { host.telegram.MasterUserID = 0 }, "master_user_id is required"},
		{"operator not allowlisted", func(host *fakeHost) { host.allowed = false }, "not allowlisted"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("XDG_STATE_HOME", t.TempDir())
			host := &fakeHost{telegram: telegramConfig{MasterUserID: 42, DMChatID: 42}, registered: true, allowed: true}
			test.mutate(host)
			c := New()
			c.listenFunc = func(_, _ string) (net.Listener, error) { t.Fatal("refused Start reached listen"); return nil, nil }
			err := c.Start(context.Background(), host)
			if err == nil || !strings.Contains(err.Error(), test.want) || !strings.Contains(host.logText(), test.want) {
				t.Fatalf("Start error/log=%v/%q", err, host.logText())
			}
		})
	}
}

func TestLoginLinkEndpointDebounceAndNoClientIdentity(t *testing.T) {
	c, host, _ := newHandlerChannel()
	first := httptest.NewRecorder()
	c.routes().ServeHTTP(first, request(http.MethodPost, "/login/link", "", nil, true))
	second := httptest.NewRecorder()
	c.routes().ServeHTTP(second, request(http.MethodPost, "/login/link", "", nil, true))
	supplied := httptest.NewRecorder()
	c.routes().ServeHTTP(supplied, request(http.MethodPost, "/login/link", `{"user_id":99}`, nil, true))
	if first.Code != http.StatusAccepted || second.Code != http.StatusTooManyRequests || supplied.Code != http.StatusBadRequest {
		t.Fatalf("link statuses=%d/%d/%d", first.Code, second.Code, supplied.Code)
	}
	host.mu.Lock()
	deliveries := host.loginDeliveries
	host.mu.Unlock()
	if deliveries != 1 {
		t.Fatalf("login deliveries=%d, want 1", deliveries)
	}
}

func TestLoginLinkEndpointHourlyLimit(t *testing.T) {
	c, host, _ := newHandlerChannel()
	base := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	now := base
	c.now = func() time.Time { return now }
	for attempt := 0; attempt < loginLinkHourlyLimit; attempt++ {
		response := httptest.NewRecorder()
		c.routes().ServeHTTP(response, request(http.MethodPost, "/login/link", "", nil, true))
		if response.Code != http.StatusAccepted {
			t.Fatalf("attempt %d status=%d", attempt+1, response.Code)
		}
		now = now.Add(loginLinkDebounce)
	}
	limited := httptest.NewRecorder()
	c.routes().ServeHTTP(limited, request(http.MethodPost, "/login/link", "", nil, true))
	if limited.Code != http.StatusTooManyRequests {
		t.Fatalf("sixth request status=%d", limited.Code)
	}
	host.mu.Lock()
	deliveries := host.loginDeliveries
	host.mu.Unlock()
	if deliveries != loginLinkHourlyLimit {
		t.Fatalf("deliveries=%d", deliveries)
	}
}

func TestEmbeddedPagesAreSelfContainedAndUseTextContent(t *testing.T) {
	for _, name := range []string{"page.html", "login.html", "auth.html"} {
		contents, err := pages.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		text := string(contents)
		for _, unsafe := range []string{"inner" + "HTML", "outer" + "HTML", "insertAdjacent" + "HTML", "document." + "write"} {
			if strings.Contains(text, unsafe) {
				t.Fatalf("%s contains unsafe DOM sink %q", name, unsafe)
			}
		}
		if strings.Contains(text, "http://") || strings.Contains(text, "https://") {
			t.Fatalf("%s contains an unsafe DOM sink or external resource", name)
		}
		if !strings.Contains(text, "textContent") {
			t.Fatalf("%s does not render copy with textContent", name)
		}
		if strings.Contains(text, "\t") {
			t.Fatalf("%s inline JavaScript contains a tab", name)
		}
	}
	page, err := pages.ReadFile("page.html")
	if err != nil {
		t.Fatal(err)
	}
	pageText := string(page)
	const allowlist = "const allowedSchemes = ['http:', 'https:', 'mailto:', 'tel:'];"
	if !strings.Contains(pageText, allowlist) {
		t.Fatal("page does not contain the exact safe link-scheme allowlist")
	}
	for _, scheme := range []string{"javascript:", "data:", "vbscript:"} {
		if strings.Contains(allowlist, scheme) {
			t.Fatalf("unsafe scheme %q appears in renderer href allowlist", scheme)
		}
	}
	for _, marker := range []string{
		"return 'mine:' + id", "return 'agent:' + id", "return 'status:' + id",
		"createElement('table')", "createElement('blockquote')", "createElement('span')",
		"setAttribute('aria-label', 'spoiler')", "console.error('web: markdown render failed'",
		"let detached = false", "detached ? 'no session attached — reconnected'", "detached = false",
	} {
		if !strings.Contains(pageText, marker) {
			t.Fatalf("page is missing renderer/state marker %q", marker)
		}
	}
	if got := strings.Count(pageText, "markConnected();"); got != 3 {
		t.Fatalf("page marks %d event kinds connected, want message/typing/edit only", got)
	}
}

func TestPageHeadersDenyFraming(t *testing.T) {
	w := httptest.NewRecorder()
	setPageHeaders(w)
	if got := w.Header().Get("X-Frame-Options"); got != "DENY" {
		t.Fatalf("X-Frame-Options=%q, want DENY", got)
	}
}

func TestCapabilities(t *testing.T) {
	caps := New().Capabilities()
	if caps.Channel != Name || !caps.RichText || !caps.RichTables || caps.MaxMessageRunes != maxMessageRunes || caps.MaxMessageRunesSource != 0 || !caps.Typing || !caps.EditMessages || caps.InlineKeyboards || caps.Polls || caps.Reactions || len(caps.MediaKinds) != 0 {
		t.Fatalf("capabilities=%+v", caps)
	}
}

var _ io.Writer = (*streamRecorder)(nil)
