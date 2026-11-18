package web

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/mappings"
)

const (
	frontOrigin   = "https://boards.example.test"
	frontAddress  = "192.0.2.10"
	frontHeader   = "X-Front-Identity"
	frontOperator = "owner@example.test"
)

// newFrontTest is a started board channel behind a configured front, with one
// board and one C3 login (which must never count on the front).
func newFrontTest(t *testing.T) (*Channel, *fakeBoardHost, *http.Cookie) {
	t.Helper()
	c, host, cookie := newBoardTest(t)
	c.cfg.Front = mappings.WebFront{Origin: frontOrigin, From: frontAddress, IdentityHeader: frontHeader, Operator: frontOperator}
	c.allowedOrigin[frontOrigin] = true
	c.interfaceAddresses = func() ([]net.Addr, error) { return nil, nil } // from is on another host
	createBoard(t, c, "plan")
	return c, host, cookie
}

// useLocalFrom points the front at a local from, as Start would see it.
func useLocalFrom(c *Channel) {
	c.cfg.Front.From = "127.0.0.1"
}

// frontRequest is what the front forwards: its Host, its address as the TCP
// peer unless peer says otherwise, and the identity it verified.
func frontRequest(method, path, body, peer string, identities ...string) *http.Request {
	r := request(method, path, body, nil, false)
	r.Host = "boards.example.test"
	r.RemoteAddr = net.JoinHostPort(peer, "43210")
	r.Header.Set("Origin", frontOrigin)
	for _, identity := range identities {
		r.Header.Add(frontHeader, identity)
	}
	return r
}

func serve(c *Channel, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	c.routes().ServeHTTP(w, r)
	return w
}

// TestFrontVouchedOperatorReachesEveryRouteAndAnyoneElseGetsOne403: on the
// front's Host the operator the front vouches for reaches the web chat and
// boards alike; any other request, a C3 login included, gets the same 403 on
// every path, never a login page or the fresh-link endpoint.
func TestFrontVouchedOperatorReachesEveryRouteAndAnyoneElseGetsOne403(t *testing.T) {
	c, host, cookie := newFrontTest(t)
	for _, path := range []string{"/", "/sw.js", "/manifest.webmanifest", "/board", "/board/plan", "/board/plan/state", "/board/assets/board.js"} {
		if w := serve(c, frontRequest(http.MethodGet, path, "", frontAddress, frontOperator)); w.Code != http.StatusOK {
			t.Fatalf("front operator GET %s=%d, want 200", path, w.Code)
		}
	}
	chat, _ := pages.ReadFile("page.html")
	if w := serve(c, frontRequest(http.MethodGet, "/", "", frontAddress, frontOperator)); w.Body.String() != string(chat) {
		t.Fatal("front operator GET / is not the web chat page")
	}
	requesters := map[string]func(method, path string) *http.Request{
		"C3 login, no identity": func(method, path string) *http.Request {
			r := frontRequest(method, path, "", frontAddress)
			r.AddCookie(cookie)
			return r
		},
		"another identity": func(method, path string) *http.Request {
			return frontRequest(method, path, "", frontAddress, "guest@example.test")
		},
		"two identities": func(method, path string) *http.Request {
			return frontRequest(method, path, "", frontAddress, frontOperator, frontOperator)
		},
		"another peer": func(method, path string) *http.Request {
			return frontRequest(method, path, "", "192.0.2.99", frontOperator)
		},
	}
	for name, requester := range requesters {
		for _, route := range []struct{ method, path string }{
			{http.MethodGet, "/"}, {http.MethodGet, "/login"}, {http.MethodPost, "/login/link"}, {http.MethodGet, "/auth"},
			{http.MethodPost, "/auth"}, {http.MethodPost, "/logout"}, {http.MethodPost, "/send"}, {http.MethodGet, "/events"},
			{http.MethodPost, "/voice"}, {http.MethodPost, "/voice-note"}, {http.MethodGet, "/audio/unlock"},
			{http.MethodGet, "/files/00112233445566778899aabbccddeeff"}, {http.MethodGet, "/healthz"}, {http.MethodGet, "/sw.js"},
			{http.MethodGet, "/no-such-route"}, {http.MethodGet, "/board/plan"}, {http.MethodGet, "/board/plan/state"},
		} {
			w := serve(c, requester(route.method, route.path))
			if w.Code != http.StatusForbidden || w.Body.String() != "{\"error\":\"forbidden\"}\n" || w.Header().Get("Set-Cookie") != "" {
				t.Fatalf("%s: front %s %s=%d %q, want the uniform 403", name, route.method, route.path, w.Code, w.Body.String())
			}
		}
	}
	if host.loginDeliveries != 0 {
		t.Fatalf("a refused front request sent %d login links", host.loginDeliveries)
	}
	if board, _ := c.board.BoardByName("plan"); board.Link != frontOrigin+"/board/plan" {
		t.Fatalf("link %q, want the front's", board.Link)
	}
	w := serve(c, frontRequest(http.MethodGet, "/board/plan", "", frontAddress, frontOperator))
	if !strings.HasSuffix(w.Header().Get("Content-Security-Policy"), "; frame-src "+frontOrigin+"/board/plan/page") {
		t.Fatalf("front shell frame-src: %q", w.Header().Get("Content-Security-Policy"))
	}
}

// TestOffTheFrontOnlyTheC3LoginSignsIn: off the front the identity header
// never counts, for the web chat as for boards; C3's own login does.
func TestOffTheFrontOnlyTheC3LoginSignsIn(t *testing.T) {
	c, host, cookie := newFrontTest(t)
	withHeader := func(method, path, body string, cookie *http.Cookie) *http.Request {
		r := request(method, path, body, cookie, true)
		r.Header.Set(frontHeader, frontOperator)
		return r
	}
	if w := serve(c, withHeader(http.MethodGet, "/", "", nil)); w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/login" {
		t.Fatalf("identity header off the front GET /=%d %q, want the C3 login", w.Code, w.Header().Get("Location"))
	}
	if w := serve(c, withHeader(http.MethodGet, "/login", "", nil)); w.Code != http.StatusOK {
		t.Fatalf("off the front GET /login=%d, want the login page", w.Code)
	}
	if w := serve(c, withHeader(http.MethodPost, "/send", `{"text":"header only","client_id":"h"}`, nil)); w.Code != http.StatusUnauthorized {
		t.Fatalf("identity header off the front POST /send=%d, want 401", w.Code)
	}
	if w := serve(c, withHeader(http.MethodPost, "/send", `{"text":"signed in","client_id":"h"}`, cookie)); w.Code != http.StatusAccepted {
		t.Fatalf("C3 login off the front POST /send=%d, want 202", w.Code)
	}
	if emitted := host.emittedSnapshot(); len(emitted) != 1 || emitted[0].Text != "signed in" {
		t.Fatalf("emitted %+v, want only the signed-in send", emitted)
	}
}

// TestFrontViewerChatStateNeverSignsInOffTheFront: the front's operator gets
// the web chat's per-viewer state (client ids, stream, presence, spoken
// replies, audio, documents, voice notes) under the front viewer id, which no
// cookie hashes to and which is never persisted; C3's login state is untouched.
func TestFrontViewerChatStateNeverSignsInOffTheFront(t *testing.T) {
	c, host, cookie := newFrontTest(t)
	audio := []byte("ID3-front-audio")
	host.synthesize = func(context.Context, c3types.SpeechRequest) (c3types.SpeechResult, error) {
		return c3types.SpeechResult{Audio: audio, MIME: "audio/mpeg", Provider: "fake"}, nil
	}
	front := func(method, path, body string) *httptest.ResponseRecorder {
		r := frontRequest(method, path, body, frontAddress, frontOperator)
		r.AddCookie(cookie) // ignored on the front: the state must land on the front viewer
		return serve(c, r)
	}

	send := front(http.MethodPost, "/send", `{"text":"from the front","client_id":"f1"}`)
	retry := front(http.MethodPost, "/send", `{"text":"from the front","client_id":"f1"}`)
	emitted := host.emittedSnapshot()
	if send.Code != http.StatusAccepted || retry.Code != http.StatusAccepted || send.Body.String() != retry.Body.String() ||
		len(emitted) != 1 || emitted[0].Sender.UserID != 42 || emitted[0].Sender.Username != "operator" {
		t.Fatalf("front send=%d %s retry=%d %s emitted %+v", send.Code, send.Body, retry.Code, retry.Body, emitted)
	}
	if conflict := front(http.MethodPost, "/send", `{"text":"changed","client_id":"f1"}`); conflict.Code != http.StatusConflict {
		t.Fatalf("front client id reuse=%d, want 409", conflict.Code)
	}
	c.clientMu.Lock()
	_, isFrontKey := c.clientMessages[clientMessageKey{sessionID: c.frontViewerID(), clientID: "f1"}]
	c.clientMu.Unlock()
	if !isFrontKey {
		t.Fatal("the front send's client id is not keyed by the front viewer")
	}

	streamContext, cancel := context.WithCancel(context.Background())
	stream := newStreamRecorder()
	done := make(chan struct{})
	go func() {
		c.routes().ServeHTTP(stream, frontRequest(http.MethodGet, "/events", "", frontAddress, frontOperator).WithContext(streamContext))
		close(done)
	}()
	defer func() { cancel(); <-done }()
	waitFor(t, func() bool { _, body := stream.snapshot(); return strings.Contains(body, "event: presence") })
	if voice := front(http.MethodPost, "/voice", `{"enabled":true}`); voice.Code != http.StatusNoContent {
		t.Fatalf("front voice preference=%d %s", voice.Code, voice.Body)
	}
	waitFor(t, func() bool { _, body := stream.snapshot(); return strings.Contains(body, `"voice":true`) })
	c.authMu.Lock()
	isCookieVoice := c.sessions[sessionKey(cookie.Value)].voice
	sessionCount := len(c.sessions)
	c.authMu.Unlock()
	if isCookieVoice || sessionCount != 1 {
		t.Fatalf("the front changed C3 login state: cookie voice %v, sessions %d", isCookieVoice, sessionCount)
	}
	if _, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "spoken answer"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, body := stream.snapshot(); return strings.Contains(body, "event: audio") })
	_, body := stream.snapshot()
	audioURL := regexp.MustCompile(`"url":"(/audio/[^"]+)"`).FindStringSubmatch(body)
	if len(audioURL) != 2 {
		t.Fatalf("no audio URL in the front stream: %q", body)
	}
	if w := front(http.MethodGet, audioURL[1], ""); w.Code != http.StatusOK || w.Body.String() != string(audio) {
		t.Fatalf("front audio=%d %q", w.Code, w.Body.String())
	}
	if w := front(http.MethodGet, "/audio/unlock", ""); w.Code != http.StatusOK {
		t.Fatalf("front audio unlock=%d", w.Code)
	}
	source := writeDocumentSource(t, t.TempDir(), "report.html", "<!doctype html><title>report</title>")
	token, _, err := c.storeDocument(source, "report.html")
	if err != nil {
		t.Fatal(err)
	}
	if w := front(http.MethodGet, "/files/"+token, ""); w.Code != http.StatusOK {
		t.Fatalf("front document=%d", w.Code)
	}

	forged := &http.Cookie{Name: sessionCookieName, Value: c.frontViewerID()}
	for name, cookie := range map[string]*http.Cookie{"no cookie": nil, "the front viewer id as a cookie": forged} {
		for _, path := range []string{audioURL[1], "/files/" + token} {
			if w := serve(c, request(http.MethodGet, path, "", cookie, false)); w.Code != http.StatusUnauthorized {
				t.Fatalf("off the front, %s, GET %s=%d, want 401", name, path, w.Code)
			}
		}
		if w := serve(c, request(http.MethodPost, "/send", `{"text":"x","client_id":"f1"}`, cookie, true)); w.Code != http.StatusUnauthorized {
			t.Fatalf("off the front, %s, POST /send=%d, want 401", name, w.Code)
		}
	}

	t.Run("voice note", func(t *testing.T) {
		if _, err := exec.LookPath("ffmpeg"); err != nil {
			t.Skip("ffmpeg not available")
		}
		r := frontRequest(http.MethodPost, "/voice-note", string(sineWAV(t, time.Second)), frontAddress, frontOperator)
		r.Header.Set("Content-Type", "audio/wav")
		r.Header.Set("X-Client-Id", "front-voice")
		w := serve(c, r)
		rows := host.emittedSnapshot()
		last := rows[len(rows)-1]
		if w.Code != http.StatusAccepted || len(last.Attachments) != 1 || last.Attachments[0].Kind != "voice" || last.Sender.UserID != 42 {
			t.Fatalf("front voice note=%d %s, last emitted %+v", w.Code, w.Body, last)
		}
	})

	// The front's client ids sit beside C3's login in memory; reloading the
	// saved state must keep that login, not reject the file as corrupt.
	if err := c.loadState(host); err != nil {
		t.Fatal(err)
	}
	if w := serve(c, request(http.MethodGet, "/", "", cookie, false)); w.Code != http.StatusOK {
		t.Fatalf("C3 login after a state reload=%d, want 200", w.Code)
	}
}

// TestFrontKeepsTheSameOriginRules: a vouched request still needs same-origin
// evidence for a POST, and the stream refuses a foreign Origin.
func TestFrontKeepsTheSameOriginRules(t *testing.T) {
	c, _, _ := newFrontTest(t)
	post := func(path, body, origin, fetchSite string) int {
		r := frontRequest(http.MethodPost, path, body, frontAddress, frontOperator)
		r.Header.Del("Origin")
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		if fetchSite != "" {
			r.Header.Set("Sec-Fetch-Site", fetchSite)
		}
		return serve(c, r).Code
	}
	const foreign = "https://elsewhere.example.test"
	for _, path := range []string{"/send", "/voice", "/speak", "/voice-note"} {
		if code := post(path, `{}`, foreign, ""); code != http.StatusForbidden {
			t.Fatalf("front POST %s from a foreign origin=%d, want 403", path, code)
		}
		if code := post(path, `{}`, "", ""); code != http.StatusForbidden {
			t.Fatalf("front POST %s without origin evidence=%d, want 403", path, code)
		}
	}
	if code := post("/send", `{"text":"same site","client_id":"s1"}`, "", "same-origin"); code != http.StatusAccepted {
		t.Fatalf("front POST /send with Sec-Fetch-Site=%d, want 202", code)
	}
	if code := post("/send", `{"text":"front origin","client_id":"s2"}`, frontOrigin, ""); code != http.StatusAccepted {
		t.Fatalf("front POST /send from the front origin=%d, want 202", code)
	}
	events := frontRequest(http.MethodGet, "/events", "", frontAddress, frontOperator)
	events.Header.Set("Origin", foreign)
	if w := serve(c, events); w.Code != http.StatusForbidden {
		t.Fatalf("front stream from a foreign origin=%d, want 403", w.Code)
	}
}

// TestFrontHasNoC3SignIn: on the front the operator's sign-in pages lead to
// the chat, and C3's token exchange, fresh link, logout and health check don't
// exist; the Telegram login link still opens off the front.
func TestFrontHasNoC3SignIn(t *testing.T) {
	c, host, cookie := newFrontTest(t)
	for _, path := range []string{"/login", "/auth"} {
		if w := serve(c, frontRequest(http.MethodGet, path, "", frontAddress, frontOperator)); w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/" {
			t.Fatalf("front GET %s=%d %q, want 303 to /", path, w.Code, w.Header().Get("Location"))
		}
	}
	link, err := c.MintLoginLink(42)
	if err != nil || !strings.HasPrefix(link, c.baseURL+"/auth#") {
		t.Fatalf("login link %q %v, want it off the front", link, err)
	}
	token := tokenFromLink(t, link)
	if w := serve(c, frontRequest(http.MethodPost, "/auth", `{"token":"`+token+`"}`, frontAddress, frontOperator)); w.Code != http.StatusNotFound || w.Header().Get("Set-Cookie") != "" {
		t.Fatalf("front POST /auth=%d cookie %q, want 404 and no cookie", w.Code, w.Header().Get("Set-Cookie"))
	}
	if w := postAuth(c, token); w.Code != http.StatusSeeOther || w.Header().Get("Set-Cookie") == "" {
		t.Fatalf("the link off the front after a front try=%d, want it unspent", w.Code)
	}
	if w := serve(c, frontRequest(http.MethodPost, "/login/link", "", frontAddress, frontOperator)); w.Code != http.StatusNotFound || host.loginDeliveries != 0 {
		t.Fatalf("front fresh link=%d, deliveries %d; want 404 and none", w.Code, host.loginDeliveries)
	}
	logout := frontRequest(http.MethodPost, "/logout", "", frontAddress, frontOperator)
	logout.AddCookie(cookie)
	if w := serve(c, logout); w.Code != http.StatusNotFound {
		t.Fatalf("front logout=%d, want 404", w.Code)
	}
	if w := serve(c, request(http.MethodGet, "/", "", cookie, false)); w.Code != http.StatusOK {
		t.Fatalf("C3 login after a front logout=%d, want it kept", w.Code)
	}
	if w := serve(c, frontRequest(http.MethodGet, "/healthz", "", frontAddress, frontOperator)); w.Code != http.StatusNotFound {
		t.Fatalf("front /healthz=%d, want 404", w.Code)
	}
	if w := serve(c, request(http.MethodGet, "/healthz", "", nil, false)); w.Code != http.StatusOK {
		t.Fatalf("direct /healthz=%d: the front rule leaked off the front", w.Code)
	}
}

func TestFrontIdentityCountsOnlyFromTheFrontAddress(t *testing.T) {
	c, host, cookie := newFrontTest(t)
	for name, r := range map[string]*http.Request{
		"another tailnet peer": frontRequest(http.MethodGet, "/board/plan/state", "", "192.0.2.99", frontOperator),
		"wrong identity":       frontRequest(http.MethodGet, "/board/plan/state", "", frontAddress, "Owner@example.test"),
		"two identities":       frontRequest(http.MethodGet, "/board/plan/state", "", frontAddress, frontOperator, frontOperator),
		"no identity":          frontRequest(http.MethodGet, "/board/plan/state", "", frontAddress),
	} {
		if w := serve(c, r); w.Code != http.StatusForbidden {
			t.Fatalf("%s=%d, want 403", name, w.Code)
		}
	}
	withCookie := frontRequest(http.MethodGet, "/board/plan/state", "", frontAddress)
	withCookie.AddCookie(cookie)
	if w := serve(c, withCookie); w.Code != http.StatusForbidden {
		t.Fatalf("C3 login on the front=%d, want 403 (only the front's identity counts there)", w.Code)
	}
	direct := request(http.MethodGet, "/board/plan/state", "", nil, false)
	direct.RemoteAddr = "192.0.2.99:43210"
	direct.Header.Set(frontHeader, frontOperator)
	if w := serve(c, direct); w.Code != http.StatusUnauthorized {
		t.Fatalf("identity header off the front=%d, want 401", w.Code)
	}

	state := serve(c, frontRequest(http.MethodGet, "/board/plan/state", "", frontAddress, frontOperator))
	var view boardStateView
	if state.Code != http.StatusOK || json.Unmarshal(state.Body.Bytes(), &view) != nil || view.Viewer["role"] != "owner" {
		t.Fatalf("front operator state=%d %s", state.Code, state.Body.String())
	}
	frontPage := "/board/plan/page?cap=" + view.Cap
	if page := serve(c, frontRequest(http.MethodGet, frontPage, "", frontAddress, frontOperator)); page.Code != http.StatusOK {
		t.Fatalf("the front viewer's cap on the front=%d, want 200 without any cookie", page.Code)
	}
	for name, r := range map[string]*http.Request{
		"off the front":            request(http.MethodGet, frontPage, "", nil, false),
		"front Host, another peer": frontRequest(http.MethodGet, frontPage, "", "192.0.2.99", frontOperator),
		"front, another identity":  frontRequest(http.MethodGet, frontPage, "", frontAddress, "guest@example.test"),
	} {
		if w := serve(c, r); w.Code != http.StatusForbidden {
			t.Fatalf("the front viewer's cap %s=%d, want 403", name, w.Code)
		}
	}
	sessionCap := boardStateOf(t, c, cookie, "plan").Cap
	if w := serve(c, frontRequest(http.MethodGet, "/board/plan/page?cap="+sessionCap, "", frontAddress, frontOperator)); w.Code != http.StatusForbidden {
		t.Fatalf("a C3 login's cap on the front=%d, want 403", w.Code)
	}
	if w := getPage(c, "cap="+sessionCap); w.Code != http.StatusOK {
		t.Fatalf("a C3 login's cap off the front=%d, want 200", w.Code)
	}
	act := serve(c, frontRequest(http.MethodPost, "/board/plan/act", `{"client_id":"f","op":"comment","part":"1","text":"from the phone"}`, frontAddress, frontOperator))
	emitted := host.emittedSnapshot()
	if act.Code != http.StatusAccepted || len(emitted) != 1 || emitted[0].Sender.UserID != 42 || emitted[0].Sender.Username != "operator" {
		t.Fatalf("front act=%d %s emitted %+v", act.Code, act.Body.String(), emitted)
	}
}

// TestFrontAddressReachesNothingUnderAnotherName: the front also proxies this
// port under other names; a request from the front's address under any Host
// but the front origin reaches no route, so the web chat stays off the front.
// A loopback front address can't be told from a local client: there the Host
// alone decides (a known residual).
func TestFrontAddressReachesNothingUnderAnotherName(t *testing.T) {
	c, _, cookie := newFrontTest(t)
	alias := func(method, path string) *http.Request {
		r := frontRequest(method, path, "", frontAddress, frontOperator)
		r.Host = "laptop--8371.example.test"
		r.AddCookie(cookie)
		return r
	}
	for _, route := range []struct{ method, path string }{
		{http.MethodGet, "/"}, {http.MethodGet, "/login"}, {http.MethodGet, "/events"}, {http.MethodPost, "/send"},
		{http.MethodGet, "/healthz"}, {http.MethodGet, "/board"}, {http.MethodGet, "/board/plan"}, {http.MethodGet, "/board/plan/state"},
	} {
		if w := serve(c, alias(route.method, route.path)); w.Code != http.StatusNotFound {
			t.Fatalf("front address, alias Host, %s %s=%d, want 404", route.method, route.path, w.Code)
		}
	}
	if w := serve(c, frontRequest(http.MethodGet, "/board/plan/state", "", frontAddress, frontOperator)); w.Code != http.StatusOK {
		t.Fatalf("front origin board state=%d, want 200", w.Code)
	}
	if w := serve(c, request(http.MethodGet, "/", "", cookie, false)); w.Code != http.StatusOK {
		t.Fatalf("direct web chat from another peer=%d, want 200", w.Code)
	}

	useLocalFrom(c)
	local := request(http.MethodGet, "/", "", cookie, false)
	local.RemoteAddr = "127.0.0.1:43210"
	if w := serve(c, local); w.Code != http.StatusOK {
		t.Fatalf("loopback front address: local web chat=%d, want 200 (the Host decides)", w.Code)
	}
}

// TestFrontHostCountsInEverySpellingOfItsName: with a loopback front address
// the Host alone keeps a request on the front's rules (boards only), so each
// spelling of the front's own name a proxy may route as the front (case, one
// trailing dot, an empty or zero-padded default port, an IP literal's form)
// counts as the front, and so does its name with a port that can't be parsed.
// Another port does not.
func TestFrontHostCountsInEverySpellingOfItsName(t *testing.T) {
	c, _, cookie := newFrontTest(t)
	useLocalFrom(c)
	local := func(host, path string) *http.Request {
		r := request(http.MethodGet, path, "", cookie, false)
		r.RemoteAddr = "127.0.0.1:43210"
		r.Host = host
		return r
	}
	isFront := func(host string) bool {
		t.Helper()
		chat, state := serve(c, local(host, "/")).Code, serve(c, local(host, "/board/plan/state")).Code
		switch {
		case chat == http.StatusNotFound && state == http.StatusForbidden:
			return true
		case chat == http.StatusOK && state == http.StatusOK:
			return false
		}
		t.Fatalf("Host %q: / =%d, /board/plan/state =%d; want the front's 404/403 or the web chat's 200/200", host, chat, state)
		return false
	}
	for _, host := range []string{
		"boards.example.test", "BOARDS.Example.TEST", "boards.example.test.", "boards.example.test.:443",
		"boards.example.test:0443", "boards.example.test:", "boards.example.test:https", "boards.example.test:443:1",
	} {
		if !isFront(host) {
			t.Errorf("Host %q reached the web chat from the loopback front address; want the front's rules", host)
		}
	}
	for _, host := range []string{"boards.example.test:8443", "boards.example.test..", "laptop.example.test"} {
		if isFront(host) {
			t.Errorf("Host %q counted as the front", host)
		}
	}

	c.cfg.Front.Origin = "https://[2001:db8::10]"
	for _, host := range []string{"[2001:DB8::10]", "[2001:db8:0:0::10]:443", "[2001:db8::10]:"} {
		if !isFront(host) {
			t.Errorf("Host %q is the IPv6 front's own literal; want the front's rules", host)
		}
	}
}

func TestOnlyTheOperatorIdentityIsTheOwner(t *testing.T) {
	c, host, _ := newFrontTest(t)
	const guest = "guest@example.test"
	// The operator's frame capability, minted on the front: the guest's
	// request with it must be refused for who asks, not for a missing cap.
	var view boardStateView
	if state := serve(c, frontRequest(http.MethodGet, "/board/plan/state", "", frontAddress, frontOperator)); state.Code != http.StatusOK || json.Unmarshal(state.Body.Bytes(), &view) != nil || view.Cap == "" {
		t.Fatalf("operator state=%d %s", state.Code, state.Body.String())
	}
	operatorPage := "/board/plan/page?cap=" + view.Cap
	if w := serve(c, frontRequest(http.MethodGet, operatorPage, "", frontAddress, frontOperator)); w.Code != http.StatusOK {
		t.Fatalf("operator page with their cap=%d, want 200", w.Code)
	}
	for _, route := range []struct{ method, path, body string }{
		{http.MethodGet, "/board", ""}, {http.MethodGet, "/board/plan", ""}, {http.MethodGet, "/board/plan/state", ""},
		{http.MethodGet, "/board/plan/events", ""}, {http.MethodGet, operatorPage, ""},
		{http.MethodGet, "/board/assets/board.js", ""}, {http.MethodPost, "/board/plan/close", ""},
		{http.MethodPost, "/board/plan/act", `{"client_id":"g","op":"comment","part":"1","text":"let me in"}`},
	} {
		if w := serve(c, frontRequest(route.method, route.path, route.body, frontAddress, guest)); w.Code != http.StatusForbidden {
			t.Fatalf("guest %s %s=%d, want 403", route.method, route.path, w.Code)
		}
	}
	if len(host.emittedSnapshot()) != 0 || len(host.closed) != 0 {
		t.Fatalf("a guest reached the session: emitted %d, closed %v", len(host.emittedSnapshot()), host.closed)
	}
	if board, _ := c.board.BoardByName("plan"); board.IsClosed {
		t.Fatal("a guest closed the board")
	}
}

// startFrontChannel starts web through Start behind a front whose from is
// from, on a host whose interface list interfaceAddresses gives, with a board.
func startFrontChannel(t *testing.T, from string, interfaceAddresses func() ([]net.Addr, error)) (*Channel, *fakeBoardHost) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	host := newFakeBoardHost()
	host.webConfig.Front = mappings.WebFront{Origin: frontOrigin, From: from, IdentityHeader: frontHeader, Operator: frontOperator}
	c := New()
	c.listenFunc = func(_, _ string) (net.Listener, error) { return newFakeListener(), nil }
	c.interfaceAddresses = interfaceAddresses
	if err := c.Start(t.Context(), host); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop() })
	c.board.assets = boardFixtures
	if err := c.board.Start(t.Context(), host); err != nil {
		t.Fatal(err)
	}
	createBoard(t, c, "plan")
	return c, host
}

// TestFrontWithALocalFromServesBoardsOnly: a from that is loopback or one of
// this host's own addresses can't tell the front from any local process, the
// operator's own browser included, so there the front reaches board routes
// only and every other route is 404, as before D044 (an unreadable interface
// list counts as local). Only a from on another host opens the web chat, and
// only then does the channel name the front as where the chat is.
func TestFrontWithALocalFromServesBoardsOnly(t *testing.T) {
	interfaces := func(addresses ...string) func() ([]net.Addr, error) {
		return func() ([]net.Addr, error) {
			var list []net.Addr
			for _, address := range addresses {
				list = append(list, &net.IPNet{IP: net.ParseIP(address), Mask: net.CIDRMask(24, 32)})
			}
			return list, nil
		}
	}
	unreadable := func() ([]net.Addr, error) { return nil, errors.New("interfaces unreadable") }
	for _, setup := range []struct {
		name, from         string
		interfaceAddresses func() ([]net.Addr, error)
		isChatOnFront      bool
	}{
		{"loopback", "127.0.0.1", interfaces("198.51.100.7"), false},
		{"IPv6 loopback", "::1", interfaces("198.51.100.7"), false},
		{"this host's own address", frontAddress, interfaces("198.51.100.7", frontAddress), false},
		{"unreadable interface list", frontAddress, unreadable, false},
		{"another host", frontAddress, interfaces("198.51.100.7"), true},
	} {
		c, host := startFrontChannel(t, setup.from, setup.interfaceAddresses)
		operator := func(method, path, body string) int {
			return serve(c, frontRequest(method, path, body, setup.from, frontOperator)).Code
		}
		if state := operator(http.MethodGet, "/board/plan/state", ""); state != http.StatusOK {
			t.Fatalf("%s: front operator board state=%d, want 200", setup.name, state)
		}
		if guest := serve(c, frontRequest(http.MethodGet, "/board/plan/state", "", setup.from, "guest@example.test")); guest.Code != http.StatusForbidden {
			t.Fatalf("%s: front guest board state=%d, want 403", setup.name, guest.Code)
		}
		send := `{"text":"through the front","client_id":"f"}`
		chat, sent := operator(http.MethodGet, "/", ""), operator(http.MethodPost, "/send", send)
		if setup.isChatOnFront {
			if chat != http.StatusOK || sent != http.StatusAccepted || len(host.emittedSnapshot()) != 1 || c.ChatFrontOrigin() != frontOrigin {
				t.Fatalf("%s: front operator chat=%d send=%d emitted %d origin %q; want the web chat on the front", setup.name, chat, sent, len(host.emittedSnapshot()), c.ChatFrontOrigin())
			}
			continue
		}
		pastTheGate := httptest.NewRecorder()
		c.handleSend(pastTheGate, frontRequest(http.MethodPost, "/send", send, setup.from, frontOperator))
		if chat != http.StatusNotFound || sent != http.StatusNotFound || pastTheGate.Code != http.StatusNotFound || len(host.emittedSnapshot()) != 0 || c.ChatFrontOrigin() != "" {
			t.Fatalf("%s: front operator chat=%d send=%d (handler alone %d) emitted %d origin %q; want boards only", setup.name, chat, sent, pastTheGate.Code, len(host.emittedSnapshot()), c.ChatFrontOrigin())
		}
		ended, end := context.WithCancel(t.Context())
		end() // a stream that opens returns at once instead of hanging the test
		for _, route := range []struct{ method, path string }{
			{http.MethodGet, "/events"}, {http.MethodPost, "/voice"}, {http.MethodGet, "/audio/unlock"},
			{http.MethodGet, "/files/00112233445566778899aabbccddeeff"}, {http.MethodGet, "/login"}, {http.MethodGet, "/healthz"},
		} {
			r := frontRequest(route.method, route.path, "", setup.from, frontOperator).WithContext(ended)
			if code := serve(c, r).Code; code != http.StatusNotFound {
				t.Fatalf("%s: front operator %s %s=%d, want 404", setup.name, route.method, route.path, code)
			}
		}
	}
}

// TestFrontChatClosesWhenFromBecomesLocalAfterStart: whether from is local is
// judged on every request, so a from that becomes one of this host's own
// addresses after start (a tailnet address coming up late) closes the web chat
// on the front at once, and the channel stops naming the front as the chat.
func TestFrontChatClosesWhenFromBecomesLocalAfterStart(t *testing.T) {
	hostAddresses := []net.Addr{&net.IPNet{IP: net.ParseIP("198.51.100.7"), Mask: net.CIDRMask(24, 32)}}
	c, host := startFrontChannel(t, frontAddress, func() ([]net.Addr, error) { return hostAddresses, nil })
	send := func(clientID string) int {
		return serve(c, frontRequest(http.MethodPost, "/send", `{"text":"through the front","client_id":"`+clientID+`"}`, frontAddress, frontOperator)).Code
	}
	if code := send("before"); code != http.StatusAccepted || c.ChatFrontOrigin() != frontOrigin {
		t.Fatalf("from on another host: send=%d origin %q, want 202 and the front", code, c.ChatFrontOrigin())
	}
	hostAddresses = append(hostAddresses, &net.IPNet{IP: net.ParseIP(frontAddress), Mask: net.CIDRMask(24, 32)})
	if code := send("after"); code != http.StatusNotFound || c.ChatFrontOrigin() != "" || len(host.emittedSnapshot()) != 1 {
		t.Fatalf("from became local: send=%d origin %q emitted %d, want 404, no front, one message", code, c.ChatFrontOrigin(), len(host.emittedSnapshot()))
	}
	if state := serve(c, frontRequest(http.MethodGet, "/board/plan/state", "", frontAddress, frontOperator)).Code; state != http.StatusOK {
		t.Fatalf("from became local: board state=%d, want 200 (boards stay on the front)", state)
	}
}

func TestWebStartTakesTheFrontOriginAndRefusesABrokenFront(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	host := persistentHost()
	host.webConfig.Front = mappings.WebFront{Origin: frontOrigin, From: frontAddress, IdentityHeader: frontHeader, Operator: frontOperator}
	c := startPersistentChannel(t, host, nil)
	defer c.Stop()
	if !c.allowedOrigin[frontOrigin] || !c.isFrontRequest(frontRequest(http.MethodGet, "/board", "", frontAddress)) {
		t.Fatalf("started front: origin allowed %v", c.allowedOrigin[frontOrigin])
	}
	broken := persistentHost()
	broken.webConfig.Front = mappings.WebFront{Origin: frontOrigin, From: "not-an-address", IdentityHeader: frontHeader, Operator: frontOperator}
	if err := New().Start(t.Context(), broken); err == nil || !strings.Contains(err.Error(), "front.from") {
		t.Fatalf("Start with a broken front: %v", err)
	}
}
