package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	createBoard(t, c, "plan")
	return c, host, cookie
}

// frontRequest is what the front forwards: its Host, its address as the TCP
// peer unless peer says otherwise, and the identity it verified.
func frontRequest(method, path, body, peer string, identities ...string) *http.Request {
	r := request(method, path, body, nil, false)
	r.Host = "boards.example.test"
	r.RemoteAddr = peer + ":43210"
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

func TestFrontHostReachesOnlyBoardRoutes(t *testing.T) {
	c, _, cookie := newFrontTest(t)
	for _, path := range []string{"/board", "/board/plan", "/board/plan/state", "/board/assets/board.js"} {
		if w := serve(c, frontRequest(http.MethodGet, path, "", frontAddress, frontOperator)); w.Code != http.StatusOK {
			t.Fatalf("front operator GET %s=%d, want 200", path, w.Code)
		}
	}
	for _, route := range []struct{ method, path string }{
		{http.MethodGet, "/"}, {http.MethodGet, "/login"}, {http.MethodPost, "/login/link"}, {http.MethodGet, "/auth"},
		{http.MethodPost, "/send"}, {http.MethodGet, "/events"}, {http.MethodGet, "/files/00112233445566778899aabbccddeeff"},
		{http.MethodGet, "/healthz"}, {http.MethodGet, "/sw.js"}, {http.MethodGet, "/boards"}, {http.MethodGet, "/board/../send"},
	} {
		r := frontRequest(route.method, route.path, "", frontAddress, frontOperator)
		r.AddCookie(cookie)
		w := serve(c, r)
		if w.Code != http.StatusNotFound {
			t.Fatalf("front %s %s=%d, want it kept off the front", route.method, route.path, w.Code)
		}
	}
	if w := serve(c, request(http.MethodGet, "/healthz", "", nil, false)); w.Code != http.StatusOK {
		t.Fatalf("direct /healthz=%d: the front rule leaked off the front", w.Code)
	}
	if board, _ := c.board.BoardByName("plan"); board.Link != frontOrigin+"/board/plan" {
		t.Fatalf("link %q, want the front's", board.Link)
	}
	w := serve(c, frontRequest(http.MethodGet, "/board/plan", "", frontAddress, frontOperator))
	if !strings.HasSuffix(w.Header().Get("Content-Security-Policy"), "; frame-src "+frontOrigin+"/board/plan/page") {
		t.Fatalf("front shell frame-src: %q", w.Header().Get("Content-Security-Policy"))
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
	direct.RemoteAddr = frontAddress + ":43210"
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

func TestOnlyTheOperatorIdentityIsTheOwner(t *testing.T) {
	c, host, _ := newFrontTest(t)
	const guest = "guest@example.test"
	for _, route := range []struct{ method, path, body string }{
		{http.MethodGet, "/board", ""}, {http.MethodGet, "/board/plan", ""}, {http.MethodGet, "/board/plan/state", ""},
		{http.MethodGet, "/board/plan/events", ""}, {http.MethodGet, "/board/plan/page", ""},
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
