package web

import (
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/Andrometiq/c3/internal/c3types"
)

// The authenticated HTTPS front (channels.web.front) serves the web chat and
// boards, or boards only when its from is local. A request whose Host is the
// front's is the operator only when its TCP peer is the front's address and
// the identity header names the operator; C3's own login never counts there,
// and the header never counts elsewhere.

const frontViewerPrefix = "front:"

// boardViewer is who asks for a web-chat or board route (named for boards,
// which used it first). Its id keys the viewer's streams, client ids and frame
// capabilities: the web session key, a SHA-256 hex digest, or for the front
// frontViewerPrefix plus identity, which no cookie hashes to.
type boardViewer struct {
	id      string
	sender  c3types.Sender
	isFront bool
}

func (c *Channel) hasFront() bool { return c.cfg.Front.Origin != "" }

func (c *Channel) frontViewerID() string { return frontViewerPrefix + c.cfg.Front.Operator }

// isLocalAddress: address is loopback or one of this host's own, so a request
// from it may be any local process, the operator's own browser included. An
// unreadable interface list counts as local.
func (c *Channel) isLocalAddress(address string) bool {
	ip := net.ParseIP(address)
	if ip == nil || ip.IsLoopback() || ip.IsUnspecified() {
		return true
	}
	hostAddresses, err := c.interfaceAddresses()
	if err != nil {
		return true
	}
	for _, hostAddress := range hostAddresses {
		if interfaceIP(hostAddress).Equal(ip) {
			return true
		}
	}
	return false
}

func interfaceIP(address net.Addr) net.IP {
	switch address := address.(type) {
	case *net.IPNet:
		return address.IP
	case *net.IPAddr:
		return address.IP
	}
	return nil
}

// isFrontRoute: the front reaches this route. With a local from it reaches
// board routes only, as before D044: a local from can't tell the front from
// any process on this host.
func (c *Channel) isFrontRoute(r *http.Request) bool {
	return c.isChatOnFront || isBoardPath(r.URL.Path)
}

// isBoardPath judges the cleaned path, so "/board/../send" is not a board path.
func isBoardPath(rawPath string) bool {
	cleaned := path.Clean(rawPath)
	return cleaned == "/board" || strings.HasPrefix(cleaned, "/board/")
}

// frontClosedPaths are C3's own sign-in, sign-out and health endpoints, which
// don't exist on the front: the front signs the operator in and out.
var frontClosedPaths = map[string]bool{"/login": true, "/login/link": true, "/auth": true, "/logout": true, "/healthz": true}

// admitFrontRequest applies the front's rules before any route and reports
// whether the request goes on. Anyone the front doesn't vouch for gets the
// same 403 on every path; the operator's GET of a sign-in page goes to the
// chat, and the other closed paths are 404.
func (c *Channel) admitFrontRequest(w http.ResponseWriter, r *http.Request) bool {
	if !c.isFrontOperator(r) {
		writeBoardError(w, http.StatusForbidden, "forbidden")
		return false
	}
	cleaned := path.Clean(r.URL.Path)
	if !frontClosedPaths[cleaned] {
		return true
	}
	if r.Method == http.MethodGet && (cleaned == "/login" || cleaned == "/auth") {
		http.Redirect(w, r, "/", http.StatusSeeOther)
	} else {
		http.NotFound(w, r)
	}
	return false
}

// isFrontRequest: the Host is the front origin's, compared canonically. A Host
// whose port can't be parsed counts when its name is the front's, since the
// front's rules refuse more.
func (c *Channel) isFrontRequest(r *http.Request) bool {
	if !c.hasFront() {
		return false
	}
	front, err := url.Parse(c.cfg.Front.Origin)
	if err != nil {
		return false
	}
	frontName, frontPort, _ := canonicalHost(front.Host)
	name, port, isParsed := canonicalHost(r.Host)
	return strings.EqualFold(name, frontName) && (port == frontPort || !isParsed)
}

// canonicalHost splits a Host into its name, without brackets or one trailing
// dot and with an IP literal in canonical form, and its port as a number, 443
// when absent or empty. isParsed is false when what follows the name is no port.
func canonicalHost(host string) (name string, port uint64, isParsed bool) {
	name, portText, _ := strings.Cut(host, ":")
	isParsed = true
	if inside, isBracketed := strings.CutPrefix(host, "["); isBracketed {
		var rest string
		var hasColon bool
		name, rest, _ = strings.Cut(inside, "]")
		portText, hasColon = strings.CutPrefix(rest, ":")
		isParsed = hasColon || rest == ""
	}
	name = strings.TrimSuffix(name, ".")
	if ip := net.ParseIP(name); ip != nil {
		name = ip.String()
	}
	port = 443
	if portText != "" {
		number, err := strconv.ParseUint(portText, 10, 16)
		port, isParsed = number, isParsed && err == nil
	}
	return name, port, isParsed
}

// isFrontPeer: the request's TCP peer is the front's address.
func (c *Channel) isFrontPeer(r *http.Request) bool {
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	peerIP := net.ParseIP(peer)
	return c.hasFront() && err == nil && peerIP != nil && peerIP.Equal(net.ParseIP(c.cfg.Front.From))
}

// isOffFrontAlias is a request the front proxied under another of its names
// for this port. A loopback from can't tell the front from a local client,
// so then the Host alone decides, as before.
func (c *Channel) isOffFrontAlias(r *http.Request) bool {
	return c.isFrontPeer(r) && !net.ParseIP(c.cfg.Front.From).IsLoopback() && !c.isFrontRequest(r)
}

func (c *Channel) isFrontOperator(r *http.Request) bool {
	identities := r.Header.Values(c.cfg.Front.IdentityHeader)
	return c.isFrontPeer(r) && len(identities) == 1 && identities[0] == c.cfg.Front.Operator
}

// boardViewer returns the viewer of a web-chat or board request, or the
// status that refuses it: on the front 404 for a route it doesn't reach and
// 403 for anyone but the operator, elsewhere 401 without a live web login.
func (c *Channel) boardViewer(r *http.Request, touch bool) (boardViewer, int) {
	if c.isFrontRequest(r) {
		if !c.isFrontRoute(r) {
			return boardViewer{}, http.StatusNotFound
		}
		if !c.isFrontOperator(r) {
			return boardViewer{}, http.StatusForbidden
		}
		return boardViewer{
			id:      c.frontViewerID(),
			sender:  c3types.Sender{UserID: c.operatorID, Username: "operator"},
			isFront: true,
		}, 0
	}
	key, current, ok := c.authenticate(r, touch)
	if !ok {
		return boardViewer{}, http.StatusUnauthorized
	}
	return boardViewer{id: key, sender: c3types.Sender{UserID: current.userID, Username: current.username}}, 0
}

// capabilityViewers lists the viewer ids a frame capability may name on this
// request. On the front it is the front's operator, and only when the front
// vouches for this request; elsewhere each live operator login. So a cap
// minted on one path never opens the page on the other.
func (c *Channel) capabilityViewers(r *http.Request) []string {
	if c.isFrontRequest(r) {
		if !c.isFrontOperator(r) {
			return nil
		}
		return []string{c.frontViewerID()}
	}
	now := c.now()
	var viewers []string
	c.authMu.Lock()
	for key, current := range c.sessions {
		if now.Sub(current.lastSeen) < sessionIdleTTL && current.userID == c.operatorID {
			viewers = append(viewers, key)
		}
	}
	c.authMu.Unlock()
	return viewers
}

// requestOrigin is the origin the browser loaded this request from, which the
// shell's frame-src must name exactly.
func (c *Channel) requestOrigin(r *http.Request) string {
	if c.isFrontRequest(r) {
		return normalizeOrigin(c.cfg.Front.Origin)
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if origin := normalizeOrigin(scheme + "://" + r.Host); c.allowedOrigin[origin] {
		return origin
	}
	return c.baseURL
}

// ChatFrontOrigin is where the authenticated front serves the web chat, or ""
// when it doesn't: no front, or a local from that keeps it to boards.
func (c *Channel) ChatFrontOrigin() string {
	if !c.isChatOnFront {
		return ""
	}
	return c.boardLinkBase()
}

// boardLinkBase is where a board's shell link points: the front when one is
// configured, else the web channel's own URL.
func (c *Channel) boardLinkBase() string {
	if c.hasFront() {
		return strings.TrimSuffix(normalizeOrigin(c.cfg.Front.Origin), "/")
	}
	return c.baseURL
}
