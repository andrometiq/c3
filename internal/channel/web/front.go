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

// The authenticated HTTPS front (channels.web.front) serves boards only. A
// request whose Host is the front's reaches only board routes, and it is the
// operator only when its TCP peer is the front's address and the identity
// header names the operator; C3's own login never counts there.

const frontViewerPrefix = "front:"

// boardViewer is who asks for a board route. Its id is what a frame
// capability names: the web session key, or frontViewerPrefix plus identity.
type boardViewer struct {
	id     string
	sender c3types.Sender
}

func (c *Channel) hasFront() bool { return c.cfg.Front.Origin != "" }

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

// boardViewer returns the viewer of a board request, or the status that
// refuses it: 403 on the front for anyone but the operator, 401 elsewhere
// without a live web login.
func (c *Channel) boardViewer(r *http.Request, touch bool) (boardViewer, int) {
	if c.isFrontRequest(r) {
		if !c.isFrontOperator(r) {
			return boardViewer{}, http.StatusForbidden
		}
		return boardViewer{
			id:     frontViewerPrefix + c.cfg.Front.Operator,
			sender: c3types.Sender{UserID: c.operatorID, Username: "operator"},
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
		return []string{frontViewerPrefix + c.cfg.Front.Operator}
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

// boardLinkBase is where a board's shell link points: the front when one is
// configured, else the web channel's own URL.
func (c *Channel) boardLinkBase() string {
	if c.hasFront() {
		return strings.TrimSuffix(normalizeOrigin(c.cfg.Front.Origin), "/")
	}
	return c.baseURL
}

// isBoardPath judges the cleaned path, so "/board/../send" is not a board path.
func isBoardPath(rawPath string) bool {
	cleaned := path.Clean(rawPath)
	return cleaned == "/board" || strings.HasPrefix(cleaned, "/board/")
}
