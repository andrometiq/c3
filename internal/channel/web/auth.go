package web

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const sessionCookieName = "c3_web_session"

var errInvalidLogin = errors.New("link invalid or expired — request a fresh one")

type loginToken struct {
	userID   int64
	username string
	expires  time.Time
}

type session struct {
	userID        int64
	username      string
	created       time.Time
	lastSeen      time.Time
	savedLastSeen time.Time
	voice         bool
}

func randomID() (string, error) {
	buffer := make([]byte, 32)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

// MintLoginLink creates a single-use token for the configured operator. The
// token is carried only in the URL fragment.
func (c *Channel) MintLoginLink(userID int64) (string, error) {
	c.ensureState()
	if userID == 0 || userID != c.operatorID {
		return "", errors.New("web: login links are available only for the configured operator")
	}
	token, err := randomID()
	if err != nil {
		return "", err
	}
	c.authMu.Lock()
	for existing, current := range c.tokens {
		if !c.now().Before(current.expires) {
			delete(c.tokens, existing)
		}
	}
	c.tokens[token] = loginToken{userID: userID, username: "operator", expires: c.now().Add(loginTokenTTL)}
	c.authMu.Unlock()
	return c.baseURL + "/auth#" + token, nil
}

func (c *Channel) HasLiveSession(userID int64) bool {
	c.ensureState()
	now := c.now()
	var expired []string
	found := false
	c.authMu.Lock()
	for key, current := range c.sessions {
		if now.Sub(current.lastSeen) >= sessionIdleTTL {
			delete(c.sessions, key)
			expired = append(expired, key)
			continue
		}
		if current.userID == userID {
			found = true
		}
	}
	c.authMu.Unlock()
	if len(expired) != 0 {
		c.deleteClientMessagesForSessions(expired)
		for _, key := range expired {
			c.closeSessionStreams(key)
		}
		c.persistSessions("idle session eviction")
	}
	return found
}

func (c *Channel) consumeToken(token string) (loginToken, bool) {
	now := c.now()
	c.authMu.Lock()
	defer c.authMu.Unlock()
	current, ok := c.tokens[token]
	if !ok {
		return loginToken{}, false
	}
	delete(c.tokens, token)
	if !now.Before(current.expires) {
		return loginToken{}, false
	}
	return current, true
}

func (c *Channel) createSession(token loginToken) (string, error) {
	cookieValue, err := randomID()
	if err != nil {
		return "", err
	}
	key := sessionKey(cookieValue)
	now := c.now()
	c.authMu.Lock()
	c.sessions[key] = &session{
		userID: token.userID, username: token.username,
		created: now, lastSeen: now, savedLastSeen: now,
	}
	c.authMu.Unlock()
	if err := c.saveSessions(); err != nil {
		c.authMu.Lock()
		delete(c.sessions, key)
		c.authMu.Unlock()
		return "", err
	}
	return cookieValue, nil
}

func (c *Channel) authenticate(r *http.Request, touch bool) (string, session, bool) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil || cookie.Value == "" {
		return "", session{}, false
	}
	key := sessionKey(cookie.Value)
	now := c.now()
	persistTouch := false
	c.authMu.Lock()
	current, ok := c.sessions[key]
	if !ok {
		c.authMu.Unlock()
		return "", session{}, false
	}
	if now.Sub(current.lastSeen) >= sessionIdleTTL || c.operatorID != 0 && current.userID != c.operatorID {
		delete(c.sessions, key)
		c.authMu.Unlock()
		c.deleteClientMessagesForSessions([]string{key})
		c.closeSessionStreams(key)
		c.persistSessions("idle session eviction")
		return "", session{}, false
	}
	if touch {
		current.lastSeen = now
		if now.Sub(current.savedLastSeen) >= sessionTouchWriteInterval {
			persistTouch = true
		}
	}
	copy := *current
	c.authMu.Unlock()
	if persistTouch {
		c.persistSessions("session last_seen")
	}
	return key, copy, true
}

func (c *Channel) destroySession(key string) {
	c.authMu.Lock()
	delete(c.sessions, key)
	c.authMu.Unlock()
	c.deleteClientMessagesForSessions([]string{key})
	c.closeSessionStreams(key)
	c.persistSessions("session logout")
}

func (c *Channel) deleteClientMessagesForSessions(sessionKeys []string) {
	deleted := make(map[string]bool, len(sessionKeys))
	for _, key := range sessionKeys {
		deleted[key] = true
	}
	c.clientMu.Lock()
	for key := range c.clientMessages {
		if deleted[key.sessionID] {
			delete(c.clientMessages, key)
		}
	}
	c.clientMu.Unlock()
}

func (c *Channel) setSessionCookie(w http.ResponseWriter, r *http.Request, id string) {
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookieName, Value: id, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteLaxMode, Secure: r.TLS != nil || strings.HasPrefix(c.cfg.PublicURL, "https://"),
		MaxAge: int(sessionIdleTTL / time.Second), Expires: c.now().Add(sessionIdleTTL),
	})
}

func clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookieName, Value: "", Path: "/", HttpOnly: true,
		SameSite: http.SameSiteLaxMode, MaxAge: -1,
	})
}

func (c *Channel) sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin != "" {
		return c.allowedOrigin[normalizeOrigin(origin)]
	}
	return r.Header.Get("Sec-Fetch-Site") == "same-origin"
}

func (c *Channel) eventsOriginAllowed(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	return origin == "" || c.allowedOrigin[normalizeOrigin(origin)]
}

func normalizeOrigin(origin string) string {
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return origin
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	return parsed.String()
}

func (c *Channel) delayFailedAuth(remoteAddress string) {
	host, _, err := net.SplitHostPort(remoteAddress)
	if err != nil {
		host = remoteAddress
	}
	now := c.now()
	c.authMu.Lock()
	for remote, current := range c.failedAuth {
		if !current.After(now) {
			delete(c.failedAuth, remote)
		}
	}
	deadline := now.Add(c.failedAttemptDelay)
	if prior := c.failedAuth[host]; prior.After(now) {
		deadline = prior.Add(c.failedAttemptDelay)
	}
	ceiling := now.Add(maxFailedAuthDelay)
	if deadline.After(ceiling) {
		deadline = ceiling
	}
	c.failedAuth[host] = deadline
	c.authMu.Unlock()
	if wait := deadline.Sub(now); wait > 0 {
		c.sleep(wait)
	}
}

func (c *Channel) reserveLoginLink() bool {
	now := c.now()
	c.authMu.Lock()
	defer c.authMu.Unlock()
	cutoff := now.Add(-time.Hour)
	kept := c.loginLinkTimes[:0]
	for _, mintTime := range c.loginLinkTimes {
		if mintTime.After(cutoff) {
			kept = append(kept, mintTime)
		}
	}
	c.loginLinkTimes = kept
	if c.loginLinkInFlight || len(kept) >= loginLinkHourlyLimit || len(kept) > 0 && now.Sub(kept[len(kept)-1]) < loginLinkDebounce {
		return false
	}
	c.loginLinkInFlight = true
	return true
}

func (c *Channel) finishLoginLink(success bool) {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	c.loginLinkInFlight = false
	if success {
		c.loginLinkTimes = append(c.loginLinkTimes, c.now())
	}
}
