package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

func (c *Channel) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", c.handleIndex)
	mux.HandleFunc("GET /login", c.handleLogin)
	mux.HandleFunc("POST /login/link", c.handleLoginLink)
	mux.HandleFunc("GET /auth", c.handleAuthPage)
	mux.HandleFunc("POST /auth", c.handleAuth)
	mux.HandleFunc("POST /logout", c.handleLogout)
	mux.HandleFunc("GET /events", c.handleEvents)
	mux.HandleFunc("POST /send", c.handleSend)
	mux.HandleFunc("POST /voice-note", c.handleVoiceNote)
	mux.HandleFunc("POST /voice", c.handleVoicePreference)
	mux.HandleFunc("GET /audio/unlock", c.handleAudioUnlock)
	mux.HandleFunc("GET /audio/{token}", c.handleAudio)
	mux.HandleFunc("GET /healthz", c.handleHealth)
	mux.HandleFunc("GET /ca.crt", c.handleCACertificate)
	return mux
}

func (c *Channel) handleCACertificate(w http.ResponseWriter, r *http.Request) {
	if !c.cfg.TLS {
		http.NotFound(w, r)
		return
	}
	certificate := c.caCertificatePEM()
	if len(certificate) == 0 {
		http.NotFound(w, r)
		return
	}
	setPageHeaders(w)
	w.Header().Set("Content-Type", "application/x-x509-ca-cert")
	w.Header().Set("Content-Disposition", `attachment; filename="c3-web-ca.crt"`)
	_, _ = w.Write(certificate)
}

func (c *Channel) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if _, _, ok := c.authenticate(r, false); !ok {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	c.servePage(w, "page.html")
}

func (c *Channel) handleLogin(w http.ResponseWriter, _ *http.Request) {
	c.servePage(w, "login.html")
}

func (c *Channel) handleAuthPage(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Referrer-Policy", "no-referrer")
	c.servePage(w, "auth.html")
}

func (c *Channel) servePage(w http.ResponseWriter, name string) {
	page, err := pages.ReadFile(name)
	if err != nil {
		http.Error(w, "page unavailable", http.StatusInternalServerError)
		return
	}
	setPageHeaders(w)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(page)
}

func setPageHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; media-src 'self'; img-src 'none'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
}

func (c *Channel) handleAuth(w http.ResponseWriter, r *http.Request) {
	c.postDeadline(w, r)
	if !c.sameOrigin(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := decodeJSON(w, r, &body); err != nil || body.Token == "" {
		c.denyAuth(w, r)
		return
	}
	token, ok := c.consumeToken(body.Token)
	if !ok {
		c.denyAuth(w, r)
		return
	}
	sessionID, err := c.createSession(token)
	if err != nil {
		http.Error(w, "authentication unavailable", http.StatusInternalServerError)
		return
	}
	c.setSessionCookie(w, r, sessionID)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (c *Channel) denyAuth(w http.ResponseWriter, r *http.Request) {
	c.delayFailedAuth(r.RemoteAddr)
	setPageHeaders(w)
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = io.WriteString(w, errInvalidLogin.Error()+"\n")
}

func (c *Channel) handleLogout(w http.ResponseWriter, r *http.Request) {
	c.postDeadline(w, r)
	if !c.sameOrigin(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, postBodyLimit)
	if body, err := io.ReadAll(r.Body); err != nil || len(strings.TrimSpace(string(body))) != 0 {
		http.Error(w, "request body must be empty", http.StatusBadRequest)
		return
	}
	sessionID, _, ok := c.authenticate(r, false)
	if !ok {
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return
	}
	c.destroySession(sessionID)
	clearSessionCookie(w)
	w.WriteHeader(http.StatusNoContent)
}

func (c *Channel) handleSend(w http.ResponseWriter, r *http.Request) {
	c.postDeadline(w, r)
	if !c.sameOrigin(r) {
		writeSendError(w, http.StatusForbidden, "not allowed")
		return
	}
	sessionID, current, ok := c.authenticate(r, true)
	if !ok {
		writeSendError(w, http.StatusUnauthorized, "sign in again")
		return
	}
	var body struct {
		Text     string `json:"text"`
		ClientID string `json:"client_id"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeSendError(w, http.StatusRequestEntityTooLarge, "message too long")
		} else {
			writeSendError(w, http.StatusBadRequest, "invalid request")
		}
		return
	}
	if !validSendText(body.Text) || body.ClientID == "" {
		writeSendError(w, http.StatusBadRequest, "invalid request")
		return
	}
	if utf8.RuneCountInString(body.Text) > maxMessageRunes {
		writeSendError(w, http.StatusRequestEntityTooLarge, "message too long")
		return
	}
	messageID, status, err := c.acceptInbound(sessionID, current, body.Text, body.ClientID)
	if errors.Is(err, errClientIDConflict) {
		writeSendError(w, status, err.Error())
		return
	}
	if status == http.StatusForbidden {
		writeSendError(w, status, "not allowed")
		return
	}
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "2")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		MessageID int64 `json:"message_id"`
	}{MessageID: messageID})
}

func writeSendError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{Error: message})
}

func (c *Channel) handleLoginLink(w http.ResponseWriter, r *http.Request) {
	c.postDeadline(w, r)
	if !c.sameOrigin(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	limited := http.MaxBytesReader(w, r.Body, postBodyLimit)
	body, err := io.ReadAll(limited)
	if err != nil || strings.TrimSpace(string(body)) != "" {
		http.Error(w, "request body must be empty", http.StatusBadRequest)
		return
	}
	if !c.reserveLoginLink() {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "a login link was requested recently", http.StatusTooManyRequests)
		return
	}
	delivery, ok := c.host.(loginDeliveryHost)
	if !ok {
		c.finishLoginLink(false)
		http.Error(w, "login-link delivery unavailable", http.StatusServiceUnavailable)
		return
	}
	sent, err := delivery.SendWebLoginLink("requested from the web login page")
	c.finishLoginLink(err == nil && sent)
	if err != nil {
		http.Error(w, "login-link delivery unavailable", http.StatusServiceUnavailable)
		return
	}
	if !sent {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "a login link was requested recently", http.StatusTooManyRequests)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (c *Channel) handleEvents(w http.ResponseWriter, r *http.Request) {
	if !c.eventsOriginAllowed(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	sessionID, _, ok := c.authenticate(r, true)
	if !ok {
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	client, replay, incomplete := c.connectStream(sessionID, r.Header.Get("Last-Event-ID"))
	defer c.removeStream(sessionID, client)
	c.authMu.Lock()
	current := c.sessions[sessionID]
	voice := current != nil && current.voice
	c.authMu.Unlock()
	if err := writeSSE(w, streamEvent{kind: "prefs", payload: streamPayload{Voice: &voice}}); err != nil {
		return
	}
	if incomplete {
		_ = writeSSE(w, streamEvent{kind: "status", payload: streamPayload{Text: "history may be incomplete", Timestamp: streamTimestamp(c.now())}})
	}
	for _, event := range replay {
		if err := writeSSE(w, event); err != nil {
			return
		}
	}
	flusher.Flush()

	heartbeat := time.NewTicker(c.heartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case event := <-client.events:
			if err := writeSSE(w, event); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ":hb\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-client.done:
			return
		case <-r.Context().Done():
			return
		}
	}
}

func writeSSE(w io.Writer, event streamEvent) error {
	data, err := marshalStreamEvent(event)
	if err != nil {
		return err
	}
	if event.sequence > 0 {
		if _, err := fmt.Fprintf(w, "id: %d\n", event.sequence); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.kind, data)
	return err
}

func (c *Channel) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, postBodyLimit)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request must contain one JSON value")
	}
	return nil
}

func (c *Channel) postDeadline(w http.ResponseWriter, r *http.Request) {
	deadline := time.Now().Add(15 * time.Second)
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(deadline)
	_ = controller.SetWriteDeadline(deadline)
}
