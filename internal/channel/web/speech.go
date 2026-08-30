package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

const (
	speechConcurrency   = 2
	speechQueueLimit    = 8
	speechTimeout       = 60 * time.Second
	speechErrorInterval = 60 * time.Second
	audioCacheTTL       = 15 * time.Minute
	audioCacheEntries   = 24
	audioCacheBytes     = 32 << 20
)

const (
	spokenRepliesOnMessage  = "The operator turned spoken replies ON in the web chat: replies are read aloud. Follow the SPOKEN REPLIES rules — lead with the answer in one or two plain sentences, no markdown syntax, summarise code instead of dictating it."
	spokenRepliesOffMessage = "The operator turned spoken replies OFF: replies are read, not heard — normal formatting applies."
)

type synthesizerHost interface {
	Synthesize(context.Context, c3types.SpeechRequest) (c3types.SpeechResult, error)
}

type speechJob struct {
	messageID      int64
	text           string
	explicit       bool
	requestContext context.Context
	result         chan speechResult
}

type speechResult struct {
	token string
	err   error
}

type cachedAudio struct {
	audio     []byte
	messageID int64
	created   time.Time
}

func (c *Channel) handleVoicePreference(w http.ResponseWriter, r *http.Request) {
	c.postDeadline(w, r)
	if !c.sameOrigin(r) {
		writeSendError(w, http.StatusForbidden, "not allowed")
		return
	}
	sessionID, _, ok := c.authenticate(r, true)
	if !ok {
		writeSendError(w, http.StatusUnauthorized, "sign in again")
		return
	}
	var body struct {
		Enabled *bool `json:"enabled"`
	}
	if err := decodeJSON(w, r, &body); err != nil || body.Enabled == nil {
		writeSendError(w, http.StatusBadRequest, "invalid request")
		return
	}

	c.authMu.Lock()
	current := c.sessions[sessionID]
	if current == nil {
		c.authMu.Unlock()
		writeSendError(w, http.StatusUnauthorized, "sign in again")
		return
	}
	previous := current.voice
	if previous == *body.Enabled {
		c.authMu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	current.voice = *body.Enabled
	c.authMu.Unlock()
	if err := c.saveSessions(); err != nil {
		c.authMu.Lock()
		if current := c.sessions[sessionID]; current != nil && current.voice == *body.Enabled {
			current.voice = previous
		}
		c.authMu.Unlock()
		if c.host != nil {
			c.host.Logf("web: persist voice preference: %v", err)
		}
		writeSendError(w, http.StatusInternalServerError, "voice preference unavailable")
		return
	}
	voice := *body.Enabled
	c.broadcastSession(sessionID, streamEvent{kind: "prefs", payload: streamPayload{Voice: &voice}})
	c.emitVoicePreferenceNotice(voice)
	w.WriteHeader(http.StatusNoContent)
}

func (c *Channel) emitVoicePreferenceNotice(enabled bool) {
	title := "Spoken replies OFF"
	message := spokenRepliesOffMessage
	if enabled {
		title = "Spoken replies ON"
		message = spokenRepliesOnMessage
	}
	inbound := &c3types.Inbound{
		Channel: Name, ChatID: c.operatorID, Kind: c3types.InboundSystem,
		Event: &c3types.InboundEvent{System: &c3types.SystemEvent{
			Source: "web", Level: "info", Title: title, Message: message,
		}},
	}
	// This trusted channel-authored system notice bypasses GateInbound, just as
	// the broker's broadcastSystemEvent notices do.
	if c.host == nil || !c.host.Emit(inbound) {
		if c.host != nil {
			c.host.Logf("web: dropped %q system event before worker admission", title)
		}
	}
}

func (c *Channel) hasVoiceSession() bool {
	if !c.HasLiveSession(c.operatorID) {
		return false
	}
	enabled := make(map[string]bool)
	c.authMu.Lock()
	for sessionID, current := range c.sessions {
		if current.userID == c.operatorID && current.voice {
			enabled[sessionID] = true
		}
	}
	c.authMu.Unlock()
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	for sessionID := range enabled {
		if len(c.streams[sessionID]) != 0 {
			return true
		}
	}
	return false
}

func (c *Channel) enqueueSpeech(job speechJob) bool {
	c.speechMu.Lock()
	if c.speechContext == nil || c.speechContext.Err() != nil {
		c.speechMu.Unlock()
		return false
	}
	if c.speechActive < speechConcurrency {
		c.speechActive++
		c.speechMu.Unlock()
		go c.runSpeech(job)
		return true
	}
	if len(c.speechQueue) >= speechQueueLimit {
		c.speechMu.Unlock()
		if c.host != nil {
			c.host.Logf("web: speech queue full; dropped message_id=%d", job.messageID)
		}
		return false
	}
	c.speechQueue = append(c.speechQueue, job)
	c.speechMu.Unlock()
	return true
}

func (c *Channel) runSpeech(job speechJob) {
	outcome := speechResult{}
	defer func() {
		if recovered := recover(); recovered != nil {
			outcome.err = fmt.Errorf("speech synthesis panic")
			if c.host != nil {
				c.host.Logf("web: speech panic for message_id=%d: %v", job.messageID, recovered)
			}
		}
		if job.result != nil {
			job.result <- outcome
		}
		c.finishSpeech()
	}()
	if !job.explicit && !c.hasVoiceSession() {
		return
	}
	host, ok := c.host.(synthesizerHost)
	if !ok {
		outcome.err = c3types.ErrNoSynthesizer
		if !job.explicit {
			c.speechFailed(job.messageID, outcome.err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(c.speechContext, speechTimeout)
	stopRequestCancel := func() bool { return false }
	if job.requestContext != nil {
		stopRequestCancel = context.AfterFunc(job.requestContext, cancel)
	}
	result, err := host.Synthesize(ctx, c3types.SpeechRequest{Text: job.text, Language: ""})
	stopRequestCancel()
	cancel()
	if err != nil {
		if errors.Is(err, c3types.ErrNothingToSay) || c.speechContext.Err() != nil {
			outcome.err = err
			return
		}
		outcome.err = err
		if !job.explicit {
			c.speechFailed(job.messageID, err)
		} else if c.host != nil {
			c.host.Logf("web: explicit speech unavailable for message_id=%d: %v", job.messageID, err)
		}
		return
	}
	if len(result.Audio) == 0 || result.MIME != "audio/mpeg" {
		outcome.err = fmt.Errorf("invalid synthesizer result")
		if !job.explicit {
			c.speechFailed(job.messageID, outcome.err)
		}
		return
	}
	token, err := c.cacheSpeech(job.messageID, result.Audio)
	if err != nil {
		outcome.err = err
		if !job.explicit {
			c.speechFailed(job.messageID, err)
		}
		return
	}
	outcome.token = token
	if job.explicit {
		return
	}
	c.broadcastVoiceSessions(streamEvent{kind: "audio", payload: streamPayload{
		MessageID: job.messageID, URL: "/audio/" + token, Bytes: len(result.Audio), Provider: result.Provider,
	}})
}

func (c *Channel) handleSpeak(w http.ResponseWriter, r *http.Request) {
	c.postDeadline(w, r)
	if !c.sameOrigin(r) {
		writeSendError(w, http.StatusForbidden, "not allowed")
		return
	}
	if _, _, ok := c.authenticate(r, true); !ok {
		writeSendError(w, http.StatusUnauthorized, "sign in again")
		return
	}
	var body struct {
		MessageID int64 `json:"message_id"`
	}
	if err := decodeJSON(w, r, &body); err != nil || body.MessageID <= 0 {
		writeSendError(w, http.StatusBadRequest, "invalid request")
		return
	}
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Time{})
	_ = controller.SetWriteDeadline(time.Time{})
	text, ok := c.agentReplyText(body.MessageID)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if token, ok := c.audioTokenForMessage(body.MessageID); ok {
		writeSpeakResult(w, body.MessageID, token)
		return
	}
	if _, ok := c.host.(synthesizerHost); !ok {
		writeSendError(w, http.StatusServiceUnavailable, "speech unavailable")
		return
	}
	result := make(chan speechResult, 1)
	if !c.enqueueSpeech(speechJob{
		messageID: body.MessageID, text: text, explicit: true,
		requestContext: r.Context(), result: result,
	}) {
		writeSendError(w, http.StatusServiceUnavailable, "speech busy")
		return
	}
	select {
	case outcome := <-result:
		if outcome.err != nil || outcome.token == "" {
			writeSendError(w, http.StatusServiceUnavailable, "speech unavailable")
			return
		}
		writeSpeakResult(w, body.MessageID, outcome.token)
	case <-r.Context().Done():
		return
	}
}

func writeSpeakResult(w http.ResponseWriter, messageID int64, token string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		URL       string `json:"url"`
		MessageID int64  `json:"message_id"`
	}{URL: "/audio/" + token, MessageID: messageID})
}

func (c *Channel) agentReplyText(messageID int64) (string, bool) {
	c.replayMu.Lock()
	defer c.replayMu.Unlock()
	text := ""
	found := false
	for _, event := range c.replay {
		if event.payload.MessageID != messageID {
			continue
		}
		switch event.kind {
		case "message":
			text = event.payload.Text
			found = true
		case "edit":
			if found && (event.payload.Voice == nil || !*event.payload.Voice) {
				text = event.payload.Text
			}
		}
	}
	return text, found
}

func (c *Channel) finishSpeech() {
	c.speechMu.Lock()
	if c.speechContext == nil || c.speechContext.Err() != nil {
		c.speechQueue = nil
		if c.speechActive > 0 {
			c.speechActive--
		}
		c.speechMu.Unlock()
		return
	}
	if len(c.speechQueue) == 0 {
		c.speechActive--
		c.speechMu.Unlock()
		return
	}
	next := c.speechQueue[0]
	c.speechQueue = append([]speechJob(nil), c.speechQueue[1:]...)
	c.speechMu.Unlock()
	go c.runSpeech(next)
}

func (c *Channel) speechFailed(messageID int64, err error) {
	if c.host != nil {
		c.host.Logf("web: speech unavailable for message_id=%d: %v", messageID, err)
	}
	now := c.now()
	c.speechErrorMu.Lock()
	if !c.lastSpeechError.IsZero() && now.Sub(c.lastSpeechError) < speechErrorInterval {
		c.speechErrorMu.Unlock()
		return
	}
	c.lastSpeechError = now
	c.speechErrorMu.Unlock()
	c.broadcastVoiceSessions(streamEvent{kind: "status", payload: streamPayload{
		Text: "🔇 voice unavailable: " + shortSpeechError(err), Timestamp: streamTimestamp(now),
	}})
}

func shortSpeechError(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, c3types.ErrNoSynthesizer):
		return "not configured"
	default:
		return "provider error"
	}
}

func (c *Channel) cacheSpeech(messageID int64, audio []byte) (string, error) {
	if len(audio) > audioCacheBytes {
		return "", errors.New("synthesized audio exceeds cache limit")
	}
	token, err := randomHexID()
	if err != nil {
		return "", err
	}
	now := c.now()
	c.audioMu.Lock()
	defer c.audioMu.Unlock()
	c.evictExpiredAudioLocked(now)
	for len(c.audioCache) >= audioCacheEntries || c.audioBytes+int64(len(audio)) > audioCacheBytes {
		if !c.evictOldestAudioLocked() {
			return "", errors.New("synthesized audio cache is full")
		}
	}
	copy := append([]byte(nil), audio...)
	c.audioCache[token] = cachedAudio{audio: copy, messageID: messageID, created: now}
	c.audioBytes += int64(len(copy))
	return token, nil
}

func (c *Channel) evictExpiredAudioLocked(now time.Time) {
	for token, current := range c.audioCache {
		if now.Sub(current.created) >= audioCacheTTL {
			c.audioBytes -= int64(len(current.audio))
			delete(c.audioCache, token)
		}
	}
}

func (c *Channel) evictOldestAudioLocked() bool {
	if len(c.audioCache) == 0 {
		return false
	}
	tokens := make([]string, 0, len(c.audioCache))
	for token := range c.audioCache {
		tokens = append(tokens, token)
	}
	sort.Slice(tokens, func(i, j int) bool {
		left, right := c.audioCache[tokens[i]], c.audioCache[tokens[j]]
		if left.created.Equal(right.created) {
			return tokens[i] < tokens[j]
		}
		return left.created.Before(right.created)
	})
	oldest := tokens[0]
	c.audioBytes -= int64(len(c.audioCache[oldest].audio))
	delete(c.audioCache, oldest)
	return true
}

func (c *Channel) audioForToken(token string) (cachedAudio, bool) {
	c.audioMu.Lock()
	defer c.audioMu.Unlock()
	c.evictExpiredAudioLocked(c.now())
	audio, ok := c.audioCache[token]
	return audio, ok
}

func (c *Channel) audioTokenForMessage(messageID int64) (string, bool) {
	c.audioMu.Lock()
	defer c.audioMu.Unlock()
	c.evictExpiredAudioLocked(c.now())
	token := ""
	var newest time.Time
	for candidate, audio := range c.audioCache {
		if audio.messageID != messageID || (!newest.IsZero() && audio.created.Before(newest)) {
			continue
		}
		token = candidate
		newest = audio.created
	}
	return token, token != ""
}

func (c *Channel) handleAudio(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if _, _, ok := c.authenticate(r, false); !ok {
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return
	}
	audio, ok := c.audioForToken(r.PathValue("token"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	serveMP3(w, r, audio.created, audio.audio)
}

func (c *Channel) handleAudioUnlock(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if _, _, ok := c.authenticate(r, false); !ok {
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return
	}
	audio, err := pages.ReadFile("unlock.mp3")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	serveMP3(w, r, time.Time{}, audio)
}

func serveMP3(w http.ResponseWriter, r *http.Request, modified time.Time, audio []byte) {
	w.Header().Set("Content-Type", "audio/mpeg")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Accept-Ranges", "bytes")
	http.ServeContent(w, r, "audio.mp3", modified, bytes.NewReader(audio))
}
