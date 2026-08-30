package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

type streamPayload struct {
	MessageID       int64      `json:"message_id,omitempty"`
	ReplyTo         int64      `json:"reply_to,omitempty"`
	ClientID        string     `json:"client_id,omitempty"`
	Text            string     `json:"text,omitempty"`
	Timestamp       *time.Time `json:"timestamp,omitempty"`
	Active          bool       `json:"active,omitempty"`
	Voice           *bool      `json:"voice,omitempty"`
	DurationSeconds float64    `json:"duration_seconds,omitempty"`
	URL             string     `json:"url,omitempty"`
	Bytes           int        `json:"bytes,omitempty"`
	Provider        string     `json:"provider,omitempty"`
	Replay          bool       `json:"replay,omitempty"`
	Attached        *bool      `json:"attached,omitempty"`
	CLI             string     `json:"cli,omitempty"`
	CWD             string     `json:"cwd,omitempty"`
	PID             int        `json:"pid,omitempty"`
	SessionID       string     `json:"session_id,omitempty"`
	Since           string     `json:"since,omitempty"`
}

type streamEvent struct {
	sequence int64
	kind     string
	payload  streamPayload
}

type streamClient struct {
	events chan streamEvent
	done   chan struct{}
	once   sync.Once
}

func (client *streamClient) close() {
	client.once.Do(func() { close(client.done) })
}

func (c *Channel) SendReply(args c3types.ReplyArgs) (int64, error) {
	if err := c.checkDestination(args.Channel, args.ChatID, args.TopicID); err != nil {
		return 0, err
	}
	if args.Poll != nil || len(args.Media) != 0 || len(args.Buttons) != 0 {
		return 0, fmt.Errorf("%w: rich outbound content", errUnsupported)
	}
	if args.ReplyTo != nil && *args.ReplyTo > 0 && isVoiceReadbackNotice(args.Text) {
		voice := true
		c.publish(streamEvent{kind: "edit", payload: streamPayload{
			MessageID: *args.ReplyTo, Text: args.Text, Voice: &voice,
		}})
		return *args.ReplyTo, nil
	}
	messageID := c.nextReplyMessageID()
	payload := streamPayload{MessageID: messageID, Text: args.Text, Timestamp: streamTimestamp(c.now())}
	if args.ReplyTo != nil && *args.ReplyTo > 0 {
		payload.ReplyTo = *args.ReplyTo
	}
	event := streamEvent{kind: "message", payload: payload}
	if isStatusText(args.Text) {
		event.kind = "status"
	}
	c.publish(event)
	if event.kind == "message" && c.hasVoiceSession() {
		c.enqueueSpeech(speechJob{messageID: messageID, text: args.Text})
	}
	return messageID, nil
}

func (c *Channel) SendTyping(chatID int64, threadID *int64) error {
	if err := c.checkDestination(Name, chatID, threadID); err != nil {
		return err
	}
	c.publish(streamEvent{kind: "typing", payload: streamPayload{Active: true}})
	return nil
}

func (c *Channel) EditMessage(args c3types.EditArgs) (*c3types.EditResult, error) {
	if err := c.checkDestination(args.Channel, args.ChatID, nil); err != nil {
		return nil, err
	}
	if args.MessageID <= 0 {
		return nil, errors.New("web: edit requires a positive message id")
	}
	if len(args.Buttons) != 0 {
		return nil, fmt.Errorf("%w: buttons", errUnsupported)
	}
	c.publish(streamEvent{
		kind:    "edit",
		payload: streamPayload{MessageID: args.MessageID, Text: args.Text, Timestamp: streamTimestamp(c.now())},
	})
	return &c3types.EditResult{MessageID: args.MessageID}, nil
}

func (c *Channel) React(c3types.ReactArgs) error { return fmt.Errorf("%w: reactions", errUnsupported) }

func (c *Channel) DownloadAttachment(string) (string, error) {
	return "", fmt.Errorf("%w: attachments", errUnsupported)
}

func (c *Channel) StopPoll(int64, int64) (*c3types.PollResult, error) {
	return nil, fmt.Errorf("%w: polls", errUnsupported)
}

func (c *Channel) CreateTopic(int64, string) (int64, error) {
	return 0, fmt.Errorf("%w: topics", errUnsupported)
}

func (c *Channel) ValidateTopic(int64, int64) error {
	return fmt.Errorf("%w: topics", errUnsupported)
}

func (c *Channel) checkDestination(channelName string, chatID int64, topicID *int64) error {
	if channelName != "" && channelName != Name {
		return fmt.Errorf("web: destination channel %q does not match %q", channelName, Name)
	}
	if chatID != c.operatorID || topicID != nil {
		return errors.New("web: destination is not the claimed operator route")
	}
	return nil
}

func isStatusText(text string) bool {
	return strings.HasPrefix(text, "📨 Held") || strings.HasPrefix(text, "⏸ Permission") || strings.HasPrefix(text, "⚠️")
}

func isVoiceReadbackNotice(text string) bool {
	return strings.HasPrefix(text, c3types.VoiceTranscriptionFailureNoticePrefix) ||
		strings.HasPrefix(text, c3types.VoiceDownloadFailureNoticePrefix)
}

func streamTimestamp(value time.Time) *time.Time { return &value }

func (c *Channel) nextReplyMessageID() int64 {
	c.idMu.Lock()
	c.nextReplyID++
	messageID := c.nextReplyID
	c.idMu.Unlock()
	c.persistSessions("reply-id advance")
	return messageID
}

func (c *Channel) publish(event streamEvent) {
	if event.kind == "message" || event.kind == "own" || event.kind == "edit" {
		c.replayMu.Lock()
		c.idMu.Lock()
		c.nextEventID++
		event.sequence = c.nextEventID
		c.idMu.Unlock()
		c.persistSessions("event-id advance")
		c.replay = append(c.replay, event)
		if len(c.replay) > replayLimit {
			c.replay = append([]streamEvent(nil), c.replay[len(c.replay)-replayLimit:]...)
		}
		if err := c.appendReplay(event); err != nil && c.host != nil {
			c.host.Logf("web: persist replay event %d: %v", event.sequence, err)
		}
		c.broadcastLocked(event)
		c.replayMu.Unlock()
		return
	}
	c.broadcast(event)
}

// replayMu must be held so an SSE connect cannot fall into the gap between a
// replay snapshot and registration for live events.
func (c *Channel) broadcastLocked(event streamEvent) {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	c.broadcastToClientsLocked(event)
}

func (c *Channel) broadcast(event streamEvent) {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	c.broadcastToClientsLocked(event)
}

func (c *Channel) broadcastSession(sessionID string, event streamEvent) {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	for client := range c.streams[sessionID] {
		select {
		case client.events <- event:
		default:
			client.close()
			delete(c.streams[sessionID], client)
		}
	}
	if len(c.streams[sessionID]) == 0 {
		delete(c.streams, sessionID)
	}
}

func (c *Channel) broadcastVoiceSessions(event streamEvent) {
	enabled := make(map[string]bool)
	c.authMu.Lock()
	for sessionID, current := range c.sessions {
		if current.voice {
			enabled[sessionID] = true
		}
	}
	c.authMu.Unlock()
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	for sessionID, clients := range c.streams {
		if !enabled[sessionID] {
			continue
		}
		for client := range clients {
			select {
			case client.events <- event:
			default:
				client.close()
				delete(clients, client)
			}
		}
		if len(clients) == 0 {
			delete(c.streams, sessionID)
		}
	}
}

func (c *Channel) broadcastToClientsLocked(event streamEvent) {
	for sessionID, clients := range c.streams {
		for client := range clients {
			select {
			case client.events <- event:
			default:
				client.close()
				delete(clients, client)
			}
		}
		if len(clients) == 0 {
			delete(c.streams, sessionID)
		}
	}
}

func (c *Channel) connectStream(sessionID, lastEventID string) (*streamClient, []streamEvent, bool) {
	client := &streamClient{events: make(chan streamEvent, 64), done: make(chan struct{})}
	c.replayMu.Lock()
	defer c.replayMu.Unlock()
	replay, incomplete := replayAfter(c.replay, lastEventID)
	c.streamMu.Lock()
	if c.streams[sessionID] == nil {
		c.streams[sessionID] = make(map[*streamClient]struct{})
	}
	c.streams[sessionID][client] = struct{}{}
	c.streamMu.Unlock()
	return client, replay, incomplete
}

func replayAfter(events []streamEvent, lastEventID string) ([]streamEvent, bool) {
	if lastEventID == "" {
		incomplete := len(events) > 0 && events[0].sequence > 1
		return append([]streamEvent(nil), events...), incomplete
	}
	var last int64
	if _, err := fmt.Sscan(lastEventID, &last); err != nil || last < 0 {
		return nil, true
	}
	if len(events) == 0 {
		return nil, true
	}
	if last < events[0].sequence {
		return append([]streamEvent(nil), events...), true
	}
	for index := range events {
		if events[index].sequence > last {
			return append([]streamEvent(nil), events[index:]...), false
		}
	}
	return nil, false
}

func (c *Channel) removeStream(sessionID string, client *streamClient) {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	clients := c.streams[sessionID]
	delete(clients, client)
	if len(clients) == 0 {
		delete(c.streams, sessionID)
	}
	client.close()
}

func (c *Channel) closeSessionStreams(sessionID string) {
	c.streamMu.Lock()
	clients := c.streams[sessionID]
	delete(c.streams, sessionID)
	for client := range clients {
		client.close()
	}
	c.streamMu.Unlock()
}

func (c *Channel) closeAllStreams() {
	c.streamMu.Lock()
	for sessionID, clients := range c.streams {
		for client := range clients {
			client.close()
		}
		delete(c.streams, sessionID)
	}
	c.streamMu.Unlock()
}

func marshalStreamEvent(event streamEvent) ([]byte, error) {
	return json.Marshal(event.payload)
}
