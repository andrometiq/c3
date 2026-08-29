package web

import (
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

type clientMessageKey struct {
	sessionID string
	clientID  string
}

type clientMessage struct {
	mu        sync.Mutex
	messageID int64
	text      string
	status    int
	created   time.Time
}

var errClientIDConflict = errors.New("client_id was already used for different text")

// acceptInbound serializes one client id across retries and stamps every trust-
// boundary field from the authenticated session and channel config.
func (c *Channel) acceptInbound(sessionID string, current session, text, clientID string) (int64, int, error) {
	key := clientMessageKey{sessionID: sessionID, clientID: clientID}
	now := c.now()
	c.clientMu.Lock()
	for existingKey, existing := range c.clientMessages {
		if now.Sub(existing.created) >= clientMessageTTL {
			delete(c.clientMessages, existingKey)
		}
	}
	record := c.clientMessages[key]
	if record == nil {
		record = &clientMessage{messageID: c.nextID(), text: text, created: now}
		c.clientMessages[key] = record
	}
	c.clientMu.Unlock()

	record.mu.Lock()
	defer record.mu.Unlock()
	if record.text != text {
		return record.messageID, http.StatusConflict, errClientIDConflict
	}
	if record.status != 0 {
		return record.messageID, record.status, nil
	}
	inbound := &c3types.Inbound{
		Channel: Name, ChatID: c.operatorID, TopicID: nil,
		MessageID: record.messageID,
		Sender:    c3types.Sender{UserID: current.userID, Username: current.username},
		Text:      text,
		Timestamp: now,
		ConvKind:  c3types.ConvKindDM,
		Kind:      c3types.InboundMessage,
		V:         c3types.InboundRecordVersion,
	}
	switch c.host.GateInbound(inbound) {
	case channel.GateInboundAllow:
		if !c.host.Emit(inbound) {
			return record.messageID, http.StatusServiceUnavailable, nil
		}
		record.status = http.StatusAccepted
		c.publish(streamEvent{
			kind: "own",
			payload: streamPayload{
				MessageID: record.messageID,
				ClientID:  clientID,
				Text:      inbound.Text,
				Timestamp: inbound.Timestamp,
			},
		})
		return record.messageID, http.StatusAccepted, nil
	default:
		// Gate drops are final. Retrying cannot turn a denied identity into an
		// allowed one and would create a busy loop.
		record.status = http.StatusForbidden
		return record.messageID, record.status, nil
	}
}

func validSendText(text string) bool {
	return strings.TrimSpace(text) != ""
}
