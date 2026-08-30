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
	mu            sync.Mutex
	messageID     int64
	textHash      [32]byte
	kind          string
	status        int
	created       time.Time
	voiceFileID   string
	voiceSize     int64
	voiceDuration float64
}

var errClientIDConflict = errors.New("client_id was already used for different content")

// acceptInbound serializes one client id across retries and stamps every trust-
// boundary field from the authenticated session and channel config.
func (c *Channel) acceptInbound(sessionID string, current session, text, clientID string) (int64, int, error) {
	key := clientMessageKey{sessionID: sessionID, clientID: clientID}
	now := c.now()
	hash := textHash(text)
	pruned := false
	c.clientMu.Lock()
	for existingKey, existing := range c.clientMessages {
		if now.Sub(existing.created) >= clientMessageTTL {
			delete(c.clientMessages, existingKey)
			pruned = true
		}
	}
	record := c.clientMessages[key]
	c.clientMu.Unlock()
	if record == nil {
		candidate := &clientMessage{messageID: c.nextInboundMessageID(), textHash: hash, created: now}
		c.clientMu.Lock()
		record = c.clientMessages[key]
		if record == nil {
			record = candidate
			c.clientMessages[key] = record
		}
		c.clientMu.Unlock()
	}

	record.mu.Lock()
	if record.kind != "" || record.textHash != hash {
		record.mu.Unlock()
		if pruned {
			c.persistSessions("stale client-message pruning")
		}
		return record.messageID, http.StatusConflict, errClientIDConflict
	}
	if record.status != 0 {
		messageID, status := record.messageID, record.status
		record.mu.Unlock()
		if pruned {
			c.persistSessions("stale client-message pruning")
		}
		return messageID, status, nil
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
			record.mu.Unlock()
			if pruned {
				c.persistSessions("stale client-message pruning")
			}
			return record.messageID, http.StatusServiceUnavailable, nil
		}
		record.status = http.StatusAccepted
		record.mu.Unlock()
		c.persistSessions("client-message insert")
		c.publish(streamEvent{
			kind: "own",
			payload: streamPayload{
				MessageID: record.messageID,
				ClientID:  clientID,
				Text:      inbound.Text,
				Timestamp: streamTimestamp(inbound.Timestamp),
			},
		})
		return record.messageID, http.StatusAccepted, nil
	default:
		// Gate drops are final. Retrying cannot turn a denied identity into an
		// allowed one and would create a busy loop.
		record.status = http.StatusForbidden
		messageID, status := record.messageID, record.status
		record.mu.Unlock()
		c.persistSessions("client-message insert")
		return messageID, status, nil
	}
}

func (c *Channel) nextInboundMessageID() int64 {
	c.idMu.Lock()
	c.nextInboundFloor++
	messageID := c.nextInboundFloor
	c.idMu.Unlock()
	c.persistSessions("inbound-id advance")
	return messageID
}

func validSendText(text string) bool {
	return strings.TrimSpace(text) != ""
}
