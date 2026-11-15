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

// clientMessageKey names one client message. Scope is "" for the web chat and
// the board name for a board's act.
type clientMessageKey struct {
	scope     string
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

// inboundRoute is everything acceptInbound stamps that differs between the web
// chat and a board: the route, the dedup records and whether they persist, the
// message-id source, the line the session reads, and the sender's own copy.
type inboundRoute struct {
	host    channel.Host
	channel string
	chatID  int64
	records map[clientMessageKey]*clientMessage // guarded by clientMu
	persist func(reason string)
	nextID  func() int64
	line    func(*c3types.Inbound) // sets Text (and ReplyTo) once MessageID is known; nil sends the content as is
	echo    func(inbound *c3types.Inbound, clientID string)
}

func (c *Channel) webChatInbound() inboundRoute {
	return inboundRoute{
		host: c.host, channel: Name, chatID: c.operatorID,
		records: c.clientMessages, persist: c.persistSessions, nextID: c.nextInboundMessageID,
		echo: c.echoOwnMessage,
	}
}

// acceptInbound serializes one client id across retries and stamps every trust-
// boundary field from the authenticated sender and the route.
func (c *Channel) acceptInbound(route inboundRoute, key clientMessageKey, sender c3types.Sender, content string) (int64, int, error) {
	now := c.now()
	hash := textHash(content)
	pruned := false
	c.clientMu.Lock()
	for existingKey, existing := range route.records {
		if now.Sub(existing.created) >= clientMessageTTL {
			delete(route.records, existingKey)
			pruned = true
		}
	}
	record := route.records[key]
	c.clientMu.Unlock()
	if record == nil {
		candidate := &clientMessage{messageID: route.nextID(), textHash: hash, created: now}
		c.clientMu.Lock()
		record = route.records[key]
		if record == nil {
			record = candidate
			route.records[key] = record
		}
		c.clientMu.Unlock()
	}

	record.mu.Lock()
	if record.kind != "" || record.textHash != hash {
		record.mu.Unlock()
		if pruned {
			route.persist("stale client-message pruning")
		}
		return record.messageID, http.StatusConflict, errClientIDConflict
	}
	if record.status != 0 {
		messageID, status := record.messageID, record.status
		record.mu.Unlock()
		if pruned {
			route.persist("stale client-message pruning")
		}
		return messageID, status, nil
	}
	inbound := &c3types.Inbound{
		Channel: route.channel, ChatID: route.chatID, TopicID: nil,
		MessageID: record.messageID,
		Sender:    sender,
		Text:      content,
		Timestamp: now,
		ConvKind:  c3types.ConvKindDM,
		Kind:      c3types.InboundMessage,
		V:         c3types.InboundRecordVersion,
	}
	if route.line != nil {
		route.line(inbound)
	}
	switch route.host.GateInbound(inbound) {
	case channel.GateInboundAllow:
		if !route.host.Emit(inbound) {
			record.mu.Unlock()
			if pruned {
				route.persist("stale client-message pruning")
			}
			return record.messageID, http.StatusServiceUnavailable, nil
		}
		record.status = http.StatusAccepted
		record.mu.Unlock()
		route.persist("client-message insert")
		route.echo(inbound, key.clientID)
		return record.messageID, http.StatusAccepted, nil
	default:
		// Gate drops are final. Retrying cannot turn a denied identity into an
		// allowed one and would create a busy loop.
		record.status = http.StatusForbidden
		messageID, status := record.messageID, record.status
		record.mu.Unlock()
		route.persist("client-message insert")
		return messageID, status, nil
	}
}

// hasClientMessage reports whether key already names a message inside the
// dedup window, so acceptInbound will treat it as a retry, not a new message.
func (c *Channel) hasClientMessage(records map[clientMessageKey]*clientMessage, key clientMessageKey) bool {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	record := records[key]
	return record != nil && c.now().Sub(record.created) < clientMessageTTL
}

// echoOwnMessage is the web chat's copy of what the operator sent.
func (c *Channel) echoOwnMessage(inbound *c3types.Inbound, clientID string) {
	c.publish(streamEvent{
		kind: "own",
		payload: streamPayload{
			MessageID: inbound.MessageID,
			ClientID:  clientID,
			Text:      c3types.WithTestInjectionMarker(inbound, inbound.Text),
			Timestamp: streamTimestamp(inbound.Timestamp),
		},
	})
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
