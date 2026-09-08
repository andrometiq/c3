package broker

import (
	"encoding/json"
	"log"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

func (b *Broker) handleRenderState(stub *Stub, raw []byte) {
	var msg ipc.RenderStateMsg
	if err := json.Unmarshal(raw, &msg); err != nil || msg.State == "" {
		return
	}
	before := stub.RenderRoute()
	stub.SetRenderRoute(msg.State, msg.Reason, true)
	if before != stub.RenderRoute() {
		b.notifyRenderRoute(stub)
	}
}

// Route notices reuse the held coalescer and fire once per session/route. Queue
// status uses the in-memory index, never a concurrent queue-file read.
func (b *Broker) notifyRenderRoute(stub *Stub) {
	route := stub.RenderRoute()
	if route.State == ipc.RenderCapable {
		return
	}
	for _, key := range stub.Routes() {
		if !stub.takeRenderNotice(key) {
			continue
		}
		if b.HeldNotices != nil && !b.HeldNotices.ShouldSend(key) {
			continue
		}
		text := route.Text() + " Messages remain available through fetch_queue."
		if b.Queue != nil {
			count := b.Queue.StatusFor(queueRouteKey(key)).Pending
			if count > 0 {
				text = heldReplyText(key.Channel, count) + "\n\n" + route.Text()
			}
		} else {
			text += "\n" + queueDisabledWarning
		}
		// Channel calls must not delay the attach result or IPC reader.
		go func(key RouteKey, text string) {
			ch, err := b.Channel(key.Channel)
			if err != nil {
				return
			}
			var topic *int64
			if key.HasTopic {
				id := key.TopicID
				topic = &id
			}
			if _, err := ch.SendReply(c3types.ReplyArgs{Channel: key.Channel, ChatID: key.ChatID, TopicID: topic, Text: text}); err != nil {
				log.Printf("live route notice failed: %v", err)
			}
		}(key, text)
	}
}
