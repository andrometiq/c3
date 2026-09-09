package broker

import (
	"encoding/json"
	"log"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

func (b *Broker) handleRenderState(stub *Stub, raw []byte) {
	if stub.negotiated() {
		return
	}
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

// Route notices share the Held cooldown, with one sending loop per route.
// State changes coalesce while waiting, so a recent ordinary Held notice cannot
// permanently suppress the route's latest state.
func (b *Broker) notifyRenderRoute(stub *Stub) {
	for _, key := range stub.Routes() {
		if stub.scheduleRenderNotice(key) {
			go b.sendRenderNotice(stub, key)
		}
	}
}

func (b *Broker) sendRenderNotice(stub *Stub, key RouteKey) {
	for {
		for b.HeldNotices != nil && !b.HeldNotices.ShouldSend(key) {
			timer := time.NewTimer(b.HeldNotices.remaining(key))
			select {
			case <-b.ctx.Done():
				timer.Stop()
				stub.finishRenderNotice(key, ipc.RenderRoute{}, false)
				return
			case <-timer.C:
			}
		}
		// Do not emit stale notices after release or holder replacement.
		if holder, _ := b.Routes.Holder(key); holder != stub {
			stub.finishRenderNotice(key, ipc.RenderRoute{}, false)
			return
		}
		route, needed := stub.nextRenderNotice(key)
		if !needed {
			return
		}
		text := route.Text() + " Messages remain available through fetch_queue."
		if b.Queue != nil {
			if count := max(0, b.Queue.StatusFor(queueRouteKey(key)).Pending-b.attemptingCount(key)); count > 0 {
				text = heldReplyText(key.Channel, count) + "\n\n" + route.Text()
			}
		} else {
			text += "\n" + queueDisabledWarning
		}
		ch, err := b.Channel(key.Channel)
		if err != nil {
			stub.finishRenderNotice(key, route, false)
			return
		}
		var topic *int64
		if key.HasTopic {
			id := key.TopicID
			topic = &id
		}
		_, err = ch.SendReply(c3types.ReplyArgs{Channel: key.Channel, ChatID: key.ChatID, TopicID: topic, Text: text})
		if err != nil {
			log.Printf("live route notice failed: %v", err)
		}
		if !stub.finishRenderNotice(key, route, err == nil) {
			return
		}
	}
}
