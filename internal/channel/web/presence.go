package web

import (
	"time"

	"github.com/Andrometiq/c3/internal/channel"
)

// SetRouteHolder implements channel.PresenceNotifier for web's one operator
// route. Presence is memory-only and its SSE event is deliberately live-only.
func (c *Channel) SetRouteHolder(chatID int64, topicID *int64, holder *channel.RouteHolder) {
	if chatID != c.operatorID || topicID != nil {
		return
	}
	var current *channel.RouteHolder
	if holder != nil {
		copy := *holder
		current = &copy
	}
	c.presenceMu.Lock()
	c.holder = current
	c.presenceMu.Unlock()
	c.publish(c.presenceEvent(false))
}

func (c *Channel) presenceEvent(replay bool) streamEvent {
	c.presenceMu.RLock()
	var holder *channel.RouteHolder
	if c.holder != nil {
		copy := *c.holder
		holder = &copy
	}
	c.presenceMu.RUnlock()
	attached := holder != nil
	payload := streamPayload{Attached: &attached, Replay: replay}
	if holder != nil {
		payload.CLI = holder.CLI
		payload.CWD = holder.CWD
		payload.PID = holder.PID
		payload.SessionID = holder.SessionID
		if !holder.Since.IsZero() {
			payload.Since = holder.Since.UTC().Format(time.RFC3339)
		}
	}
	return streamEvent{kind: "presence", payload: payload}
}
