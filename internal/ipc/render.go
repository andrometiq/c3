package ipc

import (
	"time"
)

const (
	RenderCapable      = "capable"
	RenderProbing      = "probing"
	RenderQueueOnly    = "queue_only"
	RenderCrossSession = "cross_session"
)

// RenderRoute describes host delivery, independently of broker connectivity.
type RenderRoute struct {
	AcceptedBy string    `json:"accepted_by,omitempty"`
	Transport  string    `json:"confirmed_transport,omitempty"`
	Confirmed  time.Time `json:"confirmed_at,omitzero"`
	Held       int       `json:"-"`
	State      string    `json:"render_state"`
	Reason     string    `json:"render_reason,omitempty"`
}

type RenderStateMsg struct {
	ReceiptShapeDrift string `json:"receipt_shape_drift,omitempty"`
	Channel           string `json:"channel,omitempty"`
	ChatID            int64  `json:"chat_id,omitempty"`
	TopicID           *int64 `json:"topic_id,omitempty"`
	Op                Op     `json:"op"`
	RenderRoute
}

func (r RenderRoute) Semantic() string { return r.State + ":" + r.Reason }
func (r RenderRoute) Text() string {
	if r.State == "waiting" || r.State == "live_channel" || r.State == "live_inbox" || r.State == "pull_only" {
		text := r.State
		if r.State == "live_channel" {
			text = "live: channel"
		}
		if r.State == "live_inbox" {
			text = "live: inbox"
		}
		if r.State == "pull_only" {
			text = "pull-only"
			if r.Reason != "" {
				text += " (" + r.Reason + ")"
			}
		}
		if !r.Confirmed.IsZero() {
			age := time.Since(r.Confirmed).Round(time.Second)
			if age < 0 {
				age = 0
			}
			if r.State == "pull_only" || r.State == "waiting" {
				transport := r.Transport
				if transport == "" {
					transport = "channel"
				}
				text += ", was " + transport
			}
			milestone := "confirmed"
			if r.AcceptedBy != "" {
				milestone = "accepted by " + r.AcceptedBy
			}
			text += ", " + milestone + " " + age.String() + " ago"
		}
		return "Live route: " + text + "."
	}

	state := "queue-only"
	switch r.State {
	case RenderCapable:
		state = "channel"
	case RenderCrossSession:
		state = "cross-session"
	case RenderProbing:
		state = "probing"
	}
	if r.Reason != "" {
		state += " (" + r.Reason + ")"
	}
	return "Live route: " + state + "."
}
