package ipc

const (
	RenderCapable   = "capable"
	RenderProbing   = "probing"
	RenderQueueOnly = "queue_only"
)

// RenderRoute describes host delivery, independently of broker connectivity.
type RenderRoute struct {
	State  string `json:"render_state"`
	Reason string `json:"render_reason,omitempty"`
}

type RenderStateMsg struct {
	Op Op `json:"op"`
	RenderRoute
}

func (r RenderRoute) Text() string {
	state := "queue-only"
	switch r.State {
	case RenderCapable:
		state = "channel"
	case RenderProbing:
		state = "probing"
	}
	if r.Reason != "" {
		state += " (" + r.Reason + ")"
	}
	return "Live route: " + state + "."
}
