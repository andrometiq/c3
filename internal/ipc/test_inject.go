package ipc

// TestInject is deliberately separate from Inbound: callers cannot choose a
// real channel, update offset, event kind, delivery token, or durable identity.
const OpTestInject Op = "test_inject"

type TestInjectReq struct {
	Op           Op     `json:"op"`
	Topic        int64  `json:"topic"`
	Text         string `json:"text"`
	From         int64  `json:"from,omitempty"`
	Kind         string `json:"kind,omitempty"`
	Count        int    `json:"count,omitempty"`
	VoiceDelayMS int    `json:"voice_delay_ms,omitempty"`
}

type TestInjectResp struct {
	Op         Op      `json:"op"`
	Accepted   bool    `json:"accepted"`
	MessageIDs []int64 `json:"message_ids,omitempty"`
	Err        string  `json:"err,omitempty"`
}
