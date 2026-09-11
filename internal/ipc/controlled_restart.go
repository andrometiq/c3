package ipc

const (
	OpBrokerRestart      Op = "broker_restart"
	OpBrokerRestartReply Op = "broker_restart_reply"
)

type BrokerRestartReq struct {
	Op Op `json:"op"`
}
type BrokerRestartReply struct {
	Op  Op     `json:"op"`
	OK  bool   `json:"ok"`
	Err string `json:"err,omitempty"`
}
