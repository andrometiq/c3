package main

// transientClient is a one-shot helper for CLI subcommands (`c3-broker
// topics`, `c3-broker status`) that need to query the running daemon
// without registering as a long-lived adapter. Connects, says hello with
// CLI="c3-broker-cli", sends one request, reads one response, closes.
//
// The broker treats this like any other adapter: the stub gets
// registered, ConnID assigned, and on conn drop the new
// claims-survive-PID-alive logic keeps any claims (there shouldn't be
// any here — these helpers don't attach). The OS reaps the PID after
// process exit; broker's PID-liveness check then frees any leftover
// state.

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/ipc"
)

// dialBroker opens a transient connection to the broker socket and
// completes the hello handshake. Returns the wrapped Conn the caller
// reads/writes. On error, the broker is unreachable (not running, socket
// missing, etc.).
func dialBroker() (*ipc.Conn, error) {
	sockPath := broker.SocketPath()
	c, err := net.DialTimeout("unix", sockPath, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w (is the broker running?)", sockPath, err)
	}
	conn := ipc.NewConn(c)
	cwd, _ := os.Getwd()
	if err := conn.WriteJSON(ipc.HelloMsg{
		Op:  ipc.OpHello,
		CLI: "c3-broker-cli",
		PID: os.Getpid(),
		CWD: cwd,
	}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("hello write: %w", err)
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("hello-ack read: %w", err)
	}
	op, err := ipc.PeekOp(raw)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("hello-ack parse: %w", err)
	}
	if op != ipc.OpHelloAck {
		_ = conn.Close()
		return nil, fmt.Errorf("hello-ack unexpected op: %s", op)
	}
	return conn, nil
}

// fetchTopicsList connects, sends OpListTopics, reads the response, closes.
func fetchTopicsList() (*ipc.TopicsListMsg, error) {
	conn, err := dialBroker()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err := conn.WriteJSON(ipc.ListTopicsReq{Op: ipc.OpListTopics}); err != nil {
		return nil, fmt.Errorf("write list_topics: %w", err)
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		return nil, fmt.Errorf("read topics_list: %w", err)
	}
	var msg ipc.TopicsListMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("parse topics_list: %w", err)
	}
	return &msg, nil
}

// fetchClaimsList connects, sends OpListClaims, reads the response, closes.
func fetchClaimsList() (*ipc.ClaimsListMsg, error) {
	conn, err := dialBroker()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err := conn.WriteJSON(ipc.ListClaimsReq{Op: ipc.OpListClaims}); err != nil {
		return nil, fmt.Errorf("write list_claims: %w", err)
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		return nil, fmt.Errorf("read claims_list: %w", err)
	}
	var msg ipc.ClaimsListMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("parse claims_list: %w", err)
	}
	return &msg, nil
}

// fetchHealthList connects, sends OpListHealth, reads the response, closes.
func fetchHealthList() (*ipc.HealthListMsg, error) {
	conn, err := dialBroker()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err := conn.WriteJSON(ipc.ListHealthReq{Op: ipc.OpListHealth}); err != nil {
		return nil, fmt.Errorf("write list_health: %w", err)
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		return nil, fmt.Errorf("read health_list: %w", err)
	}
	var msg ipc.HealthListMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("parse health_list: %w", err)
	}
	return &msg, nil
}
