package main

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/ipc"
)

func TestToolForward_ProductionConnUnavailableUntilHelloAck(t *testing.T) {
	a := newAdapter()
	adapterSide, brokerSide := net.Pipe()
	t.Cleanup(func() {
		_ = adapterSide.Close()
		_ = brokerSide.Close()
	})
	conn := ipc.NewConn(adapterSide)
	peer := newBrokerPeer(brokerSide)

	// Model connectBroker's production publication. Direct test-installed
	// connections leave connHelloPending at its zero value and remain usable.
	a.prepareSessionRecoverForConn(context.Background(), conn)
	a.bmu.Lock()
	a.conn = conn
	a.connHelloPending = true
	a.bmu.Unlock()

	req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
		Name:      "reply",
		Arguments: json.RawMessage(`{"text":"hello"}`),
	}}
	type callResult struct {
		result *mcp.CallToolResult
		err    error
	}
	preHelloDone := make(chan callResult, 1)
	go func() {
		result, err := a.toolForward("reply")(context.Background(), req)
		preHelloDone <- callResult{result: result, err: err}
	}()
	select {
	case got := <-preHelloDone:
		if got.err != nil {
			t.Fatalf("pre-hello toolForward error: %v", got.err)
		}
		if !got.result.IsError {
			t.Fatal("pre-hello toolForward reported success")
		}
	case raw := <-peer.frames:
		var call ipc.ToolCallReq
		if err := json.Unmarshal(raw, &call); err == nil {
			response, _ := json.Marshal(ipc.ToolResultMsg{
				Op: ipc.OpToolResult, ID: call.ID, Result: map[string]any{"ok": true},
			})
			a.dispatchToolResult(response)
		}
		t.Fatalf("ordinary tool frame overtook HelloAck readiness: %s", raw)
	case <-time.After(time.Second):
		t.Fatal("pre-hello toolForward did not refuse promptly")
	}

	helloDone := make(chan error, 1)
	go func() { helloDone <- a.hello() }()
	raw, ok := peer.next(t, time.Second)
	if !ok {
		t.Fatal("hello frame was not sent on the raw connection")
	}
	if op := frameOp(t, raw); op != ipc.OpHello {
		t.Fatalf("pre-ack frame op = %q, want %q", op, ipc.OpHello)
	}
	if err := peer.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpError}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-helloDone:
		if err == nil {
			t.Fatal("hello accepted a response that was not HelloAck")
		}
	case <-time.After(time.Second):
		t.Fatal("hello did not reject the wrong response op")
	}
	if a.currentConn() != nil {
		t.Fatal("wrong hello response published the connection as ready")
	}

	go func() { helloDone <- a.hello() }()
	raw, ok = peer.next(t, time.Second)
	if !ok {
		t.Fatal("retry hello frame was not sent on the raw connection")
	}
	if op := frameOp(t, raw); op != ipc.OpHello {
		t.Fatalf("retry frame op = %q, want %q", op, ipc.OpHello)
	}
	if err := peer.WriteJSON(ipc.HelloAckMsg{
		Op: ipc.OpHelloAck, ConnID: 7, ProtocolVersion: ipc.ProtocolVersion,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-helloDone:
		if err != nil {
			t.Fatalf("hello failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("hello did not finish after HelloAck")
	}

	callDone := make(chan *mcp.CallToolResult, 1)
	go func() {
		result, _ := a.toolForward("reply")(context.Background(), req)
		callDone <- result
	}()
	raw, ok = peer.next(t, time.Second)
	if !ok {
		t.Fatal("toolForward did not write after HelloAck readiness")
	}
	var call ipc.ToolCallReq
	if err := json.Unmarshal(raw, &call); err != nil {
		t.Fatalf("decode post-hello tool frame: %v", err)
	}
	if call.Op != ipc.OpToolCall || call.Name != "reply" {
		t.Fatalf("post-hello tool frame = %+v", call)
	}
	response, err := json.Marshal(ipc.ToolResultMsg{
		Op: ipc.OpToolResult, ID: call.ID, Result: map[string]any{"ok": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	a.dispatchToolResult(response)
	select {
	case result := <-callDone:
		if result == nil || result.IsError {
			t.Fatalf("post-hello toolForward result = %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("post-hello toolForward did not complete")
	}
}

func TestHello_OldAckCannotPublishReplacementConnection(t *testing.T) {
	a := newAdapter()
	oldAdapter, oldBroker := net.Pipe()
	newAdapter, newBroker := net.Pipe()
	t.Cleanup(func() {
		_ = oldAdapter.Close()
		_ = oldBroker.Close()
		_ = newAdapter.Close()
		_ = newBroker.Close()
	})
	oldConn := ipc.NewConn(oldAdapter)
	newConn := ipc.NewConn(newAdapter)
	peer := ipc.NewConn(oldBroker)
	a.bmu.Lock()
	a.conn = oldConn
	a.connHelloPending = true
	a.bmu.Unlock()

	helloSeen := make(chan struct{})
	allowAck := make(chan struct{})
	go func() {
		_, _ = peer.ReadFrame()
		close(helloSeen)
		<-allowAck
		_ = peer.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck})
	}()
	helloDone := make(chan error, 1)
	go func() { helloDone <- a.hello() }()
	select {
	case <-helloSeen:
	case <-time.After(time.Second):
		t.Fatal("hello frame was not sent")
	}

	a.bmu.Lock()
	a.conn = newConn
	a.connHelloPending = true
	a.bmu.Unlock()
	close(allowAck)
	if err := <-helloDone; err == nil {
		t.Fatal("old connection's HelloAck published its replacement as usable")
	}
	if got := a.currentConn(); got != nil {
		t.Fatalf("replacement connection %p became usable from old connection's HelloAck", got)
	}
}
