package main

import (
	"context"
	"encoding/json"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"testing"
	"time"
)

func TestAskToolReturnsControlledRestartError(t *testing.T) {
	isolateAdapterTest(t)
	a, peer := adapterWithConn(t)

	resultCh := make(chan *mcp.CallToolResult, 1)
	go func() {
		res, _ := a.toolAsk(context.Background(), askRequest(t, "Pick one", []string{"A", "B", "C"}))
		resultCh <- res
	}()

	// The tool must send an AskRegisterReq carrying the question + options + askID.
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("read ask_register frame: %v", err)
	}
	var reg ipc.AskRegisterReq
	if err := json.Unmarshal(raw, &reg); err != nil {
		t.Fatalf("unmarshal ask_register: %v", err)
	}
	if reg.Op != ipc.OpAskRegister {
		t.Fatalf("op = %q, want %q", reg.Op, ipc.OpAskRegister)
	}
	if reg.Question != "Pick one" || len(reg.Options) != 3 {
		t.Fatalf("unexpected ask_register payload: %+v", reg)
	}
	if reg.AskID == "" {
		t.Fatal("ask_register must carry a generated ask id")
	}

	// Existing error frames reach the waiting tool without a new protocol.
	a.dispatchAskRegistered(mustMarshal(t, ipc.AskRegisteredMsg{
		Op: ipc.OpAskRegistered, AskID: reg.AskID, OK: true, MessageID: 99,
	}))
	a.dispatchAskResult(mustMarshal(t, ipc.AskResultMsg{
		Op: ipc.OpAskResult, AskID: reg.AskID, Err: "C3 is restarting; this request was cancelled — ask again.",
	}))

	select {
	case res := <-resultCh:
		if !res.IsError {
			t.Fatalf("unexpected tool error: %q", resultText(t, res))
		}
		if got := resultText(t, res); got != "ask: C3 is restarting; this request was cancelled — ask again." {
			t.Fatalf("unexpected cancellation result: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ask tool did not return after the answer was pushed")
	}
}
