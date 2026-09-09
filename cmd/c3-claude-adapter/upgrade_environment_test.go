//go:build linux || darwin

package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Reproduce the unexpected frame from the maintainer's dump using only a
// private handoff and listening inbox. Recovery after self-exec is intentional:
// the replacement must register its identity with the broker before tool calls.
func TestResumeSDKSessionEnvironment(t *testing.T) {
	isolateAdapterTest(t)
	inbox, _ := fakeInbox(t, nil, "")
	t.Setenv("CLAUDE_CODE_MESSAGING_SOCKET", inbox.socketPath)
	t.Setenv("CLAUDE_CODE_MESSAGING_TOKEN", inbox.token)
	const id = "11111111-2222-4333-8444-555555555555"
	t.Setenv("CLAUDE_CODE_SESSION_ID", id)
	a, peer := adapterWithConn(t)
	if err := sessionhandoff.Write(id, sessionhandoff.Entry{StableSessionID: id, TranscriptPath: a.livePath(), UnixNano: time.Now().UnixNano()}); err != nil {
		t.Fatal(err)
	}
	state := upgradeResumeState{Contract: upgradeContract(), MCP: mcp.ServerSessionState{InitializeParams: &mcp.InitializeParams{ProtocolVersion: "2025-03-26"}, InitializedParams: &mcp.InitializedParams{}}}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(upgradeResumeEnv, base64.StdEncoding.EncodeToString(raw))
	opts, err := a.restoreUpgrade([]string{"--mcp-resume"})
	if err != nil {
		t.Fatal(err)
	}
	pair := connectUpgradeSDK(t, a, a.buildMCPServer(), opts)
	done := make(chan error, 1)
	go func() {
		for _, want := range []ipc.Op{ipc.OpRecoverSession, ipc.OpFetchQueue} {
			raw, err := peer.ReadFrame()
			if err != nil {
				done <- err
				return
			}
			op, err := ipc.PeekOp(raw)
			if err != nil || op != want {
				done <- fmt.Errorf("got %s, want %s: %v", op, want, err)
				pair.abort()
				return
			}
			if op == ipc.OpRecoverSession {
				var request ipc.RecoverSessionReq
				if err := json.Unmarshal(raw, &request); err != nil || request.StableSessionID != id {
					done <- fmt.Errorf("wrong recovery identity: %v", err)
					pair.abort()
					return
				}
				reply, _ := json.Marshal(ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult})
				a.dispatchRecoverSessionResult(reply)
			} else {
				var request ipc.FetchQueueReq
				if err := json.Unmarshal(raw, &request); err != nil {
					done <- err
					return
				}
				reply, _ := json.Marshal(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: request.ID})
				a.dispatchFetchQueueResult(reply)
			}
		}
		done <- nil
	}()
	if err := pair.conn.Write(pair.ctx, &jsonrpc.Request{ID: mustID(t, "resumed-fetch"), Method: "tools/call", Params: json.RawMessage(`{"name":"fetch_queue","arguments":{}}`)}); err != nil {
		t.Fatal(err)
	}
	msg, err := pair.conn.Read(pair.ctx)
	if err != nil {
		t.Fatal(err)
	}
	response, ok := msg.(*jsonrpc.Response)
	if !ok || response.Error != nil {
		t.Fatalf("resumed fetch: %#v", msg)
	}
	var result mcp.CallToolResult
	if err := json.Unmarshal(response.Result, &result); err != nil || result.IsError {
		t.Fatalf("resumed result: %s %v", response.Result, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if current, settled := a.currentStableIdentity(); !settled || current.StableSessionID != id {
		t.Fatal("resume did not register identity")
	}
	if os.Getenv("CLAUDE_CODE_SESSION_ID") != id {
		t.Fatal("production resume cleared the host identity")
	}
}
