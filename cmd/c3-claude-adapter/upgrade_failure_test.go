package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type upgradeObservedWrite struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *upgradeObservedWrite) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(p)
}
func blockedUpgradeBroker(t *testing.T, a *adapter) <-chan struct{} {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	observed := &upgradeObservedWrite{Conn: client, entered: make(chan struct{})}
	a.conn = ipc.NewConn(observed)
	return observed.entered
}
func TestUpgradeFetchWriteCancellation(t *testing.T) {
	isolateAdapterTest(t)
	a := newAdapter()
	entered := blockedUpgradeBroker(t, a)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.toolFetchQueue(ctx, &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Arguments: json.RawMessage(`{}`)}})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("fetch never wrote")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled fetch stuck writing to broker")
	}
}

func TestUpgradeSDKFatalChild(t *testing.T) {
	isolateAdapterTest(t)
	if os.Getenv("TEST_C3_SDK_FATAL") != "1" {
		t.Skip("subprocess failure fixture")
	}
	a := newAdapter()
	a.upgrade.resumed = true
	entered := blockedUpgradeBroker(t, a)
	pair := connectUpgradeSDK(t, a, a.buildMCPServer(), &mcp.ServerSessionOptions{State: &mcp.ServerSessionState{InitializeParams: &mcp.InitializeParams{ProtocolVersion: "2025-03-26"}, InitializedParams: &mcp.InitializedParams{}}})
	if err := pair.conn.Write(pair.ctx, &jsonrpc.Request{ID: mustID(t, "blocked-fetch"), Method: "tools/call", Params: json.RawMessage(`{"name":"fetch_queue","arguments":{}}`)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-pair.ctx.Done():
		t.Fatal("fetch never blocked")
	}
	t.Fatal("intentional SDK assertion failure while broker write is blocked")
}
func TestUpgradeSDKFatalCleanup(t *testing.T) {
	isolateAdapterTest(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestUpgradeSDKFatalChild$", "-test.count=1", "-test.timeout=3s")
	cmd.Env = append(os.Environ(), "TEST_C3_SDK_FATAL=1")
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil || err == nil || !strings.Contains(string(out), "intentional SDK assertion failure") || strings.Contains(string(out), "test timed out") {
		t.Fatalf("failed assertion did not exit normally: %v\n%s", err, out)
	}
}
