package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestDeliveryHostReadinessPrerequisite(t *testing.T) {
	isolateAdapterTest(t)
	// G: "notifications/initialized ... AND notify transport present".
	a, _, _ := liveFixture(t, ipc.RenderCapable)
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
	for _, initialized := range []bool{false, true} {
		for _, present := range []bool{false, true} {
			tx := a.notifyTx
			a.deliveryHostInitialized.Store(initialized)
			if !present {
				a.notifyTx = nil
			}
			facts := a.deliveryFacts()
			if facts.Channel.Eligible != (initialized && present) || (!initialized || !present) && (facts.Inbox.Eligible || len(a.deliveryOffer()) != 0) {
				t.Fatalf("initialized=%v present=%v facts=%+v", initialized, present, facts)
			}
			a.notifyTx = tx
		}
	}
}

type deliveryErrorConn struct{ scriptedConn }

func (*deliveryErrorConn) Write(context.Context, jsonrpc.Message) error {
	return errors.New("injected notify write failure")
}

func TestDeliveryNotifyFailureReportsImmediately(t *testing.T) {
	isolateAdapterTest(t)
	// G: "missing notify transport or a failed notify write ... failed immediately".
	// Disable the receipt ticker: an implementation that waits for polling or
	// expiry cannot pass. Both a missing wrapper and a missing inner connection
	// are definitive failures, even with a full 15-second observation budget.
	for _, kind := range []string{"missing", "unconnected", "write_error"} {
		t.Run(kind, func(t *testing.T) {
			a, _, frames, ctx := negotiatedAdapter(t)
			a.deliveryLoopOnce.Do(func() {})
			switch kind {
			case "missing":
				a.notifyTx = nil
			case "unconnected":
				a.notifyTx = newNotifyTransport(&scriptedTransport{conn: &scriptedConn{}})
			case "write_error":
				a.notifyTx = newNotifyTransport(&scriptedTransport{conn: &deliveryErrorConn{}})
				if _, err := a.notifyTx.Connect(ctx); err != nil {
					t.Fatal(err)
				}
			}
			raw, _ := json.Marshal(ipc.DeliverMsg{Op: ipc.OpDeliver, Token: "notify-failed", Transport: "channel", DeadlineMS: 15000})
			a.handleDeliver(ctx, raw)
			select {
			case raw := <-frames:
				var result ipc.AttemptResultMsg
				if json.Unmarshal(raw, &result) != nil || result.Op != ipc.OpAttemptResult || result.Token != "notify-failed" || result.Outcome != "failed" || result.Reason == "" {
					t.Fatal(string(raw))
				}
			case <-time.After(time.Second):
				t.Fatal("failure waited for receipt polling or deadline")
			}
			a.pollDeliveries() // A later poll must not repeat the result.
			select {
			case raw := <-frames:
				if strings.Contains(string(raw), `"op":"attempt_result"`) {
					t.Fatal("duplicate failure", string(raw))
				}
			default:
			}
		})
	}
}

func TestDeliveryHostReadinessInitializedNotification(t *testing.T) {
	isolateAdapterTest(t)
	// An initialize request (or ping) alone does not establish host readiness.
	a, _, _ := liveFixture(t, ipc.RenderCapable)
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
	host := startReadinessHost(t, a)
	sendReadinessInitialize(t, host)
	if a.deliveryHostInitialized.Load() || len(a.deliveryOffer()) != 0 {
		t.Fatal("initialize advertised delivery before initialized")
	}
	if err := host.Write(context.Background(), &jsonrpc.Request{Method: "notifications/initialized"}); err != nil {
		t.Fatal(err)
	}
	waitReadiness(t, "initialized notification not observed", a.deliveryHostInitialized.Load)
	if !a.deliveryFacts().Channel.Eligible {
		t.Fatal("ready host stayed ineligible")
	}
}

func waitReadiness(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(what)
}
func startReadinessHost(t *testing.T, a *adapter) mcp.Connection {
	t.Helper()
	srv := a.buildMCPServer()
	clientT, serverT := mcp.NewInMemoryTransports()
	a.notifyTx = newNotifyTransport(serverT)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Run(ctx, a.notifyTx); close(done) }()
	host, err := clientT.Connect(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		host.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("MCP server failed to stop")
		}
	})
	return host
}
func sendReadinessInitialize(t *testing.T, host mcp.Connection) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := &jsonrpc.Request{ID: mustID(t, float64(1)), Method: "initialize", Params: json.RawMessage(`{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}`)}
	if err := host.Write(ctx, req); err != nil {
		t.Fatal(err)
	}
	msg, err := host.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	response, ok := msg.(*jsonrpc.Response)
	if !ok || response.Error != nil {
		t.Fatalf("initialize response: %+v", msg)
	}
}
