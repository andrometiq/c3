package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestFetchReceiptBrokerAdapterToolResult(t *testing.T) {
	isolateAdapterTest(t)
	b := broker.New(reconnectSwitchMappings())
	t.Cleanup(b.Shutdown)
	if err := b.RegisterChannel(&reconnectSwitchChannel{}); err != nil {
		t.Fatal(err)
	}
	a, _, _ := liveFixture(t, ipc.RenderQueueOnly)
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "no live transport"}
	a.deliveryHostInitialized.Store(true)
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close() })
	a.conn = ipc.NewConn(client)
	go b.HandleConn(server)
	ctx, cancel := context.WithCancel(context.Background())
	a.runCtx = ctx
	if err := a.hello(); err != nil {
		t.Fatal(err)
	}
	if !a.fetchReceiptAccepted() {
		t.Fatal("pull-only receipt mode not confirmed")
	}
	s := b.Stubs.Snapshot()[0]
	var routes []queue.RouteKey
	for _, topic := range []int64{281, 282} {
		key := broker.MakeRouteKey("telegram", -100, &topic)
		b.Routes.Claim(key, s)
		s.AddRoute(key)
		s.MarkRouteConfirmed(key)
		route := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &topic}
		routes = append(routes, route)
		if err := b.Queue.Append(route, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &topic, MessageID: topic, Text: "held", Timestamp: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan struct{})
	go func() { a.brokerReader(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		client.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("reader did not stop")
		}
	})
	req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Arguments: json.RawMessage(`{"limit":"all"}`)}}
	req.Params.Meta = map[string]any{"claudecode/toolUseId": "tool-group"}
	r, err := a.toolFetchQueue(ctx, req)
	if err != nil || r.IsError {
		t.Fatal(r, err)
	}
	text := r.Content[0].(*mcp.TextContent).Text
	if strings.Count(text, "member ") != 2 || !strings.HasSuffix(text, ipc.FetchReceiptEnd) {
		t.Fatal(text)
	}
	for _, route := range routes {
		if b.Queue.StatusFor(route).Pending != 1 {
			t.Fatal("tool return consumed before transcript")
		}
	}
	peek := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Arguments: json.RawMessage(`{"ack":false,"limit":"all"}`)}}
	p, _ := a.toolFetchQueue(ctx, peek)
	if strings.Contains(p.Content[0].(*mcp.TextContent).Text, "[281]") {
		t.Fatal("open receipt fetched twice")
	}
	if err := os.WriteFile(a.livePath(), fetchTranscriptResult("tool-group", text, false), 0600); err != nil {
		t.Fatal(err)
	}
	waitReadiness(t, "group tool receipt did not retire", func() bool {
		for _, route := range routes {
			if b.Queue.StatusFor(route).Pending != 0 {
				return false
			}
		}
		return true
	})
	if !strings.Contains(a.renderDeliveryBacklog(2, nil, "topic"), "2 message(s) for topic \"topic\" are fetchable now") || a.renderDeliveryBacklog(0, nil, "") != "0 message(s) fetchable now." {
		t.Fatal("attach fetchable count missing")
	}
}
