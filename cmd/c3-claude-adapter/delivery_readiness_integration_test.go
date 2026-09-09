//go:build linux || darwin

package main

import (
	"context"
	"encoding/json"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

func TestDeliveryStartupHelloBeforeHostInitialized(t *testing.T) {
	isolateAdapterTest(t)
	// G: "hello_ack before host initialize → no attempt; after initialized →
	// re-hello → offer → deliver works", with backlog already durable at hello.
	for _, transport := range []string{"channel", "inbox"} {
		t.Run(transport, func(t *testing.T) {
			for _, name := range []string{"C3_QUEUE_DIR", "XDG_CONFIG_HOME", "XDG_STATE_HOME"} {
				t.Setenv(name, t.TempDir())
			}
			t.Setenv("CLAUDE_CODE_SESSION_ID", "")
			b := broker.New(reconnectSwitchMappings())
			t.Cleanup(b.Shutdown)
			if err := b.RegisterChannel(&reconnectSwitchChannel{}); err != nil {
				t.Fatal(err)
			}
			a := newAdapter()
			path := seedLiveTranscript(t, a)
			a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
			var pushes <-chan inboxPush
			if transport == "inbox" {
				tx, p := deliveryInbox(t, false)
				a.crossSession = tx
				pushes = p
				a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "no dev-channels flag on host"}
			}
			topic := int64(281)
			key := broker.MakeRouteKey("telegram", -100, &topic)
			route := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &topic}
			in := c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &topic, MessageID: 1, Text: "startup backlog", Timestamp: time.Now()}
			if _, err := b.Queue.AppendTracked(route, &in); err != nil {
				t.Fatal(err)
			}
			var dials atomic.Int32
			a.connectBrokerFn = func() error {
				client, server := net.Pipe()
				t.Cleanup(func() { client.Close() })
				go b.HandleConn(server)
				a.bmu.Lock()
				a.conn = ipc.NewConn(client)
				a.helloPending = true
				a.bmu.Unlock()
				dials.Add(1)
				return nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			a.runCtx = ctx
			if err := a.connectBrokerFn(); err != nil {
				t.Fatal(err)
			}
			if err := a.hello(); err != nil {
				t.Fatal(err)
			}
			if a.deliveryAccepted.Load() || len(a.deliveryOffer()) != 0 {
				t.Fatal("startup hello offered before host readiness")
			}
			old := b.Stubs.Snapshot()[0]
			if old.CanRenderPush() {
				t.Fatal("startup hello enabled a legacy push before readiness")
			}
			b.Routes.Claim(key, old)
			old.AddRoute(key)
			old.MarkRouteConfirmed(key)
			old.SetOutputRoute(key)
			b.Workers.Submit(key, broker.Job{Kind: broker.JobAttemptWake})
			host := startReadinessHost(t, a)
			done := make(chan struct{})
			go func() { a.brokerReader(ctx); close(done) }()
			t.Cleanup(func() {
				cancel()
				if c := a.rawConn(); c != nil {
					c.Close()
				}
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("broker reader failed to stop")
				}
			})
			assertWaiting := func() {
				t.Helper()
				time.Sleep(150 * time.Millisecond)
				a.liveMu.Lock()
				attempts := len(a.deliveryObservers)
				a.liveMu.Unlock()
				if attempts != 0 || a.deliveryAccepted.Load() || dials.Load() != 1 {
					t.Fatal("backlog attempted before initialized")
				}
				if n, _ := b.Queue.Pending(route); n != 1 {
					t.Fatal("backlog disappeared before readiness")
				}
			}
			assertWaiting()
			sendReadinessInitialize(t, host)
			assertWaiting()
			if err := host.Write(ctx, &jsonrpc.Request{Method: "notifications/initialized"}); err != nil {
				t.Fatal(err)
			}
			waitReadiness(t, "readiness never re-helloed", a.deliveryAccepted.Load)
			if dials.Load() != 2 {
				t.Fatal("unexpected reconnect count")
			}
			if transport == "inbox" {
				p := nextInbox(t, pushes)
				appendPeerReceipt(t, path, p.User.Message.Content)
			} else {
				readCtx, stop := context.WithTimeout(ctx, 3*time.Second)
				defer stop()
				msg, err := host.Read(readCtx)
				if err != nil {
					t.Fatal(err)
				}
				note, ok := msg.(*jsonrpc.Request)
				if !ok || note.Method != "notifications/claude/channel" {
					t.Fatalf("delivery: %+v", msg)
				}
				var params struct{ Meta map[string]string }
				if err := json.Unmarshal(note.Params, &params); err != nil {
					t.Fatal(err)
				}
				token := params.Meta["c3_delivery_id"]
				a.liveMu.Lock()
				o := a.deliveryObservers[token]
				a.liveMu.Unlock()
				if token == "" || o == nil {
					t.Fatal("backlog used legacy path")
				}
				appendChannelReceipt(t, path, token)
			}
			waitReadiness(t, "ready delivery not retired", func() bool { n, _ := b.Queue.Pending(route); return n == 0 })
		})
	}
}
