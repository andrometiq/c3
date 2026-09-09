//go:build linux || darwin

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

func TestNegotiatedInboxBrokerAdapterLifecycle(t *testing.T) {
	isolateAdapterTest(t)
	// P3/P6: broker fallback gets a new token, adapter sends the existing inbox
	// frames, and only attempt_result from the peer receipt retires the real row.
	for _, tc := range []struct {
		first string
		index int
	}{{"channel", 0}, {"inbox", 0}, {"channel", 2}, {"inbox", 2}} {
		first := tc.first
		t.Run(fmt.Sprintf("%s/record-%d", first, tc.index), func(t *testing.T) {
			for _, name := range []string{"C3_QUEUE_DIR", "XDG_CONFIG_HOME", "XDG_STATE_HOME"} {
				t.Setenv(name, t.TempDir())
			}
			t.Setenv("CLAUDE_CODE_SESSION_ID", "")
			b := newTestBroker(t, reconnectSwitchMappings())
			t.Cleanup(b.Shutdown)
			if err := b.RegisterChannel(&reconnectSwitchChannel{}); err != nil {
				t.Fatal(err)
			}
			client, server := net.Pipe()
			t.Cleanup(func() { client.Close() })
			go b.HandleConn(server)
			a, _, _ := liveFixture(t, ipc.RenderCapable)
			a.deliveryHostInitialized.Store(true)
			a.conn = ipc.NewConn(client)
			a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "no dev-channels flag on host"}
			if first == "channel" {
				a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
			}
			tx, pushes := deliveryInbox(t, false)
			a.crossSession = tx
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			a.runCtx = ctx
			if err := a.hello(); err != nil {
				t.Fatal(err)
			}
			if !a.deliveryAccepted.Load() {
				t.Fatal("bilateral inbox acceptance failed")
			}
			stub := b.Stubs.Snapshot()[0]
			topic := int64(281)
			key := broker.MakeRouteKey("telegram", -100, &topic)
			b.Routes.Claim(key, stub)
			stub.AddRoute(key)
			stub.MarkRouteConfirmed(key)
			stub.SetOutputRoute(key)
			route := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &topic}
			if !b.Workers.Submit(key, broker.Job{Kind: broker.JobInbound, Inbound: &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &topic, MessageID: 1, Text: "durable inbox", Timestamp: time.Now()}}) {
				t.Fatal("worker refused")
			}
			var tokens []string
			for {
				client.SetReadDeadline(time.Now().Add(4 * time.Second))
				raw, err := a.conn.ReadFrame()
				if err != nil {
					t.Fatal(err)
				}
				op, _ := ipc.PeekOp(raw)
				if op == ipc.OpRenderState {
					a.handleDeliveryState(raw)
					continue
				}
				if op != ipc.OpDeliver {
					t.Fatal(string(raw))
				}
				var f ipc.DeliverMsg
				json.Unmarshal(raw, &f)
				tokens = append(tokens, f.Token)
				if f.Transport == "channel" {
					// Fail an established notify connection without changing readiness.
					a.notifyTx.Disconnect()
				}
				a.handleDeliver(ctx, raw)
				if f.Transport == "inbox" {
					break
				}
			}
			if (first == "channel" && (len(tokens) != 2 || tokens[0] == tokens[1])) || (first == "inbox" && len(tokens) != 1) {
				t.Fatal("fallback identity", tokens)
			}
			p := nextInbox(t, pushes)
			if n, _ := b.Queue.Pending(route); n != 1 {
				t.Fatal("write retired row")
			}
			// Preserve the captured host envelope, inserting the actual wire block.
			var record map[string]any
			if err := json.Unmarshal(realHostPeerIntakeReceipt(t, tc.index, "unused", "unused"), &record); err != nil {
				t.Fatal(err)
			}
			if tc.index == 0 {
				record["content"] = p.User.Message.Content
			} else {
				record["attachment"].(map[string]any)["prompt"] = p.User.Message.Content
			}
			receipt, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			f, err := os.OpenFile(a.livePath(), os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.Write(append(receipt, '\n'))
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				client.SetReadDeadline(deadline)
				raw, err := a.conn.ReadFrame()
				if err != nil {
					t.Fatal(err)
				}
				if op, _ := ipc.PeekOp(raw); op == ipc.OpRenderState {
					a.handleDeliveryState(raw)
				}
				if strings.Contains(a.liveRoutePreamble(), "live: inbox, confirmed") {
					break
				}
			}
			if n, _ := b.Queue.Pending(route); n != 0 {
				t.Fatal("receipt failed retirement")
			}
			if !strings.Contains(a.liveRoutePreamble(), "live: inbox, confirmed") {
				t.Fatal("missing broker inbox display")
			}
			client.SetReadDeadline(time.Time{})
		})
	}
}
