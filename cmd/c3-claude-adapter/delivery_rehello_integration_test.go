//go:build linux || darwin

package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

func waitRehelloCondition(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(what)
}

func TestDeliveryRehelloFreshSessionNegotiates(t *testing.T) {
	isolateAdapterTest(t)
	// F: "fresh session hello (no transcript) → legacy; transcript appears →
	// re-hello with offer → hello_ack accepts → next inbound ... negotiated deliver".
	for _, appears := range []string{"transcript", "socket"} {
		t.Run(appears, func(t *testing.T) {
			for _, name := range []string{"C3_QUEUE_DIR", "XDG_CONFIG_HOME", "XDG_STATE_HOME"} {
				t.Setenv(name, t.TempDir())
			}
			t.Setenv("CLAUDE_CODE_SESSION_ID", "")
			b := broker.New(reconnectSwitchMappings())
			t.Cleanup(b.Shutdown)
			if err := b.RegisterChannel(&reconnectSwitchChannel{}); err != nil {
				t.Fatal(err)
			}
			a, out, _ := liveFixture(t, ipc.RenderCapable)
			a.deliveryHostInitialized.Store(true) // This fixture isolates transcript/socket availability.
			path := a.livePath()
			a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
			var pushes <-chan inboxPush
			var restore func()
			if appears == "transcript" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				restore = func() {
					if err := os.WriteFile(path, nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				tx, p := deliveryInbox(t, false)
				pushes = p
				a.crossSession = tx
				a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "no dev-channels flag on host"}
				if err := os.Rename(tx.socketPath, tx.socketPath+".away"); err != nil {
					t.Fatal(err)
				}
				restore = func() {
					if err := os.Rename(tx.socketPath+".away", tx.socketPath); err != nil {
						t.Fatal(err)
					}
				}
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
				t.Fatal("fresh ineligible session negotiated")
			}
			old := b.Stubs.Snapshot()[0]
			topic := int64(281)
			key := broker.MakeRouteKey("telegram", -100, &topic)
			b.Routes.Claim(key, old)
			old.AddRoute(key)
			old.MarkRouteConfirmed(key)
			old.SetOutputRoute(key)
			done := make(chan struct{})
			go func() { a.brokerReader(ctx); close(done) }()
			t.Cleanup(func() {
				cancel()
				if c := a.rawConn(); c != nil {
					c.Close()
				}
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("reader failed to stop")
				}
			})
			restore()
			waitRehelloCondition(t, "eligible session never negotiated", a.deliveryAccepted.Load)
			next, _ := b.Routes.Holder(key)
			if next == nil || next.ConnID == old.ConnID || dials.Load() != 2 {
				t.Fatal("re-hello failed to transfer claim exactly once")
			}
			if !b.Workers.Submit(key, broker.Job{Kind: broker.JobInbound, Inbound: &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &topic, MessageID: 1, Text: "after re-hello", Timestamp: time.Now()}}) {
				t.Fatal("worker refused")
			}
			if appears == "socket" {
				p := nextInbox(t, pushes)
				if !strings.Contains(p.User.Message.Content, `c3_attempt="inbox:`) {
					t.Fatal("not a negotiated inbox attempt")
				}
				appendPeerReceipt(t, path, p.User.Message.Content)
			} else {
				waitRehelloCondition(t, "no negotiated channel frame", func() bool { return len(out.Bytes()) > 0 })
				var notification struct {
					Params struct{ Meta map[string]string }
				}
				if err := json.Unmarshal(out.Bytes(), &notification); err != nil {
					t.Fatal(err)
				}
				token := notification.Params.Meta["c3_delivery_id"]
				a.liveMu.Lock()
				observer := a.deliveryObservers[token]
				a.liveMu.Unlock()
				if token == "" || observer == nil {
					t.Fatal("inbound remained legacy after re-hello")
				}
				appendChannelReceipt(t, path, token)
			}
			route := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &topic}
			waitRehelloCondition(t, "negotiated receipt did not retire durable row", func() bool { n, _ := b.Queue.Pending(route); return n == 0 })
			time.Sleep(250 * time.Millisecond)
			if dials.Load() != 2 {
				t.Fatal("facts generated more than one re-hello")
			}
		})
	}
}
