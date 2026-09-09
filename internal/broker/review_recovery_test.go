package broker

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

func TestFetchAndHolderDeathNeverResurrectOrDuplicate(t *testing.T) {
	for _, fetchFirst := range []bool{true, false} {
		name := "death before fetch"
		if fetchFirst {
			name = "fetch then death"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("C3_QUEUE_DIR", t.TempDir())
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			defer b.Shutdown()
			tid := int64(914)
			key := MakeRouteKey("telegram", -1001234567890, &tid)
			qrk := queueRouteKey(key)
			liveHolder(t, b, key)
			w := newRouteWorker(context.Background(), key, time.Hour, b)
			defer w.Stop()
			w.flushInbounds(context.Background(), []*c3types.Inbound{inbound(tid, 1, "one")})
			rows, err := b.Queue.PeekTracked(qrk, -1)
			if err != nil || len(rows) != 1 {
				t.Fatalf("setup %v %v", rows, err)
			}
			if len(w.pendingAck) != 1 || len(w.pendingAck[0].ids) != 1 || w.pendingAck[0].ids[0] != rows[0].RecordID {
				t.Fatal("recovery did not capture durable identity")
			}
			if fetchFirst {
				result := make(chan FetchResult, 1)
				w.handleFetch(context.Background(), &FetchJob{All: true, Ack: true, ResultCh: result})
				got := <-result
				if got.Err != nil || len(got.Messages) != 1 {
					t.Fatalf("fetch %v", got)
				}
				// An unrelated successful reply cannot re-arm the fetched recovery entry.
				out := make(chan OutboundResult, 1)
				w.dispatchOutbound(context.Background(), &OutboundJob{Tool: "reply", Args: map[string]any{"text": "done"}, ResultCh: out})
				if got := <-out; got.Err != nil {
					t.Fatal(got.Err)
				}
			}
			w.flushPendingAck("holder died")
			w.flushPendingAck("repeated death sweep")
			left, err := b.Queue.PeekTracked(qrk, -1)
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if fetchFirst {
				want = 0
			}
			if len(left) != want {
				t.Fatalf("after death: %d rows, want %d", len(left), want)
			}
			if !fetchFirst && left[0].RecordID != rows[0].RecordID {
				t.Fatal("recovery replaced the existing row")
			}
		})
	}
}

func TestPartialFetchRetiresOnlyConsumedRecoverySources(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	liveHolder(t, b, key)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()
	w.flushInbounds(context.Background(), []*c3types.Inbound{inbound(tid, 1, "one"), inbound(tid, 2, "two")})
	result := make(chan FetchResult, 1)
	w.handleFetch(context.Background(), &FetchJob{Limit: 1, Ack: true, ResultCh: result})
	if got := <-result; got.Err != nil || len(got.Messages) != 1 {
		t.Fatalf("fetch: %+v", got)
	}
	if len(w.pendingAck) != 1 || len(w.pendingAck[0].sources) != 1 || w.pendingAck[0].sources[0].MessageID != 2 {
		t.Fatalf("recovery: %+v", w.pendingAck)
	}
	w.flushPendingAck("holder died")
	rows, err := b.Queue.Peek(queueRouteKey(key), -1)
	if err != nil || len(rows) != 1 || rows[0].MessageID != 2 {
		t.Fatalf("wrong recovery: %+v %v", rows, err)
	}
}

// Both generations handshake with the production broker, then exercise its
// queue-consume operation using the hello-derived owner.
func TestHelloAckRecoveryContract(t *testing.T) {
	for _, state := range []string{"", ipc.RenderCapable} {
		name := "legacy"
		if state != "" {
			name = "receipt confirming"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("C3_QUEUE_DIR", t.TempDir())
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			defer b.Shutdown()
			client, server := net.Pipe()
			defer client.Close()
			done := make(chan struct{})
			go func() { b.HandleConn(server); close(done) }()
			peer := ipc.NewConn(client)
			if err := peer.WriteJSON(ipc.HelloMsg{Build: "test", Op: ipc.OpHello, CLI: "claude", PID: os.Getpid(), RenderState: state}); err != nil {
				t.Fatal(err)
			}
			if _, err := peer.ReadFrame(); err != nil {
				t.Fatal(err)
			}
			stub := b.Stubs.Snapshot()[0]
			if stub.ReceiptConfirming != (state != "") {
				t.Fatal("hello capability not retained")
			}
			tid := int64(914)
			key := MakeRouteKey("telegram", -1001234567890, &tid)
			b.Routes.Claim(key, stub)
			stub.AddRoute(key)
			stub.MarkRouteConfirmed(key)
			// Drive a local worker, then consume on that same worker after reading the
			// broker-parsed ack's barrier job below.
			w := newRouteWorker(context.Background(), key, time.Hour, b)
			defer w.Stop()
			pushes := make(chan ipc.InboundMsg, 1)
			go func() {
				raw, err := peer.ReadFrame()
				if err != nil {
					return
				}
				var in ipc.InboundMsg
				_ = json.Unmarshal(raw, &in)
				pushes <- in
			}()
			w.flushInbounds(context.Background(), []*c3types.Inbound{inbound(tid, 1, "one")})
			push := <-pushes
			// handleConsume is the same broker worker operation dispatched by the IPC
			// handler; supplying its hello-derived owner pins compatibility semantics.
			w.handleConsume(context.Background(), &ConsumeJob{MessageID: push.Inbound.MessageID, Token: push.DeliveryToken, Count: push.Covered, Owner: stub})
			if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 0 {
				t.Fatalf("ack left %d durable rows", n)
			}
			w.flushPendingAck("holder died")
			w.flushPendingAck("repeat")
			want := 0
			if state == "" {
				want = 1
			}
			if n, _ := b.Queue.Pending(queueRouteKey(key)); n != want {
				t.Fatalf("recovery=%d want=%d", n, want)
			}
			client.Close()
			<-done
		})
	}
}
