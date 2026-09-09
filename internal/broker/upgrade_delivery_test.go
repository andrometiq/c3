package broker

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestUpgradeAckConcurrentClaimTransfer(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	key := MakeRouteKey("telegram", -100, nil)
	old := &Stub{CLI: "claude", PID: os.Getpid(), ConnID: 1, ReceiptConfirming: true}
	next := &Stub{CLI: "claude", PID: os.Getpid(), ConnID: 2}
	b.Routes.Claim(key, old)
	old.AddRoute(key)
	old.MarkRouteConfirmed(key)
	ids, err := b.Queue.AppendTracked(queueRouteKey(key), &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 7, Text: "durable", Timestamp: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	w := &RouteWorker{broker: b, key: key}
	w.recordCoveredByPush(7, "token", []string{ids})
	transferred := make(chan struct{})
	b.upgrades.beforeAckRemoval = func() {
		// Called while withConfirmedHolder owns RLock. Force a writer to wait;
		// another RLock inside this callback's caller can now never complete.
		go func() { b.Routes.TransferAllByConnID(1, next); close(transferred) }()
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if !b.Routes.mu.TryRLock() {
				return
			}
			b.Routes.mu.RUnlock()
			time.Sleep(time.Millisecond)
		}
		t.Error("claim writer never queued")
	}
	done := make(chan struct{})
	go func() {
		w.handleConsume(context.Background(), &ConsumeJob{Owner: old, MessageID: 7, Token: "token", Count: 1})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ack/reconnect deadlock")
	}
	select {
	case <-transferred:
	case <-time.After(time.Second):
		t.Fatal("claim transfer blocked")
	}
	if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 0 {
		t.Fatal("ack failed to retire row")
	}
}
