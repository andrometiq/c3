//go:build linux || darwin

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
)

func TestUpgradePushAckExecHelloDeliveryOnce(t *testing.T) {
	isolateAdapterTest(t)
	for _, name := range []string{"C3_QUEUE_DIR", "XDG_CONFIG_HOME", "XDG_STATE_HOME"} {
		t.Setenv(name, t.TempDir())
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	b := broker.New(reconnectSwitchMappings())
	defer b.Shutdown()
	if err := b.RegisterChannel(&reconnectSwitchChannel{}); err != nil {
		t.Fatal(err)
	}
	topic := int64(281)
	key := broker.MakeRouteKey("telegram", -100, &topic)
	route := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &topic}
	dial := func(build string) (*ipc.Conn, <-chan []byte) {
		t.Helper()
		client, server := net.Pipe()
		t.Cleanup(func() { client.Close() })
		go b.HandleConn(server)
		_ = client.SetDeadline(time.Now().Add(5 * time.Second))
		conn := ipc.NewConn(client)
		if err := conn.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: os.Getpid(), CWD: "/project", Build: build, RenderState: ipc.RenderCapable}); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ReadFrame(); err != nil {
			t.Fatal(err)
		}
		_ = client.SetDeadline(time.Time{})
		frames := make(chan []byte, 32)
		go func() {
			defer close(frames)
			for {
				raw, err := conn.ReadFrame()
				if err != nil {
					return
				}
				frames <- raw
			}
		}()
		return conn, frames
	}
	conn, frames := dial("old")
	old := b.Stubs.Snapshot()[0]
	b.Routes.Claim(key, old)
	old.AddRoute(key)
	old.MarkRouteConfirmed(key)
	old.SetOutputRoute(key)
	a, out, _ := liveFixture(t, ipc.RenderCapable)
	a.conn = conn
	a.liveTimeout = 2 * time.Second
	ready, calls := upgradeReadyAdapter(t)
	a.upgrade.wire, a.upgrade.read, a.upgrade.exec = ready.upgrade.wire, ready.upgrade.read, ready.upgrade.exec
	send := func(id int64) {
		if !b.Workers.Submit(key, broker.Job{Kind: broker.JobInbound, Inbound: &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &topic, MessageID: id, Text: "message", Timestamp: time.Now()}}) {
			t.Fatal("worker refused")
		}
	}
	inbound := func(frames <-chan []byte) []byte {
		t.Helper()
		timeout := time.NewTimer(3 * time.Second)
		defer timeout.Stop()
		for {
			select {
			case raw, ok := <-frames:
				if !ok {
					t.Fatal("broker connection closed before inbound")
				}
				if op, _ := ipc.PeekOp(raw); op == ipc.OpInbound {
					return raw
				}
			case <-timeout.C:
				t.Fatal("no inbound")
				return nil
			}
		}
	}
	send(1)
	raw := inbound(frames)
	var push ipc.InboundMsg
	if err := json.Unmarshal(raw, &push); err != nil {
		t.Fatal(err)
	}
	a.handleInbound(context.Background(), raw)
	a.acceptUpgrade(&ipc.UpgradeHint{Path: "adapter", Build: "next"})
	if reason := a.tryUpgrade(a.upgrade.hint); !strings.Contains(reason, "ack") || *calls != 0 {
		t.Fatalf("exec before ack: %s", reason)
	}
	if n, _ := b.Queue.Pending(route); n != 1 {
		t.Fatal("push consumed before receipt")
	}
	appendChannelReceipt(t, a.livePath(), push.DeliveryToken)
	waitRehelloCondition(t, "ack did not retire row", func() bool { n, _ := b.Queue.Pending(route); return n == 0 })
	waitRehelloCondition(t, "ack observer still active", func() bool { a.liveMu.Lock(); defer a.liveMu.Unlock(); return a.liveActive == 0 })
	if reason := a.tryUpgrade(a.upgrade.hint); reason != "" || *calls != 1 {
		t.Fatalf("idle exec: %s calls=%d", reason, *calls)
	}
	_ = conn.Close()
	nextConn, nextFrames := dial("next")
	next, _ := b.Routes.Holder(key)
	if next == nil || next.ConnID == old.ConnID || next.Build != "next" || !next.RouteConfirmed(key) {
		t.Fatal("new build lost claim")
	}
	resumed, nextOut, _ := liveFixture(t, ipc.RenderCapable)
	resumed.conn = nextConn
	resumed.liveTimeout = 2 * time.Second
	send(2)
	raw = inbound(nextFrames)
	if err := json.Unmarshal(raw, &push); err != nil {
		t.Fatal(err)
	}
	if push.Inbound.MessageID != 2 {
		t.Fatal("redelivered previous inbound")
	}
	resumed.handleInbound(context.Background(), raw)
	appendChannelReceipt(t, resumed.livePath(), push.DeliveryToken)
	waitRehelloCondition(t, "next inbound not retired", func() bool { n, _ := b.Queue.Pending(route); return n == 0 })
	if strings.Count(string(out.Bytes()), "notifications/claude/channel") != 1 || strings.Count(string(nextOut.Bytes()), "notifications/claude/channel") != 1 {
		t.Fatal("duplicate delivery")
	}
}
