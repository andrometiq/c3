//go:build linux || darwin

package main

import (
	"context"
	"encoding/json"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
)

// Recreates the review's real consumption + holder-death path. No Claude or
// broker daemon: HandleConn and the durable worker run inside the test process.
func TestReviewQuotedReceiptConsumesDurableRow(t *testing.T) {
	for _, variant := range []string{"quoted", "oversized", "valid"} {
		t.Run(variant, func(t *testing.T) { crossSessionBrokerLifecycle(t, variant) })
	}
}

func crossSessionBrokerLifecycle(t *testing.T, variant string) {
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	for _, name := range []string{"C3_QUEUE_DIR", "XDG_CONFIG_HOME", "XDG_STATE_HOME"} {
		t.Setenv(name, t.TempDir())
	}
	b := broker.New(reconnectSwitchMappings())
	defer b.Shutdown()
	if err := b.RegisterChannel(&reconnectSwitchChannel{}); err != nil {
		t.Fatal(err)
	}
	holder := exec.Command("sleep", "60")
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Process.Kill(); _ = holder.Wait() }()
	client, server := net.Pipe()
	defer client.Close()
	handlerDone := make(chan struct{})
	go func() { b.HandleConn(server); close(handlerDone) }()
	conn := ipc.NewConn(client)
	if err := conn.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: holder.Process.Pid, RenderState: ipc.RenderProbing}); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	stub := b.Stubs.Snapshot()[0]
	tid := int64(281)
	key := broker.MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	b.Routes.Claim(key, stub)
	stub.AddRoute(key)
	stub.MarkRouteConfirmed(key)
	stub.SetOutputRoute(key)
	submit := func(job broker.Job) {
		t.Helper()
		if !b.Workers.SubmitWait(key, job) {
			t.Fatal("worker refused job")
		}
	}
	a := newAdapter()
	seedLiveTranscript(t, a)
	a.conn = conn
	a.liveTimeout = 600 * time.Millisecond
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "channel not registered"}
	tx, pushes := fakeInbox(t, nil, "")
	a.crossSession = tx
	sessionID := "11111111-2222-4333-8444-555555555555"
	a.setCurrentStableIdentity(sessionhandoff.Entry{StableSessionID: sessionID})
	a.resetLiveRoute(false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.runCtx = ctx
	submit(broker.Job{Kind: broker.JobInbound, Inbound: &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 2, Text: "still durable", Timestamp: time.Now()}})
	_ = client.SetReadDeadline(time.Now().Add(4 * time.Second))
	raw, err := conn.ReadFrame()
	_ = client.SetReadDeadline(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if op, _ := ipc.PeekOp(raw); op != ipc.OpInbound {
		t.Fatalf("expected inbound: %s", raw)
	}
	rows, err := b.Queue.PeekTracked(qrk, -1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("initial row: %+v %v", rows, err)
	}
	recordID := rows[0].RecordID
	var in ipc.InboundMsg
	if err := json.Unmarshal(raw, &in); err != nil {
		t.Fatal(err)
	}
	frame := buildClaudeChannelFrame(&in.Inbound)
	if variant == "oversized" {
		// A valid durable row can expand past IPC's cap after rendering/JSON escaping.
		frame["content"] = strings.Repeat("<", ipc.MaxFrameSize/6)
	}
	a.pushWithReadback(ctx, in, frame)
	if variant != "oversized" {
		push := nextInbox(t, pushes)
		if push.User.SessionID != sessionID {
			t.Fatal("known stable session UUID missing from user frame")
		}
		content := push.User.Message.Content
		if variant == "quoted" {
			content = "Please explain this quote: " + content
		}
		appendPeerReceipt(t, a.livePath(), content)
	}
	deadline := time.Now().Add(2 * time.Second)
	for a.liveRoute().State == ipc.RenderProbing && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	want := 1
	if variant == "valid" {
		want = 0
	}
	invalidPromotion := variant != "valid" && a.liveRoute().State != ipc.RenderQueueOnly
	// An IPC round trip is a barrier after any adapter ack and before the worker
	// snapshot. This proves actual broker consumption, not just adapter output.
	if err := conn.WriteJSON(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "barrier", All: true, Ack: false}); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		raw, err := conn.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if op, _ := ipc.PeekOp(raw); op == ipc.OpFetchQueueResult {
			break
		}
	}
	_ = client.SetReadDeadline(time.Time{})
	// For a valid receipt route publication precedes ack; allow its writer to
	// finish, then inspect the queue. Invalid receipts must never remove the row.
	deadline = time.Now().Add(time.Second)
	for {
		rows, err = b.Queue.PeekTracked(qrk, -1)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == want || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(rows) != want || (want == 1 && rows[0].RecordID != recordID) {
		t.Errorf("before death: rows=%+v want=%d", rows, want)
	}
	cancel()
	client.Close()
	<-handlerDone
	_ = holder.Process.Kill()
	_ = holder.Wait()
	submit(broker.Job{Kind: broker.JobInbound, Inbound: &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, Kind: c3types.InboundSystem, Text: "sweep"}})
	result := make(chan broker.FetchResult, 1)
	submit(broker.Job{Kind: broker.JobFetch, Fetch: &broker.FetchJob{All: true, ResultCh: result}})
	select {
	case got := <-result:
		if got.Err != nil || len(got.Messages) != want {
			t.Errorf("after holder death: %+v want=%d", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("death sweep stalled")
	}
	rows, err = b.Queue.PeekTracked(qrk, -1)
	if err != nil || len(rows) != want || (want == 1 && rows[0].RecordID != recordID) {
		t.Error("holder death changed durable row identity")
	}
	if invalidPromotion {
		t.Errorf("invalid receipt promoted route: %+v", a.liveRoute())
	}
	if variant == "oversized" {
		select {
		case <-pushes:
			t.Fatal("oversized delivery reached inbox")
		default:
		}
	}
}
