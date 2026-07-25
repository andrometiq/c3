package broker

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/karthikeyan5/c3/internal/c3types"
	"github.com/karthikeyan5/c3/internal/ipc"
	"github.com/karthikeyan5/c3/internal/queue"
)

func TestHandleFetchQueue_ConsumesOldest(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	for i := int64(1); i <= 4; i++ {
		_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: i, Text: "m", Timestamp: time.Now()})
	}
	// Stub holding the route. claimedHolder calls Routes.Claim but does NOT set
	// the stub's CurrentRoute; handleFetchQueue resolves the route via
	// stub.CurrentRoute(), so set it explicitly (mirroring the retranscribe test
	// below) — otherwise the handler returns the no-route Err branch and the
	// assertions below fail.
	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	stub.MarkRouteConfirmed() // destructive Ack=true fetch requires a confirmed claim (§5 tripwire)

	agentSide, brokerSide := newConnPair(t)
	_ = brokerSide
	req := ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "1", Limit: 2, Ack: true}
	raw, _ := json.Marshal(req)
	go b.handleFetchQueue(brokerSide, stub, raw)

	resp := readFetchResp(t, agentSide)
	if len(resp.Messages) != 2 || resp.Messages[0].MessageID != 1 {
		t.Fatalf("fetch_queue returned %+v, want 2 oldest", resp.Messages)
	}
	if resp.Remaining != 2 {
		t.Fatalf("remaining = %d, want 2", resp.Remaining)
	}
	if n, _ := b.Queue.Pending(qrk); n != 2 {
		t.Fatalf("ack=true should consume; pending=%d, want 2", n)
	}
}

func TestHandleFetchQueue_NoRoute(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	stub := &Stub{CLI: "claude"} // no route claimed
	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "1", Ack: true})
	go b.handleFetchQueue(brokerSide, stub, raw)
	resp := readFetchResp(t, agentSide)
	if resp.Err == "" {
		t.Fatal("fetch_queue before attach should return an Err")
	}
}

// ack=false PEEKS: returns the oldest batch WITHOUT advancing the cursor, and
// Remaining reflects what is still queued after this (non-consuming) batch.
func TestHandleFetchQueue_PeekDoesNotConsume(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	for i := int64(1); i <= 4; i++ {
		_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: i, Text: "m", Timestamp: time.Now()})
	}
	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)

	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "1", Limit: 2, Ack: false})
	go b.handleFetchQueue(brokerSide, stub, raw)
	resp := readFetchResp(t, agentSide)
	if len(resp.Messages) != 2 || resp.Messages[0].MessageID != 1 {
		t.Fatalf("peek returned %+v, want 2 oldest", resp.Messages)
	}
	if resp.Remaining != 2 {
		t.Fatalf("peek remaining = %d, want 2 (after this non-consuming batch of 2)", resp.Remaining)
	}
	if n, _ := b.Queue.Pending(qrk); n != 4 {
		t.Fatalf("ack=false must NOT consume; pending=%d, want 4", n)
	}
}

func TestHandleRetranscribe_ReRunsSTT(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(_ context.Context, p c3types.VoicePayload) (string, error) {
		if p.FileID == "vf" {
			return "fresh transcript", nil
		}
		return "", nil
	})
	stub := &Stub{CLI: "claude"}
	stub.SetRoute(&RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 914})

	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "1", FileID: "vf"})
	go b.handleRetranscribe(brokerSide, stub, raw)
	resp := readRetranscribeResp(t, agentSide)
	if resp.Text != "fresh transcript" {
		t.Fatalf("retranscribe text = %q, want 'fresh transcript'", resp.Text)
	}
}

// message_id with NO matching queued message is a clean queue no-op: it must not
// error and must still return the fresh transcript. (The in-place refresh only
// fires when the message is still queued; here nothing is queued.)
func TestHandleRetranscribe_AbsentMessageIDStillReturnsTranscript(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(_ context.Context, p c3types.VoicePayload) (string, error) {
		return "fresh transcript", nil
	})
	stub := &Stub{CLI: "claude"}
	stub.SetRoute(&RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 914})

	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "2", FileID: "vf", MessageID: 999})
	go b.handleRetranscribe(brokerSide, stub, raw)
	resp := readRetranscribeResp(t, agentSide)
	if resp.Err != "" {
		t.Fatalf("retranscribe with absent message_id should not error; got %q", resp.Err)
	}
	if resp.Text != "fresh transcript" {
		t.Fatalf("retranscribe text = %q, want 'fresh transcript' (absent message_id is a queue no-op)", resp.Text)
	}
}

// Per spec Component 5: when message_id is given AND that message is still queued
// for the route, retranscribe refreshes its stored Text in place so a subsequent
// fetch_queue returns the corrected transcript (not the old STT-failure
// placeholder).
func TestHandleRetranscribe_RefreshesQueuedMessageInPlace(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(_ context.Context, p c3types.VoicePayload) (string, error) {
		return "fresh transcript", nil
	})

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	// A queued voice message whose stored Text is an STT-failure placeholder.
	_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 5, Text: "[STT FAILED]", Timestamp: time.Now()})
	// A second queued message that must be left untouched.
	_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 6, Text: "other", Timestamp: time.Now()})

	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)

	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "3", FileID: "vf", MessageID: 5})
	go b.handleRetranscribe(brokerSide, stub, raw)
	resp := readRetranscribeResp(t, agentSide)
	if resp.Err != "" {
		t.Fatalf("retranscribe should not error; got %q", resp.Err)
	}
	if resp.Text != "fresh transcript" {
		t.Fatalf("retranscribe text = %q, want 'fresh transcript'", resp.Text)
	}

	// A subsequent fetch_queue (peek) must return the refreshed text for msg 5,
	// and the untouched text for msg 6.
	fraw, _ := json.Marshal(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "4", All: true, Ack: false})
	go b.handleFetchQueue(brokerSide, stub, fraw)
	fresp := readFetchResp(t, agentSide)
	if len(fresp.Messages) != 2 {
		t.Fatalf("fetch_queue returned %d messages, want 2", len(fresp.Messages))
	}
	for _, m := range fresp.Messages {
		switch m.MessageID {
		case 5:
			if m.Text != "fresh transcript" {
				t.Fatalf("queued msg 5 text = %q, want refreshed 'fresh transcript'", m.Text)
			}
		case 6:
			if m.Text != "other" {
				t.Fatalf("queued msg 6 text = %q, want untouched 'other'", m.Text)
			}
		default:
			t.Fatalf("unexpected queued message id %d", m.MessageID)
		}
	}
}

// Item B (retranscribe timeout): a slow/hung STT provider must NOT block the IPC
// read goroutine forever. handleRetranscribe bounds FireOnVoiceReceived with
// retranscribeTimeout; an OnVoiceReceived callback that respects ctx (returns ""
// once the bounded ctx fires) lets the handler return a "still failing" error
// within the cap rather than wedging. We shorten retranscribeTimeout so the test
// is fast, and assert the handler returns well inside a generous outer deadline.
func TestHandleRetranscribe_BoundsHungProvider(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	prev := retranscribeTimeout
	retranscribeTimeout = 50 * time.Millisecond
	defer func() { retranscribeTimeout = prev }()

	// A provider that hangs until the (bounded) ctx is cancelled, then returns no
	// transcript — exactly how a respectful-but-stuck provider behaves under a
	// deadline.
	started := make(chan struct{}, 1)
	b.Plugins.OnVoiceReceived(func(ctx context.Context, _ c3types.VoicePayload) (string, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done() // block until handleRetranscribe's timeout fires
		return "", ctx.Err()
	})
	stub := &Stub{CLI: "claude"}
	stub.SetRoute(&RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 914})

	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "9", FileID: "vf"})

	respCh := make(chan ipc.RetranscribeResp, 1)
	go func() {
		b.handleRetranscribe(brokerSide, stub, raw)
	}()
	go func() { respCh <- readRetranscribeResp(t, agentSide) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("OnVoiceReceived callback was never invoked")
	}

	select {
	case resp := <-respCh:
		// The bounded ctx fired, the provider returned "", and the handler reported
		// the still-failing error rather than blocking indefinitely.
		if resp.Err == "" {
			t.Fatalf("expected a 'still failing' error after the bounded timeout; got Text=%q", resp.Text)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handleRetranscribe did not return after the bounded timeout — a hung provider can wedge the IPC read loop")
	}
}

func TestHandleInboundDelivered_MergedBatchConsumesAllCovered(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	for i := int64(1); i <= 5; i++ {
		_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: i, Text: "m", Timestamp: time.Now()})
	}
	// Install the worker and the exact identities represented by this synthetic
	// merged push. A count-only ack is deliberately non-destructive.
	b.Workers.mu.Lock()
	w := b.Workers.spawnLocked(key)
	b.Workers.mu.Unlock()
	w.recordCoveredByPush(3, []int64{1, 2, 3})
	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	stub.MarkRouteConfirmed() // live-push ack consume requires a confirmed claim (§5 tripwire)

	// A merged push of 3 lines, acked once with Count=3.
	raw, _ := json.Marshal(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 3, OK: true, Count: 3})
	b.handleInboundDelivered(stub, raw)

	// Poll until the async JobConsume drains 3 (oldest 1,2,3), leaving 4,5.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if n, _ := b.Queue.Pending(qrk); n == 2 {
			break
		}
		if time.Now().After(deadline) {
			n, _ := b.Queue.Pending(qrk)
			t.Fatalf("merged ack(Count=3) should consume 3; pending=%d, want 2", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, _ := b.Queue.Peek(qrk, 5)
	if len(got) != 2 || got[0].MessageID != 4 {
		t.Fatalf("after merged ack, head=%+v, want msgs 4,5", got)
	}
}

// TestHandleFetchQueue_WorkerStall_ReturnsErrorNotWedge proves A3 (defense-in-
// depth) for the NON-DESTRUCTIVE peek path (Ack=false): a worker that genuinely
// STALLS (never writes its result channel) must degrade to a clean
// FetchQueueResp{Err} within workerJobTimeout instead of wedging the connection's
// single serial read loop forever. A discarded peek consumes NOTHING, so
// abandoning it on a stall is loss-free — which is why the timeout is kept ONLY
// for Ack=false (M1, W1 review: the Ack=true destructive path must instead block,
// covered by TestHandleFetchQueue_AckTrue_NoTimeoutBlocksUntilWorker).
//
// The stall is induced by parking the route's single worker on a reply whose
// channel.SendReply blocks; the peek job the handler submits then sits behind it
// unserviced, so its resultCh is never written — exactly the true-stall case
// Phase 1's errWorkerStopped fast-path does NOT cover.
func TestHandleFetchQueue_WorkerStall_ReturnsErrorNotWedge(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b, bc := brokerWithBlockingReply(t)

	prev := workerJobTimeout
	workerJobTimeout = 50 * time.Millisecond
	defer func() { workerJobTimeout = prev }()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)

	// Park the route's single worker on the blocking reply so the peek job that
	// handleFetchQueue submits to the SAME route is never serviced.
	parkCh := make(chan OutboundResult, 1)
	if !b.Workers.Submit(key, Job{Kind: JobOutbound, Outbound: &OutboundJob{Tool: "reply", Args: map[string]any{"text": "park"}, ResultCh: parkCh}}) {
		t.Fatal("failed to submit parking reply job")
	}
	select {
	case <-bc.started:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never entered the blocking SendReply")
	}

	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "1", Limit: 2, Ack: false})

	done := make(chan struct{})
	go func() {
		b.handleFetchQueue(brokerSide, stub, raw)
		close(done)
	}()

	respCh := make(chan ipc.FetchQueueResp, 1)
	go func() { respCh <- readFetchResp(t, agentSide) }()

	select {
	case resp := <-respCh:
		if resp.Err == "" {
			t.Fatalf("stalled worker: expected a non-empty Err, got %+v", resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handleFetchQueue did not return on a stalled worker — the read loop is wedged")
	}

	// The handler itself must also have RETURNED (the read loop is free to serve
	// the next op), not merely written a response.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleFetchQueue did not return after writing the timeout error")
	}
}

// An Ack=true fetch abandoned while queued must not consume later into a
// readerless result channel. The handler times out first and cancels the job's
// lease; when the busy worker eventually reaches the job it peeks instead,
// leaving every held line available for the agent's retry.
func TestHandleFetchQueue_AckTrue_TimeoutCancelsConsume(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b, bc := brokerWithBlockingReply(t)

	prev := workerJobTimeout
	workerJobTimeout = 50 * time.Millisecond
	defer func() { workerJobTimeout = prev }()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	for i := int64(1); i <= 3; i++ {
		_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: i, Text: "m", Timestamp: time.Now()})
	}
	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	stub.MarkRouteConfirmed() // destructive Ack=true fetch requires a confirmed claim (§5 tripwire)

	// Park the route's single worker on the blocking reply so the Ack=true fetch
	// queued behind it is not serviced until we release the worker.
	parkCh := make(chan OutboundResult, 1)
	if !b.Workers.Submit(key, Job{Kind: JobOutbound, Outbound: &OutboundJob{Tool: "reply", Args: map[string]any{"text": "park"}, ResultCh: parkCh}}) {
		t.Fatal("failed to submit parking reply job")
	}
	select {
	case <-bc.started:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never entered the blocking SendReply")
	}

	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "1", All: true, Ack: true})

	done := make(chan struct{})
	go func() {
		b.handleFetchQueue(brokerSide, stub, raw)
		close(done)
	}()
	respCh := make(chan ipc.FetchQueueResp, 1)
	go func() { respCh <- readFetchResp(t, agentSide) }()

	// The broker must time out before an adapter's longer tool timeout. The
	// handler cancels the lease before the worker can reach it.
	select {
	case resp := <-respCh:
		if resp.Err == "" {
			t.Fatalf("stalled Ack=true fetch should report timeout, got %+v", resp)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Ack=true fetch did not time out before the adapter could abandon it")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleFetchQueue did not return after its timeout response")
	}

	// Release the worker, then put a barrier behind the canceled fetch. Receiving
	// the backlog result proves the worker already processed the canceled job.
	bc.release <- struct{}{}
	backlogCh := make(chan BacklogResult, 1)
	if !b.Workers.Submit(key, Job{Kind: JobBacklog, Backlog: &BacklogJob{PeekN: 1, ResultCh: backlogCh}}) {
		t.Fatal("submit barrier backlog job")
	}
	select {
	case <-backlogCh:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process the canceled fetch")
	}

	if n, _ := b.Queue.Pending(qrk); n != 3 {
		t.Fatalf("orphaned Ack=true fetch consumed held lines after caller left; pending=%d, want 3", n)
	}
}

func TestFetchLease_TimeoutRacingStartedConsumeWaitsForResult(t *testing.T) {
	lease := newFetchLease()
	if !lease.beginConsume() {
		t.Fatal("fresh lease refused consume")
	}

	canceled := make(chan bool, 1)
	go func() { canceled <- lease.cancel() }()
	select {
	case <-canceled:
		t.Fatal("timeout cancellation passed an in-progress destructive consume")
	case <-time.After(20 * time.Millisecond):
	}

	lease.finishConsume()
	select {
	case won := <-canceled:
		if won {
			t.Fatal("cancel claimed it won after consume had started")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not unblock after consume finished")
	}
}

func newConnPair(t *testing.T) (agent, broker *ipc.Conn) {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	return ipc.NewConn(a), ipc.NewConn(b)
}

func readFetchResp(t *testing.T, c *ipc.Conn) ipc.FetchQueueResp {
	t.Helper()
	raw, err := c.ReadFrame()
	if err != nil {
		t.Fatalf("read fetch resp: %v", err)
	}
	var r ipc.FetchQueueResp
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func readRetranscribeResp(t *testing.T, c *ipc.Conn) ipc.RetranscribeResp {
	t.Helper()
	raw, err := c.ReadFrame()
	if err != nil {
		t.Fatalf("read retranscribe resp: %v", err)
	}
	var r ipc.RetranscribeResp
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	return r
}
