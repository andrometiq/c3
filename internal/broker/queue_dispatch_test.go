package broker

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

type cachedProbeChannel struct {
	*probeChannel
	path string
}

func (c *cachedProbeChannel) CachedVoicePath(fileID string) string {
	if fileID == "cached-voice" {
		return c.path
	}
	return ""
}

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

func TestHandleRetranscribe_DownHealthCachedSucceedsOffline(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	cachePath := t.TempDir() + "/cached-voice.oga"
	if err := os.WriteFile(cachePath, []byte("cached audio"), 0o600); err != nil {
		t.Fatal(err)
	}
	ch := &cachedProbeChannel{probeChannel: &probeChannel{fakeChannel: &fakeChannel{}}, path: cachePath}
	b := brokerWithGenericChannel(t, mfWithTelegram(), ch)
	defer b.Shutdown()
	b.setLastHealth(c3types.HealthEvent{Channel: "telegram", State: c3types.HealthStateDown, Since: time.Now()})
	var calls atomic.Int64
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		calls.Add(1)
		return "offline transcript", nil
	})
	stub := &Stub{CLI: "claude"}
	route := MakeRouteKey("telegram", -100, ptrI64(914))
	stub.SetRoute(&route)
	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "cached-down", FileID: "cached-voice"})
	go b.handleRetranscribe(brokerSide, stub, raw)
	resp := readRetranscribeResp(t, agentSide)
	if resp.Err != "" || resp.Text != "offline transcript" {
		t.Fatalf("cached retranscribe while DOWN = %+v", resp)
	}
	if calls.Load() != 1 || ch.calls.Load() != 0 {
		t.Fatalf("cached DOWN path calls: STT=%d network probes=%d; want 1,0", calls.Load(), ch.calls.Load())
	}
}

func TestHandleRetranscribe_DownHealthUncachedFailsFastAndParks(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	ch := &probeChannel{fakeChannel: &fakeChannel{}, size: 100}
	b := brokerWithProbe(t, ch)
	defer b.Shutdown()
	b.setLastHealth(c3types.HealthEvent{Channel: "telegram", State: c3types.HealthStateDown, Since: time.Now()})
	var calls atomic.Int64
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		calls.Add(1)
		return "must not run", nil
	})
	stub := &Stub{CLI: "claude"}
	route := MakeRouteKey("telegram", -100, ptrI64(914))
	stub.SetRoute(&route)
	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "uncached-down", FileID: "uncached-voice", MessageID: 44})
	started := time.Now()
	go b.handleRetranscribe(brokerSide, stub, raw)
	resp := readRetranscribeResp(t, agentSide)
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("uncached DOWN retranscribe blocked the IPC read loop for %s", elapsed)
	}
	if !strings.Contains(resp.Err, "DOWN") || !strings.Contains(resp.Err, "automatic retry continues") {
		t.Fatalf("uncached DOWN response = %+v", resp)
	}
	if calls.Load() != 0 || ch.calls.Load() != 0 {
		t.Fatalf("uncached DOWN path burned work: STT=%d probes=%d", calls.Load(), ch.calls.Load())
	}
	key := voiceScheduleKey{route: route, messageID: 44, fileID: "uncached-voice"}
	b.Voice.mu.Lock()
	entry := b.Voice.entries[key]
	parked := entry != nil && entry.state == voiceWaiting && !entry.firstFailure.IsZero() && len(entry.hooks) == 0
	b.Voice.mu.Unlock()
	if !parked {
		t.Fatal("uncached manual entry did not remain parked for automatic recovery")
	}
}

func TestHandleRetranscribe_WithoutRouteOrMessageIDIsTranscriptOnly(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		return "transcript only", nil
	})

	cases := []struct {
		name string
		stub *Stub
		req  ipc.RetranscribeReq
	}{
		{name: "no route", stub: &Stub{CLI: "claude"}, req: ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "no-route", FileID: "voice-a", MessageID: 77}},
		{name: "no message id", stub: func() *Stub {
			s := &Stub{CLI: "claude"}
			route := MakeRouteKey("telegram", -100, ptrI64(914))
			s.SetRoute(&route)
			return s
		}(), req: ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "no-message", FileID: "voice-b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			agentSide, brokerSide := newConnPair(t)
			raw, _ := json.Marshal(tc.req)
			go b.handleRetranscribe(brokerSide, tc.stub, raw)
			if resp := readRetranscribeResp(t, agentSide); resp.Err != "" || resp.Text != "transcript only" {
				t.Fatalf("transcript-only response = %+v", resp)
			}
		})
	}
	if all := b.Queue.StatusAll(); len(all) != 0 {
		t.Fatalf("transcript-only retranscribe wrote queue routes: %+v", all)
	}
}

// With no pending owner, manual retranscribe returns synchronously and lands the
// same result as an ordinary durable revision line.
func TestHandleRetranscribe_AbsentMessageIDReturnsAndAppendsRevision(t *testing.T) {
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
		t.Fatalf("retranscribe text = %q, want 'fresh transcript'", resp.Text)
	}
	rows, err := b.Queue.Peek(queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: ptrI64(914)}, -1)
	if err != nil || len(rows) != 1 || !strings.Contains(rows[0].Text, "[transcript update for voice message 999]") {
		t.Fatalf("manual revision rows=%+v err=%v", rows, err)
	}
}

func TestHandleRetranscribe_ResolvesPendingOwnerInPlace(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(_ context.Context, _ c3types.VoicePayload) (string, error) {
		return "manual transcript", nil
	})

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	pending := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 5,
		Text:        "caption\n" + voicePendingText("vf"),
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "vf"}},
		Timestamp:   time.Now(),
	}
	recordID, err := b.Queue.AppendTracked(qrk, pending, "vf")
	if err != nil {
		t.Fatal(err)
	}
	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "pending", FileID: "vf", MessageID: 5})
	go b.handleRetranscribe(brokerSide, stub, raw)
	if resp := readRetranscribeResp(t, agentSide); resp.Err != "" || resp.Text != "manual transcript" {
		t.Fatalf("retranscribe response=%+v", resp)
	}

	rows, err := b.Queue.PeekTracked(qrk, -1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("resolved rows=%+v err=%v", rows, err)
	}
	if rows[0].RecordID != recordID || len(rows[0].VoicePending) != 0 {
		t.Fatalf("pending owner was replaced/appended instead of resolved in place: %+v", rows[0])
	}
	if rows[0].Inbound.Text != "caption\n[Transcribed voice]: manual transcript" {
		t.Fatalf("resolved text=%q", rows[0].Inbound.Text)
	}
}

// Legacy final rows have nil VoicePending and are never re-enriched. Manual
// retranscribe preserves them and appends an explicit revision instead.
func TestHandleRetranscribe_LegacyFinalRowAppendsRevisionWithoutMutation(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(_ context.Context, p c3types.VoicePayload) (string, error) {
		return "fresh transcript", nil
	})

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	const original = "legacy final voice text"
	_ = b.Queue.Append(qrk, &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 5,
		Text:        original,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "vf"}},
		Timestamp:   time.Now(),
	})
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

	// A subsequent fetch_queue sees both original rows plus one revision.
	fraw, _ := json.Marshal(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "4", All: true, Ack: false})
	go b.handleFetchQueue(brokerSide, stub, fraw)
	fresp := readFetchResp(t, agentSide)
	if len(fresp.Messages) != 3 {
		t.Fatalf("fetch_queue returned %d messages, want 3", len(fresp.Messages))
	}
	if fresp.Messages[0].Text != original || fresp.Messages[1].Text != "other" {
		t.Fatalf("manual retranscribe mutated legacy rows: %+v", fresp.Messages)
	}
	if got := fresp.Messages[2].Text; !strings.HasPrefix(got, "[transcript update for voice message 5]\n") || !strings.Contains(got, "fresh transcript") {
		t.Fatalf("manual revision text=%q", got)
	}
}

// A drained voice copy and an unrelated organic target message can share the
// same Telegram MessageID. Retranscribe must not validate the drained voice,
// then let the store skip it and rewrite the organic collision.
func TestHandleRetranscribe_DrainedVoiceCollisionDoesNotRewriteOrganicMessage(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		return "fresh transcript", nil
	})

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	const drainedText = "[Transcribed voice]: frozen source transcript"
	if err := b.Queue.Append(qrk, &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 5,
		Text:        drainedText,
		DrainedFrom: "telegram__-200__412",
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "V1"}},
		Timestamp:   time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	const organicText = "unrelated organic target message"
	if err := b.Queue.Append(qrk, &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 5,
		Text: organicText, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "collision", FileID: "V1", MessageID: 5})
	go b.handleRetranscribe(brokerSide, stub, raw)
	if resp := readRetranscribeResp(t, agentSide); resp.Text != "fresh transcript" || resp.Err != "" {
		t.Fatalf("retranscribe response = %+v, want transcript returned despite queue no-op", resp)
	}

	got, err := b.Queue.Peek(qrk, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Text != drainedText || got[1].Text != organicText ||
		!strings.Contains(got[2].Text, "fresh transcript") {
		t.Fatalf("collision refresh rewrote a different queue record: %+v", got)
	}
}

// A slow provider must not wedge the IPC read loop. The caller wait expires,
// while the scheduler-owned attempt continues independently until its own bound.
func TestHandleRetranscribe_BoundsCallerWaitWithoutCancelingLease(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	prev := retranscribeTimeout
	retranscribeTimeout = 50 * time.Millisecond
	defer func() { retranscribeTimeout = prev }()

	// A provider that remains in flight beyond the shortened caller wait.
	started := make(chan struct{}, 1)
	b.Plugins.OnVoiceReceived(func(ctx context.Context, _ c3types.VoicePayload) (string, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done() // scheduler cancellation/deadline, not caller-wait cancellation
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
		if resp.Err == "" {
			t.Fatalf("expected a caller-wait timeout; got Text=%q", resp.Text)
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
	var recordIDs []string
	for i := int64(1); i <= 5; i++ {
		recordID, err := b.Queue.AppendTracked(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: i, Text: "m", Timestamp: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		recordIDs = append(recordIDs, recordID)
	}
	// Install the worker and the exact identities represented by this synthetic
	// merged push. A count-only ack is deliberately non-destructive.
	b.Workers.mu.Lock()
	w := b.Workers.spawnLocked(key)
	b.Workers.mu.Unlock()
	w.recordCoveredByPush(3, "", recordIDs[:3])
	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	stub.MarkRouteConfirmed() // live-push ack consume requires a confirmed claim (§5 tripwire)
	// The other half of the synthetic push: the ack is routed by the route the
	// push went out on, recorded on the stub at push time (see Stub.pushRoutes).
	// Without it this fabricated ack matches no push and consumes nothing.
	stub.RecordPushRoute(3, "", key)

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
