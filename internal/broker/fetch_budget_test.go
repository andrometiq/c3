package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

// These tests drive the REAL handler (handleFetchQueue) over a REAL ipc.Conn
// against a REAL queue store, because the defect only exists at that seam: the
// worker consumes, the handler encodes, and WriteJSON refuses an oversize frame
// having written NOTHING (internal/ipc/conn.go). Anything that stops short of the
// write cannot observe "the queue mutated and the caller was told nothing".

// budgetFixture wires a broker + a confirmed route claim on one topic.
func budgetFixture(t *testing.T, chatID int64, topicID int64) (*Broker, *Stub, queue.RouteKey, string, *fakeChannel) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	t.Cleanup(b.Shutdown)

	tid := topicID
	key := MakeRouteKey("telegram", chatID, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: chatID, TopicID: &tid}
	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	stub.MarkRouteConfirmed() // §5 tripwire: a destructive ack=true fetch needs a confirmed claim
	return b, stub, qrk, dir, fc
}

// seedQueue appends n records of roughly bytesEach through the NORMAL producer
// path, ids starting at firstID.
func seedQueue(t *testing.T, b *Broker, qrk queue.RouteKey, firstID int64, n, bytesEach int) {
	t.Helper()
	body := strings.Repeat("x", bytesEach)
	for i := range n {
		in := &c3types.Inbound{
			Channel: "telegram", ChatID: qrk.ChatID, TopicID: qrk.TopicID,
			MessageID: firstID + int64(i), Text: body, Timestamp: time.Now(),
		}
		if err := b.Queue.Append(qrk, in); err != nil {
			t.Fatalf("seed append %d: %v", firstID+int64(i), err)
		}
	}
}

// seedRawRecord writes one record STRAIGHT into the route's .jsonl, bypassing
// queue.Append. That is not a test shortcut — it is the production shape of the
// case under test: a record too big to encode can only be a legacy line already
// on disk (Append now bounds what it stores), and it is exactly what an
// unbounded producer left behind before that bound existed.
func seedRawRecord(t *testing.T, dir string, qrk queue.RouteKey, in c3types.Inbound) {
	t.Helper()
	data, err := json.Marshal(&in)
	if err != nil {
		t.Fatalf("marshal raw record: %v", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, qrk.File()+".jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("open raw jsonl: %v", err)
	}
	defer f.Close()
	if _, err := f.Write(append(data, '\n')); err != nil {
		t.Fatalf("write raw record: %v", err)
	}
}

// fetchOverIPC runs one fetch_queue through the real handler and reads the
// response frame. ok=false means NOTHING reached the caller within the timeout —
// which is precisely what an oversize (refused) response looks like from the
// adapter's side: silence, while the queue has already been mutated.
func fetchOverIPC(t *testing.T, b *Broker, stub *Stub, req ipc.FetchQueueReq) (ipc.FetchQueueResp, bool) {
	t.Helper()
	agentSide, brokerSide := newConnPair(t)
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal req: %v", err)
	}
	go b.handleFetchQueue(brokerSide, stub, raw)

	type result struct {
		resp ipc.FetchQueueResp
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		frame, rerr := agentSide.ReadFrame()
		if rerr != nil {
			ch <- result{err: rerr}
			return
		}
		var r ipc.FetchQueueResp
		uerr := json.Unmarshal(frame, &r)
		ch <- result{resp: r, err: uerr}
	}()
	select {
	case got := <-ch:
		if got.err != nil {
			t.Fatalf("reading the fetch_queue response failed: %v", got.err)
		}
		return got.resp, true
	case <-time.After(5 * time.Second):
		return ipc.FetchQueueResp{}, false
	}
}

// waitForFetchPending waits for the durable queue state the response claims.
// handleFetchQueue writes its result before the worker's follow-on channel
// notification, so a one-shot read can observe the old state on a busy runner.
// This keeps the loss assertions exact: the queue must reach this count, not
// merely change eventually.
func waitForFetchPending(t *testing.T, b *Broker, qrk queue.RouteKey, want int, desc string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, _ := b.Queue.Pending(qrk)
		if got == want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("defect: %s: fetch_queue reported %d remaining but the durable queue stayed at %d; an async result was asserted before its queue mutation settled", desc, want, got)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// waitForFetchReplyWhere waits for the asynchronous operator-facing notice
// emitted after an oversized record is moved aside. It does not accept a reply
// merely because one appeared: pred pins the content and route the test needs.
func waitForFetchReplyWhere(t *testing.T, fc *fakeChannel, desc string, pred func(c3types.ReplyArgs) bool) c3types.ReplyArgs {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		for _, reply := range fc.sendRepliesSnapshot() {
			if pred(reply) {
				return reply
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("defect: %s: the asynchronous fetch_queue operator notice never arrived; replies sent: %+v", desc, fc.sendRepliesSnapshot())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// frameSize is what WriteJSON measures against ipc.MaxFrameSize.
func frameSize(t *testing.T, resp ipc.FetchQueueResp) int {
	t.Helper()
	enc, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal resp: %v", err)
	}
	return len(enc) + 1
}

// drainAll repeatedly fetches until the route is empty (or the loop budget runs
// out) and returns every MessageID delivered along the way, so a test can prove
// nothing was silently eaten between calls.
func drainAll(t *testing.T, b *Broker, stub *Stub, qrk queue.RouteKey, req ipc.FetchQueueReq) []int64 {
	t.Helper()
	var got []int64
	for range 60 {
		pending, _ := b.Queue.Pending(qrk)
		if pending == 0 {
			break
		}
		resp, ok := fetchOverIPC(t, b, stub, req)
		if !ok {
			t.Fatalf("drain: no response reached the caller with %d still queued", pending)
		}
		if resp.Err != "" {
			t.Fatalf("drain: fetch_queue error %q with %d still queued", resp.Err, pending)
		}
		if len(resp.Messages) == 0 {
			t.Fatalf("drain STALLED: fetch_queue returned 0 messages with %d still queued — the route can never empty", pending)
		}
		for _, m := range resp.Messages {
			got = append(got, m.MessageID)
		}
	}
	return got
}

// trashHolds reports whether any retained file under .trash/ contains needle.
func trashHolds(t *testing.T, dir, needle string) bool {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, ".trash", "*"))
	if err != nil {
		t.Fatalf("glob trash: %v", err)
	}
	for _, m := range matches {
		data, rerr := os.ReadFile(m)
		if rerr != nil {
			continue
		}
		if strings.Contains(string(data), needle) {
			return true
		}
	}
	return false
}

// CASE 1 — all=true, ack=true over a 5 MiB head record followed by four ordinary
// ones. The head alone cannot be encoded into any frame. The pre-fix helper
// answered "cannot measure" for that case, the caller read that as "unbounded",
// and the consume took the WHOLE queue: five records destroyed, response refused,
// caller told nothing. The four innocent records behind the head are the point —
// the blast radius of one undeliverable record must not be the whole route.
func TestFetchQueue_OversizeHeadRecord_DoesNotDestroyTheQueueBehindIt(t *testing.T) {
	b, stub, qrk, dir, fc := budgetFixture(t, -100, 914)

	const oversizeMark = "OVERSIZE-HEAD-BODY"
	seedRawRecord(t, dir, qrk, c3types.Inbound{
		Channel: "telegram", ChatID: qrk.ChatID, TopicID: qrk.TopicID,
		MessageID: 1, Text: oversizeMark + strings.Repeat("y", 5*1024*1024),
		Timestamp: time.Now(),
	})
	seedQueue(t, b, qrk, 2, 4, 64) // ids 2..5, ordinary size

	before, _ := b.Queue.Pending(qrk)
	if before != 5 {
		t.Fatalf("fixture: pending=%d, want 5", before)
	}

	resp, ok := fetchOverIPC(t, b, stub, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "t1", All: true, Ack: true})
	if !ok {
		after, _ := b.Queue.Pending(qrk)
		t.Fatalf("MESSAGE LOSS: the response was never written (an unencodable frame), yet the queue went from %d pending to %d — "+
			"the queue was consumed before the response was known to be encodable; %d messages were destroyed and the caller was told nothing",
			before, after, before-after)
	}
	_ = waitForFetchPending(t, b, qrk, resp.Remaining, "oversize-head fetch")
	if got := frameSize(t, resp); got > ipc.MaxFrameSize {
		t.Fatalf("response frame is %d bytes (cap %d) — it cannot be written, so everything it reports as delivered is lost", got, ipc.MaxFrameSize)
	}

	// The four ordinary records must all still be deliverable.
	delivered := map[int64]bool{}
	for _, m := range resp.Messages {
		delivered[m.MessageID] = true
	}
	for _, id := range drainAll(t, b, stub, qrk, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "t1", All: true, Ack: true}) {
		delivered[id] = true
	}
	for id := int64(2); id <= 5; id++ {
		if !delivered[id] {
			t.Errorf("MESSAGE LOSS: ordinary message %d was never delivered — it sat behind one undeliverable record and was destroyed with it", id)
		}
	}

	// And the undeliverable record itself is accounted for, not silently dropped:
	// the agent is told in-band, and the bytes are retained.
	if !delivered[1] {
		t.Error("the agent was never told what happened to message 1 — a record that vanishes without a word is the data-loss incident, not the bug")
	}
	if !trashHolds(t, dir, oversizeMark) {
		t.Error("the undeliverable record's bytes were not retained under .trash/ — nothing is recoverable")
	}

	// And so is the OPERATOR, on the route itself. A message that leaves the
	// queue without the human hearing about it is the incident, not the bug.
	notice := waitForFetchReplyWhere(t, fc, "oversize-head record moved aside", func(r c3types.ReplyArgs) bool {
		return r.Channel == qrk.Channel && r.ChatID == qrk.ChatID && r.TopicID != nil && *r.TopicID == *qrk.TopicID && strings.Contains(r.Text, "too large")
	})
	if notice.ReplyTo != nil {
		t.Errorf("oversize fetch notice should address the route, not quote one arbitrary message: %+v", notice)
	}
}

// CASE 2 — all=false. The pre-fix budget ran ONLY under `if job.All`, and the
// broker forwards req.Limit verbatim (docs/ADAPTERS.md publishes the 50-cap as
// what the BUILT-INS do, not as a contract), so a plain limited fetch of ordinary
// records assembled a 5 MB frame, consumed all of it, and delivered none of it.
func TestFetchQueue_LimitedFetchIsBudgetedToo(t *testing.T) {
	b, stub, qrk, _, _ := budgetFixture(t, -101, 915)

	const count = 50
	seedQueue(t, b, qrk, 1, count, 100*1024) // ~5 MB against a 4 MiB frame

	before, _ := b.Queue.Pending(qrk)
	resp, ok := fetchOverIPC(t, b, stub, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "t2", Limit: count, Ack: true})
	if !ok {
		after, _ := b.Queue.Pending(qrk)
		t.Fatalf("MESSAGE LOSS: limit=%d was never budgeted — the response was unencodable and never written, but pending went %d → %d: "+
			"the queue was consumed before the response was known to be encodable; %d messages were destroyed and the caller was told nothing",
			count, before, after, before-after)
	}
	after := waitForFetchPending(t, b, qrk, resp.Remaining, "limited fetch")
	if got := frameSize(t, resp); got > ipc.MaxFrameSize {
		t.Fatalf("limited fetch produced a %d-byte frame (cap %d) — unwritable", got, ipc.MaxFrameSize)
	}
	if len(resp.Messages)+after != before {
		t.Fatalf("MESSAGE LOSS: delivered %d, %d left queued, but %d were queued before — %d records are unaccounted for",
			len(resp.Messages), after, before, before-after-len(resp.Messages))
	}

	// Every record must still arrive across repeated calls.
	delivered := map[int64]bool{}
	for _, m := range resp.Messages {
		delivered[m.MessageID] = true
	}
	for _, id := range drainAll(t, b, stub, qrk, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "t2", Limit: count, Ack: true}) {
		delivered[id] = true
	}
	for id := int64(1); id <= count; id++ {
		if !delivered[id] {
			t.Errorf("MESSAGE LOSS: message %d never reached the caller", id)
		}
	}
}

// A conforming third-party adapter may send any limit it likes — the 50-cap is
// documented as built-in behaviour, not as a required contract. limit=2000 over a
// queue of ordinary messages must not be able to destroy the queue.
func TestFetchQueue_HugeAdapterLimitCannotDrainWhatItCannotSend(t *testing.T) {
	b, stub, qrk, _, _ := budgetFixture(t, -102, 916)

	const count = 60
	seedQueue(t, b, qrk, 1, count, 100*1024) // ~6 MB total — over one frame

	before, _ := b.Queue.Pending(qrk)
	resp, ok := fetchOverIPC(t, b, stub, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "t3", Limit: 2000, Ack: true})
	if !ok {
		after, _ := b.Queue.Pending(qrk)
		t.Fatalf("MESSAGE LOSS: limit=2000 was forwarded verbatim and never budgeted — nothing was written back but pending went %d → %d: "+
			"%d messages were consumed before the response was known to be encodable and the caller was told nothing", before, after, before-after)
	}
	after := waitForFetchPending(t, b, qrk, resp.Remaining, "huge adapter-limit fetch")
	if len(resp.Messages)+after != before {
		t.Fatalf("MESSAGE LOSS: delivered %d + queued %d != %d originally queued", len(resp.Messages), after, before)
	}
}

// CASE 3 — the echoed request id is part of the frame. docs/ADAPTERS.md tells
// adapter authors to generate the id themselves and publishes no length bound, so
// a legal request can carry a 1 MiB id. The pre-fix budget counted only the
// messages (plus an 8 KiB envelope constant that does not cover the id), so the
// response overflowed by exactly the id the caller chose — and the consume had
// already happened. An id that would leave no usable room is refused OUTRIGHT,
// and a refusal must not cost a single message.
func TestFetchQueue_OversizeRequestIDIsRefusedWithoutConsuming(t *testing.T) {
	b, stub, qrk, _, _ := budgetFixture(t, -103, 917)

	const count = 30
	seedQueue(t, b, qrk, 1, count, 128*1024) // ~3.9 MiB: fits alone, not with the id
	bigID := strings.Repeat("i", 1024*1024)

	before, _ := b.Queue.Pending(qrk)
	resp, ok := fetchOverIPC(t, b, stub, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: bigID, All: true, Ack: true})
	if !ok {
		after, _ := b.Queue.Pending(qrk)
		t.Fatalf("MESSAGE LOSS: the %d-byte echoed id pushed the response past the frame cap so nothing was written, but pending went %d → %d: "+
			"%d messages were consumed before the response was known to be encodable and the caller was told nothing",
			len(bigID), before, after, before-after)
	}
	after := waitForFetchPending(t, b, qrk, before, "refused oversize-id fetch")
	if resp.Err == "" {
		t.Fatalf("a %d-byte correlation id was accepted silently; it is echoed into the same %d-byte frame as the messages",
			len(bigID), ipc.MaxFrameSize)
	}
	if len(resp.Messages) != 0 || after != before {
		t.Fatalf("MESSAGE LOSS: a REFUSED fetch consumed %d of %d queued messages (%d left)", before-after, before, after)
	}

	// The route is undamaged: a normal id drains it completely.
	delivered := map[int64]bool{}
	for _, id := range drainAll(t, b, stub, qrk, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "ok", All: true, Ack: true}) {
		delivered[id] = true
	}
	for id := int64(1); id <= count; id++ {
		if !delivered[id] {
			t.Errorf("MESSAGE LOSS: message %d never reached the caller after a refused fetch", id)
		}
	}
}

// CASE 3b — an id whose ENCODED length is far larger than len(id): control bytes
// become \u00XX, six bytes each. An id inside the accepted bound must be echoed
// VERBATIM (that is the published contract) and must be paid for out of the
// message budget, not out of the frame's safety margin.
func TestFetchQueue_EscapingHeavyRequestIDIsEchoedAndPaidFor(t *testing.T) {
	b, stub, qrk, _, _ := budgetFixture(t, -104, 918)

	const count = 20
	seedQueue(t, b, qrk, 1, count, 128*1024) // ~2.6 MiB
	// 1000 raw bytes, ~6 KiB once JSON-escaped: len() and encoded length differ 6x.
	escID := strings.Repeat("\x01", 1000)

	before, _ := b.Queue.Pending(qrk)
	resp, ok := fetchOverIPC(t, b, stub, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: escID, All: true, Ack: true})
	if !ok {
		after, _ := b.Queue.Pending(qrk)
		t.Fatalf("MESSAGE LOSS: an id of %d raw bytes encodes to ~%d and overflowed the frame; nothing was written but pending went %d → %d — "+
			"%d messages were consumed before the response was known to be encodable and the caller was told nothing",
			len(escID), len(escID)*6, before, after, before-after)
	}
	after := waitForFetchPending(t, b, qrk, resp.Remaining, "escaping-id fetch")
	if resp.Err != "" {
		t.Fatalf("an id inside the bound was refused: %s", resp.Err)
	}
	if resp.ID != escID {
		t.Fatal("the response must echo the id verbatim — the budget may not silently truncate it")
	}
	if got := frameSize(t, resp); got > ipc.MaxFrameSize {
		t.Fatalf("response with the escaped id is %d bytes (cap %d) — the id was measured raw, not encoded", got, ipc.MaxFrameSize)
	}
	if len(resp.Messages)+after != before {
		t.Fatalf("MESSAGE LOSS: delivered %d + queued %d != %d originally queued", len(resp.Messages), after, before)
	}
}

// The sizing arithmetic is the load-bearing part of the fix, so pin it directly:
// what fetchFrameFit says fits must FIT, and one more must NOT — measured against
// the same encoder that will write the frame. This is what makes the belt-check
// inside fetchFrameFit unreachable rather than merely unlikely.
func TestFetchFrameFit_IsExactAndCountsTheEncodedID(t *testing.T) {
	// Fine-grained records so a boundary actually lands inside the id's cost.
	msgs := make([]c3types.Inbound, 2400)
	body := strings.Repeat("b", 2*1024)
	for i := range msgs {
		msgs[i] = c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: int64(i + 1), Text: body, Timestamp: time.Now()}
	}
	sizeWith := func(id string, k int) int {
		return frameSize(t, ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: id, Messages: msgs[:k], Remaining: len(msgs)})
	}

	tiny := "t1"
	escaped := strings.Repeat("\x01", 1000) // ~6 KiB encoded, 6x its len()

	fitTiny, err := fetchFrameFit(tiny, len(msgs), msgs)
	if err != nil {
		t.Fatalf("unexpected envelope error: %v", err)
	}
	fitEsc, err := fetchFrameFit(escaped, len(msgs), msgs)
	if err != nil {
		t.Fatalf("unexpected envelope error: %v", err)
	}

	for _, tc := range []struct {
		name string
		id   string
		fit  int
	}{{"tiny id", tiny, fitTiny}, {"escaping-heavy id", escaped, fitEsc}} {
		if tc.fit <= 0 || tc.fit >= len(msgs) {
			t.Fatalf("%s: fit=%d — the fixture must land strictly inside the batch", tc.name, tc.fit)
		}
		if got := sizeWith(tc.id, tc.fit); got > ipc.MaxFrameSize {
			t.Errorf("%s: %d records were declared to fit but encode to %d bytes (cap %d) — the budget under-counts, which is the loss path",
				tc.name, tc.fit, got, ipc.MaxFrameSize)
		}
		if got := sizeWith(tc.id, tc.fit+1); got <= ipc.MaxFrameSize {
			t.Errorf("%s: one more record (%d) still fits at %d bytes (cap %d) — the budget over-counts and short-changes every fetch",
				tc.name, tc.fit+1, got, ipc.MaxFrameSize)
		}
	}
	if fitEsc >= fitTiny {
		t.Errorf("the escaped id (%d raw bytes, ~%d encoded) cost nothing: %d records fit with it vs %d without — "+
			"an id measured with len() instead of the encoder is exactly how the frame overflows after the consume",
			len(escaped), len(escaped)*6, fitEsc, fitTiny)
	}
}

// A head record too large for any frame is a fact about THAT record, and it must
// be reported as such — not as "no bound could be measured", which is what the
// caller previously read as "no bound applies" before draining the whole queue.
func TestFetchFrameFit_UnencodableHeadIsNotAnUnboundedFetch(t *testing.T) {
	msgs := []c3types.Inbound{
		{Channel: "telegram", MessageID: 1, Text: strings.Repeat("x", ipc.MaxFrameSize+1024)},
		{Channel: "telegram", MessageID: 2, Text: "small"},
	}
	fit, err := fetchFrameFit("t1", len(msgs), msgs)
	if err != nil {
		t.Fatalf("an oversize RECORD is not an envelope failure: %v", err)
	}
	if fit != 0 {
		t.Fatalf("fit=%d for a record larger than the whole frame, want 0", fit)
	}

	// And an envelope that cannot fit is a DIFFERENT answer — an error, never a
	// number the caller might read as a licence to consume.
	if _, err := fetchFrameFit(strings.Repeat("i", ipc.MaxFrameSize), 0, msgs[1:]); err == nil {
		t.Fatal("an id that fills the whole frame must be reported as an error, not as a fit count")
	}
}

// ack=false is a PEEK and must stay non-destructive even when the head record can
// never be encoded — internal/broker/observe.go rides this same path and its
// contract is that it never consumes. The caller must still be told why the batch
// is short instead of getting silence.
func TestFetchQueue_PeekOverOversizeHeadMutatesNothing(t *testing.T) {
	b, stub, qrk, dir, _ := budgetFixture(t, -105, 919)

	seedRawRecord(t, dir, qrk, c3types.Inbound{
		Channel: "telegram", ChatID: qrk.ChatID, TopicID: qrk.TopicID,
		MessageID: 1, Text: strings.Repeat("z", 5*1024*1024), Timestamp: time.Now(),
	})
	seedQueue(t, b, qrk, 2, 3, 64)

	before, _ := b.Queue.Pending(qrk)
	resp, ok := fetchOverIPC(t, b, stub, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "t4", All: true, Ack: false})
	if !ok {
		t.Fatal("the peek response was never written — the caller got silence instead of an explanation")
	}
	after := waitForFetchPending(t, b, qrk, resp.Remaining, "oversize-head peek")
	if after != before {
		t.Fatalf("a PEEK mutated the queue: pending %d → %d (observe.go rides this path and must never consume)", before, after)
	}
	if len(resp.Messages) == 0 {
		t.Fatal("the peek returned nothing at all — the caller cannot tell an empty queue from an undeliverable head record")
	}
	if got := frameSize(t, resp); got > ipc.MaxFrameSize {
		t.Fatalf("peek response is %d bytes (cap %d) — unwritable", got, ipc.MaxFrameSize)
	}
}
