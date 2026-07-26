package broker

// drain_test.go covers the phase-2 drain core (drain.go): the 4-step flow's
// counts and routing, the Step-D advisory split (nudge vs notice), the
// idempotency/crash-ordering invariants (INV-2/3), the no-dedup-seed rule
// (INV-8), the surviving-intersection report (INV-5), the per-source in-flight
// lock (A1), and the selector contract (A2 ordinals). Uses the existing broker
// test harness: fakeChannel captures SendReply, a net.Pipe-backed stub conn
// captures the targeted SystemEvent nudge.

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// drainTestBroker builds an isolated broker (throwaway queue dir) wired with a
// SendReply-recording fakeChannel.
func drainTestBroker(t *testing.T) (*Broker, *fakeChannel) {
	t.Helper()
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	t.Cleanup(b.Shutdown)
	return b, fc
}

// drainSrc / drainDst are the two registered topics from mfWithTelegram:
// "c3" (-100/281, group main) and "feature-x" (-200/412, group work).
func drainSrc() RouteKey {
	return RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 281}
}
func drainDst() RouteKey {
	return RouteKey{Channel: "telegram", ChatID: -200, HasTopic: true, TopicID: 412}
}

// drainSpec builds the resolved spec the command layer would hand Drain.
func drainSpec(sel ...DrainSelector) DrainSpec {
	spec := DrainSpec{Source: drainSrc(), Target: drainDst(), SourceName: "genie", TargetName: "redtruck"}
	if len(sel) > 0 {
		spec.Selector = sel[0]
	}
	return spec
}

// drainSrcMsg builds a source-routed organic message with a fresh timestamp.
// These fixtures pass through the real 14-day retention cap, so an absolute date
// turns the suite into a time bomb once the wall clock advances past it.
func drainSrcMsg(id int64, text string) *c3types.Inbound {
	return &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: ptrI64(281), MessageID: id,
		Sender:    c3types.Sender{UserID: 11, Username: "alice"},
		Text:      text,
		Timestamp: time.Now().UTC(),
	}
}

func TestDrainSrcMsg_UsesFreshTimestamp(t *testing.T) {
	m := drainSrcMsg(1, "fresh")
	age := time.Since(m.Timestamp)
	if age < 0 || age > time.Minute {
		t.Fatalf("drainSrcMsg timestamp is not fresh (age=%v); queue.MaxAge will eventually make drain tests silently evict their fixtures", age)
	}
}

// drainSeed appends messages straight into a route's durable queue (test setup
// runs before any worker touches the route, so single-owner is honored by
// sequencing).
func drainSeed(t *testing.T, b *Broker, key RouteKey, msgs ...*c3types.Inbound) {
	t.Helper()
	for _, m := range msgs {
		if err := b.Queue.Append(queueRouteKey(key), m); err != nil {
			t.Fatalf("seed append: %v", err)
		}
	}
}

// drainPeekAll returns a route's full pending snapshot.
func drainPeekAll(t *testing.T, b *Broker, key RouteKey) []c3types.Inbound {
	t.Helper()
	got, err := b.Queue.Peek(queueRouteKey(key), -1)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	return got
}

// drainAttachLiveTarget claims key with a live, connected stub whose conn is a
// real ipc.Conn over net.Pipe, and returns a channel of the frames the broker
// writes to it (closed when the pipe closes).
func drainAttachLiveTarget(t *testing.T, b *Broker, key RouteKey) (<-chan []byte, *Stub) {
	t.Helper()
	adapterEnd, brokerEnd := net.Pipe()
	stub := &Stub{CLI: "claude", PID: os.Getpid(), CWD: "/proj", ConnID: 91, Conn: ipc.NewConn(brokerEnd)}
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatal("claim target failed")
	}
	frames := make(chan []byte, 16)
	reader := ipc.NewConn(adapterEnd)
	go func() {
		defer close(frames)
		for {
			raw, err := reader.ReadFrame()
			if err != nil {
				return
			}
			cp := make([]byte, len(raw))
			copy(cp, raw)
			frames <- cp
		}
	}()
	t.Cleanup(func() { _ = adapterEnd.Close(); _ = brokerEnd.Close() })
	return frames, stub
}

// TestDrain_AllToUnattachedTarget_NoticeToTargetTopic: the happy path into an
// unattached target — counts, source emptied, target order, and exactly ONE
// drain-notice posted in the TARGET topic (never per message).
func TestDrain_AllToUnattachedTarget_NoticeToTargetTopic(t *testing.T) {
	b, fc := drainTestBroker(t)
	drainSeed(t, b, drainSrc(), drainSrcMsg(1, "one"), drainSrcMsg(2, "two"), drainSrcMsg(3, "three"))

	res, err := b.Drain(drainSpec())
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if res.Requested != 3 || res.Appended != 3 || res.PresenceSkipped != 0 ||
		res.RemovedFromSource != 3 || res.AlreadyGone != 0 || res.TargetPending != 3 {
		t.Fatalf("result counts wrong: %+v", res)
	}
	if res.WindowLo != 1 || res.WindowHi != 3 || res.Clamped {
		t.Fatalf("window echo wrong: lo=%d hi=%d clamped=%v", res.WindowLo, res.WindowHi, res.Clamped)
	}
	if !strings.Contains(res.FirstPreview, "one") {
		t.Errorf("FirstPreview = %q, want the oldest moved line", res.FirstPreview)
	}
	if res.SourceName != "genie" || res.TargetName != "redtruck" {
		t.Errorf("resolved names not echoed: %+v", res)
	}
	if got := drainPeekAll(t, b, drainSrc()); len(got) != 0 {
		t.Fatalf("source must be emptied, still holds %d", len(got))
	}
	got := drainPeekAll(t, b, drainDst())
	if len(got) != 3 || got[0].MessageID != 1 || got[1].MessageID != 2 || got[2].MessageID != 3 {
		t.Fatalf("target must hold the 3 moved lines oldest-first, got %+v", got)
	}
	if !res.NoticeSent || res.NudgeSent {
		t.Fatalf("unattached target: want NoticeSent (no nudge), got %+v", res)
	}
	notices := 0
	for _, rp := range fc.sendRepliesSnapshot() {
		if !strings.Contains(rp.Text, "drained in") {
			continue
		}
		notices++
		if rp.ChatID != -200 || rp.TopicID == nil || *rp.TopicID != 412 {
			t.Errorf("drain-notice posted to %d/%v, want the TARGET topic -200/412", rp.ChatID, rp.TopicID)
		}
		if !strings.Contains(rp.Text, "3 drained in from «genie»") || !strings.Contains(rp.Text, "3 total queued") || !strings.Contains(rp.Text, "fetch_queue") {
			t.Errorf("drain-notice text wrong: %q", rp.Text)
		}
	}
	if notices != 1 {
		t.Fatalf("want exactly ONE drain-notice per drain, got %d", notices)
	}
}

// TestDrain_RewritesRoutingAndStampsProvenance: a moved record carries the
// TARGET routing fields (graft 2 — record-keyed paths like the held-notice are
// built from the record, worker.go forwardOrFallback), the canonical NUMERIC
// source key in DrainedFrom (B6), exactly one banner prefix computed from the
// ORIGINAL fields, and preserved MessageID/Sender/Timestamp/Attachments (R14).
func TestDrain_RewritesRoutingAndStampsProvenance(t *testing.T) {
	b, fc := drainTestBroker(t)
	orig := drainSrcMsg(7, "hello world")
	orig.Attachments = []c3types.Attachment{{Kind: "voice", FileID: "VF7", MIME: "audio/ogg", Size: 512}}
	drainSeed(t, b, drainSrc(), orig)

	if _, err := b.Drain(drainSpec()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	got := drainPeekAll(t, b, drainDst())
	if len(got) != 1 {
		t.Fatalf("target pending = %d, want 1", len(got))
	}
	m := got[0]
	if m.Channel != "telegram" || m.ChatID != -200 || m.TopicID == nil || *m.TopicID != 412 {
		t.Fatalf("routing not rewritten to the target: chan=%s chat=%d topic=%v", m.Channel, m.ChatID, m.TopicID)
	}
	if m.DrainedFrom != "telegram__-100__281" {
		t.Fatalf("DrainedFrom = %q, want the canonical numeric source key telegram__-100__281 (B6)", m.DrainedFrom)
	}
	if !strings.HasPrefix(m.Text, "↩︎ from «genie» · @alice · ") {
		t.Fatalf("banner missing/wrong (must use ORIGINAL sender): %q", m.Text)
	}
	if n := strings.Count(m.Text, "↩︎ from «"); n != 1 {
		t.Fatalf("banner prefix must appear exactly once, found %d in %q", n, m.Text)
	}
	if !strings.HasSuffix(m.Text, "\nhello world") {
		t.Fatalf("body must follow the banner verbatim: %q", m.Text)
	}
	if m.MessageID != 7 || m.Sender.Username != "alice" || m.Sender.UserID != 11 {
		t.Fatalf("MessageID/Sender must be preserved verbatim: %+v", m)
	}
	if len(m.Attachments) != 1 || m.Attachments[0].FileID != "VF7" || m.Attachments[0].Kind != "voice" {
		t.Fatalf("attachments must be preserved (drained voice stays re-downloadable): %+v", m.Attachments)
	}
	if !m.Timestamp.Equal(orig.Timestamp) {
		t.Fatalf("timestamp must be preserved: %v vs %v", m.Timestamp, orig.Timestamp)
	}

	// A held-notice built FROM this record must post to the TARGET topic —
	// the worker builds ReplyArgs from the record's ChatID/TopicID, so a
	// missing rewrite would mis-post to the source topic.
	pre := len(fc.sendRepliesSnapshot())
	w := newRouteWorker(context.Background(), drainDst(), time.Hour, b)
	defer w.Stop()
	rec := m
	w.forwardOrFallback(context.Background(), &rec, 1)
	replies := fc.sendRepliesSnapshot()
	if len(replies) != pre+1 {
		t.Fatalf("want one held-notice from the moved record, got %d new sends", len(replies)-pre)
	}
	last := replies[len(replies)-1]
	if last.ChatID != -200 || last.TopicID == nil || *last.TopicID != 412 {
		t.Fatalf("held-notice from the moved record posted to %d/%v, want the TARGET -200/412", last.ChatID, last.TopicID)
	}
}

// TestDrain_AttachedTargetNudge_NoUserLinePush is the head-alignment
// regression from spec §10: an attached target with pre-existing backlog K>0
// gets EXACTLY ONE broker-originated SystemEvent (Covered=0, no user content)
// and NO OpInbound user-line push; the backlog stays intact until fetch_queue,
// which then returns backlog+drained in order. This is the test that would
// have caught the original proposals' tail-append + blind head-consume flaw.
func TestDrain_AttachedTargetNudge_NoUserLinePush(t *testing.T) {
	b, fc := drainTestBroker(t)
	ts := time.Now().UTC()
	backlog1 := &c3types.Inbound{Channel: "telegram", ChatID: -200, TopicID: ptrI64(412), MessageID: 100, Text: "backlog-1", Timestamp: ts}
	backlog2 := &c3types.Inbound{Channel: "telegram", ChatID: -200, TopicID: ptrI64(412), MessageID: 101, Text: "backlog-2", Timestamp: ts}
	drainSeed(t, b, drainDst(), backlog1, backlog2)
	drainSeed(t, b, drainSrc(), drainSrcMsg(1, "moved-1"), drainSrcMsg(2, "moved-2"))

	frames, _ := drainAttachLiveTarget(t, b, drainDst())

	res, err := b.Drain(drainSpec())
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if !res.NudgeSent || res.NoticeSent {
		t.Fatalf("live holder: want NudgeSent (no topic notice), got %+v", res)
	}

	var raw []byte
	select {
	case raw = <-frames:
	case <-time.After(2 * time.Second):
		t.Fatal("no nudge frame reached the live holder within 2s")
	}
	var push ipc.InboundMsg
	if err := json.Unmarshal(raw, &push); err != nil {
		t.Fatalf("unmarshal nudge frame: %v", err)
	}
	if push.Op != ipc.OpInbound || push.Inbound.Kind != c3types.InboundSystem {
		t.Fatalf("frame is not a system-event push: %s", raw)
	}
	if push.Inbound.Event == nil || push.Inbound.Event.System == nil {
		t.Fatalf("system payload missing: %s", raw)
	}
	msg := push.Inbound.Event.System.Message
	if !strings.Contains(msg, "2 message(s) drained in from «genie»") ||
		!strings.Contains(msg, "fetch_queue") || !strings.Contains(msg, "4 total queued") {
		t.Errorf("nudge text wrong: %q", msg)
	}
	// A4: the nudge must be un-consumable — Covered stays the zero value
	// (omitted on the wire) — and carry no user content.
	if push.Covered != 0 || push.Pending != 0 {
		t.Fatalf("nudge must carry Covered=0/Pending=0, got covered=%d pending=%d", push.Covered, push.Pending)
	}
	if strings.Contains(string(raw), `"covered"`) {
		t.Fatalf("covered must be omitted on the wire (zero value): %s", raw)
	}
	if push.Inbound.Text != "" || push.Inbound.ChatID != 0 {
		t.Fatalf("nudge must carry no user content/routing: %s", raw)
	}
	// NO further pushes — drained user lines are never live-pushed (INV-6).
	select {
	case extra, ok := <-frames:
		if ok {
			t.Fatalf("unexpected extra push frame (user-line push?): %s", extra)
		}
	case <-time.After(150 * time.Millisecond):
	}
	for _, rp := range fc.sendRepliesSnapshot() {
		if strings.Contains(rp.Text, "drained in") {
			t.Fatal("nudge path must not ALSO post the topic drain-notice")
		}
	}

	// Backlog intact until fetch_queue; the fetch then returns backlog+drained
	// oldest-first (head-aligned by construction).
	resultCh := make(chan FetchResult, 1)
	if !b.Workers.Submit(drainDst(), Job{Kind: JobFetch, Fetch: &FetchJob{All: true, Ack: true, ResultCh: resultCh}}) {
		t.Fatal("submit fetch")
	}
	fr := <-resultCh
	if fr.Err != nil {
		t.Fatalf("fetch: %v", fr.Err)
	}
	if len(fr.Messages) != 4 {
		t.Fatalf("fetch returned %d messages, want 4 (2 backlog + 2 drained)", len(fr.Messages))
	}
	for i, want := range []int64{100, 101, 1, 2} {
		if fr.Messages[i].MessageID != want {
			t.Fatalf("fetch order[%d] = id %d, want %d (backlog first, drained after)", i, fr.Messages[i].MessageID, want)
		}
	}
}

// TestDrain_RenderIncapableHolder_FallsBackToNotice: a live holder whose host
// cannot render channel pushes (forked session) is treated as no-holder for
// the nudge — the drain-notice posts to the topic instead (§7 edge table).
func TestDrain_RenderIncapableHolder_FallsBackToNotice(t *testing.T) {
	b, fc := drainTestBroker(t)
	drainSeed(t, b, drainSrc(), drainSrcMsg(1, "one"))
	frames, stub := drainAttachLiveTarget(t, b, drainDst())
	stub.SetCannotRender(true)

	res, err := b.Drain(drainSpec())
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if res.NudgeSent || !res.NoticeSent {
		t.Fatalf("render-incapable holder: want the notice path, got %+v", res)
	}
	select {
	case raw, ok := <-frames:
		if ok {
			t.Fatalf("render-incapable holder must receive no push, got %s", raw)
		}
	case <-time.After(150 * time.Millisecond):
	}
	found := false
	for _, rp := range fc.sendRepliesSnapshot() {
		if strings.Contains(rp.Text, "drained in") {
			found = true
			if rp.ChatID != -200 || rp.TopicID == nil || *rp.TopicID != 412 {
				t.Errorf("notice posted to %d/%v, want the TARGET -200/412", rp.ChatID, rp.TopicID)
			}
		}
	}
	if !found {
		t.Fatal("drain-notice must post to the target topic when the holder can't render")
	}
}

// TestDrain_StepBReRunIsIdempotent: re-submitting the SAME Step-B input to the
// target worker appends nothing — the presence check skips exactly as many
// copies as already landed (INV-3).
func TestDrain_StepBReRunIsIdempotent(t *testing.T) {
	b, _ := drainTestBroker(t)
	srcFile := queueRouteKey(drainSrc()).File()
	copies := make([]c3types.Inbound, 2)
	for i, id := range []int64{1, 2} {
		m := drainSrcMsg(id, "copy")
		m.DrainedFrom = srcFile
		m.ChatID, m.TopicID = -200, ptrI64(412)
		copies[i] = *m
	}
	w := newRouteWorker(context.Background(), drainDst(), time.Hour, b)
	defer w.Stop()
	submit := func() DrainAppendResult {
		ch := make(chan DrainAppendResult, 1)
		if !w.Submit(Job{Kind: JobDrainAppend, DrainAppend: &DrainAppendJob{From: srcFile, Messages: copies, ResultCh: ch}}) {
			t.Fatal("submit append job")
		}
		return <-ch
	}
	first := submit()
	if first.Err != nil || first.Appended != 2 || first.Skipped != 0 {
		t.Fatalf("first run: %+v", first)
	}
	second := submit()
	if second.Err != nil || second.Appended != 0 || second.Skipped != 2 {
		t.Fatalf("re-run of Step B must append ZERO new lines (presence check): %+v", second)
	}
	if got := drainPeekAll(t, b, drainDst()); len(got) != 2 {
		t.Fatalf("target must hold exactly 2 copies after the re-run, got %d", len(got))
	}
}

// TestDrain_CrashBetweenCopyAndRemove_ReissueConverges: copies landed (Step B)
// but the source was never cleaned (crash before Step C). Re-issuing the same
// drain presence-skips every copy, cleans the source, and leaves NO doubles
// in the target (INV-2 + INV-3).
func TestDrain_CrashBetweenCopyAndRemove_ReissueConverges(t *testing.T) {
	b, _ := drainTestBroker(t)
	drainSeed(t, b, drainSrc(), drainSrcMsg(1, "one"), drainSrcMsg(2, "two"))
	// Simulate the crashed prior attempt's landed copies: stamped with the
	// canonical source key, routed to the target — what Step B durably wrote.
	srcFile := queueRouteKey(drainSrc()).File()
	for _, id := range []int64{1, 2} {
		cp := drainSrcMsg(id, "landed earlier")
		cp.DrainedFrom = srcFile
		cp.ChatID, cp.TopicID = -200, ptrI64(412)
		if err := b.Queue.Append(queueRouteKey(drainDst()), cp); err != nil {
			t.Fatalf("simulate landed copy: %v", err)
		}
	}

	res, err := b.Drain(drainSpec())
	if err != nil {
		t.Fatalf("re-issue must converge, got: %v", err)
	}
	if res.Appended != 0 || res.PresenceSkipped != 2 {
		t.Fatalf("re-issue must presence-skip both copies (appended=%d skipped=%d)", res.Appended, res.PresenceSkipped)
	}
	if res.RemovedFromSource != 2 || res.AlreadyGone != 0 {
		t.Fatalf("re-issue must clean the source: %+v", res)
	}
	if got := drainPeekAll(t, b, drainSrc()); len(got) != 0 {
		t.Fatalf("source must finally be cleaned, still holds %d", len(got))
	}
	if got := drainPeekAll(t, b, drainDst()); len(got) != 2 {
		t.Fatalf("no double-append: target must hold exactly 2 copies, got %d", len(got))
	}
}

// TestDrain_DuplicateMessageIDMultiset_MovesFirstOccurrenceOnly: a queue can
// hold two lines with one MessageID (edited message re-dispatch, A2). Draining
// 1 moves the FIRST occurrence and leaves the second — the frozen selection
// addresses occurrences (lines), not unique ids.
func TestDrain_DuplicateMessageIDMultiset_MovesFirstOccurrenceOnly(t *testing.T) {
	b, _ := drainTestBroker(t)
	drainSeed(t, b, drainSrc(), drainSrcMsg(5, "original"), drainSrcMsg(5, "edited"))

	res, err := b.Drain(drainSpec(DrainSelector{Kind: SelectFirstN, N: 1}))
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if res.Requested != 1 || res.Appended != 1 || res.RemovedFromSource != 1 || res.AlreadyGone != 0 {
		t.Fatalf("multiset drain counts wrong: %+v", res)
	}
	srcLeft := drainPeekAll(t, b, drainSrc())
	if len(srcLeft) != 1 || srcLeft[0].MessageID != 5 || srcLeft[0].Text != "edited" {
		t.Fatalf("the SECOND same-id occurrence must stay on the source: %+v", srcLeft)
	}
	dstGot := drainPeekAll(t, b, drainDst())
	if len(dstGot) != 1 || !strings.HasSuffix(dstGot[0].Text, "\noriginal") {
		t.Fatalf("the FIRST occurrence must be the moved one: %+v", dstGot)
	}
}

// TestDrain_DuplicateMessageID_RangeSelectsLaterOccurrence: the wrong-line-
// removal regression. Pending [ord1={id5,"orig"}, ord2={id7,"mid"},
// ord3={id5,"edited"}] and a range selecting ONLY ordinal 3 must remove the
// LATER id-5 occurrence ("edited") and KEEP ordinal 1 ("orig") — a per-id
// count would have silently removed the first occurrence instead.
func TestDrain_DuplicateMessageID_RangeSelectsLaterOccurrence(t *testing.T) {
	b, _ := drainTestBroker(t)
	drainSeed(t, b, drainSrc(), drainSrcMsg(5, "orig"), drainSrcMsg(7, "mid"), drainSrcMsg(5, "edited"))

	res, err := b.Drain(drainSpec(DrainSelector{Kind: SelectRange, Lo: 3, Hi: 3}))
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if res.Requested != 1 || res.Appended != 1 || res.RemovedFromSource != 1 || res.AlreadyGone != 0 {
		t.Fatalf("range drain counts wrong: %+v", res)
	}
	srcLeft := drainPeekAll(t, b, drainSrc())
	if len(srcLeft) != 2 || srcLeft[0].MessageID != 5 || srcLeft[0].Text != "orig" ||
		srcLeft[1].MessageID != 7 || srcLeft[1].Text != "mid" {
		t.Fatalf("source must end [id5 orig, id7 mid] — ordinal 1 KEPT, ordinal 3 removed: %+v", srcLeft)
	}
	dstGot := drainPeekAll(t, b, drainDst())
	if len(dstGot) != 1 || dstGot[0].MessageID != 5 || !strings.HasSuffix(dstGot[0].Text, "\nedited") {
		t.Fatalf("the moved line must be the EDITED (later) occurrence: %+v", dstGot)
	}
}

// TestDrain_NoDedupSeed_OrganicSameIDStillDelivers pins INV-8: the drain
// append must NOT seed the target worker's deliveredDedup — an ORGANIC target
// message that happens to share a MessageID with a drained-in line must still
// persist (cross-chat ids collide freely).
func TestDrain_NoDedupSeed_OrganicSameIDStillDelivers(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	cc := mf.Channels["telegram"]
	cc.DebounceMS = 10 // fast debounce flush so the organic line lands quickly
	mf.Channels["telegram"] = cc
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	moved := drainSrcMsg(7, "moved")
	drainSeed(t, b, drainSrc(), moved)
	if _, err := b.Drain(drainSpec()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	// Same POOL worker that ran the drain append (60s idle, still alive): if
	// the drain had recorded id 7 in deliveredDedup, this organic inbound would
	// be suppressed as a "replay" and lost.
	organic := &c3types.Inbound{Channel: "telegram", ChatID: -200, TopicID: ptrI64(412), MessageID: 7, Text: "organic", Timestamp: time.Now()}
	if !b.Workers.Submit(drainDst(), Job{Kind: JobInbound, Inbound: organic}) {
		t.Fatal("submit organic inbound")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if n, _ := b.Queue.Pending(queueRouteKey(drainDst())); n == 2 {
			return
		}
		if time.Now().After(deadline) {
			n, _ := b.Queue.Pending(queueRouteKey(drainDst()))
			t.Fatalf("organic same-id message suppressed (target pending=%d, want 2) — drained ids must never seed deliveredDedup (INV-8)", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestDrain_SourceEqualsTarget_Rejected: asserted in Drain even though the
// command layer rejects it upstream (fail-closed).
func TestDrain_SourceEqualsTarget_Rejected(t *testing.T) {
	b, _ := drainTestBroker(t)
	drainSeed(t, b, drainSrc(), drainSrcMsg(1, "one"))
	spec := drainSpec()
	spec.Target = spec.Source
	_, err := b.Drain(spec)
	if !errors.Is(err, ErrDrainSameRoute) {
		t.Fatalf("want ErrDrainSameRoute, got %v", err)
	}
	if got := drainPeekAll(t, b, drainSrc()); len(got) != 1 {
		t.Fatal("source must be untouched on a same-route reject")
	}
}

// TestDrain_ConcurrentSecondDrainOnSameSource_Rejected: a second Drain issued
// while the first is genuinely mid-flight (between Steps B and C, via the test
// hook) rejects immediately with DrainInProgressError (A1), and the lock is
// released once the first finishes.
func TestDrain_ConcurrentSecondDrainOnSameSource_Rejected(t *testing.T) {
	b, _ := drainTestBroker(t)
	drainSeed(t, b, drainSrc(), drainSrcMsg(1, "one"))
	var second error
	drainTestHookAfterCopy = func() {
		_, second = b.Drain(drainSpec())
	}
	defer func() { drainTestHookAfterCopy = nil }()
	if _, err := b.Drain(drainSpec()); err != nil {
		t.Fatalf("first drain: %v", err)
	}
	drainTestHookAfterCopy = nil
	var inProg *DrainInProgressError
	if !errors.As(second, &inProg) {
		t.Fatalf("mid-flight second drain must reject with DrainInProgressError, got %v", second)
	}
	if inProg.Source != "genie" {
		t.Errorf("error must name the busy source: %+v", inProg)
	}
	// Lock released after the first drain: the next attempt reaches the (now
	// empty) source and reports EmptySourceError, not in-progress.
	_, err := b.Drain(drainSpec())
	var empty *EmptySourceError
	if !errors.As(err, &empty) {
		t.Fatalf("after the first drain the lock must be free (want EmptySourceError), got %v", err)
	}
}

// TestDrain_PartialSurvival_ConsumedBetweenFreezeAndRemove: a frozen line a
// concurrent fetch consumes between freeze and remove is REPORTED as already
// gone (INV-5 surviving intersection), never an error; its copy still landed
// (bounded double, house posture).
func TestDrain_PartialSurvival_ConsumedBetweenFreezeAndRemove(t *testing.T) {
	b, _ := drainTestBroker(t)
	drainSeed(t, b, drainSrc(), drainSrcMsg(1, "one"), drainSrcMsg(2, "two"))
	drainTestHookAfterCopy = func() {
		ch := make(chan FetchResult, 1)
		if !b.Workers.Submit(drainSrc(), Job{Kind: JobFetch, Fetch: &FetchJob{Limit: 1, Ack: true, ResultCh: ch}}) {
			t.Error("hook: submit consume failed")
			return
		}
		if r := <-ch; r.Err != nil || len(r.Messages) != 1 {
			t.Errorf("hook: consumed %d msgs err=%v, want 1", len(r.Messages), r.Err)
		}
	}
	defer func() { drainTestHookAfterCopy = nil }()

	res, err := b.Drain(drainSpec())
	if err != nil {
		t.Fatalf("partial survival must be a report, not an error: %v", err)
	}
	if res.Requested != 2 || res.Appended != 2 || res.RemovedFromSource != 1 || res.AlreadyGone != 1 {
		t.Fatalf("surviving-intersection report wrong: %+v", res)
	}
	found := false
	for _, wmsg := range res.Warnings {
		if strings.Contains(wmsg, "already gone") {
			found = true
		}
	}
	if !found {
		t.Fatalf("want an already-gone warning, got %v", res.Warnings)
	}
	if got := drainPeekAll(t, b, drainSrc()); len(got) != 0 {
		t.Fatalf("source must end empty (1 consumed + 1 removed), still holds %d", len(got))
	}
	if got := drainPeekAll(t, b, drainDst()); len(got) != 2 {
		t.Fatalf("both copies landed in the target (bounded double), got %d", len(got))
	}
}

// TestDrain_EmptySource_TypedError: nothing pending rejects before any
// mutation and without any advisory.
func TestDrain_EmptySource_TypedError(t *testing.T) {
	b, fc := drainTestBroker(t)
	_, err := b.Drain(drainSpec())
	var empty *EmptySourceError
	if !errors.As(err, &empty) {
		t.Fatalf("want EmptySourceError, got %v", err)
	}
	if empty.Source != "genie" {
		t.Errorf("error must name the source: %+v", empty)
	}
	if n := len(fc.sendRepliesSnapshot()); n != 0 {
		t.Fatalf("a rejected drain must send nothing, got %d sends", n)
	}
}

// TestDrain_RangeBeyondPending_TypedErrorCarriesPending: an explicit range
// past the queue rejects (never clamps) and the error carries the actual
// pending count for the correction hint.
func TestDrain_RangeBeyondPending_TypedErrorCarriesPending(t *testing.T) {
	b, _ := drainTestBroker(t)
	drainSeed(t, b, drainSrc(), drainSrcMsg(1, "a"), drainSrcMsg(2, "b"), drainSrcMsg(3, "c"))
	_, err := b.Drain(drainSpec(DrainSelector{Kind: SelectRange, Lo: 2, Hi: 5}))
	var re *RangeBeyondPendingError
	if !errors.As(err, &re) {
		t.Fatalf("want RangeBeyondPendingError, got %v", err)
	}
	if re.Pending != 3 || re.Lo != 2 || re.Hi != 5 {
		t.Fatalf("error must carry the actual pending count: %+v", re)
	}
	if !strings.Contains(err.Error(), "3 pending") {
		t.Errorf("error text must state the pending count: %q", err.Error())
	}
	if got := drainPeekAll(t, b, drainSrc()); len(got) != 3 {
		t.Fatal("source must be untouched on a rejected range")
	}
}

// TestDrain_BrokerBusy_TypedError: Workers.Submit returning false surfaces the
// typed broker-busy error naming the step (here: freeze — nothing changed).
func TestDrain_BrokerBusy_TypedError(t *testing.T) {
	b, _ := drainTestBroker(t)
	drainSeed(t, b, drainSrc(), drainSrcMsg(1, "a"))
	b.Workers.Stop()
	_, err := b.Drain(drainSpec())
	var busy *DrainBusyError
	if !errors.As(err, &busy) {
		t.Fatalf("want DrainBusyError, got %v", err)
	}
	if busy.Step != DrainStepFreeze || busy.CopiesLanded {
		t.Fatalf("busy at %q landed=%v, want the freeze step with nothing landed", busy.Step, busy.CopiesLanded)
	}
}

// TestDrain_LiveAttachedSourceWarning: draining a live-attached source is
// allowed (the blessed pool is unattached) but the result carries the ⚠
// warning for the reply (§7 edge table).
func TestDrain_LiveAttachedSourceWarning(t *testing.T) {
	b, _ := drainTestBroker(t)
	drainSeed(t, b, drainSrc(), drainSrcMsg(1, "a"))
	claimedHolder(t, b, drainSrc())

	res, err := b.Drain(drainSpec())
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	found := false
	for _, wmsg := range res.Warnings {
		if strings.Contains(wmsg, "live-attached") {
			found = true
		}
	}
	if !found {
		t.Fatalf("want the live-attached source warning, got %v", res.Warnings)
	}
}

// TestApplyDrainSelector_Table pins the selector contract: all / first-N
// clamps / explicit ranges reject past-pending / lo-hi validation / ordinals
// number occurrences (lines), not unique ids (A2).
func TestApplyDrainSelector_Table(t *testing.T) {
	pending := []c3types.Inbound{
		{MessageID: 5, Text: "first-5"},
		{MessageID: 5, Text: "second-5"},
		{MessageID: 9, Text: "nine"},
	}
	texts := func(in []c3types.Inbound) []string {
		out := make([]string, len(in))
		for i := range in {
			out[i] = in[i].Text
		}
		return out
	}
	cases := []struct {
		name      string
		sel       DrainSelector
		wantTexts []string
		wantLo    int
		wantHi    int
		clamped   bool
		wantErr   any // nil, or a pointer for errors.As
	}{
		{"all", DrainSelector{Kind: SelectAll}, []string{"first-5", "second-5", "nine"}, 1, 3, false, nil},
		{"firstN exact", DrainSelector{Kind: SelectFirstN, N: 2}, []string{"first-5", "second-5"}, 1, 2, false, nil},
		{"firstN clamps", DrainSelector{Kind: SelectFirstN, N: 10}, []string{"first-5", "second-5", "nine"}, 1, 3, true, nil},
		{"firstN zero rejects", DrainSelector{Kind: SelectFirstN, N: 0}, nil, 0, 0, false, new(*BadSelectorError)},
		{"range ordinals count occurrences", DrainSelector{Kind: SelectRange, Lo: 2, Hi: 2}, []string{"second-5"}, 2, 2, false, nil},
		{"range mid", DrainSelector{Kind: SelectRange, Lo: 2, Hi: 3}, []string{"second-5", "nine"}, 2, 3, false, nil},
		{"range lo below 1 rejects", DrainSelector{Kind: SelectRange, Lo: 0, Hi: 2}, nil, 0, 0, false, new(*BadSelectorError)},
		{"range inverted rejects", DrainSelector{Kind: SelectRange, Lo: 3, Hi: 2}, nil, 0, 0, false, new(*BadSelectorError)},
		{"range beyond pending rejects", DrainSelector{Kind: SelectRange, Lo: 2, Hi: 5}, nil, 0, 0, false, new(*RangeBeyondPendingError)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			captured, lo, hi, clamped, err := applyDrainSelector(pending, tc.sel)
			if tc.wantErr != nil {
				if err == nil {
					t.Fatalf("want error, got captured=%v", texts(captured))
				}
				if !errors.As(err, tc.wantErr) {
					t.Fatalf("wrong error type: %v", err)
				}
				// The beyond-pending reject must carry the actual pending count.
				var re *RangeBeyondPendingError
				if errors.As(err, &re) && re.Pending != 3 {
					t.Fatalf("RangeBeyondPendingError.Pending = %d, want 3", re.Pending)
				}
				return
			}
			if err != nil {
				t.Fatalf("applyDrainSelector: %v", err)
			}
			got := texts(captured)
			if len(got) != len(tc.wantTexts) {
				t.Fatalf("captured %v, want %v", got, tc.wantTexts)
			}
			for i := range got {
				if got[i] != tc.wantTexts[i] {
					t.Fatalf("captured %v, want %v", got, tc.wantTexts)
				}
			}
			if lo != tc.wantLo || hi != tc.wantHi || clamped != tc.clamped {
				t.Fatalf("window lo=%d hi=%d clamped=%v, want %d/%d/%v", lo, hi, clamped, tc.wantLo, tc.wantHi, tc.clamped)
			}
		})
	}
}
