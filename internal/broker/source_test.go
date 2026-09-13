package broker

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/intake"
	"github.com/Andrometiq/c3/internal/queue"
)

func brokerSource(in *c3types.Inbound, updateID int64) *intake.Source {
	source := &intake.Source{Channel: in.Channel, ChatID: in.ChatID, TopicID: in.TopicID,
		MessageID: in.MessageID, UpdateID: updateID, Text: in.Text, Attachments: []intake.SourceAttachment{}}
	for _, att := range in.Attachments {
		source.Attachments = append(source.Attachments, intake.SourceAttachment{Kind: att.Kind, FileID: att.FileID, Size: att.Size, MIME: att.MIME, Name: att.Name})
	}
	return source.Clone()
}

func brokerSourceJSON(t *testing.T, source *intake.Source) string {
	t.Helper()
	data, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func reopenedSourceRows(t *testing.T, dir string, route RouteKey) []queue.TrackedInbound {
	t.Helper()
	store, err := queue.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	rows, err := store.PeekTracked(queueRouteKey(route), -1)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func sourceWorkerSnapshot(t *testing.T, b *Broker, route RouteKey) []queue.TrackedInbound {
	t.Helper()
	ch := make(chan DrainPeekResult, 1)
	if !b.Workers.Submit(route, Job{Kind: JobDrainPeek, DrainPeek: &DrainPeekJob{ResultCh: ch}}) {
		t.Fatal("peek rejected")
	}
	select {
	case result := <-ch:
		if result.Err != nil {
			t.Fatal(result.Err)
		}
		return result.Tracked
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not return source snapshot")
		return nil
	}
}

func TestSourceHostDebouncePrimaryAppendAndEditRestart(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	in := inbound(914, 1, "first body")
	route := MakeRouteKey(in.Channel, in.ChatID, in.TopicID)
	first := brokerSource(in, 1001)
	wantFirst := brokerSourceJSON(t, first)
	host := NewBrokerHost(b, "telegram")
	if !host.Emit(in, first) {
		t.Fatal("original emit rejected")
	}
	first.Text = "caller mutation"
	*first.TopicID = 999
	in.Text = "caller presentation mutation"
	edit := inbound(914, 1, "edited body")
	edit.Edited = true
	second := brokerSource(edit, 1002)
	wantSecond := brokerSourceJSON(t, second)
	if !host.Emit(edit, second) {
		t.Fatal("edit emit rejected")
	}
	waitForVoiceCondition(t, "debounced source rows", func() bool { return len(sourceWorkerSnapshot(t, b, route)) == 2 })
	b.Shutdown()
	rows := reopenedSourceRows(t, dir, route)
	if len(rows) != 2 || brokerSourceJSON(t, rows[0].Source) != wantFirst || brokerSourceJSON(t, rows[1].Source) != wantSecond {
		t.Fatalf("restarted occurrences=%+v", rows)
	}
	if rows[0].Inbound.Text != "first body" || !rows[1].Inbound.Edited {
		t.Fatal("host ownership or edit identity changed")
	}
}

func TestSourceFallbackAppendRestart(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	in := inbound(914, 1, "fallback body")
	route := MakeRouteKey(in.Channel, in.ChatID, in.TopicID)
	w := newRouteWorker(context.Background(), route, time.Hour, b)
	w.Stop()
	source := brokerSource(in, 1001)
	want := brokerSourceJSON(t, source)
	item := occurrence(in, source)
	source.Text = "caller mutation"
	w.forwardOccurrences(context.Background(), in, []inboundOccurrence{item}, 0, nil, false)
	rows := reopenedSourceRows(t, dir, route)
	if len(rows) != 1 || brokerSourceJSON(t, rows[0].Source) != want {
		t.Fatalf("fallback source=%+v", rows)
	}
}

func TestSourcePendingAckHolderDeathRestoresOwnCopies(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	in := inbound(914, 1, "original body")
	route := MakeRouteKey(in.Channel, in.ChatID, in.TopicID)
	_, pushed := liveHolder(t, b, route)
	w := newRouteWorker(context.Background(), route, time.Hour, b)
	w.Stop()
	source := brokerSource(in, 1001)
	want := brokerSourceJSON(t, source)
	w.flushOccurrences(context.Background(), []inboundOccurrence{occurrence(in, source)})
	select {
	case <-pushed:
	case <-time.After(3 * time.Second):
		t.Fatal("live push missing")
	}
	before, err := b.Queue.PeekTracked(queueRouteKey(route), -1)
	if err != nil || len(before) != 1 || len(w.pendingAck) != 1 {
		t.Fatalf("rows=%d pending=%d err=%v", len(before), len(w.pendingAck), err)
	}
	source.Text = "caller mutation"
	in.Text = "caller presentation mutation"
	if _, err := b.Queue.Consume(queueRouteKey(route), -1); err != nil {
		t.Fatal(err)
	}
	w.flushPendingAck("session exited")
	rows := reopenedSourceRows(t, dir, route)
	if len(rows) != 1 || rows[0].RecordID == before[0].RecordID || rows[0].Inbound.Text != "original body" || brokerSourceJSON(t, rows[0].Source) != want {
		t.Fatalf("holder restore=%+v", rows)
	}
}

func TestSourceDrainPreservesOccurrenceAcrossDestinationRewrite(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	src, dst := drainSrc(), drainDst()
	in := &c3types.Inbound{Channel: src.Channel, ChatID: src.ChatID, TopicID: ptrI64(src.TopicID), MessageID: 9, Text: "caption", Timestamp: time.Now()}
	source := brokerSource(in, 1001)
	want := brokerSourceJSON(t, source)
	id, err := b.Queue.AppendTrackedSource(queueRouteKey(src), in, source, "audio-a")
	if err != nil {
		t.Fatal(err)
	}
	result, err := b.Drain(DrainSpec{Source: src, Target: dst, SourceName: "acme", TargetName: "example"})
	if err != nil || result.Appended != 1 || result.RemovedFromSource != 1 {
		t.Fatalf("drain=%+v err=%v", result, err)
	}
	b.Shutdown()
	rows := reopenedSourceRows(t, dir, dst)
	if len(rows) != 1 || rows[0].SourceRecordID != id || rows[0].Inbound.ChatID != dst.ChatID || *rows[0].Inbound.TopicID != dst.TopicID || len(rows[0].VoicePending) != 0 || brokerSourceJSON(t, rows[0].Source) != want {
		t.Fatalf("drain source=%+v", rows)
	}
}

func TestSourceSchedulerRestartTargetsKeepOwnOccurrenceInReplacement(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	b.Voice.Stop()
	route, original, _ := schedulerVoice(9, "shared-audio")
	original.Text = "first caption"
	edit := cloneVoiceInbound(original)
	edit.Edited = true
	edit.Text = "edited caption"
	edit.Attachments = append(edit.Attachments, c3types.Attachment{Kind: "document", FileID: "doc", Name: "acme.txt"})
	first, second := brokerSource(&original, 1001), brokerSource(&edit, 1002)
	wants := map[int64]string{1001: brokerSourceJSON(t, first), 1002: brokerSourceJSON(t, second)}
	w := newRouteWorker(context.Background(), route, time.Hour, b)
	w.Stop()
	w.flushOccurrences(context.Background(), []inboundOccurrence{occurrence(&original, first), occurrence(&edit, second)})
	before := reopenedSourceRows(t, dir, route)
	if len(before) != 2 {
		t.Fatalf("pending rows=%d", len(before))
	}
	var err error
	b.Queue, err = queue.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Queue.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	scheduler := newVoiceScheduler(b, newFakeVoiceClock())
	b.Voice = scheduler
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var attempts atomic.Int64
	scheduler.runAttempt = func(ctx context.Context, attempt voiceAttempt) voiceAttemptResult {
		attempts.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			return voiceAttemptResult{}
		}
		return voiceAttemptResult{success: true, segmentText: "resolved transcript", transcript: "resolved transcript"}
	}
	scheduler.RecoverPending()
	key := voiceScheduleKey{route: route, messageID: original.MessageID, fileID: "shared-audio"}
	scheduler.mu.Lock()
	entry := scheduler.entries[key]
	if entry == nil || len(entry.targets) != 2 {
		scheduler.mu.Unlock()
		t.Fatal("restart did not attach both durable targets")
	}
	// Poison the shared audio presentation: replacements must use each target.
	entry.inbound.Attachments = nil
	entry.inbound.ChatID = 999
	scheduler.mu.Unlock()
	first.Attachments[0].FileID = "caller mutation"
	second.Text = "caller mutation"
	if _, err := b.Queue.Consume(queueRouteKey(route), -1); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	waitForVoiceCondition(t, "both replacement occurrences", func() bool {
		rows := sourceWorkerSnapshot(t, b, route)
		return len(rows) == 2 && len(rows[0].VoicePending) == 0 && len(rows[1].VoicePending) == 0
	})
	b.Shutdown()
	rows := reopenedSourceRows(t, dir, route)
	seen := map[int64]bool{}
	for _, row := range rows {
		if row.Source == nil {
			t.Fatal("replacement lost source")
		}
		update := row.Source.UpdateID
		if brokerSourceJSON(t, row.Source) != wants[update] || row.Inbound.ChatID != route.ChatID {
			t.Fatalf("replacement occurrence=%+v", row)
		}
		expectedAttachments := 1
		if update == 1002 {
			expectedAttachments = 2
		}
		if len(row.Inbound.Attachments) != expectedAttachments {
			t.Fatalf("update %d attachments=%+v", update, row.Inbound.Attachments)
		}
		if row.RecordID == before[0].RecordID || row.RecordID == before[1].RecordID {
			t.Fatal("test did not exercise replacement")
		}
		seen[update] = true
	}
	if !seen[1001] || !seen[1002] || attempts.Load() != 1 {
		t.Fatalf("occurrences=%v audio attempts=%d", seen, attempts.Load())
	}
}
