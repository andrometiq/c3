package broker

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/intake"
	"github.com/Andrometiq/c3/internal/queue"
)

func TestAttachmentStateWorkerTerminalOutcomesRestart(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "done"
		if failed {
			name = "failed"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("C3_QUEUE_DIR", dir)
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			registerGateChannel(b, newGateChannel(100, nil))
			defer b.Shutdown()
			raw := "  raw words\nwith whitespace  "
			b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
				if failed {
					return "[STT FAILED: provider_unavailable]", nil
				}
				return raw, nil
			})
			route, in, _ := schedulerVoice(9, "audio-a")
			in.Text = "caption"
			in.Attachments = append(in.Attachments, c3types.Attachment{Kind: "photo", FileID: "photo"}, c3types.Attachment{Kind: "voice"})
			source := brokerSource(&in, 1001)
			if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &in, source: source}) {
				t.Fatal("submit rejected")
			}
			waitForVoiceCondition(t, "terminal metadata", func() bool {
				rows := sourceWorkerSnapshot(t, b, route)
				return len(rows) == 1 && len(rows[0].VoicePending) == 0
			})
			b.Shutdown()
			rows := reopenedSourceRows(t, dir, route)
			if len(rows) != 1 || len(rows[0].AttachmentsState) != 3 {
				t.Fatalf("rows=%+v", rows)
			}
			states := rows[0].AttachmentsState
			if states[1].STT != intake.STTNotApplicable || states[2].STT != intake.STTFailed || states[2].Error != "missing_file_id" {
				t.Fatalf("non-scheduled states=%+v", states)
			}
			if failed {
				if states[0].STT != intake.STTFailed || states[0].Error != "provider_unavailable" || states[0].Transcript != "" {
					t.Fatalf("failure=%+v", states[0])
				}
			} else if states[0].STT != intake.STTDone || states[0].Transcript != raw || states[0].Error != "" || !strings.Contains(rows[0].Inbound.Text, "[Transcribed voice]: "+raw) {
				t.Fatalf("success=%+v text=%q", states[0], rows[0].Inbound.Text)
			}
		})
	}
}

func TestAttachmentStateFallbackAndPendingAckRestorationOwnCopies(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	route, in, _ := schedulerVoice(9, "audio-a")
	in.Text = "[Transcribed voice]: raw words"
	source := brokerSource(&in, 1001)
	want := intake.AttachmentsState{{Index: 0, STT: intake.STTDone, Transcript: "raw words"}}
	states := want.Clone()
	w := newRouteWorker(context.Background(), route, time.Hour, b)
	w.Stop()
	item := occurrence(&in, source, states)
	states[0].Transcript = "caller mutation"
	w.forwardOccurrences(context.Background(), &in, []inboundOccurrence{item}, 0, nil, false)
	rows := reopenedSourceRows(t, dir, route)
	if len(rows) != 1 || !reflect.DeepEqual(rows[0].AttachmentsState, want) {
		t.Fatalf("fallback=%+v", rows)
	}
	_, pushed := liveHolder(t, b, route)
	w.forwardOccurrences(context.Background(), &in, []inboundOccurrence{item}, 1, []string{rows[0].RecordID}, true)
	select {
	case <-pushed:
	case <-time.After(3 * time.Second):
		t.Fatal("push missing")
	}
	if len(w.pendingAck) != 1 || !reflect.DeepEqual(w.pendingAck[0].sources[0].attachmentsState, want) {
		t.Fatalf("pending ack=%+v", w.pendingAck)
	}
	item.attachmentsState[0].Transcript = "post-push mutation"
	if _, err := b.Queue.Consume(queueRouteKey(route), -1); err != nil {
		t.Fatal(err)
	}
	w.flushPendingAck("session exited")
	restored := reopenedSourceRows(t, dir, route)
	if len(restored) != 1 || restored[0].RecordID == rows[0].RecordID || !reflect.DeepEqual(restored[0].AttachmentsState, want) {
		t.Fatalf("restored=%+v", restored)
	}
}

func TestAttachmentStateDrainFreezesSnapshot(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	src, dst := drainSrc(), drainDst()
	in := &c3types.Inbound{Channel: src.Channel, ChatID: src.ChatID, TopicID: ptrI64(src.TopicID), MessageID: 9, Text: "caption", Timestamp: time.Now(), Attachments: []c3types.Attachment{{Kind: "voice", FileID: "audio-a"}, {Kind: "voice", FileID: "audio-b"}}}
	source := brokerSource(in, 1001)
	states := intake.AttachmentsState{{Index: 0, STT: intake.STTDone, Transcript: "raw words"}, {Index: 1, STT: intake.STTPending}}
	want := states.Clone()
	id, err := b.Queue.AppendTrackedIntake(queueRouteKey(src), in, source, states, "audio-b")
	if err != nil {
		t.Fatal(err)
	}
	states[0].Transcript = "caller mutation"
	result, err := b.Drain(DrainSpec{Source: src, Target: dst, SourceName: "acme", TargetName: "example"})
	if err != nil || result.Appended != 1 {
		t.Fatalf("drain=%+v err=%v", result, err)
	}
	b.Shutdown()
	rows := reopenedSourceRows(t, dir, dst)
	if len(rows) != 1 || rows[0].SourceRecordID != id || len(rows[0].VoicePending) != 0 || !reflect.DeepEqual(rows[0].AttachmentsState, want) {
		t.Fatalf("frozen=%+v", rows)
	}
	store, err := queue.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	if recovered := store.PendingVoiceRowsAll(); len(recovered) != 0 {
		t.Fatalf("frozen copy eligible for recovery: %+v", recovered)
	}
}

func TestAttachmentStateSchedulerRestartReplacementFullVector(t *testing.T) {
	for _, manual := range []bool{false, true} {
		name := "automatic"
		if manual {
			name = "manual lease promoted by recovery"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("C3_QUEUE_DIR", dir)
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			defer b.Shutdown()
			b.Voice.Stop()
			route, in, _ := schedulerVoice(9, "audio-a")
			in.Attachments = append(in.Attachments, c3types.Attachment{Kind: "voice", FileID: "audio-b"}, c3types.Attachment{Kind: "voice", FileID: "audio-a"}, c3types.Attachment{Kind: "photo", FileID: "photo"}, c3types.Attachment{Kind: "voice", FileID: "already-done"})
			source := brokerSource(&in, 1001)
			states := intake.NewAttachmentsState(source, []string{"audio-a", "audio-b"})
			states[4] = intake.AttachmentState{Index: 4, STT: intake.STTDone, Transcript: "previous raw"}
			id, err := b.Queue.AppendTrackedIntake(queueRouteKey(route), &in, source, states, "audio-a", "audio-b")
			if err != nil {
				t.Fatal(err)
			}
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
			var once sync.Once
			defer once.Do(func() { close(release) })
			scheduler.runAttempt = func(ctx context.Context, attempt voiceAttempt) voiceAttemptResult {
				select {
				case <-release:
				case <-ctx.Done():
					return voiceAttemptResult{}
				}
				if attempt.key.fileID == "audio-b" {
					return voiceAttemptResult{detail: "provider_unavailable", segmentText: "failure presentation"}
				}
				return voiceAttemptResult{success: true, transcript: "new raw", segmentText: "[Transcribed voice]: new raw"}
			}
			if manual {
				if err := scheduler.ScheduleManual(route, "", in, in.Attachments[0], false, nil); err != nil {
					t.Fatal(err)
				}
			}
			scheduler.RecoverPending()
			// Consume after the targets snapshot the restarted row, forcing replacements.
			if _, err := b.Queue.Consume(queueRouteKey(route), -1); err != nil {
				t.Fatal(err)
			}
			states[4].Transcript = "caller mutation"
			once.Do(func() { close(release) })
			waitForVoiceCondition(t, "both replacement vectors", func() bool { return len(sourceWorkerSnapshot(t, b, route)) == 2 })
			b.Shutdown()
			rows := reopenedSourceRows(t, dir, route)
			want := intake.AttachmentsState{
				{Index: 0, STT: intake.STTDone, Transcript: "new raw"},
				{Index: 1, STT: intake.STTFailed, Error: "provider_unavailable"},
				{Index: 2, STT: intake.STTDone, Transcript: "new raw"},
				{Index: 3, STT: intake.STTNotApplicable},
				{Index: 4, STT: intake.STTDone, Transcript: "previous raw"},
			}
			if len(rows) != 2 {
				t.Fatalf("rows=%+v", rows)
			}
			for _, row := range rows {
				if row.RecordID == id || len(row.Inbound.Attachments) != 5 || !reflect.DeepEqual(row.Source, source) || !reflect.DeepEqual(row.AttachmentsState, want) {
					t.Fatalf("replacement=%+v", row)
				}
			}
		})
	}
}
