package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/intake"
	"github.com/Andrometiq/c3/internal/queue"
)

// Keep scheduler dispatch explicit so failures and retry durability are observable.
func voiceBudgetHarness(t *testing.T) (*Broker, *RouteWorker, *VoiceScheduler, *gateChannel, chan *ResolveVoiceJob, string) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	g := newGateChannel(100, nil)
	b := brokerWithChannel(t, mfWithTelegram(), g.fakeChannel)
	registerGateChannel(b, g)
	b.Voice.Stop()
	b.Workers.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	s := &VoiceScheduler{
		broker: b, ctx: ctx, cancel: cancel, accepting: true,
		entries: make(map[voiceScheduleKey]*voiceEntry), clock: newFakeVoiceClock(),
		wake: make(chan struct{}, 1), stopped: make(chan struct{}), resolveDelay: time.Second,
	}
	jobs := make(chan *ResolveVoiceJob, 8)
	s.submit = func(_ RouteKey, job Job) bool { jobs <- job.ResolveVoice; return true }
	b.Voice = s
	route, _, _ := schedulerVoice(9, "audio-a")
	w := newRouteWorker(context.Background(), route, time.Hour, b)
	w.Stop()
	t.Cleanup(b.Shutdown)
	return b, w, s, g, jobs, dir
}

func budgetGroup() *voiceGroup {
	head := make(chan struct{})
	close(head)
	return &voiceGroup{
		finished: make(map[voiceScheduleKey]bool), transcripts: make(map[string]string),
		echo: voiceEchoReservation{prev: head, mine: make(chan struct{})},
	}
}

func addBudgetTarget(s *VoiceScheduler, w *RouteWorker, group *voiceGroup, in c3types.Inbound, source *intake.Source, id, fileID, raw string) voiceScheduleKey {
	key := voiceScheduleKey{route: w.key, messageID: in.MessageID, fileID: fileID}
	outcome := intake.STTOutcome{STT: intake.STTDone, Transcript: raw}
	target := voiceResolveTarget{recordID: id, inbound: in, source: source, group: group,
		attachmentsState: intake.NewAttachmentsState(source, group.order)}
	entry := &voiceEntry{
		key: key, state: voiceResolveReady, groups: []*voiceGroup{group},
		targets: map[string]voiceResolveTarget{id: target}, applied: make(map[string]bool),
		resolve: voiceResolve{outcome: outcome, success: true, transcript: raw, segmentText: "[Transcribed voice]: " + raw},
	}
	s.entries[key] = entry
	s.finishGroupMemberLocked(entry, group)
	return key
}

func runBudgetResolve(t *testing.T, w *RouteWorker, s *VoiceScheduler, jobs chan *ResolveVoiceJob, key voiceScheduleKey) {
	t.Helper()
	s.submitResolve(key)
	select {
	case job := <-jobs:
		w.handleResolveVoice(context.Background(), job)
	default:
		t.Fatal("resolve was not submitted")
	}
}

func waitBudgetEcho(t *testing.T, group *voiceGroup) {
	t.Helper()
	select {
	case <-group.echo.mine:
	case <-time.After(3 * time.Second):
		t.Fatal("readback did not finish")
	}
}

func TestVoiceBudgetPersistsBeforeReadbackAndRetriesUncertainWrites(t *testing.T) {
	for _, revision := range []bool{false, true} {
		for _, oversize := range []bool{false, true} {
			for _, failSync := range []bool{false, true} {
				t.Run(fmt.Sprintf("revision=%v/oversize=%v/sync-failure=%v", revision, oversize, failSync), func(t *testing.T) {
					b, w, s, g, jobs, dir := voiceBudgetHarness(t)
					_, in, _ := schedulerVoice(9, "audio-a")
					in.Text = "caption\n" + voicePendingText("audio-a")
					source := brokerSource(&in, 1001)
					qrk := queueRouteKey(w.key)
					id, err := b.Queue.AppendTrackedSource(qrk, &in, source, "audio-a")
					if err != nil {
						t.Fatal(err)
					}
					if revision {
						if _, err := b.Queue.Consume(qrk, -1); err != nil {
							t.Fatal(err)
						}
					}
					raw := "  raw transcript\nverbatim  "
					if oversize {
						raw = strings.Repeat("x", queue.MaxRecordBytes/2)
					}
					group := budgetGroup()
					group.order, group.remaining = []string{"audio-a"}, 1
					key := addBudgetTarget(s, w, group, in, source, id, "audio-a", raw)
					injected := errors.New("injected post-write directory fsync failure")
					if failSync {
						b.Queue.SyncDirTestHook = func() error { return injected }
					}
					runBudgetResolve(t, w, s, jobs, key)
					if failSync {
						for attempt := 0; attempt < 2; attempt++ {
							rows, err := b.Queue.PeekTracked(qrk, -1)
							if err != nil || len(rows) != 1 || len(rows[0].VoicePending) != 0 {
								t.Fatalf("failure must leave exactly one visible terminal row: rows=%d err=%v", len(rows), err)
							}
							if _, terminal := rows[0].AttachmentsState.TerminalOutcome(rows[0].Source, "audio-a"); !terminal {
								t.Fatal("visible bytes have no terminal marker")
							}
							if len(g.readbackSnapshot()) != 0 || len(exceptionalReplies(g.sendRepliesSnapshot())) != 0 {
								t.Fatal("readback escaped before durability was re-established")
							}
							entry := s.entries[key]
							if entry == nil || entry.applied[id] || entry.state != voiceResolveReady {
								t.Fatal("uncertain write was not rescheduled")
							}
							s.clock.(*fakeVoiceClock).Advance(s.resolveDelay)
							if attempt == 1 {
								b.Queue.SyncDirTestHook = nil
							}
							runBudgetResolve(t, w, s, jobs, key)
						}
					}
					waitBudgetEcho(t, group)
					if s.entries[key] != nil {
						t.Fatal("durable resolve remains scheduled")
					}
					// A later dispatcher pass must not generate another readback or revision.
					s.clock.(*fakeVoiceClock).Advance(s.resolveDelay)
					s.submitResolve(key)
					select {
					case <-jobs:
						t.Fatal("completed outcome was resubmitted")
					default:
					}
					b.Shutdown()
					rows := reopenedSourceRows(t, dir, w.key)
					if len(rows) != 1 || !source.SameOccurrence(rows[0].Source) || len(rows[0].VoicePending) != 0 {
						t.Fatalf("recovered terminal occurrence count=%d", len(rows))
					}
					want := intake.STTOutcome{STT: intake.STTDone, Transcript: raw}
					notice := fmt.Sprintf(voiceTranscriptTooLargeText, queue.MaxRecordBytes)
					segment := "[Transcribed voice]: " + raw
					if oversize {
						want = intake.STTOutcome{STT: intake.STTFailed, Error: "transcript_too_large"}
						segment = notice
					}
					final, terminal := rows[0].AttachmentsState.TerminalOutcome(rows[0].Source, "audio-a")
					if !terminal || final != want {
						t.Fatal("recovered outcome differs from persisted budget decision")
					}
					wantText := "caption\n" + segment
					if revision {
						wantText = "[transcript update for voice message 9]\n" + segment
					}
					if rows[0].Inbound.Text != wantText {
						t.Fatal("durable presentation does not match final outcome")
					}
					data, err := os.ReadFile(filepath.Join(dir, qrk.File()+".jsonl"))
					if err != nil {
						t.Fatal(err)
					}
					if len(bytes.TrimSpace(data)) > queue.MaxRecordBytes || bytes.Count(data, []byte{'\n'}) != 1 {
						t.Fatal("disk does not contain exactly one bounded record")
					}
					if oversize {
						if bytes.Contains(data, []byte(`"transcript":`)) || len(g.readbackSnapshot()) != 0 {
							t.Fatal("oversize transcript was stored or echoed as success")
						}
						replies := exceptionalReplies(g.sendRepliesSnapshot())
						if len(replies) != 1 || !strings.Contains(replies[0].Text, notice) || strings.Contains(replies[0].Text, "does not need to resend") {
							t.Fatalf("oversize readback must say resend shorter exactly once: %+v", replies)
						}
					} else {
						readbacks := g.readbackSnapshot()
						if len(readbacks) != 1 || readbacks[0].Transcript != raw {
							t.Fatalf("raw success readback=%+v", readbacks)
						}
					}
				})
			}
		}
	}
}

func TestVoiceBudgetWaitsForSiblingPersistenceAndPreservesItsSegment(t *testing.T) {
	b, w, s, g, jobs, dir := voiceBudgetHarness(t)
	_, in, _ := schedulerVoice(9, "audio-a")
	in.Attachments = append(in.Attachments, c3types.Attachment{Kind: "voice", FileID: "audio-b"})
	in.Text = "caption\n" + voicePendingText("audio-a") + "\n" + voicePendingText("audio-b")
	source := brokerSource(&in, 1001)
	id, err := b.Queue.AppendTrackedSource(queueRouteKey(w.key), &in, source, "audio-a", "audio-b")
	if err != nil {
		t.Fatal(err)
	}
	group := budgetGroup()
	group.order, group.remaining = []string{"audio-a", "audio-b"}, 2
	first := addBudgetTarget(s, w, group, in, source, id, "audio-a", "small sibling")
	second := addBudgetTarget(s, w, group, in, source, id, "audio-b", strings.Repeat("x", queue.MaxRecordBytes/2))
	runBudgetResolve(t, w, s, jobs, first)
	if len(g.readbackSnapshot()) != 0 || len(exceptionalReplies(g.sendRepliesSnapshot())) != 0 {
		t.Fatal("group readback ran before the second budget was resolved")
	}
	rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if err != nil || len(rows) != 1 || rows[0].AttachmentsState[1].STT != intake.STTPending {
		t.Fatal("scheduler's unbudgeted sibling outcome entered durable metadata")
	}
	runBudgetResolve(t, w, s, jobs, second)
	waitBudgetEcho(t, group)
	b.Shutdown()
	rows = reopenedSourceRows(t, dir, w.key)
	notice := fmt.Sprintf(voiceTranscriptTooLargeText, queue.MaxRecordBytes)
	if len(rows) != 1 || rows[0].Inbound.Text != "caption\n[Transcribed voice]: small sibling\n"+notice || rows[0].AttachmentsState[1].Error != "transcript_too_large" {
		t.Fatal("budget fallback damaged the sibling or was not recovered")
	}
	readbacks := g.readbackSnapshot()
	if len(readbacks) != 1 || readbacks[0].Transcript != "small sibling" {
		t.Fatalf("readback included unpersisted sibling: %+v", readbacks)
	}
	replies := exceptionalReplies(g.sendRepliesSnapshot())
	if len(replies) != 1 || !strings.Contains(replies[0].Text, notice) {
		t.Fatalf("missing sibling oversize notice: %+v", replies)
	}
	encoded, err := json.Marshal(rows[0].AttachmentsState)
	if err != nil || bytes.Contains(encoded, []byte(strings.Repeat("x", 100))) {
		t.Fatal("recovered metadata retained the rejected raw transcript")
	}
}

func TestVoiceBudgetIsPerOccurrenceTarget(t *testing.T) {
	for _, revision := range []bool{false, true} {
		t.Run(fmt.Sprintf("revision=%v", revision), func(t *testing.T) {
			b, w, s, g, jobs, dir := voiceBudgetHarness(t)
			_, in, _ := schedulerVoice(9, "audio-a")
			in.Text = voicePendingText("audio-a")
			raw := strings.Repeat("x", 350000)
			var groups []*voiceGroup
			var shared *voiceEntry
			var key voiceScheduleKey
			for i := int64(0); i < 2; i++ {
				source := brokerSource(&in, 1001+i)
				if i == 1 {
					source.Text = strings.Repeat("source ", 60000)
				}
				id, err := b.Queue.AppendTrackedSource(queueRouteKey(w.key), &in, source, "audio-a")
				if err != nil {
					t.Fatal(err)
				}
				group := budgetGroup()
				group.order, group.remaining = []string{"audio-a"}, 1
				groups = append(groups, group)
				key = addBudgetTarget(s, w, group, in, source, id, "audio-a", raw)
				if shared == nil {
					shared = s.entries[key]
				} else {
					shared.targets[id] = s.entries[key].targets[id]
					shared.groups = append(shared.groups, group)
					s.entries[key] = shared
				}
			}
			if revision {
				if _, err := b.Queue.Consume(queueRouteKey(w.key), -1); err != nil {
					t.Fatal(err)
				}
			}
			runBudgetResolve(t, w, s, jobs, key)
			for _, group := range groups {
				waitBudgetEcho(t, group)
			}
			b.Shutdown()
			rows := reopenedSourceRows(t, dir, w.key)
			if len(rows) != 2 {
				t.Fatalf("recovered targets=%d", len(rows))
			}
			for _, row := range rows {
				final, terminal := row.AttachmentsState.TerminalOutcome(row.Source, "audio-a")
				if !terminal {
					t.Fatal("target has no terminal result")
				}
				if row.Source.UpdateID == 1001 {
					if final.STT != intake.STTDone || final.Transcript != raw {
						t.Fatal("small target inherited another target's budget failure")
					}
				} else if final.STT != intake.STTFailed || final.Error != "transcript_too_large" || final.Transcript != "" {
					t.Fatal("large source overhead was omitted from the target budget")
				}
			}
			readbacks := g.readbackSnapshot()
			replies := exceptionalReplies(g.sendRepliesSnapshot())
			if len(readbacks) != 1 || readbacks[0].Transcript != raw || len(replies) != 1 || !strings.Contains(replies[0].Text, "resend it in shorter parts") {
				t.Fatal("readbacks did not reflect the two independent persisted budgets")
			}
		})
	}
}
