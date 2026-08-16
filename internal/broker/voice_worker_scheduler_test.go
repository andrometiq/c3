package broker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/queue"
)

func TestVoiceWorkerMultiVoiceCompletesOnceInEitherOrder(t *testing.T) {
	tests := []struct {
		name           string
		fetchErr       map[string]error
		wantTranscript []string
		wantAgent      []string
		wantNotice     []string
	}{
		{name: "two transcripts", wantTranscript: []string{"words V1", "words V2"}},
		{name: "transcript and refusal", fetchErr: map[string]error{"V2": tooBigErr()}, wantTranscript: []string{"words V1"}, wantAgent: []string{"[voice too big:"}, wantNotice: []string{"over the bot server's size limit"}},
		{name: "both refused", fetchErr: map[string]error{
			"V1": tooBigErr(),
			"V2": errors.New("telegram: GetFile: Bad Request: file reference expired"),
		}, wantAgent: []string{"[voice too big:", "[voice download failed:"}, wantNotice: []string{"over the bot server's size limit", "file reference expired"}},
	}
	orders := [][]string{{"V1", "V2"}, {"V2", "V1"}}
	for _, tc := range tests {
		for _, order := range orders {
			orderName := order[0] + "-then-" + order[1]
			t.Run(tc.name+"/"+orderName, func(t *testing.T) {
				g := newGateChannel(100, nil)
				started := map[string]chan struct{}{"V1": make(chan struct{}), "V2": make(chan struct{})}
				release := map[string]chan struct{}{"V1": make(chan struct{}), "V2": make(chan struct{})}
				g.answer = func(fileID string) (int64, error) {
					close(started[fileID])
					<-release[fileID]
					return 100, tc.fetchErr[fileID]
				}
				b := gateBroker(t, g)
				defer b.Shutdown()
				setFastVoiceDebounce(b)
				b.Plugins.OnVoiceReceived(func(_ context.Context, p c3types.VoicePayload) (string, error) {
					return "words " + p.FileID, nil
				})
				route := MakeRouteKey("telegram", -100, nil)
				_, pushes := liveHolderFrames(t, b, route)
				in := c3types.Inbound{
					Channel: "telegram", ChatID: -100, MessageID: 8901,
					Attachments: []c3types.Attachment{{Kind: "voice", FileID: "V1"}, {Kind: "voice", FileID: "V2"}},
				}
				if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &in}) {
					t.Fatal("multi-voice submit rejected")
				}
				<-started["V1"]
				<-started["V2"]
				b.Workers.mu.Lock()
				worker := b.Workers.workers[route]
				var echoDone <-chan struct{}
				if worker != nil {
					echoDone = worker.prevEchoDone
				}
				b.Workers.mu.Unlock()
				if echoDone == nil {
					t.Fatal("voice row did not reserve its echo-chain link")
				}
				for i, fileID := range order {
					close(release[fileID])
					if i == len(order)-1 {
						continue
					}
					key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: fileID}
					waitForVoiceCondition(t, fileID+" terminal completion", func() bool {
						state, _, ok := schedulerEntrySnapshot(b.Voice, key)
						return ok && (state == voiceResolveReady || state == voiceResolveSubmitted)
					})
				}
				push := waitInboundPush(t, pushes)
				for _, want := range append(append([]string{}, tc.wantTranscript...), tc.wantAgent...) {
					if !strings.Contains(push.Inbound.Text, want) {
						t.Fatalf("one grouped push is missing %q: %q", want, push.Inbound.Text)
					}
				}
				select {
				case extra := <-pushes:
					t.Fatalf("multi-voice row woke the agent more than once: %+v", extra)
				case <-time.After(100 * time.Millisecond):
				}
				if len(tc.wantTranscript) > 0 {
					rbs := g.waitReadbacks(t, 1)
					if len(rbs) != 1 {
						t.Fatalf("joined transcript readback count=%d, want 1", len(rbs))
					}
					for _, want := range tc.wantTranscript {
						if !strings.Contains(rbs[0].Transcript, want) {
							t.Fatalf("joined readback missing %q: %q", want, rbs[0].Transcript)
						}
					}
				} else if got := len(g.readbackSnapshot()); got != 0 {
					t.Fatalf("refusal-only result sent %d transcript readback(s), want 0", got)
				}
				if len(tc.wantNotice) > 0 {
					g.waitReplyContaining(t, tc.wantNotice[len(tc.wantNotice)-1])
					replies := g.sendRepliesSnapshot()
					if len(replies) != 1 {
						t.Fatalf("joined refusal notice count=%d, want 1: %+v", len(replies), replies)
					}
					for _, want := range tc.wantNotice {
						if !strings.Contains(replies[0].Text, want) {
							t.Fatalf("joined refusal notice missing %q: %q", want, replies[0].Text)
						}
					}
				} else if got := len(g.sendRepliesSnapshot()); got != 0 {
					t.Fatalf("transcript-only result sent %d refusal notice(s), want 0", got)
				}
				select {
				case <-echoDone:
				case <-time.After(2 * time.Second):
					t.Fatal("grouped terminal path leaked its echo-chain reservation")
				}
			})
		}
	}
}

func TestVoiceWorkerEmptyFileIDReachesAgentAndHuman(t *testing.T) {
	g := newGateChannel(100, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()
	setFastVoiceDebounce(b)
	route := MakeRouteKey("telegram", -100, nil)
	_, pushes := liveHolderFrames(t, b, route)
	in := c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 8990,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: ""}},
	}
	if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &in}) {
		t.Fatal("malformed voice submit rejected")
	}
	push := waitInboundPush(t, pushes)
	if !strings.Contains(push.Inbound.Text, "missing_file_id") {
		t.Fatalf("agent did not receive malformed voice failure: %q", push.Inbound.Text)
	}
	g.waitReplyContaining(t, "Couldn't transcribe")
	if got := len(g.sendRepliesSnapshot()); got != 1 {
		t.Fatalf("malformed voice human notice count=%d, want 1", got)
	}
}

func setFastVoiceDebounce(b *Broker) {
	b.SetMappings(&mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {DebounceMS: 10, DebounceMaxMessages: 50},
		},
	})
}

func waitFetchResult(t *testing.T, result <-chan FetchResult) FetchResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("route worker did not answer fetch while STT was running")
		return FetchResult{}
	}
}

func TestVoiceWorkerPersistsBeforeSTTAndKeepsServingRoute(t *testing.T) {
	g := newGateChannel(100, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()
	setFastVoiceDebounce(b)

	persisted := make(chan struct{})
	var persistedOnce sync.Once
	var persistedBeforeSTT atomic.Bool
	b.SetPersistedCallback(func(*c3types.Inbound) {
		persistedBeforeSTT.Store(true)
		persistedOnce.Do(func() { close(persisted) })
	})
	sttStarted := make(chan struct{})
	releaseSTT := make(chan struct{})
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		close(sttStarted)
		if !persistedBeforeSTT.Load() {
			t.Error("STT began before markPersisted advanced the source offset")
		}
		<-releaseSTT
		return "eventual transcript", nil
	})

	route, in, _ := schedulerVoice(8001, "voice-blocked")
	if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &in}) {
		t.Fatal("inbound submit rejected")
	}
	select {
	case <-persisted:
	case <-time.After(3 * time.Second):
		t.Fatal("voice row was not persisted promptly")
	}
	select {
	case <-sttStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("STT did not start")
	}

	tracked, err := b.Queue.PeekTracked(queueRouteKey(route), -1)
	if err != nil || len(tracked) != 1 {
		t.Fatalf("durable placeholder rows=%+v err=%v", tracked, err)
	}
	if len(tracked[0].VoicePending) != 1 || tracked[0].VoicePending[0] != "voice-blocked" ||
		!strings.Contains(tracked[0].Inbound.Text, "transcription in progress") {
		t.Fatalf("STT started without an honest durable pending row: %+v", tracked[0])
	}

	fetchCh := make(chan FetchResult, 1)
	if !b.Workers.Submit(route, Job{Kind: JobFetch, Fetch: &FetchJob{All: true, ResultCh: fetchCh}}) {
		t.Fatal("fetch submit rejected while STT was running")
	}
	fetched := waitFetchResult(t, fetchCh)
	if fetched.Err != nil || len(fetched.Messages) != 1 || !strings.Contains(fetched.Messages[0].Text, "transcription in progress") {
		t.Fatalf("fetch while STT ran=%+v; want the durable placeholder", fetched)
	}

	outboundCh := make(chan OutboundResult, 1)
	if !b.Workers.Submit(route, Job{Kind: JobOutbound, Outbound: &OutboundJob{
		Tool: "not_a_real_tool", ResultCh: outboundCh,
	}}) {
		t.Fatal("outbound submit rejected while STT was running")
	}
	select {
	case <-outboundCh:
	case <-time.After(3 * time.Second):
		t.Fatal("outbound job was blacked out by STT")
	}

	close(releaseSTT)
	waitForVoiceQueueText(t, b, route, in.MessageID, func(text string) bool {
		return strings.Contains(text, "eventual transcript")
	})
}

func TestVoiceWorkerMixedBatchExcludesPendingVoiceFromPushAndAck(t *testing.T) {
	g := newGateChannel(100, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()
	setFastVoiceDebounce(b)
	route := MakeRouteKey("telegram", -100, nil)
	_, pushes := liveHolderFrames(t, b, route)

	sttStarted := make(chan struct{})
	releaseSTT := make(chan struct{})
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		close(sttStarted)
		<-releaseSTT
		return "voice-final", nil
	})
	plainFirst := c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 8101, Text: "plain-one"}
	_, voice, _ := schedulerVoice(8102, "voice-mixed")
	plainSecond := c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 8103, Text: "plain-two"}
	if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &plainFirst}) ||
		!b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &voice}) ||
		!b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &plainSecond}) {
		t.Fatal("mixed inbound submit rejected")
	}

	first := waitInboundPush(t, pushes)
	if first.Inbound.Text != "plain-one\nplain-two" || first.Covered != 2 || first.Pending != 1 {
		t.Fatalf("mixed immediate push=%+v; want two non-voice rows with covered=2 pending=1", first)
	}
	if len(first.Inbound.Merged) != 2 || first.Inbound.Merged[0].MessageID != plainFirst.MessageID ||
		first.Inbound.Merged[1].MessageID != plainSecond.MessageID {
		t.Fatalf("pending voice must be excluded from Merged: %+v", first.Inbound.Merged)
	}
	for _, source := range first.Inbound.Merged {
		if source.MessageID == voice.MessageID {
			t.Fatalf("pending voice %d leaked into Merged: %+v", voice.MessageID, first.Inbound.Merged)
		}
	}
	if strings.Contains(first.Inbound.Text, "transcription") || strings.Contains(first.Inbound.Text, "voice-final") {
		t.Fatalf("pending voice leaked into mixed immediate push: %+v", first)
	}
	select {
	case <-sttStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("voice enrichment did not start")
	}

	if !b.Workers.Submit(route, Job{Kind: JobConsume, Consume: &ConsumeJob{
		MessageID: first.Inbound.MessageID, Token: first.DeliveryToken, Count: first.Covered,
	}}) {
		t.Fatal("mixed push ack submit rejected")
	}
	if got := pendingWithin(t, b, route, 1); got != 1 {
		t.Fatalf("non-voice ack consumed the pending voice row; pending=%d want 1", got)
	}
	rows, err := b.Queue.PeekTracked(queueRouteKey(route), -1)
	if err != nil || len(rows) != 1 || len(rows[0].VoicePending) != 1 {
		t.Fatalf("mixed ack left rows=%+v err=%v; want only pending voice", rows, err)
	}
	recordID := rows[0].RecordID

	close(releaseSTT)
	second := waitInboundPush(t, pushes)
	if second.Inbound.MessageID != voice.MessageID || !strings.Contains(second.Inbound.Text, "voice-final") || second.Covered != 1 {
		t.Fatalf("resolved voice push=%+v", second)
	}
	if !b.Workers.Submit(route, Job{Kind: JobConsume, Consume: &ConsumeJob{
		MessageID: second.Inbound.MessageID, Token: second.DeliveryToken, Count: second.Covered,
	}}) {
		t.Fatal("voice resolve ack submit rejected")
	}
	if got := pendingWithin(t, b, route, 0); got != 0 {
		t.Fatalf("resolved voice ack did not consume record %s exactly; pending=%d", recordID, got)
	}
}

func TestVoiceWorkerFetchMidSTTAppendsOneBoundedRevision(t *testing.T) {
	g := newGateChannel(100, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()
	setFastVoiceDebounce(b)
	route := MakeRouteKey("telegram", -100, nil)
	_, pushes := liveHolderFrames(t, b, route)

	sttStarted := make(chan struct{})
	releaseSTT := make(chan struct{})
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		close(sttStarted)
		<-releaseSTT
		return "late transcript", nil
	})
	_, voice, _ := schedulerVoice(8201, "voice-consumed")
	if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &voice}) {
		t.Fatal("voice inbound submit rejected")
	}
	select {
	case <-sttStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("voice enrichment did not start")
	}

	fetchCh := make(chan FetchResult, 1)
	if !b.Workers.Submit(route, Job{Kind: JobFetch, Fetch: &FetchJob{All: true, Ack: true, ResultCh: fetchCh}}) {
		t.Fatal("fetch submit rejected")
	}
	fetched := waitFetchResult(t, fetchCh)
	if fetched.Err != nil || len(fetched.Messages) != 1 || !strings.Contains(fetched.Messages[0].Text, "transcription in progress") {
		t.Fatalf("fetch-mid-STT=%+v; want consumed placeholder", fetched)
	}
	if got := pendingWithin(t, b, route, 0); got != 0 {
		t.Fatalf("placeholder was not consumed: pending=%d", got)
	}

	// Fill exactly to the route cap while STT is parked. The revision's ordinary
	// AppendTracked + evictIfOverCap path must keep the queue at the same bound.
	for i := int64(0); i < queue.MaxMessages; i++ {
		filler := &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 9000 + i, Text: "filler"}
		if _, err := b.Queue.AppendTracked(queueRouteKey(route), filler); err != nil {
			t.Fatalf("append filler %d: %v", i, err)
		}
	}
	close(releaseSTT)
	revisionPush := waitInboundPush(t, pushes)
	wantPrefix := "[transcript update for voice message 8201]\n"
	if !strings.HasPrefix(revisionPush.Inbound.Text, wantPrefix) || !strings.Contains(revisionPush.Inbound.Text, "late transcript") || revisionPush.Covered != 1 {
		t.Fatalf("revision push=%+v", revisionPush)
	}
	waitForVoiceCondition(t, "bounded revision queue", func() bool {
		n, _ := b.Queue.Pending(queueRouteKey(route))
		return n == queue.MaxMessages
	})
	rows, err := b.Queue.Peek(queueRouteKey(route), -1)
	if err != nil {
		t.Fatal(err)
	}
	revisions := 0
	for _, row := range rows {
		if row.MessageID == voice.MessageID {
			revisions++
			if !strings.HasPrefix(row.Text, wantPrefix) {
				t.Fatalf("voice row was rewritten instead of appended as revision: %+v", row)
			}
		}
	}
	if revisions != 1 {
		t.Fatalf("revision rows=%d; want exactly one (rows=%d)", revisions, len(rows))
	}
	select {
	case extra := <-pushes:
		t.Fatalf("fetch-mid-STT produced a duplicate push: %+v", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestVoiceWorkerEditWhilePendingResolvesBothRows(t *testing.T) {
	g := newGateChannel(100, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()
	setFastVoiceDebounce(b)
	route := MakeRouteKey("telegram", -100, nil)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var attempts atomic.Int64
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		attempts.Add(1)
		once.Do(func() { close(started) })
		<-release
		return "shared edit transcript", nil
	})
	_, original, _ := schedulerVoice(8701, "voice-edit")
	edited := original
	edited.Edited = true
	edited.Text = "corrected caption"
	if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &original}) {
		t.Fatal("original submit rejected")
	}
	<-started
	if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &edited}) {
		t.Fatal("edit submit rejected")
	}
	waitForVoiceCondition(t, "both edit rows to join the pending lease", func() bool {
		return len(b.Queue.PendingVoiceRows(queueRouteKey(route))) == 2
	})
	close(release)
	waitForVoiceCondition(t, "both edit rows to resolve", func() bool {
		rows, err := b.Queue.PeekTracked(queueRouteKey(route), -1)
		if err != nil || len(rows) != 2 {
			return false
		}
		for _, row := range rows {
			if len(row.VoicePending) != 0 || !strings.Contains(row.Inbound.Text, "shared edit transcript") {
				return false
			}
		}
		return true
	})
	if got := attempts.Load(); got != 1 {
		t.Fatalf("edit collision made %d provider attempts; want one shared lease", got)
	}
}

func TestVoiceResolvePanicAfterDurableMutationDoesNotDuplicateRevisionOrPush(t *testing.T) {
	for _, tc := range []struct {
		stage      string
		wantPushes int
	}{
		{stage: "after_durable", wantPushes: 0},
		{stage: "after_push", wantPushes: 1},
	} {
		t.Run(tc.stage, func(t *testing.T) {
			t.Setenv("C3_QUEUE_DIR", t.TempDir())
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			defer b.Shutdown()
			route := MakeRouteKey("telegram", -100, ptrI64(914))
			_, pushes := liveHolderFrames(t, b, route)
			w := newRouteWorker(context.Background(), route, time.Hour, b)
			defer w.Stop()
			echoHead := make(chan struct{})
			close(echoHead)
			group := &voiceGroup{
				order: []string{"voice-panic-push"}, finished: map[voiceScheduleKey]bool{},
				transcripts: map[string]string{"voice-panic-push": "one transcript"},
				echo:        voiceEchoReservation{prev: echoHead, mine: make(chan struct{})},
			}
			key := voiceScheduleKey{route: route, messageID: 8801, fileID: "voice-panic-push"}
			target := voiceResolveTarget{recordID: "", group: group}
			b.Voice.mu.Lock()
			b.Voice.entries[key] = &voiceEntry{
				key: key, inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: ptrI64(914), MessageID: 8801},
				attachment: c3types.Attachment{Kind: "voice", FileID: key.fileID}, state: voiceResolveSubmitted,
				targets: map[string]voiceResolveTarget{"": target}, applied: map[string]bool{},
				resolve: voiceResolve{success: true, transcript: "one transcript"},
			}
			b.Voice.wg.Add(1)
			b.Voice.mu.Unlock()
			var panicked atomic.Bool
			w.voiceResolveTestHook = func(stage string) {
				if stage == tc.stage && panicked.CompareAndSwap(false, true) {
					panic("injected after durable voice mutation")
				}
			}
			job := &ResolveVoiceJob{
				Key: key, Targets: []voiceResolveTarget{target},
				Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: ptrI64(914), MessageID: 8801},
				FileID:  key.fileID, SegmentText: "[Transcribed voice]: one transcript", Success: true,
			}
			w.handleResolveVoice(context.Background(), job)
			if tc.wantPushes == 1 {
				push := waitInboundPush(t, pushes)
				if !strings.Contains(push.Inbound.Text, "one transcript") {
					t.Fatalf("first push lost transcript: %+v", push)
				}
			}
			rows, err := b.Queue.PeekTracked(queueRouteKey(route), -1)
			if err != nil || len(rows) != 1 || !strings.Contains(rows[0].Inbound.Text, "one transcript") {
				t.Fatalf("panic resolve rows=%+v err=%v", rows, err)
			}
			select {
			case extra := <-pushes:
				t.Fatalf("durably-applied panic produced an extra push: %+v", extra)
			case <-time.After(100 * time.Millisecond):
			}
			select {
			case <-group.echo.mine:
			case <-time.After(2 * time.Second):
				t.Fatal("durably-applied panic leaked its echo reservation")
			}
			b.Voice.mu.Lock()
			_, stillScheduled := b.Voice.entries[key]
			b.Voice.mu.Unlock()
			if stillScheduled {
				t.Fatal("durably-applied panic resolve was re-parked")
			}
		})
	}
}

func TestVoiceResolveRereadMissClosesEchoWithoutPush(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	route := MakeRouteKey("telegram", -100, ptrI64(914))
	_, pushes := liveHolderFrames(t, b, route)
	w := newRouteWorker(context.Background(), route, time.Hour, b)
	defer w.Stop()
	in := c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: ptrI64(914), MessageID: 8802,
		Text:        voicePendingText("voice-reread-miss"),
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "voice-reread-miss"}},
	}
	recordID, err := b.Queue.AppendTracked(queueRouteKey(route), &in, "voice-reread-miss")
	if err != nil {
		t.Fatal(err)
	}
	echoHead := make(chan struct{})
	close(echoHead)
	group := &voiceGroup{
		order: []string{"voice-reread-miss"}, finished: map[voiceScheduleKey]bool{},
		transcripts: map[string]string{"voice-reread-miss": "one transcript"},
		echo:        voiceEchoReservation{prev: echoHead, mine: make(chan struct{})},
	}
	key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: "voice-reread-miss"}
	target := voiceResolveTarget{recordID: recordID, group: group}
	b.Voice.mu.Lock()
	b.Voice.entries[key] = &voiceEntry{
		key: key, inbound: in, attachment: in.Attachments[0], state: voiceResolveSubmitted,
		targets: map[string]voiceResolveTarget{recordID: target}, applied: map[string]bool{},
		resolve: voiceResolve{success: true, transcript: "one transcript"},
	}
	b.Voice.wg.Add(1)
	b.Voice.mu.Unlock()
	var consumeErr error
	w.voiceResolveTestHook = func(stage string) {
		if stage == "after_durable" {
			_, consumeErr = b.Queue.Consume(queueRouteKey(route), -1)
		}
	}
	w.handleResolveVoice(context.Background(), &ResolveVoiceJob{
		Key: key, Targets: []voiceResolveTarget{target}, Inbound: in,
		FileID: key.fileID, SegmentText: "[Transcribed voice]: one transcript", Success: true,
	})
	if consumeErr != nil {
		t.Fatalf("consume between resolve and re-read: %v", consumeErr)
	}
	select {
	case <-group.echo.mine:
	case <-time.After(2 * time.Second):
		t.Fatal("resolved-row re-read miss leaked its echo reservation")
	}
	select {
	case push := <-pushes:
		t.Fatalf("resolved-row re-read miss pushed an unverified payload: %+v", push)
	case <-time.After(100 * time.Millisecond):
	}
	b.Voice.mu.Lock()
	_, stillScheduled := b.Voice.entries[key]
	b.Voice.mu.Unlock()
	if stillScheduled {
		t.Fatal("durably-applied re-read miss was rescheduled")
	}
}
