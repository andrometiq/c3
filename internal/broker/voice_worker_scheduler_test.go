package broker

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/queue"
)

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
	plain := c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 8101, Text: "plain-now"}
	_, voice, _ := schedulerVoice(8102, "voice-mixed")
	if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &plain}) ||
		!b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &voice}) {
		t.Fatal("mixed inbound submit rejected")
	}

	first := waitInboundPush(t, pushes)
	if first.Inbound.Text != "plain-now" || first.Covered != 1 || first.Pending != 1 {
		t.Fatalf("mixed immediate push=%+v; want only the non-voice row with covered=1 pending=1", first)
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
