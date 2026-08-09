package broker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/mappings"
)

type sttOrderingEvent struct {
	kind    string
	text    string
	replyTo int64
}

// sttOrderingChannel records SendReply and SendReadback on one timeline. The
// separate recorders used by older worker tests cannot prove that the late-order
// warning was posted before the transcript readback.
type sttOrderingChannel struct {
	*fakeChannel
	mu     sync.Mutex
	events []sttOrderingEvent
}

func (c *sttOrderingChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	id, err := c.fakeChannel.SendReply(args)
	if err != nil {
		return id, err
	}
	c.record(sttOrderingEvent{kind: "notice", text: args.Text, replyTo: derefMessageID(args.ReplyTo)})
	return id, nil
}

func (c *sttOrderingChannel) SendReadback(args c3types.ReadbackArgs) (int64, error) {
	c.record(sttOrderingEvent{kind: "readback", text: args.Transcript, replyTo: derefMessageID(args.ReplyTo)})
	return 99, nil
}

func (c *sttOrderingChannel) record(event sttOrderingEvent) {
	c.mu.Lock()
	c.events = append(c.events, event)
	c.mu.Unlock()
}

func (c *sttOrderingChannel) snapshot() []sttOrderingEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]sttOrderingEvent, len(c.events))
	copy(out, c.events)
	return out
}

func derefMessageID(id *int64) int64 {
	if id == nil {
		return 0
	}
	return *id
}

func newSTTOrderingFixture(t *testing.T) (*Broker, *RouteWorker, *sttOrderingChannel) {
	t.Helper()
	t.Setenv("C3_QUEUE_DIR", t.TempDir())

	ch := &sttOrderingChannel{fakeChannel: &fakeChannel{}}
	b := brokerWithGenericChannel(t, &mappings.MappingsFile{SchemaVersion: 1}, ch)
	t.Cleanup(b.Shutdown)
	b.Plugins.OnVoiceReceived(func(_ context.Context, _ c3types.VoicePayload) (string, error) {
		return "late spoken part", nil
	})

	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 3048}
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	t.Cleanup(w.Stop)
	return b, w, ch
}

func flushOrderingMarkerAndVoice(w *RouteWorker, markerID, voiceID int64) *c3types.Inbound {
	topicID := int64(3048)
	w.flushInbounds(context.Background(), []*c3types.Inbound{{
		Channel: "telegram", ChatID: -100, TopicID: &topicID,
		MessageID: markerID, Text: "end of multi-part message",
	}})
	voice := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &topicID, MessageID: voiceID,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "late-voice"}},
	}
	w.flushInbounds(context.Background(), []*c3types.Inbound{voice})
	return voice
}

func waitSTTOrderingEvents(t *testing.T, ch *sttOrderingChannel, replyTo int64) []sttOrderingEvent {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var relevant []sttOrderingEvent
		for _, event := range ch.snapshot() {
			if event.replyTo == replyTo {
				relevant = append(relevant, event)
			}
		}
		for _, event := range relevant {
			if event.kind == "readback" {
				return relevant
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("slow-STT ordering defect: no readback for message_id=%d within 2s; late delivery may have vanished", replyTo)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestSTTOrdering_LateVoiceAgentSurfaceNamesOvertakenMessage(t *testing.T) {
	b, w, ch := newSTTOrderingFixture(t)
	setFastVoiceDebounce(b)
	_, pushes := liveHolderFrames(t, b, w.key)
	topicID := int64(3048)
	marker := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &topicID, MessageID: 7010, Text: "end of multi-part message"}
	if !b.Workers.Submit(w.key, Job{Kind: JobInbound, Inbound: marker}) {
		t.Fatal("marker submit rejected")
	}
	_ = waitInboundPush(t, pushes)
	late := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &topicID, MessageID: 7007,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "late-voice"}},
	}
	if !b.Workers.Submit(w.key, Job{Kind: JobInbound, Inbound: late}) {
		t.Fatal("voice submit rejected")
	}
	push := waitInboundPush(t, pushes)
	waitSTTOrderingEvents(t, ch, 7007)
	durableText := waitForVoiceQueueText(t, b, w.key, late.MessageID, func(text string) bool {
		return strings.Contains(text, "late spoken part")
	})

	notice := lateVoiceOrderNotice(7007, 7010)
	noticeAt := strings.Index(push.Inbound.Text, notice)
	transcriptAt := strings.Index(push.Inbound.Text, "[Transcribed voice]: late spoken part")
	if noticeAt < 0 {
		t.Fatalf("slow-STT ordering defect: pushed agent surface %q does not say message_id=7007 was spoken before already-delivered message_id=7010", push.Inbound.Text)
	}
	if transcriptAt < 0 {
		t.Fatalf("slow-STT loss defect: late message_id=7007 warning exists but its transcript was dropped: %q", push.Inbound.Text)
	}
	if noticeAt > transcriptAt {
		t.Fatalf("slow-STT ordering defect: agent sees the late transcript before its ordering warning: %q", push.Inbound.Text)
	}
	if strings.Contains(durableText, notice) {
		t.Fatalf("late-order presentation label leaked into durable history: %q", durableText)
	}

	pending, _ := b.Queue.Pending(queueRouteKey(w.key))
	if pending != 2 {
		t.Fatalf("slow-STT loss defect: late/redelivery must remain recoverable; durable lines=%d, want marker+late voice=2", pending)
	}
}

func TestSTTOrdering_NewerMessageDuringSTTLabelsOnlyResolvedPush(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	ch := &sttOrderingChannel{fakeChannel: &fakeChannel{}}
	b := brokerWithGenericChannel(t, &mappings.MappingsFile{SchemaVersion: 1}, ch)
	defer b.Shutdown()
	setFastVoiceDebounce(b)
	started := make(chan struct{})
	release := make(chan struct{})
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		close(started)
		<-release
		return "slow spoken part", nil
	})
	route := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 3048}
	_, pushes := liveHolderFrames(t, b, route)
	topicID := int64(3048)
	voice := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &topicID, MessageID: 7007,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "slow-voice"}},
	}
	if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: voice}) {
		t.Fatal("voice submit rejected")
	}
	<-started
	newer := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &topicID, MessageID: 7010, Text: "newer ordinary message"}
	if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: newer}) {
		t.Fatal("newer submit rejected")
	}
	if push := waitInboundPush(t, pushes); push.Inbound.MessageID != newer.MessageID {
		t.Fatalf("newer ordinary push = %+v", push)
	}
	close(release)
	voicePush := waitInboundPush(t, pushes)
	notice := lateVoiceOrderNotice(7007, 7010)
	if !strings.HasPrefix(voicePush.Inbound.Text, notice+"\n") {
		t.Fatalf("resolved push did not prepend late-order notice: %q", voicePush.Inbound.Text)
	}
	durable := waitForVoiceQueueText(t, b, route, voice.MessageID, func(text string) bool {
		return strings.Contains(text, "slow spoken part")
	})
	if strings.Contains(durable, notice) {
		t.Fatalf("late-order presentation notice polluted durable row: %q", durable)
	}
}

func TestSTTOrdering_LateVoiceReadbackStaysVerbatim(t *testing.T) {
	_, w, ch := newSTTOrderingFixture(t)
	flushOrderingMarkerAndVoice(w, 7010, 7007)
	events := waitSTTOrderingEvents(t, ch, 7007)

	notice := lateVoiceOrderNotice(7007, 7010)
	readbackAt := -1
	for i, event := range events {
		if strings.Contains(event.text, notice) {
			t.Fatalf("presentation-only ordering label leaked onto the human surface: events=%+v", events)
		}
		if event.kind == "readback" {
			readbackAt = i
			if event.text != "late spoken part" {
				t.Fatalf("slow-STT readback defect: ordering hygiene contaminated the verbatim transcript; got %q", event.text)
			}
		}
	}
	if readbackAt < 0 {
		t.Fatalf("slow-STT readback disappeared: events=%+v", events)
	}
}

func TestSTTOrdering_InOrderVoiceDoesNotClaimItWasLate(t *testing.T) {
	_, w, ch := newSTTOrderingFixture(t)
	voice := flushOrderingMarkerAndVoice(w, 7006, 7007)
	events := waitSTTOrderingEvents(t, ch, 7007)

	if strings.Contains(voice.Text, "[late voice message:") {
		t.Fatalf("slow-STT ordering defect: in-order message_id=7007 was falsely labeled late after message_id=7006: %q", voice.Text)
	}
	for _, event := range events {
		if strings.Contains(event.text, "[late voice message:") {
			t.Fatalf("slow-STT readback defect: in-order message_id=7007 received a false late warning; events=%+v", events)
		}
	}
}

func TestSTTOrdering_EditReuseDoesNotClaimItWasLate(t *testing.T) {
	_, w, ch := newSTTOrderingFixture(t)
	topicID := int64(3048)
	w.flushInbounds(context.Background(), []*c3types.Inbound{{
		Channel: "telegram", ChatID: -100, TopicID: &topicID,
		MessageID: 7010, Text: "newer message",
	}})
	editedVoice := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &topicID,
		MessageID: 7007, Edited: true,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "edited-voice"}},
	}
	w.flushInbounds(context.Background(), []*c3types.Inbound{editedVoice})
	events := waitSTTOrderingEvents(t, ch, 7007)

	if strings.Contains(editedVoice.Text, "[late voice message:") {
		t.Fatalf("slow-STT ordering defect: edited_message reused message_id=7007 by design but was mislabeled as late after message_id=7010: %q", editedVoice.Text)
	}
	for _, event := range events {
		if strings.Contains(event.text, "[late voice message:") {
			t.Fatalf("slow-STT readback defect: edited_message reused message_id=7007 but the human received a false late-delivery warning; events=%+v", events)
		}
	}
}
