package broker

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestInjectedMarkerAttachAndQueuePreviews(t *testing.T) {
	b := injectionFixture(t, true)
	topic := int64(42)
	key := MakeRouteKey(TestInjectChannel, TestInjectChatID, &topic)
	for id, text := range []string{strings.Repeat("sample", 30), ""} {
		in := &c3types.Inbound{Channel: key.Channel, ChatID: key.ChatID, TopicID: &topic, MessageID: int64(id + 1), Text: text, TestInjected: true}
		if _, err := b.Queue.AppendTracked(queueRouteKey(key), in); err != nil {
			t.Fatal(err)
		}
		for _, preview := range []string{previewText(in, 1), previewLine(in, 1), drainPreview(in)} {
			if !strings.HasPrefix(preview, c3types.TestInjectedMarker) {
				t.Fatalf("truncated/lost marker: %q", preview)
			}
		}
	}
	count, items := b.backlogSummary(key)
	if count != 2 || len(items) != 2 {
		t.Fatalf("backlog %d: %+v", count, items)
	}
	for _, item := range items {
		if !strings.HasPrefix(item.Preview, c3types.TestInjectedMarker) {
			t.Fatalf("attach IPC preview lost marker: %+v", item)
		}
	}
}

func TestInjectedMarkerVoiceReadbackAndNotice(t *testing.T) {
	b, w, _, _ := shadowFixture(t)
	channel := &readbackRecorderChannel{fakeChannel: &fakeChannel{}}
	registerReadbackChannel(b, channel)
	in := &c3types.Inbound{Channel: w.key.Channel, ChatID: w.key.ChatID, MessageID: 1, TestInjected: true}
	w.echoReadback(in, "sample transcript", "sample failure notice")
	readbacks := channel.readbackSnapshot()
	if len(readbacks) != 1 || !strings.HasPrefix(readbacks[0].Transcript, c3types.TestInjectedMarker) {
		t.Fatal(readbacks)
	}
	replies := channel.sendRepliesSnapshot()
	if len(replies) != 1 || !strings.HasPrefix(replies[0].Text, c3types.TestInjectedMarker) {
		t.Fatal(replies)
	}
}
