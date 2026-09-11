package broker

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

func TestSTTOrdering_VoiceMessagesStayInTelegramOrder(t *testing.T) {
	g := newGateChannel(100, nil)
	started := map[string]chan struct{}{}
	release := map[string]chan struct{}{}
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("voice-%d", i)
		started[id], release[id] = make(chan struct{}), make(chan struct{})
	}
	g.answer = func(fileID string) (int64, error) {
		close(started[fileID])
		<-release[fileID]
		return 100, nil
	}
	b := gateBroker(t, g)
	defer b.Shutdown()
	setFastVoiceDebounce(b)
	b.Plugins.OnVoiceReceived(func(_ context.Context, p c3types.VoicePayload) (string, error) {
		return "spoken " + p.FileID, nil
	})
	route := MakeRouteKey("telegram", -100, nil)
	_, pushes := liveHolderFrames(t, b, route)
	for i := 1; i <= 3; i++ {
		fileID := fmt.Sprintf("voice-%d", i)
		in := &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: int64(7000 + i),
			Attachments: []c3types.Attachment{{Kind: "voice", FileID: fileID}}}
		if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: in}) {
			t.Fatalf("submit %s rejected", fileID)
		}
	}
	waitOrderingSignal(t, started["voice-1"], "first voice did not start")
	waitOrderingSignal(t, started["voice-2"], "second concurrent voice did not start")

	// Complete the later transcriptions first. They must remain buffered at the
	// durable resolve boundary instead of waking the agent out of order.
	close(release["voice-2"])
	waitOrderingSignal(t, started["voice-3"], "third concurrent voice did not start")
	close(release["voice-3"])
	assertNoOrderingPush(t, pushes, "later voice was delivered before voice-1 completed")

	close(release["voice-1"])
	for i := 1; i <= 3; i++ {
		fileID := fmt.Sprintf("voice-%d", i)
		push := waitInboundPush(t, pushes)
		if push.Inbound.MessageID != int64(7000+i) || !strings.Contains(push.Inbound.Text, "spoken "+fileID) {
			t.Fatalf("delivery %d out of order: %+v", i, push.Inbound)
		}
	}
	readbacks := g.waitReadbacks(t, 3)
	for i, readback := range readbacks {
		want := fmt.Sprintf("spoken voice-%d", i+1)
		if readback.Transcript != want {
			t.Fatalf("readback %d = %q, want %q", i+1, readback.Transcript, want)
		}
	}
}

func waitOrderingSignal(t *testing.T, ch <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal(failure)
	}
}

func assertNoOrderingPush(t *testing.T, ch <-chan ipc.InboundMsg, failure string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(failure)
	case <-time.After(100 * time.Millisecond):
	}
}
