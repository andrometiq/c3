package broker

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

func TestVoiceEndToEndTransientParksUntilHealthUpThenPushesOnce(t *testing.T) {
	g := newGateChannel(100, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()
	setFastVoiceDebounce(b)
	t.Setenv("C3_HEALTH_FILE", t.TempDir()+"/health.json")
	b.Voice.retryBase = 500 * time.Millisecond
	b.Voice.retryCap = time.Second
	b.Voice.jitter = func(d time.Duration) time.Duration { return d }

	var attempts atomic.Int64
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		if attempts.Add(1) == 1 {
			return "[STT FETCH FAILED: network is unreachable]", nil
		}
		return "recovered words", nil
	})
	route, in, _ := schedulerVoice(8301, "voice-health")
	_, pushes := liveHolderFrames(t, b, route)
	if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &in}) {
		t.Fatal("voice inbound submit rejected")
	}
	waitForVoiceCondition(t, "first transient attempt to park", func() bool {
		key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: "voice-health"}
		state, _, ok := schedulerEntrySnapshot(b.Voice, key)
		return attempts.Load() == 1 && ok && state == voiceWaiting
	})

	host := NewBrokerHost(b, "telegram")
	host.NotifyHealth(c3types.HealthEvent{
		Channel: "telegram", State: c3types.HealthStateDown, Since: time.Now(),
		Consec: 1, Reason: "network is unreachable",
	})
	time.Sleep(600 * time.Millisecond) // retry is due, but known-DOWN health gates it
	if got := attempts.Load(); got != 1 {
		t.Fatalf("known-DOWN health burned another attempt: %d", got)
	}
	select {
	case push := <-pushes:
		t.Fatalf("pending placeholder was pushed before terminal resolve: %+v", push)
	default:
	}

	host.NotifyHealth(c3types.HealthEvent{
		Channel: "telegram", State: c3types.HealthStateUp, Since: time.Now(),
		DownFor: 600 * time.Millisecond,
	})
	push := waitInboundPush(t, pushes)
	if push.Inbound.MessageID != in.MessageID || !strings.Contains(push.Inbound.Text, "recovered words") || push.Covered != 1 {
		t.Fatalf("recovered push=%+v", push)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("provider attempts=%d; want transient + one recovery", got)
	}
	select {
	case extra := <-pushes:
		t.Fatalf("voice resolved more than once: %+v", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestVoiceEndToEndRestartReparksPendingOutageRow(t *testing.T) {
	queueDir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", queueDir)
	mf := &mappings.MappingsFile{SchemaVersion: 1}
	b1 := New(mf)
	t.Cleanup(b1.Shutdown)
	setFastVoiceDebounce(b1)
	g1 := newGateChannel(100, nil)
	registerGateChannel(b1, g1)
	b1.Voice.retryBase = time.Hour
	b1.Voice.retryCap = time.Hour
	b1.Voice.jitter = func(d time.Duration) time.Duration { return d }
	var firstAttempts atomic.Int64
	b1.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		firstAttempts.Add(1)
		return "[STT FETCH FAILED: network is unreachable]", nil
	})
	route, in, _ := schedulerVoice(8401, "voice-restart-outage")
	if !b1.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &in}) {
		t.Fatal("first broker inbound submit rejected")
	}
	waitForVoiceCondition(t, "first broker to park transient voice", func() bool {
		rows := b1.Queue.PendingVoiceRows(queueRouteKey(route))
		key := voiceScheduleKey{route: route, messageID: in.MessageID, fileID: "voice-restart-outage"}
		state, _, ok := schedulerEntrySnapshot(b1.Voice, key)
		return len(rows) == 1 && firstAttempts.Load() == 1 && ok && state == voiceWaiting
	})
	b1.Shutdown()

	b2 := New(mf)
	defer b2.Shutdown()
	var recoveredAttempts atomic.Int64
	b2.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		recoveredAttempts.Add(1)
		return "after restart", nil
	})
	_, pushes := liveHolderFrames(t, b2, route)
	g2 := newGateChannel(100, nil)
	if err := b2.RegisterChannel(g2); err != nil {
		t.Fatalf("register recovery channel: %v", err)
	}
	push := waitInboundPush(t, pushes)
	if !strings.Contains(push.Inbound.Text, "after restart") {
		t.Fatalf("restart recovery push=%+v", push)
	}
	rows, err := b2.Queue.PeekTracked(queueRouteKey(route), -1)
	if err != nil || len(rows) != 1 || len(rows[0].VoicePending) != 0 {
		t.Fatalf("restart recovery rows=%+v err=%v; want one final row", rows, err)
	}
	if got := recoveredAttempts.Load(); got != 1 {
		t.Fatalf("restart recovery attempts=%d; want 1", got)
	}
}

func TestVoiceEndToEndShutdownMidSTTRestartsWithoutLossOrDuplicate(t *testing.T) {
	queueDir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", queueDir)
	mf := &mappings.MappingsFile{SchemaVersion: 1}
	b1 := New(mf)
	t.Cleanup(b1.Shutdown)
	setFastVoiceDebounce(b1)
	registerGateChannel(b1, newGateChannel(100, nil))
	started := make(chan struct{})
	b1.Plugins.OnVoiceReceived(func(ctx context.Context, _ c3types.VoicePayload) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})
	route, in, _ := schedulerVoice(8501, "voice-mid-stt")
	if !b1.Workers.Submit(route, Job{Kind: JobInbound, Inbound: &in}) {
		t.Fatal("first broker inbound submit rejected")
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first broker STT did not start")
	}
	if rows := b1.Queue.PendingVoiceRows(queueRouteKey(route)); len(rows) != 1 {
		t.Fatalf("mid-STT row was not durable: %+v", rows)
	}
	b1.Shutdown()

	b2 := New(mf)
	defer b2.Shutdown()
	b2.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		return "survived restart", nil
	})
	_, pushes := liveHolderFrames(t, b2, route)
	if err := b2.RegisterChannel(newGateChannel(100, nil)); err != nil {
		t.Fatalf("register restarted channel: %v", err)
	}
	push := waitInboundPush(t, pushes)
	if !strings.Contains(push.Inbound.Text, "survived restart") {
		t.Fatalf("post-restart push=%+v", push)
	}
	rows, err := b2.Queue.PeekTracked(queueRouteKey(route), -1)
	if err != nil || len(rows) != 1 || rows[0].Inbound.MessageID != in.MessageID || len(rows[0].VoicePending) != 0 {
		t.Fatalf("restart produced loss/duplicate: rows=%+v err=%v", rows, err)
	}
	select {
	case extra := <-pushes:
		t.Fatalf("restart produced duplicate push: %+v", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRetranscribeDeadCallerStillLandsDurablyAndPushes(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	route := MakeRouteKey("telegram", -100, ptrI64(914))
	holder, pushes := liveHolderFrames(t, b, route)
	holder.SetRoute(&route)
	started := make(chan struct{})
	release := make(chan struct{})
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		close(started)
		<-release
		return "caller-independent transcript", nil
	})

	agentSide, brokerSide := newConnPair(t)
	req, _ := json.Marshal(ipc.RetranscribeReq{
		Op: ipc.OpRetranscribe, ID: "dead-caller", FileID: "voice-dead-caller", MessageID: 8601,
	})
	done := make(chan struct{})
	go func() {
		b.handleRetranscribe(brokerSide, holder, req)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("manual transcription did not start")
	}
	_ = agentSide.Close() // the requesting IPC caller disappears mid-attempt
	close(release)

	push := waitInboundPush(t, pushes)
	if !strings.Contains(push.Inbound.Text, "caller-independent transcript") || push.Covered != 1 {
		t.Fatalf("dead-caller durable push=%+v", push)
	}
	rows, err := b.Queue.PeekTracked(queueRouteKey(route), -1)
	if err != nil || len(rows) != 1 || !strings.Contains(rows[0].Inbound.Text, "caller-independent transcript") {
		t.Fatalf("dead-caller durable rows=%+v err=%v", rows, err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("dead-caller retranscribe handler did not exit")
	}
}
