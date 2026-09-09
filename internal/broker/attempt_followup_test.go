package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestWorkerReadbackSurvivesDetachAndIdleExit(t *testing.T) {
	clearFetchTestEnvironment(t)
	// Follow-up A: "detach/idle exit with a pending chained echo still delivers the echo".
	for _, exit := range []string{"detach", "idle"} {
		t.Run(exit, func(t *testing.T) {
			t.Setenv("C3_QUEUE_DIR", t.TempDir())
			fc := &fakeChannel{}
			b := brokerWithChannel(t, mfWithTelegram(), fc)
			t.Cleanup(b.Shutdown)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			rc := &readbackRecorderChannel{fakeChannel: fc, beforeRecord: func(a c3types.ReadbackArgs) {
				if a.Transcript == "first" {
					close(entered)
					<-release
				}
			}}
			registerReadbackChannel(b, rc)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			w := &RouteWorker{key: MakeRouteKey("telegram", -100, nil), broker: b, queue: make(chan Job, 4), done: make(chan struct{}), idle: 30 * time.Millisecond, cancel: cancel}
			head, first, second := make(chan struct{}), make(chan struct{}), make(chan struct{})
			close(head)
			w.enqueueVoiceReadback(ctx, inboundOn(-100, nil, 1, ""), "first", "", voiceEchoReservation{prev: head, mine: first})
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("first echo never entered its send")
			}
			w.enqueueVoiceReadback(ctx, inboundOn(-100, nil, 2, ""), "second", "", voiceEchoReservation{prev: first, mine: second})
			if exit == "detach" {
				w.queue <- Job{Kind: JobRelease}
			}
			go w.run(ctx)
			t.Cleanup(w.Stop)
			select {
			case <-w.Done():
			case <-time.After(time.Second):
				t.Fatal("worker did not exit")
			}
			if err := ctx.Err(); err != nil {
				t.Fatalf("%s cancelled the legacy echo context: %v", exit, err)
			}
			select {
			case <-second:
				t.Fatal("pending echo ended before its predecessor")
			default:
			}
			unblock()
			select {
			case <-second:
			case <-time.After(time.Second):
				t.Fatal("pending echo was not delivered after worker exit")
			}
			got := rc.readbackSnapshot()
			if len(got) != 2 || got[0].Transcript != "first" || got[1].Transcript != "second" {
				t.Fatalf("echoes after %s: %+v", exit, got)
			}
		})
	}
}

func TestAttemptWriterCancelledOnWorkerExit(t *testing.T) {
	clearFetchTestEnvironment(t)
	// Follow-up A: "give the attempt writer its own context and cancel only that on run() exit".
	for _, exit := range []string{"detach", "idle"} {
		t.Run(exit, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			w := &RouteWorker{queue: make(chan Job, 4), done: make(chan struct{}), idle: 20 * time.Millisecond, cancel: cancel}
			w.startAttemptWriter(ctx)
			if w.attemptWriteCancel == nil {
				t.Fatal("writer has no independent cancellation")
			}
			writerCancel := w.attemptWriteCancel
			cancelled := make(chan struct{})
			w.attemptWriteCancel = func() { writerCancel(); close(cancelled) }
			if exit == "detach" {
				w.queue <- Job{Kind: JobRelease}
			}
			go w.run(ctx)
			t.Cleanup(w.Stop)
			select {
			case <-w.Done():
			case <-time.After(time.Second):
				t.Fatal("worker did not exit")
			}
			select {
			case <-cancelled:
			default:
				t.Fatal("worker exit left attempt writer alive")
			}
			if ctx.Err() != nil {
				t.Fatal("writer cancellation reached legacy worker context")
			}
		})
	}
}
