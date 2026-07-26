package main

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// TestIdleStartupWatchdog_CancelsWhenNoDispatch asserts the regression that
// motivated the watchdog: an adapter that Claude Code spawns but never
// drives (observed during `--resume` lifecycles where CC orphans the prior
// MCP server) must exit on its own rather than zombie forever. Concretely:
// dispatched stays false → ctx is canceled by the watchdog.
//
// Post-SDK-migration the disarm signal is set by the receiving middleware
// installed in buildMCPServer, but the watchdog logic itself is independent
// of the MCP wire layer. This test exercises the same logic with an inline
// fast-budget mini-watchdog.
func TestIdleStartupWatchdog_CancelsWhenNoDispatch(t *testing.T) {
	a := newAdapter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Inline mini-watchdog with a 50ms budget so the test stays fast.
	// Mirrors idleStartupWatchdog's logic exactly; production code uses
	// idleStartupTimeout (60s).
	go func() {
		timer := time.NewTimer(50 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if !a.dispatched.Load() {
				cancel()
			}
		}
	}()

	select {
	case <-ctx.Done():
		// Expected — watchdog fired.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("watchdog did not cancel ctx; adapter would zombie")
	}
}

// TestIdleStartupWatchdog_StaysQuietAfterDispatch asserts the disarm
// path: once the adapter has received any MCP frame, the watchdog must
// not cancel ctx no matter how long it sits idle afterward. Without this,
// a long-running session that goes quiet (no inbound, no tool calls)
// would self-destruct.
func TestIdleStartupWatchdog_StaysQuietAfterDispatch(t *testing.T) {
	a := newAdapter()
	a.dispatched.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		timer := time.NewTimer(50 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if !a.dispatched.Load() {
				cancel()
			}
		}
	}()

	select {
	case <-ctx.Done():
		t.Fatal("watchdog canceled ctx despite dispatched=true")
	case <-time.After(200 * time.Millisecond):
		// Expected — watchdog kept quiet.
	}
}

// TestDispatch_SetsDispatchedFlag confirms the disarm signal fires on
// the first MCP request. This is the integration point: without the
// flag flip, the watchdog has no way to know Claude Code is actually
// talking to us.
//
// Post-SDK-migration the flip happens via a receiving middleware
// registered in buildMCPServer. We exercise the full pipeline by
// connecting an in-memory client/server pair and issuing a ping; the
// middleware must observe the call before it reaches any handler.
func TestDispatch_SetsDispatchedFlag(t *testing.T) {
	a := newAdapter()
	if a.dispatched.Load() {
		t.Fatal("dispatched should start false")
	}

	srv := a.buildMCPServer()
	clientT, serverT := newInMemoryTransports()
	a.notifyTx = newNotifyTransport(serverT)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	srvDone := make(chan error, 1)
	go func() { srvDone <- srv.Run(ctx, a.notifyTx) }()

	client := newTestClient()
	sess, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	defer sess.Close()

	// Ping is the cheapest method that flows through the dispatch
	// pipeline — middleware must mark the adapter as dispatched before
	// the SDK reaches its built-in ping handler.
	if err := sess.Ping(ctx, nil); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if !a.dispatched.Load() {
		t.Fatal("Dispatch middleware did not set dispatched flag after ping")
	}
}

// TestRememberAttach_SanitizesSteal (item D): a user-confirmed steal is a
// ONE-SHOT confirmation, not a standing property. rememberAttach must clear Steal
// in the stored copy so a post-broker-bounce replay does not silently force-evict
// whoever holds the route (last-reconnector-wins). Other fields survive.
func TestRememberAttach_SanitizesSteal(t *testing.T) {
	a := newAdapter()
	tid := int64(914)
	a.rememberAttach(ipc.AttachReq{Op: ipc.OpAttach, Name: "c3", TopicID: &tid, Group: "main", Steal: true})

	a.amu.Lock()
	got := a.lastAttach
	a.amu.Unlock()
	if got == nil {
		t.Fatal("rememberAttach stored nothing")
	}
	if got.Steal {
		t.Fatalf("remembered attach must clear Steal (one-shot, not standing): %+v", got)
	}
	if got.Name != "c3" || got.Group != "main" || got.TopicID == nil || *got.TopicID != 914 {
		t.Fatalf("rememberAttach dropped non-Steal fields: %+v", got)
	}
}

// TestReplayLastAttach_ResendsLastAttachWithReplayFlag verifies the
// `--resume` re-attach path: after a broker reconnect the adapter
// must re-send its last successful AttachReq with Replay=true so the
// broker can re-grant the claim without firing a fresh welcome
// message. TODO #19(d) — maintainer 2026-05-18.
//
// We don't need a real broker; net.Pipe gives us an in-memory peer
// from which we can read the frame the adapter writes.
func TestReplayLastAttach_ResendsLastAttachWithReplayFlag(t *testing.T) {
	a := newAdapter()

	pipeA, pipeB := net.Pipe()
	defer pipeA.Close()
	defer pipeB.Close()

	a.bmu.Lock()
	a.conn = ipc.NewConn(pipeA)
	a.bmu.Unlock()

	tid := int64(914)
	a.rememberAttach(ipc.AttachReq{
		Op:      ipc.OpAttach,
		CWD:     "/projects/c3",
		Name:    "c3",
		TopicID: &tid,
		Group:   "main",
	})

	peer := ipc.NewConn(pipeB)
	done := make(chan struct{})
	var raw []byte
	var readErr error
	go func() {
		defer close(done)
		raw, readErr = peer.ReadFrame()
	}()

	a.replayLastAttach()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("replayLastAttach did not write a frame within 2s")
	}
	if readErr != nil {
		t.Fatalf("read frame: %v", readErr)
	}

	op, err := ipc.PeekOp(raw)
	if err != nil {
		t.Fatalf("peek op: %v", err)
	}
	if op != ipc.OpAttach {
		t.Fatalf("op=%q, want %q", op, ipc.OpAttach)
	}
	var got ipc.AttachReq
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal AttachReq: %v", err)
	}
	if !got.Replay {
		t.Errorf("replayLastAttach must set Replay=true; got %+v", got)
	}
	if got.Name != "c3" || got.CWD != "/projects/c3" || got.Group != "main" {
		t.Errorf("replayed AttachReq lost user fields: %+v", got)
	}
	if got.TopicID == nil || *got.TopicID != 914 {
		t.Errorf("replayed AttachReq lost TopicID: %+v", got)
	}
}

// TestReplayLastAttach_NoopWithoutPriorAttach is the guard for the
// pre-attach-recovery case: an adapter that reconnects before the
// user ever attached must not blast garbage at the broker. The early-
// return on `lastAttach == nil` should keep the wire silent.
func TestReplayLastAttach_NoopWithoutPriorAttach(t *testing.T) {
	a := newAdapter()
	pipeA, pipeB := net.Pipe()
	defer pipeA.Close()
	defer pipeB.Close()
	a.bmu.Lock()
	a.conn = ipc.NewConn(pipeA)
	a.bmu.Unlock()

	peer := ipc.NewConn(pipeB)
	gotFrame := make(chan struct{}, 1)
	go func() {
		if _, err := peer.ReadFrame(); err == nil {
			gotFrame <- struct{}{}
		}
	}()

	a.replayLastAttach()

	select {
	case <-gotFrame:
		t.Fatal("replayLastAttach wrote a frame despite lastAttach == nil")
	case <-time.After(200 * time.Millisecond):
		// Expected — wire stayed silent.
	}
}
