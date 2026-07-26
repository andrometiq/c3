package broker

import (
	"encoding/json"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// blockingReplyChannel embeds the test fakeChannel but makes SendReply block
// until release is closed, so a JobOutbound (reply) parks the route worker
// mid-dispatch — simulating a worker that genuinely STALLS and never writes its
// result channel. started is closed once SendReply is entered so a test can wait
// for the worker to be wedged before driving a second op behind it.
type blockingReplyChannel struct {
	*fakeChannel
	release   chan struct{}
	started   chan struct{}
	startOnce sync.Once
}

func (c *blockingReplyChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	c.startOnce.Do(func() { close(c.started) })
	<-c.release // park here until the test cleanup releases the worker
	return c.fakeChannel.SendReply(args)
}

// brokerWithBlockingReply wires a broker whose telegram channel's SendReply
// blocks, used to manufacture a stalled worker. Cleanups are registered so the
// worker is unblocked (close release) BEFORE Shutdown's wg.Wait runs — LIFO order
// matters or Shutdown would deadlock on the parked worker.
func brokerWithBlockingReply(t *testing.T) (*Broker, *blockingReplyChannel) {
	t.Helper()
	bc := &blockingReplyChannel{
		fakeChannel: &fakeChannel{},
		release:     make(chan struct{}),
		started:     make(chan struct{}),
	}
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	b.chMu.Lock()
	b.channels["telegram"] = &channelRegistration{Channel: bc}
	b.chMu.Unlock()
	t.Cleanup(func() { b.Shutdown() })      // registered first  -> runs LAST
	t.Cleanup(func() { close(bc.release) }) // registered second -> runs FIRST
	return b, bc
}

// TestHandleToolCall_WorkerStall_ReturnsError mirrors the fetch_queue stall test
// for the tool_call path: when the route worker stalls (its SendReply blocks and
// it never writes resultCh), handleToolCall must return a ToolResultMsg with a
// worker-timeout Error within workerJobTimeout rather than wedging the read loop.
func TestHandleToolCall_WorkerStall_ReturnsError(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b, _ := brokerWithBlockingReply(t)

	prev := workerJobTimeout
	workerJobTimeout = 50 * time.Millisecond
	defer func() { workerJobTimeout = prev }()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)

	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.ToolCallReq{Op: ipc.OpToolCall, ID: "1", Name: "reply", Args: map[string]any{"text": "hi"}})

	done := make(chan struct{})
	go func() {
		b.handleToolCall(brokerSide, stub, raw)
		close(done)
	}()

	respCh := make(chan ipc.ToolResultMsg, 1)
	go func() {
		frame, err := agentSide.ReadFrame()
		if err != nil {
			return
		}
		var r ipc.ToolResultMsg
		_ = json.Unmarshal(frame, &r)
		respCh <- r
	}()

	select {
	case resp := <-respCh:
		if resp.Error == nil || resp.Error.Message == "" {
			t.Fatalf("stalled worker: expected a non-empty Error, got %+v", resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handleToolCall did not return on a stalled worker — the read loop is wedged")
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleToolCall did not return after writing the timeout error")
	}
}

func runHandlerWithPeer(t *testing.T, mf *mappings.MappingsFile) (*ipc.Conn, func()) {
	t.Helper()
	a, b := net.Pipe()
	br := New(mf)
	go br.HandleConn(a)
	return ipc.NewConn(b), func() {
		_ = a.Close()
		_ = b.Close()
	}
}

func emptyMappings() *mappings.MappingsFile {
	return &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]mappings.ChannelConfig{},
		Mappings:      map[string]mappings.Mapping{},
	}
}

func TestHandle_HelloAck_NoConfig(t *testing.T) {
	mf := &mappings.MappingsFile{SchemaVersion: 1}
	peer, done := runHandlerWithPeer(t, mf)
	defer done()

	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 1, CWD: "/x"}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var ack ipc.HelloAckMsg
	if err := json.Unmarshal(raw, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Op != ipc.OpHelloAck {
		t.Errorf("op=%q, want hello_ack", ack.Op)
	}
	if !ack.NoConfig {
		t.Error("expected NoConfig=true when channels map is empty")
	}
	if ack.ConnID == 0 {
		t.Error("ConnID should be assigned")
	}
}

func TestHandle_HelloAck_NoMapping(t *testing.T) {
	mf := emptyMappings()
	mf.Channels["telegram"] = mappings.ChannelConfig{DefaultGroup: "main"}
	peer, done := runHandlerWithPeer(t, mf)
	defer done()

	_ = peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 1, CWD: "/unknown"})
	raw, _ := peer.ReadFrame()
	var ack ipc.HelloAckMsg
	_ = json.Unmarshal(raw, &ack)

	if ack.NoConfig {
		t.Error("NoConfig should be false when channel exists")
	}
	if !ack.NoMapping {
		t.Error("NoMapping should be true for unknown cwd")
	}
}

func TestHandle_ListTopics(t *testing.T) {
	mf := emptyMappings()
	mf.Channels["telegram"] = mappings.ChannelConfig{
		DefaultGroup: "main",
		Groups:       map[string]mappings.GroupConfig{"main": {ChatID: -100}},
		Topics: []mappings.Topic{
			{ChatID: -100, TopicID: 281, Name: "c3", Group: "main"},
		},
	}
	peer, done := runHandlerWithPeer(t, mf)
	defer done()

	_ = peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 1, CWD: "/x"})
	_, _ = peer.ReadFrame() // consume hello_ack

	_ = peer.WriteJSON(ipc.ListTopicsReq{Op: ipc.OpListTopics})
	raw, _ := peer.ReadFrame()

	var resp ipc.TopicsListMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Op != ipc.OpTopicsList {
		t.Errorf("op=%q, want topics_list", resp.Op)
	}
	if len(resp.Topics) != 1 || resp.Topics[0].Name != "c3" {
		t.Errorf("topics = %+v, want one entry name=c3", resp.Topics)
	}
}

// TestConnDrop_ReleasesClaimWhenPIDDead exercises the conn-drop defer
// in HandleConn: when the adapter's PID is no longer alive at conn-
// close time, every claim held by that stub must be released so a
// future attach (or fallback delivery) isn't blocked by a ghost
// holder. The trick is feeding a sentinel PID (-1) that isPIDAlive
// rejects via its `pid <= 0` short-circuit; the defer then takes the
// dead-PID branch and calls Routes.ReleaseAllByConnID. TODO #19(d) —
// maintainer 2026-05-18.
func TestConnDrop_ReleasesClaimWhenPIDDead(t *testing.T) {
	mf := emptyMappings()
	mf.Channels["telegram"] = mappings.ChannelConfig{
		DefaultGroup: "main",
		Groups:       map[string]mappings.GroupConfig{"main": {ChatID: -100}},
		DMChatID:     42,
	}
	br := New(mf)
	defer br.Shutdown()

	a, b := net.Pipe()
	handlerDone := make(chan struct{})
	go func() {
		br.HandleConn(a)
		close(handlerDone)
	}()
	peer := ipc.NewConn(b)
	defer peer.Close()

	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: -1, CWD: "/x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, Target: "dm"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	// Sanity: the claim is registered.
	if got := len(br.Routes.Snapshot()); got != 1 {
		t.Fatalf("post-attach Routes size = %d, want 1", got)
	}

	// Drop the conn — closing the broker-side pipe triggers ReadFrame
	// error and the deferred PID-dead branch.
	_ = b.Close()
	_ = a.Close()
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not return within 2s after conn drop")
	}
	if got := len(br.Routes.Snapshot()); got != 0 {
		t.Errorf("post-conn-drop Routes size = %d, want 0 (claims must be released when PID is dead)", got)
	}
}

func TestHandle_ByeClosesCleanly(t *testing.T) {
	mf := emptyMappings()
	peer, done := runHandlerWithPeer(t, mf)
	defer done()

	_ = peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 1, CWD: "/x"})
	_, _ = peer.ReadFrame()

	_ = peer.WriteJSON(ipc.ByeReq{Op: ipc.OpBye})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := peer.ReadFrame()
		if err == io.EOF {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("broker did not close conn after bye within 2s")
}
