package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// protoLogSink is a goroutine-safe log sink: brokerReader logs from its own
// goroutine while the test reads.
type protoLogSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *protoLogSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *protoLogSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func captureProtoLog(t *testing.T) *protoLogSink {
	t.Helper()
	s := &protoLogSink{}
	oldOut, oldFlags := log.Writer(), log.Flags()
	log.SetOutput(s)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
	})
	return s
}

func waitForLog(t *testing.T, s *protoLogSink, substr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(s.String(), substr) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("log never contained %q; log was:\n%s", substr, s.String())
}

// hello must carry this build's protocol version, and a broker that answers with
// a DIFFERENT version must NOT break the handshake — the adapter warns and keeps
// the session. (`c3 update` routinely leaves an old adapter on a new broker.)
func TestHello_CarriesVersion_AndSurvivesMismatchedAck(t *testing.T) {
	sink := captureProtoLog(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	a := newAdapter()
	a.conn = ipc.NewConn(c1)
	peer := ipc.NewConn(c2)

	errCh := make(chan error, 1)
	go func() { errCh <- a.hello() }()

	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("no hello frame: %v", err)
	}
	var hello ipc.HelloMsg
	if err := json.Unmarshal(raw, &hello); err != nil {
		t.Fatalf("unmarshal hello: %v\nwire: %s", err, raw)
	}
	if hello.ProtocolVersion != ipc.ProtocolVersion {
		t.Errorf("hello.ProtocolVersion = %d, want %d\nwire: %s", hello.ProtocolVersion, ipc.ProtocolVersion, raw)
	}

	// A broker one version ahead.
	brokerVersion := ipc.ProtocolVersion + 1
	if err := peer.WriteJSON(ipc.HelloAckMsg{
		Op: ipc.OpHelloAck, ConnID: 7, ProtocolVersion: brokerVersion,
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("hello() FAILED on a version mismatch (%v) — it must warn and continue", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hello() did not return after hello_ack")
	}
	if a.helloAck.ConnID != 7 {
		t.Errorf("helloAck not stored (ConnID=%d) — the handshake must complete despite the mismatch", a.helloAck.ConnID)
	}

	out := sink.String()
	if !strings.Contains(out, "PROTOCOL VERSION MISMATCH") {
		t.Errorf("no mismatch warning logged; log was:\n%s", out)
	}
	for _, want := range []string{"v" + strconv.Itoa(brokerVersion), "v" + strconv.Itoa(ipc.ProtocolVersion)} {
		if !strings.Contains(out, want) {
			t.Errorf("warning does not name %q; log was:\n%s", want, out)
		}
	}
}

func TestDetach_IncompatibleBrokerRefusesWithoutClearingReplay(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	a := newAdapter()
	a.conn = ipc.NewConn(c1)
	go func() { _, _ = ipc.NewConn(c2).ReadFrame() }()
	a.brokerVersion.Store(int64(ipc.CompatibleProtocolMax + 1))
	a.lastAttach = &ipc.AttachReq{Op: ipc.OpAttach, Name: "kept"}
	res, err := a.toolDetach(context.Background(), nil)
	if err != nil || !res.IsError {
		t.Fatalf("incompatible detach reported success: result=%+v err=%v", res, err)
	}
	if a.lastAttach == nil {
		t.Fatal("incompatible detach cleared replay state even though the broker retained the claim")
	}
}

// An ack from a broker that predates versioning (no field) is v1 — no warning.
func TestHello_LegacyAckNoVersion_NoWarning(t *testing.T) {
	sink := captureProtoLog(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	a := newAdapter()
	a.conn = ipc.NewConn(c1)
	peer := ipc.NewConn(c2)

	errCh := make(chan error, 1)
	go func() { errCh <- a.hello() }()
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := peer.WriteJSON(json.RawMessage(`{"op":"hello_ack","conn_id":3}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("hello() failed against a legacy broker: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hello() did not return")
	}
	if strings.Contains(sink.String(), "PROTOCOL VERSION MISMATCH") {
		t.Errorf("legacy ack (absent version == v1) must not warn; log was:\n%s", sink.String())
	}
}

// An op from a NEWER broker must hit the dispatch default arm: logged by name
// and skipped, never silently dropped.
func TestBrokerReader_UnknownOp_LogsAndContinues(t *testing.T) {
	sink := captureProtoLog(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	a := newAdapter()
	a.conn = ipc.NewConn(c1)
	peer := ipc.NewConn(c2)

	ctx, cancel := context.WithCancel(context.Background())
	readerDone := make(chan struct{})
	go func() { a.brokerReader(ctx); close(readerDone) }()

	if err := peer.WriteJSON(json.RawMessage(`{"op":"op_from_a_newer_broker","x":1}`)); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, sink, "op_from_a_newer_broker")
	if !strings.Contains(sink.String(), "unknown op") {
		t.Errorf("default arm should say the op is unknown; log was:\n%s", sink.String())
	}

	// "and continues": a known op right after the unknown one is still handled.
	if err := peer.WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: "still-alive-probe"}); err != nil {
		t.Fatalf("reader died after an unknown op: %v", err)
	}
	waitForLog(t, sink, "still-alive-probe")

	cancel()
	_ = c2.Close() // read error → recoverBroker sees the canceled ctx → reader returns
	select {
	case <-readerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("brokerReader did not exit")
	}
}
