package main

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// hello must carry this build's protocol version, and a broker that answers
// with a DIFFERENT version must NOT break the handshake — the adapter warns
// and keeps the session. (`c3 update` routinely leaves an old adapter on a
// new broker.) Parity with the agy/cursor adapters' protocol_version_test.
func TestHelloCarriesVersionAndSurvivesMismatchedAck(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	a := newAdapter()
	a.eventPath = "/nonexistent/events.sock" // live-capable path; irrelevant here
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
		t.Fatalf("parse hello: %v", err)
	}
	if hello.ProtocolVersion != ipc.ProtocolVersion {
		t.Fatalf("hello.ProtocolVersion = %d, want %d", hello.ProtocolVersion, ipc.ProtocolVersion)
	}

	// Answer with a future version: the handshake must still complete.
	if err := peer.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck, ConnID: 9, ProtocolVersion: ipc.ProtocolVersion + 1}); err != nil {
		t.Fatalf("write mismatched ack: %v", err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("hello failed on version mismatch (must warn and continue): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hello did not complete after mismatched ack")
	}

	// And the adapter must have recorded the peer's (mismatched) version so
	// the state-change compatibility refusals can key off it.
	if got := int(a.brokerVersion.Load()); got != ipc.ProtocolVersion+1 {
		t.Fatalf("brokerVersion = %d, want %d (peer's reported version)", got, ipc.ProtocolVersion+1)
	}

	// An incompatible dialect means destructive ops must fail closed. The
	// compatibility window is v1..v1, so the future version the peer reported
	// must be excluded — the exact predicate attach/detach/fetch_queue(ack)
	// and the inbound ack key off.
	if ipc.ProtocolStateChangesCompatible(int(a.brokerVersion.Load())) {
		t.Fatal("compatibility window must exclude a future protocol version (v1..v1)")
	}
}

// A nil-capabilities hello_ack must fall back to an all-false manifest —
// never fabricate a capability (docs/ADAPTERS.md §hello_ack).
func TestCapsOrDefaultNil(t *testing.T) {
	a := newAdapter()
	if got := a.capsOrDefault(); got.Channel != "" {
		t.Fatalf("nil capabilities fabricated a channel %q — must be zero value", got.Channel)
	}
	a.helloAck.Capabilities = nil
	if got := a.capsOrDefault(); got.Channel != "" {
		t.Fatalf("re-read after nil still fabricated: %q", got.Channel)
	}
}
