package main

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// readDeliveredAck reads one frame off the broker-side conn within a deadline and
// returns the parsed InboundDeliveredMsg + whether a frame arrived. Used by the
// C1 ack-guard test. A nil/closed read or timeout means "no ack was sent".
func readDeliveredAck(t *testing.T, c *ipc.Conn, within time.Duration) (ipc.InboundDeliveredMsg, bool) {
	t.Helper()
	type res struct {
		raw []byte
		err error
	}
	ch := make(chan res, 1)
	go func() {
		raw, err := c.ReadFrame()
		ch <- res{raw, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return ipc.InboundDeliveredMsg{}, false
		}
		var m ipc.InboundDeliveredMsg
		if err := json.Unmarshal(r.raw, &m); err != nil {
			t.Fatalf("unmarshal delivered ack: %v", err)
		}
		return m, true
	case <-time.After(within):
		return ipc.InboundDeliveredMsg{}, false
	}
}

// adapterWithConn wires an adapter with a working notifyTx (so handleInbound's
// Notify succeeds and proceeds to the ack step) and a broker-side conn whose peer
// the test reads. Returns the adapter and the agent-side conn (the broker's view).
func adapterWithConn(t *testing.T) (*adapter, *ipc.Conn) {
	t.Helper()
	a := newAdapter()

	// A notifyTx backed by an in-memory IOTransport so Notify succeeds.
	var buf safeBuffer
	tx := newNotifyTransport(&mcp.IOTransport{
		Reader: nopCloseReader{strings.NewReader("")},
		Writer: nopCloseWriter{&buf},
	})
	if _, err := tx.Connect(context.Background()); err != nil {
		t.Fatalf("notifyTx Connect: %v", err)
	}
	a.notifyTx = tx

	// Broker socket pair: the adapter writes the delivered-ack to a.conn; the test
	// reads it from the peer.
	peerSide, brokerSide := net.Pipe()
	t.Cleanup(func() { peerSide.Close(); brokerSide.Close() })
	a.bmu.Lock()
	a.conn = ipc.NewConn(brokerSide)
	a.bmu.Unlock()
	return a, ipc.NewConn(peerSide)
}

// C1 (adapter side): a TEXT push must send a delivered-ack so the broker Consumes
// the queued copy.
func TestHandleInbound_TextPushSendsDeliveredAck(t *testing.T) {
	a, peer := adapterWithConn(t)

	msg := ipc.InboundMsg{
		Op:      ipc.OpInbound,
		Covered: 1,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 7, Text: "hi"},
	}
	raw, _ := json.Marshal(msg)
	go a.handleInbound(context.Background(), raw)

	ack, got := readDeliveredAck(t, peer, time.Second)
	if !got {
		t.Fatal("a text push must send an OpInboundDelivered ack so the broker consumes the queued copy")
	}
	if !ack.OK || ack.UpdateID != 7 || ack.Count != 1 {
		t.Fatalf("text ack = %+v, want {OK:true UpdateID:7 Count:1}", ack)
	}
}

// C1 (adapter side): an EVENT push (poll_result / reaction / callback) is NEVER
// queued, so the adapter must NOT send a delivered-ack — otherwise handleConsume
// would drop a real queued backlog message the event never delivered.
func TestHandleInbound_EventPushSkipsDeliveredAck(t *testing.T) {
	a, peer := adapterWithConn(t)

	event := ipc.InboundMsg{
		Op: ipc.OpInbound,
		// The broker stamps Covered=0 for events (C1), but even a stray Covered>0
		// must not trigger an ack for an event — the IsEvent guard is the gate.
		Covered: 1,
		Inbound: c3types.Inbound{
			Channel: "telegram", ChatID: -100, MessageID: 8,
			Kind:  c3types.InboundPollResult,
			Event: &c3types.InboundEvent{PollResult: &c3types.PollResult{PollID: "p", IsClosed: true}},
		},
	}
	raw, _ := json.Marshal(event)
	go a.handleInbound(context.Background(), raw)

	if _, got := readDeliveredAck(t, peer, 300*time.Millisecond); got {
		t.Fatal("an event push must NOT send a delivered-ack (events are never queued; an ack would over-consume real backlog)")
	}
}

// Item E (push-path nudge on the PUSH content): when the broker reports remaining
// backlog (in.Pending > 0), handleInbound must decorate the PUSHED channel
// content with the recovery nudge ("(N pending — call fetch_queue)") — not only
// the delivered-ack. The existing ack tests assert the ack frame; this asserts
// the nudge text lands on the notify push itself.
func TestHandleInbound_PendingDecoratesPushWithNudge(t *testing.T) {
	a := newAdapter()

	// A notifyTx backed by an in-memory buffer we can inspect for the pushed frame.
	var buf safeBuffer
	tx := newNotifyTransport(&mcp.IOTransport{
		Reader: nopCloseReader{strings.NewReader("")},
		Writer: nopCloseWriter{&buf},
	})
	if _, err := tx.Connect(context.Background()); err != nil {
		t.Fatalf("notifyTx Connect: %v", err)
	}
	a.notifyTx = tx

	// Broker socket pair so the post-push delivered-ack write has somewhere to go.
	peerSide, brokerSide := net.Pipe()
	t.Cleanup(func() { peerSide.Close(); brokerSide.Close() })
	a.bmu.Lock()
	a.conn = ipc.NewConn(brokerSide)
	a.bmu.Unlock()
	// Drain the broker side so the ack write doesn't block on the pipe.
	go func() {
		c := ipc.NewConn(peerSide)
		for {
			if _, err := c.ReadFrame(); err != nil {
				return
			}
		}
	}()

	msg := ipc.InboundMsg{
		Op:      ipc.OpInbound,
		Covered: 1,
		Pending: 3, // broker reports 3 still queued ⇒ nudge must appear on the push
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 7, Text: "hi"},
	}
	raw, _ := json.Marshal(msg)
	a.handleInbound(context.Background(), raw)

	// The pushed notification (serialized to buf) must carry the recovery nudge.
	out := string(buf.Bytes())
	if !strings.Contains(out, "3 pending") || !strings.Contains(out, "fetch_queue") {
		t.Fatalf("push content must carry the recovery nudge '(3 pending — call fetch_queue)'; pushed frame was:\n%s", out)
	}
}
