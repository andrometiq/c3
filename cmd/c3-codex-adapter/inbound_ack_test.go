package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// readDeliveredAck reads one frame off the broker-side conn within a deadline and
// returns the parsed InboundDeliveredMsg + whether a frame arrived. A nil/closed
// read or timeout means "no ack was sent". Mirrors the Claude adapter's helper of
// the same name (cmd/c3-claude-adapter/inbound_ack_test.go).
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

// adapterWithBrokerConn wires an adapter with a broker-side conn whose peer the
// test reads (the broker's view of the adapter's delivered-ack writes). The Codex
// adapter's ack path (D-RC2) does not depend on the notify transport, so transport
// is left nil (handleInbound skips notify when a.transport == nil).
func adapterWithBrokerConn(t *testing.T) (*adapter, *ipc.Conn) {
	t.Helper()
	a := newAdapter()
	peerSide, brokerSide := net.Pipe()
	t.Cleanup(func() { peerSide.Close(); brokerSide.Close() })
	a.bmu.Lock()
	a.conn = ipc.NewConn(brokerSide)
	a.bmu.Unlock()
	return a, ipc.NewConn(peerSide)
}

// startFakeCodexAppServer stands up a WebSocket app-server that completes the
// Codex turn handshake so forwardInboundToCodexAppServer succeeds. Mirrors the
// success-responding server in forwarder_test.go. Returns the ws:// URL.
func startFakeCodexAppServer(t *testing.T) string {
	t.Helper()
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			var msg map[string]any
			if err := c.ReadJSON(&msg); err != nil {
				return
			}
			id, hasID := msg["id"]
			if !hasID {
				continue
			}
			method, _ := msg["method"].(string)
			result := map[string]any{}
			switch method {
			case "thread/loaded/list":
				result["data"] = []string{"thread-1"}
			case "turn/start":
				result["turn"] = map[string]any{"id": "turn-1"}
			default:
				result["ok"] = true
			}
			if err := c.WriteJSON(map[string]any{"id": id, "result": result}); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	return "ws" + server.URL[len("http"):]
}

// startFlakyCodexAppServer stands up a fake Codex app-server where the first
// failFirstN WebSocket dials are REFUSED (HTTP 500 → dial error → the forward
// fails), and every dial after that completes the turn handshake successfully.
// Because codexForwardLoop processes forwards serially, the Nth dial corresponds
// to the Nth enqueued inbound — so failFirstN=1 fails exactly the first (oldest)
// message and succeeds the rest. turns receives one value each time a turn/start
// completes, so a test can wait until a later message was actually delivered live
// before asserting on the (absence of an) ack.
func startFlakyCodexAppServer(t *testing.T, failFirstN int) (wsURL string, turns <-chan struct{}) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	turnCh := make(chan struct{}, 16)
	var connCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if int(connCount.Add(1)) <= failFirstN {
			http.Error(w, "forced forward failure", http.StatusInternalServerError)
			return
		}
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			var msg map[string]any
			if err := c.ReadJSON(&msg); err != nil {
				return
			}
			id, hasID := msg["id"]
			if !hasID {
				continue
			}
			method, _ := msg["method"].(string)
			result := map[string]any{}
			switch method {
			case "thread/loaded/list":
				result["data"] = []string{"thread-1"}
			case "turn/start":
				result["turn"] = map[string]any{"id": "turn-1"}
			default:
				result["ok"] = true
			}
			if err := c.WriteJSON(map[string]any{"id": id, "result": result}); err != nil {
				return
			}
			if method == "turn/start" {
				select {
				case turnCh <- struct{}{}:
				default:
				}
			}
		}
	}))
	t.Cleanup(server.Close)
	return "ws" + server.URL[len("http"):], turnCh
}

// M2 loss regression: an OLDER message whose forward FAILS (correctly no ack)
// followed by a NEWER message whose forward SUCCEEDS must NOT produce an ack for
// the newer one — the broker's consume is count-off-HEAD, so acking the newer
// message would Consume the OLDER (undelivered) message off the head → silent
// loss. The retired per-inbound-goroutine design acked the newer message here;
// the serial loop + `blocked` latch must not.
func TestCodexForward_OlderFailLaterSuccess_DoesNotAck(t *testing.T) {
	wsURL, turns := startFlakyCodexAppServer(t, 1) // first (oldest) forward fails; rest succeed
	t.Setenv("C3_CODEX_ALLOW_MANUAL_FORWARD", "1")
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "")
	t.Setenv("C3_CODEX_APP_SERVER_WS", wsURL)
	t.Setenv("C3_CODEX_THREAD_ID", "")

	a, peer := adapterWithBrokerConn(t)

	older := ipc.InboundMsg{Op: ipc.OpInbound, Covered: 1, Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 1, Text: "older"}}
	newer := ipc.InboundMsg{Op: ipc.OpInbound, Covered: 1, Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 2, Text: "newer"}}
	rawOlder, _ := json.Marshal(older)
	rawNewer, _ := json.Marshal(newer)
	a.handleInbound(rawOlder) // forward will FAIL → no ack, loop becomes blocked
	a.handleInbound(rawNewer) // forward will SUCCEED but loop is blocked → must NOT ack

	// Wait until the newer message's forward actually reaches turn/start, proving it
	// was delivered live — so the only reason for no ack is the `blocked` latch.
	select {
	case <-turns:
	case <-time.After(3 * time.Second):
		t.Fatal("the newer message's forward never completed a turn/start")
	}

	if ack, got := readDeliveredAck(t, peer, 500*time.Millisecond); got {
		t.Fatalf("no ack may be sent while the older (head) message is undelivered — got %+v; acking it would Consume the older message off the head (loss)", ack)
	}
}

// M2 common-case: two messages whose forwards both SUCCEED must both be acked, in
// queue order (head-first), with each ack carrying its own covered count. Confirms
// the serial path preserves the RC2 ack intent (the broker Consumes delivered live
// pushes) — only the failure case stops acking.
func TestCodexForward_AllSuccess_AcksInOrder(t *testing.T) {
	wsURL, _ := startFlakyCodexAppServer(t, 0) // never fail
	t.Setenv("C3_CODEX_ALLOW_MANUAL_FORWARD", "1")
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "")
	t.Setenv("C3_CODEX_APP_SERVER_WS", wsURL)
	t.Setenv("C3_CODEX_THREAD_ID", "")

	a, peer := adapterWithBrokerConn(t)

	first := ipc.InboundMsg{Op: ipc.OpInbound, Covered: 1, Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 1, Text: "first"}}
	second := ipc.InboundMsg{Op: ipc.OpInbound, Covered: 2, Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 2, Text: "second"}}
	rawFirst, _ := json.Marshal(first)
	rawSecond, _ := json.Marshal(second)
	a.handleInbound(rawFirst)
	a.handleInbound(rawSecond)

	ack1, got1 := readDeliveredAck(t, peer, 3*time.Second)
	if !got1 {
		t.Fatal("first successful forward must ack")
	}
	ack2, got2 := readDeliveredAck(t, peer, 3*time.Second)
	if !got2 {
		t.Fatal("second successful forward must ack")
	}
	if !ack1.OK || ack1.UpdateID != 1 || ack1.Count != 1 {
		t.Fatalf("ack1 = %+v, want {OK:true UpdateID:1 Count:1} (head-first)", ack1)
	}
	if !ack2.OK || ack2.UpdateID != 2 || ack2.Count != 2 {
		t.Fatalf("ack2 = %+v, want {OK:true UpdateID:2 Count:2}", ack2)
	}
}

// D-RC2: a live-forwarded text push that the app-server ACCEPTS must send an
// OpInboundDelivered ack so the broker Consumes the queued copy. Without this the
// Codex adapter never acks, and every live-forwarded message stays queued forever
// (fetch_queue keeps re-surfacing already-delivered content).
func TestHandleInbound_Codex_ForwardSuccessSendsDeliveredAck(t *testing.T) {
	wsURL := startFakeCodexAppServer(t)
	t.Setenv("C3_CODEX_ALLOW_MANUAL_FORWARD", "1")
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "")
	t.Setenv("C3_CODEX_APP_SERVER_WS", wsURL)
	t.Setenv("C3_CODEX_THREAD_ID", "")

	a, peer := adapterWithBrokerConn(t)

	msg := ipc.InboundMsg{
		Op:            ipc.OpInbound,
		Covered:       1,
		DeliveryToken: "broker-a-7",
		Inbound:       c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 7, Text: "hi"},
	}
	raw, _ := json.Marshal(msg)
	a.handleInbound(raw)

	ack, got := readDeliveredAck(t, peer, 2*time.Second)
	if !got {
		t.Fatal("a successful live forward must send an OpInboundDelivered ack so the broker consumes the queued copy")
	}
	if !ack.OK || ack.UpdateID != 7 || ack.Count != 1 || ack.DeliveryToken != "broker-a-7" {
		t.Fatalf("ack = %+v, want {OK:true UpdateID:7 Count:1 DeliveryToken:broker-a-7}", ack)
	}
}

// D-RC2 (Watch-out #6): when forwarding is enabled but the app-server is
// unreachable, the forward FAILS — the adapter must NOT ack, so the content stays
// queued for fetch_queue recovery.
func TestHandleInbound_Codex_ForwardFailureDoesNotAck(t *testing.T) {
	t.Setenv("C3_CODEX_ALLOW_MANUAL_FORWARD", "1")
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "")
	// Port 1 has no listener → dial refuses immediately → forward returns an error.
	t.Setenv("C3_CODEX_APP_SERVER_WS", "ws://127.0.0.1:1")
	t.Setenv("C3_CODEX_THREAD_ID", "")

	a, peer := adapterWithBrokerConn(t)

	msg := ipc.InboundMsg{
		Op:      ipc.OpInbound,
		Covered: 1,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 8, Text: "hi"},
	}
	raw, _ := json.Marshal(msg)
	a.handleInbound(raw)

	if _, got := readDeliveredAck(t, peer, 500*time.Millisecond); got {
		t.Fatal("a failed forward must NOT ack (content must stay queued for fetch_queue recovery)")
	}
}

// D-RC2 (Watch-out #6): when forwarding is DISABLED (notify-only path), nothing is
// delivered live, so the adapter must NOT ack — fetch_queue stays the source of
// truth. Makes the existing no-ack-when-disabled behavior explicit.
func TestHandleInbound_Codex_ForwardingDisabled_NoAck(t *testing.T) {
	t.Setenv("C3_CODEX_ALLOW_MANUAL_FORWARD", "")
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "")
	t.Setenv("C3_CODEX_APP_SERVER_WS", "")

	a, peer := adapterWithBrokerConn(t)

	msg := ipc.InboundMsg{
		Op:      ipc.OpInbound,
		Covered: 1,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 9, Text: "hi"},
	}
	raw, _ := json.Marshal(msg)
	a.handleInbound(raw)

	if _, got := readDeliveredAck(t, peer, 300*time.Millisecond); got {
		t.Fatal("forwarding disabled (notify-only) must NOT ack; fetch_queue stays the source of truth")
	}
}

// D-RC2 loss-guard: forwarding ENABLED but no app-server WS URL configured. The
// forward is a no-op (delivered nothing), so the adapter must NOT ack — otherwise
// the broker consumes the queued copy of a message the agent never received live
// (a silent drop). codexForwardingAllowed() gates only on the env FLAGS, not the
// WS URL, so this misconfiguration is reachable; the empty-URL path now returns a
// sentinel error instead of nil so the caller skips the ack.
func TestHandleInbound_Codex_ForwardingEnabledButNoWSURL_NoAck(t *testing.T) {
	t.Setenv("C3_CODEX_ALLOW_MANUAL_FORWARD", "1") // forwarding ENABLED
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "")
	t.Setenv("C3_CODEX_APP_SERVER_WS", "") // but no WS URL → forward no-ops
	t.Setenv("C3_CODEX_THREAD_ID", "")

	a, peer := adapterWithBrokerConn(t)

	msg := ipc.InboundMsg{
		Op:      ipc.OpInbound,
		Covered: 1,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 10, Text: "hi"},
	}
	raw, _ := json.Marshal(msg)
	a.handleInbound(raw)

	if _, got := readDeliveredAck(t, peer, 500*time.Millisecond); got {
		t.Fatal("forwarding enabled but no WS URL: the forward delivered nothing — must NOT ack (content must stay queued, loss-freedom)")
	}
}
