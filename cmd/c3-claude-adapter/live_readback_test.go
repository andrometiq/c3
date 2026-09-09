package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func liveFixture(t *testing.T, state string) (*adapter, *safeBuffer, <-chan []byte) {
	t.Helper()
	a, peer := adapterWithConn(t)
	a.renderRoute = ipc.RenderRoute{State: state}
	a.liveTimeout = 180 * time.Millisecond
	var output safeBuffer
	a.notifyTx = newNotifyTransport(&mcp.IOTransport{Reader: nopCloseReader{strings.NewReader("")}, Writer: nopCloseWriter{&output}})
	if _, err := a.notifyTx.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	frames := make(chan []byte, 16)
	go func() {
		defer close(frames)
		for {
			raw, err := peer.ReadFrame()
			if err != nil {
				return
			}
			frames <- raw
		}
	}()
	return a, &output, frames
}

func pushLive(t *testing.T, a *adapter, token string) {
	t.Helper()
	raw, _ := json.Marshal(ipc.InboundMsg{Op: ipc.OpInbound, Covered: 2, DeliveryToken: token,
		Inbound: c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 7, Text: "hello"}})
	a.handleInbound(context.Background(), raw)
}

func nextLiveFrame(t *testing.T, frames <-chan []byte) []byte {
	t.Helper()
	select {
	case raw := <-frames:
		return raw
	case <-time.After(2 * time.Second):
		t.Fatal("no broker frame")
		return nil
	}
}

func noLiveFrame(t *testing.T, frames <-chan []byte) {
	t.Helper()
	select {
	case raw := <-frames:
		t.Fatalf("unexpected broker frame: %s", raw)
	case <-time.After(250 * time.Millisecond):
	}
}

func TestLiveReadbackReceiptBeforeAck(t *testing.T) {
	isolateAdapterTest(t)
	for _, state := range []string{ipc.RenderCapable, ipc.RenderProbing} {
		t.Run(state, func(t *testing.T) {
			a, output, frames := liveFixture(t, state)
			a.liveTimeout = time.Second
			pushLive(t, a, "receipt-1")
			if !strings.Contains(string(output.Bytes()), `"c3_delivery_id":"receipt-1"`) {
				t.Fatal("missing marker in notification metadata")
			}
			select {
			case raw := <-frames:
				t.Fatalf("write success acked before readback: %s", raw)
			default:
			}
			// Enqueue without the attempt marker is not a receipt.
			f, err := os.OpenFile(a.livePath(), os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.WriteString(`{"type":"queue-operation","operation":"enqueue","content":"<channel c3_delivery_id=\"receipt-1\">hello</channel>"}` + "\n")
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			select {
			case raw := <-frames:
				t.Fatalf("enqueue without attempt acked: %s", raw)
			case <-time.After(120 * time.Millisecond):
			}
			appendChannelReceipt(t, a.livePath(), "receipt-1")
			raw := nextLiveFrame(t, frames)
			if state == ipc.RenderProbing {
				var update ipc.RenderStateMsg
				if json.Unmarshal(raw, &update) != nil || update.State != ipc.RenderCapable {
					t.Fatalf("probe update: %s", raw)
				}
				raw = nextLiveFrame(t, frames)
			}
			var ack ipc.InboundDeliveredMsg
			if json.Unmarshal(raw, &ack) != nil || ack.Op != ipc.OpInboundDelivered || !ack.OK || ack.Count != 2 || ack.DeliveryToken != "receipt-1" {
				t.Fatalf("ack: %s", raw)
			}
			appendChannelReceipt(t, a.livePath(), "receipt-1")
			noLiveFrame(t, frames)
		})
	}
}

func TestLiveReadbackTimeoutStopsPushes(t *testing.T) {
	isolateAdapterTest(t)
	a, output, frames := liveFixture(t, ipc.RenderProbing)
	pushLive(t, a, "timeout-1")
	var update ipc.RenderStateMsg
	raw := nextLiveFrame(t, frames)
	if json.Unmarshal(raw, &update) != nil || update.Op != ipc.OpRenderState || update.State != ipc.RenderQueueOnly || update.Reason != "live push not confirmed" {
		t.Fatalf("timeout: %s", raw)
	}
	before := string(output.Bytes())
	pushLive(t, a, "timeout-2")
	if string(output.Bytes()) != before {
		t.Fatal("message pushed after downgrade")
	}
	appendChannelReceipt(t, a.livePath(), "timeout-1")
	noLiveFrame(t, frames) // late records do not create a second ack/notice
	if a.liveRoute().State != ipc.RenderQueueOnly {
		t.Fatal("late record re-enabled live route")
	}
}

func TestLiveReadbackUnresolvableTranscript(t *testing.T) {
	isolateAdapterTest(t)
	a, output, frames := liveFixture(t, ipc.RenderCapable)
	a.liveTranscriptPath = func() string { return "" }
	pushLive(t, a, "missing-1")
	raw := nextLiveFrame(t, frames)
	var update ipc.RenderStateMsg
	if json.Unmarshal(raw, &update) != nil || update.State != ipc.RenderQueueOnly || update.Reason != "session transcript unavailable" {
		t.Fatalf("missing path: %s", raw)
	}
	if len(output.Bytes()) != 0 {
		t.Fatal("pushed without a confirmable transcript")
	}
	pushLive(t, a, "missing-2")
	noLiveFrame(t, frames)
}

func TestChannelReceiptShapeAndPartialTail(t *testing.T) {
	isolateAdapterTest(t)
	channel := `<channel source="c3" c3_attempt="channel:1" c3_delivery_id="marker">hello</channel>`
	for _, tc := range []struct {
		name  string
		entry any
		want  bool
	}{
		{"string", map[string]any{"type": "user", "message": map[string]any{"role": "user", "content": channel}}, true},
		{"blocks", map[string]any{"type": "user", "message": map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": channel}}}}, true},
		{"assistant", map[string]any{"type": "assistant", "message": map[string]any{"role": "assistant", "content": channel}}, false},
		{"tool result", map[string]any{"type": "user", "message": map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "text": channel}}}}, false},
		{"enqueue", map[string]any{"type": "queue-operation", "operation": "enqueue", "content": channel}, true},
		{"wrong role", map[string]any{"type": "user", "message": map[string]any{"role": "assistant", "content": channel}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(tc.entry)
			if got := channelReceipt(raw, "marker"); got != tc.want {
				t.Fatalf("receipt = %v", got)
			}
			if channelReceipt(raw, "other") {
				t.Fatal("wrong marker confirmed")
			}
		})
	}
	a := newAdapter()
	path := seedLiveTranscript(t, a)
	raw, _ := json.Marshal(map[string]any{"type": "user", "message": map[string]any{"role": "user", "content": channel}})
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if offset, found := scanChannelReceipt(path, 0, "marker", new(bool)); offset != 0 || found {
		t.Fatalf("partial line consumed: %d %v", offset, found)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if _, found := scanChannelReceipt(path, 0, "marker", new(bool)); !found {
		t.Fatal("completed line not confirmed")
	}
	// Oversized records are skipped without swallowing the next valid receipt.
	large := append([]byte(strings.Repeat("x", maxTranscriptLineBytes+10)+"\n"), append(raw, '\n')...)
	if err := os.WriteFile(path, large, 0600); err != nil {
		t.Fatal(err)
	}
	if _, found := scanChannelReceipt(path, 0, "marker", new(bool)); !found {
		t.Fatal("oversized line hid following receipt")
	}
}

func TestLiveRouteResetAndPreamble(t *testing.T) {
	isolateAdapterTest(t)
	for _, tc := range []struct{ state, text, relay string }{
		{ipc.RenderCapable, "channel", "available"},
		{ipc.RenderProbing, "probing", "unavailable"},
		{ipc.RenderQueueOnly, "queue-only", "unavailable"},
	} {
		t.Run(tc.state, func(t *testing.T) {
			a := newAdapter()
			seedLiveTranscript(t, a)
			a.initialRenderRoute = ipc.RenderRoute{State: tc.state}
			a.renderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "live push not confirmed"}
			a.resetLiveRoute(false)
			got := a.buildInstructions()
			if !strings.Contains(got, "Live route: "+tc.text+".") || !strings.Contains(got, "Permission relay: "+tc.relay+".") {
				t.Fatalf("preamble: %s", got)
			}
			a.liveTranscriptPath = func() string { return "" }
			a.resetLiveRoute(false)
			if a.liveRoute().State != ipc.RenderQueueOnly {
				t.Fatal("reconnect without a transcript enabled live delivery")
			}
		})
	}
}

func scanChannelReceipt(path string, offset int64, marker string, discarding *bool) (int64, bool) {
	return scanReceipt(path, offset, marker, discarding, false, "channel:1")
}

func channelReceipt(line []byte, marker string) bool {
	return deliveryReceipt(line, marker, false, "channel:1")
}
