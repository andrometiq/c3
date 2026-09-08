package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// This sanitized record was emitted by Claude Code 2.1.263, independently of
// our frame builder. Insert metadata using the host's observed key="value" form.
func realHostReceipt(t *testing.T, marker string) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/claude-2.1.263-channel.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	message := record["message"].(map[string]any)
	message["content"] = strings.Replace(message["content"].(string), "<channel ", `<channel c3_delivery_id="`+marker+`" `, 1)
	raw, err = json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

func TestRealHostChannelReceipt(t *testing.T) {
	raw := realHostReceipt(t, "real-host-marker")
	if !channelReceipt(raw, "real-host-marker") {
		t.Fatal("real host receipt rejected")
	}
	if channelReceipt(raw, "different-marker") {
		t.Fatal("different delivery accepted")
	}
	original, err := os.ReadFile("testdata/claude-2.1.263-channel.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if channelReceipt(original, "real-host-marker") {
		t.Fatal("record without marker accepted")
	}
}

func TestChannelReceiptAttributeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, text, marker string
		want               bool
	}{
		{"double", `<channel c3_delivery_id="T">body</channel>`, "T", true},
		{"single", `<channel c3_delivery_id='T'>body</channel>`, "T", true},
		{"whitespace", "<channel\tc3_delivery_id \n= 'T' >body</channel>", "T", true},
		{"self closing", `<channel c3_delivery_id='T'/>`, "T", true},
		{"decoded", `<channel c3_delivery_id='T&amp;&#34;'>body</channel>`, `T&"`, true},
		{"embedded", `<channel note=' c3_delivery_id="T"'>body</channel>`, "T", false},
		{"embedded greater than", `<channel note=' > c3_delivery_id="T"'>body</channel>`, "T", false},
		{"duplicate same", `<channel c3_delivery_id='T' c3_delivery_id='T'>body</channel>`, "T", false},
		{"duplicate different", `<channel c3_delivery_id='T' c3_delivery_id='U'>body</channel>`, "T", false},
		{"duplicate other", `<channel note='x' note='y' c3_delivery_id='T'>body</channel>`, "T", false},
		{"unterminated", `<channel c3_delivery_id='T>body</channel>`, "T", false},
		{"unquoted", `<channel c3_delivery_id=T>body</channel>`, "T", false},
		{"missing attribute separator", `<channel c3_delivery_id='T'note='x'>body</channel>`, "T", false},
		{"missing equals", `<channel c3_delivery_id 'T'>body</channel>`, "T", false},
		{"invalid entity", `<channel c3_delivery_id='T&bad;'>body</channel>`, "T", false},
		{"body", `<channel source='c3'>c3_delivery_id="T"</channel>`, "T", false},
		{"nested in body", `<channel source='c3'><channel c3_delivery_id='T'>body</channel></channel>`, "T", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(map[string]any{"type": "user", "message": map[string]any{"role": "user", "content": tc.text}})
			if got := channelReceipt(raw, tc.marker); got != tc.want {
				t.Fatalf("match = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReceiptScanDiscardsOversizeAcrossPolls(t *testing.T) {
	path := t.TempDir() + "/transcript.jsonl"
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	// A plausible receipt inside an oversized line must never be acknowledged.
	if _, err := f.WriteString(strings.Repeat(" ", 65<<20)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(realHostReceipt(t, "oversized")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(realHostReceipt(t, "valid")); err != nil {
		t.Fatal(err)
	}
	f.Close()
	for _, marker := range []string{"oversized", "valid"} {
		offset, discarding, found := int64(0), false, false
		for i := 0; i < 4; i++ {
			next, match := scanChannelReceipt(path, offset, marker, &discarding)
			if i < 2 && (next <= offset || !discarding || match) {
				t.Fatalf("poll %d did not advance discard: %d %v %v", i, next, discarding, match)
			}
			offset = next
			if match {
				found = true
				break
			}
		}
		if found != (marker == "valid") {
			t.Fatalf("%s found=%v", marker, found)
		}
	}
}

func TestLiveReceiptSurvivesAttach(t *testing.T) {
	for _, success := range []bool{true, false} {
		name := "failed"
		if success {
			name = "same route"
		}
		t.Run(name, func(t *testing.T) {
			a, _, frames := liveFixture(t, ipc.RenderCapable)
			a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
			a.liveTimeout = time.Second
			route := ipc.RouteRef{Channel: "telegram", ChatID: -100}
			a.setRouteState([]ipc.RouteRef{route}, &route)
			pushLive(t, a, "reattach")
			raw, _ := json.Marshal(ipc.AttachedMsg{Op: ipc.OpAttached, OK: success, Routes: []ipc.RouteRef{route}, Output: &route})
			a.dispatchAttached(raw)
			if success {
				nextLiveFrame(t, frames)
			} // successful retry publishes eligibility
			f, err := os.OpenFile(a.livePath(), os.O_WRONLY|os.O_APPEND, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.Write(realHostReceipt(t, "reattach"))
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			raw = nextLiveFrame(t, frames)
			var ack ipc.InboundDeliveredMsg
			if json.Unmarshal(raw, &ack) != nil || ack.Op != ipc.OpInboundDelivered || ack.DeliveryToken != "reattach" {
				t.Fatalf("receipt lost: %s", raw)
			}
			noLiveFrame(t, frames)
		})
	}
}

type blockedNotifyWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w *blockedNotifyWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return len(p), nil
}
func (*blockedNotifyWriter) Close() error { return nil }

func TestBlockedNotifyDoesNotBlockReceiptsOrTimeout(t *testing.T) {
	a, _, frames := liveFixture(t, ipc.RenderCapable)
	a.liveTimeout = time.Second
	pushLive(t, a, "earlier")
	writer := &blockedNotifyWriter{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(writer.release)
	a.notifyTx = newNotifyTransport(&mcp.IOTransport{Reader: nopCloseReader{strings.NewReader("")}, Writer: writer})
	if _, err := a.notifyTx.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { pushLive(t, a, "blocked"); close(done) }()
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("notify did not start")
	}
	appendChannelReceipt(t, a.livePath(), "earlier")
	raw := nextLiveFrame(t, frames)
	var ack ipc.InboundDeliveredMsg
	if json.Unmarshal(raw, &ack) != nil || ack.DeliveryToken != "earlier" {
		t.Fatalf("other watcher blocked: %s", raw)
	}
	raw = nextLiveFrame(t, frames)
	var state ipc.RenderStateMsg
	if json.Unmarshal(raw, &state) != nil || state.State != ipc.RenderQueueOnly {
		t.Fatalf("timeout blocked: %s", raw)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notification caller did not cancel")
	}
}

func TestLiveReceiptCancelsOnRouteOrSessionChange(t *testing.T) {
	for _, change := range []string{"route", "detach", "session"} {
		t.Run(change, func(t *testing.T) {
			a, _, frames := liveFixture(t, ipc.RenderCapable)
			a.liveTimeout = time.Second
			route := ipc.RouteRef{Channel: "telegram", ChatID: -100}
			a.setRouteState([]ipc.RouteRef{route}, &route)
			pushLive(t, a, "old-route")
			switch change {
			case "route":
				route.ChatID = -200
				a.setRouteState([]ipc.RouteRef{route}, &route)
			case "detach":
				a.amu.Lock()
				a.clearRouteStateLocked()
				a.amu.Unlock()
			case "session":
				a.setCurrentStableIdentity(sessionhandoff.Entry{StableSessionID: "new-session"})
			}
			appendChannelReceipt(t, a.livePath(), "old-route")
			noLiveFrame(t, frames)
		})
	}
}
