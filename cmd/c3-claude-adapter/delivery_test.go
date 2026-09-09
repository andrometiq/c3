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

func TestNegotiatedHelloEligibility(t *testing.T) {
	isolateAdapterTest(t)
	// P1/P2: "channel OR inbox is eligible"; no inbox in this fixture.
	for _, state := range []string{ipc.RenderCapable, ipc.RenderProbing, ipc.RenderQueueOnly, ""} {
		t.Run(state, func(t *testing.T) {
			a, _, _ := liveFixture(t, state)
			a.initialRenderRoute = ipc.RenderRoute{State: state}
			a.deliveryHostInitialized.Store(true)
			offer := a.deliveryOffer()
			eligible := state == ipc.RenderCapable || state == ipc.RenderProbing
			if (len(offer) > 0) != eligible {
				t.Fatalf("offer=%s", offer)
			}
			a.liveTranscriptPath = func() string { return "" }
			if len(a.deliveryOffer()) > 0 {
				t.Fatal("offered unreadable transcript")
			}
		})
	}
}
func negotiatedAdapter(t *testing.T) (*adapter, *safeBuffer, <-chan []byte, context.Context) {
	a, output, frames := liveFixture(t, ipc.RenderCapable)
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
	a.deliveryHostInitialized.Store(true)
	a.acceptDelivery(a.deliveryOffer(), &ipc.DeliveryAcceptance{Version: 1, Modes: []string{"channel"}})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a.runCtx = ctx
	return a, output, frames, ctx
}
func TestNegotiatedDeliverReceiptShapes(t *testing.T) {
	isolateAdapterTest(t)
	// Live incident pin: "known intake shapes ... strict opening-tag parse".
	fixtures, err := os.ReadFile("testdata/claude-2.1.266-intake.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(fixtures)), "\n")
	shapes := map[string]string{"enqueue": lines[0], "attachment": lines[3], "user": `{"type":"user","message":{"role":"user","content":"<channel c3_attempt=\"channel:1\" c3_delivery_id=\"DELIVERYTOKEN-1\">hello</channel>"}}`}
	for name, line := range shapes {
		t.Run(name, func(t *testing.T) {
			a, output, frames, ctx := negotiatedAdapter(t)
			msg := ipc.DeliverMsg{Op: ipc.OpDeliver, Token: "DELIVERYTOKEN-1", Transport: "channel", DeadlineMS: 1000, Inbound: c3types.Inbound{Text: "hello"}}
			raw, _ := json.Marshal(msg)
			a.handleDeliver(ctx, raw)
			waitDeliveryWritten(t, a, msg.Token)
			if !strings.Contains(string(output.Bytes()), `"c3_delivery_id":"DELIVERYTOKEN-1"`) || !strings.Contains(string(output.Bytes()), `"c3_attempt":"channel:1"`) {
				t.Fatal("missing receipt metadata")
			}
			f, err := os.OpenFile(a.livePath(), os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.WriteString(line + "\n")
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			raw = nextLiveFrame(t, frames)
			var result ipc.AttemptResultMsg
			if json.Unmarshal(raw, &result) != nil || result.Op != ipc.OpAttemptResult || result.Outcome != "confirmed" || result.Token != msg.Token {
				t.Fatalf("result: %s", raw)
			}
			if a.liveRoute().State != "waiting" {
				t.Fatal("adapter promoted its own route")
			}
		})
	}
}
func TestNegotiatedNoFallbackRetry(t *testing.T) {
	isolateAdapterTest(t)
	// P9: "never retry, never fall back, never change its own route state".
	a, output, frames, ctx := negotiatedAdapter(t)
	raw, _ := json.Marshal(ipc.DeliverMsg{Op: ipc.OpDeliver, Token: "expires", Transport: "channel", DeadlineMS: 50, Inbound: c3types.Inbound{Text: "hello"}})
	a.handleDeliver(ctx, raw)
	waitDeliveryWritten(t, a, "expires")
	before := len(output.Bytes())
	a.resetLiveRoute(true)
	time.Sleep(250 * time.Millisecond)
	if len(output.Bytes()) != before || a.liveRoute().State != "waiting" {
		t.Fatal("adapter retried or changed policy")
	}
	noLiveFrame(t, frames)
}
func TestNegotiatedDefinitiveFailure(t *testing.T) {
	isolateAdapterTest(t)
	a, _, frames, ctx := negotiatedAdapter(t)
	a.notifyTx = nil
	raw, _ := json.Marshal(ipc.DeliverMsg{Op: ipc.OpDeliver, Token: "failed", Transport: "channel", DeadlineMS: 1000, Inbound: c3types.Inbound{Text: "hello"}})
	a.handleDeliver(ctx, raw)
	var msg ipc.AttemptResultMsg
	raw = nextLiveFrame(t, frames)
	if json.Unmarshal(raw, &msg) != nil || msg.Outcome != "failed" || msg.Reason != "notify transport unavailable" {
		t.Fatalf("failure=%s", raw)
	}
}
func TestNegotiatedAcceptanceAbsentKeepsLegacy(t *testing.T) {
	isolateAdapterTest(t)
	a, _, _ := liveFixture(t, ipc.RenderCapable)
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
	a.deliveryHostInitialized.Store(true)
	a.acceptDelivery(a.deliveryOffer(), nil)
	if a.deliveryAccepted.Load() {
		t.Fatal("accepted without broker agreement")
	}
}

func TestNegotiatedDocsContract(t *testing.T) {
	isolateAdapterTest(t)
	// P9: "phase 2 ... P8 per-route display for negotiated sessions; legacy sessions untouched".
	for path, claims := range map[string][]string{
		"../../docs/ADAPTERS.md":  {"Provisional-negotiated", "no `fetch_receipt` mode is accepted", "lease` is refused on presence only for negotiated", "Legacy sessions retain", "c3_attempt=\"inbox:N\"", "validateCrossSessionPeer", "peer intake is VERIFIED", "channel OR inbox is eligible", "at least one mode it offered", "once per 10 seconds", "notifications/initialized` received AND notify transport present", "startup hello carries no offer"},
		"../../docs/DEBUGGING.md": {"attempt retirement released: storage retry limit reached", "attempt shadow suite divergences=0", "no phase-5 flap timer", "attempt reserved transport=inbox", "attempt finished transport=inbox outcome=confirmed", "Definite notify failures immediately send"},
		"../../DECISIONS.md":      {"D034: Negotiated channel delivery (phase 2)", "no goroutine per attempt", "D035: Inbox as a broker-owned transport (phase 3)", "supersedes the delivery parts of D031"},
	} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, claim := range claims {
			if !strings.Contains(string(body), claim) {
				t.Errorf("%s missing %q", path, claim)
			}
		}
	}
}
func TestNegotiatedDeliveryReportSemanticChange(t *testing.T) {
	isolateAdapterTest(t)
	a, _, frames, _ := negotiatedAdapter(t)
	a.pollDeliveries()
	select {
	case raw := <-frames:
		t.Fatalf("unchanged report: %s", raw)
	default:
	}
	a.liveTranscriptPath = func() string { return "" }
	a.pollDeliveries()
	raw := nextLiveFrame(t, frames)
	var report ipc.DeliveryReportMsg
	if json.Unmarshal(raw, &report) != nil || report.Op != ipc.OpDeliveryReport || report.Live.Channel.Eligible {
		t.Fatalf("report=%s", raw)
	}
	a.pollDeliveries()
	select {
	case raw := <-frames:
		t.Fatalf("repeated semantic report: %s", raw)
	default:
	}
}
func TestNegotiatedRouteDisplayUsesOutput(t *testing.T) {
	isolateAdapterTest(t)
	a, _, _, _ := negotiatedAdapter(t)
	topic := int64(7)
	ref := ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &topic}
	a.setRouteState([]ipc.RouteRef{ref}, &ref)
	raw, _ := json.Marshal(ipc.RenderStateMsg{Op: ipc.OpRenderState, Channel: "telegram", ChatID: -100, TopicID: &topic, RenderRoute: ipc.RenderRoute{State: "live_channel", Confirmed: time.Now()}})
	a.handleDeliveryState(raw)
	raw, _ = json.Marshal(ipc.RenderStateMsg{Op: ipc.OpRenderState, Channel: "other", ChatID: -200, RenderRoute: ipc.RenderRoute{State: "pull_only", Reason: "unavailable"}})
	a.handleDeliveryState(raw)
	if got := a.liveRoutePreamble(); !strings.Contains(got, "live: channel, confirmed") {
		t.Fatal(got)
	}
}

func TestNegotiatedHostDetectionFactChanges(t *testing.T) {
	isolateAdapterTest(t)
	// P9: "Send delivery_report when host detection facts change semantically".
	a, _, frames, _ := negotiatedAdapter(t)
	a.deliveryHostRoute = func() ipc.RenderRoute {
		return ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "host no longer detected"}
	}
	a.pollDeliveries()
	raw := nextLiveFrame(t, frames)
	var report ipc.DeliveryReportMsg
	json.Unmarshal(raw, &report)
	if report.Op != ipc.OpDeliveryReport || report.Live.Channel.Eligible || report.Live.Channel.Reason != "host no longer detected" {
		t.Fatalf("host change: %s", raw)
	}
	a.pollDeliveries()
	select {
	case raw := <-frames:
		t.Fatalf("repeated report: %s", raw)
	default:
	}
}

func waitDeliveryWritten(t *testing.T, a *adapter, token string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		a.liveMu.Lock()
		o := a.deliveryObservers[token]
		written := o != nil && o.written
		a.liveMu.Unlock()
		if written {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("delivery writer did not complete")
}

func TestNegotiatedAcceptedModeSubset(t *testing.T) {
	isolateAdapterTest(t)
	// Maintainer ruling (1): an ack with no offered mode means legacy.
	for _, offered := range []string{"channel", "inbox", "both"} {
		for _, modes := range [][]string{{"channel"}, {"inbox"}, {"channel", "inbox"}, {"channel", "future"}, {"inbox", "future"}, {"future"}, nil} {
			a, _, _ := liveFixture(t, ipc.RenderCapable)
			live := ipc.DeliveryLive{Channel: ipc.DeliveryEligibility{Eligible: offered != "inbox"}, Inbox: ipc.DeliveryEligibility{Eligible: offered != "channel"}}
			offer := deliveryOfferFor(live)
			has := func(mode string) bool {
				for _, m := range modes {
					if m == mode {
						return true
					}
				}
				return false
			}
			want := (live.Channel.Eligible && has("channel")) || (live.Inbox.Eligible && has("inbox"))
			for _, version := range []int{1, 2} {
				a.acceptDelivery(offer, &ipc.DeliveryAcceptance{Version: version, Modes: modes})
				enabled := want && version == 1
				if a.deliveryAccepted.Load() != enabled {
					t.Fatalf("offered=%s modes=%v version=%d", offered, modes, version)
				}
				a.liveMu.Lock()
				channel, inbox := a.deliveryChannelAccepted, a.deliveryInboxAccepted
				a.liveMu.Unlock()
				if channel != (enabled && has("channel")) || inbox != (enabled && has("inbox")) {
					t.Fatal("unknown or unaccepted mode enabled")
				}
			}
		}
	}
}

func TestNegotiatedInboxAttachAndPreamble(t *testing.T) {
	isolateAdapterTest(t)
	// P8: broker-derived inbox state is used in both attach text and preamble.
	a, _, frames, ctx := negotiatedAdapter(t)
	results := make(chan *mcp.CallToolResult, 1)
	go func() {
		r, _ := a.toolAttach(ctx, &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Arguments: json.RawMessage(`{"name":"example"}`)}})
		results <- r
	}()
	raw := nextLiveFrame(t, frames)
	if op, _ := ipc.PeekOp(raw); op != ipc.OpAttach {
		t.Fatal(string(raw))
	}
	raw, _ = json.Marshal(ipc.AttachedMsg{Op: ipc.OpAttached, OK: true, Channel: "telegram", ChatID: -100, Name: "example", DeliveryRoute: &ipc.RenderRoute{State: "live_inbox", Confirmed: time.Now(), Transport: "inbox"}})
	a.dispatchAttached(raw)
	select {
	case r := <-results:
		if r == nil || len(r.Content) == 0 || !strings.Contains(r.Content[0].(*mcp.TextContent).Text, "live: inbox, confirmed") {
			t.Fatal(r)
		}
	case <-time.After(time.Second):
		t.Fatal("attach did not complete")
	}
	if !strings.Contains(a.buildInstructions(), "live: inbox, confirmed") {
		t.Fatal("preamble omitted inbox")
	}
}
