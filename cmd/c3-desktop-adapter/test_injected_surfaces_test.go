package main

import (
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"strings"
	"testing"
)

func TestInjectedMarkerSingleRouteFetchAndPreview(t *testing.T) {
	a := newAdapter()
	topic := int64(42)
	route := ipc.RouteRef{Channel: "test-inject", ChatID: -1, TopicID: &topic}
	a.setRouteState([]ipc.RouteRef{route}, &route)
	for _, synthetic := range []bool{false, true} {
		in := c3types.Inbound{Channel: route.Channel, ChatID: route.ChatID, TopicID: &topic, Text: "sample", TestInjected: synthetic}
		if got := a.originTag(&in); got != "" {
			t.Fatalf("expected single-route elision, got %q", got)
		}
		for _, got := range []string{
			renderQueuedInbound(&in),
			renderFetchedMessages([]c3types.Inbound{in}, 0, ""),
			a.renderFetchedMessages([]c3types.Inbound{in}, 0, ""),
			renderBacklogSummary(1, []ipc.QueuedItem{{Preview: c3types.WithTestInjectionMarker(&in, "sample")}}, ""),
		} {
			if strings.Contains(got, c3types.TestInjectedMarker) != synthetic {
				t.Fatalf("synthetic=%v render=%q", synthetic, got)
			}
		}
	}
}
