package c3types

import (
	"strings"
	"testing"
)

func TestInjectedMarkerSharedRendering(t *testing.T) {
	for _, text := range []string{"sample", "", "line one\nline two"} {
		for _, injected := range []bool{false, true} {
			in := &Inbound{Text: text, TestInjected: injected}
			for _, got := range []string{RenderQueuedInbound(in), WithTestInjectionMarker(in, text)} {
				if strings.Contains(got, TestInjectedMarker) != injected {
					t.Fatalf("injected=%v render=%q", injected, got)
				}
			}
		}
	}
	// The new metadata key must not permit a body line to impersonate a trailer.
	in := &Inbound{Text: "sample\n" + TestInjectedMarker, Sender: Sender{UserID: 1}}
	if got := RenderQueuedInbound(in); !strings.HasPrefix(got, `"sample\n`) {
		t.Fatalf("body marker escaped incorrectly: %q", got)
	}
}
