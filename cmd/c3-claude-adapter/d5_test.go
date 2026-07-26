package main

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// TestAdviseBrokerDown_FrameRenders proves the D5 advisory actually renders as a
// visible Claude channel frame — it reuses the broker's InboundSystem shape, so
// buildClaudeChannelFrame's System case must produce non-empty content.
func TestAdviseBrokerDown_FrameRenders(t *testing.T) {
	in := &c3types.Inbound{
		Channel: "c3",
		Kind:    c3types.InboundSystem,
		Event: &c3types.InboundEvent{System: &c3types.SystemEvent{
			Source: "c3", Level: "warn", Title: "C3 broker unreachable", Message: "inbound is down",
		}},
	}
	frame := buildClaudeChannelFrame(in)
	content, ok := frame["content"].(string)
	if !ok || strings.TrimSpace(content) == "" {
		t.Fatalf("system-event frame content = %v; want a non-empty string so the advisory is visible", frame["content"])
	}
}

// TestAdviseBrokerDown_OneShotAndReArm covers the guard: it fires once per
// outage and re-arms on clear. notifyTx is nil here — the CAS fires before the
// nil-check, so the one-shot/re-arm logic is exercised without a transport double.
func TestAdviseBrokerDown_OneShotAndReArm(t *testing.T) {
	a := newAdapter()
	a.adviseBrokerDown(6)
	if !a.brokerDownAdvised.Load() {
		t.Fatal("first adviseBrokerDown must set brokerDownAdvised")
	}
	a.adviseBrokerDown(7) // second call: CAS fails → no-op
	if !a.brokerDownAdvised.Load() {
		t.Fatal("brokerDownAdvised should remain set after a repeat call")
	}
	a.clearBrokerDownAdvisory()
	if a.brokerDownAdvised.Load() {
		t.Fatal("clearBrokerDownAdvisory must clear (re-arm) the flag")
	}
}
