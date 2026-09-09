package main

import (
	"github.com/Andrometiq/c3/internal/c3types"
	"testing"
)

func TestInjectedChannelMetadata(t *testing.T) {
	isolateAdapterTest(t)
	for _, synthetic := range []bool{false, true} {
		frame := buildClaudeChannelFrame(&c3types.Inbound{TestInjected: synthetic, Text: "example"})
		meta := frame["meta"].(map[string]any)
		if got := meta["c3_test_injected"]; (synthetic && got != "true") || (!synthetic && got != nil) {
			t.Fatalf("synthetic=%v meta=%v", synthetic, meta)
		}
	}
}
