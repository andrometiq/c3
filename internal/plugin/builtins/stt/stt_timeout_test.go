package stt

import (
	"testing"
	"time"
)

// P1-3: the STT subprocess budget scales with stated audio size, floors at the
// base, and caps at sttMaxTimeoutSeconds — so a long voice note gets download +
// provider-attempt room instead of a fixed 300s window.
func TestSTTTimeoutForSize(t *testing.T) {
	const base = 300
	cases := []struct {
		name string
		size int64
		want time.Duration
	}{
		{"unknown size falls back to base", 0, 300 * time.Second},
		{"small under a MiB stays base", 500 << 10, 300 * time.Second},
		{"18 MiB scales up", 18 << 20, time.Duration(300+18*sttPerMiBSeconds) * time.Second},
		{"pathological size caps", 500 << 20, sttMaxTimeoutSeconds * time.Second},
	}
	for _, tc := range cases {
		if got := sttTimeoutForSize(base, tc.size); got != tc.want {
			t.Errorf("%s: sttTimeoutForSize(%d, %d) = %v, want %v", tc.name, base, tc.size, got, tc.want)
		}
	}
	// A configured base above the cap is CLAMPED — the broker's outer STT context
	// must always sit above the plugin budget, or it SIGKILLs the tree mid-provider
	// (Codex review 1, F7).
	if got := sttTimeoutForSize(sttMaxTimeoutSeconds+120, 0); got != sttMaxTimeoutSeconds*time.Second {
		t.Errorf("base above cap must be clamped to the cap: got %v", got)
	}
}
