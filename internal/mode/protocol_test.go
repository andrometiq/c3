package mode

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// testCaps is a representative manifest passed to Combined() in these tests —
// mirrors the Telegram v1 literal closely enough to exercise GuidanceFor's
// positive branches. The exact values don't matter to the mode-protocol
// assertions (those guard the protocol prose); the per-channel golden manifest
// test pins the real Telegram values.
var testCaps = c3types.Capabilities{
	Channel:         "telegram",
	RichText:        true,
	MaxMessageRunes: 4096,
	MediaKinds:      []c3types.MediaKind{c3types.MediaPhoto, c3types.MediaFile},
	CompressedPhoto: true,
	OriginalFile:    true,
	MaxSendBytes:    50 * 1024 * 1024,
	Polls:           true,
	Typing:          true,
}

// TestModeProtocol_HasCanonicalKeyPhrases guards against accidental drift
// of the user-facing protocol strings. We can't pin the whole text (it'll
// keep getting wordsmithed) but the load-bearing phrases must survive.
func TestModeProtocol_HasCanonicalKeyPhrases(t *testing.T) {
	for _, want := range []string{
		"OUTPUT MODE PROTOCOL",
		"CLI mode (DEFAULT)",
		"Telegram mode",
		"reply", // the tool name we tell agents NOT to call by default
		"switch to Telegram",
		"switch to CLI",
	} {
		if !strings.Contains(ModeProtocol, want) {
			t.Errorf("ModeProtocol missing %q:\n%s", want, ModeProtocol)
		}
	}
}

// TestModeProtocol_HasAnnounceModeAfterAttach guards the rule that the
// agent must announce its current output mode immediately after every
// successful attach. TODO #15 (maintainer 2026-05-18): broker does not
// persist mode, so the protocol mandates an explicit confirmation so
// the human knows which surface owns the next reply.
func TestModeProtocol_HasAnnounceModeAfterAttach(t *testing.T) {
	for _, want := range []string{
		"After attach",
		"currently in CLI mode",
		"currently in Telegram mode",
	} {
		if !strings.Contains(ModeProtocol, want) {
			t.Errorf("ModeProtocol missing %q:\n%s", want, ModeProtocol)
		}
	}
}

// TestMultipartProtocol_HasCanonicalKeyPhrases — same shape as above.
func TestMultipartProtocol_HasCanonicalKeyPhrases(t *testing.T) {
	for _, want := range []string{
		"MULTI-PART REPLY PROTOCOL",
		"start multi-part reply",
		"end of multi-part reply",
		"Waiting.",
	} {
		if !strings.Contains(MultipartProtocol, want) {
			t.Errorf("MultipartProtocol missing %q:\n%s", want, MultipartProtocol)
		}
	}
}

// TestCombined_PreservesLeadingNewlines locks in the wire shape every
// existing adapter relies on (their previous modeProtocol consts opened
// with "\n\n" — head text concatenated directly).
func TestCombined_PreservesLeadingNewlines(t *testing.T) {
	got := Combined(testCaps)
	if !strings.HasPrefix(got, "\n\n") {
		t.Errorf("Combined() must start with \\n\\n so it splices onto head text; got %q", got[:min(10, len(got))])
	}
}

// TestCombined_ContainsBothProtocols — the whole point of Combined() is
// that callers don't have to remember to concatenate both protocols
// themselves.
func TestCombined_ContainsBothProtocols(t *testing.T) {
	got := Combined(testCaps)
	if !strings.Contains(got, ModeProtocol) {
		t.Error("Combined() missing ModeProtocol body")
	}
	if !strings.Contains(got, MultipartProtocol) {
		t.Error("Combined() missing MultipartProtocol body")
	}
}

// TestCombined_FoldsInCapabilityGuidance is the P4 contract: the channel
// capability guidance (capability.GuidanceFor) must be spliced into the
// combined agent surface so the agent learns what the channel can render in
// the SAME init/setup delivery as the mode/multipart protocols.
func TestCombined_FoldsInCapabilityGuidance(t *testing.T) {
	got := Combined(testCaps)
	for _, want := range []string{
		"CHANNEL CAPABILITIES",
		"Typing: shown automatically",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Combined() missing capability-guidance phrase %q:\n%s", want, got)
		}
	}
}

// TestCombined_CapabilityGuidanceOrdering locks in the 2026-06-20 reorder:
// ModeProtocol (the safety-critical no-auto-reply/no-auto-switch contract) stays
// FIRST, the capability/formatting guidance moves to the MIDDLE (so it is no
// longer the forgotten tail), and the narrow MultipartProtocol voice convention
// moves LAST. Order: Mode → CHANNEL CAPABILITIES → Multipart.
func TestCombined_CapabilityGuidanceOrdering(t *testing.T) {
	got := Combined(testCaps)
	idxMode := strings.Index(got, "OUTPUT MODE PROTOCOL")
	idxCaps := strings.Index(got, "CHANNEL CAPABILITIES")
	idxMulti := strings.Index(got, "MULTI-PART REPLY PROTOCOL")
	if idxMode < 0 || idxCaps < 0 || idxMulti < 0 {
		t.Fatalf("a required section is missing: mode=%d caps=%d multi=%d", idxMode, idxCaps, idxMulti)
	}
	if !(idxMode < idxCaps && idxCaps < idxMulti) {
		t.Errorf("Combined() order must be Mode < Capabilities < Multipart; got mode=%d caps=%d multi=%d",
			idxMode, idxCaps, idxMulti)
	}
}

// TestCombined_ProtocolsSeparated guards against the two protocols being
// fused with no blank line between them (which would make the rendered
// text harder to read for the agent and for humans inspecting the MCP
// initialize response during debugging).
func TestCombined_ProtocolsSeparated(t *testing.T) {
	got := Combined(testCaps)
	// Find where ModeProtocol ends and MultipartProtocol begins, assert
	// there's at least one blank line of separation.
	idx := strings.Index(got, MultipartProtocol)
	if idx < 0 {
		t.Fatal("MultipartProtocol not found in Combined()")
	}
	// Everything before idx should end with at least "\n\n".
	prefix := got[:idx]
	if !strings.HasSuffix(prefix, "\n\n") {
		t.Errorf("ModeProtocol → MultipartProtocol transition missing blank-line separator; tail = %q",
			prefix[max(0, len(prefix)-10):])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
