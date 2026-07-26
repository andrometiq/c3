package telegram

import (
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// TestCapabilities_GoldenManifest pins the Telegram channel's v1 capability
// manifest so an accidental future change to the static literal is caught. The
// manifest is the contract the agent surface (capability.GuidanceFor) and the
// broker gate read from; a silent drift here would silently mis-advertise what
// the channel can do. Spec 2026-06-14-channel-capability-architecture §P4
// ("a per-channel golden manifest test").
func TestCapabilities_DeliversRichMessages(t *testing.T) {
	caps := (&Channel{}).Capabilities()
	if !caps.Inbound.DeliversRichMessages {
		t.Error("telegram should advertise DeliversRichMessages=true")
	}
}

func TestCapabilities_GoldenManifest(t *testing.T) {
	caps := New().Capabilities()

	if caps.Channel != Name {
		t.Errorf("Channel = %q; want %q", caps.Channel, Name)
	}
	if !caps.RichText {
		t.Error("RichText = false; want true")
	}
	if caps.Albums {
		t.Error("Albums = true; want false (v1 descopes albums to sequential sends)")
	}
	if !caps.Polls {
		t.Error("Polls = false; want true")
	}
	if !caps.Typing {
		t.Error("Typing = false; want true")
	}
	if caps.Stream.StreamViaEdit {
		t.Error("Stream.StreamViaEdit = true; want false (streaming deferred in v1)")
	}
	if caps.MaxMessageRunes != 4096 {
		t.Errorf("MaxMessageRunes = %d; want 4096", caps.MaxMessageRunes)
	}
	if want := int64(50 * 1024 * 1024); caps.MaxSendBytes != want {
		t.Errorf("MaxSendBytes = %d; want %d (50 MiB)", caps.MaxSendBytes, want)
	}

	// Spot-check the remaining load-bearing v1 values so the golden test is a
	// meaningful drift trap, not just the headline flags.
	if caps.MaxCaptionRunes != 1024 {
		t.Errorf("MaxCaptionRunes = %d; want 1024", caps.MaxCaptionRunes)
	}
	if !caps.CompressedPhoto {
		t.Error("CompressedPhoto = false; want true")
	}
	if !caps.OriginalFile {
		t.Error("OriginalFile = false; want true")
	}
	if !caps.Reactions || !caps.ReactionsSingle {
		t.Errorf("Reactions=%v ReactionsSingle=%v; want both true", caps.Reactions, caps.ReactionsSingle)
	}
	if !caps.EditMessages {
		t.Error("EditMessages = false; want true")
	}
	if !caps.Threads {
		t.Error("Threads = false; want true")
	}
	if !caps.InlineKeyboards {
		t.Error("InlineKeyboards = false; want true (P7 outbound inline keyboards)")
	}
	// Native rich messages: the channel CAN send them (raw sendRichMessage), so
	// RichMessages is true; RichTables is now ON (richTablesEnabled=true,
	// commit 0c13abf) after the GFM-table → native RichBlockTable render was
	// live-verified on Android 2026-06-17.
	if !caps.RichMessages {
		t.Error("RichMessages = false; want true (sendRichMessage via raw request)")
	}
	if !caps.RichTables {
		t.Error("RichTables = false; want true (live-verified ON 2026-06-17)")
	}
	if want := int64(20 * 1024 * 1024); caps.Inbound.MaxDownloadBytes != want {
		t.Errorf("Inbound.MaxDownloadBytes = %d; want %d (20 MiB)", caps.Inbound.MaxDownloadBytes, want)
	}
	if !caps.Inbound.DeliversRichMessages {
		t.Error("Inbound.DeliversRichMessages = false; want true (Bot API 10.1 rich-message inbound decode)")
	}

	wantKinds := []c3types.MediaKind{
		c3types.MediaPhoto, c3types.MediaFile, c3types.MediaVideo,
		c3types.MediaAudio, c3types.MediaVoice, c3types.MediaAnimation,
	}
	if len(caps.MediaKinds) != len(wantKinds) {
		t.Fatalf("MediaKinds = %v; want %v", caps.MediaKinds, wantKinds)
	}
	for i, k := range wantKinds {
		if caps.MediaKinds[i] != k {
			t.Errorf("MediaKinds[%d] = %q; want %q", i, caps.MediaKinds[i], k)
		}
	}
}
