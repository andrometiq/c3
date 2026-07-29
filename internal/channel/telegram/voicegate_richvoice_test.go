package telegram

import (
	"encoding/json"
	"testing"

	"github.com/PaulSonOfLars/gotgbot/v2"
)

// Codex review 2, finding 2. A rich message renders its blocks IN ORDER and
// aggregates their attachments in that same order, so a photo block followed by
// a voice_note block produces attachments [photo, voice] — the voice is NOT at
// index 0. The broker used to look only at Attachments[0], so that message never
// entered the voice path: no fetch, no transcription, no refusal marker, no
// notice. The audio simply vanished.
//
// These tests establish, through the REAL decoder, the attachment shapes the
// broker must cope with. The matching broker-side regressions are
// TestFlushInbounds_VoiceNotFirstAttachment_* in internal/broker/voicegate_test.go.

// richVoiceMsg builds a Message carrying a real rich_message payload and returns
// it with the raw JSON, ready for convertInbound — the same pair poll.go hands
// to dispatchMessage.
func richVoiceMsg(blocksJSON string) (*gotgbot.Message, json.RawMessage) {
	msg := voiceMsg(7100, "uniq-rich")
	msg.Voice = nil // the rich payload owns the media, not the plain voice field
	return msg, json.RawMessage(`{"blocks":[` + blocksJSON + `]}`)
}

// The headline shape: photo first, voice second.
func TestConvertInbound_RichVoiceAfterPhoto_KeepsVoiceAttachment(t *testing.T) {
	msg, raw := richVoiceMsg(`
		{"type":"photo","photo":[{"file_id":"P1","file_size":10,"width":10,"height":10}]},
		{"type":"voice_note","voice_note":{"file_id":"V1","file_size":21226288,"mime_type":"audio/ogg"}}`)

	in := convertInbound("telegram", msg, "", raw)
	if in == nil {
		t.Fatal("convertInbound returned nil for a valid rich message")
	}
	if len(in.Attachments) != 2 {
		t.Fatalf("attachments = %+v, want [photo, voice]", in.Attachments)
	}
	if in.Attachments[0].Kind != "photo" {
		t.Fatalf("attachment 0 kind = %q, want photo — block order must be preserved", in.Attachments[0].Kind)
	}
	// This is the fact the broker regression depends on: a genuine voice
	// attachment can sit at a NON-ZERO index, carrying its own file_id and size.
	if in.Attachments[1].Kind != "voice" || in.Attachments[1].FileID != "V1" || in.Attachments[1].Size != 21226288 {
		t.Fatalf("attachment 1 = %+v, want the voice_note as kind=voice file_id=V1 size=21226288", in.Attachments[1])
	}
	if in.Text == "" {
		t.Fatal("a rich message must never surface empty; the block markers are the agent's only clue")
	}
}

// And more than one voice block in a single message is decodable, so "the voice
// attachment" is never a single thing either.
func TestConvertInbound_RichTwoVoiceBlocks_KeepsBoth(t *testing.T) {
	msg, raw := richVoiceMsg(`
		{"type":"voice_note","voice_note":{"file_id":"V1","file_size":100,"mime_type":"audio/ogg"}},
		{"type":"paragraph","text":"and another"},
		{"type":"voice_note","voice_note":{"file_id":"V2","file_size":200,"mime_type":"audio/ogg"}}`)

	in := convertInbound("telegram", msg, "", raw)
	if in == nil {
		t.Fatal("convertInbound returned nil for a valid rich message")
	}
	var voices []string
	for _, a := range in.Attachments {
		if a.Kind == "voice" {
			voices = append(voices, a.FileID)
		}
	}
	if len(voices) != 2 || voices[0] != "V1" || voices[1] != "V2" {
		t.Fatalf("voice attachments = %v, want both V1 and V2 in order — a message can carry several", voices)
	}
}
