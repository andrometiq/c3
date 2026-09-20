package intake

import "testing"

func TestTerminalOutcomeRequiresEveryMatchingVoiceIndex(t *testing.T) {
	source := &Source{Attachments: []SourceAttachment{
		{Kind: "voice", FileID: "audio-a"}, {Kind: "photo", FileID: "audio-a"},
		{Kind: "voice", FileID: "audio-a"}, {Kind: "voice"},
	}}
	states := NewAttachmentsState(source, []string{"audio-a"})
	done := STTOutcome{STT: STTDone, Transcript: "raw words"}
	states[0] = AttachmentState{Index: 0, STT: done.STT, Transcript: done.Transcript}
	if _, terminal := states.TerminalOutcome(source, "audio-a"); terminal {
		t.Fatal("one terminal duplicate must not hide a pending index")
	}
	states = states.WithOutcome(source, "audio-a", done)
	if got, terminal := states.TerminalOutcome(source, "audio-a"); !terminal || got != done {
		t.Fatalf("terminal=%v outcome=%+v", terminal, got)
	}
	failed := STTOutcome{STT: STTFailed, Error: "transcript_too_large"}
	states = states.WithOutcome(source, "audio-a", failed)
	if got, terminal := states.TerminalOutcome(source, "audio-a"); !terminal || got != failed {
		t.Fatalf("terminal=%v outcome=%+v", terminal, got)
	}
	if states[1].STT != STTNotApplicable || states[3].Error != "missing_file_id" {
		t.Fatal("non-STT or missing-file state changed")
	}
	for _, id := range []string{"", "absent"} {
		if _, terminal := states.TerminalOutcome(source, id); terminal {
			t.Fatal("absence is not a durable terminal marker")
		}
	}
	states[2].Error = "different outcome"
	if _, terminal := states.TerminalOutcome(source, "audio-a"); terminal {
		t.Fatal("inconsistent duplicate indices were accepted")
	}
}
