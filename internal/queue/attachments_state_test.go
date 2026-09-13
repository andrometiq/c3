package queue

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/Andrometiq/c3/internal/intake"
)

func TestAttachmentStateAtomicTerminalPersistenceAndRestart(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "example", ChatID: -100}
	source := testSource(1001)
	source.Attachments = []intake.SourceAttachment{
		{Kind: "voice", FileID: "audio-a"}, {Kind: "photo", FileID: "photo"},
		{Kind: "voice", FileID: "audio-b"}, {Kind: "voice", FileID: "audio-a"}, {Kind: "voice"},
	}
	id, err := s.AppendTrackedSource(rk, msg(9, "pending"), source, "audio-a", "audio-b")
	if err != nil {
		t.Fatal(err)
	}
	raw := "  literal raw\nwords & <tags>  "
	done := intake.STTOutcome{STT: intake.STTDone, Transcript: raw}
	s.rewriteTestHook = func() error { return errors.New("injected before rewrite") }
	if _, _, err := s.ResolveVoiceOutcome(rk, id, "audio-a", "[Transcribed voice]: "+raw, done, nil); err == nil {
		t.Fatal("rewrite failure hidden")
	}
	s = restartSourceStore(t, s)
	rows, err := s.PeekTracked(rk, -1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if rows[0].Inbound.Text != "pending" || len(rows[0].VoicePending) != 2 || rows[0].AttachmentsState[0].STT != intake.STTPending {
		t.Fatalf("partial mutation: %+v", rows[0])
	}
	ok, allDone, err := s.ResolveVoiceOutcome(rk, id, "audio-a", "[Transcribed voice]: "+raw, done, nil)
	if err != nil || !ok || allDone {
		t.Fatalf("resolve=%v/%v/%v", ok, allDone, err)
	}
	s = restartSourceStore(t, s)
	rows, err = s.PeekTracked(rk, -1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	want := intake.AttachmentsState{
		{Index: 0, STT: intake.STTDone, Transcript: raw}, {Index: 1, STT: intake.STTNotApplicable},
		{Index: 2, STT: intake.STTPending}, {Index: 3, STT: intake.STTDone, Transcript: raw},
		{Index: 4, STT: intake.STTFailed, Error: "missing_file_id"},
	}
	if !reflect.DeepEqual(rows[0].AttachmentsState, want) || rows[0].Inbound.Text != "[Transcribed voice]: "+raw || !reflect.DeepEqual(rows[0].VoicePending, []string{"audio-b"}) {
		t.Fatalf("terminal row=%+v", rows[0])
	}
	rows[0].AttachmentsState[0].Transcript = "projection mutation"
	failed := intake.STTOutcome{STT: intake.STTFailed, Error: "provider_unavailable"}
	ok, allDone, err = s.ResolveVoiceOutcome(rk, id, "audio-b", "failure presentation", failed, nil)
	if err != nil || !ok || !allDone {
		t.Fatalf("resolve=%v/%v/%v", ok, allDone, err)
	}
	want[2] = intake.AttachmentState{Index: 2, STT: intake.STTFailed, Error: "provider_unavailable"}
	s = restartSourceStore(t, s)
	rows, err = s.PeekTracked(rk, -1)
	if err != nil || len(rows) != 1 || !reflect.DeepEqual(rows[0].AttachmentsState, want) || len(rows[0].VoicePending) != 0 {
		t.Fatalf("failed row=%+v err=%v", rows, err)
	}
	data, err := os.ReadFile(s.jsonlPath(rk))
	if err != nil {
		t.Fatal(err)
	}
	var disk map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(data), &disk); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(disk["_c3_attachments_state"], encoded) {
		t.Fatalf("disk metadata=%s", disk["_c3_attachments_state"])
	}
	public, err := s.Peek(rk, -1)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(public)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("attachments_state")) || bytes.Contains(encoded, []byte("provider_unavailable")) {
		t.Fatalf("private metadata leaked: %s", encoded)
	}
}

func TestLegacyAttachmentStateDoesNotInferRawTranscript(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "example", ChatID: -100}
	source := testSource(1001)
	legacy := storedInbound{Inbound: *msg(9, "[Transcribed voice]: old presentation"), Source: source}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.jsonlPath(rk), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	s = restartSourceStore(t, s)
	rows, err := s.PeekTracked(rk, -1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if got := rows[0].AttachmentsState[0]; got.STT != intake.STTFailed || got.Error != "metadata_unavailable" || got.Transcript != "" {
		t.Fatalf("legacy state=%+v", got)
	}
}

func TestAttachmentStateRejectsInvalidOutcomeWithoutClearingPending(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "example", ChatID: -100}
	source := testSource(1001)
	id, err := s.AppendTrackedSource(rk, msg(9, "pending"), source, "audio-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, outcome := range []intake.STTOutcome{
		{STT: intake.STTPending},
		{STT: intake.STTDone, Transcript: "raw", Error: "unexpected"},
		{STT: intake.STTFailed},
	} {
		if _, _, err := s.ResolveVoiceOutcome(rk, id, "audio-a", "invalid", outcome, nil); err == nil {
			t.Fatalf("invalid outcome accepted: %+v", outcome)
		}
	}
	if _, _, err := s.ResolveVoiceOutcome(rk, id, "audio-a", "invalid", intake.STTOutcome{STT: intake.STTDone, Transcript: "raw"}, intake.AttachmentsState{}); err == nil {
		t.Fatal("snapshot with wrong attachment count accepted")
	}
	s = restartSourceStore(t, s)
	rows, err := s.PeekTracked(rk, -1)
	if err != nil || len(rows) != 1 || rows[0].Inbound.Text != "pending" || len(rows[0].VoicePending) != 1 || rows[0].AttachmentsState[0].STT != intake.STTPending {
		t.Fatalf("invalid outcome mutated durable row: %+v err=%v", rows, err)
	}
}
