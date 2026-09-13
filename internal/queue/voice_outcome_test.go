package queue

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/intake"
)

func TestVoiceOutcomeBudgetsFullRecordAndRecovers(t *testing.T) {
	for _, revision := range []bool{false, true} {
		for _, raw := range []string{"  raw words\n& <tags>  ", strings.Repeat("x", MaxRecordBytes/2), strings.Repeat("<", MaxRecordBytes/10)} {
			name := "resolve"
			if revision {
				name = "revision"
			}
			t.Run(name+"/"+stringSizeName(raw), func(t *testing.T) {
				s := newStore(t)
				rk := RouteKey{Channel: "example", ChatID: -100}
				source := testSource(1001)
				source.Attachments = append(source.Attachments, intake.SourceAttachment{Kind: "voice", FileID: "audio-a"})
				in := msg(9, "caption; pending; sibling untouched")
				id, err := s.AppendTrackedSource(rk, in, source, "audio-a")
				if err != nil {
					t.Fatal(err)
				}
				done := intake.STTOutcome{STT: intake.STTDone, Transcript: raw}
				text, notice := "caption; [Transcribed voice]: "+raw+"; sibling untouched", "caption; resend shorter; sibling untouched"
				var final intake.STTOutcome
				if revision {
					if _, err := s.Consume(rk, -1); err != nil {
						t.Fatal(err)
					}
					id, final, err = s.AppendVoiceOutcome(rk, id, "audio-a", *in, msg(9, text), notice, source, nil, done)
				} else {
					var resolved, allDone bool
					resolved, allDone, final, err = s.ResolveVoiceOutcome(rk, id, "audio-a", text, notice, done, nil)
					if !resolved || !allDone {
						t.Fatalf("resolve=%v/%v err=%v", resolved, allDone, err)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				want := done
				wantText := text
				if len(raw) > 100 {
					want = intake.STTOutcome{STT: intake.STTFailed, Error: "transcript_too_large"}
					wantText = notice
				}
				if final != want {
					t.Fatalf("final state=%s error=%q raw bytes=%d", final.STT, final.Error, len(final.Transcript))
				}
				data, err := os.ReadFile(s.jsonlPath(rk))
				if err != nil {
					t.Fatal(err)
				}
				for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
					if len(line) > MaxRecordBytes {
						t.Fatalf("stored %d bytes", len(line))
					}
				}
				if want.STT == intake.STTFailed && bytes.Contains(data, []byte(`"transcript":`)) {
					t.Fatal("oversize raw transcript stored on disk")
				}
				s = restartSourceStore(t, s)
				rows, err := s.PeekTracked(rk, -1)
				if err != nil || len(rows) != 1 {
					t.Fatalf("recovered rows=%d err=%v", len(rows), err)
				}
				row := rows[0]
				got, terminal := row.AttachmentsState.TerminalOutcome(row.Source, "audio-a")
				if row.RecordID != id || row.Inbound.Text != wantText || len(row.VoicePending) != 0 || !terminal || got != want || !reflect.DeepEqual(row.Source, source) {
					t.Fatal("recovered row lost identity, presentation, source, or terminal vector")
				}
			})
		}
	}
}

func stringSizeName(s string) string {
	if len(s) < 100 {
		return "under bound"
	}
	if strings.HasPrefix(s, "<") {
		return "JSON escaping exceeds bound"
	}
	return "duplicated transcript exceeds bound"
}

func TestVoiceResolveLeavesUnrelatedLegacyOversizeRowUntouched(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "example", ChatID: -100}
	legacy := storedInbound{Inbound: *msg(8, "[Transcribed voice]: "+strings.Repeat("legacy words ", MaxRecordBytes/10))}
	before, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) <= MaxRecordBytes {
		t.Fatal("fixture must exceed the bound")
	}
	if err := os.WriteFile(s.jsonlPath(rk), append(before, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	id, err := s.AppendTrackedSource(rk, msg(9, "pending"), testSource(1001), "audio-a")
	if err != nil {
		t.Fatal(err)
	}
	if ok, done, _, err := s.ResolveVoiceOutcome(rk, id, "audio-a", "[Transcribed voice]: small", "notice", intake.STTOutcome{STT: intake.STTDone, Transcript: "small"}, nil); err != nil || !ok || !done {
		t.Fatalf("resolve=%v/%v err=%v", ok, done, err)
	}
	after, err := os.ReadFile(s.jsonlPath(rk))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.SplitN(after, []byte{'\n'}, 2)[0], before) {
		t.Fatal("unrelated legacy line changed during rewrite")
	}
	s = restartSourceStore(t, s)
	rows, err := s.PeekTracked(rk, -1)
	if err != nil || len(rows) != 2 || rows[0].Inbound.Text != legacy.Text {
		t.Fatalf("legacy transcript lost on restart: rows=%d err=%v", len(rows), err)
	}
}

func TestVoiceRevisionRetryAfterDirSyncFailureKeepsOneRowPerFile(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "example", ChatID: -100}
	source := testSource(1001)
	source.Attachments = append(source.Attachments, intake.SourceAttachment{Kind: "voice", FileID: "audio-b"})
	states := intake.NewAttachmentsState(source, nil)
	for _, fileID := range []string{"audio-a", "audio-b"} {
		states = states.WithOutcome(source, fileID, intake.STTOutcome{STT: intake.STTDone, Transcript: fileID})
	}
	in := msg(9, "original")
	injected := errors.New("injected post-append directory sync failure")
	s.SyncDirTestHook = func() error { return injected }
	outcome := intake.STTOutcome{STT: intake.STTDone, Transcript: "audio-a"}
	if _, _, err := s.AppendVoiceOutcome(rk, "original-id", "audio-a", *in, msg(9, "revision a"), "notice", source, states, outcome); !errors.Is(err, injected) {
		t.Fatalf("append err=%v", err)
	}
	visible, err := s.PeekTracked(rk, -1)
	if err != nil || len(visible) != 1 {
		t.Fatalf("post-append bytes not visible: rows=%d err=%v", len(visible), err)
	}
	// A second failed fsync must neither append nor claim durability.
	if _, _, err := s.AppendVoiceOutcome(rk, "original-id", "audio-a", *in, msg(9, "revision a"), "notice", source, states, outcome); !errors.Is(err, injected) {
		t.Fatalf("retry err=%v", err)
	}
	s = restartSourceStore(t, s)
	id, final, err := s.AppendVoiceOutcome(rk, "original-id", "audio-a", *in, msg(9, "revision a"), "notice", source, states, outcome)
	if err != nil || id != visible[0].RecordID || final != outcome {
		t.Fatalf("retry id=%q final=%+v err=%v", id, final, err)
	}
	outcome.Transcript = "audio-b"
	if _, _, err := s.AppendVoiceOutcome(rk, "original-id", "audio-b", *in, msg(9, "revision b"), "notice", source, states, outcome); err != nil {
		t.Fatal(err)
	}
	s = restartSourceStore(t, s)
	rows, err := s.PeekTracked(rk, -1)
	if err != nil || len(rows) != 2 || rows[0].Inbound.Text != "revision a" || rows[1].Inbound.Text != "revision b" {
		t.Fatalf("retry duplicated a revision or suppressed its sibling: rows=%d err=%v", len(rows), err)
	}
}

func TestVoiceBudgetUnfitFallbackDoesNotMutatePendingRow(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "example", ChatID: -100}
	source := testSource(1001)
	id, err := s.AppendTrackedSource(rk, msg(9, "pending"), source, "audio-a")
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.jsonlPath(rk))
	if err != nil {
		t.Fatal(err)
	}
	tooLarge := strings.Repeat("x", MaxRecordBytes)
	if resolved, _, final, err := s.ResolveVoiceOutcome(rk, id, "audio-a", tooLarge, tooLarge, intake.STTOutcome{STT: intake.STTDone, Transcript: tooLarge}, nil); err == nil || resolved || final != (intake.STTOutcome{}) {
		t.Fatal("unfit fallback was accepted or returned a persisted outcome")
	}
	after, err := os.ReadFile(s.jsonlPath(rk))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed budget decision changed durable bytes")
	}
	s = restartSourceStore(t, s)
	rows, err := s.PeekTracked(rk, -1)
	if err != nil || len(rows) != 1 || len(rows[0].VoicePending) != 1 || rows[0].AttachmentsState[0].STT != intake.STTPending {
		t.Fatal("failed budget decision lost recoverable pending work")
	}
}
