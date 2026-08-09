package queue

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestResolveVoiceText_PerFileAtomicAndAllDoneOnce(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	recordID, err := s.AppendTracked(rk, msg(7, "both pending"), "voice-a", "voice-b")
	if err != nil {
		t.Fatal(err)
	}

	resolved, allDone, err := s.ResolveVoiceText(rk, recordID, "voice-a", "voice-a done; voice-b pending")
	if err != nil || !resolved || allDone {
		t.Fatalf("first resolve = resolved %v allDone %v err %v", resolved, allDone, err)
	}
	rows := s.PendingVoiceRows(rk)
	if len(rows) != 1 || rows[0].Inbound.Text != "voice-a done; voice-b pending" ||
		len(rows[0].VoicePending) != 1 || rows[0].VoicePending[0] != "voice-b" {
		t.Fatalf("first resolve did not atomically update text and one file ID: %+v", rows)
	}

	resolved, allDone, err = s.ResolveVoiceText(rk, recordID, "voice-a", "duplicate must not overwrite")
	if err != nil || resolved || allDone {
		t.Fatalf("duplicate resolve = resolved %v allDone %v err %v", resolved, allDone, err)
	}

	injected := errors.New("crash before voice rewrite")
	s.rewriteTestHook = func() error { return injected }
	resolved, allDone, err = s.ResolveVoiceText(rk, recordID, "voice-b", "both done")
	if !errors.Is(err, injected) || resolved || allDone {
		t.Fatalf("failed resolve = resolved %v allDone %v err %v", resolved, allDone, err)
	}
	s.rewriteTestHook = nil
	rows = s.PendingVoiceRows(rk)
	if len(rows) != 1 || rows[0].Inbound.Text != "voice-a done; voice-b pending" ||
		len(rows[0].VoicePending) != 1 || rows[0].VoicePending[0] != "voice-b" {
		t.Fatalf("failed rewrite changed durable voice state: %+v", rows)
	}

	resolved, allDone, err = s.ResolveVoiceText(rk, recordID, "voice-b", "both done")
	if err != nil || !resolved || !allDone {
		t.Fatalf("final resolve = resolved %v allDone %v err %v", resolved, allDone, err)
	}
	if rows := s.PendingVoiceRows(rk); len(rows) != 0 {
		t.Fatalf("completed row still scans as pending: %+v", rows)
	}
	peek, err := s.PeekTracked(rk, -1)
	if err != nil || len(peek) != 1 || peek[0].Inbound.Text != "both done" || len(peek[0].VoicePending) != 0 {
		t.Fatalf("final row = %+v err=%v", peek, err)
	}
	resolved, allDone, err = s.ResolveVoiceText(rk, recordID, "voice-b", "second final")
	if err != nil || resolved || allDone {
		t.Fatalf("allDone fired more than once: resolved %v allDone %v err %v", resolved, allDone, err)
	}
}

func TestResolveVoiceText_ConsumedRowIsCleanNoOp(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	recordID, err := s.AppendTracked(rk, msg(7, "pending"), "voice-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Consume(rk, 1); err != nil {
		t.Fatal(err)
	}
	resolved, allDone, err := s.ResolveVoiceText(rk, recordID, "voice-a", "too late")
	if err != nil || resolved || allDone {
		t.Fatalf("consumed resolve = resolved %v allDone %v err %v", resolved, allDone, err)
	}
}

func TestPendingVoiceRows_LegacyTextIsFinalAndStoreWideScanFindsOnlyFlags(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	s, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	rkA := RouteKey{Channel: "telegram", ChatID: -100}
	topic := int64(42)
	rkB := RouteKey{Channel: "telegram", ChatID: -200, TopicID: &topic}

	legacy := msg(1, "[voice note looks pending, but has no private flag]")
	legacyJSON, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.jsonlPath(rkA), append(legacyJSON, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendTracked(rkA, msg(2, "pending a"), "voice-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendTracked(rkB, msg(3, "pending b"), "voice-b"); err != nil {
		t.Fatal(err)
	}
	if err := s.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}

	rowsA := s.PendingVoiceRows(rkA)
	if len(rowsA) != 1 || rowsA[0].Inbound.MessageID != 2 {
		t.Fatalf("route scan re-enriched legacy placeholder text: %+v", rowsA)
	}
	all := s.PendingVoiceRowsAll()
	found := map[string]int64{}
	for rk, rows := range all {
		if len(rows) != 1 {
			t.Fatalf("store-wide scan returned unexpected rows for %s: %+v", rk.File(), rows)
		}
		found[rk.File()] = rows[0].Inbound.MessageID
	}
	if len(found) != 2 || found[rkA.File()] != 2 || found[rkB.File()] != 3 {
		t.Fatalf("store-wide pending voice scan = %+v", found)
	}

	raw, err := os.ReadFile(s.jsonlPath(rkA))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	var oldReader struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &oldReader); err != nil || oldReader.Text != "pending a" {
		t.Fatalf("old reader rejected additive voice state: %+v err=%v", oldReader, err)
	}
}
