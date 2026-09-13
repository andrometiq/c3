package queue

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/intake"
)

func testSource(updateID int64) *intake.Source {
	topic, sender := int64(7), int64(42)
	return &intake.Source{Channel: "example", ChatID: -100, TopicID: &topic, MessageID: 9,
		SenderID: &sender, UpdateID: updateID, Text: "original <caption> & words",
		Attachments: []intake.SourceAttachment{{Kind: "voice", FileID: "audio-a", Size: 123, MIME: "audio/ogg"}, {Kind: "document", FileID: "doc-b", Size: 456, MIME: "text/plain", Name: "acme.txt"}}}
}

func sourceJSON(t *testing.T, source *intake.Source) string {
	t.Helper()
	b, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func restartSourceStore(t *testing.T, s *Store) *Store {
	t.Helper()
	next, err := NewStore(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	return next
}

func TestSourcePersistsAcrossEnrichmentProjectionAndRestart(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "example", ChatID: -100}
	source := testSource(1001)
	want := sourceJSON(t, source)
	id, err := s.AppendTrackedSource(rk, msg(9, "pending"), source, "audio-a")
	if err != nil {
		t.Fatal(err)
	}
	source.Text = "caller mutation"
	*source.TopicID = 99
	*source.SenderID = 99
	source.Attachments[0].FileID = "caller mutation"
	s = restartSourceStore(t, s)
	rows, err := s.PeekTracked(rk, -1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	if got := sourceJSON(t, rows[0].Source); got != want {
		t.Fatalf("source=%s want=%s", got, want)
	}
	rows[0].Source.Attachments[0].FileID = "projection mutation"
	if applied, done, err := s.ResolveVoiceText(rk, id, "audio-a", "enriched text"); err != nil || !applied || !done {
		t.Fatalf("resolve=%v/%v/%v", applied, done, err)
	}
	s = restartSourceStore(t, s)
	rows, err = s.PeekTracked(rk, -1)
	if err != nil || len(rows) != 1 || rows[0].RecordID != id || rows[0].Inbound.Text != "enriched text" {
		t.Fatalf("enrichment rows=%+v err=%v", rows, err)
	}
	if got := sourceJSON(t, rows[0].Source); got != want {
		t.Fatalf("enrichment changed source: %s", got)
	}
	data, err := os.ReadFile(s.jsonlPath(rk))
	if err != nil {
		t.Fatal(err)
	}
	var disk map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(data), &disk); err != nil {
		t.Fatal(err)
	}
	if string(disk["_c3_source"]) != want {
		t.Fatalf("private disk source=%s", disk["_c3_source"])
	}
	public, err := s.Peek(rk, -1)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(public)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("_c3_source")) || bytes.Contains(encoded, []byte("update_id")) {
		t.Fatalf("private source leaked: %s", encoded)
	}
}

func TestSourceConflictKeepsFirstAcrossRoutesAndRestartUntilRowsRetire(t *testing.T) {
	s := newStore(t)
	a := RouteKey{Channel: "example", ChatID: -100}
	b := RouteKey{Channel: "example", ChatID: -200}
	first := testSource(1001)
	want := sourceJSON(t, first)
	if err := s.Append(a, msg(9, "presentation"), first); err != nil {
		t.Fatal(err)
	}
	s = restartSourceStore(t, s)
	conflict := first.Clone()
	conflict.Text = "private conflict text"
	conflict.Attachments[0].Name = "private conflict name"
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)
	if _, err := s.AppendDrainedTracked(b, msg(9, "copy"), "original-row", conflict); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "source conflict") || !strings.Contains(logs.String(), "update_id=1001") || strings.Contains(logs.String(), first.Text) || strings.Contains(logs.String(), conflict.Text) || strings.Contains(logs.String(), conflict.Attachments[0].Name) {
		t.Fatalf("conflict log=%s", logs.String())
	}
	s = restartSourceStore(t, s)
	rows, err := s.PeekTracked(b, -1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	if got := sourceJSON(t, rows[0].Source); got != want {
		t.Fatalf("conflict overwrote first source: %s", got)
	}
	edit := conflict.Clone()
	edit.UpdateID++
	if err := s.Append(b, msg(9, "edit"), edit); err != nil {
		t.Fatal(err)
	}
	s = restartSourceStore(t, s)
	rows, err = s.PeekTracked(b, -1)
	if err != nil || len(rows) != 2 || sourceJSON(t, rows[1].Source) != sourceJSON(t, edit) {
		t.Fatalf("edit occurrence lost: %+v err=%v", rows, err)
	}
	if _, err := s.Consume(a, -1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Consume(b, -1); err != nil {
		t.Fatal(err)
	}
	logs.Reset()
	if err := s.Append(a, msg(9, "new lifetime"), conflict); err != nil {
		t.Fatal(err)
	}
	s = restartSourceStore(t, s)
	rows, err = s.PeekTracked(a, -1)
	if err != nil || len(rows) != 1 || sourceJSON(t, rows[0].Source) != sourceJSON(t, conflict) {
		t.Fatalf("source survived past row lifetime: %+v err=%v", rows, err)
	}
	if strings.Contains(logs.String(), "source conflict") {
		t.Fatalf("retired source still participated: %s", logs.String())
	}
}

func TestSourceOccurrenceKeyIncludesChannelChatAndNullableTopic(t *testing.T) {
	s := newStore(t)
	route := RouteKey{Channel: "example", ChatID: 1}
	first := testSource(1001)
	sources := []*intake.Source{first}
	for i := range 4 {
		next := first.Clone()
		next.Text = "independent occurrence"
		switch i {
		case 0:
			next.Channel = "acme"
		case 1:
			next.ChatID++
		case 2:
			next.TopicID = nil
		case 3:
			*next.TopicID = 0
		}
		sources = append(sources, next)
	}
	for _, source := range sources {
		if err := s.Append(route, msg(9, "presentation"), source); err != nil {
			t.Fatal(err)
		}
	}
	s = restartSourceStore(t, s)
	rows, err := s.PeekTracked(route, -1)
	if err != nil || len(rows) != len(sources) {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	for i, row := range rows {
		if sourceJSON(t, row.Source) != sourceJSON(t, sources[i]) {
			t.Fatalf("key conflated occurrence %d", i)
		}
	}
}
