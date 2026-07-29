package mappings

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSessionAttachment_UpsertLookup(t *testing.T) {
	mf := &MappingsFile{}
	tid := int64(914)
	sa := SessionAttachment{Channel: "telegram", ChatID: -100, TopicID: &tid, Name: "c3", LastAttachedAt: time.Now().UTC()}
	mf.UpsertSessionAttachment("claude", "sess-1", sa)
	got, ok := mf.LookupSessionAttachment("claude", "sess-1")
	if !ok || got.Name != "c3" || got.TopicID == nil || *got.TopicID != 914 {
		t.Fatalf("lookup = %+v, ok=%v", got, ok)
	}
	if _, ok := mf.LookupSessionAttachment("claude", "nope"); ok {
		t.Fatal("unknown id should miss")
	}
	if _, ok := mf.LookupSessionAttachment("", "sess-1"); ok {
		t.Fatal("empty CLI should miss")
	}
	if _, ok := mf.LookupSessionAttachment("claude", ""); ok {
		t.Fatal("empty id should miss")
	}
}

func TestSessionAttachment_UpsertEmptyIDNoOp(t *testing.T) {
	mf := &MappingsFile{}
	mf.UpsertSessionAttachment("claude", "", SessionAttachment{Name: "c3"})
	mf.UpsertSessionAttachment("", "session", SessionAttachment{Name: "c3"})
	if len(mf.SessionAttachmentsByCLI) != 0 {
		t.Fatalf("incomplete namespace must not be stored; got %d CLI families", len(mf.SessionAttachmentsByCLI))
	}
}

func TestSessionAttachment_Recoverable(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	ttl := 30 * 24 * time.Hour
	if !(SessionAttachment{LastAttachedAt: now.Add(-time.Hour)}).Recoverable(now, ttl) {
		t.Fatal("fresh should be recoverable")
	}
	if (SessionAttachment{LastAttachedAt: now.Add(-time.Hour), Detached: true}).Recoverable(now, ttl) {
		t.Fatal("tombstoned must not be recoverable")
	}
	if (SessionAttachment{LastAttachedAt: now.Add(-31 * 24 * time.Hour)}).Recoverable(now, ttl) {
		t.Fatal("expired must not be recoverable")
	}
}

func TestSessionAttachment_Tombstone(t *testing.T) {
	mf := &MappingsFile{}
	mf.UpsertSessionAttachment("claude", "s", SessionAttachment{Name: "c3", LastAttachedAt: time.Now().UTC()})
	mf.TombstoneSessionAttachment("claude", "s")
	got, ok := mf.LookupSessionAttachment("claude", "s")
	if !ok || !got.Detached {
		t.Fatalf("expected tombstoned entry, got %+v ok=%v", got, ok)
	}
	mf.TombstoneSessionAttachment("claude", "missing") // must not panic
}

func TestSessionAttachment_UpsertClearsTombstone(t *testing.T) {
	mf := &MappingsFile{}
	mf.UpsertSessionAttachment("claude", "s", SessionAttachment{Name: "c3", LastAttachedAt: time.Now().UTC()})
	mf.TombstoneSessionAttachment("claude", "s")
	mf.UpsertSessionAttachment("claude", "s", SessionAttachment{Name: "c3", LastAttachedAt: time.Now().UTC()})
	got, _ := mf.LookupSessionAttachment("claude", "s")
	if got.Detached {
		t.Fatal("re-attach must clear the tombstone")
	}
}

func TestSessionAttachment_Prune(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	ttl := 30 * 24 * time.Hour
	mf := &MappingsFile{}
	mf.UpsertSessionAttachment("claude", "old", SessionAttachment{LastAttachedAt: now.Add(-40 * 24 * time.Hour)})
	mf.UpsertSessionAttachment("codex", "new", SessionAttachment{LastAttachedAt: now.Add(-time.Hour)})
	mf.SessionAttachments = map[string]SessionAttachment{
		"legacy-old": {LastAttachedAt: now.Add(-40 * 24 * time.Hour)},
	}
	if n := mf.PruneSessionAttachments(now, ttl); n != 2 {
		t.Fatalf("pruned = %d, want 2", n)
	}
	if _, ok := mf.LookupSessionAttachment("claude", "old"); ok {
		t.Fatal("old should be pruned")
	}
	if _, ok := mf.LookupSessionAttachment("codex", "new"); !ok {
		t.Fatal("new should survive")
	}
}

func TestSessionAttachment_CloneDeepCopies(t *testing.T) {
	tid := int64(914)
	mf := &MappingsFile{}
	mf.UpsertSessionAttachment("claude", "s", SessionAttachment{Name: "c3", TopicID: &tid, LastAttachedAt: time.Now().UTC()})
	cl := mf.Clone()
	sa, ok := cl.LookupSessionAttachment("claude", "s")
	if !ok || sa.Name != "c3" || sa.TopicID == nil || *sa.TopicID != 914 {
		t.Fatalf("Clone dropped/garbled session attachment: %+v ok=%v", sa, ok)
	}
	// Deep copy: mutating the clone's TopicID must not leak into the original.
	*sa.TopicID = 1
	if got := mf.SessionAttachmentsByCLI["claude"]["s"]; got.TopicID == nil || *got.TopicID != 914 {
		t.Fatalf("Clone must deep-copy TopicID; original mutated to %v", got.TopicID)
	}
}

func TestSessionAttachment_OmitemptyAndRoundTrip(t *testing.T) {
	// Empty store stays off the wire (old config files unchanged).
	b, _ := json.Marshal(&MappingsFile{SchemaVersion: 1})
	if strings.Contains(string(b), "session_attachments") {
		t.Fatalf("empty store must be omitted: %s", b)
	}
	// Round-trip a populated store.
	tid := int64(914)
	mf := &MappingsFile{SchemaVersion: 1}
	mf.UpsertSessionAttachment("claude", "s", SessionAttachment{Channel: "telegram", ChatID: -100, TopicID: &tid, Name: "c3", LastAttachedAt: time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)})
	raw, _ := json.Marshal(mf)
	var back MappingsFile
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	sa, ok := back.LookupSessionAttachment("claude", "s")
	if !ok || sa.Name != "c3" || sa.TopicID == nil || *sa.TopicID != 914 {
		t.Fatalf("round-trip = %+v ok=%v", sa, ok)
	}
}

func TestSessionAttachment_SameHostIDIsIndependentAcrossCLIFamilies(t *testing.T) {
	now := time.Now().UTC()
	mf := &MappingsFile{}
	mf.UpsertSessionAttachment("claude", "same-id", SessionAttachment{Name: "claude-topic", LastAttachedAt: now})
	mf.UpsertSessionAttachment("codex", "same-id", SessionAttachment{Name: "codex-topic", LastAttachedAt: now})

	claude, claudeOK := mf.LookupSessionAttachment("claude", "same-id")
	codex, codexOK := mf.LookupSessionAttachment("codex", "same-id")
	if !claudeOK || !codexOK || claude.Name != "claude-topic" || codex.Name != "codex-topic" {
		t.Fatalf("two CLI families sharing one host id collided: claude=%+v/%v codex=%+v/%v", claude, claudeOK, codex, codexOK)
	}
}

func TestSessionAttachment_LegacyKeyCanIdentifyOnlyOneCLIFamily(t *testing.T) {
	mf := &MappingsFile{SessionAttachments: map[string]SessionAttachment{
		"same-id": {Name: "legacy-topic", LastAttachedAt: time.Now().UTC()},
	}}

	claude, ok := mf.ClaimLegacySessionAttachment("claude", "same-id")
	if !ok || claude.Name != "legacy-topic" {
		t.Fatalf("first CLI could not claim the legacy attachment: %+v ok=%v", claude, ok)
	}
	if _, ok := mf.ClaimLegacySessionAttachment("codex", "same-id"); ok {
		t.Fatal("one unqualified legacy key identified two CLI families")
	}
	if _, stillLegacy := mf.SessionAttachments["same-id"]; stillLegacy {
		t.Fatal("claimed legacy attachment was not removed from the unqualified store")
	}
}

func TestSessionAttachment_LegacyKeyCannotEscapeAnExistingCLINamespace(t *testing.T) {
	now := time.Now().UTC()
	mf := &MappingsFile{
		SessionAttachments: map[string]SessionAttachment{
			"same-id": {Name: "legacy-topic", LastAttachedAt: now},
		},
		SessionAttachmentsByCLI: map[string]map[string]SessionAttachment{
			"claude": {
				"same-id": {Name: "claude-topic", LastAttachedAt: now},
			},
		},
	}

	if _, ok := mf.ClaimLegacySessionAttachment("codex", "same-id"); ok {
		t.Fatal("a legacy key already claimed by one CLI family became evidence for a second family")
	}
}
