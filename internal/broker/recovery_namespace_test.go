package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

func recoverViaCLI(t *testing.T, b *Broker, cli string, pid int, stableID string) (*ipc.Conn, ipc.RecoverSessionResp) {
	t.Helper()
	peer, _ := peerPair(t, b)
	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: cli, PID: pid, CWD: "/project"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := peer.WriteJSON(ipc.RecoverSessionReq{
		Op: ipc.OpRecoverSession, StableSessionID: stableID, CWD: "/project",
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.RecoverSessionResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	return peer, resp
}

func TestRecoverSession_SameHostIDAcrossCLIFamiliesRecoversOwnRoutes(t *testing.T) {
	mf := mfWithTelegram()
	now := time.Now().UTC()
	topicClaude, topicCodex := int64(281), int64(412)
	mf.UpsertSessionAttachment("claude", "same-host-id", mappings.SessionAttachment{
		Channel: "telegram", ChatID: -100, TopicID: &topicClaude, Name: "c3", Group: "main", LastAttachedAt: now,
	})
	mf.UpsertSessionAttachment("codex", "same-host-id", mappings.SessionAttachment{
		Channel: "telegram", ChatID: -200, TopicID: &topicCodex, Name: "feature-x", Group: "work", LastAttachedAt: now,
	})
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()

	claudePeer, claude := recoverViaCLI(t, b, "claude", 101, "same-host-id")
	defer claudePeer.Close()
	codexPeer, codex := recoverViaCLI(t, b, "codex", 202, "same-host-id")
	defer codexPeer.Close()

	if !claude.Recovered || claude.TopicID == nil || *claude.TopicID != topicClaude {
		t.Fatalf("Claude recovered another CLI family's route: %+v", claude)
	}
	if !codex.Recovered || codex.TopicID == nil || *codex.TopicID != topicCodex {
		t.Fatalf("Codex recovered another CLI family's route: %+v", codex)
	}
}

func TestRecoverSession_LegacyIdentityCanBeClaimedByOnlyOneCLIFamily(t *testing.T) {
	topic := int64(281)
	mf := mfWithTelegram()
	mf.SessionAttachments = map[string]mappings.SessionAttachment{
		"legacy-id": {
			Channel: "telegram", ChatID: -100, TopicID: &topic, Name: "c3",
			LastAttachedAt: time.Now().UTC(),
		},
	}
	b := brokerWithChannel(t, mf, &fakeChannel{})
	defer b.Shutdown()
	path, err := mappings.DefaultPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}

	if _, ok := b.lookupSessionAttachment("claude", "legacy-id"); !ok {
		t.Fatal("first CLI family could not migrate its legacy recovery record")
	}
	if _, ok := b.lookupSessionAttachment("codex", "legacy-id"); ok {
		t.Fatal("one legacy stable id became recovery evidence for two CLI families")
	}
}

func TestRecoverSession_LegacyIdentityMigrationFailureRefusesRecovery(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	notDir := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(notDir, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", notDir)

	mf := &mappings.MappingsFile{SessionAttachments: map[string]mappings.SessionAttachment{
		"legacy-id": {Name: "c3", LastAttachedAt: time.Now().UTC()},
	}}
	b := New(mf)
	defer b.Shutdown()

	if _, ok := b.lookupSessionAttachment("claude", "legacy-id"); ok {
		t.Fatal("an unpersisted legacy namespace claim was used as recovery identity")
	}
	if _, exists := b.Mappings().SessionAttachments["legacy-id"]; !exists {
		t.Fatal("failed migration mutated the in-memory legacy record")
	}
	if _, exists := b.Mappings().LookupSessionAttachment("claude", "legacy-id"); exists {
		t.Fatal("failed migration installed an in-memory namespaced identity")
	}
}
