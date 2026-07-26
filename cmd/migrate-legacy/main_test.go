package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Andrometiq/c3/internal/mappings"
)

func TestMigrate_FreshConfig(t *testing.T) {
	dir := t.TempDir()
	envFile := filepath.Join(dir, "telegram.env")
	cfgFile := filepath.Join(dir, "config.json")
	outFile := filepath.Join(dir, "out.json")

	if err := os.WriteFile(envFile, []byte("TELEGRAM_BOT_TOKEN=tok123\nOTHER=xx\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgFile, []byte(`{"group_chat_id": -100, "dm_chat_id": 42}`), 0600); err != nil {
		t.Fatal(err)
	}

	if err := migrate(envFile, cfgFile, outFile); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	var mf mappings.MappingsFile
	if err := json.Unmarshal(data, &mf); err != nil {
		t.Fatal(err)
	}
	if mf.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, want 1", mf.SchemaVersion)
	}
	tg := mf.Channels["telegram"]
	if tg.BotToken != "tok123" {
		t.Errorf("BotToken = %q, want tok123", tg.BotToken)
	}
	if tg.DMChatID != 42 {
		t.Errorf("DMChatID = %d, want 42", tg.DMChatID)
	}
	if tg.DefaultGroup != "main" {
		t.Errorf("DefaultGroup = %q, want main", tg.DefaultGroup)
	}
	if tg.Groups["main"].ChatID != -100 {
		t.Errorf("Groups[main].ChatID = %d, want -100", tg.Groups["main"].ChatID)
	}

	info, _ := os.Stat(outFile)
	if info.Mode().Perm() != 0600 {
		t.Errorf("mode = %o, want 0600", info.Mode().Perm())
	}
}

func TestMigrate_RefusesIfOutputExists(t *testing.T) {
	dir := t.TempDir()
	envFile := filepath.Join(dir, "e.env")
	cfgFile := filepath.Join(dir, "c.json")
	outFile := filepath.Join(dir, "out.json")

	os.WriteFile(envFile, []byte("TELEGRAM_BOT_TOKEN=t"), 0600)
	os.WriteFile(cfgFile, []byte(`{"group_chat_id": -1, "dm_chat_id": 1}`), 0600)
	os.WriteFile(outFile, []byte("{}"), 0600)

	err := migrate(envFile, cfgFile, outFile)
	if err == nil {
		t.Error("expected refusal when output already exists, got nil")
	}
}

func TestMigrate_MissingEnv(t *testing.T) {
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "c.json")
	outFile := filepath.Join(dir, "out.json")
	os.WriteFile(cfgFile, []byte(`{"group_chat_id": -1, "dm_chat_id": 1}`), 0600)

	err := migrate(filepath.Join(dir, "missing.env"), cfgFile, outFile)
	if err == nil {
		t.Error("expected error for missing env file, got nil")
	}
}
