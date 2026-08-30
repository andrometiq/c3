package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Andrometiq/c3/internal/mappings"
)

func TestRunInstallDesktopRecordsTTSHandler(t *testing.T) {
	isolateDesktopInstallRuntime(t)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module github.com/Andrometiq/c3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	handler := filepath.Join(root, "plugins", "c3", "tts", "tts-handler.py")
	if err := os.MkdirAll(filepath.Dir(handler), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(handler, []byte("# handler\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("C3_SRC_DIR", root)
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	mappingPath := filepath.Join(configHome, "c3", "mappings.json")
	mapping := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]mappings.ChannelConfig{},
		Mappings:      map[string]mappings.Mapping{},
		Plugins:       map[string]map[string]any{"stt": {"enabled": false}},
	}
	if err := writeMappingsFile(mappingPath, mapping); err != nil {
		t.Fatal(err)
	}
	if err := runInstallDesktop([]string{"--config", filepath.Join(t.TempDir(), "claude_desktop_config.json")}); err != nil {
		t.Fatal(err)
	}
	written, err := mappings.Read(mappingPath)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := written.Plugins["tts"]["handler_path"].(string); got != handler {
		t.Fatalf("TTS handler_path = %q, want %q", got, handler)
	}
}
