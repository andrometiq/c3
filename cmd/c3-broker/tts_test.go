package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Andrometiq/c3/internal/mappings"
)

func TestRunTTSSayWritesOnlyMP3Bytes(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("C3_QUEUE_DIR", filepath.Join(t.TempDir(), "queue"))
	handler := filepath.Join(t.TempDir(), "tts-handler.py")
	script := "import sys\nprint('provider=fake chunks=1 bytes=7 ms=1', file=sys.stderr)\nsys.stdout.buffer.write(b'ID3fake')\n"
	if err := os.WriteFile(handler, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	mapping := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]mappings.ChannelConfig{},
		Mappings:      map[string]mappings.Mapping{},
		Plugins: map[string]map[string]any{
			"tts": {"enabled": true, "handler_path": handler, "timeout_seconds": 5},
		},
	}
	path := filepath.Join(configHome, "c3", "mappings.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := mappings.Write(path, mapping); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runTTSWithWriter([]string{"say", "hello", "world"}, &output); err != nil {
		t.Fatalf("runTTS: %v", err)
	}
	if got := output.String(); got != "ID3fake" {
		t.Fatalf("stdout = %q", got)
	}
}
