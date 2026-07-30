package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/mappings"
)

func TestRunInstallDesktop_RecordsSourceHandlerForNonPluginHosts(t *testing.T) {
	isolateDesktopInstallRuntime(t)
	src := desktopSTTSourceTree(t)
	t.Setenv("C3_SRC_DIR", src)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	if err := runInstallDesktop([]string{"--config", filepath.Join(t.TempDir(), "claude_desktop_config.json")}); err != nil {
		t.Fatalf("runInstallDesktop: %v", err)
	}
	want := filepath.Join(src, "plugins", "c3", "stt", "stt-handler.py")
	mf, err := mappings.Read(filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "c3", "mappings.json"))
	if err != nil {
		t.Fatalf("read written mappings: %v", err)
	}
	if got, _ := mf.Plugins["stt"]["handler_path"].(string); got != want {
		t.Fatalf("written handler_path = %q, want %q — Desktop install omitted the durable STT handler resolution", got, want)
	}
}

func TestInstallSTTHandlerPath_PreservesExplicitHandlerOverride(t *testing.T) {
	isolateDesktopInstallRuntime(t)
	src := desktopSTTSourceTree(t)
	t.Setenv("C3_SRC_DIR", src)
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	path := filepath.Join(configHome, "c3", "mappings.json")
	custom := filepath.Join(t.TempDir(), "custom-handler.py")
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]mappings.ChannelConfig{},
		Mappings:      map[string]mappings.Mapping{},
		Plugins:       map[string]map[string]any{"stt": {"handler_path": custom}},
	}
	if err := writeMappingsFile(path, mf); err != nil {
		t.Fatalf("write mappings: %v", err)
	}

	if err := runInstallDesktop([]string{"--config", filepath.Join(t.TempDir(), "claude_desktop_config.json")}); err != nil {
		t.Fatalf("runInstallDesktop: %v", err)
	}
	got, err := mappings.Read(path)
	if err != nil {
		t.Fatalf("read mappings: %v", err)
	}
	if actual, _ := got.Plugins["stt"]["handler_path"].(string); actual != custom {
		t.Fatalf("handler_path = %q, want explicit %q — desktop installer overwrote the explicit STT handler override", actual, custom)
	}
}

func TestRunInstallDesktop_RecordsHandlerFromReleaseBundle(t *testing.T) {
	isolateDesktopInstallRuntime(t)
	release := t.TempDir()
	handler := filepath.Join(release, "plugins", "c3", "stt", "stt-handler.py")
	writeCompleteSTTBundleAt(t, filepath.Dir(handler))
	t.Setenv("C3_SRC_DIR", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Chdir(t.TempDir())
	previous := desktopExecutablePath
	desktopExecutablePath = func() (string, error) { return filepath.Join(release, "c3-broker"), nil }
	t.Cleanup(func() { desktopExecutablePath = previous })

	if err := runInstallDesktop([]string{"--config", filepath.Join(t.TempDir(), "claude_desktop_config.json")}); err != nil {
		t.Fatalf("runInstallDesktop: %v", err)
	}
	mf, err := mappings.Read(filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "c3", "mappings.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := mf.Plugins["stt"]["handler_path"].(string); got != handler {
		t.Fatalf("packaged handler_path = %q, want %q — installer ignored the release bundle beside c3-broker", got, handler)
	}
}

func TestRunInstallDesktop_PreflightsMappingsBeforeChangingDesktopConfig(t *testing.T) {
	isolateDesktopInstallRuntime(t)
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	mappingsPath := filepath.Join(configHome, "c3", "mappings.json")
	if err := os.MkdirAll(filepath.Dir(mappingsPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mappingsPath, []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	desktopConfig := filepath.Join(t.TempDir(), "claude_desktop_config.json")

	if err := runInstallDesktop([]string{"--config", desktopConfig}); err == nil {
		t.Fatal("corrupt mappings must stop install-desktop before it changes Claude Desktop config")
	}
	if _, err := os.Stat(desktopConfig); !os.IsNotExist(err) {
		t.Fatalf("Desktop config was written before mappings preflight failed: %v", err)
	}
}

func TestRunInstallDesktop_MissingRuntimeHandlerFailsLoudly(t *testing.T) {
	isolateDesktopInstallRuntime(t)
	t.Setenv("C3_SRC_DIR", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Chdir(t.TempDir())
	previous := desktopExecutablePath
	desktopExecutablePath = func() (string, error) { return filepath.Join(t.TempDir(), "c3-broker"), nil }
	t.Cleanup(func() { desktopExecutablePath = previous })
	desktopConfig := filepath.Join(t.TempDir(), "claude_desktop_config.json")

	if err := runInstallDesktop([]string{"--config", desktopConfig}); err == nil {
		t.Fatal("install-desktop succeeded without an explicit or discoverable STT handler")
	}
	if _, err := os.Stat(desktopConfig); !os.IsNotExist(err) {
		t.Fatalf("Desktop config was written despite the missing runtime handler: %v", err)
	}
}

func TestRunInstallDesktop_DisabledSTTNeedsNoRuntimeHandler(t *testing.T) {
	isolateDesktopInstallRuntime(t)
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("C3_SRC_DIR", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("HOME", t.TempDir())
	t.Chdir(t.TempDir())
	previousExecutable := desktopExecutablePath
	desktopExecutablePath = func() (string, error) { return filepath.Join(t.TempDir(), "c3-broker"), nil }
	t.Cleanup(func() { desktopExecutablePath = previousExecutable })

	mappingsPath := filepath.Join(configHome, "c3", "mappings.json")
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]mappings.ChannelConfig{},
		Mappings:      map[string]mappings.Mapping{},
		Plugins:       map[string]map[string]any{"stt": {"enabled": false}},
	}
	if err := writeMappingsFile(mappingsPath, mf); err != nil {
		t.Fatal(err)
	}
	desktopConfig := filepath.Join(t.TempDir(), "claude_desktop_config.json")

	if err := runInstallDesktop([]string{"--config", desktopConfig}); err != nil {
		t.Fatalf("install-desktop with STT explicitly disabled: %v", err)
	}
	if _, err := os.Stat(desktopConfig); err != nil {
		t.Fatalf("Desktop config was not written for an operator who disabled STT: %v", err)
	}
	got, err := mappings.Read(mappingsPath)
	if err != nil {
		t.Fatal(err)
	}
	if enabled, _ := got.Plugins["stt"]["enabled"].(bool); enabled {
		t.Fatal("install-desktop changed plugins.stt.enabled=false while bypassing the handler requirement")
	}
	if _, exists := got.Plugins["stt"]["handler_path"]; exists {
		t.Fatal("install-desktop added a handler_path even though STT is explicitly disabled")
	}
}

func TestRunInstallDesktop_MappingsWriteFailureLeavesDesktopConfigUntouched(t *testing.T) {
	isolateDesktopInstallRuntime(t)
	src := desktopSTTSourceTree(t)
	t.Setenv("C3_SRC_DIR", src)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	desktopConfig := filepath.Join(t.TempDir(), "claude_desktop_config.json")
	const original = "{\"unrelated\":\"preserve me\"}\n"
	if err := os.WriteFile(desktopConfig, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	previousWrite := desktopWriteMappingsFile
	desktopWriteMappingsFile = func(string, *mappings.MappingsFile) error {
		return errors.New("forced mappings write failure")
	}
	t.Cleanup(func() { desktopWriteMappingsFile = previousWrite })

	if err := runInstallDesktop([]string{"--config", desktopConfig}); err == nil {
		t.Fatal("forced mappings write failure must stop install-desktop")
	}
	got, err := os.ReadFile(desktopConfig)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != original {
		t.Fatalf("Desktop config changed before mappings commit succeeded: got %q, want original %q", got, original)
	}
}

func TestRunInstallDesktop_RefusesWhileBrokerCanMutateMappings(t *testing.T) {
	isolateDesktopInstallRuntime(t)
	src := desktopSTTSourceTree(t)
	t.Setenv("C3_SRC_DIR", src)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	pidFile, err := broker.PidFilePath()
	if err != nil {
		t.Fatal(err)
	}
	lock, err := broker.AcquireSingleton(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lock.Release)
	desktopConfig := filepath.Join(t.TempDir(), "claude_desktop_config.json")

	if err := runInstallDesktop([]string{"--config", desktopConfig}); err == nil {
		t.Fatal("install-desktop must refuse while a live broker can race its mappings update")
	}
	if _, err := os.Stat(desktopConfig); !os.IsNotExist(err) {
		t.Fatalf("Desktop config changed despite broker/mappings lock contention: %v", err)
	}
}

func isolateDesktopInstallRuntime(t *testing.T) {
	t.Helper()
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
}

func desktopSTTSourceTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module github.com/Andrometiq/c3\n"), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	handler := filepath.Join(root, "plugins", "c3", "stt", "stt-handler.py")
	if err := os.MkdirAll(filepath.Dir(handler), 0o755); err != nil {
		t.Fatalf("mkdir handler parent: %v", err)
	}
	if err := os.WriteFile(handler, []byte("# handler\n"), 0o644); err != nil {
		t.Fatalf("write handler: %v", err)
	}
	return root
}
