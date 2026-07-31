package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestMergeCursorMCP_CreatesFresh(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mcp.json")
	if err := mergeCursorMCP(path); err != nil {
		t.Fatalf("mergeCursorMCP: %v", err)
	}
	got := readCursorMCP(t, path)
	cmd, _ := got["mcpServers"].(map[string]any)["c3"].(map[string]any)["command"].(string)
	if cmd != "c3-cursor-adapter" {
		t.Fatalf("c3.command = %q, want c3-cursor-adapter", cmd)
	}
}

func TestMergeCursorMCP_PreservesOtherServers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mcp.json")
	initial := []byte(`{
  "mcpServers": {
    "playwright": {"command": "npx", "args": ["-y", "@playwright/mcp"]},
    "c3": {"command": "c3-claude-adapter"}
  }
}
`)
	if err := os.WriteFile(path, initial, 0644); err != nil {
		t.Fatal(err)
	}
	if err := mergeCursorMCP(path); err != nil {
		t.Fatalf("mergeCursorMCP: %v", err)
	}
	got := readCursorMCP(t, path)
	servers := got["mcpServers"].(map[string]any)
	if cmd, _ := servers["c3"].(map[string]any)["command"].(string); cmd != "c3-cursor-adapter" {
		t.Fatalf("c3.command = %q, want c3-cursor-adapter (must replace Claude adapter)", cmd)
	}
	pw, ok := servers["playwright"].(map[string]any)
	if !ok || pw["command"] != "npx" {
		t.Fatalf("playwright server corrupted: %#v", servers["playwright"])
	}
}

func TestInstallCursorAt_WritesUnderCursorDir(t *testing.T) {
	dir := t.TempDir()
	home := t.TempDir()
	if err := installCursorAt(dir, home); err != nil {
		t.Fatalf("installCursorAt: %v", err)
	}
	path := filepath.Join(dir, "mcp.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected mcp.json: %v", err)
	}
	for _, name := range []string{"fetch.md", "c3-fetch.md"} {
		cmdPath := filepath.Join(dir, "commands", name)
		data, err := os.ReadFile(cmdPath)
		if err != nil {
			t.Fatalf("expected %s: %v", cmdPath, err)
		}
		if !bytes.Contains(data, []byte("fetch_queue")) {
			t.Fatalf("%s missing fetch_queue instruction", name)
		}
	}
}

func TestMergeMCPDisabled_AddsClaudePluginIds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mcp-disabled.json")
	if err := os.WriteFile(path, []byte(`["other"]`+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := mergeMCPDisabled(path, cursorClaudePluginMCPIds); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, id := range list {
		have[id] = true
	}
	if !have["other"] || !have["plugin-c3-c3"] {
		t.Fatalf("list = %v, want other + plugin-c3-c3", list)
	}
}

func TestDisableClaudePluginC3InProjects(t *testing.T) {
	projects := t.TempDir()
	proj := filepath.Join(projects, "home-karthi-arogara")
	if err := os.MkdirAll(proj, 0700); err != nil {
		t.Fatal(err)
	}
	n := disableClaudePluginC3InProjects(projects)
	if n != 1 {
		t.Fatalf("updated %d projects, want 1", n)
	}
	path := filepath.Join(proj, "mcp-disabled.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		t.Fatal(err)
	}
	if len(list) < 1 || !containsStr(list, "plugin-c3-c3") {
		t.Fatalf("disabled list = %v", list)
	}
}

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

func readCursorMCP(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, data)
	}
	return root
}
