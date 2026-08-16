package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readDcodeMCP(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return root
}

func TestMergeDcodeMCP_CreatesFresh(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".mcp.json")
	if err := mergeDcodeMCP(path); err != nil {
		t.Fatalf("mergeDcodeMCP: %v", err)
	}
	got := readDcodeMCP(t, path)
	cmd, _ := got["mcpServers"].(map[string]any)["c3"].(map[string]any)["command"].(string)
	if cmd != "c3-dcode-adapter" {
		t.Fatalf("c3.command = %q, want c3-dcode-adapter", cmd)
	}
}

func TestMergeDcodeMCP_PreservesOtherServersAndKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".mcp.json")
	initial := []byte(`{
  "mcpServers": {
    "fs": {"command": "fs-server"},
    "c3": {"command": "c3-claude-adapter"}
  },
  "models": {"auto_classifier": "zai:glm-5.2"}
}
`)
	if err := os.WriteFile(path, initial, 0644); err != nil {
		t.Fatal(err)
	}
	if err := mergeDcodeMCP(path); err != nil {
		t.Fatalf("mergeDcodeMCP: %v", err)
	}
	got := readDcodeMCP(t, path)
	servers := got["mcpServers"].(map[string]any)
	if cmd, _ := servers["c3"].(map[string]any)["command"].(string); cmd != "c3-dcode-adapter" {
		t.Fatalf("c3.command = %q, want c3-dcode-adapter (must replace any prior entry)", cmd)
	}
	fs, ok := servers["fs"].(map[string]any)
	if !ok || fs["command"] != "fs-server" {
		t.Fatalf("fs server corrupted: %#v", servers["fs"])
	}
	models, ok := got["models"].(map[string]any)
	if !ok || models["auto_classifier"] != "zai:glm-5.2" {
		t.Fatalf("unrelated top-level key corrupted: %#v", got["models"])
	}
}

// The skills ARE the slash commands in dcode (no plugin command component):
// install must write SKILL.md bodies whose frontmatter parses with the name
// matching the directory, and whose prompts reference the server-prefixed
// MCP tool names (c3_attach / c3_fetch_queue / c3_topics).
func TestInstallDcodeSkills_WritesSkillCommands(t *testing.T) {
	dir := t.TempDir()
	if err := installDcodeSkills(dir); err != nil {
		t.Fatalf("installDcodeSkills: %v", err)
	}
	for _, name := range []string{"c3-attach", "c3-fetch", "c3-topics"} {
		path := filepath.Join(dir, name, "SKILL.md")
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("skill %s missing SKILL.md: %v", name, err)
		}
		text := string(body)
		if !strings.HasPrefix(text, "---\nname: "+name+"\n") {
			t.Errorf("skill %s: frontmatter must start with `name: %s`, got: %.60q", name, name, text)
		}
		if !strings.Contains(text, "description:") {
			t.Errorf("skill %s: missing description (it IS the trigger surface)", name)
		}
	}
	attach, _ := os.ReadFile(filepath.Join(dir, "c3-attach", "SKILL.md"))
	if !strings.Contains(string(attach), "`c3_attach`") {
		t.Errorf("c3-attach must reference the server-prefixed tool name c3_attach")
	}
	fetch, _ := os.ReadFile(filepath.Join(dir, "c3-fetch", "SKILL.md"))
	if !strings.Contains(string(fetch), "`c3_fetch_queue`") {
		t.Errorf("c3-fetch must reference the server-prefixed tool name c3_fetch_queue")
	}
	topics, _ := os.ReadFile(filepath.Join(dir, "c3-topics", "SKILL.md"))
	if !strings.Contains(string(topics), "`c3_topics`") {
		t.Errorf("c3-topics must reference the server-prefixed tool name c3_topics")
	}
}

// Idempotent re-install must converge (install owns the text; the user does
// not edit C3's own skill files).
func TestInstallDcodeSkills_Idempotent(t *testing.T) {
	dir := t.TempDir()
	if err := installDcodeSkills(dir); err != nil {
		t.Fatalf("first install: %v", err)
	}
	first, err := os.ReadFile(filepath.Join(dir, "c3-fetch", "SKILL.md"))
	if err != nil {
		t.Fatal(err)
	}
	if err := installDcodeSkills(dir); err != nil {
		t.Fatalf("second install: %v", err)
	}
	second, err := os.ReadFile(filepath.Join(dir, "c3-fetch", "SKILL.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("re-install changed the skill body — must be idempotent overwrite")
	}
}
