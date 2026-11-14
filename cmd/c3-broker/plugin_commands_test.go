package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const pluginCommandsDir = "../../plugins/c3/commands/"

// commandFile splits a plugin command file into its frontmatter lines and body.
func commandFile(t *testing.T, name string) ([]string, string) {
	t.Helper()
	data, err := os.ReadFile(pluginCommandsDir + name)
	if err != nil {
		t.Fatal(err)
	}
	frontmatter, body, ok := strings.Cut(strings.TrimPrefix(string(data), "---\n"), "\n---\n")
	if !ok {
		t.Fatalf("%s: no frontmatter", name)
	}
	return strings.Split(frontmatter, "\n"), body
}

func hasLine(lines []string, want string) bool {
	for _, line := range lines {
		if line == want {
			return true
		}
	}
	return false
}

// Claude Code runs a command file's shell line before the model sees it only
// in the !`cmd` form, and only when allowed-tools permits that exact command.
// A failed pre-run stops the command, so only commands whose failure output
// speaks for itself are pre-run.
func TestPluginCommandsPreRunTheirBrokerCommand(t *testing.T) {
	for file, command := range map[string]string{
		"ping.md":     "c3-broker ping",
		"sessions.md": "c3-broker sessions",
		"status.md":   "c3-broker status",
		"topics.md":   "c3-broker topics",
	} {
		frontmatter, body := commandFile(t, file)
		if !hasLine(frontmatter, "allowed-tools: Bash("+command+")") {
			t.Errorf("%s: frontmatter does not allow exactly Bash(%s)", file, command)
		}
		if !strings.Contains(body, "\n!`"+command+"`\n") {
			t.Errorf("%s: %q is not in the pre-run !`…` form", file, command)
		}
	}
}

// /c3:update and /c3:pair change state (binaries and broker; the allowlist), so
// they run only when the user types them, and the agent runs the command so it
// reads the recovery guidance on failure.
func TestPluginCommandsWithSideEffectsAreTypedOnly(t *testing.T) {
	for file, allowed := range map[string]string{
		"update.md": "Bash(c3-broker update)",
		"pair.md":   "Bash(c3-broker pair:*)",
	} {
		frontmatter, body := commandFile(t, file)
		if !hasLine(frontmatter, "disable-model-invocation: true") {
			t.Errorf("%s: the model can invoke it", file)
		}
		if !hasLine(frontmatter, "allowed-tools: "+allowed) {
			t.Errorf("%s: frontmatter does not allow exactly %s", file, allowed)
		}
		if strings.Contains(body, "\n!") {
			t.Errorf("%s: has a pre-run line; its failure guidance would never be read", file)
		}
	}
}

// A bare "!cmd" line is plain text that reads as if it already ran.
func TestPluginCommandsHaveNoBareBangLines(t *testing.T) {
	files, err := filepath.Glob(pluginCommandsDir + "*.md")
	if err != nil || len(files) == 0 {
		t.Fatalf("no command files: %v", err)
	}
	for _, path := range files {
		_, body := commandFile(t, filepath.Base(path))
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, "!") && !strings.HasPrefix(line, "!`") {
				t.Errorf("%s: bare %q never runs", filepath.Base(path), line)
			}
		}
	}
}
