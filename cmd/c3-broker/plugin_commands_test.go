package main

import (
	"os"
	"strings"
	"testing"
)

// Claude Code runs a command file's shell line before the model sees it only
// in the !`cmd` form, and only when allowed-tools permits that exact command.
// A bare "!cmd" line is plain text, left for the model to run (or not).
func TestPluginCommandsPreRunTheirBrokerCommand(t *testing.T) {
	for file, command := range map[string]string{
		"pair.md":     "c3-broker pair $ARGUMENTS",
		"ping.md":     "c3-broker ping",
		"sessions.md": "c3-broker sessions",
		"status.md":   "c3-broker status",
		"topics.md":   "c3-broker topics",
		"update.md":   "c3-broker update",
	} {
		data, err := os.ReadFile("../../plugins/c3/commands/" + file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		frontmatter, _, _ := strings.Cut(strings.TrimPrefix(text, "---\n"), "\n---\n")
		allowed := "Bash(" + command + ")"
		if file == "pair.md" {
			allowed = "Bash(c3-broker pair:*)"
		}
		if !strings.Contains(frontmatter, "\nallowed-tools: "+allowed) {
			t.Errorf("%s: frontmatter does not allow %s", file, allowed)
		}
		if !strings.Contains(text, "\n!`"+command+"`\n") {
			t.Errorf("%s: %q is not in the pre-run !`…` form", file, command)
		}
		if strings.Contains(text, "\n!"+command+"\n") {
			t.Errorf("%s: still has the bare %q line, which never runs", file, "!"+command)
		}
	}
}
