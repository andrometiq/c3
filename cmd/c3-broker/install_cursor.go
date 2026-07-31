package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Claude-plugin MCP ids Cursor synthesizes when it also loads ~/.claude/plugins.
// Leaving them enabled next to mcp.json's c3 → c3-cursor-adapter yields two C3
// servers and a welcome line that says "claude".
var cursorClaudePluginMCPIds = []string{
	"plugin-c3-c3",
	"c3@c3",
	"plugin:c3@c3",
}

// runInstallCursor configures the host for C3 × Cursor Agent CLI:
//   - ensures ~/.cursor exists
//   - merges mcpServers.c3 → c3-cursor-adapter into ~/.cursor/mcp.json
//     (preserves every other server)
//   - disables Claude-plugin C3 MCP ids in known Cursor project mcp-disabled.json
//   - prints verification steps
//
// Cursor has no Claude-style channel push and no idle-wake API, so this
// adapter is poll-only: inbound stays in the durable queue until the agent
// calls fetch_queue. Do not point Cursor at c3-claude-adapter — that host
// reports render-capable and can black-hole inbound.
func runInstallCursor(args []string) error {
	_ = args
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	return installCursorAt(filepath.Join(home, ".cursor"), home)
}

func installCursorAt(cursorDir, home string) error {
	if err := os.MkdirAll(cursorDir, 0700); err != nil {
		return fmt.Errorf("failed to create %s: %w", cursorDir, err)
	}

	mcpPath := filepath.Join(cursorDir, "mcp.json")
	if err := mergeCursorMCP(mcpPath); err != nil {
		return err
	}

	disabledN := disableClaudePluginC3InProjects(filepath.Join(cursorDir, "projects"))

	if err := installCursorFetchCommands(filepath.Join(cursorDir, "commands")); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not install Cursor slash commands: %v\n", err)
	} else {
		fmt.Printf("slash commands: %s/{fetch,c3-fetch}.md\n", filepath.Join(cursorDir, "commands"))
	}

	fmt.Printf("Successfully registered C3 for Cursor Agent CLI at:\n  %s\n\n", mcpPath)

	if p, err := lookPath("c3-cursor-adapter"); err != nil {
		fmt.Fprintf(os.Stderr, "warning: c3-cursor-adapter not on PATH — run: make build && make install\n")
	} else {
		fmt.Printf("adapter: %s\n", p)
	}
	if p, err := lookPath("c3-broker"); err != nil {
		fmt.Fprintf(os.Stderr, "warning: c3-broker not on PATH — run: make build && make install\n")
	} else {
		fmt.Printf("broker:  %s\n", p)
	}

	if claudePluginC3Installed(home) {
		fmt.Println()
		fmt.Println("Note: Cursor also loads Claude Code plugins from ~/.claude/plugins.")
		fmt.Println("Your installed c3@c3 Claude plugin would spawn c3-claude-adapter")
		fmt.Println("alongside c3-cursor-adapter (two C3 entries in /mcp; welcome says claude).")
		if disabledN > 0 {
			fmt.Printf("Disabled Claude-plugin C3 MCP ids in %d Cursor project dir(s).\n", disabledN)
		}
		fmt.Println("If /mcp still shows two C3s after restart:")
		fmt.Println("  agent mcp disable plugin-c3-c3")
		fmt.Println("  agent mcp enable c3")
	}

	fmt.Println("\nNext:")
	fmt.Println("  # Approve the server once if prompted:")
	fmt.Println("  agent mcp enable c3")
	fmt.Println("  # Restart Cursor Agent CLI in a project:")
	fmt.Println("  agent")
	fmt.Println("  # in session: attach name=<topic>")
	fmt.Println("  # inbound is poll-only — /fetch-queue (MCP prompt), /c3-fetch, or fetch_queue")
	fmt.Println()
	fmt.Println("Welcome should show: 🤖 cursor → <topic>  (not claude).")
	fmt.Println()
	return nil
}

// cursorFetchCommandMarkdown is the body of ~/.cursor/commands/{fetch,c3-fetch}.md.
// Cursor Agent CLI's slash menu does not reliably parse YAML frontmatter for
// commands — a leading "---" shows up as the menu description ("---"). Lead
// with a `#` title + one-line summary instead (same idea as Cursor's own
// command examples). Kept in lockstep with plugins/c3/commands/fetch.md.
var cursorFetchCommandMarkdown = strings.TrimSpace(`
# Drain C3 Telegram queue

Drain held inbound for this session — bare = all; optional N = oldest N.

Pull the held inbound Telegram messages for this session's attached C3 topic.

1. Prefer the MCP prompt named fetch-queue on server c3 if the host surfaces MCP prompts as slash/get-prompt — that path drains the queue deterministically and injects the messages. Pass limit from $ARGUMENTS when present.
2. Otherwise call the MCP tool fetch_queue on server c3 (not plugin-c3-c3):
   - No argument → limit: "all", ack: true
   - Positive integer N in $ARGUMENTS → limit: N, ack: true
3. Render each message (sender, reply-to, text/transcript, attachments with file_id). State how many remain. If empty, one line. If not attached, show the error and point at attach. No other commentary.

$ARGUMENTS
`) + "\n"

// installCursorFetchCommands writes fetch.md and c3-fetch.md under commandsDir
// so Cursor Agent CLI / IDE list /fetch and /c3-fetch. Idempotent overwrite —
// the text is owned by C3 install, not the user.
func installCursorFetchCommands(commandsDir string) error {
	if err := os.MkdirAll(commandsDir, 0700); err != nil {
		return err
	}
	for _, name := range []string{"fetch.md", "c3-fetch.md"} {
		path := filepath.Join(commandsDir, name)
		if err := os.WriteFile(path, []byte(cursorFetchCommandMarkdown), 0644); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	return nil
}

func claudePluginC3Installed(home string) bool {
	// installed_plugins.json or the on-disk cache both count.
	paths := []string{
		filepath.Join(home, ".claude", "plugins", "installed_plugins.json"),
		filepath.Join(home, ".claude", "plugins", "cache", "c3"),
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
}

// disableClaudePluginC3InProjects merges cursorClaudePluginMCPIds into every
// existing <projects>/*/mcp-disabled.json (and creates the file when the
// project dir already exists). Best-effort — missing projects dir is fine.
func disableClaudePluginC3InProjects(projectsDir string) int {
	entries, err := os.ReadDir(projectsDir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(projectsDir, e.Name(), "mcp-disabled.json")
		if err := mergeMCPDisabled(path, cursorClaudePluginMCPIds); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not update %s: %v\n", path, err)
			continue
		}
		n++
	}
	return n
}

func mergeMCPDisabled(path string, ids []string) error {
	var list []string
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(data) > 0 {
			if err := json.Unmarshal(data, &list); err != nil {
				return fmt.Errorf("parse: %w", err)
			}
		}
	case os.IsNotExist(err):
		// create fresh
	default:
		return err
	}
	have := map[string]bool{}
	for _, id := range list {
		have[id] = true
	}
	changed := false
	for _, id := range ids {
		if !have[id] {
			list = append(list, id)
			have[id] = true
			changed = true
		}
	}
	if !changed && err == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	out, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	out = append(out, '\n')
	return os.WriteFile(path, out, 0644)
}

// mergeCursorMCP writes mcpServers.c3.command = c3-cursor-adapter into path,
// creating a minimal file when absent and preserving other servers / keys.
func mergeCursorMCP(path string) error {
	root := map[string]any{}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(data) > 0 {
			if err := json.Unmarshal(data, &root); err != nil {
				return fmt.Errorf("parse %s: %w (fix JSON or move the file aside)", path, err)
			}
		}
	case os.IsNotExist(err):
		// fresh file
	default:
		return fmt.Errorf("read %s: %w", path, err)
	}

	servers, _ := root["mcpServers"].(map[string]any)
	if servers == nil {
		servers = map[string]any{}
		root["mcpServers"] = servers
	}
	servers["c3"] = map[string]any{
		"command": "c3-cursor-adapter",
	}

	out, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return err
	}
	out = append(out, '\n')
	if err := os.WriteFile(path, out, 0644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
