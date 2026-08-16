package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// runInstallDcode configures the host for C3 × dcode (deepagents-code):
//   - merges mcpServers.c3 → c3-dcode-adapter into ~/.deepagents/.mcp.json
//     (preserves every other server)
//   - installs /skill:c3-attach, /skill:c3-fetch, /skill:c3-topics slash
//     commands as dcode user skills (dcode has no plugin command component;
//     user skills ARE its slash-command surface — see installDcodeSkills)
//   - prints verification steps
//
// dcode discovers MCP servers from ~/.deepagents/.mcp.json (user-level),
// <project>/.deepagents/.mcp.json, and <project>/.mcp.json; user-level is
// the install target so every project gets the bridge.
//
// Live inbound needs the TUI launched with
// DEEPAGENTS_CODE_EXTERNAL_EVENT_SOCKET=1 (env-only; there is no config.toml
// key for it), which the printed instructions cover. Without it the adapter
// runs pull-only via fetch_queue.
func runInstallDcode(args []string) error {
	_ = args
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	return installDcodeAt(filepath.Join(home, ".deepagents"))
}

func installDcodeAt(deepagentsDir string) error {
	if err := os.MkdirAll(deepagentsDir, 0700); err != nil {
		return fmt.Errorf("failed to create %s: %w", deepagentsDir, err)
	}

	mcpPath := filepath.Join(deepagentsDir, ".mcp.json")
	if err := mergeDcodeMCP(mcpPath); err != nil {
		return err
	}

	if err := installDcodeSkills(filepath.Join(deepagentsDir, "agent", "skills")); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not install dcode slash skills: %v\n", err)
	} else {
		fmt.Printf("slash commands: %s/c3-{attach,fetch,topics}/SKILL.md → /skill:c3-attach, /skill:c3-fetch, /skill:c3-topics\n",
			filepath.Join(deepagentsDir, "agent", "skills"))
	}

	fmt.Printf("Successfully registered C3 for dcode at:\n  %s\n\n", mcpPath)

	if p, err := lookPath("c3-dcode-adapter"); err != nil {
		fmt.Fprintf(os.Stderr, "warning: c3-dcode-adapter not on PATH — run: make build && make install\n")
	} else {
		fmt.Printf("adapter: %s\n", p)
	}
	if p, err := lookPath("c3-broker"); err != nil {
		fmt.Fprintf(os.Stderr, "warning: c3-broker not on PATH — run: make build && make install\n")
	} else {
		fmt.Printf("broker:  %s\n", p)
	}

	fmt.Println("\nLive inbound (recommended; Linux only — the adapter binds the TUI's")
	fmt.Println("event socket via a /proc ancestor walk, which macOS does not have):")
	fmt.Println("  DEEPAGENTS_CODE_EXTERNAL_EVENT_SOCKET=1 dcode")
	fmt.Println("The adapter finds the TUI's events-<pid>.sock automatically and Telegram")
	fmt.Println("messages arrive in the conversation as [Telegram] user turns.")
	fmt.Println()
	fmt.Println("Without that flag the adapter runs pull-only: messages wait in C3's")
	fmt.Println("durable queue and the agent drains them with the fetch_queue tool.")
	fmt.Println()
	fmt.Println("Note: project-level .mcp.json files are untrusted by default in dcode —")
	fmt.Println("approve the c3 server if prompted, or use /mcp to enable it.")
	fmt.Println()
	fmt.Println("Next:")
	fmt.Println("  dcode   # in a project dir")
	fmt.Println("  # in session: attach name=<topic>  (or /skill:c3-attach <topic>)")
	fmt.Println("  # slash commands: /skill:c3-attach [topic] · /skill:c3-fetch [n] · /skill:c3-topics")
	fmt.Println()
	return nil
}

// dcodeSkill is one installable C3 slash skill.
type dcodeSkill struct {
	name string
	body string
}

// dcodeSkills are the C3 verbs worth a keyboard shortcut in dcode, written
// as thin prompt wrappers over the c3 MCP tools (dcode prefixes MCP tool
// names with the server: c3_attach, c3_fetch_queue, c3_topics). dcode has no
// plugin command component — user skills at ~/.deepagents/agent/skills/ are
// its slash-command surface (/skill:<name> [args], autocomplete included) —
// so install-dcode writes these three, mirroring install-cursor's
// ~/.cursor/commands/ precedent. Kept in lockstep with plugins/c3/commands/.
var dcodeSkills = []dcodeSkill{
	{
		name: "c3-attach",
		body: `---
name: c3-attach
description: Attach this dcode session to a C3 Telegram topic. Empty = silently re-attach this session's own topic, or (first time) show a picker. "dm" = actual DM. "<int>" = topic_id. "<name>" = topic by name. "create <name>" = create that topic immediately.
---

User typed: $ARGUMENTS

Call the MCP tool ` + "`c3_attach`" + ` with ` + "`expr`" + ` set to the user's argument string ("$ARGUMENTS" verbatim; empty string when bare). The broker parses it (rules in C3's docs/COMMANDS.md) and either silent-claims the topic or returns a proposal (` + "`create`" + ` / ` + "`pick_topic`" + ` / ` + "`use_existing_other_group`" + ` / ` + "`disambiguate_dm`" + ` / ` + "`force_steal`" + `) requiring confirmation.

If the response is **needs_confirmation**: ask the user which option in plain conversation (never auto-pick, never assume, never attach silently), then re-invoke ` + "`c3_attach`" + ` with the exact arguments shown on the chosen option's line. ` + "`force_steal`" + ` proposals additionally need an explicit yes before passing ` + "`steal=true`" + `.

For a successful attach: display the broker's response as-is. If a backlog notice follows (N message(s) held), mention that ` + "`/skill:c3-fetch`" + ` drains them. For errors that are not proposals: display the error verbatim — the most common is "no telegram bot_token configured", which points at ` + "`c3-broker setup`" + ` from a shell.`,
	},
	{
		name: "c3-fetch",
		body: `---
name: c3-fetch
description: Drain this session's C3 Telegram queue — bare drains all held messages; optional number N fetches the oldest N. Use after "N pending — call fetch_queue" nudges or when asked to check Telegram.
---

Pull the held inbound Telegram messages for this session's attached topic by calling the MCP tool ` + "`c3_fetch_queue`" + `, then render them.

- Argument (optional): $ARGUMENTS
- No argument → call it with ` + "`limit: \"all\"`" + ` — drain the entire queue in one shot.
- Argument is a positive integer N → call it with ` + "`limit: N`" + ` — fetch the oldest N.
- Always use ` + "`ack: true`" + ` (the default) so fetched messages are consumed.

Render each message: sender, reply-to (if any), the text/transcript, and note any attachment (with its file_id — ` + "`c3_download_attachment`" + ` fetches the bytes). State how many remain in the queue. If the queue is empty, say so in one line. If the call errors (e.g. this session isn't attached), show the error verbatim and point at ` + "`/skill:c3-attach`" + `. No other commentary.`,
	},
	{
		name: "c3-topics",
		body: `---
name: c3-topics
description: List known C3 Telegram topics across all groups, with which session (if any) currently claims each. Use to find a topic id before attaching.
---

Call the MCP tool ` + "`c3_topics`" + ` and display its output verbatim. No commentary.`,
	},
}

// installDcodeSkills writes the C3 slash skills under skillsDir (expected
// ~/.deepagents/agent/skills). Idempotent overwrite — the text is owned by
// C3 install, not the user. The directory is a trusted skill root in dcode,
// so no trust prompt fires on /skill: invocation.
func installDcodeSkills(skillsDir string) error {
	if err := os.MkdirAll(skillsDir, 0700); err != nil {
		return err
	}
	for _, s := range dcodeSkills {
		dir := filepath.Join(skillsDir, s.name)
		if err := os.MkdirAll(dir, 0700); err != nil {
			return fmt.Errorf("%s: %w", dir, err)
		}
		path := filepath.Join(dir, "SKILL.md")
		if err := os.WriteFile(path, []byte(s.body), 0644); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	return nil
}

// mergeDcodeMCP writes mcpServers.c3.command = c3-dcode-adapter into path,
// creating a minimal file when absent and preserving other servers / keys.
// Mirrors mergeCursorMCP's shape discipline.
func mergeDcodeMCP(path string) error {
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
		"command": "c3-dcode-adapter",
	}

	out, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return err
	}
	out = append(out, '\n')
	if err := os.WriteFile(path, out, 0600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
