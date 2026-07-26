package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/channel/telegram"
	"github.com/Andrometiq/c3/internal/mode"
)

// HostCLI is the discovered identity of the CLI driving setup.
//
// This drives two install-flow decisions:
//   - the restart instruction printed at the end of `c3-broker setup` (#8)
//   - whether Codex's persistent MCP config gets a c3 entry written into
//     `~/.codex/config.toml` (#9)
//
// We default to HostClaude when nothing matches — that's the install path
// that's been around longest and is most likely to be correct if the user
// just ran `c3-broker setup` from a plain shell.
type HostCLI int

const (
	HostUnknown HostCLI = iota
	HostClaude
	HostCodex
)

// String returns a short, lowercase identifier suitable for log lines.
func (h HostCLI) String() string {
	switch h {
	case HostClaude:
		return "claude"
	case HostCodex:
		return "codex"
	default:
		return "unknown"
	}
}

// DetectHostCLI inspects the environment to identify the CLI that spawned
// the current setup invocation. Detection order, most specific first:
//
//  1. $C3_HOST_CLI explicit override ("claude" | "codex"). Lets the user
//     force a path during testing or when a wrapper script invokes setup.
//  2. $CLAUDECODE=1 or $CLAUDE_PLUGIN_ROOT set ⇒ Claude Code. Claude Code
//     exports CLAUDECODE=1 to every subprocess and CLAUDE_PLUGIN_ROOT to
//     plugin-invoked subprocesses.
//  3. $CODEX_HOME set ⇒ Codex. (Codex doesn't always export a marker env
//     variable, but $CODEX_HOME is the documented config-dir override and
//     is the closest analogue to CLAUDE_PLUGIN_ROOT.)
//  4. Default: HostClaude. Historical default — the install flow originated
//     with Claude Code and a plain-shell run of `c3-broker setup` is most
//     plausibly part of a Claude Code session.
//
// Returns HostUnknown only if explicit override is set to an unrecognized
// value — callers should treat HostUnknown the same as HostClaude for
// printing restart instructions, but skip the Codex-specific MCP write.
func DetectHostCLI() HostCLI {
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("C3_HOST_CLI"))); v != "" {
		switch v {
		case "claude", "claude-code", "claudecode":
			return HostClaude
		case "codex":
			return HostCodex
		default:
			return HostUnknown
		}
	}
	if os.Getenv("CLAUDECODE") == "1" || os.Getenv("CLAUDE_PLUGIN_ROOT") != "" {
		return HostClaude
	}
	if os.Getenv("CODEX_HOME") != "" {
		return HostCodex
	}
	return HostClaude
}

// ensureCodexMCPRegistration writes a [mcp_servers.c3_codex] block into
// $CODEX_HOME/config.toml (defaults to ~/.codex/config.toml) if no such
// section is already present. Idempotent: presence is detected by a line
// match on the section header.
//
// We intentionally don't use `codex mcp add` from setup — it requires the
// `codex` binary to be on PATH and adds another failure mode (binary
// missing, version mismatch, interactive prompt). A direct append to the
// TOML file is the same operation Codex itself would perform and gives us
// deterministic behavior under all install conditions.
//
// Returns (path, didWrite, err). didWrite=false on idempotent skip.
func ensureCodexMCPRegistration() (string, bool, error) {
	configPath, err := codexConfigPath()
	if err != nil {
		return "", false, err
	}
	existing, err := os.ReadFile(configPath)
	if err != nil && !os.IsNotExist(err) {
		return configPath, false, fmt.Errorf("read %s: %w", configPath, err)
	}
	if containsCodexC3Section(string(existing)) {
		return configPath, false, nil
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		return configPath, false, fmt.Errorf("mkdir %s: %w", filepath.Dir(configPath), err)
	}

	// Append-only — never rewrite existing config. We prefix with a blank
	// line so we don't accidentally extend a trailing TOML table.
	block := codexC3MCPBlock()
	var toWrite string
	if len(existing) == 0 {
		toWrite = block
	} else if strings.HasSuffix(string(existing), "\n\n") {
		toWrite = block
	} else if strings.HasSuffix(string(existing), "\n") {
		toWrite = "\n" + block
	} else {
		toWrite = "\n\n" + block
	}

	f, err := os.OpenFile(configPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return configPath, false, fmt.Errorf("open %s: %w", configPath, err)
	}
	defer f.Close()
	if _, err := f.WriteString(toWrite); err != nil {
		return configPath, false, fmt.Errorf("write %s: %w", configPath, err)
	}
	return configPath, true, nil
}

// codexConfigPath returns $CODEX_HOME/config.toml (or ~/.codex/config.toml
// fallback). Errors only if $HOME isn't resolvable AND $CODEX_HOME is
// unset.
func codexConfigPath() (string, error) {
	if home := os.Getenv("CODEX_HOME"); home != "" {
		return filepath.Join(home, "config.toml"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", fmt.Errorf("cannot resolve $HOME and $CODEX_HOME is unset")
	}
	return filepath.Join(home, ".codex", "config.toml"), nil
}

// containsCodexC3Section returns true if the config file already declares
// a c3_codex MCP server OR any of its descendant sub-tables
// (e.g. `[mcp_servers.c3_codex.tools.attach]`). We match the section
// header prefix `[mcp_servers.c3_codex` so a user-curated config that
// configures only sub-tables (a malformed but plausibly hand-edited
// case) still signals presence — appending a fresh parent block here
// would produce two-stanza confusion. We still avoid false-positives
// on comment lines via TrimSpace + the `[` prefix anchor.
//
// Closes report MINOR m4 (2026-05-19).
func containsCodexC3Section(s string) bool {
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "[mcp_servers.c3_codex") {
			continue
		}
		// Distinguish the exact parent header and the descendant table
		// headers from a coincidental substring (e.g. a TOML inline
		// table value that mentions the name). The line must continue
		// with either `]` (parent) or `.` (subtable).
		rest := trimmed[len("[mcp_servers.c3_codex"):]
		if strings.HasPrefix(rest, "]") || strings.HasPrefix(rest, ".") {
			return true
		}
	}
	return false
}

// codexC3MCPBlock is the TOML stanza appended to ~/.codex/config.toml.
// Mirrors what `codex mcp add c3_codex -- c3-codex-adapter` would write,
// minus the tools.* approval_mode entries (those are per-user policy
// decisions and we don't want to lock the user into "approve" if they
// prefer otherwise).
//
// The header comment carries a version + per-day date marker so a
// future the maintainer (or follow-up agent) can distinguish "this user
// upgraded from v0.1 → v0.2" from "user hand-edited". Stable per-day
// (UTC) — rerunning setup on the same day produces byte-identical
// output, which preserves the existing-section idempotency contract
// in ensureCodexMCPRegistration. Closes report NIT n2 (2026-05-19).
func codexC3MCPBlock() string {
	return fmt.Sprintf(`# c3 v0.1 — written by c3-broker setup on %s
[mcp_servers.c3_codex]
command = "c3-codex-adapter"
`, time.Now().UTC().Format("2006-01-02"))
}

// AGENTS.md installer (Codex protocol surfacing)
//
// Why: Codex's MCP host does NOT read the `instructions` field from the
// MCP initialize response (parallel-agent investigation 2026-05-18:
// openai/codex#6148 closed-not-planned; empirical session-rollout grep;
// binary symbol audit). Codex DOES read `~/.codex/AGENTS.md` and
// concatenates it into the developer_instructions block — that's the
// channel where the per-session OUTPUT MODE PROTOCOL has to land for
// Codex to honor it.
//
// The Claude Code adapter doesn't need this — Claude DOES read MCP
// initialize `instructions`, so the protocol travels with the plugin
// over the wire. For Codex, we install it once at setup time into the
// user's persistent agent-instructions file, idempotently.

// agentsMdBlockStart / agentsMdBlockEnd delimit the c3-managed block
// inside the user's AGENTS.md. We only touch lines BETWEEN these
// markers (inclusive) — everything else in the file is left alone. The
// markers are HTML comments so they're invisible in any markdown
// renderer that might display the file.
const (
	agentsMdBlockStart = "<!-- c3:protocol START -->"
	agentsMdBlockEnd   = "<!-- c3:protocol END -->"
)

// ensureCodexAgentsMd writes (or refreshes) the c3-managed protocol
// block in $CODEX_HOME/AGENTS.md (default ~/.codex/AGENTS.md).
//
// Idempotent semantics:
//   - if the file doesn't exist: create it with just the block.
//   - if the file exists and contains the delimited block: replace
//     ONLY the block body, leaving everything outside the markers
//     untouched. Re-running with no protocol-text change is a no-op
//     at the file level (same content written) — didWrite is still
//     true so the caller can log "refreshed."
//   - if the file exists but has no block: append the block (with a
//     blank-line separator from prior content).
//
// Parent dir is created with 0700 if missing.
//
// Returns (path, didWrite, err). didWrite=false ONLY when the file
// already has the block AND the body matches exactly. Callers can
// surface a quieter "already up to date" message in that case.
func ensureCodexAgentsMd() (string, bool, error) {
	path, err := codexAgentsMdPath()
	if err != nil {
		return "", false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return path, false, fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}

	body := codexAgentsMdBlock()
	existing, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return path, false, fmt.Errorf("read %s: %w", path, err)
	}

	var next string
	if len(existing) == 0 {
		next = body
	} else {
		replaced, found := replaceCodexAgentsMdBlock(string(existing), body)
		if found {
			next = replaced
		} else {
			next = appendCodexAgentsMdBlock(string(existing), body)
		}
	}

	if string(existing) == next {
		return path, false, nil
	}
	if err := os.WriteFile(path, []byte(next), 0o600); err != nil {
		return path, false, fmt.Errorf("write %s: %w", path, err)
	}
	return path, true, nil
}

// codexAgentsMdPath returns $CODEX_HOME/AGENTS.md (or ~/.codex/AGENTS.md
// fallback). Errors only if $HOME isn't resolvable AND $CODEX_HOME is
// unset.
func codexAgentsMdPath() (string, error) {
	if home := os.Getenv("CODEX_HOME"); home != "" {
		return filepath.Join(home, "AGENTS.md"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", fmt.Errorf("cannot resolve $HOME and $CODEX_HOME is unset")
	}
	return filepath.Join(home, ".codex", "AGENTS.md"), nil
}

// codexAgentsMdBlock returns the full delimited markdown block we
// install into AGENTS.md. The body is sourced from internal/mode so
// the Claude MCP-initialize path and the Codex AGENTS.md path quote
// identical protocol text (TODO #20 single-source-of-truth audit).
//
// Note: mode.Combined() opens with "\n\n" (it splices onto Claude's
// head text). We strip that here — the AGENTS.md block already
// provides its own structural newlines around the markers, and a
// double-newline-at-top would render as an empty paragraph in any
// markdown viewer.
func codexAgentsMdBlock() string {
	// Setup runs with NO live broker/connection, so caps come from the static
	// channel literal. telegram.New() merely allocates (it does not dial the
	// Bot API), making Capabilities() a pure literal here — same manifest the
	// live broker would deliver over hello_ack, so the AGENTS.md guidance
	// matches what Claude gets at MCP-initialize (Codex/Claude parity).
	caps := telegram.New().Capabilities()
	body := strings.TrimLeft(mode.Combined(caps), "\n")
	return agentsMdBlockStart + "\n" +
		"<!-- c3 manages this block. Do not edit between markers; rerun `c3-broker setup` to refresh. -->\n\n" +
		body + "\n\n" +
		agentsMdBlockEnd
}

// replaceCodexAgentsMdBlock substitutes the body between the markers
// with a fresh block. Returns (result, true) if the markers were
// found, (existing, false) otherwise. Tolerates leading whitespace on
// the marker lines (a user editing the file might indent the markers
// inside a list item).
//
// We match on the first occurrence of each marker — if a user
// duplicated the markers we leave the second pair alone. Belt-and-
// suspenders: in practice nobody hand-duplicates HTML comments.
func replaceCodexAgentsMdBlock(existing, newBlock string) (string, bool) {
	startIdx := strings.Index(existing, agentsMdBlockStart)
	if startIdx < 0 {
		return existing, false
	}
	endIdx := strings.Index(existing[startIdx:], agentsMdBlockEnd)
	if endIdx < 0 {
		// Start marker without an end marker — corrupt block. Don't
		// rewrite blindly; treat as "not found" so the appender takes
		// over. (Appender will produce a duplicate-marker file, which
		// is loud and recoverable; silent overwrite is not.)
		return existing, false
	}
	endIdx += startIdx + len(agentsMdBlockEnd)
	return existing[:startIdx] + newBlock + existing[endIdx:], true
}

// appendCodexAgentsMdBlock appends the block to existing content with
// a blank-line separator so it doesn't fuse onto a prior paragraph or
// list item.
func appendCodexAgentsMdBlock(existing, block string) string {
	switch {
	case existing == "":
		return block
	case strings.HasSuffix(existing, "\n\n"):
		return existing + block
	case strings.HasSuffix(existing, "\n"):
		return existing + "\n" + block
	default:
		return existing + "\n\n" + block
	}
}
