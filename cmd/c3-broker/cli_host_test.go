package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// clearHostEnv unsets every env var DetectHostCLI consults so a test
// starts from a known empty baseline. Used at the top of each Detect*
// test because Go test binaries inherit the developer's shell env.
func clearHostEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"C3_HOST_CLI", "CLAUDECODE", "CLAUDE_PLUGIN_ROOT", "CODEX_HOME"} {
		t.Setenv(k, "")
		_ = os.Unsetenv(k)
	}
}

func TestDetectHostCLI_ExplicitOverride(t *testing.T) {
	for _, tc := range []struct {
		val  string
		want HostCLI
	}{
		{"claude", HostClaude},
		{"claude-code", HostClaude},
		{"CLAUDECODE", HostClaude},
		{"codex", HostCodex},
		{"Codex", HostCodex},
		{"  codex  ", HostCodex},
		{"nonsense", HostUnknown},
	} {
		t.Run(tc.val, func(t *testing.T) {
			clearHostEnv(t)
			t.Setenv("C3_HOST_CLI", tc.val)
			if got := DetectHostCLI(); got != tc.want {
				t.Errorf("DetectHostCLI($C3_HOST_CLI=%q)=%v, want %v", tc.val, got, tc.want)
			}
		})
	}
}

func TestDetectHostCLI_ClaudeFromEnv(t *testing.T) {
	clearHostEnv(t)
	t.Setenv("CLAUDECODE", "1")
	if got := DetectHostCLI(); got != HostClaude {
		t.Errorf("CLAUDECODE=1 → %v, want HostClaude", got)
	}

	clearHostEnv(t)
	t.Setenv("CLAUDE_PLUGIN_ROOT", "/tmp/plugin")
	if got := DetectHostCLI(); got != HostClaude {
		t.Errorf("CLAUDE_PLUGIN_ROOT set → %v, want HostClaude", got)
	}
}

func TestDetectHostCLI_CodexFromEnv(t *testing.T) {
	clearHostEnv(t)
	t.Setenv("CODEX_HOME", "/tmp/codex")
	if got := DetectHostCLI(); got != HostCodex {
		t.Errorf("CODEX_HOME set → %v, want HostCodex", got)
	}
}

func TestDetectHostCLI_DefaultIsClaude(t *testing.T) {
	clearHostEnv(t)
	if got := DetectHostCLI(); got != HostClaude {
		t.Errorf("no env set → %v, want HostClaude (historical default)", got)
	}
}

// ensureCodexMCPRegistration

func TestEnsureCodexMCPRegistration_CreatesFileAndAppendsBlock(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CODEX_HOME", dir)

	path, didWrite, err := ensureCodexMCPRegistration()
	if err != nil {
		t.Fatalf("ensureCodexMCPRegistration: %v", err)
	}
	if !didWrite {
		t.Fatal("expected didWrite=true on fresh config")
	}
	if path != filepath.Join(dir, "config.toml") {
		t.Errorf("path=%s, want %s/config.toml", path, dir)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "[mcp_servers.c3_codex]") {
		t.Errorf("written file lacks [mcp_servers.c3_codex]:\n%s", got)
	}
	if !strings.Contains(string(got), `command = "c3-codex-adapter"`) {
		t.Errorf("written file lacks command line:\n%s", got)
	}
}

func TestEnsureCodexMCPRegistration_Idempotent(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CODEX_HOME", dir)
	path := filepath.Join(dir, "config.toml")

	// Pre-existing config containing an unrelated MCP server plus
	// c3_codex. Format mirrors the user's actual ~/.codex/config.toml
	// observed during dev (with .tools.* sub-tables).
	existing := `[mcp_servers.openaiDeveloperDocs]
url = "https://developers.openai.com/mcp"

[mcp_servers.c3_codex]
command = "c3-codex-adapter"

[mcp_servers.c3_codex.tools.attach]
approval_mode = "approve"
`
	if err := os.WriteFile(path, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}

	got, didWrite, err := ensureCodexMCPRegistration()
	if err != nil {
		t.Fatalf("ensureCodexMCPRegistration: %v", err)
	}
	if didWrite {
		t.Error("expected didWrite=false when c3_codex section already exists")
	}
	if got != path {
		t.Errorf("path=%s, want %s", got, path)
	}

	after, _ := os.ReadFile(path)
	if string(after) != existing {
		t.Errorf("file was modified on idempotent path:\nwant:\n%s\ngot:\n%s", existing, after)
	}
}

func TestEnsureCodexMCPRegistration_AppendsBlankLineSeparator(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CODEX_HOME", dir)
	path := filepath.Join(dir, "config.toml")

	// Existing config with no trailing newline → block should still be
	// separated by a blank line so we don't fuse onto a prior table.
	existing := `[tui]
session_picker_view = "comfortable"`
	if err := os.WriteFile(path, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := ensureCodexMCPRegistration(); err != nil {
		t.Fatalf("ensureCodexMCPRegistration: %v", err)
	}
	got, _ := os.ReadFile(path)
	// Separator + header-comment + parent-table line must appear
	// consecutively. The comment now carries `# c3 vX.Y` (NIT n2,
	// 2026-05-19) so we anchor on the version marker rather than the
	// exact prior wording.
	if !strings.Contains(string(got), "[tui]\nsession_picker_view = \"comfortable\"\n\n# c3 v0.1") {
		t.Errorf("expected blank-line separator before new block (anchored on `# c3 v0.1` header); got:\n%s", got)
	}
	if !strings.Contains(string(got), "[mcp_servers.c3_codex]") {
		t.Errorf("appended file missing c3_codex header:\n%s", got)
	}
}

// TestCodexC3MCPBlock_HasVersionMarker pins the version + per-day-UTC
// date marker on the generated block. Reruns of setup on the same day
// must produce byte-identical output (preserves
// ensureCodexMCPRegistration's idempotency contract — once the
// `[mcp_servers.c3_codex]` header is detected we never overwrite).
// Closes report NIT n2 (2026-05-19).
func TestCodexC3MCPBlock_HasVersionMarker(t *testing.T) {
	block := codexC3MCPBlock()
	if !strings.HasPrefix(block, "# c3 v0.1 — written by c3-broker setup on ") {
		t.Errorf("block missing version-prefixed header; got:\n%s", block)
	}
	today := time.Now().UTC().Format("2006-01-02")
	if !strings.Contains(block, today) {
		t.Errorf("block missing today's UTC date %q; got:\n%s", today, block)
	}
	// Same-day reruns must be byte-identical (idempotency).
	again := codexC3MCPBlock()
	if again != block {
		t.Errorf("codexC3MCPBlock not stable within a single second:\nfirst:\n%s\nsecond:\n%s", block, again)
	}
}

func TestContainsCodexC3Section(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"[mcp_servers.c3_codex]", true},
		{"  [mcp_servers.c3_codex]  ", true},
		{"[mcp_servers.c3_codex]\ncommand = \"...\"", true},
		{"# [mcp_servers.c3_codex] in a comment", false}, // comment-only line
		{"[mcp_servers.other]", false},
		// Sub-table without parent — a user-curated file that only configures
		// `.tools.<x>` sections must still signal presence. Otherwise the
		// installer appends a fresh parent block, producing two-stanza
		// confusion (report MINOR m4, 2026-05-19).
		{"[mcp_servers.c3_codex.tools.attach]", true},
		{"  [mcp_servers.c3_codex.env]  ", true},
		// Sibling whose name STARTS with c3_codex but is a different server
		// must NOT match — anchor on `]` or `.` after the name.
		{"[mcp_servers.c3_codex2]", false},
		{"[mcp_servers.c3_codexLegacy]", false},
	}
	for _, c := range cases {
		if got := containsCodexC3Section(c.in); got != c.want {
			t.Errorf("containsCodexC3Section(%q)=%v, want %v", c.in, got, c.want)
		}
	}
}

// codexConfigPath

func TestCodexConfigPath_RespectsCODEX_HOME(t *testing.T) {
	t.Setenv("CODEX_HOME", "/custom/codex")
	got, err := codexConfigPath()
	if err != nil {
		t.Fatal(err)
	}
	if got != "/custom/codex/config.toml" {
		t.Errorf("got %s, want /custom/codex/config.toml", got)
	}
}

func TestCodexConfigPath_FallsBackToHome(t *testing.T) {
	_ = os.Unsetenv("CODEX_HOME")
	t.Setenv("HOME", "/fake/home")
	got, err := codexConfigPath()
	if err != nil {
		t.Fatal(err)
	}
	if got != "/fake/home/.codex/config.toml" {
		t.Errorf("got %s, want /fake/home/.codex/config.toml", got)
	}
}

// ensureCodexAgentsMd — AGENTS.md installer covering create / replace /
// idempotent-rerun / parent-dir-creation paths.

func TestEnsureCodexAgentsMd_CreatesFileWhenAbsent(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CODEX_HOME", dir)

	path, didWrite, err := ensureCodexAgentsMd()
	if err != nil {
		t.Fatalf("ensureCodexAgentsMd: %v", err)
	}
	if !didWrite {
		t.Fatal("expected didWrite=true on fresh file")
	}
	want := filepath.Join(dir, "AGENTS.md")
	if path != want {
		t.Errorf("path=%s, want %s", path, want)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), agentsMdBlockStart) || !strings.Contains(string(got), agentsMdBlockEnd) {
		t.Errorf("file missing block markers:\n%s", got)
	}
	if !strings.Contains(string(got), "OUTPUT MODE PROTOCOL") {
		t.Errorf("file missing ModeProtocol body:\n%s", got)
	}
	if !strings.Contains(string(got), "MULTI-PART REPLY PROTOCOL") {
		t.Errorf("file missing MultipartProtocol body:\n%s", got)
	}
	if !strings.Contains(string(got), "ON-THE-GO MODE (phone, web chat)") {
		t.Errorf("file missing OnTheGoProtocol body:\n%s", got)
	}
	for _, want := range []string{`attach with expr "+web"`, "`output` tool", "NEVER auto-switch"} {
		if !strings.Contains(string(got), want) {
			t.Errorf("file missing multi-route mode phrase %q:\n%s", want, got)
		}
	}
}

func TestEnsureCodexAgentsMd_ReplacesExistingBlock(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CODEX_HOME", dir)
	path := filepath.Join(dir, "AGENTS.md")

	// User has hand-edited content above + below an old c3 block whose
	// inner body is stale. Rerunning setup must replace ONLY the block
	// body, preserving the user's surrounding edits.
	existing := "# User notes\n" +
		"My personal guidance here.\n" +
		"\n" +
		agentsMdBlockStart + "\n" +
		"OLD STALE BODY — should be gone after rerun\n" +
		agentsMdBlockEnd + "\n" +
		"\n" +
		"## After the block\n" +
		"More user notes that must survive.\n"
	if err := os.WriteFile(path, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}

	_, didWrite, err := ensureCodexAgentsMd()
	if err != nil {
		t.Fatalf("ensureCodexAgentsMd: %v", err)
	}
	if !didWrite {
		t.Fatal("expected didWrite=true when block body changes")
	}
	got, _ := os.ReadFile(path)
	gs := string(got)
	if !strings.Contains(gs, "# User notes") || !strings.Contains(gs, "My personal guidance here.") {
		t.Errorf("pre-block user content lost:\n%s", gs)
	}
	if !strings.Contains(gs, "## After the block") || !strings.Contains(gs, "More user notes that must survive.") {
		t.Errorf("post-block user content lost:\n%s", gs)
	}
	if strings.Contains(gs, "OLD STALE BODY") {
		t.Errorf("stale block body survived rewrite:\n%s", gs)
	}
	if !strings.Contains(gs, "OUTPUT MODE PROTOCOL") {
		t.Errorf("new block missing protocol body:\n%s", gs)
	}
}

func TestEnsureCodexAgentsMd_IdempotentOnRerun(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CODEX_HOME", dir)
	path := filepath.Join(dir, "AGENTS.md")

	// First run creates the file.
	if _, didWrite, err := ensureCodexAgentsMd(); err != nil || !didWrite {
		t.Fatalf("first run: didWrite=%v err=%v", didWrite, err)
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Second run with identical protocol text must be a no-op at the
	// file level (didWrite=false, file content unchanged).
	_, didWrite, err := ensureCodexAgentsMd()
	if err != nil {
		t.Fatalf("rerun: %v", err)
	}
	if didWrite {
		t.Error("expected didWrite=false on idempotent rerun")
	}
	second, _ := os.ReadFile(path)
	if string(first) != string(second) {
		t.Errorf("rerun mutated file:\nbefore:\n%s\nafter:\n%s", first, second)
	}
}

func TestEnsureCodexAgentsMd_CreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	// Point CODEX_HOME at a nested path that doesn't exist yet — the
	// installer must mkdir -p the parent (mirroring the install_codex
	// MCP-block behaviour).
	nested := filepath.Join(dir, "deep", "nested", "codex")
	t.Setenv("CODEX_HOME", nested)

	if _, _, err := ensureCodexAgentsMd(); err != nil {
		t.Fatalf("ensureCodexAgentsMd: %v", err)
	}
	if _, err := os.Stat(filepath.Join(nested, "AGENTS.md")); err != nil {
		t.Errorf("AGENTS.md not created at %s: %v", nested, err)
	}
}

func TestEnsureCodexAgentsMd_AppendsWhenMissingMarkers(t *testing.T) {
	// File exists but has no c3 block — should append, not overwrite.
	dir := t.TempDir()
	t.Setenv("CODEX_HOME", dir)
	path := filepath.Join(dir, "AGENTS.md")

	existing := "# My agents file\nUser content with no c3 markers.\n"
	if err := os.WriteFile(path, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}

	_, didWrite, err := ensureCodexAgentsMd()
	if err != nil {
		t.Fatalf("ensureCodexAgentsMd: %v", err)
	}
	if !didWrite {
		t.Fatal("expected didWrite=true when block is absent")
	}
	got, _ := os.ReadFile(path)
	gs := string(got)
	if !strings.HasPrefix(gs, "# My agents file\nUser content with no c3 markers.\n") {
		t.Errorf("user content not preserved at top:\n%s", gs)
	}
	if !strings.Contains(gs, agentsMdBlockStart) || !strings.Contains(gs, agentsMdBlockEnd) {
		t.Errorf("appended file missing block markers:\n%s", gs)
	}
}

// replaceCodexAgentsMdBlock unit-level — corrupt block (start marker,
// no end) should refuse to rewrite and signal "not found" so the
// appender (rather than a destructive overwrite) takes over.
func TestReplaceCodexAgentsMdBlock_CorruptBlockNotReplaced(t *testing.T) {
	corrupt := "# user notes\n" + agentsMdBlockStart + "\nincomplete block, no end marker\n"
	out, ok := replaceCodexAgentsMdBlock(corrupt, "new block")
	if ok {
		t.Error("expected ok=false on corrupt block (start without end)")
	}
	if out != corrupt {
		t.Errorf("corrupt input mutated:\nwant:\n%s\ngot:\n%s", corrupt, out)
	}
}

// codexAgentsMdPath sanity — respects CODEX_HOME, falls back to ~/.codex.
func TestCodexAgentsMdPath_RespectsCODEX_HOME(t *testing.T) {
	t.Setenv("CODEX_HOME", "/custom/codex")
	got, err := codexAgentsMdPath()
	if err != nil {
		t.Fatal(err)
	}
	if got != "/custom/codex/AGENTS.md" {
		t.Errorf("got %s, want /custom/codex/AGENTS.md", got)
	}
}

func TestCodexAgentsMdPath_FallsBackToHome(t *testing.T) {
	_ = os.Unsetenv("CODEX_HOME")
	t.Setenv("HOME", "/fake/home")
	got, err := codexAgentsMdPath()
	if err != nil {
		t.Fatal(err)
	}
	if got != "/fake/home/.codex/AGENTS.md" {
		t.Errorf("got %s, want /fake/home/.codex/AGENTS.md", got)
	}
}
