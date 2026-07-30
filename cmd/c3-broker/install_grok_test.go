package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// countTableHeader counts real (uncommented) occurrences of a given table
// header in text, using the same detector the patcher uses. A count > 1 means
// duplicate TOML tables — invalid config.
func countTableHeader(text, name string) int {
	n := 0
	for _, ln := range strings.Split(text, "\n") {
		if got, ok := tableHeader(ln); ok && got == name {
			n++
		}
	}
	return n
}

// assertNoDuplicateTables fails if any C3-owned table header appears more than
// once in text.
func assertNoDuplicateTables(t *testing.T, text string) {
	t.Helper()
	for _, name := range []string{"cli", "mcp_servers.c3", "plugins"} {
		if c := countTableHeader(text, name); c > 1 {
			t.Fatalf("duplicate [%s] table (%d occurrences) — invalid TOML:\n%s", name, c, text)
		}
	}
}

func TestPatchGrokConfig_Fresh(t *testing.T) {
	out, changed, err := patchGrokConfig("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected change")
	}
	if !strings.Contains(out, "use_leader = true") || !strings.Contains(out, "c3-grok-adapter") {
		t.Fatalf("got %q", out)
	}
	assertNoDuplicateTables(t, out)
}

func TestPatchGrokConfig_Idempotent(t *testing.T) {
	first, _, err := patchGrokConfig("")
	if err != nil {
		t.Fatalf("first patch error: %v", err)
	}
	second, changed, err := patchGrokConfig(first)
	if err != nil {
		t.Fatalf("second patch error: %v", err)
	}
	if changed {
		t.Fatalf("second patch should be no-op; got %q vs %q", second, first)
	}
	assertNoDuplicateTables(t, second)
}

func TestPatchGrokConfig_FlipClaudeAdapter(t *testing.T) {
	in := "[mcp_servers.c3]\ncommand = \"c3-claude-adapter\"\nenabled = true\n"
	out, changed, err := patchGrokConfig(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed || !strings.Contains(out, "c3-grok-adapter") {
		t.Fatalf("got %q", out)
	}
	if strings.Contains(out, "c3-claude-adapter") {
		t.Fatalf("claude adapter should be replaced, not left behind: %q", out)
	}
	assertNoDuplicateTables(t, out)
}

// A commented-out use_leader must not be silently overridden — the tool must
// fail loudly with the manual edit rather than report success with leader off.
func TestPatchGrokConfig_CommentedUseLeaderFails(t *testing.T) {
	in := "[cli]\n# use_leader = true\n"
	_, _, err := patchGrokConfig(in)
	if err == nil {
		t.Fatal("expected fail-loud error for commented-out use_leader")
	}
	if !strings.Contains(err.Error(), "use_leader = true") {
		t.Fatalf("error should include the manual edit; got %q", err.Error())
	}
}

// A live use_leader = false under [cli] is safe to flip in place.
func TestPatchGrokConfig_UseLeaderFalseFlips(t *testing.T) {
	in := "[cli]\nuse_leader = false\n"
	out, changed, err := patchGrokConfig(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected change")
	}
	if !strings.Contains(out, "use_leader = true") || strings.Contains(out, "use_leader = false") {
		t.Fatalf("use_leader not flipped to true: %q", out)
	}
	if countTableHeader(out, "cli") != 1 {
		t.Fatalf("expected exactly one [cli] table: %q", out)
	}
	assertNoDuplicateTables(t, out)
}

// An existing [mcp_servers.c3] pointing at an unrecognized command must fail
// loudly rather than appending a duplicate table (invalid TOML).
func TestPatchGrokConfig_ExistingC3PointsElsewhere(t *testing.T) {
	in := "[mcp_servers.c3]\ncommand = \"/usr/local/bin/my-wrapper\"\nenabled = true\n"
	out, _, err := patchGrokConfig(in)
	if err == nil {
		t.Fatal("expected fail-loud error for unrecognized c3 command")
	}
	if !strings.Contains(err.Error(), "c3-grok-adapter") {
		t.Fatalf("error should include the manual edit; got %q", err.Error())
	}
	// Even the returned (unwritten) text must not contain a duplicate table.
	assertNoDuplicateTables(t, out)
}

// An existing [plugins] with an inline enabled array gets "c3" added in place —
// never a second [plugins] table.
func TestPatchGrokConfig_ExistingPluginsArray(t *testing.T) {
	in := "[plugins]\nenabled = [\"foo\", \"bar\"]\n"
	out, changed, err := patchGrokConfig(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected change")
	}
	if !strings.Contains(out, "\"c3\"") {
		t.Fatalf("c3 not added to enabled array: %q", out)
	}
	if !strings.Contains(out, "\"foo\"") || !strings.Contains(out, "\"bar\"") {
		t.Fatalf("existing plugins should be preserved: %q", out)
	}
	if countTableHeader(out, "plugins") != 1 {
		t.Fatalf("expected exactly one [plugins] table: %q", out)
	}
	assertNoDuplicateTables(t, out)
}

// An existing [plugins] table that already lists c3 (even multi-line) is left
// untouched — no duplicate, no error.
func TestPatchGrokConfig_ExistingPluginsHasC3(t *testing.T) {
	in := "[plugins]\nenabled = [\n  \"foo\",\n  \"c3\",\n]\n"
	out, changed, err := patchGrokConfig(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if changed {
		// Note: [cli] and [mcp_servers.c3] are still appended, so `changed` is
		// true overall; assert only that plugins wasn't duplicated.
	}
	if countTableHeader(out, "plugins") != 1 {
		t.Fatalf("expected exactly one [plugins] table: %q", out)
	}
	assertNoDuplicateTables(t, out)
}

// A [mcp_servers.c3] table whose command already points at the grok adapter but
// is missing enabled gets enabled inserted in place, not a duplicate table.
func TestPatchGrokConfig_ExistingGrokMissingEnabled(t *testing.T) {
	in := "[mcp_servers.c3]\ncommand = \"c3-grok-adapter\"\n"
	out, _, err := patchGrokConfig(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "enabled = true") {
		t.Fatalf("enabled not ensured: %q", out)
	}
	if countTableHeader(out, "mcp_servers.c3") != 1 {
		t.Fatalf("expected exactly one [mcp_servers.c3] table: %q", out)
	}
	assertNoDuplicateTables(t, out)
}

// Broad guarantee: across a spread of pre-existing configs, no successful patch
// output ever contains a duplicated C3-owned table header.
func TestPatchGrokConfig_NeverDuplicatesTables(t *testing.T) {
	inputs := []string{
		"",
		"[cli]\nuse_leader = true\n",
		"[cli]\nuse_leader = false\n",
		"[mcp_servers.c3]\ncommand = \"c3-claude-adapter\"\nenabled = true\n",
		"[mcp_servers.c3]\ncommand = \"c3-grok-adapter\"\nenabled = true\n",
		"[plugins]\nenabled = [\"other\"]\n",
		"[plugins]\nother = 1\n",
		"[cli]\nuse_leader = true\n\n[mcp_servers.c3]\ncommand = \"c3-grok-adapter\"\nenabled = true\n\n[plugins]\nenabled = [\"c3\"]\n",
	}
	for _, in := range inputs {
		out, _, err := patchGrokConfig(in)
		if err != nil {
			// Fail-loud inputs are covered by dedicated tests; skip here.
			continue
		}
		assertNoDuplicateTables(t, out)
	}
}

func TestFindGrokPluginSource_PrefersFreshPrebuiltLayout(t *testing.T) {
	release := t.TempDir()
	plugin := filepath.Join(release, "plugins", "c3-grok")
	for name, body := range map[string]string{
		".mcp.json":                          "{}\n",
		"plugin.json":                        "{}\n",
		filepath.Join("hooks", "hooks.json"): "{}\n",
	} {
		path := filepath.Join(plugin, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	oldExecutable := grokExecutablePath
	grokExecutablePath = func() (string, error) {
		return filepath.Join(release, "c3-broker"), nil
	}
	t.Cleanup(func() { grokExecutablePath = oldExecutable })

	got := findGrokPluginSource()
	want, err := filepath.Abs(plugin)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("findGrokPluginSource() = %q, want packaged plugin %q", got, want)
	}
}
