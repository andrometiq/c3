package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var grokExecutablePath = os.Executable

// runInstallGrok configures the host for C3 × Grok Build:
//   - ensures [cli] use_leader = true (required for live inject)
//   - pins [mcp_servers.c3] command = c3-grok-adapter
//   - prints plugin install + reload instructions
//
// Non-destructive: only appends/updates the keys we own; leaves other config alone.
func runInstallGrok(args []string) error {
	_ = args
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	cfgPath := filepath.Join(home, ".grok", "config.toml")
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o755); err != nil {
		return err
	}

	raw, err := os.ReadFile(cfgPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	text := string(raw)
	text, changed, err := patchGrokConfig(text)
	if err != nil {
		// Fail loudly with the manual edit rather than writing a config that is
		// invalid TOML or that silently leaves leader mode off.
		fmt.Fprintf(os.Stderr, "install-grok: cannot safely update %s:\n  %v\n", cfgPath, err)
		return err
	}
	if changed {
		// Preserve the existing file's mode; only a freshly created config gets
		// 0o644 (a fresh config carries no secrets).
		mode := os.FileMode(0o644)
		if fi, statErr := os.Stat(cfgPath); statErr == nil {
			mode = fi.Mode().Perm()
		}
		if err := writeFileAtomic(cfgPath, []byte(text), mode); err != nil {
			return err
		}
		fmt.Printf("updated %s\n", cfgPath)
	} else {
		fmt.Printf("already configured: %s\n", cfgPath)
	}

	// Verify adapter on PATH.
	if p, err := lookPath("c3-grok-adapter"); err != nil {
		fmt.Fprintf(os.Stderr, "warning: c3-grok-adapter not on PATH — run: go install ./cmd/c3-grok-adapter\n")
	} else {
		fmt.Printf("adapter: %s\n", p)
	}

	pluginSrc := findGrokPluginSource()
	fmt.Println()
	fmt.Println("Next:")
	if pluginSrc != "" {
		fmt.Printf("  grok plugin install %s --trust\n", pluginSrc)
	} else {
		fmt.Println("  grok plugin install <path-to-c3>/plugins/c3-grok --trust")
	}
	fmt.Println("  # ensure leader mode, then start Grok:")
	fmt.Println("  grok --leader")
	fmt.Println("  # in session: attach name=<topic>")
	fmt.Println()
	fmt.Println("After rebuilding c3-grok-adapter, reload MCP (/mcps) or restart the leader")
	fmt.Println("so the new binary is spawned (TUI restart alone keeps the old MCP process).")
	return nil
}

// patchGrokConfig returns the config text with the keys C3 owns set correctly:
// [cli] use_leader = true, [mcp_servers.c3] command = "c3-grok-adapter" +
// enabled = true, and "c3" present in [plugins] enabled.
//
// It is line-aware rather than a substring rewriter: it finds real (uncommented)
// table headers and edits the keys inside the right table, and it NEVER appends
// a table that already exists (which would be duplicate — invalid TOML). When it
// cannot make a change safely — a commented-out use_leader it must not override,
// an [mcp_servers.c3] pointing at an unknown command, or a multi-line [plugins]
// enabled array — it returns an error carrying the exact manual edit instead of
// writing a broken or silently-wrong config.
func patchGrokConfig(text string) (string, bool, error) {
	orig := text
	if strings.TrimSpace(text) == "" {
		text = "# managed in part by c3-broker install-grok\n"
	}
	var err error
	if text, err = patchLeader(text); err != nil {
		return orig, false, err
	}
	if text, err = patchMCPServer(text); err != nil {
		return orig, false, err
	}
	if text, err = patchPlugins(text); err != nil {
		return orig, false, err
	}
	return text, text != orig, nil
}

// patchLeader ensures [cli] use_leader = true, editing only the real key under
// [cli] (never a commented or foreign occurrence). It fails loudly rather than
// silently leaving leader mode off.
func patchLeader(text string) (string, error) {
	lines, sections := splitSections(text)

	// A real use_leader key under [cli].
	if i := findKeyInSection(lines, sections, "cli", "use_leader"); i >= 0 {
		if v, _ := keyOnLine(lines[i], "use_leader"); firstToken(v) == "true" {
			return text, nil // already enabled
		}
		lines[i] = setValue(lines[i], "true")
		return strings.Join(lines, "\n"), nil
	}

	// No live key. A commented-out use_leader anywhere is ambiguous — the user may
	// have deliberately disabled it — so refuse instead of overriding a comment.
	for _, ln := range lines {
		if commentedKey(ln, "use_leader") {
			return text, errors.New(
				"[cli] use_leader is present but commented out — refusing to override your choice. " +
					"To enable leader mode (required for live Telegram inject), edit the file and set under the [cli] table:\n" +
					"    use_leader = true")
		}
	}

	// Insert under an existing [cli] header, else append a fresh [cli] table.
	if i := findHeaderIdx(lines, "cli"); i >= 0 {
		lines = insertAfter(lines, i, "use_leader = true")
		return strings.Join(lines, "\n"), nil
	}
	return ensureTrailingNewline(text) + "\n[cli]\nuse_leader = true\n", nil
}

// patchMCPServer ensures [mcp_servers.c3] pins command = "c3-grok-adapter" and
// enabled = true. If the table is absent it appends the block; if present it
// edits the keys in place and never appends a second [mcp_servers.c3].
func patchMCPServer(text string) (string, error) {
	const block = "\n# C3 Grok adapter (live Telegram inject via leader)\n[mcp_servers.c3]\ncommand = \"c3-grok-adapter\"\nenabled = true\n"

	if findHeader(text, "mcp_servers.c3") < 0 {
		return ensureTrailingNewline(text) + block, nil
	}
	var err error
	if text, err = ensureMCPCommand(text); err != nil {
		return text, err
	}
	return ensureMCPEnabled(text), nil
}

// ensureMCPCommand sets command = "c3-grok-adapter" inside the existing
// [mcp_servers.c3] table. It migrates the known claude adapter in place, leaves
// an already-correct command alone, and refuses (fail loud) to clobber any other
// custom command.
func ensureMCPCommand(text string) (string, error) {
	lines, sections := splitSections(text)
	headerIdx := findHeaderIdx(lines, "mcp_servers.c3")
	cmdIdx := findKeyInSection(lines, sections, "mcp_servers.c3", "command")
	if cmdIdx < 0 {
		lines = insertAfter(lines, headerIdx, `command = "c3-grok-adapter"`)
		return strings.Join(lines, "\n"), nil
	}
	v, _ := keyOnLine(lines[cmdIdx], "command")
	cmd, _ := tomlStringValue(v)
	switch cmd {
	case "c3-grok-adapter":
		return text, nil // already correct
	case "c3-claude-adapter":
		lines[cmdIdx] = setValue(lines[cmdIdx], `"c3-grok-adapter"`)
		return strings.Join(lines, "\n"), nil
	default:
		return text, fmt.Errorf(
			"[mcp_servers.c3] already sets command = %q, which is neither c3-grok-adapter nor c3-claude-adapter — "+
				"refusing to overwrite a custom command. To use C3 with Grok, set under [mcp_servers.c3]:\n"+
				"    command = \"c3-grok-adapter\"", cmd)
	}
}

// ensureMCPEnabled sets enabled = true inside the existing [mcp_servers.c3]
// table (inserting the key if absent, flipping it if not already true).
func ensureMCPEnabled(text string) string {
	lines, sections := splitSections(text)
	if i := findKeyInSection(lines, sections, "mcp_servers.c3", "enabled"); i >= 0 {
		if v, _ := keyOnLine(lines[i], "enabled"); firstToken(v) == "true" {
			return text
		}
		lines[i] = setValue(lines[i], "true")
		return strings.Join(lines, "\n")
	}
	anchor := findKeyInSection(lines, sections, "mcp_servers.c3", "command")
	if anchor < 0 {
		anchor = findHeaderIdx(lines, "mcp_servers.c3")
	}
	lines = insertAfter(lines, anchor, "enabled = true")
	return strings.Join(lines, "\n")
}

// patchPlugins ensures "c3" is enabled under [plugins]. If [plugins] is absent
// it appends it; if present it adds "c3" to a single-line enabled array (or
// inserts the array if missing), and never appends a second [plugins]. A
// multi-line enabled array it cannot edit safely is a fail-loud.
func patchPlugins(text string) (string, error) {
	if findHeader(text, "plugins") < 0 {
		return ensureTrailingNewline(text) + "\n[plugins]\nenabled = [\"c3\"]\n", nil
	}
	lines, sections := splitSections(text)
	headerIdx := findHeaderIdx(lines, "plugins")
	enIdx := findKeyInSection(lines, sections, "plugins", "enabled")
	if enIdx < 0 {
		lines = insertAfter(lines, headerIdx, `enabled = ["c3"]`)
		return strings.Join(lines, "\n"), nil
	}
	if sectionHasQuoted(lines, sections, "plugins", "c3") {
		return text, nil // already listed
	}
	newLine, ok := addToInlineArray(lines[enIdx], "c3")
	if !ok {
		return text, errors.New(
			"[plugins] enabled is a multi-line array this installer won't edit — add C3 manually so the list under [plugins] includes it, e.g.:\n" +
				"    enabled = [\n      ...existing...,\n      \"c3\",\n    ]")
	}
	lines[enIdx] = newLine
	return strings.Join(lines, "\n"), nil
}

// --- line-aware TOML helpers (no external dependency) ---

// tomlTableHeaderRe matches a standard [table] or array-of-tables [[table]]
// header anchored at line start, allowing leading whitespace and a trailing
// comment. Group 1 is the (trimmed) table name.
var tomlTableHeaderRe = regexp.MustCompile(`^\s*\[\[?\s*([^\[\]]+?)\s*\]\]?\s*(#.*)?$`)

// tableHeader returns the table name if line is a real (uncommented) TOML table
// header. Comment lines and key lines return ok=false.
func tableHeader(line string) (name string, ok bool) {
	if strings.HasPrefix(strings.TrimSpace(line), "#") {
		return "", false
	}
	m := tomlTableHeaderRe.FindStringSubmatch(line)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// keyOnLine returns the raw right-hand side if line is a real (uncommented)
// assignment "<key> = <value>" at any indentation. Table headers and comments
// return ok=false. The value may still carry a trailing inline comment.
func keyOnLine(line, key string) (value string, ok bool) {
	t := strings.TrimSpace(line)
	if t == "" || strings.HasPrefix(t, "#") || strings.HasPrefix(t, "[") {
		return "", false
	}
	eq := strings.IndexByte(t, '=')
	if eq < 0 || strings.TrimSpace(t[:eq]) != key {
		return "", false
	}
	return strings.TrimSpace(t[eq+1:]), true
}

// commentedKey reports whether line is a commented-out assignment to key, e.g.
// "# use_leader = true".
func commentedKey(line, key string) bool {
	t := strings.TrimSpace(line)
	if !strings.HasPrefix(t, "#") {
		return false
	}
	t = strings.TrimSpace(strings.TrimLeft(t, "#"))
	eq := strings.IndexByte(t, '=')
	if eq < 0 {
		return false
	}
	return strings.TrimSpace(t[:eq]) == key
}

// splitSections splits text into lines and, for each line, the table it belongs
// to ("" = the root table above any header; a header line maps to its own table).
func splitSections(text string) (lines, sections []string) {
	lines = strings.Split(text, "\n")
	sections = make([]string, len(lines))
	cur := ""
	for i, ln := range lines {
		if name, ok := tableHeader(ln); ok {
			cur = name
		}
		sections[i] = cur
	}
	return lines, sections
}

func findHeaderIdx(lines []string, name string) int {
	for i := range lines {
		if n, ok := tableHeader(lines[i]); ok && n == name {
			return i
		}
	}
	return -1
}

func findHeader(text, name string) int {
	return findHeaderIdx(strings.Split(text, "\n"), name)
}

func findKeyInSection(lines, sections []string, section, key string) int {
	for i := range lines {
		if sections[i] != section {
			continue
		}
		if _, ok := keyOnLine(lines[i], key); ok {
			return i
		}
	}
	return -1
}

// sectionHasQuoted reports whether the quoted string "elem" appears anywhere in
// the given section (covers single- and multi-line arrays alike).
func sectionHasQuoted(lines, sections []string, section, elem string) bool {
	q := `"` + elem + `"`
	for i := range lines {
		if sections[i] == section && strings.Contains(lines[i], q) {
			return true
		}
	}
	return false
}

func firstToken(s string) string {
	if f := strings.Fields(s); len(f) > 0 {
		return f[0]
	}
	return ""
}

// setValue rewrites the right-hand side of an assignment line, preserving the
// leading indentation and key. Any inline comment on the old value is dropped.
func setValue(line, newVal string) string {
	eq := strings.IndexByte(line, '=')
	if eq < 0 {
		return line
	}
	return line[:eq+1] + " " + newVal
}

func insertAfter(lines []string, idx int, extra ...string) []string {
	out := make([]string, 0, len(lines)+len(extra))
	out = append(out, lines[:idx+1]...)
	out = append(out, extra...)
	out = append(out, lines[idx+1:]...)
	return out
}

func ensureTrailingNewline(s string) string {
	if s == "" || strings.HasSuffix(s, "\n") {
		return s
	}
	return s + "\n"
}

// tomlStringValue extracts the contents of the first single- or double-quoted
// string in v (a right-hand side). ok=false if there is no quoted string.
func tomlStringValue(v string) (string, bool) {
	for _, q := range []byte{'"', '\''} {
		if i := strings.IndexByte(v, q); i >= 0 {
			if j := strings.IndexByte(v[i+1:], q); j >= 0 {
				return v[i+1 : i+1+j], true
			}
		}
	}
	return "", false
}

// addToInlineArray inserts elem (as a quoted string) into a single-line TOML
// array on the given assignment line. ok=false if the line has no complete
// [...] pair (e.g. a multi-line array whose closing bracket is on a later line).
func addToInlineArray(line, elem string) (string, bool) {
	open := strings.IndexByte(line, '[')
	if open < 0 {
		return "", false
	}
	rel := strings.IndexByte(line[open:], ']')
	if rel < 0 {
		return "", false // multi-line array — closing bracket not on this line
	}
	closeB := open + rel
	quoted := `"` + elem + `"`
	if strings.TrimSpace(line[open+1:closeB]) == "" {
		return line[:open+1] + quoted + line[closeB:], true
	}
	return line[:closeB] + ", " + quoted + line[closeB:], true
}

// writeFileAtomic writes data to path via a same-directory temp file + rename so
// a crash, SIGKILL, or ENOSPC mid-write can never truncate the existing file.
// Mirrors the repo convention in internal/mappings.Write.
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".config.*.tmp")
	if err != nil {
		return fmt.Errorf("create tempfile: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write tempfile: %w", err)
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("chmod tempfile: %w", err)
	}
	// fsync data before the rename so a power loss can't leave a zero-byte config.
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("fsync tempfile: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close tempfile: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return fmt.Errorf("rename %s -> %s: %w", tmpPath, path, err)
	}
	// fsync the parent dir so the rename itself is durable.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func lookPath(file string) (string, error) {
	path := os.Getenv("PATH")
	for _, dir := range filepath.SplitList(path) {
		if dir == "" {
			// Skip empty PATH entries. Resolving "" to "." (CWD) is a footgun:
			// on Windows an empty element (a trailing/doubled ';') would let a
			// binary dropped in the current directory be resolved and, for
			// install-desktop, baked into claude_desktop_config.json as an
			// auto-run command. A real binary is never in an empty entry.
			continue
		}
		p := filepath.Join(dir, file)
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p, nil
		}
	}
	return "", fmt.Errorf("not found")
}

func findGrokPluginSource() string {
	// Prebuilt installs keep the packaged plugin beside c3-broker. Prefer it
	// over ambient CWD so `install-grok` is one-command usable after the source
	// archive has been discarded.
	if exe, err := grokExecutablePath(); err == nil {
		if resolved, resolveErr := filepath.EvalSymlinks(exe); resolveErr == nil {
			exe = resolved
		}
		if plugin := existingGrokPlugin(filepath.Join(filepath.Dir(exe), "plugins", "c3-grok")); plugin != "" {
			return plugin
		}
	}

	// Walk up from the working directory looking for the plugin dir — the
	// documented flow runs install-grok from a source checkout.
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		c := filepath.Join(dir, "plugins", "c3-grok")
		if plugin := existingGrokPlugin(c); plugin != "" {
			return plugin
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func existingGrokPlugin(dir string) string {
	for _, required := range []string{".mcp.json", "plugin.json", filepath.Join("hooks", "hooks.json")} {
		info, err := os.Stat(filepath.Join(dir, required))
		if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			return ""
		}
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return ""
	}
	return abs
}
