// Package archguard mechanically enforces the C3 channel-capability no-leak
// boundary (spec docs/specs/2026-06-14-channel-capability-architecture.md,
// "No-leak strategy (R7)"). These tests are the CI grep-guard: a future change
// that leaks a Telegram-specific detail into core, dials a cross-host socket
// from core, or widens the capability package's import surface fails the build
// with a message that names the offending file + reason.
//
// Why a separate package: it scans the whole module's source from a neutral
// vantage point and must not be coupled to any package it polices.
package archguard

import (
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const modulePath = "github.com/Andrometiq/c3"

// telegramPkgRel is the ONE package allowed to name Telegram specifics. All
// Telegram rendering/wire detail lives here; nothing above it may.
const telegramPkgRel = "internal/channel/telegram"

// moduleRoot walks up from the test's working directory to the directory
// containing go.mod (robust regardless of where `go test` is invoked from).
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not locate go.mod walking up from %s", dir)
		}
		dir = parent
	}
}

// goSourceFiles returns every *.go file under root, optionally excluding
// _test.go files. Vendored / hidden dirs and the testdata convention are
// skipped.
func goSourceFiles(t *testing.T, root string, includeTests bool) []string {
	t.Helper()
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			base := info.Name()
			if base == "vendor" || base == "testdata" || (strings.HasPrefix(base, ".") && base != ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if !includeTests && strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return files
}

// rel returns the module-root-relative slash path for a file.
func rel(t *testing.T, root, path string) string {
	t.Helper()
	r, err := filepath.Rel(root, path)
	if err != nil {
		t.Fatalf("rel(%s,%s): %v", root, path, err)
	}
	return filepath.ToSlash(r)
}

// inTelegramPkg reports whether the module-relative path lives in the one
// package allowed to name Telegram specifics.
func inTelegramPkg(relPath string) bool {
	return strings.HasPrefix(relPath, telegramPkgRel+"/")
}

// bannedLiterals are Telegram-specific tokens that must not appear in core
// (anything outside internal/channel/telegram). They are matched against the
// CODE tokens of each file (comments are tokenized separately and ignored), so
// a literal merely mentioned in a doc comment is allowed — only real code
// references are leaks. `4096` is the bare Telegram message-length limit;
// matched only as a numeric literal so unrelated 4096s elsewhere would still
// be caught as a number (the one legitimate non-Telegram 4096 is allowlisted).
var bannedLiterals = []string{
	"gotgbot",
	"MarkdownV2",
	"message_thread_id",
	"parse_mode",
	"sendPhoto",
	"4096",
}

// sanctionedLiteralOccurrence records a known, deliberate, non-leak occurrence
// of a banned literal in core code, with the reason it is allowed. Keep this
// list TINY and documented — every entry is a residual leak we have reasoned
// about, not a place to silence the guard. Paths are module-root-relative.
type sanctionedLiteralOccurrence struct {
	relPath string
	literal string
	reason  string
}

var sanctionedLiterals = []sanctionedLiteralOccurrence{
	{
		// The Claude Code channel-notification `meta` record uses field names
		// defined by Claude Code's channels protocol (channels-reference.md),
		// not by Telegram. `message_thread_id` / `reply_to_message_id` happen
		// to coincide with Telegram's names, but this is the CLAUDE channel
		// wire contract (asserted in cmd/c3-claude-adapter/wire_test.go), not a
		// Telegram leak. The adapter is the Claude-protocol boundary, not core
		// formatting/dispatch.
		relPath: "cmd/c3-claude-adapter/main.go",
		literal: "message_thread_id",
		reason:  "Claude Code channel-notification meta field name (channels protocol), not a Telegram detail",
	},
	{
		// A log-tail truncation buffer size; unrelated to Telegram's
		// message-length limit. Named locally as `const maxTail = 4096`.
		relPath: "cmd/c3-broker/setup.go",
		literal: "4096",
		reason:  "go-install log-tail buffer size, not the Telegram message limit",
	},
}

func isSanctioned(relPath, literal string) bool {
	for _, s := range sanctionedLiterals {
		if s.relPath == relPath && s.literal == literal {
			return true
		}
	}
	return false
}

// codeTokenStrings tokenizes a Go source file and returns the textual content
// of every CODE token (identifiers, string/char/number literals), skipping
// comments. This lets the literal scan ignore mentions in doc comments while
// still catching real code references — including bare numeric literals and
// substrings inside string literals.
func codeTokenStrings(t *testing.T, path string) []string {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	fset := token.NewFileSet()
	file := fset.AddFile(path, fset.Base(), len(src))
	var s scanner.Scanner
	// Mode 0 (NOT ScanComments) makes the scanner drop comments entirely, so
	// COMMENT tokens never surface here.
	s.Init(file, src, nil, 0)
	var out []string
	for {
		_, tok, lit := s.Scan()
		if tok == token.EOF {
			break
		}
		if lit != "" {
			out = append(out, lit)
		}
	}
	return out
}

// TestNoTelegramLiteralsInCore asserts no Telegram-specific literal appears in
// the CODE of any *.go file outside internal/channel/telegram (comments are
// ignored; a tiny documented allowlist covers sanctioned non-Telegram
// coincidences). A failure names the file + literal so a future dev sees why.
func TestNoTelegramLiteralsInCore(t *testing.T) {
	root := moduleRoot(t)
	for _, path := range goSourceFiles(t, root, false /* skip _test.go */) {
		relPath := rel(t, root, path)
		if inTelegramPkg(relPath) {
			continue // the one package allowed to name Telegram specifics
		}
		toks := codeTokenStrings(t, path)
		for _, lit := range bannedLiterals {
			for _, tok := range toks {
				if strings.Contains(tok, lit) {
					if isSanctioned(relPath, lit) {
						break // documented non-leak; stop checking this literal here
					}
					t.Errorf("Telegram leak: %s contains banned literal %q in core code. "+
						"Telegram-specific detail must stay inside %s/. If this is a sanctioned "+
						"non-Telegram coincidence, add it to sanctionedLiterals with a reason.",
						relPath, lit, telegramPkgRel)
					break // one report per (file,literal) is enough
				}
			}
		}
	}
}

// importsOf parses just the import block of a Go file and returns the import
// paths (unquoted).
func importsOf(t *testing.T, fset *token.FileSet, path string) []string {
	t.Helper()
	f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse imports %s: %v", path, err)
	}
	var out []string
	for _, imp := range f.Imports {
		out = append(out, strings.Trim(imp.Path.Value, `"`))
	}
	return out
}

// TestChannelFirewallDoesNotImportGotgbot asserts the channel firewall package
// (internal/channel, top-level only — NOT the telegram sub-package) never
// imports gotgbot. The firewall is channel-neutral; pulling in the Telegram
// client here would collapse the abstraction.
func TestChannelFirewallDoesNotImportGotgbot(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	dir := filepath.Join(root, "internal", "channel")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue // non-recursive: the telegram sub-package is excluded
		}
		path := filepath.Join(dir, e.Name())
		for _, imp := range importsOf(t, fset, path) {
			if strings.Contains(imp, "gotgbot") {
				t.Errorf("firewall leak: internal/channel/%s imports %q; the channel "+
					"firewall must stay channel-neutral — gotgbot belongs only in %s/.",
					e.Name(), imp, telegramPkgRel)
			}
		}
	}
}

// isStdlibImport reports whether an import path is a Go standard-library
// package (no dot in the first path segment — stdlib paths have no domain).
func isStdlibImport(path string) bool {
	first := path
	if i := strings.IndexByte(path, '/'); i >= 0 {
		first = path[:i]
	}
	return !strings.Contains(first, ".")
}

// TestCapabilityPackagePure asserts internal/capability imports ONLY
// internal/c3types + the Go standard library — no other internal package, no
// third-party dependency. This keeps the pure gate dependency-free (it must not
// reach into broker/channel/gotgbot, per the spec's L2 layer rule).
func TestCapabilityPackagePure(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	allowedInternal := modulePath + "/internal/c3types"
	dir := filepath.Join(root, "internal", "capability")
	for _, path := range goSourceFiles(t, dir, false /* skip _test.go */) {
		relPath := rel(t, root, path)
		for _, imp := range importsOf(t, fset, path) {
			if isStdlibImport(imp) {
				continue
			}
			if imp == allowedInternal {
				continue
			}
			t.Errorf("capability purity leak: %s imports %q; internal/capability may "+
				"import ONLY %q + the Go standard library (no other internal package, "+
				"no third-party dependency).", relPath, imp, allowedInternal)
		}
	}
}

// TestNoTCPDialInCore defends the single-host invariant: broker, adapters and
// the Telegram client share one host + filesystem (unix socket c3.sock). Core
// must not introduce a cross-host TCP socket. We scan the core dispatch /
// capability / firewall surface for a net.Dial* call whose network literal is
// "tcp". gotgbot's own HTTP lives inside the telegram package (out of scope
// here); the legitimate broker dial is a "unix" socket, which is allowed.
func TestNoTCPDialInCore(t *testing.T) {
	root := moduleRoot(t)
	// Core scope: dispatch + the pure gate + the firewall file. Deliberately
	// narrow — this is where an accidental cross-host socket in core would land.
	scopeFiles := goSourceFiles(t, filepath.Join(root, "internal", "broker"), false)
	scopeFiles = append(scopeFiles, goSourceFiles(t, filepath.Join(root, "internal", "capability"), false)...)
	scopeFiles = append(scopeFiles, filepath.Join(root, "internal", "channel", "channel.go"))

	for _, path := range scopeFiles {
		relPath := rel(t, root, path)
		toks := codeTokenStrings(t, path)
		// A net.Dial("tcp"...) / net.DialTimeout("tcp"...) shows up as the
		// adjacent code tokens `net`, `Dial`/`DialTimeout`, and a "tcp" string
		// literal. The string literal alone is the load-bearing signal; a "tcp"
		// network in core is what we forbid.
		hasDial := false
		hasTCP := false
		for _, tok := range toks {
			if tok == "Dial" || tok == "DialTimeout" || tok == "DialContext" {
				hasDial = true
			}
			if tok == `"tcp"` || tok == `"tcp4"` || tok == `"tcp6"` {
				hasTCP = true
			}
		}
		if hasDial && hasTCP {
			t.Errorf("single-host invariant breach: %s contains a net.Dial* with a TCP "+
				"network. Core must not open a cross-host TCP socket — the broker, "+
				"adapters and Telegram client share one host (unix socket).", relPath)
		}
	}
}
