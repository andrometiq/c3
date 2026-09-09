package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every top-level test starts here, before creating adapters or overriding its
// own fixtures. Clear unknown future variables too; never inherit a terminal's
// session identity, messaging credentials, resume payload, or C3 file overrides.
// t.Setenv restores the caller's environment only after test resource cleanup.
func isolateAdapterTest(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "CLAUDE_") || strings.HasPrefix(name, "C3_") {
			t.Setenv(name, "")
		}
	}
	root := t.TempDir()
	for _, name := range []string{"XDG_STATE_HOME", "XDG_CONFIG_HOME", "XDG_RUNTIME_DIR", "XDG_CACHE_HOME", "LOCALAPPDATA"} {
		dir := filepath.Join(root, name)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv(name, dir)
	}
	t.Setenv("C3_QUEUE_DIR", filepath.Join(root, "queue"))
}

func TestAdapterEnvironmentIsolation(t *testing.T) {
	isolateAdapterTest(t)
	for _, name := range []string{"CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_MESSAGING_TOKEN", "CLAUDE_CODE_FUTURE_SETTING", "C3_MCP_RESUME_STATE", "C3_FUTURE_SETTING"} {
		t.Setenv(name, "ambient-fixture")
	}
	outer := os.Getenv("XDG_STATE_HOME")
	t.Run("isolated", func(t *testing.T) {
		isolateAdapterTest(t)
		for _, entry := range os.Environ() {
			name, value, _ := strings.Cut(entry, "=")
			if (strings.HasPrefix(name, "CLAUDE_") || strings.HasPrefix(name, "C3_")) && name != "C3_QUEUE_DIR" && value != "" {
				t.Fatalf("inherited variable %s", name)
			}
		}
		if os.Getenv("XDG_STATE_HOME") == outer {
			t.Fatal("reused ambient handoff directory")
		}
	})
	if os.Getenv("CLAUDE_CODE_SESSION_ID") != "ambient-fixture" {
		t.Fatal("fixture overrides were not restored")
	}
}

// Prevent future tests (including pure unit tests that later gain fixtures)
// from accidentally reopening the maintainer-environment dependency.
func TestAdapterTestSetupCoverage(t *testing.T) {
	isolateAdapterTest(t)
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, "Test") || fn.Name.Name == "TestMain" {
				continue
			}
			isolated := false
			if fn.Body != nil && len(fn.Body.List) > 0 {
				if stmt, ok := fn.Body.List[0].(*ast.ExprStmt); ok {
					if call, ok := stmt.X.(*ast.CallExpr); ok {
						if name, ok := call.Fun.(*ast.Ident); ok {
							isolated = name.Name == "isolateAdapterTest"
						}
					}
				}
			}
			if !isolated {
				t.Errorf("%s: %s must isolate its environment before any setup", path, fn.Name.Name)
			}
		}
	}
}
