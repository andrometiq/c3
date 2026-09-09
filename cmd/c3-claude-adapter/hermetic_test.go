package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/buildidentity"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// Every top-level test starts here, before creating adapters or overriding its
// own fixtures. Clear unknown future variables too; never inherit a terminal's
// session identity, messaging credentials, resume payload, or C3 file overrides.
// t.Setenv restores the caller's environment only after test resource cleanup.
// PATH contains only OS utilities; broker fixtures separately inject installed
// adapter discovery, so even an OS directory cannot supply an ambient adapter.
// Unix sockets created by the tests in private temporary directories are allowed;
// inherited host sockets and credentials are not.
func isolateAdapterTest(t *testing.T) {
	t.Helper()
	path := "/usr/bin" + string(os.PathListSeparator) + "/bin"
	if runtime.GOOS == "windows" {
		path = filepath.Join(os.Getenv("SystemRoot"), "System32")
	}
	t.Setenv("PATH", path)
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

// Always construct in-process brokers here, before exposing them to a reader.
// No lookup or build inspection of a machine-installed binary is permitted.
func newTestBroker(t *testing.T, mf *mappings.MappingsFile) *broker.Broker {
	t.Helper()
	return broker.New(mf, broker.WithInstalledAdapterLookup(func() (buildidentity.Installed, error) {
		return buildidentity.Installed{}, nil
	}))
}

func TestAdapterEnvironmentIsolation(t *testing.T) {
	isolateAdapterTest(t)
	for _, name := range []string{"CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_MESSAGING_TOKEN", "CLAUDE_CODE_FUTURE_SETTING", "C3_MCP_RESUME_STATE", "C3_FUTURE_SETTING"} {
		t.Setenv(name, "ambient-fixture")
	}
	outer := os.Getenv("XDG_STATE_HOME")
	t.Setenv("PATH", filepath.Join(t.TempDir(), "ambient-bin"))
	outerPath := os.Getenv("PATH")
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
		if os.Getenv("PATH") == outerPath {
			t.Fatal("inherited executable search path")
		}
	})
	if os.Getenv("CLAUDE_CODE_SESSION_ID") != "ambient-fixture" {
		t.Fatal("fixture overrides were not restored")
	}
}

func TestAdapterBrokerDiscoveryIsolation(t *testing.T) {
	isolateAdapterTest(t)
	b := newTestBroker(t, reconnectSwitchMappings())
	t.Cleanup(b.Shutdown)
	// Injection, not PATH filtering alone, must keep pre-feature connections
	// free of ambient fallback notices (and the net.Pipe handshake deadlock).
	path := t.TempDir()
	t.Setenv("PATH", path)
	if err := os.WriteFile(filepath.Join(path, "c3-claude-adapter"), []byte("invalid installed executable"), 0700); err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close() })
	go b.HandleConn(server)
	client.SetDeadline(time.Now().Add(time.Second))
	peer := ipc.NewConn(client)
	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: os.Getpid(), CWD: "/work"}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var ack ipc.HelloAckMsg
	if json.Unmarshal(raw, &ack) != nil || ack.Op != ipc.OpHelloAck || ack.Upgrade != nil {
		t.Fatalf("hello=%s", raw)
	}
	client.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	if raw, err := peer.ReadFrame(); err == nil {
		t.Fatalf("ambient upgrade frame: %s", raw)
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
		brokerAliases := map[string]bool{}
		for _, imp := range file.Imports {
			if strings.Trim(imp.Path.Value, `"`) == "github.com/Andrometiq/c3/internal/broker" {
				alias := "broker"
				if imp.Name != nil {
					alias = imp.Name.Name
				}
				brokerAliases[alias] = true
			}
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "newTestBroker" {
				ast.Inspect(decl, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok {
						if pkg, ok := sel.X.(*ast.Ident); ok && brokerAliases[pkg.Name] && sel.Sel.Name == "New" {
							t.Errorf("%s: %s must use newTestBroker to isolate installed-adapter discovery", path, "broker construction")
						}
					}
					if lit, ok := n.(*ast.CompositeLit); ok {
						if sel, ok := lit.Type.(*ast.SelectorExpr); ok {
							if pkg, ok := sel.X.(*ast.Ident); ok && brokerAliases[pkg.Name] && sel.Sel.Name == "Broker" {
								t.Errorf("%s: raw broker bypasses installed-adapter isolation", path)
							}
						}
					}
					return true
				})
			}
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
