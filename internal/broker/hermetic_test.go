package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Andrometiq/c3/internal/buildidentity"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// Preserve per-test queue/config overrides, but never inherit executable search
// paths. Test-owned Unix sockets in private temporary directories are allowed;
// installed adapters and ambient session sockets are not fixture dependencies.
func isolateBrokerDiscovery(t *testing.T) {
	t.Helper()
	path := "/usr/bin" + string(os.PathListSeparator) + "/bin"
	if runtime.GOOS == "windows" {
		path = filepath.Join(os.Getenv("SystemRoot"), "System32")
	}
	t.Setenv("PATH", path)
}

func noInstalledTestAdapter() (buildidentity.Installed, error) {
	return buildidentity.Installed{}, nil
}

func newTestBroker(t *testing.T, mf *mappings.MappingsFile) *Broker {
	t.Helper()
	isolateBrokerDiscovery(t)
	return New(mf, WithInstalledAdapterLookup(noInstalledTestAdapter))
}

// Upgrade policy tests need only the registry, not workers or durable storage.
func newUpgradeTestBroker(t *testing.T) *Broker {
	t.Helper()
	isolateBrokerDiscovery(t)
	b := &Broker{}
	WithInstalledAdapterLookup(noInstalledTestAdapter)(b)
	return b
}

func TestBrokerInstalledDiscoveryIsolation(t *testing.T) {
	clearFetchTestEnvironment(t)
	ambient := t.TempDir()
	t.Setenv("PATH", ambient)
	b := newTestBroker(t, mfWithTelegram())
	t.Cleanup(b.Shutdown)
	if os.Getenv("PATH") == ambient {
		t.Fatal("inherited PATH")
	}
	// Deliberately poison PATH again after setup: dependency injection must
	// prevent inspection even when a fixture overrides executable search paths.
	t.Setenv("PATH", ambient)
	if err := os.WriteFile(filepath.Join(ambient, "c3-claude-adapter"), []byte("invalid installed executable"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, hello := range []ipc.HelloMsg{
		{CLI: "claude"},
		{CLI: "claude", Build: "old", UpgradeDisabled: true},
		{CLI: "claude", Build: "old", ResumeContract: "contract"},
	} {
		var ack ipc.HelloAckMsg
		if fallback := b.prepareUpgrade(hello, &Stub{}, &ack); fallback != "" || ack.Upgrade != nil {
			t.Fatalf("ambient upgrade: %+v %q", ack, fallback)
		}
		if b.upgradeStale(&Stub{CLI: "claude", Build: hello.Build}) {
			t.Fatal("disabled discovery inferred a stale build")
		}
	}
}

func TestBrokerTestSetupCoverage(t *testing.T) {
	clearFetchTestEnvironment(t)
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
			if fn, ok := decl.(*ast.FuncDecl); ok && (fn.Name.Name == "newTestBroker" || fn.Name.Name == "newUpgradeTestBroker") {
				continue
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "New" {
						t.Errorf("%s: use newTestBroker to isolate installed-adapter discovery", path)
					}
				}
				if lit, ok := n.(*ast.CompositeLit); ok {
					if id, ok := lit.Type.(*ast.Ident); ok && id.Name == "Broker" {
						t.Errorf("%s: use newUpgradeTestBroker for an isolated registry", path)
					}
				}
				return true
			})
		}
	}
}
