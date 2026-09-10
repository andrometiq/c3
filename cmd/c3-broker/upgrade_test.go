package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/buildidentity"
	"github.com/Andrometiq/c3/internal/ipc"
)

func TestUpgradeBounce(t *testing.T) {
	oldStop, oldStart := stopBrokerFn, ensureBrokerUpFn
	defer func() { stopBrokerFn, ensureBrokerUpFn = oldStop, oldStart }()
	var calls []string
	stopBrokerFn = func() (bool, string) { calls = append(calls, "stop"); return true, "" }
	ensureBrokerUpFn = func() { calls = append(calls, "start") }
	if err := bounceUpgradeBroker(); err != nil || strings.Join(calls, ",") != "stop,start" {
		t.Fatalf("%v %v", calls, err)
	}
}
func TestUpgradeBuildAndUpdatePaths(t *testing.T) {
	for path, wants := range map[string][]string{
		"../../plugins/c3/commands/build.md":  {"make install && c3-broker restart"},
		"../../plugins/c3/commands/update.md": {"!c3-broker update", "bounces the broker"},
		"update.go":                           {"bounceUpgradeBroker()"},
		"setup.go":                            {"sourceBuildFlags(ctx, srcDir)", "restartBrokerForNewConfig()"},
		"../../Makefile":                      {"git describe --always --dirty", buildidentity.Symbol, "VERSION_LDFLAGS :=", "go install -ldflags \"$(VERSION_LDFLAGS)\""},
		"../../scripts/package.sh":            {"describe --always --dirty", buildidentity.Symbol},
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range wants {
			if !strings.Contains(string(data), want) {
				t.Fatalf("%s missing %s", path, want)
			}
		}
	}
	if got := sourceBuildFlags(context.Background(), t.TempDir()+"/missing"); got != "-X "+buildidentity.Symbol+"=dev" {
		t.Fatal(got)
	}
}
func TestUpgradeStatusBuild(t *testing.T) {
	got := renderSessionsTable([]ipc.SessionEntry{{CLI: "claude", Build: "old", Stale: true}})
	if !strings.Contains(got, "Build") || !strings.Contains(got, "old (stale)") {
		t.Fatal(got)
	}
}

func TestUpgradeDocumentationAndLogs(t *testing.T) {
	for path, wants := range map[string][]string{
		"../../README.md":                  {"## Updating C3", "/c3:update", "upgrade themselves", "/mcp", "v0.2.1-79", "picked up in place"},
		"../../docs/USAGE.md":              {"broker bounce", "self-exec", "permission"},
		"../../docs/DEBUGGING.md":          {"upgrade hint sent conn=", "adapter upgrade: exec", "adapter resumed after upgrade", "upgrade postponed:", "upgrade fallback notice sent conn="},
		"../../docs/ADAPTERS.md":           {"Provisional", "hello.build", "hello_ack.upgrade", "resume_contract"},
		"../../DECISIONS.md":               {"D037: Seamless adapter upgrade by self-exec with MCP resume"},
		"../../internal/broker/upgrade.go": {"upgrade hint sent conn=%d from=%s to=%s", "upgrade fallback notice sent conn=%d"},
		"../c3-claude-adapter/upgrade.go":  {"adapter upgrade: exec %s (build %s)", "adapter resumed after upgrade (build %s)", "upgrade postponed: %s"},
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range wants {
			if !strings.Contains(string(data), want) {
				t.Fatalf("%s missing %s", path, want)
			}
		}
	}
}
