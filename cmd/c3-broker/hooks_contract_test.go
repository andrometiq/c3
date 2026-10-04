package main

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
)

type pluginHooks struct {
	Hooks map[string][]struct {
		Hooks []struct {
			Command string `json:"command"`
		} `json:"hooks"`
	} `json:"hooks"`
}

// hookCommandsByEvent reads the c3 plugin's hooks.json.
func hookCommandsByEvent(t *testing.T) map[string][]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "plugins", "c3", "hooks", "hooks.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file pluginHooks
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	commands := map[string][]string{}
	for event, groups := range file.Hooks {
		for _, group := range groups {
			for _, hook := range group.Hooks {
				commands[event] = append(commands[event], hook.Command)
			}
		}
	}
	return commands
}

// Every registered hook names a subcommand this binary handles, and every
// PreToolUse hook is guarded so a failing or older binary can't block a tool
// call (Claude Code treats PreToolUse exit 2 as a block).
func TestPluginHooksContract(t *testing.T) {
	commands := hookCommandsByEvent(t)
	if len(commands["PreToolUse"]) == 0 || len(commands["PermissionDenied"]) == 0 {
		t.Fatalf("approval hooks not registered: %v", commands)
	}
	for event, list := range commands {
		for _, command := range list {
			fields := strings.Fields(command)
			if len(fields) < 2 || fields[0] != "c3-broker" || hookCommands[fields[1]] == nil {
				t.Fatalf("%s hook %q does not name a handled subcommand", event, command)
			}
			if event == "PreToolUse" && !strings.HasSuffix(command, " || exit 0") {
				t.Fatalf("PreToolUse hook %q lacks the '|| exit 0' guard", command)
			}
		}
	}
}

func buildBroker(t *testing.T, binDir string) {
	t.Helper()
	build := exec.Command("go", "build", "-o", filepath.Join(binDir, "c3-broker"), ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
}

// The real binary against a broker that closes as soon as it has allowed the
// call: the delivery confirmation fails, and the hook still prints the allow
// and exits 0.
func TestPreToolUseHookBinaryKeepsAllowWhenDeliveryFails(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("hooks are silent on Windows: the broker peer can't be verified")
	}
	dir := t.TempDir()
	buildBroker(t, dir)
	listener, err := net.Listen("unix", filepath.Join(dir, "c3.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			return
		}
		conn := ipc.NewConn(raw)
		defer conn.Close()
		if _, err := conn.ReadFrame(); err != nil { // hook_hello
			return
		}
		_ = conn.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck})
		if _, err := conn.ReadFrame(); err != nil { // grant_check
			return
		}
		_ = conn.WriteJSON(ipc.GrantCheckResp{Op: ipc.OpGrantCheckResult, Allow: true})
	}()
	cmd := exec.Command(filepath.Join(dir, "c3-broker"), "pretooluse-hook")
	cmd.Env = []string{"PATH=" + dir, "HOME=" + dir, "XDG_RUNTIME_DIR=" + dir, "XDG_CONFIG_HOME=" + dir}
	cmd.Stdin = strings.NewReader(`{"session_id":"s","cwd":"/w","tool_name":"Bash","tool_use_id":"toolu_1",` +
		`"tool_input":{"command":"ls"}}`)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil || !strings.Contains(stdout.String(), `"permissionDecision":"allow"`) {
		t.Fatalf("err=%v stdout=%q", err, stdout.String())
	}
}

// Run for real: each hook exits 0 with no output on an empty payload and no
// broker, an unknown future hook does too, and the guarded commands exit 0
// with no output when c3-broker fails with exit 2 or is missing.
func TestPluginHooksExitZeroSilently(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("exercises the POSIX shell form of the hook commands")
	}
	home := t.TempDir()
	binDir := filepath.Join(home, "bin")
	buildBroker(t, binDir)
	stubDir := filepath.Join(home, "stub")
	if err := os.MkdirAll(stubDir, 0o700); err != nil {
		t.Fatal(err)
	}
	stub := "#!/bin/sh\necho 'c3-broker: unknown subcommand' >&2\nexit 2\n"
	if err := os.WriteFile(filepath.Join(stubDir, "c3-broker"), []byte(stub), 0o700); err != nil {
		t.Fatal(err)
	}
	run := func(path string, name string, args ...string) {
		t.Helper()
		cmd := exec.Command(name, args...)
		cmd.Env = []string{"PATH=" + path, "HOME=" + home, "XDG_RUNTIME_DIR=" + home, "XDG_CONFIG_HOME=" + home}
		cmd.Stdin = strings.NewReader("{}")
		var stdout bytes.Buffer
		cmd.Stdout = &stdout
		if err := cmd.Run(); err != nil || stdout.Len() != 0 {
			t.Fatalf("%s %v with PATH=%s: err=%v stdout=%q", name, args, path, err, stdout.String())
		}
	}
	for _, list := range hookCommandsByEvent(t) {
		for _, command := range list {
			run(binDir, filepath.Join(binDir, "c3-broker"), strings.Fields(command)[1])
			if strings.Contains(command, "||") {
				run(binDir, "/bin/sh", "-c", command)
				run(stubDir, "/bin/sh", "-c", command)
				run(filepath.Join(home, "empty"), "/bin/sh", "-c", command)
			}
		}
	}
	run(binDir, filepath.Join(binDir, "c3-broker"), "some-future-hook")
}
