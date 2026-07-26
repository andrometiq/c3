package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/shimconfig"
)

func TestInjectC3Plugin_FlagAbsent_Prepends(t *testing.T) {
	got := injectC3Plugin([]string{"--resume"})
	want := []string{devChannelsFlag, c3PluginTag, "--resume"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestInjectC3Plugin_FlagAbsent_NoArgs(t *testing.T) {
	got := injectC3Plugin(nil)
	want := []string{devChannelsFlag, c3PluginTag}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestInjectC3Plugin_FlagPresent_C3AlreadyThere_NoChange(t *testing.T) {
	in := []string{devChannelsFlag, c3PluginTag, "--resume"}
	got := injectC3Plugin(in)
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("got %#v, want %#v", got, in)
	}
}

func TestInjectC3Plugin_FlagPresent_OtherPluginsTooKeepsThem(t *testing.T) {
	in := []string{devChannelsFlag, "plugin:other@x", c3PluginTag, "plugin:foo@y", "--resume"}
	got := injectC3Plugin(in)
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("got %#v, want %#v", got, in)
	}
}

func TestInjectC3Plugin_FlagPresent_NoC3_AppendsToValueList(t *testing.T) {
	in := []string{devChannelsFlag, "plugin:other@x", "plugin:foo@y", "--resume"}
	got := injectC3Plugin(in)
	want := []string{devChannelsFlag, "plugin:other@x", "plugin:foo@y", c3PluginTag, "--resume"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestInjectC3Plugin_FlagPresent_NoValues_AppendsC3(t *testing.T) {
	in := []string{devChannelsFlag, "--resume"}
	got := injectC3Plugin(in)
	want := []string{devChannelsFlag, c3PluginTag, "--resume"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestInjectC3Plugin_FlagPresent_AtEndOfArgv_AppendsC3(t *testing.T) {
	in := []string{"--resume", devChannelsFlag}
	got := injectC3Plugin(in)
	want := []string{"--resume", devChannelsFlag, c3PluginTag}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestInjectC3Plugin_EqualsForm_C3AlreadyThere_NoChange(t *testing.T) {
	in := []string{devChannelsFlag + "=" + c3PluginTag, "--resume"}
	got := injectC3Plugin(in)
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("got %#v, want %#v", got, in)
	}
}

func TestInjectC3Plugin_EqualsForm_DifferentValue_AppendsC3AfterFlag(t *testing.T) {
	in := []string{devChannelsFlag + "=plugin:other@x", "--resume"}
	got := injectC3Plugin(in)
	want := []string{devChannelsFlag + "=plugin:other@x", c3PluginTag, "--resume"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestInjectC3Plugin_DoubleDashTerminator_PrependsBefore(t *testing.T) {
	// `--` short-circuits the search; we treat the flag as absent and
	// prepend at the start.
	in := []string{"--", devChannelsFlag, c3PluginTag}
	got := injectC3Plugin(in)
	want := []string{devChannelsFlag, c3PluginTag, "--", devChannelsFlag, c3PluginTag}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestInjectC3Plugin_FlagPresent_ValueListTerminatesAtNextFlag(t *testing.T) {
	in := []string{devChannelsFlag, "plugin:other@x", "--resume", "session-id-1"}
	got := injectC3Plugin(in)
	want := []string{devChannelsFlag, "plugin:other@x", c3PluginTag, "--resume", "session-id-1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestInjectC3Plugin_DoesNotMutateInput(t *testing.T) {
	in := []string{"--resume"}
	snapshot := append([]string(nil), in...)
	_ = injectC3Plugin(in)
	if !reflect.DeepEqual(in, snapshot) {
		t.Fatalf("input mutated: got %#v, was %#v", in, snapshot)
	}
}

func TestFindRealClaude_PrefersC3RealOverride(t *testing.T) {
	// Isolate shim config so a developer-machine ~/.config/c3/claude-shim.json
	// can't shadow the env-var override test.
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	override := filepath.Join(t.TempDir(), "real-claude")
	if err := os.WriteFile(override, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("C3_CLAUDE_REAL", override)
	got, err := findRealClaude("/anywhere/claude")
	if err != nil {
		t.Fatal(err)
	}
	if got != override {
		t.Fatalf("got %q, want %q", got, override)
	}
}

func TestFindRealClaude_PrefersConfigOverPathWalk(t *testing.T) {
	dir := t.TempDir()
	xdg := filepath.Join(dir, "xdg")
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv("C3_CLAUDE_REAL", "")

	// Config-supplied real-claude.
	cfgClaude := filepath.Join(dir, "real", "claude")
	if err := os.MkdirAll(filepath.Dir(cfgClaude), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgClaude, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfgPath, err := shimconfig.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := shimconfig.Save(cfgPath, cfgClaude); err != nil {
		t.Fatal(err)
	}

	// PATH-walk decoy: a different `claude` exists on PATH. Config
	// must win.
	decoyDir := filepath.Join(dir, "decoy")
	if err := os.MkdirAll(decoyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	decoy := filepath.Join(decoyDir, "claude")
	if err := os.WriteFile(decoy, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", decoyDir)

	got, err := findRealClaude("/anywhere/shim/claude")
	if err != nil {
		t.Fatal(err)
	}
	if got != cfgClaude {
		t.Fatalf("got %q, want %q (config should beat PATH walk)", got, cfgClaude)
	}
}

func TestFindRealClaude_EnvVarBeatsConfig(t *testing.T) {
	dir := t.TempDir()
	xdg := filepath.Join(dir, "xdg")
	t.Setenv("XDG_CONFIG_HOME", xdg)

	cfgClaude := filepath.Join(dir, "from-config", "claude")
	if err := os.MkdirAll(filepath.Dir(cfgClaude), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgClaude, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfgPath, err := shimconfig.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := shimconfig.Save(cfgPath, cfgClaude); err != nil {
		t.Fatal(err)
	}

	envClaude := filepath.Join(dir, "from-env", "claude")
	if err := os.MkdirAll(filepath.Dir(envClaude), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(envClaude, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("C3_CLAUDE_REAL", envClaude)

	got, err := findRealClaude("/anywhere/shim/claude")
	if err != nil {
		t.Fatal(err)
	}
	if got != envClaude {
		t.Fatalf("got %q, want %q (env var should beat config)", got, envClaude)
	}
}

func TestFindRealClaude_CorruptConfigFallsBackToPathWalk(t *testing.T) {
	dir := t.TempDir()
	xdg := filepath.Join(dir, "xdg")
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv("C3_CLAUDE_REAL", "")

	cfgPath, err := shimconfig.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgPath, []byte("not valid json"), 0o600); err != nil {
		t.Fatal(err)
	}

	pathDir := filepath.Join(dir, "pathdir")
	if err := os.MkdirAll(pathDir, 0o755); err != nil {
		t.Fatal(err)
	}
	pathClaude := filepath.Join(pathDir, "claude")
	if err := os.WriteFile(pathClaude, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", pathDir)

	got, err := findRealClaude("/anywhere/shim/claude")
	if err != nil {
		t.Fatal(err)
	}
	if got != pathClaude {
		t.Fatalf("got %q, want %q (corrupt config should silently fall back)", got, pathClaude)
	}
}

func TestFindRealClaude_ConfigPathMissing_FallsBackToPathWalk(t *testing.T) {
	dir := t.TempDir()
	xdg := filepath.Join(dir, "xdg")
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv("C3_CLAUDE_REAL", "")

	cfgPath, err := shimconfig.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o700); err != nil {
		t.Fatal(err)
	}
	// Config points at a binary that no longer exists.
	if err := shimconfig.Save(cfgPath, filepath.Join(dir, "nonexistent")); err != nil {
		t.Fatal(err)
	}

	pathDir := filepath.Join(dir, "pathdir")
	if err := os.MkdirAll(pathDir, 0o755); err != nil {
		t.Fatal(err)
	}
	pathClaude := filepath.Join(pathDir, "claude")
	if err := os.WriteFile(pathClaude, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", pathDir)

	got, err := findRealClaude("/anywhere/shim/claude")
	if err != nil {
		t.Fatal(err)
	}
	if got != pathClaude {
		t.Fatalf("got %q, want %q (config-target-missing should fall back)", got, pathClaude)
	}
}

func TestFindRealClaude_ConfigPathResolvesToShim_FallsBackToPathWalk(t *testing.T) {
	// Pathological case: config points at the shim itself (e.g. user
	// re-symlinked something pointing back at the shim). Must NOT cause
	// an exec loop. Fall back to PATH walk.
	dir := t.TempDir()
	xdg := filepath.Join(dir, "xdg")
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv("C3_CLAUDE_REAL", "")

	shimBin := filepath.Join(dir, "shim", "claude-shim")
	if err := os.MkdirAll(filepath.Dir(shimBin), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(shimBin, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	shimSelf := filepath.Join(dir, "shim", "claude")
	if err := os.Symlink(shimBin, shimSelf); err != nil {
		t.Fatal(err)
	}

	// Config points at the shim's own resolved binary.
	cfgPath, err := shimconfig.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := shimconfig.Save(cfgPath, shimBin); err != nil {
		t.Fatal(err)
	}

	pathDir := filepath.Join(dir, "pathdir")
	if err := os.MkdirAll(pathDir, 0o755); err != nil {
		t.Fatal(err)
	}
	pathClaude := filepath.Join(pathDir, "claude")
	if err := os.WriteFile(pathClaude, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", pathDir)

	got, err := findRealClaude(shimSelf)
	if err != nil {
		t.Fatal(err)
	}
	if got != pathClaude {
		t.Fatalf("got %q, want %q (config-resolves-to-shim must fall back)", got, pathClaude)
	}
}

func TestFindRealClaude_SkipsShimSelfOnPath(t *testing.T) {
	// Isolate shim config so a developer-machine claude-shim.json doesn't
	// shadow the PATH-walk test.
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	// Set up: a "real" claude in dir A, and a symlink to the shim in dir B.
	// Self is the symlink in B. Walk should find the binary in A.
	shimDir := t.TempDir()
	realDir := t.TempDir()

	shim := filepath.Join(shimDir, "claude-shim-bin")
	if err := os.WriteFile(shim, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	shimLink := filepath.Join(shimDir, "claude")
	if err := os.Symlink(shim, shimLink); err != nil {
		t.Fatal(err)
	}
	real := filepath.Join(realDir, "claude")
	if err := os.WriteFile(real, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("PATH", shimDir+string(os.PathListSeparator)+realDir)
	t.Setenv("C3_CLAUDE_REAL", "")

	got, err := findRealClaude(shimLink)
	if err != nil {
		t.Fatal(err)
	}
	if got != real {
		t.Fatalf("got %q, want %q (shim=%q)", got, real, shimLink)
	}
}

func TestFindRealClaude_ErrorsWhenNoneFound(t *testing.T) {
	// Isolate shim config so a developer-machine claude-shim.json doesn't
	// satisfy the lookup and make this test see a result.
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	dir := t.TempDir()
	t.Setenv("PATH", dir)
	t.Setenv("C3_CLAUDE_REAL", "")
	_, err := findRealClaude(filepath.Join(dir, "claude"))
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "could not find real claude") {
		t.Fatalf("got %v, want 'could not find real claude' error", err)
	}
}

// TestShim_EndToEnd_ExecsRealClaudeWithInjectedFlag builds the shim, points
// PATH at a fake real-claude that records its argv, and runs the shim. The
// fake binary writes argv to a file we then read back. Verifies the
// happy-path injection contract through a real process exec.
func TestShim_EndToEnd_ExecsRealClaudeWithInjectedFlag(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain unavailable; can't build shim binary")
	}
	dir := t.TempDir()
	shimBin := filepath.Join(dir, "claude-shim")
	build := exec.Command("go", "build", "-o", shimBin, ".")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("build shim: %v", err)
	}

	realDir := t.TempDir()
	realClaude := filepath.Join(realDir, "claude")
	argvLog := filepath.Join(realDir, "argv.txt")
	// Fake claude writes its argv (one arg per line) to argvLog.
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$@\" > " + argvLog + "\n"
	if err := os.WriteFile(realClaude, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	// Symlink the built shim into a PATH dir earlier than realDir, so
	// findRealClaude has to walk past it.
	shimDir := t.TempDir()
	shimLink := filepath.Join(shimDir, "claude")
	if err := os.Symlink(shimBin, shimLink); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "flag-absent",
			args: []string{"--resume"},
			want: []string{devChannelsFlag, c3PluginTag, "--resume"},
		},
		{
			name: "flag-present-c3-there",
			args: []string{devChannelsFlag, c3PluginTag, "--resume"},
			want: []string{devChannelsFlag, c3PluginTag, "--resume"},
		},
		{
			name: "flag-present-no-c3",
			args: []string{devChannelsFlag, "plugin:other@x", "--resume"},
			want: []string{devChannelsFlag, "plugin:other@x", c3PluginTag, "--resume"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_ = os.Remove(argvLog)
			cmd := exec.Command(shimLink, tc.args...)
			cmd.Env = append(os.Environ(),
				"PATH="+shimDir+string(os.PathListSeparator)+realDir,
				"C3_CLAUDE_REAL=", // force PATH walk
			)
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("shim run: %v", err)
			}
			data, err := os.ReadFile(argvLog)
			if err != nil {
				t.Fatalf("read argv log: %v", err)
			}
			lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
			if !reflect.DeepEqual(lines, tc.want) {
				t.Fatalf("real-claude saw argv %#v, want %#v", lines, tc.want)
			}
		})
	}
}
