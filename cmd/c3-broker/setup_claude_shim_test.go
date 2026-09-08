package main

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise both setup entry points: accepting the general setup consent must
// never imply consent to replacing claude on PATH.
func TestSetupClaudeShim_OptIn(t *testing.T) {
	for _, tc := range []struct {
		name, answer string
		args         []string
		want         bool
	}{
		{name: "interactive-default", answer: "\n"},
		{name: "interactive-eof"},
		{name: "interactive-no", answer: "no\n"},
		{name: "interactive-invalid", answer: "maybe\n"},
		{name: "interactive-yes", answer: "yes\n", want: true},
		{name: "finish-default", args: []string{"finish"}},
		{name: "finish-no", args: []string{"finish", "--claude-shim=false"}},
		{name: "finish-yes", args: []string{"finish", "--claude-shim"}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sandboxSetupEnv(t)
			t.Setenv("C3_NO_PROMPT", "1")
			writeTestMappings(t, legacyNoAllowlistMappings())
			if err := os.MkdirAll(filepath.Dir(defaultSTTEnvPath()), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(defaultSTTEnvPath(), []byte("# already configured\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"ok":true,"result":{"username":"test_bot"}}`)
			}))
			defer srv.Close()
			t.Setenv("C3_TELEGRAM_API_URL", srv.URL)
			calls := 0
			installClaudeShimFn = func(args []string) error {
				calls++
				if len(args) != 0 {
					t.Errorf("installer args = %v, want defaults", args)
				}
				return nil
			}
			out := captureStdout(t, func() {
				// Continue and keep existing token, followed by the wrapper answer.
				withStdin(t, "\n\n"+tc.answer, func() {
					if err := runSetupWithArgs(tc.args); err != nil {
						t.Fatal(err)
					}
				})
			})
			wantCalls := 0
			if tc.want {
				wantCalls = 1
			}
			if calls != wantCalls {
				t.Fatalf("installer called %d times, want %d", calls, wantCalls)
			}
			if tc.args == nil && strings.Count(out, "Install the C3 claude launcher wrapper") != 1 {
				t.Fatalf("missing single wrapper prompt: %s", out)
			}
			if !tc.want && (!strings.Contains(out, "wrapper NOT installed") || !strings.Contains(out, "c3-broker install-claude-shim")) {
				t.Fatalf("missing skip notice / recovery command: %s", out)
			}
			if !strings.Contains(out, "/c3:status is a Claude slash command; c3-broker status is the shell command") {
				t.Fatalf("missing command distinction: %s", out)
			}
		})
	}
}

func TestMaybeInstallClaudeShim_OtherHostsSkip(t *testing.T) {
	prev := installClaudeShimFn
	installClaudeShimFn = func(args []string) error { t.Fatal("unexpected installer call"); return nil }
	t.Cleanup(func() { installClaudeShimFn = prev })
	for _, host := range []HostCLI{HostCodex, HostUnknown} {
		for _, optedIn := range []bool{false, true} {
			if err := maybeInstallClaudeShim(host, optedIn); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSetupFinish_ClaudeShimFailureIsLoud(t *testing.T) {
	sandboxSetupEnv(t)
	writeTestMappings(t, legacyNoAllowlistMappings())
	installClaudeShimFn = func(args []string) error { return errors.New("self-test failed") }
	out := captureStdout(t, func() {
		if err := runSetupWithArgs([]string{"finish", "--claude-shim"}); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(out, "[claude shim NOT installed]") || !strings.Contains(out, "self-test failed") {
		t.Fatalf("failed opt-in install was not surfaced: %s", out)
	}
}

func TestSetupFinish_RejectsInvalidArguments(t *testing.T) {
	for _, args := range [][]string{{"finish", "--claude-shim=maybe"}, {"finish", "--unknown"}, {"finish", "unexpected"}} {
		if err := runSetupWithArgs(args); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

// TestPrintShimInstallFailure_BlockOnBothStreams asserts that the
// structured failure surface emits the same actionable block on BOTH
// stdout AND stderr (the agent transcript sees stdout; raw shells see
// stderr). The block MUST include:
//   - the named header `[claude shim NOT installed]`
//   - the underlying error message
//   - the actionable next-step command `c3-broker install-claude-shim --force`
//   - an explanation of what --force does
//
// Closes M2 from 2026-05-19 code review.
func TestPrintShimInstallFailure_BlockOnBothStreams(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := errors.New("simulated: existing non-shim file at ~/.local/bin/claude")
	printShimInstallFailure(&stdout, &stderr, err)

	for _, tc := range []struct {
		name   string
		stream string
	}{
		{"stdout", stdout.String()},
		{"stderr", stderr.String()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(tc.stream, "[claude shim NOT installed]") {
				t.Errorf("%s missing structured header `[claude shim NOT installed]`; got:\n%s", tc.name, tc.stream)
			}
			if !strings.Contains(tc.stream, "simulated: existing non-shim file") {
				t.Errorf("%s missing underlying error text; got:\n%s", tc.name, tc.stream)
			}
			if !strings.Contains(tc.stream, "c3-broker install-claude-shim --force") {
				t.Errorf("%s missing actionable next-step `c3-broker install-claude-shim --force`; got:\n%s", tc.name, tc.stream)
			}
			if !strings.Contains(tc.stream, "--force") || !strings.Contains(tc.stream, "overwrite") {
				t.Errorf("%s does not explain what --force does; got:\n%s", tc.name, tc.stream)
			}
		})
	}
}
