// claude-shim is the C3 Claude Code launcher wrapper. It auto-injects
// `--dangerously-load-development-channels plugin:c3@c3` before exec'ing the
// real claude binary, so users don't have to remember the flag.
//
// Idempotency contract (load-bearing — see TODO #17, 2026-05-18):
//
//  1. Flag absent in argv → prepend the flag with `plugin:c3@c3`.
//  2. Flag present and `plugin:c3@c3` already in its argument list → exec
//     real claude unmodified. No double-injection.
//  3. Flag present but `plugin:c3@c3` not in its list → append
//     `plugin:c3@c3` to the existing flag's value list, preserving other
//     plugin tags.
//
// Resolves the real claude binary by walking $PATH and skipping any entry
// that resolves (via symlink) to this shim. Uses syscall.Exec so signals
// and tty pass through unchanged.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Andrometiq/c3/internal/shimconfig"
)

const (
	devChannelsFlag = "--dangerously-load-development-channels"
	c3PluginTag     = "plugin:c3@c3"
)

func main() {
	if err := run(os.Args, os.Environ()); err != nil {
		fmt.Fprintf(os.Stderr, "c3 claude-shim: %v\n", err)
		os.Exit(1)
	}
}

func run(argv0AndArgs, env []string) error {
	if len(argv0AndArgs) == 0 {
		return fmt.Errorf("missing argv[0]")
	}
	self := argv0AndArgs[0]
	args := argv0AndArgs[1:]

	realClaude, err := findRealClaude(self)
	if err != nil {
		return err
	}

	newArgs := injectC3Plugin(args)

	// execReplace replaces this process (unix: syscall.Exec) so signals and the
	// tty pass through unchanged — critical for an interactive TUI launcher. On
	// Windows it runs the target as a child and exits with its code (no execve).
	execArgs := append([]string{realClaude}, newArgs...)
	return execReplace(realClaude, execArgs, env)
}

// injectC3Plugin applies the idempotency contract documented in the package
// comment. Returns a new argv (the input slice is not mutated).
//
// Parsing rules for the multi-value flag, matching claude's documented form:
//   - `--dangerously-load-development-channels foo bar` — both `foo` and
//     `bar` are values until the next `--flag` or end of argv.
//   - `--dangerously-load-development-channels=foo` — single value via
//     `=`-attached form. Subsequent positional args are NOT part of the
//     flag in this form.
//   - A positional arg that doesn't start with `-` IS consumed as a flag
//     value (mirrors how plugin tags like `plugin:foo@bar` look).
//   - `--` terminates the value list.
func injectC3Plugin(args []string) []string {
	flagIdx, valStart, valEnd, attachedValue := findDevChannelsFlag(args)

	if flagIdx < 0 {
		// Case 1: flag absent → prepend.
		out := make([]string, 0, len(args)+2)
		out = append(out, devChannelsFlag, c3PluginTag)
		out = append(out, args...)
		return out
	}

	// Flag present — check whether c3PluginTag already appears in its
	// value list. For the `=` form, valStart == flagIdx+1 == valEnd
	// (no space-separated values), and the single value is in
	// attachedValue.
	values := args[valStart:valEnd]
	if attachedValue != "" {
		if attachedValue == c3PluginTag {
			return append([]string(nil), args...)
		}
		// `=` form with a different value — append a new space-separated
		// c3 tag after the `=` form so we don't mangle the existing
		// attached value. We insert it as a separate `plugin:c3@c3`
		// positional value, which claude will pick up as a second value
		// for the same flag.
		out := make([]string, 0, len(args)+1)
		out = append(out, args[:flagIdx+1]...)
		out = append(out, c3PluginTag)
		out = append(out, args[flagIdx+1:]...)
		return out
	}
	for _, v := range values {
		if v == c3PluginTag {
			// Case 2: already present → exec unmodified (return a copy
			// so callers can't accidentally mutate the input).
			return append([]string(nil), args...)
		}
	}
	// Case 3: flag present but tag missing → splice tag in at the end of
	// the existing value list.
	out := make([]string, 0, len(args)+1)
	out = append(out, args[:valEnd]...)
	out = append(out, c3PluginTag)
	out = append(out, args[valEnd:]...)
	return out
}

// findDevChannelsFlag locates the `--dangerously-load-development-channels`
// flag in argv and returns:
//
//	flagIdx  — index of the flag token itself, or -1 if absent.
//	valStart — index of the first space-separated value (== flagIdx+1).
//	valEnd   — index one past the last space-separated value (so
//	           args[valStart:valEnd] is the value slice). For the `=`
//	           form, valStart == valEnd == flagIdx+1.
//	attached — the value parsed from the `=`-attached form, or "" if the
//	           space-separated form (or absent).
//
// Only the FIRST occurrence of the flag is honoured — claude's own behaviour
// with a repeated flag is unspecified, and re-injecting into a second
// occurrence risks doubling up.
func findDevChannelsFlag(args []string) (flagIdx, valStart, valEnd int, attached string) {
	for i, a := range args {
		if a == "--" {
			return -1, 0, 0, ""
		}
		if a == devChannelsFlag {
			flagIdx = i
			valStart = i + 1
			valEnd = valStart
			for valEnd < len(args) {
				v := args[valEnd]
				if v == "--" {
					break
				}
				if strings.HasPrefix(v, "-") {
					break
				}
				valEnd++
			}
			return flagIdx, valStart, valEnd, ""
		}
		if strings.HasPrefix(a, devChannelsFlag+"=") {
			attached = strings.TrimPrefix(a, devChannelsFlag+"=")
			return i, i + 1, i + 1, attached
		}
	}
	return -1, 0, 0, ""
}

// findRealClaude walks $PATH for a `claude` executable that isn't this shim.
// $C3_CLAUDE_REAL overrides the search for testing / for users with a custom
// claude install path. Falls back to a glob of NVM versions only as a
// last-ditch (parallels findRealCodex).
func findRealClaude(self string) (string, error) {
	if explicit := os.Getenv("C3_CLAUDE_REAL"); explicit != "" {
		return explicit, nil
	}

	selfAbs, _ := filepath.Abs(self)
	if resolved, err := filepath.EvalSymlinks(selfAbs); err == nil {
		selfAbs = resolved
	}

	// Config-supplied real-claude: written by `c3-broker
	// install-claude-shim` when it takes over a user-curated symlink
	// at ~/.local/bin/claude. Corrupt/missing/invalid config silently
	// falls through to the PATH walk — shim never hard-fails on
	// config issues. See TODO.md #17.
	if cfgPath, err := shimconfig.Path(); err == nil {
		if candidate, ok := shimconfig.Load(cfgPath); ok {
			if info, err := os.Stat(candidate); err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
				resolved, err := filepath.EvalSymlinks(candidate)
				if err == nil && resolved != selfAbs {
					return candidate, nil
				}
			}
		}
	}

	pathParts := filepath.SplitList(os.Getenv("PATH"))
	for _, dir := range pathParts {
		if dir == "" {
			continue
		}
		candidate := filepath.Join(dir, "claude")
		info, err := os.Stat(candidate)
		if err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
			continue
		}
		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			continue
		}
		if resolved == selfAbs {
			continue
		}
		return candidate, nil
	}
	return "", errors.New("could not find real claude in $PATH (shim excluded); set C3_CLAUDE_REAL")
}
