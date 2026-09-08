package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/shimconfig"
)

// shimSentinel is a byte sequence embedded in the claude-shim binary that
// install-claude-shim looks for before overwriting an existing
// `~/.local/bin/claude`. Any binary containing this sentinel is treated as
// a previously-installed C3 shim (safe to overwrite). Anything else
// requires --force.
//
// The sentinel string lives in the launcher source as a string constant —
// referenced via the launcher's package comment and the error message it
// prints — so it survives a `go build -trimpath -ldflags=-s` strip.
const shimSentinel = "c3 claude-shim"

func runInstallClaudeShim(args []string) error {
	fs := flag.NewFlagSet("install-claude-shim", flag.ContinueOnError)
	force := fs.Bool("force", false, "overwrite an existing claude binary even if it isn't a C3 shim")
	target := fs.String("path", "", "install path (default: $HOME/.local/bin/claude)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	launcher := filepath.Join(filepath.Dir(exe), "claude-shim")
	if _, err := os.Stat(launcher); err != nil {
		return fmt.Errorf("claude-shim launcher not found next to c3-broker at %s; run `/c3:build` or install `./cmd/c3-broker ./cmd/claude-shim` first", launcher)
	}

	installPath := *target
	if installPath == "" {
		installPath = filepath.Join(home, ".local", "bin", "claude")
	}

	if err := installClaudeShim(installPath, launcher, *force); err != nil {
		return err
	}
	fmt.Printf("%s -> %s\n", installPath, launcher)
	return nil
}

// installClaudeShim places and tests a symlink at installPath pointing at launcher.
// A failed test removes the new link and restores any replaced entry.
// Safety contract:
//   - If installPath doesn't exist → create the symlink.
//   - If installPath is a symlink (any target) → replace it. We assume any
//     symlink at this name is either already pointing at our launcher or
//     was put there by a prior `install-claude-shim` run.
//   - If installPath is a real file and force=false → check whether it
//     contains shimSentinel; if so, replace (it's a previously-installed
//     shim binary that was copied rather than symlinked). Otherwise refuse
//     with a clear error.
//   - If installPath is a real file and force=true → replace unconditionally.
func installClaudeShim(installPath, launcher string, force bool) error {
	if err := os.MkdirAll(filepath.Dir(installPath), 0o755); err != nil {
		return err
	}

	previousTarget := ""
	info, statErr := os.Lstat(installPath)
	switch {
	case statErr != nil && errors.Is(statErr, os.ErrNotExist):
		// Fresh install.
	case statErr != nil:
		return statErr
	case info.Mode()&os.ModeSymlink != 0:
		var err error
		previousTarget, err = os.Readlink(installPath)
		if err != nil {
			return err
		}
		// Existing symlink — may be a previous shim install (target ==
		// launcher), or a user-curated link pointing at the real claude
		// binary. In the latter case, persist the resolved real-claude
		// path to ~/.config/c3/claude-shim.json so the shim runtime can
		// re-find it after we replace this symlink.
		if resolved, err := filepath.EvalSymlinks(installPath); err == nil {
			resolvedLauncher, lerr := filepath.EvalSymlinks(launcher)
			if lerr != nil {
				resolvedLauncher = launcher
			}
			if resolved == resolvedLauncher {
				// Idempotent short-circuit: the symlink already points
				// at our launcher. Skipping the remove+recreate keeps
				// the inode stable and closes the brief window where
				// installPath wouldn't exist on disk (race with a
				// concurrent `claude` invocation). Closes report
				// MINOR m6 (2026-05-19).
				return testClaudeShim(installPath)
			}
			if rinfo, sErr := os.Stat(resolved); sErr == nil && !rinfo.IsDir() && rinfo.Mode()&0o111 != 0 {
				if cfgPath, pErr := shimconfig.Path(); pErr == nil {
					// Best-effort: a write failure here must not
					// block the install. The shim falls back to
					// the PATH walk if the config is missing.
					_ = shimconfig.Save(cfgPath, resolved)
				}
			}
		}
	default:
		// Regular file (or other). Allow only if it's our sentinel-marked
		// shim binary, or --force.
		if !force {
			isShim, err := fileContainsSentinel(installPath, shimSentinel)
			if err != nil {
				return fmt.Errorf("inspect %s: %w", installPath, err)
			}
			if !isShim {
				return fmt.Errorf("refusing to overwrite non-shim file at %s; pass --force to override", installPath)
			}
		}
	}

	// Preserve regular files too (including --force installs) until validation
	// succeeds. Keep the backup beside the destination so rename stays local.
	backup := ""
	if statErr == nil && previousTarget == "" {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("refusing to replace non-regular file at %s", installPath)
		}
		f, err := os.CreateTemp(filepath.Dir(installPath), ".claude-backup-*")
		if err != nil {
			return err
		}
		backup = f.Name()
		if err := f.Close(); err != nil {
			_ = os.Remove(backup)
			return err
		}
		if err := os.Rename(installPath, backup); err != nil {
			_ = os.Remove(backup)
			return err
		}
	} else if statErr == nil {
		if err := os.Remove(installPath); err != nil {
			return err
		}
	}
	installErr := os.Symlink(launcher, installPath)
	if installErr == nil {
		installErr = testClaudeShim(installPath)
		if installErr != nil {
			if err := os.Remove(installPath); err != nil {
				return fmt.Errorf("%w; rollback could not remove %s: %v", installErr, installPath, err)
			}
		}
	}
	if installErr != nil {
		var restoreErr error
		if previousTarget != "" {
			restoreErr = os.Symlink(previousTarget, installPath)
		} else if backup != "" {
			restoreErr = os.Rename(backup, installPath)
		}
		if restoreErr != nil {
			return fmt.Errorf("%w; rollback failed: %v (backup: %s)", installErr, restoreErr, backup)
		}
		return fmt.Errorf("%w; restored previous launcher state at %s", installErr, installPath)
	}
	if backup != "" {
		return os.Remove(backup)
	}
	return nil
}

// testClaudeShim exercises the installed path, including real-Claude resolution.
// Daemon help may exit nonzero when agent view is disabled; that notice still
// proves dispatch reached the daemon command. --version must always exit zero.
func testClaudeShim(installPath string) error {
	// --path may be relative; never let exec resolve a bare name via PATH.
	installPath, err := filepath.Abs(installPath)
	if err != nil {
		return err
	}
	for _, args := range [][]string{{"daemon", "--help"}, {"--version"}} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cmd := exec.CommandContext(ctx, installPath, args...)
		cmd.WaitDelay = time.Second // bound inherited output pipes as well
		out, err := cmd.CombinedOutput()
		ctxErr := ctx.Err()
		cancel()
		if ctxErr != nil {
			return fmt.Errorf("Claude wrapper self-test %s: %w", strings.Join(args, " "), ctxErr)
		}
		if args[0] == "daemon" {
			firstLine, _, _ := strings.Cut(strings.ToLower(strings.TrimSpace(string(out))), "\n")
			disabled := (strings.Contains(firstLine, "agent view") || strings.Contains(firstLine, "agent-view")) &&
				(strings.Contains(firstLine, "disabled") || strings.Contains(firstLine, "not enabled"))
			var exitErr *exec.ExitError
			if disabled && (err == nil || errors.As(err, &exitErr)) {
				continue
			}
			if err == nil && strings.Contains(firstLine, "claude daemon") {
				continue
			}
			return fmt.Errorf("Claude wrapper self-test daemon --help: expected daemon usage or agent-view-disabled notice (exit: %v), got %q", err, out)
		}
		if err != nil {
			return fmt.Errorf("Claude wrapper self-test --version: %w; output: %q", err, out)
		}
	}
	return nil
}

func runUninstallClaudeShim(args []string) error {
	fs := flag.NewFlagSet("uninstall-claude-shim", flag.ContinueOnError)
	force := fs.Bool("force", false, "remove the file even if it doesn't look like a C3 shim")
	target := fs.String("path", "", "install path (default: $HOME/.local/bin/claude)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	installPath := *target
	if installPath == "" {
		installPath = filepath.Join(home, ".local", "bin", "claude")
	}

	removed, err := uninstallClaudeShim(installPath, *force)
	if err != nil {
		return err
	}
	if removed {
		fmt.Printf("removed %s\n", installPath)
	} else {
		fmt.Printf("nothing to remove at %s\n", installPath)
	}
	return nil
}

// uninstallClaudeShim is idempotent:
//   - Missing path → returns (false, nil).
//   - Symlink → assume prior shim install; remove unconditionally.
//   - Regular file with shim sentinel → remove.
//   - Regular file without sentinel and not force → refuse.
//   - force=true → remove regardless.
func uninstallClaudeShim(installPath string, force bool) (bool, error) {
	info, err := os.Lstat(installPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || force {
		if err := os.Remove(installPath); err != nil {
			return false, err
		}
		return true, nil
	}
	isShim, err := fileContainsSentinel(installPath, shimSentinel)
	if err != nil {
		return false, fmt.Errorf("inspect %s: %w", installPath, err)
	}
	if !isShim {
		return false, fmt.Errorf("refusing to remove non-shim file at %s; pass --force to override", installPath)
	}
	if err := os.Remove(installPath); err != nil {
		return false, err
	}
	return true, nil
}

// fileContainsSentinel returns true iff the file contains the given byte
// sequence anywhere in its contents. Used to safely detect a previously-
// installed (and possibly copied, not symlinked) claude-shim binary.
func fileContainsSentinel(path, sentinel string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	// Scan in 64KB chunks with a slop overlap of len(sentinel) to handle
	// the case where the sentinel straddles a chunk boundary.
	chunk := make([]byte, 65536)
	slop := []byte(sentinel)
	overlap := len(slop) - 1
	if overlap < 0 {
		overlap = 0
	}
	buf := make([]byte, 0, len(chunk)+overlap)
	for {
		n, err := f.Read(chunk)
		if n > 0 {
			buf = append(buf, chunk[:n]...)
			if bytes.Contains(buf, []byte(sentinel)) {
				return true, nil
			}
			// keep only last `overlap` bytes for boundary-spanning matches
			if len(buf) > overlap {
				buf = append(buf[:0], buf[len(buf)-overlap:]...)
			}
		}
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
}
