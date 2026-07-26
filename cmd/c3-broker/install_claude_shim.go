package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

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

// installClaudeShim places a symlink at installPath pointing at launcher.
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

	info, statErr := os.Lstat(installPath)
	switch {
	case statErr != nil && errors.Is(statErr, os.ErrNotExist):
		// Fresh install.
	case statErr != nil:
		return statErr
	case info.Mode()&os.ModeSymlink != 0:
		// Existing symlink — may be a previous shim install (target ==
		// launcher), or a user-curated link pointing at the real claude
		// binary. In the latter case, persist the resolved real-claude
		// path to ~/.config/c3/claude-shim.json so the shim runtime can
		// re-find it after we replace this symlink. See TODO.md #17 fix
		// option (a), locked 2026-05-18.
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
				return nil
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

	if statErr == nil {
		if err := os.Remove(installPath); err != nil {
			return err
		}
	}
	return os.Symlink(launcher, installPath)
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
