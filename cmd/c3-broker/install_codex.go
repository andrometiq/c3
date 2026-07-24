package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func runInstallCodexShim(args []string) error {
	fs := flag.NewFlagSet("install-codex-shim", flag.ContinueOnError)
	force := fs.Bool("force", false, "replace a target even if it is a regular file rather than a symlink")
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
	launcher := filepath.Join(filepath.Dir(exe), "codex")
	if _, err := os.Stat(launcher); err != nil {
		return fmt.Errorf("codex launcher not found next to c3-broker at %s; install the release tarball's binaries or run `go install ./cmd/...` first", launcher)
	}
	installed, err := installCodexShims(home, launcher, *force)
	if err != nil {
		return err
	}
	if len(installed) == 0 {
		fmt.Printf("nothing to do: %s is already C3's codex launcher\n", launcher)
		return nil
	}
	for _, path := range installed {
		fmt.Printf("%s -> %s\n", path, launcher)
	}
	return nil
}

func absClean(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		return filepath.Clean(p)
	}
	return abs
}

// isLauncherItself reports whether a shim target IS the launcher binary, in
// which case shimming it would destroy it.
//
// An already-installed shim is deliberately NOT "the launcher itself": it is a
// SYMLINK that resolves to the launcher, and re-pointing it is the idempotent
// re-install we want. So os.SameFile is consulted only for a non-symlink target,
// which catches the real case (a symlinked parent directory making two different
// paths name the same file) without swallowing the refresh.
func isLauncherItself(target, launcher string) bool {
	absT, absL := absClean(target), absClean(launcher)
	if absT == absL {
		return true
	}
	fi, err := os.Lstat(absT)
	if err != nil || fi.Mode()&os.ModeSymlink != 0 {
		return false
	}
	ft, err := os.Stat(absT)
	if err != nil {
		return false
	}
	fl, err := os.Stat(absL)
	if err != nil {
		return false
	}
	return os.SameFile(ft, fl)
}

func installCodexShims(home, launcher string, force bool) ([]string, error) {
	targets := []string{filepath.Join(home, ".local", "bin", "codex")}
	nvmBins, err := filepath.Glob(filepath.Join(home, ".nvm", "versions", "node", "*", "bin"))
	if err != nil {
		return nil, err
	}
	for _, bin := range nvmBins {
		targets = append(targets, filepath.Join(bin, "codex"))
	}

	installed := make([]string, 0, len(targets))
	for _, target := range targets {
		// NEVER shim the launcher onto itself. On the prebuilt install the
		// binaries land in ~/.local/bin, so `launcher` IS the first target: the
		// remove+symlink below would delete C3's own shipped codex binary and
		// replace it with a self-referential symlink, leaving the user's `codex`
		// ELOOP-broken with the binary gone — and, because the nvm entries are
		// re-pointed at that broken link, their real codex gone too. There is
		// nothing to shim when the target already is the launcher.
		// (v0.1.0 release audit, 2026-07-25.)
		if isLauncherItself(target, launcher) {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return installed, err
		}
		// A symlink here is the expected case (a previous shim, or the npm/nvm
		// symlink we are deliberately re-pointing). A REGULAR file is somebody's
		// actual codex binary, and removing it destroys it — refuse without an
		// explicit --force, mirroring install-claude-shim's contract.
		if fi, lerr := os.Lstat(target); lerr == nil && fi.Mode().IsRegular() && !force {
			return installed, fmt.Errorf("refusing to replace the regular file at %s: it is not a symlink, so it may be your real codex binary; pass --force to override", target)
		}
		if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
			return installed, err
		}
		if err := os.Symlink(launcher, target); err != nil {
			return installed, err
		}
		installed = append(installed, target)
	}
	return installed, nil
}
