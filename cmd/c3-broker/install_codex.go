package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Andrometiq/c3/internal/codexlauncher"
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
	staged := filepath.Join(home, ".local", "libexec", "c3", "codex")
	if err := ensureCodexLauncher(launcher, staged, *force, codexlauncher.IsC3); err != nil {
		return err
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

// ensureCodexLauncher materializes the opt-in launcher next to c3-broker from
// the off-PATH staging location used by the install guide. A file that is not
// positively identified as C3's launcher is never replaced without --force.
// If both paths are C3 launchers, the staged copy refreshes the live one.
func ensureCodexLauncher(launcher, staged string, force bool, isC3 func(string) bool) error {
	liveIsC3 := isC3(launcher)
	stagedIsC3 := isC3(staged)

	if liveIsC3 && !stagedIsC3 {
		// Legacy/source installs may already have a valid launcher but no staged
		// copy. Keep using it.
		return nil
	}
	if !stagedIsC3 {
		return fmt.Errorf("C3 codex launcher is not staged at %s; re-run install §1 (prebuilt) or build it with `go build -o %s ./cmd/codex` from the C3 source tree", staged, staged)
	}

	if _, err := os.Lstat(launcher); err == nil && !liveIsC3 && !force {
		return fmt.Errorf("refusing to replace %s: it is not C3's codex launcher, so it may be your real Codex binary; pass --force to override", launcher)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	return replaceExecutable(staged, launcher)
}

// replaceExecutable copies src to a temporary sibling, then renames it over
// dst. The rename keeps a refresh atomic on Unix and never writes through a
// pre-existing symlink.
func replaceExecutable(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".c3-codex-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := io.Copy(tmp, in); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o755); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, dst)
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
