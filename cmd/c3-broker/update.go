package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/osutil"
	"github.com/Andrometiq/c3/internal/updater"
	"github.com/Andrometiq/c3/internal/version"
)

const updateUsage = `c3-broker update — check for and install the latest C3 release.

Usage:
  c3-broker update           Download the latest release, verify its SHA256SUMS,
                             and replace the installed binaries in place.
  c3-broker update --check   Report the current vs latest version without
                             downloading or installing anything.

A successful install bounces the broker. Reconnecting Claude adapters receive
an upgrade hint and replace themselves when their MCP session can resume safely.

Windows: --check works, but installation is refused because live .exe files
cannot be replaced safely. Fully quit C3, then re-extract the release tarball.
`

// updateCheckTimeout / updateRunTimeout bound the CLI's own context; the updater
// package has its own per-request client timeouts inside these.
const (
	cliCheckTimeout = 30 * time.Second
	cliRunTimeout   = 15 * time.Minute
)

// runUpdate implements `c3-broker update [--check]`. It operates on the version
// of THIS binary (the one being run) and installs into the directory this binary
// lives in — so `c3-broker update` from a shell updates the on-disk c3-broker
// and its core sibling binaries, then bounces the running daemon.
func runUpdate(args []string) error {
	checkOnly := false
	for _, a := range args {
		switch a {
		case "--check":
			checkOnly = true
		case "-h", "--help":
			fmt.Print(updateUsage)
			return nil
		default:
			return fmt.Errorf("unknown argument %q (try --check or --help)", a)
		}
	}

	cur := version.Current()
	if version.IsDev() {
		// A dev build has no release identity to compare against or update to.
		fmt.Printf("c3-broker update: this is a dev build (no embedded release version) — nothing to update.\n" +
			"Install a prebuilt release binary, or build a tagged release (`make dist`).\n")
		return nil
	}

	if checkOnly {
		ctx, cancel := context.WithTimeout(context.Background(), cliCheckTimeout)
		defer cancel()
		res, err := updater.CheckOnly(ctx, cur, updater.DefaultClient())
		if err != nil {
			return fmt.Errorf("check failed: %w", err)
		}
		if !res.UpdateAvailable {
			fmt.Printf("c3 is up to date (running %s; latest %s).\n", cur, res.LatestVersion)
			return nil
		}
		fmt.Printf("c3 update available: %s → %s.\nRun `c3-broker update` (or /c3:update) to install.\n",
			cur, res.LatestVersion)
		return nil
	}

	fmt.Printf("c3-broker update: checking for a newer release (running %s)…\n", cur)
	ctx, cancel := context.WithTimeout(context.Background(), cliRunTimeout)
	defer cancel()
	res, err := updater.Update(ctx, updater.Options{CurrentVersion: cur, Client: updater.DefaultClient()})
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	if !res.Installed {
		fmt.Printf("c3 is already up to date (running %s; latest %s).\n", cur, res.LatestVersion)
		return nil
	}
	if err := bounceUpgradeBroker(); err != nil {
		return err
	}
	printPostInstall(res.LatestVersion)
	return nil
}

// printPostInstall tells the user what happened and what to do next after a
// successful binary swap and broker bounce.
func printPostInstall(newVersion string) {
	fmt.Printf("C3 binaries updated to %s. The broker bounce triggers adapter upgrade hints. Compatible open Claude sessions upgrade themselves; older or incompatible adapters show a notice to run /mcp and reconnect c3.\n", newVersion)
	fmt.Println("Plugin slash commands and hooks update separately through /plugin.")
}

// runningBrokerPID returns the pid of a live broker from the pid file, or 0 if
// none is running. Best-effort: a parse error or dead pid ⇒ 0.
func runningBrokerPID() int {
	pidFile, err := broker.PidFilePath()
	if err != nil {
		return 0
	}
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0
	}
	if !osutil.ProcessSignalable(pid) {
		return 0 // not alive (ESRCH) or not ours (EPERM treated as not-actionable)
	}
	return pid
}

// runVersion prints this binary's build version.
func runVersion() error {
	fmt.Println(version.Current())
	return nil
}
