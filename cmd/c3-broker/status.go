package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/version"
)

// runStatus prints a read-only health check.
//
// Sections:
//   - Daemon: pid file path + alive?
//   - Socket: path + reachable?
//   - Mappings: parses + validates? (or error)
//   - Channels: each name + (if alive) basic info from config
//   - Live claims: live route table via OpListClaims (or a note if the broker is down)
func runStatus() error {
	var b strings.Builder
	fmt.Fprintln(&b, "C3 broker status")
	fmt.Fprintln(&b, "================")
	// Build version of THIS c3-broker binary (the CLI tool). "dev" for an
	// uninjected local build. The running daemon may differ after a swap-but-not-
	// yet-restarted update; health.json carries the daemon's own version.
	fmt.Fprintf(&b, "  version:   %s\n", version.Current())

	// Daemon liveness via pid file + flock probe.
	pidFile := broker.PidFilePath()
	fmt.Fprintf(&b, "  pid file:  %s\n", pidFile)
	if data, err := os.ReadFile(pidFile); err == nil && len(data) > 0 {
		fmt.Fprintf(&b, "  pid:       %s", string(data))
	} else {
		fmt.Fprintln(&b, "  pid:       (not running — no pid file)")
	}
	fmt.Fprintf(&b, "  log file:  %s\n", broker.LogPath())

	// Socket.
	sockPath := broker.SocketPath()
	fmt.Fprintf(&b, "  socket:    %s", sockPath)
	if _, err := os.Stat(sockPath); err == nil {
		c, dialErr := net.Dial("unix", sockPath)
		if dialErr == nil {
			fmt.Fprintln(&b, "  ✓ reachable")
			_ = c.Close()
		} else {
			fmt.Fprintf(&b, "  (stat ok, dial: %v)\n", dialErr)
		}
	} else {
		fmt.Fprintln(&b, "  (not present)")
	}

	// Mappings file.
	mfPath, _ := mappings.DefaultPath()
	fmt.Fprintf(&b, "  mappings:  %s\n", mfPath)
	mf, err := mappings.Read(mfPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintln(&b, "             (does not exist — run `c3-broker setup`)")
		} else {
			fmt.Fprintf(&b, "             ✗ parse error: %v\n", err)
		}
	} else {
		if err := mf.Validate(); err != nil {
			fmt.Fprintf(&b, "             ✗ validate error: %v\n", err)
		} else {
			fmt.Fprintf(&b, "             ✓ schema_version=%d, %d channels, %d mappings, %d plugins\n",
				mf.SchemaVersion, len(mf.Channels), len(mf.Mappings), len(mf.Plugins))
		}
	}

	// Channels (config-only since this is read-only against a running daemon).
	if mf != nil {
		fmt.Fprintln(&b, "Channels:")
		for name, cc := range mf.Channels {
			tokenSet := cc.BotToken != ""
			fmt.Fprintf(&b, "  - %-10s token=%v default_group=%q groups=%d topics=%d\n",
				name, tokenSet, cc.DefaultGroup, len(cc.Groups), len(cc.Topics))
		}
	}

	// Plugins (config-only).
	if mf != nil && len(mf.Plugins) > 0 {
		fmt.Fprintln(&b, "Plugins:")
		for name, cfg := range mf.Plugins {
			enabled, _ := cfg["enabled"].(bool)
			fmt.Fprintf(&b, "  - %-10s enabled=%v\n", name, enabled)
		}
	}

	// Channel health via OpListHealth. Shows the last fetch-health edge per
	// channel (DOWN since … / UP). Only channels that have reported at least one
	// edge appear; a freshly-started healthy broker shows "(no health events
	// reported yet)".
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Channel health:")
	if healthList, err := fetchHealthList(); err != nil {
		fmt.Fprintf(&b, "  (broker unreachable: %v)\n", err)
	} else if len(healthList.Health) == 0 {
		fmt.Fprintln(&b, "  (no health events reported yet)")
	} else {
		for _, h := range healthList.Health {
			ch := h.Channel
			if ch == "" {
				ch = "channel"
			}
			if h.State == "down" {
				since := time.Unix(h.SinceUnix, 0).Format("15:04")
				reason := h.Reason
				if reason == "" {
					reason = "transport failures"
				}
				fmt.Fprintf(&b, "  • %s fetch: DOWN since %s (%d consecutive %s, down %s)\n",
					ch, since, h.Consec, reason, (time.Duration(h.DownForSec) * time.Second).String())
			} else {
				fmt.Fprintf(&b, "  • %s fetch: UP\n", ch)
			}
		}
	}

	// Live claims via OpListClaims (transient client → broker). When the
	// broker is up, this shows the actual route table; when it's down, we
	// note that fact instead of the old apologetic placeholder.
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Live claims:")
	if claimsList, err := fetchClaimsList(); err != nil {
		fmt.Fprintf(&b, "  (broker unreachable: %v)\n", err)
	} else if len(claimsList.Claims) == 0 {
		fmt.Fprintln(&b, "  (no claims)")
	} else {
		for _, c := range claimsList.Claims {
			route := fmt.Sprintf("%s/%d", c.Channel, c.ChatID)
			if c.HasTopic {
				if c.TopicName != "" {
					route += fmt.Sprintf("/topic-%d (%q)", c.TopicID, c.TopicName)
				} else {
					route += fmt.Sprintf("/topic-%d", c.TopicID)
				}
			} else {
				route += "/dm"
			}
			liveness := "connected"
			if !c.Connected {
				liveness = "disconnected (claim survives while pid alive)"
			}
			fmt.Fprintf(&b, "  • %s — held by %s pid %d conn=%d [%s]\n",
				route, c.HolderCLI, c.HolderPID, c.ConnID, liveness)
		}
	}

	fmt.Print(b.String())
	return nil
}

// runValidate parses + validates the mappings file at args[0] (or default
// path). Exits 0 on valid, 1 on invalid.
func runValidate(args []string) error {
	path := ""
	if len(args) > 0 {
		path = args[0]
	}
	if path == "" {
		var err error
		path, err = mappings.DefaultPath()
		if err != nil {
			return err
		}
	}
	mf, err := mappings.Read(path)
	if err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	if err := mf.Validate(); err != nil {
		return fmt.Errorf("validate %s: %w", path, err)
	}
	fmt.Printf("✓ %s is valid (schema_version=%d, %d channels, %d mappings)\n",
		path, mf.SchemaVersion, len(mf.Channels), len(mf.Mappings))
	return nil
}

// runRelease would drop the claim on a route bound to args[0] (cwd) via a
// release-by-cwd IPC op. That op is deferred to v2; for v1 there is no
// broker-side release, so this prints the two supported ways to free a held
// topic instead of failing opaquely. (No `os.Exit(failure)` — this is guidance,
// not an error: the caller in main.go only treats a non-nil return as failure.)
func runRelease(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: c3-broker release <cwd>")
	}
	cwd := args[0]
	fmt.Printf(`c3-broker release is not a live operation in v1 (the release-by-cwd IPC op is v2).

To free the topic bound to %q, use one of the two supported paths:

  1. From the session that HOLDS it: run the `+"`/c3:detach`"+` tool (or just
     `+"`/exit`"+` that CLI). The claim drops as soon as that session lets go.

  2. From the NEW session that wants it: attach by the topic's name and confirm
     the force-steal, e.g. `+"`/c3:attach <topic>`"+` then re-invoke attach with
     steal=true when prompted. This evicts the current holder.

Run `+"`c3-broker status`"+` (or the `+"`topics`"+` tool) to see which session currently
holds the route.
`, cwd)
	return nil
}
