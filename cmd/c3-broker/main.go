// c3-broker is the C3 daemon and operational tool. Subcommands:
//
//	c3-broker             (default) — run as daemon
//	c3-broker setup       — interactive config; calls Telegram getMe; writes mappings.json
//	c3-broker status      — read-only health check
//	c3-broker validate    — parse + validate mappings.json
//	c3-broker release CWD — drop the claim on a route bound to CWD
//	c3-broker reload-config — re-read mappings.json without dropping live claims (running broker only)
//	c3-broker install-codex-shim — install Codex launcher symlinks
//	c3-broker install-claude-shim — install Claude Code launcher wrapper
//	c3-broker uninstall-claude-shim — remove the installed Claude Code wrapper
//	c3-broker update [--check] — install the latest GitHub release (checksum-verified atomic swap)
//	c3-broker version — print this binary's build version
//
// Singleton-per-machine via flock on $XDG_RUNTIME_DIR/c3-broker.pid (or
// fallback). Spawned by adapters via exec.Command + setsid; runs until its
// parent process group is killed or it receives SIGTERM/SIGINT.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/channel/telegram"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/osutil"
	"github.com/Andrometiq/c3/internal/plugin"
	"github.com/Andrometiq/c3/internal/plugin/builtins/stt"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
)

// builtinPlugins lists the plugins compiled into the broker. Order matters
// for chain hooks (lower priority runs first); broker.RegisterBuiltinPlugins
// runs them in slice order.
var builtinPlugins = []broker.BuiltinPlugin{
	{Name: stt.Name, Register: func(h plugin.Host) error { return stt.Register(h) }},
}

// Exit codes follow BSD sysexits(3) where applicable so shell scripts can
// branch on the cause. Generic runtime failures still use 1 (EX_GENERIC).
const (
	exitOK      = 0  // success
	exitFailure = 1  // generic runtime error
	exitUsage   = 2  // unknown subcommand / malformed args
	exitDataErr = 65 // EX_DATAERR — mappings.json invalid
	exitNoInput = 66 // EX_NOINPUT — mappings.json missing/unreadable
	exitConfig  = 78 // EX_CONFIG — config-time failure (setup, install-codex-shim)
)

func main() {
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "setup":
			if err := runSetup(); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker setup: %v\n", err)
				os.Exit(exitConfig)
			}
			return
		case "status":
			if err := runStatus(); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker status: %v\n", err)
				os.Exit(exitFailure)
			}
			return
		case "topics":
			if err := runTopics(); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker topics: %v\n", err)
				os.Exit(exitFailure)
			}
			return
		case "validate":
			if err := runValidate(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker validate: %v\n", err)
				// validate's job is exactly to test config integrity, so
				// surface EX_DATAERR (invalid contents) when we can't
				// even read/parse it.
				os.Exit(exitDataErr)
			}
			return
		case "release":
			if err := runRelease(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker release: %v\n", err)
				os.Exit(exitFailure)
			}
			return
		case "install-codex-shim":
			if err := runInstallCodexShim(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker install-codex-shim: %v\n", err)
				os.Exit(exitConfig)
			}
			return
		case "pair":
			if err := runPair(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker pair: %v\n", err)
				os.Exit(exitFailure)
			}
			return
		case "ping":
			if err := runPing(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker ping: %v\n", err)
				os.Exit(exitFailure)
			}
			return
		case "sessions":
			if err := runSessions(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker sessions: %v\n", err)
				os.Exit(exitFailure)
			}
			return
		case "install-claude-shim":
			if err := runInstallClaudeShim(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker install-claude-shim: %v\n", err)
				os.Exit(exitConfig)
			}
			return
		case "uninstall-claude-shim":
			if err := runUninstallClaudeShim(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker uninstall-claude-shim: %v\n", err)
				os.Exit(exitConfig)
			}
			return
		case "install-grok":
			if err := runInstallGrok(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker install-grok: %v\n", err)
				os.Exit(exitConfig)
			}
			return
		case "install-agy":
			if err := runInstallAgy(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker install-agy: %v\n", err)
				os.Exit(exitConfig)
			}
			return
		case "install-desktop":
			if err := runInstallDesktop(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker install-desktop: %v\n", err)
				os.Exit(exitConfig)
			}
			return
		case "session-hook":
			// SessionStart hook (c3 plugin). NEVER connects to the broker and is
			// designed to exit 0 even on bad input — a hook that errors would
			// break the user's session. runSessionHook returns nil unconditionally.
			_ = runSessionHook()
			return
		case "update":
			if err := runUpdate(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker update: %v\n", err)
				os.Exit(exitFailure)
			}
			return
		case "version", "--version", "-v":
			if err := runVersion(); err != nil {
				fmt.Fprintf(os.Stderr, "c3-broker version: %v\n", err)
				os.Exit(exitFailure)
			}
			return
		case "--help", "-h", "help":
			fmt.Print(usage)
			return
		default:
			fmt.Fprintf(os.Stderr, "c3-broker: unknown subcommand %q\n%s", os.Args[1], usage)
			os.Exit(exitUsage)
		}
	}

	if err := runDaemon(); err != nil {
		fmt.Fprintf(os.Stderr, "c3-broker: %v\n", err)
		os.Exit(exitFailure)
	}
}

const usage = `c3-broker — C3 daemon and operational tool.

Usage:
  c3-broker             Run as daemon (singleton-per-machine).
  c3-broker setup       Interactive config — gather bot token, DM chat id,
                        group chat id; validate against Telegram; write
                        ~/.config/c3/mappings.json (mode 0600).
  c3-broker status      Read-only health check (broker, socket, mappings,
                        channels, plugins, live claims).
  c3-broker topics      List known topics + claim state (queries the
                        running broker via the unix socket).
  c3-broker validate [path]
                        Parse + validate mappings.json. Defaults to default
                        path. Exits 0 on valid, 1 on invalid.
  c3-broker release <cwd>
                        Drop the claim on a route bound to <cwd>.
                        (Runtime op against a running broker.)
  c3-broker install-codex-shim
                        Symlink the Go Codex launcher into ~/.local/bin and
                        Node-manager bin directories.
  c3-broker install-claude-shim [--force] [--path PATH]
                        Install a Claude Code wrapper at PATH (default
                        ~/.local/bin/claude) that auto-injects
                        --dangerously-load-development-channels plugin:c3@c3
                        when the user runs claude. Idempotent — preserves
                        the flag if already passed.
  c3-broker uninstall-claude-shim [--force] [--path PATH]
                        Remove the installed Claude Code wrapper.
  c3-broker install-grok
                        Configure Grok Build for C3: enable leader mode,
                        pin mcp_servers.c3 → c3-grok-adapter, print plugin
                        install steps. Live Telegram inject requires leader.
  c3-broker install-desktop [--config PATH]
                        Register the C3 adapter with Claude Desktop: merge an
                        mcpServers.c3 entry into claude_desktop_config.json at
                        the per-OS default path (or --config PATH, e.g. an MSIX
                        install). Poll-only bridge; every other server is kept.
  c3-broker pair [dm|group <chat_id>]
                        Arm a pairing window on the running broker and print
                        the generated 4-digit code. Default target is "dm".
  c3-broker ping        Send a one-shot "this is me" reply to the Telegram
                        route the calling session currently holds, so the
                        human reading Telegram can identify which CLI tab
                        owns the topic. Matches by CWD against live stubs.
  c3-broker sessions    List every live Claude Code / Codex session the
                        broker is currently tracking, with its CWD and
                        attached topic. Marks the calling session if it
                        can be matched via a parent-PID walk.
  c3-broker session-hook
                        Internal: the c3 plugin's SessionStart hook. Reads
                        the hook JSON on stdin, maps the ephemeral MCP
                        instance id to the stable --resume session id, and
                        writes a handoff the adapter reads to auto-attach a
                        resumed session. Never touches the broker socket;
                        always exits 0.
  c3-broker update [--check]
                        Update C3 to the latest GitHub release: download the
                        platform tarball, verify it against SHA256SUMS, and
                        stage and replace the eight core binaries in place (and
                        refresh a verified, already-installed C3 Codex launcher). --check only
                        reports current vs latest without installing. Does NOT
                        stop a running broker (it keeps its old code until it
                        restarts); prints how to bounce it after installing.
  c3-broker version     Print this binary's build version ("dev" if unversioned).
  c3-broker --help      This text.
`

func runDaemon() (err error) {
	pidFile, err := broker.PidFilePath()
	if err != nil {
		return fmt.Errorf("resolve broker pid file: %w", err)
	}
	lock, err := broker.AcquireSingleton(pidFile)
	if err != nil {
		// Sibling broker already running; expected when an adapter races and
		// we lose. Exit silently.
		return nil
	}
	defer lock.Release()

	logFile, logPath := broker.SetupLogging()
	if logFile != nil {
		defer logFile.Close()
		fmt.Fprintf(os.Stderr, "c3-broker: log file %s\n", logPath)
	}

	// From here on, log any FATAL error to broker.log (SetupLogging wired the
	// stdlib logger above). An adapter-spawned broker's stderr is /dev/null, so
	// without this every boot-time death is invisible — no breadcrumb to debug,
	// which is exactly the silent-death class the recovery audit flagged.
	// Registered AFTER logFile.Close so (LIFO) it runs while the file is open.
	defer func() {
		if err != nil {
			log.Printf("c3-broker: FATAL: %v", err)
		}
	}()

	// v0.1.0-rc1's updater copied only binaries. A stable release broker carries
	// the runtime and repairs that legacy layout on its first restart. Failure is
	// loud but non-fatal: text routing must stay available when STT storage is
	// unwritable.
	if repaired, repairErr := ensureEmbeddedReleaseSTT(); repairErr != nil {
		log.Printf("c3-broker: WARNING embedded STT repair failed: %v", repairErr)
		fmt.Fprintf(os.Stderr, "c3-broker: WARNING embedded STT repair failed: %v\n", repairErr)
	} else if repaired {
		log.Printf("c3-broker: installed embedded STT runtime into the user-data fallback")
	}

	mfPath, err := mappings.DefaultPath()
	if err != nil {
		return fmt.Errorf("resolve mappings path: %w", err)
	}
	// Resilient load: ReadWithBak falls back to the last-good mappings.json.bak
	// if the primary is present but corrupt (a hand-edit typo), so a bad edit no
	// longer dead-ends boot. A MISSING primary still seeds a skeleton; a corrupt
	// primary with NO usable .bak fails loudly (we do NOT seed a skeleton over a
	// real-but-broken config — that could silently mis-route messages).
	mf, usedBak, rerr := mappings.ReadWithBak(mfPath)
	if rerr != nil {
		if os.IsNotExist(rerr) {
			mf = &mappings.MappingsFile{
				SchemaVersion: 1,
				Channels:      map[string]mappings.ChannelConfig{},
				Mappings:      map[string]mappings.Mapping{},
			}
			if mkErr := os.MkdirAll(filepath.Dir(mfPath), 0700); mkErr != nil {
				return fmt.Errorf("mkdir mappings parent: %w", mkErr)
			}
			if wErr := mappings.Write(mfPath, mf); wErr != nil {
				return fmt.Errorf("write skeleton mappings: %w", wErr)
			}
			log.Printf("c3-broker: wrote skeleton %s — run `c3-broker setup` to configure", mfPath)
			fmt.Fprintf(os.Stderr, "c3-broker: wrote skeleton %s — run `c3-broker setup` to configure\n", mfPath)
		} else {
			return fmt.Errorf("read mappings %s (present but invalid, no usable .bak): %w", mfPath, rerr)
		}
	} else if usedBak {
		log.Printf("c3-broker: WARNING %s was invalid; loaded %s.bak instead — re-run `c3-broker setup` or fix the file", mfPath, mfPath)
		fmt.Fprintf(os.Stderr, "c3-broker: WARNING %s was invalid; loaded %s.bak instead\n", mfPath, mfPath)
	}
	if err := mf.Validate(); err != nil {
		return fmt.Errorf("validate mappings: %w", err)
	}

	// Prune expired auto-attach-on-resume entries so session_attachments doesn't
	// grow unbounded. Best-effort: a write failure is non-fatal (the in-memory
	// pruned state is still used this run).
	if n := mf.PruneSessionAttachments(time.Now().UTC(), broker.SessionAttachmentTTL); n > 0 {
		log.Printf("c3-broker: pruned %d expired session attachment(s)", n)
		if wErr := mappings.Write(mfPath, mf); wErr != nil {
			log.Printf("c3-broker: WARNING failed to persist session-attachment prune: %v", wErr)
		}
	}

	// Bound the SessionStart-hook handoff dir (instance-id → stable-id files
	// written by `c3-broker session-hook`). Best-effort; a leftover handoff is
	// harmless (the adapter only reads its own instance id), but stale files
	// shouldn't accumulate. 24h is well past the seconds-scale read window.
	if n := sessionhandoff.PruneStale(24*time.Hour, time.Now()); n > 0 {
		log.Printf("c3-broker: pruned %d stale session-handoff entr(ies)", n)
	}

	br := broker.New(mf)

	// Register builtin plugins BEFORE channels start emitting inbound, so the
	// plugin pipeline is ready when first messages arrive.
	if err := br.RegisterBuiltinPlugins(builtinPlugins); err != nil {
		return fmt.Errorf("register plugins: %w", err)
	}

	// One-time startup write of the connectivity status file the Claude Code
	// status line reads. At boot lastHealth is empty ⇒ writes "{}", which
	// clears any stale file left by a prior crash without falsely asserting
	// up/down. Ambient-only (never via NotifyHealth — no spurious popup). Done
	// before channels start emitting so the empty boot snapshot can never race
	// a detection-driven write.
	br.WriteHealthFile()

	// Slow refresh ticker: re-write health.json every healthRefreshInterval
	// regardless of health edges, so written_unix stays current while the
	// broker is alive. A reader treats a stale written_unix (or a dead pid) as
	// broker-down — closing the silent-death gap where a crashed broker froze
	// health.json at its last "up" value (audit health-observability-2). The
	// goroutine exits on br.Shutdown() (ctx cancel), so it never leaks.
	br.StartHealthRefresh()

	// Ask reaper: periodically expire stale `ask` round-trips (untapped, or
	// multi-select toggled-but-never-Done) and best-effort clear their live
	// keyboards, so an ask can't leak or leave a tappable keyboard past the
	// adapter's answer timeout (FIX-1). Exits on br.Shutdown() (ctx cancel).
	br.StartAskReaper()

	// Signal handling is registered BEFORE the update checker so the checker's
	// self-restart callback can post into our own sigC (a portable stand-in for
	// the old syscall.Kill-self). SIGTERM/SIGINT shut down; reload signals (SIGHUP
	// on unix, none on Windows) trigger a config reload in the loop below.
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, append([]os.Signal{syscall.SIGTERM, syscall.SIGINT}, osutil.ReloadSignals()...)...)

	// Auto-update checker: always-on availability check (surfaces the status-line
	// notice + log), and — when mappings.auto_update is enabled — self-updates and
	// requests a graceful restart. The callback posts SIGTERM into our own sigC so
	// the normal signal path below drains (srv.Stop + Shutdown) and exits 0; an
	// adapter reconnect then auto-spawns the freshly-swapped broker binary. Posting
	// to our own channel (rather than syscall.Kill) is fully portable — SIGTERM is a
	// valid value on Windows even though the OS never delivers it. Dev builds (no
	// injected version) disable the checker. Exits on ctx cancel.
	br.StartUpdateChecker(func() {
		select {
		case sigC <- syscall.SIGTERM:
		default:
		}
	})

	if cc, ok := mf.Channels["telegram"]; ok && cc.BotToken != "" {
		if err := br.RegisterChannel(telegram.New()); err != nil {
			return fmt.Errorf("register telegram channel: %w", err)
		}
	} else {
		fmt.Fprintln(os.Stderr, "c3-broker: no telegram bot_token in mappings.json — running without inbound transport")
	}

	// Default-deny posture (TODO #1): if no users are allowlisted yet,
	// auto-arm DM pairing so the operator can register their account
	// without manually editing mappings.json. The code is logged to
	// broker.log and echoed on stderr so the human sees it on a fresh
	// install. No auto re-arm on TTL expiry — manual /c3:pair after that.
	if code := br.AutoStartDMPairingIfEmpty(); code != "" {
		fmt.Fprintf(os.Stderr,
			"c3-broker: PAIRING — send `%s` to your bot within %v to pair (DM)\n",
			code, broker.PairTTL)
	}

	// Capability marker: tells `/c3:reload-config` we support SIGHUP-driven
	// config reload. Old brokers (pre-2026-05-15) lack this file and the
	// slash command refuses to fire — sending SIGHUP to a broker without
	// a handler terminates it (Go default) and indirectly kills the MCP
	// adapter via CC's recycle behavior. Rewritten on every startup;
	// removed at clean shutdown so a stale file from a crashed broker
	// doesn't falsely advertise capabilities for a future older broker.
	capsPath, err := broker.CapsFilePath()
	if err != nil {
		return fmt.Errorf("resolve broker capabilities file: %w", err)
	}
	if err := os.WriteFile(capsPath, []byte("sighup-reload\npair-mode-start\n"), 0600); err != nil {
		log.Printf("warn: write caps file %s: %v", capsPath, err)
	}
	defer os.Remove(capsPath)

	sockPath, err := broker.SocketPath()
	if err != nil {
		return fmt.Errorf("resolve broker socket: %w", err)
	}
	srv, err := broker.Listen(sockPath, br)
	if err != nil {
		// Sibling broker already serving the socket — same silent exit as
		// flock collision. 2026-05-09: prevents the
		// two-brokers-overlapping-on-the-socket bug after a restart race.
		if errors.Is(err, broker.ErrSiblingListening) {
			fmt.Fprintf(os.Stderr, "c3-broker: %v — exiting silently\n", err)
			return nil
		}
		return fmt.Errorf("listen on socket: %w", err)
	}
	fmt.Fprintf(os.Stderr, "c3-broker: listening on %s (pid %d)\n", sockPath, os.Getpid())

	for sig := range sigC {
		if osutil.IsReloadSignal(sig) {
			// Config reload — re-read mappings.json from disk and swap
			// the in-memory pointer. The /c3:reload-config slash command
			// sends this. Replaces the old /c3:restart-broker bounce
			// (which killed the adapter as a side effect — see
			// 2026-05-14 RESUME notes).
			newMF, err := mappings.Read(mfPath)
			if err != nil {
				log.Printf("SIGHUP: reload %s failed: %v — keeping existing config", mfPath, err)
				continue
			}
			if err := newMF.Validate(); err != nil {
				log.Printf("SIGHUP: validate %s failed: %v — keeping existing config", mfPath, err)
				continue
			}
			br.SetMappings(newMF)
			log.Printf("SIGHUP: reloaded mappings from %s (channels=%d, mappings=%d, plugins=%d)",
				mfPath, len(newMF.Channels), len(newMF.Mappings), len(newMF.Plugins))
			continue
		}
		// SIGTERM / SIGINT — shut down.
		log.Printf("received signal=%v, shutting down", sig)
		break
	}

	// Graceful shutdown with a hard watchdog. srv.Stop() closes the listener AND
	// every live adapter connection (so the HandleConn read loops parked in a
	// blocking ReadFrame return); br.Shutdown() drains the worker pool and stops
	// channels. Both bound their own waits, but as a LAST-RESORT backstop we cap
	// the whole sequence: if it hasn't finished within shutdownWatchdog, force
	// exit(0).
	//
	// A forced exit is SAFE and loss-free BY DESIGN: the durable inbound queue
	// persists every accepted message, and the Telegram offset watermark only
	// advances past a durably-stored update — so an abrupt exit re-delivers at
	// most the in-flight tail on the next start (dedup bounds it). That is the
	// same property that has always made `kill -9` the documented recovery. A
	// bounded forced exit is therefore strictly better than a wedged drain, which
	// keeps the singleton flock and strands auto-update (the freshly installed
	// binary cannot start while the old process holds the lock).
	if !runShutdown(func() { srv.Stop(); br.Shutdown() }, shutdownWatchdog) {
		log.Printf("c3-broker: graceful shutdown did not complete within %s — forcing exit(0). Loss-free by design: the durable queue + offset watermark redeliver the in-flight tail on restart (same guarantee as kill -9).", shutdownWatchdog)
		os.Exit(exitOK)
	}
	return nil
}

// shutdownWatchdog is the hard ceiling on graceful shutdown. srv.Stop and
// br.Shutdown each bound their own waits well under this (5s conn drain + 8s
// worker drain); this only fires if something wedges past all of them. 15s
// leaves headroom over the inner budgets while still beating both any human's
// patience and the Telegram long-poll's ~55s reconnect window, so auto-update's
// respawn is never stranded behind a still-held singleton flock.
const shutdownWatchdog = 15 * time.Second

// runShutdown runs shutdown in a goroutine and reports whether it completed
// within timeout. Factored out of the signal path so the watchdog DECISION is
// unit-testable without a real os.Exit: a test passes a shutdown func that blocks
// forever and asserts runShutdown returns false. On a false return the caller
// (main) force-exits; the leaked goroutine dies with the process.
func runShutdown(shutdown func(), timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		shutdown()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
