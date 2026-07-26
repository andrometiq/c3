package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/osutil"
	"github.com/Andrometiq/c3/internal/spawn"
)

// telegramChannelName is the mappings.json channel key setup manages.
const telegramChannelName = "telegram"

// printShimInstallFailure renders the structured failure surface for a
// failed compulsory claude-shim install. Writes the same block to BOTH
// stdout AND stderr so the user sees it regardless of whether setup is
// driven from a Claude Code agent (which captures stdout in its
// transcript and only intermittently surfaces stderr) or from a raw
// shell (which surfaces stderr more reliably but where the long setup
// transcript can bury a single line).
//
// The block is greppable via the literal header
// `[claude shim NOT installed]`. The actionable next-step (`c3-broker
// install-claude-shim --force`) and an explanation of what --force
// trades off are included in the same block so the user does not have
// to consult docs to recover.
//
// Closes M2 from the 2026-05-19 code review.
func printShimInstallFailure(stdout, stderr io.Writer, err error) {
	block := fmt.Sprintf(`[claude shim NOT installed]
  error: %v

  The shim is required for c3 channels to surface in Claude Code.
  Most common cause: an existing non-shim `+"`~/.local/bin/claude`"+`
  (e.g. one installed by NVM, npm, or a manual symlink to the real
  claude binary).

  To overwrite the existing file:
    c3-broker install-claude-shim --force

  --force overwrites a non-shim file at ~/.local/bin/claude. If this
  is the first time you have run install-claude-shim, the original
  claude symlink target may not have been persisted to
  ~/.config/c3/claude-shim.json yet, and --force will lose the path
  to your real claude binary. Verify a successful one-time install
  without --force first when possible — it persists the resolved
  real-claude path before the shim takes over.
`, err)
	// Write errors on stdout/stderr at this point in setup are
	// non-actionable: the process is about to print the restart
	// instruction and exit; a partial failure-block write is still
	// more useful than a panic. Match the rest of setup.go which
	// also ignores fmt.Fprintf errors on these streams.
	_, _ = io.WriteString(stdout, block)
	_, _ = io.WriteString(stderr, block)
}

// setupUsage documents the setup phases. The bare invocation is the full
// interactive flow (secondary path, for a real terminal); the phased
// subcommands are the building blocks the agent-guided /c3:setup flow
// drives one step at a time.
const setupUsage = `c3-broker setup — configure C3.

Usage:
  c3-broker setup                 Full interactive flow (needs a TTY).
  c3-broker setup token [TOKEN]   Validate + record the bot token. Reads the
                                  token from stdin when no argument is given
                                  (pipe it: printf '%s' "$T" | c3-broker setup token).
  c3-broker setup pair dm [--code NNNN] [--timeout-sec N] [--id USER_ID]
                                  Discover + record your Telegram user id: the
                                  command watches the bot's inbox for the
                                  4-digit code you send it in a DM. --id skips
                                  pairing and records the id directly (last resort).
  c3-broker setup pair group [--code NNNN] [--timeout-sec N] [--name NAME] [--id CHAT_ID]
                                  Same, for a group: send the code in the group
                                  and the group's chat id is discovered and
                                  recorded. --name is the config name for the
                                  group (default: the group's title, else "main").
  c3-broker setup stt             Voice-transcription key setup (interactive or
                                  piped answers).
  c3-broker setup finish          Host integrations (claude shim / codex MCP),
                                  broker restart with the new config, and the
                                  post-setup "what now" summary.

The primary setup experience is /c3:setup inside Claude Code — the agent
drives these phases and walks you through each step.
`

// runSetup is the `c3-broker setup` subcommand entry point. Dispatches on
// the argv following "setup": no args = the full interactive flow; a phase
// name = one composable step of the agent-guided flow. Parsing os.Args here
// (rather than in main.go) keeps the whole setup surface in this file.
func runSetup() error {
	return runSetupWithArgs(os.Args[2:])
}

// runSetupWithArgs is runSetup with the argv injected, for tests.
func runSetupWithArgs(args []string) error {
	if len(args) == 0 {
		return runSetupInteractive()
	}
	switch args[0] {
	case "token":
		return runSetupToken(args[1:])
	case "pair":
		return runSetupPair(args[1:])
	case "stt":
		return runSetupSTT()
	case "finish":
		return runSetupFinish()
	case "--help", "-h", "help":
		// WriteString (not fmt.Print) — the usage text contains a literal
		// printf example that would trip vet's format-directive check.
		_, _ = os.Stdout.WriteString(setupUsage)
		return nil
	default:
		return fmt.Errorf("unknown setup phase %q\n%s", args[0], setupUsage)
	}
}

// runSetupInteractive is the full interactive flow (2026-06-30 rework of the
// 2026-05-19 original, per the fresh-install UX feedback):
//
//  1. Load existing config; derive what is ALREADY configured (#4 — never
//     re-show a completed step).
//  2. Educational preamble + consent gate (C3_NO_PROMPT-aware).
//  3. Bot token — keep a still-valid existing token on enter, else collect
//     + validate via getMe (with inline @BotFather guidance).
//  4. Kick off the core-binary `go install` in the background.
//  5. DM pairing (#5) — a 4-digit code sent to the bot discovers and
//     records the user id; no @userinfobot hunt. Manual id entry survives
//     only as a last-resort line inside the pairing prompt.
//  6. Group checklist (privacy/group/Topics/admin — no "create the bot"
//     step; the token step already covered it) + group pairing (#6): the
//     code sent in the group discovers the chat id.
//  7. STT setup (#3 — short copy), host integrations, broker restart with
//     the new config, and a stand-alone "what now" block (#8/#9/#10).
func runSetupInteractive() error {
	mfPath, err := mappings.DefaultPath()
	if err != nil {
		return err
	}

	r := bufio.NewReader(os.Stdin)

	mf, existed, loadErr := loadOrInitMappings(mfPath)
	if loadErr != nil {
		// Present but unreadable. Setup is the tool that fixes config, so
		// start from a fresh skeleton rather than dead-ending — but say so.
		fmt.Fprintf(os.Stderr, "warning: existing %s is unreadable (%v) — starting from a fresh config\n", mfPath, loadErr)
		mf = skeletonMappings()
		existed = false
	}
	progress := deriveProgress(mf, defaultSTTEnvPath())

	if existed {
		fmt.Printf("Existing config found at %s.\n", mfPath)
		fmt.Println(progress.summaryLine())
		fmt.Println("Setup keeps what is already configured and only asks for what's missing.")
		fmt.Println("(To start over from scratch, delete the file first, then re-run setup.)")
		fmt.Print("Continue? [Y/n]: ")
		if !readBoolDefault(r, true) {
			fmt.Println("Aborted.")
			return nil
		}
		fmt.Println()
	}

	// Preamble then consent gate. Consent honours C3_NO_PROMPT so a
	// non-interactive driver doesn't deadlock.
	printPreamble()
	if !confirmInstall(r) {
		fmt.Println()
		fmt.Println(consentDeclinedMsg)
		return nil
	}
	fmt.Println()

	// Bot token (only true prerequisite); validated via getMe. Must come
	// before the background install kicks off — the install is meaningless
	// if the user abandons setup at the token step.
	token, botUsername, err := promptBotToken(r, progress.Token)
	if err != nil {
		return err
	}
	applyBotToken(mf, token)
	if err := writeMappingsFile(mfPath, mf); err != nil {
		return err
	}

	// Kick off the core-binary `go install`. The opt-in cmd/codex launcher is
	// deliberately excluded so setup cannot replace the user's real Codex CLI.
	// The pairing waits and the
	// group checklist (minutes of user activity) cover this comfortably;
	// cold rebuild of C3 is ~30s, warm ~5s. Cancellable via ctx so a Ctrl-C
	// during the interactive walk doesn't leave a dangling go subprocess.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	installCh := startBackgroundInstall(ctx)

	needDM := progress.DMChatID == 0
	needGroup := progress.GroupChatID == 0

	// Pairing owns the bot's getUpdates stream, so a running broker (which
	// long-polls the same stream) must be stopped first — two consumers
	// steal each other's updates. It is restarted at the end of setup, and —
	// via the deferred restore below — on EVERY early error return in
	// between (a failed pairing must never strand the broker stopped).
	//
	// Stopping is not enough on its own: a live adapter auto-respawns a dead
	// broker within seconds (adapter recoverBroker → connectBroker), and the
	// respawn would re-read the token we just wrote and steal getUpdates —
	// gate-dropping the pairing code. So setup also HOLDS the broker
	// singleton flock for the whole pairing window; a respawned broker then
	// exits silently by design (runDaemon treats a lost flock race as a
	// no-op). The lock is released before anything that (re)starts a broker.
	var pairingLock *broker.SingletonLock
	if needDM || needGroup {
		stopped, note := stopBrokerFn()
		if stopped {
			fmt.Println(note)
		} else if note != "" {
			fmt.Fprintln(os.Stderr, note)
		}
		defer func() {
			// LIFO within this defer: release the flock FIRST so the broker
			// we restore can actually take it. ensureBrokerUpFn is
			// idempotent, so on the success path (where
			// restartBrokerForNewConfig already ran) this is a no-op.
			releasePairingLock(&pairingLock)
			if stopped {
				ensureBrokerUpFn()
			}
		}()
		if lock, lockErr := acquireBrokerPairingLock(); lockErr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not hold the broker singleton lock during pairing (%v) — if an adapter respawns the broker mid-window, pairing may miss your code\n", lockErr)
		} else {
			pairingLock = lock
		}
	}

	if needDM {
		userID, err := pairDMInteractive(r, token, botUsername)
		if err != nil {
			return err
		}
		applyDMPair(mf, userID)
		if err := writeMappingsFile(mfPath, mf); err != nil {
			return err
		}
		fmt.Printf("✓ paired — your Telegram user id is %d (recorded + allowlisted)\n", userID)
	} else {
		// Legacy configs (pre-allowlist schema, or a dropped allowlist) can
		// hold dm_chat_id/groups WITHOUT the allowlist entries — the
		// default-deny gate then drops all inbound while setup reports
		// "already paired". Re-apply the pair mutations idempotently (they
		// only append what's missing).
		if repairAllowlist(mf) {
			if err := writeMappingsFile(mfPath, mf); err != nil {
				return err
			}
			fmt.Println("✓ repaired missing allowlist entries for the configured DM/group (legacy config)")
		}
		fmt.Printf("✓ DM already paired: Telegram user id %d\n", progress.DMChatID)
	}

	if needGroup {
		walkBotGroupChecklist(r)
		capture, err := pairGroupInteractive(r, token, groupPairSenderGate(mf))
		if err != nil {
			return err
		}
		name := promptGroupName(r, capture.ChatTitle)
		applyGroupPair(mf, name, capture.ChatID, capture.ChatTitle)
		if err := writeMappingsFile(mfPath, mf); err != nil {
			return err
		}
		fmt.Printf("✓ paired — group %q has chat_id %d (recorded + allowlisted)\n", name, capture.ChatID)
	} else {
		if repairAllowlist(mf) {
			if err := writeMappingsFile(mfPath, mf); err != nil {
				return err
			}
			fmt.Println("✓ repaired missing allowlist entries for the configured DM/group (legacy config)")
		}
		fmt.Printf("✓ group already configured: %q (chat_id %d)\n", progress.GroupName, progress.GroupChatID)
	}

	// Pairing done — release the flock BEFORE anything below can start a
	// broker (restartBrokerForNewConfig at the end of this flow).
	releasePairingLock(&pairingLock)

	fmt.Printf("✓ wrote %s (mode 0600)\n", mfPath)

	// Join the background install. Almost always instant after the pairing
	// waits, but if the user was fast we wait visibly.
	if err := joinBackgroundInstall(installCh); err != nil {
		// Non-fatal for the rest of the flow — mappings.json is already
		// written, the user can manually run `/c3:build` or
		// the core package set after. Surface the error and continue.
		fmt.Fprintf(os.Stderr, "warning: background `go install` failed: %v\n", err)
		fmt.Fprintln(os.Stderr, "  Run `/c3:build` to retry the core binaries; the Codex launcher remains opt-in via INSTALL.md §5.")
	}

	host := DetectHostCLI()

	// Optional STT setup (most users want this — voice is the primary
	// input channel for c3). Never re-shown once configured (#4).
	sttWritten := progress.STTConfigured
	if progress.STTConfigured {
		fmt.Printf("✓ STT already configured (%s) — run `c3-broker setup stt` to change keys\n", defaultSTTEnvPath())
	} else {
		written, sttErr := promptSTTSetup(r)
		if sttErr != nil {
			// Non-fatal: STT setup is optional. Surface the error and keep
			// the mappings.json writes that already succeeded.
			fmt.Fprintf(os.Stderr, "warning: STT setup skipped: %v\n", sttErr)
		}
		sttWritten = written
	}

	installHostIntegrations(host)

	// #10 (2026-05-18): under Codex, setup typically runs without a
	// connected TTY (the agent invokes it programmatically), so the STT
	// prompts get auto-skipped with empty responses. Tell the user
	// explicitly when no STT env file was written so they don't end up
	// with voice messages silently failing.
	if !sttWritten {
		sttHint(host)
	}

	// Bounce the broker so the freshly-written config is what's live.
	// SIGHUP is NOT enough after a token change — the telegram channel is
	// initialized at broker start (see /c3:reload-config), so stop + respawn.
	fmt.Println()
	restartBrokerForNewConfig()

	fmt.Println()
	fmt.Println(postSetupWhatNow(host))
	return nil
}

// runSetupToken is the `c3-broker setup token` phase: read a bot token from
// argv or stdin, validate it via getMe, and upsert it into mappings.json.
// The agent-guided /c3:setup flow drives this step; piping the token via
// stdin keeps it off the process arg list.
func runSetupToken(args []string) error {
	var token string
	if len(args) > 0 {
		token = strings.TrimSpace(args[0])
	} else {
		data, err := io.ReadAll(io.LimitReader(os.Stdin, 1024))
		if err != nil {
			return fmt.Errorf("read token from stdin: %w", err)
		}
		token = strings.TrimSpace(string(data))
	}
	if token == "" {
		return errors.New("no token provided — pass it as an argument or pipe it via stdin")
	}
	username, err := validateBotToken(token)
	if err != nil {
		return fmt.Errorf("token validation failed: %w", err)
	}
	mfPath, err := mappings.DefaultPath()
	if err != nil {
		return err
	}
	mf, _, err := loadOrInitMappings(mfPath)
	if err != nil {
		return fmt.Errorf("existing %s is unreadable: %w — fix or delete it, then re-run", mfPath, err)
	}
	applyBotToken(mf, token)
	if err := writeMappingsFile(mfPath, mf); err != nil {
		return err
	}
	fmt.Printf("✓ token valid; bot is @%s\n", username)
	fmt.Printf("✓ wrote %s (mode 0600)\n", mfPath)
	fmt.Println("Next: `c3-broker setup pair dm` to discover + record your user id.")
	return nil
}

// runSetupPair is the `c3-broker setup pair dm|group` phase. Unlike
// `c3-broker pair` (which arms a window on the RUNNING broker for adding
// extra users/groups at runtime), this phase runs while setup owns the
// bot's getUpdates stream and DISCOVERS the id — the whole point is that
// the user never has to find a user id or chat id by hand (#5/#6).
func runSetupPair(args []string) error {
	usage := errors.New("usage: c3-broker setup pair dm|group [--code NNNN] [--timeout-sec N] [--name NAME] [--id ID]")
	if len(args) == 0 {
		return usage
	}
	target := args[0]
	if target != "dm" && target != "group" {
		return usage
	}

	fs := flag.NewFlagSet("setup pair", flag.ContinueOnError)
	code := fs.String("code", "", "4-digit pairing code to watch for (default: generated)")
	timeoutSec := fs.Int("timeout-sec", 300, "how long to wait for the code before giving up")
	name := fs.String("name", "", "config name for the paired group (group only; default: the group's title, else \"main\")")
	directID := fs.Int64("id", 0, "skip pairing and record this id directly (last resort)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	mfPath, err := mappings.DefaultPath()
	if err != nil {
		return err
	}
	mf, _, err := loadOrInitMappings(mfPath)
	if err != nil {
		return fmt.Errorf("existing %s is unreadable: %w — fix or delete it, then re-run", mfPath, err)
	}
	token := mf.Channels[telegramChannelName].BotToken
	if token == "" && *directID == 0 {
		return errors.New("no bot token configured yet — run `c3-broker setup token` first")
	}

	// Last-resort direct entry: record the id without pairing.
	if *directID != 0 {
		switch target {
		case "dm":
			if *directID <= 0 {
				return fmt.Errorf("--id %d: a Telegram user id is a positive integer", *directID)
			}
			applyDMPair(mf, *directID)
			if err := writeMappingsFile(mfPath, mf); err != nil {
				return err
			}
			fmt.Printf("✓ recorded user id %d (manual entry) + allowlisted for DM\n", *directID)
		case "group":
			if *directID >= 0 {
				return fmt.Errorf("--id %d: a Telegram group chat id is a negative integer", *directID)
			}
			gname := *name
			if gname == "" {
				gname = "main"
			}
			applyGroupPair(mf, gname, *directID, "")
			if err := writeMappingsFile(mfPath, mf); err != nil {
				return err
			}
			fmt.Printf("✓ recorded group %q chat_id %d (manual entry) + allowlisted\n", gname, *directID)
		}
		return nil
	}

	pairCode := *code
	if pairCode == "" {
		pairCode, err = generatePairCode()
		if err != nil {
			return err
		}
	} else if !isPairCode(pairCode) {
		return fmt.Errorf("--code %q: must be exactly 4 digits", pairCode)
	}

	// Pairing needs exclusive getUpdates access; pause a running broker
	// for the duration and put it back after (success or failure). The
	// pause only HOLDS if setup also takes the broker singleton flock: a
	// live adapter auto-respawns a stopped broker within seconds, and the
	// respawn would steal getUpdates and gate-drop the pairing code. With
	// the flock held, a respawned broker exits silently by design
	// (runDaemon treats a lost flock race as a no-op).
	stopped, note := stopBrokerFn()
	if note != "" {
		fmt.Println(note)
	}
	var pairingLock *broker.SingletonLock
	defer func() {
		// Release the flock FIRST so the broker we restore can take it.
		releasePairingLock(&pairingLock)
		if stopped {
			ensureBrokerUpFn()
		}
	}()
	if lock, lockErr := acquireBrokerPairingLock(); lockErr != nil {
		fmt.Fprintf(os.Stderr, "warning: could not hold the broker singleton lock during pairing (%v) — if an adapter respawns the broker mid-window, pairing may miss your code\n", lockErr)
	} else {
		pairingLock = lock
	}

	deadline := time.Now().Add(time.Duration(*timeoutSec) * time.Second)
	fmt.Printf("PAIRING CODE: %s\n", pairCode)

	switch target {
	case "dm":
		fmt.Printf("Send exactly `%s` to your bot in a DM (press START first). Waiting up to %ds...\n", pairCode, *timeoutSec)
		poller := newPairPoller(token, pairCode, pairTargetDM)
		capture, err := poller.wait(deadline)
		if err != nil {
			return fmt.Errorf("%w — re-run for a fresh code, or record the id directly with `c3-broker setup pair dm --id <user_id>`", err)
		}
		applyDMPair(mf, capture.UserID)
		if err := writeMappingsFile(mfPath, mf); err != nil {
			return err
		}
		fmt.Printf("✓ paired — Telegram user id %d recorded + allowlisted for DM\n", capture.UserID)
		fmt.Println("Next: `c3-broker setup pair group` to discover + record the group's chat id.")
	case "group":
		poller := newPairPoller(token, pairCode, pairTargetGroup)
		poller.senderAllowed = groupPairSenderGate(mf)
		if poller.senderAllowed != nil {
			fmt.Printf("Send exactly `%s` in the group (bot added, Topics on) FROM YOUR PAIRED ACCOUNT — codes from other members are ignored. Waiting up to %ds...\n", pairCode, *timeoutSec)
		} else {
			fmt.Printf("Send exactly `%s` in the group (bot added, Topics on — any member can send it). Waiting up to %ds...\n", pairCode, *timeoutSec)
		}
		capture, err := poller.wait(deadline)
		if err != nil {
			return fmt.Errorf("%w — usual causes: bot privacy mode still enabled (@BotFather → /setprivacy → Disable) or bot not in the group. Re-run for a fresh code, or record the id directly with `c3-broker setup pair group --id <chat_id>`", err)
		}
		gname := *name
		if gname == "" {
			gname = fallbackGroupName(capture.ChatTitle)
		}
		applyGroupPair(mf, gname, capture.ChatID, capture.ChatTitle)
		if err := writeMappingsFile(mfPath, mf); err != nil {
			return err
		}
		fmt.Printf("✓ paired — group %q has chat_id %d (recorded + allowlisted)\n", gname, capture.ChatID)
		fmt.Println("Next: `c3-broker setup finish` for host integrations + broker restart.")
	}
	return nil
}

// runSetupSTT is the `c3-broker setup stt` phase: the STT key prompts,
// standalone. Works interactively (TTY) or with piped answers.
func runSetupSTT() error {
	r := bufio.NewReader(os.Stdin)
	written, err := promptSTTSetup(r)
	if err != nil {
		return err
	}
	if !written {
		sttHint(DetectHostCLI())
	}
	return nil
}

// runSetupFinish is the `c3-broker setup finish` phase: verify the config,
// install host integrations, restart the broker so the new config is live,
// and print the stand-alone post-setup guidance (#8/#10).
func runSetupFinish() error {
	mfPath, err := mappings.DefaultPath()
	if err != nil {
		return err
	}
	mf, err := mappings.Read(mfPath)
	if err != nil {
		return fmt.Errorf("no usable config at %s (%v) — run the earlier setup phases first", mfPath, err)
	}
	if err := mf.Validate(); err != nil {
		return fmt.Errorf("config at %s does not validate: %w", mfPath, err)
	}
	progress := deriveProgress(mf, defaultSTTEnvPath())
	if progress.Token == "" {
		return errors.New("no bot token configured — run `c3-broker setup token` first")
	}
	if progress.DMChatID == 0 {
		fmt.Fprintln(os.Stderr, "warning: no DM pairing recorded — run `c3-broker setup pair dm`")
	}
	if progress.GroupChatID == 0 {
		fmt.Fprintln(os.Stderr, "warning: no group configured — run `c3-broker setup pair group`")
	}
	// Legacy configs can hold dm_chat_id/groups without allowlist entries
	// (pre-allowlist schema, or a dropped allowlist) — the default-deny gate
	// then drops all inbound even though every step reads as done. Repair
	// idempotently; finish runs on both setup paths, so this catches the
	// agent-guided flow too.
	if repairAllowlist(mf) {
		if err := writeMappingsFile(mfPath, mf); err != nil {
			return err
		}
		fmt.Println("✓ repaired missing allowlist entries for the configured DM/group (legacy config)")
	}

	host := DetectHostCLI()
	installHostIntegrations(host)
	if !progress.STTConfigured {
		sttHint(host)
	}

	fmt.Println()
	restartBrokerForNewConfig()

	fmt.Println()
	fmt.Println(postSetupWhatNow(host))
	return nil
}

// installHostIntegrations runs the host-CLI-specific install steps shared
// by the interactive flow and `setup finish`.
func installHostIntegrations(host HostCLI) {
	// #9 (2026-05-18): when setup is driven from Codex CLI, register the
	// c3 MCP server into Codex's persistent config so the user doesn't
	// have to do it by hand. (The `codex` launcher shim already wires up
	// per-invocation config via -c flags, but that only kicks in when
	// the user launches Codex through the shim — a vanilla `codex` would
	// otherwise miss the MCP server entirely.) Idempotent: existing
	// [mcp_servers.c3_codex] section is left untouched.
	if host == HostCodex {
		path, didWrite, err := ensureCodexMCPRegistration()
		switch {
		case err != nil:
			fmt.Fprintf(os.Stderr, "warning: could not register C3 MCP server in %s: %v\n", path, err)
		case didWrite:
			fmt.Printf("✓ registered C3 MCP server in %s\n", path)
		default:
			fmt.Printf("✓ C3 MCP server already registered in %s\n", path)
		}

		// #11 (2026-05-18): Codex's MCP host does NOT read the
		// `instructions` field from the initialize response (parallel
		// agent investigation, openai/codex#6148 closed-not-planned).
		// Install the OUTPUT MODE PROTOCOL + MULTI-PART REPLY PROTOCOL
		// into ~/.codex/AGENTS.md instead — Codex concatenates that
		// file into developer_instructions. Idempotent: replaces the
		// delimited block on rerun, leaves the rest of the file alone.
		amdPath, amdWrote, amdErr := ensureCodexAgentsMd()
		switch {
		case amdErr != nil:
			fmt.Fprintf(os.Stderr, "warning: could not install C3 protocol block in %s: %v\n", amdPath, amdErr)
		case amdWrote:
			fmt.Printf("✓ installed C3 protocol block in %s\n", amdPath)
		default:
			fmt.Printf("✓ C3 protocol block already up to date in %s\n", amdPath)
		}
	}

	// #17 (2026-05-18): when setup is driven from Claude Code, install the
	// claude-shim symlink at ~/.local/bin/claude unconditionally. This is
	// COMPULSORY, no prompt, no opt-out — the shim is the only supported
	// path for getting the dev-channels flag right, and a failed/skipped
	// install is the misconfiguration item #18 was meant to catch (now
	// closed as subsumed). Non-fatal if it fails (e.g. a real `claude`
	// binary already lives at the target path without a sentinel) —
	// surface a hint so the user can run install manually with --force.
	if err := maybeInstallClaudeShim(host); err != nil {
		// Compulsory under HostClaude — a silent skip is the exact
		// failure mode item #18 was meant to close. Print to BOTH
		// stdout and stderr so the agent transcript (stdout) and the
		// raw shell (stderr) both see the structured failure block.
		// Reasoning recorded in the 2026-05-19 code-review pass (item M2).
		printShimInstallFailure(os.Stdout, os.Stderr, err)
	}
}

// ---------------------------------------------------------------------------
// Config state: load, progress tracking, upserts
// ---------------------------------------------------------------------------

// skeletonMappings returns an empty-but-valid v1 mappings file.
func skeletonMappings() *mappings.MappingsFile {
	return &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]mappings.ChannelConfig{},
		Mappings:      map[string]mappings.Mapping{},
	}
}

// loadOrInitMappings reads path. A missing file yields a fresh skeleton
// (existed=false, nil error). A present-but-unreadable file returns
// (nil, true, err) — callers decide whether to abort (phased commands)
// or start over (interactive flow).
func loadOrInitMappings(path string) (*mappings.MappingsFile, bool, error) {
	mf, err := mappings.Read(path)
	if err != nil {
		if os.IsNotExist(err) {
			return skeletonMappings(), false, nil
		}
		return nil, true, err
	}
	if mf.Channels == nil {
		mf.Channels = map[string]mappings.ChannelConfig{}
	}
	if mf.Mappings == nil {
		mf.Mappings = map[string]mappings.Mapping{}
	}
	return mf, true, nil
}

// setupProgress records which setup steps are already satisfied by the
// config on disk. This is the substrate for item #4: setup must react to
// what is already configured and never re-show a completed step.
type setupProgress struct {
	Token         string
	DMChatID      int64
	GroupName     string
	GroupChatID   int64
	STTConfigured bool
}

// deriveProgress inspects a mappings file + the STT env path and reports
// what is already configured.
func deriveProgress(mf *mappings.MappingsFile, sttEnvPath string) setupProgress {
	var p setupProgress
	if mf != nil {
		cc := mf.Channels[telegramChannelName]
		p.Token = cc.BotToken
		p.DMChatID = cc.DMChatID
		// Prefer the default group; otherwise any configured group counts.
		if cc.DefaultGroup != "" {
			if g, ok := cc.Groups[cc.DefaultGroup]; ok && g.ChatID != 0 {
				p.GroupName, p.GroupChatID = cc.DefaultGroup, g.ChatID
			}
		}
		if p.GroupChatID == 0 {
			for gname, g := range cc.Groups {
				if g.ChatID != 0 {
					p.GroupName, p.GroupChatID = gname, g.ChatID
					break
				}
			}
		}
	}
	if st, err := os.Stat(sttEnvPath); err == nil && st.Size() > 0 {
		p.STTConfigured = true
	}
	return p
}

// summaryLine renders the already-configured overview for the re-run banner.
func (p setupProgress) summaryLine() string {
	part := func(label string, done bool) string {
		if done {
			return label + " ✓"
		}
		return label + " —"
	}
	group := "group"
	if p.GroupName != "" {
		group = fmt.Sprintf("group %q", p.GroupName)
	}
	return "  " + strings.Join([]string{
		part("bot token", p.Token != ""),
		part("DM pairing", p.DMChatID != 0),
		part(group, p.GroupChatID != 0),
		part("STT keys", p.STTConfigured),
	}, ", ")
}

// defaultSTTEnvPath is where promptSTTSetup writes the API keys.
func defaultSTTEnvPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = os.Getenv("HOME")
	}
	return filepath.Join(home, ".claude", "stt.env")
}

// applyBotToken upserts the bot token, preserving every other field.
func applyBotToken(mf *mappings.MappingsFile, token string) {
	cc := mf.Channels[telegramChannelName]
	cc.BotToken = token
	mf.Channels[telegramChannelName] = cc
}

// applyDMPair records the paired operator identity: DM chat id, master
// user id, and the default-deny allowlist entry — the same allowlist
// mutation the broker's own DM pairing acceptance performs
// (internal/broker/pairing.go acceptDMPair), plus the setup-owned
// identity fields.
func applyDMPair(mf *mappings.MappingsFile, userID int64) {
	cc := mf.Channels[telegramChannelName]
	cc.DMChatID = userID
	cc.MasterUserID = userID
	mf.Channels[telegramChannelName] = cc
	mf.AddAllowedUser(userID)
}

// applyGroupPair records a discovered group: the groups entry, the
// default_group (only when unset — a re-run adding a second group must
// not silently steal the default), and the allowlist entry (mirroring
// the broker's acceptGroupPair).
func applyGroupPair(mf *mappings.MappingsFile, name string, chatID int64, title string) {
	cc := mf.Channels[telegramChannelName]
	if cc.Groups == nil {
		cc.Groups = map[string]mappings.GroupConfig{}
	}
	cc.Groups[name] = mappings.GroupConfig{ChatID: chatID, Title: title}
	if cc.DefaultGroup == "" {
		cc.DefaultGroup = name
	}
	mf.Channels[telegramChannelName] = cc
	mf.AddAllowedGroup(chatID)
}

// repairAllowlist idempotently re-applies the pair mutations for every
// identity already recorded in the config, and reports whether anything was
// missing. A legacy config (pre-allowlist schema, or the known
// dropped-allowlist incident) can hold dm_chat_id/groups WITHOUT allowlist
// entries — the default-deny inbound gate then drops everything while setup
// reports the steps as done. applyDMPair/applyGroupPair only append missing
// entries, so this never clobbers an intact config.
func repairAllowlist(mf *mappings.MappingsFile) bool {
	cc := mf.Channels[telegramChannelName]
	repaired := false
	if cc.DMChatID != 0 && !mf.IsUserAllowed(cc.DMChatID) {
		applyDMPair(mf, cc.DMChatID)
		repaired = true
	}
	for name, g := range cc.Groups {
		if g.ChatID != 0 && !mf.IsGroupAllowed(g.ChatID) {
			applyGroupPair(mf, name, g.ChatID, g.Title)
			repaired = true
		}
	}
	return repaired
}

// groupPairSenderGate returns the sender filter for GROUP pairing. Once an
// operator allowlist exists (both flows pair the DM first, so by group time
// the owner is allowlisted), only an allowlisted sender's code is accepted —
// a random group member must not be able to complete the pairing. The
// any-sender behavior survives only for the bootstrap case (empty Users).
func groupPairSenderGate(mf *mappings.MappingsFile) func(int64) bool {
	if len(mf.AllowlistOrEmpty().Users) == 0 {
		return nil
	}
	return mf.IsUserAllowed
}

// writeMappingsFile persists mf at path (0600, parent 0700).
func writeMappingsFile(path string, mf *mappings.MappingsFile) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("mkdir mappings parent: %w", err)
	}
	if err := mappings.Write(path, mf); err != nil {
		return fmt.Errorf("write mappings: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Bot token prompt
// ---------------------------------------------------------------------------

// promptBotToken collects + validates the bot token. When a still-valid
// token already exists, enter keeps it (#4 — a completed step is offered,
// not re-imposed). The fresh-ask copy carries the @BotFather walkthrough
// inline so a user with no bot is never stuck at the prompt.
func promptBotToken(r *bufio.Reader, existing string) (string, string, error) {
	if existing != "" {
		username, err := validateBotToken(existing)
		if err == nil {
			fmt.Printf("✓ found existing bot token — bot is @%s\n", username)
			fmt.Print("Press enter to keep it, or paste a new token: ")
			replacement, rerr := readPassword(r)
			fmt.Println()
			if rerr != nil {
				return "", "", fmt.Errorf("read token: %w", rerr)
			}
			if replacement == "" {
				return existing, username, nil
			}
			return validateProvidedToken(replacement)
		}
		fmt.Fprintf(os.Stderr, "warning: existing bot token failed validation (%v) — paste a fresh one\n", err)
	}
	fmt.Println("Telegram bot token")
	fmt.Println("  No bot yet? In Telegram: message @BotFather → /newbot → follow the")
	fmt.Println("  prompts → copy the HTTP token (the 1234567:abc... string).")
	fmt.Print("Bot token: ")
	token, err := readPassword(r)
	fmt.Println() // newline after the silent prompt
	if err != nil {
		return "", "", fmt.Errorf("read token: %w", err)
	}
	if token == "" {
		return "", "", errors.New("bot token is required")
	}
	return validateProvidedToken(token)
}

// validateProvidedToken validates token via getMe and echoes the result.
func validateProvidedToken(token string) (string, string, error) {
	username, err := validateBotToken(token)
	if err != nil {
		return "", "", fmt.Errorf("token validation failed: %w", err)
	}
	fmt.Printf("✓ token valid; bot is @%s\n", username)
	return token, username, nil
}

// ---------------------------------------------------------------------------
// Pairing (setup-side discovery)
// ---------------------------------------------------------------------------

// pairTarget is which surface a setup pairing watches.
type pairTarget int

const (
	// pairTargetDM — watch private chats; capture the sender's user id.
	pairTargetDM pairTarget = iota
	// pairTargetGroup — watch group chats; capture the group's chat id
	// (the sender is incidental — we trust the group, not the member,
	// same trust model as internal/broker/pairing.go).
	pairTargetGroup
)

// pairCapture is the identity discovered by a successful pairing.
type pairCapture struct {
	UserID    int64
	ChatID    int64
	ChatTitle string
}

// pairPoller watches the bot's getUpdates stream for a message whose
// entire (trimmed) text is the 4-digit code, and captures the identity.
//
// This is the setup-side twin of the broker's pairing gate
// (internal/broker/pairing.go): the broker version arms a window on a
// RUNNING broker via /c3:pair, while this one runs when setup owns the
// getUpdates stream — which is also the only way to DISCOVER a group's
// chat id (the broker's group pairing requires the chat id up front).
type pairPoller struct {
	base        string // Bot-API base URL; telegramAPIBase() in production
	token       string
	code        string
	target      pairTarget
	pollTimeout int           // getUpdates long-poll seconds; 0 in tests
	retryDelay  time.Duration // sleep after a transient fetch error
	client      *http.Client
	progress    func(remaining time.Duration) // optional between-poll hook
	// senderAllowed, when non-nil, restricts a GROUP pairing match to codes
	// sent by an approved sender (the already-allowlisted operator). nil =
	// any sender (the bootstrap case). See groupPairSenderGate.
	senderAllowed func(userID int64) bool
	// warn is where non-fatal poller warnings go; nil = os.Stderr. A test
	// seam so the full-page-skip warning is assertable.
	warn io.Writer
}

// warnWriter returns the poller's warning sink (os.Stderr by default).
func (p *pairPoller) warnWriter() io.Writer {
	if p.warn != nil {
		return p.warn
	}
	return os.Stderr
}

// newPairPoller returns a production-configured poller.
func newPairPoller(token, code string, target pairTarget) *pairPoller {
	return &pairPoller{
		base:        telegramAPIBase(),
		token:       token,
		code:        code,
		target:      target,
		pollTimeout: 20,
		retryDelay:  2 * time.Second,
		// Client timeout must exceed the long-poll window or every empty
		// poll would surface as a transient error.
		client: &http.Client{Timeout: 35 * time.Second},
	}
}

// pairUpdate is one getUpdates result element (the subset setup needs).
type pairUpdate struct {
	UpdateID int64 `json:"update_id"`
	Message  *struct {
		Text string `json:"text"`
		From *struct {
			ID int64 `json:"id"`
		} `json:"from"`
		Chat struct {
			ID    int64  `json:"id"`
			Title string `json:"title"`
		} `json:"chat"`
	} `json:"message"`
}

// fetchPairUpdates calls getUpdates once. fatal=true means retrying cannot
// help (revoked/invalid token); a non-fatal error is transient — a network
// blip, a 5xx, or a 409 while a just-stopped broker's long poll drains on
// Telegram's side.
func (p *pairPoller) fetchPairUpdates(offset int64) (updates []pairUpdate, fatal bool, err error) {
	url := fmt.Sprintf("%s/bot%s/getUpdates?timeout=%d&offset=%d", p.base, p.token, p.pollTimeout, offset)
	resp, err := p.client.Get(url)
	if err != nil {
		// Redact before wrapping: Go's *url.Error embeds the full
		// token-bearing request URL, and this error reaches stderr.
		return nil, false, fmt.Errorf("network: %s", redactToken(err.Error(), p.token))
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("read body: %w", err)
	}
	var parsed struct {
		OK          bool         `json:"ok"`
		ErrorCode   int          `json:"error_code"`
		Description string       `json:"description"`
		Result      []pairUpdate `json:"result"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, false, fmt.Errorf("parse: %w", err)
	}
	if !parsed.OK {
		switch parsed.ErrorCode {
		case http.StatusUnauthorized, http.StatusNotFound:
			return nil, true, fmt.Errorf("telegram rejected the bot token: %s", parsed.Description)
		case http.StatusConflict:
			return nil, false, fmt.Errorf("another getUpdates consumer is active (%s) — if a C3 broker or another bot instance is still running, stop it (`pkill -x c3-broker`)", parsed.Description)
		default:
			return nil, false, fmt.Errorf("telegram: %s", parsed.Description)
		}
	}
	return parsed.Result, false, nil
}

// pairPageFull is Telegram's getUpdates page cap (limit defaults to 100). A
// page this full may be hiding the code behind it, so it is the ONLY case in
// which wait advances the getUpdates offset — see the loss-freedom note on
// wait.
const pairPageFull = 100

// pairMaxWrongCodes is the fail-closed cap on wrong 4-digit candidate codes
// observed in the watched chat type during one pairing window. Past it the
// window aborts — someone is guessing codes.
const pairMaxWrongCodes = 10

// wait polls until the code arrives, the deadline passes, or a fatal
// error occurs. Telegram keeps a 24h update backlog, so a code the user
// sent BEFORE the first poll is still captured — the interactive flow's
// "press enter to start waiting" gate loses nothing.
//
// Loss-freedom: wait polls WITHOUT advancing the getUpdates offset, so no
// unrelated update (or the broker's unconfirmed backlog) is ever confirmed —
// i.e. irreversibly destroyed — by setup, on success OR failure. Re-polls
// return the same backlog again; `seen` dedupes it in memory. Nothing is
// confirmed even on a successful match: the restarted broker re-fetches the
// whole backlog, and the pairing code resurfacing there as a normal message
// is acceptable noise (message loss is not). The single exception: a FULL
// page (pairPageFull updates) with no code would hide every later update
// forever, so as a last resort wait advances past that page — confirming it
// server-side — with a loud warning.
func (p *pairPoller) wait(deadline time.Time) (pairCapture, error) {
	var offset int64
	seen := make(map[int64]bool)
	wrongCodes := 0
	var lastTransient string
	for time.Now().Before(deadline) {
		updates, fatal, err := p.fetchPairUpdates(offset)
		if err != nil {
			if fatal {
				return pairCapture{}, err
			}
			// Transient — retry until the deadline; surface each distinct
			// cause once so a persistent 409 isn't silent.
			if err.Error() != lastTransient {
				fmt.Fprintf(p.warnWriter(), "  (retrying: %v)\n", err)
				lastTransient = err.Error()
			}
			time.Sleep(p.retryDelay)
			continue
		}
		lastTransient = ""
		pageMax := int64(-1)
		for _, upd := range updates {
			if upd.UpdateID > pageMax {
				pageMax = upd.UpdateID
			}
			if seen[upd.UpdateID] {
				continue
			}
			seen[upd.UpdateID] = true
			m := upd.Message
			if m == nil {
				continue
			}
			// Scope to the watched chat type first: positive chat id =
			// private chat (chat id == user id) — the same signal
			// internal/broker/pairing.go isPrivateChat uses.
			inScope := (p.target == pairTargetDM && m.Chat.ID > 0) ||
				(p.target == pairTargetGroup && m.Chat.ID < 0)
			if !inScope {
				continue
			}
			text := strings.TrimSpace(m.Text)
			if text != p.code {
				// Fail closed on repeated wrong 4-digit guesses in the
				// watched chat type (non-code chatter doesn't count).
				if isPairCode(text) {
					wrongCodes++
					if wrongCodes >= pairMaxWrongCodes {
						return pairCapture{}, fmt.Errorf("too many wrong codes (%d) during the pairing window — aborting", wrongCodes)
					}
				}
				continue
			}
			switch p.target {
			case pairTargetDM:
				if m.From != nil && m.From.ID > 0 {
					return pairCapture{UserID: m.From.ID, ChatID: m.Chat.ID}, nil
				}
			case pairTargetGroup:
				var fromID int64
				if m.From != nil {
					fromID = m.From.ID
				}
				if p.senderAllowed != nil && !p.senderAllowed(fromID) {
					fmt.Fprintf(p.warnWriter(), "  (ignored the group code from non-allowlisted sender %d — send it from the paired account)\n", fromID)
					continue
				}
				return pairCapture{UserID: fromID, ChatID: m.Chat.ID, ChatTitle: m.Chat.Title}, nil
			}
		}
		if len(updates) >= pairPageFull && pageMax >= offset {
			// Last resort: a full page with no code — advance past it so
			// later updates (which may hold the code) become visible.
			offset = pageMax + 1
			fmt.Fprintf(p.warnWriter(), "warning: pairing scanned a full page of %d unrelated Telegram updates without finding the code — skipping past them to keep looking; those updates are confirmed to Telegram and will NOT be re-delivered to the broker\n", len(updates))
		}
		if p.progress != nil {
			p.progress(time.Until(deadline))
		}
	}
	return pairCapture{}, fmt.Errorf("pairing window expired without receiving code %s", p.code)
}

// generatePairCode returns a crypto-random 4-digit zero-padded code — the
// same shape internal/broker/pairing.go generates for /c3:pair, so pairing
// looks identical to the user in both flows.
func generatePairCode() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(10000))
	if err != nil {
		return "", fmt.Errorf("pair code: %w", err)
	}
	return fmt.Sprintf("%04d", n.Int64()), nil
}

// isPairCode reports whether s is exactly 4 ASCII digits.
func isPairCode(s string) bool {
	if len(s) != 4 {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// pairWaitBudget is how long one interactive pairing attempt waits.
const pairWaitBudget = 5 * time.Minute

// pairArmPrompt doubles as the last-resort manual entry: pressing enter
// starts the pairing wait; pasting a numeric id records it directly.
// This one line is ALL that remains of the manual-id collection path.
const pairArmPrompt = "Press enter to start waiting (or paste the numeric id directly if you already know it): "

// dmPairIntro is the DM pairing banner shown before the wait.
func dmPairIntro(botUsername, code string) string {
	return fmt.Sprintf(`Pair your Telegram account — a code replaces any id hunting
  1. In Telegram, open a DM with @%s and press START.
  2. Send exactly this code as a message: %s`, botUsername, code)
}

// groupPairIntro is the group pairing banner shown before the wait.
// restricted=true once an operator allowlist exists: only the paired
// account's code is accepted then (see groupPairSenderGate).
func groupPairIntro(code string, restricted bool) string {
	if restricted {
		return fmt.Sprintf(`Pair the group — C3 discovers the group's chat id from a code
  In the group (bot added, Topics on), send exactly this code: %s
  Send it from your own (paired) account — codes from other members are ignored.`, code)
	}
	return fmt.Sprintf(`Pair the group — C3 discovers the group's chat id from a code
  In the group (bot added, Topics on), send exactly this code: %s
  Any member can send it — C3 records the group, not the sender.`, code)
}

// waitProgressPrinter returns a between-poll hook that prints a throttled
// "still waiting" line so a long pairing wait doesn't look hung.
func waitProgressPrinter() func(time.Duration) {
	last := time.Now()
	return func(remaining time.Duration) {
		if time.Since(last) < 45*time.Second {
			return
		}
		last = time.Now()
		fmt.Printf("  …still waiting (%s left)\n", remaining.Round(time.Second))
	}
}

// pairDMInteractive runs the interactive DM pairing loop: show the code,
// offer the arm-or-direct-entry gate, wait, retry with a fresh code on
// expiry (3 attempts).
func pairDMInteractive(r *bufio.Reader, token, botUsername string) (int64, error) {
	for attempt := 1; attempt <= 3; attempt++ {
		code, err := generatePairCode()
		if err != nil {
			return 0, err
		}
		fmt.Println()
		fmt.Println(dmPairIntro(botUsername, code))
		fmt.Print(pairArmPrompt)
		if line := readLine(r); line != "" {
			id, perr := strconv.ParseInt(line, 10, 64)
			if perr == nil && id > 0 {
				return id, nil
			}
			fmt.Println("  (not a positive numeric user id — starting the pairing wait instead)")
		}
		fmt.Printf("Waiting up to %s for code %s...\n", pairWaitBudget, code)
		poller := newPairPoller(token, code, pairTargetDM)
		poller.progress = waitProgressPrinter()
		capture, err := poller.wait(time.Now().Add(pairWaitBudget))
		if err == nil {
			return capture.UserID, nil
		}
		fmt.Fprintf(os.Stderr, "pairing attempt %d failed: %v\n", attempt, err)
		fmt.Println("  Check: did you press START on the bot? Sent the exact 4 digits, nothing else?")
	}
	return 0, errors.New("DM pairing did not complete — re-run `c3-broker setup`, or run `c3-broker setup pair dm --id <user_id>`")
}

// pairGroupInteractive is pairDMInteractive's group twin. The direct-entry
// gate accepts a negative chat id. senderAllowed (nil = any sender) scopes
// which sender's code completes the pairing — see groupPairSenderGate.
func pairGroupInteractive(r *bufio.Reader, token string, senderAllowed func(int64) bool) (pairCapture, error) {
	for attempt := 1; attempt <= 3; attempt++ {
		code, err := generatePairCode()
		if err != nil {
			return pairCapture{}, err
		}
		fmt.Println()
		fmt.Println(groupPairIntro(code, senderAllowed != nil))
		fmt.Print(pairArmPrompt)
		if line := readLine(r); line != "" {
			id, perr := strconv.ParseInt(line, 10, 64)
			if perr == nil && id < 0 {
				return pairCapture{ChatID: id}, nil
			}
			fmt.Println("  (group chat ids are negative integers — starting the pairing wait instead)")
		}
		fmt.Printf("Waiting up to %s for code %s...\n", pairWaitBudget, code)
		poller := newPairPoller(token, code, pairTargetGroup)
		poller.senderAllowed = senderAllowed
		poller.progress = waitProgressPrinter()
		capture, err := poller.wait(time.Now().Add(pairWaitBudget))
		if err == nil {
			return capture, nil
		}
		fmt.Fprintf(os.Stderr, "pairing attempt %d failed: %v\n", attempt, err)
		fmt.Println("  Usual causes: bot privacy mode still enabled (@BotFather → /setprivacy →")
		fmt.Println("  Disable) or the bot isn't actually a member of the group.")
	}
	return pairCapture{}, errors.New("group pairing did not complete — re-run `c3-broker setup`, or run `c3-broker setup pair group --id <chat_id>`")
}

// promptGroupName asks what to call the paired group in config. Default
// "main" (the convention across docs); the group's Telegram title is shown
// for orientation when known.
func promptGroupName(r *bufio.Reader, title string) string {
	if title != "" {
		fmt.Printf("Paired Telegram group: %q\n", title)
	}
	fmt.Print("Config name for this group (used as the default for new topics) [main]: ")
	name := readLine(r)
	if name == "" {
		return "main"
	}
	return name
}

// fallbackGroupName picks the phased pair-group default name: the group's
// Telegram title when present, else "main".
func fallbackGroupName(title string) string {
	if t := strings.TrimSpace(title); t != "" {
		return t
	}
	return "main"
}

// ---------------------------------------------------------------------------
// Broker lifecycle around setup
// ---------------------------------------------------------------------------

// stopBrokerFn / ensureBrokerUpFn are the package-level indirections through
// which setup touches the broker lifecycle. Production points at the real
// helpers below; tests swap fakes so no real broker is stopped or spawned
// (same pattern as installRunFn / installClaudeShimFn).
var (
	stopBrokerFn     = stopBrokerIfRunning
	ensureBrokerUpFn = ensureBrokerUp
)

// acquireBrokerPairingLock takes the broker singleton flock (the same lock
// runDaemon holds) so that, for the whole pairing window, any broker an
// adapter auto-respawns loses the flock race and exits silently at startup —
// instead of stealing the bot's getUpdates stream and gate-dropping the
// pairing code. Retries briefly: the just-stopped broker may still be
// releasing the lock, or an adapter may have respawned one in the gap (stop
// it again and retry). While held, the pid file carries setup's own pid.
func acquireBrokerPairingLock() (*broker.SingletonLock, error) {
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		lock, err := broker.AcquireSingleton(broker.PidFilePath())
		if err == nil {
			return lock, nil
		}
		lastErr = err
		// Someone else holds it — a respawned broker, most likely. Stop it
		// and try again.
		stopBrokerFn()
		time.Sleep(200 * time.Millisecond)
	}
	return nil, lastErr
}

// releasePairingLock releases *lock if held and nils it, so the deferred
// safety-net release and the explicit happy-path release can't double-free.
func releasePairingLock(lock **broker.SingletonLock) {
	if *lock != nil {
		(*lock).Release()
		*lock = nil
	}
}

// brokerReachable reports whether a broker answers on the unix socket.
func brokerReachable() bool {
	c, err := net.DialTimeout("unix", broker.SocketPath(), 500*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// stopBrokerIfRunning stops a running broker so setup can own the bot's
// getUpdates stream during pairing (two consumers steal each other's
// updates) and so a finished setup can respawn it on the new config.
// Safe: route claims live in mappings.json + the broker's PID-liveness
// logic, and adapters auto-respawn a broker, so a bounce loses nothing.
//
// Returns whether a broker was stopped plus a human-readable note ("" when
// there was simply nothing to stop).
func stopBrokerIfRunning() (bool, string) {
	if !brokerReachable() {
		return false, ""
	}
	data, err := os.ReadFile(broker.PidFilePath())
	if err != nil {
		return false, "note: a broker looks alive but its pid file is unreadable — if pairing never sees your code, run `pkill -x c3-broker` and retry"
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 1 {
		return false, "note: the broker pid file is malformed — if pairing never sees your code, run `pkill -x c3-broker` and retry"
	}
	if err := osutil.TerminateProcess(pid); err != nil {
		return false, fmt.Sprintf("note: could not stop the running broker (pid %d): %v — if pairing never sees your code, stop it manually", pid, err)
	}
	// Wait for the process to actually exit — the singleton lock and the
	// getUpdates long poll are only released then.
	for i := 0; i < 50; i++ {
		if !osutil.ProcessSignalable(pid) { // gone
			return true, "(stopped the running C3 broker so pairing codes come straight to setup; it restarts when setup finishes)"
		}
		time.Sleep(100 * time.Millisecond)
	}
	return true, fmt.Sprintf("note: broker (pid %d) is slow to exit — pairing may miss messages until it stops", pid)
}

// ensureBrokerUp spawns a detached broker when none is reachable, then
// waits briefly for the socket. Safe to call when one is already running
// (and safe to race an adapter's own spawn — the singleton flock makes
// every spare exit silently).
func ensureBrokerUp() {
	if brokerReachable() {
		return
	}
	exe, err := os.Executable()
	if err != nil {
		exe = "c3-broker"
	}
	if err := spawn.Detached(exec.Command(exe)); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not start the broker: %v — it will auto-start on the next /c3:attach\n", err)
		return
	}
	for i := 0; i < 20; i++ {
		if brokerReachable() {
			fmt.Println("✓ broker running with the new config")
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "note: broker spawn requested but the socket isn't up yet — check `c3-broker status` in a moment")
}

// restartBrokerForNewConfig bounces the broker (stop if running, then
// ensure one is up) so the config setup just wrote is what's actually
// live. A SIGHUP reload is NOT sufficient after a token change — the
// telegram channel is initialized at broker start.
func restartBrokerForNewConfig() {
	if stopped, note := stopBrokerFn(); stopped {
		fmt.Println("Restarting the C3 broker with the new config...")
	} else if note != "" {
		fmt.Fprintln(os.Stderr, note)
	}
	ensureBrokerUpFn()
}

// ---------------------------------------------------------------------------
// Post-setup guidance
// ---------------------------------------------------------------------------

// postSetupWhatNow renders the "Setup complete — what now" block. Both
// paths (agent-guided and bare CLI) end here, so the text must stand alone
// (#8). The launch command deliberately carries NO --resume (#9 — the user
// appends it only when they want to resume), and the block walks attach →
// first message → a 30-second tour (#10).
func postSetupWhatNow(host HostCLI) string {
	if host == HostCodex {
		return `Setup complete — what now
  1. Restart Codex so the C3 MCP server loads:
       exit (Ctrl-D), then run: codex
     (use ` + "`codex resume --last`" + ` only if you want your previous conversation back)
  2. In the session, use the c3 attach tool to bind this project to a Telegram topic.
  3. From your phone, send a text or voice note to that topic — it surfaces in the CLI.

  30-second tour: ` + "`c3-broker status`" + ` shows broker health; the c3 topics tool
  lists topics + claims; voice notes are transcribed automatically once STT
  keys are configured (` + "`c3-broker setup stt`" + `).`
	}
	return `Setup complete — what now
  1. Launch Claude Code with the C3 channel enabled:
       claude --dangerously-load-development-channels plugin:c3@c3
     (append --resume yourself only if you want to pick up a previous session)
  2. In the session, run /c3:attach to bind this project to a Telegram topic.
  3. From your phone, send a text or voice note to that topic — it surfaces in the CLI.

  30-second tour: /c3:status shows broker health; /c3:topics lists topics +
  claims; /c3:pair allowlists another person or group later; voice notes are
  transcribed automatically once STT keys are configured (` + "`c3-broker setup stt`" + `).`
}

// installResult carries the outcome of the background `go install`.
type installResult struct {
	err      error
	output   []byte // combined stdout+stderr from `go install` (for failure reporting)
	duration time.Duration
	skipped  bool // true if source dir wasn't discoverable; not an error
}

// installRunFn is the package-level indirection through which
// startBackgroundInstall actually runs the build. Tests swap in a
// fake to assert ordering / propagate fake errors without touching
// the user's GOBIN. Same pattern as installClaudeShimFn.
var installRunFn = defaultInstallRun

// defaultInstallPackages are the C3 binaries installed by setup and /c3:build.
// cmd/codex is intentionally absent: it is an opt-in launcher named exactly
// like the user's real CLI, and INSTALL.md §5 is the only path that may install
// it onto PATH.
var defaultInstallPackages = []string{
	"./cmd/c3-broker",
	"./cmd/c3-claude-adapter",
	"./cmd/c3-codex-adapter",
	"./cmd/c3-grok-adapter",
	"./cmd/c3-agy-adapter",
	"./cmd/c3-desktop-adapter",
	"./cmd/claude-shim",
	"./cmd/migrate-legacy",
}

// defaultInstallRun is the production implementation: locate the
// source dir, install the core binary package set from it. Skipped (not an
// error) if the source dir can't be discovered — the user can
// rebuild manually after setup completes.
func defaultInstallRun(ctx context.Context) installResult {
	srcDir, ok := discoverSourceDir()
	if !ok {
		return installResult{skipped: true}
	}
	start := time.Now()
	args := append([]string{"install"}, defaultInstallPackages...)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = srcDir
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	return installResult{
		err:      err,
		output:   out,
		duration: time.Since(start),
	}
}

// startBackgroundInstall launches the install in a goroutine and
// returns a buffered channel that emits exactly one installResult
// when done. Buffered so the goroutine doesn't block if the join is
// late.
func startBackgroundInstall(ctx context.Context) <-chan installResult {
	ch := make(chan installResult, 1)
	go func() {
		ch <- installRunFn(ctx)
	}()
	return ch
}

// joinBackgroundInstall blocks on the channel, prints a status line,
// and returns any underlying error (nil on skip or success).
func joinBackgroundInstall(ch <-chan installResult) error {
	// Show a heartbeat if we're going to wait more than a tick — the
	// usual case is "user finished prompts after install completed"
	// so the receive is instant.
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()
	var result installResult
	select {
	case result = <-ch:
		// instant — let defer Stop() reclaim the timer
	case <-timer.C:
		fmt.Println("Waiting for the core C3 binaries to finish building in the background...")
		result = <-ch
	}
	if result.skipped {
		fmt.Println("(skipped background build — source dir not found near c3-broker; run `/c3:build` manually if you need fresh core binaries)")
		return nil
	}
	if result.err != nil {
		tail := result.output
		const maxTail = 4096
		if len(tail) > maxTail {
			tail = tail[len(tail)-maxTail:]
		}
		fmt.Fprintf(os.Stderr, "go install failed after %s:\n%s\n", result.duration.Round(time.Millisecond), tail)
		return result.err
	}
	fmt.Printf("✓ core binaries built (`go install`, %s)\n", result.duration.Round(time.Millisecond))
	return nil
}

// discoverSourceDir locates the C3 source tree (one with go.mod
// module = github.com/Andrometiq/c3) so the core `go install` has
// a working directory.
//
// Resolution order:
//  1. $C3_SRC_DIR override (testing / unusual installs).
//  2. Walk up from os.Executable() looking for a matching go.mod.
//  3. ~/src/c3 (canonical clone path from docs/INSTALL.md).
//
// Returns ("", false) if nothing resolves — caller should skip the
// background build and tell the user to rebuild manually.
func discoverSourceDir() (string, bool) {
	if v := strings.TrimSpace(os.Getenv("C3_SRC_DIR")); v != "" {
		if isC3SourceDir(v) {
			return v, true
		}
		return "", false
	}
	if exe, err := os.Executable(); err == nil {
		if dir := walkUpForC3GoMod(filepath.Dir(exe)); dir != "" {
			return dir, true
		}
	}
	if home, err := os.UserHomeDir(); err == nil {
		guess := filepath.Join(home, "src", "c3")
		if isC3SourceDir(guess) {
			return guess, true
		}
	}
	return "", false
}

// walkUpForC3GoMod ascends parent directories looking for a go.mod
// whose module declaration matches github.com/Andrometiq/c3.
// Returns "" if none found within a small fixed budget (10 levels).
func walkUpForC3GoMod(start string) string {
	dir := start
	for i := 0; i < 10; i++ {
		if isC3SourceDir(dir) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
	return ""
}

// isC3SourceDir returns true iff dir contains a go.mod whose module
// declaration is the C3 module path.
func isC3SourceDir(dir string) bool {
	data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			mod := strings.TrimSpace(strings.TrimPrefix(line, "module"))
			return mod == "github.com/Andrometiq/c3"
		}
	}
	return false
}

// checklistStep is one entry of the group-preparation walk.
type checklistStep struct {
	title string
	body  []string
}

// botGroupChecklistSteps is the group-preparation walk. By the time this
// runs the bot token is already validated, so there is deliberately no
// "create the bot" step (#4, 2026-06-30 install feedback: never re-show a
// completed step) — and no manual id collection tail: the ids are
// discovered by the pairing codes that follow (#5/#6).
func botGroupChecklistSteps() []checklistStep {
	return []checklistStep{
		{
			"Disable privacy mode",
			[]string{
				"  In @BotFather: /setprivacy → pick your bot → Disable.",
				"  Without this, the bot only sees messages that mention or reply",
				"  to it — group pairing and topic routing both need it off.",
				"  Cannot be done over the Bot API.",
			},
		},
		{
			"Create a Telegram group",
			[]string{
				"  A regular group is fine; it auto-promotes to a supergroup",
				"  when you turn Topics on in the next-but-one step.",
			},
		},
		{
			"Add your bot to the group",
			[]string{
				"  Group → members → Add → search the bot's @username.",
			},
		},
		{
			"Enable Topics in the group",
			[]string{
				"  Group settings → Topics → On. Do this BEFORE the admin step —",
				"  the \"Allow create topics\" admin right only appears once Topics",
				"  are enabled.",
			},
		},
		{
			"Promote the bot to admin with these rights",
			[]string{
				"  Manage Topics, Send Messages, Delete Messages, Pin Messages.",
				"  Everything else off.",
			},
		},
	}
}

// walkBotGroupChecklist guides the user through the group-preparation
// steps, one at a time, with a "press enter when done" pause between
// steps. Mirrors the checklist in docs/INSTALL.md — but inline, so the
// user reads it in the terminal rather than having to flip to a doc.
//
// If stdin is piped (non-interactive), we still print each step but
// don't pause — the agent driving setup will move on and the human
// reading the captured output can pause where they need to.
func walkBotGroupChecklist(r *bufio.Reader) {
	fmt.Println()
	fmt.Println("Step-by-step: Telegram group setup")
	fmt.Println()
	fmt.Println("(Use Telegram Desktop, iOS, Android, or macOS — NOT Telegram Web for")
	fmt.Println(" the group steps. Web's Topics + admin-rights UIs are incomplete.)")
	fmt.Println()
	steps := botGroupChecklistSteps()
	// Honor C3_NO_PROMPT in addition to the TTY check so a
	// non-interactive setup driver (which already bypasses the consent
	// gate via isNoPromptSet) also skips the step pauses. Without this,
	// a non-interactive driver that's piping prompts through stdin
	// gets a TTY-detection negative (good) but a different one if it's
	// faking a TTY via `script(1)` (bad). Closes report NIT n4
	// (2026-05-19).
	interactive := term.IsTerminal(int(syscall.Stdin)) && !isNoPromptSet()
	for i, s := range steps {
		fmt.Printf("%d. %s\n", i+1, s.title)
		for _, line := range s.body {
			fmt.Println(line)
		}
		fmt.Println()
		if interactive {
			fmt.Printf("   (press enter when done with step %d)", i+1)
			readLine(r)
			fmt.Println()
		}
	}
	fmt.Println("Group ready. Next: a quick pairing code — C3 discovers the group's")
	fmt.Println("chat id from it automatically, no id hunting needed.")
}

// sttHint prints a per-host instruction telling the user how to get STT
// keys wired up when the interactive prompt didn't capture any. Reasons
// it might not have:
//   - user said no to "Set up STT?"
//   - both API-key prompts were empty
//   - non-interactive caller (agent driving stdin) — silently skipped
//     the whole branch
//
// Under Claude Code, the prompts usually surface fine, so this is brief.
// Under Codex, we walk the user through the manual file format because
// the interactive path is unreliable.
func sttHint(host HostCLI) {
	envPath := defaultSTTEnvPath()
	fmt.Println()
	if host == HostCodex {
		fmt.Printf("STT API keys were not wired up. Codex runs setup non-interactively so the\n")
		fmt.Printf("Y/N prompts above auto-skipped. To enable voice transcription, create %s\n", envPath)
		fmt.Println("(mode 0600) with at least one of:")
		fmt.Println()
		fmt.Println("  OPENROUTER_API_KEY=...   # Gemini 3 Flash (https://openrouter.ai/keys)")
		fmt.Println("  SARVAM_API_KEY=...       # Saaras v3 (https://dashboard.sarvam.ai)")
		fmt.Println("  ELEVENLABS_API_KEY=...   # optional, ElevenLabs Scribe v2")
		fmt.Println()
		fmt.Println("Or run `c3-broker setup stt` in a real terminal (e.g. plain shell, not via")
		fmt.Println("the Codex agent) and answer the STT prompts.")
		return
	}
	fmt.Printf("(STT keys not configured — voice messages will surface as `[STT FAILED: ...]`.\n")
	fmt.Printf(" Run `c3-broker setup stt` any time, or hand-write %s.)\n", envPath)
}

// sttNotes is the compressed post-write footer (#3: the old section was a
// wall of text). One line per concept; details live in stt-pkg/README.md.
const sttNotes = `Notes:
  • Own STT provider: drop a transcribe() module at
    plugins/c3/stt/stt-pkg/providers/<name>.py and add it to your --chain.
  • Mishears? Add terms to ~/.config/c3/stt-vocabulary.txt (one per line;
    format in plugins/c3/stt/stt-pkg/README.md) — agents will prompt you too.
  • Long audio (>30s) routing needs ffmpeg/ffprobe from your OS package manager.`

// promptSTTSetup asks the user if they want voice transcription, and if
// so collects API keys for the bundled provider chain and writes a 0600
// env file at ~/.claude/stt.env that the broker's STT subprocess inherits.
//
// The default chain is Gemini 3 Flash (via OpenRouter) → Sarvam Saaras v3.
// Gemini 3 Flash (google/gemini-3-flash-preview) is called out because it
// handles multilingual audio and mid-sentence language switches well.
//
// Returns (written, err). written=true iff the env file was created with
// at least one key. The caller uses this to decide whether to print a
// post-setup hint reminding the user how to wire keys up manually.
func promptSTTSetup(r *bufio.Reader) (bool, error) {
	fmt.Println()
	fmt.Println("Voice transcription (optional)")
	fmt.Println("Voice notes from Telegram are transcribed and handed to the CLI as text.")
	fmt.Println("Default provider chain: Gemini 3 Flash → Sarvam Saaras v3. Gemini 3 Flash")
	fmt.Println("(google/gemini-3-flash-preview via OpenRouter) handles multilingual audio")
	fmt.Println("and mid-sentence language switches well.")
	fmt.Println()
	fmt.Print("Set up STT? [Y/n]: ")
	yes := readBoolDefault(r, true)
	if !yes {
		fmt.Println("Skipping STT setup. Voice messages will surface as `[STT FAILED: handler_missing]` until you configure it.")
		return false, nil
	}

	envPath := defaultSTTEnvPath()
	if err := os.MkdirAll(filepath.Dir(envPath), 0o700); err != nil {
		return false, fmt.Errorf("mkdir %s: %w", filepath.Dir(envPath), err)
	}

	fmt.Println()
	fmt.Println("API keys — at least one; empty skips that provider:")
	fmt.Print("  OPENROUTER_API_KEY (https://openrouter.ai/keys): ")
	openrouter, err := readPassword(r)
	if err != nil {
		return false, fmt.Errorf("read OPENROUTER_API_KEY: %w", err)
	}
	fmt.Println()

	fmt.Print("  SARVAM_API_KEY (https://dashboard.sarvam.ai): ")
	sarvam, err := readPassword(r)
	if err != nil {
		return false, fmt.Errorf("read SARVAM_API_KEY: %w", err)
	}
	fmt.Println()

	if openrouter == "" && sarvam == "" {
		fmt.Println("No keys provided — skipping STT env file write.")
		return false, nil
	}

	var sb strings.Builder
	sb.WriteString("# c3 STT API keys — written by `c3-broker setup`.\n")
	sb.WriteString("# Add more providers (e.g. ELEVENLABS_API_KEY) by hand if needed.\n")
	if openrouter != "" {
		sb.WriteString("OPENROUTER_API_KEY=" + openrouter + "\n")
	}
	if sarvam != "" {
		sb.WriteString("SARVAM_API_KEY=" + sarvam + "\n")
	}

	if err := os.WriteFile(envPath, []byte(sb.String()), 0o600); err != nil {
		return false, fmt.Errorf("write %s: %w", envPath, err)
	}
	fmt.Printf("✓ wrote %s (mode 0600)\n", envPath)
	fmt.Println(sttNotes)
	return true, nil
}

// installClaudeShimFn is the package-level indirection through which
// maybeInstallClaudeShim invokes the shim installer. Production code
// points it at runInstallClaudeShim; tests swap in a fake to assert
// that setup invoked it (and with what args) without needing a real
// claude-shim launcher binary next to the test exe.
var installClaudeShimFn = runInstallClaudeShim

// maybeInstallClaudeShim runs the claude-shim installer with defaults
// (i.e. no flag args — default install path of ~/.local/bin/claude,
// no --force) when the host CLI is Claude Code. Other hosts (Codex,
// Unknown) are a no-op.
//
// This is the runSetup() Claude-host branch factored into a testable
// helper. See item #17 in TODO.md: the shim install is COMPULSORY
// under Claude Code per the maintainer's 2026-05-18 call — no prompt,
// no opt-out.
func maybeInstallClaudeShim(host HostCLI) error {
	if host != HostClaude {
		return nil
	}
	return installClaudeShimFn(nil)
}

// readBoolDefault is readBool but uses the explicit default instead of
// the function literal's hard-coded fallback.
func readBoolDefault(r *bufio.Reader, def bool) bool {
	line := readLine(r)
	switch strings.ToLower(line) {
	case "y", "yes":
		return true
	case "n", "no":
		return false
	default:
		return def
	}
}

// telegramAPIBase is the Bot-API base URL setup's own HTTP calls use
// (getMe validation + pairing getUpdates). Honors the C3_TELEGRAM_API_URL
// env override the telegram channel also applies (env beats default), so
// setup works behind a Bot-API reverse proxy too.
func telegramAPIBase() string {
	if v := strings.TrimSpace(os.Getenv("C3_TELEGRAM_API_URL")); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "https://api.telegram.org"
}

// validateBotToken calls Telegram's getMe and returns the bot's username on
// success. Errors on 401 (invalid token), network failure, or non-OK
// responses.
func validateBotToken(token string) (string, error) {
	return validateBotTokenAt(telegramAPIBase(), token)
}

// redactToken masks the bot token anywhere it appears in s. Go's *url.Error
// embeds the full request URL — for the Bot API that means the token-bearing
// /bot<token>/ path segment — and setup prints these errors to stderr, so
// every setup HTTP error must pass through here before it can be returned.
func redactToken(s, token string) string {
	if token == "" {
		return s
	}
	return strings.ReplaceAll(s, token, "<redacted>")
}

// validateBotTokenAt is validateBotToken against an explicit base URL
// (injectable for tests).
func validateBotTokenAt(base, token string) (string, error) {
	url := base + "/bot" + token + "/getMe"
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		// Redact before wrapping — the transport error embeds the URL.
		return "", fmt.Errorf("network: %s", redactToken(err.Error(), token))
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var parsed struct {
		OK     bool `json:"ok"`
		Result struct {
			Username string `json:"username"`
		} `json:"result"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	if !parsed.OK {
		return "", fmt.Errorf("telegram: %s", parsed.Description)
	}
	return parsed.Result.Username, nil
}

func readLine(r *bufio.Reader) string {
	line, _ := r.ReadString('\n')
	return strings.TrimSpace(line)
}

// readPassword reads a single line of input with terminal echo disabled
// when stdin is a TTY (so the bot token doesn't end up in scroll-back
// or screen-recording footage). Falls back to plain reads when stdin is
// piped/redirected (CI, automation), since there's no terminal to mute.
func readPassword(r *bufio.Reader) (string, error) {
	if term.IsTerminal(int(syscall.Stdin)) {
		b, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}
	return readLine(r), nil
}
