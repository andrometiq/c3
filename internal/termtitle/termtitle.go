// Package termtitle emits OSC-0 ("set window title") ANSI escapes to a
// writer (defaults to os.Stderr) so the terminal-emulator title-bar reflects
// the currently-attached C3 topic.
//
// Closes TODO #19(a). Surface decision (locked 2026-05-18,
// MORNING-REVIEW-2026-05-19.md): an ANSI escape from the adapter, NOT a
// Claude-Code statusline plugin. Reasons:
//
//  1. Works for both Claude Code and Codex with one code path — the maintainer's
//     standing "every flow must work the same in Codex" principle.
//  2. No settings.json edits — universal OSC-0 escape that xterm,
//     gnome-terminal, alacritty, kitty, tmux, screen, iTerm2, and
//     terminator all honor.
//  3. This is the standard idiom (vim, tmux, ssh agents all do it).
//
// Wire format: \x1b]0;TITLE\x07. The OSC-0 form sets both the window title
// AND the icon name; OSC-2 sets only the window title; OSC-1 only the icon
// name. OSC-0 is the safe default — broader compatibility, single emit.
//
// Output sink: stderr by default, not stdout. Stdout carries MCP JSON-RPC
// framing in the adapters; an escape there would corrupt the protocol.
// Stderr is where adapters already write log lines, so a control sequence
// there is consistent with prior art.
//
// Gating:
//   - C3_NO_TERMINAL_TITLE — truthy values (1/true/yes/on, case-insensitive)
//     suppress all emits. Default = enabled. For users in non-tty contexts
//     or with title-bar-noise tooling that doesn't want the escape.
//   - isatty(2) — only emit if stderr is actually a terminal. Pipe / log
//     capture / non-interactive contexts get nothing.
//
// Failure paths never emit. Only AttachedMsg.OK=true triggers a title
// update; NeedsConfirmation / Status=no_topics_configured /
// Status=policy_rejected / Err-set / nil-pointer all return without
// writing.
package termtitle

import (
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"

	"github.com/Andrometiq/c3/internal/ipc"
)

// Output sink + tty probe are package-level vars so adapter-level
// integration tests can swap them out without touching the real
// os.Stderr or the process's tty. Production code path uses os.Stderr
// + term.IsTerminal; tests redirect via SetWriter / SetIsTTY.
var (
	out       io.Writer = os.Stderr
	isTTYFunc           = isStderrTTY
)

// SetWriter swaps the package-level writer (default os.Stderr). Tests
// only. Returns the previous writer so tests can restore it.
func SetWriter(w io.Writer) io.Writer {
	prev := out
	out = w
	return prev
}

// SetIsTTY swaps the package-level isTTY probe (default
// term.IsTerminal(stderr.Fd())). Tests only. Returns the previous
// function so tests can restore it.
func SetIsTTY(fn func() bool) func() bool {
	prev := isTTYFunc
	isTTYFunc = fn
	return prev
}

// EmitAttach writes the title-bar escape derived from msg to the package
// writer (os.Stderr by default), honoring the global gates
// (C3_NO_TERMINAL_TITLE off, writer isatty). No-op on any failure path
// (nil msg or msg.OK==false). Called from the adapter's attach-success
// branch.
func EmitAttach(msg *ipc.AttachedMsg) {
	EmitTo(out, isTTYFunc(), Suppressed(), msg)
}

// Clear writes the empty-title escape to the package writer, honoring the
// same gates. Called from the adapter's detach-success branch so the
// user's terminal title returns to the emulator default.
func Clear() {
	ClearTo(out, isTTYFunc(), Suppressed())
}

// EmitTo is the testable variant of EmitAttach: explicit writer, isTTY
// hint, suppressed hint. Used by both EmitAttach (with os.Stderr +
// real isatty + real env) and unit tests (with a bytes.Buffer + fixed
// flags) so behavior can be verified without touching the real
// terminal or the process environment.
func EmitTo(w io.Writer, isTTY bool, suppressed bool, msg *ipc.AttachedMsg) {
	if msg == nil || !msg.OK {
		return
	}
	if suppressed || !isTTY {
		return
	}
	writeTitle(w, FormatTitle(msg))
}

// ClearTo is the testable variant of Clear.
func ClearTo(w io.Writer, isTTY bool, suppressed bool) {
	if suppressed || !isTTY {
		return
	}
	writeTitle(w, "")
}

// FormatTitle is the pure title-string builder for an AttachedMsg.
// Returns the title body WITHOUT the surrounding OSC-0 escape framing
// (so tests can assert the exact "c3: foo · bar" / "c3: dm" form
// independently of the wire bytes).
//
// Rules:
//   - Name=="dm"             → "c3: dm" (canonical DM attach, also covers the
//     disambiguate_dm "actual DM" branch).
//   - Name set + Group set   → "c3: <name> · <group>" (the common case).
//   - Name set, no Group     → "c3: <name>" (defensive — broker normally
//     populates Group; tolerate the missing field).
//   - Name empty             → "c3" (defensive — never emit "c3: " with a
//     dangling colon).
//
// Bullet separator is U+00B7 MIDDLE DOT to match the task brief's
// example verbatim.
func FormatTitle(msg *ipc.AttachedMsg) string {
	if msg == nil {
		return "c3"
	}
	name := strings.TrimSpace(msg.Name)
	if name == "dm" {
		return "c3: dm"
	}
	if name == "" {
		return "c3"
	}
	group := strings.TrimSpace(msg.Group)
	if group == "" {
		return "c3: " + name
	}
	return "c3: " + name + " · " + group
}

// Suppressed reports whether the C3_NO_TERMINAL_TITLE env var is set to a
// truthy value. Matches the C3_NO_PROMPT precedent in
// cmd/c3-broker/preamble.go.
func Suppressed() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("C3_NO_TERMINAL_TITLE")))
	switch v {
	case "", "0", "false", "no", "off":
		return false
	}
	return true
}

// isStderrTTY reports whether os.Stderr is connected to a terminal.
// Wrapped so tests don't depend on the build host's stderr.
func isStderrTTY() bool {
	return term.IsTerminal(int(os.Stderr.Fd()))
}

// writeTitle writes the OSC-0 escape framing around title to w. Any
// write error is swallowed — title-bar updates are best-effort by
// design; the adapter's primary surface is stdout MCP framing, and a
// failed escape write must never derail the attach response. Comment
// pointing reviewers at the explicit-discard convention used elsewhere
// in c3 (see internal/broker for the same pattern around
// `_ = conn.WriteJSON(...)`).
func writeTitle(w io.Writer, title string) {
	// OSC-0: ESC ] 0 ; <text> BEL. The BEL terminator (\x07) is more
	// broadly compatible than the ST form (\x1b\x5c).
	_, _ = fmt.Fprintf(w, "\x1b]0;%s\x07", title)
}
