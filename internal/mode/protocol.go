// Package mode is the single source of truth for the per-session protocol
// text that c3 surfaces to any agent driving the adapter.
//
// the maintainer's standing principle (2026-05-18, TODO #20): anything duplicated
// between Claude and Codex adapters / install paths (protocol text, install
// instructions, restart commands, tool descriptions, setup-time effects)
// must have ONE source of truth. Implementation surface can differ
// (e.g. Claude reads this from the MCP `instructions` field; Codex reads
// it from a delimited block in `~/.codex/AGENTS.md`) but the underlying
// string must be defined once.
//
// First concrete extraction: ModeProtocol (CLI vs Telegram mode contract)
// and MultipartProtocol (the "start multi-part reply" voice-dictation
// convention) — both previously duplicated as Go constants in each adapter.
//
// Three consumers as of 2026-05-19 (NIT n5):
//  1. Claude adapter — splices Combined() onto its MCP-initialize
//     `instructions` field; see cmd/c3-claude-adapter/main.go.
//  2. Codex adapter — splices Combined() onto its MCP-initialize
//     `instructions` field as well (best-effort: Codex's MCP host
//     does not currently surface this field — see (3)); see
//     cmd/c3-codex-adapter/main.go.
//  3. c3-broker setup's AGENTS.md installer — writes Combined() into
//     a delimited block in `~/.codex/AGENTS.md`, which Codex DOES
//     concatenate into developer_instructions. Lives at
//     cmd/c3-broker/cli_host.go::ensureCodexAgentsMd (+
//     codexAgentsMdBlock).
//
// If you extend the protocol, update all three call sites' tests, not
// just this package's.
package mode

import (
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/capability"
)

// ModeProtocol is the per-session output-mode contract every agent using
// c3 must honor. Appended to every adapter MCP-initialize instructions
// variant so the rule travels with the plugin, not with the user's
// AGENTS.md. the maintainer's standing instruction (2026-05-15): make this part
// of the plugin contract so any agent using c3 understands the protocol
// without per-user setup.
const ModeProtocol = "OUTPUT MODE PROTOCOL (per-session, agent-only state — default CLI mode on every fresh session):\n" +
	"• CLI mode (DEFAULT): your replies go to the CLI terminal. Telegram is INPUT-ONLY — voice and replies from Telegram arrive as `<channel>` blocks. DO NOT call the `reply` tool to respond unless the user explicitly asks.\n" +
	"• Telegram mode: your substantive replies go to Telegram via the `reply` tool. Switch ONLY when the user EXPLICITLY asks (e.g. \"switch to Telegram\" / \"Telegram mode\", or \"switch to CLI\" / \"CLI mode\"). NEVER auto-switch by inferring where the user is — the user sending a message FROM Telegram (phone) is NOT a switch request, and neither is 'stepping away from the laptop'. If you think a switch is warranted, ASK and get explicit confirmation first; do not switch unilaterally.\n" +
	"• The mode is your responsibility to track — the broker doesn't store it. Always start in CLI mode; honor the user's EXPLICIT switch instructions immediately, but never switch on your own.\n" +
	"• After attach completes, briefly announce your current output mode (\"currently in CLI mode\" / \"currently in Telegram mode\") so the human has explicit confirmation of where replies will land."

// MultipartProtocol is the voice-dictation convention: the user announces
// "start multi-part reply", fires a series of short voice bursts that each
// individually look like complete prompts, and the agent must wait for
// "end of multi-part reply" before reasoning over the collected set.
//
// Without this protocol, agents respond to each voice burst as a standalone
// prompt and the user can't dictate a complex thought across multiple short
// utterances. Surfaced to every c3-driven agent via the same path as
// ModeProtocol (MCP `instructions` for Claude, AGENTS.md block for Codex).
const MultipartProtocol = "MULTI-PART REPLY PROTOCOL:\n" +
	"• When the user says \"start multi-part reply\" / \"multi-part reply\", do NOT respond to subsequent messages individually. Acknowledge each with one word (\"Waiting.\") only.\n" +
	"• Process and respond to ALL collected messages at once when the user says \"end of multi-part reply\".\n" +
	"• Reason: lets the user dictate complex thoughts as short voice bursts without intermediate interruption."

// Combined returns the concatenation that adapters splice onto the tail
// of their MCP-initialize `instructions` string. Leading "\n\n" preserves
// the historical wire shape — every adapter previously hard-coded its
// modeProtocol const with that exact prefix, so existing tests / live
// behaviour stay byte-identical for the ModeProtocol section. Between
// the protocols we use a single "\n\n" for separation.
//
// The capability.GuidanceFor(c) section is the channel-capability surface (CMG
// spec §L5) — folded in so the agent learns, in the SAME init/setup delivery as
// the mode/multipart protocols, what the channel can render (rich text, media,
// polls, typing, streaming). c is the resolvable channel's manifest; callers
// source it from the hello_ack / attached payload (live adapters) or from the
// static channel literal (broker setup, which has no live connection). A zero
// Capabilities value renders honest all-NO guidance, so a nil-caps fallback
// never panics.
//
// Ordering (2026-06-20): ModeProtocol stays FIRST — it is the safety-critical
// no-auto-reply / no-auto-switch contract and must not be demoted. The capability
// guidance moves to the MIDDLE (right after the mode contract, read while the
// rules are fresh) so the formatting guidance is no longer the forgotten tail;
// the narrow MultipartProtocol voice-burst convention moves LAST. The leading
// "\n\n" + ModeProtocol byte-shape is preserved for the existing wire tests.
func Combined(c c3types.Capabilities) string {
	return "\n\n" + ModeProtocol + "\n\n" + capability.GuidanceFor(c) + "\n\n" + MultipartProtocol
}
