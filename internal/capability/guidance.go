package capability

import (
	"fmt"
	"strings"

	"github.com/Andrometiq/c3/internal/c3types"
)

// GuidanceFor renders the agent-facing capability + formatting guidance for a
// channel, derived ENTIRELY from the manifest c so it can never drift from what
// the channel actually does (the anti-drift property the P4 golden test
// asserts). It emits POSITIVE guidance for supported capabilities and explicit
// NEGATIVE guidance for any false capability, so the agent never formulates
// content the channel can't deliver.
//
// Spec 2026-06-14-channel-capability-architecture §"GuidanceFor template".
func GuidanceFor(c c3types.Capabilities) string {
	var b strings.Builder

	fmt.Fprintf(&b, "CHANNEL CAPABILITIES (%s):\n", c.Channel)

	// Rich text. Prescriptive, not merely permissive: format for readability.
	// The dividing line is the CONTENT, not a blanket rule — short conversational
	// answers stay plain, but structured/dense content gets the matching markdown.
	if c.RichText {
		b.WriteString("- Rich text: YES — and you SHOULD use it whenever structure makes a reply easier to read.\n")
		b.WriteString("  Write standard markdown: bold/italic for emphasis, lists for steps or enumerations, tables for\n")
		b.WriteString("  comparisons, inline code and fenced blocks for code/commands/paths, block quotes for quoted\n")
		b.WriteString("  text, plus links, strikethrough, spoilers. Keep a one-line answer plain, but never leave\n")
		b.WriteString("  structured content (steps, comparisons, multiple points, code) as an unbroken wall of text —\n")
		b.WriteString("  e.g. render three findings as a numbered list, not one run-on sentence. C3 converts + escapes\n")
		b.WriteString("  for you — do NOT hand-write HTML or channel tags.\n")
		if c.MaxMessageRunes > 0 {
			fmt.Fprintf(&b, "  A reply longer than ~%d chars is split automatically into several messages\n", c.MaxMessageRunes)
			b.WriteString("  (edits/replies reference the first).\n")
		} else {
			b.WriteString("  Long replies are split automatically into several messages (edits/replies reference the first).\n")
		}
		if c.ExpandableQuotes {
			b.WriteString("  For a long quoted block that should collapse behind a 'Show more' chevron, end the\n")
			b.WriteString("  blockquote with a line containing only `||` (still capped at the message length limit).\n")
		}
		// Table guidance, gated on RichTables (anti-drift golden test). When the
		// channel renders GFM tables natively, advertise that; otherwise keep the
		// honest monospace wording (P6) — tables render as an aligned monospace
		// block and Telegram does NOT scroll wide content uniformly, so we do not
		// claim cross-client horizontal scroll.
		if c.RichTables {
			b.WriteString("  Tables render natively as real tables — just write a normal GFM pipe table;\n")
			b.WriteString("  no need to keep them narrow.\n")
		} else {
			b.WriteString("  Wide tables: rendered as a monospace block for column alignment. Telegram does NOT\n")
			b.WriteString("  scroll wide content uniformly — desktop/web WRAP (breaking alignment), Android scrolls.\n")
			b.WriteString("  Keep tables narrow (transpose, or fewer columns); for a truly wide table, send an image.\n")
		}
	} else {
		b.WriteString("- Rich text: NOT supported — write PLAIN text only; markdown/HTML will appear as literal characters.\n")
		if c.MaxMessageRunes > 0 {
			fmt.Fprintf(&b, "  A reply longer than ~%d chars is split automatically into several messages (edits/replies reference the first).\n", c.MaxMessageRunes)
		}
	}

	// Media.
	if len(c.MediaKinds) > 0 {
		b.WriteString("- Media: send via the `media` arg.")
		if c.OriginalFile {
			b.WriteString(` kind="file" delivers the ORIGINAL bytes (PDFs, logs, originals);`)
		}
		if c.CompressedPhoto {
			b.WriteString("\n  ")
			b.WriteString(`kind="photo" is a COMPRESSED in-chat preview (loses original bytes/EXIF).`)
		}
		others := otherMediaKinds(c.MediaKinds)
		if len(others) > 0 {
			fmt.Fprintf(&b, " Also: %s.", strings.Join(others, ", "))
		}
		if c.MaxSendBytes > 0 {
			fmt.Fprintf(&b, " Max ~%dMB per item.", c.MaxSendBytes/(1024*1024))
		}
		if !c.Albums {
			b.WriteString(" Multiple items are sent one after another.")
		}
		b.WriteString("\n")
	} else {
		b.WriteString("- Media: NOT supported — this channel is text-only.\n")
	}

	// Polls.
	if c.Polls {
		b.WriteString("- Polls: supported via the `poll` tool (question + 2-10 options). Set type=\"quiz\" with\n")
		b.WriteString("  correct_option (0-based) and an optional explanation (shown on a wrong answer) for a quiz;\n")
		b.WriteString("  anonymous (default true) and multiple (ignored for a quiz) tune behavior; add a timer with\n")
		b.WriteString("  open_period (seconds) OR close_date (Unix ts).\n")
		// Poll-result reading (P4) — gated on the manifest delivery bool so the
		// guidance stays honest (anti-drift golden test).
		if c.Inbound.DeliversPollResults {
			b.WriteString("  Poll results: delivered automatically as a `<channel>` event when the poll CLOSES —\n")
			b.WriteString("  AGGREGATE tallies only (counts per option + total voters), never per-voter identity.\n")
			b.WriteString("  Use the `stop_poll` tool (with the poll's message_id) to force-close and read the\n")
			b.WriteString("  final tally early.\n")
		}
	} else {
		b.WriteString("- Polls: NOT supported — render the choices as numbered text in a normal reply.\n")
	}

	// Inline keyboards (outbound buttons via the `reply` tool's `buttons` arg).
	// Gated on the manifest bool so the guidance stays honest (anti-drift golden
	// test). A `data` button's tap comes back as a callback event (see the
	// inbound-callback line below) the agent can act on; a `url` button just opens
	// a link.
	if c.InlineKeyboards {
		b.WriteString("- Buttons: attach an inline keyboard with the `buttons` arg (rows of buttons). A button is\n")
		b.WriteString("  either {text, data} (a callback button — its tap comes back to you as a `<channel>` event\n")
		b.WriteString("  carrying the data, so you can act on it) or {text, url} (just opens a link). Keep callback\n")
		b.WriteString("  data short (<=64 bytes).\n")
		// Ask round-trip (Phase 1): the blocking, correlated way to get an answer
		// back. Distinct from fire-and-forget `reply`-with-buttons (whose taps
		// arrive as informational events with no tool result) and from the host's
		// AskUserQuestion (which has no channel surface), so call it out explicitly.
		b.WriteString("- Asking a question with choices: use the `ask` tool (question + options). It shows the options\n")
		b.WriteString("  as buttons and BLOCKS until the human taps one, returning the chosen option as the tool result —\n")
		b.WriteString("  so use it (NOT the host's AskUserQuestion, NOT a fire-and-forget `reply` with buttons) whenever\n")
		b.WriteString("  you need the human to pick before you continue. Set `multi:true` for multi-select (the human\n")
		b.WriteString("  toggles options and taps Done; you get the selected list), and `allow_skip:true` to add a Skip\n")
		b.WriteString("  button. (Free-text / Other questions are not yet supported.)\n")
		b.WriteString("  Anyone allowlisted who can post in this topic can tap an answer — the question is not\n")
		b.WriteString("  restricted to a single user, so treat the reply as coming from the group, not one identity.\n")
		b.WriteString("  A tapped `ask` answer or `reply` button is informational input — it does NOT authorize actions\n")
		b.WriteString("  your harness blocks in auto-accept mode's safety review (high-severity persistence, ~/.claude\n")
		b.WriteString("  self-mod); those need a terminal confirmation or normal permission mode, so don't loop on a tap.\n")
	}

	// Reactions (outbound `react` tool).
	if c.Reactions {
		if c.ReactionsSingle {
			b.WriteString("- Reactions: supported via the `react` tool (ONE emoji per message).\n")
		} else {
			b.WriteString("- Reactions: supported via the `react` tool.\n")
		}
	} else {
		b.WriteString("- Reactions: NOT supported.\n")
	}

	// Inbound events (P4): reactions on / button presses on the bot's messages
	// arrive as `<channel>` events. Gated on the manifest delivery bools.
	if c.Inbound.DeliversReactions {
		b.WriteString("- Inbound reactions: when someone reacts to a message, you receive a `<channel>` event\n")
		b.WriteString("  with the added/removed emoji.\n")
	}
	if c.Inbound.DeliversCallbacks {
		b.WriteString("- Button presses: inline-keyboard callbacks arrive as `<channel>` events (auto-acknowledged\n")
		b.WriteString("  for you).\n")
	}

	// Edits.
	if c.EditMessages {
		b.WriteString("- Editing: supported via the `edit_message` tool (an edit is a single message — it cannot be split).\n")
	} else {
		b.WriteString("- Editing: NOT supported — send a new message instead.\n")
	}

	// Typing.
	if c.Typing {
		b.WriteString("- Typing: shown automatically while you work — do NOT call any typing tool.\n")
	} else {
		b.WriteString("- Typing: not shown on this channel.\n")
	}

	// Streaming.
	if c.Stream.StreamViaEdit {
		b.WriteString("- Streaming of reasoning: available on this channel.\n")
	} else {
		b.WriteString("- Streaming of reasoning: NOT available on this channel.\n")
	}

	return b.String()
}

// otherMediaKinds returns the supported media kinds other than photo/file (which
// have dedicated positive lines above), in a stable order, for the "Also: …"
// clause.
func otherMediaKinds(kinds []c3types.MediaKind) []string {
	order := []c3types.MediaKind{
		c3types.MediaVideo,
		c3types.MediaAudio,
		c3types.MediaVoice,
		c3types.MediaAnimation,
	}
	set := make(map[c3types.MediaKind]bool, len(kinds))
	for _, k := range kinds {
		set[k] = true
	}
	var out []string
	for _, k := range order {
		if set[k] {
			out = append(out, string(k))
		}
	}
	return out
}
