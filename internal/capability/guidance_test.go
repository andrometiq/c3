package capability

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// telegramLikeCaps is a full-featured manifest mirroring the Telegram literal,
// used to anti-drift-check the POSITIVE guidance lines.
func telegramLikeCaps() c3types.Capabilities {
	return c3types.Capabilities{
		Channel:         "telegram",
		RichText:        true,
		MaxMessageRunes: 4096,
		MaxCaptionRunes: 1024,
		AutoChunks:      true,
		MediaKinds: []c3types.MediaKind{
			c3types.MediaPhoto, c3types.MediaFile, c3types.MediaVideo,
			c3types.MediaAudio, c3types.MediaVoice, c3types.MediaAnimation,
		},
		CompressedPhoto:  true,
		OriginalFile:     true,
		Albums:           false,
		MaxSendBytes:     50 * 1024 * 1024,
		Polls:            true,
		Reactions:        true,
		ReactionsSingle:  true,
		EditMessages:     true,
		Threads:          true,
		Typing:           true,
		ExpandableQuotes: true,
		InlineKeyboards:  true,
		Inbound: c3types.InboundCaps{
			DeliversPollResults: true,
			DeliversReactions:   true,
			DeliversCallbacks:   true,
		},
		Stream: c3types.StreamCaps{StreamViaEdit: false},
	}
}

func assertContainsAll(t *testing.T, s string, subs []string) {
	t.Helper()
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			t.Errorf("guidance missing expected substring:\n  want: %q\n  got:\n%s", sub, s)
		}
	}
}

func assertContainsNone(t *testing.T, s string, subs []string) {
	t.Helper()
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			t.Errorf("guidance unexpectedly contains substring %q\n  got:\n%s", sub, s)
		}
	}
}

func TestGuidanceFor_PositiveLines(t *testing.T) {
	g := GuidanceFor(telegramLikeCaps())
	assertContainsAll(t, g, []string{
		"CHANNEL CAPABILITIES (telegram):",
		// Rich text — prescriptive (format-for-readability), not merely permissive.
		"Rich text: YES",
		"Write standard markdown",
		"you SHOULD use it whenever structure makes a reply easier to read",
		"never leave\n  structured content (steps, comparisons, multiple points, code) as an unbroken wall of text",
		"render three findings as a numbered list, not one run-on sentence",
		// Expandable "Show more" blockquote guidance (gated on ExpandableQuotes).
		"collapse behind a 'Show more' chevron",
		"end the\n  blockquote with a line containing only `||`",
		// Wide-table honesty line (P6) — gated on RichTables=false (the default in
		// telegramLikeCaps). Asserts the HONEST cross-client claim (desktop/web
		// WRAP, Android scrolls), not "scrolls everywhere".
		"Wide tables: rendered as a monospace block for column alignment",
		"Telegram does NOT\n  scroll wide content uniformly",
		"desktop/web WRAP (breaking alignment), Android scrolls",
		"Keep tables narrow (transpose, or fewer columns); for a truly wide table, send an image.",
		// Media: the load-bearing file-vs-photo distinction.
		`kind="file" delivers the ORIGINAL bytes`,
		`kind="photo" is a COMPRESSED in-chat preview`,
		// Typing is automatic, not a tool.
		"shown automatically while you work",
		"do NOT call any typing tool",
		// Polls supported, including the full P2 surface (quiz/explanation/timed).
		"Polls: supported",
		`type="quiz"`,
		"correct_option",
		"explanation",
		"open_period",
		"close_date",
		// Poll-result reading (P4) — gated on Inbound.DeliversPollResults.
		"delivered automatically as a `<channel>` event when the poll CLOSES",
		"AGGREGATE tallies only",
		"`stop_poll` tool",
		// Inbound reaction + callback events (P4) — gated on the delivery bools.
		"Inbound reactions:",
		"Button presses:",
		"auto-acknowledged",
		// Outbound inline keyboards (P7) — gated on InlineKeyboards.
		"Buttons: attach an inline keyboard with the `buttons` arg",
		"{text, data}",
		"{text, url}",
		"Keep callback\n  data short (<=64 bytes)",
		// Ask round-trip (Phase 1) — also gated on InlineKeyboards.
		"Asking a question with choices: use the `ask` tool",
		"BLOCKS until the human taps one",
		"NOT the host's AskUserQuestion, NOT a fire-and-forget `reply` with buttons",
		// Sender-gating reality (FIX-3): the answer is not locked to one user.
		"Anyone allowlisted who can post in this topic can tap an answer",
		// Auto-mode honesty (DOGFOOD F): a tap is informational, not an authorization.
		"A tapped `ask` answer or `reply` button is informational input",
	})
	// On a fully-supported channel, the feature negatives do not appear.
	// (Streaming is OFF even on the full Telegram manifest in v1, so its
	// negative line is asserted by TestGuidanceFor_NegativeLines, not here.)
	assertContainsNone(t, g, []string{
		"Polls: NOT supported",
		"Rich text: NOT supported",
		"Media: NOT supported",
		// RichTables is OFF in telegramLikeCaps, so the native-table line must NOT
		// appear (the monospace wording asserted above is shown instead).
		"Tables render natively as real tables",
	})
}

// TestGuidanceFor_RichTablesOn asserts the table guidance flips when RichTables
// is enabled: the native-table wording appears and the monospace wording is
// suppressed. This pins both branches of the RichTables gate (anti-drift).
func TestGuidanceFor_RichTablesOn(t *testing.T) {
	caps := telegramLikeCaps()
	caps.RichTables = true
	g := GuidanceFor(caps)
	assertContainsAll(t, g, []string{
		"Tables render natively as real tables — just write a normal GFM pipe table;",
		"no need to keep them narrow.",
	})
	assertContainsNone(t, g, []string{
		"Wide tables: rendered as a monospace block for column alignment",
		"Keep tables narrow (transpose, or fewer columns); for a truly wide table, send an image.",
	})
}

func TestGuidanceFor_NegativeLines(t *testing.T) {
	caps := telegramLikeCaps()
	caps.Polls = false
	caps.Stream.StreamViaEdit = false // explicitly the v1 default
	caps.ExpandableQuotes = false     // a channel without the Show-more affordance
	caps.InlineKeyboards = false      // a channel without outbound inline keyboards
	caps.Inbound.DeliversPollResults = false
	caps.Inbound.DeliversReactions = false
	caps.Inbound.DeliversCallbacks = false
	g := GuidanceFor(caps)
	assertContainsAll(t, g, []string{
		"Polls: NOT supported",
		"Streaming of reasoning: NOT available",
	})
	// Inbound-event + expandable-quote guidance are gated on manifest bools — a
	// channel without those capabilities must NOT advertise them.
	assertContainsNone(t, g, []string{
		"collapse behind a 'Show more' chevron",
		"delivered automatically as a `<channel>` event when the poll CLOSES",
		"Inbound reactions:",
		"Button presses:",
		// Outbound inline-keyboard guidance is gated on InlineKeyboards.
		"Buttons: attach an inline keyboard with the `buttons` arg",
		// The ask-tool guidance is gated on InlineKeyboards too.
		"Asking a question with choices: use the `ask` tool",
		// The sender-gating note lives inside the InlineKeyboards block.
		"Anyone allowlisted who can post in this topic can tap an answer",
		// The auto-mode honesty note also lives inside the InlineKeyboards block.
		"A tapped `ask` answer or `reply` button is informational input",
	})
}
