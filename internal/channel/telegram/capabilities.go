package telegram

import (
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// Bot API limits and ceilings the Telegram manifest advertises. Kept as named
// constants here (inside the telegram package) so no caller above this layer
// names the raw numbers — the spec's no-leak rule (R7).
const (
	// maxMessageRunes is Telegram's per-message text limit (UTF-16 units). This
	// is the HARD wire ceiling: a sent message over this is rejected with a 400.
	maxMessageRunes = 4096
	// maxMessageRunesSource is the SOURCE-markdown budget the chunker splits at,
	// kept below the hard maxMessageRunes so that the markdown→HTML conversion
	// (mdToTelegramHTML — escaping `< > &` and wrapping `**x**`→`<b>x</b>`,
	// `[t](u)`→`<a href="u">t</a>`, etc.) cannot push a near-limit chunk over the
	// hard wire limit and earn a 400. The plaintext fallback (outbound.go) stays
	// the net for pathological chunks; this headroom just keeps normal chunked
	// prose from ever hitting it.
	//
	// Headroom rationale: ~10% (410 units) below 4096. Typical agent prose
	// expands only a little under HTML conversion (a few escaped `< > &` and a
	// handful of bold/italic/link tags per 4 KiB), so 10% comfortably absorbs the
	// growth WITHOUT over-fragmenting ordinary text into extra messages. Markup-
	// dense or escape-heavy chunks that still exceed 4096 after conversion remain
	// caught by the plaintext fallback. Chosen as a conservative-but-not-wasteful
	// middle ground rather than a worst-case (every char escaped) budget, which
	// would needlessly split common replies.
	maxMessageRunesSource = 3686
	// maxCaptionRunes is Telegram's per-media caption limit (UTF-16 units).
	maxCaptionRunes = 1024
	// maxSendBytes is the Bot API upload ceiling for media we send (50 MiB).
	maxSendBytes = 50 * 1024 * 1024
	// minEditInterval is the floor between successive edits before Telegram
	// starts rate-limiting; unused while streaming is deferred but reported
	// in the manifest for honesty.
	minEditInterval = 1 * time.Second

	// Inline-keyboard limits (P7). These are Telegram Bot API facts and live in
	// this package only (the no-leak rule); the pure gate makes only the neutral
	// keep-or-drop decision.
	//
	// maxCallbackDataBytes is Telegram's callback_data ceiling: 1-64 BYTES (not
	// runes). A button whose Data exceeds this earns a raw 400, so we pre-check
	// and return a clear error instead.
	maxCallbackDataBytes = 64
	// maxKeyboardRows / maxButtonsPerRow are conservative shape caps. Telegram
	// does not document a hard per-row/row-count number for inline keyboards, but
	// an enormous keyboard is rejected/unusable; these keep the agent honest and
	// turn an over-large keyboard into a clear error rather than a 400.
	maxKeyboardRows  = 100
	maxButtonsPerRow = 8

	// Rich-message limits (Bot API 10.1, sendRichMessage). These are Telegram Bot
	// API facts and live in this package only (the no-leak rule R7) — no raw rich
	// limit number reaches core. Verified against the live "Rich Message Limits"
	// section (2026-06-17): a rich message allows ≤32768 UTF-8 chars and ≤500
	// blocks (a table's ROWS count as blocks; cells do not), and a table allows
	// ≤20 columns. richTableEligible (sendrich.go) enforces the per-table caps so
	// an over-cap table falls back to the monospace renderer instead of earning a
	// Telegram 400.
	maxRichBlocks  = 500
	maxRichColumns = 20
	maxRichChars   = 32768
)

// richTablesEnabled is the switch for native rich-message tables
// (sendRichMessage). It is true (ENABLED 2026-06-17, live-verified on Android):
// every detected GFM table renders as a REAL native RichBlockTable via the
// native-table route (SendReply → sendRich, sendrich.go). The proven monospace
// <pre> block remains the fallback for over-cap tables, send errors, and old
// clients whose graceful degradation is undocumented.
const richTablesEnabled = true // ON: live-verified on Android 2026-06-17 (GFM tables render as native RichBlockTable); monospace <pre> kept as the error/old-client fallback

// Capabilities returns the static Telegram capability manifest. This is the
// authoritative inventory of what the Telegram channel can do; core code and
// the agent surface read it (carried over hello_ack / attached in later
// phases). It is a pure literal — no live bot state is consulted.
//
// v1 descopes: Albums=false (sequential single sends), Stream.StreamViaEdit=
// false (reasoning streaming deferred — no observable source frame).
func (c *Channel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{
		Channel:               Name,
		RichText:              true,
		MaxMessageRunes:       maxMessageRunes,
		MaxMessageRunesSource: maxMessageRunesSource,
		MaxCaptionRunes:       maxCaptionRunes,
		AutoChunks:            true,
		MediaKinds: []c3types.MediaKind{
			c3types.MediaPhoto,
			c3types.MediaFile,
			c3types.MediaVideo,
			c3types.MediaAudio,
			c3types.MediaVoice,
			c3types.MediaAnimation,
		},
		CompressedPhoto:  true,
		OriginalFile:     true,
		Albums:           false, // descoped in v1 — sequential single sends.
		MaxSendBytes:     maxSendBytes,
		Polls:            true,
		Reactions:        true,
		ReactionsSingle:  true,
		EditMessages:     true,
		Threads:          true,
		Typing:           true,
		ExpandableQuotes: true,
		InlineKeyboards:  true,
		// RichMessages reflects that this channel CAN send native rich messages
		// (Bot API 10.1 sendRichMessage via the raw request method). RichTables
		// follows richTablesEnabled, now ENABLED (live-verified on Android
		// 2026-06-17), so the agent is told tables render natively; the monospace
		// <pre> path stays as the error/old-client fallback.
		RichMessages: true,
		RichTables:   richTablesEnabled,
		Inbound: c3types.InboundCaps{
			// C3 declares NO download ceiling, and does not enforce one. The
			// 20 MiB number is a fact about api.telegram.org today, not about
			// the Bot API: a self-hosted server "download[s] files without a
			// size limit"
			// (https://core.telegram.org/bots/api#using-a-local-bot-api-server)
			// and Telegram can move its own number whenever it likes. A baked-in
			// ceiling is therefore a false refusal waiting to happen — in BOTH
			// directions — so the bot server is the only authority: C3 asks it
			// (getFile) and reports whatever it answers. 0 means UNDECLARED.
			MaxDownloadBytes: 0,
			// The attachment kinds Telegram delivers inbound. Mapped onto the
			// neutral MediaKind set; sticker/video_note have no neutral kind
			// in v1 and are omitted.
			InboundKinds: []c3types.MediaKind{
				c3types.MediaPhoto,
				c3types.MediaFile,
				c3types.MediaVideo,
				c3types.MediaAudio,
				c3types.MediaVoice,
				c3types.MediaAnimation,
			},
			SupportsReplyContext: true,
			// P4: Telegram delivers aggregate poll tallies (poll update + stopPoll),
			// inbound reaction changes (message_reaction), and inline-keyboard
			// callbacks (auto-acked, then surfaced). Per-voter answers are NOT
			// surfaced (Q-RESULT-1 aggregate-only).
			DeliversPollResults:  true,
			DeliversReactions:    true,
			DeliversCallbacks:    true,
			DeliversRichMessages: true,
		},
		Stream: c3types.StreamCaps{
			StreamViaEdit:   false, // deferred in v1.
			MinEditInterval: minEditInterval,
		},
	}
}
