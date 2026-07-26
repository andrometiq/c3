package c3types

import "time"

// Capabilities is the flat, JSON-serializable per-channel capability
// manifest returned by channel.Channel.Capabilities(). Spec
// (2026-06-14-channel-capability-architecture) §"Key interfaces (sketch)".
//
// It describes feature-level capabilities only (booleans + numeric limits +
// supported media kinds) plus an Inbound sub-section and a Stream sub-section.
// It carries NO Telegram-specific identifiers — core code and the agent
// surface read this manifest; the channel implementation owns the concrete
// rendering/limits behind it.
//
// These types are additive in P0 and may be unused until later phases wire
// them into the Outbound path, the gate, and the IPC payloads.
type Capabilities struct {
	// Channel is the canonical channel name this manifest describes
	// (e.g. "telegram").
	Channel string `json:"Channel"`

	// RichText reports whether the channel renders markdown markup.
	RichText bool `json:"RichText"`

	// MaxMessageRunes is the maximum length of a single text message,
	// measured in the units the channel counts (Telegram counts UTF-16
	// code units). AutoChunks reports whether longer text is split
	// automatically into multiple messages.
	MaxMessageRunes int `json:"MaxMessageRunes"`
	// MaxMessageRunesSource is the SOURCE-markdown budget the chunker should
	// split at, which is intentionally LESS than the hard MaxMessageRunes wire
	// limit to leave headroom for the expansion that channel-side rendering
	// applies to the source before sending (e.g. a channel that converts
	// markdown to HTML escapes `< > &` and wraps `**x**` in tags, all of which
	// lengthen the text). The gate measures the SOURCE length against this
	// budget so a converted message stays under the hard limit. Channel-neutral:
	// no rendering detail leaks here, only the int budget. When 0 (a channel
	// that sets no headroom, or whose rendering does not expand the source) the
	// gate falls back to splitting at MaxMessageRunes — byte-identical to the
	// pre-headroom behavior.
	MaxMessageRunesSource int `json:"MaxMessageRunesSource"`
	// MaxCaptionRunes is the maximum length of a media caption.
	MaxCaptionRunes int  `json:"MaxCaptionRunes"`
	AutoChunks      bool `json:"AutoChunks"`

	// MediaKinds lists the media kinds the channel can send. CompressedPhoto
	// reports whether a compressed in-chat photo preview is supported;
	// OriginalFile reports whether byte-for-byte original-file delivery is
	// supported; Albums reports whether multiple media can be grouped into a
	// single album (FALSE in v1 — sequential single sends).
	MediaKinds      []MediaKind `json:"MediaKinds"`
	CompressedPhoto bool        `json:"CompressedPhoto"`
	OriginalFile    bool        `json:"OriginalFile"`
	Albums          bool        `json:"Albums"`

	// MaxSendBytes is the per-item upload size ceiling.
	MaxSendBytes int64 `json:"MaxSendBytes"`

	// Feature flags.
	Polls           bool `json:"Polls"`
	Reactions       bool `json:"Reactions"`
	ReactionsSingle bool `json:"ReactionsSingle"`
	EditMessages    bool `json:"EditMessages"`
	Threads         bool `json:"Threads"`
	Typing          bool `json:"Typing"`

	// ExpandableQuotes reports whether the channel can render a long quoted
	// block as a collapsible "Show more" affordance (Telegram's
	// expandable_blockquote). Channel-neutral: the trigger construct and wire
	// rendering live in the channel implementation.
	ExpandableQuotes bool `json:"ExpandableQuotes"`

	// InlineKeyboards reports whether the channel can attach an inline keyboard
	// (rows of Buttons) to an outbound message — {text + data} callback buttons
	// (a tap comes back as an InboundCallback event) and {text + url} link
	// buttons. When false the capability gate DROPS any Outbound.Buttons and
	// records a degradation note, so a leaner channel fails gracefully rather
	// than erroring. Channel-neutral: the wire markup + any byte/row limits live
	// in the channel implementation.
	InlineKeyboards bool `json:"InlineKeyboards"`

	// RichMessages reports whether the channel can send native rich messages
	// (structured blocks beyond inline markup) — e.g. Telegram's Bot API 10.1
	// sendRichMessage. Channel-neutral: the wire method, payload shape, and any
	// rich-message limits live entirely in the channel implementation.
	RichMessages bool `json:"RichMessages"`

	// RichTables reports whether the channel renders a GFM pipe table as a REAL
	// native table (not a monospace approximation). When true the agent may write
	// ordinary GFM pipe tables without keeping them narrow; when false the channel
	// falls back to a monospace block and the guidance says to keep tables narrow.
	// Channel-neutral: how a table is detected, capped, and sent lives in the
	// channel implementation.
	RichTables bool `json:"RichTables"`

	// Inbound describes inbound-direction capabilities.
	Inbound InboundCaps `json:"Inbound"`
	// Stream describes reasoning-streaming capabilities (DEFERRED in v1).
	Stream StreamCaps `json:"Stream"`
}

// InboundCaps describes the inbound-direction capabilities of a channel.
type InboundCaps struct {
	// MaxDownloadBytes is the size ceiling for downloading an inbound
	// attachment.
	MaxDownloadBytes int64 `json:"MaxDownloadBytes"`
	// InboundKinds lists the attachment kinds the channel delivers inbound.
	InboundKinds []MediaKind `json:"InboundKinds"`
	// SupportsReplyContext reports whether inbound messages can carry a
	// quote-reply context.
	SupportsReplyContext bool `json:"SupportsReplyContext"`

	// DeliversPollResults reports whether the channel surfaces aggregate poll
	// tallies (counts per option, total voters, is_closed) to the agent as
	// inbound events. Q-RESULT-1: aggregate-only, final-on-close + stop_poll.
	DeliversPollResults bool `json:"DeliversPollResults"`
	// DeliversReactions reports whether the channel surfaces inbound reaction
	// changes (added/removed emoji) to the agent as inbound events.
	DeliversReactions bool `json:"DeliversReactions"`
	// DeliversCallbacks reports whether the channel surfaces inline-keyboard
	// button presses (callbacks) to the agent as inbound events. The channel
	// auto-acks the callback before surfacing it (Q-RESULT-2).
	DeliversCallbacks bool `json:"DeliversCallbacks"`
	// DeliversRichMessages reports that the channel decodes inbound Bot API 10.1
	// rich messages (Message.rich_message) into the agent-facing Text + media
	// attachments, rather than surfacing them empty.
	DeliversRichMessages bool `json:"DeliversRichMessages"`
}

// StreamCaps describes a channel's ability to stream in-flight reasoning.
// In v1 streaming is DEFERRED (StreamViaEdit is always false); see the spec
// §"Streaming reality (R4)".
type StreamCaps struct {
	// StreamViaEdit reports whether reasoning can be streamed by editing an
	// in-flight message. FALSE in v1.
	StreamViaEdit bool `json:"StreamViaEdit"`
	// MinEditInterval is the minimum interval between successive edits the
	// channel will tolerate without rate-limiting.
	MinEditInterval time.Duration `json:"MinEditInterval"`
}

// Markup is the channel-neutral markup intent for an Outbound message:
//   - "none"     — plain text, no markup.
//   - "markdown" — agent-authored standard markdown; the channel converts.
//   - "native"   — restricted opaque pass-through in the channel's own dialect
//     (the gate flags its use).
type Markup string

const (
	MarkupNone     Markup = "none"
	MarkupMarkdown Markup = "markdown"
	MarkupNative   Markup = "native"
)

// MediaKind is the channel-neutral kind of a media item.
//   - "photo"     — compressed in-chat preview (loses original bytes/EXIF).
//   - "file"      — byte-for-byte original document.
//   - "video" / "audio" / "voice" / "animation" — their respective media.
type MediaKind string

const (
	MediaPhoto     MediaKind = "photo"
	MediaFile      MediaKind = "file"
	MediaVideo     MediaKind = "video"
	MediaAudio     MediaKind = "audio"
	MediaVoice     MediaKind = "voice"
	MediaAnimation MediaKind = "animation"
)

// MediaItem is one piece of outbound media. Exactly one of Path (a local
// file on the shared single host) or URL (fetched server-side by the channel)
// is set. Caption is an optional caption; Spoiler hides the item behind a
// spoiler overlay where supported.
type MediaItem struct {
	Kind    MediaKind `json:"Kind"`
	Path    string    `json:"Path"`
	URL     string    `json:"URL"`
	Caption string    `json:"Caption"`
	Spoiler bool      `json:"Spoiler"`
}

// PollKind is the channel-neutral kind of a poll. The zero value ("") behaves
// exactly as a regular poll, so existing callers that never set Kind are
// unaffected.
type PollKind string

const (
	// PollRegular is an ordinary multiple-choice poll (the default).
	PollRegular PollKind = "regular"
	// PollQuiz is a quiz poll: one option is the correct answer and an optional
	// explanation is shown on a wrong answer.
	PollQuiz PollKind = "quiz"
)

// PollSpec describes an outbound poll. Options is the ordered list of answer
// strings; Anonymous and MultipleAnswers tune the poll behavior. The Kind /
// CorrectOption / Explanation / OpenPeriodSec / CloseDateUnix fields extend the
// regular poll to the full send surface (quiz mode, explanation, timed polls).
// A zero-value Kind ("") is treated as a regular poll, so existing callers that
// set only the first four fields produce byte-identical behavior.
type PollSpec struct {
	Question        string   `json:"Question"`
	Options         []string `json:"Options"`
	Anonymous       bool     `json:"Anonymous"`
	MultipleAnswers bool     `json:"MultipleAnswers"`

	// Kind selects regular vs quiz. "" => regular (back-compat with existing
	// callers).
	Kind PollKind `json:"Kind"`
	// CorrectOption is the 0-based index of the correct answer. Required when
	// Kind==quiz; ignored otherwise. A pointer so 0 (a valid index) is
	// distinguishable from unset.
	CorrectOption *int `json:"CorrectOption"`
	// Explanation is shown when a quiz answer is wrong (0-200 chars). Ignored
	// for a regular poll.
	Explanation string `json:"Explanation"`
	// OpenPeriodSec is the number of seconds the poll stays open before it
	// auto-closes; 0 means unset. Mutually exclusive with CloseDateUnix.
	OpenPeriodSec int `json:"OpenPeriodSec"`
	// CloseDateUnix is the Unix timestamp at which the poll auto-closes; 0 means
	// unset. Mutually exclusive with OpenPeriodSec.
	CloseDateUnix int64 `json:"CloseDateUnix"`
}

// Alteration is one structured record of a change the capability gate made to
// an Outbound while degrading it to fit a channel's manifest (e.g. demoting a
// photo to a document, splitting over-limit text). Kind is a stable machine
// tag; Detail is a human-readable explanation. The pure gate returns these;
// the impure broker dispatch writes the durable log line from them.
type Alteration struct {
	Kind   string `json:"Kind"`
	Detail string `json:"Detail"`
}
