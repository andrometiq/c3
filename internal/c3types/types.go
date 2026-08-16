// Package c3types holds wire-shaped Go types shared by channels, plugins,
// and the broker. Spec §4.1.
package c3types

import "time"

// Inbound is one message received from a Channel, post-debounce and
// post-plugin-pipeline, on its way to a CLI adapter. The broker emits
// this as ipc.OpInbound when a stub holds the route claim.
//
// TopicID semantics for Telegram: nil = DM (no topic), &1 = General
// (Telegram's reserved root topic), and any positive int64 > 1 = a
// custom forum topic.
//
// ChatID sign convention follows Telegram's Bot API: positive = user
// or DM, negative = group/channel, -100… = supergroup/channel.
// EVERY exported field below carries an EXPLICIT json tag. That is not
// decoration: until these tags existed, encoding/json fell back to the Go field
// name verbatim, so the Go identifiers WERE the on-disk queue format
// (internal/queue/store.go marshals this struct straight into a .jsonl with no
// intermediate DTO) and WERE the IPC wire format third-party adapters read.
// The original fields therefore retain BYTE-IDENTICAL Go-name keys; newer
// additive fields use their explicitly specified keys. Once introduced, every
// key is frozen: a rename — however innocuous — would silently change both
// formats and eat users' queued messages. Do NOT "tidy" any existing tag.
type Inbound struct {
	Channel       string         `json:"Channel"`
	ChatID        int64          `json:"ChatID"`
	TopicID       *int64         `json:"TopicID"` // nil = no topic, &1 = General, >1 = custom
	MessageID     int64          `json:"MessageID"`
	Sender        Sender         `json:"Sender"`
	Text          string         `json:"Text"`
	Attachments   []Attachment   `json:"Attachments"`
	ReplyTo       *ReplyContext  `json:"ReplyTo"`
	Timestamp     time.Time      `json:"Timestamp"`
	MediaGroupID  string         `json:"media_group_id,omitempty"`
	ForwardOrigin *ForwardOrigin `json:"forward_origin,omitempty"`
	// Merged carries delivery-time source boundaries for a debounced push. Queue
	// rows are persisted before merging, so this presentation-only field is never
	// stored on an organic row.
	Merged []MergedSource `json:"merged,omitempty"`

	// Kind classifies this inbound. The zero value ("") is an ordinary text/
	// media message — every pre-existing caller and the whole delivery path are
	// unchanged when Kind is unset (back-compat). A non-empty Kind marks a
	// synthesized channel EVENT (poll result, reaction, callback) whose payload
	// rides in Event. The route worker treats a Kind != "" inbound specially:
	// it flushes ALONE (never merged into a text debounce batch) and BYPASSES
	// voice/STT handling (CB-1). See internal/broker/worker.go.
	Kind InboundKind `json:"Kind,omitempty"`
	// Event carries the channel-neutral payload for a non-message Kind. Nil for
	// an ordinary message.
	Event *InboundEvent `json:"Event,omitempty"`

	// DrainedFrom is the canonical numeric route key of the immediate prior hop
	// when this line was moved into its current queue by a drain; empty for
	// organic messages. It keeps drain provenance machine-readable (beyond the
	// human banner baked into Text) and, being keyed on numeric chat/topic ids,
	// survives topic renames. The omitempty tag mirrors Kind/Event: an empty
	// DrainedFrom is omitted so every pre-drain marshal stays byte-identical, and
	// an old JSONL line without the field unmarshals to "" (zero migration).
	DrainedFrom string `json:"DrainedFrom,omitempty"`

	// V is the on-disk/wire RECORD FORMAT version of this Inbound. Absent or 0
	// MEANS VERSION 1 — every record written before this field existed has no
	// "V" key at all, and there is no migration pass that could add one, so
	// "missing == 1" is the only workable reading. Writers set 1 from now on.
	//
	// Why it exists: the format had no version marker whatsoever. Every record
	// already on users' disks and every frame already on the IPC wire was
	// structurally indistinguishable from any future revision of it, so the FIRST
	// incompatible change would have had nothing to branch on — a reader could not
	// tell "old record, apply the old reading" from "new record, apply the new
	// one". This field is the branch point, added while the format still has
	// exactly one version and the answer is therefore unambiguous.
	//
	// Readers MUST NOT reject an unknown HIGHER value. A newer writer sharing a
	// queue directory or a socket with an older reader is a normal mixed-version
	// state (a partially-updated install), and hard-failing on V > known turns a
	// cosmetic skew into lost messages — the exact outcome this versioning is here
	// to make survivable. Treat a higher V as "best-effort decode".
	//
	// omitempty is REQUIRED: without it every record this build writes would gain
	// a "V":0 key, which is itself an unannounced format change — precisely what
	// is being prevented.
	V int `json:"V,omitempty"`

	// ConvKind is the conversation kind the message arrived in: "dm" or "group",
	// stated BY THE CHANNEL that received it. Empty means "the channel did not
	// say" — for those records the consumer MUST fall back to its existing legacy
	// heuristic, which keeps every pre-existing record and every channel that has
	// not been taught to set this working unchanged.
	//
	// Why it exists: the broker currently decides DM-vs-group by testing
	// `in.ChatID > 0` (internal/broker/pairing.go). That is TELEGRAM's chat-id
	// sign convention — positive = user/DM, negative = group/supergroup — living
	// inside channel-neutral broker code, and it feeds the allowlist trust
	// boundary, i.e. a channel whose ids do not follow Telegram's sign rule would
	// be classified wrong at exactly the place where being wrong decides who is
	// allowed in. The channel already knows the answer for certain; this field
	// lets it state the fact instead of leaving the broker to infer it from an
	// identifier's sign.
	//
	// omitempty is REQUIRED, for the same reason as V: an unconditional
	// "ConvKind":"" on every written record would itself be a format change.
	ConvKind string `json:"ConvKind,omitempty"`

	// Edited marks an inbound that re-delivers an ALREADY-DELIVERED MessageID
	// with NEW content — a user editing a message they already sent. The channel
	// states it; the broker needs it because its delivered-dedup is keyed on
	// MessageID alone (internal/broker/worker.go deliveredDedup), which cannot
	// tell an amendment from the crash-replay it exists to suppress. Without this
	// bit an edit is classified a duplicate, skipped for storage, and its update
	// acked — the user's correction is destroyed silently.
	//
	// omitempty is REQUIRED, same reason as V and ConvKind: an unconditional
	// "Edited":false on every record would itself be a format change. An old
	// JSONL line without the key unmarshals to false, the correct reading.
	Edited bool `json:"Edited,omitempty"`
}

// InboundRecordVersion is the record format version a writer stamps into
// Inbound.V today. It is 1 because the format that shipped without a V key at
// all IS version 1 — a stamped 1 and an absent V describe the identical shape,
// which is what makes adding the marker a no-op for every existing reader.
// Bump it only alongside an actual incompatible change to the record shape.
const InboundRecordVersion = 1

// The two values Inbound.ConvKind may take. Anything else — including "" —
// means the channel did not state a kind, and the consumer falls back to its
// own rule (see internal/broker/pairing.go isPrivateChat).
const (
	ConvKindDM    = "dm"
	ConvKindGroup = "group"
)

// IsEvent reports whether this inbound is a synthesized channel event (poll
// result / reaction / callback) rather than an ordinary text/media message.
// The route worker uses this to keep events out of the text-debounce/STT path.
func (in *Inbound) IsEvent() bool {
	return in != nil && in.Kind != InboundMessage
}

// InboundKind classifies an Inbound. The zero value is an ordinary message; the
// other kinds mark synthesized channel events surfaced to the agent. All values
// are channel-neutral — no Telegram identifier leaks into this type.
type InboundKind string

const (
	// InboundMessage is the zero value: an ordinary text/media message. Existing
	// callers that never set Kind produce exactly this, so delivery is unchanged.
	InboundMessage InboundKind = ""
	// InboundPollResult is an aggregate poll tally (counts per option, total
	// voters, is_closed). Surfaced on poll close / stop_poll, never per-voter.
	InboundPollResult InboundKind = "poll_result"
	// InboundReaction is a change of reactions on a message (added/removed set).
	InboundReaction InboundKind = "reaction"
	// InboundCallback is an inline-keyboard button press (already auto-acked by
	// the channel before this is surfaced).
	InboundCallback InboundKind = "callback"
	// InboundSystem is a broker-originated system advisory (NOT user input):
	// e.g. a channel-health alert broadcast to every live CLI session. It is
	// trusted (broker-sourced) and therefore bypasses the inbound allowlist
	// gate — see broker.broadcastSystemEvent. The payload rides in
	// InboundEvent.System.
	InboundSystem InboundKind = "system"
)

// InboundEvent is the channel-neutral payload for a non-message Inbound. Exactly
// one field is set, matching the owning Inbound.Kind. No Telegram/gotgbot types
// appear here — the channel converts its wire shapes into these neutral structs.
type InboundEvent struct {
	PollResult *PollResult    `json:"PollResult,omitempty"`
	Reaction   *ReactionEvent `json:"Reaction,omitempty"`
	Callback   *CallbackEvent `json:"Callback,omitempty"`
	System     *SystemEvent   `json:"System,omitempty"`
}

// SystemEvent is a broker-originated advisory surfaced to the agent (Inbound.Kind
// == InboundSystem). It carries NO user content — it is an operational signal
// (e.g. "the Telegram fetch is DOWN, your phone messages won't arrive"). It is
// channel-neutral: Source names which channel/subsystem raised it as a plain
// string; no Telegram/gotgbot type appears here. Level is a coarse severity the
// adapters can render ("warn"/"info"). Title is a short headline; Message is the
// one-line detail.
type SystemEvent struct {
	Source  string `json:"Source"` // e.g. "telegram" — the channel/subsystem that raised this
	Level   string `json:"Level"`  // "warn" | "info"
	Title   string `json:"Title"`
	Message string `json:"Message"`
}

// HealthState is the coarse fetch-health state of a channel's inbound path.
type HealthState string

const (
	// HealthStateUp means the channel's inbound fetch is healthy (recently
	// succeeded).
	HealthStateUp HealthState = "up"
	// HealthStateDown means the channel cannot reach its upstream to fetch
	// inbound — phone messages will not arrive until it recovers.
	HealthStateDown HealthState = "down"
)

// HealthEvent is a channel-neutral fetch-health transition the broker fans out
// to its out-of-band sinks (desktop notify, CLI broadcast, status line, log).
// It is emitted EXACTLY on an edge (UP→DOWN / DOWN→UP), never per attempt, so a
// consumer sees two loud signals per outage cycle. No Telegram/gotgbot type
// appears here — the channel translates its transport state into this neutral
// shape.
type HealthEvent struct {
	Channel string        `json:"Channel"` // e.g. "telegram"
	State   HealthState   `json:"State"`   // "up" | "down"
	Since   time.Time     `json:"Since"`   // when the channel entered this state
	Consec  int           `json:"Consec"`  // consecutive transport failures (for a DOWN edge)
	Reason  string        `json:"Reason"`  // short human cause, e.g. "dial failures" / "timeout"
	DownFor time.Duration `json:"DownFor"`
}

// PollResult is an aggregate poll tally. Q-RESULT-1 = AGGREGATE + FINAL-ON-CLOSE:
// it surfaces counts per option + total voters + is_closed, NEVER per-individual-
// voter identity.
type PollResult struct {
	PollID      string            `json:"PollID"`
	Question    string            `json:"Question"`
	TotalVoters int               `json:"TotalVoters"`
	IsClosed    bool              `json:"IsClosed"`
	Options     []PollOptionTally `json:"Options"`
}

// PollOptionTally is one option's vote count in a PollResult.
type PollOptionTally struct {
	Text       string `json:"Text"`
	VoterCount int    `json:"VoterCount"`
}

// ReactionEvent is a change of reactions on a single message. Added/Removed are
// the set-difference of the new vs old reaction lists, rendered as display
// strings (standard emoji verbatim; custom/paid reactions as the sentinels
// "[custom]"/"[paid]" so the agent sees that SOMETHING reacted, never silently
// dropped).
type ReactionEvent struct {
	MessageID int64    `json:"MessageID"`
	Actor     Sender   `json:"Actor"`
	Added     []string `json:"Added"`
	Removed   []string `json:"Removed"`
}

// CallbackEvent is an inline-keyboard button press. CallbackID is the id the
// channel needs to answerCallbackQuery (it auto-acks before surfacing this).
// Data is the opaque callback payload string attached to the button.
type CallbackEvent struct {
	CallbackID string `json:"CallbackID"`
	MessageID  int64  `json:"MessageID"`
	Actor      Sender `json:"Actor"`
	Data       string `json:"Data"`
}

// Sender identifies the originator of an Inbound or ReplyContext. UserID
// is the channel's canonical user identifier; Username is a display-only
// secondary that may be empty (e.g. a user without a Telegram @handle).
type Sender struct {
	UserID   int64  `json:"UserID"`
	Username string `json:"Username"`
}

// ForwardOrigin is the flattened provenance of a forwarded message.
type ForwardOrigin struct {
	Kind string `json:"kind"`           // "user" | "hidden_user" | "chat" | "channel"
	Name string `json:"name,omitempty"` // best available display name
}

// MergedSource describes one source message inside a merged delivery push.
type MergedSource struct {
	MessageID     int64          `json:"message_id"`
	Sender        Sender         `json:"sender"`
	Text          string         `json:"text,omitempty"`
	ForwardOrigin *ForwardOrigin `json:"forward_origin,omitempty"`
}

// Attachment is one piece of attached media on an Inbound. Channel
// implementations fill the fields they can; absent metadata is the
// zero value. Kind is an open enum on Telegram terms.
type Attachment struct {
	Kind            string `json:"Kind"` // "voice", "audio", "video", "video_note", "document", "photo", "sticker"
	FileID          string `json:"FileID"`
	Size            int64  `json:"Size"`
	MIME            string `json:"MIME"`
	Name            string `json:"Name"`
	SourceMessageID int64  `json:"source_message_id,omitempty"`
}

// ReplyContext describes the message an Inbound is replying to (quote-
// reply). MessageID is the target message's id; User is its author;
// Text is a snippet of the target's content for context (may be empty
// or truncated by the channel).
type ReplyContext struct {
	MessageID int64  `json:"MessageID"`
	User      Sender `json:"User"`
	Text      string `json:"Text"`
}

// Outbound is one tool-call from a CLI adapter to a Channel for delivery.
// Used for both the `reply` tool and as the base shape for derived
// args (see ReplyArgs alias).
//
// Outbound is channel-neutral: Markup is the markup intent (none/markdown/
// native) the channel renders for; Media is the channel-neutral media list;
// Poll is an optional outbound poll. No Telegram-specific identifier (e.g.
// "HTML"/"MarkdownV2" parse modes, file paths) leaks into this shape — the
// channel translates Markup/Media/Poll into its own dialect.
type Outbound struct {
	Channel string      `json:"Channel"`
	ChatID  int64       `json:"ChatID"`
	TopicID *int64      `json:"TopicID"`
	Text    string      `json:"Text"`
	Markup  Markup      `json:"Markup"`
	Media   []MediaItem `json:"Media"`
	Poll    *PollSpec   `json:"Poll"`
	// ReplyTo is a MESSAGE ID (the message being replied to), not a route or a
	// user — the Go name does not contain "MessageID", so a name-based audit of
	// this file misses it. Tagged "ReplyTo" like everything else: the wire key is
	// and always was the Go field name, and correcting the name here would break
	// every adapter that speaks it.
	ReplyTo *int64 `json:"ReplyTo"`

	// Buttons is an optional inline keyboard attached to the message: rows of
	// Buttons (so [][]Button is rows-of-buttons). The zero value (nil/empty)
	// means NO keyboard — a message without buttons is byte-identical to today,
	// so this is fully back-compat. A channel that does not advertise inline-
	// keyboard support degrades by dropping the keyboard (with a note); the
	// channel that does support it translates these neutral Buttons into its own
	// wire markup. No channel-specific limit (e.g. callback-data byte ceiling)
	// is encoded here — those are enforced in the channel implementation.
	Buttons [][]Button `json:"Buttons,omitempty"`
}

// Button is one channel-neutral inline-keyboard button. Text is the visible
// label. EXACTLY ONE of Data or URL is set:
//   - Data is an opaque callback payload (callback_data): tapping the button
//     comes back to the agent as an InboundCallback event carrying this string,
//     so the agent can act on it (e.g. SSHGate-style approve/deny). Keep it
//     short — channels cap callback payloads (Telegram: 1-64 bytes), enforced in
//     the channel.
//   - URL is a link the button opens; tapping it does NOT come back to the agent.
//
// A Button with neither (or both) of Data/URL is invalid; the reply-tool parser
// and the channel reject it with a clear error rather than silently dropping it.
type Button struct {
	Text string `json:"Text"`
	Data string `json:"Data,omitempty"`
	URL  string `json:"URL,omitempty"`
}

// ReplyArgs is the argument shape for the `reply` MCP tool. Aliased to
// Outbound because the wire shape is the same — channels accept the
// same struct for both internal forwarding and adapter-initiated
// replies.
type ReplyArgs = Outbound

// EditArgs is the argument shape for the `edit_message` MCP tool.
// MessageID identifies the message to edit; Text is the new content;
// Markup is the channel-neutral markup intent (none/markdown/native),
// the same convention as Outbound — so edits join the markup system.
type EditArgs struct {
	Channel   string `json:"Channel"`
	ChatID    int64  `json:"ChatID"`
	MessageID int64  `json:"MessageID"`
	Text      string `json:"Text"`
	Markup    Markup `json:"Markup"`

	// Buttons replaces the message's inline keyboard (same rows-of-buttons shape
	// as Outbound.Buttons). Semantics:
	//   - nil  → leave the existing keyboard UNTOUCHED (back-compat: edit_progress
	//     / placeholder edits never carry buttons and must not strip one).
	//   - non-nil EMPTY (`[][]Button{}`) → CLEAR the keyboard (Telegram removes it
	//     when sent an empty inline_keyboard). This is how the `ask` round-trip
	//     drops the buttons once the question is answered.
	//   - non-empty → set that keyboard.
	// A channel without inline-keyboard support ignores this field.
	Buttons [][]Button `json:"Buttons,omitempty"`
}

// EditResult is returned from Channel.EditMessage. MessageID echoes
// back the edited message's id (channels may rewrite it on edit; today
// Telegram doesn't, but the contract leaves room).
type EditResult struct {
	MessageID int64 `json:"MessageID"`
}

// ReactArgs is the argument shape for the `react` MCP tool. Emoji must
// be a single-codepoint Telegram-supported reaction emoji; non-emoji
// values are rejected by the channel.
type ReactArgs struct {
	Channel   string `json:"Channel"`
	ChatID    int64  `json:"ChatID"`
	MessageID int64  `json:"MessageID"`
	Emoji     string `json:"Emoji"`
}

// ReadbackArgs is the argument shape for the OPTIONAL Channel.SendReadback method
// — the voice-transcript "readback" a channel echoes back to the source chat
// after STT succeeds. It is deliberately NOT part of the channel.Channel
// interface: only channels that can render the readback implement it, and the
// broker reaches it via an optional interface type-assert (see
// internal/broker/worker.go), so other channels need no changes.
//
// ChatID/TopicID/ReplyTo follow the same conventions as Outbound (TopicID nil =
// DM/no thread; ReplyTo = the source voice message_id, for the reply-quote UX).
// Transcript is the FULL verbatim transcript — the channel never truncates or
// summarizes it (only a preview may elide the middle).
type ReadbackArgs struct {
	ChatID int64 `json:"ChatID"`
	// ReplyTo is a MESSAGE ID (the source voice message), not a route or a user —
	// same name-does-not-say-so trap as Outbound.ReplyTo. Tagged "ReplyTo",
	// byte-identical to the Go field name, like every other field here.
	ReplyTo    *int64 `json:"ReplyTo"` // source voice message_id (reply-quote)
	TopicID    *int64 `json:"TopicID"`
	Transcript string `json:"Transcript"` // full verbatim transcript
}

// VoicePayload is the input the plugin host passes to OnVoiceReceived
// callbacks (the STT plugin's entry point). FileID identifies the
// voice attachment on the source channel; MIME and Size are hints
// for choosing the right transcription provider.
type VoicePayload struct {
	Channel   string `json:"Channel"`
	ChatID    int64  `json:"ChatID"`
	TopicID   *int64 `json:"TopicID"`
	MessageID int64  `json:"MessageID"`
	FileID    string `json:"FileID"`
	MIME      string `json:"MIME"`
	Size      int64  `json:"Size"`
}
