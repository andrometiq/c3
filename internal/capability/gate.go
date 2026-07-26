// Package capability is the pure, channel-neutral validate+degrade choke point
// (the "Gate") between the broker and a Channel, plus the agent-facing
// capability guidance renderer. It is L2 in the Capability-Manifest-Gate
// architecture (spec 2026-06-14-channel-capability-architecture §"Layers").
//
// Purity contract (load-bearing, enforced by the P7 grep-guard): this package
// imports ONLY internal/c3types + the Go standard library. It does NOT log,
// stat files, dial the network, or import broker/channel/gotgbot. The gate
// returns structured Alterations; the impure broker dispatch writes the durable
// log line from them.
package capability

import (
	"fmt"
	"unicode/utf16"

	"github.com/Andrometiq/c3/internal/c3types"
)

// Gate is the single validate+degrade choke point for an outbound message. It
// takes a channel's capability manifest and one logical Outbound and returns:
//
//   - parts: the Outbound split into one or more ordered messages, each whose
//     text fits the channel's MaxMessageRunes (measured in UTF-16 code units —
//     what Telegram counts). Each part carries the same Markup/ChatID/TopicID;
//     ReplyTo applies only to the FIRST part (reply-threading targets the first
//     message of a multi-part reply).
//   - notes: human-readable strings the dispatch surfaces back to the agent so
//     it knows what was altered (or rejected).
//   - alts: structured Alteration records the impure dispatch turns into durable
//     log lines.
//   - err: a non-nil error signals a HARD REJECTION — the message cannot be
//     delivered as authored and dispatch must surface the error to the agent
//     WITHOUT sending anything (matching how dispatch surfaces tool errors).
//
// v1 responsibilities:
//   - Validate: reject a poll when the channel can't do polls (!c.Polls).
//   - Degrade markup: force Markup=none when the channel has no rich text.
//   - Chunk text: construct-aware split of the SOURCE markdown so a split never
//     bisects a fenced code block, a [label](url) link, or a blockquote run.
//   - Degrade media (P3): demote an unsupported Kind (photo→file when the
//     channel can't send a compressed photo but can send an original file;
//     otherwise drop the item with a note + Alteration).
//
// Single-part content contract (P3): each emitted part carries AT MOST ONE of
// {text, a single media item, a poll}. The layout is:
//
//	parts = [text chunks...] ++ [one part per media item] ++ [poll part if any]
//
// ReplyTo applies only to the FIRST part overall (reply-threading targets the
// first message of a multi-part reply). Album grouping is descoped in v1 — every
// media item is its own single send.
func Gate(c c3types.Capabilities, out c3types.Outbound) (parts []c3types.Outbound, notes []string, alts []c3types.Alteration, err error) {
	// --- Validation (hard rejection) -------------------------------------
	// A poll on a channel that can't render one cannot be silently dropped or
	// degraded into something else server-side — the agent must know so it can
	// re-render the choices as numbered text. Surface this as an error so
	// dispatch tells the agent (no send), matching tool-error surfacing.
	if out.Poll != nil && !c.Polls {
		return nil, nil,
			[]c3types.Alteration{{
				Kind:   "poll_rejected",
				Detail: "poll dropped: channel does not support polls",
			}},
			fmt.Errorf("polls are not supported on this channel — render the choices as numbered text in a normal reply instead")
	}

	// Structural poll validation (pure, channel-neutral). The single source of
	// truth for poll shape lives here — the broker dispatch and the telegram
	// channel must NOT duplicate these checks. Option-count bounds are C3
	// POLICY, not Telegram API facts (the live API is more permissive). A
	// non-nil error is a HARD REJECTION (no send). Quiz degradations (e.g.
	// clearing MultipleAnswers) are recorded on a COPY of the spec so the
	// caller's PollSpec is never mutated; the validated poll is emitted below.
	pollSpec := out.Poll
	if out.Poll != nil {
		copySpec := *out.Poll
		validated, pollNotes, pollAlts, perr := validatePoll(&copySpec)
		if perr != nil {
			return nil, nil,
				[]c3types.Alteration{{
					Kind:   "poll_rejected",
					Detail: perr.Error(),
				}},
				perr
		}
		pollSpec = validated
		notes = append(notes, pollNotes...)
		alts = append(alts, pollAlts...)
	}

	// --- Inline-keyboard support-or-degrade (neutral) --------------------
	// Buttons ride the FIRST emitted part (the message the agent is sending).
	// This is the pure NEUTRAL decision only: keep the keyboard when the channel
	// advertises inline-keyboard support, otherwise DROP it with a note +
	// Alteration (a graceful degradation, NOT a hard error — a leaner channel
	// just loses the buttons). Any channel-specific limit (callback_data byte
	// ceiling, max rows/buttons) is a channel fact and is validated in-package
	// (internal/channel/telegram), never with a literal here.
	buttons := out.Buttons
	if len(buttons) > 0 && !c.InlineKeyboards {
		notes = append(notes, "buttons are not supported on this channel — the inline keyboard was dropped")
		alts = append(alts, c3types.Alteration{
			Kind:   "buttons_dropped",
			Detail: "inline keyboard dropped (channel InlineKeyboards=false)",
		})
		buttons = nil
	}

	// --- Markup degradation ----------------------------------------------
	markup := out.Markup
	if !c.RichText && markup != c3types.MarkupNone {
		notes = append(notes, "rich text is not supported on this channel — markdown was sent as plain text")
		alts = append(alts, c3types.Alteration{
			Kind:   "markup_degraded",
			Detail: fmt.Sprintf("markup %q downgraded to none (channel RichText=false)", string(markup)),
		})
		markup = c3types.MarkupNone
	}

	// --- Text chunking (construct-aware, UTF-16 measured) ----------------
	// The chunker measures the SOURCE text. The channel may render the source
	// (e.g. markdown→HTML) before sending, which EXPANDS it, so a source chunk
	// that fits the hard wire limit can exceed it once rendered. To leave room
	// for that expansion the gate splits at MaxMessageRunesSource (a conservative
	// budget BELOW the hard MaxMessageRunes wire limit) when the channel
	// advertises one. When it is unset (0) — a channel with no headroom or whose
	// rendering does not expand the source — the gate falls back to the hard
	// MaxMessageRunes, byte-identical to the pre-headroom behavior. Either way
	// MaxMessageRunes (or the budget) <= 0 means "no advertised limit" — emit a
	// single part. No raw numeric limit appears here: both numbers come from the
	// channel manifest (the no-leak rule — see internal/archguard).
	limit := c.MaxMessageRunes
	if c.MaxMessageRunesSource > 0 {
		limit = c.MaxMessageRunesSource
	}
	var textParts []string
	if limit <= 0 || utf16Len(out.Text) <= limit {
		textParts = []string{out.Text}
	} else {
		var chunkAlts []c3types.Alteration
		textParts, chunkAlts = chunkMarkdown(out.Text, limit)
		if len(textParts) > 1 {
			notes = append(notes, fmt.Sprintf("reply was split into %d messages (edits/replies reference the first)", len(textParts)))
		}
		alts = append(alts, chunkAlts...)
		for _, a := range chunkAlts {
			if a.Kind == "hard_split" {
				notes = append(notes, "a single construct exceeded the message limit and was hard-split — formatting may be affected")
				break
			}
		}
	}

	// --- Media degradation (P3, data-driven) -----------------------------
	// For each media item, if its Kind is not in the channel's MediaKinds set,
	// demote it (photo→file when the channel can't compress but can send an
	// original file) or drop it (with a note + Alteration). For Telegram all
	// kinds are supported so this is a no-op; it is generic so a leaner channel
	// degrades correctly.
	mediaItems := make([]c3types.MediaItem, 0, len(out.Media))
	for _, m := range out.Media {
		if mediaKindSupported(c, m.Kind) {
			mediaItems = append(mediaItems, m)
			continue
		}
		// Unsupported kind. The only sanctioned demotion is photo→file: a
		// compressed-photo preview can fall back to the original-file send.
		if m.Kind == c3types.MediaPhoto && !c.CompressedPhoto && c.OriginalFile &&
			mediaKindSupported(c, c3types.MediaFile) {
			demoted := m
			demoted.Kind = c3types.MediaFile
			mediaItems = append(mediaItems, demoted)
			notes = append(notes, "a photo was sent as a file (this channel cannot send a compressed photo preview)")
			alts = append(alts, c3types.Alteration{
				Kind:   "media_demoted",
				Detail: "photo demoted to file (channel CompressedPhoto=false, OriginalFile=true)",
			})
			continue
		}
		// No safe demotion — drop it so the agent knows.
		notes = append(notes, fmt.Sprintf("a %q media item was dropped: this channel cannot send it", string(m.Kind)))
		alts = append(alts, c3types.Alteration{
			Kind:   "media_dropped",
			Detail: fmt.Sprintf("media kind %q unsupported and not demotable (channel MediaKinds=%v)", string(m.Kind), c.MediaKinds),
		})
	}

	// --- Emit parts ------------------------------------------------------
	// Layout: [text chunks...] ++ [one part per media item] ++ [poll part].
	// Each part carries AT MOST ONE of {text, single media item, poll}. ReplyTo
	// rides only the FIRST part overall.
	parts = make([]c3types.Outbound, 0, len(textParts)+len(mediaItems)+1)
	// Suppress empty text parts ONLY when there is other content (media/poll) to
	// carry the reply — otherwise an empty text part would send a bogus empty
	// message ahead of the media. When text is the only content, an empty part is
	// preserved so dispatch surfaces the natural "empty message" error as before.
	hasOtherContent := len(mediaItems) > 0 || out.Poll != nil
	for _, t := range textParts {
		if t == "" && hasOtherContent {
			continue
		}
		parts = append(parts, c3types.Outbound{
			Channel: out.Channel,
			ChatID:  out.ChatID,
			TopicID: out.TopicID,
			Text:    t,
			Markup:  markup,
		})
	}
	for i := range mediaItems {
		parts = append(parts, c3types.Outbound{
			Channel: out.Channel,
			ChatID:  out.ChatID,
			TopicID: out.TopicID,
			Markup:  markup,
			Media:   []c3types.MediaItem{mediaItems[i]},
		})
	}
	if out.Poll != nil {
		parts = append(parts, c3types.Outbound{
			Channel: out.Channel,
			ChatID:  out.ChatID,
			TopicID: out.TopicID,
			Poll:    pollSpec,
		})
	}
	// ReplyTo rides only the FIRST part overall (text if any, else first media,
	// else the poll). textParts always has >=1 element ("" when Text empty), so
	// parts is non-empty whenever there is any content.
	if out.ReplyTo != nil && len(parts) > 0 {
		parts[0].ReplyTo = out.ReplyTo
	}
	// The inline keyboard (if kept) rides the FIRST part overall too — it attaches
	// to the message the agent is sending. Dropped on an unsupported channel above.
	if len(buttons) > 0 && len(parts) > 0 {
		parts[0].Buttons = buttons
	}
	return parts, notes, alts, nil
}

// C3 poll-policy bounds. These are C3 POLICY, not Telegram API facts — the live
// API is more permissive (it now allows a 1-option poll and caps options higher
// than 10). C3 deliberately requires 2-10 options because a 1-option poll is
// meaningless for a relay. The 300/200 limits match both rc.34 and the live API.
const (
	minPollOptions       = 2
	maxPollOptions       = 10
	maxPollQuestionRunes = 300
	maxPollExplainRunes  = 200
)

// validatePoll runs the pure, channel-neutral structural validation for a poll
// and returns the (possibly degraded) spec, agent-facing notes, structured
// alterations, and a hard-rejection error. It is the SINGLE source of truth for
// poll shape: neither the broker dispatch nor the telegram channel re-checks
// these. The caller passes a COPY (mutations like clearing MultipleAnswers for a
// quiz must not touch the caller's PollSpec).
//
// Policy vs API: option-count bounds are C3 policy (rejection messages say so);
// quiz requires a correct option (Telegram 400s otherwise); explanation length
// is sane. open_period/close_date are only checked for mutual exclusivity — the
// gate does NOT impose a 5-600 range (the live API allows far higher and the
// pinned lib enforces nothing, so a hard range here would reject valid polls).
func validatePoll(spec *c3types.PollSpec) (*c3types.PollSpec, []string, []c3types.Alteration, error) {
	var notes []string
	var alts []c3types.Alteration

	if len(spec.Options) < minPollOptions {
		return nil, nil, nil, fmt.Errorf("a poll needs at least %d options (C3 policy); got %d", minPollOptions, len(spec.Options))
	}
	if len(spec.Options) > maxPollOptions {
		return nil, nil, nil, fmt.Errorf("a poll allows at most %d options (C3 policy); got %d", maxPollOptions, len(spec.Options))
	}
	if runeLen(spec.Question) > maxPollQuestionRunes {
		return nil, nil, nil, fmt.Errorf("poll question is %d chars, over the %d-char limit", runeLen(spec.Question), maxPollQuestionRunes)
	}

	if spec.Kind == c3types.PollQuiz {
		if spec.CorrectOption == nil {
			return nil, nil, nil, fmt.Errorf("a quiz poll requires correct_option (the 0-based index of the right answer)")
		}
		if *spec.CorrectOption < 0 || *spec.CorrectOption >= len(spec.Options) {
			return nil, nil, nil, fmt.Errorf("correct_option %d is out of range; must be 0..%d", *spec.CorrectOption, len(spec.Options)-1)
		}
		if runeLen(spec.Explanation) > maxPollExplainRunes {
			return nil, nil, nil, fmt.Errorf("poll explanation is %d chars, over the %d-char limit", runeLen(spec.Explanation), maxPollExplainRunes)
		}
		// A quiz poll ignores multiple answers — match that by clearing it and
		// telling the agent (degradation, not a rejection).
		if spec.MultipleAnswers {
			spec.MultipleAnswers = false
			notes = append(notes, "multiple answers are ignored for a quiz poll — the setting was cleared")
			alts = append(alts, c3types.Alteration{
				Kind:   "poll_multiple_ignored_quiz",
				Detail: "MultipleAnswers cleared (ignored for quiz polls)",
			})
		}
	}

	// open_period and close_date both auto-close the poll; Telegram accepts only
	// one. Enforce mutual exclusivity ONLY — no range check (see doc comment).
	if spec.OpenPeriodSec != 0 && spec.CloseDateUnix != 0 {
		return nil, nil, nil, fmt.Errorf("open_period and close_date are mutually exclusive — set at most one")
	}

	return spec, notes, alts, nil
}

// mediaKindSupported reports whether the channel's manifest lists kind in its
// sendable MediaKinds.
func mediaKindSupported(c c3types.Capabilities, kind c3types.MediaKind) bool {
	for _, k := range c.MediaKinds {
		if k == kind {
			return true
		}
	}
	return false
}

// utf16Len returns the length of s in UTF-16 code units — the unit Telegram
// counts a message length in. A BMP rune is 1 unit; an astral rune (e.g. most
// emoji) is a surrogate pair = 2 units. We measure on []rune→utf16.Encode so
// the count matches Telegram's, not Go's byte or rune count.
func utf16Len(s string) int {
	return len(utf16.Encode([]rune(s)))
}

// runeLen returns the number of Unicode code points in s. Poll question /
// explanation limits are stated by Telegram in characters (code points), not
// UTF-16 units, so this is the right measure for those checks.
func runeLen(s string) int {
	return len([]rune(s))
}
