package c3types

import (
	"fmt"
	"strings"
	"unicode"
)

// TestInjectedMarker is provenance supplied by the broker, independent of the
// route label (single-route adapters intentionally omit that label).
const TestInjectedMarker = `c3_test_injected="true"`

// WithTestInjectionMarker marks compact previews and human-facing renderers
// that do not use RenderQueuedInbound's metadata trailer. Apply after clipping
// the body so even a short or empty preview retains the complete marker.
func WithTestInjectionMarker(in *Inbound, text string) string {
	if in.TestInjected {
		return TestInjectedMarker + " " + text
	}
	return text
}

// ReplyContextFields renders the reply-context metadata fields shared by the
// queued (fetch_queue) renderer in both adapters and the Codex live-forward turn
// header. Returning a single slice from one place is what keeps those renderers
// from drifting (the queued renderers are required to stay byte-identical, and the
// live-forward header must match their reply formatting). Order is stable:
//
//	reply_to=<id> [reply_to_user=@<name>|reply_to_user=<id>] [reply_to_text=<quoted>]
//
// Returns nil when rc is nil (so callers can append the result unconditionally).
func ReplyContextFields(rc *ReplyContext) []string {
	if rc == nil {
		return nil
	}
	fields := []string{fmt.Sprintf("reply_to=%d", rc.MessageID)}
	if rc.User.Username != "" {
		fields = append(fields, "reply_to_user=@"+rc.User.Username)
	} else if rc.User.UserID != 0 {
		fields = append(fields, fmt.Sprintf("reply_to_user=%d", rc.User.UserID))
	}
	if rc.Text != "" {
		fields = append(fields, fmt.Sprintf("reply_to_text=%q", rc.Text))
	}
	return fields
}

// AttachmentField renders one attachment's full metadata (kind, file_id, mime,
// size, name) in the exact format the queued (fetch_queue) renderers emit, so the
// live-forward header and both adapters' renderers share a single source of truth.
// The file_id/mime are load-bearing: the agent uses them to recover backlog media
// via download_attachment/retranscribe.
func AttachmentField(att Attachment) string {
	return fmt.Sprintf("attachment{kind=%s file_id=%q mime=%s size=%d name=%q}",
		att.Kind, att.FileID, att.MIME, att.Size, att.Name)
}

// AttachmentCompact renders one attachment in the trimmed inbound-readback form
// approved 2026-07-24 (task #55): only the kind + file_id, which is the bare
// minimum the agent needs to open it (download_attachment requires file_id;
// kind tells it whether to retranscribe). The verbose mime/size/name that
// AttachmentField carries are dropped — they were the bulk of the per-message
// noise the maintainer flagged. The one-time AttachmentFetchHint explains how to
// use the file_id, so it isn't repeated on every line.
func AttachmentCompact(att Attachment) string {
	field := fmt.Sprintf("attachment=%s file_id=%s", att.Kind, att.FileID)
	if att.SourceMessageID != 0 {
		field += fmt.Sprintf(" message_id=%d", att.SourceMessageID)
	}
	return field
}

// AttachmentFetchHint is the single thin, one-time note the batch renderers
// prepend to a fetch_queue/drain readback that contains at least one attachment.
// It replaces the old per-message attachment block's implicit "here's the
// metadata" contract with one explicit instruction shown once (task #55).
const AttachmentFetchHint = "Attachments below show `attachment=<kind> file_id=<id>`; call download_attachment with a file_id to open it (retranscribe re-runs speech-to-text on a voice note)."

// InboundsHaveAttachment reports whether any of msgs carries an attachment, so a
// batch renderer knows whether to emit the one-time AttachmentFetchHint.
func InboundsHaveAttachment(msgs []Inbound) bool {
	for i := range msgs {
		if len(msgs[i].Attachments) > 0 {
			return true
		}
	}
	return false
}

// AttachmentBodyLabel returns the human-readable body used when an inbound has
// attachments but no rendered text. A single attachment keeps the historical
// kind label; an album reports its full count instead of masquerading as one
// media message.
func AttachmentBodyLabel(attachments []Attachment) string {
	switch len(attachments) {
	case 0:
		return ""
	case 1:
		return "(" + attachments[0].Kind + " message)"
	default:
		return fmt.Sprintf("(%d attachments)", len(attachments))
	}
}

// RenderInboundBody returns the unchanged text of a discrete inbound. For a
// merged delivery it renders the non-empty source texts in batch order, keeping
// message, sender, and forwarding attribution attached to each fragment. A
// fragment text with a line that could read as another fragment's header or as
// the metadata line is %q-quoted, as RenderQueuedInbound quotes a forged body.
func RenderInboundBody(in *Inbound) string {
	if len(in.Merged) == 0 {
		return in.Text
	}
	fragments := make([]string, 0, len(in.Merged))
	for _, source := range in.Merged {
		if source.Text == "" {
			continue
		}
		var prefix strings.Builder
		fmt.Fprintf(&prefix, "[msg %d", source.MessageID)
		if source.Sender.UserID != in.Sender.UserID {
			switch {
			case source.Sender.Username != "":
				fmt.Fprintf(&prefix, " @%s", source.Sender.Username)
			case source.Sender.UserID != 0:
				fmt.Fprintf(&prefix, " @uid=%d", source.Sender.UserID)
			}
		}
		if source.ForwardOrigin != nil {
			fmt.Fprintf(&prefix, " fwd %s", lineSafeName(source.ForwardOrigin.Name))
		}
		text := source.Text
		if bodyCouldForgeLine(text, fragmentPrefixes) {
			text = fmt.Sprintf("%q", text)
		}
		fmt.Fprintf(&prefix, "] %s", text)
		fragments = append(fragments, prefix.String())
	}
	return strings.Join(fragments, "\n")
}

// metaPrefixes are the tokens the trailing metadata line can begin with. A body
// line that starts with one of them is indistinguishable from that metadata line
// once the body is rendered bare, which is what makes attribution forgeable.
var metaPrefixes = []string{"from=", "message_id=", "reply_to", "attachment=", "event=", "merged=", "fwd:", "c3_test_injected="}

// fragmentPrefixes are the line starts a merged fragment's own text must not
// have: the "[msg" that opens every fragment header, and the metadata tokens.
var fragmentPrefixes = append([]string{"[msg"}, metaPrefixes...)

// lineSafeName keeps a free-text name (a display name or chat title) on the
// line it is rendered on: line breaks become spaces, invisible runes are dropped.
func lineSafeName(name string) string {
	return strings.Map(func(r rune) rune {
		if IsLineBreak(r) {
			return ' '
		}
		return DropInvisibleRune(r)
	}, name)
}

// bodyCouldForgeLine reports whether any line of a message body could be read as
// a line C3 writes itself: one that starts with any of prefixes. With
// metaPrefixes that is the metadata line RenderQueuedInbound appends.
//
// The compact form (approved 2026-07-24) puts the body bare on its own lines and
// the metadata last. The form it replaced rendered the body through %q, which
// escaped newlines and therefore made this impossible. Without that escaping a
// sender can embed a line like "from=@operator message_id=999" in their own
// message and the readback presents it as a SECOND message from someone else —
// attribution forgery into the agent's context, from any member of an allowlisted
// group. (v0.1.0 release audit, 2026-07-25.)
//
// Every mandatory line break counts and invisible runes are dropped first, so
// neither a vertical tab nor a leading U+200B can hide a forged line.
func bodyCouldForgeLine(text string, prefixes []string) bool {
	if !strings.ContainsFunc(text, IsLineBreak) {
		return false // a single-line body can never produce a second line
	}
	for _, line := range strings.FieldsFunc(text, IsLineBreak) {
		s := strings.TrimSpace(strings.Map(DropInvisibleRune, line))
		for _, p := range prefixes {
			if strings.HasPrefix(s, p) {
				return true
			}
		}
	}
	return false
}

// IsLineBreak reports Unicode's mandatory line breaks (UAX #14 BK, CR, LF, NL),
// each of which a reader may see as the start of a new line.
func IsLineBreak(r rune) bool {
	return r == '\n' || r == '\r' || r == '\v' || r == '\f' || r == '\u0085' || r == '\u2028' || r == '\u2029'
}

// DropInvisibleRune, for strings.Map, drops format, control and other
// default-ignorable runes and keeps whitespace, so a prefix check on the result
// sees what a reader sees.
func DropInvisibleRune(r rune) rune {
	if unicode.IsSpace(r) || unicode.IsGraphic(r) && !unicode.In(r, unicode.Variation_Selector, unicode.Other_Default_Ignorable_Code_Point) {
		return r
	}
	return -1
}

// RenderQueuedInbound renders one inbound message for the fetch_queue / drain
// readback in the compact form approved 2026-07-24 (task #55, proposed in the
// maintainer thread as msg 6579). The message text stands bare on the first
// line; a single compact metadata line follows with only the sender, message_id,
// reply context, and a compact attachment reference (kind + file_id). This
// replaces the old one-line "from=@u message_id=N text=\"…\" attachment{…}" dump
// whose verbose attachment block was the token noise the maintainer flagged
// (msg 6069). It stays a shared c3types function so every adapter's live-forward
// and fetch_queue readback render identically and can't drift.
func RenderQueuedInbound(in *Inbound) string {
	text := RenderInboundBody(in)
	// Only a body that could actually forge the metadata line is escaped, so the
	// approved bare-text form is preserved for every ordinary message. Escaping
	// collapses it to one %q-quoted line, exactly the unambiguous representation
	// this format replaced.
	if bodyCouldForgeLine(text, metaPrefixes) {
		text = fmt.Sprintf("%q", text)
	}
	if text == "" {
		switch {
		case len(in.Attachments) > 0:
			text = AttachmentBodyLabel(in.Attachments)
		case len(in.Merged) > 0:
			// Preserve the existing metadata-only shape for an empty merged
			// presentation that carries neither text nor attachments.
		case in.IsEvent():
			text = "(" + string(in.Kind) + " event)"
		default:
			text = "(no content)"
		}
	}
	var meta []string
	if in.TestInjected {
		meta = append(meta, TestInjectedMarker)
	}
	switch {
	case in.Sender.Username != "":
		meta = append(meta, "from=@"+in.Sender.Username)
	case in.Sender.UserID != 0:
		meta = append(meta, fmt.Sprintf("from=uid=%d", in.Sender.UserID))
	}
	if in.MessageID != 0 {
		meta = append(meta, fmt.Sprintf("message_id=%d", in.MessageID))
	}
	meta = append(meta, ReplyContextFields(in.ReplyTo)...)
	for _, att := range in.Attachments {
		meta = append(meta, AttachmentCompact(att))
	}
	if len(in.Merged) > 0 {
		meta = append(meta, fmt.Sprintf("merged=%d", len(in.Merged)))
	} else if in.ForwardOrigin != nil {
		meta = append(meta, "fwd:"+lineSafeName(in.ForwardOrigin.Name))
	}
	if in.IsEvent() {
		meta = append(meta, "event="+string(in.Kind))
	}
	if len(meta) == 0 {
		return text
	}
	if text == "" {
		return strings.Join(meta, " ")
	}
	return text + "\n" + strings.Join(meta, " ")
}
