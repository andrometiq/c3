package broker

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// Voice size gate — 2026-07-27 incident (local-notes/INCIDENT-2026-07-27-voice-
// limit-ordering.md, Ask #1). A 21,226,288-byte voice note ran the whole STT
// chain twice and surfaced the generic
//
//	⚠️ [voice transcription failed: error] The audio is saved and recoverable —
//	the user does not need to resend…
//
// which was false on every clause: nothing was saved (the download never
// succeeded), nothing is recoverable through the bot API, and resending in
// smaller parts is the ONLY fix. Telegram's getFile ceiling is a hard 20MB
// ("For the moment, bots can download files of up to 20MB in size" —
// https://core.telegram.org/bots/api#getfile); over it the bot gets
// `Bad Request: file is too big` and no bytes, ever.
//
// C3 already knows the size before it tries: Telegram puts file_size on the
// incoming voice object (internal/channel/telegram/inbound.go), so the gate
// costs nothing and runs BEFORE any getFile/STT attempt.
//
// The ceiling itself is read from the channel's capability manifest
// (Capabilities().Inbound.MaxDownloadBytes), never hardcoded here — the raw
// Bot-API number stays inside the telegram package (the no-leak rule, R7).

// voiceOverDownloadLimit reports whether an attachment of size bytes is beyond
// what chanName's bot can download, and the ceiling it was measured against.
// A channel that declares no ceiling (0) never gates — silence in the manifest
// must not become a refusal to transcribe.
func (b *Broker) voiceOverDownloadLimit(chanName string, size int64) (limit int64, over bool) {
	ch, err := b.Channel(chanName)
	if err != nil {
		return 0, false
	}
	limit = ch.Capabilities().Inbound.MaxDownloadBytes
	return limit, limit > 0 && size > limit
}

// attachmentSizer is the OPTIONAL channel capability that answers "how big is
// this attachment?" without downloading it. Channels that cannot answer simply
// do not implement it and the size gate is skipped for them.
type attachmentSizer interface {
	AttachmentSize(fileID string) (int64, error)
}

// attachmentTooBigRefusal returns a ready-to-surface refusal when fileID is
// known to be too big for chanName's bot to fetch, or "" when it is fetchable,
// unknown, or the probe itself failed. This is the file_id-only entry point:
// retranscribe carries no size, so the size has to be asked for.
//
// A probe that fails for any OTHER reason returns "" — a flaky network must
// never turn into a refusal to transcribe.
func (b *Broker) attachmentTooBigRefusal(chanName, fileID string) string {
	ch, err := b.Channel(chanName)
	if err != nil {
		return ""
	}
	sizer, ok := ch.(attachmentSizer)
	if !ok {
		return ""
	}
	size, serr := sizer.AttachmentSize(fileID)
	if errors.Is(serr, channel.ErrAttachmentTooLarge) {
		// The transport refused before it could report a number (getFile itself
		// fails above the ceiling), so its own message carries the cause.
		return serr.Error() + " — unrecoverable via the bot API; ask the sender to resend it in shorter parts, or to drop the file to the agent directly"
	}
	if serr != nil {
		return ""
	}
	if limit := ch.Capabilities().Inbound.MaxDownloadBytes; limit > 0 && size > limit {
		return voiceTooBigAgentText(size, limit)
	}
	return ""
}

// mbString renders a byte count in MiB with ONE decimal place. Integer MB is
// useless at exactly the boundary this gate reports on: 21,226,288 bytes and a
// 20 MiB ceiling both floor to "20 MB", so "20 MB > 20 MB limit" reads as a
// contradiction to whoever has to act on it.
func mbString(bytes int64) string {
	return strconv.FormatFloat(float64(bytes)/(1024*1024), 'f', 1, 64) + " MB"
}

// voiceTooBigAgentText is the AGENT-facing stand-in for a voice note C3 refused
// to fetch. It deliberately shares no wording with sttFailureText: that text
// promises the audio is saved and retryable, and here every one of those
// promises is false. The distinct "[voice too big:" marker lets an agent tell
// the two apart at a glance, and the raw byte count removes any argument about
// rounding.
func voiceTooBigAgentText(size, limit int64) string {
	return fmt.Sprintf("[voice too big: %s (%d bytes) > %s bot download limit — unrecoverable via the bot API. The audio was NOT saved: download_attachment and retranscribe will fail the same way, so do not call them. Ask the sender to resend it in shorter parts, or to drop the file to the agent directly]",
		mbString(size), size, mbString(limit))
}

// voiceTooBigNotice is the HUMAN-facing notice echoed back into the chat, in
// place of the generic "couldn't transcribe that voice note — try again". It
// names the size, the limit, and the one action that actually works, so the
// sender can re-record immediately instead of waiting for the agent to diagnose
// it mid-dictation.
func voiceTooBigNotice(size, limit int64) string {
	return fmt.Sprintf("⚠️ This voice note is %s — over the %s bot download limit, so I can't fetch it at all. Please resend it in shorter parts (or drop the file to the agent directly).",
		mbString(size), mbString(limit))
}

// voiceAttachmentSize returns the size the channel reported for in's voice
// attachment. 0 means "not stated" — Telegram sets file_size on voice, but a
// channel that omits it must fall through to the normal STT path rather than be
// treated as over any limit.
func voiceAttachmentSize(in *c3types.Inbound) int64 {
	if len(in.Attachments) == 0 {
		return 0
	}
	return in.Attachments[0].Size
}
