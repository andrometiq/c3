package broker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// Voice fetchability gate — 2026-07-27 incident
// (local-notes/INCIDENT-2026-07-27-voice-limit-ordering.md, Ask #1). A
// 21,226,288-byte voice note ran the whole STT chain twice (two HTTP 400s) and
// surfaced the generic
//
//	⚠️ [voice transcription failed: error] The audio is saved and recoverable —
//	the user does not need to resend…
//
// which was false on every clause: nothing was saved (the download never
// succeeded), nothing is recoverable through the bot API, and sharing the file
// another way is the only fix. Masking the real cause was the whole defect.
//
// THE RULE (maintainer, 2026-07-29): C3 holds no size limit of its own and
// compares nothing against one. The bot server is the sole authority on what it
// will hand over — api.telegram.org's current number is not the Bot API's, a
// self-hosted server has none, and either can change — so a baked-in ceiling is
// a false refusal waiting to happen in both directions. C3 ASKS, and reports
// what it hears.
//
// The ask is the fetch itself: the broker fetches the audio once over the
// channel's own transport and hands the STT handler that local file, so the
// handler never fetches and a refusal is reported in the server's terms before
// STT runs.

// voiceFetcher is the OPTIONAL channel capability that fetches a voice note into
// a fresh attempt-owned local file the caller removes. Channels that cannot
// fetch do not implement it, and their voice runs as before.
type voiceFetcher interface {
	FetchVoice(ctx context.Context, fileID string) (path string, size int64, err error)
}

// voiceFetchRefusal fetches att through fetcher. On success it returns the
// attempt-owned path and its size; otherwise refusal is the attempt's outcome.
//
// The channel's typed error is authoritative: only a
// *channel.AttachmentTransientError retries (parked for at least its After).
// Anything else, a size refusal included, is terminal: STT does not run, and
// both surfaces carry the server's own words.
func voiceFetchRefusal(ctx context.Context, fetcher voiceFetcher, att c3types.Attachment) (path string, size int64, refusal *voiceAttemptResult) {
	path, size, err := fetcher.FetchVoice(ctx, att.FileID)
	if err == nil {
		return path, size, nil
	}
	agent, human := fetchFailureTexts(err, att.Size)
	var transient *channel.AttachmentTransientError
	if errors.As(err, &transient) {
		return "", 0, &voiceAttemptResult{transient: true, retryAfter: transient.After, detail: agent}
	}
	return "", 0, &voiceAttemptResult{segmentText: agent, notice: human, detail: agent}
}

// sttFetchFailedPrefix mirrors stt.FetchFailedPrefix. It is duplicated rather
// than imported for the same reason isSTTFailureMarker is: the broker does not
// depend on the STT builtin, so the marker vocabulary crosses that boundary as a
// string. Keep the two in step.
const sttFetchFailedPrefix = "[STT FETCH FAILED: "

// sttFetchFailure reports the handler's own fetch error when a transcript
// stand-in carries one. Only a handler given no local file fetches (a channel
// without voiceFetcher); when that fetch is what failed, its concrete cause must
// survive into the scheduler's terminal recovery text.
func sttFetchFailure(transcript string) (string, bool) {
	detail, ok := strings.CutPrefix(transcript, sttFetchFailedPrefix)
	if !ok {
		return "", false
	}
	return strings.TrimSuffix(detail, "]"), true
}

// voiceAttachments returns every voice attachment on in, in arrival order.
//
// Position is NOT identity: a rich message decodes its blocks in order
// (telegram/richdecode.go), so a photo block followed by a voice_note block
// yields attachments [photo, voice]. Looking only at Attachments[0] meant that
// message never entered the voice path at all — no fetch, no transcription, no
// refusal marker, no notice: the audio simply vanished (Codex review 2,
// finding 2). Telegram allows many blocks per rich message, so "the voice one"
// is a filter, never an index.
func voiceAttachments(in *c3types.Inbound) []c3types.Attachment {
	var out []c3types.Attachment
	for _, a := range in.Attachments {
		if a.Kind == "voice" {
			out = append(out, a)
		}
	}
	return out
}

// fetchFailureTexts renders the two surfaces for a fetch the server refused.
// statedSize is the size the ORIGINAL update carried (0 when Telegram omitted
// it — Voice.file_size is Optional), used only to make the message concrete.
func fetchFailureTexts(cause error, statedSize int64) (agentText, notice string) {
	if errors.Is(cause, channel.ErrAttachmentTooLarge) {
		return voiceTooBigAgentText(cause, statedSize), voiceTooBigNotice(statedSize)
	}
	return voiceFetchFailedAgentText(cause), voiceFetchFailedNotice(cause)
}

// voiceCachedPath returns the channel's retained local copy of this file_id's
// audio, or "". The STT failure text names it (and claims the audio is saved
// only when it exists), and a channel without voiceFetcher sizes the STT budget
// from it. A channel that retains its own audio (channel.LocalAudioProvider,
// e.g. web) answers from that store. Best-effort; "" when the channel has no
// accessor.
func (b *Broker) voiceCachedPath(chanName, fileID string) string {
	ch, err := b.Channel(chanName)
	if err != nil {
		return ""
	}
	if cp, ok := ch.(interface{ CachedVoicePath(string) string }); ok {
		return cp.CachedVoicePath(fileID)
	}
	if provider, ok := ch.(channel.LocalAudioProvider); ok {
		if path, err := provider.LocalAudioPath(fileID); err == nil {
			return path
		}
	}
	return ""
}

// Stable openings for the agent-facing terminal voice outcomes.
const (
	sttFailureOpening       = "⚠️ [voice transcription failed:"
	voiceTooBigOpening      = "[voice too big:"
	voiceFetchFailedOpening = "[voice download failed:"
)

// mbString renders a byte count in MiB with ONE decimal place. Integer MB is
// useless at exactly the boundary these messages report on: 21,226,288 bytes and
// a 20 MiB ceiling both floor to "20 MB", so a reader is shown two identical
// numbers and told one exceeds the other.
func mbString(bytes int64) string {
	return strconv.FormatFloat(float64(bytes)/(1024*1024), 'f', 1, 64) + " MB"
}

// sizeSuffix describes a stated size, or nothing at all when the update did not
// carry one. Never guesses: an unstated size is simply not mentioned.
func sizeSuffix(statedSize int64) string {
	if statedSize <= 0 {
		return ""
	}
	return fmt.Sprintf(" (%s / %d bytes, as stated on the incoming message)", mbString(statedSize), statedSize)
}

// voiceTooBigAgentText is the AGENT-facing marker for a voice note the bot
// server refuses to hand over. It shares no wording with sttFailureText: that
// text promises the audio is saved and retryable, and here every one of those
// promises is false. The distinct "[voice too big:" prefix lets an agent tell
// the two apart at a glance.
//
// It SUGGESTS the two routes that keep the original audio — share that same file
// another way, or split it — and says nothing at all about re-recording. Whether
// to re-record is the sender's own call, and neither recommending nor forbidding
// it is C3's business (maintainer, 2026-07-29).
func voiceTooBigAgentText(cause error, statedSize int64) string {
	return fmt.Sprintf(voiceTooBigOpening+" %s%s. Unrecoverable via the bot API — the audio was NOT saved, and download_attachment / retranscribe will fail identically, so do not call them. Recovery: ask the sender to share the SAME file another way (a local file drop), or to SPLIT that same file and resend the parts.]",
		cause.Error(), sizeSuffix(statedSize))
}

// voiceTooBigNotice is the human-facing notice echoed into the chat, in place of
// the generic "couldn't transcribe that voice note — try again". Short, and it
// names the one thing that works.
func voiceTooBigNotice(statedSize int64) string {
	size := ""
	if statedSize > 0 {
		size = " (" + mbString(statedSize) + ")"
	}
	return fmt.Sprintf(c3types.VoiceDownloadFailureNoticePrefix+"%s — it's over the bot server's size limit. Send the same file another way (file drop), or split that same file and resend it.", size)
}

// voiceFetchFailedAgentText passes a non-size fetch failure through verbatim.
// The agent is told plainly that STT never ran and why, instead of being handed
// a transcription-failed message that names the wrong subsystem.
func voiceFetchFailedAgentText(cause error) string {
	return fmt.Sprintf(voiceFetchFailedOpening+" %s. STT was NOT run — transcription begins with this same fetch, so it would have failed the same way. The audio was not transcribed. If the cause above is transient, retranscribe with the same file_id; if it persists, ask the sender to share the file another way.]",
		cause.Error())
}

// voiceFetchFailedNotice is the human-facing form of the same passthrough.
func voiceFetchFailedNotice(cause error) string {
	return fmt.Sprintf(c3types.VoiceDownloadFailureNoticePrefix+", so it wasn't transcribed: %s", cause.Error())
}

// appendVoiceMarker joins the agent surface with a refusal marker. It APPENDS
// and never replaces: a caption is the sender's own words, and a rich-message
// voice block always arrives carrying its own "[voice_note]" text
// (telegram/richdecode.go). The earlier "only when the text is empty" rule meant
// either of those silently swallowed the refusal — the agent saw the caption and
// no word that the audio had not been fetched.
func appendVoiceMarker(existing, marker string) string {
	if existing == "" {
		return marker
	}
	return existing + "\n" + marker
}
