package broker

import (
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
// The ask is getFile: it is exactly the call the STT handler makes first, it
// transfers no body, and it answers with either a file path or the server's
// refusal. So the probe IS the attempt, run once up front instead of twice
// inside a chain that cannot report what went wrong.

// attachmentSizer is the OPTIONAL channel capability that asks the transport
// whether it will hand a file over, and how big it says the file is, without
// downloading it. Channels that cannot ask do not implement it and are never
// gated.
type attachmentSizer interface {
	AttachmentSize(fileID string) (int64, error)
}

// voiceFetchRefusal asks the channel whether in's voice attachment can be
// fetched at all, and returns the agent-facing marker and human-facing notice
// when it cannot. refuse=false means "run STT".
//
// Three outcomes, and none of them consults a number of C3's own:
//
//   - The server will serve the file ⇒ STT runs, whatever the size. A limit C3
//     invented could only refuse a file that was about to work.
//   - The server refuses it as too big ⇒ permanent. STT never runs, and both
//     surfaces say so in the server's own terms.
//   - The ask fails some other way ⇒ STT still does not run: the transcription
//     chain opens with this same fetch, so it would fail identically and report
//     a generic "transcription failed" over the top of a real error. The real
//     error is passed through instead.
func (b *Broker) voiceFetchRefusal(chanName string, att c3types.Attachment) (agentText, notice string, refuse, retryable bool, size int64) {
	ch, err := b.Channel(chanName)
	if err != nil {
		return "", "", false, false, 0
	}
	sizer, ok := ch.(attachmentSizer)
	if !ok || att.FileID == "" {
		return "", "", false, false, 0 // nothing to ask, or nobody to ask
	}
	sz, perr := sizer.AttachmentSize(att.FileID)
	if perr != nil {
		agent, human := fetchFailureTexts(perr, att.Size)
		// Retryable ONLY for a transient network condition — never a too-big / bad-file
		// refusal, which retrying can never fix (P0-2, fail-closed classification).
		retry := !errors.Is(perr, channel.ErrAttachmentTooLarge) && isNetworkTransient(perr.Error())
		return agent, human, true, retry, 0
	}
	// The probe succeeded: return the authoritative size so the live path can scale
	// the STT budget even when Telegram omitted file_size (att.Size == 0) — F8.
	return "", "", false, false, sz
}

// sttFetchFailedPrefix mirrors stt.FetchFailedPrefix. It is duplicated rather
// than imported for the same reason isSTTFailureMarker is: the broker does not
// depend on the STT builtin, so the marker vocabulary crosses that boundary as a
// string. Keep the two in step.
const sttFetchFailedPrefix = "[STT FETCH FAILED: "

// sttFetchFailure reports the handler's own fetch error when a transcript
// stand-in carries one. The STT handler performs its own getFile — the broker's
// preflight cannot speak for it (a failover advance or plain TOCTOU between the
// two calls), so when the handler's fetch is the thing that failed, its concrete
// cause must survive into the scheduler's terminal recovery text.
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

// attachmentFetchRefusal is the file_id-only entry point (retranscribe carries
// no inbound), returning the agent-facing text for a file the server will not
// hand over, or "" when it will. Same rule, same single source of truth.
// It also returns the server-stated size on success (0 on refusal / unknown), so
// the caller can size-scale the STT budget from the SAME getFile probe rather than
// issuing a second one (P1-3).
// It also returns the server-stated size on success (0 on refusal) and whether a
// refusal is a TRANSIENT network condition worth re-parking a retry for (F4) — never
// for a too-big / bad-file refusal, which retrying cannot fix.
func (b *Broker) attachmentFetchRefusal(chanName, fileID string) (refusal string, size int64, retryable bool) {
	ch, err := b.Channel(chanName)
	if err != nil {
		return "", 0, false
	}
	sizer, ok := ch.(attachmentSizer)
	if !ok || fileID == "" {
		return "", 0, false
	}
	sz, perr := sizer.AttachmentSize(fileID)
	if perr != nil {
		agent, _ := fetchFailureTexts(perr, 0)
		return agent, 0, !errors.Is(perr, channel.ErrAttachmentTooLarge) && isNetworkTransient(perr.Error())
	}
	return "", sz, false
}

// voiceCachedLocally reports whether the channel already has this file_id's audio in
// its local cache (so retranscribe can skip the network preflight and the handler
// can reuse the bytes — F11). Best-effort; false when the channel has no accessor.
func (b *Broker) voiceCachedLocally(chanName, fileID string) bool {
	return b.voiceCachedPath(chanName, fileID) != ""
}

func (b *Broker) voiceCachedPath(chanName, fileID string) string {
	ch, err := b.Channel(chanName)
	if err != nil {
		return ""
	}
	cp, ok := ch.(interface{ CachedVoicePath(string) string })
	if !ok {
		return ""
	}
	return cp.CachedVoicePath(fileID)
}

// The openings C3 uses when it authors a voice message's whole agent-surface
// text. They are named once, here, so wholeTextIsC3sVoiceWrite recognizes
// exactly what the writers below produce — if a writer's wording changes, its
// opening travels with it.
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
	return fmt.Sprintf("⚠️ Couldn't download this voice note%s — it's over the bot server's size limit. Send the same file another way (file drop), or split that same file and resend it.", size)
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
	return fmt.Sprintf("⚠️ Couldn't download this voice note, so it wasn't transcribed: %s", cause.Error())
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
