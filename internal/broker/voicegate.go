package broker

import (
	"errors"
	"fmt"
	"strconv"

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
func (b *Broker) voiceFetchRefusal(in *c3types.Inbound) (agentText, notice string, refuse bool) {
	ch, err := b.Channel(in.Channel)
	if err != nil {
		return "", "", false
	}
	sizer, ok := ch.(attachmentSizer)
	fileID := voiceAttachmentFileID(in)
	if !ok || fileID == "" {
		return "", "", false // nothing to ask, or nobody to ask
	}
	if _, perr := sizer.AttachmentSize(fileID); perr != nil {
		agent, human := fetchFailureTexts(perr, voiceAttachmentSize(in))
		return agent, human, true
	}
	return "", "", false
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
func (b *Broker) attachmentFetchRefusal(chanName, fileID string) string {
	ch, err := b.Channel(chanName)
	if err != nil {
		return ""
	}
	sizer, ok := ch.(attachmentSizer)
	if !ok || fileID == "" {
		return ""
	}
	if _, perr := sizer.AttachmentSize(fileID); perr != nil {
		agent, _ := fetchFailureTexts(perr, 0)
		return agent
	}
	return ""
}

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
	return fmt.Sprintf("[voice too big: %s%s. Unrecoverable via the bot API — the audio was NOT saved, and download_attachment / retranscribe will fail identically, so do not call them. Recovery: ask the sender to share the SAME file another way (a local file drop), or to SPLIT that same file and resend the parts.]",
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
	return fmt.Sprintf("[voice download failed: %s. STT was NOT run — transcription begins with this same fetch, so it would have failed the same way. The audio was not transcribed. If the cause above is transient, retranscribe with the same file_id; if it persists, ask the sender to share the file another way.]",
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

// voiceAttachmentSize returns the size the channel reported for in's voice
// attachment. 0 means NOT STATED — Telegram marks Voice.file_size Optional
// (https://core.telegram.org/bots/api#voice) — and nothing is decided from it;
// it only makes a message concrete when it happens to be there.
func voiceAttachmentSize(in *c3types.Inbound) int64 {
	if len(in.Attachments) == 0 {
		return 0
	}
	return in.Attachments[0].Size
}

// voiceAttachmentFileID returns the file_id of in's voice attachment, or "" when
// there is nothing to ask about.
func voiceAttachmentFileID(in *c3types.Inbound) string {
	if len(in.Attachments) == 0 {
		return ""
	}
	return in.Attachments[0].FileID
}
