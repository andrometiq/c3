package c3types

const (
	// VoiceTranscriptionFailureNoticePrefix identifies a failed STT readback notice.
	VoiceTranscriptionFailureNoticePrefix = "⚠️ Couldn't transcribe that voice note"
	// VoiceDownloadFailureNoticePrefix identifies a failed voice-download readback notice.
	VoiceDownloadFailureNoticePrefix = "⚠️ Couldn't download this voice note"
	// VoiceNoSpeechNotice is a board voice note's readback when STT heard no speech.
	VoiceNoSpeechNotice = "🔇 No speech heard in that voice note."
	// VoiceSlowNotice tells the sender a voice note's transcription is slow and being retried.
	VoiceSlowNotice = "Got this voice note. Transcription is taking longer than usual; C3 is retrying."
)

// A voice note's status from C3, for a channel that shows it on the note
// itself (channel.VoiceStatusSender): heard comes as a readback, the others
// as a VoiceStatusArgs.
const (
	VoiceStatusHeard    = "heard"
	VoiceStatusNoSpeech = "no_speech"
	VoiceStatusFailed   = "failed"
	VoiceStatusSlow     = "slow" // still transcribing; a final status follows
)

// VoiceStatusArgs is a C3 notice about one voice note, sent only by the
// broker: Status is VoiceStatusNoSpeech, VoiceStatusFailed or VoiceStatusSlow,
// and Text is C3's notice for the sender.
type VoiceStatusArgs struct {
	ChatID    int64  `json:"ChatID"`
	TopicID   *int64 `json:"TopicID"`
	MessageID int64  `json:"MessageID"` // the voice note
	Status    string `json:"Status"`
	Text      string `json:"Text"`
}
