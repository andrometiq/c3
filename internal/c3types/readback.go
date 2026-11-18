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
