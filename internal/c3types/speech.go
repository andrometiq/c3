package c3types

import "errors"

// SpeechRequest is one text-to-speech synthesis request.
type SpeechRequest struct {
	Text     string
	Language string
}

// SpeechResult is synthesized audio plus its provider metadata.
type SpeechResult struct {
	Audio    []byte
	MIME     string
	Provider string
}

var (
	// ErrNothingToSay means preprocessing found no speakable prose.
	ErrNothingToSay = errors.New("nothing to say")
	// ErrNoSynthesizer means no TTS plugin registered a synthesizer.
	ErrNoSynthesizer = errors.New("no synthesizer registered")
)
