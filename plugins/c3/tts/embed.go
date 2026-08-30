// Package ttsassets embeds the release TTS runtime in c3-broker.
package ttsassets

import "embed"

// Runtime contains only production runtime files.
//
//go:embed tts-handler.py tts-pkg/tts.py tts-pkg/providers/*.py
var Runtime embed.FS
