package telegram

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// TestSendMedia_RejectsCaptionOverLimitAfterConversion is the regression test
// for the 2026-06-15 triple-review finding: the caption-length guard measured
// the RAW caption but SENT the markdown→HTML-converted caption. A near-limit
// formatted caption (e.g. **…**) balloons past 1024 once converted, so Telegram
// would 400 with no fallback. The guard must measure the CONVERTED caption and
// reject pre-send.
//
// Constructs a caption that is exactly maxCaptionRunes RAW but over the limit
// once `**…**` becomes `<b>…</b>` (+5 UTF-16 units). A URL media item is used so
// sendMedia reaches the caption check without a live bot or local file (the
// check returns before any bot/rate call).
func TestSendMedia_RejectsCaptionOverLimitAfterConversion(t *testing.T) {
	inner := strings.Repeat("x", maxCaptionRunes-4) // **xxxx…** = maxCaptionRunes raw
	raw := "**" + inner + "**"
	if captionUTF16Len(raw) != maxCaptionRunes {
		t.Fatalf("test setup: raw caption is %d units, want exactly %d", captionUTF16Len(raw), maxCaptionRunes)
	}
	converted := mdToTelegramHTML(raw) // <b>…</b> = maxCaptionRunes + 5
	if captionUTF16Len(converted) <= maxCaptionRunes {
		t.Fatalf("test setup: converted caption is %d units, expected over %d", captionUTF16Len(converted), maxCaptionRunes)
	}

	c := &Channel{}
	item := c3types.MediaItem{
		Kind:    c3types.MediaPhoto,
		URL:     "https://example.com/pic.png",
		Caption: raw,
	}
	_, err := c.sendMedia(c3types.ReplyArgs{ChatID: -100}, item)
	if err == nil {
		t.Fatal("expected a pre-send rejection for a converted-over-limit caption, got nil")
	}
	if !strings.Contains(err.Error(), "formatted caption") {
		t.Errorf("error should explain the FORMATTED caption is over limit; got %q", err.Error())
	}
}

// TestSendMedia_CaptionGuardKeysOffConvertedLength confirms the guard's
// measurement semantics: it must key off the CONVERTED length, not the raw.
// A caption whose CONVERTED form is exactly at the limit must NOT be over,
// while the converted-over case from the regression above must be. (We assert
// the measurement directly to avoid the rate/bot path that begins after the
// caption check.)
func TestSendMedia_CaptionGuardKeysOffConvertedLength(t *testing.T) {
	// Plain text exactly at the limit: conversion is a no-op, converted == limit.
	atLimitRaw := strings.Repeat("y", maxCaptionRunes)
	if got := captionUTF16Len(mdToTelegramHTML(atLimitRaw)); got > maxCaptionRunes {
		t.Errorf("an at-limit plain caption must not be over the limit once converted; got %d > %d", got, maxCaptionRunes)
	}
	// A short raw caption whose markdown expands but still fits must not be over.
	link := "[docs](https://example.com)"
	if got := captionUTF16Len(mdToTelegramHTML(link)); got > maxCaptionRunes {
		t.Errorf("a short link caption should fit after conversion; got %d", got)
	}
}

// TestSendMedia_HonorsButtons is the M1 regression: the gate rides a kept inline
// keyboard on the FIRST emitted part, which may be a media part (a media reply
// with no text). sendMedia previously ignored args.Buttons, silently dropping
// the keyboard. It must now build the markup — proven here because an INVALID
// keyboard (an empty row) returns the build error before any bot call. Were the
// buttons still ignored, sendMedia would proceed past the build with no error.
// (A URL item with a short caption reaches the button build without a live bot or
// local file.)
func TestSendMedia_HonorsButtons(t *testing.T) {
	c := &Channel{}
	item := c3types.MediaItem{
		Kind:    c3types.MediaPhoto,
		URL:     "https://example.com/pic.png",
		Caption: "look",
	}
	args := c3types.ReplyArgs{ChatID: -100, Buttons: [][]c3types.Button{{}}} // empty row
	_, err := c.sendMedia(args, item)
	if err == nil || !strings.Contains(err.Error(), "row 1 is empty") {
		t.Fatalf("sendMedia must honor (build) args.Buttons; got %v", err)
	}
}
