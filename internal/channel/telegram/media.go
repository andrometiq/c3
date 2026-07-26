package telegram

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"unicode/utf16"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
)

// captionUTF16Len counts a caption's length in UTF-16 code units — the unit
// Telegram measures caption length in (an astral rune is a surrogate pair = 2
// units). The caption-length policy is the gate's job; this is the in-channel
// last-resort guard.
func captionUTF16Len(s string) int {
	return len(utf16.Encode([]rune(s)))
}

// sendMedia sends ONE media item (one part = one item; album grouping is
// descoped in v1 — the gate emits one part per item). It dispatches by Kind to
// the matching Bot API send method, honoring the caption (markdown→HTML via the
// same converter as text), message_thread_id (TopicID), reply_parameters
// (ReplyTo), and spoiler where the kind supports it. Returns the sent
// message_id.
//
// Source resolution (single-host invariant): a non-empty Path is a local file
// on the shared host (opened + uploaded as multipart); otherwise a non-empty URL
// is passed to Telegram for server-side fetch. Path takes precedence when both
// are set.
//
// Validation (impure — correct in the channel, not the pure gate): for a Path
// item we os.Stat the file (clear error if missing) and reject if it exceeds
// MaxSendBytes (50 MiB). A URL's size is unknowable pre-send, so we rely on
// Telegram's own rejection surfaced as the send error.
func (c *Channel) sendMedia(args c3types.ReplyArgs, item c3types.MediaItem) (int64, error) {
	if item.Path == "" && item.URL == "" {
		return 0, errors.New("telegram: media item has neither path nor url")
	}

	// Resolve the file source + validate a local Path (existence + size).
	var src gotgbot.InputFileOrString
	var fh *os.File
	if item.Path != "" {
		info, err := os.Stat(item.Path)
		if err != nil {
			if os.IsNotExist(err) {
				return 0, fmt.Errorf("telegram: media file not found: %s", item.Path)
			}
			return 0, fmt.Errorf("telegram: stat media file %s: %w", item.Path, err)
		}
		if info.IsDir() {
			return 0, fmt.Errorf("telegram: media path is a directory, not a file: %s", item.Path)
		}
		if info.Size() > maxSendBytes {
			return 0, fmt.Errorf("telegram: media file too large: %s is %d bytes, over the %d-byte (50 MiB) send limit",
				item.Path, info.Size(), int64(maxSendBytes))
		}
		f, err := os.Open(item.Path)
		if err != nil {
			return 0, fmt.Errorf("telegram: open media file %s: %w", item.Path, err)
		}
		fh = f
		defer fh.Close()
		src = gotgbot.InputFileByReader(filepath.Base(item.Path), fh)
	} else {
		src = gotgbot.InputFileByURL(item.URL)
	}

	// Caption: convert markdown→HTML the same way text replies do, then measure
	// the CONVERTED caption — that is what Telegram actually receives and counts.
	// Measuring the RAW caption would let a near-limit formatted caption balloon
	// past 1024 once converted (e.g. `<a href="…">…</a>` is far longer than its
	// markdown source) and Telegram would 400 with no fallback. We reject the
	// converted over-limit caption here as a clean pre-send error instead. (v1:
	// no gate trim+follow-up; a clear actionable rejection is the behavior.)
	caption := item.Caption
	captionHTML := mdToTelegramHTML(caption)
	if captionUTF16Len(captionHTML) > maxCaptionRunes {
		return 0, fmt.Errorf("telegram: formatted caption is %d chars, over the %d-char caption limit — shorten it or send the text as a separate message (markdown formatting expands the length Telegram counts)",
			captionUTF16Len(captionHTML), maxCaptionRunes)
	}

	// Inline keyboard (P7). The gate rides any kept Buttons on the FIRST emitted
	// part, which may be this media part (e.g. a media reply with no text). Build
	// the Telegram markup here too so buttons on a media reply aren't silently
	// dropped; a limit breach is a clear error (no send), matching the text path.
	var markup *gotgbot.InlineKeyboardMarkup
	if len(args.Buttons) > 0 {
		m, err := buildInlineKeyboard(args.Buttons)
		if err != nil {
			return 0, err
		}
		markup = m
	}

	if err := c.rate.Wait(c.ctx, args.ChatID); err != nil {
		return 0, fmt.Errorf("telegram: rate-wait: %w", err)
	}

	var (
		msg *gotgbot.Message
		err error
	)
	switch item.Kind {
	case c3types.MediaPhoto:
		opts := &gotgbot.SendPhotoOpts{
			Caption:         captionHTML,
			HasSpoiler:      item.Spoiler,
			RequestOpts:     c.requestOptsFor("sendPhoto"),
			MessageThreadId: threadID(args.TopicID),
			ReplyParameters: replyParams(args.ReplyTo),
		}
		if caption != "" {
			opts.ParseMode = "HTML"
		}
		if markup != nil {
			opts.ReplyMarkup = markup
		}
		msg, err = c.bot.SendPhoto(args.ChatID, src, opts)
	case c3types.MediaFile:
		opts := &gotgbot.SendDocumentOpts{
			Caption:         captionHTML,
			RequestOpts:     c.requestOptsFor("sendDocument"),
			MessageThreadId: threadID(args.TopicID),
			ReplyParameters: replyParams(args.ReplyTo),
		}
		if caption != "" {
			opts.ParseMode = "HTML"
		}
		if markup != nil {
			opts.ReplyMarkup = markup
		}
		msg, err = c.bot.SendDocument(args.ChatID, src, opts)
	case c3types.MediaVideo:
		opts := &gotgbot.SendVideoOpts{
			Caption:    captionHTML,
			HasSpoiler: item.Spoiler,
			// Hint that the upload is progressive-playback friendly so clients
			// can start playing before the full file downloads. Duration/size
			// metadata is not plumbed through MediaItem yet (deferred).
			SupportsStreaming: true,
			RequestOpts:       c.requestOptsFor("sendVideo"),
			MessageThreadId:   threadID(args.TopicID),
			ReplyParameters:   replyParams(args.ReplyTo),
		}
		if caption != "" {
			opts.ParseMode = "HTML"
		}
		if markup != nil {
			opts.ReplyMarkup = markup
		}
		msg, err = c.bot.SendVideo(args.ChatID, src, opts)
	case c3types.MediaAudio:
		opts := &gotgbot.SendAudioOpts{
			Caption:         captionHTML,
			RequestOpts:     c.requestOptsFor("sendAudio"),
			MessageThreadId: threadID(args.TopicID),
			ReplyParameters: replyParams(args.ReplyTo),
		}
		if caption != "" {
			opts.ParseMode = "HTML"
		}
		if markup != nil {
			opts.ReplyMarkup = markup
		}
		msg, err = c.bot.SendAudio(args.ChatID, src, opts)
	case c3types.MediaVoice:
		opts := &gotgbot.SendVoiceOpts{
			Caption:         captionHTML,
			RequestOpts:     c.requestOptsFor("sendVoice"),
			MessageThreadId: threadID(args.TopicID),
			ReplyParameters: replyParams(args.ReplyTo),
		}
		if caption != "" {
			opts.ParseMode = "HTML"
		}
		if markup != nil {
			opts.ReplyMarkup = markup
		}
		msg, err = c.bot.SendVoice(args.ChatID, src, opts)
	case c3types.MediaAnimation:
		opts := &gotgbot.SendAnimationOpts{
			Caption:         captionHTML,
			HasSpoiler:      item.Spoiler,
			RequestOpts:     c.requestOptsFor("sendAnimation"),
			MessageThreadId: threadID(args.TopicID),
			ReplyParameters: replyParams(args.ReplyTo),
		}
		if caption != "" {
			opts.ParseMode = "HTML"
		}
		if markup != nil {
			opts.ReplyMarkup = markup
		}
		msg, err = c.bot.SendAnimation(args.ChatID, src, opts)
	default:
		return 0, fmt.Errorf("telegram: unsupported media kind %q", string(item.Kind))
	}

	if err != nil {
		c.recordOutboundErr(err)
		return 0, c.scrubTokenf("telegram: send %s: %w", string(item.Kind), err)
	}
	c.recordOutboundSuccess()
	return msg.MessageId, nil
}

// threadID maps a *int64 TopicID into the int64 the gotgbot opts expect (0 = no
// thread, the same zero-value gotgbot treats as "omit").
func threadID(topicID *int64) int64 {
	if topicID == nil {
		return 0
	}
	return *topicID
}

// replyParams builds gotgbot ReplyParameters from a *int64 ReplyTo, or nil.
func replyParams(replyTo *int64) *gotgbot.ReplyParameters {
	if replyTo == nil {
		return nil
	}
	return &gotgbot.ReplyParameters{
		MessageId:                *replyTo,
		AllowSendingWithoutReply: true,
	}
}
