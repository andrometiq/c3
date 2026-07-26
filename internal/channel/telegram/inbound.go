package telegram

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
)

// convertInbound translates a gotgbot.Message into a c3types.Inbound, applying
// the channel-side STT prefix to voice messages (Plan 5 will replace the
// transcript text via the STT plugin; for now voice messages get an empty
// Text and an attachment kind="voice").
//
// Returns nil for messages we don't surface (service messages like
// forum_topic_created — spec §10 deferred).
func convertInbound(channel string, msg *gotgbot.Message, sttPrefix string, richRaw json.RawMessage) *c3types.Inbound {
	if msg == nil {
		return nil
	}
	// Skip pure-service messages we don't surface.
	if isUnsupportedService(msg) {
		return nil
	}

	in := &c3types.Inbound{
		Channel:   channel,
		ChatID:    msg.Chat.Id,
		MessageID: msg.MessageId,
		Sender:    convertSender(msg.From),
		Timestamp: time.Unix(msg.Date, 0).UTC(),
	}
	if msg.MessageThreadId != 0 {
		t := msg.MessageThreadId
		in.TopicID = &t
	}
	if msg.ReplyToMessage != nil {
		in.ReplyTo = &c3types.ReplyContext{
			MessageID: msg.ReplyToMessage.MessageId,
			User:      convertSender(msg.ReplyToMessage.From),
			Text:      replyText(msg.ReplyToMessage),
		}
	}

	// Rich message (Bot API 10.1) takes precedence: a rich message IS the
	// message. richRaw is non-nil only when the channel captured a rich_message
	// AND the rich_inbound toggle is on (see poll.go). Decode to markdown +
	// attachments; on decode failure fall back to a non-empty marker so a rich
	// message is never surfaced empty.
	if len(richRaw) > 0 {
		md, atts, ok := decodeRichMessage(richRaw)
		if !ok {
			md = "[rich message]"
		}
		in.Text = md
		in.Attachments = append(in.Attachments, atts...)
		return in
	}

	// Body + attachments. Order matters: voice handled first because it gets
	// special STT-prefix treatment in Plan 5; text/caption-only messages are
	// the simplest path.
	switch {
	case msg.Voice != nil:
		in.Text = "" // STT plugin (Plan 5) populates this; for v1 channel-only,
		// the broker's plugin pipeline (Plan 5) substitutes Text with the
		// transcript prefixed by sttPrefix. Until then this is empty so the
		// adapter can fall back to caption or "(voice message)".
		_ = sttPrefix // referenced for future STT integration
		in.Attachments = append(in.Attachments, c3types.Attachment{
			Kind:   "voice",
			FileID: msg.Voice.FileId,
			Size:   msg.Voice.FileSize,
			MIME:   msg.Voice.MimeType,
		})
		if msg.Caption != "" {
			in.Text = msg.Caption
		}
	case len(msg.Photo) > 0:
		// Pick the highest-resolution PhotoSize.
		best := pickBestPhoto(msg.Photo)
		in.Attachments = append(in.Attachments, c3types.Attachment{
			Kind:   "photo",
			FileID: best.FileId,
			Size:   best.FileSize,
		})
		in.Text = msg.Caption
	case msg.Document != nil:
		in.Attachments = append(in.Attachments, c3types.Attachment{
			Kind:   "document",
			FileID: msg.Document.FileId,
			Size:   msg.Document.FileSize,
			MIME:   msg.Document.MimeType,
			Name:   msg.Document.FileName,
		})
		in.Text = msg.Caption
	case msg.Audio != nil:
		in.Attachments = append(in.Attachments, c3types.Attachment{
			Kind:   "audio",
			FileID: msg.Audio.FileId,
			Size:   msg.Audio.FileSize,
			MIME:   msg.Audio.MimeType,
			Name:   msg.Audio.FileName,
		})
		in.Text = msg.Caption
	case msg.Video != nil:
		in.Attachments = append(in.Attachments, c3types.Attachment{
			Kind:   "video",
			FileID: msg.Video.FileId,
			Size:   msg.Video.FileSize,
			MIME:   msg.Video.MimeType,
			Name:   msg.Video.FileName,
		})
		in.Text = msg.Caption
	case msg.VideoNote != nil:
		in.Attachments = append(in.Attachments, c3types.Attachment{
			Kind:   "video_note",
			FileID: msg.VideoNote.FileId,
			Size:   msg.VideoNote.FileSize,
		})
		in.Text = msg.Caption
	case msg.Sticker != nil:
		in.Attachments = append(in.Attachments, c3types.Attachment{
			Kind:   "sticker",
			FileID: msg.Sticker.FileId,
			Size:   msg.Sticker.FileSize,
		})
		// Telegram stickers don't carry captions in the message; surface an
		// emoji label if available.
		if msg.Sticker.Emoji != "" {
			in.Text = msg.Sticker.Emoji
		}
	default:
		in.Text = msg.Text
	}

	return in
}

// convertSender extracts a Sender from a gotgbot User pointer.
func convertSender(u *gotgbot.User) c3types.Sender {
	if u == nil {
		return c3types.Sender{}
	}
	return c3types.Sender{
		UserID:   u.Id,
		Username: u.Username,
	}
}

// replyText returns the text or caption of a replied-to message, whichever is
// present. Empty string if neither.
func replyText(msg *gotgbot.Message) string {
	if msg.Text != "" {
		return msg.Text
	}
	return msg.Caption
}

// pickBestPhoto returns the highest-resolution PhotoSize from a slice.
// Telegram returns photos sorted by size (smallest first) but we don't rely
// on that — pick by Width*Height.
func pickBestPhoto(sizes []gotgbot.PhotoSize) gotgbot.PhotoSize {
	best := sizes[0]
	bestArea := best.Width * best.Height
	for _, p := range sizes[1:] {
		area := p.Width * p.Height
		if area > bestArea {
			best = p
			bestArea = area
		}
	}
	return best
}

// isUnsupportedService returns true for service messages C3 doesn't surface
// in v1 (forum_topic_created/edited/closed, new_chat_members, etc.). Spec §10
// defers forum_topic_* tracking; surfacing these as inbound would be noise.
func isUnsupportedService(msg *gotgbot.Message) bool {
	if msg.ForumTopicCreated != nil ||
		msg.ForumTopicEdited != nil ||
		msg.ForumTopicClosed != nil ||
		msg.ForumTopicReopened != nil ||
		msg.GeneralForumTopicHidden != nil ||
		msg.GeneralForumTopicUnhidden != nil {
		return true
	}
	if len(msg.NewChatMembers) > 0 || msg.LeftChatMember != nil {
		return true
	}
	if msg.NewChatTitle != "" || msg.NewChatPhoto != nil || msg.DeleteChatPhoto {
		return true
	}
	return false
}

// formatChatID is a cheap helper for log lines.
func formatChatID(chatID int64) string { return strconv.FormatInt(chatID, 10) }
