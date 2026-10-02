package telegram

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
)

// SendApprovalCard sends an auto-mode approval card: args.Text is HTML the
// broker has already escaped, args.Buttons its Allow/Deny keyboard. ctx is the
// approval request's deadline and bounds both the rate-limit wait and the HTTP
// send. When args.ReplyTo is set the card must reply to that message (its
// overflow attachment): if the message is gone the send fails rather than
// posting a card detached from its input.
func (c *Channel) SendApprovalCard(ctx context.Context, args c3types.ReplyArgs) (int64, error) {
	if c.bot == nil {
		return 0, errors.New("telegram: channel not started")
	}
	opts := &gotgbot.SendMessageOpts{
		MessageThreadId:    threadID(args.TopicID),
		ParseMode:          "HTML",
		LinkPreviewOptions: &gotgbot.LinkPreviewOptions{IsDisabled: true},
		RequestOpts:        c.requestOptsFor("sendMessage"),
	}
	if args.ReplyTo != nil {
		opts.ReplyParameters = &gotgbot.ReplyParameters{MessageId: *args.ReplyTo}
	}
	if len(args.Buttons) > 0 {
		keyboard, err := buildInlineKeyboard(args.Buttons)
		if err != nil {
			return 0, err
		}
		opts.ReplyMarkup = keyboard
	}
	if err := c.rate.Wait(ctx, args.ChatID); err != nil {
		return 0, fmt.Errorf("telegram: rate-wait: %w", err)
	}
	msg, err := c.bot.SendMessageWithContext(ctx, args.ChatID, args.Text, opts)
	if err != nil {
		c.recordOutboundErr(err)
		return 0, c.scrubTokenf("telegram: approval card: %w", err)
	}
	c.recordOutboundSuccess()
	return msg.MessageId, nil
}

// SendApprovalDocument uploads an approval card's overflow input as a
// plain-text document straight from memory, with args.Text as the caption.
// ctx bounds the rate-limit wait and the upload, as for SendApprovalCard.
func (c *Channel) SendApprovalDocument(ctx context.Context, args c3types.ReplyArgs, name string, content []byte) (int64, error) {
	if c.bot == nil {
		return 0, errors.New("telegram: channel not started")
	}
	if captionUTF16Len(args.Text) > maxCaptionRunes {
		return 0, errors.New("telegram: approval caption exceeds the caption limit")
	}
	opts := &gotgbot.SendDocumentOpts{
		Caption:                     args.Text,
		MessageThreadId:             threadID(args.TopicID),
		DisableContentTypeDetection: true,
		RequestOpts:                 c.requestOptsFor("sendDocument"),
	}
	if err := c.rate.Wait(ctx, args.ChatID); err != nil {
		return 0, fmt.Errorf("telegram: rate-wait: %w", err)
	}
	file := gotgbot.InputFileByReader(name, bytes.NewReader(content))
	msg, err := c.bot.SendDocumentWithContext(ctx, args.ChatID, file, opts)
	if err != nil {
		c.recordOutboundErr(err)
		return 0, c.scrubTokenf("telegram: approval attachment: %w", err)
	}
	c.recordOutboundSuccess()
	return msg.MessageId, nil
}
