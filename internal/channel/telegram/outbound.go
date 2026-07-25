package telegram

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/karthikeyan5/c3/internal/c3types"
)

// SendReply sends ONE Telegram part and returns its message_id. A part carries
// AT MOST ONE of {text, a single media item, a poll} — the pure capability.Gate
// splits a logical reply into such parts and dispatch sends one part per call.
// Honors message_thread_id when args.TopicID is non-nil and reply_parameters
// when args.ReplyTo is non-nil.
//
// Part-by-content dispatch (P3):
//   - args.Poll != nil → send a poll (sendPoll).
//   - len(args.Media) == 1 → send that one media item by Kind (sendMedia).
//   - else → send args.Text as a single text message (the path below).
//
// Chunking is NOT done here. The gate splits a long logical reply into parts
// that each fit Telegram's limit — SendReply sends one. This removes, by
// construction, the prior silent-success-on-chunk-k>0 bug where a failed Nth
// chunk logged, broke the loop, and returned success.
//
// Markup mapping (channel-neutral intent → Telegram wire):
//   - MarkupMarkdown OR "" (empty/zero value = the MARKDOWN DEFAULT): run
//     mdToTelegramHTML, send parse_mode=HTML. Broker-internal callers (welcome,
//     fallback, ping) construct ReplyArgs WITHOUT setting Markup and rely on the
//     empty value meaning auto-convert; without this their markdown would render
//     as literal characters.
//   - MarkupNative: send the text as-is (pre-formed HTML), parse_mode=HTML.
//   - MarkupNone: plain text, no parse_mode.
func (c *Channel) SendReply(args c3types.ReplyArgs) (int64, error) {
	if c.bot == nil {
		return 0, errors.New("telegram: channel not started")
	}
	// A part carries at most one of {poll, single media item, text}.
	if args.Poll != nil {
		return c.sendPoll(args)
	}
	if len(args.Media) == 1 {
		return c.sendMedia(args, args.Media[0])
	}
	if len(args.Media) > 1 {
		// The gate emits one media item per part; >1 here means a caller bypassed
		// the gate. Fail loudly rather than silently send only the first.
		return 0, fmt.Errorf("telegram: SendReply got %d media items in one part — the gate emits one item per part", len(args.Media))
	}

	// Native rich-message table route (Bot API 10.1 sendRichMessage), gated on the
	// richTablesEnabled switch (now ENABLED). When the reply is rich-eligible (a
	// detected GFM table within caps on a markdown reply), send the WHOLE reply as
	// native markdown so Telegram renders real tables. On ANY error fall through to
	// the existing monospace/plaintext path so a message is never lost.
	if richTableEligible(richTablesEnabled, args.Markup, args.Text) {
		if id, err := c.sendRich(args); err == nil {
			return id, nil
		} else {
			c.logf("telegram: sendRichMessage failed, falling back to monospace path: %v", err)
		}
	}

	// Empty/zero-value Markup is the MARKDOWN DEFAULT (see doc comment).
	convertMd := args.Markup == c3types.MarkupMarkdown || args.Markup == ""
	useHTML := convertMd || args.Markup == c3types.MarkupNative

	text := args.Text
	opts := &gotgbot.SendMessageOpts{}
	if args.TopicID != nil {
		opts.MessageThreadId = *args.TopicID
	}
	if convertMd {
		text = mdToTelegramHTML(text)
	}
	if useHTML {
		opts.ParseMode = "HTML"
	}
	if args.ReplyTo != nil {
		opts.ReplyParameters = &gotgbot.ReplyParameters{
			MessageId:                *args.ReplyTo,
			AllowSendingWithoutReply: true,
		}
	}
	// Inline keyboard (P7). The gate has already dropped buttons on a channel
	// that does not advertise InlineKeyboards, so reaching here means the
	// keyboard is intended; build the Telegram markup and enforce the Telegram-
	// specific limits (callback_data 1-64 bytes, max rows/buttons-per-row). A
	// limit breach is a clear error (no send), not a silent drop.
	if len(args.Buttons) > 0 {
		markup, err := buildInlineKeyboard(args.Buttons)
		if err != nil {
			return 0, err
		}
		opts.ReplyMarkup = markup
	}
	opts.RequestOpts = c.requestOptsFor("sendMessage")
	if err := c.rate.Wait(c.ctx, args.ChatID); err != nil {
		return 0, fmt.Errorf("telegram: rate-wait: %w", err)
	}
	msg, err := c.bot.SendMessage(args.ChatID, text, opts)
	if err != nil && convertMd && isParseEntityError(err) {
		// Plaintext fallback (per the predecessor bot's bot/delivery.send.ts pattern). Our
		// markdown converter occasionally produces malformed HTML for
		// pathological input; re-send the ORIGINAL text as plain text rather
		// than dropping the message.
		c.logf("telegram: HTML parse error, retrying as plaintext: %v", err)
		plainOpts := *opts
		plainOpts.ParseMode = ""
		msg, err = c.bot.SendMessage(args.ChatID, args.Text, &plainOpts)
	}
	if err != nil {
		c.recordOutboundErr(err)
		// Outbound-health feed site #1 (CRITIQUE FOLD #2): a single un-retried
		// SendReply failure. feedOutboundFailure counts it ONLY if it is a genuine
		// transient (not permanent / format / 429). This is NOT inside the shared
		// recordOutboundErr, which fires per-attempt and would multi-count.
		c.feedOutboundFailure(err, "SendReply transient send error")
		return 0, c.scrubTokenf("telegram: SendMessage: %w", err)
	}
	c.recordOutboundSuccess()
	return msg.MessageId, nil
}

// isParseEntityError returns whether a SendMessage error indicates Telegram
// rejected the entities we sent (malformed HTML or MarkdownV2). On these we
// retry plain-text rather than drop the message — pattern from a prior
// TypeScript Telegram bot's extensions/telegram/src/bot/delivery.send.ts
// (sub-agent research 2026-05-09).
func isParseEntityError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "can't parse entities") ||
		strings.Contains(s, "parse entities") ||
		strings.Contains(s, "find end of the entity") ||
		strings.Contains(s, "Bad Request: can't parse")
}

// buildInlineKeyboard converts the channel-neutral [][]c3types.Button (rows of
// buttons) into a gotgbot.InlineKeyboardMarkup and enforces the Telegram-
// specific limits that belong in this package (the no-leak rule): each button
// needs a non-empty Text and EXACTLY ONE of Data (a callback button) or URL (a
// link button); callback_data must be 1-64 BYTES; and the keyboard shape stays
// within the conservative row / per-row caps. Any breach returns a clear,
// actionable error so the agent learns precisely what was wrong instead of
// getting an opaque Telegram 400. Returns a *InlineKeyboardMarkup (the gotgbot
// ReplyMarkup) on success.
func buildInlineKeyboard(rows [][]c3types.Button) (*gotgbot.InlineKeyboardMarkup, error) {
	if len(rows) > maxKeyboardRows {
		return nil, fmt.Errorf("telegram: too many keyboard rows (%d > %d)", len(rows), maxKeyboardRows)
	}
	kb := make([][]gotgbot.InlineKeyboardButton, 0, len(rows))
	for ri, row := range rows {
		// An empty row (`[]`) is rejected, not silently dropped: Telegram 400s on
		// an empty keyboard row. A clear error tells the agent precisely which row.
		if len(row) == 0 {
			return nil, fmt.Errorf("telegram: buttons row %d is empty", ri+1)
		}
		if len(row) > maxButtonsPerRow {
			return nil, fmt.Errorf("telegram: too many buttons in row %d (%d > %d)", ri+1, len(row), maxButtonsPerRow)
		}
		outRow := make([]gotgbot.InlineKeyboardButton, 0, len(row))
		for bi, b := range row {
			if b.Text == "" {
				return nil, fmt.Errorf("telegram: button at row %d position %d has no text", ri+1, bi+1)
			}
			hasData := b.Data != ""
			hasURL := b.URL != ""
			if hasData == hasURL {
				return nil, fmt.Errorf("telegram: button %q must set EXACTLY ONE of data (callback) or url (link)", b.Text)
			}
			btn := gotgbot.InlineKeyboardButton{Text: b.Text}
			if hasData {
				if n := len(b.Data); n > maxCallbackDataBytes {
					return nil, fmt.Errorf("telegram: button %q callback data is %d bytes, over the %d-byte limit — keep it short",
						b.Text, n, maxCallbackDataBytes)
				}
				btn.CallbackData = b.Data
			} else {
				btn.Url = b.URL
			}
			outRow = append(outRow, btn)
		}
		kb = append(kb, outRow)
	}
	return &gotgbot.InlineKeyboardMarkup{InlineKeyboard: kb}, nil
}

// SendTyping sends a typing chat action. Used both for the typing indicator
// (spec §7.1) and as the validate_topic primitive (spec §6 — sending a typing
// action with a thread_id implicitly validates the thread exists).
func (c *Channel) SendTyping(chatID int64, threadID *int64) error {
	if c.bot == nil {
		return errors.New("telegram: channel not started")
	}
	opts := &gotgbot.SendChatActionOpts{
		RequestOpts: c.requestOptsFor("sendChatAction"),
	}
	if threadID != nil {
		opts.MessageThreadId = *threadID
	}
	if err := c.rate.Wait(c.ctx, chatID); err != nil {
		return fmt.Errorf("telegram: rate-wait: %w", err)
	}
	if _, err := c.bot.SendChatAction(chatID, "typing", opts); err != nil {
		c.recordOutboundErr(err)
		return c.scrubTokenf("telegram: SendChatAction: %w", err)
	}
	c.recordOutboundSuccess()
	return nil
}

// EditMessage edits a previously-sent message's text. Used by the
// edit_progress tool (spec §7.2) and by the broker's placeholder lifecycle.
//
// Markup mapping (P2b — the converter is now wired into edits, so an edited
// message renders rich just like a reply; today EditMessage did not convert at
// all). Same rule as SendReply:
//   - MarkupMarkdown OR "" (empty/zero value = the MARKDOWN DEFAULT): run
//     mdToTelegramHTML, send parse_mode=HTML. Broker-internal callers that build
//     EditArgs WITHOUT setting Markup rely on the empty value meaning
//     auto-convert; without this their markdown would render as literal chars.
//   - MarkupNative: send the text as-is (pre-formed HTML), parse_mode=HTML.
//   - MarkupNone: plain text, no parse_mode.
//
// Plaintext fallback on a parse error mirrors SendReply: a malformed-HTML edit
// is retried as the original plain text rather than dropped.
func (c *Channel) EditMessage(args c3types.EditArgs) (*c3types.EditResult, error) {
	if c.bot == nil {
		return nil, errors.New("telegram: channel not started")
	}

	convertMd := args.Markup == c3types.MarkupMarkdown || args.Markup == ""
	useHTML := convertMd || args.Markup == c3types.MarkupNative

	text := args.Text
	opts := &gotgbot.EditMessageTextOpts{
		ChatId:      args.ChatID,
		MessageId:   args.MessageID,
		RequestOpts: c.requestOptsFor("editMessageText"),
	}
	if convertMd {
		text = mdToTelegramHTML(text)
	}
	if useHTML {
		opts.ParseMode = "HTML"
	}
	// Inline keyboard (Phase 1 ask round-trip). A non-nil args.Buttons sets the
	// message's reply markup; a non-nil EMPTY keyboard CLEARS it (Telegram removes
	// the keyboard when sent an empty inline_keyboard). A nil args.Buttons leaves
	// the existing keyboard untouched, so pre-existing edit callers (edit_progress
	// / placeholder lifecycle) keep their byte-identical behavior. Reuses
	// buildInlineKeyboard so the same callback_data/shape limits apply.
	if args.Buttons != nil {
		markup, err := buildInlineKeyboard(args.Buttons)
		if err != nil {
			return nil, err
		}
		// EditMessageTextOpts.ReplyMarkup is a concrete InlineKeyboardMarkup value
		// (unlike SendMessageOpts' ReplyMarkup interface), so dereference. An empty
		// InlineKeyboard slice serializes as `{"inline_keyboard":[]}`, which Telegram
		// treats as "remove the keyboard".
		opts.ReplyMarkup = *markup
	}
	if err := c.rate.Wait(c.ctx, args.ChatID); err != nil {
		return nil, fmt.Errorf("telegram: rate-wait: %w", err)
	}
	_, _, err := c.bot.EditMessageText(text, opts)
	if err != nil && convertMd && isParseEntityError(err) {
		c.logf("telegram: HTML parse error on edit, retrying as plaintext: %v", err)
		plainOpts := *opts
		plainOpts.ParseMode = ""
		_, _, err = c.bot.EditMessageText(args.Text, &plainOpts)
	}
	if err != nil {
		c.recordOutboundErr(err)
		return nil, c.scrubTokenf("telegram: EditMessageText: %w", err)
	}
	c.recordOutboundSuccess()
	return &c3types.EditResult{MessageID: args.MessageID}, nil
}

// allowedReactionEmoji is Telegram's fixed set of standard reaction emoji
// accepted by setMessageReaction (ReactionTypeEmoji). Anything outside this set
// is rejected by the API with a raw 400; we pre-validate so the agent gets a
// clear, actionable error instead. Sourced verbatim from the documented list on
// ReactionTypeEmoji.Emoji (gotgbot gen_types.go; https://core.telegram.org/bots/api#reactiontypeemoji).
// This is a Telegram-specific fact and intentionally lives in this package only.
var allowedReactionEmoji = map[string]struct{}{
	"👍": {}, "👎": {}, "❤": {}, "🔥": {}, "🥰": {}, "👏": {}, "😁": {}, "🤔": {},
	"🤯": {}, "😱": {}, "🤬": {}, "😢": {}, "🎉": {}, "🤩": {}, "🤮": {}, "💩": {},
	"🙏": {}, "👌": {}, "🕊": {}, "🤡": {}, "🥱": {}, "🥴": {}, "😍": {}, "🐳": {},
	"❤‍🔥": {}, "🌚": {}, "🌭": {}, "💯": {}, "🤣": {}, "⚡": {}, "🍌": {}, "🏆": {},
	"💔": {}, "🤨": {}, "😐": {}, "🍓": {}, "🍾": {}, "💋": {}, "🖕": {}, "😈": {},
	"😴": {}, "😭": {}, "🤓": {}, "👻": {}, "👨‍💻": {}, "👀": {}, "🎃": {}, "🙈": {},
	"😇": {}, "😨": {}, "🤝": {}, "✍": {}, "🤗": {}, "🫡": {}, "🎅": {}, "🎄": {},
	"☃": {}, "💅": {}, "🤪": {}, "🗿": {}, "🆒": {}, "💘": {}, "🙉": {}, "🦄": {},
	"😘": {}, "💊": {}, "🙊": {}, "😎": {}, "👾": {}, "🤷‍♂": {}, "🤷": {}, "🤷‍♀": {},
	"😡": {},
}

// React sets a single-emoji reaction on a message.
func (c *Channel) React(args c3types.ReactArgs) error {
	if c.bot == nil {
		return errors.New("telegram: channel not started")
	}
	if _, ok := allowedReactionEmoji[args.Emoji]; !ok {
		return fmt.Errorf("telegram: unsupported reaction emoji %q; Telegram allows only its fixed standard set (👍 👎 ❤ 🔥 🥰 👏 😁 🤔 … 😡 — see https://core.telegram.org/bots/api#reactiontypeemoji)", args.Emoji)
	}
	opts := &gotgbot.SetMessageReactionOpts{
		Reaction: []gotgbot.ReactionType{
			gotgbot.ReactionTypeEmoji{Emoji: args.Emoji},
		},
		RequestOpts: c.requestOptsFor("setMessageReaction"),
	}
	if err := c.rate.Wait(c.ctx, args.ChatID); err != nil {
		return fmt.Errorf("telegram: rate-wait: %w", err)
	}
	if _, err := c.bot.SetMessageReaction(args.ChatID, args.MessageID, opts); err != nil {
		c.recordOutboundErr(err)
		return c.scrubTokenf("telegram: SetMessageReaction: %w", err)
	}
	c.recordOutboundSuccess()
	return nil
}

// DownloadAttachment fetches a Telegram file by file_id and saves it to a
// local cache dir. Returns the local path. Bot API caps at 20MB.
//
// Local cache layout:
//
//	$XDG_CACHE_HOME/c3/telegram/attachments/<file_unique_basename>
//	~/.cache/c3/telegram/attachments/<...>  (fallback)
func (c *Channel) DownloadAttachment(fileID string) (string, error) {
	if c.bot == nil {
		return "", errors.New("telegram: channel not started")
	}
	f, err := c.bot.GetFile(fileID, &gotgbot.GetFileOpts{
		RequestOpts: c.requestOptsFor("getFile"),
	})
	if err != nil {
		c.recordOutboundErr(err)
		return "", c.scrubTokenf("telegram: GetFile: %w", err)
	}
	if f.FilePath == "" {
		return "", errors.New("telegram: GetFile returned empty file_path (file may be too large or expired)")
	}

	// Size pre-check (cap-aware): the Bot API download ceiling is 20 MiB. The
	// inbound Attachment.Size is not reachable through the channel.Channel
	// DownloadAttachment(fileID) signature, so we pre-check the GetFile result's
	// FileSize (set for most file types) and reject BEFORE streaming the body,
	// with a clear MB-vs-limit message rather than a late copy failure. FileSize
	// can be 0/absent for some kinds; in that case we fall through and rely on the
	// HTTP layer (a 20 MiB+ file won't have produced a FilePath above anyway).
	if f.FileSize > maxDownloadBytes {
		return "", fmt.Errorf("telegram: attachment too large to download (%d MB > %d MB limit)",
			f.FileSize/(1024*1024), int64(maxDownloadBytes)/(1024*1024))
	}

	cacheDir, err := attachmentsCacheDir()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return "", fmt.Errorf("telegram: mkdir cache: %w", err)
	}

	// Local filename: keep the file_unique_id stable across redownloads + the
	// upstream basename for human-friendliness.
	base := filepath.Base(f.FilePath)
	localPath := filepath.Join(cacheDir, fmt.Sprintf("%s_%s", f.FileUniqueId, base))
	if _, err := os.Stat(localPath); err == nil {
		return localPath, nil // cached
	}

	// The download URL contains the bot token; we never include it in
	// any error or log line. The relative file path is enough for
	// debugging. fileDownloadURL builds it against the ACTIVE endpoint (P2) so
	// downloads follow the same reverse proxy as every other call.
	filePath := strings.TrimPrefix(f.FilePath, "/")
	dlURL := c.fileDownloadURL(filePath)
	req, err := http.NewRequestWithContext(c.ctx, http.MethodGet, dlURL, nil)
	if err != nil {
		// net/url errors quote the offending URL, which carries the token.
		return "", c.scrubTokenf("telegram: build download request for %q: %w", filePath, err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// *url.Error prints the FULL request URL — token and all — and this error
		// is handed straight back to the agent as a tool result. Scrub it.
		return "", c.scrubTokenf("telegram: download %q: %w", filePath, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("telegram: download %q: HTTP %d", filePath, resp.StatusCode)
	}

	out, err := os.OpenFile(localPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", fmt.Errorf("telegram: create %s: %w", localPath, err)
	}
	defer out.Close()
	if _, err := io.Copy(out, resp.Body); err != nil {
		_ = os.Remove(localPath)
		return "", fmt.Errorf("telegram: copy to %s: %w", localPath, err)
	}
	return localPath, nil
}

// fileDownloadURL builds the /file/bot<token>/<path> download URL against the
// ACTIVE Bot-API endpoint (P2). This MUST use the configured base — not a
// hardcoded api.telegram.org — or media downloads silently stay on the
// IP-blocked host after a proxy swap. An empty active endpoint (default config)
// falls back to gotgbot.DefaultAPIURL, preserving today's exact behavior. The
// returned URL contains the bot token; NEVER log it.
func (c *Channel) fileDownloadURL(filePath string) string {
	apiBase := strings.TrimSuffix(c.activeEndpointURL(), "/")
	if apiBase == "" {
		apiBase = gotgbot.DefaultAPIURL
	}
	return fmt.Sprintf("%s/file/bot%s/%s", apiBase, c.cfg.BotToken, url.PathEscape(filePath))
}

// CreateTopic creates a new forum topic. Spec §6: rate-limit handling honors
// parameters.retry_after but does NOT silently retry on 429 — instead it
// surfaces the error so the agent can tell the user. Bulk topic creation is
// not a supported flow.
func (c *Channel) CreateTopic(chatID int64, name string) (int64, error) {
	if c.bot == nil {
		return 0, errors.New("telegram: channel not started")
	}
	if err := c.rate.Wait(c.ctx, chatID); err != nil {
		return 0, fmt.Errorf("telegram: rate-wait: %w", err)
	}
	t, err := c.bot.CreateForumTopic(chatID, name, &gotgbot.CreateForumTopicOpts{
		RequestOpts: c.requestOptsFor("createForumTopic"),
	})
	if err != nil {
		c.recordOutboundErr(err)
		return 0, c.scrubTokenf("telegram: CreateForumTopic %q: %w", name, err)
	}
	c.recordOutboundSuccess()
	return t.MessageThreadId, nil
}

// ValidateTopic confirms a topic exists by sending a transient typing action.
// On a real topic this fires a brief typing indicator; on an invalid one
// Telegram returns 400.
func (c *Channel) ValidateTopic(chatID int64, threadID int64) error {
	return c.SendTyping(chatID, &threadID)
}

func attachmentsCacheDir() (string, error) {
	if x := os.Getenv("XDG_CACHE_HOME"); x != "" {
		return filepath.Join(x, "c3", "telegram", "attachments"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("telegram: resolve home: %w", err)
	}
	return filepath.Join(home, ".cache", "c3", "telegram", "attachments"), nil
}
