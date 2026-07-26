package telegram

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Andrometiq/c3/internal/c3types"
)

// sendrich.go — outbound native rich-message tables (Bot API 10.1
// sendRichMessage). All Bot-API-10.1 rich-message wire knowledge lives in this
// file (the no-leak rule R7): the method name, the rich_message payload shape,
// and the rich-table caps never escape the telegram package.
//
// WHY raw, not a generated gotgbot method: no released gotgbot supports Bot API
// 10.1 (latest rc.35 = 10.0; C3 pins rc.34). So we ride gotgbot's PUBLIC raw
// request method — *gotgbot.Bot.RequestWithContext(ctx, method, params, opts) —
// the exact machinery every generated method uses: it reuses C3's token, the
// hardened BaseBotClient transport, the per-method timeout, and the rate limiter
// just like SendReply (outbound.go). This is a forward-compatible bridge: when
// gotgbot ships 10.1 (rc.36+), this swaps to the generated method with NO change
// to C3's callers.
//
// HOW a GFM table becomes a native table: you do NOT build any RichBlock JSON.
// You send the ORIGINAL GFM pipe-table markdown in rich_message.markdown and
// Telegram parses GFM → RichBlockTable itself — header row, `:---`/`:--:`/`---:`
// alignment, and inline **bold** / `code` / ||spoiler|| inside cells all
// survive. The RichBlock* tree is only the RECEIVED representation (inbound, a
// later phase); the send path never constructs it.
//
// ENABLED: the route is gated behind richTablesEnabled (capabilities.go), which
// is true (live-verified 2026-06-17), so detected GFM tables send as native rich
// tables. See SendReply for the routing branch and the monospace fallback (used
// for over-cap tables, send errors, and old clients).

// richTableEligible reports whether a reply should be sent as a native rich
// message (sendRichMessage) instead of the existing monospace path. It is a PURE
// decision (no network, no channel state) so it can be unit-tested directly.
//
// All of these must hold:
//   - rich tables are enabled (richTablesEnabled is true);
//   - the markup intent is markdown (empty/zero value is the markdown default) —
//     a native pass-through (MarkupNative) or plain (MarkupNone) reply is left on
//     the existing path;
//   - the whole reply is within the rich-message char budget (≤ maxRichChars);
//   - the text contains at least one detected GFM pipe table (detectTable);
//   - EVERY detected table is within the rich-table caps (≤ maxRichColumns
//     columns, ≤ maxRichBlocks rows). A table over a cap falls back to the
//     monospace renderer for the whole message — native rendering of an
//     over-cap table would be rejected by Telegram (400), so we never attempt it.
func richTableEligible(enabled bool, markup c3types.Markup, text string) bool {
	if !enabled {
		return false
	}
	// Empty/zero-value Markup is the MARKDOWN DEFAULT (mirrors SendReply).
	if !(markup == c3types.MarkupMarkdown || markup == "") {
		return false
	}
	// A reply over the rich-message char budget would be rejected (400); leave it
	// on the existing path (which chunks before conversion).
	if len(text) > maxRichChars {
		return false
	}
	lines := strings.Split(text, "\n")
	sawTable := false
	for i := 0; i < len(lines); {
		rows, end, ok := detectTable(lines, i)
		if !ok {
			i++
			continue
		}
		sawTable = true
		if !richTableWithinCaps(rows) {
			return false
		}
		i = end
	}
	return sawTable
}

// richTableWithinCaps reports whether a single detected table's parsed rows fit
// the Bot API 10.1 rich-table caps owned by this package: at most maxRichColumns
// columns (the header fixes the column count) and at most maxRichBlocks rows
// (table rows count as rich blocks). Caps live here so no raw limit number leaks
// to core (R7).
func richTableWithinCaps(rows [][]string) bool {
	if len(rows) == 0 {
		return false
	}
	if len(rows) > maxRichBlocks {
		return false
	}
	cols := 0
	for _, r := range rows {
		if len(r) > cols {
			cols = len(r)
		}
	}
	return cols <= maxRichColumns
}

// buildRichParams builds the sendRichMessage request params for the markdown
// dialect. It is PURE (no network) so the param shape is unit-testable. The
// payload is exactly:
//
//	{
//	  "chat_id": <id>,
//	  "message_thread_id": <topic>,        // only when topicID != nil
//	  "rich_message": {"markdown": <md>},  // GFM markdown — Telegram parses it
//	  "reply_parameters": {...},           // only when replyTo != nil
//	}
//
// Markdown dialect only; the html dialect (borders/spans/caption) is a later
// phase.
func buildRichParams(chatID int64, md string, topicID, replyTo *int64) map[string]any {
	params := map[string]any{
		"chat_id":      chatID,
		"rich_message": map[string]any{"markdown": md},
	}
	if topicID != nil {
		params["message_thread_id"] = *topicID
	}
	if replyTo != nil {
		params["reply_parameters"] = map[string]any{
			"message_id":                  *replyTo,
			"allow_sending_without_reply": true,
		}
	}
	return params
}

// sendRich sends ONE reply as a native rich message via sendRichMessage and
// returns the new message_id. It reuses the SAME rate-limit + record-outbound
// pattern as SendReply (outbound.go): rate.Wait → RequestWithContext →
// recordOutboundErr/recordOutboundSuccess, with the per-method timeout from
// requestOptsFor.
//
// The reply text is sent UNCHANGED as GFM markdown (rich_message.markdown);
// Telegram converts the pipe table(s) into native RichBlockTable(s). On any
// error the caller (SendReply) falls back to the existing monospace path so a
// message is never lost.
func (c *Channel) sendRich(args c3types.ReplyArgs) (int64, error) {
	if c.bot == nil {
		return 0, errors.New("telegram: channel not started")
	}
	params := buildRichParams(args.ChatID, args.Text, args.TopicID, args.ReplyTo)
	if err := c.rate.Wait(c.ctx, args.ChatID); err != nil {
		return 0, fmt.Errorf("telegram: rate-wait: %w", err)
	}
	raw, err := c.bot.RequestWithContext(
		c.ctx,
		"sendRichMessage",
		params,
		c.requestOptsFor("sendRichMessage"),
	)
	if err != nil {
		c.recordOutboundErr(err)
		return 0, fmt.Errorf("telegram: sendRichMessage: %w", err)
	}
	var res struct {
		MessageId int64 `json:"message_id"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		// A success response we can't decode is still an error for the caller's
		// fallback — record it so the breaker doesn't see a false success.
		c.recordOutboundErr(err)
		return 0, fmt.Errorf("telegram: sendRichMessage: decode result: %w", err)
	}
	c.recordOutboundSuccess()
	return res.MessageId, nil
}

// buildRichHTMLParams builds the sendRichMessage request params for the HTML
// dialect (the voice-readback LONG band, readback.go). It mirrors
// buildRichParams but carries rich_message.html instead of .markdown:
//
//	{
//	  "chat_id": <id>,
//	  "message_thread_id": <topic>,    // only when topicID != nil
//	  "rich_message": {"html": <html>},
//	  "reply_parameters": {...},       // only when replyTo != nil
//	}
//
// PURE (no network) so the param shape is unit-testable.
func buildRichHTMLParams(chatID int64, html string, topicID, replyTo *int64) map[string]any {
	params := map[string]any{
		"chat_id":      chatID,
		"rich_message": map[string]any{"html": html},
	}
	if topicID != nil {
		params["message_thread_id"] = *topicID
	}
	if replyTo != nil {
		params["reply_parameters"] = map[string]any{
			"message_id":                  *replyTo,
			"allow_sending_without_reply": true,
		}
	}
	return params
}

// sendRichHTML sends ONE rich message in the HTML dialect (rich_message.html)
// and returns the new message_id. It is the SIBLING of sendRich used by the
// voice-readback LONG band (readback.go): IDENTICAL rate.Wait →
// RequestWithContext("sendRichMessage") → recordOutboundErr/Success machinery,
// only the dialect differs (html vs markdown). sendRich (the GFM-table path) and
// richTableEligible are intentionally left byte-identical — this is an additive
// sibling, not a change to the table route.
func (c *Channel) sendRichHTML(chatID int64, html string, topicID, replyTo *int64) (int64, error) {
	if c.bot == nil {
		return 0, errors.New("telegram: channel not started")
	}
	params := buildRichHTMLParams(chatID, html, topicID, replyTo)
	if err := c.rate.Wait(c.ctx, chatID); err != nil {
		return 0, fmt.Errorf("telegram: rate-wait: %w", err)
	}
	raw, err := c.bot.RequestWithContext(
		c.ctx,
		"sendRichMessage",
		params,
		c.requestOptsFor("sendRichMessage"),
	)
	if err != nil {
		c.recordOutboundErr(err)
		return 0, fmt.Errorf("telegram: sendRichMessage(html): %w", err)
	}
	var res struct {
		MessageId int64 `json:"message_id"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		c.recordOutboundErr(err)
		return 0, fmt.Errorf("telegram: sendRichMessage(html): decode result: %w", err)
	}
	c.recordOutboundSuccess()
	return res.MessageId, nil
}
