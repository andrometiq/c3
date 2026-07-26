package broker

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"unicode/utf16"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/capability"
	"github.com/Andrometiq/c3/internal/channel"
)

// dispatchTool translates a tool-call (name, args) into a channel method call.
// Returns the MCP-shape result map (with content[].text or attachment_path
// fields) and any error. The destination is ALWAYS the claimed route key; a
// tool call cannot address anything else (see rejectDestinationOverride).
func dispatchTool(ch channel.Channel, key RouteKey, tool string, args map[string]any) (res map[string]any, err error) {
	// SECURITY AUDIT: every agent-initiated outbound records the destination it
	// actually went to. dispatchTool is the single funnel for every tool call, and
	// the destination below is structurally the claimed route — this line is the
	// evidence. Without it a plain text send leaves NO trace: logAlterations only
	// writes when the gate altered something, and the media-send line needs a
	// local Path, so a text-only exfiltration is invisible in the log. Metadata
	// only, never message text (log.go content policy).
	defer func() {
		log.Printf("outbound-dispatch chan=%s chat=%d topic=%s tool=%s ok=%t",
			key.Channel, key.ChatID, TopicKeyStr(key), tool, err == nil)
	}()
	if derr := rejectDestinationOverride(key, args); derr != nil {
		return nil, derr
	}
	switch tool {
	case "reply":
		return dispatchReply(ch, key, args)
	case "react":
		return dispatchReact(ch, key, args)
	case "edit_message":
		return dispatchEditMessage(ch, key, args)
	case "send_typing":
		return dispatchSendTyping(ch, key, args)
	case "poll":
		return dispatchPoll(ch, key, args)
	case "stop_poll":
		return dispatchStopPoll(ch, key, args)
	case "download_attachment":
		return dispatchDownloadAttachment(ch, args)
	default:
		return nil, fmt.Errorf("unknown tool %q", tool)
	}
}

func dispatchReply(ch channel.Channel, key RouteKey, args map[string]any) (map[string]any, error) {
	out := c3types.Outbound{
		Channel: key.Channel,
		ChatID:  key.ChatID,
		TopicID: routeTopicID(key),
		Text:    argString(args, "text", ""),
		// The agent writes standard markdown; C3 converts + escapes it for the
		// channel. Markup is always the markdown intent (MarkupNative remains a
		// dormant internal escape hatch in the type system but is not agent-
		// selectable in v1).
		Markup: c3types.MarkupMarkdown,
		Media:  mediaFromArgs(args),
	}
	if rt := argInt64Ptr(args, "reply_to"); rt != nil {
		out.ReplyTo = rt
	}
	buttons, err := buttonsFromArgs(args)
	if err != nil {
		return nil, err
	}
	out.Buttons = buttons

	// Route through the pure capability gate: it validates (hard-reject), down-
	// converts (e.g. markup→none), and splits the text into ordered parts that
	// each fit the channel's limits. A non-nil err is a HARD REJECTION — surface
	// it to the agent without sending anything (matching tool-error surfacing).
	parts, notes, alts, err := capability.Gate(ch.Capabilities(), out)
	if err != nil {
		return nil, err
	}

	// The gate is pure; dispatch (impure) writes the durable alteration log.
	logAlterations(key, out.ChatID, alts)

	return sendParts(ch, key, parts, notes)
}

// logAlterations writes the durable outbound-alteration log line for each
// structured Alteration the pure gate returned.
func logAlterations(key RouteKey, chatID int64, alts []c3types.Alteration) {
	for _, a := range alts {
		log.Printf("outbound-alteration chan=%s chat=%d topic=%s kind=%s detail=%q",
			key.Channel, chatID, TopicKeyStr(key), a.Kind, a.Detail)
	}
}

// sendParts implements the multi-part send contract: send parts sequentially,
// in order; on part-k failure STOP (fail-fast) and report exactly how many
// landed. NEVER report silent success. The agent-visible id is the FIRST part's
// id; reply-threading/edits target the first part. Shared by dispatchReply and
// dispatchPoll (a poll rides a single part).
func sendParts(ch channel.Channel, key RouteKey, parts []c3types.Outbound, notes []string) (map[string]any, error) {
	var firstID int64
	for i, part := range parts {
		// SECURITY AUDIT (2026-06-15): record the absolute path of every local
		// file being sent out to a chat. Under the trusted-local-agent model the
		// media `path` arg may reference ANY broker-readable file (the bot token,
		// ssh keys, …), so a prompt-injected agent could exfiltrate secrets to the
		// chat. That is accepted by the threat model but MUST be detectable — this
		// log line is the audit trail. URL-only media (no Path) is not logged here.
		for _, m := range part.Media {
			if m.Path == "" {
				continue
			}
			abs, err := filepath.Abs(m.Path)
			if err != nil {
				abs = m.Path // best-effort: log the raw path if Abs fails
			}
			log.Printf("media-send chan=%s chat=%d topic=%s kind=%s path=%q",
				key.Channel, part.ChatID, TopicKeyStr(key), m.Kind, abs)
		}
		id, err := ch.SendReply(part)
		if i == 0 {
			firstID = id
		}
		if err != nil {
			if i == 0 {
				return nil, fmt.Errorf("send failed: %w", err)
			}
			// Some parts already landed: tell the agent precisely how far we got.
			return nil, fmt.Errorf("partial send: sent %d of %d; part %d failed: %w",
				i, len(parts), i+1, err)
		}
	}

	result := fmt.Sprintf("sent (id: %d)", firstID)
	if len(parts) > 1 {
		result = fmt.Sprintf("sent %d messages (first id: %d)", len(parts), firstID)
	}
	if len(notes) > 0 {
		result += "\n" + strings.Join(notes, "\n")
	}
	return mcpText(result), nil
}

// dispatchPoll builds an Outbound carrying a PollSpec from the `poll` tool args,
// runs it through the pure capability gate (so a channel without poll support is
// hard-rejected with the agent note), and sends it via the unified parts loop —
// the poll rides a single part → SendReply → sendPoll.
func dispatchPoll(ch channel.Channel, key RouteKey, args map[string]any) (map[string]any, error) {
	question := argString(args, "question", "")
	if question == "" {
		return nil, fmt.Errorf("poll: question required")
	}
	// Option-count and full poll-shape validation (incl. quiz/timed rules) live
	// in the pure capability gate — dispatch just builds the spec and surfaces
	// the gate's hard-rejection error. Do NOT re-check options here (that check
	// moved to the gate so it cannot diverge).
	out := c3types.Outbound{
		Channel: key.Channel,
		ChatID:  key.ChatID,
		TopicID: routeTopicID(key),
		Poll: &c3types.PollSpec{
			Question:        question,
			Options:         argStringSlice(args, "options"),
			Anonymous:       argBool(args, "anonymous", true),
			MultipleAnswers: argBool(args, "multiple", false),
			Kind:            c3types.PollKind(argString(args, "type", string(c3types.PollRegular))),
			CorrectOption:   argIntPtr(args, "correct_option"),
			Explanation:     argString(args, "explanation", ""),
			OpenPeriodSec:   int(argInt64(args, "open_period", 0)),
			CloseDateUnix:   argInt64(args, "close_date", 0),
		},
	}

	// Gate validates (hard-reject when !Polls) + emits a single poll part.
	parts, notes, alts, err := capability.Gate(ch.Capabilities(), out)
	if err != nil {
		return nil, err
	}
	logAlterations(key, out.ChatID, alts)
	return sendParts(ch, key, parts, notes)
}

// dispatchStopPoll force-closes a bot-sent poll and returns its final aggregate
// tally as MCP text. This is the deterministic read path (the passive `poll`
// update only arrives on close). message_id identifies the original poll message
// — the agent gets it back from the `poll` tool's send result. The tool is gated
// at registration on caps.Polls; here we surface the channel error verbatim if
// the channel can't stop the poll (e.g. not the bot's poll, wrong message id).
func dispatchStopPoll(ch channel.Channel, key RouteKey, args map[string]any) (map[string]any, error) {
	messageID := argInt64(args, "message_id", 0)
	if messageID == 0 {
		return nil, fmt.Errorf("stop_poll: message_id required")
	}
	res, err := ch.StopPoll(key.ChatID, messageID)
	if err != nil {
		return nil, err
	}
	return mcpText(formatPollResult(res)), nil
}

// formatPollResult renders a PollResult tally into a compact human-readable line
// for the stop_poll MCP return. Aggregate only (no per-voter identity).
func formatPollResult(r *c3types.PollResult) string {
	if r == nil {
		return "poll stopped (no tally returned)"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Poll results: %q — %d vote", r.Question, r.TotalVoters)
	if r.TotalVoters != 1 {
		b.WriteString("s")
	}
	if r.IsClosed {
		b.WriteString(" (closed)")
	}
	for _, o := range r.Options {
		fmt.Fprintf(&b, "\n  %s: %d", o.Text, o.VoterCount)
	}
	return b.String()
}

// mediaFromArgs parses the reply tool's `media` array arg into channel-neutral
// MediaItems. `media` is a JSON array of objects: {kind, path, url, caption,
// spoiler}. An item with neither path nor url, or with an empty/unknown kind, is
// skipped here; the channel surfaces a clear send error for any genuinely bad
// item that slips through. Kind defaults to "file" (byte-for-byte original) when
// omitted.
func mediaFromArgs(args map[string]any) []c3types.MediaItem {
	var out []c3types.MediaItem

	raw, ok := args["media"]
	if !ok {
		return nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil
	}
	for _, v := range list {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		path := argString(m, "path", "")
		urlStr := argString(m, "url", "")
		if path == "" && urlStr == "" {
			continue
		}
		kind := c3types.MediaKind(argString(m, "kind", string(c3types.MediaFile)))
		if kind == "" {
			kind = c3types.MediaFile
		}
		item := c3types.MediaItem{
			Kind:    kind,
			Path:    path,
			URL:     urlStr,
			Caption: argString(m, "caption", ""),
		}
		if sp, ok := m["spoiler"].(bool); ok {
			item.Spoiler = sp
		}
		out = append(out, item)
	}
	return out
}

// c3InternalCallbackPrefix is the callback_data namespace of C3's own inert
// keyboard indicator (callbackChosenData = "c3:_chosen", internal/channel/
// telegram/poll.go): a tap there is swallowed by the channel and never surfaced
// as an event, so agent data must not land in it either. Reserved as a whole
// namespace, not as the single literal, so a future C3-internal callback needs
// no second fix here.
const c3InternalCallbackPrefix = "c3:"

// reservedCallbackPrefixes are the callback_data namespaces C3 OWNS. The
// inbound side routes a tap by prefix to a C3 resolver instead of to the agent —
// "perm:" → resolvePerm (worker.go), "ask:" → resolveAsk (worker.go), "c3:" →
// swallowed by the channel (poll.go) — and resolvePerm treats such a tap as the
// human's tool-use verdict. An agent-authored button must therefore never mint
// one.
var reservedCallbackPrefixes = []string{permCallbackPrefix, askCallbackPrefix, c3InternalCallbackPrefix}

// buttonsFromArgs parses the reply tool's `buttons` arg into a channel-neutral
// inline keyboard ([][]c3types.Button — ROWS of buttons). `buttons` is a JSON
// 2-D array: an array of rows, each row an array of {text, data, url} objects.
// Shape is validated here (mirroring mediaFromArgs' helper style) with a clear
// error rather than a silent drop: each button needs a non-empty `text` and
// EXACTLY ONE of `data` (a callback button) or `url` (a link button). Returns
// (nil, nil) when the arg is absent or empty — a reply with no buttons is
// byte-identical to today. Channel-specific limits (callback-data byte ceiling,
// max rows) are NOT checked here; the channel enforces those.
func buttonsFromArgs(args map[string]any) ([][]c3types.Button, error) {
	raw, ok := args["buttons"]
	if !ok {
		return nil, nil
	}
	rows, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("buttons must be an array of rows (each row an array of buttons)")
	}
	out := make([][]c3types.Button, 0, len(rows))
	for ri, r := range rows {
		rowAny, ok := r.([]any)
		if !ok {
			return nil, fmt.Errorf("buttons row %d must be an array of buttons", ri+1)
		}
		// An empty row (`[]`) is rejected, not silently sent: Telegram 400s on an
		// empty keyboard row, and `[[]]` would otherwise pass as a "keyboard" with
		// no actionable buttons.
		if len(rowAny) == 0 {
			return nil, fmt.Errorf("buttons row %d is empty", ri+1)
		}
		row := make([]c3types.Button, 0, len(rowAny))
		for bi, v := range rowAny {
			m, ok := v.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("buttons row %d position %d must be an object {text, data|url}", ri+1, bi+1)
			}
			text := argString(m, "text", "")
			if text == "" {
				return nil, fmt.Errorf("buttons row %d position %d: text is required", ri+1, bi+1)
			}
			data := argString(m, "data", "")
			urlStr := argString(m, "url", "")
			if (data == "") == (urlStr == "") {
				return nil, fmt.Errorf("button %q: set EXACTLY ONE of data (callback) or url (link)", text)
			}
			// The inbound side routes a tap by prefix, so a reserved prefix here
			// would let the agent mint a payload C3 hands to its own resolver — and
			// even without that, such a tap is swallowed and never reaches the agent.
			// REFUSE, don't rewrite: the tap echoes Data back verbatim, so a silent
			// escape would break the agent's own comparison of the value.
			for _, p := range reservedCallbackPrefixes {
				if strings.HasPrefix(data, p) {
					return nil, fmt.Errorf("button %q: callback data %q starts with %q, a prefix C3 reserves for its own taps (permission Allow/Deny, ask answers, keyboard state) — a tap there is routed to C3, not to you; use a different prefix", text, data, p)
				}
			}
			row = append(row, c3types.Button{Text: text, Data: data, URL: urlStr})
		}
		out = append(out, row)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func dispatchReact(ch channel.Channel, key RouteKey, args map[string]any) (map[string]any, error) {
	a := c3types.ReactArgs{
		Channel:   key.Channel,
		ChatID:    key.ChatID,
		MessageID: argInt64(args, "message_id", 0),
		Emoji:     argString(args, "emoji", ""),
	}
	if err := ch.React(a); err != nil {
		return nil, err
	}
	return mcpText("reacted"), nil
}

func dispatchEditMessage(ch channel.Channel, key RouteKey, args map[string]any) (map[string]any, error) {
	// Edits join the markup system: the agent writes standard markdown and C3
	// converts it (MarkupNative stays a dormant internal escape hatch, not agent-
	// selectable in v1).
	markup := c3types.MarkupMarkdown
	caps := ch.Capabilities()
	text := argString(args, "text", "")

	// Edits join the markup system but are SINGLE messages — they cannot be
	// split. Apply the same markup degradation the gate would (!RichText→none)
	// so an edit renders consistently with a reply, but reject (don't split) an
	// over-limit edit because there is no second message to overflow into.
	notes := []string{}
	if !caps.RichText && markup != c3types.MarkupNone {
		markup = c3types.MarkupNone
		notes = append(notes, "rich text is not supported on this channel — markdown was sent as plain text")
		log.Printf("outbound-alteration chan=%s chat=%d topic=%s kind=markup_degraded detail=%q",
			key.Channel, key.ChatID, TopicKeyStr(key),
			"edit markup downgraded to none (channel RichText=false)")
	}
	if caps.MaxMessageRunes > 0 && utf16Len(text) > caps.MaxMessageRunes {
		return nil, fmt.Errorf("edit text is %d chars, over the channel's %d-char limit; an edit is a single message and cannot be split — shorten it or send a new reply instead",
			utf16Len(text), caps.MaxMessageRunes)
	}

	// An edit carries no Buttons and therefore DROPS any inline keyboard on the
	// target message. c3types.EditArgs documents a nil Buttons as "leave the
	// existing keyboard untouched", but that behaviour is not expressible through
	// the Telegram client: EditMessageText assigns reply_markup unconditionally
	// from a CONCRETE struct whose only slice carries omitempty, so a nil and an
	// empty keyboard marshal to the identical `reply_markup={}` — the very bytes
	// C3 itself sends to CLEAR a keyboard when a perm/ask resolves. An edit that
	// must keep a keyboard has to re-send it.
	a := c3types.EditArgs{
		Channel:   key.Channel,
		ChatID:    key.ChatID,
		MessageID: argInt64(args, "message_id", 0),
		Text:      text,
		Markup:    markup,
	}
	r, err := ch.EditMessage(a)
	if err != nil {
		return nil, err
	}
	result := fmt.Sprintf("edited (id: %d)", r.MessageID)
	if len(notes) > 0 {
		result += "\n" + strings.Join(notes, "\n")
	}
	return mcpText(result), nil
}

// utf16Len counts a string's length in UTF-16 code units — the unit channels
// like Telegram measure message length in. Used for the edit-over-limit check;
// the multi-part reply path measures inside the pure gate.
func utf16Len(s string) int {
	return len(utf16.Encode([]rune(s)))
}

func dispatchSendTyping(ch channel.Channel, key RouteKey, args map[string]any) (map[string]any, error) {
	if err := ch.SendTyping(key.ChatID, routeTopicID(key)); err != nil {
		return nil, err
	}
	return mcpText("typing"), nil
}

func dispatchDownloadAttachment(ch channel.Channel, args map[string]any) (map[string]any, error) {
	fid := argString(args, "file_id", "")
	if fid == "" {
		return nil, fmt.Errorf("download_attachment: file_id required")
	}
	path, err := ch.DownloadAttachment(fid)
	if err != nil {
		return nil, err
	}
	return mcpText(path), nil
}

// mcpText returns the standard MCP-shape result with one text entry.
func mcpText(s string) map[string]any {
	return map[string]any{
		"content": []map[string]any{
			{"type": "text", "text": s},
		},
	}
}

// argString returns the string value at key, or fallback. Tolerates non-string
// values (e.g. int) by stringifying.
func argString(args map[string]any, key, fallback string) string {
	if v, ok := args[key]; ok {
		switch x := v.(type) {
		case string:
			return x
		case fmt.Stringer:
			return x.String()
		default:
			return fmt.Sprintf("%v", x)
		}
	}
	return fallback
}

// argBool returns the bool value at key, or fallback. Accepts a real bool and
// the string forms "true"/"false".
func argBool(args map[string]any, key string, fallback bool) bool {
	v, ok := args[key]
	if !ok {
		return fallback
	}
	switch x := v.(type) {
	case bool:
		return x
	case string:
		switch x {
		case "true":
			return true
		case "false":
			return false
		}
	}
	return fallback
}

// argStringSlice returns the []string at key. Accepts a JSON array of strings
// (the []any form json.Unmarshal produces); non-string elements are skipped.
// Returns nil when the key is absent or not an array.
func argStringSlice(args map[string]any, key string) []string {
	raw, ok := args[key]
	if !ok {
		return nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, v := range list {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// argInt64 returns the int64 value at key, or fallback. Accepts float64
// (json default for numbers) and string forms.
func argInt64(args map[string]any, key string, fallback int64) int64 {
	v, ok := args[key]
	if !ok {
		return fallback
	}
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		var n int64
		_, err := fmt.Sscanf(x, "%d", &n)
		if err == nil {
			return n
		}
	}
	return fallback
}

// argInt64Ptr returns &v if args[key] is set and parseable; nil otherwise.
func argInt64Ptr(args map[string]any, key string) *int64 {
	if _, ok := args[key]; !ok {
		return nil
	}
	v := argInt64(args, key, 0)
	return &v
}

// argIntPtr returns &v ONLY when args[key] is present AND actually parses to an
// int; nil otherwise. Used for nullable int args (e.g. a quiz poll's
// correct_option, where 0 is a valid index and must be distinguishable from "not
// set"). A present-but-unparseable value (JSON null, a non-numeric string) must
// return nil — NOT &0 — so it is not silently treated as option 0; the downstream
// validatePoll then surfaces "quiz requires correct_option" instead of marking
// the first option correct by accident.
func argIntPtr(args map[string]any, key string) *int {
	v, ok := args[key]
	if !ok {
		return nil
	}
	var n int
	switch x := v.(type) {
	case int64:
		n = int(x)
	case int:
		n = x
	case float64:
		n = int(x)
	case string:
		var parsed int64
		if _, err := fmt.Sscanf(x, "%d", &parsed); err != nil {
			return nil
		}
		n = int(parsed)
	default:
		// JSON null, bool, nested object/array — not a parseable int.
		return nil
	}
	return &n
}

// routeTopicID renders the CLAIMED route's topic as the *int64 the outbound
// types use. nil = no topic (a DM or a non-forum group).
func routeTopicID(route RouteKey) *int64 {
	if !route.HasTopic {
		return nil
	}
	v := route.TopicID
	return &v
}

// rejectDestinationOverride enforces that a tool call goes to the route the
// session actually claimed. chat_id/topic_id are in NO tool schema and no
// in-tree caller sets them, yet dispatch used to honour them — so a prompt-
// injected agent could post into, react in or edit ANOTHER session's topic (the
// `topics` tool hands it every chat/thread pair), and a `topic_id: 0` escaped a
// forum topic into the group's General chat with no reconnaissance at all.
// Refused on PRESENCE, not on disagreement: argInt64 falls back to the route's
// own id for an unparseable value (JSON null, a bool, a string), so a
// disagreement check would silently ACCEPT `chat_id: null`. Loud, so a
// third-party adapter that sets one finds out instead of sending to the wrong
// place. The house pattern is handleRetranscribe (queue_dispatch.go), which
// derives chan/chat/topic from the claimed route rather than from args.
// Metadata only in the log — never message text (log.go content policy).
func rejectDestinationOverride(key RouteKey, args map[string]any) error {
	for _, f := range []string{"chat_id", "topic_id"} {
		if _, ok := args[f]; ok {
			log.Printf("outbound-destination-refused chan=%s chat=%d topic=%s field=%s",
				key.Channel, key.ChatID, TopicKeyStr(key), f)
			return fmt.Errorf("%s is not a tool argument: a tool call always goes to the route this session attached to", f)
		}
	}
	return nil
}
