package telegram

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/karthikeyan5/c3/internal/c3types"
)

// readback.go — the Go home of the voice-transcript "readback" that the Python
// STT handler used to send itself (send_transcript_to_telegram). After STT
// succeeds, the broker calls SendReadback (via an OPTIONAL interface — only this
// channel implements it; see internal/broker/worker.go) to echo the transcript
// back to the source chat as ONE message. It is ADDITIVE and NON-FATAL: it is a
// SEND, so it can never affect inbound delivery, persistence, or loss-freedom,
// and every send error here is best-effort.
//
// It REUSES the channel's existing senders — sendMessage (TINY/SHORT bands),
// sendRichMessage via sendRichHTML (LONG/DEADZONE bands), and SendDocument (HUGE
// band) — and never writes raw HTTP. The FROZEN render format (locked with the maintainer
// 2026-06-30): a summary preview on top → "Full Transcript" heading → the WHOLE
// verbatim transcript — a normal message with an expandable blockquote when it
// fits one message (≤4096 UTF-16), a PLAIN rich message with Telegram's native
// "Show More" when much longer (DISPLAYED >9000, up to ~32k assembled), a rich
// message with a searchable <details> collapse for the 4096–9000 dead zone (no
// native "Show More" there), and a .txt document only for anything huge. The
// transcript is NEVER truncated or summarized; only the preview elides the middle.

// readbackBand is the render band chosen by the transcript's DISPLAYED length.
type readbackBand int

const (
	// bandTiny — too few sentences for a meaningful middle elision: the bare
	// header + the whole escaped transcript, no summary/heading/collapse.
	bandTiny readbackBand = iota
	// bandShort — summary + heading + the whole transcript in an expandable
	// blockquote, when the DISPLAYED message fits one sendMessage (≤4096 UTF-16).
	bandShort
	// bandLong — a PLAIN rich message (no blockquote) that Telegram's native
	// "Show More" collapses once content exceeds ~8000 chars, when the SHORT
	// message's DISPLAYED length is past the 9000 native-collapse margin but the
	// assembled rich HTML still fits the rich budget.
	bandLong
	// bandDeadzone — the 4096 < DISPLAYED ≤ 9000 window: too long for one
	// sendMessage, too short for Telegram's native "Show More". Renders as a rich
	// message that wraps the whole transcript in a searchable <details> collapse.
	bandDeadzone
	// bandHuge — the .txt document fallback: a transcript whose assembled rich HTML
	// exceeds the rich budget (>32k), plus the last-resort target when any of the
	// above send paths errors. (The 4096–9000 dead zone is now bandDeadzone.)
	bandHuge
)

func (b readbackBand) String() string {
	switch b {
	case bandTiny:
		return "tiny"
	case bandShort:
		return "short"
	case bandLong:
		return "long"
	case bandDeadzone:
		return "deadzone"
	case bandHuge:
		return "huge"
	default:
		return "unknown"
	}
}

const (
	// readbackTinyMaxSentences — a transcript with fewer than this many sentences
	// renders as the bare TINY band (no preview/elision/collapse). The preview
	// shows first 3 + last 3 = 6 sentences; below 2× that (≤12) the elided preview
	// would reveal as much as it hides, so only >12 sentences get the elided
	// preview. 13 = first count at which the middle (= total − 6) exceeds the 6 shown.
	readbackTinyMaxSentences = 13
	// readbackTinyMaxU16 — the DISPLAYED-length ceiling (UTF-16 units) below which a
	// transcript renders as the bare TINY band REGARDLESS of sentence count: a short
	// note that fits on one screen must show the whole verbatim text with no middle
	// elision, even when it has many short sentences. Tunable.
	readbackTinyMaxU16 = 1000
	// readbackPreviewPartMaxU16 — the per-part hard cap (UTF-16 units) on the preview
	// head (first 3) and tail (last 3): a few very long, rambly sentences are elided
	// with '…' so the summary can't fill the screen — the head loses its END, the tail
	// (last visible line) its START, so the preview never trails off. Tunable.
	readbackPreviewPartMaxU16 = 220
	// readbackShortMaxU16 — the SHORT band's DISPLAYED-length ceiling in UTF-16
	// code units (Telegram's per-message cap). The measurement strips tags and
	// counts entities as their visible character, so a fit here cannot 400.
	readbackShortMaxU16 = 4096
	// readbackNativeMinU16 — the LONG band's DISPLAYED-length floor in UTF-16 code
	// units. Telegram's native rich-message "Show More" appears once content
	// exceeds ~8000 chars (Telegram's blog); 9000 is a safe margin. The
	// 4096 < x ≤ 9000 window has no native "Show More" (too long for one message,
	// too short for native collapse) → it renders as the bandDeadzone <details>
	// collapse rich message instead.
	readbackNativeMinU16 = 9000
	// readbackRichMaxBytes — the LONG band's assembled-HTML budget in UTF-8 bytes.
	// Conservative below the real 32768 sendRichMessage cap (over which Telegram
	// 400s with RICH_MESSAGE_TEXT_TOO_LONG); over this → the .txt document band.
	readbackRichMaxBytes = 32000
	// readbackCaptionMaxU16 — the .txt document caption ceiling (UTF-16 units),
	// matching Telegram's 1024 caption cap.
	readbackCaptionMaxU16 = 1024
	// readbackCaptionPreviewBudget — the visible-preview budget folded into the
	// .txt caption BEFORE HTML escaping, kept well under readbackCaptionMaxU16 so
	// the escaped caption (plus the fixed header) stays under the 1024 cap for
	// ordinary speech.
	readbackCaptionPreviewBudget = 700
)

// readbackSentenceRe matches a sentence-ending punctuation rune (Latin
// .!?, Devanagari danda ।/॥, ellipsis …) followed by whitespace. It is the
// Go port of the Python W4 splitter `(?<=[.!?।॥…])\s+`: RE2 has no lookbehind,
// so splitSentences ends each sentence after the punctuation rune and consumes
// the trailing whitespace itself.
var readbackSentenceRe = regexp.MustCompile(`[.!?…।॥]\s+`)

// splitSentences is a pragmatic, multilingual-ish sentence split. Preview-only —
// imperfect is fine, because the openable/attached full text is always the
// verbatim original. Each sentence KEEPS its trailing punctuation; only the
// inter-sentence whitespace is dropped.
func splitSentences(t string) []string {
	t = strings.TrimSpace(t)
	if t == "" {
		return nil
	}
	var out []string
	last := 0
	for _, loc := range readbackSentenceRe.FindAllStringIndex(t, -1) {
		// loc[0] starts at the single punctuation rune; end the sentence right
		// after that rune (mirrors the lookbehind: punctuation stays with the
		// sentence, the matched whitespace is consumed).
		_, size := utf8.DecodeRuneInString(t[loc[0]:])
		end := loc[0] + size
		if seg := strings.TrimSpace(t[last:end]); seg != "" {
			out = append(out, seg)
		}
		last = loc[1]
	}
	if last < len(t) {
		if seg := strings.TrimSpace(t[last:]); seg != "" {
			out = append(out, seg)
		}
	}
	return out
}

// buildPreview returns the summary's first-3-sentences string, last-3-sentences
// string, and M (= total − 6, the count of elided middle sentences) for the
// SHORT/LONG bands. For fewer than readbackTinyMaxSentences sentences there is
// no meaningful elision: it returns all sentences joined as first3, an empty
// last3, and M = 0 (the caller uses the TINY band, or a whole-text caption).
func buildPreview(sents []string) (first3, last3 string, more int) {
	n := len(sents)
	if n < readbackTinyMaxSentences {
		return strings.Join(sents, " "), "", 0
	}
	return strings.Join(sents[:3], " "), strings.Join(sents[n-3:], " "), n - 6
}

// uint16Len returns the UTF-16 code-unit length of s — the unit Telegram counts
// (an astral rune like 🎤 is a surrogate pair = 2 units, where Go's len() counts
// bytes and a []rune counts 1). Mirrors media.go's captionUTF16Len; kept here as
// the readback band-selection measure (and a directly unit-tested pure helper).
func uint16Len(s string) int {
	return len(utf16.Encode([]rune(s)))
}

// htmlEscape escapes dynamic text for Telegram HTML: '&' FIRST (so we never
// double-escape), then '<' and '>'. Tags in the readback templates are
// intentional and are NOT passed through this. Same displayed output as
// format.go's escapeText — duplicated as a small, directly-tested pure helper.
func htmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

// capUTF16 truncates s to at most n UTF-16 code units on rune boundaries,
// keeping the START and appending a trailing '…' when it cut. Used for the
// preview HEAD (which reads as the start of the note trailing off) and to keep
// the .txt caption preview under Telegram's caption cap.
func capUTF16(s string, n int) string {
	if uint16Len(s) <= n {
		return s
	}
	runes := []rune(s)
	for len(runes) > 0 && uint16Len(string(runes))+1 > n {
		runes = runes[:len(runes)-1]
	}
	return string(runes) + "…"
}

// capUTF16Tail truncates s to at most n UTF-16 code units on rune boundaries,
// keeping the END (the tail) and prepending a LEADING '…' when it cut — the
// mirror of capUTF16. Used for the preview's LAST visible line so it reads as
// the natural end of the sentence and NEVER ends on a trailing '…' (an end-cut
// there looked chopped mid-sentence). Rune-safe: an astral rune (surrogate pair)
// is dropped whole, so this never splits a multi-byte character or emoji.
func capUTF16Tail(s string, n int) string {
	if uint16Len(s) <= n {
		return s
	}
	runes := []rune(s)
	for len(runes) > 0 && uint16Len(string(runes))+1 > n {
		runes = runes[1:]
	}
	return "…" + string(runes)
}

// capPreviewParts hard-caps the preview head (f3) and tail (l3) at
// readbackPreviewPartMaxU16 UTF-16 units so a few very long, rambly sentences
// can't fill the screen (BUG B). The two parts are elided from OPPOSITE ends so
// the summary reads naturally and its visible text never ends on a trailing '…':
//   - f3 is the HEAD — elide its END (trailing '…'): it reads as the start of the
//     note trailing off before the "✂️ N more sentences ✂️" gap.
//   - l3 is the TAIL and the preview's LAST visible line — keep its END and elide
//     its START (leading '…'): it reads as the natural end of that sentence.
//
// When more == 0 there is no elided middle or separate tail: f3 IS the whole
// preview and its own last visible line, so it too keeps its END (leading '…')
// rather than trailing off. Caps the UNescaped text so every assembly escapes
// once (entity-safe, like the caption); capUTF16/capUTF16Tail are no-ops when a
// part already fits, so this never double-elides.
func capPreviewParts(f3, l3 string, more int) (string, string) {
	if more == 0 {
		return capUTF16Tail(f3, readbackPreviewPartMaxU16), l3
	}
	return capUTF16(f3, readbackPreviewPartMaxU16), capUTF16Tail(l3, readbackPreviewPartMaxU16)
}

// renderReadback chooses the band and builds the EXACT string to send for a
// transcript, with NO network — so band selection, the preview elision, the
// measurement, and the escaping are all unit-testable directly. It returns the
// Telegram API method name, the payload to send, and the band:
//   - bandTiny     → ("sendMessage",     HTML message text)
//   - bandShort    → ("sendMessage",     HTML message text)
//   - bandLong     → ("sendRichMessage", rich HTML)
//   - bandDeadzone → ("sendRichMessage", rich HTML with a <details> collapse)
//   - bandHuge     → ("sendDocument",    "" — the caller builds the .txt + caption)
func renderReadback(transcript string) (method, payload string, band readbackBand) {
	full := strings.TrimSpace(transcript)
	sents := splitSentences(full)
	words := len(strings.Fields(full))

	// TINY — the whole transcript displayed verbatim (header + escaped transcript),
	// with no summary/elision/collapse. Chosen when EITHER there are too few sentences
	// for a meaningful middle elision (< readbackTinyMaxSentences) OR the whole
	// DISPLAYED message is short enough to sit on one screen (≤ readbackTinyMaxU16),
	// REGARDLESS of sentence count — a short note of many short sentences fits and must
	// not get the middle elision. BOTH tiny paths are guarded by the sendMessage hard
	// cap: if the DISPLAYED tiny message would exceed readbackShortMaxU16 (Telegram's
	// 4096 post-parse limit), fall through to the length-sized band logic rather than
	// emit an oversized sendMessage — which also closes a latent bug where a
	// few-but-huge-sentence note used to emit an over-4096 TINY message. The visible
	// header mirrors the payload header with its <b> tags stripped (tags cost 0).
	tinyDisplayedU16 := uint16Len("🎤 Voice transcript\n") + uint16Len(full)
	if (len(sents) < readbackTinyMaxSentences || tinyDisplayedU16 <= readbackTinyMaxU16) &&
		tinyDisplayedU16 <= readbackShortMaxU16 {
		return "sendMessage", "🎤 <b>Voice transcript</b>\n" + htmlEscape(full), bandTiny
	}

	f3, l3, more := buildPreview(sents)
	// BUG B: hard-cap each preview part so a few very long, rambly sentences can't fill
	// the screen — at readbackPreviewPartMaxU16 UTF-16 units per part. The head elides
	// its END and the tail (last visible line) keeps its END with a leading '…', so the
	// summary never ends chopped mid-sentence on a trailing '…' (see capPreviewParts).
	// Cap the UNescaped text then escape once (entity-safe, like the caption); every
	// SHORT/DEADZONE/LONG assembly below reads the capped f3/l3, so the elided summary
	// and its DISPLAYED-length measurement stay consistent.
	f3, l3 = capPreviewParts(f3, l3, more)
	header := fmt.Sprintf("🎤 <b>Voice transcript</b> · ~%d words", words)

	// Preview block: f3, plus the elision line + tail ONLY when there is an
	// elided middle. more == 0 happens on the fallthrough path (fewer than
	// readbackTinyMaxSentences sentences but DISPLAYED past the sendMessage
	// cap): buildPreview then returns (all, "", 0), and embedding the elision
	// unconditionally would render a literal "✂️✂️ 0 more sentences ✂️✂️" plus
	// an empty tail paragraph. Mirrors readbackCaption's `if more > 0` guard.
	summaryHTML := htmlEscape(f3)
	summaryVisible := f3
	previewHTML := "<p>" + htmlEscape(f3) + "</p>"
	if more > 0 {
		elision := fmt.Sprintf("✂️✂️ %d more sentences ✂️✂️", more)
		summaryHTML += "\n<i>" + elision + "</i>\n" + htmlEscape(l3)
		summaryVisible += "\n" + elision + "\n" + l3
		previewHTML += "<p><i>" + elision + "</i></p>" +
			"<p>" + htmlEscape(l3) + "</p>"
	}

	// SHORT candidate — summary + heading + the whole transcript in an expandable
	// blockquote. Measure the DISPLAYED text: tags cost 0, and an escaped entity
	// costs its single visible char, so we measure the unescaped visible string
	// (and send the escaped one), exactly as the W4 Python did.
	shortHTML := header + "\n" + summaryHTML +
		"\n\n<b>Full Transcript</b>\n<blockquote expandable>" + htmlEscape(full) + "</blockquote>"
	shortVisible := fmt.Sprintf("🎤 Voice transcript · ~%d words", words) + "\n" + summaryVisible +
		"\n\nFull Transcript\n" + full
	shortVisibleU16 := uint16Len(shortVisible)
	if shortVisibleU16 <= readbackShortMaxU16 {
		return "sendMessage", shortHTML, bandShort
	}

	// LONG / DEADZONE candidates — both are PLAIN rich messages (no \n: rich
	// messages don't honor it, so the summary/heading use <p> blocks). LONG is a
	// plain <p> body that Telegram's native "Show More" collapses once content
	// exceeds ~8000 chars, so a body whose DISPLAYED length is past the 9000 margin
	// collapses smoothly. The 4096 < shortVisible ≤ 9000 dead zone (too short for
	// native "Show More") instead wraps the whole transcript in a searchable
	// <details> collapse. Budget both on the assembled HTML's UTF-8 BYTE length.
	richHTML := "<p>" + header + "</p>" +
		previewHTML +
		"<p><b>Full Transcript</b></p>" +
		"<p>" + htmlEscape(full) + "</p>"
	detailsHTML := "<p>" + header + "</p>" +
		previewHTML +
		"<details><summary><b>📄 Full Transcript</b></summary><p>" + htmlEscape(full) + "</p></details>"

	// DEADZONE — the 4096 < shortVisible ≤ 9000 window renders as a searchable
	// <details> collapse rich message.
	if shortVisibleU16 <= readbackNativeMinU16 && len(detailsHTML) <= readbackRichMaxBytes {
		return "sendRichMessage", detailsHTML, bandDeadzone
	}
	// LONG — past the 9000 native-collapse margin, the plain rich body fits budget.
	if shortVisibleU16 > readbackNativeMinU16 && len(richHTML) <= readbackRichMaxBytes {
		return "sendRichMessage", richHTML, bandLong
	}

	// DOCUMENT — rich HTML over the budget (>32k assembled), or a dead-zone
	// detailsHTML somehow over budget: the caller writes the whole verbatim
	// transcript as a .txt file + caption.
	return "sendDocument", "", bandHuge
}

// readbackCaption builds the .txt document's caption: the summary header + a
// visible preview, escaped, and capped under Telegram's caption ceiling. Used by
// the HUGE band and as the caption when an earlier band's send errors and
// cascades to the document.
func readbackCaption(transcript string) string {
	full := strings.TrimSpace(transcript)
	sents := splitSentences(full)
	words := len(strings.Fields(full))
	f3, l3, more := buildPreview(sents)
	// Match the message bands' per-part preview cap (BUG B) before the caption's own
	// budget cap below, so the caption's head/tail are bounded the same way — head
	// end-cut, tail (last visible line) start-cut with a leading '…'. No-op when a
	// part already fits, so this never double-elides. (The caption's own whole-body
	// budget cap below intentionally stays an END-cut: it keeps the head + the
	// "N more sentences" marker when the .txt caption ceiling forces a further trim.)
	f3, l3 = capPreviewParts(f3, l3, more)

	header := fmt.Sprintf("🎤 <b>Voice transcript</b> · ~%d words", words)
	body := f3
	if more > 0 {
		body = fmt.Sprintf("%s\n✂️✂️ %d more sentences ✂️✂️\n%s", f3, more, l3)
	}
	// Cap the VISIBLE preview BEFORE escaping, so truncation always lands on a rune
	// boundary of the UNescaped text — never inside an HTML entity (&amp;/&lt;/&gt;),
	// which would be malformed HTML and 400 the send (the .txt document path has no
	// parse_mode fallback). The header carries fixed, intentional <b> tags and is
	// small; if a preview dense with &/</> still pushes the escaped caption past the
	// 1024-UTF-16 ceiling, shrink the VISIBLE budget and re-escape until it fits —
	// we NEVER capUTF16 the already-escaped string.
	visibleBudget := readbackCaptionPreviewBudget
	caption := header + "\n" + htmlEscape(capUTF16(body, visibleBudget))
	for uint16Len(caption) > readbackCaptionMaxU16 && visibleBudget > 0 {
		visibleBudget -= 128
		if visibleBudget < 0 {
			visibleBudget = 0
		}
		caption = header + "\n" + htmlEscape(capUTF16(body, visibleBudget))
	}
	return caption
}

// Readback outbound-retry budget. A transient outbound blip (network/timeout/
// 5xx, or a 429) must not silently DROP the transcript echo — that echo is the
// sender's only confirmation of what the agent received. We retry the whole send
// a few times with short exponential backoff, bounded so a persistently-down
// wire gives up within a couple of seconds rather than stalling the worker's
// synchronous flush loop. Format/permanent errors are NOT retried here — they
// either cascade to a smaller format (inside sendReadbackOnce) or fail fast.
const (
	readbackRetryMaxAttempts = 3                      // 1 initial try + 2 retries
	readbackRetryBaseBackoff = 400 * time.Millisecond // doubles each retry
	readbackRetryMaxBackoff  = 3 * time.Second        // per-wait cap (also caps a 429 retry_after)
)

// isRetryableSendErr reports whether a send error is worth retrying the SAME
// send: a transient network/5xx condition or a 429. These clear on their own.
// It is deliberately FALSE for permanent errors (chat-not-found, blocked,
// 401/403) and format/parse errors — retrying an identical send won't help
// those, and in the cascade a smaller format is the right move, not a retry.
func isRetryableSendErr(err error) bool {
	class, _ := classifyError(err)
	return class == errClassTransient || class == errClassRateLimited
}

// SendReadback echoes a voice transcript back to the source chat as ONE Telegram
// message in the frozen readback format. It reuses the channel's existing
// senders (no raw HTTP) and is the optional interface the broker reaches after
// STT succeeds (worker.flushInbounds). Returns the sent message_id.
//
// Resilience (fixes a silent-drop): sendReadbackOnce degrades TINY/SHORT/LONG/
// DEADZONE → .txt document → short notice ONLY on FORMAT/permanent errors — a
// smaller message can't help when the wire is down. A TRANSIENT error (network/
// timeout/5xx/429) short-circuits that cascade and bubbles here, where we retry
// the whole send with bounded backoff. Only a persistently-unhealthy outbound
// path makes us give up — loudly logged, still non-fatal upstream (the agent
// already has the transcript; only the chat echo is lost). The transcript is
// NEVER truncated or summarized.
func (c *Channel) SendReadback(args c3types.ReadbackArgs) (int64, error) {
	if c.bot == nil {
		return 0, errors.New("telegram: channel not started")
	}
	id, err := c.retryReadbackSend(func() (int64, error) { return c.sendReadbackOnce(args) })
	return id, c.scrubToken(err)
}

// retryReadbackSend runs send with bounded exponential backoff, retrying ONLY
// retryable (transient/429) failures. A deterministic failure (permanent/format)
// returns immediately; exhausting the retries returns the last error after a
// loud log. Aborts promptly if the channel context is cancelled. send is a seam
// so the retry policy is unit-testable without a live bot.
func (c *Channel) retryReadbackSend(send func() (int64, error)) (int64, error) {
	backoff := readbackRetryBaseBackoff
	var lastErr error
	for attempt := 1; attempt <= readbackRetryMaxAttempts; attempt++ {
		id, err := send()
		if err == nil {
			return id, nil
		}
		lastErr = err
		class, retryAfter := classifyError(err)
		if class != errClassTransient && class != errClassRateLimited {
			return 0, err // deterministic — retrying won't help
		}
		if attempt == readbackRetryMaxAttempts {
			break
		}
		wait := backoff
		if class == errClassRateLimited && retryAfter > 0 {
			wait = time.Duration(retryAfter) * time.Second
		}
		if wait > readbackRetryMaxBackoff {
			wait = readbackRetryMaxBackoff
		}
		c.logf("telegram: readback attempt %d/%d failed, retrying in %v: %v",
			attempt, readbackRetryMaxAttempts, wait, err)
		select {
		case <-time.After(wait):
		case <-c.ctx.Done():
			return 0, c.ctx.Err()
		}
		backoff *= 2
	}
	c.logf("telegram: readback PERMANENTLY dropped after %d transient-failed attempts "+
		"(transcript echo lost; agent already has the text): %v", readbackRetryMaxAttempts, lastErr)
	// Outbound-health feed site #2 (CRITIQUE FOLD #2 + #4): a readback give-up is
	// ONE failure event (already 3 retried attempts). feedOutboundFailure counts
	// it ONLY if lastErr is a genuine transient — a pure-429 exhaustion (429s are
	// retried by retryReadbackSend) must NOT drive outbound-DOWN, since 429 = a
	// reachable server pushing back. The ctx-cancel early return above (normal
	// shutdown) never reaches here.
	c.feedOutboundFailure(lastErr, "readback send exhausted retries")
	return 0, lastErr
}

// sendReadbackOnce performs a single readback send: render the band, send it,
// and — ONLY on a format/permanent error — degrade to the .txt document, then a
// short notice. A retryable (transient/429) error short-circuits the cascade and
// is returned to SendReadback's retry loop, because a smaller format won't send
// on a wire that's down.
func (c *Channel) sendReadbackOnce(args c3types.ReadbackArgs) (int64, error) {
	method, payload, band := renderReadback(args.Transcript)

	switch band {
	case bandTiny, bandShort:
		id, err := c.sendReadbackMessage(args, payload)
		if err == nil {
			return id, nil
		}
		if isRetryableSendErr(err) {
			return 0, err
		}
		c.logf("telegram: readback %s (%s) format error, falling back to .txt document: %v", band, method, err)
	case bandLong, bandDeadzone:
		id, err := c.sendRichHTML(args.ChatID, payload, args.TopicID, args.ReplyTo)
		if err == nil {
			return id, nil
		}
		if isRetryableSendErr(err) {
			return 0, err
		}
		c.logf("telegram: readback %s (%s) format error, falling back to .txt document: %v", band, method, err)
	case bandHuge:
		// Falls straight through to the document path below.
	}

	// HUGE band, or a TINY/SHORT/LONG/DEADZONE FORMAT error → the whole verbatim
	// transcript as a .txt document, captioned with the summary preview.
	id, err := c.sendReadbackDocument(args, readbackCaption(args.Transcript))
	if err == nil {
		return id, nil
	}
	if isRetryableSendErr(err) {
		return 0, err
	}
	c.logf("telegram: readback .txt document format error, falling back to a short notice: %v", err)

	// Document failed on format too → a short plain notice (last best-effort).
	id, err = c.sendReadbackNotice(args)
	if err != nil {
		c.logf("telegram: readback short notice failed (giving up, non-fatal upstream): %v", err)
		return 0, err
	}
	return id, nil
}

// sendReadbackMessage sends a TINY/SHORT band payload via sendMessage, with the
// SAME pattern as SendReply (outbound.go): rate.Wait, requestOptsFor, the
// isParseEntityError plaintext fallback, recordOutboundErr/Success, and
// MessageThreadId + ReplyParameters from TopicID/ReplyTo. htmlText is pre-formed
// HTML (parse_mode HTML); the parse-error fallback re-sends the raw transcript.
func (c *Channel) sendReadbackMessage(args c3types.ReadbackArgs, htmlText string) (int64, error) {
	opts := &gotgbot.SendMessageOpts{
		ParseMode:       "HTML",
		RequestOpts:     c.requestOptsFor("sendMessage"),
		MessageThreadId: threadID(args.TopicID),
		ReplyParameters: replyParams(args.ReplyTo),
	}
	if err := c.rate.Wait(c.ctx, args.ChatID); err != nil {
		return 0, fmt.Errorf("telegram: rate-wait: %w", err)
	}
	msg, err := c.bot.SendMessage(args.ChatID, htmlText, opts)
	if err != nil && isParseEntityError(err) {
		// Mirror SendReply: a malformed-HTML readback retries as the plain
		// transcript rather than dropping the message.
		c.logf("telegram: readback HTML parse error, retrying as plaintext: %v", err)
		plainOpts := *opts
		plainOpts.ParseMode = ""
		msg, err = c.bot.SendMessage(args.ChatID, args.Transcript, &plainOpts)
	}
	if err != nil {
		c.recordOutboundErr(err)
		return 0, fmt.Errorf("telegram: readback sendMessage: %w", err)
	}
	c.recordOutboundSuccess()
	return msg.MessageId, nil
}

// sendReadbackDocument sends the full verbatim transcript as a .txt document (the
// HUGE / fallback band), captioned with the summary preview. It reuses gotgbot's
// SendDocument — the same call sendMedia rides, NOT raw HTTP — with the SAME
// rate.Wait + recordOutbound + thread/reply pattern as the rest of the channel.
// The temp file is removed after the send.
func (c *Channel) sendReadbackDocument(args c3types.ReadbackArgs, captionHTML string) (int64, error) {
	f, err := os.CreateTemp("", "c3-voice-transcript-*.txt")
	if err != nil {
		return 0, fmt.Errorf("telegram: readback temp file: %w", err)
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, werr := f.WriteString(args.Transcript); werr != nil {
		f.Close()
		return 0, fmt.Errorf("telegram: readback write temp: %w", werr)
	}
	if cerr := f.Close(); cerr != nil {
		return 0, fmt.Errorf("telegram: readback close temp: %w", cerr)
	}
	fh, err := os.Open(tmp)
	if err != nil {
		return 0, fmt.Errorf("telegram: readback open temp: %w", err)
	}
	defer fh.Close()

	opts := &gotgbot.SendDocumentOpts{
		Caption:         captionHTML,
		ParseMode:       "HTML",
		RequestOpts:     c.requestOptsFor("sendDocument"),
		MessageThreadId: threadID(args.TopicID),
		ReplyParameters: replyParams(args.ReplyTo),
	}
	if err := c.rate.Wait(c.ctx, args.ChatID); err != nil {
		return 0, fmt.Errorf("telegram: rate-wait: %w", err)
	}
	msg, err := c.bot.SendDocument(args.ChatID, gotgbot.InputFileByReader("voice-transcript.txt", fh), opts)
	if err != nil {
		c.recordOutboundErr(err)
		return 0, fmt.Errorf("telegram: readback sendDocument: %w", err)
	}
	c.recordOutboundSuccess()
	return msg.MessageId, nil
}

// sendReadbackNotice sends the short plain notice that is the last resort when
// every other band failed. Same send pattern as the others; non-fatal upstream.
func (c *Channel) sendReadbackNotice(args c3types.ReadbackArgs) (int64, error) {
	opts := &gotgbot.SendMessageOpts{
		ParseMode:       "HTML",
		RequestOpts:     c.requestOptsFor("sendMessage"),
		MessageThreadId: threadID(args.TopicID),
		ReplyParameters: replyParams(args.ReplyTo),
	}
	if err := c.rate.Wait(c.ctx, args.ChatID); err != nil {
		return 0, fmt.Errorf("telegram: rate-wait: %w", err)
	}
	msg, err := c.bot.SendMessage(args.ChatID,
		"🎤 <b>Voice transcript</b> (too long to display; delivery failed — see logs)", opts)
	if err != nil {
		c.recordOutboundErr(err)
		return 0, fmt.Errorf("telegram: readback notice: %w", err)
	}
	c.recordOutboundSuccess()
	return msg.MessageId, nil
}
