package telegram

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/PaulSonOfLars/gotgbot/v2"
)

// rbTGErr builds a Telegram API error with the given HTTP code, for exercising
// the readback retry classification.
func rbTGErr(code int) error {
	return &gotgbot.TelegramError{Code: code, Description: "test"}
}

// TestReadbackAmbiguousTimeoutIsNotRetried pins the double-post fix: a send that
// times out with no server response is ambiguous — Telegram may have already
// created the echo before the deadline — so retryReadbackSend must NOT repost
// it. It stops after a single send. A 5xx, by contrast, means Telegram
// responded and the message was not created, so it still retries to the cap.
func TestReadbackAmbiguousTimeoutIsNotRetried(t *testing.T) {
	c, _ := newWiredChannel()
	c.ctx = context.Background()

	// Ambiguous post-send timeout: exactly one send, no repost.
	calls := 0
	if _, err := c.retryReadbackSend(func() (int64, error) {
		calls++
		return 0, context.DeadlineExceeded
	}); err == nil {
		t.Fatal("ambiguous timeout should surface as an error")
	}
	if calls != 1 {
		t.Fatalf("ambiguous timeout must not be retried (double-post risk); sends=%d, want 1", calls)
	}

	// Contrast: a 5xx (server responded, message not created) retries to the cap.
	calls = 0
	if _, err := c.retryReadbackSend(func() (int64, error) {
		calls++
		return 0, rbTGErr(500)
	}); err == nil {
		t.Fatal("exhausted 5xx retries should surface an error")
	}
	if calls != readbackRetryMaxAttempts {
		t.Fatalf("5xx should retry to the cap; sends=%d, want %d", calls, readbackRetryMaxAttempts)
	}
}

// TestIsRetryableSendErr pins the retry classification used by the readback
// send: transient (network/5xx) and 429 are retryable; permanent (4xx other
// than 429) and nil are not.
func TestIsRetryableSendErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"5xx transient", rbTGErr(500), true},
		{"429 rate-limited", rbTGErr(429), true},
		{"400 permanent", rbTGErr(400), false},
		{"403 permanent", rbTGErr(403), false},
		{"nil", nil, false},
		{"plain network error", errors.New("dial tcp: connection refused"), true},
	}
	for _, tc := range cases {
		if got := isRetryableSendErr(tc.err); got != tc.want {
			t.Errorf("%s: isRetryableSendErr=%v want %v", tc.name, got, tc.want)
		}
	}
}

// TestIsRetryableSendErr_WrappedErrors pins that classification survives %w
// wrapping. The real send path (readback.go's sendReadback* helpers) returns
// errors wrapped with fmt.Errorf("...: %w", err), and classifyError relies on
// errors.As to reach the underlying *gotgbot.TelegramError. A future %w->%v
// slip in any wrapper would silently misclassify (e.g. a wrapped 500 would stop
// being retried) and no BARE-error test would catch it — these do.
func TestIsRetryableSendErr_WrappedErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"wrapped 400 permanent", fmt.Errorf("telegram: readback sendMessage: %w", rbTGErr(400)), false},
		{"wrapped 500 transient", fmt.Errorf("telegram: readback sendMessage: %w", rbTGErr(500)), true},
		{"doubly-wrapped 429", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", rbTGErr(429))), true},
	}
	for _, tc := range cases {
		if got := isRetryableSendErr(tc.err); got != tc.want {
			t.Errorf("%s: isRetryableSendErr=%v want %v", tc.name, got, tc.want)
		}
	}
}

// TestRetryReadbackSend_TransientThenSuccess: a transient blip is retried and the
// echo IS delivered — the exact silent-drop this fix closes (a dogfood tester saw
// transcripts vanish from the chat during transient telegram-fetch-DOWN windows).
func TestRetryReadbackSend_TransientThenSuccess(t *testing.T) {
	c := &Channel{host: &fakeHost{}, ctx: context.Background()}
	calls := 0
	id, err := c.retryReadbackSend(func() (int64, error) {
		calls++
		if calls < 2 {
			return 0, rbTGErr(500)
		}
		return 42, nil
	})
	if err != nil {
		t.Fatalf("want success after transient retry, got %v", err)
	}
	if id != 42 || calls != 2 {
		t.Fatalf("id=%d calls=%d, want id=42 calls=2 (one retry)", id, calls)
	}
}

// TestRetryReadbackSend_PermanentNoRetry: a permanent error (e.g. chat-not-found)
// returns immediately — no wasted retries against a deterministic failure.
func TestRetryReadbackSend_PermanentNoRetry(t *testing.T) {
	c := &Channel{host: &fakeHost{}, ctx: context.Background()}
	calls := 0
	_, err := c.retryReadbackSend(func() (int64, error) {
		calls++
		return 0, rbTGErr(400)
	})
	if err == nil {
		t.Fatal("want permanent error returned")
	}
	if calls != 1 {
		t.Fatalf("calls=%d, want 1 (no retry on permanent)", calls)
	}
}

// TestRetryReadbackSend_ExhaustsAndGivesUp: a persistently-down wire gives up
// after the bounded attempt count and returns the last error (non-fatal upstream).
func TestRetryReadbackSend_ExhaustsAndGivesUp(t *testing.T) {
	c := &Channel{host: &fakeHost{}, ctx: context.Background()}
	calls := 0
	_, err := c.retryReadbackSend(func() (int64, error) {
		calls++
		return 0, rbTGErr(500)
	})
	if err == nil {
		t.Fatal("want error after exhausting retries")
	}
	if calls != readbackRetryMaxAttempts {
		t.Fatalf("calls=%d, want %d", calls, readbackRetryMaxAttempts)
	}
}

// TestRetryReadbackSend_CtxCancelAborts: a cancelled channel context aborts the
// backoff wait promptly instead of sleeping out the whole budget.
func TestRetryReadbackSend_CtxCancelAborts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &Channel{host: &fakeHost{}, ctx: ctx}
	calls := 0
	_, err := c.retryReadbackSend(func() (int64, error) {
		calls++
		return 0, rbTGErr(500)
	})
	if err == nil {
		t.Fatal("want ctx error on cancel")
	}
	if calls != 1 {
		t.Fatalf("calls=%d, want 1 (aborted at first backoff)", calls)
	}
}

// rbSentenceDoc builds an ASCII transcript (u16 == byte len == rune count) of
// roughly target chars made of uniquely-tagged sentences "S0 wwww.". Returns
// (text, tokens) where token i ("S{i}") begins sentence i. Mirrors the Python
// test's sentence_doc so the band fixtures are comparable across the two suites.
func rbSentenceDoc(target, sentLen int) (string, []string) {
	var parts, tokens []string
	i, total := 0, 0
	for total < target {
		tag := fmt.Sprintf("S%d", i)
		fillN := sentLen - len(tag) - 2
		if fillN < 1 {
			fillN = 1
		}
		s := tag + " " + strings.Repeat("w", fillN) + "."
		parts = append(parts, s)
		tokens = append(tokens, tag)
		total += len(s) + 1
		i++
	}
	return strings.Join(parts, " "), tokens
}

func TestSplitSentences(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"latin", "One. Two. Three.", []string{"One.", "Two.", "Three."}},
		{"mixed terminators", "Hi! Are you ok? Yes.", []string{"Hi!", "Are you ok?", "Yes."}},
		{"devanagari danda", "क ख। ग घ। च", []string{"क ख।", "ग घ।", "च"}},
		{"ellipsis", "Well… I think so.", []string{"Well…", "I think so."}},
		{"runon no punctuation", "word0 word1 word2 word3", []string{"word0 word1 word2 word3"}},
		{"trailing whitespace trimmed", "  A.  B.  ", []string{"A.", "B."}},
		{"empty", "   ", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := splitSentences(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("splitSentences(%q) = %#v; want %#v", tc.in, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("splitSentences(%q)[%d] = %q; want %q", tc.in, i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestBuildPreview(t *testing.T) {
	// >= 13 sentences: first 3 / last 3 / M = total − 6.
	thirteen := []string{"a.", "b.", "c.", "d.", "e.", "f.", "g.", "h.", "i.", "j.", "k.", "l.", "m."}
	f3, l3, more := buildPreview(thirteen)
	if f3 != "a. b. c." {
		t.Errorf("first3 = %q; want %q", f3, "a. b. c.")
	}
	if l3 != "k. l. m." {
		t.Errorf("last3 = %q; want %q", l3, "k. l. m.")
	}
	if more != 7 {
		t.Errorf("more = %d; want 7 (13 − 6)", more)
	}

	// < 13 sentences: all joined as first3, empty last3, M = 0 (no elision).
	twelve := []string{"a.", "b.", "c.", "d.", "e.", "f.", "g.", "h.", "i.", "j.", "k.", "l."}
	want := "a. b. c. d. e. f. g. h. i. j. k. l."
	f3, l3, more = buildPreview(twelve)
	if f3 != want || l3 != "" || more != 0 {
		t.Errorf("buildPreview(12) = (%q, %q, %d); want (%q, \"\", 0)", f3, l3, more, want)
	}
}

func TestUint16Len(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"", 0},
		{"abc", 3},
		{"🎤", 2},      // astral rune = surrogate pair = 2 UTF-16 units
		{"a🎤🎤", 5},    // 1 + 2 + 2
		{"नमस्ते", 6}, // BMP runes count 1 each
	}
	for _, tc := range cases {
		if got := uint16Len(tc.in); got != tc.want {
			t.Errorf("uint16Len(%q) = %d; want %d", tc.in, got, tc.want)
		}
	}
}

func TestHTMLEscape(t *testing.T) {
	// '&' first, no double escaping.
	if got := htmlEscape("a & b < c > d"); got != "a &amp; b &lt; c &gt; d" {
		t.Errorf("htmlEscape = %q", got)
	}
	if strings.Contains(htmlEscape("a < b"), "&amp;lt;") {
		t.Error("htmlEscape double-escaped '<' (& applied after <)")
	}
	// A literal close tag in the transcript is neutralized.
	got := htmlEscape("please </blockquote> literally")
	if !strings.Contains(got, "&lt;/blockquote&gt;") || strings.Contains(got, "</blockquote>") {
		t.Errorf("htmlEscape did not neutralize </blockquote>: %q", got)
	}
}

func TestRenderReadback_BandSelection(t *testing.T) {
	tiny, _ := "One. Two. Three.", 0       // 3 sentences → TINY
	short, _ := rbSentenceDoc(2000, 40)    // ~48 sentences, ~2k visible → SHORT
	deadzone, _ := rbSentenceDoc(5000, 40) // ~5k visible (4096<x≤9000 dead zone) → DEADZONE <details>
	long, _ := rbSentenceDoc(12000, 40)    // ~12k visible (>9000) but rich ≤32000 → LONG
	huge, _ := rbSentenceDoc(33000, 40)    // rich html >32000 bytes → HUGE

	cases := []struct {
		name       string
		in         string
		wantMethod string
		wantBand   readbackBand
	}{
		{"tiny", tiny, "sendMessage", bandTiny},
		{"short", short, "sendMessage", bandShort},
		{"deadzone", deadzone, "sendRichMessage", bandDeadzone},
		{"long", long, "sendRichMessage", bandLong},
		{"huge", huge, "sendDocument", bandHuge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			method, payload, band := renderReadback(tc.in)
			if band != tc.wantBand || method != tc.wantMethod {
				t.Fatalf("renderReadback(%s) → method=%q band=%v; want method=%q band=%v",
					tc.name, method, band, tc.wantMethod, tc.wantBand)
			}
			switch band {
			case bandHuge:
				if payload != "" {
					t.Errorf("HUGE band payload should be empty (caller builds the doc), got %q", payload)
				}
			default:
				if payload == "" {
					t.Errorf("%v band payload should be non-empty", band)
				}
			}
			if band == bandDeadzone && !strings.Contains(payload, "<details>") {
				t.Errorf("DEADZONE band payload must contain a <details> collapse, got %q", payload)
			}
		})
	}
}

// TestRenderReadback_ShortExact pins the EXACT assembled SHORT-band string
// (header · word count, first-3 … M more … last-3 summary, the heading, and the
// whole verbatim transcript in an expandable blockquote). The fixture must exceed
// readbackTinyMaxU16 DISPLAYED so it clears the length-based TINY gate (BUG A) and
// actually lands in SHORT; it uses short 40-char sentences so the per-part preview
// cap (BUG B) is a no-op and the assembly is fully predictable. (The original
// ~150-u16 13-sentence fixture asserted the pre-BUG-A behavior — a screen-sized note
// getting the middle elision — and now correctly renders as TINY; see
// TestRenderReadback_ManyShortSentencesTiny.)
func TestRenderReadback_ShortExact(t *testing.T) {
	in, _ := rbSentenceDoc(1500, 40) // >1000 u16 displayed, ~37 short sentences → SHORT
	method, payload, band := renderReadback(in)
	if band != bandShort || method != "sendMessage" {
		t.Fatalf("got method=%q band=%v; want sendMessage/short", method, band)
	}
	sents := splitSentences(in)
	f3, l3, more := buildPreview(sents)
	f3, l3 = capPreviewParts(f3, l3, more) // no-op for 40-char sentences
	words := len(strings.Fields(in))
	want := fmt.Sprintf("🎤 <b>Voice transcript</b> · ~%d words", words) + "\n" +
		htmlEscape(f3) + "\n<i>" + fmt.Sprintf("✂️✂️ %d more sentences ✂️✂️", more) + "</i>\n" +
		htmlEscape(l3) + "\n\n<b>Full Transcript</b>\n" +
		"<blockquote expandable>" + htmlEscape(in) + "</blockquote>"
	if payload != want {
		t.Errorf("SHORT payload mismatch:\n got: %q\nwant: %q", payload, want)
	}
	// Structural pins that survive the programmatic want (guard against silent
	// co-mutation of code + expectation).
	if !strings.Contains(payload, "<blockquote expandable>") {
		t.Error("SHORT band must wrap the full transcript in an expandable blockquote")
	}
	if !strings.Contains(payload, "<b>Full Transcript</b>") {
		t.Error("SHORT band must carry the Full Transcript heading")
	}
	if !strings.Contains(payload, fmt.Sprintf("✂️✂️ %d more sentences ✂️✂️", more)) {
		t.Error("SHORT band must carry the middle-elision marker")
	}
}

// TestRenderReadback_LongAssembly verifies the LONG band's assembly: <p> blocks
// for the summary/heading AND a PLAIN <p> body for the whole transcript (no \n —
// rich messages don't honor it; no blockquote — Telegram's native "Show More"
// collapses the long plain rich message), against the same parts, without
// hardcoding the multi-KB transcript. The input is sized past the 9000 DISPLAYED
// native-collapse floor so it lands in LONG (not the 4k–9k document dead zone).
func TestRenderReadback_LongAssembly(t *testing.T) {
	in, _ := rbSentenceDoc(12000, 40)
	method, payload, band := renderReadback(in)
	if band != bandLong || method != "sendRichMessage" {
		t.Fatalf("got method=%q band=%v; want sendRichMessage/long", method, band)
	}
	sents := splitSentences(in)
	f3, l3, more := buildPreview(sents)
	words := len(strings.Fields(in))
	want := fmt.Sprintf("<p>🎤 <b>Voice transcript</b> · ~%d words</p>", words) +
		"<p>" + htmlEscape(f3) + "</p>" +
		fmt.Sprintf("<p><i>✂️✂️ %d more sentences ✂️✂️</i></p>", more) +
		"<p>" + htmlEscape(l3) + "</p>" +
		"<p><b>Full Transcript</b></p>" +
		"<p>" + htmlEscape(in) + "</p>"
	if payload != want {
		t.Errorf("LONG payload mismatch:\n got (len=%d): %.120q…\nwant (len=%d): %.120q…",
			len(payload), payload, len(want), want)
	}
	if strings.Contains(payload, "\n") {
		t.Error("LONG band must not contain \\n (rich messages don't honor it)")
	}
	if strings.Contains(payload, "<blockquote") {
		t.Error("LONG band must be a plain <p> body, not a blockquote")
	}
	if strings.Contains(payload, "<details") {
		t.Error("LONG band must be a plain <p> body, not a <details> collapse")
	}
	if !strings.Contains(payload, "<p>") {
		t.Error("LONG band must assemble the summary/body as <p> blocks")
	}
	if !strings.Contains(payload, fmt.Sprintf("✂️✂️ %d more sentences ✂️✂️", more)) {
		t.Error("LONG band must carry the elision marker")
	}
	if len(payload) > readbackRichMaxBytes {
		t.Errorf("LONG payload %d bytes exceeds rich budget %d", len(payload), readbackRichMaxBytes)
	}
}

// TestRenderReadback_TinyEscapes confirms the TINY band escapes dynamic text and
// carries no summary/heading/collapse.
func TestRenderReadback_TinyEscapes(t *testing.T) {
	in := "Tom & Jerry < Spike. Render </blockquote> please. Done."
	method, payload, band := renderReadback(in)
	if band != bandTiny || method != "sendMessage" {
		t.Fatalf("got method=%q band=%v; want sendMessage/tiny", method, band)
	}
	if !strings.HasPrefix(payload, "🎤 <b>Voice transcript</b>\n") {
		t.Errorf("TINY payload prefix wrong: %q", payload)
	}
	for _, want := range []string{"&amp;", "&lt;", "&gt;", "&lt;/blockquote&gt;"} {
		if !strings.Contains(payload, want) {
			t.Errorf("TINY payload missing escape %q: %q", want, payload)
		}
	}
	if strings.Contains(payload, "</blockquote>") || strings.Contains(payload, "<blockquote") {
		t.Error("TINY band must not carry a structural blockquote")
	}
	if strings.Contains(payload, "Full Transcript") || strings.Contains(payload, "more sentences") {
		t.Error("TINY band must carry no summary/heading")
	}
}

// TestRenderReadback_TwelveSentencesTiny pins the no-elide threshold: 12
// sentences (= 2× the 6-sentence preview) still render as the bare TINY band,
// because only >12 sentences have a meaningful middle to elide.
func TestRenderReadback_TwelveSentencesTiny(t *testing.T) {
	in := "A. B. C. D. E. F. G. H. I. J. K. L."
	if n := len(splitSentences(in)); n != 12 {
		t.Fatalf("fixture has %d sentences, want 12", n)
	}
	method, payload, band := renderReadback(in)
	if band != bandTiny || method != "sendMessage" {
		t.Fatalf("got method=%q band=%v; want sendMessage/tiny", method, band)
	}
	want := "🎤 <b>Voice transcript</b>\n" + htmlEscape(in)
	if payload != want {
		t.Errorf("TINY payload mismatch:\n got: %q\nwant: %q", payload, want)
	}
	if strings.Contains(payload, "more sentences") || strings.Contains(payload, "Full Transcript") {
		t.Error("TINY band must carry no summary/elision/heading")
	}
}

// TestReadbackCaption keeps the .txt fallback caption within the Telegram cap.
func TestReadbackCaption(t *testing.T) {
	in, _ := rbSentenceDoc(33000, 300)
	capt := readbackCaption(in)
	if uint16Len(capt) > readbackCaptionMaxU16 {
		t.Errorf("caption %d UTF-16 units exceeds %d", uint16Len(capt), readbackCaptionMaxU16)
	}
	if !strings.HasPrefix(capt, "🎤 <b>Voice transcript</b> · ~") {
		t.Errorf("caption prefix wrong: %.60q", capt)
	}
}

// Entity-safety regression (review finding 3): a transcript whose preview is dense
// with &/</> must produce a caption that is (a) under the Telegram caption cap AND
// (b) free of any SLICED HTML entity. Truncating the already-escaped string could cut
// "&amp;" → "&am", which is malformed HTML and 400s the .txt document send (that path
// has no plaintext fallback), so the whole transcript would fail to deliver. The cap
// must apply to the UNescaped preview and escape exactly once.
func TestReadbackCaption_EntityDense_NoSlicedEntity(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 60; i++ {
		b.WriteString("a & b < c > d & e < f > g & h < i > j & k. ")
	}
	capt := readbackCaption(b.String())

	if u := uint16Len(capt); u > readbackCaptionMaxU16 {
		t.Fatalf("entity-dense caption is %d UTF-16 units, exceeds cap %d", u, readbackCaptionMaxU16)
	}
	// The header carries only literal <b> tags (no '&'), so every '&' in the caption
	// must begin a COMPLETE entity produced by htmlEscape. A dangling/partial "&..."
	// means truncation sliced an entity — the exact bug this guards. '&' is ASCII, so
	// byte-scanning is safe.
	for i := 0; i < len(capt); i++ {
		if capt[i] != '&' {
			continue
		}
		rest := capt[i:]
		if !(strings.HasPrefix(rest, "&amp;") || strings.HasPrefix(rest, "&lt;") || strings.HasPrefix(rest, "&gt;")) {
			t.Fatalf("caption has a sliced/partial HTML entity at byte %d: %.12q", i, rest)
		}
	}
}

// TestRenderReadback_ManyShortSentencesTiny is the BUG A regression: a real 45s
// note — many short sentences, ~131 words — whose whole DISPLAYED message is under
// readbackTinyMaxU16 must render as the bare TINY band (whole verbatim text, NO
// middle elision), even though it has far more than readbackTinyMaxSentences
// sentences. Before the fix it hit the SHORT-band "✂️ N more sentences ✂️" elision
// despite easily fitting on screen.
func TestRenderReadback_ManyShortSentencesTiny(t *testing.T) {
	parts := []string{
		"Hey I just finished the call with the vendor.",  // 9
		"They can ship the units by next Tuesday.",       // 8
		"The price is a little higher than we hoped.",    // 9
		"But the quality is much better this time.",      // 8
		"I told them we need the invoice first.",         // 8
		"Can you check the budget for this quarter?",     // 8
		"We might need to move some money around.",       // 8
		"Also the paint order is still pending today.",   // 8
		"I will follow up with them tomorrow morning.",   // 8
		"The team wants to meet on Thursday instead.",    // 8
		"That works for me if it works for you.",         // 9
		"Let me know your thoughts when you get this.",   // 9
		"Nothing else is urgent on my side today.",       // 8
		"Talk soon and thanks for all of your help.",     // 9
		"One more thing about the new shipping labels.",  // 8
		"We should reuse the old template for now okay.", // 9
	}
	in := strings.Join(parts, " ")

	sents := splitSentences(in)
	if len(sents) <= readbackTinyMaxSentences {
		t.Fatalf("fixture has %d sentences, want well over %d to exercise the length gate",
			len(sents), readbackTinyMaxSentences)
	}
	if words := len(strings.Fields(in)); words < 120 || words > 145 {
		t.Fatalf("fixture has %d words, want ~131", words)
	}
	displayed := uint16Len("🎤 Voice transcript\n" + in)
	if displayed >= readbackTinyMaxU16 {
		t.Fatalf("fixture DISPLAYED is %d u16, want < %d", displayed, readbackTinyMaxU16)
	}

	method, payload, band := renderReadback(in)
	if band != bandTiny || method != "sendMessage" {
		t.Fatalf("got method=%q band=%v; want sendMessage/tiny (BUG A: a screen-sized note must not be elided)", method, band)
	}
	// TINY carries the WHOLE verbatim transcript (no special chars → escape is identity).
	if !strings.Contains(payload, in) {
		t.Errorf("TINY payload must contain the full transcript verbatim; got %q", payload)
	}
	for _, marker := range []string{"✂️", "more sentences", "Full Transcript", "<blockquote"} {
		if strings.Contains(payload, marker) {
			t.Errorf("TINY payload must carry no elision/summary/collapse, but contains %q", marker)
		}
	}
}

// TestRenderReadback_TinyLengthBoundary pins the BUG A length gate at exactly
// readbackTinyMaxU16 DISPLAYED units, using fixtures with 13 sentences (== the
// count threshold, so the sentence-count gate never fires and only the length gate
// decides). Just-at → TINY; one unit over → the length-sized band (SHORT here).
func TestRenderReadback_TinyLengthBoundary(t *testing.T) {
	// mk builds an ASCII transcript (u16 == byte len) of exactly fullU16 units with
	// exactly 13 sentences: 12 "a." sentences + one padded final sentence.
	mk := func(fullU16 int) string {
		prefix := strings.Repeat("a. ", 12) // 12 sentences, 36 chars
		rest := fullU16 - len(prefix)       // chars for the padded 13th sentence
		if rest < 2 {
			rest = 2
		}
		return prefix + strings.Repeat("w", rest-1) + "."
	}
	// header visible = "🎤 Voice transcript\n" = 20 u16; DISPLAYED = 20 + fullU16.
	const headerU16 = 20
	under := mk(readbackTinyMaxU16 - headerU16)    // DISPLAYED == readbackTinyMaxU16 → TINY
	over := mk(readbackTinyMaxU16 - headerU16 + 1) // DISPLAYED == readbackTinyMaxU16+1 → SHORT

	if n := len(splitSentences(under)); n != 13 {
		t.Fatalf("under fixture has %d sentences, want 13", n)
	}
	if got := uint16Len("🎤 Voice transcript\n" + under); got != readbackTinyMaxU16 {
		t.Fatalf("under DISPLAYED = %d, want exactly %d", got, readbackTinyMaxU16)
	}
	if got := uint16Len("🎤 Voice transcript\n" + over); got != readbackTinyMaxU16+1 {
		t.Fatalf("over DISPLAYED = %d, want exactly %d", got, readbackTinyMaxU16+1)
	}

	if _, _, band := renderReadback(under); band != bandTiny {
		t.Errorf("just-at %d u16 (13 sentences) → band=%v; want tiny", readbackTinyMaxU16, band)
	}
	if _, _, band := renderReadback(over); band != bandShort {
		t.Errorf("just-over %d u16 (13 sentences) → band=%v; want short", readbackTinyMaxU16, band)
	}
}

// TestRenderReadback_RamblyPreviewCapped is the BUG B regression, updated for the
// tail-keep fix: when the first and last sentences are very long and rambly, the
// preview head (first 3) and tail (last 3) must each be hard-capped at
// readbackPreviewPartMaxU16 UTF-16 units so the summary can't fill the screen — but
// from OPPOSITE ends. The HEAD keeps its start and trails off with a trailing '…';
// the TAIL (the preview's last visible line) keeps its END with a LEADING '…' and
// must NOT end on a trailing '…'. The full uncapped head string must NOT appear in
// the preview region.
func TestRenderReadback_RamblyPreviewCapped(t *testing.T) {
	rambly := strings.TrimSpace(strings.Repeat("word ", 120)) + " end." // one ~604-char sentence
	var parts []string
	parts = append(parts, rambly) // sentence 0 (long)
	for i := 0; i < 12; i++ {
		parts = append(parts, "ok.") // short middle sentences
	}
	parts = append(parts, rambly) // last sentence (long)
	in := strings.Join(parts, " ")

	method, payload, band := renderReadback(in)
	if band != bandShort || method != "sendMessage" {
		t.Fatalf("got method=%q band=%v; want sendMessage/short (sanity for the fixture)", method, band)
	}

	sents := splitSentences(in)
	rawF3, rawL3, more := buildPreview(sents)
	if uint16Len(rawF3) <= readbackPreviewPartMaxU16 || uint16Len(rawL3) <= readbackPreviewPartMaxU16 {
		t.Fatalf("fixture preview parts are not over the cap (head=%d tail=%d, cap=%d); nothing to elide",
			uint16Len(rawF3), uint16Len(rawL3), readbackPreviewPartMaxU16)
	}
	capF3, capL3 := capPreviewParts(rawF3, rawL3, more)

	for name, part := range map[string]string{"head": capF3, "tail": capL3} {
		if u := uint16Len(part); u > readbackPreviewPartMaxU16+1 {
			t.Errorf("preview %s is %d u16, exceeds cap %d(+1)", name, u, readbackPreviewPartMaxU16)
		}
		if !strings.Contains(payload, htmlEscape(part)) {
			t.Errorf("rendered payload is missing the capped %s preview", name)
		}
	}
	// HEAD: keeps its start, trails off with a TRAILING '…' (never a leading one).
	if !strings.HasSuffix(capF3, "…") {
		t.Errorf("preview head was cut but does not end with a trailing '…': %q", capF3)
	}
	if strings.HasPrefix(capF3, "…") {
		t.Errorf("preview head must not start with '…' (it keeps the START): %q", capF3)
	}
	// TAIL: keeps its END with a LEADING '…' and must NOT trail off — this is the fix.
	if !strings.HasPrefix(capL3, "…") {
		t.Errorf("preview tail was cut but does not start with a leading '…': %q", capL3)
	}
	if strings.HasSuffix(capL3, "…") {
		t.Errorf("BUG: preview tail (last visible line) ends on a trailing '…' — chopped mid-sentence: %q", capL3)
	}
	if !strings.HasSuffix(capL3, "end.") {
		t.Errorf("preview tail should keep the sentence's natural ending, got %q", capL3)
	}
	// The whole verbatim transcript (which necessarily contains the full head text)
	// still lands in the expandable blockquote — the cap only bounds the SUMMARY. So
	// scope the "uncapped head is gone" and "no trailing '…'" checks to the PREVIEW
	// region (everything before the Full Transcript heading).
	previewRegion := payload
	if idx := strings.Index(payload, "<b>Full Transcript</b>"); idx >= 0 {
		previewRegion = payload[:idx]
	}
	if !strings.Contains(previewRegion, htmlEscape(capF3)) {
		t.Error("preview region is missing the capped head")
	}
	if strings.Contains(previewRegion, htmlEscape(rawF3)) {
		t.Error("BUG B: preview region still contains the full uncapped head")
	}
	if trimmed := strings.TrimRight(previewRegion, "\n "); strings.HasSuffix(trimmed, "…") {
		t.Errorf("preview must not end on a trailing '…': %q", trimmed)
	}
}

// TestRenderReadback_FewButHugeNotOversized is the latent-oversize regression: a
// note of a FEW very long sentences (each ~1500 u16) has fewer than
// readbackTinyMaxSentences sentences, so before the fix it took the unconditional
// TINY path and emitted a sendMessage payload far over Telegram's 4096 post-parse
// cap (a guaranteed 400). The guard must now route it away from sendMessage.
func TestRenderReadback_FewButHugeNotOversized(t *testing.T) {
	one := strings.Repeat("w", 1498) + "." // one ~1499-char sentence
	in := strings.TrimSpace(strings.Repeat(one+" ", 5))
	if n := len(splitSentences(in)); n >= readbackTinyMaxSentences {
		t.Fatalf("fixture has %d sentences, want fewer than %d to hit the old TINY path", n, readbackTinyMaxSentences)
	}

	method, payload, band := renderReadback(in)
	if method == "sendMessage" && uint16Len(payload) > readbackShortMaxU16 {
		t.Fatalf("few-but-huge note emitted an oversized sendMessage: %d u16 (> %d) band=%v",
			uint16Len(payload), readbackShortMaxU16, band)
	}
}

// TestRenderReadback_FewButHuge_NoZeroElision pins the more==0 fallthrough
// render: a note with fewer than readbackTinyMaxSentences sentences whose
// DISPLAYED length is past the sendMessage cap gets buildPreview's
// (all, "", 0) and lands in a rich band. The assembly must then omit the
// elision line AND the empty tail paragraph — no literal
// "✂️✂️ 0 more sentences ✂️✂️", no empty <p></p> — in every band, mirroring
// readbackCaption's `if more > 0` guard, and must still not be oversized.
func TestRenderReadback_FewButHuge_NoZeroElision(t *testing.T) {
	one := strings.Repeat("w", 1498) + "." // one ~1499-char sentence
	cases := []struct {
		name       string
		sentences  int
		wantBand   readbackBand
		wantMethod string
	}{
		// 5 × ~1500 ≈ 7.5k displayed → the 4096 < x ≤ 9000 dead zone.
		{"deadzone", 5, bandDeadzone, "sendRichMessage"},
		// 7 × ~1500 ≈ 10.5k displayed → past the 9000 native-collapse margin.
		{"long", 7, bandLong, "sendRichMessage"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := strings.TrimSpace(strings.Repeat(one+" ", tc.sentences))
			if n := len(splitSentences(in)); n >= readbackTinyMaxSentences {
				t.Fatalf("fixture has %d sentences, want fewer than %d (the more==0 path)", n, readbackTinyMaxSentences)
			}
			if _, _, more := buildPreview(splitSentences(in)); more != 0 {
				t.Fatalf("fixture preview more = %d, want 0", more)
			}

			method, payload, band := renderReadback(in)
			if band != tc.wantBand || method != tc.wantMethod {
				t.Fatalf("got method=%q band=%v; want %q/%v", method, band, tc.wantMethod, tc.wantBand)
			}
			if strings.Contains(payload, "0 more sentences") {
				t.Error("payload renders the literal \"0 more sentences\" elision line")
			}
			if strings.Contains(payload, "✂️") {
				t.Error("more==0 payload must carry no elision markers at all")
			}
			for _, empty := range []string{"<p></p>", "<i></i>", "<p><i></i></p>"} {
				if strings.Contains(payload, empty) {
					t.Errorf("more==0 payload contains an empty element %q (the phantom tail paragraph)", empty)
				}
			}
			// Still within the send budget for its method.
			if len(payload) > readbackRichMaxBytes {
				t.Errorf("payload is %d bytes, over the %d rich budget", len(payload), readbackRichMaxBytes)
			}
		})
	}
}

// TestCapUTF16Tail pins the tail-keeping cap used for the preview's last visible
// line: a no-op when the input already fits, otherwise it keeps the END behind a
// LEADING '…', stays within the UTF-16 budget, carries no trailing '…', and drops
// astral runes whole — never splitting a multi-byte character / emoji surrogate pair.
func TestCapUTF16Tail(t *testing.T) {
	// No-op: already fits → unchanged, no leading '…'.
	if got := capUTF16Tail("short tail.", 220); got != "short tail." {
		t.Errorf("capUTF16Tail(fits) = %q; want unchanged", got)
	}
	// ASCII cut: keeps the END with a leading '…', ends on the original's last chars,
	// within budget, no trailing '…'.
	long := strings.Repeat("a", 300) + "END."
	got := capUTF16Tail(long, 50)
	if !strings.HasPrefix(got, "…") {
		t.Errorf("cut result must start with a leading '…': %q", got)
	}
	if !strings.HasSuffix(got, "END.") {
		t.Errorf("cut result must keep the tail (…END.): %q", got)
	}
	if strings.HasSuffix(got, "…") {
		t.Errorf("cut result must not end on a trailing '…': %q", got)
	}
	if u := uint16Len(got); u > 50 {
		t.Errorf("cut result is %d u16, over the %d budget", u, 50)
	}
	// Multi-byte / emoji safety: a tail of astral runes (🎤 = 2 UTF-16 units each) is
	// cut on rune boundaries — within budget, valid UTF-8, keeping only WHOLE emoji.
	emoji := strings.Repeat("x", 100) + strings.Repeat("🎤", 20)
	e := capUTF16Tail(emoji, 21) // 1 for '…' + 10 whole 🎤 (20 u16)
	if u := uint16Len(e); u > 21 {
		t.Errorf("emoji cut is %d u16, over the %d budget", u, 21)
	}
	if !utf8.ValidString(e) {
		t.Errorf("emoji cut produced invalid UTF-8: %q", e)
	}
	if !strings.HasPrefix(e, "…") || !strings.HasSuffix(e, "🎤") {
		t.Errorf("emoji cut must be …<whole-emoji tail>, got %q", e)
	}
	// Every rune kept after the leading '…' must be a full 🎤 (no head 'x' leaked in,
	// no split surrogate).
	for _, r := range strings.TrimPrefix(e, "…") {
		if r != '🎤' {
			t.Errorf("emoji cut kept an unexpected rune %q in %q", r, e)
		}
	}
}

// TestRenderReadback_TailKeptNoTrailingEllipsis is the core fix regression: when the
// preview's tail (last 3 sentences) is a single long sentence that must be shortened
// to fit the per-part budget, the rendered preview keeps the sentence's END behind a
// LEADING '…' and the visible preview never ends on a trailing '…' — it no longer
// looks chopped mid-sentence.
func TestRenderReadback_TailKeptNoTrailingEllipsis(t *testing.T) {
	// 12 short sentences + one long (~1000 char) final sentence → 13 sentences (more =
	// 7). The final sentence blows the tail's per-part cap, and the whole note is past
	// readbackTinyMaxU16 so it lands in SHORT (not TINY).
	long := strings.TrimSpace(strings.Repeat("blah ", 200)) + " naturalsentenceending."
	var parts []string
	for i := 0; i < 12; i++ {
		parts = append(parts, "ok.")
	}
	parts = append(parts, long)
	in := strings.Join(parts, " ")

	method, payload, band := renderReadback(in)
	if band != bandShort || method != "sendMessage" {
		t.Fatalf("got method=%q band=%v; want sendMessage/short (fixture sanity)", method, band)
	}

	sents := splitSentences(in)
	_, l3, more := buildPreview(sents)
	if more <= 0 {
		t.Fatalf("fixture more = %d, want > 0", more)
	}
	if uint16Len(l3) <= readbackPreviewPartMaxU16 {
		t.Fatalf("fixture tail is %d u16, not over the %d cap — nothing to elide", uint16Len(l3), readbackPreviewPartMaxU16)
	}
	capL3 := capUTF16Tail(l3, readbackPreviewPartMaxU16)
	if !strings.HasPrefix(capL3, "…") {
		t.Errorf("tail preview must start with a leading '…': %q", capL3)
	}
	if strings.HasSuffix(capL3, "…") {
		t.Errorf("tail preview must NOT end on a trailing '…': %q", capL3)
	}
	if !strings.HasSuffix(capL3, "naturalsentenceending.") {
		t.Errorf("tail preview must keep the sentence's natural ending: %q", capL3)
	}
	if !strings.Contains(payload, htmlEscape(capL3)) {
		t.Error("payload is missing the tail-kept preview line")
	}
	// The visible preview (everything before the Full Transcript heading) must not end
	// on a trailing '…'. In SHORT the summary is plain text, so trimming trailing layout
	// whitespace lands on the tail's final visible char.
	region := payload
	if idx := strings.Index(payload, "<b>Full Transcript</b>"); idx >= 0 {
		region = payload[:idx]
	}
	if trimmed := strings.TrimRight(region, "\n "); strings.HasSuffix(trimmed, "…") {
		t.Errorf("preview must not end on a trailing '…': %q", trimmed)
	}
}

// TestRenderReadback_FewButHugeTailKept covers the more==0 fallthrough: a few very
// long sentences (no elided middle, no separate tail) render f3 as the whole preview
// AND its own last visible line, so it too keeps its END behind a leading '…' and
// never trails off on a '…'.
func TestRenderReadback_FewButHugeTailKept(t *testing.T) {
	one := strings.Repeat("w", 1490) + " naturalsentenceending." // one long sentence
	in := strings.TrimSpace(strings.Repeat(one+" ", 5))          // 5 sentences → deadzone, more==0
	sents := splitSentences(in)
	f3, l3, more := buildPreview(sents)
	if more != 0 || l3 != "" {
		t.Fatalf("fixture more=%d l3=%q, want more==0 and empty l3", more, l3)
	}
	if uint16Len(f3) <= readbackPreviewPartMaxU16 {
		t.Fatalf("fixture head is %d u16, not over the %d cap", uint16Len(f3), readbackPreviewPartMaxU16)
	}
	capHead := capUTF16Tail(f3, readbackPreviewPartMaxU16)
	if !strings.HasPrefix(capHead, "…") {
		t.Errorf("more==0 preview must start with a leading '…': %q", capHead)
	}
	if strings.HasSuffix(capHead, "…") {
		t.Errorf("more==0 preview must NOT end on a trailing '…': %q", capHead)
	}
	if !strings.HasSuffix(capHead, "naturalsentenceending.") {
		t.Errorf("more==0 preview must keep the natural ending: %q", capHead)
	}

	method, payload, band := renderReadback(in)
	if method != "sendRichMessage" {
		t.Fatalf("got method=%q band=%v; want a rich band for the few-but-huge note", method, band)
	}
	if !strings.Contains(payload, htmlEscape(capHead)) {
		t.Error("payload is missing the tail-kept more==0 preview")
	}
	// The f3 paragraph in the rich body must END with the tail-kept text (…</p>), never
	// a trailing-'…' paragraph.
	if !strings.Contains(payload, htmlEscape(capHead)+"</p>") {
		t.Error("more==0 preview paragraph must end with the tail-kept head text")
	}
	if strings.Contains(payload, "✂️") {
		t.Error("more==0 payload must carry no elision markers")
	}
}
