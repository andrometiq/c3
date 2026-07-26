package capability

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// richCaps is a Telegram-like manifest: rich text on, 4096 limit, polls on.
func richCaps() c3types.Capabilities {
	return c3types.Capabilities{
		Channel:         "telegram",
		RichText:        true,
		MaxMessageRunes: 4096,
		Polls:           true,
	}
}

func altKinds(alts []c3types.Alteration) []string {
	var out []string
	for _, a := range alts {
		out = append(out, a.Kind)
	}
	return out
}

func notesContain(notes []string, sub string) bool {
	for _, n := range notes {
		if strings.Contains(n, sub) {
			return true
		}
	}
	return false
}

func TestGate_ShortText_SinglePart(t *testing.T) {
	parts, notes, alts, err := Gate(richCaps(), c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: "hello world", Markup: c3types.MarkupMarkdown,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("expected exactly 1 part for short text; got %d", len(parts))
	}
	if parts[0].Text != "hello world" {
		t.Errorf("part text mutated: %q", parts[0].Text)
	}
	if parts[0].Markup != c3types.MarkupMarkdown {
		t.Errorf("markup should be preserved on a rich-text channel; got %q", parts[0].Markup)
	}
	if len(notes) != 0 {
		t.Errorf("expected no notes for an unaltered short reply; got %v", notes)
	}
	if len(alts) != 0 {
		t.Errorf("expected no alterations; got %v", altKinds(alts))
	}
}

func TestGate_NoRichText_DegradesMarkup(t *testing.T) {
	caps := richCaps()
	caps.RichText = false
	parts, notes, alts, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: "**bold**", Markup: c3types.MarkupMarkdown,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) != 1 || parts[0].Markup != c3types.MarkupNone {
		t.Fatalf("expected markup degraded to none; parts=%+v", parts)
	}
	if !notesContain(notes, "rich text is not supported") {
		t.Errorf("expected a degradation note; got %v", notes)
	}
	wantAlt := false
	for _, a := range alts {
		if a.Kind == "markup_degraded" {
			wantAlt = true
		}
	}
	if !wantAlt {
		t.Errorf("expected a markup_degraded Alteration; got %v", altKinds(alts))
	}
}

func TestGate_PollUnsupported_HardReject(t *testing.T) {
	caps := richCaps()
	caps.Polls = false
	parts, _, alts, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: "vote",
		Poll: &c3types.PollSpec{Question: "q?", Options: []string{"a", "b"}},
	})
	if err == nil {
		t.Fatalf("expected a hard-reject error when polls unsupported")
	}
	if parts != nil {
		t.Errorf("expected no parts on hard reject; got %+v", parts)
	}
	found := false
	for _, a := range alts {
		if a.Kind == "poll_rejected" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a poll_rejected Alteration; got %v", altKinds(alts))
	}
}

func TestGate_LongText_SplitsWithNote(t *testing.T) {
	caps := richCaps()
	caps.MaxMessageRunes = 50
	// Two paragraphs that together exceed 50 units -> split into >1 part.
	long := strings.Repeat("alpha ", 12) + "\n\n" + strings.Repeat("beta ", 12)
	parts, notes, _, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: long, Markup: c3types.MarkupMarkdown,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) < 2 {
		t.Fatalf("expected the long reply to split into >1 part; got %d", len(parts))
	}
	for i, p := range parts {
		if utf16Len(p.Text) > caps.MaxMessageRunes {
			t.Errorf("part %d over limit: %d > %d", i, utf16Len(p.Text), caps.MaxMessageRunes)
		}
	}
	if !notesContain(notes, "split into") {
		t.Errorf("expected a split note; got %v", notes)
	}
}

// TestGate_SourceBudget_SplitsEarlierThanHardLimit exercises the chunk
// HTML-overflow guard: when a channel advertises a MaxMessageRunesSource budget
// below its hard MaxMessageRunes, the gate must split the SOURCE at the smaller
// budget (leaving headroom for the rendering expansion the channel applies
// before sending), so a chunk that WOULD fit the hard wire limit but exceed it
// after rendering is split earlier. Concretely: text that fits the hard limit
// but exceeds the source budget must be split into >1 part, each within the
// budget.
func TestGate_SourceBudget_SplitsEarlierThanHardLimit(t *testing.T) {
	caps := richCaps()
	caps.MaxMessageRunes = 50       // hard wire limit
	caps.MaxMessageRunesSource = 30 // smaller source-markdown budget (headroom)
	// Two paragraphs that together fit the 50 hard limit but exceed the 30 source
	// budget -> must split under the budget (it would NOT split at the hard 50).
	p1 := strings.Repeat("a", 20)
	p2 := strings.Repeat("b", 20)
	long := p1 + "\n\n" + p2 // 42 units total: <50 hard limit, >30 source budget
	if utf16Len(long) > caps.MaxMessageRunes {
		t.Fatalf("test setup: source %d must fit the hard limit %d", utf16Len(long), caps.MaxMessageRunes)
	}
	parts, notes, _, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: long, Markup: c3types.MarkupMarkdown,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) < 2 {
		t.Fatalf("expected the source budget to force a split into >1 part (fits hard limit, exceeds budget); got %d", len(parts))
	}
	for i, p := range parts {
		if utf16Len(p.Text) > caps.MaxMessageRunesSource {
			t.Errorf("part %d over the source budget: %d > %d", i, utf16Len(p.Text), caps.MaxMessageRunesSource)
		}
	}
	if !notesContain(notes, "split into") {
		t.Errorf("expected a split note; got %v", notes)
	}
}

// TestGate_SourceBudgetUnset_BackCompat asserts that when MaxMessageRunesSource
// is 0 (a channel that sets no headroom — the pre-Phase-5 state) the gate falls
// back to splitting at the hard MaxMessageRunes exactly as before: text that
// fits the hard limit emits a single unsplit part. This guards the
// backward-compat contract for channels that never set the source budget.
func TestGate_SourceBudgetUnset_BackCompat(t *testing.T) {
	caps := richCaps()
	caps.MaxMessageRunes = 50
	caps.MaxMessageRunesSource = 0 // unset — must behave exactly as today
	// 42 units: fits the hard 50 limit; with no source budget it must NOT split.
	text := strings.Repeat("a", 20) + "\n\n" + strings.Repeat("b", 20)
	parts, notes, alts, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: text, Markup: c3types.MarkupMarkdown,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("expected a single part when MaxMessageRunesSource is unset and text fits the hard limit; got %d", len(parts))
	}
	if parts[0].Text != text {
		t.Errorf("text mutated with unset source budget: got %q want %q", parts[0].Text, text)
	}
	if len(notes) != 0 {
		t.Errorf("expected no split note for an unsplit single part; got %v", notes)
	}
	if len(alts) != 0 {
		t.Errorf("expected no alterations for an unsplit single part; got %v", altKinds(alts))
	}
}

// mediaCaps is a Telegram-like manifest with the full media kind set and both
// CompressedPhoto + OriginalFile enabled — used by the media-splitting tests.
func mediaCaps() c3types.Capabilities {
	c := richCaps()
	c.MediaKinds = []c3types.MediaKind{
		c3types.MediaPhoto, c3types.MediaFile, c3types.MediaVideo,
		c3types.MediaAudio, c3types.MediaVoice, c3types.MediaAnimation,
	}
	c.CompressedPhoto = true
	c.OriginalFile = true
	return c
}

func TestGate_ReplyToOnFirstPartOnly(t *testing.T) {
	caps := mediaCaps()
	caps.MaxMessageRunes = 50
	rt := int64(99)
	long := strings.Repeat("alpha ", 12) + "\n\n" + strings.Repeat("beta ", 12)
	parts, _, _, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: long, Markup: c3types.MarkupMarkdown,
		ReplyTo: &rt,
		Media:   []c3types.MediaItem{{Kind: c3types.MediaFile, Path: "/tmp/x"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) < 2 {
		t.Fatalf("need >1 part to test first-part-only carry; got %d", len(parts))
	}
	// First part carries ReplyTo and is a text part (text comes before media).
	if parts[0].ReplyTo == nil || *parts[0].ReplyTo != rt {
		t.Errorf("first part should carry ReplyTo; got %+v", parts[0].ReplyTo)
	}
	if parts[0].Text == "" {
		t.Errorf("first part should be the text part; got media/empty: %+v", parts[0])
	}
	if len(parts[0].Media) != 0 {
		t.Errorf("first (text) part should NOT carry Media in P3; got %+v", parts[0].Media)
	}
	// No subsequent part carries ReplyTo.
	for i := 1; i < len(parts); i++ {
		if parts[i].ReplyTo != nil {
			t.Errorf("part %d should not carry ReplyTo", i)
		}
	}
}

// TestGate_MediaItemPerPart asserts the P3 layout: each media item lands on its
// OWN part (no text on it), appended after the text parts, and no album grouping.
func TestGate_MediaItemPerPart(t *testing.T) {
	caps := mediaCaps()
	parts, notes, alts, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: "see attached", Markup: c3types.MarkupMarkdown,
		Media: []c3types.MediaItem{
			{Kind: c3types.MediaFile, Path: "/tmp/a.pdf"},
			{Kind: c3types.MediaPhoto, Path: "/tmp/b.jpg"},
			{Kind: c3types.MediaVideo, Path: "/tmp/c.mp4"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(alts) != 0 {
		t.Errorf("all kinds supported — expected no alterations; got %v", altKinds(alts))
	}
	if len(notes) != 0 {
		t.Errorf("all kinds supported — expected no notes; got %v", notes)
	}
	// 1 text part + 3 media parts.
	if len(parts) != 4 {
		t.Fatalf("expected 4 parts (1 text + 3 media); got %d", len(parts))
	}
	if parts[0].Text != "see attached" || len(parts[0].Media) != 0 {
		t.Errorf("part 0 should be the text part; got %+v", parts[0])
	}
	wantKinds := []c3types.MediaKind{c3types.MediaFile, c3types.MediaPhoto, c3types.MediaVideo}
	for i, want := range wantKinds {
		p := parts[i+1]
		if p.Text != "" {
			t.Errorf("media part %d should carry no text; got %q", i, p.Text)
		}
		if len(p.Media) != 1 {
			t.Fatalf("media part %d should carry exactly 1 item; got %d", i, len(p.Media))
		}
		if p.Media[0].Kind != want {
			t.Errorf("media part %d kind = %q; want %q", i, p.Media[0].Kind, want)
		}
	}
}

// TestGate_MediaOnly_NoEmptyTextPart asserts that media with empty text does not
// emit a leading empty text part.
func TestGate_MediaOnly_NoEmptyTextPart(t *testing.T) {
	caps := mediaCaps()
	parts, _, _, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: "",
		Media: []c3types.MediaItem{{Kind: c3types.MediaPhoto, Path: "/tmp/x.jpg"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("expected exactly 1 part (the media item); got %d", len(parts))
	}
	if parts[0].Text != "" || len(parts[0].Media) != 1 {
		t.Errorf("the single part should carry only the media item; got %+v", parts[0])
	}
}

// TestGate_UnsupportedKind_Dropped asserts an unsupported, non-demotable media
// kind is dropped with a note + Alteration, while supported kinds survive.
func TestGate_UnsupportedKind_Dropped(t *testing.T) {
	// A channel that only sends files (no video).
	caps := richCaps()
	caps.MediaKinds = []c3types.MediaKind{c3types.MediaFile}
	caps.OriginalFile = true
	parts, notes, alts, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: "",
		Media: []c3types.MediaItem{
			{Kind: c3types.MediaFile, Path: "/tmp/a.pdf"},
			{Kind: c3types.MediaVideo, Path: "/tmp/b.mp4"}, // unsupported, not demotable
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only the file survives → 1 media part.
	if len(parts) != 1 {
		t.Fatalf("expected 1 surviving media part; got %d", len(parts))
	}
	if parts[0].Media[0].Kind != c3types.MediaFile {
		t.Errorf("surviving item should be the file; got %q", parts[0].Media[0].Kind)
	}
	if !notesContain(notes, "dropped") {
		t.Errorf("expected a drop note; got %v", notes)
	}
	found := false
	for _, a := range alts {
		if a.Kind == "media_dropped" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a media_dropped Alteration; got %v", altKinds(alts))
	}
}

// TestGate_PhotoDemotedToFile asserts photo→file demotion when the channel
// cannot send a compressed photo but can send an original file.
func TestGate_PhotoDemotedToFile(t *testing.T) {
	// Synthetic caps: photo NOT in MediaKinds, CompressedPhoto=false, file ok.
	caps := richCaps()
	caps.MediaKinds = []c3types.MediaKind{c3types.MediaFile}
	caps.CompressedPhoto = false
	caps.OriginalFile = true
	parts, notes, alts, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: "",
		Media: []c3types.MediaItem{{Kind: c3types.MediaPhoto, Path: "/tmp/x.jpg", Caption: "cap"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) != 1 || len(parts[0].Media) != 1 {
		t.Fatalf("expected 1 demoted media part; got %+v", parts)
	}
	if parts[0].Media[0].Kind != c3types.MediaFile {
		t.Errorf("photo should be demoted to file; got %q", parts[0].Media[0].Kind)
	}
	if parts[0].Media[0].Caption != "cap" || parts[0].Media[0].Path != "/tmp/x.jpg" {
		t.Errorf("demotion should preserve path/caption; got %+v", parts[0].Media[0])
	}
	if !notesContain(notes, "sent as a file") {
		t.Errorf("expected a photo-demotion note; got %v", notes)
	}
	found := false
	for _, a := range alts {
		if a.Kind == "media_demoted" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a media_demoted Alteration; got %v", altKinds(alts))
	}
}

// TestGate_PollOnOwnPart asserts a poll travels on its own part appended after
// text + media (and carries no text/media).
func TestGate_PollOnOwnPart(t *testing.T) {
	caps := mediaCaps()
	parts, _, _, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: "vote now", Markup: c3types.MarkupMarkdown,
		Media: []c3types.MediaItem{{Kind: c3types.MediaPhoto, Path: "/tmp/x.jpg"}},
		Poll:  &c3types.PollSpec{Question: "q?", Options: []string{"a", "b"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 1 text + 1 media + 1 poll.
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts (text+media+poll); got %d", len(parts))
	}
	pollPart := parts[2]
	if pollPart.Poll == nil {
		t.Fatalf("last part should carry the poll; got %+v", pollPart)
	}
	if pollPart.Text != "" || len(pollPart.Media) != 0 {
		t.Errorf("poll part should carry only the poll; got %+v", pollPart)
	}
	// No other part carries the poll.
	for i := 0; i < 2; i++ {
		if parts[i].Poll != nil {
			t.Errorf("part %d should not carry the poll", i)
		}
	}
}

// intPtr is a small helper for the nullable CorrectOption pointer.
func intPtr(n int) *int { return &n }

// TestGate_PollRegular_BackCompat asserts a regular poll (Kind unset) passes
// through unchanged — the byte-identical back-compat contract for existing
// callers that set only the original four fields.
func TestGate_PollRegular_BackCompat(t *testing.T) {
	in := &c3types.PollSpec{Question: "Lunch?", Options: []string{"Pizza", "Tacos"}, MultipleAnswers: true}
	parts, notes, alts, err := Gate(richCaps(), c3types.Outbound{
		Channel: "telegram", ChatID: 1, Poll: in,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) != 1 || parts[0].Poll == nil {
		t.Fatalf("expected 1 poll part; got %d", len(parts))
	}
	got := parts[0].Poll
	if got.Question != "Lunch?" || len(got.Options) != 2 || !got.MultipleAnswers {
		t.Errorf("regular poll altered: %+v", got)
	}
	if len(notes) != 0 || len(alts) != 0 {
		t.Errorf("regular poll should not generate notes/alterations; got notes=%v alts=%v", notes, alts)
	}
	// The caller's spec must not be mutated.
	if !in.MultipleAnswers {
		t.Errorf("gate mutated the caller's PollSpec")
	}
}

func TestGate_Poll_TooFewOptions(t *testing.T) {
	_, _, _, err := Gate(richCaps(), c3types.Outbound{
		Channel: "telegram", ChatID: 1,
		Poll: &c3types.PollSpec{Question: "q?", Options: []string{"only"}},
	})
	if err == nil || !strings.Contains(err.Error(), "policy") {
		t.Fatalf("expected a policy hard-reject for <2 options; got %v", err)
	}
}

func TestGate_Poll_TooManyOptions(t *testing.T) {
	opts := make([]string, 11)
	for i := range opts {
		opts[i] = "o"
	}
	_, _, _, err := Gate(richCaps(), c3types.Outbound{
		Channel: "telegram", ChatID: 1,
		Poll: &c3types.PollSpec{Question: "q?", Options: opts},
	})
	if err == nil || !strings.Contains(err.Error(), "policy") {
		t.Fatalf("expected a policy hard-reject for >10 options; got %v", err)
	}
}

func TestGate_Quiz_RequiresCorrectOption(t *testing.T) {
	_, _, _, err := Gate(richCaps(), c3types.Outbound{
		Channel: "telegram", ChatID: 1,
		Poll: &c3types.PollSpec{Question: "q?", Options: []string{"a", "b"}, Kind: c3types.PollQuiz},
	})
	if err == nil || !strings.Contains(err.Error(), "correct_option") {
		t.Fatalf("expected a hard-reject for a quiz without correct_option; got %v", err)
	}
}

func TestGate_Quiz_CorrectOptionOutOfRange(t *testing.T) {
	_, _, _, err := Gate(richCaps(), c3types.Outbound{
		Channel: "telegram", ChatID: 1,
		Poll: &c3types.PollSpec{Question: "q?", Options: []string{"a", "b"}, Kind: c3types.PollQuiz, CorrectOption: intPtr(2)},
	})
	if err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("expected an out-of-range hard-reject; got %v", err)
	}
}

func TestGate_Quiz_Valid_ClearsMultiple(t *testing.T) {
	parts, notes, alts, err := Gate(richCaps(), c3types.Outbound{
		Channel: "telegram", ChatID: 1,
		Poll: &c3types.PollSpec{
			Question: "Capital?", Options: []string{"Paris", "Rome"},
			Kind: c3types.PollQuiz, CorrectOption: intPtr(0), Explanation: "Paris is the capital.",
			MultipleAnswers: true,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error on a valid quiz: %v", err)
	}
	if parts[0].Poll.MultipleAnswers {
		t.Errorf("quiz poll must have MultipleAnswers cleared")
	}
	if !notesContain(notes, "ignored for a quiz") {
		t.Errorf("expected a note that multiple answers were cleared; got %v", notes)
	}
	if got := altKinds(alts); len(got) != 1 || got[0] != "poll_multiple_ignored_quiz" {
		t.Errorf("expected poll_multiple_ignored_quiz alteration; got %v", got)
	}
}

func TestGate_Quiz_ExplanationTooLong(t *testing.T) {
	_, _, _, err := Gate(richCaps(), c3types.Outbound{
		Channel: "telegram", ChatID: 1,
		Poll: &c3types.PollSpec{
			Question: "q?", Options: []string{"a", "b"},
			Kind: c3types.PollQuiz, CorrectOption: intPtr(0), Explanation: strings.Repeat("x", 201),
		},
	})
	if err == nil || !strings.Contains(err.Error(), "explanation") {
		t.Fatalf("expected an over-length explanation hard-reject; got %v", err)
	}
}

func TestGate_Poll_TimerMutuallyExclusive(t *testing.T) {
	_, _, _, err := Gate(richCaps(), c3types.Outbound{
		Channel: "telegram", ChatID: 1,
		Poll: &c3types.PollSpec{
			Question: "q?", Options: []string{"a", "b"},
			OpenPeriodSec: 60, CloseDateUnix: 1_900_000_000,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected a mutual-exclusivity hard-reject; got %v", err)
	}
}

func TestGate_Poll_LongOpenPeriodAllowed(t *testing.T) {
	// A 1-hour poll (3600s) is live-valid even though rc.34's doc says 5-600;
	// the gate must NOT hard-reject it.
	_, _, _, err := Gate(richCaps(), c3types.Outbound{
		Channel: "telegram", ChatID: 1,
		Poll: &c3types.PollSpec{Question: "q?", Options: []string{"a", "b"}, OpenPeriodSec: 3600},
	})
	if err != nil {
		t.Fatalf("a long open_period must be allowed; got %v", err)
	}
}

// TestGate_Buttons_KeptOnSupportedChannel asserts that when the channel
// advertises InlineKeyboards, the gate carries the neutral keyboard through onto
// the FIRST emitted part unchanged, with no note/alteration.
func TestGate_Buttons_KeptOnSupportedChannel(t *testing.T) {
	caps := richCaps()
	caps.InlineKeyboards = true
	btns := [][]c3types.Button{
		{{Text: "Approve", Data: "approve:1"}, {Text: "Deny", Data: "deny:1"}},
		{{Text: "Docs", URL: "https://example.com"}},
	}
	parts, notes, alts, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: "decide:", Markup: c3types.MarkupMarkdown,
		Buttons: btns,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("expected 1 part; got %d", len(parts))
	}
	if len(parts[0].Buttons) != 2 || len(parts[0].Buttons[0]) != 2 || len(parts[0].Buttons[1]) != 1 {
		t.Fatalf("keyboard shape not carried onto the first part; got %+v", parts[0].Buttons)
	}
	if parts[0].Buttons[0][0].Text != "Approve" || parts[0].Buttons[0][0].Data != "approve:1" {
		t.Errorf("callback button not preserved; got %+v", parts[0].Buttons[0][0])
	}
	if parts[0].Buttons[1][0].URL != "https://example.com" {
		t.Errorf("url button not preserved; got %+v", parts[0].Buttons[1][0])
	}
	if len(notes) != 0 || len(alts) != 0 {
		t.Errorf("supported channel should not note/alter buttons; notes=%v alts=%v", notes, altKinds(alts))
	}
}

// TestGate_Buttons_DroppedOnUnsupportedChannel asserts the graceful degradation:
// a channel without InlineKeyboards has the keyboard DROPPED (not a hard error),
// with a note + a buttons_dropped Alteration, and the message still sends.
func TestGate_Buttons_DroppedOnUnsupportedChannel(t *testing.T) {
	caps := richCaps()
	caps.InlineKeyboards = false
	parts, notes, alts, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: "decide:", Markup: c3types.MarkupMarkdown,
		Buttons: [][]c3types.Button{{{Text: "Approve", Data: "approve:1"}}},
	})
	if err != nil {
		t.Fatalf("buttons on an unsupported channel must DEGRADE, not error; got %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("expected the text part to still send; got %d parts", len(parts))
	}
	if parts[0].Buttons != nil {
		t.Errorf("buttons should be dropped on an unsupported channel; got %+v", parts[0].Buttons)
	}
	if !notesContain(notes, "buttons are not supported") {
		t.Errorf("expected a buttons-dropped note; got %v", notes)
	}
	found := false
	for _, a := range alts {
		if a.Kind == "buttons_dropped" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a buttons_dropped Alteration; got %v", altKinds(alts))
	}
}

// TestGate_NoButtons_BackCompat asserts a reply with no buttons is unchanged: the
// emitted part carries a nil Buttons field (byte-identical to pre-P7) and no
// note/alteration is generated.
func TestGate_NoButtons_BackCompat(t *testing.T) {
	caps := richCaps()
	caps.InlineKeyboards = true
	parts, notes, alts, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: "hello", Markup: c3types.MarkupMarkdown,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) != 1 || parts[0].Buttons != nil {
		t.Fatalf("a reply without buttons must carry nil Buttons; got %+v", parts)
	}
	if len(notes) != 0 || len(alts) != 0 {
		t.Errorf("no-buttons reply should not note/alter; notes=%v alts=%v", notes, altKinds(alts))
	}
}

func TestGate_NoLimit_SinglePart(t *testing.T) {
	caps := richCaps()
	caps.MaxMessageRunes = 0 // no advertised limit
	long := strings.Repeat("x", 100000)
	parts, _, _, err := Gate(caps, c3types.Outbound{
		Channel: "telegram", ChatID: 1, Text: long, Markup: c3types.MarkupMarkdown,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) != 1 {
		t.Errorf("expected a single part when MaxMessageRunes<=0; got %d", len(parts))
	}
}
