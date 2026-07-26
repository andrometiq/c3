package broker

import (
	"bytes"
	"context"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// pollChannel is a test channel whose Capabilities advertise poll support and
// which records the parts it is asked to send. Used to assert dispatchPoll
// routes a poll through the gate and onto a SendReply part.
type pollChannel struct {
	mu             sync.Mutex
	polls          bool
	sent           []c3types.ReplyArgs
	stopPollCalls  []stopPollCall
	stopPollResult *c3types.PollResult
}

type stopPollCall struct {
	chatID    int64
	messageID int64
}

func (p *pollChannel) Name() string                              { return "telegram" }
func (p *pollChannel) Start(context.Context, channel.Host) error { return nil }
func (p *pollChannel) Stop() error                               { return nil }
func (p *pollChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{Channel: "telegram", Polls: p.polls}
}
func (p *pollChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, args)
	return int64(len(p.sent)), nil
}
func (p *pollChannel) SendTyping(int64, *int64) error { return nil }
func (p *pollChannel) EditMessage(c3types.EditArgs) (*c3types.EditResult, error) {
	return &c3types.EditResult{}, nil
}
func (p *pollChannel) React(c3types.ReactArgs) error             { return nil }
func (p *pollChannel) DownloadAttachment(string) (string, error) { return "", nil }
func (p *pollChannel) CreateTopic(int64, string) (int64, error)  { return 0, nil }
func (p *pollChannel) ValidateTopic(int64, int64) error          { return nil }
func (p *pollChannel) StopPoll(chatID, messageID int64) (*c3types.PollResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopPollCalls = append(p.stopPollCalls, stopPollCall{chatID, messageID})
	if p.stopPollResult != nil {
		return p.stopPollResult, nil
	}
	return &c3types.PollResult{}, nil
}

func (p *pollChannel) stopPollSnapshot() []stopPollCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]stopPollCall, len(p.stopPollCalls))
	copy(out, p.stopPollCalls)
	return out
}

func (p *pollChannel) sentSnapshot() []c3types.ReplyArgs {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]c3types.ReplyArgs, len(p.sent))
	copy(out, p.sent)
	return out
}

func pollArgs() map[string]any {
	return map[string]any{
		"question": "Lunch?",
		"options":  []any{"Pizza", "Tacos"},
	}
}

// TestDispatchPoll_RejectedWhenUnsupported asserts the gate hard-rejects a poll
// on a channel that does not support polls, and NOTHING is sent.
func TestDispatchPoll_RejectedWhenUnsupported(t *testing.T) {
	ch := &pollChannel{polls: false}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	res, err := dispatchPoll(ch, key, pollArgs())
	if err == nil {
		t.Fatalf("expected a hard-reject error when polls unsupported; got result %v", res)
	}
	if !strings.Contains(err.Error(), "poll") {
		t.Errorf("error should mention polls; got %v", err)
	}
	if got := ch.sentSnapshot(); len(got) != 0 {
		t.Errorf("nothing should be sent on a rejected poll; got %d sends", len(got))
	}
}

// TestDispatchPoll_SendsPollPart asserts a poll is routed onto a SendReply part
// carrying the PollSpec when the channel supports polls.
func TestDispatchPoll_SendsPollPart(t *testing.T) {
	ch := &pollChannel{polls: true}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	args := pollArgs()
	args["multiple"] = true
	args["anonymous"] = false
	if _, err := dispatchPoll(ch, key, args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sent := ch.sentSnapshot()
	if len(sent) != 1 {
		t.Fatalf("expected exactly 1 part sent (the poll); got %d", len(sent))
	}
	part := sent[0]
	if part.Poll == nil {
		t.Fatalf("the sent part should carry the poll; got %+v", part)
	}
	if part.Poll.Question != "Lunch?" || len(part.Poll.Options) != 2 {
		t.Errorf("poll spec mismatch: %+v", part.Poll)
	}
	if !part.Poll.MultipleAnswers || part.Poll.Anonymous {
		t.Errorf("poll flags not honored: %+v", part.Poll)
	}
	if part.Text != "" || len(part.Media) != 0 {
		t.Errorf("poll part should carry only the poll; got %+v", part)
	}
}

// TestDispatchPoll_QuizArgs asserts the quiz/timed args flow through dispatch
// into the PollSpec the channel receives (with MultipleAnswers cleared by the
// gate for a quiz).
func TestDispatchPoll_QuizArgs(t *testing.T) {
	ch := &pollChannel{polls: true}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	args := map[string]any{
		"question":       "Capital of France?",
		"options":        []any{"Paris", "Rome"},
		"type":           "quiz",
		"correct_option": float64(0), // json numbers arrive as float64
		"explanation":    "Paris is the capital.",
		"open_period":    float64(120),
		"multiple":       true, // should be cleared for a quiz
	}
	if _, err := dispatchPoll(ch, key, args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sent := ch.sentSnapshot()
	if len(sent) != 1 || sent[0].Poll == nil {
		t.Fatalf("expected 1 poll part; got %d", len(sent))
	}
	p := sent[0].Poll
	if p.Kind != c3types.PollQuiz {
		t.Errorf("expected quiz kind; got %q", p.Kind)
	}
	if p.CorrectOption == nil || *p.CorrectOption != 0 {
		t.Errorf("correct_option not threaded through; got %v", p.CorrectOption)
	}
	if p.Explanation != "Paris is the capital." {
		t.Errorf("explanation not threaded through; got %q", p.Explanation)
	}
	if p.OpenPeriodSec != 120 {
		t.Errorf("open_period not threaded through; got %d", p.OpenPeriodSec)
	}
	if p.MultipleAnswers {
		t.Errorf("MultipleAnswers should be cleared for a quiz")
	}
}

// TestDispatchPoll_QuizMissingCorrectOption asserts a quiz without
// correct_option is hard-rejected by the gate and nothing is sent.
func TestDispatchPoll_QuizMissingCorrectOption(t *testing.T) {
	ch := &pollChannel{polls: true}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	_, err := dispatchPoll(ch, key, map[string]any{
		"question": "q?",
		"options":  []any{"a", "b"},
		"type":     "quiz",
	})
	if err == nil || !strings.Contains(err.Error(), "correct_option") {
		t.Fatalf("expected a correct_option hard-reject; got %v", err)
	}
	if got := ch.sentSnapshot(); len(got) != 0 {
		t.Errorf("nothing should be sent for an invalid quiz; got %d", len(got))
	}
}

// TestDispatchPoll_RequiresTwoOptions asserts a poll with fewer than 2 options
// is hard-rejected (by the pure gate, which now owns option-count validation —
// the duplicate check was deleted from dispatch in P2/CB-4) and that NOTHING is
// sent to the channel.
func TestDispatchPoll_RequiresTwoOptions(t *testing.T) {
	ch := &pollChannel{polls: true}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	_, err := dispatchPoll(ch, key, map[string]any{
		"question": "One?",
		"options":  []any{"only"},
	})
	if err == nil {
		t.Fatalf("expected an error for a 1-option poll")
	}
	if got := ch.sentSnapshot(); len(got) != 0 {
		t.Errorf("nothing should be sent for an invalid poll; got %d", len(got))
	}
}

// TestDispatchStopPoll_CallsChannelAndFormatsTally asserts stop_poll routes to
// the channel's StopPoll (with the route chat + the message_id arg) and formats
// the returned aggregate tally as MCP text.
func TestDispatchStopPoll_CallsChannelAndFormatsTally(t *testing.T) {
	ch := &pollChannel{polls: true, stopPollResult: &c3types.PollResult{
		Question: "Lunch?", TotalVoters: 3, IsClosed: true,
		Options: []c3types.PollOptionTally{{Text: "Pizza", VoterCount: 2}, {Text: "Tacos", VoterCount: 1}},
	}}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	res, err := dispatchTool(ch, key, "stop_poll", map[string]any{"message_id": float64(42)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	calls := ch.stopPollSnapshot()
	if len(calls) != 1 || calls[0].chatID != -100 || calls[0].messageID != 42 {
		t.Fatalf("StopPoll not called with the route chat + message_id; got %+v", calls)
	}
	text := mcpResultText(t, res)
	for _, want := range []string{"Poll results:", "Lunch?", "3 votes", "(closed)", "Pizza: 2", "Tacos: 1"} {
		if !strings.Contains(text, want) {
			t.Errorf("stop_poll result missing %q; got:\n%s", want, text)
		}
	}
}

// TestDispatchStopPoll_RequiresMessageID asserts a missing message_id is a hard
// error and the channel is never called.
func TestDispatchStopPoll_RequiresMessageID(t *testing.T) {
	ch := &pollChannel{polls: true}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	if _, err := dispatchTool(ch, key, "stop_poll", map[string]any{}); err == nil {
		t.Fatal("expected an error when message_id is missing")
	}
	if got := ch.stopPollSnapshot(); len(got) != 0 {
		t.Errorf("StopPoll must not be called without a message_id; got %d calls", len(got))
	}
}

// mcpResultText extracts the first content[].text from an MCP-shape result map.
func mcpResultText(t *testing.T, res map[string]any) string {
	t.Helper()
	content, ok := res["content"].([]map[string]any)
	if !ok || len(content) == 0 {
		t.Fatalf("result has no content[]: %+v", res)
	}
	s, _ := content[0]["text"].(string)
	return s
}

// TestDispatchReply_MediaArgSplitsIntoParts asserts the `media` array arg is
// parsed and the gate splits it into one SendReply part per item, after the
// text part.
func TestDispatchReply_MediaArgSplitsIntoParts(t *testing.T) {
	ch := &mediaReplyChannel{}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	args := map[string]any{
		"text": "see attached",
		"media": []any{
			map[string]any{"kind": "file", "path": "/tmp/a.pdf", "caption": "report"},
			map[string]any{"kind": "photo", "url": "https://example.com/x.jpg", "spoiler": true},
		},
	}
	if _, err := dispatchReply(ch, key, args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sent := ch.sentSnapshot()
	// 1 text part + 2 media parts.
	if len(sent) != 3 {
		t.Fatalf("expected 3 parts (text + 2 media); got %d", len(sent))
	}
	if sent[0].Text != "see attached" || len(sent[0].Media) != 0 {
		t.Errorf("part 0 should be the text part; got %+v", sent[0])
	}
	if len(sent[1].Media) != 1 || sent[1].Media[0].Kind != c3types.MediaFile ||
		sent[1].Media[0].Path != "/tmp/a.pdf" || sent[1].Media[0].Caption != "report" {
		t.Errorf("part 1 should carry the file item; got %+v", sent[1].Media)
	}
	if len(sent[2].Media) != 1 || sent[2].Media[0].Kind != c3types.MediaPhoto ||
		sent[2].Media[0].URL != "https://example.com/x.jpg" || !sent[2].Media[0].Spoiler {
		t.Errorf("part 2 should carry the photo URL item with spoiler; got %+v", sent[2].Media)
	}
}

// TestSendParts_MediaPathAuditLogged covers the 2026-06-15 security finding: the
// broker must emit an audit line for every outbound media item with a non-empty
// local Path (a prompt-injected agent could exfil the bot token / ssh keys to the
// chat; that is accepted under the trusted-local-agent model but MUST be
// detectable). The path is resolved with filepath.Abs. URL-only media must NOT
// produce a media-send line.
func TestSendParts_MediaPathAuditLogged(t *testing.T) {
	var buf bytes.Buffer
	oldOut := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
	}()

	ch := &mediaReplyChannel{}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	args := map[string]any{
		"text": "see attached",
		"media": []any{
			map[string]any{"kind": "file", "path": "relative/secret.pdf"},
			map[string]any{"kind": "photo", "url": "https://example.com/x.jpg"},
		},
	}
	if _, err := dispatchReply(ch, key, args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := buf.String()
	wantAbs, _ := filepath.Abs("relative/secret.pdf")
	if !strings.Contains(out, "media-send") {
		t.Fatalf("expected a media-send audit line; log was:\n%s", out)
	}
	if !strings.Contains(out, "path="+strconv.Quote(wantAbs)) {
		t.Errorf("audit line must record the ABSOLUTE path %q; log was:\n%s", wantAbs, out)
	}
	// URL-only media must not be audited (nothing local is being read).
	if strings.Contains(out, "x.jpg") {
		t.Errorf("URL-only media must NOT produce a media-send audit line; log was:\n%s", out)
	}
	// Exactly one media-send line (the file item only).
	if n := strings.Count(out, "media-send"); n != 1 {
		t.Errorf("expected exactly 1 media-send line (the Path item); got %d; log was:\n%s", n, out)
	}
}

// buttonChannel is a test channel that advertises inline-keyboard support and
// records the parts it is asked to send, so dispatchReply's buttons-arg parsing
// can be asserted end-to-end (parsed → gate keeps → SendReply part).
type buttonChannel struct {
	mu   sync.Mutex
	sent []c3types.ReplyArgs
}

func (m *buttonChannel) Name() string                              { return "telegram" }
func (m *buttonChannel) Start(context.Context, channel.Host) error { return nil }
func (m *buttonChannel) Stop() error                               { return nil }
func (m *buttonChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{Channel: "telegram", RichText: true, InlineKeyboards: true}
}
func (m *buttonChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, args)
	return int64(len(m.sent)), nil
}
func (m *buttonChannel) SendTyping(int64, *int64) error { return nil }
func (m *buttonChannel) EditMessage(c3types.EditArgs) (*c3types.EditResult, error) {
	return &c3types.EditResult{}, nil
}
func (m *buttonChannel) React(c3types.ReactArgs) error             { return nil }
func (m *buttonChannel) DownloadAttachment(string) (string, error) { return "", nil }
func (m *buttonChannel) CreateTopic(int64, string) (int64, error)  { return 0, nil }
func (m *buttonChannel) ValidateTopic(int64, int64) error          { return nil }
func (m *buttonChannel) StopPoll(int64, int64) (*c3types.PollResult, error) {
	return &c3types.PollResult{}, nil
}
func (m *buttonChannel) sentSnapshot() []c3types.ReplyArgs {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]c3types.ReplyArgs, len(m.sent))
	copy(out, m.sent)
	return out
}

// TestDispatchReply_ButtonsArgParsed asserts the `buttons` 2-D array arg is
// parsed into Outbound.Buttons and (on a keyboard-supporting channel) ridden onto
// the first SendReply part.
func TestDispatchReply_ButtonsArgParsed(t *testing.T) {
	ch := &buttonChannel{}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	args := map[string]any{
		"text": "decide:",
		"buttons": []any{
			[]any{
				map[string]any{"text": "Approve", "data": "approve:1"},
				map[string]any{"text": "Deny", "data": "deny:1"},
			},
			[]any{
				map[string]any{"text": "Docs", "url": "https://example.com"},
			},
		},
	}
	if _, err := dispatchReply(ch, key, args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sent := ch.sentSnapshot()
	if len(sent) != 1 {
		t.Fatalf("expected 1 part; got %d", len(sent))
	}
	kb := sent[0].Buttons
	if len(kb) != 2 || len(kb[0]) != 2 || len(kb[1]) != 1 {
		t.Fatalf("keyboard shape not parsed; got %+v", kb)
	}
	if kb[0][0].Text != "Approve" || kb[0][0].Data != "approve:1" {
		t.Errorf("callback button not parsed; got %+v", kb[0][0])
	}
	if kb[1][0].URL != "https://example.com" || kb[1][0].Data != "" {
		t.Errorf("url button not parsed; got %+v", kb[1][0])
	}
}

// TestDispatchReply_ButtonsArgBadShape asserts a button that sets neither
// data nor url is a clear error and nothing is sent.
func TestDispatchReply_ButtonsArgBadShape(t *testing.T) {
	ch := &buttonChannel{}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	args := map[string]any{
		"text": "decide:",
		"buttons": []any{
			[]any{map[string]any{"text": "Nothing"}}, // neither data nor url
		},
	}
	_, err := dispatchReply(ch, key, args)
	if err == nil || !strings.Contains(err.Error(), "EXACTLY ONE") {
		t.Fatalf("expected an EXACTLY-ONE shape error; got %v", err)
	}
	if got := ch.sentSnapshot(); len(got) != 0 {
		t.Errorf("nothing should be sent on a bad button shape; got %d", len(got))
	}
}

// TestDispatchReply_EmptyButtonRowRejected asserts an empty inline-keyboard row
// (`[[]]`) is a clear error, not silently sent as a buttonless "keyboard".
// Telegram 400s on an empty keyboard row, so it must be caught up front.
func TestDispatchReply_EmptyButtonRowRejected(t *testing.T) {
	ch := &buttonChannel{}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	args := map[string]any{
		"text":    "decide:",
		"buttons": []any{[]any{}}, // one empty row
	}
	_, err := dispatchReply(ch, key, args)
	if err == nil || !strings.Contains(err.Error(), "row 1 is empty") {
		t.Fatalf("expected an empty-row error; got %v", err)
	}
	if got := ch.sentSnapshot(); len(got) != 0 {
		t.Errorf("nothing should be sent on an empty button row; got %d", len(got))
	}
}

// TestDispatchPoll_QuizNullCorrectOptionRejected is the M2 regression: a quiz
// whose correct_option is present-but-unparseable (JSON null, a non-numeric
// string) must NOT be silently treated as option 0. argIntPtr returns nil for
// such a value, so the gate surfaces the "quiz requires correct_option" reject
// and NOTHING is sent.
func TestDispatchPoll_QuizNullCorrectOptionRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  any
	}{
		{"null", nil},
		{"non-numeric-string", "abc"},
		{"bool", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ch := &pollChannel{polls: true}
			key := RouteKey{Channel: "telegram", ChatID: -100}
			_, err := dispatchPoll(ch, key, map[string]any{
				"question":       "q?",
				"options":        []any{"a", "b"},
				"type":           "quiz",
				"correct_option": tc.val,
			})
			if err == nil || !strings.Contains(err.Error(), "correct_option") {
				t.Fatalf("expected a correct_option hard-reject; got %v", err)
			}
			if got := ch.sentSnapshot(); len(got) != 0 {
				t.Errorf("nothing should be sent for an unparseable quiz correct_option; got %d", len(got))
			}
		})
	}
}

// TestArgIntPtr_OnlyNonNilWhenParseable asserts argIntPtr directly: a present,
// parseable value returns a pointer to it (0 is a valid index, distinct from
// "not set"); a present-but-unparseable value returns nil (NOT &0); an absent
// key returns nil.
func TestArgIntPtr_OnlyNonNilWhenParseable(t *testing.T) {
	if p := argIntPtr(map[string]any{"x": float64(0)}, "x"); p == nil || *p != 0 {
		t.Errorf("a parseable 0 must return &0; got %v", p)
	}
	if p := argIntPtr(map[string]any{"x": float64(2)}, "x"); p == nil || *p != 2 {
		t.Errorf("a parseable 2 must return &2; got %v", p)
	}
	if p := argIntPtr(map[string]any{"x": nil}, "x"); p != nil {
		t.Errorf("a present null must return nil (not &0); got %v (*=%d)", p, *p)
	}
	if p := argIntPtr(map[string]any{"x": "abc"}, "x"); p != nil {
		t.Errorf("a non-numeric string must return nil; got %v (*=%d)", p, *p)
	}
	if p := argIntPtr(map[string]any{}, "x"); p != nil {
		t.Errorf("an absent key must return nil; got %v (*=%d)", p, *p)
	}
}

// TestDispatchReply_NoButtons_BackCompat asserts a reply with no buttons arg
// carries a nil Buttons field — byte-identical to pre-P7.
func TestDispatchReply_NoButtons_BackCompat(t *testing.T) {
	ch := &buttonChannel{}
	key := RouteKey{Channel: "telegram", ChatID: -100}
	if _, err := dispatchReply(ch, key, map[string]any{"text": "hi"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sent := ch.sentSnapshot()
	if len(sent) != 1 || sent[0].Buttons != nil {
		t.Fatalf("a no-buttons reply must carry nil Buttons; got %+v", sent)
	}
}

// typingRecorderChannel records SendTyping (chatID, threadID) calls so tests can
// assert the legacy send_typing dispatch op and the validate_topic piggyback
// (which routes ValidateTopic → SendTyping) still drive the channel after P5
// removed the agent-facing send_typing tool.
type typingRecorderChannel struct {
	mu     sync.Mutex
	typing []typingCall
}

type typingCall struct {
	chatID   int64
	threadID *int64
}

func (c *typingRecorderChannel) Name() string                              { return "telegram" }
func (c *typingRecorderChannel) Start(context.Context, channel.Host) error { return nil }
func (c *typingRecorderChannel) Stop() error                               { return nil }
func (c *typingRecorderChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{Channel: "telegram", Typing: true}
}
func (c *typingRecorderChannel) SendReply(c3types.ReplyArgs) (int64, error) { return 0, nil }
func (c *typingRecorderChannel) SendTyping(chatID int64, threadID *int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.typing = append(c.typing, typingCall{chatID: chatID, threadID: threadID})
	return nil
}
func (c *typingRecorderChannel) EditMessage(c3types.EditArgs) (*c3types.EditResult, error) {
	return &c3types.EditResult{}, nil
}
func (c *typingRecorderChannel) React(c3types.ReactArgs) error             { return nil }
func (c *typingRecorderChannel) DownloadAttachment(string) (string, error) { return "", nil }
func (c *typingRecorderChannel) CreateTopic(int64, string) (int64, error)  { return 0, nil }
func (c *typingRecorderChannel) StopPoll(int64, int64) (*c3types.PollResult, error) {
	return &c3types.PollResult{}, nil
}

// ValidateTopic piggybacks SendTyping — the same path Telegram uses to confirm
// a thread exists (channel/telegram/outbound.go:ValidateTopic).
func (c *typingRecorderChannel) ValidateTopic(chatID, threadID int64) error {
	return c.SendTyping(chatID, &threadID)
}

func (c *typingRecorderChannel) typingSnapshot() []typingCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]typingCall, len(c.typing))
	copy(out, c.typing)
	return out
}

// TestDispatchTool_LegacySendTypingStillHandled asserts the broker dispatch keeps
// HANDLING a send_typing op (legacy in-flight callers + the internal relay) even
// though P5 removed send_typing from the agent-facing tool set. The op must route
// to the channel's SendTyping with the route's chat/topic.
func TestDispatchTool_LegacySendTypingStillHandled(t *testing.T) {
	ch := &typingRecorderChannel{}
	tid := int64(281)
	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: tid}

	res, err := dispatchTool(ch, key, "send_typing", map[string]any{})
	if err != nil {
		t.Fatalf("send_typing op should still be handled by dispatch; got err %v", err)
	}
	if res == nil {
		t.Fatal("send_typing op should return a result")
	}
	got := ch.typingSnapshot()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 SendTyping call from the legacy op; got %d", len(got))
	}
	if got[0].chatID != -100 || got[0].threadID == nil || *got[0].threadID != tid {
		t.Errorf("send_typing should target the route's chat/topic; got %+v", got[0])
	}
}

// TestValidateTopic_PiggybacksSendTyping asserts the validate_topic primitive
// still works after P5: ValidateTopic routes through SendTyping (the typing
// action with a thread_id implicitly validates the thread). P5 must not have
// disturbed the channel's SendTyping method that this relies on.
func TestValidateTopic_PiggybacksSendTyping(t *testing.T) {
	ch := &typingRecorderChannel{}
	if err := ch.ValidateTopic(-100, 412); err != nil {
		t.Fatalf("ValidateTopic should succeed; got %v", err)
	}
	got := ch.typingSnapshot()
	if len(got) != 1 {
		t.Fatalf("ValidateTopic should fire exactly 1 SendTyping; got %d", len(got))
	}
	if got[0].chatID != -100 || got[0].threadID == nil || *got[0].threadID != 412 {
		t.Errorf("ValidateTopic should SendTyping for the validated thread; got %+v", got[0])
	}
}

// mediaReplyChannel is a test channel with the full media-kind manifest that
// records the parts it is asked to send.
type mediaReplyChannel struct {
	mu   sync.Mutex
	sent []c3types.ReplyArgs
}

func (m *mediaReplyChannel) Name() string                              { return "telegram" }
func (m *mediaReplyChannel) Start(context.Context, channel.Host) error { return nil }
func (m *mediaReplyChannel) Stop() error                               { return nil }
func (m *mediaReplyChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{
		Channel:  "telegram",
		RichText: true,
		MediaKinds: []c3types.MediaKind{
			c3types.MediaPhoto, c3types.MediaFile, c3types.MediaVideo,
			c3types.MediaAudio, c3types.MediaVoice, c3types.MediaAnimation,
		},
		CompressedPhoto: true,
		OriginalFile:    true,
	}
}
func (m *mediaReplyChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, args)
	return int64(len(m.sent)), nil
}
func (m *mediaReplyChannel) SendTyping(int64, *int64) error { return nil }
func (m *mediaReplyChannel) EditMessage(c3types.EditArgs) (*c3types.EditResult, error) {
	return &c3types.EditResult{}, nil
}
func (m *mediaReplyChannel) React(c3types.ReactArgs) error             { return nil }
func (m *mediaReplyChannel) DownloadAttachment(string) (string, error) { return "", nil }
func (m *mediaReplyChannel) CreateTopic(int64, string) (int64, error)  { return 0, nil }
func (m *mediaReplyChannel) ValidateTopic(int64, int64) error          { return nil }
func (m *mediaReplyChannel) StopPoll(int64, int64) (*c3types.PollResult, error) {
	return &c3types.PollResult{}, nil
}

func (m *mediaReplyChannel) sentSnapshot() []c3types.ReplyArgs {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]c3types.ReplyArgs, len(m.sent))
	copy(out, m.sent)
	return out
}
