package broker

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// hardeningChannel records EVERY outbound the dispatch layer asks it to make —
// sends, reactions, edits, typing pulses and poll stops — so a security test can
// assert not only that a call was refused but that NOTHING reached the chat. It
// advertises RichText so a plain markdown reply produces no gate alteration (the
// case that used to leave no log line at all).
type hardeningChannel struct {
	mu     sync.Mutex
	sent   []c3types.ReplyArgs
	reacts []c3types.ReactArgs
	edits  []c3types.EditArgs
	typing []int64
	stops  []int64
}

func (h *hardeningChannel) Name() string                              { return "telegram" }
func (h *hardeningChannel) Start(context.Context, channel.Host) error { return nil }
func (h *hardeningChannel) Stop() error                               { return nil }
func (h *hardeningChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{Channel: "telegram", RichText: true, InlineKeyboards: true, Polls: true}
}

func (h *hardeningChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sent = append(h.sent, args)
	return int64(len(h.sent)), nil
}

func (h *hardeningChannel) SendTyping(chatID int64, _ *int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.typing = append(h.typing, chatID)
	return nil
}

func (h *hardeningChannel) EditMessage(args c3types.EditArgs) (*c3types.EditResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.edits = append(h.edits, args)
	return &c3types.EditResult{MessageID: args.MessageID}, nil
}

func (h *hardeningChannel) React(args c3types.ReactArgs) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reacts = append(h.reacts, args)
	return nil
}

func (h *hardeningChannel) DownloadAttachment(string) (string, error) { return "", nil }
func (h *hardeningChannel) CreateTopic(int64, string) (int64, error)  { return 0, nil }
func (h *hardeningChannel) ValidateTopic(int64, int64) error          { return nil }

func (h *hardeningChannel) StopPoll(chatID, _ int64) (*c3types.PollResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stops = append(h.stops, chatID)
	return &c3types.PollResult{}, nil
}

func (h *hardeningChannel) sentSnapshot() []c3types.ReplyArgs {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]c3types.ReplyArgs, len(h.sent))
	copy(out, h.sent)
	return out
}

// callCount is every outbound the channel was asked to perform, of any kind.
func (h *hardeningChannel) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.sent) + len(h.reacts) + len(h.edits) + len(h.typing) + len(h.stops)
}

func hardeningButtonArgs(data string) map[string]any {
	return map[string]any{
		"text": "decide:",
		"buttons": []any{
			[]any{map[string]any{"text": "Tap", "data": data}},
		},
	}
}

// TestDispatchReply_ReservedCallbackPrefixRefused pins the outbound half of the
// callback-data namespace. The inbound side routes a tap by PREFIX — "perm:" to
// resolvePerm, "ask:" to resolveAsk, "c3:" swallowed by the channel — as if such
// a payload could only have been minted by C3 itself. buttonsFromArgs is the one
// path from an agent to Outbound.Buttons, so the reservation has to be enforced
// there or the assumption is false.
func TestDispatchReply_ReservedCallbackPrefixRefused(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 281}
	for _, data := range []string{"perm:allow:abcde", "perm:x", "ask:AAAAAAAA:0", "c3:_chosen", "c3:anything"} {
		ch := &hardeningChannel{}
		_, err := dispatchReply(ch, key, hardeningButtonArgs(data))
		if err == nil {
			t.Errorf("agent-supplied callback data %q was accepted with its C3-reserved prefix intact: the agent can mint a payload the inbound side hands to C3's own resolver (a permission Allow/Deny verdict or an ask answer), and a tap on it never reaches the agent at all", data)
			continue
		}
		if !strings.Contains(err.Error(), "reserves") {
			t.Errorf("callback data %q was refused, but the error does not tell the agent author that the prefix is RESERVED (so they cannot know what to change): %v", data, err)
		}
		if n := ch.callCount(); n != 0 {
			t.Errorf("callback data %q: a refused button must send NOTHING, but %d outbound call(s) still reached the channel", data, n)
		}
	}
}

// TestDispatchReply_PrefixAdjacentCallbackDataStillAllowed is the narrowness
// control: only the three reserved NAMESPACES are refused. A prefix-adjacent
// value must still go through verbatim, because the tap echoes Data back to the
// agent and its own `data == …` comparison depends on it.
func TestDispatchReply_PrefixAdjacentCallbackDataStillAllowed(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 281}
	for _, data := range []string{"approve:1", "askew", "asks:1", "permit:1", "c3", "c3_chosen", "myperm:1"} {
		ch := &hardeningChannel{}
		if _, err := dispatchReply(ch, key, hardeningButtonArgs(data)); err != nil {
			t.Errorf("callback data %q is not in a C3-reserved namespace (perm: / ask: / c3:) but was refused — the reserved-prefix check matches too broadly and is breaking legitimate agent buttons: %v", data, err)
			continue
		}
		sent := ch.sentSnapshot()
		if len(sent) != 1 || len(sent[0].Buttons) != 1 || len(sent[0].Buttons[0]) != 1 {
			t.Errorf("callback data %q: expected exactly one button on one part; got %+v", data, sent)
			continue
		}
		if got := sent[0].Buttons[0][0].Data; got != data {
			t.Errorf("callback data was rewritten (%q → %q) instead of passed through: the tap echoes Data back verbatim, so the agent's own comparison of the value would silently fail", data, got)
		}
	}
}

// TestDispatchTool_DestinationOverrideRefused pins the destination guard. A
// caller-supplied chat_id/topic_id is in no tool schema and is set by no in-tree
// caller, but every adapter forwards the model's arg map verbatim — so honouring
// one lets an agent attached to topic A post into, react in, edit or stop a poll
// in a chat/topic it never claimed. Refusal is on PRESENCE: the `chat_id: nil`
// case below is silently ACCEPTED by a guard that compares values, because
// argInt64 falls back to the route's own id for an unparseable value.
func TestDispatchTool_DestinationOverrideRefused(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 281}
	cases := []struct {
		name string
		tool string
		args map[string]any
	}{
		{"reply into another chat", "reply", map[string]any{"text": "x", "chat_id": float64(-999)}},
		{"reply into another topic", "reply", map[string]any{"text": "x", "topic_id": float64(99)}},
		{"reply escaping into the group's General chat", "reply", map[string]any{"text": "x", "topic_id": float64(0)}},
		{"reply with a chat_id that agrees with the claim", "reply", map[string]any{"text": "x", "chat_id": float64(-100)}},
		{"reply with an unparseable chat_id", "reply", map[string]any{"text": "x", "chat_id": nil}},
		{"react in another chat", "react", map[string]any{"message_id": float64(7), "emoji": "👍", "chat_id": float64(-999)}},
		{"edit in another chat", "edit_message", map[string]any{"message_id": float64(7), "text": "x", "chat_id": float64(-999)}},
		{"stop_poll in another chat", "stop_poll", map[string]any{"message_id": float64(7), "chat_id": float64(-999)}},
		{"typing in another topic", "send_typing", map[string]any{"topic_id": float64(99)}},
	}
	for _, tc := range cases {
		ch := &hardeningChannel{}
		if _, err := dispatchTool(ch, key, tc.tool, tc.args); err == nil {
			t.Errorf("%s: %q accepted a caller-supplied destination, so an agent can address a chat/topic this session never claimed while the claim registry and every log line still say otherwise", tc.name, tc.tool)
		}
		if n := ch.callCount(); n != 0 {
			t.Errorf("%s: a refused destination must reach the channel ZERO times; it was called %d time(s)", tc.name, n)
		}
	}
}

// TestDispatchReply_DestinationIsAlwaysTheClaimedRoute is the defence in depth
// behind that guard: dispatchReply is called DIRECTLY here, past
// rejectDestinationOverride, so this fails if any dispatcher goes back to
// reading its destination out of the agent's args.
func TestDispatchReply_DestinationIsAlwaysTheClaimedRoute(t *testing.T) {
	offRoute := map[string]any{"text": "x", "chat_id": float64(-999), "topic_id": float64(99)}

	t.Run("forum topic", func(t *testing.T) {
		ch := &hardeningChannel{}
		key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 281}
		if _, err := dispatchReply(ch, key, offRoute); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		sent := ch.sentSnapshot()
		if len(sent) != 1 {
			t.Fatalf("expected exactly 1 part; got %d", len(sent))
		}
		if sent[0].ChatID != key.ChatID || sent[0].TopicID == nil || *sent[0].TopicID != key.TopicID {
			t.Fatalf("the reply was addressed from the agent's args instead of from the claimed route: it went to chat %d topic %s, but this session claimed chat %d topic %d",
				sent[0].ChatID, TopicPtrStr(sent[0].TopicID), key.ChatID, key.TopicID)
		}
	})

	t.Run("no topic", func(t *testing.T) {
		ch := &hardeningChannel{}
		key := RouteKey{Channel: "telegram", ChatID: -100}
		if _, err := dispatchReply(ch, key, offRoute); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		sent := ch.sentSnapshot()
		if len(sent) != 1 {
			t.Fatalf("expected exactly 1 part; got %d", len(sent))
		}
		if sent[0].ChatID != key.ChatID || sent[0].TopicID != nil {
			t.Fatalf("a route with no topic must stay topic-less: the args steered the reply to chat %d topic %s instead of chat %d with no topic",
				sent[0].ChatID, TopicPtrStr(sent[0].TopicID), key.ChatID)
		}
	})
}

// TestDispatchTool_LogsOutboundDestination pins the outbound audit trail. A
// plain text reply produces no gate alteration and no media-send line, so
// without this line the broker log cannot show, after an incident, where an
// agent's send actually went — while the inbound side is fully audited.
func TestDispatchTool_LogsOutboundDestination(t *testing.T) {
	var buf bytes.Buffer
	oldOut, oldFlags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
	}()

	ch := &hardeningChannel{}
	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 281}

	if _, err := dispatchTool(ch, key, "reply", map[string]any{"text": "plain"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	const want = "outbound-dispatch chan=telegram chat=-100 topic=281 tool=reply ok=true"
	if got := buf.String(); !strings.Contains(got, want) {
		t.Fatalf("a plain-text outbound left NO destination in the log, so an exfiltration through `reply` is untraceable: wanted a line containing %q; log was:\n%s", want, got)
	}

	buf.Reset()
	if _, err := dispatchTool(ch, key, "reply", map[string]any{"text": "x", "chat_id": float64(-999)}); err == nil {
		t.Fatal("a caller-supplied chat_id must be refused")
	}
	got := buf.String()
	if !strings.Contains(got, "tool=reply ok=false") {
		t.Errorf("a FAILED outbound is not audited, so the log records only the calls that succeeded; log was:\n%s", got)
	}
	if !strings.Contains(got, "outbound-destination-refused chan=telegram chat=-100 topic=281 field=chat_id") {
		t.Errorf("a refused destination leaves no log line naming the offending field, so an operator cannot tell an agent tried to send off-route; log was:\n%s", got)
	}
}
