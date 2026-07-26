package telegram

import (
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// dispatchRaw drives the REAL inbound path the broker uses: the raw getUpdates
// result array → parseUpdates (byte-identical to Bot.GetUpdates) → dispatchUpdate
// for each update. This is the path that exposed the live callback drop — the
// prior callback tests hand-built a *gotgbot.Message pointer, but the wire decode
// produces a gotgbot.Message VALUE, which the old *Message type-assertion missed.
func (c *Channel) dispatchRaw(t *testing.T, raw []byte) {
	t.Helper()
	updates, probes, err := parseUpdates(raw)
	if err != nil {
		t.Fatalf("parseUpdates: %v", err)
	}
	for i := range updates {
		var richRaw []byte
		if i < len(probes) {
			richRaw = richRawFor(probes[i])
		}
		c.dispatchUpdate(&updates[i], richRaw)
	}
}

// TestDispatchCallback_FreshTopicMessage_RoutesViaWireParse is the live-bug
// reproduction: an inline-keyboard tap on a FRESH message in a forum TOPIC,
// decoded exactly as the broker decodes getUpdates, must surface as an
// InboundCallback routed to the right chat + topic — NOT be dropped.
//
// gotgbot rc.34's CallbackQuery.UnmarshalJSON resolves `message` via
// unmarshalMaybeInaccessibleMessage, which stores a gotgbot.Message VALUE (date
// != 0). The old dispatchCallback type-asserted to *gotgbot.Message (pointer),
// which never matches the value, so every real callback was dropped.
func TestDispatchCallback_FreshTopicMessage_RoutesViaWireParse(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannelWithPolls(h) // bot is nil → auto-ack skipped; routing is the unit under test

	// Realistic getUpdates result: a callback on a fresh forum-topic message.
	// Non-zero `date` ⇒ accessible; message_thread_id=98 + is_topic_message ⇒
	// the tap belongs to topic 98, not General.
	raw := []byte(`[{
		"update_id": 483747726,
		"callback_query": {
			"id": "cbq-1",
			"from": {"id": 7, "is_bot": false, "first_name": "Ada", "username": "ada"},
			"chat_instance": "-987654321",
			"data": "approve:42",
			"message": {
				"message_id": 200,
				"date": 1719500000,
				"chat": {"id": -1001234567890, "type": "supergroup", "title": "C3"},
				"message_thread_id": 98,
				"is_topic_message": true,
				"from": {"id": 111, "is_bot": true, "first_name": "bot"},
				"text": "Approve this?"
			}
		}
	}]`)

	c.dispatchRaw(t, raw)

	if h.emitCount() != 1 {
		t.Fatalf("a fresh topic-message callback must route (1 emit); got %d (live drop reproduced)", h.emitCount())
	}
	in := h.emitted[0]
	if in.Kind != c3types.InboundCallback {
		t.Fatalf("wrong kind: %q", in.Kind)
	}
	if in.ChatID != -1001234567890 {
		t.Errorf("ChatID: got %d want -1001234567890", in.ChatID)
	}
	if in.TopicID == nil || *in.TopicID != 98 {
		t.Errorf("TopicID must be the message thread 98 (not General/0); got %v", in.TopicID)
	}
	if in.MessageID != 200 {
		t.Errorf("MessageID: got %d want 200", in.MessageID)
	}
	cb := in.Event.Callback
	if cb == nil || cb.CallbackID != "cbq-1" || cb.Data != "approve:42" || cb.MessageID != 200 {
		t.Errorf("callback payload wrong: %+v", cb)
	}
	if in.Sender.UserID != 7 {
		t.Errorf("Sender.UserID: got %d want 7", in.Sender.UserID)
	}
}

// TestDispatchCallback_NonTopicMessage_RoutesViaWireParse confirms the fix is not
// topic-specific: a plain (non-topic) chat callback also routes, with a nil
// TopicID. This is the general inline-keyboard callback that was ALSO dropped.
func TestDispatchCallback_NonTopicMessage_RoutesViaWireParse(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannelWithPolls(h)

	raw := []byte(`[{
		"update_id": 483747727,
		"callback_query": {
			"id": "cbq-2",
			"from": {"id": 7, "is_bot": false, "first_name": "Ada"},
			"chat_instance": "-1",
			"data": "ans:yes",
			"message": {
				"message_id": 201,
				"date": 1719500001,
				"chat": {"id": 7, "type": "private"},
				"text": "Yes?"
			}
		}
	}]`)

	c.dispatchRaw(t, raw)

	if h.emitCount() != 1 {
		t.Fatalf("a non-topic callback must route (1 emit); got %d", h.emitCount())
	}
	in := h.emitted[0]
	if in.TopicID != nil {
		t.Errorf("non-topic callback must have nil TopicID; got %v", in.TopicID)
	}
	if in.ChatID != 7 || in.MessageID != 201 {
		t.Errorf("route wrong: chat=%d msg=%d", in.ChatID, in.MessageID)
	}
}

// TestDispatchCallback_PermTapGateDrop_AnsweredViaWireParse drives the full
// wire-decode path for the 2026-06-30 live bug: a permission-keyboard Allow tap
// ("perm:allow:<id>") from a sender the inbound gate refuses. The verdict must
// NOT be honored (no emit), but the tap must be answered with an explicit
// not-authorized notice — before the fix the channel spent the query's single
// answer on a bare auto-ack and the drop was invisible ("Allow did nothing").
func TestDispatchCallback_PermTapGateDrop_AnsweredViaWireParse(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundDrop}
	rc := &recordingBotClient{}
	c := makeChannelWithBot(h, rc)

	raw := []byte(`[{
		"update_id": 483747729,
		"callback_query": {
			"id": "cbq-perm",
			"from": {"id": 8, "is_bot": false, "first_name": "Sam"},
			"chat_instance": "-987654321",
			"data": "perm:allow:abcde",
			"message": {
				"message_id": 210,
				"date": 1719500002,
				"chat": {"id": -1001234567890, "type": "supergroup", "title": "C3"},
				"message_thread_id": 98,
				"is_topic_message": true,
				"from": {"id": 111, "is_bot": true, "first_name": "bot"},
				"text": "🔐 Permission: Bash"
			}
		}
	}]`)

	c.dispatchRaw(t, raw)

	if h.emitCount() != 0 {
		t.Fatalf("a gate-dropped perm tap must never be emitted (verdicts only from allowlisted senders); got %d", h.emitCount())
	}
	calls := rc.callsFor("answerCallbackQuery")
	if len(calls) != 1 {
		t.Fatalf("the refused tap must be answered exactly once (silent drop reproduced); got %d", len(calls))
	}
	if id, _ := calls[0].params["callback_query_id"].(string); id != "cbq-perm" {
		t.Errorf("answered wrong query id %q, want cbq-perm", id)
	}
	if text, _ := calls[0].params["text"].(string); text == "" {
		t.Error("the refused tap's answer must carry a not-authorized notice, not a bare ack")
	}
}

// TestDispatchCallback_InaccessibleMessage_DroppedViaWireParse covers the
// genuinely-inaccessible variant decoded from the wire (date==0 per the Bot API):
// gotgbot resolves it to a gotgbot.InaccessibleMessage value with no usable chat,
// so it is auto-acked (no-op here) and dropped — never surfaced with garbage
// coordinates.
func TestDispatchCallback_InaccessibleMessage_DroppedViaWireParse(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannelWithPolls(h)

	raw := []byte(`[{
		"update_id": 483747728,
		"callback_query": {
			"id": "cbq-3",
			"from": {"id": 7, "is_bot": false, "first_name": "Ada"},
			"chat_instance": "-1",
			"data": "approve:99",
			"message": {
				"message_id": 1,
				"date": 0,
				"chat": {"id": -1001234567890, "type": "supergroup"}
			}
		}
	}]`)

	c.dispatchRaw(t, raw)

	if h.emitCount() != 0 {
		t.Errorf("an inaccessible-message callback must not be routed; got %d emits", h.emitCount())
	}
}
