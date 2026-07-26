package telegram

import (
	"testing"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/channel"
)

// genericTapWithKeyboard builds an accessible-message callback carrying `data`,
// whose source message shows a two-option inline keyboard — the shape of a
// hand-rolled reply(buttons=…) choice prompt.
func genericTapWithKeyboard(id, data string) *gotgbot.CallbackQuery {
	return &gotgbot.CallbackQuery{
		Id:   id,
		From: gotgbot.User{Id: 7},
		Data: data,
		Message: &gotgbot.Message{
			MessageId: 300,
			Chat:      gotgbot.Chat{Id: -100},
			ReplyMarkup: &gotgbot.InlineKeyboardMarkup{
				InlineKeyboard: [][]gotgbot.InlineKeyboardButton{
					{{Text: "A · Disable", CallbackData: "choice:a"}},
					{{Text: "B · Live", CallbackData: "choice:b"}},
				},
			},
		},
	}
}

// collapsedButtons flattens the InlineKeyboardMarkup passed to an
// editMessageReplyMarkup call (gotgbot stores the raw struct in the param map).
func collapsedButtons(t *testing.T, call recordedCall) []gotgbot.InlineKeyboardButton {
	t.Helper()
	mk, ok := call.params["reply_markup"].(gotgbot.InlineKeyboardMarkup)
	if !ok {
		t.Fatalf("reply_markup param must be an InlineKeyboardMarkup; got %T", call.params["reply_markup"])
	}
	var btns []gotgbot.InlineKeyboardButton
	for _, row := range mk.InlineKeyboard {
		btns = append(btns, row...)
	}
	return btns
}

// TestDispatchCallback_GenericTap_CollapsesKeyboard pins the 2026-07-12 live-bug
// fix: a hand-rolled reply-button tap must collapse the keyboard to a single inert
// "✓ <label>" indicator (so it can't be pressed again and the message records the
// choice) AND still auto-ack + surface the event. Before the fix the keyboard
// stayed live — pressable repeatedly, never showing the selection.
func TestDispatchCallback_GenericTap_CollapsesKeyboard(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	rc := &recordingBotClient{}
	c := makeChannelWithBot(h, rc)

	c.dispatchCallback(9, genericTapWithKeyboard("cb-choice", "choice:a"))

	if h.emitCount() != 1 {
		t.Fatalf("a generic callback must still emit; got %d", h.emitCount())
	}
	if got := len(rc.callsFor("answerCallbackQuery")); got != 1 {
		t.Fatalf("a generic callback must be auto-acked once; got %d", got)
	}
	edits := rc.callsFor("editMessageReplyMarkup")
	if len(edits) != 1 {
		t.Fatalf("a generic tap must collapse the keyboard exactly once; got %d edits", len(edits))
	}
	btns := collapsedButtons(t, edits[0])
	if len(btns) != 1 {
		t.Fatalf("collapsed keyboard must have exactly one button; got %d", len(btns))
	}
	if btns[0].Text != "✓ A · Disable" {
		t.Errorf("collapsed button label = %q, want the tapped option %q", btns[0].Text, "✓ A · Disable")
	}
	if btns[0].CallbackData != callbackChosenData {
		t.Errorf("collapsed button data = %q, want the inert indicator %q", btns[0].CallbackData, callbackChosenData)
	}
}

// TestDispatchCallback_AskTap_DoesNotCollapse: an "ask:" tap must NOT be collapsed
// by the generic path — the ask flow owns that keyboard (single-select clears it,
// multi-select toggles it live). It still emits (the broker resolves it).
func TestDispatchCallback_AskTap_DoesNotCollapse(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	rc := &recordingBotClient{}
	c := makeChannelWithBot(h, rc)

	c.dispatchCallback(9, genericTapWithKeyboard("cb-ask", "ask:abcde:0"))

	if h.emitCount() != 1 {
		t.Fatalf("an ask tap must still emit (broker resolves it); got %d", h.emitCount())
	}
	if got := len(rc.callsFor("editMessageReplyMarkup")); got != 0 {
		t.Fatalf("an ask tap must NOT be collapsed by the channel; got %d edits", got)
	}
}

// TestDispatchCallback_ChosenIndicator_Inert: a tap on the collapsed indicator
// button is a no-op — bare-acked to clear the spinner, never re-collapsed, never
// surfaced as an event. This is what ends the "keep pressing it" behavior.
func TestDispatchCallback_ChosenIndicator_Inert(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	rc := &recordingBotClient{}
	c := makeChannelWithBot(h, rc)

	c.dispatchCallback(9, genericTapWithKeyboard("cb-inert", callbackChosenData))

	if h.emitCount() != 0 {
		t.Fatalf("a tap on the inert indicator must NOT emit; got %d", h.emitCount())
	}
	if got := len(rc.callsFor("editMessageReplyMarkup")); got != 0 {
		t.Fatalf("the inert indicator must not re-collapse; got %d edits", got)
	}
	if got := len(rc.callsFor("answerCallbackQuery")); got != 1 {
		t.Fatalf("the inert indicator must still be acked once (clear the spinner); got %d", got)
	}
}
