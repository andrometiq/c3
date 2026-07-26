package telegram

import (
	"fmt"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// askButtonsForTest mirrors the broker's askKeyboard output format
// (one button per option, callback_data "ask:<askID>:<idx>"). It is replicated
// here rather than imported because telegram is a lower layer than broker and
// must not depend on it; this test pins the format/cap contract that the
// broker's keyboard relies on.
func askButtonsForTest(askID string, options []string) [][]c3types.Button {
	rows := make([][]c3types.Button, 0, len(options))
	for i, opt := range options {
		rows = append(rows, []c3types.Button{{
			Text: opt,
			Data: fmt.Sprintf("ask:%s:%d", askID, i),
		}})
	}
	return rows
}

// TestAskKeyboard_RoundTripsThroughBuildInlineKeyboard asserts that a
// single-select ask keyboard at the MAX supported option count passes
// buildInlineKeyboard cleanly and every callback_data stays within Telegram's
// 64-byte ceiling — so the broker's `ask` send never trips the channel's limits.
func TestAskKeyboard_RoundTripsThroughBuildInlineKeyboard(t *testing.T) {
	const askID = "abcd2345" // 8-char base32, no colon
	n := maxKeyboardRows     // one option per row → the single-select ceiling
	options := make([]string, n)
	for i := range options {
		options[i] = "An option label that is reasonably long"
	}

	rows := askButtonsForTest(askID, options)
	markup, err := buildInlineKeyboard(rows)
	if err != nil {
		t.Fatalf("buildInlineKeyboard rejected a max-count ask keyboard: %v", err)
	}
	if len(markup.InlineKeyboard) != n {
		t.Fatalf("keyboard has %d rows, want %d", len(markup.InlineKeyboard), n)
	}
	for ri, row := range markup.InlineKeyboard {
		if len(row) != 1 {
			t.Fatalf("row %d has %d buttons, want 1", ri, len(row))
		}
		if d := row[0].CallbackData; len(d) > maxCallbackDataBytes {
			t.Fatalf("callback_data %q is %d bytes, over the %d-byte cap", d, len(d), maxCallbackDataBytes)
		}
	}

	// One option past the row ceiling must be a clear error, not a malformed send.
	tooMany := askButtonsForTest(askID, make([]string, maxKeyboardRows+1))
	if _, err := buildInlineKeyboard(tooMany); err == nil {
		t.Fatal("an ask keyboard over the row ceiling must be rejected with a clear error")
	}
}
