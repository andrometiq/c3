package telegram

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// TestBuildInlineKeyboard_DataAndURLButtons asserts the channel-neutral Button
// rows map onto the gotgbot InlineKeyboardMarkup: a {text, data} button becomes a
// CallbackData button and a {text, url} button becomes a Url button, preserving
// the row/column shape.
func TestBuildInlineKeyboard_DataAndURLButtons(t *testing.T) {
	markup, err := buildInlineKeyboard([][]c3types.Button{
		{{Text: "Approve", Data: "approve:1"}, {Text: "Deny", Data: "deny:1"}},
		{{Text: "Docs", URL: "https://example.com"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	kb := markup.InlineKeyboard
	if len(kb) != 2 || len(kb[0]) != 2 || len(kb[1]) != 1 {
		t.Fatalf("keyboard shape wrong; got %+v", kb)
	}
	// Data button → CallbackData set, Url empty.
	if kb[0][0].Text != "Approve" || kb[0][0].CallbackData != "approve:1" || kb[0][0].Url != "" {
		t.Errorf("data button mapped wrong; got %+v", kb[0][0])
	}
	if kb[0][1].CallbackData != "deny:1" {
		t.Errorf("second data button mapped wrong; got %+v", kb[0][1])
	}
	// URL button → Url set, CallbackData empty.
	if kb[1][0].Text != "Docs" || kb[1][0].Url != "https://example.com" || kb[1][0].CallbackData != "" {
		t.Errorf("url button mapped wrong; got %+v", kb[1][0])
	}
}

// TestBuildInlineKeyboard_CallbackDataTooLong asserts a callback_data over the
// 64-byte Telegram limit is rejected with a clear error (not an opaque 400).
func TestBuildInlineKeyboard_CallbackDataTooLong(t *testing.T) {
	long := strings.Repeat("x", maxCallbackDataBytes+1)
	_, err := buildInlineKeyboard([][]c3types.Button{
		{{Text: "Big", Data: long}},
	})
	if err == nil || !strings.Contains(err.Error(), "64-byte") {
		t.Fatalf("expected a 64-byte-limit error; got %v", err)
	}
}

// TestBuildInlineKeyboard_CallbackDataAtLimit asserts a callback_data EXACTLY at
// the 64-byte limit is accepted (boundary check).
func TestBuildInlineKeyboard_CallbackDataAtLimit(t *testing.T) {
	atLimit := strings.Repeat("x", maxCallbackDataBytes)
	markup, err := buildInlineKeyboard([][]c3types.Button{
		{{Text: "OK", Data: atLimit}},
	})
	if err != nil {
		t.Fatalf("a 64-byte callback data must be accepted; got %v", err)
	}
	if markup.InlineKeyboard[0][0].CallbackData != atLimit {
		t.Errorf("callback data not preserved at the limit")
	}
}

// TestBuildInlineKeyboard_RequiresExactlyOnePayload asserts a button with neither
// or both of data/url is rejected with a clear error.
func TestBuildInlineKeyboard_RequiresExactlyOnePayload(t *testing.T) {
	// Neither.
	if _, err := buildInlineKeyboard([][]c3types.Button{{{Text: "x"}}}); err == nil ||
		!strings.Contains(err.Error(), "EXACTLY ONE") {
		t.Errorf("expected EXACTLY-ONE error for a payload-less button; got %v", err)
	}
	// Both.
	if _, err := buildInlineKeyboard([][]c3types.Button{
		{{Text: "x", Data: "d", URL: "https://e.com"}},
	}); err == nil || !strings.Contains(err.Error(), "EXACTLY ONE") {
		t.Errorf("expected EXACTLY-ONE error for a data+url button; got %v", err)
	}
}

// TestBuildInlineKeyboard_RequiresText asserts a button with no label is rejected.
func TestBuildInlineKeyboard_RequiresText(t *testing.T) {
	_, err := buildInlineKeyboard([][]c3types.Button{{{Data: "d"}}})
	if err == nil || !strings.Contains(err.Error(), "no text") {
		t.Fatalf("expected a missing-text error; got %v", err)
	}
}

// TestBuildInlineKeyboard_EmptyRowRejected asserts an empty row (`[]`) is a clear
// error, not silently sent — Telegram 400s on an empty keyboard row.
func TestBuildInlineKeyboard_EmptyRowRejected(t *testing.T) {
	_, err := buildInlineKeyboard([][]c3types.Button{{}})
	if err == nil || !strings.Contains(err.Error(), "row 1 is empty") {
		t.Fatalf("expected an empty-row error; got %v", err)
	}
	// An empty row sandwiched between valid rows is also caught (with its index).
	_, err = buildInlineKeyboard([][]c3types.Button{
		{{Text: "ok", Data: "d"}},
		{},
	})
	if err == nil || !strings.Contains(err.Error(), "row 2 is empty") {
		t.Fatalf("expected an empty-row-2 error; got %v", err)
	}
}

// TestBuildInlineKeyboard_ShapeLimits asserts the conservative row / per-row caps
// turn an over-large keyboard into a clear error.
func TestBuildInlineKeyboard_ShapeLimits(t *testing.T) {
	// Too many buttons in a row.
	wide := make([]c3types.Button, maxButtonsPerRow+1)
	for i := range wide {
		wide[i] = c3types.Button{Text: "b", Data: "d"}
	}
	if _, err := buildInlineKeyboard([][]c3types.Button{wide}); err == nil ||
		!strings.Contains(err.Error(), "too many buttons") {
		t.Errorf("expected a too-many-buttons-per-row error; got %v", err)
	}
	// Too many rows.
	tall := make([][]c3types.Button, maxKeyboardRows+1)
	for i := range tall {
		tall[i] = []c3types.Button{{Text: "b", Data: "d"}}
	}
	if _, err := buildInlineKeyboard(tall); err == nil ||
		!strings.Contains(err.Error(), "too many keyboard rows") {
		t.Errorf("expected a too-many-rows error; got %v", err)
	}
}
