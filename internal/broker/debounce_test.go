package broker

import (
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestMergeBatch_SingleElementUnchanged(t *testing.T) {
	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 1,
		Text: "hello",
	}
	out := mergeBatch([]*c3types.Inbound{in})
	if out != in {
		t.Errorf("single-element batch should return input pointer unchanged")
	}
}

func TestMergeBatch_ConcatenatesText(t *testing.T) {
	batch := []*c3types.Inbound{
		{Channel: "telegram", ChatID: -100, MessageID: 1, Text: "first", Timestamp: time.Unix(100, 0)},
		{Channel: "telegram", ChatID: -100, MessageID: 2, Text: "second"},
		{Channel: "telegram", ChatID: -100, MessageID: 3, Text: "third"},
	}
	out := mergeBatch(batch)
	if out.Text != "first\nsecond\nthird" {
		t.Errorf("Text=%q", out.Text)
	}
}

func TestMergeBatch_LatestMessageIDWins(t *testing.T) {
	batch := []*c3types.Inbound{
		{MessageID: 10},
		{MessageID: 20},
		{MessageID: 15},
	}
	out := mergeBatch(batch)
	if out.MessageID != 15 {
		t.Errorf("MessageID=%d, want last in slice (15) regardless of value", out.MessageID)
	}
}

func TestMergeBatch_EarliestTimestamp(t *testing.T) {
	batch := []*c3types.Inbound{
		{MessageID: 1, Timestamp: time.Unix(200, 0)},
		{MessageID: 2, Timestamp: time.Unix(100, 0)},
	}
	out := mergeBatch(batch)
	if !out.Timestamp.Equal(time.Unix(200, 0)) {
		t.Errorf("Timestamp = %v, want first batch entry's (Unix 200)", out.Timestamp)
	}
}

func TestMergeBatch_SkipsEmptyText(t *testing.T) {
	batch := []*c3types.Inbound{
		{MessageID: 1, Text: ""},
		{MessageID: 2, Text: "real"},
		{MessageID: 3, Text: ""},
	}
	out := mergeBatch(batch)
	if out.Text != "real" {
		t.Errorf("Text=%q, empty entries should be skipped", out.Text)
	}
}

func TestMergeBatch_FirstReplyToWins(t *testing.T) {
	r1 := &c3types.ReplyContext{MessageID: 100}
	r2 := &c3types.ReplyContext{MessageID: 200}
	batch := []*c3types.Inbound{
		{MessageID: 1, ReplyTo: nil},
		{MessageID: 2, ReplyTo: r1},
		{MessageID: 3, ReplyTo: r2},
	}
	out := mergeBatch(batch)
	if out.ReplyTo == nil || out.ReplyTo.MessageID != 100 {
		t.Errorf("ReplyTo = %+v, want first non-nil (r1, MessageID=100)", out.ReplyTo)
	}
}

func TestMergeBatch_ConcatenatesAttachments(t *testing.T) {
	batch := []*c3types.Inbound{
		{MessageID: 1, Attachments: []c3types.Attachment{{Kind: "photo", FileID: "p1"}}},
		{MessageID: 2, Attachments: []c3types.Attachment{{Kind: "voice", FileID: "v1"}}},
	}
	out := mergeBatch(batch)
	if len(out.Attachments) != 2 {
		t.Fatalf("Attachments=%d, want 2", len(out.Attachments))
	}
	if out.Attachments[0].FileID != "p1" || out.Attachments[1].FileID != "v1" {
		t.Errorf("Attachments=%+v", out.Attachments)
	}
}
