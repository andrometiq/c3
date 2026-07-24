package c3types

import (
	"strings"
	"testing"
)

// RenderQueuedInbound must put the message text bare on the first line, then a
// single compact metadata line — the trimmed inbound-readback form approved
// 2026-07-24 (task #55). Sender, message_id, reply context, and a kind+file_id
// attachment reference are kept (functionality); the verbose mime/size/name
// block is dropped (the token noise the maintainer flagged).
func TestRenderQueuedInbound_CompactFormat(t *testing.T) {
	got := RenderQueuedInbound(&Inbound{
		Channel:   "telegram",
		ChatID:    -100,
		MessageID: 42,
		Sender:    Sender{Username: "user"},
		Text:      "hello there",
		ReplyTo: &ReplyContext{
			MessageID: 100,
			User:      Sender{Username: "other"},
			Text:      "prior",
		},
		Attachments: []Attachment{{Kind: "photo", FileID: "PHOTO1", MIME: "image/jpeg", Size: 2048, Name: "pic.jpg"}},
	})

	// Message text stands bare on the first line.
	lines := strings.SplitN(got, "\n", 2)
	if lines[0] != "hello there" {
		t.Errorf("first line = %q, want bare message text %q", lines[0], "hello there")
	}
	if len(lines) != 2 {
		t.Fatalf("want a text line + one compact meta line; got %q", got)
	}
	// Kept, compact metadata.
	for _, want := range []string{"from=@user", "message_id=42", "reply_to=100", "reply_to_user=@other", "attachment=photo", "file_id=PHOTO1"} {
		if !strings.Contains(lines[1], want) {
			t.Errorf("meta line missing %q; got %q", want, lines[1])
		}
	}
	// Dropped verbose attachment fields.
	for _, gone := range []string{"image/jpeg", "size=2048", "pic.jpg", "text=\"hello"} {
		if strings.Contains(got, gone) {
			t.Errorf("render still carries trimmed field %q; got %q", gone, got)
		}
	}
}

// A media message with no transcribed text renders a kind-labelled placeholder as
// its text line rather than an empty first line.
func TestRenderQueuedInbound_EmptyTextMedia(t *testing.T) {
	got := RenderQueuedInbound(&Inbound{
		Channel:     "telegram",
		MessageID:   7,
		Attachments: []Attachment{{Kind: "voice", FileID: "V1"}},
	})
	if !strings.HasPrefix(got, "(voice message)\n") {
		t.Errorf("want a (voice message) text line first; got %q", got)
	}
	if !strings.Contains(got, "attachment=voice file_id=V1") {
		t.Errorf("want compact attachment ref; got %q", got)
	}
}

func TestAttachmentCompact(t *testing.T) {
	got := AttachmentCompact(Attachment{Kind: "document", FileID: "DOC9", MIME: "application/pdf", Size: 999, Name: "report.pdf"})
	if got != "attachment=document file_id=DOC9" {
		t.Errorf("AttachmentCompact = %q, want %q", got, "attachment=document file_id=DOC9")
	}
}

func TestInboundsHaveAttachment(t *testing.T) {
	if InboundsHaveAttachment([]Inbound{{Text: "a"}, {Text: "b"}}) {
		t.Error("no attachments present, want false")
	}
	if !InboundsHaveAttachment([]Inbound{{Text: "a"}, {Attachments: []Attachment{{Kind: "photo", FileID: "P"}}}}) {
		t.Error("an attachment is present, want true")
	}
}
