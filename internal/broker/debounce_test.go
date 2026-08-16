package broker

import (
	"reflect"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestMergeBatch_SingleElementUnchanged(t *testing.T) {
	origin := &c3types.ForwardOrigin{Kind: "user", Name: "Forwarded User"}
	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 1,
		Text: "hello", MediaGroupID: "album-1", ForwardOrigin: origin,
		Attachments: []c3types.Attachment{{Kind: "photo", FileID: "p1"}},
	}
	want := *in
	out := mergeBatch([]*c3types.Inbound{in})
	if out != in {
		t.Errorf("single-element batch should return input pointer unchanged")
	}
	if !reflect.DeepEqual(*out, want) {
		t.Fatalf("single-element batch changed: got %+v want %+v", *out, want)
	}
	if len(out.Merged) != 0 || out.Attachments[0].SourceMessageID != 0 {
		t.Fatalf("single-element batch gained delivery structure: %+v", out)
	}
}

func TestMergeBatch_PreservesPerSourceStructure(t *testing.T) {
	firstOrigin := &c3types.ForwardOrigin{Kind: "user", Name: "Alice Forward"}
	secondOrigin := &c3types.ForwardOrigin{Kind: "channel", Name: "News"}
	batch := []*c3types.Inbound{
		{
			MessageID: 11, Sender: c3types.Sender{UserID: 1, Username: "alice"}, Text: "caption",
			ForwardOrigin: firstOrigin,
			Attachments:   []c3types.Attachment{{Kind: "photo", FileID: "photo-11"}},
		},
		{
			MessageID: 12, Sender: c3types.Sender{UserID: 2, Username: "bob"}, Text: "",
			ForwardOrigin: secondOrigin,
			Attachments:   []c3types.Attachment{{Kind: "document", FileID: "document-12"}},
		},
	}

	out := mergeBatch(batch)
	wantMerged := []c3types.MergedSource{
		{MessageID: 11, Sender: batch[0].Sender, Text: "caption", ForwardOrigin: firstOrigin},
		{MessageID: 12, Sender: batch[1].Sender, ForwardOrigin: secondOrigin},
	}
	if !reflect.DeepEqual(out.Merged, wantMerged) {
		t.Fatalf("Merged = %+v, want %+v", out.Merged, wantMerged)
	}
	if out.ForwardOrigin != nil {
		t.Fatalf("mixed origins must clear the merged top-level origin: %+v", out.ForwardOrigin)
	}
	if got := []int64{out.Attachments[0].SourceMessageID, out.Attachments[1].SourceMessageID}; !reflect.DeepEqual(got, []int64{11, 12}) {
		t.Fatalf("attachment source ids = %v, want [11 12]", got)
	}
	for i, source := range batch {
		for _, attachment := range source.Attachments {
			if attachment.SourceMessageID != 0 {
				t.Fatalf("source batch[%d] attachment mutated: %+v", i, attachment)
			}
		}
	}
}

func TestMergeBatch_ReportAmbiguityCaseIsRecoverable(t *testing.T) {
	batch := []*c3types.Inbound{
		{MessageID: 101, Text: "caption one", Attachments: []c3types.Attachment{{Kind: "photo", FileID: "photo-1"}}},
		{MessageID: 102, Text: "caption two", Attachments: []c3types.Attachment{{Kind: "photo", FileID: "photo-2"}}},
		{MessageID: 103, Text: "standalone one"},
		{MessageID: 104, Text: "caption three", Attachments: []c3types.Attachment{{Kind: "photo", FileID: "photo-3"}}},
		{MessageID: 105, Text: "standalone two"},
		{MessageID: 106, Attachments: []c3types.Attachment{{Kind: "document", FileID: "document-1"}}},
		{MessageID: 107, Text: "caption four", Attachments: []c3types.Attachment{{Kind: "photo", FileID: "photo-4"}}},
	}

	out := mergeBatch(batch)
	if len(out.Merged) != 7 {
		t.Fatalf("Merged source count = %d, want 7", len(out.Merged))
	}
	for i, source := range out.Merged {
		if source.MessageID != batch[i].MessageID || source.Text != batch[i].Text {
			t.Errorf("Merged[%d] = %+v, want id=%d text=%q", i, source, batch[i].MessageID, batch[i].Text)
		}
	}
	if len(out.Attachments) != 5 {
		t.Fatalf("attachment count = %d, want 5", len(out.Attachments))
	}
	wantPairs := map[string]int64{
		"photo-1": 101, "photo-2": 102, "photo-3": 104, "document-1": 106, "photo-4": 107,
	}
	for _, attachment := range out.Attachments {
		if want := wantPairs[attachment.FileID]; attachment.SourceMessageID != want {
			t.Errorf("attachment %s source = %d, want %d", attachment.FileID, attachment.SourceMessageID, want)
		}
	}
	if out.Merged[5].Text != "" || len(batch[2].Attachments) != 0 || len(batch[4].Attachments) != 0 {
		t.Fatal("captionless document or standalone text structure was lost")
	}
}

func TestMergeBatch_CarriesOnlyUniformForwardOrigin(t *testing.T) {
	origin := &c3types.ForwardOrigin{Kind: "hidden_user", Name: "Source"}
	out := mergeBatch([]*c3types.Inbound{
		{MessageID: 1, ForwardOrigin: origin},
		{MessageID: 2, ForwardOrigin: &c3types.ForwardOrigin{Kind: "hidden_user", Name: "Source"}},
	})
	if out.ForwardOrigin == nil || *out.ForwardOrigin != *origin {
		t.Fatalf("uniform origin = %+v, want %+v", out.ForwardOrigin, origin)
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
