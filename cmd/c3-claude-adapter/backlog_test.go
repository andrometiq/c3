package main

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

func TestRenderBacklogSummary(t *testing.T) {
	got := renderBacklogSummary(2, []ipc.QueuedItem{
		{MessageID: 5, Sender: "@k", Kind: "text", Preview: "hello"},
		{MessageID: 6, Sender: "@k", Kind: "voice", Preview: ""},
	}, "myproject")
	// Assert per-ITEM content, not just any '2': both items' message_ids, the
	// text preview, and the voice item's "(voice)" fallback must render, plus the
	// fetch_queue hint. This catches a broken item-rendering loop. The topic name
	// must appear (§5 named nudge) so a stale/wrong advertisement is distinguishable.
	for _, want := range []string{"fetch_queue", "[5]", "hello", "[6]", "(voice)", "myproject"} {
		if !strings.Contains(got, want) {
			t.Errorf("backlog summary missing %q; got %q", want, got)
		}
	}
}

// D-A2: the queued (fetch_queue) renderer must expose the full reply context —
// reply_to_user and reply_to_text — not just reply_to=<id>. Kept byte-identical to
// the Codex adapter's renderQueuedInbound.
func TestRenderQueuedInbound_FullReplyContext(t *testing.T) {
	got := renderQueuedInbound(&c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 9,
		ReplyTo: &c3types.ReplyContext{
			MessageID: 100,
			User:      c3types.Sender{UserID: 42, Username: "alice"},
			Text:      ".",
		},
	})
	for _, want := range []string{"reply_to=100", "reply_to_user=@alice", `reply_to_text="."`} {
		if !strings.Contains(got, want) {
			t.Errorf("renderQueuedInbound missing %q; got %q", want, got)
		}
	}
}

// count > len(items) must render the "…and N more" truncation line.
func TestRenderBacklogSummary_AndMore(t *testing.T) {
	got := renderBacklogSummary(5, []ipc.QueuedItem{
		{MessageID: 1, Sender: "@k", Kind: "text", Preview: "a"},
		{MessageID: 2, Sender: "@k", Kind: "text", Preview: "b"},
		{MessageID: 3, Sender: "@k", Kind: "text", Preview: "c"},
	}, "myproject")
	if !strings.Contains(got, "and 2 more") {
		t.Errorf("backlog summary = %q, want '…and 2 more' truncation line", got)
	}
}

func TestRenderBacklogSummary_Empty(t *testing.T) {
	if got := renderBacklogSummary(0, nil, "myproject"); got != "" {
		t.Errorf("empty backlog summary = %q, want empty string", got)
	}
}

func TestPendingNudge(t *testing.T) {
	// §5: the nudge names the topic so a stale/wrong advertisement is distinguishable.
	got := pendingNudge(3, "myproject")
	if !strings.Contains(got, "3 pending") || !strings.Contains(got, "fetch_queue") || !strings.Contains(got, "myproject") {
		t.Errorf("pendingNudge(3, \"myproject\") = %q, want count + fetch_queue + topic name", got)
	}
	// route=="" falls back to the name-less form.
	if got := pendingNudge(3, ""); !strings.Contains(got, "3 pending") || strings.Contains(got, "topic") {
		t.Errorf("pendingNudge(3, \"\") = %q, want name-less fallback", got)
	}
	if got := pendingNudge(0, "myproject"); got != "" {
		t.Errorf("pendingNudge(0) = %q, want empty", got)
	}
}

func TestDecoratePushContent_CarriesNudgeOnBacklog(t *testing.T) {
	got := decoratePushContent("hello", 2, "myproject")
	if !strings.Contains(got, "hello") || !strings.Contains(got, "2 pending") || !strings.Contains(got, "fetch_queue") || !strings.Contains(got, "myproject") {
		t.Errorf("push content = %q, want body + '2 pending' + fetch_queue nudge + topic name", got)
	}
	if got := decoratePushContent("hello", 0, "myproject"); got != "hello" {
		t.Errorf("no-backlog push content = %q, want unchanged 'hello'", got)
	}
}

func TestRenderFetchedMessages_ExposesAttachmentFileID(t *testing.T) {
	got := renderFetchedMessages([]c3types.Inbound{{
		Channel: "telegram", ChatID: -100, MessageID: 7,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "VOICE123", MIME: "audio/ogg", Size: 2048, Name: "note.ogg"}},
	}}, 0, "myproject")
	// #55: kind + file_id are kept (download_attachment/retranscribe still work),
	// and the one-time fetch hint is prepended once.
	for _, want := range []string{"VOICE123", "attachment=voice", "message_id=7", "download_attachment"} {
		if !strings.Contains(got, want) {
			t.Errorf("fetch_queue render missing %q; got %q", want, got)
		}
	}
	// The verbose per-message mime/size/name block is dropped.
	for _, gone := range []string{"audio/ogg", "note.ogg", "size=2048"} {
		if strings.Contains(got, gone) {
			t.Errorf("fetch_queue render still carries trimmed field %q; got %q", gone, got)
		}
	}
	if n := strings.Count(got, "call download_attachment"); n != 1 {
		t.Errorf("fetch hint should appear once; appeared %d times in %q", n, got)
	}
}

// renderFetchedMessages with no messages must report the empty-queue line.
func TestRenderFetchedMessages_Empty(t *testing.T) {
	if got := renderFetchedMessages(nil, 0, "myproject"); !strings.Contains(got, "empty") {
		t.Errorf("empty fetch render = %q, want an 'empty' line", got)
	}
}

// A non-zero Remaining must append the pending nudge (naming the topic, §5) so the
// agent keeps draining.
func TestRenderFetchedMessages_RemainingNudge(t *testing.T) {
	got := renderFetchedMessages([]c3types.Inbound{{
		Channel: "telegram", MessageID: 1, Text: "hi",
	}}, 4, "myproject")
	if !strings.Contains(got, "4 pending") || !strings.Contains(got, "fetch_queue") || !strings.Contains(got, "myproject") {
		t.Errorf("fetch render with remaining=4 missing nudge/topic; got %q", got)
	}
}

// Item C: a numeric-STRING limit ("5") must be parsed and honored, not silently
// dropped to the default 3 (the old switch matched neither "all" nor float64).
// Covers "all", JSON-number, string-number, clamps, and unparseable/absent.
func TestParseFetchLimit(t *testing.T) {
	cases := []struct {
		name      string
		in        any
		wantLimit int
		wantAll   bool
	}{
		{"string-number 5", "5", 5, false},
		{"string-number padded", " 7 ", 7, false},
		{"all lowercase", "all", 0, true},
		{"all mixed case", "ALL", 0, true},
		{"json number", float64(4), 4, false},
		{"string over cap clamps to 50", "999", 50, false},
		{"string under 1 clamps to 1", "0", 1, false},
		{"json number over cap clamps", float64(123), 50, false},
		{"unparseable falls back to default 3", "abc", 3, false},
		{"absent falls back to default 3", nil, 3, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotLimit, gotAll := parseFetchLimit(tc.in)
			if gotLimit != tc.wantLimit || gotAll != tc.wantAll {
				t.Fatalf("parseFetchLimit(%#v) = (%d, %v), want (%d, %v)", tc.in, gotLimit, gotAll, tc.wantLimit, tc.wantAll)
			}
		})
	}
}
