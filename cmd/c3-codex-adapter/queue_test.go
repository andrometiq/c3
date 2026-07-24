package main

import (
	"strings"
	"testing"

	"github.com/karthikeyan5/c3/internal/c3types"
	"github.com/karthikeyan5/c3/internal/ipc"
)

func TestRenderFetchedMessages_Codex(t *testing.T) {
	got := renderFetchedMessages([]c3types.Inbound{
		{Channel: "telegram", ChatID: -100, MessageID: 1, Text: "hi", Sender: c3types.Sender{Username: "k"}},
	}, 2, "myproject")
	// §5: the remaining nudge names the topic so a stale/wrong advertisement is
	// distinguishable.
	if !strings.Contains(got, "hi") || !strings.Contains(got, "fetch_queue") || !strings.Contains(got, "myproject") {
		t.Errorf("rendered = %q, want body + remaining nudge + topic name", got)
	}
}

// renderFetchedMessages must still expose the attachment's kind + file_id so the
// agent can recover backlog voice via download_attachment/retranscribe (spec
// Component 4), and it must prepend the one-time fetch hint exactly once (#55:
// the verbose per-message mime/size/name block is dropped, but functionality is
// preserved — the file_id is kept and the "how to open it" note is shown once).
func TestRenderFetchedMessages_Codex_ExposesAttachmentFileID(t *testing.T) {
	got := renderFetchedMessages([]c3types.Inbound{{
		Channel: "telegram", ChatID: -100, MessageID: 7,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "VOICE123", MIME: "audio/ogg", Size: 2048, Name: "note.ogg"}},
	}}, 0, "myproject")
	for _, want := range []string{"VOICE123", "attachment=voice", "message_id=7", "download_attachment"} {
		if !strings.Contains(got, want) {
			t.Errorf("fetch_queue render missing %q; got %q", want, got)
		}
	}
	// #55: the trimmed per-message block no longer carries mime/size/name.
	for _, gone := range []string{"audio/ogg", "note.ogg", "size=2048"} {
		if strings.Contains(got, gone) {
			t.Errorf("fetch_queue render still carries trimmed field %q; got %q", gone, got)
		}
	}
	// The one-time fetch hint appears exactly once, not per message.
	if n := strings.Count(got, "call download_attachment"); n != 1 {
		t.Errorf("fetch hint should appear once; appeared %d times in %q", n, got)
	}
}

// D-A2: the queued (fetch_queue) renderer must expose the full reply context —
// reply_to_user and reply_to_text — not just reply_to=<id>. Mirrors the Claude
// adapter's renderQueuedInbound (the two are byte-identical).
func TestRenderQueuedInbound_FullReplyContext_Codex(t *testing.T) {
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

func TestPendingNudge_Codex(t *testing.T) {
	// §5: the nudge names the topic so a stale/wrong advertisement is distinguishable.
	got := pendingNudge(2, "myproject")
	if !strings.Contains(got, "2 pending") || !strings.Contains(got, "myproject") {
		t.Errorf("pendingNudge(2, \"myproject\") = %q, want count + topic name", got)
	}
	if got := pendingNudge(2, ""); !strings.Contains(got, "2 pending") || strings.Contains(got, "topic") {
		t.Errorf("pendingNudge(2, \"\") = %q, want name-less fallback", got)
	}
	if got := pendingNudge(0, "myproject"); got != "" {
		t.Errorf("pendingNudge(0) = %q, want empty", got)
	}
}

// renderFetchedMessages on an empty pull tells the agent the queue is empty
// rather than returning a misleading blank result.
func TestRenderFetchedMessages_Codex_Empty(t *testing.T) {
	if got := renderFetchedMessages(nil, 0, "myproject"); !strings.Contains(got, "empty") {
		t.Errorf("empty fetch render = %q, want an 'empty' notice", got)
	}
}

// renderBacklogSummary must render the count line + fetch_queue hint even when
// the broker degraded to a count-only summary (QueuedCount>0 with an EMPTY
// QueuedSummary slice — Peek failed broker-side). Without this the agent would
// see nothing and never drain its backlog.
func TestRenderBacklogSummary_Codex_CountOnly(t *testing.T) {
	got := renderBacklogSummary(3, nil, "myproject")
	// §5: even the degraded count-only summary names the topic.
	if !strings.Contains(got, "3") || !strings.Contains(got, "fetch_queue") || !strings.Contains(got, "myproject") {
		t.Errorf("count-only backlog summary = %q, want the count + fetch_queue hint + topic name", got)
	}
}

func TestRenderBacklogSummary_Codex_Empty(t *testing.T) {
	if got := renderBacklogSummary(0, nil, "myproject"); got != "" {
		t.Errorf("empty backlog summary = %q, want empty string", got)
	}
}

func TestRenderBacklogSummary_Codex_PerItem(t *testing.T) {
	got := renderBacklogSummary(2, []ipc.QueuedItem{
		{MessageID: 5, Sender: "@k", Kind: "text", Preview: "hello"},
		{MessageID: 6, Sender: "@k", Kind: "voice", Preview: ""},
	}, "myproject")
	for _, want := range []string{"fetch_queue", "[5]", "hello", "[6]", "(voice)", "myproject"} {
		if !strings.Contains(got, want) {
			t.Errorf("backlog summary missing %q; got %q", want, got)
		}
	}
}

// Item C: a numeric-STRING limit ("5") must be parsed and honored, not silently
// dropped to the default 3 (the old switch matched neither "all" nor float64).
func TestParseFetchLimit_Codex(t *testing.T) {
	cases := []struct {
		name      string
		in        any
		wantLimit int
		wantAll   bool
	}{
		{"string-number 5", "5", 5, false},
		{"all", "all", 0, true},
		{"json number", float64(4), 4, false},
		{"string over cap clamps to 50", "999", 50, false},
		{"unparseable falls back to default 3", "xyz", 3, false},
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
