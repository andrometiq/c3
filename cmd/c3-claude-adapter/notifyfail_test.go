package main

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// TestInboundContentSummary_CapturesContent guards D4 (adapter-ipc-4): on a
// notify FAIL the inbound is otherwise lost with no record (the broker already
// counted it "delivered" when it wrote to our IPC socket), so the summary used
// in the failure log MUST include the actual content — sender, text, and
// attachment summary — to be recoverable from adapter.log.
func TestInboundContentSummary_CapturesContent(t *testing.T) {
	in := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -100,
		MessageID: 7,
		Text:      "important message that must not be lost",
		Sender:    c3types.Sender{Username: "alice", UserID: 42},
		Attachments: []c3types.Attachment{
			{Kind: "document", Size: 1234},
		},
	}
	got := inboundContentSummary(in)
	for _, want := range []string{"from=@alice", `text="important message that must not be lost"`, "attach=document/1234"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary missing %q; got %q", want, got)
		}
	}
}

// TestInboundContentSummary_Event renders an event kind so an event lost on a
// notify FAIL is still identifiable.
func TestInboundContentSummary_Event(t *testing.T) {
	in := &c3types.Inbound{
		Channel: "telegram",
		Kind:    c3types.InboundPollResult,
		Event:   &c3types.InboundEvent{PollResult: &c3types.PollResult{PollID: "p1"}},
	}
	got := inboundContentSummary(in)
	if !strings.Contains(got, "event=poll_result") {
		t.Errorf("summary missing event kind; got %q", got)
	}
}

// TestInboundContentSummary_Empty: a contentless inbound yields a stable marker
// rather than an empty string (so the log line is never ambiguous).
func TestInboundContentSummary_Empty(t *testing.T) {
	if got := inboundContentSummary(&c3types.Inbound{}); got != "(no content)" {
		t.Errorf("empty inbound summary: want (no content); got %q", got)
	}
}
