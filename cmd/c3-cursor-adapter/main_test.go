package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestParseFetchLimit locks the hardened, on-parity semantics ported from the
// Grok/Claude adapters (findings 1+3). The pre-fix cursor version forwarded a
// negative numeric limit RAW — and the broker worker treats n<0 as the
// consume-ALL sentinel with default ack=true, which would destructively drain
// and retire the whole durable queue (internal/broker/queue_dispatch.go relies
// on the ADAPTER to clamp). It also ignored numeric STRINGS ("5" fell to the
// default 3), matched "all" case-sensitively, and had no max cap.
//
// Invariants asserted below: "all"/"ALL" => All; a numeric string is honored and
// clamped to [1,50]; a JSON number is honored and clamped; anything unparseable
// or absent falls back to the spec default 3; and the returned Limit is NEVER
// negative (the finding-1 safety property).
func TestParseFetchLimit(t *testing.T) {
	cases := []struct {
		name      string
		in        any
		wantLimit int
		wantAll   bool
	}{
		{"all lowercase", "all", 0, true},
		{"all uppercase", "ALL", 0, true},
		{"all mixed case", "AlL", 0, true},
		{"string number 5", "5", 5, false},
		{"string number padded", " 7 ", 7, false},
		{"json number 5", float64(5), 5, false},
		{"json negative -1 falls back to default", float64(-1), 3, false},
		{"string negative -1 clamps to at least 1", "-1", 1, false},
		{"json zero falls back to default", float64(0), 3, false},
		{"string zero clamps to at least 1", "0", 1, false},
		{"json 1000 clamps to 50", float64(1000), 50, false},
		{"string 1000 clamps to 50", "1000", 50, false},
		{"unparseable falls back to default 3", "abc", 3, false},
		{"absent falls back to default 3", nil, 3, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotLimit, gotAll := parseFetchLimit(tc.in)
			if gotLimit != tc.wantLimit || gotAll != tc.wantAll {
				t.Fatalf("parseFetchLimit(%#v) = (%d, %v), want (%d, %v)",
					tc.in, gotLimit, gotAll, tc.wantLimit, tc.wantAll)
			}
			// Finding-1 safety property: the Limit must NEVER be negative, or the
			// broker worker's n<0 consume-ALL sentinel would drain the queue.
			if gotLimit < 0 {
				t.Fatalf("parseFetchLimit(%#v) returned NEGATIVE limit %d — would trip the broker consume-ALL sentinel", tc.in, gotLimit)
			}
			// When not draining everything, a real bounded pull must be >= 1.
			if !gotAll && gotLimit < 1 {
				t.Fatalf("parseFetchLimit(%#v) returned bounded limit %d < 1", tc.in, gotLimit)
			}
		})
	}
}

func TestParseFetchLimitStr(t *testing.T) {
	cases := []struct {
		in        string
		wantLimit int
		wantAll   bool
	}{
		{"", 0, true},
		{"all", 0, true},
		{"ALL", 0, true},
		{"5", 5, false},
		{" 3 ", 3, false},
		{"999", 50, false},
		{"0", 0, true},
		{"nope", 0, true},
	}
	for _, c := range cases {
		gotL, gotA := parseFetchLimitStr(c.in)
		if gotL != c.wantLimit || gotA != c.wantAll {
			t.Errorf("parseFetchLimitStr(%q) = (%d,%v), want (%d,%v)",
				c.in, gotL, gotA, c.wantLimit, c.wantAll)
		}
	}
}

// textBlocks extracts the Text of every *mcp.TextContent in a CallToolResult,
// so the assertions below don't depend on block ordering helpers.
func textBlocks(t *testing.T, res *mcp.CallToolResult) []string {
	t.Helper()
	if res == nil {
		t.Fatal("nil CallToolResult")
	}
	var out []string
	for _, c := range res.Content {
		tc, ok := c.(*mcp.TextContent)
		if !ok {
			t.Fatalf("content block is %T, want *mcp.TextContent", c)
		}
		out = append(out, tc.Text)
	}
	return out
}

// TestMapResult locks finding 2: the broker ALWAYS returns the standard MCP
// shape {"content":[{"type":"text","text":…}]} (internal/broker/dispatch.go
// mcpText) and NEVER a top-level "text" key. The pre-fix mapResult read
// m["text"], so every forwarded tool result reached the agent as a raw JSON
// envelope. mapResult must now extract the content[].text blocks and keep the
// JSON dump only as the true fallback.
func TestMapResult(t *testing.T) {
	t.Run("broker-shaped single text block", func(t *testing.T) {
		in := map[string]any{
			"content": []any{
				map[string]any{"type": "text", "text": "done"},
			},
		}
		got := textBlocks(t, mapResult(in))
		if len(got) != 1 || got[0] != "done" {
			t.Fatalf("got %#v, want [\"done\"]", got)
		}
	})

	t.Run("broker-shaped multi text block", func(t *testing.T) {
		in := map[string]any{
			"content": []any{
				map[string]any{"type": "text", "text": "first"},
				map[string]any{"type": "text", "text": "second"},
			},
		}
		got := textBlocks(t, mapResult(in))
		if len(got) != 2 || got[0] != "first" || got[1] != "second" {
			t.Fatalf("got %#v, want [\"first\" \"second\"]", got)
		}
	})

	t.Run("missing content falls back to JSON dump", func(t *testing.T) {
		in := map[string]any{"foo": "bar"}
		got := textBlocks(t, mapResult(in))
		if len(got) != 1 {
			t.Fatalf("got %d blocks, want 1 fallback block: %#v", len(got), got)
		}
		// The fallback is the JSON-encoded whole map, NOT an extracted string.
		if !strings.Contains(got[0], `"foo"`) || !strings.Contains(got[0], `"bar"`) {
			t.Fatalf("fallback block %q is not the JSON dump of the result map", got[0])
		}
	})

	t.Run("malformed content (not array) falls back to JSON dump", func(t *testing.T) {
		in := map[string]any{"content": "not-an-array"}
		got := textBlocks(t, mapResult(in))
		if len(got) != 1 || !strings.Contains(got[0], `"content"`) {
			t.Fatalf("got %#v, want single JSON-dump fallback block", got)
		}
	})

	t.Run("nil result yields empty text", func(t *testing.T) {
		got := textBlocks(t, mapResult(nil))
		if len(got) != 1 || got[0] != "" {
			t.Fatalf("got %#v, want single empty block", got)
		}
	})
}

// TestStopPollDescriptionHonesty locks finding 4: cursor is a pull-only host
// (CannotRenderChannels), so the automatic poll-close <channel> event is NOT
// rendered here and — because synthesized channel events are never queued
// (internal/broker/worker.go) — is not recoverable via fetch_queue either. The
// stop_poll description must therefore NOT promise that results "arrive
// automatically", and must point the agent at the deterministic read.
func TestStopPollDescriptionHonesty(t *testing.T) {
	a := newAdapter()
	srv := a.buildMCPServer()
	if srv == nil {
		t.Fatal("buildMCPServer returned nil")
	}

	clientT, serverT := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = srv.Run(ctx, serverT) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.0"}, nil)
	sess, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	defer sess.Close()

	listResult, err := sess.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	var desc string
	found := false
	for _, tool := range listResult.Tools {
		if tool.Name == "stop_poll" {
			desc = tool.Description
			found = true
			break
		}
	}
	if !found {
		t.Fatal("stop_poll tool not registered")
	}
	if strings.Contains(desc, "arrive automatically") {
		t.Errorf("stop_poll description still promises auto-arrival (pull-only host cannot render events): %q", desc)
	}
	if !strings.Contains(desc, "fetch_queue") {
		t.Errorf("stop_poll description should reference fetch_queue when setting honest expectations: %q", desc)
	}
	if !strings.Contains(desc, "pull-only") {
		t.Errorf("stop_poll description should state this host is pull-only: %q", desc)
	}
}
