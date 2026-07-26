package telegram

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// A normal 3-column GFM table with an alignment delimiter row and inline styling
// in a cell — the canonical rich-eligible reply.
const sampleTable = "| Name | Role | Notes |\n" +
	"|:-----|:----:|------:|\n" +
	"| Ada  | **Lead** | `wip` |\n" +
	"| Bob  | Eng  | done |"

// wideTable has 21 header columns — one over the maxRichColumns (20) cap.
func wideTable() string {
	hdr := make([]string, 21)
	del := make([]string, 21)
	row := make([]string, 21)
	for i := range hdr {
		hdr[i] = "c"
		del[i] = "---"
		row[i] = "x"
	}
	return "| " + strings.Join(hdr, " | ") + " |\n" +
		"| " + strings.Join(del, " | ") + " |\n" +
		"| " + strings.Join(row, " | ") + " |"
}

// TestRichTableEligible_Routing covers the routing decision: a table on a
// markdown reply with the switch ON is rich-eligible; the same with the switch
// OFF is not; a non-table reply is never rich-eligible; and a table wider than
// the column cap falls back even when the switch is ON.
func TestRichTableEligible_Routing(t *testing.T) {
	cases := []struct {
		name    string
		enabled bool
		markup  c3types.Markup
		text    string
		want    bool
	}{
		{"table + switch ON → rich", true, c3types.MarkupMarkdown, sampleTable, true},
		{"table + empty-markup (default) ON → rich", true, "", sampleTable, true},
		{"table + switch OFF → monospace", false, c3types.MarkupMarkdown, sampleTable, false},
		{"non-table reply → not rich", true, c3types.MarkupMarkdown, "just some prose, no table", false},
		{"table wider than col cap → fallback even when ON", true, c3types.MarkupMarkdown, wideTable(), false},
		{"native markup intent → not rich", true, c3types.MarkupNative, sampleTable, false},
		{"plain markup intent → not rich", true, c3types.MarkupNone, sampleTable, false},
		{"prose with a stray pipe but no delimiter → not rich", true, c3types.MarkupMarkdown, "a | b is not a table", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := richTableEligible(tc.enabled, tc.markup, tc.text); got != tc.want {
				t.Errorf("richTableEligible(%v, %q, ...) = %v; want %v", tc.enabled, tc.markup, got, tc.want)
			}
		})
	}
}

// TestRichTableEligible_OverRowCap proves the row cap (maxRichBlocks) is enforced
// so a table with too many rows falls back even when the switch is ON.
func TestRichTableEligible_OverRowCap(t *testing.T) {
	var b strings.Builder
	b.WriteString("| a | b |\n| --- | --- |\n")
	for i := 0; i <= maxRichBlocks; i++ { // header + delimiter + (maxRichBlocks+1) body rows
		b.WriteString("| x | y |\n")
	}
	if richTableEligible(true, c3types.MarkupMarkdown, b.String()) {
		t.Error("a table over the row cap must NOT be rich-eligible (should fall back to monospace)")
	}
}

// TestRichTableEligible_OverCharBudget proves an over-budget reply is left on the
// existing (chunking) path.
func TestRichTableEligible_OverCharBudget(t *testing.T) {
	huge := sampleTable + "\n" + strings.Repeat("x", maxRichChars)
	if richTableEligible(true, c3types.MarkupMarkdown, huge) {
		t.Error("a reply over the rich-message char budget must NOT be rich-eligible")
	}
}

// TestBuildRichParams_Shape pins the sendRichMessage param-builder shape:
// chat_id, rich_message.markdown, the optional message_thread_id (topic), and
// the optional reply_parameters (reply).
func TestBuildRichParams_Shape(t *testing.T) {
	t.Run("minimal (no topic, no reply)", func(t *testing.T) {
		p := buildRichParams(-100, sampleTable, nil, nil)
		if got, ok := p["chat_id"].(int64); !ok || got != -100 {
			t.Errorf("chat_id = %v; want int64(-100)", p["chat_id"])
		}
		rm, ok := p["rich_message"].(map[string]any)
		if !ok {
			t.Fatalf("rich_message missing or wrong type: %T", p["rich_message"])
		}
		if md, ok := rm["markdown"].(string); !ok || md != sampleTable {
			t.Errorf("rich_message.markdown = %q; want the original markdown", rm["markdown"])
		}
		// No html dialect key (markdown-only in this phase).
		if _, exists := rm["html"]; exists {
			t.Error("rich_message must NOT carry an html key (markdown dialect only)")
		}
		if _, exists := p["message_thread_id"]; exists {
			t.Error("message_thread_id must be ABSENT when no topic is set")
		}
		if _, exists := p["reply_parameters"]; exists {
			t.Error("reply_parameters must be ABSENT when not replying")
		}
	})

	t.Run("with topic and reply", func(t *testing.T) {
		topic := int64(42)
		replyTo := int64(777)
		p := buildRichParams(-100, sampleTable, &topic, &replyTo)
		if got, ok := p["message_thread_id"].(int64); !ok || got != 42 {
			t.Errorf("message_thread_id = %v; want int64(42)", p["message_thread_id"])
		}
		rp, ok := p["reply_parameters"].(map[string]any)
		if !ok {
			t.Fatalf("reply_parameters missing or wrong type: %T", p["reply_parameters"])
		}
		if got, ok := rp["message_id"].(int64); !ok || got != 777 {
			t.Errorf("reply_parameters.message_id = %v; want int64(777)", rp["message_id"])
		}
		if got, ok := rp["allow_sending_without_reply"].(bool); !ok || !got {
			t.Errorf("reply_parameters.allow_sending_without_reply = %v; want true", rp["allow_sending_without_reply"])
		}
	})
}

// TestRichTablesEnabled_On guards the ON reality: rich-tables was intentionally
// enabled (commit 0c13abf flipped richTablesEnabled=true) after the GFM-table →
// native RichBlockTable render was live-verified on Android 2026-06-17. The
// switch must stay ON and Capabilities().RichTables must reflect it. If this
// fails, the switch was flipped back OFF by accident — do NOT "fix" by disabling
// rich-tables; it is correctly ON.
func TestRichTablesEnabled_On(t *testing.T) {
	if !richTablesEnabled {
		t.Error("richTablesEnabled must be true (intentionally ON; live-verified on Android 2026-06-17)")
	}
	if caps := New().Capabilities(); !caps.RichTables {
		t.Error("Capabilities().RichTables must reflect the ON switch (true)")
	}
}
