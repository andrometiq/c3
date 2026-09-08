package main

import (
	"encoding/json"
	"log"
	"log/slog"
	"strings"
	"testing"
)

func TestCrossSessionReceiptWithHostWrapper(t *testing.T) {
	// Deliberately fixture-shaped, not described as live-captured. Metadata like
	// origin.kind/isMeta is required when present; only a peer user record confirms.
	for _, tc := range []struct {
		name, content string
		want          bool
	}{
		{"bare", `<channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">body</channel>`, true},
		{"prefix", `Peer input: <channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">body</channel> Host guidance.`, true},
		{"wrapper", `<cross-session-message from="c3"><channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">body</channel></cross-session-message>`, true},
		{"first marker wins", `<channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="other">body</channel><channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">nested</channel>`, false},
		{"malformed first", `<channel broken><channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">nested</channel>`, false},
		{"missing close", `Peer input: <channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">body`, false},
		{"self close", `Peer input: <channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T"/>`, false},
		{"self close with stray closer", `Peer input: <channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T"/></channel>`, false},
		{"duplicate", `Peer input: <channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T" source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">body</channel>`, false},
		{"embedded attribute", `Peer input: <channel note=' source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T"'>body</channel>`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, blocks := range []bool{false, true} {
				var content any = tc.content
				if blocks {
					content = []any{map[string]any{"type": "text", "text": tc.content}}
				}
				line, _ := json.Marshal(map[string]any{"type": "user", "isMeta": true, "origin": map[string]any{"kind": "peer"}, "message": map[string]any{"role": "user", "content": content}})
				if got := deliveryReceipt(line, "T", true, "cross-session:1"); got != tc.want {
					t.Fatalf("peer receipt = %v want %v", got, tc.want)
				}
				if !strings.HasPrefix(tc.content, "<channel") && channelReceipt(line, "T") {
					t.Fatal("channel route accepted a peer prefix")
				}
				for _, bad := range []string{
					strings.Replace(string(line), `"role":"user"`, `"role":"assistant"`, 1),
					strings.Replace(string(line), `"type":"user"`, `"type":"queue-operation"`, 1),
				} {
					if deliveryReceipt([]byte(bad), "T", true, "cross-session:1") {
						t.Fatal("non-user receipt accepted")
					}
				}
			}
		})
	}
	line := []byte(`{"type":"user","message":{"role":"user","content":[{"type":"text","text":"<channel source='plugin:c3:c3' c3_attempt='cross-session:1' c3_delivery_id='other'>first</channel>"},{"type":"text","text":"<channel source='plugin:c3:c3' c3_attempt='cross-session:1' c3_delivery_id='T'>second</channel>"}]}}`)
	if deliveryReceipt(line, "T", true, "cross-session:1") {
		t.Fatal("skipped the first opener in another content block")
	}
}

func TestCrossSessionReceiptProvenance(t *testing.T) {
	block := `<channel source="plugin:c3:c3" c3_delivery_id="T" c3_attempt="cross-session:1">body</channel>`
	for _, tc := range []struct {
		name          string
		meta          any
		origin        any
		present, want bool
	}{
		{"peer", true, map[string]any{"kind": "peer"}, true, true},
		{"no origin", true, nil, false, true},
		{"not meta", false, nil, false, false},
		{"missing meta", nil, nil, false, false},
		{"null origin", true, nil, true, false},
		{"other origin", true, map[string]any{"kind": "channel"}, true, false},
		{"empty origin", true, map[string]any{}, true, false},
		{"string origin", true, "peer", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := map[string]any{"type": "user", "isMeta": tc.meta, "message": map[string]any{"role": "user", "content": block}}
			if tc.present {
				entry["origin"] = tc.origin
			}
			line, _ := json.Marshal(entry)
			if got := deliveryReceipt(line, "T", true, "cross-session:1"); got != tc.want {
				t.Fatalf("receipt=%v want=%v", got, tc.want)
			}
		})
	}
	for _, content := range []any{
		" " + block,
		"Peer input:\n<cross-session-message from=\"c3\">" + block + "</cross-session-message>",
		[]any{map[string]any{"type": "text", "text": "quoted next"}, map[string]any{"type": "text", "text": block}},
		[]any{map[string]any{"type": "tool_result", "text": "quoted next"}, map[string]any{"type": "text", "text": block}},
	} {
		line, _ := json.Marshal(map[string]any{"type": "user", "isMeta": true, "message": map[string]any{"role": "user", "content": content}})
		if deliveryReceipt(line, "T", true, "cross-session:1") {
			t.Fatal("non-prefix block accepted")
		}
	}
	for _, attempt := range []string{"channel:1", "cross-session:2", ""} {
		line, _ := json.Marshal(map[string]any{"type": "user", "isMeta": true, "message": map[string]any{"role": "user", "content": strings.Replace(block, "cross-session:1", attempt, 1)}})
		if deliveryReceipt(line, "T", true, "cross-session:1") {
			t.Fatalf("wrong attempt accepted: %s", attempt)
		}
	}
}

func TestCrossSessionRejectedCandidateDebugRedactsSecrets(t *testing.T) {
	old := slog.Default()
	oldWriter, oldFlags := log.Writer(), log.Flags()
	defer func() { slog.SetDefault(old); log.SetOutput(oldWriter); log.SetFlags(oldFlags) }()
	var output safeBuffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	marker := "DELIVERY-SECRET"
	text := `<unknown-wrapper token="AUTH-SECRET">` + strings.Repeat("x", 150) + marker
	line, _ := json.Marshal(map[string]any{"type": "user", "isMeta": true, "message": map[string]any{"role": "user", "content": text}})
	if deliveryReceipt(line, marker, true, "cross-session:1") {
		t.Fatal("unknown prefix accepted")
	}
	var event struct {
		Level  string
		Prefix string `json:"prefix_redacted"`
	}
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	if event.Level != "DEBUG" || len([]rune(event.Prefix)) != 120 || !strings.HasPrefix(event.Prefix, "<") {
		t.Fatalf("missing bounded debug preview: %+v", event)
	}
	if strings.Contains(string(output.Bytes()), marker) || strings.Contains(string(output.Bytes()), "AUTH-SECRET") {
		t.Fatal("secret leaked")
	}
}
