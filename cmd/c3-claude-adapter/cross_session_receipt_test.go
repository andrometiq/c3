package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCrossSessionReceiptWithHostWrapper(t *testing.T) {
	// Deliberately fixture-shaped, not described as live-captured. Metadata like
	// origin.kind/isMeta is tolerated; only a complete user-role record confirms.
	for _, tc := range []struct {
		name, content string
		want          bool
	}{
		{"bare", `<channel c3_delivery_id="T">body</channel>`, true},
		{"prefix", `Peer input: <channel c3_delivery_id="T">body</channel> Host guidance.`, true},
		{"wrapper", `<cross-session-message from="c3"><channel c3_delivery_id="T">body</channel></cross-session-message>`, true},
		{"first marker wins", `<channel c3_delivery_id="other">body</channel><channel c3_delivery_id="T">nested</channel>`, false},
		{"malformed first", `<channel broken><channel c3_delivery_id="T">nested</channel>`, false},
		{"missing close", `Peer: <channel c3_delivery_id="T">body`, false},
		{"self close", `Peer: <channel c3_delivery_id="T"/>`, false},
		{"self close with stray closer", `Peer: <channel c3_delivery_id="T"/></channel>`, false},
		{"duplicate", `Peer: <channel c3_delivery_id="T" c3_delivery_id="T">body</channel>`, false},
		{"embedded attribute", `Peer: <channel note=' c3_delivery_id="T"'>body</channel>`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, blocks := range []bool{false, true} {
				var content any = tc.content
				if blocks {
					content = []any{map[string]any{"type": "text", "text": tc.content}}
				}
				line, _ := json.Marshal(map[string]any{"type": "user", "isMeta": true, "origin": map[string]any{"kind": "peer"}, "message": map[string]any{"role": "user", "content": content}})
				if got := deliveryReceipt(line, "T", true); got != tc.want {
					t.Fatalf("peer receipt = %v want %v", got, tc.want)
				}
				if !strings.HasPrefix(tc.content, "<channel") && channelReceipt(line, "T") {
					t.Fatal("channel route accepted a peer prefix")
				}
				for _, bad := range []string{
					strings.Replace(string(line), `"role":"user"`, `"role":"assistant"`, 1),
					strings.Replace(string(line), `"type":"user"`, `"type":"queue-operation"`, 1),
				} {
					if deliveryReceipt([]byte(bad), "T", true) {
						t.Fatal("non-user receipt accepted")
					}
				}
			}
		})
	}
	line := []byte(`{"type":"user","message":{"role":"user","content":[{"type":"text","text":"<channel c3_delivery_id='other'>first</channel>"},{"type":"text","text":"<channel c3_delivery_id='T'>second</channel>"}]}}`)
	if deliveryReceipt(line, "T", true) {
		t.Fatal("skipped the first opener in another content block")
	}
}
