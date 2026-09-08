package main

import (
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestCrossSessionRealHostPeerReceipt(t *testing.T) {
	raw, err := os.ReadFile("testdata/claude-2.1.263-peer.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, old, replacement string
		want                   bool
	}{
		{"matching attempt", "", "", true},
		{"wrong delivery", "DELIVERYTOKEN-3", "DELIVERYTOKEN-other", false},
		{"wrong attempt", "cross-session:1", "cross-session:2", false},
		{"wrong from", `"from":"c3"`, `"from":"other"`, false},
		{"missing from", `"from":"c3",`, "", false},
		{"not meta", `"isMeta":true`, `"isMeta":false`, false},
		{"wrong kind", `"kind":"peer"`, `"kind":"channel"`, false},
		{"wrong type", `"type":"user"`, `"type":"assistant"`, false},
		{"wrong role", `"role":"user"`, `"role":"assistant"`, false},
		{"optional pid absent", `"verifiedPeerPid":12345,`, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line := raw
			if tc.old != "" {
				if !strings.Contains(string(raw), tc.old) {
					t.Fatal("fixture mutation did not match")
				}
				line = []byte(strings.Replace(string(raw), tc.old, tc.replacement, 1))
			}
			if got := deliveryReceipt(line, "DELIVERYTOKEN-3", true, "cross-session:1"); got != tc.want {
				t.Fatalf("receipt=%v want=%v", got, tc.want)
			}
			path := t.TempDir() + "/transcript.jsonl"
			if err := os.WriteFile(path, line, 0600); err != nil {
				t.Fatal(err)
			}
			if _, got := scanReceipt(path, 0, "DELIVERYTOKEN-3", new(bool), true, "cross-session:1"); got != tc.want {
				t.Fatalf("readback=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestCrossSessionReceiptWithHostPrefix(t *testing.T) {
	// Exercise block parsing behind the verified host prefix and peer provenance.
	for _, tc := range []struct {
		name, content string
		want          bool
	}{
		{"block", `<channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">body</channel>`, true},
		{"unverified prefix", `Peer input: <channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">body</channel> Host guidance.`, false},
		{"wrapper", `<cross-session-message from="c3"><channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">body</channel></cross-session-message>`, false},
		{"first marker wins", `<channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="other">body</channel><channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">nested</channel>`, false},
		{"malformed first", `<channel broken><channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">nested</channel>`, false},
		{"missing close", `<channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">body`, false},
		{"self close", `<channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T"/>`, false},
		{"self close with stray closer", `<channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T"/></channel>`, false},
		{"duplicate", `<channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T" source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">body</channel>`, false},
		{"embedded attribute", `<channel note=' source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T"'>body</channel>`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, blocks := range []bool{false, true} {
				var content any = "Another Claude session sent a message:\n" + tc.content
				if blocks {
					content = []any{map[string]any{"type": "text", "text": content}}
				}
				line, _ := json.Marshal(map[string]any{"type": "user", "isMeta": true, "origin": map[string]any{"kind": "peer", "from": "c3"}, "message": map[string]any{"role": "user", "content": content}})
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
	line := []byte(`{"type":"user","isMeta":true,"origin":{"kind":"peer","from":"c3"},"message":{"role":"user","content":[{"type":"text","text":"Another Claude session sent a message:\n<channel source='plugin:c3:c3' c3_attempt='cross-session:1' c3_delivery_id='other'>first</channel>"},{"type":"text","text":"Another Claude session sent a message:\n<channel source='plugin:c3:c3' c3_attempt='cross-session:1' c3_delivery_id='T'>second</channel>"}]}}`)
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
		{"peer", true, map[string]any{"kind": "peer", "from": "c3"}, true, true},
		{"no origin", true, nil, false, false},
		{"not meta", false, map[string]any{"kind": "peer", "from": "c3"}, true, false},
		{"missing meta", nil, map[string]any{"kind": "peer", "from": "c3"}, true, false},
		{"null origin", true, nil, true, false},
		{"other origin", true, map[string]any{"kind": "channel", "from": "c3"}, true, false},
		{"missing from", true, map[string]any{"kind": "peer"}, true, false},
		{"other from", true, map[string]any{"kind": "peer", "from": "other"}, true, false},
		{"empty origin", true, map[string]any{}, true, false},
		{"string origin", true, "peer", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := map[string]any{"type": "user", "isMeta": tc.meta, "message": map[string]any{"role": "user", "content": "Another Claude session sent a message:\n" + block}}
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
		block,
		"Peer input: " + block,
		"Peer input:\n" + block,
		"<cross-session-message from=\"c3\">" + block,
		"Another Claude session sent a message:\n " + block,
		"Another Claude session sent a message:\nAnother Claude session sent a message:\n" + block,
		" " + block,
		"Peer input:\n<cross-session-message from=\"c3\">" + block + "</cross-session-message>",
		[]any{map[string]any{"type": "text", "text": "quoted next"}, map[string]any{"type": "text", "text": "Another Claude session sent a message:\n" + block}},
		[]any{map[string]any{"type": "tool_result", "text": "quoted next"}, map[string]any{"type": "text", "text": "Another Claude session sent a message:\n" + block}},
	} {
		line, _ := json.Marshal(map[string]any{"type": "user", "isMeta": true, "origin": map[string]any{"kind": "peer", "from": "c3"}, "message": map[string]any{"role": "user", "content": content}})
		if deliveryReceipt(line, "T", true, "cross-session:1") {
			t.Fatal("non-prefix block accepted")
		}
	}
	for _, attempt := range []string{"channel:1", "cross-session:2", ""} {
		line, _ := json.Marshal(map[string]any{"type": "user", "isMeta": true, "origin": map[string]any{"kind": "peer", "from": "c3"}, "message": map[string]any{"role": "user", "content": "Another Claude session sent a message:\n" + strings.Replace(block, "cross-session:1", attempt, 1)}})
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
	text := `<unknown-wrapper token="AUTH-SECRET" attempt="cross-session:1">` + strings.Repeat("x", 150) + marker
	line, _ := json.Marshal(map[string]any{"type": "user", "isMeta": true, "origin": map[string]any{"kind": "peer", "from": "c3"}, "message": map[string]any{"role": "user", "content": text}})
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
	if strings.Contains(string(output.Bytes()), marker) || strings.Contains(string(output.Bytes()), "AUTH-SECRET") || strings.Contains(string(output.Bytes()), "cross-session:1") {
		t.Fatal("secret leaked")
	}
}

func TestCrossSessionRejectedCandidateDebugSwitch(t *testing.T) {
	oldWriter, oldFlags := log.Writer(), log.Flags()
	oldLevel := slog.SetLogLoggerLevel(slog.LevelInfo)
	defer func() {
		slog.SetLogLoggerLevel(oldLevel)
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	}()
	var output safeBuffer
	log.SetOutput(&output)
	line := []byte(`{"type":"user","isMeta":true,"origin":{"kind":"peer","from":"c3"},"message":{"role":"user","content":"Unknown host prefix: DELIVERY-SECRET cross-session:1"}}`)
	deliveryReceipt(line, "DELIVERY-SECRET", true, "cross-session:1")
	if len(output.Bytes()) != 0 {
		t.Fatal("debug preview logged without debug enabled")
	}
	// run enables this bridge when C3_DEBUG=1; exercise the actual default
	// handler writing through log, without installing a custom slog handler.
	slog.SetLogLoggerLevel(slog.LevelDebug)
	deliveryReceipt(line, "DELIVERY-SECRET", true, "cross-session:1")
	got := string(output.Bytes())
	if !strings.Contains(got, "cross-session receipt candidate rejected") || !strings.Contains(got, "prefix_redacted=") {
		t.Fatalf("debug switch did not reach adapter logger: %s", got)
	}
	for _, secret := range []string{"DELIVERY-SECRET", "cross-session:1", "Unknown host prefix"} {
		if strings.Contains(got, secret) {
			t.Fatal("secret leaked through adapter logger")
		}
	}
}
