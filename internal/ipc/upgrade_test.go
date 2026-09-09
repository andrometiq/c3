package ipc

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestUpgradeBuildHelloWire(t *testing.T) {
	for _, msg := range []any{HelloMsg{Op: OpHello, Build: "build"}, HelloAckMsg{Op: OpHelloAck, Build: "build", Upgrade: &UpgradeHint{Path: "adapter", Build: "next"}}} {
		raw, err := json.Marshal(msg)
		if err != nil || !strings.Contains(string(raw), `"build":"build"`) {
			t.Fatalf("%s %v", raw, err)
		}
	}
	var old HelloMsg
	if err := json.Unmarshal([]byte(`{"op":"hello","cli":"claude"}`), &old); err != nil || old.Build != "" {
		t.Fatal(old, err)
	}
}
