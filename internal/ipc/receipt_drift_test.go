package ipc

import (
	"strings"
	"testing"
)

func TestReceiptShapeHintHostMetadata(t *testing.T) {
	for _, version := range []string{"", "2.1.266\nforged", "/private/path", strings.Repeat("1", 65)} {
		if got := ReceiptHostVersion(version); got != "unknown" {
			t.Fatal(got)
		}
	}
	if ReceiptShapeHint("") != "" {
		t.Fatal("absent diagnostic should be silent")
	}
	want := "receipt shapes may have changed (host 2.1.266): run scripts/live-matrix/run.sh --collect-only --fixtures"
	if ReceiptShapeHint("2.1.266") != want {
		t.Fatal("hint drifted")
	}
}
