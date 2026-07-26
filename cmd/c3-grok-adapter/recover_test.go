package main

import (
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
)

func TestRenderGrokRecoverNotice_Empty(t *testing.T) {
	if got := renderGrokRecoverNotice(ipc.RecoverSessionResp{}); got != "" {
		t.Fatalf("empty name should yield empty notice, got %q", got)
	}
}

func TestRenderGrokRecoverNotice_NoQueue(t *testing.T) {
	got := renderGrokRecoverNotice(ipc.RecoverSessionResp{Name: "c3", Recovered: true})
	if got == "" || !containsAll(got, "c3", "auto-attached") {
		t.Fatalf("got %q", got)
	}
}

func TestRenderGrokRecoverNotice_WithQueue(t *testing.T) {
	got := renderGrokRecoverNotice(ipc.RecoverSessionResp{
		Name: "c3", Recovered: true, QueuedCount: 2,
		QueuedSummary: []ipc.QueuedItem{
			{MessageID: 1, Sender: "@a", Kind: "text", Preview: "hi"},
		},
	})
	if !containsAll(got, "c3", "2", "fetch_queue", "@a") {
		t.Fatalf("got %q", got)
	}
}

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		if !containsStr(s, p) {
			return false
		}
	}
	return true
}

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		(len(s) > 0 && (func() bool {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		})()))
}
