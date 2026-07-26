package main

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
)

func TestBuildInstructions_NoConfig(t *testing.T) {
	a := &adapter{helloAck: ipc.HelloAckMsg{NoConfig: true}}
	out := a.buildInstructions()
	if !strings.Contains(out, "/c3:setup") {
		t.Fatalf("NoConfig head missing setup hint: %q", out)
	}
}

func TestBuildInstructions_Default(t *testing.T) {
	a := &adapter{helloAck: ipc.HelloAckMsg{}}
	out := a.buildInstructions()
	if !strings.Contains(out, "Use the `attach` tool") {
		t.Fatalf("default head missing attach hint: %q", out)
	}
}

func TestInstanceIDFromEnv(t *testing.T) {
	t.Setenv("CLAUDE_CODE_SESSION_ID", "inst-abc")
	if got := instanceIDFromEnv(); got != "inst-abc" {
		t.Fatalf("instanceIDFromEnv = %q, want inst-abc", got)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	if got := instanceIDFromEnv(); got != "" {
		t.Fatalf("unset instance id should be empty, got %q", got)
	}
}

// renderRecoverNotice is the new surface for auto-attach-on-resume (the notice
// the adapter emits after a successful RecoverSessionResp).
func TestRenderRecoverNotice_WithBacklog(t *testing.T) {
	out := renderRecoverNotice(ipc.RecoverSessionResp{Recovered: true, Name: "c3", QueuedCount: 2})
	if !strings.Contains(out, `Auto-attached to "c3"`) || !strings.Contains(out, "2 messages held") || !strings.Contains(out, "fetch_queue") {
		t.Fatalf("backlog notice malformed: %q", out)
	}
}

func TestRenderRecoverNotice_SingularAndNone(t *testing.T) {
	one := renderRecoverNotice(ipc.RecoverSessionResp{Recovered: true, Name: "c3", QueuedCount: 1})
	if !strings.Contains(one, "1 message held") {
		t.Fatalf("expected singular '1 message held': %q", one)
	}
	none := renderRecoverNotice(ipc.RecoverSessionResp{Recovered: true, Name: "c3", QueuedCount: 0})
	if !strings.Contains(none, `Auto-attached to "c3"`) || strings.Contains(none, "held") {
		t.Fatalf("zero-backlog notice should not mention held messages: %q", none)
	}
	if renderRecoverNotice(ipc.RecoverSessionResp{Recovered: true, Name: ""}) != "" {
		t.Fatal("a nameless recover should render nothing")
	}
}
