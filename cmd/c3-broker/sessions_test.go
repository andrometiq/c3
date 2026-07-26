package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
)

func TestRenderSessionsTable_EmptyList(t *testing.T) {
	got := renderSessionsTable(nil)
	if got != "0 c3 sessions.\n" {
		t.Errorf("got %q, want \"0 c3 sessions.\\n\"", got)
	}
	got2 := renderSessionsTable([]ipc.SessionEntry{})
	if got2 != "0 c3 sessions.\n" {
		t.Errorf("empty slice: got %q, want \"0 c3 sessions.\\n\"", got2)
	}
}

func TestRenderSessionsTable_SingleSession_NoAttach(t *testing.T) {
	sessions := []ipc.SessionEntry{
		{CLI: "claude", PID: 159254, CWD: "/projects/c3"},
	}
	got := renderSessionsTable(sessions)
	if !strings.Contains(got, "1 c3 session") {
		t.Errorf("missing session count header: %q", got)
	}
	if !strings.Contains(got, "claude") {
		t.Errorf("missing CLI column: %q", got)
	}
	if !strings.Contains(got, "159254") {
		t.Errorf("missing PID column: %q", got)
	}
	// Unattached row renders AttachedTo as "-".
	if !strings.Contains(got, " - ") && !strings.Contains(got, " -\n") {
		t.Errorf("unattached row should render AttachedTo as '-': %q", got)
	}
}

func TestRenderSessionsTable_HomeShortenedCWD(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		t.Skip("no home dir")
	}
	sessions := []ipc.SessionEntry{
		{CLI: "claude", PID: 1, CWD: filepath.Join(home, "projects", "app")},
	}
	got := renderSessionsTable(sessions)
	if !strings.Contains(got, "~/projects/app") {
		t.Errorf("CWD should be home-shortened with ~: %q", got)
	}
	if strings.Contains(got, home) {
		t.Errorf("rendered output should not include the literal home prefix %q: %q", home, got)
	}
}

func TestRenderSessionsTable_MarksThisSession(t *testing.T) {
	sessions := []ipc.SessionEntry{
		{CLI: "claude", PID: 1001, CWD: "/p/a", AttachedTo: "c3 (main)", IsThisSession: true},
		{CLI: "claude", PID: 1002, CWD: "/p/b"},
		{CLI: "codex", PID: 1003, CWD: "/p/c", AttachedTo: "widget (work)"},
	}
	got := renderSessionsTable(sessions)
	lines := strings.Split(got, "\n")
	var yesCount int
	for _, line := range lines {
		// Skip the header / count line.
		if strings.Contains(line, "1001") || strings.Contains(line, "1002") || strings.Contains(line, "1003") {
			if strings.Contains(line, "yes") {
				yesCount++
				if !strings.Contains(line, "1001") {
					t.Errorf("yes marker on wrong row: %q", line)
				}
			}
		}
	}
	if yesCount != 1 {
		t.Errorf("expected exactly one 'yes' marker, got %d in output:\n%s", yesCount, got)
	}
}

func TestRenderSessionsTable_AttachedAndUnattachedFormatting(t *testing.T) {
	// Mixed rows render attached topics verbatim and unattached as "-".
	sessions := []ipc.SessionEntry{
		{CLI: "claude", PID: 1, CWD: "/p", AttachedTo: "c3 (main)"},
		{CLI: "claude", PID: 2, CWD: "/q"}, // unattached
	}
	got := renderSessionsTable(sessions)
	if !strings.Contains(got, "c3 (main)") {
		t.Errorf("attached row should render label: %q", got)
	}
}
