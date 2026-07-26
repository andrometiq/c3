package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/proctree"
)

// runSessions is the `c3-broker sessions` subcommand. Lists every live
// adapter the broker is currently tracking with its attached state,
// marking the session that owns the calling terminal (best effort).
//
// Usage:
//
//	c3-broker sessions
//
// The matching slash command is /c3:sessions (TODO #19e, 2026-05-19).
// Mirrors `c3-broker ping` (cmd/c3-broker/ping.go).
func runSessions(_ []string) error {
	cwd, _ := os.Getwd()
	pid := proctree.BestEffortCallerPID()

	conn, err := dialBroker()
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.WriteJSON(ipc.ListSessionsReq{
		Op: ipc.OpListSessions, PID: pid, CWD: cwd,
	}); err != nil {
		return fmt.Errorf("write list_sessions: %w", err)
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		return fmt.Errorf("read list_sessions_reply: %w", err)
	}
	var resp ipc.ListSessionsReplyMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("parse list_sessions_reply: %w", err)
	}
	fmt.Print(renderSessionsTable(resp.Sessions))
	return nil
}

// renderSessionsTable formats a slice of SessionEntry into the
// monospace-friendly table the slash command surfaces to the user.
// CWD is home-shortened to "~/..." for readability. Empty AttachedTo
// renders as "-". IsThisSession=true renders as "yes" in the This?
// column; false renders blank (keeps non-matching rows visually
// quiet so the "you are here" marker stands out).
func renderSessionsTable(sessions []ipc.SessionEntry) string {
	if len(sessions) == 0 {
		return "0 c3 sessions.\n"
	}

	home, _ := os.UserHomeDir()
	rows := make([][]string, 0, len(sessions))
	for _, s := range sessions {
		cwd := s.CWD
		if home != "" && strings.HasPrefix(cwd, home) {
			cwd = "~" + cwd[len(home):]
		}
		attached := s.AttachedTo
		if attached == "" {
			attached = "-"
		}
		thisMark := ""
		if s.IsThisSession {
			thisMark = "yes"
		}
		rows = append(rows, []string{
			s.CLI,
			strconv.Itoa(s.PID),
			cwd,
			attached,
			thisMark,
		})
	}

	headers := []string{"CLI", "PID", "CWD", "Attached", "This?"}
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, r := range rows {
		for i, c := range r {
			if l := len(c); l > widths[i] {
				widths[i] = l
			}
		}
	}

	var b strings.Builder
	noun := "sessions"
	if len(sessions) == 1 {
		noun = "session"
	}
	fmt.Fprintf(&b, "%d c3 %s:\n\n", len(sessions), noun)
	// Header row.
	writeRow(&b, headers, widths)
	// Body rows.
	for _, r := range rows {
		writeRow(&b, r, widths)
	}
	return b.String()
}

// writeRow renders one row with each cell left-padded to widths[i].
// Two-space separator between columns. The trailing column is
// written without padding so the final line doesn't carry trailing
// spaces.
func writeRow(b *strings.Builder, cells []string, widths []int) {
	for i, c := range cells {
		if i == len(cells)-1 {
			b.WriteString(c)
		} else {
			fmt.Fprintf(b, "%-*s  ", widths[i], c)
		}
	}
	b.WriteString("\n")
}
