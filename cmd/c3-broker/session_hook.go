package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/queue"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
)

// sessionHookInput is the subset of the SessionStart hook's stdin JSON we use.
// Claude Code sends {session_id, cwd, source, transcript_path, hook_event_name};
// extra fields are ignored.
type sessionHookInput struct {
	SessionID string `json:"session_id"`
	CWD       string `json:"cwd"`
	Source    string `json:"source"`
}

// runSessionHook implements `c3-broker session-hook`, wired to C3 SessionStart
// hooks for Claude Code and Grok Build.
//
// Claude: maps EPHEMERAL per-MCP-spawn instance id (UUID dir in $CLAUDE_ENV_FILE
// == CLAUDE_CODE_SESSION_ID) → STABLE session id (stdin session_id).
//
// Grok: stdin/env already carry the stable UUID (session_id / GROK_SESSION_ID).
// We write a handoff keyed by that stable id so tools can re-read it; the Grok
// adapter primarily recovers via OpRecoverSession using active_sessions.json /
// GROK_SESSION_ID, but the handoff is a durable belt-and-suspenders record.
//
// CRITICAL: NEVER touches the broker socket; ALWAYS exit 0 (hook must not break
// the user's session).
func runSessionHook() error {
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "c3-broker session-hook: read stdin: %v (ignoring)\n", err)
		return nil
	}
	var in sessionHookInput
	if err := json.Unmarshal(raw, &in); err != nil {
		fmt.Fprintf(os.Stderr, "c3-broker session-hook: parse stdin: %v (ignoring)\n", err)
		return nil
	}
	// Grok/Antigravity hooks may use sessionId camelCase or conversation_id in some payloads; accept them via
	// a second pass if session_id empty.
	if in.SessionID == "" {
		var alt struct {
			SessionID      string `json:"sessionId"`
			ConversationID string `json:"conversation_id"`
			ConvID         string `json:"conversationId"`
			CWD            string `json:"cwd"`
			Source         string `json:"source"`
		}
		_ = json.Unmarshal(raw, &alt)
		if alt.SessionID != "" {
			in.SessionID = alt.SessionID
		} else if alt.ConversationID != "" {
			in.SessionID = alt.ConversationID
		} else if alt.ConvID != "" {
			in.SessionID = alt.ConvID
		}
		if in.CWD == "" {
			in.CWD = alt.CWD
		}
		if in.Source == "" {
			in.Source = alt.Source
		}
	}
	if in.SessionID == "" {
		if s := os.Getenv("ANTIGRAVITY_CONVERSATION_ID"); s != "" {
			in.SessionID = s
		} else if s := os.Getenv("GROK_SESSION_ID"); s != "" {
			in.SessionID = s
		}
	}
	if in.CWD == "" {
		if s := os.Getenv("GROK_WORKSPACE_ROOT"); s != "" {
			in.CWD = s
		} else if s := os.Getenv("CLAUDE_PROJECT_DIR"); s != "" {
			in.CWD = s
		}
	}

	instanceID := ""
	if env := os.Getenv("CLAUDE_ENV_FILE"); env != "" {
		instanceID = filepath.Base(filepath.Dir(env))
	}
	// Grok: no CLAUDE_ENV_FILE — key the handoff by the stable session id itself.
	// Only take this path when Grok env is present so Claude hooks without
	// CLAUDE_ENV_FILE still no-op (TestRunSessionHook_EmptyEnvFileNoOp).
	if (instanceID == "" || instanceID == "." || instanceID == string(filepath.Separator)) &&
		(os.Getenv("GROK_SESSION_ID") != "" || os.Getenv("GROK_HOOK_EVENT") != "") {
		// The Grok session id arrives straight from hook stdin, so any same-user
		// process can set it. Only trust it as the handoff filename stem when it
		// is a clean base name — no path separators, no "."/".." — mirroring the
		// invariant sessionhandoff.Path enforces. A crafted id (e.g. "../x") is
		// rejected here and falls through to the no-op below, so it can never key
		// a write outside the 0700 handoff dir.
		if in.SessionID != "" && in.SessionID == filepath.Base(in.SessionID) &&
			in.SessionID != "." && in.SessionID != ".." {
			instanceID = in.SessionID
		}
	}
	if instanceID == "" || instanceID == "." || instanceID == string(filepath.Separator) || in.SessionID == "" {
		fmt.Fprintf(os.Stderr, "c3-broker session-hook: no usable instance/session id (instance=%q session_present=%v) — nothing to do\n",
			instanceID, in.SessionID != "")
		return nil
	}

	if err := sessionhandoff.Write(instanceID, sessionhandoff.Entry{
		StableSessionID: in.SessionID,
		CWD:             in.CWD,
		Source:          in.Source,
		UnixNano:        time.Now().UnixNano(),
	}); err != nil {
		fmt.Fprintf(os.Stderr, "c3-broker session-hook: write handoff: %v (ignoring)\n", err)
		return nil
	}

	// On a RESUMED session ONLY (never startup/clear/compact), surface any held
	// backlog into the agent's first-turn context so the durable queue drains
	// without waiting for the user to ask. Best-effort; failures print nothing.
	if in.Source == "resume" {
		printResumeBacklogHint(in.SessionID)
	}
	return nil
}

// printResumeBacklogHint prints ONE SessionStart-context line when the resumed
// session's last-attached topic still has held messages in the durable queue,
// telling the agent to call fetch_queue at the start of its first reply.
//
// It is a filesystem-only HINT — mappings.json plus the lock-free queue store —
// and NEVER touches the broker socket (session_hook's load-bearing invariant,
// see the runSessionHook doc): any error (unresolvable path, unreadable mappings,
// no attachment, empty queue) prints nothing and returns. Slight staleness is
// acceptable: the append-only store tolerates concurrent broker writes, and this
// is only a hint. A SessionStart hook's stdout is added to the model's context
// (Claude Code hooks contract, verified 2026-07-18), so one plain line suffices —
// no structured JSON needed.
func printResumeBacklogHint(stableID string) {
	path, err := mappings.DefaultPath()
	if err != nil {
		return
	}
	mf, err := mappings.Read(path)
	if err != nil {
		return
	}
	sa, ok := mf.LookupSessionAttachment(stableID)
	if !ok {
		return
	}
	store, err := queue.NewStore(queue.QueueDir())
	if err != nil {
		return
	}
	// Mirror routeKeyFromSessionAttachment (internal/broker/attach.go), but produce
	// a queue.RouteKey directly. sa.TopicID nil ⇒ DM/no-topic (File() → "…__none").
	n, _ := store.Pending(queue.RouteKey{Channel: sa.Channel, ChatID: sa.ChatID, TopicID: sa.TopicID})
	if n <= 0 {
		return
	}
	name := sa.Name
	if name == "" {
		name = "the last topic"
	}
	noun := "message"
	if n > 1 {
		noun = "messages"
	}
	fmt.Printf("C3: this resumed session re-attaches to Telegram topic %q — %d held %s waiting in the durable queue. At the start of your first reply, call fetch_queue to surface them to the user before proceeding.\n", name, n, noun)
}
