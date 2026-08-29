// Package sessionhandoff is the on-disk rendezvous between the C3 SessionStart
// hook (run as `c3-broker session-hook`) and the C3 adapter.
//
// Why it exists: Claude Code's STABLE, --resume-able session id (== the
// transcript .jsonl filename) is delivered ONLY to a SessionStart hook (its
// stdin `session_id`). The adapter, spawned ~2s BEFORE the hook fires, knows
// only its own EPHEMERAL per-MCP-spawn instance id (CLAUDE_CODE_SESSION_ID).
// The verified rendezvous: that instance id equals the UUID directory in the
// hook's $CLAUDE_ENV_FILE path. So the hook writes a handoff keyed on the
// instance id, mapping it to the stable id; the adapter reads its own
// instance id's handoff shortly after hello and asks the broker to recover.
//
// Everything here is fail-closed: a missing/corrupt file is "not found", never
// an error that could block the adapter or break the user's session.
package sessionhandoff

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Entry is the SessionStart-hook → adapter handoff payload.
type Entry struct {
	StableSessionID string `json:"stable_session_id"`
	CWD             string `json:"cwd,omitempty"`
	Source          string `json:"source,omitempty"`
	TranscriptPath  string `json:"transcript_path,omitempty"`
	UnixNano        int64  `json:"unix_nano"`
}

// Dir is $XDG_STATE_HOME/c3/session-instances (fallback
// ~/.local/state/c3/session-instances). It is created (0700) by Write.
func Dir() (string, error) {
	state := os.Getenv("XDG_STATE_HOME")
	if state == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("sessionhandoff: resolve home: %w", err)
		}
		if home == "" {
			return "", fmt.Errorf("sessionhandoff: empty home dir")
		}
		state = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(state, "c3", "session-instances"), nil
}

// Path returns Dir()/<instanceID>.json. instanceID must be non-empty and a
// clean base name — any path separator or "." / ".." component is rejected, so
// a crafted CLAUDE_ENV_FILE can never escape the handoff directory.
func Path(instanceID string) (string, error) {
	if instanceID == "" {
		return "", fmt.Errorf("sessionhandoff: empty instance id")
	}
	// Must be exactly its own clean base name: no separators, no "."/"..".
	if instanceID != filepath.Base(instanceID) ||
		instanceID == "." || instanceID == ".." ||
		instanceID == string(filepath.Separator) {
		return "", fmt.Errorf("sessionhandoff: unsafe instance id %q", instanceID)
	}
	dir, err := Dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, instanceID+".json"), nil
}

// Write atomically writes e for instanceID (mkdir 0700, temp file 0600 +
// rename). Returns an error on a bad instanceID or an I/O failure; callers in
// the hook log-and-exit-0 on any error (fail-closed, never break the session).
func Write(instanceID string, e Entry) error {
	path, err := Path(instanceID)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("sessionhandoff: mkdir %s: %w", dir, err)
	}
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("sessionhandoff: marshal: %w", err)
	}
	tmp, err := os.CreateTemp(dir, instanceID+".*.tmp")
	if err != nil {
		return fmt.Errorf("sessionhandoff: temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sessionhandoff: chmod: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sessionhandoff: write: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("sessionhandoff: close: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("sessionhandoff: rename: %w", err)
	}
	return nil
}

// Read returns the entry for instanceID. ok=false when the file is absent,
// unreadable, corrupt, or carries no stable id, or when instanceID is unsafe —
// every "can't use it" case collapses to a silent miss so the adapter just
// falls through to today's no-recovery behavior.
func Read(instanceID string) (Entry, bool) {
	path, err := Path(instanceID)
	if err != nil {
		return Entry{}, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Entry{}, false
	}
	var e Entry
	if err := json.Unmarshal(data, &e); err != nil {
		return Entry{}, false
	}
	if e.StableSessionID == "" {
		return Entry{}, false
	}
	return e, true
}

// PruneStale deletes handoff entries older than maxAge (by UnixNano) relative
// to now. Best-effort: unreadable/corrupt entries are skipped (they'll be
// overwritten by the next hook for that instance). Returns the count deleted.
// The broker calls this on start to bound the directory.
func PruneStale(maxAge time.Duration, now time.Time) int {
	dir, err := Dir()
	if err != nil {
		return 0
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	cutoff := now.Add(-maxAge).UnixNano()
	deleted := 0
	for _, de := range ents {
		if de.IsDir() {
			continue
		}
		name := de.Name()
		if filepath.Ext(name) != ".json" {
			continue
		}
		full := filepath.Join(dir, name)
		data, err := os.ReadFile(full)
		if err != nil {
			continue
		}
		var e Entry
		if err := json.Unmarshal(data, &e); err != nil {
			continue
		}
		if e.UnixNano < cutoff {
			if os.Remove(full) == nil {
				deleted++
			}
		}
	}
	return deleted
}
