package telegram

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// offsetStore persists the highest update_id we've handed off to the
// broker's per-route workers. On restart, the pollLoop seeds its `offset`
// from this so Telegram doesn't redeliver updates we already accepted.
//
// Note on the durability boundary: we persist after dispatch (host.Emit),
// not after the worker forwards to a CLI. Worst case on a crash mid-flush:
// we lose the messages currently sitting in worker queues but Telegram
// won't redeliver them either. That's the same exposure as today;
// persisting offset here only fixes the "broker restart re-runs the last
// 24h of updates" footgun. A stronger guarantee (per-update completion
// tracking) is the predecessor bot's bot-update-tracker design — bigger
// change, deferred.
//
// File: $XDG_STATE_HOME/c3/<channel>-offset.json (mode 0600).
type offsetStore struct {
	path string
	mu   sync.Mutex
}

// newOffsetStore returns a store keyed by channel name. The directory is
// created on first use.
func newOffsetStore(channelName string) (*offsetStore, error) {
	dir := xdgStateHomeC3()
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("offsetStore: mkdir %s: %w", dir, err)
	}
	return &offsetStore{
		path: filepath.Join(dir, fmt.Sprintf("%s-offset.json", channelName)),
	}, nil
}

type offsetRecord struct {
	HighestCompletedUpdateID int64 `json:"highest_completed_update_id"`
}

// Load reads the persisted offset; returns 0 (and no error) if the file
// doesn't exist yet.
func (s *offsetStore) Load() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("offsetStore: read %s: %w", s.path, err)
	}
	var rec offsetRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return 0, fmt.Errorf("offsetStore: parse %s: %w", s.path, err)
	}
	return rec.HighestCompletedUpdateID, nil
}

// Save atomically writes offset to disk. Idempotent; safe across restarts.
func (s *offsetStore) Save(offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(offsetRecord{HighestCompletedUpdateID: offset})
	if err != nil {
		return fmt.Errorf("offsetStore: marshal: %w", err)
	}
	tmp := s.path + ".tmp"
	// fsync the data file then the parent directory so a crash between
	// rename and journal flush can't leave a zero-byte offset file (it
	// would silently re-process the last 24h of updates on next start).
	// (daemon.md §5.1)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("offsetStore: open %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("offsetStore: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("offsetStore: fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("offsetStore: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("offsetStore: rename %s → %s: %w", tmp, s.path, err)
	}
	// Windows disallows fsync on a directory handle (ERROR_ACCESS_DENIED). This
	// is REQUIRED, not durability polish: a returned error here fails Save, so
	// the offset never persists and every restart re-processes ~24h of updates.
	// The data file was already fsync'd and the rename succeeded.
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		return fmt.Errorf("offsetStore: open dir for fsync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("offsetStore: fsync dir: %w", err)
	}
	return nil
}

// xdgStateHomeC3 returns $XDG_STATE_HOME/c3 (or ~/.local/state/c3 fallback).
func xdgStateHomeC3() string {
	if x := os.Getenv("XDG_STATE_HOME"); x != "" {
		return filepath.Join(x, "c3")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "state", "c3")
}
