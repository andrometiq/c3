package telegram

import (
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// P1-4: the inbox cache lookup matches the full "-<file_id>.oga" suffix (a file_id
// can contain '-'), requires a non-empty file, and honors STT_INBOX_DIR.
func TestInboxCachedVoicePath(t *testing.T) {
	inbox := t.TempDir()
	t.Setenv("STT_INBOX_DIR", inbox)

	fileID := "AwA-CB-xyz" // file_id with embedded '-' to exercise full-suffix match
	good := filepath.Join(inbox, "1699999999999-"+fileID+".oga")
	if err := os.WriteFile(good, []byte("opus"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := inboxCachedVoicePath(fileID); got != good {
		t.Fatalf("cache hit: got %q, want %q", got, good)
	}
	if got := inboxCachedVoicePath("nosuchid"); got != "" {
		t.Fatalf("unknown id returned %q, want empty", got)
	}

	// An empty (incomplete) file must not be served.
	if err := os.WriteFile(filepath.Join(inbox, "1699999999998-EMPTY.oga"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := inboxCachedVoicePath("EMPTY"); got != "" {
		t.Fatalf("empty cached file served %q, want empty", got)
	}

	// A ".oga.part" temp or other suffix must NOT match (never serve a partial).
	if err := os.WriteFile(filepath.Join(inbox, "111-XID.oga.part"), []byte("half"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := inboxCachedVoicePath("XID"); got != "" {
		t.Fatalf("non-.oga suffix matched: %q", got)
	}

	// F1: a DIFFERENT note whose file_id ends in the requested id must NOT match —
	// exact match after the millis, not a "-<id>.oga" suffix (file_id can contain '-').
	if err := os.WriteFile(filepath.Join(inbox, "222-XYZ-abc.oga"), []byte("otheraudio"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := inboxCachedVoicePath("abc"); got != "" {
		t.Fatalf("suffix collision served a different note's audio: %q", got)
	}
	// But the full file_id "XYZ-abc" still matches its own file.
	if got := inboxCachedVoicePath("XYZ-abc"); got != filepath.Join(inbox, "222-XYZ-abc.oga") {
		t.Fatalf("full file_id with '-' should match its own file, got %q", got)
	}
	// A non-millis (non-digit) prefix must NOT match the convention.
	if err := os.WriteFile(filepath.Join(inbox, "notmillis-def.oga"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := inboxCachedVoicePath("def"); got != "" {
		t.Fatalf("non-millis-prefixed file matched: %q", got)
	}
}

// P1-4: a cache hit is served WITHOUT any getFile/network call — the exact
// degraded-network case where the bytes were already on disk but a re-fetch could
// not fit the worker deadline (2026-08-08 incident). The server here 500s on every
// request, so any network touch fails the test.
func TestDownloadAttachment_ServesInboxCacheWithoutNetwork(t *testing.T) {
	var hits int32
	c := getFileChannelHandler(t, func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	inbox := t.TempDir()
	t.Setenv("STT_INBOX_DIR", inbox)

	fileID := "VOICE-abc-123"
	cached := filepath.Join(inbox, "1699999999999-"+fileID+".oga")
	if err := os.WriteFile(cached, []byte("opusbytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := c.DownloadAttachment(fileID)
	if err != nil {
		t.Fatalf("cache-first download errored: %v", err)
	}
	if got != cached {
		t.Fatalf("served %q, want cached %q", got, cached)
	}
	if n := atomic.LoadInt32(&hits); n != 0 {
		t.Fatalf("cache hit made %d network request(s); must serve from disk without getFile", n)
	}
}
