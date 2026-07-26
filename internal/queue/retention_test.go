package queue

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// --- test helpers for the .trash/ retention window ---

func lsTrash(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(QueueDir(), trashDirName))
	if err != nil {
		t.Fatalf("readdir trash: %v", err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

func readTrashInbounds(t *testing.T, name string) []c3types.Inbound {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(QueueDir(), trashDirName, name))
	if err != nil {
		t.Fatalf("read trash %s: %v", name, err)
	}
	var out []c3types.Inbound
	for _, ln := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.TrimSpace(ln) == "" {
			continue
		}
		var in c3types.Inbound
		if err := json.Unmarshal([]byte(ln), &in); err != nil {
			t.Fatalf("unmarshal trash line %q: %v", ln, err)
		}
		out = append(out, in)
	}
	return out
}

func seedTrashFile(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(QueueDir(), trashDirName, name), []byte(content), 0600); err != nil {
		t.Fatalf("seed trash %s: %v", name, err)
	}
}

// retiredJSONL returns the single non-evicted retired .jsonl in trash (the
// runbook's <base>.<stamp>.jsonl), failing if it isn't there.
func retiredJSONL(t *testing.T) string {
	t.Helper()
	for _, n := range lsTrash(t) {
		if strings.HasSuffix(n, ".jsonl") && !strings.HasSuffix(n, ".evicted.jsonl") {
			return n
		}
	}
	t.Fatalf("no retired .jsonl in trash: %v", lsTrash(t))
	return ""
}

// TestRetention_DrainMovesPairToTrash pins the core invariant: a Consume-at-EOF
// drain moves the whole pair into .trash/ (never os.Remove), with the .jsonl
// carrying the full pre-drain history and the .cur carrying the pre-drain cursor
// (the final-batch boundary), under one shared stamp.
func TestRetention_DrainMovesPairToTrash(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		if err := s.Append(rk, msg(i, "m")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if _, err := s.Consume(rk, 2); err != nil { // persists .cur = 2
		t.Fatal(err)
	}
	if _, err := s.Consume(rk, -1); err != nil { // drain the last line → retire
		t.Fatal(err)
	}
	// Live pair gone (the existing delete-on-empty asserts still hold).
	if _, err := os.Stat(filepath.Join(QueueDir(), rk.File()+".jsonl")); !os.IsNotExist(err) {
		t.Fatalf("live jsonl not retired: stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(QueueDir(), rk.File()+".cur")); !os.IsNotExist(err) {
		t.Fatalf("live cur not retired: stat err = %v", err)
	}
	// Exactly one retired pair (no evict snapshot here).
	var tj, tc string
	for _, n := range lsTrash(t) {
		switch {
		case strings.HasSuffix(n, ".evicted.jsonl"):
			t.Fatalf("unexpected evict snapshot %s", n)
		case strings.HasSuffix(n, ".jsonl"):
			tj = n
		case strings.HasSuffix(n, ".cur"):
			tc = n
		}
	}
	if tj == "" || tc == "" {
		t.Fatalf("retired pair missing in trash: %v", lsTrash(t))
	}
	sj, okj := trashStamp(tj)
	sc, okc := trashStamp(tc)
	if !okj || !okc || sj != sc {
		t.Fatalf("retired pair should share one stamp: jsonl=%d(%v) cur=%d(%v)", sj, okj, sc, okc)
	}
	// The .jsonl is the full line history at drain time (all 3 original lines).
	ins := readTrashInbounds(t, tj)
	if len(ins) != 3 || ins[0].MessageID != 1 || ins[2].MessageID != 3 {
		t.Fatalf("retired jsonl = %+v, want all 3 original lines", ins)
	}
	// The .cur is the pre-drain cursor (2) → the final drained batch is lines[2:].
	curData, err := os.ReadFile(filepath.Join(QueueDir(), trashDirName, tc))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(curData)) != "2" {
		t.Fatalf("retired cur = %q, want \"2\"", string(curData))
	}
}

// TestRecovery_FinalBatchRoundTrip encodes runbook step 3 (no live file): move
// BOTH files back → the pre-drain cursor is restored, so only the final drained
// batch (msg 3) replays.
func TestRecovery_FinalBatchRoundTrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	s1, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		if err := s1.Append(rk, msg(i, "m")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s1.Consume(rk, 2); err != nil { // cursor = 2
		t.Fatal(err)
	}
	if _, err := s1.Consume(rk, -1); err != nil { // drain → retire pair
		t.Fatal(err)
	}
	// Runbook step 3: mv .trash/<base>.<stamp>.{jsonl,cur} back to the live paths.
	base := rk.File()
	tr := filepath.Join(dir, trashDirName)
	var tj, tc string
	entries, _ := os.ReadDir(tr)
	for _, e := range entries {
		n := e.Name()
		switch {
		case strings.HasSuffix(n, ".evicted.jsonl"):
		case strings.HasSuffix(n, ".jsonl"):
			tj = n
		case strings.HasSuffix(n, ".cur"):
			tc = n
		}
	}
	if tj == "" || tc == "" {
		t.Fatalf("expected retired pair in trash, got %v", entries)
	}
	if err := os.Rename(filepath.Join(tr, tj), filepath.Join(dir, base+".jsonl")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(tr, tc), filepath.Join(dir, base+".cur")); err != nil {
		t.Fatal(err)
	}
	// Fresh store + recover indexes the restored route.
	s2, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	if n, _ := s2.Pending(rk); n != 1 {
		t.Fatalf("recovered pending = %d, want 1 (final batch only)", n)
	}
	got, err := s2.Consume(rk, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].MessageID != 3 {
		t.Fatalf("recovered consume = %+v, want exactly msg 3", got)
	}
}

// TestRecovery_MergeWithLiveFile encodes runbook step 4 (new messages arrived
// since the drain): concat trash .jsonl + live .jsonl and drop the cursor →
// every message replays; the new msg 4 exactly once, over-delivery of 1–3
// tolerated, loss of any is a failure.
func TestRecovery_MergeWithLiveFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	s1, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		if err := s1.Append(rk, msg(i, "m")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s1.Consume(rk, -1); err != nil { // drain all → retire (no .cur)
		t.Fatal(err)
	}
	if err := s1.Append(rk, msg(4, "m")); err != nil { // live file recreated with msg 4
		t.Fatal(err)
	}
	base := rk.File()
	tr := filepath.Join(dir, trashDirName)
	var tj string
	entries, _ := os.ReadDir(tr)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".jsonl") && !strings.HasSuffix(e.Name(), ".evicted.jsonl") {
			tj = e.Name()
		}
	}
	if tj == "" {
		t.Fatalf("expected retired jsonl in trash, got %v", entries)
	}
	trashData, err := os.ReadFile(filepath.Join(tr, tj))
	if err != nil {
		t.Fatal(err)
	}
	liveData, err := os.ReadFile(filepath.Join(dir, base+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	merged := append(append([]byte{}, trashData...), liveData...)
	tmp := filepath.Join(dir, base+".jsonl.merge")
	if err := os.WriteFile(tmp, merged, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, base+".jsonl")); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(filepath.Join(dir, base+".cur")) // drop cursor → whole merged file replays
	// Fresh store + recover + drain.
	s2, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	got, err := s2.Consume(rk, -1)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]int{}
	for _, m := range got {
		seen[m.MessageID]++
	}
	for id := int64(1); id <= 4; id++ {
		if seen[id] < 1 {
			t.Fatalf("msg %d lost after merge-recovery (seen=%v)", id, seen)
		}
	}
	if seen[4] != 1 {
		t.Fatalf("msg 4 served %d times, want exactly once", seen[4])
	}
}

// TestGCTrash_TTLExpiry: only trash files older than TrashTTL are removed; the
// live queue file is never a GC candidate.
func TestGCTrash_TTLExpiry(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	if err := s.Append(rk, msg(1, "keep")); err != nil { // a live file GC must not touch
		t.Fatal(err)
	}
	now := time.Now()
	expired := now.Add(-TrashTTL - time.Hour).UnixNano()
	fresh := now.Add(-time.Hour).UnixNano()
	seedTrashFile(t, fmt.Sprintf("telegram__-100__none.%d.jsonl", expired), `{"message_id":9}`)
	seedTrashFile(t, fmt.Sprintf("telegram__-100__none.%d.jsonl", fresh), `{"message_id":10}`)

	s.sweepTrash(now)

	names := lsTrash(t)
	if len(names) != 1 {
		t.Fatalf("after TTL sweep trash = %v, want only the fresh file", names)
	}
	if got, _ := trashStamp(names[0]); got != fresh {
		t.Fatalf("survivor stamp = %d, want fresh %d", got, fresh)
	}
	if n, _ := s.Pending(rk); n != 1 {
		t.Fatalf("live pending after GC = %d, want 1 (GC must not touch live files)", n)
	}
	if _, err := os.Stat(filepath.Join(QueueDir(), rk.File()+".jsonl")); err != nil {
		t.Fatalf("live jsonl gone after GC: %v", err)
	}
}

// TestGCTrash_FileCapEviction: over the file cap, oldest-first eviction until
// within cap; the newest snapshots (likeliest recovery targets) survive.
func TestGCTrash_FileCapEviction(t *testing.T) {
	s := newStore(t)
	now := time.Now()
	stamps := make([]int64, 5)
	for i := 0; i < 5; i++ {
		stamps[i] = now.Add(-time.Duration(5-i) * time.Minute).UnixNano() // ascending
		seedTrashFile(t, fmt.Sprintf("telegram__-100__none.%d.jsonl", stamps[i]), `{"message_id":1}`)
	}
	s.sweepTrashCaps(now, TrashTTL, TrashMaxBytes, 3) // cap files to 3

	names := lsTrash(t)
	if len(names) != 3 {
		t.Fatalf("after file-cap sweep trash has %d files, want 3: %v", len(names), names)
	}
	survivors := map[int64]bool{}
	for _, n := range names {
		st, _ := trashStamp(n)
		survivors[st] = true
	}
	for _, old := range stamps[:2] {
		if survivors[old] {
			t.Fatalf("oldest stamp %d survived file-cap eviction", old)
		}
	}
	for _, keep := range stamps[2:] {
		if !survivors[keep] {
			t.Fatalf("newest stamp %d was evicted by file cap", keep)
		}
	}
}

// TestGCTrash_ByteCapEviction: over the byte cap, oldest-first eviction until
// within cap.
func TestGCTrash_ByteCapEviction(t *testing.T) {
	s := newStore(t)
	now := time.Now()
	content := strings.Repeat("x", 50) // 50 bytes each; 5 files = 250 bytes
	stamps := make([]int64, 5)
	for i := 0; i < 5; i++ {
		stamps[i] = now.Add(-time.Duration(5-i) * time.Minute).UnixNano()
		seedTrashFile(t, fmt.Sprintf("telegram__-100__none.%d.jsonl", stamps[i]), content)
	}
	s.sweepTrashCaps(now, TrashTTL, 150, TrashMaxFiles) // cap bytes to 150 → keep newest 3

	names := lsTrash(t)
	var total int64
	survivors := map[int64]bool{}
	for _, n := range names {
		info, err := os.Stat(filepath.Join(QueueDir(), trashDirName, n))
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
		st, _ := trashStamp(n)
		survivors[st] = true
	}
	if total > 150 {
		t.Fatalf("trash total = %d bytes, want <=150 after byte-cap eviction", total)
	}
	for _, old := range stamps[:2] {
		if survivors[old] {
			t.Fatalf("oldest stamp %d survived byte-cap eviction", old)
		}
	}
	for _, keep := range stamps[2:] {
		if !survivors[keep] {
			t.Fatalf("newest stamp %d was evicted by byte cap", keep)
		}
	}
}

// TestRetire_FailTowardReplay mirrors the deletePair-error contract: a non-ENOENT
// rename failure (here a read-only .trash/) makes Consume return an error with the
// live .jsonl intact and Pending unchanged; restoring perms lets the drain succeed
// and retire the pair.
func TestRetire_FailTowardReplay(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("chmod-based permission denial is a no-op for root")
	}
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		if err := s.Append(rk, msg(i, "m")); err != nil {
			t.Fatal(err)
		}
	}
	trash := filepath.Join(QueueDir(), trashDirName)
	if err := os.Chmod(trash, 0500); err != nil { // read-only: rename-in fails EACCES
		t.Fatal(err)
	}
	defer os.Chmod(trash, 0700)

	if _, err := s.Consume(rk, -1); err == nil {
		t.Fatal("Consume that can't retire into a read-only .trash/ should error")
	}
	if _, err := os.Stat(filepath.Join(QueueDir(), rk.File()+".jsonl")); err != nil {
		t.Fatalf("live jsonl must survive a failed retire: %v", err)
	}
	if n, _ := s.Pending(rk); n != 3 {
		t.Fatalf("pending after failed retire = %d, want 3 (fail-toward-replay)", n)
	}

	if err := os.Chmod(trash, 0700); err != nil {
		t.Fatal(err)
	}
	got, err := s.Consume(rk, -1)
	if err != nil {
		t.Fatalf("drain after restoring perms: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("drain after restore returned %d msgs, want 3", len(got))
	}
	if _, err := os.Stat(filepath.Join(QueueDir(), rk.File()+".jsonl")); !os.IsNotExist(err) {
		t.Fatal("live jsonl should be retired after a successful drain")
	}
	_ = retiredJSONL(t) // present in trash
}

// TestRetire_EmptyQueueDrainIsNoop pins the both-renames-ENOENT tolerance: a
// Consume on a route with NO files at all returns ([], nil) and creates nothing
// in .trash/ (today's behavior via deletePair's IsNotExist).
func TestRetire_EmptyQueueDrainIsNoop(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100} // never appended
	got, err := s.Consume(rk, -1)
	if err != nil {
		t.Fatalf("empty-queue drain errored: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty-queue drain returned %d msgs, want 0", len(got))
	}
	if names := lsTrash(t); len(names) != 0 {
		t.Fatalf("empty-queue drain created trash files: %v", names)
	}
}

// TestRetire_CurFirstCrashOrdering simulates a crash between the two renames
// (.cur already moved to trash, .jsonl still live). Because .cur leaves FIRST,
// recovery finds no live cursor → cursor 0 → ALL lines replay (nothing hidden
// behind a stale cursor). This is why the order is load-bearing.
func TestRetire_CurFirstCrashOrdering(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	s1, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		if err := s1.Append(rk, msg(i, "m")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s1.Consume(rk, 1); err != nil { // persists .cur = 1
		t.Fatal(err)
	}
	base := rk.File()
	// Step (1) done, step (2) crashed: move only the live .cur into .trash/.
	stamp := time.Now().UnixNano()
	if err := os.Rename(
		filepath.Join(dir, base+".cur"),
		filepath.Join(dir, trashDirName, fmt.Sprintf("%s.%d.cur", base, stamp)),
	); err != nil {
		t.Fatal(err)
	}
	s2, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	if n, _ := s2.Pending(rk); n != 3 {
		t.Fatalf("intermediate-crash recovery pending = %d, want 3 (full replay, nothing hidden)", n)
	}
	got, _ := s2.Consume(rk, -1)
	if len(got) != 3 || got[0].MessageID != 1 || got[2].MessageID != 3 {
		t.Fatalf("replay = %+v, want msgs 1,2,3", got)
	}
}

// TestSnapshotDropped_EvictSnapshot: an over-cap EvictOverCap writes the dropped
// lines to .trash/<base>.<stamp>.evicted.jsonl before rewriting the live file.
func TestSnapshotDropped_EvictSnapshot(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= MaxMessages+3; i++ {
		if err := s.Append(rk, msg(i, "m")); err != nil {
			t.Fatal(err)
		}
	}
	dropped, err := s.EvictOverCap(rk)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 3 {
		t.Fatalf("dropped = %d, want 3", dropped)
	}
	var snap string
	for _, n := range lsTrash(t) {
		if strings.HasSuffix(n, ".evicted.jsonl") {
			snap = n
		}
	}
	if snap == "" {
		t.Fatalf("no evict snapshot in trash: %v", lsTrash(t))
	}
	ins := readTrashInbounds(t, snap)
	if len(ins) != 3 || ins[0].MessageID != 1 || ins[2].MessageID != 3 {
		t.Fatalf("evict snapshot = %+v, want exactly the dropped msgs 1,2,3", ins)
	}
	// The live queue keeps exactly MaxMessages (existing evict-math unchanged).
	if n, _ := s.Pending(rk); n != MaxMessages {
		t.Fatalf("pending after evict = %d, want %d", n, MaxMessages)
	}
}

// TestFirstFreeStamp_BumpsOnCollision pins the stamp-collision walk directly: on
// a clash it bumps stamp++ until a free stamp; with no clash it returns start.
func TestFirstFreeStamp_BumpsOnCollision(t *testing.T) {
	taken := map[int64]bool{500: true, 501: true, 502: true}
	if got := firstFreeStamp(500, func(s int64) bool { return taken[s] }); got != 503 {
		t.Fatalf("collision walk = %d, want 503", got)
	}
	if got := firstFreeStamp(600, func(s int64) bool { return taken[s] }); got != 600 {
		t.Fatalf("no-collision = %d, want 600 (start)", got)
	}
}

// TestRetire_DoesNotClobberExistingTrash: a retire whose initial stamp would
// collide lands at a distinct name — the pre-existing retained file is untouched.
func TestRetire_DoesNotClobberExistingTrash(t *testing.T) {
	s := newStore(t)
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	base := rk.File()
	preName := fmt.Sprintf("%s.%d.jsonl", base, time.Now().UnixNano())
	seedTrashFile(t, preName, `{"MessageID":777}`)

	for i := int64(1); i <= 2; i++ {
		if err := s.Append(rk, msg(i, "m")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Consume(rk, -1); err != nil {
		t.Fatal(err)
	}
	// Pre-seeded file is intact (not clobbered).
	pre := readTrashInbounds(t, preName)
	if len(pre) != 1 || pre[0].MessageID != 777 {
		t.Fatalf("pre-seeded trash clobbered: %+v", pre)
	}
	// A distinct retired jsonl for the drain exists too.
	count := 0
	for _, n := range lsTrash(t) {
		if strings.HasSuffix(n, ".jsonl") && !strings.HasSuffix(n, ".evicted.jsonl") {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("retained jsonl count = %d, want 2 (pre-seed + drain, no clobber)", count)
	}
}

// TestRecoverOnStartup_FullyConsumedPairRetiredToTrash extends the delete-on-empty
// startup path: a fully-consumed pair found at startup (cursor at EOF, pair not
// yet removed) is RETIRED to .trash/, not hard-deleted.
func TestRecoverOnStartup_FullyConsumedPairRetiredToTrash(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	s1, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	rk := RouteKey{Channel: "telegram", ChatID: -100}
	if err := s1.Append(rk, msg(1, "m")); err != nil {
		t.Fatal(err)
	}
	if err := s1.Append(rk, msg(2, "m")); err != nil {
		t.Fatal(err)
	}
	base := rk.File()
	// Crash where cursor reached EOF but the pair was neither deleted nor retired:
	// write .cur = 2 directly so RecoverOnStartup hits the fully-consumed branch.
	if err := os.WriteFile(filepath.Join(dir, base+".cur"), []byte("2"), 0600); err != nil {
		t.Fatal(err)
	}
	s2, err := NewStore(QueueDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	if n, _ := s2.Pending(rk); n != 0 {
		t.Fatalf("fully-consumed route recovers to %d pending, want 0", n)
	}
	if _, err := os.Stat(filepath.Join(dir, base+".jsonl")); !os.IsNotExist(err) {
		t.Fatal("live jsonl should be retired at startup")
	}
	_ = retiredJSONL(t) // retired, not deleted
}

// TestNewStore_RetentionDisabledWhenTrashBlocked (item G): a stray regular FILE
// named .trash occupies the retention dir's path, so MkdirAll can't create it.
// NewStore must NOT fail (that would take down the PRIMARY durable queue over a
// defense-in-depth subfeature); it runs with retention disabled and drains
// hard-delete the pair instead of retiring it.
func TestNewStore_RetentionDisabledWhenTrashBlocked(t *testing.T) {
	dir := t.TempDir()
	// Occupy the .trash path with a regular file → MkdirAll(.trash) fails (ENOTDIR).
	if err := os.WriteFile(filepath.Join(dir, trashDirName), []byte("stray"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore must succeed (retention disabled), not fail: %v", err)
	}
	if !s.retentionDisabled {
		t.Fatal("retention should be disabled when .trash/ can't be created")
	}

	rk := RouteKey{Channel: "telegram", ChatID: -100}
	for i := int64(1); i <= 3; i++ {
		if err := s.Append(rk, msg(i, "m")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	got, err := s.Consume(rk, -1) // drain → retirePair falls back to a hard delete
	if err != nil {
		t.Fatalf("drain with retention disabled: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("drain returned %d msgs, want 3", len(got))
	}
	// The live pair is hard-deleted (not retired — there's no .trash/ dir to hold it).
	if _, err := os.Stat(filepath.Join(dir, rk.File()+".jsonl")); !os.IsNotExist(err) {
		t.Fatalf("live jsonl should be hard-deleted; stat err = %v", err)
	}
	// The stray .trash FILE is untouched — still a regular file, never a directory.
	fi, err := os.Stat(filepath.Join(dir, trashDirName))
	if err != nil {
		t.Fatalf("stray .trash file vanished: %v", err)
	}
	if fi.IsDir() {
		t.Fatal("the stray .trash regular file must NOT have become a directory")
	}
}

// TestConcurrentRetireAndSweep_NoRace runs two route workers, each draining
// (retire) and GC-sweeping the shared .trash/ concurrently. Under -race it pins
// that concurrent sweeps tolerate ENOENT and never touch live route files. The
// single-owner model holds per route (each goroutine owns exactly one route).
func TestConcurrentRetireAndSweep_NoRace(t *testing.T) {
	s := newStore(t)
	ta, tb := int64(11), int64(22)
	rkA := RouteKey{Channel: "telegram", ChatID: -100, TopicID: &ta}
	rkB := RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tb}
	var wg sync.WaitGroup
	wg.Add(2)
	run := func(rk RouteKey) {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			if err := s.Append(rk, msg(int64(i+1), "m")); err != nil {
				t.Errorf("append: %v", err)
				return
			}
			if _, err := s.Consume(rk, -1); err != nil { // drains + retires
				t.Errorf("consume: %v", err)
				return
			}
			s.sweepTrash(time.Now()) // concurrent GC over the shared .trash/
		}
	}
	go run(rkA)
	go run(rkB)
	wg.Wait()
	// The final Consume of each route drained it — GC never resurrected/hid a live line.
	if n, _ := s.Pending(rkA); n != 0 {
		t.Fatalf("rkA pending = %d, want 0", n)
	}
	if n, _ := s.Pending(rkB); n != 0 {
		t.Fatalf("rkB pending = %d, want 0", n)
	}
}
