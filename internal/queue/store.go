package queue

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// Status is a per-route snapshot for /status and /queue. Pending is lines after
// the cursor; OldestUnix and NewestUnix are the MIN and MAX timestamps over the
// PENDING lines (0 when empty). They are NOT head/tail positional (B5): a
// drained-in line carries its original timestamp and lands at the tail, so a
// positional "oldest = head" would misreport — MIN/MAX read correctly regardless
// of file position.
type Status struct {
	Pending    int
	OldestUnix int64
	NewestUnix int64
}

// Store owns the queue directory. It is single-owner per route (the broker
// funnels every route's file ops through that route's RouteWorker goroutine), so
// the file ops hold no per-file locks. Only the cheap cross-route status index
// is mutex-guarded — it touches no files.
type Store struct {
	dir string

	mu sync.Mutex // guards idx ONLY (the cross-route status counters)
	// idx is keyed by the canonical RouteKey.File() string, NOT by RouteKey:
	// RouteKey.TopicID is a *int64 and queueRouteKey mints a FRESH pointer per
	// call, so two RouteKeys for the same logical route are DISTINCT Go map keys.
	// Keying by File() collapses that per-call pointer churn into exactly one
	// canonical entry per route — so the count is deterministic, drain clears it,
	// and StatusFor/StatusAll stay file-free / race-free (I7).
	idx map[string]Status

	// lastTrashGC is the UnixNano of the last gcTrash sweep. It throttles the
	// GC piggybacked on retirePair/snapshotDropped to once per trashGCInterval
	// via a lock-free CAS — no goroutine, ticker, or shutdown lifecycle. The
	// startup sweep bypasses it.
	lastTrashGC atomic.Int64

	// retentionDisabled is set when the .trash/ retention dir could not be
	// created at construction (item G). Retention is defense-in-depth on TOP of
	// the primary durable queue, so its subdir failing must NOT take down the
	// whole queue: the store runs with retirePair hard-deleting (deletePair) and
	// snapshotDropped / gcTrash no-op'ing. Read-only after NewStore, so no lock.
	retentionDisabled bool
}

// NewStore creates the queue dir (0700) and returns a Store. Call
// RecoverOnStartup once after construction to rebuild the index from disk.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("queue: mkdir %s: %w", dir, err)
	}
	s := &Store{dir: dir, idx: map[string]Status{}}
	if err := os.MkdirAll(filepath.Join(dir, trashDirName), 0700); err != nil {
		// Item G: the .trash/ retention window is defense-in-depth on top of the
		// primary durable queue. Its subdir failing to create (e.g. a stray FILE
		// named .trash occupies the path) must NOT disable the whole queue — that
		// would drop the PRIMARY loss protection because a secondary subfeature
		// failed. Fail toward keeping the queue: run with retention DISABLED
		// (retirePair hard-deletes, snapshotDropped + gcTrash no-op). Loud log so
		// the degraded mode is visible in broker.log.
		log.Printf("queue: WARNING could not create retention dir %s: %v — running with .trash retention DISABLED; drains hard-delete instead of retaining for TrashTTL", filepath.Join(dir, trashDirName), err)
		s.retentionDisabled = true
	}
	return s, nil
}

func (s *Store) jsonlPath(rk RouteKey) string { return filepath.Join(s.dir, rk.File()+".jsonl") }
func (s *Store) curPath(rk RouteKey) string   { return filepath.Join(s.dir, rk.File()+".cur") }

// Append writes one JSON line and fsyncs it (data + parent dir), then refreshes
// the status index. The caller (worker) only treats the source update_id as
// offset-eligible AFTER this returns nil.
func (s *Store) Append(rk RouteKey, in *c3types.Inbound) error {
	data, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("queue: marshal: %w", err)
	}
	f, err := os.OpenFile(s.jsonlPath(rk), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("queue: open append: %w", err)
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		_ = f.Close()
		return fmt.Errorf("queue: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("queue: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("queue: close: %w", err)
	}
	// fsync the parent dir so a freshly O_CREATE'd .jsonl's directory entry is
	// durable across a power-loss crash — otherwise a crash right after Append
	// returns nil could lose the message the broker already treated as persisted
	// (and whose update_id it has since let the Telegram offset advance past).
	if err := s.syncDir(); err != nil {
		return err
	}
	s.refreshIndex(rk)
	return nil
}

// syncDir fsyncs the queue directory so newly-created or renamed entries (new
// .jsonl on first Append, renamed .cur/.jsonl on writeCursor/rewrite) are
// durable across a crash. Mirrors offset_store.go's dir-fsync.
func (s *Store) syncDir() error {
	// Windows disallows fsync on a directory handle (FlushFileBuffers returns
	// ERROR_ACCESS_DENIED). Skipping it here is REQUIRED for correctness, not
	// just durability polish: Append() propagates this error, the poll loop
	// treats it as "not persisted" and never advances the Telegram offset —
	// Telegram then redelivers the same updates forever (infinite re-queue +
	// re-notify loop, the Windows spam). Do not "optimize" this guard away.
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("queue: open dir for fsync: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("queue: fsync dir: %w", err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("queue: close dir: %w", err)
	}
	return nil
}

// readLines returns the parsed inbound lines and the cursor. Corrupt lines are
// skipped (logged via the returned skipped count is implicit — caller logs).
func (s *Store) readLines(rk RouteKey) (lines []c3types.Inbound, cursor int, err error) {
	f, err := os.Open(s.jsonlPath(rk))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("queue: open read: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			lines = append(lines, c3types.Inbound{}) // keep line-number alignment
			continue
		}
		var in c3types.Inbound
		if jerr := json.Unmarshal([]byte(line), &in); jerr != nil {
			// Corrupt line: keep a placeholder so the cursor's line-number stays
			// aligned with the file, but mark it skippable via a zero MessageID
			// caller filters. We tag it with a sentinel channel so Peek/Consume
			// can drop it.
			lines = append(lines, c3types.Inbound{Channel: corruptSentinel})
			continue
		}
		lines = append(lines, in)
	}
	if serr := sc.Err(); serr != nil {
		return nil, 0, fmt.Errorf("queue: scan: %w", serr)
	}
	cursor = s.readCursor(rk)
	return lines, cursor, nil
}

const corruptSentinel = "\x00corrupt"

// pendingFrom returns the non-corrupt lines after the cursor.
func pendingFrom(lines []c3types.Inbound, cursor int) []c3types.Inbound {
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(lines) {
		cursor = len(lines)
	}
	out := make([]c3types.Inbound, 0, len(lines)-cursor)
	for _, in := range lines[cursor:] {
		if in.Channel == corruptSentinel {
			continue
		}
		out = append(out, in)
	}
	return out
}

// Peek returns up to n oldest pending messages without advancing the cursor.
func (s *Store) Peek(rk RouteKey, n int) ([]c3types.Inbound, error) {
	lines, cursor, err := s.readLines(rk)
	if err != nil {
		return nil, err
	}
	pending := pendingFrom(lines, cursor)
	if n >= 0 && n < len(pending) {
		pending = pending[:n]
	}
	return pending, nil
}

// Consume returns up to n oldest pending messages AND advances the cursor past
// them (corrupt placeholder lines are stepped over too). Retires the pair into
// .trash/ (recoverable for TrashTTL) when the cursor reaches EOF.
func (s *Store) Consume(rk RouteKey, n int) ([]c3types.Inbound, error) {
	lines, cursor, err := s.readLines(rk)
	if err != nil {
		return nil, err
	}
	// n < 0 is the "consume all" sentinel; clamp the capacity hint so a negative
	// n never reaches make() as a cap (which would panic "makeslice: cap out of
	// range"). For "all", size the hint to the worst-case pending count.
	cap0 := n
	if cap0 < 0 {
		cap0 = len(lines) - cursor
	}
	if cap0 < 0 {
		cap0 = 0
	}
	out := make([]c3types.Inbound, 0, cap0)
	pos := cursor
	for pos < len(lines) && (n < 0 || len(out) < n) {
		in := lines[pos]
		pos++
		if in.Channel == corruptSentinel {
			continue // step over corrupt lines, don't return them
		}
		out = append(out, in)
	}
	// Advance the cursor to pos (after the last consumed/skipped line).
	if pos >= len(lines) {
		if err := s.retirePair(rk); err != nil {
			return nil, err
		}
	} else if err := s.writeCursor(rk, pos); err != nil {
		return nil, err
	}
	s.refreshIndex(rk)
	return out, nil
}

// Pending returns the count of pending messages and the oldest (MIN) pending
// timestamp (zero time when empty).
func (s *Store) Pending(rk RouteKey) (int, time.Time) {
	n, oldest, _ := s.pendingStats(rk)
	return n, oldest
}

// pendingStats returns the pending count plus the MIN and MAX pending timestamps
// (both zero time when empty) from ONE readLines scan — the shared source for
// Pending (MIN) and refreshIndex (MIN + MAX). Ages are MIN/MAX over the pending
// set, NOT head/tail positional (B5), so a drained-in line whose original
// timestamp lands at the tail is still reflected. Zero-timestamp lines (synthetic
// / unset) are ignored so a stray one never drags MIN to the epoch — mirroring
// oldestSuffix's Unix<=0 guard.
func (s *Store) pendingStats(rk RouteKey) (n int, oldest, newest time.Time) {
	lines, cursor, err := s.readLines(rk)
	if err != nil {
		return 0, time.Time{}, time.Time{}
	}
	pending := pendingFrom(lines, cursor)
	for _, in := range pending {
		if in.Timestamp.IsZero() {
			continue
		}
		if oldest.IsZero() || in.Timestamp.Before(oldest) {
			oldest = in.Timestamp
		}
		if newest.IsZero() || in.Timestamp.After(newest) {
			newest = in.Timestamp
		}
	}
	return len(pending), oldest, newest
}

// StatusFor returns the single-route snapshot from the in-memory status index
// (Pending + OldestUnix). Cheap; touches NO files — so a per-topic /status read
// is race-free against the route worker's concurrent Append/Consume/rewrite (it
// reads the same mutex-guarded counters StatusAll uses, not the .jsonl/.cur off
// the worker goroutine). A route with nothing queued returns the zero Status
// (Pending 0). See I7.
//
// RouteKey's TopicID is a *int64, so two RouteKeys for the same route can carry
// distinct pointers and would NOT be equal as Go map keys. The index is therefore
// keyed by the canonical File() basename (what the rest of the store and the
// on-disk layout key on), so a single keyed lookup matches by VALUE identity
// regardless of which pointer the caller's RouteKey carries.
func (s *Store) StatusFor(rk RouteKey) Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.idx[rk.File()]
}

// StatusAll returns a snapshot of the in-memory status index (all known routes
// with pending > 0). Cheap; touches no files.
func (s *Store) StatusAll() map[RouteKey]Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[RouteKey]Status, len(s.idx))
	for fileBase, v := range s.idx {
		if v.Pending > 0 {
			if rk, ok := parseRouteFile(fileBase); ok {
				out[rk] = v
			}
		}
	}
	return out
}

// EvictOverCap drops the oldest lines exceeding MaxMessages OR older than MaxAge
// (a cap-only rewrite), adjusting the cursor by the number dropped. Returns the
// count dropped (0 when under cap). Never silent — the broker logs + sends a
// Telegram notice when dropped > 0.
func (s *Store) EvictOverCap(rk RouteKey) (int, error) {
	lines, cursor, err := s.readLines(rk)
	if err != nil || len(lines) == 0 {
		return 0, err
	}
	// rewrite() strips corrupt placeholders from the file, so the cap/age/cursor
	// math must run on the corrupt-free real-line view — otherwise corrupt lines
	// inflate the count (over-evicting) and desync the rewritten file's length
	// from newCursor (risking double-serve or skip). Project lines→real and map
	// the cursor into the same corrupt-free coordinate space.
	real := make([]c3types.Inbound, 0, len(lines))
	cursorReal := 0 // cursor mapped into the corrupt-free index space
	for i, in := range lines {
		if i < cursor && in.Channel != corruptSentinel {
			cursorReal++
		}
		if in.Channel == corruptSentinel {
			continue
		}
		real = append(real, in)
	}
	if len(real) == 0 {
		// Only corrupt lines remained; rewrite drops them all. The resulting
		// zero-byte .jsonl carries nothing recoverable, so retirePair plain-removes
		// it rather than cluttering .trash/ with an empty snapshot.
		if err := s.rewrite(rk, real); err != nil {
			return 0, err
		}
		if err := s.retirePair(rk); err != nil {
			return 0, err
		}
		s.refreshIndex(rk)
		return 0, nil
	}
	cutoff := time.Now().Add(-MaxAge)
	// Find how many leading real lines to drop: by age first, then by count.
	drop := 0
	for _, in := range real {
		if !in.Timestamp.IsZero() && in.Timestamp.Before(cutoff) {
			drop++
			continue
		}
		break
	}
	if len(real)-drop > MaxMessages {
		drop += (len(real) - drop) - MaxMessages
	}
	if drop == 0 {
		return 0, nil
	}
	if drop > len(real) {
		drop = len(real)
	}
	kept := real[drop:]
	// Snapshot the dropped lines into .trash/ BEFORE the live rewrite discards
	// them, so a cap/age eviction stays recoverable for TrashTTL — UNLESS retention
	// is disabled (the .trash/ dir couldn't be created at NewStore), in which case
	// snapshotDropped no-ops and the dropped lines are hard-deleted by the rewrite.
	// A crash between the snapshot and the rewrite leaves the lines in both places —
	// a harmless duplicate GC'd later; a snapshot failure returns before the
	// rewrite, so the live queue is untouched (fail-toward-keeping).
	if err := s.snapshotDropped(rk, real[:drop]); err != nil {
		return 0, err
	}
	if err := s.rewrite(rk, kept); err != nil {
		return 0, err
	}
	newCursor := cursorReal - drop
	if newCursor < 0 {
		newCursor = 0
	}
	if newCursor >= len(kept) {
		if err := s.retirePair(rk); err != nil {
			return 0, err
		}
	} else if err := s.writeCursor(rk, newCursor); err != nil {
		return 0, err
	}
	s.refreshIndex(rk)
	return drop, nil
}

// RefreshText rewrites, in place, the Text of the still-pending queued line whose
// Inbound.MessageID == messageID. Used by retranscribe to replace a stored STT-
// failure placeholder with a corrected transcript so a later fetch_queue returns
// the fixed text. Only lines AT/AFTER the cursor (not yet consumed) are eligible —
// an already-consumed or never-queued message_id is a clean no-op. Returns whether
// a line was refreshed.
//
// It reuses the same cap-safe atomic rewrite path as EvictOverCap (temp file →
// fsync → rename → dir-fsync), so the on-disk update is crash-safe. Like that
// path, rewrite() strips corrupt placeholder lines, so the cursor is remapped into
// the corrupt-free coordinate space to stay aligned with the rewritten file.
func (s *Store) RefreshText(rk RouteKey, messageID int64, newText string) (bool, error) {
	if messageID == 0 {
		return false, nil // unidentifiable; never refresh
	}
	lines, cursor, err := s.readLines(rk)
	if err != nil || len(lines) == 0 {
		return false, err
	}
	// Project lines→real (corrupt-free) and map the cursor into that index space,
	// mirroring EvictOverCap so a rewrite that drops corrupt lines doesn't desync
	// the cursor from the rewritten file.
	real := make([]c3types.Inbound, 0, len(lines))
	cursorReal := 0
	for i, in := range lines {
		if in.Channel == corruptSentinel {
			continue
		}
		if i < cursor {
			cursorReal++
		}
		real = append(real, in)
	}
	// Find the matching message among the still-pending (cursor-onward) real lines.
	found := false
	for idx := cursorReal; idx < len(real); idx++ {
		// B7: drained-in copies keep frozen text; organic STT refresh (matched by
		// per-chat MessageID) must never hit a moved line that collides on id.
		if real[idx].DrainedFrom != "" {
			continue
		}
		if real[idx].MessageID == messageID {
			real[idx].Text = newText
			found = true
			break
		}
	}
	if !found {
		return false, nil
	}
	if err := s.rewrite(rk, real); err != nil {
		return false, err
	}
	// rewrite() may have dropped corrupt lines that sat before the cursor; persist
	// the remapped cursor so consumed lines stay consumed.
	if cursorReal != cursor {
		if cursorReal >= len(real) {
			if err := s.retirePair(rk); err != nil {
				return false, err
			}
		} else if err := s.writeCursor(rk, cursorReal); err != nil {
			return false, err
		}
	}
	s.refreshIndex(rk)
	return true, nil
}

// RemoveIDs removes, from the PENDING region of the route's file, the lines
// selected by sel — for each MessageID, the set of 1-based OCCURRENCE ORDINALS
// (that id's position among its OWN pending occurrences, oldest→newest in file
// order) to remove — and returns the removed records in file order. It is the
// non-prefix companion to Consume (which can only advance the single cursor
// past an oldest prefix, store.go Consume): a drain of "6-10 leaving 1-5" is a
// mid-range removal Consume cannot express.
//
// Selection is by OCCURRENCE POSITION, not a per-id count (amendment A2,
// hardened): a queue can legitimately hold two lines with one MessageID — an
// edited message re-dispatches with the SAME id — and the caller selects
// LINES, so sel[5]={2} removes ONLY the 2nd id-5 line and keeps the 1st (a
// per-id count budget would always take the FIRST occurrence — a silent
// wrong-line removal when the operator named the later one). Ids absent from
// pending, and ordinals no longer present (a concurrent consume shifted that
// id's occurrences down), match nothing and are simply not in the returned
// slice, so a crash-retry re-issue converges on the surviving intersection
// (idempotent).
//
// It reuses the proven composite (same internals as EvictOverCap/RefreshText):
// readLines + cursor projection → walk pending counting per-id occurrences and
// partition selected vs kept → snapshotDropped the removed lines to .trash/
// (recoverable for TrashTTL, INV-4) BEFORE the atomic rewrite (tmp → fsync →
// rename → dir-fsync) → cursor remap → retirePair when the pending region
// empties → refreshIndex. Only lines AT/AFTER the cursor are eligible; the
// consumed (pre-cursor) region is preserved. Corrupt placeholder lines are
// stepped over — never removed, never counted in the walk. A call that
// matches nothing mutates no file (idempotent no-op).
func (s *Store) RemoveIDs(rk RouteKey, sel map[int64][]int) (removed []c3types.Inbound, err error) {
	if len(sel) == 0 {
		return nil, nil // nothing requested — clean no-op, no file I/O
	}
	lines, cursor, err := s.readLines(rk)
	if err != nil || len(lines) == 0 {
		return nil, err
	}
	// Project lines→real (corrupt-free) and map the cursor into that index space,
	// mirroring RefreshText/EvictOverCap so a rewrite that drops corrupt lines does
	// not desync the cursor from the rewritten file.
	real := make([]c3types.Inbound, 0, len(lines))
	cursorReal := 0
	for i, in := range lines {
		if in.Channel == corruptSentinel {
			continue
		}
		if i < cursor {
			cursorReal++
		}
		real = append(real, in)
	}
	// Project the caller's ordinal lists into per-id ordinal SETS. The caller's
	// frozen selection is never mutated (phase 2 reuses it across a re-issue).
	want := make(map[int64]map[int]bool, len(sel))
	for id, ords := range sel {
		set := make(map[int]bool, len(ords))
		for _, o := range ords {
			set[o] = true
		}
		want[id] = set
	}
	// Walk the still-pending (cursor-onward) real lines in file order, counting
	// each id's occurrences as we go and partitioning into removed (this
	// occurrence's ordinal was selected) vs kept. The consumed region
	// (real[:cursorReal]) is preserved verbatim.
	kept := make([]c3types.Inbound, 0, len(real))
	kept = append(kept, real[:cursorReal]...)
	seen := make(map[int64]int)
	for idx := cursorReal; idx < len(real); idx++ {
		in := real[idx]
		seen[in.MessageID]++
		if want[in.MessageID][seen[in.MessageID]] {
			removed = append(removed, in)
			continue
		}
		kept = append(kept, in)
	}
	if len(removed) == 0 {
		return nil, nil // no pending line matched — mutate nothing (idempotent)
	}
	// Snapshot the removed lines into .trash/ BEFORE the live rewrite discards them,
	// so a wrong-source/wrong-range drain stays recoverable for TrashTTL (INV-4). A
	// snapshot failure returns before the rewrite, so the live queue is untouched
	// (fail-toward-keeping); a crash between snapshot and rewrite leaves the lines in
	// both places — a harmless duplicate GC'd later.
	if err := s.snapshotDropped(rk, removed); err != nil {
		return nil, err
	}
	if err := s.rewrite(rk, kept); err != nil {
		return nil, err
	}
	// The consumed region is unchanged, so the new cursor is cursorReal (the count of
	// consumed real lines). rewrite() stripped any corrupt lines, so the .cur must be
	// persisted into the corrupt-free coordinate space regardless of its old value.
	if cursorReal >= len(kept) {
		if err := s.retirePair(rk); err != nil {
			return nil, err
		}
	} else if err := s.writeCursor(rk, cursorReal); err != nil {
		return nil, err
	}
	s.refreshIndex(rk)
	return removed, nil
}

// RecoverOnStartup scans the queue dir and rebuilds the status index. A route
// whose cursor is at/after its line count has its pair retired into .trash/;
// otherwise the index records the derived pending count. It finishes with one
// unthrottled GC sweep of .trash/ (safe: this runs before the worker pool exists,
// so startup has zero concurrency). The .jsonl-suffix filter skips the .trash
// directory entry, so retained files are never rescanned as routes.
func (s *Store) RecoverOnStartup() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("queue: readdir: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		base := strings.TrimSuffix(name, ".jsonl")
		rk, ok := parseRouteFile(base)
		if !ok {
			continue
		}
		lines, cursor, rerr := s.readLines(rk)
		if rerr != nil {
			continue
		}
		if cursor >= len(lines) {
			_ = s.retirePair(rk)
			continue
		}
		s.refreshIndex(rk)
	}
	now := time.Now()
	s.lastTrashGC.Store(now.UnixNano())
	s.sweepTrash(now)
	return nil
}

// --- cursor + file helpers ---

func (s *Store) readCursor(rk RouteKey) int {
	data, err := os.ReadFile(s.curPath(rk))
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func (s *Store) writeCursor(rk RouteKey, n int) error {
	tmp := s.curPath(rk) + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(n)), 0600); err != nil {
		return fmt.Errorf("queue: write cursor: %w", err)
	}
	if err := os.Rename(tmp, s.curPath(rk)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("queue: rename cursor: %w", err)
	}
	if err := s.syncDir(); err != nil {
		return err
	}
	return nil
}

func (s *Store) deletePair(rk RouteKey) error {
	_ = os.Remove(s.curPath(rk))
	if err := os.Remove(s.jsonlPath(rk)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("queue: remove jsonl: %w", err)
	}
	return nil
}

// --- retention window (.trash/) ---

// trashDir is the retention subdirectory under the queue dir.
func (s *Store) trashDir() string { return filepath.Join(s.dir, trashDirName) }

// Retained-file path builders. A retired pair shares one stamp; evict snapshots
// insert ".evicted". The stamp is a UnixNano, parsed back positionally from the
// end by trashStamp (the base is config-supplied and may contain dots).
func (s *Store) trashJSONL(base string, stamp int64) string {
	return filepath.Join(s.trashDir(), fmt.Sprintf("%s.%d.jsonl", base, stamp))
}
func (s *Store) trashCur(base string, stamp int64) string {
	return filepath.Join(s.trashDir(), fmt.Sprintf("%s.%d.cur", base, stamp))
}
func (s *Store) trashEvicted(base string, stamp int64) string {
	return filepath.Join(s.trashDir(), fmt.Sprintf("%s.%d.evicted.jsonl", base, stamp))
}

func pathExists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

// firstFreeStamp returns the first stamp >= start for which taken(stamp) is
// false. Retirement/snapshot use it to resolve the rare clock-step collision by
// bumping stamp++ (per-route ops are serialized on one worker goroutine, so the
// walk is short and bounded).
func firstFreeStamp(start int64, taken func(int64) bool) int64 {
	stamp := start
	for taken(stamp) {
		stamp++
	}
	return stamp
}

// retirePair moves a route's queue pair into .trash/ instead of hard-deleting it,
// so any drain (right topic, wrong topic, rogue skill, orphaned consume) stays
// recoverable for TrashTTL. Same directory subtree ⇒ same filesystem ⇒ rename(2)
// is atomic (EXDEV impossible).
//
// A zero-byte .jsonl (the all-corrupt rewrite, or an evict that dropped every
// line — already snapshotted) carries nothing recoverable, so it is plain-removed
// like deletePair rather than trashed.
//
// Ordering is load-bearing: the .cur is renamed FIRST. The only non-loss-free
// intermediate is "live .cur, no .jsonl" — a later Append would recreate a fresh
// .jsonl whose lines sit behind the stale cursor and go invisible — so the cursor
// must leave before the history (deletePair removed .cur before .jsonl for the
// same reason). ENOENT is tolerated on BOTH renames: a never-partially-consumed
// route has no .cur, and an empty-queue drain (no .jsonl at all) must stay a
// no-op. Only a NON-ENOENT rename failure is an error, and it returns before
// touching .jsonl — so Consume fails exactly like today's deletePair-error path
// (batch not returned, live queue intact, fail-toward-replay).
//
// The post-rename dir fsyncs (step 3) are durability polish only, and by the time
// they run BOTH renames already succeeded — the pair is in .trash/ and the live
// queue is gone. Returning their error would make Consume report failure with the
// live queue already empty, violating the live-queue-intact-on-error contract, so
// they LOG-and-continue (item F). Only the PRE-rename errors keep the strict
// fail-toward-replay semantics above.
//
// When retention is disabled (the .trash/ dir couldn't be created — item G), the
// whole path degrades to a hard delete (deletePair), same fail-toward-replay
// error semantics as today's delete-on-empty.
func (s *Store) retirePair(rk RouteKey) error {
	if s.retentionDisabled {
		return s.deletePair(rk) // retention off: hard-delete, no .trash/ to retire into.
	}
	if fi, err := os.Stat(s.jsonlPath(rk)); err == nil && fi.Size() == 0 {
		return s.deletePair(rk)
	}
	base := rk.File()
	stamp := firstFreeStamp(time.Now().UnixNano(), func(st int64) bool {
		return pathExists(s.trashJSONL(base, st)) || pathExists(s.trashCur(base, st))
	})
	// (1) .cur first (ENOENT tolerated).
	if err := os.Rename(s.curPath(rk), s.trashCur(base, stamp)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("queue: retire cur: %w", err)
	}
	// (2) then .jsonl (ENOENT tolerated).
	if err := os.Rename(s.jsonlPath(rk), s.trashJSONL(base, stamp)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("queue: retire jsonl: %w", err)
	}
	// (3) durability polish: fsync the queue dir (entries removed) and .trash/
	// (entries added). Correctness holds without these (either rename state is
	// loss-free); they harden against power loss. Both renames already succeeded,
	// so a failure here must NOT be returned — the live queue is already gone and
	// returning would make Consume claim failure with an empty live queue,
	// breaking the fail-toward-replay contract. Log and continue (item F).
	if err := s.syncDir(); err != nil {
		log.Printf("queue: retire %s: post-rename dir fsync failed (durability polish, ignored): %v", base, err)
	}
	if err := fsyncDir(s.trashDir()); err != nil {
		log.Printf("queue: retire %s: post-rename .trash fsync failed (durability polish, ignored): %v", base, err)
	}
	s.gcTrash(time.Now())
	return nil
}

// snapshotDropped atomically writes the lines EvictOverCap is about to drop to
// .trash/<base>.<stamp>.evicted.jsonl (tmp → fsync → rename inside .trash/, the
// same crash-safe pattern as rewrite) BEFORE the live rewrite discards them.
// Corrupt placeholder lines carry no recoverable content and are skipped; an
// empty (or all-corrupt) dropped set is a no-op.
func (s *Store) snapshotDropped(rk RouteKey, dropped []c3types.Inbound) error {
	if s.retentionDisabled {
		return nil // retention off (item G): no .trash/ to snapshot into; EvictOverCap drops the lines.
	}
	real := make([]c3types.Inbound, 0, len(dropped))
	for _, in := range dropped {
		if in.Channel == corruptSentinel {
			continue
		}
		real = append(real, in)
	}
	if len(real) == 0 {
		return nil
	}
	base := rk.File()
	stamp := firstFreeStamp(time.Now().UnixNano(), func(st int64) bool {
		return pathExists(s.trashEvicted(base, st))
	})
	final := s.trashEvicted(base, stamp)
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("queue: open evict snapshot: %w", err)
	}
	w := bufio.NewWriter(f)
	for _, in := range real {
		data, merr := json.Marshal(in)
		if merr != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("queue: marshal evict snapshot: %w", merr)
		}
		_, _ = w.Write(append(data, '\n'))
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("queue: flush evict snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("queue: fsync evict snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("queue: close evict snapshot: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("queue: rename evict snapshot: %w", err)
	}
	if err := fsyncDir(s.trashDir()); err != nil {
		return err
	}
	s.gcTrash(time.Now())
	return nil
}

// fsyncDir fsyncs a directory so newly-created or renamed entries in it are
// durable across a crash. Mirrors syncDir (which fsyncs the queue dir).
func fsyncDir(path string) error {
	// Windows disallows fsync on a directory handle; skip it there (see syncDir).
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("queue: open dir for fsync: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("queue: fsync dir: %w", err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("queue: close dir: %w", err)
	}
	return nil
}

// trashStamp extracts the UnixNano stamp encoded in a .trash/ filename, or
// (0, false) when it doesn't parse — the caller then falls back to ModTime, so
// foreign junk (including a crashed .tmp) still ages out of the window.
func trashStamp(name string) (int64, bool) {
	s := name
	switch {
	case strings.HasSuffix(s, ".evicted.jsonl"):
		s = strings.TrimSuffix(s, ".evicted.jsonl")
	case strings.HasSuffix(s, ".jsonl"):
		s = strings.TrimSuffix(s, ".jsonl")
	case strings.HasSuffix(s, ".cur"):
		s = strings.TrimSuffix(s, ".cur")
	default:
		return 0, false
	}
	dot := strings.LastIndexByte(s, '.')
	if dot < 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(s[dot+1:], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// gcTrash runs the .trash/ sweep, throttled to once per trashGCInterval via a
// lock-free CAS on lastTrashGC. Piggybacked on retirePair/snapshotDropped — the
// only points where trash grows — so no goroutine/ticker exists. A fresh store
// (lastTrashGC == 0) runs immediately.
func (s *Store) gcTrash(now time.Time) {
	if s.retentionDisabled {
		return // retention off (item G): nothing ever lands in .trash/, nothing to GC.
	}
	last := s.lastTrashGC.Load()
	if last != 0 && now.Sub(time.Unix(0, last)) < trashGCInterval {
		return
	}
	if !s.lastTrashGC.CompareAndSwap(last, now.UnixNano()) {
		return // another goroutine just claimed the sweep — let it run
	}
	s.sweepTrash(now)
}

// sweepTrash is the unthrottled GC body against the production caps.
func (s *Store) sweepTrash(now time.Time) {
	s.sweepTrashCaps(now, TrashTTL, TrashMaxBytes, TrashMaxFiles)
}

// sweepTrashCaps TTL-expires then cap-enforces .trash/, evicting oldest-first.
// It ONLY ever ReadDirs and Removes inside .trash/ — live queue files are never
// candidates. Retained files are written exactly once (atomic rename-in) and
// never modified, so concurrent sweeps from two route workers are safe: an
// already-removed file yields a tolerated ENOENT. A fresh retire can't be
// TTL-selected (fresh stamp) and cap-eviction is oldest-first, so the newest
// snapshots (the likeliest recovery targets) survive. The caps are parameterized
// so tests can exercise eviction without materializing 256 MiB; production always
// passes the consts via sweepTrash.
func (s *Store) sweepTrashCaps(now time.Time, ttl time.Duration, maxBytes int64, maxFiles int) {
	entries, err := os.ReadDir(s.trashDir())
	if err != nil {
		return // trash dir missing/unreadable — nothing to GC
	}
	type tf struct {
		name   string
		ageRef int64 // UnixNano age reference (filename stamp, else ModTime)
		size   int64
	}
	files := make([]tf, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, ierr := e.Info()
		if ierr != nil {
			continue
		}
		ageRef, ok := trashStamp(e.Name())
		if !ok {
			ageRef = info.ModTime().UnixNano()
		}
		files = append(files, tf{name: e.Name(), ageRef: ageRef, size: info.Size()})
	}
	// 1) TTL expiry.
	cutoff := now.Add(-ttl).UnixNano()
	kept := files[:0]
	for _, f := range files {
		if f.ageRef < cutoff {
			_ = os.Remove(filepath.Join(s.trashDir(), f.name))
			continue
		}
		kept = append(kept, f)
	}
	files = kept
	// 2) Cap enforcement — oldest-first (ascending age reference).
	var total int64
	for _, f := range files {
		total += f.size
	}
	if len(files) <= maxFiles && total <= maxBytes {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].ageRef < files[j].ageRef })
	evicted := 0
	i := 0
	for i < len(files) && (len(files)-i > maxFiles || total > maxBytes) {
		f := files[i]
		if err := os.Remove(filepath.Join(s.trashDir(), f.name)); err == nil || os.IsNotExist(err) {
			total -= f.size
			evicted++
		}
		i++
	}
	if evicted > 0 {
		// Deliberately logged: cap eviction shortens the promised retention window
		// for the oldest retained pairs (they were still within TTL), so it must be
		// visible in broker.log. (The retire/NewStore degradation logs in items F/G
		// are the other deliberate broker.log lines in this package.)
		log.Printf("queue: trash GC evicted %d sub-TTL retained file(s) to stay within caps (limits: %d files / %d bytes)", evicted, maxFiles, maxBytes)
	}
}

// rewrite atomically replaces the jsonl with the given lines (the cap valve
// EvictOverCap, the STT-fix RefreshText, and the drain primitive RemoveIDs).
func (s *Store) rewrite(rk RouteKey, lines []c3types.Inbound) error {
	tmp := s.jsonlPath(rk) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("queue: open rewrite: %w", err)
	}
	w := bufio.NewWriter(f)
	for _, in := range lines {
		if in.Channel == corruptSentinel {
			continue
		}
		data, merr := json.Marshal(in)
		if merr != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("queue: marshal rewrite: %w", merr)
		}
		_, _ = w.Write(append(data, '\n'))
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("queue: flush rewrite: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("queue: fsync rewrite: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("queue: close rewrite: %w", err)
	}
	if err := os.Rename(tmp, s.jsonlPath(rk)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("queue: rename rewrite: %w", err)
	}
	if err := s.syncDir(); err != nil {
		return err
	}
	return nil
}

// refreshIndex recomputes the cheap status counters for one route (Pending +
// MIN/MAX pending ages). Every mutation path funnels through here (Append,
// Consume, EvictOverCap, RefreshText, RemoveIDs, RecoverOnStartup) and this is
// the ONLY writer of the index, so a full MIN/MAX recompute here keeps NewestUnix
// correct everywhere with no extra rescan — the pendingStats scan replaces the
// Pending scan this already did.
func (s *Store) refreshIndex(rk RouteKey) {
	n, oldest, newest := s.pendingStats(rk)
	key := rk.File()
	s.mu.Lock()
	defer s.mu.Unlock()
	if n == 0 {
		delete(s.idx, key)
		return
	}
	s.idx[key] = Status{Pending: n, OldestUnix: oldest.Unix(), NewestUnix: newest.Unix()}
}

// parseRouteFile reverses RouteKey.File(): "<channel>__<chat>__<topic|none>".
func parseRouteFile(base string) (RouteKey, bool) {
	parts := strings.Split(base, "__")
	if len(parts) != 3 {
		return RouteKey{}, false
	}
	chat, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return RouteKey{}, false
	}
	rk := RouteKey{Channel: parts[0], ChatID: chat}
	if parts[2] != "none" {
		tid, terr := strconv.ParseInt(parts[2], 10, 64)
		if terr != nil {
			return RouteKey{}, false
		}
		rk.TopicID = &tid
	}
	return rk, true
}
