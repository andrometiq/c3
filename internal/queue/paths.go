// Package queue is C3's durable, per-route, append-only on-disk inbound queue.
// Every received Telegram inbound is persisted here (one JSONL line per message)
// before its update_id becomes eligible to advance the Telegram offset, so an
// accepted-but-undelivered message is never lost. The store is single-owner: all
// file operations for a route are funneled through that route's RouteWorker
// goroutine in the broker, so it holds no per-file locks.
package queue

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Caps — never silent (the broker logs + sends a Telegram notice on overflow).
const (
	// MaxMessages is the per-route line cap; EvictOverCap drops oldest beyond it.
	MaxMessages = 1000
	// MaxAge is the per-route age cap; EvictOverCap drops lines older than this.
	MaxAge = 14 * 24 * time.Hour
	// MaxRecordBytes bounds ONE record's marshaled size. Append enforces it; it is
	// the only byte bound in the package (the others are count and age), and
	// Append is the single gate every producer passes through — the debounce-merge
	// re-queue, a plugin that replaces the merged text, an STT transcript straight
	// off a subprocess's stdout. None of those is bounded at its own source.
	//
	// It has to sit UNDER two independent ceilings, and it is deliberately an
	// order of magnitude under the lower one:
	//   - 8 MiB, readLines' scanner cap. One record past it makes the WHOLE route
	//     unreadable — Peek fails, and the route reports empty while holding
	//     everything (see pendingStats).
	//   - 4 MiB, ipc.MaxFrameSize. A record past it can never be delivered in a
	//     fetch_queue response, because the response is one frame. (Not imported:
	//     the queue package does not depend on the IPC layer, and a record bound
	//     that tracked the frame cap exactly would leave zero room for the
	//     envelope anyway.)
	// 1 MiB is far above any legitimate record — a Telegram text message is 4096
	// characters, and the largest thing that lands here is an STT transcript of a
	// long voice note, on the order of a few hundred KB.
	MaxRecordBytes = 1 << 20
)

// Retention window (.trash/). A drained/evicted route pair is renamed into
// <QueueDir>/.trash instead of hard-deleted, so any drain — right topic, wrong
// topic, rogue skill, orphaned consume — is recoverable for TrashTTL. GC piggybacks
// on retire/snapshot (throttled) plus one unthrottled sweep at startup, and only
// ever touches files inside .trash/.
const (
	// TrashTTL is how long a retired pair is kept before GC removes it. Held to
	// MaxAge so a wrongly-drained message survives exactly as long as an
	// undelivered one would have — one retention story (≥14 days, held or drained).
	TrashTTL = MaxAge
	// TrashMaxBytes caps total .trash/ bytes; GC evicts oldest-first beyond it.
	TrashMaxBytes = 256 << 20 // 256 MiB
	// TrashMaxFiles caps the .trash/ file count; GC evicts oldest-first beyond it.
	TrashMaxFiles = 8192
	// trashGCInterval throttles the piggybacked GC sweep (a CAS timestamp gates
	// it — no goroutine/ticker). The startup sweep bypasses the throttle.
	trashGCInterval = 10 * time.Minute
	// trashDirName is the retention subdirectory under QueueDir().
	trashDirName = ".trash"
)

// RouteKey identifies one queued route. TopicID nil = DM / no topic.
type RouteKey struct {
	Channel string
	ChatID  int64
	TopicID *int64
}

// File returns the filesystem-safe basename (no extension) for this route:
// "<channel>__<chat_id>__<topic|none>". The store appends ".jsonl"/".cur".
func (rk RouteKey) File() string {
	topic := "none"
	if rk.TopicID != nil {
		topic = fmt.Sprintf("%d", *rk.TopicID)
	}
	return fmt.Sprintf("%s__%d__%s", rk.Channel, rk.ChatID, topic)
}

// QueueDir resolves the queue directory: $C3_QUEUE_DIR (override, tests), else
// $XDG_STATE_HOME/c3/queue, else ~/.local/state/c3/queue. Mirrors the offset
// store's XDG convention so queue files sit beside <channel>-offset.json.
func QueueDir() string {
	if env := os.Getenv("C3_QUEUE_DIR"); env != "" {
		return env
	}
	if x := os.Getenv("XDG_STATE_HOME"); x != "" {
		return filepath.Join(x, "c3", "queue")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "state", "c3", "queue")
}
