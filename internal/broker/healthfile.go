package broker

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/Andrometiq/c3/internal/version"
)

// healthFileEntry is the per-channel shape nested under "channels" in
// health.json for the Claude Code status line to read. JSON-tagged for the
// bash reader.
type healthFileEntry struct {
	State     string `json:"state"` // "up" | "down"
	SinceUnix int64  `json:"since_unix,omitempty"`
	SinceHHMM string `json:"since_hhmm,omitempty"` // ev.Since.Format("15:04"), local time
	Reason    string `json:"reason,omitempty"`
	Consec    int    `json:"consec,omitempty"`
}

// healthFile is the top-level wrapper written to health.json. It carries broker
// liveness alongside the per-channel snapshot so a reader can detect a DEAD
// broker (frozen file) — not just a DOWN channel:
//
//   - BrokerPID is os.Getpid(); a reader does kill(pid,0) (or equivalent) and
//     treats "pid not alive" as broker-down/unknown regardless of channel state.
//   - WrittenUnix is refreshed on EVERY write (edges + the slow refresh ticker),
//     so a reader treats now-WrittenUnix > 2x the refresh interval (>90s) as a
//     stale/dead broker even when the pid happens to be reused.
//
// Previously the top level was a flat map of channel→entry, which froze at its
// last value (usually "up") when the broker process died, showing green while
// C3 was completely dead (audit finding health-observability-2).
type healthFile struct {
	BrokerPID   int                        `json:"broker_pid"`
	WrittenUnix int64                      `json:"written_unix"`
	Channels    map[string]healthFileEntry `json:"channels"`
	// Version is the running broker's build version ("dev" for an uninjected
	// build). Always present so a reader can show which C3 is running.
	Version string `json:"version,omitempty"`
	// UpdateAvailable + LatestVersion are the always-on update notice (R1): set
	// once the ~6h checker finds a newer stable release, so the status line can
	// render "c3 update available — /c3:update". Both omitempty, so a reader on
	// the current version sees a byte-identical file to before this field existed.
	UpdateAvailable bool   `json:"update_available,omitempty"`
	LatestVersion   string `json:"latest_version,omitempty"`
}

// healthRefreshInterval is the slow refresh cadence for the liveness ticker
// (see startHealthRefresh). A reader treats now-written_unix > 2x this (>90s)
// as a stale/dead broker. Kept slow so the always-on write is cheap.
const healthRefreshInterval = 45 * time.Second

// WriteHealthFile atomically writes the current broker-liveness + per-channel
// health snapshot to HealthFilePath() for the status line to read. Best-effort:
// any error is logged and ignored (the status cache + broker log are the
// backstops). broker_pid and written_unix are stamped on EVERY call so a reader
// can detect a frozen/dead broker; written_unix is refreshed by the slow ticker
// (startHealthRefresh) even when no health edge has fired. At startup lastHealth
// is empty, so channels is "{}" — clearing any stale per-channel state from a
// prior crash and reading as "no outage" while still carrying live pid/time.
// Atomic via CreateTemp-in-same-dir + rename; no fsync (best-effort).
//
// Concurrency: lastHealthSnapshot() reads under the broker's healthMu; the
// wrapper fields (pid, written_unix) are stack-local per call, so concurrent
// WriteHealthFile calls (edge-driven + ticker-driven) each produce one complete
// generation and the atomic rename makes the last writer win — no shared
// mutable state is introduced.
func (b *Broker) WriteHealthFile() {
	snap := b.lastHealthSnapshot()
	channels := make(map[string]healthFileEntry, len(snap))
	for ch, ev := range snap {
		channels[ch] = healthFileEntry{
			State:     string(ev.State),
			SinceUnix: ev.Since.Unix(),
			SinceHHMM: ev.Since.Format("15:04"),
			Reason:    ev.Reason,
			Consec:    ev.Consec,
		}
	}
	updateAvail, latest := b.UpdateAvailability()
	out := healthFile{
		BrokerPID:       os.Getpid(),
		WrittenUnix:     time.Now().Unix(),
		Channels:        channels,
		Version:         version.Current(),
		UpdateAvailable: updateAvail,
		LatestVersion:   latest,
	}
	data, err := json.Marshal(out)
	if err != nil {
		log.Printf("health-file: marshal failed: %v", err)
		return
	}
	path := HealthFilePath()
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		log.Printf("health-file: mkdir %s failed: %v", dir, err)
		return
	}
	tmp, err := os.CreateTemp(dir, ".health.*.tmp")
	if err != nil {
		log.Printf("health-file: create temp failed: %v", err)
		return
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		log.Printf("health-file: write temp failed: %v", err)
		return
	}
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		log.Printf("health-file: chmod temp failed: %v", err)
		return
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		log.Printf("health-file: close temp failed: %v", err)
		return
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		log.Printf("health-file: rename failed: %v", err)
	}
}

// StartHealthRefresh launches a background goroutine that re-writes health.json
// every healthRefreshInterval regardless of health edges, so written_unix stays
// current while the broker is alive (and a reader can distinguish a live-but-DOWN
// channel from a DEAD broker whose file froze). The goroutine exits when the
// broker context is cancelled (Shutdown), so it never leaks. Edge-driven
// WriteHealthFile calls and these ticker-driven ones are concurrency-safe (see
// WriteHealthFile). Call once, after the broker is constructed.
func (b *Broker) StartHealthRefresh() {
	go func() {
		t := time.NewTicker(healthRefreshInterval)
		defer t.Stop()
		for {
			select {
			case <-b.ctx.Done():
				return
			case <-t.C:
				// Guard so a panic in WriteHealthFile (negligible surface, but
				// symmetry with the other supervised broker goroutines) can't
				// crash the process or kill the ticker.
				func() {
					defer recoverGoroutine("healthRefresh")
					b.WriteHealthFile()
				}()
			}
		}
	}()
}
