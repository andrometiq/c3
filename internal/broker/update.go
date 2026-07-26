package broker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/updater"
	"github.com/Andrometiq/c3/internal/version"
)

const (
	// updateCheckInterval is the always-on availability-check cadence. GitHub's
	// anonymous API limit (60/hr/IP) is far above this; the check is a single
	// small GET and its result is cached until the next tick.
	updateCheckInterval = 6 * time.Hour
	// updateInitialDelay staggers the first check off the boot path so a slow
	// network probe never delays the broker coming up.
	updateInitialDelay = 30 * time.Second
	// updateCheckTimeout bounds one availability check.
	updateCheckTimeout = 25 * time.Second
	// updateInstallTimeout bounds a full self-update (download + verify + swap).
	updateInstallTimeout = 12 * time.Minute
)

// setUpdateAvailable records that a newer stable release exists. Idempotent —
// re-called on every check that still sees the update.
func (b *Broker) setUpdateAvailable(latest string) {
	b.updateMu.Lock()
	defer b.updateMu.Unlock()
	b.updateAvailable = true
	b.latestVersion = latest
}

// UpdateAvailability reports whether a newer stable release has been detected and
// its version. Read by WriteHealthFile (status-line notice) and `c3-broker
// status`. Safe on a nil broker (returns false) so health writes never panic.
func (b *Broker) UpdateAvailability() (bool, string) {
	if b == nil {
		return false, ""
	}
	b.updateMu.RLock()
	defer b.updateMu.RUnlock()
	return b.updateAvailable, b.latestVersion
}

// StartUpdateChecker launches the always-on update-availability checker: one
// check after a short delay, then every updateCheckInterval, until the broker
// context is cancelled. On finding a newer STABLE release it (1) records the
// availability — surfaced by the status line via health.json and logged (R1,
// toggle-independent) — and (2) ONLY when mappings.auto_update is enabled,
// performs the self-update and requests a graceful restart via requestShutdown.
//
// A dev build (no injected version) disables the checker entirely: there is no
// release identity to compare against, so it would either nag forever or update
// to something it can't reason about. requestShutdown must trigger the daemon's
// normal drain-and-exit (main.go sends itself SIGTERM). Call once, after the
// broker is constructed.
func (b *Broker) StartUpdateChecker(requestShutdown func()) {
	if version.IsDev() {
		log.Printf("update: dev build (no release version) — auto-update checks disabled")
		return
	}
	go func() {
		defer recoverGoroutine("updateChecker")
		select {
		case <-b.ctx.Done():
			return
		case <-time.After(updateInitialDelay):
		}
		b.runUpdateCheck(requestShutdown)
		t := time.NewTicker(updateCheckInterval)
		defer t.Stop()
		for {
			select {
			case <-b.ctx.Done():
				return
			case <-t.C:
				b.runUpdateCheck(requestShutdown)
			}
		}
	}()
}

// runUpdateCheck performs one availability check and, when auto_update is on and
// an update exists, drives the self-update. All failures are non-fatal and
// logged (a failed check / network blip is NEVER surfaced to the user).
func (b *Broker) runUpdateCheck(requestShutdown func()) {
	ctx, cancel := context.WithTimeout(b.ctx, updateCheckTimeout)
	defer cancel()

	res, err := updater.CheckOnly(ctx, version.Current(), updater.DefaultClient())
	if err != nil {
		// Network failure or API hiccup — debug-level; never user-facing.
		log.Printf("update: check failed (non-fatal): %v", err)
		return
	}
	if !res.UpdateAvailable {
		return
	}

	// R1: always-on notice, independent of the auto_update toggle.
	b.setUpdateAvailable(res.LatestVersion)
	log.Printf("update: c3 %s available (running %s) — run /c3:update", res.LatestVersion, version.Current())
	b.WriteHealthFile() // push update_available/latest_version into the status-line file

	// R3: self-install only when opted in.
	if !b.Mappings().AutoUpdateEnabled() {
		return
	}
	b.performAutoUpdate(requestShutdown)
}

// performAutoUpdate runs the verify-then-swap self-update, and on success
// notifies sessions/topics once and requests a graceful restart. On failure the
// binaries are left untouched (updater guarantees this) and the broker keeps
// running the old version; the notice stays up so the user can update manually.
//
// The install gets its OWN, longer-timeout context derived from b.ctx (NOT the
// check's short context) so a slow download isn't cut off — while a broker
// shutdown still cancels it cleanly.
func (b *Broker) performAutoUpdate(requestShutdown func()) {
	log.Printf("update: auto_update ON — installing latest release now")
	ictx, cancel := context.WithTimeout(b.ctx, updateInstallTimeout)
	defer cancel()

	res, err := updater.Update(ictx, updater.Options{
		CurrentVersion: version.Current(),
		Client:         updater.DefaultClient(),
	})
	if err != nil {
		log.Printf("update: auto-update FAILED (binaries untouched): %v", err)
		return
	}
	if !res.Installed {
		log.Printf("update: auto-update no-op (installed=false latest=%s)", res.LatestVersion)
		return
	}

	// Binaries swapped on disk. The running process still holds the OLD inode
	// (safe on Linux — see updater.ExecutableDir). Announce loudly, notify once,
	// then drain and exit so an adapter reconnect spawns the NEW broker.
	log.Printf("update: INSTALLED c3 %s (was %s) — draining and restarting; adapters reconnect and auto-spawn the new broker",
		res.LatestVersion, res.CurrentVersion)
	b.notifyUpdateRestart(res.LatestVersion)
	if requestShutdown != nil {
		requestShutdown()
	}
}

// notifyUpdateRestart posts a one-shot "updated, restarting" advisory to every
// live CLI session (the proven broker-originated system-event path) AND to each
// distinct attached Telegram topic (best-effort). Called while the channel is
// still alive, before the drain begins.
func (b *Broker) notifyUpdateRestart(newVersion string) {
	msg := fmt.Sprintf("c3 updated to %s — broker restarting, sessions reconnect automatically.", newVersion)
	// (1) CLI sessions — reuse the trusted, broker-originated system-event path.
	b.broadcastSystemEvent(&c3types.SystemEvent{
		Source:  "c3",
		Level:   "info",
		Title:   "C3 updated",
		Message: msg,
	})
	// (2) Telegram topics — one push per distinct attached route.
	b.notifyAttachedTopics(msg)
}

// notifyAttachedTopics best-effort posts text to each distinct route currently
// claimed by a session, using the same broker-originated SendReply path as the
// attach welcome. De-duplicated by route key; per-route failures are logged and
// skipped, never fatal (we are on our way out anyway).
func (b *Broker) notifyAttachedTopics(text string) {
	seen := map[RouteKey]bool{}
	for _, e := range b.Routes.Snapshot() {
		if seen[e.Key] {
			continue
		}
		seen[e.Key] = true
		ch, err := b.Channel(e.Key.Channel)
		if err != nil {
			continue
		}
		var topicID *int64
		if e.Key.HasTopic {
			t := e.Key.TopicID
			topicID = &t
		}
		if _, err := ch.SendReply(c3types.ReplyArgs{
			Channel: e.Key.Channel,
			ChatID:  e.Key.ChatID,
			TopicID: topicID,
			Text:    text,
		}); err != nil {
			log.Printf("update-notify: send to %s failed: %v", routeKeyStr(e.Key), err)
		}
	}
}
