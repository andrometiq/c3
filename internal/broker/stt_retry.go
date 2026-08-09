package broker

import (
	"log"
	"strings"
)

// P0-2 — auto-retry STT after a network-transient fetch failure.
//
// The 2026-08-08 cascade: the broker started before Wi-Fi was up, an STT download
// failed with "Network is unreachable", and the voice note was delivered as a
// failure notice — even though the broker OBSERVED the network return minutes later
// ("connected as @<bot>"). Nothing retried; the human had to notice and re-ask.
//
// Now: a voice note whose STT FETCH failed on a recognized-transient condition is
// PARKED (parkSTTRetry, from the flushInbounds voice path), and when the Telegram
// fetch-health machine transitions DOWN→UP the broker re-attempts each parked note
// (onChannelRecovered → JobRetrySTT on the route's own worker) and delivers a late
// success through the same P0-1 late-delivery path. Fail-closed on classification
// (only known-transient signatures retry), bounded, deduped, and attempt-capped so
// a permanent failure can never become a retry loop.

const (
	// maxSTTRetries bounds the parked-retry list (drop-oldest beyond it).
	maxSTTRetries = 32
	// maxSTTRetryAttempts caps re-attempts per parked note, so a note that keeps
	// failing transiently across several recovery edges eventually stops.
	maxSTTRetryAttempts = 2
)

// sttRetryEntry is one parked STT retry.
type sttRetryEntry struct {
	route     RouteKey
	messageID int64
	fileID    string
	attempts  int
}

// isNetworkTransient reports whether a fetch-error string looks like a TRANSIENT
// network condition worth auto-retrying once connectivity returns. FAIL-CLOSED
// allowlist: only recognized-transient signatures return true, so a permanent
// failure (bad/expired file_id, too-big, a provider error) is never retry-looped.
// Applied ONLY to fetch failures (download), never to provider/transcription
// failures — a download that timed out IS a network condition.
func isNetworkTransient(s string) bool {
	l := strings.ToLower(s)
	for _, sig := range []string{
		"network is unreachable",               // errno 101 (the 2026-08-08 incident)
		"temporary failure in name resolution", // DNS not up yet
		"no route to host",
		"connection refused",
		"connection reset",
		"timed out",
		"timeout",
		"dial tcp", // Go dial failure
		"no such host",
	} {
		if strings.Contains(l, sig) {
			return true
		}
	}
	return false
}

// parkSTTRetry records a network-transient STT failure for auto-retry on the next
// fetch-health recovery. Deduped by (route, messageID, fileID); bounded (drops the
// oldest past maxSTTRetries). A no-op with an empty fileID (nothing to re-fetch).
func (b *Broker) parkSTTRetry(route RouteKey, messageID int64, fileID string) {
	if fileID == "" {
		return
	}
	b.sttRetryMu.Lock()
	defer b.sttRetryMu.Unlock()
	for i := range b.sttRetries {
		if b.sttRetries[i].route == route && b.sttRetries[i].messageID == messageID && b.sttRetries[i].fileID == fileID {
			return // already parked
		}
	}
	if len(b.sttRetries) >= maxSTTRetries {
		b.sttRetries = b.sttRetries[1:] // drop oldest
	}
	b.sttRetries = append(b.sttRetries, sttRetryEntry{route: route, messageID: messageID, fileID: fileID})
	log.Printf("stt-retry parked chan=%s chat=%d topic=%s msg=%d file_id=%s — will retry when Telegram fetch recovers",
		route.Channel, route.ChatID, TopicKeyStr(route), messageID, fileID)
}

// onChannelRecovered re-attempts every parked STT retry for a channel whose fetch
// health just recovered (DOWN→UP). It takes the entries for that channel, then off
// the health-notify goroutine submits a JobRetrySTT to each route's own worker (so
// the blocking STT re-run and the delivery stay single-owner per route). Called
// from BrokerHost.NotifyHealth on the UP edge.
func (b *Broker) onChannelRecovered(channel string) {
	b.sttRetryMu.Lock()
	var pending, keep []sttRetryEntry
	for _, e := range b.sttRetries {
		if e.route.Channel == channel {
			pending = append(pending, e)
		} else {
			keep = append(keep, e)
		}
	}
	b.sttRetries = keep
	b.sttRetryMu.Unlock()
	if len(pending) == 0 {
		return
	}
	log.Printf("stt-retry: %s fetch recovered — re-attempting %d parked voice note(s)", channel, len(pending))
	go func() {
		defer recoverGoroutine("broker.onChannelRecovered")
		for _, e := range pending {
			if !b.Workers.Submit(e.route, Job{Kind: JobRetrySTT, RetrySTT: &RetrySTTJob{
				MessageID: e.messageID, FileID: e.fileID, Attempts: e.attempts,
			}}) {
				log.Printf("stt-retry chan=%s msg=%d file_id=%s: worker unavailable, re-parking",
					e.route.Channel, e.messageID, e.fileID)
				b.reparkSTTRetry(e)
			}
		}
	}()
}

// reparkSTTRetry re-parks an entry after a failed retry attempt, incrementing its
// attempt count and dropping it once the cap is reached (so a note that keeps
// failing transiently across recovery edges eventually stops). Bounded like park.
func (b *Broker) reparkSTTRetry(e sttRetryEntry) {
	if e.attempts+1 >= maxSTTRetryAttempts {
		log.Printf("stt-retry chan=%s msg=%d file_id=%s: gave up after %d attempt(s)",
			e.route.Channel, e.messageID, e.fileID, e.attempts+1)
		return
	}
	b.sttRetryMu.Lock()
	defer b.sttRetryMu.Unlock()
	for i := range b.sttRetries {
		if b.sttRetries[i].route == e.route && b.sttRetries[i].messageID == e.messageID && b.sttRetries[i].fileID == e.fileID {
			return // already re-parked
		}
	}
	if len(b.sttRetries) >= maxSTTRetries {
		b.sttRetries = b.sttRetries[1:]
	}
	b.sttRetries = append(b.sttRetries, sttRetryEntry{route: e.route, messageID: e.messageID, fileID: e.fileID, attempts: e.attempts + 1})
	log.Printf("stt-retry chan=%s msg=%d file_id=%s: re-parked (attempt %d)",
		e.route.Channel, e.messageID, e.fileID, e.attempts+1)
}
