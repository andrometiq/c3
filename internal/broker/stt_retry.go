package broker

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// P0-2 — auto-retry STT after a network-transient fetch failure.
//
// The 2026-08-08 cascade: the broker started before Wi-Fi was up, an STT download
// failed with "Network is unreachable", and the voice note was delivered as a
// failure notice — even though the broker OBSERVED the network return minutes later
// ("connected as @<bot>"). Nothing retried; the human had to notice and re-ask.
//
// Now: a voice note whose STT FETCH failed on a recognized-transient condition is
// PARKED (parkSTTRetry, from the flushInbounds voice path). When the Telegram
// fetch-health machine transitions DOWN→UP — and, independently, on a periodic
// sweep (sttRetrySweeper) so STT-only failures and post-edge parks are still caught —
// the broker re-attempts each parked note in a globally BOUNDED pool (runSTTRetry,
// OFF the route workers so a retry never blocks a route for the STT window), then
// hands only the fast append/deliver to the route worker (JobDeliverTranscript) for
// the P0-1 late-delivery path. Fail-closed classification (only known-transient
// signatures retry), bounded, deduped, and attempt-capped so a permanent failure can
// never become a retry loop.

const (
	// maxSTTRetries bounds the parked-retry list (drop-oldest beyond it).
	maxSTTRetries = 32
	// maxSTTRetryAttempts caps re-attempts per parked note, so a note that keeps
	// failing transiently across several recovery edges eventually stops.
	maxSTTRetryAttempts = 2
	// sttRetryConcurrency bounds concurrent STT re-runs across ALL routes so a network
	// recovery cannot launch a transcription herd (Codex review 1, F5).
	sttRetryConcurrency = 2
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
		"temporary failure in name resolution", // DNS not up yet (glibc)
		"name or service not known",            // DNS (glibc, other form)
		"nodename nor servname provided",       // DNS (macOS/BSD)
		"no such host",                         // Go resolver
		"no route to host",
		"connection refused",
		"connection reset",
		"timed out",
		"timeout",
		"dial tcp",            // Go dial failure
		"bad gateway",         // HTTP 502 (proxy/CDN blip)
		"service unavailable", // HTTP 503
		"gateway timeout",     // HTTP 504
		"temporarily unavailable",
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

// onChannelRecovered re-attempts the parked STT retries for a channel whose fetch
// health just recovered (DOWN→UP), from BrokerHost.NotifyHealth on the UP edge.
func (b *Broker) onChannelRecovered(channel string) { b.drainSTTRetries(channel) }

// sttRetrySweepInterval is the edge-INDEPENDENT retry cadence (Codex review 1, F2):
// an STT fetch failure does not itself drive Telegram fetch-health DOWN→UP, so a note
// parked while health stayed UP — or parked just after a recovery edge was processed —
// would otherwise never retry. The sweeper re-attempts parked notes periodically
// regardless of edges. A var so tests can shorten it.
var sttRetrySweepInterval = 2 * time.Minute

// sttRetrySweeper periodically re-attempts parked STT retries, independent of the
// fetch-health recovery edge. Started once at broker New; exits on broker ctx cancel.
func (b *Broker) sttRetrySweeper() {
	defer recoverGoroutine("broker.sttRetrySweeper")
	t := time.NewTicker(sttRetrySweepInterval)
	defer t.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-t.C:
			b.drainSTTRetries("") // all channels
		}
	}
}

// drainSTTRetries takes the parked entries (all when channel=="", else that
// channel's) and re-attempts each in the bounded retry pool. It must NOT run inline
// on a hot goroutine (the health-notify goroutine, or the sweeper): runSTTRetry
// blocks on the concurrency semaphore and the STT chain, so each entry gets its own
// goroutine that gates on sttRetrySem.
func (b *Broker) drainSTTRetries(channel string) {
	b.sttRetryMu.Lock()
	var pending, keep []sttRetryEntry
	for _, e := range b.sttRetries {
		if channel == "" || e.route.Channel == channel {
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
	if channel == "" {
		log.Printf("stt-retry: periodic sweep re-attempting %d parked voice note(s)", len(pending))
	} else {
		log.Printf("stt-retry: %s fetch recovered — re-attempting %d parked voice note(s)", channel, len(pending))
	}
	for _, e := range pending {
		go b.runSTTRetry(e)
	}
}

// runSTTRetry re-attempts ONE parked voice note. The blocking STT runs HERE, in the
// bounded retry pool (NOT on a route worker), so a retry never blocks the route for
// the STT window; only the fast append/deliver is handed back to the route worker.
// Concurrency across all routes is capped by sttRetrySem, so a recovery can never
// launch a transcription herd (Codex review 1, F5). Re-parks (attempt-capped) only on
// a still-transient failure; gives up on a permanent refusal or a provider failure
// (re-running would not help).
func (b *Broker) runSTTRetry(e sttRetryEntry) {
	defer recoverGoroutine("broker.runSTTRetry")
	select {
	case b.sttRetrySem <- struct{}{}:
		defer func() { <-b.sttRetrySem }()
	case <-b.ctx.Done():
		return
	}
	if b.Plugins == nil {
		return
	}
	// If the audio is cached locally, skip the network preflight entirely (offline
	// retry). Otherwise probe: a transient refusal re-parks (a flap mid-retry), a
	// permanent one (too-big / expired / gone) gives up (F4).
	var size int64
	if !b.voiceCachedLocally(e.route.Channel, e.fileID) {
		refusal, sz, retryable := b.attachmentFetchRefusal(e.route.Channel, e.fileID)
		if refusal != "" {
			if retryable {
				b.reparkSTTRetry(e)
			} else {
				log.Printf("stt-retry chan=%s msg=%d file_id=%s: server refuses the file (permanent) — giving up: %s",
					e.route.Channel, e.messageID, e.fileID, refusal)
			}
			return
		}
		size = sz
	}
	var topicID *int64
	if e.route.HasTopic {
		t := e.route.TopicID
		topicID = &t
	}
	sttCtx, cancel := context.WithTimeout(b.ctx, sttFlushTimeout)
	transcript := b.Plugins.FireOnVoiceReceived(sttCtx, c3types.VoicePayload{
		Channel:   e.route.Channel,
		ChatID:    e.route.ChatID,
		TopicID:   topicID,
		MessageID: e.messageID,
		FileID:    e.fileID,
		Size:      size,
	})
	cancel()
	if detail, ok := sttFetchFailure(transcript); ok {
		if isNetworkTransient(detail) {
			b.reparkSTTRetry(e)
		} else {
			log.Printf("stt-retry chan=%s msg=%d file_id=%s: fetch failed non-transiently — giving up: %s",
				e.route.Channel, e.messageID, e.fileID, detail)
		}
		return
	}
	if transcript == "" || isSTTFailureMarker(transcript) {
		log.Printf("stt-retry chan=%s msg=%d file_id=%s: download recovered but transcription still failed — not re-parking (network is not the problem)",
			e.route.Channel, e.messageID, e.fileID)
		return
	}
	// Success → hand the fast append/deliver to the route's single-owner worker.
	resultCh := make(chan DeliverTranscriptResult, 1)
	if !b.Workers.Submit(e.route, Job{Kind: JobDeliverTranscript, DeliverTranscript: &DeliverTranscriptJob{
		MessageID: e.messageID, FileID: e.fileID, Transcript: transcript, ResultCh: resultCh,
	}}) {
		log.Printf("stt-retry chan=%s msg=%d file_id=%s: transcribed but delivery worker unavailable — re-parking",
			e.route.Channel, e.messageID, e.fileID)
		b.reparkSTTRetry(e)
		return
	}
	select {
	case res := <-resultCh:
		if res.Delivered {
			log.Printf("stt-retry chan=%s msg=%d file_id=%s: transcribed after recovery (%d chars) and delivered",
				e.route.Channel, e.messageID, e.fileID, len(transcript))
		} else {
			log.Printf("stt-retry chan=%s msg=%d file_id=%s: transcribed but delivery did not land (%v) — re-parking",
				e.route.Channel, e.messageID, e.fileID, res.Err)
			b.reparkSTTRetry(e)
		}
	case <-time.After(workerJobTimeout):
		log.Printf("stt-retry chan=%s msg=%d file_id=%s: delivery worker did not respond in %s — re-parking",
			e.route.Channel, e.messageID, e.fileID, workerJobTimeout)
		b.reparkSTTRetry(e)
	case <-b.ctx.Done():
		return
	}
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
