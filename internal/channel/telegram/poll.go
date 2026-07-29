package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// updateProbe captures ONLY the rich_message raw JSON that gotgbot rc.34 drops
// during typed unmarshal (it has no RichMessage field). We unmarshal the raw
// getUpdates response a second time into this minimal shape and pair it with the
// typed updates by array index.
type updateProbe struct {
	Message       *messageProbe `json:"message"`
	EditedMessage *messageProbe `json:"edited_message"`
}

type messageProbe struct {
	RichMessage json.RawMessage `json:"rich_message"`
}

// parseUpdates unmarshals a raw getUpdates result array into BOTH the typed
// gotgbot updates (the existing downstream path, byte-identical to what
// Bot.GetUpdates produced) and the rich-message probes (same array, same order).
// Pure — unit-tested without network.
func parseUpdates(raw []byte) ([]gotgbot.Update, []updateProbe, error) {
	var updates []gotgbot.Update
	if err := json.Unmarshal(raw, &updates); err != nil {
		return nil, nil, err
	}
	var probes []updateProbe
	// Best-effort: if the probe unmarshal somehow fails, proceed with no rich
	// data rather than dropping the whole batch.
	_ = json.Unmarshal(raw, &probes)
	return updates, probes, nil
}

// richRawFor returns the rich_message raw JSON for an update's message (or edited
// message), or nil if absent.
func richRawFor(p updateProbe) json.RawMessage {
	if p.Message != nil && len(p.Message.RichMessage) > 0 {
		return p.Message.RichMessage
	}
	if p.EditedMessage != nil && len(p.EditedMessage.RichMessage) > 0 {
		return p.EditedMessage.RichMessage
	}
	return nil
}

// allowedUpdates is the explicit opt-in list for getUpdates. Telegram's
// allowed_updates is a RE-LISTING contract: ONLY the listed types are delivered,
// and an omitted type is silently dropped server-side. P4 adds "poll" so the
// aggregate poll-result update is delivered (the prior build omitted it — the
// exact mechanism that made poll-reading dead). It is added IN LOCKSTEP with the
// dispatchUpdate case below; never subscribe a type without its handler, or
// updates buffer only to be dropped at the default case.
//
// Note: we deliberately do NOT list "poll_answer". Per Q-RESULT-1 the agent
// receives AGGREGATE tallies only (final-on-close + stop_poll), never per-voter
// identities, so subscribing per-voter answers would only invite spam we drop.
// callback_query + message_reaction were already subscribed; P4 gives them real
// dispatch handling.
var allowedUpdates = []string{"message", "edited_message", "callback_query", "message_reaction", "poll"}

// pollIdleBackoff paces the poll loop's NO-PROGRESS re-poll. When a long-poll
// returns a non-empty batch in which every update was a dedup-skip of an un-acked
// in-flight update still at the frontier (offset = Committed()+1), Telegram keeps
// re-returning that same update on every call until its message is durably
// persisted (a slow voice → STT inbound can hold the frontier for 12–110s). The
// offset cannot — and must not — advance past an un-persisted update (that is the
// loss-freedom property), so without pacing the loop would re-poll instantly at
// ~3 getUpdates/sec for the whole persist window. We sleep this long between such
// no-progress polls instead. It never delays a genuinely-new update: a new update
// IS dispatched, so the loop does not pace that iteration.
const pollIdleBackoff = 1 * time.Second

// pollLoop runs Bot.GetUpdates in a loop, converting each Message into an
// Inbound and emitting via host.Emit. Honors c.ctx cancellation.
//
// Error handling (per prior-art parity, see DEBUGGING.md):
//   - 409 Conflict: another getUpdates is active for this token. Drive
//     fetch-health (so a PERSISTENT conflict still raises the out-of-band DOWN
//     alert), back off with an escalating delay, and RETRY — do NOT exit. A 409
//     is usually transient (a client-side long-poll timeout racing the server's
//     still-open prior poll) and self-heals on the next success; the broker
//     singleton already prevents a second LOCAL broker. Exiting would turn a
//     few-second blip into a dead bot needing a manual kill+restart.
//   - 401/403 Permanent: increment authBreaker. After auth401Threshold
//     consecutive 401s, the breaker trips: sleep long (5min) between
//     attempts so a revoked-token retry-storm can't get the bot deleted.
//   - 429 Rate-limited: honor parameters.retry_after (capped at 60s) before
//     the next attempt. Don't compound onto the backoff timer.
//   - Transient (network/timeout/5xx): exponential backoff 1s → 30s, reset
//     on success.
//   - Any non-classified: treated as transient (better to retry than crash).
//
// Long-poll budget is sized via timeoutFor("getUpdates", ...) — see
// resilience.go. Don't shorten it past the server-side Timeout or we'll
// re-introduce the 2026-05-09 bug.
func (c *Channel) pollLoop() {
	const (
		longPoll            = longPollTimeoutSeconds
		maxRetryAfter       = 60 * time.Second
		trippedSleep        = 5 * time.Minute
		baseBackoff         = time.Second
		maxBackoff          = 30 * time.Second
		offsetSaveFailAlert = 5 // consecutive Save failures before one loud advisory
	)

	// consecTransient counts consecutive TRANSIENT GetUpdates failures (the
	// IP-block signature). When it reaches endpointFailoverThreshold AND more
	// than one endpoint is configured, we advance activeEndpoint to the next
	// base and reset the counter (P2 endpoint failover). It is reset on the
	// first success and on any non-transient outcome — failover is for the
	// "this base is unreachable" case ONLY, never for 401/403 (follow you
	// everywhere) or 409 (conflict). This composes with the Phase-1 fetch-health
	// machine: the health edge still fires; failover just also rotates the base.
	var consecTransient int

	// consecConflict counts consecutive 409 conflicts (diagnostic, surfaced in
	// the log so a one-off transient 409 is distinguishable from a persistent
	// second poller). Reset on the first success and on any non-conflict outcome.
	var consecConflict int

	// consecSaveFail counts consecutive offset-persistence failures so a
	// sustained failure escalates from a quiet per-line log to ONE loud advisory
	// (at offsetSaveFailAlert): a silently-failing Save re-floods the CLI with
	// re-delivered updates on the next restart, which dedup can't cover for long.
	var consecSaveFail int

	// Effective 409-conflict backoff bounds. Zero fields ⇒ the production
	// defaults; tests inject millisecond values to exercise the retry path fast.
	conflictBase := c.conflictBackoffBase
	if conflictBase <= 0 {
		conflictBase = defaultConflictBackoffBase
	}
	conflictMax := c.conflictBackoffMax
	if conflictMax <= 0 {
		conflictMax = defaultConflictBackoffMax
	}
	conflictBackoff := conflictBase

	// Offset resumes from the PERSISTED store. A supervised panic-restart
	// re-enters here and re-Loads, so it resumes from the last SAVED offset, not
	// in-memory progress — if Save was failing (see the offset-save advisory
	// below) a restart can re-deliver recent updates; the dedup map bounds that.
	//
	// lastSaved tracks the highest offset we have written to the store, so we
	// only Save on a real advance. With the persisted-offset tracker (offTrk,
	// set in Start), the tracker is the source of truth across a supervised
	// panic-restart (it lives on the Channel, not re-seeded here), so we resume
	// from its committed prefix; the next fetch is Committed()+1.
	var offset int64
	var lastSaved int64
	if c.offTrk != nil {
		lastSaved = c.offTrk.Committed()
		offset = lastSaved + 1
		if lastSaved > 0 {
			c.host.Logf("telegram: resuming from persisted-offset tracker committed=%d (next=%d)", lastSaved, offset)
		}
	} else if c.offsets != nil {
		if loaded, err := c.offsets.Load(); err == nil && loaded > 0 {
			offset = loaded + 1
			lastSaved = loaded
			c.host.Logf("telegram: resuming from persisted offset=%d (next=%d)", loaded, offset)
		} else if err != nil {
			c.host.Logf("telegram: offset Load failed (%v); starting from 0", err)
		}
	}
	backoff := baseBackoff

	// lastInflightRefetchLogged throttles the "re-poll skip" log to once per
	// distinct frontier update_id — see logDedupSkip. Poll-loop-local: the loop is
	// single-goroutine, so no lock is needed.
	var lastInflightRefetchLogged int64

	// Component 2: persist the offset "periodically AND on shutdown". The loop
	// already saves per successful batch (the periodic half); this defer covers the
	// shutdown half — on ANY loop exit (ctx cancelled / return) write the highest
	// durably-committed offset so a final batch persisted just before shutdown is
	// not lost to a restart (which would re-deliver it; dedup bounds it but the
	// save avoids the churn). It is a no-op when nothing advanced since the last
	// Save (saveTo <= lastSaved) or when there is no offset store, so it never
	// double-writes the steady-state per-batch save.
	defer func() {
		if c.offTrk == nil || c.offsets == nil {
			return
		}
		if saveTo := c.offTrk.Committed(); saveTo > lastSaved {
			if err := c.offsets.Save(saveTo); err != nil {
				c.host.Logf("telegram: final offset Save on shutdown failed: %v", err)
			} else {
				c.host.Logf("telegram: persisted offset=%d on shutdown", saveTo)
			}
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// If the auth breaker is tripped, sleep long and retry sparingly.
		// Eventually the user fixes the token; one success clears the breaker.
		if c.authBrk.IsTripped() {
			c.host.Logf("telegram: auth breaker TRIPPED (%d consecutive 401s); sleeping %v before next probe — fix bot_token in mappings.json",
				c.authBrk.Consec(), trippedSleep)
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(trippedSleep):
			}
		}

		params := map[string]any{
			"offset":          offset,
			"timeout":         longPoll,
			"allowed_updates": allowedUpdates,
		}
		raw, err := c.bot.RequestWithContext(c.ctx, "getUpdates", params, c.requestOptsFor("getUpdates"))
		var updates []gotgbot.Update
		var probes []updateProbe
		if err == nil {
			updates, probes, err = parseUpdates(raw)
		}
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// Shutdown. We intentionally do NOT run the conflict-clear below:
				// the channel is stopping and conflictActive is per-Channel state
				// that dies with the goroutines, so a stale true is harmless here.
				return
			}
			class, retryAfter := classifyError(err)
			if class != errClassConflict {
				// A non-conflict outcome breaks any consecutive-409 streak: reset
				// the conflict counter/backoff and clear conflictActive so a 409 is
				// no longer treated as the current inbound blocker (the success path
				// clears these too). This is also what keeps the consecConflict
				// comment honest ("reset on any non-conflict outcome").
				consecConflict = 0
				conflictBackoff = conflictBase
				c.conflictActive.Store(false)
			}
			switch class {
			case errClassConflict:
				// 409 — Telegram reports another getUpdates is active for this
				// token. This is USUALLY transient and self-healing, NOT a reason
				// to give up: after a client-side long-poll timeout (flaky
				// network/proxy), our next getUpdates races the server's
				// still-open prior long-poll and draws a 409 that clears within
				// seconds. The broker singleton (flock + listen socket) already
				// prevents a second LOCAL broker, so the old behavior — exit
				// pollLoop and demand a manual kill+restart — turned a few-second
				// blip into a dead bot only a human could revive (the 2026-06-22
				// fresh-laptop incident). Instead: drive fetch-health (so a
				// PERSISTENT conflict still raises the out-of-band DOWN alert once
				// consecFails crosses downAfterFails), back off with an escalating
				// delay, and RETRY. The first success auto-recovers and resets —
				// no restart, no debugging. A genuine second poller elsewhere just
				// keeps us in slow-retry + DOWN-alerting until it goes away, at
				// which point the next poll succeeds on its own.
				//
				// A 409 follows the token to every endpoint (like 401/403), so it
				// never triggers endpoint failover — reset the transient streak.
				consecTransient = 0
				consecConflict++
				c.conflictActive.Store(true)
				c.reportHealth(c.health.RecordFailure("409 conflict (another getUpdates active)"))
				c.logf("telegram: 409 CONFLICT (consec=%d) — another getUpdates is active for this bot token; backing off %v and retrying. Self-heals once the other poll ends; no kill/restart needed. Error: %v",
					consecConflict, conflictBackoff, err)
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(conflictBackoff):
				}
				conflictBackoff *= 2
				if conflictBackoff > conflictMax {
					conflictBackoff = conflictMax
				}
				continue
			case errClassPermanent:
				// 401/403 — token issue. Trip-on-N pattern. Only a TRIPPED
				// breaker (token revoked / persistently rejected) drives the
				// health machine DOWN — a couple of transient auth blips
				// shouldn't raise the out-of-band alarm.
				// Permanent (401/403) follows you to every endpoint — never a
				// reason to fail over. Reset the transient streak.
				consecTransient = 0
				tripped := c.authBrk.RecordFail()
				c.logf("telegram: GetUpdates permanent error (consec=%d, tripped=%v): %v",
					c.authBrk.Consec(), tripped, err)
				if tripped {
					c.reportHealth(c.health.RecordFailure("token revoked / persistent 401"))
				}
				wait := backoff
				if tripped {
					wait = trippedSleep
				}
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(wait):
				}
				if !tripped {
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
				continue
			case errClassRateLimited:
				// 429 is a healthy, reachable server pushing back — it is
				// explicitly NOT a fetch-health failure. Do NOT call
				// RecordFailure here (the central false-positive guard:
				// "429 is NOT down"). The endpoint is reachable, so it is not a
				// failover trigger either — reset the transient streak.
				consecTransient = 0
				wait := time.Duration(retryAfter) * time.Second
				if wait <= 0 {
					wait = 5 * time.Second
				}
				if wait > maxRetryAfter {
					wait = maxRetryAfter
				}
				c.host.Logf("telegram: GetUpdates 429 rate-limited; honoring retry_after=%ds (capped at %v)",
					retryAfter, maxRetryAfter)
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(wait):
				}
				continue
			case errClassTransient:
				// Transient network/timeout/5xx — the IP-block / unreachable
				// signature. Feed the health machine; it flips DOWN after
				// downAfterFails consecutive transient failures and the edge
				// fans out the out-of-band alert.
				c.logf("telegram: GetUpdates transient error (backoff %v): %v", backoff, err)
				c.reportHealth(c.health.RecordFailure("transient (network/timeout/5xx)"))
				consecTransient++
				c.maybeFailover(&consecTransient)
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			default:
				// Unclassified is treated as transient (see classifyError) —
				// drive the health machine the same way, INCLUDING the failover
				// counter (an unreachable host can surface as an unclassified
				// network error).
				c.logf("telegram: GetUpdates unclassified error (treating as transient, backoff %v): %v", backoff, err)
				c.reportHealth(c.health.RecordFailure("unclassified (treated as transient)"))
				consecTransient++
				c.maybeFailover(&consecTransient)
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(backoff):
				}
				continue
			}
		}
		// Success — clear auth breaker, reset transient backoff, and record a
		// healthy fetch. A GetUpdates that returned without a transport error is
		// HEALTHY even when it carried ZERO updates (the normal quiet-night
		// long-poll timeout) — that's the central false-positive fix: success
		// here, not a failure. The FIRST success after an outage fires the
		// RECOVERED edge.
		c.authBrk.RecordSuccess()
		c.reportHealth(c.health.RecordSuccess())
		backoff = baseBackoff
		// A success after a 409 means the conflict cleared (the racing poll
		// ended / the other instance went away) — reset the conflict backoff and
		// streak so a later, unrelated conflict starts fresh from the base delay,
		// and clear conflictActive so the heartbeat may resume clearing DOWN.
		conflictBackoff = conflictBase
		consecConflict = 0
		c.conflictActive.Store(false)
		// A working endpoint sticks: the first success clears the failover streak
		// so we don't rotate away from a base that just recovered.
		consecTransient = 0

		var advanced bool
		// dispatchedAny tracks whether THIS batch produced any newly-dispatched
		// update. It stays false when the batch was empty or every update was a
		// dedup-skip (the un-acked-frontier spin signature) — that is the signal to
		// pace the next no-progress re-poll (see pollIdleBackoff at loop end).
		var dispatchedAny bool
		for i := range updates {
			u := updates[i]
			// Register the accepted update as in-flight in the persisted-offset
			// tracker BEFORE dispatch. The committed offset advances past it only
			// once its message is durably persisted (the broker's persist callback
			// MarkDone-s it) or it is a no-op outcome (markUpdateDone below). When
			// offTrk is nil (conflict/resilience unit tests), we fall back to the
			// legacy highest-seen in-memory advance.
			if c.offTrk != nil {
				c.offTrk.Register(u.UpdateId)
			}
			if c.dedup != nil && c.dedup.SeenOrAdd(&u) {
				lastInflightRefetchLogged = c.logDedupSkip(u.UpdateId, lastInflightRefetchLogged)
				// A dedup-skip is NOT a persist. With offTrk, this is either a
				// genuine Telegram redelivery of an already-committed update (id
				// <= committed ⇒ Register/MarkDone are no-ops) OR an in-flight
				// update being re-fetched because the next-fetch offset tracks
				// Committed()+1 — that one must stay in-flight (the persist
				// callback owns its MarkDone), so we do NOT markUpdateDone here.
				// Without offTrk we keep the legacy highest-seen advance.
				if c.offTrk == nil && u.UpdateId >= offset {
					offset = u.UpdateId + 1
					advanced = true
				}
				continue
			}
			var richRaw json.RawMessage
			if i < len(probes) {
				richRaw = richRawFor(probes[i])
			}
			c.dispatchGuarded(&u, richRaw)
			dispatchedAny = true
			if c.offTrk == nil && u.UpdateId >= offset {
				offset = u.UpdateId + 1
				advanced = true
			}
		}
		// Persist the offset. With the tracker, save only the highest CONTIGUOUS
		// durably-persisted (or no-op) update_id — never past an update whose
		// Append is still in-flight (that is the whole safety property). Without
		// it (unit tests), fall back to the legacy highest-seen advance.
		var saveTo int64
		var doSave bool
		if c.offTrk != nil {
			if cur := c.offTrk.Committed(); cur > lastSaved {
				saveTo, doSave = cur, true
			}
			// The next getUpdates resumes from the contiguous-committed prefix so
			// an in-flight (unpersisted) update is re-fetched after a crash and
			// never silently acked. Steady-state re-fetches are suppressed by the
			// dedup map.
			offset = c.offTrk.Committed() + 1
		} else if advanced {
			saveTo, doSave = offset-1, true
		}
		if doSave && c.offsets != nil {
			// Best-effort: log the failure but don't stop polling.
			if err := c.offsets.Save(saveTo); err != nil {
				consecSaveFail++
				c.host.Logf("telegram: offset Save failed (consec=%d): %v", consecSaveFail, err)
				if consecSaveFail == offsetSaveFailAlert {
					c.host.Logf("telegram: WARNING offset persistence has failed %d times in a row — a broker restart will RE-DELIVER recent updates (dedup can't cover a long gap) until this clears; check the offset store path is writable.", consecSaveFail)
				}
			} else {
				consecSaveFail = 0
				lastSaved = saveTo
			}
		}

		// Pace the NO-PROGRESS re-poll. A non-empty batch that dispatched nothing
		// new means every update was a dedup-skip of an un-acked in-flight update
		// still sitting at the frontier (offset = Committed()+1) — a slow inbound
		// (voice → STT) that Telegram re-returns on EVERY long-poll because the
		// offset cannot advance past it until its message is durably persisted.
		// Looping instantly here burns ~3 getUpdates/sec for the whole persist
		// window (429 risk, log spam) and accomplishes nothing. Sleep
		// ~pollIdleBackoff between such no-progress polls instead. This adds ZERO
		// latency to genuinely-new updates (a new update IS dispatched →
		// dispatchedAny=true → no pacing) and preserves loss-freedom: the offset
		// still only advances on durable persist; we never skip the in-flight
		// update, only re-fetch it less often until its persist callback advances
		// Committed().
		if len(updates) > 0 && !dispatchedAny {
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(pollIdleBackoff):
			}
		}
	}
}

// dispatchGuarded runs dispatchUpdate under a panic recover. A panic while
// handling ONE update (a malformed payload, a nil deref in a converter, a bug
// in a plugin) must NOT kill polling: we log the panic + stack and return, and
// the caller advances the offset past this update so it is SKIPPED rather than
// re-fetched forever (a poison pill would otherwise wedge inbound permanently,
// or — under the goroutine supervisor — tight-loop on restart). Losing one bad
// update is the correct trade against inbound going dead. A dispatch panic is a
// code bug, not a Telegram-reachability problem, so it does NOT drive
// fetch-health DOWN (that would be a false outage alarm).
func (c *Channel) dispatchGuarded(u *gotgbot.Update, richRaw json.RawMessage) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 8192)
			n := runtime.Stack(buf, false)
			c.host.Logf("telegram: dispatch PANIC recovered (update=%d skipped): %v\n%s", u.UpdateId, r, buf[:n])
			// A panicked (poison) update is SKIPPED: mark it done so the
			// persisted offset advances past it rather than re-fetching it
			// forever (the contiguous-prefix tracker would otherwise wedge here).
			c.markUpdateDone(u.UpdateId)
		}
	}()
	c.dispatchUpdate(u, richRaw)
}

// dispatchUpdate converts a single Update into an Inbound (or several) and
// emits to the host. EditedMessage flows as new inbound — a real edit is a
// fresh message to the session — EXCEPT when its deliverable content is
// unchanged from what was already dispatched (a reaction-triggered phantom
// edited_message): dispatchMessage suppresses those via editSuppressor.
func (c *Channel) dispatchUpdate(u *gotgbot.Update, richRaw json.RawMessage) {
	switch {
	case u.Message != nil:
		// The message path owns its own offset marking: markUpdateDone on every
		// no-op outcome (skip/status/gate-drop/pair-consumed) and the
		// msgToUpdate seam → persist-callback MarkDone on the routed path.
		c.dispatchMessage(u.UpdateId, u.Message, false, richRaw)
	case u.EditedMessage != nil:
		c.dispatchMessage(u.UpdateId, u.EditedMessage, true, richRaw)
	case u.Poll != nil:
		// Events (poll_result / callback / reaction) are NEVER persisted; mark
		// the update done unconditionally so the offset advances past it,
		// regardless of whether the inner handler surfaced or dropped it.
		c.dispatchPollUpdate(u.UpdateId, u.Poll)
		c.markUpdateDone(u.UpdateId)
	case u.CallbackQuery != nil:
		c.dispatchCallback(u.UpdateId, u.CallbackQuery)
		c.markUpdateDone(u.UpdateId)
	case u.MessageReaction != nil:
		c.dispatchReaction(u.UpdateId, u.MessageReaction)
		c.markUpdateDone(u.UpdateId)
	default:
		// Any other subscribed-but-unhandled type. Should not occur given the
		// allowedUpdates list, but log metadata so a future subscription that
		// forgets its handler is visible rather than silently dropped. Never
		// persisted — mark done so it does not wedge the contiguous prefix.
		c.host.Logf("telegram: drop update=%d (subscribed type with no dispatch handler)", u.UpdateId)
		c.markUpdateDone(u.UpdateId)
	}
}

// dispatchPollUpdate converts an aggregate `poll` update into a poll_result
// InboundEvent and emits it via the inbound gate. Q-RESULT-1 = AGGREGATE +
// FINAL-ON-CLOSE: we surface the tally on close (IsClosed) — the firm
// requirement — and refresh the retained latest tally either way. We never
// surface per-voter identity (no poll_answer subscription).
//
// CB-2: the aggregate `poll` update carries NO chat and NO user. We recover the
// originating route + owner from the sentPolls map (populated on sendPoll) and
// stamp Sender.UserID with the route owner so the inbound passes IsUserAllowed
// in a DM. A poll id we never sent (unknown) is dropped with a metadata-only log
// — it is not ours to route.
func (c *Channel) dispatchPollUpdate(updateID int64, poll *gotgbot.Poll) {
	if poll == nil || poll.Id == "" {
		return
	}
	sp, ok := c.sentPolls.Get(poll.Id)
	if !ok {
		// Not a poll we sent (or evicted from the bounded map). Nothing to route.
		c.host.Logf("telegram: drop poll update=%d poll-unknown (not in sentPolls / evicted)", updateID)
		return
	}

	tally := pollTallyFromGotgbot(poll)
	// Refresh the retained latest tally under the map lock — the worker goroutine
	// (StopPoll) can mutate the same *sentPoll concurrently, so a Get→mutate→Put
	// read-modify-write would race. UpdateTally also bumps LRU recency.
	c.sentPolls.UpdateTally(poll.Id, tally)

	// Q-RESULT-1: the firm requirement is FINAL-ON-CLOSE (+ stop_poll). An
	// open-poll aggregate update is retained above for on-demand reads but NOT
	// surfaced to the agent (avoids per-vote spam). Only emit on close.
	if !poll.IsClosed {
		c.host.Logf("telegram: poll update=%d poll=%s open (tally retained, not surfaced — final-on-close)", updateID, poll.Id)
		return
	}

	// Late-event-after-worker-idle (v1 accepted limit): emitEvent → host.Emit →
	// Workers.Submit. If the route's worker has already idle-exited, Submit
	// returns false and host.Emit logs a metadata-only "emit SATURATED" line (see
	// internal/broker/host.go). For v1 a late aggregate tally on a long-closed
	// poll may thus be dropped — that is acceptable and visible in the log;
	// stop_poll is the deterministic read path that never depends on a live
	// worker. We do NOT silently lose it (the DROP log is the audit trail).
	in := pollResultInbound(c.Name(), sp, poll.Id, tally)
	c.emitEvent(updateID, in, "poll_result")
}

// permTapDataPrefix mirrors the broker's permCallbackPrefix (internal/broker/
// perm.go): the opaque callback_data namespace of relayed tool-use permission
// keyboards ("perm:<verb>:<id>"). Taps in this namespace get a DEFERRED ack —
// see dispatchCallback. Keep the two constants in lockstep.
const permTapDataPrefix = "perm:"

// askTapDataPrefix mirrors the broker's askCallbackPrefix (internal/broker/
// ask.go): the callback_data namespace of `ask`-tool keyboards ("ask:<id>:<idx>").
// The ask flow OWNS these keyboards — single-select clears them on resolve,
// multi-select toggles them in place — so the generic post-tap collapse below
// MUST skip them. Keep in lockstep with askCallbackPrefix.
const askTapDataPrefix = "ask:"

// callbackChosenData is the callback_data stamped on the single inert indicator
// button a generic keyboard collapses to after a tap (see
// collapseCallbackKeyboard). A tap on that indicator is a no-op: acked to clear
// the spinner, never re-collapsed or surfaced as an event.
const callbackChosenData = "c3:_chosen"

// Perm-tap feedback texts, delivered via answerCallbackQuery when the tap can
// never reach the broker's resolver (which otherwise owns the deferred answer).
// Short (Telegram caps answer text at 200 chars) and content-free — they never
// echo the prompt body.
const (
	permTapNotAuthorizedText = "⛔ Not authorized — this Telegram account/chat is not paired with C3, so the tap was ignored."
	permTapNotProcessedText  = "⚠️ C3 could not process the tap right now — try again shortly."
	permTapUnroutableText    = "⚠️ This permission prompt is no longer accessible — tap not processed."
)

// callbackAckToastText is the confirmation toast for a NON-permission callback
// tap (an ask-tool selection or a hand-rolled reply(buttons=…) button). The
// bare empty-opts ack only cleared the loading spinner and showed nothing, so
// the tap had no immediate confirmation — this brief non-alert toast is that
// feedback, mirroring how the perm path carries its outcome Text in its own ack
// (see AnswerCallback). Accurate for every non-perm kind, and truthful even on
// the rare inaccessible-message drop path: we DID receive the tap.
const callbackAckToastText = "✅ Received"

// dispatchCallback auto-acks an inline-keyboard callback (Q-RESULT-2: no
// "loading…" spinner, no agent-in-the-loop for the ack) and THEN surfaces it as
// a callback InboundEvent. P7 wires outbound keyboards that make these
// actionable; P4 only routes the inbound press.
//
// EXCEPTION — permission taps ("perm:" namespace) get a DEFERRED ack. Telegram
// answers a callback query exactly once, and for a permission tap that single
// answer is the only user-visible feedback surface a button press has. The
// original bare auto-ack spent it before the broker decided anything, so a tap
// the broker refused (non-operator / expired / unknown) looked like "Allow did
// nothing" — the 2026-06-30 fresh-install live bug. Now the broker's
// resolvePerm answers with the tap's real outcome, and every early exit below
// that stops a perm tap short of the broker answers explicitly instead
// (emitPermTap) — a perm tap can never again die silently.
//
// Pass-1 C4: CallbackQuery.Message is a MaybeInaccessibleMessage (interface).
// gotgbot rc.34 resolves it in CallbackQuery.UnmarshalJSON via
// unmarshalMaybeInaccessibleMessage (custom_helpers.go), which stores the
// concrete *value* type — gotgbot.Message for an accessible message (date != 0)
// or gotgbot.InaccessibleMessage for a deleted/too-old one (date == 0) — NOT a
// pointer. The original code type-asserted to *gotgbot.Message, which never
// matches the wire-decoded value, so EVERY real inline-keyboard callback was
// dropped ("message inaccessible") — the 2026-06-25 live ask/keyboard bug.
//
// resolveCallbackMessage below accepts both the value (the real getUpdates
// decode path) and the pointer (hand-built CallbackQuery values in tests / any
// future synthetic caller), returning nil only for the genuinely-inaccessible
// variant. An inaccessible message has no routable chat/thread, so we drop it
// with a metadata-only log after still auto-acking (the user's button must not
// spin either way).
func (c *Channel) dispatchCallback(updateID int64, cq *gotgbot.CallbackQuery) {
	if cq == nil {
		return
	}
	// A tap on the inert post-selection indicator (see collapseCallbackKeyboard)
	// is a no-op: clear the spinner and stop — never re-collapse or surface it as
	// an event. This is what makes a collapsed keyboard un-re-pressable.
	if cq.Data == callbackChosenData {
		if c.bot != nil {
			if _, err := c.bot.AnswerCallbackQuery(cq.Id, &gotgbot.AnswerCallbackQueryOpts{}); err != nil {
				c.logf("telegram: callback update=%d indicator ack failed: %v", updateID, err)
			}
		}
		return
	}
	permTap := strings.HasPrefix(cq.Data, permTapDataPrefix)
	// Auto-ack immediately, regardless of whether we can route the event. A
	// failed ack is logged but not fatal — the surfacing still proceeds. Perm
	// taps are the exception: their single answer is deferred to the outcome
	// (see the EXCEPTION note above).
	if !permTap && c.bot != nil {
		// Carry a confirmation toast, not a bare empty ack: the empty ack cleared
		// the spinner but showed nothing, so this toast is the tap's only
		// immediate feedback. ShowAlert stays false — a lightweight toast, not a
		// modal (see callbackAckToastText).
		if _, err := c.bot.AnswerCallbackQuery(cq.Id, &gotgbot.AnswerCallbackQueryOpts{Text: callbackAckToastText}); err != nil {
			c.logf("telegram: callback update=%d answerCallbackQuery failed: %v", updateID, err)
		}
	}

	msg := resolveCallbackMessage(cq.Message)
	if msg == nil {
		// Inaccessible message (deleted / too old) — no chat to route to. A perm
		// tap still gets its deferred answer (a notice); anything else was already
		// acked above (with the confirmation toast).
		if permTap {
			c.answerPermTap(updateID, cq.Id, permTapUnroutableText, false)
		}
		c.host.Logf("telegram: drop callback update=%d (message inaccessible; answered, cannot route)", updateID)
		return
	}

	in := &c3types.Inbound{
		Channel:   c.Name(),
		ChatID:    msg.Chat.Id,
		TopicID:   topicPtrFromThread(msg.MessageThreadId),
		MessageID: msg.MessageId,
		Sender:    convertSender(&cq.From),
		Timestamp: time.Now(),
		ConvKind:  convKindFromChat(msg.Chat),
		Kind:      c3types.InboundCallback,
		Event: &c3types.InboundEvent{
			Callback: &c3types.CallbackEvent{
				CallbackID: cq.Id,
				MessageID:  msg.MessageId,
				Actor:      convertSender(&cq.From),
				Data:       cq.Data,
			},
		},
	}
	if permTap {
		c.emitPermTap(updateID, in, cq.Id)
		return
	}
	// Generic agent-facing callback (a hand-rolled reply(buttons=…) tap). Collapse
	// the keyboard to a single inert "✓ <label>" so it can't be pressed again and
	// the message permanently records the choice. SKIP "ask:" — the ask flow owns
	// that keyboard (multi-select toggles it live); perm taps already returned above.
	if !strings.HasPrefix(cq.Data, askTapDataPrefix) {
		c.collapseCallbackKeyboard(updateID, msg, cq.Data)
	}
	c.emitEvent(updateID, in, "callback")
}

// collapseCallbackKeyboard edits a generic callback message's inline keyboard down
// to a single inert button showing the tapped option's label ("✓ <label>"), so the
// keyboard can't be pressed again and the message permanently records the choice.
// Best-effort: any failure is logged (metadata only) and never blocks surfacing
// the event. The label is looked up from the message's OWN reply markup by
// matching the tapped callback_data; a data-only synthetic tap falls back to a
// generic confirmation.
func (c *Channel) collapseCallbackKeyboard(updateID int64, msg *gotgbot.Message, data string) {
	if c.bot == nil || msg == nil || msg.ReplyMarkup == nil {
		return
	}
	label := ""
	for _, row := range msg.ReplyMarkup.InlineKeyboard {
		for _, btn := range row {
			if btn.CallbackData == data {
				label = btn.Text
			}
		}
	}
	if label == "" {
		label = "Selected"
	}
	collapsed := gotgbot.InlineKeyboardMarkup{
		InlineKeyboard: [][]gotgbot.InlineKeyboardButton{{
			{Text: "✓ " + label, CallbackData: callbackChosenData},
		}},
	}
	if _, _, err := c.bot.EditMessageReplyMarkup(&gotgbot.EditMessageReplyMarkupOpts{
		ChatId:      msg.Chat.Id,
		MessageId:   msg.MessageId,
		ReplyMarkup: collapsed,
	}); err != nil {
		c.logf("telegram: callback update=%d collapse keyboard failed: %v", updateID, err)
	}
}

// emitPermTap runs a permission-keyboard tap ("perm:" namespace) through the
// inbound gate and emits it — emitEvent's contract plus the deferred-ack duty:
// dispatchCallback skipped the bare auto-ack for perm taps, so every outcome
// that stops the tap short of the broker's resolvePerm (which owns the answer
// on the routed path) must answer the callback query here instead. A refused
// tap stays REFUSED — the verdict is never honored — only the silence goes.
func (c *Channel) emitPermTap(updateID int64, in *c3types.Inbound, callbackID string) {
	switch c.host.GateInbound(in) {
	case channel.GateInboundDrop:
		// Non-allowlisted sender/chat: the tap must not reach the broker (the
		// allowlist gate is the security boundary), but unlike a generic gate
		// drop — where strangers see a dead bot — the tapper is answered with an
		// explicit refusal: only someone who can already SEE the prompt's
		// keyboard can tap it, so the alert leaks nothing and turns the
		// fresh-install "Allow did nothing" trap into an actionable message.
		// Metadata-only log, as ever.
		c.host.Logf("telegram: GATE drop update=%d kind=perm_tap chat=%d msg=%d sender=%d",
			updateID, in.ChatID, in.MessageID, in.Sender.UserID)
		c.answerPermTap(updateID, callbackID, permTapNotAuthorizedText, true)
		return
	case channel.GateInboundPairConsumed:
		// Unreachable for a callback (no text body to match a pairing code) —
		// parity with emitEvent. Still answered: no perm-tap branch may be silent.
		c.host.Logf("telegram: GATE pair-consumed update=%d kind=perm_tap chat=%d (unexpected for an event)",
			updateID, in.ChatID)
		c.answerPermTap(updateID, callbackID, permTapNotProcessedText, false)
		return
	}
	c.host.Logf("telegram: inbound update=%d kind=perm_tap chat=%d msg=%d", updateID, in.ChatID, in.MessageID)
	if !c.host.Emit(in) {
		// Worker queue full / stopped: the broker will never see this tap, so its
		// deferred answer must happen here. The pending perm stays live
		// broker-side — the notice invites a re-tap.
		c.answerPermTap(updateID, callbackID, permTapNotProcessedText, false)
	}
}

// answerPermTap answers a permission tap's callback query with outcome feedback
// — a toast, or a modal alert for the must-not-miss refusals. Best-effort: a
// failed answer is logged (metadata only) and the client eventually clears the
// spinner on its own.
func (c *Channel) answerPermTap(updateID int64, callbackID, text string, showAlert bool) {
	if err := c.AnswerCallback(callbackID, text, showAlert); err != nil {
		c.logf("telegram: perm-tap update=%d answerCallbackQuery failed: %v", updateID, err)
	}
}

// AnswerCallback answers an inline-keyboard callback query with optional toast
// text (empty = bare ack) or a modal alert. This is the broker-facing
// callbackAnswerer capability (see internal/broker/perm.go): the permission
// relay uses it to deliver the deferred per-tap outcome for "perm:" keyboards
// (resolved / not-authorized / expired) after dispatchCallback skipped the
// early bare ack for that namespace.
func (c *Channel) AnswerCallback(callbackID, text string, showAlert bool) error {
	if c.bot == nil {
		return errors.New("telegram: bot not started")
	}
	_, err := c.bot.AnswerCallbackQuery(callbackID, &gotgbot.AnswerCallbackQueryOpts{
		Text:      text,
		ShowAlert: showAlert,
	})
	return c.scrubToken(err)
}

// resolveCallbackMessage extracts the accessible *gotgbot.Message from a
// CallbackQuery.Message (a MaybeInaccessibleMessage interface), or nil when the
// message is the inaccessible variant (deleted / too old) or absent.
//
// CRUCIAL: gotgbot rc.34 unmarshals the interface to a concrete VALUE, not a
// pointer (see unmarshalMaybeInaccessibleMessage in the module's
// custom_helpers.go) — so the wire path yields a gotgbot.Message value. We also
// accept *gotgbot.Message so hand-built CallbackQuery values (tests / future
// synthetic callers) keep working. gotgbot.InaccessibleMessage (date == 0) has no
// routable chat/thread and returns nil → the caller drops it gracefully.
func resolveCallbackMessage(m gotgbot.MaybeInaccessibleMessage) *gotgbot.Message {
	switch v := m.(type) {
	case gotgbot.Message:
		return &v
	case *gotgbot.Message:
		return v
	default:
		// gotgbot.InaccessibleMessage (value or pointer) or nil — not routable.
		return nil
	}
}

// dispatchReaction converts a message_reaction update into a reaction
// InboundEvent. Pass-1 C3: OldReaction/NewReaction are []ReactionType (an
// interface over emoji / custom-emoji / paid). Added/Removed are the set-diff of
// new vs old, rendered via reactionTypeString — standard emoji verbatim, custom/
// paid as the "[custom]"/"[paid]" sentinel (never silently dropped).
func (c *Channel) dispatchReaction(updateID int64, mr *gotgbot.MessageReactionUpdated) {
	if mr == nil {
		return
	}
	added, removed := diffReactions(mr.OldReaction, mr.NewReaction)
	if len(added) == 0 && len(removed) == 0 {
		// No net change (e.g. only custom/paid churn that diffed to nothing) —
		// nothing meaningful to surface.
		c.host.Logf("telegram: reaction update=%d no net change (skipped)", updateID)
		return
	}
	var actor c3types.Sender
	if mr.User != nil {
		actor = convertSender(mr.User)
	}
	in := &c3types.Inbound{
		Channel:   c.Name(),
		ChatID:    mr.Chat.Id,
		MessageID: mr.MessageId,
		Sender:    actor,
		Timestamp: time.Now(),
		ConvKind:  convKindFromChat(mr.Chat),
		Kind:      c3types.InboundReaction,
		Event: &c3types.InboundEvent{
			Reaction: &c3types.ReactionEvent{
				MessageID: mr.MessageId,
				Actor:     actor,
				Added:     added,
				Removed:   removed,
			},
		},
	}
	c.emitEvent(updateID, in, "reaction")
}

// emitEvent runs a synthesized event Inbound through the inbound gate and emits
// it. CB-3: replicates the metadata-only GATE-drop logging contract — no content
// is ever logged, strangers see nothing. Mirrors dispatchMessage's gate handling
// without the message-only STT/kind plumbing.
func (c *Channel) emitEvent(updateID int64, in *c3types.Inbound, kind string) {
	// Synthesized events (poll_result / reaction / callback) are NEVER persisted
	// to the durable queue — they are delivered live or dropped. The offset
	// marking for the event update_id is owned by dispatchUpdate (which calls
	// markUpdateDone after every event-dispatch case, covering early returns too).
	switch c.host.GateInbound(in) {
	case channel.GateInboundDrop:
		c.host.Logf("telegram: GATE drop update=%d kind=%s chat=%d msg=%d sender=%d",
			updateID, kind, in.ChatID, in.MessageID, in.Sender.UserID)
		return
	case channel.GateInboundPairConsumed:
		// An event body never carries a pairing code; this branch is here for
		// completeness/parity with the message path.
		c.host.Logf("telegram: GATE pair-consumed update=%d kind=%s chat=%d (unexpected for an event)",
			updateID, kind, in.ChatID)
		return
	}
	c.host.Logf("telegram: inbound update=%d kind=%s chat=%d msg=%d", updateID, kind, in.ChatID, in.MessageID)
	// The bool return is intentionally ignored for events: the event update_id's
	// offset marking is owned by dispatchUpdate (it calls markUpdateDone after the
	// event-dispatch case unconditionally), so a queue-full Emit drop of an event
	// does not strand its update — the offset advances regardless. Events are
	// never persisted, so there is no msgToUpdate seam to clean up here.
	_ = c.host.Emit(in)
}

// pollTallyFromGotgbot converts a gotgbot.Poll into the in-package aggregate
// snapshot.
func pollTallyFromGotgbot(poll *gotgbot.Poll) *pollTally {
	opts := make([]pollOptionTally, 0, len(poll.Options))
	for _, o := range poll.Options {
		opts = append(opts, pollOptionTally{Text: o.Text, VoterCount: int(o.VoterCount)})
	}
	return &pollTally{
		Question:    poll.Question,
		TotalVoters: int(poll.TotalVoterCount),
		IsClosed:    poll.IsClosed,
		Options:     opts,
	}
}

// pollResultInbound builds the channel-neutral poll_result Inbound for a tally,
// stamping the stored route + owner (CB-2). Shared by the passive poll-update
// path and stop_poll.
func pollResultInbound(channelName string, sp *sentPoll, pollID string, tally *pollTally) *c3types.Inbound {
	opts := make([]c3types.PollOptionTally, 0, len(tally.Options))
	for _, o := range tally.Options {
		opts = append(opts, c3types.PollOptionTally{Text: o.Text, VoterCount: o.VoterCount})
	}
	return &c3types.Inbound{
		Channel:   channelName,
		ChatID:    sp.ChatID,
		TopicID:   sp.TopicID,
		MessageID: sp.MessageID,
		Sender:    c3types.Sender{UserID: sp.OwnerUserID}, // CB-2: route-owner stamp
		Timestamp: time.Now(),
		// Only a bare chat id survives in sentPoll, so this is the one path
		// that still derives the kind from the id sign — inside the channel
		// that owns that id space.
		ConvKind: convKindFromChatID(sp.ChatID),
		Kind:     c3types.InboundPollResult,
		Event: &c3types.InboundEvent{
			PollResult: &c3types.PollResult{
				PollID:      pollID,
				Question:    tally.Question,
				TotalVoters: tally.TotalVoters,
				IsClosed:    tally.IsClosed,
				Options:     opts,
			},
		},
	}
}

// diffReactions computes the added/removed display-string sets between an old
// and new reaction list. Each ReactionType is rendered by reactionTypeString.
func diffReactions(old, current []gotgbot.ReactionType) (added, removed []string) {
	oldSet := reactionStringSet(old)
	newSet := reactionStringSet(current)
	for s := range newSet {
		if !oldSet[s] {
			added = append(added, s)
		}
	}
	for s := range oldSet {
		if !newSet[s] {
			removed = append(removed, s)
		}
	}
	return added, removed
}

// reactionStringSet renders a reaction list into a set of display strings.
func reactionStringSet(rs []gotgbot.ReactionType) map[string]bool {
	set := make(map[string]bool, len(rs))
	for _, r := range rs {
		if s := reactionTypeString(r); s != "" {
			set[s] = true
		}
	}
	return set
}

// reactionTypeString renders a single ReactionType into a display string.
// Pass-1 C3: standard emoji verbatim; custom/paid reactions become the sentinels
// "[custom]"/"[paid]" so the agent sees that SOMETHING reacted rather than the
// reaction being silently dropped. An unknown future type falls back to its
// GetType() tag wrapped in brackets.
func reactionTypeString(r gotgbot.ReactionType) string {
	switch v := r.(type) {
	case gotgbot.ReactionTypeEmoji:
		return v.Emoji
	case gotgbot.ReactionTypeCustomEmoji:
		return "[custom]"
	case gotgbot.ReactionTypePaid:
		return "[paid]"
	default:
		if r == nil {
			return ""
		}
		return "[" + r.GetType() + "]"
	}
}

// isBrokerCommand reports whether text is a broker-owned bot command —
// "/status", "/queue" or "/drain" — optionally "@<botname>"-suffixed,
// case-insensitive, after trimming. Only the FIRST token decides (and the
// "@botname" strip applies to that token ONLY — amendment A5: truncating at
// the first '@' anywhere would amputate /drain arguments containing '@').
// "/statusly" and "please /status" are NOT matched; "/drain genie all" is.
// The broker's HandleCommand does the real parsing — a matched-but-malformed
// command it declines ("", false) falls through to normal routing.
func (c *Channel) isBrokerCommand(text string) bool {
	t := strings.TrimSpace(text)
	if i := strings.IndexAny(t, " \t\n\r"); i >= 0 {
		t = t[:i]
	}
	if i := strings.IndexByte(t, '@'); i >= 0 { // A5: first token only
		t = t[:i]
	}
	return strings.EqualFold(t, "/status") ||
		strings.EqualFold(t, "/queue") ||
		strings.EqualFold(t, "/drain")
}

// markUpdateDone marks an update done in the persisted-offset tracker for every
// NON-persist outcome (gated, dropped, non-message, pair-consumed, dedup-skip,
// broker commands /status //queue //drain). Nil-safe so the early-build paths
// and tests that leave offTrk unset (the conflict/resilience suites) are no-ops.
func (c *Channel) markUpdateDone(updateID int64) {
	if c.offTrk != nil {
		c.offTrk.MarkDone(updateID)
	}
}

// logDedupSkip emits the appropriate log line for a dedup-skipped update and
// returns the updated throttle cursor (the last in-flight frontier id logged).
//
// The persisted-offset tracker deliberately keeps the getUpdates offset at
// Committed()+1 — never past an un-persisted update — so a crash at the frontier
// is loss-free: Telegram redelivers the un-acked update. A consequence is that
// EVERY poll re-draws any update whose durable persist is still in flight (a slow
// voice → STT can hold the frontier for up to sttFlushTimeout), and the dedup map
// skips each redraw. That is expected — NOT a "recent duplicate" — so logging it
// every ~1s (pollIdleBackoff) is misleading spam (the ROADMAP re-poll/dedup-skip
// live regression, 2026-06-27). An in-flight re-fetch (id still > Committed()) is
// therefore logged at most ONCE per distinct frontier id: enough to make a
// genuinely wedged (never-marked-done) update visible without flooding the log,
// while a normal STT window logs a single line and goes quiet. A GENUINE
// redelivery of an already-committed update (id <= Committed() — rare: a real
// Telegram glitch, or a post-restart redraw against a seeded offset) keeps its
// per-occurrence "recent duplicate" line, since those are worth seeing.
//
// This changes ONLY the logging; the dedup + offset semantics are untouched, so
// the loss-free guarantee is preserved.
func (c *Channel) logDedupSkip(id, lastInflightLogged int64) int64 {
	if c.offTrk != nil && id > c.offTrk.Committed() {
		if id != lastInflightLogged {
			c.host.Logf("telegram: re-poll skip update=%d — still awaiting durable persist; offset holds at the un-acked frontier (loss-free by design)", id)
		}
		return id
	}
	c.host.Logf("telegram: dedup skip update=%d (recent duplicate)", id)
	return lastInflightLogged
}

// topicPtrFromThread converts a gotgbot message_thread_id (0 = none) into the
// *int64 TopicID convention (nil = no topic).
func topicPtrFromThread(threadID int64) *int64 {
	if threadID == 0 {
		return nil
	}
	t := threadID
	return &t
}

// dispatchMessage converts and emits one message. Logs metadata only —
// see DEBUGGING.md for the content policy.
//
// Default-deny gate (TODO #1, locked 2026-05-18): every inbound runs
// through host.GateInbound BEFORE host.Emit. Non-allowlisted senders
// see their messages silently dropped (no broker worker invoked, no
// log of the body — strangers see a dead bot). The only exception is
// during an active pairing window, where a body matching the 4-digit
// code is consumed by the broker (allowlist updated + persisted) and
// the message itself is not forwarded.
func (c *Channel) dispatchMessage(updateID int64, msg *gotgbot.Message, edited bool, richRaw json.RawMessage) {
	if !c.cfg.RichInboundEnabled() {
		richRaw = nil // toggle off ⇒ rich messages surface as today (empty)
	}
	in := convertInbound(c.Name(), msg, c.cfg.STTPrefix, richRaw)
	if in == nil {
		c.host.Logf("telegram: skip update=%d msg=%d chat=%d thread=%d (unsupported service)",
			updateID, msg.MessageId, msg.Chat.Id, msg.MessageThreadId)
		// Unsupported service message — never persisted; unblock the offset.
		c.markUpdateDone(updateID)
		return
	}
	// State the amendment fact for the broker: an edited_message re-dispatches
	// with the SAME MessageID, and the broker's delivered-dedup is keyed on that
	// id alone. (The phantom-edit suppressor below drops content-free edits, so
	// anything that survives to Emit carries content the session must see.)
	in.Edited = edited
	kind := "text"
	if len(in.Attachments) > 0 && in.Attachments[0].Kind != "" {
		kind = in.Attachments[0].Kind
	}
	// I-SEC: the allowlist gate runs FIRST — BEFORE the /status intercept. A
	// non-allowlisted stranger must get nothing back (no reply) and must never
	// leak the broker-wide /status summary (topic names, pending counts, session
	// counts) in a DM / group-General. So a stranger's "/status" falls into the
	// GateInboundDrop branch below (silent default-deny). Only a GateInboundAllow
	// result reaches the /status intercept that follows the switch.
	switch c.host.GateInbound(in) {
	case channel.GateInboundDrop:
		// Silent drop. Do NOT log content; metadata only — surface enough
		// to debug a misconfigured allowlist via broker.log without
		// echoing message text or sender handle. See default-deny posture
		// in TODO #1.
		c.host.Logf("telegram: GATE drop update=%d msg=%d chat=%d thread=%d sender=%d kind=%s",
			updateID, msg.MessageId, msg.Chat.Id, msg.MessageThreadId, in.Sender.UserID, kind)
		// Dropped — never persisted; unblock the offset over this update.
		c.markUpdateDone(updateID)
		return
	case channel.GateInboundPairConsumed:
		// Body matched an active pairing code; allowlist already updated
		// by the broker. Message itself is a control-plane signal — do
		// NOT forward as inbound content.
		c.host.Logf("telegram: GATE pair-consumed update=%d msg=%d chat=%d thread=%d sender=%d (allowlist updated)",
			updateID, msg.MessageId, msg.Chat.Id, msg.MessageThreadId, in.Sender.UserID)
		// Pair-consumed control signal — never persisted; unblock the offset.
		c.markUpdateDone(updateID)
		return
	}
	// Gate ALLOWED (allowlisted sender). Broker-owned command intercept: a
	// "/status", "/queue" or "/drain" inbound from an allowlisted sender is
	// handled by the broker directly (it answers + is NEVER queued or routed to
	// an agent). It is intercepted AFTER the gate (I-SEC) so a stranger can
	// never reach it. Other commands fall through to normal routing.
	//
	// A6: the intercept fires only for attachment-free messages — convertInbound
	// puts media CAPTIONS into in.Text, and a handled caption would silently
	// swallow the attachment (command-in-caption is unsupported in v1).
	//
	// Empty-reply skip: handled with reply == "" means there is NOTHING to send
	// — either an operator-gated command silently dropped for a non-operator
	// (INV-7: zero bytes back, no hint the command exists) or an async command
	// (/drain, /queue <q>) that posts its own reply from a broker goroutine
	// (A1). The update is still marked done either way.
	if len(in.Attachments) == 0 && c.isBrokerCommand(in.Text) {
		if reply, handled := c.host.HandleCommand(in); handled {
			if reply == "" {
				c.host.Logf("telegram: command handled silently update=%d chat=%d thread=%d (async or denied; not routed)",
					updateID, msg.Chat.Id, msg.MessageThreadId)
			} else if _, err := c.SendReply(c3types.ReplyArgs{
				Channel: c.Name(), ChatID: in.ChatID, TopicID: in.TopicID, Text: reply,
				// MarkupNone (security): these broker-generated replies
				// (/status, bare /queue, parse/resolve rejects) interpolate
				// topic names — renameable by any group member — and echo
				// operator tokens; plain text keeps them inert instead of
				// rendering as live Telegram markdown from the trusted bot.
				Markup: c3types.MarkupNone,
			}); err != nil {
				c.logf("telegram: command reply send failed update=%d chat=%d: %v", updateID, in.ChatID, err)
			} else {
				c.host.Logf("telegram: command reply sent update=%d chat=%d thread=%d (not routed)", updateID, in.ChatID, msg.MessageThreadId)
			}
			// Handled, never persisted — unblock the offset over this update.
			c.markUpdateDone(updateID)
			return
		}
	}
	// Phantom-edit suppression. Telegram emits edited_message updates for
	// non-content changes too — documented: "may at times be triggered by
	// changes to message fields that are either unavailable or not actively
	// used by your bot"; the live trigger is a REACTION on the message,
	// including C3's own `react` tool. Re-dispatching one re-runs STT and
	// delivers the same transcription again (2026-07-19 duplicate report), so
	// an edit whose deliverable fingerprint is unchanged is dropped here — as
	// is one on a message too old to edit, or one echoing C3's own react
	// (2026-07-27, editsupp.go suppressReason). A same-update_id redelivery is
	// exempt (the loss-free Append-retry path must re-dispatch), and so is any
	// edit whose content actually changed. Runs AFTER the gate so
	// non-allowlisted senders can never populate the baseline memory.
	if c.editSupp != nil {
		fp := editFingerprint(in, msg)
		// msg.Date is the ORIGINAL send time and stays put across an edit, so it
		// dates the message, not the update — which is what the age rule needs.
		if edited {
			if why := c.editSupp.suppressReason(msg.Chat.Id, msg.MessageId, updateID, fp, msg.Date); why != "" {
				c.host.Logf("telegram: suppress phantom edit update=%d msg=%d chat=%d thread=%d kind=%s — %s; not re-dispatched",
					updateID, msg.MessageId, msg.Chat.Id, msg.MessageThreadId, kind, why)
				c.markUpdateDone(updateID)
				return
			}
		}
		// Baseline the just-dispatched content (original OR real edit).
		// Recorded before Emit: a capacity-dropped Emit leaves the baseline
		// set, which at worst suppresses a later byte-identical edit of a
		// message the session never saw — the same visibility loss as the
		// drop itself, and the drop is already logged loudly.
		c.editSupp.record(msg.Chat.Id, msg.MessageId, updateID, fp)
	}
	c.host.Logf("telegram: inbound update=%d msg=%d chat=%d thread=%d kind=%s edited=%v",
		updateID, msg.MessageId, msg.Chat.Id, msg.MessageThreadId, kind, edited)
	// Record the (chat_id, message_id) → update_id seam BEFORE Emit so the broker's
	// persist callback (fired after Append+fsync) can MarkDone the right source
	// update. Scoped by CHAT because message_ids are per-chat and two chats persist
	// concurrently (see seamKey). FIFO-append (not overwrite): two in-flight updates
	// can share a (chat, message_id) — an edited_message during the persist window —
	// and each must resolve its own persist outcome in order. Guarded on the
	// tracker being live (Start seeds msgToUpdate alongside it); unit tests that
	// leave offTrk nil never persist, so the seam is unused.
	if c.offTrk != nil {
		c.mu.Lock()
		c.seamStageLocked(in.ChatID, in.MessageID, updateID)
		c.mu.Unlock()
	}
	// Emit reports false when the route worker is still saturated after Emit's own
	// bounded grace window (submitGraceWindow). The seam above already staged
	// msgToUpdate + the tracker's in-flight registration for this update.
	//
	// This used to mark the update done so the contiguous-prefix offset advanced
	// past it (I4), on the reasoning that a >64 burst must not wedge all inbound.
	// But Telegram never redelivers an ACKNOWLEDGED update, so advancing destroyed
	// a message the user sent — silently, and only visible in broker.log. The
	// maintainer's call (v0.1.0 release audit) reverses that trade: redelivery
	// churn under overload is preferable to deliberate loss.
	if !c.host.Emit(in) && c.offTrk != nil {
		// The route worker is saturated and this message was NOT persisted anywhere.
		//
		// Do NOT mark the update done. Leaving it in-flight holds the
		// contiguous-prefix offset, so Telegram retains the update and redelivers
		// it once the route drains — the message survives instead of being dropped.
		// This is the same loss-free shape as a failed durable Append.
		//
		// Remove exactly THIS update's staged seam entry (not the front, which may
		// belong to an earlier still-in-flight update on the same message_id), and
		// forget its poll-side dedup record so the redelivery genuinely
		// RE-DISPATCHES instead of being dedup-skipped until the 5-minute TTL.
		// Pacing note: this re-poll is NOT paced by pollIdleBackoff. dispatchedAny
		// is set unconditionally above, so a held dispatch reads as progress and
		// the idle backoff at the loop end is skipped. The real pacing is the
		// submitGraceWindow Emit already spent before refusing — 2s per held
		// update, one getUpdates per batch.
		c.mu.Lock()
		c.seamRemoveLocked(in.ChatID, in.MessageID, updateID)
		c.mu.Unlock()
		if c.dedup != nil {
			c.dedup.forget(updateID)
		}
	}
}
