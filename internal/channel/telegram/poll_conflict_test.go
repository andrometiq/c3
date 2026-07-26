package telegram

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
)

// funcBotClient is a scripted gotgbot.BotClient test double. Each getUpdates
// call invokes fn(callNumber) and returns its (raw, err); an optional per-call
// delay (ctx-aware) throttles the loop so a success path that re-polls
// immediately can't busy-spin. Bot.RequestWithContext delegates straight here
// with no error wrapping, so an *gotgbot.TelegramError returned by fn reaches
// classifyError verbatim.
type funcBotClient struct {
	mu    sync.Mutex
	n     int
	fn    func(call int) (json.RawMessage, error)
	delay time.Duration
}

func (f *funcBotClient) RequestWithContext(ctx context.Context, token, method string, params map[string]any, opts *gotgbot.RequestOpts) (json.RawMessage, error) {
	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(f.delay):
		}
	}
	f.mu.Lock()
	f.n++
	call := f.n
	f.mu.Unlock()
	return f.fn(call)
}

func (f *funcBotClient) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

func (f *funcBotClient) GetAPIURL(*gotgbot.RequestOpts) string               { return "" }
func (f *funcBotClient) FileURL(string, string, *gotgbot.RequestOpts) string { return "" }

func newConflictTestChannel(h *fakeHost, bc gotgbot.BotClient) *Channel {
	c := &Channel{
		host:    h,
		cfg:     Config{},
		bot:     &gotgbot.Bot{Token: "test", BotClient: bc},
		authBrk: newAuthBreaker(auth401Threshold),
		health:  newFetchHealth(),
		// Tiny backoff so the conflict-retry path is exercised without
		// multi-second sleeps (zero would fall back to the production consts).
		conflictBackoffBase: time.Millisecond,
		conflictBackoffMax:  5 * time.Millisecond,
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

func startPollLoop(c *Channel) chan struct{} {
	done := make(chan struct{})
	go func() {
		c.pollLoop()
		close(done)
	}()
	return done
}

func awaitDone(t *testing.T, done chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pollLoop did not return within 2s after ctx cancel")
	}
}

func waitUntil(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

func hasDownEvent(h *fakeHost) bool {
	for _, ev := range h.healthEvents() {
		if ev.State == c3types.HealthStateDown {
			return true
		}
	}
	return false
}

// TestPollLoop_TransientConflictRetriesAndRecovers is the core regression guard
// for the 2026-06-22 fresh-laptop incident: a flaky-network long-poll timeout
// drew a 409 ("another getUpdates active"), and the OLD pollLoop treated 409 as
// terminal — it exited after exactly one call and inbound stayed dead until a
// human killed and restarted the broker.
//
// Fixed behavior: a 409 backs off and RETRIES, so the call count climbs past 1
// and the next (healthy, empty) getUpdates succeeds — fully automatic recovery,
// no restart. A single transient 409 (consec=1 < downAfter=3) must NOT raise the
// out-of-band DOWN alert.
func TestPollLoop_TransientConflictRetriesAndRecovers(t *testing.T) {
	h := &fakeHost{}
	fb := &funcBotClient{
		delay: time.Millisecond,
		fn: func(call int) (json.RawMessage, error) {
			if call == 1 {
				return nil, &gotgbot.TelegramError{Code: 409, Description: "Conflict: terminated by other getUpdates request"}
			}
			return json.RawMessage("[]"), nil // healthy empty long-poll
		},
	}
	c := newConflictTestChannel(h, fb)
	done := startPollLoop(c)

	if !waitUntil(2*time.Second, func() bool { return fb.count() >= 3 }) {
		c.cancel()
		awaitDone(t, done)
		t.Fatalf("getUpdates called %d times; want >=3 — a terminal 409 calls exactly once and never recovers", fb.count())
	}
	if down, _, _, _, _, _ := c.health.snapshot(); down {
		t.Fatalf("a single transient 409 (consec=1 < downAfter=3) must not flip fetch-health DOWN")
	}

	c.cancel()
	awaitDone(t, done)
}

// TestPollLoop_PacesNoProgressRepollOnUnackedFrontier is the regression guard
// for the slow-inbound poll-spin bug. When a slow inbound (voice → STT, 12–110s)
// sits at the frontier (offset = Committed()+1), Telegram's long-poll returns it
// IMMEDIATELY on every call (it is >= offset and un-acked). The dedup map skips
// it, and with offTrk live the skip branch does nothing — so the OLD loop would
// `continue` and re-poll instantly at ~3 getUpdates/sec for the whole persist
// window (thousands of redundant calls, 429 risk, log spam).
//
// Fixed behavior: a non-empty batch that dispatched NOTHING new (every update was
// a dedup-skip of the un-acked frontier) paces ~pollIdleBackoff between polls
// instead of spinning. Construction: seed committed=4 so update_id=5 is the next
// contiguous frontier id; the message is Emitted but NO persist callback fires in
// this test, so committed never advances past 5 — update 5 stays in-flight and
// Telegram (the fake) re-returns it on every call. After the first sighting
// dispatches it once, every later sighting is a dedup-skip → the loop must pace.
func TestPollLoop_PacesNoProgressRepollOnUnackedFrontier(t *testing.T) {
	h := &fakeHost{}       // default decision = GateInboundAllow; Emit returns true
	fb := &funcBotClient{} // delay 0 → an UNPACED loop spins as fast as the CPU allows
	c := newConflictTestChannel(h, fb)
	c.offTrk = newOffsetTracker(4)
	c.msgToUpdate = map[int64][]int64{}
	c.dedup = newUpdateDedup(2000, 5*time.Minute)

	upd := []gotgbot.Update{{
		UpdateId: 5,
		Message: &gotgbot.Message{
			MessageId: 500,
			Date:      time.Now().Unix(),
			Chat:      gotgbot.Chat{Id: 7, Type: "private"},
			From:      &gotgbot.User{Id: 7, Username: "u"},
			Text:      "slow-inbound-stub",
		},
	}}
	raw, _ := json.Marshal(upd)
	fb.fn = func(call int) (json.RawMessage, error) { return raw, nil }

	done := startPollLoop(c)
	defer func() { c.cancel(); awaitDone(t, done) }()

	// First sighting dispatches once (dispatchedAny=true) and registers update 5
	// in-flight; it never persists, so committed stays 4 and Telegram re-returns 5.
	if !waitUntil(2*time.Second, func() bool { return h.emitCount() >= 1 }) {
		t.Fatal("frontier update was never dispatched on first sighting")
	}

	// From here every re-fetch is a dedup-skip of the un-acked frontier → no new
	// dispatch → the loop MUST pace at ~pollIdleBackoff rather than spin. Observe a
	// window: an UNPACED loop does thousands of getUpdates here; a paced one ~1–2.
	base := fb.count()
	window := pollIdleBackoff + 500*time.Millisecond
	time.Sleep(window)
	got := fb.count() - base
	maxPaced := int(window/pollIdleBackoff) + 3 // generous margin vs. the ~thousands an unpaced spin does
	if got > maxPaced {
		t.Fatalf("no-progress re-poll is not paced: %d getUpdates in %v (want <= %d; an unpaced spin does thousands)", got, window, maxPaced)
	}
	if h.emitCount() != 1 {
		t.Fatalf("the in-flight frontier update must be dispatched exactly once (dedup-skipped after); emitCount=%d, want 1", h.emitCount())
	}
}

// TestPollLoop_NewUpdatesDispatchWithoutPacing is the companion: the no-progress
// pacing must add ZERO latency to genuinely-new updates. Every getUpdates returns
// a BRAND-NEW update id, so every batch dispatches → dispatchedAny=true → the loop
// never paces. It should therefore dispatch many updates well within a single
// pollIdleBackoff window (if pacing wrongly applied to new updates, dispatching N
// of them would take ~N × pollIdleBackoff).
func TestPollLoop_NewUpdatesDispatchWithoutPacing(t *testing.T) {
	h := &fakeHost{} // GateInboundAllow; Emit returns true
	fb := &funcBotClient{delay: time.Millisecond}
	c := newConflictTestChannel(h, fb)
	c.offTrk = newOffsetTracker(1000)
	c.msgToUpdate = map[int64][]int64{}
	c.dedup = newUpdateDedup(2000, 5*time.Minute)

	fb.fn = func(call int) (json.RawMessage, error) {
		upd := []gotgbot.Update{{
			UpdateId: int64(1000 + call), // a new id every call ⇒ never a dedup-skip
			Message: &gotgbot.Message{
				MessageId: int64(5000 + call),
				Date:      time.Now().Unix(),
				Chat:      gotgbot.Chat{Id: 7, Type: "private"},
				From:      &gotgbot.User{Id: 7, Username: "u"},
				Text:      "fresh",
			},
		}}
		raw, _ := json.Marshal(upd)
		return raw, nil
	}

	done := startPollLoop(c)
	defer func() { c.cancel(); awaitDone(t, done) }()

	// 5 distinct new updates must dispatch well under 5×pollIdleBackoff — proving
	// new updates are NOT subject to the no-progress pacing.
	if !waitUntil(pollIdleBackoff, func() bool { return h.emitCount() >= 5 }) {
		t.Fatalf("new updates were paced: only %d dispatched within %v (want >= 5; new updates must not pace)", h.emitCount(), pollIdleBackoff)
	}
}

// TestPollLoop_PersistentConflictAlertsAndKeepsPolling covers the genuine
// second-poller case (e.g. another machine holding the token). The conflict is
// NOT self-healing on its own, so two things must happen: (1) the fetch-health
// machine flips DOWN so the operator is alerted out-of-band, and (2) the loop
// KEEPS polling (never exits) so it self-heals the instant the other poller
// stops — still no manual broker restart. The old code recorded a single
// failure and exited, so neither held.
func TestPollLoop_PersistentConflictAlertsAndKeepsPolling(t *testing.T) {
	h := &fakeHost{}
	fb := &funcBotClient{
		delay: time.Millisecond,
		fn: func(call int) (json.RawMessage, error) {
			return nil, &gotgbot.TelegramError{Code: 409, Description: "Conflict"}
		},
	}
	c := newConflictTestChannel(h, fb)
	done := startPollLoop(c)

	if !waitUntil(2*time.Second, func() bool { return hasDownEvent(h) }) {
		c.cancel()
		awaitDone(t, done)
		t.Fatal("persistent 409 must drive fetch-health DOWN (out-of-band alert); none fired")
	}
	base := fb.count()
	if !waitUntil(2*time.Second, func() bool { return fb.count() > base+2 }) {
		c.cancel()
		awaitDone(t, done)
		t.Fatalf("pollLoop stopped polling under persistent 409 (count stuck at %d); it must keep retrying so it self-heals", fb.count())
	}

	c.cancel()
	awaitDone(t, done)
}
