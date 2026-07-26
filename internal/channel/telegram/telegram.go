// Package telegram is the Telegram channel for C3, implementing the
// internal/channel.Channel interface against the Bot API via gotgbot/v2.
//
// Spec §6 — cleanroom Go rewrite of what the Python POC demonstrated.
package telegram

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/mappings"
)

// longPollTimeoutSeconds is the server-side hold for getUpdates. Telegram
// allows up to 50; 25 balances latency vs connection churn. Used by
// timeoutFor to size the HTTP timeout for getUpdates calls.
const longPollTimeoutSeconds = 25

// auth401Threshold is the consecutive-401 count that trips the auth breaker.
// 10 leaves headroom for transient auth weirdness while still cutting off a
// retry-storm before it gets the bot banned.
const auth401Threshold = 10

// endpointFailoverThreshold is the consecutive-TRANSIENT-failure count that
// triggers an endpoint advance when more than one Bot-API base is configured.
// Transient failures (network/timeout/5xx) are the IP-block signature; 5 gives
// the active endpoint a fair retry budget before swapping to the next one. Only
// consulted when len(endpoints) > 1 (a single-endpoint setup never fails over).
const endpointFailoverThreshold = 5

// defaultConflictBackoffBase / defaultConflictBackoffMax bound the escalating
// backoff the poll loop applies on a 409 conflict ("another getUpdates is
// active for this token"). A 409 is USUALLY transient and self-healing: after a
// client-side long-poll timeout (flaky network/proxy), the next getUpdates can
// race Telegram's still-open prior long-poll and draw a 409 that clears within
// a few seconds. So pollLoop retries with backoff instead of exiting. The base
// is long enough for the stale long-poll to drain server-side; the cap keeps a
// GENUINE second poller (e.g. another machine) from becoming a tight retry-storm
// that Telegram could read as abuse. See pollLoop's errClassConflict case.
const (
	defaultConflictBackoffBase = 5 * time.Second
	defaultConflictBackoffMax  = 60 * time.Second
)

// telegramAPIURLEnv is the env override for the primary Bot-API base URL. It
// wins over APIBaseURL in mappings.json, matching the C3_LOG_FILE precedent
// (env beats file). Empty/unset leaves the config field untouched.
const telegramAPIURLEnv = "C3_TELEGRAM_API_URL"

// Name is the canonical channel name used in mappings.json:channels.telegram.*.
const Name = "telegram"

// Config is the channel-specific config under mappings.json:channels.telegram.
type Config struct {
	BotToken            string                          `json:"bot_token"`
	DefaultGroup        string                          `json:"default_group"`
	Groups              map[string]mappings.GroupConfig `json:"groups"`
	DMChatID            int64                           `json:"dm_chat_id"`
	MasterUserID        int64                           `json:"master_user_id"`
	DebounceMS          int                             `json:"debounce_ms"`
	DebounceMaxMessages int                             `json:"debounce_max_messages"`
	FallbackCooldownS   int                             `json:"fallback_cooldown_s"`
	STTPrefix           string                          `json:"stt_prefix"`

	// APIBaseURL is the Bot-API base the channel talks to. Empty means
	// gotgbot's default (api.telegram.org) — byte-for-byte today's behavior.
	// Telegram's IPs are null-routed in India, so the maintainer can point this
	// at a reverse proxy they OWN (e.g. a Cloudflare Worker on a custom domain).
	// The bot token is interpolated into every request path, so this must ONLY
	// ever be a maintainer-controlled host (validated https:// at Start). The
	// C3_TELEGRAM_API_URL env var overrides this field (see Start).
	APIBaseURL string `json:"api_base_url,omitempty"`
	// APIBaseURLs is an optional ordered failover list appended after
	// APIBaseURL. The poll loop advances to the next endpoint after a run of
	// transient failures (the IP-block signature) and len(endpoints) > 1.
	APIBaseURLs []string `json:"api_base_urls,omitempty"`
	// RichInbound gates decoding of inbound rich messages. nil/absent ⇒ true.
	// Bridged from mappings.ChannelConfig via host.Config (json.Marshal →
	// json.Unmarshal); the json tag MUST match the mappings side.
	RichInbound *bool `json:"rich_inbound,omitempty"`
}

// RichInboundEnabled reports whether inbound rich-message decoding is on.
// Absent config (nil) ⇒ true (decode by default).
func (c Config) RichInboundEnabled() bool {
	return c.RichInbound == nil || *c.RichInbound
}

// Channel is the Telegram channel implementation. Construct via New, register
// via the broker's channel registry.
type Channel struct {
	bot        *gotgbot.Bot
	host       channel.Host
	cfg        Config
	authBrk    *authBreaker
	offsets    *offsetStore
	dedup      *updateDedup
	editSupp   *editSuppressor // phantom-edit (content-unchanged edited_message) suppression
	rate       *rateLimiter
	sentPolls  *sentPollMap // pollID → route+owner for poll-result routing (P4)
	httpClient *http.Client // shared transport for non-gotgbot calls (file downloads)

	// endpoints is the ordered, deduped list of Bot-API base URLs the channel
	// may use. It is always non-empty: with no config it is [""], where "" means
	// gotgbot's DefaultAPIURL (api.telegram.org) — byte-for-byte today's default
	// behavior. With config it is [APIBaseURL-or-"", ...APIBaseURLs] deduped.
	// requestOptsFor reads endpoints[activeEndpoint] as the per-call APIURL, so a
	// "" entry transparently resolves to gotgbot's default via GetAPIURL.
	endpoints []string
	// activeEndpoint indexes endpoints. The poll loop advances it on a run of
	// transient failures (P2 failover) when len(endpoints) > 1; every other call
	// site reads it via requestOptsFor, so the swap is picked up on the next call.
	activeEndpoint atomic.Int32

	// conflictBackoffBase / conflictBackoffMax bound the poll loop's escalating
	// 409-conflict backoff. Zero ⇒ the default consts (defaultConflictBackoff*).
	// A test seam: the conflict-retry path can be exercised with millisecond
	// backoffs instead of the multi-second production values. Never set in
	// production code — Start leaves them zero so pollLoop uses the defaults.
	conflictBackoffBase time.Duration
	conflictBackoffMax  time.Duration

	// identityLogged guards the one-time "connected as @<name>" log emitted by
	// the heartbeat on its first successful getMe. Because boot is offline-safe
	// (DisableTokenCheck), Bot.User is "<missing>" at start, so the familiar
	// identity line is logged later, once, when Telegram is first reachable.
	identityLogged atomic.Bool

	// conflictActive is true while a getUpdates 409 conflict is the current
	// reason inbound is down (set on a 409, cleared on the first non-conflict
	// getUpdates outcome — success OR any other error). It exists because the
	// poll loop now STAYS ALIVE across a persistent 409 (see poll.go): without
	// this flag the 5-min getMe heartbeat — which never sees a 409 — would
	// RecordSuccess and falsely clear the DOWN alert while inbound is still
	// conflict-dead. recordHeartbeatSuccess consults it. Cross-goroutine
	// (pollLoop writes, heartbeat reads), hence atomic.
	conflictActive atomic.Bool

	// health is the single fetch-health state machine. It is the ONLY source
	// of "is Telegram reachable?" — it replaced the two prior competing
	// false-positive watchdogs (stallWatchdog + heartbeat's HEARTBEAT-FAILED
	// alarm). Driven from pollLoop (RecordSuccess/RecordFailure), the
	// silenceWatchdog (CheckSilence), and the heartbeat (RecordFailure on
	// getMe failure). The machine's own lastSuccess timestamp now owns
	// silence detection (the old standalone lastPollReturn atomic was
	// retired). Its edges route through the reachability combiner below (they no
	// longer call host.NotifyHealth directly) so inbound + outbound surface on
	// ONE telegram status-line entry.
	health *fetchHealth

	// reach combines health (inbound) with the outbound-health machine it OWNS
	// (reach.out — "can we SEND to Telegram after retries?") into the single
	// telegram reachability state that reaches host.NotifyHealth. It is the ONLY
	// place NotifyHealth is fired for this channel, so inbound-down and
	// outbound-down for one root cause produce ONE notification, not two. The
	// combiner owns the outbound machine (single lock, single source of truth —
	// see reachability.go); outbound is fed at exactly two failure sites (SendReply
	// transient error + readback give-up) and the shared success hook, never the
	// per-attempt recordOutboundErr (see feedOutboundFailure).
	reach *reachability

	// pollDone is closed when pollLoop returns. pollLoop now exits ONLY on
	// ctx-cancel (shutdown) — a 409 conflict no longer terminates it (it backs
	// off and retries). The silence watchdog watches this so it stops checking
	// once polling has cleanly stopped at shutdown.
	pollDone chan struct{}

	// Persisted-offset wiring (Component 2). offTrk advances the committed
	// offset only over durably-persisted (or no-op) updates. msgToUpdate maps a
	// stored inbound's MessageID back to the source update_id(s) so the broker's
	// persist callback can MarkDone the right update. It is a FIFO SLICE per
	// message_id, not a scalar: two in-flight updates can share a message_id (an
	// edited_message arriving during the original's persist window), and a scalar
	// map would overwrite — the first update would never mark done (wedge) and the
	// callback could mark a not-yet-persisted second update done (latent loss).
	// Staging appends; the persist callbacks pop-front, resolving them in stage
	// order. mu guards msgToUpdate.
	mu          sync.Mutex
	offTrk      *offsetTracker
	msgToUpdate map[int64][]int64

	ctx    context.Context
	cancel context.CancelFunc
}

// primaryBaseFromEnv applies the C3_TELEGRAM_API_URL env override to a config
// primary base: a non-empty (trimmed) env value WINS over the config value
// (env-beats-file, matching the C3_LOG_FILE precedent); an empty/unset env
// leaves the config value untouched. Pure function so the precedence is unit-
// testable without a live bot.
func primaryBaseFromEnv(cfgPrimary string) string {
	if v := strings.TrimSpace(os.Getenv(telegramAPIURLEnv)); v != "" {
		return v
	}
	return cfgPrimary
}

// New returns an unstarted Telegram Channel. The bot connection is established
// in Start; New just allocates.
func New() *Channel {
	return &Channel{}
}

// Name returns the channel identifier.
func (c *Channel) Name() string { return Name }

// Start reads config from host, creates the gotgbot.Bot, and returns once the
// channel is ready to be polled. The actual getUpdates loop launches in a
// follow-up commit; for the scaffolding pass, Start just validates the token
// via Bot.GetMe so the broker fails fast on bad config.
func (c *Channel) Start(ctx context.Context, host channel.Host) error {
	if err := host.Config(Name, &c.cfg); err != nil {
		return fmt.Errorf("telegram: read config: %w", err)
	}
	if c.cfg.BotToken == "" {
		return errors.New("telegram: bot_token missing in mappings.json:channels.telegram")
	}

	// Env override for the primary Bot-API base URL (env beats mappings.json,
	// matching the C3_LOG_FILE precedent). Empty/unset leaves cfg.APIBaseURL.
	c.cfg.APIBaseURL = primaryBaseFromEnv(c.cfg.APIBaseURL)

	// Build the ordered, deduped endpoint list. [APIBaseURL-or-"" , ...APIBaseURLs].
	// Both empty ⇒ [""] which means gotgbot's DefaultAPIURL (api.telegram.org) —
	// byte-for-byte today's behavior. Each NON-empty base is validated so a typo
	// can never send the bot token to a bad host. We never log the token or a
	// token-bearing URL; only the active endpoint's host is logged.
	endpoints, err := buildEndpoints(c.cfg.APIBaseURL, c.cfg.APIBaseURLs)
	if err != nil {
		return fmt.Errorf("telegram: %w", err)
	}
	c.endpoints = endpoints
	c.activeEndpoint.Store(0)
	if active := c.endpoints[0]; active != "" {
		// Log only the host (never the token-bearing path).
		if u, perr := url.Parse(active); perr == nil {
			host.Logf("telegram: using Bot-API endpoint host=%s (%d configured)", u.Host, len(c.endpoints))
		}
	}

	// Custom HTTP transport with explicit network-layer timeouts so a
	// half-open TCP socket (NAT timeout, mid-stream firewall drop) gets
	// surfaced as an error well before a request hangs forever. Defaults
	// in net/http are MaxIdleConns=100, no per-component timeouts —
	// fine for normal use but gives no upper bound on a stuck connection.
	//
	// Sub-agent research (2026-05-09, prior TypeScript bot + grammyjs/runner): the
	// "polling silently stops" failure mode comes from a hung getUpdates
	// where the kernel never sees a FIN. ResponseHeaderTimeout caps each
	// HTTP response-header wait; combined with the long-poll's own server
	// timeout (25s), this gives gotgbot's request-context a hard ceiling.
	// The stall watchdog (see pollLoop / stallWatchdog) is the second line
	// of defense for cases where this network-layer cap somehow doesn't fire.
	httpTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: time.Duration(longPollTimeoutSeconds+10) * time.Second,
	}

	// Custom BaseBotClient with DefaultRequestOpts set to the "send" budget
	// (20s). Per-call sites pass RequestOpts with method-specific timeouts via
	// requestOptsFor — getUpdates gets the long-poll budget, getMe gets a
	// short control budget, etc. The default catches anything we forget to
	// override and prevents falling back to gotgbot's 5s.
	//
	// APIURL on DefaultRequestOpts is the client-level fallback base (the active
	// endpoint at start). Per-call requestOptsFor sets APIURL too and takes
	// precedence (GetAPIURL: per-call → default → DefaultAPIURL), so failover
	// still works; this just ensures any call that ever forgets a per-call APIURL
	// still honors the configured proxy. "" stays gotgbot's default.
	botClient := &gotgbot.BaseBotClient{
		Client: http.Client{Transport: httpTransport},
		DefaultRequestOpts: &gotgbot.RequestOpts{
			Timeout: 20 * time.Second,
			APIURL:  c.activeEndpointURL(),
		},
	}
	// Reuse the same transport for file downloads (DownloadAttachment).
	// http.DefaultClient has Timeout: 0 (infinite) and no transport-layer
	// timeouts; relying on it would bypass the entire timeout discipline
	// the gotgbot path goes through (daemon.md §11.1-§11.2).
	c.httpClient = &http.Client{
		Transport: httpTransport,
		Timeout:   60 * time.Second, // Bot API caps at 20MB; a healthy download is seconds.
	}
	// DisableTokenCheck makes NewBot construction OFFLINE-SAFE: gotgbot otherwise
	// does a blocking GetMe at construction, so on a flaky-wake / IP-blocked
	// network the broker would fail to start at all (RegisterChannel → runDaemon
	// → exit) — the exact incident class the poll-loop 409 fix addresses, but one
	// beat earlier, before the resilient poll/heartbeat machinery even exists.
	// Unreachability is now handled by that machinery instead of aborting boot.
	// Trade-off: Bot.User is "<missing>" until the first successful call, so the
	// heartbeat logs the confirmed @username on its first success.
	bot, err := gotgbot.NewBot(c.cfg.BotToken, &gotgbot.BotOpts{
		BotClient:         botClient,
		DisableTokenCheck: true,
		RequestOpts:       c.requestOptsFor("getMe"),
	})
	if err != nil {
		return c.scrubTokenf("telegram: NewBot: %w", err)
	}
	c.bot = bot
	c.host = host
	c.authBrk = newAuthBreaker(auth401Threshold)
	c.health = newFetchHealth()
	c.reach = newReachability() // constructs its own owned outbound-health machine
	c.dedup = newUpdateDedup(2000, 5*time.Minute)
	// 48h = Telegram's user edit window; 8192 baselines ≈ a few weeks of dogfood
	// traffic across every topic, bounded to ~1 MiB.
	c.editSupp = newEditSuppressor(8192, 48*time.Hour)
	c.rate = newRateLimiter()
	c.sentPolls = newSentPollMap(2000)
	if store, sErr := newOffsetStore(Name); sErr == nil {
		c.offsets = store
	} else {
		host.Logf("telegram: offset store unavailable (%v); restarts will re-process the last 24h of updates", sErr)
	}

	// Persisted-offset tracker (Component 2): the committed offset advances only
	// over updates that are durably persisted (Append+fsync) or no-ops (gated /
	// dropped / non-message / handled command). Seed it from the persisted store
	// so a restart resumes from the last SAVED offset. The broker fires
	// SetPersistedCallback once per stored inbound; we MarkDone the source
	// update_id, advancing the contiguous prefix.
	var loaded int64
	if c.offsets != nil {
		loaded, _ = c.offsets.Load()
	}
	c.offTrk = newOffsetTracker(loaded)
	c.msgToUpdate = map[int64][]int64{}
	if bh, ok := host.(interface {
		SetPersistedCallback(func(*c3types.Inbound))
	}); ok {
		bh.SetPersistedCallback(c.onPersisted)
	}
	if bh, ok := host.(interface {
		SetPersistFailedCallback(func(*c3types.Inbound))
	}); ok {
		bh.SetPersistFailedCallback(c.onPersistFailed)
	}

	c.ctx, c.cancel = context.WithCancel(ctx)

	// Token check is deferred (offline-safe boot), so bot.Username is "<missing>"
	// here. The heartbeat logs "connected as @<name>" once it confirms identity
	// on its first successful getMe.
	host.Logf("telegram: started (token-check deferred; identity confirmed on first successful call)")

	// Register the broker-owned bot commands so they autocomplete in Telegram's
	// "/" menu (A8: menu hint only — the poll intercept needs no registration).
	// Best-effort: a failure here never blocks Start (the commands still work
	// when typed; only the menu hint is missing).
	go func() {
		// Pass nil opts, NOT &SetMyCommandsOpts{}. gotgbot's SetMyCommands does
		// `v["scope"] = opts.Scope` unconditionally whenever opts != nil, so an
		// empty opts sends scope=null — which Telegram rejects with "can't parse
		// BotCommandScope: BotCommandScope must be an Object". nil opts omits the
		// scope param entirely, so Telegram applies the default scope (what we want).
		if _, err := c.bot.SetMyCommands(
			[]gotgbot.BotCommand{
				{Command: "status", Description: "Show C3 broker + queue status"},
				{Command: "queue", Description: "List pooled queues · /queue <name> peeks one (operator)"},
				{Command: "drain", Description: "Move queued messages to a topic (operator)"},
			},
			nil,
		); err != nil {
			c.logf("telegram: setMyCommands(/status,/queue,/drain) failed (non-fatal): %v", err)
		}
	}()

	// The fetch-health machine seeds its lastSuccess to now on construction
	// (see newFetchHealth), so the first ~90s after start don't spuriously
	// trip the silence arm before any GetUpdates has returned.
	c.pollDone = make(chan struct{})

	// Start the long-poll loop in a goroutine. Returns immediately after
	// kicking off — Telegram-side processing is async from the broker's
	// startup path. Each long-lived goroutine runs under superviseLoop so a
	// panic is recovered + logged + drives health DOWN + the loop restarts,
	// instead of crashing the whole broker process (the silent-death class the
	// recovery audit flagged — a panic is even quieter than the old 409-exit).
	go func() {
		defer close(c.pollDone)
		c.superviseLoop("pollLoop", superviseRestartBackoff, c.pollLoop)
	}()
	go c.superviseLoop("silenceWatchdog", superviseRestartBackoff, c.silenceWatchdog)
	go c.superviseLoop("heartbeat", superviseRestartBackoff, c.heartbeat)
	return nil
}

// seamStageLocked appends update_id to the FIFO of in-flight update_ids awaiting
// a persist outcome for message_id. Caller holds c.mu. FIFO (append here,
// pop-front in the persist callbacks) because two updates can share a message_id
// (an edited_message arriving during the original's persist window); staging in
// order lets the callbacks resolve them in the SAME order, so the first-staged
// update is the first marked — never the overwrite bug of the old scalar map.
func (c *Channel) seamStageLocked(msgID, updateID int64) {
	c.msgToUpdate[msgID] = append(c.msgToUpdate[msgID], updateID)
}

// seamPopFrontLocked removes and returns the oldest staged update_id for
// message_id (FIFO). Caller holds c.mu. Returns (0,false) if none staged. The key
// is deleted when its slice empties so an absent key reads as "nothing staged".
func (c *Channel) seamPopFrontLocked(msgID int64) (int64, bool) {
	ids := c.msgToUpdate[msgID]
	if len(ids) == 0 {
		return 0, false
	}
	uid := ids[0]
	if len(ids) == 1 {
		delete(c.msgToUpdate, msgID)
	} else {
		c.msgToUpdate[msgID] = ids[1:]
	}
	return uid, true
}

// seamRemoveLocked removes the first staged entry equal to updateID for
// message_id. Caller holds c.mu. Used by the Emit-DROP cleanup, which must drop
// exactly the entry it JUST staged — not the front, which may belong to an
// earlier still-in-flight update sharing the message_id. Returns true if removed.
func (c *Channel) seamRemoveLocked(msgID, updateID int64) bool {
	ids := c.msgToUpdate[msgID]
	for i, id := range ids {
		if id == updateID {
			ids = append(ids[:i], ids[i+1:]...)
			if len(ids) == 0 {
				delete(c.msgToUpdate, msgID)
			} else {
				c.msgToUpdate[msgID] = ids
			}
			return true
		}
	}
	return false
}

// onPersisted is the durable-persist callback registered with the broker
// (SetPersistedCallback). When an inbound's source update is durably appended,
// resolve its update_id via the FIFO seam (pop-front) and MarkDone it, advancing
// the committed offset over it.
func (c *Channel) onPersisted(in *c3types.Inbound) {
	c.mu.Lock()
	uid, found := c.seamPopFrontLocked(in.MessageID)
	c.mu.Unlock()
	if found && c.offTrk != nil {
		c.offTrk.MarkDone(uid)
	}
}

// onPersistFailed is the durable-persist-FAILURE callback (SetPersistFailedCallback).
// A failed Append deliberately does NOT advance the offset — the update stays
// in-flight so Telegram redelivers it (loss-free). But the poll-side dedup
// recorded that update at first dispatch (5-min TTL), so without eviction the
// redelivery is dedup-skipped and the "loss-free retry" never actually retries
// (item 1). So: pop the seam entry (the redelivery re-stages it fresh) and forget
// its dedup entry so the redelivery genuinely re-dispatches + re-Appends. The
// committed offset is intentionally left un-advanced.
func (c *Channel) onPersistFailed(in *c3types.Inbound) {
	c.mu.Lock()
	uid, found := c.seamPopFrontLocked(in.MessageID)
	c.mu.Unlock()
	if found && c.dedup != nil {
		c.dedup.forget(uid)
	}
}

// superviseRestartBackoff is the pause before a panicked long-lived goroutine
// is restarted, so a deterministically-panicking loop can't tight-spin.
const superviseRestartBackoff = 2 * time.Second

// superviseLoop runs body, and if it PANICS, recovers it, logs the panic +
// stack, drives the fetch-health machine DOWN (so the operator is alerted
// out-of-band), waits restartBackoff, and re-runs body — a lightweight
// supervisor. A NORMAL return from body means it observed ctx cancel /
// shutdown, so the supervisor returns too (no restart). ctx cancellation is
// honored during the backoff. This converts an unrecovered goroutine panic —
// which in Go crashes the ENTIRE broker process (and then, with no breadcrumb
// in broker.log, a silent death) — into a logged, alerted, auto-restarted loop.
func (c *Channel) superviseLoop(name string, restartBackoff time.Duration, body func()) {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		if !c.runGuarded(name, body) {
			return // body returned normally → shutdown; stop supervising.
		}
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(restartBackoff):
		}
	}
}

// runGuarded runs body under a panic recover. On panic it returns true after
// logging the panic + stack and driving health DOWN via RecordFailure; on a
// normal return it returns false and touches nothing. Separated from
// superviseLoop so the recover semantics are unit-testable directly.
func (c *Channel) runGuarded(name string, body func()) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			buf := make([]byte, 8192)
			n := runtime.Stack(buf, false)
			c.host.Logf("telegram: %s PANIC recovered: %v\n%s", name, r, buf[:n])
			c.reportHealth(c.health.RecordFailure(name + " panic"))
		}
	}()
	body()
	return false
}

// reportHealth handles an INBOUND fetch-health transition edge. It is called
// OUTSIDE the fetchHealth lock (the Record*/Check* methods return the edge under
// their own lock; the caller then invokes this) — we never hold a state mutex
// across a notify fan-out. A healthNoChange transition is a no-op. Rather than
// firing host.NotifyHealth itself, it maps the inbound edge to a down-bool and
// routes it through the reachability combiner, which fires NotifyHealth ONLY on
// the COMBINED edge — so inbound-down and outbound-down for one root cause
// surface as ONE status-line notification. host.NotifyHealth is non-blocking
// (the broker fans out asynchronously) and the alert is delivered entirely
// out-of-band (status line + log), because Telegram is the dead path.
func (c *Channel) reportHealth(tr healthTransition) {
	if tr == healthNoChange {
		return
	}
	if c.reach == nil {
		// Bare-struct unit tests that don't wire the combiner: preserve the
		// original single-machine direct-notify behavior. Production always wires
		// c.reach in Start, so this branch is test-only.
		c.notifyInboundDirect(tr)
		return
	}
	// Combiner path (production): pass fetchHealth's FRESH AUTHORITATIVE down-bool
	// (snapshot().down) — NOT a bool derived from the transition edge — to the
	// combiner, so the last writer always reconciles inbound state and no stale
	// edge can wedge it. The combiner recomputes the COMBINED telegram state and
	// fires only on the combined edge. On recovery the wire-proof outbound reset
	// now lives INSIDE recordInbound (atomic under r.mu), so there is no separate
	// ForceReset call here.
	down, consec, _, reason, _, lastSuccess := c.health.snapshot()
	// Key the log to the AUTHORITATIVE `down`, not the transition, for honesty: a
	// concurrent edge could have flipped the machine back by the time we snapshot,
	// and we must never log "DOWN" for a machine that reads up (or vice versa).
	if down {
		c.host.Logf("telegram: INBOUND fetch DOWN — cannot reach Telegram to fetch updates (consec=%d, reason=%s).",
			consec, reason)
	} else {
		c.host.Logf("telegram: INBOUND fetch RECOVERED — Telegram reachable again (last success %s).",
			lastSuccess.Format("15:04:05"))
	}
	if fire, ev := c.reach.recordInbound(down, consec); fire {
		c.fireCombined(ev)
	}
}

// notifyInboundDirect is the legacy single-machine notify used ONLY when the
// combiner is not wired (bare-struct unit tests). It mirrors the pre-combiner
// reportHealth: build the inbound HealthEvent and fire host.NotifyHealth
// directly. Production reroutes through the combiner (see reportHealth).
func (c *Channel) notifyInboundDirect(tr healthTransition) {
	_, consec, since, reason, downFor, lastSuccess := c.health.snapshot()
	ev := c3types.HealthEvent{
		Channel: c.Name(),
		Since:   since,
		Consec:  consec,
		Reason:  reason,
		DownFor: downFor,
	}
	switch tr {
	case healthWentDown:
		ev.State = c3types.HealthStateDown
		c.host.Logf("telegram: FETCH DOWN — cannot reach Telegram to fetch updates (consec=%d, reason=%s). Inbound is offline until this recovers; surfaced on the status line.",
			consec, reason)
	case healthRecovered:
		ev.State = c3types.HealthStateUp
		c.host.Logf("telegram: FETCH RECOVERED — Telegram reachable again (last success %s).",
			lastSuccess.Format("15:04:05"))
	}
	c.host.NotifyHealth(ev)
}

// feedOutboundFailure feeds ONE outbound failure EVENT into the combiner-owned
// outbound-health machine, but ONLY for a genuine transient (network/timeout/5xx)
// failure. A permanent error (401/403/4xx — the token breaker's job), a format
// error, and a 429 rate-limit (a reachable server pushing back, mirroring
// inbound's 429 handling) are NOT outbound-DOWN signals and are skipped. It is
// called at EXACTLY two sites — the SendReply text error path and the readback
// give-up — NEVER inside the shared per-attempt recordOutboundErr (CRITIQUE FOLD
// #2: that would multi-count a single give-up's retries and defeat the downAfter
// debounce). The combiner drives the machine + recompute atomically under r.mu,
// firing host.NotifyHealth only on the COMBINED edge.
func (c *Channel) feedOutboundFailure(err error, reason string) {
	if c.reach == nil || err == nil {
		return
	}
	if class, _ := classifyError(err); class != errClassTransient {
		return
	}
	if fire, ev := c.reach.outboundFailure(reason); fire {
		c.fireCombined(ev)
	}
}

// fireCombined logs the combined telegram reachability edge and fans it out via
// host.NotifyHealth. Both the inbound and outbound routes reach it, so it is the
// SINGLE NotifyHealth call for the combiner path — preserving the broker's one
// health entry per channel name.
func (c *Channel) fireCombined(ev c3types.HealthEvent) {
	switch ev.State {
	case c3types.HealthStateDown:
		c.host.Logf("telegram: REACHABILITY DOWN — %s (consec=%d). Surfacing on the status line.",
			ev.Reason, ev.Consec)
	case c3types.HealthStateUp:
		c.host.Logf("telegram: REACHABILITY RECOVERED — Telegram reachable again; status line cleared.")
	}
	c.host.NotifyHealth(ev)
}

// silenceWatchdog drives the fetch-health machine's max-silence arm: the
// "silent death" failure mode where HTTP-layer timeouts somehow fail to fire and
// GetUpdates hangs past the long-poll budget, producing neither a success nor a
// fast error. It replaced the old observe-and-log-only stallWatchdog: instead of
// emitting a separate "STALL DETECTED" line (a SECOND competing dead-bot
// signal), it folds the 90s threshold into the ONE health machine via
// CheckSilence and routes any resulting edge through the same reportHealth
// notification path. Patterned after grammyjs/runner's POLL_STALL_THRESHOLD_MS
// (sub-agent research 2026-05-09).
const silenceCheckInterval = 30 * time.Second

func (c *Channel) silenceWatchdog() {
	ticker := time.NewTicker(silenceCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.pollDone:
			// pollLoop exited (ctx cancel / shutdown — a 409 conflict no longer
			// ends it). The silence concept doesn't apply anymore; stop checking.
			return
		case <-ticker.C:
			c.reportHealth(c.health.CheckSilence())
		}
	}
}

// heartbeat pings getMe at a fixed interval as an independent liveness
// probe. If the bot is "silently dead" (Telegram-side rotated us off, or
// our token revoked, or our network broke in a way pollLoop hasn't
// surfaced), this catches it within a few minutes regardless of whether
// any users are sending messages.
//
// Single-notification-path change (2026-06-17): the heartbeat no longer keeps
// its OWN consecutive-fail count or emits a separate "HEARTBEAT FAILED" line —
// that was a second competing dead-bot signal that produced false-positive
// spam alongside the poll loop. Instead it feeds the SAME fetch-health machine:
//   - getMe error => health.RecordFailure (a transport-class failure to reach
//     Telegram, EXCEPT a 429, which is the server pushing back — reachable, so
//     it is NOT recorded as down), and
//   - getMe success => health.RecordSuccess (proof Telegram is reachable),
//
// routing any edge through the same reportHealth fan-out.
const heartbeatInterval = 5 * time.Minute

func (c *Channel) heartbeat() {
	// Wait one full interval before the first probe so startup races
	// don't cause spurious early failures.
	timer := time.NewTimer(heartbeatInterval)
	defer timer.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-timer.C:
		}
		me, err := c.bot.GetMe(&gotgbot.GetMeOpts{
			RequestOpts: c.requestOptsFor("getMe"),
		})
		if err == nil && me != nil && c.identityLogged.CompareAndSwap(false, true) {
			// First reachable getMe — confirm the bot identity (deferred from
			// boot by DisableTokenCheck). Preserves the familiar log signal.
			c.host.Logf("telegram: connected as @%s (identity confirmed)", me.Username)
		}
		if err != nil {
			class, _ := classifyError(err)
			// 429 is a reachable server pushing back — never "down". Every
			// other class (transient/permanent/conflict) means we couldn't
			// complete a control call, which feeds the health machine.
			if class != errClassRateLimited {
				c.logf("telegram: heartbeat getMe failed (class=%s): %v", class, err)
				c.reportHealth(c.health.RecordFailure("heartbeat getMe " + class.String()))
			} else {
				c.logf("telegram: heartbeat getMe 429 rate-limited (reachable; not counted as down): %v", err)
			}
		} else {
			c.recordHeartbeatSuccess()
		}
		timer.Reset(heartbeatInterval)
	}
}

// recordHeartbeatSuccess records a getMe success into the health machine —
// EXCEPT while a getUpdates 409 conflict is active. getMe never returns a 409
// (conflict is exclusive to getUpdates / setWebhook), so a getMe success proves
// only "Telegram reachable + token valid", NOT "inbound is flowing". Since the
// poll loop now stays alive across a persistent 409, clearing DOWN here would
// falsely signal RECOVERED while inbound is still conflict-dead. So when a
// conflict is active we log and leave the DOWN alert standing; only a real
// getUpdates success (which clears conflictActive) recovers it.
func (c *Channel) recordHeartbeatSuccess() {
	if c.conflictActive.Load() {
		c.host.Logf("telegram: heartbeat getMe ok, but a getUpdates 409 conflict is active — NOT clearing DOWN (inbound still conflicted)")
		return
	}
	c.reportHealth(c.health.RecordSuccess())
}

// Stop halts the polling loop and shuts down the bot.
func (c *Channel) Stop() error {
	if c.cancel != nil {
		c.cancel()
	}
	return nil
}

// Outbound tool implementations live in outbound.go.
