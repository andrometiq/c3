package telegram

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"
)

// errClass tags a Telegram error so the polling loop and outbound call sites
// can pick the right recovery: backoff, sleep retry-after, exit cleanly, or
// trip a circuit-breaker.
//
// Adapted from a prior TypeScript Telegram bot's
// `extensions/telegram/src/fetch.ts` error classification — they're TS+grammy,
// we're Go+gotgbot, but the shape of
// "what telegram errors mean and how to react" is the same.
type errClass int

const (
	errClassNone        errClass = iota // success / nil
	errClassPermanent                   // 401, 403, other 4xx — won't fix on retry
	errClassRateLimited                 // 429 with retry_after seconds
	errClassConflict                    // 409 — another poller holds this token
	errClassTransient                   // network / timeout / 5xx — retry with backoff
)

func (c errClass) String() string {
	switch c {
	case errClassPermanent:
		return "permanent"
	case errClassRateLimited:
		return "rate_limited"
	case errClassConflict:
		return "conflict"
	case errClassTransient:
		return "transient"
	default:
		return "none"
	}
}

// classifyError returns the error class and (for rate-limited) the
// retry_after value Telegram requested in seconds.
func classifyError(err error) (errClass, int) {
	if err == nil {
		return errClassNone, 0
	}
	var tg *gotgbot.TelegramError
	if errors.As(err, &tg) {
		switch tg.Code {
		case 401, 403:
			return errClassPermanent, 0
		case 409:
			return errClassConflict, 0
		case 429:
			ra := 0
			if tg.ResponseParams != nil {
				ra = int(tg.ResponseParams.RetryAfter)
			}
			return errClassRateLimited, ra
		}
		// Other 4xx: bad request, chat-not-found, etc. Permanent — retrying
		// won't help.
		if tg.Code >= 400 && tg.Code < 500 {
			return errClassPermanent, 0
		}
		// 5xx: Telegram's problem, keep trying.
		return errClassTransient, 0
	}
	// Non-TelegramError: usually network / context-deadline / dns. All
	// transient by default — better to retry an unknown than to crash.
	if isTransientNetworkError(err) {
		return errClassTransient, 0
	}
	return errClassTransient, 0
}

// isTransientNetworkError matches the kinds of errors gotgbot+net/http surface
// when the wire is flaky. Inspired by the predecessor bot's
// FALLBACK_RETRY_ERROR_CODES (`extensions/telegram/src/fetch.ts`).
func isTransientNetworkError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	s := err.Error()
	for _, needle := range []string{
		"connection refused",
		"connection reset",
		"no such host",
		"i/o timeout",
		"broken pipe",
		"EOF",
		"server closed",
		"use of closed network connection",
	} {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}

// timeoutFor picks the per-method HTTP timeout. Long-poll gets the budget for
// its server-side hold + slack; control calls (getMe, setMyCommands, etc.)
// get a short budget; sends/edits get medium; downloads get long.
//
// Generalizes the lesson from the 2026-05-09 polling-timeout bug: a single
// global HTTP timeout either starves the long-poll or makes everything else
// hang. Per-method matches the predecessor bot's `bot-core.ts` resolveTelegramRequestTimeoutMs.
func timeoutFor(method string, longPollSeconds int) time.Duration {
	switch method {
	case "getUpdates":
		// Server holds up to longPollSeconds, then needs additional time to
		// flush the response. 30s slack covers high-latency networks. The
		// 2026-05-09 fix used 10s slack and still hit occasional timeouts.
		return time.Duration(longPollSeconds)*time.Second + 30*time.Second
	case "getMe", "deleteWebhook", "setMyCommands", "setWebhook", "logOut", "close":
		return 10 * time.Second
	case "getFile":
		return 30 * time.Second
	default:
		// Sends, edits, reactions, typing, forum-topic CRUD — all should
		// complete in a couple seconds normally; 20s buys slack for slow
		// networks without making failures hang.
		return 20 * time.Second
	}
}

// requestOptsFor returns gotgbot.RequestOpts with the per-method timeout AND the
// active Bot-API base URL filled in. It is the SINGLE chokepoint for both the
// timeout discipline (never fall back to gotgbot's 5s DefaultTimeout) and the
// configurable/failover endpoint: it sets APIURL to endpoints[activeEndpoint],
// where "" transparently means gotgbot's DefaultAPIURL (api.telegram.org). Per
// GetAPIURL precedence (per-call APIURL → DefaultRequestOpts → DefaultAPIURL),
// this per-call value wins, so a mid-run endpoint advance is picked up on the
// very next call. Every outbound + poll site MUST use this method.
func (c *Channel) requestOptsFor(method string) *gotgbot.RequestOpts {
	return &gotgbot.RequestOpts{
		Timeout: timeoutFor(method, longPollTimeoutSeconds),
		APIURL:  c.activeEndpointURL(),
	}
}

// APIBaseURL is activeEndpointURL exported for out-of-package callers that must
// talk to the SAME Bot-API host this channel is currently using — today the STT
// plugin, whose handler performs its own getFile. Reading api_base_url from
// mappings instead would ignore both C3_TELEGRAM_API_URL and any failover
// advance, so a probe that succeeded here could be followed by a fetch against a
// different, unreachable endpoint (Codex review 2, finding 1). "" means the
// gotgbot default base, api.telegram.org.
func (c *Channel) APIBaseURL() string { return c.activeEndpointURL() }

// activeEndpointURL returns the current Bot-API base, "" meaning gotgbot's
// default. It is defensive against an unbuilt endpoints slice (e.g. a *Channel
// constructed directly in a unit test, bypassing Start): an empty list resolves
// to "" (the default), never panicking on an out-of-range index.
func (c *Channel) activeEndpointURL() string {
	if len(c.endpoints) == 0 {
		return ""
	}
	i := int(c.activeEndpoint.Load())
	if i < 0 || i >= len(c.endpoints) {
		return ""
	}
	return c.endpoints[i]
}

// maybeFailover advances the active Bot-API endpoint when the consecutive
// transient-failure streak (*consec) has reached endpointFailoverThreshold AND
// more than one endpoint is configured. On advance it rotates activeEndpoint to
// the next base, logs ONCE (host only, never the token), and resets *consec.
// With a single endpoint configured it is a no-op (nothing to fail over to).
// Driven from the poll loop, which owns the transient-failure count. The new
// endpoint is picked up automatically by every requestOptsFor on the next call.
func (c *Channel) maybeFailover(consec *int) {
	if len(c.endpoints) <= 1 || *consec < endpointFailoverThreshold {
		return
	}
	cur := c.activeEndpoint.Load()
	next := (cur + 1) % int32(len(c.endpoints))
	c.activeEndpoint.Store(next)
	*consec = 0
	c.host.Logf("telegram: endpoint failover after %d consecutive transient failures — advancing to endpoint %d/%d (%s)",
		endpointFailoverThreshold, next+1, len(c.endpoints), endpointHostLabel(c.endpoints[next]))
}

// endpointHostLabel renders an endpoint for logging WITHOUT ever exposing the
// token (the base never contains it, but parse defensively). "" → "default".
func endpointHostLabel(base string) string {
	if base == "" {
		return "default (api.telegram.org)"
	}
	if u, err := url.Parse(base); err == nil && u.Host != "" {
		return u.Host
	}
	return "configured"
}

// buildEndpoints assembles the ordered, deduped Bot-API base list from the
// primary base + an optional failover list. The result is ALWAYS non-empty:
//   - both empty ⇒ [""], where "" means gotgbot's DefaultAPIURL — byte-for-byte
//     today's default behavior, no validation performed.
//   - otherwise ⇒ [primary-or-"" , ...extras], each NON-empty base validated and
//     deduped (first occurrence wins, order preserved).
//
// Validation: every non-empty base must url.Parse cleanly and be https://,
// EXCEPT http:// is allowed ONLY for an explicit localhost / 127.0.0.1 host (a
// local reverse proxy). Anything else is rejected — the bot token is
// interpolated into the request path, so a typo must never send it to a bad
// host. We never return or log the token; errors name only the offending base.
func buildEndpoints(primary string, extras []string) ([]string, error) {
	primary = strings.TrimSpace(primary)
	raw := make([]string, 0, 1+len(extras))
	raw = append(raw, primary) // may be "" (= gotgbot default)
	for _, e := range extras {
		raw = append(raw, strings.TrimSpace(e))
	}

	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, base := range raw {
		if base == "" {
			if seen[""] {
				continue
			}
			seen[""] = true
			out = append(out, "") // gotgbot default; nothing to validate
			continue
		}
		if err := validateAPIBaseURL(base); err != nil {
			return nil, err
		}
		// Dedup on the NORMALIZED (trailing-slash-trimmed) form so
		// "https://x/" and "https://x" collapse to a single endpoint.
		norm := strings.TrimSuffix(base, "/")
		if seen[norm] {
			continue
		}
		seen[norm] = true
		out = append(out, norm)
	}
	if len(out) == 0 {
		// Defensive: raw always has at least the primary, so this can't happen,
		// but never return an empty slice (requestOptsFor indexes [0]).
		out = append(out, "")
	}
	return out, nil
}

// validateAPIBaseURL rejects a base URL that could leak the bot token to a bad
// host. It must parse, carry a host, and be https:// — with the single
// exception of http:// for an explicit localhost / 127.0.0.1 host (local
// reverse proxy). The error names only the base (never the token).
func validateAPIBaseURL(base string) error {
	u, err := url.Parse(base)
	if err != nil {
		return errors.New("invalid api_base_url " + strconv.Quote(base) + ": " + err.Error())
	}
	if u.Host == "" {
		return errors.New("invalid api_base_url " + strconv.Quote(base) + ": missing host")
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		host := u.Hostname()
		if host == "localhost" || host == "127.0.0.1" || host == "::1" {
			return nil
		}
		return errors.New("refusing api_base_url " + strconv.Quote(base) +
			": http:// is only allowed for a localhost reverse proxy (the bot token must not transit a non-TLS remote host)")
	default:
		return errors.New("refusing api_base_url " + strconv.Quote(base) +
			": scheme must be https:// (got " + strconv.Quote(u.Scheme) + ")")
	}
}

// authBreaker tracks consecutive 401s on the channel. Once `threshold` is
// reached, the breaker trips and the channel suspends polling — preventing a
// retry-storm against a revoked token (which Telegram can interpret as abuse
// and respond to by banning the bot, per the predecessor bot's
// `sendchataction-401-backoff.ts` warning).
type authBreaker struct {
	mu        sync.Mutex
	consec    int
	threshold int
	tripped   bool
}

func newAuthBreaker(threshold int) *authBreaker {
	return &authBreaker{threshold: threshold}
}

// RecordFail increments the consecutive-failure counter. Returns whether the
// breaker is now tripped.
func (b *authBreaker) RecordFail() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consec++
	if b.consec >= b.threshold {
		b.tripped = true
	}
	return b.tripped
}

// RecordSuccess clears any consecutive-failure state.
func (b *authBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consec = 0
	b.tripped = false
}

// IsTripped returns whether the breaker is currently tripped.
func (b *authBreaker) IsTripped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tripped
}

// Consec returns the current consecutive-failure count (diagnostic).
func (b *authBreaker) Consec() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.consec
}

// recordOutboundErr classifies an outbound call's error and feeds the auth
// breaker. Called from every Send/Edit/React/etc. on err. Logs loud on
// permanent (401/403/4xx) so the user sees the underlying problem; transient
// errors are quieter (the agent already sees them surface as tool errors).
//
// 429 retry_after is logged but NOT auto-retried at this layer — the agent
// can decide whether to retry based on the error.
func (c *Channel) recordOutboundErr(err error) {
	class, retryAfter := classifyError(err)
	switch class {
	case errClassPermanent:
		tripped := c.authBrk.RecordFail()
		c.logf("telegram: outbound permanent error (consec=%d, tripped=%v): %v",
			c.authBrk.Consec(), tripped, err)
	case errClassRateLimited:
		c.host.Logf("telegram: outbound 429 rate-limited; retry_after=%ds", retryAfter)
	case errClassConflict:
		c.logf("telegram: outbound 409 CONFLICT (unexpected on outbound): %v", err)
	}
}

// recordOutboundSuccess clears the auth breaker. Any successful API call is
// proof the token still works. It ALSO drives the outbound-health machine's
// success (through the combiner, so it fires a RECOVERED notification only on
// the combined edge). Feeding SUCCESS via this shared hook is safe (CRITIQUE
// FOLD #2) — only the FAILURE feed must avoid the shared per-attempt path.
func (c *Channel) recordOutboundSuccess() {
	if c.authBrk != nil {
		c.authBrk.RecordSuccess()
	}
	if c.reach != nil {
		if fire, ev := c.reach.outboundSuccess(); fire {
			c.fireCombined(ev)
		}
	}
}
