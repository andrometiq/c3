package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/karthikeyan5/c3/internal/c3types"
	"github.com/karthikeyan5/c3/internal/channel"
	"github.com/karthikeyan5/c3/internal/ipc"
)

// BrokerHost is the broker's concrete implementation of channel.Host.
// One Host is created per channel registration.
//
// Plan 4A scope: scaffold for Plan 4B (Telegram). Plan 5 adds plugin.Host
// concrete impl with hook chain + plugin tools.
type BrokerHost struct {
	broker  *Broker
	channel string // channel name (e.g. "telegram")
}

// NewBrokerHost binds a Host to a (broker, channel-name) pair.
func NewBrokerHost(b *Broker, chanName string) *BrokerHost {
	return &BrokerHost{broker: b, channel: chanName}
}

// Compile-time check: BrokerHost implements channel.Host.
var _ channel.Host = (*BrokerHost)(nil)

// Config marshals mappings.json:channels.<name> via JSON-roundtrip into target.
// Returns error if the channel section is missing.
func (h *BrokerHost) Config(name string, target any) error {
	if h.broker.Mappings() == nil || h.broker.Mappings().Channels == nil {
		return fmt.Errorf("broker host: no channels in mappings.json")
	}
	cc, ok := h.broker.Mappings().Channels[name]
	if !ok {
		return fmt.Errorf("broker host: channel %q not in mappings.json", name)
	}
	data, err := json.Marshal(cc)
	if err != nil {
		return fmt.Errorf("broker host: marshal channel config: %w", err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("broker host: unmarshal channel config: %w", err)
	}
	return nil
}

// Emit submits an inbound to the per-route worker pool. The worker drains
// the pipeline (STT, OnInbound chain, debounce, forward to claimed stub).
//
// Returns true when the inbound was accepted onto the worker queue. A false
// return means the route worker is saturated (cap 64) or stopped and this message
// has NOT been persisted anywhere.
//
// This used to be a capacity DROP: the caller marked the update done so the
// contiguous-prefix offset advanced past it (I4), trading one lost message for not
// wedging all inbound on a burst. That is silent, unrecoverable loss of a message
// the user sent — the exact thing the durable queue exists to prevent — and it was
// invisible outside broker.log.
//
// Now: SubmitWait absorbs a momentary burst (submitGraceWindow), and if the route
// is still saturated the caller HOLDS the Telegram offset so the message is
// redelivered instead of destroyed. Telegram retains unacknowledged updates, so
// the message survives; the redelivery loop is paced by pollIdleBackoff.
// (v0.1.0 release audit, 2026-07-25 — maintainer's call on the I4 trade-off.)
func (h *BrokerHost) Emit(in *c3types.Inbound) bool {
	if in == nil {
		return false
	}
	key := MakeRouteKey(in.Channel, in.ChatID, in.TopicID)
	if !h.broker.Workers.SubmitWait(key, Job{Kind: JobInbound, Inbound: in}) {
		log.Printf("emit SATURATED chan=%s chat=%d topic=%s msg=%d: worker queue full after %s — HOLDING the offset so Telegram redelivers (not dropped)",
			in.Channel, in.ChatID, TopicPtrStr(in.TopicID), in.MessageID, submitGraceWindow)
		return false
	}
	return true
}

// Logf writes to the broker's structured log (currently stdlib log).
func (h *BrokerHost) Logf(format string, args ...any) {
	log.Printf(format, args...)
}

// SetPersistedCallback delegates to the broker so a channel that holds a
// persisted-offset tracker (telegram) can be notified after each inbound is
// durably stored. The telegram channel discovers this via an interface
// type-assertion on its host at Start; it is not part of the channel.Host
// interface (only the telegram channel needs it).
func (h *BrokerHost) SetPersistedCallback(fn func(in *c3types.Inbound)) {
	h.broker.SetPersistedCallback(fn)
}

// SetPersistFailedCallback delegates to the broker so the telegram channel can be
// notified when an inbound's durable Append FAILED (item 1: evict the poll-side
// dedup entry so the held offset's redelivery genuinely retries). Discovered via
// an interface type-assertion at Start, like SetPersistedCallback.
func (h *BrokerHost) SetPersistFailedCallback(fn func(in *c3types.Inbound)) {
	h.broker.SetPersistFailedCallback(fn)
}

// Done returns the broker's shutdown channel.
func (h *BrokerHost) Done() <-chan struct{} {
	return h.broker.ctx.Done()
}

// GateInbound runs the inbound through the broker's allowlist + pairing
// gate. The channel layer calls this before Emit; only GateInboundAllow
// proceeds downstream. See internal/broker/pairing.go.
func (h *BrokerHost) GateInbound(in *c3types.Inbound) channel.GateInboundDecision {
	switch h.broker.Gate(in) {
	case GateAllow:
		return channel.GateInboundAllow
	case GatePairConsumed:
		return channel.GateInboundPairConsumed
	default:
		return channel.GateInboundDrop
	}
}

func (h *BrokerHost) NotifyHealth(ev c3types.HealthEvent) {
	// --- Ambient tier: always on, synchronous, never gated. ---
	// Health edges surface ONLY on the ambient status line — no desktop popup,
	// no CLI in-session broadcast (removed 2026-07-07 per maintainer).
	// (c) status cache for `c3-broker status`.
	h.broker.setLastHealth(ev)

	// (d) broker log — one loud edge line.
	if ev.State == c3types.HealthStateDown {
		log.Printf("HEALTH chan=%s state=DOWN since=%s consec=%d reason=%q — inbound offline; surfaced on the status line",
			ev.Channel, ev.Since.Format("15:04:05"), ev.Consec, ev.Reason)
	} else {
		log.Printf("HEALTH chan=%s state=UP (recovered, was down %s) — inbound restored",
			ev.Channel, ev.DownFor.Round(time.Second))
	}

	// (e) status file the Claude Code status line reads.
	h.broker.WriteHealthFile()
}

// broadcastSystemEvent writes a broker-originated system InboundEvent to EVERY
// live CLI session. Whichever CLI the user is looking at sees the advisory.
//
// SECURITY BOUNDARY (explicit): this BYPASSES the inbound allowlist gate
// (host.GateInbound / broker.Gate) and the per-route worker pool / debounce.
// That bypass is sound ONLY because the event is BROKER-ORIGINATED and TRUSTED
// — it carries no user content and is not user input, so the default-deny
// allowlist (which exists to keep STRANGERS' messages out) does not apply. This
// path must NEVER be used to deliver anything user-sourced; user inbound ALWAYS
// goes through host.Emit → GateInbound. The producers of these events are
// broker-internal advisories (currently the update-restart notice,
// notifyUpdateRestart in update.go); health edges no longer broadcast — they
// surface only on the ambient status line.
//
// Delivery is a direct write to each alive stub's conn (mirrors the worker's
// forwardOrFallback write), best-effort per conn: a failed write to one session
// is logged and skipped, never fatal.
func (b *Broker) broadcastSystemEvent(sysev *c3types.SystemEvent) {
	if sysev == nil {
		return
	}
	in := c3types.Inbound{
		Channel: sysev.Source,
		Kind:    c3types.InboundSystem,
		Event:   &c3types.InboundEvent{System: sysev},
		// No ChatID/Sender/Text — this is broker-originated, not a routed
		// user message.
	}
	delivered := 0
	for _, s := range b.Stubs.Snapshot() {
		// Skip the transient CLI clients (status/topics/etc.) — they're not
		// long-lived agent sessions and close immediately.
		if s.CLI == "c3-broker-cli" {
			continue
		}
		conn, ok := s.ConnValue().(*ipc.Conn)
		if !ok || conn == nil {
			continue
		}
		if err := conn.WriteJSON(ipc.InboundMsg{Op: ipc.OpInbound, Inbound: in}); err != nil {
			log.Printf("health-broadcast: write to cli=%s pid=%d conn=%d failed: %v",
				s.CLI, s.PID, s.ConnID, err)
			continue
		}
		delivered++
	}
	log.Printf("health-broadcast: system advisory %q delivered to %d live CLI session(s)",
		sysev.Title, delivered)
}

// channelRegistration entries inside the broker.
type channelRegistration struct {
	Channel channel.Channel
	Host    *BrokerHost
}
