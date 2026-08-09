package broker

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/plugin"
	"github.com/Andrometiq/c3/internal/proctree"
	"github.com/Andrometiq/c3/internal/queue"
)

// Broker holds the in-memory state shared by all connections: stubs registry,
// routes table, worker pool, channel registry, fallback tracker, and a
// copy-on-write atomic pointer to the mappings.json config snapshot.
//
// Concurrency model for the mappings pointer:
//   - Readers call Mappings() — atomic Load, lock-free, always returns a
//     consistent immutable snapshot.
//   - Writers (UpsertTopic/UpsertMapping during attach, SetMappings on
//     SIGHUP reload) go through mutateMappings(), which holds mutationMu
//     across a Clone → mutate → Store cycle. Concurrent mutations
//     serialize; readers never see a half-updated state.
//
// Previously Mappings was a public *MappingsFile field accessed directly
// by ~20 call sites across attach/handler/worker. Concurrent
// HandleConn goroutines mutated the inner maps via UpsertTopic while
// other goroutines iterated them via range — classic Go data race
// (BLOCKER, code-review 2026-05-15).
type Broker struct {
	Stubs     *StubRegistry
	Routes    *Routes
	Workers   *WorkerPool
	Fallbacks *fallbackTracker
	// HeldNotices throttles the "held — nothing lost" auto-reply on edit-capable
	// channels to one per route per short window (defaultHeldNoticeCooldown) so a
	// burst of holds coalesces into a single notice instead of a flood (msg 6083).
	HeldNotices *fallbackTracker
	Plugins     *PluginHost
	Pairing     *pairingState

	// Asks is the registry of in-flight `ask` round-trips (blocking,
	// correlated question→answer over an inline keyboard). Registered before the
	// question is sent (fast-tap race) and resolved on the route worker goroutine
	// when the human taps. See ask.go.
	Asks *askRegistry

	// Perms is the registry of in-flight permission relays (Claude Code tool-use
	// prompts surfaced as Allow/Deny keyboards). Fire-and-forget: registered before
	// the keyboard is sent and resolved on the route worker goroutine when the
	// operator taps. See perm.go.
	Perms *permRegistry

	// deliveryTokenPrefix is random per broker lifetime; deliveryTokenSeq makes
	// each tracked live push unique within that lifetime. The prefix is
	// load-bearing: after a broker restart, a stale adapter ack must never collide
	// with a fresh push's token and consume its durable record.
	deliveryTokenPrefix string
	deliveryTokenSeq    atomic.Uint64

	// Queue is the durable per-route inbound hold buffer. All file ops for a
	// route are funneled through that route's RouteWorker goroutine (single
	// owner ⇒ no file locks). May be nil if queue init failed at New (durable
	// hold disabled for the run, logged loudly) — callers must nil-check.
	Queue *queue.Store
	Voice *VoiceScheduler

	// voiceRecoveryOnce runs the store-wide pending-voice scan after the first
	// channel has registered, when both plugin callbacks and channel cache/fetch
	// capabilities are available. C3 has one channel today; the scan itself is
	// store-wide so future channel rows are not hidden.
	voiceRecoveryOnce sync.Once

	// drains is the per-SOURCE in-flight drain guard (drain.go, amendment A1/B8):
	// a second Drain on a source that is already mid-drain rejects immediately
	// (DrainInProgressError) instead of interleaving a double-copy. Keyed by the
	// source's canonical queue file key; the zero value is ready to use.
	drains drainLocks

	mappings   atomic.Pointer[mappings.MappingsFile]
	mutationMu sync.Mutex

	// persistedCB is invoked (best-effort, off the hot path is fine) when an
	// inbound's source update_id has been durably appended to the queue. The
	// telegram channel registers this to advance its persisted-offset tracker.
	// nil ⇒ no-op (non-telegram / unit tests).
	persistedMu sync.RWMutex
	persistedCB func(in *c3types.Inbound)
	// persistFailedCB mirrors persistedCB for the FAILURE case: invoked when an
	// inbound's durable Append FAILED so the telegram channel can evict that
	// update's poll-side dedup entry and let the held Telegram offset redeliver +
	// genuinely retry (item 1). Guarded by the same persistedMu. nil ⇒ no-op.
	persistFailedCB func(in *c3types.Inbound)

	ctx    context.Context
	cancel context.CancelFunc

	chMu     sync.RWMutex
	channels map[string]*channelRegistration

	// desktopNotifier raises a local desktop popup for a channel-health edge.
	// Snapshotted once at broker start (the launching shell carries the desktop
	// session env). One of the out-of-band health sinks; never blocks/crashes.
	// An interface so tests can inject a fake (real impl: *desktopNotifier).
	// desktop notifications removed 2026-07-07 per maintainer; retained dormant, health surfaces only on the status line
	desktopNotifier healthNotifier

	// healthMu guards lastHealth — the most recent HealthEvent per channel,
	// cached so `c3-broker status` can render a "Channel health:" line. Updated
	// in NotifyHealth on every edge; read by handleHealth.
	healthMu   sync.RWMutex
	lastHealth map[string]c3types.HealthEvent

	// sessionPIDResolver maps a registered stub's PID to the real CLI
	// session pid by walking up the /proc tree (defaults to
	// proctree.CLISessionPID). A Claude stub registers under its ADAPTER's
	// pid (comm "c3-claude-adapt"); the slash-command caller resolves the
	// real claude ancestor pid. This resolver bridges that gap in the ping /
	// sessions PID-match. Injectable so handler tests can supply a synthetic
	// process tree without a real /proc.
	sessionPIDResolver func(int) int

	// updateMu guards the always-on "a newer C3 release exists" state, set by the
	// update checker (update.go) and read by WriteHealthFile (to surface the
	// status-line notice) and `c3-broker status`. Independent of the auto_update
	// toggle — the notice fires regardless.
	updateMu        sync.RWMutex
	updateAvailable bool
	latestVersion   string
}

const defaultWorkerIdle = 60 * time.Second

// New returns a Broker with empty registries and the given mappings config.
func New(mf *mappings.MappingsFile) *Broker {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		Stubs:               NewStubRegistry(),
		Routes:              NewRoutes(),
		Fallbacks:           newFallbackTracker(defaultFallbackCooldown),
		HeldNotices:         newFallbackTracker(defaultHeldNoticeCooldown),
		Pairing:             newPairingState(),
		Asks:                newAskRegistry(),
		Perms:               newPermRegistry(),
		deliveryTokenPrefix: rand.Text(),
		ctx:                 ctx,
		cancel:              cancel,
		channels:            map[string]*channelRegistration{},
		desktopNotifier:     newDesktopNotifier(), // desktop notifications removed 2026-07-07 per maintainer; retained dormant, health surfaces only on the status line
		lastHealth:          map[string]c3types.HealthEvent{},
		sessionPIDResolver:  proctree.CLISessionPID,
	}
	b.mappings.Store(mf)
	// Durable inbound queue. A queue init failure must NOT stop the broker (it
	// degrades to the old in-memory-only path), but log loudly so the operator
	// knows durable hold is disabled for this run.
	if q, err := queue.NewStore(queue.QueueDir()); err != nil {
		log.Printf("queue: init failed (%v) — durable inbound hold DISABLED for this run", err)
		b.Queue = nil
	} else {
		if rerr := q.RecoverOnStartup(); rerr != nil {
			log.Printf("queue: recovery scan failed: %v", rerr)
		}
		b.Queue = q
	}
	b.Workers = NewWorkerPool(ctx, defaultWorkerIdle, b)
	b.Plugins = newPluginHost(b)
	b.Voice = newVoiceScheduler(b, nil)
	return b
}

func (b *Broker) mintDeliveryToken() string {
	return fmt.Sprintf("%s-%x", b.deliveryTokenPrefix, b.deliveryTokenSeq.Add(1))
}

// Mappings returns a read-only snapshot of the current mappings file.
// Always returns a consistent (atomically-loaded) pointer; callers may
// read fields and iterate inner maps without locking. NEVER mutate the
// returned struct — use mutateMappings for that.
func (b *Broker) Mappings() *mappings.MappingsFile {
	return b.mappings.Load()
}

// mutateMappings applies fn to a cloned snapshot and atomically swaps in
// the result. Serializes against other mutations; readers proceed
// concurrently against the old snapshot until the Store completes.
func (b *Broker) mutateMappings(fn func(*mappings.MappingsFile)) {
	b.mutationMu.Lock()
	defer b.mutationMu.Unlock()
	current := b.mappings.Load()
	next := current.Clone()
	fn(next)
	b.mappings.Store(next)
}

// BuiltinPlugin describes one compiled-in plugin: a Name (used for log
// lines and to scope mappings.json:plugins.<name>) and a Register
// function the broker invokes with the plugin Host at startup.
// The broker's main() builds the slice of BuiltinPlugin and passes it
// to RegisterBuiltinPlugins.
type BuiltinPlugin struct {
	Name     string
	Register func(plugin.Host) error
}

// RegisterBuiltinPlugins calls each builtin plugin's Register with the
// broker's plugin host. Should be called by main() after the broker is
// constructed but BEFORE channels are registered (so plugins are subscribed
// before any inbound flows).
func (b *Broker) RegisterBuiltinPlugins(builtins []BuiltinPlugin) error {
	for _, bp := range builtins {
		if err := bp.Register(b.Plugins); err != nil {
			return fmt.Errorf("plugin %s: %w", bp.Name, err)
		}
	}
	return nil
}

// RegisterChannel adds a channel to the broker. The channel is started
// (which validates config and connects to the upstream API) before the
// registration is recorded — if Start fails, no registration happens.
// A name that is already registered is REFUSED (error, nothing started):
// registering a second channel under one name is a duplicate, never a replace.
func (b *Broker) RegisterChannel(ch channel.Channel) error {
	b.chMu.Lock()
	_, dup := b.channels[ch.Name()]
	b.chMu.Unlock()
	if dup {
		// Never displace: the incumbent is already polling. Overwriting the map
		// entry would leave it live, unreachable via Channel() and invisible to
		// ShutdownWithin — so it is never Stop()ed and keeps issuing getUpdates
		// on its token. Two pollers on one token is the 409-conflict wedge
		// paths.go:10-22 records for the two-broker case, reproduced inside one
		// process, while RegisterChannel returns nil. Refuse instead.
		return fmt.Errorf("broker: channel %q already registered", ch.Name())
	}
	host := NewBrokerHost(b, ch.Name())
	if err := ch.Start(b.ctx, host); err != nil {
		return fmt.Errorf("broker: start channel %q: %w", ch.Name(), err)
	}
	b.chMu.Lock()
	b.channels[ch.Name()] = &channelRegistration{Channel: ch, Host: host}
	b.chMu.Unlock()
	b.voiceRecoveryOnce.Do(b.Voice.RecoverPending)
	// Degraded mode is announced HERE, not in New(): New() has no channel to speak
	// through and no session to speak to, so its loud log line is the only trace —
	// and a log nobody tails is exactly how this stayed silent. This is the first
	// moment an operator-visible surface exists.
	if b.Queue == nil {
		b.announceQueueDegraded(ch)
	}
	return nil
}

// announceQueueDegraded tells the operator, once per channel per broker run, that
// the durable inbound queue is disabled and messages arriving with no session
// attached are destroyed (see queueDisabledWarning in fallback.go for the
// mechanism). It invents no new notification path: it reuses the same pair
// notifyUpdateRestart uses — the trusted broker-originated system-event broadcast
// to live CLI sessions, and a direct SendReply on the channel.
//
// The Telegram leg targets the operator's DM rather than every known topic: the
// DM is the one route that is always the operator's own, and fanning a startup
// warning across every project topic is the kind of noise people mute. At cold
// start no session has connected yet, so the DM is the leg that actually lands;
// the broadcast covers the case where a session is already live.
func (b *Broker) announceQueueDegraded(ch channel.Channel) {
	msg := queueDisabledWarning + " Fix the queue directory (or its permissions) and restart the broker."
	b.broadcastSystemEvent(&c3types.SystemEvent{
		Source:  "c3",
		Level:   "warn",
		Title:   "Durable queue DISABLED",
		Message: msg,
	})
	cc, ok := b.Mappings().Channels[ch.Name()]
	if !ok || cc.DMChatID == 0 {
		log.Printf("queue: degraded-mode notice not sent on channel %q — no operator DM configured", ch.Name())
		return
	}
	if _, err := ch.SendReply(c3types.ReplyArgs{Channel: ch.Name(), ChatID: cc.DMChatID, Text: "⚠️ " + msg}); err != nil {
		log.Printf("queue: degraded-mode notice to %s DM failed: %v", ch.Name(), err)
	}
}

// Channel returns the registered channel implementation by name.
func (b *Broker) Channel(name string) (channel.Channel, error) {
	b.chMu.RLock()
	defer b.chMu.RUnlock()
	reg, ok := b.channels[name]
	if !ok {
		return nil, fmt.Errorf("broker: channel %q not registered", name)
	}
	return reg.Channel, nil
}

// capsForChannel returns the static capability manifest for the named channel,
// or nil when the channel is not registered/resolvable. Used to populate the
// additive Capabilities field on hello_ack and attached payloads so the
// adapters can fold GuidanceFor(caps) into the agent surface. Nil is a valid
// wire value (omitempty) — older adapters ignore it and newer ones fall back
// to a sensible default.
func (b *Broker) capsForChannel(name string) *c3types.Capabilities {
	if name == "" {
		return nil
	}
	ch, err := b.Channel(name)
	if err != nil {
		return nil
	}
	caps := ch.Capabilities()
	return &caps
}

// Channels returns the names of all registered channels (diagnostic).
func (b *Broker) Channels() []string {
	b.chMu.RLock()
	defer b.chMu.RUnlock()
	out := make([]string, 0, len(b.channels))
	for name := range b.channels {
		out = append(out, name)
	}
	return out
}

// defaultShutdownGrace bounds Broker.Shutdown's worker-pool drain. Workers are
// ctx-cancelled first (no new outbound dispatch; in-flight channel calls unwind),
// but a worker stuck in a call that doesn't observe ctx cancel — an outbound send
// bounded only by its own ~20s HTTP timeout, or a voice-readback echo — must not
// wedge the whole exit. After this budget it is abandoned; the durable queue
// makes that loss-free, and main's watchdog is the outer backstop.
const defaultShutdownGrace = 8 * time.Second

// Shutdown drains in-flight work and stops all subsystems, bounding the
// worker-pool drain to defaultShutdownGrace. See ShutdownWithin for the ordering
// rationale.
func (b *Broker) Shutdown() {
	b.ShutdownWithin(defaultShutdownGrace)
}

// ShutdownWithin stops all subsystems in the order required for clean shutdown,
// bounding the worker-pool drain to grace (grace<=0 waits indefinitely):
//
//  1. Stop voice-scheduler admission, cancel and join its fixed goroutines and
//     accepted resolve jobs. After this returns nothing can call a route worker,
//     channel API, or STT plugin from the scheduler.
//  2. Stop the worker pool so no new outbound tool-calls dispatch.
//     StopWithin cancels the per-route worker contexts; any in-flight
//     dispatchOutbound returns from its channel call (or drops out of the
//     queue) before the channel is torn down. A worker that can't cancel in
//     time is abandoned after grace (loss-free — the durable queue redelivers).
//  3. Stop each channel — by now the worker pool isn't issuing fresh
//     Telegram calls, so Channel.Stop() can drain whatever's already
//     in flight (HTTP requests, polling goroutine) without racing new
//     submissions.
//  4. Cancel the broker context — propagates to any goroutine that
//     was holding a derived context (most are already stopped).
//
// The previous ordering (channels first, then workers) was the bug:
// workers held *Channel refs and called Channel.SendReply mid-flight
// while the channel's Stop was tearing down state, producing 20-second
// hangs on stale request opts timeouts. Addresses code-review-
// 2026-05-15 MAJOR #5 (daemon.md §1.3 drain order).
func (b *Broker) ShutdownWithin(grace time.Duration) {
	b.Voice.Stop()
	if !b.Workers.StopWithin(grace) {
		log.Printf("broker: worker pool did not drain within %s — proceeding with shutdown (leaked workers exit with the process; durable queue keeps this loss-free)", grace)
	}
	b.chMu.Lock()
	for _, reg := range b.channels {
		_ = reg.Channel.Stop()
	}
	b.chMu.Unlock()
	b.cancel()
}

// setLastHealth caches the most recent HealthEvent for a channel. Called from
// BrokerHost.NotifyHealth on every edge; read by handleHealth for the
// `c3-broker status` health line.
func (b *Broker) setLastHealth(ev c3types.HealthEvent) {
	b.healthMu.Lock()
	defer b.healthMu.Unlock()
	// Compare-and-skip: edges are detected in order (under fetchHealth.mu) but
	// processed by NotifyHealth on un-serialized goroutines, so an older edge
	// can be processed after a newer one. ev.Since is when the state was entered
	// and increases strictly across alternating edges, so never let an older
	// edge overwrite a newer one — otherwise the cache/status line could stick
	// on a stale state until the next genuine transition.
	if prev, ok := b.lastHealth[ev.Channel]; ok && ev.Since.Before(prev.Since) {
		return
	}
	b.lastHealth[ev.Channel] = ev
}

// lastHealthSnapshot returns a copy of the per-channel last-health cache.
func (b *Broker) lastHealthSnapshot() map[string]c3types.HealthEvent {
	b.healthMu.RLock()
	defer b.healthMu.RUnlock()
	out := make(map[string]c3types.HealthEvent, len(b.lastHealth))
	for k, v := range b.lastHealth {
		out[k] = v
	}
	return out
}

// SetMappings atomically swaps the in-memory mappings pointer. Called on
// SIGHUP to reload config from disk without restarting the daemon. Holds
// the mutation lock so concurrent UpsertTopic/UpsertMapping calls
// serialize against the swap rather than racing it.
//
// Caveats:
//   - In-flight Upsert mutations that landed BEFORE this call are
//     overwritten by the freshly-loaded file. SIGHUP after a manual
//     mappings.json edit is the expected use case; running it in the
//     middle of an attach race is a user error.
//   - Channel-level config (bot_token, group chat_ids, debounce_ms)
//     is consumed by the Telegram channel ONCE at Start; swapping
//     mappings doesn't re-init the channel. Adding new topics or
//     cwd→topic mappings works fine; changing the bot token requires
//     a full broker restart.
func (b *Broker) SetMappings(mf *mappings.MappingsFile) {
	b.mutationMu.Lock()
	defer b.mutationMu.Unlock()
	b.mappings.Store(mf)
}

// SetPersistedCallback registers the durable-persist notifier (the telegram
// channel sets this to advance its persisted-offset tracker). Safe to call once
// at channel start.
func (b *Broker) SetPersistedCallback(fn func(in *c3types.Inbound)) {
	b.persistedMu.Lock()
	defer b.persistedMu.Unlock()
	b.persistedCB = fn
}

// notifyPersisted invokes the registered persist callback, if any.
func (b *Broker) notifyPersisted(in *c3types.Inbound) {
	b.persistedMu.RLock()
	fn := b.persistedCB
	b.persistedMu.RUnlock()
	if fn != nil {
		fn(in)
	}
}

// SetPersistFailedCallback registers the durable-persist-FAILURE notifier (the
// telegram channel sets this to evict a poll-side dedup entry on Append failure
// so the held offset's redelivery genuinely retries — item 1). Safe to call once
// at channel start.
func (b *Broker) SetPersistFailedCallback(fn func(in *c3types.Inbound)) {
	b.persistedMu.Lock()
	defer b.persistedMu.Unlock()
	b.persistFailedCB = fn
}

// notifyPersistFailed invokes the registered persist-failure callback, if any.
func (b *Broker) notifyPersistFailed(in *c3types.Inbound) {
	b.persistedMu.RLock()
	fn := b.persistFailedCB
	b.persistedMu.RUnlock()
	if fn != nil {
		fn(in)
	}
}

// queueRouteKey converts a broker RouteKey into the queue package's RouteKey.
func queueRouteKey(k RouteKey) queue.RouteKey {
	rk := queue.RouteKey{Channel: k.Channel, ChatID: k.ChatID}
	if k.HasTopic {
		t := k.TopicID
		rk.TopicID = &t
	}
	return rk
}
