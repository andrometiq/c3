package broker

import (
	"github.com/Andrometiq/c3/internal/ipc"
	"sync"
	"sync/atomic"
)

// Stub is the broker's view of a connected adapter. ConnID is the
// late-result-discard token described in spec §4.5.1.
//
// A stub is "alive" if (a) Conn is non-nil OR (b) Conn is nil but its PID
// is still alive in the OS process table. Claims tied to a stub stay valid
// for as long as the stub is alive — a momentary conn drop does NOT release
// the claim. This keeps the broker as the authoritative owner of "who has
// what topic" and prevents racing adapters (codex auto-attach, claude
// reconnect-replay, etc.) from stealing each other's claims during conn
// churn.
//
// Process death is detected via `kill -0 PID` (Linux/Unix). On a
// confirmed-dead holder, Routes.Claim will release the stale claim and
// grant the new one.
type Stub struct {
	Build            string // immutable hello identity
	UpgradeDisabled  bool
	ResumeContract   string
	deliveryReady    atomic.Bool
	delivery         *negotiatedSession
	claimGenerations map[RouteKey]uint64
	claimSequence    uint64
	CLI              string
	PID              int
	CWD              string
	ConnID           uint64

	// Conn is opaque from the registry's POV — broker package wires it after
	// constructing, used by route workers to write inbound to the right
	// adapter. Type is *ipc.Conn but kept as any here to avoid the import
	// cycle in the registry file. Nil when the stub is in disconnected
	// (waiting-for-reconnect) state.
	connMu sync.RWMutex
	Conn   any

	// routes is the ordered set of claims held by this stub. output names the
	// one route used for outbound calls that do not select another held route.
	// Both are owned by the Stub and guarded by stubMu; the Routes table remains
	// the authority for route -> holder ownership.
	//
	// replied records whether this connection has successfully dispatched at
	// least one `reply` on each held route. It is the deterministic typing-relay
	// gate: activity on one route must not pulse "typing…" on a sibling route.
	// It lives on the per-connection Stub (NOT the per-RouteKey RouteWorker,
	// which outlives sessions) so it resets naturally on reconnect. Guarded by
	// stubMu.
	stubMu sync.Mutex
	routes []RouteKey
	output *RouteKey
	// confirmed records that each held route was set by a
	// LEGITIMATE claim site — an explicit/own-recover attach through tryClaim or
	// recoverSession — as opposed to any future code path that might bind a route
	// without a real claim. The two destructive consume paths (handleFetchQueue's
	// Ack=true fetch and handleInboundDelivered's live-push ack) refuse to consume
	// unless the route is present, so a silent-bind regression can never drain a
	// queue the session didn't choose. It is a property of each claim, not the
	// connection: removing a route clears its entry, re-arming the tripwire.
	// Honest scope (spec §5): every legitimate claim sets it via MarkRouteConfirmed,
	// so it is true on every legitimate live claim — this is fail-closed insurance
	// against a future regression, NOT a cure for a misbehaving LLM courier (§8).
	// Deliberately NOT set inside AddRoute: binding a route and CONFIRMING it are
	// separate acts, so a future silent AddRoute leaves the tripwire armed.
	// Guarded by stubMu.
	confirmed map[RouteKey]bool
	// Delivery eligibility, probe reservation, and notice history are owned by
	// stubMu. Empty state preserves the legacy adapter default (capable).
	ReceiptConfirming   bool // immutable hello capability; legacy ack keeps recovery
	renderRoute         ipc.RenderRoute
	renderProbeSent     bool
	renderNoticeRoutes  map[RouteKey]string
	renderNoticePending map[RouteKey]bool
	// peerProtocolVersion is the normalized IPC dialect observed on hello.
	// Sensitive dispatch reads this stored connection identity rather than
	// re-decoding or assuming the current build's dialect.
	peerProtocolVersion int
	// stableSessionID is the host CLI's STABLE per-session id (Claude: the
	// transcript / --resume id), learned from the SessionStart-hook handoff and
	// delivered to the broker via RecoverSessionReq AFTER hello. The broker keys
	// both recording (persistMapping / recordSessionAttachment) and recovery on
	// it. Empty until a recover op arrives (or never, for non-hook sessions —
	// fail-closed → no recording, no recovery). Guarded by stubMu; mutated via
	// SetStableSessionID.
	stableSessionID string
	// explicitlyDetached records that the USER detached this connection (the
	// `detach` tool → OpRelease → handleRelease) — as opposed to a conn drop or a
	// process exit, neither of which is a detach. It is the identity barrier for
	// late recovery: handleRelease can only tombstone the PERSISTED attachment when
	// the stable session id is already known, but on a resumed session the
	// SessionStart handoff that carries that id lands AFTER hello and the adapter's
	// recovery watch stays live for ~30 minutes, so a detach can legitimately
	// arrive while the broker still knows no identity. Without this flag the late
	// RecoverSessionReq finds an untombstoned attachment and re-claims the very
	// route the user just left — an explicit user action silently undone by a
	// background process that arrived later. Set by handleRelease only when the
	// held set becomes empty; releasing one sibling route must not block recovery
	// of the remainder. CLEARED by tryClaim, so a deliberate re-attach on the same
	// connection is honored and only RECOVERY is blocked; read by recoverSession
	// (the claim site) and by handleRecoverSession (which upgrades it to the
	// durable tombstone once the identity finally arrives). Guarded by stubMu.
	explicitlyDetached bool
	replied            map[RouteKey]bool

	// pushRoutes records, per outstanding live push, the ROUTE that push went out
	// on. DeliveryToken is the primary key: unlike MessageID it is unique across
	// routes and edits. MessageID remains only for legacy adapters that do not echo
	// the token, and that fallback consumes only when exactly one matching record
	// exists — ambiguity leaves every durable line queued.
	//
	// Bounded by maxCoveredByPush. Written by the route worker goroutine, read by
	// this connection's handler goroutine, so both go through stubMu.
	pushRoutes map[int64][]pushRouteRecord
	pushOrder  []pushRouteOrder
}

type pushRouteRecord struct {
	token string
	key   RouteKey
}

type pushRouteOrder struct {
	messageID int64
	token     string
}

// MarkDisconnected records that the stub's conn has dropped. The claim
// survives as long as the PID is alive.
func (s *Stub) MarkDisconnected() {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	s.Conn = nil
}

// Reattach swaps in a fresh conn (e.g., after the adapter reconnects). The
// stub's identity (CLI, PID, CWD) is unchanged; ConnID is bumped by the
// caller before this is invoked.
func (s *Stub) Reattach(conn any, newConnID uint64) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	s.Conn = conn
	s.ConnID = newConnID
}

// IsConnected reports whether the stub currently has an active conn.
func (s *Stub) IsConnected() bool {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return s.Conn != nil
}

// ConnValue returns the stub's current conn under the conn lock (or nil when
// disconnected). Use this for a race-free read of Conn from outside the handler
// goroutine — e.g. broker.broadcastSystemEvent, which writes to every live
// session concurrently with MarkDisconnected/Reattach.
func (s *Stub) ConnValue() any {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return s.Conn
}

// IsAlive returns whether the stub is in a state where its claims are
// protected. A connected stub is always alive. A disconnected stub is alive
// if its PID still exists in the OS process table — meaning the user's
// adapter process is still around and we're waiting for it to reconnect.
//
// Claims are PID-lived: there is deliberately NO disconnect timeout. A holder
// that never reconnects keeps its claim until its process exits (or a human
// force-steals the topic); how long it has been disconnected is not consulted.
// The broker used to carry a Disconnected timestamp that no line of code read,
// which invited the opposite belief — it was deleted rather than left to imply
// a lifetime rule the code does not implement.
func (s *Stub) IsAlive() bool {
	if s.IsConnected() {
		return true
	}
	return isPIDAlive(s.PID)
}

// isPIDAlive is defined per-OS in pidalive_unix.go / pidalive_windows.go.

// RouteSnapshot returns the held routes in claim order and the output route
// from one locked snapshot. The returned slice and pointer are copies.
func (s *Stub) RouteSnapshot() ([]RouteKey, *RouteKey) {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	return append([]RouteKey(nil), s.routes...), copyRouteKey(s.output)
}

// OutputRoute returns a copy of the route used for outbound calls without a
// selector, or nil when the stub holds no routes.
func (s *Stub) OutputRoute() *RouteKey {
	_, output := s.RouteSnapshot()
	return output
}

// Routes returns the held routes in claim order. The returned slice is a copy.
func (s *Stub) Routes() []RouteKey {
	routes, _ := s.RouteSnapshot()
	return routes
}

// HasRoute reports whether key is in the held set.
func (s *Stub) HasRoute(key RouteKey) bool {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	return s.hasRouteLocked(key)
}

func (s *Stub) hasRouteLocked(key RouteKey) bool {
	for _, held := range s.routes {
		if held == key {
			return true
		}
	}
	return false
}

// AddRoute appends key to the held set if absent. It deliberately does not mark
// the route confirmed: binding and confirming remain separate acts so the
// destructive-consume tripwire stays fail-closed. The first route becomes the
// output until an explicit output selection is made.
func (s *Stub) AddRoute(key RouteKey) {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	if s.hasRouteLocked(key) {
		return
	}
	s.claimSequence++
	if s.claimGenerations == nil {
		s.claimGenerations = map[RouteKey]uint64{}
	}
	s.claimGenerations[key] = s.claimSequence
	s.routes = append(s.routes, key)
	if s.output == nil {
		k := key
		s.output = &k
	}
}

// RemoveRoute removes one held route and its per-route state. If it was the
// output route, the most recently claimed remaining route becomes output.
func (s *Stub) RemoveRoute(key RouteKey) (removed bool, wasOutput bool, newOutput *RouteKey) {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	return s.removeRouteLocked(key)
}

func (s *Stub) removeRouteLocked(key RouteKey) (removed bool, wasOutput bool, newOutput *RouteKey) {
	idx := -1
	for i, held := range s.routes {
		if held == key {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false, false, copyRouteKey(s.output)
	}
	wasOutput = s.output != nil && *s.output == key
	s.routes = append(s.routes[:idx], s.routes[idx+1:]...)
	delete(s.confirmed, key)
	delete(s.replied, key)
	if wasOutput {
		s.output = nil
		if len(s.routes) > 0 {
			k := s.routes[len(s.routes)-1]
			s.output = &k
		}
	}
	return true, wasOutput, copyRouteKey(s.output)
}

func copyRouteKey(key *RouteKey) *RouteKey {
	if key == nil {
		return nil
	}
	k := *key
	return &k
}

// SetOutputRoute selects key as output only when it is already held.
func (s *Stub) SetOutputRoute(key RouteKey) bool {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	if !s.hasRouteLocked(key) {
		return false
	}
	k := key
	s.output = &k
	return true
}

// ClearRoutes drops the held set, output, and all per-route confirmation and
// reply state. Outstanding push-route correlations are intentionally retained,
// matching the old detach behavior: an in-flight ack still identifies the route
// it was pushed on, but the cleared confirmation gate prevents consumption.
func (s *Stub) ClearRoutes() {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	s.routes = nil
	s.output = nil
	s.confirmed = nil
	s.replied = nil
}

// ClearRouteIf removes key only when it is held. This is the steal-eviction
// primitive: a stolen route and its confirmation must disappear without
// disturbing sibling claims a victim may already hold or be switching to.
func (s *Stub) ClearRouteIf(key RouteKey) (removed bool, wasOutput bool, newOutput *RouteKey) {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	return s.removeRouteLocked(key)
}

// MarkRouteConfirmed records that key was established by a legitimate claim
// site. It is intentionally a no-op for an unheld route.
func (s *Stub) MarkRouteConfirmed(key RouteKey) {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	if !s.hasRouteLocked(key) {
		return
	}
	if s.confirmed == nil {
		s.confirmed = make(map[RouteKey]bool)
	}
	s.confirmed[key] = true
}

// RouteConfirmed reports whether key was established by a legitimate claim.
func (s *Stub) RouteConfirmed(key RouteKey) bool {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	return s.confirmed[key]
}

// SetStableSessionID records the host CLI's stable per-session id, learned from
// the SessionStart-hook handoff and delivered via RecoverSessionReq. Idempotent;
// last write wins. Guarded by stubMu like the route-set methods.
func (s *Stub) SetStableSessionID(id string) {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	s.stableSessionID = id
}

// StableSessionIDValue returns the stub's stable session id (empty until a
// recover op set it, or for a non-hook session). Guarded by stubMu.
func (s *Stub) StableSessionIDValue() string {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	return s.stableSessionID
}

// SetExplicitlyDetached sets (true, from handleRelease) or clears (false, from
// tryClaim's successful explicit claim) this connection's user-detached barrier.
// See the explicitlyDetached field doc. Guarded by stubMu like the route set.
func (s *Stub) SetExplicitlyDetached(v bool) {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	s.explicitlyDetached = v
}

// ExplicitlyDetached reports whether the user detached this connection and has
// not explicitly re-attached since. recoverSession refuses to claim while it is
// set, and handleRecoverSession refuses the route restore (keeping only the
// identity registration) when a recover op lands on such a connection. Guarded
// by stubMu.
func (s *Stub) ExplicitlyDetached() bool {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	return s.explicitlyDetached
}

// SetCannotRender records whether this session's host silently drops channel
// push notifications (from HelloMsg.CannotRenderChannels). Set once at hello,
// before the stub is claimable. Guarded by stubMu like the route set.
func (s *Stub) SetCannotRender(v bool) {
	s.SetRenderRoute("", "", v)
}

// CanRenderPush reports whether the broker may push channel notifications to this
// holder (and mark them delivered). False for a host that silently drops them —
// the forked-session blackhole — in which case inbound is held in the queue
// instead. Default true (renderable) for old adapters that never reported the
// flag. Guarded by stubMu.
func (s *Stub) CanRenderPush() bool {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	return s.renderRoute.State != ipc.RenderQueueOnly
}

func (s *Stub) SetPeerProtocolVersion(version int) {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	s.peerProtocolVersion = version
}

func (s *Stub) PeerProtocolVersion() int {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	return s.peerProtocolVersion
}

// MarkReplied records that this connection has dispatched at least one reply
// to key. It is intentionally a no-op for an unheld route.
func (s *Stub) MarkReplied(key RouteKey) {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	if !s.hasRouteLocked(key) {
		return
	}
	if s.replied == nil {
		s.replied = make(map[RouteKey]bool)
	}
	s.replied[key] = true
}

// HasReplied reports whether this connection has replied on key. The typing
// relay is route-local and must not arm from a reply on a sibling route.
func (s *Stub) HasReplied(key RouteKey) bool {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	return s.replied[key]
}

// RecordPushRoute remembers the route a live push went out on, keyed primarily
// by its broker-minted token and secondarily by MessageID for legacy adapters.
// Called BEFORE the frame is written — a fast adapter can ack on this
// connection's handler goroutine before the worker reaches its next statement.
func (s *Stub) RecordPushRoute(pushID int64, token string, key RouteKey) {
	if pushID == 0 {
		return
	}
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	// At the cap, evict the OLDEST record rather than refusing the newest: a
	// dropped record only means its ack resolves to nothing and the consume is
	// dropped, leaving that line queued (a recoverable duplicate). Refusing the
	// newest would blind exactly the pushes still in flight.
	for len(s.pushOrder) >= maxCoveredByPush {
		oldest := s.pushOrder[0]
		s.pushOrder = s.pushOrder[1:]
		recs := s.pushRoutes[oldest.messageID]
		for i := range recs {
			if recs[i].token != oldest.token {
				continue
			}
			recs = append(recs[:i], recs[i+1:]...)
			break
		}
		if len(recs) == 0 {
			delete(s.pushRoutes, oldest.messageID)
		} else {
			s.pushRoutes[oldest.messageID] = recs
		}
	}
	if s.pushRoutes == nil {
		s.pushRoutes = make(map[int64][]pushRouteRecord, maxCoveredByPush)
	}
	s.pushRoutes[pushID] = append(s.pushRoutes[pushID], pushRouteRecord{token: token, key: key})
	s.pushOrder = append(s.pushOrder, pushRouteOrder{messageID: pushID, token: token})
}

// TakePushRoute returns and clears the exact tokened push route. For a legacy
// no-token ack, it succeeds only when exactly one outstanding record matches the
// MessageID; two matching routes/edits are ambiguous and remain queued.
func (s *Stub) TakePushRoute(pushID int64, token string) *RouteKey {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	recs := s.pushRoutes[pushID]
	if len(recs) == 0 {
		return nil
	}
	idx := -1
	if token == "" {
		if len(recs) != 1 {
			return nil
		}
		idx = 0
	} else {
		for i := range recs {
			if recs[i].token == token {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil
		}
	}
	k := recs[idx].key
	recordToken := recs[idx].token
	recs = append(recs[:idx], recs[idx+1:]...)
	if len(recs) == 0 {
		delete(s.pushRoutes, pushID)
	} else {
		s.pushRoutes[pushID] = recs
	}
	for i, record := range s.pushOrder {
		if record.messageID == pushID && record.token == recordToken {
			s.pushOrder = append(s.pushOrder[:i], s.pushOrder[i+1:]...)
			break
		}
	}
	return &k
}

// AdoptPushRoutes moves prev's outstanding live-push records onto this stub so an
// ack arriving AFTER an adapter reconnect still resolves to the route its push
// went out on. Called from the hello reconnect branch beside the stable-session-id
// carry. prev's records are older, so they keep the front of the FIFO; anything
// already recorded on this stub (a push that landed between the claim transfer
// and this call) is preserved behind them.
func (s *Stub) AdoptPushRoutes(prev *Stub) {
	if prev == nil || prev == s {
		return
	}
	prev.stubMu.Lock()
	routes, order := prev.pushRoutes, prev.pushOrder
	prev.pushRoutes, prev.pushOrder = nil, nil
	prev.stubMu.Unlock()
	if len(order) == 0 {
		return
	}
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	if s.pushRoutes == nil {
		s.pushRoutes, s.pushOrder = routes, order
		return
	}
	for id, recs := range routes {
		s.pushRoutes[id] = append(recs, s.pushRoutes[id]...)
	}
	s.pushOrder = append(order, s.pushOrder...)
}

// StubRegistry holds connected adapters keyed by ConnID. Concurrent-safe.
type StubRegistry struct {
	mu     sync.RWMutex
	next   atomic.Uint64
	byConn map[uint64]*Stub
}

// NewStubRegistry returns an empty registry. The first ConnID handed out is 1
// (uint64 0 is reserved for "no stub").
func NewStubRegistry() *StubRegistry {
	return &StubRegistry{byConn: map[uint64]*Stub{}}
}

// Register creates a new Stub with a monotonic ConnID and returns it. The
// stable session id (used for auto-attach-on-resume) is NOT set here — it
// arrives later via RecoverSessionReq and is stored with SetStableSessionID.
func (r *StubRegistry) Register(cli string, pid int, cwd string, conn any, initialize ...func(*Stub)) *Stub {
	id := r.next.Add(1)
	s := &Stub{CLI: cli, PID: pid, CWD: cwd, ConnID: id, Conn: conn}
	for _, init := range initialize {
		init(s)
	}
	r.mu.Lock()
	r.byConn[id] = s
	r.mu.Unlock()
	return s
}

// Get returns the stub for connID and whether it's present.
func (r *StubRegistry) Get(connID uint64) (*Stub, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.byConn[connID]
	return s, ok
}

// Unregister removes the stub. No-op if not present.
func (r *StubRegistry) Unregister(connID uint64) {
	r.mu.Lock()
	delete(r.byConn, connID)
	r.mu.Unlock()
}

// Snapshot returns a copy of all currently-registered stubs. Used by status.
func (r *StubRegistry) Snapshot() []*Stub {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Stub, 0, len(r.byConn))
	for _, s := range r.byConn {
		out = append(out, s)
	}
	return out
}

// SetRenderRoute applies additive hello/update fields; legacy adapters retain
// the old boolean default. Unknown explicit states fail closed.
func (s *Stub) SetRenderRoute(state, reason string, cannot bool) {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	if state == "" {
		if cannot {
			state = ipc.RenderQueueOnly
		} else {
			state = ipc.RenderCapable
		}
	}
	if state != ipc.RenderCapable && state != ipc.RenderProbing && state != ipc.RenderCrossSession {
		state = ipc.RenderQueueOnly
	}
	s.renderRoute = ipc.RenderRoute{State: state, Reason: reason}
	s.renderProbeSent = false
}

func (s *Stub) RenderRoute() ipc.RenderRoute {
	if s.negotiated() {
		key := s.OutputRoute()
		if key != nil {
			return s.deliveryRoute(*key)
		}
		return ipc.RenderRoute{State: "waiting"}
	}
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	if s.renderRoute.State == "" {
		return ipc.RenderRoute{State: ipc.RenderCapable}
	}
	return s.renderRoute
}

// scheduleRenderNotice reserves one sending loop per route through completion.
func (s *Stub) scheduleRenderNotice(key RouteKey) bool {
	route := s.RenderRouteFor(key)
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	state := route.Semantic()
	if s.renderNoticePending[key] || s.renderNoticeRoutes[key] == state {
		return false
	}
	if s.renderNoticePending == nil {
		s.renderNoticePending = map[RouteKey]bool{}
	}
	s.renderNoticePending[key] = true
	return true
}

// nextRenderNotice rechecks after the cooldown: an intervening transition back
// to the last sent state needs no notice. Clearing pending under the same lock
// lets a later state change reserve a new loop without losing an update.
func (s *Stub) nextRenderNotice(key RouteKey) (ipc.RenderRoute, bool) {
	route := s.RenderRouteFor(key)
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	if s.renderNoticeRoutes[key] == route.Semantic() {
		delete(s.renderNoticePending, key)
		return ipc.RenderRoute{}, false
	}
	return route, true
}

func (s *Stub) finishRenderNotice(key RouteKey, route ipc.RenderRoute, sent bool) bool {
	current := s.RenderRouteFor(key)
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	if sent {
		if s.renderNoticeRoutes == nil {
			s.renderNoticeRoutes = map[RouteKey]string{}
		}
		s.renderNoticeRoutes[key] = route.Semantic()
		if current.Semantic() != route.Semantic() {
			return true // keep the reservation while sending the latest state
		}
	}
	delete(s.renderNoticePending, key)
	return false
}

// Reserve the first probe across all routes held by this session. Later human
// messages use the normal durable hold path until its receipt arrives.
func (s *Stub) tryRenderPush() bool {
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	if s.renderRoute.State == ipc.RenderProbing {
		if s.renderProbeSent {
			return false
		}
		s.renderProbeSent = true
		return true
	}
	return s.renderRoute.State != ipc.RenderQueueOnly
}
