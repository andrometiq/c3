package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
)

// recoverRouteIdentity records the stable identity under which a route was
// established. Provenance is enforced only when such an identity existed:
// attach-before-recover remains unconstrained, and its first RecoverSessionReq
// names the conversation. automatic distinguishes replay/reconnect carriage
// from a human attach so anonymous automatic state can still be rejected when
// the registering identity already has a different server-side route record.
type recoverRouteIdentity struct {
	stableID     string
	requireMatch bool
	automatic    bool
}

// HandleConn drives one adapter connection through its lifecycle. Owns the
// connection — closes it on return.
func (b *Broker) HandleConn(nc net.Conn) {
	conn := ipc.NewConn(nc)
	defer conn.Close()
	// A panic on a crafted/garbage frame must drop only THIS connection, not
	// crash the whole broker (one connection-handler goroutine per adapter).
	defer recoverGoroutine("HandleConn")

	// Stage 1: hello.
	raw, err := conn.ReadFrame()
	if err != nil {
		return
	}
	op, err := ipc.PeekOp(raw)
	if err != nil || op != ipc.OpHello {
		_ = conn.WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: "expected hello first"})
		return
	}
	var hello ipc.HelloMsg
	if err := json.Unmarshal(raw, &hello); err != nil {
		_ = conn.WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: "malformed hello"})
		return
	}

	// Protocol version: retain the connection for safe operations, but refuse
	// destructive/ownership-changing ops outside the explicitly implemented
	// compatibility window. `c3 update` can therefore remain diagnosable without
	// letting an unknown dialect mutate claims or durable queues.
	if w := ipc.BrokerProtocolWarning(hello.CLI, hello.PID, hello.ProtocolVersion); w != "" {
		log.Print(w)
	}

	// Reconnect detection: if we have a disconnected stub for the same
	// (CLI, PID, CWD), this is the same adapter coming back after a brief
	// drop. Transfer its claims to a fresh ConnID instead of registering a
	// new stub and racing the original's claims. Per the "broker is the
	// authority" principle, claims survive conn drops as long as PID lives.
	var stub *Stub
	var routeIdentity recoverRouteIdentity
	if existing := b.Routes.FindByLogicalSession(hello.CLI, hello.PID, hello.CWD); existing != nil {
		stub = b.registerDeliveryHello(hello, conn, existing)
		oldConnID := existing.ConnID
		// Carry the stable session id across a reconnect so a post-reconnect
		// recover op isn't needed to re-key recording — the same logical session
		// keeps its recovery identity. (No-op when the prior stub never received
		// one; non-hook sessions stay empty.)
		if sid := existing.StableSessionIDValue(); sid != "" {
			stub.SetStableSessionID(sid)
		}
		// Negotiation is immutable before publishing the new stub. A changed
		// contract releases old evidence before transferring any claims.
		if (existing.negotiated() || stub.negotiated()) && existing.delivery != stub.delivery {
			existing.deliveryReady.Store(false)
			b.attempts.release(existing, "mode_changed", time.Now())
			b.reconnectDelivery(existing, stub)
		}
		// Unregister the OLD stub (now superseded) and transfer its claims.
		b.Stubs.Unregister(oldConnID)
		transferred := b.Routes.TransferAllByConnID(oldConnID, stub)
		// Carry every claim onto the new stub, not just the routing-table entries.
		// Routes must never say this stub holds routes while the stub itself says it
		// holds nothing: every stub-derived path reads this ordered held set.
		// inbound keeps arriving here (worker.go's Routes.Holder resolves to this
		// stub) while `reply`/`react`/`poll` answer "no route claimed", the
		// delivered-ack is dropped so the same lines are handed out again by
		// fetch_queue, a bare attach falls through to the picker, and /c3:ping and
		// /c3:sessions report "not attached". That window is PERMANENT if the
		// adapter's attach replay never lands (write error, ValidateTopic failure,
		// nothing to replay). Confirmation is a property of the CLAIM and it is the
		// same claim, so re-derive it from the old stub rather than confirming
		// blindly — a route bound without a real claim must stay fail-closed.
		if len(transferred) > 0 {
			routeIdentity = recoverRouteIdentity{
				stableID:     existing.StableSessionIDValue(),
				requireMatch: existing.StableSessionIDValue() != "",
				automatic:    true,
			}
			transferredSet := make(map[RouteKey]bool, len(transferred))
			for _, key := range transferred {
				transferredSet[key] = true
			}
			bind := func(key RouteKey) {
				if !transferredSet[key] {
					return
				}
				stub.AddRoute(key)
				if existing.RouteConfirmed(key) {
					stub.MarkRouteConfirmed(key)
				}
				delete(transferredSet, key)
			}
			for _, key := range existing.Routes() {
				bind(key)
			}
			// A leftover means the table and old stub already disagreed. Preserve
			// ownership rather than dropping a transferred claim; sort for a stable
			// fallback order.
			leftovers := make([]RouteKey, 0, len(transferredSet))
			for key := range transferredSet {
				leftovers = append(leftovers, key)
			}
			sort.Slice(leftovers, func(i, j int) bool { return routeKeyStr(leftovers[i]) < routeKeyStr(leftovers[j]) })
			for _, key := range leftovers {
				stub.AddRoute(key)
			}
			if output := existing.OutputRoute(); output != nil && stub.SetOutputRoute(*output) {
				// Preserved exactly.
			} else if routes := stub.Routes(); len(routes) > 0 {
				stub.SetOutputRoute(routes[len(routes)-1])
			}
			b.enqueueOutputRoleChange(stub, nil, stub.OutputRoute())
		}
		// Carry the outstanding live-push records too: a delivered-ack that arrives
		// after the reconnect must still resolve to the route its push went out on
		// (see Stub.pushRoutes). Ordered after the transfer so a push racing this
		// hello records onto the stub that now holds the claim.
		b.reconnectDelivery(existing, stub)
		if !stub.negotiated() && !existing.negotiated() {
			stub.AdoptPushRoutes(existing)
			b.attempts.adopt(existing, shadowHolder(stub), time.Now())
		}
		log.Printf("hello: RECONNECT cli=%s pid=%d cwd=%q old-conn=%d new-conn=%d (claims transferred)",
			hello.CLI, hello.PID, hello.CWD, oldConnID, stub.ConnID)
	} else {
		stub = b.registerDeliveryHello(hello, conn, nil)
		log.Printf("hello: NEW cli=%s pid=%d cwd=%q conn=%d",
			hello.CLI, hello.PID, hello.CWD, stub.ConnID)
	}
	stub.SetPeerProtocolVersion(ipc.PeerProtocolVersion(hello.ProtocolVersion))

	render := stub.RenderRoute()
	log.Printf("hello: cli=%s pid=%d conn=%d %s", hello.CLI, hello.PID, stub.ConnID, render.Text())

	// Defer: mark the stub as disconnected and decide whether to release
	// its claims based on PID liveness. The "claims preserved while PID
	// alive" rule covers the common case where the adapter is briefly
	// reconnecting (network blip, broker bounce). When the PID is
	// already dead at conn-drop time — e.g. Claude Code killed the
	// adapter for /mcp reconnect, the user quit the CLI — preserving
	// the claim would only block fallback delivery and future attaches
	// from competing PIDs.
	//
	// Defense-in-depth: forwardOrFallback also checks IsAlive on every
	// dispatch and releases dead-holder claims (kernel may not reap the
	// process by the time this defer runs).
	defer func() {
		stub.MarkDisconnected()
		if isPIDAlive(stub.PID) {
			b.attempts.release(stub, "disconnect", time.Now())
			log.Printf("conn-drop: cli=%s pid=%d cwd=%q conn=%d (claims preserved while pid alive)",
				stub.CLI, stub.PID, stub.CWD, stub.ConnID)
			return
		}
		b.releaseDelivery(stub)
		b.attempts.release(stub, "holder_death", time.Now())
		released := b.Routes.ReleaseAllByConnID(stub.ConnID)
		log.Printf("conn-drop: cli=%s pid=%d cwd=%q conn=%d (PID dead — released %d claim(s))",
			stub.CLI, stub.PID, stub.CWD, stub.ConnID, len(released))
	}()
	defer b.Stubs.Unregister(stub.ConnID)

	ack := b.buildHelloAck(hello, stub)
	fallback := b.prepareUpgrade(hello, stub, &ack)
	if stub.negotiated() {
		ack.Delivery = &ipc.DeliveryAcceptance{Version: 1, Modes: []string{"channel", "inbox"}}
	}
	if err := conn.WriteJSON(ack); err != nil {
		return
	}

	if fallback != "" {
		b.sendUpgradeNotice(stub, fallback)
	}
	if stub.negotiated() {
		stub.deliveryReady.Store(true)
		b.rearmDelivery(stub)
	}
	// Stage 2: dispatch loop.
	for {
		raw, err := conn.ReadFrame()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				// transient — let the connection close.
			}
			return
		}
		op, err := ipc.PeekOp(raw)
		if err != nil {
			_ = conn.WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: err.Error()})
			continue
		}
		if (op == ipc.OpAttemptResult || op == ipc.OpDeliveryReport) && !stub.negotiated() {
			attemptNoop()
			continue
		}
		if refuseIncompatibleStateChange(conn, stub, op, raw) {
			continue
		}
		switch op {
		case ipc.OpTestInject:
			b.handleTestInject(conn, raw)
		case ipc.OpAttach:
			before := stub.Routes()
			stableAtAttach := stub.StableSessionIDValue()
			var attachReq ipc.AttachReq
			_ = json.Unmarshal(raw, &attachReq)
			b.handleAttach(conn, stub, raw)
			if after := stub.Routes(); len(after) > 0 {
				routeChanged := !sameRouteOrder(before, after)
				if attachReq.Replay {
					// A replay is automatic old-state carriage, but an
					// identity-empty fresh stub has no provenance to enforce.
					// Its first recover may name the conversation, subject to
					// the server-side recorded-route conflict check below.
					routeIdentity = recoverRouteIdentity{
						stableID:     stableAtAttach,
						requireMatch: stableAtAttach != "",
						automatic:    true,
					}
				} else if routeChanged {
					// Preserve D4's attach-first ruling: a human-driven attach
					// made before the first stable id is known may be recorded by
					// that first recover op.
					routeIdentity = recoverRouteIdentity{
						stableID:     stableAtAttach,
						requireMatch: stableAtAttach != "",
					}
				}
			}
		case ipc.OpListTopics:
			b.handleListTopics(conn)
		case ipc.OpListClaims:
			b.handleListClaims(conn)
		case ipc.OpListHealth:
			b.handleHealth(conn)
		case ipc.OpRelease:
			if b.handleRelease(conn, stub, raw) {
				routeIdentity = recoverRouteIdentity{}
			}
		case ipc.OpSetOutputRoute:
			b.handleSetOutputRoute(conn, stub, raw)
		case ipc.OpToolCall:
			b.handleToolCall(conn, stub, raw)
		case ipc.OpAskRegister:
			b.handleAskRegister(conn, stub, raw)
		case ipc.OpPermissionRequest:
			b.handlePermissionRequest(conn, stub, raw)
		case ipc.OpPermissionSettled:
			b.handlePermissionSettled(conn, stub, raw)
		case ipc.OpWebLoginLink:
			b.handleWebLoginLink(conn)
		case ipc.OpWebCA:
			b.handleWebCA(conn)
		case ipc.OpFetchQueue:
			b.handleFetchQueue(conn, stub, raw)
		case ipc.OpObserve:
			b.handleObserve(conn, stub, raw)
		case ipc.OpRetranscribe:
			b.handleRetranscribe(conn, stub, raw)
		case ipc.OpRecoverSession:
			b.handleRecoverSession(conn, stub, raw, &routeIdentity)
		case ipc.OpAttemptResult:
			b.handleAttemptResult(stub, raw)
		case ipc.OpDeliveryReport:
			b.handleDeliveryReport(stub, raw)
		case ipc.OpRenderState:
			b.handleRenderState(stub, raw)
		case ipc.OpInboundDelivered:
			b.handleInboundDelivered(stub, raw)
		case ipc.OpPairModeStart:
			b.handlePairModeStart(conn, raw)
		case ipc.OpPingThisSession:
			b.handlePingThisSession(conn, raw)
		case ipc.OpListSessions:
			b.handleListSessions(conn, raw)
		case ipc.OpBye:
			return
		default:
			_ = conn.WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: "op not implemented yet: " + string(op)})
		}
	}
}

// buildHelloAck constructs the hello ack for a freshly-registered stub: the
// NoConfig / NoMapping signals and the resolvable channel's capability manifest.
//
// Auto-attach-on-resume is NOT done here. The STABLE, --resume-able session id
// is delivered only by a SessionStart hook that fires ~2s AFTER the adapter
// spawns, so recovery can't happen during the hello handshake — it runs later
// via RecoverSessionReq (handleRecoverSession). buildHelloAck therefore keeps
// only its pre-recovery behavior.
func (b *Broker) buildHelloAck(hello ipc.HelloMsg, stub *Stub) ipc.HelloAckMsg {
	ack := ipc.HelloAckMsg{
		Op:     ipc.OpHelloAck,
		ConnID: stub.ConnID,
		// Always stamped, so an adapter can name the disagreement from its side
		// too (an older broker omits it ⇒ the adapter reads v1).
		ProtocolVersion: ipc.ProtocolVersion,
	}
	if len(b.Mappings().Channels) == 0 {
		ack.NoConfig = true
	} else if _, ok := b.Mappings().LookupByCwd(hello.CWD); !ok {
		ack.NoMapping = true
	}
	// Capability manifest: prefer the cwd-mapped channel, then the default. nil
	// is a valid wire value — the adapter falls back to a default Capabilities.
	chanName := ""
	if m, ok := b.Mappings().LookupByCwd(hello.CWD); ok && m.Channel != "" && b.isRegisteredEnabled(m.Channel) {
		chanName = m.Channel
	} else {
		chanName = b.primaryChannel()
	}
	ack.Capabilities = b.capsForChannel(chanName)
	return ack
}

// handleRelease detaches either one selected held route or the whole set. Only
// an empty resulting set raises the explicitly-detached barrier and tombstones
// recovery; a targeted release rewrites the stored set so sibling routes remain
// recoverable. It returns whether no routes remain.
//
// The tombstone is keyed on the stable session id, which the broker may not have
// yet: a resumed session's id arrives on a SessionStart handoff that can land
// long after hello (the adapter watches for it for ~30 minutes), and the `detach`
// tool writes OpRelease unconditionally, even holding no route. "Unknown identity
// ⇒ nothing to tombstone" is therefore a REACHABLE state, not a theoretical one,
// and the recover op that arrives afterwards used to re-claim the route the user
// had just left. So the release also raises a per-connection barrier that late
// recovery cannot reverse (Stub.explicitlyDetached) — the identity-independent
// half of the same decision.
func (b *Broker) handleRelease(conn *ipc.Conn, stub *Stub, raw []byte) bool {
	var req ipc.ReleaseReq
	if err := json.Unmarshal(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: "malformed release: " + err.Error()})
		return len(stub.Routes()) == 0
	}
	if req.Target != "" {
		key, err := b.resolveHeldRoute(stub, req.Target)
		if err != nil {
			_ = conn.WriteJSON(ipc.ReleaseResp{Op: ipc.OpReleaseResult, Err: err.Error()})
			return false
		}
		if holder, held := b.Routes.Holder(key); !held || holder != stub {
			_ = conn.WriteJSON(ipc.ReleaseResp{Op: ipc.OpReleaseResult, Err: "release refused: selected route is no longer held"})
			return false
		}
		b.Routes.Release(key, stub.ConnID)
		removed, _, newOutput := stub.RemoveRoute(key)
		if !removed {
			_ = conn.WriteJSON(ipc.ReleaseResp{Op: ipc.OpReleaseResult, Err: "release refused: selected route is not held"})
			return false
		}
		b.enqueueOutputRoleChange(stub, &key, newOutput)
		if len(stub.Routes()) > 0 {
			stub.SetExplicitlyDetached(false)
			if sid := stub.StableSessionIDValue(); sid != "" && b.dropStoredRoute(stub.CLI, sid, key, newOutput) {
				_ = b.SaveMappings()
			}
			routes, output := b.routeSetRefs(stub)
			_ = conn.WriteJSON(ipc.ReleaseResp{Op: ipc.OpReleaseResult, OK: true, Routes: routes, Output: output})
			return false
		}
	} else {
		b.Routes.ReleaseAllByConnID(stub.ConnID)
		stub.ClearRoutes()
	}
	// Set unconditionally (not only in the empty-id case): the flag means "the
	// user detached every route on THIS connection", so it is true whether or not
	// we could also write the durable tombstone below. tryClaim clears it on the
	// next explicit attach.
	stub.SetExplicitlyDetached(true)
	if sid := stub.StableSessionIDValue(); sid != "" {
		if b.tombstoneSessionAttachment(stub.CLI, sid) {
			_ = b.SaveMappings()
		}
	}
	if req.Target != "" {
		routes, output := b.routeSetRefs(stub)
		_ = conn.WriteJSON(ipc.ReleaseResp{Op: ipc.OpReleaseResult, OK: true, Routes: routes, Output: output})
	}
	return true
}

func (b *Broker) handleSetOutputRoute(conn *ipc.Conn, stub *Stub, raw []byte) {
	var req ipc.SetOutputRouteReq
	if err := json.Unmarshal(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.SetOutputRouteResp{Op: ipc.OpSetOutputRouteResult, Err: "malformed set_output_route: " + err.Error()})
		return
	}
	key, err := b.resolveHeldRoute(stub, req.Target)
	if err != nil {
		_ = conn.WriteJSON(ipc.SetOutputRouteResp{Op: ipc.OpSetOutputRouteResult, Err: err.Error()})
		return
	}
	oldOutput := stub.OutputRoute()
	if !stub.SetOutputRoute(key) {
		_ = conn.WriteJSON(ipc.SetOutputRouteResp{Op: ipc.OpSetOutputRouteResult, Err: "selected route is not held"})
		return
	}
	b.enqueueOutputRoleChange(stub, oldOutput, &key)
	b.recordCurrentRoutesForStable(stub)
	output := b.routeRefForKey(key)
	_ = conn.WriteJSON(ipc.SetOutputRouteResp{
		Op: ipc.OpSetOutputRouteResult, OK: true, Output: &output,
	})
}

// handleRecoverSession is the adapter → broker recover op: the resumed session's
// STABLE id (learned from the SessionStart-hook handoff) arrives here on the
// stub's own connection, so the broker maps stub→stable-id directly. It then
// takes ONE of two dual-path-recording branches:
//
//   - Stub ALREADY attached: RECORD the held set and output under the stable id only
//     when it was a manual attach-before-recover, was carried under this same
//     stable id, or arrived anonymously and does not conflict with that id's
//     server-side record. A set stamped for a different id, or anonymous
//     automatic carriage that conflicts with an existing record, is an identity
//     switch: release it without a tombstone, then recover the new identity's
//     own set.
//   - Stub NOT attached: attempt recoverSession — re-claim the set the stable id
//     last held, skipping individual collisions. On success, report its output
//     plus that route's held backlog count so the adapter
//     can surface a one-shot auto-attach notification.
//
// Ahead of the not-attached branch sits the detach barrier: a stub the user
// EXPLICITLY detached keeps the identity registration but gets no route restore
// and no welcome, and its attachment is tombstoned now that there is an id to key
// the tombstone on (see the branch comment).
//
// Fail-closed: a malformed request or an empty stable id records nothing and
// recovers nothing.
func (b *Broker) handleRecoverSession(conn *ipc.Conn, stub *Stub, raw []byte, routeIdentity *recoverRouteIdentity) {
	var req ipc.RecoverSessionReq
	if err := json.Unmarshal(raw, &req); err != nil || req.StableSessionID == "" {
		_ = conn.WriteJSON(ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult, Err: "bad recover_session"})
		return
	}
	prev := stub.StableSessionIDValue()
	stampedRouteIdentityMismatch := routeIdentity != nil &&
		routeIdentity.requireMatch &&
		routeIdentity.stableID != req.StableSessionID
	recordedRouteIdentityMismatch := false
	if routeIdentity != nil && routeIdentity.automatic && !routeIdentity.requireMatch {
		if stub.OutputRoute() != nil {
			if recorded, ok := b.lookupSessionAttachment(stub.CLI, req.StableSessionID); ok &&
				recorded.Recoverable(time.Now(), SessionAttachmentTTL) {
				recordedRouteIdentityMismatch = !sessionAttachmentMatchesStub(recorded, stub)
			}
		}
	}
	routeIdentityMismatch := stampedRouteIdentityMismatch || recordedRouteIdentityMismatch
	switched := (prev != "" && prev != req.StableSessionID) || routeIdentityMismatch
	stub.SetStableSessionID(req.StableSessionID)
	defer func() {
		if routeIdentity == nil {
			return
		}
		if len(stub.Routes()) == 0 {
			*routeIdentity = recoverRouteIdentity{}
			return
		}
		*routeIdentity = recoverRouteIdentity{
			stableID:     req.StableSessionID,
			requireMatch: true,
		}
	}()
	if switched {
		if cur := stub.OutputRoute(); cur != nil {
			b.Routes.ReleaseAllByConnID(stub.ConnID)
			stub.ClearRoutes()
			from := prev
			if routeIdentityMismatch {
				from = routeIdentity.stableID
			}
			if from == "" {
				from = "<unregistered>"
			}
			log.Printf("recover: identity switch conn=%d %s → %s — released %s, recovering the new session's topic",
				stub.ConnID, from, req.StableSessionID, routeKeyStr(*cur))
		}
		// A detach barrier records a user action in the previous conversation.
		// Switching identities must not carry it into the new conversation.
		stub.SetExplicitlyDetached(false)
	}
	resp := ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult}
	if len(stub.Routes()) > 0 {
		// Attach-first: a fresh session already claimed routes before this recover
		// op arrived. Record the set under the stable id so a
		// future resume recovers it. No re-claim. This is bookkeeping, not an
		// auto-re-attach, so it runs regardless of the auto_attach_on_resume gate.
		b.recordCurrentRoutesForStable(stub)
	} else if stub.ExplicitlyDetached() {
		// The user detached this connection BEFORE its stable id was known, so
		// handleRelease had nothing to tombstone. Now that the identity has arrived,
		// take exactly half this frame: KEEP the identity registration
		// (SetStableSessionID above already ran, so a later EXPLICIT attach on this
		// connection is still recorded against this session and a future resume can
		// recover THAT), and DROP the route restore + its "resumed" welcome, which
		// would undo a deliberate user action. Dropping the whole frame instead
		// would silently un-key this session, costing the user recording they never
		// asked to lose.
		//
		// Write the tombstone now that there is something to key it on. The
		// per-connection flag dies with the connection while the persisted
		// attachment outlives it, so without this a broker bounce — routine, and the
		// Grok adapter re-fires recovery on every reconnect — would hand the
		// detached route straight back on the next connection. Writing it here makes
		// "detach before the identity arrived" mean exactly what "detach after the
		// identity arrived" already means at handleRelease, instead of a weaker
		// promise that depends on invisible timing. It is also reversible in the
		// normal way: any explicit attach upserts the attachment and clears it.
		if b.tombstoneSessionAttachment(stub.CLI, req.StableSessionID) {
			_ = b.SaveMappings()
		}
		log.Printf("recover: REFUSED session=%s — this connection was explicitly detached before its id was known; identity recorded, route NOT restored, attachment tombstoned", req.StableSessionID)
	} else if !b.Mappings().AutoAttachOnResumeEnabled() {
		// Gate (default ON post-redesign — nil ⇒ enabled; only an explicit
		// "auto_attach_on_resume": false disables it): the stable id is recorded
		// above via SetStableSessionID (so a later SIGHUP-enable / attach still
		// works), but the broker does NOT auto-re-claim the last route. Read live
		// from the current mappings snapshot, so a SIGHUP config reload flips it
		// without a restart. One line per resumed session (recover fires once).
		log.Printf("recover: auto-attach-on-resume DISABLED by config — session=%s not re-attached (remove \"auto_attach_on_resume\": false from mappings.json to re-enable)", req.StableSessionID)
	} else if key, cnt, preview, ok := b.recoverSession(stub); ok {
		resp.Recovered = true
		resp.Channel = key.Channel
		resp.ChatID = key.ChatID
		if key.HasTopic {
			t := key.TopicID
			resp.TopicID = &t
		}
		// Count AND preview come from recoverSession's SINGLE backlogSummary peek
		// (§3c) — one job, so they always describe the same snapshot. The preview
		// SURFACES the held messages into the resumed session (not just a count,
		// BUG #2); the authoritative live count is fetch_queue's Remaining.
		resp.QueuedCount = cnt
		resp.QueuedSummary = preview
		// Name/Group: prefer the recorded attachment, fall back to the topic
		// registry. DM (no topic) reports name "dm".
		if sa, ok := b.lookupSessionAttachment(stub.CLI, req.StableSessionID); ok {
			resp.Name = sa.Name
			resp.Group = sa.Group
		}
		if resp.Name == "" {
			if key.HasTopic {
				if tp, ok := b.Mappings().LookupTopicByID(key.Channel, key.ChatID, key.TopicID); ok {
					resp.Name = tp.Name
					resp.Group = tp.Group
				}
			} else {
				resp.Name = nonTopicRouteName(key.Channel)
			}
		}
		resp.Routes, resp.Output = b.routeSetRefs(stub)
		// Guaranteed-visible confirmation: post a one-shot Telegram note to the
		// recovered topic. The adapter's CLI notice can be dropped by Claude Code
		// when it fires in the resume idle gap (2026-06-24), so the Telegram
		// message is the reliable signal that auto-attach-on-resume happened.
		// Async so a slow send never delays this recover response.
		go b.sendRecoverWelcome(stub, key, resp.Name, cnt)
	}
	_ = conn.WriteJSON(resp)
}

// handleToolCall dispatches a tool-call to the worker for the selected held
// route, defaulting to output. The result returns asynchronously via the worker's
// OutboundJob.ResultCh; we block this connection's read loop on it (which is
// fine because the writer mutex on the Conn allows other goroutines —
// inbound forwarding — to write concurrently).
func (b *Broker) handleToolCall(conn *ipc.Conn, stub *Stub, raw []byte) {
	var req ipc.ToolCallReq
	if err := json.Unmarshal(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: "malformed tool_call: " + err.Error()})
		return
	}

	route, forwardedArgs, err := b.resolveToolRoute(stub, req.Args)
	if err != nil {
		_ = conn.WriteJSON(ipc.ToolResultMsg{
			Op: ipc.OpToolResult, ID: req.ID,
			Error: &ipc.ErrorPayload{Code: -32000, Message: err.Error()},
		})
		return
	}

	resultCh := make(chan OutboundResult, 1)
	job := Job{Kind: JobOutbound, Outbound: &OutboundJob{
		Tool:     req.Name,
		Args:     forwardedArgs,
		ResultCh: resultCh,
	}}
	if !b.Workers.Submit(route, job) {
		_ = conn.WriteJSON(ipc.ToolResultMsg{
			Op: ipc.OpToolResult, ID: req.ID,
			Error: &ipc.ErrorPayload{Code: -32000, Message: "worker queue full or stopped"},
		})
		return
	}

	var res OutboundResult
	select {
	case res = <-resultCh:
	case <-time.After(workerJobTimeout):
		// A3: an EXITED worker already replied errWorkerStopped fast; this fires only
		// for a worker that genuinely STALLED. Return THIS tool call's clean error so
		// the read loop is not wedged on the never-written resultCh.
		_ = conn.WriteJSON(ipc.ToolResultMsg{
			Op: ipc.OpToolResult, ID: req.ID,
			Error: &ipc.ErrorPayload{Code: -32000, Message: fmt.Sprintf("worker did not respond within %s", workerJobTimeout)},
		})
		return
	}
	resp := ipc.ToolResultMsg{Op: ipc.OpToolResult, ID: req.ID}
	if res.Err != nil {
		resp.Error = &ipc.ErrorPayload{Code: -32000, Message: res.Err.Error()}
	} else {
		resp.Result = res.Result
	}
	_ = conn.WriteJSON(resp)
}

// handleAskRegister registers a blocking, correlated `ask`, sends the question to
// the stub's claimed route with an inline keyboard (single- or multi-select, with
// an optional Skip button per the req flags), and replies with
// a SYNCHRONOUS OpAskRegistered ack. The ANSWER is pushed later as an unsolicited
// OpAskResult when the human taps (resolveAsk, worker.go) — this handler does NOT
// block on it (mirrors OpInbound delivery, not the inline handleToolCall wait).
//
// AskRegisterReq carries no route, so the broker chooses the first held route
// (output first) that supports inline keyboards. An empty set fails fast. The
// pendingAsk is registered BEFORE the send (fast-tap race), and removed on a send
// error so the tool call returns immediately rather than after the answer timeout.
func (b *Broker) handleAskRegister(conn *ipc.Conn, stub *Stub, raw []byte) {
	var req ipc.AskRegisterReq
	if err := json.Unmarshal(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.AskRegisteredMsg{
			Op: ipc.OpAskRegistered, OK: false, Err: "malformed ask_register: " + err.Error(),
		})
		return
	}
	if req.AskID == "" {
		_ = conn.WriteJSON(ipc.AskRegisteredMsg{
			Op: ipc.OpAskRegistered, OK: false, Err: "ask_register: missing ask id",
		})
		return
	}
	routes := orderedHeldRoutes(stub)
	if len(routes) == 0 {
		_ = conn.WriteJSON(ipc.AskRegisteredMsg{
			Op: ipc.OpAskRegistered, AskID: req.AskID, OK: false,
			Err: "ask before attach: no route claimed",
		})
		return
	}
	// Single- and multi-select both require a non-empty option list (free-text /
	// Other are not yet supported; the adapter rejects them before this point).
	if len(req.Options) == 0 {
		_ = conn.WriteJSON(ipc.AskRegisteredMsg{
			Op: ipc.OpAskRegistered, AskID: req.AskID, OK: false,
			Err: "ask: at least one option is required (single/multi-select)",
		})
		return
	}
	route := routes[0]
	outputChannel, outputErr := b.Channel(route.Channel)
	ch, err := outputChannel, outputErr
	for _, candidate := range routes {
		candidateChannel, lookupErr := b.Channel(candidate.Channel)
		if lookupErr == nil && candidateChannel.Capabilities().InlineKeyboards {
			route = candidate
			ch = candidateChannel
			err = nil
			break
		}
	}
	if err != nil {
		_ = conn.WriteJSON(ipc.AskRegisteredMsg{
			Op: ipc.OpAskRegistered, AskID: req.AskID, OK: false,
			Err: fmt.Sprintf("channel lookup: %v", err),
		})
		return
	}
	// Capability gate (FIX-4): `ask` is an inline-keyboard round-trip, so refuse
	// it on a channel that can't render inline keyboards rather than silently
	// SendReply-ing buttons it will drop. No behavior change for Telegram
	// (InlineKeyboards=true); closes the latent gap for future text-only channels.
	// Checked BEFORE register, so there is no pending entry to clean up.
	if !ch.Capabilities().InlineKeyboards {
		_ = conn.WriteJSON(ipc.AskRegisteredMsg{
			Op: ipc.OpAskRegistered, AskID: req.AskID, OK: false,
			Err: "channel does not support interactive questions",
		})
		return
	}

	// Register BEFORE the send so a human who taps before the sendMessage
	// round-trip returns still finds a live ask to resolve. Phase 2 carries the
	// multi / allow_skip flags + per-option selection state (sized to the option
	// list) so toggles and the Done/Skip buttons resolve correctly.
	//
	// owner is THIS stub, the session that asked — not a holder re-read from the
	// routes table at register time. The route was selected from the stub's held set
	// above, so the asking session is already in hand; re-deriving it later reopens
	// the window in which a force_steal makes the NEW holder the owner of this
	// session's question (see pendingAsk.owner).
	p := &pendingAsk{
		askID: req.AskID, route: route, question: req.Question, options: req.Options,
		multi: req.Multi, allowSkip: req.AllowSkip, selected: make([]bool, len(req.Options)),
		owner: stub,
	}
	if !b.registerAsk(p) {
		_ = conn.WriteJSON(ipc.AskRegisteredMsg{
			Op: ipc.OpAskRegistered, AskID: req.AskID, OK: false,
			Err: "ask id collision — retry",
		})
		return
	}

	var topicID *int64
	if route.HasTopic {
		t := route.TopicID
		topicID = &t
	}
	msgID, err := ch.SendReply(c3types.ReplyArgs{
		Channel: route.Channel,
		ChatID:  route.ChatID,
		TopicID: topicID,
		Text:    req.Question,
		Buttons: askKeyboardFor(p),
	})
	if err != nil {
		// Send failed (oversized keyboard / Telegram error) — drop the pending and
		// return fast so the tool errors immediately, not after the answer timeout.
		b.Asks.delete(req.AskID)
		_ = conn.WriteJSON(ipc.AskRegisteredMsg{
			Op: ipc.OpAskRegistered, AskID: req.AskID, OK: false,
			Err: fmt.Sprintf("ask send failed: %v", err),
		})
		return
	}
	b.Asks.setMessageID(req.AskID, msgID)
	log.Printf("ask REGISTERED chan=%s chat=%d topic=%s ask=%s opts=%d msg=%d",
		route.Channel, route.ChatID, TopicKeyStr(route), req.AskID, len(req.Options), msgID)
	_ = conn.WriteJSON(ipc.AskRegisteredMsg{
		Op: ipc.OpAskRegistered, AskID: req.AskID, OK: true, MessageID: msgID,
	})
}

// handlePermissionRequest relays a Claude Code tool-use permission prompt to the
// first keyboard-capable held route as an Allow/Deny inline keyboard. Mirrors
// handleAskRegister (output-first capability selection, register-before-send, store
// messageID) but is FIRE-AND-FORGET: there is no blocking tool to unblock, so a
// nil route / channel error / send failure is logged and dropped with NO error
// reply (CC simply keeps waiting in its TUI). The operator's tap later pushes an
// OpPermissionVerdict (resolvePerm, worker.go).
func (b *Broker) handlePermissionRequest(_ *ipc.Conn, stub *Stub, raw []byte) {
	var req ipc.PermissionReq
	if err := json.Unmarshal(raw, &req); err != nil {
		log.Printf("perm: malformed permission_request: %v", err)
		return
	}
	if req.RequestID == "" {
		log.Printf("perm: permission_request missing request id — dropping")
		return
	}
	routes := orderedHeldRoutes(stub)
	if len(routes) == 0 {
		// No claim to surface on, and no blocking tool to error — drop + log.
		log.Printf("perm DROP id=%s tool=%s: no route claimed", req.RequestID, req.ToolName)
		return
	}
	var route *RouteKey
	var ch channel.Channel
	for _, candidate := range routes {
		candidateChannel, err := b.Channel(candidate.Channel)
		if err != nil || !candidateChannel.Capabilities().InlineKeyboards {
			continue
		}
		selected := candidate
		route = &selected
		ch = candidateChannel
		break
	}
	if route == nil {
		b.sendPermissionNotice(routes[0], req)
		return
	}
	for _, candidate := range routes {
		if candidate == *route {
			continue
		}
		candidateChannel, err := b.Channel(candidate.Channel)
		if err == nil && !candidateChannel.Capabilities().InlineKeyboards {
			b.sendPermissionNotice(candidate, req)
		}
	}

	// Register BEFORE the send so a fast operator tap (before the sendMessage
	// round-trip returns) still finds a live perm to resolve.
	//
	// owner is THIS stub, the session whose tool call is waiting — not a holder
	// re-read from the routes table at register time. The route came from
	// the held set, so the requesting session is already in hand; deriving
	// the owner later stamps whoever holds the route by then, which a force_steal
	// in that window makes a DIFFERENT session (see pendingPerm.owner).
	p := &pendingPerm{requestID: req.RequestID, route: *route, toolName: req.ToolName, preview: req.Preview, owner: stub}
	if !b.registerPerm(p) {
		log.Printf("perm DROP id=%s: request id collision or registry full", req.RequestID)
		return
	}

	var topicID *int64
	if route.HasTopic {
		t := route.TopicID
		topicID = &t
	}
	text := permPromptText(req.ToolName, req.Preview)
	// Fresh-install hardening (2026-06-30 live bug): with NO DM-paired operator
	// the resolvePerm sender-gate refuses EVERY tap, so a bare Allow/Deny
	// keyboard would be a trap. Say so on the prompt itself; the keyboard still
	// renders because pairing and then re-tapping works (the pending perm lives
	// permExpiryTTL).
	if len(b.Mappings().AllowlistOrEmpty().Users) == 0 {
		text += "\n\n" + permNoOperatorHint
	}
	msgID, err := ch.SendReply(c3types.ReplyArgs{
		Channel: route.Channel,
		ChatID:  route.ChatID,
		TopicID: topicID,
		Text:    text,
		// MarkupNone is load-bearing, not cosmetic. The body carries req.Preview —
		// the literal command the agent wants to run — and the zero Markup value
		// means MARKDOWN: the renderer turns *x* and _x_ into italics (eating the
		// characters, so a path or flag containing them displays differently from
		// what executes) and ||x|| into a Telegram spoiler that HIDES part of the
		// command behind a tap-to-reveal. The operator would be authorising a
		// string they were never shown. This prompt is the one control that turns
		// an untrusted tap into code execution on their machine, so it must render
		// verbatim. (v0.1.0 release audit, 2026-07-25.)
		Markup:  c3types.MarkupNone,
		Buttons: permKeyboard(req.RequestID),
	})
	if err != nil {
		// Send failed — drop the pending so a never-shown keyboard can't be resolved.
		b.Perms.delete(req.RequestID)
		log.Printf("perm DROP id=%s: send failed: %v", req.RequestID, err)
		return
	}
	if settled, ok := b.Perms.setMessageID(req.RequestID, msgID); ok {
		b.editPermMessage(settled.route, settled.requestID, settled.messageID, permSettledText(settled.toolName, settled.preview, settled.outcome, time.Now()), [][]c3types.Button{})
		log.Printf("perm settled chan=%s chat=%d topic=%s id=%s tool=%s outcome=%s msg=%d",
			settled.route.Channel, settled.route.ChatID, TopicKeyStr(settled.route), settled.requestID, settled.toolName, settled.outcome, settled.messageID)
		return
	}
	log.Printf("perm REGISTERED chan=%s chat=%d topic=%s id=%s tool=%s msg=%d",
		route.Channel, route.ChatID, TopicKeyStr(*route), req.RequestID, req.ToolName, msgID)
}

func (b *Broker) sendPermissionNotice(route RouteKey, req ipc.PermissionReq) {
	ch, err := b.Channel(route.Channel)
	if err != nil {
		log.Printf("perm NOTICE id=%s: channel lookup: %v", req.RequestID, err)
		return
	}
	var topicID *int64
	if route.HasTopic {
		topic := route.TopicID
		topicID = &topic
	}
	text := fmt.Sprintf("⏸ Permission needed at the laptop: %s — %s", req.ToolName, req.Preview)
	if _, err := ch.SendReply(c3types.ReplyArgs{
		Channel: route.Channel, ChatID: route.ChatID, TopicID: topicID,
		Text: text, Markup: c3types.MarkupNone,
	}); err != nil {
		log.Printf("perm NOTICE id=%s: channel %s send failed: %v", req.RequestID, route.Channel, err)
		return
	}
	log.Printf("perm NOTICE id=%s: channel %s requires laptop approval", req.RequestID, route.Channel)
}

func (b *Broker) handleListTopics(conn *ipc.Conn) {
	resp := ipc.TopicsListMsg{Op: ipc.OpTopicsList}
	for chanName, cc := range b.Mappings().Channels {
		for _, tp := range cc.Topics {
			entry := ipc.TopicEntry{
				Channel: chanName, ChatID: tp.ChatID,
				TopicID: tp.TopicID, Name: tp.Name, Group: tp.Group,
			}
			key := MakeRouteKey(chanName, tp.ChatID, ptrI64Val(tp.TopicID))
			if holder, ok := b.Routes.Holder(key); ok {
				// Liveness is checked HERE, at read time, not merely at claim
				// time. A holder that is disconnected AND whose PID is gone is
				// reaped and the topic reported free — otherwise `topics`
				// renders a ghost claim for a CLI that has exited, and disagrees
				// with `c3-broker status`, which has always reaped at read time
				// (handleListClaims below). Two views of one fact must not be
				// able to give different answers.
				if holder.IsAlive() {
					entry.ClaimedBy = &ipc.Holder{CLI: holder.CLI, PID: holder.PID, CWD: holder.CWD}
				} else {
					b.attempts.release(holder, "holder_death", time.Now())
					b.Routes.Release(key, holder.ConnID)
				}
			}
			resp.Topics = append(resp.Topics, entry)
		}
	}
	if b.isRegisteredEnabled("web") {
		if key, _, err := b.webOperatorRoute(); err == nil {
			entry := ipc.TopicEntry{
				Channel: "web", ChatID: key.ChatID, Name: "web", RouteKind: "dm",
			}
			if holder, ok := b.Routes.Holder(key); ok {
				if holder.IsAlive() {
					entry.ClaimedBy = &ipc.Holder{CLI: holder.CLI, PID: holder.PID, CWD: holder.CWD}
				} else {
					b.attempts.release(holder, "holder_death", time.Now())
					b.Routes.Release(key, holder.ConnID)
				}
			}
			resp.Topics = append(resp.Topics, entry)
		}
	}
	_ = conn.WriteJSON(resp)
}

// handleListClaims returns a snapshot of every live route claim. Used by
// `c3-broker status` (transient client) to render the live-claims section
// without dropping the apologetic note we used to ship.
func (b *Broker) handleListClaims(conn *ipc.Conn) {
	resp := ipc.ClaimsListMsg{Op: ipc.OpClaimsList}
	for _, e := range b.Routes.Snapshot() {
		if !e.Stub.IsAlive() {
			// Dead holder (disconnected AND PID gone): reap it and omit it so
			// `c3-broker status` never renders a ghost claim. An alive-but-
			// disconnected holder (brief reconnect window) still shows, labelled
			// as disconnected by the renderer. Snapshot returns a copy, so
			// Releasing during iteration is safe.
			b.attempts.release(e.Stub, "holder_death", time.Now())
			b.Routes.Release(e.Key, e.Stub.ConnID)
			continue
		}
		entry := ipc.ClaimEntry{
			Channel:   e.Key.Channel,
			ChatID:    e.Key.ChatID,
			HasTopic:  e.Key.HasTopic,
			TopicID:   e.Key.TopicID,
			HolderCLI: e.Stub.CLI,
			HolderPID: e.Stub.PID,
			HolderCWD: e.Stub.CWD,
			ConnID:    e.Stub.ConnID,
			Connected: e.Stub.IsConnected(),
		}
		render := e.Stub.RenderRouteFor(e.Key)
		entry.ConfirmedAt = render.Confirmed
		entry.ConfirmedTransport = render.Transport
		entry.RenderState, entry.RenderReason = render.State, render.Reason
		if output := e.Stub.OutputRoute(); output != nil && *output == e.Key {
			entry.IsOutput = true
		}
		if e.Key.HasTopic {
			if tp, ok := b.Mappings().LookupTopicByID(e.Key.Channel, e.Key.ChatID, e.Key.TopicID); ok {
				entry.TopicName = tp.Name
				entry.GroupName = tp.Group
			}
		}
		resp.Claims = append(resp.Claims, entry)
	}
	_ = conn.WriteJSON(resp)
}

// handleHealth returns a snapshot of the broker's per-channel cached
// fetch-health (the last HealthEvent per channel). Used by `c3-broker status`
// to render the "Channel health:" line. Mirrors handleListClaims.
func (b *Broker) handleHealth(conn *ipc.Conn) {
	resp := ipc.HealthListMsg{
		Op:            ipc.OpHealthList,
		QueueDegraded: b.Queue == nil,
	}
	for ch, ev := range b.lastHealthSnapshot() {
		entry := ipc.HealthEntry{
			Channel:   ch,
			State:     string(ev.State),
			SinceUnix: ev.Since.Unix(),
			Consec:    ev.Consec,
			Reason:    ev.Reason,
		}
		if ev.State == c3types.HealthStateDown {
			entry.DownForSec = int64(ev.DownFor.Seconds())
		}
		resp.Health = append(resp.Health, entry)
	}
	if webChannel, err := b.Channel("web"); err == nil {
		if provider, ok := webChannel.(channel.CertificateProvider); ok {
			if _, fingerprint, err := provider.CACertificatePEM(); err == nil {
				resp.WebCAFingerprint = fingerprint
			}
		}
	}
	_ = conn.WriteJSON(resp)
}

// handlePairModeStart arms a pairing window and returns the generated
// 4-digit code so the CLI can display it. Idempotent in the sense that
// re-arming the same surface generates a NEW code and extends the TTL —
// users running /c3:pair twice get fresh codes, no leftover stale ones.
func (b *Broker) handlePairModeStart(conn *ipc.Conn, raw []byte) {
	var req ipc.PairModeStartReq
	if err := json.Unmarshal(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.PairModeReplyMsg{
			Op: ipc.OpPairModeReply, OK: false,
			Err: "malformed pair_mode_start: " + err.Error(),
		})
		return
	}
	resp := ipc.PairModeReplyMsg{Op: ipc.OpPairModeReply, Target: req.Target, ChatID: req.ChatID}
	switch req.Target {
	case "dm":
		code, err := b.Pairing.StartDM()
		if err != nil {
			resp.Err = err.Error()
			_ = conn.WriteJSON(resp)
			return
		}
		resp.OK = true
		resp.Code = code
		resp.TTLSec = int(PairTTL.Seconds())
		log.Printf("pairing: DM ARMED via IPC — code=%s ttl=%v", code, PairTTL)
	case "group":
		if req.ChatID == 0 {
			resp.Err = "group pair requires chat_id"
			_ = conn.WriteJSON(resp)
			return
		}
		code, err := b.Pairing.StartGroup(req.ChatID)
		if err != nil {
			resp.Err = err.Error()
			_ = conn.WriteJSON(resp)
			return
		}
		resp.OK = true
		resp.Code = code
		resp.TTLSec = int(PairTTL.Seconds())
		log.Printf("pairing: group ARMED via IPC — chat=%d code=%s ttl=%v", req.ChatID, code, PairTTL)
	default:
		resp.Err = "target must be \"dm\" or \"group\""
	}
	_ = conn.WriteJSON(resp)
}

// handlePingThisSession dispatches a one-shot "this is me" reply on
// behalf of the slash command `/c3:ping` (TODO #19(b)). The calling
// client is the transient `c3-broker ping` subcommand, NOT the user's
// adapter — so we match the user's actual session by CWD against the
// live stub registry. The matched stub's claimed route is the target.
//
// Synchronous on the channel send so the slash command can surface
// failures (channel down, send error). Ping is rare; latency is fine.
func (b *Broker) handlePingThisSession(conn *ipc.Conn, raw []byte) {
	var req ipc.PingThisSessionReq
	if err := json.Unmarshal(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.PingThisSessionReplyMsg{
			Op: ipc.OpPingThisSessionReply, OK: false,
			Err: "malformed ping_this_session: " + err.Error(),
		})
		return
	}

	// Find the attached user session. Skip the transient client itself by
	// requiring a held route; the c3-broker-cli stub never attaches.
	//
	// Three-tier match (FIX 2, 2026-06-04), shared with /c3:sessions via
	// stubMatchesPID:
	//
	//  Tier 1 (primary, PID): if the caller supplied a PID hint (its
	//  best-effort walk up the PPID chain — see proctree.BestEffortCallerPID),
	//  match the stub whose PID equals it OR whose CLI-session ancestor pid
	//  equals it. The CLI-ancestor arm is essential: a Claude stub registers
	//  under its ADAPTER's pid (comm "c3-claude-adapt"), while the caller
	//  resolves the real claude pid — the adapter's PARENT. Without the
	//  ancestor arm, req.PID(claude) never equals stub.PID(adapter) and the
	//  ping reports "not attached" even when attached. PID is also the stable
	//  identity that survives the CWD collapse (claude launched from a parent
	//  dir, slash command run from a project subdir).
	//
	//  Tier 3 (tertiary fallback, CWD): when a PID hint WAS supplied but NO
	//  stub matched by pid-or-CLI-ancestor (the walk failed, or the stub
	//  registered under a pid we can't bridge), fall back to CWD-equality
	//  matching before giving up — a robustness add so a failed walk still
	//  has a chance. Also used when no PID hint was supplied at all (PID==0).
	//
	// Determinism: in every tier, when >1 stub matches (rare — a reconnect
	// re-registered the same logical session under a new ConnID before the
	// old stub was reaped, or two adapters share a project dir), pick the
	// one with the highest ConnID. The registry mints monotonic ConnIDs via
	// atomic.Uint64, so "highest" == "most recently registered" == "the
	// session the user most likely meant". Closes report MINOR m1
	// (2026-05-19) for the CWD tier; the same tiebreak holds in the PID tier.
	var target *Stub
	candidateCount := 0
	matchRule := "none"
	pidIdentifiedUnattached := false
	if req.PID != 0 {
		for _, s := range b.Stubs.Snapshot() {
			rule, ok := b.stubMatchesPID(s, req.PID)
			if !ok {
				continue
			}
			// A PID / CLI-ancestor match IS this session — even if it never
			// attached (e.g. auto-reattach-on-resume failed). Note it so we can
			// report "not attached" instead of falling through to the CWD tier,
			// which would impersonate a neighbor sharing the launch dir.
			if len(s.Routes()) == 0 {
				pidIdentifiedUnattached = true
				continue
			}
			candidateCount++
			if target == nil || s.ConnID > target.ConnID {
				target = s
				matchRule = rule
			}
		}
	}
	// The PID walk positively identified THIS session but it isn't attached: say
	// so, and never reach the CWD tier. Reporting a neighbor's topic as "this is
	// me" is the exact misattribution /c3:ping exists to prevent (2026-08-25).
	if target == nil && pidIdentifiedUnattached {
		_ = conn.WriteJSON(ipc.PingThisSessionReplyMsg{
			Op: ipc.OpPingThisSessionReply, OK: false,
			Err: "this session is not attached; use /c3:attach first",
		})
		return
	}
	// Tertiary CWD fallback: only when the PID hint was absent, or matched nothing
	// at all (attached or not) — never after a positive but unattached PID match.
	if target == nil {
		for _, s := range b.Stubs.Snapshot() {
			if len(s.Routes()) == 0 {
				continue
			}
			if req.CWD == "" || !completeIdentity(s.CLI, s.PID) {
				continue
			}
			if s.CWD != req.CWD {
				continue
			}
			candidateCount++
			if target == nil || s.ConnID > target.ConnID {
				target = s
				matchRule = "cwd"
			}
		}
	}
	if candidateCount > 1 && target != nil {
		log.Printf("ping: multiple stubs matched (rule=%s); targeting most recent (conn=%d pid=%d cwd=%q)",
			matchRule, target.ConnID, target.PID, target.CWD)
	}
	if target != nil {
		log.Printf("ping: matched by %s (req.pid=%d → conn=%d pid=%d cwd=%q)",
			matchRule, req.PID, target.ConnID, target.PID, target.CWD)
	}
	if target == nil {
		_ = conn.WriteJSON(ipc.PingThisSessionReplyMsg{
			Op: ipc.OpPingThisSessionReply, OK: false,
			Err: "not attached; use /c3:attach first",
		})
		return
	}

	key := target.OutputRoute()
	if key == nil {
		_ = conn.WriteJSON(ipc.PingThisSessionReplyMsg{
			Op: ipc.OpPingThisSessionReply, OK: false,
			Err: "not attached: no route; use /c3:attach first",
		})
		return
	}
	ch, err := b.Channel(key.Channel)
	if err != nil {
		_ = conn.WriteJSON(ipc.PingThisSessionReplyMsg{
			Op: ipc.OpPingThisSessionReply, OK: false,
			Err: fmt.Sprintf("channel lookup: %v", err),
		})
		return
	}

	label := pingTopicLabel(b, *key)
	text := pingText(target, label)
	var topicID *int64
	if key.HasTopic {
		t := key.TopicID
		topicID = &t
	}
	if _, err := ch.SendReply(c3types.ReplyArgs{
		Channel: key.Channel,
		ChatID:  key.ChatID,
		TopicID: topicID,
		Text:    text,
	}); err != nil {
		log.Printf("ping: send failed for %s: %v", routeKeyStr(*key), err)
		_ = conn.WriteJSON(ipc.PingThisSessionReplyMsg{
			Op: ipc.OpPingThisSessionReply, OK: false,
			Err: fmt.Sprintf("send: %v", err),
		})
		return
	}
	log.Printf("ping: sent for %s cli=%s cwd=%q", routeKeyStr(*key), target.CLI, target.CWD)
	_ = conn.WriteJSON(ipc.PingThisSessionReplyMsg{
		Op: ipc.OpPingThisSessionReply, OK: true,
		Channel: key.Channel, Topic: label, SentText: text,
	})
}

// stubMatchesPID reports whether the stub corresponds to the caller's
// resolved CLI session pid (reqPID), and names the rule that matched. A stub
// matches when EITHER:
//
//   - reqPID == stub.PID — the direct case (stub registered under the CLI
//     pid itself, or a non-adapter stub), OR
//   - reqPID == sessionPIDResolver(stub.PID) — the CLI-ancestor case: the
//     Claude adapter registers under its OWN pid (comm "c3-claude-adapt"),
//     so we walk up from stub.PID (strict predicate skips the adapter) to the
//     real claude/codex ancestor and compare that. This is THE bridge that
//     makes /c3:ping and /c3:sessions work for Claude stubs.
//
// reqPID==0 never matches (caller had no usable hint). The shared resolver is
// proctree.CLISessionPID by default; injectable for tests.
func (b *Broker) stubMatchesPID(s *Stub, reqPID int) (rule string, ok bool) {
	if reqPID == 0 {
		return "", false
	}
	if s.PID == reqPID {
		return "pid", true
	}
	if resolve := b.sessionPIDResolver; resolve != nil {
		if resolve(s.PID) == reqPID {
			return "cli-ancestor", true
		}
	}
	return "", false
}

// pingTopicLabel returns the human label for a route key — "dm" for
// non-topic routes, the topic's mapped name when known, else a
// "topic-<id>" fallback so we never surface a bare integer.
func pingTopicLabel(b *Broker, key RouteKey) string {
	if !key.HasTopic {
		return nonTopicRouteName(key.Channel)
	}
	if tp, ok := b.Mappings().LookupTopicByID(key.Channel, key.ChatID, key.TopicID); ok && tp.Name != "" {
		return tp.Name
	}
	return fmt.Sprintf("topic-%d", key.TopicID)
}

// pingText renders the one-shot identification message. Same
// home-shorten + cli-fallback conventions as welcomeText so the two
// messages look like siblings in the Telegram view.
func pingText(stub *Stub, label string) string {
	// Show the resolved project dir (launchCWD/topicName when it exists),
	// matching the on-attach welcome — not the bare launch dir. label IS
	// the topic name; resolveAttachCWD refines downward or returns the
	// launch cwd unchanged (and "" for the DM/no-cwd case).
	cwd := resolveAttachCWD(stub.CWD, label)
	if home, err := os.UserHomeDir(); err == nil && home != "" && strings.HasPrefix(cwd, home) {
		cwd = "~" + cwd[len(home):]
	}
	cli := stub.CLI
	if cli == "" {
		cli = "cli"
	}
	ts := time.Now().UTC().Format(time.RFC3339)
	if cwd == "" {
		return fmt.Sprintf("📍 c3-ping — %s attached to **%s** · pid %d · %s", cli, label, stub.PID, ts)
	}
	return fmt.Sprintf("📍 c3-ping\n📁 `%s`\n🤖 `%s` → **%s**\nPID %d · %s", cwd, cli, label, stub.PID, ts)
}

// handleListSessions returns a snapshot of every live adapter stub
// for the `/c3:sessions` slash command (TODO #19e, 2026-05-19). The
// transient client itself — CLI=="c3-broker-cli", set by dialBroker —
// is filtered out so the caller doesn't see itself listed. Entries
// are ordered descending by ConnID (most-recently-registered first)
// so the listing is deterministic regardless of map-iteration order.
//
// The handler also tags the entry whose PID matches the caller's PID
// hint (set by the CLI to its best-effort walk up the PPID chain from
// the slash command's shell-out — see cmd/c3-broker/sessions.go) with
// IsThisSession=true so the rendered table can mark "you are here".
func (b *Broker) handleListSessions(conn *ipc.Conn, raw []byte) {
	var req ipc.ListSessionsReq
	// Tolerate empty body: PID and CWD are both optional hints.
	_ = json.Unmarshal(raw, &req)

	snap := b.Stubs.Snapshot()
	entries := make([]ipc.SessionEntry, 0, len(snap))
	for _, s := range snap {
		// The transient sessions/topics/status/pair CLI connections
		// all hello as "c3-broker-cli" (see cmd/c3-broker/client.go::
		// dialBroker). We never want to list those — including the
		// /c3:sessions invocation itself, which is exactly such a
		// transient.
		if s.CLI == "c3-broker-cli" {
			continue
		}
		cli := s.CLI
		if cli == "" {
			// Defensive: an adapter that sent a blank CLI string would
			// otherwise render as a blank table column. Normalize to
			// "?" so the column always has SOMETHING.
			cli = "?"
		}
		e := ipc.SessionEntry{
			Build: s.Build, Stale: b.upgradeStale(s),
			CLI:    cli,
			PID:    s.PID,
			CWD:    s.CWD,
			ConnID: s.ConnID,
		}
		render := s.RenderRoute()
		e.RenderState, e.RenderReason = render.State, render.Reason
		e.ConfirmedAt = render.Confirmed
		e.ConfirmedTransport = render.Transport
		routes := orderedHeldRoutes(s)
		if len(routes) > 0 {
			labels := make([]string, 0, len(routes))
			for _, route := range routes {
				labels = append(labels, sessionTopicLabel(b, route))
			}
			e.AttachedTo = strings.Join(labels, ", ")
		}
		// "you are here" marker. Same PID-match as /c3:ping (FIX 2,
		// 2026-06-04): direct stub.PID equality OR the stub's CLI-session
		// ancestor pid (so a Claude stub registered under its adapter pid is
		// marked when the caller resolved the real claude pid).
		if _, ok := b.stubMatchesPID(s, req.PID); ok {
			e.IsThisSession = true
		}
		entries = append(entries, e)
	}
	// Most-recent first.
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].ConnID > entries[j].ConnID
	})
	_ = conn.WriteJSON(ipc.ListSessionsReplyMsg{
		Op:       ipc.OpListSessionsReply,
		Sessions: entries,
	})
}

// sessionTopicLabel formats the AttachedTo cell for /c3:sessions.
// Same conventions as pingTopicLabel for the "dm" / "topic-<id>"
// fallbacks; adds a "(group)" suffix when the topic has a non-empty
// Group set in mappings — so the user-visible cell looks like
// `c3 (main)` instead of just `c3`. Keeps pingTopicLabel unchanged
// (its consumer doesn't want the group qualifier).
func sessionTopicLabel(b *Broker, key RouteKey) string {
	if !key.HasTopic {
		return nonTopicRouteName(key.Channel)
	}
	tp, ok := b.Mappings().LookupTopicByID(key.Channel, key.ChatID, key.TopicID)
	if !ok || tp.Name == "" {
		return fmt.Sprintf("topic-%d", key.TopicID)
	}
	if tp.Group != "" {
		return fmt.Sprintf("%s (%s)", tp.Name, tp.Group)
	}
	return tp.Name
}

func ptrI64Val(v int64) *int64 { return &v }
