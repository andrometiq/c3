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
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

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

	// Protocol version: WARN on mismatch, never refuse. `c3 update` swaps the
	// binaries and exits the broker while adapter processes belonging to
	// already-running CLI sessions reconnect to the NEW broker with their OLD
	// code — mixed-version pairs are a normal event on a single host, not an
	// anomaly. Refusing here would turn every routine update into a hard outage;
	// a logged warning turns it into a diagnosable line. Absence (older adapter)
	// normalizes to v1 and logs nothing.
	if w := ipc.BrokerProtocolWarning(hello.CLI, hello.PID, hello.ProtocolVersion); w != "" {
		log.Print(w)
	}

	// Reconnect detection: if we have a disconnected stub for the same
	// (CLI, PID, CWD), this is the same adapter coming back after a brief
	// drop. Transfer its claims to a fresh ConnID instead of registering a
	// new stub and racing the original's claims. Per the "broker is the
	// authority" principle, claims survive conn drops as long as PID lives.
	var stub *Stub
	if existing := b.Routes.FindByLogicalSession(hello.CLI, hello.PID, hello.CWD); existing != nil {
		stub = b.Stubs.Register(hello.CLI, hello.PID, hello.CWD, conn)
		oldConnID := existing.ConnID
		// Carry the stable session id across a reconnect so a post-reconnect
		// recover op isn't needed to re-key recording — the same logical session
		// keeps its recovery identity. (No-op when the prior stub never received
		// one; non-hook sessions stay empty.)
		if sid := existing.StableSessionIDValue(); sid != "" {
			stub.SetStableSessionID(sid)
		}
		// Apply the render-capability flag BEFORE the claims transfer: the moment
		// the claim moves to this fresh stub the delivery path consults it, and
		// setting the flag only in the shared block below would leave a window
		// where one inbound could still take the push path into a host that
		// can't render. Idempotent with the shared block below.
		if hello.CannotRenderChannels {
			stub.SetCannotRender(true)
		}
		// Unregister the OLD stub (now superseded) and transfer its claims.
		b.Stubs.Unregister(oldConnID)
		b.Routes.TransferAllByConnID(oldConnID, stub)
		log.Printf("hello: RECONNECT cli=%s pid=%d cwd=%q old-conn=%d new-conn=%d (claims transferred)",
			hello.CLI, hello.PID, hello.CWD, oldConnID, stub.ConnID)
	} else {
		stub = b.Stubs.Register(hello.CLI, hello.PID, hello.CWD, conn)
		log.Printf("hello: NEW cli=%s pid=%d cwd=%q conn=%d",
			hello.CLI, hello.PID, hello.CWD, stub.ConnID)
	}

	// Record whether this host can render channel pushes (from the adapter's
	// /proc detection). Set for BOTH new and reconnect stubs so a reconnecting
	// adapter re-reports it. When it cannot render, forwardOrFallback holds this
	// holder's inbound in the durable queue instead of acking it lost — the
	// forked-session blackhole fix. Absent field (old adapter) → false →
	// renderable, no regression.
	if hello.CannotRenderChannels {
		stub.SetCannotRender(true)
		log.Printf("hello: cli=%s pid=%d conn=%d reports it CANNOT render channel pushes — inbound will be HELD (queue + fetch_queue)",
			hello.CLI, hello.PID, stub.ConnID)
	}

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
			log.Printf("conn-drop: cli=%s pid=%d cwd=%q conn=%d (claims preserved while pid alive)",
				stub.CLI, stub.PID, stub.CWD, stub.ConnID)
			return
		}
		released := b.Routes.ReleaseAllByConnID(stub.ConnID)
		log.Printf("conn-drop: cli=%s pid=%d cwd=%q conn=%d (PID dead — released %d claim(s))",
			stub.CLI, stub.PID, stub.CWD, stub.ConnID, len(released))
	}()
	defer b.Stubs.Unregister(stub.ConnID)

	ack := b.buildHelloAck(hello, stub)
	if err := conn.WriteJSON(ack); err != nil {
		return
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
		switch op {
		case ipc.OpAttach:
			b.handleAttach(conn, stub, raw)
		case ipc.OpListTopics:
			b.handleListTopics(conn)
		case ipc.OpListClaims:
			b.handleListClaims(conn)
		case ipc.OpListHealth:
			b.handleHealth(conn)
		case ipc.OpRelease:
			b.handleRelease(stub)
		case ipc.OpToolCall:
			b.handleToolCall(conn, stub, raw)
		case ipc.OpAskRegister:
			b.handleAskRegister(conn, stub, raw)
		case ipc.OpPermissionRequest:
			b.handlePermissionRequest(conn, stub, raw)
		case ipc.OpFetchQueue:
			b.handleFetchQueue(conn, stub, raw)
		case ipc.OpObserve:
			b.handleObserve(conn, stub, raw)
		case ipc.OpRetranscribe:
			b.handleRetranscribe(conn, stub, raw)
		case ipc.OpRecoverSession:
			b.handleRecoverSession(conn, stub, raw)
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
	if m, ok := b.Mappings().LookupByCwd(hello.CWD); ok && m.Channel != "" {
		chanName = m.Channel
	} else {
		chanName = b.defaultChannel()
	}
	ack.Capabilities = b.capsForChannel(chanName)
	return ack
}

// handleRelease drops the stub's claim on an explicit detach (OpRelease) and
// tombstones its session attachment so a later resume of the SAME session stays
// unattached — a deliberate detach is remembered. The dead-PID conn-drop path in
// HandleConn must NOT call this: that's a process exit, not a user detach, and
// tombstoning there would wipe recovery on every quit-without-detach (defeating
// the feature). Conn-drop releases claims directly via Routes.ReleaseAllByConnID.
func (b *Broker) handleRelease(stub *Stub) {
	b.Routes.ReleaseAllByConnID(stub.ConnID)
	stub.SetRoute(nil) // also clears routeConfirmed — a detach re-arms the §5 consume tripwire
	if sid := stub.StableSessionIDValue(); sid != "" {
		b.mutateMappings(func(mf *mappings.MappingsFile) {
			mf.TombstoneSessionAttachment(sid)
		})
		_ = b.SaveMappings()
	}
}

// handleRecoverSession is the adapter → broker recover op: the resumed session's
// STABLE id (learned from the SessionStart-hook handoff) arrives here on the
// stub's own connection, so the broker maps stub→stable-id directly. It then
// takes ONE of two dual-path-recording branches:
//
//   - Stub ALREADY attached (a fresh session bound by cwd before this arrived):
//     RECORD the current route under the stable id (so a FUTURE resume can
//     recover it) — do NOT re-claim or steal. resp.Recovered stays false.
//   - Stub NOT attached: attempt recoverSession — re-claim the route the stable
//     id was last attached to, when recoverable and not held by another live
//     session. On success, report it + the held backlog count so the adapter
//     can surface a one-shot auto-attach notification.
//
// Fail-closed: a malformed request or an empty stable id records nothing and
// recovers nothing.
func (b *Broker) handleRecoverSession(conn *ipc.Conn, stub *Stub, raw []byte) {
	var req ipc.RecoverSessionReq
	if err := json.Unmarshal(raw, &req); err != nil || req.StableSessionID == "" {
		_ = conn.WriteJSON(ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult, Err: "bad recover_session"})
		return
	}
	stub.SetStableSessionID(req.StableSessionID)
	resp := ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult}
	if cur := stub.CurrentRoute(); cur != nil {
		// Attach-first: a fresh session already claimed a route (by cwd) before
		// this recover op arrived. Record that route under the stable id so a
		// future resume recovers it. No re-claim. This is bookkeeping, not an
		// auto-re-attach, so it runs regardless of the auto_attach_on_resume gate.
		b.recordCurrentRouteForStable(stub, *cur)
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
		if sa, ok := b.Mappings().LookupSessionAttachment(req.StableSessionID); ok {
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
				resp.Name = "dm"
			}
		}
		// Guaranteed-visible confirmation: post a one-shot Telegram note to the
		// recovered topic. The adapter's CLI notice can be dropped by Claude Code
		// when it fires in the resume idle gap (2026-06-24), so the Telegram
		// message is the reliable signal that auto-attach-on-resume happened.
		// Async so a slow send never delays this recover response.
		go b.sendRecoverWelcome(stub, key, resp.Name, cnt)
	}
	_ = conn.WriteJSON(resp)
}

// handleToolCall dispatches a tool-call to the worker for the stub's
// currently-claimed route. The result returns asynchronously via the worker's
// OutboundJob.ResultCh; we block this connection's read loop on it (which is
// fine because the writer mutex on the Conn allows other goroutines —
// inbound forwarding — to write concurrently).
func (b *Broker) handleToolCall(conn *ipc.Conn, stub *Stub, raw []byte) {
	var req ipc.ToolCallReq
	if err := json.Unmarshal(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: "malformed tool_call: " + err.Error()})
		return
	}

	route := stub.CurrentRoute()
	if route == nil {
		_ = conn.WriteJSON(ipc.ToolResultMsg{
			Op: ipc.OpToolResult, ID: req.ID,
			Error: &ipc.ErrorPayload{Code: -32000, Message: "tool_call before attach: no route claimed"},
		})
		return
	}

	resultCh := make(chan OutboundResult, 1)
	job := Job{Kind: JobOutbound, Outbound: &OutboundJob{
		Tool:     req.Name,
		Args:     req.Args,
		ResultCh: resultCh,
	}}
	if !b.Workers.Submit(*route, job) {
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
// Route resolution mirrors handleToolCall: AskRegisterReq carries no route, so it
// is derived from stub.CurrentRoute(); a nil route returns OK=false fast. The
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
	route := stub.CurrentRoute()
	if route == nil {
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
	ch, err := b.Channel(route.Channel)
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
	p := &pendingAsk{
		askID: req.AskID, route: *route, question: req.Question, options: req.Options,
		multi: req.Multi, allowSkip: req.AllowSkip, selected: make([]bool, len(req.Options)),
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
		route.Channel, route.ChatID, TopicKeyStr(*route), req.AskID, len(req.Options), msgID)
	_ = conn.WriteJSON(ipc.AskRegisteredMsg{
		Op: ipc.OpAskRegistered, AskID: req.AskID, OK: true, MessageID: msgID,
	})
}

// handlePermissionRequest relays a Claude Code tool-use permission prompt to the
// stub's claimed route as an Allow/Deny inline keyboard. Mirrors handleAskRegister
// (route via stub.CurrentRoute, capability gate, register-before-send, store
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
	route := stub.CurrentRoute()
	if route == nil {
		// No claim to surface on, and no blocking tool to error — drop + log.
		log.Printf("perm DROP id=%s tool=%s: no route claimed", req.RequestID, req.ToolName)
		return
	}
	ch, err := b.Channel(route.Channel)
	if err != nil {
		log.Printf("perm DROP id=%s: channel lookup: %v", req.RequestID, err)
		return
	}
	// Capability gate: permission relay is an inline-keyboard round-trip, so refuse
	// it on a channel that can't render keyboards rather than silently dropping
	// buttons. No behavior change for Telegram (InlineKeyboards=true).
	if !ch.Capabilities().InlineKeyboards {
		log.Printf("perm DROP id=%s: channel %s does not support interactive keyboards", req.RequestID, route.Channel)
		return
	}

	// Register BEFORE the send so a fast operator tap (before the sendMessage
	// round-trip returns) still finds a live perm to resolve.
	p := &pendingPerm{requestID: req.RequestID, route: *route, toolName: req.ToolName, preview: req.Preview}
	if !b.registerPerm(p) {
		log.Printf("perm DROP id=%s: request id collision", req.RequestID)
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
	b.Perms.setMessageID(req.RequestID, msgID)
	log.Printf("perm REGISTERED chan=%s chat=%d topic=%s id=%s tool=%s msg=%d",
		route.Channel, route.ChatID, TopicKeyStr(*route), req.RequestID, req.ToolName, msgID)
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
	resp := ipc.HealthListMsg{Op: ipc.OpHealthList}
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
	// requiring CurrentRoute != nil; the c3-broker-cli stub never attaches.
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
	if req.PID != 0 {
		for _, s := range b.Stubs.Snapshot() {
			if s.CurrentRoute() == nil {
				continue
			}
			rule, ok := b.stubMatchesPID(s, req.PID)
			if !ok {
				continue
			}
			candidateCount++
			if target == nil || s.ConnID > target.ConnID {
				target = s
				matchRule = rule
			}
		}
	}
	// Tertiary CWD fallback: no PID hint, or PID hint matched nothing.
	if target == nil {
		for _, s := range b.Stubs.Snapshot() {
			if s.CurrentRoute() == nil {
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

	key := target.CurrentRoute()
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
		return "dm"
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
			CLI:    cli,
			PID:    s.PID,
			CWD:    s.CWD,
			ConnID: s.ConnID,
		}
		if rk := s.CurrentRoute(); rk != nil {
			e.AttachedTo = sessionTopicLabel(b, *rk)
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
		return "dm"
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
