package broker

import (
	"context"
	"log"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// grantCheckTimeout bounds the grant_check answer: the PreToolUse hook gives
// its whole round trip 300 ms.
const grantCheckTimeout = 300 * time.Millisecond

// grantDeliveredWait bounds the wait for grant_delivered after an allow. The
// hook sends it as soon as it has printed.
const grantDeliveredWait = time.Second

// handleAutoClient serves the single op of a transient hook connection, whose
// first frame (helloFrame) is an ipc.HookHelloMsg. The connection is never registered
// as an adapter and can never claim a route. HandleConn closes it when this
// returns.
func (b *Broker) handleAutoClient(conn *ipc.Conn, helloFrame []byte) {
	var hello ipc.HookHelloMsg
	if ipc.DecodeStrict(helloFrame, &hello) != nil || ipc.PeerProtocolVersion(hello.ProtocolVersion) != ipc.ProtocolVersion {
		return
	}
	// Nothing below may hold the connection past the hook's own bound.
	ctx, cancel := context.WithTimeout(b.ctx, autoHookBudgetMax)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if conn.WriteJSONContext(ctx, ipc.HelloAckMsg{Op: ipc.OpHelloAck}) != nil {
		return
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		return
	}
	switch op, _ := ipc.PeekOp(raw); op {
	case ipc.OpAutoDenied:
		b.handleAutoDenied(conn, hello.CLI, raw)
	case ipc.OpGrantCheck:
		b.handleGrantCheck(conn, hello.CLI, raw)
	}
}

// handleAutoDenied runs one denial: it opens (or joins) the request, sends the
// card for a new one, and holds the connection through the handoff (§5.4).
// Every path that arms nothing ends with "none" or a closed connection, which
// the hook treats the same way.
func (b *Broker) handleAutoDenied(conn *ipc.Conn, helloCLI string, raw []byte) {
	received := time.Now()
	if !b.Mappings().AutoModeApprovalSettings().Enabled {
		writeAutoDecision(conn, received.Add(autoReplyTimeout), autoNone)
		return
	}
	var req ipc.AutoDeniedReq
	inputHash, refusal := decodeAutoDenied(raw, helloCLI, &req)
	waiter := &autoWaiter{conn: conn}
	var r *autoRequest
	var isNew bool
	if refusal == "" {
		key := autoKey{AutoCallContext: req.AutoCallContext, inputHash: inputHash}
		b.awaitAutoConfirmation(autoConsumedCall{key: key, toolUseID: req.ToolUseID})
		r, isNew, refusal = b.openAutoRequest(req, inputHash, waiter, received)
	}
	if r == nil {
		if refusal != "" {
			// The classifier reason is not tool input; it is logged (escaped and
			// capped) so an unknown form can be added deliberately (§5.7).
			log.Printf("auto-approval no-card session=%q tool=%q hash=%s classifier_reason=%q cause=%q",
				truncateRunes(req.SessionID, 100), truncateRunes(req.ToolName, 100), inputHash,
				truncateRunes(req.Reason, 200), refusal)
		}
		writeAutoDecision(conn, received.Add(autoReplyTimeout), autoNone)
		return
	}
	frames, disconnected := readAutoFrames(conn)
	defer b.dropAutoWaiter(r, waiter)
	if isNew {
		go b.sendAutoCard(r, req.ToolInput, req.Reason)
	}
	b.awaitAutoOutcome(r, waiter, frames, disconnected)
}

// decodeAutoDenied strictly decodes an auto_denied frame and hashes its
// tool_input. On failure it returns a fixed cause for the audit log instead of
// the decoder's error, which can quote input. A hook names its CLI in hello
// and in the op; they must agree.
func decodeAutoDenied(raw []byte, helloCLI string, req *ipc.AutoDeniedReq) (inputHash, refusal string) {
	if ipc.DecodeStrict(raw, req) != nil {
		return "", "malformed frame"
	}
	if req.CLI != helloCLI || req.SessionID == "" || req.CWD == "" || req.ToolName == "" {
		return "", "missing or mismatched call context"
	}
	inputHash, err := ipc.ToolInputHash(req.ToolInput)
	if err != nil {
		return "", "tool_input is not strict JSON"
	}
	return inputHash, ""
}

// readAutoFrames watches a waiting hook connection. HandleConn's dispatch loop
// is not reading it, so without this a hook that exits would go unnoticed
// until the deadline. A second unanswered frame is a protocol violation and
// counts as the hook going away.
func readAutoFrames(conn *ipc.Conn) (<-chan []byte, <-chan struct{}) {
	frames := make(chan []byte, 1)
	disconnected := make(chan struct{})
	go func() {
		defer close(disconnected)
		for {
			frame, err := conn.ReadFrame()
			if err != nil {
				return
			}
			select {
			case frames <- frame:
			default:
				return
			}
		}
	}()
	return frames, disconnected
}

// awaitAutoOutcome drives one waiter through the handoff: "approved" once the
// operator taps Allow, then "armed" after this waiter's ack arms the grant (or
// after another waiter's ack did), else "none" before the deadline. It returns
// when this waiter's exchange is over.
func (b *Broker) awaitAutoOutcome(r *autoRequest, waiter *autoWaiter, frames <-chan []byte, disconnected <-chan struct{}) {
	isApprovedSent := false
	for {
		b.auto.mu.Lock()
		isLive := b.refreshAutoLocked(r)
		state, changed, isArmed := r.state, r.changed, waiter.isArmed
		b.auto.mu.Unlock()
		switch {
		case isArmed:
			// Armed by another waiter while this one was connected. The grant is
			// already consumable (and may be consumed); telling this hook is best
			// effort, so it is not bound to r.ctx, which consumption cancels.
			writeAutoDecision(waiter.conn, r.deadline, r.decision(ipc.AutoDecisionArmed))
			return
		case !isLive:
			writeAutoDecision(waiter.conn, r.deadline, autoNone)
			return
		case state == autoApproved && !isApprovedSent:
			if waiter.conn.WriteJSONContext(r.ctx, r.decision(ipc.AutoDecisionApproved)) != nil {
				return
			}
			isApprovedSent = true
		}
		select {
		case <-disconnected:
			return
		case <-changed:
		case <-r.ctx.Done():
		case frame := <-frames:
			var ack ipc.AutoAckMsg
			if !isApprovedSent || ipc.DecodeStrict(frame, &ack) != nil || ack.Op != ipc.OpAutoAck || ack.RequestID != r.id {
				return
			}
			if b.armAutoGrant(r, waiter) {
				return
			}
		}
	}
}

// autoNone tells a hook there is no grant.
var autoNone = ipc.AutoDecisionMsg{Op: ipc.OpAutoDecision, State: ipc.AutoDecisionNone}

// writeAutoDecision writes one decision frame, unless deadline has passed:
// after the deadline the broker answers nothing (§5.4).
func writeAutoDecision(conn *ipc.Conn, deadline time.Time, decision ipc.AutoDecisionMsg) {
	if !time.Now().Before(deadline) {
		return
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	ctx, cancelReply := context.WithTimeout(ctx, autoReplyTimeout)
	defer cancelReply()
	_ = conn.WriteJSONContext(ctx, decision)
}

// handleGrantCheck answers the PreToolUse hook. Allow is true only for a grant
// this request consumed; every error is a plain false. The lookup is an exact
// key match, so a malformed hash simply matches nothing.
//
// After an allow it waits briefly for grant_delivered, the hook's word that it
// printed the allow. Without it, a later denial of the same call is not taken
// for a veto (vetoAutoLocked).
func (b *Broker) handleGrantCheck(conn *ipc.Conn, helloCLI string, raw []byte) {
	var req ipc.GrantCheckReq
	isAllowed := ipc.DecodeStrict(raw, &req) == nil && req.CLI == helloCLI && b.consumeAutoGrant(req)
	isDelivered := false
	if isAllowed && req.ToolUseID != "" {
		call := autoConsumedCall{key: autoKey{AutoCallContext: req.AutoCallContext, inputHash: req.InputHash},
			toolUseID: req.ToolUseID}
		defer func() { b.finishAutoConfirmation(call, isDelivered) }()
	}
	ctx, cancel := context.WithTimeout(b.ctx, grantCheckTimeout)
	defer cancel()
	err := conn.WriteJSONContext(ctx, ipc.GrantCheckResp{Op: ipc.OpGrantCheckResult, Allow: isAllowed})
	if err != nil || !isAllowed || req.ToolUseID == "" {
		return
	}
	frames := make(chan []byte, 1)
	go func() { // ends when HandleConn closes conn after this returns
		if frame, err := conn.ReadFrame(); err == nil {
			frames <- frame
		}
	}()
	select {
	case frame := <-frames:
		var delivered ipc.GrantDeliveredMsg
		isDelivered = ipc.DecodeStrict(frame, &delivered) == nil && delivered.Op == ipc.OpGrantDelivered &&
			delivered.ToolUseID == req.ToolUseID
	case <-time.After(grantDeliveredWait):
	case <-b.ctx.Done():
	}
}
