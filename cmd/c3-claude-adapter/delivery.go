package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

type deliveryObserver struct {
	path       string
	offset     int64
	deadline   time.Time
	attempt    string
	discarding bool
	result     string
	reason     string
}

// P1/P9: "the adapter OFFERS ONLY WHEN channel is eligible". Detection remains
// fail-closed; a flagless host retains its legacy adapter-owned inbox fallback.
func (a *adapter) deliveryFacts() ipc.DeliveryLive {
	route := a.initialRenderRoute
	if a.deliveryHostRoute != nil {
		route = a.deliveryHostRoute()
	}
	channel := ipc.DeliveryEligibility{Eligible: route.State == ipc.RenderCapable || route.State == ipc.RenderProbing, Reason: route.Reason}
	if channel.Eligible {
		channel.Reason = ""
		if _, ok := transcriptOffset(a.livePath()); !ok {
			channel.Eligible = false
			channel.Reason = "session transcript unavailable"
		}
	}
	inbox := ipc.DeliveryEligibility{Reason: "no owning session socket"}
	if a.crossSession != nil {
		if _, err := a.crossSession.validatedSocket(); err == nil {
			inbox = ipc.DeliveryEligibility{Eligible: true}
		}
	}
	return ipc.DeliveryLive{Channel: channel, Inbox: inbox}
}
func (a *adapter) deliveryOffer() json.RawMessage {
	live := a.deliveryFacts()
	if !live.Channel.Eligible {
		return nil
	}
	data, _ := json.Marshal(ipc.DeliveryOffer{Version: 1, Live: live, Receipts: "transcript", Fetch: "receipt"})
	return data
}
func (a *adapter) acceptDelivery(offer json.RawMessage, accepted *ipc.DeliveryAcceptance) {
	enabled := len(offer) > 0 && accepted.ChannelOnly()
	a.deliveryAccepted.Store(enabled)
	a.liveMu.Lock()
	if !enabled {
		a.deliveryObservers = nil
		a.deliveryRoutes = nil
	} else {
		a.deliveryLastFacts = a.deliveryFacts()
		if a.renderRoute.State != "live_channel" && a.renderRoute.State != "pull_only" {
			a.renderRoute = ipc.RenderRoute{State: "waiting"}
		}
	}
	a.liveMu.Unlock()
}
func (a *adapter) handleDeliver(ctx context.Context, raw []byte) {
	if !a.deliveryAccepted.Load() {
		return
	}
	var msg ipc.DeliverMsg
	if ipc.StrictJSON(raw, &msg) != nil || msg.Token == "" || msg.Transport != "channel" || msg.DeadlineMS <= 0 || msg.DeadlineMS > 15000 {
		return
	}
	deadline := time.Now().Add(time.Duration(msg.DeadlineMS) * time.Millisecond)
	frame := buildClaudeChannelFrame(&msg.Inbound)
	if content, ok := frame["content"].(string); ok {
		frame["content"] = a.originTag(&msg.Inbound) + content
	}
	path := a.livePath()
	offset, available := transcriptOffset(path)
	a.liveMu.Lock()
	if a.deliveryObservers[msg.Token] != nil {
		a.liveMu.Unlock()
		return
	}
	if len(a.deliveryObservers) >= maxLiveReadbacks {
		a.liveMu.Unlock()
		if conn := a.currentConn(); conn != nil {
			writeLiveFrame(conn, ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: msg.Token, Outcome: "failed", Reason: "observation limit reached"})
		}
		return
	}
	a.liveAttempt++
	attempt := fmt.Sprintf("channel:%d", a.liveAttempt)
	observer := &deliveryObserver{path: path, offset: offset, deadline: deadline, attempt: attempt}
	if !available {
		observer.result = "failed"
		observer.reason = "session transcript unavailable"
	}
	if a.deliveryObservers == nil {
		a.deliveryObservers = map[string]*deliveryObserver{}
	}
	a.deliveryObservers[msg.Token] = observer
	a.liveMu.Unlock()
	a.deliveryLoopOnce.Do(func() {
		loopCtx := ctx
		if a.runCtx != nil {
			loopCtx = a.runCtx
		}
		go a.observeDeliveries(loopCtx)
	})
	if !available {
		return
	}
	meta := frame["meta"].(map[string]any)
	meta["c3_delivery_id"] = msg.Token
	meta["c3_attempt"] = attempt
	writeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if a.notifyTx == nil || a.notifyTx.Notify(writeCtx, "notifications/claude/channel", frame) != nil {
		a.liveMu.Lock()
		if current := a.deliveryObservers[msg.Token]; current == observer {
			observer.result = "failed"
			observer.reason = "channel notify write failed"
		}
		a.liveMu.Unlock()
	}
}

// Observation survives a socket reconnect of this adapter process. It never
// retries delivery, selects a fallback, or promotes/downgrades route state.
func (a *adapter) observeDeliveries(ctx context.Context) {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if !a.deliveryAccepted.Load() {
			continue
		}
		a.pollDeliveries()
	}
}
func (a *adapter) pollDeliveries() {
	facts := a.deliveryFacts()
	conn := a.currentConn()
	a.liveMu.Lock()
	changed := facts != a.deliveryLastFacts
	if conn != nil && changed {
		a.deliveryLastFacts = facts
	}
	type result struct {
		token    string
		observer *deliveryObserver
		frame    ipc.AttemptResultMsg
	}
	var results []result
	for token, o := range a.deliveryObservers {
		if !time.Now().Before(o.deadline) {
			delete(a.deliveryObservers, token)
			continue
		}
		if o.result == "" {
			if _, ok := transcriptOffset(o.path); !ok {
				o.result = "failed"
				o.reason = "session transcript unavailable"
			} else {
				a.liveScanMu.Lock()
				offset, found := scanReceipt(o.path, o.offset, token, &o.discarding, false, o.attempt)
				a.liveScanMu.Unlock()
				o.offset = offset
				if found {
					o.result = "confirmed"
				}
			}
		}
		if o.result != "" && conn != nil {
			results = append(results, result{token, o, ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: token, Outcome: o.result, Reason: o.reason}})
		}
	}
	a.liveMu.Unlock()
	if changed && conn != nil {
		writeLiveFrame(conn, ipc.DeliveryReportMsg{Op: ipc.OpDeliveryReport, Live: facts})
	}
	for _, r := range results {
		wc, cancel := context.WithTimeout(context.Background(), time.Second)
		err := conn.WriteJSONContext(wc, r.frame)
		cancel()
		if err == nil {
			a.liveMu.Lock()
			if a.deliveryObservers[r.token] == r.observer {
				delete(a.deliveryObservers, r.token)
			}
			a.liveMu.Unlock()
		}
	}
}
func (a *adapter) handleDeliveryState(raw []byte) {
	if !a.deliveryAccepted.Load() {
		return
	}
	var msg ipc.RenderStateMsg
	if json.Unmarshal(raw, &msg) != nil {
		return
	}
	a.liveMu.Lock()
	if a.deliveryRoutes == nil {
		a.deliveryRoutes = map[routeKey]ipc.RenderRoute{}
	}
	a.deliveryRoutes[routeKeyFor(msg.Channel, msg.ChatID, msg.TopicID)] = msg.RenderRoute
	a.renderRoute = msg.RenderRoute
	a.liveMu.Unlock()
}
