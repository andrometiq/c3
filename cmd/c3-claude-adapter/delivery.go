package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

type deliveryObserver struct {
	injected   bool
	path       string
	offset     int64
	deadline   time.Time
	attempt    string
	discarding bool
	result     string
	reason     string
	cross      bool
	written    bool
	reporting  bool
}

// P1/P2: offer when "channel OR inbox is eligible"; both need a readable transcript.
func (a *adapter) deliveryFacts() ipc.DeliveryLive {
	if a.upgrade.quiescing.Load() {
		unavailable := ipc.DeliveryEligibility{Reason: "adapter upgrade pending"}
		return ipc.DeliveryLive{Channel: unavailable, Inbox: unavailable}
	}
	if reason := a.deliveryHostReason(); reason != "" {
		unavailable := ipc.DeliveryEligibility{Reason: reason}
		return ipc.DeliveryLive{Channel: unavailable, Inbox: unavailable}
	}
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
		if _, ok := transcriptOffset(a.livePath()); !ok {
			inbox.Reason = "session transcript unavailable"
		} else if channel.Eligible && !a.deliveryAccepted.Load() {
			// A channel offer can negotiate before probing the optional inbox.
			// Old brokers therefore retain the channel path's exact socket behavior.
			inbox.Reason = "owning session socket not yet verified"
		} else if err := a.validateDeliveryInbox(); err == nil {
			inbox = ipc.DeliveryEligibility{Eligible: true}
		} else {
			inbox.Reason = "owning session socket unavailable or invalid"
		}
	}
	return ipc.DeliveryLive{Channel: channel, Inbox: inbox}
}
func (a *adapter) deliveryOffer() json.RawMessage {
	return deliveryOfferFor(a.deliveryFacts())
}
func deliveryOfferFor(live ipc.DeliveryLive) json.RawMessage {
	if !live.Channel.Eligible && !live.Inbox.Eligible {
		return nil
	}
	data, _ := json.Marshal(ipc.DeliveryOffer{Version: 1, Live: live, Receipts: "transcript", Fetch: "receipt"})
	return data
}
func (a *adapter) acceptDelivery(offer json.RawMessage, accepted *ipc.DeliveryAcceptance) {
	offered := ipc.ParseDeliveryOffer(offer)
	enabled := accepted.AcceptsOffer(offered)
	inbox := enabled && accepted.HasMode("inbox")
	a.deliveryAccepted.Store(enabled)
	a.liveMu.Lock()
	a.deliveryInboxAccepted = inbox
	a.deliveryChannelAccepted = enabled && accepted.HasMode("channel")
	if !enabled {
		a.deliveryObservers = nil
		a.deliveryRoutes = nil
	} else {
		// Stop every legacy observer and invalidate its retry/probe latches.
		a.liveGeneration++
		a.livePending = nil
		a.liveCrossSession = false
		a.deliveryLastFacts = offered.Live
		if a.renderRoute.State != "live_channel" && a.renderRoute.State != "live_inbox" && a.renderRoute.State != "pull_only" {
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
	if ipc.StrictJSON(raw, &msg) != nil || msg.Token == "" || (msg.Transport != "channel" && msg.Transport != "inbox") || msg.DeadlineMS <= 0 || msg.DeadlineMS > 15000 {
		return
	}
	deadline := time.Now().Add(time.Duration(msg.DeadlineMS) * time.Millisecond)
	frame := buildClaudeChannelFrame(&msg.Inbound)
	if content, ok := frame["content"].(string); ok {
		frame["content"] = a.originTag(&msg.Inbound) + content
	}
	path := a.livePath()
	offset, available := transcriptOffset(path)
	failure := a.deliveryHostReason()
	if failure == "" && !available {
		failure = "session transcript unavailable"
	}
	a.liveMu.Lock()
	if a.upgrade.quiescing.Load() {
		failure = "adapter upgrade pending"
	}
	if (msg.Transport == "inbox" && !a.deliveryInboxAccepted) || (msg.Transport == "channel" && !a.deliveryChannelAccepted) {
		a.liveMu.Unlock()
		return
	}
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
	attempt := fmt.Sprintf("%s:%d", msg.Transport, a.liveAttempt)
	observer := &deliveryObserver{injected: msg.Inbound.TestInjected, path: path, offset: offset, deadline: deadline, attempt: attempt, cross: msg.Transport == "inbox"}
	if failure != "" {
		observer.result = "failed"
		observer.reason = failure
	}
	if a.deliveryObservers == nil {
		a.deliveryObservers = map[string]*deliveryObserver{}
	}
	a.deliveryObservers[msg.Token] = observer
	if observer.injected {
		log.Printf("TEST DELIVERY token=%s attempt=%s transport=%s budget_ms=%d", msg.Token, attempt, msg.Transport, msg.DeadlineMS)
	}
	a.liveMu.Unlock()
	a.startDeliveryLoops(ctx)
	if failure != "" {
		a.reportDeliveryResult(msg.Token, observer)
		return
	}
	meta := frame["meta"].(map[string]any)
	meta["c3_delivery_id"] = msg.Token
	meta["c3_attempt"] = attempt
	sessionID := ""
	if observer.cross {
		if entry, ok := a.currentStableIdentity(); ok {
			sessionID = entry.StableSessionID
		} else if entry, ok := resolveTerminalHandoff(instanceIDFromEnv()); ok {
			sessionID = entry.StableSessionID
		}
	}
	select {
	case a.deliveryWrites <- deliveryWrite{token: msg.Token, observer: observer, frame: frame, sessionID: sessionID}:
	default:
		a.finishDeliveryWrite(msg.Token, observer, "write admission full")
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
		a.flushUpgradeNotice()
		a.pollUpgrade(time.Now())
		if !a.deliveryAccepted.Load() {
			a.pollDeliveryRehello(a.deliveryFacts(), time.Now())
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
	}
	var results []result
	for token, o := range a.deliveryObservers {
		if !time.Now().Before(o.deadline) {
			delete(a.deliveryObservers, token)
			continue
		}
		if o.result == "" && o.written {
			if _, ok := transcriptOffset(o.path); !ok {
				o.result = "failed"
				o.reason = "session transcript unavailable"
			} else {
				a.liveScanMu.Lock()
				offset, found := scanReceipt(o.path, o.offset, token, &o.discarding, o.cross, o.attempt)
				a.liveScanMu.Unlock()
				o.offset = offset
				if found {
					o.result = "confirmed"
				}
			}
		}
		if o.result != "" && conn != nil {
			results = append(results, result{token, o})
		}
	}
	a.liveMu.Unlock()
	if changed && conn != nil {
		writeLiveFrame(conn, ipc.DeliveryReportMsg{Op: ipc.OpDeliveryReport, Live: facts})
	}
	for _, r := range results {
		a.reportDeliveryResult(r.token, r.observer)
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
