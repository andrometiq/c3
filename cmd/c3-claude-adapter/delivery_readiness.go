package main

import (
	"context"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// G: "notifications/initialized (and the notify transport exists)". The atomic
// flag publishes startup's notifyTx assignment before the observer reads it;
// the transport is installed before the host can complete the MCP handshake.
func (a *adapter) deliveryHostReason() string {
	if !a.deliveryHostInitialized.Load() {
		return "host MCP initialization pending"
	}
	if a.notifyTx == nil {
		return "notify transport unavailable"
	}
	return ""
}

// G: definite failures report immediately, independent of the receipt ticker.
// Both the writer and the observer use this gate to avoid duplicate results.
// Retain evidence on IPC failure so a surviving observer can retry after reconnect.
func (a *adapter) reportDeliveryResult(token string, observer *deliveryObserver) {
	a.liveMu.Lock()
	conn := a.currentConn()
	if conn == nil || a.deliveryObservers[token] != observer || observer.result == "" || observer.reporting {
		a.liveMu.Unlock()
		return
	}
	observer.reporting = true
	frame := ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: token, Outcome: observer.result, Reason: observer.reason}
	a.liveMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := conn.WriteJSONContext(ctx, frame)
	cancel()
	a.liveMu.Lock()
	observer.reporting = false
	if err == nil && a.deliveryObservers[token] == observer {
		delete(a.deliveryObservers, token)
	}
	a.liveMu.Unlock()
}
