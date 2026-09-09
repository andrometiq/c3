package main

import (
	"context"
	"net"
	"time"
)

// P2: eligibility requires the owning socket, including kernel peer identity.
// This probe sends no frames and never discloses credentials. Send revalidates
// the connected peer again, closing the pathname-substitution race (D031/R2).
func (a *adapter) validateDeliveryInbox() error {
	path, err := a.crossSession.validatedSocket()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return err
	}
	defer conn.Close()
	return validateCrossSessionPeer(conn, a.crossSession.hostPID)
}

type deliveryWrite struct {
	token     string
	observer  *deliveryObserver
	frame     map[string]any
	sessionID string
}

func (a *adapter) startDeliveryLoops(ctx context.Context) {
	if a.runCtx != nil {
		ctx = a.runCtx
	}
	a.deliveryLoopOnce.Do(func() { go a.observeDeliveries(ctx) })
	a.deliveryWriterOnce.Do(func() {
		a.deliveryWrites = make(chan deliveryWrite, maxLiveReadbacks)
		go a.writeDeliveries(ctx)
	})
}

// P6: "inbox is its own new 15 s attempt with a 2 s bounded write".
// One bounded writer per adapter, never a goroutine per attempt; the broker
// reader and transcript observer remain independent of socket write admission.
func (a *adapter) writeDeliveries(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-a.deliveryWrites:
			a.liveMu.Lock()
			current := a.deliveryAccepted.Load() && a.deliveryObservers[req.token] == req.observer
			if current && time.Now().Before(req.observer.deadline) {
				a.upgrade.deliveryWrites.Add(1)
			} else {
				current = false
			}
			a.liveMu.Unlock()
			if !current {
				continue
			}
			wc, cancel := context.WithDeadline(ctx, req.observer.deadline)
			reason := a.deliveryHostReason()
			if reason != "" {
				// Readiness may change while awaiting write admission.
				cancel()
				a.finishDeliveryWrite(req.token, req.observer, reason)
				a.upgrade.deliveryWrites.Add(-1)
				continue
			}
			if req.observer.cross {
				block, err := crossSessionChannelBlock(req.frame)
				if err == nil {
					err = a.crossSession.Send(wc, block, req.sessionID)
				}
				if err != nil {
					reason = "inbox write failed"
				}
			} else if a.notifyTx == nil || a.notifyTx.Notify(wc, "notifications/claude/channel", req.frame) != nil {
				reason = "channel notify write failed"
			}
			cancel()
			a.finishDeliveryWrite(req.token, req.observer, reason)
			a.upgrade.deliveryWrites.Add(-1)
		}
	}
}

func (a *adapter) finishDeliveryWrite(token string, observer *deliveryObserver, reason string) {
	a.liveMu.Lock()
	if a.deliveryObservers[token] != observer {
		a.liveMu.Unlock()
		return
	}
	observer.written = true
	if reason != "" {
		observer.result, observer.reason = "failed", reason
	}
	a.liveMu.Unlock()
	if reason != "" {
		a.reportDeliveryResult(token, observer)
	}
}
