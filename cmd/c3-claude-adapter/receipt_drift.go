package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

const receiptDriftThreshold = 3

// Session-local diagnostics, guarded by liveMu. These never select transports,
// change deadlines, or authorize retirement. Preserve them through self-exec.
type receiptDiagnostics struct {
	HostVersion string
	FirstRecord string
	Consecutive int
	Drift       bool
}

type receiptObservation struct {
	size   int64
	grew   bool
	record string
}

func (o *receiptObservation) growth(path string) {
	if size, ok := transcriptOffset(path); ok {
		o.grew = o.grew || size > o.size
		o.size = size
	}
}

// Classify only AFTER the unchanged strict receipt predicate succeeds. No
// unknown envelope or token-shaped prose becomes receipt evidence here.
func (o *receiptObservation) scan(path string, offset int64, token string, discarding *bool, cross bool, attempt string) int64 {
	o.growth(path)
	next, _ := scanReceiptRecords(path, offset, discarding, func(line []byte) bool {
		if !deliveryReceipt(line, token, cross, attempt) {
			return false
		}
		var envelope struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(line, &envelope)
		o.record = envelope.Type
		switch o.record {
		case "queue-operation":
			o.record += "/enqueue"
		case "attachment":
			o.record += "/queued_command"
		}
		return true
	})
	return next
}

// liveMu held. Fetch confirmations log the first record too, but fetch expiry
// does not participate in the consecutive LIVE attempt alarm.
func (a *adapter) receiptConfirmed(record string) {
	d := &a.receiptDiagnostics
	if d.FirstRecord == "" && record != "" {
		d.FirstRecord = record
		log.Printf("first delivery confirmed by record=%s (host %s)", record, ipc.ReceiptHostVersion(d.HostVersion))
	}
}

// liveMu held; each attempt calls this once at its terminal observation.
func (a *adapter) receiptUnconfirmed(o receiptObservation, expired bool) {
	d := &a.receiptDiagnostics
	if !expired || !o.grew || o.record != "" {
		d.Consecutive = 0
		return
	}
	d.Consecutive++
	if d.Consecutive >= receiptDriftThreshold && !d.Drift {
		d.Drift = true
		log.Print(ipc.ReceiptShapeHint(ipc.ReceiptHostVersion(d.HostVersion)))
	}
}

// Evidence retries across a broken socket must not count an old outcome again
// after newer attempts have completed. liveMu held.
func (a *adapter) recordDeliveryReceiptOutcome(o *deliveryObserver, expired bool) {
	if o.receiptDone {
		return
	}
	o.receiptDone = true
	if o.result == "confirmed" {
		a.receiptDiagnostics.Consecutive = 0
		a.receiptConfirmed(o.receipt.record)
	} else {
		a.receiptUnconfirmed(o.receipt, expired && o.result == "")
	}
}

func (a *adapter) receiptDriftHost() string {
	a.liveMu.Lock()
	defer a.liveMu.Unlock()
	if a.receiptDiagnostics.Drift {
		return ipc.ReceiptHostVersion(a.receiptDiagnostics.HostVersion)
	}
	return ""
}

// Send diagnostic-only reports separately from eligibility: an older strict
// delivery_report parser can ignore them without losing a capability change.
// Legacy peers receive an empty-state diagnostic update; old brokers ignore
// it, so reporting can never reset their probe admission latch.
func (a *adapter) publishReceiptDrift() {
	a.livePublishMu.Lock()
	defer a.livePublishMu.Unlock()
	a.liveMu.Lock()
	conn := a.currentConn()
	if !a.receiptDiagnostics.Drift || conn == nil || conn == a.receiptDriftSent {
		a.liveMu.Unlock()
		return
	}
	host := ipc.ReceiptHostVersion(a.receiptDiagnostics.HostVersion)
	op := ipc.OpRenderState
	if a.deliveryAccepted.Load() {
		op = ipc.OpDeliveryReport
	}
	frame := struct {
		Op   ipc.Op `json:"op"`
		Host string `json:"receipt_shape_drift"`
	}{op, host}
	a.liveMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := conn.WriteJSONContext(ctx, frame)
	cancel()
	if err == nil {
		a.liveMu.Lock()
		a.receiptDriftSent = conn
		a.liveMu.Unlock()
	}
}
