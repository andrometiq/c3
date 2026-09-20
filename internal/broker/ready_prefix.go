package broker

import (
	"fmt"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

func fetchModeError(mode string, owner *Stub) error {
	switch mode {
	case "":
		return nil
	case "ready_prefix":
		if !owner.intakeMetadataActive() {
			return fmt.Errorf("fetch_queue: ready_prefix requires the negotiated intake_metadata capability")
		}
		return nil
	default:
		return fmt.Errorf("fetch_queue: unknown mode")
	}
}

// Return selectable rows, the physical pending count, and the first blocker.
func (w *RouteWorker) intakeFetchRows(mode string) ([]queue.TrackedInbound, int, *ipc.BlockedOn, error) {
	if mode != "ready_prefix" {
		rows, err := w.visibleAttemptRows(-1)
		return rows, len(rows), nil, err
	}
	rows, err := w.broker.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if err != nil {
		return nil, 0, nil, err
	}
	reserved := map[string]bool{}
	for _, a := range w.openAttempts() {
		for _, member := range a.Members {
			reserved[member.ID] = true
		}
	}
	for i, row := range rows {
		reason := ""
		switch {
		case reserved[row.RecordID]:
			reason = "reserved"
		case row.Source == nil:
			reason = "no_source"
		case row.Inbound.DrainedFrom != "":
			reason = "frozen"
		case len(row.VoicePending) > 0:
			reason = "stt_pending"
		}
		if reason != "" {
			return rows[:i], len(rows), &ipc.BlockedOn{RecordID: row.RecordID, Reason: reason}, nil
		}
	}
	return rows, len(rows), nil, nil
}
