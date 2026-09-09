package broker

import (
	"fmt"
	"time"

	"github.com/Andrometiq/c3/internal/queue"
)

// Status is read on the queue's owning worker, including on idle routes. It
// does not schedule delivery or prepare identities as a side effect of a read.
func (b *Broker) statusSnapshot(key RouteKey) (queue.Status, error) {
	if b.Queue == nil {
		return queue.Status{}, nil
	}
	done := make(chan BacklogResult, 1)
	if b.Workers == nil || !b.Workers.Submit(key, Job{Kind: JobBacklog, Backlog: &BacklogJob{Status: true, ResultCh: done}}) {
		return queue.Status{}, errWorkerStopped
	}
	timer := time.NewTimer(workerJobTimeout)
	defer timer.Stop()
	select {
	case result := <-done:
		return result.Status, result.Err
	case <-b.ctx.Done():
		return queue.Status{}, b.ctx.Err()
	case <-timer.C:
		return queue.Status{}, fmt.Errorf("status snapshot timed out")
	}
}

func queuedStatus(rows []queue.TrackedInbound) queue.Status {
	st := queue.Status{Pending: len(rows)}
	var oldest, newest time.Time
	for _, row := range rows {
		stamp := row.Inbound.Timestamp
		if stamp.IsZero() {
			continue
		}
		if oldest.IsZero() || stamp.Before(oldest) {
			oldest = stamp
		}
		if newest.IsZero() || stamp.After(newest) {
			newest = stamp
		}
	}
	if !oldest.IsZero() {
		st.OldestUnix = oldest.Unix()
	}
	if !newest.IsZero() {
		st.NewestUnix = newest.Unix()
	}
	return st
}
