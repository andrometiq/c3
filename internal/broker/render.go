package broker

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

// One sender per route, including across reconnects. Readbacks reserve the
// ordinary notice slot while the voice echo waits for its FIFO predecessor.
type routeNotices struct {
	mu     sync.Mutex
	routes map[RouteKey]*routeNotice
}
type routeNotice struct {
	heldRows      string
	heldVersion   uint64
	held, pending bool
	readbacks     int
}

func (b *Broker) handleRenderState(stub *Stub, raw []byte) {
	if stub.negotiated() {
		return
	}
	var msg ipc.RenderStateMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if msg.ReceiptShapeDrift != "" {
		stub.stubMu.Lock()
		stub.receiptShapeDrift = ipc.ReceiptHostVersion(msg.ReceiptShapeDrift)
		stub.stubMu.Unlock()
	}
	if msg.State == "" {
		return // diagnostic-only update; never rearm legacy probe admission
	}
	stub.SetRenderRoute(msg.State, msg.Reason, true)
	for _, key := range stub.Routes() {
		b.wakeDelivery(key)
	}
	b.notifyRenderRoute(stub)
}

func (b *Broker) notifyRenderRoute(stub *Stub) {
	for _, key := range stub.Routes() {
		b.evaluateNotices(key, false)
	}
}

// Called after scheduling, never between a failed attempt and its fallback.
func (b *Broker) evaluateNotices(key RouteKey, held bool) {
	b.notices.mu.Lock()
	start := b.updateNoticeLocked(key, held)
	b.notices.mu.Unlock()
	if start {
		go b.sendRenderNotice(key)
	}
}

func (b *Broker) noticeRoute(key RouteKey) ipc.RenderRoute {
	s, _ := b.Routes.Holder(key)
	r := ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "no session attached"}
	if s != nil {
		r = s.RenderRouteFor(key)
	}
	// Facts can change while the host is recording an already offered attempt.
	// Such an offer is still live, even if no further transport is eligible.
	if r.State == "pull_only" || r.State == ipc.RenderQueueOnly {
		for _, a := range b.attempts.snapshot(time.Now()) {
			if a.Route == key && b.noticeAttemptOpen(a) && a.Transport != "fetch" {
				if a.Negotiated {
					r.State = "waiting"
				} else {
					r.State = ipc.RenderProbing
				}
				r.Reason = ""
				break
			}
		}
	}
	return r
}

// Caller holds notices.mu. Queue identities keep concurrent evaluations from
// losing a new backlog while an outbound send is in progress.
func (b *Broker) updateNoticeLocked(key RouteKey, held bool) bool {
	n := &b.notices
	if n.routes == nil {
		n.routes = map[RouteKey]*routeNotice{}
	}
	r := n.routes[key]
	if r == nil {
		r = &routeNotice{}
		n.routes[key] = r
	}
	if held && b.Queue != nil {
		rows, err := b.queuedRows(key)
		if err == nil {
			signature := noticeRowSignature(rows)
			if signature != r.heldRows {
				r.held = len(rows) > 0
				r.heldRows = signature
				r.heldVersion++
			}
			// Open attempts are excluded from Held counts, but are not an empty
			// backlog episode: they can still return to the queue.
			if b.Queue.StatusFor(queueRouteKey(key)).Pending == 0 && b.HeldNotices != nil {
				b.HeldNotices.clearEpisode(key)
			}
		}
	}
	if !r.pending && r.held && r.readbacks == 0 {
		r.pending = true
		return true
	}
	return false
}

func (b *Broker) sendRenderNotice(key RouteKey) {
	for {
		b.notices.mu.Lock()
		version := b.notices.routes[key].heldVersion
		b.notices.mu.Unlock()
		rows, err := b.noticeSnapshot(key)
		b.notices.mu.Lock()
		r := b.notices.routes[key]
		if r.heldVersion != version {
			b.notices.mu.Unlock()
			continue
		}
		if err != nil || len(rows) == 0 || r.readbacks > 0 || !hasHeldSource(key, rows) {
			r.pending = false
			b.notices.mu.Unlock()
			return
		}
		var reservation time.Time
		if b.HeldNotices != nil {
			reservation = b.HeldNotices.reserveHeld(key)
		}
		if b.HeldNotices != nil && reservation.IsZero() {
			if key.Channel != "telegram" {
				delay := b.HeldNotices.remaining(key)
				b.notices.mu.Unlock()
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
					continue
				case <-b.ctx.Done():
					timer.Stop()
				}
				b.notices.mu.Lock()
			}
			r.pending = false
			b.notices.mu.Unlock()
			return
		}
		b.notices.mu.Unlock()
		ch, err := b.Channel(key.Channel)
		if err == nil {
			_, err = ch.SendReply(c3types.ReplyArgs{Channel: key.Channel, ChatID: key.ChatID,
				TopicID: topicPointer(key), ReplyTo: heldReplySource(key, rows), Text: heldReplyText(key.Channel, len(rows))})
		}
		b.notices.mu.Lock()
		if err == nil {
			if r.heldVersion == version {
				r.held = false
			}
		} else {
			if b.HeldNotices != nil {
				b.HeldNotices.cancelHeld(key, reservation)
			}
			log.Printf("delivery notice failed route=%s: %v", routeKeyStr(key), err)
		}
		if err == nil && r.heldVersion != version {
			b.notices.mu.Unlock()
			continue
		}
		r.pending = false
		b.notices.mu.Unlock()
		return
	}
}

func hasHeldSource(key RouteKey, rows []queue.TrackedInbound) bool {
	for _, row := range rows {
		if !row.Inbound.IsEvent() && (key.Channel != "telegram" || len(row.VoicePending) == 0) {
			return true
		}
	}
	return false
}

func heldReplySource(key RouteKey, rows []queue.TrackedInbound) *int64 {
	if key.Channel != "telegram" {
		return nil
	}
	for _, row := range rows {
		in := row.Inbound
		if !in.IsEvent() && in.DrainedFrom == "" && row.SourceRecordID == "" && len(row.VoicePending) == 0 && in.MessageID > 0 && MakeRouteKey(in.Channel, in.ChatID, in.TopicID) == key {
			return &in.MessageID
		}
	}
	return nil
}

func noticeRowSignature(rows []queue.TrackedInbound) string {
	var ids []string
	for _, row := range rows {
		ids = append(ids, row.RecordID+":"+rowRevision(row))
	}
	return strings.Join(ids, ",")
}

// Recount on the existing worker, after its scheduler, so a delayed sender
// cannot inspect an enqueue halfway between persistence and reservation.
// Idle routes acquire a worker too; a stopped pool permits offline inspection.
func (b *Broker) noticeSnapshot(key RouteKey) ([]queue.TrackedInbound, error) {
	if b.Workers == nil {
		return nil, errWorkerStopped
	}
	b.Workers.mu.Lock()
	w := b.Workers.workers[key]
	stopped := b.Workers.stopped
	b.Workers.mu.Unlock()
	if stopped && (w == nil || workerExited(w)) {
		// Offline/direct lifecycle fixtures have no concurrent worker. A running
		// broker always creates or reuses the owner, even for an idle route.
		return b.queuedRows(key)
	}
	done := make(chan BacklogResult, 1)
	if !b.Workers.Submit(key, Job{Kind: JobBacklog, Backlog: &BacklogJob{Notice: true, ResultCh: done}}) {
		return nil, errWorkerStopped
	}
	timer := time.NewTimer(workerJobTimeout)
	defer timer.Stop()
	select {
	case result := <-done:
		return result.NoticeRows, result.Err
	case <-b.ctx.Done():
		return nil, b.ctx.Err()
	case <-timer.C:
		return nil, errWorkerStopped
	}
}
