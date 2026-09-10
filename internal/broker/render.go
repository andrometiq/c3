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

const routeNoticeWindow = 60 * time.Second

// Ordinary notices share one sender per route, including across reconnects.
// Only successful sends advance announced; pending stays set through SendReply.
type routeNotices struct {
	mu     sync.Mutex
	routes map[RouteKey]*routeNotice
	window time.Duration // zero uses the production window; tests can shorten it
}
type routeNotice struct {
	route         ipc.RenderRoute
	since         time.Time
	announced     string
	heldRows      string
	heldVersion   uint64
	held, pending bool
	changed       chan struct{}
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

// Caller holds notices.mu. Read the current route inside the lock so concurrent
// wakeups cannot replace a newer candidate with an older snapshot.
func (b *Broker) updateNoticeLocked(key RouteKey, held bool) bool {
	n := &b.notices
	if n.routes == nil {
		n.routes = map[RouteKey]*routeNotice{}
	}
	r := n.routes[key]
	if r == nil {
		r = &routeNotice{changed: make(chan struct{}, 1)}
		n.routes[key] = r
	}
	route := b.noticeRoute(key)
	if r.route.Semantic() != route.Semantic() {
		r.since = time.Now()
		select {
		case r.changed <- struct{}{}:
		default:
		}
	}
	r.route = route
	if held && b.Queue != nil {
		rows, err := b.queuedRows(key)
		if err == nil {
			signature := noticeRowSignature(rows)
			if signature != r.heldRows {
				r.held = len(rows) > 0
				r.heldRows = signature
				r.heldVersion++
				select {
				case r.changed <- struct{}{}:
				default:
				}
			}
		}
	}
	if !r.pending && (r.held || r.announced != route.Semantic()) {
		r.pending = true
		return true
	}

	return false
}

func (b *Broker) sendRenderNotice(key RouteKey) {
	for {
		if b.ctx.Err() != nil {
			b.notices.mu.Lock()
			b.notices.routes[key].pending = false
			b.notices.mu.Unlock()
			return
		}
		b.notices.mu.Lock()
		version := b.notices.routes[key].heldVersion
		b.notices.mu.Unlock()
		rows, countErr := b.noticeSnapshot(key)
		b.notices.mu.Lock()
		b.updateNoticeLocked(key, false)
		r := b.notices.routes[key]
		if r.heldVersion != version {
			b.notices.mu.Unlock()
			continue
		}
		window := b.notices.window
		if window == 0 {
			window = routeNoticeWindow
		}
		routeChanged := r.announced != r.route.Semantic()
		delay := max(0, time.Until(r.since.Add(window)))
		text := ""
		heldSignature := ""
		route := r.route
		if r.held {
			count := len(rows)
			if countErr != nil {
				// Preserve the pending Held through an unreadable snapshot. The next
				// evaluation can retry the same identities without a new inbound.
				r.pending = false
				b.notices.mu.Unlock()
				return
			}
			if count == 0 || b.Queue == nil {
				r.held = false
				if r.heldRows != "" {
					r.heldRows = ""
					r.heldVersion++
				}
			} else if b.HeldNotices == nil || b.HeldNotices.ShouldSend(key) {
				text = heldReplyText(key.Channel, count) + "\n" + route.Text()
				heldSignature = noticeRowSignature(rows)
			} else {
				heldDelay := b.HeldNotices.remaining(key)
				if !routeChanged || heldDelay < delay {
					delay = heldDelay
				}
			}
		}
		if text == "" && routeChanged && time.Since(r.since) >= window {
			text = route.Text()
		}
		if text == "" && !routeChanged && !r.held {
			r.pending = false
			b.notices.mu.Unlock()
			return
		}
		changed := r.changed
		b.notices.mu.Unlock()
		if text == "" {
			timer := time.NewTimer(max(time.Millisecond, delay))
			select {
			case <-b.ctx.Done():
				timer.Stop()
				b.notices.mu.Lock()
				r.pending = false
				b.notices.mu.Unlock()
				return
			case <-changed:
				timer.Stop()
			case <-timer.C:
			}
			continue
		}
		ch, err := b.Channel(key.Channel)
		if err == nil {
			var topic *int64
			if key.HasTopic {
				id := key.TopicID
				topic = &id
			}
			_, err = ch.SendReply(c3types.ReplyArgs{Channel: key.Channel, ChatID: key.ChatID, TopicID: topic, Text: text})
		}
		b.notices.mu.Lock()
		if err == nil {
			// Held includes the calm route line, so it also counts as an announcement.
			r.announced = route.Semantic()
			if heldSignature != "" && (r.heldVersion == version || r.heldRows == heldSignature) {
				r.held = false
				r.heldRows = heldSignature
			}
		} else {
			log.Printf("delivery notice failed route=%s: %v", routeKeyStr(key), err)
			r.pending = false
			b.notices.mu.Unlock()
			return
		}
		b.notices.mu.Unlock()
	}
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
