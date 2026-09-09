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
	held, pending bool
	changed       chan struct{}
}

func (b *Broker) handleRenderState(stub *Stub, raw []byte) {
	if stub.negotiated() {
		return
	}
	var msg ipc.RenderStateMsg
	if err := json.Unmarshal(raw, &msg); err != nil || msg.State == "" {
		return
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
			if a.Route == key && a.Outcome == "open" && a.Transport != "fetch" {
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
		rows, countErr := b.noticeSnapshot(key)
		b.notices.mu.Lock()
		b.updateNoticeLocked(key, false)
		r := b.notices.routes[key]
		window := b.notices.window
		if window == 0 {
			window = routeNoticeWindow
		}
		routeChanged := r.announced != r.route.Semantic()
		delay := max(0, time.Until(r.since.Add(window)))
		text := ""
		route := r.route
		if r.held {
			count := len(rows)
			if countErr != nil || count == 0 || b.Queue == nil {
				r.held = false
			} else if b.HeldNotices == nil || b.HeldNotices.ShouldSend(key) {
				text = heldReplyText(key.Channel, count) + "\n" + route.Text()
				r.heldRows = noticeRowSignature(rows)
				r.held = false
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
// An idle route (or a direct unit fixture) has no concurrent queue mutation.
func (b *Broker) noticeSnapshot(key RouteKey) ([]queue.TrackedInbound, error) {
	var w *RouteWorker
	if b.Workers != nil {
		b.Workers.mu.Lock()
		w = b.Workers.workers[key]
		b.Workers.mu.Unlock()
	}
	if w == nil || workerExited(w) {
		return b.queuedRows(key)
	}
	done := make(chan BacklogResult, 1)
	if !w.Submit(Job{Kind: JobBacklog, Backlog: &BacklogJob{Notice: true, ResultCh: done}}) {
		return nil, errWorkerStopped
	}
	timer := time.NewTimer(workerJobTimeout)
	defer timer.Stop()
	select {
	case result := <-done:
		return result.NoticeRows, result.Err
	case <-w.Done():
		return nil, errWorkerStopped
	case <-b.ctx.Done():
		return nil, b.ctx.Err()
	case <-timer.C:
		return nil, errWorkerStopped
	}
}
