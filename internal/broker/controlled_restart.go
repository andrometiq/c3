package broker

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

const (
	RestartPromptGrace               = 60 * time.Second
	RestartCancellationNoticeTimeout = 5 * time.Second
	restartAskRefused                = "C3 is restarting; new questions are paused — ask again after reconnect."
	restartPermRefused               = "C3 is restarting; new permission relays are paused. This request may still be waiting at the laptop."
	restartAskCancelled              = "C3 is restarting; this request was cancelled — ask again."
	restartPermCancelled             = "C3 is restarting; this permission relay was cancelled. The request may still be waiting at the laptop — cancel it there and ask again."
	restartPermTap                   = "C3 is restarting; this permission relay was cancelled. Check the requesting session."
)

type promptDrain struct {
	mu        sync.Mutex
	draining  atomic.Bool
	sealed    atomic.Bool
	first     time.Time
	active    int
	ctx       context.Context
	cancel    context.CancelFunc
	abandoned bool
}

func (d *promptDrain) begin(registration bool) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sealed.Load() || registration && d.draining.Load() {
		return false
	}
	d.active++
	return true
}
func (d *promptDrain) end() { d.mu.Lock(); d.active--; d.mu.Unlock() }

// BeginControlledRestart synchronously closes admission. Repeated intent keeps
// the first deadline; no socket or channel work runs under this lock.
func (b *Broker) BeginControlledRestart() time.Time {
	b.prompts.mu.Lock()
	defer b.prompts.mu.Unlock()
	if !b.prompts.draining.Load() {
		b.prompts.ctx, b.prompts.cancel = context.WithCancel(context.Background())
		b.prompts.first = time.Now()
		b.prompts.draining.Store(true)
	}
	return b.prompts.first
}

// AbandonControlledRestart leaves prompt state to ordinary process teardown.
func (b *Broker) AbandonControlledRestart() {
	b.prompts.mu.Lock()
	defer b.prompts.mu.Unlock()
	b.prompts.abandoned = true
	if b.prompts.cancel != nil {
		b.prompts.cancel()
	}
}

func (b *Broker) restartContext() context.Context {
	b.prompts.mu.Lock()
	defer b.prompts.mu.Unlock()
	if b.prompts.ctx == nil {
		return context.Background()
	}
	return b.prompts.ctx
}

func (b *Broker) DrainRequests() {
	deadline := b.BeginControlledRestart().Add(RestartPromptGrace)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	b.drainRequestsUntil(deadline, time.Now, ticker.C)
}

func (b *Broker) drainRequestsUntil(deadline time.Time, now func() time.Time, ticks <-chan time.Time) {
	ctx := b.restartContext()
	for now().Before(deadline) {
		b.prompts.mu.Lock()
		if b.prompts.abandoned {
			b.prompts.mu.Unlock()
			return
		}
		b.Asks.mu.Lock()
		b.Perms.mu.Lock()
		empty := len(b.Asks.m)+len(b.Perms.m)+b.prompts.active == 0
		if empty {
			b.prompts.sealed.Store(true)
		}
		b.Perms.mu.Unlock()
		b.Asks.mu.Unlock()
		b.prompts.mu.Unlock()
		if empty {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticks:
		}
	}
	b.cancelRestartPrompts()
}

type restartNotice struct {
	kind, id  string
	route     RouteKey
	messageID int64
	text      string
	owner     *Stub
}

func (b *Broker) cancelRestartPrompts() {
	b.cancelRestartPromptsWithin(RestartCancellationNoticeTimeout)
}

func (b *Broker) cancelRestartPromptsWithin(budget time.Duration) {
	var notices []restartNotice
	b.prompts.mu.Lock()
	if b.prompts.abandoned {
		b.prompts.mu.Unlock()
		return
	}
	b.prompts.sealed.Store(true)
	b.Asks.mu.Lock()
	for id, p := range b.Asks.m {
		p.cancelled.Store(true)
		notices = append(notices, restartNotice{"ask", id, p.route, p.messageID, p.question + "\n\n" + restartAskCancelled, p.owner})
		delete(b.Asks.m, id)
	}
	b.Asks.mu.Unlock()
	b.Perms.mu.Lock()
	for id, p := range b.Perms.m {
		if !p.settled {
			p.cancelled.Store(true)
			if b.Perms.cancelled == nil {
				b.Perms.cancelled = make(map[string]RouteKey)
			}
			b.Perms.cancelled[id] = p.route
			notices = append(notices, restartNotice{"permission", id, p.route, p.messageID, permPromptText(p.toolName, p.preview) + "\n\n" + restartPermCancelled, nil})
		}
		delete(b.Perms.m, id)
	}
	b.Perms.mu.Unlock()
	b.prompts.mu.Unlock()
	ctx, cancel := context.WithTimeout(b.restartContext(), budget)
	defer cancel()
	jobs := make(chan restartNotice)
	done := make(chan restartNotice, len(notices))
	for i := 0; i < min(16, len(notices)); i++ {
		go func() {
			for n := range jobs {
				if ctx.Err() != nil {
					return
				}
				b.notifyRestart(ctx, n)
				done <- n
			}
		}()
	}
	sent, completed := 0, 0
	for completed < len(notices) {
		var out chan restartNotice
		var next restartNotice
		if sent < len(notices) {
			out = jobs
			next = notices[sent]
		}
		select {
		case out <- next:
			sent++
		case <-done:
			completed++
		case <-ctx.Done():
			close(jobs)
			for _, n := range notices {
				log.Printf("controlled restart notification possibly unfinished kind=%s id=%s route=%v", n.kind, n.id, n.route)
			}
			return
		}
	}
	close(jobs)
}

func (b *Broker) notifyRestart(ctx context.Context, n restartNotice) {
	if n.kind == "ask" && n.owner != nil {
		recipient := n.owner
		if current, ok := b.ownerRecipient(n.owner, n.route); ok {
			recipient = current
		}
		conn, ok := recipient.ConnValue().(*ipc.Conn)
		if !ok || conn == nil {
			log.Printf("controlled restart ask error undelivered id=%s route=%v: owner disconnected", n.id, n.route)
		} else if err := conn.WriteJSONContext(ctx, ipc.AskResultMsg{Op: ipc.OpAskResult, AskID: n.id, Err: restartAskCancelled}); err != nil {
			log.Printf("controlled restart ask error undelivered id=%s route=%v: %v", n.id, n.route, err)
		}
	}
	if ctx.Err() != nil {
		return
	}
	b.renderRestartNoticeContext(ctx, n)
}

func (b *Broker) renderRestartNotice(n restartNotice) {
	b.renderRestartNoticeContext(b.restartContext(), n)
}

func (b *Broker) renderRestartNoticeContext(ctx context.Context, n restartNotice) {
	if ctx.Err() != nil {
		return
	}
	if n.messageID != 0 {
		if found, err := b.editKeyboardMessage(n.route, n.messageID, n.text, [][]c3types.Button{}, c3types.MarkupNone); found && err == nil {
			return
		}
	}
	if ctx.Err() != nil {
		return
	}
	ch, err := b.Channel(n.route.Channel)
	if err == nil {
		var topic *int64
		if n.route.HasTopic {
			t := n.route.TopicID
			topic = &t
		}
		_, err = ch.SendReply(c3types.ReplyArgs{Channel: n.route.Channel, ChatID: n.route.ChatID, TopicID: topic, Text: n.text, Markup: c3types.MarkupNone, Buttons: [][]c3types.Button{}})
	}
	if err != nil {
		log.Printf("controlled restart notification failed kind=%s id=%s route=%v: %v", n.kind, n.id, n.route, err)
	}
}

func (b *Broker) refuseRestartPermission(stub *Stub, id string) {
	routes := orderedHeldRoutes(stub)
	if len(routes) == 0 {
		log.Printf("controlled restart permission refused id=%s: no route", id)
		return
	}
	route := routes[0]
	for _, candidate := range routes {
		ch, err := b.Channel(candidate.Channel)
		if err == nil && ch.Capabilities().InlineKeyboards {
			route = candidate
			break
		}
	}
	b.renderRestartNotice(restartNotice{kind: "permission", id: id, route: route, text: restartPermRefused})
}

func (b *Broker) askRegistrationError() string {
	if b.prompts.draining.Load() {
		return restartAskRefused
	}
	return "ask id collision — retry"
}
func (r *askRegistry) deletePending(p *pendingAsk) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m[p.askID] == p {
		delete(r.m, p.askID)
	}
}
func (r *permRegistry) deletePending(p *pendingPerm) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m[p.requestID] == p {
		delete(r.m, p.requestID)
	}
}
func (r *askRegistry) publish(p *pendingAsk, id int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m[p.askID] != p || p.cancelled.Load() {
		return false
	}
	p.messageID = id
	return true
}
func (r *permRegistry) publish(p *pendingPerm, id int64) (*pendingPerm, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p.cancelled.Load() {
		return nil, false
	}
	if p.settled {
		p.messageID = id
		if r.m[p.requestID] == p {
			delete(r.m, p.requestID)
		}
		return p, true
	}
	if r.m[p.requestID] == p {
		p.messageID = id
	}
	return nil, false
}
func (r *askRegistry) takeRoute(id string, route RouteKey) (*pendingAsk, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.drain != nil && r.drain.sealed.Load() {
		return nil, false
	}
	p := r.m[id]
	if p == nil || p.route != route {
		return nil, false
	}
	delete(r.m, id)
	return p, true
}
func (r *permRegistry) takeCallback(id string, route RouteKey, messageID int64) (*pendingPerm, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.drain != nil && r.drain.sealed.Load() {
		return nil, r.cancelledCallbackText(id, route)
	}
	p := r.m[id]
	if p == nil || p.settled {
		return nil, permAnswerGoneText
	}
	if p.route != route {
		return nil, permAnswerWrongRouteText
	}
	if p.messageID != 0 && messageID != 0 && p.messageID != messageID {
		return nil, permAnswerWrongMessageText
	}
	delete(r.m, id)
	return p, ""
}

func (b *Broker) handleBrokerRestart(conn *ipc.Conn, stub *Stub) {
	reply := ipc.BrokerRestartReply{Op: ipc.OpBrokerRestartReply}
	if stub.CLI != "c3-broker-cli" {
		reply.Err = "controlled restart requires c3-broker-cli"
	} else if b.RestartRequested == nil {
		reply.Err = "controlled restart is unavailable"
	} else {
		if b.prompts.begin(false) {
			defer b.prompts.end()
		}
		b.BeginControlledRestart()
		b.RestartRequested()
		reply.OK = true
	}
	if err := conn.WriteJSON(reply); err != nil {
		log.Printf("controlled restart reply failed: %v", err)
	}
}

// Caller holds the registry lock. This bounded set records only this process's
// cap cancellations; it is not persisted or used for recovery.
func (r *permRegistry) cancelledCallbackText(id string, route RouteKey) string {
	if original, ok := r.cancelled[id]; ok && original == route {
		return restartPermTap
	}
	return permAnswerGoneText
}
func (r *permRegistry) afterRestartCallback(id string, route RouteKey) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cancelledCallbackText(id, route)
}
