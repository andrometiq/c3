package broker

import (
	"context"
	"sync"
	"time"
)

// WorkerPool manages route workers keyed by RouteKey. Workers are started
// lazily on first job submission and reaped when they exit (idle timeout
// or release/stop). Each worker holds a back-reference to the broker so it
// can look up routes, channels, and fire fallbacks during dispatch.
type WorkerPool struct {
	ctx     context.Context
	cancel  context.CancelFunc
	idle    time.Duration
	broker  *Broker
	mu      sync.Mutex
	workers map[RouteKey]*RouteWorker
	wg      sync.WaitGroup
	// stopped is set true under mu by Stop() BEFORE cancel()/wg.Wait(). Submit
	// checks it under mu and refuses to spawn once set, so spawnLocked's wg.Add(1)
	// can never begin after wg.Wait() has started (the classic WaitGroup-reuse
	// panic) and no orphan worker is spun during shutdown (m2, W1 review). The
	// mutex-guarded flag — not ctx.Err() — is the race-free fix: cancel() is not
	// under mu and would not synchronize with the Add.
	stopped bool
}

// NewWorkerPool returns a pool with the given idle timeout for new workers.
// The broker reference is used by the worker dispatch path to resolve
// claims, channels, and fallback state.
func NewWorkerPool(parent context.Context, idle time.Duration, broker *Broker) *WorkerPool {
	ctx, cancel := context.WithCancel(parent)
	return &WorkerPool{
		ctx:     ctx,
		cancel:  cancel,
		idle:    idle,
		broker:  broker,
		workers: map[RouteKey]*RouteWorker{},
	}
}

// Submit enqueues job for the given route key, starting a worker if none exists.
// Returns false if the worker queue is genuinely full.
//
// A2: a map entry can point at a worker whose run() has already EXITED (idle/ctx/
// release) but whose async reaper has not yet deleted it. Such a worker is treated
// as absent and respawned, so a job is never handed to a dead worker. To also
// cover the worker exiting in the window between releasing p.mu and w.Submit, the
// submit is retried ONCE: if w.Submit returns false we re-enter under p.mu,
// re-check Done(), and respawn if it exited. A1 guarantees a stopped worker's
// Submit returns false rather than stranding, so this terminates — the retry
// either lands on a live worker or a freshly-spawned one. (A genuinely full queue
// on a live worker returns false on both attempts, preserving the old semantics.)
// submitGraceWindow is how long SubmitWait keeps retrying a full route worker
// before giving up. It exists to absorb a MOMENTARY burst, not to wait out a
// genuinely stuck route.
//
// It is deliberately short: Emit runs on the single Telegram poll goroutine
// (poll.go dispatches updates synchronously), so every millisecond spent here
// delays EVERY route's inbound and the next getUpdates. Two seconds is enough for
// a worker to drain a job or two under normal load; past that the caller holds the
// Telegram offset instead, and the redelivery is paced by pollIdleBackoff.
const submitGraceWindow = 2 * time.Second

// submitRetryInterval is how often SubmitWait re-attempts within the window. The
// retry is a poll rather than a blocking channel send because RouteWorker.Submit
// takes w.mu, and shutdown drains the queue under that same lock — a blocking send
// while holding it would deadlock against a worker that is exiting.
const submitRetryInterval = 25 * time.Millisecond

// SubmitWait is Submit with a bounded wait: it retries a full worker for up to
// submitGraceWindow before returning false. Use it on the inbound path, where a
// false return means the caller must hold the channel's offset so the message is
// redelivered rather than lost.
func (p *WorkerPool) SubmitWait(key RouteKey, job Job) bool {
	if p.Submit(key, job) {
		return true // fast path: room now, no timers allocated
	}
	deadline := time.NewTimer(submitGraceWindow)
	defer deadline.Stop()
	tick := time.NewTicker(submitRetryInterval)
	defer tick.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-tick.C:
			if p.Submit(key, job) {
				return true
			}
		}
	}
}

func (p *WorkerPool) Submit(key RouteKey, job Job) bool {
	for attempt := 0; attempt < 2; attempt++ {
		p.mu.Lock()
		// m2 (W1 review): the pool is shutting down (Stop set this under p.mu before
		// cancel()/wg.Wait()). Refuse WITHOUT spawning so spawnLocked's wg.Add(1)
		// never runs after wg.Wait() began (WaitGroup-reuse panic) and we don't spin
		// orphan workers. Stopped-check and the wg.Add now both happen under p.mu.
		if p.stopped {
			p.mu.Unlock()
			return false
		}
		w, ok := p.workers[key]
		if ok && workerExited(w) {
			ok = false // exited but not yet reaped — treat as absent, respawn below
		}
		if !ok {
			w = p.spawnLocked(key)
		}
		p.mu.Unlock()

		if w.Submit(job) {
			return true
		}
		// w.Submit returned false: either the worker exited between unlock and
		// Submit (A1 ⇒ it returned false instead of stranding) or its queue is full.
		// Loop once more — the Done() re-check respawns for the former; for a full
		// queue the worker is still live so we don't respawn and the retry also
		// fails, returning false.
	}
	return false
}

// workerExited reports whether w's run goroutine has already returned (Done
// closed). Caller should hold p.mu when using the result to mutate the map.
func workerExited(w *RouteWorker) bool {
	select {
	case <-w.Done():
		return true
	default:
		return false
	}
}

// spawnLocked creates a fresh worker for key, installs it in the map (overwriting
// any exited entry), and starts its reaper. Caller MUST hold p.mu.
func (p *WorkerPool) spawnLocked(key RouteKey) *RouteWorker {
	w := newRouteWorker(p.ctx, key, p.idle, p.broker)
	p.workers[key] = w
	p.wg.Add(1)
	go func() {
		<-w.Done()
		p.mu.Lock()
		// Delete-if-same (A2): only remove the entry if it still points at THIS
		// worker. A respawn may have already replaced it; deleting unconditionally
		// would clobber the live respawn (and the respawn's own reaper would then
		// have nothing to clean up).
		if cur, ok := p.workers[key]; ok && cur == w {
			delete(p.workers, key)
		}
		p.mu.Unlock()
		p.wg.Done()
	}()
	return w
}

// Stop signals all workers to exit and waits for them (unbounded). Retained for
// tests and any caller that wants a guaranteed full drain.
func (p *WorkerPool) Stop() {
	p.StopWithin(0)
}

// StopWithin signals all workers to exit and waits up to timeout for them
// (timeout<=0 waits indefinitely, i.e. Stop's behavior). It sets stopped under
// p.mu BEFORE cancel()/wait so a concurrent Submit (e.g. a live poll goroutine
// still routing during Broker.Shutdown) observes stopped and refuses to spawn —
// guaranteeing no spawnLocked wg.Add(1) can begin after wg.Wait() has started
// (m2, W1 review).
//
// Returns true if every worker exited within the budget. A false return means a
// worker is stuck in a call that doesn't observe ctx cancel (e.g. an outbound
// send bounded only by its own HTTP timeout, or a readback echo); the caller is
// expected to be on a hard-exit path where the leaked goroutine dies with the
// process, and the durable queue makes an abrupt exit loss-free.
func (p *WorkerPool) StopWithin(timeout time.Duration) bool {
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()
	p.cancel()
	if timeout <= 0 {
		p.wg.Wait()
		return true
	}
	return waitGroupTimeout(&p.wg, timeout)
}

// waitGroupTimeout waits for wg up to timeout, returning true if it completed and
// false if the timeout elapsed first. On a false return the spawned waiter
// goroutine outlives the call until wg eventually completes — acceptable only on
// a process-exit path (shutdown), where a leaked goroutine dies with the process.
func waitGroupTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Active returns the number of currently-running workers (diagnostic).
func (p *WorkerPool) Active() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}
