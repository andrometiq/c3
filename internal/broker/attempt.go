package broker

import (
	"fmt"
	"log"
	"slices"
	"sync"
	"time"
)

// Legacy entries remain observation only. Negotiated entries supply retirement
// identities; their terminal transitions run on the route worker. At most
// 128 entries per route and 4096 globally
// are retained. Admission evicts the oldest terminal entries first; if a cap
// consists entirely of active entries, the new observation is discarded. This
// never rejects a delivery. Expiry is lazy, using monotonic time.Time values.
const (
	attemptRouteCap  = 128
	attemptGlobalCap = 4 * 1024
)

type attemptHolder struct {
	Stub      *Stub
	ConnID    uint64
	SessionID string
	// Captured when a negotiated attempt reserves its exact claim.
	ClaimGeneration uint64
}

type attemptMember struct {
	ID       string
	Revision string // digest for negotiated rows; empty for legacy shadow
}

type attemptRecord struct {
	FetchGroup        *attemptFetchGroup
	Negotiated        bool
	Evidence          bool
	RemovalTries      int
	Token             string
	Route             RouteKey
	Transport         string // channel | inbox | fetch | unknown
	Holder            attemptHolder
	Members           []attemptMember
	Started, Deadline time.Time
	Outcome, Reason   string // open | confirmed | failed | expired | released | unknown
	WriteOutcome      string
	Retired           []string
	Dropped           map[string]string // record id -> observed removal cause
	Adopted, Diverged bool
}

type attemptKey struct {
	token string
	route RouteKey
}

// attemptTable stores negotiated delivery authority and legacy shadow records.
// It owns no timers, workers, or durable state.
// All transitions take their clock and inputs from their caller. No method
// consults routing, coverage, recovery, or queue state to choose an action.
type attemptTable struct {
	mu      sync.Mutex
	entries map[attemptKey]*attemptRecord
	order   []attemptKey
	logf    func(string) // isolated diagnostic sink for unit tests
}

// Set once by TestMain before any broker runs; observes every default table.
var attemptShadowDivergence func()

func shadowHolder(s *Stub) attemptHolder {
	if s == nil {
		return attemptHolder{}
	}
	s.stubMu.Lock()
	defer s.stubMu.Unlock()
	return attemptHolder{Stub: s, ConnID: s.ConnID, SessionID: s.stableSessionID}
}

func attemptMembers(ids []string) []attemptMember {
	var out []attemptMember
	for _, id := range ids {
		if id != "" {
			out = append(out, attemptMember{ID: id})
		}
	}
	return out
}

func memberIDs(members []attemptMember) []string {
	ids := make([]string, 0, len(members))
	for _, m := range members {
		ids = append(ids, m.ID)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

func (t *attemptTable) expireLocked(now time.Time) {
	for _, a := range t.entries {
		if !a.Negotiated && a.Outcome == "open" && !a.Deadline.IsZero() && !now.Before(a.Deadline) {
			a.Outcome, a.Reason = "expired", "unobserved"
		}
	}
}

func (t *attemptTable) open(a attemptRecord, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(now)
	k := attemptKey{a.Token, a.Route}
	if a.Token == "" || len(a.Members) == 0 || t.entries[k] != nil {
		return
	}
	count := 0
	for key := range t.entries {
		if key.route == a.Route {
			count++
		}
	}
	for count >= attemptRouteCap || len(t.entries) >= attemptGlobalCap {
		victim := -1
		for i, key := range t.order {
			if t.entries[key].Outcome != "open" && (count < attemptRouteCap || key.route == a.Route) {
				victim = i
				break
			}
		}
		if victim < 0 {
			return
		}
		key := t.order[victim]
		if key.route == a.Route {
			count--
		}
		delete(t.entries, key)
		t.order = slices.Delete(t.order, victim, victim+1)
	}
	if t.entries == nil {
		t.entries = make(map[attemptKey]*attemptRecord)
	}
	a.Started, a.Outcome = now, "open"
	switch a.Transport {
	case "channel":
		a.Deadline = now.Add(15 * time.Second)
	case "fetch":
		a.Deadline = now.Add(60 * time.Second)
	case "inbox":
		if a.Negotiated {
			a.Deadline = now.Add(15 * time.Second)
		}
	case "unknown": // no observable deadline for these in phase 1
	default:
		a.Transport = "unknown"
	}
	a.Members = slices.Clone(a.Members)
	t.entries[k] = &a
	t.order = append(t.order, k)
}

func (t *attemptTable) write(token string, route RouteKey, err error, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(now)
	if a := t.entries[attemptKey{token, route}]; a != nil {
		a.WriteOutcome = "success"
		if err != nil {
			a.WriteOutcome = "failure"
			if a.Outcome == "open" {
				a.Outcome, a.Reason = "failed", "write_failed"
			}
		}
	}
}

func (t *attemptTable) fail(token string, holder *Stub, reason string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(now)
	for _, a := range t.entries {
		if token != "" && a.Token == token && a.Holder.Stub == holder && a.Outcome == "open" {
			a.Outcome, a.Reason = "failed", reason
		}
	}
}

// Confirm records the actual authorised retirement even after a hypothetical
// deadline or disconnect. The shadow must not enforce future receipt rules.
func (t *attemptTable) confirm(token string, route RouteKey, retired []string, reason string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(now)
	a := t.entries[attemptKey{token, route}]
	if a == nil {
		return
	}
	expected := memberIDs(a.Members)
	removed := slices.Clone(retired)
	slices.Sort(removed)
	removed = slices.Compact(removed)
	if !slices.Equal(expected, removed) && !a.Diverged {
		// A receipt group can have several route records; rate-limit by token.
		already := false
		for _, other := range t.entries {
			if other.Token == token && other.Diverged {
				already = true
			}
		}
		a.Diverged = true
		if !already {
			line := fmt.Sprintf("attempt shadow DIVERGED token=%s route=%s expected=%v removed=%v reason=%s", token, routeKeyStr(route), expected, removed, reason)
			if t.logf != nil {
				t.logf(line)
			} else {
				log.Print(line)
				if attemptShadowDivergence != nil {
					attemptShadowDivergence()
				}
			}
		}
	}
	a.Outcome, a.Reason, a.Retired = "confirmed", reason, removed
}

func (t *attemptTable) release(holder *Stub, reason string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(now)
	for _, a := range t.entries {
		if !a.Negotiated && a.Holder.Stub == holder && (a.Outcome == "open" || (a.Outcome == "expired" && a.Reason == "unobserved")) {
			a.Outcome, a.Reason = "released", reason
		}
	}
}

// Today's reconnect adopts outstanding push routes even after a socket drop.
// Reopen a disconnect observation while within budget, retaining the same token.
func (t *attemptTable) adopt(old *Stub, next attemptHolder, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(now)
	for _, a := range t.entries {
		if a.Negotiated || a.Holder.Stub != old || (!a.Deadline.IsZero() && !now.Before(a.Deadline)) {
			continue
		}
		if a.Transport != "channel" && a.Transport != "inbox" {
			continue
		}
		if a.Outcome == "open" || (a.Outcome == "released" && a.Reason == "disconnect") {
			a.Holder, a.Adopted, a.Outcome, a.Reason = next, true, "open", "adopted"
		}
	}
}

// Reconcile every surviving membership, including observations already expired
// or failed: today's late ack can still retire these rows. Confirmed records
// remain immutable history. This never removes a row from the real queue.
func (t *attemptTable) drop(route RouteKey, ids []string, cause, exceptToken string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(now)
	for _, a := range t.entries {
		if a.Negotiated || a.Route != route || a.Token == exceptToken || a.Outcome == "confirmed" {
			continue
		}
		kept := a.Members[:0]
		for _, m := range a.Members {
			if slices.Contains(ids, m.ID) {
				if a.Dropped == nil {
					a.Dropped = make(map[string]string)
				}
				a.Dropped[m.ID] = cause
			} else {
				kept = append(kept, m)
			}
		}
		a.Members = kept
		if len(kept) == 0 && a.Outcome == "open" {
			a.Outcome, a.Reason = "released", "members_removed"
		}
	}
}

func cloneAttempt(a *attemptRecord) attemptRecord {
	c := *a
	c.Members, c.Retired = slices.Clone(a.Members), slices.Clone(a.Retired)
	c.Dropped = make(map[string]string, len(a.Dropped))
	for id, reason := range a.Dropped {
		c.Dropped[id] = reason
	}
	return c
}

func (t *attemptTable) lookup(token string, now time.Time) []attemptRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(now)
	var out []attemptRecord
	for _, k := range t.order {
		if k.token == token {
			out = append(out, cloneAttempt(t.entries[k]))
		}
	}
	return out
}

func (t *attemptTable) snapshot(now time.Time) []attemptRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(now)
	out := make([]attemptRecord, 0, len(t.entries))
	for _, k := range t.order {
		out = append(out, cloneAttempt(t.entries[k]))
	}
	return out
}
