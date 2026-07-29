package broker

// observe.go implements OpObserve: a READ-ONLY, no-claim peek of a topic's
// durable queue, resolved by name/target/topic_id. It is the broker half of the
// Desktop panel's "Watch" mode — it lets a surface DISPLAY any topic's inbox
// without owning the single exclusive claim, so a Claude Code session in
// Telegram mode can keep the claim (and keep replying) while the panel shows the
// same messages live. It never consumes and never mutates a route; the only
// state it reads is the mappings registry (to resolve the name) and ROUTES (to
// report the current holder). Peeking is keyed on the RouteKey via the worker
// pool (workers.go Submit), exactly like backlogSummary / peekAndRenderQueue —
// so it works for a route this stub does NOT own.

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// observe resolution statuses (mirror ObserveResp.Status).
const (
	observeOK             = "ok"
	observeNotFound       = "not_found"
	observeAmbiguous      = "ambiguous"
	observeDMUnconfigured = "dm_unconfigured"
	observeNoChannel      = "no_channel"
)

// observeResolution is the read-only result of resolving an observe target to a
// route key. Exactly one of {status==observeOK with a valid key} or {a non-ok
// status} holds. name/group are the registry-authoritative display identity.
type observeResolution struct {
	key    RouteKey
	name   string
	group  string
	status string
}

// resolveTopicRoute resolves an observe target (target="dm" / topic_id / name)
// to a RouteKey WITHOUT claiming it, WITHOUT any channel round-trip
// (ValidateTopic), and WITHOUT mutating the registry. It is the read-only slice
// of attach.go's resolution logic. A topic_id with no local queue simply peeks
// empty — honest, and cheaper than validating against Telegram for a display
// peek.
func (b *Broker) resolveTopicRoute(chanName, name, target string, topicID *int64, group string) observeResolution {
	cc, ok := b.Mappings().Channels[chanName]
	if !ok {
		return observeResolution{status: observeNoChannel}
	}

	// target="dm" → the channel's DM route (universal, never cwd-mapped).
	if strings.EqualFold(target, "dm") {
		if cc.DMChatID == 0 {
			return observeResolution{status: observeDMUnconfigured}
		}
		return observeResolution{
			key:    MakeRouteKey(chanName, cc.DMChatID, nil),
			name:   "dm",
			status: observeOK,
		}
	}

	// topic_id (+optional group) → the id-addressed route. No ValidateTopic:
	// we only peek a local queue; an id with no queue peeks empty.
	if topicID != nil {
		gName, gCfg, gok := b.resolveGroup(cc, group)
		if !gok {
			return observeResolution{status: observeNoChannel}
		}
		tid := *topicID
		name := ""
		if tp, tok := b.Mappings().LookupTopicByID(chanName, gCfg.ChatID, tid); tok {
			name = tp.Name
		}
		if name == "" {
			name = "topic-" + formatInt64(tid)
		}
		return observeResolution{
			key:    MakeRouteKey(chanName, gCfg.ChatID, &tid),
			name:   name,
			group:  gName,
			status: observeOK,
		}
	}

	// name → default group first, then across groups.
	if name != "" {
		if tp, tok := b.Mappings().LookupTopicInDefaultGroup(chanName, name); tok {
			tid := tp.TopicID
			return observeResolution{
				key:    MakeRouteKey(chanName, tp.ChatID, &tid),
				name:   tp.Name,
				group:  tp.Group,
				status: observeOK,
			}
		}
		hits := b.Mappings().LookupTopicAcrossGroups(chanName, name)
		switch {
		case len(hits) == 1:
			tp := hits[0]
			tid := tp.TopicID
			return observeResolution{
				key:    MakeRouteKey(chanName, tp.ChatID, &tid),
				name:   tp.Name,
				group:  tp.Group,
				status: observeOK,
			}
		case len(hits) > 1:
			return observeResolution{status: observeAmbiguous, name: name}
		}
		return observeResolution{status: observeNotFound, name: name}
	}

	return observeResolution{status: observeNotFound}
}

// handleObserve services OpObserve: resolve the target read-only, report the
// current holder, and peek the queue by key (no claim, no consume). Parallels
// handleFetchQueue's non-destructive branch, but the route comes from resolving
// the request's own target instead of stub.CurrentRoute — that is the whole
// point: a surface can observe a topic it does not own.
//
// This handler NEVER calls Routes.Claim / ForceReleaseKey / Release / SetRoute /
// MarkRouteConfirmed and NEVER mutates mappings. It is pure read + peek.
func (b *Broker) handleObserve(conn *ipc.Conn, stub *Stub, raw []byte) {
	var req ipc.ObserveReq
	if err := json.Unmarshal(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.ObserveResp{Op: ipc.OpObserveResult, OK: false, Err: "malformed observe: " + err.Error()})
		return
	}

	chanName := req.Channel
	if chanName == "" {
		chanName = b.defaultChannel()
	}
	res := b.resolveTopicRoute(chanName, req.Name, req.Target, req.TopicID, req.Group)
	if res.status != observeOK {
		// A non-ok resolution still returns a well-formed response so the panel
		// can render the state (not_found → offer take-over-creates, etc.).
		_ = conn.WriteJSON(ipc.ObserveResp{
			Op: ipc.OpObserveResult, ID: req.ID, OK: false,
			Status: res.status, Channel: chanName, Name: res.nameOr(req.Name),
		})
		return
	}

	// Current holder (diagnostic only; never mutated). A dead holder is reported
	// as unclaimed — its claim is about to be reaped and must not read as "held".
	resp := ipc.ObserveResp{
		Op: ipc.OpObserveResult, ID: req.ID, OK: true, Status: observeOK,
		Channel: res.key.Channel, ChatID: res.key.ChatID, Name: res.name, Group: res.group,
	}
	if res.key.HasTopic {
		t := res.key.TopicID
		resp.TopicID = &t
	}
	if holder, held := b.Routes.Holder(res.key); held && holder.IsAlive() {
		resp.Holder = &ipc.Holder{CLI: holder.CLI, PID: holder.PID, CWD: holder.CWD}
		resp.HeldByYou = sameLogicalSession(holder, stub)
	}

	// Peek by KEY (no claim). Ack is hard-false: an observe never consumes.
	// Mirror handleFetchQueue's non-destructive branch — a discarded peek
	// consumes nothing, so timing out a genuinely stalled worker is safe.
	if b.Queue == nil || b.Workers == nil {
		resp.Err = "durable queue disabled for this run"
		_ = conn.WriteJSON(resp)
		return
	}
	resultCh := make(chan FetchResult, 1)
	// An unset/zero Limit on an observe means "show everything waiting" — a
	// display peek wants the full inbox, not the empty batch a literal Limit=0
	// would produce (the adapter, unlike fetch_queue, does not pre-default it).
	all := req.All || req.Limit <= 0
	observeEnvelope, err := json.Marshal(resp)
	if err != nil {
		resp.Err = "observe: cannot size response envelope: " + err.Error()
		_ = conn.WriteJSON(resp)
		return
	}
	job := Job{Kind: JobFetch, Fetch: &FetchJob{
		Limit: req.Limit, All: all, Ack: false, ResultCh: resultCh,
		// fetchFrameFit measures a FetchQueueResp. Reserve this response's whole
		// encoded base so its extra resolved-identity/holder fields cannot turn a
		// proven-fit peek into an oversized ObserveResp. Deliberate over-reserve:
		// the fetch envelope remains counted too, keeping future additive fields safe.
		FrameReserve: len(observeEnvelope),
	}}
	if !b.Workers.Submit(res.key, job) {
		resp.Err = "worker queue full or stopped"
		_ = conn.WriteJSON(resp)
		return
	}
	select {
	case fr := <-resultCh:
		if fr.Err != nil {
			resp.Err = fr.Err.Error()
		} else {
			resp.Messages = fr.Messages
			resp.Remaining = fr.Remaining
		}
	case <-time.After(workerJobTimeout):
		resp.Err = "observe: worker did not respond within " + workerJobTimeout.String()
	}
	_ = conn.WriteJSON(resp)
}

// nameOr returns the resolution's display name, falling back to the requested
// name (used for the not_found/ambiguous responses so the panel can echo what
// the user typed).
func (r observeResolution) nameOr(reqName string) string {
	if r.name != "" {
		return r.name
	}
	return reqName
}
