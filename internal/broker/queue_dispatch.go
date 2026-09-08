package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// retranscribeTimeout bounds how long the synchronous MCP handler waits on its
// VoiceScheduler result hook. Timing out or losing the caller never cancels the
// enrichment lease: its one durable resolve still lands and delivers. It remains
// above the scheduler/STT outer deadline so a healthy long note returns directly.
// It is a var only so tests can shorten the caller wait.
var retranscribeTimeout = 750 * time.Second

// workerJobTimeout bounds every blocking worker round-trip the broker performs
// on an IPC read goroutine (fetch_queue, tool_call, the attach backlog-summary
// peek). Phase 1 made an EXITED worker reply
// errWorkerStopped fast, so the common failure already unblocks <-resultCh; this
// is the defense-in-depth backstop for a worker that genuinely STALLS without
// exiting (a hung handler / stuck send) — then nothing is ever sent on resultCh
// and the broker's single serial per-connection read loop would wedge forever.
// On timeout each handler returns its own clean error/no-op for THIS op and the
// read loop keeps serving the connection. It is a var (not a const) only so a
// test can shorten it; production never reassigns it.
var workerJobTimeout = 30 * time.Second

// maxFetchIDBytes bounds the correlation id a fetch_queue request may carry. See
// handleFetchQueue for why an unbounded id is a problem at all. A real id is a
// uuid, a nanoid or a counter — tens of bytes — so a kilobyte is roughly 30x the
// largest plausible one and cannot be reached by accident. One KiB is also
// deliberately not the bare 4096 literal: archguard reserves that number for
// Telegram's message-length boundary outside the channel package, and a
// correlation-id limit has no reason to create a sanctioned collision.
const maxFetchIDBytes = 1024

// handleFetchQueue routes a pull through every held route's worker (output
// first, then claim order), or one selected held route. Each queue retains its
// single-owner worker access. Limit caps the combined batch; All drains all.
func (b *Broker) handleFetchQueue(conn *ipc.Conn, stub *Stub, raw []byte) {
	var req ipc.FetchQueueReq
	if err := json.Unmarshal(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, Err: "malformed fetch_queue: " + err.Error()})
		return
	}
	// The correlation id is echoed VERBATIM into the same frame as the messages,
	// and docs/ADAPTERS.md tells adapter authors to generate it themselves without
	// publishing any length bound — so it is caller-chosen weight inside a
	// 4 MiB frame. The worker counts it against the batch budget (fetchFrameFit),
	// which is enough to keep the response sendable; this bound handles the
	// degenerate end of the same lever, where the id alone would leave no room for
	// any message and every fetch would come back empty for a reason the caller
	// could never see. A real id is a uuid or a counter — 1 KiB is already
	// roughly 30x that.
	//
	// The refusal cannot echo an id it just called too long, so it answers with
	// none: an adapter matching on id will time out this one request (and find the
	// reason in broker.log), which is strictly better than a queue it can never
	// drain. Nothing is consumed on this path.
	if len(req.ID) > maxFetchIDBytes {
		log.Printf("fetch_queue conn=%d: refused — correlation id is %d bytes (max %d); nothing was read or consumed",
			stub.ConnID, len(req.ID), maxFetchIDBytes)
		_ = conn.WriteJSON(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult,
			Err: fmt.Sprintf("fetch_queue refused: correlation id is %d bytes, over the %d-byte limit — it is echoed into the same %d-byte frame as the messages. Nothing was consumed; retry with a short id.",
				len(req.ID), maxFetchIDBytes, ipc.MaxFrameSize)})
		return
	}
	var routes []RouteKey
	if req.Channel != "" {
		route, err := b.resolveHeldRoute(stub, req.Channel)
		if err != nil {
			_ = conn.WriteJSON(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: req.ID, Err: err.Error()})
			return
		}
		routes = []RouteKey{route}
	} else {
		routes = orderedHeldRoutes(stub)
	}
	if len(routes) == 0 {
		_ = conn.WriteJSON(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: req.ID, Err: "fetch_queue before attach: no route claimed"})
		return
	}
	resp := b.fetchSelectedRoutes(stub, req, routes)
	// Do NOT discard this error. A refused write puts nothing on the wire, so the
	// adapter sits until its own timeout with no explanation anywhere — the worst
	// shape a failure can take. The batch is sized against this exact response
	// before anything is consumed (RouteWorker.fetchFrameFit), so an oversize frame
	// can no longer reach here; what remains is transport failure (a dead peer),
	// and on the Ack path those messages HAVE been consumed. Say so plainly —
	// they are recoverable from the queue's retention window, but only by someone
	// who knows to look.
	if err := conn.WriteJSON(resp); err != nil {
		lost := ""
		if req.Ack && len(resp.Messages) > 0 {
			lost = fmt.Sprintf(" — those %d message(s) were already consumed and did NOT reach the session; recover them from the queue retention window", len(resp.Messages))
		}
		log.Printf("fetch_queue conn=%d: response not sent (%d messages, remaining=%d): %v%s",
			stub.ConnID, len(resp.Messages), resp.Remaining, err, lost)
		return
	}
	// A successful destructive pull is SILENT to the topic. The plumbing does not
	// talk to the human: when the agent has drained the held messages it responds
	// with real content, and that response is the only confirmation the human
	// needs. A broker-minted "Fetched N queued item" receipt is noise on every
	// live path and was removed (it had fired for every CLI, not just poll-only).
}

// fetchSelectedRoutes performs one ordered multi-route fetch over a caller's
// snapshot. The worker applies the authoritative ownership + confirmation gate
// immediately around every destructive queue mutation. A stale or unconfirmed
// route is skipped and counted in Remaining; valid siblings continue.
func (b *Broker) fetchSelectedRoutes(stub *Stub, req ipc.FetchQueueReq, routes []RouteKey) ipc.FetchQueueResp {
	resp := ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: req.ID}
	remainingLimit := req.Limit
	authorizedRoutes := 0
	for routeIndex, route := range routes {
		limit := remainingLimit
		if req.All {
			limit = -1
		}
		frameReserve := 0
		if len(resp.Messages) > 0 {
			if encoded, err := json.Marshal(resp); err == nil {
				frameReserve = len(encoded)
			}
		}
		res, err := b.fetchHeldRoute(stub, route, req, limit, frameReserve)
		if res.SkipReason != "" {
			pending, _ := b.Queue.Pending(queueRouteKey(route))
			resp.Remaining += pending
			log.Printf("fetch_queue conn=%d: SKIPPED route %s: %s", stub.ConnID, b.routeLabel(route), res.SkipReason)
			continue
		}
		authorizedRoutes++
		if err != nil {
			if len(resp.Messages) == 0 {
				resp.Err = err.Error()
			} else {
				// Earlier routes may already have consumed the messages now in the
				// response. Do not turn that successful delivery into a tool error the
				// adapter discards. Report a partial batch with an honest remaining
				// count so the next fetch retries this and later routes.
				for _, pendingRoute := range routes[routeIndex:] {
					pending, _ := b.Queue.Pending(queueRouteKey(pendingRoute))
					resp.Remaining += pending
				}
				log.Printf("fetch_queue conn=%d: partial multi-route result after %d message(s): route %s failed: %v",
					stub.ConnID, len(resp.Messages), routeKeyStr(route), err)
			}
			break
		}
		// Keep each consumed record's identity on its route's response copy.
		// Codex invalidates only matching pending pushes; neither the combined
		// Remaining count nor this response's frame order is a drain boundary.
		resp.Messages = append(resp.Messages, res.Messages...)
		resp.Remaining += res.Remaining
		if !req.All && remainingLimit > 0 {
			remainingLimit -= len(res.Messages)
			if remainingLimit < 0 {
				remainingLimit = 0
			}
		}
	}
	if authorizedRoutes == 0 && resp.Err == "" {
		if req.Ack {
			resp.Err = "fetch_queue(ack=true) refused: no selected route is still held and confirmed by an explicit claim"
		} else {
			resp.Err = "fetch_queue refused: no selected route is still held by this session"
		}
	}
	return resp
}

func (b *Broker) fetchHeldRoute(stub *Stub, route RouteKey, req ipc.FetchQueueReq, limit, frameReserve int) (FetchResult, error) {
	resultCh := make(chan FetchResult, 1)
	var lease *fetchLease
	if req.Ack {
		lease = newFetchLease()
	}
	job := Job{Kind: JobFetch, Fetch: &FetchJob{
		Limit: limit, All: req.All, Ack: req.Ack,
		RespID: req.ID, FrameReserve: frameReserve,
		Owner: stub, Lease: lease, ResultCh: resultCh,
	}}
	if !b.Workers.Submit(route, job) {
		return FetchResult{}, fmt.Errorf("worker queue full or stopped")
	}
	select {
	case result := <-resultCh:
		return result, result.Err
	case <-time.After(workerJobTimeout):
		if req.Ack && lease != nil && !lease.cancel() {
			result := <-resultCh
			return result, result.Err
		}
		return FetchResult{}, fmt.Errorf("fetch_queue: worker did not respond within %s", workerJobTimeout)
	}
}

// handleRetranscribe joins or creates the same scheduler lease as automatic
// enrichment. Its wire contract stays synchronous, but caller timeout/death is
// irrelevant to the durable resolve: the hook is only a view of work owned by
// VoiceScheduler, never ownership of that work.
func (b *Broker) handleRetranscribe(conn *ipc.Conn, stub *Stub, raw []byte) {
	var req ipc.RetranscribeReq
	if err := json.Unmarshal(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.RetranscribeResp{Op: ipc.OpRetranscribeResult, Err: "malformed retranscribe: " + err.Error()})
		return
	}
	if req.FileID == "" {
		_ = conn.WriteJSON(ipc.RetranscribeResp{Op: ipc.OpRetranscribeResult, ID: req.ID, Err: "retranscribe: file_id required"})
		return
	}
	route := stub.OutputRoute()
	transcriptOnly := route == nil || req.MessageID == 0
	chanName := "telegram"
	var chatID int64
	var topicID *int64
	if route != nil {
		chanName = route.Channel
		chatID = route.ChatID
		if route.HasTopic {
			t := route.TopicID
			topicID = &t
		}
	}
	if b.Plugins == nil {
		_ = conn.WriteJSON(ipc.RetranscribeResp{Op: ipc.OpRetranscribeResult, ID: req.ID, Err: "no STT plugin registered"})
		return
	}
	manualRoute := MakeRouteKey(chanName, chatID, topicID)
	if route != nil {
		manualRoute = *route
	}
	att := c3types.Attachment{Kind: "voice", FileID: req.FileID}
	in := c3types.Inbound{
		Channel: chanName, ChatID: chatID, TopicID: topicID, MessageID: req.MessageID,
		Attachments: []c3types.Attachment{att}, Timestamp: time.Now(),
	}
	hook := make(chan voiceScheduleResult, 1)
	resp := ipc.RetranscribeResp{Op: ipc.OpRetranscribeResult, ID: req.ID}
	if b.Voice == nil {
		resp.Err = "retranscribe: voice scheduler is stopping"
		_ = conn.WriteJSON(resp)
		return
	}
	if err := b.Voice.ScheduleManual(manualRoute, "", in, att, transcriptOnly, hook); err != nil {
		resp.Err = "retranscribe: " + err.Error()
		_ = conn.WriteJSON(resp)
		return
	}

	timer := time.NewTimer(retranscribeTimeout)
	defer timer.Stop()
	select {
	case result := <-hook:
		if result.Err != nil {
			resp.Err = "retranscribe: " + result.Err.Error()
			log.Printf("retranscribe chan=%s file_id=%s msg=%d ok=false: %v", chanName, req.FileID, req.MessageID, result.Err)
		} else {
			resp.Text = result.Transcript
			log.Printf("retranscribe chan=%s file_id=%s msg=%d ok=true", chanName, req.FileID, req.MessageID)
		}
	case <-timer.C:
		resp.Err = fmt.Sprintf("retranscribe: timed out after %s waiting for transcription; durable enrichment continues", retranscribeTimeout)
		log.Printf("retranscribe chan=%s file_id=%s msg=%d: caller wait expired after %s — durable enrichment continues", chanName, req.FileID, req.MessageID, retranscribeTimeout)
	}
	if err := conn.WriteJSON(resp); err != nil {
		log.Printf("retranscribe chan=%s file_id=%s msg=%d: response not sent: %v — durable enrichment is unaffected", chanName, req.FileID, req.MessageID, err)
	}
}

// handleInboundDelivered consumes the queued message(s) covered by a live push
// after the adapter acks it (OK=true), on the route that push ACTUALLY went out
// on — not whatever route the stub holds by the time the ack lands. A merged
// push covers Count stored lines and is acked once, so all of them are consumed
// (not 1, which would orphan Count-1 as phantom backlog) — by IDENTITY, from the
// record the push left behind; handleConsume never guesses at the queue head.
// OK=false leaves it queued (backlog + recovery nudge). No response is sent — it
// is a one-way ack.
//
// Count is forwarded VERBATIM (no 0→1 bump): a Count<=0 ack covered no stored
// lines (the adapter should not even ack events now, but an older one might echo
// Covered=0), and handleConsume skips Count<=0 so it never consumes a real
// backlog message the push didn't deliver (C1).
func (b *Broker) handleInboundDelivered(stub *Stub, raw []byte) {
	var msg ipc.InboundDeliveredMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		log.Printf("inbound_delivered: malformed: %v", err)
		return
	}
	if !msg.OK {
		log.Printf("inbound_delivered NACK update=%d — leaving queued (backlog)", msg.UpdateID)
		return
	}
	// Drop a zero-/negative-covered ack outright — there is nothing to consume and
	// no job worth dispatching (handleConsume would skip it anyway).
	if msg.Count < 1 {
		log.Printf("inbound_delivered update=%d count=%d — nothing to consume (event / zero-covered ack)", msg.UpdateID, msg.Count)
		return
	}
	// The ack carries no route. Its broker-minted delivery token identifies the
	// exact outstanding push; a legacy no-token ack is accepted only when exactly
	// one record matches UpdateID. The stub's CURRENT route can have moved
	// between the push and the ack: the agent attaches to another topic mid-turn
	// while the grok adapter is still inside injectWithRetry's backoff (~2 min over
	// 12 attempts — it is retrying precisely BECAUSE the agent is mid-turn). Using
	// the output route here would dispatch this DESTRUCTIVE consume to a worker that
	// never made the push: same chat it is a duplicate plus a permanently inflated
	// pending count, and across two chats — Telegram message ids are unique per
	// CHAT, not globally — it removes lines from the WRONG route's queue.
	//
	// Dispatch it to the route this session was ACTUALLY pushed on, recorded at
	// push time. No wire change is needed: the broker already knew both halves of
	// the correlation when it wrote the frame. Deliberately NOT taken from the
	// adapter either — the least-trusted party must not get to name the queue it
	// drains.
	route := stub.TakePushRoute(msg.UpdateID, msg.DeliveryToken)
	if route == nil {
		log.Printf("inbound_delivered update=%d count=%d conn=%d: no unique live push correlation for this session (unknown token / broker restart / record cap / ambiguous legacy MessageID) — consume DROPPED (the line stays queued, recoverable via fetch_queue)", msg.UpdateID, msg.Count, stub.ConnID)
		return
	}
	// ALSO (whole-branch review): surface a dropped consume like the sibling
	// handlers (handleFetchQueue / handleToolCall) do, so a full/stopped worker
	// queue that silently swallows the live-ack — leaving Count lines stranded as
	// phantom backlog — is visible in broker.log rather than lost.
	if ok := b.Workers.Submit(*route, Job{Kind: JobConsume, Consume: &ConsumeJob{
		MessageID: msg.UpdateID,
		Token:     msg.DeliveryToken,
		Count:     msg.Count,
		Owner:     stub,
	}}); !ok {
		log.Printf("inbound_delivered update=%d count=%d: worker queue full or stopped — consume DROPPED (Count lines remain as backlog)", msg.UpdateID, msg.Count)
	}
}
