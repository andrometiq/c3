package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// retranscribeTimeout bounds the synchronous STT chain run on the IPC read
// goroutine in handleRetranscribe, so a slow/hung provider cannot wedge that
// adapter's single-threaded read loop indefinitely. It sits just ABOVE the STT
// builtin's own subprocess budget (defaultTimeoutSeconds = 300s) so a healthy
// long voice note that legitimately runs near 300s still completes (the inner,
// ctx-derived 300s deadline fires first and returns a real "still failing"
// error), while a provider that hangs BEFORE its own deadline applies (e.g. a
// stuck network download) is still cut off in bounded time. It is a var (not a
// const) only so a test can shorten it; production never reassigns it.
var retranscribeTimeout = 330 * time.Second

// workerJobTimeout bounds every blocking worker round-trip the broker performs
// on an IPC read goroutine (fetch_queue, tool_call, the retranscribe in-place
// refresh, the attach backlog-summary peek). Phase 1 made an EXITED worker reply
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
// largest plausible one and cannot be reached by accident.
const maxFetchIDBytes = 1024

// handleFetchQueue routes a fetch_queue pull through the claimed route's worker
// (single-owner file access). Limit default + max are clamped by the adapter;
// the broker honors All (drain everything) and Ack (consume vs peek).
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
	// could never see. A real id is a uuid or a counter — 4 KiB is ~100x that.
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
	route := stub.CurrentRoute()
	if route == nil {
		_ = conn.WriteJSON(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: req.ID, Err: "fetch_queue before attach: no route claimed"})
		return
	}
	// Spec §5 tripwire: refuse the DESTRUCTIVE (Ack=true) consume unless the current
	// claim was set by a legitimate claim site (MarkRouteConfirmed). Every real
	// attach/own-recover confirms the route, so this never trips a legitimate flow —
	// it is fail-closed insurance so a future silent-bind regression cannot drain a
	// queue. The non-destructive peek (Ack=false) is unaffected: it consumes nothing.
	if req.Ack && !stub.RouteConfirmed() {
		_ = conn.WriteJSON(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: req.ID, Err: "fetch_queue(ack=true) refused: route not confirmed by an explicit claim"})
		return
	}
	resultCh := make(chan FetchResult, 1)
	var lease *fetchLease
	if req.Ack {
		lease = newFetchLease()
	}
	job := Job{Kind: JobFetch, Fetch: &FetchJob{
		Limit: req.Limit, All: req.All, Ack: req.Ack,
		// The response echoes req.ID verbatim into the SAME frame as the messages,
		// so the worker's frame budget has to know it before it consumes anything.
		RespID: req.ID,
		Lease:  lease, ResultCh: resultCh,
	}}
	if !b.Workers.Submit(*route, job) {
		_ = conn.WriteJSON(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: req.ID, Err: "worker queue full or stopped"})
		return
	}
	var res FetchResult
	// A stalled worker must not wedge this connection's serial read loop. For an
	// Ack=true timeout, the lease makes cancellation atomic with starting the
	// destructive Consume. If cancellation wins, a late worker downgrades to
	// Peek. If Consume already started, cancellation waits for that short local
	// queue operation and we deliver its real result rather than orphaning it.
	select {
	case res = <-resultCh:
	case <-time.After(workerJobTimeout):
		if req.Ack && lease != nil && !lease.cancel() {
			res = <-resultCh
			break
		}
		_ = conn.WriteJSON(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: req.ID, Err: "fetch_queue: worker did not respond within " + workerJobTimeout.String()})
		return
	}
	resp := ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: req.ID, Remaining: res.Remaining}
	if res.Err != nil {
		resp.Err = res.Err.Error()
	} else {
		resp.Messages = res.Messages
	}
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
		log.Printf("fetch_queue chan=%s chat=%d: response not sent (%d messages, remaining=%d): %v%s",
			route.Channel, route.ChatID, len(resp.Messages), resp.Remaining, err, lost)
	}
}

// handleRetranscribe re-runs the STT provider chain on a cached voice attachment
// by file_id and returns the fresh transcript. Downloads via the channel are
// handled inside the STT plugin chain (it owns DownloadAttachment).
//
// In-place refresh (spec Component 5): when message_id is given AND that message
// is still queued for the claimed route, the fresh transcript replaces the stored
// Text in place (a cap-safe rewrite routed through the route's single-owner
// worker), so a later fetch_queue returns the corrected transcript rather than the
// old STT-failure placeholder. A message_id that isn't queued (never seen /
// already consumed) is a clean no-op — the transcript is still returned.
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
	route := stub.CurrentRoute()
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
	// Ask the bot server first, same rule as the live path (voicegate.go,
	// 2026-07-27 incident Ask #1). Without it an unfetchable file re-runs the
	// whole chain and comes back as "STT provider still failing (no
	// transcript)", which sends the agent hunting for a provider outage that
	// isn't there: the server simply will not serve the file.
	if refusal := b.attachmentFetchRefusal(chanName, req.FileID); refusal != "" {
		log.Printf("retranscribe chan=%s file_id=%s msg=%d ok=false: the bot server will not serve this file — %s", chanName, req.FileID, req.MessageID, refusal)
		_ = conn.WriteJSON(ipc.RetranscribeResp{Op: ipc.OpRetranscribeResult, ID: req.ID, Err: "retranscribe: " + refusal})
		return
	}
	// Bound the STT chain (network download + provider call) so a slow/hung
	// provider cannot block this adapter's single-threaded IPC read loop forever.
	// FireOnVoiceReceived honors ctx (the STT builtin wires its subprocess to a
	// ctx-derived deadline), so a timeout here unblocks the read loop even if a
	// provider ignores its own budget. The cap sits just above the STT
	// subprocess default (300s) so a healthy long voice note still completes,
	// while a truly stuck call still returns an error in bounded time.
	ctx, cancel := context.WithTimeout(context.Background(), retranscribeTimeout)
	defer cancel()
	transcript := b.Plugins.FireOnVoiceReceived(ctx, c3types.VoicePayload{
		Channel:   chanName,
		ChatID:    chatID,
		TopicID:   topicID,
		MessageID: req.MessageID,
		FileID:    req.FileID,
	})
	resp := ipc.RetranscribeResp{Op: ipc.OpRetranscribeResult, ID: req.ID}
	if transcript == "" {
		resp.Err = "retranscribe: STT provider still failing (no transcript)"
		log.Printf("retranscribe chan=%s file_id=%s msg=%d ok=false", chanName, req.FileID, req.MessageID)
		_ = conn.WriteJSON(resp)
		return
	}
	resp.Text = transcript

	// In-place refresh of the still-queued line for this message_id (spec
	// Component 5). Routed through the route's single-owner worker so the cap-safe
	// rewrite never touches the route's files off the worker goroutine. Best-effort:
	// a refresh miss/failure does not change the returned transcript (the agent
	// already has it), so we log on error but still return Text.
	refreshed := false
	if req.MessageID != 0 && route != nil && b.Workers != nil && b.Queue != nil {
		resultCh := make(chan RefreshResult, 1)
		job := Job{Kind: JobRefreshText, Refresh: &RefreshTextJob{MessageID: req.MessageID, NewText: transcript, ResultCh: resultCh}}
		if b.Workers.Submit(*route, job) {
			select {
			case res := <-resultCh:
				if res.Err != nil {
					log.Printf("retranscribe refresh chan=%s file_id=%s msg=%d: refresh error: %v", chanName, req.FileID, req.MessageID, res.Err)
				}
				refreshed = res.Refreshed
			case <-time.After(workerJobTimeout):
				// A3: the STT result was already delivered above; the in-place refresh
				// is best-effort. A stalled refresh worker must not wedge the read loop —
				// log (mirroring the refresh-failure log) and fall through to return Text.
				log.Printf("retranscribe refresh chan=%s file_id=%s msg=%d: worker did not respond within %s — skipping in-place refresh", chanName, req.FileID, req.MessageID, workerJobTimeout)
			}
		} else {
			log.Printf("retranscribe refresh chan=%s file_id=%s msg=%d: worker queue full or stopped", chanName, req.FileID, req.MessageID)
		}
	}
	log.Printf("retranscribe chan=%s file_id=%s msg=%d ok=true refreshed=%v", chanName, req.FileID, req.MessageID, refreshed)
	_ = conn.WriteJSON(resp)
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
	// Spec §5 tripwire (SAME guard as the fetch Ack=true path): this live-push ack is
	// the OTHER destructive consume path, so gate it on a confirmed claim too —
	// guarding only fetch would leave the ack-consume drainable off an unconfirmed
	// route. Fail-closed insurance; a legitimate holder always has a confirmed route.
	if !stub.RouteConfirmed() {
		log.Printf("inbound_delivered update=%d count=%d — route not confirmed by an explicit claim; consume DROPPED (§5 tripwire, Count lines remain as backlog)", msg.UpdateID, msg.Count)
		return
	}
	// The ack carries NO route, and the stub's CURRENT route can have moved
	// between the push and the ack: the agent attaches to another topic mid-turn
	// while the grok adapter is still inside injectWithRetry's backoff (~2 min over
	// 12 attempts — it is retrying precisely BECAUSE the agent is mid-turn). Using
	// CurrentRoute() here dispatched this DESTRUCTIVE consume to a worker that
	// never made the push: same chat it is a duplicate plus a permanently inflated
	// pending count, and across two chats — Telegram message ids are unique per
	// CHAT, not globally — it removes lines from the WRONG route's queue.
	//
	// Dispatch it to the route this session was ACTUALLY pushed on, recorded at
	// push time. No wire change is needed: the broker already knew both halves of
	// the correlation when it wrote the frame. Deliberately NOT taken from the
	// adapter either — the least-trusted party must not get to name the queue it
	// drains.
	route := stub.TakePushRoute(msg.UpdateID)
	if route == nil {
		log.Printf("inbound_delivered update=%d count=%d conn=%d: no live push recorded for this session (broker restart / record cap / ack for a push we never made) — consume DROPPED (the line stays queued, recoverable via fetch_queue)", msg.UpdateID, msg.Count, stub.ConnID)
		return
	}
	// ALSO (whole-branch review): surface a dropped consume like the sibling
	// handlers (handleFetchQueue / handleToolCall) do, so a full/stopped worker
	// queue that silently swallows the live-ack — leaving Count lines stranded as
	// phantom backlog — is visible in broker.log rather than lost.
	if ok := b.Workers.Submit(*route, Job{Kind: JobConsume, Consume: &ConsumeJob{MessageID: msg.UpdateID, Count: msg.Count}}); !ok {
		log.Printf("inbound_delivered update=%d count=%d: worker queue full or stopped — consume DROPPED (Count lines remain as backlog)", msg.UpdateID, msg.Count)
	}
}
