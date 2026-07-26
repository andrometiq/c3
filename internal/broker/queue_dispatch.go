package broker

import (
	"context"
	"encoding/json"
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

// handleFetchQueue routes a fetch_queue pull through the claimed route's worker
// (single-owner file access). Limit default + max are clamped by the adapter;
// the broker honors All (drain everything) and Ack (consume vs peek).
func (b *Broker) handleFetchQueue(conn *ipc.Conn, stub *Stub, raw []byte) {
	var req ipc.FetchQueueReq
	if err := json.Unmarshal(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, Err: "malformed fetch_queue: " + err.Error()})
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
		Lease: lease, ResultCh: resultCh,
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
	_ = conn.WriteJSON(resp)
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

// handleInboundDelivered consumes the oldest queued message(s) for the stub's
// route after the Claude adapter acks a successful live push (OK=true). A merged
// push covers Count stored lines and is acked once, so Count lines are consumed
// off the head (not 1, which would orphan Count-1 as phantom backlog). OK=false
// leaves it queued (backlog + recovery nudge). No response is sent — it is a
// one-way ack.
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
	route := stub.CurrentRoute()
	if route == nil {
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
	// ALSO (whole-branch review): surface a dropped consume like the sibling
	// handlers (handleFetchQueue / handleToolCall) do, so a full/stopped worker
	// queue that silently swallows the live-ack — leaving Count lines stranded as
	// phantom backlog — is visible in broker.log rather than lost.
	if ok := b.Workers.Submit(*route, Job{Kind: JobConsume, Consume: &ConsumeJob{MessageID: msg.UpdateID, Count: msg.Count}}); !ok {
		log.Printf("inbound_delivered update=%d count=%d: worker queue full or stopped — consume DROPPED (Count lines remain as backlog)", msg.UpdateID, msg.Count)
	}
}
