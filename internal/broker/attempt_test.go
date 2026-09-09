package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

func requireShadow(t *testing.T, b *Broker, token, outcome, reason string) attemptRecord {
	t.Helper()
	rows := b.attempts.lookup(token, time.Now())
	if len(rows) != 1 {
		t.Fatalf("token %q: want one attempt, got %+v", token, rows)
	}
	a := rows[0]
	if a.Outcome != outcome || (reason != "" && a.Reason != reason) || a.Diverged {
		t.Fatalf("want %s/%s with no divergence, got %+v", outcome, reason, a)
	}
	return a
}

func shadowFixture(t *testing.T) (*Broker, *RouteWorker, *Stub, <-chan ipc.InboundMsg) {
	t.Helper()
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	key := MakeRouteKey("telegram", -100, nil)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	t.Cleanup(w.Stop)
	stub, frames := liveHolderFrames(t, b, key)
	stub.AddRoute(key)
	stub.MarkRouteConfirmed(key)
	stub.SetStableSessionID("test-session")
	return b, w, stub, frames
}

func shadowPushOne(t *testing.T, w *RouteWorker, frames <-chan ipc.InboundMsg, stamp time.Time) ipc.InboundMsg {
	t.Helper()
	in := inboundOn(-100, nil, 1, "hello")
	in.Timestamp = stamp
	id, err := w.broker.Queue.AppendTracked(queueRouteKey(w.key), in)
	if err != nil {
		t.Fatal(err)
	}
	w.forwardOrFallbackCovering(context.Background(), in, []*c3types.Inbound{in}, 1, []string{id}, true)
	push := waitInboundPush(t, frames)
	a := requireShadow(t, w.broker, push.DeliveryToken, "open", "")
	if a.Transport != "channel" || a.WriteOutcome != "success" || !slices.Equal(memberIDs(a.Members), push.RecordIDs) {
		t.Fatalf("push observation: %+v, frame=%+v", a, push)
	}
	return push
}

func TestShadowLifecycle(t *testing.T) {
	for _, seam := range []string{"ack", "legacy_empty_token_ack", "nack", "invalid_count", "holder_death", "drain", "eviction", "legacy_fetch", "unobserved"} {
		t.Run(seam, func(t *testing.T) {
			b, w, holder, frames := shadowFixture(t)
			stamp := time.Now()
			if seam == "eviction" {
				stamp = stamp.Add(-queue.MaxAge - time.Hour)
			}
			push := shadowPushOne(t, w, frames, stamp)
			token := push.DeliveryToken
			ack := func(token string) {
				w.handleConsume(context.Background(), &ConsumeJob{MessageID: 1, Token: token, Count: 1, Owner: holder})
			}
			switch seam {
			case "ack", "legacy_empty_token_ack":
				ackToken := token
				if seam == "legacy_empty_token_ack" {
					ackToken = ""
				}
				ack(ackToken)
				a := requireShadow(t, b, token, "confirmed", "live_ack")
				if !slices.Equal(a.Retired, push.RecordIDs) || a.Holder.SessionID != "test-session" {
					t.Fatalf("ack: %+v", a)
				}
			case "nack", "invalid_count":
				msg := ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 1, DeliveryToken: token, Count: 1}
				if seam == "invalid_count" {
					msg.OK, msg.Count = true, 0
				}
				raw, _ := json.Marshal(msg)
				b.handleInboundDelivered(holder, raw)
				requireShadow(t, b, token, "failed", seam)
				if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 1 {
					t.Fatal("rejected ack consumed row")
				}
			case "holder_death":
				w.flushPendingAck("holder exited")
				requireShadow(t, b, token, "released", "holder_death")
				if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 1 {
					t.Fatal("death lost row")
				}
			case "drain", "eviction":
				if seam == "drain" {
					result := make(chan DrainRemoveResult, 1)
					w.handleDrainRemove(&DrainRemoveJob{RecordIDs: push.RecordIDs, ResultCh: result})
					if res := <-result; res.Err != nil || len(res.Removed) != 1 {
						t.Fatalf("drain: %+v", res)
					}
				} else {
					w.evictIfOverCap(queueRouteKey(w.key))
				}
				a := requireShadow(t, b, token, "released", "members_removed")
				if len(a.Members) != 0 || a.Dropped[push.RecordIDs[0]] != seam {
					t.Fatalf("removed: %+v", a)
				}
				ack(token)
				a = requireShadow(t, b, token, "confirmed", "live_ack")
				if len(a.Retired) != 0 {
					t.Fatal("late ack retired removed row")
				}
			case "legacy_fetch":
				result := make(chan FetchResult, 1)
				w.handleFetch(context.Background(), &FetchJob{Ack: true, All: true, Owner: holder, Lease: newFetchLease(), ResultCh: result})
				if res := <-result; res.Err != nil || len(res.Messages) != 1 {
					t.Fatalf("fetch: %+v", res)
				}
				rows := b.attempts.snapshot(time.Now())
				if len(rows) != 2 {
					t.Fatalf("fetch attempts: %+v", rows)
				}
				a := requireShadow(t, b, rows[1].Token, "confirmed", "legacy_consume")
				if a.Transport != "fetch" || !slices.Equal(a.Retired, push.RecordIDs) || a.Deadline.Sub(a.Started) != 60*time.Second {
					t.Fatalf("fetch: %+v", a)
				}
				requireShadow(t, b, token, "released", "members_removed")
				ack(token)
				requireShadow(t, b, token, "confirmed", "live_ack")
			case "unobserved":
				a := requireShadow(t, b, token, "open", "")
				if a.Deadline.Sub(a.Started) != 15*time.Second {
					t.Fatal("channel budget")
				}
				b.attempts.expire(a.Deadline)
				requireShadow(t, b, token, "expired", "unobserved")
				ack(token) // the shadow's hypothetical timeout cannot change today's ack
				requireShadow(t, b, token, "confirmed", "live_ack")
			}
		})
	}
}

func TestShadowMergedAndUntrackedPush(t *testing.T) {
	for _, seam := range []string{"merged", "event", "degraded", "no_identities"} {
		t.Run(seam, func(t *testing.T) {
			b, w, holder, frames := shadowFixture(t)
			in := inboundOn(-100, nil, 1, "one")
			switch seam {
			case "merged":
				w.flushInbounds(context.Background(), []*c3types.Inbound{in, inboundOn(-100, nil, 2, "two")})
			case "event":
				in.Kind = c3types.InboundPollResult
				in.Event = &c3types.InboundEvent{PollResult: &c3types.PollResult{PollID: "p", IsClosed: true}}
				w.forwardOrFallback(context.Background(), in, 0)
			case "degraded":
				b.Queue = nil
				w.flushInbounds(context.Background(), []*c3types.Inbound{in})
			case "no_identities":
				w.forwardOrFallbackCovering(context.Background(), in, []*c3types.Inbound{in}, 0, nil, true)
			}
			push := waitInboundPush(t, frames)
			rows := b.attempts.snapshot(time.Now())
			if seam != "merged" {
				if len(rows) != 0 || push.DeliveryToken != "" {
					t.Fatalf("untracked push: %+v %+v", rows, push)
				}
				return
			}
			if len(rows) != 1 || len(rows[0].Members) != 2 || push.Covered != 2 {
				t.Fatalf("merged: %+v %+v", rows, push)
			}
			w.handleConsume(context.Background(), &ConsumeJob{MessageID: push.Inbound.MessageID, Token: push.DeliveryToken, Count: 2, Owner: holder})
			a := requireShadow(t, b, push.DeliveryToken, "confirmed", "live_ack")
			if len(a.Retired) != 2 {
				t.Fatalf("merged ack: %+v", a)
			}
		})
	}
}

func TestShadowReconnectDisconnectAdoptionAck(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, fastDebounceTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	key := MakeRouteKey("telegram", -100, nil)
	peer := reconnectHello(t, b, os.Getpid(), "/project")
	old := b.Stubs.Snapshot()[0]
	b.Routes.Claim(key, old)
	old.AddRoute(key)
	old.MarkRouteConfirmed(key)
	if !b.Workers.Submit(key, Job{Kind: JobInbound, Inbound: inboundOn(-100, nil, 1, "live")}) {
		t.Fatal("submit")
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var push ipc.InboundMsg
	if err := json.Unmarshal(raw, &push); err != nil || push.DeliveryToken == "" {
		t.Fatalf("push: %s %v", raw, err)
	}
	requireShadow(t, b, push.DeliveryToken, "open", "")
	peer.Close()
	waitForVoiceCondition(t, "disconnect observation", func() bool {
		rows := b.attempts.lookup(push.DeliveryToken, time.Now())
		return len(rows) == 1 && rows[0].Outcome == "released"
	})
	requireShadow(t, b, push.DeliveryToken, "released", "disconnect")
	peer = reconnectHello(t, b, os.Getpid(), "/project")
	a := requireShadow(t, b, push.DeliveryToken, "open", "adopted")
	if !a.Adopted || a.Holder.Stub == old || a.Holder.ConnID == old.ConnID {
		t.Fatalf("adopted: %+v", a)
	}
	if err := peer.WriteJSON(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 1, Count: 1, OK: true, DeliveryToken: push.DeliveryToken}); err != nil {
		t.Fatal(err)
	}
	waitForVoiceCondition(t, "confirmed observation", func() bool {
		rows := b.attempts.lookup(push.DeliveryToken, time.Now())
		return len(rows) == 1 && rows[0].Outcome == "confirmed"
	})
	a = requireShadow(t, b, push.DeliveryToken, "confirmed", "live_ack")
	if !slices.Equal(a.Retired, push.RecordIDs) {
		t.Fatalf("adopted ack: %+v", a)
	}
}

func TestShadowVoiceResolutionPushAck(t *testing.T) {
	g := newGateChannel(100, nil)
	b := gateBroker(t, g)
	t.Cleanup(b.Shutdown)
	setFastVoiceDebounce(b)
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) { return "resolved voice", nil })
	route := MakeRouteKey("telegram", -100, nil)
	holder, frames := liveHolderFrames(t, b, route)
	holder.AddRoute(route)
	holder.MarkRouteConfirmed(route)
	in := inboundOn(-100, nil, 1, "")
	in.Attachments = []c3types.Attachment{{Kind: "voice", FileID: "voice-shadow"}}
	if !b.Workers.Submit(route, Job{Kind: JobInbound, Inbound: in}) {
		t.Fatal("submit")
	}
	push := waitInboundPush(t, frames)
	rows := b.attempts.snapshot(time.Now())
	if len(rows) != 1 || !slices.Equal(memberIDs(rows[0].Members), push.RecordIDs) || !strings.Contains(push.Inbound.Text, "resolved voice") {
		t.Fatalf("voice push: %+v, attempts=%+v", push, rows)
	}
	raw, _ := json.Marshal(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 1, Count: 1, OK: true, DeliveryToken: push.DeliveryToken})
	b.handleInboundDelivered(holder, raw)
	waitForVoiceCondition(t, "voice confirmed", func() bool {
		return b.attempts.lookup(push.DeliveryToken, time.Now())[0].Outcome == "confirmed"
	})
	a := requireShadow(t, b, push.DeliveryToken, "confirmed", "live_ack")
	if !slices.Equal(a.Retired, push.RecordIDs) {
		t.Fatalf("voice retired: %+v", a)
	}
}

func TestShadowPushWriteFailure(t *testing.T) {
	b, w, holder, _ := shadowFixture(t)
	holder.Conn.(*ipc.Conn).Close()
	in := inboundOn(-100, nil, 1, "write failure")
	id, err := b.Queue.AppendTracked(queueRouteKey(w.key), in)
	if err != nil {
		t.Fatal(err)
	}
	w.forwardOrFallbackCovering(context.Background(), in, []*c3types.Inbound{in}, 1, []string{id}, true)
	rows := b.attempts.snapshot(time.Now())
	if len(rows) != 1 {
		t.Fatalf("attempts: %+v", rows)
	}
	a := requireShadow(t, b, rows[0].Token, "failed", "write_failed")
	if a.WriteOutcome != "failure" {
		t.Fatalf("write: %+v", a)
	}
	if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 1 {
		t.Fatal("failed push consumed")
	}
}

func TestShadowPartialDrainMergedAck(t *testing.T) {
	b, w, holder, frames := shadowFixture(t)
	w.flushInbounds(context.Background(), []*c3types.Inbound{inboundOn(-100, nil, 1, "one"), inboundOn(-100, nil, 2, "two")})
	push := waitInboundPush(t, frames)
	if len(push.RecordIDs) != 2 {
		t.Fatalf("batch: %+v", push)
	}
	result := make(chan DrainRemoveResult, 1)
	w.handleDrainRemove(&DrainRemoveJob{RecordIDs: push.RecordIDs[:1], ResultCh: result})
	if res := <-result; res.Err != nil || len(res.Removed) != 1 {
		t.Fatalf("drain: %+v", res)
	}
	a := requireShadow(t, b, push.DeliveryToken, "open", "")
	if !slices.Equal(memberIDs(a.Members), push.RecordIDs[1:]) {
		t.Fatalf("partial members: %+v", a)
	}
	w.handleConsume(context.Background(), &ConsumeJob{MessageID: push.Inbound.MessageID, Token: push.DeliveryToken, Count: 2, Owner: holder})
	a = requireShadow(t, b, push.DeliveryToken, "confirmed", "live_ack")
	if !slices.Equal(a.Retired, push.RecordIDs[1:]) {
		t.Fatalf("partial retired: %+v", a)
	}
}

func TestShadowFetchPeekCancelledAndRejected(t *testing.T) {
	for _, mode := range []string{"peek", "cancelled", "rejected"} {
		t.Run(mode, func(t *testing.T) {
			b, w, holder, _ := shadowFixture(t)
			if _, err := b.Queue.AppendTracked(queueRouteKey(w.key), inboundOn(-100, nil, 1, "held")); err != nil {
				t.Fatal(err)
			}
			lease := newFetchLease()
			if mode == "cancelled" {
				lease.cancel()
			}
			if mode == "rejected" {
				holder = &Stub{ConnID: 99}
			}
			result := make(chan FetchResult, 1)
			w.handleFetch(context.Background(), &FetchJob{Ack: mode != "peek", All: true, Owner: holder, Lease: lease, ResultCh: result})
			<-result
			if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 1 {
				t.Fatal("non-consuming fetch consumed")
			}
			if got := b.attempts.snapshot(time.Now()); len(got) != 0 {
				t.Fatalf("non-consuming fetch attempt: %+v", got)
			}
		})
	}
}

func TestShadowAckRejectedHolderAndWorker(t *testing.T) {
	for _, reason := range []string{"unauthorized", "worker_unavailable", "unknown_correlation", "wrong_holder"} {
		t.Run(reason, func(t *testing.T) {
			b, w, holder, frames := shadowFixture(t)
			push := shadowPushOne(t, w, frames, time.Now())
			switch reason {
			case "unauthorized":
				b.Routes.Release(w.key, holder.ConnID)
				w.handleConsume(context.Background(), &ConsumeJob{MessageID: 1, Token: push.DeliveryToken, Count: 1, Owner: holder})
			case "worker_unavailable":
				b.Workers.Stop()
			case "unknown_correlation":
				holder.TakePushRoute(1, push.DeliveryToken)
			case "wrong_holder":
				holder = &Stub{ConnID: 99}
			}
			if reason != "unauthorized" {
				raw, _ := json.Marshal(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 1, Count: 1, OK: true, DeliveryToken: push.DeliveryToken})
				b.handleInboundDelivered(holder, raw)
			}
			if reason == "wrong_holder" {
				requireShadow(t, b, push.DeliveryToken, "open", "")
			} else {
				requireShadow(t, b, push.DeliveryToken, "failed", reason)
			}
			if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 1 {
				t.Fatal("rejected ack consumed")
			}
		})
	}
}

func TestAttemptTableTransitions(t *testing.T) {
	now := time.Now()
	route := MakeRouteKey("telegram", 1, nil)
	for _, transport := range []string{"channel", "fetch", "inbox", "unknown"} {
		t.Run(transport, func(t *testing.T) {
			var table attemptTable
			table.open(attemptRecord{Token: "a", Route: route, Transport: transport, Members: attemptMembers([]string{"one", "two"})}, now)
			table.open(attemptRecord{Token: "a", Route: route, Members: attemptMembers([]string{"other"})}, now)
			a := table.lookup("a", now)[0]
			if len(a.Members) != 2 {
				t.Fatal("duplicate open replaced attempt")
			}
			a.Members[0].ID = "mutated snapshot"
			if table.lookup("a", now)[0].Members[0].ID != "one" {
				t.Fatal("snapshot aliases table")
			}
			table.drop(route, []string{"one"}, "drain", "", now)
			table.confirm("a", route, []string{"two"}, "live_ack", now)
			a = table.lookup("a", now)[0]
			if a.Diverged || a.Outcome != "confirmed" || a.Dropped["one"] != "drain" {
				t.Fatalf("partial: %+v", a)
			}
			if (transport == "unknown" || transport == "inbox") && !a.Deadline.IsZero() {
				t.Fatal("guessed deadline")
			}
		})
	}
	var table attemptTable
	table.open(attemptRecord{Token: "failed", Route: route, Transport: "channel", Members: attemptMembers([]string{"one"})}, now)
	table.write("failed", route, errors.New("socket failed"), now)
	a := table.lookup("failed", now)[0]
	if a.Outcome != "failed" || a.Reason != "write_failed" || a.WriteOutcome != "failure" {
		t.Fatalf("write: %+v", a)
	}
}

func TestAttemptShadowDivergenceOncePerToken(t *testing.T) {
	var lines []string
	table := attemptTable{logf: func(line string) { lines = append(lines, line) }}
	now := time.Now()
	for _, chat := range []int64{1, 2} {
		route := MakeRouteKey("telegram", chat, nil)
		table.open(attemptRecord{Token: "forced", Route: route, Transport: "channel", Members: attemptMembers([]string{"expected"})}, now)
		for range 2 {
			table.confirm("forced", route, []string{"different"}, "test_injected", now)
		}
	}
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "attempt shadow DIVERGED token=forced route=") || !strings.Contains(lines[0], "expected=[expected] removed=[different] reason=test_injected") {
		t.Fatalf("divergence diagnostic: %v", lines)
	}
}

func TestAttemptBoundedCapEviction(t *testing.T) {
	for _, global := range []bool{false, true} {
		t.Run(fmt.Sprint("global=", global), func(t *testing.T) {
			var table attemptTable
			now := time.Now()
			cap := attemptRouteCap
			if global {
				cap = attemptGlobalCap
			}
			routeFor := func(i int) RouteKey {
				chat := int64(1)
				if global {
					chat = int64(i/attemptRouteCap + 1)
				}
				return MakeRouteKey("telegram", chat, nil)
			}
			open := func(i int) {
				table.open(attemptRecord{Token: fmt.Sprint(i), Route: routeFor(i), Transport: "channel", Members: attemptMembers([]string{"row"})}, now)
			}
			for i := range cap {
				open(i)
			}
			open(cap)
			if len(table.snapshot(now)) != cap || len(table.lookup(fmt.Sprint(cap), now)) != 0 {
				t.Fatal("evicted active attempt")
			}
			// Keep the oldest entry active: the oldest terminal one must go first.
			table.confirm("1", routeFor(1), []string{"row"}, "test", now)
			table.confirm("2", routeFor(2), []string{"row"}, "test", now)
			open(cap)
			if len(table.lookup("0", now)) != 1 || len(table.lookup("1", now)) != 0 || len(table.lookup("2", now)) != 1 {
				t.Fatal("wrong cap victim")
			}
			if len(table.snapshot(now)) != cap {
				t.Fatal("cap exceeded")
			}
		})
	}
}

func TestAttemptConcurrentSnapshots(t *testing.T) {
	var table attemptTable
	now := time.Now()
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token := fmt.Sprint(i)
			route := MakeRouteKey("telegram", int64(i), nil)
			table.open(attemptRecord{Token: token, Route: route, Transport: "channel", Members: attemptMembers([]string{"row"})}, now)
			table.write(token, route, nil, now)
			table.snapshot(now)
			table.confirm(token, route, []string{"row"}, "test", now)
		}()
	}
	wg.Wait()
}
