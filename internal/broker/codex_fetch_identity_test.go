package broker

import (
	"context"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

func TestFetchReturnsOnlyConsumedRecordIdentities(t *testing.T) {
	for _, mode := range []string{"selected", "partial", "all", "peek", "stolen", "unconfirmed"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("C3_QUEUE_DIR", t.TempDir())
			b := newTestBroker(t, multiMappings())
			defer b.Shutdown()
			registerTestChannel(b, &fakeChannel{})
			registerTestChannel(b, &webFakeChannel{fakeChannel: &fakeChannel{}})
			owner := b.Stubs.Register("codex", 11, "/project", nil)
			tg, web := MakeRouteKey("telegram", -100, ptrI64Val(281)), MakeRouteKey("web", 42, nil)
			claimConfirmed(t, b, owner, tg)
			claimConfirmed(t, b, owner, web)
			owner.SetOutputRoute(tg)
			appendQueued(t, b, tg, 1, "first")
			appendQueued(t, b, tg, 2, "second")
			appendQueued(t, b, web, 1, "sibling with same message id")
			want := map[string]bool{}
			for _, key := range []RouteKey{tg, web} {
				rows, err := b.Queue.PeekTracked(queueRouteKey(key), -1)
				if err != nil {
					t.Fatal(err)
				}
				for _, r := range rows {
					want[r.RecordID] = true
				}
			}
			req := ipc.FetchQueueReq{ID: "fetch", Ack: true, All: true}
			switch mode {
			case "selected":
				req.Channel = "telegram"
			case "partial":
				req.All = false
				req.Limit = 1
			case "peek":
				req.Ack = false
			case "stolen":
				b.Routes.ForceReleaseKey(tg)
				thief := b.Stubs.Register("codex", 22, "/other", nil)
				claimConfirmed(t, b, thief, tg)
			case "unconfirmed":
				owner.ClearRouteIf(tg)
				owner.AddRoute(tg)
			}
			// Preserve the snapshot in the stolen case: the worker must enforce ownership.
			var resp ipc.FetchQueueResp
			if mode == "stolen" {
				resp = b.fetchSelectedRoutes(owner, req, []RouteKey{tg, web})
			} else {
				resp = fetchForTest(t, b, owner, req)
			}
			if resp.Err != "" {
				t.Fatal(resp.Err)
			}
			consumed := map[string]bool{}
			for _, m := range resp.Messages {
				if !req.Ack {
					if m.ConsumedRecordID != "" {
						t.Fatal("peek advertised consumption")
					}
					continue
				}
				if !want[m.ConsumedRecordID] || consumed[m.ConsumedRecordID] {
					t.Fatalf("missing/duplicate/unknown identity: %+v", m)
				}
				consumed[m.ConsumedRecordID] = true
			}
			for _, key := range []RouteKey{tg, web} {
				rows, err := b.Queue.PeekTracked(queueRouteKey(key), -1)
				if err != nil {
					t.Fatal(err)
				}
				for _, r := range rows {
					if consumed[r.RecordID] {
						t.Fatal("advertised identity still queued")
					}
					delete(want, r.RecordID)
				}
			}
			if len(want) != len(consumed) {
				t.Fatalf("consumed %d records but reported %d", len(want), len(consumed))
			}
			expected := map[string]int{"selected": 2, "partial": 1, "all": 3, "peek": 0, "stolen": 1, "unconfirmed": 1}[mode]
			if len(consumed) != expected {
				t.Fatalf("consumed=%v, want %d for %s", consumed, expected, mode)
			}
		})
	}
}

func TestDebounceAndMergedPushUseDurableFetchIdentities(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	pushes := capturingHolder(t, b, key)
	owner, _ := b.Routes.Holder(key)
	owner.AddRoute(key)
	owner.MarkRouteConfirmed(key)
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()
	// Persist one old row. New sources are still held in the debounce buffer and
	// therefore outside this fetch boundary; flushing them later must preserve it.
	old, err := b.Queue.AppendTracked(queueRouteKey(key), inbound(tid, 1, "old"))
	if err != nil {
		t.Fatal(err)
	}
	held := []*c3types.Inbound{inbound(tid, 2, "debounced-first"), inbound(tid, 3, "debounced-second")}
	ch := make(chan FetchResult, 1)
	w.handleFetch(context.Background(), &FetchJob{All: true, Ack: true, Owner: owner, ResultCh: ch})
	fetched := <-ch
	if len(fetched.Messages) != 1 || fetched.Messages[0].ConsumedRecordID != old {
		t.Fatalf("wrong pre-flush boundary: %+v", fetched)
	}
	w.flushInbounds(context.Background(), held)
	var push ipc.InboundMsg
	select {
	case push = <-pushes:
	case <-time.After(time.Second):
		t.Fatal("merged push missing")
	}
	if len(push.RecordIDs) != 2 || push.DeliveryToken == "" || push.Covered != 2 {
		t.Fatalf("push lacks durable identity: %+v", push)
	}
	for _, id := range push.RecordIDs {
		if id == old {
			t.Fatal("debounced row inherited old drain identity")
		}
	}
	// Partially drain the merged push and acknowledge its original token. Only
	// its surviving source is removed; unrelated queued data remains intact.
	w.handleFetch(context.Background(), &FetchJob{Limit: 1, Ack: true, Owner: owner, ResultCh: ch})
	partial := <-ch
	if len(partial.Messages) != 1 || partial.Messages[0].ConsumedRecordID != push.RecordIDs[0] {
		t.Fatalf("partial boundary disagrees with push: %+v", partial)
	}
	unrelated, err := b.Queue.AppendTracked(queueRouteKey(key), inbound(tid, 4, "unrelated"))
	if err != nil {
		t.Fatal(err)
	}
	w.handleConsume(context.Background(), &ConsumeJob{MessageID: push.Inbound.MessageID, Token: push.DeliveryToken, Count: push.Covered, Owner: owner})
	rows, err := b.Queue.PeekTracked(queueRouteKey(key), -1)
	if err != nil || len(rows) != 1 || rows[0].RecordID != unrelated {
		t.Fatalf("token ack consumed wrong survivors: %+v, %v", rows, err)
	}
}
