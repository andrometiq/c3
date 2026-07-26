package broker

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

func readObserveResp(t *testing.T, c *ipc.Conn) ipc.ObserveResp {
	t.Helper()
	raw, err := c.ReadFrame()
	if err != nil {
		t.Fatalf("read observe resp: %v", err)
	}
	var r ipc.ObserveResp
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

// c3's queue route key for topic "c3" in mfWithTelegram (topic 281, chat -100).
func c3QueueKey() (RouteKey, queue.RouteKey) {
	tid := int64(281)
	return MakeRouteKey("telegram", -100, &tid), queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
}

func TestResolveTopicRoute(t *testing.T) {
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	t.Run("dm", func(t *testing.T) {
		r := b.resolveTopicRoute("telegram", "", "dm", nil, "")
		if r.status != observeOK || r.key.HasTopic || r.key.ChatID != 42 || r.name != "dm" {
			t.Fatalf("dm resolve = %+v", r)
		}
	})
	t.Run("name in default group", func(t *testing.T) {
		r := b.resolveTopicRoute("telegram", "c3", "", nil, "")
		if r.status != observeOK || !r.key.HasTopic || r.key.TopicID != 281 || r.name != "c3" {
			t.Fatalf("c3 resolve = %+v", r)
		}
	})
	t.Run("name across groups", func(t *testing.T) {
		// feature-x lives in the non-default "work" group. The resolved group MUST
		// come back so the panel can take it over BY ID (a plain name-attach can't
		// claim a non-default-group topic).
		r := b.resolveTopicRoute("telegram", "feature-x", "", nil, "")
		if r.status != observeOK || r.key.TopicID != 412 || r.name != "feature-x" || r.group != "work" {
			t.Fatalf("feature-x resolve = %+v (want group=work)", r)
		}
	})
	t.Run("topic_id", func(t *testing.T) {
		tid := int64(281)
		r := b.resolveTopicRoute("telegram", "", "", &tid, "")
		if r.status != observeOK || r.key.TopicID != 281 || r.name != "c3" {
			t.Fatalf("topic_id resolve = %+v", r)
		}
	})
	t.Run("not found", func(t *testing.T) {
		r := b.resolveTopicRoute("telegram", "nope", "", nil, "")
		if r.status != observeNotFound {
			t.Fatalf("nope resolve status = %q, want not_found", r.status)
		}
	})
	t.Run("no channel", func(t *testing.T) {
		r := b.resolveTopicRoute("nosuch", "c3", "", nil, "")
		if r.status != observeNoChannel {
			t.Fatalf("no-channel resolve status = %q, want no_channel", r.status)
		}
	})
}

// TestHandleObserve_PeekNoClaim is the core invariant: a stub can observe a
// topic held by ANOTHER live session — it gets the messages + the holder, the
// route table is UNCHANGED (the other stub still holds it, the observer never
// appears in ROUTES), and the queue is NOT consumed.
func TestHandleObserve_PeekNoClaim(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	key, qrk := c3QueueKey()
	for i := int64(1); i <= 3; i++ {
		_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: qrk.TopicID, MessageID: i, Text: "m", Timestamp: time.Now()})
	}
	// The Claude Code session owns the route.
	owner := claimedHolder(t, b, key) // CLI claude, pid self, cwd /proj

	// A DIFFERENT session observes (different cwd → not the same logical session).
	observer := &Stub{CLI: "desktop", PID: os.Getpid(), CWD: "/other", ConnID: 9, Conn: "live"}
	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.ObserveReq{Op: ipc.OpObserve, ID: "1", Name: "c3"})
	go b.handleObserve(brokerSide, observer, raw)
	resp := readObserveResp(t, agentSide)

	if !resp.OK || resp.Status != observeOK {
		t.Fatalf("observe not ok: %+v", resp)
	}
	if len(resp.Messages) != 3 {
		t.Fatalf("observe returned %d messages, want 3 (peek)", len(resp.Messages))
	}
	if resp.Holder == nil || resp.Holder.CLI != "claude" {
		t.Fatalf("observe holder = %+v, want claude", resp.Holder)
	}
	if resp.HeldByYou {
		t.Fatal("HeldByYou must be false — the observer is a different session")
	}
	// Resolved identity must come back so the panel can take over BY ID.
	if resp.TopicID == nil || *resp.TopicID != 281 || resp.Group != "main" {
		t.Fatalf("observe identity = topic %v group %q, want 281/main", resp.TopicID, resp.Group)
	}
	// Route table unchanged: owner still holds it, observer never claimed.
	if h, ok := b.Routes.Holder(key); !ok || h != owner {
		t.Fatalf("route holder changed by observe: ok=%v h=%+v", ok, h)
	}
	// Queue NOT consumed by the peek.
	if n, _ := b.Queue.Pending(qrk); n != 3 {
		t.Fatalf("observe consumed the queue; pending=%d, want 3", n)
	}
}

// TestHandleObserve_Unclaimed: observing a topic nobody holds returns the
// messages with a nil Holder (unclaimed → panel offers a plain take-over).
func TestHandleObserve_Unclaimed(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	_, qrk := c3QueueKey()
	_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: qrk.TopicID, MessageID: 1, Text: "hi", Timestamp: time.Now()})

	observer := &Stub{CLI: "desktop", PID: os.Getpid(), CWD: "/other", ConnID: 9, Conn: "live"}
	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.ObserveReq{Op: ipc.OpObserve, ID: "1", Name: "c3"})
	go b.handleObserve(brokerSide, observer, raw)
	resp := readObserveResp(t, agentSide)
	if !resp.OK || resp.Holder != nil || resp.HeldByYou {
		t.Fatalf("unclaimed observe = %+v, want ok + nil holder + not-held-by-you", resp)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("observe returned %d, want 1", len(resp.Messages))
	}
}

// TestHandleObserve_HeldByYou: when the observing stub IS the holder, HeldByYou
// is true (so the panel enables the owner-only Hand/Auto affordances).
func TestHandleObserve_HeldByYou(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	key, _ := c3QueueKey()
	owner := claimedHolder(t, b, key)

	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.ObserveReq{Op: ipc.OpObserve, ID: "1", Name: "c3"})
	go b.handleObserve(brokerSide, owner, raw) // observe with the OWNER stub
	resp := readObserveResp(t, agentSide)
	if !resp.OK || resp.Holder == nil || !resp.HeldByYou {
		t.Fatalf("self-observe = %+v, want ok + holder + HeldByYou", resp)
	}
}

// TestHandleObserve_NotFound: an unknown name returns a well-formed not_found
// response (OK=false) so the panel can offer take-over-creates.
func TestHandleObserve_NotFound(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	observer := &Stub{CLI: "desktop", PID: os.Getpid(), CWD: "/other", ConnID: 9, Conn: "live"}
	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.ObserveReq{Op: ipc.OpObserve, ID: "1", Name: "ghost"})
	go b.handleObserve(brokerSide, observer, raw)
	resp := readObserveResp(t, agentSide)
	if resp.OK || resp.Status != observeNotFound || resp.Name != "ghost" {
		t.Fatalf("not-found observe = %+v", resp)
	}
}

// TestHandleObserve_NeverConsumes: two back-to-back observes return the same
// messages (a peek advances nothing).
func TestHandleObserve_NeverConsumes(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	_, qrk := c3QueueKey()
	for i := int64(1); i <= 2; i++ {
		_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: qrk.TopicID, MessageID: i, Text: "m", Timestamp: time.Now()})
	}
	observer := &Stub{CLI: "desktop", PID: os.Getpid(), CWD: "/other", ConnID: 9, Conn: "live"}
	for pass := 0; pass < 2; pass++ {
		agentSide, brokerSide := newConnPair(t)
		raw, _ := json.Marshal(ipc.ObserveReq{Op: ipc.OpObserve, ID: "1", Name: "c3", All: true})
		go b.handleObserve(brokerSide, observer, raw)
		resp := readObserveResp(t, agentSide)
		if len(resp.Messages) != 2 {
			t.Fatalf("pass %d: observe returned %d, want 2 (peek never consumes)", pass, len(resp.Messages))
		}
	}
	if n, _ := b.Queue.Pending(qrk); n != 2 {
		t.Fatalf("observe consumed; pending=%d, want 2", n)
	}
}
