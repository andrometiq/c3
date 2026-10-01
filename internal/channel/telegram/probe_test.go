package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
)

var getMeOK = json.RawMessage(`{"id":1,"is_bot":true,"first_name":"bot","username":"bot"}`)

func newProbeTestChannel(h *fakeHost, bc gotgbot.BotClient) *Channel {
	c := &Channel{
		host:     h,
		health:   newFetchHealth(),
		reach:    newReachability(),
		probeBot: &gotgbot.Bot{Token: "test", BotClient: bc},
		hbKick:   make(chan struct{}, 1),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

// Plan test (g): a DOWN edge right after an UP tick kicks the heartbeat, so the
// next probe runs 60 s later rather than at the 5-min tick; probes keep a 60 s
// cadence while DOWN and return to 5 min after UP.
func TestHeartbeat_DownCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var mu sync.Mutex
		var calls []time.Duration
		c := newProbeTestChannel(&fakeHost{}, &funcBotClient{fn: func(int) (json.RawMessage, error) {
			mu.Lock()
			calls = append(calls, time.Since(start))
			mu.Unlock()
			return getMeOK, nil
		}})
		done := make(chan struct{})
		go func() { c.heartbeat(); close(done) }()

		time.Sleep(heartbeatInterval + time.Second) // the UP tick at 5m has run
		// Send-kind DOWN: probe successes cannot clear it, so the cadence stays 60 s.
		c.feedOutboundFailure(rbTGErr(500), "x")
		c.feedOutboundFailure(rbTGErr(500), "x")
		time.Sleep(3*time.Minute + 29*time.Second)  // 8m30s
		c.recordOutboundSuccess()                   // UP
		time.Sleep(11*time.Minute + 30*time.Second) // 20m
		c.cancel()
		<-done

		want := []time.Duration{
			5 * time.Minute,
			6*time.Minute + time.Second, 7*time.Minute + time.Second, 8*time.Minute + time.Second,
			9*time.Minute + time.Second, // already scheduled when UP landed
			14*time.Minute + time.Second, 19*time.Minute + time.Second,
		}
		mu.Lock()
		defer mu.Unlock()
		if len(calls) != len(want) {
			t.Fatalf("probe times = %v, want %v", calls, want)
		}
		for i := range want {
			if calls[i] != want[i] {
				t.Fatalf("probe times = %v, want %v", calls, want)
			}
		}
	})
}

// Plan test (g), in-flight case: a DOWN edge while a getMe is in flight is
// handled when it returns, so the first probe after DOWN starts within 70 s.
func TestHeartbeat_DownDuringInflightProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var c *Channel
		var mu sync.Mutex
		var calls []time.Duration
		c = newProbeTestChannel(&fakeHost{}, &funcBotClient{fn: func(call int) (json.RawMessage, error) {
			mu.Lock()
			calls = append(calls, time.Since(start))
			mu.Unlock()
			if call == 1 {
				c.feedOutboundFailure(rbTGErr(500), "x")
				c.feedOutboundFailure(rbTGErr(500), "x") // DOWN at 5m, mid-probe
				time.Sleep(10 * time.Second)
			}
			return getMeOK, nil
		}})
		done := make(chan struct{})
		go func() { c.heartbeat(); close(done) }()
		time.Sleep(heartbeatInterval + 2*time.Minute)
		c.cancel()
		<-done

		mu.Lock()
		defer mu.Unlock()
		if len(calls) < 2 {
			t.Fatalf("probe times = %v, want a probe after the DOWN edge", calls)
		}
		if gap := calls[1] - heartbeatInterval; gap > 70*time.Second {
			t.Fatalf("first probe after DOWN started %v after it, want <= 70s (times %v)", gap, calls)
		}
	})
}

// Plan test (f): every probe opens a fresh connection, even while polls on the
// main transport complete concurrently and keep their pooled connection.
func TestProbeTransport_AlwaysFreshConnection(t *testing.T) {
	var mu sync.Mutex
	probeAddrs := map[string]int{}
	pollAddrs := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if strings.HasSuffix(r.URL.Path, "/getMe") {
			probeAddrs[r.RemoteAddr]++
			mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true,"result":` + string(getMeOK) + `}`))
			return
		}
		pollAddrs[r.RemoteAddr]++
		mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true,"result":[]}`))
	}))
	defer srv.Close()

	c := &Channel{cfg: Config{BotToken: "1:test"}, endpoints: []string{srv.URL}}
	c.transport = newBotTransport(func(string) string { return "" }, newSetupDialer(t.Logf))
	defer c.transport.CloseIdleConnections()
	bot, err := c.newBot(c.transport)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := c.newBot(newProbeTransport(c.transport))
	if err != nil {
		t.Fatal(err)
	}

	const rounds = 5
	for range rounds {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := bot.GetUpdates(&gotgbot.GetUpdatesOpts{RequestOpts: c.requestOptsFor("getUpdates")}); err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := probe.GetMe(&gotgbot.GetMeOpts{RequestOpts: c.requestOptsFor("getMe")}); err != nil {
				t.Error(err)
			}
		}()
		wg.Wait()
	}

	mu.Lock()
	defer mu.Unlock()
	if len(probeAddrs) != rounds {
		t.Fatalf("%d probes used %d connections, want one fresh connection each: %v", rounds, len(probeAddrs), probeAddrs)
	}
	for addr := range probeAddrs {
		if pollAddrs[addr] > 0 {
			t.Fatalf("probe reused the poll connection %s", addr)
		}
	}
	if len(pollAddrs) >= rounds {
		t.Fatalf("main transport opened %d connections for %d polls; the fixture should show it pooling", len(pollAddrs), rounds)
	}
}

// Plan test (j): repeated held ambiguous readbacks feed one send-kind event
// each ⇒ DOWN after 2, and nothing is reposted.
func TestReadbackAmbiguousHoldFeedsOutbound(t *testing.T) {
	c, h := newWiredChannel()
	sends := 0
	for range 2 {
		if _, err := c.retryReadbackSend(func() (int64, error) {
			sends++
			return 0, context.DeadlineExceeded
		}); err == nil {
			t.Fatal("ambiguous timeout should surface as an error")
		}
	}
	if sends != 2 {
		t.Fatalf("sends = %d, want 2 (no reposts)", sends)
	}
	evs := h.healthEvents()
	if len(evs) != 1 || evs[0].State != c3types.HealthStateDown {
		t.Fatalf("events = %+v, want one DOWN after two held readbacks", evs)
	}
	c.recordProbeSuccess()
	if !c.reach.isDown() {
		t.Fatal("a probe success cleared send evidence from held readbacks")
	}
}
