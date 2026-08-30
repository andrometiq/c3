package broker

import (
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

type multiPresenceChannel struct {
	*fakeChannel
	name string
	mu   sync.Mutex
	seen []presenceCall
}

func (c *multiPresenceChannel) Name() string { return c.name }

func (c *multiPresenceChannel) Capabilities() c3types.Capabilities {
	if c.fakeChannel.caps != nil {
		return *c.fakeChannel.caps
	}
	return c3types.Capabilities{Channel: c.name, EditMessages: true, Reactions: true}
}

func (c *multiPresenceChannel) SetRouteHolder(chatID int64, topicID *int64, holder *channel.RouteHolder) {
	call := presenceCall{chatID: chatID}
	if topicID != nil {
		copy := *topicID
		call.topicID = &copy
	}
	if holder != nil {
		copy := *holder
		call.holder = &copy
	}
	c.mu.Lock()
	c.seen = append(c.seen, call)
	c.mu.Unlock()
}

func (c *multiPresenceChannel) resetPresence() {
	c.mu.Lock()
	c.seen = nil
	c.mu.Unlock()
}

func (c *multiPresenceChannel) presenceSnapshot() []presenceCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]presenceCall(nil), c.seen...)
}

type routeCaptureChannel struct {
	*fakeChannel
	name    string
	reactMu sync.Mutex
	reacts  []c3types.ReactArgs
}

func (c *routeCaptureChannel) Name() string { return c.name }

func (c *routeCaptureChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{
		Channel: c.name, RichText: true, EditMessages: true, Reactions: true,
	}
}

func (c *routeCaptureChannel) React(args c3types.ReactArgs) error {
	c.reactMu.Lock()
	c.reacts = append(c.reacts, args)
	c.reactMu.Unlock()
	return nil
}

func (c *routeCaptureChannel) reactSnapshot() []c3types.ReactArgs {
	c.reactMu.Lock()
	defer c.reactMu.Unlock()
	return append([]c3types.ReactArgs(nil), c.reacts...)
}

func multiMappings() *mappings.MappingsFile {
	mf := mfWithTelegram()
	telegram := mf.Channels["telegram"]
	telegram.MasterUserID = 42
	mf.Channels["telegram"] = telegram
	mf.Channels["web"] = mappings.ChannelConfig{Listen: "127.0.0.1:8371"}
	return mf
}

func registerTestChannel(b *Broker, ch channel.Channel) {
	b.chMu.Lock()
	b.channels[ch.Name()] = &channelRegistration{Channel: ch}
	b.chMu.Unlock()
}

func claimConfirmed(t *testing.T, b *Broker, stub *Stub, key RouteKey) {
	t.Helper()
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatalf("claim %v failed", key)
	}
	stub.AddRoute(key)
	stub.MarkRouteConfirmed(key)
}

func TestAddModeKeepsPriorRoutes(t *testing.T) {
	b, _, _ := brokerWithWeb(t, nil)
	defer b.Shutdown()
	peer, closeConn := peerPair(t, b)
	defer closeConn()
	helloAck(t, peer, "/proj")

	first := readAttach(t, peer, ipc.AttachReq{Op: ipc.OpAttach, Name: "c3", Replay: true})
	if !first.OK {
		t.Fatalf("initial attach: %+v", first)
	}
	added := readAttach(t, peer, ipc.AttachReq{Op: ipc.OpAttach, Channel: "web", Add: true, Replay: true})
	if !added.OK || len(added.Routes) != 2 || added.Output == nil || added.Output.Channel != "web" {
		t.Fatalf("add response=%+v, want telegram+web with web output", added)
	}
	if added.Routes[0].Channel != "telegram" || added.Routes[1].Channel != "web" {
		t.Fatalf("add claim order=%+v, want telegram then web", added.Routes)
	}
	stubs := b.Stubs.Snapshot()
	if len(stubs) != 1 || len(stubs[0].Routes()) != 2 {
		t.Fatalf("held set after add=%v", stubs)
	}

	telegramPresence := &multiPresenceChannel{fakeChannel: &fakeChannel{}, name: "telegram"}
	webPresence := &multiPresenceChannel{fakeChannel: &fakeChannel{}, name: "web"}
	registerTestChannel(b, telegramPresence)
	registerTestChannel(b, webPresence)
	for _, key := range stubs[0].Routes() {
		b.notifyRouteHolder(routePresenceChange{key: key, stub: stubs[0], since: time.Now()})
	}
	if got := telegramPresence.presenceSnapshot(); len(got) != 1 || got[0].holder == nil || got[0].holder.Role != "input" {
		t.Fatalf("telegram presence=%+v, want input", got)
	}
	if got := webPresence.presenceSnapshot(); len(got) != 1 || got[0].holder == nil || got[0].holder.Role != "output" {
		t.Fatalf("web presence=%+v, want output", got)
	}
}

func TestSwitchModeReleasesEveryOtherRoute(t *testing.T) {
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	stub := b.Stubs.Register("claude", 11, "/proj", struct{}{})
	a := MakeRouteKey("telegram", -100, ptrI64Val(281))
	bKey := MakeRouteKey("web", 42, nil)
	c := MakeRouteKey("telegram", -200, ptrI64Val(412))
	claimConfirmed(t, b, stub, a)
	claimConfirmed(t, b, stub, bKey)
	stub.SetOutputRoute(bKey)
	if !b.tryClaim(nil, stub, c, "feature-x", false, true, false) {
		t.Fatal("switch claim failed")
	}
	if got := stub.Routes(); len(got) != 1 || got[0] != c {
		t.Fatalf("switch routes=%v, want only %v", got, c)
	}
	for _, released := range []RouteKey{a, bKey} {
		if _, held := b.Routes.Holder(released); held {
			t.Fatalf("switch left route table claim %v", released)
		}
	}
}

func TestAddModeRefusesSecondRouteOnSameChannel(t *testing.T) {
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	stub := b.Stubs.Register("claude", 11, "/proj", struct{}{})
	first := MakeRouteKey("telegram", -100, ptrI64Val(281))
	second := MakeRouteKey("telegram", -200, ptrI64Val(412))
	claimConfirmed(t, b, stub, first)
	agent, brokerConn := newConnPair(t)
	go b.tryClaim(brokerConn, stub, second, "feature-x", false, true, true)
	raw, err := agent.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.AttachedMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Err, "already holding telegram · c3") || !strings.Contains(resp.Err, "detach target=telegram") {
		t.Fatalf("same-channel refusal=%q", resp.Err)
	}
	if got := stub.Routes(); len(got) != 1 || got[0] != first {
		t.Fatalf("refused add changed routes: %v", got)
	}
	if _, held := b.Routes.Holder(second); held {
		t.Fatal("refused same-channel add claimed the route table")
	}
}

func TestExprPlusSetsAdd(t *testing.T) {
	for _, test := range []struct {
		expr, channelName, name string
	}{
		{expr: "+web", channelName: "web"},
		{expr: "+c3", name: "c3"},
		{expr: "+ c3", name: "c3"},
	} {
		req := ipc.AttachReq{Expr: test.expr}
		applyExprToAttachReq(&req)
		if !req.Add || req.Channel != test.channelName || req.Name != test.name {
			t.Errorf("expr %q => %+v", test.expr, req)
		}
	}
	plain := ipc.AttachReq{Expr: "web"}
	applyExprToAttachReq(&plain)
	if plain.Add || plain.Channel != "web" {
		t.Fatalf("plain web changed semantics: %+v", plain)
	}
}

func targetedReleaseForTest(t *testing.T, b *Broker, stub *Stub, target string) (ipc.ReleaseResp, bool) {
	t.Helper()
	agent, brokerConn := newConnPair(t)
	raw, _ := json.Marshal(ipc.ReleaseReq{Op: ipc.OpRelease, Target: target})
	emptied := make(chan bool, 1)
	go func() {
		emptied <- b.handleRelease(brokerConn, stub, raw)
	}()
	frame, err := agent.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.ReleaseResp
	if err := json.Unmarshal(frame, &resp); err != nil {
		t.Fatal(err)
	}
	return resp, <-emptied
}

func TestDetachTargetReleasesOneKeepsRest(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	b := New(multiMappings())
	defer b.Shutdown()
	telegram := &fakeChannel{}
	web := &webFakeChannel{fakeChannel: &fakeChannel{}}
	registerTestChannel(b, telegram)
	registerTestChannel(b, web)
	stub := b.Stubs.Register("claude", 11, "/proj", nil)
	stub.SetStableSessionID("multi-session")
	telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
	webKey := MakeRouteKey("web", 42, nil)
	claimConfirmed(t, b, stub, telegramKey)
	claimConfirmed(t, b, stub, webKey)
	stub.SetOutputRoute(webKey)
	b.recordCurrentRoutesForStable(stub)
	resp, emptied := targetedReleaseForTest(t, b, stub, "web")
	if emptied {
		t.Fatal("target detach reported an empty set")
	}
	if !resp.OK || len(resp.Routes) != 1 || resp.Output == nil || routeKeyFromRef(*resp.Output) != telegramKey {
		t.Fatalf("target detach response=%+v", resp)
	}
	if stub.ExplicitlyDetached() {
		t.Fatal("target detach raised the whole-session detach barrier")
	}
	if got := stub.OutputRoute(); got == nil || *got != telegramKey {
		t.Fatalf("output fallback=%v, want telegram", got)
	}
	sa, ok := b.Mappings().LookupSessionAttachment("claude", "multi-session")
	if !ok || sa.Detached || len(sa.Routes) != 1 || routeKeyFromRef(sa.Routes[0]) != telegramKey {
		t.Fatalf("stored attachment after targeted detach=%+v ok=%v", sa, ok)
	}
	b.Routes.Release(telegramKey, stub.ConnID)
	stub.ClearRoutes()
	resumed := b.Stubs.Register("claude", 12, "/proj", nil)
	resumed.SetStableSessionID("multi-session")
	if output, _, _, recovered := b.recoverSession(resumed); !recovered || output != telegramKey {
		t.Fatalf("remaining telegram route was not recoverable: output=%v recovered=%v", output, recovered)
	}
}

func TestDetachTargetRespondsWithRemainingSet(t *testing.T) {
	b := New(multiMappings())
	defer b.Shutdown()
	stub := b.Stubs.Register("claude", 11, "/proj", nil)
	telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
	webKey := MakeRouteKey("web", 42, nil)
	claimConfirmed(t, b, stub, telegramKey)
	claimConfirmed(t, b, stub, webKey)
	stub.SetOutputRoute(webKey)

	resp, emptied := targetedReleaseForTest(t, b, stub, "web")
	if emptied || !resp.OK || resp.Op != ipc.OpReleaseResult {
		t.Fatalf("target detach result=%+v emptied=%v", resp, emptied)
	}
	if len(resp.Routes) != 1 || routeKeyFromRef(resp.Routes[0]) != telegramKey {
		t.Fatalf("remaining routes=%+v, want telegram", resp.Routes)
	}
	if resp.Output == nil || routeKeyFromRef(*resp.Output) != telegramKey {
		t.Fatalf("output fallback in response=%+v, want telegram", resp.Output)
	}

	failed, emptied := targetedReleaseForTest(t, b, stub, "web")
	if emptied || failed.OK || failed.Op != ipc.OpReleaseResult || failed.Err == "" {
		t.Fatalf("unheld target response=%+v emptied=%v", failed, emptied)
	}
}

func TestDetachLastRouteTombstones(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	stub := b.Stubs.Register("claude", 11, "/proj", nil)
	stub.SetStableSessionID("last-route")
	key := MakeRouteKey("telegram", -100, ptrI64Val(281))
	claimConfirmed(t, b, stub, key)
	b.recordCurrentRoutesForStable(stub)
	resp, emptied := targetedReleaseForTest(t, b, stub, "telegram")
	if !emptied {
		t.Fatal("final target detach did not empty the set")
	}
	if !resp.OK || len(resp.Routes) != 0 || resp.Output != nil {
		t.Fatalf("final target detach response=%+v", resp)
	}
	sa, ok := b.Mappings().LookupSessionAttachment("claude", "last-route")
	if !stub.ExplicitlyDetached() || !ok || !sa.Detached {
		t.Fatalf("last detach barrier/tombstone: barrier=%v attachment=%+v ok=%v", stub.ExplicitlyDetached(), sa, ok)
	}
}

func appendQueued(t *testing.T, b *Broker, key RouteKey, messageID int64, text string) {
	t.Helper()
	topicID := routeTopicID(key)
	if err := b.Queue.Append(queueRouteKey(key), &c3types.Inbound{
		Channel: key.Channel, ChatID: key.ChatID, TopicID: topicID,
		MessageID: messageID, Text: text, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
}

func fetchForTest(t *testing.T, b *Broker, stub *Stub, req ipc.FetchQueueReq) ipc.FetchQueueResp {
	t.Helper()
	agent, brokerConn := newConnPair(t)
	raw, _ := json.Marshal(req)
	go b.handleFetchQueue(brokerConn, stub, raw)
	return readFetchResp(t, agent)
}

func TestFetchQueueDrainsAllHeldRoutes(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(multiMappings())
	defer b.Shutdown()
	registerTestChannel(b, &fakeChannel{})
	registerTestChannel(b, &webFakeChannel{fakeChannel: &fakeChannel{}})
	stub := b.Stubs.Register("claude", 11, "/proj", nil)
	telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
	webKey := MakeRouteKey("web", 42, nil)
	claimConfirmed(t, b, stub, telegramKey)
	claimConfirmed(t, b, stub, webKey)
	stub.SetOutputRoute(webKey)
	appendQueued(t, b, telegramKey, 1, "telegram")
	appendQueued(t, b, webKey, 2, "web")

	resp := fetchForTest(t, b, stub, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "all", All: true, Ack: true})
	if resp.Err != "" || len(resp.Messages) != 2 {
		t.Fatalf("multi-route fetch=%+v", resp)
	}
	if resp.Messages[0].Channel != "web" || resp.Messages[1].Channel != "telegram" {
		t.Fatalf("fetch order=%v,%v, want output web then telegram", resp.Messages[0].Channel, resp.Messages[1].Channel)
	}
	for _, key := range []RouteKey{telegramKey, webKey} {
		if pending, _ := b.Queue.Pending(queueRouteKey(key)); pending != 0 {
			t.Fatalf("route %v pending=%d after drain", key, pending)
		}
	}
}

func TestFetchQueueChannelSelector(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(multiMappings())
	defer b.Shutdown()
	registerTestChannel(b, &fakeChannel{})
	registerTestChannel(b, &webFakeChannel{fakeChannel: &fakeChannel{}})
	stub := b.Stubs.Register("claude", 11, "/proj", nil)
	telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
	webKey := MakeRouteKey("web", 42, nil)
	claimConfirmed(t, b, stub, telegramKey)
	claimConfirmed(t, b, stub, webKey)
	appendQueued(t, b, telegramKey, 1, "telegram")
	appendQueued(t, b, webKey, 2, "web")

	bad := fetchForTest(t, b, stub, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "bad", Channel: "unknown", All: true, Ack: true})
	if bad.Err == "" {
		t.Fatal("unheld selector succeeded")
	}
	for _, key := range []RouteKey{telegramKey, webKey} {
		if pending, _ := b.Queue.Pending(queueRouteKey(key)); pending != 1 {
			t.Fatalf("unheld selector consumed %v: pending=%d", key, pending)
		}
	}
	selected := fetchForTest(t, b, stub, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "one", Channel: "telegram", All: true, Ack: true})
	if selected.Err != "" || len(selected.Messages) != 1 || selected.Messages[0].Channel != "telegram" {
		t.Fatalf("selected fetch=%+v", selected)
	}
	if pending, _ := b.Queue.Pending(queueRouteKey(webKey)); pending != 1 {
		t.Fatalf("selected fetch consumed web: pending=%d", pending)
	}
}

func TestFetchQueueSkipsRouteStolenAfterSnapshot(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(multiMappings())
	defer b.Shutdown()
	registerTestChannel(b, &fakeChannel{})

	victim := b.Stubs.Register("claude", 11, "/victim", nil)
	key := MakeRouteKey("telegram", -100, ptrI64Val(281))
	claimConfirmed(t, b, victim, key)
	appendQueued(t, b, key, 1, "must stay queued")
	staleRoutes := orderedHeldRoutes(victim)

	if evicted := b.Routes.ForceReleaseKey(key); evicted != victim {
		t.Fatalf("evicted=%p, want victim %p", evicted, victim)
	}
	// Deliberately leave the victim's stub snapshot + confirmed bit stale to
	// model the ForceReleaseKey -> ClearRouteIf window from the steal path.
	thief := b.Stubs.Register("codex", 22, "/thief", nil)
	if _, claimed := b.Routes.Claim(key, thief); !claimed {
		t.Fatal("thief did not claim stolen route")
	}
	thief.AddRoute(key)
	thief.MarkRouteConfirmed(key)

	logs := captureProtoLog(t)
	resp := b.fetchSelectedRoutes(victim, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "stale", All: true, Ack: true}, staleRoutes)
	if resp.Err == "" || len(resp.Messages) != 0 || resp.Remaining != 1 {
		t.Fatalf("stale-snapshot fetch=%+v", resp)
	}
	if pending, _ := b.Queue.Pending(queueRouteKey(key)); pending != 1 {
		t.Fatalf("stale holder consumed stolen route: pending=%d", pending)
	}
	if got := logs.String(); !strings.Contains(got, "SKIPPED route telegram · c3") || !strings.Contains(got, "no longer held") {
		t.Fatalf("stolen-route skip log missing route label/reason: %q", got)
	}
}

func TestPerRouteConfirmedGate(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(multiMappings())
	defer b.Shutdown()
	registerTestChannel(b, &fakeChannel{})
	registerTestChannel(b, &webFakeChannel{fakeChannel: &fakeChannel{}})
	stub := b.Stubs.Register("claude", 11, "/proj", nil)
	confirmed := MakeRouteKey("telegram", -100, ptrI64Val(281))
	unconfirmed := MakeRouteKey("web", 42, nil)
	claimConfirmed(t, b, stub, confirmed)
	if _, ok := b.Routes.Claim(unconfirmed, stub); !ok {
		t.Fatal("claim unconfirmed route")
	}
	stub.AddRoute(unconfirmed)
	appendQueued(t, b, confirmed, 1, "confirmed")
	appendQueued(t, b, unconfirmed, 2, "unconfirmed")

	if resp := fetchForTest(t, b, stub, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "u", Channel: "web", All: true, Ack: true}); resp.Err == "" {
		t.Fatal("unconfirmed route fetch consumed")
	}
	if resp := fetchForTest(t, b, stub, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "c", Channel: "telegram", All: true, Ack: true}); resp.Err != "" || len(resp.Messages) != 1 {
		t.Fatalf("confirmed sibling fetch=%+v", resp)
	}
	if pending, _ := b.Queue.Pending(queueRouteKey(unconfirmed)); pending != 1 {
		t.Fatalf("unconfirmed fetch changed queue: pending=%d", pending)
	}
	appendQueued(t, b, confirmed, 3, "confirmed sibling still drains")
	logs := captureProtoLog(t)
	drained := fetchForTest(t, b, stub, ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "mixed", All: true, Ack: true})
	if drained.Err != "" || len(drained.Messages) != 1 || drained.Messages[0].MessageID != 3 || drained.Remaining != 1 {
		t.Fatalf("mixed-confirmation drain=%+v", drained)
	}
	if pending, _ := b.Queue.Pending(queueRouteKey(unconfirmed)); pending != 1 {
		t.Fatalf("mixed drain touched unconfirmed sibling: pending=%d", pending)
	}
	if got := logs.String(); !strings.Contains(got, "SKIPPED route web") || !strings.Contains(got, "not confirmed") {
		t.Fatalf("unconfirmed-route skip log missing route label/reason: %q", got)
	}

	recordID, err := b.Queue.AppendTracked(queueRouteKey(unconfirmed), &c3types.Inbound{
		Channel: "web", ChatID: 42, MessageID: 7, Text: "push", Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	b.Workers.mu.Lock()
	worker := b.Workers.spawnLocked(unconfirmed)
	b.Workers.mu.Unlock()
	worker.recordCoveredByPush(7, "token", []string{recordID})
	stub.RecordPushRoute(7, "token", unconfirmed)
	raw, _ := json.Marshal(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered, UpdateID: 7, DeliveryToken: "token", OK: true, Count: 1})
	b.handleInboundDelivered(stub, raw)
	if pending, _ := b.Queue.Pending(queueRouteKey(unconfirmed)); pending != 2 {
		t.Fatalf("unconfirmed push ack consumed: pending=%d", pending)
	}
	stub.MarkRouteConfirmed(unconfirmed)
	stub.RecordPushRoute(7, "token", unconfirmed)
	b.handleInboundDelivered(stub, raw)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if pending, _ := b.Queue.Pending(queueRouteKey(unconfirmed)); pending == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	pending, _ := b.Queue.Pending(queueRouteKey(unconfirmed))
	t.Fatalf("confirmed push ack did not consume tracked line: pending=%d", pending)
}

func setOutputForTest(t *testing.T, b *Broker, stub *Stub, target string) ipc.SetOutputRouteResp {
	t.Helper()
	agent, brokerConn := newConnPair(t)
	raw, _ := json.Marshal(ipc.SetOutputRouteReq{Op: ipc.OpSetOutputRoute, Target: target})
	go b.handleSetOutputRoute(brokerConn, stub, raw)
	frame, err := agent.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.SetOutputRouteResp
	if err := json.Unmarshal(frame, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestSetOutputRouteValidatesHeld(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	b := New(multiMappings())
	defer b.Shutdown()
	stub := b.Stubs.Register("claude", 11, "/proj", nil)
	stub.SetStableSessionID("output-session")
	telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
	webKey := MakeRouteKey("web", 42, nil)
	claimConfirmed(t, b, stub, telegramKey)
	claimConfirmed(t, b, stub, webKey)
	stub.SetOutputRoute(webKey)
	b.recordCurrentRoutesForStable(stub)
	bad := setOutputForTest(t, b, stub, "other")
	if bad.OK || bad.Err == "" {
		t.Fatalf("unheld set_output response=%+v", bad)
	}
	if output := stub.OutputRoute(); output == nil || *output != webKey {
		t.Fatalf("unheld selector changed output: %v", output)
	}
	good := setOutputForTest(t, b, stub, "c3")
	if !good.OK || good.Output == nil || good.Output.Channel != "telegram" {
		t.Fatalf("held topic selector response=%+v", good)
	}
	stored, ok := b.Mappings().LookupSessionAttachment("claude", "output-session")
	if !ok || stored.Output == nil || stored.Output.Channel != "telegram" || stored.Channel != "telegram" {
		t.Fatalf("stored output/legacy fields=%+v ok=%v", stored, ok)
	}
}

func TestSetOutputRouteReEmitsPresence(t *testing.T) {
	b := New(multiMappings())
	defer b.Shutdown()
	telegram := &multiPresenceChannel{fakeChannel: &fakeChannel{}, name: "telegram"}
	web := &multiPresenceChannel{fakeChannel: &fakeChannel{}, name: "web"}
	registerTestChannel(b, telegram)
	registerTestChannel(b, web)
	stub := b.Stubs.Register("claude", 11, "/proj", nil)
	telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
	webKey := MakeRouteKey("web", 42, nil)
	claimConfirmed(t, b, stub, telegramKey)
	claimConfirmed(t, b, stub, webKey)
	stub.SetOutputRoute(webKey)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(telegram.presenceSnapshot()) >= 1 && len(web.presenceSnapshot()) >= 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	telegram.resetPresence()
	web.resetPresence()
	if resp := setOutputForTest(t, b, stub, "telegram"); !resp.OK {
		t.Fatalf("set output: %+v", resp)
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		tg, wb := telegram.presenceSnapshot(), web.presenceSnapshot()
		if len(tg) >= 1 && len(wb) >= 1 {
			if tg[len(tg)-1].holder.Role != "output" || wb[len(wb)-1].holder.Role != "input" {
				t.Fatalf("presence roles telegram=%+v web=%+v", tg, wb)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("presence was not re-emitted: telegram=%+v web=%+v", telegram.presenceSnapshot(), web.presenceSnapshot())
}

func TestSetOutputRouteGatedByProtocol(t *testing.T) {
	stub := &Stub{CLI: "claude", PID: 11, ConnID: 9}
	stub.SetPeerProtocolVersion(ipc.CompatibleProtocolMax + 1)
	agent, brokerConn := newConnPair(t)
	done := make(chan bool, 1)
	go func() {
		done <- refuseIncompatibleStateChange(brokerConn, stub, ipc.OpSetOutputRoute, []byte(`{"op":"set_output_route","target":"web"}`))
	}()
	raw, err := agent.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.SetOutputRouteResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if !<-done || resp.Op != ipc.OpSetOutputRouteResult || resp.Err == "" {
		t.Fatalf("protocol gate response=%+v", resp)
	}
}

func toolCallForTest(t *testing.T, b *Broker, stub *Stub, id, name string, args map[string]any) ipc.ToolResultMsg {
	t.Helper()
	agent, brokerConn := newConnPair(t)
	raw, _ := json.Marshal(ipc.ToolCallReq{Op: ipc.OpToolCall, ID: id, Name: name, Args: args})
	go b.handleToolCall(brokerConn, stub, raw)
	frame, err := agent.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.ToolResultMsg
	if err := json.Unmarshal(frame, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestToolChannelSelectorHeldOnly(t *testing.T) {
	b := New(multiMappings())
	defer b.Shutdown()
	telegram := &routeCaptureChannel{fakeChannel: &fakeChannel{}, name: "telegram"}
	web := &routeCaptureChannel{fakeChannel: &fakeChannel{}, name: "web"}
	registerTestChannel(b, telegram)
	registerTestChannel(b, web)
	stub := b.Stubs.Register("claude", 11, "/proj", nil)
	telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
	webKey := MakeRouteKey("web", 42, nil)
	claimConfirmed(t, b, stub, telegramKey)
	claimConfirmed(t, b, stub, webKey)
	stub.SetOutputRoute(webKey)

	calls := []struct {
		name string
		args map[string]any
	}{
		{name: "reply", args: map[string]any{"channel": "telegram", "text": "hello"}},
		{name: "edit_message", args: map[string]any{"channel": "telegram", "message_id": float64(7), "text": "edited"}},
		{name: "react", args: map[string]any{"channel": "telegram", "message_id": float64(7), "emoji": "ok"}},
	}
	for i, call := range calls {
		if resp := toolCallForTest(t, b, stub, string(rune('a'+i)), call.name, call.args); resp.Error != nil {
			t.Fatalf("%s selector failed: %+v", call.name, resp.Error)
		}
	}
	if len(telegram.sendRepliesSnapshot()) != 1 || len(telegram.editCallsSnapshot()) != 1 || len(telegram.reactSnapshot()) != 1 {
		t.Fatalf("telegram calls reply/edit/react=%d/%d/%d", len(telegram.sendRepliesSnapshot()), len(telegram.editCallsSnapshot()), len(telegram.reactSnapshot()))
	}
	if len(web.sendRepliesSnapshot()) != 0 || len(web.editCallsSnapshot()) != 0 || len(web.reactSnapshot()) != 0 {
		t.Fatal("selected tools reached the output web channel")
	}
	if !stub.HasReplied(telegramKey) || stub.HasReplied(webKey) {
		t.Fatal("reply state was not recorded on the selected route only")
	}
	beforeReplies := len(telegram.sendRepliesSnapshot())
	if resp := toolCallForTest(t, b, stub, "bad", "reply", map[string]any{"channel": "other", "text": "no"}); resp.Error == nil {
		t.Fatal("unheld channel selector succeeded")
	}
	if len(telegram.sendRepliesSnapshot()) != beforeReplies {
		t.Fatal("unheld selector sent to a channel")
	}
	if resp := toolCallForTest(t, b, stub, "raw", "reply", map[string]any{"channel": "telegram", "chat_id": float64(-100), "text": "no"}); resp.Error == nil || !strings.Contains(resp.Error.Message, "chat_id is not a tool argument") {
		t.Fatalf("raw destination override response=%+v", resp.Error)
	}
	_, forwarded, err := b.resolveToolRoute(stub, map[string]any{"channel": "telegram", "text": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if _, leaked := forwarded["channel"]; leaked {
		t.Fatal("channel selector leaked into worker/channel args")
	}
}

func TestPermGoesToKeyboardRouteAndNoticesTheRest(t *testing.T) {
	b := New(multiMappings())
	defer b.Shutdown()
	telegramCaps := c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}
	telegram := &fakeChannel{caps: &telegramCaps}
	web := &webFakeChannel{fakeChannel: &fakeChannel{}}
	registerTestChannel(b, telegram)
	registerTestChannel(b, web)
	stub := b.Stubs.Register("claude", 11, "/proj", nil)
	telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
	webKey := MakeRouteKey("web", 42, nil)
	claimConfirmed(t, b, stub, telegramKey)
	claimConfirmed(t, b, stub, webKey)
	stub.SetOutputRoute(webKey)
	raw, _ := json.Marshal(ipc.PermissionReq{Op: ipc.OpPermissionRequest, RequestID: "perm-multi", ToolName: "Bash", Preview: "go test"})
	b.handlePermissionRequest(nil, stub, raw)
	tgReplies := telegram.sendRepliesSnapshot()
	webReplies := web.sendRepliesSnapshot()
	if len(tgReplies) != 1 || len(tgReplies[0].Buttons) == 0 {
		t.Fatalf("keyboard replies=%+v", tgReplies)
	}
	if len(webReplies) != 1 || len(webReplies[0].Buttons) != 0 || !strings.HasPrefix(webReplies[0].Text, "⏸ Permission") {
		t.Fatalf("web notice=%+v", webReplies)
	}
	b.Perms.mu.Lock()
	pendingPerm := b.Perms.m["perm-multi"]
	b.Perms.mu.Unlock()
	if pendingPerm == nil || pendingPerm.route != telegramKey {
		t.Fatalf("pending permission route=%+v", pendingPerm)
	}
}

func TestAskGoesToKeyboardRoute(t *testing.T) {
	b := New(multiMappings())
	defer b.Shutdown()
	telegramCaps := c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}
	telegram := &fakeChannel{caps: &telegramCaps, replyReturnID: 77}
	web := &webFakeChannel{fakeChannel: &fakeChannel{}}
	registerTestChannel(b, telegram)
	registerTestChannel(b, web)
	stub := b.Stubs.Register("claude", 11, "/proj", nil)
	telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
	webKey := MakeRouteKey("web", 42, nil)
	claimConfirmed(t, b, stub, telegramKey)
	claimConfirmed(t, b, stub, webKey)
	stub.SetOutputRoute(webKey)
	agent, brokerConn := newConnPair(t)
	raw, _ := json.Marshal(ipc.AskRegisterReq{Op: ipc.OpAskRegister, AskID: "ask-multi", Question: "Choose", Options: []string{"A", "B"}})
	go b.handleAskRegister(brokerConn, stub, raw)
	frame, err := agent.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.AskRegisteredMsg
	if err := json.Unmarshal(frame, &resp); err != nil || !resp.OK {
		t.Fatalf("ask response=%+v err=%v", resp, err)
	}
	if len(telegram.sendRepliesSnapshot()) != 1 || len(web.sendRepliesSnapshot()) != 0 {
		t.Fatalf("ask routing telegram=%d web=%d", len(telegram.sendRepliesSnapshot()), len(web.sendRepliesSnapshot()))
	}
	b.Asks.mu.Lock()
	pendingAsk := b.Asks.m["ask-multi"]
	b.Asks.mu.Unlock()
	if pendingAsk == nil || pendingAsk.route != telegramKey {
		t.Fatalf("pending ask route=%+v", pendingAsk)
	}
}

func TestStealOneRouteEvictsFromSet(t *testing.T) {
	t.Run("output route sends one notice and falls back", func(t *testing.T) {
		b := New(multiMappings())
		defer b.Shutdown()
		victimSide, peerSide := net.Pipe()
		defer victimSide.Close()
		defer peerSide.Close()
		victim := b.Stubs.Register("claude", 21, "/victim", ipc.NewConn(victimSide))
		telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
		webKey := MakeRouteKey("web", 42, nil)
		claimConfirmed(t, b, victim, telegramKey)
		claimConfirmed(t, b, victim, webKey)
		victim.SetOutputRoute(webKey)
		thief := b.Stubs.Register("codex", 99, "/thief", struct{}{})
		if !b.tryClaim(nil, thief, webKey, "web", true, true, false) {
			t.Fatal("steal failed")
		}
		if victim.HasRoute(webKey) || victim.RouteConfirmed(webKey) {
			t.Fatal("stolen route or confirmation remained on victim")
		}
		if output := victim.OutputRoute(); output == nil || *output != telegramKey {
			t.Fatalf("victim output fallback=%v", output)
		}
		peer := ipc.NewConn(peerSide)
		raw, err := peer.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		var inbound ipc.InboundMsg
		if err := json.Unmarshal(raw, &inbound); err != nil {
			t.Fatal(err)
		}
		if inbound.Inbound.Event == nil || inbound.Inbound.Event.System == nil {
			t.Fatalf("steal notice=%+v", inbound)
		}
		message := inbound.Inbound.Event.System.Message
		if !strings.Contains(message, "output route web was taken by codex (pid 99)") || !strings.Contains(message, "replies now go to telegram · c3") {
			t.Fatalf("steal notice=%q", message)
		}
		_ = peerSide.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		if _, err := peer.ReadFrame(); err == nil {
			t.Fatal("output steal delivered more than one SystemEvent")
		}
	})

	t.Run("non-output route sends no notice", func(t *testing.T) {
		b := New(multiMappings())
		defer b.Shutdown()
		victimSide, peerSide := net.Pipe()
		defer victimSide.Close()
		defer peerSide.Close()
		victim := b.Stubs.Register("claude", 21, "/victim", ipc.NewConn(victimSide))
		telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
		webKey := MakeRouteKey("web", 42, nil)
		claimConfirmed(t, b, victim, telegramKey)
		claimConfirmed(t, b, victim, webKey)
		victim.SetOutputRoute(telegramKey)
		thief := b.Stubs.Register("codex", 99, "/thief", struct{}{})
		if !b.tryClaim(nil, thief, webKey, "web", true, true, false) {
			t.Fatal("steal failed")
		}
		_ = peerSide.SetReadDeadline(time.Now().Add(75 * time.Millisecond))
		if _, err := ipc.NewConn(peerSide).ReadFrame(); err == nil {
			t.Fatal("stealing a non-output route sent a SystemEvent")
		}
	})

	t.Run("last output reports no routes held", func(t *testing.T) {
		b := New(multiMappings())
		defer b.Shutdown()
		victimSide, peerSide := net.Pipe()
		defer victimSide.Close()
		defer peerSide.Close()
		victim := b.Stubs.Register("claude", 21, "/victim", ipc.NewConn(victimSide))
		webKey := MakeRouteKey("web", 42, nil)
		claimConfirmed(t, b, victim, webKey)
		thief := b.Stubs.Register("codex", 99, "/thief", struct{}{})
		if !b.tryClaim(nil, thief, webKey, "web", true, true, false) {
			t.Fatal("steal failed")
		}
		raw, err := ipc.NewConn(peerSide).ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		var inbound ipc.InboundMsg
		if err := json.Unmarshal(raw, &inbound); err != nil {
			t.Fatal(err)
		}
		if inbound.Inbound.Event == nil || inbound.Inbound.Event.System == nil || !strings.HasSuffix(inbound.Inbound.Event.System.Message, "no routes held") {
			t.Fatalf("last-route steal notice=%+v", inbound)
		}
	})
}

func recoverableMultiAttachment() mappings.SessionAttachment {
	topicID := int64(281)
	telegram := mappings.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &topicID, Name: "c3", Group: "main"}
	web := mappings.RouteRef{Channel: "web", ChatID: 42, Name: "web"}
	return mappings.SessionAttachment{
		Channel: "web", ChatID: 42, Name: "web",
		LastAttachedAt: time.Now().UTC(),
		Routes:         []mappings.RouteRef{telegram, web},
		Output:         &web,
	}
}

func TestRecoverReclaimsSetAndOutput(t *testing.T) {
	t.Run("full set and output", func(t *testing.T) {
		t.Setenv("C3_QUEUE_DIR", t.TempDir())
		mf := multiMappings()
		mf.UpsertSessionAttachment("claude", "multi", recoverableMultiAttachment())
		b := New(mf)
		defer b.Shutdown()
		stub := b.Stubs.Register("claude", 11, "/proj", nil)
		stub.SetStableSessionID("multi")
		output, _, _, ok := b.recoverSession(stub)
		if !ok || output.Channel != "web" || len(stub.Routes()) != 2 {
			t.Fatalf("recover output=%v routes=%v ok=%v", output, stub.Routes(), ok)
		}
		for _, key := range stub.Routes() {
			if !stub.RouteConfirmed(key) {
				t.Fatalf("recovered route not confirmed: %v", key)
			}
		}
	})

	t.Run("one collision skips only that route", func(t *testing.T) {
		t.Setenv("C3_QUEUE_DIR", t.TempDir())
		mf := multiMappings()
		mf.UpsertSessionAttachment("claude", "multi", recoverableMultiAttachment())
		b := New(mf)
		defer b.Shutdown()
		telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
		other := b.Stubs.Register("codex", 88, "/other", struct{}{})
		claimConfirmed(t, b, other, telegramKey)
		stub := b.Stubs.Register("claude", 11, "/proj", nil)
		stub.SetStableSessionID("multi")
		output, _, _, ok := b.recoverSession(stub)
		if !ok || output.Channel != "web" || len(stub.Routes()) != 1 || stub.Routes()[0].Channel != "web" {
			t.Fatalf("collision recover output=%v routes=%v ok=%v", output, stub.Routes(), ok)
		}
	})

	t.Run("stored output collision falls back to remaining route", func(t *testing.T) {
		t.Setenv("C3_QUEUE_DIR", t.TempDir())
		mf := multiMappings()
		mf.UpsertSessionAttachment("claude", "multi", recoverableMultiAttachment())
		b := New(mf)
		defer b.Shutdown()
		webKey := MakeRouteKey("web", 42, nil)
		other := b.Stubs.Register("codex", 88, "/other", struct{}{})
		claimConfirmed(t, b, other, webKey)
		stub := b.Stubs.Register("claude", 11, "/proj", nil)
		stub.SetStableSessionID("multi")
		logs := captureProtoLog(t)
		output, _, _, ok := b.recoverSession(stub)
		telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
		if !ok || output != telegramKey || len(stub.Routes()) != 1 || stub.Routes()[0] != telegramKey {
			t.Fatalf("output-collision recover output=%v routes=%v ok=%v", output, stub.Routes(), ok)
		}
		if current := stub.OutputRoute(); current == nil || *current != telegramKey {
			t.Fatalf("output-collision fallback=%v, want telegram", current)
		}
		if got := logs.String(); !strings.Contains(got, `recover: SKIPPED session=multi route="web"`) || !strings.Contains(got, "held by another live session") {
			t.Fatalf("collision skip log missing: %q", got)
		}
	})

	t.Run("legacy single route", func(t *testing.T) {
		t.Setenv("C3_QUEUE_DIR", t.TempDir())
		mf := mfWithTelegram()
		topicID := int64(281)
		mf.UpsertSessionAttachment("claude", "legacy", mappings.SessionAttachment{
			Channel: "telegram", ChatID: -100, TopicID: &topicID, Name: "c3",
			LastAttachedAt: time.Now().UTC(),
		})
		b := New(mf)
		defer b.Shutdown()
		stub := b.Stubs.Register("claude", 11, "/proj", nil)
		stub.SetStableSessionID("legacy")
		output, _, _, ok := b.recoverSession(stub)
		if !ok || output != MakeRouteKey("telegram", -100, &topicID) || len(stub.Routes()) != 1 {
			t.Fatalf("legacy recover output=%v routes=%v ok=%v", output, stub.Routes(), ok)
		}
	})
}

func TestRecoverSessionRespCarriesSet(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b, _, _ := brokerWithWeb(t, nil)
	defer b.Shutdown()
	b.Mappings().UpsertSessionAttachment("claude", "multi", recoverableMultiAttachment())

	_, done, resp := recoverViaPeer(t, b, "/proj", "multi")
	defer done()

	if !resp.Recovered || len(resp.Routes) != 2 {
		t.Fatalf("recover response=%+v, want complete two-route set", resp)
	}
	if resp.Output == nil || resp.Output.Channel != "web" {
		t.Fatalf("recover output=%+v, want web", resp.Output)
	}
}

func TestSessionAttachmentMatchesStubIgnoresRouteOrder(t *testing.T) {
	attachment := recoverableMultiAttachment()
	stub := &Stub{}
	webKey := MakeRouteKey("web", 42, nil)
	telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
	stub.AddRoute(webKey)
	stub.AddRoute(telegramKey)
	stub.SetOutputRoute(webKey)
	if !sessionAttachmentMatchesStub(attachment, stub) {
		t.Fatalf("same route set with different order did not match: stored=%+v stub=%v", attachment.Routes, stub.Routes())
	}
	stub.RemoveRoute(telegramKey)
	if sessionAttachmentMatchesStub(attachment, stub) {
		t.Fatal("different route sets matched")
	}
}

func TestSessionsListsEveryHeldRoute(t *testing.T) {
	b := New(multiMappings())
	defer b.Shutdown()
	stub := b.Stubs.Register("claude", 11, "/proj", struct{}{})
	telegramKey := MakeRouteKey("telegram", -100, ptrI64Val(281))
	webKey := MakeRouteKey("web", 42, nil)
	claimConfirmed(t, b, stub, telegramKey)
	claimConfirmed(t, b, stub, webKey)
	stub.SetOutputRoute(webKey)

	agent, brokerConn := newConnPair(t)
	raw, _ := json.Marshal(ipc.ListSessionsReq{Op: ipc.OpListSessions})
	go b.handleListSessions(brokerConn, raw)
	frame, err := agent.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var sessions ipc.ListSessionsReplyMsg
	if err := json.Unmarshal(frame, &sessions); err != nil {
		t.Fatal(err)
	}
	if len(sessions.Sessions) != 1 || sessions.Sessions[0].AttachedTo != "web, c3 (main)" {
		t.Fatalf("sessions=%+v", sessions.Sessions)
	}

	agent2, brokerConn2 := newConnPair(t)
	go b.handleListClaims(brokerConn2)
	frame, err = agent2.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var claims ipc.ClaimsListMsg
	if err := json.Unmarshal(frame, &claims); err != nil {
		t.Fatal(err)
	}
	outputs := 0
	for _, claim := range claims.Claims {
		if claim.ConnID != stub.ConnID {
			continue
		}
		if claim.IsOutput {
			outputs++
		}
	}
	if len(claims.Claims) != 2 || outputs != 1 {
		t.Fatalf("claims=%+v, want two rows and one output", claims.Claims)
	}
}
