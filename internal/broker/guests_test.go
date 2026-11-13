package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// Fake ids only: operator 7, operator DM 42, guests 501 and 502.
func mfWithGuests(guests ...int64) *mappings.MappingsFile {
	mf := mfWithTelegram()
	cc := mf.Channels["telegram"]
	cc.PrivateHolder = &mappings.PrivateHolder{CLI: "genie"}
	mf.Channels["telegram"] = cc
	mf.Allowlist = &mappings.Allowlist{Users: []int64{7}, Groups: []int64{-100}, Guests: guests}
	return mf
}

func guestDM(id int64, text string) *c3types.Inbound {
	return &c3types.Inbound{Channel: "telegram", ChatID: id, Sender: c3types.Sender{UserID: id}, Text: text, ConvKind: c3types.ConvKindDM}
}

func readOp(t *testing.T, peer *ipc.Conn, op ipc.Op) []byte {
	t.Helper()
	for range 20 {
		raw, err := peer.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		var head struct{ Op ipc.Op }
		_ = json.Unmarshal(raw, &head)
		if head.Op == op {
			return raw
		}
	}
	t.Fatalf("no %s frame", op)
	return nil
}

func helloAs(t *testing.T, peer *ipc.Conn, cli string, pid int, renderState string) {
	t.Helper()
	hello := ipc.HelloMsg{Build: "test", Op: ipc.OpHello, CLI: cli, PID: pid, CWD: "/g", CannotRenderChannels: renderState != ipc.RenderCapable, RenderState: renderState}
	if err := peer.WriteJSON(hello); err != nil {
		t.Fatal(err)
	}
	readOp(t, peer, ipc.OpHelloAck)
}

func attachPrivate(t *testing.T, peer *ipc.Conn) ipc.AttachedMsg {
	t.Helper()
	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, Channel: "telegram", Target: "private"}); err != nil {
		t.Fatal(err)
	}
	var ack ipc.AttachedMsg
	if err := json.Unmarshal(readOp(t, peer, ipc.OpAttached), &ack); err != nil {
		t.Fatal(err)
	}
	return ack
}

func toolCall(t *testing.T, peer *ipc.Conn, name string, args map[string]any) ipc.ToolResultMsg {
	t.Helper()
	if err := peer.WriteJSON(ipc.ToolCallReq{Op: ipc.OpToolCall, ID: "t1", Name: name, Args: args}); err != nil {
		t.Fatal(err)
	}
	var result ipc.ToolResultMsg
	if err := json.Unmarshal(readOp(t, peer, ipc.OpToolResult), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestGuestGate(t *testing.T) {
	b := brokerWithChannel(t, mfWithGuests(501), &fakeChannel{})
	defer b.Shutdown()

	if got := b.Gate(guestDM(501, "hi")); got != GateAllow {
		t.Fatalf("guest DM = %v, want allow", got)
	}
	cases := map[string]*c3types.Inbound{
		"non-guest DM":            guestDM(999, "hi"),
		"guest in a foreign chat": {Channel: "telegram", ChatID: 502, Sender: c3types.Sender{UserID: 501}, Text: "hi", ConvKind: c3types.ConvKindDM},
		"guest in unknown group":  {Channel: "telegram", ChatID: -300, Sender: c3types.Sender{UserID: 501}, Text: "hi", ConvKind: c3types.ConvKindGroup},
		"guest DM with topic":     {Channel: "telegram", ChatID: 501, TopicID: guestPtr(3), Sender: c3types.Sender{UserID: 501}, Text: "hi", ConvKind: c3types.ConvKindDM},
	}
	for name, in := range cases {
		if got := b.Gate(in); got != GateDrop {
			t.Errorf("%s = %v, want drop", name, got)
		}
	}
}

func TestGuestNeverTouchesPairingWindows(t *testing.T) {
	b := brokerWithChannel(t, mfWithGuests(501), &fakeChannel{})
	defer b.Shutdown()
	dmCode, err := b.Pairing.StartDM()
	if err != nil {
		t.Fatal(err)
	}
	groupCode, err := b.Pairing.StartGroup(-300)
	if err != nil {
		t.Fatal(err)
	}

	for range 10 {
		b.Gate(guestDM(501, "1234"))
	}
	b.Gate(guestDM(501, dmCode))
	if w := b.Pairing.DMWindow(); w == nil || w.Code != dmCode {
		t.Fatal("a guest consumed or closed the operator DM window")
	}
	group := &c3types.Inbound{Channel: "telegram", ChatID: -300, Sender: c3types.Sender{UserID: 501}, Text: groupCode, ConvKind: c3types.ConvKindGroup}
	if got := b.Gate(group); got != GateDrop {
		t.Fatalf("guest group pairing = %v, want drop", got)
	}
	if b.Mappings().IsGroupAllowed(-300) || b.Mappings().IsUserAllowed(501) {
		t.Fatal("a guest changed the operator allowlist")
	}
}

func TestGuestBrokerCommandsAreSilent(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithGuests(501), fc)
	defer b.Shutdown()
	host := NewBrokerHost(b, "telegram")

	for _, text := range []string{"/status", "/queue", "/queue c3", "/drain 1 -> c3", "/status@bot"} {
		reply, handled := host.HandleCommand(guestDM(501, text))
		if !handled || reply != "" {
			t.Errorf("%q = (%q, %v), want silent handled", text, reply, handled)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if sent := fc.sendRepliesSnapshot(); len(sent) != 0 {
		t.Fatalf("guest command produced %d send(s)", len(sent))
	}
	if reply, handled := host.HandleCommand(guestDM(501, "/cancel")); handled || reply != "" {
		t.Fatal("other slash text must route to the holder as text")
	}
}

func TestGuestTapsRefusedOnOperatorCards(t *testing.T) {
	b := brokerWithChannel(t, mfWithGuests(501), &fakeChannel{})
	defer b.Shutdown()
	route := MakeRouteKey("telegram", 501, nil)
	actor := c3types.Sender{UserID: 501}
	if b.resolvePerm(route, &c3types.CallbackEvent{Actor: actor, Data: "perm:allow:abcde", MessageID: 1}) {
		t.Fatal("guest permission tap accepted")
	}
	b.resolveAutoTap(route, &c3types.CallbackEvent{Actor: actor, Data: "auto:approve:x", MessageID: 1})
}

func TestPrivateAttachRefusals(t *testing.T) {
	cases := []struct {
		name, cli, render string
		mf                *mappings.MappingsFile
	}{
		{"no private_holder config", "genie", ipc.RenderQueueOnly, func() *mappings.MappingsFile {
			mf := mfWithGuests(501)
			cc := mf.Channels["telegram"]
			cc.PrivateHolder = nil
			mf.Channels["telegram"] = cc
			return mf
		}()},
		{"claude claiming queue-only", "claude", ipc.RenderQueueOnly, mfWithGuests(501)},
		{"configured cli that renders", "genie", ipc.RenderCapable, mfWithGuests(501)},
		{"configured cli cross-session", "genie", ipc.RenderCrossSession, mfWithGuests(501)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := brokerWithChannel(t, tc.mf, &fakeChannel{})
			defer b.Shutdown()
			peer, done := peerPair(t, b)
			defer done()
			helloAs(t, peer, tc.cli, os.Getpid(), tc.render)
			if ack := attachPrivate(t, peer); ack.OK {
				t.Fatal("private attach accepted")
			}
			if _, held := b.Routes.Holder(MakeRouteKey("telegram", 501, nil)); held {
				t.Fatal("guest route claimed after refusal")
			}
		})
	}
}

func TestPrivateHolderRoutesAndSelectors(t *testing.T) {
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithGuests(501, 502), fc)
	defer b.Shutdown()
	peer, done := peerPair(t, b)
	defer done()
	helloAs(t, peer, "genie", os.Getpid(), ipc.RenderQueueOnly)

	ack := attachPrivate(t, peer)
	if !ack.OK || len(ack.Routes) != 2 || ack.Output != nil {
		t.Fatalf("attach = %+v, want two guest routes and no output", ack)
	}

	if r := toolCall(t, peer, "reply", map[string]any{"text": "x"}); r.Error == nil {
		t.Fatal("reply without a selector reached a guest")
	}
	for _, selector := range []string{"dm:999", "dm:42", "dm:0501", "dm:-501", "dm:501x"} {
		if r := toolCall(t, peer, "reply", map[string]any{"text": "x", "channel": selector}); r.Error == nil {
			t.Errorf("selector %q accepted", selector)
		}
	}
	if r := toolCall(t, peer, "reply", map[string]any{"text": "x", "channel": "dm:501", "chat_id": 502}); r.Error == nil {
		t.Fatal("raw chat_id accepted")
	}
	if r := toolCall(t, peer, "reply", map[string]any{"text": "hello", "channel": "dm:502"}); r.Error != nil {
		t.Fatalf("dm:502 reply: %+v", r.Error)
	}
	sent := fc.sendRepliesSnapshot()
	if len(sent) != 1 || sent[0].ChatID != 502 || !sent[0].IsHolderSend() {
		t.Fatalf("sends = %+v, want one holder send to 502", sent)
	}

	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, Target: "dm"}); err != nil {
		t.Fatal(err)
	}
	var other ipc.AttachedMsg
	_ = json.Unmarshal(readOp(t, peer, ipc.OpAttached), &other)
	if other.OK {
		t.Fatal("private holder attached a second kind of route")
	}

	second, closeSecond := peerPair(t, b)
	defer closeSecond()
	helloAs(t, second, "genie", 1, ipc.RenderQueueOnly)
	if ack := attachPrivate(t, second); ack.OK {
		t.Fatal("second private holder accepted")
	}
}

func TestPrivateHolderCannotTakePushes(t *testing.T) {
	stub := &Stub{}
	stub.SetRenderRoute(ipc.RenderCapable, "", false)
	stub.private.Store(true)
	if stub.CanRenderPush() || stub.tryRenderPush() {
		t.Fatal("private holder took a live push")
	}
	stub.delivery = &negotiatedSession{live: ipc.DeliveryLive{Channel: ipc.DeliveryEligibility{Eligible: true}}}
	stub.delivery.mu.Lock()
	live := stub.acceptedLive()
	stub.delivery.mu.Unlock()
	if live.Channel.Eligible || live.Inbox.Eligible {
		t.Fatal("private holder has a live transport")
	}
}

func TestPrivateHolderWithNoGuestsFetchesEmpty(t *testing.T) {
	b := brokerWithChannel(t, mfWithGuests(), &fakeChannel{})
	defer b.Shutdown()
	peer, done := peerPair(t, b)
	defer done()
	helloAs(t, peer, "genie", os.Getpid(), ipc.RenderQueueOnly)
	if ack := attachPrivate(t, peer); !ack.OK || len(ack.Routes) != 0 {
		t.Fatalf("attach = %+v", ack)
	}
	if err := peer.WriteJSON(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "f"}); err != nil {
		t.Fatal(err)
	}
	var resp ipc.FetchQueueResp
	_ = json.Unmarshal(readOp(t, peer, ipc.OpFetchQueueResult), &resp)
	if resp.Err != "" || len(resp.Messages) != 0 {
		t.Fatalf("fetch = %+v", resp)
	}
}

func TestReloadSyncsGuestRoutesAndDropsRemovedRows(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithGuests(501), &fakeChannel{})
	defer b.Shutdown()
	peer, done := peerPair(t, b)
	defer done()
	helloAs(t, peer, "genie", os.Getpid(), ipc.RenderQueueOnly)
	attachPrivate(t, peer)

	removed := MakeRouteKey("telegram", 501, nil)
	if err := b.Queue.Append(queueRouteKey(removed), guestDM(501, "queued")); err != nil {
		t.Fatal(err)
	}
	b.SetMappings(mfWithGuests(502))

	if _, held := b.Routes.Holder(removed); held {
		t.Fatal("removed guest route still held")
	}
	if holder, held := b.Routes.Holder(MakeRouteKey("telegram", 502, nil)); !held || holder.CLI != "genie" {
		t.Fatal("added guest route not claimed by the private holder")
	}
	waitFor(t, "removed guest rows dropped", func() bool {
		n, _ := b.Queue.Pending(queueRouteKey(removed))
		return n == 0
	})
}

func TestBrokerTextNeverReachesPrivateChats(t *testing.T) {
	b := brokerWithChannel(t, mfWithGuests(501), &fakeChannel{})
	defer b.Shutdown()
	host := NewBrokerHost(b, "telegram")
	for chat, want := range map[int64]bool{501: false, 777: false, 42: true, 7: true, -100: true} {
		if got := host.AllowBrokerSend(chat, nil); got != want {
			t.Errorf("chat %d = %v, want %v", chat, got, want)
		}
	}
	if !host.AllowBrokerSend(501, guestPtr(2)) {
		t.Error("a topic route is not a private chat")
	}
}

func TestGuestChatsExcludedFromOperatorQueues(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithGuests(501), &fakeChannel{})
	defer b.Shutdown()
	guest := MakeRouteKey("telegram", 501, nil)
	if err := b.Queue.Append(queueRouteKey(guest), guestDM(501, "private")); err != nil {
		t.Fatal(err)
	}
	for _, row := range b.queueIndex(cmdScope{channel: "telegram", dm: true}) {
		if row.key == guest {
			t.Fatal("guest chat listed in /queue")
		}
	}
	topic := MakeRouteKey("telegram", -100, guestPtr(281))
	for _, spec := range []DrainSpec{{Source: guest, Target: topic}, {Source: topic, Target: guest}} {
		if _, err := b.Drain(spec); err == nil || !strings.Contains(err.Error(), "private") {
			t.Fatalf("drain %+v err = %v", spec, err)
		}
	}
}

func TestSaveKeepsHandEditedGuests(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mappings.json")
	disk := mfWithGuests(502)
	if err := mappings.Write(path, disk); err != nil {
		t.Fatal(err)
	}
	memory := mfWithGuests(501, 502)
	got := withDiskGuests(path, memory)
	if g := got.AllowlistOrEmpty().Guests; len(g) != 1 || g[0] != 502 {
		t.Fatalf("guests = %v, want the on-disk [502]", g)
	}
	if len(memory.Allowlist.Guests) != 2 {
		t.Fatal("in-memory mappings mutated")
	}
}

func TestConvertedEventRowsAreMarked(t *testing.T) {
	ev := &c3types.Inbound{Channel: "telegram", ChatID: 501, Kind: c3types.InboundReaction,
		Event: &c3types.InboundEvent{Reaction: &c3types.ReactionEvent{MessageID: 9, Added: []string{"x"}}}}
	row := eventMessage(ev)
	if row == nil || row.ConvertedFrom != c3types.InboundReaction {
		t.Fatalf("row = %+v, want ConvertedFrom=reaction", row)
	}
}

func guestPtr(v int64) *int64 { return &v }

func TestConvertedEventRowDoesNotStallReadyPrefix(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, s, routes := readyPrefixFixture(t)
	appendReadyPrefixRow(t, b, routes[0], false, false, false)
	reaction := eventMessage(&c3types.Inbound{Channel: "telegram", ChatID: -100, Kind: c3types.InboundReaction,
		Event: &c3types.InboundEvent{Reaction: &c3types.ReactionEvent{MessageID: 9, Added: []string{"x"}}}})
	if _, err := b.Queue.AppendTrackedIntake(queueRouteKey(routes[0]), reaction, nil, nil); err != nil {
		t.Fatal(err)
	}
	appendReadyPrefixRow(t, b, routes[0], false, false, false)

	raw := intakeFetchWire(t, b, s, ipc.FetchQueueReq{ID: "q", Mode: "ready_prefix", All: true})
	var resp struct {
		Messages  []c3types.Inbound `json:"messages"`
		BlockedOn *ipc.BlockedOn    `json:"blocked_on"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.BlockedOn != nil || len(resp.Messages) != 3 || resp.Messages[1].ConvertedFrom != c3types.InboundReaction {
		t.Fatalf("fetch = %s", raw)
	}
}

func TestConvertedTapKeepsCallback(t *testing.T) {
	row := eventMessage(&c3types.Inbound{Channel: "telegram", ChatID: 501, Kind: c3types.InboundCallback,
		Event: &c3types.InboundEvent{Callback: &c3types.CallbackEvent{CallbackID: "cb-7", MessageID: 9, Data: "ask:a1:yes"}}})
	if row == nil || row.ConvertedFrom != c3types.InboundCallback || row.ConvertedCallbackID != "cb-7" || row.ConvertedCallbackData != "ask:a1:yes" {
		t.Fatalf("row = %+v", row)
	}
}

func TestGuestReactionIsDropped(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithGuests(501), &fakeChannel{})
	defer b.Shutdown()
	key := MakeRouteKey("telegram", 501, nil)
	w := &RouteWorker{broker: b, key: key}
	ev := &c3types.Inbound{Channel: "telegram", ChatID: 501, Kind: c3types.InboundReaction,
		Event: &c3types.InboundEvent{Reaction: &c3types.ReactionEvent{MessageID: 9, Added: []string{"x"}}}}
	if !w.convertEvent(t.Context(), ev) {
		t.Fatal("guest reaction not handled")
	}
	if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 0 {
		t.Fatalf("guest reaction queued %d row(s)", n)
	}
}
