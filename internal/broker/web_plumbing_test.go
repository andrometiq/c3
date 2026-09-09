package broker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

type webFakeChannel struct {
	*fakeChannel
	loginMu   sync.Mutex
	live      bool
	mintCalls int
	link      string
	mintErr   error
}

type slowTelegramChannel struct {
	*fakeChannel
	entered chan struct{}
	release chan struct{}
}

func (f *slowTelegramChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	close(f.entered)
	<-f.release
	return f.fakeChannel.SendReply(args)
}

func (f *webFakeChannel) Name() string { return "web" }
func (f *webFakeChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{Channel: "web", SpokenReplies: true, EditMessages: true}
}
func (f *webFakeChannel) HasLiveSession(int64) bool {
	f.loginMu.Lock()
	defer f.loginMu.Unlock()
	return f.live
}
func (f *webFakeChannel) MintLoginLink(int64) (string, error) {
	f.loginMu.Lock()
	defer f.loginMu.Unlock()
	f.mintCalls++
	if f.link == "" {
		f.link = "https://device.example/auth#test-token"
	}
	return f.link, f.mintErr
}
func (f *webFakeChannel) mintCount() int {
	f.loginMu.Lock()
	defer f.loginMu.Unlock()
	return f.mintCalls
}

func brokerWithWeb(t *testing.T, enabled *bool) (*Broker, *fakeChannel, *webFakeChannel) {
	t.Helper()
	mf := mfWithTelegram()
	telegramConfig := mf.Channels["telegram"]
	telegramConfig.MasterUserID = 42
	mf.Channels["telegram"] = telegramConfig
	mf.Channels["web"] = mappings.ChannelConfig{Enabled: enabled, Listen: "127.0.0.1:8371"}
	mf.Allowlist = &mappings.Allowlist{Users: []int64{42}}
	telegramChannel := &fakeChannel{}
	b := brokerWithChannel(t, mf, telegramChannel)
	webChannel := &webFakeChannel{fakeChannel: &fakeChannel{}, link: "https://device.example/auth#test-token"}
	b.chMu.Lock()
	b.channels["web"] = &channelRegistration{Channel: webChannel}
	b.chMu.Unlock()
	return b, telegramChannel, webChannel
}

func readAttach(t *testing.T, peer *ipc.Conn, req ipc.AttachReq) ipc.AttachedMsg {
	t.Helper()
	if err := peer.WriteJSON(req); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var attached ipc.AttachedMsg
	if err := json.Unmarshal(raw, &attached); err != nil {
		t.Fatal(err)
	}
	return attached
}

func TestPersistedWebInboundCollisionDoesNotInvokeTelegramCallback(t *testing.T) {
	b := newTestBroker(t, mfWithTelegram())
	defer b.Shutdown()
	telegramSeam := []int64{17}
	var webPersisted int
	var telegramFailed, webFailed int
	b.SetPersistedCallback("telegram", func(in *c3types.Inbound) {
		if len(telegramSeam) > 0 && in.ChatID == 42 && in.MessageID == 7 {
			telegramSeam = telegramSeam[1:]
		}
	})
	b.SetPersistedCallback("web", func(*c3types.Inbound) { webPersisted++ })
	b.SetPersistFailedCallback("telegram", func(*c3types.Inbound) { telegramFailed++ })
	b.SetPersistFailedCallback("web", func(*c3types.Inbound) { webFailed++ })

	b.notifyPersisted(&c3types.Inbound{Channel: "web", ChatID: 42, MessageID: 7})
	if len(telegramSeam) != 1 {
		t.Fatal("persisted web inbound popped the overlapping Telegram seam entry")
	}
	if webPersisted != 1 {
		t.Fatalf("web callback count=%d, want 1", webPersisted)
	}
	b.notifyPersisted(&c3types.Inbound{Channel: "telegram", ChatID: 42, MessageID: 7})
	if len(telegramSeam) != 0 {
		t.Fatal("Telegram inbound did not invoke Telegram's own callback")
	}
	b.notifyPersistFailed(&c3types.Inbound{Channel: "web", ChatID: 42, MessageID: 7})
	if telegramFailed != 0 || webFailed != 1 {
		t.Fatalf("web persist failure dispatched telegram=%d web=%d, want 0/1", telegramFailed, webFailed)
	}
	b.notifyPersistFailed(&c3types.Inbound{Channel: "telegram", ChatID: 42, MessageID: 7})
	if telegramFailed != 1 {
		t.Fatalf("telegram persist failure callback count=%d, want 1", telegramFailed)
	}
}

func TestAttachResolutionWithTelegramAndWeb(t *testing.T) {
	b, _, _ := brokerWithWeb(t, nil)
	defer b.Shutdown()
	stub := &Stub{CLI: "claude", PID: 1}
	telegramRoute := MakeRouteKey("telegram", -100, ptrI64Val(281))
	bindOutputRouteForTest(stub, &telegramRoute)

	cases := []struct {
		name string
		req  ipc.AttachReq
	}{
		{"dm", ipc.AttachReq{Target: "dm"}},
		{"name", ipc.AttachReq{Name: "c3"}},
		{"id", ipc.AttachReq{TopicID: ptrI64Val(281)}},
		{"bare current route", ipc.AttachReq{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, errText := b.resolveAttachChannel(&tc.req, stub)
			if got != "telegram" || errText != "" {
				t.Fatalf("resolveAttachChannel=%q, %q; want telegram", got, errText)
			}
		})
	}
}

func TestAttachExprChannelSelectors(t *testing.T) {
	for _, expr := range []string{"web", "WEB", "telegram", "Telegram"} {
		req := ipc.AttachReq{Expr: expr}
		applyExprToAttachReq(&req)
		if req.Channel != strings.ToLower(expr) || req.Name != "" {
			t.Errorf("expr %q parsed as channel=%q name=%q", expr, req.Channel, req.Name)
		}
	}
}

func TestAttachWebClaimsOperatorRouteAndSendsLoginLink(t *testing.T) {
	b, telegramChannel, webChannel := brokerWithWeb(t, nil)
	defer b.Shutdown()
	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/workspace/project")

	attached := readAttach(t, peer, ipc.AttachReq{Op: ipc.OpAttach, Expr: "web"})
	if !attached.OK || attached.Channel != "web" || attached.ChatID != 42 || attached.TopicID != nil {
		t.Fatalf("attach web = %+v", attached)
	}
	if !strings.Contains(attached.Notice, "sent to your Telegram DM") || !strings.Contains(attached.Notice, "reply-tool") || !strings.Contains(attached.Notice, "Turn on 🔊 Voice on the page to hear replies; a system notice tells you when it is on.") || !strings.Contains(attached.Notice, "laptop") {
		t.Fatalf("attach web guidance incomplete: %q", attached.Notice)
	}
	if attached.Capabilities == nil || !attached.Capabilities.SpokenReplies {
		t.Fatalf("attach web capabilities = %+v, want SpokenReplies=true", attached.Capabilities)
	}
	if _, ok := b.Routes.Holder(MakeRouteKey("web", 42, nil)); !ok {
		t.Fatal("web operator route was not claimed")
	}
	if webChannel.mintCount() != 1 {
		t.Fatalf("MintLoginLink calls=%d, want 1", webChannel.mintCount())
	}
	replies := telegramChannel.sendRepliesSnapshot()
	if len(replies) != 1 {
		t.Fatalf("Telegram DM calls=%d, want 1", len(replies))
	}
	got := replies[0]
	for _, want := range []string{"CLI:", "CWD:", "Session ID:", "If you did not request this, ignore it."} {
		if !strings.Contains(got.Text, want) {
			t.Errorf("login DM missing %q: %q", want, got.Text)
		}
	}
	if got.ChatID != 42 || !got.DisableLinkPreview || got.Buttons != nil {
		t.Fatalf("login DM destination/options = %+v", got)
	}
}

func TestAttachStructuredTopicNamedWebStillUsesTelegram(t *testing.T) {
	b, _, _ := brokerWithWeb(t, nil)
	defer b.Shutdown()
	mf := b.Mappings().Clone()
	cc := mf.Channels["telegram"]
	cc.Topics = append(cc.Topics, mappings.Topic{ChatID: -100, TopicID: 900, Name: "web", Group: "main"})
	mf.Channels["telegram"] = cc
	b.SetMappings(mf)

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/workspace/project")
	attached := readAttach(t, peer, ipc.AttachReq{Op: ipc.OpAttach, Name: "web"})
	if !attached.OK || attached.Channel != "telegram" || attached.TopicID == nil || *attached.TopicID != 900 {
		t.Fatalf("structured name=web attached %+v, want Telegram topic 900", attached)
	}
}

func TestDisabledWebStanzaDoesNotAffectAttachOrPicker(t *testing.T) {
	disabled := false
	b, _, _ := brokerWithWeb(t, &disabled)
	defer b.Shutdown()
	if got := b.defaultChannel(); got != "telegram" {
		t.Fatalf("defaultChannel=%q, want telegram", got)
	}
	stub := &Stub{CLI: "claude", PID: 1, CWD: "/workspace/project"}
	proposal := b.buildPickTopic(stub, "telegram", stub.CWD)
	if proposal.WebAvailable {
		t.Fatal("disabled web stanza appeared in picker")
	}
	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/workspace/project")
	attached := readAttach(t, peer, ipc.AttachReq{Op: ipc.OpAttach, Name: "c3"})
	if !attached.OK || attached.Channel != "telegram" {
		t.Fatalf("disabled web affected named attach: %+v", attached)
	}
}

func TestPickerShowsWebSuggestion(t *testing.T) {
	b, _, _ := brokerWithWeb(t, nil)
	defer b.Shutdown()
	proposal := b.buildPickTopic(&Stub{}, "telegram", "")
	got := ipc.FormatAttached(&ipc.AttachedMsg{NeedsConfirmation: true, Proposal: proposal})
	if !strings.Contains(got, "web — on-the-go chat (`attach web`)") {
		t.Fatalf("picker omitted web suggestion: %q", got)
	}
}

func TestHelloAndObserveKeepTelegramPrimaryWithWebRegistered(t *testing.T) {
	b, _, _ := brokerWithWeb(t, nil)
	defer b.Shutdown()
	ack := b.buildHelloAck(ipc.HelloMsg{Build: "test", CWD: "/unmapped"}, &Stub{ConnID: 1})
	if ack.Capabilities == nil || ack.Capabilities.Channel != "telegram" {
		t.Fatalf("unmapped hello capabilities = %+v, want telegram", ack.Capabilities)
	}
	res := b.resolveTopicRoute(b.primaryChannel(), "c3", "", nil, "")
	if res.status != observeOK || res.key.Channel != "telegram" {
		t.Fatalf("observe primary resolution = %+v, want telegram", res)
	}
}

func TestTopicsListsWebRouteAndHolder(t *testing.T) {
	b, _, _ := brokerWithWeb(t, nil)
	defer b.Shutdown()
	key := MakeRouteKey("web", 42, nil)
	holder := &Stub{CLI: "claude", PID: 1, CWD: "/workspace/project", ConnID: 9}
	if _, ok := b.Routes.Claim(key, holder); !ok {
		t.Fatal("claim web route")
	}
	a, z := net.Pipe()
	defer a.Close()
	defer z.Close()
	go b.handleListTopics(ipc.NewConn(z))
	raw, err := ipc.NewConn(a).ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var list ipc.TopicsListMsg
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatal(err)
	}
	for _, topic := range list.Topics {
		if topic.Channel == "web" {
			if topic.ChatID != 42 || topic.RouteKind != "dm" || topic.ClaimedBy == nil || topic.ClaimedBy.CLI != "claude" {
				t.Fatalf("web topics row = %+v", topic)
			}
			return
		}
	}
	t.Fatal("topics response omitted web route")
}

func TestWebLoginLinkAttachDebounceAndLiveSession(t *testing.T) {
	b, telegramChannel, webChannel := brokerWithWeb(t, nil)
	defer b.Shutdown()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := b.sendWebLoginLink(&Stub{CLI: "claude"}, "attach web", true, true)
			if err != nil {
				t.Errorf("sendWebLoginLink: %v", err)
			}
			if result != webLoginSent && result != webLoginDebounced {
				t.Errorf("unexpected delivery result: %v", result)
			}
		}()
	}
	wg.Wait()
	if webChannel.mintCount() != 1 || len(telegramChannel.sendRepliesSnapshot()) != 1 {
		t.Fatalf("concurrent attach links: mint=%d dm=%d, want 1/1", webChannel.mintCount(), len(telegramChannel.sendRepliesSnapshot()))
	}
	if sent, err := NewBrokerHost(b, "web").SendWebLoginLink("requested from the web login page"); err != nil || sent {
		t.Fatalf("login-page request did not share attach debounce: sent=%v err=%v", sent, err)
	}
	if webChannel.mintCount() != 1 || len(telegramChannel.sendRepliesSnapshot()) != 1 {
		t.Fatal("login-page request inside attach debounce sent a second link")
	}
	webChannel.loginMu.Lock()
	webChannel.live = true
	webChannel.loginMu.Unlock()
	b.loginLinkMu.Lock()
	b.loginLinkLast = map[int64]time.Time{}
	b.loginLinkMu.Unlock()
	result, err := b.sendWebLoginLink(&Stub{}, "attach web", true, true)
	if err != nil || result != webLoginAlreadyLive {
		t.Fatalf("live-session link result=%v err=%v", result, err)
	}
	if webChannel.mintCount() != 1 {
		t.Fatalf("live session minted another link: %d", webChannel.mintCount())
	}
}

func TestWebLoginLinkDoesNotHoldLockDuringTelegramSend(t *testing.T) {
	b, telegramChannel, _ := brokerWithWeb(t, nil)
	defer b.Shutdown()
	slow := &slowTelegramChannel{
		fakeChannel: telegramChannel,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	b.chMu.Lock()
	b.channels["telegram"].Channel = slow
	b.chMu.Unlock()

	firstDone := make(chan error, 1)
	go func() {
		_, err := b.sendWebLoginLink(&Stub{CLI: "claude"}, "attach web", true, true)
		firstDone <- err
	}()
	select {
	case <-slow.entered:
	case <-time.After(time.Second):
		t.Fatal("Telegram send did not start")
	}

	secondDone := make(chan webLoginDelivery, 1)
	go func() {
		result, err := b.sendWebLoginLink(&Stub{CLI: "codex"}, "attach web", true, true)
		if err != nil {
			t.Errorf("second attach: %v", err)
		}
		secondDone <- result
	}()
	select {
	case result := <-secondDone:
		if result != webLoginDebounced {
			t.Fatalf("second attach result=%v, want debounced", result)
		}
	case <-time.After(time.Second):
		t.Fatal("slow Telegram send held loginLinkMu across I/O")
	}
	close(slow.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first attach: %v", err)
	}
}

func TestWebLoginLinkSendFailureRollsBackDebounce(t *testing.T) {
	b, telegramChannel, webChannel := brokerWithWeb(t, nil)
	defer b.Shutdown()
	telegramChannel.mu.Lock()
	telegramChannel.sendReplyErr = errors.New("delivery failed")
	telegramChannel.mu.Unlock()
	if _, err := b.sendWebLoginLink(&Stub{}, "attach web", true, true); err == nil {
		t.Fatal("failed Telegram send returned nil error")
	}
	telegramChannel.mu.Lock()
	telegramChannel.sendReplyErr = nil
	telegramChannel.mu.Unlock()
	result, err := b.sendWebLoginLink(&Stub{}, "attach web", true, true)
	if err != nil || result != webLoginSent {
		t.Fatalf("retry result=%v err=%v", result, err)
	}
	if got := webChannel.mintCount(); got != 2 {
		t.Fatalf("mint calls=%d, want retry to mint again", got)
	}
}

func TestAttachWebReleasesTelegramAndTelegramInboundIsHeld(t *testing.T) {
	b, telegramChannel, _ := brokerWithWeb(t, nil)
	defer b.Shutdown()
	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/workspace/project")
	telegramAttach := readAttach(t, peer, ipc.AttachReq{Op: ipc.OpAttach, Target: "dm"})
	if !telegramAttach.OK {
		t.Fatalf("telegram attach: %+v", telegramAttach)
	}
	webAttach := readAttach(t, peer, ipc.AttachReq{Op: ipc.OpAttach, Expr: "web"})
	if !webAttach.OK {
		t.Fatalf("web attach: %+v", webAttach)
	}
	telegramKey := MakeRouteKey("telegram", 42, nil)
	if _, held := b.Routes.Holder(telegramKey); held {
		t.Fatal("attach web left the Telegram claim held")
	}
	worker := &RouteWorker{key: telegramKey, broker: b, dedup: newDeliveredDedup(8)}
	in := &c3types.Inbound{Channel: "telegram", ChatID: 42, MessageID: 99, Text: "while away"}
	worker.forwardOrFallback(context.Background(), in, 0)
	replies := telegramChannel.sendRepliesSnapshot()
	held := false
	for _, reply := range replies {
		held = held || strings.Contains(reply.Text, "Held — nothing lost")
	}
	if !held {
		t.Fatalf("Telegram inbound after switch was not held: %+v", replies)
	}
}

func TestWebHeldCopyAndKeyboardlessPermissionNotice(t *testing.T) {
	if got := heldReplyText("web", 3); got != "📨 Held — no session is attached. Attach one from the CLI: `attach web`." {
		t.Fatalf("web held copy = %q", got)
	}
	b, _, webChannel := brokerWithWeb(t, nil)
	defer b.Shutdown()
	key := MakeRouteKey("web", 42, nil)
	worker := &RouteWorker{key: key, broker: b, dedup: newDeliveredDedup(8)}
	worker.forwardOrFallback(context.Background(), &c3types.Inbound{
		Channel: "web", ChatID: 42, MessageID: 8, Text: "unclaimed",
	}, 0)
	replies := webChannel.sendRepliesSnapshot()
	if len(replies) != 1 || replies[0].Text != heldReplyText("web", 1) {
		t.Fatalf("unclaimed web route notice = %+v", replies)
	}

	stub := &Stub{CLI: "claude", PID: 1, ConnID: 11}
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatal("claim web")
	}
	bindOutputRouteForTest(stub, &key)
	b.handlePermissionRequest(nil, stub, mustMarshalJSON(t, ipc.PermissionReq{
		Op: ipc.OpPermissionRequest, RequestID: "perm-web", ToolName: "Bash", Preview: "go test ./...",
	}))
	replies = webChannel.sendRepliesSnapshot()
	if len(replies) != 2 || replies[1].Text != "⏸ Permission needed at the laptop: Bash — go test ./..." || replies[1].Buttons != nil {
		t.Fatalf("keyboardless permission notice = %+v", replies)
	}
	b.Perms.mu.Lock()
	defer b.Perms.mu.Unlock()
	if len(b.Perms.m) != 0 {
		t.Fatalf("keyboardless permission registered verdict path: %d entries", len(b.Perms.m))
	}
}
