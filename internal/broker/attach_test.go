package broker

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// fakeChannel is a test-only channel.Channel implementation. Tracks calls
// and returns canned responses.
type fakeChannel struct {
	mu              sync.Mutex
	createCalls     []createCall
	validateCalls   []validateCall
	replyCalls      []c3types.ReplyArgs
	editCalls       []c3types.EditArgs
	createReturnID  int64
	createReturnErr error
	validateErr     error
	sendReplyErr    error
	// editMessages drives Capabilities().EditMessages so a test can opt into
	// the edit-capable held-notice path. Default false preserves the
	// cooldown-gated fallback behavior for every existing test.
	editMessages bool
	// replyReturnID is the message id SendReply returns (0 by default, matching
	// the original behavior).
	replyReturnID int64
	// editErr, when set, makes EditMessage return an error so a test can exercise
	// an edit failure.
	editErr error
	// caps optionally overrides the channel capability manifest. nil keeps the
	// historical default (Channel="telegram" + EditMessages from the field above)
	// so existing tests are unaffected; perm/ask tests that need inline keyboards
	// set it.
	caps *c3types.Capabilities
}

type createCall struct {
	chatID int64
	name   string
}
type validateCall struct {
	chatID int64
	tid    int64
}

func (f *fakeChannel) Name() string                                  { return "telegram" }
func (f *fakeChannel) Start(_ context.Context, _ channel.Host) error { return nil }
func (f *fakeChannel) Stop() error                                   { return nil }
func (f *fakeChannel) Capabilities() c3types.Capabilities {
	if f.caps != nil {
		return *f.caps
	}
	return c3types.Capabilities{Channel: "telegram", EditMessages: f.editMessages}
}
func (f *fakeChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replyCalls = append(f.replyCalls, args)
	if f.sendReplyErr != nil {
		return 0, f.sendReplyErr
	}
	return f.replyReturnID, nil
}
func (f *fakeChannel) sendRepliesSnapshot() []c3types.ReplyArgs {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]c3types.ReplyArgs, len(f.replyCalls))
	copy(out, f.replyCalls)
	return out
}
func (f *fakeChannel) editCallsSnapshot() []c3types.EditArgs {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]c3types.EditArgs, len(f.editCalls))
	copy(out, f.editCalls)
	return out
}
func (f *fakeChannel) SendTyping(int64, *int64) error { return nil }
func (f *fakeChannel) EditMessage(args c3types.EditArgs) (*c3types.EditResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.editCalls = append(f.editCalls, args)
	if f.editErr != nil {
		return nil, f.editErr
	}
	return &c3types.EditResult{MessageID: args.MessageID}, nil
}
func (f *fakeChannel) editSnapshot() []c3types.EditArgs {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]c3types.EditArgs, len(f.editCalls))
	copy(out, f.editCalls)
	return out
}
func (f *fakeChannel) React(c3types.ReactArgs) error             { return nil }
func (f *fakeChannel) DownloadAttachment(string) (string, error) { return "", nil }

func (f *fakeChannel) CreateTopic(chatID int64, name string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls = append(f.createCalls, createCall{chatID, name})
	if f.createReturnErr != nil {
		return 0, f.createReturnErr
	}
	return f.createReturnID, nil
}

func (f *fakeChannel) ValidateTopic(chatID int64, tid int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.validateCalls = append(f.validateCalls, validateCall{chatID, tid})
	return f.validateErr
}

func (f *fakeChannel) StopPoll(int64, int64) (*c3types.PollResult, error) {
	return &c3types.PollResult{}, nil
}

// brokerWithChannel builds a Broker pre-wired with a fakeChannel and the
// supplied mappings. Returns the broker and a function to feed the broker a
// scratch mappings.json path so SaveMappings doesn't clobber the user's real
// config during tests.
func brokerWithChannel(t *testing.T, mf *mappings.MappingsFile, fc *fakeChannel) *Broker {
	t.Helper()
	// Redirect SaveMappings to a scratch file via XDG_CONFIG_HOME.
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	b := New(mf)
	// Bypass Broker.RegisterChannel (which would call fc.Start with a
	// real broker context); register manually.
	b.chMu.Lock()
	b.channels[fc.Name()] = &channelRegistration{Channel: fc}
	b.chMu.Unlock()
	return b
}

// peer pair: returns the adapter-side ipc.Conn and the broker-side closure
// that runs HandleConn against the broker-side end. Caller calls returned
// closure to start the handler in a goroutine.
func peerPair(t *testing.T, b *Broker) (*ipc.Conn, func()) {
	t.Helper()
	a, brokerSide := net.Pipe()
	go b.HandleConn(brokerSide)
	return ipc.NewConn(a), func() { _ = a.Close() }
}

func mfWithTelegram() *mappings.MappingsFile {
	return &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {
				DefaultGroup: "main",
				Groups: map[string]mappings.GroupConfig{
					"main": {ChatID: -100},
					"work": {ChatID: -200},
				},
				DMChatID: 42,
				Topics: []mappings.Topic{
					{ChatID: -100, TopicID: 281, Name: "c3", Group: "main"},
					{ChatID: -200, TopicID: 412, Name: "feature-x", Group: "work"},
				},
			},
		},
		Mappings: map[string]mappings.Mapping{},
	}
}

func helloAck(t *testing.T, peer *ipc.Conn, cwd string) {
	t.Helper()
	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 1, CWD: cwd}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
}

func TestAttach_DM(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/x")

	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, Target: "dm"}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var ack ipc.AttachedMsg
	if err := json.Unmarshal(raw, &ack); err != nil {
		t.Fatal(err)
	}
	if !ack.OK {
		t.Fatalf("DM attach failed: %s", ack.Err)
	}
	if ack.ChatID != 42 {
		t.Errorf("ChatID=%d, want 42", ack.ChatID)
	}
	if ack.Name != "dm" {
		t.Errorf("Name=%q, want dm", ack.Name)
	}
	if ack.TopicID != nil {
		t.Errorf("DM should have nil TopicID, got %v", ack.TopicID)
	}
}

func TestAttach_NameInDefaultGroup_SilentClaim(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/projects/c3")

	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/projects/c3", Name: "c3"}); err != nil {
		t.Fatal(err)
	}
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if !ack.OK {
		t.Fatalf("attach failed: %s", ack.Err)
	}
	if ack.NeedsConfirmation {
		t.Error("found-in-default-group should be silent claim, not proposal")
	}
	if ack.TopicID == nil || *ack.TopicID != 281 {
		t.Errorf("TopicID=%v, want &281", ack.TopicID)
	}
	// Mapping persisted.
	if _, ok := b.Mappings().LookupByCwd("/projects/c3"); !ok {
		t.Error("expected /projects/c3 mapping to be persisted")
	}
}

func TestAttach_NameInOtherGroup_ProposesDisambiguation(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/projects/feature-x")

	// feature-x exists in group "work" but not "main" (default).
	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/projects/feature-x", Name: "feature-x"})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if ack.OK {
		t.Error("expected proposal not OK")
	}
	if !ack.NeedsConfirmation {
		t.Errorf("expected NeedsConfirmation=true, got %+v", ack)
	}
	if ack.Proposal == nil || ack.Proposal.Action != "use_existing_other_group" {
		t.Errorf("Proposal=%+v, want use_existing_other_group", ack.Proposal)
	}
	if ack.Proposal.Existing == nil || ack.Proposal.Existing.Group != "work" {
		t.Errorf("Existing.Group=%v", ack.Proposal.Existing)
	}
	if ack.Proposal.Alternative == nil || ack.Proposal.Alternative.Action != "create" {
		t.Errorf("Alternative=%+v, want action=create", ack.Proposal.Alternative)
	}
}

func TestAttach_UnknownName_ProposesCreation(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/projects/widget-foo")

	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/projects/widget-foo", Name: "widget-foo"})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if ack.OK {
		t.Error("expected proposal not OK")
	}
	if ack.Proposal == nil || ack.Proposal.Action != "create" || ack.Proposal.Name != "widget-foo" {
		t.Errorf("Proposal=%+v", ack.Proposal)
	}
	// No CreateTopic call yet (proposal phase).
	if len(fc.createCalls) != 0 {
		t.Errorf("createCalls=%d, want 0 in proposal phase", len(fc.createCalls))
	}
}

func TestAttach_CreateTrue_CallsChannelCreateAndPersists(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{createReturnID: 917}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/projects/widget-foo")

	_ = peer.WriteJSON(ipc.AttachReq{
		Op: ipc.OpAttach, CWD: "/projects/widget-foo",
		Name: "widget-foo", Create: true,
	})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if !ack.OK {
		t.Fatalf("create failed: %s", ack.Err)
	}
	if ack.TopicID == nil || *ack.TopicID != 917 {
		t.Errorf("TopicID=%v, want &917", ack.TopicID)
	}
	if len(fc.createCalls) != 1 {
		t.Errorf("expected 1 CreateTopic call, got %d", len(fc.createCalls))
	}
	if fc.createCalls[0].name != "widget-foo" {
		t.Errorf("CreateTopic name=%q", fc.createCalls[0].name)
	}
	if _, ok := b.Mappings().LookupTopicByID("telegram", -100, 917); !ok {
		t.Error("topic should have been registered")
	}
	if _, ok := b.Mappings().LookupByCwd("/projects/widget-foo"); !ok {
		t.Error("cwd mapping should have been persisted")
	}
}

func TestAttach_TopicID_ValidatesAndClaims(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/x")

	tid := int64(999)
	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/x", TopicID: &tid})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if !ack.OK {
		t.Fatalf("attach by id failed: %s", ack.Err)
	}
	if len(fc.validateCalls) != 1 || fc.validateCalls[0].tid != 999 {
		t.Errorf("expected one ValidateTopic call for 999, got %+v", fc.validateCalls)
	}
	if _, ok := b.Mappings().LookupTopicByID("telegram", -100, 999); !ok {
		t.Error("placeholder topic should be in registry after attach-by-id")
	}
}

// TestAttach_TopicID_ChatIDCrossCheck (item 3) pins the id-addressed-replay
// fail-closed guard: when the replay carries the topic's chat_id, attachByTopicID
// refuses if the named group resolves to a DIFFERENT chat (so a same-id thread in
// the wrong chat can't be silently claimed); a MATCHING chat_id claims; and a
// zero/absent chat_id keeps the pre-fix behavior (no cross-check).
func TestAttach_TopicID_ChatIDCrossCheck(t *testing.T) {
	// group "work" → chat -200; group "main" → chat -100.
	tid := int64(412)

	t.Run("mismatch refused", func(t *testing.T) {
		b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
		defer b.Shutdown()
		peer, done := peerPair(t, b)
		defer done()
		helloAck(t, peer, "/x")

		// Topic 412 lives in "work"/-200, but the replay names group "main" (chat
		// -100) and cross-checks against the real chat -200 → mismatch → refuse.
		_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/x", TopicID: &tid, Group: "main", ChatID: -200})
		raw, _ := peer.ReadFrame()
		var ack ipc.AttachedMsg
		_ = json.Unmarshal(raw, &ack)
		if ack.OK {
			t.Fatalf("mismatched chat_id must be refused; got OK ack %+v", ack)
		}
		if !strings.Contains(ack.Err, "cross-check") {
			t.Errorf("refusal should name the cross-check; got err %q", ack.Err)
		}
		// No route claimed, no validate performed.
		if _, held := b.Routes.Holder(MakeRouteKey("telegram", -100, &tid)); held {
			t.Error("refused attach must not claim the default-group route")
		}
	})

	t.Run("match claims", func(t *testing.T) {
		fc := &fakeChannel{}
		b := brokerWithChannel(t, mfWithTelegram(), fc)
		defer b.Shutdown()
		peer, done := peerPair(t, b)
		defer done()
		helloAck(t, peer, "/x")

		// group "work" resolves to chat -200 == the supplied chat_id → claim.
		_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/x", TopicID: &tid, Group: "work", ChatID: -200})
		raw, _ := peer.ReadFrame()
		var ack ipc.AttachedMsg
		_ = json.Unmarshal(raw, &ack)
		if !ack.OK {
			t.Fatalf("matching chat_id must claim; got err %q", ack.Err)
		}
		if ack.ChatID != -200 || ack.TopicID == nil || *ack.TopicID != 412 {
			t.Errorf("claimed the wrong route: %+v", ack)
		}
	})

	t.Run("zero chat_id skips cross-check", func(t *testing.T) {
		b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
		defer b.Shutdown()
		peer, done := peerPair(t, b)
		defer done()
		helloAck(t, peer, "/x")

		// No chat_id → today's behavior: group "work" claims topic 412 as before.
		_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/x", TopicID: &tid, Group: "work"})
		raw, _ := peer.ReadFrame()
		var ack ipc.AttachedMsg
		_ = json.Unmarshal(raw, &ack)
		if !ack.OK {
			t.Fatalf("absent chat_id must preserve today's claim behavior; got err %q", ack.Err)
		}
		if ack.ChatID != -200 {
			t.Errorf("ChatID=%d, want -200", ack.ChatID)
		}
	})
}

func TestAttach_SavedMapping_ShowsPickerNoClaim(t *testing.T) {
	// Post-redesign a bare attach NEVER consults the saved cwd→topic mapping
	// (PATH A deleted, spec §2). With an empty stableSessionID (no recover op
	// yet) and no already-held route, a bare attach shows the picker and claims
	// NOTHING — the cwd map only seeds a suggestion (Phase 2), never a bind.
	mf := mfWithTelegram()
	mf.Mappings["/projects/widget"] = mappings.Mapping{
		Channel: "telegram", ChatID: -100, TopicID: 281,
		Name: "c3", Group: "main",
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/projects/widget")

	// Bare `attach` (no name) → picker, no silent claim.
	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/projects/widget"})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if ack.OK {
		t.Fatalf("saved-mapping bare attach must NOT silently claim; got OK Name=%q", ack.Name)
	}
	if ack.Proposal == nil || ack.Proposal.Action != "pick_topic" {
		t.Fatalf("bare attach must return a pick_topic proposal; got Status=%q Proposal=%+v Err=%q",
			ack.Status, ack.Proposal, ack.Err)
	}
	tid := int64(281)
	if h, ok := b.Routes.Holder(MakeRouteKey("telegram", -100, &tid)); ok {
		t.Errorf("the cwd-mapped topic must stay UNCLAIMED; got holder=%+v", h)
	}
}

func TestAttach_ExplicitNameOverridesSavedMapping(t *testing.T) {
	// 2026-05-09: a stale cwd mapping was silently overriding an
	// explicit name argument. Saved mapping must NOT win when the user
	// explicitly asks for a different topic name.
	mf := mfWithTelegram()
	mf.Mappings["/projects/widget"] = mappings.Mapping{
		Channel: "telegram", ChatID: -100, TopicID: 281,
		Name: "c3", Group: "main",
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/projects/widget")

	// Explicit name "widget" differs from saved mapping ("c3"). Saved
	// mapping should NOT silent-claim. We expect a propose-creation since
	// "widget" isn't in topics.
	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/projects/widget", Name: "widget"})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if ack.OK {
		t.Fatalf("expected propose-creation (not silent-claim from saved); got OK with Name=%q", ack.Name)
	}
	if !ack.NeedsConfirmation {
		t.Errorf("expected NeedsConfirmation=true; got false. Err=%s", ack.Err)
	}
	if ack.Proposal == nil || ack.Proposal.Name != "widget" {
		t.Errorf("expected proposal for name 'widget'; got %+v", ack.Proposal)
	}
}

func TestAttach_AlreadyClaimed_ReturnsHolderError(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// First connection claims c3 topic.
	peer1, done1 := peerPair(t, b)
	defer done1()
	helloAck(t, peer1, "/p1")
	_ = peer1.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/p1", Name: "c3"})
	raw, _ := peer1.ReadFrame()
	var ack1 ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack1)
	if !ack1.OK {
		t.Fatalf("first attach failed: %s", ack1.Err)
	}

	// Second connection tries the same — should fail.
	peer2, done2 := peerPair(t, b)
	defer done2()
	helloAck(t, peer2, "/p2")
	_ = peer2.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/p2", Name: "c3"})
	raw2, _ := peer2.ReadFrame()
	var ack2 ipc.AttachedMsg
	_ = json.Unmarshal(raw2, &ack2)
	if ack2.OK {
		t.Error("second attach to held route should fail")
	}
	if ack2.Err == "" {
		t.Error("expected error message identifying the holder")
	}
}

// _ = filepath kept to silence the unused-import warning since this test
// file may evolve; remove if removed elsewhere.
var _ = filepath.Join
var _ = time.Now

// ─── welcome-message-on-attach tests (TODO.md pre-release UX bug #1) ───────

func TestWelcomeText_HomeShortened(t *testing.T) {
	home, _ := os.UserHomeDir()
	if home == "" {
		t.Skip("no home dir")
	}
	stub := &Stub{CLI: "claude", CWD: filepath.Join(home, "projects", "app")}
	got := welcomeText(stub, "c3", "")
	if !strings.Contains(got, "~/projects/app") {
		t.Errorf("welcomeText should home-shorten cwd, got %q", got)
	}
	if strings.Contains(got, home) {
		t.Errorf("welcomeText should not include literal home prefix, got %q", got)
	}
}

func TestWelcomeText_IncludesCLIAndLabel(t *testing.T) {
	stub := &Stub{CLI: "claude", CWD: "/tmp/x"}
	got := welcomeText(stub, "my-project", "")
	for _, want := range []string{"claude", "my-project"} {
		if !strings.Contains(got, want) {
			t.Errorf("welcomeText missing %q in %q", want, got)
		}
	}
}

func TestWelcomeText_NoPID(t *testing.T) {
	// maintainer feedback 2026-05-14: PID is mechanical clutter,
	// don't include it.
	stub := &Stub{CLI: "claude", PID: 12345, CWD: "/tmp/x"}
	got := welcomeText(stub, "label", "")
	if strings.Contains(got, "12345") {
		t.Errorf("welcomeText should not include PID, got %q", got)
	}
}

func TestWelcomeText_NoCWD_StillFriendly(t *testing.T) {
	stub := &Stub{CLI: "claude"}
	got := welcomeText(stub, "label", "")
	if got == "" {
		t.Fatal("welcomeText with no cwd should still return something")
	}
	if !strings.Contains(got, "claude") || !strings.Contains(got, "label") {
		t.Errorf("welcomeText cwd-less: %q", got)
	}
}

func TestTryClaim_FreshClaimTriggersWelcome(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	stub := &Stub{CLI: "claude", PID: 1, CWD: "/home/u/proj"}
	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, stub, key, "c3", false, false) {
		t.Fatal("fresh claim should succeed (nil conn is OK because we won't hit the collision branch)")
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(fc.sendRepliesSnapshot()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	calls := fc.sendRepliesSnapshot()
	if len(calls) != 1 {
		t.Fatalf("want 1 welcome SendReply, got %d", len(calls))
	}
	r := calls[0]
	if r.ChatID != -100 || r.TopicID == nil || *r.TopicID != 914 {
		t.Errorf("welcome went to wrong destination: chat=%d topic=%v", r.ChatID, r.TopicID)
	}
	if !strings.Contains(r.Text, "claude") {
		t.Errorf("welcome text missing cli: %q", r.Text)
	}
}

func TestTryClaim_SameLogicalSessionReclaimSuppressesWelcome(t *testing.T) {
	// Adapter reconnect replays attach. Same CLI+PID+CWD → should NOT
	// re-send welcome (would spam the topic on every CLI bounce).
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)

	// First claim by stub#1.
	stub1 := &Stub{CLI: "claude", PID: 99, CWD: "/home/u/proj"}
	if !b.tryClaim(nil, stub1, key, "c3", false, false) {
		t.Fatal("first claim should succeed")
	}
	// Wait for the welcome to land.
	time.Sleep(50 * time.Millisecond)
	first := len(fc.sendRepliesSnapshot())
	if first != 1 {
		t.Fatalf("first claim should send 1 welcome, got %d", first)
	}

	// Simulate an adapter reconnect — same logical session (CLI+PID+CWD),
	// different ConnID.
	stub2 := &Stub{CLI: "claude", PID: 99, CWD: "/home/u/proj"}
	if !b.tryClaim(nil, stub2, key, "c3", false, false) {
		t.Fatal("re-claim by same logical session should succeed")
	}
	time.Sleep(50 * time.Millisecond)
	second := len(fc.sendRepliesSnapshot())
	if second != first {
		t.Errorf("same-session re-claim sent %d additional welcomes (want 0): total=%d, first=%d",
			second-first, second, first)
	}
}

// ─── cwd resolution + conflict guard (TODO.md pre-release UX bugs #2/#3) ───

func TestResolveAttachCWD_EmptyLaunchReturnsEmpty(t *testing.T) {
	if got := resolveAttachCWD("", "anything"); got != "" {
		t.Errorf("got %q, want \"\"", got)
	}
}

func TestResolveAttachCWD_NoTopicReturnsLaunch(t *testing.T) {
	if got := resolveAttachCWD("/home/u/proj", ""); got != "/home/u/proj" {
		t.Errorf("got %q, want /home/u/proj (no topic = no refinement)", got)
	}
}

func TestResolveAttachCWD_LaunchBasenameMatchesTopic(t *testing.T) {
	// Most common pattern: user launches claude FROM the project dir.
	if got := resolveAttachCWD("/home/u/c3", "c3"); got != "/home/u/c3" {
		t.Errorf("got %q, want /home/u/c3 (basename matches)", got)
	}
}

func TestResolveAttachCWD_RefinesToSubdirectory(t *testing.T) {
	// User launched in a parent directory; <launch>/<topic> exists.
	root := t.TempDir()
	sub := filepath.Join(root, "c3")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	got := resolveAttachCWD(root, "c3")
	if got != sub {
		t.Errorf("got %q, want %q (subdir exists, refine downward)", got, sub)
	}
}

func TestResolveAttachCWD_SubdirNotADirectory(t *testing.T) {
	// <launch>/<topic> exists but is a regular file, not a dir → fall back.
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "c3"), []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	got := resolveAttachCWD(root, "c3")
	if got != root {
		t.Errorf("got %q, want %q (subdir is a file, fall back to launch)", got, root)
	}
}

func TestResolveAttachCWD_SubdirMissingFallsBack(t *testing.T) {
	// User launched in a workspace root and attached to a topic with no matching
	// subdir. Best-effort fallback: persist the launch dir; conflict guard
	// in persistMapping will warn if this collides.
	root := t.TempDir()
	got := resolveAttachCWD(root, "topic-with-no-subdir")
	if got != root {
		t.Errorf("got %q, want %q (no subdir, fall back to launch)", got, root)
	}
}

func TestPersistMapping_RefinesToProjectSubdirectory(t *testing.T) {
	// End-to-end: stub launched in the PARENT of the project dir;
	// persistMapping should resolve down to the project subdir so two
	// topics attached from the same parent don't clobber each other's
	// default mappings.
	root := t.TempDir()
	c3 := filepath.Join(root, "c3")
	sthapati := filepath.Join(root, "sthapati")
	for _, d := range []string{c3, sthapati} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// First attach: c3.
	stub := &Stub{CLI: "claude", PID: 1, CWD: root}
	b.persistMapping(stub, "telegram", -100, 914, "c3", "main")
	// Second attach: sthapati (same launch root).
	b.persistMapping(stub, "telegram", -100, 207, "sthapati", "main")

	if got, ok := b.Mappings().LookupByCwd(c3); !ok || got.TopicID != 914 {
		t.Errorf("expected %s → topic 914, got %+v (ok=%v)", c3, got, ok)
	}
	if got, ok := b.Mappings().LookupByCwd(sthapati); !ok || got.TopicID != 207 {
		t.Errorf("expected %s → topic 207, got %+v (ok=%v)", sthapati, got, ok)
	}
	if _, ok := b.Mappings().LookupByCwd(root); ok {
		t.Errorf("expected NO mapping at launch root %s, but one was persisted (silent-rebind bug back?)", root)
	}
}

func TestPersistMapping_RecordsSessionAttachment(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	stub := &Stub{CLI: "claude", PID: 1, CWD: t.TempDir()}
	stub.SetStableSessionID("sess-xyz")
	b.persistMapping(stub, "telegram", -100, 914, "c3", "main")

	sa, ok := b.Mappings().LookupSessionAttachment("claude", "sess-xyz")
	if !ok {
		t.Fatal("session attachment not recorded")
	}
	if sa.Name != "c3" || sa.ChatID != -100 || sa.TopicID == nil || *sa.TopicID != 914 || sa.Group != "main" || sa.Detached {
		t.Fatalf("session attachment = %+v", sa)
	}
}

func TestPersistMapping_EmptyStableIDNoOp(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	stub := &Stub{CLI: "claude", PID: 1, CWD: t.TempDir()} // no stable id set
	b.persistMapping(stub, "telegram", -100, 914, "c3", "main")
	if len(b.Mappings().SessionAttachmentsByCLI) != 0 {
		t.Fatalf("empty stable id must not record an attachment; got %d CLI namespaces", len(b.Mappings().SessionAttachmentsByCLI))
	}
}

func TestPersistMapping_RecordsSessionAttachmentEvenOnRebindRefusal(t *testing.T) {
	// Even when the cwd rebind is refused (saved default unchanged), the
	// session-id recovery entry must still be recorded — it's keyed on the
	// session, not the cwd.
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	root := t.TempDir()
	name1 := filepath.Base(root)
	stub := &Stub{CLI: "claude", PID: 1, CWD: root}
	stub.SetStableSessionID("sess-1")
	b.persistMapping(stub, "telegram", -100, 914, name1, "main") // first: cwd→914 persisted

	// Second attach to a DIFFERENT topic from the same cwd → rebind refused.
	stub2 := &Stub{CLI: "claude", PID: 1, CWD: root}
	stub2.SetStableSessionID("sess-2")
	b.persistMapping(stub2, "telegram", -100, 207, name1, "main")

	if got, ok := b.Mappings().LookupByCwd(root); !ok || got.TopicID != 914 {
		t.Fatalf("cwd default should stay 914 (rebind refused), got %+v ok=%v", got, ok)
	}
	sa, ok := b.Mappings().LookupSessionAttachment("claude", "sess-2")
	if !ok || sa.TopicID == nil || *sa.TopicID != 207 {
		t.Fatalf("session attachment for sess-2 should record topic 207 despite cwd-rebind refusal, got %+v ok=%v", sa, ok)
	}
}

func TestPersistMapping_RebindRefusesToOverwrite(t *testing.T) {
	// maintainer 2026-05-14: "rebinding should be explicit." persistMapping
	// must NOT silently change a saved cwd → topic mapping. The live
	// claim is granted upstream in tryClaim; persistMapping's job is
	// preserving the saved default until the user takes an explicit
	// action (edit mappings.json).
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	root := t.TempDir()
	stub := &Stub{CLI: "claude", PID: 1, CWD: root}

	// First attach claims topic 914 under a name matching root's
	// basename → resolveAttachCWD returns root → persisted.
	name1 := filepath.Base(root)
	b.persistMapping(stub, "telegram", -100, 914, name1, "main")
	if got, ok := b.Mappings().LookupByCwd(root); !ok || got.TopicID != 914 {
		t.Fatalf("first attach should persist topic 914: got %+v ok=%v", got, ok)
	}

	// Second attach to a different topic (no subdir refinement, falls
	// back to root). Should NOT clobber.
	b.persistMapping(stub, "telegram", -100, 207, "other", "main")

	got, ok := b.Mappings().LookupByCwd(root)
	if !ok {
		t.Fatal("first mapping should still be in place after refused rebind")
	}
	if got.TopicID != 914 {
		t.Errorf("rebind silently overwrote saved default: got topic %d, want 914", got.TopicID)
	}
}

// ─── welcome firing rules: Replay is the sole suppression signal ───────────
//
// History (2026-05-14): an earlier belt-and-suspenders 30-second post-startup
// recovery window false-positived against a real user attach — maintainer typed
// `attach` 21 seconds after a broker restart and the welcome never fired.
// The recovery window was removed. Replay (in AttachReq) is now the
// authoritative signal that an attach is operational recovery rather than
// user-initiated; same-logical-session re-claims are still suppressed by
// the upstream isFresh check in tryClaim.

func TestSendWelcome_FreshUserAttachJustAfterBrokerStartup_Fires(t *testing.T) {
	// Regression for 2026-05-14: a fresh `attach` typed shortly after the
	// broker started must fire welcome. Older code suppressed any attach
	// within 30s of broker startup (intended for adapter replays) and
	// silently swallowed legitimate user attaches.
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// Deliberately do NOT backdate any startup time — the broker is
	// "just-now-started" from the framework's perspective. Fresh user
	// attach (replay=false). Must still send.
	stub := &Stub{CLI: "claude", PID: 1, CWD: "/home/u/proj"}
	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, stub, key, "c3", false, false /*replay*/) {
		t.Fatal("fresh claim should succeed")
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(fc.sendRepliesSnapshot()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(fc.sendRepliesSnapshot()); got != 1 {
		t.Errorf("fresh user attach must fire welcome regardless of broker uptime: got %d sends, want 1", got)
	}
}

func TestTryClaim_ReplayFlagSuppressesWelcomeAfterBrokerBounce(t *testing.T) {
	// Distinct from the same-session reclaim case: this models a broker
	// restart, where the new broker's Routes map is empty. The adapter
	// sets Replay=true on its restored AttachReq so the broker knows the
	// attach is operational recovery, not a user-initiated attach.
	// Without the Replay flag, every broker bounce would post a welcome
	// to every reconnecting session.
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	stub := &Stub{CLI: "claude", PID: 99, CWD: "/home/u/proj"}
	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)

	if !b.tryClaim(nil, stub, key, "c3", false /*steal*/, true /*replay*/) {
		t.Fatal("replay claim against an empty Routes map should still succeed")
	}
	time.Sleep(50 * time.Millisecond)
	if got := len(fc.sendRepliesSnapshot()); got != 0 {
		t.Errorf("replay attach sent %d welcomes, want 0 (user didn't initiate)", got)
	}
}

// ─── atomic attach switch (W1 Phase 5 / spec §C) ───────────────────────────
//
// tryClaim must claim the NEW route before releasing the OLD one, and release
// the old one ONLY on a successful claim. A failed claim (live collision) must
// leave the stub's existing claim fully intact — otherwise a rejected
// attach-switch strands the stub attached to nothing and later messages to the
// old route are silently held as "no claim".

// drainedConn returns an *ipc.Conn whose writes are continuously drained by a
// background reader, so a synchronous WriteJSON (e.g. tryClaim's force_steal
// proposal on the collision path) completes instead of blocking on net.Pipe's
// unbuffered wire. Closed via t.Cleanup.
func drainedConn(t *testing.T) *ipc.Conn {
	t.Helper()
	near, far := net.Pipe()
	go func() {
		peer := ipc.NewConn(far)
		for {
			if _, err := peer.ReadFrame(); err != nil {
				return
			}
		}
	}()
	conn := ipc.NewConn(near)
	t.Cleanup(func() {
		_ = near.Close()
		_ = far.Close()
	})
	return conn
}

func TestTryClaim_FailedSwitchKeepsOldRoute(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	dmKey := MakeRouteKey("telegram", 42, nil)
	tid := int64(281)
	c3Key := MakeRouteKey("telegram", -100, &tid)

	// 1. stubA claims the DM. Registered (not a bare &Stub) so it gets a
	// distinct ConnID — otherwise the zero-ConnID would make step 3's Claim
	// look like an idempotent self-reclaim instead of a cross-session
	// collision.
	stubA := b.Stubs.Register("claude", 1, "/a", nil)
	if !b.tryClaim(nil, stubA, dmKey, "DM", false, false) {
		t.Fatal("stubA: DM claim should succeed")
	}

	// 2. A second, alive, different-logical-session stub claims topic c3. Its
	// PID is a real live process, so the collision in step 3 is a LIVE
	// collision (not a dead-holder displacement that would succeed).
	stubB := b.Stubs.Register("codex", os.Getpid(), "/b", nil)
	if !b.tryClaim(nil, stubB, c3Key, "c3", false, false) {
		t.Fatal("stubB: c3 claim should succeed")
	}

	// 3. stubA tries to SWITCH to c3 without steal → live collision, false.
	conn := drainedConn(t)
	if b.tryClaim(conn, stubA, c3Key, "c3", false, false) {
		t.Fatal("stubA: switch to held c3 (steal=false) must fail with a collision")
	}

	// 4. The failed switch must leave stubA's OLD route fully intact — both the
	// stub's recorded route and the routes-table claim.
	if got := stubA.CurrentRoute(); got == nil || *got != dmKey {
		t.Errorf("stubA.CurrentRoute()=%v, want DM key %v (old route dropped on failed switch)", got, dmKey)
	}
	if h, ok := b.Routes.Holder(dmKey); !ok || h != stubA {
		t.Errorf("Routes.Holder(DM)=%v ok=%v, want stubA — failed switch released the old claim", h, ok)
	}
	// stubB still holds c3 untouched.
	if h, ok := b.Routes.Holder(c3Key); !ok || h != stubB {
		t.Errorf("Routes.Holder(c3)=%v ok=%v, want stubB", h, ok)
	}
}

func TestTryClaim_SuccessfulSwitchReleasesOld(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	dmKey := MakeRouteKey("telegram", 42, nil)
	tid := int64(281)
	c3Key := MakeRouteKey("telegram", -100, &tid)

	stubA := b.Stubs.Register("claude", 1, "/a", nil)
	if !b.tryClaim(nil, stubA, dmKey, "DM", false, false) {
		t.Fatal("stubA: DM claim should succeed")
	}

	// Switch to an UNHELD topic → succeeds.
	if !b.tryClaim(nil, stubA, c3Key, "c3", false, false) {
		t.Fatal("stubA: switch to unheld c3 should succeed")
	}

	// Single-claim invariant: stubA now holds c3, and the old DM route is free
	// (no double-hold).
	if got := stubA.CurrentRoute(); got == nil || *got != c3Key {
		t.Errorf("stubA.CurrentRoute()=%v, want c3 key %v", got, c3Key)
	}
	if h, ok := b.Routes.Holder(c3Key); !ok || h != stubA {
		t.Errorf("Routes.Holder(c3)=%v ok=%v, want stubA", h, ok)
	}
	if h, ok := b.Routes.Holder(dmKey); ok && h == stubA {
		t.Errorf("Routes.Holder(DM) still stubA — old route NOT released on successful switch (double-hold)")
	}
}

// TestPing_SendsReplyToAttachedRoute is the happy-path for the
// `/c3:ping` slash command. Attach a fake adapter to a topic; then a
// separate transient client (mimicking `c3-broker ping`) connects with
// the SAME cwd and issues PingThisSessionReq. The broker must locate
// the attached stub by CWD and dispatch one SendReply on its claimed
// route. TODO #19(b).
func TestPing_SendsReplyToAttachedRoute(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// First adapter — the "user session" — attaches to topic c3.
	adapter, doneA := peerPair(t, b)
	defer doneA()
	if err := adapter.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 99, CWD: "/projects/c3"}); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := adapter.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/projects/c3", Name: "c3"}); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	// Wait for the welcome SendReply so we can distinguish it from the
	// ping reply.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(fc.sendRepliesSnapshot()) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	beforePing := len(fc.sendRepliesSnapshot())

	// Transient ping client — different ConnID, different CLI label,
	// SAME CWD as the user's adapter.
	pinger, doneP := peerPair(t, b)
	defer doneP()
	if err := pinger.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "c3-broker-cli", PID: 100, CWD: "/projects/c3"}); err != nil {
		t.Fatal(err)
	}
	if _, err := pinger.ReadFrame(); err != nil {
		t.Fatal(err)
	}

	if err := pinger.WriteJSON(ipc.PingThisSessionReq{Op: ipc.OpPingThisSession, CWD: "/projects/c3"}); err != nil {
		t.Fatal(err)
	}
	raw, err := pinger.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.PingThisSessionReplyMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("parse ping reply: %v", err)
	}
	if !resp.OK {
		t.Fatalf("ping reply not OK: %q", resp.Err)
	}
	if resp.Channel != "telegram" || resp.Topic != "c3" {
		t.Errorf("ping reply channel=%q topic=%q, want telegram/c3", resp.Channel, resp.Topic)
	}
	if !strings.Contains(resp.SentText, "c3-ping") {
		t.Errorf("ping SentText missing c3-ping marker: %q", resp.SentText)
	}

	// Exactly one new SendReply, going to the attached route, content
	// mentions cwd + cli.
	calls := fc.sendRepliesSnapshot()
	if len(calls) != beforePing+1 {
		t.Fatalf("expected exactly one ping SendReply (was %d, now %d)", beforePing, len(calls))
	}
	r := calls[len(calls)-1]
	if r.ChatID != -100 || r.TopicID == nil || *r.TopicID != 281 {
		t.Errorf("ping went to wrong destination: chat=%d topic=%v", r.ChatID, r.TopicID)
	}
	for _, want := range []string{"c3-ping", "/projects/c3", "claude"} {
		if !strings.Contains(r.Text, want) {
			t.Errorf("ping text missing %q: %s", want, r.Text)
		}
	}
}

// TestPing_NoAttachedStubReturnsError covers the user-error case where
// the calling session has no current route claim. The broker must
// reply OK=false with an error message that the slash command can
// surface — and NOT call SendReply.
func TestPing_NoAttachedStubReturnsError(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	pinger, done := peerPair(t, b)
	defer done()
	if err := pinger.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "c3-broker-cli", PID: 1, CWD: "/nowhere"}); err != nil {
		t.Fatal(err)
	}
	if _, err := pinger.ReadFrame(); err != nil {
		t.Fatal(err)
	}

	if err := pinger.WriteJSON(ipc.PingThisSessionReq{Op: ipc.OpPingThisSession, CWD: "/nowhere"}); err != nil {
		t.Fatal(err)
	}
	raw, _ := pinger.ReadFrame()
	var resp ipc.PingThisSessionReplyMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("parse ping reply: %v", err)
	}
	if resp.OK {
		t.Fatalf("ping should fail when no stub matches cwd, got OK=true: %+v", resp)
	}
	if resp.Err == "" {
		t.Error("ping reply missing Err message")
	}
	if !strings.Contains(strings.ToLower(resp.Err), "not attached") {
		t.Errorf("ping Err should mention 'not attached', got %q", resp.Err)
	}
	if got := len(fc.sendRepliesSnapshot()); got != 0 {
		t.Errorf("ping should not SendReply on the unattached error path, got %d calls", got)
	}
}

// TestPing_MultipleStubsAtCWD_TargetsMostRecent is the determinism guard
// for `/c3:ping` when >1 stub matches the calling CWD. Registers two
// stubs at the same cwd, each holding its own claim (different topics
// in the same group), then issues a PingThisSessionReq. The broker
// MUST pick the most-recently-registered stub (highest ConnID), not
// whichever map iteration happens to visit first. Closes report
// MINOR m1 (2026-05-19).
func TestPing_MultipleStubsAtCWD_TargetsMostRecent(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	const cwd = "/projects/shared"

	// First stub at the cwd — claims topic 281 ("c3").
	older := b.Stubs.Register("claude", 1001, cwd, nil)
	tid1 := int64(281)
	key1 := MakeRouteKey("telegram", -100, &tid1)
	if !b.tryClaim(nil, older, key1, "c3", false, false) {
		t.Fatal("older stub: claim failed")
	}

	// Second stub at the SAME cwd — different PID, claims topic 412
	// ("feature-x" in group "work").
	newer := b.Stubs.Register("codex", 1002, cwd, nil)
	tid2 := int64(412)
	key2 := MakeRouteKey("telegram", -200, &tid2)
	if !b.tryClaim(nil, newer, key2, "feature-x", false, false) {
		t.Fatal("newer stub: claim failed")
	}
	if newer.ConnID <= older.ConnID {
		t.Fatalf("test setup wrong: newer.ConnID=%d not > older.ConnID=%d",
			newer.ConnID, older.ConnID)
	}

	// Give the async sendWelcome calls a chance to land — they're
	// goroutines and we don't want them to be counted as the ping reply.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(fc.sendRepliesSnapshot()) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	beforePing := len(fc.sendRepliesSnapshot())

	// Now issue the ping from a transient client at the same cwd.
	pinger, done := peerPair(t, b)
	defer done()
	if err := pinger.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "c3-broker-cli", PID: 9999, CWD: cwd}); err != nil {
		t.Fatal(err)
	}
	if _, err := pinger.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := pinger.WriteJSON(ipc.PingThisSessionReq{Op: ipc.OpPingThisSession, CWD: cwd}); err != nil {
		t.Fatal(err)
	}
	raw, err := pinger.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.PingThisSessionReplyMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("parse ping reply: %v", err)
	}
	if !resp.OK {
		t.Fatalf("ping reply not OK: %q", resp.Err)
	}

	// Reply must reference the NEWER stub's topic (feature-x), not the
	// older one (c3).
	if resp.Topic != "feature-x" {
		t.Errorf("ping targeted wrong stub: topic=%q, want %q (most-recent stub's claim)",
			resp.Topic, "feature-x")
	}

	// The new SendReply must go to the NEWER stub's destination
	// (chat=-200, topic=412), not (chat=-100, topic=281).
	calls := fc.sendRepliesSnapshot()
	if len(calls) != beforePing+1 {
		t.Fatalf("expected exactly one ping SendReply (was %d, now %d)", beforePing, len(calls))
	}
	r := calls[len(calls)-1]
	if r.ChatID != -200 || r.TopicID == nil || *r.TopicID != 412 {
		t.Errorf("ping went to older stub's destination: chat=%d topic=%v; want chat=-200 topic=412",
			r.ChatID, r.TopicID)
	}
	if !strings.Contains(r.Text, "codex") {
		t.Errorf("ping text should mention CLI=codex (newer stub's CLI): %q", r.Text)
	}
}

// ─── 3-state attach status (2026-05-19) ────────────────────────────────────
//
// Every success path must emit Status=ok so the adapter formatter can
// distinguish success from the new structured failure states
// (no_topics_configured, policy_rejected). See
// docs/plans/2026-05-19-codex-policy-3state.md.

func TestAttach_DM_EmitsStatusOK(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/x")

	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, Target: "dm"})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if !ack.OK {
		t.Fatalf("attach failed: %s", ack.Err)
	}
	if ack.Status != ipc.AttachStatusOK {
		t.Errorf("Status=%q want %q", ack.Status, ipc.AttachStatusOK)
	}
}

func TestAttach_ByName_EmitsStatusOK(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/projects/c3")

	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/projects/c3", Name: "c3"})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if !ack.OK {
		t.Fatalf("attach failed: %s", ack.Err)
	}
	if ack.Status != ipc.AttachStatusOK {
		t.Errorf("Status=%q want %q", ack.Status, ipc.AttachStatusOK)
	}
}

func TestAttach_ByTopicID_EmitsStatusOK(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/x")

	tid := int64(999)
	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/x", TopicID: &tid})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if !ack.OK {
		t.Fatalf("attach failed: %s", ack.Err)
	}
	if ack.Status != ipc.AttachStatusOK {
		t.Errorf("Status=%q want %q", ack.Status, ipc.AttachStatusOK)
	}
}

func TestAttach_CreateTrue_EmitsStatusOK(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{createReturnID: 917}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/projects/widget-foo")

	_ = peer.WriteJSON(ipc.AttachReq{
		Op: ipc.OpAttach, CWD: "/projects/widget-foo",
		Name: "widget-foo", Create: true,
	})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if !ack.OK {
		t.Fatalf("create failed: %s", ack.Err)
	}
	if ack.Status != ipc.AttachStatusOK {
		t.Errorf("Status=%q want %q", ack.Status, ipc.AttachStatusOK)
	}
}

func TestAttach_NoChannelsConfigured_EmitsStatusNoTopicsConfigured(t *testing.T) {
	// Broker has zero channels in mappings — user hasn't run setup yet.
	// Should surface AttachStatusNoTopicsConfigured so the formatter can
	// tell the user "run c3-broker setup", not the generic "attach failed".
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]mappings.ChannelConfig{},
		Mappings:      map[string]mappings.Mapping{},
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/x")

	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, Target: "dm"})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if ack.OK {
		t.Error("attach with no channels should fail")
	}
	if ack.Status != ipc.AttachStatusNoTopicsConfigured {
		t.Errorf("Status=%q want %q", ack.Status, ipc.AttachStatusNoTopicsConfigured)
	}
}

func TestAttach_DM_ChannelWithoutDMChatID_EmitsStatusNoTopicsConfigured(t *testing.T) {
	// Channel exists but DMChatID is unset (Topics may or may not exist).
	// Targeted "DM" attach should report the structured status so the
	// formatter can render the actionable next-step.
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {
				DefaultGroup: "main",
				Groups: map[string]mappings.GroupConfig{
					"main": {ChatID: -100},
				},
				DMChatID: 0,
				Topics:   nil,
			},
		},
		Mappings: map[string]mappings.Mapping{},
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/x")

	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, Target: "dm"})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if ack.OK {
		t.Error("attach DM with no DMChatID should fail")
	}
	if ack.Status != ipc.AttachStatusNoTopicsConfigured {
		t.Errorf("Status=%q want %q (DM=0 should be 'no destinations configured')",
			ack.Status, ipc.AttachStatusNoTopicsConfigured)
	}
}

func TestAttach_PolicyRejectedHint_ShortCircuitsToStatusPolicyRejected(t *testing.T) {
	// Codex adapter sets PolicyRejected=true on a re-invoke after the
	// agent observed the host's policy layer reject the prior attach.
	// Broker MUST short-circuit: don't validate, don't claim, don't
	// register topics. Just surface the structured status so the
	// adapter formatter renders the actionable user message.
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/x")

	_ = peer.WriteJSON(ipc.AttachReq{
		Op: ipc.OpAttach, CWD: "/x", Name: "c3",
		PolicyRejected: true,
	})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if ack.OK {
		t.Error("attach with PolicyRejected hint must not return OK")
	}
	if ack.Status != ipc.AttachStatusPolicyRejected {
		t.Errorf("Status=%q want %q", ack.Status, ipc.AttachStatusPolicyRejected)
	}
	if ack.NeedsConfirmation {
		t.Error("policy_rejected is terminal, not a proposal")
	}
	if len(fc.createCalls) > 0 || len(fc.validateCalls) > 0 {
		t.Errorf("policy_rejected must short-circuit; got create=%d validate=%d",
			len(fc.createCalls), len(fc.validateCalls))
	}
}

func TestAttach_SavedMapping_ShowsPickerNotStatusOK(t *testing.T) {
	// Flipped from the old status-OK assertion: a bare attach against a saved
	// cwd mapping no longer emits AttachStatusOK — it shows the picker (spec §2).
	mf := mfWithTelegram()
	mf.Mappings["/projects/widget"] = mappings.Mapping{
		Channel: "telegram", ChatID: -100, TopicID: 281,
		Name: "c3", Group: "main",
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/projects/widget")

	_ = peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, CWD: "/projects/widget"})
	raw, _ := peer.ReadFrame()
	var ack ipc.AttachedMsg
	_ = json.Unmarshal(raw, &ack)

	if ack.OK || ack.Status == ipc.AttachStatusOK {
		t.Fatalf("saved-mapping bare attach must not emit status OK; got OK=%v Status=%q", ack.OK, ack.Status)
	}
	if ack.Proposal == nil || ack.Proposal.Action != "pick_topic" {
		t.Fatalf("bare attach must return a pick_topic proposal; got Proposal=%+v", ack.Proposal)
	}
}
