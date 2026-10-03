package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// autoTestSend is one card or document the fake channel accepted.
type autoTestSend struct {
	id      int64
	args    c3types.ReplyArgs
	name    string // document only
	content []byte // document only
}

// autoTestChannel is permFakeChannel (callback answers, edits) plus the
// approval-card sends. Cards and documents share one message id sequence, as
// on Telegram.
type autoTestChannel struct {
	permFakeChannel
	sendMu      sync.Mutex
	sends       []autoTestSend
	nextID      int64
	sendErr     error           // fails every send
	documentErr error           // fails document sends only
	hold        chan struct{}   // when set, sends block until it closes or ctx ends
	entered     chan struct{}   // when set, signalled as a send starts
	returned    chan struct{}   // when set, signalled as a send returns
	docBarrier  *sync.WaitGroup // when set, each document send waits for the others
	afterSend   func(autoTestSend)
}

func signal(ch chan struct{}) {
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (f *autoTestChannel) send(ctx context.Context, sent autoTestSend, failure error) (int64, error) {
	signal(f.entered)
	defer signal(f.returned)
	if f.hold != nil {
		select {
		case <-f.hold:
		case <-ctx.Done():
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.sendMu.Lock()
	if failure == nil {
		failure = f.sendErr
	}
	if failure != nil {
		f.sendMu.Unlock()
		return 0, failure
	}
	f.nextID++
	sent.id = f.nextID
	f.sends = append(f.sends, sent)
	f.sendMu.Unlock()
	if f.afterSend != nil {
		f.afterSend(sent)
	}
	return sent.id, nil
}

func (f *autoTestChannel) SendApprovalCard(ctx context.Context, args c3types.ReplyArgs) (int64, error) {
	return f.send(ctx, autoTestSend{args: args}, nil)
}

func (f *autoTestChannel) SendApprovalDocument(ctx context.Context, args c3types.ReplyArgs, name string, content []byte) (int64, error) {
	id, err := f.send(ctx, autoTestSend{args: args, name: name, content: content}, f.documentErr)
	if f.docBarrier != nil {
		f.docBarrier.Done()
		f.docBarrier.Wait()
	}
	return id, err
}

func (f *autoTestChannel) cards() []autoTestSend {
	var cards []autoTestSend
	for _, sent := range f.sent() {
		if len(sent.args.Buttons) > 0 {
			cards = append(cards, sent)
		}
	}
	return cards
}

func (f *autoTestChannel) sent() []autoTestSend {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	return append([]autoTestSend(nil), f.sends...)
}

type autoFixture struct {
	t     *testing.T
	b     *Broker
	ch    *autoTestChannel
	owner *Stub
	route RouteKey
	req   ipc.AutoDeniedReq
}

// newAutoFixture: an enabled broker with one live Claude adapter, session
// "session-fixture", holding a confirmed Telegram topic route.
func newAutoFixture(t *testing.T) *autoFixture {
	t.Helper()
	mf := mfWithTelegram()
	mf.AddAllowedUser(testOperatorUID)
	mf.AutoModeApproval = &mappings.AutoModeApprovalConfig{Enabled: true}
	ch := &autoTestChannel{}
	b := newTestBroker(t, mf)
	b.chMu.Lock()
	b.channels["telegram"] = &channelRegistration{Channel: ch}
	b.chMu.Unlock()
	route := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 7}
	owner := b.Stubs.Register("claude", 4242, "/workspace", struct{}{})
	owner.SetStableSessionID("session-fixture")
	claimConfirmed(t, b, owner, route)
	req := ipc.AutoDeniedReq{
		Op:              ipc.OpAutoDenied,
		AutoCallContext: ipc.AutoCallContext{CLI: "claude", SessionID: "session-fixture", CWD: "/workspace", ToolName: "Bash"},
		ToolInput:       json.RawMessage(`{"command":"echo fixture"}`),
		Reason:          "[Code from External]",
		BudgetMS:        320000,
	}
	return &autoFixture{t: t, b: b, ch: ch, owner: owner, route: route, req: req}
}

func (f *autoFixture) setEnabled(isEnabled bool) {
	mf := f.b.Mappings().Clone()
	mf.AutoModeApproval.Enabled = isEnabled
	f.b.SetMappings(mf)
}

// open registers a request directly, with a waiter that has no connection.
func (f *autoFixture) open(t *testing.T, req ipc.AutoDeniedReq) *autoRequest {
	t.Helper()
	hash, err := ipc.ToolInputHash(req.ToolInput)
	if err != nil {
		t.Fatal(err)
	}
	r, isNew, refusal := f.b.openAutoRequest(req, hash, &autoWaiter{}, time.Now())
	if r == nil || !isNew {
		t.Fatalf("request not opened: %s", refusal)
	}
	return r
}

// armed opens a request and forces it armed, skipping the handoff.
func (f *autoFixture) armed(t *testing.T) *autoRequest {
	t.Helper()
	r := f.open(t, f.req)
	f.b.auto.mu.Lock()
	r.state = autoArmed
	r.expires = time.Now().Add(time.Minute)
	f.b.auto.mu.Unlock()
	return r
}

func (f *autoFixture) check() ipc.GrantCheckReq {
	hash, _ := ipc.ToolInputHash(f.req.ToolInput)
	return ipc.GrantCheckReq{Op: ipc.OpGrantCheck, AutoCallContext: f.req.AutoCallContext, InputHash: hash}
}

func (f *autoFixture) tap(r *autoRequest, verb string) {
	f.b.auto.mu.Lock()
	messageID := r.messageID
	f.b.auto.mu.Unlock()
	f.b.resolveAutoTap(f.route, &c3types.CallbackEvent{
		CallbackID: "callback", MessageID: messageID,
		Data: autoCallbackPrefix + verb + ":" + r.id, Actor: c3types.Sender{UserID: testOperatorUID},
	})
}

// startHook runs a denial hook connection; it returns the hook's side.
func (f *autoFixture) startHook(t *testing.T, req ipc.AutoDeniedReq) (*ipc.Conn, <-chan struct{}) {
	t.Helper()
	hookSide, brokerSide := net.Pipe()
	done := make(chan struct{})
	raw, _ := json.Marshal(req)
	go func() {
		defer close(done)
		defer brokerSide.Close()
		f.b.handleAutoDenied(ipc.NewConn(brokerSide), "claude", raw)
	}()
	t.Cleanup(func() {
		hookSide.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("hook handler leaked")
		}
	})
	return ipc.NewConn(hookSide), done
}

// postedCard waits for a request whose card message id is recorded.
func (f *autoFixture) postedCard(t *testing.T) *autoRequest {
	t.Helper()
	var found *autoRequest
	eventually(t, func() bool {
		f.b.auto.mu.Lock()
		defer f.b.auto.mu.Unlock()
		for _, r := range f.b.auto.byID {
			if r.messageID != 0 {
				found = r
				return true
			}
		}
		return false
	})
	return found
}

func (f *autoFixture) state(r *autoRequest) autoState {
	f.b.auto.mu.Lock()
	defer f.b.auto.mu.Unlock()
	return r.state
}

func (f *autoFixture) waiterCount(r *autoRequest) int {
	f.b.auto.mu.Lock()
	defer f.b.auto.mu.Unlock()
	return len(r.waiters)
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}

func readDecision(t *testing.T, hook *ipc.Conn) ipc.AutoDecisionMsg {
	t.Helper()
	frame, err := hook.ReadFrame()
	if err != nil {
		t.Fatalf("read decision: %v", err)
	}
	var decision ipc.AutoDecisionMsg
	if err := json.Unmarshal(frame, &decision); err != nil || decision.Op != ipc.OpAutoDecision {
		t.Fatalf("bad decision frame %s", frame)
	}
	return decision
}

func ack(t *testing.T, hook *ipc.Conn, r *autoRequest) {
	t.Helper()
	if err := hook.WriteJSON(ipc.AutoAckMsg{Op: ipc.OpAutoAck, RequestID: r.id}); err != nil {
		t.Fatal(err)
	}
}

// The full loop: a card, Allow, approved → ack → armed, then exactly one
// matching retry is allowed and the card says so without claiming execution.
func TestAutoHandoffArmsOnlyAfterAck(t *testing.T) {
	f := newAutoFixture(t)
	hook, done := f.startHook(t, f.req)
	r := f.postedCard(t)
	f.tap(r, "allow")
	if f.b.consumeAutoGrant(f.check()) {
		t.Fatal("the tap alone armed a grant")
	}
	if decision := readDecision(t, hook); decision.State != ipc.AutoDecisionApproved || decision.RequestID != r.id {
		t.Fatalf("want approved, got %+v", decision)
	}
	if f.b.consumeAutoGrant(f.check()) {
		t.Fatal("approved without an ack armed a grant")
	}
	ack(t, hook, r)
	if decision := readDecision(t, hook); decision.State != ipc.AutoDecisionArmed {
		t.Fatalf("want armed, got %+v", decision)
	}
	<-done
	if !f.b.consumeAutoGrant(f.check()) || f.b.consumeAutoGrant(f.check()) {
		t.Fatal("the handoff did not produce exactly one grant")
	}
	eventually(t, func() bool {
		edits := f.ch.editCallsSnapshot()
		return len(edits) > 0 && strings.Contains(edits[len(edits)-1].Text, "Retry authorized (execution unconfirmed)")
	})
	edit := f.ch.editCallsSnapshot()[len(f.ch.editCallsSnapshot())-1]
	if edit.Buttons == nil || len(edit.Buttons) != 0 || edit.MessageID != r.messageID || edit.Markup != c3types.MarkupNative {
		t.Fatalf("consumed card edit = %+v", edit)
	}
}

// A grant matches only the exact call in the exact context, once, before it
// expires, and only while the adapter it was issued for still holds the same
// claim.
func TestAutoGrantConsumedOnceInExactContext(t *testing.T) {
	cases := map[string]func(f *autoFixture, r *autoRequest, check *ipc.GrantCheckReq){
		"expired": func(f *autoFixture, r *autoRequest, _ *ipc.GrantCheckReq) {
			f.b.auto.mu.Lock()
			r.expires = time.Now().Add(-time.Second)
			f.b.auto.mu.Unlock()
		},
		"session":   func(_ *autoFixture, _ *autoRequest, check *ipc.GrantCheckReq) { check.SessionID = "other" },
		"sub-agent": func(_ *autoFixture, _ *autoRequest, check *ipc.GrantCheckReq) { check.AgentID = "agent-1" },
		"cwd":       func(_ *autoFixture, _ *autoRequest, check *ipc.GrantCheckReq) { check.CWD = "/elsewhere" },
		"tool":      func(_ *autoFixture, _ *autoRequest, check *ipc.GrantCheckReq) { check.ToolName = "Write" },
		"input": func(_ *autoFixture, _ *autoRequest, check *ipc.GrantCheckReq) {
			check.InputHash = strings.Repeat("a", 64)
		},
		"cli namespace": func(_ *autoFixture, _ *autoRequest, check *ipc.GrantCheckReq) { check.CLI = "codex" },
		"new adapter for the same session": func(f *autoFixture, _ *autoRequest, _ *ipc.GrantCheckReq) {
			replacement := f.b.Stubs.Register("claude", 4242, "/workspace", struct{}{})
			replacement.SetStableSessionID(f.req.SessionID)
			f.b.Stubs.Unregister(f.owner.ConnID)
			f.b.Routes.TransferAllByConnID(f.owner.ConnID, replacement)
			replacement.AddRoute(f.route)
			replacement.MarkRouteConfirmed(f.route)
		},
		"released and reclaimed": func(f *autoFixture, _ *autoRequest, _ *ipc.GrantCheckReq) {
			f.b.Routes.Release(f.route, f.owner.ConnID)
			claimConfirmed(f.t, f.b, f.owner, f.route)
		},
		"stolen and reclaimed": func(f *autoFixture, _ *autoRequest, _ *ipc.GrantCheckReq) {
			f.b.Routes.ForceReleaseKey(f.route)
			f.owner.ClearRouteIf(f.route)
			claimConfirmed(f.t, f.b, f.owner, f.route)
		},
		"session id switched": func(f *autoFixture, _ *autoRequest, _ *ipc.GrantCheckReq) { f.owner.SetStableSessionID("after-clear") },
		"disabled and re-enabled": func(f *autoFixture, _ *autoRequest, _ *ipc.GrantCheckReq) {
			f.setEnabled(false)
			f.setEnabled(true)
		},
		"adapter disconnected": func(f *autoFixture, _ *autoRequest, _ *ipc.GrantCheckReq) { f.owner.MarkDisconnected() },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newAutoFixture(t)
			r := f.armed(t)
			check := f.check()
			mutate(f, r, &check)
			if f.b.consumeAutoGrant(check) {
				t.Fatal("grant consumed outside its exact context")
			}
		})
	}
	t.Run("once", func(t *testing.T) {
		f := newAutoFixture(t)
		f.armed(t)
		if !f.b.consumeAutoGrant(f.check()) || f.b.consumeAutoGrant(f.check()) {
			t.Fatal("grant not consumed exactly once")
		}
	})
}

func TestAutoConcurrentConsumeIsAtomic(t *testing.T) {
	f := newAutoFixture(t)
	f.armed(t)
	var successes atomic.Int32
	var group sync.WaitGroup
	start := make(chan struct{})
	for range 64 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			if f.b.consumeAutoGrant(f.check()) {
				successes.Add(1)
			}
		}()
	}
	close(start)
	group.Wait()
	if successes.Load() != 1 {
		t.Fatalf("%d concurrent consumes succeeded, want 1", successes.Load())
	}
}

// A failed "armed" write arms nothing.
func TestAutoArmedWriteFailureArmsNothing(t *testing.T) {
	f := newAutoFixture(t)
	r := f.open(t, f.req)
	hookSide, brokerSide := net.Pipe()
	hookSide.Close()
	defer brokerSide.Close()
	waiter := &autoWaiter{conn: ipc.NewConn(brokerSide)}
	f.b.auto.mu.Lock()
	r.waiters = map[*autoWaiter]struct{}{waiter: {}}
	r.state = autoApproved
	f.b.auto.mu.Unlock()
	if !f.b.armAutoGrant(r, waiter) || f.b.consumeAutoGrant(f.check()) || f.state(r) != autoCancelled {
		t.Fatal("a failed armed write left a grant")
	}
}

// A hook that blocks on the armed write can't stall a config reload, and the
// reload stops the handoff with nothing armed.
func TestAutoSlowArmedWriteDoesNotBlockDisable(t *testing.T) {
	f := newAutoFixture(t)
	r := f.open(t, f.req)
	hookSide, brokerSide := net.Pipe() // nobody reads hookSide
	defer hookSide.Close()
	defer brokerSide.Close()
	waiter := &autoWaiter{conn: ipc.NewConn(brokerSide)}
	f.b.auto.mu.Lock()
	r.state = autoApproved
	r.waiters = map[*autoWaiter]struct{}{waiter: {}}
	f.b.auto.mu.Unlock()
	done := make(chan struct{})
	go func() { defer close(done); f.b.armAutoGrant(r, waiter) }()
	eventually(t, func() bool { f.b.auto.mu.Lock(); defer f.b.auto.mu.Unlock(); return r.isArming })
	if f.b.consumeAutoGrant(f.check()) {
		t.Fatal("grant consumable before the armed write completed")
	}
	disabled := make(chan struct{})
	go func() { f.setEnabled(false); close(disabled) }()
	select {
	case <-disabled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("a blocked handoff stalled the config reload")
	}
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond): // well under autoReplyTimeout: only cancellation ends it
		t.Fatal("disable did not stop the handoff")
	}
	if f.b.consumeAutoGrant(f.check()) {
		t.Fatal("the stopped handoff armed a grant")
	}
}

// Timeout, hook disconnect, disable and detach each end the request with no
// grant, release the hook, and make a later Allow inert.
func TestAutoNoGrantAfterTimeoutDisconnectOrInvalidation(t *testing.T) {
	names := []string{"timeout", "hook disconnect", "disable", "detach topic", "detach all",
		"stolen", "attach switch", "identity switch"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			f := newAutoFixture(t)
			hook, done := f.startHook(t, f.req)
			r := f.postedCard(t)
			switch name {
			case "timeout":
				f.b.auto.mu.Lock()
				r.deadline = time.Now().Add(-time.Second)
				f.b.auto.mu.Unlock()
				f.b.sweepAuto()
			case "hook disconnect":
				hook.Close()
			case "disable":
				f.setEnabled(false)
			case "detach topic", "detach all":
				adapterSide, brokerSide := net.Pipe()
				defer adapterSide.Close()
				defer brokerSide.Close()
				go func() { _, _ = ipc.NewConn(adapterSide).ReadFrame() }()
				release := ipc.ReleaseReq{Op: ipc.OpRelease}
				if name == "detach topic" {
					release.Target = "telegram"
				}
				raw, _ := json.Marshal(release)
				f.b.handleRelease(ipc.NewConn(brokerSide), f.owner, raw)
			case "stolen":
				thief := f.b.Stubs.Register("claude", 5151, "/elsewhere", struct{}{})
				if !f.b.tryClaim(nil, thief, f.route, "thief", true, true, false) {
					t.Fatal("steal refused")
				}
			case "attach switch":
				other := f.route
				other.TopicID++
				if !f.b.tryClaim(nil, f.owner, other, "other", false, true, false) {
					t.Fatal("switch refused")
				}
			case "identity switch":
				adapterSide, brokerSide := net.Pipe()
				defer adapterSide.Close()
				defer brokerSide.Close()
				go func() { _, _ = ipc.NewConn(adapterSide).ReadFrame() }()
				raw, _ := json.Marshal(ipc.RecoverSessionReq{Op: ipc.OpRecoverSession, StableSessionID: "after-clear"})
				f.b.handleRecoverSession(ipc.NewConn(brokerSide), f.owner, raw, nil)
			}
			eventually(t, func() bool { return !f.state(r).isLive() })
			if name != "timeout" && name != "hook disconnect" {
				if decision := readDecision(t, hook); decision.State != ipc.AutoDecisionNone {
					t.Fatalf("want none, got %+v", decision)
				}
			}
			f.tap(r, "allow")
			if f.b.consumeAutoGrant(f.check()) {
				t.Fatal("a late Allow produced a grant")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("the hook was not released")
			}
		})
	}
}

// Only an allowlisted operator's tap on the exact card of a pending request
// changes anything. Every tap is answered.
func TestAutoTapBinding(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(f *autoFixture, r *autoRequest, route *RouteKey, cb *c3types.CallbackEvent)
		want   autoState
	}{
		{"non-operator", func(_ *autoFixture, _ *autoRequest, _ *RouteKey, cb *c3types.CallbackEvent) { cb.Actor.UserID = 99 }, autoPending},
		{"other chat", func(_ *autoFixture, _ *autoRequest, route *RouteKey, _ *c3types.CallbackEvent) { route.ChatID-- }, autoPending},
		{"other topic", func(_ *autoFixture, _ *autoRequest, route *RouteKey, _ *c3types.CallbackEvent) { route.TopicID++ }, autoPending},
		{"other message", func(_ *autoFixture, _ *autoRequest, _ *RouteKey, cb *c3types.CallbackEvent) { cb.MessageID++ }, autoPending},
		{"before the card id is recorded", func(_ *autoFixture, r *autoRequest, _ *RouteKey, cb *c3types.CallbackEvent) {
			r.messageID, cb.MessageID = 0, 0
		}, autoPending},
		{"malformed", func(_ *autoFixture, r *autoRequest, _ *RouteKey, cb *c3types.CallbackEvent) {
			cb.Data = autoCallbackPrefix + "bogus:" + r.id
		}, autoPending},
		{"unknown id", func(_ *autoFixture, _ *autoRequest, _ *RouteKey, cb *c3types.CallbackEvent) {
			cb.Data = autoCallbackPrefix + "allow:" + strings.Repeat("0", 32)
		}, autoPending},
		{"stale", func(_ *autoFixture, r *autoRequest, _ *RouteKey, _ *c3types.CallbackEvent) {
			r.deadline = time.Now().Add(-time.Second)
		}, autoTimedOut},
		{"deny", func(_ *autoFixture, r *autoRequest, _ *RouteKey, cb *c3types.CallbackEvent) {
			cb.Data = autoCallbackPrefix + "deny:" + r.id
		}, autoDenied},
		{"duplicate", func(*autoFixture, *autoRequest, *RouteKey, *c3types.CallbackEvent) {}, autoApproved},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f := newAutoFixture(t)
			r := f.open(t, f.req)
			route := f.route
			cb := &c3types.CallbackEvent{CallbackID: "callback", MessageID: 10, Data: autoCallbackPrefix + "allow:" + r.id,
				Actor: c3types.Sender{UserID: testOperatorUID}}
			f.b.auto.mu.Lock()
			r.messageID = 10
			test.mutate(f, r, &route, cb)
			f.b.auto.mu.Unlock()
			if test.name == "duplicate" {
				f.b.resolveAutoTap(route, cb) // Allow
				cb.Data = autoCallbackPrefix + "deny:" + r.id
			}
			before := len(f.ch.answersSnapshot())
			f.b.resolveAutoTap(route, cb)
			if state := f.state(r); state != test.want {
				t.Fatalf("state %s, want %s", state, test.want)
			}
			if len(f.ch.answersSnapshot()) != before+1 {
				t.Fatal("tap not answered exactly once")
			}
			if f.b.consumeAutoGrant(f.check()) {
				t.Fatal("a tap alone armed a grant")
			}
			if answers := f.ch.answersSnapshot(); test.name == "duplicate" && answers[len(answers)-1].Text != autoAnswerDecidedText {
				t.Fatalf("Deny after Allow answered %q", answers[len(answers)-1].Text)
			}
		})
	}
	t.Run("replay after the request ended", func(t *testing.T) {
		f := newAutoFixture(t)
		r := f.armed(t)
		f.b.auto.mu.Lock()
		r.messageID = 10
		f.b.auto.mu.Unlock()
		if !f.b.consumeAutoGrant(f.check()) {
			t.Fatal("grant not consumable")
		}
		f.tap(r, "allow")
		if answers := f.ch.answersSnapshot(); answers[len(answers)-1].Text != autoAnswerGoneText {
			t.Fatalf("replayed tap answered %q", answers[len(answers)-1].Text)
		}
	})
}

// A second denial of the same call joins the live request: one card, the same
// outcome, and one waiter leaving doesn't cancel it.
func TestAutoDeduplicatedWaiters(t *testing.T) {
	f := newAutoFixture(t)
	first, firstDone := f.startHook(t, f.req)
	r := f.postedCard(t)
	second, secondDone := f.startHook(t, f.req)
	eventually(t, func() bool { return f.waiterCount(r) == 2 })
	if len(f.ch.sent()) != 1 {
		t.Fatal("a duplicate denial sent another card")
	}
	first.Close()
	<-firstDone
	if f.state(r) != autoPending {
		t.Fatal("one waiter leaving cancelled the request")
	}
	f.tap(r, "allow")
	if readDecision(t, second).State != ipc.AutoDecisionApproved {
		t.Fatal("remaining waiter not approved")
	}
	ack(t, second, r)
	if readDecision(t, second).State != ipc.AutoDecisionArmed {
		t.Fatal("remaining waiter not armed")
	}
	<-secondDone
	if !f.b.consumeAutoGrant(f.check()) {
		t.Fatal("deduplicated grant not consumable")
	}
}

// Every waiter connected when the grant is armed is told "armed", even after
// the grant is consumed; there is still only one grant.
func TestAutoAllLiveWaitersToldArmed(t *testing.T) {
	f := newAutoFixture(t)
	first, firstDone := f.startHook(t, f.req)
	r := f.postedCard(t)
	second, secondDone := f.startHook(t, f.req)
	eventually(t, func() bool { return f.waiterCount(r) == 2 })
	f.tap(r, "allow")
	if readDecision(t, first).State != ipc.AutoDecisionApproved || readDecision(t, second).State != ipc.AutoDecisionApproved {
		t.Fatal("waiters not approved")
	}
	ack(t, first, r)
	if readDecision(t, first).State != ipc.AutoDecisionArmed {
		t.Fatal("first waiter not armed")
	}
	<-firstDone
	if !f.b.consumeAutoGrant(f.check()) {
		t.Fatal("grant not consumable")
	}
	if readDecision(t, second).State != ipc.AutoDecisionArmed {
		t.Fatal("second waiter not told armed")
	}
	<-secondDone
	if f.b.consumeAutoGrant(f.check()) {
		t.Fatal("second waiter produced another grant")
	}
}

// A hook that dies after reading "armed" leaves the approved grant usable
// (the brief's accepted residual).
func TestAutoHookExitAfterArmedKeepsGrant(t *testing.T) {
	f := newAutoFixture(t)
	hook, done := f.startHook(t, f.req)
	r := f.postedCard(t)
	f.tap(r, "allow")
	readDecision(t, hook)
	ack(t, hook, r)
	if readDecision(t, hook).State != ipc.AutoDecisionArmed {
		t.Fatal("not armed")
	}
	hook.Close()
	<-done
	if !f.b.consumeAutoGrant(f.check()) {
		t.Fatal("hook exit revoked the armed grant")
	}
}

// An ack that is early, for another request, of the wrong op or malformed arms
// nothing.
func TestAutoInvalidAckNeverArms(t *testing.T) {
	for _, name := range []string{"before approval", "wrong request", "wrong op", "malformed"} {
		t.Run(name, func(t *testing.T) {
			f := newAutoFixture(t)
			hook, done := f.startHook(t, f.req)
			r := f.postedCard(t)
			if name != "before approval" {
				f.tap(r, "allow")
				if readDecision(t, hook).State != ipc.AutoDecisionApproved {
					t.Fatal("missing approved")
				}
			}
			var frame any = ipc.AutoAckMsg{Op: ipc.OpAutoAck, RequestID: r.id}
			switch name {
			case "wrong request":
				frame = ipc.AutoAckMsg{Op: ipc.OpAutoAck, RequestID: "other"}
			case "wrong op":
				frame = ipc.AutoAckMsg{Op: ipc.OpBye, RequestID: r.id}
			case "malformed":
				frame = map[string]any{"op": ipc.OpAutoAck, "request_id": 3}
			}
			if err := hook.WriteJSON(frame); err != nil {
				t.Fatal(err)
			}
			if reply, err := hook.ReadFrame(); err == nil && strings.Contains(string(reply), ipc.AutoDecisionArmed) {
				t.Fatal("an invalid ack was answered armed")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("an invalid ack left the hook waiting")
			}
			if f.b.consumeAutoGrant(f.check()) || f.state(r) != autoCancelled {
				t.Fatal("an invalid ack armed a grant")
			}
		})
	}
}

// A denial of a call whose grant is already armed gets no card and no second
// grant; the armed one is untouched.
func TestAutoDenialWhileArmedGetsNoCard(t *testing.T) {
	f := newAutoFixture(t)
	f.armed(t)
	hook, _ := f.startHook(t, f.req)
	if readDecision(t, hook).State != ipc.AutoDecisionNone || len(f.ch.sent()) != 0 {
		t.Fatal("an armed call got another card or retry")
	}
	if !f.b.consumeAutoGrant(f.check()) || f.b.consumeAutoGrant(f.check()) {
		t.Fatal("the duplicate denial changed the armed grant")
	}
}

// observedClassifierBlocks are the PermissionDenied reasons recorded for
// auto-mode classifier blocks. They are the fixtures the allowlist must match
// exactly: no more, no fewer.
var observedClassifierBlocks = []string{
	"[Code from External]",
	"[Auto-Mode Bypass]",
	"[Credential Exploration]",
	"[Git Destructive]",
}

// Each observed classifier-block form gets a card, and the allowlist holds
// exactly those forms.
func TestAutoObservedClassifierBlocksGetCards(t *testing.T) {
	if got := autoClassifierBlockReasons["claude"]; !slices.Equal(got, observedClassifierBlocks) {
		t.Fatalf("allowlist %q differs from the observed fixtures %q", got, observedClassifierBlocks)
	}
	if len(autoClassifierBlockReasons) != 1 {
		t.Fatalf("allowlist covers CLIs without observed fixtures: %v", autoClassifierBlockReasons)
	}
	for _, reason := range observedClassifierBlocks {
		f := newAutoFixture(t)
		req := f.req
		req.Reason = reason
		f.startHook(t, req)
		f.postedCard(t)
	}
	for _, near := range []string{"[git destructive]", "[Git Destructive] ", "Git Destructive", "Classifier unavailable"} {
		if isClassifierBlock("claude", near) {
			t.Fatalf("%q accepted", near)
		}
	}
}

// Every path that must not produce a card answers none, sends nothing and arms
// nothing.
func TestAutoNoCardPaths(t *testing.T) {
	cases := map[string]func(f *autoFixture, req *ipc.AutoDeniedReq){
		"feature off":          func(f *autoFixture, _ *ipc.AutoDeniedReq) { f.setEnabled(false) },
		"no verdict":           func(_ *autoFixture, req *ipc.AutoDeniedReq) { req.Reason = "Auto mode classifier unavailable" },
		"unknown form":         func(_ *autoFixture, req *ipc.AutoDeniedReq) { req.Reason = "[Some New Block]" },
		"near miss":            func(_ *autoFixture, req *ipc.AutoDeniedReq) { req.Reason = "[Code from External] " },
		"empty reason":         func(_ *autoFixture, req *ipc.AutoDeniedReq) { req.Reason = "" },
		"unregistered CLI":     func(_ *autoFixture, req *ipc.AutoDeniedReq) { req.CLI = "codex" },
		"unknown session":      func(_ *autoFixture, req *ipc.AutoDeniedReq) { req.SessionID = "unknown" },
		"budget nearly spent":  func(_ *autoFixture, req *ipc.AutoDeniedReq) { req.BudgetMS = 5000 },
		"duplicate input keys": func(_ *autoFixture, req *ipc.AutoDeniedReq) { req.ToolInput = json.RawMessage(`{"a":1,"a":2}`) },
		"missing cwd":          func(_ *autoFixture, req *ipc.AutoDeniedReq) { req.CWD = "" },
		"card send fails":      func(f *autoFixture, _ *ipc.AutoDeniedReq) { f.ch.sendErr = errors.New("telegram down") },
		"two adapters for the session": func(f *autoFixture, _ *ipc.AutoDeniedReq) {
			other := f.b.Stubs.Register("claude", 4243, "/workspace", struct{}{})
			other.SetStableSessionID(f.req.SessionID)
		},
		"two Telegram routes, output on web": func(f *autoFixture, _ *ipc.AutoDeniedReq) {
			second := f.route
			second.TopicID++
			claimConfirmed(f.t, f.b, f.owner, second)
			web := RouteKey{Channel: "web", ChatID: 100}
			claimConfirmed(f.t, f.b, f.owner, web)
			f.owner.SetOutputRoute(web)
		},
		"web only": func(f *autoFixture, _ *ipc.AutoDeniedReq) {
			f.b.Routes.Release(f.route, f.owner.ConnID)
			f.owner.ClearRoutes()
			claimConfirmed(f.t, f.b, f.owner, RouteKey{Channel: "web", ChatID: 100})
		},
		"unconfirmed claim": func(f *autoFixture, _ *ipc.AutoDeniedReq) {
			f.owner.ClearRoutes()
			f.owner.AddRoute(f.route)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newAutoFixture(t)
			req := f.req
			mutate(f, &req)
			hook, _ := f.startHook(t, req)
			if decision := readDecision(t, hook); decision.State != ipc.AutoDecisionNone {
				t.Fatalf("got %+v", decision)
			}
			for _, sent := range f.ch.sent() {
				if len(sent.args.Buttons) > 0 {
					t.Fatal("a card was sent")
				}
			}
			if f.b.consumeAutoGrant(f.check()) {
				t.Fatal("a grant was armed")
			}
		})
	}
	t.Run("malformed frame", func(t *testing.T) {
		f := newAutoFixture(t)
		hookSide, brokerSide := net.Pipe()
		defer hookSide.Close()
		go func() {
			defer brokerSide.Close()
			f.b.handleAutoDenied(ipc.NewConn(brokerSide), "claude", []byte(`{"tool_input":`))
		}()
		if readDecision(t, ipc.NewConn(hookSide)).State != ipc.AutoDecisionNone {
			t.Fatal("malformed frame not answered none")
		}
	})
}

// A denial that gets no card is audit-logged with the classifier's reason
// (escaped and capped), so the allowlist can be extended. Feature off logs
// nothing.
func TestAutoUnknownReasonIsAudited(t *testing.T) {
	f := newAutoFixture(t)
	req := f.req
	req.Reason = "[New Block]\u202e" + strings.Repeat("r", 400)
	line := captureLog(t, func() {
		hook, _ := f.startHook(t, req)
		readDecision(t, hook)
	})
	if !strings.Contains(line, `classifier_reason="[New Block]\u202e`) || !strings.Contains(line, `cause="unknown reason"`) {
		t.Fatalf("unknown reason not audited: %s", line)
	}
	if strings.Contains(line, strings.Repeat("r", 250)) || strings.Contains(line, "echo fixture") {
		t.Fatal("reason not capped, or input logged")
	}
	raw := []byte(`{"op":"auto_denied","cli":"claude","session_id":"session-fixture","cwd":"/workspace",` +
		`"tool_name":"Bash","reason":"[Code from External]","budget_ms":320000,` +
		`"tool_input":{"secret-fixture":1,"secret-fixture":2}}`)
	line = captureLog(t, func() {
		hookSide, brokerSide := net.Pipe()
		defer hookSide.Close()
		go func() { defer brokerSide.Close(); f.b.handleAutoDenied(ipc.NewConn(brokerSide), "claude", raw) }()
		readDecision(t, ipc.NewConn(hookSide))
	})
	if !strings.Contains(line, `cause="malformed frame"`) || strings.Contains(line, "secret-fixture") {
		t.Fatalf("rejected input not audited by a fixed cause: %s", line)
	}
	f.setEnabled(false)
	quiet := captureLog(t, func() {
		hook, _ := f.startHook(t, req)
		readDecision(t, hook)
	})
	if strings.Contains(quiet, "no-card") {
		t.Fatal("feature off is not silent")
	}
}

// An overflowing input goes first as a document; each card replies to its own
// document even when both documents are sent before either card.
func TestAutoOverflowCardRepliesToItsFile(t *testing.T) {
	f := newAutoFixture(t)
	letters := map[string]string{}
	var requests []*autoRequest
	var inputs []json.RawMessage
	for _, letter := range []string{"Q", "Z"} {
		req := f.req
		req.ToolInput = json.RawMessage(`{"content":"` + strings.Repeat(letter, 5000) + `"}`)
		r := f.open(t, req)
		letters[autoShortID(r.id)] = letter
		requests, inputs = append(requests, r), append(inputs, req.ToolInput)
	}
	barrier := &sync.WaitGroup{}
	barrier.Add(2)
	f.ch.docBarrier = barrier
	var group sync.WaitGroup
	for index := range requests {
		group.Add(1)
		go func() { defer group.Done(); f.b.sendAutoCard(requests[index], inputs[index], f.req.Reason) }()
	}
	group.Wait()
	sent := f.ch.sent()
	if len(sent) != 4 || sent[0].content == nil || sent[1].content == nil || sent[2].content != nil || sent[3].content != nil {
		t.Fatal("sends did not interleave as document, document, card, card")
	}
	documents := map[int64]autoTestSend{sent[0].id: sent[0], sent[1].id: sent[1]}
	for _, card := range sent[2:] {
		if card.args.ReplyTo == nil {
			t.Fatal("overflow card is not a reply")
		}
		document, ok := documents[*card.args.ReplyTo]
		if !ok {
			t.Fatal("card replies to something other than a document")
		}
		shortID := strings.TrimSuffix(strings.TrimPrefix(document.name, "c3-approval-"), ".txt")
		if !strings.Contains(card.args.Text, shortID) || !strings.Contains(document.args.Text, shortID) {
			t.Fatal("card bound to another request's document")
		}
		letter := letters[shortID]
		if !strings.Contains(string(document.content), strings.Repeat(letter, autoWrapRunes)) {
			t.Fatal("document does not hold its request's input")
		}
	}
}

// A failed attachment stops the card: no card points at a file that isn't
// there.
func TestAutoFailedAttachmentSendsNoCard(t *testing.T) {
	f := newAutoFixture(t)
	req := f.req
	req.ToolInput = json.RawMessage(`{"content":"` + strings.Repeat("a", 5000) + `"}`)
	r := f.open(t, req)
	f.ch.documentErr = errors.New("telegram: approval attachment: Bad Request: chat not found")
	output := captureLog(t, func() { f.b.sendAutoCard(r, req.ToolInput, req.Reason) })
	if len(f.ch.cards()) != 0 {
		t.Fatal("a card was sent after its attachment failed")
	}
	if f.state(r) != autoCancelled || f.b.consumeAutoGrant(f.check()) {
		t.Fatal("attachment failure left a live request")
	}
	if !strings.Contains(output, `detail="attachment: telegram: approval attachment: Bad Request: chat not found"`) {
		t.Fatalf("attachment failure not audited: %s", output)
	}
}

// A card or attachment that reaches Telegram is always audited with its message
// id, even when the request ended while it was in flight; an attachment left
// without a card is recorded as orphaned.
func TestAutoPostedMessagesAreAlwaysAudited(t *testing.T) {
	overflowing := json.RawMessage(`{"content":"` + strings.Repeat("a", 5000) + `"}`)
	t.Run("card after the request ended", func(t *testing.T) {
		f := newAutoFixture(t)
		req := f.req
		req.ToolInput = overflowing
		r := f.open(t, req)
		f.ch.afterSend = func(sent autoTestSend) {
			if sent.content == nil {
				f.b.cancelAuto(nil, nil, "test")
			}
		}
		output := captureLog(t, func() { f.b.sendAutoCard(r, req.ToolInput, req.Reason) })
		for _, want := range []string{`attachment-sent id=` + r.id, `detail="msg=1"`, `card-sent id=` + r.id, `detail="msg=2 state=cancelled"`} {
			if !strings.Contains(output, want) {
				t.Fatalf("audit missing %q:\n%s", want, output)
			}
		}
	})
	t.Run("orphaned attachment", func(t *testing.T) {
		f := newAutoFixture(t)
		req := f.req
		req.ToolInput = overflowing
		r := f.open(t, req)
		f.ch.afterSend = func(autoTestSend) { f.b.cancelAuto(nil, nil, "test") }
		output := captureLog(t, func() { f.b.sendAutoCard(r, req.ToolInput, req.Reason) })
		if !strings.Contains(output, "attachment-orphaned id="+r.id) || len(f.ch.cards()) != 0 {
			t.Fatalf("orphaned attachment not audited, or a card was sent:\n%s", output)
		}
	})
}

// A refused tap is audited with the request's tool, hash and the tapped verb.
func TestAutoRefusedTapAuditFields(t *testing.T) {
	f := newAutoFixture(t)
	r := f.open(t, f.req)
	output := captureLog(t, func() {
		f.b.resolveAutoTap(f.route, &c3types.CallbackEvent{CallbackID: "callback", MessageID: 10,
			Data: autoCallbackPrefix + "deny:" + r.id, Actor: c3types.Sender{UserID: 99}})
	})
	for _, want := range []string{"tap-deny-refused", "id=" + r.id, `tool="Bash"`, "hash=" + r.key.inputHash, "actor=99", "not an operator"} {
		if !strings.Contains(output, want) {
			t.Fatalf("refused tap audit missing %q: %s", want, output)
		}
	}
}

// If the attachment or the card can't be sent, there is no card and no grant,
// and the audit line keeps the channel's error.
func TestAutoCardSendFailureIsAuditedAndCancels(t *testing.T) {
	f := newAutoFixture(t)
	req := f.req
	req.ToolInput = json.RawMessage(`{"content":"` + strings.Repeat("a", 5000) + `"}`)
	r := f.open(t, req)
	f.ch.sendErr = errors.New("telegram: approval attachment: unable to sendDocument: Bad Request: chat not found")
	output := captureLog(t, func() { f.b.sendAutoCard(r, req.ToolInput, req.Reason) })
	if f.state(r) != autoCancelled || f.b.consumeAutoGrant(f.check()) {
		t.Fatal("attachment failure left a live request")
	}
	if !strings.Contains(output, "card-failed") || !strings.Contains(output, "chat not found") || strings.Contains(output, "aaaa") {
		t.Fatalf("error not audited, or input logged: %s", output)
	}
}

// The request deadline is min(wait_seconds, budget − 5 s), and the route's
// undecided-card limit holds.
func TestAutoDeadlineFromBudgetAndRouteLimit(t *testing.T) {
	f := newAutoFixture(t)
	mf := f.b.Mappings().Clone()
	mf.AutoModeApproval.MaxPending = 1
	mf.AutoModeApproval.WaitSeconds = 30
	f.b.SetMappings(mf)
	req := f.req
	req.BudgetMS = 10000
	r := f.open(t, req)
	if remaining := time.Until(r.deadline); remaining > 5*time.Second || remaining < 4*time.Second {
		t.Fatalf("deadline in %v, want about 5 s", remaining)
	}
	next := f.req
	next.CWD = "/other"
	hash, _ := ipc.ToolInputHash(next.ToolInput)
	if other, _, refusal := f.b.openAutoRequest(next, hash, &autoWaiter{}, time.Now()); other != nil || !strings.Contains(refusal, "max_pending") {
		t.Fatalf("limit bypassed (%s)", refusal)
	}
}

// wait_seconds caps the deadline; a request that has ended sends no card.
func TestAutoWaitSecondsCapsDeadline(t *testing.T) {
	f := newAutoFixture(t)
	mf := f.b.Mappings().Clone()
	mf.AutoModeApproval.WaitSeconds = 30
	f.b.SetMappings(mf)
	r := f.open(t, f.req)
	if remaining := time.Until(r.deadline); remaining > 30*time.Second || remaining < 29*time.Second {
		t.Fatalf("deadline in %v, want about 30 s", remaining)
	}
	f.b.cancelAuto(nil, nil, "test")
	f.b.sendAutoCard(r, f.req.ToolInput, f.req.Reason)
	if len(f.ch.sent()) != 0 {
		t.Fatal("an ended request sent a card")
	}
}

// A send that outlives the hook budget is abandoned: no card, no grant.
func TestAutoSlowCardSendBoundedByBudget(t *testing.T) {
	f := newAutoFixture(t)
	f.ch.hold = make(chan struct{})
	f.ch.returned = make(chan struct{}, 1)
	req := f.req
	req.BudgetMS = 5100
	hook, done := f.startHook(t, req)
	if frame, err := hook.ReadFrame(); err == nil && strings.Contains(string(frame), ipc.AutoDecisionArmed) {
		t.Fatal("slow send authorized a retry")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("slow send held the hook past its deadline")
	}
	close(f.ch.hold) // a send that ignored the deadline would now complete
	select {
	case <-f.ch.returned:
	case <-time.After(time.Second):
		t.Fatal("the held send never returned")
	}
	if len(f.ch.sent()) != 0 || f.b.consumeAutoGrant(f.check()) {
		t.Fatal("expired send produced a card or grant")
	}
}

// Ending a request aborts its in-flight send, for the card and the document.
func TestAutoCancelAbortsInFlightSend(t *testing.T) {
	for _, isOverflow := range []bool{false, true} {
		f := newAutoFixture(t)
		f.ch.hold = make(chan struct{})
		f.ch.entered = make(chan struct{}, 1)
		req := f.req
		if isOverflow {
			req.ToolInput = json.RawMessage(`{"content":"` + strings.Repeat("a", 5000) + `"}`)
		}
		r := f.open(t, req)
		done := make(chan struct{})
		go func() { defer close(done); f.b.sendAutoCard(r, req.ToolInput, req.Reason) }()
		<-f.ch.entered
		f.b.cancelAuto(nil, nil, "test")
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("overflow=%t: the send outlived its request", isOverflow)
		}
		close(f.ch.hold)
		if len(f.ch.sent()) != 0 || f.state(r) != autoCancelled {
			t.Fatalf("overflow=%t: send completed after cancellation", isOverflow)
		}
	}
}

// Allow, then the deadline passes, then the ack arrives: nothing is armed and
// the hook gets no frame after the deadline.
func TestAutoLateAckAfterDeadlineArmsNothing(t *testing.T) {
	f := newAutoFixture(t)
	hook, done := f.startHook(t, f.req)
	r := f.postedCard(t)
	f.tap(r, "allow")
	if readDecision(t, hook).State != ipc.AutoDecisionApproved {
		t.Fatal("missing approved")
	}
	f.b.auto.mu.Lock()
	r.deadline = time.Now().Add(-time.Millisecond)
	f.b.auto.mu.Unlock()
	ack(t, hook, r)
	if frame, err := hook.ReadFrame(); err == nil {
		t.Fatalf("got a frame after the deadline: %s", frame)
	}
	<-done
	if f.state(r) != autoTimedOut || f.b.consumeAutoGrant(f.check()) {
		t.Fatal("a late ack armed a grant")
	}
}

// The real arming path caps the grant at 120 s, whatever is configured.
func TestAutoGrantLifetimeCappedOnArming(t *testing.T) {
	for configured, want := range map[int]time.Duration{30: 30 * time.Second, 900: 120 * time.Second, 0: 120 * time.Second} {
		f := newAutoFixture(t)
		mf := f.b.Mappings().Clone()
		mf.AutoModeApproval.GrantTTLSeconds = configured
		f.b.SetMappings(mf)
		hook, done := f.startHook(t, f.req)
		r := f.postedCard(t)
		f.tap(r, "allow")
		readDecision(t, hook)
		ack(t, hook, r)
		if readDecision(t, hook).State != ipc.AutoDecisionArmed {
			t.Fatal("not armed")
		}
		<-done
		f.b.auto.mu.Lock()
		remaining := time.Until(r.expires)
		f.b.auto.mu.Unlock()
		if remaining > want || remaining < want-2*time.Second {
			t.Fatalf("grant_ttl_seconds=%d: grant lives %v, want %v", configured, remaining, want)
		}
	}
}

// Request ids are 128-bit random hex, and a fresh broker never honours a tap
// for an earlier broker's card, even when the message ids coincide.
func TestAutoRequestIDsAndRestart(t *testing.T) {
	seen := map[string]bool{}
	for range 10000 {
		id, err := newAutoRequestID()
		if err != nil || len(id) != 32 || strings.Trim(id, "0123456789abcdef") != "" || seen[id] {
			t.Fatalf("bad or repeated id %q (%v)", id, err)
		}
		seen[id] = true
	}
	before := newAutoFixture(t)
	old := before.open(t, before.req)
	after := newAutoFixture(t) // a restarted broker: fresh registry, fresh channel
	current := after.open(t, after.req)
	for _, r := range []*autoRequest{old, current} {
		r.messageID = 1
	}
	if old.id == current.id {
		t.Fatal("a restarted broker reused a request id")
	}
	after.b.resolveAutoTap(after.route, &c3types.CallbackEvent{CallbackID: "callback", MessageID: 1,
		Data: autoCallbackPrefix + "allow:" + old.id, Actor: c3types.Sender{UserID: testOperatorUID}})
	if answers := after.ch.answersSnapshot(); answers[len(answers)-1].Text != autoAnswerGoneText || after.state(current) != autoPending {
		t.Fatal("a tap on an earlier broker's card changed a request")
	}
}

// A real adapter connection closing cancels its pending request at once, not
// at the next sweep.
func TestAutoAdapterDisconnectCancelsThroughHandleConn(t *testing.T) {
	f := newAutoFixture(t)
	f.b.Stubs.Unregister(f.owner.ConnID)
	f.b.Routes.Release(f.route, f.owner.ConnID)
	adapter, closeAdapter := peerPair(t, f.b)
	defer closeAdapter()
	hello := ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 4242, CWD: "/workspace", ProtocolVersion: ipc.ProtocolVersion}
	if err := adapter.WriteJSON(hello); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			if _, err := adapter.ReadFrame(); err != nil {
				return
			}
		}
	}()
	stubs := f.b.Stubs.Snapshot()
	if len(stubs) != 1 {
		t.Fatalf("%d stubs", len(stubs))
	}
	f.owner = stubs[0]
	f.owner.SetStableSessionID("session-fixture")
	claimConfirmed(t, f.b, f.owner, f.route)
	hook, _ := f.startHook(t, f.req)
	r := f.postedCard(t)
	closeAdapter()
	if readDecision(t, hook).State != ipc.AutoDecisionNone {
		t.Fatal("hook not answered none")
	}
	if f.state(r) != autoCancelled {
		t.Fatalf("state %s, want cancelled", f.state(r))
	}
}

// The card goes to the output route when it is Telegram.
func TestAutoCardGoesToOutputRoute(t *testing.T) {
	f := newAutoFixture(t)
	for topic := int64(8); topic <= 9; topic++ {
		route := f.route
		route.TopicID = topic
		claimConfirmed(f.t, f.b, f.owner, route)
		f.owner.SetOutputRoute(route)
	}
	if r := f.open(t, f.req); r.route.TopicID != 9 {
		t.Fatalf("card route %v, want the output route", r.route)
	}
}

// Terminal states clear the keyboard, keep the card's content and never claim
// execution.
func TestAutoCardEditsShowOutcome(t *testing.T) {
	for state, status := range autoCardStatus {
		t.Run(string(state), func(t *testing.T) {
			f := newAutoFixture(t)
			r := f.open(t, f.req)
			f.b.auto.mu.Lock()
			r.messageID, r.cardText = 10, "card body"
			f.b.setAutoStateLocked(r, state, "")
			f.b.auto.mu.Unlock()
			eventually(t, func() bool { return len(f.ch.editCallsSnapshot()) > 0 })
			edit := f.ch.editCallsSnapshot()[0]
			if edit.Buttons == nil || len(edit.Buttons) != 0 || edit.MessageID != 10 ||
				!strings.HasPrefix(edit.Text, "card body") || !strings.Contains(edit.Text, status) {
				t.Fatalf("edit = %+v", edit)
			}
			if strings.Contains(strings.ToLower(edit.Text), "succeeded") || strings.Contains(edit.Text, " ran") {
				t.Fatal("card claims execution")
			}
		})
	}
}

// Approval taps are never delivered to the agent, whatever their content.
func TestAutoCallbacksNeverReachAgent(t *testing.T) {
	for _, data := range []string{autoCallbackPrefix + "broken", autoCallbackPrefix + "allow:" + strings.Repeat("0", 32)} {
		f := newAutoFixture(t)
		agentSide, brokerSide := net.Pipe()
		f.owner.connMu.Lock()
		f.owner.Conn = ipc.NewConn(brokerSide)
		f.owner.connMu.Unlock()
		_ = agentSide.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		worker := &RouteWorker{broker: f.b, key: f.route}
		done := make(chan struct{})
		go func() {
			defer close(done)
			worker.flushEvent(context.Background(), &c3types.Inbound{Channel: "telegram", ChatID: f.route.ChatID,
				Kind: c3types.InboundCallback, Event: &c3types.InboundEvent{Callback: &c3types.CallbackEvent{Data: data,
					Actor: c3types.Sender{UserID: testOperatorUID}}}})
		}()
		if frame, err := ipc.NewConn(agentSide).ReadFrame(); err == nil {
			t.Fatalf("approval tap reached the agent: %s", frame)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("approval tap blocked in delivery")
		}
		agentSide.Close()
		brokerSide.Close()
	}
}

// A hook connection is served without becoming an adapter or touching the
// session's own registration.
func TestAutoHookClientNeverRegisters(t *testing.T) {
	f := newAutoFixture(t)
	hookSide, brokerSide := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); f.b.HandleConn(brokerSide) }()
	hook := ipc.NewConn(hookSide)
	defer hook.Close()
	hello := ipc.HookHelloMsg{Op: ipc.OpHookHello, CLI: "claude", PID: 4242, ProtocolVersion: ipc.ProtocolVersion}
	if err := hook.WriteJSON(hello); err != nil {
		t.Fatal(err)
	}
	if _, err := hook.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if len(f.b.Stubs.Snapshot()) != 1 {
		t.Fatal("the hook registered as an adapter")
	}
	if err := hook.WriteJSON(f.check()); err != nil {
		t.Fatal(err)
	}
	frame, err := hook.ReadFrame()
	var result ipc.GrantCheckResp
	if err != nil || json.Unmarshal(frame, &result) != nil || result.Op != ipc.OpGrantCheckResult || result.Allow {
		t.Fatalf("grant check answered %s (%v)", frame, err)
	}
	<-done
	if len(f.b.Stubs.Snapshot()) != 1 || !f.owner.IsConnected() {
		t.Fatal("the hook displaced the adapter")
	}
}

// Claude Code can deny a retry it allowed through the hook. That denial (same
// call, same tool_use_id as the consuming check) gets no new card and no
// grant; the original card says the retry was blocked. A later identical call
// with a new tool_use_id is a fresh request, and the record lapses with the
// grant lifetime.
func TestAutoVetoAfterAllow(t *testing.T) {
	f := newAutoFixture(t)
	r := f.armed(t)
	f.b.auto.mu.Lock()
	r.messageID, r.cardText = 10, "card body"
	f.b.auto.mu.Unlock()
	check := f.check()
	check.ToolUseID = "toolu_retry"
	if !f.b.consumeAutoGrant(check) {
		t.Fatal("grant not consumed")
	}
	f.b.markAutoDelivered(autoConsumedCall{key: r.key, toolUseID: "toolu_retry"})
	vetoed := f.req
	vetoed.ToolUseID = "toolu_retry"
	var decision ipc.AutoDecisionMsg
	output := captureLog(t, func() {
		hook, _ := f.startHook(t, vetoed)
		decision = readDecision(t, hook)
	})
	if decision.State != ipc.AutoDecisionNone || len(f.ch.sent()) != 0 {
		t.Fatalf("vetoed retry got %+v and %d sends", decision, len(f.ch.sent()))
	}
	if !strings.Contains(output, "vetoed-after-allow id="+r.id) || f.state(r) != autoVetoed {
		t.Fatalf("veto not recorded (state %s): %s", f.state(r), output)
	}
	eventually(t, func() bool {
		edits := f.ch.editCallsSnapshot()
		return len(edits) > 0 && strings.Contains(edits[len(edits)-1].Text, "Claude Code still blocked the retry; it did not run")
	})

	fresh := f.req
	fresh.ToolUseID = "toolu_later"
	f.startHook(t, fresh)
	f.postedCard(t)
}

func TestAutoVetoRecordIsExactAndLapses(t *testing.T) {
	for _, name := range []string{"other tool_use_id", "no tool_use_id", "lapsed"} {
		t.Run(name, func(t *testing.T) {
			f := newAutoFixture(t)
			r := f.armed(t)
			check := f.check()
			check.ToolUseID = "toolu_retry"
			if !f.b.consumeAutoGrant(check) {
				t.Fatal("grant not consumed")
			}
			f.b.markAutoDelivered(autoConsumedCall{key: r.key, toolUseID: "toolu_retry"})
			denial := f.req
			denial.ToolUseID = "toolu_retry"
			switch name {
			case "other tool_use_id":
				denial.ToolUseID = "toolu_other"
			case "no tool_use_id":
				denial.ToolUseID = ""
			case "lapsed":
				f.b.auto.mu.Lock()
				for call, record := range f.b.auto.consumed {
					record.expires = time.Now().Add(-time.Second)
					f.b.auto.consumed[call] = record
				}
				f.b.auto.mu.Unlock()
			}
			f.startHook(t, denial)
			f.postedCard(t) // a normal new request, not a veto
		})
	}
}

// If the hook never confirmed it printed the allow (it missed its budget, or
// predates grant_delivered), a denial of that same call is not a veto: Claude
// Code overrode nothing. The old request closes as allow-undelivered and the
// operator gets a fresh card.
func TestAutoUndeliveredAllowGetsANewCard(t *testing.T) {
	f := newAutoFixture(t)
	r := f.armed(t)
	f.b.auto.mu.Lock()
	r.messageID, r.cardText = 10, "card body"
	f.b.auto.mu.Unlock()
	check := f.check()
	check.ToolUseID = "toolu_retry"
	if !f.b.consumeAutoGrant(check) {
		t.Fatal("grant not consumed")
	}
	denial := f.req
	denial.ToolUseID = "toolu_retry"
	var fresh *autoRequest
	output := captureLog(t, func() {
		f.startHook(t, denial)
		fresh = f.postedCard(t)
	})
	if fresh == r || f.state(r) != autoUndelivered {
		t.Fatalf("no fresh request (old state %s)", f.state(r))
	}
	if !strings.Contains(output, "allow-undelivered id="+r.id) || strings.Contains(output, "vetoed-after-allow") {
		t.Fatalf("audit: %s", output)
	}
	eventually(t, func() bool {
		for _, edit := range f.ch.editCallsSnapshot() {
			if edit.MessageID == 10 && strings.Contains(edit.Text, "didn't reach Claude Code") {
				return true
			}
		}
		return false
	})
}

// grant_delivered on the grant_check connection marks the record delivered;
// a mismatched tool_use_id or no frame leaves it undelivered.
func TestAutoGrantDeliveredFrame(t *testing.T) {
	for _, name := range []string{"delivered", "wrong tool_use_id", "no frame"} {
		t.Run(name, func(t *testing.T) {
			f := newAutoFixture(t)
			r := f.armed(t)
			check := f.check()
			check.ToolUseID = "toolu_retry"
			raw, _ := json.Marshal(check)
			hookSide, brokerSide := net.Pipe()
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer brokerSide.Close()
				f.b.handleGrantCheck(ipc.NewConn(brokerSide), "claude", raw)
			}()
			hook := ipc.NewConn(hookSide)
			if _, err := hook.ReadFrame(); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "delivered":
				_ = hook.WriteJSON(ipc.GrantDeliveredMsg{Op: ipc.OpGrantDelivered, ToolUseID: "toolu_retry"})
			case "wrong tool_use_id":
				_ = hook.WriteJSON(ipc.GrantDeliveredMsg{Op: ipc.OpGrantDelivered, ToolUseID: "toolu_other"})
			case "no frame":
				hook.Close()
			}
			<-done
			f.b.auto.mu.Lock()
			record := f.b.auto.consumed[autoConsumedCall{key: r.key, toolUseID: "toolu_retry"}]
			f.b.auto.mu.Unlock()
			if record.isDelivered != (name == "delivered") {
				t.Fatalf("delivered=%t", record.isDelivered)
			}
			hook.Close()
		})
	}
}

// Malformed, mismatched or ambiguous grant checks answer false and leave the
// grant for the real retry.
func TestAutoGrantCheckErrorsNeverAllow(t *testing.T) {
	f := newAutoFixture(t)
	f.armed(t)
	valid, _ := json.Marshal(f.check())
	for _, raw := range []string{
		`{`,
		`{"op":"grant_check","cli":"claude","session_id":"session-fixture","input_hash":"bogus"}`,
		`{"session_id":"other",` + string(valid[1:]),
		string(valid) + ` {}`,
	} {
		hookSide, brokerSide := net.Pipe()
		go func() { defer brokerSide.Close(); f.b.handleGrantCheck(ipc.NewConn(brokerSide), "claude", []byte(raw)) }()
		frame, err := ipc.NewConn(hookSide).ReadFrame()
		hookSide.Close()
		var result ipc.GrantCheckResp
		if err != nil || json.Unmarshal(frame, &result) != nil || result.Allow {
			t.Fatalf("%s answered %s (%v)", raw, frame, err)
		}
	}
	hookSide, brokerSide := net.Pipe()
	go func() { defer brokerSide.Close(); f.b.handleGrantCheck(ipc.NewConn(brokerSide), "codex", valid) }()
	frame, _ := ipc.NewConn(hookSide).ReadFrame()
	hookSide.Close()
	if strings.Contains(string(frame), `"allow":true`) {
		t.Fatal("a check whose CLI disagrees with its hello was allowed")
	}
	if !f.b.consumeAutoGrant(f.check()) {
		t.Fatal("a rejected check consumed the grant")
	}
}

// The audit log records the lifecycle and the tapper, never input text.
func TestAutoAuditNeverLogsInput(t *testing.T) {
	f := newAutoFixture(t)
	req := f.req
	req.ToolInput = json.RawMessage(`{"command":"unique-command-fixture","password":"unique-secret-fixture"}`)
	var r *autoRequest
	text := captureLog(t, func() {
		r = f.open(t, req)
		f.b.auto.mu.Lock()
		r.messageID = 10
		f.b.auto.mu.Unlock()
		f.tap(r, "allow")
		f.b.cancelAuto(f.owner, nil, "test")
	})
	if strings.Contains(text, "unique-command-fixture") || strings.Contains(text, "unique-secret-fixture") {
		t.Fatal("audit logged input text")
	}
	for _, want := range []string{"created", "approved", "tap-allow", "cancelled", r.id, r.key.inputHash, `tool="Bash"`, fmt.Sprintf("actor=%d", testOperatorUID)} {
		if !strings.Contains(text, want) {
			t.Fatalf("audit missing %q:\n%s", want, text)
		}
	}
}

// The socket is owner-only: any local process that can connect to it can
// approve or consume grants.
func TestBrokerSocketOwnerOnly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("socket ACL is managed by the parent directory on Windows")
	}
	f := newAutoFixture(t)
	path := t.TempDir() + "/broker.sock"
	server, err := Listen(path, f.b)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("socket mode %o, want 0600", info.Mode().Perm())
	}
}
