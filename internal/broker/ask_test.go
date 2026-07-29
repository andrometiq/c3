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

// TestResolveAsk_SingleSelect drives the broker-side resolution path: a
// registered pendingAsk, fed a callback "ask:<id>:1", resolves with options[1]
// and pushes an OpAskResult to the route holder's conn; a non-matching callback
// data does NOT resolve (so the generic event path proceeds); and a stale tap
// for an already-resolved ask returns false.
func TestResolveAsk_SingleSelect(t *testing.T) {
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	// A holder stub whose conn is the broker side of a pipe; the test reads the
	// pushed OpAskResult from the agent side.
	agentSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = agentSide.Close(); _ = brokerSide.Close() })
	agentConn := ipc.NewConn(agentSide)
	stub := b.Stubs.Register("claude", 4242, "/work", ipc.NewConn(brokerSide))

	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 5}
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatal("claim failed")
	}
	stub.SetRoute(&key)

	options := []string{"A", "B", "C"}
	b.Asks.register(&pendingAsk{askID: "abc12345", route: key, question: "Pick one", options: options, messageID: 77, owner: stub})

	// A non-ask callback must NOT resolve — the generic event path must proceed.
	if b.resolveAsk(key, &c3types.CallbackEvent{Data: "vote:abc12345:1"}) {
		t.Fatal("a non-ask callback must not resolve an ask (generic event must proceed)")
	}
	// An ask callback for an UNKNOWN id must NOT resolve, and must leave our live
	// ask untouched.
	if b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:deadbeef:0"}) {
		t.Fatal("a callback for an unknown ask id must not resolve")
	}
	if !b.Asks.has("abc12345") {
		t.Fatal("a non-matching callback must leave the pending ask registered")
	}

	// Start the holder-side reader BEFORE resolving (net.Pipe writes block until read).
	done := make(chan ipc.AskResultMsg, 1)
	go func() {
		raw, err := agentConn.ReadFrame()
		if err != nil {
			close(done)
			return
		}
		var m ipc.AskResultMsg
		_ = json.Unmarshal(raw, &m)
		done <- m
	}()

	// The matching tap resolves with options[1] == "B" and suppresses the event.
	if !b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:abc12345:1", MessageID: 77}) {
		t.Fatal("a matching ask callback must resolve (and suppress the generic event)")
	}

	select {
	case m, ok := <-done:
		if !ok {
			t.Fatal("holder conn read failed before an OpAskResult was pushed")
		}
		if m.Op != ipc.OpAskResult {
			t.Fatalf("pushed op = %q, want %q", m.Op, ipc.OpAskResult)
		}
		if m.AskID != "abc12345" {
			t.Fatalf("ask id = %q, want abc12345", m.AskID)
		}
		if len(m.Answer.Selected) != 1 || m.Answer.Selected[0] != "B" {
			t.Fatalf("resolved answer = %+v, want Selected=[B]", m.Answer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no OpAskResult pushed to the holder conn")
	}

	// The resolved ask is removed; the message was edited (keyboard cleared).
	if b.Asks.has("abc12345") {
		t.Fatal("a resolved ask must be removed from the registry")
	}
	if got := fc.editSnapshot(); len(got) != 1 {
		t.Fatalf("resolve should edit the message exactly once (mark + clear keyboard); got %d edits", len(got))
	} else if got[0].MessageID != 77 {
		t.Fatalf("edit targeted message %d, want 77", got[0].MessageID)
	} else if got[0].Buttons == nil {
		t.Fatal("resolve must clear the keyboard by passing a non-nil (empty) Buttons to EditMessage")
	}

	// A second (stale) tap for the same, already-resolved ask must NOT resolve.
	if b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:abc12345:0"}) {
		t.Fatal("a stale tap for an already-resolved ask must return false (generic path proceeds)")
	}
}

// TestAskRegistry_ExpiresStale drives FIX-1's reaper: an ask whose createdAt is
// older than askExpiryTTL is removed by sweepExpiredAsks, which best-effort
// clears its live keyboard (a timed-out body + empty Buttons), while a fresh ask
// survives untouched.
func TestAskRegistry_ExpiresStale(t *testing.T) {
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 5}

	owner := pendingOwner(t, b, key)
	stale := &pendingAsk{
		askID: "stale123", route: key, question: "Old?", options: []string{"A", "B"},
		selected: make([]bool, 2), messageID: 55, owner: owner,
		createdAt: time.Now().Add(-askExpiryTTL - time.Minute),
	}
	if !b.registerAsk(stale) {
		t.Fatal("register stale failed")
	}
	fresh := &pendingAsk{
		askID: "fresh123", route: key, question: "New?", options: []string{"A"},
		selected: make([]bool, 1), messageID: 66, createdAt: time.Now(), owner: owner,
	}
	if !b.registerAsk(fresh) {
		t.Fatal("register fresh failed")
	}

	b.sweepExpiredAsks()

	if b.Asks.has("stale123") {
		t.Fatal("a stale ask must be removed by the sweep")
	}
	if !b.Asks.has("fresh123") {
		t.Fatal("a fresh ask must survive the sweep")
	}
	edits := fc.editSnapshot()
	if len(edits) != 1 {
		t.Fatalf("sweep must clear exactly the stale ask's keyboard (1 edit); got %d", len(edits))
	}
	if edits[0].MessageID != 55 {
		t.Fatalf("sweep edit targeted msg %d, want 55 (the stale ask)", edits[0].MessageID)
	}
	if edits[0].Buttons == nil || len(edits[0].Buttons) != 0 {
		t.Fatalf("sweep must clear the keyboard via a non-nil empty Buttons; got %+v", edits[0].Buttons)
	}
	if !strings.Contains(edits[0].Text, "Timed out") {
		t.Fatalf("sweep edit body should record a timeout; got %q", edits[0].Text)
	}
}

// TestAskRegistry_CapEvictsOldest drives FIX-1's size cap: filling the registry
// to maxPendingAsks and registering one more evicts the OLDEST entry (smallest
// createdAt) and best-effort clears its keyboard, while the new ask is retained.
func TestAskRegistry_CapEvictsOldest(t *testing.T) {
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 5}
	base := time.Now().Add(-time.Hour)
	owner := pendingOwner(t, b, key)

	// Fill exactly to the cap; the i==0 entry is the oldest (smallest createdAt)
	// and carries messageID 1 so we can assert the eviction edit targets it.
	for i := 0; i < maxPendingAsks; i++ {
		p := &pendingAsk{
			askID: fmt.Sprintf("cap%05d", i), route: key, question: "Q",
			options: []string{"A"}, selected: make([]bool, 1), messageID: int64(i + 1),
			createdAt: base.Add(time.Duration(i) * time.Millisecond), owner: owner,
		}
		if !b.registerAsk(p) {
			t.Fatalf("register %d failed", i)
		}
	}
	if got := fc.editSnapshot(); len(got) != 0 {
		t.Fatalf("filling to cap must not evict anything; got %d edits", len(got))
	}

	// One more, over cap → evict the oldest (cap00000, messageID 1).
	over := &pendingAsk{
		askID: "capOVER01", route: key, question: "Q2", options: []string{"A"},
		selected: make([]bool, 1), messageID: 9999, createdAt: time.Now(), owner: owner,
	}
	if !b.registerAsk(over) {
		t.Fatal("over-cap register failed")
	}
	if b.Asks.has("cap00000") {
		t.Fatal("the oldest ask must be evicted when the cap is exceeded")
	}
	if !b.Asks.has("capOVER01") {
		t.Fatal("the new ask must be registered after eviction")
	}
	edits := fc.editSnapshot()
	if len(edits) != 1 {
		t.Fatalf("eviction must clear exactly one keyboard; got %d edits", len(edits))
	}
	if edits[0].MessageID != 1 {
		t.Fatalf("eviction edit targeted msg %d, want 1 (the oldest)", edits[0].MessageID)
	}
	if edits[0].Buttons == nil || len(edits[0].Buttons) != 0 {
		t.Fatalf("eviction must clear the keyboard via a non-nil empty Buttons; got %+v", edits[0].Buttons)
	}
}

// TestHandleAskRegister_RejectsNonKeyboardChannel drives FIX-4's capability gate:
// handleAskRegister on a channel whose Capabilities report InlineKeyboards=false
// (the default fakeChannel) replies OK=false with a clear reason and registers no
// pending ask.
func TestHandleAskRegister_RejectsNonKeyboardChannel(t *testing.T) {
	fc := &fakeChannel{} // Capabilities().InlineKeyboards == false
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	agentSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = agentSide.Close(); _ = brokerSide.Close() })
	stub := b.Stubs.Register("claude", 4242, "/work", ipc.NewConn(brokerSide))

	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 5}
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatal("claim failed")
	}
	stub.SetRoute(&key)

	raw, err := json.Marshal(ipc.AskRegisterReq{
		Op: ipc.OpAskRegister, AskID: "nokbd123", Question: "Pick one", Options: []string{"A", "B"},
	})
	if err != nil {
		t.Fatalf("marshal ask_register: %v", err)
	}

	// handleAskRegister writes the ack to the broker-side conn; read it from the
	// agent side. net.Pipe writes block until read, so run the handler in a goroutine.
	go b.handleAskRegister(ipc.NewConn(brokerSide), stub, raw)

	agentConn := ipc.NewConn(agentSide)
	done := make(chan ipc.AskRegisteredMsg, 1)
	go func() {
		frame, err := agentConn.ReadFrame()
		if err != nil {
			close(done)
			return
		}
		var m ipc.AskRegisteredMsg
		_ = json.Unmarshal(frame, &m)
		done <- m
	}()

	select {
	case m, ok := <-done:
		if !ok {
			t.Fatal("no ack frame read from handleAskRegister")
		}
		if m.OK {
			t.Fatalf("a non-keyboard channel must refuse ask register; got OK=true (%+v)", m)
		}
		if !strings.Contains(m.Err, "does not support interactive questions") {
			t.Fatalf("err = %q, want it to name the unsupported-keyboard reason", m.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handleAskRegister did not reply")
	}
	if b.Asks.has("nokbd123") {
		t.Fatal("a refused ask must not leave a pending entry registered")
	}
}

// askKeyboardHasButton reports whether any button in the keyboard has the exact
// text. Used by the multi-select / skip keyboard assertions below.
func askKeyboardHasButton(rows [][]c3types.Button, text string) bool {
	for _, row := range rows {
		for _, b := range row {
			if b.Text == text {
				return true
			}
		}
	}
	return false
}

// TestResolveAsk_MultiSelect_ToggleThenDone drives the multi-select path: each
// option tap TOGGLES selection (editing the keyboard in place WITHOUT removing
// the ask), and a trailing "Done" tap resolves with the selected option list.
// Toggling idx0 on, idx2 on, then idx0 off leaves {C}; Done resolves Selected=[C].
func TestResolveAsk_MultiSelect_ToggleThenDone(t *testing.T) {
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	agentSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = agentSide.Close(); _ = brokerSide.Close() })
	agentConn := ipc.NewConn(agentSide)
	stub := b.Stubs.Register("claude", 4242, "/work", ipc.NewConn(brokerSide))

	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 5}
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatal("claim failed")
	}
	stub.SetRoute(&key)

	options := []string{"A", "B", "C"}
	b.Asks.register(&pendingAsk{
		askID: "multi1234", route: key, question: "Pick some", options: options,
		multi: true, selected: make([]bool, len(options)), messageID: 77, owner: stub,
	})

	// Toggle idx0 ON — handled (true), keyboard edited, ask still registered, NO answer pushed.
	if !b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:multi1234:0", MessageID: 77}) {
		t.Fatal("a multi-select option tap must be handled (return true) to suppress the generic event")
	}
	if !b.Asks.has("multi1234") {
		t.Fatal("a toggle must NOT remove the ask from the registry")
	}
	// Toggle idx2 ON.
	if !b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:multi1234:2", MessageID: 77}) {
		t.Fatal("second toggle must be handled")
	}
	// Toggle idx0 OFF.
	if !b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:multi1234:0", MessageID: 77}) {
		t.Fatal("toggle-off must be handled")
	}
	if !b.Asks.has("multi1234") {
		t.Fatal("the ask must remain registered through every toggle, until Done")
	}

	// One EditMessage per toggle; the latest keyboard reflects {C} and keeps the question.
	edits := fc.editSnapshot()
	if len(edits) != 3 {
		t.Fatalf("expected one EditMessage per toggle (3), got %d", len(edits))
	}
	last := edits[len(edits)-1]
	if last.Text != "Pick some" {
		t.Fatalf("a toggle edit must keep the question as the message text, got %q", last.Text)
	}
	if last.Buttons == nil {
		t.Fatal("a toggle edit must carry a rebuilt keyboard")
	}
	if !askKeyboardHasButton(last.Buttons, "✓ C") {
		t.Fatalf("selected option C must show a ✓ prefix; keyboard=%+v", last.Buttons)
	}
	if !askKeyboardHasButton(last.Buttons, "A") || askKeyboardHasButton(last.Buttons, "✓ A") {
		t.Fatalf("unselected option A must NOT show a ✓ prefix; keyboard=%+v", last.Buttons)
	}
	if !askKeyboardHasButton(last.Buttons, "✅ Done") {
		t.Fatalf("a multi keyboard must carry a Done button; keyboard=%+v", last.Buttons)
	}

	// Read the holder push BEFORE Done (net.Pipe write blocks until read).
	done := make(chan ipc.AskResultMsg, 1)
	go func() {
		raw, err := agentConn.ReadFrame()
		if err != nil {
			close(done)
			return
		}
		var m ipc.AskResultMsg
		_ = json.Unmarshal(raw, &m)
		done <- m
	}()

	// Done resolves with the selected list and removes the ask.
	if !b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:multi1234:done", MessageID: 77}) {
		t.Fatal("Done must resolve a multi-select ask")
	}
	select {
	case m, ok := <-done:
		if !ok {
			t.Fatal("holder conn read failed before an OpAskResult was pushed")
		}
		if m.Op != ipc.OpAskResult || m.AskID != "multi1234" {
			t.Fatalf("pushed msg = %+v, want OpAskResult for multi1234", m)
		}
		if len(m.Answer.Selected) != 1 || m.Answer.Selected[0] != "C" {
			t.Fatalf("resolved answer = %+v, want Selected=[C]", m.Answer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no OpAskResult pushed on Done")
	}
	if b.Asks.has("multi1234") {
		t.Fatal("Done must remove the ask from the registry")
	}
	// Done edits once more, clearing the keyboard (non-nil empty Buttons).
	edits = fc.editSnapshot()
	if len(edits) != 4 {
		t.Fatalf("expected a 4th edit on Done (clear keyboard), got %d", len(edits))
	}
	if edits[3].Buttons == nil || len(edits[3].Buttons) != 0 {
		t.Fatalf("Done must clear the keyboard via a non-nil empty Buttons; got %+v", edits[3].Buttons)
	}
}

// TestResolveAsk_Skip drives the Skip path: an "ask:<id>:skip" tap resolves the
// ask with AskAnswer{Skipped:true}, removes it, and clears the keyboard.
func TestResolveAsk_Skip(t *testing.T) {
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()

	agentSide, brokerSide := net.Pipe()
	t.Cleanup(func() { _ = agentSide.Close(); _ = brokerSide.Close() })
	agentConn := ipc.NewConn(agentSide)
	stub := b.Stubs.Register("claude", 4242, "/work", ipc.NewConn(brokerSide))

	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 5}
	if _, ok := b.Routes.Claim(key, stub); !ok {
		t.Fatal("claim failed")
	}
	stub.SetRoute(&key)

	b.Asks.register(&pendingAsk{
		askID: "skip1234", route: key, question: "Pick or skip", options: []string{"A", "B"},
		allowSkip: true, selected: make([]bool, 2), messageID: 88, owner: stub,
	})

	done := make(chan ipc.AskResultMsg, 1)
	go func() {
		raw, err := agentConn.ReadFrame()
		if err != nil {
			close(done)
			return
		}
		var m ipc.AskResultMsg
		_ = json.Unmarshal(raw, &m)
		done <- m
	}()

	if !b.resolveAsk(key, &c3types.CallbackEvent{Data: "ask:skip1234:skip", MessageID: 88}) {
		t.Fatal("a skip tap must resolve the ask")
	}
	select {
	case m, ok := <-done:
		if !ok {
			t.Fatal("holder conn read failed before an OpAskResult was pushed")
		}
		if m.Op != ipc.OpAskResult || m.AskID != "skip1234" {
			t.Fatalf("pushed msg = %+v, want OpAskResult for skip1234", m)
		}
		if !m.Answer.Skipped {
			t.Fatalf("skip answer must set Skipped=true, got %+v", m.Answer)
		}
		if len(m.Answer.Selected) != 0 {
			t.Fatalf("skip answer must carry no selection, got %+v", m.Answer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no OpAskResult pushed on skip")
	}
	if b.Asks.has("skip1234") {
		t.Fatal("skip must remove the ask from the registry")
	}
	edits := fc.editSnapshot()
	if len(edits) != 1 || edits[0].MessageID != 88 {
		t.Fatalf("skip must edit the message once (msg 88); got %+v", edits)
	}
	if edits[0].Buttons == nil || len(edits[0].Buttons) != 0 {
		t.Fatalf("skip must clear the keyboard via a non-nil empty Buttons; got %+v", edits[0].Buttons)
	}
}

// TestAskKeyboardFor_MultiAndSkip pins the generalized keyboard builder: a multi
// ask renders ✓ prefixes for selected options plus a Done button, allow_skip adds
// a Skip button, and every callback_data parses back and stays within the cap.
func TestAskKeyboardFor_MultiAndSkip(t *testing.T) {
	p := &pendingAsk{
		askID: "kbd23456", options: []string{"A", "B", "C"},
		multi: true, allowSkip: true, selected: []bool{false, true, false},
	}
	rows := askKeyboardFor(p)
	// 3 options + Done + Skip = 5 rows.
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows (3 options + Done + Skip), got %d", len(rows))
	}
	if !askKeyboardHasButton(rows, "A") || askKeyboardHasButton(rows, "✓ A") {
		t.Fatalf("unselected A must have no ✓; rows=%+v", rows)
	}
	if !askKeyboardHasButton(rows, "✓ B") {
		t.Fatalf("selected B must show ✓; rows=%+v", rows)
	}
	if !askKeyboardHasButton(rows, "✅ Done") {
		t.Fatalf("multi keyboard must carry a Done button; rows=%+v", rows)
	}
	if !askKeyboardHasButton(rows, "⏭ Skip") {
		t.Fatalf("allow_skip keyboard must carry a Skip button; rows=%+v", rows)
	}
	for _, row := range rows {
		for _, btn := range row {
			if len(btn.Data) > 64 {
				t.Fatalf("callback_data %q is %d bytes, over Telegram's 64-byte cap", btn.Data, len(btn.Data))
			}
			id, action, _ := parseAskCallback(btn.Data)
			if id != "kbd23456" || action == askActionNone {
				t.Fatalf("button data %q did not parse to a valid ask action (got id=%q action=%v)", btn.Data, id, action)
			}
		}
	}
	// The Done / Skip callbacks parse to their distinct actions.
	if _, action, _ := parseAskCallback(askDoneData("kbd23456")); action != askActionDone {
		t.Fatalf("done callback parsed to action %v, want Done", action)
	}
	if _, action, _ := parseAskCallback(askSkipData("kbd23456")); action != askActionSkip {
		t.Fatalf("skip callback parsed to action %v, want Skip", action)
	}
}

// TestAskKeyboard_RoundTripsThroughParse pins the broker's keyboard builder
// against its own parser: every button askKeyboard produces must parse back to
// the right (askID, idx), and at the max single-select option count each
// callback_data stays well within Telegram's 64-byte ceiling.
func TestAskKeyboard_RoundTripsThroughParse(t *testing.T) {
	const askID = "abcd2345" // 8-char base32, no colon
	n := 100
	options := make([]string, n)
	for i := range options {
		options[i] = "opt"
	}
	rows := askKeyboard(askID, options)
	if len(rows) != n {
		t.Fatalf("askKeyboard produced %d rows, want %d (one button per row)", len(rows), n)
	}
	for i, row := range rows {
		if len(row) != 1 {
			t.Fatalf("row %d has %d buttons, want 1 (single-select)", i, len(row))
		}
		data := row[0].Data
		if len(data) > 64 {
			t.Fatalf("callback_data %q is %d bytes, over Telegram's 64-byte cap", data, len(data))
		}
		gotID, gotIdx, ok := parseAskData(data)
		if !ok {
			t.Fatalf("parseAskData(%q) failed to parse a callback we generated", data)
		}
		if gotID != askID || gotIdx != i {
			t.Fatalf("round-trip mismatch for %q: got (%q,%d) want (%q,%d)", data, gotID, gotIdx, askID, i)
		}
	}
}
