package telegram

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// makeChannelWithPolls builds a test Channel with a fakeHost and an initialized
// sentPolls map (the poll-update path needs it).
func makeChannelWithPolls(host channel.Host) *Channel {
	c := makeChannel(host)
	c.sentPolls = newSentPollMap(16)
	return c
}

// TestDispatchPollUpdate_ClosedEmitsStampedResult covers the headline read path
// + CB-2: a CLOSED aggregate poll update for a poll we sent is converted into a
// poll_result Inbound stamped with the route owner (so it passes the DM gate)
// and emitted.
func TestDispatchPollUpdate_ClosedEmitsStampedResult(t *testing.T) {
	const owner = int64(555)
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannelWithPolls(h)
	// Pretend we sent this poll on a DM route (ChatID == owner's user id).
	c.sentPolls.Put("p1", &sentPoll{ChatID: owner, MessageID: 42, OwnerUserID: owner})

	poll := &gotgbot.Poll{
		Id: "p1", Question: "Lunch?", TotalVoterCount: 3, IsClosed: true,
		Options: []gotgbot.PollOption{
			{Text: "Pizza", VoterCount: 2},
			{Text: "Tacos", VoterCount: 1},
		},
	}
	c.dispatchPollUpdate(7, poll)

	if h.emitCount() != 1 {
		t.Fatalf("a closed poll update should emit exactly 1 poll_result; got %d", h.emitCount())
	}
	in := h.emitted[0]
	if in.Kind != c3types.InboundPollResult {
		t.Fatalf("wrong kind: %q", in.Kind)
	}
	if in.Sender.UserID != owner {
		t.Errorf("CB-2: poll_result must be stamped with the route owner %d; got %d", owner, in.Sender.UserID)
	}
	if in.ChatID != owner || in.MessageID != 42 {
		t.Errorf("poll_result not routed to the original route: chat=%d msg=%d", in.ChatID, in.MessageID)
	}
	pr := in.Event.PollResult
	if pr == nil || pr.TotalVoters != 3 || !pr.IsClosed || len(pr.Options) != 2 {
		t.Fatalf("poll_result tally wrong: %+v", pr)
	}
	if pr.Options[0].Text != "Pizza" || pr.Options[0].VoterCount != 2 {
		t.Errorf("option tally wrong: %+v", pr.Options)
	}
}

// TestDispatchPollUpdate_OpenNotSurfaced asserts the FINAL-ON-CLOSE rule: an
// OPEN aggregate update is retained (latest tally) but NOT surfaced to the agent
// (no per-vote spam).
func TestDispatchPollUpdate_OpenNotSurfaced(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannelWithPolls(h)
	c.sentPolls.Put("p1", &sentPoll{ChatID: 555, MessageID: 42, OwnerUserID: 555})

	poll := &gotgbot.Poll{Id: "p1", Question: "Q", TotalVoterCount: 1, IsClosed: false,
		Options: []gotgbot.PollOption{{Text: "A", VoterCount: 1}, {Text: "B", VoterCount: 0}}}
	c.dispatchPollUpdate(7, poll)

	if h.emitCount() != 0 {
		t.Fatalf("an OPEN poll update must NOT be surfaced (final-on-close); got %d emits", h.emitCount())
	}
	// But the latest tally is retained for an on-demand read.
	sp, ok := c.sentPolls.Get("p1")
	if !ok || sp.LatestTally == nil || sp.LatestTally.TotalVoters != 1 {
		t.Errorf("open-poll tally should be retained; got %+v", sp)
	}
}

// TestDispatchPollUpdate_UnknownPollDropped asserts a poll we never sent (absent
// from sentPolls) is dropped (nothing to route).
func TestDispatchPollUpdate_UnknownPollDropped(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannelWithPolls(h)
	c.dispatchPollUpdate(7, &gotgbot.Poll{Id: "unknown", IsClosed: true})
	if h.emitCount() != 0 {
		t.Errorf("an unknown poll id must be dropped; got %d emits", h.emitCount())
	}
}

// TestPollTally_ConcurrentUpdateNoRace exercises the two sites that mutate a
// shared *sentPoll's LatestTally concurrently — the poll-loop goroutine
// (dispatchPollUpdate) and the worker goroutine (StopPoll). Both now funnel
// through the in-lock sentPollMap.UpdateTally, so the previously-unsynchronized
// read-modify-write is gone. StopPoll itself requires a live bot (network), so
// this drives its mutation via UpdateTally directly — the exact in-lock write it
// performs — alongside dispatchPollUpdate, and must pass under `go test -race`.
func TestPollTally_ConcurrentUpdateNoRace(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannelWithPolls(h)
	c.sentPolls.Put("p-race", &sentPoll{ChatID: 555, MessageID: 42, OwnerUserID: 555})

	const iters = 2000
	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine A: poll-loop path — an OPEN aggregate update refreshes the tally
	// under the lock (not surfaced, but it mutates LatestTally via UpdateTally).
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			c.dispatchPollUpdate(int64(i), &gotgbot.Poll{
				Id: "p-race", Question: "Q", TotalVoterCount: int64(i), IsClosed: false,
				Options: []gotgbot.PollOption{{Text: "A", VoterCount: int64(i)}},
			})
		}
	}()

	// Goroutine B: StopPoll's mutation — the same in-lock UpdateTally write.
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			c.sentPolls.UpdateTally("p-race", &pollTally{
				Question: "Q", TotalVoters: i, IsClosed: true,
				Options: []pollOptionTally{{Text: "A", VoterCount: i}},
			})
		}
	}()

	wg.Wait()

	// The entry still exists and carries a (non-nil) tally — sanity, not the point.
	if sp, ok := c.sentPolls.Get("p-race"); !ok || sp.LatestTally == nil {
		t.Errorf("poll entry should survive concurrent updates with a tally; got %+v", sp)
	}
}

// TestDispatchReaction_SetDiff covers the Pass-1 C3 set-diff: added/removed are
// the difference of new vs old; custom/paid render as sentinels, never dropped.
func TestDispatchReaction_SetDiff(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannelWithPolls(h)

	mr := &gotgbot.MessageReactionUpdated{
		Chat:      gotgbot.Chat{Id: -100},
		MessageId: 321,
		User:      &gotgbot.User{Id: 7, Username: "ada"},
		OldReaction: []gotgbot.ReactionType{
			gotgbot.ReactionTypeEmoji{Emoji: "👍"},
		},
		NewReaction: []gotgbot.ReactionType{
			gotgbot.ReactionTypeEmoji{Emoji: "🔥"},
			gotgbot.ReactionTypeCustomEmoji{CustomEmojiId: "x"},
		},
	}
	c.dispatchReaction(9, mr)

	if h.emitCount() != 1 {
		t.Fatalf("expected 1 reaction emit; got %d", h.emitCount())
	}
	re := h.emitted[0].Event.Reaction
	if re == nil {
		t.Fatal("reaction event payload missing")
	}
	// 👍 removed, 🔥 + [custom] added.
	if !contains(re.Added, "🔥") || !contains(re.Added, "[custom]") {
		t.Errorf("added set wrong: %+v (custom must render as sentinel, not dropped)", re.Added)
	}
	if !contains(re.Removed, "👍") {
		t.Errorf("removed set wrong: %+v", re.Removed)
	}
	if re.Actor.Username != "ada" {
		t.Errorf("actor not carried: %+v", re.Actor)
	}
}

// TestDispatchCallback_InaccessibleMessageDropped covers Pass-1 C4: when
// CallbackQuery.Message is the inaccessible variant we cannot route it, so it is
// dropped (after the auto-ack, which is a no-op here since bot is nil) — and NOT
// surfaced with garbage coordinates.
func TestDispatchCallback_InaccessibleMessageDropped(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannelWithPolls(h) // bot is nil → auto-ack is skipped, drop still works

	cq := &gotgbot.CallbackQuery{
		Id:      "cb1",
		From:    gotgbot.User{Id: 7},
		Data:    "approve:42",
		Message: gotgbot.InaccessibleMessage{MessageId: 1, Chat: gotgbot.Chat{Id: -100}},
	}
	c.dispatchCallback(9, cq)
	if h.emitCount() != 0 {
		t.Errorf("an inaccessible-message callback must not be routed; got %d emits", h.emitCount())
	}
}

// TestDispatchCallback_AccessibleSurfaced asserts an accessible callback is
// converted into a callback InboundEvent carrying the callback id + data and
// routed to the message's chat. (bot is nil so the auto-ack is skipped; the
// surfacing is the unit under test.)
func TestDispatchCallback_AccessibleSurfaced(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannelWithPolls(h)

	cq := &gotgbot.CallbackQuery{
		Id:   "cb1",
		From: gotgbot.User{Id: 7, Username: "ada"},
		Data: "approve:42",
		Message: &gotgbot.Message{
			MessageId: 200,
			Chat:      gotgbot.Chat{Id: -100},
		},
	}
	c.dispatchCallback(9, cq)
	if h.emitCount() != 1 {
		t.Fatalf("expected 1 callback emit; got %d", h.emitCount())
	}
	in := h.emitted[0]
	if in.Kind != c3types.InboundCallback {
		t.Fatalf("wrong kind: %q", in.Kind)
	}
	cb := in.Event.Callback
	if cb == nil || cb.CallbackID != "cb1" || cb.Data != "approve:42" || cb.MessageID != 200 {
		t.Errorf("callback payload wrong: %+v", cb)
	}
	if in.ChatID != -100 {
		t.Errorf("callback should route to the message chat; got %d", in.ChatID)
	}
}

// recordingBotClient is a gotgbot.BotClient double that records every Bot API
// request (method + params) and returns `true` — the wire shape
// answerCallbackQuery expects. Lets the perm-tap tests assert exactly which
// callbacks were answered, with what toast text.
type recordingBotClient struct {
	mu    sync.Mutex
	calls []recordedCall
}

type recordedCall struct {
	method string
	params map[string]any
}

func (r *recordingBotClient) RequestWithContext(_ context.Context, _, method string, params map[string]any, _ *gotgbot.RequestOpts) (json.RawMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, recordedCall{method: method, params: params})
	return json.RawMessage("true"), nil
}

func (r *recordingBotClient) GetAPIURL(*gotgbot.RequestOpts) string               { return "" }
func (r *recordingBotClient) FileURL(string, string, *gotgbot.RequestOpts) string { return "" }

func (r *recordingBotClient) callsFor(method string) []recordedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []recordedCall
	for _, c := range r.calls {
		if c.method == method {
			out = append(out, c)
		}
	}
	return out
}

// makeChannelWithBot is makeChannelWithPolls plus a live (recorded) bot, so the
// answerCallbackQuery path actually runs instead of no-opping on a nil bot.
func makeChannelWithBot(host channel.Host, rc *recordingBotClient) *Channel {
	c := makeChannelWithPolls(host)
	c.bot = &gotgbot.Bot{Token: "test", BotClient: rc}
	return c
}

// permTapQuery builds an accessible-message callback query carrying `data`.
func permTapQuery(id, data string, userID int64) *gotgbot.CallbackQuery {
	return &gotgbot.CallbackQuery{
		Id:   id,
		From: gotgbot.User{Id: userID},
		Data: data,
		Message: &gotgbot.Message{
			MessageId: 300,
			Chat:      gotgbot.Chat{Id: -100},
		},
	}
}

// TestDispatchCallback_PermTap_AckDeferredToBroker: a "perm:" tap that passes
// the gate must NOT be auto-acked by the channel. A callback query can be
// answered exactly once, and for a permission tap that single answer is the
// only user-visible feedback surface Telegram offers — the broker's
// resolvePerm answers it with the tap's real outcome. An early empty ack here
// would eat the feedback and reproduce the 2026-06-30 "Allow did nothing" bug.
func TestDispatchCallback_PermTap_AckDeferredToBroker(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	rc := &recordingBotClient{}
	c := makeChannelWithBot(h, rc)

	c.dispatchCallback(9, permTapQuery("cb-perm", "perm:allow:abcde", 7))

	if h.emitCount() != 1 {
		t.Fatalf("a gated-through perm tap must emit; got %d", h.emitCount())
	}
	if calls := rc.callsFor("answerCallbackQuery"); len(calls) != 0 {
		t.Fatalf("the channel must defer the perm-tap ack to the broker; got %d early answers: %+v", len(calls), calls)
	}
}

// TestDispatchCallback_PermTap_GateDrop_AnswersNotAuthorized is the DM-variant
// live-bug regression: a perm tap from a sender the inbound gate refuses
// (non-allowlisted) must still be REFUSED — but answered with an explicit
// not-authorized alert instead of dying silently. The verdict is never honored
// (emit count stays 0); only the silence is removed.
func TestDispatchCallback_PermTap_GateDrop_AnswersNotAuthorized(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundDrop}
	rc := &recordingBotClient{}
	c := makeChannelWithBot(h, rc)

	c.dispatchCallback(9, permTapQuery("cb-stranger", "perm:allow:abcde", 99))

	if h.emitCount() != 0 {
		t.Fatalf("a gate-dropped perm tap must NOT be emitted; got %d", h.emitCount())
	}
	calls := rc.callsFor("answerCallbackQuery")
	if len(calls) != 1 {
		t.Fatalf("a gate-dropped perm tap must be answered exactly once (silence = the live bug); got %d", len(calls))
	}
	text, _ := calls[0].params["text"].(string)
	if !strings.Contains(text, "Not authorized") {
		t.Fatalf("gate-drop answer must say not-authorized; got %q", text)
	}
	if alert, _ := calls[0].params["show_alert"].(bool); !alert {
		t.Error("gate-drop answer should be an alert (a plain toast is too easy to miss)")
	}
}

// TestDispatchCallback_PermTap_EmitDrop_Answered: when the worker queue refuses
// the tap (Emit returns false — queue full / route worker gone) the broker will
// never answer the deferred ack, so the channel must. The pending perm stays
// live broker-side, so the notice invites a re-tap.
func TestDispatchCallback_PermTap_EmitDrop_Answered(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, emitDrops: true}
	rc := &recordingBotClient{}
	c := makeChannelWithBot(h, rc)

	c.dispatchCallback(9, permTapQuery("cb-full", "perm:deny:abcde", 7))

	calls := rc.callsFor("answerCallbackQuery")
	if len(calls) != 1 {
		t.Fatalf("an emit-dropped perm tap must be answered by the channel; got %d", len(calls))
	}
	if text, _ := calls[0].params["text"].(string); text == "" {
		t.Error("emit-drop answer should carry a try-again notice, not a bare ack")
	}
}

// TestDispatchCallback_PermTap_Inaccessible_Answered: a perm tap whose message
// is inaccessible (deleted / too old) cannot be routed, so the channel answers
// the deferred ack itself — never a spinner, never silence.
func TestDispatchCallback_PermTap_Inaccessible_Answered(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	rc := &recordingBotClient{}
	c := makeChannelWithBot(h, rc)

	cq := &gotgbot.CallbackQuery{
		Id:      "cb-gone",
		From:    gotgbot.User{Id: 7},
		Data:    "perm:allow:abcde",
		Message: gotgbot.InaccessibleMessage{MessageId: 1, Chat: gotgbot.Chat{Id: -100}},
	}
	c.dispatchCallback(9, cq)

	if h.emitCount() != 0 {
		t.Fatalf("an inaccessible perm tap must not be routed; got %d emits", h.emitCount())
	}
	calls := rc.callsFor("answerCallbackQuery")
	if len(calls) != 1 {
		t.Fatalf("an unroutable perm tap must still be answered; got %d", len(calls))
	}
	if text, _ := calls[0].params["text"].(string); text == "" {
		t.Error("unroutable perm-tap answer should carry a notice")
	}
}

// TestDispatchCallback_NonPermTap_AutoAckImmediate pins the contract for every
// OTHER callback (ask keyboards, agent-rendered reply buttons): immediate
// auto-ack (Q-RESULT-2 — no spinner, no agent-in-the-loop), then surfaced. The
// ack now carries a brief confirmation toast instead of empty opts — the empty
// ack cleared the spinner but showed nothing, leaving a non-perm tap with no
// immediate feedback. It stays a lightweight toast (ShowAlert false), never a
// modal alert.
func TestDispatchCallback_NonPermTap_AutoAckImmediate(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	rc := &recordingBotClient{}
	c := makeChannelWithBot(h, rc)

	c.dispatchCallback(9, permTapQuery("cb-ask", "ask:abcde:0", 7))

	if h.emitCount() != 1 {
		t.Fatalf("a non-perm callback must emit; got %d", h.emitCount())
	}
	calls := rc.callsFor("answerCallbackQuery")
	if len(calls) != 1 {
		t.Fatalf("a non-perm callback must be auto-acked exactly once; got %d", len(calls))
	}
	text, _ := calls[0].params["text"].(string)
	if text == "" {
		t.Error("the non-perm auto-ack must carry a confirmation toast, not a bare ack")
	}
	if text != callbackAckToastText {
		t.Errorf("the confirmation toast must be callbackAckToastText %q; got %q", callbackAckToastText, text)
	}
	// ShowAlert stays false → gotgbot omits show_alert entirely (a lightweight
	// toast, not a modal). Its presence would mean we surfaced an alert.
	if _, ok := calls[0].params["show_alert"]; ok {
		t.Error("the confirmation toast must stay a lightweight toast (ShowAlert false), not a modal alert")
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
