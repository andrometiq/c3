package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/mappings"
)

// --- CB-1: the route worker must be event-aware ---------------------------
//
// mergeBatch (the debounce collapse) historically copied ONLY the text/media
// fields and dropped any Kind/Event, and flushInbounds ran hasVoice/STT over
// whatever was in the batch. So an inbound EVENT sharing a debounce window with
// a text message would be corrupted (its Kind/Event spliced away) and an event
// would be run through STT. The fix: an event flushes ALONE (never merged into a
// text batch) and BYPASSES the voice/STT path. These tests prove both.

// TestCB1_EventSharingWindowDeliveredIntactAndSeparately drives the real run
// loop: a text message and a poll_result event are submitted back-to-back into
// one debounce window. The event must be forwarded (a) as its own inbound, not
// concatenated into the text, and (b) with its Kind/Event intact.
func TestCB1_EventSharingWindowDeliveredIntactAndSeparately(t *testing.T) {
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	var mu sync.Mutex
	var forwarded []*c3types.Inbound
	b.Plugins.OnInbound(func(_ context.Context, in *c3types.Inbound) (*c3types.Inbound, bool) {
		mu.Lock()
		// Copy so a later mutation can't race the assertion.
		cp := *in
		forwarded = append(forwarded, &cp)
		mu.Unlock()
		return in, true
	})

	// The default debounce window (1500ms) is long enough that the text is still
	// buffered when the event is submitted microseconds later — they genuinely
	// "share a window." The event submission must flush the buffered text and
	// then forward the event alone.
	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	text := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 1, Text: "hello there",
		Timestamp: time.Now(),
	}
	event := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 2,
		Kind: c3types.InboundPollResult,
		Event: &c3types.InboundEvent{PollResult: &c3types.PollResult{
			PollID: "p1", Question: "Lunch?", TotalVoters: 3, IsClosed: true,
			Options: []c3types.PollOptionTally{{Text: "Pizza", VoterCount: 2}, {Text: "Tacos", VoterCount: 1}},
		}},
		Timestamp: time.Now(),
	}

	if !w.Submit(Job{Kind: JobInbound, Inbound: text}) {
		t.Fatal("submit text failed")
	}
	// The event submission must flush the buffered text and then forward the
	// event alone — both happen synchronously inside the run goroutine, so poll
	// for the two forwarded inbounds.
	if !w.Submit(Job{Kind: JobInbound, Inbound: event}) {
		t.Fatal("submit event failed")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(forwarded)
		mu.Unlock()
		if n >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(forwarded) != 2 {
		t.Fatalf("expected exactly 2 separate forwards (text, then event); got %d: %+v", len(forwarded), forwarded)
	}
	// First forward is the text batch — it must be an ordinary message with the
	// text intact and NOT carry the event.
	gotText := forwarded[0]
	if gotText.IsEvent() {
		t.Errorf("first forward should be the plain text message, not an event; got Kind=%q", gotText.Kind)
	}
	if gotText.Text != "hello there" {
		t.Errorf("text message corrupted: Text=%q want %q", gotText.Text, "hello there")
	}
	if gotText.Event != nil {
		t.Errorf("text message must NOT carry an event payload; got %+v", gotText.Event)
	}
	// Second forward is the event — its Kind/Event must be intact (not dropped by
	// a text-only merge) and it must not have been spliced with the text.
	gotEvent := forwarded[1]
	if !gotEvent.IsEvent() || gotEvent.Kind != c3types.InboundPollResult {
		t.Fatalf("second forward should be the poll_result event; got Kind=%q event=%v", gotEvent.Kind, gotEvent.Event)
	}
	if gotEvent.Event == nil || gotEvent.Event.PollResult == nil {
		t.Fatal("event payload dropped — the exact CB-1 corruption this test guards against")
	}
	pr := gotEvent.Event.PollResult
	if pr.PollID != "p1" || pr.Question != "Lunch?" || pr.TotalVoters != 3 || !pr.IsClosed {
		t.Errorf("poll_result payload corrupted: %+v", pr)
	}
	if gotEvent.Text != "" {
		t.Errorf("event must not be text-spliced with the message; got Text=%q", gotEvent.Text)
	}
}

// TestCB1_EventBypassesSTT proves an event Inbound never runs through the
// voice/STT substitution even if it (pathologically) carried a voice
// attachment. The STT plugin must NOT be invoked for an event.
func TestCB1_EventBypassesSTT(t *testing.T) {
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	var sttCalls int
	var mu sync.Mutex
	b.Plugins.OnVoiceReceived(func(_ context.Context, _ c3types.VoicePayload) (string, error) {
		mu.Lock()
		sttCalls++
		mu.Unlock()
		return "SHOULD-NOT-RUN", nil
	})

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	// An event that pathologically also carries a voice attachment. flushEvent
	// must NOT consult the STT plugin; flushInbounds' STT loop also guards
	// IsEvent() defensively.
	event := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 9,
		Kind:        c3types.InboundReaction,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "v1"}},
		Event: &c3types.InboundEvent{Reaction: &c3types.ReactionEvent{
			MessageID: 9, Added: []string{"👍"},
		}},
		Timestamp: time.Now(),
	}
	// Direct flushEvent call is the unit under test (the run loop routes here).
	w.flushEvent(context.Background(), event)

	mu.Lock()
	defer mu.Unlock()
	if sttCalls != 0 {
		t.Fatalf("STT plugin invoked %d times for an event — events must bypass STT (CB-1)", sttCalls)
	}
	if event.Text == "SHOULD-NOT-RUN" {
		t.Fatal("event text was overwritten by STT substitution — CB-1 violated")
	}
}

// --- CB-2: stamp the route-owner UserID so DM poll_result isn't gate-dropped --
//
// An aggregate poll_result carries no voter, so a synthesized inbound with
// UserID==0 would hit IsUserAllowed(0) and be dropped in EVERY DM. The channel
// stamps the route-owner UserID, and the gate additionally allow-lists a
// poll_result on the route the bot initiated. These tests prove a poll_result is
// NOT gate-dropped on a DM route.

// TestCB2_PollResultNotGateDroppedInDM_WithStampedOwner asserts the normal path:
// a DM poll_result whose Sender.UserID is the (allowlisted) owner passes the
// gate via the IsUserAllowed fast-path.
func TestCB2_PollResultNotGateDroppedInDM_WithStampedOwner(t *testing.T) {
	const owner = int64(424242)
	mf := &mappings.MappingsFile{SchemaVersion: 1, Allowlist: &mappings.Allowlist{Users: []int64{owner}}}
	b := New(mf)
	defer b.Shutdown()

	// DM route: ChatID > 0 == the owner's user id (Telegram private-chat invariant).
	in := &c3types.Inbound{
		Channel: "telegram", ChatID: owner, MessageID: 5,
		Sender: c3types.Sender{UserID: owner}, // CB-2 stamp
		Kind:   c3types.InboundPollResult,
		Event:  &c3types.InboundEvent{PollResult: &c3types.PollResult{PollID: "p1", IsClosed: true}},
	}
	if got := b.Gate(in); got != GateAllow {
		t.Fatalf("DM poll_result with stamped owner should pass the gate; got %v (would be the headline-dead-in-DM regression)", got)
	}
}

// TestCB2_PollResultNotGateDroppedInDM_OwnerUnknown is the safety net: even if
// the owner could NOT be determined (stamp==0), a poll_result on a DM route must
// not be silently dropped — the gate has an explicit allow for InboundPollResult.
func TestCB2_PollResultNotGateDroppedInDM_OwnerUnknown(t *testing.T) {
	// Empty allowlist — IsUserAllowed(anything) is false. A 0-stamp poll_result
	// in a DM must STILL pass (the bot-initiated-route safety net), not drop.
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: 777, MessageID: 6,
		Sender: c3types.Sender{UserID: 0}, // owner unknown / unstamped
		Kind:   c3types.InboundPollResult,
		Event:  &c3types.InboundEvent{PollResult: &c3types.PollResult{PollID: "p2", IsClosed: true}},
	}
	if got := b.Gate(in); got != GateAllow {
		t.Fatalf("DM poll_result on a bot-initiated route must not be gate-dropped even with owner==0; got %v", got)
	}
}

// TestCB2_StrangerReactionStillGateDropped confirms the allowlist stays
// authoritative for events carrying a real user: a reaction from a non-
// allowlisted stranger in a DM is still dropped (the CB-2 allow is poll_result-
// only, not a blanket event bypass).
func TestCB2_StrangerReactionStillGateDropped(t *testing.T) {
	b := New(&mappings.MappingsFile{SchemaVersion: 1}) // empty allowlist
	defer b.Shutdown()

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: 999, MessageID: 7,
		Sender: c3types.Sender{UserID: 999}, // a real, non-allowlisted user
		Kind:   c3types.InboundReaction,
		Event:  &c3types.InboundEvent{Reaction: &c3types.ReactionEvent{MessageID: 7, Added: []string{"👍"}}},
	}
	if got := b.Gate(in); got != GateDrop {
		t.Fatalf("a stranger's reaction in a DM must be gate-dropped (allowlist authoritative); got %v", got)
	}
}
