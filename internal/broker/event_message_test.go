package broker

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

const eventChannelOffer = `{"version":1,"live":{"channel":{"eligible":true,"event_push_renders":true},"inbox":{"eligible":false}},"receipts":"transcript","fetch":"receipt"}`

func TestFlushEventLegacyRenderStates(t *testing.T) {
	for _, state := range []string{"", ipc.RenderCapable, ipc.RenderQueueOnly, ipc.RenderProbing, ipc.RenderCrossSession, "unknown"} {
		t.Run(state, func(t *testing.T) {
			b, w, holder, frames := shadowFixture(t)
			holder.SetRenderRoute(state, "", false)
			capable := state == "" || state == ipc.RenderCapable
			if got := eventPushRenders(holder); got != capable {
				t.Fatalf("render predicate=%t", got)
			}
			event := callbackInbound(w.key)
			event.Event.Callback.Label = "Continue"
			w.flushEvent(context.Background(), event)
			if capable {
				frame := waitInboundPush(t, frames)
				if frame.Inbound.Kind != c3types.InboundCallback || frame.Inbound.Event.Callback.Label != "Continue" {
					t.Fatalf("event changed: %+v", frame)
				}
			} else {
				select {
				case f := <-frames:
					t.Fatalf("unexpected push: %+v", f)
				case <-time.After(20 * time.Millisecond):
				}
			}
			want := 1
			if capable {
				want = 0
			}
			if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != want {
				t.Fatalf("pending=%d want=%d", n, want)
			}
			if state == ipc.RenderProbing && !holder.tryRenderPush() {
				t.Fatal("event consumed the probe")
			}
		})
	}
}

func TestFlushEventNegotiatedInbox(t *testing.T) {
	b, w, holder, frames, ctx := negotiatedFixture(t)
	holder.delivery.mu.Lock()
	holder.delivery.live.Channel.Eligible = false
	holder.delivery.live.Inbox.Eligible = true
	holder.delivery.mu.Unlock()
	event := callbackInbound(w.key)
	w.flushEvent(ctx, event)
	frame := nextDeliver(t, frames)
	rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if frame.Transport != "inbox" || frame.Inbound.IsEvent() || frame.Inbound.Text != rows[0].Inbound.Text || frame.Inbound.MessageID != 0 {
		t.Fatalf("delivery=%+v", frame)
	}
	if a := w.liveAttempt(); a == nil || a.Transport != "inbox" || len(a.Members) != 1 || a.Members[0].ID != rows[0].RecordID {
		t.Fatalf("reservation=%+v", a)
	}
	w.handleAttemptResult(resultFor(holder, frame, "confirmed"))
	if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 0 {
		t.Fatalf("zero-ID confirmation left %d rows", n)
	}
}

func TestFlushEventNegotiatedChannel(t *testing.T) {
	b, w, holder, frames := shadowFixture(t)
	b.configureDelivery(holder, []byte(channelOffer))
	ack := b.buildHelloAck(ipc.HelloMsg{Capabilities: []string{"event_push_renders:1"}}, holder)
	if len(ack.AcceptedCapabilities) != 1 || ack.AcceptedCapabilities[0] != "event_push_renders:1" {
		t.Fatalf("capability not accepted: %+v", ack)
	}
	b.handleDeliveryReport(holder, []byte(`{"op":"delivery_report","accepted":["channel","inbox"],"accepted_capabilities":["event_push_renders:1"],"live":{"channel":{"eligible":true,"event_push_renders":true},"inbox":{"eligible":false}}}`))
	event := callbackInbound(w.key)
	w.flushEvent(context.Background(), event)
	frame := waitInboundPush(t, frames)
	if frame.Inbound.Kind != c3types.InboundCallback || frame.Inbound.Event.Callback.Data != "choose" {
		t.Fatalf("event changed: %+v", frame)
	}
	if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 0 {
		t.Fatal("capable event queued")
	}
	holder.deliveryModes.Store(&ipc.DeliveryAcceptance{Version: 1, Modes: []string{"inbox"}})
	if eventPushRenders(holder) {
		t.Fatal("unaccepted channel cannot render despite live eligibility")
	}
}

func TestFlushEventRepeatedTaps(t *testing.T) {
	b, w, holder, frames := shadowFixture(t)
	holder.SetCannotRender(true)
	for i := 0; i < 2; i++ {
		w.flushEvent(context.Background(), callbackInbound(w.key))
	}
	rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if err != nil || len(rows) != 2 || rows[0].RecordID == rows[1].RecordID {
		t.Fatalf("distinct tap rows=%+v err=%v", rows, err)
	}
	if !hasHeldSource(w.key, rows) {
		t.Fatal("converted events must produce held notice")
	}
	if source := heldReplySource(w.key, rows); source == nil || *source != 3 {
		t.Fatalf("held reply source=%v", source)
	}
	result := make(chan FetchResult, 1)
	w.handleFetch(context.Background(), &FetchJob{Ack: true, All: true, Owner: holder, Lease: newFetchLease(), ResultCh: result})
	fetched := <-result
	if fetched.Err != nil || len(fetched.Messages) != 2 {
		t.Fatalf("fetch=%+v", fetched)
	}
	if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 0 {
		t.Fatal("fetch did not consume taps")
	}
	select {
	case f := <-frames:
		t.Fatalf("unexpected push: %+v", f)
	default:
	}
}

func TestFlushEventReservedCallbacks(t *testing.T) {
	for _, prefix := range []string{askCallbackPrefix, permCallbackPrefix, autoCallbackPrefix} {
		t.Run(prefix, func(t *testing.T) {
			b, w, holder, frames := shadowFixture(t)
			holder.SetCannotRender(true)
			event := callbackInbound(w.key)
			event.Event.Callback.Data = prefix + "missing"
			if prefix == askCallbackPrefix {
				b.Asks.register(&pendingAsk{askID: "pending", route: w.key, question: "Choose", options: []string{"Continue"}, messageID: 3, owner: holder})
				event.Event.Callback.Data = askCallbackPrefix + "pending:0"
			}
			w.flushEvent(context.Background(), event)
			if prefix == askCallbackPrefix && b.Asks.has("pending") {
				t.Fatal("ask did not resolve")
			}
			if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 0 {
				t.Fatal("reserved callback converted")
			}
			select {
			case f := <-frames:
				t.Fatalf("reserved push: %+v", f)
			case <-time.After(20 * time.Millisecond):
			}
		})
	}
}

func TestEventMessageText(t *testing.T) {
	actor := c3types.Sender{UserID: 7, Username: "operator"}
	for _, tc := range []struct {
		name    string
		kind    c3types.InboundKind
		payload c3types.InboundEvent
		want    string
	}{
		{"callback", c3types.InboundCallback, c3types.InboundEvent{Callback: &c3types.CallbackEvent{MessageID: 3, Actor: actor, Label: "Continue\nnow", Data: "yes\nplease"}}, `[button tap] @operator pressed "Continue\nnow" (data="yes\nplease") on message 3`},
		{"reaction", c3types.InboundReaction, c3types.InboundEvent{Reaction: &c3types.ReactionEvent{MessageID: 3, Actor: actor, Added: []string{"👍", "[custom]"}, Removed: []string{"👎"}}}, `[reaction] @operator added ["👍" "[custom]"] removed ["👎"] on message 3`},
		{"poll", c3types.InboundPollResult, c3types.InboundEvent{PollResult: &c3types.PollResult{PollID: "poll", Question: "Proceed?", TotalVoters: 4, IsClosed: true, Options: []c3types.PollOptionTally{{Text: "Yes", VoterCount: 3}, {Text: "No", VoterCount: 1}}}}, `[poll result] poll="poll" question="Proceed?" on message 3: options=["Yes"=3, "No"=1] total voters=4 closed=true`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, w, holder, _ := shadowFixture(t)
			holder.SetCannotRender(true)
			event := callbackInbound(w.key)
			event.Kind = tc.kind
			event.Event = &tc.payload
			w.flushEvent(context.Background(), event)
			rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
			if err != nil || len(rows) != 1 {
				t.Fatalf("rows=%+v err=%v", rows, err)
			}
			got := rows[0].Inbound
			if got.Text != tc.want || got.Sender != event.Sender || !got.Timestamp.Equal(event.Timestamp) || got.Channel != event.Channel || got.ChatID != event.ChatID || got.Kind != "" || got.Event != nil || got.ReplyTo.MessageID != 3 {
				t.Fatalf("converted=%+v want text=%q", got, tc.want)
			}
		})
	}
}

func TestFlushEventUnclaimedDropped(t *testing.T) {
	b, w, holder, _ := shadowFixture(t)
	b.Routes.Release(w.key, holder.ConnID)
	w.flushEvent(context.Background(), callbackInbound(w.key))
	if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 0 {
		t.Fatal("unclaimed event queued")
	}
	ch, _ := b.Channel("telegram")
	if got := len(ch.(*fakeChannel).sendRepliesSnapshot()); got != 0 {
		t.Fatal("unclaimed event bounced")
	}
}

func TestFlushEventPluginBeforeConversion(t *testing.T) {
	for _, drop := range []bool{false, true} {
		t.Run(map[bool]string{false: "transform", true: "drop"}[drop], func(t *testing.T) {
			b, w, holder, frames := shadowFixture(t)
			holder.SetCannotRender(true)
			calls := 0
			b.Plugins.OnInbound(func(_ context.Context, in *c3types.Inbound) (*c3types.Inbound, bool) {
				calls++
				if in.Kind != c3types.InboundCallback {
					t.Error("plugin received converted text")
				}
				in.Event.Callback.Label = "Transformed"
				return in, drop
			})
			w.flushEvent(context.Background(), callbackInbound(w.key))
			if calls != 1 {
				t.Fatalf("plugin calls=%d", calls)
			}
			rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
			want := 1
			if drop {
				want = 0
			}
			if err != nil || len(rows) != want {
				t.Fatalf("rows=%+v err=%v", rows, err)
			}
			if !drop && rows[0].Inbound.Text != `[button tap] @operator pressed "Transformed" (data="choose") on message 3` {
				t.Fatalf("conversion ignored plugin: %+v", rows[0])
			}
			select {
			case f := <-frames:
				t.Fatalf("unexpected push: %+v", f)
			default:
			}
		})
	}
}

func TestFlushEventAppendFailureNotice(t *testing.T) {
	b, w, holder, frames := shadowFixture(t)
	holder.SetCannotRender(true)
	brokenDir := t.TempDir()
	brokenStore, err := queue.NewStore(brokenDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(brokenDir); err != nil {
		t.Fatal(err)
	}
	b.Queue = brokenStore
	persisted, failed := 0, 0
	b.SetPersistedCallback("telegram", func(*c3types.Inbound) { persisted++ })
	b.SetPersistFailedCallback("telegram", func(*c3types.Inbound) { failed++ })
	w.flushEvent(context.Background(), callbackInbound(w.key))
	if persisted != 0 || failed != 0 {
		t.Fatalf("event must not claim a message offset seam: persisted=%d failed=%d", persisted, failed)
	}
	ch, _ := b.Channel("telegram")
	replies := ch.(*fakeChannel).sendRepliesSnapshot()
	if len(replies) != 1 || !strings.Contains(replies[0].Text, "delivery is unconfirmed; durable fallback failed") || !strings.Contains(replies[0].Text, "may not be redelivered") || strings.Contains(replies[0].Text, "NOT lost") {
		t.Fatalf("failure notice=%+v", replies)
	}
	select {
	case f := <-frames:
		t.Fatalf("unexpected push: %+v", f)
	default:
	}
}

func TestFlushEventNilQueueFailureNotice(t *testing.T) {
	b, w, holder, frames := shadowFixture(t)
	holder.SetCannotRender(true)
	b.Queue = nil
	var logs matrixLogBuffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)
	for i := 0; i < 2; i++ {
		w.flushEvent(context.Background(), callbackInbound(w.key))
	}
	ch, _ := b.Channel("telegram")
	replies := ch.(*fakeChannel).sendRepliesSnapshot()
	if len(replies) != 1 || !strings.Contains(replies[0].Text, "delivery is unconfirmed; durable fallback failed") {
		t.Fatalf("cooldown-gated event failure notice=%+v", replies)
	}
	if strings.Count(logs.text(), "event persist FAIL") != 2 || !strings.Contains(logs.text(), "automatic redelivery is not guaranteed") {
		t.Fatalf("missing truthful storage failure logs: %s", logs.text())
	}
	select {
	case frame := <-frames:
		t.Fatalf("unexpected push: %+v", frame)
	default:
	}
}

func TestFlushEventDispatchCapabilityChanges(t *testing.T) {
	for _, change := range []string{"holder_transfer", "legacy_downgrade", "negotiated_downgrade"} {
		t.Run(change, func(t *testing.T) {
			b, w, holder, frames := shadowFixture(t)
			w.Stop()
			w = &RouteWorker{key: w.key, broker: b}
			t.Cleanup(func() {
				if w.attemptTick != nil {
					w.attemptTick.Stop()
				}
				if w.attemptWriteCancel != nil {
					w.attemptWriteCancel()
				}
			})
			if change == "negotiated_downgrade" {
				holder.eventPushFactsActive.Store(true)
				b.configureDelivery(holder, []byte(eventChannelOffer))
				b.handleDeliveryReport(holder, []byte(`{"op":"delivery_report","accepted":["channel","inbox"]}`))
			}
			calls, changes := 0, 0
			b.Plugins.OnInbound(func(_ context.Context, in *c3types.Inbound) (*c3types.Inbound, bool) {
				calls++
				return in, false
			})
			w.eventDispatchTestHook = func(stage string) {
				if change == "holder_transfer" && stage == "before_holder" {
					replacement := &Stub{ConnID: holder.ConnID + 1, CLI: holder.CLI, PID: holder.PID, Conn: holder.ConnValue()}
					replacement.SetCannotRender(true)
					if routes := b.Routes.TransferAllByConnID(holder.ConnID, replacement); len(routes) != 1 {
						t.Fatalf("transfer routes=%v", routes)
					}
					changes++
				} else if change != "holder_transfer" && stage == "before_write" {
					if change == "negotiated_downgrade" {
						holder.delivery.mu.Lock()
						holder.delivery.live.Channel.Eligible = false
						holder.delivery.live.Inbox.Eligible = false
						holder.delivery.mu.Unlock()
					} else {
						holder.SetCannotRender(true)
					}
					changes++
				}
			}
			w.flushEvent(context.Background(), callbackInbound(w.key))
			if calls != 1 || changes != 1 {
				t.Fatalf("plugin calls=%d changes=%d", calls, changes)
			}
			rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
			if err != nil || len(rows) != 1 || rows[0].Inbound.IsEvent() || !strings.Contains(rows[0].Inbound.Text, "[button tap]") {
				t.Fatalf("converted rows=%+v err=%v", rows, err)
			}
			select {
			case frame := <-frames:
				t.Fatalf("unexpected push: %+v", frame)
			default:
			}
		})
	}
}

// eventBlockedConn holds WriteJSON inside the socket write until the test has
// processed a capability report. The IPC writer itself remains unchanged.
type eventBlockedConn struct {
	net.Conn
	entered  chan struct{}
	release  chan struct{}
	writeErr error
	once     sync.Once
}

func (c *eventBlockedConn) Write(p []byte) (int, error) {
	c.once.Do(func() {
		close(c.entered)
		<-c.release
	})
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(p), nil
}

func TestFlushEventCapabilityDuringWrite(t *testing.T) {
	for _, negotiated := range []bool{false, true} {
		for _, downgrade := range []bool{false, true} {
			for _, writeFails := range []bool{false, true} {
				name := fmt.Sprintf("negotiated=%t/downgrade=%t/write_error=%t", negotiated, downgrade, writeFails)
				t.Run(name, func(t *testing.T) {
					b, original, holder, _ := shadowFixture(t)
					b.Workers.Stop()
					original.Stop()
					w := &RouteWorker{key: original.key, broker: b}
					t.Cleanup(func() {
						if w.attemptTick != nil {
							w.attemptTick.Stop()
						}
						if w.attemptWriteCancel != nil {
							w.attemptWriteCancel()
						}
					})
					if negotiated {
						holder.eventPushFactsActive.Store(true)
						b.configureDelivery(holder, []byte(eventChannelOffer))
						b.handleDeliveryReport(holder, []byte(`{"op":"delivery_report","accepted":["channel","inbox"]}`))
					}
					left, right := net.Pipe()
					defer left.Close()
					defer right.Close()
					blocked := &eventBlockedConn{Conn: left, entered: make(chan struct{}), release: make(chan struct{})}
					if writeFails {
						blocked.writeErr = io.ErrClosedPipe
					}
					holder.Reattach(ipc.NewConn(blocked), holder.ConnID)
					var release sync.Once
					unblock := func() { release.Do(func() { close(blocked.release) }) }
					defer unblock()
					var logs matrixLogBuffer
					previous := log.Writer()
					log.SetOutput(&logs)
					defer log.SetOutput(previous)
					done := make(chan struct{})
					go func() {
						defer close(done)
						w.flushEvent(context.Background(), callbackInbound(w.key))
					}()
					select {
					case <-blocked.entered:
					case <-time.After(5 * time.Second):
						t.Fatal("event never entered socket write")
					}
					if downgrade {
						if negotiated {
							b.handleDeliveryReport(holder, []byte(`{"op":"delivery_report","live":{"channel":{"eligible":false},"inbox":{"eligible":false}}}`))
						} else {
							holder.SetCannotRender(true)
						}
						if eventPushRenders(holder) {
							t.Fatal("downgrade did not disable rendering")
						}
					}
					unblock()
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("event write did not finish")
					}
					rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
					want := 0
					if downgrade || writeFails {
						want = 1
					}
					if err != nil || len(rows) != want {
						t.Fatalf("rows=%+v err=%v want=%d", rows, err, want)
					}
					if (downgrade || writeFails) && (rows[0].Inbound.IsEvent() || !strings.Contains(rows[0].Inbound.Text, "[button tap]")) {
						t.Fatalf("not a converted event: %+v", rows[0])
					}
					if got := strings.Contains(logs.text(), "reason=write_failed_or_capability_changed"); got != (downgrade || writeFails) {
						t.Fatalf("post-write conversion log=%t want=%t: %s", got, downgrade || writeFails, logs.text())
					}
				})
			}
		}
	}
}

func TestFlushEventNegotiatedProbing(t *testing.T) {
	for _, history := range []string{"", "channel", "inbox", "report_downgrade"} {
		t.Run(history, func(t *testing.T) {
			b, w, holder, frames, ctx := negotiatedFixture(t)
			if history == "report_downgrade" {
				holder.eventPushFactsActive.Store(true)
				b.handleDeliveryReport(holder, []byte(`{"op":"delivery_report","live":{"channel":{"eligible":true,"event_push_renders":true},"inbox":{"eligible":false}}}`))
				if !eventPushRenders(holder) {
					t.Fatal("capable report ignored")
				}
				b.handleDeliveryReport(holder, []byte(`{"op":"delivery_report","live":{"channel":{"eligible":true},"inbox":{"eligible":false}}}`))
			}
			holder.delivery.mu.Lock()
			if history == "channel" || history == "inbox" {
				holder.delivery.proven = true
				holder.delivery.route(w.key).Confirmed = time.Now()
				holder.delivery.route(w.key).Transport = history
			}
			holder.delivery.mu.Unlock()
			w.flushEvent(ctx, callbackInbound(w.key))
			frame := nextDeliver(t, frames)
			rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
			if err != nil || len(rows) != 1 || rows[0].Inbound.IsEvent() || frame.Inbound.IsEvent() || !strings.Contains(frame.Inbound.Text, "[button tap]") {
				t.Fatalf("probing event not converted: frame=%+v rows=%+v err=%v", frame, rows, err)
			}
			w.handleAttemptResult(resultFor(holder, frame, "confirmed"))
			if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 0 {
				t.Fatalf("confirmation left %d rows", n)
			}
		})
	}
}

func TestFlushEventNegotiatedPullOnlyReceipt(t *testing.T) {
	b, w, holder, frames, ctx := negotiatedFixture(t)
	receiptHolder(holder)
	w.flushEvent(ctx, callbackInbound(w.key))
	rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if err != nil || len(rows) != 1 || rows[0].Inbound.IsEvent() || !hasHeldSource(w.key, rows) {
		t.Fatalf("held rows=%+v err=%v", rows, err)
	}
	select {
	case f := <-frames:
		t.Fatalf("unexpected push: %+v", f)
	default:
	}
	group := &attemptFetchGroup{token: b.mintDeliveryToken()}
	fetched := reserveFetch(t, w, holder, group, -1)
	if len(fetched.Messages) != 1 || len(fetched.Members) != 1 {
		t.Fatalf("fetch=%+v", fetched)
	}
	if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 1 {
		t.Fatalf("fetch retired before receipt: %d", n)
	}
	w.handleAttemptResult(fetchResult(holder, group.token))
	if n, _ := b.Queue.Pending(queueRouteKey(w.key)); n != 0 {
		t.Fatalf("receipt left %d rows", n)
	}
}

func TestFlushEventDisconnectedHolder(t *testing.T) {
	for _, negotiated := range []bool{false, true} {
		t.Run(fmt.Sprint(negotiated), func(t *testing.T) {
			b, w, holder, frames := shadowFixture(t)
			b.Workers.Stop()
			w.Stop()
			w = &RouteWorker{key: w.key, broker: b}
			t.Cleanup(func() {
				if w.attemptTick != nil {
					w.attemptTick.Stop()
				}
				if w.attemptWriteCancel != nil {
					w.attemptWriteCancel()
				}
			})
			if negotiated {
				holder.eventPushFactsActive.Store(true)
				b.configureDelivery(holder, []byte(eventChannelOffer))
				b.handleDeliveryReport(holder, []byte(`{"op":"delivery_report","accepted":["channel","inbox"]}`))
			}
			holder.MarkDisconnected()
			w.flushEvent(context.Background(), callbackInbound(w.key))
			rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
			if err != nil || len(rows) != 1 || rows[0].Inbound.IsEvent() || !strings.Contains(rows[0].Inbound.Text, "[button tap]") {
				t.Fatalf("rows=%+v err=%v", rows, err)
			}
			if got, claimed := b.Routes.Holder(w.key); !claimed || got != holder {
				t.Fatal("disconnected holder lost its claim")
			}
			select {
			case f := <-frames:
				t.Fatalf("unexpected push: %+v", f)
			default:
			}
		})
	}
}

func TestFlushEventPostWriteAppendFailure(t *testing.T) {
	b, w, holder, _ := shadowFixture(t)
	b.Workers.Stop()
	w.Stop()
	w = &RouteWorker{key: w.key, broker: b}
	brokenDir := t.TempDir()
	brokenStore, err := queue.NewStore(brokenDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(brokenDir); err != nil {
		t.Fatal(err)
	}
	b.Queue = brokenStore
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	blocked := &eventBlockedConn{Conn: left, entered: make(chan struct{}), release: make(chan struct{})}
	holder.Reattach(ipc.NewConn(blocked), holder.ConnID)
	var logs matrixLogBuffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)
	done := make(chan struct{})
	go func() { defer close(done); w.flushEvent(context.Background(), callbackInbound(w.key)) }()
	var once sync.Once
	unblock := func() { once.Do(func() { close(blocked.release) }) }
	defer unblock()
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("event never entered write")
	}
	holder.SetCannotRender(true)
	unblock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("event write did not finish")
	}
	ch, _ := b.Channel("telegram")
	replies := ch.(*fakeChannel).sendRepliesSnapshot()
	if len(replies) != 1 || !strings.Contains(replies[0].Text, "delivery is unconfirmed; durable fallback failed") || strings.Contains(replies[0].Text, "was not delivered") {
		t.Fatalf("notice=%+v", replies)
	}
	if !strings.Contains(logs.text(), "delivery is unconfirmed; durable fallback failed") || strings.Contains(logs.text(), "was not delivered") {
		t.Fatalf("logs=%s", logs.text())
	}
}
