package broker

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

// Degraded mode is Broker.Queue == nil: queue.NewStore failed at startup and
// broker.go New() chose to keep running without a durable queue. In that mode
// flushInbounds still calls markPersisted on every inbound — it must, or the
// source update_id wedges in-flight forever and ALL inbound re-polls forever —
// so the update is acked to Telegram with nothing written anywhere, and Telegram
// never redelivers an acked update. Everything arriving with no session attached
// is destroyed for the whole run.
//
// That trade ("lose it" over "wedge the bridge") stands. What these tests pin is
// that it is never SILENT: the operator is told at startup, on `/status`, and —
// on the exact path that destroys the message — by a warning instead of the
// "nothing lost" reassurance.

// degradeQueueDir points C3_QUEUE_DIR at a path whose parent is a regular FILE,
// so queue.NewStore's os.MkdirAll fails with ENOTDIR. That is a real instance of
// the trigger New() catches (unwritable dir / wrong permissions / occupied
// path). Nothing is faked by assigning b.Queue = nil, so every test below runs
// the production degrade end to end.
func degradeQueueDir(t *testing.T) {
	t.Helper()
	blocker := filepath.Join(t.TempDir(), "queue-parent-is-a-file")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("seeding the blocker file failed: %v", err)
	}
	t.Setenv("C3_QUEUE_DIR", filepath.Join(blocker, "queue"))
}

// degradedBrokerWithChannel is brokerWithChannel with a queue that could not be
// opened. It asserts the degrade actually happened — otherwise these tests would
// silently pass while exercising the healthy path.
func degradedBrokerWithChannel(t *testing.T, fc *fakeChannel) *Broker {
	t.Helper()
	degradeQueueDir(t)
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	if b.Queue != nil {
		t.Fatalf("setup did not degrade: a queue opened at %q, so this test would pass without ever exercising the degraded path", queue.QueueDir())
	}
	return b
}

// unclaimedWorker returns a worker for a route no session holds — the path that
// destroys the message in degraded mode.
func unclaimedWorker(t *testing.T, b *Broker, key RouteKey) *RouteWorker {
	t.Helper()
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	t.Cleanup(w.Stop)
	return w
}

func heldInbound(msgID int64) []*c3types.Inbound {
	return []*c3types.Inbound{{
		Channel: "telegram", ChatID: -100, MessageID: msgID,
		Text: "deploy the hotfix before standup", Timestamp: time.Now(),
	}}
}

// TestDegradedMode_HeldNoticeWarnsInsteadOfSayingNothingLost pins the auto-reply
// on the losing path itself (flushInbounds → forwardOrFallbackCovering → no
// claim), not the text helper: reverting the one call-site line that picks
// heldDegradedText brings the whole lie back, and this fails.
//
// Both channel shapes are covered because the held-notice branches on
// EditMessages, and only one of those two branches is the one Telegram takes.
func TestDegradedMode_HeldNoticeWarnsInsteadOfSayingNothingLost(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit bool
	}{
		{"edit-capable channel (Telegram)", true},
		{"channel without message edits", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := &fakeChannel{editMessages: tc.edit}
			b := degradedBrokerWithChannel(t, fc)
			defer b.Shutdown()
			w := unclaimedWorker(t, b, RouteKey{Channel: "telegram", ChatID: -100})

			w.flushInbounds(context.Background(), heldInbound(7))

			replies := fc.sendRepliesSnapshot()
			if len(replies) != 1 {
				t.Fatalf("a message destroyed by degraded mode produced %d auto-replies; want exactly 1 warning telling the operator it is gone", len(replies))
			}
			got := replies[0].Text
			if strings.Contains(got, "nothing lost") {
				t.Fatalf("the auto-reply still promises \"nothing lost\" while the durable queue is DISABLED — this message has already been acked to Telegram, written nowhere, and is unrecoverable. Reply was:\n%s", got)
			}
			if !strings.Contains(got, queueDisabledWarning) {
				t.Fatalf("the auto-reply never says the durable queue is disabled, so the operator has no way to know their message was dropped. Reply was:\n%s", got)
			}
		})
	}
}

// TestDegradedMode_WarningIsOnTheFiveMinuteCooldown pins the cadence choice: the
// degraded warning takes the long Fallbacks cooldown even on an edit-capable
// channel, where the healthy held-notice uses the 10s HeldNotices debounce.
// Deleting the `!degraded &&` guard puts the warning back on the 10s window and
// this fails.
func TestDegradedMode_WarningIsOnTheFiveMinuteCooldown(t *testing.T) {
	fc := &fakeChannel{editMessages: true} // the branch that would use the 10s debounce
	b := degradedBrokerWithChannel(t, fc)
	defer b.Shutdown()
	key := RouteKey{Channel: "telegram", ChatID: -100}
	w := unclaimedWorker(t, b, key)

	// Consume the leading edge of THIS route's 5-minute cooldown so the hold
	// below lands inside the window.
	if !b.Fallbacks.ShouldSend(key) {
		t.Fatal("setup: a fresh route's fallback cooldown should be open")
	}

	w.flushInbounds(context.Background(), heldInbound(8))

	if n := len(fc.sendRepliesSnapshot()); n != 0 {
		t.Fatalf("%d lost-message warning(s) fired inside the 5-minute cooldown: the degraded warning is riding the 10s held-notice debounce, so a burst repeats it six times a minute and the operator learns to ignore the one alert that means data is being destroyed", n)
	}
}

// TestHealthyQueue_HeldNoticeKeepsThePerMessageDebounce is the control for the
// test above: with a working queue the same edit-capable channel is NOT gated by
// the 5-minute cooldown, so that test observes a real tracker switch rather than
// a cooldown both modes happened to share.
func TestHealthyQueue_HeldNoticeKeepsThePerMessageDebounce(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{editMessages: true}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()
	key := RouteKey{Channel: "telegram", ChatID: -100}
	w := unclaimedWorker(t, b, key)

	b.Fallbacks.ShouldSend(key) // close the 5-minute window; healthy mode must not care

	w.flushInbounds(context.Background(), heldInbound(9))

	if n := len(fc.sendRepliesSnapshot()); n != 1 {
		t.Fatalf("healthy-mode held-notice sent %d replies, want 1: it must stay on the 10s HeldNotices debounce so each safely-queued message still re-notifies, not inherit the degraded warning's 5-minute cooldown", n)
	}
}

// TestHealthyQueue_HeldNoticeStillSaysNothingLost guards the other direction: the
// reassurance must survive for the mode where it is TRUE. It also checks the
// message really is on disk, so the reassurance is earned rather than asserted.
func TestHealthyQueue_HeldNoticeStillSaysNothingLost(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{editMessages: true}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()
	key := RouteKey{Channel: "telegram", ChatID: -100}
	w := unclaimedWorker(t, b, key)

	w.flushInbounds(context.Background(), heldInbound(10))

	if n, _ := b.Queue.Pending(queueRouteKey(key)); n != 1 {
		t.Fatalf("a working queue holds %d messages after one hold, want 1 — the reassurance below would be unearned", n)
	}
	replies := fc.sendRepliesSnapshot()
	if len(replies) != 1 {
		t.Fatalf("held-notice count = %d, want 1", len(replies))
	}
	got := replies[0].Text
	if !strings.Contains(got, "Held — nothing lost") || !strings.Contains(got, "1 message queued") {
		t.Fatalf("the durable queue is working and the message is on disk, but the operator was not reassured — the degraded warning has been applied to the healthy path too. Reply was:\n%s", got)
	}
	if strings.Contains(got, queueDisabledWarning) {
		t.Fatalf("a healthy broker warned that its queue is DISABLED — a false alarm on every held message trains the operator to ignore the real one. Reply was:\n%s", got)
	}
}

// TestRegisterChannel_AnnouncesDegradedQueueAtStartup pins the startup alert on
// both legs it is supposed to reuse: the broker-originated system-event
// broadcast to live CLI sessions, and the Telegram send to the operator's DM.
// Deleting the announceQueueDegraded call — or either leg of it — fails here.
func TestRegisterChannel_AnnouncesDegradedQueueAtStartup(t *testing.T) {
	degradeQueueDir(t)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	b := New(mfWithTelegram())
	defer b.Shutdown()
	if b.Queue != nil {
		t.Fatalf("setup did not degrade: a queue opened at %q", queue.QueueDir())
	}

	// A live CLI session, so the system-event leg has a real recipient.
	agentSide, brokerSide := net.Pipe()
	t.Cleanup(func() { agentSide.Close(); brokerSide.Close() })
	agentConn := ipc.NewConn(agentSide)
	b.Stubs.Register("claude", 4242, "/work", ipc.NewConn(brokerSide))

	// RegisterChannel writes to that unbuffered pipe, so it must not run on the
	// goroutine that has to drain it.
	fc := &fakeChannel{}
	errc := make(chan error, 1)
	go func() { errc <- b.RegisterChannel(fc) }()

	msg, ok := readBroadcastWithin(agentConn, 2*time.Second)
	if !ok {
		t.Fatal("the broker came up with its durable queue DISABLED and told no live session — the degrade stays silent until messages start disappearing")
	}
	if msg.Inbound.Kind != c3types.InboundSystem || msg.Inbound.Event == nil || msg.Inbound.Event.System == nil {
		t.Fatalf("startup advisory is not a broker-originated system event: %+v", msg.Inbound)
	}
	if !strings.Contains(msg.Inbound.Event.System.Message, queueDisabledWarning) {
		t.Fatalf("the session-facing advisory does not say the durable queue is disabled, so an agent still believes fetch_queue can recover held messages. Message was:\n%s", msg.Inbound.Event.System.Message)
	}
	if err := <-errc; err != nil {
		t.Fatalf("RegisterChannel: %v", err)
	}

	replies := fc.sendRepliesSnapshot()
	if len(replies) != 1 {
		t.Fatalf("startup sent %d Telegram notices, want exactly 1 to the operator's DM: the human who has to fix the queue directory is not at a CLI", len(replies))
	}
	if replies[0].ChatID != 42 { // mfWithTelegram's dm_chat_id
		t.Fatalf("degraded-mode notice went to chat %d, want the operator DM (42) — a startup warning fanned across project topics is one people mute", replies[0].ChatID)
	}
	if !strings.Contains(replies[0].Text, queueDisabledWarning) {
		t.Fatalf("the operator DM does not say the durable queue is disabled. Text was:\n%s", replies[0].Text)
	}
}

// TestRegisterChannel_HealthyStartupAnnouncesNothing: a warning that also fires
// when nothing is wrong is a warning the operator mutes.
func TestRegisterChannel_HealthyStartupAnnouncesNothing(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	b := New(mfWithTelegram())
	defer b.Shutdown()
	fc := &fakeChannel{}
	if err := b.RegisterChannel(fc); err != nil {
		t.Fatalf("RegisterChannel: %v", err)
	}
	if n := len(fc.sendRepliesSnapshot()); n != 0 {
		t.Fatalf("a normal startup with a working queue DM'd the operator %d time(s); the degraded warning must fire only when the queue is actually disabled", n)
	}
}

// TestRegisterChannel_DegradedWithNoOperatorDMSendsNothing: with no dm_chat_id
// configured there is no operator DM to warn, and chat 0 is not a chat.
func TestRegisterChannel_DegradedWithNoOperatorDMSendsNothing(t *testing.T) {
	degradeQueueDir(t)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	mf := mfWithTelegram()
	cc := mf.Channels["telegram"]
	cc.DMChatID = 0
	mf.Channels["telegram"] = cc

	b := New(mf)
	defer b.Shutdown()
	fc := &fakeChannel{}
	if err := b.RegisterChannel(fc); err != nil {
		t.Fatalf("RegisterChannel: %v", err)
	}
	if n := len(fc.sendRepliesSnapshot()); n != 0 {
		t.Fatalf("sent %d degraded-mode notice(s) with no operator DM configured — that addresses chat 0, which is not a chat", n)
	}
}

// TestStatus_ReportsDegradedQueue: the held-notice tells the operator to "Send
// /status to check", so /status is what they will look at. Both renderings must
// carry the warning — without it the topic view reads "0 queued · broker up" and
// the global view reads the single word "Broker up", which are the two most
// reassuring things C3 can say at the moment they are least true.
func TestStatus_ReportsDegradedQueue(t *testing.T) {
	fc := &fakeChannel{}
	b := degradedBrokerWithChannel(t, fc)
	defer b.Shutdown()
	host := NewBrokerHost(b, "telegram")
	tid := int64(281)

	topicReply, handled := host.HandleCommand(&c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, Text: "/status"})
	if !handled {
		t.Fatal("/status in a topic should be handled")
	}
	if !strings.Contains(topicReply, queueDisabledWarning) {
		t.Fatalf("/status in a topic reads as a healthy broker with an empty queue, when in fact nothing CAN be queued and every held message is being destroyed. Reply was:\n%s", topicReply)
	}

	dmReply, handled := host.HandleCommand(&c3types.Inbound{Channel: "telegram", ChatID: 42, Text: "/status"})
	if !handled {
		t.Fatal("/status in DM should be handled")
	}
	if !strings.Contains(dmReply, queueDisabledWarning) {
		t.Fatalf("the global /status summary never mentions that the durable queue is disabled. Reply was:\n%s", dmReply)
	}
}

// TestHealthList_DegradedQueueStateCannotDisappear pins the running broker's
// contribution to `c3-broker status`. Removing QueueDegraded from handleHealth
// makes the CLI process indistinguishable from an old/healthy broker, so the
// persistent warning disappears even though Queue is nil.
func TestHealthList_DegradedQueueStateCannotDisappear(t *testing.T) {
	degradeQueueDir(t)
	b := New(mfWithTelegram())
	defer b.Shutdown()
	if b.Queue != nil {
		t.Fatal("setup did not trigger queue.NewStore startup failure; health_list test would be vacuous")
	}
	got, raw := readHealthList(t, b)
	if !got.QueueDegraded {
		t.Fatalf("health_list omitted queue_degraded=true while Broker.Queue is nil, so c3-broker status cannot persist the startup failure. Frame: %s", raw)
	}
}

func TestHealthList_HealthyQueueDoesNotCryWolf(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(mfWithTelegram())
	defer b.Shutdown()
	if b.Queue == nil {
		t.Fatal("setup unexpectedly disabled the durable queue; healthy control would be vacuous")
	}
	got, raw := readHealthList(t, b)
	if got.QueueDegraded {
		t.Fatalf("health_list reports queue_degraded=true while the durable queue is open, training operators to ignore the real warning. Frame: %s", raw)
	}
}

func readHealthList(t *testing.T, b *Broker) (ipc.HealthListMsg, []byte) {
	t.Helper()
	clientSide, brokerSide := net.Pipe()
	defer clientSide.Close()
	defer brokerSide.Close()
	clientConn := ipc.NewConn(clientSide)
	go b.handleHealth(ipc.NewConn(brokerSide))

	raw, err := clientConn.ReadFrame()
	if err != nil {
		t.Fatalf("read health_list: %v", err)
	}
	var got ipc.HealthListMsg
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode health_list: %v", err)
	}
	return got, raw
}

// TestDocsQuoteTheRealNotices holds the public docs to the claim README.md makes
// about itself — "Those are the strings C3 actually renders, copied out of the
// code — not a mock-up." A reassurance that has quietly stopped matching the code
// is the exact defect this whole file exists for, so the promise is enforced
// rather than maintained by hand.
func TestDocsQuoteTheRealNotices(t *testing.T) {
	for _, doc := range []struct {
		path   string
		quotes []string
	}{
		{"../../README.md", []string{heldReplyText(1), heldDegradedText()}},
		// The log substring matters as much as the notices: USAGE.md tells an
		// operator what to grep broker.log for, and a paraphrase there sends
		// them looking for a line C3 never writes. Quote it, don't describe it.
		{"../../docs/USAGE.md", []string{
			"⚠️ NOT held — that message was dropped.",
			"📨 Held — nothing lost.",
			degradedDropLogPhrase,
		}},
	} {
		body, err := os.ReadFile(doc.path)
		if err != nil {
			t.Fatalf("read %s: %v", doc.path, err)
		}
		for _, q := range doc.quotes {
			if !strings.Contains(string(body), q) {
				t.Fatalf("%s no longer contains the notice C3 actually sends, so the docs promise users something the broker does not say. Missing:\n%s", doc.path, q)
			}
		}
	}
}

// TestStatus_HealthyQueueHasNoDegradedWarning is the control: /status must not
// cry wolf while the queue is fine.
func TestStatus_HealthyQueueHasNoDegradedWarning(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTelegram(), fc)
	defer b.Shutdown()
	host := NewBrokerHost(b, "telegram")
	tid := int64(281)

	topicReply, _ := host.HandleCommand(&c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, Text: "/status"})
	dmReply, _ := host.HandleCommand(&c3types.Inbound{Channel: "telegram", ChatID: 42, Text: "/status"})
	for _, got := range []string{topicReply, dmReply} {
		if strings.Contains(got, queueDisabledWarning) {
			t.Fatalf("a healthy broker's /status warns that its durable queue is DISABLED. Reply was:\n%s", got)
		}
	}
}
