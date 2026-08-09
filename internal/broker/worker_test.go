package broker

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/mappings"
)

// TestMain isolates the durable queue to a throwaway dir for the whole broker
// test package, so any test that drives a message through the delivery pipeline
// (flushInbounds/Emit/forwardOrFallback now persist to disk) never writes to the
// user's real ~/.local/state/c3/queue. Individual tests may still override with
// t.Setenv("C3_QUEUE_DIR", t.TempDir()) for a fresh per-test dir.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "c3-broker-queue-test")
	if err != nil {
		panic(err)
	}
	_ = os.Setenv("C3_QUEUE_DIR", dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func TestWorker_IdleShutdown(t *testing.T) {
	w := newRouteWorker(context.Background(), RouteKey{Channel: "x"}, 50*time.Millisecond, nil)
	select {
	case <-w.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker did not exit on idle within 500ms")
	}
}

func TestWorker_StopExits(t *testing.T) {
	w := newRouteWorker(context.Background(), RouteKey{Channel: "x"}, time.Hour, nil)
	go w.Stop()
	select {
	case <-w.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker did not exit on Stop")
	}
}

func TestWorker_ReleaseJobExits(t *testing.T) {
	w := newRouteWorker(context.Background(), RouteKey{Channel: "x"}, time.Hour, nil)
	if !w.Submit(Job{Kind: JobRelease}) {
		t.Fatal("Submit should succeed before stop")
	}
	select {
	case <-w.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker did not exit on JobRelease")
	}
}

func TestWorker_SubmitAfterStopReturnsFalse(t *testing.T) {
	w := newRouteWorker(context.Background(), RouteKey{Channel: "x"}, time.Hour, nil)
	w.Stop()
	if w.Submit(Job{Kind: JobInbound}) {
		t.Error("Submit after Stop should return false")
	}
}

// P2-5: when the audio was downloaded before STT failed, the recovery text names
// the cached path (recovery is a no-brainer); with no cached path the text is the
// same actionable guidance, minus the caching clause.
func TestSttFailureText_NamesCachedPathWhenPresent(t *testing.T) {
	att := c3types.Attachment{FileID: "F1", MIME: "audio/ogg", Kind: "voice"}
	with := sttFailureText(att, "timeout", "/inbox/123-F1.oga")
	if !strings.Contains(with, "/inbox/123-F1.oga") || !strings.Contains(with, "cached locally") {
		t.Fatalf("cached path not named: %q", with)
	}
	without := sttFailureText(att, "timeout", "")
	if strings.Contains(without, "cached locally") {
		t.Fatalf("no-cache text must not mention caching: %q", without)
	}
	for _, want := range []string{"download_attachment", "retranscribe", "F1"} {
		if !strings.Contains(without, want) {
			t.Fatalf("recovery guidance missing %q: %q", want, without)
		}
	}
}

// Regression for the 2026-08-07 msg-8881 5-minute blackout: a worker that exits
// with an un-started JobInbound still in its queue must NOT drop it silently. The
// drain recovers it via the persist-FAILED callback (offset held, poll-side dedup
// forgotten so Telegram's redelivery re-dispatches) — never via the persisted
// callback, which would advance the offset and lose the message.
func TestShutdown_DroppedInboundRecoversViaPersistFailed(t *testing.T) {
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	var mu sync.Mutex
	var failed, persisted []int64
	b.SetPersistFailedCallback(func(in *c3types.Inbound) {
		mu.Lock()
		failed = append(failed, in.MessageID)
		mu.Unlock()
	})
	b.SetPersistedCallback(func(in *c3types.Inbound) {
		mu.Lock()
		persisted = append(persisted, in.MessageID)
		mu.Unlock()
	})

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	w.Stop() // run() exits; the (never-closed) buffered queue channel stays usable.

	// Simulate an inbound that Emit enqueued (Submit returned true) but the worker
	// exited before starting it — put it directly on the queue, then drain via the
	// idempotent shutdown().
	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 8881,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "v"}},
	}
	w.queue <- Job{Kind: JobInbound, Inbound: in}
	w.shutdown()

	mu.Lock()
	defer mu.Unlock()
	if len(failed) != 1 || failed[0] != 8881 {
		t.Fatalf("dropped inbound not recovered via persist-failed: got %v, want [8881]", failed)
	}
	if len(persisted) != 0 {
		t.Fatalf("dropped inbound must NOT advance the offset: persisted callback fired for %v", persisted)
	}
}

// A dropped EVENT inbound needs no persist-failed recovery — events are never
// persisted (no seam; the offset was already marked done in dispatchUpdate).
func TestShutdown_DroppedEventNotRecovered(t *testing.T) {
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	var mu sync.Mutex
	var failed []int64
	b.SetPersistFailedCallback(func(in *c3types.Inbound) {
		mu.Lock()
		failed = append(failed, in.MessageID)
		mu.Unlock()
	})

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	w.Stop()

	event := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 2200,
		Kind:  c3types.InboundPollResult,
		Event: &c3types.InboundEvent{PollResult: &c3types.PollResult{PollID: "p", IsClosed: true}},
	}
	w.queue <- Job{Kind: JobInbound, Inbound: event}
	w.shutdown()

	mu.Lock()
	defer mu.Unlock()
	if len(failed) != 0 {
		t.Fatalf("dropped event must not trigger persist-failed recovery: got %v", failed)
	}
}

// Regression test for 2026-05-14: when the STT handler script went missing,
// the plugin silently disabled itself at startup and voice messages reached
// the agent as a bare "(voice message)" with no indication anything was
// wrong. The broker now surfaces a self-documenting recovery message: the
// agent learns the audio exists, the exact file_id, and that it can fetch
// (download_attachment) or retry (retranscribe) without the user resending.
func TestFlushInbounds_VoiceWithoutSTTPluginGetsSelfDocumentingFailure(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 42,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "VFILE", MIME: "audio/ogg", Size: 1000}},
	}
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	for _, want := range []string{"transcription failed", "VFILE", "download_attachment", "retranscribe", "does not need to resend"} {
		if !strings.Contains(in.Text, want) {
			t.Errorf("STT failure text missing %q; got %q", want, in.Text)
		}
	}
}

func TestFlushInbounds_VoiceWithCaptionKeepsCaptionWhenSTTAbsent(t *testing.T) {
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	in := &c3types.Inbound{
		Channel:     "telegram",
		ChatID:      -100,
		MessageID:   43,
		Text:        "user-typed caption",
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "v1"}},
	}
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	// The original rule here was "don't clobber user-deliberate text with the
	// marker", and it was enforced by writing NO marker at all whenever text was
	// present. That does not compose past a single voice note: a rich message
	// always carries block-marker text, so the failure was never stated, and a
	// second voice that transcribed then masked the human notice too — the
	// failure became invisible on both surfaces (Codex review 3, finding 2).
	// Appending honors the rule as it was meant: the caption is untouched AND
	// the agent is told what happened.
	if !strings.HasPrefix(in.Text, "user-typed caption") {
		t.Errorf("voice with caption but no STT plugin: in.Text=%q, want the caption preserved and first (don't clobber user-deliberate text)", in.Text)
	}
	if !strings.Contains(in.Text, "transcription failed") {
		t.Errorf("voice with caption but no STT plugin: in.Text=%q, want the failure appended — a caption must not be able to hide that the voice never transcribed", in.Text)
	}
}

func TestFlushInbounds_VoiceWithSTTPluginUsesTranscript(t *testing.T) {
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	b.Plugins.OnVoiceReceived(func(ctx context.Context, p c3types.VoicePayload) (string, error) {
		return "transcribed text", nil
	})

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	in := &c3types.Inbound{
		Channel:     "telegram",
		ChatID:      -100,
		MessageID:   44,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "v1"}},
	}
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	if !strings.Contains(in.Text, "transcribed text") {
		t.Errorf("voice with STT plugin: in.Text=%q, want transcript embedded", in.Text)
	}
}

// Phase 3 (spec §E): the per-inbound STT call in flushInbounds runs on the
// worker run-loop ctx. Before the fix it had NO per-call deadline (only the STT
// builtin's ~300s subprocess budget), so a download that hangs BEFORE that
// budget applies blocks the worker goroutine — and any JobFetch/JobConsume
// queued behind it — for up to ~5 min. The fix wraps each call in a per-call
// timeout (sttFlushTimeout). A timed-out call returns "" and falls through to
// the existing self-documenting sttFailureText placeholder path. This test
// registers a stub STT plugin that BLOCKS until its ctx is cancelled, shortens
// sttFlushTimeout, and asserts flushInbounds returns promptly with the
// placeholder (not a ~5min hang, not an empty transcript).
func TestFlushInbounds_VoiceSTTTimeout_FallsBackToPlaceholder(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())

	orig := sttFlushTimeout
	sttFlushTimeout = 50 * time.Millisecond
	defer func() { sttFlushTimeout = orig }()

	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	// Stub STT that hangs until its (per-call) ctx is cancelled, then returns
	// an empty transcript — exactly what a download stuck past the deadline does.
	b.Plugins.OnVoiceReceived(func(ctx context.Context, p c3types.VoicePayload) (string, error) {
		<-ctx.Done()
		return "", nil
	})

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 45,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "HUNGFILE", MIME: "audio/ogg", Size: 1000}},
	}

	done := make(chan struct{})
	go func() {
		w.flushInbounds(context.Background(), []*c3types.Inbound{in})
		close(done)
	}()

	// Must return in ~the timeout, not the ~300s STT budget or ~5min hang.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("flushInbounds did not return within 5s — STT call is not bounded by sttFlushTimeout")
	}

	for _, want := range []string{"transcription failed", "HUNGFILE", "download_attachment", "retranscribe", "does not need to resend"} {
		if !strings.Contains(in.Text, want) {
			t.Errorf("STT-timeout fallback text missing %q; got %q", want, in.Text)
		}
	}
}

// Regression for the common STT-failure path: the STT builtin signals failure by
// returning a NON-EMPTY "[STT FAILED: <reason> — see <path>]" marker (not an
// empty string). Before the fix that marker took the transcript!="" branch and
// reached the agent verbatim — it names the log but NOT the file_id or the
// retranscribe tool. The fix routes the marker through sttFailureText, so the
// agent sees the rich, self-documenting recovery text with the parsed reason and
// never the raw marker.
func TestFlushInbounds_VoiceSTTFailureMarkerBecomesRichText(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	// STT builtin's real failure signal: a non-empty marker, not "".
	b.Plugins.OnVoiceReceived(func(ctx context.Context, p c3types.VoicePayload) (string, error) {
		return "[STT FAILED: token_unavailable — see /var/log/c3/broker.log]", nil
	})

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 46,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "MARKERFILE", MIME: "audio/ogg", Size: 1000}},
	}
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	for _, want := range []string{"transcription failed", "MARKERFILE", "download_attachment", "retranscribe", "token_unavailable"} {
		if !strings.Contains(in.Text, want) {
			t.Errorf("marker→rich text missing %q; got %q", want, in.Text)
		}
	}
	if strings.Contains(in.Text, "[STT FAILED:") {
		t.Errorf("agent-facing text still contains the raw failure marker; got %q", in.Text)
	}
}

func TestSTTFailureReason(t *testing.T) {
	cases := []struct {
		name       string
		transcript string
		want       string
	}{
		{"empty", "", "no_transcript"},
		{"marker with hint", "[STT FAILED: token_unavailable — see /var/log/c3/broker.log]", "token_unavailable"},
		{"marker without hint", "[STT FAILED: timeout]", "timeout"},
		{"garbage", "not a marker at all", "stt_failed"},
		{"empty reason", "[STT FAILED: ]", "stt_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sttFailureReason(tc.transcript); got != tc.want {
				t.Errorf("sttFailureReason(%q) = %q, want %q", tc.transcript, got, tc.want)
			}
		})
	}
}

// Regression test for 2026-05-14: after /mcp reconnect, Claude Code killed
// the old adapter; the broker kept the now-stale claim "while pid alive"
// and every inbound failed with `holder.Conn is not *ipc.Conn` because the
// MarkDisconnected'd stub had nil Conn. Worse, no Telegram fallback fired
// either (the stale claim made `claimed=true`). Fix: liveness check at
// dispatch time, release-and-fall-through when holder PID is dead.
func TestForwardOrFallback_StaleClaim_ReleasesAndFallsThrough(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)

	// Stub with a PID that's definitively dead. PIDs above PID_MAX
	// (typically 4194304 on Linux, 99999 on macOS) reliably return
	// ESRCH from kill(2). 1<<30 is safely beyond both. MarkDisconnected
	// mirrors the post-conn-drop state.
	deadStub := &Stub{
		CLI: "claude", PID: 1 << 30, CWD: "/home/u/proj", ConnID: 99,
	}
	deadStub.MarkDisconnected()
	b.Routes.Claim(key, deadStub)
	if _, held := b.Routes.Holder(key); !held {
		t.Fatal("test setup: claim should be in place before delivery")
	}

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	in := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -1001234567890,
		TopicID:   &tid,
		MessageID: 1133,
		Text:      "Hi",
	}
	w.forwardOrFallback(context.Background(), in, 1)

	// Stale claim must be released.
	if _, held := b.Routes.Holder(key); held {
		t.Error("stale dead-holder claim should have been released on dispatch")
	}
	// Fallback should have fired since claim was cleared.
	if got := len(fc.sendRepliesSnapshot()); got != 1 {
		t.Errorf("expected fallback SendReply after releasing stale claim, got %d sends", got)
	}
}

// Companion (D2 / adapter-ipc-1): a holder that IS alive but currently has nil
// Conn (between adapter reconnects) must keep its claim — the adapter will be
// back on the wire shortly, so we don't release it. But THIS inbound must NOT
// be dropped silently: a message sent during the reconnect window used to be
// lost forever with nothing to replay it. Instead it bounces to the SAME
// Telegram fallback the STALE branch uses, so the user is told to resend.
func TestForwardOrFallback_AliveButDisconnectedHolder_BouncesToFallback(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)

	// Use this test process's PID — guaranteed alive.
	aliveStub := &Stub{
		CLI: "claude", PID: os.Getpid(), CWD: "/home/u/proj", ConnID: 99,
	}
	aliveStub.MarkDisconnected() // alive but disconnected
	b.Routes.Claim(key, aliveStub)

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -1001234567890, TopicID: &tid,
		MessageID: 1134, Text: "Hi",
	}
	w.forwardOrFallback(context.Background(), in, 1)

	// The claim is preserved: the holder is alive and will rebind on reconnect.
	if _, held := b.Routes.Holder(key); !held {
		t.Error("alive-but-disconnected claim must be preserved (adapter may reconnect)")
	}
	// The inbound is bounced to the Telegram fallback (not silently dropped) so
	// the user knows to resend once the adapter reconnects.
	if got := len(fc.sendRepliesSnapshot()); got != 1 {
		t.Errorf("expected fallback SendReply for alive-but-disconnected holder, got %d sends", got)
	}
}

// Companion to the bounce: a synthesized EVENT (not a human message) on an
// alive-but-disconnected route must NOT bounce the fallback boilerplate into
// the chat — events are never bounced (a late poll close shouldn't spam the
// topic). It falls through and is dropped with a metadata-only log, same as the
// unclaimed-event path.
func TestForwardOrFallback_AliveButDisconnectedHolder_EventNotBounced(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)

	aliveStub := &Stub{CLI: "claude", PID: os.Getpid(), CWD: "/home/u/proj", ConnID: 99}
	aliveStub.MarkDisconnected()
	b.Routes.Claim(key, aliveStub)

	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	event := &c3types.Inbound{
		Channel: "telegram", ChatID: -1001234567890, TopicID: &tid,
		MessageID: 2201, Kind: c3types.InboundPollResult,
		Event: &c3types.InboundEvent{PollResult: &c3types.PollResult{PollID: "p-late", IsClosed: true}},
	}
	w.forwardOrFallback(context.Background(), event, 0)

	if _, held := b.Routes.Holder(key); !held {
		t.Error("alive-but-disconnected claim must be preserved for an event too")
	}
	if got := len(fc.sendRepliesSnapshot()); got != 0 {
		t.Errorf("an event must not bounce the fallback boilerplate; got %d sends", got)
	}
}

// A synthesized channel EVENT (e.g. a late timed/auto-close poll_result) that is
// forwarded on a route with NO live claim must NOT bounce the "no CLI attached"
// fallback boilerplate into the chat — it is not a human message awaiting a
// reply. It is dropped with a metadata-only log instead. Regression for the
// completeness-batch finding: a poll closing after the session detached spammed
// the topic with the fallback text.
func TestForwardOrFallback_UnclaimedEvent_DoesNotBounceFallback(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)

	// No claim is registered: the route is unclaimed (session detached).
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	defer w.Stop()

	event := &c3types.Inbound{
		Channel:   "telegram",
		ChatID:    -1001234567890,
		TopicID:   &tid,
		MessageID: 2200,
		Kind:      c3types.InboundPollResult,
		Event: &c3types.InboundEvent{PollResult: &c3types.PollResult{
			PollID: "p-late", IsClosed: true,
		}},
	}
	w.forwardOrFallback(context.Background(), event, 0)

	// The event must be dropped — no fallback boilerplate sent to the chat.
	if got := len(fc.sendRepliesSnapshot()); got != 0 {
		t.Errorf("expected no fallback send for an unclaimed event, got %d sends", got)
	}
}

// brokerWithGenericChannel wires a broker pre-registered with an arbitrary
// channel.Channel (not just *fakeChannel), mirroring brokerWithChannel's manual
// registration so the typing-relay tests can use a SendTyping-recording channel.
func brokerWithGenericChannel(t *testing.T, mf *mappings.MappingsFile, ch channel.Channel) *Broker {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	b := New(mf)
	b.chMu.Lock()
	b.channels[ch.Name()] = &channelRegistration{Channel: ch}
	b.chMu.Unlock()
	return b
}

// claimedHolder registers a connected, alive stub holding key and returns it.
func claimedHolder(t *testing.T, b *Broker, key RouteKey) *Stub {
	t.Helper()
	s := &Stub{CLI: "claude", PID: os.Getpid(), CWD: "/proj", ConnID: 7, Conn: "live"}
	b.Routes.Claim(key, s)
	return s
}

// TestTypingRelay_ArmsOnlyWhenHolderRepliedAndTypingCap covers the deterministic
// arm gate (P5): the typing ticker arms on a delivered inbound ONLY IF the holder
// has replied (Telegram-mode proxy) AND the channel advertises Typing.
func TestTypingRelay_ArmsOnlyWhenHolderRepliedAndTypingCap(t *testing.T) {
	tid := int64(281)
	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: tid}

	t.Run("not armed before holder has replied", func(t *testing.T) {
		ch := &typingRecorderChannel{}
		b := brokerWithGenericChannel(t, mfWithTelegram(), ch)
		defer b.Shutdown()
		holder := claimedHolder(t, b, key)
		w := newRouteWorker(context.Background(), key, time.Hour, b)
		// Stop the run goroutine before driving the relay calls directly: armTyping
		// and the run loop's `case <-w.typingC` both touch worker-goroutine-owned
		// state (typingC/typingTicker) with no lock, so a direct call from the test
		// goroutine while run is live races the select. Stop cancels run first.
		w.Stop()

		w.armTyping(holder) // holder.HasReplied() == false
		if w.typingTicker != nil || w.typingC != nil {
			t.Fatal("typing must NOT arm before the holder has replied (CLI-mode gate)")
		}
	})

	t.Run("armed once holder has replied", func(t *testing.T) {
		ch := &typingRecorderChannel{} // Typing: true
		b := brokerWithGenericChannel(t, mfWithTelegram(), ch)
		defer b.Shutdown()
		holder := claimedHolder(t, b, key)
		holder.MarkReplied()
		w := newRouteWorker(context.Background(), key, time.Hour, b)
		// Stop run before driving the relay directly (see the first subtest's note):
		// armTyping/pulseTyping are worker-goroutine semantics being driven manually.
		w.Stop()

		w.armTyping(holder)
		if w.typingTicker == nil || w.typingC == nil {
			t.Fatal("typing should arm once the holder has replied and Typing cap is set")
		}
		// A pulse must fire SendTyping for the route's chat/topic.
		w.pulseTyping(context.Background())
		got := ch.typingSnapshot()
		if len(got) != 1 || got[0].chatID != -100 || got[0].threadID == nil || *got[0].threadID != tid {
			t.Fatalf("pulse should SendTyping for the route; got %+v", got)
		}
	})

	t.Run("not armed when channel lacks Typing cap", func(t *testing.T) {
		ch := &noTypingChannel{} // Typing: false
		b := brokerWithGenericChannel(t, mfWithTelegram(), ch)
		defer b.Shutdown()
		holder := claimedHolder(t, b, key)
		holder.MarkReplied()
		w := newRouteWorker(context.Background(), key, time.Hour, b)
		// Stop run before driving the relay directly (see the first subtest's note).
		w.Stop()

		w.armTyping(holder)
		if w.typingTicker != nil {
			t.Fatal("typing must NOT arm when the channel does not advertise Typing")
		}
	})
}

// TestTypingRelay_DisarmIsIdempotentAndStopsTicker covers disarm (P5): the first
// reply of a turn disarms the ticker; disarm is idempotent and clears state.
func TestTypingRelay_DisarmIsIdempotentAndStopsTicker(t *testing.T) {
	tid := int64(281)
	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: tid}
	ch := &typingRecorderChannel{}
	b := brokerWithGenericChannel(t, mfWithTelegram(), ch)
	defer b.Shutdown()
	holder := claimedHolder(t, b, key)
	holder.MarkReplied()
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	// Stop run before driving the relay directly: arm/disarm and the run loop's
	// `case <-w.typingC` both touch unlocked worker-goroutine state, so a direct
	// call while run is live races the select. Stop cancels run first.
	w.Stop()

	w.armTyping(holder)
	if w.typingTicker == nil {
		t.Fatal("setup: ticker should be armed")
	}
	w.disarmTyping()
	if w.typingTicker != nil || w.typingC != nil {
		t.Fatal("disarm should stop the ticker and clear state")
	}
	w.disarmTyping() // idempotent — must not panic on a nil ticker
}

// TestTypingRelay_ReArmKeepsCadence covers re-arm (P5): re-arming an already-armed
// ticker is a no-op (keeps the same ticker), so a turn's tool calls don't reset
// the cadence.
func TestTypingRelay_ReArmKeepsCadence(t *testing.T) {
	tid := int64(281)
	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: tid}
	ch := &typingRecorderChannel{}
	b := brokerWithGenericChannel(t, mfWithTelegram(), ch)
	defer b.Shutdown()
	holder := claimedHolder(t, b, key)
	holder.MarkReplied()
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	// Stop run before driving the relay directly (see DisarmIsIdempotent's note).
	w.Stop()

	w.armTyping(holder)
	first := w.typingTicker
	w.armTyping(holder) // re-arm
	if w.typingTicker != first {
		t.Fatal("re-arm should keep the existing ticker (steady cadence), not replace it")
	}
}

// noTypingChannel advertises Typing:false so the arm gate can be tested.
type noTypingChannel struct{ typingRecorderChannel }

func (c *noTypingChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{Channel: "telegram", Typing: false}
}

// TestTypingRelay_IdlesOutWhenNoReply is the regression test for the
// 2026-06-15 triple-review finding: a typing tick must NOT extend the worker's
// idle lifetime. Before the fix, the run loop reset the idle timer on EVERY
// select iteration (including the typing-tick case); since typingInterval <
// idle, an armed ticker re-armed idle forever, so a worker that took an inbound
// (or a non-reply tool call) but never replied — e.g. the user switched to CLI
// mode mid-turn — pulsed "typing…" to Telegram indefinitely and never idled out.
//
// Here we arm the relay via a successful non-reply tool call (react), which arms
// typing in the worker goroutine (race-free), then never reply. With a short
// idle and short pulse cadence the worker MUST idle out and disarm the relay.
func TestTypingRelay_IdlesOutWhenNoReply(t *testing.T) {
	tid := int64(281)
	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: tid}
	ch := &typingRecorderChannel{} // Typing: true; react returns nil
	b := brokerWithGenericChannel(t, mfWithTelegram(), ch)
	defer b.Shutdown()
	holder := claimedHolder(t, b, key)
	holder.MarkReplied() // arm gate: holder must have replied at least once

	w := newRouteWorker(context.Background(), key, 150*time.Millisecond, b)
	w.typingIvl = 20 * time.Millisecond // many pulses fit inside the idle window
	defer w.Stop()

	// A successful non-reply tool call arms the relay from the worker goroutine.
	resultCh := make(chan OutboundResult, 1)
	if !w.Submit(Job{Kind: JobOutbound, Outbound: &OutboundJob{
		Tool: "react", Args: map[string]any{"message_id": int64(1), "emoji": "👍"}, ResultCh: resultCh,
	}}) {
		t.Fatal("Submit react job failed")
	}
	if r := <-resultCh; r.Err != nil {
		t.Fatalf("react tool call should succeed (arms typing); got err %v", r.Err)
	}

	// The worker must idle out despite the typing ticker pulsing the whole time.
	select {
	case <-w.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("worker did NOT idle out — a typing tick is still extending the idle timer (unbounded pulsing)")
	}

	// After the run goroutine has exited (Done closed), no concurrent writes
	// remain, so reading typing state here is race-free. The defer disarmTyping()
	// in run() must have stopped the relay.
	if w.typingTicker != nil || w.typingC != nil {
		t.Fatal("typing relay must be disarmed once the worker idles out")
	}

	// Pulsing happened (proof the relay was live) but is bounded — it did not run
	// away. Far fewer than a runaway count.
	if n := len(ch.typingSnapshot()); n == 0 {
		t.Fatal("expected at least one typing pulse while the relay was armed")
	}
}

// TestTypingRelay_DisarmsAfterMaxPulses covers the belt-and-suspenders bound:
// even if (hypothetically) the idle timeout never trips, the relay self-disarms
// after maxTypingPulses consecutive unanswered pulses. We drive pulseTyping
// directly (worker goroutine semantics: armTyping then pulseTyping are the same
// calls the run loop makes) on a long-idle worker so only the cap can stop it.
func TestTypingRelay_DisarmsAfterMaxPulses(t *testing.T) {
	tid := int64(281)
	key := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: tid}
	ch := &typingRecorderChannel{}
	b := brokerWithGenericChannel(t, mfWithTelegram(), ch)
	defer b.Shutdown()
	holder := claimedHolder(t, b, key)
	holder.MarkReplied()

	// Long idle + manual stepping so the run loop's own typing-tick never races
	// our direct calls (Stop cancels the loop before we touch state).
	w := newRouteWorker(context.Background(), key, time.Hour, b)
	w.Stop() // stop the run goroutine; we drive the relay calls directly below

	w.armTyping(holder)
	if w.typingTicker == nil {
		t.Fatal("setup: relay should arm")
	}
	// Pulse up to the cap: pulses 1..maxTypingPulses succeed; the next disarms.
	for i := 0; i < maxTypingPulses; i++ {
		w.pulseTyping(context.Background())
		if w.typingTicker == nil {
			t.Fatalf("relay disarmed too early at pulse %d (cap is %d)", i+1, maxTypingPulses)
		}
	}
	w.pulseTyping(context.Background()) // exceeds the cap
	if w.typingTicker != nil || w.typingC != nil {
		t.Fatalf("relay must self-disarm after exceeding %d consecutive unanswered pulses", maxTypingPulses)
	}
}

// TestWorker_StoppedAfterIdleExit pins A1's mutual-exclusion invariant: once a
// worker's run() has exited (here via the IDLE timeout), Submit must report the
// worker stopped (return false) rather than accepting a job into the buffer of a
// dead worker whose run goroutine will never read it (the original strand). This
// complements TestWorker_SubmitAfterStopReturnsFalse (which covers the explicit
// Stop() path) by covering the idle exit path, where pre-fix `stopped` stayed
// false for the worker's whole life.
func TestWorker_StoppedAfterIdleExit(t *testing.T) {
	w := newRouteWorker(context.Background(), RouteKey{Channel: "x"}, 20*time.Millisecond, nil)
	select {
	case <-w.Done():
	case <-time.After(time.Second):
		t.Fatal("worker did not idle out within 1s")
	}
	if w.Submit(Job{Kind: JobInbound}) {
		t.Error("Submit after idle exit must return false (worker is stopped)")
	}
}

// TestWorker_IdleExitDrainsPendingFetch pins A1's loss-free drain: when run()
// exits with jobs still buffered in w.queue, every result-channel-bearing job
// must be replied to with an error instead of being left stranded — a caller
// blocked on <-resultCh would otherwise hang forever (the original fetch_queue
// wedge).
//
// All four exit paths (ctx.Done, idleTimer, queue-closed, JobRelease) funnel
// through the SAME single `defer w.shutdown()`, so exercising one exercises the
// drain for all. We drive the JobRelease exit because it is the only
// DETERMINISTIC strand: the idle/ctx selects race the queue read (Go `select`
// picks a ready case at random when a job is also buffered), whereas a
// JobRelease read returns WITHOUT re-entering the select, deterministically
// leaving the jobs queued behind it un-processed for shutdown() to drain.
// Mirrors JobFetch + JobOutbound (Watch-out: JobInbound carries no ResultCh and
// must be dropped silently — exercised implicitly here, asserted by no hang).
func TestWorker_IdleExitDrainsPendingFetch(t *testing.T) {
	fetchCh := make(chan FetchResult, 1)
	outCh := make(chan OutboundResult, 1)

	// Build the worker WITHOUT starting run(), pre-load the queue so all jobs are
	// buffered before the loop reads anything, THEN start run(). The loop reads the
	// leading JobRelease and returns; the JobFetch + JobOutbound behind it are never
	// processed and must be drained on exit. A no-ResultCh JobInbound is also
	// buffered to prove the drain drops it silently (no panic, no hang).
	w := &RouteWorker{
		key:   RouteKey{Channel: "x"},
		queue: make(chan Job, 64),
		idle:  time.Hour,
		done:  make(chan struct{}),
	}
	w.queue <- Job{Kind: JobRelease}
	w.queue <- Job{Kind: JobFetch, Fetch: &FetchJob{All: true, ResultCh: fetchCh}}
	w.queue <- Job{Kind: JobInbound} // no ResultCh — must be dropped silently
	w.queue <- Job{Kind: JobOutbound, Outbound: &OutboundJob{Tool: "reply", ResultCh: outCh}}
	go w.run(context.Background())

	select {
	case r := <-fetchCh:
		if r.Err == nil {
			t.Error("drained JobFetch must carry a non-nil error, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("pending JobFetch was stranded — run() exit did not drain the queue")
	}
	select {
	case r := <-outCh:
		if r.Err == nil {
			t.Error("drained JobOutbound must carry a non-nil error, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("pending JobOutbound was stranded — run() exit did not drain the queue")
	}
}

func TestWorker_OutboundStubReturnsErr(t *testing.T) {
	w := newRouteWorker(context.Background(), RouteKey{Channel: "x"}, time.Hour, nil)
	defer w.Stop()

	resultCh := make(chan OutboundResult, 1)
	if !w.Submit(Job{Kind: JobOutbound, Outbound: &OutboundJob{Tool: "reply", ResultCh: resultCh}}) {
		t.Fatal("Submit failed")
	}
	select {
	case r := <-resultCh:
		if r.Err == nil {
			t.Error("expected stub error in Phase 4A")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("no result within 500ms")
	}
}

// readbackRecorderChannel embeds *fakeChannel and additionally implements the
// OPTIONAL SendReadback method, so flushInbounds' echoReadback hook resolves it
// via the readbacker type-assert. A channel WITHOUT this method (the bare
// fakeChannel) is skipped silently — the additive contract.
type readbackRecorderChannel struct {
	*fakeChannel
	rbMu      sync.Mutex
	readbacks []c3types.ReadbackArgs
	// beforeRecord, when non-nil, runs at the TOP of SendReadback BEFORE the
	// readback is recorded and WITHOUT holding rbMu — so a Phase-3 ordering test
	// can inject a per-echo delay or blocking handshake without the recorder's own
	// mutex serializing the sends (which would mask a reordering bug). Set once
	// before any flush, read only from the echo goroutines. nil keeps the original
	// immediate-record behavior for every existing test.
	beforeRecord func(c3types.ReadbackArgs)
}

func (r *readbackRecorderChannel) SendReadback(a c3types.ReadbackArgs) (int64, error) {
	if r.beforeRecord != nil {
		r.beforeRecord(a)
	}
	r.rbMu.Lock()
	defer r.rbMu.Unlock()
	r.readbacks = append(r.readbacks, a)
	return 99, nil
}

func (r *readbackRecorderChannel) readbackSnapshot() []c3types.ReadbackArgs {
	r.rbMu.Lock()
	defer r.rbMu.Unlock()
	out := make([]c3types.ReadbackArgs, len(r.readbacks))
	copy(out, r.readbacks)
	return out
}

// waitReadbacks polls until at least n readbacks are recorded, or fails after a
// timeout. The echo now fires from a DETACHED goroutine (flushInbounds dispatches
// echoReadback with `go` so a retrying send can't stall the worker's serial
// loop), so tests must wait for it rather than read synchronously.
func (r *readbackRecorderChannel) waitReadbacks(t *testing.T, n int) []c3types.ReadbackArgs {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		rbs := r.readbackSnapshot()
		if len(rbs) >= n {
			return rbs
		}
		if time.Now().After(deadline) {
			t.Fatalf("want >= %d SendReadback, got %d after 2s", n, len(rbs))
			return rbs
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// waitReplyContaining polls the recorder's SendReply calls until one contains
// substr, or fails after a timeout. Same rationale as waitReadbacks: the failure
// notice is now sent from the detached echo goroutine.
func (r *readbackRecorderChannel) waitReplyContaining(t *testing.T, substr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		for _, rp := range r.sendRepliesSnapshot() {
			if strings.Contains(rp.Text, substr) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("never saw a SendReply containing %q within 2s", substr)
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func registerReadbackChannel(b *Broker, rc *readbackRecorderChannel) {
	b.chMu.Lock()
	b.channels[rc.Name()] = &channelRegistration{Channel: rc}
	b.chMu.Unlock()
}

// TestFlushInbounds_ReadbackOnSuccess: a REAL transcript drives exactly one
// SendReadback carrying the verbatim transcript (ChatID/TopicID/ReplyTo set), and
// no "couldn't transcribe" failure notice. The agent surface (in.Text) still
// carries the transcript — the echo is purely additive.
func TestFlushInbounds_ReadbackOnSuccess(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	rc := &readbackRecorderChannel{fakeChannel: &fakeChannel{}}
	registerReadbackChannel(b, rc)
	b.Plugins.OnVoiceReceived(func(ctx context.Context, p c3types.VoicePayload) (string, error) {
		return "hello from the voice note", nil
	})

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	tid := int64(7)
	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 51,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "VF"}},
	}
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	rbs := rc.waitReadbacks(t, 1)
	if len(rbs) != 1 {
		t.Fatalf("want exactly 1 SendReadback on success, got %d", len(rbs))
	}
	if rbs[0].Transcript != "hello from the voice note" {
		t.Errorf("readback transcript = %q; want verbatim transcript", rbs[0].Transcript)
	}
	if rbs[0].ChatID != -100 || rbs[0].ReplyTo == nil || *rbs[0].ReplyTo != 51 ||
		rbs[0].TopicID == nil || *rbs[0].TopicID != 7 {
		t.Errorf("readback routing wrong: %+v", rbs[0])
	}
	if !strings.Contains(in.Text, "hello from the voice note") {
		t.Errorf("agent surface lost the transcript: in.Text=%q", in.Text)
	}
	for _, rp := range rc.sendRepliesSnapshot() {
		if strings.Contains(rp.Text, "Couldn't transcribe") {
			t.Error("a success readback must not also send the failure notice")
		}
	}
}

// TestFlushInbounds_ReadbackFailureNotice: an STT failure marker drives NO
// SendReadback but DOES send the human "couldn't transcribe" notice via the
// channel's normal SendReply. The agent-surface text (in.Text) is the RICH
// sttFailureText recovery message (the raw marker is routed through it), not the
// verbatim "[STT FAILED:" marker.
func TestFlushInbounds_ReadbackFailureNotice(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	rc := &readbackRecorderChannel{fakeChannel: &fakeChannel{}}
	registerReadbackChannel(b, rc)
	b.Plugins.OnVoiceReceived(func(ctx context.Context, p c3types.VoicePayload) (string, error) {
		return "[STT FAILED: timeout — see /tmp/broker.log]", nil
	})

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 52,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "VF"}},
	}
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	// The failure notice is sent from the detached echo goroutine now — wait
	// for it, then assert (no readback on STT failure).
	rc.waitReplyContaining(t, "Couldn't transcribe that voice note")
	if n := len(rc.readbackSnapshot()); n != 0 {
		t.Fatalf("want 0 SendReadback on STT failure, got %d", n)
	}
	sawNotice := false
	for _, rp := range rc.sendRepliesSnapshot() {
		if strings.Contains(rp.Text, "Couldn't transcribe that voice note") {
			sawNotice = true
			if rp.ReplyTo == nil || *rp.ReplyTo != 52 {
				t.Errorf("failure notice should reply-quote the voice msg, got %+v", rp)
			}
		}
	}
	if !sawNotice {
		t.Error("STT failure must send the human 'couldn't transcribe' notice via SendReply")
	}
	// The agent-surface text is now the RICH recovery message (the marker is
	// routed through sttFailureText), carrying the parsed reason — NOT the raw
	// "[STT FAILED:" marker, which named only the log, not the file_id/retranscribe.
	if strings.Contains(in.Text, "[STT FAILED:") {
		t.Errorf("agent-surface text should not carry the raw marker: in.Text=%q", in.Text)
	}
	for _, want := range []string{"transcription failed", "timeout", "VF", "retranscribe"} {
		if !strings.Contains(in.Text, want) {
			t.Errorf("agent-surface rich text missing %q: in.Text=%q", want, in.Text)
		}
	}
}

// TestFlushInbounds_EchoOrderingChained: two voice notes in ONE topic where the
// FIRST echo's send is DELAYED (simulates a transient/429 backoff) and the
// SECOND's is immediate. The Phase-3 chain must keep the courtesy echoes in
// arrival order — the second must NOT post before the first. Without the chain (a
// bare detached `go`) the fast second echo would record first and this fails.
func TestFlushInbounds_EchoOrderingChained(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	rc := &readbackRecorderChannel{fakeChannel: &fakeChannel{}}
	// Delay ONLY the first note's send. The delay runs BEFORE rbMu is taken, so it
	// cannot serialize the second send via the recorder's own lock — only the
	// echo chain can preserve order.
	rc.beforeRecord = func(a c3types.ReadbackArgs) {
		if a.Transcript == "first" {
			time.Sleep(100 * time.Millisecond)
		}
	}
	registerReadbackChannel(b, rc)
	b.Plugins.OnVoiceReceived(func(ctx context.Context, p c3types.VoicePayload) (string, error) {
		if p.MessageID == 1 {
			return "first", nil
		}
		return "second", nil
	})

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	voice := []c3types.Attachment{{Kind: "voice", FileID: "VF"}}
	in1 := &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 1, Attachments: voice}
	in2 := &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 2, Attachments: voice}

	// flushInbounds runs on the serial run goroutine in production; calling it
	// twice in sequence here mirrors that exactly and sets the chain in arrival
	// order (in1 then in2). This also exercises ordering ACROSS batches.
	w.flushInbounds(context.Background(), []*c3types.Inbound{in1})
	w.flushInbounds(context.Background(), []*c3types.Inbound{in2})

	rbs := rc.waitReadbacks(t, 2)
	if len(rbs) < 2 {
		t.Fatalf("want >= 2 readbacks, got %d", len(rbs))
	}
	if rbs[0].Transcript != "first" || rbs[1].Transcript != "second" {
		t.Fatalf("echo order violated: got %q then %q; want \"first\" then \"second\" — chain must preserve per-topic arrival order",
			rbs[0].Transcript, rbs[1].Transcript)
	}
}

// TestFlushInbounds_EchoChainCtxCancel: cancel the run-loop ctx while a SECOND
// echo is parked waiting on the FIRST echo's chain link. The parked echo must
// take the ctx.Done() branch and exit PROMPTLY (closing its own link) instead of
// deadlocking behind the still-running first echo — and the first echo, once
// unblocked, must also finish (no goroutine leak on shutdown).
func TestFlushInbounds_EchoChainCtxCancel(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	echo1Entered := make(chan struct{})
	releaseEcho1 := make(chan struct{})
	rc := &readbackRecorderChannel{fakeChannel: &fakeChannel{}}
	rc.beforeRecord = func(a c3types.ReadbackArgs) {
		if a.Transcript == "first" {
			close(echo1Entered) // echo1 is now inside its send; mine1 still open
			<-releaseEcho1      // hold it there so echo2 stays parked on <-prev
		}
	}
	registerReadbackChannel(b, rc)
	b.Plugins.OnVoiceReceived(func(ctx context.Context, p c3types.VoicePayload) (string, error) {
		if p.MessageID == 1 {
			return "first", nil
		}
		return "second", nil
	})

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	voice := []c3types.Attachment{{Kind: "voice", FileID: "VF"}}
	in1 := &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 1, Attachments: voice}
	in2 := &c3types.Inbound{Channel: "telegram", ChatID: -100, MessageID: 2, Attachments: voice}

	// Echo #1: prev is the pre-closed head, so it runs immediately, enters its
	// (blocked) send, and holds mine1 open. w.prevEchoDone is read only on this
	// test goroutine (the same one that calls flushInbounds), so it's race-free.
	w.flushInbounds(ctx, []*c3types.Inbound{in1})
	mine1 := w.prevEchoDone
	<-echo1Entered // echo1 now stuck in send; mine1 not yet closed

	// Echo #2: prev = mine1 (open), so it PARKS on <-prev while also watching ctx.
	w.flushInbounds(ctx, []*c3types.Inbound{in2})
	mine2 := w.prevEchoDone

	// Cancel mid-chain: echo2 is parked on <-prev. It MUST take ctx.Done() and
	// exit promptly (close mine2) rather than wait for the blocked echo1.
	cancel()
	select {
	case <-mine2:
		// echo2 exited + closed its link → prompt, no deadlock.
	case <-time.After(2 * time.Second):
		t.Fatal("echo2 parked on <-prev did not exit within 2s of ctx cancel — chain deadlocked/leaked on shutdown")
	}

	// echo2 aborted BEFORE sending; echo1 is still blocked in its send, so nothing
	// has been recorded yet.
	if got := rc.readbackSnapshot(); len(got) != 0 {
		t.Fatalf("echo2 should abort before sending on ctx cancel; got %d readback(s): %+v", len(got), got)
	}

	// Release echo1 and confirm it too finishes (closes mine1) — no leak.
	close(releaseEcho1)
	select {
	case <-mine1:
	case <-time.After(2 * time.Second):
		t.Fatal("echo1 did not finish after release — goroutine leaked")
	}
}
