package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

func restartBroker(t *testing.T, mf *mappings.MappingsFile) (*Broker, *permFakeChannel) {
	t.Helper()
	if mf == nil {
		mf = mfWithTelegram()
		mf.AddAllowedUser(testOperatorUID)
	}
	fc := &permFakeChannel{fakeChannel: fakeChannel{replyReturnID: 71, caps: &c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}}}
	b := brokerWithPermChannel(t, mf, fc)
	t.Cleanup(b.Shutdown)
	return b, fc
}
func restartFrames(t *testing.T, conn *ipc.Conn) <-chan []byte {
	t.Helper()
	frames := make(chan []byte, 20)
	go func() {
		defer close(frames)
		for {
			raw, err := conn.ReadFrame()
			if err != nil {
				return
			}
			frames <- raw
		}
	}()
	return frames
}
func restartFrame(t *testing.T, frames <-chan []byte) []byte {
	t.Helper()
	select {
	case raw := <-frames:
		if raw == nil {
			t.Fatal("connection closed")
		}
		return raw
	case <-time.After(time.Second):
		t.Fatal("missing frame")
		return nil
	}
}
func restartJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func registerRestartPerm(t *testing.T, b *Broker, s *Stub) {
	t.Helper()
	b.handlePermissionRequest(nil, s, restartJSON(t, ipc.PermissionReq{Op: ipc.OpPermissionRequest, RequestID: "pending-perm", ToolName: "Bash", Preview: "echo *literal*"}))
	if !b.Perms.has("pending-perm") {
		t.Fatal("permission not registered")
	}
}
func restartTap() *c3types.CallbackEvent {
	return &c3types.CallbackEvent{Data: permDenyData("pending-perm"), MessageID: 71, CallbackID: "tap", Actor: c3types.Sender{UserID: testOperatorUID}}
}
func assertNoRestartFrame(t *testing.T, frames <-chan []byte) {
	t.Helper()
	select {
	case raw := <-frames:
		t.Fatalf("unexpected frame: %s", raw)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestControlledRestartRefusesRegistrationsAndQueuesLegacyPush(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, fc := restartBroker(t, nil)
	key := drainSrc()
	s, c := bindingSession(t, b, key, "claude", os.Getpid(), t.TempDir())
	frames := restartFrames(t, c)
	b.BeginControlledRestart()
	b.handleAskRegister(s.ConnValue().(*ipc.Conn), s, []byte(`{"ask_id":"new","question":"Continue?","options":["yes"]}`))
	var ack ipc.AskRegisteredMsg
	json.Unmarshal(restartFrame(t, frames), &ack)
	if ack.OK || !strings.Contains(ack.Err, "restarting") || b.Asks.has("new") {
		t.Fatalf("new ask admitted: %+v", ack)
	}
	b.handlePermissionRequest(nil, s, []byte(`{"request_id":"new","tool_name":"Bash","preview":"echo test"}`))
	if b.Perms.has("new") {
		t.Fatal("new permission admitted")
	}
	if got := fc.sendRepliesSnapshot(); len(got) != 1 || !strings.Contains(got[0].Text, "laptop") {
		t.Fatalf("missing permission refusal notice: %+v", got)
	}
	b.Workers.Stop()
	w := &RouteWorker{key: key, broker: b}
	w.flushInbounds(context.Background(), []*c3types.Inbound{drainSrcMsg(32, "during drain")})
	rows, err := b.Queue.Peek(queueRouteKey(key), -1)
	if err != nil || len(rows) != 1 || rows[0].Text != "during drain" {
		t.Fatalf("drain push not durable: %+v %v", rows, err)
	}
	assertNoRestartFrame(t, frames)
}

func TestControlledRestartQueuesNegotiatedPushAndSettlesExistingAttempt(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, w, s, frames, ctx := negotiatedFixture(t)
	negotiatedAppend(t, w, 1, "already offered")
	w.scheduleAttempt(ctx, false)
	first := nextAttemptDelivery(t, w, frames)
	b.BeginControlledRestart()
	w.flushInbounds(ctx, []*c3types.Inbound{inboundOn(w.key.ChatID, nil, 2, "during drain")})
	w.handleAttemptResult(resultFor(s, first, "confirmed"))
	rows, err := b.Queue.Peek(queueRouteKey(w.key), -1)
	if err != nil || len(rows) != 1 || rows[0].MessageID != 2 {
		t.Fatalf("drain/receipt state: %+v %v", rows, err)
	}
	w.scheduleAttempt(ctx, false)
	select {
	case extra := <-frames:
		t.Fatalf("new push during drain: %+v", extra)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestControlledRestartWaitsForPermissionSettlement(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, _ := restartBroker(t, nil)
	s, c := bindingSession(t, b, drainSrc(), "claude", os.Getpid(), t.TempDir())
	frames := restartFrames(t, c)
	registerRestartPerm(t, b, s)
	b.BeginControlledRestart()
	done := make(chan struct{})
	go func() { b.DrainRequests(); close(done) }()
	select {
	case <-done:
		t.Fatal("drain skipped pending request")
	case <-time.After(30 * time.Millisecond):
	}
	if !b.resolvePerm(drainSrc(), restartTap()) {
		t.Fatal("existing settlement refused during drain")
	}
	var verdict ipc.PermissionVerdictMsg
	json.Unmarshal(restartFrame(t, frames), &verdict)
	if verdict.RequestID != "pending-perm" || verdict.Behavior != "deny" {
		t.Fatal(verdict)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("settlement did not release drain")
	}
}

func TestControlledRestartWaitsForAskAnswer(t *testing.T) {
	for _, suffix := range []string{"0", "done", "skip"} {
		t.Run(suffix, func(t *testing.T) {
			clearFetchTestEnvironment(t)
			b, _ := restartBroker(t, nil)
			s, c := bindingSession(t, b, drainSrc(), "claude", os.Getpid(), t.TempDir())
			frames := restartFrames(t, c)
			b.Asks.register(&pendingAsk{askID: "q", route: drainSrc(), owner: s, question: "Pick", options: []string{"yes"}, selected: []bool{true}, multi: suffix == "done", allowSkip: true, messageID: 71})
			b.BeginControlledRestart()
			done := make(chan struct{})
			go func() { b.DrainRequests(); close(done) }()
			if !b.resolveAsk(drainSrc(), &c3types.CallbackEvent{Data: "ask:q:" + suffix, MessageID: 71}) {
				t.Fatal("answer refused")
			}
			var answer ipc.AskResultMsg
			if err := json.Unmarshal(restartFrame(t, frames), &answer); err != nil {
				t.Fatal(err)
			}
			if answer.Err != "" || answer.AskID != "q" {
				t.Fatal(answer)
			}
			if suffix == "skip" && !answer.Answer.Skipped {
				t.Fatal(answer)
			}
			if suffix != "skip" && (len(answer.Answer.Selected) != 1 || answer.Answer.Selected[0] != "yes") {
				t.Fatal(answer)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("answer did not finish drain")
			}
		})
	}
}

func TestControlledRestartHumanGraceExceedsFourSeconds(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, _ := restartBroker(t, nil)
	b.Asks.register(&pendingAsk{askID: "q", route: drainSrc(), question: "Pick"})
	first := b.BeginControlledRestart()
	clock := make(chan time.Time)
	ticks := make(chan time.Time)
	done := make(chan struct{})
	go func() {
		b.drainRequestsUntil(first.Add(RestartPromptGrace), func() time.Time { return <-clock }, ticks)
		close(done)
	}()
	clock <- first.Add(5 * time.Second)
	ticks <- first.Add(5 * time.Second)
	if b.prompts.sealed.Load() || !b.Asks.has("q") {
		t.Fatal("four-second bound survived")
	}
	clock <- first.Add(59 * time.Second)
	ticks <- first.Add(59 * time.Second)
	if b.prompts.sealed.Load() {
		t.Fatal("sealed before human grace")
	}
	clock <- first.Add(60 * time.Second)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cap did not finish")
	}
	if b.Asks.has("q") {
		t.Fatal("cap retained question")
	}
}

func TestControlledRestartCapCancelsPendingPrompts(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, fc := restartBroker(t, nil)
	s, c := bindingSession(t, b, drainSrc(), "claude", os.Getpid(), t.TempDir())
	frames := restartFrames(t, c)
	b.Asks.register(&pendingAsk{askID: "q", route: drainSrc(), owner: s, question: "Pick *one*", messageID: 72})
	registerRestartPerm(t, b, s)
	b.BeginControlledRestart()
	b.cancelRestartPrompts()
	if b.Asks.has("q") || b.Perms.has("pending-perm") {
		t.Fatal("pending authority retained")
	}
	var answer ipc.AskResultMsg
	json.Unmarshal(restartFrame(t, frames), &answer)
	if answer.Err != restartAskCancelled || answer.Op != ipc.OpAskResult || len(answer.Answer.Selected) != 0 {
		t.Fatal(answer)
	}
	edits := fc.editSnapshot()
	if len(edits) != 2 {
		t.Fatalf("edits=%+v", edits)
	}
	for _, e := range edits {
		if e.Markup != c3types.MarkupNone || e.Buttons == nil || len(e.Buttons) != 0 {
			t.Fatalf("active keyboard: %+v", e)
		}
	}
	assertNoRestartFrame(t, frames)
}

func TestControlledRestartCancellationTextAndMarkup(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, fc := restartBroker(t, nil)
	b.Asks.register(&pendingAsk{askID: "q", route: drainSrc(), question: "Pick *one*", messageID: 72})
	b.Perms.register(&pendingPerm{requestID: "p", route: drainSrc(), toolName: "Bash", preview: "echo *literal*", messageID: 73})
	b.BeginControlledRestart()
	b.cancelRestartPrompts()
	want := map[int64]string{72: "Pick *one*\n\nC3 is restarting; this request was cancelled — ask again.", 73: "🔐 Permission: Bash\n\necho *literal*\n\nC3 is restarting; this permission relay was cancelled. The request may still be waiting at the laptop — cancel it there and ask again."}
	for _, e := range fc.editSnapshot() {
		if e.Text != want[e.MessageID] || e.Markup != c3types.MarkupNone || e.Buttons == nil || len(e.Buttons) > 0 {
			t.Fatalf("render=%+v", e)
		}
		delete(want, e.MessageID)
	}
	if len(want) != 0 {
		t.Fatal(want)
	}
}

func TestControlledRestartCancellationUsesOriginalRoute(t *testing.T) {
	for _, topic := range []bool{false, true} {
		t.Run(fmt.Sprint(topic), func(t *testing.T) {
			clearFetchTestEnvironment(t)
			b, fc := restartBroker(t, nil)
			route := drainSrc()
			route.HasTopic = topic
			s, c := bindingSession(t, b, route, "claude", os.Getpid(), t.TempDir())
			frames := restartFrames(t, c)
			b.Asks.register(&pendingAsk{askID: "q", route: route, owner: s, question: "Pick", messageID: 72})
			b.Perms.register(&pendingPerm{requestID: "p", route: route, owner: s, toolName: "Bash", messageID: 73})
			other := route
			other.ChatID--
			bindOutputRouteForTest(s, &other)
			_, otherConn := stealRoute(t, b, route)
			otherFrames := restartFrames(t, otherConn)
			fc.editErr = errors.New("edit unavailable")
			b.BeginControlledRestart()
			b.cancelRestartPrompts()
			restartFrame(t, frames)
			for _, r := range fc.sendRepliesSnapshot() {
				if r.ChatID != route.ChatID || r.Channel != route.Channel || (r.TopicID != nil) != route.HasTopic || route.HasTopic && *r.TopicID != route.TopicID {
					t.Fatalf("wrong route: %+v", r)
				}
			}
			assertNoRestartFrame(t, otherFrames)
		})
	}
}

func TestControlledRestartRepeatedIntentDoesNotExtendDeadline(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, _ := restartBroker(t, nil)
	first := b.BeginControlledRestart()
	for i := 0; i < 100; i++ {
		if !first.Equal(b.BeginControlledRestart()) {
			t.Fatal("deadline extended")
		}
	}
}

func TestControlledRestartLateCallbacksCannotBecomeInput(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, _ := restartBroker(t, nil)
	s, c := bindingSession(t, b, drainSrc(), "claude", os.Getpid(), t.TempDir())
	frames := restartFrames(t, c)
	registerRestartPerm(t, b, s)
	b.BeginControlledRestart()
	b.cancelRestartPrompts()
	b.resolvePerm(drainSrc(), restartTap())
	if !b.resolveAsk(drainSrc(), &c3types.CallbackEvent{Data: "ask:gone:0"}) {
		t.Fatal("post-cap callback not suppressed")
	}
	assertNoRestartFrame(t, frames)
}

type restartBarrierChannel struct {
	*permFakeChannel
	sendEntered, sendRelease chan struct{}
	editEntered, editRelease chan struct{}
}

func (c *restartBarrierChannel) SendReply(a c3types.ReplyArgs) (int64, error) {
	if len(a.Buttons) > 0 && c.sendEntered != nil {
		close(c.sendEntered)
		<-c.sendRelease
	}
	return c.permFakeChannel.SendReply(a)
}
func (c *restartBarrierChannel) EditMessage(a c3types.EditArgs) (*c3types.EditResult, error) {
	if len(a.Buttons) > 0 && c.editEntered != nil {
		close(c.editEntered)
		<-c.editRelease
	}
	return c.permFakeChannel.EditMessage(a)
}
func installRestartBarrier(b *Broker, c *restartBarrierChannel) {
	b.chMu.Lock()
	b.channels[c.Name()] = &channelRegistration{Channel: c}
	b.chMu.Unlock()
}
func restartWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("barrier timed out")
	}
}

func TestControlledRestartLateSendClearsCancelledKeyboard(t *testing.T) {
	for _, kind := range []string{"ask", "permission"} {
		t.Run(kind, func(t *testing.T) {
			clearFetchTestEnvironment(t)
			b, fc := restartBroker(t, nil)
			c := &restartBarrierChannel{permFakeChannel: fc, sendEntered: make(chan struct{}), sendRelease: make(chan struct{})}
			installRestartBarrier(b, c)
			s, conn := bindingSession(t, b, drainSrc(), "claude", os.Getpid(), t.TempDir())
			frames := restartFrames(t, conn)
			done := make(chan struct{})
			go func() {
				defer close(done)
				if kind == "ask" {
					b.handleAskRegister(s.ConnValue().(*ipc.Conn), s, []byte(`{"ask_id":"q","question":"Pick","options":["yes"]}`))
				} else {
					b.handlePermissionRequest(nil, s, []byte(`{"request_id":"p","tool_name":"Bash"}`))
				}
			}()
			restartWait(t, c.sendEntered)
			b.BeginControlledRestart()
			b.cancelRestartPrompts()
			close(c.sendRelease)
			restartWait(t, done)
			if kind == "ask" {
				for i := 0; i < 2; i++ {
					raw := restartFrame(t, frames)
					var m struct {
						OK  bool
						Err string
					}
					json.Unmarshal(raw, &m)
					if m.OK || m.Err != restartAskCancelled {
						t.Fatalf("late success: %s", raw)
					}
				}
			}
			edits := fc.editSnapshot()
			if len(edits) == 0 || len(edits[len(edits)-1].Buttons) != 0 || !strings.Contains(edits[len(edits)-1].Text, "cancelled") {
				t.Fatalf("late keyboard: %+v", edits)
			}
			if b.Asks.has("q") || b.Perms.has("p") {
				t.Fatal("late publication revived entry")
			}
		})
	}
}

func TestControlledRestartLateMultiEditCannotRestoreButtons(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, fc := restartBroker(t, nil)
	c := &restartBarrierChannel{permFakeChannel: fc, editEntered: make(chan struct{}), editRelease: make(chan struct{})}
	installRestartBarrier(b, c)
	s, conn := bindingSession(t, b, drainSrc(), "claude", os.Getpid(), t.TempDir())
	frames := restartFrames(t, conn)
	b.Asks.register(&pendingAsk{askID: "q", route: drainSrc(), owner: s, question: "Pick", options: []string{"yes"}, selected: []bool{false}, multi: true, messageID: 71})
	done := make(chan struct{})
	go func() { b.resolveAsk(drainSrc(), &c3types.CallbackEvent{Data: "ask:q:0", MessageID: 71}); close(done) }()
	restartWait(t, c.editEntered)
	b.BeginControlledRestart()
	b.cancelRestartPrompts()
	close(c.editRelease)
	restartWait(t, done)
	restartFrame(t, frames)
	edits := fc.editSnapshot()
	last := edits[len(edits)-1]
	if len(last.Buttons) != 0 || last.Text != "Pick\n\n"+restartAskCancelled {
		t.Fatalf("late toggle won: %+v", edits)
	}
}

func TestControlledRestartSettledDeferredPermissionIsNotCancelled(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, fc := restartBroker(t, nil)
	c := &restartBarrierChannel{permFakeChannel: fc, sendEntered: make(chan struct{}), sendRelease: make(chan struct{})}
	installRestartBarrier(b, c)
	s, conn := bindingSession(t, b, drainSrc(), "claude", os.Getpid(), t.TempDir())
	frames := restartFrames(t, conn)
	done := make(chan struct{})
	go func() {
		b.handlePermissionRequest(nil, s, []byte(`{"request_id":"p","tool_name":"Bash"}`))
		close(done)
	}()
	restartWait(t, c.sendEntered)
	b.BeginControlledRestart()
	b.handlePermissionSettled(nil, s, []byte(`{"request_id":"p","outcome":"allow"}`))
	b.cancelRestartPrompts()
	close(c.sendRelease)
	restartWait(t, done)
	edits := fc.editSnapshot()
	if len(edits) != 1 || !strings.Contains(edits[0].Text, "Allowed in the CLI") || strings.Contains(edits[0].Text, "cancelled") {
		t.Fatalf("false cancellation: %+v", edits)
	}
	assertNoRestartFrame(t, frames)
}

func TestControlledRestartRegistrationCrossesAdmissionBoundary(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, _ := restartBroker(t, nil)
	if !b.prompts.begin(true) {
		t.Fatal("admission refused")
	}
	b.BeginControlledRestart()
	b.cancelRestartPrompts()
	if _, ok := b.Asks.register(&pendingAsk{askID: "late"}); ok {
		t.Fatal("late ask admitted")
	}
	if _, ok := b.Perms.register(&pendingPerm{requestID: "late"}); ok {
		t.Fatal("late permission admitted")
	}
	b.prompts.end()
}

func TestControlledRestartCapRacesAnswerExactlyOnce(t *testing.T) {
	for _, kind := range []string{"ask", "permission"} {
		t.Run(kind, func(t *testing.T) {
			clearFetchTestEnvironment(t)
			b, _ := restartBroker(t, nil)
			s, c := bindingSession(t, b, drainSrc(), "claude", os.Getpid(), t.TempDir())
			frames := restartFrames(t, c)
			if kind == "ask" {
				b.Asks.register(&pendingAsk{askID: "q", route: drainSrc(), owner: s, question: "Pick", options: []string{"yes"}, messageID: 71})
			} else {
				registerRestartPerm(t, b, s)
			}
			b.BeginControlledRestart()
			start := make(chan struct{})
			done := make(chan struct{})
			go func() {
				<-start
				if kind == "ask" {
					b.resolveAsk(drainSrc(), &c3types.CallbackEvent{Data: "ask:q:0", MessageID: 71})
				} else {
					b.resolvePerm(drainSrc(), restartTap())
				}
				close(done)
			}()
			close(start)
			b.cancelRestartPrompts()
			restartWait(t, done)
			if b.Asks.has("q") || b.Perms.has("pending-perm") {
				t.Fatal("entry revived")
			}
			if kind == "ask" {
				restartFrame(t, frames)
			} else {
				select {
				case raw := <-frames:
					var v ipc.PermissionVerdictMsg
					json.Unmarshal(raw, &v)
					if v.Behavior != "deny" {
						t.Fatal(string(raw))
					}
				default:
				}
			}
			assertNoRestartFrame(t, frames)
		})
	}
}

func TestControlledRestartRejectFloodDoesNotStarve(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, _ := restartBroker(t, nil)
	s, c := bindingSession(t, b, drainSrc(), "claude", os.Getpid(), t.TempDir())
	registerRestartPerm(t, b, s)
	b.BeginControlledRestart()
	floodDone := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(floodDone)
		for {
			select {
			case <-stop:
				return
			default:
				b.handlePermissionRequest(nil, s, []byte(`{"request_id":"new","tool_name":"Bash"}`))
			}
		}
	}()
	drainDone := make(chan struct{})
	go func() { b.DrainRequests(); close(drainDone) }()
	b.handlePermissionSettled(nil, s, []byte(`{"request_id":"pending-perm","outcome":"allow"}`))
	restartWait(t, drainDone)
	close(stop)
	restartWait(t, floodDone)
	if b.Perms.has("new") {
		t.Fatal("flood admitted")
	}
	c.Close()
}

func TestControlledRestartSuppressesDirectAdvisoryPushes(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, fc := restartBroker(t, nil)
	s, c := bindingSession(t, b, drainSrc(), "claude", os.Getpid(), t.TempDir())
	frames := restartFrames(t, c)
	b.BeginControlledRestart()
	if b.sendSystemEventTo(s, &c3types.SystemEvent{Source: "telegram", Message: "test"}) {
		t.Fatal("direct advisory delivered")
	}
	result := &DrainResult{Appended: 1, TargetPending: 1}
	b.drainAdvisory(DrainSpec{Target: drainSrc()}, result)
	if result.NudgeSent || len(fc.sendRepliesSnapshot()) != 1 {
		t.Fatal("drain fallback missing")
	}
	assertNoRestartFrame(t, frames)
}

func TestBrokerIgnoresObsoleteRequestCheckpoints(t *testing.T) {
	clearFetchTestEnvironment(t)
	dir := t.TempDir()
	t.Setenv("C3_QUEUE_DIR", dir)
	path := filepath.Join(dir, "obsolete.request.json")
	data := []byte(`{"broken":`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	b, _ := restartBroker(t, nil)
	if b.Queue == nil || len(b.Asks.m)+len(b.Perms.m) != 0 {
		t.Fatal("obsolete checkpoint affected startup")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(data) {
		t.Fatal("obsolete file touched")
	}
}

type restartWedgedChannel struct {
	*permFakeChannel
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (c *restartWedgedChannel) EditMessage(a c3types.EditArgs) (*c3types.EditResult, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.permFakeChannel.EditMessage(a)
}
func TestControlledRestartCancellationNoticeFailureIsBounded(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, fc := restartBroker(t, nil)
	c := &restartWedgedChannel{permFakeChannel: fc, release: make(chan struct{}), entered: make(chan struct{})}
	b.chMu.Lock()
	b.channels[c.Name()] = &channelRegistration{Channel: c}
	b.chMu.Unlock()
	b.Asks.register(&pendingAsk{askID: "q", route: drainSrc(), question: "Pick", messageID: 71})
	b.BeginControlledRestart()
	start := time.Now()
	b.cancelRestartPromptsWithin(20 * time.Millisecond)
	if time.Since(start) > time.Second || b.Asks.has("q") {
		t.Fatal("notification wedge held cancellation")
	}
	restartWait(t, c.entered)
	close(c.release)
}

func TestControlledRestartAdministrativeReplyPrecedesTeardown(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, _ := restartBroker(t, nil)
	s, c := bindingSession(t, b, drainSrc(), "c3-broker-cli", os.Getpid(), t.TempDir())
	done := make(chan struct{})
	b.RestartRequested = func() { go func() { b.DrainRequests(); close(done) }() }
	handled := make(chan struct{})
	go func() { b.handleBrokerRestart(s.ConnValue().(*ipc.Conn), s); close(handled) }()
	// The reply is deliberately unread: teardown must wait for its write.
	select {
	case <-done:
		t.Fatal("teardown raced restart acknowledgement")
	case <-time.After(20 * time.Millisecond):
	}
	raw, err := c.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var reply ipc.BrokerRestartReply
	if err := json.Unmarshal(raw, &reply); err != nil || !reply.OK {
		t.Fatalf("%s %v", raw, err)
	}
	restartWait(t, handled)
	restartWait(t, done)
}
