package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
)

const TestInjectChannel = "test-inject"
const TestInjectChatID int64 = -1

// EnableTestInjection is called only by the explicit scratch-server entrypoint,
// before accepting connections. Normal daemon startup never calls it. Keeping a
// separate channel namespace means every reply, edit, voice echo and route notice
// resolves to the local sink, including notices emitted long after intake.
func (b *Broker) EnableTestInjection() error {
	if b.Queue == nil {
		return errors.New("test injection requires a durable queue")
	}
	for name := range b.Mappings().Channels {
		if name != TestInjectChannel {
			return errors.New("test injection requires an isolated broker with no real channels configured")
		}
	}
	ch := &testInjectChannel{voices: map[string]testVoice{}}
	if err := b.RegisterChannel(ch); err != nil {
		return err
	}
	b.Plugins.OnVoiceReceived(ch.transcribe)
	b.testInjector = ch
	log.Print("WARNING: TEST INJECTION ENABLED; synthetic input only; all channel output goes to the local log")
	return nil
}

// Inject uses the same gated Host.Emit -> worker -> durable append -> debounce
// -> delivery/voice pipeline as Telegram. Accepted means admitted, not persisted;
// callers must observe durability separately. There is no Telegram offset/ack.
func (b *Broker) Inject(req ipc.TestInjectReq) ipc.TestInjectResp {
	r := ipc.TestInjectResp{Op: ipc.OpTestInject}
	if b.testInjector == nil {
		r.Err = "test injection disabled; start a scratch test-serve with --allow-test-inject"
		log.Print("WARNING: refused TEST INJECTION on broker without explicit opt-in")
		return r
	}
	if req.Count == 0 {
		req.Count = 1
	}
	if req.Kind == "" {
		req.Kind = "text"
	}
	if req.Topic < 1 || req.Text == "" || len(req.Text) > 64*1024 || req.Count < 1 || req.Count > 2 || req.VoiceDelayMS < 0 || req.VoiceDelayMS > 60000 || (req.Kind != "text" && req.Kind != "voice" && req.Kind != "photo") || (req.Kind != "voice" && req.VoiceDelayMS != 0) {
		r.Err = "require topic >= 1, nonempty text <= 64 KiB, count 1..2, kind text/voice/photo and voice delay 0..60000 ms"
		return r
	}
	if req.From == 0 {
		req.From = 1
	}
	h := NewBrokerHost(b, TestInjectChannel)
	for i := 0; i < req.Count; i++ {
		id := b.testInjector.seq.Add(1)
		in := &c3types.Inbound{Channel: TestInjectChannel, ChatID: TestInjectChatID, TopicID: &req.Topic, MessageID: id, Sender: c3types.Sender{UserID: req.From, Username: "test-user"}, Timestamp: time.Now().UTC(), TestInjected: true, Text: req.Text}
		if h.GateInbound(in) != channel.GateInboundAllow {
			r.Err = "synthetic route not allowlisted"
			return r
		}
		if req.Kind != "text" {
			fileID := fmt.Sprintf("test-%d", id)
			mime := "image/png"
			if req.Kind == "voice" {
				mime = "audio/ogg"
				b.testInjector.mu.Lock()
				b.testInjector.voices[fileID] = testVoice{req.Text, time.Duration(req.VoiceDelayMS) * time.Millisecond}
				b.testInjector.mu.Unlock()
				in.Text = ""
			}
			in.Attachments = []c3types.Attachment{{Kind: req.Kind, FileID: fileID, MIME: mime, Size: 1}}
		}
		if !h.Emit(in) {
			r.Err = "worker refused synthetic input"
			return r
		}
		r.MessageIDs = append(r.MessageIDs, id)
		log.Printf("TEST INJECT accepted topic=%d message_id=%d kind=%s source=test-inject", req.Topic, id, req.Kind)
	}
	r.Accepted = true
	return r
}

func (b *Broker) handleTestInject(conn *ipc.Conn, raw []byte) {
	var req ipc.TestInjectReq
	if err := ipc.StrictJSON(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.TestInjectResp{Op: ipc.OpTestInject, Err: "malformed test injection"})
		return
	}
	_ = conn.WriteJSON(b.Inject(req))
}

func (w *RouteWorker) logTestAttempt(token, phase string) {
	if w.key.Channel != TestInjectChannel || w.broker.testInjector == nil {
		return
	}
	for _, a := range w.broker.attempts.lookup(token, time.Now()) {
		if a.Route != w.key {
			continue
		}
		log.Printf("TEST ATTEMPT token=%s topic=%d transport=%s phase=%s members=%d retired=%d elapsed_ms=%d", token, w.key.TopicID, a.Transport, phase, len(a.Members), len(a.Retired), time.Since(a.Started).Milliseconds())
	}
}

type testVoice struct {
	text  string
	delay time.Duration
}
type testInjectChannel struct {
	seq    atomic.Int64
	mu     sync.Mutex
	voices map[string]testVoice
}

func (*testInjectChannel) Name() string                              { return TestInjectChannel }
func (*testInjectChannel) Start(context.Context, channel.Host) error { return nil }
func (*testInjectChannel) Stop() error                               { return nil }
func (*testInjectChannel) Capabilities() c3types.Capabilities        { return c3types.Capabilities{} }
func (ch *testInjectChannel) SendReply(a c3types.ReplyArgs) (int64, error) {
	log.Printf("TEST SINK reply topic=%s text=%q", TopicPtrStr(a.TopicID), a.Text)
	return ch.seq.Add(1), nil
}
func (*testInjectChannel) SendTyping(int64, *int64) error { return nil }
func (*testInjectChannel) EditMessage(a c3types.EditArgs) (*c3types.EditResult, error) {
	log.Printf("TEST SINK edit text=%q", a.Text)
	return &c3types.EditResult{}, nil
}
func (*testInjectChannel) React(c3types.ReactArgs) error { return nil }
func (*testInjectChannel) DownloadAttachment(string) (string, error) {
	return "", errors.New("synthetic attachment has metadata only")
}
func (*testInjectChannel) StopPoll(int64, int64) (*c3types.PollResult, error) {
	return nil, errors.New("test channel has no polls")
}
func (*testInjectChannel) CreateTopic(int64, string) (int64, error) {
	return 0, errors.New("use an explicit test topic id")
}
func (*testInjectChannel) ValidateTopic(chat, topic int64) error {
	if chat != TestInjectChatID || topic < 1 {
		return errors.New("invalid test route")
	}
	return nil
}
func (ch *testInjectChannel) transcribe(ctx context.Context, v c3types.VoicePayload) (string, error) {
	ch.mu.Lock()
	voice, ok := ch.voices[v.FileID]
	ch.mu.Unlock()
	if v.Channel != TestInjectChannel || !ok {
		return "", errors.New("unknown synthetic voice")
	}
	defer func() { ch.mu.Lock(); delete(ch.voices, v.FileID); ch.mu.Unlock() }()
	timer := time.NewTimer(voice.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-timer.C:
		return voice.text, nil
	}
}
