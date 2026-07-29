package broker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// Voice fetchability gate — 2026-07-27 incident, Ask #1
// (local-notes/INCIDENT-2026-07-27-voice-limit-ordering.md). A 21,226,288-byte
// voice note ran the STT chain twice (two HTTP 400s) and surfaced "The audio is
// saved and recoverable — the user does not need to resend", which was false on
// every clause.
//
// The rule these tests pin (maintainer, 2026-07-29): C3 owns NO size limit and
// compares nothing against one. The bot server decides; C3 asks it with a
// bodyless getFile before running STT, and reports what it hears.

const incidentVoiceBytes = int64(21226288) // msg 6994, 2026-07-27

// probeChannel answers the fetchability ask. size/err are what the transport
// says; calls counts how often it was asked.
type probeChannel struct {
	*fakeChannel
	size  int64
	err   error
	calls atomic.Int64
}

func (p *probeChannel) AttachmentSize(string) (int64, error) {
	p.calls.Add(1)
	return p.size, p.err
}

// gateChannel is a readback recorder that ALSO answers the fetchability ask, so
// one double drives both the skip decision and the human notice it produces.
type gateChannel struct {
	*readbackRecorderChannel
	size  int64
	err   error
	calls atomic.Int64
}

func (g *gateChannel) AttachmentSize(string) (int64, error) {
	g.calls.Add(1)
	return g.size, g.err
}

func newGateChannel(size int64, err error) *gateChannel {
	return &gateChannel{
		readbackRecorderChannel: &readbackRecorderChannel{fakeChannel: &fakeChannel{}},
		size:                    size,
		err:                     err,
	}
}

func registerGateChannel(b *Broker, g *gateChannel) {
	b.chMu.Lock()
	b.channels[g.Name()] = &channelRegistration{Channel: g}
	b.chMu.Unlock()
}

// tooBigErr is what a transport hands back when its server refuses on size: its
// OWN words, tagged with the sentinel that means "permanent, do not retry".
func tooBigErr() error {
	return errors.Join(
		errors.New("telegram: the bot server refused to download this file — unable to getFile: Bad Request: file is too big"),
		channel.ErrAttachmentTooLarge,
	)
}

func voiceInbound(msgID, size int64) *c3types.Inbound {
	return &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: msgID,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "F-BIG", MIME: "audio/ogg", Size: size}},
	}
}

// countingSTT registers a voice callback that records how often it ran. The
// count is the point: on a file the server refuses, the handler must not run AT
// ALL (today it runs, fails, and logs an HTTP 400).
func countingSTT(b *Broker, transcript string) *atomic.Int64 {
	var calls atomic.Int64
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		calls.Add(1)
		return transcript, nil
	})
	return &calls
}

func gateBroker(t *testing.T, g *gateChannel) *Broker {
	t.Helper()
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	registerGateChannel(b, g)
	return b
}

func flushVoice(t *testing.T, b *Broker, in *c3types.Inbound) {
	t.Helper()
	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})
}

// The headline behavior: the server refuses on size, STT never runs, the agent
// is told the truth in its own distinct marker, and the human is offered the two
// routes that keep the original audio.
func TestFlushInbounds_ServerRefusesOnSize_SkipsSTTAndTellsTheTruth(t *testing.T) {
	g := newGateChannel(0, tooBigErr())
	b := gateBroker(t, g)
	defer b.Shutdown()
	calls := countingSTT(b, "should never be produced")

	in := voiceInbound(6994, incidentVoiceBytes)
	flushVoice(t, b, in)

	if got := calls.Load(); got != 0 {
		t.Fatalf("STT ran %d time(s) on a file the bot server refuses to serve; want 0 — transcription opens with that same fetch", got)
	}
	if !strings.Contains(in.Text, "[voice too big:") {
		t.Fatalf("agent surface must carry the distinct too-big marker; got %q", in.Text)
	}
	if strings.Contains(in.Text, "saved and recoverable") {
		t.Fatalf("agent surface still claims the audio is saved and recoverable — false for a file that was never fetched; got %q", in.Text)
	}
	if !strings.Contains(in.Text, "file is too big") {
		t.Fatalf("agent surface must quote the SERVER's own refusal, not a cause C3 decided for it; got %q", in.Text)
	}
	if !strings.Contains(in.Text, "20.2 MB") {
		t.Fatalf("agent surface must name the size the update stated when it stated one; got %q", in.Text)
	}
	if !strings.Contains(in.Text, "SPLIT that same file") {
		t.Fatalf("agent surface must name the recovery that works — share or split the SAME file; got %q", in.Text)
	}

	g.waitReplyContaining(t, "over the bot server's size limit")
	for _, rp := range g.sendRepliesSnapshot() {
		if strings.Contains(rp.Text, "try again") {
			t.Fatalf("human notice tells the sender to try again, which cannot work for a file the server refuses; got %q", rp.Text)
		}
	}

	// Neither surface may mention re-recording, in ANY direction. Whether to
	// re-record is the sender's own call; recommending it throws away audio that
	// is perfectly fine, and forbidding it puts the idea in their head for no
	// reason (maintainer, 2026-07-29). We suggest the two routes that keep the
	// original file and stay silent on the rest.
	surfaces := []string{in.Text}
	for _, rp := range g.sendRepliesSnapshot() {
		surfaces = append(surfaces, rp.Text)
	}
	for _, text := range surfaces {
		if strings.Contains(strings.ToLower(text), "record") {
			t.Fatalf("a refusal surface talks about re-recording; it must simply suggest sharing or splitting the same file and say nothing about recording. got %q", text)
		}
	}
}

// No local ceiling exists any more, so SIZE ALONE never refuses. A 21 MB note
// the server is willing to serve must transcribe — that is the false refusal
// this rule exists to prevent, and it is exactly what a self-hosted Bot API
// server does ("Download files without a size limit").
func TestFlushInbounds_ServerServesLargeFile_StillTranscribes(t *testing.T) {
	g := newGateChannel(incidentVoiceBytes, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()
	calls := countingSTT(b, "transcribed a large file")

	in := voiceInbound(6995, incidentVoiceBytes)
	flushVoice(t, b, in)

	if got := calls.Load(); got != 1 {
		t.Fatalf("STT ran %d time(s) on a 21 MB file the server agreed to serve; want 1 — C3 must hold no size limit of its own", got)
	}
	if !strings.Contains(in.Text, "transcribed a large file") {
		t.Fatalf("a file the server serves must surface its transcript; got %q", in.Text)
	}
}

// Telegram marks Voice.file_size Optional, so an update that omits it must be
// answered by ASKING, not by assuming the file is small.
func TestFlushInbounds_UnstatedSize_StillAsksTheServer(t *testing.T) {
	g := newGateChannel(0, tooBigErr())
	b := gateBroker(t, g)
	defer b.Shutdown()
	calls := countingSTT(b, "should never be produced")

	in := voiceInbound(6996, 0) // Telegram omitted file_size
	flushVoice(t, b, in)

	if got := g.calls.Load(); got != 1 {
		t.Fatalf("the server was asked %d time(s) about a voice note with no stated size; want 1 — an unstated size is unknown, not small", got)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("STT ran %d time(s) on a refused file whose size the update omitted; want 0", got)
	}
	if strings.Contains(in.Text, "0 bytes") || strings.Contains(in.Text, "0.0 MB") {
		t.Fatalf("a size that was never stated must not be reported as a number; got %q", in.Text)
	}
}

// A refusal C3 cannot classify is passed through VERBATIM rather than dressed as
// a transcription failure. Transparent beats classified: this is what happens
// the day Telegram rewords "file is too big", and it still tells both sides the
// truth instead of blaming the STT provider.
func TestFlushInbounds_UnrecognizedRefusal_PassesTheRealErrorThrough(t *testing.T) {
	g := newGateChannel(0, errors.New("telegram: GetFile: unable to getFile: Bad Request: file is too large"))
	b := gateBroker(t, g)
	defer b.Shutdown()
	calls := countingSTT(b, "should never be produced")

	in := voiceInbound(6997, incidentVoiceBytes)
	flushVoice(t, b, in)

	if got := calls.Load(); got != 0 {
		t.Fatalf("STT ran %d time(s) after the fetch had already failed; want 0 — the chain begins with that same fetch and would report a generic failure over a real error", got)
	}
	if !strings.Contains(in.Text, "file is too large") {
		t.Fatalf("the agent must see the server's ACTUAL words when C3 cannot classify them; got %q", in.Text)
	}
	if strings.Contains(in.Text, "saved and recoverable") {
		t.Fatalf("a download-classified failure must never carry the transcription-failed reassurance; got %q", in.Text)
	}
	g.waitReplyContaining(t, "file is too large")
}

// A channel that cannot be asked, or a route with no resolvable channel, must
// not become a refusal — the gate stands down and STT runs as before.
func TestFlushInbounds_NothingToAsk_StillTranscribes(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()
	// readbackRecorderChannel has no AttachmentSize method.
	registerReadbackChannel(b, &readbackRecorderChannel{fakeChannel: &fakeChannel{}})
	calls := countingSTT(b, "no probe available")

	in := voiceInbound(6998, incidentVoiceBytes)
	flushVoice(t, b, in)

	if got := calls.Load(); got != 1 {
		t.Fatalf("STT ran %d time(s) on a channel that cannot be asked; want 1 — an unanswerable question is not a refusal", got)
	}
}

// A caption is the sender's own words AND the refusal is the one thing the agent
// must not miss, so both survive. The old "only when the text is empty" rule
// dropped the refusal entirely whenever a caption — or a rich-voice
// "[voice_note]" marker — was present.
func TestFlushInbounds_RefusalIsAppendedToExistingText(t *testing.T) {
	for _, tc := range []struct{ name, existing string }{
		{"ordinary voice with a caption", "user-typed caption"},
		{"rich-message voice block", "[voice_note]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newGateChannel(0, tooBigErr())
			b := gateBroker(t, g)
			defer b.Shutdown()
			countingSTT(b, "unused")

			in := voiceInbound(6999, incidentVoiceBytes)
			in.Text = tc.existing
			flushVoice(t, b, in)

			if !strings.Contains(in.Text, tc.existing) {
				t.Fatalf("the sender's own text was clobbered; in.Text=%q, want it to still contain %q", in.Text, tc.existing)
			}
			if !strings.Contains(in.Text, "[voice too big:") {
				t.Fatalf("existing text swallowed the refusal — the agent is never told the audio was not fetched; in.Text=%q", in.Text)
			}
		})
	}
}

func brokerWithProbe(t *testing.T, pc *probeChannel) *Broker {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	b := New(mfWithTelegram())
	b.chMu.Lock()
	b.channels[pc.Name()] = &channelRegistration{Channel: pc}
	b.chMu.Unlock()
	return b
}

func retranscribeOn(t *testing.T, b *Broker, fileID string) ipc.RetranscribeResp {
	t.Helper()
	stub := &Stub{CLI: "claude"}
	stub.SetRoute(&RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 914})
	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "1", FileID: fileID})
	go b.handleRetranscribe(brokerSide, stub, raw)
	return readRetranscribeResp(t, agentSide)
}

// retranscribe on a refused file used to re-run the whole chain and answer "STT
// provider still failing (no transcript)" — sending the agent after a provider
// outage that does not exist. It must ask first and pass the real cause on.
func TestHandleRetranscribe_ServerRefuses_ReportsTheRealCause(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &probeChannel{fakeChannel: &fakeChannel{}, err: tooBigErr()}
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()
	calls := countingSTT(b, "should never be produced")

	resp := retranscribeOn(t, b, "F-BIG")

	if got := calls.Load(); got != 0 {
		t.Fatalf("retranscribe ran STT %d time(s) on a file the server will not serve; want 0", got)
	}
	if !strings.Contains(resp.Err, "file is too big") {
		t.Fatalf("retranscribe must report the SERVER's cause, not a generic provider failure; got %q", resp.Err)
	}
	if strings.Contains(resp.Err, "still failing") {
		t.Fatalf("retranscribe still blames the STT provider for a fetch the server refused; got %q", resp.Err)
	}
}

// A file the server agrees to serve transcribes, whatever its size.
func TestHandleRetranscribe_ServerServesFile_StillTranscribes(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &probeChannel{fakeChannel: &fakeChannel{}, size: incidentVoiceBytes}
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()
	calls := countingSTT(b, "fresh transcript")

	resp := retranscribeOn(t, b, "F-OK")

	if resp.Err != "" {
		t.Fatalf("a file the server serves was refused; err=%q", resp.Err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("STT ran %d time(s) for a servable file; want 1", got)
	}
}

// A channel with no probe cannot be asked, so retranscribe proceeds as before.
func TestAttachmentFetchRefusal_ChannelWithoutProbe_NeverRefuses(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	if got := b.attachmentFetchRefusal("telegram", "F-BIG"); got != "" {
		t.Fatalf("a channel that cannot be asked must not produce a refusal; got %q", got)
	}
}

// Integer MB collapses the two numbers a size message exists to compare: the
// incident file and a 20 MiB ceiling both floor to "20 MB".
func TestMBString_KeepsTheDecimal(t *testing.T) {
	if got := mbString(incidentVoiceBytes); got != "20.2 MB" {
		t.Fatalf("mbString(%d) = %q, want 20.2 MB — integer MB prints 20 MB and reads as UNDER a 20 MB limit", incidentVoiceBytes, got)
	}
}

// A size the update never carried must not be invented as "0.0 MB".
func TestSizeSuffix_UnstatedSizeSaysNothing(t *testing.T) {
	if got := sizeSuffix(0); got != "" {
		t.Fatalf("sizeSuffix(0) = %q, want empty — reporting a size Telegram never sent is a made-up fact", got)
	}
	if got := sizeSuffix(incidentVoiceBytes); !strings.Contains(got, "20.2 MB") {
		t.Fatalf("sizeSuffix must state a size that WAS given; got %q", got)
	}
}
