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
	"github.com/Andrometiq/c3/internal/queue"
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
	// answer, when set, decides per file_id — for messages carrying several
	// voice attachments with different fates.
	answer func(fileID string) (int64, error)
}

func (g *gateChannel) AttachmentSize(fileID string) (int64, error) {
	g.calls.Add(1)
	if g.answer != nil {
		return g.answer(fileID)
	}
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
		{"rich-message marker text", "[voice_note]"},
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

// ── Codex review 2, finding 2 — position is not identity ──────────────────────
//
// A rich message decodes its blocks in order, so a photo block followed by a
// voice_note block yields attachments [photo, voice]. That real shape is
// established through the actual decoder in
// internal/channel/telegram/voicegate_richvoice_test.go
// (TestConvertInbound_RichVoiceAfterPhoto_KeepsVoiceAttachment); these tests
// feed the SAME shape to the broker, which used to look only at Attachments[0]
// and therefore never entered the voice path at all.

// photoThenVoice is the decoded shape of a rich message whose blocks are photo,
// then voice_note.
func photoThenVoice(msgID int64, voiceSize int64) *c3types.Inbound {
	return &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: msgID,
		Text: "[photo]\n\n[voice_note]",
		Attachments: []c3types.Attachment{
			{Kind: "photo", FileID: "P1", Size: 10},
			{Kind: "voice", FileID: "V1", MIME: "audio/ogg", Size: voiceSize},
		},
	}
}

// A voice note that is not the first attachment must still be transcribed.
func TestFlushInbounds_VoiceNotFirstAttachment_IsTranscribed(t *testing.T) {
	g := newGateChannel(1000, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()
	calls := countingSTT(b, "the second attachment spoke")

	in := photoThenVoice(7101, 1000)
	flushVoice(t, b, in)

	if got := calls.Load(); got != 1 {
		t.Fatalf("STT ran %d time(s) for a voice note sitting at attachment index 1; want 1 — checking only Attachments[0] drops the audio with no fetch, no marker and no notice", got)
	}
	if !strings.Contains(in.Text, "the second attachment spoke") {
		t.Fatalf("the transcript never reached the agent surface; got %q", in.Text)
	}
	if !strings.Contains(in.Text, "[photo]") {
		t.Fatalf("the rich message's own block markers were destroyed; got %q", in.Text)
	}
}

// …and one the server refuses must still produce BOTH honest surfaces.
func TestFlushInbounds_VoiceNotFirstAttachment_RefusalStillSurfaces(t *testing.T) {
	g := newGateChannel(0, tooBigErr())
	b := gateBroker(t, g)
	defer b.Shutdown()
	calls := countingSTT(b, "should never be produced")

	in := photoThenVoice(7102, incidentVoiceBytes)
	flushVoice(t, b, in)

	if got := calls.Load(); got != 0 {
		t.Fatalf("STT ran %d time(s) on a refused file; want 0", got)
	}
	if !strings.Contains(in.Text, "[voice too big:") {
		t.Fatalf("a refused voice note at index 1 produced no agent marker — the audio vanishes silently; got %q", in.Text)
	}
	if !strings.Contains(in.Text, "20.2 MB") {
		t.Fatalf("the marker must describe THIS attachment's size, not another attachment's; got %q", in.Text)
	}
	g.waitReplyContaining(t, "over the bot server's size limit")
}

// Multi-voice: the first keeps single-voice semantics, every additional one
// APPENDS. Neither is dropped, and a refusal mixed with a transcript reports
// both to the human.
func TestFlushInbounds_TwoVoiceAttachments_BothSurface(t *testing.T) {
	g := newGateChannel(100, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()
	var n atomic.Int64
	b.Plugins.OnVoiceReceived(func(_ context.Context, p c3types.VoicePayload) (string, error) {
		n.Add(1)
		return "transcript of " + p.FileID, nil
	})

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 7103,
		Attachments: []c3types.Attachment{
			{Kind: "voice", FileID: "V1", Size: 100},
			{Kind: "voice", FileID: "V2", Size: 200},
		},
	}
	flushVoice(t, b, in)

	if got := n.Load(); got != 2 {
		t.Fatalf("STT ran %d time(s) for two voice attachments; want 2 — the second one is a message the user sent and must not be discarded", got)
	}
	for _, want := range []string{"transcript of V1", "transcript of V2"} {
		if !strings.Contains(in.Text, want) {
			t.Fatalf("agent surface is missing %q; got %q", want, in.Text)
		}
	}
}

// The recovery text names the attachment it is ABOUT. With a photo first, the
// old code quoted Attachments[0].FileID and sent the agent to download the photo.
func TestSTTFailureText_NamesItsOwnAttachment(t *testing.T) {
	g := newGateChannel(1000, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		return "", nil // STT ran and produced nothing
	})

	in := photoThenVoice(7104, 1000)
	in.Text = "" // let the failure text own the surface
	flushVoice(t, b, in)

	if !strings.Contains(in.Text, `file_id="V1"`) {
		t.Fatalf("the recovery instruction must name the VOICE attachment; got %q", in.Text)
	}
	if strings.Contains(in.Text, `file_id="P1"`) {
		t.Fatalf("the recovery instruction names the photo — the agent would download the wrong attachment; got %q", in.Text)
	}
}

// ── Codex review 2, finding 1 — the handler's own fetch failure ───────────────
//
// The STT handler performs its OWN getFile, so the broker's preflight cannot
// speak for it (a failover advance between the two calls, or plain TOCTOU). When
// that second fetch fails, its cause used to collapse to "[STT FAILED: error]"
// and then to the generic transcription-failed text — the exact opacity the
// ruling forbids.
func TestFlushInbounds_HandlerFetchFailure_ReportsTheServersWordsNotAGenericFailure(t *testing.T) {
	g := newGateChannel(1000, nil) // preflight SUCCEEDS; the handler's fetch is what fails
	b := gateBroker(t, g)
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		return "[STT FETCH FAILED: getFile failed (error_code=400): Bad Request: file is too big]", nil
	})

	in := voiceInbound(7105, incidentVoiceBytes)
	flushVoice(t, b, in)

	if !strings.Contains(in.Text, "Bad Request: file is too big") {
		t.Fatalf("the handler's fetch failure lost the server's actual error on the way up; got %q", in.Text)
	}
	if strings.Contains(in.Text, "saved and recoverable") {
		t.Fatalf("a fetch that never delivered any bytes still claims the audio is saved and recoverable; got %q", in.Text)
	}
	if strings.Contains(in.Text, "[STT FAILED:") {
		t.Fatalf("a fetch failure is still labelled a transcription failure, which names the wrong subsystem; got %q", in.Text)
	}
	g.waitReplyContaining(t, "Bad Request: file is too big")
	for _, rp := range g.sendRepliesSnapshot() {
		if strings.Contains(rp.Text, "try again") {
			t.Fatalf("the human is told to try again for a fetch the server refused; got %q", rp.Text)
		}
	}
}

// A single message can transcribe one voice and have another refused. The human
// must hear BOTH — the readback for what worked and the notice for what did not.
// They used to be either/or, so a mixed message told the human only half of it.
func TestFlushInbounds_MixedTranscriptAndRefusal_HumanHearsBoth(t *testing.T) {
	g := newGateChannel(0, nil)
	// V1 is servable, V2 is refused.
	g.answer = func(fileID string) (int64, error) {
		if fileID == "V2" {
			return 0, tooBigErr()
		}
		return 100, nil
	}
	b := gateBroker(t, g)
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(_ context.Context, p c3types.VoicePayload) (string, error) {
		return "transcript of " + p.FileID, nil
	})

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 7106,
		Attachments: []c3types.Attachment{
			{Kind: "voice", FileID: "V1", Size: 100},
			{Kind: "voice", FileID: "V2", Size: incidentVoiceBytes},
		},
	}
	flushVoice(t, b, in)

	if !strings.Contains(in.Text, "transcript of V1") {
		t.Fatalf("the servable voice never transcribed; got %q", in.Text)
	}
	if !strings.Contains(in.Text, "[voice too big:") {
		t.Fatalf("the refused voice produced no marker; got %q", in.Text)
	}
	// The refusal notice must be sent even though another attachment transcribed.
	g.waitReplyContaining(t, "over the bot server's size limit")
	// …and the transcript must still be read back.
	rbs := g.waitReadbacks(t, 1)
	if !strings.Contains(rbs[0].Transcript, "transcript of V1") {
		t.Fatalf("readback = %q, want the transcript that succeeded", rbs[0].Transcript)
	}
}

// ── Codex review 3, finding 2 — nothing may be invisible ─────────────────────

// The reviewer's exact trigger: voice(V1 ordinary STT failure), paragraph,
// voice(V2 succeeds). V1's failure used to reach NEITHER surface — the agent
// text was non-empty (rich block markers) so no failure text was written, and
// V2's success made the readback fire instead of any notice.
func TestFlushInbounds_OneVoiceFailsAnotherSucceeds_FailureIsNotSwallowed(t *testing.T) {
	g := newGateChannel(100, nil)
	b := gateBroker(t, g)
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(_ context.Context, p c3types.VoicePayload) (string, error) {
		if p.FileID == "V1" {
			return "", nil // fetched fine; the provider chain produced nothing
		}
		return "transcript of V2", nil
	})

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 7200,
		Text: "[voice_note]\n\nand another\n\n[voice_note]", // rich block markers
		Attachments: []c3types.Attachment{
			{Kind: "voice", FileID: "V1", Size: 100},
			{Kind: "voice", FileID: "V2", Size: 100},
		},
	}
	flushVoice(t, b, in)

	if !strings.Contains(in.Text, "transcription failed") {
		t.Fatalf("V1's STT failure never reached the agent — rich messages always have block text, so the old empty-text guard never fired; got %q", in.Text)
	}
	if !strings.Contains(in.Text, `file_id="V1"`) {
		t.Fatalf("the failure must name the attachment that failed; got %q", in.Text)
	}
	if !strings.Contains(in.Text, "transcript of V2") {
		t.Fatalf("the successful attachment must still surface; got %q", in.Text)
	}
	// …and the human is told too, even though a readback fires for V2.
	g.waitReplyContaining(t, "Couldn't transcribe")
	rbs := g.waitReadbacks(t, 1)
	if !strings.Contains(rbs[0].Transcript, "transcript of V2") {
		t.Fatalf("readback = %q, want V2's transcript", rbs[0].Transcript)
	}
}

// N refusals with DISTINCT causes: every distinct cause reaches the human.
// Keeping only the first silently dropped the later servers' words.
func TestFlushInbounds_DistinctRefusalCauses_AllReachTheHuman(t *testing.T) {
	g := newGateChannel(0, nil)
	g.answer = func(fileID string) (int64, error) {
		if fileID == "V1" {
			return 0, tooBigErr()
		}
		return 0, errors.New("telegram: GetFile: unable to getFile: Bad Request: file reference expired")
	}
	b := gateBroker(t, g)
	defer b.Shutdown()
	countingSTT(b, "unused")

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 7201,
		Attachments: []c3types.Attachment{
			{Kind: "voice", FileID: "V1", Size: incidentVoiceBytes},
			{Kind: "voice", FileID: "V2", Size: 100},
		},
	}
	flushVoice(t, b, in)

	g.waitReplyContaining(t, "file reference expired")
	joined := ""
	for _, rp := range g.sendRepliesSnapshot() {
		joined += rp.Text + "\n"
	}
	if !strings.Contains(joined, "over the bot server's size limit") {
		t.Fatalf("the FIRST refusal's cause was dropped from the human surface; got %q", joined)
	}
}

// Identical causes are not repeated at the human — two attachments refused for
// the same reason is one thing to say, not two.
func TestFlushInbounds_IdenticalRefusalCauses_AreNotRepeated(t *testing.T) {
	g := newGateChannel(0, tooBigErr())
	b := gateBroker(t, g)
	defer b.Shutdown()
	countingSTT(b, "unused")

	in := &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: 7202,
		Attachments: []c3types.Attachment{
			{Kind: "voice", FileID: "V1", Size: 100},
			{Kind: "voice", FileID: "V2", Size: 100},
		},
	}
	flushVoice(t, b, in)

	g.waitReplyContaining(t, "over the bot server's size limit")
	for _, rp := range g.sendRepliesSnapshot() {
		if n := strings.Count(rp.Text, "over the bot server's size limit"); n > 1 {
			t.Fatalf("the same cause was reported %d times in one notice; got %q", n, rp.Text)
		}
	}
}

// ── Codex review 3, finding 4 — retranscribe ─────────────────────────────────

// A fetch marker is a FAILURE. It used to be handed back as Text (and written
// into the queued message) as though it were a transcript.
func TestHandleRetranscribe_HandlerFetchFailure_IsAnErrorNotATranscript(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &probeChannel{fakeChannel: &fakeChannel{}, size: 100}
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		return "[STT FETCH FAILED: getFile failed (error_code=400): Bad Request: file is too big]", nil
	})

	resp := retranscribeOn(t, b, "V1")

	if resp.Text != "" {
		t.Fatalf("a control marker was returned as a transcript: %q — the agent would treat it as the user's words, and it would be written into the queued message", resp.Text)
	}
	if !strings.Contains(resp.Err, "Bad Request: file is too big") {
		t.Fatalf("retranscribe must report the fetch cause the server gave; got %q", resp.Err)
	}
}

// …and so is an ordinary STT failure marker.
func TestHandleRetranscribe_STTFailureMarker_IsAnErrorNotATranscript(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &probeChannel{fakeChannel: &fakeChannel{}, size: 100}
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		return "[STT FAILED: handler_missing — see /tmp/broker.log]", nil
	})

	resp := retranscribeOn(t, b, "V1")

	if resp.Text != "" {
		t.Fatalf("an STT failure marker was returned as a transcript: %q", resp.Text)
	}
	if resp.Err == "" {
		t.Fatal("a failure marker must be reported as an error")
	}
}

// R3b — OWNERSHIP is a file identity, never a way the text reads.
func TestRecordOwnsVoice(t *testing.T) {
	voice := c3types.Attachment{Kind: "voice", FileID: "V1"}
	photo := c3types.Attachment{Kind: "photo", FileID: "P1"}

	for _, tc := range []struct {
		name   string
		rec    c3types.Inbound
		fileID string
		want   bool
	}{
		{"the voice this refresh is for", c3types.Inbound{Attachments: []c3types.Attachment{voice}}, "V1", true},
		{"a different voice's file_id", c3types.Inbound{Attachments: []c3types.Attachment{voice}}, "V9", false},
		{"a photo-only record", c3types.Inbound{Attachments: []c3types.Attachment{photo}}, "V1", false},
		{"no file_id at all", c3types.Inbound{Attachments: []c3types.Attachment{voice}}, "", false},
		{"two voices — which segment is whose is unknowable", c3types.Inbound{Attachments: []c3types.Attachment{voice, {Kind: "voice", FileID: "V2"}}}, "V1", false},
		{"the voice alongside a photo", c3types.Inbound{Attachments: []c3types.Attachment{photo, voice}}, "V1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := recordOwnsVoice(tc.rec, tc.fileID); got != tc.want {
				t.Fatalf("recordOwnsVoice(%+v, %q) = %v, want %v — a refresh that cannot prove the message is about this voice can rewrite one it is not",
					tc.rec.Attachments, tc.fileID, got, tc.want)
			}
		})
	}
}

// R3a/R3c — only C3's own trailing segment is replaced, and it is written back
// in the shape that lets the NEXT refresh find it.
func TestReplaceC3VoiceSegment(t *testing.T) {
	const prefix = "[Transcribed voice]: "

	for _, tc := range []struct {
		name, text, want string
		ok               bool
	}{
		{
			name: "C3's write is the whole text",
			text: prefix + "old words",
			want: prefix + "NEW", ok: true,
		},
		{
			// R3a: the user's caption opens with the transcript prefix. Matching
			// on a prefix alone let retranscribe replace the user's own words.
			name: "a user caption that opens exactly like a transcript",
			text: prefix + "the user's own caption\n" + prefix + "old words",
			want: prefix + "the user's own caption\n" + prefix + "NEW", ok: true,
		},
		{
			name: "rich block markers in front",
			text: "[photo]\n\n[voice_note]\n" + prefix + "old words",
			want: "[photo]\n\n[voice_note]\n" + prefix + "NEW", ok: true,
		},
		{
			name: "a failure placeholder is C3's segment too",
			text: "caption\n" + sttFailureOpening + " no_transcript] …",
			want: "caption\n" + prefix + "NEW", ok: true,
		},
		{
			name: "a refusal marker is C3's segment too",
			text: "caption\n" + voiceTooBigOpening + " …]",
			want: "caption\n" + prefix + "NEW", ok: true,
		},
		{
			name: "a download failure placeholder is C3's segment too",
			text: "caption\n" + voiceFetchFailedOpening + " getFile refused]",
			want: "caption\n" + prefix + "NEW", ok: true,
		},
		{
			// A multi-line transcript must not be split on its own newlines.
			name: "C3's transcript spans several lines",
			text: "caption\n" + prefix + "line one\nline two\nline three",
			want: "caption\n" + prefix + "NEW", ok: true,
		},
		{
			// An opening mid-sentence is the speaker's words, not a segment.
			name: "the prefix appears mid-line inside speech",
			text: prefix + "he said " + prefix + "quote unquote",
			want: prefix + "NEW", ok: true,
		},
		{
			name: "nothing C3 wrote", text: "just the user talking", ok: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := replaceC3VoiceSegment(tc.text, prefix, "NEW")
			if ok != tc.ok {
				t.Fatalf("replaceC3VoiceSegment(%q) ok=%v, want %v — a C3-authored trailing segment went stale after successful retranscription", tc.text, ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Fatalf("replaceC3VoiceSegment(%q)\n got %q\nwant %q — everything before C3's own segment belongs to someone else", tc.text, got, tc.want)
			}
		})
	}
}

// R3c — the shape written back must be the shape recognized, or refresh works
// exactly once and the queue keeps the first refreshed text forever.
func TestReplaceC3VoiceSegment_IsRepeatable(t *testing.T) {
	const prefix = "[Transcribed voice]: "

	once, ok := replaceC3VoiceSegment("caption\n"+prefix+"first", prefix, "second")
	if !ok {
		t.Fatal("first refresh found no segment")
	}
	twice, ok := replaceC3VoiceSegment(once, prefix, "third")
	if !ok {
		t.Fatalf("second refresh found no segment in %q — the first one stored a shape it cannot recognize, so the queued text is stuck at the first refresh forever", once)
	}
	if twice != "caption\n"+prefix+"third" {
		t.Fatalf("second refresh produced %q, want the caption plus the newest transcript", twice)
	}
}

// The guard has to be WIRED IN, not merely correct. A rich photo+voice message
// IS refreshable — the voice is identified and only C3's own trailing segment is
// rewritten — but everything that arrived with the message must survive it.
// Store.RefreshText replaces the whole field, so without the segment rule a
// successful retranscribe of one voice wipes the block markers with it.
func TestHandleRetranscribe_RefreshPreservesEverythingItDoesNotOwn(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &probeChannel{fakeChannel: &fakeChannel{}, size: 100}
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		return "fresh transcript", nil
	})

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	const stored = "[photo]\n\n[Transcribed voice]: the original words"
	_ = b.Queue.Append(qrk, &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 5,
		Text: stored,
		Attachments: []c3types.Attachment{
			{Kind: "photo", FileID: "P1"},
			{Kind: "voice", FileID: "V1"},
		},
		Timestamp: time.Now(),
	})

	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)

	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "1", FileID: "V1", MessageID: 5})
	go b.handleRetranscribe(brokerSide, stub, raw)
	resp := readRetranscribeResp(t, agentSide)

	// The agent still gets its transcript — the refresh is best-effort, the
	// answer is not.
	if resp.Text != "fresh transcript" {
		t.Fatalf("retranscribe text = %q, want the transcript returned even when the refresh is refused", resp.Text)
	}

	fraw, _ := json.Marshal(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "2", All: true, Ack: false})
	go b.handleFetchQueue(brokerSide, stub, fraw)
	fresp := readFetchResp(t, agentSide)
	if len(fresp.Messages) != 1 {
		t.Fatalf("fetch_queue returned %d messages, want 1", len(fresp.Messages))
	}
	got := fresp.Messages[0].Text
	if !strings.HasPrefix(got, "[photo]\n\n") {
		t.Fatalf("the queued message was rewritten to %q — the rich block markers that arrived with it were destroyed; only C3's own trailing segment may be replaced", got)
	}
	if !strings.Contains(got, "fresh transcript") {
		t.Fatalf("the refreshed transcript never landed; stored text = %q", got)
	}
	if strings.Contains(got, "the original words") {
		t.Fatalf("C3's own previous segment was left behind alongside the new one; stored text = %q", got)
	}
}

// R3a, end to end: a single voice whose USER CAPTION opens exactly like a
// transcript. The old predicate accepted it and retranscribe replaced the user's
// words with the new transcript.
func TestHandleRetranscribe_DoesNotOverwriteACaptionThatLooksLikeATranscript(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &probeChannel{fakeChannel: &fakeChannel{}, size: 100}
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		return "fresh transcript", nil
	})

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	const caption = "[Transcribed voice]: this is the user's own caption"
	_ = b.Queue.Append(qrk, &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 5,
		Text:        caption + "\n[Transcribed voice]: the original transcript",
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "V1"}},
		Timestamp:   time.Now(),
	})

	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	agentSide, brokerSide := newConnPair(t)
	raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "1", FileID: "V1", MessageID: 5})
	go b.handleRetranscribe(brokerSide, stub, raw)
	if resp := readRetranscribeResp(t, agentSide); resp.Text != "fresh transcript" {
		t.Fatalf("retranscribe text = %q", resp.Text)
	}

	fraw, _ := json.Marshal(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "2", All: true, Ack: false})
	go b.handleFetchQueue(brokerSide, stub, fraw)
	fresp := readFetchResp(t, agentSide)
	got := fresp.Messages[0].Text
	if !strings.HasPrefix(got, caption) {
		t.Fatalf("the user's caption was destroyed; stored text = %q, want it to still open with %q", got, caption)
	}
	if !strings.Contains(got, "fresh transcript") {
		t.Fatalf("the refreshed transcript never landed; stored text = %q", got)
	}
}

// R3b, end to end: a record the transcript is NOT about must not be rewritten,
// even when the agent supplies a valid voice file_id and that message's id.
func TestHandleRetranscribe_RefusesAMessageThatDoesNotCarryThatVoice(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &probeChannel{fakeChannel: &fakeChannel{}, size: 100}
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		return "fresh transcript", nil
	})

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	const stored = "[Transcribed voice]: a caption on a photo-only message"
	_ = b.Queue.Append(qrk, &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 5,
		Text:        stored,
		Attachments: []c3types.Attachment{{Kind: "photo", FileID: "P1"}},
		Timestamp:   time.Now(),
	})

	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	agentSide, brokerSide := newConnPair(t)
	// A perfectly valid voice file_id — for a voice this message does not carry.
	raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: "1", FileID: "V-ELSEWHERE", MessageID: 5})
	go b.handleRetranscribe(brokerSide, stub, raw)
	_ = readRetranscribeResp(t, agentSide)

	fraw, _ := json.Marshal(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "2", All: true, Ack: false})
	go b.handleFetchQueue(brokerSide, stub, fraw)
	fresp := readFetchResp(t, agentSide)
	if got := fresp.Messages[0].Text; got != stored {
		t.Fatalf("a message that carries no such voice was rewritten to %q; want %q untouched — the refresh proved nothing about which message this transcript belongs to", got, stored)
	}
}

// R3c, end to end: refresh twice. The first refresh used to store the raw
// transcript, leaving nothing recognizable, so the second could never land.
func TestHandleRetranscribe_RefreshIsRepeatable(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &probeChannel{fakeChannel: &fakeChannel{}, size: 100}
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()
	var n atomic.Int64
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		if n.Add(1) == 1 {
			return "first refresh", nil
		}
		return "second refresh", nil
	})

	tid := int64(914)
	key := MakeRouteKey("telegram", -100, &tid)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
	_ = b.Queue.Append(qrk, &c3types.Inbound{
		Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 5,
		Text:        sttFailureOpening + " no_transcript] …",
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "V1"}},
		Timestamp:   time.Now(),
	})

	stub := claimedHolder(t, b, key)
	stub.SetRoute(&key)
	agentSide, brokerSide := newConnPair(t)

	for i, id := range []string{"1", "2"} {
		raw, _ := json.Marshal(ipc.RetranscribeReq{Op: ipc.OpRetranscribe, ID: id, FileID: "V1", MessageID: 5})
		go b.handleRetranscribe(brokerSide, stub, raw)
		if resp := readRetranscribeResp(t, agentSide); resp.Err != "" {
			t.Fatalf("retranscribe %d errored: %q", i+1, resp.Err)
		}
	}

	fraw, _ := json.Marshal(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "3", All: true, Ack: false})
	go b.handleFetchQueue(brokerSide, stub, fraw)
	fresp := readFetchResp(t, agentSide)
	if got := fresp.Messages[0].Text; !strings.Contains(got, "second refresh") {
		t.Fatalf("stored text = %q, want the SECOND refresh — storing the raw transcript left no shape to recognize, so refresh worked exactly once", got)
	}
}
