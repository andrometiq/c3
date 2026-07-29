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

// Voice size gate — 2026-07-27 incident, Ask #1
// (local-notes/INCIDENT-2026-07-27-voice-limit-ordering.md). A 21,226,288-byte
// voice note ran the STT chain twice (two HTTP 400s) and surfaced "The audio is
// saved and recoverable — the user does not need to resend", which was false on
// every clause. Telegram's getFile ceiling is a hard 20MB
// (https://core.telegram.org/bots/api#getfile), and C3 already had the size on
// the incoming update.

const (
	incidentVoiceBytes = int64(21226288) // msg 6994, 2026-07-27
	botDownloadLimit   = int64(20 * 1024 * 1024)
)

// capsWithDownloadLimit builds a telegram-shaped manifest declaring limit as the
// inbound download ceiling — the ONLY place the gate reads the number from.
func capsWithDownloadLimit(limit int64) *c3types.Capabilities {
	return &c3types.Capabilities{
		Channel: "telegram",
		Inbound: c3types.InboundCaps{MaxDownloadBytes: limit},
	}
}

// gateChannel is a readback recorder that also declares a download ceiling, so
// one double drives both the skip decision and the human notice it produces.
func gateChannel(limit int64) *readbackRecorderChannel {
	return &readbackRecorderChannel{fakeChannel: &fakeChannel{caps: capsWithDownloadLimit(limit)}}
}

func voiceInbound(msgID, size int64) *c3types.Inbound {
	return &c3types.Inbound{
		Channel: "telegram", ChatID: -100, MessageID: msgID,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "F-BIG", MIME: "audio/ogg", Size: size}},
	}
}

// countingSTT registers a voice callback that records how many times it ran and
// returns transcript. The count is the point: over the ceiling the handler must
// not run AT ALL (today it runs, fails, and logs an HTTP 400).
func countingSTT(b *Broker, transcript string) *atomic.Int64 {
	var calls atomic.Int64
	b.Plugins.OnVoiceReceived(func(context.Context, c3types.VoicePayload) (string, error) {
		calls.Add(1)
		return transcript, nil
	})
	return &calls
}

// The headline behavior: over the ceiling, STT never runs, the agent is told the
// truth in its own distinct marker, and the human is told the size, the limit,
// and the only action that works.
func TestFlushInbounds_VoiceOverDownloadLimit_SkipsSTTAndTellsTheTruth(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	rc := gateChannel(botDownloadLimit)
	registerReadbackChannel(b, rc)
	calls := countingSTT(b, "should never be produced")

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	in := voiceInbound(6994, incidentVoiceBytes)
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	if got := calls.Load(); got != 0 {
		t.Fatalf("STT ran %d time(s) on a voice note the bot API can never download; want 0 — the size was on the update before any getFile attempt", got)
	}
	if !strings.Contains(in.Text, "[voice too big:") {
		t.Fatalf("agent surface must carry the distinct too-big marker; got %q", in.Text)
	}
	if strings.Contains(in.Text, "saved and recoverable") {
		t.Fatalf("agent surface still claims the audio is saved and recoverable — false for an over-limit note: nothing was fetched and resending is the only fix; got %q", in.Text)
	}
	if !strings.Contains(in.Text, "20.2 MB") || !strings.Contains(in.Text, "20.0 MB") {
		t.Fatalf("agent surface must name the actual size AND the ceiling; got %q", in.Text)
	}

	rc.waitReplyContaining(t, "over the 20.0 MB bot download limit")
	for _, rp := range rc.sendRepliesSnapshot() {
		if strings.Contains(rp.Text, "try again") {
			t.Fatalf("human notice tells the sender to try again, which cannot work for an over-limit note; got %q", rp.Text)
		}
	}
}

// The boundary is EXCLUSIVE: a note exactly at the ceiling is downloadable, so it
// must transcribe. A gate that refuses at the limit silently kills the largest
// notes that actually work.
func TestFlushInbounds_VoiceAtDownloadLimit_StillTranscribes(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	registerReadbackChannel(b, gateChannel(botDownloadLimit))
	calls := countingSTT(b, "transcribed at the boundary")

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	in := voiceInbound(6995, botDownloadLimit)
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	if got := calls.Load(); got != 1 {
		t.Fatalf("STT ran %d time(s) for a note exactly AT the ceiling; want 1 — that file is downloadable", got)
	}
	if !strings.Contains(in.Text, "transcribed at the boundary") {
		t.Fatalf("a note at the ceiling must surface its transcript; got %q", in.Text)
	}
}

// A channel that declares no ceiling (0) must never be gated. Silence in the
// manifest means "unknown", and treating unknown as zero would refuse to
// transcribe every voice note on that channel.
func TestFlushInbounds_ChannelDeclaresNoDownloadLimit_StillTranscribes(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	registerReadbackChannel(b, gateChannel(0))
	calls := countingSTT(b, "no ceiling declared")

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	in := voiceInbound(6996, incidentVoiceBytes)
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	if got := calls.Load(); got != 1 {
		t.Fatalf("STT ran %d time(s) on a channel that declares no download ceiling; want 1 — an undeclared limit must not gate", got)
	}
}

// A route whose channel cannot be resolved (unit harnesses, a channel that
// failed to register) has no manifest to read, so the gate must stand down
// rather than block transcription on a missing answer.
func TestFlushInbounds_UnresolvableChannel_StillTranscribes(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	calls := countingSTT(b, "no channel registered")

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	in := voiceInbound(6997, incidentVoiceBytes)
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	if got := calls.Load(); got != 1 {
		t.Fatalf("STT ran %d time(s) with no resolvable channel; want 1 — an unreadable manifest must not become a refusal", got)
	}
}

// A deliberate caption is the sender's own words and outranks any C3 placeholder
// — same rule the STT-failure path already follows. The human notice still fires,
// so the news is not lost.
func TestFlushInbounds_VoiceOverLimitKeepsSenderCaption(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	rc := gateChannel(botDownloadLimit)
	registerReadbackChannel(b, rc)
	countingSTT(b, "unused")

	w := newRouteWorker(context.Background(), RouteKey{Channel: "telegram", ChatID: -100}, time.Hour, b)
	defer w.Stop()

	in := voiceInbound(6998, incidentVoiceBytes)
	in.Text = "user-typed caption"
	w.flushInbounds(context.Background(), []*c3types.Inbound{in})

	if in.Text != "user-typed caption" {
		t.Fatalf("an over-limit voice note clobbered the sender's caption; in.Text=%q", in.Text)
	}
	rc.waitReplyContaining(t, "over the 20.0 MB bot download limit")
}

// sizeProbeChannel adds the OPTIONAL AttachmentSize probe, which is how the
// file_id-only paths (retranscribe) learn a size they were never given.
type sizeProbeChannel struct {
	*fakeChannel
	size  int64
	err   error
	calls atomic.Int64
}

func (s *sizeProbeChannel) AttachmentSize(string) (int64, error) {
	s.calls.Add(1)
	return s.size, s.err
}

func brokerWithProbe(t *testing.T, pc *sizeProbeChannel) *Broker {
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

// retranscribe on an over-limit file used to re-run the whole chain and answer
// "STT provider still failing (no transcript)" — sending the agent after a
// provider outage that does not exist. It must refuse up front, naming size.
func TestHandleRetranscribe_OverLimitFile_RefusedWithSizeCause(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &sizeProbeChannel{fakeChannel: &fakeChannel{caps: capsWithDownloadLimit(botDownloadLimit)}, size: incidentVoiceBytes}
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()
	calls := countingSTT(b, "should never be produced")

	resp := retranscribeOn(t, b, "F-BIG")

	if got := calls.Load(); got != 0 {
		t.Fatalf("retranscribe ran STT %d time(s) on an unfetchable file; want 0", got)
	}
	if !strings.Contains(resp.Err, "voice too big") || !strings.Contains(resp.Err, "20.2 MB") {
		t.Fatalf("retranscribe error must name SIZE as the cause, not a generic provider failure; got %q", resp.Err)
	}
}

// The probe is a diagnostic, never a gate of its own: a probe that FAILS must
// leave retranscribe working exactly as before. The size it returns alongside
// that error is not an answer — a value handed back with an error has not been
// established, and acting on it would refuse a file nobody ever measured.
func TestHandleRetranscribe_ProbeFailure_DoesNotBlockTranscription(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &sizeProbeChannel{
		fakeChannel: &fakeChannel{caps: capsWithDownloadLimit(botDownloadLimit)},
		size:        incidentVoiceBytes, // over the ceiling, but NOT established: err is set
		err:         errors.New("telegram: GetFile: network unreachable"),
	}
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()
	countingSTT(b, "fresh transcript")

	resp := retranscribeOn(t, b, "F-FLAKY")

	if resp.Err != "" {
		t.Fatalf("a failed size probe turned into a refusal; err=%q", resp.Err)
	}
	if resp.Text != "fresh transcript" {
		t.Fatalf("retranscribe text = %q, want the transcript — a flaky probe must not stop transcription", resp.Text)
	}
}

// A file the probe reports as fetchable must transcribe. The refusal is for
// files over the ceiling, not for every file the probe happens to measure.
func TestHandleRetranscribe_ProbeUnderLimit_StillTranscribes(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &sizeProbeChannel{fakeChannel: &fakeChannel{caps: capsWithDownloadLimit(botDownloadLimit)}, size: botDownloadLimit}
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()
	calls := countingSTT(b, "fresh transcript")

	resp := retranscribeOn(t, b, "F-OK")

	if resp.Err != "" {
		t.Fatalf("a fetchable file was refused; err=%q", resp.Err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("STT ran %d time(s) for a fetchable file; want 1", got)
	}
}

// When the transport refuses at getFile it reports no number, only the tagged
// cause. That tag must still reach the agent as a size answer.
func TestAttachmentTooBigRefusal_SizeSentinelSurfacesAsSizeCause(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	pc := &sizeProbeChannel{
		fakeChannel: &fakeChannel{caps: capsWithDownloadLimit(botDownloadLimit)},
		err:         errors.New("telegram: getFile refused: over the 20.0 MB bot download limit: " + channel.ErrAttachmentTooLarge.Error()),
	}
	pc.err = errors.Join(pc.err, channel.ErrAttachmentTooLarge)
	b := brokerWithProbe(t, pc)
	defer b.Shutdown()

	refusal := b.attachmentTooBigRefusal("telegram", "F-BIG")
	if refusal == "" {
		t.Fatal("a probe tagged ErrAttachmentTooLarge produced no refusal — the permanent size cause would be reported as a generic STT failure")
	}
	if !strings.Contains(refusal, "resend it in shorter parts") {
		t.Fatalf("refusal must state the action that actually works; got %q", refusal)
	}
}

// A channel with no probe cannot answer the size question, so the file_id-only
// paths must proceed rather than refuse on an unknown.
func TestAttachmentTooBigRefusal_ChannelWithoutProbe_NeverRefuses(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{caps: capsWithDownloadLimit(botDownloadLimit)})
	defer b.Shutdown()

	if got := b.attachmentTooBigRefusal("telegram", "F-BIG"); got != "" {
		t.Fatalf("a channel that cannot report sizes must not produce a refusal; got %q", got)
	}
}

// Integer MB collapses the two numbers this gate exists to compare: the incident
// file and the ceiling both floor to "20 MB", so the message would read
// "20 MB > 20 MB limit" and prove nothing.
func TestMBString_DistinguishesSizeFromLimit(t *testing.T) {
	size, limit := mbString(incidentVoiceBytes), mbString(botDownloadLimit)
	if size == limit {
		t.Fatalf("size and limit render identically (%q) — the reader cannot see the file is over", size)
	}
	if size != "20.2 MB" || limit != "20.0 MB" {
		t.Fatalf("mbString(%d)=%q, mbString(%d)=%q; want 20.2 MB and 20.0 MB", incidentVoiceBytes, size, botDownloadLimit, limit)
	}
}
