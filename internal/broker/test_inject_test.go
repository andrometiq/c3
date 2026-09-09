package broker

import (
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

func injectionFixture(t *testing.T, enabled bool) *Broker {
	t.Helper()
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	mf := &mappings.MappingsFile{SchemaVersion: 1, Mappings: map[string]mappings.Mapping{}, Allowlist: &mappings.Allowlist{Groups: []int64{-1}}, Channels: map[string]mappings.ChannelConfig{TestInjectChannel: {DebounceMS: 20}}}
	b := New(mf)
	t.Cleanup(b.Shutdown)
	if enabled {
		if err := b.EnableTestInjection(); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

func TestInjectionSocketGateAndStrictRequest(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "opted_in"}[enabled], func(t *testing.T) {
			b := injectionFixture(t, enabled)
			peer, done := peerPair(t, b)
			defer done()
			if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "c3-broker-cli", PID: os.Getpid()}); err != nil {
				t.Fatal(err)
			}
			nextWireOp(t, peer, ipc.OpHelloAck)
			for _, raw := range []string{`{"op":"test_inject","topic":1,"text":"example","channel":"telegram"}`, `{"op":"test_inject","topic":1,"text":"example"}`} {
				if err := peer.WriteJSON(json.RawMessage(raw)); err != nil {
					t.Fatal(err)
				}
				var response ipc.TestInjectResp
				if err := json.Unmarshal(nextWireOp(t, peer, ipc.OpTestInject), &response); err != nil {
					t.Fatal(err)
				}
				want := enabled && !strings.Contains(raw, "telegram")
				if response.Accepted != want || (!want && response.Err == "") {
					t.Fatalf("%s: %+v", raw, response)
				}
			}
		})
	}
}

func TestInjectionPersistenceKindsAndIsolation(t *testing.T) {
	for _, kind := range []string{"text", "voice", "photo"} {
		t.Run(kind, func(t *testing.T) {
			b := injectionFixture(t, true)
			var telegramAcks atomic.Int32
			b.SetPersistedCallback("telegram", func(*c3types.Inbound) { telegramAcks.Add(1) })
			topic := int64(42)
			key := MakeRouteKey(TestInjectChannel, -1, &topic)
			r := b.Inject(ipc.TestInjectReq{Topic: topic, Text: "example", Kind: kind, Count: 2, VoiceDelayMS: map[string]int{"voice": 200}[kind]})
			if !r.Accepted || len(r.MessageIDs) != 2 || r.MessageIDs[0] == r.MessageIDs[1] {
				t.Fatal(r)
			}
			waitForFetchPending(t, b, queueRouteKey(key), 2, "injected persistence")
			rows, err := b.Queue.PeekTracked(queueRouteKey(key), -1)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				if !row.Inbound.TestInjected || row.Inbound.Channel != TestInjectChannel || row.RecordID == "" {
					t.Fatalf("missing provenance/durability: %+v", row)
				}
				if kind == "voice" && len(row.VoicePending) == 0 {
					t.Fatal("voice skipped durable placeholder")
				}
				if kind == "photo" && (len(row.Inbound.Attachments) != 1 || row.Inbound.Attachments[0].Kind != "photo") {
					t.Fatal(row)
				}
			}
			if kind == "voice" {
				for _, id := range r.MessageIDs {
					waitForVoiceQueueText(t, b, key, id, func(s string) bool { return strings.Contains(s, "[Transcribed voice]: example") })
				}
				final, _ := b.Queue.PeekTracked(queueRouteKey(key), -1)
				for i, row := range final {
					if row.RecordID != rows[i].RecordID || len(row.VoicePending) != 0 || rowRevision(row) == rowRevision(rows[i]) || !row.Inbound.TestInjected {
						t.Fatal("revision identity lost", row)
					}
				}
			}
			if telegramAcks.Load() != 0 {
				t.Fatal("synthetic traffic acknowledged Telegram")
			}
			merged := mergeBatch([]*c3types.Inbound{&rows[0].Inbound, &rows[1].Inbound})
			if !merged.TestInjected {
				t.Fatal("debounce lost test provenance")
			}
		})
	}
}

func TestInjectionRefusesRealConfigurationAndInvalidInput(t *testing.T) {
	b := injectionFixture(t, false)
	mf := b.Mappings().Clone()
	mf.Channels["telegram"] = mappings.ChannelConfig{}
	b.SetMappings(mf)
	if err := b.EnableTestInjection(); err == nil {
		t.Fatal("enabled alongside real configuration")
	}
	b = injectionFixture(t, true)
	for _, req := range []ipc.TestInjectReq{{Topic: 0, Text: "x"}, {Topic: 1}, {Topic: 1, Text: "x", Kind: "callback"}, {Topic: 1, Text: "x", Count: 3}, {Topic: 1, Text: "x", VoiceDelayMS: 1}, {Topic: 1, Text: "x", Kind: "voice", VoiceDelayMS: 60001}, {Topic: 1, Text: strings.Repeat("x", 65537)}} {
		if r := b.Inject(req); r.Accepted || r.Err == "" {
			t.Fatal(r)
		}
	}
	// No callback or timer should be needed to reject disabled/degraded intake.
	b = injectionFixture(t, false)
	b.Queue = nil
	if err := b.EnableTestInjection(); err == nil {
		t.Fatal("enabled without durability")
	}
}
