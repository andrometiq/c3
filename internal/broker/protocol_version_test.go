package broker

import (
	"bytes"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// protoLogBuf is a goroutine-safe log sink. HandleConn runs in its own
// goroutine and keeps logging (conn-drop, etc.) while the test reads, so the
// plain bytes.Buffer used by captureLog would race here.
type protoLogBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *protoLogBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *protoLogBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureProtoLog redirects the default logger to a goroutine-safe buffer for
// the duration of the test and returns a reader for it.
func captureProtoLog(t *testing.T) *protoLogBuf {
	t.Helper()
	lb := &protoLogBuf{}
	oldOut, oldFlags := log.Writer(), log.Flags()
	log.SetOutput(lb)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
	})
	return lb
}

func mappingsWithTopic() *mappings.MappingsFile {
	mf := emptyMappings()
	mf.Channels["telegram"] = mappings.ChannelConfig{
		DefaultGroup: "main",
		Groups:       map[string]mappings.GroupConfig{"main": {ChatID: -100}},
		Topics: []mappings.Topic{
			{ChatID: -100, TopicID: 281, Name: "c3", Group: "main"},
		},
	}
	return mf
}

// A newer/older adapter must be WARNED, never refused. Refusing would turn every
// `c3 update` — which swaps binaries under adapters that then reconnect with old
// code — into a hard outage. This asserts BOTH halves: the warning names both
// versions, AND the connection is fully live afterwards (a real op round-trips).
func TestHello_ProtocolMismatch_WarnsAndKeepsConnectionLive(t *testing.T) {
	lb := captureProtoLog(t)
	peer, done := runHandlerWithPeer(t, mappingsWithTopic())
	defer done()

	peerVersion := ipc.ProtocolVersion + 1
	if err := peer.WriteJSON(ipc.HelloMsg{
		Op: ipc.OpHello, CLI: "claude", PID: 4242, CWD: "/x",
		ProtocolVersion: peerVersion,
	}); err != nil {
		t.Fatal(err)
	}

	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("no reply to a version-mismatched hello: %v", err)
	}
	op, err := ipc.PeekOp(raw)
	if err != nil {
		t.Fatal(err)
	}
	if op == ipc.OpError {
		t.Fatalf("broker REFUSED a version mismatch (sent %s) — it must warn and continue: %s", op, raw)
	}
	var ack ipc.HelloAckMsg
	if err := json.Unmarshal(raw, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Op != ipc.OpHelloAck {
		t.Fatalf("op=%q, want hello_ack", ack.Op)
	}
	if ack.ConnID == 0 {
		t.Error("ConnID should be assigned — the stub must be registered despite the mismatch")
	}
	if ack.ProtocolVersion != ipc.ProtocolVersion {
		t.Errorf("ack.ProtocolVersion = %d, want %d (broker must stamp its own version)",
			ack.ProtocolVersion, ipc.ProtocolVersion)
	}

	out := lb.String()
	if !strings.Contains(out, "PROTOCOL VERSION MISMATCH") {
		t.Errorf("no mismatch warning logged; log was:\n%s", out)
	}
	for _, want := range []string{
		"v" + strconv.Itoa(peerVersion),         // the peer's version
		"v" + strconv.Itoa(ipc.ProtocolVersion), // ours
		"cli=claude",
		"pid=4242",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("warning does not name %q; log was:\n%s", want, out)
		}
	}

	// The load-bearing assertion: the connection is LIVE, not just un-refused.
	if err := peer.WriteJSON(ipc.ListTopicsReq{Op: ipc.OpListTopics}); err != nil {
		t.Fatalf("write on a mismatched connection failed: %v", err)
	}
	raw, err = peer.ReadFrame()
	if err != nil {
		t.Fatalf("mismatched connection was closed instead of served: %v", err)
	}
	var resp ipc.TopicsListMsg
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Op != ipc.OpTopicsList || len(resp.Topics) != 1 || resp.Topics[0].Name != "c3" {
		t.Errorf("post-mismatch op not served correctly: %+v", resp)
	}
}

// An adapter from before this field existed omits protocol_version entirely.
// That is version 1 by definition: no warning, no error, business as usual.
func TestHello_AbsentProtocolVersion_MeansV1_NoWarning(t *testing.T) {
	lb := captureProtoLog(t)
	peer, done := runHandlerWithPeer(t, mappingsWithTopic())
	defer done()

	// Marshal by hand to be certain the key is absent from the wire, exactly as
	// an older build would send it.
	legacy := []byte(`{"op":"hello","cli":"claude","pid":7,"cwd":"/x"}`)
	if bytes.Contains(legacy, []byte("protocol_version")) {
		t.Fatal("fixture must not carry protocol_version")
	}
	// json.RawMessage marshals verbatim, so this hits the wire byte-for-byte.
	if err := peer.WriteJSON(json.RawMessage(legacy)); err != nil {
		t.Fatal(err)
	}

	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("legacy hello got no ack: %v", err)
	}
	var ack ipc.HelloAckMsg
	if err := json.Unmarshal(raw, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Op != ipc.OpHelloAck || ack.ConnID == 0 {
		t.Fatalf("legacy hello not accepted: %+v", ack)
	}
	if ack.ProtocolVersion != ipc.ProtocolVersion {
		t.Errorf("ack.ProtocolVersion = %d, want %d", ack.ProtocolVersion, ipc.ProtocolVersion)
	}
	if out := lb.String(); strings.Contains(out, "PROTOCOL VERSION MISMATCH") {
		t.Errorf("absent protocol_version must NOT warn (absence == v1); log was:\n%s", out)
	}
}

// The matching case: an adapter of the same build states its version explicitly
// and gets no warning.
func TestHello_MatchingProtocolVersion_NoWarning(t *testing.T) {
	lb := captureProtoLog(t)
	peer, done := runHandlerWithPeer(t, mappingsWithTopic())
	defer done()

	if err := peer.WriteJSON(ipc.HelloMsg{
		Op: ipc.OpHello, CLI: "codex", PID: 9, CWD: "/x",
		ProtocolVersion: ipc.ProtocolVersion,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if out := lb.String(); strings.Contains(out, "PROTOCOL VERSION MISMATCH") {
		t.Errorf("same-version hello must not warn; log was:\n%s", out)
	}
}
