package ipc

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

// A peer that predates versioning omits the field: absence is v1, never an error.
func TestPeerProtocolVersion_AbsenceIsV1(t *testing.T) {
	for _, raw := range []int{0, -1, -99} {
		if got := PeerProtocolVersion(raw); got != 1 {
			t.Errorf("PeerProtocolVersion(%d) = %d, want 1", raw, got)
		}
		if ProtocolMismatch(raw) != (ProtocolVersion != 1) {
			t.Errorf("ProtocolMismatch(%d) disagrees with the v1 default", raw)
		}
	}
	if got := PeerProtocolVersion(7); got != 7 {
		t.Errorf("PeerProtocolVersion(7) = %d, want 7 (explicit versions pass through)", got)
	}
}

// hello must carry the version on the wire under the agreed key, and an old
// adapter's hello (no key) must decode to the zero value — which normalizes to
// v1 rather than failing.
func TestHelloMsg_CarriesAndDefaultsProtocolVersion(t *testing.T) {
	out, err := json.Marshal(HelloMsg{
		Op: OpHello, CLI: "claude", PID: 1, CWD: "/x", ProtocolVersion: ProtocolVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"protocol_version":`+strconv.Itoa(ProtocolVersion)) {
		t.Errorf("hello wire missing protocol_version: %s", out)
	}

	var legacy HelloMsg
	if err := json.Unmarshal([]byte(`{"op":"hello","cli":"claude","pid":1,"cwd":"/x"}`), &legacy); err != nil {
		t.Fatalf("a versionless hello must still decode: %v", err)
	}
	if legacy.ProtocolVersion != 0 {
		t.Errorf("absent field decoded to %d, want 0", legacy.ProtocolVersion)
	}
	if PeerProtocolVersion(legacy.ProtocolVersion) != 1 {
		t.Error("absent protocol_version must normalize to v1")
	}

	// Additive: omitempty keeps the key off the wire for a zero value, so a
	// hello built by code that doesn't set it looks exactly like an old one.
	out, err = json.Marshal(HelloMsg{Op: OpHello, CLI: "claude", PID: 1, CWD: "/x"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "protocol_version") {
		t.Errorf("zero ProtocolVersion must be omitted: %s", out)
	}
}

// Same contract on the reply leg.
func TestHelloAckMsg_CarriesAndDefaultsProtocolVersion(t *testing.T) {
	out, err := json.Marshal(HelloAckMsg{Op: OpHelloAck, ConnID: 3, ProtocolVersion: ProtocolVersion})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"protocol_version":`+strconv.Itoa(ProtocolVersion)) {
		t.Errorf("hello_ack wire missing protocol_version: %s", out)
	}

	var legacy HelloAckMsg
	if err := json.Unmarshal([]byte(`{"op":"hello_ack","conn_id":3}`), &legacy); err != nil {
		t.Fatalf("a versionless hello_ack must still decode: %v", err)
	}
	if PeerProtocolVersion(legacy.ProtocolVersion) != 1 {
		t.Error("an ack from an older broker must read as v1")
	}
	if ProtocolMismatch(legacy.ProtocolVersion) != (ProtocolVersion != 1) {
		t.Error("older-broker ack mismatch verdict is wrong")
	}
}

// An unknown FIELD from a newer peer must be ignored, not rejected — the
// additive-only guarantee. (This is also why DisallowUnknownFields is banned.)
func TestHello_UnknownFieldsAreIgnored(t *testing.T) {
	var h HelloMsg
	err := json.Unmarshal([]byte(`{"op":"hello","cli":"claude","pid":1,"cwd":"/x","protocol_version":9,"brand_new_field":{"a":1}}`), &h)
	if err != nil {
		t.Fatalf("unknown field must not break decoding: %v", err)
	}
	if h.ProtocolVersion != 9 || h.CLI != "claude" {
		t.Errorf("known fields lost: %+v", h)
	}
}

// The warning text is the whole product here: it must name BOTH versions and
// say the connection survived. And it must be empty when the versions agree —
// including the absent-means-v1 case.
func TestProtocolWarnings(t *testing.T) {
	if w := BrokerProtocolWarning("claude", 42, ProtocolVersion); w != "" {
		t.Errorf("matching versions must not warn, got: %s", w)
	}
	if w := AdapterProtocolWarning("claude", ProtocolVersion); w != "" {
		t.Errorf("matching versions must not warn, got: %s", w)
	}
	if ProtocolVersion == 1 {
		if w := BrokerProtocolWarning("claude", 42, 0); w != "" {
			t.Errorf("absent version must not warn while current version is 1, got: %s", w)
		}
		if w := AdapterProtocolWarning("claude", 0); w != "" {
			t.Errorf("absent version must not warn while current version is 1, got: %s", w)
		}
	}

	other := ProtocolVersion + 1
	bw := BrokerProtocolWarning("claude", 42, other)
	for _, want := range []string{
		"PROTOCOL VERSION MISMATCH",
		"cli=claude", "pid=42",
		"v" + strconv.Itoa(other), "v" + strconv.Itoa(ProtocolVersion),
		"ACCEPTED",
	} {
		if !strings.Contains(bw, want) {
			t.Errorf("broker warning missing %q: %s", want, bw)
		}
	}

	aw := AdapterProtocolWarning("codex", other)
	for _, want := range []string{
		"PROTOCOL VERSION MISMATCH",
		"codex",
		"v" + strconv.Itoa(other), "v" + strconv.Itoa(ProtocolVersion),
		"ACCEPTED",
	} {
		if !strings.Contains(aw, want) {
			t.Errorf("adapter warning missing %q: %s", want, aw)
		}
	}
}
