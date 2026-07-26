package ipc

import "fmt"

// ProtocolVersion is the IPC wire-protocol version this build speaks. It rides
// on `hello` (adapter → broker) and `hello_ack` (broker → adapter) so the two
// sides of a connection can NAME their disagreement instead of failing silently.
//
// WHEN TO BUMP IT: bump on any change a peer speaking the OTHER version could
// MISINTERPRET — renaming or removing a field, changing a field's type, units,
// or meaning, changing what an existing op does, changing who is responsible for
// a step of an existing exchange, or making previously-optional behavior
// mandatory.
//
// WHEN NOT TO BUMP IT: do NOT bump for additive changes — a new field (always
// `omitempty`) or a brand-new op. Additive changes are safe in both directions
// by construction: unknown fields are ignored (encoding/json drops them, and C3
// deliberately never calls DisallowUnknownFields), and an unknown op is logged
// and skipped by every adapter's dispatch `default:` arm. Bumping for those
// would fire mismatch warnings that tell the operator nothing.
//
// Version 1 is the wire as it stood BEFORE this field existed, so an omitted
// protocol_version means 1 — see PeerProtocolVersion.
const ProtocolVersion = 1

// legacyProtocolVersion is what a peer that predates this field speaks. Absence
// is not an error and never will be: the first C3 releases shipped no version
// on the handshake at all.
const legacyProtocolVersion = 1

// PeerProtocolVersion normalizes the raw protocol_version a peer sent. Zero
// (field absent, or an explicit 0) and any nonsense negative value mean "peer
// predates versioning" ⇒ version 1. Never returns an error: an absent version
// is a supported, expected case, not a fault.
func PeerProtocolVersion(raw int) int {
	if raw <= 0 {
		return legacyProtocolVersion
	}
	return raw
}

// ProtocolMismatch reports whether a peer's reported version differs from this
// build's, after normalizing absence to version 1.
func ProtocolMismatch(peerRaw int) bool {
	return PeerProtocolVersion(peerRaw) != ProtocolVersion
}

// BrokerProtocolWarning returns the broker's operator-facing warning line for an
// adapter speaking a different protocol version, or "" when they agree.
//
// The broker WARNS and KEEPS THE CONNECTION. Refusing would turn every routine
// `c3 update` into a hard outage — the updater swaps the binaries and the broker
// exits, while adapter processes started by already-running CLI sessions
// reconnect to the NEW broker with their OLD code. Warning turns that same
// moment into one diagnosable line in the log.
func BrokerProtocolWarning(cli string, pid int, peerRaw int) string {
	if !ProtocolMismatch(peerRaw) {
		return ""
	}
	return fmt.Sprintf(
		"hello: PROTOCOL VERSION MISMATCH — adapter cli=%s pid=%d speaks protocol v%d, this broker speaks v%d. "+
			"Connection ACCEPTED and fully live (C3 never refuses on version); ops that changed between v%d and v%d may misbehave on this session. "+
			"Fix: restart that CLI so it spawns an adapter from the same c3 build as the broker.",
		cli, pid, PeerProtocolVersion(peerRaw), ProtocolVersion, PeerProtocolVersion(peerRaw), ProtocolVersion)
}

// AdapterProtocolWarning returns an adapter's operator-facing warning line for a
// broker speaking a different protocol version, or "" when they agree. Like the
// broker side, the adapter logs and carries on — the handshake still succeeds.
func AdapterProtocolWarning(cli string, peerRaw int) string {
	if !ProtocolMismatch(peerRaw) {
		return ""
	}
	return fmt.Sprintf(
		"hello: PROTOCOL VERSION MISMATCH — broker speaks protocol v%d, this %s adapter speaks v%d. "+
			"Handshake ACCEPTED and this session keeps working; ops that changed between v%d and v%d may misbehave. "+
			"Fix: restart this CLI (and, if it is the older side, the c3 broker) so both run the same c3 build.",
		PeerProtocolVersion(peerRaw), cli, ProtocolVersion, PeerProtocolVersion(peerRaw), ProtocolVersion)
}
