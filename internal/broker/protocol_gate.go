package broker

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Andrometiq/c3/internal/ipc"
)

// refuseIncompatibleStateChange keeps a mixed-version connection useful for
// safe reads while failing closed before an unknown dialect can mutate a claim
// or durable queue. It returns true when the op was handled as a refusal.
func refuseIncompatibleStateChange(conn *ipc.Conn, stub *Stub, op ipc.Op, raw []byte) bool {
	peerVersion := stub.PeerProtocolVersion()
	if ipc.ProtocolStateChangesCompatible(peerVersion) {
		return false
	}

	reason := fmt.Sprintf(
		"protocol v%d is outside this broker's implemented state-change compatibility window v%d..v%d; restart the CLI to match the broker",
		peerVersion, ipc.CompatibleProtocolMin, ipc.CompatibleProtocolMax,
	)
	refuse := func() {
		log.Printf("protocol gate REFUSED op=%s cli=%s pid=%d conn=%d peer=v%d broker=v%d: incompatible state change",
			op, stub.CLI, stub.PID, stub.ConnID, peerVersion, ipc.ProtocolVersion)
	}

	switch op {
	case ipc.OpAttach:
		refuse()
		_ = conn.WriteJSON(ipc.AttachedMsg{Op: ipc.OpAttached, OK: false, Err: reason})
		return true
	case ipc.OpRelease:
		refuse()
		_ = conn.WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: reason})
		return true
	case ipc.OpFetchQueue:
		var req ipc.FetchQueueReq
		if json.Unmarshal(raw, &req) != nil || !req.Ack {
			return false
		}
		refuse()
		_ = conn.WriteJSON(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: req.ID, Err: reason})
		return true
	case ipc.OpInboundDelivered:
		refuse()
		_ = conn.WriteJSON(ipc.ErrorMsg{Op: ipc.OpError, Err: reason})
		return true
	case ipc.OpRecoverSession:
		refuse()
		_ = conn.WriteJSON(ipc.RecoverSessionResp{Op: ipc.OpRecoverSessionResult, Err: reason})
		return true
	case ipc.OpRetranscribe:
		var req ipc.RetranscribeReq
		_ = json.Unmarshal(raw, &req)
		refuse()
		_ = conn.WriteJSON(ipc.RetranscribeResp{Op: ipc.OpRetranscribeResult, ID: req.ID, Err: reason})
		return true
	default:
		return false
	}
}
