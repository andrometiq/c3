package broker

import "github.com/Andrometiq/c3/internal/ipc"

// P6: "Each accepted live transport is tried at most once per batch."
// The token and membership carry the cycle across a failed transport without
// admitting new arrivals or releasing the unproven session's slot.
func (r *negotiatedRoute) nextTransport(live ipc.DeliveryLive) string {
	if live.Channel.Eligible && !r.TriedChannel {
		return "channel"
	}
	if live.Inbox.Eligible && !r.TriedInbox {
		return "inbox"
	}
	return ""
}

func (r *negotiatedRoute) clearCycle() {
	r.CycleToken = ""
	r.CycleMembers = nil
	r.TriedChannel, r.TriedInbox = false, false
}
