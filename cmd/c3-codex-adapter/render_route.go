package main

import (
	"context"
	"log"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// Use the same route-state contract as Claude: broker connectivity alone does
// not mean the host can accept inbound. Reasons contain no session details.
func (a *adapter) codexRenderRoute() ipc.RenderRoute {
	a.deliveryStateMu.Lock()
	unsupported, uncertain := a.queueUnsupported, a.deliveryUncertain
	a.deliveryStateMu.Unlock()
	reason := ""
	switch {
	case uncertain:
		reason = "fetch outcome unknown; restart required"
	case unsupported:
		reason = "queue method unsupported"
	case !codexForwardingAllowed():
		reason = "live forwarding disabled"
	case a.codexForwardConfig().ThreadID == "":
		reason = "conversation identity unresolved"
	default:
		return ipc.RenderRoute{State: ipc.RenderCapable}
	}
	return ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: reason}
}

// Like Claude's publisher, serialize updates and read the latest state before
// writing. Call asynchronously when holding deliveryStateMu. Hello holds the
// same publication lock so an update cannot overtake its startup snapshot.
func (a *adapter) publishCodexRenderRoute() {
	a.renderPublishMu.Lock()
	defer a.renderPublishMu.Unlock()
	conn := a.currentConn()
	if conn == nil {
		return // hello will carry the state on reconnect
	}
	route := a.codexRenderRoute()
	previous := a.publishedRoute
	if previous.State == "" {
		previous.State = ipc.RenderCapable // legacy broker default before hello
	}
	if route == previous {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := conn.WriteJSONContext(ctx, ipc.RenderStateMsg{Op: ipc.OpRenderState, RenderRoute: route}); err != nil {
		log.Printf("codex route update failed: %v", err)
		// Force a new hello instead of leaving the broker with stale capability.
		_ = conn.Close()
		return
	}
	a.publishedRoute = route
}
