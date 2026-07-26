package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

const recoverRespTimeout = 8 * time.Second

// recover fields live on adapter (declared here via methods).

// trySessionRecover fires OpRecoverSession once after hello so a resumed Grok
// session silently re-claims its last topic (Claude parity, Grok-flavored:
// stable id is the Grok session UUID from env / active_sessions.json).
func (a *adapter) trySessionRecover(ctx context.Context) {
	sid := a.stableSessionID()
	if sid == "" {
		log.Printf("recover-session: no Grok session id yet — skip auto-attach (will register id on first attach)")
		return
	}
	a.fireRecover(ctx, sid, a.cwd())
}

// stableSessionID returns the Grok session UUID this adapter is bound to — the
// leader-bound id when set, else the env / active_sessions.json resolution.
// leader.sessionID is written under leader.mu everywhere (connectLocked,
// fireRecover, bindSessionIDForAttach), so the read takes the same mutex: an
// unlocked read of a Go string racing those writers is a torn-read data race
// (pointer/length pair). leader.mu guards field state only (I/O runs under
// leader.ioMu), so this never waits behind an in-flight Inject's socket I/O;
// every caller runs on its own goroutine (trySessionRecover, the MCP attach
// handler, refireRecoverOnReconnect's goroutine) — never on brokerReader.
func (a *adapter) stableSessionID() string {
	if a.leader != nil {
		a.leader.mu.Lock()
		sid := a.leader.sessionID
		a.leader.mu.Unlock()
		if sid != "" {
			return sid
		}
	}
	return resolveGrokSessionID()
}

// cwd returns the working directory to report on recover ops — the
// leader-bound cwd when set (read under leader.mu, same contract as
// stableSessionID), else env / process cwd.
func (a *adapter) cwd() string {
	if a.leader != nil {
		a.leader.mu.Lock()
		cwd := a.leader.cwd
		a.leader.mu.Unlock()
		if cwd != "" {
			return cwd
		}
	}
	if v := os.Getenv("C3_GROK_CWD"); v != "" {
		return v
	}
	cwd, _ := os.Getwd()
	return cwd
}

func (a *adapter) fireRecover(ctx context.Context, stableID, cwd string) {
	if stableID == "" {
		return
	}
	if !a.recoverFired.CompareAndSwap(false, true) {
		return
	}
	// Ensure inject targets this session.
	if a.leader != nil {
		a.leader.mu.Lock()
		a.leader.sessionID = stableID
		if cwd != "" {
			a.leader.cwd = cwd
		}
		a.leader.mu.Unlock()
	}

	respCh := make(chan ipc.RecoverSessionResp, 1)
	a.rsmu.Lock()
	a.rsPending = respCh
	a.rsmu.Unlock()
	defer func() {
		a.rsmu.Lock()
		if a.rsPending == respCh {
			a.rsPending = nil
		}
		a.rsmu.Unlock()
	}()

	conn := a.currentConn()
	if conn == nil {
		return
	}
	if err := conn.WriteJSON(ipc.RecoverSessionReq{
		Op: ipc.OpRecoverSession, StableSessionID: stableID, CWD: cwd,
	}); err != nil {
		log.Printf("recover-session: write failed: %v", err)
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-time.After(recoverRespTimeout):
		log.Printf("recover-session: no response within %v", recoverRespTimeout)
		return
	case resp := <-respCh:
		if resp.Err != "" {
			log.Printf("recover-session: broker err: %s", resp.Err)
			return
		}
		// Even when Recovered=false, broker has bound stable id on this stub —
		// future attaches will record session attachment for next resume.
		if !resp.Recovered {
			log.Printf("recover-session: session=%s registered (no prior attachment to re-claim)", stableID)
			return
		}
		a.rememberAttach(rememberedIdentityReq(cwd, resp.ChatID, resp.TopicID, resp.Group))
		a.setAttachedTopic(resp.Name)
		log.Printf("recover-session: auto-attached to %q (queued=%d)", resp.Name, resp.QueuedCount)
		if text := renderGrokRecoverNotice(resp); text != "" {
			a.emitRecoverNotice(text)
		}
	}
}

func (a *adapter) dispatchRecoverSessionResult(raw []byte) {
	var resp ipc.RecoverSessionResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		return
	}
	a.rsmu.Lock()
	ch := a.rsPending
	a.rsmu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- resp:
	default:
	}
}

func renderGrokRecoverNotice(resp ipc.RecoverSessionResp) string {
	name := resp.Name
	if name == "" {
		return ""
	}
	if resp.QueuedCount > 0 {
		noun := "message"
		if resp.QueuedCount != 1 {
			noun = "messages"
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "C3: auto-attached to %q (resumed session). ~%d %s held — call fetch_queue (limit:\"all\") to drain:",
			name, resp.QueuedCount, noun)
		for _, it := range resp.QueuedSummary {
			preview := it.Preview
			if preview == "" {
				preview = "(" + it.Kind + ")"
			}
			fmt.Fprintf(&sb, "\n  • [%d] %s %s: %s", it.MessageID, it.Sender, it.Kind, preview)
		}
		return sb.String()
	}
	return fmt.Sprintf("C3: auto-attached to %q (resumed session). Live Telegram inject is active.", name)
}

func (a *adapter) emitRecoverNotice(text string) {
	if a.transport == nil || text == "" {
		return
	}
	if err := a.transport.Notify(context.Background(), "notifications/message", map[string]any{
		"level":  "info",
		"logger": "c3",
		"data":   text,
	}); err != nil {
		log.Printf("recover notice notify failed: %v — %s", err, text)
	}
}

// refireRecoverOnReconnect re-registers this session's stable id on a FRESH
// broker connection (§3d2, ported from the Claude adapter). A broker RESTART
// (self-update / rebuild) yields a fresh stub with no stable id and no
// reconnect-transfer, so without this the new broker never learns the sid:
// recordSessionAttachment no-ops on an empty stable id, post-restart attach
// changes are never recorded, and a later Grok resume either silently
// re-attaches to the STALE pre-restart topic or finds nothing to recover. It
// demotes the recoverFired guard from once-per-process to once-per-connection
// (reset + re-fire) — in a goroutine, because fireRecover blocks on the
// RecoverSessionResp that brokerReader (whose recovery loop calls this) must
// be free to read, and stableSessionID() may briefly contend on leader.mu.
//
// Ordering is safe: replayLastAttach's synchronous write already put the
// replayed attach on the wire before this fires, and the broker's same-conn
// serial dispatch processes that attach FIRST — so the recover takes the
// record-only branch when the replay restored the route (and the gated
// own-route recover when the replay's proposal was discarded).
func (a *adapter) refireRecoverOnReconnect(ctx context.Context) {
	a.recoverFired.Store(false)
	go func() {
		sid := a.stableSessionID()
		if sid == "" {
			return // no session id resolvable — nothing to re-register yet;
			// the next attach's ensureStableSessionRegistered retries.
		}
		a.fireRecover(ctx, sid, a.cwd())
	}()
}

// ensureStableSessionRegistered tells the broker this stub's stable session id
// (so attach records session attachment for resume) without claiming a route.
func (a *adapter) ensureStableSessionRegistered(ctx context.Context) {
	sid := a.stableSessionID()
	if sid == "" {
		return
	}
	// fireRecover is once per BROKER CONNECTION (refireRecoverOnReconnect
	// resets the guard after a reconnect); if it already fired on this
	// connection, the stable id is already registered on the live stub.
	if a.recoverFired.Load() {
		return
	}
	a.fireRecover(ctx, sid, a.cwd())
}

// bindSessionIDForAttach freezes inject + recover identity from cwd/env at
// attach time (multi-session: prefer the active_sessions entry matching cwd).
func (a *adapter) bindSessionIDForAttach(cwd string) {
	sid := resolveGrokSessionIDForCWD(cwd)
	if sid == "" {
		sid = resolveGrokSessionID()
	}
	if sid == "" {
		return
	}
	if a.leader != nil {
		a.leader.mu.Lock()
		a.leader.sessionID = sid
		a.leader.cwd = cwd
		a.leader.mu.Unlock()
	}
}
