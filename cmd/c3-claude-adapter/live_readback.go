package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

// A stdout write is transport success, never a receipt. Give the host 15s to
// append a complete user channel record, including delayed/partial writes.
const liveReadbackWindow = 15 * time.Second
const maxLiveReadbacks = 64

func (a *adapter) liveRoute() ipc.RenderRoute {
	a.liveMu.Lock()
	defer a.liveMu.Unlock()
	return a.renderRoute
}

func (a *adapter) livePath() string {
	if a.liveTranscriptPath != nil {
		return a.liveTranscriptPath()
	}
	return a.permissionTranscriptPath()
}

func transcriptOffset(path string) (int64, bool) {
	if path == "" {
		return 0, false
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return 0, false
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return 0, false
	}
	return info.Size(), true
}

// Explicit attach or reconnect is the only retry boundary after a timeout.
func (a *adapter) resetLiveRoute(publish bool) {
	a.liveMu.Lock()
	defer a.liveMu.Unlock()
	if a.initialRenderRoute.State == "" {
		return
	} // uninitialised test adapter
	route := a.initialRenderRoute
	if route.State != ipc.RenderQueueOnly {
		if _, ok := transcriptOffset(a.livePath()); !ok {
			route = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "session transcript unavailable"}
		}
	}
	a.liveGeneration++
	a.livePending = nil
	a.renderRoute = route
	if publish {
		a.publishLiveRouteLocked(a.currentConn())
	}
}

func (a *adapter) publishLiveRouteLocked(conn *ipc.Conn) {
	if conn != nil {
		if err := conn.WriteJSON(ipc.RenderStateMsg{Op: ipc.OpRenderState, RenderRoute: a.renderRoute}); err != nil {
			log.Printf("live route update failed: %v", err)
		}
	}
}

func (a *adapter) downgradeLiveLocked(conn *ipc.Conn, reason string) {
	if a.renderRoute.State == ipc.RenderQueueOnly {
		return
	}
	a.renderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: reason}
	log.Printf("live route: queue-only (%s)", reason)
	a.publishLiveRouteLocked(conn)
}

func (a *adapter) liveRoutePreamble() string {
	route := a.liveRoute()
	relay := "unavailable"
	// Permission relay needs the host's registered channel, but does not depend
	// on transcript availability. A confirmed probe also proves registration.
	if a.initialRenderRoute.State == ipc.RenderCapable || route.State == ipc.RenderCapable {
		relay = "available"
	}
	text := route.Text() + " Permission relay: " + relay + "."
	if route.State != ipc.RenderCapable {
		text += " Use fetch_queue to retrieve held messages."
	}
	return text + "\n\n"
}

func (a *adapter) pushWithReadback(ctx context.Context, in ipc.InboundMsg, frame map[string]any) {
	// Synthesized events have no durable receipt contract.
	if in.Inbound.IsEvent() {
		if a.notifyTx != nil {
			_ = a.notifyTx.Notify(ctx, "notifications/claude/channel", frame)
		}
		return
	}
	a.liveMu.Lock()
	defer a.liveMu.Unlock()
	conn := a.currentConn()
	if a.renderRoute.State == ipc.RenderQueueOnly || conn == nil {
		return
	}
	if a.liveActive >= maxLiveReadbacks {
		a.downgradeLiveLocked(conn, "live confirmation limit reached")
		return
	}
	if a.renderRoute.State == ipc.RenderProbing && len(a.livePending) > 0 {
		// Leave the durable row held. Never evict an outstanding receipt to make room.
		return
	}
	path := a.livePath()
	offset, ok := transcriptOffset(path)
	if !ok {
		a.downgradeLiveLocked(conn, "session transcript unavailable")
		return
	}
	marker := in.DeliveryToken
	if marker == "" {
		// Old brokers have no token. A fresh marker still distinguishes this write
		// from old transcript records; the ack retains the legacy empty token.
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			a.downgradeLiveLocked(conn, "delivery marker unavailable")
			return
		}
		marker = hex.EncodeToString(nonce[:])
	}
	if a.livePending[marker] {
		return
	}
	meta, ok := frame["meta"].(map[string]any)
	if !ok {
		a.downgradeLiveLocked(conn, "channel metadata unavailable")
		return
	}
	meta["c3_delivery_id"] = marker
	if a.livePending == nil {
		a.livePending = map[string]bool{}
	}
	a.livePending[marker] = true
	window := a.liveTimeout
	if window <= 0 {
		window = liveReadbackWindow
	}
	deadline := time.Now().Add(window)
	if a.notifyTx == nil || a.notifyTx.Notify(ctx, "notifications/claude/channel", frame) != nil {
		delete(a.livePending, marker)
		a.downgradeLiveLocked(conn, "live push not confirmed")
		return
	}
	generation := a.liveGeneration
	a.liveActive++
	if a.runCtx != nil {
		ctx = a.runCtx
	}
	go a.awaitLiveReadback(ctx, conn, generation, path, offset, marker, in, deadline)
}

func (a *adapter) awaitLiveReadback(ctx context.Context, conn *ipc.Conn, generation uint64, path string, offset int64, marker string, in ipc.InboundMsg, deadline time.Time) {
	defer func() {
		a.liveMu.Lock()
		a.liveActive--
		if generation == a.liveGeneration {
			delete(a.livePending, marker)
		}
		a.liveMu.Unlock()
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		a.liveMu.Lock()
		current := generation == a.liveGeneration && conn == a.currentConn()
		a.liveMu.Unlock()
		if !current {
			return
		}
		a.liveScanMu.Lock()
		next, found := scanChannelReceipt(path, offset, marker)
		a.liveScanMu.Unlock()
		offset = next
		a.liveMu.Lock()
		if generation != a.liveGeneration || conn != a.currentConn() {
			a.liveMu.Unlock()
			return
		}
		if found {
			delete(a.livePending, marker)
			// A late receipt for another push may consume that exact row, but cannot
			// undo a timeout downgrade. Only attach/reconnect may re-enable pushes.
			if a.renderRoute.State == ipc.RenderProbing {
				a.renderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
				a.publishLiveRouteLocked(conn)
			}
			log.Printf("live readback confirmed msg=%d", in.Inbound.MessageID)
			_ = conn.WriteJSON(ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered,
				UpdateID: in.Inbound.MessageID, OK: true, Count: in.Covered, DeliveryToken: in.DeliveryToken})
			a.liveMu.Unlock()
			return
		}
		if !time.Now().Before(deadline) {
			delete(a.livePending, marker)
			a.downgradeLiveLocked(conn, "live push not confirmed")
			a.liveMu.Unlock()
			return
		}
		a.liveMu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func scanChannelReceipt(path string, offset int64, marker string) (int64, bool) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return offset, false
	}
	f, err := os.Open(path)
	if err != nil {
		return offset, false
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return offset, false
	}
	if info.Size() < offset {
		offset = 0
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return offset, false
	}
	found := false
	// Read only this size snapshot, even if the host keeps appending. The next
	// poll retries a partial final record from its start. Oversized lines never ack.
	_, _ = scanTranscriptRecords(io.LimitReader(f, min(info.Size()-offset, 32<<20)), offset,
		func(start, end int64, line []byte, oversized bool) bool {
			offset = end
			found = !oversized && channelReceipt(line, marker)
			return !found
		}, nil)
	return offset, found
}

func channelReceipt(line []byte, marker string) bool {
	var entry struct {
		Type    string `json:"type"`
		Message struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"message"`
	}
	if json.Unmarshal(line, &entry) != nil || entry.Type != "user" || entry.Message.Role != "user" {
		return false
	}
	matches := func(text string) bool {
		// Match the marker only in a channel opening tag, not quoted tool output or
		// a queue-operation's content. Meta values are rendered as tag attributes.
		for {
			_, rest, ok := strings.Cut(text, "<channel ")
			if !ok {
				return false
			}
			tag, after, ok := strings.Cut(rest, ">")
			if !ok {
				return false
			}
			if strings.Contains(" "+tag, ` c3_delivery_id="`+marker+`"`) && strings.Contains(after, "</channel>") {
				return true
			}
			text = after
		}
	}
	var text string
	if json.Unmarshal(entry.Message.Content, &text) == nil {
		return matches(text)
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(entry.Message.Content, &blocks) != nil {
		return false
	}
	for _, block := range blocks {
		if block.Type == "text" && matches(block.Text) {
			return true
		}
	}
	return false
}
