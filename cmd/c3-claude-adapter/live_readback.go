package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
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
	a.syncLiveConnectionLocked(a.rawConn())
	route := a.initialRenderRoute
	if route.State != ipc.RenderQueueOnly {
		if _, ok := transcriptOffset(a.livePath()); !ok {
			route = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "session transcript unavailable"}
		}
	}
	a.renderRoute = route
	if publish {
		a.publishLiveRouteLocked(a.currentConn())
	}
}

// Callers own liveMu; capture state there, but never write IPC while holding it.
func (a *adapter) publishLiveRouteLocked(conn *ipc.Conn) {
	if conn == nil {
		return
	}
	route := a.renderRoute
	go a.publishLiveRoute(conn, route)
}

// Serialize state publications, dropping superseded snapshots before writing.
func (a *adapter) publishLiveRoute(conn *ipc.Conn, route ipc.RenderRoute) {
	a.livePublishMu.Lock()
	defer a.livePublishMu.Unlock()
	a.liveMu.Lock()
	current := route == a.renderRoute && conn == a.currentConn()
	a.liveMu.Unlock()
	if current {
		writeLiveFrame(conn, ipc.RenderStateMsg{Op: ipc.OpRenderState, RenderRoute: route})
	}
}

func writeLiveFrame(conn *ipc.Conn, frame any) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := conn.WriteJSONContext(ctx, frame); err != nil {
		log.Printf("live receipt IPC failed: %v", err)
	}
}

func (a *adapter) syncLiveConnectionLocked(conn *ipc.Conn) {
	if a.liveConn != conn {
		a.liveConn = conn
		a.liveGeneration++
		a.livePending = nil
	}
}

func (a *adapter) cancelLiveReadbacks() {
	a.liveMu.Lock()
	a.liveGeneration++
	a.livePending = nil
	a.liveMu.Unlock()
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
	locked := true
	defer func() {
		if locked {
			a.liveMu.Unlock()
		}
	}()
	conn := a.currentConn()
	if a.renderRoute.State == ipc.RenderQueueOnly || conn == nil {
		return
	}
	a.syncLiveConnectionLocked(conn)
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
	generation := a.liveGeneration
	a.liveActive++
	if a.runCtx != nil {
		ctx = a.runCtx
	}
	// Register and capture offset before output; the timer is independent of stdout.
	go a.awaitLiveReadback(ctx, conn, generation, path, offset, marker, in, deadline)
	a.liveMu.Unlock()
	locked = false
	writeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if a.notifyTx == nil || a.notifyTx.Notify(writeCtx, "notifications/claude/channel", frame) != nil {
		a.liveMu.Lock()
		if generation == a.liveGeneration && conn == a.currentConn() {
			a.downgradeLiveLocked(conn, "live push not confirmed")
		}
		a.liveMu.Unlock()
	}
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
	discarding := false
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		a.liveMu.Lock()
		current := generation == a.liveGeneration && conn == a.currentConn()
		a.liveMu.Unlock()
		if !current {
			return
		}
		a.liveScanMu.Lock()
		next, found := scanChannelReceipt(path, offset, marker, &discarding)
		a.liveScanMu.Unlock()
		offset = next
		a.liveMu.Lock()
		if generation != a.liveGeneration || conn != a.currentConn() {
			a.liveMu.Unlock()
			return
		}
		if found && time.Now().Before(deadline) {
			delete(a.livePending, marker)
			// A late receipt for another push may consume that exact row, but cannot
			// undo a timeout downgrade. Only attach/reconnect may re-enable pushes.
			promote := a.renderRoute.State == ipc.RenderProbing
			if promote {
				a.renderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
			}
			a.liveMu.Unlock()
			if promote {
				a.publishLiveRoute(conn, ipc.RenderRoute{State: ipc.RenderCapable})
			}
			log.Printf("live readback confirmed msg=%d", in.Inbound.MessageID)
			writeLiveFrame(conn, ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered,
				UpdateID: in.Inbound.MessageID, OK: true, Count: in.Covered, DeliveryToken: in.DeliveryToken})
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

func scanChannelReceipt(path string, offset int64, marker string, discarding *bool) (int64, bool) {
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
		*discarding = false
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return offset, false
	}
	// Bound each poll's work and memory. Once oversized, advance through the
	// record across polls, retaining only a discard bit until its newline.
	r := bufio.NewReader(io.LimitReader(f, min(info.Size()-offset, 32<<20)))
	start := offset
	var line []byte
	for {
		chunk, err := r.ReadSlice('\n')
		offset += int64(len(chunk))
		if !*discarding {
			if len(line)+len(chunk) > maxTranscriptLineBytes {
				*discarding = true
				line = nil
			} else {
				line = append(line, chunk...)
			}
		}
		if err == nil {
			if !*discarding && channelReceipt(line, marker) {
				return offset, true
			}
			*discarding = false
			line = line[:0]
			start = offset
			continue
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if !*discarding {
			offset = start
		} // retry incomplete, bounded record
		return offset, false
	}
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
		// A channel receipt starts with its opening tag. Never search body text
		// or another attribute's value for a second, embedded marker.
		text = strings.TrimSpace(text)
		if !strings.HasPrefix(text, "<channel") {
			return false
		}
		decoder := xml.NewDecoder(strings.NewReader(text))
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		tag, ok := token.(xml.StartElement)
		if !ok || tag.Name.Local != "channel" || tag.Name.Space != "" {
			return false
		}
		seen := map[xml.Name]bool{}
		matched := false
		for _, attr := range tag.Attr {
			if seen[attr.Name] {
				return false
			}
			seen[attr.Name] = true
			if attr.Name.Space == "" && attr.Name.Local == "c3_delivery_id" {
				matched = attr.Value == marker
			}
		}
		// Token parses the complete opening tag (quotes, escapes and self-close).
		// Channel bodies are plain text, not necessarily valid XML.
		opener := text[:decoder.InputOffset()]
		// encoding/xml accepts adjacent attributes without whitespace. Require
		// the host's quoted-value boundary too, so malformed tags fail closed.
		var quote byte
		for i := 0; i < len(opener); i++ {
			c := opener[i]
			if quote == 0 {
				if c == '\'' || c == '"' {
					quote = c
				}
			} else if c == quote {
				quote = 0
				if i+1 < len(opener) && !strings.ContainsRune(" \t\r\n/>", rune(opener[i+1])) {
					return false
				}
			}
		}
		return matched && (strings.HasSuffix(opener, "/>") || strings.Contains(text[decoder.InputOffset():], "</channel>"))
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
