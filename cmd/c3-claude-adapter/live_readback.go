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
	"maps"
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
	if a.liveCrossSession {
		// Cross-session is re-probed from channel eligibility on every attach.
		a.liveGeneration++
		a.livePending = nil
	}
	a.liveCrossSession = false
	route := a.initialRenderRoute
	if route.State == ipc.RenderQueueOnly && a.crossSession != nil {
		if _, err := a.crossSession.validatedSocket(); err != nil {
			route.Reason += "; " + err.Error()
		} else {
			route = a.crossSessionProbeLocked(route.Reason)
		}
	} else if route.State == ipc.RenderQueueOnly && a.crossSessionReason != "" {
		route.Reason += "; " + a.crossSessionReason
	}
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

// liveMu held. A probe is still unconfirmed, so use the existing broker probe
// reservation; only its receipt may advertise the new live state.
func (a *adapter) crossSessionProbeLocked(reason string) ipc.RenderRoute {
	a.liveCrossSession = true
	return ipc.RenderRoute{State: ipc.RenderProbing, Reason: "cross-session awaiting confirmation; " + reason + "; permission relay unavailable"}
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
	a.liveMu.Lock()
	route, cross := a.renderRoute, a.liveCrossSession
	a.liveMu.Unlock()
	relay := "unavailable"
	// Permission relay needs the host's registered channel, but does not depend
	// on transcript availability. A confirmed probe also proves registration.
	if !cross && route.State != ipc.RenderCrossSession && (a.initialRenderRoute.State == ipc.RenderCapable || route.State == ipc.RenderCapable) {
		relay = "available"
	}
	text := route.Text() + " Permission relay: " + relay + "."
	if a.crossSession != nil || route.State == ipc.RenderCrossSession {
		text += " On the cross-session route, inbound arrives as peer user turns. The Telegram Allow/Deny permission relay and native AskUserQuestion answering are unavailable; peer messages are never permission approval. Slash commands inside messages arrive as text. C3's own ask tool still works. Reply with the C3 reply tool as usual."
	}
	if route.State != ipc.RenderCapable && route.State != ipc.RenderCrossSession {
		text += " Use fetch_queue to retrieve held messages."
	}
	return text + "\n\n"
}

func (a *adapter) pushWithReadback(ctx context.Context, in ipc.InboundMsg, frame map[string]any) {
	a.pushWithReadbackGeneration(ctx, in, frame, nil)
}

func (a *adapter) pushWithReadbackGeneration(ctx context.Context, in ipc.InboundMsg, frame map[string]any, expected *uint64) {
	// Synthesized events have no durable receipt contract.
	if in.Inbound.IsEvent() {
		if a.liveRoute().State == ipc.RenderCapable && a.notifyTx != nil {
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
	if expected != nil && *expected != a.liveGeneration {
		return
	}
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
	// The channel encoder may still be reading its frame when its deadline
	// expires. A fallback owns a fresh envelope and metadata map, even when
	// retrying the same token, so it cannot mutate the in-flight channel write.
	frame = maps.Clone(frame)
	meta = maps.Clone(meta)
	frame["meta"] = meta
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
	cross := a.liveCrossSession
	var sent chan bool
	if cross {
		sent = make(chan bool, 1)
	}
	a.liveActive++
	if a.runCtx != nil {
		ctx = a.runCtx
	}
	// Register and capture offset before output; the timer is independent of stdout.
	go a.awaitLiveReadback(ctx, conn, generation, path, offset, marker, in, frame, cross, sent, deadline)
	a.liveMu.Unlock()
	locked = false
	if cross {
		// Socket completion must not stall brokerReader (including tools/results).
		go func() {
			writeCtx, cancel := context.WithDeadline(ctx, deadline)
			defer cancel()
			block, err := crossSessionChannelBlock(frame)
			if err == nil {
				err = a.crossSession.Send(writeCtx, block)
			}
			if err != nil {
				log.Printf("cross-session transport failed: %v", err)
			}
			sent <- err == nil
		}()
		return
	}
	writeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if a.notifyTx == nil || a.notifyTx.Notify(writeCtx, "notifications/claude/channel", frame) != nil {
		a.liveMu.Lock()
		if generation == a.liveGeneration && conn == a.currentConn() && !a.liveCrossSession && a.crossSession == nil {
			a.downgradeLiveLocked(conn, "live push not confirmed")
		}
		a.liveMu.Unlock()
	}
}

func (a *adapter) awaitLiveReadback(ctx context.Context, conn *ipc.Conn, generation uint64, path string, offset int64, marker string, in ipc.InboundMsg, frame map[string]any, cross bool, sent <-chan bool, deadline time.Time) {
	defer func() {
		a.liveMu.Lock()
		a.liveActive--
		if generation == a.liveGeneration {
			delete(a.livePending, marker)
		}
		a.liveMu.Unlock()
	}()
	discarding := false
	transportOK, transportFailed, received := !cross, false, false
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
		next, found := scanReceipt(path, offset, marker, &discarding, cross)
		a.liveScanMu.Unlock()
		offset = next
		received = received || found
		select {
		case ok := <-sent:
			transportOK, transportFailed = ok, !ok
			sent = nil
		default:
		}
		a.liveMu.Lock()
		if generation != a.liveGeneration || conn != a.currentConn() {
			a.liveMu.Unlock()
			return
		}
		if received && transportOK && time.Now().Before(deadline) {
			delete(a.livePending, marker)
			// A late receipt for another push may consume that exact row, but cannot
			// undo a timeout downgrade. Only attach/reconnect may re-enable pushes.
			promote := a.renderRoute.State == ipc.RenderProbing && cross == a.liveCrossSession
			if promote {
				if cross {
					a.renderRoute = ipc.RenderRoute{State: ipc.RenderCrossSession, Reason: strings.TrimPrefix(a.renderRoute.Reason, "cross-session awaiting confirmation; ")}
				} else {
					a.renderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
				}
			}
			route := a.renderRoute
			a.liveMu.Unlock()
			if promote {
				a.publishLiveRoute(conn, route)
			}
			log.Printf("live readback confirmed msg=%d", in.Inbound.MessageID)
			writeLiveFrame(conn, ipc.InboundDeliveredMsg{Op: ipc.OpInboundDelivered,
				UpdateID: in.Inbound.MessageID, OK: true, Count: in.Covered, DeliveryToken: in.DeliveryToken})
			return
		}
		if transportFailed || !time.Now().Before(deadline) {
			delete(a.livePending, marker)
			retry := false
			retryGeneration := generation
			if cross == a.liveCrossSession {
				if cross {
					a.downgradeLiveLocked(conn, "cross-session push not confirmed")
				} else if a.crossSession != nil {
					// The channel attempt has expired. Retry this exact durable
					// delivery once on the fallback, with a fresh receipt offset.
					a.liveGeneration++
					a.livePending = nil
					retryGeneration = a.liveGeneration
					a.renderRoute = a.crossSessionProbeLocked("channel push not confirmed")
					a.publishLiveRouteLocked(conn)
					retry = true
				} else {
					a.downgradeLiveLocked(conn, "live push not confirmed")
				}
			}
			a.liveMu.Unlock()
			if retry {
				a.pushWithReadbackGeneration(ctx, in, frame, &retryGeneration)
			}
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
	return scanReceipt(path, offset, marker, discarding, false)
}

func scanReceipt(path string, offset int64, marker string, discarding *bool, cross bool) (int64, bool) {
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
			if !*discarding && deliveryReceipt(line, marker, cross) {
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
	return deliveryReceipt(line, marker, false)
}

func deliveryReceipt(line []byte, marker string, cross bool) bool {
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
		if cross {
			// Peer framing is host-owned and its transcript shape is not yet
			// live-verified. Inspect ONLY the first channel opener, never skip
			// an unmatched/malformed opener in search of a nested marker.
			start := strings.Index(text, "<channel ")
			if start < 0 {
				return false
			}
			text = text[start:]
		}
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
		if strings.HasSuffix(opener, "/>") {
			return matched && !cross
		}
		return matched && strings.Contains(text[decoder.InputOffset():], "</channel>")
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
		if cross && block.Type == "text" && strings.Contains(block.Text, "<channel ") {
			return matches(block.Text) // first opener across the entire content array
		}
		if block.Type == "text" && matches(block.Text) {
			return true
		}
	}
	return false
}
