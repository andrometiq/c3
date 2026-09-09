//go:build linux || darwin

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type inboxPush struct {
	Auth struct{ Type, Token string }
	User struct {
		Type, From, Priority string
		SessionID            string `json:"session_id"`
		Message              struct{ Role, Content string }
	}
	Err   error
	Bytes int
}

// An independent receiver checks framing and waits for the client's write EOF
// before closing. No Claude process, broker daemon, or personal files involved.
func fakeInbox(t *testing.T, hold <-chan struct{}, response string) (*crossSessionTransport, <-chan inboxPush) {
	t.Helper()
	root, err := os.MkdirTemp("", "c3-inbox-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "cc-socks"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "cc-socks", fmt.Sprintf("%d.sock", os.Getpid()))
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	pushes := make(chan inboxPush, 16)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			reader := bufio.NewReader(conn)
			var push inboxPush
			for i := 0; i < 2; i++ {
				line, err := reader.ReadBytes('\n')
				push.Bytes += len(line)
				if err != nil {
					push.Err = err
					break
				}
				if i == 0 {
					push.Err = json.Unmarshal(line, &push.Auth)
				} else {
					push.Err = json.Unmarshal(line, &push.User)
				}
				if push.Err != nil {
					break
				}
			}
			if push.Err == nil {
				if _, err := reader.ReadByte(); err != io.EOF {
					push.Err = fmt.Errorf("expected write EOF, got %v", err)
				}
			}
			pushes <- push
			if hold != nil {
				<-hold
			}
			if response != "" {
				_, _ = io.WriteString(conn, response)
			}
			_ = conn.Close()
		}
	}()
	return &crossSessionTransport{runtimeDir: root, socketPath: path, token: "TEST-SECRET-INBOX-TOKEN", hostPID: os.Getpid()}, pushes
}

func nextInbox(t *testing.T, pushes <-chan inboxPush) inboxPush {
	t.Helper()
	select {
	case push := <-pushes:
		if push.Err != nil {
			t.Fatal(push.Err)
		}
		return push
	case <-time.After(2 * time.Second):
		t.Fatal("no inbox push")
		return inboxPush{}
	}
}

func appendPeerReceipt(t *testing.T, path, content string) {
	t.Helper()
	// Match the peer framing verified in Claude Code 2.1.263.
	line, err := json.Marshal(map[string]any{"type": "user", "isMeta": true, "origin": map[string]any{"kind": "peer", "from": "c3"},
		"message": map[string]any{"role": "user", "content": "Another Claude session sent a message:\n" + content + "\n\nHost peer guidance."}})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		t.Fatal(err)
	}
}

func TestCrossSessionTransportFrames(t *testing.T) {
	isolateAdapterTest(t)
	tx, pushes := fakeInbox(t, nil, "")
	content := "<channel c3_delivery_id=\"one\">\n/text & body\n</channel>"
	if err := tx.Send(context.Background(), content, "11111111-2222-4333-8444-555555555555"); err != nil {
		t.Fatal(err)
	}
	push := nextInbox(t, pushes)
	if push.Auth.Type != "auth" || push.Auth.Token != tx.token {
		t.Fatal("incorrect authentication frame")
	}
	if push.User.SessionID != "11111111-2222-4333-8444-555555555555" || push.User.Type != "user" || push.User.From != "c3" || push.User.Priority != "next" || push.User.Message.Role != "user" || push.User.Message.Content != content {
		t.Fatalf("incorrect user frame: %+v", push.User)
	}
}

func TestCrossSessionStartupCapturesOwnEnvironmentOnce(t *testing.T) {
	isolateAdapterTest(t)
	tx, pushes := fakeInbox(t, nil, "")
	t.Setenv("XDG_RUNTIME_DIR", tx.runtimeDir)
	t.Setenv("CLAUDE_CODE_MESSAGING_SOCKET", tx.socketPath)
	t.Setenv("CLAUDE_CODE_MESSAGING_TOKEN", "")
	if got, _ := startupCrossSessionTransport(); got != nil {
		t.Fatal("missing token enabled fallback")
	}
	t.Setenv("CLAUDE_CODE_MESSAGING_TOKEN", tx.token)
	got, reason := startupCrossSessionTransport()
	if got == nil || !strings.Contains(reason, "owning host") {
		t.Fatalf("startup accepted this process instead of its parent: %s", reason)
	}
	// The fixture listener runs in this test process; the real startup must refuse
	// it. Explicitly supply the fixture owner only after asserting that refusal.
	got.hostPID = os.Getpid()
	t.Setenv("CLAUDE_CODE_MESSAGING_TOKEN", "changed-token")
	t.Setenv("CLAUDE_CODE_MESSAGING_SOCKET", "changed-socket")
	if err := got.Send(context.Background(), "captured", ""); err != nil {
		t.Fatal(err)
	}
	if nextInbox(t, pushes).Auth.Token != tx.token {
		t.Fatal("delivery re-read environment")
	}
}

func TestCrossSessionAttachReportsProbeAndLimits(t *testing.T) {
	isolateAdapterTest(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	a, _, frames := liveFixture(t, ipc.RenderQueueOnly)
	tx, _ := fakeInbox(t, nil, "")
	a.crossSession = tx
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "channel not registered"}
	results := make(chan *mcp.CallToolResult, 1)
	go func() {
		result, _ := a.toolAttach(context.Background(), &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Arguments: json.RawMessage(`{"name":"example"}`)}})
		results <- result
	}()
	raw := nextLiveFrame(t, frames)
	if op, _ := ipc.PeekOp(raw); op != ipc.OpAttach {
		t.Fatalf("expected attach: %s", raw)
	}
	raw, _ = json.Marshal(ipc.AttachedMsg{Op: ipc.OpAttached, OK: true, Channel: "telegram", ChatID: -100, Name: "example"})
	a.dispatchAttached(raw)
	select {
	case result := <-results:
		text := result.Content[0].(*mcp.TextContent).Text
		for _, want := range []string{"cross-session awaiting confirmation", "channel not registered", "permission relay unavailable", "peer user turns", "C3 reply tool"} {
			if !strings.Contains(text, want) {
				t.Fatalf("attach omitted %s: %s", want, text)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("attach did not return")
	}
}

func TestCrossSessionTransportRejectsUnsafePaths(t *testing.T) {
	isolateAdapterTest(t)
	tx, _ := fakeInbox(t, nil, "")
	outside, _ := fakeInbox(t, nil, "")
	regular := filepath.Join(tx.runtimeDir, "file")
	if err := os.WriteFile(regular, nil, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(tx.runtimeDir, "escape")
	if err := os.Symlink(outside.runtimeDir, link); err != nil {
		t.Fatal(err)
	}
	socketLink := filepath.Join(tx.runtimeDir, "socket-link")
	if err := os.Symlink(tx.socketPath, socketLink); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{outside.socketPath, regular, socketLink, filepath.Join(link, "s"), "relative.sock", filepath.Join(tx.runtimeDir, "missing")} {
		copy := *tx
		copy.socketPath = path
		if err := copy.Send(context.Background(), "test", ""); err == nil {
			t.Fatalf("unsafe path accepted: %s", path)
		}
	}
	if err := os.Chmod(tx.runtimeDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := tx.Send(context.Background(), "test", ""); err == nil {
		t.Fatal("non-private runtime accepted")
	}
}

func TestCrossSessionTransportBoundedClose(t *testing.T) {
	isolateAdapterTest(t)
	for _, kind := range []string{"stall", "response"} {
		t.Run(kind, func(t *testing.T) {
			var hold chan struct{}
			response := ""
			if kind == "stall" {
				hold = make(chan struct{})
				defer close(hold)
			} else {
				response = "unexpected data"
			}
			tx, _ := fakeInbox(t, hold, response)
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()
			start := time.Now()
			if err := tx.Send(ctx, "test", ""); err == nil {
				t.Fatal("non-clean peer close accepted")
			}
			if time.Since(start) > time.Second {
				t.Fatal("socket wait was unbounded")
			}
		})
	}
}

func TestCrossSessionBlockMatchesHostFixture(t *testing.T) {
	isolateAdapterTest(t)
	raw, err := os.ReadFile("testdata/claude-2.1.263-channel.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	var record struct{ Message struct{ Content string } }
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	frame := map[string]any{"content": "[Transcribed voice]: example message body", "meta": map[string]any{
		"attachment_file_id": "FILEID", "attachment_kind": "voice", "attachment_mime": "audio/ogg", "attachment_size": "3820112",
		"chat_id": "-1001111111111", "message_id": "11196", "message_thread_id": "914", "reply_to_message_id": "914",
		"reply_to_user": "bot", "ts": "2026-09-08T10:02:54.000Z", "user": "operator", "user_id": "1"}}
	block, err := crossSessionChannelBlock(frame)
	if err != nil || block != record.Message.Content {
		t.Fatalf("host block differs: %s (%v)", block, err)
	}
	frame["meta"].(map[string]any)["c3_delivery_id"] = `marker&"`
	block, err = crossSessionChannelBlock(frame)
	if err != nil {
		t.Fatal(err)
	}
	frame["meta"].(map[string]any)["c3_attempt"] = "cross-session:1"
	block, _ = crossSessionChannelBlock(frame)
	line, _ := json.Marshal(map[string]any{"type": "user", "isMeta": true, "origin": map[string]any{"kind": "peer", "from": "c3"}, "message": map[string]any{"role": "user", "content": "Another Claude session sent a message:\n" + block}})
	if !deliveryReceipt(line, `marker&"`, true, "cross-session:1") {
		t.Fatal("escaped metadata did not round trip")
	}
}

func TestCrossSessionEligibility(t *testing.T) {
	isolateAdapterTest(t)
	for _, tc := range []struct {
		state string
		env   bool
		want  string
	}{
		{ipc.RenderCapable, true, ipc.RenderCapable}, {ipc.RenderProbing, true, ipc.RenderProbing},
		{ipc.RenderQueueOnly, true, ipc.RenderProbing}, {ipc.RenderQueueOnly, false, ipc.RenderQueueOnly},
	} {
		t.Run(fmt.Sprintf("%s-%v", tc.state, tc.env), func(t *testing.T) {
			a, output, frames := liveFixture(t, tc.state)
			tx, pushes := fakeInbox(t, nil, "")
			if tc.env {
				a.crossSession = tx
			}
			a.initialRenderRoute = ipc.RenderRoute{State: tc.state, Reason: "channel not registered"}
			a.resetLiveRoute(false)
			if a.liveRoute().State != tc.want {
				t.Fatalf("route: %+v", a.liveRoute())
			}
			pushLive(t, a, "eligible")
			if tc.env && tc.state == ipc.RenderQueueOnly {
				nextInbox(t, pushes)
				if len(output.Bytes()) != 0 {
					t.Fatal("queue-only tried channel")
				}
			} else {
				if tc.state != ipc.RenderQueueOnly {
					appendChannelReceipt(t, a.livePath(), "eligible")
					nextLiveFrame(t, frames)
				}
				select {
				case <-pushes:
					t.Fatal("fallback used before channel failed")
				case <-time.After(40 * time.Millisecond):
				}
			}
		})
	}
}

func TestCrossSessionProbeReceiptAndAckOnce(t *testing.T) {
	isolateAdapterTest(t)
	a, output, frames := liveFixture(t, ipc.RenderQueueOnly)
	tx, pushes := fakeInbox(t, nil, "")
	a.crossSession = tx
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "channel not registered"}
	a.liveTimeout = time.Second
	a.resetLiveRoute(false)
	pushLive(t, a, "peer-receipt")
	push := nextInbox(t, pushes)
	select {
	case raw := <-frames:
		t.Fatalf("socket close acked: %s", raw)
	case <-time.After(120 * time.Millisecond):
	}
	pushLive(t, a, "held-during-probe")
	select {
	case <-pushes:
		t.Fatal("second probe pushed")
	default:
	}
	appendPeerReceipt(t, a.livePath(), push.User.Message.Content)
	var state ipc.RenderStateMsg
	raw := nextLiveFrame(t, frames)
	if json.Unmarshal(raw, &state) != nil || state.State != ipc.RenderCrossSession || !strings.Contains(state.Reason, "permission relay unavailable") {
		t.Fatalf("promotion: %s", raw)
	}
	var ack ipc.InboundDeliveredMsg
	raw = nextLiveFrame(t, frames)
	if json.Unmarshal(raw, &ack) != nil || ack.Op != ipc.OpInboundDelivered || !ack.OK || ack.Count != 2 || ack.DeliveryToken != "peer-receipt" {
		t.Fatalf("ack: %s", raw)
	}
	appendPeerReceipt(t, a.livePath(), push.User.Message.Content)
	noLiveFrame(t, frames)
	if len(output.Bytes()) != 0 {
		t.Fatal("fallback emitted channel notifications")
	}
	for _, want := range []string{"Live route: cross-session (channel not registered; permission relay unavailable)", "peer user turns", "Allow/Deny", "AskUserQuestion", "Slash commands", "ask tool still works", "C3 reply tool as usual"} {
		if !strings.Contains(a.buildInstructions(), want) {
			t.Fatalf("missing guidance: %s", want)
		}
	}
	// Subsequent confirmed-route messages use the same receipt gate.
	pushLive(t, a, "next-peer")
	push = nextInbox(t, pushes)
	appendPeerReceipt(t, a.livePath(), push.User.Message.Content)
	raw = nextLiveFrame(t, frames)
	if json.Unmarshal(raw, &ack) != nil || ack.DeliveryToken != "next-peer" {
		t.Fatalf("live route ack: %s", raw)
	}
}

func TestCrossSessionReceiptAlsoRequiresCleanClose(t *testing.T) {
	isolateAdapterTest(t)
	for _, response := range []string{"", "unexpected"} {
		t.Run(response, func(t *testing.T) {
			a, _, frames := liveFixture(t, ipc.RenderQueueOnly)
			hold := make(chan struct{})
			tx, pushes := fakeInbox(t, hold, response)
			a.crossSession = tx
			a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "channel not registered"}
			a.liveTimeout = time.Second
			a.resetLiveRoute(false)
			pushLive(t, a, "before-close")
			push := nextInbox(t, pushes)
			appendPeerReceipt(t, a.livePath(), push.User.Message.Content)
			select {
			case raw := <-frames:
				close(hold)
				t.Fatalf("receipt acked before socket completed: %s", raw)
			case <-time.After(120 * time.Millisecond):
			}
			close(hold)
			var state ipc.RenderStateMsg
			raw := nextLiveFrame(t, frames)
			if err := json.Unmarshal(raw, &state); err != nil {
				t.Fatal(err)
			}
			if response == "" {
				if state.State != ipc.RenderCrossSession {
					t.Fatalf("clean close did not promote: %s", raw)
				}
				raw = nextLiveFrame(t, frames)
				if op, _ := ipc.PeekOp(raw); op != ipc.OpInboundDelivered {
					t.Fatalf("missing ack: %s", raw)
				}
			} else {
				if state.State != ipc.RenderQueueOnly {
					t.Fatalf("bad exchange promoted: %s", raw)
				}
				noLiveFrame(t, frames)
			}
		})
	}
}

func TestCrossSessionTimeoutAndFailureStopPushes(t *testing.T) {
	isolateAdapterTest(t)
	var logs safeBuffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			a, output, frames := liveFixture(t, ipc.RenderQueueOnly)
			tx, pushes := fakeInbox(t, nil, "")
			a.crossSession = tx
			a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "channel not registered"}
			a.resetLiveRoute(false)
			if fail {
				tx.socketPath = filepath.Join(tx.runtimeDir, tx.token)
			}
			pushLive(t, a, "unconfirmed")
			if !fail {
				nextInbox(t, pushes)
			}
			var state ipc.RenderStateMsg
			raw := nextLiveFrame(t, frames)
			if json.Unmarshal(raw, &state) != nil || state.State != ipc.RenderQueueOnly || state.Reason != "cross-session push not confirmed" {
				t.Fatalf("failure: %s", raw)
			}
			pushLive(t, a, "do-not-retry")
			appendChannelReceipt(t, a.livePath(), "unconfirmed")
			noLiveFrame(t, frames)
			select {
			case <-pushes:
				t.Fatal("pushed after failure")
			default:
			}
			if len(output.Bytes()) != 0 {
				t.Fatal("channel used after fallback failure")
			}
			if strings.Contains(string(logs.Bytes()), tx.token) {
				t.Fatal("credential leaked in logs")
			}
		})
	}
}

func TestCrossSessionChannelFirstOnReconnect(t *testing.T) {
	isolateAdapterTest(t)
	a, output, frames := liveFixture(t, ipc.RenderProbing)
	a.deliveryHostInitialized.Store(true) // Reconnect of an already initialized host.
	tx, pushes := fakeInbox(t, nil, "")
	a.crossSession = tx
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderProbing, Reason: "channels flag present, awaiting confirmation"}
	pushLive(t, a, "channel-then-peer")
	if !strings.Contains(string(output.Bytes()), "channel-then-peer") {
		t.Fatal("channel was not first")
	}
	push := nextInbox(t, pushes) // automatic retry after the channel receipt window
	appendPeerReceipt(t, a.livePath(), push.User.Message.Content)
	for a.liveRoute().State != ipc.RenderCrossSession {
		nextLiveFrame(t, frames)
	}
	// Reconnect runs the real hello path against a fake broker.
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	hellos := make(chan ipc.HelloMsg, 1)
	go func() {
		peer := ipc.NewConn(server)
		raw, err := peer.ReadFrame()
		if err != nil {
			return
		}
		var hello ipc.HelloMsg
		_ = json.Unmarshal(raw, &hello)
		hellos <- hello
		_ = peer.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck})
	}()
	a.connectBrokerFn = func() error {
		a.bmu.Lock()
		a.conn = ipc.NewConn(client)
		a.helloPending = true
		a.bmu.Unlock()
		return nil
	}
	if err := a.reconnectBroker(); err != nil {
		t.Fatal(err)
	}
	hello := <-hellos
	if hello.RenderState != ipc.RenderProbing || strings.Contains(hello.RenderReason, "cross-session") || !hello.CannotRenderChannels {
		t.Fatalf("reconnect did not try channel first: %+v", hello)
	}
	before := len(output.Bytes())
	pushLive(t, a, "reconnected-channel")
	if len(output.Bytes()) <= before {
		t.Fatal("reconnect did not emit channel probe")
	}
	select {
	case <-pushes:
		t.Fatal("reconnect tried peer before channel")
	default:
	}
}

func TestLiveReadbackHostIntakePreventsCrossSessionFallback(t *testing.T) {
	isolateAdapterTest(t)
	for _, tc := range []struct {
		name  string
		index int
	}{{"enqueue", 0}, {"attachment", 3}} {
		t.Run(tc.name, func(t *testing.T) {
			a, _, frames := liveFixture(t, ipc.RenderProbing)
			a.liveTimeout = time.Second
			tx, pushes := fakeInbox(t, nil, "")
			a.crossSession = tx
			pushLive(t, a, "intake-marker")
			if err := os.WriteFile(a.livePath(), realHostIntakeReceipt(t, tc.index, "intake-marker"), 0600); err != nil {
				t.Fatal(err)
			}
			var state ipc.RenderStateMsg
			if raw := nextLiveFrame(t, frames); json.Unmarshal(raw, &state) != nil || state.State != ipc.RenderCapable {
				t.Fatalf("intake did not confirm channel: %s", raw)
			}
			var ack ipc.InboundDeliveredMsg
			if raw := nextLiveFrame(t, frames); json.Unmarshal(raw, &ack) != nil || ack.Op != ipc.OpInboundDelivered || !ack.OK || ack.DeliveryToken != "intake-marker" || ack.Count != 2 {
				t.Fatalf("intake ack: %s", raw)
			}
			// Later remove and absorbed attachment must not acknowledge again.
			f, err := os.OpenFile(a.livePath(), os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.Write(append(realHostIntakeReceipt(t, 2, "intake-marker"), realHostIntakeReceipt(t, 3, "intake-marker")...))
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			select {
			case push := <-pushes:
				t.Fatalf("unexpected fallback: %+v", push)
			case raw := <-frames:
				t.Fatalf("duplicate ack or route flap: %s", raw)
			case <-time.After(a.liveTimeout + 100*time.Millisecond):
			}
			a.liveMu.Lock()
			attempts, cross := a.liveAttempt, a.liveCrossSession
			a.liveMu.Unlock()
			if attempts != 1 || cross || a.liveRoute().State != ipc.RenderCapable {
				t.Fatal("channel intake started fallback or changed route")
			}
		})
	}
}
