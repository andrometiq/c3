//go:build linux || darwin

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
)

func TestReviewUnrelatedUserSocketMustNotReceiveCredentials(t *testing.T) {
	isolateAdapterTest(t)
	tx, pushes := fakeInbox(t, nil, "")
	// This is a same-user socket with no owning-host PID basename.
	wrong := filepath.Join(filepath.Dir(tx.socketPath), "unrelated.sock")
	if err := os.Rename(tx.socketPath, wrong); err != nil {
		t.Fatal(err)
	}
	tx.socketPath = wrong
	if err := tx.Send(context.Background(), "private input", ""); err == nil {
		t.Fatal("unrelated user socket accepted")
	}
	select {
	case <-pushes:
		t.Fatal("unrelated socket received credentials")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReviewQuotedTextMustNotBeReceipt(t *testing.T) {
	isolateAdapterTest(t)
	block := `<channel source="plugin:c3:c3" c3_attempt="cross-session:1" c3_delivery_id="T">body</channel>`
	for _, text := range []string{"Please explain this quote: " + block, "<!--" + block + "-->", `<wrapper note='` + block + `'>quoted</wrapper>`} {
		line, _ := json.Marshal(map[string]any{"type": "user", "isMeta": true, "origin": map[string]any{"kind": "peer", "from": "c3"}, "message": map[string]any{"role": "user", "content": "Another Claude session sent a message:\n" + text}})
		if deliveryReceipt(line, "T", true, "cross-session:1") {
			t.Errorf("quoted text accepted: %s", text)
		}
	}
}

func TestReviewLateChannelReceiptMustNotConfirmCrossSession(t *testing.T) {
	isolateAdapterTest(t)
	a, output, frames := liveFixture(t, ipc.RenderProbing)
	tx, pushes := fakeInbox(t, nil, "")
	a.crossSession = tx
	pushLive(t, a, "late-channel")
	var notification struct{ Params map[string]any }
	if err := json.Unmarshal(output.Bytes(), &notification); err != nil {
		t.Fatal(err)
	}
	block, err := crossSessionChannelBlock(notification.Params)
	if err != nil {
		t.Fatal(err)
	}
	nextInbox(t, pushes)
	appendPeerReceipt(t, a.livePath(), block) // Even peer metadata cannot turn a channel attempt into fallback.
	for {
		raw := nextLiveFrame(t, frames)
		if op, _ := ipc.PeekOp(raw); op == ipc.OpInboundDelivered {
			t.Fatal("late channel receipt acknowledged fallback")
		}
		var state ipc.RenderStateMsg
		_ = json.Unmarshal(raw, &state)
		if state.State == ipc.RenderCrossSession {
			t.Fatal("late channel receipt promoted fallback")
		}
		if state.State == ipc.RenderQueueOnly {
			break
		}
	}
	if strings.Contains(a.liveRoute().Reason, tx.token) {
		t.Fatal("credential in route reason")
	}
}

func TestCrossSessionPeerPIDMismatchWritesZeroBytes(t *testing.T) {
	isolateAdapterTest(t)
	tx, pushes := fakeInbox(t, nil, "")
	// Rename to the expected parent PID, but keep the listener in this process.
	tx.hostPID = os.Getppid()
	path := filepath.Join(filepath.Dir(tx.socketPath), fmt.Sprintf("%d.sock", tx.hostPID))
	if err := os.Rename(tx.socketPath, path); err != nil {
		t.Fatal(err)
	}
	tx.socketPath = path
	if _, err := tx.validatedSocket(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Send(context.Background(), "private input", ""); err == nil || !strings.Contains(err.Error(), "connected peer") {
		t.Fatalf("peer PID mismatch: %v", err)
	}
	select {
	case push := <-pushes:
		if push.Bytes != 0 || push.Err != io.EOF {
			t.Fatalf("peer received %d bytes before rejection: %v", push.Bytes, push.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("fake inbox did not observe disconnect")
	}
}

func TestCrossSessionOwningAncestor(t *testing.T) {
	isolateAdapterTest(t)
	for _, tc := range []struct {
		name    string
		args    map[int][]string
		parents map[int]int
		want    int
	}{
		{"parent", map[int][]string{10: {"claude"}}, nil, 10},
		{"wrapper", map[int][]string{10: {"sh"}, 20: {"claude"}, 30: {"claude"}}, map[int]int{10: 20, 20: 30}, 20},
		{"nearest", map[int][]string{10: {"claude"}, 20: {"claude"}}, map[int]int{10: 20}, 10},
		{"node", map[int][]string{10: {"sh"}, 20: {"node", "/opt/node_modules/@anthropic-ai/claude-code/cli.js"}}, map[int]int{10: 20}, 20},
		{"unreadable", map[int][]string{20: {"claude"}}, map[int]int{10: 20}, 0},
		{"cycle", map[int][]string{10: {"sh"}, 20: {"sh"}}, map[int]int{10: 20, 20: 10}, 0},
		{"direct parent only", map[int][]string{10: {"host-wrapper"}}, map[int]int{10: 1}, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := owningClaudePID(10, fakeTree(tc.args, tc.parents)); got != tc.want {
				t.Fatalf("owner=%d, want %d", got, tc.want)
			}
		})
	}
}

func TestCrossSessionOutboundCapBeforeAuth(t *testing.T) {
	isolateAdapterTest(t)
	for _, body := range []string{strings.Repeat("x", ipc.MaxFrameSize), strings.Repeat("<", ipc.MaxFrameSize/6)} {
		tx, pushes := fakeInbox(t, nil, "")
		if err := tx.Send(context.Background(), body, ""); err == nil || !strings.Contains(err.Error(), "exceeds IPC cap") {
			t.Fatalf("oversize: %v", err)
		}
		select {
		case <-pushes:
			t.Fatal("oversized frame disclosed auth")
		case <-time.After(30 * time.Millisecond):
		}
	}
}

func TestCrossSessionReceiptCannotConfirmChannelAttempt(t *testing.T) {
	isolateAdapterTest(t)
	a, output, frames := liveFixture(t, ipc.RenderProbing)
	a.liveTimeout = time.Second
	pushLive(t, a, "same-token")
	var notification struct{ Params map[string]any }
	if err := json.Unmarshal(output.Bytes(), &notification); err != nil {
		t.Fatal(err)
	}
	notification.Params["meta"].(map[string]any)["c3_attempt"] = "cross-session:1"
	block, err := crossSessionChannelBlock(notification.Params)
	if err != nil {
		t.Fatal(err)
	}
	appendPeerReceipt(t, a.livePath(), block)
	noLiveFrame(t, frames)
	// A bare peer record also cannot borrow the outstanding channel watcher.
	appendTranscriptContent(t, a.livePath(), block, true)
	noLiveFrame(t, frames)
	appendChannelReceipt(t, a.livePath(), "same-token")
	nextLiveFrame(t, frames) // promotion
	var ack ipc.InboundDeliveredMsg
	if raw := nextLiveFrame(t, frames); json.Unmarshal(raw, &ack) != nil || !ack.OK {
		t.Fatalf("correct channel attempt not confirmed: %s", raw)
	}
}

func appendTranscriptContent(t *testing.T, path, content string, meta bool) {
	t.Helper()
	line, err := json.Marshal(map[string]any{"type": "user", "isMeta": meta, "origin": map[string]any{"kind": "peer", "from": "c3"}, "message": map[string]any{"role": "user", "content": content}})
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

func TestCrossSessionCredentialsRequirePIDAndUID(t *testing.T) {
	isolateAdapterTest(t)
	pid, uid := os.Getpid(), uint32(os.Getuid())
	for _, tc := range []struct {
		host, peer int
		uid        uint32
		want       bool
	}{
		{pid, pid, uid, true}, {pid, pid + 1, uid, false}, {pid, pid, uid ^ 1, false}, {0, 0, uid, false},
	} {
		if got := crossSessionCredentialsMatch(tc.host, tc.peer, tc.uid); got != tc.want {
			t.Fatalf("credential match=%v want=%v", got, tc.want)
		}
	}
}

func TestCrossSessionValidatesConnectedDescriptor(t *testing.T) {
	isolateAdapterTest(t)
	tx, _ := fakeInbox(t, nil, "")
	conn, err := net.Dial("unix", tx.socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := os.Remove(tx.socketPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tx.socketPath, []byte("substituted pathname"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateCrossSessionPeer(conn, tx.hostPID); err != nil {
		t.Fatalf("validation followed substituted path instead of connected socket: %v", err)
	}
	conn.Close()
	if err := validateCrossSessionPeer(conn, tx.hostPID); err == nil {
		t.Fatal("closed fd accepted")
	}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	if err := validateCrossSessionPeer(client, tx.hostPID); err == nil {
		t.Fatal("connection without kernel credentials accepted")
	}
}

func TestCrossSessionUsesSessionStartUUID(t *testing.T) {
	isolateAdapterTest(t)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "adapter-instance")
	sessionID := "11111111-2222-4333-8444-555555555555"
	if err := sessionhandoff.Write("adapter-instance", sessionhandoff.Entry{StableSessionID: sessionID}); err != nil {
		t.Fatal(err)
	}
	a, _, _ := liveFixture(t, ipc.RenderQueueOnly)
	tx, pushes := fakeInbox(t, nil, "")
	a.crossSession = tx
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly}
	a.resetLiveRoute(false)
	pushLive(t, a, "handoff")
	if push := nextInbox(t, pushes); push.User.SessionID != sessionID {
		t.Fatal("SessionStart UUID not sent")
	}
}

func TestCrossSessionAttemptCounterSurvivesReprobe(t *testing.T) {
	isolateAdapterTest(t)
	a, _, frames := liveFixture(t, ipc.RenderQueueOnly)
	tx, pushes := fakeInbox(t, nil, "")
	a.crossSession = tx
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly}
	a.resetLiveRoute(false)
	pushLive(t, a, "repeat-token")
	old := nextInbox(t, pushes)
	if !strings.Contains(old.User.Message.Content, `c3_attempt="cross-session:1"`) {
		t.Fatal("first marker missing")
	}
	a.resetLiveRoute(false)
	pushLive(t, a, "repeat-token")
	current := nextInbox(t, pushes)
	if !strings.Contains(current.User.Message.Content, `c3_attempt="cross-session:2"`) {
		t.Fatal("attempt counter reset at reprobe")
	}
	appendPeerReceipt(t, a.livePath(), old.User.Message.Content)
	raw := nextLiveFrame(t, frames)
	var state ipc.RenderStateMsg
	if json.Unmarshal(raw, &state) != nil || state.State != ipc.RenderQueueOnly {
		t.Fatalf("old attempt confirmed new probe: %s", raw)
	}
}
