//go:build linux || darwin

package main

import (
	"bufio"
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

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// Auth-less connections are capability probes. A real delivery must still
// send both existing frames and half-close, then await the peer's clean close.
func deliveryInbox(t *testing.T, stall bool) (*crossSessionTransport, <-chan inboxPush) {
	t.Helper()
	root, err := os.MkdirTemp("", "c3-inbox-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	if err := os.Mkdir(filepath.Join(root, "cc-socks"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "cc-socks", fmt.Sprintf("%d.sock", os.Getpid()))
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	pushes := make(chan inboxPush, 32)
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			c.SetDeadline(time.Now().Add(3 * time.Second))
			reader := bufio.NewReader(c)
			auth, err := reader.ReadBytes('\n')
			if len(auth) == 0 {
				c.Close()
				continue
			}
			var p inboxPush
			p.Bytes = len(auth)
			p.Err = err
			if err == nil {
				p.Err = json.Unmarshal(auth, &p.Auth)
			}
			user, err := reader.ReadBytes('\n')
			p.Bytes += len(user)
			if err != nil {
				p.Err = err
			} else if e := json.Unmarshal(user, &p.User); e != nil {
				p.Err = e
			}
			if _, err := reader.ReadByte(); err != io.EOF {
				p.Err = fmt.Errorf("expected half-close: %v", err)
			}
			pushes <- p
			if stall {
				time.Sleep(2500 * time.Millisecond)
			}
			c.Close()
		}
	}()
	return &crossSessionTransport{runtimeDir: root, socketPath: path, token: "fixture-credential", hostPID: os.Getpid()}, pushes
}

func inboxAdapter(t *testing.T, stall bool) (*adapter, *safeBuffer, <-chan []byte, <-chan inboxPush, context.Context) {
	t.Helper()
	a, out, frames := liveFixture(t, ipc.RenderQueueOnly)
	tx, pushes := deliveryInbox(t, stall)
	a.crossSession = tx
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "no dev-channels flag on host"}
	a.acceptDelivery(a.deliveryOffer(), &ipc.DeliveryAcceptance{Version: 1, Modes: []string{"channel", "inbox"}})
	if !a.deliveryAccepted.Load() {
		t.Fatal("flagless inbox did not negotiate")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a.runCtx = ctx
	return a, out, frames, pushes, ctx
}
func inboxDeliver(t *testing.T, a *adapter, ctx context.Context, token string, budget int64) {
	t.Helper()
	raw, _ := json.Marshal(ipc.DeliverMsg{Op: ipc.OpDeliver, Token: token, Transport: "inbox", DeadlineMS: budget, Inbound: c3types.Inbound{Text: "hello"}})
	a.handleDeliver(ctx, raw)
}
func nextAttemptResult(t *testing.T, frames <-chan []byte) ipc.AttemptResultMsg {
	t.Helper()
	for {
		raw := nextLiveFrame(t, frames)
		var result ipc.AttemptResultMsg
		if json.Unmarshal(raw, &result) != nil {
			t.Fatal(string(raw))
		}
		if result.Op == ipc.OpAttemptResult {
			return result
		}
		if result.Op != ipc.OpDeliveryReport {
			t.Fatal(string(raw))
		}
	}
}

func TestNegotiatedInboxFlaglessAndBackground(t *testing.T) {
	// P1/P2: "channel OR inbox is eligible"; existing fakeTree host detection,
	// including a background/fork host beneath a flagged interactive ancestor.
	for _, args := range [][]string{{"claude"}, {"claude", "--bg"}, {"claude", "--fork-session", "--print"}} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			a, out, frames, pushes, ctx := inboxAdapter(t, false)
			tree := fakeTree(map[int][]string{10: {"c3-claude-adapter"}, 11: args, 12: {"claude", devChannelsFlag, "plugin:c3@c3"}}, map[int]int{10: 11, 11: 12, 12: 1})
			a.deliveryHostRoute = func() ipc.RenderRoute { return detectRenderRoute("linux", 10, tree) }
			facts := a.deliveryFacts()
			if facts.Channel.Eligible || !facts.Inbox.Eligible {
				t.Fatal(facts)
			}
			inboxDeliver(t, a, ctx, "inbox-token", 15000)
			push := nextInbox(t, pushes)
			if push.Auth.Token != a.crossSession.token || push.User.Type != "user" || push.User.From != "c3" || push.User.Priority != "next" || !strings.Contains(push.User.Message.Content, `c3_delivery_id="inbox-token"`) || !strings.Contains(push.User.Message.Content, `c3_attempt="inbox:1"`) {
				t.Fatal("incorrect inbox frames")
			}
			appendPeerReceipt(t, a.livePath(), push.User.Message.Content)
			result := nextAttemptResult(t, frames)
			if result.Outcome != "confirmed" || result.Token != "inbox-token" {
				t.Fatal(result)
			}
			if len(out.Bytes()) != 0 || a.liveRoute().State != "waiting" {
				t.Fatal("adapter selected channel or promoted display")
			}
			a.liveMu.Lock()
			legacy := a.liveCrossSession || len(a.livePending) > 0
			a.liveMu.Unlock()
			if legacy {
				t.Fatal("negotiated delivery used legacy latches")
			}
		})
	}
}

func TestNegotiatedInboxFixturesFailClosed(t *testing.T) {
	// P3/R2: "peer provenance" and "enqueue/queued_command variants keep the fail-closed prefix rule".
	peer, err := os.ReadFile("testdata/claude-2.1.263-peer.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	intake, err := os.ReadFile("testdata/claude-2.1.266-intake.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(intake)), "\n")
	cases := map[string]struct {
		line string
		want bool
	}{
		"2.1.263-peer":                     {strings.ReplaceAll(strings.ReplaceAll(string(peer), "cross-session:1", "inbox:1"), "DELIVERYTOKEN-3", "fixture"), true},
		"2.1.266-enqueue-no-prefix":        {strings.ReplaceAll(strings.ReplaceAll(lines[1], "cross-session:3", "inbox:1"), "DELIVERYTOKEN-1", "fixture"), false},
		"2.1.266-queued-command-no-prefix": {strings.ReplaceAll(strings.ReplaceAll(lines[3], "channel:1", "inbox:1"), "DELIVERYTOKEN-1", "fixture"), false},
	}
	for _, name := range []string{"2.1.266-enqueue-no-prefix", "2.1.266-queued-command-no-prefix"} {
		tc := cases[name]
		var entry map[string]any
		json.Unmarshal([]byte(tc.line), &entry)
		if entry["type"] == "queue-operation" {
			entry["content"] = "Another Claude session sent a message:\n" + entry["content"].(string)
		} else {
			att := entry["attachment"].(map[string]any)
			att["prompt"] = "Another Claude session sent a message:\n" + att["prompt"].(string)
			att["origin"] = map[string]any{"kind": "peer", "from": "c3"}
		}
		raw, _ := json.Marshal(entry)
		cases[strings.ReplaceAll(name, "no-prefix", "prefixed")] = struct {
			line string
			want bool
		}{string(raw), true}
	}
	valid := cases["2.1.263-peer"].line
	for name, pair := range map[string][2]string{"not-meta": {`"isMeta":true`, `"isMeta":false`}, "wrong-kind": {`"kind":"peer"`, `"kind":"channel"`}, "wrong-from": {`"from":"c3"`, `"from":"other"`}, "wrong-prefix": {"Another Claude session sent a message:", "Peer input:"}, "old-channel-attempt": {"inbox:1", "channel:1"}} {
		cases[name] = struct {
			line string
			want bool
		}{strings.Replace(valid, pair[0], pair[1], 1), false}
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			a, _, frames, pushes, ctx := inboxAdapter(t, false)
			inboxDeliver(t, a, ctx, "fixture", 15000)
			nextInbox(t, pushes)
			waitDeliveryWritten(t, a, "fixture")
			f, err := os.OpenFile(a.livePath(), os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.WriteString(tc.line + "\n")
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			a.pollDeliveries()
			if tc.want {
				if r := nextAttemptResult(t, frames); r.Outcome != "confirmed" {
					t.Fatal(r)
				}
			} else {
				noLiveFrame(t, frames)
			}
		})
	}
}

func TestNegotiatedInboxWriteBoundAndFailure(t *testing.T) {
	// P6: "2 s bounded write" is inside the fresh 15 s observation deadline.
	a, _, frames, pushes, ctx := inboxAdapter(t, true)
	start := time.Now()
	inboxDeliver(t, a, ctx, "stalled", 15000)
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("inbox write blocked broker reader")
	}
	nextInbox(t, pushes)
	var result ipc.AttemptResultMsg
	select {
	case raw := <-frames:
		if json.Unmarshal(raw, &result) != nil {
			t.Fatal(string(raw))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bounded inbox write did not report failure")
	}
	elapsed := time.Since(start)
	if result.Outcome != "failed" || result.Reason != "inbox write failed" || elapsed < 1800*time.Millisecond || elapsed > 3*time.Second {
		t.Fatal(result, elapsed)
	}
	if a.liveRoute().State != "waiting" {
		t.Fatal("adapter chose exhaustion policy")
	}
}

func TestNegotiatedInboxEligibilityChangesAndNoUnsafeWrite(t *testing.T) {
	// P2/P5: "semantic-change rule; rearm on change"; D031: no auth to an invalid peer.
	a, _, frames, pushes, _ := inboxAdapter(t, false)
	a.pollDeliveries()
	select {
	case f := <-frames:
		t.Fatal("unchanged report", string(f))
	default:
	}
	tx := a.crossSession
	if err := os.Chmod(tx.runtimeDir, 0755); err != nil {
		t.Fatal(err)
	}
	a.pollDeliveries()
	var report ipc.DeliveryReportMsg
	raw := nextLiveFrame(t, frames)
	json.Unmarshal(raw, &report)
	if report.Op != ipc.OpDeliveryReport || report.Live.Inbox.Eligible {
		t.Fatal(string(raw))
	}
	a.pollDeliveries()
	select {
	case f := <-frames:
		t.Fatal("repeated report", string(f))
	default:
	}
	if len(a.deliveryOffer()) != 0 {
		t.Fatal("neither eligible still offered")
	}
	if err := os.Chmod(tx.runtimeDir, 0700); err != nil {
		t.Fatal(err)
	}
	a.pollDeliveries()
	raw = nextLiveFrame(t, frames)
	json.Unmarshal(raw, &report)
	if !report.Live.Inbox.Eligible {
		t.Fatal(string(raw))
	}
	moved := tx.socketPath + ".away"
	if err := os.Rename(tx.socketPath, moved); err != nil {
		t.Fatal(err)
	}
	a.pollDeliveries()
	raw = nextLiveFrame(t, frames)
	json.Unmarshal(raw, &report)
	if report.Live.Inbox.Eligible {
		t.Fatal("disappeared socket still eligible")
	}
	if err := os.Rename(moved, tx.socketPath); err != nil {
		t.Fatal(err)
	}
	a.pollDeliveries()
	raw = nextLiveFrame(t, frames)
	json.Unmarshal(raw, &report)
	if !report.Live.Inbox.Eligible {
		t.Fatal("reappeared socket still ineligible")
	}
	a.liveTranscriptPath = func() string { return "" }
	if a.deliveryFacts().Inbox.Eligible || len(a.deliveryOffer()) > 0 {
		t.Fatal("unreadable transcript still eligible")
	}
	select {
	case p := <-pushes:
		t.Fatal("eligibility probe disclosed credentials", p.Bytes)
	default:
	}
}

func TestNegotiatedInboxPeerPIDMismatch(t *testing.T) {
	a, _, frames, pushes, ctx := inboxAdapter(t, false)
	tx := a.crossSession
	wrong := os.Getpid() + 100000
	path := filepath.Join(tx.runtimeDir, "cc-socks", fmt.Sprintf("%d.sock", wrong))
	if err := os.Rename(tx.socketPath, path); err != nil {
		t.Fatal(err)
	}
	tx.socketPath, tx.hostPID = path, wrong
	if a.deliveryFacts().Inbox.Eligible {
		t.Fatal("unvalidated peer eligible")
	}
	inboxDeliver(t, a, ctx, "invalid-peer", 15000)
	if r := nextAttemptResult(t, frames); r.Outcome != "failed" {
		t.Fatal(r)
	}
	select {
	case p := <-pushes:
		t.Fatal("credentials sent to wrong peer", p.Bytes)
	default:
	}
}

func TestNegotiatedInboxNoSelfFallbackOrRetry(t *testing.T) {
	// P3: adapter self-fallback and probe latches are legacy only.
	a, out, frames, pushes, ctx := inboxAdapter(t, false)
	raw, _ := json.Marshal(ipc.DeliverMsg{Op: ipc.OpDeliver, Token: "channel-expired", Transport: "channel", DeadlineMS: 100, Inbound: c3types.Inbound{Text: "hello"}})
	a.handleDeliver(ctx, raw)
	waitDeliveryWritten(t, a, "channel-expired")
	time.Sleep(250 * time.Millisecond)
	select {
	case p := <-pushes:
		t.Fatal("self fallback", p.Bytes)
	default:
	}
	inboxDeliver(t, a, ctx, "inbox-expired", 100)
	nextInbox(t, pushes)
	before := len(out.Bytes())
	a.resetLiveRoute(true)
	pushLive(t, a, "legacy-frame")
	time.Sleep(250 * time.Millisecond)
	select {
	case p := <-pushes:
		t.Fatal("self retry", p.Bytes)
	default:
	}
	if len(out.Bytes()) != before || a.liveRoute().State != "waiting" {
		t.Fatal("legacy policy ran")
	}
	noLiveFrame(t, frames)
}
