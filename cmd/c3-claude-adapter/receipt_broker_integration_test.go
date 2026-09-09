package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// No host or daemon runs here: a real in-process broker owns persistence and
// acknowledgement, while the supplied host record stands in for transcript IO.
func TestReceiptBrokerLifecycle(t *testing.T) {
	for _, variant := range []string{"receipt", "enqueue receipt", "same route attach", "failed attach", "crash before receipt", "timeout fetch then death", "death before fetch"} {
		t.Run(variant, func(t *testing.T) {
			t.Setenv("CLAUDE_CODE_SESSION_ID", "")
			t.Setenv("C3_QUEUE_DIR", t.TempDir())
			t.Setenv("XDG_CONFIG_HOME", t.TempDir())
			t.Setenv("XDG_STATE_HOME", t.TempDir())
			b := broker.New(reconnectSwitchMappings())
			defer b.Shutdown()
			if err := b.RegisterChannel(&reconnectSwitchChannel{}); err != nil {
				t.Fatal(err)
			}
			// This inert child supplies a real liveness boundary without launching a CLI.
			holder := exec.Command("sleep", "60")
			if err := holder.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = holder.Process.Kill(); _ = holder.Wait() }()
			client, server := net.Pipe()
			defer client.Close()
			handlerDone := make(chan struct{})
			go func() { b.HandleConn(server); close(handlerDone) }()
			conn := ipc.NewConn(client)
			if err := conn.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: holder.Process.Pid, RenderState: ipc.RenderCapable}); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ReadFrame(); err != nil {
				t.Fatal(err)
			}
			stub := b.Stubs.Snapshot()[0]
			tid := int64(281)
			key := broker.MakeRouteKey("telegram", -100, &tid)
			qrk := queue.RouteKey{Channel: "telegram", ChatID: -100, TopicID: &tid}
			b.Routes.Claim(key, stub)
			stub.AddRoute(key)
			stub.MarkRouteConfirmed(key)
			stub.SetOutputRoute(key)
			older := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 1, Text: "older backlog", Timestamp: time.Now()}
			olderID, err := b.Queue.AppendTracked(qrk, older)
			if err != nil {
				t.Fatal(err)
			}
			a := newAdapter()
			seedLiveTranscript(t, a)
			a.conn = conn
			a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
			a.liveTimeout = 2 * time.Second
			if variant == "enqueue receipt" {
				// A configured fallback would start another attempt on timeout.
				a.crossSession = &crossSessionTransport{}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a.runCtx = ctx
			route := ipc.RouteRef{Channel: "telegram", ChatID: -100, TopicID: &tid, Name: "topic-a", Group: "main"}
			a.setRouteState([]ipc.RouteRef{route}, &route)
			var output safeBuffer
			a.notifyTx = newNotifyTransport(&mcp.IOTransport{Reader: nopCloseReader{strings.NewReader("")}, Writer: nopCloseWriter{&output}})
			if _, err := a.notifyTx.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			submit := func(job broker.Job) {
				t.Helper()
				if !b.Workers.SubmitWait(key, job) {
					t.Fatal("worker refused job")
				}
			}
			snapshot := func() []c3types.Inbound {
				t.Helper()
				result := make(chan broker.FetchResult, 1)
				submit(broker.Job{Kind: broker.JobFetch, Fetch: &broker.FetchJob{All: true, ResultCh: result}})
				select {
				case got := <-result:
					if got.Err != nil {
						t.Fatal(got.Err)
					}
					return got.Messages
				case <-time.After(3 * time.Second):
					t.Fatal("queue snapshot stalled")
					return nil
				}
			}
			read := func(op ipc.Op) []byte {
				t.Helper()
				_ = client.SetReadDeadline(time.Now().Add(4 * time.Second))
				defer client.SetReadDeadline(time.Time{})
				for {
					raw, err := conn.ReadFrame()
					if err != nil {
						t.Fatal(err)
					}
					if got, _ := ipc.PeekOp(raw); got == op {
						return raw
					}
				}
			}
			// The broker frame is produced by its real admission/push path.
			submit(broker.Job{Kind: broker.JobInbound, Inbound: &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, MessageID: 2, Text: "live body", Timestamp: time.Now()}})
			raw := read(ipc.OpInbound)
			a.handleInbound(ctx, raw)
			if got := snapshot(); len(got) != 2 {
				t.Fatalf("stdout prematurely consumed: %+v", got)
			}
			var notification struct {
				Params struct {
					Meta map[string]string `json:"meta"`
				} `json:"params"`
			}
			if err := json.Unmarshal(output.Bytes(), &notification); err != nil {
				t.Fatal(err)
			}
			marker := notification.Params.Meta["c3_delivery_id"]
			if marker == "" {
				t.Fatal("notification has no marker")
			}
			switch variant {
			case "same route attach", "failed attach":
				name := "topic-a"
				if variant == "failed attach" {
					name = "missing-topic"
				}
				request := newAttachReq(t, map[string]any{"channel": "telegram", "name": name, "group": "main"})
				attachDone := make(chan *mcp.CallToolResult, 1)
				go func() { result, _ := a.toolAttach(ctx, request); attachDone <- result }()
				result := read(ipc.OpAttached)
				var attached ipc.AttachedMsg
				if json.Unmarshal(result, &attached) != nil || attached.OK != (variant == "same route attach") {
					t.Fatalf("attach setup: %s", result)
				}
				a.dispatchAttached(result)
				select {
				case result := <-attachDone:
					if result == nil {
						t.Fatal("toolAttach returned no result")
					}
				case <-time.After(time.Second):
					t.Fatal("toolAttach did not finish")
				}
			}
			switch variant {
			case "receipt", "enqueue receipt", "same route attach", "failed attach":
				// Insert the marker actually emitted by the adapter into the independent
				// fixture; no call to buildClaudeChannelFrame manufactures the receipt.
				f, err := os.OpenFile(a.livePath(), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				receipt := realHostReceipt(t, marker)
				if variant == "enqueue receipt" {
					receipt = realHostIntakeReceipt(t, 0, marker)
				}
				_, err = f.Write(receipt)
				f.Close()
				if err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(3 * time.Second)
				for len(snapshot()) != 1 && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				rows, err := b.Queue.PeekTracked(qrk, -1)
				if err != nil || len(rows) != 1 || rows[0].RecordID != olderID {
					t.Fatalf("ack removed wrong row: %+v %v", rows, err)
				}
				if variant == "enqueue receipt" {
					deadline := time.Now().Add(3 * time.Second)
					for {
						a.liveMu.Lock()
						active, attempts, cross := a.liveActive, a.liveAttempt, a.liveCrossSession
						a.liveMu.Unlock()
						if attempts != 1 || cross {
							t.Fatal("enqueue started cross-session fallback")
						}
						if active == 0 {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("receipt watcher did not finish")
						}
						time.Sleep(10 * time.Millisecond)
					}
					if a.liveRoute().State != ipc.RenderCapable || len(snapshot()) != 1 {
						t.Fatal("enqueue did not consume exactly its row")
					}
				}
			case "timeout fetch then death":
				deadline := time.Now().Add(3 * time.Second)
				for a.liveRoute().State != ipc.RenderQueueOnly && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				if a.liveRoute().State != ipc.RenderQueueOnly {
					t.Fatal("no timeout")
				}
				if err := conn.WriteJSON(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "explicit-fetch", All: true, Ack: true}); err != nil {
					t.Fatal(err)
				}
				response := read(ipc.OpFetchQueueResult)
				var fetched ipc.FetchQueueResp
				if json.Unmarshal(response, &fetched) != nil || len(fetched.Messages) != 2 {
					t.Fatalf("fetch failed: %s", response)
				}
			}
			// Crash/re-attach variants leave the adapter's receipt loop before death.
			cancel()
			client.Close()
			<-handlerDone
			_ = holder.Process.Kill()
			_ = holder.Wait()
			// A synthesized event triggers the real dead-holder sweep without adding
			// another durable human row; a following worker job is the completion barrier.
			submit(broker.Job{Kind: broker.JobInbound, Inbound: &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &tid, Kind: c3types.InboundSystem, Text: "sweep"}})
			left := snapshot()
			want := 1
			if variant == "crash before receipt" || variant == "death before fetch" {
				want = 2
			}
			if variant == "timeout fetch then death" {
				want = 0
			}
			if len(left) != want {
				t.Fatalf("after holder death: %+v; want %d rows", left, want)
			}
		})
	}
}
