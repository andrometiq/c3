package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const testOwnerKey = "host_abcdef12_1234_5678"

func ownerHandoffFixture(t *testing.T) (*adapter, sessionhandoff.Entry) {
	t.Helper()
	t.Setenv("CLAUDE_CODE_SESSION_ID", "A")
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	a := newAdapter()
	a.ownerKey, a.ownerKeyOK = testOwnerKey, true
	a.initialRenderRoute = ipc.RenderRoute{State: ipc.RenderCapable}
	a.deliveryHostInitialized.Store(true)
	a.notifyTx = newNotifyTransport(nil)
	path := filepath.Join(t.TempDir(), "B.jsonl")
	if err := os.WriteFile(path, []byte("existing transcript\n"), 0600); err != nil {
		t.Fatal(err)
	}
	entry := sessionhandoff.Entry{StableSessionID: "B", TranscriptPath: path, CWD: "/workspace/project", UnixNano: 10}
	writeOwnerHandoff(t, "B", entry)
	return a, entry
}

func writeOwnerHandoff(t *testing.T, key string, entry sessionhandoff.Entry) {
	t.Helper()
	if err := sessionhandoff.Write(key, entry); err != nil {
		t.Fatal(err)
	}
}

func assertOwnerLiveGate(t *testing.T, a *adapter, path string) {
	t.Helper()
	if got := a.permissionTranscriptPath(); got != path {
		t.Fatalf("transcript=%q, want %q", got, path)
	}
	live := a.deliveryFacts().Channel
	if path == "" {
		if live.Eligible || live.Reason != "session transcript unavailable" {
			t.Fatalf("gate must close: %+v", live)
		}
	} else if !live.Eligible {
		t.Fatalf("gate must open for %s: %+v", path, live)
	}
}

func TestOwnerHandoffResumeLiveGate(t *testing.T) {
	isolateAdapterTest(t)
	for _, tc := range []struct {
		name, key                     string
		exact, disabled, noTranscript bool
	}{
		{name: "own owner", key: testOwnerKey},
		{name: "foreign pid", key: "host_abcdef12_9999_5678"},
		{name: "reused pid", key: "host_abcdef12_1234_9999"},
		{name: "previous boot", key: "host_99999999_1234_5678"},
		{name: "unidentified owner", key: testOwnerKey, disabled: true},
		{name: "exact key wins", key: testOwnerKey, exact: true},
		{name: "exact key without transcript stays closed", key: testOwnerKey, exact: true, noTranscript: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, entry := ownerHandoffFixture(t)
			a.ownerKeyOK = !tc.disabled
			writeOwnerHandoff(t, tc.key, entry)
			ownerPath, err := sessionhandoff.Path(tc.key)
			if err != nil {
				t.Fatal(err)
			}
			ownerBefore, err := os.ReadFile(ownerPath)
			if err != nil {
				t.Fatal(err)
			}
			want := ""
			if tc.key == testOwnerKey && !tc.disabled {
				want = entry.TranscriptPath
			}
			if tc.exact {
				exact := entry
				exact.StableSessionID = "A"
				exact.TranscriptPath = filepath.Join(t.TempDir(), "A.jsonl")
				if err := os.WriteFile(exact.TranscriptPath, nil, 0600); err != nil {
					t.Fatal(err)
				}
				if tc.noTranscript {
					exact.TranscriptPath = ""
				}
				writeOwnerHandoff(t, "A", exact)
				want = exact.TranscriptPath
			}
			// Drive the production resolver and delivery eligibility gate before recovery.
			assertOwnerLiveGate(t, a, want)
			got, ok := a.watchForHandoff(context.Background(), "A", time.Millisecond, 0)
			if ok != (want != "" || tc.exact) || (ok && got.TranscriptPath != want) {
				t.Fatalf("watch=(%+v,%v), want path %q", got, ok, want)
			}
			if a.ownerResolved.Load() != (ok && !tc.exact) {
				t.Fatal("wrong resolution source")
			}
			ownerAfter, err := os.ReadFile(ownerPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(ownerBefore, ownerAfter) {
				t.Fatal("consumer modified owner alias")
			}
		})
	}
}

func TestOwnerHandoffLateAppearance(t *testing.T) {
	isolateAdapterTest(t)
	a, entry := ownerHandoffFixture(t)
	assertOwnerLiveGate(t, a, "")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type result struct {
		entry sessionhandoff.Entry
		ok    bool
	}
	done := make(chan result, 1)
	go func() { e, ok := a.watchForHandoff(ctx, "A", time.Millisecond, time.Second); done <- result{e, ok} }()
	select {
	case got := <-done:
		t.Fatalf("watch returned before owner appeared: %+v", got)
	case <-time.After(25 * time.Millisecond):
	}
	writeOwnerHandoff(t, testOwnerKey, entry)
	select {
	case got := <-done:
		if !got.ok || got.entry != entry {
			t.Fatalf("late owner not resolved: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("watch missed late owner")
	}
	assertOwnerLiveGate(t, a, entry.TranscriptPath)
}

// The same watch -> fireRecover -> broker response path used at startup, with
// synthetic IPC. No owner-resolution flags or stable identity are seeded here.
func recoverOwnerFixture(t *testing.T, a *adapter) *recoveryBroker {
	t.Helper()
	entry, ok := a.watchForHandoff(context.Background(), "A", time.Millisecond, 0)
	if !ok {
		t.Fatal("no startup handoff")
	}
	broker := newRecoveryBroker(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan recoverOutcome, 1)
	a.recoverStarted.Store(true)
	go func() { done <- a.fireRecover(ctx, entry) }()
	req := nextIdentitySwitchRecover(t, broker)
	if req.StableSessionID != entry.StableSessionID {
		t.Fatalf("recover=%+v", req)
	}
	answerRecover(t, a)
	select {
	case outcome := <-done:
		if !outcome.registered {
			t.Fatal("recovery failed to register identity")
		}
	case <-ctx.Done():
		t.Fatal("recovery did not finish")
	}
	return broker
}

func ownerSnapshotHost(t *testing.T, a *adapter, ctx context.Context) *mcp.ClientSession {
	t.Helper()
	srv := a.buildMCPServer()
	srv.AddTool(&mcp.Tool{Name: "owner_snapshot", InputSchema: map[string]any{"type": "object"}},
		func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			a.awaitIdentitySettled(ctx)
			snapshot := a.capturePermissionSnapshot()
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("%s:%d", snapshot.transcriptPath, snapshot.mainOffset)}}}, nil
		})
	clientT, serverT := mcp.NewInMemoryTransports()
	a.notifyTx = newNotifyTransport(serverT)
	done := make(chan struct{})
	go func() { _ = srv.Run(ctx, a.notifyTx); close(done) }()
	client, err := newTestClient().Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("MCP server failed to stop")
		}
	})
	return client
}

func TestOwnerHandoffToolsCallRefresh(t *testing.T) {
	isolateAdapterTest(t)
	for _, exact := range []bool{false, true} {
		t.Run(fmt.Sprintf("exact=%v", exact), func(t *testing.T) {
			a, entry := ownerHandoffFixture(t)
			writeOwnerHandoff(t, testOwnerKey, entry)
			if exact {
				entry.StableSessionID = "A"
				entry.TranscriptPath = filepath.Join(t.TempDir(), "A.jsonl")
				if err := os.WriteFile(entry.TranscriptPath, []byte("old transcript\n"), 0600); err != nil {
					t.Fatal(err)
				}
				writeOwnerHandoff(t, "A", entry)
			}
			broker := recoverOwnerFixture(t, a)
			assertOwnerLiveGate(t, a, entry.TranscriptPath)
			initial := a.capturePermissionSnapshot()
			if initial.mainOffset == 0 {
				t.Fatal("fixture needs a nonzero initial offset")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			a.runCtx = ctx
			client := ownerSnapshotHost(t, a, ctx)

			// SessionStart compact re-fire keeps the identity and replaces its transcript.
			compact := entry
			compact.UnixNano = 20
			compact.Source = "compact"
			compact.CWD = "/workspace/after-cd"
			compact.TranscriptPath = filepath.Join(t.TempDir(), "compact.jsonl")
			if err := os.WriteFile(compact.TranscriptPath, nil, 0600); err != nil {
				t.Fatal(err)
			}
			writeOwnerHandoff(t, compact.StableSessionID, compact)
			if !exact {
				writeOwnerHandoff(t, testOwnerKey, compact)
			}
			got, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "owner_snapshot", Arguments: map[string]any{}})
			if err != nil {
				t.Fatal(err)
			}
			if text := multiRouteResultText(t, got); text != compact.TranscriptPath+":0" {
				t.Fatalf("compact tools/call used stale transcript/offset: %s", text)
			}
			current, ok := a.currentStableIdentity()
			if !ok || current.TranscriptPath != compact.TranscriptPath || current.UnixNano != 20 || current.CWD != compact.CWD {
				t.Fatalf("compact identity not refreshed: %+v", current)
			}

			// Clear only changes the owner and new stable alias; the old stable alias
			// remains self-referential. An id-resolved session must ignore this write.
			cleared := compact
			cleared.StableSessionID = "C"
			cleared.Source = "clear"
			cleared.UnixNano = 30
			cleared.TranscriptPath = filepath.Join(t.TempDir(), "C.jsonl")
			if err := os.WriteFile(cleared.TranscriptPath, nil, 0600); err != nil {
				t.Fatal(err)
			}
			writeOwnerHandoff(t, "C", cleared)
			writeOwnerHandoff(t, testOwnerKey, cleared)
			type callResult struct {
				result *mcp.CallToolResult
				err    error
			}
			done := make(chan callResult, 1)
			go func() {
				r, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "owner_snapshot", Arguments: map[string]any{}})
				done <- callResult{r, err}
			}()
			want := compact
			if !exact {
				req := nextIdentitySwitchRecover(t, broker)
				if req.StableSessionID != "C" {
					t.Fatalf("clear recovered wrong session: %+v", req)
				}
				answerRecover(t, a)
				want = cleared
			}
			select {
			case result := <-done:
				if result.err != nil {
					t.Fatal(result.err)
				}
				if text := multiRouteResultText(t, result.result); text != want.TranscriptPath+":0" {
					t.Fatalf("clear tools/call transcript=%s, want %s", text, want.TranscriptPath)
				}
			case <-ctx.Done():
				t.Fatal("tools/call did not finish")
			}
			current, ok = a.currentStableIdentity()
			if !ok || current.StableSessionID != want.StableSessionID {
				t.Fatalf("clear identity=%+v, want %s", current, want.StableSessionID)
			}
			if exact {
				select {
				case frame := <-broker.frames:
					t.Fatalf("id-resolved adapter switched via owner: %s", frame)
				default:
				}
			}
			assertOwnerLiveGate(t, a, want.TranscriptPath)
		})
	}
}
