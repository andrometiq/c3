package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
)

func newTranscriptTestAdapter(t *testing.T) *adapter {
	t.Helper()
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	return newAdapter()
}

func seedPendingTranscriptPermission(a *adapter, requestID, transcriptPath string, offset int64, since time.Time, subagentOffsets map[string]int64) {
	a.permMu.Lock()
	a.initPermissionStateLocked()
	if subagentOffsets == nil {
		subagentOffsets = map[string]int64{}
	}
	a.permPending[requestID] = &pendingTranscriptPermission{
		since: since, transcriptPath: transcriptPath, mainOffset: offset, subagentOffsets: subagentOffsets,
	}
	a.permTranscriptPath = transcriptPath
	a.permMu.Unlock()
}

func transcriptToolResultLine(items ...map[string]any) []byte {
	content := make([]map[string]any, 0, len(items))
	for _, item := range items {
		content = append(content, item)
	}
	raw, _ := json.Marshal(map[string]any{
		"type": "user", "message": map[string]any{"content": content},
	})
	return append(raw, '\n')
}

func toolResult(toolUseID string, isError bool) map[string]any {
	item := map[string]any{"type": "tool_result", "tool_use_id": toolUseID}
	if isError {
		item["is_error"] = true
	}
	return item
}

func pollAndReadSettles(t *testing.T, a *adapter, peer *ipc.Conn, count int) []ipc.PermissionSettledMsg {
	t.Helper()
	done := make(chan struct{})
	go func() {
		a.pollPendingPermissions(time.Now())
		close(done)
	}()
	settles := make([]ipc.PermissionSettledMsg, 0, count)
	for range count {
		result := make(chan ipc.PermissionSettledMsg, 1)
		go func() {
			raw, err := peer.ReadFrame()
			if err != nil {
				return
			}
			var msg ipc.PermissionSettledMsg
			if json.Unmarshal(raw, &msg) == nil {
				result <- msg
			}
		}()
		select {
		case msg := <-result:
			settles = append(settles, msg)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for permission_settled")
		}
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("permission transcript poll did not finish")
	}
	return settles
}

func pendingTranscriptCount(a *adapter) int {
	a.permMu.Lock()
	defer a.permMu.Unlock()
	return len(a.permPending)
}

func TestRequestIDCandidatesVectors(t *testing.T) {
	// These three candidate-zero pairs come from same-session broker and
	// transcript records observed within three seconds of one another. They are
	// real host vectors, not values derived by this implementation.
	real := []struct {
		toolUseID string
		requestID string
	}{
		{toolUseID: "toolu_011nfN92iTr3PFK4djEQwHgP", requestID: "xjhbv"},
		{toolUseID: "toolu_017n7n5KN2it1eCabdGXyZyu", requestID: "ktyya"},
		{toolUseID: "toolu_01UnKZw3HiemK2LiEG3mogXN", requestID: "seyqg"},
	}
	for _, test := range real {
		got := requestIDCandidates(test.toolUseID)
		if len(got) != 11 || got[0] != test.requestID {
			t.Errorf("requestIDCandidates(%q) = %v; candidate 0 want %q", test.toolUseID, got, test.requestID)
		}
	}

	// Unlike the three real pairs above, this full candidate sequence is
	// self-derived. It pins the corrected rehash order :0 through :9 as well as
	// the FNV-1a and base-25 mechanics.
	wantSynthetic := []string{"wokmv", "xuuvs", "rzouk", "jkgyg", "dqaxz", "yowqp", "stqph", "keitd", "ejcsw", "zhykm", "tnsje"}
	got := requestIDCandidates("toolu_01ABCDEF0123456789")
	if len(got) != len(wantSynthetic) {
		t.Fatalf("synthetic candidate count = %d, want %d", len(got), len(wantSynthetic))
	}
	for index := range got {
		if got[index] != wantSynthetic[index] {
			t.Fatalf("synthetic candidate %d = %q, want %q; all=%v", index, got[index], wantSynthetic[index], got)
		}
	}
}

func TestTranscriptWatcher_MatchesOutcomesMultipleResultsAndRemoves(t *testing.T) {
	a, peer := adapterWithConn(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	path := filepath.Join(t.TempDir(), "session.jsonl")
	allowTool := "toolu_allow"
	errorTool := "toolu_error"
	ignoredTool := "toolu_ignored"
	allowID := strings.ToUpper(requestIDCandidates(allowTool)[0])
	errorID := requestIDCandidates(errorTool)[0]
	ignoredID := requestIDCandidates(ignoredTool)[0]
	seedPendingTranscriptPermission(a, allowID, path, 0, time.Now().Add(-time.Second), nil)
	seedPendingTranscriptPermission(a, errorID, path, 0, time.Now().Add(-time.Second), nil)
	seedPendingTranscriptPermission(a, ignoredID, path, 0, time.Now().Add(-time.Second), nil)
	line := transcriptToolResultLine(
		toolResult(allowTool, false),
		map[string]any{"type": "text", "tool_use_id": ignoredTool},
		toolResult(errorTool, true),
	)
	if err := os.WriteFile(path, line, 0600); err != nil {
		t.Fatal(err)
	}
	settles := pollAndReadSettles(t, a, peer, 2)
	got := map[string]string{}
	for _, settle := range settles {
		got[strings.ToLower(settle.RequestID)] = settle.Outcome
		if settle.Op != ipc.OpPermissionSettled {
			t.Fatalf("op = %q, want permission_settled", settle.Op)
		}
	}
	if got[strings.ToLower(allowID)] != "allow" || got[errorID] != "unknown" {
		t.Fatalf("settled outcomes = %v", got)
	}
	if pendingTranscriptCount(a) != 1 {
		t.Fatalf("matched prompts must be removed and ignored result retained; pending=%d", pendingTranscriptCount(a))
	}
}

func TestTranscriptWatcher_PartialLineIsNotConsumed(t *testing.T) {
	a, peer := adapterWithConn(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	path := filepath.Join(t.TempDir(), "session.jsonl")
	toolUseID := "toolu_partial"
	requestID := requestIDCandidates(toolUseID)[0]
	line := transcriptToolResultLine(toolResult(toolUseID, false))
	if err := os.WriteFile(path, bytes.TrimSuffix(line, []byte{'\n'}), 0600); err != nil {
		t.Fatal(err)
	}
	seedPendingTranscriptPermission(a, requestID, path, 0, time.Now().Add(-time.Second), nil)
	a.pollPendingPermissions(time.Now())
	a.permMu.Lock()
	offset := a.permPending[requestID].mainOffset
	a.permMu.Unlock()
	if offset != 0 || pendingTranscriptCount(a) != 1 {
		t.Fatalf("partial line was consumed: offset=%d pending=%d", offset, pendingTranscriptCount(a))
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.Write([]byte{'\n'})
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	settles := pollAndReadSettles(t, a, peer, 1)
	if settles[0].RequestID != requestID || pendingTranscriptCount(a) != 0 {
		t.Fatalf("completed line did not settle: %+v pending=%d", settles, pendingTranscriptCount(a))
	}
}

func TestTranscriptWatcher_FileLifecycle(t *testing.T) {
	t.Run("missing then created", func(t *testing.T) {
		a, peer := adapterWithConn(t)
		t.Setenv("CLAUDE_CODE_SESSION_ID", "")
		path := filepath.Join(t.TempDir(), "late.jsonl")
		toolUseID := "toolu_late_file"
		requestID := requestIDCandidates(toolUseID)[0]
		seedPendingTranscriptPermission(a, requestID, path, 0, time.Now().Add(-time.Second), nil)
		a.pollPendingPermissions(time.Now())
		if pendingTranscriptCount(a) != 1 {
			t.Fatal("missing transcript removed the prompt")
		}
		if err := os.WriteFile(path, transcriptToolResultLine(toolResult(toolUseID, false)), 0600); err != nil {
			t.Fatal(err)
		}
		settles := pollAndReadSettles(t, a, peer, 1)
		if settles[0].Outcome != "allow" {
			t.Fatalf("late file outcome = %q", settles[0].Outcome)
		}
	})

	t.Run("shrink advances to new size without rereading", func(t *testing.T) {
		sink := captureProtoLog(t)
		a := newTranscriptTestAdapter(t)
		t.Setenv("CLAUDE_CODE_SESSION_ID", "")
		path := filepath.Join(t.TempDir(), "shrunk.jsonl")
		if err := os.WriteFile(path, bytes.Repeat([]byte{'x'}, 4096), 0600); err != nil {
			t.Fatal(err)
		}
		toolUseID := "toolu_shrunk"
		requestID := requestIDCandidates(toolUseID)[0]
		seedPendingTranscriptPermission(a, requestID, path, 4096, time.Now().Add(-time.Second), nil)
		line := transcriptToolResultLine(toolResult(toolUseID, true))
		if err := os.WriteFile(path, line, 0600); err != nil {
			t.Fatal(err)
		}
		a.pollPendingPermissions(time.Now())
		a.pollPendingPermissions(time.Now())
		a.permMu.Lock()
		offset := a.permPending[requestID].mainOffset
		a.permMu.Unlock()
		if offset != int64(len(line)) || pendingTranscriptCount(a) != 1 {
			t.Fatalf("shrink cursor/pending = %d/%d, want %d/1", offset, pendingTranscriptCount(a), len(line))
		}
		if count := strings.Count(sink.String(), "perm: transcript shrank"); count != 1 {
			t.Fatalf("shrink anomaly log count = %d, want 1; log:\n%s", count, sink.String())
		}
	})
}

func TestTranscriptWatcher_ContentStringAndOldSubagentAreIgnored(t *testing.T) {
	a := newTranscriptTestAdapter(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	toolUseID := "toolu_string_content"
	requestID := requestIDCandidates(toolUseID)[0]
	raw, _ := json.Marshal(map[string]any{
		"type": "user", "message": map[string]any{"content": `{"type":"tool_result","tool_use_id":"` + toolUseID + `"}`},
	})
	if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	since := time.Now()
	seedPendingTranscriptPermission(a, requestID, path, 0, since, nil)

	oldPath := filepath.Join(subagentTranscriptRoot(path), "finished", "agent-old.jsonl")
	if err := os.MkdirAll(filepath.Dir(oldPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oldPath, transcriptToolResultLine(toolResult(toolUseID, false)), 0600); err != nil {
		t.Fatal(err)
	}
	oldTime := since.Add(-time.Hour)
	if err := os.Chtimes(oldPath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	originalOpen := openPermissionTranscript
	var mu sync.Mutex
	var opened []string
	openPermissionTranscript = func(name string) (*os.File, error) {
		mu.Lock()
		opened = append(opened, name)
		mu.Unlock()
		return os.Open(name)
	}
	t.Cleanup(func() { openPermissionTranscript = originalOpen })
	a.pollPendingPermissions(time.Now())
	if pendingTranscriptCount(a) != 1 {
		t.Fatal("string content or an old subagent file settled the prompt")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, name := range opened {
		if name == oldPath {
			t.Fatalf("subagent file older than the prompt was opened: %v", opened)
		}
	}
}

func TestParseTranscriptToolResults_IsErrorShapesAreIndependent(t *testing.T) {
	line := transcriptToolResultLine(
		map[string]any{"type": "tool_result", "tool_use_id": "toolu_absent"},
		map[string]any{"type": "tool_result", "tool_use_id": "toolu_false", "is_error": false},
		map[string]any{"type": "tool_result", "tool_use_id": "toolu_odd", "is_error": "not-a-bool"},
		map[string]any{"type": "tool_result", "tool_use_id": "toolu_true", "is_error": true},
	)
	results := parseTranscriptToolResults(bytes.TrimSuffix(line, []byte{'\n'}))
	want := []transcriptToolResult{
		{toolUseID: "toolu_absent", outcome: "allow"},
		{toolUseID: "toolu_false", outcome: "allow"},
		{toolUseID: "toolu_true", outcome: "unknown"},
	}
	if len(results) != len(want) {
		t.Fatalf("parsed results = %+v, want %+v", results, want)
	}
	for i := range want {
		if results[i] != want[i] {
			t.Fatalf("parsed result %d = %+v, want %+v", i, results[i], want[i])
		}
	}
}

func TestTranscriptWatcher_SubagentTranscriptSettles(t *testing.T) {
	a, peer := adapterWithConn(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	toolUseID := "toolu_subagent"
	requestID := requestIDCandidates(toolUseID)[0]
	seedPendingTranscriptPermission(a, requestID, path, 0, time.Now().Add(-time.Second), nil)
	subagentPath := filepath.Join(subagentTranscriptRoot(path), "workflow", "nested", "agent-one.jsonl")
	if err := os.MkdirAll(filepath.Dir(subagentPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(subagentPath, transcriptToolResultLine(toolResult(toolUseID, false)), 0600); err != nil {
		t.Fatal(err)
	}
	settles := pollAndReadSettles(t, a, peer, 1)
	if settles[0].RequestID != requestID || settles[0].Outcome != "allow" {
		t.Fatalf("subagent settle = %+v", settles[0])
	}
}

func TestTranscriptWatcher_OversizedLineAndFollowingLine(t *testing.T) {
	a, peer := adapterWithConn(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	path := filepath.Join(t.TempDir(), "session.jsonl")
	oversizedTool := "toolu_oversized"
	followingTool := "toolu_following"
	oversizedID := requestIDCandidates(oversizedTool)[0]
	followingID := requestIDCandidates(followingTool)[0]
	seedPendingTranscriptPermission(a, oversizedID, path, 0, time.Now().Add(-time.Second), nil)
	seedPendingTranscriptPermission(a, followingID, path, 0, time.Now().Add(-time.Second), nil)
	var file bytes.Buffer
	file.WriteString(`{"tool_use_id":"` + oversizedTool + `","padding":"`)
	file.Write(bytes.Repeat([]byte{'x'}, maxTranscriptLineBytes+1))
	file.WriteString("\"}\n")
	file.Write(transcriptToolResultLine(toolResult(followingTool, false)))
	if err := os.WriteFile(path, file.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	settles := pollAndReadSettles(t, a, peer, 2)
	got := map[string]string{}
	for _, settle := range settles {
		got[settle.RequestID] = settle.Outcome
	}
	if got[oversizedID] != "unknown" || got[followingID] != "allow" {
		t.Fatalf("oversized/following outcomes = %v", got)
	}
}

func TestTranscriptWatcher_NeverReadsBeforeCapturedOffset(t *testing.T) {
	a := newTranscriptTestAdapter(t)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	toolUseID := "toolu_before_offset"
	requestID := requestIDCandidates(toolUseID)[0]
	preexisting := bytes.Repeat([]byte{'x'}, 1<<20)
	preexisting = append(preexisting, '\n')
	preexisting = append(preexisting, transcriptToolResultLine(toolResult(toolUseID, false))...)
	if err := os.WriteFile(path, preexisting, 0600); err != nil {
		t.Fatal(err)
	}
	captured := int64(len(preexisting))
	appended := transcriptToolResultLine(map[string]any{"type": "text", "text": "later"})
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.Write(appended)
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	seedPendingTranscriptPermission(a, requestID, path, captured, time.Now().Add(-time.Second), nil)
	settles, stats, err := a.scanPermissionTranscript(path, captured, false)
	if err != nil {
		t.Fatal(err)
	}
	if stats.start != captured || stats.bytesRead != int64(len(appended)) {
		t.Fatalf("scan start/read = %d/%d, want %d/%d", stats.start, stats.bytesRead, captured, len(appended))
	}
	if len(settles) != 0 || pendingTranscriptCount(a) != 1 {
		t.Fatal("watcher read and matched a tool_result before the captured offset")
	}
}

func TestTranscriptWatcher_BrokerVerdictRemovesPending(t *testing.T) {
	a, peer := adapterWithConn(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	path := filepath.Join(t.TempDir(), "session.jsonl")
	toolUseID := "toolu_verdict_first"
	requestID := requestIDCandidates(toolUseID)[0]
	if err := os.WriteFile(path, transcriptToolResultLine(toolResult(toolUseID, false)), 0600); err != nil {
		t.Fatal(err)
	}
	seedPendingTranscriptPermission(a, requestID, path, 0, time.Now().Add(-time.Second), nil)
	a.dispatchPermissionVerdict(mustMarshal(t, ipc.PermissionVerdictMsg{
		Op: ipc.OpPermissionVerdict, RequestID: requestID, Behavior: "deny",
	}))
	a.pollPendingPermissions(time.Now())
	if pendingTranscriptCount(a) != 0 {
		t.Fatal("broker verdict did not remove the pending transcript observation")
	}
	done := make(chan error, 1)
	go func() {
		done <- a.currentConn().WriteJSON(ipc.PermissionReq{
			Op: ipc.OpPermissionRequest, RequestID: "sentinel",
		})
	}()
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if op, _ := ipc.PeekOp(raw); op != ipc.OpPermissionRequest {
		t.Fatalf("a settle followed the broker verdict; first later frame was %s", raw)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTranscriptWatcher_RetriesObservedSettleAfterReconnect(t *testing.T) {
	a, peer := adapterWithConn(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	path := filepath.Join(t.TempDir(), "session.jsonl")
	toolUseID := "toolu_reconnect_retry"
	requestID := requestIDCandidates(toolUseID)[0]
	if err := os.WriteFile(path, transcriptToolResultLine(toolResult(toolUseID, false)), 0600); err != nil {
		t.Fatal(err)
	}
	seedPendingTranscriptPermission(a, requestID, path, 0, time.Now().Add(-time.Second), nil)
	a.bmu.Lock()
	connection := a.conn
	a.conn = nil
	a.bmu.Unlock()
	a.pollPendingPermissions(time.Now())
	if pendingTranscriptCount(a) != 1 {
		t.Fatal("an observed settle was lost while the broker was reconnecting")
	}
	a.bmu.Lock()
	a.conn = connection
	a.bmu.Unlock()
	settles := pollAndReadSettles(t, a, peer, 1)
	if settles[0].RequestID != requestID || settles[0].Outcome != "allow" {
		t.Fatalf("retried settle = %+v", settles[0])
	}
}

func TestTranscriptWatcher_LifecycleAndTTL(t *testing.T) {
	a := newTranscriptTestAdapter(t)
	path := filepath.Join(t.TempDir(), "missing.jsonl")
	a.addPendingPermission("abcde", permissionSnapshot{
		since: time.Now(), transcriptPath: path, subagentOffsets: map[string]int64{},
	})
	waitPermissionWatcherState(t, a, true)
	a.dispatchPermissionVerdict(mustMarshal(t, ipc.PermissionVerdictMsg{
		Op: ipc.OpPermissionVerdict, RequestID: "abcde", Behavior: "allow",
	}))
	waitPermissionWatcherState(t, a, false)
	a.addPendingPermission("bcdef", permissionSnapshot{
		since: time.Now(), transcriptPath: path, subagentOffsets: map[string]int64{},
	})
	waitPermissionWatcherState(t, a, true)
	a.removePendingPermission("bcdef")
	waitPermissionWatcherState(t, a, false)
}

func TestTranscriptWatcher_TTLExpiryLogsMissingSettle(t *testing.T) {
	sink := captureProtoLog(t)
	a := newTranscriptTestAdapter(t)
	path := filepath.Join(t.TempDir(), "missing.jsonl")
	seedPendingTranscriptPermission(a, "abcde", path, 0, time.Now().Add(-permissionPendingTTL-time.Second), nil)
	a.pollPendingPermissions(time.Now())
	if pendingTranscriptCount(a) != 0 {
		t.Fatal("30-minute adapter permission TTL did not remove the entry")
	}
	if !strings.Contains(sink.String(), "permission abcde expired without a transcript settle") {
		t.Fatalf("TTL drift signal missing; log:\n%s", sink.String())
	}
}

func TestPermissionTranscriptLateHandoffLogsUnwatchedCount(t *testing.T) {
	sink := captureProtoLog(t)
	a := newTranscriptTestAdapter(t)
	a.addPendingPermission("first", permissionSnapshot{since: time.Now()})
	a.addPendingPermission("second", permissionSnapshot{since: time.Now()})
	a.removePendingPermission("first")

	a.setCurrentStableIdentity(sessionhandoff.Entry{
		StableSessionID: "stable-session", TranscriptPath: filepath.Join(t.TempDir(), "session.jsonl"), UnixNano: 1,
	})
	if !strings.Contains(sink.String(), "transcript path became available with 1 pending permission(s) without a watcher") {
		t.Fatalf("late-handoff count missing; log:\n%s", sink.String())
	}
	a.permMu.Lock()
	withoutWatcher, running := len(a.permWithoutWatcher), a.permWatcherRunning
	a.permMu.Unlock()
	if withoutWatcher != 0 || running {
		t.Fatalf("late handoff retroactively started observation: unwatched=%d running=%v", withoutWatcher, running)
	}
}

func TestOldBrokerPermissionSettledErrorDisablesOnce(t *testing.T) {
	sink := captureProtoLog(t)
	a := newTranscriptTestAdapter(t)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	seedPendingTranscriptPermission(a, "abcde", path, 0, time.Now(), nil)

	a.handleBrokerError(permissionSettledOldError)
	a.handleBrokerError(permissionSettledOldError)
	a.addPendingPermission("bcdef", permissionSnapshot{since: time.Now(), transcriptPath: path})
	if count := strings.Count(sink.String(), permissionSettledOldLog); count != 1 {
		t.Fatalf("old-broker notice count = %d, want 1; log:\n%s", count, sink.String())
	}
	if !a.permSettleDisabled.Load() || pendingTranscriptCount(a) != 0 {
		t.Fatalf("settling was not disabled cleanly: disabled=%v pending=%d", a.permSettleDisabled.Load(), pendingTranscriptCount(a))
	}
}

func waitPermissionWatcherState(t *testing.T, a *adapter, want bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		a.permMu.Lock()
		got := a.permWatcherRunning
		a.permMu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("permission watcher running state did not become %v", want)
}

func TestTranscriptPathRefreshesForSameStableSession(t *testing.T) {
	a := newTranscriptTestAdapter(t)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	a.setCurrentStableIdentity(sessionhandoff.Entry{
		StableSessionID: "stable-session", TranscriptPath: "/state/old.jsonl", UnixNano: 1,
	})
	if err := sessionhandoff.Write("stable-session", sessionhandoff.Entry{
		StableSessionID: "stable-session", TranscriptPath: "/state/new.jsonl", UnixNano: 2,
	}); err != nil {
		t.Fatal(err)
	}
	a.checkForIdentitySwitch(context.Background())
	current, ok := a.currentStableIdentity()
	if !ok || current.TranscriptPath != "/state/new.jsonl" {
		t.Fatalf("same-session handoff did not refresh transcript path: ok=%v entry=%+v", ok, current)
	}
}

func TestAdvanceIdentityHandoff_KeepsKnownTranscriptPathOnEmptyRewrite(t *testing.T) {
	a := newTranscriptTestAdapter(t)
	known := filepath.Join(t.TempDir(), "known.jsonl")
	a.setCurrentStableIdentity(sessionhandoff.Entry{
		StableSessionID: "stable-session", TranscriptPath: known, CWD: "old-cwd", Source: "old-source", UnixNano: 1,
	})
	a.advanceIdentityHandoff("stable-session", sessionhandoff.Entry{
		StableSessionID: "stable-session", CWD: "new-cwd", Source: "new-source", UnixNano: 2,
	})
	current, ok := a.currentStableIdentity()
	if !ok || current.TranscriptPath != known || current.CWD != "new-cwd" || current.Source != "new-source" {
		t.Fatalf("empty transcript rewrite damaged handoff: ok=%v entry=%+v", ok, current)
	}
}

func TestCapturePermissionSnapshotReadsHandoffAndFileOffsets(t *testing.T) {
	a := newAdapter()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("CLAUDE_CODE_SESSION_ID", "adapter-instance")
	path := filepath.Join(t.TempDir(), "stable-session.jsonl")
	if err := os.WriteFile(path, []byte("existing-main\n"), 0600); err != nil {
		t.Fatal(err)
	}
	subagentPath := filepath.Join(subagentTranscriptRoot(path), "agent-existing.jsonl")
	if err := os.MkdirAll(filepath.Dir(subagentPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(subagentPath, []byte("existing-subagent\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := sessionhandoff.Write("adapter-instance", sessionhandoff.Entry{
		StableSessionID: "stable-session", TranscriptPath: path, UnixNano: 1,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := a.capturePermissionSnapshot()
	if snapshot.transcriptPath != path || snapshot.mainOffset != int64(len("existing-main\n")) {
		t.Fatalf("main transcript snapshot = %+v", snapshot)
	}
	if snapshot.subagentOffsets[subagentPath] != int64(len("existing-subagent\n")) {
		t.Fatalf("subagent offsets = %v", snapshot.subagentOffsets)
	}
}

func TestInterceptConn_CapturesOffsetBeforeAsyncPermissionRelay(t *testing.T) {
	a, peer := adapterWithConn(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	a.setCurrentStableIdentity(sessionhandoff.Entry{
		StableSessionID: "stable-session", TranscriptPath: path, UnixNano: 1,
	})
	// A is already pending when B is captured. A's poll consumes the line before
	// B's async handler can add B, so B can settle only if its own captured
	// offset gate makes the next poll seek back far enough.
	requestA := requestIDCandidates("toolu_existing_prompt")[0]
	seedPendingTranscriptPermission(a, requestA, path, 0, time.Now().Add(-time.Second), nil)
	toolUseID := "toolu_between_notification_and_handler"
	requestID := requestIDCandidates(toolUseID)[0]
	permissionRequest := &jsonrpc.Request{
		Method: permissionRequestMethod,
		Params: json.RawMessage(`{"request_id":"` + requestID + `","tool_name":"Bash"}`),
	}
	next := &jsonrpc.Request{ID: mustID(t, float64(1)), Method: "tools/call"}
	tx := newNotifyTransport(&scriptedTransport{conn: &scriptedConn{frames: []jsonrpc.Message{permissionRequest, next}}})
	tx.SetPermissionHandler(a.handlePermissionRequest)
	tx.SetPermissionSnapshotter(a.capturePermissionSnapshot)
	conn, err := tx.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if message, err := conn.Read(context.Background()); err != nil || message != next {
		t.Fatalf("intercept read = %v, %v", message, err)
	}
	if err := os.WriteFile(path, transcriptToolResultLine(toolResult(toolUseID, false)), 0600); err != nil {
		t.Fatal(err)
	}
	a.pollPendingPermissions(time.Now())
	a.permMu.Lock()
	offsetA := a.permPending[requestA].mainOffset
	_, bAddedEarly := a.permPending[requestID]
	a.permMu.Unlock()
	if offsetA == 0 || bAddedEarly {
		t.Fatalf("pre-registration poll state: A offset=%d B added=%v", offsetA, bAddedEarly)
	}
	first, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if op, _ := ipc.PeekOp(first); op != ipc.OpPermissionRequest {
		t.Fatalf("settle preceded permission request on the wire: %s", first)
	}
	result := make(chan []byte, 1)
	go func() {
		raw, err := peer.ReadFrame()
		if err == nil {
			result <- raw
		}
	}()
	select {
	case raw := <-result:
		var settled ipc.PermissionSettledMsg
		if json.Unmarshal(raw, &settled) != nil || settled.RequestID != requestID {
			t.Fatalf("tool_result appended before handler was missed: %s", raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tool_result appended between notification and handler did not settle")
	}
	a.removePendingPermission(requestA)
	waitPermissionWatcherState(t, a, false)
}

func TestOversizedScannerDoesNotMatchEscapedContent(t *testing.T) {
	toolUseID := "toolu_literal"
	var hashes []uint32
	scanner := oversizedToolIDScanner{}
	scanner.Write([]byte(`{"text":"\"tool_use_id\":\"`+toolUseID+`\""}`), func(hash uint32) {
		hashes = append(hashes, hash)
	})
	if len(hashes) != 0 {
		t.Fatalf("escaped content token produced hashes: %v", hashes)
	}
}
