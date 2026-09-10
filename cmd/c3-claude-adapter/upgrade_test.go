package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/buildidentity"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestUpgradeToolContract(t *testing.T) {
	isolateAdapterTest(t)
	a := newAdapter()
	pair := connectUpgradeSDK(t, a, a.buildMCPServer(), nil)
	ctx := pair.ctx
	cs, err := newTestClient().Connect(ctx, &scriptedTransport{conn: pair.conn}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pair.abort(); _ = cs.Close() })
	list, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(list.Tools)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != upgradeToolContract {
		t.Fatalf("tool contract changed: got %s; change the compatibility epoch and require reconnect", got)
	}
	// Each cached field must independently change the same hash used by the pin.
	for _, field := range []string{"name", "inputSchema", "description"} {
		t.Run(field, func(t *testing.T) {
			var changed []*mcp.Tool
			if err := json.Unmarshal(data, &changed); err != nil {
				t.Fatal(err)
			}
			baseline, err := json.Marshal(changed)
			if err != nil || sha256.Sum256(baseline) != sum {
				t.Fatalf("tool clone changed the hash before mutation: %v", err)
			}
			switch field {
			case "name":
				changed[0].Name += "_changed"
			case "inputSchema":
				changed[0].InputSchema = map[string]any{"type": "string"}
			case "description":
				changed[0].Description += " changed"
			}
			mutated, err := json.Marshal(changed)
			if err != nil || sha256.Sum256(mutated) == sum {
				t.Fatalf("%s is not covered by the contract hash: %v", field, err)
			}
		})
	}
}

func TestResumeSDKMethodMatrix(t *testing.T) {
	isolateAdapterTest(t)
	state := upgradeResumeState{Contract: upgradeContract(), MCP: mcp.ServerSessionState{
		InitializeParams: &mcp.InitializeParams{ProtocolVersion: "2025-03-26"}, InitializedParams: &mcp.InitializedParams{},
	}}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(upgradeResumeEnv, base64.StdEncoding.EncodeToString(raw))
	a, peer := adapterWithConn(t)
	opts, err := a.restoreUpgrade([]string{"--mcp-resume"})
	if err != nil {
		t.Fatal(err)
	}
	srv := a.buildMCPServer()
	initialized := make(chan error, 1)
	srv.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			result, err := next(ctx, method, req)
			if method == "notifications/initialized" {
				initialized <- err
			}
			return result, err
		}
	})
	pair := connectUpgradeSDK(t, a, srv, opts)
	ctx, conn := pair.ctx, pair.conn
	call := func(method, params string) json.RawMessage {
		t.Helper()
		id := mustID(t, method)
		if err := conn.Write(ctx, &jsonrpc.Request{ID: id, Method: method, Params: json.RawMessage(params)}); err != nil {
			t.Fatal(err)
		}
		msg, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		response, ok := msg.(*jsonrpc.Response)
		if !ok || response.ID != id || response.Error != nil {
			t.Fatalf("%s: %#v", method, msg)
		}
		return response.Result
	}
	// tools/list is the first frame: no initialize request is sent.
	var list mcp.ListToolsResult
	if err := json.Unmarshal(call("tools/list", `{}`), &list); err != nil {
		t.Fatal(err)
	}
	toolJSON, err := json.Marshal(list.Tools)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(toolJSON)
	if hex.EncodeToString(sum[:]) != upgradeToolContract {
		t.Fatal("resumed tools differ from cached contract")
	}
	brokerDone := make(chan error, 1)
	go func() {
		raw, err := peer.ReadFrame()
		if err != nil {
			brokerDone <- err
			return
		}
		if op, err := ipc.PeekOp(raw); err != nil || op != ipc.OpFetchQueue {
			brokerDone <- fmt.Errorf("expected fetch_queue, got %s: %v", op, err)
			pair.abort()
			return
		}
		var request ipc.FetchQueueReq
		if err := json.Unmarshal(raw, &request); err != nil {
			brokerDone <- err
			return
		}
		response, err := json.Marshal(ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: request.ID})
		if err == nil {
			a.dispatchFetchQueueResult(response)
		}
		brokerDone <- err
	}()
	var toolResult mcp.CallToolResult
	if err := json.Unmarshal(call("tools/call", `{"name":"fetch_queue","arguments":{}}`), &toolResult); err != nil {
		t.Fatal(err)
	}
	if err := <-brokerDone; err != nil {
		t.Fatal(err)
	}
	if toolResult.IsError || len(toolResult.Content) != 1 {
		t.Fatalf("tool failed: %+v", toolResult)
	}
	if content, ok := toolResult.Content[0].(*mcp.TextContent); !ok || !strings.Contains(content.Text, "queue is empty") {
		t.Fatalf("tool response: %+v", toolResult.Content)
	}
	if result := call("ping", `{}`); string(result) != `{}` {
		t.Fatalf("ping: %s", result)
	}
	if err := conn.Write(ctx, &jsonrpc.Request{Method: "notifications/initialized", Params: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-initialized:
		if err != nil || !a.deliveryHostInitialized.Load() {
			t.Fatalf("initialized: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	notified := make(chan error, 1)
	go func() {
		notified <- a.notifyTx.Notify(ctx, "notifications/claude/channel", map[string]any{"content": "after resume", "meta": map[string]any{}})
	}()
	msg, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	notice, ok := msg.(*jsonrpc.Request)
	if !ok || notice.ID.IsValid() || notice.Method != "notifications/claude/channel" || !strings.Contains(string(notice.Params), "after resume") {
		t.Fatalf("notification: %#v", msg)
	}
	if err := <-notified; err != nil {
		t.Fatal(err)
	}
	call("ping", `{}`) // The repeated initialized notification leaves the session usable.
}

type upgradeClosingConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *upgradeClosingConn) Close() error {
	c.once.Do(func() { close(c.entered); <-c.release })
	return c.Conn.Close()
}

func TestUpgradeRetryConcurrentAdmission(t *testing.T) {
	isolateAdapterTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a, output, _ := liveFixture(t, ipc.RenderCapable)
	left, right := net.Pipe()
	defer right.Close()
	closing := &upgradeClosingConn{Conn: left, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(closing.release) }) }
	defer release()
	defer left.Close()
	a.conn = ipc.NewConn(closing)
	ready, _ := upgradeReadyAdapter(t)
	a.upgrade.wire = ready.upgrade.wire
	wire := a.upgrade.wire
	input := []byte("{\"jsonrpc\":\"2.0\",\"id\":41,\"method\":\"ping\"}\n")
	wire.readAvailable = func(p []byte) (int, error) { n := copy(p, input); input = input[n:]; return n, nil }
	a.acceptUpgrade(&ipc.UpgradeHint{Path: "adapter", Build: "next"})
	a.upgrade.quiescing.Store(false)
	a.upgrade.retryAt = time.Now().Add(-time.Second)
	gateDone := make(chan struct{})
	go func() { a.pollUpgrade(time.Now()); close(gateDone) }()
	select {
	case <-closing.entered:
	case <-ctx.Done():
		t.Fatal("retry did not close socket")
	}
	// Race real push and MCP admission against an in-progress socket close.
	pushStarted, pushDone := make(chan struct{}), make(chan struct{})
	raw, _ := json.Marshal(ipc.InboundMsg{Op: ipc.OpInbound, DeliveryToken: "racing-push", Inbound: c3types.Inbound{Text: "hello"}})
	go func() { close(pushStarted); a.handleInbound(ctx, raw); close(pushDone) }()
	readStarted, readDone := make(chan struct{}), make(chan error, 1)
	go func() { close(readStarted); _, err := wire.Read(ctx); readDone <- err }()
	<-pushStarted
	<-readStarted
	select {
	case <-pushDone:
		t.Fatal("push admission escaped the socket-close lock")
	case err := <-readDone:
		t.Fatalf("MCP admission escaped the socket-close lock: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	release()
	<-gateDone
	select {
	case <-pushDone:
	case <-ctx.Done():
		t.Fatal("push did not leave row held")
	}
	if a.liveActive != 0 || len(a.livePending) != 0 || len(output.Bytes()) != 0 {
		t.Fatal("push was admitted after close")
	}
	select {
	case err := <-readDone:
		t.Fatalf("MCP admission resumed before broker recovery: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	a.acceptUpgrade(nil) // New hello finishes before request admission resumes.
	a.finishUpgradeRecovery()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("MCP admission did not resume")
	}
	// The opposite ordering: a goroutine-admitted request makes retry postpone.
	if a.withUpgradeRehelloIdle(func() { t.Error("closed with an admitted request") }) {
		t.Fatal("retry accepted a busy transport")
	}
}

func TestUpgradePausedReaderClose(t *testing.T) {
	isolateAdapterTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wire := &upgradeTransport{resumeReads: make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, err := wire.Read(ctx); done <- err }()
	if err := wire.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("close: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("paused reader did not close")
	}
}
func TestResumeSDKCallWithoutInitialize(t *testing.T) {
	isolateAdapterTest(t)
	a := newAdapter()
	a.upgrade.resumed = true
	pair := connectUpgradeSDK(t, a, a.buildMCPServer(), &mcp.ServerSessionOptions{State: &mcp.ServerSessionState{InitializeParams: &mcp.InitializeParams{ProtocolVersion: "2025-03-26"}, InitializedParams: &mcp.InitializedParams{}}})
	ctx, conn := pair.ctx, pair.conn
	id := mustID(t, "retained-host-id-41")
	if err := conn.Write(ctx, &jsonrpc.Request{ID: id, Method: "tools/call", Params: json.RawMessage(`{"name":"fetch_queue","arguments":{}}`)}); err != nil {
		t.Fatal(err)
	}
	msg, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	response, ok := msg.(*jsonrpc.Response)
	if !ok || response.ID != id || response.Error != nil || !strings.Contains(string(response.Result), "broker reconnecting") {
		t.Fatalf("resume call: %#v", msg)
	}
}
func upgradeReadyAdapter(t *testing.T) (*adapter, *int) {
	t.Helper()
	a := newAdapter()
	a.upgrade.wire = &upgradeTransport{output: io.Discard, state: mcp.ServerSessionState{InitializeParams: &mcp.InitializeParams{ProtocolVersion: "2025-03-26"}, InitializedParams: &mcp.InitializedParams{}}}
	a.upgrade.read = func(path string) (buildidentity.Installed, error) {
		return buildidentity.Installed{Path: path, Build: "next", Contract: upgradeContract()}, nil
	}
	calls := new(int)
	a.upgrade.exec = func(path string, args, env []string) error {
		*calls++
		if args[len(args)-1] != "--mcp-resume" {
			t.Fatal(args)
		}
		return nil
	}
	return a, calls
}
func TestUpgradeExecGating(t *testing.T) {
	isolateAdapterTest(t)
	if !upgradeSupported {
		t.Skip("exec unsupported")
	}
	for _, kind := range []string{"request", "partial frame", "write", "ack", "attempt", "permission", "idle"} {
		t.Run(kind, func(t *testing.T) {
			a, calls := upgradeReadyAdapter(t)
			switch kind {
			case "request":
				a.upgrade.wire.calls = map[jsonrpc.ID]bool{mustID(t, float64(1)): true}
			case "partial frame":
				a.upgrade.wire.buffer = []byte(`{"id":`)
			case "write":
				a.upgrade.wire.writes = 1
			case "ack":
				a.liveActive = 1
			case "attempt":
				a.deliveryObservers = map[string]*deliveryObserver{"token": {}}
			case "permission":
				a.permWithoutWatcher = map[string]struct{}{"request": {}}
			}
			reason := a.tryUpgrade(&ipc.UpgradeHint{Path: "adapter", Build: "next"})
			if kind == "idle" {
				if reason != "" || *calls != 1 {
					t.Fatalf("%s calls=%d", reason, *calls)
				}
			} else if reason == "" || *calls != 0 {
				t.Fatalf("mid-work exec: %s calls=%d", reason, *calls)
			}
		})
	}
}
func TestUpgradeTransportRetainsFramesUntilResponse(t *testing.T) {
	isolateAdapterTest(t)
	wire := &upgradeTransport{output: io.Discard}
	input := []byte("{\"jsonrpc\":\"2.0\",\"id\":41,\"method\":\"ping\"}\n{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"ping\"}\n")
	wire.readAvailable = func(p []byte) (int, error) { n := copy(p, input); input = input[n:]; return n, nil }
	ctx := context.Background()
	first, err := wire.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !wire.busy() {
		t.Fatal("consumed frames untracked")
	}
	if err = wire.Write(ctx, &jsonrpc.Response{ID: first.(*jsonrpc.Request).ID, Result: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if !wire.busy() {
		t.Fatal("prefetched second frame lost")
	}
	second, err := wire.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = wire.Write(ctx, &jsonrpc.Response{ID: second.(*jsonrpc.Request).ID, Result: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if wire.busy() {
		t.Fatal("idle wire blocked")
	}
}

func TestUpgradeRetriesExhausted(t *testing.T) {
	isolateAdapterTest(t)
	a, _ := upgradeReadyAdapter(t)
	a.liveActive = 1
	hint := &ipc.UpgradeHint{Path: "adapter", Build: "next"}
	for i := 0; i < upgradeMaxTries; i++ {
		a.acceptUpgrade(hint)
		a.pollUpgrade(time.Now().Add(upgradeDrainTimeout + time.Second))
		if a.upgrade.quiescing.Load() {
			t.Fatal("postponed adapter kept refusing pushes")
		}
	}
	if !a.upgrade.disabled.Load() {
		t.Fatal("retries did not arm fallback hello")
	}
}
func TestUpgradeResumeRestoresStateAndIdleReadiness(t *testing.T) {
	isolateAdapterTest(t)
	if !upgradeSupported {
		t.Skip("exec unsupported")
	}
	a, _ := upgradeReadyAdapter(t)
	a.receiptDiagnostics = receiptDiagnostics{HostVersion: "2.1.266", FirstRecord: "queue-operation/enqueue", Consecutive: 3, Drift: true}
	route := ipc.RouteRef{Channel: "telegram", ChatID: 17, Name: "project"}
	a.setRouteState([]ipc.RouteRef{route}, &route)
	var transferred string
	a.upgrade.exec = func(_ string, _ []string, env []string) error {
		for _, entry := range env {
			if value, ok := strings.CutPrefix(entry, upgradeResumeEnv+"="); ok {
				transferred = value
			}
		}
		return nil
	}
	if reason := a.tryUpgrade(&ipc.UpgradeHint{Path: "adapter", Build: "next"}); reason != "" {
		t.Fatal(reason)
	}
	t.Setenv(upgradeResumeEnv, transferred)
	next := newAdapter()
	opts, err := next.restoreUpgrade([]string{"--mcp-resume"})
	if err != nil || opts.State.InitializeParams.ProtocolVersion != "2025-03-26" || !next.dispatched.Load() || !next.upgrade.resumed || next.outputRoute.Name != "project" {
		t.Fatalf("resume: %+v %v", opts, err)
	}
	if next.receiptDiagnostics != a.receiptDiagnostics {
		t.Fatal("self-exec lost receipt diagnostics")
	}
	if next.buildInstructions() == "" {
		t.Fatal("unexpected empty normal instructions")
	}
}
func TestUpgradeNoticeWaitsForReadyAndRelaysVerbatim(t *testing.T) {
	isolateAdapterTest(t)
	a, out, _ := liveFixture(t, ipc.RenderCapable)
	const notice = "C3 was updated to next. This session still runs the previous adapter: run /mcp and reconnect c3 (or restart the session) to switch."
	a.upgrade.notice = notice
	a.flushUpgradeNotice()
	if len(out.Bytes()) != 0 {
		t.Fatal("notice sent before initialization")
	}
	a.deliveryHostInitialized.Store(true)
	a.flushUpgradeNotice()
	a.flushUpgradeNotice()
	var frame struct{ Params struct{ Content string } }
	if json.Unmarshal(out.Bytes(), &frame) != nil || frame.Params.Content != notice {
		t.Fatal(string(out.Bytes()))
	}
	if !strings.Contains(a.buildInstructions(), "verbatim") {
		t.Fatal("preamble lost relay rule")
	}
}

func TestUpgradeRetryDoesNotInterruptRequest(t *testing.T) {
	isolateAdapterTest(t)
	a, _ := upgradeReadyAdapter(t)
	a.upgrade.wire.calls = map[jsonrpc.ID]bool{mustID(t, float64(7)): true}
	if a.withUpgradeRehelloIdle(nil) {
		t.Fatal("retry would cancel in-flight tool call")
	}
	a.upgrade.wire.calls = nil
	a.liveActive = 1
	if a.withUpgradeRehelloIdle(nil) {
		t.Fatal("retry would interrupt ack")
	}
	a.liveActive = 0
	if !a.withUpgradeRehelloIdle(nil) {
		t.Fatal("idle retry blocked")
	}
}

func TestUpgradeContractEpoch(t *testing.T) {
	isolateAdapterTest(t)
	sum := sha256.Sum256([]byte(upgradeContractEpoch + ":" + upgradeToolContract))
	if hex.EncodeToString(sum[:]) != upgradeContract() {
		t.Fatal("update the embedded resume contract when tools or the cached-contract epoch changes; older sessions must reconnect")
	}
}

func TestUpgradeExecFailureRequiresFallback(t *testing.T) {
	isolateAdapterTest(t)
	if !upgradeSupported {
		t.Skip("exec unsupported")
	}
	a, _ := upgradeReadyAdapter(t)
	a.upgrade.exec = func(string, []string, []string) error { return io.ErrUnexpectedEOF }
	if reason := a.tryUpgrade(&ipc.UpgradeHint{Path: "adapter", Build: "next"}); reason == "" || !a.upgrade.disabled.Load() {
		t.Fatal("failed exec must keep the old process and request fallback", reason)
	}
}
func TestUpgradeChangedContractRequiresFallback(t *testing.T) {
	isolateAdapterTest(t)
	if !upgradeSupported {
		t.Skip("exec unsupported")
	}
	a, calls := upgradeReadyAdapter(t)
	a.upgrade.read = func(path string) (buildidentity.Installed, error) {
		return buildidentity.Installed{Path: path, Build: "next", Contract: "incompatible"}, nil
	}
	if reason := a.tryUpgrade(&ipc.UpgradeHint{Path: "adapter", Build: "next"}); reason == "" || *calls != 0 || !a.upgrade.disabled.Load() {
		t.Fatal("contract change must require reconnect", reason)
	}
}

func TestUpgradeCanceledReaderDoesNotWaitForSocket(t *testing.T) {
	isolateAdapterTest(t)
	a, _ := adapterWithConn(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { a.brokerReader(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled reconnect waited for another broker frame")
	}
}
