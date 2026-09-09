package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/buildidentity"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestUpgradeToolContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a := newAdapter()
	srv := a.buildMCPServer()
	ct, st := mcp.NewInMemoryTransports()
	ss, err := srv.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	cs, err := newTestClient().Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
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
}
func TestResumeSDKCallWithoutInitialize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a := newAdapter()
	a.upgrade.resumed = true
	// This raw client sends tools/call as its FIRST frame. The real SDK must
	// dispatch the tool, rather than reject it as invalid during initialization.
	ct, st := mcp.NewInMemoryTransports()
	ss, err := a.buildMCPServer().Connect(ctx, st, &mcp.ServerSessionOptions{State: &mcp.ServerSessionState{InitializeParams: &mcp.InitializeParams{ProtocolVersion: "2025-03-26"}, InitializedParams: &mcp.InitializedParams{}}})
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	conn, err := ct.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	id := mustID(t, "retained-host-id-41")
	if err = conn.Write(ctx, &jsonrpc.Request{ID: id, Method: "tools/call", Params: json.RawMessage(`{"name":"fetch_queue","arguments":{}}`)}); err != nil {
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
	if !upgradeSupported {
		t.Skip("exec unsupported")
	}
	a, _ := upgradeReadyAdapter(t)
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
	if next.buildInstructions() == "" {
		t.Fatal("unexpected empty normal instructions")
	}
}
func TestUpgradeNoticeWaitsForReadyAndRelaysVerbatim(t *testing.T) {
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
	a, _ := upgradeReadyAdapter(t)
	a.upgrade.wire.calls = map[jsonrpc.ID]bool{mustID(t, float64(7)): true}
	if a.upgradeRehelloIdle() {
		t.Fatal("retry would cancel in-flight tool call")
	}
	a.upgrade.wire.calls = nil
	a.liveActive = 1
	if a.upgradeRehelloIdle() {
		t.Fatal("retry would interrupt ack")
	}
	a.liveActive = 0
	if !a.upgradeRehelloIdle() {
		t.Fatal("idle retry blocked")
	}
}

func TestUpgradeContractEpoch(t *testing.T) {
	sum := sha256.Sum256([]byte(upgradeContractEpoch + ":" + upgradeToolContract))
	if hex.EncodeToString(sum[:]) != upgradeContract() {
		t.Fatal("update the embedded resume contract when tools or the cached-contract epoch changes; older sessions must reconnect")
	}
}

func TestUpgradeExecFailureRequiresFallback(t *testing.T) {
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
