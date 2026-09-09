package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Andrometiq/c3/internal/buildidentity"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Change this compatibility epoch whenever cached capabilities, instructions,
// schemas OR semantics require a real reconnect. The tool-contract test pins the
// corresponding SDK tools/list result; changing that test alone is insufficient.
const upgradeContractEpoch = "1"
const upgradeToolContract = "5b06d4fe9d3a3f8ae401a0c14793a1df5eaf54e88a1136509a936aa009370ff5"
const upgradeContractMarker = "C3_MCP_CONTRACT_V1:4bd931b168e0f26397b8634893ee56cd2d1638a8854a0aec945c47bb8d799cb3:"

func upgradeContract() string {
	return strings.TrimSuffix(strings.TrimPrefix(upgradeContractMarker, "C3_MCP_CONTRACT_V1:"), ":")
}

const upgradeResumeEnv = "C3_MCP_RESUME_STATE"
const upgradeDrainTimeout = 30 * time.Second
const upgradeMaxTries = 3

type adapterUpgrade struct {
	reconnecting   atomic.Bool
	reported       bool
	waitingHello   bool
	noticeMu       sync.Mutex
	notice         string
	mu             sync.Mutex
	wire           *upgradeTransport // assigned before hello/observers
	hint           *ipc.UpgradeHint
	deadline       time.Time
	retryAt        time.Time
	tries          int
	disabled       atomic.Bool
	quiescing      atomic.Bool
	resumed        bool
	deliveryWrites atomic.Int64
	exec           func(string, []string, []string) error // tests never replace a process
	read           func(string) (buildidentity.Installed, error)
}
type upgradeResumeState struct {
	Contract       string
	MCP            mcp.ServerSessionState
	Routes         []ipc.RouteRef
	Output         *ipc.RouteRef
	Attach         *ipc.AttachReq
	AttachStableID string
	StableID       string
	Handoff        sessionhandoff.Entry
	Guidance       string
}

func (a *adapter) restoreUpgrade(args []string) (*mcp.ServerSessionOptions, error) {
	resume := false
	for _, arg := range args {
		if arg == "--mcp-resume" {
			resume = true
		}
	}
	if !resume {
		return nil, nil
	}
	encoded := os.Getenv(upgradeResumeEnv)
	_ = os.Unsetenv(upgradeResumeEnv)
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	var state upgradeResumeState
	if err = json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("invalid MCP resume state: %w", err)
	}
	if state.Contract != upgradeContract() || state.MCP.InitializeParams == nil || state.MCP.InitializedParams == nil {
		return nil, fmt.Errorf("incompatible MCP resume state")
	}
	a.setRouteState(state.Routes, state.Output)
	a.lastAttach, a.lastAttachStableID = state.Attach, state.AttachStableID
	a.currentStableID, a.currentHandoffEntry = state.StableID, state.Handoff
	a.initGuidanceChannel = state.Guidance
	a.upgrade.resumed = true
	a.dispatched.Store(true)
	if a.upgrade.wire != nil {
		a.upgrade.wire.state = state.MCP
	}
	log.Printf("adapter resumed after upgrade (build %s)", buildidentity.Current())
	return &mcp.ServerSessionOptions{State: &state.MCP}, nil
}
func (a *adapter) acceptUpgrade(hint *ipc.UpgradeHint) {
	if hint == nil || hint.Build == buildidentity.Current() || a.upgrade.disabled.Load() {
		a.upgrade.mu.Lock()
		a.upgrade.hint = nil
		a.upgrade.quiescing.Store(false)
		a.upgrade.mu.Unlock()
		return
	}
	u := &a.upgrade
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.hint == nil || u.hint.Build != hint.Build {
		u.tries = 0
	}
	u.reported = false
	u.waitingHello = false
	u.hint = hint
	u.deadline = time.Now().Add(upgradeDrainTimeout)
	u.retryAt = time.Time{}
	u.quiescing.Store(true)
}
func (a *adapter) pollUpgrade(now time.Time) {
	u := &a.upgrade
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.hint == nil || u.waitingHello {
		return
	}
	if !u.retryAt.IsZero() {
		if !now.Before(u.retryAt) {
			a.withUpgradeRehelloIdle(func() {
				if c := a.currentConn(); c != nil {
					u.waitingHello = true
					u.quiescing.Store(true)
					// Keep unread host frames outside the SDK until recovery has
					// canceled old broker calls and installed the new connection.
					if u.wire != nil {
						u.wire.resumeReads = make(chan struct{})
					}
					_ = c.Close()
				}
			})
		}
		return
	}
	if !u.reported {
		if c := a.currentConn(); c != nil {
			if a.deliveryAccepted.Load() {
				writeLiveFrame(c, ipc.DeliveryReportMsg{Op: ipc.OpDeliveryReport, Live: a.deliveryFacts()})
			} else {
				a.liveMu.Lock()
				a.renderRoute = ipc.RenderRoute{State: ipc.RenderQueueOnly, Reason: "adapter upgrade pending"}
				route := a.renderRoute
				a.liveMu.Unlock()
				a.publishLiveRoute(c, route)
			}
			u.reported = true
		}
	}
	reason := a.tryUpgrade(u.hint)
	if reason == "" {
		u.hint = nil
		return
	} // fake exec success in tests
	if now.Before(u.deadline) && !u.disabled.Load() {
		return
	}
	log.Printf("upgrade postponed: %s", reason)
	u.tries++
	u.quiescing.Store(false)
	if u.tries >= upgradeMaxTries || u.disabled.Load() {
		u.disabled.Store(true)
		u.retryAt = now
		return
	}
	// Retry only through another hello, after allowing existing work to settle.
	u.retryAt = now.Add(deliveryRehelloInterval)
}
func (a *adapter) tryUpgrade(hint *ipc.UpgradeHint) string {
	u := &a.upgrade
	if u.reconnecting.Load() {
		return "broker recovery in flight"
	}
	if !upgradeSupported || u.wire == nil {
		return "self-exec unsupported"
	}
	if !a.recoverMu.TryLock() {
		return "session recovery in flight"
	}
	defer a.recoverMu.Unlock()
	a.liveMu.Lock()
	defer a.liveMu.Unlock()
	if a.liveActive != 0 || len(a.livePending) != 0 {
		return "legacy push awaiting ack"
	}
	if u.deliveryWrites.Load() != 0 {
		return "delivery write in flight"
	}
	if len(a.deliveryObservers) != 0 {
		return "delivery attempt open"
	}
	a.permMu.Lock()
	defer a.permMu.Unlock()
	if len(a.permPending) != 0 || len(a.permWithoutWatcher) != 0 {
		return "permission relay in flight"
	}
	u.wire.mu.Lock()
	defer u.wire.mu.Unlock()
	if u.wire.busy() {
		return "MCP request, partial frame or stdout write in flight"
	}
	if u.wire.state.InitializeParams == nil || u.wire.state.InitializedParams == nil {
		return "host initialization pending"
	}
	// Validate only once idle, with input admission frozen. Exec is always
	// against this path; a concurrent installer replacing it remains a limitation.
	read := u.read
	if read == nil {
		read = buildidentity.Read
	}
	installed, err := read(hint.Path)
	if err != nil || installed.Build != hint.Build || installed.Contract != upgradeContract() {
		u.disabled.Store(true)
		return "installed binary or MCP contract changed"
	}
	a.amu.Lock()
	state := upgradeResumeState{Contract: upgradeContract(), MCP: u.wire.state, Routes: a.routes, Output: a.outputRoute, Attach: a.lastAttach, AttachStableID: a.lastAttachStableID, Guidance: a.initGuidanceChannel}
	a.amu.Unlock()
	a.idmu.Lock()
	state.StableID, state.Handoff = a.currentStableID, a.currentHandoffEntry
	a.idmu.Unlock()
	raw, err := json.Marshal(state)
	if err != nil {
		return "resume state encoding failed"
	}
	env := []string{}
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, upgradeResumeEnv+"=") {
			env = append(env, value)
		}
	}
	env = append(env, upgradeResumeEnv+"="+base64.StdEncoding.EncodeToString(raw))
	args := []string{hint.Path}
	for _, arg := range os.Args[1:] {
		if arg != "--mcp-resume" {
			args = append(args, arg)
		}
	}
	args = append(args, "--mcp-resume")
	exec := u.exec
	if exec == nil {
		exec = upgradeExec
	}
	log.Printf("adapter upgrade: exec %s (build %s)", hint.Path, hint.Build)
	if err := exec(hint.Path, args, env); err != nil {
		u.disabled.Store(true)
		return "exec failed: " + err.Error()
	}
	return ""
}
func (a *adapter) upgradeNotificationDone(method string) {
	if a.upgrade.wire != nil && method == "notifications/initialized" {
		a.upgrade.wire.notificationDone()
	}
}
func (a *adapter) runMCP(ctx context.Context, server *mcp.Server, opts *mcp.ServerSessionOptions) error {
	session, err := server.Connect(ctx, a.notifyTx, opts)
	if err != nil {
		return err
	}
	if opts != nil {
		a.deliveryHostInitialized.Store(true)
	}
	stop := context.AfterFunc(ctx, func() { _ = session.Close() })
	defer stop()
	defer session.Close()
	return session.Wait()
}

// Upgrade notices use the existing system-event path, but wait for MCP readiness
// if the hello preceded initialization. No timer or additional goroutine needed.
func (a *adapter) holdUpgradeNotice(in ipc.InboundMsg) bool {
	if in.Inbound.Event == nil || in.Inbound.Event.System == nil {
		return false
	}
	event := in.Inbound.Event.System
	if event.Source != "c3-broker" || event.Title != "C3 adapter update" {
		return false
	}
	a.upgrade.noticeMu.Lock()
	a.upgrade.notice = event.Message
	a.upgrade.noticeMu.Unlock()
	return true
}
func (a *adapter) flushUpgradeNotice() {
	if !a.deliveryHostInitialized.Load() || a.notifyTx == nil {
		return
	}
	a.upgrade.noticeMu.Lock()
	defer a.upgrade.noticeMu.Unlock()
	if a.upgrade.notice == "" {
		return
	}
	if a.notifyTx.Notify(context.Background(), "notifications/claude/channel", map[string]any{"content": a.upgrade.notice, "meta": map[string]any{}}) == nil {
		a.upgrade.notice = ""
	}
}

// The retry itself must not cancel a broker call or discard an observer. Socket
// replacement waits for the same work as exec; a later natural hello also retries.
func (a *adapter) withUpgradeRehelloIdle(action func()) bool {
	if a.upgrade.reconnecting.Load() {
		return false
	}
	if !a.recoverMu.TryLock() {
		return false
	}
	defer a.recoverMu.Unlock()
	a.liveMu.Lock()
	defer a.liveMu.Unlock()
	if a.liveActive != 0 || len(a.livePending) != 0 || len(a.deliveryObservers) != 0 || a.upgrade.deliveryWrites.Load() != 0 {
		return false
	}
	a.permMu.Lock()
	defer a.permMu.Unlock()
	if len(a.permPending) != 0 || len(a.permWithoutWatcher) != 0 {
		return false
	}
	if wire := a.upgrade.wire; wire != nil {
		wire.mu.Lock()
		defer wire.mu.Unlock()
		if wire.busy() {
			return false
		}
	}
	if action != nil {
		action() // Admission locks remain held through the socket close.
	}
	return true
}

func (a *adapter) finishUpgradeRecovery() {
	a.upgrade.reconnecting.Store(false)
	if wire := a.upgrade.wire; wire != nil {
		wire.mu.Lock()
		defer wire.mu.Unlock()
		if wire.resumeReads != nil {
			close(wire.resumeReads)
			wire.resumeReads = nil
		}
	}
}

// The saved SDK state already includes notifications/initialized. A host may
// repeat it (with or without initialize) across the handoff; acknowledge it
// locally instead of asking the SDK to initialize an already initialized session.
func (a *adapter) resumedInitialized(method string) bool {
	return a.upgrade.resumed && method == "notifications/initialized"
}
