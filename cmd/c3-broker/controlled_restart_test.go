package main

import (
	"encoding/json"
	"github.com/Andrometiq/c3/internal/c3types"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/ipc"
)

func TestControlledRestartOldBrokerIsNotSignalled(t *testing.T) {
	for _, reply := range []string{`{"op":"error","err":"unknown op broker_restart"}`, `{"op":"broker_restart_reply","ok":"yes"}`, `{"op":"unrelated","ok":true}`, "timeout"} {
		t.Run(reply, func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()
			go func() {
				c := ipc.NewConn(server)
				if _, err := c.ReadFrame(); err != nil {
					return
				}
				if reply == "timeout" {
					return
				}
				c.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck})
				c.ReadFrame()
				server.Write([]byte(reply + "\n"))
			}()
			started := time.Now()
			err := requestControlledRestart(client, started.Add(30*time.Millisecond))
			if err == nil {
				t.Fatal("old or malformed broker accepted")
			}
			if strings.Contains(reply, "unknown op") && err.Error() != unsupportedControlledRestart {
				t.Fatal(err)
			}
			if time.Since(started) > time.Second {
				t.Fatal("unbounded handshake")
			}
		})
	}
}

func TestControlledRestartEntryPoints(t *testing.T) {
	oldStop, oldStart := stopBrokerForControlledRestartFn, ensureBrokerUpFn
	defer func() { stopBrokerForControlledRestartFn, ensureBrokerUpFn = oldStop, oldStart }()
	for _, config := range []bool{false, true} {
		var calls []string
		stopBrokerForControlledRestartFn = func() (bool, string) { calls = append(calls, "stop"); return true, "" }
		ensureBrokerUpFn = func() { calls = append(calls, "start") }
		if config {
			restartBrokerForNewConfig()
		} else if err := bounceUpgradeBroker(); err != nil {
			t.Fatal(err)
		}
		if strings.Join(calls, ",") != "stop,start" {
			t.Fatal(calls)
		}
	}
	client, server := net.Pipe()
	defer server.Close()
	frames := make(chan ipc.Op, 2)
	go func() {
		c := ipc.NewConn(server)
		raw, _ := c.ReadFrame()
		var h ipc.HelloMsg
		json.Unmarshal(raw, &h)
		frames <- h.Op
		c.WriteJSON(ipc.HelloAckMsg{Op: ipc.OpHelloAck})
		raw, _ = c.ReadFrame()
		var r ipc.BrokerRestartReq
		json.Unmarshal(raw, &r)
		frames <- r.Op
		c.WriteJSON(ipc.BrokerRestartReply{Op: ipc.OpBrokerRestartReply, OK: true})
	}()
	if err := requestControlledRestart(client, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if <-frames != ipc.OpHello || <-frames != ipc.OpBrokerRestart {
		t.Fatal("wrong exchange")
	}
}

func TestUpgradeBounceRefusesSuccessorWhileOldBrokerStillLives(t *testing.T) {
	oldStop, oldStart := stopBrokerForControlledRestartFn, ensureBrokerUpFn
	defer func() { stopBrokerForControlledRestartFn, ensureBrokerUpFn = oldStop, oldStart }()
	stopBrokerForControlledRestartFn = func() (bool, string) { return false, "old broker did not exit" }
	starts := 0
	ensureBrokerUpFn = func() { starts++ }
	if err := bounceUpgradeBroker(); err == nil {
		t.Fatal("timeout hidden")
	}
	restartBrokerForNewConfig()
	if starts != 0 {
		t.Fatal("successor started")
	}
}

func TestControlledRestartWatchdogAndExitWait(t *testing.T) {
	if broker.RestartPromptGrace != 60*time.Second || broker.RestartCancellationNoticeTimeout != 5*time.Second || controlledRestartWatchdog != 90*time.Second || shutdownWatchdog != 15*time.Second {
		t.Fatal("wrong budgets")
	}
	if waitBrokerExit(42, time.Now(), func(int) bool { return true }) {
		t.Fatal("live process reported stopped")
	}
	if !waitBrokerExit(42, time.Now(), func(int) bool { return false }) {
		t.Fatal("exited process not recognized")
	}
	block := make(chan struct{})
	defer close(block)
	if runShutdown(func() { <-block }, time.Millisecond) {
		t.Fatal("watchdog ignored")
	}
}

func TestRunShutdown_WedgedPermissionSettlement(t *testing.T) {
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "C3_") || strings.HasPrefix(key, "CLAUDE_CODE_") {
			t.Setenv(key, "")
		}
	}
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	if broker.RestartPromptGrace >= controlledRestartWatchdog {
		t.Fatal("settlement bound must leave the watchdog as backstop")
	}
	b := newRegistrationBroker(t, registrationMappings(nil))
	block, entered := make(chan struct{}), make(chan struct{})
	ch := &wedgedSettlementChannel{registrationChannel: registrationChannel{name: "telegram"}, block: block, entered: entered, sent: make(chan struct{})}
	if err := b.RegisterChannel(ch); err != nil {
		t.Fatal(err)
	}
	key := broker.MakeRouteKey("telegram", 42, nil)
	cwd := t.TempDir()
	stub := b.Stubs.Register("claude", os.Getpid(), cwd, nil)
	b.Routes.Claim(key, stub)
	stub.AddRoute(key)
	stub.MarkRouteConfirmed(key)
	a, c := net.Pipe()
	defer a.Close()
	handled := make(chan struct{})
	go func() { b.HandleConn(c); close(handled) }()
	conn := ipc.NewConn(a)
	if err := conn.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: os.Getpid(), CWD: cwd, Build: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteJSON(ipc.PermissionReq{Op: ipc.OpPermissionRequest, RequestID: "wedged", ToolName: "Bash", Preview: "echo test"}); err != nil {
		t.Fatal(err)
	}
	<-ch.sent
	if err := conn.WriteJSON(ipc.PermissionSettledMsg{Op: ipc.OpPermissionSettled, RequestID: "wedged", Outcome: "allow"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("settlement did not reach channel edit")
	}
	b.BeginControlledRestart()
	finished := make(chan struct{})
	start := time.Now()
	clean := runShutdown(func() { shutdownControlledBroker(b, func() { c.Close(); <-handled }); close(finished) }, 50*time.Millisecond)
	close(block)
	if clean || time.Since(start) > time.Second {
		t.Fatal("wedged settlement bypassed the watchdog exit decision")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("released shutdown did not finish")
	}
}

type wedgedSettlementChannel struct {
	registrationChannel
	block, entered, sent chan struct{}
}

func (*wedgedSettlementChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{Channel: "telegram", InlineKeyboards: true}
}
func (c *wedgedSettlementChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	id, err := c.registrationChannel.SendReply(args)
	close(c.sent)
	return id, err
}
func (c *wedgedSettlementChannel) EditMessage(c3types.EditArgs) (*c3types.EditResult, error) {
	close(c.entered)
	<-c.block
	return &c3types.EditResult{MessageID: 1}, nil
}

func TestSignalShutdownPreservesMasterPath(t *testing.T) {
	// Pin the distinct dispatch paths, including reload preceding signal teardown.
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	controlled := strings.Index(source, "case <-restartC:")
	signal := strings.Index(source, "case sig = <-sigC:")
	reload := strings.Index(source, "if osutil.IsReloadSignal(sig)")
	immediate := strings.Index(source, "runShutdown(func() { srv.Stop(); br.Shutdown() }, shutdownWatchdog)")
	if controlled < 0 || signal < controlled || reload < signal || immediate < reload {
		t.Fatal("signal and controlled paths changed")
	}
	if strings.Contains(source[signal:immediate], "DrainRequests") || strings.Contains(source[signal:immediate], "BeginControlledRestart") {
		t.Fatal("OS shutdown gained prompt protection")
	}
}
