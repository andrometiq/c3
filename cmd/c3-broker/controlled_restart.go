package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/osutil"
)

const (
	controlledRestartWatchdog        = 90 * time.Second
	controlledRestartExchangeTimeout = 2 * time.Second
	controlledRestartExitTimeout     = 91 * time.Second
)
const controlledRestartWatchdogText = "c3-broker: controlled restart exceeded 90s; forcing exit. Some request results or cancellation notices may not have been delivered. Check the requesting session."
const unsupportedControlledRestart = "The running broker does not support controlled restart. It was left running; restart it manually to activate the installed build. Pending prompts are not protected during that manual restart."

var stopBrokerForControlledRestartFn = stopBrokerForControlledRestart

func requestControlledRestart(c net.Conn, deadline time.Time) error {
	defer c.Close()
	if err := c.SetDeadline(deadline); err != nil {
		return err
	}
	conn := ipc.NewConn(c)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	if err = conn.WriteJSONContext(ctx, ipc.HelloMsg{Op: ipc.OpHello, CLI: "c3-broker-cli", PID: os.Getpid(), CWD: cwd, ProtocolVersion: ipc.ProtocolVersion}); err != nil {
		return err
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		return fmt.Errorf("controlled restart hello: %w", err)
	}
	var hello ipc.HelloAckMsg
	if err = json.Unmarshal(raw, &hello); err != nil {
		return err
	}
	if hello.Op != ipc.OpHelloAck || !ipc.ProtocolStateChangesCompatible(hello.ProtocolVersion) {
		return errors.New("controlled restart: incompatible hello reply")
	}
	if err = conn.WriteJSONContext(ctx, ipc.BrokerRestartReq{Op: ipc.OpBrokerRestart}); err != nil {
		return err
	}
	raw, err = conn.ReadFrame()
	if err != nil {
		return fmt.Errorf("controlled restart reply: %w", err)
	}
	var reply ipc.BrokerRestartReply
	if err = json.Unmarshal(raw, &reply); err != nil {
		return err
	}
	if reply.Op == ipc.OpError {
		return errors.New(unsupportedControlledRestart)
	}
	if reply.Op != ipc.OpBrokerRestartReply {
		return errors.New("controlled restart: malformed reply")
	}
	if !reply.OK || reply.Err != "" {
		return fmt.Errorf("controlled restart refused: %s", reply.Err)
	}
	return nil
}

func stopBrokerForControlledRestart() (bool, string) {
	started := time.Now()
	path, err := broker.SocketPath()
	if err != nil {
		return false, err.Error()
	}
	c, err := net.DialTimeout("unix", path, time.Until(started.Add(controlledRestartExchangeTimeout)))
	if err != nil {
		// Only a missing socket establishes that there is no broker to stop.
		if errors.Is(err, os.ErrNotExist) {
			return false, ""
		}
		return false, fmt.Sprintf("controlled restart connection failed: %v", err)
	}
	pidPath, err := broker.PidFilePath()
	if err != nil {
		c.Close()
		return false, err.Error()
	}
	data, err := os.ReadFile(pidPath)
	if err != nil {
		c.Close()
		return false, err.Error()
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 1 {
		c.Close()
		return false, "controlled restart: malformed broker pid"
	}
	if err = requestControlledRestart(c, started.Add(controlledRestartExchangeTimeout)); err != nil {
		return false, err.Error()
	}
	if !waitBrokerExit(pid, started.Add(controlledRestartExitTimeout), osutil.ProcessSignalable) {
		return false, fmt.Sprintf("broker (pid %d) did not exit after the controlled restart watchdog; successor was not started", pid)
	}
	return true, ""
}

func waitBrokerExit(pid int, deadline time.Time, alive func(int) bool) bool {
	for alive(pid) {
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(min(100*time.Millisecond, time.Until(deadline)))
	}
	return true
}

func shutdownControlledBroker(b *broker.Broker, stop func()) {
	b.DrainRequests()
	stop()
	b.Shutdown()
}

// The timer is armed by intent, independently of startup and event dispatch.
type restartIntent struct {
	mu      sync.Mutex
	timer   *time.Timer
	stopped bool
	events  chan struct{}
}

func newRestartIntent() *restartIntent { return &restartIntent{events: make(chan struct{}, 1)} }
func (r *restartIntent) request(begin func() time.Time, budget time.Duration, expire func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped || r.timer != nil {
		return
	}
	first := begin()
	r.timer = time.AfterFunc(time.Until(first.Add(budget)), expire)
	r.events <- struct{}{}
}
func (r *restartIntent) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = true
	if r.timer != nil {
		r.timer.Stop()
	}
}

// Recheck signals after a competing event wins: select alone randomizes ties.
func nextRestartEvent(signals <-chan os.Signal, event <-chan struct{}, reload func(os.Signal)) os.Signal {
	ready := false
	for {
		var sig os.Signal
		select {
		case sig = <-signals:
		default:
			if ready {
				return nil
			}
			select {
			case sig = <-signals:
			case <-event:
				ready = true
				continue
			}
		}
		if osutil.IsReloadSignal(sig) {
			reload(sig)
			continue
		}
		return sig
	}
}

func awaitControlledShutdown(signals <-chan os.Signal, shutdown func(), abandon func(), reload func(os.Signal)) os.Signal {
	done := make(chan struct{})
	go func() { shutdown(); close(done) }()
	sig := nextRestartEvent(signals, done, reload)
	if sig != nil {
		abandon()
	}
	return sig
}
