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
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/osutil"
)

const controlledRestartWatchdog = 90 * time.Second
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
	c, err := net.DialTimeout("unix", path, time.Until(started.Add(2*time.Second)))
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
	if err = requestControlledRestart(c, started.Add(2*time.Second)); err != nil {
		return false, err.Error()
	}
	if !waitBrokerExit(pid, started.Add(controlledRestartWatchdog+time.Second), osutil.ProcessSignalable) {
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
