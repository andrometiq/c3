//go:build linux || darwin

package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/spawn"
)

// exec(2) keeps a spawned broker as this process's child but drops the goroutine
// waiting on it, so the old image must hand the broker's pid to the next one.
func TestUpgradeExecHandsSpawnedBrokerToNextImage(t *testing.T) {
	isolateAdapterTest(t)
	a, _ := upgradeReadyAdapter(t)
	broker := exec.Command("sleep", "30")
	if err := spawn.Detached(broker); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Process.Kill() })
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
	raw, err := base64.StdEncoding.DecodeString(transferred)
	if err != nil {
		t.Fatal(err)
	}
	var state struct{ UnreapedChildren []int }
	if err = json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(state.UnreapedChildren, broker.Process.Pid) {
		t.Fatalf("defect: resume state %v omits spawned broker pid %d; it becomes a zombie of the next image", state.UnreapedChildren, broker.Process.Pid)
	}
}

// Broker recovery can spawn on another goroutine while the upgrade encodes its
// state and execs. A spawn that completes before the exec must be in the
// handed-off list; one that is held off until the exec fails is not owed.
func TestUpgradeExecHandsOffBrokerSpawnedDuringUpgrade(t *testing.T) {
	isolateAdapterTest(t)
	a, _ := upgradeReadyAdapter(t)
	broker := exec.Command("sleep", "30")
	spawned := make(chan error, 1)
	t.Cleanup(func() {
		if <-spawned == nil {
			_ = broker.Process.Kill()
		}
	})
	var transferred string
	isSpawnedBeforeExec := false
	a.upgrade.exec = func(_ string, _ []string, env []string) error {
		for _, entry := range env {
			if value, ok := strings.CutPrefix(entry, upgradeResumeEnv+"="); ok {
				transferred = value
			}
		}
		go func() { spawned <- spawn.Detached(broker) }()
		select {
		case err := <-spawned:
			spawned <- err
			isSpawnedBeforeExec = err == nil
		case <-time.After(200 * time.Millisecond):
		}
		return errors.New("exec refused") // the spawn may now proceed in this image
	}
	_ = a.tryUpgrade(&ipc.UpgradeHint{Path: "adapter", Build: "next"})
	if !isSpawnedBeforeExec {
		return
	}
	raw, err := base64.StdEncoding.DecodeString(transferred)
	if err != nil {
		t.Fatal(err)
	}
	var state struct{ UnreapedChildren []int }
	if err = json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(state.UnreapedChildren, broker.Process.Pid) {
		t.Fatalf("defect: broker pid %d spawned during the upgrade, before exec, is missing from the handed-off %v", broker.Process.Pid, state.UnreapedChildren)
	}
}

// The next image must wait on a broker the previous image spawned, so the
// broker's exit is reaped instead of leaving a zombie parented to the adapter.
func TestUpgradeResumeReapsBrokerSpawnedBeforeExec(t *testing.T) {
	isolateAdapterTest(t)
	a, _ := upgradeReadyAdapter(t)
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
	// A child with no waiter in this image: what exec leaves behind.
	broker := exec.Command("sleep", "30")
	if err := broker.Start(); err != nil {
		t.Fatal(err)
	}
	pid := broker.Process.Pid
	t.Cleanup(func() { _ = broker.Process.Kill(); _ = broker.Wait() })
	raw, err := base64.StdEncoding.DecodeString(transferred)
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]any
	if err = json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	state["UnreapedChildren"] = []int{pid}
	if raw, err = json.Marshal(state); err != nil {
		t.Fatal(err)
	}
	t.Setenv(upgradeResumeEnv, base64.StdEncoding.EncodeToString(raw))
	if _, err = newAdapter().restoreUpgrade([]string{"--mcp-resume"}); err != nil {
		t.Fatal(err)
	}
	if err = syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
			return
		}
	}
	t.Fatalf("defect: broker pid %d spawned before the upgrade exec was never reaped (zombie)", pid)
}
