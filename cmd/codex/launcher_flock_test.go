//go:build !windows

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const (
	holdLauncherLockEnv = "C3_TEST_HOLD_LAUNCHER_LOCK"
	lockReadyFileEnv    = "C3_TEST_LAUNCHER_LOCK_READY_FILE"
	lockReleaseFileEnv  = "C3_TEST_LAUNCHER_LOCK_RELEASE_FILE"
	startSignalFileEnv  = "C3_TEST_APP_SERVER_START_SIGNAL_FILE"
)

// TestChooseAppServerURL_DefaultUsesKernelEphemeralPort protects the default
// path from regressing to one predictable shared port. A fixed default lets
// simultaneous launchers select the same destination before either child binds.
func TestChooseAppServerURL_DefaultUsesKernelEphemeralPort(t *testing.T) {
	got := chooseAppServerURL(defaultWSURL)
	_, port, err := parseWSURL(got)
	if err != nil || port == 0 || got == defaultWSURL {
		t.Fatalf("defect: the default Codex launcher reused predictable shared port %q instead of an OS-assigned ephemeral port; got %q (err=%v)", defaultWSURL, got, err)
	}
}

func TestAppServerLaunchLock_UsesC3RuntimeDirectory(t *testing.T) {
	runtimeDir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeDir)
	lock, err := acquireAppServerLaunchLock()
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	lockPath := filepath.Join(runtimeDir, "c3-codex-launcher.lock")
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("defect: Codex launcher flock was not created in C3's per-user runtime directory (%s); a shared /tmp lock lets users interfere with each other's startup: %v", lockPath, err)
	}
}

// TestStartAppServer_InterprocessFlockCoversChildReadiness proves the lock is
// held by a different PROCESS, not merely by a Go mutex. The holder releases
// only after the assertion, and no app-server child may start before then.
func TestStartAppServer_InterprocessFlockCoversChildReadiness(t *testing.T) {
	runtimeDir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeDir)
	ready := filepath.Join(t.TempDir(), "lock-ready")
	release := filepath.Join(t.TempDir(), "lock-release")
	started := filepath.Join(t.TempDir(), "app-server-started")

	holder := exec.Command(os.Args[0], "-test.run=^$")
	holder.Env = append(os.Environ(),
		holdLauncherLockEnv+"=1",
		lockReadyFileEnv+"="+ready,
		lockReleaseFileEnv+"="+release,
	)
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(release, nil, 0o600)
		_ = holder.Wait()
	})
	waitForFile(t, ready)

	t.Setenv(fakeAppServerEnv, "1")
	t.Setenv(startSignalFileEnv, started)
	fakePIDs := filepath.Join(t.TempDir(), "fake-app-server-pids")
	t.Setenv(fakeAppServerPIDsEnv, fakePIDs)
	cleanupFakeAppServerGroups(t, fakePIDs)
	adapterPath := filepath.Join(t.TempDir(), "adapter")
	cwd := t.TempDir()
	result := make(chan struct {
		url string
		err error
	}, 1)
	go func() {
		url, err := startAppServer(os.Args[0], adapterPath, defaultWSURL, cwd, "topic")
		result <- struct {
			url string
			err error
		}{url, err}
	}()

	time.Sleep(200 * time.Millisecond)
	if _, err := os.Stat(started); err == nil {
		t.Fatal("defect: a second launcher started its app-server while another process held the C3 runtime flock; choose-port → start-child → readiness is not serialized")
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("launcher did not start after the competing process released its flock: %v", got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("launcher remained blocked after the competing process released its flock")
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for test launcher-lock holder signal %s", path)
}
