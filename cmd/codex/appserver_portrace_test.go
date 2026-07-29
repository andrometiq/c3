package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// freePort (appserver_identity_test.go) returns a port that was free a moment
// ago — a probe, not a reservation. That is exactly the condition under test, so
// these tests reproduce it rather than engineering around it.

// TestEnsureAppServer_LosingThePortRaceIsNotAdoption pins the startup half of
// the never-adopt rule.
//
// The defect it guards: chooseAppServerURL picks a port by PROBING it, which
// reserves nothing. Between that probe and our child binding, another launcher
// can probe the same port and win the bind. The winner's listener is reachable
// from here, so a loop that accepts "the port is reachable" as "my child is up"
// hands this session the WINNER's app-server — two Codex sessions on one server,
// which is exactly the cross-talk incident never-adopt exists to prevent. It
// would arrive through the startup window instead of the front door, and every
// surface would report success.
//
// The scenario is staged deterministically rather than by timing: the fake codex
// signals that it has started, waits for the test to occupy the port on its
// behalf (standing in for the launcher that won the race), and only then exits —
// which is what a child that lost a bind race actually does.
func TestEnsureAppServer_LosingThePortRaceIsNotAdoption(t *testing.T) {
	dir := t.TempDir()
	started := filepath.Join(dir, "started")
	release := filepath.Join(dir, "release")

	fake := filepath.Join(dir, "codex")
	script := fmt.Sprintf("#!/bin/sh\ntouch %q\nwhile [ ! -f %q ]; do sleep 0.02; done\nexit 1\n", started, release)
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	port := freePort(t)
	wsURL := fmt.Sprintf("ws://127.0.0.1:%d", port)

	// The rival launcher: occupies the port once our child is running, so that
	// when our child dies the port is live and owned by somebody else.
	go func() {
		for i := 0; i < 500; i++ {
			if _, err := os.Stat(started); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return // port taken by something else; the assertions below will say so
		}
		defer ln.Close()
		_ = os.WriteFile(release, nil, 0o600)
		time.Sleep(10 * time.Second) // hold it for the duration of the call
	}()

	err := ensureAppServer(fake, filepath.Join(dir, "adapter"), wsURL, dir, "some-topic")
	if err == nil {
		t.Fatalf("ensureAppServer ADOPTED an app-server it did not start: our child exited without "+
			"binding %s, another process holds that port, and this session was handed it anyway — "+
			"two Codex sessions now share one app-server and each will see the other's messages", wsURL)
	}
	if !isLostPortRace(err) {
		t.Fatalf("losing the port race was not reported as retryable, so the caller cannot move to "+
			"another port and the session dies instead of recovering; err=%v", err)
	}
	if !strings.Contains(err.Error(), wsURL) {
		t.Errorf("the error does not name the contended URL, which is the one thing an operator "+
			"needs to act on; err=%v", err)
	}
}

// TestEnsureAppServer_ChildDeathWithNoListenerIsNotARace is the control: a child
// that dies with the port still free is a genuine startup failure (bad binary,
// missing feature flag). Reporting THAT as a lost race would send the caller
// hopping ports forever, retrying a launch that can never succeed.
func TestEnsureAppServer_ChildDeathWithNoListenerIsNotARace(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "codex")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	port := freePort(t)
	wsURL := fmt.Sprintf("ws://127.0.0.1:%d", port)

	err := ensureAppServer(fake, filepath.Join(dir, "adapter"), wsURL, dir, "some-topic")
	if err == nil {
		t.Fatal("a child that exited without ever listening was reported as a successful start")
	}
	if isLostPortRace(err) {
		t.Fatalf("a plain startup failure was misreported as a lost port race, so the caller will "+
			"retry the same broken launch on port after port instead of surfacing it; err=%v", err)
	}
	if !strings.Contains(err.Error(), appServerLogPath(port)) {
		t.Errorf("the failure does not point at the app-server log, which is the only place the "+
			"real reason exists; err=%v", err)
	}
}

func TestEnsureAppServer_ChildExitReapsItsHelperProcessGroup(t *testing.T) {
	dir := t.TempDir()
	helperPIDFile := filepath.Join(dir, "helper-pid")
	fake := filepath.Join(dir, "codex")
	script := fmt.Sprintf("#!/bin/sh\nsleep 30 &\necho $! > %q\nexit 1\n", helperPIDFile)
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	port := freePort(t)
	err := ensureAppServer(fake, filepath.Join(dir, "adapter"),
		fmt.Sprintf("ws://127.0.0.1:%d", port), dir, "some-topic")
	if err == nil {
		t.Fatal("child exit unexpectedly reported success")
	}
	data, err := os.ReadFile(helperPIDFile)
	if err != nil {
		t.Fatal(err)
	}
	helperPID, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for syscall.Kill(helperPID, 0) == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := syscall.Kill(helperPID, 0); err != syscall.ESRCH {
		t.Fatalf("defect: app-server parent exited but helper pid %d survived its process-group cleanup: %v", helperPID, err)
	}
}

// TestStartAppServer_MovesToAnotherPortAfterLosingOne pins the recovery: losing a
// race must cost a port, not the session. Without the retry the launcher reports
// a hard failure for a condition that resolves by simply trying the next port.
func TestStartAppServer_MovesToAnotherPortAfterLosingOne(t *testing.T) {
	dir := t.TempDir()
	// The stand-in app-server is THIS test binary re-executed (see the init()
	// below), not a shell script calling nc: nc is not installed everywhere, and
	// a test that skips protects nothing. Re-exec keeps the fake self-contained.
	//
	// The FIRST port must be free when startAppServer probes it, or
	// chooseAppServerURL simply walks past it and the retry path — the thing
	// under test — never runs. So the port is poisoned instead: the child asked
	// to bind it loses the race on cue, exactly as a real losing launcher does.
	port := freePort(t)
	started := filepath.Join(dir, "started")
	release := filepath.Join(dir, "release")
	t.Setenv(fakeAppServerEnv, "1")
	t.Setenv(fakePoisonPortEnv, strconv.Itoa(port))
	t.Setenv(fakeSignalDirEnv, dir)
	fakePIDs := filepath.Join(dir, "fake-app-server-pids")
	t.Setenv(fakeAppServerPIDsEnv, fakePIDs)
	cleanupFakeAppServerGroups(t, fakePIDs)

	// The rival launcher, which wins the contended port while our first child is
	// still up — so when that child dies the port is live and owned by someone else.
	go func() {
		for i := 0; i < 500; i++ {
			if _, err := os.Stat(started); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return
		}
		defer ln.Close()
		_ = os.WriteFile(release, nil, 0o600)
		time.Sleep(20 * time.Second)
	}()

	got, err := startAppServer(os.Args[0], filepath.Join(dir, "adapter"),
		fmt.Sprintf("ws://127.0.0.1:%d", port), dir, "some-topic")
	if err != nil {
		t.Fatalf("startAppServer gave up after losing ONE port race — losing a race should cost a "+
			"port, not the session: %v", err)
	}
	if strings.HasSuffix(got, fmt.Sprintf(":%d", port)) {
		t.Fatalf("startAppServer returned the CONTENDED url %s — it adopted the process that won "+
			"that port instead of starting its own app-server elsewhere", got)
	}
	pid := appServerPID(t, got)
	if pgid, err := syscall.Getpgid(pid); err != nil || pgid != pid {
		t.Fatalf("defect: app-server pid %d is not isolated in its own process group (pgid=%d, err=%v); cleanup would kill only its parent and leak helpers", pid, pgid, err)
	}
}

func TestRun_ReapsAppServerWhenTUIExits(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "fake-app-server-pids")
	t.Setenv("C3_CODEX_REAL", os.Args[0])
	t.Setenv("C3_CODEX_ADAPTER", filepath.Join(dir, "adapter"))
	t.Setenv(fakeAppServerEnv, "1")
	t.Setenv(fakeAppServerPIDsEnv, pidFile)
	t.Setenv("XDG_RUNTIME_DIR", dir)

	// The app-server child sees --listen and binds. The TUI child is the same
	// re-executed test binary without --listen, so the fake init exits
	// immediately. run must then reap the app-server process group.
	_ = run(nil, filepath.Join(dir, "launcher"))
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(data))
	if len(fields) != 1 {
		t.Fatalf("setup: fake app-server pids = %q, want one", data)
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = killAppServerProcessGroup(pid) })
	deadline := time.Now().Add(2 * time.Second)
	for syscall.Kill(pid, 0) == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := syscall.Kill(pid, 0); err != syscall.ESRCH {
		t.Fatalf("defect: run returned but its per-launch app-server pid %d survived: %v", pid, err)
	}
}

// fakeAppServerEnv turns a re-executed copy of this test binary into a stand-in
// Codex app-server: it binds the --listen port it was given and stays up.
const (
	fakeAppServerEnv     = "C3_TEST_FAKE_APP_SERVER"
	fakePoisonPortEnv    = "C3_TEST_FAKE_APP_SERVER_POISON_PORT"
	fakeSignalDirEnv     = "C3_TEST_FAKE_APP_SERVER_SIGNAL_DIR"
	fakeAppServerPIDsEnv = "C3_TEST_FAKE_APP_SERVER_PIDS"
)

// init runs before the test framework parses flags, so a child process started
// with fakeAppServerEnv set never runs the suite — it becomes the app-server.
// Asked for the poisoned port it loses the bind race on cue (announce, wait to
// be overtaken, exit) instead of binding; on any other port it binds and holds,
// which is what a real app-server does.
func init() {
	if os.Getenv(holdLauncherLockEnv) != "" {
		lock, err := acquireAppServerLaunchLock()
		if err != nil {
			os.Exit(2)
		}
		defer lock.Release()
		_ = os.WriteFile(os.Getenv(lockReadyFileEnv), nil, 0o600)
		for i := 0; i < 1000; i++ {
			if _, err := os.Stat(os.Getenv(lockReleaseFileEnv)); err == nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		os.Exit(2)
	}
	if os.Getenv(fakeAppServerEnv) == "" {
		return
	}
	addr := ""
	for i, a := range os.Args {
		if a == "--listen" && i+1 < len(os.Args) {
			addr = strings.TrimPrefix(os.Args[i+1], "ws://")
		}
	}
	if addr == "" {
		os.Exit(2)
	}
	if started := os.Getenv(startSignalFileEnv); started != "" {
		_ = os.WriteFile(started, nil, 0o600)
	}
	if pids := os.Getenv(fakeAppServerPIDsEnv); pids != "" {
		if file, err := os.OpenFile(pids, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600); err == nil {
			_, _ = fmt.Fprintf(file, "%d\n", os.Getpid())
			_ = file.Close()
		}
	}
	if poison := os.Getenv(fakePoisonPortEnv); poison != "" && strings.HasSuffix(addr, ":"+poison) {
		dir := os.Getenv(fakeSignalDirEnv)
		_ = os.WriteFile(filepath.Join(dir, "started"), nil, 0o600)
		for i := 0; i < 1000; i++ {
			if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		os.Exit(1) // lost the bind — exactly what a losing launcher's child does
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		os.Exit(1)
	}
	defer ln.Close()
	select {} // hold the port until the parent kills us
}

func isLostPortRace(err error) bool {
	return err != nil && strings.Contains(err.Error(), errAppServerLostPortRace.Error())
}

func cleanupFakeAppServerGroups(t *testing.T, pidFile string) {
	t.Helper()
	t.Cleanup(func() {
		data, err := os.ReadFile(pidFile)
		if err != nil && !os.IsNotExist(err) {
			t.Errorf("read fake app-server PID registry: %v", err)
			return
		}
		for _, line := range strings.Fields(string(data)) {
			pid, err := strconv.Atoi(line)
			if err != nil || pid <= 0 {
				t.Errorf("invalid fake app-server PID %q in cleanup registry", line)
				continue
			}
			pgid, err := syscall.Getpgid(pid)
			if err == syscall.ESRCH {
				continue
			}
			if err != nil {
				t.Errorf("read fake app-server process group for pid %d: %v", pid, err)
				continue
			}
			if pgid != pid {
				t.Errorf("defect: fake app-server pid %d shares process group %d; group cleanup would kill the test runner instead of leaked helpers", pid, pgid)
				_ = syscall.Kill(pid, syscall.SIGKILL)
				continue
			}
			if err := killAppServerProcessGroup(pid); err != nil {
				t.Errorf("kill fake app-server process group %d: %v", pid, err)
				continue
			}
			waitForProcessGroupExit(t, pid)
		}
	})
}

func appServerPID(t *testing.T, wsURL string) int {
	t.Helper()
	_, port, err := parseWSURL(wsURL)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(appServerMetaPath(port))
	if err != nil {
		t.Fatal(err)
	}
	var meta struct {
		PID int `json:"pid"`
	}
	if err := json.Unmarshal(data, &meta); err != nil || meta.PID <= 0 {
		t.Fatalf("invalid app-server metadata for %s: pid=%d err=%v", wsURL, meta.PID, err)
	}
	return meta.PID
}

func waitForProcessGroupExit(t *testing.T, pgid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(-pgid, 0)
		if err == syscall.ESRCH {
			return
		}
		if err != nil {
			t.Errorf("probe fake app-server process group %d after cleanup: %v", pgid, err)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("defect: fake app-server process group %d survived test cleanup; it will become an orphan", pgid)
}
