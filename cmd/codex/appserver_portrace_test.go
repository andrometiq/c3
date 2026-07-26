package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
}

// fakeAppServerEnv turns a re-executed copy of this test binary into a stand-in
// Codex app-server: it binds the --listen port it was given and stays up.
const (
	fakeAppServerEnv  = "C3_TEST_FAKE_APP_SERVER"
	fakePoisonPortEnv = "C3_TEST_FAKE_APP_SERVER_POISON_PORT"
	fakeSignalDirEnv  = "C3_TEST_FAKE_APP_SERVER_SIGNAL_DIR"
)

// init runs before the test framework parses flags, so a child process started
// with fakeAppServerEnv set never runs the suite — it becomes the app-server.
// Asked for the poisoned port it loses the bind race on cue (announce, wait to
// be overtaken, exit) instead of binding; on any other port it binds and holds,
// which is what a real app-server does.
func init() {
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
