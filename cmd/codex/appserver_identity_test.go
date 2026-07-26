//go:build !windows

package main

import (
	"encoding/json"
	"net"
	"os"
	"strconv"
	"testing"
)

// These tests exist because of a live incident on 2026-07-26: two Codex sessions,
// attached to two different Telegram topics, both received every message. The
// broker had routed each message to the correct adapter — the adapters then
// forwarded them into the SAME app-server, because the second launcher adopted
// the first's.
//
// The class of bug, stated so the tests can guard the class and not the instance:
//
//	TWO SESSIONS MUST NEVER SHARE A DELIVERY DESTINATION UNLESS EACH CAN PROVE
//	IT IS THE SAME SESSION.
//
// "Prove" is the load-bearing word. The old code compared (cwd, topic, adapter)
// field by field, and inferTopicName deliberately returns "" for a launch from
// the shared root — so two unknown identities compared equal and were treated as
// one session. Failing to separate costs a process; failing to share correctly
// delivers one person's messages to another. The tests below pin that asymmetry.

// freePort returns a port with nothing listening on it, and a port that IS
// listening along with a closer.
func listeningPort(t *testing.T) (port int, close func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, portText, _ := net.SplitHostPort(ln.Addr().String())
	p, _ := strconv.Atoi(portText)
	return p, func() { _ = ln.Close() }
}

func freePort(t *testing.T) int {
	t.Helper()
	p, closer := listeningPort(t)
	closer() // release it immediately; the port is now free
	return p
}

// seedMeta writes an app-server record for port as if a launcher had started one,
// and removes it when the test ends. pid defaults to this test process, which is
// alive — the liveness check is exercised explicitly in its own test.
func seedMeta(t *testing.T, port int, cwd, topic, adapter string, pid int) string {
	t.Helper()
	wsURL := "ws://127.0.0.1:" + strconv.Itoa(port)
	data, _ := json.MarshalIndent(map[string]any{
		"ws_url": wsURL,
		"pid":    pid,
		"signature": map[string]string{
			"cwd":     cwd,
			"topic":   topic,
			"adapter": adapter,
		},
	}, "", "  ")
	path := appServerMetaPath(port)
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("seed meta: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	return wsURL
}

// THE REGRESSION TEST for the live incident. Two Codex sessions launched from the
// shared root — which is exactly how they were launched — both infer topic "".
// The second must not conclude the first's app-server is its own.
func TestAppServerMetaMatches_TwoUnknownIdentitiesMustNotMatch(t *testing.T) {
	const (
		root    = "/home/example/workspace"
		adapter = "/usr/local/bin/c3-codex-adapter"
	)
	port := freePort(t)
	// Session one launched from the root, recorded itself with an empty topic.
	wsURL := seedMeta(t, port, root, "", adapter, os.Getpid())

	// Session two: same root, same adapter, same empty topic. Every field is
	// equal — and that must NOT be enough.
	if appServerMetaMatches(wsURL, root, "", adapter) {
		t.Fatal("a session with an unknown identity adopted another unknown identity's app-server. " +
			"This is the 2026-07-26 incident: two Telegram topics delivered into one Codex TUI, " +
			"because \"\" == \"\" was read as \"same session\"")
	}
}

// The fix must not be "never reuse". A session that genuinely knows who it is
// still adopts its own app-server across a relaunch.
func TestAppServerMetaMatches_KnownIdentityStillReuses(t *testing.T) {
	const (
		cwd     = "/home/example/workspace/proj"
		topic   = "proj"
		adapter = "/usr/local/bin/c3-codex-adapter"
	)
	port := freePort(t)
	wsURL := seedMeta(t, port, cwd, topic, adapter, os.Getpid())

	if !appServerMetaMatches(wsURL, cwd, topic, adapter) {
		t.Fatal("a session with a known, matching identity failed to adopt its own app-server; " +
			"the fix must separate unknown identities, not disable reuse entirely")
	}
}

// Every way two sessions can differ must prevent adoption.
func TestAppServerMetaMatches_AnyDifferenceRefuses(t *testing.T) {
	const (
		cwd     = "/home/example/workspace/proj"
		topic   = "proj"
		adapter = "/usr/local/bin/c3-codex-adapter"
	)
	cases := []struct {
		name                    string
		askCwd, askTopic, askAd string
		why                     string
	}{
		{"different topic, same cwd", cwd, "other", adapter,
			"two sessions in one directory attached to different topics — the live incident's shape once topics are known"},
		{"same topic, different cwd", "/home/example/workspace/elsewhere", topic, adapter,
			"same topic name reached from a different directory is not the same session"},
		{"different adapter binary", cwd, topic, "/opt/other/c3-codex-adapter",
			"a different adapter build may speak a different protocol version"},
		{"asking session has no topic", cwd, "", adapter,
			"an unknown identity must not adopt a KNOWN one either — it cannot prove it is that session"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			port := freePort(t)
			wsURL := seedMeta(t, port, cwd, topic, adapter, os.Getpid())
			if appServerMetaMatches(wsURL, tc.askCwd, tc.askTopic, tc.askAd) {
				t.Errorf("adopted an app-server belonging to a different session.\nwhy this matters: %s", tc.why)
			}
		})
	}
}

// A record whose process is gone must not vouch for whatever has since taken the
// port. Reachability proves a socket exists, not whose it is.
func TestAppServerMetaMatches_StaleRecordWithDeadProcessRefuses(t *testing.T) {
	const (
		cwd     = "/home/example/workspace/proj"
		topic   = "proj"
		adapter = "/usr/local/bin/c3-codex-adapter"
	)
	port := freePort(t)
	// PID 0 is never a live user process; Signal(0) on it fails.
	wsURL := seedMeta(t, port, cwd, topic, adapter, 0)

	if appServerMetaMatches(wsURL, cwd, topic, adapter) {
		t.Fatal("a stale record whose app-server process is dead was accepted; " +
			"the port may now belong to an unrelated process, and adopting it sends this " +
			"session's messages there")
	}
}

// The record is per-port. It used to be one file per user, so a second
// app-server silently erased the first's record — after which the first was
// unidentifiable and could be adopted or orphaned by any later launch.
func TestAppServerMeta_IsPerPortNotPerUser(t *testing.T) {
	const (
		cwdA, topicA = "/home/example/workspace/a", "a"
		cwdB, topicB = "/home/example/workspace/b", "b"
		adapter      = "/usr/local/bin/c3-codex-adapter"
	)
	portA, portB := freePort(t), freePort(t)
	if portA == portB {
		t.Skip("ephemeral ports collided")
	}
	if appServerMetaPath(portA) == appServerMetaPath(portB) {
		t.Fatalf("two ports share one record path (%s); the second app-server erases the first's identity",
			appServerMetaPath(portA))
	}

	wsA := seedMeta(t, portA, cwdA, topicA, adapter, os.Getpid())
	wsB := seedMeta(t, portB, cwdB, topicB, adapter, os.Getpid())

	// Writing B must leave A intact and still self-identifying.
	if !appServerMetaMatches(wsA, cwdA, topicA, adapter) {
		t.Error("recording a second app-server destroyed the first's record")
	}
	if !appServerMetaMatches(wsB, cwdB, topicB, adapter) {
		t.Error("the second app-server's own record is wrong")
	}
	// And neither may answer for the other.
	if appServerMetaMatches(wsA, cwdB, topicB, adapter) || appServerMetaMatches(wsB, cwdA, topicA, adapter) {
		t.Error("one app-server's record vouched for a different session")
	}
}

// End to end through the port chooser: a second launch from the shared root,
// with the default port already occupied, must be sent somewhere else.
func TestChooseAppServerURL_SecondRootLaunchGetsItsOwnPort(t *testing.T) {
	const root = "/home/example/workspace"

	// Occupy a port and pretend it is the default the launcher would ask for.
	occupied, closeFn := listeningPort(t)
	defer closeFn()
	requested := "ws://127.0.0.1:" + strconv.Itoa(occupied)

	// The identity check says "not mine" — which is what an empty topic must
	// always say.
	got := chooseAppServerURL(requested, root, "", func(string) bool { return false })

	if got == requested {
		t.Fatalf("second launch was pointed at the occupied app-server %s; "+
			"it must take its own port, or both sessions' Telegram messages surface in one TUI", got)
	}
	if _, port, err := parseWSURL(got); err != nil || port == occupied {
		t.Fatalf("chooser returned %q, which is not a distinct usable ws:// URL", got)
	}
}

// The complement: when the identity check genuinely recognises the app-server,
// the chooser reuses it rather than spawning a needless second one.
func TestChooseAppServerURL_KnownOwnAppServerIsReused(t *testing.T) {
	occupied, closeFn := listeningPort(t)
	defer closeFn()
	requested := "ws://127.0.0.1:" + strconv.Itoa(occupied)

	got := chooseAppServerURL(requested, "/home/example/workspace/proj", "proj",
		func(candidate string) bool { return candidate == requested })

	if got != requested {
		t.Fatalf("chooser abandoned this session's own app-server (got %q, want %q)", got, requested)
	}
}
