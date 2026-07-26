//go:build !windows

package main

import (
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// These tests exist because of a live incident on 2026-07-26: two Codex
// sessions, attached to two different Telegram topics, both received every
// message. The broker had routed each message to the correct adapter — the
// adapters then forwarded them into the SAME app-server, because the second
// launcher adopted the first's.
//
// The class of bug, stated so the tests can guard the class and not the
// instance:
//
//	TWO SESSIONS MUST NEVER SHARE A DELIVERY DESTINATION UNLESS EACH CAN PROVE
//	IT IS THE SAME SESSION.
//
// The first fix made an UNKNOWN identity refuse to match, because the original
// code compared (cwd, topic, adapter) field by field and inferTopicName returns
// "" for a launch from the shared root — so two unknown identities compared
// equal. That stopped the incident as it happened, and it was not enough: two
// launches from any project SUBDIRECTORY both infer the project's name, so they
// still matched field for field and still merged.
//
// The defect was categorical. (cwd, topic, adapter) is a DESCRIPTION, not an
// identity — two sessions launched the same way share it forever, so it can only
// ever answer "were these started alike?", never "are these the same session?".
// No repair of the fields fixes that, so adoption is gone entirely: a busy port
// is always refused and a fresh app-server always started. Adoption bought
// nothing worth keeping — Codex threads live in their own rollout files and
// `codex resume` reloads them onto any app-server.
//
// That costs at most one extra process. The alternative cost is one person's
// messages arriving in someone else's session. Those are not symmetrical, and
// these tests pin the asymmetry.

// listeningPort returns a port with something listening on it, plus a closer.
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

// deadPID returns a PID that is genuinely not running: a short-lived child that
// has already been waited on. Using a real reaped PID rather than 0 keeps the
// "malformed record" case (pid <= 0) distinct from the "recorded process has
// exited" case, which are answered differently.
func deadPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("/bin/true") // no shell: nothing here is interpolated
	if err := cmd.Start(); err != nil {
		t.Fatalf("spawn throwaway process: %v", err)
	}
	pid := cmd.Process.Pid
	_ = cmd.Wait()
	return pid
}

// seedMeta writes an app-server record for port as if a launcher had started
// one, and removes it when the test ends.
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

// THE REGRESSION TEST for the class. A second launch whose recorded signature
// matches this one EXACTLY — same cwd, same non-empty topic, same adapter, and a
// process that really is alive — must still not be adopted.
//
// This is the case the first fix let through: two Codex sessions launched from
// any subdirectory of a project both infer that project's name, so every field
// is equal and the liveness check passes. Equal descriptions are not proof of
// one session.
func TestEnsureAppServer_SameSignatureIsStillNotMe(t *testing.T) {
	const (
		cwd     = "/home/example/workspace/proj/internal"
		topic   = "proj"
		adapter = "/usr/local/bin/c3-codex-adapter"
	)
	port, closeFn := listeningPort(t)
	defer closeFn()
	// os.Getpid() is unambiguously alive, so liveness cannot be what refuses
	// here — only the never-adopt rule can.
	wsURL := seedMeta(t, port, cwd, topic, adapter, os.Getpid())

	err := ensureAppServer("/usr/bin/codex", adapter, wsURL, cwd, topic)
	if err == nil {
		t.Fatal("a second launch adopted a running app-server whose signature merely MATCHED its own. " +
			"Two sessions launched the same way always match, so this is the 2026-07-26 incident again, " +
			"reachable from any project subdirectory. A busy port must always be refused.")
	}
	if !strings.Contains(err.Error(), "Refusing to share an app-server") {
		t.Errorf("the refusal must say plainly what it refuses and why; got: %v", err)
	}
}

// The refusal has to be actionable, or the user is left with a launcher that
// simply will not start. It names the holder and the escape hatch.
func TestEnsureAppServer_RefusalNamesTheHolderAndTheWayOut(t *testing.T) {
	const (
		cwd     = "/home/example/workspace/proj"
		adapter = "/usr/local/bin/c3-codex-adapter"
	)
	port, closeFn := listeningPort(t)
	defer closeFn()
	wsURL := seedMeta(t, port, cwd, "proj", adapter, os.Getpid())

	err := ensureAppServer("/usr/bin/codex", adapter, wsURL, cwd, "proj")
	if err == nil {
		t.Fatal("expected a refusal on a busy port")
	}
	msg := err.Error()
	for _, want := range []string{
		strconv.Itoa(os.Getpid()), // who holds it
		cwd,                       // where that one was launched from
		"C3_CODEX_APP_SERVER_WS",  // how to get past it
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal is missing %q — a user cannot act on it.\ngot: %s", want, msg)
		}
	}
}

// A busy port with NO record at all (a foreign process) must be refused just as
// firmly, and must not fall over trying to describe an owner it cannot identify.
func TestEnsureAppServer_ForeignProcessOnPortIsRefused(t *testing.T) {
	port, closeFn := listeningPort(t)
	defer closeFn()
	wsURL := "ws://127.0.0.1:" + strconv.Itoa(port)
	// Deliberately no seedMeta: nothing on disk describes this listener.

	err := ensureAppServer("/usr/bin/codex", "/usr/local/bin/c3-codex-adapter", wsURL, "/tmp/x", "x")
	if err == nil {
		t.Fatal("an unidentified process holding the port was adopted as this session's app-server")
	}
	if !strings.Contains(err.Error(), "already in use") {
		t.Errorf("unexpected refusal wording: %v", err)
	}
}

// The port chooser must walk past a busy port unconditionally — including when
// the busy one is described exactly like this launch.
func TestChooseAppServerURL_BusyPortAlwaysMovesOn(t *testing.T) {
	occupied, closeFn := listeningPort(t)
	defer closeFn()
	requested := "ws://127.0.0.1:" + strconv.Itoa(occupied)
	seedMeta(t, occupied, "/home/example/workspace/proj", "proj",
		"/usr/local/bin/c3-codex-adapter", os.Getpid())

	got := chooseAppServerURL(requested)

	if got == requested {
		t.Fatalf("chooser returned the occupied app-server %s; both sessions' Telegram messages "+
			"would surface in one TUI", got)
	}
	if _, port, err := parseWSURL(got); err != nil || port == occupied {
		t.Fatalf("chooser returned %q, which is not a distinct usable ws:// URL", got)
	}
}

// A free port is used as-is — never-adopt must not turn into never-use.
func TestChooseAppServerURL_FreePortIsUsedAsIs(t *testing.T) {
	free := freePort(t)
	requested := "ws://127.0.0.1:" + strconv.Itoa(free)

	if got := chooseAppServerURL(requested); got != requested {
		t.Fatalf("chooser walked away from a FREE port (got %q, want %q); every launch would climb "+
			"the port range for no reason", got, requested)
	}
}

// The record is per-port. It used to be one file per user, so a second
// app-server silently erased the first's. It is diagnostics now rather than a
// gate, but a diagnostic that names the wrong process is worse than none.
func TestAppServerMeta_IsPerPortAndNamesItsOwnHolder(t *testing.T) {
	const adapter = "/usr/local/bin/c3-codex-adapter"
	portA, portB := freePort(t), freePort(t)
	if portA == portB {
		t.Skip("ephemeral ports collided")
	}
	if appServerMetaPath(portA) == appServerMetaPath(portB) {
		t.Fatalf("two ports share one record path (%s); the second app-server erases the first's",
			appServerMetaPath(portA))
	}

	wsA := seedMeta(t, portA, "/home/example/workspace/a", "a", adapter, os.Getpid())
	wsB := seedMeta(t, portB, "/home/example/workspace/b", "b", adapter, deadPID(t))

	ownerA := appServerPortOwner(wsA)
	if !strings.Contains(ownerA, "/home/example/workspace/a") ||
		!strings.Contains(ownerA, strconv.Itoa(os.Getpid())) {
		t.Errorf("port A's owner description is wrong or was overwritten by port B's record: %q", ownerA)
	}
	if strings.Contains(ownerA, "/home/example/workspace/b") {
		t.Errorf("port A described using port B's record: %q", ownerA)
	}

	// PID 0 is never a live process: the description must say the record is
	// stale rather than vouch for whatever now holds the port.
	if ownerB := appServerPortOwner(wsB); !strings.Contains(ownerB, "no longer running") {
		t.Errorf("a record whose process is dead must be reported as stale, not as the holder: %q", ownerB)
	}
}

// Nothing on disk means we say nothing rather than guessing.
func TestAppServerPortOwner_UnknownPortSaysNothing(t *testing.T) {
	if got := appServerPortOwner("ws://127.0.0.1:" + strconv.Itoa(freePort(t))); got != "" {
		t.Errorf("described an app-server we have no record of: %q", got)
	}
}
