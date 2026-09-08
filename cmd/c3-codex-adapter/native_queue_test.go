//go:build !windows

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

const nativeQueueTestThread = "11111111-2222-7333-8444-555555555555"

func nativeQueueExecutable(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0700); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestNativeQueuePreservesLiteralMessageAndPinsThread(t *testing.T) {
	dir := t.TempDir()
	bin := nativeQueueExecutable(t, `printf '%s\n' "$@" > "$C3_QUEUE_TEST_ARGS"
printf 'Queued message test-id for thread %s.\n' "$3"
`)
	argsPath := filepath.Join(dir, "args")
	t.Setenv("C3_QUEUE_TEST_ARGS", argsPath)
	in := &c3types.Inbound{Text: "literal $(touch NEVER_EXECUTE) `echo x`\nsecond line", MessageID: 42}
	cfg := codexForwardConfig{QueueBin: bin, ThreadID: nativeQueueTestThread, CWD: dir}
	if err := forwardInboundToCodexAppServer(context.Background(), in, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	want := "queue\n--thread\n" + nativeQueueTestThread + "\n--message\n" + formatInboundTurnText(in) + "\n"
	if string(got) != want {
		t.Fatalf("arguments changed: got %q, want %q", got, want)
	}
	if _, err := os.Stat(filepath.Join(dir, "NEVER_EXECUTE")); !os.IsNotExist(err) {
		t.Fatal("message was interpreted as shell code")
	}
}

func TestNativeQueueRefusesAmbiguousIdentity(t *testing.T) {
	for _, id := range []string{"", "project", " ", "--last"} {
		err := forwardInboundToCodexQueue(context.Background(), &c3types.Inbound{}, codexForwardConfig{QueueBin: "/does/not/exist", ThreadID: id})
		if err == nil || !strings.Contains(err.Error(), "explicit") {
			t.Fatalf("identity %q was not refused before invocation: %v", id, err)
		}
	}
	if err := forwardInboundToCodexQueue(context.Background(), &c3types.Inbound{}, codexForwardConfig{QueueBin: "codex", ThreadID: nativeQueueTestThread}); err == nil {
		t.Fatal("relative executable must not be accepted")
	}
}

func TestNativeQueueRequiresConfirmedAcceptance(t *testing.T) {
	for name, body := range map[string]string{
		"failure":       "exit 1\n",
		"empty success": "exit 0\n",
		"wrong thread":  "echo 'Queued message test-id for thread other.'\n",
		"timeout":       "exec sleep 10\n",
	} {
		t.Run(name, func(t *testing.T) {
			cfg := codexForwardConfig{QueueBin: nativeQueueExecutable(t, body), ThreadID: nativeQueueTestThread, Timeout: 100 * time.Millisecond}
			if err := forwardInboundToCodexAppServer(context.Background(), &c3types.Inbound{Text: "hi"}, cfg); err == nil {
				t.Fatal("failed/unconfirmed delivery must not permit broker acknowledgement")
			}
		})
	}
}

func TestNativeQueueIsExplicitOptIn(t *testing.T) {
	t.Setenv("C3_CODEX_QUEUE_BIN", "/opt/codex/bin/codex")
	t.Setenv("C3_CODEX_THREAD_ID", nativeQueueTestThread)
	t.Setenv("C3_CODEX_REMOTE_BRIDGE", "")
	t.Setenv("C3_CODEX_ALLOW_MANUAL_FORWARD", "")
	if !codexForwardingAllowed() || codexForwardConfigFromEnv().QueueBin != "/opt/codex/bin/codex" {
		t.Fatal("native queue configuration not wired to forwarder")
	}
	t.Setenv("C3_CODEX_QUEUE_BIN", "")
	if codexForwardingAllowed() {
		t.Fatal("queue delivery must not silently enable itself")
	}
}

func TestNativeQueueAcceptanceControlsBrokerAck(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "lost"}[lost], func(t *testing.T) {
			dir := t.TempDir()
			argsPath, release := filepath.Join(dir, "args"), filepath.Join(dir, "release")
			t.Setenv("C3_QUEUE_TEST_ARGS", argsPath)
			t.Setenv("C3_QUEUE_TEST_RELEASE", release)
			t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600) })
			ending := `printf 'Queued message test-id for thread %s.\n' "$3"`
			if lost {
				ending = "exit 1"
			}
			bin := nativeQueueExecutable(t, `printf '%s\n' "$5" > "$C3_QUEUE_TEST_ARGS"
: > "$C3_QUEUE_TEST_ARGS.ready"
while [ ! -f "$C3_QUEUE_TEST_RELEASE" ]; do sleep 0.01; done
`+ending+"\n")
			t.Setenv("C3_CODEX_QUEUE_BIN", bin)
			t.Setenv("C3_CODEX_THREAD_ID", nativeQueueTestThread)
			a, peer := adapterWithBrokerConn(t)
			acks := ackFrames(t, peer)
			enqueueRecord(a, "telegram", 9, "native literal $(do-not-run)", "native-row")
			deadline := time.Now().Add(time.Second)
			for {
				if _, err := os.Stat(argsPath + ".ready"); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("native transport never received payload")
				}
				time.Sleep(time.Millisecond)
			}
			got, err := os.ReadFile(argsPath)
			if err != nil || !strings.Contains(string(got), "native literal $(do-not-run)") {
				t.Fatalf("literal payload lost: %s %v", got, err)
			}
			expectNoAck(t, acks)
			if err := os.WriteFile(release, nil, 0600); err != nil {
				t.Fatal(err)
			}
			if lost {
				expectNoAck(t, acks)
			} else {
				expectAck(t, acks, 9, "native literal $(do-not-run)")
			}
		})
	}
}
