package main

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
)

// TestRunStatus_DegradedQueueCannotReadAsHealthy is an end-to-end mutation
// guard for the persistent CLI surface. Removing either the broker's
// QueueDegraded stamp or the c3-broker rendering branch makes this test name the
// same defect: a live broker that is destroying unattached messages reads as
// healthy in the command operators use to diagnose it.
func TestRunStatus_DegradedQueueCannotReadAsHealthy(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	oldHealth, oldClaims := statusFetchHealth, statusFetchClaims
	statusFetchHealth = func() (*ipc.HealthListMsg, error) {
		return &ipc.HealthListMsg{Op: ipc.OpHealthList, QueueDegraded: true}, nil
	}
	statusFetchClaims = func() (*ipc.ClaimsListMsg, error) {
		return &ipc.ClaimsListMsg{Op: ipc.OpClaimsList}, nil
	}
	t.Cleanup(func() {
		statusFetchHealth, statusFetchClaims = oldHealth, oldClaims
	})

	var statusErr error
	out := captureStdout(t, func() {
		statusErr = runStatus()
	})
	if statusErr != nil {
		t.Fatalf("runStatus: %v", statusErr)
	}
	if !strings.Contains(out, "durable queue: DISABLED") ||
		!strings.Contains(out, "not saved and cannot be recovered") {
		t.Fatalf("c3-broker status omitted the persistent durable-queue warning, so a live broker destroying unattached messages reads as healthy. Output:\n%s", out)
	}
}

func TestRunStatus_HealthyQueueDoesNotCryWolf(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	oldHealth, oldClaims := statusFetchHealth, statusFetchClaims
	statusFetchHealth = func() (*ipc.HealthListMsg, error) {
		return &ipc.HealthListMsg{Op: ipc.OpHealthList}, nil
	}
	statusFetchClaims = func() (*ipc.ClaimsListMsg, error) {
		return &ipc.ClaimsListMsg{Op: ipc.OpClaimsList}, nil
	}
	t.Cleanup(func() {
		statusFetchHealth, statusFetchClaims = oldHealth, oldClaims
	})

	var statusErr error
	out := captureStdout(t, func() {
		statusErr = runStatus()
	})
	if statusErr != nil {
		t.Fatalf("runStatus: %v", statusErr)
	}
	if strings.Contains(out, "durable queue: DISABLED") {
		t.Fatalf("c3-broker status warns that the durable queue is disabled when health_list says it is healthy, training operators to ignore the real warning. Output:\n%s", out)
	}
}
