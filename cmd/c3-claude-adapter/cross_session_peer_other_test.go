//go:build !linux && !darwin

package main

import "testing"

func TestCrossSessionWithoutPeerPIDFailsClosed(t *testing.T) {
	isolateAdapterTest(t)
	if _, err := (&crossSessionTransport{}).validatedSocket(); err == nil {
		t.Fatal("fallback enabled without peer PID facility")
	}
	if err := validateCrossSessionPeer(nil, 2); err == nil {
		t.Fatal("unverifiable peer accepted")
	}
}
