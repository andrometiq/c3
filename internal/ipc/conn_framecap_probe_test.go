package ipc

import (
	"errors"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// realSocketPair returns a connected pair over a REAL unix socket, not
// net.Pipe. net.Pipe is synchronous and unbuffered — each Read sees exactly one
// Write — which is a forgiving shape for a bufio refill loop: an implementation
// that only works when every Read happens to deliver a full buffer passes over
// net.Pipe and fails on a kernel socket, which coalesces and splits at will.
func realSocketPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "s.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	type res struct {
		c   net.Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := ln.Accept()
		ch <- res{c, err}
	}()
	client, err = net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatalf("accept: %v", r.err)
	}
	t.Cleanup(func() { _ = client.Close(); _ = r.c.Close() })
	return client, r.c
}

// The cap must fire when the attacker DRIPS bytes in small, unaligned writes
// over a real socket — not just when frames arrive in tidy buffer-sized chunks.
func TestReadFrame_CapFiresOnDrippedBytesOverRealSocket(t *testing.T) {
	client, server := realSocketPair(t)

	go func() {
		// 977 bytes: not a power of two, never aligned with readBufSize, so the
		// refill boundary is crossed at odd offsets every time.
		blob := []byte(strings.Repeat("Z", 977))
		for written := 0; written < MaxFrameSize+512*1024; {
			n, err := client.Write(blob)
			if err != nil {
				return // reader cut us off — the pass condition
			}
			written += n
		}
	}()

	c := NewConn(server)
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		raw, err := c.ReadFrame()
		done <- result{len(raw), err}
	}()

	select {
	case r := <-done:
		if r.err == nil {
			t.Fatalf("cap never fired: ReadFrame returned %d bytes with nil error", r.n)
		}
		if !errors.Is(r.err, ErrFrameTooLarge) {
			t.Fatalf("got %v, want ErrFrameTooLarge", r.err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("ReadFrame never returned on a dripped oversize stream — still accumulating")
	}
}

// The actual EXT-15 claim — "allocation is bounded" — MEASURED, not inferred
// from reading the code. Offer far more than the cap across several connections
// and watch the heap.
func TestReadFrame_HeapStaysBoundedUnderOversizeStream(t *testing.T) {
	const conns = 8
	const perConn = 16 * 1024 * 1024 // 128 MiB offered in total

	var peak uint64
	for i := 0; i < conns; i++ {
		client, server := realSocketPair(t)
		go func() {
			blob := make([]byte, 64*1024)
			for w := 0; w < perConn; {
				n, err := client.Write(blob)
				if err != nil {
					return
				}
				w += n
			}
		}()
		c := NewConn(server)
		if _, err := c.ReadFrame(); !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("conn %d: got %v, want ErrFrameTooLarge", i, err)
		}
		_ = server.Close()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if m.HeapAlloc > peak {
			peak = m.HeapAlloc
		}
	}
	t.Logf("offered %d MiB across %d conns; peak HeapAlloc = %d MiB",
		conns*perConn/(1<<20), conns, peak/(1<<20))
	// Deliberately generous (the cap is 4 MiB; growth can transiently hold ~8).
	// Anything approaching the 128 MiB offered means the cap is not bounding.
	if peak > 48*1024*1024 {
		t.Fatalf("heap reached %d MiB — allocation is not bounded by the cap", peak/(1<<20))
	}
}

// The cap boundary is exact in BOTH directions, pinned so a later refactor
// cannot quietly move it by one byte and start rejecting real traffic (or start
// accepting a frame the peer will refuse).
func TestFrameCap_BoundaryIsExact(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload int
		wantErr bool
	}{
		{"payload-plus-newline-equals-cap", MaxFrameSize - 1, false},
		{"one-byte-past-cap", MaxFrameSize, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := realSocketPair(t)
			go func() {
				buf := make([]byte, tc.payload)
				for i := range buf {
					buf[i] = 'q'
				}
				_, _ = client.Write(buf)
				_, _ = client.Write([]byte("\n"))
			}()
			c := NewConn(server)
			raw, err := c.ReadFrame()
			if tc.wantErr {
				if !errors.Is(err, ErrFrameTooLarge) {
					t.Fatalf("payload %d + newline: err=%v len=%d, want ErrFrameTooLarge",
						tc.payload, err, len(raw))
				}
				return
			}
			if err != nil {
				t.Fatalf("payload %d + newline rejected: %v", tc.payload, err)
			}
			if len(raw) != tc.payload {
				t.Fatalf("got %d bytes, want %d", len(raw), tc.payload)
			}
		})
	}
}

// After the cap fires the connection is DESYNCED — the remaining bytes cannot
// be attributed to a frame boundary. This pins the contract that callers must
// CLOSE rather than retry: if a later read ever came back as a clean frame, a
// retrying caller would execute attacker-chosen bytes that arrived after a
// flood as if they were a legitimate message.
func TestReadFrame_AfterCapTheConnIsDesynced(t *testing.T) {
	client, server := realSocketPair(t)

	const bait = `{"op":"attach","name":"attacker"}`
	go func() {
		blob := make([]byte, 64*1024)
		for w := 0; w < MaxFrameSize+256*1024; {
			n, err := client.Write(blob)
			if err != nil {
				return
			}
			w += n
		}
		_, _ = client.Write([]byte("\n" + bait + "\n"))
	}()

	c := NewConn(server)
	if _, err := c.ReadFrame(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("first read: got %v, want ErrFrameTooLarge", err)
	}
	raw, err := c.ReadFrame()
	if err == nil && string(raw) == bait {
		t.Fatal("connection RESYNCED after the cap fired — a caller that retried " +
			"instead of closing would act on a frame the attacker planted after a flood")
	}
}

// WriteJSON must refuse to emit a frame the peer's ReadFrame would reject.
// Without this the two directions disagree and an oversized-but-legal response
// (fetch_queue with all=true over a long-held queue is the real one) is written
// to the wire, rejected on arrival, and takes the whole connection down with
// every in-flight request on it.
func TestWriteJSON_RefusesOversizeFrame(t *testing.T) {
	client, server := realSocketPair(t)
	c := NewConn(client)

	// A JSON string of MaxFrameSize 'a's marshals to strictly more than
	// MaxFrameSize once quotes and the newline are added.
	//
	// Run it off the test goroutine with a deadline: an UNCAPPED WriteJSON does
	// not just wrongly succeed, it BLOCKS — it pushes megabytes at a socket
	// nobody is draining while holding the write mutex, wedging every other
	// writer on this Conn. Without the deadline that regression shows up as a
	// hung test suite instead of a failing assertion.
	errCh := make(chan error, 1)
	go func() { errCh <- c.WriteJSON(map[string]string{"text": strings.Repeat("a", MaxFrameSize)}) }()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(10 * time.Second):
		t.Fatal("WriteJSON BLOCKED on an oversize frame instead of refusing it — " +
			"it is pushing an unreadable frame at the peer while holding the write mutex")
	}
	if err == nil {
		t.Fatal("WriteJSON accepted an oversize frame — the peer's ReadFrame would " +
			"reject it and drop the connection")
	}
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}

	// Nothing must have reached the wire, and the connection must still work:
	// a refused write is a per-request failure, not a connection failure.
	if err := c.WriteJSON(map[string]string{"op": "ping"}); err != nil {
		t.Fatalf("connection unusable after a refused write: %v", err)
	}
	peer := NewConn(server)
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("peer read: %v", err)
	}
	if got := string(raw); got != `{"op":"ping"}` {
		t.Fatalf("peer received %q — a partial oversize frame leaked onto the wire", got)
	}
}

// The write cap and the read cap must agree on the boundary: the largest frame
// WriteJSON emits must be one ReadFrame accepts. An off-by-one between them is
// the silent version of the bug this whole file exists for.
func TestFrameCap_WriteAndReadBoundariesAgree(t *testing.T) {
	client, server := realSocketPair(t)
	c := NewConn(client)
	peer := NewConn(server)

	// `"` + payload + `"` + `\n` == MaxFrameSize exactly.
	payload := strings.Repeat("a", MaxFrameSize-3)
	done := make(chan error, 1)
	go func() { done <- c.WriteJSON(payload) }()

	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame rejected the largest frame WriteJSON emits: %v", err)
	}
	if werr := <-done; werr != nil {
		t.Fatalf("WriteJSON rejected a frame of exactly the cap: %v", werr)
	}
	if len(raw) != MaxFrameSize-1 {
		t.Fatalf("round-tripped %d bytes, want %d", len(raw), MaxFrameSize-1)
	}
}
