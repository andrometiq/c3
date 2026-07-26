package ipc

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
)

func errorIsFrameTooLarge(err error) bool { return errors.Is(err, ErrFrameTooLarge) }

// oversizeWriter streams `total` bytes containing no newline at all, then
// closes. It models a peer that ignores newline framing — the pre-negotiation
// denial-of-service the frame cap is supposed to stop.
func oversizeWriter(t *testing.T, w net.Conn, total int) {
	t.Helper()
	go func() {
		defer w.Close()
		blob := bytes.Repeat([]byte("A"), 64*1024)
		for written := 0; written < total; {
			n, err := w.Write(blob)
			if err != nil {
				return // reader closed on us — that is the pass condition
			}
			written += n
		}
	}()
}

// TestReadFrame_RejectsOversizeFrame is the EXT-15 regression test: a peer that
// never sends a newline must be cut off at MaxFrameSize, not allowed to drive
// unbounded allocation in the broker before hello/auth has happened.
func TestReadFrame_RejectsOversizeFrame(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	oversizeWriter(t, client, MaxFrameSize+128*1024)

	c := NewConn(server)
	type result struct {
		raw []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		raw, err := c.ReadFrame()
		done <- result{raw, err}
	}()

	select {
	case res := <-done:
		if res.err == nil {
			t.Fatalf("ReadFrame accepted an oversize frame: returned %d bytes with nil error (cap of %d never fired)",
				len(res.raw), MaxFrameSize)
		}
		if !errorIsFrameTooLarge(res.err) {
			t.Fatalf("ReadFrame returned %v, want a frame-too-large error", res.err)
		}
		if len(res.raw) != 0 {
			t.Fatalf("ReadFrame returned %d bytes alongside the error; want none", len(res.raw))
		}
	case <-time.After(15 * time.Second):
		_ = server.Close()
		t.Fatal("ReadFrame never returned: it kept accumulating a newline-less stream (unbounded allocation)")
	}
}

// TestReadFrame_AcceptsLargeLegalFrame guards the other side of the cap: a
// frame just under the limit, spanning many read-buffer refills, must still
// reassemble byte-for-byte.
func TestReadFrame_AcceptsLargeLegalFrame(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := bytes.Repeat([]byte("x"), MaxFrameSize-1)
	go func() {
		_, _ = client.Write(payload)
		_, _ = client.Write([]byte("\n"))
	}()

	c := NewConn(server)
	raw, err := c.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame rejected a legal %d-byte frame: %v", len(payload), err)
	}
	if !bytes.Equal(raw, payload) {
		t.Fatalf("frame corrupted: got %d bytes, want %d", len(raw), len(payload))
	}
}

// TestReadFrame_MultiChunkFramesStayAligned exercises the reassembly path with
// frames larger than one read-buffer refill, back to back, to prove the reader
// does not lose or merge frame boundaries.
func TestReadFrame_MultiChunkFramesStayAligned(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Includes the exact read-buffer boundaries, where ReadSlice flips between
	// "delimiter as the final byte of a full buffer" and ErrBufferFull.
	sizes := []int{
		1, 200 * 1024, 7,
		readBufSize - 1, readBufSize, readBufSize + 1,
		512*1024 + 3, 0,
	}
	go func() {
		for i, n := range sizes {
			_, _ = client.Write(bytes.Repeat([]byte{byte('a' + i)}, n))
			_, _ = client.Write([]byte("\n"))
		}
	}()

	c := NewConn(server)
	for i, n := range sizes {
		raw, err := c.ReadFrame()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		want := bytes.Repeat([]byte{byte('a' + i)}, n)
		if !bytes.Equal(raw, want) {
			t.Fatalf("frame %d: got %d bytes, want %d", i, len(raw), n)
		}
	}
}

// TestReadFrame_UnterminatedTailAtEOF pins the pre-existing (and intentional)
// behavior that a final frame without a trailing newline is still delivered
// when the peer closes cleanly.
func TestReadFrame_UnterminatedTailAtEOF(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	go func() {
		_, _ = client.Write([]byte(`{"op":"hello"}`))
		client.Close()
	}()

	c := NewConn(server)
	raw, err := c.ReadFrame()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(raw) != `{"op":"hello"}` {
		t.Fatalf("got %q", raw)
	}
}
