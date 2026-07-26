package ipc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

// Conn wraps a net.Conn with newline-JSON framing and a write mutex. Multiple
// goroutines can safely call WriteJSON concurrently; only one frame at a time
// reaches the wire.
//
// Spec §4.4.1: "the IPC socket is duplex and a single connection carries
// both request/response (synchronous tool calls) and unsolicited push
// (broker → adapter inbound). Both sides use a single bufio.Writer guarded
// by a sync.Mutex; line-by-line frames are atomic."
type Conn struct {
	c   net.Conn
	w   *bufio.Writer
	wmu sync.Mutex
	r   *bufio.Reader
}

// MaxFrameSize bounds the largest IPC frame we'll accept on a single
// ReadFrame call, counting the terminating newline. Without a cap a peer
// (broker, adapter, or any process that opened the socket) could stream
// bytes without a newline and exhaust memory — and on the broker side that
// happens BEFORE hello, i.e. before any negotiation. 4 MiB is well above any
// legitimate frame: the largest real frames are MCP tool-call results that
// embed Telegram message content, on the order of tens of KB.
const MaxFrameSize = 4 * 1024 * 1024

// (Exported deliberately: docs/ADAPTERS.md publishes this number as part of the
// frozen third-party wire contract, and the broker sizes its own fetch_queue
// batches against it. A hardcoded copy in either place would drift.)

// readBufSize is the per-connection bufio.Reader buffer. It is deliberately
// far smaller than MaxFrameSize: bufio.NewReaderSize allocates eagerly, so
// sizing it at the cap would cost 4 MiB per connection the instant a peer
// connects — the very resource exhaustion this file is defending against,
// just paid up front. ReadFrame reassembles across refills instead, growing
// only as far as an actual frame requires and never past MaxFrameSize.
const readBufSize = 64 * 1024

// ErrFrameTooLarge is returned by ReadFrame when a peer streams more than
// MaxFrameSize bytes without a newline, and by WriteJSON when the frame WE are
// about to emit would exceed the same cap.
//
// On the READ side the connection is left unusable by design — callers must
// close it rather than attempt to resynchronize, since the remaining bytes
// cannot be attributed to any frame boundary.
//
// On the WRITE side nothing reaches the wire, so the connection stays healthy
// and usable; the caller gets a local, named error instead. That asymmetry is
// the point: the cap has to be enforced on both sides or C3 can marshal a frame
// that C3 itself cannot read back — the peer's ReadFrame would reject it and
// drop a live connection, taking every in-flight request with it.
var ErrFrameTooLarge = errors.New("ipc: frame exceeds maximum size")

// NewConn wraps a net.Conn. Owner is responsible for calling Close.
func NewConn(c net.Conn) *Conn {
	return &Conn{
		c: c,
		w: bufio.NewWriter(c),
		r: bufio.NewReaderSize(c, readBufSize),
	}
}

// WriteJSON marshals v and writes one newline-terminated frame to the wire.
// Safe for concurrent use.
//
// Returns ErrFrameTooLarge, having written NOTHING, if the marshaled frame
// would exceed MaxFrameSize — the same cap ReadFrame enforces on the way in.
// Without this check the two directions disagree: a response that is legal to
// build (e.g. fetch_queue with all=true draining a long-held queue) would be
// put on the wire and then rejected by the peer's ReadFrame, which desyncs and
// tears down a connection that was doing nothing wrong. Failing here keeps the
// blast radius at one request instead of one connection.
func (c *Conn) WriteJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("ipc: marshal: %w", err)
	}
	// +1 for the terminating newline: MaxFrameSize counts it on the read side,
	// so the two boundaries have to be computed the same way.
	if len(data)+1 > MaxFrameSize {
		return fmt.Errorf("%w: refusing to write a %d-byte frame (cap %d) — the peer's ReadFrame would reject it and drop the connection",
			ErrFrameTooLarge, len(data)+1, MaxFrameSize)
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if _, err := c.w.Write(data); err != nil {
		return err
	}
	if err := c.w.WriteByte('\n'); err != nil {
		return err
	}
	return c.w.Flush()
}

// ReadFrame reads one \n-terminated frame and returns its bytes (without the
// trailing newline). Returns io.EOF when the peer closes cleanly. Returns
// ErrFrameTooLarge once a peer has streamed MaxFrameSize bytes without a
// newline (defense against a peer that ignores the framing).
//
// This deliberately does NOT use bufio.Reader.ReadBytes: ReadBytes handles
// bufio.ErrBufferFull internally (see bufio.collectFragments — it catches the
// sentinel, clones the full buffer into a growing [][]byte, and loops), so the
// sentinel is never returned to the caller and any cap tested against it is
// dead code. ReadSlice is the primitive that does surface ErrBufferFull, so we
// drive the refill loop ourselves and count bytes as we go.
//
// NOT safe for concurrent use — only one reader goroutine per Conn.
func (c *Conn) ReadFrame() ([]byte, error) {
	var frame []byte
	for {
		// ReadSlice returns a view into the bufio buffer, valid only until the
		// next read — appendCapped copies, so callers still get owned bytes.
		chunk, readErr := c.r.ReadSlice('\n')

		banked, capErr := appendCapped(frame, chunk)
		if capErr != nil {
			return nil, capErr
		}
		frame = banked

		switch {
		case readErr == nil:
			// Delimiter found — frame complete.
			return trimEOL(frame), nil
		case errors.Is(readErr, bufio.ErrBufferFull):
			// Buffer filled with no delimiter in sight: refill and keep going,
			// now one chunk closer to the cap.
			continue
		case errors.Is(readErr, io.EOF):
			// A clean close with a trailing unterminated frame still delivers
			// that frame; a clean close with nothing pending is plain EOF.
			if len(frame) == 0 {
				return nil, io.EOF
			}
			return trimEOL(frame), nil
		default:
			return nil, readErr
		}
	}
}

// appendCapped appends src to dst, refusing to let the accumulated frame grow
// past MaxFrameSize. It also grows the accumulator by hand rather than leaning
// on append's doubling, so a peer sitting just under the cap cannot provoke an
// allocation of twice the cap.
func appendCapped(dst, src []byte) ([]byte, error) {
	if len(src) == 0 {
		return dst, nil
	}
	need := len(dst) + len(src)
	if need > MaxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes without a newline (peer not respecting newline framing)",
			ErrFrameTooLarge, MaxFrameSize)
	}
	if cap(dst) < need {
		grown := cap(dst) * 2
		if grown < need {
			grown = need
		}
		if grown > MaxFrameSize {
			grown = MaxFrameSize
		}
		next := make([]byte, len(dst), grown)
		copy(next, dst)
		dst = next
	}
	return append(dst, src...), nil
}

// trimEOL strips a trailing \n and an optional preceding \r.
func trimEOL(frame []byte) []byte {
	n := len(frame)
	if n > 0 && frame[n-1] == '\n' {
		n--
		if n > 0 && frame[n-1] == '\r' {
			n--
		}
	}
	return frame[:n]
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	return c.c.Close()
}
