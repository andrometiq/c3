package broker

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Server is a unix-socket accept loop driven by a Broker.
type Server struct {
	ln   net.Listener
	br   *Broker
	wg   sync.WaitGroup
	stop atomic.Bool

	// connMu guards conns — the set of live accepted connections. Stop() closes
	// every entry so the per-connection HandleConn goroutines, parked in a
	// blocking ReadFrame (no read deadline), return at once. Closing ONLY the
	// listener leaves persistent adapter conns open, so their handlers never
	// unwind and wg.Wait() blocks forever — the SIGTERM drain-wedge (see Stop).
	connMu sync.Mutex
	conns  map[net.Conn]struct{}
}

// ErrSiblingListening is returned by Listen when another broker is already
// serving the socket. The caller should treat it like an Acquire-singleton
// failure (silently exit, do not race).
var ErrSiblingListening = errors.New("broker: socket already served by sibling")

// Listen binds a unix socket at path (mode 0600) and starts accepting
// connections. Caller must Stop() at shutdown to drain in-flight handlers.
//
// Singleton enforcement (2026-05-09 incident — two brokers ended up
// with overlapping listen sockets after a restart-broker race that bypassed
// the pidfile flock by deleting the inode):
//
//   - PROBE before unlink: try to dial the path. If something answers, a
//     sibling is alive — return ErrSiblingListening so the caller exits.
//     The pidfile flock should normally catch this earlier; this is a
//     defense-in-depth check at the actual contention point (the bound
//     listen address).
//   - Only after the probe fails do we unlink + bind.
//
// This makes the listen socket the authoritative singleton: even if two
// brokers race past the pidfile flock (rm-pidfile bug, stale flock inode,
// etc.), exactly one ends up serving the socket.
func Listen(path string, br *Broker) (*Server, error) {
	if siblingAlive(path) {
		return nil, fmt.Errorf("%w (path=%s)", ErrSiblingListening, path)
	}
	if _, err := os.Stat(path); err == nil {
		_ = os.Remove(path)
	}

	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	// Windows can't chmod an AF_UNIX socket file (os.Chmod returns "cannot be
	// accessed by the system"), and 0600 socket perms aren't the access-control
	// mechanism there anyway — the parent dir's NTFS ACL is. Skip the chmod on
	// Windows so it doesn't turn an otherwise-good listen into a FATAL; keep the
	// owner-only hardening on Unix.
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0600); err != nil {
			_ = ln.Close()
			return nil, err
		}
	}

	s := &Server{ln: ln, br: br}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// siblingAlive returns whether the socket at path is actively served by
// some process. Cheap dial-and-close — if anyone accepts, true. ECONNREFUSED
// (file exists but no listener) and ENOENT (no file) both return false.
func siblingAlive(path string) bool {
	c, err := net.DialTimeout("unix", path, 250*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			if s.stop.Load() {
				return
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			return
		}
		if !s.trackConn(c) {
			// Stop fired between Accept and tracking: don't start a handler whose
			// blocking read we could no longer interrupt. Drop the conn and stop
			// accepting — closeConns already ran, so nothing else will close it.
			_ = c.Close()
			return
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer s.untrackConn(c)
			s.br.HandleConn(c)
		}(c)
	}
}

// trackConn registers a live accepted connection so Stop can close it. Returns
// false if Stop has already fired — signaling the caller to drop the connection
// rather than start an unstoppable handler for it.
func (s *Server) trackConn(c net.Conn) bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.stop.Load() {
		return false
	}
	if s.conns == nil {
		s.conns = make(map[net.Conn]struct{})
	}
	s.conns[c] = struct{}{}
	return true
}

// untrackConn removes a connection whose handler has returned.
func (s *Server) untrackConn(c net.Conn) {
	s.connMu.Lock()
	delete(s.conns, c)
	s.connMu.Unlock()
}

// closeConns closes every live accepted connection, unblocking the HandleConn
// read loops parked in ReadFrame so they can return.
func (s *Server) closeConns() {
	s.connMu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.conns = nil
	s.connMu.Unlock()
}

// serverStopGrace bounds how long Stop waits for connection handlers to unwind
// after their sockets are closed. Closing the conns makes every parked ReadFrame
// return at once, so handlers normally exit in microseconds — this bound only
// guards a handler blocked elsewhere (e.g. mid tool-call on a stalled worker)
// from wedging shutdown. On timeout Stop returns anyway; the leaked goroutines
// die with the process and the durable queue keeps the exit loss-free.
const serverStopGrace = 5 * time.Second

// Stop closes the listener AND every live adapter connection, then drains
// in-flight handlers (bounded by serverStopGrace).
//
// Closing the connections is load-bearing. The per-conn handlers sit in a
// blocking ReadFrame with no read deadline; closing ONLY the listener (the
// original behavior) left persistent adapter conns open, so those reads never
// returned and wg.Wait() blocked forever. That was the SIGTERM drain-wedge that
// stranded auto-update — the broker logged "shutting down" and then hung until it
// was SIGKILLed, holding the singleton flock the whole time so the freshly
// installed binary could never start.
func (s *Server) Stop() {
	if !s.stop.CompareAndSwap(false, true) {
		return
	}
	_ = s.ln.Close()
	s.closeConns()
	if !waitGroupTimeout(&s.wg, serverStopGrace) {
		log.Printf("broker: server drain exceeded %s — proceeding with shutdown (handlers exit with the process)", serverStopGrace)
	}
}
