package telegram

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// Connection-setup race budgets. raceBudget covers DNS and every attempt and
// fits under the shortest request deadline on this transport (getMe, 10 s).
// DNS may use at most raceBudget-setupReserve, and the last candidates start no
// later than deadline-setupReserve, so every started attempt gets setupReserve.
const (
	raceBudget    = 8 * time.Second
	setupStagger  = 2 * time.Second
	setupReserve  = 2 * time.Second
	maxCandidates = 4
)

// setupError is returned by setupDialer only when every attempt failed before a
// connection was handed to net/http, so no HTTP request bytes were sent. It has
// no Unwrap and no Timeout method on purpose: it must never read as an
// ambiguous send timeout.
type setupError struct{ msg string }

func (e *setupError) Error() string { return e.msg }

func isSetupError(err error) bool {
	var se *setupError
	return errors.As(err, &se)
}

// setupDialer is the transport's DialTLSContext: it races TCP+TLS setup across
// the resolved addresses of the Bot API host and remembers the winner per host.
// The preference map is shared by every transport cloned from the same one.
type setupDialer struct {
	lookupNetIP func(ctx context.Context, network, host string) ([]netip.Addr, error)
	dialContext func(ctx context.Context, network, addr string) (net.Conn, error)
	rootCAs     *x509.CertPool // nil = system roots
	logf        func(format string, args ...any)

	budget  time.Duration
	stagger time.Duration
	reserve time.Duration

	mu        sync.Mutex
	preferred map[string]netip.Addr
}

func newSetupDialer(logf func(format string, args ...any)) *setupDialer {
	return &setupDialer{
		lookupNetIP: net.DefaultResolver.LookupNetIP,
		dialContext: (&net.Dialer{KeepAlive: 30 * time.Second}).DialContext,
		logf:        logf,
		budget:      raceBudget,
		stagger:     setupStagger,
		reserve:     setupReserve,
		preferred:   map[string]netip.Addr{},
	}
}

// proxyConfigured reports whether any variable net/http's ProxyFromEnvironment
// reads for an http(s) proxy is set. Any proxy disables the race entirely.
func proxyConfigured(getenv func(string) string) bool {
	for _, name := range []string{"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"} {
		if getenv(name) != "" {
			return true
		}
	}
	return false
}

// botAPIClient is the http.Client for Bot API method calls. Redirects are never
// followed, so a POST is never re-sent elsewhere and a setupError covers the
// whole request.
func botAPIClient(transport http.RoundTripper) http.Client {
	return http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

type attemptResult struct {
	index int
	conn  *tls.Conn
	stage string
	err   error
	took  time.Duration
}

func (d *setupDialer) DialTLSContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(d.budget)
	raceCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	addrs, err := d.resolve(raceCtx, host)
	if err != nil {
		return nil, err
	}
	candidates := d.candidates(host, addrs)

	results := make(chan attemptResult, len(candidates))
	failures := make([]string, len(candidates))
	started, inFlight := 0, 0
	var firstStart, lastStart time.Time
	start := func() {
		go d.attempt(raceCtx, network, net.JoinHostPort(candidates[started].String(), port), host, started, results)
		started++
		inFlight++
		lastStart = time.Now()
	}
	start()
	firstStart = lastStart
	lateStart := deadline.Add(-d.reserve)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for inFlight > 0 || started < len(candidates) {
		var timerC <-chan time.Time
		if started < len(candidates) {
			next := lastStart.Add(d.stagger)
			if lateStart.Before(next) {
				next = lateStart
			}
			timer.Reset(time.Until(next))
			timerC = timer.C
		}
		select {
		case r := <-results:
			inFlight--
			if r.err == nil {
				cancel()
				go drainAttempts(results, inFlight)
				d.recordWinner(host, candidates, r.index, failures, time.Since(firstStart))
				return r.conn, nil
			}
			failures[r.index] = describeFailure(r)
			if started < len(candidates) {
				start()
			}
		case <-timerC:
			start() // past lateStart the timer refires at once, so the rest start together
		}
	}
	return nil, newSetupError(addr, candidates, failures)
}

func (d *setupDialer) resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{ip}, nil
	}
	resolveCtx, cancel := context.WithTimeout(ctx, d.budget-d.reserve)
	defer cancel()
	addrs, err := d.lookupNetIP(resolveCtx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, &net.DNSError{Err: "no addresses", Name: host, IsNotFound: true}
	}
	return addrs, nil
}

// candidates interleaves the DNS answers by family (first answer's family
// first, resolver order within a family), moves the remembered winner to the
// front if DNS still returns it, then truncates to maxCandidates.
func (d *setupDialer) candidates(host string, addrs []netip.Addr) []netip.Addr {
	var primary, secondary []netip.Addr
	firstIs4 := addrs[0].Unmap().Is4()
	for _, a := range addrs {
		a = a.Unmap()
		if a.Is4() == firstIs4 {
			primary = append(primary, a)
		} else {
			secondary = append(secondary, a)
		}
	}
	ordered := make([]netip.Addr, 0, len(addrs))
	for i := 0; i < len(primary) || i < len(secondary); i++ {
		if i < len(primary) {
			ordered = append(ordered, primary[i])
		}
		if i < len(secondary) {
			ordered = append(ordered, secondary[i])
		}
	}
	d.mu.Lock()
	preferred, isKnown := d.preferred[host]
	d.mu.Unlock()
	if isKnown {
		for i, a := range ordered {
			if a == preferred {
				copy(ordered[1:i+1], ordered[:i])
				ordered[0] = preferred
				break
			}
		}
	}
	if len(ordered) > maxCandidates {
		ordered = ordered[:maxCandidates]
	}
	return ordered
}

func (d *setupDialer) attempt(ctx context.Context, network, addr, host string, index int, out chan<- attemptResult) {
	begin := time.Now()
	raw, err := d.dialContext(ctx, network, addr)
	if err != nil {
		out <- attemptResult{index: index, stage: "tcp", err: err, took: time.Since(begin)}
		return
	}
	conn := tls.Client(raw, &tls.Config{ServerName: host, RootCAs: d.rootCAs})
	if err := conn.HandshakeContext(ctx); err != nil {
		raw.Close()
		out <- attemptResult{index: index, stage: "tls", err: err, took: time.Since(begin)}
		return
	}
	out <- attemptResult{index: index, conn: conn}
}

// drainAttempts closes connections from attempts that finish after the winner.
func drainAttempts(results <-chan attemptResult, pending int) {
	for ; pending > 0; pending-- {
		if r := <-results; r.conn != nil {
			r.conn.Close()
		}
	}
}

func (d *setupDialer) recordWinner(host string, candidates []netip.Addr, winner int, failures []string, elapsed time.Duration) {
	won := candidates[winner]
	d.mu.Lock()
	previous, hadPrevious := d.preferred[host]
	d.preferred[host] = won
	d.mu.Unlock()
	if won == previous || (!hadPrevious && winner == 0) || d.logf == nil {
		return
	}
	first := candidates[0]
	what := fmt.Sprintf("did not complete in %.1fs", elapsed.Seconds())
	if winner == 0 {
		what = "is no longer the preferred address"
		first = previous
	} else if failures[0] != "" {
		what = "failed (" + strings.SplitN(failures[0], ":", 2)[0] + ")"
	}
	d.logf("telegram: connection setup via %s [%s] %s; now preferring %s [%s]",
		family(first), first, what, family(won), won)
}

func describeFailure(r attemptResult) string {
	var timeout interface{ Timeout() bool }
	if errors.Is(r.err, context.DeadlineExceeded) || (errors.As(r.err, &timeout) && timeout.Timeout()) {
		return fmt.Sprintf("%s: timed out after %.1fs", r.stage, r.took.Seconds())
	}
	if errors.Is(r.err, context.Canceled) {
		return r.stage + ": cancelled"
	}
	var op *net.OpError
	if errors.As(r.err, &op) && op.Err != nil {
		return r.stage + ": " + op.Err.Error()
	}
	return r.stage + ": " + r.err.Error()
}

func newSetupError(addr string, candidates []netip.Addr, failures []string) *setupError {
	parts := make([]string, len(candidates))
	for i, a := range candidates {
		parts[i] = fmt.Sprintf("%s [%s]: %s", family(a), a, failures[i])
	}
	return &setupError{msg: "connection setup to " + addr + " failed: " + strings.Join(parts, "; ")}
}

func family(a netip.Addr) string {
	if a.Is4() {
		return "ipv4"
	}
	return "ipv6"
}
