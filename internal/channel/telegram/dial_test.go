package telegram

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"
)

const (
	testHost    = "api.example.test"
	testToken   = "123456:TEST-TOKEN-not-real"
	testBudget  = time.Second
	testStagger = 250 * time.Millisecond
	testReserve = 250 * time.Millisecond
)

var (
	v6a = netip.MustParseAddr("2001:db8::1")
	v6b = netip.MustParseAddr("2001:db8::2")
	v6c = netip.MustParseAddr("2001:db8::3")
	v4a = netip.MustParseAddr("192.0.2.1")
	v4b = netip.MustParseAddr("192.0.2.2")
	v4c = netip.MustParseAddr("192.0.2.3")
	v4d = netip.MustParseAddr("192.0.2.4")
	v4e = netip.MustParseAddr("192.0.2.5")
	v4f = netip.MustParseAddr("192.0.2.6")
)

// --- certificates ---

type testPKI struct {
	roots     *x509.CertPool
	good      tls.Certificate // issued by the test root for testHost
	wrongHost tls.Certificate // issued by the test root for another name
	untrusted tls.Certificate // self-signed for testHost, not in roots
}

var testCerts = sync.OnceValue(func() testPKI {
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "c3 test root"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		panic(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	leaf := func(name string, parent *x509.Certificate, parentKey *ecdsa.PrivateKey, serial int64) tls.Certificate {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: name},
			DNSNames:     []string{name},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		if parent == nil {
			parent, parentKey = template, key
		}
		der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
		if err != nil {
			panic(err)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}
	return testPKI{
		roots:     roots,
		good:      leaf(testHost, ca, caKey, 2),
		wrongHost: leaf("other.example.test", ca, caKey, 3),
		untrusted: leaf(testHost, nil, nil, 4),
	}
})

// --- fake origin servers ---

type serverMode int32

const (
	modeServe    serverMode = iota // TLS, answer every request 200
	modeStallTLS                   // accept TCP, never answer the ClientHello
	modeSilent                     // TLS, read the request, never answer
	modeRedirect                   // TLS, answer 307
)

type fakeServer struct {
	ln         net.Listener
	cert       tls.Certificate
	mode       atomic.Int32
	redirectTo string

	accepts, closed, handshakes, requests atomic.Int32

	mu    sync.Mutex
	conns []net.Conn
	wg    sync.WaitGroup
}

func newFakeServer(t *testing.T, cert tls.Certificate, mode serverMode) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeServer{ln: ln, cert: cert}
	s.mode.Store(int32(mode))
	s.wg.Add(1)
	go s.serve()
	t.Cleanup(func() {
		ln.Close()
		s.mu.Lock()
		for _, c := range s.conns {
			c.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}

func (s *fakeServer) serve() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.accepts.Add(1)
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		s.wg.Add(1)
		go s.handle(conn)
	}
}

// handle counts closed only once the peer has closed (or the test tears down).
func (s *fakeServer) handle(conn net.Conn) {
	defer s.wg.Done()
	defer s.closed.Add(1)
	defer conn.Close()
	mode := serverMode(s.mode.Load())
	if mode == modeStallTLS {
		io.Copy(io.Discard, conn)
		return
	}
	tc := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{s.cert}})
	if err := tc.Handshake(); err != nil {
		io.Copy(io.Discard, conn)
		return
	}
	s.handshakes.Add(1)
	reader := bufio.NewReader(tc)
	for {
		req, err := http.ReadRequest(reader)
		if err != nil {
			return
		}
		s.requests.Add(1)
		io.Copy(io.Discard, req.Body)
		switch mode {
		case modeSilent:
			io.Copy(io.Discard, reader)
			return
		case modeRedirect:
			fmt.Fprintf(tc, "HTTP/1.1 307 Temporary Redirect\r\nLocation: %s\r\nContent-Length: 0\r\n\r\n", s.redirectTo)
		default:
			body := `{"ok":true,"result":{"id":1,"is_bot":true,"first_name":"t","username":"t"}}`
			fmt.Fprintf(tc, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
		}
	}
}

func (s *fakeServer) open() int32 { return s.accepts.Load() - s.closed.Load() }

// --- fake network: candidate IPs routed to loopback listeners ---

type fakeNet struct {
	mu        sync.Mutex
	routes    map[netip.Addr]string
	blackhole map[netip.Addr]bool
	latency   map[netip.Addr]time.Duration
	dialed    []netip.Addr
}

func newFakeNet() *fakeNet {
	return &fakeNet{routes: map[netip.Addr]string{}, blackhole: map[netip.Addr]bool{}, latency: map[netip.Addr]time.Duration{}}
}

func (f *fakeNet) route(ip netip.Addr, s *fakeServer) { f.routes[ip] = s.ln.Addr().String() }

func (f *fakeNet) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.dialed = append(f.dialed, ap.Addr())
	target, routed := f.routes[ap.Addr()]
	isBlackhole := f.blackhole[ap.Addr()]
	latency := f.latency[ap.Addr()]
	f.mu.Unlock()
	if isBlackhole {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if !routed {
		return nil, &net.OpError{Op: "dial", Net: network, Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	}
	select {
	case <-time.After(latency):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var d net.Dialer
	return d.DialContext(ctx, network, target)
}

func (f *fakeNet) dialedAddrs() []netip.Addr {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]netip.Addr(nil), f.dialed...)
}

type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logSink) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// lookupAfter answers for testHost after delay (or fails when ctx ends first).
func lookupAfter(delay time.Duration, addrs ...netip.Addr) func(context.Context, string, string) ([]netip.Addr, error) {
	return func(ctx context.Context, _, host string) ([]netip.Addr, error) {
		if host != testHost {
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		}
		select {
		case <-time.After(delay):
			return append([]netip.Addr(nil), addrs...), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func newTestDialer(fn *fakeNet, logs *logSink, addrs ...netip.Addr) *setupDialer {
	d := newSetupDialer(logs.logf)
	d.lookupNetIP = lookupAfter(0, addrs...)
	d.dialContext = fn.dial
	d.rootCAs = testCerts().roots
	d.budget, d.stagger, d.reserve = testBudget, testStagger, testReserve
	return d
}

// testTransport is the production transport with the test dialer and no
// environment proxy (hermetic even when the test runner has one set).
func testTransport(t *testing.T, d *setupDialer, disableKeepAlives bool) *http.Transport {
	tr := newBotTransport(func(string) string { return "" }, d)
	tr.Proxy = nil
	tr.DisableKeepAlives = disableKeepAlives
	t.Cleanup(tr.CloseIdleConnections)
	return tr
}

func testBot(tr http.RoundTripper, timeout time.Duration) *gotgbot.Bot {
	return &gotgbot.Bot{Token: testToken, BotClient: &gotgbot.BaseBotClient{
		Client:             botAPIClient(tr),
		DefaultRequestOpts: &gotgbot.RequestOpts{Timeout: timeout, APIURL: "https://" + testHost},
	}}
}

func dialTest(d *setupDialer) (net.Conn, error) {
	return d.DialTLSContext(context.Background(), "tcp", net.JoinHostPort(testHost, "443"))
}

func getOK(t *testing.T, client *http.Client) {
	t.Helper()
	resp, err := client.Get("https://" + testHost + "/")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func (d *setupDialer) preferredFor(host string) netip.Addr {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.preferred[host]
}

func waitGoroutines(t *testing.T, baseline int) {
	t.Helper()
	if !waitUntil(3*time.Second, func() bool { return runtime.NumGoroutine() <= baseline }) {
		t.Fatalf("goroutines = %d, baseline %d", runtime.NumGoroutine(), baseline)
	}
}

// 1
func TestSetupRaceStallThenGood(t *testing.T) {
	stall := newFakeServer(t, testCerts().good, modeStallTLS)
	good := newFakeServer(t, testCerts().good, modeServe)
	fn := newFakeNet()
	fn.route(v6a, stall)
	fn.route(v4a, good)
	logs := &logSink{}
	d := newTestDialer(fn, logs, v6a, v4a)
	client := &http.Client{Transport: testTransport(t, d, true)}

	begin := time.Now()
	getOK(t, client)
	if took := time.Since(begin); took < testStagger || took > testStagger+500*time.Millisecond {
		t.Fatalf("first request took %v, want about the stagger (%v)", took, testStagger)
	}
	if !waitUntil(time.Second, func() bool { return stall.accepts.Load() == 1 && stall.open() == 0 }) {
		t.Fatalf("stalled socket not closed: accepts=%d open=%d", stall.accepts.Load(), stall.open())
	}

	begin = time.Now()
	getOK(t, client)
	if took := time.Since(begin); took >= testStagger {
		t.Fatalf("second request took %v; the learned address should be dialed first", took)
	}
	if got := stall.accepts.Load(); got != 1 {
		t.Fatalf("stalled listener accepts = %d after the second request, want 1", got)
	}
	if got := good.accepts.Load(); got != 2 {
		t.Fatalf("good listener accepts = %d, want 2 fresh connections", got)
	}
	lines := logs.all()
	if len(lines) != 1 || !strings.Contains(lines[0], "now preferring ipv4 [192.0.2.1]") || !strings.Contains(lines[0], "via ipv6 [2001:db8::1]") {
		t.Fatalf("log lines = %q", lines)
	}
}

// 2
func TestSetupRaceHealthyFirstAddress(t *testing.T) {
	first := newFakeServer(t, testCerts().good, modeServe)
	other := newFakeServer(t, testCerts().good, modeServe)
	fn := newFakeNet()
	fn.route(v6a, first)
	fn.route(v4a, other)
	logs := &logSink{}
	d := newTestDialer(fn, logs, v6a, v4a)

	conn, err := dialTest(d)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	time.Sleep(testStagger + 50*time.Millisecond)
	if got := fn.dialedAddrs(); len(got) != 1 || got[0] != v6a {
		t.Fatalf("dialed %v, want only %v", got, v6a)
	}
	if other.accepts.Load() != 0 {
		t.Fatal("second listener was contacted")
	}
	if lines := logs.all(); len(lines) != 0 {
		t.Fatalf("unexpected log: %q", lines)
	}
	if d.preferredFor(testHost) != v6a {
		t.Fatalf("preferred = %v", d.preferredFor(testHost))
	}
}

// 3
func TestSetupRaceFirstRefusedStartsNextImmediately(t *testing.T) {
	good := newFakeServer(t, testCerts().good, modeServe)
	fn := newFakeNet() // v6a unrouted: connection refused
	fn.route(v4a, good)
	d := newTestDialer(fn, &logSink{}, v6a, v4a)

	begin := time.Now()
	conn, err := dialTest(d)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if took := time.Since(begin); took >= testStagger/2 {
		t.Fatalf("took %v; a refused first address must not wait for the stagger", took)
	}
}

// 4
func TestSetupRaceAllStallReturnsSetupError(t *testing.T) {
	stallA := newFakeServer(t, testCerts().good, modeStallTLS)
	stallB := newFakeServer(t, testCerts().good, modeStallTLS)
	fn := newFakeNet()
	fn.route(v6a, stallA)
	fn.route(v4a, stallB)
	d := newTestDialer(fn, &logSink{}, v6a, v4a)
	bot := testBot(testTransport(t, d, false), 5*time.Second)

	begin := time.Now()
	_, err := bot.GetMeWithContext(context.Background(), nil)
	took := time.Since(begin)
	if took < testBudget-50*time.Millisecond || took > testBudget+500*time.Millisecond {
		t.Fatalf("failed after %v, want about the race budget %v", took, testBudget)
	}
	var se *setupError
	if !errors.As(err, &se) || !isSetupError(err) {
		t.Fatalf("err = %v, want a *setupError", err)
	}
	msg := se.Error()
	for _, want := range []string{testHost + ":443", "timed out", "ipv6 [2001:db8::1]", "ipv4 [192.0.2.1]"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("setup error %q lacks %q", msg, want)
		}
	}
	for _, banned := range []string{testToken, "/bot", "getMe"} {
		if strings.Contains(msg, banned) {
			t.Fatalf("setup error %q contains %q", msg, banned)
		}
	}
	if !definitelyNotSent(err) {
		t.Fatal("a setup error must count as definitely not sent")
	}
	if class, _ := classifyError(err); class != errClassTransient {
		t.Fatalf("class = %v, want transient", class)
	}
	if strings.Contains(err.Error(), testToken) {
		t.Fatal("wrapped error leaks the token")
	}
	if !waitUntil(time.Second, func() bool { return stallA.open() == 0 && stallB.open() == 0 }) {
		t.Fatal("stalled sockets left open")
	}
}

// 5
func TestSetupRaceVerifiesCertificatesAgainstHost(t *testing.T) {
	for name, bad := range map[string]tls.Certificate{
		"untrusted issuer":       testCerts().untrusted,
		"trusted CA, wrong host": testCerts().wrongHost,
	} {
		t.Run(name+"/bad first, good second", func(t *testing.T) {
			badServer := newFakeServer(t, bad, modeServe)
			good := newFakeServer(t, testCerts().good, modeServe)
			fn := newFakeNet()
			fn.route(v6a, badServer)
			fn.route(v4a, good)
			d := newTestDialer(fn, &logSink{}, v6a, v4a)

			conn, err := dialTest(d)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if state := conn.(*tls.Conn).ConnectionState(); state.PeerCertificates[0].Subject.CommonName != testHost {
				t.Fatalf("connected to %q", state.PeerCertificates[0].Subject.CommonName)
			}
			if !waitUntil(time.Second, func() bool { return good.handshakes.Load() == 1 }) || d.preferredFor(testHost) != v4a {
				t.Fatalf("good handshakes = %d preferred = %v", good.handshakes.Load(), d.preferredFor(testHost))
			}
			if !waitUntil(time.Second, func() bool { return badServer.accepts.Load() == 1 && badServer.open() == 0 }) {
				t.Fatal("socket with the bad certificate was not closed")
			}
			if badServer.handshakes.Load() != 0 || badServer.requests.Load() != 0 {
				t.Fatal("bad-certificate server completed a handshake or got a request")
			}
		})
		t.Run(name+"/all bad", func(t *testing.T) {
			badA := newFakeServer(t, bad, modeServe)
			badB := newFakeServer(t, bad, modeServe)
			fn := newFakeNet()
			fn.route(v6a, badA)
			fn.route(v4a, badB)
			d := newTestDialer(fn, &logSink{}, v6a, v4a)

			conn, err := dialTest(d)
			if conn != nil || !isSetupError(err) || !strings.Contains(err.Error(), "certificate") {
				t.Fatalf("conn = %v err = %v, want no conn and a certificate setup error", conn, err)
			}
			client := &http.Client{Transport: testTransport(t, d, false)}
			if _, err := client.Get("https://" + testHost + "/"); err == nil || !isSetupError(err) {
				t.Fatalf("request err = %v, want a setup error", err)
			}
			if !waitUntil(time.Second, func() bool { return badA.open() == 0 && badB.open() == 0 }) {
				t.Fatal("sockets left open")
			}
			for _, s := range []*fakeServer{badA, badB} {
				if s.accepts.Load() != 2 || s.handshakes.Load() != 0 || s.requests.Load() != 0 {
					t.Fatalf("accepts=%d handshakes=%d requests=%d", s.accepts.Load(), s.handshakes.Load(), s.requests.Load())
				}
			}
		})
	}
}

// 6
func TestSetupRaceSimultaneousWinnersReturnOne(t *testing.T) {
	a := newFakeServer(t, testCerts().good, modeServe)
	b := newFakeServer(t, testCerts().good, modeServe)
	fn := newFakeNet()
	fn.route(v6a, a)
	fn.route(v4a, b)
	d := newTestDialer(fn, &logSink{}, v6a, v4a)
	d.stagger = 0
	// Neither attempt reports until both handshakes have completed, so the
	// winner is chosen with a second finished connection already in hand.
	var both sync.WaitGroup
	both.Add(2)
	d.afterHandshake = func() {
		both.Done()
		both.Wait()
	}

	conn, err := dialTest(d)
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(time.Second, func() bool { return a.handshakes.Load()+b.handshakes.Load() == 2 && a.open()+b.open() == 1 }) {
		t.Fatalf("handshakes=%d open=%d, want 2 completed and only the winner open",
			a.handshakes.Load()+b.handshakes.Load(), a.open()+b.open())
	}
	conn.Close()
	if !waitUntil(time.Second, func() bool { return a.open()+b.open() == 0 }) {
		t.Fatal("winner not closed")
	}
}

func TestDrainAttemptsClosesLateWinners(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	results := make(chan attemptResult, 2)
	results <- attemptResult{err: errors.New("refused")}
	results <- attemptResult{conn: tls.Client(client, &tls.Config{ServerName: testHost})}
	drainAttempts(results, 2)
	if _, err := client.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("late winner not closed: %v", err)
	}
}

// 7
func TestSetupRaceStalledResolverReturnsResolverError(t *testing.T) {
	fn := newFakeNet()
	d := newTestDialer(fn, &logSink{}, v4a)
	resolverErr := &net.DNSError{Err: "server misbehaving", Name: testHost, IsTimeout: true}
	d.lookupNetIP = func(ctx context.Context, _, _ string) ([]netip.Addr, error) {
		<-ctx.Done()
		return nil, resolverErr
	}

	begin := time.Now()
	_, err := dialTest(d)
	took := time.Since(begin)
	if resolveBudget := testBudget - testReserve; took < resolveBudget-20*time.Millisecond || took > resolveBudget+200*time.Millisecond {
		t.Fatalf("returned after %v, want about %v", took, resolveBudget)
	}
	if err != resolverErr || isSetupError(err) {
		t.Fatalf("err = %v, want the resolver's error unchanged", err)
	}
	if len(fn.dialedAddrs()) != 0 {
		t.Fatal("dialed without an address")
	}
}

// 8
func TestSetupRaceSlowDNSStillGivesAlternateTheReserve(t *testing.T) {
	good := newFakeServer(t, testCerts().good, modeServe)
	fn := newFakeNet()
	fn.blackhole[v6a] = true
	fn.route(v4a, good)
	fn.latency[v4a] = 150 * time.Millisecond // started at the stagger it would miss the deadline
	d := newTestDialer(fn, &logSink{}, v6a, v4a)
	d.lookupNetIP = lookupAfter(testBudget-testReserve-50*time.Millisecond, v6a, v4a)

	conn, err := dialTest(d)
	if err != nil {
		t.Fatalf("err = %v, want success via the alternate", err)
	}
	conn.Close()
	if d.preferredFor(testHost) != v4a {
		t.Fatalf("preferred = %v, want %v", d.preferredFor(testHost), v4a)
	}
}

// 9
func TestSetupRaceWorkingLateCandidate(t *testing.T) {
	good := newFakeServer(t, testCerts().good, modeServe)
	spare := newFakeServer(t, testCerts().good, modeServe)
	fn := newFakeNet()
	fn.blackhole[v4a], fn.blackhole[v4b], fn.blackhole[v4c] = true, true, true
	fn.route(v4d, good)
	fn.route(v4e, spare)
	d := newTestDialer(fn, &logSink{}, v4a, v4b, v4c, v4d, v4e)
	d.stagger = 400 * time.Millisecond // starts at 0, 400, then the 3rd and 4th together at deadline-reserve

	begin := time.Now()
	conn, err := dialTest(d)
	if err != nil {
		t.Fatalf("err = %v, want success via the 4th candidate", err)
	}
	conn.Close()
	if took := time.Since(begin); took >= testBudget {
		t.Fatalf("took %v, past the race budget", took)
	}
	for _, a := range fn.dialedAddrs() {
		if a == v4e {
			t.Fatal("5th resolved address was dialed")
		}
	}
	if spare.accepts.Load() != 0 || d.preferredFor(testHost) != v4d {
		t.Fatalf("spare accepts = %d preferred = %v", spare.accepts.Load(), d.preferredFor(testHost))
	}
}

// 10
func TestSetupRaceManyStallingCandidates(t *testing.T) {
	fn := newFakeNet()
	addrs := []netip.Addr{v4a, v4b, v4c, v4d, v4e, v4f}
	var servers []*fakeServer
	for _, a := range addrs {
		s := newFakeServer(t, testCerts().good, modeStallTLS)
		fn.route(a, s)
		servers = append(servers, s)
	}
	d := newTestDialer(fn, &logSink{}, addrs...)
	baseline := runtime.NumGoroutine()

	begin := time.Now()
	conn, err := dialTest(d)
	took := time.Since(begin)
	if conn != nil || !isSetupError(err) {
		t.Fatalf("conn = %v err = %v, want a setup error", conn, err)
	}
	if took < testBudget-50*time.Millisecond || took > testBudget+500*time.Millisecond {
		t.Fatalf("failed after %v, want about %v", took, testBudget)
	}
	if got := len(fn.dialedAddrs()); got != maxCandidates {
		t.Fatalf("dialed %d addresses, want %d", got, maxCandidates)
	}
	if !waitUntil(time.Second, func() bool {
		for _, s := range servers {
			if s.open() != 0 {
				return false
			}
		}
		return true
	}) {
		t.Fatal("sockets left open")
	}
	waitGoroutines(t, baseline)
}

// 11
func TestSetupRaceCancelledRequest(t *testing.T) {
	t.Run("all stall", func(t *testing.T) {
		stallA := newFakeServer(t, testCerts().good, modeStallTLS)
		stallB := newFakeServer(t, testCerts().good, modeStallTLS)
		fn := newFakeNet()
		fn.route(v6a, stallA)
		fn.route(v4a, stallB)
		d := newTestDialer(fn, &logSink{}, v6a, v4a)
		client := &http.Client{Transport: testTransport(t, d, false)}
		baseline := runtime.NumGoroutine()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+testHost+"/", nil)
		begin := time.Now()
		_, err := client.Do(req)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(begin) > 300*time.Millisecond {
			t.Fatalf("err = %v after %v, want the caller's deadline promptly", err, time.Since(begin))
		}
		if !waitUntil(testBudget+500*time.Millisecond, func() bool { return stallA.open() == 0 && stallB.open() == 0 }) {
			t.Fatal("detached dial left sockets open past the race budget")
		}
		waitGoroutines(t, baseline)
	})
	t.Run("late success is pooled", func(t *testing.T) {
		stall := newFakeServer(t, testCerts().good, modeStallTLS)
		good := newFakeServer(t, testCerts().good, modeServe)
		fn := newFakeNet()
		fn.route(v6a, stall)
		fn.route(v4a, good)
		d := newTestDialer(fn, &logSink{}, v6a, v4a)
		tr := testTransport(t, d, false)
		client := &http.Client{Transport: tr}

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+testHost+"/", nil)
		if _, err := client.Do(req); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want the caller's deadline", err)
		}
		if !waitUntil(testBudget, func() bool { return good.handshakes.Load() == 1 && stall.open() == 0 }) {
			t.Fatalf("late dial: good handshakes = %d stalled open = %d", good.handshakes.Load(), stall.open())
		}
		if good.open() != 1 {
			t.Fatalf("good open = %d, want the one pooled connection", good.open())
		}
		tr.CloseIdleConnections()
		if !waitUntil(time.Second, func() bool { return good.open() == 0 }) {
			t.Fatal("the late connection was not in the idle pool")
		}
	})
}

// 12
func TestSetupRaceNoReplayAfterRequestSent(t *testing.T) {
	silent := newFakeServer(t, testCerts().good, modeSilent)
	fn := newFakeNet()
	fn.route(v4a, silent)
	d := newTestDialer(fn, &logSink{}, v4a)
	bot := testBot(testTransport(t, d, false), 500*time.Millisecond)

	_, err := bot.GetMeWithContext(context.Background(), nil)
	if err == nil || isSetupError(err) {
		t.Fatalf("err = %v, want a non-setup failure", err)
	}
	if silent.accepts.Load() != 1 || silent.requests.Load() != 1 {
		t.Fatalf("accepts = %d requests = %d, want 1 and 1", silent.accepts.Load(), silent.requests.Load())
	}
}

// 13
func TestBotAPIClientDoesNotFollowRedirects(t *testing.T) {
	origin := newFakeServer(t, testCerts().good, modeRedirect)
	origin.redirectTo = "https://redirect.example.test/elsewhere"
	fn := newFakeNet()
	fn.route(v4a, origin)
	fn.blackhole[v4b] = true
	d := newTestDialer(fn, &logSink{}, v4a)
	var targetLookups atomic.Int32
	d.lookupNetIP = func(ctx context.Context, network, host string) ([]netip.Addr, error) {
		if host == "redirect.example.test" {
			targetLookups.Add(1)
			return []netip.Addr{v4b}, nil
		}
		return lookupAfter(0, v4a)(ctx, network, host)
	}
	bot := testBot(testTransport(t, d, false), 2*time.Second)

	_, err := bot.GetMeWithContext(context.Background(), nil)
	if err == nil || isSetupError(err) {
		t.Fatalf("err = %v, want a non-setup error", err)
	}
	if origin.requests.Load() != 1 {
		t.Fatalf("origin requests = %d, want exactly one POST", origin.requests.Load())
	}
	for _, a := range fn.dialedAddrs() {
		if a == v4b {
			t.Fatal("redirect target was dialed")
		}
	}
	if targetLookups.Load() != 0 {
		t.Fatal("redirect target was resolved")
	}
}

// 14
func TestRaceInstalledOnlyWithoutProxy(t *testing.T) {
	d := newSetupDialer(nil)
	for _, name := range []string{"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"} {
		for _, value := range []string{"https://proxy.example.test:3128", "http://proxy.example.test:3128"} {
			env := map[string]string{name: value}
			if tr := newBotTransport(func(k string) string { return env[k] }, d); tr.DialTLSContext != nil {
				t.Errorf("%s=%s: race hook installed with a proxy configured", name, value)
			}
		}
	}
	if tr := newBotTransport(func(string) string { return "" }, d); tr.DialTLSContext == nil {
		t.Fatal("no proxy configured: race hook missing")
	}
}

// 15
func TestSetupRaceLearnedPreferenceMovesWhenItStalls(t *testing.T) {
	a := newFakeServer(t, testCerts().good, modeServe)
	b := newFakeServer(t, testCerts().good, modeServe)
	fn := newFakeNet()
	fn.route(v6a, a)
	fn.route(v4a, b)
	logs := &logSink{}
	d := newTestDialer(fn, logs, v6a, v4a)
	client := &http.Client{Transport: testTransport(t, d, true)}

	getOK(t, client)
	if d.preferredFor(testHost) != v6a {
		t.Fatalf("preferred = %v, want %v", d.preferredFor(testHost), v6a)
	}
	a.mode.Store(int32(modeStallTLS))
	getOK(t, client)
	if d.preferredFor(testHost) != v4a {
		t.Fatalf("preferred = %v, want %v after the learned address stalled", d.preferredFor(testHost), v4a)
	}
	before := len(fn.dialedAddrs())
	getOK(t, client)
	if dialed := fn.dialedAddrs()[before:]; len(dialed) != 1 || dialed[0] != v4a {
		t.Fatalf("third request dialed %v, want only %v", dialed, v4a)
	}
	if lines := logs.all(); len(lines) != 1 || !strings.Contains(lines[0], "now preferring ipv4 [192.0.2.1]") {
		t.Fatalf("log lines = %q", lines)
	}
}

// 16
func TestSetupRaceCandidateOrder(t *testing.T) {
	answers := []netip.Addr{v4a, v4b, v4c, v6a, v6b, v6c}
	d := newSetupDialer(nil)

	if got, want := d.candidates(testHost, answers), []netip.Addr{v4a, v6a, v4b, v6b}; !equalAddrs(got, want) {
		t.Fatalf("interleaved = %v, want %v", got, want)
	}
	if got, want := d.candidates(testHost, []netip.Addr{v6a, v4a, v4b}), []netip.Addr{v6a, v4a, v4b}; !equalAddrs(got, want) {
		t.Fatalf("v6-first = %v, want %v", got, want)
	}

	d.preferred[testHost] = v6c // 6th after interleaving
	if got, want := d.candidates(testHost, answers), []netip.Addr{v6c, v4a, v6a, v4b}; !equalAddrs(got, want) {
		t.Fatalf("promoted = %v, want %v (promote, then truncate)", got, want)
	}

	absent := netip.MustParseAddr("2001:db8::99")
	d.preferred[testHost] = absent
	for _, a := range d.candidates(testHost, answers) {
		if a == absent {
			t.Fatal("remembered winner no longer in DNS was re-added")
		}
	}

	t.Run("dialed first", func(t *testing.T) {
		good := newFakeServer(t, testCerts().good, modeServe)
		fn := newFakeNet()
		fn.route(v6c, good)
		dialer := newTestDialer(fn, &logSink{}, answers...)
		dialer.preferred[testHost] = v6c
		conn, err := dialTest(dialer)
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
		if got := fn.dialedAddrs(); len(got) != 1 || got[0] != v6c {
			t.Fatalf("dialed %v, want only %v", got, v6c)
		}
	})
}

func equalAddrs(a, b []netip.Addr) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// 17
func TestSetupRaceSharedByMainAndProbeTransports(t *testing.T) {
	a := newFakeServer(t, testCerts().good, modeServe)
	b := newFakeServer(t, testCerts().good, modeStallTLS)
	fn := newFakeNet()
	fn.route(v6a, a)
	fn.route(v4a, b)
	logs := &logSink{}
	d := newTestDialer(fn, logs, v6a, v4a)
	d.stagger = 10 * time.Millisecond
	mainTransport := testTransport(t, d, false)
	probe := mainTransport.Clone()
	probe.DisableKeepAlives = true
	t.Cleanup(probe.CloseIdleConnections)

	// At most one address stalls at a time, and an address starts stalling only
	// a gap (well over the stagger) after the other resumed serving, so every
	// request always has one address that serves for its whole race.
	const gap = 80 * time.Millisecond
	stop := make(chan struct{})
	flipped := make(chan struct{})
	go func() {
		defer close(flipped)
		serving, stalling := a, b
		for {
			select {
			case <-stop:
				return
			case <-time.After(gap):
			}
			stalling.mode.Store(int32(modeServe))
			time.Sleep(gap)
			serving.mode.Store(int32(modeStallTLS))
			serving, stalling = stalling, serving
		}
	}()
	runUntil := time.Now().Add(8 * gap)

	var wg sync.WaitGroup
	var errsMu sync.Mutex
	var errs []error
	for i := 0; i < 4; i++ {
		tr := http.RoundTripper(mainTransport)
		if i%2 == 1 {
			tr = probe
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Transport: tr}
			for time.Now().Before(runUntil) {
				resp, err := client.Get("https://" + testHost + "/")
				if err != nil {
					errsMu.Lock()
					errs = append(errs, err)
					errsMu.Unlock()
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-flipped
	for _, err := range errs {
		t.Errorf("request failed: %v", err)
	}
	if len(logs.all()) == 0 {
		t.Fatal("the preferred address never flipped; the test exercised nothing")
	}
	if p := d.preferredFor(testHost); p != v6a && p != v4a {
		t.Fatalf("preferred = %v, want one of the winners", p)
	}
}
