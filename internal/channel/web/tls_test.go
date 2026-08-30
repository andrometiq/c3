package web

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func tlsTestHost(config Config) *fakeHost {
	return &fakeHost{
		webConfig:  config,
		telegram:   telegramConfig{MasterUserID: 42, DMChatID: 42},
		registered: true,
		allowed:    true,
	}
}

func startTLSWithFakeListener(t *testing.T, config Config) (*Channel, *fakeHost, []string) {
	t.Helper()
	host := tlsTestHost(config)
	c := New()
	var addresses []string
	c.listenFunc = func(_, address string) (net.Listener, error) {
		addresses = append(addresses, address)
		resolved, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			return nil, err
		}
		return &fakeListener{address: resolved, done: make(chan struct{})}, nil
	}
	if err := c.Start(context.Background(), host); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return c, host, addresses
}

func readTestCertificate(t *testing.T, path string) (*x509.Certificate, []byte) {
	t.Helper()
	certificatePEM, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(certificatePEM)
	if block == nil {
		t.Fatalf("%s is not PEM", path)
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, certificatePEM
}

type memoryListener struct {
	connections chan net.Conn
	closed      chan struct{}
	once        sync.Once
	address     net.Addr
}

func newMemoryListener() *memoryListener {
	return &memoryListener{
		connections: make(chan net.Conn),
		closed:      make(chan struct{}),
		address:     &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 43871},
	}
}

func (listener *memoryListener) Accept() (net.Conn, error) {
	select {
	case connection := <-listener.connections:
		return connection, nil
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

func (listener *memoryListener) Close() error {
	listener.once.Do(func() { close(listener.closed) })
	return nil
}

func (listener *memoryListener) Addr() net.Addr { return listener.address }

func (listener *memoryListener) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case listener.connections <- server:
		return client, nil
	case <-listener.closed:
		_ = client.Close()
		_ = server.Close()
		return nil, net.ErrClosed
	case <-ctx.Done():
		_ = client.Close()
		_ = server.Close()
		return nil, ctx.Err()
	}
}

func TestTLSMaterialCreatedAndReused(t *testing.T) {
	stateRoot := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	config := Config{Listen: "127.0.0.1:8371", PublicURL: "https://127.0.0.1:8371", TLS: true}

	first, firstHost, _ := startTLSWithFakeListener(t, config)
	directory := filepath.Join(stateRoot, "c3", "web")
	for path, mode := range map[string]os.FileMode{
		directory:                                0o700,
		filepath.Join(directory, caKeyFile):      0o600,
		filepath.Join(directory, caCertFile):     0o644,
		filepath.Join(directory, serverKeyFile):  0o600,
		filepath.Join(directory, serverCertFile): 0o644,
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if got := info.Mode().Perm(); got != mode {
			t.Errorf("mode %s = %o, want %o", path, got, mode)
		}
	}
	firstFingerprint := first.caFingerprint()
	firstLeaf, firstLeafPEM := readTestCertificate(t, filepath.Join(directory, serverCertFile))
	if !strings.Contains(firstHost.logText(), "new CA generated") {
		t.Fatalf("first Start log omitted new CA guidance: %q", firstHost.logText())
	}
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	second, secondHost, _ := startTLSWithFakeListener(t, config)
	defer second.Stop()
	secondLeaf, secondLeafPEM := readTestCertificate(t, filepath.Join(directory, serverCertFile))
	if second.caFingerprint() != firstFingerprint {
		t.Fatalf("CA fingerprint changed: %q -> %q", firstFingerprint, second.caFingerprint())
	}
	if firstLeaf.SerialNumber.Cmp(secondLeaf.SerialNumber) != 0 || !bytes.Equal(firstLeafPEM, secondLeafPEM) {
		t.Fatal("second Start reissued an unchanged server certificate")
	}
	if strings.Contains(secondHost.logText(), "new CA generated") {
		t.Fatalf("second Start claimed to generate the CA: %q", secondHost.logText())
	}
}

func TestTLSChangingPublicURLReissuesOnlyLeaf(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	firstConfig := Config{Listen: "100.100.10.20:8371", PublicURL: "https://100.100.10.20:8371", TLS: true}
	first, _, _ := startTLSWithFakeListener(t, firstConfig)
	directory := filepath.Join(os.Getenv("XDG_STATE_HOME"), "c3", "web")
	firstLeaf, _ := readTestCertificate(t, filepath.Join(directory, serverCertFile))
	firstFingerprint := first.caFingerprint()
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	secondConfig := firstConfig
	secondConfig.PublicURL = "https://100.100.10.21:8371"
	second, _, _ := startTLSWithFakeListener(t, secondConfig)
	defer second.Stop()
	secondLeaf, _ := readTestCertificate(t, filepath.Join(directory, serverCertFile))
	if firstLeaf.SerialNumber.Cmp(secondLeaf.SerialNumber) == 0 {
		t.Fatal("public_url host change reused the old leaf")
	}
	if second.caFingerprint() != firstFingerprint {
		t.Fatal("public_url host change replaced the CA")
	}
}

func TestTLSCorruptCAKeyRefusesWithoutRegeneration(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	config := Config{Listen: "127.0.0.1:8371", PublicURL: "https://127.0.0.1:8371", TLS: true}
	first, _, _ := startTLSWithFakeListener(t, config)
	directory := filepath.Join(os.Getenv("XDG_STATE_HOME"), "c3", "web")
	_, caPEM := readTestCertificate(t, filepath.Join(directory, caCertFile))
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}
	corrupt := []byte("not a private key\n")
	if err := os.WriteFile(filepath.Join(directory, caKeyFile), corrupt, 0o600); err != nil {
		t.Fatal(err)
	}

	host := tlsTestHost(config)
	c := New()
	var listeners []*fakeListener
	c.listenFunc = func(_, address string) (net.Listener, error) {
		resolved, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			return nil, err
		}
		listener := &fakeListener{address: resolved, done: make(chan struct{})}
		listeners = append(listeners, listener)
		return listener, nil
	}
	err := c.Start(context.Background(), host)
	if err == nil || !strings.Contains(err.Error(), "refusing to replace the trusted CA") {
		t.Fatalf("Start error = %v", err)
	}
	keyAfter, readErr := os.ReadFile(filepath.Join(directory, caKeyFile))
	if readErr != nil || !bytes.Equal(keyAfter, corrupt) {
		t.Fatalf("corrupt key was replaced: data=%q err=%v", keyAfter, readErr)
	}
	_, caAfter := readTestCertificate(t, filepath.Join(directory, caCertFile))
	if !bytes.Equal(caAfter, caPEM) {
		t.Fatal("CA certificate changed after corrupt-key refusal")
	}
	for _, listener := range listeners {
		select {
		case <-listener.done:
		default:
			t.Fatal("failed Start left a listener open")
		}
	}
}

func TestTLSCACertificateWithoutKeyRefusesAndRewritesNothing(t *testing.T) {
	stateRoot := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	config := Config{Listen: "127.0.0.1:8371", PublicURL: "https://127.0.0.1:8371", TLS: true}
	first, _, _ := startTLSWithFakeListener(t, config)
	directory := filepath.Join(stateRoot, "c3", "web")
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	unchanged := make(map[string][]byte)
	for _, name := range []string{caCertFile, serverKeyFile, serverCertFile} {
		contents, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			t.Fatal(err)
		}
		unchanged[name] = contents
	}
	if err := os.Remove(filepath.Join(directory, caKeyFile)); err != nil {
		t.Fatal(err)
	}

	host := tlsTestHost(config)
	channel := New()
	channel.listenFunc = func(_, address string) (net.Listener, error) {
		resolved, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			return nil, err
		}
		return &fakeListener{address: resolved, done: make(chan struct{})}, nil
	}
	err := channel.Start(context.Background(), host)
	if err == nil || !strings.Contains(err.Error(), "private CA is incomplete or unreadable") {
		t.Fatalf("Start error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, caKeyFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing ca.key was recreated: %v", err)
	}
	for name, before := range unchanged {
		after, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil || !bytes.Equal(after, before) {
			t.Errorf("%s changed after incomplete-CA refusal: err=%v", name, err)
		}
	}
}

func TestTLSMismatchedCAKeyRefusesWithoutRegeneration(t *testing.T) {
	stateRoot := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	config := Config{Listen: "127.0.0.1:8371", PublicURL: "https://127.0.0.1:8371", TLS: true}
	first, _, _ := startTLSWithFakeListener(t, config)
	directory := filepath.Join(stateRoot, "c3", "web")
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	otherDirectory := t.TempDir()
	if _, _, _, err := generateCA(otherDirectory, time.Now()); err != nil {
		t.Fatal(err)
	}
	otherKey, err := os.ReadFile(filepath.Join(otherDirectory, caKeyFile))
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(directory, caKeyFile)
	if err := os.WriteFile(keyPath, otherKey, 0o600); err != nil {
		t.Fatal(err)
	}
	certificatePath := filepath.Join(directory, caCertFile)
	certificateBefore, err := os.ReadFile(certificatePath)
	if err != nil {
		t.Fatal(err)
	}

	host := tlsTestHost(config)
	channel := New()
	channel.listenFunc = func(_, address string) (net.Listener, error) {
		resolved, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			return nil, err
		}
		return &fakeListener{address: resolved, done: make(chan struct{})}, nil
	}
	err = channel.Start(context.Background(), host)
	if err == nil || !strings.Contains(err.Error(), "private CA key does not match ca.crt") {
		t.Fatalf("Start error = %v", err)
	}
	keyAfter, keyErr := os.ReadFile(keyPath)
	certificateAfter, certificateErr := os.ReadFile(certificatePath)
	if keyErr != nil || !bytes.Equal(keyAfter, otherKey) {
		t.Errorf("mismatched ca.key changed after refusal: err=%v", keyErr)
	}
	if certificateErr != nil || !bytes.Equal(certificateAfter, certificateBefore) {
		t.Errorf("ca.crt changed after mismatch refusal: err=%v", certificateErr)
	}
}

func TestTLSPrepareRefusesMissingStateHome(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("HOME", "")
	t.Setenv("USERPROFILE", "")
	channel := New()
	channel.cfg = Config{PublicURL: "https://127.0.0.1:8371", TLS: true}
	if _, err := channel.prepareTLS("127.0.0.1:8371"); err == nil || !strings.Contains(err.Error(), "resolve home directory") {
		t.Fatalf("prepareTLS error = %v", err)
	}
}

func TestAtomicWriteCertificateFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "certificate.pem")
	if err := atomicWriteCertificateFile(path, []byte("certificate\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil || string(contents) != "certificate\n" {
		t.Fatalf("atomic certificate contents=%q err=%v", contents, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o644 {
		t.Fatalf("atomic certificate mode=%v, want 0644", got)
	}
}

func TestTLSLeafSANUsageValidityAndChain(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	config := Config{Listen: "100.100.10.20:8371", PublicURL: "https://100.100.10.20:8371", TLS: true}
	c, _, _ := startTLSWithFakeListener(t, config)
	defer c.Stop()
	directory := filepath.Join(os.Getenv("XDG_STATE_HOME"), "c3", "web")
	leaf, _ := readTestCertificate(t, filepath.Join(directory, serverCertFile))
	ca, _ := readTestCertificate(t, filepath.Join(directory, caCertFile))

	for _, wanted := range []string{"127.0.0.1", "::1", "100.100.10.20"} {
		found := false
		for _, ip := range leaf.IPAddresses {
			found = found || ip.String() == wanted
		}
		if !found {
			t.Errorf("leaf IP SANs %v omit %s", leaf.IPAddresses, wanted)
		}
	}
	foundLocalhost := false
	for _, name := range leaf.DNSNames {
		foundLocalhost = foundLocalhost || name == "localhost"
	}
	if !foundLocalhost {
		t.Errorf("leaf DNS SANs %v omit localhost", leaf.DNSNames)
	}
	if len(leaf.ExtKeyUsage) != 1 || leaf.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
		t.Errorf("leaf ExtKeyUsage = %v", leaf.ExtKeyUsage)
	}
	if leaf.NotAfter.Sub(leaf.NotBefore) > 366*24*time.Hour {
		t.Errorf("leaf validity = %s, want at most 366 days", leaf.NotAfter.Sub(leaf.NotBefore))
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots: roots, DNSName: "100.100.10.20", KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Fatalf("verify leaf: %v", err)
	}
}

func TestCertificateFingerprintFormatAndDigest(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	config := Config{Listen: "127.0.0.1:8371", PublicURL: "https://127.0.0.1:8371", TLS: true}
	channel, _, _ := startTLSWithFakeListener(t, config)
	defer channel.Stop()
	certificate, _ := readTestCertificate(t, filepath.Join(os.Getenv("XDG_STATE_HOME"), "c3", "web", caCertFile))
	fingerprint := certificateFingerprint(certificate)
	if len(fingerprint) != 95 {
		t.Fatalf("fingerprint length=%d, want 95: %q", len(fingerprint), fingerprint)
	}
	if fingerprint != strings.ToUpper(fingerprint) {
		t.Fatalf("fingerprint is not upper-hex: %q", fingerprint)
	}
	for index := 2; index < len(fingerprint); index += 3 {
		if fingerprint[index] != ':' {
			t.Fatalf("fingerprint separator at index %d = %q, want colon: %q", index, fingerprint[index], fingerprint)
		}
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(fingerprint, ":", ""))
	if err != nil {
		t.Fatalf("decode fingerprint: %v", err)
	}
	digest := sha256.Sum256(certificate.Raw)
	if !bytes.Equal(decoded, digest[:]) {
		t.Fatalf("fingerprint does not equal SHA-256 of certificate DER")
	}
}

func TestTLSListenerHandshakeCAEndpointAndSecureCookie(t *testing.T) {
	stateRoot := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	listen := "127.0.0.1:43871"
	config := Config{Listen: listen, PublicURL: "https://" + listen, TLS: true}
	host := tlsTestHost(config)
	c := New()
	listener := newMemoryListener()
	c.listenFunc = func(_, address string) (net.Listener, error) {
		if address != listen {
			t.Fatalf("listen address = %q", address)
		}
		return listener, nil
	}
	if err := c.Start(context.Background(), host); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	caPEM, err := os.ReadFile(filepath.Join(stateRoot, "c3", "web", caCertFile))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append CA")
	}
	transport := &http.Transport{
		DialContext:     listener.DialContext,
		TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
	}
	client := &http.Client{Transport: transport}
	defer transport.CloseIdleConnections()
	baseURL := "https://" + c.listen

	response, err := client.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("trusted GET /healthz: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d", response.StatusCode)
	}
	_ = response.Body.Close()

	caResponse, err := client.Get(baseURL + "/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	servedCA, err := io.ReadAll(caResponse.Body)
	_ = caResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if caResponse.StatusCode != http.StatusOK || !bytes.Equal(servedCA, caPEM) {
		t.Fatalf("GET /ca.crt status/equality = %d/%v", caResponse.StatusCode, bytes.Equal(servedCA, caPEM))
	}
	if got := caResponse.Header.Get("Content-Type"); got != "application/x-x509-ca-cert" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := caResponse.Header.Get("Content-Disposition"); got != `attachment; filename="c3-web-ca.crt"` {
		t.Errorf("Content-Disposition = %q", got)
	}
	if got := caResponse.Header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q", got)
	}

	untrustedTransport := &http.Transport{
		DialContext:     listener.DialContext,
		TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool()},
	}
	untrustedClient := &http.Client{Transport: untrustedTransport, Timeout: time.Second}
	if response, err := untrustedClient.Get(baseURL + "/healthz"); err == nil {
		_ = response.Body.Close()
		t.Fatal("TLS client with an empty root pool succeeded")
	}
	untrustedTransport.CloseIdleConnections()

	plainTransport := &http.Transport{DialContext: listener.DialContext}
	plainClient := &http.Client{Transport: plainTransport, Timeout: time.Second}
	plainResponse, plainErr := plainClient.Get("http://" + c.listen + "/healthz")
	if plainErr == nil {
		defer plainResponse.Body.Close()
		if plainResponse.StatusCode == http.StatusOK {
			t.Fatal("plaintext request succeeded on the TLS listener")
		}
	}
	plainTransport.CloseIdleConnections()

	link, err := c.MintLoginLink(42)
	if err != nil {
		t.Fatal(err)
	}
	token := tokenFromLink(t, link)
	authRequest, err := http.NewRequest(http.MethodPost, baseURL+"/auth", strings.NewReader(fmt.Sprintf(`{"token":%q}`, token)))
	if err != nil {
		t.Fatal(err)
	}
	authRequest.Header.Set("Origin", config.PublicURL)
	authRequest.Header.Set("Content-Type", "application/json")
	authClient := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	authResponse, err := authClient.Do(authRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer authResponse.Body.Close()
	if authResponse.StatusCode != http.StatusSeeOther {
		body, _ := io.ReadAll(authResponse.Body)
		t.Fatalf("POST /auth status/body = %d/%q", authResponse.StatusCode, body)
	}
	cookies := authResponse.Cookies()
	if len(cookies) != 1 || !cookies[0].Secure {
		t.Fatalf("TLS session cookie = %+v", cookies)
	}
}

func TestCACertificateRouteOffAndProviderOff(t *testing.T) {
	c, _, _ := newHandlerChannel()
	response := httptest.NewRecorder()
	c.routes().ServeHTTP(response, request(http.MethodGet, "/ca.crt", "", nil, false))
	if response.Code != http.StatusNotFound {
		t.Fatalf("GET /ca.crt without TLS = %d", response.Code)
	}
	if _, _, err := c.CACertificatePEM(); err == nil || err.Error() != "web tls is not enabled" {
		t.Fatalf("CACertificatePEM without TLS error = %v", err)
	}
}

func TestTLSNonLoopbackBindsLoopbackTwinAndUsesHTTPSOrigins(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	config := Config{Listen: "100.100.10.20:8371", PublicURL: "https://100.100.10.20:8371", TLS: true}
	c, _, addresses := startTLSWithFakeListener(t, config)
	defer c.Stop()
	if got := strings.Join(addresses, ","); got != "100.100.10.20:8371,127.0.0.1:8371" {
		t.Fatalf("listen addresses = %q", got)
	}
	for _, origin := range []string{"https://127.0.0.1:8371", "https://localhost:8371", config.PublicURL} {
		if !c.allowedOrigin[origin] {
			t.Errorf("allowed origins omit %q: %v", origin, c.allowedOrigin)
		}
	}
	for _, origin := range []string{"http://127.0.0.1:8371", "http://localhost:8371"} {
		if c.allowedOrigin[origin] {
			t.Errorf("TLS allowed origins contain plaintext %q", origin)
		}
	}
}

func TestTLSStartRefusesUnreachableOrUnsafeListener(t *testing.T) {
	for _, test := range []struct {
		name   string
		config Config
		want   string
	}{
		{
			name:   "loopback cannot serve tailnet public URL",
			config: Config{Listen: "127.0.0.1:8371", PublicURL: "https://100.100.10.20:8371", TLS: true},
			want:   "phone cannot reach a loopback-only listener",
		},
		{
			name:   "all interfaces",
			config: Config{Listen: "0.0.0.0:8371", PublicURL: "https://100.100.10.20:8371", TLS: true},
			want:   "all-interfaces binds are refused",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			host := tlsTestHost(test.config)
			c := New()
			c.listenFunc = func(_, _ string) (net.Listener, error) {
				t.Fatal("refused TLS config reached listen")
				return nil, nil
			}
			err := c.Start(context.Background(), host)
			if err == nil || !strings.Contains(err.Error(), test.want) || !strings.Contains(host.logText(), test.want) {
				t.Fatalf("Start error/log = %v/%q", err, host.logText())
			}
		})
	}
}
