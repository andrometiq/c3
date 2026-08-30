package web

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	caKeyFile      = "ca.key"
	caCertFile     = "ca.crt"
	serverKeyFile  = "server.key"
	serverCertFile = "server.crt"
)

type certificateMaterial struct {
	caPEM       []byte
	certificate tls.Certificate
	fingerprint string
}

type subjectAltNames struct {
	dnsNames    []string
	ipAddresses []net.IP
	dnsSet      map[string]struct{}
	ipSet       map[string]struct{}
}

func (c *Channel) prepareTLS(listen string) (bool, error) {
	stateHome, err := xdgStateHomeC3()
	if err != nil {
		return false, err
	}
	directory := filepath.Join(stateHome, "web")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return false, fmt.Errorf("create certificate directory %s: %w", directory, err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return false, fmt.Errorf("secure certificate directory %s: %w", directory, err)
	}
	now := time.Now()
	caKey, caCertificate, caPEM, generated, err := loadOrCreateCA(directory, now)
	if err != nil {
		return false, err
	}
	sans, commonName, err := derivedSubjectAltNames(listen, c.cfg.PublicURL)
	if err != nil {
		return false, err
	}
	leafPEM, leafKeyPEM, leafCertificate, err := loadOrCreateLeaf(directory, now, caKey, caCertificate, sans, commonName)
	if err != nil {
		return false, err
	}
	chainPEM := make([]byte, 0, len(leafPEM)+len(caPEM))
	chainPEM = append(chainPEM, leafPEM...)
	chainPEM = append(chainPEM, caPEM...)
	certificate, err := tls.X509KeyPair(chainPEM, leafKeyPEM)
	if err != nil {
		return false, fmt.Errorf("load server certificate chain: %w", err)
	}
	certificate.Leaf = leafCertificate

	c.tlsMu.Lock()
	c.tlsMaterial = certificateMaterial{
		caPEM:       append([]byte(nil), caPEM...),
		certificate: certificate,
		fingerprint: certificateFingerprint(caCertificate),
	}
	c.tlsMu.Unlock()
	return generated, nil
}

func loadOrCreateCA(directory string, now time.Time) (*ecdsa.PrivateKey, *x509.Certificate, []byte, bool, error) {
	keyPath := filepath.Join(directory, caKeyFile)
	certPath := filepath.Join(directory, caCertFile)
	_, keyErr := os.Stat(keyPath)
	_, certErr := os.Stat(certPath)
	if errors.Is(keyErr, os.ErrNotExist) && errors.Is(certErr, os.ErrNotExist) {
		key, certificate, certificatePEM, err := generateCA(directory, now)
		return key, certificate, certificatePEM, err == nil, err
	}
	if keyErr != nil || certErr != nil {
		return nil, nil, nil, false, fmt.Errorf("private CA is incomplete or unreadable (ca.key: %v; ca.crt: %v); refusing to replace the trusted CA", keyErr, certErr)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("read private CA key: %w; refusing to replace the trusted CA", err)
	}
	certificatePEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("read private CA certificate: %w; refusing to replace the trusted CA", err)
	}
	key, err := parseECDSAPrivateKey(keyPEM)
	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("parse private CA key: %w; refusing to replace the trusted CA", err)
	}
	certificate, err := parseCertificate(certificatePEM)
	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("parse private CA certificate: %w; refusing to replace the trusted CA", err)
	}
	if !certificate.IsCA || !certificate.BasicConstraintsValid || certificate.KeyUsage&x509.KeyUsageCertSign == 0 {
		return nil, nil, nil, false, errors.New("private CA certificate is not a certificate authority; refusing to replace the trusted CA")
	}
	if now.Before(certificate.NotBefore) || !now.Before(certificate.NotAfter) {
		return nil, nil, nil, false, errors.New("private CA certificate is outside its validity period; refusing to replace the trusted CA")
	}
	if !publicKeysMatch(&key.PublicKey, certificate.PublicKey) {
		return nil, nil, nil, false, errors.New("private CA key does not match ca.crt; refusing to replace the trusted CA")
	}
	return key, certificate, certificatePEM, false, nil
}

func generateCA(directory string, now time.Time) (*ecdsa.PrivateKey, *x509.Certificate, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate private CA key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate private CA serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "C3 web CA"},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.AddDate(10, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create private CA certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal private CA key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := atomicWriteCertificatePair(
		filepath.Join(directory, caCertFile), certificatePEM, 0o644,
		filepath.Join(directory, caKeyFile), keyPEM, 0o600,
	); err != nil {
		return nil, nil, nil, err
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse generated private CA certificate: %w", err)
	}
	return key, certificate, certificatePEM, nil
}

func loadOrCreateLeaf(directory string, now time.Time, caKey *ecdsa.PrivateKey, caCertificate *x509.Certificate, sans subjectAltNames, commonName string) ([]byte, []byte, *x509.Certificate, error) {
	certificatePath := filepath.Join(directory, serverCertFile)
	keyPath := filepath.Join(directory, serverKeyFile)
	certificatePEM, certificateErr := os.ReadFile(certificatePath)
	keyPEM, keyErr := os.ReadFile(keyPath)
	if certificateErr == nil && keyErr == nil {
		certificate, certificateParseErr := parseCertificate(certificatePEM)
		key, keyParseErr := parseECDSAPrivateKey(keyPEM)
		if certificateParseErr == nil && keyParseErr == nil &&
			publicKeysMatch(&key.PublicKey, certificate.PublicKey) &&
			certificate.CheckSignatureFrom(caCertificate) == nil &&
			certificate.NotAfter.After(now.Add(30*24*time.Hour)) &&
			sans.equalCertificate(certificate) {
			return certificatePEM, keyPEM, certificate, nil
		}
	}
	return generateLeaf(directory, now, caKey, caCertificate, sans, commonName)
}

func generateLeaf(directory string, now time.Time, caKey *ecdsa.PrivateKey, caCertificate *x509.Certificate, sans subjectAltNames, commonName string) ([]byte, []byte, *x509.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate server key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate server certificate serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     sans.dnsNames,
		IPAddresses:  sans.ipAddresses,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCertificate, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create server certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal server key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := atomicWriteCertificatePair(
		filepath.Join(directory, serverCertFile), certificatePEM, 0o644,
		filepath.Join(directory, serverKeyFile), keyPEM, 0o600,
	); err != nil {
		return nil, nil, nil, err
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse generated server certificate: %w", err)
	}
	return certificatePEM, keyPEM, certificate, nil
}

func derivedSubjectAltNames(listen, publicURL string) (subjectAltNames, string, error) {
	sans := subjectAltNames{dnsSet: make(map[string]struct{}), ipSet: make(map[string]struct{})}
	sans.addHost("localhost")
	sans.addHost("127.0.0.1")
	sans.addHost("::1")
	listenHost, _, err := net.SplitHostPort(listen)
	if err != nil {
		return subjectAltNames{}, "", fmt.Errorf("derive listen certificate name: %w", err)
	}
	sans.addHost(listenHost)
	parsedPublicURL, err := url.Parse(publicURL)
	if err != nil || parsedPublicURL.Hostname() == "" {
		return subjectAltNames{}, "", errors.New("derive public_url certificate name: missing host")
	}
	commonName := parsedPublicURL.Hostname()
	sans.addHost(commonName)
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		sans.addHost(hostname)
	}
	return sans, commonName, nil
}

func (sans *subjectAltNames) addHost(host string) {
	host = strings.TrimSuffix(strings.TrimSpace(host), ".")
	if host == "" {
		return
	}
	if ip := net.ParseIP(host); ip != nil {
		key := ip.String()
		if _, exists := sans.ipSet[key]; exists {
			return
		}
		sans.ipSet[key] = struct{}{}
		sans.ipAddresses = append(sans.ipAddresses, ip)
		return
	}
	host = strings.ToLower(host)
	if _, exists := sans.dnsSet[host]; exists {
		return
	}
	sans.dnsSet[host] = struct{}{}
	sans.dnsNames = append(sans.dnsNames, host)
}

func (sans subjectAltNames) equalCertificate(certificate *x509.Certificate) bool {
	if len(certificate.DNSNames) != len(sans.dnsSet) || len(certificate.IPAddresses) != len(sans.ipSet) {
		return false
	}
	for _, name := range certificate.DNSNames {
		if _, exists := sans.dnsSet[strings.ToLower(strings.TrimSuffix(name, "."))]; !exists {
			return false
		}
	}
	for _, ip := range certificate.IPAddresses {
		if _, exists := sans.ipSet[ip.String()]; !exists {
			return false
		}
	}
	return true
}

func parseECDSAPrivateKey(data []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, errors.New("expected PKCS#8 PRIVATE KEY PEM")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok || key.Curve != elliptic.P256() {
		return nil, errors.New("expected ECDSA P-256 private key")
	}
	return key, nil
}

func parseCertificate(data []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("expected CERTIFICATE PEM")
	}
	return x509.ParseCertificate(block.Bytes)
}

func publicKeysMatch(left, right any) bool {
	leftDER, leftErr := x509.MarshalPKIXPublicKey(left)
	rightDER, rightErr := x509.MarshalPKIXPublicKey(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftDER, rightDER)
}

func randomSerial() (*big.Int, error) {
	for {
		serialBytes := make([]byte, 16)
		if _, err := rand.Read(serialBytes); err != nil {
			return nil, err
		}
		serial := new(big.Int).SetBytes(serialBytes)
		if serial.Sign() > 0 {
			return serial, nil
		}
	}
}

func certificateFingerprint(certificate *x509.Certificate) string {
	digest := sha256.Sum256(certificate.Raw)
	hexadecimal := fmt.Sprintf("%X", digest)
	parts := make([]string, 0, len(hexadecimal)/2)
	for index := 0; index < len(hexadecimal); index += 2 {
		parts = append(parts, hexadecimal[index:index+2])
	}
	return strings.Join(parts, ":")
}

func atomicWriteCertificateFile(path string, data []byte, mode os.FileMode) error {
	temporaryPath, err := prepareCertificateFile(path, data, mode)
	if err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		_ = os.Remove(temporaryPath)
		return fmt.Errorf("replace certificate file %s: %w", path, err)
	}
	if err := syncCertificateDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync certificate directory: %w", err)
	}
	return nil
}

func atomicWriteCertificatePair(certificatePath string, certificate []byte, certificateMode os.FileMode, keyPath string, key []byte, keyMode os.FileMode) error {
	temporaryKey, err := prepareCertificateFile(keyPath, key, keyMode)
	if err != nil {
		return err
	}
	removeTemporaryKey := func() { _ = os.Remove(temporaryKey) }
	temporaryCertificate, err := prepareCertificateFile(certificatePath, certificate, certificateMode)
	if err != nil {
		removeTemporaryKey()
		return err
	}
	removeTemporaryCertificate := func() { _ = os.Remove(temporaryCertificate) }

	if err := os.Rename(temporaryCertificate, certificatePath); err != nil {
		removeTemporaryCertificate()
		removeTemporaryKey()
		return fmt.Errorf("replace certificate file %s: %w", certificatePath, err)
	}
	if err := syncCertificateDirectory(filepath.Dir(certificatePath)); err != nil {
		removeTemporaryKey()
		_ = removePublishedCertificate(certificatePath)
		return fmt.Errorf("sync certificate directory after replacing %s: %w", certificatePath, err)
	}
	if err := os.Rename(temporaryKey, keyPath); err != nil {
		removeTemporaryKey()
		if cleanupErr := removePublishedCertificate(certificatePath); cleanupErr != nil {
			return fmt.Errorf("replace certificate key %s: %v; remove new certificate: %w", keyPath, err, cleanupErr)
		}
		return fmt.Errorf("replace certificate key %s: %w", keyPath, err)
	}
	if err := syncCertificateDirectory(filepath.Dir(keyPath)); err != nil {
		return fmt.Errorf("sync certificate directory after replacing %s: %w", keyPath, err)
	}
	return nil
}

func prepareCertificateFile(path string, data []byte, mode os.FileMode) (string, error) {
	temporary, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return "", fmt.Errorf("create temporary certificate file: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanup := func() { _ = os.Remove(temporaryPath) }
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		cleanup()
		return "", fmt.Errorf("chmod temporary certificate file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		cleanup()
		return "", fmt.Errorf("write temporary certificate file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		cleanup()
		return "", fmt.Errorf("sync temporary certificate file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		cleanup()
		return "", fmt.Errorf("close temporary certificate file: %w", err)
	}
	return temporaryPath, nil
}

func removePublishedCertificate(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncCertificateDirectory(filepath.Dir(path))
}

func syncCertificateDirectory(directory string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer directoryHandle.Close()
	return directoryHandle.Sync()
}

// xdgStateHomeC3 returns $XDG_STATE_HOME/c3 (or ~/.local/state/c3 fallback).
func xdgStateHomeC3() (string, error) {
	if x := os.Getenv("XDG_STATE_HOME"); x != "" {
		return filepath.Join(x, "c3"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user state directory: %w", err)
	}
	return filepath.Join(home, ".local", "state", "c3"), nil
}

func (c *Channel) caCertificatePEM() []byte {
	c.tlsMu.RLock()
	defer c.tlsMu.RUnlock()
	return append([]byte(nil), c.tlsMaterial.caPEM...)
}

func (c *Channel) caFingerprint() string {
	c.tlsMu.RLock()
	defer c.tlsMu.RUnlock()
	return c.tlsMaterial.fingerprint
}

func (c *Channel) tlsConfig() *tls.Config {
	c.tlsMu.RLock()
	defer c.tlsMu.RUnlock()
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{c.tlsMaterial.certificate},
		NextProtos:   []string{"h2", "http/1.1"},
	}
}

// CACertificatePEM returns the private-CA certificate and its SHA-256
// fingerprint for operator installation. It never returns a private key.
func (c *Channel) CACertificatePEM() ([]byte, string, error) {
	if !c.cfg.TLS {
		return nil, "", errors.New("web tls is not enabled")
	}
	certificate := c.caCertificatePEM()
	fingerprint := c.caFingerprint()
	if len(certificate) == 0 || fingerprint == "" {
		return nil, "", errors.New("web tls certificate is unavailable")
	}
	return certificate, fingerprint, nil
}
