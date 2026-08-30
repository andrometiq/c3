// Package web provides C3's private, browser-based text channel.
package web

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/mappings"
)

const (
	Name                 = "web"
	defaultListen        = "127.0.0.1:8371"
	maxMessageRunes      = 16000
	postBodyLimit        = 64 << 10
	loginTokenTTL        = 10 * time.Minute
	sessionIdleTTL       = 24 * time.Hour
	clientMessageTTL     = 10 * time.Minute
	loginLinkDebounce    = 60 * time.Second
	loginLinkHourlyLimit = 5
	replayLimit          = 200
	maxFailedAuthDelay   = 5 * time.Second
)

var errUnsupported = errors.New("web: operation unsupported")

//go:embed *.html
var pages embed.FS

// Config is the channels.web stanza.
type Config struct {
	Enabled   *bool  `json:"enabled,omitempty"`
	Listen    string `json:"listen,omitempty"`
	PublicURL string `json:"public_url,omitempty"`
	TLS       bool   `json:"tls,omitempty"`
}

type telegramConfig struct {
	MasterUserID int64 `json:"master_user_id"`
	DMChatID     int64 `json:"dm_chat_id"`
}

type registrationHost interface {
	ChannelRegistered(name string) bool
}

type allowlistHost interface {
	UserAllowed(userID int64) bool
}

type queuedIDHost interface {
	MaxQueuedMessageID(channelName string, chatID int64, topicID *int64) (int64, error)
}

type loginDeliveryHost interface {
	SendWebLoginLink(requestedBy string) (sent bool, err error)
}

// Channel implements channel.Channel, channel.LoginLinker, and
// channel.CertificateProvider.
type Channel struct {
	host       channel.Host
	cfg        Config
	operatorID int64

	lifecycleMu   sync.Mutex
	server        *http.Server
	listener      net.Listener
	listeners     []net.Listener
	cancel        context.CancelFunc
	listen        string
	baseURL       string
	allowedOrigin map[string]bool

	idMu          sync.Mutex
	nextMessageID int64

	authMu            sync.Mutex
	tokens            map[string]loginToken
	sessions          map[string]*session
	failedAuth        map[string]time.Time
	loginLinkTimes    []time.Time
	loginLinkInFlight bool

	clientMu       sync.Mutex
	clientMessages map[clientMessageKey]*clientMessage

	streamMu sync.Mutex
	streams  map[string]map[*streamClient]struct{}

	replayMu    sync.Mutex
	replay      []streamEvent
	nextEventID int64

	now                func() time.Time
	sleep              func(time.Duration)
	failedAttemptDelay time.Duration
	heartbeatInterval  time.Duration
	listenFunc         func(network, address string) (net.Listener, error)

	tlsMu       sync.RWMutex
	tlsMaterial certificateMaterial
}

var _ channel.Channel = (*Channel)(nil)
var _ channel.LoginLinker = (*Channel)(nil)
var _ channel.CertificateProvider = (*Channel)(nil)

// New returns an unstarted channel.
func New() *Channel {
	return &Channel{
		tokens:             make(map[string]loginToken),
		sessions:           make(map[string]*session),
		failedAuth:         make(map[string]time.Time),
		clientMessages:     make(map[clientMessageKey]*clientMessage),
		streams:            make(map[string]map[*streamClient]struct{}),
		now:                time.Now,
		sleep:              time.Sleep,
		failedAttemptDelay: 500 * time.Millisecond,
		heartbeatInterval:  20 * time.Second,
		allowedOrigin:      make(map[string]bool),
		listenFunc:         net.Listen,
	}
}

func (c *Channel) Name() string { return Name }

func (c *Channel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{
		Channel: Name, RichText: false, MaxMessageRunes: maxMessageRunes,
		EditMessages: true, Typing: true, MediaKinds: []c3types.MediaKind{},
	}
}

// Start validates the private listener and operator identity before accepting
// any browser traffic.
func (c *Channel) Start(ctx context.Context, host channel.Host) error {
	if host == nil {
		return errors.New("web: nil host")
	}
	c.ensureState()
	registered, ok := host.(registrationHost)
	if !ok || !registered.ChannelRegistered("telegram") {
		return c.refuse(host, "telegram must be registered before web")
	}
	if err := host.Config(Name, &c.cfg); err != nil {
		return c.refuse(host, "read config: %v", err)
	}
	if err := validateConfig(c.cfg); err != nil {
		return c.refuse(host, "%v", err)
	}
	var telegram telegramConfig
	if err := host.Config("telegram", &telegram); err != nil {
		return c.refuse(host, "read telegram operator config: %v", err)
	}
	if telegram.MasterUserID == 0 {
		return c.refuse(host, "channels.telegram.master_user_id is required")
	}
	allowed, ok := host.(allowlistHost)
	if !ok || !allowed.UserAllowed(telegram.MasterUserID) {
		return c.refuse(host, "channels.telegram.master_user_id is not allowlisted")
	}
	if telegram.DMChatID == 0 {
		host.Logf("web: WARNING channels.telegram.dm_chat_id is 0; login links cannot be delivered")
	}

	listen := c.cfg.Listen
	if listen == "" {
		listen = defaultListen
	}
	hostName, _, _ := net.SplitHostPort(listen)
	if !isLoopbackHost(hostName) {
		host.Logf("web: WARNING listen=%s is non-loopback; the web session can drive an agent on this machine", listen)
	}
	if !mappings.WebPublicURLIsPrivate(c.cfg.PublicURL) {
		host.Logf("web: WARNING public_url host is neither loopback nor *.ts.net; keep the agent-driving surface private")
	}

	if seed, ok := host.(queuedIDHost); ok {
		maxID, err := seed.MaxQueuedMessageID(Name, telegram.MasterUserID, nil)
		if err != nil {
			return c.refuse(host, "read held queue for message-id seed: %v", err)
		}
		c.nextMessageID = maxID
	}
	configuredHost, configuredPort, _ := net.SplitHostPort(listen)
	if c.cfg.TLS {
		if c.cfg.PublicURL == "" {
			return c.refuse(host, "tls requires public_url")
		}
		if !mappings.WebTLSListenHostAllowed(configuredHost) {
			return c.refuse(host, "tls listen host must be loopback, localhost, or a Tailscale address; all-interfaces binds are refused")
		}
		publicURL, _ := url.Parse(c.cfg.PublicURL)
		if isLoopbackHost(configuredHost) && !isLoopbackHost(publicURL.Hostname()) {
			return c.refuse(host, "phone cannot reach a loopback-only listener")
		}
	}
	listener, err := c.listenFunc("tcp", listen)
	if err != nil {
		return c.refuse(host, "listen %s: %v", listen, err)
	}
	listeners := []net.Listener{listener}

	actualListen := listener.Addr().String()
	_, actualPort, _ := net.SplitHostPort(actualListen)
	if configuredHost == "" {
		configuredHost = "127.0.0.1"
	}
	if c.cfg.TLS && !isLoopbackHost(configuredHost) {
		loopbackListen := net.JoinHostPort("127.0.0.1", actualPort)
		loopbackListener, err := c.listenFunc("tcp", loopbackListen)
		if err != nil {
			_ = listener.Close()
			return c.refuse(host, "listen %s: %v", loopbackListen, err)
		}
		listeners = append(listeners, loopbackListener)
	}
	caGenerated := false
	if c.cfg.TLS {
		caGenerated, err = c.prepareTLS(net.JoinHostPort(configuredHost, configuredPort))
		if err != nil {
			for _, current := range listeners {
				_ = current.Close()
			}
			return c.refuse(host, "tls: %v", err)
		}
	}
	baseURL := c.cfg.PublicURL
	if baseURL == "" {
		baseURL = "http://" + net.JoinHostPort(configuredHost, actualPort)
	}
	scheme := "http"
	if c.cfg.TLS {
		scheme = "https"
	}
	allowedOrigins := map[string]bool{
		normalizeOrigin(scheme + "://" + net.JoinHostPort("127.0.0.1", actualPort)): true,
		normalizeOrigin(scheme + "://" + net.JoinHostPort("localhost", actualPort)): true,
	}
	if c.cfg.PublicURL != "" {
		allowedOrigins[normalizeOrigin(c.cfg.PublicURL)] = true
	}

	childContext, cancel := context.WithCancel(ctx)
	server := &http.Server{
		Handler:           c.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	if c.cfg.TLS {
		server.TLSConfig = c.tlsConfig()
	}
	c.lifecycleMu.Lock()
	c.host = host
	c.operatorID = telegram.MasterUserID
	c.listener = listener
	c.listeners = listeners
	c.server = server
	c.cancel = cancel
	c.listen = actualListen
	c.baseURL = strings.TrimSuffix(baseURL, "/")
	c.allowedOrigin = allowedOrigins
	c.lifecycleMu.Unlock()

	serve := func(listener net.Listener) {
		var err error
		if c.cfg.TLS {
			err = server.ServeTLS(listener, "", "")
		} else {
			err = server.Serve(listener)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			host.Logf("web: listener stopped: %v", err)
		}
	}
	for _, current := range listeners {
		go serve(current)
	}
	go func() {
		<-childContext.Done()
		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownContext)
		c.closeAllStreams()
	}()
	if c.cfg.TLS {
		host.Logf("web: listening https://%s (tls: private CA sha256 %s)", actualListen, c.caFingerprint())
		if caGenerated {
			host.Logf("web: new CA generated — run 'c3-broker web ca' to send it to the operator")
		}
	} else {
		host.Logf("web: listening on %s", actualListen)
	}
	return nil
}

func (c *Channel) refuse(host channel.Host, format string, args ...any) error {
	err := fmt.Errorf("web: "+format, args...)
	host.Logf("%v", err)
	return err
}

func (c *Channel) Stop() error {
	c.lifecycleMu.Lock()
	server, cancel := c.server, c.cancel
	listeners := append([]net.Listener(nil), c.listeners...)
	c.server = nil
	c.listener = nil
	c.listeners = nil
	c.cancel = nil
	c.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if server == nil {
		for _, listener := range listeners {
			_ = listener.Close()
		}
		return nil
	}
	ctx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	err := server.Shutdown(ctx)
	for _, listener := range listeners {
		_ = listener.Close()
	}
	c.closeAllStreams()
	return err
}

func (c *Channel) nextID() int64 {
	c.idMu.Lock()
	defer c.idMu.Unlock()
	c.nextMessageID++
	return c.nextMessageID
}

func (c *Channel) ensureState() {
	if c.now == nil {
		c.now = time.Now
	}
	if c.sleep == nil {
		c.sleep = time.Sleep
	}
	if c.failedAttemptDelay == 0 {
		c.failedAttemptDelay = 500 * time.Millisecond
	}
	if c.heartbeatInterval == 0 {
		c.heartbeatInterval = 20 * time.Second
	}
	if c.tokens == nil {
		c.tokens = make(map[string]loginToken)
	}
	if c.sessions == nil {
		c.sessions = make(map[string]*session)
	}
	if c.failedAuth == nil {
		c.failedAuth = make(map[string]time.Time)
	}
	if c.clientMessages == nil {
		c.clientMessages = make(map[clientMessageKey]*clientMessage)
	}
	if c.streams == nil {
		c.streams = make(map[string]map[*streamClient]struct{})
	}
	if c.allowedOrigin == nil {
		c.allowedOrigin = make(map[string]bool)
	}
	if c.listenFunc == nil {
		c.listenFunc = net.Listen
	}
}

func validateConfig(cfg Config) error {
	listen := cfg.Listen
	if listen == "" {
		listen = defaultListen
	}
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("listen must be host:port: %w", err)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("listen has invalid port %q", port)
	}
	if cfg.PublicURL == "" {
		return nil
	}
	u, err := url.Parse(cfg.PublicURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || strings.ContainsAny(cfg.PublicURL, "?#") {
		return errors.New("public_url must be https://host[:port] with no path, query, fragment, or userinfo")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("public_url has invalid port %q", port)
		}
	}
	return nil
}

func isLoopbackHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
