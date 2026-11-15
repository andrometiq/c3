package mappings

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/Andrometiq/c3/internal/channel"
)

// Validate returns nil if the MappingsFile is internally consistent, or a
// concrete error describing the first inconsistency found.
//
// Checks:
//   - schema_version is recognized.
//   - For each channel: default_group, if set, exists in groups.
//   - For each topic: its group, if set, exists in groups.
//   - For each mapping: its channel exists.
//
// This does NOT validate against Telegram (e.g. that chat_ids are real groups
// the bot has access to). Network validation lives in the channel module.
func (mf *MappingsFile) Validate() error {
	if mf == nil {
		return fmt.Errorf("mappings: nil file")
	}
	if mf.SchemaVersion != 1 {
		return fmt.Errorf("mappings: unsupported schema_version %d (want 1)", mf.SchemaVersion)
	}
	for chanName, cc := range mf.Channels {
		if chanName == channel.BoardChannelName {
			return fmt.Errorf("mappings: channel %q takes no stanza: boards run on the web channel's listener and login", chanName)
		}
		if chanName == "web" {
			if err := validateWebChannelConfig(cc); err != nil {
				return err
			}
		} else if cc.Front != (WebFront{}) {
			return fmt.Errorf("mappings: channel %q does not support front", chanName)
		}
		if cc.DefaultGroup != "" {
			if _, ok := cc.Groups[cc.DefaultGroup]; !ok {
				return fmt.Errorf("mappings: channel %q default_group %q not in groups", chanName, cc.DefaultGroup)
			}
		}
		for _, tp := range cc.Topics {
			if tp.Group == "" {
				continue
			}
			if _, ok := cc.Groups[tp.Group]; !ok {
				return fmt.Errorf("mappings: channel %q topic %q references unknown group %q", chanName, tp.Name, tp.Group)
			}
		}
	}
	for cwd, m := range mf.Mappings {
		if _, ok := mf.Channels[m.Channel]; !ok {
			return fmt.Errorf("mappings: cwd %q maps to unknown channel %q", cwd, m.Channel)
		}
	}
	return nil
}

func validateWebChannelConfig(cc ChannelConfig) error {
	if cc.BotToken != "" {
		return fmt.Errorf("mappings: channel %q does not support bot_token", "web")
	}
	if cc.DMChatID != 0 {
		return fmt.Errorf("mappings: channel %q does not support dm_chat_id", "web")
	}
	if cc.MasterUserID != 0 {
		return fmt.Errorf("mappings: channel %q must use channels.telegram.master_user_id", "web")
	}
	if cc.DefaultGroup != "" {
		return fmt.Errorf("mappings: channel %q does not support default_group", "web")
	}
	if len(cc.Groups) != 0 {
		return fmt.Errorf("mappings: channel %q does not support groups", "web")
	}
	if len(cc.Topics) != 0 {
		return fmt.Errorf("mappings: channel %q does not support topics", "web")
	}
	if cc.DebounceMS != 0 || cc.DebounceMaxMessages != 0 || cc.FallbackCooldownS != 0 || cc.STTPrefix != "" || cc.APIBaseURL != "" || len(cc.APIBaseURLs) != 0 || cc.RichInbound != nil {
		return fmt.Errorf("mappings: channel %q permits only enabled, listen, public_url, tls, and front", "web")
	}
	if err := ValidateWebFront(cc.Front); err != nil {
		return fmt.Errorf("mappings: channel %q %w", "web", err)
	}
	listen := cc.Listen
	if listen == "" {
		listen = "127.0.0.1:8371"
	}
	listenHost, port, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("mappings: channel %q listen must be host:port: %w", "web", err)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("mappings: channel %q listen has invalid port %q", "web", port)
	}
	if cc.TLS && !WebTLSListenHostAllowed(listenHost) {
		return fmt.Errorf("mappings: channel %q tls listen host must be loopback, localhost, or a Tailscale address; all-interfaces binds are refused", "web")
	}
	if cc.TLS && cc.PublicURL == "" {
		return fmt.Errorf("mappings: channel %q tls requires public_url", "web")
	}
	if cc.PublicURL != "" && !IsHTTPSOrigin(cc.PublicURL) {
		return fmt.Errorf("mappings: channel %q public_url must be https://host[:port] with no path, query, fragment, userinfo, or invalid port", "web")
	}
	return nil
}

// IsHTTPSOrigin reports whether raw is exactly https://host[:port].
func IsHTTPSOrigin(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || strings.ContainsAny(raw, "?#") {
		return false
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		return err == nil && n >= 1 && n <= 65535
	}
	return true
}

var headerNameRule = regexp.MustCompile(`^[A-Za-z0-9-]+$`)

// ValidateWebFront checks channels.web.front; the zero value means no front.
func ValidateWebFront(front WebFront) error {
	if front == (WebFront{}) {
		return nil
	}
	if !IsHTTPSOrigin(front.Origin) {
		return errors.New("front.origin must be https://host[:port] with no path, query, fragment, or userinfo")
	}
	if net.ParseIP(front.From) == nil {
		return errors.New("front.from must be the front's IP address")
	}
	if !headerNameRule.MatchString(front.IdentityHeader) {
		return errors.New("front.identity_header must be an HTTP header name")
	}
	if strings.TrimSpace(front.Operator) == "" || strings.ContainsFunc(front.Operator, unicode.IsControl) {
		return errors.New("front.operator must be the owner's identity as the front names it")
	}
	return nil
}

// WebTLSListenHostAllowed reports whether host is safe for the private-CA web listener.
func WebTLSListenHostAllowed(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || tailscaleIP(ip))
}

func tailscaleIP(ip net.IP) bool {
	tailscaleIPv4 := &net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}
	tailscaleIPv6 := &net.IPNet{IP: net.ParseIP("fd7a:115c:a1e0::"), Mask: net.CIDRMask(48, 128)}
	return tailscaleIPv4.Contains(ip) || tailscaleIPv6.Contains(ip)
}

// WebPublicURLIsPrivate reports whether a validated web public URL names a
// loopback host, Tailscale IP address, or Tailscale HTTPS name. Empty means no
// public URL.
func WebPublicURLIsPrivate(raw string) bool {
	if raw == "" {
		return true
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if host == "localhost" || strings.HasSuffix(host, ".ts.net") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || tailscaleIP(ip))
}
