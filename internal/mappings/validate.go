package mappings

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
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
		if chanName == "web" {
			if err := validateWebChannelConfig(cc); err != nil {
				return err
			}
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
		return fmt.Errorf("mappings: channel %q permits only enabled, listen, and public_url", "web")
	}
	if cc.Listen != "" {
		_, port, err := net.SplitHostPort(cc.Listen)
		if err != nil {
			return fmt.Errorf("mappings: channel %q listen must be host:port: %w", "web", err)
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("mappings: channel %q listen has invalid port %q", "web", port)
		}
	}
	if cc.PublicURL == "" {
		return nil
	}
	u, err := url.Parse(cc.PublicURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || strings.ContainsAny(cc.PublicURL, "?#") {
		return fmt.Errorf("mappings: channel %q public_url must be https://host[:port] with no path, query, fragment, or userinfo", "web")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("mappings: channel %q public_url has invalid port %q", "web", port)
		}
	}
	return nil
}

// WebPublicURLIsPrivate reports whether a validated web public URL names a
// loopback host or a Tailscale HTTPS name. Empty means no public URL.
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
	return ip != nil && ip.IsLoopback()
}
