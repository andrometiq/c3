package broker

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
)

const webLoginLinkDebounce = 60 * time.Second

type webLoginDelivery int

const (
	webLoginSent webLoginDelivery = iota
	webLoginAlreadyLive
	webLoginDebounced
)

func (b *Broker) webOperatorRoute() (RouteKey, int64, error) {
	telegramConfig, ok := b.Mappings().Channels["telegram"]
	if !ok || telegramConfig.MasterUserID == 0 {
		return RouteKey{}, 0, fmt.Errorf("channels.telegram.master_user_id is not configured")
	}
	allowed := false
	for _, userID := range b.Mappings().AllowlistOrEmpty().Users {
		if userID == telegramConfig.MasterUserID {
			allowed = true
			break
		}
	}
	if !allowed {
		return RouteKey{}, 0, fmt.Errorf("channels.telegram.master_user_id is not allowlisted")
	}
	if telegramConfig.DMChatID == 0 {
		return RouteKey{}, 0, fmt.Errorf("channels.telegram.dm_chat_id is not configured")
	}
	return MakeRouteKey("web", telegramConfig.MasterUserID, nil), telegramConfig.DMChatID, nil
}

func (b *Broker) sendWebLoginLink(stub *Stub, requestedBy string, skipLive, debounce bool) (webLoginDelivery, error) {
	key, dmChatID, err := b.webOperatorRoute()
	if err != nil {
		return webLoginSent, err
	}
	webChannel, err := b.Channel("web")
	if err != nil {
		return webLoginSent, err
	}
	linker, ok := webChannel.(channel.LoginLinker)
	if !ok {
		return webLoginSent, fmt.Errorf("web channel does not support login links")
	}

	b.loginLinkMu.Lock()
	if skipLive && linker.HasLiveSession(key.ChatID) {
		b.loginLinkMu.Unlock()
		return webLoginAlreadyLive, nil
	}
	now := time.Now()
	if debounce && now.Sub(b.loginLinkLast[key.ChatID]) < webLoginLinkDebounce {
		b.loginLinkMu.Unlock()
		return webLoginDebounced, nil
	}
	link, err := linker.MintLoginLink(key.ChatID)
	if err != nil {
		b.loginLinkMu.Unlock()
		return webLoginSent, fmt.Errorf("mint web login link: %w", err)
	}
	previous, hadPrevious := b.loginLinkLast[key.ChatID]
	b.loginLinkLast[key.ChatID] = now
	b.loginLinkMu.Unlock()

	telegramChannel, err := b.Channel("telegram")
	if err != nil {
		b.rollbackWebLoginLinkTimestamp(key.ChatID, now, previous, hadPrevious)
		return webLoginSent, fmt.Errorf("telegram channel is required to deliver web login links: %w", err)
	}
	text := webLoginLinkText(stub, requestedBy, link)
	if _, err := telegramChannel.SendReply(c3types.ReplyArgs{
		Channel: "telegram", ChatID: dmChatID, Text: text,
		Markup: c3types.MarkupNone, DisableLinkPreview: true,
	}); err != nil {
		b.rollbackWebLoginLinkTimestamp(key.ChatID, now, previous, hadPrevious)
		return webLoginSent, fmt.Errorf("send web login link through telegram: %w", err)
	}
	return webLoginSent, nil
}

func (b *Broker) webTLSEnabled() bool {
	webChannel, err := b.Channel("web")
	if err != nil {
		return false
	}
	provider, ok := webChannel.(channel.CertificateProvider)
	if !ok {
		return false
	}
	_, _, err = provider.CACertificatePEM()
	return err == nil
}

func (b *Broker) rollbackWebLoginLinkTimestamp(userID int64, reservedAt, previous time.Time, hadPrevious bool) {
	b.loginLinkMu.Lock()
	defer b.loginLinkMu.Unlock()
	if b.loginLinkLast[userID] != reservedAt {
		return
	}
	if hadPrevious {
		b.loginLinkLast[userID] = previous
	} else {
		delete(b.loginLinkLast, userID)
	}
}

func webLoginLinkText(stub *Stub, requestedBy, link string) string {
	if stub == nil {
		return fmt.Sprintf("🔐 C3 web login\nRequested by: %s\n%s\n\nIf you did not request this, ignore it.", requestedBy, link)
	}
	cli := stub.CLI
	if cli == "" {
		cli = "unknown"
	}
	cwd := stub.CWD
	if cwd == "" {
		cwd = "(not reported)"
	}
	sessionID := stub.StableSessionIDValue()
	if sessionID == "" {
		sessionID = "(not reported)"
	}
	return fmt.Sprintf("🔐 C3 web login\nCLI: %s\nCWD: %s\nSession ID: %s\n%s\n\nIf you did not request this, ignore it.",
		cli, cwd, sessionID, link)
}

func webAttachGuidance(delivery webLoginDelivery, err error, tlsEnabled bool) string {
	prefix := ""
	switch {
	case err != nil:
		log.Printf("web attach: login-link delivery failed: %v", err)
		prefix = "The web route was claimed, but its Telegram login link could not be sent; run `c3-broker web link`."
	case delivery == webLoginAlreadyLive:
		prefix = "The existing authenticated web session is ready."
	case delivery == webLoginDebounced:
		prefix = "A web login link was sent to your Telegram DM recently."
	default:
		prefix = "A web login link was sent to your Telegram DM."
	}
	guidance := []string{
		prefix,
		"Use reply-tool (\"Telegram\") mode so `reply` lands on the claimed web route.",
		"Permission prompts and `ask` must be answered at the laptop; use pre-approved permissions for an on-the-go drive.",
	}
	if tlsEnabled {
		guidance = append(guidance, `Phone browsers need the C3 web CA installed once — run "c3-broker web ca" to send it.`)
	}
	return strings.Join(guidance, " ")
}

func (b *Broker) handleWebLoginLink(conn *ipc.Conn) {
	_, err := b.sendWebLoginLink(nil, "c3-broker web link", false, false)
	resp := ipc.WebLoginLinkReply{Op: ipc.OpWebLoginLinkReply, OK: err == nil}
	if err != nil {
		resp.Err = err.Error()
	}
	_ = conn.WriteJSON(resp)
}

func (b *Broker) handleWebCA(conn *ipc.Conn) {
	_, dmChatID, err := b.webOperatorRoute()
	if err != nil {
		_ = conn.WriteJSON(ipc.WebCAReply{Op: ipc.OpWebCAReply, Err: err.Error()})
		return
	}
	webChannel, err := b.Channel("web")
	if err != nil {
		_ = conn.WriteJSON(ipc.WebCAReply{Op: ipc.OpWebCAReply, Err: "web tls is not enabled"})
		return
	}
	provider, ok := webChannel.(channel.CertificateProvider)
	if !ok {
		_ = conn.WriteJSON(ipc.WebCAReply{Op: ipc.OpWebCAReply, Err: "web tls is not enabled"})
		return
	}
	certificatePEM, fingerprint, err := provider.CACertificatePEM()
	if err != nil {
		_ = conn.WriteJSON(ipc.WebCAReply{Op: ipc.OpWebCAReply, Err: "web tls is not enabled"})
		return
	}
	directory, err := os.MkdirTemp("", "c3-web-ca-*")
	if err != nil {
		_ = conn.WriteJSON(ipc.WebCAReply{Op: ipc.OpWebCAReply, Err: fmt.Sprintf("create temporary CA directory: %v", err)})
		return
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "c3-web-ca.crt")
	if err := os.WriteFile(path, certificatePEM, 0o644); err != nil {
		_ = conn.WriteJSON(ipc.WebCAReply{Op: ipc.OpWebCAReply, Err: fmt.Sprintf("write temporary CA certificate: %v", err)})
		return
	}
	publicURL := b.Mappings().Channels["web"].PublicURL
	caption := fmt.Sprintf(`C3 web certificate — SHA-256 %s. Install it once per phone. Android: open the file, install as a CA certificate (the "network may be monitored" notice is expected). iPhone: install the profile, then Settings → General → About → Certificate Trust Settings → enable full trust. Then open %s. If you did not request this, ignore it.`, fingerprint, publicURL)
	telegramChannel, err := b.Channel("telegram")
	if err != nil {
		_ = conn.WriteJSON(ipc.WebCAReply{Op: ipc.OpWebCAReply, Err: fmt.Sprintf("telegram channel is required to deliver the web CA: %v", err)})
		return
	}
	if _, err := telegramChannel.SendReply(c3types.ReplyArgs{
		Channel: "telegram",
		ChatID:  dmChatID,
		Markup:  c3types.MarkupNone,
		Media: []c3types.MediaItem{{
			Kind: c3types.MediaFile, Path: path, Caption: caption,
		}},
	}); err != nil {
		_ = conn.WriteJSON(ipc.WebCAReply{Op: ipc.OpWebCAReply, Err: fmt.Sprintf("send web CA through telegram: %v", err)})
		return
	}
	_ = conn.WriteJSON(ipc.WebCAReply{Op: ipc.OpWebCAReply, OK: true, Fingerprint: fingerprint})
}
