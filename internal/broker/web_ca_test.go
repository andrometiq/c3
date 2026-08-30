package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

type certificateWebChannel struct {
	*webFakeChannel
	certificate []byte
	fingerprint string
	err         error
}

func (channel *certificateWebChannel) CACertificatePEM() ([]byte, string, error) {
	return append([]byte(nil), channel.certificate...), channel.fingerprint, channel.err
}

type caCaptureTelegramChannel struct {
	*fakeChannel
	mu       sync.Mutex
	reply    c3types.ReplyArgs
	contents []byte
	readErr  error
}

func (channel *caCaptureTelegramChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	channel.mu.Lock()
	defer channel.mu.Unlock()
	channel.reply = args
	if len(args.Media) == 1 {
		channel.contents, channel.readErr = os.ReadFile(args.Media[0].Path)
	}
	return 1, nil
}

func requestWebCA(t *testing.T, broker *Broker) ipc.WebCAReply {
	t.Helper()
	peer, done := peerPair(t, broker)
	defer done()
	helloAck(t, peer, "/workspace/project")
	if err := peer.WriteJSON(ipc.WebCAReq{Op: ipc.OpWebCA}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var reply ipc.WebCAReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatal(err)
	}
	return reply
}

func TestWebCAIPCDeliversCertificateFileToOperator(t *testing.T) {
	broker, telegram, webChannel := brokerWithWeb(t, nil)
	defer broker.Shutdown()
	const fingerprint = "AA:BB:CC:DD"
	certificate := []byte("-----BEGIN CERTIFICATE-----\npublic-ca\n-----END CERTIFICATE-----\n")
	provider := &certificateWebChannel{
		webFakeChannel: webChannel,
		certificate:    certificate,
		fingerprint:    fingerprint,
	}
	capture := &caCaptureTelegramChannel{fakeChannel: telegram}
	broker.chMu.Lock()
	broker.channels["web"].Channel = provider
	broker.channels["telegram"].Channel = capture
	broker.chMu.Unlock()
	mappings := broker.Mappings().Clone()
	webConfig := mappings.Channels["web"]
	webConfig.TLS = true
	webConfig.PublicURL = "https://100.100.10.20:8371"
	mappings.Channels["web"] = webConfig
	broker.SetMappings(mappings)

	reply := requestWebCA(t, broker)
	if !reply.OK || reply.Op != ipc.OpWebCAReply || reply.Fingerprint != fingerprint || reply.Err != "" {
		t.Fatalf("web_ca reply = %+v", reply)
	}
	capture.mu.Lock()
	delivered := capture.reply
	contents := append([]byte(nil), capture.contents...)
	readErr := capture.readErr
	capture.mu.Unlock()
	if readErr != nil || !bytes.Equal(contents, certificate) {
		t.Fatalf("certificate path was not readable during Telegram send: data=%q err=%v", contents, readErr)
	}
	if delivered.Channel != "telegram" || delivered.ChatID != 42 || delivered.Markup != c3types.MarkupNone || len(delivered.Media) != 1 {
		t.Fatalf("Telegram CA delivery = %+v", delivered)
	}
	media := delivered.Media[0]
	if media.Kind != c3types.MediaFile || filepath.Base(media.Path) != "c3-web-ca.crt" {
		t.Fatalf("CA media item = %+v", media)
	}
	for _, wanted := range []string{fingerprint, "Install it once per phone", "https://100.100.10.20:8371", "If you did not request this, ignore it."} {
		if !strings.Contains(media.Caption, wanted) {
			t.Errorf("CA caption omits %q: %q", wanted, media.Caption)
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		_, err := os.Stat(media.Path)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("temporary CA directory was not removed: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWebCAIPCRefusesWhenTLSIsOff(t *testing.T) {
	broker, _, _ := brokerWithWeb(t, nil)
	defer broker.Shutdown()
	reply := requestWebCA(t, broker)
	if reply.OK || reply.Op != ipc.OpWebCAReply || reply.Err != "web tls is not enabled" || reply.Fingerprint != "" {
		t.Fatalf("web_ca TLS-off reply = %+v", reply)
	}
}

func TestBrokerHostWebLoginLinkWithTLSRecordsDebounce(t *testing.T) {
	broker, telegram, webChannel := brokerWithWeb(t, nil)
	defer broker.Shutdown()
	provider := &certificateWebChannel{
		webFakeChannel: webChannel,
		certificate:    []byte("public CA"),
		fingerprint:    "11:22:33:44",
	}
	broker.chMu.Lock()
	broker.channels["web"].Channel = provider
	broker.chMu.Unlock()

	host := NewBrokerHost(broker, "web")
	sent, err := host.SendWebLoginLink("requested from the web login page")
	if err != nil || !sent {
		t.Fatalf("first TLS login-page request: sent=%v err=%v", sent, err)
	}
	sent, err = host.SendWebLoginLink("requested from the web login page")
	if err != nil || sent {
		t.Fatalf("second TLS login-page request: sent=%v err=%v", sent, err)
	}
	if got := webChannel.mintCount(); got != 1 {
		t.Fatalf("TLS login-page mint count=%d, want 1", got)
	}
	if got := len(telegram.sendRepliesSnapshot()); got != 1 {
		t.Fatalf("TLS login-page Telegram sends=%d, want 1", got)
	}
}

func TestWebTLSAttachGuidanceAndStatusFingerprint(t *testing.T) {
	broker, _, webChannel := brokerWithWeb(t, nil)
	defer broker.Shutdown()
	provider := &certificateWebChannel{
		webFakeChannel: webChannel,
		certificate:    []byte("public CA"),
		fingerprint:    "11:22:33:44",
	}
	broker.chMu.Lock()
	broker.channels["web"].Channel = provider
	broker.chMu.Unlock()

	peer, done := peerPair(t, broker)
	defer done()
	helloAck(t, peer, "/workspace/project")
	attached := readAttach(t, peer, ipc.AttachReq{Op: ipc.OpAttach, Expr: "web"})
	if !attached.OK || !strings.Contains(attached.Notice, `Phone browsers need the C3 web CA installed once — run "c3-broker web ca" to send it.`) {
		t.Fatalf("TLS web attach guidance = %+v", attached)
	}

	a, z := net.Pipe()
	defer a.Close()
	defer z.Close()
	go broker.handleHealth(ipc.NewConn(z))
	raw, err := ipc.NewConn(a).ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var health ipc.HealthListMsg
	if err := json.Unmarshal(raw, &health); err != nil {
		t.Fatal(err)
	}
	if health.WebCAFingerprint != provider.fingerprint {
		t.Fatalf("health web CA fingerprint = %q", health.WebCAFingerprint)
	}
}
