package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/buildidentity"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

type registrationChannel struct {
	name     string
	startErr error
	started  bool
	mu       sync.Mutex
	replies  []c3types.ReplyArgs
	mints    int
}

func (c *registrationChannel) Name() string { return c.name }
func (c *registrationChannel) Start(context.Context, channel.Host) error {
	c.started = true
	return c.startErr
}
func (*registrationChannel) Stop() error { return nil }
func (c *registrationChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{Channel: c.name, Threads: c.name == "telegram"}
}
func (c *registrationChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replies = append(c.replies, args)
	return 1, nil
}
func (*registrationChannel) SendTyping(int64, *int64) error { return nil }
func (*registrationChannel) EditMessage(c3types.EditArgs) (*c3types.EditResult, error) {
	return &c3types.EditResult{}, nil
}
func (*registrationChannel) React(c3types.ReactArgs) error             { return nil }
func (*registrationChannel) DownloadAttachment(string) (string, error) { return "", nil }
func (*registrationChannel) StopPoll(int64, int64) (*c3types.PollResult, error) {
	return nil, errors.New("unsupported")
}
func (*registrationChannel) CreateTopic(int64, string) (int64, error) {
	return 0, errors.New("unsupported")
}
func (*registrationChannel) ValidateTopic(int64, int64) error { return nil }
func (*registrationChannel) HasLiveSession(int64) bool        { return false }
func (c *registrationChannel) MintLoginLink(int64) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mints++
	return "https://device.example/auth#test-token", nil
}
func (c *registrationChannel) replySnapshot() []c3types.ReplyArgs {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]c3types.ReplyArgs(nil), c.replies...)
}

func registrationMappings(webEnabled *bool) *mappings.MappingsFile {
	return &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {BotToken: "test", MasterUserID: 42, DMChatID: 42},
			"web":      {Enabled: webEnabled},
		},
		Mappings:  map[string]mappings.Mapping{},
		Allowlist: &mappings.Allowlist{Users: []int64{42}},
	}
}

// Channel-registration fixtures must not acquire installed-adapter or queue
// dependencies if they later add hello/status assertions.
func newRegistrationBroker(t *testing.T, mf *mappings.MappingsFile) *broker.Broker {
	t.Helper()
	path := "/usr/bin" + string(os.PathListSeparator) + "/bin"
	if runtime.GOOS == "windows" {
		path = filepath.Join(os.Getenv("SystemRoot"), "System32")
	}
	t.Setenv("PATH", path)
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	return broker.New(mf, broker.WithInstalledAdapterLookup(func() (buildidentity.Installed, error) { return buildidentity.Installed{}, nil }))
}

func TestRegisterConfiguredChannelsWebFailureKeepsTelegram(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	b := newRegistrationBroker(t, registrationMappings(nil))
	defer b.Shutdown()
	telegramChannel := &registrationChannel{name: "telegram"}
	webChannel := &registrationChannel{name: "web", startErr: errors.New("listen: address in use")}
	var constructed []string
	registerConfiguredChannels(b, b.Mappings(),
		func() channel.Channel {
			constructed = append(constructed, "telegram")
			return telegramChannel
		},
		func() channel.Channel {
			constructed = append(constructed, "web")
			return webChannel
		},
	)
	if strings.Join(constructed, ",") != "telegram,web" {
		t.Fatalf("construction order=%v, want telegram then web", constructed)
	}
	if _, err := b.Channel("telegram"); err != nil {
		t.Fatalf("Telegram was not kept after web Start failed: %v", err)
	}
	if _, err := b.Channel("web"); err == nil {
		t.Fatal("failed web channel was recorded as registered")
	}
	if !telegramChannel.started || !webChannel.started {
		t.Fatalf("registration order did not attempt both channels: telegram=%v web=%v", telegramChannel.started, webChannel.started)
	}
}

func TestRegisterConfiguredChannelsRegistersEnabledWebAfterTelegram(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	b := newRegistrationBroker(t, registrationMappings(nil))
	defer b.Shutdown()
	telegramChannel := &registrationChannel{name: "telegram"}
	webChannel := &registrationChannel{name: "web"}
	var order []string
	registerConfiguredChannels(b, b.Mappings(),
		func() channel.Channel {
			order = append(order, "telegram")
			return telegramChannel
		},
		func() channel.Channel {
			if _, err := b.Channel("telegram"); err != nil {
				t.Fatalf("web factory ran before Telegram registered: %v", err)
			}
			order = append(order, "web")
			return webChannel
		},
	)
	if strings.Join(order, ",") != "telegram,web" {
		t.Fatalf("registration order=%v", order)
	}
	if _, err := b.Channel("telegram"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Channel("web"); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterConfiguredChannelsDoesNotConstructDisabledWeb(t *testing.T) {
	disabled := false
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	b := newRegistrationBroker(t, registrationMappings(&disabled))
	defer b.Shutdown()
	constructed := 0
	registerConfiguredChannels(b, b.Mappings(),
		func() channel.Channel { return &registrationChannel{name: "telegram"} },
		func() channel.Channel {
			constructed++
			return &registrationChannel{name: "web"}
		},
	)
	if constructed != 0 {
		t.Fatalf("disabled web factory called %d times", constructed)
	}
}

func TestRunStatusPrintsWebListener(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	mf := registrationMappings(nil)
	webConfig := mf.Channels["web"]
	webConfig.PublicURL = "https://device.ts.net"
	mf.Channels["web"] = webConfig
	path, err := mappings.DefaultPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := mappings.Write(path, mf); err != nil {
		t.Fatal(err)
	}
	oldHealth, oldClaims := statusFetchHealth, statusFetchClaims
	statusFetchHealth = func() (*ipc.HealthListMsg, error) { return &ipc.HealthListMsg{}, nil }
	statusFetchClaims = func() (*ipc.ClaimsListMsg, error) {
		return &ipc.ClaimsListMsg{Claims: []ipc.ClaimEntry{
			{Channel: "web", ChatID: 42, HolderCLI: "claude", HolderPID: 7, ConnID: 9, Connected: true, IsOutput: true},
			{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 281, TopicName: "c3", HolderCLI: "claude", HolderPID: 7, ConnID: 9, Connected: true},
		}}, nil
	}
	t.Cleanup(func() { statusFetchHealth, statusFetchClaims = oldHealth, oldClaims })
	out := captureStdout(t, func() {
		if err := runStatus(); err != nil {
			t.Errorf("runStatus: %v", err)
		}
	})
	for _, want := range []string{"web", `listen="127.0.0.1:8371"`, `public_url="https://device.ts.net"`, "web/42/dm", "held by claude pid 7", "[output]", "[input]"} {
		if !strings.Contains(out, want) {
			t.Errorf("status missing %q:\n%s", want, out)
		}
	}
}

func TestRunWebLinkUsesDaemonWritePath(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	b := newRegistrationBroker(t, registrationMappings(nil))
	defer b.Shutdown()
	telegramChannel := &registrationChannel{name: "telegram"}
	webChannel := &registrationChannel{name: "web"}
	if err := b.RegisterChannel(telegramChannel); err != nil {
		t.Fatal(err)
	}
	if err := b.RegisterChannel(webChannel); err != nil {
		t.Fatal(err)
	}
	socketPath, err := broker.SocketPath()
	if err != nil {
		t.Fatal(err)
	}
	server, err := broker.Listen(socketPath, b)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	out := captureStdout(t, func() {
		if err := runWeb([]string{"link"}); err != nil {
			t.Errorf("runWeb: %v", err)
		}
	})
	if !strings.Contains(out, "sent to the configured Telegram operator DM") {
		t.Fatalf("runWeb output = %q", out)
	}
	replies := telegramChannel.replySnapshot()
	if len(replies) != 1 || !replies[0].DisableLinkPreview || !strings.Contains(replies[0].Text, "c3-broker web link") {
		t.Fatalf("web link Telegram delivery = %+v", replies)
	}
}
