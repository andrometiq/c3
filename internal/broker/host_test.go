package broker

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/mappings"
)

func TestBrokerHost_ConfigUnmarshal(t *testing.T) {
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {BotToken: "tok", DefaultGroup: "main"},
		},
	}
	b := New(mf)
	defer b.Shutdown()

	host := NewBrokerHost(b, "telegram")
	type tgCfg struct {
		BotToken     string `json:"bot_token"`
		DefaultGroup string `json:"default_group"`
	}
	var cfg tgCfg
	if err := host.Config("telegram", &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.BotToken != "tok" || cfg.DefaultGroup != "main" {
		t.Errorf("got %+v, want BotToken=tok DefaultGroup=main", cfg)
	}
}

func TestBrokerHost_ConfigMissingChannel(t *testing.T) {
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	host := NewBrokerHost(b, "telegram")
	var cfg struct{}
	err := host.Config("telegram", &cfg)
	if err == nil || !strings.Contains(err.Error(), "mappings.json") {
		t.Errorf("expected missing-channel error mentioning mappings.json, got %v", err)
	}
}

func TestBrokerHost_EmitSubmitsToWorker(t *testing.T) {
	b := New(&mappings.MappingsFile{SchemaVersion: 1})
	defer b.Shutdown()

	host := NewBrokerHost(b, "telegram")
	id := int64(281)
	host.Emit(&c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: &id})

	if b.Workers.Active() == 0 {
		t.Error("expected a worker active after Emit")
	}
}
