package broker

import (
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

func TestResolveSwarmChannel(t *testing.T) {
	b := New(&mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {
				BotToken: "primary",
				Bots: map[string]mappings.BotConfig{
					"glm":  {BotToken: "g"},
					"grok": {BotToken: "k"},
				},
			},
		},
	})
	got, err := b.resolveSwarmChannel("telegram", "", "grok")
	if err != nil || got != "telegram:grok" {
		t.Fatalf("cli family grok → %q %v, want telegram:grok", got, err)
	}
	got, err = b.resolveSwarmChannel("telegram", "glm", "grok")
	if err != nil || got != "telegram:glm" {
		t.Fatalf("explicit bot=glm → %q %v, want telegram:glm", got, err)
	}
	if _, err := b.resolveSwarmChannel("telegram", "nope", "grok"); err == nil {
		t.Fatal("unknown explicit bot must fail")
	}
	got, err = b.resolveSwarmChannel("telegram", "", "claude")
	if err != nil || got != "telegram" {
		t.Fatalf("unmatched CLI stays on primary, got %q %v", got, err)
	}
}

func TestResolveSwarmChannel_NoBots(t *testing.T) {
	b := New(&mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {BotToken: "primary"},
		},
	})
	got, err := b.resolveSwarmChannel("telegram", "", "grok")
	if err != nil || got != "telegram" {
		t.Fatalf("no bots → primary, got %q %v", got, err)
	}
	if _, err := b.resolveSwarmChannel("telegram", "glm", "grok"); err == nil {
		t.Fatal("bot= with empty bots map must fail")
	}
}

func TestStampSwarm(t *testing.T) {
	b := New(&mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {BotToken: "t", Bots: map[string]mappings.BotConfig{"glm": {BotToken: "g"}}},
		},
	})
	msg := ipc.AttachedMsg{OK: true, Channel: "telegram:glm"}
	b.stampSwarm(&msg)
	if !msg.Swarm {
		t.Fatal("extra-bot attach must set Swarm")
	}
	plain := New(&mappings.MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]mappings.ChannelConfig{"telegram": {BotToken: "t"}},
	})
	msg2 := ipc.AttachedMsg{OK: true, Channel: "telegram"}
	plain.stampSwarm(&msg2)
	if msg2.Swarm {
		t.Fatal("single-bot attach must not set Swarm")
	}
}
