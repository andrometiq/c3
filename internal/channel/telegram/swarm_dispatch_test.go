package telegram

import (
	"testing"

	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/swarm"
)

func swarmChannel(h *fakeHost) *Channel {
	c := makeChannel(h)
	c.cfg.Bots = map[string]mappings.BotConfig{"glm": {BotToken: "tok"}}
	c.swarmStore = swarm.NewStore()
	c.botUsername.Store("glm_bot")
	return c
}

func TestSwarmDispatch_DropsUntilTagged(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := swarmChannel(h)
	c.dispatchMessage(1, textMsg("hello everyone", 7), false, nil)
	if h.emitCount() != 0 {
		t.Fatalf("untagged traffic must not emit, got %d", h.emitCount())
	}
	c.dispatchMessage(2, textMsg("@glm_bot run the tests", 7), false, nil)
	if h.emitCount() != 1 {
		t.Fatalf("mention must emit, got %d", h.emitCount())
	}
	c.dispatchMessage(3, textMsg("and then lint", 7), false, nil)
	if h.emitCount() != 2 {
		t.Fatalf("sticky follow-up must emit, got %d", h.emitCount())
	}
}

func TestSwarmDispatch_MuteStopsListening(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := swarmChannel(h)
	c.dispatchMessage(1, textMsg("@glm_bot hi", 7), false, nil)
	c.dispatchMessage(2, textMsg("/mute", 7), false, nil)
	if h.emitCount() != 1 {
		t.Fatalf("/mute must not emit as agent inbound, got %d", h.emitCount())
	}
	c.dispatchMessage(3, textMsg("still here?", 7), false, nil)
	if h.emitCount() != 1 {
		t.Fatalf("after /mute untagged traffic must drop, got %d", h.emitCount())
	}
}

func TestSwarmDispatch_DisabledWithoutBots(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannel(h)
	c.swarmStore = swarm.NewStore()
	c.dispatchMessage(1, textMsg("hello everyone", 7), false, nil)
	if h.emitCount() != 1 {
		t.Fatalf("single-bot C3 must still emit untagged messages, got %d", h.emitCount())
	}
}
