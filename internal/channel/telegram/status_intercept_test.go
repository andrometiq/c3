package telegram

import (
	"testing"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

func TestIsBrokerCommand(t *testing.T) {
	c := &Channel{}
	cases := map[string]bool{
		"/status":                 true,
		" /status ":               true,
		"/STATUS":                 true,
		"/status@c3bot":           true,
		"/statusly":               false,
		"hello":                   false,
		"please /status":          false,
		"/queue":                  true,
		"/QUEUE":                  true,
		"/queue genie":            true,
		"/queue@c3bot genie 26":   true, // A5: @ stripped from the FIRST token only
		"/queuex":                 false,
		"/drain":                  true,
		"/drain genie first 10":   true,
		"/drain@c3bot g all to x": true,
		"/drainx":                 false,
		"please /drain x all":     false,
		"":                        false,
	}
	for text, want := range cases {
		if got := c.isBrokerCommand(text); got != want {
			t.Errorf("isBrokerCommand(%q) = %v, want %v", text, got, want)
		}
	}
}

// Spec invariant: an intercepted "/status" must NEVER be Emitted (routed to an
// agent) — the broker handles it and the channel returns early. Drive
// dispatchMessage directly the same way the existing dispatch_gate_test.go cases
// do (hand-built *gotgbot.Message), with a fakeHost whose HandleCommand claims
// "/status". The recover guards the intercept's SendReply call (the bare
// makeChannel has a nil bot); the assertion that matters is Emit==0.
func TestStatusCommand_NotRouted(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, cmdHandled: true}
	c := makeChannel(h)
	func() {
		defer func() { _ = recover() }() // tolerate nil-bot SendReply in the intercept
		c.dispatchMessage(1, textMsg("/status", 42), false, nil)
	}()
	if got := h.emitCount(); got != 0 {
		t.Errorf("/status must not be routed: Emit called %d times, want 0", got)
	}
}

// INV-7 / B9: a handled command with an EMPTY reply (the operator-gate silent
// drop, or an async /drain//queue <q> that posts its own reply later) must send
// ZERO bytes back from the intercept and must not be routed. The bare
// makeChannel has a nil bot, so ANY SendReply attempt would panic — running
// dispatchMessage WITHOUT a recover wrapper is the zero-bytes-on-wire proof.
func TestBrokerCommand_EmptyReply_SkipsSendAndDoesNotRoute(t *testing.T) {
	h := &fakeHost{
		decision: channel.GateInboundAllow,
		cmdFn:    func(*c3types.Inbound) (string, bool) { return "", true }, // silent drop / async
	}
	c := makeChannel(h)
	c.dispatchMessage(1, textMsg("/drain genie all", 42), false, nil) // no recover: a send would panic
	if got := h.emitCount(); got != 0 {
		t.Errorf("silently-handled /drain must not be routed: Emit called %d times, want 0", got)
	}
	if !h.handleCommandCalled() {
		t.Error("HandleCommand should have been consulted for an allowlisted /drain")
	}
}

// A6: a media CAPTION that reads like a command must NOT be intercepted — the
// attachment would be silently swallowed. The message routes normally instead
// (command-in-caption is unsupported in v1).
func TestBrokerCommand_CaptionWithAttachment_NotIntercepted(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow, cmdFn: func(*c3types.Inbound) (string, bool) { return "", true }}
	c := makeChannel(h)
	msg := &gotgbot.Message{
		MessageId: 101,
		From:      &gotgbot.User{Id: 42},
		Chat:      gotgbot.Chat{Id: 42},
		Date:      1715151931,
		Voice:     &gotgbot.Voice{FileId: "f-1", Duration: 3},
		Caption:   "/drain genie all",
	}
	c.dispatchMessage(2, msg, false, nil)
	if h.handleCommandCalled() {
		t.Error("caption command must not reach HandleCommand when attachments are present (A6)")
	}
	if got := h.emitCount(); got != 1 {
		t.Errorf("caption+attachment must route normally: Emit called %d times, want 1", got)
	}
}

// I-SEC gate-first ordering: a stranger's /drain dies at the gate — the command
// handler is never consulted and nothing is emitted or replied.
func TestBrokerCommand_StrangerDrain_DiesAtGate(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundDrop}
	c := makeChannel(h)
	c.dispatchMessage(3, textMsg("/drain genie all", 99), false, nil) // no recover: a send would panic
	if h.handleCommandCalled() {
		t.Error("a stranger's /drain must never reach HandleCommand (gate-first)")
	}
	if got := h.emitCount(); got != 0 {
		t.Errorf("Emit called %d times for gated /drain; want 0", got)
	}
}
