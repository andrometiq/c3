package broker

import (
	"encoding/json"
	"net"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// TestAttach_RefusesChannelThatIsConfiguredButNotRunning pins the difference
// between a channel mappings.json DECLARES and a channel the broker actually
// STARTED.
//
// The defect it guards: every attach path validated chanName against
// mappings.json and none of them checked b.Channel(name), so attach returned OK
// on a channel with no transport. The session believed it was attached, the
// claim registry agreed, and no message moved in either direction — the worst
// shape of failure, because everything reports success.
//
// This is reachable in production, not just in tests. cmd/c3-broker/main.go
// registers the Telegram channel only when the stanza carries a bot_token;
// otherwise it prints "running without inbound transport" and carries on. So a
// user who adds the channel stanza before pasting the token lands here — and
// "but it's right there in my mappings.json" is exactly what they would check
// first, which is why the refusal has to name that distinction out loud.
func TestAttach_RefusesChannelThatIsConfiguredButNotRunning(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	mf := emptyMappings()
	mf.Channels["telegram"] = mappings.ChannelConfig{
		DefaultGroup: "main",
		Groups:       map[string]mappings.GroupConfig{"main": {ChatID: -100}},
		DMChatID:     42,
	}
	// New() and NOT brokerWithChannel(): configured, deliberately not registered.
	br := New(mf)
	defer br.Shutdown()

	a, b := net.Pipe()
	go br.HandleConn(a)
	peer := ipc.NewConn(b)
	defer peer.Close()

	if err := peer.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 1, CWD: "/x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, Target: "dm"}); err != nil {
		t.Fatal(err)
	}
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var ack ipc.AttachedMsg
	if err := json.Unmarshal(raw, &ack); err != nil {
		t.Fatal(err)
	}

	if ack.OK {
		t.Fatalf("attach SUCCEEDED on a channel that is configured but has no running transport: the session "+
			"now believes it holds a route through which nothing can be sent or received, and every surface "+
			"reports success (ack=%+v)", ack)
	}
	if got := len(br.Routes.Snapshot()); got != 0 {
		t.Errorf("a refused attach still left %d claim(s) in the routes table — the topic is now blocked for "+
			"a session that could actually serve it", got)
	}
	for _, want := range []string{"configured", "not running"} {
		if !strings.Contains(ack.Err, want) {
			t.Errorf("the refusal does not name the configured-vs-running distinction (missing %q), which is "+
				"the one thing the user needs to understand; Err=%q", want, ack.Err)
		}
	}
}
