package broker

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/queue"
)

func TestHandleCommand_StatusInTopic(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -1001234567890, TopicID: &tid}
	_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -1001234567890, TopicID: &tid, MessageID: 1, Text: "x", Timestamp: time.Now().Add(-2 * time.Hour)})

	host := NewBrokerHost(b, "telegram")
	in := &c3types.Inbound{Channel: "telegram", ChatID: -1001234567890, TopicID: &tid, Text: "/status"}
	reply, handled := host.HandleCommand(in)
	if !handled {
		t.Fatal("/status in a topic should be handled")
	}
	if !strings.Contains(reply, "1 queued") {
		t.Errorf("in-topic status = %q, want '1 queued'", reply)
	}
}

func TestHandleCommand_GlobalInDM(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	tid := int64(914)
	qrk := queue.RouteKey{Channel: "telegram", ChatID: -1001234567890, TopicID: &tid}
	_ = b.Queue.Append(qrk, &c3types.Inbound{Channel: "telegram", ChatID: -1001234567890, TopicID: &tid, MessageID: 1, Text: "x", Timestamp: time.Now()})

	host := NewBrokerHost(b, "telegram")
	in := &c3types.Inbound{Channel: "telegram", ChatID: 555, TopicID: nil, Text: "/status"} // DM
	reply, handled := host.HandleCommand(in)
	if !handled {
		t.Fatal("/status in DM should be handled")
	}
	if !strings.Contains(reply, "Broker up") {
		t.Errorf("global status = %q, want 'Broker up'", reply)
	}
}

// TestStatusForTopic_LiveHolderReadsAttached: a connected, PID-alive holder must
// render the surface name (not a generic "CLI") + " attached".
func TestStatusForTopic_LiveHolderReadsAttached(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	live := &Stub{CLI: "claude", PID: os.Getpid(), ConnID: 1, Conn: struct{}{}}
	if _, ok := b.Routes.Claim(key, live); !ok {
		t.Fatal("claim should succeed")
	}
	got := b.statusForTopic("telegram", -1001234567890, &tid)
	if !strings.Contains(got, "· Claude Code attached · broker up") {
		t.Fatalf("live claude holder should read 'Claude Code attached', got %q", got)
	}
}

// TestStatusForTopic_NamesTheSurface: /status names the ACTUAL attached surface,
// not a generic "CLI" — a Desktop/cowork session (adapter CLI id "desktop") must
// read "Claude Desktop attached", not "CLI attached" (operator report 2026-07-24).
func TestStatusForTopic_NamesTheSurface(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(915)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	live := &Stub{CLI: "desktop", PID: os.Getpid(), ConnID: 1, Conn: struct{}{}}
	if _, ok := b.Routes.Claim(key, live); !ok {
		t.Fatal("claim should succeed")
	}
	got := b.statusForTopic("telegram", -1001234567890, &tid)
	if !strings.Contains(got, "· Claude Desktop attached · broker up") {
		t.Fatalf("desktop holder should read 'Claude Desktop attached', got %q", got)
	}
	if strings.Contains(got, "CLI attached") {
		t.Fatalf("must not render a generic 'CLI attached' for a desktop session, got %q", got)
	}
}

// TestStatusForTopic_ReapsDeadHolderAtReadTime: a dead reference (disconnected +
// PID gone) must render "nothing attached" AND be reaped from the routes map at
// read time — not linger until the next inbound sweeps it. This is the exact bug
// the operator hit: /status reported "attached" for a session that had exited.
func TestStatusForTopic_ReapsDeadHolderAtReadTime(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()

	tid := int64(914)
	key := MakeRouteKey("telegram", -1001234567890, &tid)
	// Conn nil (disconnected) AND PID<=0 => IsAlive()==false: a dead reference.
	dead := &Stub{CLI: "claude", PID: 0, ConnID: 2}
	if _, ok := b.Routes.Claim(key, dead); !ok {
		t.Fatal("claim should succeed")
	}
	got := b.statusForTopic("telegram", -1001234567890, &tid)
	if !strings.Contains(got, "· nothing attached · broker up") {
		t.Fatalf("dead holder must read 'nothing attached', got %q", got)
	}
	if _, held := b.Routes.Holder(key); held {
		t.Fatal("dead holder must be reaped from the routes map at read time")
	}
}

func TestHandleCommand_NonStatusNotHandled(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	host := NewBrokerHost(b, "telegram")
	if _, handled := host.HandleCommand(&c3types.Inbound{Channel: "telegram", Text: "hello"}); handled {
		t.Error("non-command text must not be handled")
	}
}
