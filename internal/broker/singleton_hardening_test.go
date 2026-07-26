package broker

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// dupNameChannel is a channel.Channel whose Name() is settable and whose
// Start/Stop are observable. It embeds the shared fakeChannel for the ~12
// interface methods this file does not care about.
type dupNameChannel struct {
	*fakeChannel
	name    string
	started bool
	stopped bool
}

func (d *dupNameChannel) Name() string { return d.name }
func (d *dupNameChannel) Start(context.Context, channel.Host) error {
	d.started = true
	return nil
}
func (d *dupNameChannel) Stop() error {
	d.stopped = true
	return nil
}

// TestRegisterChannel_RefusesDuplicateName pins the rule that RegisterChannel
// must never DISPLACE an already-registered name.
//
// The defect it guards: RegisterChannel used to Start() the newcomer and then
// plain-assign it over the incumbent's map entry. The incumbent's poll
// goroutine, heartbeat and health machine are already live at that point, but
// it is now unreachable via Channel() and invisible to ShutdownWithin's
// `for _, reg := range b.channels { reg.Channel.Stop() }` — so it is never
// stopped and keeps issuing getUpdates on its bot token. Two pollers on one
// token is a 409 wedge, and RegisterChannel returned nil while it happened.
func TestRegisterChannel_RefusesDuplicateName(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	b := New(mfWithTelegram())
	defer b.Shutdown()

	incumbent := &dupNameChannel{fakeChannel: &fakeChannel{}, name: "telegram"}
	if err := b.RegisterChannel(incumbent); err != nil {
		t.Fatalf("first RegisterChannel failed: %v", err)
	}

	newcomer := &dupNameChannel{fakeChannel: &fakeChannel{}, name: "telegram"}
	err := b.RegisterChannel(newcomer)

	if err == nil {
		t.Errorf("RegisterChannel ACCEPTED a duplicate name: the incumbent %q is now displaced from b.channels while still polling — unreachable via Channel() and never Stop()ed at shutdown, i.e. two pollers on one bot token", newcomer.name)
	}
	if newcomer.started {
		t.Errorf("the duplicate channel was Start()ed before being refused: it has connected upstream and is now a second live poller nothing holds a reference to")
	}
	got, gotErr := b.Channel("telegram")
	if gotErr != nil {
		t.Fatalf("Channel(%q) lost the incumbent registration entirely: %v", "telegram", gotErr)
	}
	if got != channel.Channel(incumbent) {
		t.Errorf("Channel(%q) returns the DUPLICATE, not the incumbent: the running channel was silently displaced", "telegram")
	}
}

// TestStateRoot_AgreesWithAndWithoutXDG pins that the plugin state root is ONE
// directory regardless of whether the broker was spawned with XDG_STATE_HOME
// set.
//
// The defect it guards: the fallback branch returned ~/.local/state/c3 while
// the env branch returned $XDG_STATE_HOME/c3/state. $XDG_STATE_HOME defaults to
// ~/.local/state, so the same logical directory resolved two ways — a broker
// spawned by an adapter that inherited the env and one spawned from a bare
// shell read DIFFERENT plugin state, and Load() cannot tell "never saved" from
// "looking in the wrong place". The env branch is the documented contract
// (internal/plugin/host.go: "$XDG_STATE_HOME/c3/state/<plugin-name>/"), so the
// second assertion pins that shape rather than mere internal agreement.
func TestStateRoot_AgreesWithAndWithoutXDG(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	// Empty means "unset" to stateRoot's `x != ""` check, and t.Setenv restores
	// whatever the developer's environment had.
	t.Setenv("XDG_STATE_HOME", "")
	withoutXDG := stateRoot()
	withoutXDGCache := cacheRoot()

	t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".local", "state"))
	withXDG := stateRoot()

	if withoutXDG != withXDG {
		t.Errorf("stateRoot() resolves ONE logical directory to two paths depending on env: without XDG_STATE_HOME=%q, with XDG_STATE_HOME=%q — a broker spawned with a different env silently reads a different plugin state dir", withoutXDG, withXDG)
	}
	if want := filepath.Join("c3", "state"); !strings.HasSuffix(withXDG, want) {
		t.Errorf("stateRoot()=%q does not end in %q — it no longer matches the documented plugin contract $XDG_STATE_HOME/c3/state/<plugin-name>/ (internal/plugin/host.go)", withXDG, want)
	}

	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, ".cache"))
	if withCacheXDG := cacheRoot(); withCacheXDG != withoutXDGCache {
		t.Errorf("cacheRoot() has regressed into the same env divergence stateRoot had: without XDG_CACHE_HOME=%q, with XDG_CACHE_HOME=%q", withoutXDGCache, withCacheXDG)
	}
}

// mfWithTwoChannels is mfWithTelegram plus a second channel carrying a topic of
// the SAME name — the ambiguity defaultChannel used to resolve by coin flip.
func mfWithTwoChannels() *mappings.MappingsFile {
	mf := mfWithTelegram()
	mf.Channels["slack"] = mappings.ChannelConfig{
		DefaultGroup: "main",
		Groups:       map[string]mappings.GroupConfig{"main": {ChatID: -100}},
		DMChatID:     43,
		Topics:       []mappings.Topic{{ChatID: -100, TopicID: 281, Name: "c3", Group: "main"}},
	}
	return mf
}

// TestDefaultChannel_AmbiguousRefusesToGuess pins that with 2+ configured
// channels the broker refuses to pick one rather than coin-flipping.
//
// The defect it guards: defaultChannel() was `for name := range …Channels {
// return name }`. Go randomises map iteration order, so it returned a DIFFERENT
// channel per call, and no adapter sets AttachReq.Channel — so it decided the
// channel for EVERY attach. The coin flip is not cosmetic: the queue file name,
// the saved cwd default and the capability manifest are all keyed on it, so the
// session gets claimed on one channel while its inbound is queued under the
// other, silently.
func TestDefaultChannel_AmbiguousRefusesToGuess(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	b := New(mfWithTwoChannels())
	defer b.Shutdown()

	for i := 0; i < 100; i++ {
		if got := b.defaultChannel(); got != "" {
			t.Fatalf("defaultChannel() GUESSED %q on call %d with 2 channels configured: map iteration order picks the winner, so the session binds to a channel the user never named (call it again and it may answer differently)", got, i+1)
		}
	}
}

// TestAttach_AmbiguousChannelIsReportedNotGuessed is the wire-level half of the
// same defect: an attach that cannot resolve a channel must FAIL with advice
// that names the real problem, not silently claim a coin-flipped route and not
// render the canned "run c3-broker setup" (there IS a channel configured — two
// of them).
func TestAttach_AmbiguousChannelIsReportedNotGuessed(t *testing.T) {
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTwoChannels(), fc)
	defer b.Shutdown()

	peer, done := peerPair(t, b)
	defer done()
	helloAck(t, peer, "/x")

	if err := peer.WriteJSON(ipc.AttachReq{Op: ipc.OpAttach, Name: "c3"}); err != nil {
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
		t.Fatalf("attach SUCCEEDED with two channels configured and none named: it claimed channel=%q by coin flip — inbound for the other channel queues to a file this session never reads", ack.Channel)
	}
	// The remedy must be one the user can actually perform. AttachReq has a
	// Channel field, but no shipped adapter exposes it in its attach tool schema,
	// so "pass channel=<name>" is advice nobody can follow — the refusal has to
	// name the configured channels and point at the file the user can edit.
	for _, want := range []string{"slack", "telegram", "mappings.json"} {
		if !strings.Contains(ack.Err, want) {
			t.Errorf("ambiguous attach refusal is missing %q, so the user cannot act on it; Err=%q (status=%q)", want, ack.Err, ack.Status)
		}
	}
	if strings.Contains(ack.Err, "pass channel=") {
		t.Errorf("the refusal tells the user to pass channel=<name>, which NO adapter's attach schema exposes — a refusal whose remedy cannot be performed is worse than the guess it replaced; Err=%q", ack.Err)
	}
	if ack.Status == ipc.AttachStatusNoTopicsConfigured {
		t.Errorf("ambiguous attach reported Status=%q, which format.go renders as \"run c3-broker setup\" — misleading when mappings.json already configures two channels", ack.Status)
	}
}

// TestDefaultChannel_SingleChannelUnchanged is the regression guard: the fix
// must not make the ordinary one-channel install ambiguous.
func TestDefaultChannel_SingleChannelUnchanged(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	b := New(mfWithTelegram())
	defer b.Shutdown()

	if got := b.defaultChannel(); got != "telegram" {
		t.Errorf("defaultChannel()=%q with exactly one channel configured, want \"telegram\": the ambiguity guard has broken the normal single-channel install", got)
	}
}

// TestPersistMapping_RefusesCrossChannelCwdRebind pins that the cwd→topic
// rebind guard treats the CHANNEL as part of the saved identity.
//
// The defect it guards: the guard compared only (ChatID, TopicID). Chat ids and
// topic ids are per-channel namespaces, so two channels can carry numerically
// identical ones — and a numerically-matching attach on a DIFFERENT channel
// sailed past the refusal and rewrote the saved default's Channel. That saved
// channel is what the queue file, the recovery entry and the capability
// manifest key on, so the wrong binding becomes sticky across restarts.
func TestPersistMapping_RefusesCrossChannelCwdRebind(t *testing.T) {
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mfWithTwoChannels(), fc)
	defer b.Shutdown()

	// Basename == topic name, so resolveAttachCWD returns cwd unchanged.
	cwd := filepath.Join(t.TempDir(), "c3")
	stub := &Stub{CLI: "claude", PID: 1, CWD: cwd}

	b.persistMapping(stub, "telegram", -100, 281, "c3", "main")
	if got, ok := b.Mappings().LookupByCwd(cwd); !ok || got.Channel != "telegram" {
		t.Fatalf("setup: cwd default not saved under telegram (ok=%v, channel=%q)", ok, got.Channel)
	}

	// Same chat id, same topic id, DIFFERENT channel.
	b.persistMapping(stub, "slack", -100, 281, "c3", "main")

	got, ok := b.Mappings().LookupByCwd(cwd)
	if !ok {
		t.Fatalf("cwd default disappeared after the second persistMapping")
	}
	if got.Channel != "telegram" {
		t.Errorf("the cwd rebind guard let a default REBIND ACROSS CHANNELS (telegram → %q) because it compares only chat_id/topic_id, which are per-channel namespaces; the saved channel keys the queue file, the recovery entry and the capability manifest", got.Channel)
	}
}
