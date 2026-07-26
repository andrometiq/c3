package broker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Andrometiq/c3/internal/version"
)

func TestUpdateAvailability_DefaultAndSet(t *testing.T) {
	b := newTestBroker()
	if avail, latest := b.UpdateAvailability(); avail || latest != "" {
		t.Errorf("default: got (%v, %q), want (false, \"\")", avail, latest)
	}
	b.setUpdateAvailable("v2.3.4")
	if avail, latest := b.UpdateAvailability(); !avail || latest != "v2.3.4" {
		t.Errorf("after set: got (%v, %q), want (true, v2.3.4)", avail, latest)
	}

	// Nil receiver is safe (WriteHealthFile can be called on any broker).
	var nilb *Broker
	if avail, _ := nilb.UpdateAvailability(); avail {
		t.Error("nil broker should report no update")
	}
}

func TestWriteHealthFile_UpdateFields(t *testing.T) {
	hf := filepath.Join(t.TempDir(), "health.json")
	t.Setenv("C3_HEALTH_FILE", hf)
	b := newTestBroker()

	// Before any update is detected: version present, update fields absent (omitempty).
	b.WriteHealthFile()
	got := readHealthFile(t, hf)
	if got.Version != version.Current() {
		t.Errorf("version = %q, want %q", got.Version, version.Current())
	}
	if got.UpdateAvailable || got.LatestVersion != "" {
		t.Errorf("pre-detect: update fields set unexpectedly: %+v", got)
	}
	// Raw JSON must omit the update keys when no update exists (byte-compat).
	raw, _ := os.ReadFile(hf)
	if s := string(raw); containsAny(s, "update_available", "latest_version") {
		t.Errorf("update keys must be omitted when no update: %s", s)
	}

	// After detection: fields surface for the status line.
	b.setUpdateAvailable("v9.9.9")
	b.WriteHealthFile()
	got = readHealthFile(t, hf)
	if !got.UpdateAvailable || got.LatestVersion != "v9.9.9" {
		t.Errorf("post-detect: got update=%v latest=%q, want true/v9.9.9", got.UpdateAvailable, got.LatestVersion)
	}
}

func TestNotifyAttachedTopics_DedupsAndSends(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)

	tid1 := int64(281)
	tid2 := int64(412)
	k1 := MakeRouteKey("telegram", -100, &tid1)
	k2 := MakeRouteKey("telegram", -200, &tid2)

	// Two distinct routes, each claimed by a live stub.
	if _, ok := b.Routes.Claim(k1, &Stub{CLI: "claude", PID: os.Getpid(), CWD: "/a", ConnID: 1, Conn: struct{}{}}); !ok {
		t.Fatal("claim k1")
	}
	if _, ok := b.Routes.Claim(k2, &Stub{CLI: "codex", PID: os.Getpid(), CWD: "/b", ConnID: 2, Conn: struct{}{}}); !ok {
		t.Fatal("claim k2")
	}

	b.notifyAttachedTopics("c3 updated to v9.9.9 — broker restarting")

	sent := fc.sendRepliesSnapshot()
	if len(sent) != 2 {
		t.Fatalf("expected 2 sends (one per distinct route), got %d", len(sent))
	}
	// Each send carries the message and targets a claimed route's chat+topic.
	chats := map[int64]bool{}
	for _, s := range sent {
		if s.Text == "" {
			t.Error("empty notify text")
		}
		if s.TopicID == nil {
			t.Error("expected a topic id on the notify")
		}
		chats[s.ChatID] = true
	}
	if !chats[-100] || !chats[-200] {
		t.Errorf("notify did not reach both routes: %v", chats)
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}
