package mappings

import (
	"testing"
	"time"
)

func TestClone_DeepCopyIsolatesMutations(t *testing.T) {
	original := &MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]ChannelConfig{
			"telegram": {
				BotToken: "tok",
				Groups: map[string]GroupConfig{
					"main": {ChatID: -100, Title: "Main"},
				},
				Topics: []Topic{{ChatID: -100, TopicID: 1, Name: "a"}},
			},
		},
		Mappings: map[string]Mapping{
			"/home/u/proj": {Channel: "telegram", ChatID: -100, TopicID: 1, CreatedAt: time.Now()},
		},
		Plugins: map[string]map[string]any{
			"stt": {"enabled": true},
		},
	}

	clone := original.Clone()

	// Mutate every nested container in the clone; verify original unchanged.
	clone.Channels["telegram"] = ChannelConfig{BotToken: "tampered"}
	clone.Channels["new"] = ChannelConfig{BotToken: "added-after-clone"}
	clone.Mappings["/home/u/proj"] = Mapping{Name: "tampered"}
	clone.Mappings["/new"] = Mapping{Name: "added-after-clone"}
	clone.Plugins["stt"]["enabled"] = false

	if got := original.Channels["telegram"].BotToken; got != "tok" {
		t.Errorf("clone leak: original.Channels[telegram].BotToken = %q, want tok", got)
	}
	if _, ok := original.Channels["new"]; ok {
		t.Error("clone leak: original.Channels has post-clone insertion 'new'")
	}
	if got := original.Mappings["/home/u/proj"].ChatID; got != -100 {
		t.Errorf("clone leak: original.Mappings[/home/u/proj].ChatID = %d, want -100", got)
	}
	if _, ok := original.Mappings["/new"]; ok {
		t.Error("clone leak: original.Mappings has post-clone insertion '/new'")
	}
	if got := original.Plugins["stt"]["enabled"]; got != true {
		t.Errorf("clone leak: original.Plugins[stt][enabled] = %v, want true", got)
	}

	// Topics slice: mutating clone's slice must not touch original.
	clone.Channels["telegram"] = ChannelConfig{
		Topics: []Topic{{ChatID: 999, TopicID: 999}},
	}
	if got := original.Channels["telegram"].Topics[0].ChatID; got != -100 {
		t.Errorf("topics slice leak: got %d, want -100", got)
	}
}

func TestClone_PreservesNotifications(t *testing.T) {
	no := false
	original := &MappingsFile{SchemaVersion: 1, Notifications: &NotificationsConfig{Invasive: &no}}
	clone := original.Clone()
	if clone.Notifications == nil || clone.Notifications.Invasive == nil {
		t.Fatal("clone dropped Notifications")
	}
	if *clone.Notifications.Invasive != false {
		t.Errorf("clone Invasive = %v, want false", *clone.Notifications.Invasive)
	}
	// Mutating the clone must not touch the original (deep copy).
	*clone.Notifications.Invasive = true
	if *original.Notifications.Invasive != false {
		t.Error("clone leak: mutating clone changed original Invasive")
	}
}

func TestClone_PreservesRichInbound(t *testing.T) {
	no := false
	original := &MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]ChannelConfig{"telegram": {RichInbound: &no}},
	}
	clone := original.Clone()
	cc := clone.Channels["telegram"]
	if cc.RichInbound == nil {
		t.Fatal("clone dropped ChannelConfig.RichInbound")
	}
	if *cc.RichInbound != false {
		t.Errorf("clone RichInbound = %v, want false", *cc.RichInbound)
	}
	// Deep copy: mutating the clone must not touch the original.
	*cc.RichInbound = true
	if *original.Channels["telegram"].RichInbound != false {
		t.Error("clone leak: mutating clone changed original RichInbound")
	}
}

func TestClone_DeepCopiesChannelEnabled(t *testing.T) {
	enabled := false
	original := &MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]ChannelConfig{"web": {Enabled: &enabled}},
	}
	clone := original.Clone()
	cloned := clone.Channels["web"]
	if cloned.Enabled == nil || *cloned.Enabled {
		t.Fatalf("clone Enabled = %v, want explicit false", cloned.Enabled)
	}
	*cloned.Enabled = true
	if *original.Channels["web"].Enabled {
		t.Fatal("mutating cloned Enabled changed the original pointer")
	}
}

func TestClone_PreservesWebTLS(t *testing.T) {
	original := &MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]ChannelConfig{"web": {TLS: true}},
	}
	if !original.Clone().Channels["web"].TLS {
		t.Fatal("clone dropped ChannelConfig.TLS")
	}
}

func TestClone_NilSafe(t *testing.T) {
	var mf *MappingsFile
	if got := mf.Clone(); got != nil {
		t.Errorf("nil.Clone() = %v, want nil", got)
	}
	empty := &MappingsFile{SchemaVersion: 1}
	clone := empty.Clone()
	if clone == nil || clone.SchemaVersion != 1 {
		t.Errorf("empty.Clone() = %+v, want SchemaVersion=1", clone)
	}
}
