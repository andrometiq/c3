package mappings

import (
	"strings"
	"testing"
)

func TestValidate_Ok(t *testing.T) {
	mf := newTestFile()
	mf.SchemaVersion = 1
	if err := mf.Validate(); err != nil {
		t.Errorf("Validate failed on valid file: %v", err)
	}
}

func TestValidate_BadSchemaVersion(t *testing.T) {
	mf := newTestFile()
	mf.SchemaVersion = 99
	err := mf.Validate()
	if err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Errorf("expected schema_version error, got %v", err)
	}
}

func TestValidate_DefaultGroupNotInGroups(t *testing.T) {
	mf := newTestFile()
	mf.SchemaVersion = 1
	cc := mf.Channels["telegram"]
	cc.DefaultGroup = "ghost"
	mf.Channels["telegram"] = cc

	err := mf.Validate()
	if err == nil || !strings.Contains(err.Error(), "default_group") {
		t.Errorf("expected default_group error, got %v", err)
	}
}

func TestValidate_TopicGroupNotInGroups(t *testing.T) {
	mf := newTestFile()
	mf.SchemaVersion = 1
	cc := mf.Channels["telegram"]
	cc.Topics = append(cc.Topics, Topic{ChatID: -300, TopicID: 5, Name: "x", Group: "phantom"})
	mf.Channels["telegram"] = cc

	err := mf.Validate()
	if err == nil || !strings.Contains(err.Error(), "phantom") {
		t.Errorf("expected phantom-group error, got %v", err)
	}
}

func TestValidate_MappingChannelMissing(t *testing.T) {
	mf := newTestFile()
	mf.SchemaVersion = 1
	mf.Mappings["/home/u/orphan"] = Mapping{
		Channel: "ghost-channel", ChatID: -100, TopicID: 1,
	}
	err := mf.Validate()
	if err == nil || !strings.Contains(err.Error(), "ghost-channel") {
		t.Errorf("expected unknown-channel error, got %v", err)
	}
}

func TestValidate_WebConfig(t *testing.T) {
	valid := []ChannelConfig{
		{},
		{Listen: "127.0.0.1:8371"},
		{Listen: "[::1]:8371", PublicURL: "https://device.example"},
		{PublicURL: "https://device.example:8443"},
	}
	for _, config := range valid {
		mf := newTestFile()
		mf.SchemaVersion = 1
		mf.Channels["web"] = config
		if err := mf.Validate(); err != nil {
			t.Errorf("valid web config %+v: %v", config, err)
		}
	}

	invalid := []struct {
		name   string
		config ChannelConfig
	}{
		{"listen missing port", ChannelConfig{Listen: "127.0.0.1"}},
		{"listen bad port", ChannelConfig{Listen: "127.0.0.1:70000"}},
		{"public http", ChannelConfig{PublicURL: "http://device.example"}},
		{"public path", ChannelConfig{PublicURL: "https://device.example/auth"}},
		{"public slash path", ChannelConfig{PublicURL: "https://device.example/"}},
		{"public query", ChannelConfig{PublicURL: "https://device.example?q=1"}},
		{"public empty query", ChannelConfig{PublicURL: "https://device.example?"}},
		{"public fragment", ChannelConfig{PublicURL: "https://device.example#x"}},
		{"public empty fragment", ChannelConfig{PublicURL: "https://device.example#"}},
		{"public bad port", ChannelConfig{PublicURL: "https://device.example:70000"}},
		{"web master id", ChannelConfig{MasterUserID: 42}},
		{"web bot token", ChannelConfig{BotToken: "secret"}},
		{"web dm chat", ChannelConfig{DMChatID: 42}},
		{"web default group", ChannelConfig{DefaultGroup: "main"}},
		{"web groups", ChannelConfig{Groups: map[string]GroupConfig{"main": {ChatID: -1}}}},
		{"web topics", ChannelConfig{Topics: []Topic{{ChatID: -1, TopicID: 2, Name: "x"}}}},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			mf := newTestFile()
			mf.SchemaVersion = 1
			mf.Channels["web"] = tc.config
			if err := mf.Validate(); err == nil {
				t.Fatalf("invalid web config accepted: %+v", tc.config)
			}
		})
	}
}

func TestWebPublicURLIsPrivate(t *testing.T) {
	for _, raw := range []string{"", "https://127.0.0.1", "https://[::1]:8443", "https://device.ts.net"} {
		if !WebPublicURLIsPrivate(raw) {
			t.Errorf("WebPublicURLIsPrivate(%q)=false, want true", raw)
		}
	}
	if WebPublicURLIsPrivate("https://chat.example.com") {
		t.Fatal("ordinary public host was classified private")
	}
}
