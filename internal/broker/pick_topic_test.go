package broker

import (
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/mappings"
)

// pickTID is a tiny *int64 helper for session-attachment fixtures.
func pickTID(v int64) *int64 { return &v }

// TestBuildPickTopic_CurrentProjectFirst: a cwd whose basename matches a
// default-group topic yields a "current project" attach_existing suggestion as
// option #1, no group arg (default group), no create row.
func TestBuildPickTopic_CurrentProjectFirst(t *testing.T) {
	mf := mfWithTelegram() // c3/281 (main/default), feature-x/412 (work)
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	p := b.buildPickTopic(nil, "telegram", "/home/x/c3")
	if p.Action != "pick_topic" {
		t.Fatalf("action = %q; want pick_topic", p.Action)
	}
	if len(p.Suggestions) == 0 {
		t.Fatalf("expected a current-project suggestion; got none")
	}
	s := p.Suggestions[0]
	if s.Kind != "attach_existing" || s.Reason != "current project" {
		t.Fatalf("option#1 = %+v; want attach_existing/current project", s)
	}
	if s.TopicID == nil || *s.TopicID != 281 || s.Name != "c3" {
		t.Fatalf("option#1 should be topic c3/281; got %+v", s)
	}
	if s.Group != "" {
		t.Errorf("default-group topic must NOT carry a group arg; got group=%q", s.Group)
	}
}

// TestBuildPickTopic_CreateNewWhenAbsent: a project name that exists in NO group
// yields a create row as option #1 ("current project (new)").
func TestBuildPickTopic_CreateNewWhenAbsent(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	p := b.buildPickTopic(nil, "telegram", "/home/x/brand-new")
	if len(p.Suggestions) == 0 || p.Suggestions[0].Kind != "create" {
		t.Fatalf("absent project must offer a create row; got %+v", p.Suggestions)
	}
	if p.Suggestions[0].Name != "brand-new" || p.Suggestions[0].Reason != "current project (new)" {
		t.Fatalf("create row = %+v; want name=brand-new reason=current project (new)", p.Suggestions[0])
	}
}

// TestBuildPickTopic_CrossGroupArm: a project name that lives in a NON-default
// group is offered as attach_existing WITH its Group set (never a duplicate-
// minting create) — the re-invoke then carries group=… and hits the right chat.
func TestBuildPickTopic_CrossGroupArm(t *testing.T) {
	mf := mfWithTelegram() // feature-x/412 lives in "work" (non-default)
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	p := b.buildPickTopic(nil, "telegram", "/home/x/feature-x")
	if len(p.Suggestions) == 0 {
		t.Fatalf("expected a cross-group current-project suggestion; got none")
	}
	s := p.Suggestions[0]
	if s.Kind != "attach_existing" {
		t.Fatalf("cross-group match must be attach_existing, not create; got %+v", s)
	}
	if s.TopicID == nil || *s.TopicID != 412 || s.Group != "work" {
		t.Fatalf("cross-group suggestion = %+v; want topic 412 group=work", s)
	}
}

// TestBuildPickTopic_CreateOmittedNoDefaultGroup: with no default group, an
// absent project cannot offer a create row (its re-invoke would fail resolveGroup).
func TestBuildPickTopic_CreateOmittedNoDefaultGroup(t *testing.T) {
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {
				// No DefaultGroup.
				Groups:   map[string]mappings.GroupConfig{"work": {ChatID: -200}},
				DMChatID: 42,
				Topics:   []mappings.Topic{{ChatID: -200, TopicID: 412, Name: "feature-x", Group: "work"}},
			},
		},
		Mappings: map[string]mappings.Mapping{},
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	p := b.buildPickTopic(nil, "telegram", "/home/x/brand-new")
	for _, s := range p.Suggestions {
		if s.Kind == "create" {
			t.Fatalf("no default group → no create row; got %+v", s)
		}
	}
}

// TestBuildPickTopic_RecentsFilters exercises every recents filter rule at once:
// dedupe-by-route, drop registry-missing (DM exempt), drop TTL-expired, KEEP
// tombstoned.
func TestBuildPickTopic_RecentsFilters(t *testing.T) {
	now := time.Now()
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {
				DefaultGroup: "main",
				Groups:       map[string]mappings.GroupConfig{"main": {ChatID: -100}, "work": {ChatID: -200}},
				DMChatID:     42,
				Topics: []mappings.Topic{
					{ChatID: -100, TopicID: 281, Name: "c3", Group: "main"},
					{ChatID: -200, TopicID: 412, Name: "feature-x", Group: "work"},
					{ChatID: -100, TopicID: 500, Name: "extra", Group: "main"},
				},
			},
		},
		Mappings: map[string]mappings.Mapping{},
		SessionAttachments: map[string]mappings.SessionAttachment{
			"dup-a":   {Channel: "telegram", ChatID: -100, TopicID: pickTID(281), Name: "c3", Group: "main", LastAttachedAt: now.Add(-1 * time.Minute)},
			"dup-b":   {Channel: "telegram", ChatID: -100, TopicID: pickTID(281), Name: "c3", Group: "main", LastAttachedAt: now.Add(-2 * time.Minute)},
			"missing": {Channel: "telegram", ChatID: -100, TopicID: pickTID(999), Name: "ghost", Group: "main", LastAttachedAt: now.Add(-90 * time.Second)},
			"dm":      {Channel: "telegram", ChatID: 42, TopicID: nil, Name: "dm", LastAttachedAt: now.Add(-8 * time.Minute)},
			"expired": {Channel: "telegram", ChatID: -100, TopicID: pickTID(500), Name: "extra", Group: "main", LastAttachedAt: now.Add(-40 * 24 * time.Hour)},
			"tomb":    {Channel: "telegram", ChatID: -200, TopicID: pickTID(412), Name: "feature-x", Group: "work", Detached: true, LastAttachedAt: now.Add(-3 * time.Minute)},
		},
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// Empty cwd → no rank-1; isolate recents.
	p := b.buildPickTopic(nil, "telegram", "")

	count281, count412, count999, count500, countDM := 0, 0, 0, 0, 0
	for _, s := range p.Suggestions {
		if s.Reason != "recently used" {
			t.Errorf("recents-only run produced a non-recent suggestion: %+v", s)
		}
		switch {
		case s.TopicID == nil && s.Kind == "attach_existing":
			countDM++
		case s.TopicID != nil && *s.TopicID == 281:
			count281++
		case s.TopicID != nil && *s.TopicID == 412:
			count412++
			if s.Group != "work" {
				t.Errorf("tombstoned 412 must carry non-default group=work; got %+v", s)
			}
		case s.TopicID != nil && *s.TopicID == 999:
			count999++
		case s.TopicID != nil && *s.TopicID == 500:
			count500++
		}
	}
	if count281 != 1 {
		t.Errorf("route 281 must appear exactly once (dedupe-by-route); got %d", count281)
	}
	if count412 != 1 {
		t.Errorf("tombstoned route 412 must be KEPT as a suggestion; got %d", count412)
	}
	if countDM != 1 {
		t.Errorf("DM recent must be kept (dm_chat_id configured, registry-exempt); got %d", countDM)
	}
	if count999 != 0 {
		t.Errorf("registry-missing route 999 must be dropped; got %d", count999)
	}
	if count500 != 0 {
		t.Errorf("TTL-expired route 500 must be dropped; got %d", count500)
	}
	// extra/500 is a registered topic the picker didn't surface → HasMore.
	if !p.HasMore {
		t.Errorf("hidden registered topic (extra/500) should set HasMore=true; got false")
	}
}

// TestBuildPickTopic_ClaimedByMarked: a live holder on the current-project topic
// is surfaced via ClaimedBy so the picker warns instead of silently stealing.
func TestBuildPickTopic_ClaimedByMarked(t *testing.T) {
	mf := mfWithTelegram()
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	holder := b.Stubs.Register("codex", 4242, "/x", struct{}{})
	tid := int64(281)
	key := MakeRouteKey("telegram", -100, &tid)
	if !b.tryClaim(nil, holder, key, "c3", false, true) { // replay=true suppresses the welcome
		t.Fatal("holder claim should succeed")
	}

	p := b.buildPickTopic(nil, "telegram", "/home/x/c3")
	if len(p.Suggestions) == 0 || p.Suggestions[0].ClaimedBy == nil {
		t.Fatalf("held current-project topic must be marked ClaimedBy; got %+v", p.Suggestions)
	}
	if p.Suggestions[0].ClaimedBy.CLI != "codex" || p.Suggestions[0].ClaimedBy.PID != 4242 {
		t.Errorf("ClaimedBy = %+v; want codex/4242", p.Suggestions[0].ClaimedBy)
	}
}

// TestBuildPickTopic_HasMoreCountsHiddenTopics pins the HasMore counting rule:
// 3 registered topics + suggestions [create][recent][recent] must set HasMore
// (only the two recents reference real topics → 3 > 2). Counting the create row
// would wrongly compute 3 > 3 = false and hide the third topic. Also asserts the
// ≤4-option budget (suggestions + full-list row).
func TestBuildPickTopic_HasMoreCountsHiddenTopics(t *testing.T) {
	now := time.Now()
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {
				DefaultGroup: "main",
				Groups:       map[string]mappings.GroupConfig{"main": {ChatID: -100}},
				DMChatID:     42,
				Topics: []mappings.Topic{
					{ChatID: -100, TopicID: 101, Name: "t1", Group: "main"},
					{ChatID: -100, TopicID: 102, Name: "t2", Group: "main"},
					{ChatID: -100, TopicID: 103, Name: "t3", Group: "main"},
				},
			},
		},
		Mappings: map[string]mappings.Mapping{},
		SessionAttachments: map[string]mappings.SessionAttachment{
			"r1": {Channel: "telegram", ChatID: -100, TopicID: pickTID(101), Name: "t1", Group: "main", LastAttachedAt: now.Add(-1 * time.Minute)},
			"r2": {Channel: "telegram", ChatID: -100, TopicID: pickTID(102), Name: "t2", Group: "main", LastAttachedAt: now.Add(-2 * time.Minute)},
		},
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	// cwd basename absent from the registry → a create row leads.
	p := b.buildPickTopic(nil, "telegram", "/home/x/absent")
	if len(p.Suggestions) != 3 || p.Suggestions[0].Kind != "create" {
		t.Fatalf("expected [create][recent][recent]; got %+v", p.Suggestions)
	}
	if !p.HasMore {
		t.Fatalf("t3 is hidden → HasMore must be true (create row must NOT count as a shown topic)")
	}
	rows := len(p.Suggestions)
	if p.HasMore {
		rows++
	}
	if rows > maxPickOptions {
		t.Fatalf("rendered rows %d exceed the %d-option budget", rows, maxPickOptions)
	}
}

// TestBuildPickTopic_CwdMappingDMLegacy (item H1): a legacy cwd→route mapping with
// TopicID==0 means the DM (the deleted PATH A normalized a DM cwd mapping to 0). It
// must render as a DM row (TopicID nil → the formatter's target="dm" form), not a
// bogus topic_id=0 re-invoke.
func TestBuildPickTopic_CwdMappingDMLegacy(t *testing.T) {
	mf := mfWithTelegram()
	mf.Mappings["/home/x/dmproj"] = mappings.Mapping{
		Channel: "telegram", ChatID: 42, TopicID: 0, Name: "dm",
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	p := b.buildPickTopic(nil, "telegram", "/home/x/dmproj")
	if len(p.Suggestions) == 0 {
		t.Fatalf("expected a current-project DM suggestion; got none")
	}
	s := p.Suggestions[0]
	if s.Kind != "attach_existing" || s.Reason != "current project" {
		t.Fatalf("option#1 = %+v; want attach_existing/current project", s)
	}
	if s.TopicID != nil {
		t.Fatalf("legacy TopicID==0 cwd mapping must render as a DM row (TopicID nil); got %+v", s)
	}
	if s.Name != "dm" {
		t.Errorf("DM row name = %q, want dm", s.Name)
	}
	if s.ChatID != 42 {
		t.Errorf("DM row ChatID = %d, want cc.DMChatID (42)", s.ChatID)
	}
}

// TestBuildPickTopic_CwdMappingDMLegacy_NoDMConfigured (item 5a): a legacy
// TopicID==0 cwd mapping must NOT render a DM row when the channel has no
// dm_chat_id — picking it would error "dm_chat_id not set". It falls through to
// the basename arms instead (parity with the recents DM arm's cc.DMChatID==0
// skip). Here basename "dmproj" is in no group → a create row leads.
func TestBuildPickTopic_CwdMappingDMLegacy_NoDMConfigured(t *testing.T) {
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {
				DefaultGroup: "main",
				Groups:       map[string]mappings.GroupConfig{"main": {ChatID: -100}},
				// No DMChatID.
			},
		},
		Mappings: map[string]mappings.Mapping{
			"/home/x/dmproj": {Channel: "telegram", ChatID: 42, TopicID: 0, Name: "dm"},
		},
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	p := b.buildPickTopic(nil, "telegram", "/home/x/dmproj")
	for _, s := range p.Suggestions {
		if s.Kind == "attach_existing" && s.TopicID == nil {
			t.Fatalf("no dm_chat_id → must NOT render a DM row; got %+v", s)
		}
	}
	if len(p.Suggestions) != 1 || p.Suggestions[0].Kind != "create" || p.Suggestions[0].Name != "dmproj" {
		t.Fatalf("legacy DM mapping without dm_chat_id must fall through to a create row; got %+v", p.Suggestions)
	}
}

// TestBuildPickTopic_CwdMappingDMLegacy_DedupesWithRecents (item 5b): the cwd DM
// row must key on cc.DMChatID (not the mapping's stored ChatID) so it collapses
// with a recents DM row into a SINGLE "dm" row. Here the cwd mapping stores a
// STALE ChatID (99) while the configured DM chat is 42; the old code keyed the
// cwd row on 99 and the recents row on 42 → two "dm" rows. The fix keys both on
// 42 → one row.
func TestBuildPickTopic_CwdMappingDMLegacy_DedupesWithRecents(t *testing.T) {
	now := time.Now()
	mf := mfWithTelegram() // DMChatID = 42
	mf.Mappings["/home/x/dmproj"] = mappings.Mapping{
		Channel: "telegram", ChatID: 99, TopicID: 0, Name: "dm", // stale chat id
	}
	mf.SessionAttachments = map[string]mappings.SessionAttachment{
		"dm-recent": {Channel: "telegram", ChatID: 42, TopicID: nil, Name: "dm", LastAttachedAt: now.Add(-1 * time.Minute)},
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	p := b.buildPickTopic(nil, "telegram", "/home/x/dmproj")
	dmRows := 0
	for _, s := range p.Suggestions {
		if s.Kind == "attach_existing" && s.TopicID == nil {
			dmRows++
			if s.ChatID != 42 {
				t.Errorf("DM row ChatID = %d, want cc.DMChatID (42) so it dedupes with recents", s.ChatID)
			}
		}
	}
	if dmRows != 1 {
		t.Fatalf("cwd DM row + recents DM row must dedupe into ONE dm row; got %d", dmRows)
	}
}

// TestBuildPickTopic_CwdMappingStaleFallsThrough (item H2): a cwd→route mapping
// whose topic is NOT in the registry (deleted) must NOT render an unvalidated
// topic_id re-invoke, and must NOT inflate the HasMore "shown" count. It falls
// through to the basename/create arms. Here: registry holds exactly ONE topic
// (t1), the stale mapping points at phantom 777. The OLD code rendered 777 as a
// shown topic → HasMore=false, masking the real hidden t1. The fix drops the
// phantom → a create row leads and HasMore correctly surfaces t1.
func TestBuildPickTopic_CwdMappingStaleFallsThrough(t *testing.T) {
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {
				DefaultGroup: "main",
				Groups:       map[string]mappings.GroupConfig{"main": {ChatID: -100}},
				DMChatID:     42,
				Topics:       []mappings.Topic{{ChatID: -100, TopicID: 101, Name: "t1", Group: "main"}},
			},
		},
		Mappings: map[string]mappings.Mapping{
			"/home/x/ghostproj": {Channel: "telegram", ChatID: -100, TopicID: 777, Name: "ghost", Group: "main"},
		},
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	p := b.buildPickTopic(nil, "telegram", "/home/x/ghostproj")
	for _, s := range p.Suggestions {
		if s.TopicID != nil && *s.TopicID == 777 {
			t.Fatalf("stale cwd mapping rendered an unvalidated topic 777: %+v", p.Suggestions)
		}
	}
	if len(p.Suggestions) != 1 || p.Suggestions[0].Kind != "create" || p.Suggestions[0].Name != "ghostproj" {
		t.Fatalf("stale mapping must fall through to a create row for the basename; got %+v", p.Suggestions)
	}
	if !p.HasMore {
		t.Fatal("HasMore must be true: the phantom topic must not inflate shown-count and mask the real hidden topic t1")
	}
}

// TestBuildPickTopic_EmptyCwdEmptyRegistry: the degenerate payload — no cwd
// signal, no topics — is a proposal with zero suggestions and HasMore=false,
// which the formatter still renders actionably (create-by-name body).
func TestBuildPickTopic_EmptyCwdEmptyRegistry(t *testing.T) {
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {DefaultGroup: "main", Groups: map[string]mappings.GroupConfig{"main": {ChatID: -100}}, DMChatID: 42},
		},
		Mappings: map[string]mappings.Mapping{},
	}
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	defer b.Shutdown()

	p := b.buildPickTopic(nil, "telegram", "")
	if len(p.Suggestions) != 0 {
		t.Fatalf("empty cwd + empty registry → zero suggestions; got %+v", p.Suggestions)
	}
	if p.HasMore {
		t.Fatalf("no topics → HasMore must be false")
	}
	if p.Action != "pick_topic" {
		t.Fatalf("action = %q; want pick_topic", p.Action)
	}
}
