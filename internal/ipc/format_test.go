package ipc

import (
	"strings"
	"testing"
)

// TestFormatAttached_OKWithTopic verifies the OK-with-thread shape used
// when the broker auto-claims (or the agent attaches to) a real topic.
func TestFormatAttached_OKWithTopic(t *testing.T) {
	tid := int64(914)
	msg := &AttachedMsg{
		OK: true, Name: "my-feature", ChatID: -100123, TopicID: &tid,
	}
	got := FormatAttached(msg)
	for _, w := range []string{`attached to "my-feature"`, "chat -100123", "thread 914"} {
		if !strings.Contains(got, w) {
			t.Errorf("FormatAttached(ok+topic) missing %q in %q", w, got)
		}
	}
}

// TestFormatAttached_OKDM verifies the OK-DM shape (no TopicID).
func TestFormatAttached_OKDM(t *testing.T) {
	msg := &AttachedMsg{OK: true, Name: "dm", ChatID: 12345678}
	got := FormatAttached(msg)
	if !strings.Contains(got, "DM") {
		t.Errorf("FormatAttached(ok+dm) missing %q in %q", "DM", got)
	}
}

// TestFormatAttached_ProposalParity confirms every proposal action the
// broker can emit is rendered with actionable user-facing text — no
// "unspecified failure" leakage. maintainer 2026-05-18: "I absolutely need
// the flow and the same flow to work in Codex." Both adapters used to
// implement this in parallel and the codex copy silently dropped two
// branches; centralising removes that drift surface forever.
func TestFormatAttached_ProposalParity(t *testing.T) {
	cases := []struct {
		name    string
		msg     AttachedMsg
		want    []string
		wantNot []string
	}{
		{
			name: "create",
			msg: AttachedMsg{
				NeedsConfirmation: true,
				Proposal: &Proposal{
					Action: "create", Name: "my-feature", Group: "dev",
				},
			},
			// The confirm command must carry the name explicitly — a bare
			// attach(create=true) no longer works (cwd-basename backfill deleted,
			// spec §2). The name is inside attach(name="…", create=true).
			want:    []string{`attach(name="my-feature", create=true)`, "attach(topic_id=<n>)"},
			wantNot: []string{"call attach(create=true)"},
		},
		{
			name: "use_existing_other_group",
			msg: AttachedMsg{
				NeedsConfirmation: true,
				Proposal: &Proposal{
					Action:   "use_existing_other_group",
					Existing: &TopicEntry{Name: "my-feature", Group: "other", TopicID: 42},
				},
			},
			want: []string{`Found topic "my-feature" in group "other"`, "Reply yes"},
		},
		{
			name: "use_existing_other_group_with_alternative",
			msg: AttachedMsg{
				NeedsConfirmation: true,
				Proposal: &Proposal{
					Action:   "use_existing_other_group",
					Existing: &TopicEntry{Name: "my-feature", Group: "other", TopicID: 42},
					// The broker sets Alternative.Name to the searched-for name
					// (attach.go: alt := &ipc.Proposal{Action:"create", Group:gName,
					// Name:name}). The alternative re-invoke MUST carry that name — a
					// name-less attach(create=true) errors post-backfill-deletion.
					Alternative: &Proposal{Action: "create", Name: "my-feature", Group: "dev"},
				},
			},
			want:    []string{`attach(name="my-feature", create=true, group="dev")`},
			wantNot: []string{`attach(create=true, group="dev")`},
		},
		{
			name: "disambiguate_dm",
			msg: AttachedMsg{
				NeedsConfirmation: true,
				Proposal: &Proposal{
					Action:   "disambiguate_dm",
					Existing: &TopicEntry{Name: "dm", Group: "dev", TopicID: 7},
				},
			},
			want:    []string{"Ambiguous", "attach(topic_id=7)", `attach(target="dm", steal=true)`},
			wantNot: []string{"unspecified failure"},
		},
		{
			name: "force_steal",
			msg: AttachedMsg{
				NeedsConfirmation: true,
				Proposal: &Proposal{
					Action: "force_steal", Name: "tg-mux",
					Holder: &Holder{CLI: "claude", PID: 1234, CWD: "/home/x"},
				},
			},
			want:    []string{"steal=true", "claude pid 1234"},
			wantNot: []string{"unspecified failure"},
		},
		{
			name: "force_steal_desktop",
			msg: AttachedMsg{
				NeedsConfirmation: true,
				Proposal: &Proposal{
					Action: "force_steal", Name: "tg-mux",
					Holder: &Holder{CLI: "desktop", PID: 2600598, CWD: "/home/user"},
				},
			},
			// A Desktop holder must read as the user's OWN other Desktop chat, with
			// steal framed as MOVING the topic here — never the raw "desktop pid N
			// (cwd …)" triple, which is meaningless across Desktop surfaces.
			want:    []string{"your Claude Desktop C3 session", "steal=true", "move it here"},
			wantNot: []string{"pid 2600598", "cwd"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := FormatAttached(&tc.msg)
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("FormatAttached(%s) = %q\nwant contains %q", tc.name, got, w)
				}
			}
			for _, w := range tc.wantNot {
				if strings.Contains(got, w) {
					t.Errorf("FormatAttached(%s) = %q\nshould NOT contain %q", tc.name, got, w)
				}
			}
		})
	}
}

// ptrI64 is a tiny helper for building *int64 test fixtures.
func ptrI64(v int64) *int64 { return &v }

// TestFormatAttached_PickTopic is the format golden for the bare-attach friendly
// picker (spec §4). Every suggestion line must carry its EXACT runnable re-invoke
// command: topic_id for a default-group existing topic, topic_id+group for a
// non-default group, target="dm" for a DM recent, name+create=true for a create
// row. A live-held topic renders its holder suffix; the type-your-own +
// create-by-name body always prints; HasMore adds the "See the full list" row.
// The wording stays host-neutral (Codex has no AskUserQuestion / /c3:attach) and
// never instructs an auto-pick.
func TestFormatAttached_PickTopic(t *testing.T) {
	msg := &AttachedMsg{
		Op: OpAttached, OK: false, NeedsConfirmation: true,
		Proposal: &Proposal{
			Action: "pick_topic", Channel: "telegram", Project: "c3", HasMore: true,
			Suggestions: []PickSuggestion{
				{Kind: "attach_existing", Reason: "current project", Name: "c3", ChatID: -100, TopicID: ptrI64(948)},
				{Kind: "attach_existing", Reason: "recently used", Name: "feature-x", Group: "work", ChatID: -200, TopicID: ptrI64(412)},
				{Kind: "attach_existing", Reason: "recently used", Name: "dm", ChatID: 42, // TopicID nil → target="dm"
					ClaimedBy: &Holder{CLI: "codex", PID: 1234}},
			},
		},
	}
	got := FormatAttached(msg)
	for _, w := range []string{
		"ASK the user", // never-assume framing
		"use AskUserQuestion if your host has it", // host-neutral hint
		"otherwise ask in plain conversation",     // Codex fallback
		`Attach "c3" (current project)`,           // suggestion 1 description
		"attach(topic_id=948)",                    // default-group re-invoke
		`attach(topic_id=412, group="work")`,      // non-default-group re-invoke
		`attach(target="dm")`,                     // DM recent re-invoke
		"held by codex pid 1234",                  // ClaimedBy suffix
		"picking it will ask before stealing",     // steal warning
		"See the full list",                       // HasMore row
		"`topics` tool",                           // full-list flow
		`attach(name="<name>")`,                   // type-your-own body
		"/c3:attach <name>",                       // Claude slash hint
		`attach(name="<name>", create=true)`,      // create-by-name body
	} {
		if !strings.Contains(got, w) {
			t.Errorf("FormatAttached(pick_topic) missing %q in:\n%s", w, got)
		}
	}
	if strings.Contains(got, "unspecified failure") {
		t.Errorf("FormatAttached(pick_topic) leaked unspecified-failure:\n%s", got)
	}
}

// TestFormatAttached_PickTopic_Create pins the create-row rendering and that a
// create suggestion carries the name in its re-invoke (a bare attach(create=true)
// is not offered — the name must travel).
func TestFormatAttached_PickTopic_Create(t *testing.T) {
	msg := &AttachedMsg{
		Op: OpAttached, OK: false, NeedsConfirmation: true,
		Proposal: &Proposal{
			Action: "pick_topic", Channel: "telegram", Project: "newproj",
			Suggestions: []PickSuggestion{
				{Kind: "create", Reason: "current project (new)", Name: "newproj"},
			},
		},
	}
	got := FormatAttached(msg)
	for _, w := range []string{`Create new topic "newproj"`, `attach(name="newproj", create=true)`} {
		if !strings.Contains(got, w) {
			t.Errorf("FormatAttached(pick_topic create) missing %q in:\n%s", w, got)
		}
	}
	if strings.Contains(got, "See the full list") {
		t.Errorf("no HasMore → no full-list row; got:\n%s", got)
	}
}

// TestFormatAttached_PickTopic_ZeroSuggestions asserts the degenerate payload
// (empty cwd, no valid recents) still renders actionably: header + the
// create-by-name / attach-by-name body, plus a full-list row when the registry
// holds topics the picker couldn't seed (HasMore).
func TestFormatAttached_PickTopic_ZeroSuggestions(t *testing.T) {
	// With topics hidden (HasMore) — full-list row must appear.
	withTopics := &AttachedMsg{
		Op: OpAttached, OK: false, NeedsConfirmation: true,
		Proposal: &Proposal{Action: "pick_topic", Channel: "telegram", HasMore: true},
	}
	got := FormatAttached(withTopics)
	for _, w := range []string{
		`attach(name="<name>")`,
		`attach(name="<name>", create=true)`,
		"See the full list",
	} {
		if !strings.Contains(got, w) {
			t.Errorf("FormatAttached(pick_topic zero+topics) missing %q in:\n%s", w, got)
		}
	}
	// Nothing at all (empty cwd, empty registry) — body only, no full-list row.
	empty := &AttachedMsg{
		Op: OpAttached, OK: false, NeedsConfirmation: true,
		Proposal: &Proposal{Action: "pick_topic", Channel: "telegram"},
	}
	got = FormatAttached(empty)
	if !strings.Contains(got, `attach(name="<name>", create=true)`) {
		t.Errorf("FormatAttached(pick_topic empty) missing create-by-name body in:\n%s", got)
	}
	if strings.Contains(got, "See the full list") {
		t.Errorf("empty registry → no full-list row; got:\n%s", got)
	}
	if strings.Contains(got, "unspecified failure") {
		t.Errorf("FormatAttached(pick_topic empty) leaked unspecified-failure:\n%s", got)
	}
}

// TestFormatAttached_NoTopicsConfigured asserts the 3-state actionable
// guidance for the broker's no_topics_configured status.
func TestFormatAttached_NoTopicsConfigured(t *testing.T) {
	msg := &AttachedMsg{
		Op:     OpAttached,
		OK:     false,
		Status: AttachStatusNoTopicsConfigured,
		Err:    "attach dm: channels.telegram.dm_chat_id not set in mappings.json",
	}
	got := FormatAttached(msg)
	for _, w := range []string{"not configured", "c3-broker setup"} {
		if !strings.Contains(got, w) {
			t.Errorf("FormatAttached(no_topics_configured) missing %q in %q", w, got)
		}
	}
}

// TestFormatAttached_PolicyRejected asserts the 3-state actionable
// guidance for the policy_rejected status (Codex tenant policy layer).
func TestFormatAttached_PolicyRejected(t *testing.T) {
	msg := &AttachedMsg{
		Op:     OpAttached,
		OK:     false,
		Status: AttachStatusPolicyRejected,
		Err:    "host rejected",
	}
	got := FormatAttached(msg)
	for _, w := range []string{"policy layer", "tenant admin", "approve", "retry attach"} {
		if !strings.Contains(got, w) {
			t.Errorf("FormatAttached(policy_rejected) missing %q in %q", w, got)
		}
	}
}

// TestFormatAttached_CwdDefaultCollision asserts the guided message the
// broker emits when a BARE (cwd-default) attach resolves to a topic that
// is already held by a different live session. The rendered text must
// name the cwd, the resolved topic, the holder (cli+pid), and offer both
// the attach-by-name path and the steal (re-attach-by-name + confirm)
// escape hatch. SYMPTOM-3 fix.
func TestFormatAttached_CwdDefaultCollision(t *testing.T) {
	tid := int64(281)
	msg := &AttachedMsg{
		Op:      OpAttached,
		OK:      false,
		Status:  AttachStatusCwdDefaultCollision,
		Name:    "c3",
		CWD:     "/home/user/projects",
		ChatID:  -100,
		TopicID: &tid,
		Holder:  &Holder{CLI: "claude", PID: 9823, CWD: "/home/user/projects"},
	}
	got := FormatAttached(msg)
	for _, w := range []string{
		"/home/user/projects", // the colliding cwd
		"c3",                  // the resolved topic name
		"claude",              // holder cli
		"9823",                // holder pid
		"attach",              // generic guidance verb
		"steal",               // force escape hatch (re-attach by name + confirm)
	} {
		if !strings.Contains(got, w) {
			t.Errorf("FormatAttached(cwd_default_collision) missing %q in %q", w, got)
		}
	}
	if strings.Contains(got, "unspecified failure") {
		t.Errorf("FormatAttached(cwd_default_collision) leaked unspecified-failure: %q", got)
	}
}

// TestFormatAttached_ErrFallback confirms a bare Err (no Status, no
// Proposal) renders as "attach failed: <err>".
func TestFormatAttached_ErrFallback(t *testing.T) {
	msg := &AttachedMsg{OK: false, Err: "broker exploded"}
	got := FormatAttached(msg)
	if !strings.Contains(got, "attach failed: broker exploded") {
		t.Errorf("FormatAttached(err) = %q", got)
	}
}

// TestFormatAttached_UnspecifiedFailure is the final fallback when no
// signal is set on the message.
func TestFormatAttached_UnspecifiedFailure(t *testing.T) {
	msg := &AttachedMsg{OK: false}
	got := FormatAttached(msg)
	if !strings.Contains(got, "unspecified failure") {
		t.Errorf("FormatAttached(empty) = %q", got)
	}
}

// TestFormatTopics_Empty asserts the no-topics path matches the existing
// adapter contract — "no topics configured." (with period).
func TestFormatTopics_Empty(t *testing.T) {
	got := FormatTopics(&TopicsListMsg{})
	if got != "no topics configured." {
		t.Errorf("FormatTopics(empty) = %q; want %q", got, "no topics configured.")
	}
}

// TestFormatTopics_Free renders an unclaimed topic.
func TestFormatTopics_Free(t *testing.T) {
	msg := &TopicsListMsg{Topics: []TopicEntry{
		{Group: "dev", Name: "feat", ChatID: -100, TopicID: 1},
	}}
	got := FormatTopics(msg)
	for _, w := range []string{"known topics:", "dev/feat", "chat -100", "thread 1", "free"} {
		if !strings.Contains(got, w) {
			t.Errorf("FormatTopics(free) missing %q in %q", w, got)
		}
	}
}

// TestFormatTopics_Held renders a claimed topic with holder details.
func TestFormatTopics_Held(t *testing.T) {
	msg := &TopicsListMsg{Topics: []TopicEntry{
		{
			Group: "dev", Name: "feat", ChatID: -100, TopicID: 1,
			ClaimedBy: &Holder{CLI: "claude", PID: 999},
		},
	}}
	got := FormatTopics(msg)
	if !strings.Contains(got, "held by claude pid 999") {
		t.Errorf("FormatTopics(held) = %q", got)
	}
}
