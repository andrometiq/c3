package main

import (
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
)

// TestRenderRecoverNotice_SurfacesBacklogPreview covers BUG #2: a recovered
// resume that carries a backlog preview (QueuedSummary) must actively SURFACE
// the held messages (sender + kind + preview + total) and instruct the agent to
// drain the rest via fetch_queue — not just a bare count. And it must survive
// the deferred-notice (setPending → takePending) path the resume idle gap relies
// on (channel frames in the idle gap are dropped by Claude Code).
func TestRenderRecoverNotice_SurfacesBacklogPreview(t *testing.T) {
	resp := ipc.RecoverSessionResp{
		Recovered: true, Name: "c3", QueuedCount: 3,
		QueuedSummary: []ipc.QueuedItem{
			{MessageID: 1, Sender: "@k", Kind: "text", Preview: "deploy the thing"},
			{MessageID: 2, Sender: "@k", Kind: "voice", Preview: "(voice)"},
		},
	}
	out := renderRecoverNotice(resp)
	// The count is worded at-resume (§3c) — fetch_queue's Remaining is the live count.
	for _, want := range []string{"deploy the thing", "@k", "voice", "fetch_queue", "and 1 more", "at resume"} {
		if !strings.Contains(out, want) {
			t.Fatalf("recover notice missing %q:\n%s", want, out)
		}
	}
	// The preview must survive the deferred-notice flush path.
	a := &adapter{}
	a.setPendingRecoverNotice(out)
	got, ok := a.takePendingRecoverNotice()
	if !ok || !strings.Contains(got, "deploy the thing") {
		t.Fatalf("flushed deferred notice lost the backlog preview: ok=%v\n%s", ok, got)
	}
}

// TestResolvedAttachReq_BareSubstitutesResolvedIdentity pins §3d1 + item C: a
// BARE attach that the broker resolves (idempotent already-attached, or
// own-recover) is remembered as its RESOLVED identity, so a replay landing on a
// FRESH broker (self-update/rebuild restart) re-binds the same topic EXPLICITLY
// instead of regressing to a picker and silently dropping the claim. A topic
// resolution remembers {TopicID, Group} — NOT {Name, Group}, which a fresh broker
// can't re-claim across groups (attachByName's default-group step-1 + same-group
// exclusion drops the claim). A DM resolution (no topic) remembers {Target:"dm"} —
// never a "dm" name, which the broker would treat as a topic lookup.
func TestResolvedAttachReq_BareSubstitutesResolvedIdentity(t *testing.T) {
	bare := ipc.AttachReq{Op: ipc.OpAttach, CWD: "/proj"}

	tid := int64(281)
	got := resolvedAttachReq(bare, ipc.AttachedMsg{OK: true, Name: "c3", Group: "work", ChatID: -200, TopicID: &tid})
	if got.TopicID == nil || *got.TopicID != 281 || got.Group != "work" || got.ChatID != -200 || got.Name != "" || got.Target != "" || got.CWD != "/proj" {
		t.Fatalf("bare→topic remembered %+v, want {TopicID:281 Group:work ChatID:-200 CWD:/proj} (id-addressed w/ chat cross-check, no Name)", got)
	}

	got = resolvedAttachReq(bare, ipc.AttachedMsg{OK: true, Name: "dm", TopicID: nil})
	if got.Target != "dm" || got.Name != "" || got.TopicID != nil {
		t.Fatalf("bare→DM remembered %+v, want {Target:dm}", got)
	}
}

// TestResolvedAttachReq_ExplicitRememberedVerbatim proves an explicit request is
// remembered as-is, and that a later idempotent BARE OK cannot clobber it with a
// bare request: the bare OK resolves back to the SAME identity (id-addressed), so
// the remembered request keeps pointing at the live route.
func TestResolvedAttachReq_ExplicitRememberedVerbatim(t *testing.T) {
	explicit := ipc.AttachReq{Op: ipc.OpAttach, CWD: "/proj", Name: "feature-x", Group: "work"}
	if got := resolvedAttachReq(explicit, ipc.AttachedMsg{OK: true, Name: "feature-x", Group: "work"}); got != explicit {
		t.Fatalf("explicit request must be remembered verbatim: got %+v", got)
	}

	tid := int64(412)
	bare := ipc.AttachReq{Op: ipc.OpAttach, CWD: "/proj"}
	got := resolvedAttachReq(bare, ipc.AttachedMsg{OK: true, Name: "feature-x", Group: "work", ChatID: -200, TopicID: &tid})
	if got.TopicID == nil || *got.TopicID != 412 || got.Group != "work" || got.ChatID != -200 {
		t.Fatalf("idempotent bare OK must re-remember the id-addressed identity (with chat cross-check), not a bare req: got %+v", got)
	}
}

// TestTakePendingRecoverNotice covers the deferred-CLI-notice logic (task #47):
// the notice must emit at most once, and — with the pendingRecoverTTL drop
// REMOVED — it must persist until the first take however long it waited (the
// flush's live re-peek makes a stale count impossible, so the old minutes-late
// drop was a silent-loss-of-awareness bug).
func TestTakePendingRecoverNotice(t *testing.T) {
	a := &adapter{}

	// Nothing pending → no emit.
	if text, ok := a.takePendingRecoverNotice(); ok || text != "" {
		t.Fatalf("empty: got (%q, %v), want (\"\", false)", text, ok)
	}

	// Fresh pending → returned exactly once, then cleared.
	a.setPendingRecoverNotice("hello")
	if text, ok := a.takePendingRecoverNotice(); !ok || text != "hello" {
		t.Fatalf("fresh: got (%q, %v), want (\"hello\", true)", text, ok)
	}
	if text, ok := a.takePendingRecoverNotice(); ok || text != "" {
		t.Fatalf("second take must not re-emit: got (%q, %v), want (\"\", false)", text, ok)
	}

	// A notice that has waited a long time is STILL returned (no TTL drop).
	a.setPendingRecoverNotice("late")
	if text, ok := a.takePendingRecoverNotice(); !ok || text != "late" {
		t.Fatalf("long-delayed notice must still emit (no TTL): got (%q, %v), want (\"late\", true)", text, ok)
	}
}
