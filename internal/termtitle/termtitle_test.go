package termtitle

import (
	"bytes"
	"testing"

	"github.com/Andrometiq/c3/internal/ipc"
)

// helpers ────────────────────────────────────────────────────────────────────

func i64ptr(n int64) *int64 { return &n }

// FormatTitle tests — pure title-string builder ─────────────────────────────

func TestFormatTitle_Topic(t *testing.T) {
	msg := &ipc.AttachedMsg{
		OK: true, Name: "foo", Group: "bar", TopicID: i64ptr(123),
	}
	got := FormatTitle(msg)
	want := "c3: foo · bar"
	if got != want {
		t.Errorf("FormatTitle = %q; want %q", got, want)
	}
}

func TestFormatTitle_TopicNoGroup(t *testing.T) {
	msg := &ipc.AttachedMsg{
		OK: true, Name: "foo", TopicID: i64ptr(123),
	}
	got := FormatTitle(msg)
	want := "c3: foo"
	if got != want {
		t.Errorf("FormatTitle = %q; want %q", got, want)
	}
}

func TestFormatTitle_DMByNilTopicID(t *testing.T) {
	// DM is identified by Name="dm" and/or absence of TopicID. Broker's
	// canonical DM-attach response uses Name="dm" with TopicID=nil.
	msg := &ipc.AttachedMsg{OK: true, Name: "dm"}
	got := FormatTitle(msg)
	want := "c3: dm"
	if got != want {
		t.Errorf("FormatTitle = %q; want %q", got, want)
	}
}

func TestFormatTitle_DMByName(t *testing.T) {
	// Defensive: if Name=="dm" even with a TopicID set (post
	// disambiguate_dm "actual DM" branch), still render as DM.
	msg := &ipc.AttachedMsg{OK: true, Name: "dm", TopicID: i64ptr(5)}
	got := FormatTitle(msg)
	want := "c3: dm"
	if got != want {
		t.Errorf("FormatTitle = %q; want %q", got, want)
	}
}

func TestFormatTitle_EmptyName(t *testing.T) {
	// Defensive: broker should always populate Name on OK, but if not,
	// fall back to a bare "c3" rather than emitting a "c3: " with a
	// trailing colon.
	msg := &ipc.AttachedMsg{OK: true}
	got := FormatTitle(msg)
	want := "c3"
	if got != want {
		t.Errorf("FormatTitle = %q; want %q", got, want)
	}
}

// EmitTo tests — controlled writer + tty + suppress flags ───────────────────

func TestEmitTo_EmitsEscape_WhenTTY(t *testing.T) {
	var buf bytes.Buffer
	msg := &ipc.AttachedMsg{OK: true, Name: "foo", Group: "bar", TopicID: i64ptr(7)}
	EmitTo(&buf, true /*isTTY*/, false /*suppressed*/, msg)
	want := "\x1b]0;c3: foo · bar\x07"
	if got := buf.String(); got != want {
		t.Errorf("EmitTo wrote %q; want %q", got, want)
	}
}

func TestEmitTo_DM_EmitsExpected(t *testing.T) {
	var buf bytes.Buffer
	msg := &ipc.AttachedMsg{OK: true, Name: "dm"}
	EmitTo(&buf, true, false, msg)
	want := "\x1b]0;c3: dm\x07"
	if got := buf.String(); got != want {
		t.Errorf("EmitTo wrote %q; want %q", got, want)
	}
}

func TestEmitTo_Suppressed_NoEmit(t *testing.T) {
	var buf bytes.Buffer
	msg := &ipc.AttachedMsg{OK: true, Name: "foo", Group: "bar"}
	EmitTo(&buf, true, true /*suppressed*/, msg)
	if got := buf.String(); got != "" {
		t.Errorf("EmitTo emitted %q while suppressed; want empty", got)
	}
}

func TestEmitTo_NonTTY_NoEmit(t *testing.T) {
	var buf bytes.Buffer
	msg := &ipc.AttachedMsg{OK: true, Name: "foo", Group: "bar"}
	EmitTo(&buf, false /*isTTY*/, false, msg)
	if got := buf.String(); got != "" {
		t.Errorf("EmitTo emitted %q while non-tty; want empty", got)
	}
}

func TestEmitTo_FailedAttach_NoEmit(t *testing.T) {
	var buf bytes.Buffer
	msg := &ipc.AttachedMsg{
		OK: false, Status: ipc.AttachStatusNoTopicsConfigured, Err: "no topics",
	}
	EmitTo(&buf, true, false, msg)
	if got := buf.String(); got != "" {
		t.Errorf("EmitTo emitted %q on failed attach; want empty", got)
	}
}

func TestEmitTo_NeedsConfirmation_NoEmit(t *testing.T) {
	var buf bytes.Buffer
	msg := &ipc.AttachedMsg{
		OK: false, NeedsConfirmation: true,
		Proposal: &ipc.Proposal{Action: "create", Name: "foo", Group: "default"},
	}
	EmitTo(&buf, true, false, msg)
	if got := buf.String(); got != "" {
		t.Errorf("EmitTo emitted %q on NeedsConfirmation; want empty", got)
	}
}

func TestEmitTo_PolicyRejected_NoEmit(t *testing.T) {
	var buf bytes.Buffer
	msg := &ipc.AttachedMsg{
		OK: false, Status: ipc.AttachStatusPolicyRejected,
	}
	EmitTo(&buf, true, false, msg)
	if got := buf.String(); got != "" {
		t.Errorf("EmitTo emitted %q on policy_rejected; want empty", got)
	}
}

func TestEmitTo_NilMsg_NoEmit(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("EmitTo panicked on nil msg: %v", r)
		}
	}()
	var buf bytes.Buffer
	EmitTo(&buf, true, false, nil)
	if got := buf.String(); got != "" {
		t.Errorf("EmitTo emitted %q on nil msg; want empty", got)
	}
}

// ClearTo tests ─────────────────────────────────────────────────────────────

func TestClearTo_EmitsEmptyTitle_WhenTTY(t *testing.T) {
	var buf bytes.Buffer
	ClearTo(&buf, true, false)
	want := "\x1b]0;\x07"
	if got := buf.String(); got != want {
		t.Errorf("ClearTo wrote %q; want %q", got, want)
	}
}

func TestClearTo_Suppressed_NoEmit(t *testing.T) {
	var buf bytes.Buffer
	ClearTo(&buf, true, true /*suppressed*/)
	if got := buf.String(); got != "" {
		t.Errorf("ClearTo emitted %q while suppressed; want empty", got)
	}
}

func TestClearTo_NonTTY_NoEmit(t *testing.T) {
	var buf bytes.Buffer
	ClearTo(&buf, false /*isTTY*/, false)
	if got := buf.String(); got != "" {
		t.Errorf("ClearTo emitted %q while non-tty; want empty", got)
	}
}

// Env-gating tests ──────────────────────────────────────────────────────────

func TestSuppressedEnv_TruthyValues(t *testing.T) {
	allow := []string{"", "0", "false", "FALSE", "no", "No", "off", "OFF"}
	deny := []string{"1", "true", "TRUE", "yes", "Yes", "on", "ON", "anything"}

	for _, v := range allow {
		t.Setenv("C3_NO_TERMINAL_TITLE", v)
		if Suppressed() {
			t.Errorf("Suppressed()=true for %q; want false", v)
		}
	}
	for _, v := range deny {
		t.Setenv("C3_NO_TERMINAL_TITLE", v)
		if !Suppressed() {
			t.Errorf("Suppressed()=false for %q; want true", v)
		}
	}
}
