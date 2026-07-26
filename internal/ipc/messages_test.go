package ipc

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestHelloMsg_Roundtrip(t *testing.T) {
	in := HelloMsg{
		Op:           OpHello,
		CLI:          "claude",
		PID:          12345,
		CWD:          "/home/u/proj",
		Capabilities: []string{"claude/channel"},
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out HelloMsg
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.Op != OpHello || out.CLI != "claude" || out.PID != 12345 {
		t.Errorf("roundtrip mismatch: %+v", out)
	}
}

// The forked-session blackhole fix rides on an additive, omitempty
// CannotRenderChannels flag with INVERTED sense: absent ⇒ renderable. An old
// adapter (never sets it) and a capable new adapter both serialize NO field, so
// a broker decoding either sees false = renderable — the fast-path default.
func TestHelloMsg_CannotRenderChannels_OmitEmptyDefaultsRenderable(t *testing.T) {
	// Capable / old adapter: field left false ⇒ must be omitted from the wire.
	capable := HelloMsg{Op: OpHello, CLI: "claude", PID: 1, CWD: "/x"}
	raw, err := json.Marshal(capable)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(raw); strings.Contains(got, "cannot_render_channels") {
		t.Errorf("renderable hello must omit cannot_render_channels; got %s", got)
	}
	// A hello from an OLD broker/adapter that never knew the field decodes to
	// false = renderable (no regression).
	var decoded HelloMsg
	if err := json.Unmarshal([]byte(`{"op":"hello","cli":"claude","pid":1,"cwd":"/x"}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.CannotRenderChannels {
		t.Error("absent cannot_render_channels must decode to false (renderable)")
	}
	// Confident-not-capable adapter: true round-trips.
	incap := HelloMsg{Op: OpHello, CLI: "claude", PID: 1, CWD: "/x", CannotRenderChannels: true}
	raw2, _ := json.Marshal(incap)
	var out HelloMsg
	if err := json.Unmarshal(raw2, &out); err != nil {
		t.Fatal(err)
	}
	if !out.CannotRenderChannels {
		t.Errorf("cannot_render_channels=true must round-trip; got %s", string(raw2))
	}
}

func TestPeekOp_Hello(t *testing.T) {
	raw := `{"op":"hello","cli":"claude","pid":1,"cwd":"/x"}`
	op, err := PeekOp([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if op != OpHello {
		t.Errorf("got op=%q, want %q", op, OpHello)
	}
}

func TestPeekOp_MissingOp(t *testing.T) {
	raw := `{"cli":"claude"}`
	_, err := PeekOp([]byte(raw))
	if err == nil {
		t.Error("expected error for missing op, got nil")
	}
}

func TestErrorMsg_Roundtrip(t *testing.T) {
	in := ErrorMsg{Op: OpError, Err: "broker unavailable"}
	data, _ := json.Marshal(in)
	var out ErrorMsg
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.Err != "broker unavailable" {
		t.Errorf("Err=%q, want broker unavailable", out.Err)
	}
}

func TestPairModeStartReq_Roundtrip(t *testing.T) {
	in := PairModeStartReq{Op: OpPairModeStart, Target: "group", ChatID: -1009123456789}
	data, _ := json.Marshal(in)
	var out PairModeStartReq
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.Target != "group" || out.ChatID != -1009123456789 || out.Op != OpPairModeStart {
		t.Errorf("roundtrip mismatch: %+v", out)
	}
}

// ─── 3-state attach status (2026-05-19) ────────────────────────────────────
//
// AttachStatus disambiguates the attach outcome so the calling agent can
// tell the user "you need to run setup" vs "your tenant policy blocked
// this" vs "success". See docs/plans/2026-05-19-codex-policy-3state.md.

func TestAttachStatusConstants(t *testing.T) {
	// Wire-shape lock: these strings show up in adapter formatters and
	// agent prompts. Renaming them is a breaking change.
	cases := map[AttachStatus]string{
		AttachStatusOK:                  "ok",
		AttachStatusNoTopicsConfigured:  "no_topics_configured",
		AttachStatusPolicyRejected:      "policy_rejected",
		AttachStatusCwdDefaultCollision: "cwd_default_collision",
	}
	for got, want := range cases {
		if string(got) != want {
			t.Errorf("AttachStatus constant: got %q want %q", string(got), want)
		}
	}
}

func TestAttachedMsg_StatusFieldOmitEmpty(t *testing.T) {
	// Status is wire-additive: a message without Status (legacy / pre-3state
	// proposal flows) must serialize without the key so consumers doing a
	// strict re-deserialize round-trip byte-equal.
	msg := AttachedMsg{Op: OpAttached, OK: true}
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(raw); got == "" || containsJSONField(got, "status") {
		t.Errorf("AttachedMsg with empty Status must omit status field; got %q", got)
	}
}

func TestAttachedMsg_CwdDefaultCollision_Roundtrip(t *testing.T) {
	// The cwd_default_collision status carries enough fields for the
	// formatter to render the guided message: the resolved topic Name,
	// the colliding cwd (CWD), and the live Holder (cli+pid). All must
	// round-trip across the IPC wire.
	tid := int64(281)
	in := AttachedMsg{
		Op:      OpAttached,
		OK:      false,
		Status:  AttachStatusCwdDefaultCollision,
		Name:    "c3",
		CWD:     "/home/user/projects",
		ChatID:  -100,
		TopicID: &tid,
		Holder:  &Holder{CLI: "claude", PID: 9823, CWD: "/home/user/projects"},
		Err:     "cwd maps to a topic held by another session",
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out AttachedMsg
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.Status != AttachStatusCwdDefaultCollision {
		t.Errorf("Status=%q want %q", out.Status, AttachStatusCwdDefaultCollision)
	}
	if out.Name != "c3" || out.CWD != "/home/user/projects" {
		t.Errorf("Name=%q CWD=%q want c3 / /home/user/projects", out.Name, out.CWD)
	}
	if out.Holder == nil || out.Holder.CLI != "claude" || out.Holder.PID != 9823 {
		t.Errorf("Holder roundtrip mismatch: %+v", out.Holder)
	}
}

func TestAttachedMsg_HolderFieldOmitEmpty(t *testing.T) {
	// Holder is wire-additive on AttachedMsg: a message without it (every
	// pre-collision flow) must serialize without the key for byte-equal
	// round-trips.
	msg := AttachedMsg{Op: OpAttached, OK: true}
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(raw); containsJSONField(got, "holder") {
		t.Errorf("AttachedMsg with nil Holder must omit holder field; got %q", got)
	}
}

func TestAttachReq_PolicyRejectedFieldOmitEmpty(t *testing.T) {
	req := AttachReq{Op: OpAttach}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(raw); containsJSONField(got, "policy_rejected") {
		t.Errorf("AttachReq with PolicyRejected=false must omit field; got %q", got)
	}
}

// containsJSONField is a coarse substring check that finds `"<k>"` inside
// the serialized JSON. Good enough for omit-empty assertions.
func containsJSONField(j, k string) bool {
	needle := `"` + k + `"`
	for i := 0; i+len(needle) <= len(j); i++ {
		if j[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// ─── /c3:sessions wire shape (TODO #19e, 2026-05-19) ──────────────────────

func TestListSessionsReq_Roundtrip(t *testing.T) {
	in := ListSessionsReq{Op: OpListSessions, PID: 12345, CWD: "/home/u/proj"}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out ListSessionsReq
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.Op != OpListSessions || out.PID != 12345 || out.CWD != "/home/u/proj" {
		t.Errorf("roundtrip mismatch: %+v", out)
	}
}

func TestListSessionsReplyMsg_Roundtrip_NonEmpty(t *testing.T) {
	in := ListSessionsReplyMsg{
		Op: OpListSessionsReply,
		Sessions: []SessionEntry{
			{CLI: "claude", PID: 1001, CWD: "/p1", ConnID: 7, AttachedTo: "c3 (main)", IsThisSession: true},
			{CLI: "codex", PID: 1002, CWD: "/p2", ConnID: 4},
		},
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out ListSessionsReplyMsg
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.Op != OpListSessionsReply {
		t.Errorf("Op=%q want %q", out.Op, OpListSessionsReply)
	}
	if len(out.Sessions) != 2 {
		t.Fatalf("Sessions len=%d, want 2", len(out.Sessions))
	}
	got0 := out.Sessions[0]
	want0 := in.Sessions[0]
	if got0.CLI != want0.CLI || got0.PID != want0.PID || got0.CWD != want0.CWD ||
		got0.ConnID != want0.ConnID || got0.AttachedTo != want0.AttachedTo ||
		got0.IsThisSession != want0.IsThisSession {
		t.Errorf("entry[0] mismatch: %+v want %+v", got0, want0)
	}
	got1 := out.Sessions[1]
	want1 := in.Sessions[1]
	if got1.CLI != want1.CLI || got1.PID != want1.PID || got1.IsThisSession != want1.IsThisSession {
		t.Errorf("entry[1] mismatch: %+v want %+v", got1, want1)
	}
}

func TestSessionEntry_IsThisSessionFieldOmitEmpty(t *testing.T) {
	e := SessionEntry{CLI: "claude", PID: 1, CWD: "/x"}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(raw); containsJSONField(got, "is_this_session") {
		t.Errorf("SessionEntry with IsThisSession=false must omit field; got %q", got)
	}
}

func TestSessionEntry_AttachedToFieldOmitEmpty(t *testing.T) {
	e := SessionEntry{CLI: "claude", PID: 1, CWD: "/x"}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(raw); containsJSONField(got, "attached_to") {
		t.Errorf("SessionEntry with empty AttachedTo must omit field; got %q", got)
	}
}

func TestPairModeReplyMsg_Roundtrip(t *testing.T) {
	in := PairModeReplyMsg{
		Op: OpPairModeReply, OK: true,
		Code: "5829", Target: "dm", TTLSec: 600,
	}
	data, _ := json.Marshal(in)
	var out PairModeReplyMsg
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.Code != "5829" || out.Target != "dm" || out.TTLSec != 600 || !out.OK {
		t.Errorf("roundtrip mismatch: %+v", out)
	}
}

// ─── durable-inbound-queue wire shape (2026-06-22) ────────────────────────

func TestFetchQueueReqRoundTrip(t *testing.T) {
	req := FetchQueueReq{Op: OpFetchQueue, ID: "7", Limit: 3, All: false, Ack: true}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	op, err := PeekOp(data)
	if err != nil || op != OpFetchQueue {
		t.Fatalf("PeekOp = %q,%v; want fetch_queue", op, err)
	}
	var got FetchQueueReq
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Limit != 3 || got.Ack != true || got.All != false || got.ID != "7" {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}

func TestFetchQueueRespCarriesInbound(t *testing.T) {
	resp := FetchQueueResp{
		Op: OpFetchQueueResult, ID: "7", Remaining: 2,
		Messages: []c3types.Inbound{{Channel: "telegram", ChatID: -100, MessageID: 5, Text: "hi"}},
	}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var got FetchQueueResp
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 1 || got.Messages[0].Text != "hi" || got.Remaining != 2 {
		t.Errorf("resp round-trip mismatch: %+v", got)
	}
}

func TestInboundDeliveredAndRetranscribeRoundTrip(t *testing.T) {
	d, _ := json.Marshal(InboundDeliveredMsg{Op: OpInboundDelivered, UpdateID: 42, OK: true})
	if op, _ := PeekOp(d); op != OpInboundDelivered {
		t.Fatalf("delivered op = %q", op)
	}
	r, _ := json.Marshal(RetranscribeReq{Op: OpRetranscribe, ID: "9", FileID: "vf", MessageID: 5})
	if op, _ := PeekOp(r); op != OpRetranscribe {
		t.Fatalf("retranscribe op = %q", op)
	}
	var gr RetranscribeReq
	if err := json.Unmarshal(r, &gr); err != nil {
		t.Fatal(err)
	}
	if gr.FileID != "vf" || gr.MessageID != 5 {
		t.Errorf("retranscribe req mismatch: %+v", gr)
	}
}

func TestAttachedMsgCarriesBacklog(t *testing.T) {
	m := AttachedMsg{
		Op: OpAttached, OK: true, QueuedCount: 2,
		QueuedSummary: []QueuedItem{{MessageID: 5, Sender: "@k", Kind: "text", Unix: 1718722680, Preview: "hi"}},
	}
	data, _ := json.Marshal(m)
	var got AttachedMsg
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.QueuedCount != 2 || len(got.QueuedSummary) != 1 || got.QueuedSummary[0].Preview != "hi" {
		t.Errorf("backlog round-trip mismatch: %+v", got)
	}
}
