package broker

// queue_command_test.go covers the phase-3 Telegram command surface
// (queue_command.go + the HandleCommand dispatcher): the authz matrix (INV-7
// silent drop with zero bytes sent), the B3 grammar table, the A3 index-only
// bare-/queue render, the /queue <q> pagination + STT-failure preview + 4096
// cap, B2 same-group scoping, the A7 dm rules, DP-1 serial friction, and
// /drain end-to-end through HandleCommand against a real broker fixture with
// the async reply synchronized on the fakeChannel SendReply capture.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/mappings"
)

const (
	cmdOperator    = int64(9001) // DM-allowlisted → operator
	cmdNonOperator = int64(7777) // in an allowlisted group, NOT DM-allowlisted
)

// cmdTestMappings extends the shared fixture: allowlist (operator user + both
// groups) and a third topic "notes" in group main so same-chat drains are
// expressible.
func cmdTestMappings() *mappings.MappingsFile {
	mf := mfWithTelegram()
	cc := mf.Channels["telegram"]
	cc.Topics = append(cc.Topics, mappings.Topic{ChatID: -100, TopicID: 555, Name: "notes", Group: "main"})
	mf.Channels["telegram"] = cc
	mf.Allowlist = &mappings.Allowlist{Users: []int64{cmdOperator}, Groups: []int64{-100, -200}}
	return mf
}

func cmdTestBroker(t *testing.T) (*Broker, *fakeChannel, *BrokerHost) {
	t.Helper()
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	fc := &fakeChannel{}
	b := brokerWithChannel(t, cmdTestMappings(), fc)
	t.Cleanup(b.Shutdown)
	return b, fc, NewBrokerHost(b, "telegram")
}

// Fixture routes: "c3" and "notes" live in group main (-100), "feature-x" in
// group work (-200), the DM at chat 42.
func rC3() RouteKey { return RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 281} }
func rNotes() RouteKey {
	return RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 555}
}
func rFX() RouteKey { return RouteKey{Channel: "telegram", ChatID: -200, HasTopic: true, TopicID: 412} }
func rDM() RouteKey { return RouteKey{Channel: "telegram", ChatID: 42} }

// inGroupC3 builds a command typed in the c3 topic (group main).
func inGroupC3(text string, uid int64) *c3types.Inbound {
	return &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: ptrI64(281),
		Sender: c3types.Sender{UserID: uid}, Text: text}
}

// inDM builds a command typed in the operator DM.
func inDM(text string, uid int64) *c3types.Inbound {
	return &c3types.Inbound{Channel: "telegram", ChatID: 42,
		Sender: c3types.Sender{UserID: uid}, Text: text}
}

// seedTexts appends n text messages to key, oldest-first, ids 1..n, texts
// "<prefix> <i>", newest one minute apart ending ~1m ago.
func seedTexts(t *testing.T, b *Broker, key RouteKey, n int, prefix string) {
	t.Helper()
	for i := 1; i <= n; i++ {
		in := &c3types.Inbound{
			Channel: key.Channel, ChatID: key.ChatID, MessageID: int64(i),
			Sender:    c3types.Sender{UserID: 11, Username: "alice"},
			Text:      fmt.Sprintf("%s %d", prefix, i),
			Timestamp: time.Now().Add(-time.Duration(n-i+1) * time.Minute),
		}
		if key.HasTopic {
			in.TopicID = ptrI64(key.TopicID)
		}
		drainSeed(t, b, key, in)
	}
}

// waitReplyWhere polls the SendReply capture until a reply matches pred —
// the async-command synchronization point (no blind sleeps).
func waitReplyWhere(t *testing.T, fc *fakeChannel, desc string, pred func(c3types.ReplyArgs) bool) c3types.ReplyArgs {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		for _, r := range fc.sendRepliesSnapshot() {
			if pred(r) {
				return r
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for reply (%s); captured: %+v", desc, fc.sendRepliesSnapshot())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// --- authz matrix (INV-7, B9) ---------------------------------------------------

// A group-allowlisted NON-operator gets a silent drop for /drain and
// /queue <q>: handled=true, EMPTY reply, and zero bytes ever sent (the drop is
// decided synchronously — no goroutine is spawned, so the capture stays empty).
func TestAuthz_NonOperator_SilentDropForDrainAndPeek(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 3, "body")

	for _, cmd := range []string{"/drain c3 first 1 to notes", "/queue c3", "/queue c3 2"} {
		reply, handled := host.HandleCommand(inGroupC3(cmd, cmdNonOperator))
		if !handled {
			t.Fatalf("%q from a non-operator must be handled=true (swallowed), got false", cmd)
		}
		if reply != "" {
			t.Fatalf("%q from a non-operator must return an EMPTY reply (silent drop), got %q", cmd, reply)
		}
	}
	if got := fc.sendRepliesSnapshot(); len(got) != 0 {
		t.Fatalf("silent drop must send ZERO bytes; captured %+v", got)
	}
	// Nothing moved either.
	if got := len(drainPeekAll(t, b, rNotes())); got != 0 {
		t.Fatalf("non-operator /drain must not move anything; notes has %d", got)
	}
}

// Bare /queue and /status stay group-cleared: a non-operator in an allowlisted
// group gets a real (index-only) answer.
func TestAuthz_NonOperator_GroupClearedBareQueueAndStatus(t *testing.T) {
	b, _, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 2, "body")

	reply, handled := host.HandleCommand(inGroupC3("/queue", cmdNonOperator))
	if !handled || !strings.Contains(reply, "Pooled queues") {
		t.Fatalf("bare /queue must stay group-cleared; got handled=%v reply=%q", handled, reply)
	}
	reply, handled = host.HandleCommand(inGroupC3("/status", cmdNonOperator))
	if !handled || !strings.Contains(reply, "queued") {
		t.Fatalf("/status must stay group-cleared; got handled=%v reply=%q", handled, reply)
	}
}

// The operator is allowed on all three commands; the async ones deliver their
// reply through SendReply.
func TestAuthz_Operator_AllThreeCommands(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 3, "body")

	if reply, handled := host.HandleCommand(inGroupC3("/status", cmdOperator)); !handled || reply == "" {
		t.Fatalf("/status: handled=%v reply=%q", handled, reply)
	}
	if reply, handled := host.HandleCommand(inGroupC3("/queue", cmdOperator)); !handled || !strings.Contains(reply, "[1] c3") {
		t.Fatalf("bare /queue: handled=%v reply=%q", handled, reply)
	}
	if reply, handled := host.HandleCommand(inGroupC3("/queue c3", cmdOperator)); !handled || reply != "" {
		t.Fatalf("/queue c3 must go async: handled=%v reply=%q", handled, reply)
	}
	waitReplyWhere(t, fc, "peek render", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "«c3»") && strings.Contains(r.Text, "3 queued")
	})
	if reply, handled := host.HandleCommand(inGroupC3("/drain c3 first 1 to notes", cmdOperator)); !handled || reply != "" {
		t.Fatalf("/drain must go async: handled=%v reply=%q", handled, reply)
	}
	waitReplyWhere(t, fc, "drain reply", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "drained «c3» → «notes»")
	})
}

// --- dispatcher edges (A5, A6) ----------------------------------------------------

// A5: "@botname" is stripped from the FIRST token only — commands with
// arguments still dispatch, and '@' inside arguments survives.
func TestHandleCommand_BotMentionStrippedFromFirstTokenOnly(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 2, "body")

	reply, handled := host.HandleCommand(inGroupC3("/queue@c3bot", cmdOperator))
	if !handled || !strings.Contains(reply, "Pooled queues") {
		t.Fatalf("/queue@c3bot: handled=%v reply=%q", handled, reply)
	}
	if reply, handled = host.HandleCommand(inGroupC3("/drain@c3bot c3 1-1 to notes", cmdOperator)); !handled || reply != "" {
		t.Fatalf("/drain@c3bot with args: handled=%v reply=%q", handled, reply)
	}
	waitReplyWhere(t, fc, "drain reply", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "drained «c3» → «notes»")
	})
}

// A6 (broker side, belt & suspenders under the channel guard): an inbound with
// attachments is never a command, even when its caption text looks like one.
func TestHandleCommand_AttachmentCaptionNotHandled(t *testing.T) {
	_, _, host := cmdTestBroker(t)
	in := inGroupC3("/status", cmdOperator)
	in.Attachments = []c3types.Attachment{{Kind: "photo", FileID: "F9"}}
	if _, handled := host.HandleCommand(in); handled {
		t.Fatal("a captioned attachment must not be handled as a command (A6)")
	}
}

// --- bare /queue (A3 index-only + serial stability) --------------------------------

// A3: the group-cleared bare /queue renders ONLY index fields — never message
// content, never kind counts (content-class info lives in the operator-gated
// /queue <q>).
func TestQueueList_IndexOnly_NoContentNoKinds(t *testing.T) {
	b, _, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 2, "SECRETBODY")
	voice := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: ptrI64(281), MessageID: 77,
		Sender: c3types.Sender{UserID: 11}, Text: "SECRETTRANSCRIPT",
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "F1"}},
		Timestamp:   time.Now().Add(-time.Hour)}
	drainSeed(t, b, rC3(), voice)

	reply, handled := host.HandleCommand(inGroupC3("/queue", cmdNonOperator))
	if !handled {
		t.Fatal("bare /queue must be handled")
	}
	if !strings.Contains(reply, "[1] c3 · 3 queued") {
		t.Errorf("index render missing serial/name/pending: %q", reply)
	}
	if !strings.Contains(reply, "oldest ") || !strings.Contains(reply, "newest ") {
		t.Errorf("index render missing oldest/newest ages: %q", reply)
	}
	if strings.Contains(reply, "SECRET") {
		t.Errorf("bare /queue leaked message content: %q", reply)
	}
	if strings.Contains(reply, "🎤") || strings.Contains(reply, "💬") {
		t.Errorf("bare /queue must not show kind counts (A3): %q", reply)
	}
	if !strings.Contains(reply, "Drain by NAME: /drain <name> first 10 · /queue <name> for messages") {
		t.Errorf("footer must teach name addressing: %q", reply)
	}
}

// R8: ages carry a days band; zero timestamps render no age at all.
func TestQueueList_DaysBandAndZeroTimestampGuard(t *testing.T) {
	b, _, host := cmdTestBroker(t)
	old := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: ptrI64(281), MessageID: 1,
		Text: "x", Timestamp: time.Now().Add(-75 * time.Hour)}
	fresh := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: ptrI64(281), MessageID: 2,
		Text: "y", Timestamp: time.Now().Add(-30 * time.Minute)}
	drainSeed(t, b, rC3(), old, fresh)
	// notes: only a zero-timestamp line → Unix<=0 → no age suffix.
	zero := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: ptrI64(555), MessageID: 3, Text: "z"}
	drainSeed(t, b, rNotes(), zero)

	reply, _ := host.HandleCommand(inGroupC3("/queue", cmdOperator))
	if !strings.Contains(reply, "oldest 3d") {
		t.Errorf("want a days band (oldest 3d): %q", reply)
	}
	if !strings.Contains(reply, "newest 30m") {
		t.Errorf("want newest 30m: %q", reply)
	}
	for _, line := range strings.Split(reply, "\n") {
		if strings.Contains(line, "notes") && strings.Contains(line, "oldest") {
			t.Errorf("zero-timestamp queue must render no age: %q", line)
		}
	}
}

// B2: bare /queue typed in a group lists only that group's routes; typed in
// the DM it lists everything. Serials sort by the immutable key (ChatID asc),
// not by name.
func TestQueueList_ScopeGroupVsDM(t *testing.T) {
	b, _, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 2, "a")
	seedTexts(t, b, rFX(), 1, "b")

	reply, _ := host.HandleCommand(inGroupC3("/queue", cmdOperator))
	if !strings.Contains(reply, "c3") || strings.Contains(reply, "feature-x") {
		t.Errorf("group scope must hide other groups' queues: %q", reply)
	}
	reply, _ = host.HandleCommand(inDM("/queue", cmdOperator))
	if !strings.Contains(reply, "[1] feature-x") || !strings.Contains(reply, "[2] c3") {
		t.Errorf("DM scope must list all, key-sorted (-200 before -100): %q", reply)
	}
}

func TestQueueList_Empty(t *testing.T) {
	_, _, host := cmdTestBroker(t)
	reply, handled := host.HandleCommand(inDM("/queue", cmdOperator))
	if !handled || !strings.Contains(reply, "No pooled queues") {
		t.Fatalf("empty listing must be friendly: handled=%v reply=%q", handled, reply)
	}
}

// A rename must not shift serials: the sort key is the immutable route key,
// never the display name.
func TestQueueList_SerialStableUnderRename(t *testing.T) {
	b, _, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 1, "a")
	seedTexts(t, b, rFX(), 1, "b")

	reply, _ := host.HandleCommand(inDM("/queue", cmdOperator))
	if !strings.Contains(reply, "[1] feature-x") {
		t.Fatalf("precondition: feature-x is serial 1: %q", reply)
	}
	// Rename feature-x → "zzz" (a name-sort would now put c3 first).
	mf := cmdTestMappings()
	cc := mf.Channels["telegram"]
	for i := range cc.Topics {
		if cc.Topics[i].Name == "feature-x" {
			cc.Topics[i].Name = "zzz"
		}
	}
	mf.Channels["telegram"] = cc
	b.SetMappings(mf)

	reply, _ = host.HandleCommand(inDM("/queue", cmdOperator))
	if !strings.Contains(reply, "[1] zzz") || !strings.Contains(reply, "[2] c3") {
		t.Errorf("serials must be stable under rename (key-sorted): %q", reply)
	}
}

// --- /queue <q> (operator-gated content peek) ---------------------------------------

func TestQueuePeek_PaginationAndCap(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 43, "message body with some length to it, roughly the shape of a real note")

	if reply, handled := host.HandleCommand(inDM("/queue c3", cmdOperator)); !handled || reply != "" {
		t.Fatalf("/queue c3 must go async: handled=%v reply=%q", handled, reply)
	}
	page1 := waitReplyWhere(t, fc, "page 1", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "showing 1–25 of 43")
	})
	if page1.ChatID != 42 {
		t.Errorf("peek reply must go to the origin chat, got %d", page1.ChatID)
	}
	if !strings.Contains(page1.Text, "\n1. 💬") || !strings.Contains(page1.Text, "\n25. 💬") {
		t.Errorf("page 1 must show ordinals 1..25: %q", page1.Text)
	}
	if strings.Contains(page1.Text, "\n26. ") {
		t.Errorf("page 1 must stop at 25: %q", page1.Text)
	}
	if !strings.Contains(page1.Text, "/queue c3 26") {
		t.Errorf("page 1 must teach the next page: %q", page1.Text)
	}
	if !strings.Contains(page1.Text, "💬43") {
		t.Errorf("kind counts belong to the operator-gated view: %q", page1.Text)
	}
	if !strings.Contains(page1.Text, "Drain: /drain c3 1-25 [to <topic>]") {
		t.Errorf("page 1 drain footer: %q", page1.Text)
	}
	if len(page1.Text) >= 4096 {
		t.Errorf("page must stay under Telegram's 4096 cap, got %d bytes", len(page1.Text))
	}

	if reply, handled := host.HandleCommand(inDM("/queue c3 26", cmdOperator)); !handled || reply != "" {
		t.Fatalf("/queue c3 26 must go async: handled=%v reply=%q", handled, reply)
	}
	page2 := waitReplyWhere(t, fc, "page 2", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "showing 26–43 of 43")
	})
	if !strings.Contains(page2.Text, "\n26. ") || !strings.Contains(page2.Text, "\n43. ") {
		t.Errorf("page 2 must show ordinals 26..43: %q", page2.Text)
	}
	if strings.Contains(page2.Text, "/queue c3 44") {
		t.Errorf("last page must not advertise a next page: %q", page2.Text)
	}
	if !strings.Contains(page2.Text, "Drain: /drain c3 26-43 [to <topic>]") {
		t.Errorf("page 2 drain footer: %q", page2.Text)
	}
	if len(page2.Text) >= 4096 {
		t.Errorf("page 2 must stay under the cap, got %d bytes", len(page2.Text))
	}
}

// R15: a failed transcription renders as "🎤 (transcription failed)", never the
// raw recovery marker (which names file ids and log paths).
func TestQueuePeek_STTFailurePreview(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	failed := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: ptrI64(281), MessageID: 1,
		Sender:      c3types.Sender{UserID: 11, Username: "alice"},
		Text:        `⚠️ [voice transcription failed: no_transcript] The audio is saved and recoverable — call download_attachment with file_id="F-77" to retrieve it.`,
		Attachments: []c3types.Attachment{{Kind: "voice", FileID: "F-77"}},
		Timestamp:   time.Now().Add(-time.Hour)}
	drainSeed(t, b, rC3(), failed)

	host.HandleCommand(inDM("/queue c3", cmdOperator))
	page := waitReplyWhere(t, fc, "stt page", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "«c3»")
	})
	if !strings.Contains(page.Text, "🎤 (transcription failed)") {
		t.Errorf("failed STT must render the human note: %q", page.Text)
	}
	if strings.Contains(page.Text, "file_id") {
		t.Errorf("the raw recovery marker must never render: %q", page.Text)
	}
}

func TestQueuePeek_EmptyAndPastEnd(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	host.HandleCommand(inDM("/queue notes", cmdOperator))
	waitReplyWhere(t, fc, "empty queue", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "«notes» is empty")
	})
	seedTexts(t, b, rC3(), 3, "x")
	host.HandleCommand(inDM("/queue c3 9", cmdOperator))
	waitReplyWhere(t, fc, "past the end", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "start 9 is past the end")
	})
}

// Serial, name:<sigil>, and case-insensitive name forms all resolve.
func TestQueuePeek_SerialSigilAndCaseInsensitive(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 2, "x")

	for _, cmd := range []string{"/queue 1", "/queue name:c3", "/queue C3"} {
		fc.mu.Lock()
		fc.replyCalls = nil
		fc.mu.Unlock()
		if reply, handled := host.HandleCommand(inGroupC3(cmd, cmdOperator)); !handled || reply != "" {
			t.Fatalf("%q: handled=%v reply=%q", cmd, handled, reply)
		}
		waitReplyWhere(t, fc, cmd, func(r c3types.ReplyArgs) bool {
			return strings.Contains(r.Text, "«c3» · 2 queued")
		})
	}
}

// --- B2 scoping + A7 dm rules --------------------------------------------------------

func TestScope_CrossGroupReferencesReject(t *testing.T) {
	b, _, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 1, "a")
	seedTexts(t, b, rFX(), 1, "b")

	reply, handled := host.HandleCommand(inGroupC3("/queue feature-x", cmdOperator))
	if !handled || !strings.Contains(reply, "another group") {
		t.Errorf("cross-group /queue <q> must reject: handled=%v reply=%q", handled, reply)
	}
	reply, _ = host.HandleCommand(inGroupC3("/drain feature-x all", cmdOperator))
	if !strings.Contains(reply, "another group") {
		t.Errorf("cross-group /drain source must reject: %q", reply)
	}
	// Serials are scoped too: group main has exactly one pending queue.
	reply, _ = host.HandleCommand(inGroupC3("/queue 2", cmdOperator))
	if !strings.Contains(reply, "no queue with serial 2") {
		t.Errorf("out-of-scope serial must reject: %q", reply)
	}
	// From the DM everything resolves.
	if reply, handled = host.HandleCommand(inDM("/queue feature-x", cmdOperator)); !handled || reply != "" {
		t.Errorf("DM peek of any group must go async: handled=%v reply=%q", handled, reply)
	}
}

func TestScope_DmToken(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 2, "x")

	// dm as a CONTENT source from a group leaks operator-private content → reject.
	reply, _ := host.HandleCommand(inGroupC3("/queue dm", cmdOperator))
	if !strings.Contains(reply, "operator-private") {
		t.Errorf("/queue dm in a group must reject: %q", reply)
	}
	reply, _ = host.HandleCommand(inGroupC3("/drain dm all", cmdOperator))
	if !strings.Contains(reply, "operator-private") {
		t.Errorf("/drain from dm in a group must reject: %q", reply)
	}
	// dm as a TARGET is fine from anywhere.
	if reply, handled := host.HandleCommand(inGroupC3("/drain c3 first 1 to dm", cmdOperator)); !handled || reply != "" {
		t.Fatalf("/drain … to dm: handled=%v reply=%q", handled, reply)
	}
	waitReplyWhere(t, fc, "drain to dm", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "drained «c3» → «dm»")
	})
	if got := len(drainPeekAll(t, b, rDM())); got != 1 {
		t.Errorf("dm queue should hold the drained line, has %d", got)
	}
}

// A7: dm without a configured dm_chat_id rejects with a clear message.
func TestScope_DmUnconfiguredRejects(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := cmdTestMappings()
	cc := mf.Channels["telegram"]
	cc.DMChatID = 0
	mf.Channels["telegram"] = cc
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	t.Cleanup(b.Shutdown)
	host := NewBrokerHost(b, "telegram")
	seedTexts(t, b, rC3(), 1, "x")

	reply, _ := host.HandleCommand(inGroupC3("/drain c3 first 1 to dm", cmdOperator))
	if !strings.Contains(reply, "dm_chat_id") {
		t.Errorf("unset dm_chat_id must reject clearly: %q", reply)
	}
}

// A7: an ambiguous name (present in several groups, seen from the DM) rejects —
// never guess.
func TestScope_AmbiguousNameRejects(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	mf := cmdTestMappings()
	cc := mf.Channels["telegram"]
	cc.Topics = append(cc.Topics, mappings.Topic{ChatID: -200, TopicID: 999, Name: "c3", Group: "work"})
	mf.Channels["telegram"] = cc
	fc := &fakeChannel{}
	b := brokerWithChannel(t, mf, fc)
	t.Cleanup(b.Shutdown)
	host := NewBrokerHost(b, "telegram")

	reply, _ := host.HandleCommand(inDM("/queue c3", cmdOperator))
	if !strings.Contains(reply, "ambiguous") {
		t.Errorf("multi-hit name must reject as ambiguous: %q", reply)
	}
}

// --- topicless group route ("general") labeling + addressing ----------------------------

// A group's General topic (MessageThreadId 0 → TopicID nil) must never be
// labeled "dm" — that collided with the real DM in /queue and misdirected
// operators to `/drain dm`. It lists as "general", is addressable by the bare
// token `general` in-group and by serial, and the real DM keeps "dm".
func TestGeneralRoute_LabeledAndAddressable(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	gen := RouteKey{Channel: "telegram", ChatID: -100} // group main's General topic
	seedTexts(t, b, gen, 2, "general body")
	seedTexts(t, b, rDM(), 1, "dm body")

	// In-group list: the topicless route is labeled general (topicless sorts
	// before topics), never dm.
	reply, _ := host.HandleCommand(inGroupC3("/queue", cmdOperator))
	if !strings.Contains(reply, "[1] general · 2 queued") {
		t.Errorf("in-group list must label the topicless route general: %q", reply)
	}
	if strings.Contains(reply, "[1] dm") {
		t.Errorf("topicless group route must NOT be labeled dm: %q", reply)
	}
	// DM-scope list: the group General and the real DM are distinct rows with
	// distinct labels (key-sorted: -100 before 42).
	reply, _ = host.HandleCommand(inDM("/queue", cmdOperator))
	if !strings.Contains(reply, "[1] general · 2 queued") || !strings.Contains(reply, "[2] dm · 1 queued") {
		t.Errorf("DM list must label group General 'general' and the real DM 'dm': %q", reply)
	}

	// Addressable by the bare token in-group (async content peek).
	if reply, handled := host.HandleCommand(inGroupC3("/queue general", cmdOperator)); !handled || reply != "" {
		t.Fatalf("/queue general in-group must go async: handled=%v reply=%q", handled, reply)
	}
	waitReplyWhere(t, fc, "general peek by name", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "«general» · 2 queued")
	})
	// ...and by serial (queueIndex row → resolveQueueRef serial path).
	fc.mu.Lock()
	fc.replyCalls = nil
	fc.mu.Unlock()
	if reply, handled := host.HandleCommand(inGroupC3("/queue 1", cmdOperator)); !handled || reply != "" {
		t.Fatalf("/queue 1 in-group must go async: handled=%v reply=%q", handled, reply)
	}
	waitReplyWhere(t, fc, "general peek by serial", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "«general» · 2 queued")
	})

	// Drainable by name in-group; the reply echoes «general», not «dm».
	if reply, handled := host.HandleCommand(inGroupC3("/drain general first 1 to c3", cmdOperator)); !handled || reply != "" {
		t.Fatalf("/drain general in-group: handled=%v reply=%q", handled, reply)
	}
	waitReplyWhere(t, fc, "drain from general", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "drained «general» → «c3»")
	})
	moved := drainPeekAll(t, b, rC3())
	if len(moved) != 1 || !strings.HasPrefix(moved[0].Text, "↩︎ from «general»") {
		t.Fatalf("drained line must carry the general provenance banner: %+v", moved)
	}

	// From the DM the bare token is ambiguous (every group has a General) —
	// reject, never guess.
	reply, _ = host.HandleCommand(inDM("/queue general", cmdOperator))
	if !strings.Contains(reply, "ambiguous") {
		t.Errorf("DM-scope bare general must reject as ambiguous: %q", reply)
	}

	// DP-1 friction echoes the ADDRESSABLE resolved name for the topicless
	// route ("general"), never "dm" (the real DM). general has 1 left → DM rows:
	// [1] general (chat -100), [2] dm (chat 42).
	reply, _ = host.HandleCommand(inDM("/drain 1 all to feature-x", cmdOperator))
	if !strings.Contains(reply, "«general»") || !strings.Contains(reply, "/drain general") {
		t.Errorf("friction must echo the resolved name general: %q", reply)
	}
	if strings.Contains(reply, "«dm»") {
		t.Errorf("friction must not misdirect to the real DM: %q", reply)
	}
}

// --- parser table (B3) -----------------------------------------------------------------

func TestParseDrainCommand_Table(t *testing.T) {
	cases := []struct {
		in      string
		src     string
		srcQ    bool
		sel     DrainSelector
		dst     string // "" = default target
		dstQ    bool
		wantErr string // substring of the reject; "" = success
	}{
		{in: "genie all", src: "genie", sel: DrainSelector{Kind: SelectAll}},
		{in: "genie ALL", src: "genie", sel: DrainSelector{Kind: SelectAll}},
		{in: "genie first 10", src: "genie", sel: DrainSelector{Kind: SelectFirstN, N: 10}},
		{in: "genie 7", src: "genie", sel: DrainSelector{Kind: SelectFirstN, N: 7}},
		{in: "genie 6-10", src: "genie", sel: DrainSelector{Kind: SelectRange, Lo: 6, Hi: 10}},
		{in: "genie 6..10", src: "genie", sel: DrainSelector{Kind: SelectRange, Lo: 6, Hi: 10}},
		{in: "genie 6 to 10", src: "genie", sel: DrainSelector{Kind: SelectRange, Lo: 6, Hi: 10}},
		{in: "genie 6 to 10 to redtruck", src: "genie", sel: DrainSelector{Kind: SelectRange, Lo: 6, Hi: 10}, dst: "redtruck"},
		{in: "notes all to redtruck", src: "notes", sel: DrainSelector{Kind: SelectAll}, dst: "redtruck"},
		{in: "genie first 10 to notes", src: "genie", sel: DrainSelector{Kind: SelectFirstN, N: 10}, dst: "notes"},
		{in: `"my project" first 2 to "notes to self"`, src: "my project", srcQ: true,
			sel: DrainSelector{Kind: SelectFirstN, N: 2}, dst: "notes to self", dstQ: true},
		{in: `"a to b" all`, src: "a to b", srcQ: true, sel: DrainSelector{Kind: SelectAll}},
		{in: "3 6-10", src: "3", sel: DrainSelector{Kind: SelectRange, Lo: 6, Hi: 10}},
		{in: "name:3 all", src: "name:3", sel: DrainSelector{Kind: SelectAll}},
		{in: "genie all to dm", src: "genie", sel: DrainSelector{Kind: SelectAll}, dst: "dm"},
		{in: "genie all to 2", src: "genie", sel: DrainSelector{Kind: SelectAll}, dst: "2"},
		{in: "genie 0", wantErr: "N ≥ 1"},
		{in: "genie 0-4", wantErr: "start at 1"},
		{in: "genie 10-6", wantErr: "inverted"},
		{in: "genie", wantErr: "usage:"},
		{in: "", wantErr: "usage:"},
		{in: "genie sideways", wantErr: "selector"},
		{in: "genie to notes", wantErr: "selector"},
		{in: "genie all to", wantErr: "single token"}, // dangling "to": no target token
		{in: "genie all to a b", wantErr: "single token"},
		{in: `genie "unterminated all`, wantErr: "unbalanced quote"},
	}
	for _, tc := range cases {
		p, errMsg := parseDrainCommand(tc.in)
		if tc.wantErr != "" {
			if errMsg == "" || !strings.Contains(errMsg, tc.wantErr) {
				t.Errorf("parse(%q): want error containing %q, got %q", tc.in, tc.wantErr, errMsg)
			}
			continue
		}
		if errMsg != "" {
			t.Errorf("parse(%q): unexpected error %q", tc.in, errMsg)
			continue
		}
		if p.src.text != tc.src || p.src.quoted != tc.srcQ {
			t.Errorf("parse(%q): src = %+v, want %q (quoted=%v)", tc.in, p.src, tc.src, tc.srcQ)
		}
		if p.sel != tc.sel {
			t.Errorf("parse(%q): sel = %+v, want %+v", tc.in, p.sel, tc.sel)
		}
		switch {
		case tc.dst == "" && p.dst != nil:
			t.Errorf("parse(%q): dst = %+v, want default", tc.in, *p.dst)
		case tc.dst != "" && (p.dst == nil || p.dst.text != tc.dst || p.dst.quoted != tc.dstQ):
			t.Errorf("parse(%q): dst = %+v, want %q (quoted=%v)", tc.in, p.dst, tc.dst, tc.dstQ)
		}
	}
}

// --- /drain end-to-end through HandleCommand ---------------------------------------------

func TestDrainCommand_EndToEnd(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 5, "note")

	if reply, handled := host.HandleCommand(inDM("/drain c3 first 2 to feature-x", cmdOperator)); !handled || reply != "" {
		t.Fatalf("drain must go async: handled=%v reply=%q", handled, reply)
	}
	origin := waitReplyWhere(t, fc, "origin reply", func(r c3types.ReplyArgs) bool {
		return r.ChatID == 42 && strings.Contains(r.Text, "drained")
	})
	for _, want := range []string{"«c3»", "«feature-x»", "2 message(s)", "ordinals 1-2", `first: "note 1"`, "«feature-x» now has 2 queued"} {
		if !strings.Contains(origin.Text, want) {
			t.Errorf("origin reply missing %q: %q", want, origin.Text)
		}
	}
	// The moved lines landed with provenance banners; the source kept the rest.
	moved := drainPeekAll(t, b, rFX())
	if len(moved) != 2 || !strings.HasPrefix(moved[0].Text, "↩︎ from «c3»") {
		t.Fatalf("target queue wrong: %+v", moved)
	}
	if got := len(drainPeekAll(t, b, rC3())); got != 3 {
		t.Errorf("source should keep 3, has %d", got)
	}
	// Step D posted ONE drain-notice in the target topic.
	waitReplyWhere(t, fc, "target notice", func(r c3types.ReplyArgs) bool {
		return r.ChatID == -200 && strings.Contains(r.Text, "drained in from «c3»")
	})
}

// An explicit range past the queue rejects (async, from Drain's Step A) with
// the ACTUAL pending count and a usable correction.
func TestDrainCommand_RangeBeyondPending(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 5, "note")

	if reply, handled := host.HandleCommand(inDM("/drain c3 3-99 to feature-x", cmdOperator)); !handled || reply != "" {
		t.Fatalf("handled=%v reply=%q", handled, reply)
	}
	r := waitReplyWhere(t, fc, "range reject", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "has 5 pending")
	})
	if !strings.Contains(r.Text, "3-5") || !strings.Contains(r.Text, "all") {
		t.Errorf("reject must offer the correction: %q", r.Text)
	}
	if got := len(drainPeekAll(t, b, rFX())); got != 0 {
		t.Errorf("nothing may move on a rejected range, target has %d", got)
	}
}

// Omitted target defaults to the route the command was typed in.
func TestDrainCommand_DefaultTargetIsTypedRoute(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	seedTexts(t, b, rNotes(), 2, "todo")

	if reply, handled := host.HandleCommand(inGroupC3("/drain notes first 1", cmdOperator)); !handled || reply != "" {
		t.Fatalf("handled=%v reply=%q", handled, reply)
	}
	origin := waitReplyWhere(t, fc, "origin reply", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "drained «notes» → «c3»")
	})
	if origin.ChatID != -100 || origin.TopicID == nil || *origin.TopicID != 281 {
		t.Errorf("reply must land where the command was typed: %+v", origin)
	}
	landed := drainPeekAll(t, b, rC3())
	if len(landed) != 1 || !strings.HasPrefix(landed[0].Text, "↩︎ from «notes»") {
		t.Fatalf("typed-in route should hold the drained line: %+v", landed)
	}
	if got := len(drainPeekAll(t, b, rNotes())); got != 1 {
		t.Errorf("source should keep 1, has %d", got)
	}
}

func TestDrainCommand_SameRouteRejects(t *testing.T) {
	b, _, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 1, "x")
	reply, _ := host.HandleCommand(inGroupC3("/drain c3 all", cmdOperator))
	if !strings.Contains(reply, "same queue") {
		t.Errorf("source==target must reject synchronously: %q", reply)
	}
}

// --- plain-text markup (security) --------------------------------------------------------

// Broker command replies render attacker-controlled content as PLAIN TEXT: a
// queued body like "[Payroll login](https://evil)" must appear literally
// (inert), never become a live link sent by the trusted bot. Asserts the
// /queue <q> peek reply, the /drain origin reply, and the Step-D drain-notice
// all carry MarkupNone with the raw text present unescaped.
func TestCommandReplies_PlainTextMarkup(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	evil := "[Payroll login](https://evil.example)"
	in := &c3types.Inbound{Channel: "telegram", ChatID: -100, TopicID: ptrI64(281), MessageID: 1,
		Sender: c3types.Sender{UserID: 11, Username: "alice"}, Text: evil,
		Timestamp: time.Now().Add(-time.Minute)}
	drainSeed(t, b, rC3(), in)

	// /queue <q>: the preview interpolates the body raw, but as plain text.
	if reply, handled := host.HandleCommand(inDM("/queue c3", cmdOperator)); !handled || reply != "" {
		t.Fatalf("/queue c3 must go async: handled=%v reply=%q", handled, reply)
	}
	peek := waitReplyWhere(t, fc, "peek reply", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "«c3»")
	})
	if peek.Markup != c3types.MarkupNone {
		t.Errorf("/queue <q> reply Markup = %q, want MarkupNone", peek.Markup)
	}
	if !strings.Contains(peek.Text, evil) {
		t.Errorf("preview must carry the raw text unescaped (inert under MarkupNone): %q", peek.Text)
	}

	// /drain: the origin reply (echoes the first-preview) and the Step-D
	// drain-notice (interpolates the renameable source topic name) are plain.
	if reply, handled := host.HandleCommand(inDM("/drain c3 all to feature-x", cmdOperator)); !handled || reply != "" {
		t.Fatalf("/drain must go async: handled=%v reply=%q", handled, reply)
	}
	origin := waitReplyWhere(t, fc, "drain origin reply", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "drained «c3» → «feature-x»")
	})
	if origin.Markup != c3types.MarkupNone {
		t.Errorf("/drain reply Markup = %q, want MarkupNone", origin.Markup)
	}
	if !strings.Contains(origin.Text, evil) {
		t.Errorf("drain reply's first-preview must carry the raw text: %q", origin.Text)
	}
	notice := waitReplyWhere(t, fc, "step-D notice", func(r c3types.ReplyArgs) bool {
		return r.ChatID == -200 && strings.Contains(r.Text, "drained in from")
	})
	if notice.Markup != c3types.MarkupNone {
		t.Errorf("Step-D drain-notice Markup = %q, want MarkupNone", notice.Markup)
	}
}

// DP-1 friction: an `all`-drain or a cross-chat drain must NAME its source —
// a serial reference rejects with the resolved name so the confirm is one
// paste away. Same-chat serial drains with explicit ranges stay allowed.
func TestDrainCommand_SerialFrictionForAllAndCrossChat(t *testing.T) {
	b, fc, host := cmdTestBroker(t)
	seedTexts(t, b, rC3(), 3, "x")

	reply, _ := host.HandleCommand(inDM("/drain 1 all to feature-x", cmdOperator))
	if !strings.Contains(reply, "name the source") || !strings.Contains(reply, "«c3»") {
		t.Errorf("serial all-drain must reject with the resolved name: %q", reply)
	}
	reply, _ = host.HandleCommand(inDM("/drain 1 first 1 to feature-x", cmdOperator))
	if !strings.Contains(reply, "name the source") {
		t.Errorf("serial cross-chat drain must reject: %q", reply)
	}
	// Same-chat serial with an explicit range is fine (SoT example "/drain 3 6-10").
	if reply, handled := host.HandleCommand(inDM("/drain 1 2-3 to notes", cmdOperator)); !handled || reply != "" {
		t.Fatalf("same-chat serial range drain: handled=%v reply=%q", handled, reply)
	}
	waitReplyWhere(t, fc, "serial range drain", func(r c3types.ReplyArgs) bool {
		return strings.Contains(r.Text, "drained «c3» → «notes»") && strings.Contains(r.Text, "ordinals 2-3")
	})
	if got := len(drainPeekAll(t, b, rNotes())); got != 2 {
		t.Errorf("notes should hold 2 drained lines, has %d", got)
	}
}
