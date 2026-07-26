package broker

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// HandleCommand handles broker-owned Telegram bot commands: "/status",
// "/queue" and "/drain" (each optionally "@<bot>"-suffixed on the command
// token). A "/status" sent in a topic returns that topic's status; in
// DM/General it returns the global summary. "/queue" and "/drain" are the
// pooled-queue command surface (queue_command.go; design spec
// docs/.loop/pooled-queue-DESIGN-SPEC.md). Anything else returns ("", false)
// so the channel routes normally.
//
// Contract with the channel (telegram poll.go intercept):
//   - Runs AFTER the allowlist gate (I-SEC) — strangers never reach here.
//   - Runs ON the channel's poll goroutine, so nothing here may block on a
//     worker round-trip (A1): /drain and /queue <q> parse + resolve
//     synchronously, return ("", true), and post their reply from a spawned
//     goroutine via the channel's SendReply.
//   - ("", true) means handled with NOTHING to send — the channel skips the
//     send (and still marks the update done). That covers both the async path
//     and the operator-gate silent drop (INV-7).
//   - A6: an inbound carrying attachments is never a command — a media CAPTION
//     that happens to start with /drain must not swallow the attachment.
func (h *BrokerHost) HandleCommand(in *c3types.Inbound) (string, bool) {
	if in == nil {
		return "", false
	}
	if len(in.Attachments) > 0 { // A6 (the channel guards too; fail closed here)
		return "", false
	}
	text := strings.TrimSpace(in.Text)
	if text == "" || text[0] != '/' {
		return "", false
	}
	first, rest := text, ""
	if i := strings.IndexAny(text, " \t\n\r"); i >= 0 {
		first, rest = text[:i], strings.TrimSpace(text[i+1:])
	}
	// A5: strip @botname from the FIRST token only — an argument containing
	// '@' (a mention, a name) must survive intact.
	cmd := first
	if j := strings.IndexByte(cmd, '@'); j >= 0 {
		cmd = cmd[:j]
	}
	switch {
	case strings.EqualFold(cmd, "/status"):
		if rest != "" {
			return "", false // /status takes no arguments (unchanged behavior)
		}
		if in.TopicID != nil {
			return h.broker.statusForTopic(in.Channel, in.ChatID, in.TopicID), true
		}
		return h.broker.statusGlobal(), true
	case strings.EqualFold(cmd, "/queue"):
		return h.broker.queueCommand(in, rest)
	case strings.EqualFold(cmd, "/drain"):
		return h.broker.drainCommand(in, rest)
	}
	return "", false
}

// statusForTopic renders the per-topic status line.
//
// I7: reads the mutex-guarded in-memory status index (StatusFor) — NOT the queue
// files via Pending — so a /status answered on a poll goroutine never races the
// route worker's concurrent Append/Consume/rewrite (the global StatusAll path was
// already index-based; this brings the per-topic path to the same single-owner
// discipline).
func (b *Broker) statusForTopic(channelName string, chatID int64, topicID *int64) string {
	key := MakeRouteKey(channelName, chatID, topicID)
	name := b.topicDisplayName(channelName, chatID, topicID)
	pending, oldest := 0, time.Time{}
	if b.Queue != nil {
		st := b.Queue.StatusFor(queueRouteKey(key))
		pending = st.Pending
		if st.OldestUnix > 0 {
			oldest = time.Unix(st.OldestUnix, 0)
		}
	}
	attached := "nothing attached"
	if h, held := b.Routes.Holder(key); held {
		if h.IsAlive() {
			attached = surfaceLabel(h.CLI) + " attached"
		} else {
			// Dead reference: the holder's adapter is gone (disconnected AND its
			// PID is no longer in the OS process table). Verify liveness at READ
			// time and reap the stale claim on the spot so the status we print is
			// honest instead of trusting a cached map entry that only gets swept
			// lazily on the next inbound. Release is connID-guarded, so a live
			// re-claim landing between Holder and here is never clobbered.
			b.Routes.Release(key, h.ConnID)
		}
	}
	line := fmt.Sprintf("📊 %s · %d queued%s · %s · broker up", name, pending, oldestSuffix(oldest), attached)
	// Degraded mode: "0 queued · broker up" reads as healthy when it actually
	// means nothing CAN be queued. The held-notice already tells the operator to
	// "Send /status to check" — this is what they must find when they do. Own
	// line, not an inline suffix: it has to survive being skimmed.
	if b.Queue == nil {
		line += "\n" + queueDegradedStatusLine
	}
	return line
}

// queueDegradedStatusLine is the ⚠️ line both /status renderings add while the
// durable queue is disabled. Same sentence as the startup announcement and the
// held-notice (queueDisabledWarning, fallback.go) so an operator who sees it
// twice recognises it as one problem, not two.
const queueDegradedStatusLine = "⚠️ " + queueDisabledWarning

// statusGlobal renders the broker-wide summary (empty queues omitted).
func (b *Broker) statusGlobal() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "📊 Broker up (pid %d).", os.Getpid())
	if b.Queue == nil {
		// Without this the global summary is the single word "Broker up" — the most
		// reassuring thing C3 can say, at the moment it is least true.
		sb.WriteString("\n" + queueDegradedStatusLine)
		return sb.String()
	}
	all := b.Queue.StatusAll()
	if len(all) > 0 {
		sb.WriteString(" Active queues:")
		type row struct {
			name    string
			pending int
			oldest  int64
		}
		rows := make([]row, 0, len(all))
		for k, st := range all {
			rows = append(rows, row{b.topicDisplayName(k.Channel, k.ChatID, k.TopicID), st.Pending, st.OldestUnix})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
		for _, r := range rows {
			fmt.Fprintf(&sb, "\n• %s — %d%s", r.name, r.pending, oldestSuffix(time.Unix(r.oldest, 0)))
		}
	}
	attached, idle := b.sessionCounts()
	fmt.Fprintf(&sb, "\n%d attached · %d idle", attached, idle)
	return sb.String()
}

// topicDisplayName looks up the topic's friendly name; falls back to "dm"
// (the channel's configured DM chat only), "general" (a topicless GROUP route
// — a forum group's General topic arrives with MessageThreadId 0 → TopicID
// nil), or "topic-<id>". Labeling every nil-topic route "dm" collided a
// group's General queue with the real DM in /queue and misdirected operators
// to `/drain dm`; "general" is the same bare token resolveQueueRef accepts
// in-group, so every surface that shows the label also teaches an addressable
// name.
func (b *Broker) topicDisplayName(channelName string, chatID int64, topicID *int64) string {
	if topicID == nil {
		if cc, ok := b.Mappings().Channels[channelName]; ok && cc.DMChatID != 0 && cc.DMChatID == chatID {
			return "dm"
		}
		return "general"
	}
	if tp, ok := b.Mappings().LookupTopicByID(channelName, chatID, *topicID); ok && tp.Name != "" {
		return tp.Name
	}
	return fmt.Sprintf("topic-%d", *topicID)
}

// sessionCounts returns (attached, idle) live agent-session counts.
func (b *Broker) sessionCounts() (attached, idle int) {
	for _, s := range b.Stubs.Snapshot() {
		if s.CLI == "c3-broker-cli" {
			continue
		}
		if !s.IsAlive() {
			continue // dead session (disconnected + PID gone): count it as neither
		}
		if s.CurrentRoute() != nil {
			attached++
		} else {
			idle++
		}
	}
	return attached, idle
}

// surfaceLabel maps an adapter's self-reported CLI id (Stub.CLI, set on hello)
// to a human surface name, so /status names what is ACTUALLY attached — Claude
// Desktop, Codex, … — instead of a generic "CLI" that misreads a Desktop/cowork
// session as a terminal CLI. Unknown ids fall back to the raw value.
func surfaceLabel(cli string) string {
	switch cli {
	case "claude":
		return "Claude Code"
	case "desktop":
		return "Claude Desktop"
	case "codex":
		return "Codex"
	case "grok":
		return "Grok"
	case "agy":
		return "Antigravity"
	case "", "c3-broker-cli":
		return "a session"
	default:
		return cli
	}
}

// oldestSuffix renders " (oldest 2h)" or "" when there is nothing queued.
func oldestSuffix(oldest time.Time) string {
	if oldest.IsZero() || oldest.Unix() <= 0 {
		return ""
	}
	return " (oldest " + ageBand(oldest) + ")"
}

// ageBand renders a compact age: "<1m", "34m", "7h", "3d" (the days band per
// spec §3/R8 — a pooled queue can sit for days, and "51h" hides that). Callers
// guard Unix<=0 themselves (a zero/unset timestamp renders nothing, never an
// epoch-sized age).
func ageBand(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "<1m"
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}
