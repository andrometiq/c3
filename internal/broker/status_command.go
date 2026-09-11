package broker

import (
	"fmt"
	"log"
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
//   - No worker round-trip blocks the poll goroutine (A1): /status,
//     /drain and /queue <q> return ("", true) and post their reply from a spawned
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
		args := c3types.ReplyArgs{Channel: in.Channel, ChatID: in.ChatID, TopicID: topicPointer(MakeRouteKey(in.Channel, in.ChatID, in.TopicID))}
		go func() {
			defer recoverGoroutine("broker.statusCommand")
			if args.TopicID != nil {
				args.Text = h.broker.statusForTopic(args.Channel, args.ChatID, args.TopicID)
			} else {
				args.Text = h.broker.statusGlobal()
			}
			if ch, err := h.broker.Channel(args.Channel); err == nil {
				if _, err := ch.SendReply(args); err != nil {
					log.Printf("status reply failed: %v", err)
				}
			}
		}()
		return "", true
	case strings.EqualFold(cmd, "/queue"):
		return h.broker.queueCommand(in, rest)
	case strings.EqualFold(cmd, "/drain"):
		return h.broker.drainCommand(in, rest)
	}
	return "", false
}

// statusForTopic renders the per-topic status line.
//
// Counts use the same surviving queued identities as Held.
func (b *Broker) statusForTopic(channelName string, chatID int64, topicID *int64) string {
	key := MakeRouteKey(channelName, chatID, topicID)
	name := b.topicDisplayName(channelName, chatID, topicID)
	pending, oldest := 0, time.Time{}
	if b.Queue != nil {
		st, err := b.statusSnapshot(key)
		if err == nil {
			pending = st.Pending
			if st.OldestUnix > 0 {
				oldest = time.Unix(st.OldestUnix, 0)
			}
		} else {
			pending = -1
		}

	}
	attached := "nothing attached"
	if h, held := b.Routes.Holder(key); held {
		if h.IsAlive() {
			attached = surfaceLabel(h.CLI) + " attached · build " + sessionBuild(h) + " · " + b.chatDeliveryStatus(key)
		} else {
			// Dead reference: the holder's adapter is gone (disconnected AND its
			// PID is no longer in the OS process table). Verify liveness at READ
			// time and reap the stale claim on the spot so the status we print is
			// honest instead of trusting a cached map entry that only gets swept
			// lazily on the next inbound. Release is connID-guarded, so a live
			// re-claim landing between Holder and here is never clobbered.
			b.attempts.release(h, "holder_death", time.Now())
			b.Routes.Release(key, h.ConnID)
		}
	}
	count := fmt.Sprintf("%d queued%s", pending, oldestSuffix(oldest))
	if pending < 0 {
		count = "queue count unavailable"
	}
	line := fmt.Sprintf("📊 %s · %s · %s · broker up", name, count, attached)
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
		type row struct {
			name    string
			pending int
			oldest  int64
		}
		rows := make([]row, 0, len(all))
		for k := range all {
			key := MakeRouteKey(k.Channel, k.ChatID, k.TopicID)
			st, err := b.statusSnapshot(key)
			if err != nil {
				rows = append(rows, row{b.topicDisplayName(k.Channel, k.ChatID, k.TopicID), -1, 0})
				continue
			}
			if st.Pending > 0 {
				rows = append(rows, row{b.topicDisplayName(k.Channel, k.ChatID, k.TopicID), st.Pending, st.OldestUnix})
			}
		}
		if len(rows) > 0 {
			sb.WriteString(" Active queues:")
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
		for _, r := range rows {
			if r.pending < 0 {
				fmt.Fprintf(&sb, "\n• %s — queue count unavailable", r.name)
			} else {
				fmt.Fprintf(&sb, "\n• %s — %d%s", r.name, r.pending, oldestSuffix(time.Unix(r.oldest, 0)))
			}
		}
	}
	var routeLines []string
	for _, claim := range b.Routes.Snapshot() {
		if claim.Stub.IsAlive() {
			routeLines = append(routeLines, fmt.Sprintf("\n• %s · %s · build %s · %s", b.topicDisplayName(claim.Key.Channel, claim.Key.ChatID, topicPointer(claim.Key)), surfaceLabel(claim.Stub.CLI), sessionBuild(claim.Stub), b.chatDeliveryStatus(claim.Key)))
		}
	}
	sort.Strings(routeLines)
	for _, line := range routeLines {
		sb.WriteString(line)
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
		if len(s.Routes()) > 0 {
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
	case "cursor":
		return "Cursor"
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

func sessionBuild(s *Stub) string {
	if s.Build == "" {
		return "unknown"
	}
	return s.Build
}
func topicPointer(key RouteKey) *int64 {
	if key.HasTopic {
		id := key.TopicID
		return &id
	}
	return nil
}

func (b *Broker) chatDeliveryStatus(key RouteKey) string {
	route := b.noticeRoute(key)
	switch route.State {
	case "capable", "cross_session", "live_channel", "live_inbox":
		return "Live delivery available"
	case "waiting", "probing":
		return "Waiting for delivery confirmation"
	default:
		if route.Reason == "no session attached" {
			return "No session attached"
		}
		if b.Queue == nil {
			return "Live delivery unavailable"
		}
		return "Live delivery unavailable; messages are held and recoverable with fetch_queue"
	}
}
