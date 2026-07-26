package broker

// queue_command.go is the Telegram command surface over the pooled-queue drain
// core (design spec docs/.loop/pooled-queue-DESIGN-SPEC.md §2-3 + amendments
// A1/A3/A5/A6/A7, B2/B3/B9). It parses and resolves /queue and /drain; the
// mechanics live in drain.go (Broker.Drain) and the queue store — this file is
// a front door, implemented once in the broker so every channel shares it.
//
// Execution contract (A1): HandleCommand runs on the telegram poll goroutine,
// so nothing here may block on a worker round-trip. Bare /queue reads only the
// in-memory status index. /queue <q> and /drain parse + resolve synchronously
// (mappings + status index only), then spawn a goroutine that runs the peek /
// Broker.Drain and posts its own reply via the channel's SendReply;
// HandleCommand returns ("", true) for those.
//
// Authorization (INV-7, B9): /drain (mutating) and /queue <q> (cross-topic
// content) require the sender to be a DM-allowlisted operator
// (Mappings().IsUserAllowed); everyone else gets ("", true) with an EMPTY
// reply — a silent drop, no hint the command exists (default-deny). Bare
// /queue renders index-only metadata (A3) and stays group-cleared, like
// /status.
//
// Scoping (B2): typed in a group, names and serials resolve ONLY within that
// group's routes (cross-group references reject — "run /queue there");
// typed in the operator-private DM they resolve across everything.

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/queue"
)

// Grammar hints appended to parse rejects (operator-visible only — parse runs
// after the operator gate, so a hint never leaks to a non-operator).
const (
	drainGrammarHint = "usage: /drain <source> <all | first N | N | N-M | N to M> [to <target>] — quote names with spaces; bare numbers are serials from /queue; numeric names as name:<n>"
	queueGrammarHint = "usage: /queue — list pooled queues · /queue <name|serial|dm> [start] — that queue's messages"
)

// cmdScope is the B2 resolution scope a command runs in: the group chat it was
// typed in, or (typed in a private chat = the operator DM) everything on the
// channel.
type cmdScope struct {
	channel string
	dm      bool  // private chat: resolve across every route on the channel
	chatID  int64 // group scope: only routes with this chat id
}

// commandScope derives the B2 scope from where the command was typed.
func (b *Broker) commandScope(in *c3types.Inbound) cmdScope {
	if isPrivateChat(in) {
		return cmdScope{channel: in.Channel, dm: true}
	}
	return cmdScope{channel: in.Channel, chatID: in.ChatID}
}

// --- tokens (B3) --------------------------------------------------------------

// cmdToken is one parsed command token. quoted marks a "double-quoted" token:
// quoting both allows spaces in names and FORCES name interpretation (a quoted
// integer is a name, never a serial).
type cmdToken struct {
	text   string
	quoted bool
}

// tokenizeCommand splits a command remainder on whitespace, honoring double
// quotes for names with spaces (B3). No escape sequences in v1. An unbalanced
// quote is an error.
func tokenizeCommand(s string) ([]cmdToken, error) {
	var toks []cmdToken
	i, n := 0, len(s)
	for i < n {
		for i < n && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
			i++
		}
		if i >= n {
			break
		}
		if s[i] == '"' {
			j := strings.IndexByte(s[i+1:], '"')
			if j < 0 {
				return nil, errors.New(`unbalanced quote — close the " or drop it`)
			}
			toks = append(toks, cmdToken{text: s[i+1 : i+1+j], quoted: true})
			i += j + 2
			continue
		}
		j := i
		for j < n && s[j] != ' ' && s[j] != '\t' && s[j] != '\n' && s[j] != '\r' {
			j++
		}
		toks = append(toks, cmdToken{text: s[i:j]})
		i = j
	}
	return toks, nil
}

// isAllDigits reports whether s is a non-empty run of ASCII digits — the B3
// "bare integer token" test (a bare integer is a SERIAL, never a name).
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// cutPrefixFold is strings.CutPrefix with ASCII case-insensitive matching.
func cutPrefixFold(s, prefix string) (string, bool) {
	if len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix) {
		return s[len(prefix):], true
	}
	return "", false
}

// cmdNameToken renders a topic name back as a copy-pasteable command token:
// quoted when it has spaces, name:-prefixed when it would otherwise read as a
// serial. Used by footers/hints so what we teach is what parses.
func cmdNameToken(name string) string {
	if strings.ContainsAny(name, " \t") {
		return `"` + name + `"`
	}
	if isAllDigits(name) {
		return "name:" + name
	}
	return name
}

// --- serial index (bare /queue + serial resolution share it) -------------------

// queueRow is one non-empty pooled queue in the stable serial order.
type queueRow struct {
	key    RouteKey
	name   string
	status queue.Status
}

// queueIndex returns the scope's non-empty queues in the STABLE serial order:
// sorted by the immutable key (Channel, ChatID, TopicID) — never by display
// name, so a rename can't shift serials (only a queue emptying or appearing
// can). Serial N (1-based) = index N-1. Reads only the in-memory status index
// (no files, no worker round-trips — safe on the poll goroutine).
func (b *Broker) queueIndex(scope cmdScope) []queueRow {
	if b.Queue == nil {
		return nil
	}
	all := b.Queue.StatusAll()
	rows := make([]queueRow, 0, len(all))
	for qrk, st := range all {
		if qrk.Channel != scope.channel {
			continue
		}
		if !scope.dm && qrk.ChatID != scope.chatID {
			continue // B2: group scope sees only its own chat's routes
		}
		rows = append(rows, queueRow{
			key:    MakeRouteKey(qrk.Channel, qrk.ChatID, qrk.TopicID),
			name:   b.topicDisplayName(qrk.Channel, qrk.ChatID, qrk.TopicID),
			status: st,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		a, c := rows[i].key, rows[j].key
		if a.Channel != c.Channel {
			return a.Channel < c.Channel
		}
		if a.ChatID != c.ChatID {
			return a.ChatID < c.ChatID
		}
		if a.HasTopic != c.HasTopic {
			return !a.HasTopic // topicless (dm) before topics
		}
		return a.TopicID < c.TopicID
	})
	return rows
}

// --- bare /queue (A3: STRICTLY index-only) -------------------------------------

// renderQueueList renders the bare-/queue index. Group-cleared like /status, so
// it must show ONLY status-index fields — serial · name · pending · oldest ·
// newest. NO text previews, NO kind counts, NO per-route peeks (A3): those are
// content and live in the operator-gated /queue <q>.
func (b *Broker) renderQueueList(scope cmdScope) string {
	rows := b.queueIndex(scope)
	if len(rows) == 0 {
		where := ""
		if !scope.dm {
			where = " in this group"
		}
		return "📥 No pooled queues" + where + " — nothing pending."
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "📥 Pooled queues (%d)\n", len(rows))
	for i, r := range rows {
		fmt.Fprintf(&sb, "\n[%d] %s · %d queued%s", i+1, r.name, r.status.Pending, pendingAges(r.status))
	}
	sb.WriteString("\n\nDrain by NAME: /drain <name> first 10 · /queue <name> for messages")
	return sb.String()
}

// pendingAges renders " · oldest 2d · newest 4h" from a status row. Unix<=0
// (empty / zero-timestamp lines) renders nothing.
func pendingAges(st queue.Status) string {
	if st.OldestUnix <= 0 {
		return ""
	}
	s := " · oldest " + ageBand(time.Unix(st.OldestUnix, 0))
	if st.NewestUnix > 0 {
		s += " · newest " + ageBand(time.Unix(st.NewestUnix, 0))
	}
	return s
}

// --- reference resolution (B2 scope + B3 tokens + A7 dm) ------------------------

// refRole distinguishes how a resolved reference is used, which changes both
// the reject wording and the dm privacy rule.
type refRole int

const (
	// refSource is a content-bearing reference: /drain's source, /queue <q>'s
	// queue. In group scope `dm` is rejected here — rendering operator-private
	// DM content (even a preview) into a group is the exact leak class B2 exists
	// to stop.
	refSource refRole = iota
	// refTarget is /drain's destination. `dm` is fine from anywhere (nothing
	// DM-side is rendered into the group); unknown names suggest attach.
	refTarget
)

// queueRef is a resolved source/target reference.
type queueRef struct {
	key      RouteKey
	name     string
	bySerial bool // resolved via a bare-integer serial (DP-1 friction check)
}

// resolveQueueRef resolves one command token into a route per the pinned
// grammar (B3): bare integer = serial from the scope's /queue index; `dm` = the
// channel's DM route (A7: requires dm_chat_id); `general` = the topicless
// route of the CURRENT group (a forum group's General topic — rejected in DM
// scope, where every group has one); `name:<x>` forces name interpretation
// (numeric-named topics); quoted tokens are always names; names match
// case-insensitively within the B2 scope. Returns the reference or a human
// reject message (exactly one is set).
func (b *Broker) resolveQueueRef(tok cmdToken, scope cmdScope, role refRole) (queueRef, string) {
	raw := strings.TrimSpace(tok.text)
	if raw == "" {
		return queueRef{}, `⚠️ empty name — quote names with spaces, e.g. "my project"`
	}
	name := raw
	if !tok.quoted {
		if strings.EqualFold(raw, "dm") {
			cc, ok := b.Mappings().Channels[scope.channel]
			if !ok || cc.DMChatID == 0 {
				return queueRef{}, fmt.Sprintf("⚠️ dm is not configured — channels.%s.dm_chat_id is not set in mappings.json (pair the DM via c3-broker setup first)", scope.channel)
			}
			if !scope.dm && role == refSource {
				return queueRef{}, "⚠️ «dm» is operator-private — run that command in the DM instead"
			}
			return queueRef{key: RouteKey{Channel: scope.channel, ChatID: cc.DMChatID}, name: "dm"}, ""
		}
		if strings.EqualFold(raw, "general") {
			// The topicless route of the CURRENT group — the label queueIndex /
			// topicDisplayName renders for a forum group's General topic. From
			// the DM there is no "current group" (every group has a General), so
			// reject rather than guess (A7 posture).
			if scope.dm {
				return queueRef{}, "⚠️ «general» is ambiguous from the DM — every group has a General topic; use its serial from /queue, or run the command in that group"
			}
			return queueRef{key: RouteKey{Channel: scope.channel, ChatID: scope.chatID}, name: "general"}, ""
		}
		if isAllDigits(raw) {
			rows := b.queueIndex(scope)
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 || n > len(rows) {
				return queueRef{}, fmt.Sprintf("⚠️ no queue with serial %s here — run /queue for the current list (%d listed)", raw, len(rows))
			}
			r := rows[n-1]
			return queueRef{key: r.key, name: r.name, bySerial: true}, ""
		}
		if rest, ok := cutPrefixFold(raw, "name:"); ok {
			name = rest
		}
	}
	cc, ok := b.Mappings().Channels[scope.channel]
	if !ok {
		return queueRef{}, fmt.Sprintf("⚠️ channel %q has no topic registry", scope.channel)
	}
	var inScope, elsewhere []mappings.Topic
	for _, tp := range cc.Topics {
		if !strings.EqualFold(tp.Name, name) {
			continue
		}
		if scope.dm || tp.ChatID == scope.chatID {
			inScope = append(inScope, tp)
		} else {
			elsewhere = append(elsewhere, tp)
		}
	}
	switch {
	case len(inScope) == 1:
		tp := inScope[0]
		return queueRef{key: RouteKey{Channel: scope.channel, ChatID: tp.ChatID, HasTopic: true, TopicID: tp.TopicID}, name: tp.Name}, ""
	case len(inScope) > 1:
		// A7 posture: multi-hit → reject as ambiguous, never guess.
		return queueRef{}, fmt.Sprintf("⚠️ topic name «%s» is ambiguous — %d topics share it; run the command where the name is unique, or rename one", name, len(inScope))
	case len(elsewhere) > 0:
		// B2: cross-group reference from a group.
		return queueRef{}, fmt.Sprintf("⚠️ «%s» is in another group — run /queue there", name)
	}
	if role == refTarget {
		return queueRef{}, fmt.Sprintf("⚠️ no topic named «%s» — create it via attach first", name)
	}
	return queueRef{}, fmt.Sprintf("⚠️ no queue named «%s» — run /queue for the list", name)
}

// --- selector parsing (B3, forgiving) -------------------------------------------

// parseSelectorTokens reports whether toks form a COMPLETE selector. shapeOK
// false = "these tokens are not a selector" (the caller then tries the
// target-split); shapeOK true + errMsg = a selector with invalid values
// (rejected synchronously with the grammar hint). Quoted tokens are never
// selector words.
func parseSelectorTokens(toks []cmdToken) (sel DrainSelector, shapeOK bool, errMsg string) {
	for _, t := range toks {
		if t.quoted {
			return DrainSelector{}, false, ""
		}
	}
	switch len(toks) {
	case 1:
		t := toks[0].text
		if strings.EqualFold(t, "all") {
			return DrainSelector{Kind: SelectAll}, true, ""
		}
		if isAllDigits(t) {
			return firstNSel(t)
		}
		if lo, hi, ok := splitRangeToken(t); ok {
			return rangeSel(lo, hi)
		}
	case 2:
		if strings.EqualFold(toks[0].text, "first") && isAllDigits(toks[1].text) {
			return firstNSel(toks[1].text)
		}
	case 3:
		if strings.EqualFold(toks[1].text, "to") && isAllDigits(toks[0].text) && isAllDigits(toks[2].text) {
			lo, e1 := strconv.Atoi(toks[0].text)
			hi, e2 := strconv.Atoi(toks[2].text)
			if e1 != nil || e2 != nil {
				return DrainSelector{}, true, "⚠️ that range is out of numeric range — " + drainGrammarHint
			}
			return rangeSel(lo, hi)
		}
	}
	return DrainSelector{}, false, ""
}

// splitRangeToken parses the single-token range forms "N-M" and "N..M".
func splitRangeToken(t string) (lo, hi int, ok bool) {
	var a, c string
	if i := strings.Index(t, ".."); i > 0 {
		a, c = t[:i], t[i+2:]
	} else if i := strings.IndexByte(t, '-'); i > 0 {
		a, c = t[:i], t[i+1:]
	} else {
		return 0, 0, false
	}
	if !isAllDigits(a) || !isAllDigits(c) {
		return 0, 0, false
	}
	lo, e1 := strconv.Atoi(a)
	hi, e2 := strconv.Atoi(c)
	if e1 != nil || e2 != nil {
		return 0, 0, false
	}
	return lo, hi, true
}

func firstNSel(digits string) (DrainSelector, bool, string) {
	n, err := strconv.Atoi(digits)
	if err != nil {
		return DrainSelector{}, true, "⚠️ that number is out of range — " + drainGrammarHint
	}
	if n < 1 {
		return DrainSelector{}, true, fmt.Sprintf("⚠️ first-N needs N ≥ 1 (got %d) — %s", n, drainGrammarHint)
	}
	return DrainSelector{Kind: SelectFirstN, N: n}, true, ""
}

func rangeSel(lo, hi int) (DrainSelector, bool, string) {
	if lo < 1 {
		return DrainSelector{}, true, fmt.Sprintf("⚠️ ordinals start at 1 (got %d) — %s", lo, drainGrammarHint)
	}
	if lo > hi {
		return DrainSelector{}, true, fmt.Sprintf("⚠️ range %d-%d is inverted — the low ordinal comes first. %s", lo, hi, drainGrammarHint)
	}
	return DrainSelector{Kind: SelectRange, Lo: lo, Hi: hi}, true, ""
}

// drainParse is the parsed (pre-resolution) form of a /drain command.
type drainParse struct {
	src cmdToken
	sel DrainSelector
	dst *cmdToken // nil = default target (the route the command was typed in)
}

// parseDrainCommand parses everything after "/drain". Grammar (B3):
//
//	<source> <all | first N | N | N-M | N..M | N to M> [to <target>]
//
// The selector is parsed GREEDILY first: if every token after the source forms
// a complete selector (`/drain genie 6 to 10`), it IS the selector and the
// target defaults — the trailing ` to ` split applies only when the whole tail
// is not a selector (`/drain genie 6 to 10 to redtruck`, `/drain notes all to
// redtruck`). This keeps the SoT's spoken form "do 6 to 10" a range instead of
// misreading "10" as a serial target; combining a range with a target is
// always expressible (`6-10 to <t>`). The split point is the LAST unquoted
// "to" token, so quoted names containing " to " never split.
func parseDrainCommand(rest string) (drainParse, string) {
	toks, err := tokenizeCommand(rest)
	if err != nil {
		return drainParse{}, "⚠️ " + err.Error() + " — " + drainGrammarHint
	}
	if len(toks) < 2 {
		return drainParse{}, "⚠️ " + drainGrammarHint
	}
	p := drainParse{src: toks[0]}
	selToks := toks[1:]
	if sel, ok, emsg := parseSelectorTokens(selToks); ok {
		if emsg != "" {
			return drainParse{}, emsg
		}
		p.sel = sel
		return p, ""
	}
	last := -1
	for i := len(selToks) - 1; i >= 0; i-- {
		if !selToks[i].quoted && strings.EqualFold(selToks[i].text, "to") {
			last = i
			break
		}
	}
	if last < 1 {
		// No unquoted "to" (or nothing before it that could be a selector).
		return drainParse{}, "⚠️ can't read the selector — " + drainGrammarHint
	}
	tgt := selToks[last+1:]
	if len(tgt) != 1 {
		return drainParse{}, "⚠️ the target must be a single token — quote names with spaces. " + drainGrammarHint
	}
	sel, ok, emsg := parseSelectorTokens(selToks[:last])
	if !ok {
		return drainParse{}, "⚠️ can't read the selector — " + drainGrammarHint
	}
	if emsg != "" {
		return drainParse{}, emsg
	}
	p.sel = sel
	p.dst = &tgt[0]
	return p, ""
}

// --- /queue command --------------------------------------------------------------

// queueCommand handles "/queue" (bare: group-cleared index, A3) and
// "/queue <q> [start]" (operator-gated content peek, async per A1).
func (b *Broker) queueCommand(in *c3types.Inbound, rest string) (string, bool) {
	scope := b.commandScope(in)
	if strings.TrimSpace(rest) == "" {
		return b.renderQueueList(scope), true
	}
	// Operator gate (INV-7): cross-topic message CONTENT. Silent drop otherwise.
	if in.Sender.UserID == 0 || !b.Mappings().IsUserAllowed(in.Sender.UserID) {
		log.Printf("queue command DENY (silent) chan=%s chat=%d sender=%d", in.Channel, in.ChatID, in.Sender.UserID)
		return "", true
	}
	toks, err := tokenizeCommand(rest)
	if err != nil {
		return "⚠️ " + err.Error() + " — " + queueGrammarHint, true
	}
	start := 1
	switch len(toks) {
	case 1:
	case 2:
		v, aerr := strconv.Atoi(toks[1].text)
		if toks[1].quoted || !isAllDigits(toks[1].text) || aerr != nil || v < 1 {
			return "⚠️ start must be a positive ordinal — " + queueGrammarHint, true
		}
		start = v
	default:
		return "⚠️ " + queueGrammarHint, true
	}
	ref, rej := b.resolveQueueRef(toks[0], scope, refSource)
	if rej != "" {
		return rej, true
	}
	ch, cerr := b.Channel(in.Channel)
	if cerr != nil {
		return "⚠️ channel unavailable: " + cerr.Error(), true
	}
	// MarkupNone (security): the page interpolates attacker-controlled message
	// bodies raw — "[Payroll login](https://evil)" must render as inert plain
	// text, never a live link from the trusted bot (the markdown renderer does
	// not honor backslash escapes, so escaping is not an option).
	reply := c3types.ReplyArgs{Channel: in.Channel, ChatID: in.ChatID, TopicID: copyTopicPtr(in.TopicID), Markup: c3types.MarkupNone}
	// A1: the peek is a worker round-trip (the source worker may be mid-STT for
	// minutes) — run it off the poll goroutine and post the reply ourselves.
	go func() {
		defer recoverGoroutine("broker.queueCommand.peek")
		reply.Text = b.peekAndRenderQueue(ref, start)
		if _, serr := ch.SendReply(reply); serr != nil {
			log.Printf("queue peek reply send failed chat=%d: %v", reply.ChatID, serr)
		}
	}()
	return "", true
}

// peekAndRenderQueue runs the non-destructive peek on the queue's OWNING worker
// (same JobDrainPeek machinery as drain Step A — single-owner discipline) and
// renders the page. Blocking; called on a spawned goroutine only. Like Step A,
// the wait may safely time out: a peek consumes nothing.
func (b *Broker) peekAndRenderQueue(ref queueRef, start int) string {
	if b.Queue == nil || b.Workers == nil {
		return "⚠️ durable queue disabled for this run — nothing to show"
	}
	peekCh := make(chan DrainPeekResult, 1)
	if !b.Workers.Submit(ref.key, Job{Kind: JobDrainPeek, DrainPeek: &DrainPeekJob{ResultCh: peekCh}}) {
		return "⚠️ broker busy (worker queue full or stopped) — try again shortly"
	}
	select {
	case r := <-peekCh:
		if r.Err != nil {
			return fmt.Sprintf("⚠️ reading «%s» failed: %v", ref.name, r.Err)
		}
		return renderQueueMessages(ref.name, r.Pending, start)
	case <-time.After(workerJobTimeout):
		return fmt.Sprintf("⚠️ «%s»'s worker did not respond within %s (it may be mid-transcription) — try again shortly", ref.name, workerJobTimeout)
	}
}

// queuePageSize is the /queue <q> pagination window (25/page keeps a worst-case
// page well under Telegram's 4096-char message cap).
const queuePageSize = 25

// queueMsgBudget is the self-imposed byte ceiling for one /queue <q> reply,
// under Telegram's 4096 cap with margin for the sender's own formatting.
const queueMsgBudget = 4000

// renderQueueMessages renders one page of a queue's pending lines: oldest-first
// ordinals (the SAME ordinals /drain selectors address — A2: ordinals number
// LINES, exactly the frozen snapshot order), kind icon + preview + sender +
// age, kind counts in the header (content-class info lives HERE, behind the
// operator gate — A3), pagination footer, and a copy-pasteable drain hint.
func renderQueueMessages(name string, pending []c3types.Inbound, start int) string {
	total := len(pending)
	if total == 0 {
		return fmt.Sprintf("📥 «%s» is empty — nothing queued.", name)
	}
	if start > total {
		return fmt.Sprintf("📥 «%s» has %d queued — start %d is past the end.", name, total, start)
	}
	end := start + queuePageSize - 1
	if end > total {
		end = total
	}
	msg := buildQueuePage(name, pending, start, end, total, drainPreviewMax)
	if len(msg) > queueMsgBudget {
		// Degrade gracefully (emoji-heavy previews can inflate bytes): shorter
		// previews first, hard rune-safe truncation as the last resort.
		msg = buildQueuePage(name, pending, start, end, total, 24)
	}
	if len(msg) > queueMsgBudget {
		msg = truncateAtRune(msg, queueMsgBudget) + "\n…"
	}
	return msg
}

// buildQueuePage assembles one page with the given per-line preview cap.
func buildQueuePage(name string, pending []c3types.Inbound, start, end, total, previewMax int) string {
	var sb strings.Builder
	voice, text := 0, 0
	for i := range pending {
		if isVoiceKind(&pending[i]) {
			voice++
		} else {
			text++
		}
	}
	fmt.Fprintf(&sb, "📥 «%s» · %d queued", name, total)
	if voice > 0 {
		fmt.Fprintf(&sb, " · 🎤%d", voice)
	}
	if text > 0 {
		fmt.Fprintf(&sb, " · 💬%d", text)
	}
	sb.WriteString("\n")
	for i := start - 1; i < end; i++ {
		in := &pending[i]
		icon := "💬"
		if isVoiceKind(in) {
			icon = "🎤"
		}
		var preview string
		if isSTTFailurePlaceholder(in.Text) {
			// R15: a failed transcription renders as a human note, never the raw
			// recovery-instruction marker.
			icon = "🎤"
			preview = "(transcription failed)"
		} else {
			preview = previewLine(in, previewMax)
		}
		fmt.Fprintf(&sb, "\n%d. %s %s", i+1, icon, preview)
		if lbl := senderLabel(in.Sender); lbl != "" {
			sb.WriteString(" · " + lbl)
		}
		if !in.Timestamp.IsZero() && in.Timestamp.Unix() > 0 {
			sb.WriteString(" · " + ageBand(in.Timestamp))
		}
	}
	sb.WriteString("\n")
	tok := cmdNameToken(name)
	if end < total {
		fmt.Fprintf(&sb, "\nshowing %d–%d of %d · /queue %s %d", start, end, total, tok, end+1)
	} else if start > 1 {
		fmt.Fprintf(&sb, "\nshowing %d–%d of %d", start, end, total)
	}
	fmt.Fprintf(&sb, "\nDrain: /drain %s %d-%d [to <topic>]", tok, start, end)
	return sb.String()
}

// isVoiceKind reports whether an inbound's leading attachment is audio-class
// (the 🎤 icon / kind-count bucket).
func isVoiceKind(in *c3types.Inbound) bool {
	if len(in.Attachments) == 0 {
		return false
	}
	switch in.Attachments[0].Kind {
	case "voice", "audio", "video_note":
		return true
	}
	return false
}

// isSTTFailurePlaceholder detects a stored line whose Text is the STT-failure
// recovery message (worker.go sttFailureText) or the builtin's raw marker.
// Contains (not HasPrefix) so a drained-in line — whose Text gained a
// provenance banner prefix — still renders as a failed transcription.
func isSTTFailurePlaceholder(text string) bool {
	return strings.Contains(text, "[voice transcription failed") ||
		strings.Contains(text, "[STT FAILED:")
}

// (Sender attribution reuses attach.go's senderLabel — "@name" / "uid=123".)

// previewLine renders a short single-line preview of a stored line: newlines
// collapse to spaces, over-long text truncates on a rune boundary, a text-less
// media line names its attachment kind. drainPreview delegates here.
func previewLine(in *c3types.Inbound, max int) string {
	text := strings.TrimSpace(strings.ReplaceAll(in.Text, "\n", " "))
	if text == "" {
		if len(in.Attachments) > 0 {
			return "(" + in.Attachments[0].Kind + ")"
		}
		return "(no text)"
	}
	r := []rune(text)
	if len(r) > max {
		return string(r[:max]) + "…"
	}
	return text
}

// truncateAtRune cuts s to at most maxBytes bytes on a rune boundary.
func truncateAtRune(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	i := maxBytes
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return s[:i]
}

// --- /drain command ---------------------------------------------------------------

// drainCommand handles "/drain <src> <sel> [to <t>]". Operator-gated (INV-7,
// silent drop). Parse + resolve run synchronously (mappings + status index
// only); the drain itself runs on a spawned goroutine that posts its own reply
// to the topic the command was typed in (A1).
func (b *Broker) drainCommand(in *c3types.Inbound, rest string) (string, bool) {
	if in.Sender.UserID == 0 || !b.Mappings().IsUserAllowed(in.Sender.UserID) {
		log.Printf("drain command DENY (silent) chan=%s chat=%d sender=%d", in.Channel, in.ChatID, in.Sender.UserID)
		return "", true
	}
	scope := b.commandScope(in)
	p, perr := parseDrainCommand(rest)
	if perr != "" {
		return perr, true
	}
	src, rej := b.resolveQueueRef(p.src, scope, refSource)
	if rej != "" {
		return rej, true
	}
	var dst queueRef
	if p.dst == nil {
		// Default target: the route the command was typed in (§2).
		dst = queueRef{
			key:  MakeRouteKey(in.Channel, in.ChatID, in.TopicID),
			name: b.topicDisplayName(in.Channel, in.ChatID, in.TopicID),
		}
	} else {
		dst, rej = b.resolveQueueRef(*p.dst, scope, refTarget)
		if rej != "" {
			return rej, true
		}
	}
	if src.key == dst.key {
		return "⚠️ source and target are the same queue — pick a different target", true
	}
	// DP-1 friction (no confirm card in v1): an `all`-drain or a cross-CHAT
	// drain must address its SOURCE by name — a serial is one stale /queue away
	// from pointing at the wrong queue for exactly the highest-blast-radius
	// moves. The reject names the resolution so the confirm is one paste away.
	if src.bySerial && (p.sel.Kind == SelectAll || src.key.ChatID != dst.key.ChatID) {
		return fmt.Sprintf("⚠️ serial %s = «%s» — an all/cross-chat drain must name the source to confirm: /drain %s …", p.src.text, src.name, cmdNameToken(src.name)), true
	}
	ch, cerr := b.Channel(in.Channel)
	if cerr != nil {
		return "⚠️ channel unavailable: " + cerr.Error(), true
	}
	spec := DrainSpec{Source: src.key, Target: dst.key, SourceName: src.name, TargetName: dst.name, Selector: p.sel}
	// MarkupNone (security): the reply echoes operator-supplied names and the
	// first-message preview (attacker-controlled body) — plain text keeps them
	// inert (see queueCommand).
	reply := c3types.ReplyArgs{Channel: in.Channel, ChatID: in.ChatID, TopicID: copyTopicPtr(in.TopicID), Markup: c3types.MarkupNone}
	// A1: Broker.Drain blocks (B1: Steps B/C wait indefinitely on their durable
	// mutations) — never on the poll goroutine.
	go func() {
		defer recoverGoroutine("broker.drainCommand")
		res, derr := b.Drain(spec)
		reply.Text = renderDrainReply(res, derr)
		if _, serr := ch.SendReply(reply); serr != nil {
			log.Printf("drain command reply send failed chat=%d topic=%s: %v", reply.ChatID, TopicPtrStr(reply.TopicID), serr)
		}
	}()
	return "", true
}

// copyTopicPtr clones a *int64 topic id so a captured reply target can never
// alias the inbound's pointer.
func copyTopicPtr(t *int64) *int64 {
	if t == nil {
		return nil
	}
	v := *t
	return &v
}

// renderDrainReply renders a DrainResult (or its typed error) into the reply
// posted where the command was typed: resolved names echoed, ordinal window,
// moved count, first preview, target total, every warning, clamp note (§4).
func renderDrainReply(res DrainResult, err error) string {
	if err != nil {
		return renderDrainError(&res, err)
	}
	landed := res.Appended + res.PresenceSkipped
	window := fmt.Sprintf("ordinals %d-%d", res.WindowLo, res.WindowHi)
	if res.WindowLo == res.WindowHi {
		window = fmt.Sprintf("ordinal %d", res.WindowLo)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "↩︎ drained «%s» → «%s»: %d message(s) (%s, oldest-first)", res.SourceName, res.TargetName, landed, window)
	if res.Clamped {
		fmt.Fprintf(&sb, "\nclamped: only %d were pending", res.Requested)
	}
	if res.PresenceSkipped > 0 {
		fmt.Fprintf(&sb, "\n%d already in the target from an earlier attempt (skipped, not doubled)", res.PresenceSkipped)
	}
	if res.FirstPreview != "" {
		fmt.Fprintf(&sb, "\nfirst: %q", res.FirstPreview)
	}
	fmt.Fprintf(&sb, "\n«%s» now has %d queued", res.TargetName, res.TargetPending)
	for _, w := range res.Warnings {
		if strings.HasPrefix(w, "⚠") {
			sb.WriteString("\n" + w)
		} else {
			sb.WriteString("\n⚠️ " + w)
		}
	}
	return sb.String()
}

// renderDrainError renders Drain's typed errors with their operator-facing
// corrections (§7 edge table).
func renderDrainError(res *DrainResult, err error) string {
	var rbp *RangeBeyondPendingError
	var badsel *BadSelectorError
	var empty *EmptySourceError
	var inprog *DrainInProgressError
	var busy *DrainBusyError
	switch {
	case errors.As(err, &rbp):
		if rbp.Lo > rbp.Pending {
			return fmt.Sprintf("⚠️ queue «%s» has only %d pending — range %d-%d starts past the end; try all or a range within 1-%d",
				res.SourceName, rbp.Pending, rbp.Lo, rbp.Hi, rbp.Pending)
		}
		return fmt.Sprintf("⚠️ queue «%s» has %d pending — try %d-%d or all", res.SourceName, rbp.Pending, rbp.Lo, rbp.Pending)
	case errors.As(err, &badsel):
		return "⚠️ " + badsel.Reason + " — " + drainGrammarHint
	case errors.As(err, &empty):
		return "📭 " + empty.Error()
	case errors.As(err, &inprog):
		return "⏳ " + inprog.Error()
	case errors.As(err, &busy):
		// DrainBusyError's own text carries the converges-on-reissue guidance
		// when copies landed (CopiesLanded) — render it verbatim.
		return "⚠️ " + busy.Error()
	case errors.Is(err, ErrDrainSameRoute):
		return "⚠️ source and target are the same queue — pick a different target"
	}
	return "⚠️ " + err.Error()
}
