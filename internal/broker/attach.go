package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// handleAttach is the broker-side attach proposal flow per spec §5.2-§5.5.
//
// Logic:
//
//  1. Parse AttachReq.
//  2. Resolve channel (args.Channel or default).
//  3. Branch by target type:
//     a. args.Target == "dm" → claim (channel, dm_chat_id, nil), no per-cwd
//     persistence (DM is universal).
//     b. args.TopicID != nil → validate via channel.ValidateTopic + claim by
//     id; register topic in mappings.json:channels.<name>.topics with a
//     placeholder name if not already present; persist cwd mapping.
//     c. args.Name != "" (or args.Create) → attachByName: search the topic
//     registry. If found in default group → claim. If found in another
//     group → propose disambiguation. If found nowhere → propose creation.
//     d. Otherwise (BARE — no name/target/topic_id/create) → attachBare:
//     idempotent guard → the session's OWN recover (the only silent path)
//     → picker. A bare attach never synthesizes a name from cwd nor
//     silently claims a saved cwd→topic mapping (spec §1-§2).
//  4. On args.Create == true → call channel.CreateTopic, register topic,
//     claim, persist mapping.
func (b *Broker) handleAttach(conn *ipc.Conn, stub *Stub, raw []byte) {
	var req ipc.AttachReq
	if err := json.Unmarshal(raw, &req); err != nil {
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: false,
			Err: "malformed attach: " + err.Error(),
		})
		return
	}

	// Policy-rejected hint: the CLI host's policy layer rejected the
	// prior attach (e.g. Codex approvals_reviewer="auto_review"
	// surfacing an "unacceptable risk rejection"). The adapter is
	// re-invoking with this hint so we surface a clean structured
	// status — no claim, no validate, no topic registration, no
	// channel resolution. The broker can't detect the underlying
	// rejection itself (it lives upstream of the adapter in the CLI
	// host); the hint is the agent's observation passed through.
	// See docs/plans/2026-05-19-codex-policy-3state.md.
	if req.PolicyRejected {
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: false,
			Status: ipc.AttachStatusPolicyRejected,
			Err:    "CLI host policy layer rejected attach; tenant admin must approve the Telegram destination before retry",
		})
		return
	}

	chanName := req.Channel
	if chanName == "" {
		chanName = b.defaultChannel()
	}
	if chanName == "" {
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: false,
			Status: ipc.AttachStatusNoTopicsConfigured,
			Err:    "no channel registered; configure mappings.json:channels.<name>",
		})
		return
	}

	// If the caller passed a freeform Expr, parse it into structured fields
	// before dispatching. This is the shared parser every CLI's slash-command
	// wrapper invokes via `attach(expr=$ARGUMENTS)` — keeps each CLI's
	// per-command file a one-liner with no duplicated parsing.
	if req.Expr != "" {
		applyExprToAttachReq(&req)
	}

	switch {
	case strings.EqualFold(req.Target, "dm"):
		b.attachDM(conn, stub, chanName, req.Steal, req.Replay)
	case req.TopicID != nil:
		b.attachByTopicID(conn, stub, chanName, req.ChatID, *req.TopicID, req.Group, req.Steal, req.Replay)
	default:
		// Explicit name (or create=true) → attachByName (path (i)): the user
		// typed a name, so an exact bind is inherently safe. A BARE attach (no
		// name, no create) → attachBare: idempotent guard → the session's OWN
		// recover (the only silent path) → picker. A bare attach NEVER
		// synthesizes a name from cwd nor silently claims a saved cwd→topic
		// mapping — that silent mis-target was the incident this redesign
		// closes (spec §1-§2). Empty cwd is fine: it falls to the picker, not
		// an error.
		if req.Name != "" || req.Create {
			b.attachByName(conn, stub, chanName, req.Name, req.CWD, req.Group, req.Create, req.Steal, req.Replay)
			return
		}
		b.attachBare(conn, stub, chanName, req.CWD, req.Group, req.Steal, req.Replay)
	}
}

// attachBare handles a BARE attach — no explicit name, target, topic_id, or
// create. It is the ONLY path that can silently claim, and even that is
// restricted to the session's OWN previously-recorded route. Resolution order
// (spec §1):
//
//  1. Idempotent: the stub is already attached (CurrentRoute != nil) → report
//     that route, no re-claim, no re-peek. The response carries the resolved
//     Channel/ChatID/TopicID/Name so FormatAttached can render it (and the
//     adapter's replay-remember can record the resolved identity).
//  2. Own recover — the ONLY silent claim in the system: a recoverable
//     session_attachment for the stub's STABLE session id → silently re-claim
//     it. Ungated (a manual bare attach is user-initiated; the auto-resume gate
//     governs only the automatic handleRecoverSession path). recoverSession
//     re-claims the session's OWN route only — a mis-target is impossible by
//     construction.
//  3. Otherwise → picker: NEVER claims. cwd only SEEDS a suggestion (Phase 2).
//     Phase 1 emits the minimal pick_topic proposal; the ranked suggestion list
//     and its host-neutral formatter case land in Phase 2.
//
// cwd/group/steal/replay are threaded for the Phase-2 picker; Phase 1 reads only
// the stub's own state.
func (b *Broker) attachBare(conn *ipc.Conn, stub *Stub, chanName, cwd, group string, steal, replay bool) {
	// (i) Already attached — idempotent OK, no re-claim, no re-peek.
	if cur := stub.CurrentRoute(); cur != nil {
		name, groupName := "dm", ""
		var topicID *int64
		if cur.HasTopic {
			t := cur.TopicID
			topicID = &t
			if tp, ok := b.Mappings().LookupTopicByID(cur.Channel, cur.ChatID, cur.TopicID); ok {
				name = tp.Name
				groupName = tp.Group
			} else {
				name = fmt.Sprintf("topic-%d", cur.TopicID)
			}
		}
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: true,
			Status:  ipc.AttachStatusOK,
			Channel: cur.Channel, ChatID: cur.ChatID, TopicID: topicID,
			Name: name, Group: groupName,
			Capabilities: b.capsForChannel(cur.Channel),
		})
		return
	}

	// (ii) Own recover — the ONLY silent claim. Ungated: a manual bare attach
	// re-claims the session's own recorded route even for a user who disabled
	// the AUTOMATIC auto-attach-on-resume gate. recoverSession sets the route and
	// returns the queued count AND preview from a SINGLE backlogSummary peek —
	// stamp both straight onto the response (no withBacklog re-peek, which would
	// re-introduce the §3c double-peek TOCTOU on this manual path).
	if key, cnt, preview, ok := b.recoverSession(stub); ok {
		name, groupName := "dm", ""
		if sa, ok := b.Mappings().LookupSessionAttachment(stub.StableSessionIDValue()); ok {
			name = sa.Name
			groupName = sa.Group
		}
		if name == "" {
			if key.HasTopic {
				if tp, ok := b.Mappings().LookupTopicByID(key.Channel, key.ChatID, key.TopicID); ok {
					name = tp.Name
					groupName = tp.Group
				} else {
					name = fmt.Sprintf("topic-%d", key.TopicID)
				}
			} else {
				name = "dm"
			}
		}
		var topicID *int64
		if key.HasTopic {
			t := key.TopicID
			topicID = &t
		}
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: true,
			Status:  ipc.AttachStatusOK,
			Channel: key.Channel, ChatID: key.ChatID, TopicID: topicID,
			Name: name, Group: groupName,
			QueuedCount: cnt, QueuedSummary: preview,
			Capabilities: b.capsForChannel(key.Channel),
		})
		return
	}

	// (iii) Picker — NEVER claims. buildPickTopic seeds a cwd-derived
	// "current project" suggestion + recently-used topics; the human picks and
	// the agent re-invokes with an explicit topic_id/create (that re-invoke is
	// the only thing that claims). The host-neutral FormatAttached "pick_topic"
	// case renders each suggestion's exact re-invoke command.
	_ = conn.WriteJSON(ipc.AttachedMsg{
		Op: ipc.OpAttached, OK: false,
		NeedsConfirmation: true,
		Proposal:          b.buildPickTopic(stub, chanName, cwd),
	})
}

// maxPickOptions bounds the host AskUserQuestion budget for the friendly picker:
// the shown suggestions plus an optional "See the full list" row must fit within
// this many options (spec §4). The create row is itself a suggestion.
const maxPickOptions = 4

// buildPickTopic assembles the ranked suggestion set for a bare attach that
// resolved to neither an existing claim nor the session's own recorded route
// (attachBare branch iii). It NEVER claims — a claim only ever happens when the
// agent re-invokes attach with an explicit topic_id / create after the human
// picks. Ranking (spec §4), capped at 3 suggestions:
//
//  1. Current project — cwd → LookupByCwd, else basename(cwd) in the default
//     group, else basename(cwd) across groups (offered with its Group so the
//     re-invoke disambiguates instead of minting a duplicate), else a
//     "create <project>" row. Skipped entirely when cwd is empty; the create
//     row is omitted when the channel has no default group.
//  2. Recently used — the 1–2 newest session_attachments (by LastAttachedAt),
//     deduped by route, registry-validated (a DM entry is exempt — validated by
//     a configured dm_chat_id), TTL-filtered, tombstone-ALLOWED (a deliberate
//     detach blocks silent recovery, not an explicit pick).
//  3. ClaimedBy — a live-held suggestion is marked so the human is warned; the
//     re-invoke still goes through the normal force_steal confirmation.
//  4. HasMore — true when the registry holds more EXISTING topics than the
//     picker shows (FormatAttached then offers "See the full list"). It counts
//     only shown suggestions that reference a real registry topic (a create row
//     and a DM row are not registry topics), so a hidden topic is never masked
//     by a non-topic row. A final ≤4-option budget trim (suggestions + the
//     optional full-list row) keeps the host AskUserQuestion cap.
//
// stub is threaded for signature parity with the other attach helpers (and for a
// future same-session ClaimedBy suppression); the picker itself reads only the
// mappings + the live route table. Returns a *ipc.Proposal (Action "pick_topic")
// kept pure of any conn write so ranking is unit-testable.
func (b *Broker) buildPickTopic(stub *Stub, chanName, cwd string) *ipc.Proposal {
	_ = stub
	mf := b.Mappings()
	cc, hasChan := mf.Channels[chanName]

	var suggestions []ipc.PickSuggestion
	seen := map[RouteKey]bool{}
	project := ""

	markClaim := func(s *ipc.PickSuggestion, key RouteKey) {
		if holder, ok := b.Routes.Holder(key); ok && holder.IsAlive() {
			s.ClaimedBy = &ipc.Holder{CLI: holder.CLI, PID: holder.PID, CWD: holder.CWD}
		}
	}

	// (1) Current project.
	if cwd != "" {
		project = filepath.Base(cwd)
		var cur *ipc.PickSuggestion

		// (1a/1b) A saved cwd→route mapping for this channel takes priority — but it
		// must be VALIDATED like the recents arm (item H2): a stale mapping whose
		// topic was deleted from the registry must NOT render an unattachable
		// topic_id re-invoke NOR inflate the HasMore "shown" count. TopicID==0 is
		// the legacy DM marker (the deleted PATH A normalized a DM cwd mapping to
		// TopicID 0), rendered as a DM row (item H1). On a registry miss, leave cur
		// nil so the basename/create arms below get a chance (fall through).
		if m, ok := mf.LookupByCwd(cwd); ok && m.Channel == chanName {
			if m.TopicID == 0 {
				// Legacy DM cwd mapping (TopicID==0). Render the DM row ONLY when the
				// channel has a configured DM chat (parity with the recents arm) — else
				// picking it errors "dm_chat_id not set". Key it on cc.DMChatID (not the
				// mapping's stored ChatID) so it uses the SAME route key as the recents
				// DM row and the two dedupe into one "dm" row. Unconfigured → leave cur
				// nil so the basename/create arms below get a chance (fall through).
				if hasChan && cc.DMChatID != 0 {
					cur = &ipc.PickSuggestion{
						Kind: "attach_existing", Reason: "current project",
						Name: "dm", ChatID: cc.DMChatID, // TopicID nil → renders as target="dm"
					}
				}
			} else if tp, okReg := mf.LookupTopicByID(chanName, m.ChatID, m.TopicID); okReg {
				tid := tp.TopicID
				grp := ""
				if hasChan && tp.Group != cc.DefaultGroup {
					grp = tp.Group
				}
				cur = &ipc.PickSuggestion{
					Kind: "attach_existing", Reason: "current project",
					Name: tp.Name, Group: grp, ChatID: tp.ChatID, TopicID: &tid, // registry-authoritative name/group
				}
			}
			// else: mapping points at a topic no longer in the registry → fall through.
		}

		// (1c) No usable cwd mapping → seed from basename(cwd).
		if cur == nil {
			if tp, ok := mf.LookupTopicInDefaultGroup(chanName, project); ok {
				tid := tp.TopicID
				cur = &ipc.PickSuggestion{
					Kind: "attach_existing", Reason: "current project",
					Name: tp.Name, ChatID: tp.ChatID, TopicID: &tid, // default group → no group arg
				}
			} else if hits := mf.LookupTopicAcrossGroups(chanName, project); len(hits) > 0 {
				tp := hits[0]
				tid := tp.TopicID
				cur = &ipc.PickSuggestion{
					Kind: "attach_existing", Reason: "current project",
					Name: tp.Name, Group: tp.Group, ChatID: tp.ChatID, TopicID: &tid,
				}
			} else if hasChan && cc.DefaultGroup != "" {
				// Found nowhere → offer to create <project> in the default group.
				cur = &ipc.PickSuggestion{
					Kind: "create", Reason: "current project (new)", Name: project,
				}
			}
		}

		if cur != nil {
			// Mark seen + claim for any attach_existing row, INCLUDING the DM row
			// (TopicID nil → MakeRouteKey builds the DM key) so a duplicate DM
			// recents row is deduped. A create row references no route.
			if cur.Kind == "attach_existing" {
				key := MakeRouteKey(chanName, cur.ChatID, cur.TopicID)
				seen[key] = true
				markClaim(cur, key)
			}
			suggestions = append(suggestions, *cur)
		}
	}

	// (2) Recently used — newest session_attachments, filtered.
	for _, sa := range recentSessionAttachments(mf, chanName) {
		if len(suggestions) >= 3 {
			break
		}
		var key RouteKey
		var suggestion ipc.PickSuggestion
		if sa.TopicID == nil {
			// DM recent — validated by a configured DM chat, not the registry.
			if !hasChan || cc.DMChatID == 0 {
				continue
			}
			key = MakeRouteKey(chanName, cc.DMChatID, nil)
			suggestion = ipc.PickSuggestion{
				Kind: "attach_existing", Reason: "recently used",
				Name: "dm", ChatID: cc.DMChatID, // TopicID nil → renders as target="dm"
			}
		} else {
			// Drop entries whose topic no longer resolves — a topic_id re-invoke
			// would fail ValidateTopic. Use the registry's current name/group so
			// the suggestion (and its group arg) is authoritative, not stale.
			tp, ok := mf.LookupTopicByID(sa.Channel, sa.ChatID, *sa.TopicID)
			if !ok {
				continue
			}
			tid := tp.TopicID
			grp := ""
			if hasChan && tp.Group != cc.DefaultGroup {
				grp = tp.Group
			}
			key = MakeRouteKey(chanName, tp.ChatID, &tid)
			suggestion = ipc.PickSuggestion{
				Kind: "attach_existing", Reason: "recently used",
				Name: tp.Name, Group: grp, ChatID: tp.ChatID, TopicID: &tid,
			}
		}
		if seen[key] {
			continue // dedupe by route (many session ids can record the same topic)
		}
		seen[key] = true
		markClaim(&suggestion, key)
		suggestions = append(suggestions, suggestion)
	}

	// (4) HasMore + the ≤maxPickOptions budget trim. Dropping a shown existing
	// topic raises the hidden count, so recompute HasMore each pass; the loop
	// terminates because len(suggestions) strictly decreases.
	var totalExisting int
	if hasChan {
		totalExisting = len(cc.Topics)
	}
	hasMore := false
	for {
		shown := 0
		for i := range suggestions {
			if suggestions[i].Kind == "attach_existing" && suggestions[i].TopicID != nil {
				shown++
			}
		}
		hasMore = totalExisting > shown
		rows := len(suggestions)
		if hasMore {
			rows++
		}
		if rows <= maxPickOptions || len(suggestions) == 0 {
			break
		}
		suggestions = suggestions[:len(suggestions)-1]
	}

	return &ipc.Proposal{
		Action:      "pick_topic",
		Channel:     chanName,
		Project:     project,
		Suggestions: suggestions,
		HasMore:     hasMore,
	}
}

// recentSessionAttachments returns the channel's session attachments newest-first
// (by LastAttachedAt), filtered to the picker's TTL and channel. Tombstoned
// entries are KEPT (a deliberate detach blocks silent recovery, not an explicit
// pick). A stable secondary sort keeps picker ranking deterministic for tests.
func recentSessionAttachments(mf *mappings.MappingsFile, chanName string) []mappings.SessionAttachment {
	if mf == nil {
		return nil
	}
	now := time.Now()
	var out []mappings.SessionAttachment
	for _, sa := range mf.SessionAttachments {
		if sa.Channel != chanName {
			continue
		}
		if now.Sub(sa.LastAttachedAt) >= SessionAttachmentTTL {
			continue // TTL-expired (the age arm of Recoverable); tombstones are kept.
		}
		out = append(out, sa)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].LastAttachedAt.Equal(out[j].LastAttachedAt) {
			return out[i].LastAttachedAt.After(out[j].LastAttachedAt)
		}
		return sessionAttachmentSortKey(out[i]) < sessionAttachmentSortKey(out[j])
	})
	return out
}

// sessionAttachmentSortKey is a deterministic tiebreak for equal LastAttachedAt.
func sessionAttachmentSortKey(sa mappings.SessionAttachment) string {
	tid := int64(0)
	if sa.TopicID != nil {
		tid = *sa.TopicID
	}
	return fmt.Sprintf("%s\x00%d\x00%d", sa.Name, sa.ChatID, tid)
}

// applyExprToAttachReq parses the user-supplied freeform argument string and
// fills in the structured fields. Rules (documented in the AttachReq.Expr
// godoc and docs/COMMANDS.md):
//
//	""                          → leave fields untouched → BARE attach (attachBare:
//	                              idempotent guard → session's OWN recover → picker;
//	                              NEVER a silent cwd-saved claim — spec §1-§2)
//	"dm" / "DM" (case-insens)   → Target = "dm"
//	"<int>"                     → TopicID = <int>
//	"-y <name>" / "yes <name>" / "create <name>"
//	                            → Name = <name>, Create = true
//	"<anything else>"           → Name = <string>
//
// Whitespace is trimmed; unparsable input falls through to Name with the
// raw string so the broker can tell the user "no topic by that name; want
// to create?". The "create" prefix forms map to the existing Create flag —
// users who want to skip the propose/confirm round-trip type `/c3:attach
// create my-topic` and the broker creates it on the spot.
func applyExprToAttachReq(req *ipc.AttachReq) {
	expr := strings.TrimSpace(req.Expr)
	if expr == "" {
		return
	}
	if strings.EqualFold(expr, "dm") {
		req.Target = "dm"
		return
	}
	// Numeric → topic id.
	if n, err := strconv.ParseInt(expr, 10, 64); err == nil {
		v := n
		req.TopicID = &v
		return
	}
	// Create-prefixed forms.
	for _, p := range []string{"-y ", "--yes ", "yes ", "create "} {
		if strings.HasPrefix(strings.ToLower(expr), p) {
			req.Name = strings.TrimSpace(expr[len(p):])
			req.Create = true
			return
		}
	}
	req.Name = expr
}

// attachDM claims the user's 1-on-1 chat with the bot. Spec §5.5: never
// persists a per-cwd mapping; DM is universal across cwds.
//
// DM disambiguation (2026-05-09): if a topic named "dm"
// (case-insensitive) exists in the channel, we can't tell whether the user
// meant the actual Telegram DM or that topic. Surface as needs_confirmation
// with a "disambiguate_dm" proposal — LLM asks the user. If they want the
// topic, agent re-invokes `attach name="dm"` (or topic_id); for the actual
// DM, agent re-invokes with `attach target="dm"` and a confirm flag (TBD)
// or just agrees by sending steal=true to bypass. For now: agent re-issues
// using the explicit form the user chose.
func (b *Broker) attachDM(conn *ipc.Conn, stub *Stub, chanName string, steal, replay bool) {
	cc, ok := b.Mappings().Channels[chanName]
	if !ok {
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: false,
			Status: ipc.AttachStatusNoTopicsConfigured,
			Err:    fmt.Sprintf("attach: channel %q not in mappings.json", chanName),
		})
		return
	}
	if cc.DMChatID == 0 {
		// DM destination unconfigured. Whether topics exist or not, the
		// user has a partial-config gap; surface the structured status so
		// the formatter renders the actionable "run `c3-broker setup`"
		// message instead of the generic Err string.
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: false,
			Status: ipc.AttachStatusNoTopicsConfigured,
			Err:    fmt.Sprintf("attach dm: channels.%s.dm_chat_id not set in mappings.json", chanName),
		})
		return
	}

	// Disambiguation: a "dm"-named topic in the channel makes the request
	// ambiguous. Skip disambiguation if the caller already steered past it
	// by passing steal=true (which here we interpret as "I confirmed I want
	// the actual DM, just attach").
	if !steal {
		for _, tp := range cc.Topics {
			if strings.EqualFold(tp.Name, "dm") {
				_ = conn.WriteJSON(ipc.AttachedMsg{
					Op: ipc.OpAttached, OK: false,
					NeedsConfirmation: true,
					Proposal: &ipc.Proposal{
						Action:  "disambiguate_dm",
						Channel: chanName,
						Group:   tp.Group,
						Name:    tp.Name,
						Existing: &ipc.TopicEntry{
							Channel: chanName, ChatID: tp.ChatID,
							TopicID: tp.TopicID, Name: tp.Name, Group: tp.Group,
						},
					},
				})
				return
			}
		}
	}

	key := MakeRouteKey(chanName, cc.DMChatID, nil)
	if !b.tryClaim(conn, stub, key, "DM", steal, replay) {
		return
	}
	// Record the recovery entry so a resumed DM session re-attaches. The DM
	// route is universal and deliberately never cwd-mapped, so persistMapping
	// (which also writes a cwd default) is the wrong tool here — record the
	// session attachment only, keyed on the session id (nil TopicID = DM).
	b.recordSessionAttachment(stub, chanName, cc.DMChatID, nil, "dm", "")
	_ = conn.WriteJSON(b.withBacklog(key, ipc.AttachedMsg{
		Op:           ipc.OpAttached,
		OK:           true,
		Status:       ipc.AttachStatusOK,
		Channel:      chanName,
		ChatID:       cc.DMChatID,
		Name:         "dm",
		Capabilities: b.capsForChannel(chanName),
	}))
}

// attachByTopicID validates a topic id against the channel (cheap typing
// action) and, if valid, claims it. Adds to topics registry as `topic-<n>`
// if not already known. Persists cwd mapping if cwd is provided.
//
// chatID is an OPTIONAL fail-closed cross-check (0 = skip): when non-zero it must
// equal the chat the named group resolves to, or the attach is refused — this
// stops an id-addressed replay with a mismatched/absent group from binding a
// same-id thread in the wrong chat (item 3).
func (b *Broker) attachByTopicID(conn *ipc.Conn, stub *Stub, chanName string, chatID int64, topicID int64, groupName string, steal, replay bool) {
	cc, ok := b.Mappings().Channels[chanName]
	if !ok {
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: false,
			Err: fmt.Sprintf("attach: channel %q not in mappings.json", chanName),
		})
		return
	}
	gName, gCfg, ok := b.resolveGroup(cc, groupName)
	if !ok {
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: false,
			Err: fmt.Sprintf("attach: group %q not in mappings.json:channels.%s.groups", groupName, chanName),
		})
		return
	}
	// Fail-closed cross-check for id-addressed replays (item 3): if the caller
	// supplied the topic's chat, it MUST equal the chat the named group resolves
	// to. A remembered Group=="" for a topic that actually lived in a non-default
	// group would otherwise replay against the DEFAULT group's chat, and a
	// coincidental same-id live thread there would pass ValidateTopic — silently
	// claiming the WRONG topic. Refuse (fail-detached; do NOT probe other groups).
	// ChatID==0 means "no cross-check" → today's behavior is preserved.
	if chatID != 0 && gCfg.ChatID != chatID {
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: false,
			Err: fmt.Sprintf("attach --topic=%d: chat_id cross-check failed (group %q resolves to chat %d, replay expected chat %d) — refusing to claim a possibly-wrong topic",
				topicID, gName, gCfg.ChatID, chatID),
		})
		return
	}
	ch, err := b.Channel(chanName)
	if err != nil {
		_ = conn.WriteJSON(ipc.AttachedMsg{Op: ipc.OpAttached, OK: false, Err: err.Error()})
		return
	}
	if err := ch.ValidateTopic(gCfg.ChatID, topicID); err != nil {
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: false,
			Err: fmt.Sprintf("attach --topic=%d: %v", topicID, err),
		})
		return
	}

	// Register topic in registry if absent. Check-then-upsert under the
	// mutation lock so a concurrent attach for the same (chat, topic_id)
	// can't double-register.
	b.mutateMappings(func(mf *mappings.MappingsFile) {
		if _, exists := mf.LookupTopicByID(chanName, gCfg.ChatID, topicID); exists {
			return
		}
		mf.UpsertTopic(chanName, mappings.Topic{
			ChatID: gCfg.ChatID, TopicID: topicID,
			Name: fmt.Sprintf("topic-%d", topicID), Group: gName,
		})
	})

	tid := topicID
	key := MakeRouteKey(chanName, gCfg.ChatID, &tid)
	if !b.tryClaim(conn, stub, key, fmt.Sprintf("topic %d", topicID), steal, replay) {
		return
	}
	tp, _ := b.Mappings().LookupTopicByID(chanName, gCfg.ChatID, topicID)
	b.persistMapping(stub, chanName, gCfg.ChatID, topicID, tp.Name, gName)

	_ = conn.WriteJSON(b.withBacklog(key, ipc.AttachedMsg{
		Op:           ipc.OpAttached,
		OK:           true,
		Status:       ipc.AttachStatusOK,
		Channel:      chanName,
		ChatID:       gCfg.ChatID,
		TopicID:      &tid,
		Name:         tp.Name,
		Group:        gName,
		Capabilities: b.capsForChannel(chanName),
	}))
}

// attachByName runs the explicit-name search flow per spec §5.2-§5.4. It is
// reached ONLY for an explicit name (or create=true) — a bare attach with no
// name routes to attachBare, which never synthesizes a name from cwd nor
// consults the saved cwd→topic mapping (the silent mis-target class this
// redesign closes; spec §1-§2). Steps:
//
//  1. Search default group for `name` → if found, claim it.
//  2. Else search all groups → if found in non-default, propose
//     disambiguation (action="use_existing_other_group").
//  3. Else propose creation in default group (action="create").
//
// On any "propose" outcome the response carries needs_confirmation=true and
// a Proposal payload; the agent re-calls attach with create=true to confirm.
func (b *Broker) attachByName(conn *ipc.Conn, stub *Stub, chanName, name, cwd, groupName string, create, steal, replay bool) {
	cc, ok := b.Mappings().Channels[chanName]
	if !ok {
		_ = conn.WriteJSON(ipc.AttachedMsg{Op: ipc.OpAttached, OK: false,
			Err: fmt.Sprintf("attach: channel %q not in mappings.json", chanName)})
		return
	}

	gName, gCfg, ok := b.resolveGroup(cc, groupName)
	if !ok {
		_ = conn.WriteJSON(ipc.AttachedMsg{Op: ipc.OpAttached, OK: false,
			Err: fmt.Sprintf("attach: group %q not in mappings.json:channels.%s.groups", groupName, chanName)})
		return
	}

	// A bare attach never reaches here (it routes to attachBare), so name is
	// always the user-supplied explicit name. The old cwd-basename backfill is
	// deleted: a bare create=true now correctly errors rather than synthesizing
	// a name (the picker / explicit name is the tool for choosing a topic).
	if name == "" {
		_ = conn.WriteJSON(ipc.AttachedMsg{Op: ipc.OpAttached, OK: false,
			Err: "attach: provide cwd, name, target, or topic_id"})
		return
	}

	// 1. Default-group search.
	if tp, ok := b.Mappings().LookupTopicInDefaultGroup(chanName, name); ok && tp.Group == gName {
		// In the default group already — silent claim.
		tid := tp.TopicID
		key := MakeRouteKey(chanName, tp.ChatID, &tid)
		if !b.tryClaim(conn, stub, key, tp.Name, steal, replay) {
			return
		}
		b.persistMapping(stub, chanName, tp.ChatID, tp.TopicID, tp.Name, tp.Group)
		_ = conn.WriteJSON(b.withBacklog(key, ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: true,
			Status:  ipc.AttachStatusOK,
			Channel: chanName, ChatID: tp.ChatID, TopicID: &tid,
			Name: tp.Name, Group: tp.Group,
			Capabilities: b.capsForChannel(chanName),
		}))
		return
	}

	// 2. Cross-group search.
	allHits := b.Mappings().LookupTopicAcrossGroups(chanName, name)
	otherGroupHits := allHits[:0:0]
	for _, h := range allHits {
		if h.Group != gName {
			otherGroupHits = append(otherGroupHits, h)
		}
	}
	if len(otherGroupHits) > 0 && !create {
		// Propose disambiguation. Pick first hit; the agent can disambiguate
		// further if multiple exist.
		hit := otherGroupHits[0]
		alt := &ipc.Proposal{Action: "create", Channel: chanName, Group: gName, Name: name}
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: false,
			NeedsConfirmation: true,
			Proposal: &ipc.Proposal{
				Action:  "use_existing_other_group",
				Channel: chanName,
				Group:   hit.Group,
				Name:    hit.Name,
				Existing: &ipc.TopicEntry{
					Channel: chanName, ChatID: hit.ChatID,
					TopicID: hit.TopicID, Name: hit.Name, Group: hit.Group,
				},
				Alternative: alt,
			},
		})
		return
	}

	// 3. Propose or perform creation.
	if !create {
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: false,
			NeedsConfirmation: true,
			Proposal: &ipc.Proposal{
				Action: "create", Channel: chanName, Group: gName, Name: name,
			},
		})
		return
	}
	b.createAndClaim(conn, stub, chanName, gName, gCfg.ChatID, name, cwd, steal, replay)
}

// createAndClaim invokes channel.CreateTopic, registers the topic, claims, persists.
func (b *Broker) createAndClaim(conn *ipc.Conn, stub *Stub, chanName, gName string, chatID int64, name, cwd string, steal, replay bool) {
	ch, err := b.Channel(chanName)
	if err != nil {
		_ = conn.WriteJSON(ipc.AttachedMsg{Op: ipc.OpAttached, OK: false, Err: err.Error()})
		return
	}
	topicID, err := ch.CreateTopic(chatID, name)
	if err != nil {
		_ = conn.WriteJSON(ipc.AttachedMsg{Op: ipc.OpAttached, OK: false,
			Err: fmt.Sprintf("create topic %q: %v", name, err)})
		return
	}
	b.mutateMappings(func(mf *mappings.MappingsFile) {
		mf.UpsertTopic(chanName, mappings.Topic{
			ChatID: chatID, TopicID: topicID, Name: name, Group: gName,
		})
	})
	tid := topicID
	key := MakeRouteKey(chanName, chatID, &tid)
	if !b.tryClaim(conn, stub, key, name, steal, replay) {
		return
	}
	if cwd != "" {
		b.persistMapping(stub, chanName, chatID, topicID, name, gName)
	}
	_ = b.SaveMappings()

	_ = conn.WriteJSON(b.withBacklog(key, ipc.AttachedMsg{
		Op: ipc.OpAttached, OK: true,
		Status:  ipc.AttachStatusOK,
		Channel: chanName, ChatID: chatID, TopicID: &tid,
		Name: name, Group: gName,
		Capabilities: b.capsForChannel(chanName),
	}))
}

// heldByDifferentLiveSession reports whether key is currently claimed by a
// LIVE session that is NOT the caller (stub). Returns the holder when so.
//
// This mirrors the exact collision predicate Routes.Claim uses to decide
// whether a claim would be rejected (held + different-logical-session +
// IsAlive) — see routes.go. It's a read-only peek used by the SYMPTOM-3
// cwd-default collision check to surface a guided warning BEFORE attempting
// the claim, without duplicating the liveness rules. A same-logical-session
// holder (reconnect/self) or a dead holder is NOT a collision: the caller is
// (or supersedes) the holder and the claim would succeed anyway.
func (b *Broker) heldByDifferentLiveSession(key RouteKey, stub *Stub) (*Stub, bool) {
	holder, held := b.Routes.Holder(key)
	if !held {
		return nil, false
	}
	if sameLogicalSession(holder, stub) {
		return nil, false
	}
	if !holder.IsAlive() {
		return nil, false
	}
	return holder, true
}

// tryClaim attempts to add (key → stub) to ROUTES; on collision with a
// different alive holder, sends AttachedMsg with a force_steal proposal
// (the LLM-side asks the user; on confirmation, attach is re-invoked with
// steal=true).
//
// Single-claim-per-stub invariant (2026-05-09: "codex was attached
// to two topic IDs"): if this stub already holds a different route, that
// claim is released BEFORE the new one is granted. An adapter that wants
// to switch topics can do so with a single attach call; it will never end
// up holding two topics simultaneously.
//
// steal=true: the user has confirmed displacement of any existing holder.
// Force-release first, then claim. Only this path can evict a live PID's
// claim; everything else returns force_steal proposal for confirmation.
func (b *Broker) tryClaim(conn *ipc.Conn, stub *Stub, key RouteKey, label string, steal, replay bool) bool {
	// Determine whether to fire the on-attach welcome message. Two
	// suppression conditions:
	//   1. The adapter marked this attach as a replay (broker bounce or
	//      conn-drop recovery) — the user didn't ask, the adapter just
	//      transparently restored its claim.
	//   2. Same logical session is already holding this key — the
	//      claim is a no-op (re-attach during a single connection).
	isFresh := !replay
	if isFresh {
		if existing, held := b.Routes.Holder(key); held && sameLogicalSession(existing, stub) {
			isFresh = false
		}
	}

	// Atomic switch (2026-06-29 reliability fix C): claim the NEW route BEFORE
	// releasing the OLD one, and release the old one ONLY on a successful claim.
	// A failed claim (live collision) must leave the stub's existing route fully
	// intact — releasing first then failing the claim left the stub attached to
	// nothing, and later messages to the old route were silently held as "no
	// claim". The steal pre-step stays before the claim: the user has already
	// confirmed displacement, so we evict the current holder of `key` first.
	if steal {
		if evicted := b.Routes.ForceReleaseKey(key); evicted != nil && evicted != stub {
			// The evicted holder still thinks it owns `key`: its Stub.Route +
			// routeConfirmed are set, so its next destructive fetch_queue(ack=true)
			// would drain a topic it no longer owns. Clear its route (item E) so that
			// drain hits the "no route claimed" refusal instead. Use ClearRouteIf(key),
			// NOT an unconditional SetRoute(nil): a victim that is MID-SWITCH to a
			// DIFFERENT topic (its Route already re-pointed away from `key`) must not be
			// zeroed, or it would skip releasing its OLD route and leak it as an
			// orphaned claim. ClearRouteIf clears route+routeConfirmed only when the
			// stub still points at the stolen key; it's stubMu-guarded, so this
			// cross-connection call is race-safe.
			evicted.ClearRouteIf(key)
		}
	}
	holder, ok := b.Routes.Claim(key, stub)
	if !ok {
		_ = conn.WriteJSON(ipc.AttachedMsg{
			Op: ipc.OpAttached, OK: false,
			NeedsConfirmation: true,
			Proposal: &ipc.Proposal{
				Action:  "force_steal",
				Channel: key.Channel,
				Name:    label,
				Holder: &ipc.Holder{
					CLI: holder.CLI, PID: holder.PID, CWD: holder.CWD,
				},
			},
			Err: fmt.Sprintf("attach %s: held by %s pid %d (cwd %s) — re-invoke with steal=true to force",
				label, holder.CLI, holder.PID, holder.CWD),
		})
		return false
	}
	// Claim succeeded — now drop the stub's previous route. Single-claim-per-stub
	// is enforced ONLY by this explicit Release (Routes.Claim is per-key and does
	// not enforce it). The only window where the stub holds both keys is these
	// few sequential statements in one goroutine — never an observable steady
	// state. The `*old != key` guard keeps an idempotent self-reclaim (Claim
	// returns idempotent/transfer-true when this stub already holds key) from
	// releasing the very route it just kept.
	if old := stub.CurrentRoute(); old != nil && *old != key {
		b.Routes.Release(*old, stub.ConnID)
	}
	stub.SetRoute(&key)
	// This is a legitimate, human-driven explicit/steal claim — confirm the route so
	// the destructive consume paths (spec §5 tripwire) will service it.
	stub.MarkRouteConfirmed()
	if isFresh {
		go b.sendWelcome(stub, key, label)
	}
	return true
}

// sendWelcome posts a one-shot friendly confirmation to the channel after a
// successful, fresh attach. Async (off the IPC thread) — a slow Telegram
// network call must not block the AttachedMsg reply to the adapter. Errors
// are logged but never surface to the user: a missing welcome is annoying,
// a failed attach is worse.
//
// Suppressed for re-claims by the same logical session (see tryClaim's
// isFresh check) so adapter reconnects don't spam the topic.
//
// Pre-release UX bug #1 (TODO.md, 2026-05-13): without this, `attach`
// returned silence on success — the user had to send a probe message to
// confirm the route worked.
func (b *Broker) sendWelcome(stub *Stub, key RouteKey, label string) {
	if b == nil {
		return
	}
	// Suppression is handled upstream in tryClaim's isFresh check: replay
	// attaches (AttachReq.Replay=true) and same-logical-session re-claims
	// never reach this function. We previously also held a 30-second
	// post-startup recovery window here as belt-and-suspenders for the
	// case where an older adapter binary didn't yet thread the Replay
	// flag — but in practice it false-positived against legitimate
	// user-typed attaches that happened to land within 30s of broker
	// startup (maintainer 2026-05-14: typed `attach` 21s after a broker
	// restart and got no welcome). Replay is the authoritative signal;
	// trust it.
	ch, err := b.Channel(key.Channel)
	if err != nil {
		log.Printf("welcome: channel %s lookup failed: %v", key.Channel, err)
		return
	}
	var topicID *int64
	if key.HasTopic {
		t := key.TopicID
		topicID = &t
	}
	// Resolve the displayed directory the same way persistMapping resolves
	// the SAVED mapping (FIX 2, 2026-06-03). resolveAttachCWD refines the
	// raw launch dir (stub.CWD) down to <launchCWD>/<topicName> when that
	// subdir exists — so a session launched in a parent dir and attached to
	// a topic named after a project subdir shows the project, not the
	// parent. label IS the topic name on the by-name / saved-mapping attach
	// paths (the cases where refinement can fire); on the DM / topic-by-id
	// paths label is a display string that won't match any subdir, so
	// resolveAttachCWD returns stub.CWD unchanged — no behavior change.
	resolved := resolveAttachCWD(stub.CWD, label)
	text := welcomeText(stub, label, resolved)
	if _, err := ch.SendReply(c3types.ReplyArgs{
		Channel: key.Channel,
		ChatID:  key.ChatID,
		TopicID: topicID,
		Text:    text,
	}); err != nil {
		log.Printf("welcome: send failed for %s: %v", routeKeyStr(key), err)
		return
	}
	log.Printf("welcome: sent for %s cli=%s cwd=%q", routeKeyStr(key), stub.CLI, stub.CWD)
}

// welcomeText renders the on-attach confirmation. Friendly tone (PID
// intentionally omitted per pre-release UX feedback 2026-05-14: the PID
// is mechanical clutter for a human reader; cwd + cli are what matter).
//
// resolvedCWD (FIX 2, 2026-06-03) is the project dir resolved by
// resolveAttachCWD — i.e. stub.CWD refined down to the topic's project
// subdir when the user launched in a parent. When non-empty it is the
// rendered directory line, so the welcome matches the SAVED mapping
// instead of showing the bare parent launch dir. Falls back to stub.CWD
// when resolvedCWD is "" (the DM / no-refine case, where caller passes
// "" or where resolveAttachCWD declined to refine).
func welcomeText(stub *Stub, label, resolvedCWD string) string {
	cwd := resolvedCWD
	if cwd == "" {
		cwd = stub.CWD
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" && strings.HasPrefix(cwd, home) {
		cwd = "~" + cwd[len(home):]
	}
	cli := stub.CLI
	if cli == "" {
		cli = "cli"
	}
	if cwd == "" {
		return fmt.Sprintf("👋 Hi! Attached as **%s** to **%s**. Send anything — voice, text, replies. I'm listening here.", cli, label)
	}
	return fmt.Sprintf("👋 Hi! Attached and listening here.\n📁 `%s`\n🤖 `%s` → **%s**", cwd, cli, label)
}

// sendRecoverWelcome posts a one-shot Telegram confirmation to the topic when a
// resumed session auto-re-attaches (handleRecoverSession's recovered branch). It
// is the GUARANTEED-visible signal that auto-attach-on-resume happened: the
// adapter's CLI notice can be dropped by Claude Code when it fires in the resume
// idle gap (2026-06-24), but a Telegram message in the topic always lands.
// Async (off the IPC thread) so a slow network call never delays the recover
// response; errors are logged, never surfaced. Distinct wording from sendWelcome
// so the user can tell a resume re-attach from a fresh attach.
func (b *Broker) sendRecoverWelcome(stub *Stub, key RouteKey, name string, queued int) {
	if b == nil {
		return
	}
	ch, err := b.Channel(key.Channel)
	if err != nil {
		log.Printf("recover-welcome: channel %s lookup failed: %v", key.Channel, err)
		return
	}
	var topicID *int64
	if key.HasTopic {
		t := key.TopicID
		topicID = &t
	}
	text := recoverWelcomeText(name, queued)
	if _, err := ch.SendReply(c3types.ReplyArgs{
		Channel: key.Channel,
		ChatID:  key.ChatID,
		TopicID: topicID,
		Text:    text,
	}); err != nil {
		log.Printf("recover-welcome: send failed for %s: %v", routeKeyStr(key), err)
		return
	}
	log.Printf("recover-welcome: sent for %s cli=%s queued=%d", routeKeyStr(key), stub.CLI, queued)
}

// recoverWelcomeText renders the resume re-attach confirmation. Names the held
// backlog when present so the user knows messages are waiting.
func recoverWelcomeText(name string, queued int) string {
	if queued > 0 {
		noun := "message"
		if queued > 1 {
			noun = "messages"
		}
		return fmt.Sprintf("🔄 Resumed — re-attached to **%s**. %d held %s waiting.", name, queued, noun)
	}
	return fmt.Sprintf("🔄 Resumed — re-attached to **%s**. Listening here again.", name)
}

// persistMapping upserts the cwd → mapping into the in-memory MappingsFile.
// SaveMappings is called at the end of any attach that mutates state to flush
// to disk atomically.
//
// Cwd resolution (TODO.md pre-release UX bug #2, 2026-05-14): if the user
// launched Claude in a parent directory and attached to a topic whose name
// matches a subdirectory, persist that subdirectory as the mapped cwd —
// not the launch root. Without this, every topic attached from the same
// parent directory ends up clobbering the same `parent → topic` entry,
// turning every fresh attach into a silent rebind of the parent's default.
//
// Rebind guard (TODO.md pre-release UX bug #3, hardened 2026-05-14 per
// the maintainer's "should be rejected" directive): if the resolved cwd already
// maps to a *different* topic, the broker refuses to overwrite the
// saved default. The live claim still proceeds — the user has the
// session they wanted — but the default-for-next-launch stays put.
// To actually change the default, the user edits
// `~/.config/c3/mappings.json` directly. Loud log line so the rejection
// is visible.
// SessionAttachmentTTL bounds how long a recorded session→route mapping stays
// eligible for auto-attach-on-resume. Exported so the broker entrypoint can
// prune expired entries on start.
const SessionAttachmentTTL = 30 * 24 * time.Hour

// sessionRefreshInterval is how stale a recovered attachment's LastAttachedAt
// must be before a resume rewrites it. Bounds mappings.json write churn from
// reconnect bursts (broker bounces / network blips, which all re-run hello)
// while keeping the 30-day inactivity TTL reliable.
const sessionRefreshInterval = time.Hour

// routeKeyFromSessionAttachment builds the route key for a recovered session.
func routeKeyFromSessionAttachment(sa mappings.SessionAttachment) RouteKey {
	return MakeRouteKey(sa.Channel, sa.ChatID, sa.TopicID)
}

// recoverSession attempts to re-claim the route the stub's STABLE session was
// last attached to. Returns the claimed key, the held-backlog count, and ok.
// No-op (ok=false) when: no stable id, no/expired/tombstoned attachment, the
// route is held by another live session, or the claim fails.
//
// Caller MUST hold no lock AND must have already confirmed stub.CurrentRoute()
// is nil (handleRecoverSession does — the already-attached case takes the
// record-only branch instead). Uses low-level Routes.Claim (NOT tryClaim, which
// would write an AttachedMsg the conn isn't expecting and could send a welcome);
// C3's backlog is pull-not-push, so the claim never floods the conn. Refreshes
// LastAttachedAt only when staler than sessionRefreshInterval, so a burst of
// broker-bounce / reconnect-driven recover ops doesn't rewrite mappings.json
// (with its .bak + fsyncs) every time.
func (b *Broker) recoverSession(stub *Stub) (RouteKey, int, []ipc.QueuedItem, bool) {
	sid := stub.StableSessionIDValue()
	if sid == "" {
		return RouteKey{}, 0, nil, false
	}
	sa, ok := b.Mappings().LookupSessionAttachment(sid)
	if !ok || !sa.Recoverable(time.Now(), SessionAttachmentTTL) {
		return RouteKey{}, 0, nil, false
	}
	key := routeKeyFromSessionAttachment(sa)
	if _, held := b.heldByDifferentLiveSession(key, stub); held {
		log.Printf("recover: SKIPPED session=%s topic=%q (held by another live session)", sid, sa.Name)
		return RouteKey{}, 0, nil, false
	}
	if _, claimed := b.Routes.Claim(key, stub); !claimed {
		log.Printf("recover: claim FAILED session=%s topic=%q", sid, sa.Name)
		return RouteKey{}, 0, nil, false
	}
	stub.SetRoute(&key)
	// The session re-claiming its OWN recorded route is a legitimate claim — confirm
	// it so the destructive consume paths (spec §5 tripwire) will service it.
	stub.MarkRouteConfirmed()
	// SINGLE live peek: return the count AND the preview from ONE backlogSummary
	// job so they always describe the same queue snapshot. Callers (the automatic
	// handleRecoverSession and the manual attachBare path (ii)) consume both from
	// this result — no second peek, so count and preview can never disagree (the
	// stale-peek TOCTOU that §3c closes).
	cnt, preview := b.backlogSummary(key)
	if time.Since(sa.LastAttachedAt) > sessionRefreshInterval {
		b.mutateMappings(func(mf *mappings.MappingsFile) {
			if cur, ok := mf.LookupSessionAttachment(sid); ok {
				cur.LastAttachedAt = time.Now().UTC()
				mf.UpsertSessionAttachment(sid, cur)
			}
		})
		_ = b.SaveMappings()
	}
	log.Printf("recover: session=%s cli=%s pid=%d → %q (queued=%d)", sid, stub.CLI, stub.PID, sa.Name, cnt)
	return key, cnt, preview, true
}

// recordCurrentRouteForStable saves the stub's CURRENT route under its stable
// session id (the dual-path "attach BEFORE recover" arm). Derives channel /
// chatID / topicID from the RouteKey, resolves Name/Group from the topic
// registry (DM = no topic → name "dm"), and records via the session-attachment
// recorder keyed on stub.StableSessionIDValue(). No-op when the stable id is
// empty (recordSessionAttachment guards that).
func (b *Broker) recordCurrentRouteForStable(stub *Stub, key RouteKey) {
	var topicID *int64
	name, group := "dm", ""
	if key.HasTopic {
		t := key.TopicID
		topicID = &t
		if tp, ok := b.Mappings().LookupTopicByID(key.Channel, key.ChatID, key.TopicID); ok {
			name = tp.Name
			group = tp.Group
		} else {
			name = fmt.Sprintf("topic-%d", key.TopicID)
		}
	}
	b.recordSessionAttachment(stub, key.Channel, key.ChatID, topicID, name, group)
}

func (b *Broker) persistMapping(stub *Stub, chanName string, chatID, topicID int64, name, group string) {
	now := time.Now().UTC()
	cwd := resolveAttachCWD(stub.CWD, name)
	var tidPtr *int64
	if topicID != 0 {
		t := topicID
		tidPtr = &t
	}
	// Read existing-mapping check and the Upsert(s) under the same mutation
	// lock — otherwise a concurrent persistMapping for the same cwd could
	// race past the refusal check.
	var persisted bool
	stableID := stub.StableSessionIDValue()
	b.mutateMappings(func(mf *mappings.MappingsFile) {
		// Session-id recovery store — keyed on the STABLE session id, recorded
		// INDEPENDENTLY of the cwd rebind guard below (so a refused rebind, or
		// an empty cwd, still records the recovery entry). Clears any prior
		// tombstone. This is the dual-path "attach AFTER recover" arm: when a
		// RecoverSessionReq already set the stable id, an attach that lands later
		// records under it. Empty (non-hook session / recover hasn't arrived) →
		// no recording (fail-closed). The DM route records via
		// recordSessionAttachment instead (it must not also write a cwd default).
		if stableID != "" {
			mf.UpsertSessionAttachment(stableID, mappings.SessionAttachment{
				Channel: chanName, ChatID: chatID, TopicID: tidPtr,
				Name: name, Group: group, CWD: cwd, LastAttachedAt: now,
			})
			persisted = true
		}
		// cwd → topic default (existing behavior, incl. the explicit rebind
		// guard: never silently overwrite a saved cwd→topic with a different
		// topic; the live claim still proceeds upstream in tryClaim).
		if cwd != "" {
			if existing, ok := mf.LookupByCwd(cwd); ok && (existing.ChatID != chatID || existing.TopicID != topicID) {
				log.Printf("attach: REFUSED to rebind cwd=%q (saved=topic-%d %q → requested=topic-%d %q); live claim proceeds but saved default unchanged. To rebind, edit ~/.config/c3/mappings.json.",
					cwd, existing.TopicID, existing.Name, topicID, name)
			} else {
				mf.UpsertMapping(cwd, mappings.Mapping{
					Channel:        chanName,
					ChatID:         chatID,
					TopicID:        topicID,
					Name:           name,
					Group:          group,
					LastAttachedAt: now,
				})
				persisted = true
			}
		}
	})
	if persisted {
		_ = b.SaveMappings()
	}
}

// recordSessionAttachment writes ONLY the session→route recovery entry (no cwd
// mapping), for attach paths that must not persist a per-cwd default — namely
// the DM route, which is universal and deliberately never cwd-mapped. No-op when
// the host exposes no session id. Topic attaches record via persistMapping
// instead (which records the session attachment AND the cwd default together).
func (b *Broker) recordSessionAttachment(stub *Stub, chanName string, chatID int64, topicID *int64, name, group string) {
	sid := stub.StableSessionIDValue()
	if sid == "" {
		return
	}
	b.mutateMappings(func(mf *mappings.MappingsFile) {
		mf.UpsertSessionAttachment(sid, mappings.SessionAttachment{
			Channel: chanName, ChatID: chatID, TopicID: topicID,
			Name: name, Group: group, CWD: stub.CWD, LastAttachedAt: time.Now().UTC(),
		})
	})
	_ = b.SaveMappings()
}

// resolveAttachCWD picks the cwd to persist for a `cwd → topic` mapping.
//
// Order:
//  1. If launchCWD == "" → "" (nothing to persist, caller must skip).
//  2. If topicName == "" → launchCWD (no signal to refine; persist as-is).
//  3. If `filepath.Base(launchCWD) == topicName` → launchCWD (the launch
//     directory IS the project directory; basename matches).
//  4. If `<launchCWD>/<topicName>` exists as a directory → that path
//     (the user launched in a parent of the project; refine downward).
//  5. Otherwise → launchCWD (best-effort fallback; conflict-detection
//     in persistMapping will catch silent rebinds).
//
// Exported as a package-private helper so tests can pin the rules.
func resolveAttachCWD(launchCWD, topicName string) string {
	if launchCWD == "" {
		return ""
	}
	if topicName == "" {
		return launchCWD
	}
	if filepath.Base(launchCWD) == topicName {
		return launchCWD
	}
	candidate := filepath.Join(launchCWD, topicName)
	if info, err := os.Stat(candidate); err == nil && info.IsDir() {
		return candidate
	}
	return launchCWD
}

// SaveMappings writes the in-memory MappingsFile to its on-disk path. Called
// after any state mutation. Best-effort — failures are logged but don't fail
// the attach (the in-memory state is what the broker uses to route).
func (b *Broker) SaveMappings() error {
	path, err := mappings.DefaultPath()
	if err != nil {
		return err
	}
	return mappings.Write(path, b.Mappings())
}

// resolveGroup returns the group name + config for the attach's group choice.
// If groupName is empty, returns the channel's default. Returns false if the
// group isn't configured.
func (b *Broker) resolveGroup(cc mappings.ChannelConfig, groupName string) (string, mappings.GroupConfig, bool) {
	if groupName == "" {
		groupName = cc.DefaultGroup
	}
	if groupName == "" {
		return "", mappings.GroupConfig{}, false
	}
	gCfg, ok := cc.Groups[groupName]
	return groupName, gCfg, ok
}

// defaultChannel returns the first channel name in mappings, or "" if none.
// In v1 there's typically only one channel (telegram), so this is unambiguous.
func (b *Broker) defaultChannel() string {
	for name := range b.Mappings().Channels {
		return name
	}
	return ""
}

// backlogSummaryMax bounds the compact attach-time preview (full content comes
// via fetch_queue). Three rows keep the on-attach notification short.
const backlogSummaryMax = 3

// backlogSummary returns the total queued count and a compact preview (oldest up
// to backlogSummaryMax) for the just-claimed route. Peek only — never consumes;
// the agent drains via fetch_queue. Empty/zero when nothing is queued or the
// queue is disabled.
//
// I7: the total + preview are read ATOMICALLY through a SINGLE route-worker job
// (JobBacklog), mirroring JobFetch/JobConsume — never via a separate Pending-then-
// Peek off the worker goroutine, which could race the worker's concurrent Append/
// Consume/rewrite (TOCTOU: count>0 with an empty/stale preview). Called on the
// attach handler goroutine, which safely blocks on the worker's result channel
// (same pattern as handleFetchQueue). If the worker queue is full/stopped we log
// and return empty (the agent still learns of backlog via the next push's
// recovery nudge / fetch_queue).
func (b *Broker) backlogSummary(key RouteKey) (int, []ipc.QueuedItem) {
	if b.Queue == nil || b.Workers == nil {
		return 0, nil
	}
	resultCh := make(chan BacklogResult, 1)
	job := Job{Kind: JobBacklog, Backlog: &BacklogJob{PeekN: backlogSummaryMax, ResultCh: resultCh}}
	if !b.Workers.Submit(key, job) {
		log.Printf("backlog summary %s: worker queue full or stopped — skipping summary", routeKeyStr(key))
		return 0, nil
	}
	var res BacklogResult
	select {
	case res = <-resultCh:
	case <-time.After(workerJobTimeout):
		// A3: an EXITED worker already replied errWorkerStopped fast; this fires only
		// for a worker that genuinely STALLED. Fall back to the existing no-summary
		// path so the attach completes instead of wedging on the never-written
		// resultCh (the agent still learns of backlog via the next push's nudge).
		log.Printf("backlog summary %s: worker did not respond within %s — skipping summary", routeKeyStr(key), workerJobTimeout)
		return 0, nil
	}
	if res.Err != nil {
		log.Printf("backlog summary peek FAIL %s: %v", routeKeyStr(key), res.Err)
		// Total still came back fine; render the count without a preview.
		return res.Total, nil
	}
	if res.Total == 0 {
		return 0, nil
	}
	items := make([]ipc.QueuedItem, 0, len(res.Preview))
	for i := range res.Preview {
		in := &res.Preview[i]
		items = append(items, ipc.QueuedItem{
			MessageID: in.MessageID,
			Sender:    senderLabel(in.Sender),
			Kind:      inboundKindLabel(in),
			Unix:      in.Timestamp.Unix(),
			Preview:   previewText(in, 80),
		})
	}
	return res.Total, items
}

// senderLabel renders a compact sender label for the backlog preview.
func senderLabel(s c3types.Sender) string {
	if s.Username != "" {
		return "@" + s.Username
	}
	if s.UserID != 0 {
		return fmt.Sprintf("uid=%d", s.UserID)
	}
	return ""
}

// inboundKindLabel returns "text" or the first attachment kind / event kind.
func inboundKindLabel(in *c3types.Inbound) string {
	if in.IsEvent() {
		return string(in.Kind)
	}
	if len(in.Attachments) > 0 && in.Attachments[0].Kind != "" {
		return in.Attachments[0].Kind
	}
	return "text"
}

// previewText returns a rune-safe truncated snippet of an inbound's text.
func previewText(in *c3types.Inbound, n int) string {
	r := []rune(in.Text)
	if len(r) <= n {
		return in.Text
	}
	return string(r[:n]) + "…"
}

// withBacklog returns msg with the route's queued-count + compact summary
// stamped in (no-op when nothing is queued). Call it on every OK=true attach
// response so a session learns of held messages immediately.
func (b *Broker) withBacklog(key RouteKey, msg ipc.AttachedMsg) ipc.AttachedMsg {
	count, items := b.backlogSummary(key)
	msg.QueuedCount = count
	msg.QueuedSummary = items
	return msg
}
