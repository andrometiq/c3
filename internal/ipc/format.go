package ipc

import (
	"fmt"
	"strings"
)

// FormatAttached renders an AttachedMsg as the user-facing string the
// agent surfaces verbatim. Shared between the Claude and Codex adapters
// — the broker's proposal cases must render identically on either side
// (maintainer 2026-05-18: "every flow must work the same in Codex"), and
// inlining the formatter twice was the documented source of the
// previously-silent codex parity bug where two proposal actions
// (disambiguate_dm, force_steal) had no codex branch and rendered as
// "attach: unspecified failure".
//
// Wording is the older Claude-adapter form (more explicit; the Codex
// form was a trivial paraphrase that lost no information).
//
// First extracted: 2026-05-19 (audit triage, plan
// `docs/plans/2026-05-19-audit-triage.md`).
func FormatAttached(a *AttachedMsg) string {
	if a.OK {
		s := fmt.Sprintf("attached to %q", a.Name)
		if a.Channel == "web" {
			s += " (on-the-go chat)"
		} else if a.TopicID != nil {
			s += fmt.Sprintf(" (chat %d, thread %d)", a.ChatID, *a.TopicID)
		} else {
			s += fmt.Sprintf(" (chat %d, DM)", a.ChatID)
		}
		if a.Notice != "" {
			s += "\n" + a.Notice
		}
		if len(a.Routes) > 1 {
			labels := make([]string, 0, len(a.Routes))
			if a.Output != nil {
				labels = append(labels, routeRefLabel(*a.Output))
			}
			for _, route := range a.Routes {
				if a.Output != nil && routeRefsEqual(route, *a.Output) {
					continue
				}
				labels = append(labels, routeRefLabel(route))
			}
			output := "unknown"
			if a.Output != nil {
				output = routeRefLabel(*a.Output)
			}
			s += "\nholding: " + strings.Join(labels, ", ") + " · output: " + output
		}
		return s
	}
	if a.NeedsConfirmation && a.Proposal != nil {
		switch a.Proposal.Action {
		case "create":
			// The confirm command carries the name explicitly: a bare
			// attach(create=true) no longer works (the cwd-basename backfill
			// was deleted with the silent-claim redesign — spec §2).
			return fmt.Sprintf("No mapping for this directory. I'd create a new topic %q in the %q group. To proceed, call attach(name=%q, create=true). To use an existing topic instead, call attach(topic_id=<n>).",
				a.Proposal.Name, a.Proposal.Group, a.Proposal.Name)
		case "use_existing_other_group":
			alt := ""
			if a.Proposal.Alternative != nil {
				// The alternative "create" carries the name explicitly (the broker
				// sets Alternative.Name to the searched-for name — attach.go's
				// attachByName). A name-less attach(create=true) errors since the
				// cwd-basename backfill was deleted, so the re-invoke MUST carry the
				// name.
				alt = fmt.Sprintf(" or attach(name=%q, create=true, group=%q) to create a new topic in %q",
					a.Proposal.Alternative.Name, a.Proposal.Alternative.Group, a.Proposal.Alternative.Group)
			}
			return fmt.Sprintf("Found topic %q in group %q (thread %d). Reply yes to claim it%s.",
				a.Proposal.Existing.Name, a.Proposal.Existing.Group, a.Proposal.Existing.TopicID, alt)
		case "disambiguate_dm":
			ex := a.Proposal.Existing
			return fmt.Sprintf("Ambiguous: a topic named %q exists in group %q (thread %d). Did you mean attach to that topic, or to your actual Telegram DM? Confirm by calling attach(topic_id=%d) for the topic, or attach(target=\"dm\", steal=true) for the actual DM.",
				ex.Name, ex.Group, ex.TopicID, ex.TopicID)
		case "force_steal":
			h := a.Proposal.Holder
			if h.CLI == "desktop" {
				// A Claude Desktop holder has no user-meaningful pid/cwd: every
				// Desktop surface (Chat / Code / Cowork) is a SEPARATE process with
				// an IDENTICAL cwd, so "held by desktop pid N (cwd …)" reads as
				// gibberish. Name it as what it is — the user's own other Desktop C3
				// chat — and frame steal as MOVING the topic here, not evicting a
				// stranger. (2026-07-17: this was the whole of the multi-surface
				// "already attached" confusion; the routing underneath is correct.)
				return fmt.Sprintf("Topic %q is currently held by your Claude Desktop C3 session in another chat/surface. Re-invoke attach with steal=true to move it here.",
					a.Proposal.Name)
			}
			return fmt.Sprintf("Topic %q is currently held by %s pid %d (cwd %q). Re-invoke attach with steal=true to evict that session and take the claim. Only do this if the user explicitly confirms.",
				a.Proposal.Name, h.CLI, h.PID, h.CWD)
		case "pick_topic":
			return formatPickTopic(a.Proposal)
		}
	}
	// Status-aware structured failures (2026-05-19). Lets the agent tell
	// the user "you need to run setup" vs "your tenant blocked this" vs
	// the generic Err echo. See docs/plans/2026-05-19-codex-policy-3state.md.
	switch a.Status {
	case AttachStatusNoTopicsConfigured:
		return fmt.Sprintf("C3 is not configured for this destination. Run `c3-broker setup` to wire up the Telegram bot token, group chat id, and a starter topic, then retry attach. (broker said: %s)", a.Err)
	case AttachStatusPolicyRejected:
		return fmt.Sprintf("Attach rejected by your CLI host's policy layer. The Telegram destination needs tenant-admin approval before this CLI can attach. Ask the tenant admin to approve the destination, then retry attach. (host said: %s)", a.Err)
	case AttachStatusCwdDefaultCollision:
		// SYMPTOM-3: a bare cwd-default attach resolved to a topic a
		// different live session already holds. Multiple `claude`
		// instances launched from the same parent dir report identical
		// cwds, so the saved mapping is ambiguous. Guide the user toward
		// the likely-correct fix (attach a different topic by name); the
		// "force" path is to re-attach the SAME topic by its explicit
		// name, which triggers the normal force_steal confirmation —
		// there is no `--steal` CLI token (the override is steal=true,
		// handled by the agent on that confirmation).
		holder := "another session"
		if a.Holder != nil {
			holder = fmt.Sprintf("%s pid %d", a.Holder.CLI, a.Holder.PID)
		}
		return fmt.Sprintf(
			"⚠ cwd %s maps to topic %q, already held by %s (a different session).\n"+
				"  Did you mean a different topic? Attach by name:\n"+
				"      /c3:attach <topic>\n"+
				"  Or to take %q from that session, re-attach it by name and confirm the steal:\n"+
				"      /c3:attach %s",
			a.CWD, a.Name, holder, a.Name, a.Name)
	}
	if a.Err != "" {
		return "attach failed: " + a.Err
	}
	return "attach: unspecified failure"
}

// FormatRouteRef returns the short route label adapters use in tool results.
func FormatRouteRef(route RouteRef) string {
	return routeRefLabel(route)
}

// FormatSetOutputRoute renders a successful output-route change.
func FormatSetOutputRoute(resp SetOutputRouteResp) string {
	if resp.Err != "" {
		return resp.Err
	}
	if !resp.OK || resp.Output == nil {
		return "output route: unspecified failure"
	}
	return "output route → " + routeRefLabel(*resp.Output)
}

// FormatRelease renders the complete route set returned by a targeted release.
func FormatRelease(target string, resp ReleaseResp) string {
	if resp.Err != "" {
		return resp.Err
	}
	if !resp.OK {
		return "release: unspecified failure"
	}
	if len(resp.Routes) == 0 {
		return "released " + target + "; no routes held"
	}
	labels := make([]string, 0, len(resp.Routes))
	if resp.Output != nil {
		labels = append(labels, routeRefLabel(*resp.Output))
	}
	for _, route := range resp.Routes {
		if resp.Output != nil && routeRefsEqual(route, *resp.Output) {
			continue
		}
		labels = append(labels, routeRefLabel(route))
	}
	output := "unknown"
	if resp.Output != nil {
		output = routeRefLabel(*resp.Output)
	}
	return "released " + target + "; still holding " + strings.Join(labels, ", ") + "; replies go to " + output
}

func routeRefLabel(route RouteRef) string {
	if route.Channel == "web" {
		return "web"
	}
	if route.Name != "" {
		return route.Name
	}
	if route.TopicID != nil {
		return fmt.Sprintf("topic-%d", *route.TopicID)
	}
	if route.Channel != "" {
		return route.Channel
	}
	return "unknown"
}

func routeRefsEqual(a, b RouteRef) bool {
	if a.Channel != b.Channel || a.ChatID != b.ChatID || (a.TopicID == nil) != (b.TopicID == nil) {
		return false
	}
	return a.TopicID == nil || *a.TopicID == *b.TopicID
}

// formatPickTopic renders a "pick_topic" proposal (spec §4) — the bare-attach
// friendly picker. The string is shared VERBATIM by both adapters, and Codex has
// neither an AskUserQuestion tool nor the /c3:attach slash command nor an `expr`
// attach arg — so the wording is HOST-NEUTRAL: every re-invoke is a plain
// attach(...) call that both hosts' attach tool schema accepts, and the
// AskUserQuestion / /c3:attach mentions are parenthetical hints, never
// instructions to call a tool Codex lacks. Each suggestion line carries its EXACT
// re-invoke command so the agent runs it directly on the human's pick; the picker
// never auto-attaches. Renders actionably even with zero suggestions (the
// create-by-name + attach-by-name body always prints).
func formatPickTopic(p *Proposal) string {
	var b strings.Builder
	b.WriteString("This session isn't attached to a topic yet — ASK the user which one before doing anything ")
	b.WriteString("(use AskUserQuestion if your host has it; otherwise ask in plain conversation). Do NOT assume, never auto-pick:")

	n := 0
	for i := range p.Suggestions {
		s := p.Suggestions[i]
		n++
		var desc, cmd string
		switch s.Kind {
		case "create":
			desc = fmt.Sprintf("Create new topic %q", s.Name)
			cmd = fmt.Sprintf("attach(name=%q, create=true)", s.Name)
		default: // attach_existing
			reason := s.Reason
			if reason == "" {
				reason = "existing topic"
			}
			desc = fmt.Sprintf("Attach %q (%s)", s.Name, reason)
			switch {
			case s.TopicID == nil:
				cmd = `attach(target="dm")`
			case s.Group != "":
				cmd = fmt.Sprintf("attach(topic_id=%d, group=%q)", *s.TopicID, s.Group)
			default:
				cmd = fmt.Sprintf("attach(topic_id=%d)", *s.TopicID)
			}
			if s.ClaimedBy != nil {
				desc += fmt.Sprintf(" — held by %s pid %d; picking it will ask before stealing", s.ClaimedBy.CLI, s.ClaimedBy.PID)
			}
		}
		fmt.Fprintf(&b, "\n  %d. %s → %s", n, desc, cmd)
	}
	if p.WebAvailable {
		n++
		fmt.Fprintf(&b, "\n  %d. web — on-the-go chat (`attach web`)", n)
	}
	if p.HasMore {
		n++
		fmt.Fprintf(&b, "\n  %d. See the full list → call the `topics` tool, then attach by name/id", n)
	}
	b.WriteString("\nTo attach a topic by name instead: attach(name=\"<name>\") (Claude Code users can type /c3:attach <name>).")
	b.WriteString("\nTo create a new topic: attach(name=\"<name>\", create=true) (or /c3:attach create <name>).")
	return b.String()
}

// FormatTopics renders a TopicsListMsg as the user-facing string for
// the adapter `topics` MCP tool. Shared between Claude and Codex
// adapters; both inlined byte-identical copies pre-2026-05-19.
//
// Note: the broker CLI `c3-broker topics` (cmd/c3-broker/topics.go) has
// its own slightly-different rendering — no terminating period on the
// empty-list message, slightly different row layout. That divergence
// is intentional (CLI-formatted output vs adapter-formatted output);
// see audit doc 2026-05-18 entry #6.
func FormatTopics(list *TopicsListMsg) string {
	if len(list.Topics) == 0 {
		return "no topics configured."
	}
	lines := make([]string, 0, len(list.Topics)+1)
	lines = append(lines, "known topics:")
	for _, t := range list.Topics {
		state := "free"
		if t.ClaimedBy != nil {
			state = fmt.Sprintf("held by %s pid %d", t.ClaimedBy.CLI, t.ClaimedBy.PID)
		}
		if t.RouteKind == "dm" {
			lines = append(lines, fmt.Sprintf("  • %s (chat %d, no topic) — %s", t.Name, t.ChatID, state))
		} else {
			lines = append(lines, fmt.Sprintf("  • %s/%s (chat %d, thread %d) — %s",
				t.Group, t.Name, t.ChatID, t.TopicID, state))
		}
	}
	return strings.Join(lines, "\n")
}
