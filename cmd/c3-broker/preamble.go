package main

import (
	"bufio"
	"os"
	"strings"
)

// draftApproved gates whether the rendered preamble shows the
// `[DRAFT 2026-05-19 — copy pending maintainer review]` footer. Default
// false; flip to true ONLY after the maintainer reviews preambleBody and
// explicitly approves the copy.
//
// When you flip this to true, do the following in the SAME commit:
//  1. Read through preambleBody below and confirm it reflects the
//     approved copy (reviews often tighten lines during review).
//  2. Verify TestRenderPreamble_ApprovedMode_OmitsMarker passes —
//     i.e. no stray "DRAFT" or "pending maintainer review" string
//     survives in preambleBody.
//  3. Update RESUME.md / TODO.md if either references the DRAFT
//     state.
//
// Why this is a const rather than a CI grep: c3 is a maintainer-led
// project, not a multi-contributor codebase. The forcing function is
// the human reading this comment when they make the change, plus the
// paired DraftMode/ApprovedMode tests that pin the rendered output to
// the const's value. No "you can't change this constant" test — that
// pattern is hostile when the maintainer is the one approving.
const draftApproved = true // maintainer approved 2026-05-19

// preambleBody is the educational text shown at the very top of
// `c3-broker setup` before any prompt. It's the first thing a fresh
// user sees — they may have heard nothing about C3 beyond "useful for
// sending Telegram messages to Claude Code, especially voice".
//
// The DRAFT footer is NOT embedded here — it is appended by
// renderPreamble when draftApproved is false. This keeps the body
// stable across draft/approved transitions and lets the test suite
// assert on both modes from one source.
//
// Editing guidance:
//   - the maintainer rejects multi-line paragraphs. Keep blocks ≤3 lines.
//   - Cover four things only: what C3 does, what's about to happen,
//     what the user needs handy, and the consent question.
//   - The TestPreambleCopy_ContainsKeyConcepts test enforces presence
//     of the load-bearing terms (bot, group, topic, voice, Telegram,
//     STT). If you remove one of those concepts, update the test.
const preambleBody = `C3 — what it is

  C3 lets you talk to your Claude Code (or Codex) CLI sessions from
  Telegram. Text and voice both work — voice messages are transcribed
  by a custom STT pipeline (Gemini 3 Flash, Sarvam Saaras as fallback)
  and surfaced to the CLI as text. Agent replies come back into Telegram.

What we're about to set up

  1. A Telegram bot — your phone-side endpoint. Made via @BotFather.
     (If you don't have one yet, I'll walk you through it.)
  2. A Telegram group with Topics enabled — one topic per CLI session,
     so multiple projects don't collide in one chat.
  3. ~/.config/c3/mappings.json — a 600-mode config file with your bot
     token and chat ids. Stays on this machine.
  4. C3 core binaries — built from source (the PATH-shadowing Codex launcher remains opt-in).
     I'll kick this off in the background while you wire up Telegram.

What you'll need handy

  - A Telegram bot token (the 1234567:abc... string from @BotFather).
    If you don't have one yet, that's fine — the token step shows the
    exact @BotFather commands.
  - Your phone (or any Telegram client). You'll send two short pairing
    codes — one to the bot, one in the group — and C3 discovers your
    user id and the group's chat id automatically. No id hunting.

  All of it fits in the next 5 minutes if you're new to this.
`

// draftFooter is the footer line appended by renderPreamble when
// draftApproved is false. Kept as a separate const so the test suite
// can assert on its presence/absence without scanning the larger body.
const draftFooter = "\n[DRAFT 2026-05-19 — copy pending maintainer review]\n"

// renderPreamble returns the preamble text. When approved is false,
// the DRAFT footer is appended; otherwise the body is returned as-is.
// Pulled out as a function (rather than a const-with-conditional)
// so tests can exercise both modes from one source.
func renderPreamble(approved bool) string {
	if approved {
		return preambleBody
	}
	return preambleBody + draftFooter
}

// consentPrompt is the gate between the preamble and any action. The
// default on empty input is yes — most fresh installers will just
// hit enter after reading the preamble.
const consentPrompt = "Install C3 for you now? [Y/n]: "

// consentDeclinedMsg is what we print if the user says n. Points at
// the manual checklist in docs/INSTALL.md so they're not stranded.
const consentDeclinedMsg = `No problem. Manual install instructions live at:
  docs/INSTALL.md  (in the c3 source repo)

Re-run ` + "`c3-broker setup`" + ` any time to come back here.`

// printPreamble writes the educational copy to stdout. Pulled out as
// a separate function so tests can assert it ran without needing to
// capture stdout from runSetup's full body. Renders DRAFT footer iff
// draftApproved is false; see the comment above the const.
func printPreamble() {
	os.Stdout.WriteString(renderPreamble(draftApproved))
	os.Stdout.WriteString("\n")
}

// confirmInstall asks the user whether to proceed with the install
// and returns the answer.
//
// Behaviour:
//   - C3_NO_PROMPT truthy ("1", "true", "yes", case-insensitive):
//     return true without reading stdin. Lets Codex's non-interactive
//     setup path skip the gate. Prints a one-line acknowledgement so
//     it's visible in the agent's output stream.
//   - Otherwise reads a line from r; "" / "y" / "yes" → true, "n" /
//     "no" → false.
//
// The default-yes-on-empty bias is deliberate: someone who just read
// the preamble and hit enter is consenting.
func confirmInstall(r *bufio.Reader) bool {
	if isNoPromptSet() {
		os.Stdout.WriteString(consentPrompt + "y  (C3_NO_PROMPT set, proceeding)\n")
		return true
	}
	os.Stdout.WriteString(consentPrompt)
	line, _ := r.ReadString('\n')
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "n", "no":
		return false
	default:
		// "", "y", "yes", and anything else → proceed. Anything-else
		// being a yes is intentional: a fresh user typing "ok" or
		// "sure" shouldn't get bounced.
		return true
	}
}

// isNoPromptSet checks the C3_NO_PROMPT env var. Truthy values: "1",
// "true", "yes" (case-insensitive). Anything else, including unset and
// empty string, is false.
func isNoPromptSet() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("C3_NO_PROMPT")))
	switch v {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}
