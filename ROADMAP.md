# C3 — Roadmap

What's next for C3 after v0.1. Everything here is future or unbuilt; shipped work is in git history. One line per item — details land in the design when each is built.

## Interactive & trust

- Interactive Q&A: free-text / "Other" / comment answers (single/multi-select + Skip already ship).
- Native free-text option in the attach picker — today "type your own" is body prose (`/c3:attach <name>`); a real free-text choice needs the deferred free-text answer surface above.
- Codex parity for tap-to-approve, `ask`, and `detach`.
- Per-user access control — who is allowed to drive which CLI.
- Trusted-operator authorization for actions the CLI would otherwise hard-deny.
- Permission-relay niceties: "see more" expansion, a text `y/n` fallback.

## Reach

- Remote spawn + control of Claude Code / Codex sessions from chat.
- Inter-agent messaging — one agent can message another's channel (rate-limited).
- Stream the agent's reasoning to the channel.
- Be the phone surface for session managers (Claude Squad, CCManager, Conductor, …).
- A generic ACP-client adapter — one code path that spawns and drives any
  Agent-Client-Protocol agent (`session/prompt` in, `session/update` out,
  `session/request_permission` relayed to the channel).
- Consider migrating the Grok adapter's inject path from the leader socket (an
  undocumented internal surface) to the documented ACP `grok agent stdio` protocol.

## More channels

- Web-chat and voice-mode channels via the `Channel` interface. See
  [On-the-go / voice-channel design capture](docs/future/on-the-go-voice-channel.md)
  for the full on-the-go voice vision — web + native-app surfaces, magic-link /
  Telegram-login auth, agent-aware TTS (C3-owned, the STT plugin in reverse) vs.
  a live bidirectional "narrator" mode, and VoIP / phone-call reach.
- Other transports the interface already admits (Slack, Matrix, …).

## Telegram completeness

- Media-group / album assembly, media echo by `file_id`, message forwarding.
- Richer formatting (underline, inline mentions), location sends, more poll options.

## Packaging & platform

- Drop the development-channels flag once C3 ships through a trusted plugin store.
- External (non-Go) loadable plugins.
- Broker-side `/list` and `/route` commands.
- Monitoring dashboard, persistent message history, STT latency instrumentation.
- Revive the bundled Gemini STT provider with a working key, or redocument the chain on the Sarvam default — the docs still describe a Gemini-first chain.
- Async-dispatch more non-critical broker sends (as the voice-readback echo already does), preserving strict per-topic ordering.
- Delivery-contingent consume for `fetch_queue` — the live-push half now consumes by message id (the ack removes exactly the lines its push covered), but the fetch path still commits its consume before the response is known to have been received, so an adapter that abandons a fetch can have the batch consumed anyway. Close that window the same way: peek, respond, then an explicit id-targeted ack. Needed by the pooled-queue work regardless.
- Attach-replay refinements: gate `disambiguate_dm` on `Replay`, and honor `group` in the step-2 name lookup, so a replayed DM or non-default-group attach restores cleanly instead of falling to a discarded proposal. This also covers a case the steal-sanitize introduced: a DM attach that steered past disambiguation with `steal=true` is remembered as `{Target:"dm", Steal:false}` (the one-shot steal is stripped), so a fresh-broker replay lands back on the `disambiguate_dm` proposal — discarded → detached (the auto-recover net usually restores it) — which the `Replay` gate would fix.
- Surface the silent auto-recover skip: when a resumed session's own last topic is held by another live session, carry a `Skipped`/reason field and show a one-line CLI notice, instead of resuming quietly with no explanation.
- Idempotent-attach hint: an already-attached bare `attach` could return an "already attached to X — to switch topics attach by name, or detach first" hint instead of a bare confirmation. Needs an additive `AttachedMsg` field so the formatter can tell an idempotent re-confirm apart from a fresh claim.
- Live-verify auto-attach-on-resume end to end. It ships on by default (`auto_attach_on_resume` absent in mappings.json ⇒ enabled; set it to `false` to disable), but the resumed-session path has never been walked through in a real Telegram session.

## Open design questions

- Whether a typed free-text answer is also queued as a normal message, or consumed only as the answer.
- Grant UX for operator authorization (per-action prompt vs standing grant).
