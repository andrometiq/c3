# C3 — Roadmap

What's next for C3 after v0.1. Everything here is future or unbuilt; shipped work is in git history. One line per item — details land in the design when each is built.

## First after v0.1 — agent-to-agent messaging

**One agent can post into another agent's topic, so it can wake it.** Today the human is the
transport for every agent-to-agent nudge, and that is a real cost, not a theoretical one: a Codex
review sat unclaimed for hours because the Codex harness cannot schedule a turn on a file event —
only a human message wakes it. The collaboration protocol had to add a whole "nudge-by-default"
clause to work around it. C3 already owns the one thing that would fix it: it can put a message in
front of an idle agent.

The motivating case is small and concrete — *"tell the Codex topic to arm its watcher"* — and it is
the right first case precisely because it is small.

**The safeguards are the design, not a garnish.** An agent that can post outside its own route is a
new trust boundary, and v0.1 spent real effort closing exactly that shape: a tool call is now
structurally addressed to the route the session claimed, and an args-supplied destination is refused
outright. This feature deliberately reopens a narrow version of that, so it has to be built with
the door frame, not cut through the wall:

- **Addressed by topic, never by chat id.** The sender names a topic; it never supplies a raw
  destination. Reuse the refusal that already exists rather than adding a bypass beside it.
- **Rate-limited per (sender, target) pair, with a cooldown**, so a stuck agent cannot loop a
  target into uselessness. A nudge is a doorbell, not a channel.
- **Attributed and visibly non-human.** The receiving human must be able to tell at a glance that
  an agent wrote it, and which one. An agent message that reads like the operator's is the failure
  mode that ends this feature.
- **Opt-in per target**, and revocable. A topic's holder decides whether it accepts nudges.
- **Content-bounded.** A wake signal, not a side channel for work: short, no attachments, and never
  a route by which one agent instructs another to act. The board and the exchange folder stay the
  place where actual work is handed over — this only says *"go look at the board."*
- **Audited** like every other outbound: sender, target, and outcome in the log.

Open question worth settling in the design rather than in code: whether the nudge is a first-class
broker op with the human merely spectating, or a normal message the human could equally have sent.
The first is cleaner; the second is far easier to reason about when it misbehaves.

## Interactive & trust

- Interactive Q&A: free-text / "Other" / comment answers (single/multi-select + Skip already ship).
- Native free-text option in the attach picker — today "type your own" is body prose (`/c3:attach <name>`); a real free-text choice needs the deferred free-text answer surface above.
- Codex parity for tap-to-approve and `ask`.
- Per-user access control — who is allowed to drive which CLI.
- Trusted-operator authorization for actions the CLI would otherwise hard-deny.
- Permission-relay niceties: "see more" expansion, a text `y/n` fallback.

## Reach

- Remote spawn + control of Claude Code / Codex sessions from chat.
- Stream the agent's reasoning to the channel.
- Be the phone surface for session managers (Claude Squad, CCManager, Conductor, …).
- A generic ACP-client adapter — one code path that spawns and drives any
  Agent-Client-Protocol agent (`session/prompt` in, `session/update` out,
  `session/request_permission` relayed to the channel).
- Consider migrating the Grok adapter's inject path from the leader socket (an
  undocumented internal surface) to the documented ACP `grok agent stdio` protocol.

## Web surface

- Mobile UI redesign of the web chat: taste-discovery first (reference links and a react loop
  with the operator before any build), then a phone-first pass over the whole page — driving
  view, composer, history, notices — as one designed surface rather than a stack of features.

## More channels

- **Web-chat phase 1 shipped:** a real `web` channel, private text page, SSE,
  magic-link auth through the Telegram operator DM, and `attach web` routing.
  See [`docs/WEB.md`](docs/WEB.md).
- **Roadmap #14 phase 3a shipped:** the bundled TTS synthesizer and local
  verification CLI.
- **Roadmap #14 phases 3b–4 shipped:** per-session spoken web replies with
  transient playback, plus browser voice notes transcoded at the edge into the
  retained OGG/Opus STT path with transcript edits.
- **Roadmap #14 phase 6 shipped:** manifest-driven spoken-reply guidance,
  explicit on-the-go trigger protocol, and `/c3:on-the-go` / `/c3:off-the-go`
  wrappers.
- **Roadmap #14 phases 5 and 7 shipped:** foreground hands-free VAD, spoken-turn
  state machine and barge-in, plus the installable PWA shell and screen wake lock.
- The full rationale and later voice/native directions remain
  in the [on-the-go / voice-channel design capture](docs/future/on-the-go-voice-channel.md).
- Other transports the interface already admits (Slack, Matrix, …).

## Telegram completeness

- Media-group / album assembly, media echo by `file_id`, message forwarding.
- Richer formatting (underline, inline mentions), location sends, more poll options.

## Packaging & platform

- Drop the development-channels flag once C3 ships through a trusted plugin store.
- External (non-Go) loadable plugins.
- Broker-side `/list` and `/route` commands.
- Monitoring dashboard, persistent message history, STT latency instrumentation.
- Async-dispatch more non-critical broker sends (as the voice-readback echo already does), preserving strict per-topic ordering.
- Delivery-contingent consume for `fetch_queue` — the live-push half now consumes by message id (the ack removes exactly the lines its push covered), but the fetch path still commits its consume before the response is known to have been received, so an adapter that abandons a fetch can have the batch consumed anyway. Close that window the same way: peek, respond, then an explicit id-targeted ack. Needed by the pooled-queue work regardless.
- Attach-replay refinements: gate `disambiguate_dm` on `Replay`, and honor `group` in the step-2 name lookup, so a replayed DM or non-default-group attach restores cleanly instead of falling to a discarded proposal. This also covers a case the steal-sanitize introduced: a DM attach that steered past disambiguation with `steal=true` is remembered as `{Target:"dm", Steal:false}` (the one-shot steal is stripped), so a fresh-broker replay lands back on the `disambiguate_dm` proposal — discarded → detached (the auto-recover net usually restores it) — which the `Replay` gate would fix.
- Surface the silent auto-recover skip: when a resumed session's own last topic is held by another live session, carry a `Skipped`/reason field and show a one-line CLI notice, instead of resuming quietly with no explanation.
- Idempotent-attach hint: an already-attached bare `attach` could return an "already attached to X — to switch topics attach by name, or detach first" hint instead of a bare confirmation. Needs an additive `AttachedMsg` field so the formatter can tell an idempotent re-confirm apart from a fresh claim.
- Live-verify auto-attach-on-resume end to end. It ships on by default (`auto_attach_on_resume` absent in mappings.json ⇒ enabled; set it to `false` to disable). A first real resumed-session walk-through (2026-08-25) exposed a silent failure under Claude Code ≥2.1.245: the host began exporting the STABLE session id as `CLAUDE_CODE_SESSION_ID` (previously the ephemeral per-MCP-spawn id), so the adapter polled a handoff key the SessionStart hook never wrote and recovery never fired. Fixed by writing the handoff under BOTH the instance-id and stable-id keys, so recovery fires whichever id the host exports. A full live re-verify on a build carrying that fix is still pending.
- In-app conversation-switch detection (`checkForIdentitySwitch`) is structurally inert on Claude Code ≥2.1.245 and needs its own fix. The `/clear` chain hop is written at `<ephemeral>.json` (the hook keys on `CLAUDE_ENV_FILE`), but the adapter's probe key is the frozen stable id, whose `<stable>.json` alias is self-referential — so the hop is never seen. The dual-key handoff above restores the *resume* path only; "reattach works" must not be read as "switch detection works" on new hosts.

## Far future — a stateless broker over a real database

The broker holds live state in memory: open interactive requests (permission
prompts and questions awaiting an answer), attempt bookkeeping, and route
ownership. The durable inbound queue is on disk; this interactive state is not.
So a restart loses whatever was waiting for a human, and making that survivable
means writing crash-safe records, matching a later answer to a saved request, and
keeping old and new peers agreeing about the identifiers involved.

That is the beginning of a transaction log, and following it to its end means
building a database by hand. The better destination is the opposite: let the
broker hold no durable state at all. Interactive requests, attempts and ownership
move into a real datastore with real transactions. Then the broker can be killed
at any moment, restarted at any moment, and more than one can run at once,
because none of them own anything that matters.

What it would unlock: a restart, crash or OOM kill costs nothing, because nothing
was in memory to lose; updates stop being a special event, with no drain, no
deferral, no cancellation notice; and there is a path to horizontal capacity and
to running the broker somewhere other than the user's own machine.

What it costs: a hard dependency the project does not have today, a schema and a
migration story, and a latency budget on paths that are currently a map lookup.
Worth paying only once the single-machine, single-broker assumption is genuinely
in the way.

**Until then, avoid the middle ground.** Partial durability — persisting some
interactive state and reviving it after a restart — carries much of the cost of a
database and few of its guarantees. Where a restart would disrupt a human's
in-flight answer, prefer deferring the restart, or cancelling the request
honestly, over trying to resurrect it.

## Open design questions

- Whether a typed free-text answer is also queued as a normal message, or consumed only as the answer.
- Grant UX for operator authorization (per-action prompt vs standing grant).
