# Using C3

This is the day-to-day guide for someone who has C3 installed and wants to actually work with it. For first-time install, see `INSTALL.md`. For developing extensions, see `PLUGINS.md` / `CHANNELS.md` / `ADAPTERS.md`.

## What C3 does for you

C3 is a Telegram (today; more channels later) multiplexer for Claude Code, Codex, and any other CLI you have a C3 adapter for. One Telegram bot, many CLI sessions, each one bound to its own forum topic. From any CLI session you can:

- Send replies to your phone (Telegram).
- Receive messages from your phone, including voice notes (transcribed automatically) and quote-replies (with the quoted text in context).
- Have multiple terminals running simultaneously, each on a different topic, all going through the same bot.
- Switch between Claude Code and Codex on the same project — they coordinate via the broker so you don't accidentally end up with two CLIs answering the same Telegram message.

## The mental model

Three things to internalize:

- **One broker per machine.** It's a long-lived background daemon that owns the bot token, polls Telegram, and routes messages. You never start it manually — your first CLI session of the day does that for you.
- **A session remembers its own topic — it never guesses.** The first time you `attach` in a session you pick a topic (or create one); C3 records that choice against the session. When that session is resumed later, a bare `attach` silently re-attaches its **own** last topic. A brand-new session with no prior choice gets a friendly picker — it never binds a topic you didn't choose. Your cwd only *seeds* the picker's suggestions (the current project's topic ranks first); it is never a silent claim.
- **One claim per topic.** Two sessions can't hold the same topic at once. The second session sees who's holding it and stays unattached. Useful: you can park Claude Code on the topic and open Codex elsewhere; one project, one Telegram chat, no double-replies.

## The one command you'll use most

```
attach
```

Type it (or have your CLI agent type it) when you want to bind this session to a Telegram topic. A bare `attach` resolves in a fixed order and **never guesses a topic you didn't choose**:

1. **Already attached** — idempotent. The session confirms its current claim; nothing else happens.
2. **This session has attached before** — silent resume of its **own** last topic. You see "Auto-attached to 'foo'" and inbound Telegram messages start flowing. (The choice is remembered against the session, not the directory, so it only ever re-claims your own route.)
3. **First-time session (no prior choice)** — a friendly picker. C3 shows a short ranked list — the current project's topic first (seeded from cwd), then recently-used topics, plus "create new" and "see the full list" — and asks you to choose. It **never** auto-picks. Your pick is an explicit `attach(topic_id=<n>)` (or `attach(name="<name>", create=true)` to create), which is then remembered so future resumes are silent.

If the topic you target is already claimed by another live session, the broker tells you who's holding it; wait for it to detach or attach to a different topic.

Variations:

```
attach <name>           # attach by topic name. Searches default group first,
                        # then all groups, with disambiguation if found
                        # in multiple. Otherwise proposes creation.
attach <topic-id>       # claim an existing topic by Telegram thread id
                        # (a bare integer).
attach dm               # route to your personal DM with the bot. Works
                        # anywhere, no mapping persistence (DM is universal).
attach create <name>    # create + claim a new topic named <name>.
attach -y <name>        # same as `create <name>`.
```

Structured args (`name=`, `topic_id=`, `target=`, `create=`) and the group override are passed by the CLI agent via the `attach` MCP tool, not typed as flags — see `docs/COMMANDS.md` for the full parser table.

## The other commands

```
topics                  # list known topics + who's currently holding each
```

That's basically it for daily use. Once a session has attached to its topic, resuming that session re-attaches automatically and your messages flow.

## Telegram-side workflow

You created a bot with `@BotFather` and got a token. You added the bot as an **admin** with `Manage Topics` permission to a supergroup. Setup's pairing step discovered your user id (`dm_chat_id`) and the group's chat id for you — you sent a short code to the bot in DM and another in the group.

From there:

- **Sending a message into a topic** — your CLI's reply tool sends a message into the right thread. From your phone you see them in the topic.
- **Replying from your phone** — type into the topic's chat. The CLI sees your message as an inbound block.
- **Voice notes** — record on your phone, send to the topic. C3 persists a pending row immediately, then the bounded STT scheduler transcribes off the route worker; the CLI sees `[Transcribed voice]: <text>` plus the original voice attachment available for re-download if the transcript is wrong.
- **Quote-replying** — long-press a CLI message, hit Reply, type. The CLI sees the inbound with `reply_to_message_id`, `reply_to_user`, and `reply_to_text` so it knows which prior message you're answering.
- **Multi-message bursts** — multiple **text** messages in quick succession are collapsed by the debounce window (default 1500ms). Saves your CLI from getting confused by interleaved partial thoughts. Photo/file **albums** are not assembled as a unit in v1 — album siblings arrive as separate inbounds merged only by the debounce window; reliable album handling is a known gap (see [`ROADMAP.md`](../ROADMAP.md)).

## Durable inbound queue & backlog

While the durable queue is healthy, once C3 has *received* a Telegram message it is written to disk before anything else consumes it, and it stays there until a session takes it — even if no CLI is attached, or the broker was down when it later caught up. If the queue cannot start, C3 continues in the loud degraded mode described below instead of pretending this promise still holds.

How it works:

- **Held on disk before the read offset moves.** A message that arrives while no session is attached to its topic is appended to a durable per-route queue under `$XDG_STATE_HOME/c3/queue/` (fallback `~/.local/state/c3/queue/`) and `fsync`'d to disk. The broker advances the Telegram read offset only *after* the message is durably persisted — so a broker crash mid-flight leaves the message with Telegram, which redelivers it on the next poll.
- **Voice is enrichment, not an intake gate.** A voice message is persisted with an explicit transcription-in-progress placeholder before STT starts, and the read offset may advance at that point. STT has two bounded runners and never occupies the route worker. A pending row can be inspected or consumed with `fetch_queue`; if it was consumed before STT finishes, the result arrives as a separate transcript-update row instead of rewriting history. Transient download failures park with exponential backoff (30 seconds to 5 minutes), wake when channel health recovers, and become a terminal failure after 24 hours by default. Set `plugins.stt.voice_retry_expiry` to a Go duration string such as `"12h"`, or a number of seconds, to change that expiry.
- **The one exception, and it is worth knowing.** If the queue directory cannot be opened at startup — unwritable, full, wrong permissions — the broker logs `durable inbound hold DISABLED for this run` and keeps going *without* a queue. In that mode messages still reach an attached session live, but nothing is written to disk and the read offset still advances, so anything arriving while no session is attached is gone. It advances the offset deliberately: an un-acked Telegram update never clears, so refusing to ack would wedge *all* inbound rather than lose one message. C3 takes the loss and makes noise about it — see **Degraded mode** below.
- **Held-count auto-reply.** The first held message in a topic gets one reassurance reply: *"📨 Held — nothing lost. N message(s) queued. Send /status to check."* On Telegram a fresh notice fires per held message (rate-limited to one per topic per 10 seconds, carrying the *running* count) so a new arrival is always signalled; messages inside that window are queued silently, with no reply per voice note.
- **Backlog on attach.** When you `attach` to a topic with held messages, the session is told how many are queued (with a short per-message preview) and instructed to call `fetch_queue` to retrieve them. The agent decides whether to drain all at once or work through them in batches.
- **Live messages are unaffected.** When a session is attached, messages still push through immediately; they're removed from the queue once the agent has actually taken them. The queue earns its keep only when there's no live consumer.

### Degraded mode — when the durable queue is disabled

Everything above is a promise C3 can only keep while it has a queue. Without one it inverts every surface rather than going quiet, because the failure is otherwise invisible until you go looking for a message that no longer exists:

- **At startup** — a warning to your Telegram DM, plus a system advisory to any live CLI session.
- **On `/status`** — a `⚠️ ... durable queue is DISABLED` line on both the per-topic and the global summary, so the reassuring `0 queued · broker up` can never stand alone.
- **In `c3-broker status`** — the same persistent `durable queue: DEGRADED` warning appears in the one-shot shell summary for as long as this broker is running without its queue.
- **On every hold** — the auto-reply becomes *"⚠️ NOT held — that message was dropped."* instead of *"📨 Held — nothing lost."* It is deliberately on the slower 5-minute-per-topic cooldown rather than the 10-second held-notice window: the warning is the same sentence every time, and repeating it through a burst is how an alert gets ignored.
- **In `broker.log`** — the `durable inbound hold DISABLED for this run` line at startup, then a `DROPPED — durable queue disabled` line for every dropped message, carrying the sender and the text (truncated to 200 characters). Not a substitute for the queue, but enough to know what you missed.

The fix is to make the queue directory (`$XDG_STATE_HOME/c3/queue/`, fallback `~/.local/state/c3/queue/`) writable and restart the broker. Live delivery to an attached session is unaffected throughout.

### Pulling the backlog — the `fetch_queue` tool

Your CLI agent retrieves held messages with the `fetch_queue` MCP tool (both Claude Code and Codex have it):

- `limit` — how many oldest messages to pull. Default **3**, max **50**, or the string `"all"` to drain everything. Small batches let the agent process carefully one group at a time. Every response, including a finite limit, is frame-budgeted and may return only a prefix (for example, three records) with `remaining > 0`; call again until `remaining` is zero.
- `ack` — default **true**, which *consumes* the messages (walks the cursor forward; the queue files are deleted once fully drained). Pass `ack=false` to *peek* without consuming.

Each returned record normally carries sender, kind, timestamp, text or voice transcript, quote-reply context, and attachments (each with `file_id`), plus `remaining`, the count still queued. Two bounded exceptions are deliberate: a record above the 1 MiB queue-record cap has truncated text with an in-band marker; a legacy record that cannot fit in any 4 MiB IPC response is moved aside and replaced in position by a broker-authored notice carrying the original identity but **no original content**. The full original is retained only when `.trash/` retention is available; otherwise the marker or notice says no copy was kept. Treat the notice as an operational warning, not as what the sender said. Adapter authors: the response correlation id is also capped at 1 KiB because it shares this frame.

### Fixing a failed transcription — the `retranscribe` tool

STT failures are usually a transient or down provider, not lost audio. When transcription fails, the *agent* sees a self-documenting recovery message (not a dead end): it states the audio is saved, that you don't need to resend, and exactly how to recover it — `download_attachment` to fetch the audio, or `retranscribe` to re-run STT once the provider is healthy.

`retranscribe(file_id)` joins the same bounded, durable scheduler as automatic transcription and still returns the fresh transcript synchronously. Pass the optional `message_id`: if that message has a pending voice row, the scheduler resolves it in place; if the row was already consumed, is already final, or is absent, C3 appends an explicit `[transcript update for voice message …]` row. A caller timeout or disconnect does not cancel the durable resolve or its delivery. The original attachment remains available, so a failed transcription does not require re-recording.

### Checking queue depth — `/status` in Telegram

Type `/status` directly into a Telegram chat (it autocompletes in the `/` menu) to see queue depth and attach state. This is a *Telegram bot command*, distinct from the `/c3:status` CLI slash command — the broker answers it directly and never routes it to an agent.

- **In a topic** → that topic's status, e.g. `📊 myproject · 3 queued (oldest 2h) · no CLI attached · broker up`.
- **In DM or General** → a global summary across all routes (empty queues omitted), e.g.:
  ```
  📊 Broker up (pid 12345). Active queues:
  • myproject — 3 (oldest 2h)
  • docs — 1 (oldest 10m)
  1 attached · 1 idle
  ```

Two sibling bot commands manage the **pooled queues** (messages accumulating
on unattached topics): `/queue` lists every non-empty queue (`[serial] name ·
pending · oldest · newest` — metadata only), and the operator-only
`/queue <name>` / `/drain <src> <sel> [to <t>]` peek and move queued messages
between topics. Full grammar and the authorization matrix:
[`COMMANDS.md`](COMMANDS.md) "Telegram bot commands".

### Limits

- Per-route cap: **1000 messages OR 14 days**, whichever comes first. On overflow the oldest held messages are dropped, logged to `broker.log`, **and** announced in the topic (*"⚠️ queue full — dropped N oldest held message(s); attach a session soon."*) — never a silent truncation.
- Per-record cap: **1 MiB encoded JSON**. The queue retains the full original in `.trash/` first, then stores a truncated record with an in-band marker so the route can continue rather than redelivering the same unwriteable record forever. If retention is disabled, the marker says that the removed text was not kept.
- **24-hour Telegram bound (outside C3's control).** Telegram itself keeps undelivered updates for at most 24 hours. C3 can only queue what it has actually received, so a gap longer than 24 hours with **no broker polling anywhere** loses messages at Telegram's level before C3 ever sees them. Keeping a broker polling (the opt-in `systemd --user` unit helps) is the only guard against that window.

### Safe draining — named nudges and the confirmed-route guard

Two belts keep a drain from ever taking the *wrong* topic's messages:

- **Nudges name the topic.** Every "N held — call `fetch_queue`" advertisement (backlog summary, pending nudge, held notice) now names the route it refers to. A stale or mis-addressed advertisement is therefore distinguishable at a glance, rather than an anonymous count you can't place.
- **`fetch_queue(ack=true)` only consumes off a confirmed route.** The destructive drain (and the live-push delivery ack) is refused unless the session holds a route it *explicitly confirmed* — an explicit attach, a silent resume of its own topic, or an explicit pick from the picker. A session can never consume a topic it merely had suggested to it. This is fail-closed insurance: every legitimate claim sets the flag, so it only ever fires against a future regression that binds a route without a real claim.

### Queue trash & recovery

Nothing ever leaves the queue by hard delete. When a route drains — the right topic, a wrong topic, a rogue skill, an orphaned consume — the queue files are **moved into a `.trash/` subdirectory**, not removed. Any drain is therefore recoverable for the retention window (≥14 days). This is a manual, broker-side recovery — there is no Telegram surface for it.

> **Caveat — retention can be disabled.** The `.trash/` window is defense-in-depth on top of the primary durable queue, and it is **skipped** if the broker could not create the `.trash/` directory at startup (e.g. a stray file occupies the path). In that degraded mode drains **hard-delete** and are **NOT recoverable**. The broker logs exactly one line to `broker.log` when this happens:
>
> ```
> queue: WARNING could not create retention dir …/.trash: … — running with .trash retention DISABLED; drains hard-delete instead of retaining for TrashTTL
> ```
>
> If that line is present, the recovery steps below do not apply until you clear the `.trash/` obstruction and **restart the broker** (retention state is decided once, at `NewStore`).

The trash lives beside the queue, at `$XDG_STATE_HOME/c3/queue/.trash/` (fallback `~/.local/state/c3/queue/.trash/`). Four kinds of file appear there:

- **A retired drain pair** — `<base>.<stamp>.jsonl` is the full line history at the moment of the drain, and `<base>.<stamp>.cur` is the pre-drain cursor. The pair shares one `<stamp>` (a UnixNano timestamp). `<base>` is the route-key filename — `<channel>__<chat_id>__<topic|none>`, e.g. `telegram__-100__none` for a DM/no-topic route, or `telegram__-1001234__948` for topic 948.
- **An evict snapshot** — `<base>.<stamp>.evicted.jsonl` holds lines dropped by the per-route cap/age eviction (never a whole drain).
- **A quarantined corrupt line** — `<base>.<stamp>.corrupt.jsonl` holds the **raw bytes** of one or more lines the queue could not parse, copied out before they were removed from the live queue. The usual cause is a write cut short by a full disk or a power cut, and the fragment may be part of a real message. The removal is also logged to `broker.log` (`removed N unparseable line(s) …`), so this never happens silently.
- **An oversize original** — `<base>.<stamp>.oversize.jsonl` holds a full record that exceeded the 1 MiB append bound before the queued copy was truncated. A legacy record too large for any IPC frame is also moved aside under this retention policy; the session receives an identity-preserving notice rather than its content, and the topic receives `⚠️ N message(s) were too large…` so the set-aside is visible.

  > **These are NOT valid queue records — never feed one back into a live queue.** Read the file by hand to see what the fragment was. Concatenating it into `<base>.jsonl` (as the step-3 recipe below does for a *retired* `.jsonl`) would inject an unparseable line into a healthy queue, which the store would then have to quarantine all over again.

**Recovering a drained batch (broker STOPPED).** The store is single-owner via the route workers, so an external `mv` against a live broker races the writer. Stop the broker first (`pkill c3-broker` from a separate terminal), do the moves, then let the next CLI session spawn a fresh broker — `RecoverOnStartup` re-indexes the restored route, and you `fetch_queue` it from the session attached to that topic.

1. `ls "$XDG_STATE_HOME/c3/queue/.trash/"` (or `~/.local/state/c3/queue/.trash/`) and find the pair for the route; note the `<stamp>`.
2. **No live `<base>.jsonl`** (nothing new arrived since the drain): `mv` both files back to `<base>.jsonl` and `<base>.cur`. Restoring the `.cur` replays **only the final drained batch** (`lines[cursor:]`). Omit the `.cur` to replay **everything** in the file (over-delivery, never loss).
3. **Live `<base>.jsonl` exists** (new messages arrived since): concatenate the trash `.jsonl` **first**, then the live `.jsonl`, into the live name, and remove the live `.cur`. Use the retired `<base>.<stamp>.jsonl` only — never a `.corrupt.jsonl`, whose contents are not valid records:
   ```
   cat .trash/<base>.<stamp>.jsonl <base>.jsonl > <base>.jsonl.tmp \
     && mv <base>.jsonl.tmp <base>.jsonl \
     && rm -f <base>.cur
   ```
   Dropping the cursor replays the whole merged file (the old cursor no longer aligns once you prepend history — over-delivery of the recovered lines, never loss).
4. **A partial wrong-drain that's still live** (no trash pair yet — the file is still in the queue dir): there's nothing to move. Lower or delete the live `<base>.cur` (delete = replay from the first line).

**GC.** Trash is swept automatically (piggybacked on drains, plus one sweep at broker startup — no extra process): every retired file is kept at least the retention window (14 days, the same window an undelivered message gets), and hard caps of 256 MiB / 8192 files evict oldest-first if trash grows past them (the newest snapshots — the likeliest recovery targets — survive). A cap-eviction that shortens the promised window logs one line to `broker.log`.

## Multi-group setups

Configure multiple Telegram supergroups in `mappings.json`:

```json
{
  "channels": {
    "telegram": {
      "default_group": "main",
      "groups": {
        "main": {"chat_id": -1001111, "title": "Personal"},
        "work": {"chat_id": -1002222, "title": "Work multiplexer"}
      }
    }
  }
}
```

`attach` and `attach <name>` default to `main`. An agent can target the work group with structured MCP arguments such as `attach(name="<name>", group="work")`; there is no shell-style `--group` parser. The broker records the chosen group against the session, so resuming that session re-attaches to the right group.

Each group needs the bot as an admin with `Manage Topics`.

## Cross-CLI on the same project

You're in `~/projects/widget`. You started Claude Code there earlier and attached it to topic `widget` (a bare `attach` seeds the current project first in the picker, so it's one tap). Now you want to ask Codex something about the same project. From a different terminal:

```
$ cd ~/projects/widget
$ codex
```

The `codex` command goes through the C3 launcher, which spawns a Codex app-server, registers the C3 MCP adapter, and launches the visible TUI bound to that app-server. A bare `attach` from Codex shows the picker with `widget` ranked first — but marked **held by** the Claude session, so you're warned instead of silently stealing it. To take over, `/exit` the Claude session to drop the claim (`c3-broker release <cwd>` is on the roadmap but stubbed in v1), then `attach` from Codex.

If you want Claude and Codex on different topics in the same group, attach Codex to a different topic explicitly: `attach(name="widget-codex", create=true)`. The broker creates that as a sibling topic in the group, and Codex remembers it for that session's future resumes.

Recovery ids are scoped by CLI family. If two hosts happen to issue the same opaque session id, Claude's remembered topic and Codex's remembered topic remain separate; an old unqualified record can be adopted by at most one family.

## Editing mappings.json by hand

Nothing stops you. Mode 600, JSON, atomically rewritten by the broker — but if you `vim` it while the broker is running and save, the broker's next read will pick up the change. Common edits:

- Rename a topic locally (without renaming on Telegram). Changes the `name` field; routing keeps working because `topic_id` is the source of truth.
- Migrate a mapping to a new cwd (e.g. you renamed a project directory). Move the entry's key.
- Add a group. Insert under `channels.telegram.groups`; reload the broker (or restart your next session).
- Remove a stale mapping. Just delete the cwd's entry.

If you bork the file structurally (invalid JSON), the broker refuses to start and prints the parse error. Restore from your backup or fix the syntax.

## Deleting a topic

C3 doesn't auto-delete. From your phone (Telegram), long-press the topic in the group's topic list and pick "Delete topic". Then in `mappings.json`, remove the topic from `channels.telegram.topics` and any cwd mappings that pointed at it. Or run the broker's cleanup tool if it exists by the time you read this.

## When things go wrong

- **No CLI is attached, but messages keep arriving** — check `/status`. With a healthy queue they are held and the healthy notice is rate-limited to once per topic per 10 seconds; open a session, `attach`, and drain them with `fetch_queue`. If status says the durable queue is disabled, they are dropped and the explicit loss warning uses the slower 5-minute cooldown. See "Durable inbound queue & backlog" above.
- **`attach` says the topic is held** — `topics` lists who. If it's a stale claim (the holder crashed), the broker now sweeps dead-pid holders on dispatch (2026-05-14 fix); just retry `attach`. If that doesn't free it, quit Claude Code and relaunch — the new session's broker auto-spawn starts clean. Don't bounce the broker from inside CC (killing the broker also kills this session's MCP server, requiring a manual `/mcp` reconnect). From an external terminal, `pkill c3-broker` works. For mappings.json edits, `/c3:reload-config` is non-disruptive.
- **Voice transcription is wrong or failed** — never re-record. The original audio is saved; the CLI can `download_attachment` to re-listen, or `retranscribe` to re-run STT (e.g. once a flaky provider recovers). On an outright STT failure the agent sees a self-documenting message telling it exactly how to recover. The STT plugin's confidence isn't surfaced in v1; treat the transcript as a hint when accuracy matters.
- **Typing indicator** — the broker now auto-pulses a typing indicator on a route while the agent is working, once that session has replied at least once in the topic (the signal you're in an active Telegram conversation). It stops when the agent sends its reply, or after a safety timeout. A brand-new topic shows no typing until the agent's first reply, and default-CLI-mode sessions (that never reply to Telegram) never pulse it.
- **`codex` doesn't seem to be using C3** — check `which codex` returns the C3 launcher (`$GOBIN/codex` after install). Long-running shells hash; open a new terminal or `hash -r`. The launcher logs to `/tmp/c3-codex-supervisor.log` — `tail` it during a `codex` invocation to see what it thinks it's doing.
- **`reply` says to attach first** — your adapter lost local state but the broker may still hold your claim. Try `attach` again with the same target; the adapter recovers from the broker's claim (both Claude and Codex adapters do this). If that fails, `topics` shows who's holding what; `c3-broker status` from a separate shell tells you the same with more detail.

## Health checks

Quick sanity sweep when something feels off:

```
c3-broker status              # one-shot summary of everything below
ls -la "${XDG_RUNTIME_DIR:-/run/user/$UID}/c3.sock"     # broker socket; should exist + 0o600
cat "${XDG_RUNTIME_DIR:-/run/user/$UID}/c3-broker.pid"  # the running broker's pid
tail ~/.local/state/c3/broker.log     # broker activity (XDG_STATE_HOME, or default)
tail ~/.local/state/c3/adapter.log    # adapter MCP frames
tail /tmp/c3-codex-supervisor.log     # last codex launcher invocation (if Codex installed)
cat /tmp/c3-codex-app-server-$UID.json  # current app-server signature (cwd, topic, adapter path)
```

If the socket is missing or the pid file points at a nonexistent process, the next CLI session will spawn a fresh broker. If something looks corrupted, `pkill c3-broker; rm -f "${XDG_RUNTIME_DIR:-/tmp}/c3.sock" /tmp/c3-$UID.sock "${XDG_RUNTIME_DIR:-$HOME/.cache/c3}/c3-broker.pid"` resets the world — do this from a separate terminal, not from inside Claude Code (broker death + adapter recycle = double pain). Operational subcommands: `c3-broker status` prints liveness, `c3-broker topics` lists topics and live claim state, `c3-broker validate` parses mappings.json. After editing `~/.config/c3/mappings.json` by hand, `/c3:reload-config` sends SIGHUP to the running broker so it re-reads the file in-place (live claims preserved, no process churn). (`c3-broker release <cwd>` is wired but stubbed in v1 — roadmap.)

## Updating C3

C3 keeps itself current from its GitHub releases. There are three surfaces:

**The always-on notice.** The broker checks for a newer release shortly after it
starts and every ~6 hours after that. When one exists it surfaces `c3 update
available — /c3:update` in the ambient status line (via `health.json`) and logs a
line to `broker.log`. This is not gated by any toggle — you always find out an
update is available. (A build with no embedded version — one you compiled from
source rather than installed from a release — reports `dev` and never checks; it
has no release identity to compare against.)

**Manual update (one command).** Run `/c3:update` in Claude Code, or `c3-broker
update` from any shell. It queries the latest release, downloads the tarball for
your platform, verifies it against the release's `SHA256SUMS`, and validates all
nine shipped binaries plus the complete, non-empty STT handler, runner,
vocabulary, and provider layout. It replaces the eight core binaries and
bundled STT runtime in place, and refreshes `codex` only if that destination is
already a verified C3 launcher; an absent or unrelated `codex` is never created
or overwritten. Nothing is replaced before the whole download and runtime
layout are verified and every binary is staged on its destination filesystem;
each installed component then uses an atomic rename. A rare rename failure
during replacement is reported without claiming every sibling stayed
untouched. `c3-broker update --check` reports current-vs-latest without
installing.

On Windows, `--check` works but installation is refused: live `.exe` replacement
can leave a mixed-version install. Fully quit C3 and the coding CLI, then
re-extract the newer release tarball over the installed binaries.

The swap is on disk only: the **running broker keeps its old code until it
restarts**. From a separate terminal, `kill -TERM <pid>` (the command prints the
pid) bounces it — adapters reconnect with backoff and re-spawn the new broker,
replaying their attach, so live sessions recover on their own. Don't bounce the
broker from inside Claude Code (that recycles this session's MCP adapter); quit
and relaunch instead, and the next adapter spawn brings up the new binary.

**Automatic update (opt-in).** Set `"auto_update": true` in `mappings.json`
(default off) and the broker installs a newer release **itself** when its ~6h
check finds one, then does the most restartless restart available: it drains
in-flight work, posts a one-time "c3 updated to vX — broker restarting, sessions
reconnect automatically" notice to your attached topics and CLI sessions, and
exits cleanly. Because adapters already survive a broker bounce (exponential-
backoff reconnect + replay-last-attach, and a reconnect auto-spawns the broker
binary), the new broker comes up on its own and sessions reattach — no manual
step. Under **systemd** (`Restart=always`, see `docs/systemd/`) the unit restarts
it; without systemd the next adapter reconnect spawns it.

C3 updates its own **binaries and bundled STT runtime**. The other plugin files
(slash commands and hooks) update through Claude Code's marketplace — run
`/plugin` and update the c3 marketplace when it offers a newer version.

Security notes: downloads are HTTPS-only and checksum-verification is mandatory;
C3 never downgrades and never installs a prerelease automatically. There is no
release-signature check in v1 (checksum-only) — that's future work.

### Upgrading — attach behavior changes

This release changes how a session binds to a topic. Three user-visible changes:

- **Auto-attach-on-resume now defaults ON.** When you resume a session, C3
  silently re-attaches it to its **own** last topic (keyed on the session id, so
  it only ever re-claims your own route — never a neighbour's). Previously this
  was off unless you opted in. To opt **out**, set `"auto_attach_on_resume": false`
  in `mappings.json`.
- **A bare `attach` shows a picker instead of guessing from cwd.** A first-time
  session (one with no remembered topic) now gets a friendly ranked picker and
  chooses explicitly, rather than silently claiming whatever the cwd mapping
  pointed at. cwd only *seeds* the suggestions. After you pick once, future
  resumes are silent (per the first change above).
- **Codex no longer guesses an attachment at startup.** A fresh Codex session
  starts unattached and a bare `attach` opens the picker. A resumed Codex
  session still recovers its own remembered topic from its CLI-scoped stable
  identity. The old launch-time cwd guess is gone.
- **Restart your running CLI sessions after updating.** An old in-process adapter
  that replays a bare `attach` onto the freshly-restarted broker can land on the
  new picker (a discarded proposal, not a claim) and stay detached until you run a
  manual `/attach`. Inbound is held in the durable queue while detached, not
  dropped — but a quick relaunch of each live session avoids the surprise.

## Privacy and safety

- The bot token is in `mappings.json` at mode 600. Treat it like a password.
- Anyone in your Telegram supergroup can send messages that hit your CLI. The `master_user_id` field in `mappings.json` is plumbed for future per-user access control; today, the bot trusts everyone in the group. Use private supergroups.
- C3 doesn't store message history beyond what's needed for routing. If you want a message log, set up Telegram's own export, or write a plugin that subscribes to `OnInbound` and writes to a file.
- The Codex bridge spawns app-servers. They're long-lived processes that hold MCP servers loaded; they listen on `127.0.0.1` only (not exposed to the network). Concurrent launchers serialize port selection and child readiness with a per-user runtime-directory lock, and each child starts on an OS-assigned ephemeral port. If you `pkill codex-app-server` everything cleans up.
