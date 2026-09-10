# Live matrix driver

Run this **outside the coding sandbox**, from this checkout. It starts real Claude
Code sessions and uses your model credentials. The coding lane does not run it.
Prerequisites: installed Claude Code, authenticated access to Haiku, Go, Python
3.10+, tmux, and a writable private cache. No Telegram credentials are needed.

```sh
# Enumerate all 180 cells, including the 54 structurally infeasible cells:
scripts/live-matrix/run.sh --list

# First pass on a new host version: collect shapes, export reviewable fixtures.
# Defaults to the highest numeric version in ~/.local/share/claude/versions.
scripts/live-matrix/run.sh --collect-only --fixtures

# Assert the complete matrix into a separate output directory:
scripts/live-matrix/run.sh --output "$PWD/local-notes/live-matrix-assert"

# Select an installed binary and one focused cell when reviewing a failure:
scripts/live-matrix/run.sh --claude "$HOME/.local/share/claude/versions/2.1.266" \
  --cell inbox-foreground-resumed-text-single \
  --output "$PWD/local-notes/live-matrix-review"
```

Budget **2–4 hours per complete pass** (126 real cells, 216 conversational
launches including resume seeds, roughly 1–2 minutes per cell;
slow authentication/model setup can take longer). Foreground/background sleeps
are 35 seconds, deliberately beyond the 15-second live receipt window. Every
cell observes for another 45 seconds after injection, including after early
retirement. Setup has a 90-second bound per checkpoint. Two passes cost roughly
4–8 hours. Collection makes no delivery-success assertion; setup errors still
exit nonzero. Assertion mode exits nonzero for any failed selected cell.

Results are `local-notes/live-matrix/<version>/<cell>/records.jsonl`,
`records.expect.json`, `summary.json`, sanitized broker/adapter logs and proxy
events. `REPORT.md` at the output root and under the version directory retains
all 180 rows as PASS, FAIL, COLLECTED, N/A or NOT RUN. Existing results/fixtures
are never overwritten. Use a separate `--output` for another pass. Each report
is refreshed after every cell, so an interrupted run keeps completed evidence.

`--fixtures` copies records and expectation sidecars into
`cmd/c3-claude-adapter/testdata/claude-<version>/<cell>.{jsonl,expect.json}`.
Review before committing. Only previously captured envelope families are
classified automatically; unknown shapes have `accept: null` and a TODO reason.
The Go fixture test explicitly skips these unverified records. Do not turn a
TODO into a positive just to make the test green. The verified 2.1.266 inbox mid-turn enqueue and queued_command records are now
included in the corpus and recognized by the collector.

Negotiated fetch reserves rows until the complete matching host tool-result
receipt confirms the broker-authored group trailer. Unnegotiated fetch retains
consume-on-return compatibility. The proxy holds the real MCP fetch result before Claude can
record it and samples the queue. It never invents a token, receipt, transcript,
or capability report to turn that failure into a pass.

Each cell starts `c3-broker test-serve --allow-test-inject` on a new private state
and socket directory. It installs a tiny local `c3@c3` plugin into a separate
`CLAUDE_CONFIG_DIR`, retaining the installed plugin identity and using the built
adapter through the recording proxy. `SessionStart` runs the actual C3 handoff
hook. Resume cells create a seed conversation, exit, and launch `--continue` in
the same scratch cwd. An untouched fresh prompt is checked for the absence of a
transcript at injection; a version that already wrote one produces a setup FAIL.

Only authentication material and provider environment values are copied from the
user configuration; production plugins, hooks, MCP servers, and permissions are
not inherited. All XDG paths point into scratch. Harness configuration writes
use scratch paths, including plugin installation, and tmux uses a private socket.
This is configuration isolation, not filesystem confinement: HOME remains inherited
and `Bash(python3:*)` permits arbitrary Python, so Claude still has the invoking
user’s filesystem authority. Claude runs with `--model haiku`, `--tools Bash`, and `--allowedTools`
limited to C3 MCP tools and `Bash(python3:*)`. Channel cells add
`--dangerously-load-development-channels plugin:c3@c3`. Fetch cells omit it and
remove the inherited peer endpoint **only in the adapter child's environment**.
Inbox cells require the real inherited owning-session endpoint.

The proxy makes initialization timing repeatable and auto-attaches through the
real adapter's `attach` handler. The only consumed private RPC response is that
harness attach; host tool calls and channel notifications remain unchanged.
Foreground/background states require a matching host Bash tool-use record and
the running Python process's scratch marker, including the requested
`run_in_background` value. Reconnect uses `/mcp` and numbered c3/Reconnect menu
entries. If a host version changes the menu, the cell fails with its reason;
`--reconnect-keys '…'` supplies that version's comma-separated tmux key sequence.
No key sequence is silently guessed. Plugin startup, credentials, menu, readiness,
or state setup failures are visible failures, never N/A.

The isolated host pre-seeds workspace trust and first-run configuration; see
the [installed-version gate audit](FIRST-RUN.md) for every verified key and the
account/provider limits. Setup polling identifies blocking prompts immediately.
A setup failure reports one cause and marks delivery assertions NOT EVALUATED.
The development-channel warning has no saved acceptance setting in 2.1.266/267;
only its exact, visibly selected local-development confirmation is acknowledged.

`--keep-scratch` retains raw evidence **and copied credentials** in the private
cache for local debugging; default cleanup removes them. Exported records replace
all delivery tokens with TOKEN, textual identifiers with ID, numeric identifiers
with 1, users with USER, and local paths with `/work/ID`. Attempt ordinals,
transport framing and peer provenance remain intact. Receipt counts distinguish
intake enqueues from delivered user/queued-command records, and deduplicate only
identical record UUIDs, so a real second delivery is still counted.

Implementation concerns: `matrix.py` enumerates, `host.py` controls configuration
and tmux, `proxy.py` records/barriers MCP, `hook.py` captures SessionStart,
`collect.py` sanitizes and evaluates, `driver.py` orchestrates. Pure helper tests
run without Claude, tmux, a broker, or network:

```sh
python3 -m unittest discover -s scripts/live-matrix -p 'test_*.py'
bash -n scripts/live-matrix/run.sh
```

Host CLI/plugin flags follow the [Claude Code CLI reference](https://code.claude.com/docs/en/cli-usage),
[plugin reference](https://code.claude.com/docs/en/plugins-reference), and
[channel guide](https://code.claude.com/docs/en/channels). Live execution is still
required to validate a particular binary's intake envelopes and UI.

The orchestrator subset is:

```sh
scripts/live-matrix/run.sh --cell '[ci]*-[ifs]*-*-text-single'
```

This matches channel + inbox × idle + foreground + startup × fresh + resumed ×
text × single: 12 matched cells, 10 feasible, 2 N/A (fresh foreground requires a
transcript). It launches **16 Claude sessions**: four fresh launches plus six
resume seeds and six resumed launches. The driver prints these counts before
building or launching; add `--list` to print only the matching cells and counts.
`--collect-only --fixtures` still launches the same sessions.

With injection enabled, any same-UID process that can access the private socket
can invoke the hook. This is the broker socket's existing trust model; the flag
is an opt-in, not an additional caller-authentication boundary.

Phase-5 collection requires `no_false_held` and `route_line_count` evidence for
every cell. Counts start at injection and include the route line in Held. At least one
line is required, with allowance for actual holds during voice enrichment and
live startup. The driver observes for 80 seconds by default (at least 75),
covering the receipt budget and 60-second stability timer. Missing evidence,
zero lines or excess lines fails the cell. Collector
unit tests use local fixtures only and never launch the live harness.
