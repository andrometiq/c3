# Work plan

What to do next, in order. `ROADMAP.md` holds the longer-term features and `DECISIONS.md` the
rulings; this file is the near-term plan for whoever picks the work up. Keep it current: strike an
item when it lands and add the commit.

## Where things stand (2026-11-21)

- `master` is 48 commits past the last release, `v0.4.2`. Everything there is deployed on the
  maintainer's own broker and used daily; none of it has been released.
- The big unreleased feature is **live boards** (`docs/BOARD.md`): an agent-written HTML page served
  by the web channel, with a chat per part that C3 draws itself (typing, voice notes, files, replies to
  a message, read-aloud), proposals the owner approves in one tap, and parts redrawn in place. Boards
  are reached from a phone through an authenticated HTTPS front (`docs/BOARD.md`, "The authenticated
  HTTPS front").
- The web chat (`docs/WEB.md`) shares the board's voice and speech plumbing.

## 1. Ship boards as a release

The gaps between what runs today and a release, roughly in order:

1. **Recording wave on Android Chrome.** While the owner records, the shell draws a level meter
   (bars plus a ring around the mic) from a Web Audio `AnalyserNode` (`internal/channel/web/board.js`,
   `listen()` and the `WAVE` constants). On at least one Android phone the `AudioContext` created in the
   mic tap stays `suspended` and ignores `resume()`, so the wave stays flat; the recording itself
   (`MediaRecorder`) is fine. Three fix rounds failed, so redesign rather than patch: first write down
   how Android Chrome treats an `AudioContext` made before or after `getUserMedia` resolves, then pick
   one of: a fresh context per recording created after the stream arrives (closed afterwards); the
   analyser connected through a zero gain to `destination`; or a meter that doesn't use Web Audio. The
   shell sends one diagnostic line per voice note in the `X-Voice-Meter` header
   (`state=…;rate=…;frames=…;zero=…;peak=…;floor=…;lit=…`), which the broker logs as
   `board: "<name>" voice meter …`; `zero == frames` with `state=suspended` is the failure. Seen on the
   phone: with the first wave build, `state=suspended … peak=-52 lit=153` (some levels); after the
   resume fix (`6409ce1`, resume on `statechange` and on every 100 ms bar),
   `state=suspended;frames=3657;zero=3657;peak=-120;lit=0`. `scripts/board/check-voice.mjs` has a
   suspended-context case and passes, yet the phone fails, so the check doesn't reproduce Android:
   only a real Android recording proves a fix.
2. **Sheet-button redress.** The page chooses when C3's chat sheet opens and can reshape it, so a page
   could open the sheet, or move a quick answer, a proposal's button or Send, just as the owner's finger
   lands, and that tap would count as the owner's (it would not carry the "sent by the page" mark). On a
   desktop a page-opened sheet also takes keyboard focus. The mic already has this guard (a press counts
   only if it began on the mic at least 500 ms after the mic appeared or moved, `MIC_SETTLE_MS`). Fix:
   apply the same rule to every button in the sheet, settle again whenever the page reshapes the sheet,
   and focus the composer only for chats the owner opened. Add a browser check for it. A
   work-in-progress fix is on branch `board/sheet-guard-wip`: Go tests pass, but `check-voice.mjs`
   then times out at "run" (waiting for the answer in the general chat), probably because the
   check's own taps land inside the new settle window.
3. **Flaky broker tests** (below). Each one costs a pre-commit rerun.
4. **Docs and release notes:** README's board section, `docs/BOARD.md` read through once against the
   code, release notes per `AGENTS.md`, and the `plugins/c3/.claude-plugin/plugin.json` version bump.
5. **Sign-in for anyone else.** Today a board is reachable only through the maintainer's own front. A
   release needs a generic way for any user to open their boards from a phone: their own front, or a
   C3-issued link over a tunnel. Never a public, unauthenticated listener. `ROADMAP.md` has the
   release gate for this.
6. **Small UX items** found in daily use:
   - Read marks are per browser (shell-origin storage; a first visit counts everything as read).
     Server-side marks per viewer would follow the owner across devices.
   - A reply target that leaves the 500-entry log while a recording is running.
   - A mic press inside the 500 ms settle window is dropped with no feedback.
   - Some file types get the extension their bytes show (a `.docx` arrives as `.zip`).
7. **Validation:** use boards for real work on a few projects, and give the release one independent
   end-to-end review before tagging.

## 2. Board features after the release

- Others watching a board, and (if the owner allows) typing. The residual frame leaks in
  `docs/BOARD.md` must be closed first.
- A C3-drawn Approve that the page can't press.
- Boards for CLIs whose host turns C3's tools off (including the T3 adapter below).
- Handing a board to a new conversation; page history past 500 entries.
- Reply options for agents: today harnesses send buttons; research where polls and rich text fit
  and put it in the MCP instructions.

## 3. T3 Code adapter

A first-class adapter for T3 Code sessions is ruled to live in this repository (`DECISIONS.md` D046). T3 Code becomes the main path,
and the direct CLI path stays working as the fallback. Nothing is built yet; the design starts from
how T3 hosts a Claude thread and how its approval prompts reach the owner. Boards for T3 threads come
after it.

## 4. Unmerged work worth knowing about

On GitHub:
- `fix/stt-safety-fallback`: asks a second speech-to-text model when the first refuses audio for
  safety. One commit; review and merge or close.
- The branch whose tip is "fix(broker): Keep broker notices off routes the private holder holds"
  (2026-11-13): five commits for a DM-holding downstream use; check what is still needed against
  `master`.
- `codex`, `fix/codex-native-delivery`, `grok-build`, `feat/swarm-mode`: September work, now far
  behind `master`; likely superseded. Read before deleting.

Only on the maintainer's machine (pushed separately if wanted):
- `feat/xcli-grok`: the cross-CLI test rig for Grok. It needs a live probe of Grok's fetch mode, then
  the rig's live smokes.
- `fix/stt-ipv4-first`: resolves Telegram's IPv4 addresses before IPv6 for speech-to-text downloads.

## Known flaky tests

These pass alone and fail under load (timing waits), mostly in `internal/broker`:
`TestNegotiatedWorkerDeadlineHeldOnceAndLiveness`, `TestControlledRestartCapRacesAnswerExactlyOnce`,
`TestVoiceFetch_HandlerUnavailableParksUntilRestored`, `TestHandleObserve_ReservesItsLargerEnvelope`,
`TestInjectedMidTurnExactlyOnce`, and under `-race`
`TestSourceSchedulerRestartTargetsKeepOwnOccurrenceInReplacement`.
`TestWebHeldCopyAndKeyboardlessPermissionNotice` (`internal/broker/web_plumbing_test.go`) fails under `-count>1` because state
carries over between runs. When the pre-commit `make ci` fails on one of these, run that test alone
(`go test ./internal/broker -run '^Name$' -count=5`) before suspecting the change. Fixing the class
(waiting on conditions, not on time) is item 3 above.

## How work is checked here

- `make ci` is the gate; the pre-commit hook (`make hooks`) runs it. Save its output to a file and
  grep for `FAIL`: the output is long.
- Board and web chat browser checks run the real Go routes against headless Chromium with a fake
  microphone, nobody in the loop: `node scripts/board/check-voice.mjs .`,
  `node scripts/board/check-speak.mjs .`, `node scripts/board/check-webchat.mjs .` from the repository
  root (Node 20+, Chromium or `$CHROMIUM`, ffmpeg). Add `--shots <dir>` for screenshots. They start
  `TestBoardBrowserHarness` / `TestWebChatBrowserHarness`, which serve the routes with a fake agent.
- Building in a git worktree: `GOFLAGS=-buildvcs=false` (the build stamps VCS data and fails in some
  worktree layouts).
- Deploy a local build with `make install && c3-broker restart`; the restart bounces every attached
  session for a few seconds, so ask the maintainer first.
- The live cross-CLI matrix (`docs/TESTING-LIVE-MATRIX.md`) takes hours and starts real CLI sessions;
  run single cells, and leave full runs to the maintainer's own terminal.
