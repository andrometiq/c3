# Live boards

A board is one HTML page that a CLI session writes and C3 serves on the [web channel](WEB.md)'s
listener. The owner taps a part, types or talks, reads the answer in that part's thread and
approves it; the session changes that part and C3 redraws only it. Agents learn the rules from the plugin
skill (`plugins/c3/skills/board/SKILL.md`) and from each board's attach result.

**Why boards exist.** The owner must grasp the work and decide without a wall of text and without
losing track of many points at once. A board succeeds only if they understand and can work things
out with the least effort; the page's form is the agent's choice for each piece of work. C3 supplies
the layer under any form: per-part threads, voice notes, approvals, and redrawing only what changed.

## Routes and ownership

- `attach expr="+board:<name>"` (the prefix in any case) makes or re-holds a board; names match
  `^[a-z0-9][a-z0-9-]{0,39}$`, `assets` is reserved. `board:<name>` without `+`, structured
  attaches naming the `board` channel, Telegram topic names starting `board:` and a
  `channels.board` stanza are refused; an existing topic with such a name still attaches by its
  `topic_id`. Boards need the web channel running.
- A board is the route `(board, <number>, no topic)`, with its own worker and durable queue;
  numbers are never reused. A session may hold topics, the web chat and several boards. A board
  claim keeps the session's output route, unless the session held nothing.
- Only the session that made a board holds it: CLI, stable session id and folder all equal;
  anyone else is refused, `steal` included. Without a stable id it lasts only the connection.
- The session names the board as `channel="board:<name>"` on `reply`, `edit_message` and `react`,
  and lets go of it with `detach target=board:<name>`. `web` always means the web chat; a bare
  `board` with two boards held fails closed.
- While the session is gone, the owner's actions queue and the status circle's panel says how many wait. Resume
  and broker restart restore the board from the session's record (30 days, with
  `auto_attach_on_resume` on, the default); else its session re-holds it with `attach`. An
  in-place conversation switch leaves it waiting for its own conversation.
- **Make read-only** in the status panel ("This makes the board read-only for good.") does that: C3
  releases the route, drops it from the session's record and tells the session; later actions get
  409.

## What each side sees

The session gets one line per action, drawn by C3 from checked fields, each with its own id:

```text
[board plan · 1.5 · msg 412] make this shorter
[board plan · 1.5 · msg 413] Approved your message 418.
[board plan · 1.5 · msg 414] Tapped "pick-b" on your message 418.
[board plan · 1.5 · msg 415] Reacted 👍 to your message 418.
[board plan · 1.5 · msg 416] (sent by the page, not typed by the owner) Tapped "pick-b".
```

An act the page's script sent (`c3.comment`, `c3.tap`, `c3.approve`, `c3.react`) carries that mark after C3's
header and `from_page` in the log; acts from C3's own chat (the owner's typing, quick answers, a proposal's
button) carry none.

A voice note arrives once the broker has transcribed it, its transcript (or why there is none)
under C3's line. A long recording arrives as numbered parts, each when its own transcript is done,
so possibly out of order; the last says so, and a part whose audio never reached C3 still arrives,
as a line saying it was not sent. When parts stop coming, C3 ends the recording itself
([Voice](#voice)):

```text
[board plan · 1.5 · msg 416] Voice note:
[Transcribed voice]: make this shorter
[board plan · 1.5 · msg 417] Voice note (part 1; more follows):
[Transcribed voice]: first, the intro
[board plan · 1.5 · msg 419] Voice note (part 2, the last): the recording was cancelled; this part was not sent.
[board plan · 2 · msg 431] Voice note ended after part 3: no further part arrived.
```

A quote comes first as `> ` lines; a ref also arrives as the reply context. Owner text with a line
starting `[board ` is refused, invisible characters ignored.

A comment, voice note or file the owner sends as a reply names the message it answers (any on the
board, the owner's own included; `the page's msg M` when the page's script sent it) after C3's id,
with that message's first 80 characters as it now reads, its brackets turned into parentheses. The
page's own comment never answers a message. A file comes as an attachment (`photo` for an image, else
`document`, with its MIME type, size and name) under a line naming it, its caption below.
`download_attachment` with its `file_id` (C3 asks the session's other held routes when its output
route doesn't know the id) copies it to `$XDG_CACHE_HOME/c3/board/attachments/<id>.<ext>`, the extension
from the type its bytes show, and the c3 plugin lets that session `Read` the copy without a prompt.

```text
[board plan · 1.5 · msg 432 · reply to msg 418 "Shorter, and the risk moved to the top."] why the top?
[board plan · 1.5 · msg 433] File "login-error.png":
this is what I see
```

The session sends text (shown in the part its `reply_to` answers), edits of its own messages,
reactions, and pages: one regular `.html`/`.htm` file of at most 5 MiB per reply, stored as the
next revision. Buttons, polls and other media are not taken. C3 lints a page for storage,
`fetch(`/XHR, outside `src`/`href` URLs, forms and `target="_blank"`, warns the viewer and the
broker log, and stores it anyway. Text is shown as typed (the channel declares no rich text). A
reply without `reply_to` starting `📨 Held`, `⏸ Permission` or `⚠️` is only a notice in the shell; an
answer carrying `reply_to` is always logged. The page reaches C3 only through the `window.c3` API, inlined as its first script.
An agent sends a page with `text=""`: a reply with text and a file splits, and `reply_to` rides
only the first part. After an approve, its `reply_to` is the approve line's own id (413 above),
not the approved message's (418).

## URLs

| Route | What it is |
|---|---|
| `GET /board` | this broker's boards |
| `GET /board/<name>` | the shell: the page's frame filling the window, C3's status circle over its top-right corner, and C3's chat sheet when a part's chat is open |
| `GET /board/<name>/state` | title, rev, frame capability, viewer, live, holder, waiting, closed, last 500 log entries |
| `GET /board/<name>/page?cap=…` | the page with the SDK inlined; `&rev=N&raw=1` gives a stored revision as `text/plain` |
| `GET /board/<name>/events` | SSE `message`, `edit`, `reaction`, `action`, `page`, `status`, `notice`; `id:` is the log seq, `Last-Event-ID` replays after it |
| `POST /board/<name>/act` | `{client_id, op, …}`, op `comment`, `approve`, `tap` or `react`; or a voice act (below); 202 `{id}` |
| `POST /board/<name>/file` | the owner's file from C3's chat as the body (at most 20 MB), with headers `X-Client-Id`, `X-Board-Part`, `X-File-Name` and optionally `X-Caption` (both percent-encoded UTF-8; the caption at most 6 KB as sent, since a fronting proxy refuses large headers) and `X-Reply-To`; an act's checks and `client_id` retry (a reused id with other bytes is 409 before anything is kept); 202 `{id}` |
| `POST /board/<name>/close` | empty body; 204, or 500 `close_failed` (repeat to retry) |
| `POST /board/<name>/speak` | `{id}`: the session's message `id` on this board, read aloud; 200 `{token}` (cached audio first), else 404, 429 `rate`, 503 `unavailable` |
| `GET /board/<name>/audio/<token>` | that audio as MP3, under this board only; `…/audio/unlock` is C3's silent clip |

Shell assets are under `/board/assets/`. Errors are `{"error": <rule>}`: `invalid` 400,
`unauthorized` 401, `forbidden` 403, `not_found` 404, `closed` or `conflict` 409, `too_big` 413,
`unreadable` 422, `rate` 429, `offline` 503 with `Retry-After`, `unavailable` 503 (no speech: none set
up, the provider failed or its queue is full).

## The chat and the circle

The page fills the window. C3's own state is a small circle over its top-right corner: its dot is the
session (green live, amber away or reconnecting, grey read-only, pulsing while the session types), and
its badge counts the agent's replies the owner hasn't seen; nothing else badges it. A tap opens its
panel: the board's title, the live session (`cli · folder`), typing, the parts with unread replies
(title and first line, newest first), **Talk to the agent**, the last notice, **Read replies aloud**,
**Make read-only**, and at its foot how many of the owner's actions wait for the session. While it is
open, a clear shell layer covers the page: a tap anywhere else shuts it and doesn't reach the page. A notice (a reply of that kind, a voice problem) shows a
few seconds under the circle, then fades; it never reaches the frame.

C3 draws each part's chat itself, as its web chat does: a sheet rising from the bottom (a panel at the
bottom right on a wide screen) with the part's title, a line from the page (its question, or what is
decided) and up to three quick answers, then the thread: the owner's lines and the agent's answers with
their times, edits and reactions, voice notes with their live status, files (📎 name · size, the
caption under it), a play button on each answer,
the page's own acts labelled "Sent by the page", and a button for a proposal (an answer whose last line is `[apply: <label>]`, while it is the agent's
latest word there) that approves it. Under it is the composer, as in Telegram: a paperclip, then a box
that has the mic while empty and turns it into Send as the owner types; Enter sends on a keyboard and
breaks the line on a touch screen. The paperclip opens the browser's own file picker: one file of any
type, at most 20 MB (what a Telegram bot can fetch, so the agent meets one ceiling on both routes; the
shell says so before sending a bigger one), with the box's text as its caption. Its row shows the
upload's progress, then its time; the shell never serves a file back. Any message in the thread can be
answered: a swipe right on touch (a long press stays the browser's, for selecting text), its Reply
button on hover with a mouse. A bar over
the composer shows its first line with ×, and the next comment, voice note or file answers it. A row
that answers another opens with that message's first line, and a tap there scrolls to it and flashes
it; the agent's answer to the line right before it needs none. The page
asks with `c3.chat(part, {title, ask, choices, quote})` and shuts it with `c3.chat(null)`; `c3.threads()`
opens it on a tap on a part, titled by the part's first heading. The page hears `c3.on("chat")`:
`{part}` while open, `{part: null}` once shut. An owner's tap in the page that asks for no chat shuts
it, as Escape does. The sheet stays open across a frame reload, and a page that answers an approve on
the open part shuts it, so the owner sees the change, unless the owner has a draft there. **Talk to the
agent** opens the board-level chat, the part `general`, which the page can't open (`invalid`). The page
can shut or switch any chat, the ones the owner opened from the panel included.

A reply is unread until its part's chat has shown for a second in a visible tab: each browser keeps a
read mark per part in the shell origin's storage (on its first visit, what is already on the board
counts as read), and a chat shut, switched or hidden sooner marks nothing. A chat that opens on unread
replies draws a quiet "Unread" line above the first and opens scrolled to it, else at the bottom; the
line stays put while the chat is open and moves the next time it opens. A tap on an
unread part in the panel opens its chat and tells the page `c3.on("reveal", ({part}) => …)`; a handler
that returns `true` shows the part itself, else C3's SDK scrolls the part's element to the top and
flashes it. The panel names a part by the title the page last gave its chat, else its markup's (a JSON
part's `title`, else its first heading), else its address.

A page that draws its own chat still can: it tells C3 with `c3.focus(part, {dock, hasText})` the part
the owner is on, where its Send sits (an element the SDK follows through layout, scroll and keyboard,
or a box in frame pixels) and whether the box has text, and while the box is empty C3 draws its mic in
that spot (`c3.focus(part)` alone puts it at the frame's bottom-right corner; never while C3's sheet is
open).

## Voice

A tap on the mic records. While it records, the box gives way to **Cancel**, the time and a wave that
moves with the owner's voice (the docked mic draws the same in a bar beside it), and the mic turns into
a send button; under 400 ms nothing is sent. A recording keeps the part it started for: its chat can't
be shut or switched until it ends. Recording never stops on its own:
every five minutes the shell sends what it has as a numbered part and records on. Once a part has
gone out, the recording always ends with a last part the session sees: Cancel sends a `cancelled`
line in place of the last part's audio, and a part C3 refuses (413, 422, …) goes out again as a
`refused` line. **Make read-only** first ends a recording this way and waits for every voice act on
its way, then closes the board. A board made read-only from another tab takes nothing more; the
session is told the board closed.

Each recording is a voice act: `POST /board/<name>/act` with the `audio/*` body (at most 12 MiB) and
headers `X-Client-Id`, `X-Board-Part`, and for a part of a longer recording `X-Voice-Segment` (1, 2,
…) with `X-Voice-More: 1` on all but the last, and optionally `X-Voice-Recording` (one id on every
part of a recording: printable ASCII, at most 128 bytes) and `X-Reply-To` (the message every part
answers). It gets an act's checks, rate and `client_id` retry
(a retry repeats the audio and these headers, else 409), then the web chat's ffmpeg transcode (422
`unreadable` when that fails), a second open-board check (409 `closed` if the board was made
read-only meanwhile) and the broker's STT. A part whose audio never reached C3 is the same request
with no body and `X-Voice-Unsent: cancelled` (the last part only) or `refused`. A voice act's log
entry `{op: "voice", part, segment, has_more}` is at once in the stream (an unsent part's carries
`status` `cancelled` or `refused` and needs nothing more); its outcome is an `edit` of that entry
with `status`: `heard` (text: the transcript), `slow` (still trying, after two minutes),
`no_speech` or `failed` (text: C3's notice). Only the broker sets these, through the board's own
voice-status method, never through a reply, and each act takes one final status (`slow` only before
it): whatever the session's reply says, it is the session's message. The shell tells the frame each
upload's own status (`sending`, `sent` with the entry's `id`, `not_sent` with the rule), and the SDK
passes all of it as events.

**When parts stop coming.** A tab can die mid-recording, or a part and its `refused` line can both
be refused, and the session would wait for good. So after a part that says more follows, C3 waits
one part length (five minutes) plus two minutes; each later part of the recording starts the wait
again, and its last part (or a `cancelled` or `refused` line saying it is the last) ends it. If no
part comes, the session reads `Voice note ended after part 3: no further part arrived.` under C3's
header, with its own `msg` id, and the log gets `{op: "voice_ended", part, segment: 3, from: "C3",
text}`, which the page shows. A part that still comes after that is delivered as usual. On a board
made read-only the line is only logged: its route is gone and the session was told the board
closed. Parts belong to a recording by board part, or by `X-Voice-Recording` when the shell sends
it. The wait lives in memory: after a broker restart an unfinished recording isn't closed by C3;
the shell's next part or the owner's next message moves it on.

**Spoken replies.** The shell reads the session's messages aloud; the frame can't, having no network.
Each answer in C3's chat has a play button, a filled disc as Telegram draws a voice note's: a play
triangle, a spinner while C3 makes the audio, two bars while it plays (a tap stops it). A page asks with `c3.speak(id)` (`c3.speak(null)` stops) and follows
`c3.on("speech", e => …)`: `{id, state, error}`, state `loading`, `playing`, `ended` or `failed`.
**Read replies aloud** in the circle's panel (off until turned on; the browser remembers it in the
shell's own storage) reads each new message as it arrives, in order; after a reconnect, only the newest
one missed. There is one player: a tap on a reply takes the place of what plays and waits, starting the
mic stops it, a reply that comes while the mic is on waits for the recording to end, and `c3.speak`
while recording is refused (`recording`). A phone plays sound only after a tap. In Chromium a tap in
the page counts for the shell too; otherwise (and before any tap) the reply waits, fails as `blocked`
for the page, and a notice asks for a tap on the circle, which plays C3's silent clip and then the reply.

The shell asks `POST /board/<name>/speak` and plays `/board/<name>/audio/<token>`. Synthesis is the
web chat's (its provider, its two-worker, eight-waiting bound and its 15-minute in-memory audio
cache), with each board's audio kept apart from the web chat's and other boards'. A reply C3 must
synthesize counts against 12 a minute per board; one already cached is free. A board made read-only
can still be heard.

## Trust

- **Frame.** The page loads by `src` into `<iframe sandbox="allow-scripts">` with the document CSP
  (`default-src 'none'`, inline script and style, `data:` images, fonts and media, no connections
  or forms, the sandbox repeated). Its origin is opaque: no cookie, storage, network, popup or top
  navigation.
- **Capability.** `/page` needs `cap = HMAC(key, board, viewer)`, minted by `/state` after sign-in,
  and no cookie: a frame-started reload sends none in Chromium (WebKit does). A cap opens only its
  own board, while its viewer is signed in, and only on the path (front or direct) that minted it.
  Only the shell reloads the frame.
- **Shell.** It runs only its own script file, pins `frame-src` to `<origin>/board/<name>/page`,
  sends `Cross-Origin-Opener-Policy: same-origin` (no `top.opener` for the page), refuses framing,
  takes messages only from its frame, writes agent strings only as text and keeps `notice` out of
  the frame. Its circle, panel, chat sheet, mic and recording bar sit over the frame, where the page
  can't draw or script them.
- **Chat.** The page gives the sheet only a part, a title (120 characters), one line (600) and up to
  three quick answers, each one C3 would take as a tap; longer text is cut, anything else is refused,
  and all of it is shown as text in the sheet's header, never as C3's own words. Sends, quick answers
  and approves from the sheet are the shell's acts, checked as the page's are.
- **Microphone.** Only the shell records: one `getUserMedia` call, reached only from a trusted tap
  on its own mic button; the shell draws the recording state and its part itself. The frame has no
  microphone (sandboxed, opaque origin, no `allow`). The page chooses when the sheet opens and where a
  docked mic sits, so a press counts only if it went down on that mic at least 500 ms after the mic
  appeared or last moved: a press that began earlier, or anywhere else, never starts or stops a
  recording. (A keyboard or assistive click needs the shell's focus on the mic, which the page can't
  give.) This doesn't stop a page that predicts where the owner will tap after a pause; the recording
  state and its Cancel then show at once. The page can only name a part, its Send's spot and
  whether its box has text, so its script can never hear the owner or send a voice act. A spot that
  isn't four finite numbers is refused; the mic is a 44–56 px circle centred on the spot, kept inside
  the frame's visible box with its row below the status circle, and the recording bar takes the
  roomier side of that row, so no spot hides the circle, the mic or the recording state.
- **Speech.** Only the shell plays, on its own `<audio>`; the page names only a message id, a positive
  integer. C3 speaks it only when it is the session's message on that board (its latest edit), never
  an owner's act, a page or another board's message, and a refused id costs no synthesis. The speak
  route checks sign-in (the front included), same origin and board like an act. The shell plays only
  a 32-hex token C3 gave it, under its own board's path, or the unlock clip; a token plays only under
  the board that asked for it, never under another board or the web chat's `/audio/`. The page's
  script can ask for any of the session's messages without a tap, within the board's 12 a minute.
- **Acts.** Each re-checks sign-in, same origin and board; the body is decoded strictly per op and
  can't name a sender. Part `^[A-Za-z0-9._:-]{1,64}$`, text and note 16,000 runes, quote 2,000,
  tap value 64 bytes, 30 new acts a minute per board; an approve's, tap's or reaction's `ref` must
  name a session message, the owner's reply's any message on the board, and the page's comment takes
  none; a `client_id` retry is delivered once
  (not across a broker restart).
- **Files.** Only the shell sends one: the owner's pick in the browser's own picker, opened by a real
  tap on its paperclip. The page has no file call, `/act` takes no file op, and the frame has no
  network. The name is shown as text only, clipped to 120 characters with controls and path separators
  replaced; C3 stores the bytes by an id from their contents, never by the name.
- **Permissions.** A prompt's preview never reaches a board; a boards-only session gets
  "⏸ Permission prompt waiting at the laptop." as a notice. Board input can't settle a prompt.
- **A board yes.** The page's script can send any act, Approve included, without a tap, but C3 marks it
  as the page's to the session and in C3's chat. It reaches only the session that wrote the script,
  whose rule is that board input is the owner's word for that work, never for a push, deploy, release,
  message to someone else or permission answer.
  The script can draw misleading UI in its frame; the status circle's panel names the live session.

## Storage

`boards/` under the web state directory (`$XDG_STATE_HOME/c3/web`, else `~/.local/state/c3/web`),
directories 0700, files 0600: `capability.key` (the cap key) and per board `board.json` (number,
owner, title, rev, closed), `pages/<rev>.html` (the last 5; `rev` picks the served one) and
`log.jsonl` (append-only: seq, at, dir, id, op, part, text, ref, reply_to, from, for voice
segment, has_more, recording, status, and for a file name, mime, size). Held actions sit in the broker's queue.
Files the owner sends are `board-files/<id>` under the web state directory (the id from their
contents), the newest 200 kept as voice recordings are; their log entries stay, and the log never
holds the id. The upload streams into a temporary file there, which is renamed into place. Voice recordings are the web
chat's: `voice/` under the web state directory, the newest 200 kept for both. Spoken replies' audio
is kept in memory only; **Read replies aloud** and the read marks are the browser's, in the shell origin's storage. An unreadable `board.json` stops the board channel from starting.

## The authenticated HTTPS front

A phone off the tailnet reaches boards and the web chat through an authenticated HTTPS front: a
proxy that signs the owner in, forwards requests to the web listener with the browser's `Host`, and
names the signed-in identity in a header. One sign-in there covers both, with no C3 login, when the
front runs on another host. In the web stanza (all four keys, once any is set):

```json
"front": {"origin": "https://boards.example.com", "from": "192.0.2.10",
          "identity_header": "X-Front-User", "operator": "owner@example.com"}
```

A request whose `Host` is the front's (in any case, with or without one trailing dot, an IP
literal in any form, the port compared as a number with an empty one meaning 443; a port that isn't
a number counts too) is the operator only when its TCP peer is `from` and the header holds exactly
one value, equal to `operator`. C3's own login never counts on the front, and the header never
counts off it. What the front reaches depends on `from`:

- **`from` on another host** (not loopback and not one of this host's own addresses, which the web
  channel reads from the host's interfaces when it starts): the operator reaches every board and web
  chat route; anyone else gets the same `403 {"error":"forbidden"}` on every path. On the front,
  `GET /login` and `GET /auth` go to `/`, and C3's token exchange, fresh-link endpoint, logout and
  `/healthz` are 404; the Telegram login link opens the web chat directly, never through the front
  ([WEB.md](WEB.md#through-an-authenticated-https-front)).
- **A local `from`** (loopback or this host's own address, checked on every front request since an
  address can come up after start; an unreadable interface list counts too, and the web channel logs
  at start when the front serves boards only): the front reaches only `/board` and
  `/board/*` (else 404), so the web chat stays off it. Such a `from` can't tell the front from any
  process on this host, the owner's own browser included.

A request from the front's address under any other `Host` gets 404 (unless `from` is loopback). So
the front must pass the browser's `Host` unchanged: under any other its requests are not the
front's (404, or with a loopback `from` the direct web chat, which wants C3's own login). A health
check from the front can ask `/board` under the front's name: 403 without the operator's identity
means C3 is up. C3 trusts anything that can send from `from` as the front, so run the front where
nothing else can reach the web listener, or fence the listener's port to the front's own user
there. C3 can't end a stream the front keeps open after the owner signs out there; the front closes
it. With a front set, board links use its origin, and when the front serves the web chat the attach
result names it too. `c3-broker status` does not print `front`.

## Residual leaks

These escape the frame's CSP in shipping engines (seen in C3's containment probe, except DNS
prefetch, which it could not observe):
- `<link rel=preconnect>` reaches any host in Chromium and WebKit; DNS prefetch is the same class.
- Chromium opens an empty TCP connection for a frame navigation that `frame-src` then blocks; for
  a named host the DNS lookup carries the name.
- WebRTC: Chromium gathers candidates and sends STUN to any host. Safari is untested.

So a page could leak what it holds plus the viewer's IP address. Today only the owner views a
board, and a page holds only what its agent already has; this must be closed before others view.

**Not yet:** others watching (and, if the owner allows, typing), a C3-drawn Approve the
page can't press, boards for CLIs whose host turns C3's tools off, handing a board to a new
conversation, page history past 500 entries, a list across machines.
