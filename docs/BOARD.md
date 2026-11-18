# Live boards

A board is one HTML page that a CLI session writes and C3 serves on the [web channel](WEB.md)'s
listener. The owner taps a part, types, reads the answer in that part's thread and approves it;
the session changes that part and C3 redraws only it. Agents learn the rules from the plugin
skill (`plugins/c3/skills/board/SKILL.md`) and from each board's attach result.

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
- While the session is gone, the owner's actions queue and the strip says how many wait. Resume
  and broker restart restore the board from the session's record (30 days, with
  `auto_attach_on_resume` on, the default); else its session re-holds it with `attach`. An
  in-place conversation switch leaves it waiting for its own conversation.
- **Close** in the strip makes the board read-only for good: C3 releases the route, drops it from
  the session's record and tells the session; later actions get 409.

## What each side sees

The session gets one line per action, drawn by C3 from checked fields, each with its own id:

```text
[board plan · 1.5 · msg 412] make this shorter
[board plan · 1.5 · msg 413] Approved your message 418.
[board plan · 1.5 · msg 414] Tapped "pick-b" on your message 418.
[board plan · 1.5 · msg 415] Reacted 👍 to your message 418.
```

A quote comes first as `> ` lines; a ref also arrives as the reply context. Owner text with a line
starting `[board ` is refused, invisible characters ignored.

The session sends text (shown in the part its `reply_to` answers), edits of its own messages,
reactions, and pages: one regular `.html`/`.htm` file of at most 5 MiB per reply, stored as the
next revision. Buttons, polls and other media are not taken. C3 lints a page for storage,
`fetch(`/XHR, outside `src`/`href` URLs, forms and `target="_blank"`, warns the viewer and the
broker log, and stores it anyway. Text is shown as typed (the channel declares no rich text). A
reply without `reply_to` starting `📨 Held`, `⏸ Permission` or `⚠️` goes to the strip only; an
answer carrying `reply_to` is always logged. The page reaches C3 only through the `window.c3` API, inlined as its first script.
An agent sends a page with `text=""`: a reply with text and a file splits, and `reply_to` rides
only the first part. After an approve, its `reply_to` is the approve line's own id (413 above),
not the approved message's (418).

## URLs

| Route | What it is |
|---|---|
| `GET /board` | this broker's boards |
| `GET /board/<name>` | the shell: a strip (title, live session, waiting, typing, Close) over the page's frame |
| `GET /board/<name>/state` | title, rev, frame capability, viewer, live, holder, waiting, closed, last 500 log entries |
| `GET /board/<name>/page?cap=…` | the page with the SDK inlined; `&rev=N&raw=1` gives a stored revision as `text/plain` |
| `GET /board/<name>/events` | SSE `message`, `edit`, `reaction`, `action`, `page`, `status`, `notice`; `id:` is the log seq, `Last-Event-ID` replays after it |
| `POST /board/<name>/act` | `{client_id, op, …}`, op `comment`, `approve`, `tap` or `react`; 202 `{id}` |
| `POST /board/<name>/close` | empty body; 204, or 500 `close_failed` (repeat to retry) |

Shell assets are under `/board/assets/`. Errors are `{"error": <rule>}`: `invalid` 400,
`unauthorized` 401, `forbidden` 403, `not_found` 404, `closed` or `conflict` 409, `too_big` 413,
`rate` 429, `offline` 503 with `Retry-After`.

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
  the frame.
- **Acts.** Each re-checks sign-in, same origin and board; the body is decoded strictly per op and
  can't name a sender. Part `^[A-Za-z0-9._:-]{1,64}$`, text and note 16,000 runes, quote 2,000,
  tap value 64 bytes, 30 new acts a minute per board; a `ref` must name a session message; a
  `client_id` retry is delivered once (not across a broker restart).
- **Permissions.** A prompt's preview never reaches a board; a boards-only session gets
  "⏸ Permission prompt waiting at the laptop." in the strip. Board input can't settle a prompt.
- **A board yes.** The page's script can send any act, Approve included, without a tap. It reaches
  only the session that wrote the script, whose rule is that board input is the owner's word for
  that work, never for a push, deploy, release, message to someone else or permission answer.
  The script can draw misleading UI in its frame; the strip names the live session.

## Storage

`boards/` under the web state directory (`$XDG_STATE_HOME/c3/web`, else `~/.local/state/c3/web`),
directories 0700, files 0600: `capability.key` (the cap key) and per board `board.json` (number,
owner, title, rev, closed), `pages/<rev>.html` (the last 5; `rev` picks the served one) and
`log.jsonl` (append-only: seq, at, dir, id, op, part, text, ref, reply_to, from). Held actions
sit in the broker's queue. An unreadable `board.json` stops the board channel from starting.

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
- **A local `from`** (loopback or this host's own address; an unreadable interface list counts too,
  and the web channel logs that the front serves boards only): the front reaches only `/board` and
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

**Not yet:** others watching (and, if the owner allows, typing), voice, a C3-drawn Approve the
page can't press, boards for CLIs whose host turns C3's tools off, handing a board to a new
conversation, page history past 500 entries, a list across machines.
