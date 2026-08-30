# Web channel

The phase-1 `web` channel is a small, private text chat served by the broker. It
runs beside Telegram, gives the configured operator one browser conversation,
and drives the CLI session that has claimed the `(web, operator user id, no
topic)` route. It has no external resources or build step.

## Configure it

Install a broker version that contains the web resolver **before** adding the
`channels.web` stanza. An older broker sees the extra channel but cannot select
it and can refuse attaches.

Keep the Telegram stanza as the identity and login-delivery source of truth:

```json
{
  "schema_version": 1,
  "channels": {
    "telegram": {
      "bot_token": "...",
      "master_user_id": 123456789,
      "dm_chat_id": 123456789
    },
    "web": {
      "enabled": true,
      "listen": "127.0.0.1:8371",
      "public_url": "https://your-device.your-tailnet.ts.net"
    }
  },
  "allowlist": {"users": [123456789]}
}
```

`enabled` defaults to `true`. `listen` defaults to `127.0.0.1:8371`; C3 never
defaults to all interfaces. `public_url` is optional and must be one exact HTTPS
origin with no path, query, fragment, or user information. Web refuses to start
unless Telegram is already registered and `master_user_id` is nonzero and in
`allowlist.users`. Only `enabled`, `listen`, and `public_url` are legal in the
web stanza. `c3-broker status` prints the configured listener and public URL.

## Attach and use it

Run the attach tool with the exact selector:

```text
attach web
```

That releases the session's prior Telegram claim and claims the web route. To
return to a Telegram route, use `attach telegram` followed by the normal DM,
name, or topic-id selection. Because the exact words `web` and `telegram` are
selectors, a Telegram topic literally named either word must be selected with
structured `name=web` / `name=telegram` or by id.

The web route uses **reply-tool mode**: the agent's `reply` tool lands on the
claimed web route even if its host UI describes that mode as “Telegram.” A
session still drives only one route at a time. Messages arriving on the route it
released are durably held until a session claims it again.

Phase 1 is drive-only. Web has no Allow/Deny buttons and cannot answer `ask`.
Permission prompts and questions must be answered at the laptop, so an on-the-go
drive should use permissions that were deliberately pre-approved.

## Sign-in flow

1. After a successful `attach web`, the broker checks for a live browser
   session. If none exists, it creates a 10-minute, single-use login link and
   sends it to the configured Telegram operator DM with previews disabled.
2. The link opens `/auth`; its secret is after `#`, so it never reaches the
   server in the request URL, logs, referrer, or a preview fetch.
3. If the page opened inside Telegram, choose **Open in browser** first. In-app
   browsers commonly use a separate cookie jar. Then tap **Continue**.
4. Continue exchanges the secret with a same-origin POST, clears it from the
   address bar, and stores an HttpOnly, SameSite=Lax session cookie.

If the link is old, used, or unavailable, the login page's **Send me a fresh
link** button asks the broker to send another link to the configured operator.
It never accepts a user id from the browser. The endpoint allows one successful
request per minute and five per hour. The local command `c3-broker web link`
also asks the running broker to mint and DM a link; it never prints the secret.

## Phone access with Tailscale Serve

Keep C3 bound to loopback and let Tailscale terminate HTTPS on the same device:

```sh
tailscale serve --bg http://127.0.0.1:8371
tailscale serve status
```

Use the HTTPS `*.ts.net` origin reported by Serve as `channels.web.public_url`,
reload/restart C3, and keep Funnel off. This makes the browser surface available
to devices in the maintainer's tailnet without creating a public listener. If
the local Tailscale version prints different Serve syntax, follow its displayed
command help while preserving the same topology: tailnet HTTPS to the broker's
loopback HTTP listener.

## Security model (T1–T12)

1. The send body contains only `text` and `client_id`. C3 rejects extra JSON and
   stamps channel, route, operator, message kind, version, and time itself before
   running the default-deny inbound gate.
2. Login secrets are 32 random bytes, base64url encoded, expire after ten
   minutes, and are consumed atomically once. Failed exchanges do not burn a
   different valid link and are paced per remote address.
3. The secret is carried in a URL fragment. `GET /auth` is side-effect-free;
   only the operator's Continue POST can consume it.
4. Browser sessions are random, in memory, HttpOnly, SameSite=Lax, and idle out
   after 24 hours. Secure is set for TLS or an HTTPS public URL.
5. Authentication, send, logout, and fresh-link POSTs require same-origin
   evidence. SSE requires the cookie and refuses an explicitly foreign Origin.
   No route sends CORS headers.
6. The default listener is loopback. HTTP headers and idle connections have
   timeouts, POST bodies are capped at 64 KiB, and SSE has no server-wide write
   timeout. `/healthz` reveals only that the listener exists.
7. The fresh-link endpoint always targets the configured operator, is debounced
   to one per minute and five per hour, and labels the Telegram DM as requested
   from the login page.
8. A successful web attach mints at most one link during the attach debounce;
   two near-simultaneous claims do not produce a DM flood.
9. Used, expired, and unknown login secrets return the same denial copy. No
   secret is put in a query, server-rendered page, referrer, or log line.
10. Web never reads the Telegram token, poller, or offset store. Telegram is
    registered first and transports cannot replace another registration.
11. Outbound replies inherit the broker's structural claimed-route check; a
    tool cannot override the destination with supplied arguments.
12. The cookie is an **agent-driving credential**: a holder can prompt a CLI
    whose tools act on the laptop. The surface is tailnet-private by design, not
    a public chat service.

## Transport behavior and limitations

On a physical keyboard, Enter sends and Shift+Enter inserts a line break; on
touch devices Enter keeps inserting a newline and the **Send** button sends.

Browser input is accepted synchronously by `POST /send`. A `202` means “sent”:
the broker accepted it onto the route worker queue, not that it is already on
disk. A broker crash in the short interval before the worker appends can lose
it. A `503` leaves the same browser message in retry state; retries reuse the
same `client_id` and message id so accepted input is not emitted twice.

Replies use SSE with a 20-second heartbeat. The browser reconnects and replays
the last 200 reply/edit events held in memory. Typing and status notices are not
replayed. If the requested point is older than that ring, or the broker restarted
and the ring is empty, the page says **history may be incomplete**.

Other phase-1 limits: browser sessions and reply history disappear on broker
restart; there is one web conversation per operator; text only (no media,
polls, reactions, buttons, or remote permission verdicts); and a CLI session
can claim only one Telegram or web route at once.

## Phone verification checklist

- Confirm `c3-broker status` shows web enabled, loopback listener, and the exact
  `*.ts.net` public URL.
- On mobile data, open the tailnet URL and request a fresh link.
- Tap the DM link once inside Telegram, then repeat with **Open in browser**;
  confirm the normal browser retains the session and the used link is denied.
- Confirm the page says connected and replies arrive incrementally over SSE.
- Send text and verify the attached CLI receives one `<channel>` turn; force one
  retry and verify it keeps one message id.
- Trigger typing, an edit, a held notice, and a permission notice; verify their
  distinct page treatments and that permission still waits at the laptop.
- Reconnect with an old Last-Event-ID and verify **history may be incomplete**.
- Logout and verify the stream closes and the next visit returns to login.
- Finally, reattach the CLI to its Telegram topic and complete a normal Telegram
  round trip.
