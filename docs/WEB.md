# Web channel

The `web` channel is a small, private chat served by the broker. It
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
      "listen": "100.100.10.20:8371",
      "public_url": "https://100.100.10.20:8371",
      "tls": true
    }
  },
  "allowlist": {"users": [123456789]}
}
```

`enabled` defaults to `true`. `listen` defaults to `127.0.0.1:8371`; C3 never
defaults to all interfaces. `public_url` must be one exact HTTPS origin with no
path, query, fragment, or user information when `tls` is enabled. A TLS listener
may bind only loopback or a Tailscale address (`100.64.0.0/10` or
`fd7a:115c:a1e0::/48`). Web refuses to start unless Telegram is already
registered and `master_user_id` is nonzero and in `allowlist.users`. Only
`enabled`, `listen`, `public_url`, and `tls` are legal in the web stanza.
`c3-broker status` prints them and the live CA fingerprint when TLS is on.

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

## On-the-go mode

Say **“start on-the-go mode”** or **“switch to the web chat”**
(or run `/c3:on-the-go`). The agent treats that as an explicit output-mode
request: it remembers the Telegram topic it currently holds, attaches the
`web` route, switches to reply-tool (“Telegram”) mode so replies land in the
browser, and announces the switch in one line. The attach response sends or
confirms the Telegram DM login link as usual.

When **🔊 Voice** is turned on or off, the web chat sends the agent a system
notice saying **“Spoken replies ON”** or **“Spoken replies OFF.”** While it is
on, the agent follows the channel's spoken-reply guidance and writes short,
speakable prose; when it is off, normal rich-text guidance applies.

Say **“end on-the-go mode,” “back to Telegram,”** or **“back to the topic”**
(or run `/c3:off-the-go`) to re-attach the Telegram topic held before the
switch. If the agent no longer knows that topic, it asks instead of guessing,
then announces the restored route and output mode. Permission prompts and
`ask` remain laptop-side throughout on-the-go mode.

## Sign-in flow

1. After a successful `attach web`, the broker checks for a live browser
   session. If none exists, it creates a 10-minute, single-use login link and
   sends it to the configured Telegram operator DM with previews disabled.
2. The link opens `/auth`; its secret is after `#`, so it never reaches the
   server in the request URL, logs, referrer, or a preview fetch.
3. Choose **Open in browser** from Telegram. In-app browsers commonly use a
   separate cookie jar and do not trust user-installed CAs. Then tap **Continue**.
4. Continue exchanges the secret with a same-origin POST, clears it from the
   address bar, and stores an HttpOnly, SameSite=Lax session cookie.

If the link is old, used, or unavailable, the login page's **Send me a fresh
link** button asks the broker to send another link to the configured operator.
It never accepts a user id from the browser. The endpoint allows one successful
request per minute and five per hour. The local command `c3-broker web link`
also asks the running broker to mint and DM a link; it never prints the secret.

## Phone access over the tailnet (private CA)

1. Set the tailnet `listen` address, matching HTTPS `public_url`, and `tls: true`.
2. Restart the broker; confirm `c3-broker status` shows `tls=true` and `ca_sha256`.
3. Run `c3-broker web ca` (or download unauthenticated `GET /ca.crt`).
4. Android: open the file and install it as a CA certificate; the monitoring notice is expected.
5. iPhone: install the profile, then enable full trust under Certificate Trust Settings.
6. Open the Telegram magic link with **Open in browser**; in-app browsers do not trust user CAs.
7. Open `public_url` and expect a normal padlock with no certificate interstitial.
8. If the tailnet IP changes, update `listen` and `public_url`, then restart; the leaf reissues and the CA stays.

Tailnets that issue certificates can instead keep C3 on loopback and use
Tailscale Serve as the HTTPS terminator. Set Serve's `*.ts.net` origin as
`public_url`, leave `tls` off, and keep Funnel off.

## Install as an app

Sign in first, open `/`, then install from that page so the app's start URL is
the chat root. On Android, use Chrome's menu and choose **Add to Home screen**.
On iOS, use Safari's **Share** menu and choose **Add to Home Screen**. iOS
ignores the SVG app icon, so C3 also serves the required 180×180 PNG touch icon.

The installed app has its own cookie jar. Open a fresh Telegram magic link
inside the installed app and sign in there again; signing in in an ordinary
browser tab does not authenticate the installed copy. While Voice or
Hands-free is on, C3 requests a screen wake lock and re-acquires it after the
page becomes visible or the browser releases it. Unsupported browsers simply
continue without a wake lock.

The v1 mobile boundary is foreground-only: keep the installed app visible and
the phone screen on, in a cradle or on loudspeaker. The wake lock supports that
pattern but does not make iOS background audio work; iOS suspends microphone
and Web Audio capture when the app is backgrounded or the screen locks.

## Security model (T1–T18)

1. The send body contains only `text` and `client_id`. C3 rejects extra JSON and
   stamps channel, route, operator, message kind, version, and time itself before
   running the default-deny inbound gate.
2. Login secrets are 32 random bytes, base64url encoded, expire after ten
   minutes, and are consumed atomically once. Failed exchanges do not burn a
   different valid link and are paced per remote address.
3. The secret is carried in a URL fragment. `GET /auth` is side-effect-free;
   only the operator's Continue POST can consume it.
4. Browser sessions use random HttpOnly, SameSite=Lax cookies and idle out after
   24 hours. Only each cookie's SHA-256 hash is persisted; the raw credential
   exists only in the browser. The cookie lasts for the same idle window across
   browser restarts. Secure is set for TLS or an HTTPS public URL.
5. Authentication, send, logout, and fresh-link POSTs require same-origin
   evidence. SSE requires the cookie and refuses an explicitly foreign Origin.
   No route sends CORS headers.
6. The default listener is loopback. HTTP headers and idle connections have
   timeouts, ordinary POST bodies are capped at 64 KiB, voice-note bodies at
   12 MiB, and SSE has no server-wide write timeout. `/healthz` reveals only
   that the listener exists.
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
    tool cannot override the destination with supplied arguments. The trusted
    voice-preference system notice is channel-authored after an authenticated
    same-origin change and bypasses `GateInbound`, matching the broker's own
    `broadcastSystemEvent`; browser-authored messages still pass the gate.
12. The cookie is an **agent-driving credential**: a holder can prompt a CLI
    whose tools act on the laptop. Copying the web state files does not supply
    that credential. The surface is tailnet-private by design, not a public
    chat service.
13. The private CA and leaf keys are ECDSA P-256 PKCS#8 files, mode 0600, under
    `$XDG_STATE_HOME/c3/web/` (or `~/.local/state/c3/web/`); no private key is
    logged, served, or sent through Telegram.
14. IP literals are encoded as `iPAddress` SANs. The leaf also covers
    `localhost`, both loopback IPs, the configured listener and public hosts,
    and the machine hostname.
15. C3 creates the CA only when both CA files are absent. A missing, corrupt, or
    mismatched half is a startup refusal, never an automatic trust-anchor swap.
16. With `tls` enabled every listener, including the loopback twin, is HTTPS;
    C3 never leaves a plaintext listener beside it.
17. C3 sends no HSTS header. A stale private-IP HSTS entry could strand the
    phone after an address or certificate change.
18. TLS refuses empty-host, `0.0.0.0`, and `::` listeners; the agent-driving
    surface is never bound to every interface.

## Transport behavior and limitations

On a physical keyboard, Enter sends and Shift+Enter inserts a line break; on
touch devices Enter keeps inserting a newline and the **Send** button sends.

Browser input is accepted synchronously by `POST /send`. A `202` means “sent”:
the broker accepted it onto the route worker queue, not that it is already on
disk. A broker crash in the short interval before the worker appends can lose
it. A `503` leaves the same browser message in retry state; retries reuse the
same `client_id` and message id so accepted input is not emitted twice.

Replies use SSE with a 20-second heartbeat. The browser reconnects and replays
the last 200 sequenced conversation events: agent replies and edits plus
accepted operator-message echoes. If its `Last-Event-ID` is older than that
ring, the page says **history may be incomplete**. Reply ids and SSE sequence
ids remain monotonic across broker restarts. Typing and status notices are live
only and are not replayed.

Agent replies and edits render a safe Markdown subset in the browser: fenced
and inline code, bold, italic, strike, click-to-reveal `||spoilers||`, links and
bare HTTP(S) URLs, headings, one-level ordered and unordered lists,
blockquotes, horizontal rules, paragraphs/line breaks, and GFM tables. The web
channel advertises both `RichText` and `RichTables`; tables render in a
horizontal-scroll wrapper. Link targets are created only for
`http:`, `https:`, `mailto:`, and `tel:` schemes. Operator messages and status
notices remain literal text with line breaks.

## Voice

The microphone button is a push-to-talk toggle: tap once to record and again
to stop. Recordings stop automatically after five minutes; clips shorter than
400 ms are discarded, and uploads over 12 MiB are refused. Chrome and Android
normally produce WebM/Opus, while iOS commonly produces `audio/mp4` with AAC.
The channel accepts browser `audio/*` input and transcodes it at the HTTP edge
with ffmpeg to 48 kHz mono OGG/Opus before it enters the existing STT chain.
The original browser file is deleted after conversion. After origin and
authentication, `/voice-note` checks content type, size, then client-id
idempotency; a conflicting client id sent with a bad content type therefore
returns 415 rather than 409.

Converted voice notes live under the web state directory in `voice/`, mode
0600, with the newest 200 retained. Their channel-minted file ids continue to
work with `retranscribe` while retained. The optimistic `🎤 Voice note` row is
persisted like other operator rows; the transcript (or a couldn't-transcribe
notice) returns as a persisted edit underneath that label. Microphone capture
requires a secure context, so use the private-CA HTTPS link or loopback
`http://127.0.0.1`; the page hides the mic and explains the requirement on an
insecure origin.

The header's 🔊/🔇 control enables spoken agent replies for that browser
session. The enabling tap first plays the same-origin `/audio/unlock` silence
clip to satisfy mobile autoplay rules, then persists the preference. Synthesis
can incur provider cost and runs only while at least one web session has spoken
replies enabled and a connected SSE stream. Web leaves the speech language
automatic; the web mapping accepts no voice-language key.

Replies play in arrival order through one reusable player. **Stop** ends the
current queue and **Replay** repeats the last reply. Browser media-session
controls expose play, pause, stop, replay-last, and replay-previous actions on
supported lock screens. Synthesized MP3 is memory-only: audio URLs expire after
15 minutes, are not written to replay history or `sessions.json`, and are not
replayed after an SSE reconnect. Text delivery never waits for or fails with
TTS; a provider failure produces at most one short live notice per minute.

## Hands-free

The header's **🎙 Hands-free** button turns on Voice, unlocks playback in the
same tap, asks for microphone permission once, and then holds that microphone
open until Hands-free is turned off. A text-and-icon pill shows the complete
turn loop: **listening → recording → sending → thinking → speaking →
listening**. Turning Hands-free off stops capture and playback and releases the
microphone. Moving the app to the background pauses voice detection; if that
happens during a recording, the page finishes and uploads the utterance first.

| State | Entry action | Exits |
| --- | --- | --- |
| `idle` | Hands-free is off and the microphone is released. | Enabling Hands-free opens the microphone and enters `listening`, or `speaking` if a reply is already playing or queued. |
| `listening` | VAD follows the ambient floor and arms capture. Tracking for a recent reply is retained for its 90-second late-audio window. | Speech enters `recording`; matching late audio enters `speaking`; disabling enters `idle`; reconnecting with an unanswered inbound id returns to `thinking`. |
| `recording` | A recorder captures the continuously delayed microphone stream. | Hangover, the 60-second cap, or hiding the page stops after 300 ms and enters `sending`; a short utterance returns to `listening`. |
| `sending` | The captured blob uploads with its client id. All attempts share a 60-second deadline. | Acceptance records the inbound message id and enters `thinking`; failure or deadline expiry marks the bubble **not sent**, sounds the error cue, and enters `listening`. Push-to-talk uploads retain their normal retry policy. |
| `thinking` | The 90-second reply cap runs. The first live, non-replayed agent message with no `reply_to` or one matching the inbound id becomes the active reply; a different non-zero `reply_to` is ignored. Its 10-second audio wait is refreshed by typing. Transient SSE errors retain both ids; a reconnect restores this state while the inbound id is still awaited. | Audio for the active reply enters `speaking`; the audio wait, reply cap, or a closed SSE stream enters `listening`; disabling enters `idle`. `own`, `status`, `typing`, replayed, and unrelated message events do not select a reply. |
| `speaking` | Reply audio plays while VAD calibrates for barge-in. | Playback completion enters `listening`; confirmed speech enters `recording`; disabling enters `idle`. |

The dependency-free VAD samples the microphone every 50 ms. It removes each
block's mean, then estimates speech-band RMS by scaling that time-domain RMS by
the square root of the 300–3400 Hz fraction of FFT power. The thresholds
therefore remain calibrated in time-domain RMS units while rumble and
out-of-band hiss are suppressed. An adaptive ambient-noise floor is used.
Speech starts after two samples above
`max(noise × 2.5, 0.012)`, ends after 900 ms below
`max(noise × 1.8, 0.008)`, discards speech shorter than 280 ms, and caps an
utterance at 60 seconds. A continuously live 300 ms delay supplies pre-roll;
the recorder continues for another 300 ms after the end hangover. These
thresholds are tuned for a foregrounded phone in a cradle or on loudspeaker,
not a pocket or a screen-off session.

While a reply is speaking, VAD ignores its first 400 ms to sample a playback
floor from the microphone alone, then requires 400 ms above
`max(playback × 8, 0.07)` to barge in. Spoken replies stay on the browser's
ordinary `<audio>` output and are deliberately not captured into the Web Audio
graph: capturing a media element can silence it on WebKit/iOS. A successful
barge-in stops and marks the reply **interrupted**, rejects later audio chunks
for it, removes the first 200 ms of loudspeaker leakage from the next capture,
and starts recording. Microphone-only playback-floor calibration is
best-effort, especially on iOS, whose echo cancellation is weaker than
Chromium's. The armed cue is a short 880 Hz tone; captured/sending uses two
rising 660/880 Hz tones; errors use a low 220 Hz tone. Earcons are quiet, use a
5 ms attack ramp, and mute VAD for 200 ms after each cue finishes.

If road noise or loudspeaker leakage repeatedly starts capture at the wrong
time, turn Hands-free off and use the push-to-talk microphone button. Loss of
the microphone or denied permission is reported under the conversation; fix
the browser permission and tap Hands-free again.

Web state lives in `$XDG_STATE_HOME/c3/web/`, or
`~/.local/state/c3/web/` when `XDG_STATE_HOME` is unset. The directory is mode
0700. `sessions.json` stores hard floors for the next reply, inbound, and SSE
event ids alongside sessions, each session's spoken-reply preference, and
client-message outcomes. `sessions.json` and
the append-only `replay.jsonl` are mode 0600 and are
updated with fsync-backed atomic state writes or fsynced replay appends. Browser
sessions and their cookies survive broker and browser restarts for the remainder
of their 24-hour idle window, as do recent client-id outcomes and the replay
ring. Ten-minute login links, typing, and status notices do not survive restart.

When the broker holds a message because no CLI owns the web route, the page
changes its connection label to **no session attached**. A later reply, edit,
or typing event changes it back to **connected**.

The private CA must be installed once in each phone's normal browser trust
store; Telegram's in-app browser may still reject it. A changed tailnet IP
requires a config update and restart so the leaf SANs can be reissued, but does
not require reinstalling the CA.

Other current limits: there is one web conversation per operator; voice notes
are the only inbound media (no files, photos, polls, reactions, buttons, or
remote permission verdicts); and a CLI session can claim only one Telegram or
web route at once. Synthesized audio is intentionally transient rather than
conversation history. Two open tabs sharing one browser session both receive
the live audio event and play each spoken reply.

## Phone verification checklist

- Install the CA from `c3-broker web ca` in the phone's system CA trust store.
- Confirm its SHA-256 fingerprint matches `c3-broker status`, then expect a padlock.
- Confirm `c3-broker status` shows web enabled, `tls=true`, the tailnet listener,
  and the exact HTTPS public URL.
- On mobile data, open the tailnet URL and request a fresh link.
- Tap the DM link once inside Telegram, then repeat with **Open in browser**;
  confirm the normal browser retains the session and the used link is denied.
- Confirm the page says connected and replies arrive incrementally over SSE.
- Send text and verify the attached CLI receives one `<channel>` turn; force one
  retry and verify it keeps one message id.
- Grant microphone permission, record a voice note, and confirm its transcript
  edits the same `🎤` row. On iOS, verify the `audio/mp4` recorder path.
- Install from `/` after login on Android and iOS; open a fresh magic link
  inside the installed app and verify its separate cookie jar signs in.
- Enable spoken replies with one tap, hear a reply, then exercise Stop, Replay,
  and lock-screen media controls. Reconnect SSE and confirm old audio is not
  replayed.
- Enable Hands-free with one tap and complete a full
  listening→recording→sending→thinking→speaking turn with the phone foregrounded
  in a cradle. Verify the wake lock survives a visibility change and its own
  release event.
- Speak over TTS after its 400 ms calibration window; confirm playback is
  marked **interrupted**, no later chunk for that reply plays, and the new
  utterance keeps its opening word. Repeat on iOS and record any self-barge-in
  caused by weaker echo cancellation.
- Background the page while listening and confirm VAD pauses without releasing
  the mic; background it while recording and confirm the finished clip uploads.
  End the microphone track and confirm the loss message and idle state.
- Hold `/voice-note` requests in a network failure. Confirm a Hands-free bubble
  becomes **not sent**, sounds the error cue, and returns to listening 60 seconds
  after its first attempt; confirm push-to-talk continues its existing retries.
- Trigger typing, an edit, a held notice, and a permission notice; verify their
  distinct page treatments and that permission still waits at the laptop.
- Reconnect with an old Last-Event-ID and verify **history may be incomplete**.
- Logout and verify the stream closes and the next visit returns to login.
- Finally, reattach the CLI to its Telegram topic and complete a normal Telegram
  round trip.
