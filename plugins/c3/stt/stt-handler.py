#!/usr/bin/env python3
"""Voice STT handler for the Telegram channel.

Called by the Go-side STT plugin (internal/plugin/builtins/stt/stt.go) for
every incoming voice message:

    stdin (line 1):  <bot_token>\\n
    argv:            python3 stt-handler.py <chat_id> <reply_msg_id> <file_id> [<message_thread_id>]

The bot token is supplied on stdin — never via argv — so it doesn't appear
in `ps` / `/proc/<pid>/cmdline` / audit logs (addresses code-review
2026-05-15 MAJOR #1, cli.md §1.10). The Go shim writes `<token>\\n` to our
stdin before invoking us.

The optional <message_thread_id> is the forum topic the voice was sent in;
it is still accepted in argv for contract compatibility, but the handler no
longer sends anything to Telegram itself.

On success: prints the transcript to stdout (the Go shim reads it and the Go
broker/channel renders the readback echo back to Telegram — see
internal/channel/telegram/readback.go).
On failure: prints nothing + exits non-zero (the Go shim sees empty stdout,
surfaces an [STT FAILED] marker to the agent, and the broker sends the human
"couldn't transcribe" notice — see internal/broker/worker.go echoReadback).

This handler now does ONLY download + whisper + print-to-stdout; all Telegram
sending lives in Go (the "don't reinvent the wheel" move). The Go↔Python
contract is unchanged: token on stdin line 1, transcript on stdout, argv
<chat_id> <reply_msg_id> <file_id> [<message_thread_id>].
"""
import sys
import os
import json
import time
import urllib.request
import importlib.util
import logging

# ── Config ────────────────────────────────────────────────────────────────────

_HERE      = os.path.dirname(os.path.realpath(__file__))
STT_PKG    = os.path.join(_HERE, 'stt-pkg', 'stt.py')
ENV_FILE   = os.environ.get('STT_ENV_FILE',  os.path.expanduser('~/.claude/stt.env'))
INBOX_DIR  = os.environ.get('STT_INBOX_DIR', os.path.expanduser('~/.claude/channels/telegram/inbox'))
LOG_FILE   = os.environ.get('STT_LOG_FILE',  os.path.expanduser('~/.claude/channels/telegram/stt-handler.log'))

# TODO #12 (2026-05-16): on a fresh install ~/.claude/channels/telegram/
# doesn't exist, and logging.basicConfig(filename=...) does NOT create
# parent dirs — import would FileNotFoundError before any provider code
# ran, and the broker only surfaced "[STT FAILED: error]" (byte-identical
# with or without an API key, undiagnosable). Belt-and-suspenders: mkdir
# both parents up front, then try basicConfig with a stderr fallback so
# a read-only / no-perms FS still gets logs into broker.log (the broker
# captures stderr).
_LOG_DIR = os.path.dirname(LOG_FILE)
if _LOG_DIR:
    os.makedirs(_LOG_DIR, exist_ok=True)
os.makedirs(INBOX_DIR, exist_ok=True)

def _one_line(text):
    """Collapse text onto a single line so it can never start a new one.

    Anything reaching stderr may carry server- or provider-controlled bytes;
    a bare newline in it would let that text begin a line of its own."""
    return str(text).replace('\\', '\\\\').replace('\r', '\\r').replace('\n', '\\n')


class _SingleLineFormatter(logging.Formatter):
    """A Formatter whose every record occupies exactly one line."""

    def format(self, record):
        return _one_line(super().format(record))


_LOG_FORMAT  = '%(asctime)s %(levelname)s %(message)s'
_LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
try:
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.DEBUG,
        format=_LOG_FORMAT,
        datefmt=_LOG_DATEFMT,
    )
except Exception:
    # File handler couldn't be opened (read-only FS, perms, race, etc.).
    # Fall back to stderr — the broker pipes our stderr into broker.log,
    # so logs still land somewhere the operator can find.
    #
    # BELT (C3 review 4, R1): on this fallback our log records share the stream
    # the shim parses for fetch reports, and a record can carry server-controlled
    # text. So every record is forced onto ONE line with a non-marker prefix — it
    # can neither begin with the marker nor smuggle a newline that starts a new
    # line with it. The nonce is the actual guarantee; this removes the chance.
    _stderr_handler = logging.StreamHandler(sys.stderr)
    _stderr_handler.setFormatter(_SingleLineFormatter(
        '[stt-handler] ' + _LOG_FORMAT, datefmt=_LOG_DATEFMT))
    logging.basicConfig(level=logging.DEBUG, handlers=[_stderr_handler])

# ── Load API keys ─────────────────────────────────────────────────────────────

def load_env(path):
    env = {}
    try:
        with open(os.path.realpath(path)) as f:
            for line in f:
                line = line.strip()
                m = line.split('=', 1)
                if len(m) == 2 and m[0].isidentifier():
                    env[m[0]] = m[1]
    except Exception:
        pass
    return env

# ── Telegram API helpers ───────────────────────────────────────────────────────

# Bot-API base URL. Defaults to Telegram direct, but honors C3_TELEGRAM_API_URL
# (injected by the Go STT shim from mappings.json:channels.telegram.api_base_url
# or the env of the same name). This routes BOTH the getFile call and the voice
# file download through the same reverse proxy the broker uses — direct
# api.telegram.org is IP-blocked in some networks (e.g. India), which made the
# download time out (`<urlopen error timed out>`) even though the proxy was live.
API_BASE = os.environ.get('C3_TELEGRAM_API_URL', 'https://api.telegram.org').rstrip('/')

def tg(token, method, **params):
    url = f'{API_BASE}/bot{token}/{method}'
    data = json.dumps(params).encode()
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

# ── Fetch-error protocol (C3 review 3, finding 1) ─────────────────────────────
#
# The Go shim has to tell the broker EXACTLY why a fetch failed, and the broker
# refuses to render a specific server refusal as a generic "transcription
# failed". A human-readable stderr line cannot carry that: a server description
# is attacker-influenced text that can contain the delimiter, embedded newlines,
# or both — enough to fabricate a line, truncate itself, or make a non-fetch
# diagnostic look like a fetch failure.
#
# So the cause travels as ONE structured line: a unique marker ANCHORED AT LINE
# START, then the cause JSON-encoded. JSON escapes newlines, so the payload can
# never span lines or end early no matter what the server said. Nothing else is
# printed to stderr for a fetch failure — the readable form goes to the handler's
# own log — so there is no unencoded copy to inject through.
FETCH_ERROR_MARKER = 'C3-STT-FETCH-ERROR-v1 '


# The shim generates a fresh random nonce per invocation and passes it here. It
# is what makes a report PROVENANT rather than merely well-formed: stderr is a
# shared stream that also carries server descriptions (via the logging fallback)
# and provider HTTP bodies (copied from the STT subprocess), so a marker line
# alone proves nothing — a server can put one inside its own error text. It
# cannot put THIS run's nonce there, having never seen it.
FETCH_ERROR_NONCE = os.environ.get('C3_STT_FETCH_NONCE', '')


def emit_fetch_error(cause):
    """Report a fetch failure to the Go shim. Called FIRST on any fetch error,
    before anything else that could raise, so a later crash cannot swallow it."""
    try:
        payload = json.dumps({'nonce': FETCH_ERROR_NONCE, 'cause': str(cause)})
    except Exception:
        payload = json.dumps({'nonce': FETCH_ERROR_NONCE, 'cause': 'unprintable fetch error'})
    # Leading newline guarantees the marker starts a line even if earlier output
    # left the stream mid-line; the shim matches only at line start.
    sys.stderr.write('\n' + FETCH_ERROR_MARKER + payload + '\n')
    sys.stderr.flush()


class PermanentDownloadError(Exception):
    """A getFile/download failure that retrying cannot fix.

    Telegram returns {ok:false, description, error_code} for permanent
    conditions — an expired/invalid file_id, or the bot server refusing the
    file as too big. The server's own description is what gets reported; this
    code states no size limit of its own. Re-running getFile would fail
    identically, so the download loop treats this as terminal and does NOT
    burn the remaining retries (I-9)."""
    pass


def download_file(token, file_id, dest_path, tg_fn=None):
    tg_fn = tg_fn or tg
    result = tg_fn(token, 'getFile', file_id=file_id)
    # I-9: a getFile error comes back as {ok:false, description, error_code}
    # (NOT as an exception). Without this guard, dereferencing result['result']
    # raises a cryptic KeyError that the caller's generic `except Exception`
    # swallows and then retries 3× uselessly on a guaranteed-permanent failure.
    if not result.get('ok'):
        desc = result.get('description', '<no description>')
        code = result.get('error_code')
        raise PermanentDownloadError(f'getFile failed (error_code={code}): {desc}')
    file_path = result['result']['file_path']
    url = f'{API_BASE}/file/bot{token}/{file_path}'
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as r:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        # Atomic write: the STT budget can SIGKILL this process mid-download, and a
        # truncated .oga left at dest_path would be served as complete by the
        # broker's cache-first download_attachment (P1-4). Write to a temp sibling,
        # then os.replace (atomic on the same filesystem), so dest_path only ever
        # appears once it is whole. Match on the final "-<file_id>.oga" name, never
        # the ".part" temp, so a partial is never picked up.
        tmp_path = dest_path + '.part'
        try:
            with open(tmp_path, 'wb') as f:
                f.write(r.read())
            os.replace(tmp_path, dest_path)
        except BaseException:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise

# ── STT ───────────────────────────────────────────────────────────────────────

def _stderr_snippet(raw, limit=800):
    """Best-effort printable snippet of a subprocess's captured stderr, which
    may be None or — depending on Python version / text mode — bytes even when
    text=True was requested. Guard both so logging the reason never itself
    raises (I-2)."""
    if raw is None:
        return ''
    if isinstance(raw, bytes):
        raw = raw.decode('utf-8', 'replace')
    return raw.strip()[:limit]


def run_stt(audio_path, extra_env, timeout=270):
    """Dynamically load stt.py and run it. Returns transcript or None.

    timeout: subprocess budget in seconds. main() passes a value reduced by the
    time the download already consumed so download + transcribe stays under the
    broker's 300s SIGKILL (stt-pipeline-5)."""
    spec = importlib.util.spec_from_file_location('stt', STT_PKG)
    # We call the providers directly rather than spawning a subprocess,
    # so we need to inject keys into the environment first.
    for k, v in extra_env.items():
        os.environ.setdefault(k, v)

    # Use stt.py's own chain logic via its main internals
    import subprocess
    env = {**os.environ, **extra_env}
    # Tell the provider chain how much wall-clock it has so the Sarvam batch wait
    # can size itself to return gracefully BEFORE this subprocess.run timeout
    # SIGKILLs it (the provider caps wait at min(240, budget-15)). main() shrinks
    # `timeout` by the elapsed download time so a slow download can't push the
    # total past the broker's Go-side 300s context (the true backstop).
    env["C3_STT_BUDGET_SECONDS"] = str(timeout)
    # I-2: never let a TimeoutExpired (or any other subprocess failure) escape as
    # a bare traceback — that would bypass main()'s human "could not transcribe"
    # notice and the clean-exit path. Return None on any failure so the caller's
    # existing `if not transcript:` branch fires uniformly.
    try:
        result = subprocess.run(
            [sys.executable, STT_PKG, audio_path],
            capture_output=True, text=True, env=env, timeout=timeout
        )
    except subprocess.TimeoutExpired as e:
        snippet = _stderr_snippet(getattr(e, 'stderr', None))
        logging.error(f'STT subprocess timed out after {timeout}s'
                      + (f'; partial stderr: {snippet}' if snippet else ''))
        return None
    except Exception as e:
        logging.error(f'STT subprocess failed to run: {e}')
        return None
    transcript = result.stdout.strip()
    if result.returncode != 0 or not transcript:
        stderr_out = result.stderr.strip()
        logging.error(f'STT failed (rc={result.returncode}): {stderr_out}')
        # The provider's stderr can embed a server-controlled HTTP body. The
        # unescaped copy above goes to OUR log file; the copy that shares the
        # shim's stream is flattened onto one prefixed line so it cannot begin a
        # line with the fetch marker (C3 review 4, R1 route b).
        print('[stt-provider] ' + _one_line(stderr_out), file=sys.stderr)
        return None
    return transcript

# ── Cleanup ─────────────────────────────────────────────────────────────────────

def prune_inbox(keep_n):
    """Keep the newest keep_n .oga files in INBOX_DIR; delete older ones. Unlike a
    delete-immediately-after-transcription, this RETAINS recent audio so the user
    or agent can retranscribe / re-test by file_id without re-fetching, while still
    bounding disk use. keep_n comes from STT_AUDIO_RETENTION (the Go shim passes
    mappings.json:plugins.stt.audio_retention; default 500). A negative keep_n
    disables pruning (keep everything). Non-fatal — recovery never depends on this
    cache (download_attachment / retranscribe re-fetch from Telegram by file_id)."""
    if keep_n < 0:
        return
    try:
        names = [f for f in os.listdir(INBOX_DIR) if f.endswith('.oga')]
    except OSError:
        return
    stamped = []
    for f in names:
        p = os.path.join(INBOX_DIR, f)
        try:
            stamped.append((os.path.getmtime(p), p))
        except OSError:
            continue  # vanished under us (concurrent prune); skip
    stamped.sort(reverse=True)  # newest first
    for _, p in stamped[keep_n:]:
        try:
            os.remove(p)
        except OSError as e:
            logging.warning(f'prune_inbox: failed to remove {p}: {e}')


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 4:
        sys.exit(1)

    # Bot token is supplied on stdin (line 1) — never via argv — so it
    # doesn't appear in /proc/<pid>/cmdline, ps, or audit logs. The Go
    # shim writes "<token>\n" to our stdin before calling Run() (see
    # internal/plugin/builtins/stt/stt.go runHandler).
    token = sys.stdin.readline().rstrip('\n')
    if not token:
        logging.error('stt-handler: empty token on stdin (expected <token>\\n as line 1)')
        sys.exit(1)

    chat_id    = sys.argv[1]
    msg_id     = int(sys.argv[2])
    file_id    = sys.argv[3]
    thread_raw = sys.argv[4] if len(sys.argv) > 4 else ''
    thread_id  = int(thread_raw) if thread_raw else None

    # Download audio
    audio_path = os.path.join(INBOX_DIR, f'{int(time.time()*1000)}-{file_id}.oga')
    logging.info(f'Processing voice msg_id={msg_id} file_id={file_id}')
    dl_start = time.time()
    for attempt in range(1, 4):
        try:
            download_file(token, file_id, audio_path)
            fsize = os.path.getsize(audio_path)
            logging.info(f'Downloaded audio to {audio_path} ({fsize} bytes) [attempt {attempt}]')
            if fsize > 0:
                break
            logging.warning(f'Downloaded file is 0 bytes [attempt {attempt}], retrying after 2s...')
            time.sleep(2)
        except PermanentDownloadError as e:
            # I-9: non-retryable (expired/invalid file_id, >20MB getFile limit).
            # Exit WITHOUT burning the remaining retries on a guaranteed-permanent
            # failure. The Go shim sees empty stdout → [STT FAILED] marker, and the
            # broker sends the human "couldn't transcribe" notice.
            emit_fetch_error(e)
            logging.error(f'Download permanently failed (non-retryable): {e}')
            sys.exit(1)
        except Exception as e:
            logging.warning(f'Download failed [attempt {attempt}]: {e}')
            if attempt == 3:
                emit_fetch_error(e)
                logging.error(f'Download failed after 3 attempts: {e}')
                sys.exit(1)
            time.sleep(2)
    else:
        emit_fetch_error('the download produced 0 bytes after 3 attempts')
        logging.error('Download produced 0 bytes after 3 attempts')
        sys.exit(1)

    # Everything past a successful download runs under a finally that always
    # removes the cached .oga (I-10). The file has been written and is about to
    # be read by run_stt; cleanup happens after that on BOTH the success and the
    # failure-after-download (transcription failed / SystemExit) paths.
    try:
        # Load API keys
        keys = load_env(ENV_FILE)

        # Budget the transcription against the broker's ACTUAL SIGKILL deadline
        # (C3_STT_DEADLINE_SECONDS, size-scaled by the Go shim), not a hardcoded
        # 270. Subtract the download already spent and a margin so subprocess.run
        # returns before the Go SIGKILL fires (P1-3, 2026-08-08 cascade). Floored at
        # 60s so a slow download doesn't leave an unusably tiny window. Falls back to
        # 300 when the var is absent (an older shim).
        dl_elapsed = time.time() - dl_start
        deadline = int(os.environ.get('C3_STT_DEADLINE_SECONDS', '300'))
        stt_timeout = max(60, deadline - int(dl_elapsed) - 30)
        logging.info(f'Download took {dl_elapsed:.1f}s; deadline={deadline}s; STT subprocess budget={stt_timeout}s')

        # Transcribe
        transcript = run_stt(audio_path, keys, timeout=stt_timeout)
        if not transcript:
            # The I-2 timeout path also lands here (run_stt -> None). Exit
            # non-zero: the Go shim sees empty stdout → [STT FAILED] marker → the
            # broker sends the human "couldn't transcribe" notice. Telegram
            # sending is no longer this handler's job.
            logging.error(f'STT returned no transcript for {audio_path}')
            sys.exit(1)
        logging.info(f'Transcript ({len(transcript)} chars): {transcript[:80]}...')

        # Print transcript to stdout for the Go-side STT shim to read. The Go
        # broker/channel renders the Telegram readback echo from this transcript
        # (internal/channel/telegram/readback.go) — the handler sends nothing.
        print(transcript)
    finally:
        # Rolling-window audio cleanup (replaces the old delete-immediately, per
        # the maintainer's call): keep the newest N .oga in the inbox so recent
        # audio stays available for retranscribe/testing, while disk stays bounded.
        # N from STT_AUDIO_RETENTION (Go shim -> mappings.json:plugins.stt.
        # audio_retention; default 500). Runs on success AND failure-after-download
        # (SystemExit / any exception). Safe — recovery re-fetches from Telegram.
        try:
            _keep_n = int(os.environ.get('STT_AUDIO_RETENTION', '500'))
        except ValueError:
            _keep_n = 500
        prune_inbox(_keep_n)

if __name__ == '__main__':
    main()
