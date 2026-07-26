"""Soniox stt-async-v5 STT provider (async file transcription).

Requires: SONIOX_API_KEY env var (or in ~/.claude/stt.env)

Why this provider is in the default chain (2026-07-26 survey, rank 2): its
biasing API is the closest match in the market to c3's vocabulary file —
`context.terms` takes a plain list of domain terms (8,000-token budget, no
50-term ceiling), it handles code-mixed speech natively, and one-way
translation to English is included at no extra charge, which is what keeps the
Tamil-in-the-middle-of-an-English-sentence case in one pass. It also takes 5-hour
files, so long dictation never needs chunking. The honest caveat: Soniox appears
in no independent Indic benchmark, so its rank here is API capability plus
vendor claims — measure it with ../bakeoff/run_bakeoff.py before trusting it
above Gemini.

API (verified against soniox.com/docs on 2026-07-26), stdlib urllib only:
    POST   /v1/files                          multipart field "file"  -> {"id": ...}
    POST   /v1/transcriptions                 {model, file_id, ...}   -> {"id": ...}
    GET    /v1/transcriptions/{id}            -> {"status": "completed"|"error", ...}
    GET    /v1/transcriptions/{id}/transcript -> {"tokens": [{"text", "language",
                                                  "translation_status", "start_ms"}]}
    DELETE /v1/files/{id}, /v1/transcriptions/{id}   (best-effort cleanup)
Auth on every call: Authorization: Bearer <key>

Env knobs (all optional):
    C3_STT_SONIOX_MODEL           default "stt-async-v5"
    C3_STT_SONIOX_LANGUAGE_HINTS  default "en,ta,hi"  (hints, not a whitelist)
    C3_STT_SONIOX_TRANSLATE       default "en"; set empty to keep speech verbatim
"""
import os, json, time, urllib.request, urllib.error

_SONIOX_BASE = "https://api.soniox.com"

MODEL_ID = os.environ.get("C3_STT_SONIOX_MODEL", "").strip() or "stt-async-v5"

# USD per minute of audio, for the bake-off cost column. Soniox's published
# price on 2026-07-26 is $0.10/hour for async file transcription, translation
# included at no extra cost -> $0.001667/min. Cheapest in the shipped chain.
COST_PER_MINUTE_USD = 0.10 / 60.0

# Language hints bias detection without restricting it (language_hints_strict
# stays false), so an unexpected language is still transcribed.
_LANGUAGE_HINTS = [
    h.strip() for h in
    (os.environ.get("C3_STT_SONIOX_LANGUAGE_HINTS", "").strip() or "en,ta,hi").split(",")
    if h.strip()
]

# Target language for one-way translation. Matches the chain's contract: the
# transcript that reaches the agent is English, with non-English speech
# translated inline (the Gemini provider does the same thing via its prompt).
_TRANSLATE_TO = os.environ.get("C3_STT_SONIOX_TRANSLATE", "en").strip()

# context.terms budget. The documented cap is 8,000 tokens (~10,000 chars) for
# the whole context object; we stay well under it and drop the tail rather than
# let the API reject the whole request over a long vocabulary file.
_MAX_CONTEXT_CHARS = 6000

# --- Load API key ---
_SONIOX_KEY = None


def _get_key():
    global _SONIOX_KEY
    if _SONIOX_KEY:
        return _SONIOX_KEY
    _SONIOX_KEY = os.environ.get("SONIOX_API_KEY", "")
    if not _SONIOX_KEY:
        env_path = os.path.expanduser("~/.claude/stt.env")
        try:
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("SONIOX_API_KEY="):
                        _SONIOX_KEY = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
        except Exception:
            pass
    return _SONIOX_KEY


def available() -> str:
    """"" when this provider can run, else why it can't (see stt.py)."""
    return "" if _get_key() else "SONIOX_API_KEY not set (env or ~/.claude/stt.env)"


# --- Vocabulary -> context.terms ---
_VOCAB = {"terms": [], "context": ""}


def set_vocabulary(vocab):
    global _VOCAB
    _VOCAB = vocab or {"terms": [], "context": ""}


def _build_context():
    """Adapt c3's vocabulary into Soniox's `context` object, or None.

    Only `terms` is populated. The free-text `context.text` slot is left empty
    on purpose, for the same reason the Sarvam provider drops it: a topic
    narrative primes the model toward that topic and can seed a hallucinated
    transcript on unclear audio. A bare term list biases spelling without
    narrating a subject. Terms are truncated to the character budget."""
    terms, used = [], 0
    for t in _VOCAB.get("terms", []):
        preferred = (t.get("preferred") or "").strip()
        if not preferred:
            continue
        if used + len(preferred) + 2 > _MAX_CONTEXT_CHARS:
            break
        terms.append(preferred)
        used += len(preferred) + 2
    return {"terms": terms} if terms else None


# --- HTTP helpers (stdlib only) ---

def _request(path, key, method="GET", body=None, timeout=30):
    """Call an api.soniox.com JSON endpoint; return the parsed dict ({} if empty)."""
    url = _SONIOX_BASE.rstrip("/") + "/" + path.lstrip("/")
    headers = {"Authorization": f"Bearer {key}"}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else {}


def _upload_file(audio_path, audio_bytes, key, timeout=120):
    """POST /v1/files as multipart/form-data (field name "file"); return file id."""
    boundary = "----SonioxBoundary" + str(int(time.time() * 1000))
    filename = os.path.basename(audio_path) if audio_path else "audio.ogg"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + audio_bytes + f"\r\n--{boundary}--\r\n".encode()

    req = urllib.request.Request(
        _SONIOX_BASE + "/v1/files",
        data=body,
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read() or b"{}")
    file_id = data.get("id")
    if not file_id:
        raise RuntimeError(f"soniox: upload returned no file id: {data}")
    return file_id


def _delete_quietly(path, key):
    """Best-effort DELETE. Voice notes shouldn't linger in a vendor's storage,
    but a failed cleanup must never fail a transcription that already worked."""
    try:
        _request(path, key, method="DELETE", timeout=15)
    except Exception:
        pass


def _poll_until_done(transcription_id, key, poll_interval=3, timeout=240):
    """Poll GET /v1/transcriptions/{id} until status is terminal.

    Returns the final status dict. Raises TimeoutError if it never gets there
    within `timeout` — same contract as the Sarvam provider's poll loop, so the
    chain falls through to the next provider instead of hanging to SIGKILL."""
    start = time.monotonic()
    while True:
        status = _request(f"/v1/transcriptions/{transcription_id}", key)
        state = (status.get("status") or "").lower()
        if state in {"completed", "error"}:
            return status
        if time.monotonic() - start > timeout:
            raise TimeoutError(
                f"soniox: transcription {transcription_id} not finished within {timeout}s"
            )
        time.sleep(poll_interval)


def _join_tokens(tokens):
    """Join Soniox tokens into text.

    Soniox tokens carry their own leading whitespace, so a plain "" join is
    correct. Defensive fallback: if the joined result has no spaces at all but
    there were several tokens, the build clearly isn't emitting spacing — join
    with single spaces instead of handing back one unreadable mashed word."""
    joined = "".join(t.get("text") or "" for t in tokens)
    if len(tokens) > 3 and " " not in joined.strip():
        joined = " ".join((t.get("text") or "").strip() for t in tokens if (t.get("text") or "").strip())
    return joined.strip()


def _assemble_text(tokens, target_language):
    """Turn the token stream into one transcript in the target language.

    With one-way translation on, the stream is unified: speech that needed
    translating appears twice — the original token (translation_status
    "original") and its translation (translation_status "translation") — while
    speech already in the target language appears once, as an original. Taking
    only the translated tokens would therefore delete every English word; taking
    everything would double the translated spans. So: keep translations, plus
    originals already in the target language, ordered by start time."""
    if not tokens:
        return ""
    if not target_language:
        return _join_tokens(tokens)

    has_translation = any((t.get("translation_status") or "") == "translation" for t in tokens)
    if not has_translation:
        return _join_tokens(tokens)

    target = target_language.lower()
    keep = [
        t for t in tokens
        if (t.get("translation_status") or "") == "translation"
        or (t.get("language") or "").lower().startswith(target)
    ]
    if not keep:
        keep = [t for t in tokens if (t.get("translation_status") or "") == "translation"]
    # Stable sort: tokens without start_ms keep their original relative order.
    keep.sort(key=lambda t: t.get("start_ms") or 0)
    return _join_tokens(keep)


def transcribe(audio_path: str, audio_bytes: bytes) -> str | None:
    """Transcribe audio using Soniox stt-async-v5.
    Returns transcript string or None on failure."""
    key = _get_key()
    if not key:
        raise RuntimeError("SONIOX_API_KEY not available")

    # Size the poll wait to the budget the handler gave us (same convention as
    # the Sarvam batch path): stay ~15s under it so we return gracefully before
    # the subprocess timeout SIGKILLs the whole tree.
    try:
        budget = int(os.environ.get("C3_STT_BUDGET_SECONDS", "270"))
    except ValueError:
        budget = 270
    wait = max(30, min(240, budget - 15))

    file_id = _upload_file(audio_path, audio_bytes, key)
    transcription_id = None
    try:
        body = {
            "model": MODEL_ID,
            "file_id": file_id,
            "enable_language_identification": True,
        }
        if _LANGUAGE_HINTS:
            body["language_hints"] = _LANGUAGE_HINTS
        if _TRANSLATE_TO:
            body["translation"] = {"type": "one_way", "target_language": _TRANSLATE_TO}
        ctx = _build_context()
        if ctx:
            body["context"] = ctx

        created = _request("/v1/transcriptions", key, method="POST", body=body)
        transcription_id = created.get("id")
        if not transcription_id:
            raise RuntimeError(f"soniox: create returned no transcription id: {created}")

        status = _poll_until_done(transcription_id, key, timeout=wait)
        if (status.get("status") or "").lower() != "completed":
            # Fall through to the next provider rather than raising: an "error"
            # status is the vendor's verdict on this audio, not a bug here.
            return None

        result = _request(f"/v1/transcriptions/{transcription_id}/transcript", key, timeout=60)
        tokens = result.get("tokens") or []
        text = _assemble_text(tokens, _TRANSLATE_TO)
        if not text and isinstance(result.get("text"), str):
            text = result["text"].strip()
        return text or None
    finally:
        # Don't leave the audio or the transcript sitting in vendor storage.
        if transcription_id:
            _delete_quietly(f"/v1/transcriptions/{transcription_id}", key)
        _delete_quietly(f"/v1/files/{file_id}", key)
