"""ElevenLabs Scribe v2 STT provider.

Requires: ELEVENLABS_API_KEY env var (or in ~/.claude/stt.env)
API: POST https://api.elevenlabs.io/v1/speech-to-text
Model: scribe_v2  (still ElevenLabs' current STT model as of 2026-07-26 —
"Scribe v3" is a text-to-speech model, not a transcription one, so there is
nothing newer to move to here.)

2026-07-26: this provider previously ignored c3's vocabulary entirely — it had
no set_vocabulary() — while the API has supported `keyterms` biasing all along.
That was the single biggest gap in the shipped chain for proper-noun-heavy
dictation, and it is now wired.

Known limitation, and why this sits third rather than first: Scribe does not
translate. Tamil or Hindi spans come back in the source language (and script),
where the Gemini and Soniox providers return them as inline English. If your
audio is code-switched, weigh that before promoting this provider — the
bake-off harness reports non-Latin character counts per provider for exactly
this reason.
"""
import os, json, urllib.request, urllib.error

MODEL_ID = "scribe_v2"

# USD per minute of audio, for the bake-off cost column: $0.22/hr for Scribe v2
# plus $0.05/hr when keyterms biasing is used (we always send keyterms when a
# vocabulary is loaded) = $0.27/hr. Source: vendor list price recorded in the
# 2026-07-26 model survey — NOT re-verified against the pricing page here.
COST_PER_MINUTE_USD = 0.27 / 60.0

# API limits on the keyterms array (docs, 2026-07-26): at most 1000 terms, each
# under 50 characters and at most 5 words. Terms that violate those are dropped
# rather than risking a 422 that would take the whole request down. Note also
# that sending 100+ keyterms triggers a 20-second minimum billing per request —
# irrelevant for voice notes of any real length.
_MAX_KEYTERMS = 1000
_MAX_KEYTERM_CHARS = 49
_MAX_KEYTERM_WORDS = 5

_EL_KEY = None


def _get_key():
    global _EL_KEY
    if _EL_KEY:
        return _EL_KEY
    _EL_KEY = os.environ.get("ELEVENLABS_API_KEY", "")
    if not _EL_KEY:
        env_path = os.path.expanduser("~/.claude/stt.env")
        try:
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("ELEVENLABS_API_KEY="):
                        _EL_KEY = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
        except Exception:
            pass
    return _EL_KEY


def available() -> str:
    """"" when this provider can run, else why it can't (see stt.py)."""
    return "" if _get_key() else "ELEVENLABS_API_KEY not set (env or ~/.claude/stt.env)"


# --- Vocabulary -> keyterms ---
_VOCAB = {"terms": [], "context": ""}


def set_vocabulary(vocab):
    global _VOCAB
    _VOCAB = vocab or {"terms": [], "context": ""}


def _build_keyterms():
    """Adapt c3's vocabulary into ElevenLabs' `keyterms` list.

    Preferred spellings only. The vocabulary's "NOT these misspellings"
    negatives are deliberately NOT sent: keyterms is a bias-TOWARD list, so
    feeding it the wrong spellings would bias the model toward producing them.
    The free-text context is dropped for the same anti-hallucination reason the
    Sarvam and Soniox providers drop it. Returns a list (possibly empty)."""
    out, seen = [], set()
    for t in _VOCAB.get("terms", []):
        term = (t.get("preferred") or "").strip()
        if not term or term.lower() in seen:
            continue
        if len(term) > _MAX_KEYTERM_CHARS or len(term.split()) > _MAX_KEYTERM_WORDS:
            continue
        seen.add(term.lower())
        out.append(term)
        if len(out) >= _MAX_KEYTERMS:
            break
    return out


def _multipart(fields, filename, mime, audio_bytes):
    """Build a multipart/form-data body from simple text fields + one file.
    Returns (body_bytes, boundary)."""
    boundary = "----ElevenLabsBoundary1234567890"
    body = b""
    for name, value in fields:
        body += (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
            f"{value}\r\n"
        ).encode()
    body += (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        f"Content-Type: {mime}\r\n\r\n"
    ).encode() + audio_bytes + f"\r\n--{boundary}--\r\n".encode()
    return body, boundary


def transcribe(audio_path: str, audio_bytes: bytes) -> str | None:
    """Transcribe audio using ElevenLabs Scribe v2.
    Returns transcript string or None on failure.
    """
    key = _get_key()
    if not key:
        raise RuntimeError("ELEVENLABS_API_KEY not available")

    filename = os.path.basename(audio_path) if audio_path else "audio.ogg"

    # Determine content type
    if filename.endswith(".ogg") or filename.endswith(".oga"):
        mime = "audio/ogg"
    elif filename.endswith(".mp3"):
        mime = "audio/mpeg"
    elif filename.endswith(".wav"):
        mime = "audio/wav"
    else:
        mime = "application/octet-stream"

    fields = [("model_id", MODEL_ID)]
    keyterms = _build_keyterms()
    if keyterms:
        # Sent as a JSON array string — the documented multipart encoding.
        fields.append(("keyterms", json.dumps(keyterms)))

    body, boundary = _multipart(fields, filename, mime, audio_bytes)

    req = urllib.request.Request(
        "https://api.elevenlabs.io/v1/speech-to-text",
        data=body,
        headers={
            "xi-api-key": key,
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        }
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read())
            text = result.get("text", "") or ""
            return text.strip() if text.strip() else None
    except urllib.error.HTTPError as e:
        err_body = e.read().decode(errors="replace")
        raise RuntimeError(f"HTTP {e.code}: {err_body}")
