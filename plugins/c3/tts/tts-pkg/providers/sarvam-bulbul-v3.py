"""Sarvam Bulbul v3 text-to-speech provider."""

import base64
import json
import os
import urllib.request

from providers import read_response, request_timeout, validate_mp3

MODEL_ID = "bulbul:v3"
OUTPUT_MIME = "audio/mpeg"
MAX_CHARS = 2500


def available():
    return "" if os.environ.get("SARVAM_API_KEY") else "SARVAM_API_KEY not set"


def _language(text, hint):
    tamil = (hint or "").lower().split("-", 1)[0] == "ta"
    if not hint or hint.lower() == "auto":
        tamil = any("\u0b80" <= char <= "\u0bff" for char in text)
    return ("ta-IN", "kavitha") if tamil else ("en-IN", "shubh")


def synthesize(text, language, deadline_seconds, urlopen=urllib.request.urlopen):
    key = os.environ.get("SARVAM_API_KEY", "")
    if not key:
        raise RuntimeError("SARVAM_API_KEY not available")
    language_code, speaker = _language(text, language)
    body = json.dumps({
        "text": text,
        "language_code": language_code,
        "model": MODEL_ID,
        "speaker": speaker,
        "output_audio_codec": "mp3",
    }).encode()
    request = urllib.request.Request(
        "https://api.sarvam.ai/text-to-speech",
        data=body,
        headers={
            "api-subscription-key": key,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urlopen(request, timeout=request_timeout(deadline_seconds)) as response:
        result = json.loads(read_response(response))
    audios = result.get("audios") or []
    if not audios:
        raise ValueError("Sarvam response has no audio")
    return validate_mp3(base64.b64decode(audios[0], validate=True))
