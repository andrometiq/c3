"""ElevenLabs Flash v2.5 text-to-speech provider."""

import json
import os
import urllib.request

from providers import read_response, request_timeout, validate_mp3

MODEL_ID = "eleven_flash_v2_5"
OUTPUT_MIME = "audio/mpeg"
MAX_CHARS = 5000

_VOICE_ID = "21m00Tcm4TlvDq8ikWAM"


def available():
    return "" if os.environ.get("ELEVENLABS_API_KEY") else "ELEVENLABS_API_KEY not set"


def synthesize(text, language, deadline_seconds, urlopen=urllib.request.urlopen):
    key = os.environ.get("ELEVENLABS_API_KEY", "")
    if not key:
        raise RuntimeError("ELEVENLABS_API_KEY not available")
    body = {"text": text, "model_id": MODEL_ID}
    hint = (language or "").strip().lower()
    if hint and hint != "auto":
        body["language_code"] = hint.split("-", 1)[0]
    request = urllib.request.Request(
        f"https://api.elevenlabs.io/v1/text-to-speech/{_VOICE_ID}?output_format=mp3_44100_128",
        data=json.dumps(body).encode(),
        headers={
            "xi-api-key": key,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urlopen(request, timeout=request_timeout(deadline_seconds)) as response:
        return validate_mp3(read_response(response))
