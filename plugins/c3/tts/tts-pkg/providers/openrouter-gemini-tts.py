"""Gemini 3.1 Flash TTS through OpenRouter's audio speech endpoint."""

import json
import os
import subprocess
import time
import urllib.request

from providers import read_response, request_timeout, validate_mp3

MODEL_ID = "google/gemini-3.1-flash-tts-preview"
OUTPUT_MIME = "audio/mpeg"
MAX_CHARS = 4000


def available():
    return "" if os.environ.get("OPENROUTER_API_KEY") else "OPENROUTER_API_KEY not set"


def _pcm_format(content_type):
    parts = [part.strip() for part in (content_type or "").split(";")]
    if not parts or parts[0].lower() != "audio/pcm":
        raise ValueError(f"OpenRouter response is not audio/pcm: {content_type or 'missing'}")
    rate, channels = 24000, 1
    for part in parts[1:]:
        name, separator, value = part.partition("=")
        if not separator:
            continue
        if name.strip().lower() == "rate":
            rate = int(value.strip().strip('"'))
        elif name.strip().lower() == "channels":
            channels = int(value.strip().strip('"'))
    if rate <= 0 or channels <= 0:
        raise ValueError("OpenRouter returned invalid PCM rate or channel count")
    return rate, channels


def transcode_pcm_to_mp3(pcm, rate, channels, timeout):
    result = subprocess.run(
        [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-f", "s16le", "-ar", str(rate), "-ac", str(channels),
            "-i", "pipe:0", "-codec:a", "libmp3lame", "-b:a", "64k",
            "-f", "mp3", "pipe:1",
        ],
        input=pcm,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        timeout=timeout,
        shell=False,
    )
    return result.stdout


def synthesize(text, language, deadline_seconds, urlopen=urllib.request.urlopen,
               transcoder=transcode_pcm_to_mp3):
    del language  # OpenRouter's compatible endpoint relies on Gemini auto-detection.
    key = os.environ.get("OPENROUTER_API_KEY", "")
    if not key:
        raise RuntimeError("OPENROUTER_API_KEY not available")
    try:
        budget = float(deadline_seconds)
    except (TypeError, ValueError):
        budget = 120.0
    if budget <= 0:
        raise TimeoutError("TTS deadline exhausted")
    body = json.dumps({
        "model": MODEL_ID,
        "input": text,
        "voice": "Kore",
        "response_format": "pcm",
    }).encode()
    request = urllib.request.Request(
        "https://openrouter.ai/api/v1/audio/speech",
        data=body,
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    started = time.monotonic()
    with urlopen(request, timeout=request_timeout(budget)) as response:
        rate, channels = _pcm_format(response.headers.get("Content-Type", ""))
        pcm = read_response(response)
    remaining = budget - (time.monotonic() - started)
    if remaining <= 0:
        raise TimeoutError("TTS deadline exhausted before PCM transcoding")
    return validate_mp3(transcoder(pcm, rate, channels, remaining))
