"""Shared helpers for C3's bundled TTS providers."""

MAX_RESPONSE_BYTES = 25 << 20


def request_timeout(deadline_seconds):
    """Clamp one provider call to its positive remaining budget and 120s."""
    try:
        remaining = float(deadline_seconds)
    except (TypeError, ValueError):
        remaining = 120.0
    if remaining <= 0:
        raise TimeoutError("TTS deadline exhausted")
    return min(120.0, remaining)


def read_response(response):
    """Read at most 25 MiB from a provider response."""
    body = response.read(MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES:
        raise ValueError("TTS provider response exceeds 25 MiB")
    return body


def validate_mp3(audio):
    """Return audio when it has an ID3v2 header or MPEG frame sync."""
    if not isinstance(audio, bytes) or len(audio) < 2:
        raise ValueError("TTS provider returned empty or truncated audio")
    if audio.startswith(b"ID3") or (audio[0] == 0xFF and audio[1] & 0xE0 == 0xE0):
        return audio
    raise ValueError("TTS provider response is not MP3")
