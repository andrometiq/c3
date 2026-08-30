# C3 TTS provider chain

`tts.py` preprocesses reply Markdown, chunks the spoken prose, and tries the
configured providers in order. The shipped order is Sarvam Bulbul v3,
ElevenLabs Flash v2.5, then OpenRouter Gemini TTS. Set `C3_TTS_CHAIN` or pass
`tts-handler.py --chain ...` to change it.

A provider is a Python file in `providers/` exposing:

```python
MODEL_ID = "provider model id"
OUTPUT_MIME = "audio/mpeg"
MAX_CHARS = 4000

def available() -> str: ...
def synthesize(text: str, language: str, deadline_seconds: float,
               urlopen=urllib.request.urlopen) -> bytes: ...
```

`available` returns an empty string when ready or a reason to skip. `synthesize`
must return MP3 bytes and raise on any HTTP, response-shape, or audio-validation
failure. The runner synthesizes every chunk with one provider; a failed chunk
restarts the whole reply on the next provider so a reply never mixes voices.

Handlers reserve stdout for raw MP3. Log diagnostics to stderr. Provider HTTP
calls must stay within both 120 seconds and the supplied remaining deadline.
OpenRouter returns PCM and needs `ffmpeg` on `PATH`; a missing or failed
transcoder makes the chain fall through to its next configured provider.
