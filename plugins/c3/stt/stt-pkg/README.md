# STT Package — Pluggable Speech-to-Text Chain

Modular STT with provider chaining, retry, and automatic fallback. Used
by c3 as its bundled speech-to-text engine; also runnable standalone.

## Quick Start

```bash
# What's installed, what has a key, what order will run
python3 stt.py --list-providers

# Default chain
python3 stt.py audio.ogg

# One provider, no fallback
python3 stt.py audio.ogg --chain sarvam-saaras-v3

# A different order, just for this run
python3 stt.py audio.ogg --chain elevenlabs-scribe-v2,gemini-3-flash-openrouter

# Custom chain with extra retries
python3 stt.py audio.ogg --chain gemini-3-flash-openrouter,sarvam-saaras-v3 --retries 2 --retry-delay 3
```

## Structure

```
stt-pkg/
├── stt.py                                  # Main entry point
├── vocabulary.txt                          # Optional domain-vocabulary biasing
├── providers/
│   ├── gemini-3-flash-openrouter.py        # Gemini Flash via OpenRouter   (chain 1)
│   ├── soniox-stt-async-v5.py              # Soniox async v5               (chain 2)
│   ├── elevenlabs-scribe-v2.py             # ElevenLabs Scribe v2          (chain 3)
│   ├── sarvam-saaras-v3.py                 # Sarvam AI Saaras v3           (chain 4)
│   └── __init__.py
├── ../bakeoff/                             # Harness to measure them against each other
└── README.md
```

All four providers ship bundled and all four are in the default chain. That
costs nothing on an install with one key: a provider whose key is missing is
skipped instantly (see `available()` below) rather than burning attempts.

## Configuring the chain — where the order lives

The chain is **ordered**: position 1 is tried first, position 2 only if 1
fails, and so on. It resolves in exactly this precedence:

| # | Source | Use it for |
|---|---|---|
| 1 | `--chain` on the command line | ad-hoc runs, bake-offs |
| 2 | `$C3_STT_CHAIN` | **the normal way to change it** |
| 3 | `DEFAULT_CHAIN` in `stt.py` | what ships |

c3's `stt-handler.py` runs this script **without** `--chain`, so in production
the order comes from `$C3_STT_CHAIN` or the default. The handler reads
`~/.claude/stt.env` — the same file `c3-broker setup` writes API keys into —
and passes every `KEY=value` in it to this script as an environment variable.
So changing the order needs **one line, no source edit**:

```
# ~/.claude/stt.env
C3_STT_CHAIN=soniox-stt-async-v5,gemini-3-flash-openrouter,sarvam-saaras-v3
```

Two things to know about that file:

- Values in `stt.env` **override** the broker's own environment, so this line
  wins over an exported `C3_STT_CHAIN`.
- `c3-broker setup stt` **rewrites `stt.env` wholesale**, so re-running the
  interactive setup will drop a hand-added `C3_STT_CHAIN` line. Re-add it, or
  keep the chain in the broker's environment instead.

The shipped order is the 2026-07-26 model survey's ranking for
Indian-English + Tamil code-switched dictation, taken from **published
benchmarks and API capability — not from a measured run on your audio**.
Measure it yourself, then set the line above:

```bash
python3 ../bakeoff/run_bakeoff.py --audio-dir /path/to/voice-notes
```

## How It Works

1. `stt.py` resolves the chain (`--chain` → `$C3_STT_CHAIN` → default)
2. Loads each named provider from `providers/` by filename
3. Skips any provider that reports itself unavailable (no API key)
4. Runs the rest in chain order (left to right), N attempts each (1 + retries)
5. First non-empty result wins → printed to stdout
6. If a provider is exhausted, falls back to the next one
7. All skip/retry/fallback activity logged to stderr

## Provider Naming Convention

Name provider files descriptively:

```
<model-name>-<api-source>.py
```

Examples:
- `gemini-3-flash-openrouter.py` — Gemini 3 Flash routed through OpenRouter
- `sarvam-saaras-v3.py` — Sarvam AI's Saaras model, version 3
- `whisper-large-v3-groq.py` — OpenAI Whisper Large v3 via Groq
- `gpt4o-transcribe-openai.py` — GPT-4o transcribe via OpenAI directly
- `deepgram-nova-2.py` — Deepgram Nova 2

The filename (minus `.py`) is the provider name used in `--chain`. This makes chains self-documenting:

```bash
python3 stt.py audio.ogg --chain gemini-3-flash-openrouter,whisper-large-v3-groq,sarvam-saaras-v3
```

## Adding a New Provider

Create `providers/<model-name>-<api-source>.py`:

```python
"""Short description of the provider."""

def transcribe(audio_path: str, audio_bytes: bytes) -> str:
    """Transcribe audio.
    
    Args:
        audio_path: Absolute path to the audio file (for tools that need file paths)
        audio_bytes: Raw file bytes (for APIs that accept binary uploads)
    
    Returns:
        Transcript string, or None/empty string on failure.
        Raise exceptions for hard errors (stt.py will log and retry).
    """
    # Your implementation here
    return "transcript text"
```

`transcribe()` is the only required function. A provider may **optionally**
add `set_vocabulary()` to receive the shared domain vocabulary — `stt.py`
calls it (when present) right before each `transcribe()`, so you can bias the
model toward preferred spellings:

```python
_VOCAB = {"terms": [], "context": ""}

def set_vocabulary(vocab):
    """Optional. Receives the domain vocabulary loaded by stt.py.

    vocab is a dict:
        terms:   list of {"preferred": str, "not": [str], "note": str}
        context: str — a short description of the domain
    Adapt it into whatever your API accepts (system prompt, hotwords, a
    `prompt` parameter, …). See the bundled gemini/sarvam providers for
    two different adaptations.
    """
    global _VOCAB
    _VOCAB = vocab or {"terms": [], "context": ""}
```

A provider may **optionally** also add `available()`, which lets an unkeyed
provider be skipped instantly instead of failing every attempt (and sleeping
between them) on something that cannot succeed:

```python
def available() -> str:
    """"" when this provider can run, else a short reason why it can't."""
    return "" if _get_key() else "MY_ENGINE_API_KEY not set (env or ~/.claude/stt.env)"
```

Providers without `available()` behave exactly as before — always attempted.
A hook that raises is treated as available, so a bug in it can never remove a
working provider from the chain.

Two more optional module-level attributes, read by `--list-providers` and by
the bake-off harness's report — neither affects transcription:

```python
MODEL_ID = "my-engine-v2"        # what actually gets called
COST_PER_MINUTE_USD = 0.004      # published list price, or None if unknown
```

Then use it:
```bash
python3 stt.py audio.ogg --chain gemini-3-flash-openrouter,your-model-name,sarvam-saaras-v3
```

## How c3 Wires This Up

c3's broker subprocesses `plugins/c3/stt/stt-handler.py` for each voice
attachment. That handler in turn subprocesses `stt.py` from this directory
**without** `--chain`, so the live order is `$C3_STT_CHAIN` (from
`~/.claude/stt.env`) or the built-in default — see
[Configuring the chain](#configuring-the-chain--where-the-order-lives).
Pointing `plugins.stt.handler_path` in `~/.config/c3/mappings.json` at your own
handler script still works if you want to control the invocation entirely.

To use this package outside of c3 (e.g. from another voice-input tool),
just invoke `python3 stt.py <audio>` directly — stdout is the transcript,
stderr is the trace.

## Requirements

- Python 3.8+ (standard library only — no PyPI packages)
- **Gemini provider:** OPENROUTER_API_KEY (env or `~/.claude/stt.env`)
- **Soniox provider:** SONIOX_API_KEY (env or `~/.claude/stt.env`)
- **ElevenLabs provider:** ELEVENLABS_API_KEY (env or `~/.claude/stt.env`)
- **Sarvam provider:** SARVAM_API_KEY (env or `~/.claude/stt.env`)
- **`ffmpeg`** for the silence gate, **`ffprobe`** for Sarvam's >30s routing

## Tests

```bash
python3 -m pytest test_chain_config.py test_sarvam_prompt.py -q
python3 test_silence_gate.py     # stdlib-only, runs standalone
```

`test_chain_config.py` covers chain resolution, the availability hook, and the
ordering/fallback behaviour with fake providers — no network, no keys.

## Why This Exists

Gemini Flash occasionally returns HTTP 200 with `finish_reason: "stop"` but zero completion tokens and null content. Silent failure — no error code. Happens ~1 in 5 calls on some audio samples. The chain + retry pattern catches this reliably.
