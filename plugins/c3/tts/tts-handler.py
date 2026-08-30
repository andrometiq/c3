#!/usr/bin/env python3
"""Text-to-speech handler for C3.

Called by the Go-side TTS plugin (internal/plugin/builtins/tts/tts.go):

    stdin:  the whole UTF-8 text to speak (multi-line Markdown is accepted)
    argv:   python3 tts-handler.py [--language <ta|en|…>] [--chain a,b,c]
            [--check] [--out <path>]
    env:    C3_TTS_CHAIN, C3_TTS_DEADLINE_SECONDS, TTS_ENV_FILE, TTS_LOG_FILE

The default provider order is Sarvam Bulbul v3, ElevenLabs Flash v2.5, then
OpenRouter Gemini TTS; C3_TTS_CHAIN or --chain overrides it.

On success: writes raw MP3 bytes and nothing else to stdout, or to --out when
that option is supplied. Diagnostics are one-line records on stderr and in the
handler log. --check is the sole non-audio stdout mode: it prints the resolved
chain, provider availability, and key-file resolution without synthesizing.

Exit 0 means success, exit 3 means preprocessing found nothing speakable, and
exit 1 means every configured provider failed. Failure exits write no stdout.
"""

import argparse
import logging
import os
import sys
import time

HERE = os.path.dirname(os.path.realpath(__file__))
TTS_PKG_DIR = os.path.join(HERE, "tts-pkg")
sys.path.insert(0, TTS_PKG_DIR)

import tts  # noqa: E402

ENV_FILE = os.path.realpath(os.environ.get(
    "TTS_ENV_FILE", os.path.expanduser("~/.claude/tts.env")))
STT_ENV_FALLBACK = os.path.realpath(os.path.expanduser("~/.claude/stt.env"))
LOG_FILE = os.path.realpath(os.environ.get(
    "TTS_LOG_FILE", os.path.expanduser("~/.claude/channels/web/tts-handler.log")))
KEYS = ("OPENROUTER_API_KEY", "SARVAM_API_KEY", "ELEVENLABS_API_KEY")


def _one_line(text):
    return str(text).replace("\\", "\\\\").replace("\r", "\\r").replace("\n", "\\n")


class _SingleLineFormatter(logging.Formatter):
    """A Formatter whose every record occupies exactly one line."""

    def format(self, record):
        return _one_line(super().format(record))


def _configure_logging():
    formatter = _SingleLineFormatter(
        "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handlers = []
    try:
        parent = os.path.dirname(LOG_FILE)
        if parent:
            os.makedirs(parent, exist_ok=True)
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    except Exception:
        pass
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)
    handlers.append(stderr_handler)
    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)


def _read_env(path):
    values = {}
    try:
        with open(path, encoding="utf-8") as source:
            for raw in source:
                key, separator, value = raw.strip().partition("=")
                if separator and key.isidentifier():
                    values[key] = value.strip().strip('"').strip("'")
    except OSError:
        pass
    return values


def load_keys(tts_path=ENV_FILE, stt_path=STT_ENV_FALLBACK):
    """Load process env, then tts.env, then stt.env for still-missing keys."""
    tts_values = _read_env(tts_path)
    stt_values = _read_env(stt_path)
    sources = {}
    for key in KEYS:
        if os.environ.get(key):
            sources[key] = "environment"
        elif tts_values.get(key):
            os.environ[key] = tts_values[key]
            sources[key] = tts_path
        elif stt_values.get(key):
            os.environ[key] = stt_values[key]
            sources[key] = stt_path
        else:
            sources[key] = "missing"
    return sources


def _deadline():
    try:
        value = float(os.environ.get("C3_TTS_DEADLINE_SECONDS", "120"))
    except ValueError:
        value = 120.0
    return value if value > 0 else 120.0


def _chain_source(cli_chain):
    if cli_chain and cli_chain.strip():
        return "--chain"
    if (os.environ.get("C3_TTS_CHAIN") or "").strip():
        return "$C3_TTS_CHAIN"
    return "built-in default"


def check(chain, source, key_sources):
    print(f"chain={','.join(chain)} source={source}")
    print(f"tts_env_file={ENV_FILE} state={'present' if os.path.isfile(ENV_FILE) else 'missing'}")
    print(f"stt_env_fallback={STT_ENV_FALLBACK} state={'present' if os.path.isfile(STT_ENV_FALLBACK) else 'missing'}")
    for name in chain:
        try:
            provider = tts.load_provider(name)
            reason = (provider.available() or "").strip()
            state = "ready" if not reason else "unavailable"
            detail = "" if not reason else f" reason={reason}"
        except Exception as error:
            state = "unloadable"
            detail = f" reason={type(error).__name__}: {error}"
        print(f"provider={name} state={state}{detail}")
    print("keys=" + ",".join(f"{key}:{key_sources[key]}" for key in KEYS))


def main(argv=None):
    parser = argparse.ArgumentParser(description="C3 text-to-speech handler")
    parser.add_argument("--language", default="auto")
    parser.add_argument("--chain")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--out")
    args = parser.parse_args(argv)

    _configure_logging()
    key_sources = load_keys()
    chain = tts.resolve_chain(args.chain)
    if args.check:
        check(chain, _chain_source(args.chain), key_sources)
        return 0

    spoken = tts.speakable(sys.stdin.read())
    if not spoken:
        logging.info("nothing speakable after preprocessing")
        return 3

    started = time.monotonic()
    try:
        audio, provider_name, chunk_count = tts.run_chain(
            chain, spoken, args.language, _deadline(), log=logging.warning)
    except Exception as error:
        logging.error("synthesis failed: %s: %s", type(error).__name__, error)
        return 1
    elapsed_ms = int((time.monotonic() - started) * 1000)
    logging.info("provider=%s chunks=%d bytes=%d ms=%d",
                 provider_name, chunk_count, len(audio), elapsed_ms)
    if args.out:
        with open(args.out, "wb") as output:
            output.write(audio)
    else:
        sys.stdout.buffer.write(audio)
        sys.stdout.buffer.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
