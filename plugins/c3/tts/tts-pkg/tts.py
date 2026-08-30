#!/usr/bin/env python3
"""Markdown preprocessing, chunking, and fallback chain for C3 TTS."""

import importlib.util
import os
import re
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROVIDERS_DIR = os.path.join(SCRIPT_DIR, "providers")

DEFAULT_CHAIN = (
    "sarvam-bulbul-v3",
    "elevenlabs-flash-v25",
    "openrouter-gemini-tts",
)

_FENCED_CODE = re.compile(r"(^|\n)[ \t]*(```|~~~).*?\n?[ \t]*\2(?=\n|$)", re.DOTALL)
_INLINE_CODE = re.compile(r"(`+)(.*?)\1", re.DOTALL)
_LINK = re.compile(r"!?\[([^\]]+)\]\([^)]*\)")
_BARE_URL = re.compile(r"(?:https?://|www\.)[^\s<>]+", re.IGNORECASE)
_EMOJI = re.compile("[\U0001F000-\U0001FAFF\u2600-\u27BF\uFE0F\u200D]")
_TABLE_DIVIDER_CELL = re.compile(r"^:?-{3,}:?$")


class SynthesisError(RuntimeError):
    """Every configured provider was unavailable or failed."""


def _table_line(line):
    if "|" not in line:
        return None
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    if len(cells) < 2:
        return None
    if all(_TABLE_DIVIDER_CELL.match(cell.replace(" ", "")) for cell in cells):
        return ""
    return ", ".join(cell for cell in cells if cell) + "."


def speakable(text):
    """Strip Markdown and non-verbal glyphs while preserving Unicode prose."""
    if not text:
        return ""
    value = text.replace("\r\n", "\n").replace("\r", "\n")
    value = _FENCED_CODE.sub(lambda match: match.group(1) + "code omitted.\n", value)
    value = _LINK.sub(lambda match: match.group(1), value)
    value = _BARE_URL.sub("a link", value)
    value = _INLINE_CODE.sub(lambda match: match.group(2), value)

    lines = []
    for raw in value.splitlines():
        line = re.sub(r"^[ \t]*#{1,6}[ \t]+", "", raw)
        line = re.sub(r"^[ \t]*(?:>[ \t]*)+", "", line)
        bullet = re.match(r"^[ \t]*(?:[-+*]|\d+[.)])[ \t]+(.*)$", line)
        if bullet:
            line = bullet.group(1).strip()
            if line and line[-1] not in ".!?।":
                line += "."
        table = _table_line(line)
        if table is not None:
            line = table
        if line:
            lines.append(line)

    value = "\n".join(lines)
    value = value.replace("**", "").replace("__", "").replace("~~", "")
    value = value.replace("*", "").replace("_", "")
    value = value.replace("📨", "").replace("⏸", "").replace("⚠️", "")
    value = _EMOJI.sub("", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value if any(char.isalnum() for char in value) else ""


def _pack_words(sentence, max_chars):
    words = sentence.split()
    if not words:
        return []
    pieces = []
    current = words[0]
    for word in words[1:]:
        candidate = current + " " + word
        if len(candidate) <= max_chars:
            current = candidate
        else:
            pieces.append(current)
            current = word
    pieces.append(current)
    return pieces


def chunk(text, max_chars):
    """Greedily pack sentences without splitting a word."""
    if max_chars <= 0:
        raise ValueError("max_chars must be positive")
    value = (text or "").strip()
    if not value:
        return []
    sentences = [part.strip() for part in re.split(
        r"(?<=[.!?।])\s+|\n+", value) if part.strip()]
    result = []
    current = ""
    for sentence in sentences:
        if len(sentence) > max_chars:
            if current:
                result.append(current)
                current = ""
            result.extend(_pack_words(sentence, max_chars))
            continue
        candidate = sentence if not current else current + " " + sentence
        if len(candidate) <= max_chars:
            current = candidate
        else:
            result.append(current)
            current = sentence
    if current:
        result.append(current)
    return result


def resolve_chain(cli_chain=None):
    raw = cli_chain if cli_chain and cli_chain.strip() else os.environ.get("C3_TTS_CHAIN", "")
    names = [name.strip() for name in raw.split(",") if name.strip()] if raw.strip() else list(DEFAULT_CHAIN)
    return list(dict.fromkeys(names))


def load_provider(name):
    path = os.path.join(PROVIDERS_DIR, name + ".py")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"provider not found: {path}")
    spec = importlib.util.spec_from_file_location(f"tts_provider_{name.replace('-', '_')}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for symbol in ("MODEL_ID", "OUTPUT_MIME", "MAX_CHARS", "available", "synthesize"):
        if not hasattr(module, symbol):
            raise ImportError(f"provider {name} missing {symbol}")
    if module.OUTPUT_MIME != "audio/mpeg":
        raise ValueError(f"provider {name} output MIME is not audio/mpeg")
    return module


def strip_leading_id3v2(audio):
    if not audio.startswith(b"ID3"):
        return audio
    if len(audio) < 10 or any(byte & 0x80 for byte in audio[6:10]):
        raise ValueError("invalid ID3v2 header")
    size = sum(byte << shift for byte, shift in zip(audio[6:10], (21, 14, 7, 0)))
    end = 10 + size
    if end > len(audio):
        raise ValueError("truncated ID3v2 tag")
    return audio[end:]


def run_chain(chain, text, language, deadline, provider_loader=load_provider,
              clock=time.monotonic, log=None):
    """Synthesize all chunks with one provider, falling back for the whole text."""
    log = log or (lambda message: print(message, file=sys.stderr))
    started = clock()
    failures = []
    for name in chain:
        try:
            provider = provider_loader(name)
        except Exception as error:
            failures.append(f"{name}: {error}")
            log(f"provider={name} load_failed={type(error).__name__}: {error}")
            continue
        try:
            reason = (provider.available() or "").strip()
        except Exception as error:
            reason = f"availability check failed: {error}"
        if reason:
            log(f"provider={name} unavailable={reason}")
            failures.append(f"{name}: {reason}")
            continue

        parts = []
        audio_parts = []
        index = 0
        try:
            parts = chunk(text, provider.MAX_CHARS)
            if not parts:
                raise ValueError("provider produced no chunks")
            for index, part in enumerate(parts, 1):
                remaining = float(deadline) - (clock() - started)
                if remaining <= 0:
                    raise TimeoutError("overall TTS deadline exhausted")
                audio = provider.synthesize(part, language, remaining)
                if index > 1:
                    audio = strip_leading_id3v2(audio)
                audio_parts.append(audio)
            joined = b"".join(audio_parts)
            if not joined:
                raise ValueError("provider returned empty audio")
        except Exception as error:
            location = f"chunk {index}/{len(parts)}" if index else "setup"
            failures.append(f"{name}: {location}: {error}")
            log(f"provider={name} {location.replace(' ', '=')} failed={type(error).__name__}: {error}")
            continue
        return joined, name, len(parts)
    detail = "; ".join(failures) or "empty provider chain"
    raise SynthesisError(f"every TTS provider failed: {detail}")
