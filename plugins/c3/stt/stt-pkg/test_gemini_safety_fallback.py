#!/usr/bin/env python3
"""Hermetic test for providers/gemini-3-flash-openrouter.py safety fallback.

Gemini can answer an ordinary voice note with no text and a SAFETY finish
reason. The provider must then ask the fallback model once, return its
transcript, and raise (not return "no speech") if that model blocks too.

Stdlib only, no network: urllib.request.urlopen is replaced with a fake that
records each request's model and replays canned OpenRouter responses.

Runnable as:  python3 test_gemini_safety_fallback.py   (or under pytest)
"""
import importlib.util
import io
import json
import os
import sys

PROVIDER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "providers", "gemini-3-flash-openrouter.py")


PRIMARY, FALLBACK = "test/primary", "test/fallback"


def _load_provider():
    os.environ["C3_STT_GEMINI_MODEL"] = PRIMARY
    os.environ["C3_STT_GEMINI_FALLBACK_MODEL"] = FALLBACK
    spec = importlib.util.spec_from_file_location("gemini_or", PROVIDER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    os.environ.pop("C3_STT_GEMINI_MODEL")
    os.environ.pop("C3_STT_GEMINI_FALLBACK_MODEL")
    mod._OR_KEY = "test-key"
    return mod


def _choice(content, finish="stop", native="STOP"):
    return {"choices": [{"message": {"role": "assistant", "content": content},
                         "finish_reason": finish, "native_finish_reason": native}]}


BLOCKED = _choice(None, "content_filter", "SAFETY")


def _run(responses):
    """transcribe() against canned responses; returns (result, models asked)."""
    prov = _load_provider()
    asked = []
    queue = list(responses)

    def fake_urlopen(req, timeout=None):
        asked.append(json.loads(req.data)["model"])
        return io.BytesIO(json.dumps(queue.pop(0)).encode())

    prov.urllib.request.urlopen = fake_urlopen
    try:
        return prov.transcribe("note.ogg", b"OggS"), asked, prov
    except RuntimeError as e:
        return e, asked, prov


def test_safety_block_asks_fallback_once():
    result, asked, prov = _run([BLOCKED, _choice("hello there")])
    assert asked == [PRIMARY, FALLBACK], asked
    assert result == "hello there", result
    print("PASS test_safety_block_asks_fallback_once")


def test_normal_answer_makes_one_request():
    result, asked, prov = _run([_choice("hello there")])
    assert asked == [PRIMARY], asked
    assert result == "hello there", result
    print("PASS test_normal_answer_makes_one_request")


def test_fallback_also_blocked_raises():
    result, asked, prov = _run([BLOCKED, BLOCKED])
    assert asked == [PRIMARY, FALLBACK], asked
    assert isinstance(result, RuntimeError), f"expected a raise, got {result!r}"
    assert "safety" in str(result) and FALLBACK in str(result), result
    print("PASS test_fallback_also_blocked_raises")


def test_normalised_reason_alone_counts_as_block():
    only_normalised = {"choices": [{"message": {"content": ""},
                                    "finish_reason": "content_filter"}]}
    result, asked, prov = _run([only_normalised, _choice("hi")])
    assert asked == [PRIMARY, FALLBACK], asked
    assert result == "hi", result
    print("PASS test_normalised_reason_alone_counts_as_block")


def test_no_speech_is_not_a_block():
    result, asked, prov = _run([_choice("<<NO_SPEECH>>")])
    assert asked == [PRIMARY], asked
    assert result is None, result
    print("PASS test_no_speech_is_not_a_block")


if __name__ == "__main__":
    failed = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
            except Exception as e:
                failed += 1
                print(f"FAIL {name}: {type(e).__name__}: {e}")
    sys.exit(1 if failed else 0)
