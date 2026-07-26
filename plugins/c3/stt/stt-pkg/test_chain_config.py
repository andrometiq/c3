#!/usr/bin/env python3
"""Tests for the ordered provider chain: where the order comes from, when a
provider is skipped, and how fallback behaves.

Run with:  python3 -m pytest plugins/c3/stt/stt-pkg/test_chain_config.py -q
Hermetic — no network, no API keys, no audio. Providers are fakes.
"""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import stt  # noqa: E402  — main() is __main__-guarded, so importing is inert


@pytest.fixture(autouse=True)
def _clear_chain_env(monkeypatch):
    """Every test starts with no $C3_STT_CHAIN, whatever the shell had."""
    monkeypatch.delenv("C3_STT_CHAIN", raising=False)


# ── where the order is configured ────────────────────────────────────────────

def test_default_chain_is_used_when_nothing_is_configured():
    assert stt._resolve_chain() == stt.DEFAULT_CHAIN.split(",")


def test_default_chain_is_the_survey_order_top_three_then_the_incumbent():
    # The shipped order is a decision, not an accident: three ranked providers
    # and the incumbent Indic fallback last. Pin it so a reshuffle is deliberate.
    assert stt.DEFAULT_CHAIN.split(",") == [
        "gemini-3-flash-openrouter",
        "soniox-stt-async-v5",
        "elevenlabs-scribe-v2",
        "sarvam-saaras-v3",
    ]


def test_env_overrides_the_default(monkeypatch):
    monkeypatch.setenv("C3_STT_CHAIN", "elevenlabs-scribe-v2,sarvam-saaras-v3")
    assert stt._resolve_chain() == ["elevenlabs-scribe-v2", "sarvam-saaras-v3"]


def test_cli_overrides_the_env(monkeypatch):
    monkeypatch.setenv("C3_STT_CHAIN", "sarvam-saaras-v3")
    assert stt._resolve_chain("soniox-stt-async-v5") == ["soniox-stt-async-v5"]


def test_blank_values_fall_through_to_the_next_source(monkeypatch):
    monkeypatch.setenv("C3_STT_CHAIN", "   ")
    assert stt._resolve_chain("") == stt.DEFAULT_CHAIN.split(",")


def test_whitespace_and_empty_entries_are_tolerated():
    assert stt._resolve_chain(" a , , b ,") == ["a", "b"]


def test_duplicates_are_dropped_keeping_the_first_position():
    # A duplicate would otherwise make one provider burn its retries twice.
    assert stt._resolve_chain("a,b,a") == ["a", "b"]


def test_order_is_preserved_exactly_as_written():
    assert stt._resolve_chain("c,a,b") == ["c", "a", "b"]


# ── availability hook ────────────────────────────────────────────────────────

class _Provider:
    """Fake provider module. `outcomes` is consumed one entry per attempt:
    a string is returned, an exception instance is raised."""

    def __init__(self, outcomes=None, unavailable=None, has_available=True):
        self.outcomes = list(outcomes or [])
        self.calls = 0
        self.vocab_calls = 0
        self._unavailable = unavailable
        if not has_available:
            del self.available

    def available(self):
        return self._unavailable or ""

    def set_vocabulary(self, vocab):
        self.vocab_calls += 1

    def transcribe(self, audio_path, audio_bytes):
        self.calls += 1
        outcome = self.outcomes.pop(0) if self.outcomes else None
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class _NoHookProvider:
    """A v0.1.0-era provider: transcribe() only, no available(), no vocabulary."""

    def __init__(self, text=None):
        self.text = text
        self.calls = 0

    def transcribe(self, audio_path, audio_bytes):
        self.calls += 1
        return self.text


def test_provider_without_the_hook_is_always_attempted():
    assert stt._provider_unavailable_reason(_NoHookProvider()) == ""


def test_hook_reason_is_reported():
    assert stt._provider_unavailable_reason(_Provider(unavailable="KEY not set")) == "KEY not set"


def test_hook_returning_empty_means_available():
    assert stt._provider_unavailable_reason(_Provider()) == ""


def test_a_hook_that_raises_never_removes_the_provider():
    class Broken:
        def available(self):
            raise ValueError("bug in the hook")

        def transcribe(self, audio_path, audio_bytes):
            return "x"

    assert stt._provider_unavailable_reason(Broken()) == ""


# ── ordering and fallback ────────────────────────────────────────────────────

def _run(providers, **kw):
    """run_chain with sleeps disabled and logging captured."""
    log = []
    kw.setdefault("retry_delay", 0)
    result = stt.run_chain(providers, "/tmp/x.ogg", b"", {"terms": []},
                           log=log.append, **kw)
    return result, log


def test_first_provider_wins_and_later_ones_never_run():
    first, second = _Provider(["from first"]), _Provider(["from second"])
    result, _ = _run([("first", first), ("second", second)])
    assert result == "from first"
    assert second.calls == 0


def test_transcript_is_stripped():
    result, _ = _run([("p", _Provider(["  padded  "]))])
    assert result == "padded"


def test_empty_result_exhausts_the_provider_then_falls_through():
    first = _Provider(["", "   "])          # 1 attempt + 1 retry, both empty
    second = _Provider(["rescued"])
    result, log = _run([("first", first), ("second", second)])
    assert result == "rescued"
    assert first.calls == 2 and second.calls == 1
    assert any("falling back to second" in m for m in log)


def test_an_exception_falls_through_the_same_way_as_an_empty_result():
    first = _Provider([RuntimeError("boom"), RuntimeError("boom")])
    result, log = _run([("first", first), ("second", _Provider(["ok"]))])
    assert result == "ok"
    assert any("RuntimeError: boom" in m for m in log)


def test_retries_are_honoured_before_falling_through():
    first = _Provider(["", "", "third time lucky"])
    result, _ = _run([("first", first), ("second", _Provider(["fallback"]))], retries=2)
    assert result == "third time lucky"
    assert first.calls == 3


def test_all_providers_failing_returns_none():
    result, log = _run([("a", _Provider([""])), ("b", _Provider([RuntimeError("x")]))])
    assert result is None
    assert any("falling back to b" in m for m in log)


def test_unavailable_providers_are_skipped_without_burning_attempts():
    unkeyed = _Provider(["never reached"], unavailable="SONIOX_API_KEY not set")
    keyed = _Provider(["from the keyed one"])
    result, log = _run([("unkeyed", unkeyed), ("keyed", keyed)])
    assert result == "from the keyed one"
    assert unkeyed.calls == 0                       # no attempt, no retry sleep
    assert any("skipping unkeyed: SONIOX_API_KEY not set" in m for m in log)


def test_every_provider_unavailable_returns_none_with_a_clear_message():
    result, log = _run([("a", _Provider(unavailable="no key")),
                        ("b", _Provider(unavailable="no key either"))])
    assert result is None
    assert any("no runnable providers" in m for m in log)


def test_empty_provider_list_returns_none():
    result, _ = _run([])
    assert result is None


def test_vocabulary_is_reapplied_before_every_attempt():
    p = _Provider(["", "second try"])
    _run([("p", p)])
    assert p.vocab_calls == 2


def test_a_provider_without_set_vocabulary_still_runs():
    legacy = _NoHookProvider(text="legacy output")
    result, _ = _run([("legacy", legacy)])
    assert result == "legacy output" and legacy.calls == 1


# ── discovery ────────────────────────────────────────────────────────────────

def test_list_providers_finds_the_shipped_chain():
    found = {name for name, mod in stt.list_providers() if mod is not None}
    for name in stt.DEFAULT_CHAIN.split(","):
        assert name in found, f"{name} is in the default chain but not installed"


def test_every_shipped_provider_satisfies_the_contract():
    for name, mod in stt.list_providers():
        assert mod is not None, f"{name} failed to load"
        assert callable(getattr(mod, "transcribe", None)), f"{name} has no transcribe()"
        hook = getattr(mod, "available", None)
        if hook is not None:
            assert isinstance(hook(), str), f"{name}.available() must return a string"
