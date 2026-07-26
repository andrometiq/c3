#!/usr/bin/env python3
"""Tests for the bake-off scoring functions and the harness's pure logic.

Run with:  python3 -m pytest plugins/c3/stt/bakeoff/test_metrics.py -q
Hermetic — no network, no API keys, no audio, no ffprobe.
"""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "stt-pkg"))

import metrics  # noqa: E402
import run_bakeoff  # noqa: E402


# ── normalization ────────────────────────────────────────────────────────────

def test_normalize_strips_tags_punctuation_and_case():
    assert metrics.normalize("[Tamil] Hello, WORLD!") == "hello world"


def test_normalize_keeps_intra_word_apostrophes_and_hyphens():
    # Splitting "don't" into two words would invent an error no listener agrees with.
    assert metrics.normalize("Don't stop -- co-pilot.") == "don't stop co-pilot"


def test_normalize_unifies_curly_and_straight_apostrophes():
    assert metrics.normalize("don’t") == metrics.normalize("don't")


def test_strip_tags_removes_only_bracketed_tags():
    assert metrics.strip_tags("a [Hindi] b [frustrated] c").split() == ["a", "b", "c"]


# ── edit distance / WER / CER ────────────────────────────────────────────────

def test_levenshtein_known_values():
    assert metrics.levenshtein("kitten", "sitting") == 3
    assert metrics.levenshtein(["a", "b"], ["a", "b"]) == 0
    assert metrics.levenshtein([], ["a", "b", "c"]) == 3


def test_wer_perfect_and_total():
    assert metrics.wer("the quick brown fox", "The quick brown fox!") == 0.0
    assert metrics.wer("a b c d", "") == 1.0


def test_wer_counts_one_substitution_in_four_words():
    assert metrics.wer("run the kubectl command", "run the cube control command") == pytest.approx(0.5)


def test_wer_is_none_without_a_reference():
    assert metrics.wer(None, "anything") is None
    assert metrics.cer(None, "anything") is None


def test_wer_empty_reference_with_output_is_one_not_a_crash():
    assert metrics.wer("", "hallucinated words here") == 1.0
    assert metrics.wer("", "") == 0.0


def test_cer_is_gentler_than_wer_on_a_near_miss():
    ref, hyp = "we deployed nginx today", "we deployed nginks today"
    assert metrics.cer(ref, hyp) < metrics.wer(ref, hyp)


# ── vocabulary scoring — the metric that matters here ────────────────────────

VOCAB = [
    {"preferred": "kubectl", "not": ["cube control", "cube CTL"], "note": ""},
    {"preferred": "Ed25519", "not": ["ED 25519"], "note": ""},
    {"preferred": "tmux", "not": ["T mux"], "note": ""},
]


def test_vocabulary_recall_against_a_reference():
    ref = "run kubectl then rotate the Ed25519 key"
    hyp = "run kubectl then rotate the ED 25519 key"
    score = metrics.vocabulary_score(hyp, VOCAB, ref)
    assert score["expected"] == 2          # tmux is not in the reference
    assert score["hits"] == 1              # kubectl survived
    assert score["missed_terms"] == ["Ed25519"]
    assert score["recall"] == pytest.approx(0.5)
    assert score["confusions"] == 1        # "ED 25519" is a known-wrong spelling
    assert score["confused_terms"] == ["ED 25519"]


def test_vocabulary_score_without_reference_counts_terms_and_misspellings():
    score = metrics.vocabulary_score("start tmux then cube control it", VOCAB)
    assert score["expected"] == 0
    assert score["hits"] == 1
    assert score["found_terms"] == ["tmux"]
    assert score["recall"] is None         # no ground truth -> no recall claim
    assert score["confusions"] == 1


def test_a_negative_that_is_genuinely_spoken_is_not_a_confusion():
    # The speaker really did say "cube control" — the reference proves it.
    score = metrics.vocabulary_score("cube control", VOCAB, "cube control")
    assert score["confusions"] == 0


def test_term_matching_is_case_insensitive_and_whole_token():
    assert metrics.count_term("KUBECTL apply", "kubectl") == 1
    assert metrics.count_term("kubectlfoo", "kubectl") == 0
    assert metrics.count_term("use kubectl.", "kubectl") == 1


def test_term_matching_handles_non_word_edges():
    # \b would never match a term ending in '+', so the matcher uses lookaround.
    assert metrics.count_term("written in C++ mostly", "C++") == 1


def test_empty_vocabulary_scores_cleanly():
    score = metrics.vocabulary_score("anything at all", [])
    assert score["expected"] == 0 and score["hits"] == 0 and score["recall"] is None


# ── code-switch reporting ────────────────────────────────────────────────────

def test_code_switch_counts_language_tags():
    r = metrics.code_switch_report("[Tamil] vanakkam [English] hello [Tamil] again")
    assert r["language_tags"]["tamil"] == 2
    assert r["language_tags"]["english"] == 1


def test_code_switch_detects_untranslated_tamil_script():
    tamil = "வணக்கம்"
    r = metrics.code_switch_report(f"we shipped it {tamil} today")
    assert r["non_latin_chars"] == len(tamil)
    assert r["scripts"]["tamil"] == len(tamil)
    assert 0 < r["non_latin_ratio"] < 1


def test_pure_english_has_no_non_latin():
    r = metrics.code_switch_report("plain english transcript")
    assert r["non_latin_chars"] == 0 and r["non_latin_ratio"] == 0.0


# ── cost ─────────────────────────────────────────────────────────────────────

def test_cost_scales_with_duration():
    assert metrics.cost_usd(120, 0.0043) == pytest.approx(0.0086)


def test_cost_is_unknown_not_zero_when_price_or_duration_is_missing():
    assert metrics.cost_usd(120, None) is None
    assert metrics.cost_usd(None, 0.0043) is None


def test_fmt_renders_none_as_a_dash():
    assert metrics.fmt(None) == "—"
    assert metrics.fmt(0.5, ".2f") == "0.50"


# ── table rendering ──────────────────────────────────────────────────────────

def test_markdown_table_has_a_header_separator_and_all_rows():
    table = metrics.markdown_table(["a", "bb"], [["1", "2"], ["3", "4"]])
    lines = table.splitlines()
    assert len(lines) == 4
    assert set(lines[1]) <= {"|", "-"}
    assert "3" in lines[3]


# ── harness logic ────────────────────────────────────────────────────────────

class _FakeProvider:
    """Minimal stand-in for a provider module."""

    MODEL_ID = "fake-1"
    COST_PER_MINUTE_USD = 0.01

    def __init__(self, text=None, raises=None):
        self._text, self._raises = text, raises
        self.vocab_calls = 0

    def set_vocabulary(self, vocab):
        self.vocab_calls += 1

    def transcribe(self, audio_path, audio_bytes):
        if self._raises:
            raise self._raises
        return self._text


def test_transcribe_one_returns_text_and_applies_vocabulary(tmp_path):
    audio = tmp_path / "a.ogg"
    audio.write_bytes(b"\x00\x01")
    mod = _FakeProvider(text="  hello  ")
    text, latency, error = run_bakeoff.transcribe_one(mod, str(audio), {"terms": []})
    assert (text, error) == ("hello", None)
    assert latency >= 0
    assert mod.vocab_calls == 1


def test_transcribe_one_reports_an_exception_instead_of_raising(tmp_path):
    audio = tmp_path / "a.ogg"
    audio.write_bytes(b"\x00")
    text, _, error = run_bakeoff.transcribe_one(
        _FakeProvider(raises=RuntimeError("boom")), str(audio), {"terms": []})
    assert text is None and "RuntimeError: boom" in error


def test_transcribe_one_treats_empty_output_as_an_error(tmp_path):
    audio = tmp_path / "a.ogg"
    audio.write_bytes(b"\x00")
    text, _, error = run_bakeoff.transcribe_one(
        _FakeProvider(text="   "), str(audio), {"terms": []})
    assert text is None and error == "empty result"


def test_transcribe_one_survives_unreadable_audio(tmp_path):
    text, _, error = run_bakeoff.transcribe_one(
        _FakeProvider(text="x"), str(tmp_path / "missing.ogg"), {"terms": []})
    assert text is None and "cannot read audio" in error


def test_find_audio_picks_audio_only_and_honours_limit(tmp_path):
    for name in ("b.ogg", "a.oga", "notes.txt", "c.mp3"):
        (tmp_path / name).write_bytes(b"x")
    found = [os.path.basename(p) for p in run_bakeoff.find_audio(str(tmp_path))]
    assert found == ["a.oga", "b.ogg", "c.mp3"]
    assert len(run_bakeoff.find_audio(str(tmp_path), limit=2)) == 2


def test_find_audio_on_a_missing_directory_returns_empty(tmp_path):
    assert run_bakeoff.find_audio(str(tmp_path / "nope")) == []


def test_find_reference_prefers_refs_dir_then_sidecars(tmp_path):
    audio_dir, refs_dir = tmp_path / "audio", tmp_path / "refs"
    audio_dir.mkdir(), refs_dir.mkdir()
    audio = audio_dir / "note.ogg"
    audio.write_bytes(b"x")

    assert run_bakeoff.find_reference(str(audio), str(refs_dir)) is None

    (audio_dir / "note.ref.txt").write_text("sidecar ref")
    assert run_bakeoff.find_reference(str(audio), str(refs_dir)) == "sidecar ref"

    (audio_dir / "note.txt").write_text("beside audio")
    assert run_bakeoff.find_reference(str(audio), str(refs_dir)) == "beside audio"

    (refs_dir / "note.txt").write_text("from refs dir")
    assert run_bakeoff.find_reference(str(audio), str(refs_dir)) == "from refs dir"


def test_find_reference_ignores_an_empty_file(tmp_path):
    audio = tmp_path / "note.ogg"
    audio.write_bytes(b"x")
    (tmp_path / "note.txt").write_text("   \n")
    assert run_bakeoff.find_reference(str(audio), None) is None


def test_summarize_aggregates_and_keeps_unknowns_unknown():
    results = [
        {"provider": "p", "file": "1.ogg", "model": "fake-1", "transcript": "ok",
         "wer": 0.2, "cer": 0.1, "latency_seconds": 3.0, "cost_usd": 0.02,
         "duration_seconds": 120,
         "vocabulary": {"expected": 4, "hits": 3, "confusions": 1},
         "code_switch": {"non_latin_chars": 2, "language_tags": {"tamil": 1}}},
        {"provider": "p", "file": "2.ogg", "model": "fake-1", "transcript": None,
         "wer": None, "cer": None, "latency_seconds": 1.0, "cost_usd": None,
         "duration_seconds": None, "vocabulary": None, "code_switch": None},
    ]
    (s,) = run_bakeoff.summarize(results, [("p", _FakeProvider())])
    assert (s["files"], s["ok"], s["errors"]) == (2, 1, 1)
    assert s["wer"] == pytest.approx(0.2)          # the failed file isn't averaged in
    assert s["vocab_recall"] == pytest.approx(0.75)
    assert s["confusions"] == 1 and s["non_latin"] == 2 and s["lang_tags"] == 1
    assert s["cost_per_minute"] == 0.01
    assert s["total_cost"] == pytest.approx(0.02)


def test_summarize_reports_none_when_nothing_scored():
    results = [{"provider": "p", "file": "1.ogg", "model": None, "transcript": None,
                "wer": None, "cer": None, "latency_seconds": 0.5, "cost_usd": None,
                "duration_seconds": None, "vocabulary": None, "code_switch": None}]
    (s,) = run_bakeoff.summarize(results, [("p", _FakeProvider())])
    assert s["wer"] is None and s["vocab_recall"] is None
    assert s["total_cost"] is None and s["median_latency"] is None


def test_report_says_plainly_that_nothing_was_ranked_without_references():
    results = [{"provider": "p", "file": "1.ogg", "model": "fake-1",
                "transcript": "hello there", "wer": None, "cer": None, "error": None,
                "latency_seconds": 2.0, "cost_usd": None, "duration_seconds": 60,
                "vocabulary": {"expected": 0, "hits": 1, "confusions": 0},
                "code_switch": {"non_latin_chars": 0, "language_tags": {}}}]
    summary = run_bakeoff.summarize(results, [("p", _FakeProvider())])
    report = run_bakeoff.build_report(summary, results, {
        "timestamp": "now", "audio_dir": "/corpus", "file_count": 1,
        "reference_count": 0, "total_seconds": 60, "term_count": 3,
        "vocab_source": "/v.txt", "ran": ["p"], "skipped": [("q", "KEY not set")],
    })
    assert "nothing here is a ranking" in report
    assert "q (KEY not set)" in report
    assert "hello there" in report        # transcripts are still comparable


def test_report_calls_scores_measured_when_references_exist():
    results = [{"provider": "p", "file": "1.ogg", "model": "fake-1",
                "transcript": "hello", "wer": 0.1, "cer": 0.05, "error": None,
                "latency_seconds": 2.0, "cost_usd": 0.01, "duration_seconds": 60,
                "vocabulary": {"expected": 2, "hits": 2, "confusions": 0},
                "code_switch": {"non_latin_chars": 0, "language_tags": {}}}]
    summary = run_bakeoff.summarize(results, [("p", _FakeProvider())])
    report = run_bakeoff.build_report(summary, results, {
        "timestamp": "now", "audio_dir": "/corpus", "file_count": 1,
        "reference_count": 1, "total_seconds": 60, "term_count": 3,
        "vocab_source": "/v.txt", "ran": ["p"], "skipped": [],
    })
    assert "**measured** on this corpus" in report


def test_select_providers_skips_the_unkeyed_and_the_unknown():
    runnable, skipped = run_bakeoff.select_providers("no-such-provider")
    assert runnable == []
    assert skipped and skipped[0][0] == "no-such-provider"


def test_select_providers_reports_every_installed_provider():
    runnable, skipped = run_bakeoff.select_providers("")
    names = [n for n, _ in runnable] + [n for n, _ in skipped]
    # The shipped chain must all be discoverable, keyed or not.
    for expected in ("gemini-3-flash-openrouter", "soniox-stt-async-v5",
                     "elevenlabs-scribe-v2", "sarvam-saaras-v3"):
        assert expected in names
