#!/usr/bin/env python3
"""Scoring for the STT bake-off. Pure functions, stdlib only, no I/O.

Everything here is deterministic and unit-tested (test_metrics.py), so the
numbers in a bake-off report can be audited without re-running any API.

The metric set is chosen for THIS workload rather than for a leaderboard.
Overall WER is the number everyone quotes and the least useful one here: a
transcript can score a respectable WER while mangling every product name in it,
and the mangled product names are the whole reason the transcript exists. So
vocabulary accuracy is a first-class metric, scored directly against the terms
in vocabulary.txt, and code-switch behaviour is reported separately because a
provider that hands back untranslated Tamil script is failing at something WER
cannot see.
"""
import re
import unicodedata

# Bracketed tags the providers emit for language switches and tone —
# "[Tamil]", "[frustrated]". They are metadata, not spoken words, so they are
# stripped before WER/CER and counted separately by code_switch_report.
_TAG_RE = re.compile(r"\[[^\]\n]{1,40}\]")

# Unicode ranges for the scripts that show up in this corpus when a provider
# transcribes verbatim instead of translating to English.
_SCRIPT_RANGES = (
    ("tamil", 0x0B80, 0x0BFF),
    ("devanagari", 0x0900, 0x097F),
    ("telugu", 0x0C00, 0x0C7F),
    ("kannada", 0x0C80, 0x0CFF),
    ("malayalam", 0x0D00, 0x0D7F),
    ("bengali", 0x0980, 0x09FF),
    ("gurmukhi", 0x0A00, 0x0A7F),
    ("gujarati", 0x0A80, 0x0AFF),
)


def strip_tags(text: str) -> str:
    """Remove [Language] / [emotion] tags. They aren't spoken words."""
    return _TAG_RE.sub(" ", text or "")


# Typographic variants that mean the same thing as the ASCII character. NFKC
# does NOT fold these, so a provider that emits a curly apostrophe would score
# "don’t" as a different word from another provider's "don't" — a pure
# formatting difference showing up as a word error.
_PUNCT_FOLD = str.maketrans({
    "‘": "'", "’": "'", "ʼ": "'", "´": "'", "`": "'",
    "‐": "-", "‑": "-", "‒": "-", "–": "-", "—": "-",
    "―": "-",
})


def normalize(text: str) -> str:
    """Casefold, drop tags and punctuation, collapse whitespace.

    Keeps intra-word apostrophes and hyphens ("don't", "co-pilot") because
    splitting those invents word errors that no listener would agree with.
    Unicode is NFKC-normalized and typographic quotes/dashes are folded to
    ASCII first, so one provider's curly apostrophe doesn't score as a
    different word from another's straight one."""
    text = unicodedata.normalize("NFKC", strip_tags(text or "")).translate(_PUNCT_FOLD).casefold()
    text = re.sub(r"[^\w\s'\-]", " ", text)
    text = re.sub(r"(?<!\w)['\-]|['\-](?!\w)", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def words(text: str) -> list:
    """Normalized word list — the unit WER is measured in."""
    n = normalize(text)
    return n.split() if n else []


def levenshtein(a, b) -> int:
    """Edit distance between two sequences (lists or strings).

    Two-row dynamic programming: O(len(a)*len(b)) time, O(min) extra space —
    fine for voice notes, which top out in the low thousands of words."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(
                prev[j] + 1,        # deletion
                cur[j - 1] + 1,     # insertion
                prev[j - 1] + (ca != cb),  # substitution
            ))
        prev = cur
    return prev[-1]


def wer(reference: str, hypothesis: str):
    """Word error rate in [0, inf). None when there is no reference to score.

    An empty reference with a non-empty hypothesis is 1.0 (everything the
    provider produced is an insertion), not a divide-by-zero."""
    if reference is None:
        return None
    ref, hyp = words(reference), words(hypothesis)
    if not ref:
        return 0.0 if not hyp else 1.0
    return levenshtein(ref, hyp) / len(ref)


def cer(reference: str, hypothesis: str):
    """Character error rate over normalized text. None without a reference.

    Worth reporting next to WER because it degrades gracefully: a provider that
    spells a name almost right scores badly on WER (whole word wrong) but nearly
    fine on CER, and the gap between the two is itself informative."""
    if reference is None:
        return None
    ref, hyp = normalize(reference), normalize(hypothesis)
    if not ref:
        return 0.0 if not hyp else 1.0
    return levenshtein(ref, hyp) / len(ref)


def _term_pattern(term: str):
    """Case-insensitive whole-token matcher for a vocabulary term.

    Uses lookaround rather than \\b so terms that start or end with a non-word
    character still match ("C++", ".env"), which \\b would silently never do."""
    return re.compile(r"(?<!\w)" + re.escape(term) + r"(?!\w)", re.IGNORECASE)


def count_term(text: str, term: str) -> int:
    """How many times `term` appears in `text`, case-insensitively."""
    if not text or not term:
        return 0
    return len(_term_pattern(term).findall(text))


def vocabulary_score(hypothesis: str, terms: list, reference: str = None) -> dict:
    """Score a transcript against the vocabulary file's terms.

    This is the metric that actually matters for this corpus: the failure being
    chased is proper nouns and jargon, not average word accuracy.

    `terms` is the list stt.py's load_vocabulary() produces:
        [{"preferred": str, "not": [str, ...], "note": str}, ...]

    WITH a reference, a term is "expected" when the reference contains it, and
    recall is hits/expected — the honest measure. WITHOUT a reference (the case
    until the corpus arrives), there is no ground truth, so it reports what can
    still be compared side by side: how many vocabulary terms each provider
    produced, and how many known misspellings it produced. A provider emitting
    "cube control" where another emits "kubectl" is visible without any
    reference at all.

    `confusions` counts occurrences of a term's known-wrong alternatives that
    are NOT themselves in the reference — the "Vel != whale" negatives. Those
    are the errors a user notices.
    """
    expected = hits = misses = confusions = 0
    found_terms, missed_terms, confused_terms = [], [], []

    for t in terms or []:
        preferred = (t.get("preferred") or "").strip()
        if not preferred:
            continue
        in_hyp = count_term(hypothesis, preferred)

        if reference is not None:
            in_ref = count_term(reference, preferred)
            if in_ref:
                expected += 1
                if in_hyp:
                    hits += 1
                    found_terms.append(preferred)
                else:
                    misses += 1
                    missed_terms.append(preferred)
        elif in_hyp:
            hits += 1
            found_terms.append(preferred)

        for alt in t.get("not") or []:
            alt = (alt or "").strip()
            if not alt:
                continue
            # A negative that legitimately appears in the reference is not an
            # error — the speaker really did say it.
            if reference is not None and count_term(reference, alt):
                continue
            n = count_term(hypothesis, alt)
            if n:
                confusions += n
                confused_terms.append(alt)

    recall = (hits / expected) if expected else None
    return {
        "expected": expected,
        "hits": hits,
        "misses": misses,
        "recall": recall,
        "confusions": confusions,
        "found_terms": found_terms,
        "missed_terms": missed_terms,
        "confused_terms": confused_terms,
    }


def code_switch_report(text: str) -> dict:
    """Describe how a transcript handled language switching.

    Three signals, none of which WER can see:
      * `language_tags`  — [Tamil]/[Hindi]/[English] markers, counted by name.
        The chain's prompt asks for these, so their presence says the provider
        noticed a switch at all.
      * `non_latin_chars` / `scripts` — characters left in an Indic script.
        Non-zero means the provider transcribed verbatim instead of translating
        to English, which is a different product, not a better or worse WER.
      * `non_latin_ratio` — those characters as a share of all non-space
        characters, so a stray character isn't read as a wholesale failure.
    """
    text = text or ""
    tags = {}
    for match in _TAG_RE.findall(text):
        name = match.strip("[]").strip().lower()
        tags[name] = tags.get(name, 0) + 1

    scripts, non_latin = {}, 0
    for ch in strip_tags(text):
        cp = ord(ch)
        for name, lo, hi in _SCRIPT_RANGES:
            if lo <= cp <= hi:
                scripts[name] = scripts.get(name, 0) + 1
                non_latin += 1
                break

    dense = len([c for c in strip_tags(text) if not c.isspace()])
    return {
        "language_tags": tags,
        "non_latin_chars": non_latin,
        "scripts": scripts,
        "non_latin_ratio": (non_latin / dense) if dense else 0.0,
    }


def cost_usd(duration_seconds, cost_per_minute):
    """Estimated USD for one transcription. None when either input is unknown.

    Returning None rather than 0.0 for an unpriced provider keeps "free" and
    "we don't know" from looking identical in the report."""
    if duration_seconds is None or cost_per_minute is None:
        return None
    return (duration_seconds / 60.0) * cost_per_minute


def fmt(value, spec=".3f", dash="—"):
    """Format a number for a report cell, or `dash` when it is None."""
    if value is None:
        return dash
    return format(value, spec)


def markdown_table(headers: list, rows: list) -> str:
    """Render a markdown table with columns padded to their widest cell."""
    cells = [[str(h) for h in headers]] + [[str(c) for c in r] for r in rows]
    widths = [max(len(row[i]) for row in cells) for i in range(len(headers))]
    out = ["| " + " | ".join(h.ljust(widths[i]) for i, h in enumerate(cells[0])) + " |",
           "|" + "|".join("-" * (w + 2) for w in widths) + "|"]
    for row in cells[1:]:
        out.append("| " + " | ".join(c.ljust(widths[i]) for i, c in enumerate(row)) + " |")
    return "\n".join(out)
