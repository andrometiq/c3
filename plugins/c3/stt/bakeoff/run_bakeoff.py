#!/usr/bin/env python3
"""STT bake-off — run every installed provider over a corpus and compare them.

    python3 run_bakeoff.py --audio-dir /path/to/voice-notes
    python3 run_bakeoff.py --audio-dir ./corpus --refs ./corpus/refs
    python3 run_bakeoff.py --audio-dir ./corpus --dry-run

Reads the SAME providers and the SAME vocabulary the live chain uses (it
imports ../stt-pkg/stt.py rather than keeping its own copy), so a provider that
wins here is the provider that runs in production — no second implementation to
drift.

Writes to --out (default ./results/<timestamp>/):
    report.md                     the comparison tables
    results.json                  every measurement, machine-readable
    transcripts/<provider>/<file>.txt   raw output, for reading side by side

Reference transcripts are OPTIONAL. With them you get WER/CER and true
vocabulary recall. Without them you still get latency, cost, vocabulary-term
counts, code-switch behaviour and the transcripts themselves side by side —
enough to compare providers qualitatively before ground truth exists.

Providers whose API key is not configured are SKIPPED with a printed reason.
Running with no keys at all is a supported, non-crashing no-op.

Nothing here prints, logs, or stores an API key.
"""
import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
STT_PKG_DIR = os.path.join(os.path.dirname(HERE), "stt-pkg")
sys.path.insert(0, STT_PKG_DIR)
sys.path.insert(0, HERE)

import metrics  # noqa: E402
import stt      # noqa: E402  — the live chain module; main() is __main__-guarded

AUDIO_EXTS = (".oga", ".ogg", ".opus", ".mp3", ".m4a", ".wav", ".flac", ".aac", ".webm", ".mp4")


# ── corpus discovery ─────────────────────────────────────────────────────────

def find_audio(audio_dir, limit=0):
    """Audio files in `audio_dir`, sorted by name. Non-recursive on purpose —
    a corpus is a flat drop-in directory, and recursing would sweep up refs."""
    try:
        names = sorted(f for f in os.listdir(audio_dir)
                       if f.lower().endswith(AUDIO_EXTS))
    except OSError as e:
        print(f"ERROR: cannot read --audio-dir {audio_dir}: {e}", file=sys.stderr)
        return []
    paths = [os.path.join(audio_dir, n) for n in names]
    return paths[:limit] if limit and limit > 0 else paths


def find_reference(audio_path, refs_dir):
    """Reference transcript for one audio file, or None.

    Looked for, in order: <refs_dir>/<stem>.txt, then <stem>.txt and
    <stem>.ref.txt beside the audio. Any of the three works so a corpus can
    arrive either as one folder or as two."""
    stem = os.path.splitext(os.path.basename(audio_path))[0]
    candidates = []
    if refs_dir:
        candidates.append(os.path.join(refs_dir, stem + ".txt"))
    audio_dir = os.path.dirname(audio_path)
    candidates.append(os.path.join(audio_dir, stem + ".txt"))
    candidates.append(os.path.join(audio_dir, stem + ".ref.txt"))
    for path in candidates:
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    text = f.read().strip()
                if text:
                    return text
            except OSError:
                continue
    return None


def audio_duration_seconds(path):
    """Duration via ffprobe, or None when ffprobe is missing/fails.

    None propagates into the cost column as "unknown" rather than a wrong
    number — the same discipline the providers use for unpriced models."""
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=20,
        )
        return float(out.stdout.strip())
    except Exception:
        return None


# ── provider selection ───────────────────────────────────────────────────────

def select_providers(requested):
    """Resolve which providers to run.

    Returns (runnable, skipped) where runnable is [(name, module)] and skipped
    is [(name, reason)]. `requested` is a comma-separated list; empty means
    every provider installed in providers/. Ordering for an explicit request is
    the order given; otherwise filename order."""
    runnable, skipped = [], []
    if requested and requested.strip():
        names = [n.strip() for n in requested.split(",") if n.strip()]
        pairs = [(n, stt.load_provider(n)) for n in names]
    else:
        pairs = stt.list_providers()

    for name, mod in pairs:
        if mod is None:
            skipped.append((name, "not installed, or missing transcribe()"))
            continue
        reason = stt._provider_unavailable_reason(mod)
        if reason:
            skipped.append((name, reason))
            continue
        runnable.append((name, mod))
    return runnable, skipped


# ── the run ──────────────────────────────────────────────────────────────────

def transcribe_one(mod, audio_path, vocab):
    """One provider on one file. Returns (text, latency_s, error_or_None).

    Never raises: a provider blowing up is a measurement, not a crash of the
    harness — the other providers still need to run."""
    try:
        with open(audio_path, "rb") as f:
            audio_bytes = f.read()
    except OSError as e:
        return None, 0.0, f"cannot read audio: {e}"

    started = time.monotonic()
    try:
        if hasattr(mod, "set_vocabulary"):
            mod.set_vocabulary(vocab)
        text = mod.transcribe(audio_path, audio_bytes)
        latency = time.monotonic() - started
        if not text or not text.strip():
            return None, latency, "empty result"
        return text.strip(), latency, None
    except Exception as e:
        latency = time.monotonic() - started
        return None, latency, f"{type(e).__name__}: {e}"[:300]


def run(audio_files, providers, vocab, refs_dir, out_dir):
    """Run every provider over every file; return the results structure."""
    terms = vocab.get("terms", [])
    results = []

    for audio_path in audio_files:
        name = os.path.basename(audio_path)
        duration = audio_duration_seconds(audio_path)
        reference = find_reference(audio_path, refs_dir)
        print(f"\n{name}  ({metrics.fmt(duration, '.1f')}s"
              f"{', reference present' if reference else ', no reference'})")

        for provider_name, mod in providers:
            text, latency, error = transcribe_one(mod, audio_path, vocab)
            entry = {
                "file": name,
                "provider": provider_name,
                "model": getattr(mod, "MODEL_ID", "") or None,
                "duration_seconds": duration,
                "latency_seconds": round(latency, 2),
                "error": error,
                "transcript": text,
                "has_reference": reference is not None,
                "wer": None, "cer": None,
                "vocabulary": None,
                "code_switch": None,
                "cost_usd": metrics.cost_usd(
                    duration, getattr(mod, "COST_PER_MINUTE_USD", None)),
            }
            if text:
                entry["wer"] = metrics.wer(reference, text) if reference else None
                entry["cer"] = metrics.cer(reference, text) if reference else None
                entry["vocabulary"] = metrics.vocabulary_score(text, terms, reference)
                entry["code_switch"] = metrics.code_switch_report(text)
                _write_transcript(out_dir, provider_name, name, text)
            results.append(entry)

            status = error if error else (
                f"{len(text.split())} words, "
                f"WER {metrics.fmt(entry['wer'], '.3f')}" if reference else
                f"{len(text.split())} words")
            print(f"  {provider_name:<28} {latency:6.1f}s  {status}")
    return results


def _write_transcript(out_dir, provider, audio_name, text):
    """Save raw output so transcripts can be read side by side."""
    folder = os.path.join(out_dir, "transcripts", provider)
    os.makedirs(folder, exist_ok=True)
    stem = os.path.splitext(audio_name)[0]
    with open(os.path.join(folder, stem + ".txt"), "w", encoding="utf-8") as f:
        f.write(text + "\n")


# ── reporting ────────────────────────────────────────────────────────────────

def summarize(results, providers):
    """Aggregate per-provider rows from the per-(file,provider) results.

    `providers` is the [(name, module)] list that was run — the module is read
    for its published price, so the $/min column is still filled in when
    ffprobe couldn't measure a duration."""
    summary = []
    for name, mod in providers:
        rows = [r for r in results if r["provider"] == name]
        ok = [r for r in rows if r["transcript"]]
        scored = [r for r in ok if r["wer"] is not None]
        vocab_rows = [r for r in ok if r["vocabulary"]]

        expected = sum(r["vocabulary"]["expected"] for r in vocab_rows)
        hits = sum(r["vocabulary"]["hits"] for r in vocab_rows)
        summary.append({
            "provider": name,
            "model": next((r["model"] for r in rows if r["model"]), None),
            "files": len(rows),
            "ok": len(ok),
            "errors": len(rows) - len(ok),
            "wer": statistics.mean(r["wer"] for r in scored) if scored else None,
            "cer": statistics.mean(r["cer"] for r in scored) if scored else None,
            "vocab_expected": expected,
            "vocab_hits": hits,
            "vocab_recall": (hits / expected) if expected else None,
            "confusions": sum(r["vocabulary"]["confusions"] for r in vocab_rows),
            "non_latin": sum(r["code_switch"]["non_latin_chars"] for r in ok if r["code_switch"]),
            "lang_tags": sum(sum(r["code_switch"]["language_tags"].values())
                             for r in ok if r["code_switch"]),
            "median_latency": statistics.median(r["latency_seconds"] for r in ok) if ok else None,
            "cost_per_minute": getattr(mod, "COST_PER_MINUTE_USD", None),
            "total_cost": _sum_or_none(r["cost_usd"] for r in rows),
        })
    return summary


def _sum_or_none(values):
    """Sum the non-None values, or None when there were none.

    A genuine 0.0 and "no priced measurements" must not render the same."""
    known = [v for v in values if v is not None]
    return sum(known) if known else None


def build_report(summary, results, context):
    """Render report.md. States plainly what was measured and what was not."""
    scored_any = any(s["wer"] is not None for s in summary)
    lines = [
        "# STT bake-off report",
        "",
        f"- Run: {context['timestamp']}",
        f"- Corpus: `{context['audio_dir']}` — {context['file_count']} file(s), "
        f"{metrics.fmt(context['total_seconds'] / 60.0, '.1f')} min total",
        f"- References: {context['reference_count']} of {context['file_count']} files",
        f"- Vocabulary: {context['term_count']} terms from `{context['vocab_source'] or 'none'}`",
        f"- Providers run: {', '.join(context['ran']) or 'none'}",
    ]
    if context["skipped"]:
        lines.append("- Providers skipped: " + "; ".join(
            f"{n} ({r})" for n, r in context["skipped"]))
    lines += ["", "## Ranking basis", ""]
    if scored_any:
        lines += [
            "WER/CER below are **measured** on this corpus against the supplied "
            "reference transcripts. Vocabulary recall is measured against the "
            "terms that actually occur in those references.",
        ]
    else:
        lines += [
            "**No reference transcripts were supplied, so nothing here is a "
            "ranking.** WER, CER and vocabulary recall are blank by design "
            "rather than estimated. What IS measured: latency, cost, how many "
            "vocabulary terms each provider produced, how many known "
            "misspellings it produced, and the transcripts themselves — read "
            "them side by side in `transcripts/`.",
        ]
    lines += ["", "## Providers", ""]

    headers = ["provider", "model", "ok/total", "WER", "CER", "vocab recall",
               "vocab hits", "confusions", "non-latin", "median s", "$/min", "est $"]
    rows = []
    for s in summary:
        rows.append([
            s["provider"],
            s["model"] or "—",
            f"{s['ok']}/{s['files']}",
            metrics.fmt(s["wer"]),
            metrics.fmt(s["cer"]),
            metrics.fmt(s["vocab_recall"]),
            f"{s['vocab_hits']}/{s['vocab_expected']}" if s["vocab_expected"] else str(s["vocab_hits"]),
            str(s["confusions"]),
            str(s["non_latin"]),
            metrics.fmt(s["median_latency"], ".1f"),
            metrics.fmt(s["cost_per_minute"], ".5f"),
            metrics.fmt(s["total_cost"], ".4f"),
        ])
    lines += [metrics.markdown_table(headers, rows), ""]
    lines += [
        "Column notes: **vocab recall** = share of vocabulary terms present in "
        "the reference that the provider also produced (blank without "
        "references; `vocab hits` then counts terms produced at all). "
        "**confusions** = occurrences of a term's known-wrong spellings from "
        "`vocabulary.txt` (`Vel != whale, well`) — the errors a reader "
        "notices. **non-latin** = characters left in an Indic script, i.e. "
        "speech transcribed verbatim instead of translated to English. "
        "**$/min** is the provider's published list price, not a measured bill.",
        "",
        "## Per file",
        "",
    ]

    for file_name in dict.fromkeys(r["file"] for r in results):
        rows_f = [r for r in results if r["file"] == file_name]
        dur = next((r["duration_seconds"] for r in rows_f), None)
        lines += [f"### {file_name}  ({metrics.fmt(dur, '.1f')}s)", ""]
        frows = []
        for r in rows_f:
            v = r["vocabulary"] or {}
            frows.append([
                r["provider"],
                metrics.fmt(r["wer"]),
                metrics.fmt(r["cer"]),
                str(v.get("hits", "—")),
                str(v.get("confusions", "—")),
                str((r["code_switch"] or {}).get("non_latin_chars", "—")),
                metrics.fmt(r["latency_seconds"], ".1f"),
                (r.get("error") or "ok")[:60],
            ])
        lines += [metrics.markdown_table(
            ["provider", "WER", "CER", "vocab hits", "confusions",
             "non-latin", "latency s", "status"], frows), ""]
        for r in rows_f:
            if r["transcript"]:
                excerpt = r["transcript"][:600]
                ellipsis = "…" if len(r["transcript"]) > 600 else ""
                lines += [f"<details><summary>{r['provider']}</summary>", "",
                          "```", excerpt + ellipsis, "```", "", "</details>", ""]
    return "\n".join(lines) + "\n"


# ── entry point ──────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Compare STT providers over a corpus of voice notes.")
    p.add_argument("--audio-dir", help="Directory of audio files to transcribe")
    p.add_argument("--refs", default=None,
                   help="Directory of reference transcripts, <stem>.txt "
                        "(optional — without it, no WER/CER is reported)")
    p.add_argument("--providers", default="",
                   help="Comma-separated provider names (default: every "
                        "provider installed in ../stt-pkg/providers/)")
    p.add_argument("--out", default=None,
                   help="Output directory (default: ./results/<timestamp>/)")
    p.add_argument("--limit", type=int, default=0,
                   help="Only run the first N audio files")
    p.add_argument("--timeout", type=int, default=270,
                   help="Per-transcription budget in seconds passed to "
                        "providers as C3_STT_BUDGET_SECONDS (default 270)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show the corpus, the vocabulary and which providers "
                        "would run, then exit without calling any API")
    args = p.parse_args()

    os.environ["C3_STT_BUDGET_SECONDS"] = str(args.timeout)

    vocab = stt.load_vocabulary()
    terms = vocab.get("terms", [])
    runnable, skipped = select_providers(args.providers)

    print(f"Vocabulary: {len(terms)} terms from {vocab.get('source') or '(none found)'}")
    for name, reason in skipped:
        print(f"SKIP  {name}: {reason}")
    for name, _ in runnable:
        print(f"RUN   {name}")

    if not args.audio_dir:
        print("\nNo --audio-dir given, so nothing to transcribe. "
              "Drop a corpus in and re-run:\n"
              "  python3 run_bakeoff.py --audio-dir /path/to/voice-notes")
        return 0

    audio_files = find_audio(args.audio_dir, args.limit)
    if not audio_files:
        print(f"\nERROR: no audio files in {args.audio_dir} "
              f"(looked for: {', '.join(AUDIO_EXTS)})", file=sys.stderr)
        return 1

    ref_count = sum(1 for a in audio_files if find_reference(a, args.refs))
    print(f"\nCorpus: {len(audio_files)} file(s) in {args.audio_dir}; "
          f"{ref_count} with reference transcripts")

    if args.dry_run:
        for a in audio_files:
            has_ref = "ref" if find_reference(a, args.refs) else "no ref"
            print(f"  {os.path.basename(a)}  "
                  f"({metrics.fmt(audio_duration_seconds(a), '.1f')}s, {has_ref})")
        print(f"\nDry run — {len(runnable)} provider(s) × {len(audio_files)} "
              f"file(s) = {len(runnable) * len(audio_files)} API calls would run.")
        return 0

    if not runnable:
        print("\n*** NO PROVIDERS RAN — every provider was skipped (see reasons "
              "above). Set at least one API key in ~/.claude/stt.env and "
              "re-run. Nothing was measured. ***")
        return 0

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    out_dir = args.out or os.path.join(HERE, "results", timestamp)
    os.makedirs(out_dir, exist_ok=True)

    results = run(audio_files, runnable, vocab, args.refs, out_dir)
    summary = summarize(results, runnable)

    durations = [d for d in (audio_duration_seconds(a) for a in audio_files) if d]
    context = {
        "timestamp": timestamp,
        "audio_dir": os.path.abspath(args.audio_dir),
        "file_count": len(audio_files),
        "reference_count": ref_count,
        "total_seconds": sum(durations),
        "term_count": len(terms),
        "vocab_source": vocab.get("source"),
        "ran": [n for n, _ in runnable],
        "skipped": skipped,
    }

    report = build_report(summary, results, context)
    with open(os.path.join(out_dir, "report.md"), "w", encoding="utf-8") as f:
        f.write(report)
    with open(os.path.join(out_dir, "results.json"), "w", encoding="utf-8") as f:
        json.dump({"context": context, "summary": summary, "results": results},
                  f, indent=2, ensure_ascii=False)

    print("\n" + report)
    print(f"Written to {out_dir}/report.md and results.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
