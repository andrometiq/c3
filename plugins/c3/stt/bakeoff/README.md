# STT bake-off

Run every installed STT provider over the same audio and compare them on the
things that actually matter for voice notes: whether the proper nouns and
jargon survived, whether code-switched speech came back as English, how long it
took, and what it cost.

It imports the live chain (`../stt-pkg/stt.py`) rather than re-implementing it,
so the provider that wins here is the provider that runs in production.

## Drop a corpus in and run it

```bash
cd plugins/c3/stt/bakeoff

# 1. what would run, without spending anything
python3 run_bakeoff.py --audio-dir ~/voice-notes --dry-run

# 2. the real thing
python3 run_bakeoff.py --audio-dir ~/voice-notes

# 3. with ground truth, which unlocks WER/CER and true vocabulary recall
python3 run_bakeoff.py --audio-dir ~/voice-notes --refs ~/voice-notes/refs
```

That's it. Audio files go in one flat directory (`.oga .ogg .opus .mp3 .m4a
.wav .flac .aac .webm .mp4`). Reference transcripts are optional plain-text
files named after the audio — `note-01.oga` → `note-01.txt` — and the harness
looks for them in `--refs`, then beside the audio, then as `note-01.ref.txt`.

Useful flags: `--limit 3` (first N files, for a cheap smoke run),
`--providers a,b` (only these, in this order), `--out DIR` (default
`./results/<timestamp>/`), `--timeout 270` (per-transcription budget handed to
providers as `C3_STT_BUDGET_SECONDS`).

## What you need first

- **API keys** in `~/.claude/stt.env` — one `KEY=value` per line, the same file
  `c3-broker setup` writes. Providers whose key is missing are **skipped with a
  printed reason**; a run with no keys at all is a clean no-op, not a crash.
  Keys are never printed, logged, or copied into the output.
- **`ffprobe`** (from ffmpeg) for durations. Without it, durations and therefore
  cost estimates report as `—` instead of a made-up number; everything else
  still works.

## What you get

Written to the output directory:

| file | what's in it |
|---|---|
| `report.md` | the comparison tables, plus each transcript inline for reading |
| `results.json` | every measurement, machine-readable |
| `transcripts/<provider>/<file>.txt` | raw output, for diffing side by side |

The provider table reports, per provider: files transcribed vs attempted, mean
**WER** and **CER**, **vocabulary recall**, **confusions**, **non-latin**
characters, median **latency**, and **cost**.

Two of those columns are the reason this harness exists rather than a generic
WER script:

- **vocabulary recall / confusions** — scored directly against the terms in
  your vocabulary file (`~/.config/c3/stt-vocabulary.txt`, else the bundled
  `../stt-pkg/vocabulary.txt`). Recall is the share of vocabulary terms present
  in the reference that the provider also produced. Confusions counts the known
  wrong spellings — the `Vel != whale, well` negatives — that it produced
  instead. A transcript can post a respectable WER while mangling every product
  name in it, and the mangled product names are usually the whole problem.
- **non-latin** — characters left in an Indic script, i.e. speech transcribed
  verbatim where the chain's contract is inline English. That is a different
  product, not a better or worse WER, and WER cannot see it.

## Without reference transcripts

You still get latency, cost, how many vocabulary terms each provider produced,
how many known misspellings it produced, and every transcript side by side.
You do **not** get WER, CER or recall — those cells stay blank, and the report
says in as many words that nothing in it is a ranking. Blank beats invented.

Writing references is the expensive part, so a practical order is: run without
them first, read the transcripts, hand-correct the two or three that disagree
most into `refs/`, and re-run. Disagreement between two independent providers
is a good detector of exactly the words worth checking.

## Then act on the result

The chain is ordered — position 1 is tried first, 2 only if 1 fails. Once the
bake-off has told you the real order, set it in `~/.claude/stt.env`:

```
C3_STT_CHAIN=<best>,<second>,<third>
```

`python3 ../stt-pkg/stt.py --list-providers` prints the valid names, which ones
have a key, and the currently-resolved order. Full details of how the chain is
configured are in [`../stt-pkg/README.md`](../stt-pkg/README.md).

## Privacy

Bake-off output contains the verbatim text of whatever was transcribed, and
this is a public repository. `results/`, `corpus/`, `refs/` and loose audio
files are gitignored here — keep it that way, and point `--out` outside the
tree if you want to archive a run.

## Tests

```bash
python3 -m pytest test_metrics.py -q
```

Hermetic: no network, no keys, no audio. Covers the scoring functions
(WER/CER, vocabulary scoring, code-switch reporting, cost) and the harness's
own logic (corpus and reference discovery, per-provider aggregation, and that
the report refuses to call anything a ranking without references).
