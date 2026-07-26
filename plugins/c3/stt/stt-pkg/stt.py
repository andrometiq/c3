#!/usr/bin/env python3
"""Pluggable STT chain — runs providers in order until one succeeds.

Usage: python3 stt.py <audio_file>
       python3 stt.py <audio_file> --chain gemini-3-flash-openrouter,sarvam-saaras-v3
       python3 stt.py <audio_file> --chain sarvam-saaras-v3  (one provider, no fallback)
       python3 stt.py --list-providers                       (what's installed + keyed)

Config:
  --chain <providers>   Comma-separated provider names, best first (see below)
  --retries <n>         Retries per provider before moving to next (default: 1)
  --retry-delay <s>     Seconds between retries (default: 2)

WHERE THE ORDER IS CONFIGURED
-----------------------------
The chain is ordered: position 1 is tried first, position 2 only if 1 fails,
and so on. It resolves in exactly this precedence:

  1. --chain on the command line          (ad-hoc / bake-off runs)
  2. $C3_STT_CHAIN                        (the normal way to change it)
  3. DEFAULT_CHAIN below                  (what ships)

`stt-handler.py` invokes this script WITHOUT --chain, so in production the
chain comes from $C3_STT_CHAIN or the default. The handler loads
`~/.claude/stt.env` — the same file `c3-broker setup` writes API keys into —
and passes every key=value in it to this process as an environment variable.
So the supported, no-source-edit way to reorder providers is one line in
~/.claude/stt.env:

    C3_STT_CHAIN=soniox-stt-async-v5,gemini-3-flash-openrouter,sarvam-saaras-v3

Run `python3 stt.py --list-providers` to see the installed provider names,
which ones have their API key configured, and the currently-resolved chain.

Providers are Python files in the providers/ directory.
Each must implement: transcribe(audio_path: str, audio_bytes: bytes) -> str | None
Each may optionally implement:
    set_vocabulary(vocab: dict) -> None   # called before every transcribe()
    available() -> str                    # "" = ready, else a reason to skip

To add a new provider:
  1. Create providers/my-provider.py with a transcribe() function
  2. Put it in the chain: --chain gemini-3-flash-openrouter,my-provider

Stdout: clean transcript only
Stderr: retry/fallback/error logging
"""
import sys, os, importlib.util, argparse, time, json, re, subprocess

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROVIDERS_DIR = os.path.join(SCRIPT_DIR, "providers")

# --- Energy / silence gate ---
# An LLM-based STT provider (Gemini 3 Flash via OpenRouter) will confabulate a
# fluent, ENTIRELY-INVENTED transcript when handed silent/near-silent audio —
# even with explicit anti-fabrication + <<NO_SPEECH>> rules in its system prompt
# (confirmed by live test: pure digital silence -> a full fabricated lecture).
# Prompt-level rules do NOT stop it. The only reliable defense is to NOT send
# effectively-silent audio to any provider. We measure peak loudness with
# ffmpeg's volumedetect filter and, if it's below the threshold, gate out.
#
# volumedetect reports peak amplitude on stderr as e.g. "max_volume: -91.0 dB".
# Pure digital silence measures ~ -91 dB; real speech peaks far higher (roughly
# -30 to -3 dB), so a simple energy threshold cleanly separates them.
SILENCE_MAX_DB_DEFAULT = -50.0

def _resolve_silence_threshold_db(default_db=SILENCE_MAX_DB_DEFAULT):
    """Resolve the max_volume threshold (dB) below which audio counts as silent.

    Reads $C3_STT_SILENCE_MAX_DB; falls back to `default_db` when unset, blank,
    or unparseable. Never raises — a bad env value must not break transcription."""
    raw = os.environ.get("C3_STT_SILENCE_MAX_DB")
    if raw is None or not raw.strip():
        return default_db
    try:
        return float(raw.strip())
    except ValueError:
        return default_db

def _parse_max_volume_db(ffmpeg_stderr: str):
    """Parse ffmpeg volumedetect's 'max_volume: <N> dB' out of its stderr.

    Returns the float dB value, or None when the field is absent/unparseable
    (so the caller fails OPEN and skips the gate). Also matches the infinite
    floor some ffmpeg builds report for pure silence: '-inf dB' -> float('-inf')
    (< any threshold, so gated as silent — fail CLOSED for real silence) and the
    (harmless, loud) '+inf' -> float('inf'). Pure — no I/O."""
    if not ffmpeg_stderr:
        return None
    m = re.search(r"max_volume:\s*(-?(?:\d+(?:\.\d+)?|inf))\s*dB", ffmpeg_stderr)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None

def _parse_n_samples(ffmpeg_stderr: str):
    """Parse ffmpeg volumedetect's 'n_samples: <N>' out of its stderr.

    Returns the int sample count, or None when the field is absent/unparseable.
    Used to detect a COMPLETED-but-empty decode (n_samples: 0), which emits NO
    max_volume line at all yet is unambiguously silent. Pure — no I/O."""
    if not ffmpeg_stderr:
        return None
    m = re.search(r"n_samples:\s*(\d+)", ffmpeg_stderr)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None

def _resolve_measured_db(ffmpeg_stderr: str, ffmpeg_stdout: str):
    """Decide the measured peak dB from ffmpeg's output text. Pure — no I/O.

    Split out of _measure_max_volume_db so the decode-outcome logic is unit-
    testable without shelling out to ffmpeg. Returns:
      * the finite/inf max_volume dB when volumedetect reported one;
      * float('-inf') for a completed-but-empty decode (n_samples: 0 and NO
        max_volume line) — unambiguously silent, so gate it rather than fail
        open; and
      * None when there is no volumedetect summary at all (ffmpeg missing/
        errored / unparseable) — the caller then FAILS OPEN so a broken
        measurement never blocks real speech."""
    db = _parse_max_volume_db(ffmpeg_stderr or "")
    if db is None:
        db = _parse_max_volume_db(ffmpeg_stdout or "")
    if db is None:
        # No max_volume anywhere. Only treat as silent if the decode COMPLETED
        # with zero samples; otherwise stay None (fail open).
        combined = (ffmpeg_stderr or "") + "\n" + (ffmpeg_stdout or "")
        if _parse_n_samples(combined) == 0:
            return float("-inf")
    return db

def _is_effectively_silent(max_volume_db: float, threshold_db: float) -> bool:
    """True when peak loudness is below the threshold (i.e. no real speech).

    Boundary: exactly AT the threshold is NOT silent — we bias toward attempting
    transcription (fail toward doing the work) so only clearly-silent audio is
    gated. Pure — no I/O."""
    return max_volume_db < threshold_db

def _measure_max_volume_db(audio_path):
    """Measure peak loudness (dB) of an audio file via ffmpeg volumedetect.

    Thin subprocess wrapper: runs
      ffmpeg -hide_banner -i <path> -af volumedetect -f null -
    (volumedetect prints its summary to stderr) and parses out max_volume.

    Returns the float dB peak, or None if ffmpeg is missing, times out, or the
    output can't be parsed. Callers treat None as 'measurement unavailable' and
    FAIL OPEN (skip the gate) — a broken measurement must never block real
    speech. Mirrors sarvam-saaras-v3.py:_get_duration's shell-out-to-ffprobe
    style (stdlib subprocess only, no third-party deps)."""
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-i", audio_path,
             "-af", "volumedetect", "-f", "null", "-"],
            capture_output=True, text=True, timeout=15,
        )
    except Exception:
        return None
    # volumedetect writes its summary to stderr; parse there (fall back to
    # stdout defensively in case a build routes it differently). Note: 0.0 dB is
    # a legitimate value, so _resolve_measured_db tests for None explicitly
    # rather than truthiness, handles the '-inf' silence floor, and maps a
    # completed-but-empty decode (n_samples: 0) to -inf (silent).
    return _resolve_measured_db(result.stderr or "", result.stdout or "")

# --- Domain vocabulary ---
# Shared across all providers. Each provider adapts this into its own format
# (system prompt, hotwords parameter, etc.)
#
# Resolution order (first found wins):
#   1. $C3_STT_VOCAB (explicit override path)
#   2. $XDG_CONFIG_HOME/c3/stt-vocabulary.txt
#   3. ~/.config/c3/stt-vocabulary.txt
#   4. <stt-pkg>/vocabulary.txt (bundled default, generic-tech-only)
#   5. <stt-pkg>/vocabulary.json (legacy advanced format)
#
# Users keep personal/project terms in the override path so they don't
# get clobbered on c3 upgrades and don't ship to other installers.
def _vocab_search_paths():
    paths = []
    if env := os.environ.get("C3_STT_VOCAB"):
        paths.append(env)
    xdg = os.environ.get("XDG_CONFIG_HOME") or os.path.join(os.path.expanduser("~"), ".config")
    paths.append(os.path.join(xdg, "c3", "stt-vocabulary.txt"))
    paths.append(os.path.join(SCRIPT_DIR, "vocabulary.txt"))
    return paths

VOCAB_JSON_PATH = os.path.join(SCRIPT_DIR, "vocabulary.json")

def load_vocabulary():
    """Load domain vocabulary.

    Tries vocabulary.txt at the user override path first, then the
    bundled default. Format:
      - First line starting with # context: is the context description
      - Other lines starting with # are ignored (comments)
      - Each non-empty line is a preferred term
      - Optional: "Vel != whale, well, veil" to specify common misheard alternatives
      - Optional: "Vel -- a software framework" to add a note

    Falls back to vocabulary.json (advanced format) if no txt exists.
    Returns dict with 'terms' (list) and 'context' (string).
    """
    for p in _vocab_search_paths():
        if os.path.exists(p):
            try:
                vocab = _parse_vocab_txt(p)
                vocab["source"] = p
                return vocab
            except Exception:
                continue
    if os.path.exists(VOCAB_JSON_PATH):
        try:
            with open(VOCAB_JSON_PATH) as f:
                v = json.load(f)
                v.setdefault("source", VOCAB_JSON_PATH)
                return v
        except Exception:
            pass
    return {"terms": [], "context": "", "source": ""}

def _parse_vocab_txt(path):
    """Parse simple vocabulary.txt format."""
    terms = []
    context = ""
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("# context:"):
                context = line[len("# context:"):].strip()
                continue
            if line.startswith("#"):
                continue
            # Parse: "Term != alt1, alt2 -- note"
            note = ""
            nots = []
            if " -- " in line:
                line, note = line.rsplit(" -- ", 1)
                note = note.strip()
            if " != " in line:
                preferred, not_str = line.split(" != ", 1)
                preferred = preferred.strip()
                nots = [n.strip() for n in not_str.split(",")]
            else:
                preferred = line.strip()
            terms.append({"preferred": preferred, "not": nots, "note": note})
    return {"terms": terms, "context": context}

# --- Ordered provider chain ---
# Position 1 is tried first; 2 runs only if 1 fails; 3 only if 2 fails, etc.
#
# The order below is the 2026-07-26 model survey's ranking for this workload
# (Indian-English + Tamil code-switched long-form dictation with heavy
# proper-noun/jargon load). It is derived from PUBLISHED benchmarks and API
# capability, NOT from a measured run on this install's own audio. Measure it:
#     python3 ../bakeoff/run_bakeoff.py --audio-dir <corpus>
# and then set C3_STT_CHAIN to whatever actually won.
#
#   1. gemini-3-flash-openrouter — only provider with an unbounded biasing
#      mechanism (the vocabulary's "NOT these" negatives go straight into the
#      prompt), handles self-corrections, translates inline, ~9.5h per request.
#   2. soniox-stt-async-v5      — biasing API shaped like the vocabulary file
#      (context.terms), native code-mixing, one-way translation to English,
#      5h files, cheapest per hour.
#   3. elevenlabs-scribe-v2     — best independently-measured English WER,
#      keyterms biasing (1000 terms), no duration wall. Verbatim only: it does
#      NOT translate, so Tamil spans come back in Tamil.
#   4. sarvam-saaras-v3         — retained as the trailing fallback: it is the
#      incumbent, the only Indic specialist here, and the key is usually already
#      provisioned. Dropping it would regress a working install on the strength
#      of an unmeasured ranking.
#
# Providers whose API key is not configured are skipped instantly (see
# available()), so listing four costs nothing when only one is keyed.
DEFAULT_CHAIN = (
    "gemini-3-flash-openrouter,"
    "soniox-stt-async-v5,"
    "elevenlabs-scribe-v2,"
    "sarvam-saaras-v3"
)


def _resolve_chain(cli_chain=None):
    """Resolve the ordered provider chain. Pure — reads env, no I/O.

    Precedence: --chain > $C3_STT_CHAIN > DEFAULT_CHAIN. Blank/whitespace
    values at any level fall through to the next. Returns a list of provider
    names in try-order, with duplicates dropped (first occurrence wins) so a
    typo'd chain can't make one provider burn its retries twice."""
    for candidate in (cli_chain, os.environ.get("C3_STT_CHAIN"), DEFAULT_CHAIN):
        if not candidate or not candidate.strip():
            continue
        names, seen = [], set()
        for raw in candidate.split(","):
            name = raw.strip()
            if name and name not in seen:
                seen.add(name)
                names.append(name)
        if names:
            return names
    return []


def _provider_unavailable_reason(mod):
    """Why this provider cannot run at all, or "" when it can. Pure-ish.

    Optional provider hook:  available() -> str
    Returns "" when ready, or a short human reason ("SONIOX_API_KEY not set")
    otherwise. Lets an unkeyed provider be skipped INSTANTLY instead of burning
    1+retries attempts (and the retry sleeps between them) on a failure that
    cannot succeed — which is what makes a four-deep default chain free for an
    install that has only one key.

    Providers that don't define available() are always attempted: the v0.1.0
    contract is unchanged and this hook stays optional. A hook that itself
    raises is treated as available — a broken hook must never remove a working
    provider from the chain."""
    fn = getattr(mod, "available", None)
    if not callable(fn):
        return ""
    try:
        return (fn() or "").strip()
    except Exception:
        return ""


def run_chain(providers, audio_path, audio_bytes, vocab,
              retries=1, retry_delay=2.0, log=None):
    """Try providers in order; return the first non-empty transcript, else None.

    `providers` is an ordered list of (name, module). Each gets 1+retries
    attempts; a provider is exhausted on empty results OR exceptions, and the
    next one in the chain then runs. Vocabulary is re-applied before every
    attempt. `log` takes one string (defaults to stderr) — stdout belongs to
    the transcript alone."""
    log = log or (lambda msg: print(msg, file=sys.stderr))

    runnable = []
    for name, mod in providers:
        reason = _provider_unavailable_reason(mod)
        if reason:
            log(f"[stt] skipping {name}: {reason}")
            continue
        runnable.append((name, mod))
    if not runnable:
        log("[stt] no runnable providers — every provider in the chain is unavailable")
        return None

    for idx, (name, mod) in enumerate(runnable):
        max_attempts = 1 + retries  # 1 initial + N retries
        for attempt in range(1, max_attempts + 1):
            try:
                # Pass vocabulary if provider supports it
                if hasattr(mod, "set_vocabulary"):
                    mod.set_vocabulary(vocab)
                result = mod.transcribe(audio_path, audio_bytes)
                if result and result.strip():
                    if idx > 0 or attempt > 1:
                        log(f"[stt] success: {name} (attempt {attempt})")
                    return result.strip()
                log(f"[stt] {name} attempt {attempt}/{max_attempts}: empty result")
            except Exception as e:
                log(f"[stt] {name} attempt {attempt}/{max_attempts}: {type(e).__name__}: {e}")

            if attempt < max_attempts:
                log(f"[stt] retrying {name} in {retry_delay}s...")
                time.sleep(retry_delay)

        # Provider exhausted, move to next
        if idx < len(runnable) - 1:
            log(f"[stt] {name} exhausted, falling back to {runnable[idx + 1][0]}")
    return None


def list_providers():
    """Every provider installed in providers/, in filename order.

    Returns a list of (name, module_or_None). Used by --list-providers and by
    the bake-off harness so both discover providers the same way."""
    try:
        files = sorted(f for f in os.listdir(PROVIDERS_DIR)
                       if f.endswith(".py") and not f.startswith("__"))
    except OSError:
        return []
    return [(f[:-3], load_provider(f[:-3])) for f in files]


def load_provider(name):
    """Dynamically load a provider module from providers/<name>.py"""
    path = os.path.join(PROVIDERS_DIR, f"{name}.py")
    if not os.path.exists(path):
        print(f"[stt] provider not found: {path}", file=sys.stderr)
        return None
    spec = importlib.util.spec_from_file_location(f"providers.{name}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if not hasattr(mod, "transcribe"):
        print(f"[stt] provider {name} missing transcribe() function", file=sys.stderr)
        return None
    return mod

def main():
    parser = argparse.ArgumentParser(description="Pluggable STT chain")
    parser.add_argument("audio_file", nargs="?", help="Path to audio file")
    parser.add_argument("--chain", default=None,
                        help="Comma-separated provider chain, best first. "
                             "Overrides $C3_STT_CHAIN; unset falls back to "
                             f"$C3_STT_CHAIN then the built-in default ({DEFAULT_CHAIN})")
    parser.add_argument("--retries", type=int, default=1, help="Retries per provider (default: 1)")
    parser.add_argument("--retry-delay", type=float, default=2.0, help="Delay between retries in seconds (default: 2)")
    parser.add_argument("--list-providers", action="store_true",
                        help="List installed providers, their key status, and "
                             "the resolved chain, then exit")
    args = parser.parse_args()

    if args.list_providers:
        _print_provider_listing(args.chain)
        sys.exit(0)

    if not args.audio_file:
        parser.error("audio_file is required (or pass --list-providers)")

    audio_path = args.audio_file
    if not os.path.exists(audio_path):
        print(f"ERROR: File not found: {audio_path}", file=sys.stderr)
        sys.exit(1)

    with open(audio_path, "rb") as f:
        audio_bytes = f.read()

    # ── Energy / silence gate (anti-fabrication) ─────────────────────────────
    # BEFORE any provider runs: if the audio is effectively silent, do NOT hand
    # it to the LLM (it would confabulate a fabricated transcript — see the
    # module header). Exit exactly like the all-providers-failed path below
    # (empty stdout, exit 1) so the Go shim's graceful [STT FAILED] path is
    # unchanged — no new stdout contract. FAIL OPEN on any measurement problem
    # (ffmpeg missing, unparseable output, timeout): skip the gate and fall
    # through to the normal chain so real speech is never blocked by a broken
    # measurement.
    threshold_db = _resolve_silence_threshold_db()
    max_volume_db = _measure_max_volume_db(audio_path)
    if max_volume_db is None:
        print("[stt] silence gate skipped: could not measure loudness "
              "(ffmpeg missing/failed or unparseable output); proceeding to providers",
              file=sys.stderr)
    elif _is_effectively_silent(max_volume_db, threshold_db):
        print(f"[stt] gated: audio effectively silent "
              f"(max_volume={max_volume_db}dB < {threshold_db}dB); no speech to transcribe",
              file=sys.stderr)
        # Same terminal contract as "all providers failed": nothing on stdout,
        # non-zero exit -> Go shim surfaces the graceful "[STT FAILED]" notice.
        sys.exit(1)
    else:
        print(f"[stt] loudness ok (max_volume={max_volume_db}dB >= {threshold_db}dB)",
              file=sys.stderr)

    chain = _resolve_chain(args.chain)
    if not chain:
        print("ERROR: empty provider chain", file=sys.stderr)
        sys.exit(1)

    # Load domain vocabulary
    vocab = load_vocabulary()
    if vocab.get("terms"):
        src = vocab.get("source") or "?"
        print(f"[stt] loaded {len(vocab['terms'])} vocabulary terms from {src}", file=sys.stderr)

    # Load all providers upfront
    providers = []
    for name in chain:
        mod = load_provider(name)
        if mod:
            providers.append((name, mod))
        else:
            print(f"[stt] skipping unavailable provider: {name}", file=sys.stderr)

    if not providers:
        print("ERROR: no valid providers in chain", file=sys.stderr)
        sys.exit(1)

    # Run the chain in order: first non-empty transcript wins.
    transcript = run_chain(
        providers, audio_path, audio_bytes, vocab,
        retries=args.retries, retry_delay=args.retry_delay,
    )
    if transcript:
        print(transcript)
        sys.exit(0)

    print("ERROR: all providers failed", file=sys.stderr)
    sys.exit(1)


def _print_provider_listing(cli_chain=None):
    """--list-providers: what's installed, what's keyed, what will actually run.

    Prints to STDOUT (this mode produces no transcript, so stdout is free) so
    `c3-broker setup` — or a human choosing an order — can see the valid
    provider names and which ones have credentials, without reading source."""
    chain = _resolve_chain(cli_chain)
    if cli_chain and cli_chain.strip():
        source = "--chain"
    elif (os.environ.get("C3_STT_CHAIN") or "").strip():
        source = "$C3_STT_CHAIN"
    else:
        source = "built-in default"

    print("Installed providers (providers/<name>.py):")
    for name, mod in list_providers():
        if mod is None:
            print(f"  {name:<28} UNLOADABLE (no transcribe() or import error)")
            continue
        reason = _provider_unavailable_reason(mod)
        model = getattr(mod, "MODEL_ID", "") or "-"
        status = f"unavailable — {reason}" if reason else "ready"
        print(f"  {name:<28} {status:<40} model={model}")

    print(f"\nResolved chain (from {source}), tried in this order:")
    for i, name in enumerate(chain, 1):
        print(f"  {i}. {name}")
    print("\nTo change the order without editing source, put one line in "
          "~/.claude/stt.env:\n"
          "  C3_STT_CHAIN=" + ",".join(chain))


if __name__ == "__main__":
    main()
