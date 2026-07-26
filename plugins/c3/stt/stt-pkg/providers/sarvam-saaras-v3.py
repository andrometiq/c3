"""Sarvam AI Saaras v3 STT provider.

Requires: SARVAM_API_KEY env var (or in ~/.claude/stt.env)
For audio >30s: uses Sarvam's batch (bulk-job) API over native HTTP (stdlib
urllib only — no third-party deps). The batch flow (init job -> presigned
upload -> start -> poll status -> presigned download of outputs) was ported
1:1 from the `sarvamai` SDK (v0.1.28) so the `sarvamai` PyPI dependency could
be dropped. Endpoint/field fidelity matters: a wrong URL or field name
silently breaks long-voice transcription.
"""
import os, json, time, subprocess, mimetypes, urllib.request, urllib.error

# Sarvam API base (sarvamai/environment.py: SarvamAIEnvironment.PRODUCTION.base)
_SARVAM_BASE = "https://api.sarvam.ai"

MODEL_ID = "saaras:v3"

# USD per minute: Sarvam publishes this model in INR (₹30/hour list price as of
# the 2026-07-26 survey). No verified INR->USD rate belongs in source, so the
# bake-off reports this provider's cost as unknown rather than printing an
# invented conversion. Compare it on accuracy and latency; price it yourself.
COST_PER_MINUTE_USD = None

# UNRESOLVED, 2026-07-26 survey: Sarvam's June-2026 changelog marks
# /speech-to-text-translate as legacy (saaras:v2.5) while /speech-to-text
# defaults to saaras:v3 — and the live OpenAPI for /speech-to-text lists no
# `prompt` field. _transcribe_rest below still posts to the translate endpoint
# whenever a vocabulary exists, which means vocabulary biasing may be running
# against the older model. Left ALONE on purpose: this is the incumbent
# fallback that works today, the replacement request shape could not be
# verified without a live key, and a blind rewrite of a working fallback is a
# worse bet than a documented question. Settle it with the bake-off harness
# (../bakeoff/) once a key and the corpus are available.

# Dynamic vocabulary (set by main stt.py via set_vocabulary)
_VOCAB = {"terms": [], "context": ""}

def set_vocabulary(vocab):
    global _VOCAB
    _VOCAB = vocab or {"terms": [], "context": ""}

def _get_prompt():
    """Build a prompt string from vocabulary for Sarvam's prompt parameter.

    Spelling hint ONLY — we pass the domain terms so Saaras spells them
    correctly when they occur, but deliberately DROP the free-text context
    narrative. As a prompt bias, a topic sentence ("Technical discussion about
    DevOps...") primes the model toward that topic and can seed a hallucinated
    transcript on silent/unclear audio — the same failure the Gemini provider's
    anti-fabrication rules address. Terms alone don't narrate a topic.
    """
    if not _VOCAB.get("terms"):
        return None
    terms = ", ".join(t["preferred"] for t in _VOCAB["terms"])
    return f"Key terms: {terms}"

# --- Load API key ---
_SARVAM_KEY = None

def _get_key():
    global _SARVAM_KEY
    if _SARVAM_KEY:
        return _SARVAM_KEY
    _SARVAM_KEY = os.environ.get("SARVAM_API_KEY", "")
    if not _SARVAM_KEY:
        env_path = os.path.expanduser("~/.claude/stt.env")
        try:
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("SARVAM_API_KEY="):
                        _SARVAM_KEY = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
        except:
            pass
    return _SARVAM_KEY

def available() -> str:
    """"" when this provider can run, else why it can't (see stt.py)."""
    return "" if _get_key() else "SARVAM_API_KEY not set (env or ~/.claude/stt.env)"

def _get_duration(audio_path):
    """Get audio duration in seconds using ffprobe.

    Returns (duration, known). known=False when ffprobe is unavailable or fails,
    so the caller can choose a dependency-light path instead of blindly assuming
    a long note — the old behavior returned 999 and forced EVERY note (even short
    ones) onto the sarvamai-dependent batch path (stt-pipeline-4)."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", audio_path],
            capture_output=True, text=True, timeout=5
        )
        return float(result.stdout.strip()), True
    except Exception:
        return 0.0, False

def _transcribe_rest(audio_path, audio_bytes, key):
    """REST API for audio ≤30s.
    Uses /speech-to-text-translate when prompt is available (supports prompt param),
    falls back to /speech-to-text with saaras:v3 otherwise.
    """
    boundary = "----SarvamBoundary" + str(int(time.time()))
    prompt = _get_prompt()

    # Use translate endpoint if we have vocabulary (it supports prompt param)
    if prompt:
        endpoint = "https://api.sarvam.ai/speech-to-text-translate"
        MODEL = "saaras:v3"
    else:
        endpoint = "https://api.sarvam.ai/speech-to-text"
        MODEL = "saaras:v3"

    body = b""
    body += f"--{boundary}\r\n".encode()
    body += f'Content-Disposition: form-data; name="file"; filename="{os.path.basename(audio_path)}"\r\n'.encode()
    body += b"Content-Type: audio/ogg\r\n\r\n"
    body += audio_bytes
    body += b"\r\n"
    body += f"--{boundary}\r\n".encode()
    body += b'Content-Disposition: form-data; name="model"\r\n\r\n'
    body += MODEL.encode()
    body += b"\r\n"

    if not prompt:
        # Only /speech-to-text needs language_code and mode
        body += f"--{boundary}\r\n".encode()
        body += b'Content-Disposition: form-data; name="language_code"\r\n\r\n'
        body += b"unknown"
        body += b"\r\n"
        body += f"--{boundary}\r\n".encode()
        body += b'Content-Disposition: form-data; name="mode"\r\n\r\n'
        body += b"translate"
        body += b"\r\n"

    if prompt:
        body += f"--{boundary}\r\n".encode()
        body += b'Content-Disposition: form-data; name="prompt"\r\n\r\n'
        body += prompt.encode()
        body += b"\r\n"

    body += f"--{boundary}--\r\n".encode()

    req = urllib.request.Request(
        endpoint,
        data=body,
        headers={
            "api-subscription-key": key,
            "Content-Type": f"multipart/form-data; boundary={boundary}"
        }
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
        return data.get("transcript", "")

# --- Batch (bulk-job) API over native urllib ---
# Ported from the sarvamai SDK (v0.1.28). Step → SDK source mapping:
#   init job        POST speech-to-text/job/v1
#                     -> speech_to_text_job/raw_client.py:60-77 (initialise)
#   upload links    POST speech-to-text/job/v1/upload-files
#                     -> raw_client.py:402-415 (get_upload_links)
#   upload bytes    PUT  <presigned upload_url> (Azure blob)
#                     -> speech_to_text_job/job.py:361-385 (upload_files, sync)
#   start           POST speech-to-text/job/v1/{job_id}/start
#                     -> raw_client.py:292-300 (start)
#   poll status     GET  speech-to-text/job/v1/{job_id}/status
#                     -> raw_client.py:180-185 (get_status)
#   download links  POST speech-to-text/job/v1/download-files
#                     -> raw_client.py:517-530 (get_download_links)
#   download bytes  GET  <presigned download_url>
#                     -> job.py:510-531 (download_outputs, sync)
# Auth header on every api.sarvam.ai call: "api-subscription-key: <key>"
#   (core/client_wrapper.py:38 — get_headers()["api-subscription-key"]).


def _sarvam_api(path, key, method="GET", body=None, timeout=30):
    """Call an api.sarvam.ai bulk-job endpoint and return the parsed JSON dict.

    `path` is relative (e.g. "speech-to-text/job/v1"); joined to the base the
    same way the SDK does (core/http_client.py:_build_url —
    base.rstrip('/') + '/' + path.lstrip('/')). JSON body is sent with
    content-type application/json; auth via the api-subscription-key header.
    """
    url = _SARVAM_BASE.rstrip("/") + "/" + path.lstrip("/")
    headers = {"api-subscription-key": key}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else {}


def _http_put_file(url, file_path, timeout=60):
    """PUT a local file to a presigned Azure-blob URL (SDK job.py upload_files).

    Mirrors the SDK's headers exactly: x-ms-blob-type: BlockBlob plus a guessed
    Content-Type (defaulting to audio/wav when unknown — matches the sync SDK)."""
    content_type, _ = mimetypes.guess_type(file_path)
    if content_type is None:
        content_type = "audio/wav"
    with open(file_path, "rb") as f:
        data = f.read()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"x-ms-blob-type": "BlockBlob", "Content-Type": content_type},
        method="PUT",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        # 2xx == success. urlopen raises HTTPError for >=400 already.
        return 200 <= resp.status <= 226


def _http_get_bytes(url, timeout=60):
    """GET a presigned download URL and return the raw bytes."""
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _transcribe_batch(audio_path, key):
    """Batch (bulk-job) API for audio >30s — native urllib, no sarvamai dep."""
    file_name = os.path.basename(audio_path)

    # 1) Init job. Body is exactly what the SDK serializes for
    #    create_job(model="saaras:v3", mode="translate", language_code="unknown")
    #    (verified against sarvamai serialization: callback OMITted, num_speakers
    #    null, booleans default False). Returns {"job_id": ...} (BulkJobInitResponse).
    init = _sarvam_api(
        "speech-to-text/job/v1",
        key,
        method="POST",
        body={
            "job_parameters": {
                "language_code": "unknown",
                "model": "saaras:v3",
                "mode": "translate",
                "num_speakers": None,
                "with_diarization": False,
                "with_timestamps": False,
            }
        },
    )
    job_id = init["job_id"]

    # 2) Get a presigned upload URL for our file, then PUT the bytes.
    #    FilesUploadResponse.upload_urls: {file_name: {"file_url": ...}}.
    up = _sarvam_api(
        "speech-to-text/job/v1/upload-files",
        key,
        method="POST",
        body={"job_id": job_id, "files": [file_name]},
    )
    upload_url = up["upload_urls"][file_name]["file_url"]
    _http_put_file(upload_url, audio_path)

    # 3) Start processing. ptu_id is optional/None in the SDK; we omit it.
    _sarvam_api(
        f"speech-to-text/job/v1/{job_id}/start", key, method="POST"
    )

    # Wait budget derived from the handler's remaining subprocess budget
    # (C3_STT_BUDGET_SECONDS, set by stt-handler.py = 270s minus elapsed download
    # time). We keep the wait ~15s UNDER that budget so the poll loop returns/raises
    # here gracefully BEFORE the handler's subprocess.run hard-kills us. Capped at
    # 240s (long notes need it), floored at 30s. The Go-side 300s context is the
    # true backstop. Default 270 when the env isn't set (direct invocation).
    try:
        _budget = int(os.environ.get("C3_STT_BUDGET_SECONDS", "270"))
    except ValueError:
        _budget = 270
    _wait = max(30, min(240, _budget - 15))

    # 4) Poll status every 5s (SDK wait_until_complete poll_interval default)
    #    until job_state is "completed"/"failed", or the budget elapses.
    #    JobStatusResponse.job_state is the completion signal (SDK job.py:415).
    status = _poll_until_complete(job_id, key, poll_interval=5, timeout=_wait)
    state = (status.get("job_state") or "").lower()
    if state != "completed":
        return None

    # 5) For each successfully-processed file, resolve its output file name
    #    from job_details, get a presigned download URL, fetch the output JSON,
    #    and return its "transcript". Output JSON shape mirrors the REST one
    #    (the SDK download_outputs writes this raw body to {input}.json).
    for detail in (status.get("job_details") or []):
        if (detail.get("state") != "Success"):
            continue
        outputs = detail.get("outputs") or []
        if not outputs:
            continue
        output_file = outputs[0].get("file_name")
        if not output_file:
            continue
        dl = _sarvam_api(
            "speech-to-text/job/v1/download-files",
            key,
            method="POST",
            body={"job_id": job_id, "files": [output_file]},
        )
        download_url = dl["download_urls"][output_file]["file_url"]
        raw = _http_get_bytes(download_url)
        data = json.loads(raw) if raw else {}
        return data.get("transcript", "")
    return None


def _poll_until_complete(job_id, key, poll_interval=5, timeout=600):
    """Poll GET .../status until job_state is completed/failed, mirroring the
    SDK's sync wait_until_complete (job.py:411-421): poll, check terminal
    state, sleep, give up after `timeout` seconds.

    Returns the final status dict. Raises TimeoutError if the job has not
    reached a terminal state within `timeout` (same contract as the SDK)."""
    start = time.monotonic()
    while True:
        status = _sarvam_api(
            f"speech-to-text/job/v1/{job_id}/status", key, method="GET"
        )
        state = (status.get("job_state") or "").lower()
        if state in {"completed", "failed"}:
            return status
        if time.monotonic() - start > timeout:
            raise TimeoutError(
                f"Job {job_id} did not complete within {timeout} seconds."
            )
        time.sleep(poll_interval)

def transcribe(audio_path: str, audio_bytes: bytes) -> str:
    """Transcribe audio using Sarvam v3.
    Returns transcript string or None on failure.
    """
    key = _get_key()
    if not key:
        raise RuntimeError("SARVAM_API_KEY not available")

    duration, known = _get_duration(audio_path)
    if known:
        if duration <= 30:
            return _transcribe_rest(audio_path, audio_bytes, key)
        return _transcribe_batch(audio_path, key)
    # Unknown duration (ffprobe missing/failed): prefer the dependency-light REST
    # path (no sarvamai); fall back to the batch path if REST fails — either by
    # raising OR by returning an empty transcript (a 200-with-empty response,
    # e.g. the clip was actually too long for REST). This avoids forcing short
    # notes onto the sarvamai-dependent path while still handling long ones.
    try:
        t = _transcribe_rest(audio_path, audio_bytes, key)
        if t and t.strip():
            return t
        return _transcribe_batch(audio_path, key)
    except Exception:
        return _transcribe_batch(audio_path, key)
