"""Regression tests for stt-handler.py.

Run with: python3 -m unittest plugins.c3.stt.test_stt_handler -v
(from the repo root).

2026-06-30: the Telegram transcript "readback" echo moved OUT of this handler
and INTO the Go broker/channel (internal/channel/telegram/readback.go +
internal/broker/worker.go). The handler now does ONLY download + whisper +
print-to-stdout, so the old echo tests (send_transcript_to_telegram band
selection, notify_transcription_failed) are gone; what remains exercises the
download / run_stt / cleanup / main-flow contract the Go shim depends on.
"""
import importlib.util
import os
import shutil
import sys
import tempfile
import threading
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
HANDLER_PATH = os.path.join(HERE, "stt-handler.py")


def load_handler():
    spec = importlib.util.spec_from_file_location("stt_handler", HANDLER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestDownloadFileOkFalse(unittest.TestCase):
    """I-9: a getFile {ok:false} envelope (expired/invalid file_id, or the Bot
    API 20MB getFile limit -> 'file is too big') must NOT KeyError. It must be
    raised as a PermanentDownloadError so the download loop treats it as
    terminal instead of burning the 3 retries on a guaranteed failure."""

    def setUp(self):
        self.handler = load_handler()

    def test_ok_false_raises_permanent_not_keyerror(self):
        calls = []

        def fake_tg(_token, method, **params):
            calls.append((method, params))
            return {"ok": False, "description": "file is too big", "error_code": 400}

        with self.assertRaises(self.handler.PermanentDownloadError) as ctx:
            self.handler.download_file(
                "tok", "BADFID",
                "/nonexistent/should-not-be-written.oga",
                tg_fn=fake_tg,
            )
        # surfaces the real reason ...
        self.assertIn("file is too big", str(ctx.exception))
        self.assertIn("400", str(ctx.exception))
        # ... and stops at getFile: no download attempt, no extra getFile calls.
        # (Non-retry on the permanent failure is enforced by main()'s loop, which
        # has a dedicated `except PermanentDownloadError` terminal branch.)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "getFile")

    def test_ok_false_is_not_keyerror(self):
        def fake_tg(_token, method, **params):
            return {"ok": False, "description": "invalid file_id"}

        try:
            self.handler.download_file("tok", "X", "/x.oga", tg_fn=fake_tg)
        except self.handler.PermanentDownloadError:
            pass
        except KeyError:
            self.fail("download_file raised the pre-fix cryptic KeyError('result')")

    def test_concurrent_downloads_own_distinct_temp_files(self):
        """Concurrent auto/manual attempts may share a final cache path, but
        neither may overwrite or clean up the other's in-flight temp file."""
        from unittest import mock

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return b"OggS-concurrent"

        def fake_tg(_token, _method, **_params):
            return {"ok": True, "result": {"file_path": "voice/file.oga"}}

        with tempfile.TemporaryDirectory(prefix="c3-stt-download-") as tmp:
            final = os.path.join(tmp, "123-FID.oga")
            original_replace = os.replace
            replace_barrier = threading.Barrier(2)
            sources = []
            sources_lock = threading.Lock()

            def synchronized_replace(src, dst):
                with sources_lock:
                    sources.append(src)
                replace_barrier.wait(timeout=2)
                original_replace(src, dst)

            failures = []

            def run_download():
                try:
                    self.handler.download_file("tok", "FID", final, tg_fn=fake_tg)
                except BaseException as exc:
                    failures.append(exc)

            with mock.patch.object(self.handler.urllib.request, "urlopen", return_value=Response()), \
                 mock.patch.object(self.handler.os, "replace", side_effect=synchronized_replace):
                threads = [threading.Thread(target=run_download) for _ in range(2)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=3)

            self.assertEqual(failures, [])
            self.assertEqual(len(sources), 2)
            self.assertEqual(len(set(sources)), 2, "each attempt must own a unique temp")
            for source in sources:
                self.assertTrue(source.startswith(final + "."))
                self.assertTrue(source.endswith(".part"))
                self.assertFalse(os.path.exists(source), "attempt must clean only its own temp")
            self.assertTrue(os.path.exists(final))


class TestRunSttFailureReturnsNone(unittest.TestCase):
    """I-2: run_stt must never let a subprocess failure escape as a bare
    traceback (which would bypass main()'s clean non-zero exit). TimeoutExpired
    and any other exception both become a clean None."""

    def setUp(self):
        self.handler = load_handler()

    def test_timeout_expired_returns_none(self):
        import subprocess
        from unittest import mock

        def boom(*_a, **_k):
            # text=True can still yield bytes/None stderr depending on version;
            # bytes here also exercises _stderr_snippet's guard.
            raise subprocess.TimeoutExpired(
                cmd="stt", timeout=1, stderr=b"provider stalled mid-poll")

        with mock.patch("subprocess.run", boom):
            result = self.handler.run_stt("/tmp/nope.oga", {}, timeout=1)
        self.assertIsNone(result)

    def test_generic_exception_returns_none(self):
        from unittest import mock
        with mock.patch("subprocess.run", side_effect=OSError("exec failed")):
            self.assertIsNone(self.handler.run_stt("/tmp/nope.oga", {}, timeout=5))


class TestMainDownloadTerminalFailureExits(unittest.TestCase):
    """A non-retryable download failure (PermanentDownloadError) must exit
    non-zero WITHOUT burning the remaining retries. The human "couldn't
    transcribe" notice is now the Go broker's job (worker.go echoReadback): the
    handler just logs + exits, the Go shim sees empty stdout → [STT FAILED]
    marker, and the broker sends the notice."""

    def setUp(self):
        self.handler = load_handler()

    def test_permanent_download_failure_exits_nonzero_first_attempt(self):
        from unittest import mock
        import io

        download_calls = []

        def fake_download(*a, **k):
            download_calls.append((a, k))
            raise self.handler.PermanentDownloadError("file is too big")

        with mock.patch.object(self.handler, "download_file", fake_download), \
             mock.patch.object(sys, "argv",
                               ["stt-handler.py", "-100", "4711", "FID", "914"]), \
             mock.patch.object(sys, "stdin", io.StringIO("bottoken\n")):
            with self.assertRaises(SystemExit) as ctx:
                self.handler.main()

        self.assertEqual(ctx.exception.code, 1)
        # Exited on the FIRST attempt — the permanent failure didn't retry.
        self.assertEqual(len(download_calls), 1)


class TestPruneInbox(unittest.TestCase):
    """The rolling-window audio cache (replaces the old delete-immediately):
    prune_inbox(keep_n) keeps the newest keep_n .oga files in INBOX_DIR and
    deletes older ones; a negative keep_n keeps everything; a missing/unreadable
    inbox is non-fatal. Recovery never depends on this cache — download_attachment
    / retranscribe re-fetch from Telegram by file_id."""

    def setUp(self):
        self.handler = load_handler()
        self.tmp = tempfile.mkdtemp(prefix="c3-stt-prune-")
        # Point the handler's module-global INBOX_DIR (used by both prune_inbox
        # and main()'s download path) at a hermetic temp dir, restored in tearDown.
        self._saved_inbox = self.handler.INBOX_DIR
        self.handler.INBOX_DIR = self.tmp

    def tearDown(self):
        self.handler.INBOX_DIR = self._saved_inbox
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_oga(self, name, mtime):
        p = os.path.join(self.tmp, name)
        with open(p, "wb") as f:
            f.write(b"OggS")
        os.utime(p, (mtime, mtime))  # explicit mtime → deterministic ordering
        return p

    def test_keeps_newest_n(self):
        # N + k files with strictly increasing mtimes: prune_inbox(N) keeps the
        # newest N (highest mtimes) and removes the older k.
        n, k = 5, 4
        paths = [self._make_oga(f"{i}-fid.oga", mtime=1000 + i) for i in range(n + k)]
        self.handler.prune_inbox(n)
        survivors = [f for f in os.listdir(self.tmp) if f.endswith(".oga")]
        self.assertEqual(len(survivors), n, "exactly the newest N must remain")
        for i in range(n + k):
            kept = os.path.exists(paths[i])
            if i >= k:  # the n highest mtimes
                self.assertTrue(kept, f"newest file {i} should be kept")
            else:
                self.assertFalse(kept, f"older file {i} should be pruned")

    def test_negative_keeps_all(self):
        paths = [self._make_oga(f"{i}-fid.oga", mtime=1000 + i) for i in range(6)]
        self.handler.prune_inbox(-1)
        for p in paths:
            self.assertTrue(os.path.exists(p), "negative keep_n must keep every file")

    def test_zero_deletes_all(self):
        paths = [self._make_oga(f"{i}-fid.oga", mtime=1000 + i) for i in range(3)]
        self.handler.prune_inbox(0)
        for p in paths:
            self.assertFalse(os.path.exists(p), "keep_n=0 must delete every file")

    def test_missing_inbox_is_nonfatal(self):
        # An inbox dir that doesn't exist must not raise (os.listdir OSError swallowed).
        self.handler.INBOX_DIR = os.path.join(self.tmp, "does", "not", "exist")
        self.handler.prune_inbox(5)  # no raise == pass

    def test_non_oga_files_untouched(self):
        # Only .oga participate in the window; other files are never pruned.
        keep = self._make_oga("new-fid.oga", mtime=2000)
        old = self._make_oga("old-fid.oga", mtime=1000)
        other = os.path.join(self.tmp, "notes.txt")
        with open(other, "wb") as f:
            f.write(b"keepme")
        self.handler.prune_inbox(1)
        self.assertTrue(os.path.exists(keep), "newest .oga kept")
        self.assertFalse(os.path.exists(old), "older .oga pruned")
        self.assertTrue(os.path.exists(other), "non-.oga files are never pruned")

    def test_prunes_only_part_files_older_than_outer_deadline_margin(self):
        from unittest import mock
        now = self.handler.time.time()
        stale = os.path.join(self.tmp, "voice.1-deadbeef.part")
        fresh = os.path.join(self.tmp, "voice.2-live.part")
        for path in (stale, fresh):
            with open(path, "wb") as f:
                f.write(b"partial")
        os.utime(stale, (now - 1801, now - 1801))
        os.utime(fresh, (now - 1800, now - 1800))

        with mock.patch.object(self.handler.time, "time", return_value=now):
            self.handler.prune_inbox(-1)

        self.assertFalse(os.path.exists(stale), "orphan older than 1800s should be swept")
        self.assertTrue(os.path.exists(fresh), "boundary-age in-flight temp must be kept")

    def test_main_success_flow_keeps_oga_under_default_retention(self):
        """End-to-end-ish: a full main() download+transcribe+print flow now KEEPS
        the cached .oga (rolling window — one file is well within the default 500),
        replacing the old delete-immediately. download_file is faked to actually
        write a file at the path main() chose (in our temp INBOX_DIR), so we can
        assert it survives after main() returns. The Telegram echo is no longer in
        Python, so nothing is mocked for it."""
        from unittest import mock
        import io

        created = {}

        def fake_download(_token, _file_id, dest_path, **_k):
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            with open(dest_path, "wb") as f:
                f.write(b"OggS-fake-audio")
            created["path"] = dest_path

        with mock.patch.dict(os.environ):
            os.environ.pop("STT_AUDIO_RETENTION", None)  # force the default 500
            with mock.patch.object(self.handler, "download_file", fake_download), \
                 mock.patch.object(self.handler, "run_stt", lambda *a, **k: "hello world"), \
                 mock.patch.object(sys, "argv",
                                   ["stt-handler.py", "-100", "4711", "FID", "914"]), \
                 mock.patch.object(sys, "stdin", io.StringIO("bottoken\n")):
                self.handler.main()

        self.assertIn("path", created)
        self.assertTrue(
            os.path.exists(created["path"]),
            "cached .oga should be KEPT after a successful transcribe (rolling window)",
        )


class TestImportCreatesParentDirs(unittest.TestCase):
    """TODO #12 (2026-05-16): on a fresh install
    ~/.claude/channels/telegram/ doesn't exist; logging.basicConfig(
    filename=...) does not create parent dirs, so import crashed with
    FileNotFoundError and the broker only surfaced [STT FAILED: error].

    These tests guard the fix: import must mkdir LOG_FILE's parent and
    INBOX_DIR up front, regardless of where STT_LOG_FILE/STT_INBOX_DIR
    point. We point them at fresh, deeply-nested temp dirs and confirm
    the handler imports cleanly and both dirs exist afterward.
    """

    def setUp(self):
        # sys.modules cache must be cleared so the next import re-runs
        # the module-level mkdir + basicConfig with our patched env.
        sys.modules.pop("stt_handler", None)
        self._saved_env = {
            k: os.environ.get(k)
            for k in ("STT_LOG_FILE", "STT_INBOX_DIR")
        }
        self.tmp = tempfile.mkdtemp(prefix="c3-stt-test-")
        # Use deeply-nested paths to make sure mkdir is recursive
        # (the fresh-install bug was specifically about the parent dir
        # not existing — not just the leaf).
        self.log_path = os.path.join(self.tmp, "deeply", "nested", "logs", "stt.log")
        self.inbox_path = os.path.join(self.tmp, "deeply", "nested", "inbox")
        os.environ["STT_LOG_FILE"] = self.log_path
        os.environ["STT_INBOX_DIR"] = self.inbox_path

    def tearDown(self):
        sys.modules.pop("stt_handler", None)
        for k, v in self._saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_import_creates_log_and_inbox_dirs(self):
        # Sanity: neither dir exists yet.
        self.assertFalse(os.path.isdir(os.path.dirname(self.log_path)))
        self.assertFalse(os.path.isdir(self.inbox_path))
        # Import must succeed (no FileNotFoundError) AND the dirs must
        # have been created during module load — both halves of the fix.
        load_handler()
        self.assertTrue(
            os.path.isdir(os.path.dirname(self.log_path)),
            "LOG_FILE parent dir should be mkdir'd at import time",
        )
        self.assertTrue(
            os.path.isdir(self.inbox_path),
            "INBOX_DIR should be mkdir'd at import time",
        )


if __name__ == "__main__":
    unittest.main()
