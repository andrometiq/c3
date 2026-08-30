import importlib.util
import io
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

PKG_DIR = os.path.dirname(os.path.abspath(__file__))
HANDLER_PATH = os.path.join(os.path.dirname(PKG_DIR), "tts-handler.py")


def load_handler():
    spec = importlib.util.spec_from_file_location("tts_handler_test", HANDLER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HandlerTests(unittest.TestCase):
    def test_missing_tts_key_falls_back_to_stt_env(self):
        handler = load_handler()
        with tempfile.TemporaryDirectory() as tmp:
            tts_env = os.path.join(tmp, "missing-tts.env")
            stt_env = os.path.join(tmp, "stt.env")
            with open(stt_env, "w", encoding="utf-8") as output:
                output.write("SARVAM_API_KEY=from-stt\n")
            with mock.patch.dict(os.environ, {}, clear=False):
                os.environ.pop("SARVAM_API_KEY", None)
                sources = handler.load_keys(tts_env, stt_env)
                self.assertEqual(os.environ.get("SARVAM_API_KEY"), "from-stt")
                self.assertEqual(sources["SARVAM_API_KEY"], stt_env)
                os.environ.pop("SARVAM_API_KEY", None)

    def test_check_output_shape_and_key_resolution(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = os.path.join(tmp, "home")
            os.makedirs(os.path.join(home, ".claude"))
            with open(os.path.join(home, ".claude", "stt.env"), "w", encoding="utf-8") as output:
                output.write(
                    "OPENROUTER_API_KEY=literal-secret-never-print\n"
                    "SARVAM_API_KEY=two\nELEVENLABS_API_KEY=three\n")
            env = dict(os.environ)
            for key in ("OPENROUTER_API_KEY", "SARVAM_API_KEY", "ELEVENLABS_API_KEY"):
                env.pop(key, None)
            env.update({
                "HOME": home,
                "TTS_ENV_FILE": os.path.join(tmp, "missing.env"),
                "TTS_LOG_FILE": os.path.join(tmp, "handler.log"),
            })
            result = subprocess.run(
                [sys.executable, HANDLER_PATH, "--check"],
                capture_output=True, text=True, env=env, timeout=10,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = result.stdout.splitlines()
        self.assertEqual(
            lines[0],
            "chain=sarvam-bulbul-v3,elevenlabs-flash-v25,"
            "openrouter-gemini-tts source=built-in default",
        )
        self.assertTrue(any(line.startswith("tts_env_file=") for line in lines))
        self.assertTrue(any(line.startswith("stt_env_fallback=") for line in lines))
        providers = [line for line in lines if line.startswith("provider=")]
        self.assertEqual(len(providers), 3)
        self.assertTrue(all(" state=ready" in line for line in providers))
        self.assertTrue(lines[-1].startswith("keys="))
        self.assertNotIn("literal-secret-never-print", result.stdout)

    def test_symbols_only_exits_three_with_empty_stdout(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = dict(os.environ)
            env["TTS_LOG_FILE"] = os.path.join(tmp, "handler.log")
            result = subprocess.run(
                [sys.executable, HANDLER_PATH], input="⚠️ --- !!!",
                capture_output=True, text=True, env=env, timeout=10,
            )
        self.assertEqual(result.returncode, 3)
        self.assertEqual(result.stdout, "")

    def test_success_uses_chain_chunk_count_without_reloading_or_rechunking(self):
        handler = load_handler()
        with tempfile.TemporaryDirectory() as tmp:
            output_path = os.path.join(tmp, "speech.mp3")
            with mock.patch.object(handler, "_configure_logging"), \
                    mock.patch.object(handler, "load_keys", return_value={}), \
                    mock.patch.object(handler.sys, "stdin", io.StringIO("hello")), \
                    mock.patch.object(handler.tts, "run_chain",
                                      return_value=(b"ID3audio", "fake", 2)), \
                    mock.patch.object(handler.tts, "load_provider",
                                      side_effect=AssertionError("must not reload")), \
                    mock.patch.object(handler.tts, "chunk",
                                      side_effect=AssertionError("must not rechunk")):
                self.assertEqual(handler.main(["--out", output_path]), 0)
            with open(output_path, "rb") as source:
                self.assertEqual(source.read(), b"ID3audio")


if __name__ == "__main__":
    unittest.main()
