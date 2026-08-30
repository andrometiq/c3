import base64
import json
import os
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import tts
import providers


class Response:
    def __init__(self, body, content_type=None):
        self.body = body
        self.headers = {}
        if content_type is not None:
            self.headers["Content-Type"] = content_type
        self.read_size = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, size=-1):
        self.read_size = size
        return self.body


class ProviderContractTests(unittest.TestCase):
    def test_openrouter_contract_and_timeout_cap(self):
        provider = tts.load_provider("openrouter-gemini-tts")
        calls = []
        transcodes = []

        def urlopen(request, timeout):
            calls.append((request, timeout))
            return Response(b"raw pcm", "audio/pcm;rate=24000;channels=1")

        def transcode(pcm, rate, channels, timeout):
            transcodes.append((pcm, rate, channels, timeout))
            return b"ID3audio"

        with mock.patch.dict(os.environ, {"OPENROUTER_API_KEY": "secret"}, clear=False):
            audio = provider.synthesize(
                "hello", "ta", 500, urlopen=urlopen, transcoder=transcode)
        request, timeout = calls[0]
        body = json.loads(request.data)
        self.assertEqual(request.full_url, "https://openrouter.ai/api/v1/audio/speech")
        self.assertEqual(request.headers["Authorization"], "Bearer secret")
        self.assertEqual(body, {
            "model": "google/gemini-3.1-flash-tts-preview",
            "input": "hello",
            "voice": "Kore",
            "response_format": "pcm",
        })
        self.assertEqual(timeout, 120)
        self.assertEqual(transcodes[0][:3], (b"raw pcm", 24000, 1))
        self.assertGreater(transcodes[0][3], 0)
        self.assertLessEqual(transcodes[0][3], 500)
        self.assertEqual(audio, b"ID3audio")

    def test_openrouter_pcm_header_missing_rate_uses_default(self):
        provider = tts.load_provider("openrouter-gemini-tts")
        formats = []

        def transcode(pcm, rate, channels, timeout):
            formats.append((pcm, rate, channels, timeout))
            return b"\xff\xe3audio"

        with mock.patch.dict(os.environ, {"OPENROUTER_API_KEY": "secret"}, clear=False):
            provider.synthesize(
                "hello", "auto", 5,
                urlopen=lambda *_args, **_kwargs: Response(b"pcm", "audio/pcm;channels=2"),
                transcoder=transcode,
            )
        self.assertEqual(formats[0][:3], (b"pcm", 24000, 2))

    def test_openrouter_ffmpeg_failure_is_not_swallowed(self):
        provider = tts.load_provider("openrouter-gemini-tts")
        failures = (
            FileNotFoundError("ffmpeg"),
            provider.subprocess.CalledProcessError(1, ["ffmpeg"]),
        )
        for failure in failures:
            with self.subTest(failure=type(failure).__name__), \
                    mock.patch.object(provider.subprocess, "run", side_effect=failure), \
                    self.assertRaises(type(failure)):
                provider.transcode_pcm_to_mp3(b"pcm", 24000, 1, 5)

    def test_sarvam_contract_and_tamil_auto_detection(self):
        provider = tts.load_provider("sarvam-bulbul-v3")
        calls = []

        def urlopen(request, timeout):
            calls.append((request, timeout))
            encoded = base64.b64encode(b"\xff\xe3audio").decode()
            return Response(json.dumps({"audios": [encoded]}).encode())

        with mock.patch.dict(os.environ, {"SARVAM_API_KEY": "secret"}, clear=False):
            audio = provider.synthesize("வணக்கம்", "auto", 42, urlopen=urlopen)
        request, timeout = calls[0]
        body = json.loads(request.data)
        self.assertEqual(request.full_url, "https://api.sarvam.ai/text-to-speech")
        self.assertEqual(request.headers["Api-subscription-key"], "secret")
        self.assertEqual(body["model"], "bulbul:v3")
        self.assertEqual(body["language_code"], "ta-IN")
        self.assertEqual(body["speaker"], "kavitha")
        self.assertEqual(body["output_audio_codec"], "mp3")
        self.assertEqual(timeout, 42)
        self.assertEqual(audio, b"\xff\xe3audio")

    def test_sarvam_english_hint_uses_en_in_and_shubh(self):
        provider = tts.load_provider("sarvam-bulbul-v3")
        requests = []

        def urlopen(request, timeout):
            del timeout
            requests.append(request)
            encoded = base64.b64encode(b"ID3audio").decode()
            return Response(json.dumps({"audios": [encoded]}).encode())

        with mock.patch.dict(os.environ, {"SARVAM_API_KEY": "secret"}, clear=False):
            provider.synthesize("hello", "en-IN", 5, urlopen=urlopen)
        body = json.loads(requests[0].data)
        self.assertEqual((body["language_code"], body["speaker"]), ("en-IN", "shubh"))

    def test_elevenlabs_contract_and_hint(self):
        provider = tts.load_provider("elevenlabs-flash-v25")
        calls = []

        def urlopen(request, timeout):
            calls.append((request, timeout))
            return Response(b"ID3audio")

        with mock.patch.dict(os.environ, {"ELEVENLABS_API_KEY": "secret"}, clear=False):
            provider.synthesize("hello", "ta-IN", 30, urlopen=urlopen)
        request, timeout = calls[0]
        body = json.loads(request.data)
        self.assertEqual(
            request.full_url,
            "https://api.elevenlabs.io/v1/text-to-speech/21m00Tcm4TlvDq8ikWAM?output_format=mp3_44100_128",
        )
        self.assertEqual(request.headers["Xi-api-key"], "secret")
        self.assertEqual(body, {
            "text": "hello", "model_id": "eleven_flash_v2_5", "language_code": "ta",
        })
        self.assertEqual(timeout, 30)

    def test_non_audio_openrouter_response_is_rejected(self):
        provider = tts.load_provider("openrouter-gemini-tts")
        cases = ((b'{"error":{"message":"bad"}}', "application/json"),
                 (b"<html>bad</html>", "text/html"))
        with mock.patch.dict(os.environ, {"OPENROUTER_API_KEY": "secret"}, clear=False):
            for body, content_type in cases:
                with self.subTest(body=body), self.assertRaises(ValueError):
                    provider.synthesize(
                        "hello", "auto", 5,
                        urlopen=lambda *_args, body=body, content_type=content_type, **_kwargs:
                            Response(body, content_type),
                        transcoder=lambda *_args: self.fail("transcoder must not run"),
                    )

    def test_sarvam_non_mp3_error_bodies_are_rejected(self):
        provider = tts.load_provider("sarvam-bulbul-v3")
        with mock.patch.dict(os.environ, {"SARVAM_API_KEY": "secret"}, clear=False):
            for body in (b'{"error":{"message":"bad"}}', b"<html>bad</html>"):
                with self.subTest(body=body), self.assertRaises((ValueError, json.JSONDecodeError)):
                    provider.synthesize(
                        "hello", "auto", 5,
                        urlopen=lambda *_args, body=body, **_kwargs: Response(body),
                    )

    def test_elevenlabs_non_mp3_error_bodies_are_rejected(self):
        provider = tts.load_provider("elevenlabs-flash-v25")
        with mock.patch.dict(os.environ, {"ELEVENLABS_API_KEY": "secret"}, clear=False):
            for body in (b'{"error":{"message":"bad"}}', b"<html>bad</html>"):
                with self.subTest(body=body), self.assertRaises(ValueError):
                    provider.synthesize(
                        "hello", "auto", 5,
                        urlopen=lambda *_args, body=body, **_kwargs: Response(body),
                    )

    def test_oversized_provider_response_is_rejected_before_validation(self):
        provider = tts.load_provider("elevenlabs-flash-v25")
        response = Response(b"x" * (providers.MAX_RESPONSE_BYTES + 1))
        with mock.patch.dict(os.environ, {"ELEVENLABS_API_KEY": "secret"}, clear=False):
            with self.assertRaisesRegex(ValueError, "exceeds 25 MiB"):
                provider.synthesize(
                    "hello", "auto", 5,
                    urlopen=lambda *_args, **_kwargs: response,
                )
        self.assertEqual(response.read_size, providers.MAX_RESPONSE_BYTES + 1)


if __name__ == "__main__":
    unittest.main()
