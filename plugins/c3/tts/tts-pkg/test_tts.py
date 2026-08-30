import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import tts


class SpeakableTests(unittest.TestCase):
    def test_markdown_becomes_spoken_prose(self):
        source = """# Heading

**Bold** and _soft_ with `inline()` and [a label](https://example.test).

```python
print('not spoken')
```

> Quoted words

- first item
- second item!
"""
        self.assertEqual(
            tts.speakable(source),
            "Heading Bold and soft with inline() and a label. code omitted. "
            "Quoted words first item. second item!",
        )

    def test_urls_tables_and_status_glyphs(self):
        source = """| Name | State |
| --- | --- |
| API | ready |
Visit https://example.test/path. 📨 ⏸ ⚠️
"""
        self.assertEqual(
            tts.speakable(source),
            "Name, State. API, ready. Visit a link",
        )

    def test_tamil_is_untouched(self):
        text = "## வணக்கம் **உலகம்**. இது ஒரு சோதனை."
        self.assertEqual(tts.speakable(text), "வணக்கம் உலகம். இது ஒரு சோதனை.")

    def test_symbols_only_is_empty(self):
        self.assertEqual(tts.speakable("  ⚠️ 📬 --- !!! ~~  "), "")


class ChunkTests(unittest.TestCase):
    def test_sentence_aware_greedy_limits(self):
        got = tts.chunk("One two. Three four! Five six?", 20)
        self.assertEqual(got, ["One two. Three four!", "Five six?"])
        self.assertTrue(all(len(part) <= 20 for part in got))

    def test_newlines_split_sentences(self):
        self.assertEqual(tts.chunk("one two\nthree four", 10), ["one two", "three four"])

    def test_long_sentence_hard_splits_only_between_words(self):
        text = "alpha beta gamma delta epsilon"
        got = tts.chunk(text, 12)
        self.assertEqual(got, ["alpha beta", "gamma delta", "epsilon"])
        self.assertEqual(" ".join(got), text)
        self.assertTrue(all(len(part) <= 12 for part in got))

    def test_decimal_and_version_punctuation_do_not_split(self):
        text = "version 0.2.1 shipped. Next 3.5 hours."
        self.assertEqual(
            tts.chunk(text, 30),
            ["version 0.2.1 shipped.", "Next 3.5 hours."],
        )

    def test_single_word_longer_than_limit_survives_intact(self):
        self.assertEqual(tts.chunk("extraordinary", 5), ["extraordinary"])

    def test_non_positive_limit_raises(self):
        with self.assertRaises(ValueError):
            tts.chunk("hello", 0)


class FakeProvider:
    OUTPUT_MIME = "audio/mpeg"

    def __init__(self, max_chars=8, outcomes=None, unavailable=""):
        self.MAX_CHARS = max_chars
        self.outcomes = list(outcomes or [])
        self.unavailable = unavailable
        self.calls = []

    def available(self):
        return self.unavailable

    def synthesize(self, text, language, deadline_seconds):
        self.calls.append((text, language, deadline_seconds))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class ChainTests(unittest.TestCase):
    def test_chunk_two_failure_restarts_whole_text_on_next_provider(self):
        first = FakeProvider(max_chars=12, outcomes=[b"ID3first", RuntimeError("boom")])
        second = FakeProvider(max_chars=12, outcomes=[b"ID3second", b"\xff\xe3tail"])
        providers = {"first": first, "second": second}
        audio, name, chunks = tts.run_chain(
            ["first", "second"], "one two. three four.", "en", 30,
            provider_loader=providers.__getitem__, log=lambda _message: None,
        )
        self.assertEqual(name, "second")
        self.assertEqual(chunks, 2)
        self.assertEqual(audio, b"ID3second\xff\xe3tail")
        self.assertEqual([call[0] for call in first.calls], ["one two.", "three four."])
        self.assertEqual([call[0] for call in second.calls], ["one two.", "three four."])

    def test_unavailable_provider_is_skipped(self):
        skipped = FakeProvider(outcomes=[], unavailable="KEY missing")
        ready = FakeProvider(max_chars=100, outcomes=[b"ID3ready"])
        audio, name, chunks = tts.run_chain(
            ["skip", "ready"], "hello", "auto", 30,
            provider_loader={"skip": skipped, "ready": ready}.__getitem__,
            log=lambda _message: None,
        )
        self.assertEqual((audio, name, chunks), (b"ID3ready", "ready", 1))
        self.assertEqual(skipped.calls, [])

    def test_all_fail_raises(self):
        providers = {
            "a": FakeProvider(max_chars=100, outcomes=[RuntimeError("a failed")]),
            "b": FakeProvider(max_chars=100, outcomes=[RuntimeError("b failed")]),
        }
        with self.assertRaises(tts.SynthesisError):
            tts.run_chain(
                ["a", "b"], "hello", "auto", 30,
                provider_loader=providers.__getitem__, log=lambda _message: None,
            )

    def test_id3v2_is_stripped_after_first_chunk(self):
        tag = b"ID3\x04\x00\x00\x00\x00\x00\x03abc"
        provider = FakeProvider(max_chars=6, outcomes=[tag + b"FIRST", tag + b"SECOND"])
        audio, _, _ = tts.run_chain(
            ["one"], "alpha. beta.", "auto", 30,
            provider_loader=lambda _name: provider, log=lambda _message: None,
        )
        self.assertEqual(audio, tag + b"FIRSTSECOND")

    def test_invalid_and_truncated_id3v2_headers_raise(self):
        cases = (
            b"ID3",
            b"ID3\x04\x00\x00\x80\x00\x00\x00",
            b"ID3\x04\x00\x00\x00\x00\x00\x05",
        )
        for audio in cases:
            with self.subTest(audio=audio), self.assertRaises(ValueError):
                tts.strip_leading_id3v2(audio)

    def test_invalid_max_chars_falls_through_to_next_provider(self):
        broken = FakeProvider(max_chars=0, outcomes=[])
        ready = FakeProvider(max_chars=100, outcomes=[b"ID3ready"])
        audio, name, chunks = tts.run_chain(
            ["broken", "ready"], "hello", "auto", 30,
            provider_loader={"broken": broken, "ready": ready}.__getitem__,
            log=lambda _message: None,
        )
        self.assertEqual((audio, name, chunks), (b"ID3ready", "ready", 1))
        self.assertEqual(broken.calls, [])

    def test_empty_parts_are_failures_for_every_provider(self):
        loaded = []
        providers_by_name = {
            "first": FakeProvider(max_chars=100, outcomes=[]),
            "second": FakeProvider(max_chars=100, outcomes=[]),
        }

        def load(name):
            loaded.append(name)
            return providers_by_name[name]

        with self.assertRaises(tts.SynthesisError):
            tts.run_chain(
                ["first", "second"], "", "auto", 30,
                provider_loader=load, log=lambda _message: None,
            )
        self.assertEqual(loaded, ["first", "second"])

    def test_empty_joined_audio_falls_through(self):
        empty = FakeProvider(max_chars=100, outcomes=[b""])
        ready = FakeProvider(max_chars=100, outcomes=[b"ID3ready"])
        audio, name, chunks = tts.run_chain(
            ["empty", "ready"], "hello", "auto", 30,
            provider_loader={"empty": empty, "ready": ready}.__getitem__,
            log=lambda _message: None,
        )
        self.assertEqual((audio, name, chunks), (b"ID3ready", "ready", 1))

    def test_default_chain_uses_live_probe_order(self):
        self.assertEqual(tts.DEFAULT_CHAIN, (
            "sarvam-bulbul-v3",
            "elevenlabs-flash-v25",
            "openrouter-gemini-tts",
        ))


if __name__ == "__main__":
    unittest.main()
