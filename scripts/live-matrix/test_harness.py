"""Hermetic checks for the driver; no Claude, tmux or broker processes."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from collect import classify, count_receives, sanitize, verdict
from driver import false_held, write_report
from host import read_jsonl
from matrix import Cell, cells


class MatrixTests(unittest.TestCase):
    def test_complete_product_and_feasibility(self):
        matrix = cells()
        self.assertEqual(len(matrix), 180)
        self.assertEqual(len({c.name for c in matrix}), 180)
        self.assertEqual(sum(not c.infeasible for c in matrix), 126)
        for c in matrix:
            if c.infeasible:
                self.assertEqual(c.session, "fresh")
                self.assertNotIn(c.state, ("idle", "startup"))

    def test_records_ignore_partial_tail(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "records.jsonl"
            path.write_text('{"type":"user"}\n{"type":"assistant"}')
            self.assertEqual(read_jsonl(path), [{"type": "user"}])

    def test_fixture_counts_do_not_confuse_intake_and_delivery(self):
        fixture = Path(__file__).resolve().parents[2] / "cmd/c3-claude-adapter/testdata/claude-2.1.266-intake.jsonl"
        records = read_jsonl(fixture)
        # enqueue + remove + eventual attachment represent one source delivery.
        self.assertEqual(count_receives(records + [records[-1]], {"DELIVERYTOKEN-1"}, [11380]), {"11380": 1})
        duplicate = copy.deepcopy(records[-1])
        duplicate["uuid"] = "different-record"
        self.assertEqual(count_receives(records + [duplicate], {"DELIVERYTOKEN-1"}, [11380]), {"11380": 2})
        self.assertTrue(classify(records[0])["accept"])
        self.assertFalse(classify(records[2])["accept"])
        self.assertIsNone(classify(records[1])["accept"])

    def test_redaction_preserves_attempt_and_peer_provenance(self):
        sample = {"uuid": "private", "cwd": "/home/example/private", "origin": {"kind": "peer", "from": "c3", "verifiedPeerPid": 123},
                  "user": "someone", "content": 'Another Claude session sent a message:\n<channel source="plugin:c3:c3" c3_delivery_id="secret-token" c3_attempt="inbox:3" user="someone" user_id="987" chat_id="-123">sample</channel>'}
        clean = sanitize(sample, {"secret-token"})
        encoded = json.dumps(clean)
        for secret in ("private", "someone", "secret-token", "987", "-123", "/home/"):
            self.assertNotIn(secret, encoded)
        self.assertEqual(clean["origin"]["from"], "c3")
        self.assertIn('c3_attempt="inbox:3"', clean["content"])
        self.assertIn('c3_delivery_id="TOKEN"', clean["content"])
        self.assertIsInstance(clean["origin"]["verifiedPeerPid"], int)

    def evidence(self):
        return {"injected": True, "rows_final": 0, "received": {"1": 1}, "attempts": [
            {"phase": "reserved", "token": "one", "transport": "channel", "members": "1"},
            {"phase": "confirmed", "token": "one", "transport": "channel", "retired": "1", "elapsed_ms": "100"}]}

    def test_success_requires_every_contract(self):
        cell = Cell("channel", "foreground", "resumed", "text", "single")
        self.assertEqual(verdict(cell, self.evidence()), [])
        for field, bad in (("injected", False), ("rows_final", 1), ("received", {"1": 2}), ("attempt_before_ready", True), ("false_held", True), ("attempts", [])):
            evidence = self.evidence()
            evidence[field] = bad
            self.assertTrue(verdict(cell, evidence), field)
        evidence = self.evidence()
        evidence["attempts"][1]["elapsed_ms"] = "15001"
        self.assertTrue(verdict(cell, evidence))
        evidence = self.evidence()
        evidence["attempts"].append({"phase": "reserved", "token": "two", "transport": "inbox", "members": "1"})
        self.assertIn("fallback/wrong transport attempted", verdict(cell, evidence))

    def test_fetch_rejects_consume_before_tool_result(self):
        cell = Cell("fetch", "idle", "resumed", "text", "single")
        evidence = dict(self.evidence(), rows_while_fetch_result_held=0, fetch_tool_result=True, fetch_token=True)
        self.assertTrue(verdict(cell, evidence))
        evidence["rows_while_fetch_result_held"] = 1
        self.assertEqual(verdict(cell, evidence), [])

    def test_held_counts_exclude_only_open_members(self):
        begin = "TEST ATTEMPT token=x phase=reserved members=1\n"
        self.assertTrue(false_held(begin + 'TEST SINK text="Held — nothing lost.\\n1 message queued."', 1))
        self.assertFalse(false_held(begin + 'TEST SINK text="Held — nothing lost.\\n1 message queued."', 2))
        self.assertFalse(false_held(begin + "TEST ATTEMPT token=x phase=expired retired=0\n" + 'TEST SINK text="Held — nothing lost.\\n1 message queued."', 1))

    def test_report_cannot_call_collection_a_pass(self):
        cell = cells()[0]
        with tempfile.TemporaryDirectory() as root:
            write_report(Path(root), "2.1.266", {cell.name: {"status": "COLLECTED", "reasons": []}})
            report = (Path(root) / "REPORT.md").read_text()
            self.assertNotIn("| PASS |", report)
            self.assertEqual(report.count("| N/A |"), 54)
            self.assertEqual(report.count("| NOT RUN |"), 125)

    def test_imports_do_not_run_hosts(self):
        import importlib
        import proxy
        with patch("subprocess.Popen", side_effect=AssertionError("host launched")):
            importlib.reload(proxy)


if __name__ == "__main__":
    unittest.main()
