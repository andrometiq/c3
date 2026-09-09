"""Hermetic checks for the driver; no Claude, tmux or broker processes."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET
from unittest.mock import patch

from collect import classify, count_receives, sanitize, verdict
from driver import false_held, offered_before_ready, write_report
from host import read_jsonl
from matrix import Cell, cells, selection, selection_summary


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

    def test_orchestrator_subset_and_session_count(self):
        pattern = "[ci]*-[ifs]*-*-text-single"
        matched, feasible, launches = selection(pattern)
        self.assertEqual(len(matched), 12)
        self.assertEqual(len(feasible), 10)
        self.assertEqual(launches, 16)
        self.assertEqual({c.transport for c in matched}, {"channel", "inbox"})
        self.assertEqual({c.state for c in matched}, {"idle", "foreground", "startup"})
        self.assertEqual({c.session for c in matched}, {"fresh", "resumed"})
        self.assertTrue(all(c.kind == "text" and c.burst == "single" for c in matched))
        self.assertEqual({c.name for c in matched if c.infeasible}, {
            "channel-foreground-fresh-text-single", "inbox-foreground-fresh-text-single"})
        self.assertIn("launches 16 Claude sessions", selection_summary(pattern))
        self.assertEqual(selection()[2], 216)

    def test_verified_peer_intake_classification_keeps_boundaries(self):
        fixture = Path(__file__).resolve().parents[2] / "cmd/c3-claude-adapter/testdata/claude-2.1.266-peer-intake.jsonl"
        records = read_jsonl(fixture)
        self.assertEqual([classify(r)["accept"] for r in records], [True, False, True])
        attachment = copy.deepcopy(records[2])
        del attachment["attachment"]["origin"]
        self.assertFalse(classify(attachment)["accept"])
        for record in (records[0], records[2]):
            obj = copy.deepcopy(record)
            field, key = (obj, "content") if obj["type"] == "queue-operation" else (obj["attachment"], "prompt")
            field[key] = field[key].replace(' source="plugin:c3:c3"', '')
            self.assertFalse(classify(obj)["accept"])

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
        self.assertTrue(classify(records[1])["accept"])

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
        tag = ET.fromstring(clean["content"].split("\n", 1)[1])
        self.assertEqual(tag.attrib["user_id"], "ID")
        self.assertEqual(tag.attrib["chat_id"], "ID")
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
        evidence = dict(self.evidence(), attempts=[], rows_while_fetch_result_held=0, fetch_tool_result=True, fetch_token=True, fetch_source_occurrences=1)
        self.assertTrue(verdict(cell, evidence))
        evidence["rows_while_fetch_result_held"] = 1
        self.assertEqual(verdict(cell, evidence), [])

    def test_startup_detects_legacy_and_negotiated_offers(self):
        from unittest.mock import Mock
        host = Mock()
        host.events.return_value = []
        with patch("driver.broker_text", return_value=""):
            self.assertFalse(offered_before_ready(host, Path("unused"), 0))
        with patch("driver.broker_text", return_value="delivered chan=test-inject chat=-1"):
            self.assertTrue(offered_before_ready(host, Path("unused"), 0))
        with patch("driver.broker_text", return_value="TEST ATTEMPT token=x phase=reserved"):
            self.assertTrue(offered_before_ready(host, Path("unused"), 0))
        host.events.return_value = [{"event": "channel_notify"}]
        with patch("driver.broker_text", return_value=""):
            self.assertTrue(offered_before_ready(host, Path("unused"), 0))

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
