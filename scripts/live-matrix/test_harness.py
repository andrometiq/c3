"""Hermetic checks for the driver; no Claude, tmux or broker processes."""
import copy
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
import xml.etree.ElementTree as ET
from unittest.mock import Mock, patch

from collect import collect, notice_evidence, route_line_limit, classify_fetch, fetch_trailer, classify, count_receives, sanitize, verdict
from driver import false_held, offered_before_ready, run_cell, write_report
from host import Host, HostSetupError, diagnose_pane, flatten, read_jsonl
from matrix import Cell, cells, selection, selection_summary


TRUST_PANE = """Accessing workspace:
/work/scratch/cwd
Quick safety check: Is this a project you created or one you trust?
❯ No, exit
  Yes, I trust this folder
Enter to confirm · Esc to cancel
"""
DEV_CHANNEL_PANE = """WARNING: Loading development channels
--dangerously-load-development-channels is for local channel development only.
Channels: plugin:c3@c3
❯ 1. I am using this for local development
  2. Exit
"""


class HostSetupTests(unittest.TestCase):
    def test_configuration_is_seeded_before_plugin_setup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = root / "original"
            (original / ".claude").mkdir(parents=True)
            (original / ".claude.json").write_text(json.dumps({"oauthAccount": {"test": "account"},
                                                               "projects": {"/production": {}}, "mcpServers": {"production": {}}}))
            (original / ".claude/settings.json").write_text(json.dumps({
                "permissions": {"defaultMode": "bypassPermissions"}, "hooks": {"unwanted": []},
                "enabledPlugins": {"production": True}, "env": {"ANTHROPIC_API_KEY": "synthetic-key-" + "x" * 20}}))
            scratch = root / "scratch"
            scratch.mkdir()
            def plugin_setup(*args, **kwargs):
                config = scratch / "claude"
                metadata = json.loads((config / ".claude.json").read_text())
                settings = json.loads((config / "settings.json").read_text())
                self.assertTrue(metadata["hasCompletedOnboarding"])
                self.assertEqual(metadata["lastOnboardingVersion"], "2.1.267")
                self.assertEqual(metadata["lastReleaseNotesSeen"], "2.1.267")
                self.assertTrue(metadata["hasSeenAutoDefaultNudge"])
                self.assertEqual(metadata["projects"], {str((scratch / "cwd").resolve()): {
                    "hasTrustDialogAccepted": True, "hasClaudeMdExternalIncludesApproved": False,
                    "hasClaudeMdExternalIncludesWarningShown": True}})
                self.assertEqual(metadata["customApiKeyResponses"], {"approved": ["x" * 20], "rejected": []})
                self.assertNotIn("synthetic-key-", (config / ".claude.json").read_text())
                self.assertEqual((config / ".claude.json").stat().st_mode & 0o777, 0o600)
                self.assertEqual(settings, {"theme": "dark", "permissions": {"defaultMode": "default"},
                                            "enabledMcpjsonServers": ["plugin:c3:c3"], "enabledPlugins": {"c3@c3": True}})
                self.assertNotIn("mcpServers", metadata)
            with patch.dict(os.environ, {"CLAUDE_CODE_POWERUP_ONBOARDING": "step"}, clear=True), \
                    patch("host.Path.home", return_value=original), \
                    patch("host.subprocess.run", side_effect=plugin_setup) as launch, \
                    patch("host.shutil.copyfile", side_effect=AssertionError("credential read")):
                host = Host(scratch, Path("claude"), Path("broker"), Path("adapter"), root,
                            Cell("channel", "idle", "fresh", "text", "single"), "2.1.267")
            self.assertEqual(launch.call_count, 2)
            self.assertNotIn("CLAUDE_CODE_POWERUP_ONBOARDING", host.env)
            self.assertIn("--no-chrome", host.command)
            mode = host.command.index("--permission-mode")
            self.assertEqual(host.command[mode + 1], "default")
            allowed = host.command.index("--allowedTools")
            self.assertEqual(host.command[allowed + 1:allowed + 3], ["mcp__plugin_c3_c3__*", "Bash(python3:*)"])

    def host(self, root, cell=None):
        host = Host.__new__(Host)
        host.cell = cell or Cell("channel", "idle", "fresh", "text", "single")
        host.control = root / "control"
        host.control.mkdir()
        host.timeout = 1
        host.development_warning_sent = False
        host.tmux = Mock(return_value=SimpleNamespace(stdout=TRUST_PANE, stderr="", returncode=0))
        host.records = Mock(return_value=[])
        host.events = Mock(return_value=[])
        host.launch = Mock()
        host.close = Mock()
        return host

    def test_trust_prompt_fails_immediately_without_sending_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            host = self.host(Path(directory))
            with patch("host.time.sleep", side_effect=AssertionError("waited for timeout")), \
                    self.assertRaisesRegex(HostSetupError, "^host is waiting on the workspace trust prompt$"):
                host.wait_event("attached")
            self.assertEqual((host.control / "pane.txt").read_text(), TRUST_PANE)
            self.assertEqual([call.args[0] for call in host.tmux.call_args_list], ["capture-pane"])

    def test_setup_failure_skips_delivery_verdict_and_reports_not_evaluated(self):
        for transport in ("channel", "inbox", "fetch"):
            for collect_only in (False, True):
                with self.subTest(transport=transport, collect_only=collect_only), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    scratch = root / "scratch"
                    scratch.mkdir()
                    cell = Cell(transport, "idle", "resumed", "text", "single")
                    host = self.host(scratch, cell)
                    args = SimpleNamespace(output=root / "output", claude=Path("claude"), setup_timeout=90,
                                           collect_only=collect_only, fixtures=False, keep_scratch=False)
                    with patch("driver.tempfile.mkdtemp", return_value=str(scratch)), \
                            patch("driver.subprocess.Popen") as broker, patch("driver.wait_for", return_value=True), \
                            patch("driver.Host", return_value=host), \
                            patch("driver.subprocess.run", side_effect=AssertionError("injection attempted")), \
                            patch("collect.verdict", side_effect=AssertionError("delivery evaluated")):
                        result = run_cell(args, cell, root, root, "2.1.267")
                    self.assertEqual(result["status"], "FAIL")
                    self.assertEqual(result["reasons"], ["host is waiting on the workspace trust prompt"])
                    self.assertIn("durable rows retired", result["not_evaluated"])
                    self.assertIn("no false Held", result["not_evaluated"])
                    self.assertIn("each source fetched exactly once" if transport == "fetch" else "each source received exactly once",
                                  result["not_evaluated"])
                    summary = args.output / "2.1.267" / cell.name / "summary.json"
                    self.assertEqual(json.loads(summary.read_text())["not_evaluated"], result["not_evaluated"])
                    write_report(args.output, "2.1.267", {cell.name: result})
                    report = (args.output / "REPORT.md").read_text()
                    self.assertIn("NOT EVALUATED:", report)
                    self.assertNotIn("no negotiated attempt observed", report)
                    host.close.assert_called_once()
                    broker.return_value.terminate.assert_called_once()

    def test_development_warning_requires_exact_channel_and_selected_label(self):
        with tempfile.TemporaryDirectory() as directory:
            host = self.host(Path(directory))
            host.pane = Mock(side_effect=[DEV_CHANNEL_PANE, DEV_CHANNEL_PANE, "Ready"])
            host.events.return_value = [{"event": "attached"}]
            with patch("host.time.sleep"):
                self.assertEqual(host.wait_event("attached"), {"event": "attached"})
            host.tmux.assert_called_once_with("send-keys", "-t", "matrix", "Enter")
            self.assertEqual((host.control / "development-channel-prompt.txt").read_text(), DEV_CHANNEL_PANE)
            for pane in (DEV_CHANNEL_PANE.replace("plugin:c3@c3", "plugin:other@market"),
                         DEV_CHANNEL_PANE.replace("❯ 1.", "  1.").replace("  2.", "❯ 2.")):
                host.development_warning_sent = False
                host.pane = Mock(return_value=pane)
                host.tmux.reset_mock()
                with self.assertRaises(HostSetupError):
                    host.wait_event("attached")
                host.tmux.assert_not_called()

    def test_run_failure_keeps_delivery_assertions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cell = Cell("channel", "idle", "fresh", "text", "single")
            evidence = {"setup_errors": [], "run_errors": ["host exited during observation"], "injected": True}
            result = collect(cell, None, root, root / "output", evidence, collect_only=True)
            self.assertEqual(result["status"], "FAIL")
            self.assertEqual(result["not_evaluated"], [])
            self.assertIn("host exited during observation", result["reasons"])
            self.assertIn("durable rows remain", result["reasons"])

    def test_unknown_screen_and_exited_host_are_setup_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            host = self.host(Path(directory))
            host.pane = Mock(return_value="Unrecognized screen")
            with patch("host.time.monotonic", side_effect=[0, 2]), \
                    self.assertRaisesRegex(HostSetupError, "host did not reach attached; unrecognized host screen"):
                host.wait_event("attached")
            host.pane = Mock(return_value=TRUST_PANE)
            with patch("host.time.monotonic", side_effect=[0, 2]), \
                    self.assertRaisesRegex(HostSetupError, "workspace trust prompt"):
                host.wait_event("attached")
            host.tmux.return_value = SimpleNamespace(stdout="", stderr="no server running", returncode=1)
            with self.assertRaisesRegex(RuntimeError, "Claude exited or tmux capture failed") as error:
                Host.pane(host)
            self.assertNotIsInstance(error.exception, HostSetupError)
            host.pane = lambda: Host.pane(host)
            with self.assertRaisesRegex(HostSetupError, "Claude exited or tmux capture failed"):
                host.wait_event("attached")
            host.pane = Mock(return_value="Ready")
            with patch("host.time.monotonic", side_effect=[0, 2]), \
                    self.assertRaises(TimeoutError) as error:
                host.wait_event("fetch_result_waiting")
            self.assertNotIsInstance(error.exception, HostSetupError)

    def test_other_first_run_panes(self):
        for pane, name in (("Choose the text style that looks best with your terminal", "theme selection"),
                           ("Security notes: Press Enter to continue", "onboarding security notice"),
                           ("Use Claude Code's terminal setup?", "terminal setup"),
                           ("Do you want to use this API key?", "API-key approval"),
                           ("New MCP server found in this project: c3", "MCP-server approval"),
                           ("Allow external CLAUDE.md file imports?", "external CLAUDE.md imports"),
                           ("WARNING: Claude Code running in Bypass Permissions mode\nYes, I accept", "permission bypass warning"),
                           ("Do you want to proceed?", "tool permission"),
                           ("Updates to Consumer Terms and Policies", "consumer terms and privacy policy"),
                           ("Press Enter to start your trial", "Pro-trial activation"),
                           ("Currently pinned: old\nLatest available: new", "provider model upgrade"),
                           ("Select login method:", "login")):
            self.assertEqual(diagnose_pane(pane), f"host is waiting on the {name} prompt")
        self.assertIsNone(diagnose_pane("MATRIX_READY"))


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
        return {"no_false_held": True, "route_line_count": 0, "injected": True, "rows_final": 0, "received": {"1": 1}, "attempts": [
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

    def test_notice_assertions_for_every_matrix_cell(self):
        for cell in cells():
            evidence = self.evidence()
            evidence["attempts"] = [dict(e, transport=cell.transport) for e in evidence["attempts"]]
            evidence["no_false_held"] = False
            self.assertIn("no false Held assertion failed or missing", verdict(cell, evidence), cell.name)
            evidence["no_false_held"] = True
            evidence["route_line_count"] = 0
            self.assertNotIn("route line count assertion failed or missing", verdict(cell, evidence), cell.name)
            evidence["route_line_count"] = route_line_limit(cell) + 1
            self.assertIn("route line count assertion failed or missing", verdict(cell, evidence), cell.name)
            for field in ("no_false_held", "route_line_count"):
                missing = dict(evidence)
                del missing[field]
                self.assertTrue(any(field.replace("_", " ") in reason.lower() for reason in verdict(cell, missing)), cell.name)

    def test_notice_collector_after_confirmation(self):
        log = "TEST ATTEMPT token=x phase=reserved members=1 transport=channel\n"
        log += "attempt confirmed token=x route=-100/1 ms=2\n"
        log += 'TEST SINK reply text="📨 Held — nothing lost. 1 message queued. Send /status to check."\n'
        evidence = notice_evidence(log, 1)
        self.assertFalse(evidence["no_false_held"])
        self.assertEqual(evidence["route_line_count"], 0)
        self.assertTrue(notice_evidence('TEST SINK text="Live route: live: inbox, confirmed 1s ago."', 1)["no_false_held"])

    def test_fetch_rejects_consume_before_tool_result(self):
        cell = Cell("fetch", "idle", "resumed", "text", "single")
        evidence = dict(self.evidence(), attempts=[], rows_while_fetch_result_held=0, fetch_tool_result=True, fetch_token=True, fetch_source_occurrences=1)
        self.assertTrue(verdict(cell, evidence))
        evidence["rows_while_fetch_result_held"] = 1
        evidence["fetch_trailer_complete"] = True
        evidence["attempts"] = [dict(e, transport="fetch") for e in self.evidence()["attempts"]]
        self.assertEqual(verdict(cell, evidence), [])

    def test_fetch_trailer_sidecar_expectations(self):
        trailer = "[C3_FETCH_RECEIPT_V1]\ngroup group-1\nmember row-1 " + "a" * 64 + "\nmember row-2 " + "b" * 64 + "\n[/C3_FETCH_RECEIPT_V1]"
        expected = fetch_trailer(trailer)
        self.assertEqual(len(expected["members"]), 2)
        def record(text, error=False, call="call-1"):
            return {"type": "user", "message": {"content": [{"type": "tool_result", "tool_use_id": call, "is_error": error, "content": text}]}}
        good = record("body\n" + trailer)
        expectation = classify_fetch(good, expected, {"call-1"})
        self.assertTrue(expectation["accept"])
        for text in (trailer + "\nextra", trailer + "\n", trailer[:-1], trailer.replace("group-1", "wrong"), trailer.replace("member row-2 " + "b" * 64 + "\n", "")):
            self.assertFalse(classify_fetch(record(text), expected, {"call-1"})["accept"])
        self.assertFalse(classify_fetch(record(trailer, True), expected, {"call-1"})["accept"])
        self.assertFalse(classify_fetch(record(trailer, call="wrong"), expected, {"call-1"})["accept"])
        ids = [m["record_id"] for m in expected["members"]]
        clean = sanitize(good, {"group-1"}, receipt_ids=ids)
        sidecar = sanitize(expectation, {"group-1"}, receipt_ids=ids)
        self.assertEqual([m["record_id"] for m in sidecar["members"]], ["ROW1", "ROW2"])
        self.assertTrue(classify_fetch(clean, sidecar, {"ID"})["accept"])

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

    def test_composer_sees_a_prompt_that_was_never_submitted(self):
        # A swallowed Enter leaves the prompt in the input box; the caller used
        # to report that as an unresponsive host 90 seconds later.
        unsent = "\u2500\u2500\u2500\u2500\n\u276f\u00a0Reply MATRIX_READY. Do not call any tools.\n\u2500\u2500\u2500\u2500\n  manual mode on"
        submitted = "\u276f Reply MATRIX_READY. Do not call any tools.\n\u25cf MATRIX_READY\n\u2500\u2500\u2500\u2500\n\u276f\n\u2500\u2500\u2500\u2500\n  manual mode on"
        probe = flatten("Reply MATRIX_READY. Do not call any tools.")[:40]
        host = Host.__new__(Host)
        with patch.object(Host, "pane", lambda self: unsent):
            self.assertIn(probe, flatten(host.composer()))
        with patch.object(Host, "pane", lambda self: submitted):
            self.assertNotIn(probe, flatten(host.composer()))

    def test_composer_matches_a_prompt_wrapped_across_lines(self):
        wrapped = "\u2500\u2500\u2500\u2500\n\u276f Call Bash once with command \"python3 -c\n'import pathlib'\" and run_in_background=true.\n\u2500\u2500\u2500\u2500\n  manual mode on"
        probe = flatten('Call Bash once with command "python3 -c \'import pathlib\'" and run_in_background=true.')[:40]
        host = Host.__new__(Host)
        with patch.object(Host, "pane", lambda self: wrapped):
            self.assertIn(probe, flatten(host.composer()))

    def test_imports_do_not_run_hosts(self):
        import importlib
        import proxy
        with patch("subprocess.Popen", side_effect=AssertionError("host launched")):
            importlib.reload(proxy)


if __name__ == "__main__":
    unittest.main()
