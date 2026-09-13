"""Step 4/5 snapshots and same-capture production-path parity."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

from capture_context import load_capture
from collect import assemble_driver_capture, collect
from hostdriver import Adapter, Broker, ControlJournal, RunProfile, Scratch, resolve_contract
from hostdrivers.claude import ClaudeHostDriver, describe
from matrix import Cell
from verdict_core import evaluate, derive

ROOT = Path(__file__).parent / 'testdata/extractor-replay'


class DriverCaptureTests(unittest.TestCase):
    def prepare(self, source, *, reference=None):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        old = load_capture(reference or source)
        scenario, _, _ = old['inputs']
        cell = Cell(**old['context'].descriptor['cell'])
        description = describe('2.1.267', 'matrix')
        case = next(case for case in description['capabilities']['cases'] if case['id'] == cell.name)
        profile = RunProfile(description, case, scenario, resolve_contract(description, case), Path('/frozen/cli'),
                             '2.1.267', 'matrix', None, execution='replay', artifact_source=source)
        driver = ClaudeHostDriver(backend_factory=Mock(side_effect=AssertionError('CLI execution')))
        scratch = Scratch(root, 'scratch', scenario['run_id'], root/'cwd', root, ControlJournal(root/'control'))
        driver.prepare(scratch, Adapter(Path('/frozen/adapter'), 'frozen'),
                       Broker(Path('/frozen/broker'), root/'broker/c3.sock', root/'broker', scenario['run_id'], scenario['route_id']), profile)
        return driver, profile, cell, old, root

    def test_native_controls_and_finalization_remain_incomplete_without_broker_observers(self):
        from test_hostdriver import DriverControls
        controls = DriverControls()
        self.addCleanup(controls.doCleanups)
        driver = controls.prepare()
        driver.launch()
        (controls.scratch.root/'transcript.jsonl').write_text('{}\n')
        controls.checkpoint('session')
        from hostdriver import Workload
        driver.enter_state('idle', Workload('none'))
        controls.checkpoint('injection')
        before, _ = driver.observe()
        rows = dict(before.reads)['state']['records']
        self.assertEqual([row['action'] for row in rows], ['session_sample', 'state_sample'])
        self.assertEqual(rows[0]['host_session_ref'], 'conversation')
        self.assertEqual(rows[0]['transcript_records'], 1)
        self.assertFalse(any(row['action'] == 'window_end' for row in rows))
        controls.checkpoint('final')
        final, bundle = driver.observe()
        rows = dict(final.reads)['state']['records']
        self.assertEqual(rows[-2:], [dict(action='cut', barrier_id='final', cuts=[]),
                                    dict(action='window_end', observed_state='idle')])
        for table in ('ownership-observers', 'queue', 'attempt-observers', 'receipt-observers', 'sample-observers', 'contract', 'injection'):
            self.assertEqual(dict(final.reads)[table]['state'], 'missing')
        self.assertEqual(driver.observe(bundle.next_cursor)[0], final)
        from hostdrivers.claude_evidence import ClaudeObservation
        replay = ClaudeObservation(controls.scratch, controls.broker, controls.adapter,
            replace(controls.profile, execution='replay', artifact_source=controls.scratch.root), 'frozen-reader')
        replay_tables, _ = replay.observe()
        self.assertEqual(dict(replay_tables.reads)['state']['records'], rows)
        capture = assemble_driver_capture(controls.backend.cell, driver, {}, profile=controls.profile)
        self.assertEqual(capture['verdict']['status'], 'FAIL')
        self.assertIn('evidence collection incomplete', capture['verdict']['reasons'])
        output = controls.scratch.root/'output'
        result = collect(controls.backend.cell, driver, controls.scratch.root, output, {}, capture_profile=controls.profile)
        self.assertEqual(result['status'], 'FAIL')
        self.assertTrue((output/'checked/capture.json').is_file())
        self.assertTrue((output/'replay/control/pane.txt').is_file())

    def test_native_partial_tail_can_complete_but_corruption_cannot_disappear(self):
        from test_hostdriver import DriverControls
        helper = DriverControls()
        self.addCleanup(helper.doCleanups)
        driver = helper.prepare(execution='replay')
        root = helper.scratch.root
        (root/'control').mkdir(exist_ok=True)
        (root/'control/sessions.jsonl').write_text(json.dumps(dict(session_id='native-session',
                                                           transcript_path='records.jsonl')) + '\n')
        path = root/'records.jsonl'
        path.write_bytes(b'{"type":"user"')
        first, bundle = driver.observe()
        self.assertEqual(dict(first.reads)['host-records']['state'], 'partial')
        with path.open('ab') as output:
            output.write(b'}\n')
        complete, cursor = driver.observe(bundle.next_cursor)
        read = dict(complete.reads)['host-records']
        self.assertEqual(read['state'], 'complete')
        self.assertEqual(read['record_lines'], [1])
        with path.open('ab') as output:
            output.write(b'{"uuid":"a","uuid":"b"}\n')
        malformed, bundle = driver.observe(cursor.next_cursor)
        self.assertEqual(dict(malformed.reads)['host-records']['state'], 'malformed')
        path.write_bytes(b'{}\n')
        truncated, _ = driver.observe(bundle.next_cursor)
        self.assertEqual(dict(truncated.reads)['host-records']['state'], 'truncated')
        self.assertTrue(any('previously observed bytes' in issue for issue in dict(truncated.reads)['host-records']['problems']))
        (root/'control/sessions.jsonl').write_text('{}\n')
        _, changed_controls = driver.observe()
        self.assertTrue(any('supporting bytes changed or shrank' in issue for issue in changed_controls.diagnostics))
        self.assertTrue(any(ref.artifact_id == 'prior-sessions' for ref, _ in changed_controls.artifacts))

    def test_injector_facts_join_only_actual_broker_admissions(self):
        from capture_store import injection_table
        from evidence_io import checked_bytes
        request = checked_bytes(b'{"response":{"accepted":true,"message_ids":[17]},"kind":"text"}\n', format='json')
        broker = checked_bytes(b'TEST INJECT accepted topic=42 message_id=17 kind=text source=test-inject\n')
        self.assertEqual(injection_table(request, broker), dict(message_ids=[17], kind='text', broker_lines=[1]))
        self.assertIsNone(injection_table(request, checked_bytes(b'')))
        self.assertIsNone(injection_table(request, checked_bytes((broker['text'] * 2).encode())))

    def test_observation_duration_is_not_the_receipt_timeout(self):
        from types import SimpleNamespace
        from driver import default_profile
        from hostdriver import Scratch, ControlJournal
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scratch = Scratch(root, 'scratch', 'run', root/'cwd', root, ControlJournal(root/'control'))
            profile = default_profile(ClaudeHostDriver(), SimpleNamespace(collect_only=False, setup_timeout=90,
                claude=Path('/frozen/cli'), sleep_seconds=35, observe_seconds=95),
                Cell('channel', 'idle', 'fresh', 'text', 'single'), scratch, '2.1.267')
            self.assertEqual(profile.timeouts.observe_seconds, 95)
            self.assertEqual(profile.scenario['observation_duration_ms'], 95000)
            self.assertEqual(profile.workload.duration_ms, 35000)
            self.assertEqual(profile.contract['timing']['live']['limit_ms'], 15000)

    def test_same_capture_healthy_parity(self):
        for name in ('healthy-channel-double', 'healthy-channel-repeated', 'healthy-inbox-single',
                     'healthy-fetch-double', 'healthy-fetch-permuted'):
            with self.subTest(capture=name):
                driver, profile, cell, old, _ = self.prepare(ROOT/name)
                with patch('subprocess.run', side_effect=AssertionError('execution')), \
                        patch('subprocess.Popen', side_effect=AssertionError('execution')), \
                        patch('pathlib.Path.home', side_effect=AssertionError('credentials')):
                    new = assemble_driver_capture(cell, driver, {}, profile=profile)
                self.assertEqual(new['verdict'], evaluate(*old['inputs']))
                self.assertEqual(new['inputs'][2], old['inputs'][2])
                self.assertEqual(derive(*new['inputs']), derive(*old['inputs']))
                driver._backend_factory.assert_not_called()

    def test_native_capture_same_verdict_with_missing_observers(self):
        from collect import capture_observation, build_verdict_inputs
        for name in ('healthy-channel-double', 'healthy-inbox-single', 'healthy-fetch-double'):
            with self.subTest(capture=name), tempfile.TemporaryDirectory() as directory:
                source = Path(directory)
                (source/'control').mkdir()
                (source/'broker').mkdir()
                for original, target in (('records.jsonl', 'records.jsonl'), ('broker.log', 'broker/broker.log'),
                                         ('adapter.log', 'control/adapter.log')):
                    shutil.copyfile(ROOT/name/original, source/target)
                records = [json.loads(line) for line in (source/'records.jsonl').read_text().splitlines()]
                session = next(record['sessionId'] for record in records if 'sessionId' in record)
                (source/'control/sessions.jsonl').write_text(json.dumps(dict(session_id=session,
                                                              transcript_path='records.jsonl')) + '\n')
                driver, profile, cell, old, _ = self.prepare(source, reference=ROOT/name)
                current = assemble_driver_capture(cell, driver, old['evidence'], profile=profile)
                reads = old['context'].reads
                host = reads['host-records']
                baseline = capture_observation(cell, old['evidence'], reads['broker'], reads['adapter'], host['records'], host)
                baseline = evaluate(*build_verdict_inputs(cell, old['evidence'], raw_observation=baseline,
                                    pinned_scenario=profile.scenario, pinned_contract=profile.contract))
                self.assertEqual(current['verdict'], baseline)
                self.assertFalse(current['inputs'][2]['collection_complete'])
                self.assertFalse(current['context'].descriptor)
                if cell.transport != 'fetch':
                    attempts = [event for event in current['inputs'][2]['events']
                                if event['milestone'] == 'attempt_reserved']
                    self.assertTrue(attempts)
                    self.assertTrue(all(not event['collection_complete'] and not event['members'] for event in attempts))
                self.assertEqual(current['verdict']['status'], 'FAIL')
                self.assertIn('evidence collection incomplete', current['verdict']['reasons'])

    def test_cursor_repeated_snapshot_and_integrity_failure(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        source = Path(directory.name)/'source'
        shutil.copytree(ROOT/'healthy-channel-double', source)
        driver, profile, cell, _, _ = self.prepare(source)
        first, bundle = driver.observe()
        again, second = driver.observe(bundle.next_cursor)
        self.assertEqual(first, again)
        self.assertEqual(bundle.next_cursor, second.next_cursor)
        path = source/'records.jsonl'
        path.write_bytes(path.read_bytes()[:-10])
        tables, damaged = driver.observe(second.next_cursor)
        self.assertEqual(dict(tables.reads)['host-records']['state'], 'truncated')
        self.assertTrue(any('extent:' in item for item in damaged.diagnostics))
        result = assemble_driver_capture(cell, driver, {}, profile=profile)
        self.assertEqual(result['verdict']['status'], 'FAIL')
        with self.assertRaisesRegex(Exception, 'cursor'):
            driver.observe(replace(bundle.next_cursor, extents=()))

    def test_checked_export_replays_same_verdict(self):
        driver, profile, cell, old, root = self.prepare(ROOT/'healthy-channel-double')
        result = collect(cell, driver, root, root/'output', {}, capture_profile=profile)
        replay = load_capture(root/'output/checked')
        self.assertEqual(result['status'], 'PASS')
        self.assertEqual(evaluate(*replay['inputs']), evaluate(*old['inputs']))
        descriptor = json.loads((root/'output/checked/capture.json').read_text())
        self.assertEqual({entry['table'] for entry in descriptor['artifacts']},
                         {entry['table'] for entry in old['context'].inventory})


if __name__ == '__main__':
    unittest.main()
