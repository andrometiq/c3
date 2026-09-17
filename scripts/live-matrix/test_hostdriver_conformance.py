"""Frozen native artifacts -> Claude observe -> checked context -> core verdict.

Expectations are read-only. Raw sources contain no capture descriptor or driver
output table; control journals and independent broker observers have separate
origins. All fixtures are authored raw_replay, never host-version certification.
"""
from copy import deepcopy
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from capture_store import checked_context
from collect import assemble_driver_capture, fetch_trailer, result_text
from hostdriver import Adapter, Broker, ControlJournal, RunProfile, Scratch, resolve_contract
from hostdrivers.claude import ClaudeHostDriver, describe
from matrix import Cell
from replay_export import export_replay_capture
from verdict_core import derive

ROOT = Path(__file__).parent / 'testdata/hostdriver/claude'


def read(path):
    return json.loads(path.read_text())


def events(observation, milestone):
    return [event for event in observation['events'] if event['milestone'] == milestone]


class FrozenClock:
    def monotonic(self): return 70.0
    def time(self): return 70.0
    def sleep(self, seconds): raise AssertionError('replay must not wait')


def replay_exported(root):
    """Fresh-process path: exported raw bytes reenter the real driver."""
    pinned = read(root/'verdict-inputs.json')
    scenario, contract = pinned['scenario'], pinned['contract']
    cell = Cell(**read(root/'capture.json')['cell'])
    description = describe('2.1.267', 'matrix')
    case = next(c for c in description['capabilities']['cases'] if c['id']==cell.name)
    declared = resolve_contract(description, case)
    # Export pseudonymizes opaque profile IDs. Rebind only those labels after
    # checking every expected policy field; observed negotiation is never used.
    policy = lambda value: {k:v for k,v in value.items() if k not in ('id', 'capability_id')}
    if policy(contract) != policy(declared):
        raise AssertionError('export changed the pinned contract policy')
    with tempfile.TemporaryDirectory() as directory, \
            patch('subprocess.run', side_effect=AssertionError('process execution')), \
            patch('subprocess.Popen', side_effect=AssertionError('process execution')), \
            patch('pathlib.Path.home', side_effect=AssertionError('credential lookup')):
        scratch_root = Path(directory)
        profile = RunProfile(description, case, scenario, declared, Path('/frozen/cli'),
                             '2.1.267', 'matrix', None, execution='replay',
                             artifact_source=root, clock=FrozenClock())
        terminal = Mock(side_effect=AssertionError('terminal execution'))
        driver = ClaudeHostDriver(backend_factory=terminal)
        driver.prepare(Scratch(scratch_root, 'scratch', scenario['run_id'], scratch_root/'cwd',
                               scratch_root, ControlJournal(scratch_root/'control')),
                       Adapter(Path('/frozen/adapter'), 'frozen-adapter'),
                       Broker(Path('/frozen/broker'), scratch_root/'broker/c3.sock', scratch_root/'broker',
                              scenario['run_id'], scenario['route_id']), profile)
        result = assemble_driver_capture(cell, driver, {}, profile=profile)
        terminal.assert_not_called()
        return dict(inputs=result['inputs'], verdict=result['verdict'])


class ClaudeConformance(unittest.TestCase):
    maxDiff = 4000

    def capture(self, name):
        fixture = ROOT / name
        pin = read(fixture/'profile.json')
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        scratch_root = Path(temporary.name)
        source = scratch_root/'source'
        shutil.copytree(fixture/'raw', source)
        outside = scratch_root/'outside.jsonl'
        if pin.get('isolation'):
            outside.write_text('{"private":"must never be read"}\n')
            if pin['isolation']=='symlink':
                (source/'escaped.jsonl').symlink_to(outside)
        self.assertFalse((source/'capture.json').exists())
        self.assertFalse((source/'context/driver.jsonl').exists())
        self.assertFalse(list(source.rglob('expected*')))
        cell = Cell(**pin['cell'])
        description = describe(pin['host_version'], pin['mode'])
        case = next(c for c in description['capabilities']['cases'] if c['id'] == cell.name)
        scenario = pin['scenario']
        self.assertEqual(resolve_contract(description, case), pin['contract'])
        profile = RunProfile(description, case, scenario, pin['contract'],
                             Path('/frozen/cli'), pin['host_version'], pin['mode'], None,
                             execution='replay', artifact_source=source, clock=FrozenClock())
        terminal = Mock(side_effect=AssertionError('terminal execution requested'))
        driver = ClaudeHostDriver(backend_factory=terminal)
        scratch = Scratch(source, 'scratch', scenario['run_id'], source/'cwd',
                          source, ControlJournal(source/'control'))
        read_bytes = Path.read_bytes
        def contained_read(path):
            if pin.get('isolation') and path.resolve()==outside.resolve():
                raise AssertionError('outside transcript was opened')
            return read_bytes(path)
        with patch('subprocess.run', side_effect=AssertionError('process execution')), \
                patch('subprocess.Popen', side_effect=AssertionError('process execution')), \
                patch('pathlib.Path.home', side_effect=AssertionError('credential lookup')), \
                patch.object(Path, 'read_bytes', contained_read):
            driver.prepare(scratch, Adapter(Path('/frozen/adapter'), 'frozen-adapter'),
                           Broker(Path('/frozen/broker'), source/'broker/c3.sock', source/'broker',
                                  scenario['run_id'], scenario['route_id']), profile)
            if pin.get('previous_host'):
                final_bytes = (source/'records.jsonl').read_bytes()
                (source/'records.jsonl').write_bytes((fixture/pin['previous_host']).read_bytes())
                driver.observe()
                (source/'records.jsonl').write_bytes(final_bytes)
            tables, bundle = driver.observe()
            # Assert producer facts BEFORE any assembler or evaluator is called.
            self.assertEqual(dict(bundle.provenance)['kind'], 'raw_replay')
            self.assertEqual(dict(tables.reads)['state']['records'], read(fixture/'expected-driver.json'))
            diagnostics = fixture/'expected-producer-diagnostics.json'
            self.assertEqual(list(bundle.diagnostics), read(diagnostics) if diagnostics.exists() else [])
            context = checked_context(tables, bundle)
            self.assertEqual(context.descriptor['evidence']['host_session_id'], pin['expected_session'])
            self.assertEqual(context.descriptor['evidence']['session_id'], pin['evidence']['session_id'])
            self.assertEqual(driver.observe(bundle.next_cursor)[0], tables)
            result = assemble_driver_capture(cell, driver, {}, profile=profile, cursor=bundle.next_cursor)
        terminal.assert_not_called()
        self.assertEqual(result['reads'], dict(tables.reads))
        self.assertEqual(result['inputs'][2], read(fixture/'expected-observation.json'))
        self.assertEqual(result['classifications'], read(fixture/'expected-classifications.json'))
        self.assertEqual(result['diagnostics'], read(fixture/'expected-diagnostics.json'))
        self.assertEqual(result['verdict'], read(fixture/'expected-result.json'))
        return result

    def test_native_delivery(self):
        channel = self.capture('healthy-channel-double')
        self.assertEqual([e['transport'] for e in events(channel['inputs'][2], 'transcript_recorded')], ['channel']*2)
        peer = self.capture('healthy-inbox-single')
        self.assertEqual(events(peer['inputs'][2], 'transcript_recorded')[0]['transport'], 'inbox')
        for name in ('rejected-peer-provenance', 'rejected-peer-prefix'):
            rejected = self.capture(name)
            self.assertFalse(events(rejected['inputs'][2], 'transcript_recorded'))
            self.assertTrue(any(c['accept'] is False for c in rejected['classifications']))
            self.assertEqual(derive(*rejected['inputs'])['received'], {'41001': 0})

    def test_fetch(self):
        for name in ('healthy-fetch-double', 'healthy-fetch-permuted'):
            r = self.capture(name)
            o = r['inputs'][2]
            request, produced, recorded = [events(o, m)[0] for m in
                                          ('fetch_requested', 'fetch_result_produced', 'fetch_result_recorded')]
            self.assertEqual({e['operation_id'] for e in (request, produced, recorded)}, {'call-original'})
            held = r['reads']['held-fetch-response']['records'][0]
            self.assertEqual(held['id'], 'rpc-original')
            self.assertNotEqual(held['id'], recorded['operation_id'])
            self.assertEqual(produced['members'], recorded['members'])
            self.assertEqual(len(next(s for s in o['queue_snapshots'] if s['barrier_id']=='fetch_result_held')['rows']), 2)
            self.assertLess(produced['position']['time_ms'], recorded['position']['time_ms'])
        wrong = self.capture('wrong-recorded-fetch-token')
        self.assertTrue(events(wrong['inputs'][2], 'fetch_result_produced'))
        self.assertFalse(events(wrong['inputs'][2], 'fetch_result_recorded'))
        self.assertTrue(any(c['accept'] is False for c in wrong['classifications']))
        candidate = wrong['host_read']['records'][-1]['message']['content'][0]['content']
        held = wrong['reads']['held-fetch-response']['records'][0]['result']['content']
        self.assertNotEqual(fetch_trailer(result_text(candidate))['token'], fetch_trailer(result_text(held))['token'])

    def test_occurrence_identity(self):
        normal = self.capture('healthy-channel-double')
        repeated = self.capture('healthy-channel-repeated')
        self.assertEqual(events(normal['inputs'][2], 'transcript_recorded'), events(repeated['inputs'][2], 'transcript_recorded'))
        duplicate = self.capture('duplicate-host-delivery')
        host = events(duplicate['inputs'][2], 'transcript_recorded')
        self.assertEqual(len(host), 3)
        self.assertNotEqual(host[0]['payload']['host_record_id'], host[2]['payload']['host_record_id'])
        self.assertNotEqual(host[0]['delivery_id'], host[2]['delivery_id'])
        self.assertEqual(host[0]['members'], host[2]['members'])
        conflict = self.capture('conflicting-host-uuid')
        self.assertTrue(any('UUID content conflict' in d['code'] for d in conflict['diagnostics']))
        self.assertEqual(next(s for s in conflict['inputs'][2]['streams'] if s['id']=='host')['state'], 'malformed')

    def test_source_identity(self):
        good = self.capture('healthy-channel-double')
        bad = self.capture('duplicate-source-replacing-another')
        self.assertEqual([s['source_id'] for s in bad['inputs'][2]['sources']], ['41001', '41002'])
        host = events(bad['inputs'][2], 'transcript_recorded')
        self.assertEqual(host[0]['members'][0]['source_ids'], host[1]['members'][0]['source_ids'])
        self.assertEqual(good['host_read']['text'].count('MATRIX_SAMPLE'), bad['host_read']['text'].count('MATRIX_SAMPLE'))
        wrong = self.capture('occurrence-host-token')
        self.assertNotEqual(events(wrong['inputs'][2], 'transcript_recorded')[0]['token'],
                            events(wrong['inputs'][2], 'attempt_reserved')[0]['token'])
        self.assertFalse(events(wrong['inputs'][2], 'transcript_recorded')[0]['collection_complete'])

    def test_ownership(self):
        for name in ('wrong-host-session', 'misattributed-transcript'):
            r = self.capture(name)
            host = events(r['inputs'][2], 'transcript_recorded')[0]
            reservation = events(r['inputs'][2], 'attempt_reserved')[0]
            owner = next(s for s in r['inputs'][2]['streams'] if s['id']=='host')
            self.assertNotEqual(host['scope']['host_session_id'],
                                (reservation if name=='wrong-host-session' else owner)['scope']['host_session_id'])
            self.assertFalse(host['collection_complete'])
        reused = self.capture('reused-pid-different-epoch')
        o = reused['inputs'][2]
        self.assertNotEqual(events(o, 'receipt_accepted')[0]['scope']['connection_epoch_id'],
                            events(o, 'attempt_reserved')[0]['scope']['connection_epoch_id'])

    def test_collection_boundaries(self):
        missing = self.capture('missing-final-boundary')
        self.assertFalse(any(b['id']=='final' for b in missing['inputs'][2]['barriers']))
        for name in ('short-final-cut', 'early-final-cut'):
            r = self.capture(name)
            final = next(b for b in r['inputs'][2]['barriers'] if b['id']=='final')
            self.assertFalse(final['collection_complete'])
            self.assertNotEqual(final['stream_cutoffs']['host'], r['host_read']['lines'])

    def test_collection_checked_reads(self):
        for name, state in (('missing-host', 'missing'), ('malformed-host-json', 'malformed'),
                            ('malformed-host-utf8', 'malformed'), ('malformed-host-tail', 'partial')):
            with self.subTest(capture=name):
                r = self.capture(name)
                self.assertEqual(r['host_read']['state'], state)
                self.assertFalse(events(r['inputs'][2], 'transcript_recorded'))
                self.assertFalse(events(r['inputs'][2], 'fetch_result_recorded'))
                self.assertFalse(r['inputs'][2]['collection_complete'])

    def test_truncation_keeps_prior_bytes_and_invalidates_delivery(self):
        r = self.capture('truncated-host')
        self.assertEqual(r['host_read']['state'], 'truncated')
        prior = next(data for ref, data in r['bundle'].artifacts if ref.artifact_id=='prior-host-records')
        self.assertEqual(prior, (ROOT/'truncated-host/initial-records.jsonl').read_bytes())
        self.assertEqual(len(events(r['inputs'][2], 'transcript_recorded')), 1)
        self.assertFalse(events(r['inputs'][2], 'transcript_recorded')[0]['collection_complete'])

    def test_isolation_before_open(self):
        for kind in ('outside', 'symlink'):
            r = self.capture('isolation-'+kind)
            self.assertEqual(r['reads']['host-records']['state'], 'unreadable')
            self.assertIn('escapes capture root', r['reads']['host-records']['detail'])
            self.assertFalse(r['host_read']['records'])
            self.assertFalse(events(r['inputs'][2], 'fetch_result_recorded'))
            self.assertFalse(any(b'must never be read' in data for _, data in r['bundle'].artifacts))

    def test_contract(self):
        missing = self.capture('missing-contracts')
        self.assertFalse(missing['inputs'][2]['contract_observations'])
        downgrade = self.capture('contract-downgrade')
        self.assertEqual(downgrade['inputs'][1]['axes']['negotiation'], 'v1')
        self.assertEqual({s['axes']['negotiation'] for s in downgrade['inputs'][2]['contract_observations']}, {'none'})
        old = self.capture('contract-predecessor')
        self.assertEqual({s['scope']['connection_epoch_id'] for s in old['inputs'][2]['contract_observations']}, {'epoch-predecessor'})
        self.assertEqual(events(old['inputs'][2], 'attempt_reserved')[0]['scope']['connection_epoch_id'], 'epoch-original')

    def test_session(self):
        fresh = self.capture('session-fresh')['inputs'][2]['session_proof']
        self.assertTrue(fresh['proven'])
        self.assertEqual(fresh['freshness'], 'transcript_free_at_injection')
        eager = self.capture('session-eager')['inputs'][2]['session_proof']
        self.assertFalse(eager['proven'])
        resumed = self.capture('healthy-channel-double')['inputs'][2]['session_proof']
        self.assertEqual((resumed['posture'], resumed['proven']), ('resumed', True))
        mismatched = self.capture('occurrence-selected-session')['inputs'][2]['session_proof']
        self.assertFalse(mismatched['proven'])
        self.assertNotEqual(mismatched['scope']['host_session_id'], resumed['scope']['host_session_id'])
        wrong = self.capture('wrong-selected-transcript')
        self.assertIn('wrong', wrong['host_read']['text'])
        self.assertFalse(events(wrong['inputs'][2], 'transcript_recorded')[0]['collection_complete'])

    def test_state(self):
        for state in ('foreground', 'background'):
            r = self.capture('state-'+state)
            self.assertEqual(r['inputs'][2]['state_proof']['state'], state)
            self.assertTrue(r['inputs'][2]['state_proof']['proven'])
            controls = next(data for ref, data in r['bundle'].artifacts if ref.artifact_id=='controls')
            filesystem = json.loads(controls.splitlines()[0])['facts']['filesystem']
            invocation = r['host_read']['records'][0]['message']['content'][0]['input']
            self.assertTrue(filesystem['marker_exists'])
            self.assertEqual(invocation['command'], filesystem['command'])
            self.assertEqual(invocation['run_in_background'], state=='background')
        for name in ('state-missing-marker', 'state-stale-marker'):
            r = self.capture(name)
            self.assertIsNone(r['inputs'][2]['state_proof'])
            self.assertFalse(any(e['payload'].get('detail')=='state_sample' for e in r['inputs'][2]['events']))

    def test_barriers(self):
        for state in ('startup', 'reconnect'):
            r = self.capture('barrier-'+state)
            o = r['inputs'][2]
            ready = next(b for b in o['barriers'] if b['id']=='ready')
            before = next(s for s in o['queue_snapshots'] if s['barrier_id']=='before_ready')
            self.assertEqual(len(before['rows']), 2)
            self.assertTrue(ready['collection_complete'])
            self.assertFalse(derive(*r['inputs'])['offered_before_ready'])
            self.assertTrue(all(e['position']['seq'] > ready['stream_cutoffs']['broker']
                                for e in events(o, 'attempt_reserved')))
            if state=='reconnect':
                proxy = next(data for ref, data in r['bundle'].artifacts if ref.artifact_id=='proxy')
                rows = [json.loads(line) for line in proxy.splitlines()]
                held = next(row for row in rows if row['event']=='initialized_waiting')
                predecessor = next(row for row in rows if row['event']=='channel_notify')
                self.assertGreater(predecessor['time'], held['time'])
                self.assertNotEqual(predecessor['pid'], held['pid'])
                self.assertFalse(events(o, 'delivery_offered'))

    def test_submission_artifacts_are_not_delivery_witnesses(self):
        for state in ('unsent', 'wrapped', 'submitted', 'unknown', 'exited'):
            r = self.capture('submission-'+state)
            pane = next(data for ref, data in r['bundle'].artifacts if ref.artifact_id=='pane')
            self.assertEqual(pane.decode(), read(ROOT/('submission-'+state)/'terminal.json')['pane'])
            self.assertEqual({e['artifact_ref']['artifact_id'] for e in events(r['inputs'][2], 'transcript_recorded')}, {'host-records'})

    def test_export_fresh_interpreter(self):
        for name in ('healthy-channel-double', 'duplicate-host-delivery', 'occurrence-host-token', 'healthy-fetch-double'):
            r = self.capture(name)
            with tempfile.TemporaryDirectory() as directory:
                export_replay_capture(r, Path(directory))
                code = ('import json,sys; from pathlib import Path; '
                        'from test_hostdriver_conformance import replay_exported; '
                        'print(json.dumps(replay_exported(Path(sys.argv[1]))))')
                child = subprocess.run([sys.executable, '-c', code, directory], cwd=Path(__file__).parent,
                                       capture_output=True, text=True, timeout=30)
                self.assertEqual(child.returncode, 0, child.stderr)
                fresh = json.loads(child.stdout)
                saved = read(Path(directory)/'verdict-inputs.json')
                self.assertEqual(fresh['verdict'], r['verdict'])
                expected = deepcopy(saved['observation'])
                actual = deepcopy(fresh['inputs'][2])
                for observation in (expected, actual):
                    for artifact in observation['artifacts']:
                        artifact['content_digest'] = None
                self.assertEqual(actual, expected)
                self.assertEqual(derive(*fresh['inputs']), derive(**saved))
                self.assertEqual(actual['provenance']['kind'], 'raw_replay')


if __name__ == '__main__':
    unittest.main()
