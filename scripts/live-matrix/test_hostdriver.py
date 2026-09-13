"""Hermetic controls for the protocol wrapper, without live host prerequisites."""
from copy import deepcopy
from dataclasses import asdict, FrozenInstanceError, replace
import importlib
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch

import driver
from collect import build_verdict_inputs, collect
from host import Host, HostSetupError
from hostdriver import (Adapter, ArtifactBundle, BarrierPoint, Broker, ControlJournal, Deadline, DriverError,
                        ObservationCursor, ObserverTables, RunProfile, Scratch, Timeouts, Workload,
                        remaining_timeout, resolve_contract, validate_description)
from hostdrivers import registry, Registry
from hostdrivers.claude import ClaudeHostDriver, describe
from matrix import Cell, cells, selection, selection_summary


class Clock:
    def __init__(self): self.now = 100.0
    def monotonic(self): return self.now
    def time(self): return self.now
    def sleep(self, seconds): self.now += seconds


class DriverControls(unittest.TestCase):
    def prepare(self, *, cell=None, execution='live', backend_factory=None):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        cell = cell or Cell('channel', 'idle', 'resumed', 'text', 'single')
        description = describe('2.1.267', 'matrix')
        case = next(item for item in description['capabilities']['cases'] if item['id'] == cell.name)
        scratch = Scratch(root, 'scratch', 'run', root / 'cwd', root, ControlJournal(root / 'control'))
        scenario, _, _ = build_verdict_inputs(cell, {'run_id': 'run', 'route_id': 'test-inject/42'})
        profile = RunProfile(description, case, scenario, resolve_contract(description, case), Path('/synthetic/claude'),
                             '2.1.267', 'matrix', None, execution=execution, artifact_source=root if execution == 'replay' else None)
        backend = Host.__new__(Host)
        backend.root, backend.control, backend.cwd = root, root / 'control', root / 'cwd'
        backend.cwd.mkdir()
        backend.cell, backend.timeout = cell, 90
        backend.command = ['/synthetic/claude']
        backend.socket = root / 'tmux.sock'
        backend.development_warning_sent = False
        backend.env = {}
        self.pane = '────\n❯\n────'
        self.typing_lands = True
        self.enter_submits = True
        self.native_session = 'conversation'
        self.events = []
        self.keys = []
        def terminal(*args, **kwargs):
            self.keys.append(args)
            if args[0] == 'new-session':
                with (backend.control / 'sessions.jsonl').open('a') as output:
                    output.write(json.dumps(dict(session_id=self.native_session, transcript_path=str(root / 'transcript.jsonl'))) + '\n')
                self.events.extend([dict(event='proxy_started', pid=7, time=1000, inbox_env=True),
                                    dict(event='attached', pid=7, time=1000)])
            if args[0] == 'send-keys' and '-l' in args and self.typing_lands:
                self.pane = '────\n❯ ' + args[-1] + '\n────'
            if args[0] == 'send-keys' and args[-1] == 'Enter' and self.enter_submits:
                self.pane = '────\n❯\n────'
            return SimpleNamespace(stdout=self.pane, stderr='', returncode=1 if args[0] == 'has-session' else 0)
        backend.tmux = Mock(side_effect=terminal)
        backend.events = lambda name=None, after=0: [event for event in self.events if event['time'] >= after and (name is None or event['event'] == name)]
        self.clock = Clock()
        profile = replace(profile, clock=self.clock)
        wrapper = ClaudeHostDriver(backend_factory=backend_factory or (lambda *args: backend))
        broker = Broker(Path('/synthetic/broker'), root / 'broker/c3.sock', root / 'broker', 'run', 'test-inject/42')
        adapter = Adapter(Path('/synthetic/adapter'), 'build')
        wrapper.prepare(scratch, adapter, broker, profile)
        self.wrapper, self.backend, self.scratch, self.profile = wrapper, backend, scratch, profile
        self.broker, self.adapter = broker, adapter
        return wrapper

    def point(self, kind, action, *, name=None, release=None, session=None):
        name = name or {'readiness': 'ready', 'fetch_result': 'fetch', 'workload_finished': 'workload'}[kind]
        return BarrierPoint(name, kind, action, self.profile.description['operations']['barriers'][kind][name],
                            session_handle_id=session, release_id=release.release_id if release else None)

    def checkpoint(self, name):
        return self.wrapper.barrier(self.point('checkpoint', 'sample', name=name))

    def hold(self):
        armed = self.wrapper.barrier(self.point('readiness', 'arm'))
        self.wrapper.launch()
        self.events.append(dict(event='initialized_waiting', pid=7, time=1001))
        held = self.wrapper.barrier(self.point('readiness', 'wait', release=armed))
        return armed, held

    def test_transcript_selection_rejects_symlink_escape(self):
        self.prepare()
        outside = self.scratch.root.parent / (self.scratch.root.name + '-outside.jsonl')
        outside.write_text('{"private": "must never be read"}\n')
        self.addCleanup(outside.unlink)
        inside = self.scratch.root / 'transcript.jsonl'
        inside.symlink_to(outside)
        self.wrapper.launch()
        with patch('host.read_jsonl', wraps=__import__('host').read_jsonl) as read:
            with self.assertRaisesRegex(RuntimeError, 'private scratch transcript'):
                self.backend.records()
        self.assertEqual([call.args[0] for call in read.call_args_list], [self.backend.control / 'sessions.jsonl'])

    def test_preparation_detects_mutated_profile_and_keeps_pinned_contract(self):
        self.prepare()
        self.profile.contract['axes']['receipt_type'] = 'none'
        with self.assertRaises(DriverError):
            self.wrapper.prepare(self.scratch, self.adapter, self.broker, self.profile)
        self.assertEqual(self.wrapper._profile.contract['axes']['receipt_type'], 'transcript')

    def test_submission_unrecognized_unsent_composer_fails_closed(self):
        wrapper = self.prepare()
        wrapper.launch()
        # No two rules: baseline composer() returned '', so this unsent text
        # used to satisfy "prompt disappeared" on the first Enter.
        self.typing_lands = False
        self.pane = 'Changed layout\n❯ prompt still unsent'
        with patch('host.time.sleep'), self.assertRaises(DriverError) as caught:
            wrapper.send_prompt('prompt still unsent')
        self.assertEqual(caught.exception.kind, 'unrecognized_state')
        self.assertTrue(caught.exception.submission_uncertain)
        self.assertEqual(caught.exception.method, 'send_prompt')
        self.assertIn('unrecognized composer', caught.exception.detail)
        self.assertEqual([args for args in self.keys if args[0] == 'send-keys'], [
            ('send-keys', '-t', 'matrix', '-l', '--', 'prompt still unsent')])
        with self.assertRaises(DriverError):
            wrapper.send_prompt('prompt still unsent')
        self.assertEqual(sum('-l' in args for args in self.keys), 1)
        self.assertNotIn('composer_transition', (self.backend.control / 'operations.jsonl').read_text())

    def test_submission_empty_recognized_composer_has_no_proof_or_retry(self):
        wrapper = self.prepare()
        wrapper.launch()
        self.typing_lands = False
        with self.assertRaisesRegex(DriverError, 'prompt not observed') as caught:
            wrapper.send_prompt('dropped prompt')
        self.assertTrue(caught.exception.submission_uncertain)
        self.assertEqual(caught.exception.kind, 'unrecognized_state')
        with self.assertRaises(DriverError):
            wrapper.send_prompt('dropped prompt')
        self.assertEqual([keys for keys in self.keys if keys[0] == 'send-keys'],
                         [('send-keys', '-t', 'matrix', '-l', '--', 'dropped prompt')])
        self.assertNotIn('composer_transition', (self.backend.control / 'operations.jsonl').read_text())

    def test_setup_json_error_preserves_exception_type(self):
        error = json.JSONDecodeError('synthetic malformed settings', '{', 1)
        with self.assertRaises(DriverError) as caught:
            self.prepare(backend_factory=Mock(side_effect=error))
        self.assertEqual(str(caught.exception), f'JSONDecodeError: {error}')

    def test_submission_journal_failure_cannot_authorize_retry(self):
        wrapper = self.prepare()
        wrapper.launch()
        with patch('host.time.sleep'), patch.object(self.scratch.journal, 'append', side_effect=OSError('journal unavailable')):
            with self.assertRaises(DriverError) as caught:
                wrapper.send_prompt('prompt')
        self.assertEqual(caught.exception.kind, 'io_error')
        self.assertTrue(caught.exception.submission_uncertain)
        with self.assertRaises(DriverError):
            wrapper.send_prompt('prompt')
        self.assertEqual(sum('-l' in keys for keys in self.keys), 1)
        wrapper.stop(False)

    def test_submission_proof_and_timeout_and_exit(self):
        wrapper = self.prepare()
        wrapper.launch()
        with patch('host.time.sleep'):
            proof = wrapper.send_prompt('prompt')
        self.assertEqual(proof.mechanism, 'composer_transition')
        facts = self.scratch.journal.entries[proof.positions[0].seq]['facts']
        self.assertIn('prompt', facts['composer_before'])
        self.assertNotIn('prompt', facts['composer'])
        self.assertEqual(len(proof.text_digest), 64)
        self.assertTrue(proof.artifact_refs)
        self.assertIsNone(proof.scope.session_id)
        with self.assertRaises(FrozenInstanceError):
            proof.submission_id = 'different'
        self.enter_submits = False
        self.pane = '────\n❯ prompt\n────'
        with patch('host.time.sleep'), self.assertRaises(DriverError) as caught:
            wrapper.send_prompt('prompt')
        self.assertEqual(caught.exception.kind, 'timeout')
        self.assertTrue(caught.exception.submission_uncertain)
        wrapper.stop(False)
        wrapper = self.prepare()
        wrapper.launch()
        self.backend.tmux.side_effect = lambda *a, **k: SimpleNamespace(stdout='', stderr='no server', returncode=1 if a[0] == 'capture-pane' else 0)
        with patch('host.time.sleep'), self.assertRaises(DriverError) as caught:
            wrapper.send_prompt('prompt')
        self.assertEqual(caught.exception.kind, 'host_exited')
        self.assertTrue(caught.exception.submission_uncertain)

    def test_readiness_ownership_idempotent_release_and_stale_handles(self):
        wrapper = self.prepare()
        armed, held = self.hold()
        self.assertEqual((armed.status, held.status), ('armed', 'held'))
        self.assertEqual(armed.release_id, held.release_id)
        released = wrapper.barrier(self.point('readiness', 'release', release=held))
        self.assertEqual(released.status, 'released')
        self.assertTrue((self.backend.control / 'release-initialized-7').exists())
        self.assertEqual(wrapper.barrier(self.point('readiness', 'release', release=held)).release_id, held.release_id)
        for point in (replace(self.point('readiness', 'release', release=held), release_id='foreign'),
                      replace(self.point('readiness', 'release', release=held), session_handle_id='foreign'),
                      self.point('readiness', 'wait', release=held)):
            with self.assertRaises(DriverError) as caught:
                wrapper.barrier(point)
            self.assertEqual(caught.exception.kind, 'invalid_request')
        wrapper.barrier(self.point('readiness', 'arm'))
        self.assertFalse((self.backend.control / 'release-initialized-7').exists())
        with self.assertRaises(DriverError):
            wrapper.barrier(self.point('readiness', 'release', release=held))

    def test_reconnect_scopes_successor_even_with_reused_pid(self):
        wrapper = self.prepare()
        selected = wrapper.launch()
        self.checkpoint('attachment')
        old = dict(event='initialized_waiting', pid=7, time=1001)
        self.events.append(old)
        wrapper.barrier(self.point('readiness', 'arm'))
        self.backend.reconnect = Mock(return_value=old)
        with self.assertRaises(DriverError) as caught:
            wrapper.reconnect(selected)
        self.assertEqual(caught.exception.kind, 'identity_conflict')
        def replacement(keys):
            self.events.extend([dict(event='proxy_started', pid=7, time=1002),
                                dict(event='initialized_waiting', pid=7, time=1003)])
            return self.events[-1]
        self.backend.reconnect.side_effect = replacement
        result = wrapper.reconnect(selected)
        self.assertEqual(result.previous_session.host_session_id, result.current_session.host_session_id)
        self.assertIsNotNone(result.previous_session.connection_epoch_id)
        self.assertNotEqual(result.previous_session.connection_epoch_id, result.current_session.connection_epoch_id)
        self.events.append(dict(event='channel_notify', pid=6, time=1002))
        self.assertEqual(self.scratch.journal.legacy_evidence(self.checkpoint('before_ready')),
                         {'attempt_before_ready_observations': []})
        self.events.append(dict(event='channel_notify', pid=7, time=1004))
        self.assertEqual(self.scratch.journal.legacy_evidence(self.checkpoint('before_ready')),
                         {'attempt_before_ready_observations': ['gated proxy forwarded a channel notification']})

    def test_resume_binds_observed_session_and_rejects_wrong_target(self):
        wrapper = self.prepare()
        session = wrapper.launch()
        self.assertEqual(session.host_session_id, 'conversation')
        with patch('host.time.sleep'):
            resume = wrapper.stop(True)
        self.assertEqual(wrapper.stop(True), resume)
        with self.assertRaises(DriverError):
            wrapper.launch(replace(resume, scratch_id='foreign'))
        selected = wrapper.launch(resume)
        self.assertEqual(selected.posture, 'resumed')
        self.assertIn('--continue', self.keys[-1][-1])
        self.assertEqual(selected.host_session_id, resume.host_session_id)
        with patch('host.time.sleep'):
            resume = wrapper.stop(True)
        self.native_session = 'wrong-conversation'
        with self.assertRaises(DriverError) as caught:
            wrapper.launch(resume)
        self.assertEqual(caught.exception.kind, 'identity_conflict')
        wrapper.stop(False)

    def test_invalid_session_and_cursor_handles(self):
        wrapper = self.prepare()
        session = wrapper.launch()
        with self.assertRaises(DriverError):
            wrapper.reconnect(replace(session, handle_id='foreign'))
        cursor = ObservationCursor(driver_instance_id='foreign', run_id='run', snapshot_id='snapshot', extents=())
        with self.assertRaises(DriverError) as caught:
            wrapper.observe(cursor)
        self.assertEqual(caught.exception.kind, 'invalid_request')
        with self.assertRaises(DriverError):
            wrapper.launch()

    def test_fetch_barrier_holds_actual_response(self):
        wrapper = self.prepare(cell=Cell('fetch', 'idle', 'resumed', 'text', 'single'))
        wrapper.launch()
        armed = wrapper.barrier(self.point('fetch_result', 'arm'))
        frame = {'jsonrpc': '2.0', 'id': 'actual-rpc', 'result': {'content': [{'type': 'text', 'text': 'actual held result'}]}}
        self.events.append(dict(event='fetch_result_waiting', pid=7, time=1001, frame=frame))
        held = wrapper.barrier(self.point('fetch_result', 'wait', release=armed))
        self.assertEqual(self.scratch.journal.legacy_evidence(held), {'fetch_result': frame['result']})
        self.assertFalse((self.backend.control / 'release-fetch').exists())
        wrapper.barrier(self.point('fetch_result', 'release', release=held))
        self.assertTrue((self.backend.control / 'release-fetch').exists())

    def test_injection_revalidates_workload(self):
        for state in ('foreground', 'background'):
            wrapper = self.prepare(cell=Cell('channel', state, 'resumed', 'text', 'single'))
            wrapper.launch()
            marker = self.backend.cwd / 'tool-running'
            marker.touch()
            command = 'observed requested command'
            self.backend.tool_state = Mock(return_value=dict(command=command, background=state == 'background', observed=100))
            self.backend.records = Mock(return_value=[{'message': {'content': [dict(type='tool_use', name='Bash', input=dict(command=command, run_in_background=state == 'background'))]}}])
            evidence = wrapper.enter_state(state, Workload('sleep', 35000, 'workload'))
            self.assertEqual(evidence.state, state)
            self.checkpoint('injection')
            marker.unlink()
            with self.assertRaises(DriverError) as caught:
                self.checkpoint('injection')
            self.assertEqual(caught.exception.kind, 'unrecognized_state')

    def test_session_arrival_checks_fresh_transcript(self):
        wrapper = self.prepare(cell=Cell('channel', 'idle', 'fresh', 'text', 'single'))
        wrapper.launch()
        self.checkpoint('session')
        (self.scratch.root / 'transcript.jsonl').write_text('{}\n')
        with self.assertRaisesRegex(DriverError, 'transcript before injection'):
            self.checkpoint('session')

    def test_observation_retains_legacy_poll_and_incomplete_collection(self):
        wrapper = self.prepare()
        wrapper.launch()
        tables, artifacts = wrapper.observe()
        self.assertEqual(tables, ObserverTables())
        self.assertEqual(artifacts.artifacts[0][1], self.pane.encode())
        self.assertIsNone(artifacts.next_cursor)
        result = collect(Cell('channel', 'idle', 'resumed', 'text', 'single'), wrapper,
                         self.scratch.root, self.scratch.root / 'output', {'setup_errors': [], 'injected': True})
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('evidence collection incomplete', result['reasons'])
        wrapper.stop(False)
        self.assertEqual(wrapper.observe()[1].artifacts[0][1], artifacts.artifacts[0][1])
        self.assertEqual(wrapper.records(), [])

    def test_replay_and_construction_do_not_execute(self):
        factory = Mock(side_effect=AssertionError('backend execution'))
        wrapper = self.prepare(execution='replay', backend_factory=factory)
        self.assertEqual(wrapper.observe()[0], ObserverTables())
        with self.assertRaises(DriverError) as caught:
            wrapper.launch()
        self.assertEqual(caught.exception.kind, 'unsupported')
        wrapper.stop(False)
        factory.assert_not_called()
        with patch('subprocess.run', side_effect=AssertionError('process execution')), \
                patch('subprocess.Popen', side_effect=AssertionError('process execution')), \
                patch('pathlib.Path.home', side_effect=AssertionError('credential lookup')):
            import hostdrivers
            importlib.reload(hostdrivers)
            registry().create('claude').describe('2.1.267', 'matrix')
            cells()

    def test_changed_preparation_and_partial_cleanup(self):
        wrapper = self.prepare()
        wrapper.prepare(self.scratch, self.adapter, self.broker, self.profile)
        with self.assertRaises(DriverError):
            wrapper.prepare(self.scratch, self.adapter, replace(self.broker, run_id='foreign'), self.profile)
        wrapper.stop(False)
        wrapper.stop(False)
        self.assertEqual(sum(args[0] == 'kill-server' for args in self.keys), 1)
        class Partial:
            closed = 0
            def __init__(self, *args): raise OSError('synthetic setup failure')
            def close(self): type(self).closed += 1
        fresh = ClaudeHostDriver(backend_factory=Partial)
        with self.assertRaises(DriverError) as caught:
            fresh.prepare(self.scratch, self.adapter, self.broker, self.profile)
        self.assertEqual(caught.exception.kind, 'io_error')
        fresh.stop(False)
        fresh.stop(False)
        self.assertEqual(Partial.closed, 1)
        with self.assertRaises(DriverError):
            fresh.prepare(self.scratch, self.adapter, self.broker, self.profile)

    def test_prepare_rejects_unbound_paths_profiles_and_unknown_recipes(self):
        self.prepare()
        for broker in (replace(self.broker, endpoint=Path('/foreign/c3.sock')),
                       replace(self.broker, run_id='foreign')):
            with self.assertRaises(DriverError) as caught:
                ClaudeHostDriver().prepare(self.scratch, self.adapter, broker, self.profile)
            self.assertEqual(caught.exception.kind, 'invalid_request')
        for change in ({'freshness': 'new_conversation'}, {'count': 2}, {'resume_requirement': 'attachment_recovery'}):
            changed = replace(self.profile, scenario=dict(self.profile.scenario, **change))
            with self.assertRaises(DriverError) as caught:
                ClaudeHostDriver().prepare(self.scratch, self.adapter, self.broker, changed)
            self.assertEqual(caught.exception.kind, 'invalid_request')
        description = describe('99.0.0', 'matrix')
        case = next(case for case in description['capabilities']['cases'] if case['id'] == self.profile.case['id'])
        unknown = replace(self.profile, description=description, case=case, host_version='99.0.0')
        with self.assertRaises(DriverError) as caught:
            ClaudeHostDriver().prepare(self.scratch, self.adapter, self.broker, unknown)
        self.assertEqual(caught.exception.kind, 'missing_prerequisite')


class ProfileControls(unittest.TestCase):
    def test_claude_contract_axes_match_baseline_without_selecting_from_evidence(self):
        description = describe('2.1.267', 'matrix')
        validate_description(description)
        self.assertEqual(set(asdict(cells()[0])), {'transport', 'state', 'session', 'kind', 'burst'})
        for cell in cells(description):
            case = cell.capability_case
            self.assertEqual(case['freshness'], 'transcript_free_at_injection' if cell.session == 'fresh' else 'not_applicable')
            self.assertEqual(case['resume_requirement'], 'delivery_only')
            contract = resolve_contract(description, case)
            _, original, _ = build_verdict_inputs(cell, {})
            self.assertEqual(dict(contract, id=original['id']), original)
        description['capabilities']['cases'].clear()
        self.assertEqual(len(describe('2.1.267', 'matrix')['capabilities']['cases']), 180)

    def test_case_feasibility_is_driver_data_and_unknowns_are_selected(self):
        description = describe('2.1.267', 'matrix')
        description['driver_id'] = 'arbitrary'
        for case in description['capabilities']['cases']:
            case.update(feasibility='feasible', reason='')
        self.assertEqual(len(selection('*', description)[1]), 180)
        self.assertFalse(Cell('channel', 'foreground', 'fresh', 'text', 'single').infeasible)
        removed = description['capabilities']['cases'].pop(0)
        for prerequisite in description['prerequisites']:
            prerequisite['case_ids'].remove(removed['id'])
        matched, selected, _ = selection(removed['id'], description)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].feasibility, 'unknown')
        self.assertIn('1 unknown', selection_summary(removed['id'], description))
        self.assertEqual(matched, selected)
        unknown = describe('99.0.0', 'unverified-mode')
        self.assertEqual(sum(cell.feasibility == 'unknown' for cell in cells(unknown)), 126)
        self.assertEqual(len(selection('*', unknown)[1]), 126)

    def test_invalid_description_and_ambiguous_contract_fail(self):
        description = describe('2.1.267', 'matrix')
        case = deepcopy(description['capabilities']['cases'][0])
        case['contract_ids'] = [contract['id'] for contract in description['contracts']]
        with self.assertRaises(DriverError):
            resolve_contract(description, case)
        description['capabilities']['cases'].append(description['capabilities']['cases'][0])
        with self.assertRaises(DriverError):
            cells(description)
        for version, mode in (('', 'matrix'), ('2.1.267', ''), (True, 'matrix')):
            with self.assertRaises(DriverError):
                describe(version, mode)
        registered = Registry()
        registered.register('test', ClaudeHostDriver)
        with self.assertRaises(DriverError):
            registered.register('test', ClaudeHostDriver)

    def test_description_closed_records_and_all_bindings(self):
        good = describe('2.1.267', 'matrix')
        validate_description(good)
        case = good['capabilities']['cases'][0]
        self.assertEqual(resolve_contract(good, deepcopy(case)), good['contracts'][0])
        # Every required field is tested for absence and wrong type, including
        # nested closed records, rather than just the reviewer's examples.
        def records(value, path=()):
            if isinstance(value, dict):
                yield path, value
                for key, child in value.items():
                    yield from records(child, path + (key,))
            elif isinstance(value, list) and value:
                yield from records(value[0], path + (0,))
        for path, record in records(good):
            for key in record:
                for missing in (True, False):
                    bad = deepcopy(good)
                    target = bad
                    for part in path:
                        target = target[part]
                    if missing:
                        del target[key]
                    else:
                        target[key] = object()
                    # Dynamic maps may omit entries; they have no required keys.
                    if missing and (path == ('operations', 'barriers') or
                                    path[:2] == ('operations', 'barriers')):
                        continue
                    with self.subTest(path=path, key=key, missing=missing):
                        with self.assertRaises(DriverError) as caught:
                            validate_description(bad)
                        self.assertEqual(caught.exception.kind, 'invalid_request')
        for mutation in (
            lambda d: d.pop('operations'),
            lambda d: d['contracts'][0].update(capability_id='foreign'),
            lambda d: d['contracts'][0]['timing']['live'].update(limit_ms=0),
            lambda d: d['contracts'][0]['axes']['accepted_modes'].append('channel'),
            lambda d: d['capabilities']['cases'][0]['contract_ids'].append(case['contract_ids'][0]),
            lambda d: d['capabilities']['cases'][0]['contract_ids'].append('undeclared'),
            lambda d: d['contracts'].append(deepcopy(d['contracts'][0])),
            lambda d: d['prerequisites'][0]['case_ids'].append('foreign'),
        ):
            bad = deepcopy(good)
            mutation(bad)
            with self.assertRaises(DriverError) as caught:
                resolve_contract(bad, case)
            self.assertEqual(caught.exception.kind, 'invalid_request')
        for forged in (None, {}, dict(case, id='forged'), dict(case, reason='forged')):
            with self.assertRaises(DriverError) as caught:
                resolve_contract(good, forged)
            self.assertEqual(caught.exception.kind, 'invalid_request')
        for identity in ([], True, '', 'undeclared'):
            with self.assertRaises(DriverError) as caught:
                resolve_contract(good, case, identity)
            self.assertEqual(caught.exception.kind, 'invalid_request')

    def test_contract_selection_requires_one_bound_choice(self):
        description = describe('2.1.267', 'matrix')
        case = description['capabilities']['cases'][0]
        alternate = deepcopy(description['contracts'][0])
        alternate['id'] = 'alternate'
        description['contracts'].append(alternate)
        case['contract_ids'].append('alternate')
        validate_description(description)
        with self.assertRaisesRegex(DriverError, 'select exactly one') as caught:
            resolve_contract(description, case)
        self.assertEqual(caught.exception.kind, 'invalid_request')
        self.assertEqual(resolve_contract(description, case, 'alternate'), alternate)
        case['contract_ids'].clear()
        with self.assertRaises(DriverError) as caught:
            resolve_contract(description, case)
        self.assertEqual(caught.exception.kind, 'invalid_request')

    def test_operation_deadline_does_not_reset_at_nested_checkpoints(self):
        clock = Clock()
        with Deadline(10, clock):
            clock.sleep(8)
            self.assertEqual(remaining_timeout(90), 2)
            with Deadline(90, clock):
                self.assertEqual(remaining_timeout(90), 2)
                clock.sleep(2)
                with self.assertRaises(TimeoutError):
                    remaining_timeout(90)
        self.assertEqual(remaining_timeout(90), 90)
        for invalid in (0, -1, float('inf'), float('nan'), True):
            with self.assertRaises(ValueError):
                Deadline(invalid, clock)

    def test_default_cli_timeouts_and_observation_minimum(self):
        parser = driver.argument_parser()
        args = parser.parse_args([])
        self.assertEqual((args.setup_timeout, args.sleep_seconds, args.observe_seconds), (90, 35, 80))
        for sleep, observe in ((15, 80), (35, 74), (75, 79)):
            with patch('sys.argv', ['driver.py', '--sleep-seconds', str(sleep), '--observe-seconds', str(observe)]), \
                    patch('driver.Path.home', return_value=Path(tempfile.gettempdir())), \
                    patch('driver.shutil.which', side_effect=AssertionError('prerequisite probed')), \
                    patch('sys.stderr'), patch('builtins.print'), self.assertRaises(SystemExit) as caught:
                driver.main()
            self.assertEqual(caught.exception.code, 2)


class OrchestratorControls(unittest.TestCase):
    def scripted_run(self, identifier, cell, *, cleanup_failure=False, is_profile_pinned=False, exit_probe=False):
        import subprocess
        run_interpreter = subprocess.run
        interpreter_process = subprocess.Popen
        from hostdriver import ControlEvidence, Release, Scope, Session, StateEvidence, SubmissionProof
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scratch_root = root / 'scratch'
            scratch_root.mkdir()
            calls = []
            clock = Clock()
            class ScriptedDriver:
                def describe(self, version, mode):
                    result = describe(version, mode)
                    result['driver_id'] = identifier
                    return result
                def prepare(self, scratch, adapter, broker, profile):
                    self.scratch = scratch
                    calls.append('prepare')
                def launch(self, resume_handle=None):
                    calls.append('resume' if resume_handle else 'launch')
                    return Session(handle_id='selection', run_id='run', scratch_id='scratch', host_session_id=None,
                                   session_id=None, connection_epoch_id=None, claim_generation=None,
                                   posture='resumed' if resume_handle else 'fresh', artifact_refs=())
                def record(self, method, facts):
                    position, reference = self.scratch.journal.append('operation', method, facts, clock)
                    return dict(operation_id='operation', run_id='run', session_handle_id='selection',
                                scope=Scope('run', None, None, None, None, None), positions=(position,), artifact_refs=(reference,))
                def enter_state(self, state, workload):
                    calls.append(('state', state, workload.kind))
                    control = self.record('enter_state', {'legacy_evidence': {'tool_state': {'background': state == 'background'}}})
                    return StateEvidence(**control, state=state, workload_id=None, valid_from=control['positions'][0], valid_until=None)
                def barrier(self, point):
                    calls.append((point.kind, point.id, point.action))
                    facts = {'legacy_evidence': {'fetch_result': {'content': []}}} if point.kind == 'fetch_result' else {}
                    status = {'arm': 'armed', 'wait': 'held', 'release': 'released', 'sample': 'reached'}[point.action]
                    return Release(**self.record('barrier', facts), release_id='release', barrier_id=point.id, status=status, cuts=())
                def reconnect(self, session): calls.append('reconnect')
                def send_prompt(self, text): calls.append(('send', text))
                def observe(self, cursor=None):
                    calls.append('observe')
                    return ObserverTables(), ArtifactBundle('run', 'snapshot', None)
                def stop(self, preserve_session):
                    calls.append('preserve' if preserve_session else 'dispose')
                    if preserve_session:
                        return 'preserved'
                    if cleanup_failure:
                        raise DriverError('stop', 'cleanup', 'timeout', 'synthetic cleanup timeout')
            args = SimpleNamespace(output=root / 'output', claude=Path('/synthetic/cli'), setup_timeout=90,
                                   sleep_seconds=35, observe_seconds=80, collect_only=False, fixtures=False, keep_scratch=False)
            selected_profile = None
            if is_profile_pinned:
                scratch = Scratch(scratch_root, 'scratch', 'scratch', scratch_root / 'cwd', scratch_root, ControlJournal(scratch_root / 'control'))
                selected_profile = driver.default_profile(ScriptedDriver(), args, cell, scratch, '2.1.267')
                args.observe_seconds = 1
            process = Mock()
            process.terminate.side_effect = lambda: calls.append('broker-stop')
            def capture(*arguments):
                calls.append('collect')
                result = {'status': 'PASS', 'reasons': [], 'evidence': arguments[4]}
                arguments[3].mkdir(parents=True)
                (arguments[3] / 'summary.json').write_text(json.dumps(result))
                return result
            with patch('driver.tempfile.mkdtemp', return_value=str(scratch_root)), \
                    patch('driver.subprocess.Popen', return_value=process), patch('driver.wait_for'), \
                    patch('driver.subprocess.run', return_value=SimpleNamespace(stdout='{"accepted": true, "message_ids": [1]}')), \
                    patch('driver.queue_rows', return_value=[{}]), patch('driver.collect', side_effect=capture), \
                    patch('driver.save_failure_evidence') as exported, patch('driver.time.monotonic', clock.monotonic), \
                    patch('driver.time.sleep', clock.sleep), patch('driver.time.time', clock.time):
                def run():
                    return driver.run_cell(args, cell, root, root, '2.1.267', host_driver=ScriptedDriver(), profile=selected_profile)
                if cleanup_failure:
                    if exit_probe:
                        run()  # Must escape uncaught in the fresh interpreter.
                        return
                    import sys
                    with patch('subprocess.Popen', interpreter_process):
                        child = run_interpreter([sys.executable, '-c',
                            "from test_hostdriver import OrchestratorControls, Cell; "
                            "OrchestratorControls().scripted_run('arbitrary', "
                            "Cell('channel', 'idle', 'fresh', 'text', 'single'), "
                            "cleanup_failure=True, exit_probe=True)"],
                            cwd=Path(__file__).parent, capture_output=True, text=True, timeout=30)
                    self.assertEqual(child.returncode, 1)
                    self.assertIn('DriverError: synthetic cleanup timeout', child.stderr)
                    with self.assertRaisesRegex(DriverError, 'synthetic cleanup timeout'):
                        run()
                    exported.assert_called_once()
                    result = json.loads((args.output / '2.1.267' / cell.name / 'summary.json').read_text())
                    self.assertEqual(result['status'], 'FAIL')
                else:
                    result = run()
                    self.assertEqual(result['reasons'], [])
            self.assertLess(calls.index('collect'), calls.index('dispose'))
            self.assertLess(calls.index('dispose'), calls.index('broker-stop'))
            self.assertGreaterEqual(calls.count('observe'), 400)
            return calls, result

    def test_arbitrary_driver_identifier_does_not_change_orchestration(self):
        for cell in (Cell('channel', 'startup', 'fresh', 'text', 'single'),
                     Cell('channel', 'reconnect', 'resumed', 'text', 'single'),
                     Cell('fetch', 'foreground', 'resumed', 'text', 'single')):
            with self.subTest(cell=cell.name):
                first, _ = self.scripted_run('arbitrary-one', cell)
                second, _ = self.scripted_run('unrelated-two', cell)
                self.assertEqual(first, second)

    def test_pinned_observation_window_cannot_be_shortened_by_argument_drift(self):
        self.scripted_run('arbitrary', Cell('channel', 'idle', 'fresh', 'text', 'single'), is_profile_pinned=True)

    def test_pass_then_cleanup_raises_persists_failure_and_exits_nonzero(self):
        _, result = self.scripted_run('arbitrary', Cell('channel', 'idle', 'fresh', 'text', 'single'), cleanup_failure=True)
        self.assertEqual(result['reasons'], ['cleanup host stop failed: DriverError'])
