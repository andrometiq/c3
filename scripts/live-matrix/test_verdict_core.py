"""Frozen synthetic characterization and canonical correctness tests."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from collect import build_verdict_inputs, checked_read, verdict
from matrix import Cell, cells
from verdict_core import evaluate, derive, delivery_assertions
from verdict_fixture_support import author_witness, healthy_evidence, legacy_fixture_inputs

ROOT = Path(__file__).parent / 'testdata/verdict-core'


def inputs(transport='channel', burst='single', state='reconnect'):
    cell = Cell(transport, state, 'resumed', 'text', burst)
    witness = author_witness(cell, healthy_evidence(cell))
    return witness['scenario'], witness['contract'], witness['observation']


class GoldenTests(unittest.TestCase):
    def test_frozen_goldens(self):
        fixtures = sorted(ROOT.glob('*.json'))
        self.assertGreaterEqual(len(fixtures), 90)
        for path in fixtures:
            fixture = json.loads(path.read_text())
            with self.subTest(fixture=fixture['id']):
                witness = fixture['synthetic_witness']
                scenario = deepcopy(witness['scenario'])
                scenario['evaluation_mode'] = 'collect_only' if fixture['collect_only'] else 'verify'
                observation = deepcopy(witness['observation'])
                observation['setup_errors'] = list(fixture['legacy_evidence'].get('setup_errors', []))
                observation['run_errors'] = list(fixture['legacy_evidence'].get('run_errors', []))
                self.assertEqual(evaluate(scenario, witness['contract'], observation), fixture['expected_core'])
                if fixture['classification'] in ('characterization', 'known-defect baseline') and not observation['setup_errors']:
                    self.assertEqual(fixture['expected_core'], fixture['old_report'])
                    legacy_fixture_inputs(Cell(**fixture['cell']), fixture['legacy_evidence'], witness, fixture['collect_only'])

    def test_aggregate_only_fails_closed(self):
        cell = Cell('channel', 'idle', 'resumed', 'text', 'single')
        result = verdict(cell, healthy_evidence(cell), full_result=True)
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('evidence collection incomplete', result['reasons'])

    def test_thin_adapter_accepts_complete_raw_observation(self):
        cell = Cell('channel', 'reconnect', 'resumed', 'text', 'single')
        evidence = healthy_evidence(cell)
        witness = author_witness(cell, evidence)
        evidence.update({key: witness['scenario'][key] for key in ('run_id', 'route_id', 'host_session_id', 'session_id')})
        original = deepcopy(witness['observation'])
        self.assertEqual(verdict(cell, evidence, raw_observation=original, full_result=True),
                         dict(status='PASS', reasons=[], not_evaluated=[]))
        self.assertEqual(original, witness['observation'])
        evidence['run_errors'] = ['run error', 'run error']
        result = verdict(cell, evidence, raw_observation=original, collect_only=True, full_result=True)
        self.assertEqual(result, dict(status='FAIL', reasons=['run error', 'run error'], not_evaluated=[]))

    def test_feasible_scenario_mapping(self):
        feasible = [cell for cell in cells() if not cell.infeasible]
        self.assertEqual(len(feasible), 126)
        for cell in feasible:
            scenario, contract, observation = build_verdict_inputs(cell, {})
            self.assertEqual(scenario['count'], cell.count)
            self.assertEqual(scenario['transport'], cell.transport)
            self.assertFalse(observation['collection_complete'])

    def test_checked_final_jsonl(self):
        cases = [(b'{"type":"user"}\n', 'complete'), (b'{"type":"user"}', 'partial'),
                 (b'broken\n', 'malformed'), (b'\xff\n', 'malformed'), (b'[]\n', 'malformed')]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'records.jsonl'
            self.assertEqual(checked_read(path, jsonl=True)['state'], 'missing')
            for data, state in cases:
                path.write_bytes(data)
                self.assertEqual(checked_read(path, jsonl=True)['state'], state)
            with patch.object(Path, 'read_bytes', side_effect=PermissionError):
                self.assertEqual(checked_read(path, jsonl=True)['state'], 'unreadable')

    def test_known_defect_raw_projections(self):
        from collect import classify, classify_fetch, count_receives
        fixture = json.loads((ROOT / 'known-defect-unaccepted-live-intake-counted.json').read_text())
        raw = fixture['raw_synthetic_records']
        self.assertIs(classify(raw[0])['accept'], False)
        self.assertEqual(count_receives(raw, {'one'}, [1]), {'1': 1})
        self.assertEqual(fixture['old_report']['status'], 'PASS')
        fixture = json.loads((ROOT / 'known-defect-shared-sample-fetch-oracle.json').read_text())
        raw = fixture['raw_synthetic_records']
        expected = dict(token='one', members=[dict(record_id='row-1', revision='a' * 64), dict(record_id='row-2', revision='b' * 64)])
        self.assertTrue(classify_fetch(raw[0], expected, {'call'})['accept'])
        self.assertEqual(json.dumps(raw[0]).count('MATRIX_SAMPLE'), 2)
        self.assertEqual(fixture['old_report']['status'], 'PASS')
        fixture = json.loads((ROOT / 'known-defect-explicit-attach-masks-resume.json').read_text())
        self.assertTrue(any(event['milestone'] == 'explicit_attach' for event in fixture['synthetic_witness']['observation']['events']))
        self.assertEqual(fixture['expected_core']['status'], 'PASS')

    def test_setup_and_run_report_serialization(self):
        from collect import collect
        for lifecycle in ('setup_errors', 'run_errors'):
            cell = Cell('channel', 'idle', 'fresh', 'text', 'single')
            evidence = {lifecycle: ['first failure', 'second failure'], 'injected': True}
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                result = collect(cell, None, root, root / 'output', evidence, collect_only=True)
                summary = json.loads((root / 'output/summary.json').read_text())
                self.assertEqual(summary, result)
                self.assertEqual(set(summary), {'cell', 'status', 'reasons', 'evidence', 'todo_records', 'not_evaluated'})
                self.assertEqual(summary['status'], 'FAIL')
                self.assertEqual(summary['reasons'][:1 if lifecycle == 'setup_errors' else 2],
                                 ['first failure'] if lifecycle == 'setup_errors' else ['first failure', 'second failure'])
                if lifecycle == 'run_errors':
                    self.assertIn('durable rows remain', summary['reasons'])
                    self.assertIn('evidence collection incomplete', summary['reasons'])


class CoreTests(unittest.TestCase):
    def test_complete_witnesses(self):
        for transport in ('channel', 'inbox', 'fetch'):
            self.assertEqual(evaluate(*inputs(transport)), dict(status='PASS', reasons=[], not_evaluated=[]))

    def test_immutability_and_repeatability(self):
        original = inputs('fetch', 'double')
        before = deepcopy(original)
        first = evaluate(*original)
        for _ in range(3):
            self.assertEqual(evaluate(*original), first)
            derive(*original)
        self.assertEqual(original, before)

    def test_each_contract_axis(self):
        mutations = [('negotiation', 'none'), ('live_eligibility', {'channel': False, 'inbox': False}),
                     ('receipt_type', 'queue_acceptance'), ('fetch_policy', 'consume'), ('accepted_modes', ['fetch_receipt'])]
        for key, value in mutations:
            scenario, contract, observation = inputs()
            observation['contract_observations'][0]['axes'][key] = value
            with self.subTest(axis=key):
                self.assertIn('observed delivery contract does not match expected contract', evaluate(scenario, contract, observation)['reasons'])

    def test_mode_order_does_not_matter(self):
        scenario, contract, observation = inputs()
        for item in observation['contract_observations']:
            item['axes']['accepted_modes'].reverse()
        self.assertEqual(evaluate(scenario, contract, observation)['status'], 'PASS')

    def test_duplicate_event_identity(self):
        scenario, contract, observation = inputs()
        event = next(event for event in observation['events'] if event['milestone'] == 'transcript_recorded')
        observation['events'].append(deepcopy(event))
        self.assertEqual(evaluate(scenario, contract, observation)['status'], 'PASS')
        observation['events'][-1]['token'] = 'conflict'
        self.assertIn('delivery evidence identity mismatch', evaluate(scenario, contract, observation)['reasons'])

    def test_collection_is_not_optional_in_collect_only(self):
        for mode in ('verify', 'collect_only'):
            for change in ('claim', 'stream', 'manifest', 'final'):
                scenario, contract, observation = inputs()
                scenario['evaluation_mode'] = mode
                if change == 'claim':
                    observation['collection_complete'] = False
                elif change == 'stream':
                    observation['streams'][1]['state'] = 'truncated'
                elif change == 'manifest':
                    observation['streams'] = []
                else:
                    observation['barriers'] = [barrier for barrier in observation['barriers'] if barrier['id'] != 'final']
                result = evaluate(scenario, contract, observation)
                self.assertEqual(result['status'], 'FAIL')
                self.assertIn('evidence collection incomplete', result['reasons'])

    def test_identity_mismatches(self):
        for field in ('run_id', 'route_id', 'host_session_id', 'session_id', 'connection_epoch_id', 'claim_generation'):
            scenario, contract, observation = inputs()
            host = next(event for event in observation['events'] if event['milestone'] == 'transcript_recorded')
            host['scope'][field] = 2 if field == 'claim_generation' else 'wrong'
            with self.subTest(field=field):
                self.assertIn('delivery evidence identity mismatch', evaluate(scenario, contract, observation)['reasons'])
        for field in ('token', 'attempt_id', 'group_id'):
            scenario, contract, observation = inputs()
            host = next(event for event in observation['events'] if event['milestone'] == 'transcript_recorded')
            host[field] = 'wrong'
            self.assertIn('delivery evidence identity mismatch', evaluate(scenario, contract, observation)['reasons'])

    def test_schema_errors_fail_near_field(self):
        for key, value in (('count', True), ('schema_version', 2), ('transport', 'unsupported')):
            scenario, contract, observation = inputs()
            scenario[key] = value
            self.assertTrue(evaluate(scenario, contract, observation)['reasons'][0].startswith('invalid verdict input: scenario.' + key))
        scenario, contract, observation = inputs()
        del scenario['count']
        self.assertEqual(evaluate(scenario, contract, observation)['reasons'], ['invalid verdict input: scenario.count is required'])

    def test_duration_and_preinjection_receipts(self):
        scenario, contract, observation = inputs()
        scenario['observation_duration_ms'] = 1000000
        self.assertIn('evidence collection incomplete', evaluate(scenario, contract, observation)['reasons'])
        scenario['observation_duration_ms'] = 15000
        host = next(event for event in observation['events'] if event['milestone'] == 'transcript_recorded')
        host['position'].update(seq=5, time_ms=5000)
        self.assertIn('host did not receive each source exactly once', evaluate(scenario, contract, observation)['reasons'])

    def test_missing_identity_and_foreign_origin_fail_closed(self):
        for mutation in ('revision', 'epoch', 'origin'):
            scenario, contract, observation = inputs()
            host = next(event for event in observation['events'] if event['milestone'] == 'transcript_recorded')
            if mutation == 'revision':
                host['members'][0]['revision'] = dict(kind='unknown', value=None)
            elif mutation == 'epoch':
                host['scope']['connection_epoch_id'] = None
            else:
                host['origin'] = 'retirement'
            result = evaluate(scenario, contract, observation)
            self.assertEqual(result['status'], 'FAIL')
            self.assertIn('host did not receive each source exactly once', result['reasons'])
            self.assertIn('delivery evidence identity mismatch' if mutation == 'origin' else 'evidence collection incomplete', result['reasons'])

    def test_provenance_is_not_an_assertion_bypass(self):
        scenario, contract, observation = inputs()
        before = evaluate(scenario, contract, observation)
        observation['provenance'].update(known_defect_baselines=['arbitrary'], host_version='unknown', broker_build='unknown')
        self.assertEqual(evaluate(scenario, contract, observation), before)
        observation['collection_complete'] = False
        self.assertEqual(evaluate(scenario, contract, observation)['status'], 'FAIL')

    def test_core_does_not_use_io_or_clock(self):
        trace = inputs()
        with patch('builtins.open', side_effect=AssertionError('I/O')), patch('time.time', side_effect=AssertionError('clock')):
            self.assertEqual(evaluate(*trace)['status'], 'PASS')

    def test_collapsing_unequal_revisions_changes_judgment(self):
        scenario, contract, observation = inputs()
        host = next(event for event in observation['events'] if event['milestone'] == 'transcript_recorded')
        correct = host['members'][0]['revision']['value']
        host['members'][0]['revision']['value'] = 'different'
        self.assertEqual(evaluate(scenario, contract, observation)['status'], 'FAIL')
        host['members'][0]['revision']['value'] = correct
        self.assertEqual(evaluate(scenario, contract, observation)['status'], 'PASS')


class VerifierRegressionTests(unittest.TestCase):
    """Concrete mutations from SOL-P1-VERIFY, with complete positive controls."""

    def assert_failure(self, trace, *reasons):
        result = evaluate(*trace)
        self.assertEqual(result['status'], 'FAIL', result)
        self.assertEqual(result['not_evaluated'], [])
        for reason in reasons:
            self.assertIn(reason, result['reasons'])
        return result

    def test_notice_cannot_substitute_final_snapshot(self):
        fixture = json.loads((ROOT / 'held-visible-backlog.json').read_text())
        witness = fixture['synthetic_witness']
        trace = tuple(witness[key] for key in ('scenario', 'contract', 'observation'))
        self.assertEqual(evaluate(*trace)['status'], 'PASS')
        observation = trace[2]
        notice = next(event for event in observation['events'] if event['id'] == 'held')
        final = next(snapshot for snapshot in observation['queue_snapshots'] if snapshot['barrier_id'] == 'final')
        # Keep a plausible count in the final snapshot, so only chronology fails.
        final['rows'] = deepcopy(next(snapshot for snapshot in observation['queue_snapshots'] if snapshot['id'] == 'held-queue')['rows'])
        self.assertEqual(evaluate(*trace)['status'], 'PASS')
        notice['payload']['queue_snapshot_id'] = final['id']
        self.assert_failure(trace, 'no false Held assertion failed or missing', 'evidence collection incomplete')

    def test_matching_fetch_trailers_must_bind_reserved_identity(self):
        for mutation in ('wrong-token', 'unrelated-row', 'wrong-source', 'stale-revision', 'missing-member'):
            with self.subTest(mutation=mutation):
                trace = inputs('fetch', 'double')
                self.assertEqual(evaluate(*trace)['status'], 'PASS')
                for event in trace[2]['events']:
                    if event['milestone'] not in ('fetch_result_produced', 'fetch_result_recorded'):
                        continue
                    trailer = event['payload']['trailer']
                    if mutation == 'wrong-token':
                        trailer['token'] = 'unreserved-token'
                    elif mutation == 'unrelated-row':
                        member = deepcopy(trailer['members'][0])
                        member.update(row_id='unrelated-row', source_ids=[])
                        trailer['members'].append(member)
                    elif mutation == 'wrong-source':
                        trailer['members'][0]['source_ids'] = ['unrelated-source']
                    elif mutation == 'stale-revision':
                        trailer['members'][0]['revision']['value'] = 'stale'
                    else:
                        trailer['members'].pop()
                self.assert_failure(trace, 'fetch tool result lacks the complete matching receipt trailer',
                                    'delivery evidence identity mismatch')

    def test_split_sources_cannot_reuse_one_delivery_and_record(self):
        for transport in ('channel', 'fetch'):
            with self.subTest(transport=transport):
                trace = inputs(transport, 'double')
                self.assertEqual(evaluate(*trace)['status'], 'PASS')
                hosts = [event for event in trace[2]['events'] if event['origin'] == 'host-evidence']
                hosts[1]['delivery_id'] = hosts[0]['delivery_id']
                hosts[1]['payload']['host_record_id'] = hosts[0]['payload']['host_record_id']
                # JSON object key order must not create another identity scope.
                hosts[1]['scope'] = dict(reversed(list(hosts[1]['scope'].items())))
                reason = ('fetch did not return each injected source exactly once' if transport == 'fetch'
                          else 'host did not receive each source exactly once')
                self.assert_failure(trace, reason, 'delivery evidence identity mismatch', 'evidence collection incomplete')
                self.assertFalse(all(count == 1 for count in derive(*trace)['received'].values()))

    def test_fetch_trailer_cannot_include_known_uninjected_source(self):
        trace = inputs('fetch')
        self.assertEqual(evaluate(*trace)['status'], 'PASS')
        observation = trace[2]
        persisted = next(event for event in observation['events'] if event['milestone'] == 'row_persisted')
        unrelated = deepcopy(persisted['members'][0])
        unrelated.update(row_id='unrelated-row', source_ids=['uninjected-source'])
        # Even a persisted, reserved row is not an injected source in this run.
        persisted['members'].append(deepcopy(unrelated))
        reservation = next(event for event in observation['events'] if event['milestone'] == 'attempt_reserved')
        reservation['members'].append(deepcopy(unrelated))
        held = next(snapshot for snapshot in observation['queue_snapshots'] if snapshot['barrier_id'] == 'fetch_result_held')
        held['rows'].append(deepcopy(unrelated))
        for event in observation['events']:
            if event['milestone'] in ('fetch_result_produced', 'fetch_result_recorded'):
                event['payload']['trailer']['members'].append(deepcopy(unrelated))
        self.assert_failure(trace, 'fetch tool result lacks the complete matching receipt trailer',
                            'delivery evidence identity mismatch')

    def test_reobserving_one_consistent_delivery_does_not_duplicate_it(self):
        trace = inputs()
        host = next(event for event in trace[2]['events'] if event['milestone'] == 'transcript_recorded')
        copy = deepcopy(host)
        copy['id'] = 'reobserved-host'
        copy['position'].update(seq=90, time_ms=90000)
        trace[2]['events'].append(copy)
        self.assertEqual(evaluate(*trace), dict(status='PASS', reasons=[], not_evaluated=[]))

    def test_conflicting_event_id_fails_integrity_in_collect_only(self):
        trace = inputs()
        trace[0]['evaluation_mode'] = 'collect_only'
        self.assertEqual(evaluate(*trace)['status'], 'COLLECTED')
        host = next(event for event in trace[2]['events'] if event['milestone'] == 'transcript_recorded')
        conflict = deepcopy(host)
        conflict['members'][0]['source_ids'] = ['different-source']
        trace[2]['events'].append(conflict)
        self.assert_failure(trace, 'delivery evidence identity mismatch', 'evidence collection incomplete')

    def test_missing_required_members_and_axes_fail_collect_only(self):
        for transport in ('channel', 'fetch'):
            for mutation in ('host-members', 'contract-axes'):
                with self.subTest(transport=transport, mutation=mutation):
                    trace = inputs(transport)
                    trace[0]['evaluation_mode'] = 'collect_only'
                    self.assertEqual(evaluate(*trace)['status'], 'COLLECTED')
                    if mutation == 'host-members':
                        host = next(event for event in trace[2]['events'] if event['origin'] == 'host-evidence')
                        host['members'] = []
                    else:
                        trace[2]['contract_observations'][0]['axes'] = None
                    self.assert_failure(trace, 'evidence collection incomplete')

    def test_injected_rows_must_be_absent_before_injection(self):
        for identity in ('row', 'source'):
            with self.subTest(identity=identity):
                trace = inputs()
                self.assertEqual(evaluate(*trace)['status'], 'PASS')
                observation = trace[2]
                row = deepcopy(next(event for event in observation['events'] if event['milestone'] == 'row_persisted')['members'][0])
                if identity == 'source':
                    row['row_id'] = 'preexisting-row'
                next(snapshot for snapshot in observation['queue_snapshots'] if snapshot['barrier_id'] == 'pre_injection')['rows'] = [row]
                self.assert_failure(trace, 'delivery evidence identity mismatch')

    def test_every_required_barrier_needs_stream_cutoffs(self):
        for barrier_id in ('injection', 'ready', 'pre_injection', 'before_ready', 'fetch_result_held', 'final'):
            for role in ('injection', 'broker', 'host', 'transport', 'queue', 'contract', 'state'):
                with self.subTest(barrier=barrier_id, stream=role):
                    trace = inputs('fetch')
                    self.assertEqual(evaluate(*trace)['status'], 'PASS')
                    barrier = next(barrier for barrier in trace[2]['barriers'] if barrier['id'] == barrier_id)
                    del barrier['stream_cutoffs'][role]
                    self.assert_failure(trace, 'evidence collection incomplete')

    def test_required_readiness_cut_must_be_reached_and_in_range(self):
        for mutation in ('not-reached', 'unknown', 'partial', 'past-stream', 'before-stream'):
            with self.subTest(mutation=mutation):
                trace = inputs()
                self.assertEqual(evaluate(*trace)['status'], 'PASS')
                ready = next(barrier for barrier in trace[2]['barriers'] if barrier['id'] == 'ready')
                if mutation in ('not-reached', 'unknown'):
                    ready['state'] = mutation.replace('-', '_')
                elif mutation == 'partial':
                    ready['collection_complete'] = False
                else:
                    ready['stream_cutoffs']['host'] = 101 if mutation == 'past-stream' else -2
                self.assert_failure(trace, 'evidence collection incomplete')

    def test_nullable_fault_reservation_transport_does_not_crash(self):
        for health in ('expiry', 'restart', 'degraded', 'delivery_failure'):
            with self.subTest(health=health):
                trace = PolicyTests().fault_trace(health)
                self.assertEqual(evaluate(*trace)['status'], 'PASS')
                reservation = next(event for event in trace[2]['events'] if event['milestone'] == 'attempt_reserved')
                reservation['transport'] = None
                self.assert_failure(trace, 'evidence collection incomplete')

    def test_unknown_fault_order_cannot_license_duplicates(self):
        trace = PolicyTests().fault_trace('restart')
        observation = trace[2]
        host = next(event for event in observation['events'] if event['milestone'] == 'transcript_recorded')
        duplicate = deepcopy(host)
        duplicate.update(id='duplicate', delivery_id='duplicate')
        duplicate['payload']['host_record_id'] = 'duplicate'
        duplicate['position'].update(seq=56, time_ms=56000)
        observation['events'].append(duplicate)
        self.assertEqual(evaluate(*trace)['status'], 'PASS')
        fault = next(event for event in observation['events'] if event['milestone'] == 'fault_observed')
        injection = next(barrier for barrier in observation['barriers'] if barrier['id'] == 'injection')
        del injection['stream_cutoffs'][fault['position']['stream_id']]
        self.assert_failure(trace, 'observed fault does not match scenario health class',
                            'injected source lacks its required final disposition', 'evidence collection incomplete')

    def test_idle_setup_preserves_frozen_historical_prefix(self):
        for transport in ('channel', 'inbox', 'fetch'):
            for collect_only in (False, True):
                fixture = json.loads((ROOT / f'setup-idle-{transport}-{collect_only}.json').read_text())
                self.assertEqual(fixture['classification'], 'characterization')
                witness = fixture['synthetic_witness']
                self.assertIsNone(witness['scenario']['readiness_barrier_id'])
                witness['scenario']['evaluation_mode'] = 'collect_only' if collect_only else 'verify'
                witness['observation']['setup_errors'] = fixture['legacy_evidence']['setup_errors']
                result = evaluate(*(witness[key] for key in ('scenario', 'contract', 'observation')))
                self.assertEqual(result, fixture['expected_core'])
                prefix = fixture['old_report']['not_evaluated']
                self.assertEqual(result['not_evaluated'][:len(prefix)], prefix)

    def test_independent_foreign_delivery_stream_does_not_poison_target(self):
        for transport in ('channel', 'fetch'):
            trace = inputs(transport)
            expected = evaluate(*trace)
            self.assertEqual(expected['status'], 'PASS')
            # Reuse opaque local source/row/token IDs in a different scope.
            foreign = deepcopy(trace[2])
            for artifact in foreign['artifacts']:
                artifact['id'] = 'foreign-' + artifact['id']
            for collection in ('streams', 'events', 'barriers', 'queue_snapshots', 'contract_observations'):
                for item in foreign[collection]:
                    for key in ('run_id', 'route_id', 'host_session_id', 'session_id', 'connection_epoch_id'):
                        item['scope'][key] = 'foreign-' + item['scope'][key]
                    for key in ('id', 'barrier_id', 'through_barrier_id'):
                        if key in item:
                            item[key] = 'foreign-' + item[key]
                    for reference in item.get('artifact_refs', []) + ([item['artifact_ref']] if 'artifact_ref' in item else []):
                        reference['artifact_id'] = 'foreign-' + reference['artifact_id']
                    if 'artifact_ids' in item:
                        item['artifact_ids'] = ['foreign-' + identity for identity in item['artifact_ids']]
                    if 'position' in item:
                        item['position']['stream_id'] = 'foreign-' + item['position']['stream_id']
                    if 'stream_cutoffs' in item:
                        item['stream_cutoffs'] = {'foreign-' + identity: cutoff for identity, cutoff in item['stream_cutoffs'].items()}
                trace[2][collection].extend(foreign[collection])
            trace[2]['artifacts'].extend(foreign['artifacts'])
            self.assertEqual(evaluate(*trace), expected)

    def test_readiness_boundary_is_pinned_and_opaque(self):
        for mutation in ('wrong-boundary', 'missing-boundary', 'wrong-barrier-name', 'wrong-epoch'):
            with self.subTest(mutation=mutation):
                trace = inputs()
                ready = next(barrier for barrier in trace[2]['barriers'] if barrier['id'] == 'ready')
                # Barrier IDs locate observations; names identify contract boundaries.
                ready['name'] = trace[1]['readiness_boundary'] = 'opaque-boundary'
                self.assertEqual(evaluate(*trace)['status'], 'PASS')
                if mutation == 'wrong-boundary':
                    trace[1]['readiness_boundary'] = 'different-boundary'
                elif mutation == 'missing-boundary':
                    trace[1]['readiness_boundary'] = None
                elif mutation == 'wrong-barrier-name':
                    ready['name'] = 'different-boundary'
                else:
                    ready['scope']['connection_epoch_id'] = 'different-epoch'
                self.assert_failure(trace)


class AbsenceRegressionTests(unittest.TestCase):
    """Re-verifier traces and the other positive-proof gaps found in the audit."""

    def assert_incomplete(self, trace):
        for mode in ('verify', 'collect_only'):
            trace[0]['evaluation_mode'] = mode
            result = evaluate(*trace)
            self.assertEqual(result['status'], 'FAIL', result)
            self.assertIn('evidence collection incomplete', result['reasons'])
        self.assertFalse(derive(*trace)['collection_complete'])

    def test_unbound_delivery_exact_reverifier_trace(self):
        for transport in ('channel', 'inbox', 'fetch'):
            with self.subTest(transport=transport):
                trace = inputs(transport)
                self.assertEqual(evaluate(*trace)['status'], 'PASS')
                host = next(event for event in trace[2]['events'] if event['origin'] == 'host-evidence')
                host.update(attempt_id='unrelated-attempt', group_id='unrelated-group', token='unrelated-token')
                self.assert_incomplete(trace)
                self.assertEqual(derive(*trace)['received'], {'1': 0})

    def test_decoy_final_snapshot_exact_reverifier_trace(self):
        trace = inputs()
        observation = trace[2]
        final = next(barrier for barrier in observation['barriers'] if barrier['id'] == trace[0]['final_barrier_id'])
        decoy = deepcopy(final)
        decoy.update(id='early-decoy', stream_cutoffs={key: 8 for key in final['stream_cutoffs']})
        observation['barriers'].append(decoy)
        next(snapshot for snapshot in observation['queue_snapshots'] if snapshot['barrier_id'] == final['id'])['barrier_id'] = decoy['id']
        self.assert_incomplete(trace)
        self.assertIsNone(derive(*trace)['rows_final'])

    def test_required_cuts_must_be_ordered(self):
        for name, cutoff in (('pre_injection', 100), ('ready', 8), ('ready', 10), ('before_ready', 21), ('final', 20)):
            with self.subTest(name=name, cutoff=cutoff):
                trace = inputs()
                barrier = next(item for item in trace[2]['barriers'] if item['name'] == name)
                barrier['stream_cutoffs'] = {key: cutoff for key in barrier['stream_cutoffs']}
                self.assert_incomplete(trace)
        trace = inputs()
        next(item for item in trace[2]['barriers'] if item['name'] == 'ready')['stream_cutoffs']['host'] = 8
        self.assert_incomplete(trace)

    def test_contract_observation_cannot_use_same_named_decoy(self):
        for name in ('injection', 'final'):
            trace = inputs()
            barrier = deepcopy(next(item for item in trace[2]['barriers'] if item['name'] == name))
            barrier['id'] = 'decoy'
            trace[2]['barriers'].append(barrier)
            next(item for item in trace[2]['contract_observations'] if item['barrier_id'] == name)['barrier_id'] = 'decoy'
            self.assert_incomplete(trace)
            self.assertFalse(derive(*trace)['contract_matches'])

    def test_unrelated_backlog_loss_exact_reverifier_trace(self):
        trace = inputs()
        member = deepcopy(next(event for event in trace[2]['events'] if event['milestone'] == 'row_persisted')['members'][0])
        member.update(row_id='backlog-row', source_ids=['backlog-source'])
        pre = next(item for item in trace[2]['queue_snapshots'] if item['barrier_id'] == 'pre_injection')
        pre['rows'].append(deepcopy(member))
        self.assert_incomplete(trace)
        self.assertTrue(derive(*trace)['unauthorized_retirement'])
        # Full snapshots retain unrelated backlog; the injected-row report stays zero.
        for snapshot in trace[2]['queue_snapshots']:
            if snapshot is not pre:
                snapshot['rows'].append(deepcopy(member))
        trace[0]['evaluation_mode'] = 'verify'
        self.assertEqual(evaluate(*trace), dict(status='PASS', reasons=[], not_evaluated=[]))
        self.assertEqual(derive(*trace)['rows_final'], 0)

    def test_extra_contract_checkpoint_exact_reverifier_trace(self):
        trace = inputs()
        barrier = deepcopy(trace[2]['barriers'][0])
        barrier.update(id='extra-checkpoint', name='extra-checkpoint', stream_cutoffs={'contract': 55})
        trace[2]['barriers'].append(barrier)
        observed = deepcopy(trace[2]['contract_observations'][0])
        observed['barrier_id'] = barrier['id']
        trace[2]['contract_observations'].append(observed)
        self.assertEqual(evaluate(*trace), dict(status='PASS', reasons=[], not_evaluated=[]))
        for mutation in ('missing-required', 'wrong-required-axes', 'unanchored-extra', 'invalid-extra-cut'):
            changed = deepcopy(trace)
            if mutation == 'missing-required':
                changed[2]['contract_observations'].pop(0)
            elif mutation == 'wrong-required-axes':
                changed[2]['contract_observations'][0]['axes']['accepted_modes'] = []
            elif mutation == 'unanchored-extra':
                changed[2]['barriers'][-1]['stream_cutoffs'] = {'state': 55}
            else:
                changed[2]['barriers'][-1]['stream_cutoffs']['contract'] = 101
            self.assertEqual(evaluate(*changed)['status'], 'FAIL', mutation)

    def test_opaque_barrier_ids_and_unique_pre_injection(self):
        trace = inputs('fetch')
        names = {'injection', 'ready', 'final', 'pre_injection', 'before_ready', 'fetch_result_held'}
        def rename(value):
            if isinstance(value, dict):
                return {key: ('opaque-' + item if key.endswith('barrier_id') and item in names else rename(item))
                        for key, item in value.items()}
            if isinstance(value, list):
                return [rename(item) for item in value]
            return value
        trace = rename(list(trace))
        for barrier in trace[2]['barriers']:
            barrier['id'] = 'opaque-' + barrier['id']
        self.assertEqual(evaluate(*trace)['status'], 'PASS')
        duplicate = deepcopy(next(item for item in trace[2]['barriers'] if item['name'] == 'pre_injection'))
        duplicate['id'] = 'ambiguous-pre-injection'
        trace[2]['barriers'].append(duplicate)
        self.assert_incomplete(trace)

    def test_unbound_receipt_terminal_and_unknown_reservation(self):
        for milestone in ('receipt_accepted', 'attempt_terminal', 'attempt_reserved'):
            trace = PolicyTests().fault_trace('restart')
            event = next(item for item in trace[2]['events'] if item['milestone'] == milestone)
            if milestone == 'attempt_reserved':
                event = deepcopy(event)
                event['id'] = 'unrelated-reservation'
                event['members'][0]['row_id'] = 'never-persisted'
                trace[2]['events'].append(event)
            else:
                event.update(attempt_id='unbound-attempt', group_id='unbound-group', token='unbound-token')
            self.assert_incomplete(trace)

    def test_legacy_delivery_needs_an_operation_binding(self):
        for milestone in ('queue_accepted', 'input_landed'):
            for operation in (None, 'unbound-operation'):
                trace = PolicyTests().legacy(milestone)
                host = next(item for item in trace[2]['events'] if item['milestone'] == milestone)
                host['operation_id'] = operation
                self.assert_incomplete(trace)
                self.assertEqual(derive(*trace)['received'], {'1': 0})

    def test_missing_source_and_proof_anchors_fail_collect_only(self):
        for missing in ('sources', 'admission', 'state-events', 'session-events', 'source-members', 'stream-artifact'):
            with self.subTest(missing=missing):
                trace = inputs()
                if missing == 'sources':
                    trace[2]['sources'] = []
                elif missing == 'admission':
                    trace[2]['sources'][0]['admission_event_id'] = 'absent'
                elif missing.endswith('-events'):
                    trace[2][missing.split('-')[0] + '_proof']['event_ids'] = []
                else:
                    host = next(item for item in trace[2]['events'] if item['origin'] == 'host-evidence')
                    if missing == 'source-members':
                        host['members'][0]['source_ids'] = []
                    else:
                        host['artifact_ref']['artifact_id'] = 'state'
                self.assert_incomplete(trace)

    def test_delivery_requires_prior_reservation_and_source_evidence(self):
        for milestone in ('attempt_reserved', 'row_persisted', 'injection_completed'):
            trace = inputs()
            for event in trace[2]['events']:
                if event['milestone'] == milestone or milestone == 'row_persisted' and event['milestone'] == 'injection_completed':
                    seq = 5 if milestone == 'injection_completed' else 65
                    event['position'].update(seq=seq, time_ms=seq * 1000)
            self.assert_incomplete(trace)

    def test_fetch_claims_require_bound_members_and_order(self):
        for mutation in ('request-members', 'request-operation', 'request-after-result', 'result-after-host',
                         'result-no-delivery-id', 'held-before-result', 'held-after-host'):
            with self.subTest(mutation=mutation):
                trace = inputs('fetch')
                request = next(item for item in trace[2]['events'] if item['milestone'] == 'fetch_requested')
                broker = next(item for item in trace[2]['events'] if item['milestone'] == 'fetch_result_produced')
                if mutation == 'request-members':
                    request['members'] = []
                elif mutation == 'request-operation':
                    request['operation_id'] = None
                elif mutation == 'request-after-result':
                    request['position'].update(seq=41, time_ms=41000)
                elif mutation == 'result-after-host':
                    broker['position'].update(seq=55, time_ms=55000)
                elif mutation == 'result-no-delivery-id':
                    broker['delivery_id'] = None
                else:
                    cut = next(item for item in trace[2]['barriers'] if item['name'] == 'fetch_result_held')
                    cut['stream_cutoffs'] = {key: 39 if mutation == 'held-before-result' else 55 for key in cut['stream_cutoffs']}
                self.assert_incomplete(trace)

    def test_full_snapshot_reconciliation_includes_revisions_and_intermediate_loss(self):
        for mutation in ('unknown-revision', 'changed-revision', 'intermediate-loss', 'unreported-retirement'):
            trace = inputs()
            row = deepcopy(next(item for item in trace[2]['events'] if item['milestone'] == 'row_persisted')['members'][0])
            row.update(row_id='backlog', source_ids=['backlog-source'])
            for snapshot in trace[2]['queue_snapshots']:
                snapshot['rows'].append(deepcopy(row))
            self.assertEqual(evaluate(*trace)['status'], 'PASS')
            final = next(item for item in trace[2]['queue_snapshots'] if item['barrier_id'] == 'final')
            if mutation == 'unknown-revision':
                final['rows'][-1]['revision'] = dict(kind='unknown', value=None)
            elif mutation == 'changed-revision':
                final['rows'][-1]['revision']['value'] = 'unwitnessed-revision'
            elif mutation == 'intermediate-loss':
                next(item for item in trace[2]['queue_snapshots'] if item['barrier_id'] == 'before_ready')['rows'].pop()
            else:
                final['rows'].pop()
            self.assert_incomplete(trace)

    def test_valid_backlog_retirement_and_revision_update(self):
        trace = PolicyTests().fault_trace('restart')
        row = deepcopy(next(item for item in trace[2]['events'] if item['milestone'] == 'row_persisted')['members'][0])
        row.update(row_id='backlog', source_ids=['backlog-source'])
        for snapshot in trace[2]['queue_snapshots']:
            if snapshot['barrier_id'] != 'final':
                snapshot['rows'].append(deepcopy(row))
        for milestone in ('attempt_reserved', 'receipt_accepted', 'attempt_terminal'):
            event = deepcopy(next(item for item in trace[2]['events'] if item['milestone'] == milestone))
            event.update(id='backlog-' + milestone, token='backlog', group_id='backlog-group', attempt_id='backlog-attempt', members=[deepcopy(row)])
            if milestone == 'attempt_terminal':
                event['payload']['retired_members'] = [deepcopy(row)]
            trace[2]['events'].append(event)
        self.assertEqual(evaluate(*trace), dict(status='PASS', reasons=[], not_evaluated=[]))
        # A surviving revision change needs an observed update, not just a new label.
        trace = inputs()
        for snapshot in trace[2]['queue_snapshots']:
            snapshot['rows'].append(deepcopy(row))
        update = deepcopy(next(item for item in trace[2]['events'] if item['milestone'] == 'row_persisted'))
        update.update(id='backlog-update', members=[deepcopy(row)])
        update['members'][0]['revision']['value'] = 'new-revision'
        update['position'].update(seq=80, time_ms=80000)
        trace[2]['events'].append(update)
        next(item for item in trace[2]['queue_snapshots'] if item['barrier_id'] == 'final')['rows'][-1] = deepcopy(update['members'][0])
        self.assertEqual(evaluate(*trace)['status'], 'PASS')
        trace[2]['events'].remove(update)
        self.assert_incomplete(trace)

    def test_recovery_checkpoint_needs_final_scope_and_cut(self):
        for mutation in ('pre-injection', 'missing-cutoff', 'foreign-scope', 'before-injection'):
            trace = PolicyTests().fault_trace('degraded')
            recovery = next(item for item in trace[2]['events'] if item['milestone'] == 'source_recoverable')
            if mutation == 'pre-injection':
                recovery['payload']['checkpoint_id'] = 'pre_injection'
            elif mutation == 'missing-cutoff':
                final = next(item for item in trace[2]['barriers'] if item['id'] == 'final')
                del final['stream_cutoffs'][recovery['position']['stream_id']]
            elif mutation == 'foreign-scope':
                recovery['scope']['connection_epoch_id'] = 'foreign'
            else:
                recovery['position'].update(seq=5, time_ms=5000)
            self.assert_incomplete(trace)

    def test_stale_retirement_cannot_borrow_a_surviving_new_revision(self):
        trace = PolicyTests().fault_trace('restart')
        update = deepcopy(next(item for item in trace[2]['events'] if item['milestone'] == 'row_persisted'))
        update['id'] = 'new-revision'
        update['members'][0]['revision']['value'] = 'new-revision'
        update['position'].update(seq=65, time_ms=65000)
        trace[2]['events'].append(update)
        next(item for item in trace[2]['queue_snapshots'] if item['barrier_id'] == 'final')['rows'] = deepcopy(update['members'])
        self.assert_incomplete(trace)
        self.assertTrue(derive(*trace)['unauthorized_retirement'])
        self.assertFalse(derive(*trace)['disposition_preserved'])

    def test_degraded_checkpoint_does_not_replace_backlog_accounting(self):
        trace = PolicyTests().fault_trace('degraded')
        member = deepcopy(next(item for item in trace[2]['events'] if item['milestone'] == 'row_persisted')['members'][0])
        member.update(row_id='backlog', source_ids=['backlog-source'])
        for snapshot in trace[2]['queue_snapshots']:
            snapshot['rows'].append(deepcopy(member))
        self.assertEqual(evaluate(*trace)['status'], 'PASS')
        trace[2]['queue_snapshots'] = [item for item in trace[2]['queue_snapshots'] if item['barrier_id'] != 'final']
        self.assert_incomplete(trace)

    def test_negotiated_claim_requires_an_observed_owner(self):
        trace = inputs()
        for collection in ('events', 'streams', 'barriers', 'queue_snapshots', 'contract_observations'):
            for item in trace[2][collection]:
                item['scope']['claim_generation'] = None
        for name in ('state_proof', 'session_proof'):
            trace[2][name]['scope']['claim_generation'] = None
        self.assert_incomplete(trace)
        self.assertEqual(derive(*trace)['received'], {'1': 0})

    def test_automatic_recovery_is_anchored_at_injection(self):
        trace = inputs()
        trace[0]['resume_requirement'] = 'attachment_recovery'
        recovered = deepcopy(trace[2]['events'][0])
        recovered.update(id='automatic-recovery', milestone='attachment_recovered', origin='rig-control',
                         payload=dict(previous_session_id='previous-session', automatic=True))
        recovered['position'].update(stream_id='recovery', seq=5, time_ms=5000)
        recovered['artifact_ref']['artifact_id'] = 'recovery'
        trace[2]['events'].append(recovered)
        self.assertEqual(evaluate(*trace)['status'], 'PASS')
        recovered['position'].update(seq=80, time_ms=80000)
        self.assert_incomplete(trace)

    def test_ruling_preserves_unequal_token_legacy_facts_and_timing_failure(self):
        fixture = json.loads((ROOT / 'channel-unequal-token-sets.json').read_text())
        self.assertEqual(fixture['classification'], 'intentional-correction')
        self.assertEqual(fixture['defect_id'], 'accept-on-absence')
        self.assertTrue(fixture['correction_note'])
        self.assertEqual(fixture['old_report'], dict(status='FAIL', reasons=['receipt missing or outside 15-second window'], not_evaluated=[]))
        self.assertEqual(fixture['legacy_evidence']['attempts'], [
            dict(phase='reserved', token='one', transport='channel', members='1'),
            dict(phase='confirmed', token='two', transport='channel', retired='1', elapsed_ms='100')])
        self.assertEqual(fixture['synthetic_witness']['scenario']['count'], 1)
        self.assertEqual(fixture['expected_core'], dict(status='FAIL', reasons=[
            'receipt missing or outside 15-second window', 'retired rows do not match authorized delivery members',
            'injected source lacks its required final disposition', 'evidence collection incomplete'], not_evaluated=[]))

    def test_harness_claim_validation_does_not_accept_unbound_delivery(self):
        from verdict_fixture_support import witnessed_verdict
        for transport in ('channel', 'fetch'):
            cell = Cell(transport, 'idle', 'resumed', 'text', 'single')
            evidence = healthy_evidence(cell)
            evidence['attempts'] = []
            original = deepcopy(evidence)
            witness = author_witness(cell, evidence)
            # The characterization adapter remains strict about accepted counts.
            with self.assertRaises(AssertionError):
                legacy_fixture_inputs(cell, evidence, witness)
            reasons = witnessed_verdict(cell, evidence)
            self.assertIn('evidence collection incomplete', reasons)
            self.assertIn('injected source lacks its required final disposition', reasons)
            self.assertEqual(evidence, original)
            # Raw claim validation still rejects an incorrectly authored witness.
            witness['observation']['events'] = [event for event in witness['observation']['events'] if event['origin'] != 'host-evidence']
            with patch('verdict_fixture_support.author_witness', return_value=witness), self.assertRaises(AssertionError):
                witnessed_verdict(cell, evidence)

    def test_empty_or_unbound_confirmations_do_not_prove_timeliness(self):
        for transport in ('channel', 'fetch'):
            for mutation in ('empty', 'null-identities'):
                trace = inputs(transport)
                if mutation == 'empty':
                    trace[2]['events'] = [event for event in trace[2]['events'] if event['milestone'] not in ('attempt_reserved', 'attempt_terminal')]
                else:
                    for event in trace[2]['events']:
                        if event['milestone'] in ('attempt_reserved', 'attempt_terminal'):
                            event['token'] = None
                self.assert_incomplete(trace)
                reason = ('fetch group confirmation missing or outside 60-second window' if transport == 'fetch'
                          else 'receipt missing or outside 15-second window')
                self.assertIn(reason, evaluate(*trace)['reasons'])

    def test_consume_fetch_without_host_still_requires_bound_delivery(self):
        trace = inputs('fetch')
        trace[1]['axes'].update(negotiation='none', receipt_type='none', fetch_policy='consume', accepted_modes=[])
        trace[1]['milestones'] = dict(live=None, fetch=None)
        trace[1]['timing'] = dict(live=None, fetch=None)
        for observed in trace[2]['contract_observations']:
            observed['axes'] = deepcopy(trace[1]['axes'])
        trace[2]['events'] = [event for event in trace[2]['events'] if event['milestone'] not in ('attempt_reserved', 'fetch_result_recorded')]
        for event in trace[2]['events']:
            event.update(token=None, attempt_id=None, group_id=None)
        self.assertEqual(evaluate(*trace)['status'], 'PASS')
        for mutation in ('request-members', 'result-members', 'delivery-id', 'operation'):
            changed = deepcopy(trace)
            request = next(event for event in changed[2]['events'] if event['milestone'] == 'fetch_requested')
            result = next(event for event in changed[2]['events'] if event['milestone'] == 'fetch_result_produced')
            if mutation == 'request-members':
                request['members'] = []
            elif mutation == 'result-members':
                result['members'] = []
            elif mutation == 'delivery-id':
                result['delivery_id'] = None
            else:
                result['operation_id'] = 'unbound-operation'
            self.assert_incomplete(changed)
            self.assertEqual(derive(*changed)['fetch_sources_returned'], {'1': 0})


class BindingCompletenessTests(unittest.TestCase):
    """Exercise required cross-links through both the core and production adapter."""

    def assert_incomplete(self, trace):
        original = deepcopy(trace)
        scenario, contract, observation = trace
        cell = Cell(*(scenario[key] for key in ('transport', 'state', 'session', 'kind', 'burst')))
        evidence = {key: scenario[key] for key in ('run_id', 'route_id', 'host_session_id', 'session_id')}
        for mode in ('verify', 'collect_only'):
            selected = dict(scenario, evaluation_mode=mode)
            result = evaluate(selected, contract, observation)
            self.assertEqual(result['status'], 'FAIL', result)
            self.assertIn('evidence collection incomplete', result['reasons'])
            self.assertEqual(verdict(cell, evidence, raw_observation=observation,
                                     collect_only=mode == 'collect_only', full_result=True), result)
        self.assertFalse(derive(*trace)['collection_complete'])
        self.assertEqual(trace, original)

    def test_unassigned_admitted_member_exact_reverifier_trace(self):
        for transport in ('channel', 'inbox', 'fetch'):
            with self.subTest(transport=transport):
                trace = inputs(transport)
                self.assertEqual(evaluate(*trace)['status'], 'PASS')
                admission = next(event for event in trace[2]['events'] if event['milestone'] == 'injection_completed')
                extra = deepcopy(admission['members'][0])
                extra.update(row_id='unassigned-row', source_ids=['unassigned-source'])
                admission['members'].append(deepcopy(extra))
                for snapshot in trace[2]['queue_snapshots']:
                    if snapshot['barrier_id'] != 'pre_injection':
                        snapshot['rows'].append(deepcopy(extra))
                self.assert_incomplete(trace)
                projection = derive(*trace)
                self.assertFalse(projection['injected'])
                # The unrelated row survives: retirement and delivery counts cannot
                # detect this defect; the reverse admission binding must do so.
                self.assertFalse(projection['unauthorized_retirement'])
                self.assertEqual(projection['rows_final'], 0)
                self.assertEqual(projection['received'], {'1': 1})

    def test_final_snapshot_orphan_artifact_exact_reverifier_trace(self):
        for artifact in ('orphan', 'state'):
            with self.subTest(artifact=artifact):
                trace = inputs()
                self.assertEqual(evaluate(*trace)['status'], 'PASS')
                final = next(item for item in trace[2]['queue_snapshots'] if item['barrier_id'] == trace[0]['final_barrier_id'])
                trace[2]['artifacts'].append(dict(id='orphan', kind='queue', content_digest=None))
                final['artifact_refs'][0]['artifact_id'] = artifact
                self.assert_incomplete(trace)
                self.assertIsNone(derive(*trace)['rows_final'])

    def test_host_delivery_in_state_stream_exact_reverifier_trace(self):
        for transport in ('channel', 'inbox', 'fetch'):
            with self.subTest(transport=transport):
                trace = inputs(transport)
                self.assertEqual(evaluate(*trace)['status'], 'PASS')
                host = next(event for event in trace[2]['events'] if event['origin'] == 'host-evidence')
                host['position']['stream_id'] = 'state'
                host['artifact_ref']['artifact_id'] = 'state'
                self.assert_incomplete(trace)
                self.assertEqual(derive(*trace)['received'], {'1': 0})

    def test_predecessor_offer_uses_shared_broker_stream(self):
        trace = inputs()
        offer = deepcopy(trace[2]['events'][0])
        offer.update(id='predecessor-offer', milestone='delivery_offered', payload=dict(description='prior-epoch offer'))
        offer['position'].update(stream_id='broker', seq=15, time_ms=15000)
        offer['artifact_ref']['artifact_id'] = 'broker'
        offer['scope']['connection_epoch_id'] = 'predecessor'
        trace[2]['events'].append(offer)
        self.assertEqual(evaluate(*trace), dict(status='PASS', reasons=[], not_evaluated=[]))
        self.assertEqual(derive(*trace)['offered_before_ready'], [])
        # Same role, stream, and cut; only the event's epoch changes readiness.
        offer['scope']['connection_epoch_id'] = 'epoch'
        self.assertEqual(derive(*trace)['offered_before_ready'], ['prior-epoch offer'])
        self.assertEqual(evaluate(*trace)['status'], 'FAIL')

    def test_role_streams_span_event_epochs_and_claim_generations(self):
        for transport in ('channel', 'inbox', 'fetch'):
            trace = inputs(transport)
            for stream in trace[2]['streams']:
                stream['scope'].update(connection_epoch_id='stream-boundary-epoch', claim_generation=99)
            self.assertEqual(evaluate(*trace), dict(status='PASS', reasons=[], not_evaluated=[]))
            host = next(event for event in trace[2]['events'] if event['origin'] == 'host-evidence')
            host['scope']['connection_epoch_id'] = 'unobserved-epoch'
            self.assert_incomplete(trace)
            self.assertIn('delivery evidence identity mismatch', evaluate(*trace)['reasons'])

    def test_reverse_admission_membership_and_slot_uniqueness(self):
        for mutation in ('duplicate-source', 'empty-member', 'extra-admission', 'wrong-admission', 'duplicate-slot'):
            with self.subTest(mutation=mutation):
                trace = inputs(burst='double')
                admission = next(event for event in trace[2]['events'] if event['milestone'] == 'injection_completed')
                if mutation == 'duplicate-source':
                    admission['members'][1]['source_ids'] = ['1']
                elif mutation == 'empty-member':
                    member = deepcopy(admission['members'][0])
                    member.update(row_id='empty-member', source_ids=[])
                    admission['members'].append(member)
                elif mutation in ('extra-admission', 'wrong-admission'):
                    extra = deepcopy(admission)
                    extra['id'] = 'another-admission'
                    extra['members'] = extra['members'][1:]
                    extra['position'].update(seq=13, time_ms=13000)
                    trace[2]['events'].append(extra)
                    if mutation == 'wrong-admission':
                        trace[2]['sources'][0]['admission_event_id'] = extra['id']
                else:
                    trace[2]['sources'][1]['slot'] = 0
                self.assert_incomplete(trace)
                self.assertFalse(derive(*trace)['injected'])

    def test_multiple_admissions_bind_each_slot_once(self):
        trace = inputs(burst='double')
        admission = next(event for event in trace[2]['events'] if event['milestone'] == 'injection_completed')
        second = deepcopy(admission)
        second.update(id='second-admission', members=[admission['members'].pop()])
        second['position'].update(seq=13, time_ms=13000)
        trace[2]['events'].append(second)
        trace[2]['sources'][1]['admission_event_id'] = second['id']
        self.assertEqual(evaluate(*trace), dict(status='PASS', reasons=[], not_evaluated=[]))
        trace[2]['sources'][1]['admission_event_id'] = admission['id']
        self.assert_incomplete(trace)

    def test_every_required_role_needs_its_stream_and_artifact(self):
        for role in ('injection', 'broker', 'host', 'transport', 'queue', 'contract', 'state'):
            for missing in ('stream', 'artifact'):
                with self.subTest(role=role, missing=missing):
                    trace = inputs('fetch')
                    self.assertEqual(evaluate(*trace)['status'], 'PASS')
                    collection = missing + 's'
                    trace[2][collection] = [item for item in trace[2][collection] if item['id'] != role]
                    self.assert_incomplete(trace)

    def test_every_snapshot_reference_must_join_queue_role(self):
        for name in ('pre_injection', 'before_ready', 'fetch_result_held', 'final'):
            with self.subTest(snapshot=name):
                trace = inputs('fetch')
                snapshot = next(item for item in trace[2]['queue_snapshots'] if item['barrier_id'] == name)
                # A valid first reference must not mask an unbound second one.
                snapshot['artifact_refs'].append(dict(artifact_id='state', locator='wrong-role'))
                self.assert_incomplete(trace)

    def test_unused_optional_artifact_is_not_required_evidence(self):
        trace = inputs()
        trace[2]['artifacts'].append(dict(id='unused', kind='optional', content_digest=None))
        self.assertEqual(evaluate(*trace), dict(status='PASS', reasons=[], not_evaluated=[]))


class PolicyTests(unittest.TestCase):
    def legacy(self, milestone):
        scenario, contract, observation = inputs()
        contract['axes'].update(negotiation='none', receipt_type='queue_acceptance' if milestone == 'queue_accepted' else 'input_echo',
                                fetch_policy='consume', accepted_modes=[])
        contract['milestones'].update(live=milestone, fetch=None)
        contract['timing'] = {'live': None, 'fetch': None}
        observation['events'] = [event for event in observation['events'] if event['milestone'] != 'attempt_reserved']
        for event in observation['events']:
            if event['milestone'] == 'transcript_recorded':
                event['milestone'] = milestone
            event.update(token=None, group_id=None, attempt_id=None)
        for observed in observation['contract_observations']:
            observed['axes'] = deepcopy(contract['axes'])
        return scenario, contract, observation

    def test_legacy_acceptance_boundaries(self):
        for milestone in ('queue_accepted', 'input_landed'):
            trace = self.legacy(milestone)
            self.assertEqual(evaluate(*trace)['status'], 'PASS')
            self.assertNotIn('negotiated attempt observed', delivery_assertions(*trace[:2]))
            self.assertFalse(any(event['milestone'] == 'transcript_recorded' for event in trace[2]['events']))
            host = next(event for event in trace[2]['events'] if event['milestone'] == milestone)
            host['milestone'] = 'transcript_recorded'
            self.assertIn('no contract live acceptance observed', evaluate(*trace)['reasons'])

    def test_consume_fetch_with_and_without_host(self):
        for record_host in (False, True):
            scenario, contract, observation = inputs('fetch')
            contract['axes'].update(negotiation='none', receipt_type='none', fetch_policy='consume', accepted_modes=[])
            contract['milestones'] = dict(live=None, fetch='fetch_result_recorded' if record_host else None)
            contract['timing'] = dict(live=None, fetch=None)
            for observed in observation['contract_observations']:
                observed['axes'] = deepcopy(contract['axes'])
            observation['events'] = [event for event in observation['events'] if event['milestone'] != 'attempt_reserved'
                                     and (record_host or event['milestone'] != 'fetch_result_recorded')]
            for event in observation['events']:
                event.update(token=None, group_id=None, attempt_id=None)
                if event['milestone'] == 'attempt_terminal':
                    event['position'].update(seq=46, time_ms=46000)
            self.assertEqual(evaluate(scenario, contract, observation)['status'], 'PASS')
            self.assertNotIn('rows retained until fetch receipt', delivery_assertions(scenario, contract))
            next(event for event in observation['events'] if event['milestone'] == 'fetch_requested')['payload']['ack'] = False
            self.assertIn('no successful broker fetch result', evaluate(scenario, contract, observation)['reasons'])

    def fault_trace(self, health):
        scenario, contract, observation = inputs()
        scenario['health_class'] = health
        fault = deepcopy(observation['events'][0])
        fault.update(id='fault', milestone='fault_observed', origin='failure', payload=dict(kind=health, reason='authored fault'))
        fault['position'].update(seq=45, time_ms=45000)
        observation['events'].append(fault)
        if health in ('delivery_failure', 'degraded'):
            scenario['final_dispositions'] = ['recoverable']
            terminal = next(event for event in observation['events'] if event['milestone'] == 'attempt_terminal')
            terminal['payload'].update(outcome='failed', retired_members=[])
            member = deepcopy(next(event for event in observation['events'] if event['milestone'] == 'row_persisted')['members'][0])
            next(snapshot for snapshot in observation['queue_snapshots'] if snapshot['barrier_id'] == 'final')['rows'] = [member]
            if health == 'degraded':
                checkpoint = deepcopy(fault)
                checkpoint.update(id='recovery', milestone='source_recoverable', origin='persistence',
                                  payload=dict(source_ids=['1'], storage='upstream', checkpoint_id='final'))
                checkpoint['position'].update(stream_id='recovery', seq=90, time_ms=90000)
                checkpoint['artifact_ref']['artifact_id'] = 'recovery'
                observation['events'].append(checkpoint)
        return scenario, contract, observation

    def test_all_health_classes(self):
        self.assertEqual(evaluate(*inputs())['status'], 'PASS')
        for health in ('expiry', 'restart', 'degraded', 'delivery_failure'):
            trace = self.fault_trace(health)
            with self.subTest(health=health):
                self.assertEqual(evaluate(*trace), dict(status='PASS', reasons=[], not_evaluated=[]))
                trace[2]['events'] = [event for event in trace[2]['events'] if event['milestone'] != 'fault_observed']
                self.assertIn('observed fault does not match scenario health class', evaluate(*trace)['reasons'])

    def test_fault_duplicates_before_and_after(self):
        for after in (False, True):
            scenario, contract, observation = self.fault_trace('restart')
            fault = next(event for event in observation['events'] if event['milestone'] == 'fault_observed')
            fault['position'].update(seq=55, time_ms=55000)
            host = next(event for event in observation['events'] if event['milestone'] == 'transcript_recorded')
            duplicate = deepcopy(host)
            duplicate.update(id='duplicate', delivery_id='duplicate')
            duplicate['payload']['host_record_id'] = 'duplicate'
            duplicate['position'].update(seq=56 if after else 51, time_ms=56000 if after else 51000)
            observation['events'].append(duplicate)
            self.assertEqual(evaluate(scenario, contract, observation)['status'], 'PASS' if after else 'FAIL')

    def test_recovery_preserves_current_revision(self):
        trace = self.fault_trace('delivery_failure')
        final = next(snapshot for snapshot in trace[2]['queue_snapshots'] if snapshot['barrier_id'] == 'final')
        final['rows'][0]['revision']['value'] = 'stale'
        self.assertIn('injected source lacks its required final disposition', evaluate(*trace)['reasons'])

    def test_receipt_basis_is_explicit(self):
        scenario, contract, observation = inputs()
        contract['timing']['live'] = dict(limit_ms=100, basis='broker_receipt')
        terminal = next(event for event in observation['events'] if event['milestone'] == 'attempt_terminal')
        terminal['payload']['elapsed_ms'] = 100000
        self.assertEqual(evaluate(scenario, contract, observation)['status'], 'PASS')
        receipt = next(event for event in observation['events'] if event['milestone'] == 'receipt_accepted')
        receipt['payload']['elapsed_ms'] = 100
        self.assertIn('receipt missing or outside contract window', evaluate(scenario, contract, observation)['reasons'])

    def test_identity_renaming_preserves_verdict(self):
        trace = inputs('fetch', 'double')
        identities = {'one', 'row-1', 'row-2', 'revision-1', 'revision-2', 'run', 'route', 'epoch', 'host-session', 'session', 'operation'}
        def rename(value):
            if isinstance(value, dict):
                return {key: rename(item) for key, item in value.items()}
            if isinstance(value, list):
                return [rename(item) for item in value]
            return 'renamed-' + value if isinstance(value, str) and value in identities else value
        renamed = tuple(rename(item) for item in trace)
        self.assertEqual(evaluate(*trace), evaluate(*renamed))

    def test_collapsing_unequal_tokens_changes_judgment(self):
        scenario, contract, observation = inputs()
        terminal = next(event for event in observation['events'] if event['milestone'] == 'attempt_terminal')
        terminal['token'] = 'different'
        self.assertEqual(evaluate(scenario, contract, observation)['status'], 'FAIL')
        terminal['token'] = 'one'
        self.assertEqual(evaluate(scenario, contract, observation)['status'], 'PASS')

    def test_unknown_order_cannot_authorize_retirement(self):
        scenario, contract, observation = inputs()
        receipt = next(event for event in observation['events'] if event['milestone'] == 'receipt_accepted')
        receipt['position'].update(stream_id='transport', clock_id='unrelated-clock')
        receipt['artifact_ref']['artifact_id'] = 'transport'
        receipt['caused_by'] = [next(event['id'] for event in observation['events'] if event['milestone'] == 'attempt_reserved')]
        result = evaluate(scenario, contract, observation)
        self.assertIn('evidence collection incomplete', result['reasons'])
        self.assertNotIn('rows retired before contract evidence', result['reasons'])
        terminal = next(event for event in observation['events'] if event['milestone'] == 'attempt_terminal')
        terminal['caused_by'] = [receipt['id']]
        self.assertEqual(evaluate(scenario, contract, observation)['status'], 'PASS')

    def test_barrier_cut_can_establish_receipt_order(self):
        scenario, contract, observation = inputs()
        receipt = next(event for event in observation['events'] if event['milestone'] == 'receipt_accepted')
        receipt['position'].update(stream_id='transport', clock_id='unrelated-clock')
        receipt['artifact_ref']['artifact_id'] = 'transport'
        receipt['caused_by'] = [next(event['id'] for event in observation['events'] if event['milestone'] == 'attempt_reserved')]
        barrier = deepcopy(observation['barriers'][0])
        barrier.update(id='receipt-cut', name='receipt-cut', stream_cutoffs={'transport': receipt['position']['seq'], 'broker': 65})
        observation['barriers'].append(barrier)
        self.assertEqual(evaluate(scenario, contract, observation)['status'], 'PASS')

    def test_causal_cycles_are_not_authorization(self):
        scenario, contract, observation = inputs()
        receipt = next(event for event in observation['events'] if event['milestone'] == 'receipt_accepted')
        terminal = next(event for event in observation['events'] if event['milestone'] == 'attempt_terminal')
        receipt['caused_by'] = [terminal['id']]
        terminal['caused_by'] = [receipt['id']]
        result = evaluate(scenario, contract, observation)
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('evidence collection incomplete', result['reasons'])

    def test_positive_premature_retirement(self):
        scenario, contract, observation = inputs()
        terminal = next(event for event in observation['events'] if event['milestone'] == 'attempt_terminal')
        terminal['position'].update(seq=55, time_ms=55000)
        self.assertIn('rows retired before contract evidence', evaluate(scenario, contract, observation)['reasons'])


if __name__ == '__main__':
    unittest.main()
