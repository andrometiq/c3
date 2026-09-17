"""Test-only synthetic witness authoring and legacy projection validation."""
from copy import deepcopy

from collect import build_verdict_inputs
from verdict_core import derive, evaluate


def healthy_evidence(cell):
    evidence = dict(no_false_held=True, false_held=False, route_line_count=0, injected=True, rows_final=0,
                    received={str(index + 1): 1 for index in range(cell.count)}, message_ids=list(range(1, cell.count + 1)),
                    attempts=[dict(phase='reserved', token='one', transport=cell.transport, members=str(cell.count)),
                              dict(phase='confirmed', token='one', transport=cell.transport, retired=str(cell.count), elapsed_ms='100')])
    if cell.transport == 'fetch':
        evidence.update(rows_while_fetch_result_held=cell.count, fetch_tool_result=True, fetch_token=True,
                        fetch_trailer_complete=True, fetch_source_occurrences=cell.count)
    return evidence


def author_witness(cell, evidence):
    """Author hypothetical facts; these are not recovered from a historical run."""
    configuration = dict(evidence, run_id='run', route_id='route', host_session_id='host-session', session_id='session')
    scenario, contract, observation = build_verdict_inputs(cell, configuration)
    observation['provenance'].update(kind='synthetic', capture_id='authored', known_defect_baselines=[])
    observation.update(collection_complete=True, events=[], barriers=[], streams=[], artifacts=[], queue_snapshots=[])
    scope = dict(run_id='run', route_id='route', host_session_id='host-session', session_id='session', connection_epoch_id='epoch', claim_generation=1)
    roles = ('injection', 'broker', 'host', 'transport', 'queue', 'contract', 'state', 'recovery')
    for role in roles:
        observation['artifacts'].append(dict(id=role, kind=role, content_digest=None))
        observation['streams'].append(dict(id=role, role=role, scope=deepcopy(scope), state='complete', first_seq=0, last_seq=100,
                                           through_barrier_id='final', artifact_ids=[role], detail='authored complete observation window'))
    def member(index):
        return dict(row_id='row-' + str(index), revision=dict(kind='exact', value='revision-' + str(index)), source_ids=[str((index - 1) % cell.count + 1)])
    def members(count):
        return [member(index + 1) for index in range(count)]
    all_members = members(max([cell.count] + [int(attempt.get('members', attempt.get('retired', 0))) for attempt in evidence.get('attempts', [])]
                              + [value for value in (evidence.get('rows_final'), evidence.get('rows_while_fetch_result_held')) if type(value) is int]))
    def event(identity, milestone, seq, payload, *, rows=None, token='one', transport=None, role='broker', **overrides):
        result = dict(id=identity, scope=deepcopy(scope), position=dict(stream_id=role, seq=seq, clock_id='clock', time_ms=seq * 1000),
                      artifact_ref=dict(artifact_id=role, locator=identity), collection_complete=True, caused_by=[],
                      origin='host-evidence' if milestone in ('transcript_recorded', 'fetch_result_recorded') else (
                          'broker-ack' if milestone == 'receipt_accepted' else 'retirement' if milestone == 'attempt_terminal' else 'rig-control'),
                      milestone=milestone, transport=transport or cell.transport, attempt_id='attempt-' + str(token), group_id='group-' + str(token),
                      token=token, operation_id='operation', delivery_id='delivery-' + identity, members=deepcopy(rows if rows is not None else members(cell.count)), payload=deepcopy(payload))
        result.update(overrides)
        observation['events'].append(result)
        return result
    def barrier(identity, name, cutoff):
        result = dict(id=identity, name=name, scope=deepcopy(scope), state='reached', stream_cutoffs={role: cutoff for role in roles},
                      artifact_refs=[dict(artifact_id='state', locator=identity)], collection_complete=True)
        observation['barriers'].append(result)
        return result
    def snapshot(name, count, cutoff):
        if name not in {item['id'] for item in observation['barriers']}:
            barrier(name, name, cutoff)
        result = dict(id='queue-' + name, barrier_id=name, scope=deepcopy(scope), state='complete' if type(count) is int else 'unavailable',
                      rows=members(count) if type(count) is int else [], artifact_refs=[dict(artifact_id='queue', locator=name)])
        observation['queue_snapshots'].append(result)
        return result
    event('state', 'state_observed', 1, dict(state=cell.state, detail='authored state interval'), rows=[], role='state')
    barrier('injection', 'injection', 10)
    barrier('final', 'final', 100)
    event('observation-end', 'state_observed', 100, dict(state=cell.state, detail='authored observation end'), rows=[], role='state')
    observation['state_proof'] = dict(state=cell.state, barrier_id='injection', scope=deepcopy(scope), proven=True, event_ids=['state'], detail='authored')
    observation['session_proof'] = dict(posture=cell.session, freshness=scenario['freshness'], barrier_id='injection', scope=deepcopy(scope), proven=True, event_ids=['state'], detail='authored')
    observation['contract_observations'] = [dict(barrier_id=identity, scope=deepcopy(scope), axes=deepcopy(contract['axes']),
                                               artifact_refs=[dict(artifact_id='contract', locator=identity)], collection_complete=True) for identity in ('injection', 'final')]
    observation['sources'] = [dict(slot=index, source_id=str(index + 1), kind=cell.kind, admission_event_id='admission') for index in range(cell.count)]
    event('admission', 'injection_completed', 11, dict(accepted=bool(evidence.get('injected'))), role='injection')
    event('persisted', 'row_persisted', 12, {}, rows=all_members, origin='persistence')
    snapshot('pre_injection', 0, 9)
    snapshot('final', evidence.get('rows_final'), 100)
    if scenario['readiness_barrier_id']:
        barrier('ready', 'ready', 20)
        snapshot('before_ready', evidence.get('rows_before_ready', cell.count), 19)
    if evidence.get('attempt_before_ready'):
        for index, description in enumerate(evidence.get('attempt_before_ready_observations') or ['observation not recorded']):
            event('early-' + str(index), 'delivery_offered', 15 + index, dict(description=description), rows=[])
    for index, attempt in enumerate(evidence.get('attempts', [])):
        token = attempt.get('token')
        if attempt['phase'] == 'reserved':
            event('attempt-' + str(index), 'attempt_reserved', 30 + index, dict(deadline_ms=90000),
                  rows=members(int(attempt.get('members', 0))), token=token, transport=attempt['transport'])
        else:
            event('attempt-' + str(index), 'attempt_terminal', 70 + index,
                  dict(outcome=attempt['phase'], elapsed_ms=int(attempt['elapsed_ms']) if 'elapsed_ms' in attempt else None,
                       retired_members=members(int(attempt.get('retired', 0))), reason=''), rows=members(int(attempt.get('retired', 0))), token=token, transport=attempt['transport'])
            event('receipt-' + str(index), 'receipt_accepted', 60 + index, dict(elapsed_ms=90),
                  rows=members(int(attempt.get('retired', cell.count))), token=token, transport=attempt['transport'])
    if cell.transport == 'fetch':
        event('request', 'fetch_requested', 25, dict(ack=True), role='transport')
        broker_trailer = dict(state='complete' if evidence.get('fetch_token') else 'absent', token='one' if evidence.get('fetch_token') else None,
                              members=members(cell.count) if evidence.get('fetch_token') else [])
        event('broker-result', 'fetch_result_produced', 40, dict(success=True, trailer=broker_trailer), role='transport')
        snapshot('fetch_result_held', evidence.get('rows_while_fetch_result_held'), 45)
        if evidence.get('fetch_tool_result'):
            host_trailer = deepcopy(broker_trailer) if evidence.get('fetch_trailer_complete') else dict(state='absent', token=None, members=[])
            count = evidence.get('fetch_source_occurrences', 0)
            for index in range(count):
                event('host-' + str(index), 'fetch_result_recorded', 50 + index,
                      dict(host_record_id='record-' + str(index), success=True, trailer=host_trailer), rows=[member(index % cell.count + 1)], role='host')
    else:
        for source, count in evidence.get('received', {}).items():
            for index in range(count):
                row = member(int(source)) if source.isdigit() else dict(row_id='extra-row', revision=dict(kind='exact', value='extra'), source_ids=[source])
                row['source_ids'] = [source]
                event('host-' + source + '-' + str(index), 'transcript_recorded', 50 + index,
                      dict(host_record_id='record-' + source + '-' + str(index)), rows=[row], role='host')
    route_count = evidence.get('route_line_count')
    if type(route_count) is int and route_count >= 0:
        if route_count:
            event('route-notice', 'notice_emitted', 80, dict(kind='route', held_count=None,
                  route_diagnostic_lines=['automatic diagnostic'] * route_count, queue_snapshot_id=None), rows=[])
    else:
        event('unavailable-route-notice', 'notice_emitted', 80, dict(kind='route', held_count=None, route_diagnostic_lines=[], queue_snapshot_id=None), rows=[], collection_complete=False)
    if evidence.get('false_held') or evidence.get('no_false_held') is False:
        snapshot('notice/held', 0, 81)
        event('held', 'notice_emitted', 81, dict(kind='held', held_count=1, route_diagnostic_lines=[], queue_snapshot_id='queue-notice/held'), rows=[])
    elif 'no_false_held' not in evidence:
        event('held', 'notice_emitted', 81, dict(kind='held', held_count=None, route_diagnostic_lines=[], queue_snapshot_id=None), rows=[])
    return dict(scenario=scenario, contract=contract, observation=observation)


def legacy_fixture_inputs(cell, evidence, synthetic_witness, collect_only=False):
    """Validate the authored witness's legacy projection; never repair its facts."""
    witness = deepcopy(synthetic_witness)
    scenario, contract, observation = witness['scenario'], witness['contract'], witness['observation']
    scenario['evaluation_mode'] = 'collect_only' if collect_only else 'verify'
    observation['setup_errors'] = list(evidence.get('setup_errors', []))
    observation['run_errors'] = list(evidence.get('run_errors', []))
    assert scenario['count'] == cell.count
    assert all(scenario[key] == getattr(cell, key) for key in ('transport', 'state', 'session', 'kind', 'burst'))
    if not observation['setup_errors']:
        _validate_projection(cell, evidence, observation, derive(scenario, contract, observation))
    return scenario, contract, observation


def _validate_projection(cell, evidence, observation, projection):
    for key in ('injected', 'rows_final', 'rows_before_ready', 'rows_while_fetch_result_held', 'fetch_tool_result',
                    'fetch_token', 'fetch_trailer_complete', 'fetch_source_occurrences', 'route_line_count'):
        if key in evidence:
            assert projection[key] == evidence[key], (key, projection[key], evidence[key])
    if cell.transport != 'fetch' and 'received' in evidence:
        # Missing keys explicitly mean zero, not an invented occurrence.
        expected = dict.fromkeys(projection['source_ids'], 0)
        expected.update(evidence['received'])
        assert projection['received'] == expected, (projection['received'], expected)
    if 'no_false_held' in evidence:
        assert projection['no_false_held'] == (evidence['no_false_held'] is True and not evidence.get('false_held'))
    if evidence.get('attempt_before_ready'):
        assert projection['offered_before_ready'] == (evidence.get('attempt_before_ready_observations') or ['observation not recorded'])
    authored_attempts = [event for event in observation['events'] if event['id'].startswith('attempt-')]
    assert len(authored_attempts) == len(evidence.get('attempts', []))
    for old, event in zip(evidence.get('attempts', []), authored_attempts):
        assert old.get('token') == event['token']
        assert old['transport'] == event['transport']
        assert (old['phase'] == 'reserved') == (event['milestone'] == 'attempt_reserved')
        if old['phase'] != 'reserved':
            assert old['phase'] == event['payload']['outcome']
            assert event['payload']['elapsed_ms'] == (int(old['elapsed_ms']) if 'elapsed_ms' in old else None)
        key = 'members' if old['phase'] == 'reserved' else 'retired'
        actual = event['members'] if key == 'members' else event['payload']['retired_members']
        assert len(actual) == int(old.get(key, 0))


def witnessed_verdict(cell, evidence):
    """Exercise harness negatives whose legacy claims the core must reject.

    Validate authored claim cardinalities before evaluating their validity. The
    strict characterization adapter above still requires accepted projections
    to match; no witness or legacy fact is rewritten to manufacture a verdict.
    """
    witness = author_witness(cell, evidence)
    inputs = tuple(witness[key] for key in ('scenario', 'contract', 'observation'))
    observation = inputs[2]
    if observation['setup_errors']:
        return evaluate(*inputs)['reasons']
    projection = derive(*inputs)
    hosts = [event for event in observation['events'] if event['origin'] == 'host-evidence']
    occurrences = {source['source_id']: set() for source in observation['sources']}
    for event in hosts:
        for member in event['members']:
            for source in member['source_ids']:
                occurrences.setdefault(source, set()).add(event['delivery_id'])
    projection['received'] = {source: len(deliveries) for source, deliveries in occurrences.items()}
    recorded = [event for event in hosts if event['milestone'] == 'fetch_result_recorded' and event['payload']['success']]
    produced = [event for event in observation['events'] if event['milestone'] == 'fetch_result_produced' and event['payload']['success']]
    projection.update(fetch_tool_result=bool(recorded), fetch_source_occurrences=sum(projection['received'].values()) if recorded else 0,
                      fetch_token=any(event['payload']['trailer']['state'] == 'complete' and event['payload']['trailer']['token'] for event in produced),
                      fetch_trailer_complete=any(host['payload']['trailer']['state'] == 'complete'
                          and host['payload']['trailer'] == broker['payload']['trailer'] for host in recorded for broker in produced))
    _validate_projection(cell, evidence, observation, projection)
    return evaluate(*inputs)['reasons']
