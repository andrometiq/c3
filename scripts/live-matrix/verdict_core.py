"""Deterministic verdicts over normalized evidence. No extraction or execution."""
from collections import Counter
from copy import deepcopy

ROUTE_LINE_LIMIT = 0
TRANSPORTS = ('channel', 'inbox', 'fetch')
STATES = ('idle', 'foreground', 'background', 'startup', 'reconnect')
HEALTH = ('healthy', 'expiry', 'restart', 'degraded', 'delivery_failure')
HOST_MILESTONES = ('queue_accepted', 'input_landed', 'transcript_recorded', 'fetch_result_recorded')
ADDITIONAL = [
    ('expected contract observed', 'observed delivery contract does not match expected contract'),
    ('state proven at injection', 'requested host state was not proven at injection'),
    ('session posture proven', 'requested session posture was not proven at injection'),
    ('delivery identities correlated', 'delivery evidence identity mismatch'),
    ('retired rows match authorized rows', 'retired rows do not match authorized delivery members'),
    ('retirement follows contract evidence', 'rows retired before contract evidence'),
    ('fault expectation observed', 'observed fault does not match scenario health class'),
    ('source disposition preserved', 'injected source lacks its required final disposition'),
    ('automatic attachment recovered', 'automatic attachment recovery was not proven'),
    ('collection complete', 'evidence collection incomplete'),
]

# Ordered schemas also define which malformed field is reported first.
ID = 'id'
NULL_ID = ('nullable', ID)
INTEGER = int
NULL_INT = ('nullable', int)
REVISION = {'kind': ('exact', 'record_only', 'unknown'), 'value': ('nullable', str)}
MEMBER = {'row_id': ID, 'revision': REVISION, 'source_ids': [ID]}
SCOPE = dict(run_id=ID, route_id=NULL_ID, host_session_id=NULL_ID, session_id=NULL_ID,
             connection_epoch_id=NULL_ID, claim_generation=NULL_INT)
REFERENCE = {'artifact_id': ID, 'locator': str}
POSITION = dict(stream_id=ID, seq=int, clock_id=NULL_ID, time_ms=NULL_INT)
AXES = dict(negotiation=('none', 'v1'), live_eligibility=dict(channel=bool, inbox=bool),
            receipt_type=('none', 'transcript', 'queue_acceptance', 'input_echo'),
            fetch_policy=('receipt', 'consume', 'unavailable'), accepted_modes=[('channel', 'inbox', 'fetch_receipt')])
TIMING = dict(limit_ms=int, basis=('terminal_confirmation', 'broker_receipt'))
SCENARIO = dict(schema_version=int, id=ID, transport=TRANSPORTS, kind=('text', 'voice', 'photo'),
                burst=('single', 'double'), count=int, state=STATES, session=('fresh', 'resumed'),
                freshness=('transcript_free_at_injection', 'new_conversation', 'not_applicable'),
                resume_requirement=('delivery_only', 'attachment_recovery'), health_class=HEALTH,
                final_dispositions=[('delivered', 'recoverable')], evaluation_mode=('verify', 'collect_only'),
                run_id=ID, route_id=ID, host_session_id=NULL_ID, session_id=NULL_ID,
                injection_barrier_id=ID, readiness_barrier_id=NULL_ID, final_barrier_id=ID,
                observation_duration_ms=int)
CONTRACT = dict(schema_version=int, id=ID, capability_id=ID, axes=AXES,
                milestones=dict(live=('nullable', HOST_MILESTONES), fetch=('nullable', ('fetch_result_recorded',))),
                timing=dict(live=('nullable', TIMING), fetch=('nullable', TIMING)),
                readiness_boundary=NULL_ID, contract_barrier_names=[ID])
TRAILER = dict(state=('absent', 'malformed', 'complete'), token=NULL_ID, members=[MEMBER])
PAYLOADS = {
    'injection_completed': dict(accepted=bool), 'row_persisted': {},
    'attempt_reserved': dict(deadline_ms=NULL_INT),
    'receipt_accepted': dict(elapsed_ms=NULL_INT), 'receipt_rejected': dict(reason=str),
    'attempt_terminal': dict(outcome=('confirmed', 'failed', 'expired', 'released'), elapsed_ms=NULL_INT,
                             retired_members=[MEMBER], reason=str),
    'queue_accepted': dict(host_record_id=ID), 'input_landed': dict(host_record_id=ID),
    'transcript_recorded': dict(host_record_id=ID), 'fetch_requested': dict(ack=bool),
    'fetch_result_produced': dict(success=bool, trailer=TRAILER),
    'fetch_result_recorded': dict(host_record_id=ID, success=bool, trailer=TRAILER),
    'delivery_offered': dict(description=str),
    'notice_emitted': dict(kind=('held', 'route', 'other', 'degraded'), held_count=NULL_INT,
                           route_diagnostic_lines=[str], queue_snapshot_id=NULL_ID),
    'fault_observed': dict(kind=('expiry', 'restart', 'degraded', 'delivery_failure'), reason=str),
    'source_recoverable': dict(source_ids=[ID], storage=('durable_queue', 'upstream'), checkpoint_id=NULL_ID),
    'attachment_recovered': dict(previous_session_id=ID, automatic=bool), 'explicit_attach': {},
    'state_observed': dict(state=str, detail=str),
}
EVENT = dict(id=ID, scope=SCOPE, position=POSITION, artifact_ref=REFERENCE, collection_complete=bool,
             caused_by=[ID], origin=('host-evidence', 'broker-ack', 'persistence', 'retirement', 'expiry', 'failure', 'rig-control'),
             milestone=tuple(PAYLOADS), transport=('nullable', TRANSPORTS), attempt_id=NULL_ID,
             group_id=NULL_ID, token=NULL_ID, operation_id=NULL_ID, delivery_id=NULL_ID, members=[MEMBER], payload=dict)
BARRIER = dict(id=ID, name=str, scope=SCOPE, state=('reached', 'not_reached', 'unknown'),
               stream_cutoffs=dict, artifact_refs=[REFERENCE], collection_complete=bool)
SNAPSHOT = dict(id=ID, barrier_id=ID, scope=SCOPE, state=('complete', 'unavailable', 'malformed', 'inconsistent'),
                rows=[MEMBER], artifact_refs=[REFERENCE])
PROOF = dict(state=str, barrier_id=ID, scope=SCOPE, proven=bool, event_ids=[ID], detail=str)
SESSION_PROOF = dict(posture=('fresh', 'resumed'), freshness=SCENARIO['freshness'], barrier_id=ID,
                     scope=SCOPE, proven=bool, event_ids=[ID], detail=str)
PROVENANCE = dict(kind=('synthetic', 'live_capture', 'raw_replay'), capture_id=ID, schema_version=int,
                  extractor_version=str, contract_version=str, broker_build=str, adapter_build=str,
                  host_version=str, platform=str, mode=str, known_defect_baselines=[str])
STREAM = dict(id=ID, role=('injection', 'broker', 'host', 'transport', 'queue', 'contract', 'state', 'recovery'),
              scope=SCOPE, state=('complete', 'missing', 'unreadable', 'malformed', 'partial', 'truncated', 'unavailable'),
              first_seq=NULL_INT, last_seq=NULL_INT, through_barrier_id=NULL_ID, artifact_ids=[ID], detail=str)
OBSERVATION = dict(schema_version=int, provenance=PROVENANCE, setup_errors=[str], run_errors=[str],
                   collection_complete=bool, streams=[STREAM], artifacts=[dict(id=ID, kind=str, content_digest=('nullable', str))],
                   sources=[dict(slot=int, source_id=ID, kind=SCENARIO['kind'], admission_event_id=ID)],
                   events=[EVENT], barriers=[BARRIER], queue_snapshots=[SNAPSHOT],
                   contract_observations=[dict(barrier_id=ID, scope=SCOPE, axes=('nullable', AXES),
                                               artifact_refs=[REFERENCE], collection_complete=bool)],
                   state_proof=('nullable', PROOF), session_proof=('nullable', SESSION_PROOF))


class InvalidInput(ValueError):
    pass


def _validate(value, schema, path):
    def invalid(problem):
        raise InvalidInput(path + ' ' + problem)
    if isinstance(schema, tuple) and schema and schema[0] == 'nullable':
        if value is not None:
            _validate(value, schema[1], path)
    elif isinstance(schema, dict):
        if type(value) is not dict:
            invalid('must be an object')
        for key, child in schema.items():
            if key not in value:
                raise InvalidInput(path + '.' + key + ' is required')
            _validate(value[key], child, path + '.' + key)
        if any(type(key) is not str for key in value):
            invalid('keys must be strings')
        for key in sorted(value):
            if key not in schema:
                raise InvalidInput(path + '.' + key + ' is not allowed')
        if schema is REVISION:
            if value['kind'] == 'exact' and not value['value']:
                invalid('exact revision requires a value')
            if value['kind'] != 'exact' and value['value'] is not None:
                invalid('non-exact revision requires null value')
    elif isinstance(schema, list):
        if type(value) is not list:
            invalid('must be a list')
        for index, child in enumerate(value):
            _validate(child, schema[0], f'{path}[{index}]')
    elif isinstance(schema, tuple):
        if type(value) is not str or value not in schema:
            invalid('has an unsupported value')
    elif schema == ID:
        if type(value) is not str or not value:
            invalid('must be a nonempty string')
    elif type(value) is not schema:
        invalid('must be ' + schema.__name__)


def _validate_inputs(scenario, contract, observation):
    _validate(scenario, SCENARIO, 'scenario')
    if scenario['schema_version'] != 1:
        raise InvalidInput('scenario.schema_version must equal 1')
    if scenario['count'] != (1 if scenario['burst'] == 'single' else 2):
        raise InvalidInput('scenario.count must match burst')
    if scenario['session'] == 'resumed' and scenario['freshness'] != 'not_applicable':
        raise InvalidInput('scenario.freshness must be not_applicable for resumed')
    if scenario['session'] == 'fresh' and (scenario['freshness'] == 'not_applicable' or scenario['resume_requirement'] != 'delivery_only'):
        raise InvalidInput('scenario.freshness or resume_requirement conflicts with fresh')
    dispositions = scenario['final_dispositions']
    if len(dispositions) != scenario['count'] or (scenario['health_class'] == 'healthy' and set(dispositions) != {'delivered'}):
        raise InvalidInput('scenario.final_dispositions must match requested sources and health class')
    if scenario['health_class'] == 'delivery_failure' and 'recoverable' not in dispositions:
        raise InvalidInput('scenario.final_dispositions requires a recoverable source')
    if scenario['observation_duration_ms'] < 0:
        raise InvalidInput('scenario.observation_duration_ms must be nonnegative')
    _validate(contract, CONTRACT, 'contract')
    if contract['schema_version'] != 1:
        raise InvalidInput('contract.schema_version must equal 1')
    axes = contract['axes']
    modes = axes['accepted_modes']
    if len(modes) != len(set(modes)) or (axes['negotiation'] == 'none' and modes):
        raise InvalidInput('contract.axes.accepted_modes conflicts with negotiation or contains duplicates')
    transport = scenario['transport']
    if transport == 'fetch':
        if any(axes['live_eligibility'].values()) or axes['fetch_policy'] == 'unavailable':
            raise InvalidInput('contract.axes conflicts with requested fetch transport')
    elif not axes['live_eligibility'][transport]:
        raise InvalidInput('contract.axes.live_eligibility excludes requested transport')
    elif axes['negotiation'] == 'v1' and (axes['receipt_type'] != 'transcript' or transport not in modes):
        raise InvalidInput('contract.axes negotiated live requires transcript and accepted transport')
    if axes['fetch_policy'] == 'receipt' and (axes['negotiation'] != 'v1' or 'fetch_receipt' not in modes or contract['milestones']['fetch'] != 'fetch_result_recorded'):
        raise InvalidInput('contract.axes.fetch_policy receipt requires negotiation, mode and milestone')
    live_milestone = {'none': None, 'queue_acceptance': 'queue_accepted', 'input_echo': 'input_landed', 'transcript': 'transcript_recorded'}[axes['receipt_type']]
    if contract['milestones']['live'] != live_milestone:
        raise InvalidInput('contract.milestones.live conflicts with receipt_type')
    for name, timing in contract['timing'].items():
        if timing is not None and timing['limit_ms'] <= 0:
            raise InvalidInput('contract.timing.' + name + '.limit_ms must be positive')
    if axes['negotiation'] == 'v1' and transport != 'fetch' and contract['timing']['live'] is None:
        raise InvalidInput('contract.timing.live is required for negotiated live')
    if transport == 'fetch' and axes['fetch_policy'] == 'receipt' and contract['timing']['fetch'] is None:
        raise InvalidInput('contract.timing.fetch is required for receipt fetch')
    # A setup failure has no delivery envelope to validate.
    if type(observation) is not dict:
        raise InvalidInput('observation must be an object')
    for name in ('schema_version', 'setup_errors', 'run_errors', 'collection_complete'):
        if name not in observation:
            raise InvalidInput('observation.' + name + ' is required')
        _validate(observation[name], OBSERVATION[name], 'observation.' + name)
    if observation['schema_version'] != 1:
        raise InvalidInput('observation.schema_version must equal 1')
    if observation['setup_errors']:
        return
    _validate(observation, OBSERVATION, 'observation')
    if observation['provenance']['schema_version'] != 1:
        raise InvalidInput('observation.provenance.schema_version must equal 1')
    for index, event in enumerate(observation['events']):
        _validate(event['payload'], PAYLOADS[event['milestone']], f'observation.events[{index}].payload')
    for index, barrier in enumerate(observation['barriers']):
        if any(type(key) is not str for key in barrier['stream_cutoffs']):
            raise InvalidInput(f'observation.barriers[{index}].stream_cutoffs keys must be strings')
        for key in sorted(barrier['stream_cutoffs']):
            _validate(key, ID, f'observation.barriers[{index}].stream_cutoffs key')
            _validate(barrier['stream_cutoffs'][key], int, f'observation.barriers[{index}].stream_cutoffs.{key}')


def _window(contract, is_fetch):
    name, limit = ('fetch', 60000) if is_fetch else ('live', 15000)
    timing = contract['timing'][name]
    is_historical = timing == {'limit_ms': limit, 'basis': 'terminal_confirmation'}
    if is_fetch:
        return ('fetch confirmation within 60 seconds', 'fetch group confirmation missing or outside 60-second window') if is_historical else (
            'fetch confirmation within contract window', 'fetch group confirmation missing or outside contract window')
    return ('receipt within 15 seconds', 'receipt missing or outside 15-second window') if is_historical else (
        'receipt within contract window', 'receipt missing or outside contract window')


def delivery_assertions(scenario, contract):
    """Return selected delivery labels, in evaluation order."""
    is_healthy = scenario['health_class'] == 'healthy'
    labels = ['injection completed']
    if is_healthy:
        labels.append('durable rows retired')
    # The historical setup prefix includes this label even without a tested gate.
    labels.append('no attempt before initialization')
    if scenario['health_class'] != 'degraded':
        labels.append('no false Held')
    labels.append('route line count')
    if scenario['transport'] == 'fetch':
        labels.append('no live offer in fetch-only cell')
        if contract['axes']['fetch_policy'] == 'receipt':
            if is_healthy:
                labels += ['rows retained until fetch receipt', 'successful fetch tool result']
            labels.append(_window(contract, True)[0])
            if is_healthy:
                labels += ['fetch retirement count', 'complete fetch receipt trailer', 'broker fetch receipt token']
        else:
            labels.append('successful broker fetch result')
            if contract['milestones']['fetch']:
                labels.append('successful fetch tool result')
            if is_healthy:
                labels.append('fetch retirement count')
        if is_healthy:
            labels.append('each source fetched exactly once')
    elif contract['axes']['negotiation'] == 'v1':
        labels += ['negotiated attempt observed', 'no fallback or wrong transport']
        if is_healthy:
            labels.append('each source attempted once')
        labels.append(_window(contract, False)[0])
        if is_healthy:
            labels += ['retirement count', 'each source received exactly once']
    else:
        labels += ['live acceptance observed', 'no fallback or wrong transport']
        if is_healthy:
            labels += ['each source received exactly once', 'retirement count']
    labels += [label for label, _ in ADDITIONAL[:-1] if label != 'automatic attachment recovered' or scenario['resume_requirement'] == 'attachment_recovery']
    return labels


def _member_key(member):
    revision = member['revision']
    return member['row_id'], revision['kind'], revision['value']


def _members(members):
    return {_member_key(member) for member in members}


def _member_identities(members):
    return {(_member_key(member), frozenset(member['source_ids'])) for member in members}


def _source_set(members):
    return {source for member in members for source in member['source_ids']}


def _scope_matches(scope, scenario):
    return all(scope[key] == scenario[key] for key in ('run_id', 'route_id', 'host_session_id', 'session_id'))


def _same_owner(left, right):
    return left['scope'] == right['scope']


def _same_attempt(left, right):
    return (_same_owner(left, right) and all(value is not None for value in left['scope'].values())
            and all(left[key] is not None and left[key] == right[key] for key in ('attempt_id', 'group_id', 'token')))


def _before(left, right, events, barriers=()):
    """Return true, false, or unknown; never order by list position."""
    if left['id'] == right['id']:
        return False
    pending, seen = list(right['caused_by']), set()
    while pending:
        identity = pending.pop()
        if identity == left['id']:
            return True
        if identity not in seen and identity in events:
            seen.add(identity)
            pending.extend(events[identity]['caused_by'])
    a, b = left['position'], right['position']
    if a['stream_id'] == b['stream_id']:
        return a['seq'] < b['seq']
    if a['clock_id'] is not None and a['clock_id'] == b['clock_id'] and a['time_ms'] is not None and b['time_ms'] is not None:
        return a['time_ms'] < b['time_ms']
    for barrier in barriers:
        if barrier['state'] != 'reached' or not barrier['collection_complete']:
            continue
        if not _scope_matches(barrier['scope'], left['scope']) or not _scope_matches(barrier['scope'], right['scope']):
            continue
        before_cut, after_cut = _at_cut(left, barrier), _at_cut(right, barrier)
        if before_cut is True and after_cut is False:
            return True
        if before_cut is False and after_cut is True:
            return False
    return None


def _at_cut(event, barrier):
    cutoff = barrier['stream_cutoffs'].get(event['position']['stream_id'])
    return None if cutoff is None else event['position']['seq'] <= cutoff


def _cut_before(left, right):
    """A later cut cannot move any participating stream backwards."""
    if not left or not right or left['id'] == right['id']:
        return False
    a, b = left['stream_cutoffs'], right['stream_cutoffs']
    return bool(a) and set(a) <= set(b) and all(a[key] <= b[key] for key in a) and any(a[key] < b[key] for key in a)


def _reservation_matches(event, reservations):
    return [reservation for reservation in reservations if _same_attempt(event, reservation)
            and event['transport'] == reservation['transport']
            and (event['transport'] != 'fetch' or event['operation_id'] is not None
                 and event['operation_id'] == reservation['operation_id'])
            and bool(event['members'])
            and _member_identities(event['members']) <= _member_identities(reservation['members'])]


def _same_operation(left, right):
    return (_same_owner(left, right) and left['operation_id'] is not None
            and left['operation_id'] == right['operation_id'] and left['transport'] is not None and left['transport'] == right['transport'])


def _known_members(members, known, exact=False):
    return (bool(members) and len({member['row_id'] for member in members}) == len(members)
            and all(member['revision']['kind'] != 'unknown' and (not exact or member['revision']['kind'] == 'exact')
                    and bool(member['source_ids']) and len(set(member['source_ids'])) == len(member['source_ids'])
                    and set(member['source_ids']) == known.get(_member_key(member)) for member in members))


def _trailer_valid(trailer):
    return (trailer['state'] == 'complete' and trailer['token'] is not None and bool(trailer['members'])
            and len({member['row_id'] for member in trailer['members']}) == len(trailer['members'])
            and all(member['revision']['kind'] == 'exact' for member in trailer['members']))


def _trailer_bound(event, reservations, known_members):
    """Return true, false, or unknown for reservation/source binding."""
    trailer = event['payload']['trailer']
    if not _trailer_valid(trailer) or trailer['token'] != event['token']:
        return False
    if any(not member['source_ids'] or set(member['source_ids']) != known_members.get(_member_key(member))
           for member in trailer['members']):
        return False
    identities = _member_identities(trailer['members'])
    if not _member_identities(event['members']) <= identities:
        return False
    matches = _reservation_matches(event, reservations)
    # Missing reservation identity cannot prove binding, but is not a positive
    # trailer contradiction. Its primary assertion/completeness check owns it.
    return any(identities == _member_identities(reservation['members']) for reservation in matches) if matches else None


def _trailer_pair_matches(left, right):
    return (_same_owner(left, right) and left['operation_id'] is not None and left['operation_id'] == right['operation_id']
            and _trailer_valid(left['payload']['trailer']) and _trailer_valid(right['payload']['trailer'])
            and left['payload']['trailer']['token'] == right['payload']['trailer']['token']
            and _member_identities(left['payload']['trailer']['members']) == _member_identities(right['payload']['trailer']['members']))


def _trailer_matches(left, right, reservations, known_members):
    return (_trailer_pair_matches(left, right) and _trailer_bound(left, reservations, known_members) is True
            and _trailer_bound(right, reservations, known_members) is True)


def _reconcile_queue(context, persisted, retirements):
    """Account for every observed row through later cuts, including backlog."""
    complete, unauthorized, mismatch = True, False, False
    snapshots = list(context['snapshots'].values())
    events, barriers = context['events'], context['barriers']
    def after(point, event):
        if 'barrier_id' in point:
            included = _at_cut(event, barriers[point['barrier_id']])
            return None if included is None else not included
        return _before(point, event, events, barriers.values())
    origins = [(snapshot, snapshot['rows']) for snapshot in snapshots]
    origins += [(event, event['members']) for event in persisted]
    for snapshot in snapshots:
        cut = barriers[snapshot['barrier_id']]
        rows = {member['row_id']: member for member in snapshot['rows']}
        if snapshot['rows'] and not _known_members(snapshot['rows'], context['known_members']):
            complete = False
        for point, members in origins:
            if 'barrier_id' in point:
                if not _cut_before(barriers[point['barrier_id']], cut):
                    continue
            elif _at_cut(point, cut) is not True:
                continue
            for member in members:
                current, latest = member, point
                for update in persisted:
                    if _at_cut(update, cut) is not True:
                        continue
                    replacement = next((item for item in update['members'] if item['row_id'] == member['row_id']), None)
                    if replacement is not None:
                        order = after(latest, update)
                        if order is None and replacement != current:
                            complete = False
                        if order is not True:
                            continue
                        if set(replacement['source_ids']) != set(member['source_ids']):
                            mismatch = True
                            complete = False
                        current, latest = replacement, update
                surviving = rows.get(member['row_id'])
                if surviving is not None:
                    if _member_identities([surviving]) != _member_identities([current]):
                        mismatch = True
                        complete = False
                    continue
                if not any(_member_identities([current]) <= _member_identities(terminal['payload']['retired_members'])
                           and after(latest, terminal) is True and _at_cut(terminal, cut) is True for terminal in retirements):
                    unauthorized = True
                    complete = False
    return complete, unauthorized, mismatch


def _current_members(event, members, persisted, events, barriers):
    """A historical revision cannot authorize removal of a newer queue row."""
    for member in members:
        identity = _member_identities([member])
        for update in persisted:
            replacements = [item for item in update['members'] if item['row_id'] == member['row_id']]
            if not replacements or _member_identities(replacements) == identity:
                continue
            order = _before(update, event, events, barriers)
            if order is False:
                continue
            if order is None or not any(identity <= _member_identities(later['members'])
                    and _before(update, later, events, barriers) is True and _before(later, event, events, barriers) is True
                    for later in persisted):
                return False
    return True


# Per milestone: (allowed evidence roles, requires delivery bindings, required origin).
# There is no default policy: extending PAYLOADS requires extending this manifest.
# Claim selection also selects contract epochs; role and identity checks cannot drift.
EVENT_BINDINGS = {
    'injection_completed': (('injection',), False, None),
    'row_persisted': (('broker', 'queue'), False, None),
    'attempt_reserved': (('broker',), True, None),
    'receipt_accepted': (('broker', 'transport'), True, 'broker-ack'),
    'receipt_rejected': (('broker', 'transport'), False, None),
    'attempt_terminal': (('broker',), True, None),  # Nonempty retirement requires retirement origin.
    'queue_accepted': (('host',), True, 'host-evidence'),
    'input_landed': (('host',), True, 'host-evidence'),
    'transcript_recorded': (('host',), True, 'host-evidence'),
    'fetch_result_recorded': (('host',), True, 'host-evidence'),
    'fetch_requested': (('transport',), True, None),
    'fetch_result_produced': (('broker', 'transport'), True, None),
    'delivery_offered': (('broker', 'transport'), False, None),
    'notice_emitted': (('broker',), False, None),
    'fault_observed': (('broker', 'transport', 'state'), False, None),
    'source_recoverable': (('recovery',), False, None),
    'attachment_recovered': (('recovery',), False, None),
    'explicit_attach': (('recovery', 'state'), False, None),
    'state_observed': (('state',), False, None),
}


def _event_policy(event):
    # Unknown policy cannot supply a role witness and fails the manifest check.
    return EVENT_BINDINGS.get(event['milestone'], ((), False, None))


def _references_bound(item, context, roles=None, stream_ids=None):
    """Join artifacts to role streams; events and cuts own epoch attribution.

    A shared role stream can span epochs and claim generations. Its run, route,
    and sessions still have to match the evidence it supports.
    """
    references = item.get('artifact_refs', [item.get('artifact_ref')])
    return bool(references) and all(reference and reference['artifact_id'] in context['artifacts']
        and any(_scope_matches(stream['scope'], item['scope']) and stream['state'] == 'complete'
                and (roles is None or stream['role'] in roles)
                and (stream_ids is None or stream['id'] in stream_ids)
                and reference['artifact_id'] in stream['artifact_ids']
                for stream in context['streams'].values()) for reference in references)


def _event_bound(event, context):
    stream = context['streams'].get(event['position']['stream_id'])
    return bool(event['collection_complete'] and stream
                and _references_bound(event, context, _event_policy(event)[0], [stream['id']])
                and stream['first_seq'] is not None and stream['last_seq'] is not None
                and stream['first_seq'] <= event['position']['seq'] <= stream['last_seq'])


def _snapshot_bound(snapshot, context):
    barrier = context['barriers'].get(snapshot['barrier_id'])
    return bool(barrier and context['named_barrier'](barrier['name']) == barrier
                and snapshot['state'] == 'complete' and snapshot['scope'] == barrier['scope']
                and _references_bound(snapshot, context, ('queue',), barrier['stream_cutoffs'])
                and len({member['row_id'] for member in snapshot['rows']}) == len(snapshot['rows']))


def _reservation_bound(event, context, reservations):
    return any(_before(reservation, event, context['events'], context['barriers'].values()) is True
               for reservation in _reservation_matches(event, reservations))


def _admission_bound(source, scenario, contract, context):
    admission = context['events'].get(source['admission_event_id'])
    injection = context['barriers'].get(scenario['injection_barrier_id'])
    final = context['barriers'].get(scenario['final_barrier_id'])
    return bool(admission and admission in context['relevant'] and admission['milestone'] == 'injection_completed'
                and source['source_id'] in _source_set(admission['members']) and source['kind'] == scenario['kind']
                and injection and admission['scope'] == injection['scope'] and _at_cut(admission, injection) is False
                and final and _at_cut(admission, final) is True and _event_bound(admission, context)
                and _known_members(admission['members'], context['known_members'], contract['axes']['negotiation'] == 'v1'))


def _assignments_bound(sources, scenario):
    return (len(sources) == scenario['count']
            and sorted(source['slot'] for source in sources) == list(range(scenario['count']))
            and len({source['source_id'] for source in sources}) == scenario['count'])


def _admitted_sources_bound(admission, sources, contract, context):
    admitted = Counter(source for member in admission['members'] for source in member['source_ids'])
    assigned = Counter(source['source_id'] for source in sources if source['admission_event_id'] == admission['id'])
    return (bool(admitted) and admitted == assigned and all(count == 1 for count in admitted.values())
            and _known_members(admission['members'], context['known_members'], contract['axes']['negotiation'] == 'v1'))


def _proof_bound(proof, scenario, context):
    injection = context['barriers'].get(scenario['injection_barrier_id'])
    events = context['events']
    return bool(proof and proof['barrier_id'] == scenario['injection_barrier_id']
                and injection and injection['state'] == 'reached' and proof['scope'] == injection['scope']
                and _scope_matches(proof['scope'], scenario) and proof['event_ids']
                and all(identity in events and events[identity]['scope'] == proof['scope']
                        and _event_bound(events[identity], context)
                        and _at_cut(events[identity], injection) is True for identity in proof['event_ids']))


def _contract_bound(observed, context):
    barrier = context['barriers'].get(observed['barrier_id'])
    return bool(barrier and barrier['state'] == 'reached' and observed['axes'] is not None
                and observed['collection_complete'] and observed['scope'] == barrier['scope']
                and observed['scope']['connection_epoch_id'] is not None
                and _references_bound(observed, context, ('contract',), barrier['stream_cutoffs']))


def _context(scenario, contract, observation):
    """Index evidence for projections; enforcement belongs to _validate_bindings."""
    events = {}
    for event in observation['events']:
        events.setdefault(event['id'], event)
    context = dict(events=events, relevant=[event for event in events.values() if _scope_matches(event['scope'], scenario)],
                   artifacts={artifact['id'] for artifact in observation['artifacts']},
                   streams={stream['id']: stream for stream in observation['streams']},
                   barriers={barrier['id']: barrier for barrier in observation['barriers']})
    named_barriers = {}
    for barrier in context['barriers'].values():
        if _scope_matches(barrier['scope'], scenario):
            named_barriers.setdefault(barrier['name'], []).append(barrier)
    anchors = {'injection': scenario['injection_barrier_id'], 'final': scenario['final_barrier_id']}
    if scenario['readiness_barrier_id'] and contract['readiness_boundary']:
        anchors[contract['readiness_boundary']] = scenario['readiness_barrier_id']
    def named_barrier(name, epoch=None):
        candidates = named_barriers.get(name, [])
        if epoch is not None:
            candidates = [barrier for barrier in candidates if barrier['scope']['connection_epoch_id'] == epoch]
        pinned = context['barriers'].get(anchors.get(name))
        if name in anchors and (epoch is None or pinned and pinned['scope']['connection_epoch_id'] == epoch):
            candidates = [barrier for barrier in candidates if barrier['id'] == anchors[name]]
        return candidates[0] if len(candidates) == 1 else None
    context['named_barrier'] = named_barrier
    context['snapshots'] = {context['barriers'][snapshot['barrier_id']]['name']: snapshot
                            for snapshot in observation['queue_snapshots']
                            if _scope_matches(snapshot['scope'], scenario) and _snapshot_bound(snapshot, context)}
    context['invalid_deliveries'] = _delivery_conflicts(events.values())
    return context


def _delivery_conflicts(events):
    occurrences, records, invalid = {}, {}, set()
    for event in events:
        if event['milestone'] not in HOST_MILESTONES:
            continue
        scope_key = tuple(event['scope'][key] for key in SCOPE)
        occurrence = (scope_key, event['milestone'], event['delivery_id'])
        record = (scope_key, event['milestone'], event['payload']['host_record_id'])
        previous = occurrences.get(occurrence)
        conflicting = previous is not None and (previous['payload'] != event['payload']
            or _member_identities(previous['members']) != _member_identities(event['members'])
            or any(previous[key] != event[key] for key in ('transport', 'attempt_id', 'group_id', 'token', 'operation_id')))
        if conflicting or (record in records and records[record] != occurrence):
            invalid.add(occurrence)
            if record in records:
                invalid.add(records[record])
        occurrences.setdefault(occurrence, event)
        records.setdefault(record, occurrence)
    return invalid


def _binding(relation, subject, satisfied, *failures):
    # None is unknown, never a successful join. Missing proof always fails collection.
    return relation, subject, satisfied is True, ('collection_complete',) + failures


def _integrity_bindings(scenario, contract, observation, context):
    events = context['events']
    yield _binding('event identity -> contents', 'events', all(events[event['id']] == event for event in observation['events']), 'identity_mismatch')
    yield _binding('delivery <-> host record', 'events', not context['invalid_deliveries'], 'identity_mismatch')
    for name in ('artifacts', 'streams', 'barriers', 'queue_snapshots'):
        items = observation[name]
        yield _binding('unique identity', name, len({item['id'] for item in items}) == len(items))
    for event in events.values():
        pending, visited, acyclic = list(event['caused_by']), set(), True
        while pending:
            identity = pending.pop()
            if identity == event['id']:
                acyclic = False
                break
            if identity in events and identity not in visited:
                visited.add(identity)
                pending.extend(events[identity]['caused_by'])
        yield _binding('causality -> acyclic order', event['id'], acyclic and not any(
            identity in events and _before(event, events[identity], {}) is True for identity in event['caused_by']), 'identity_mismatch')
        yield _binding('causality -> event', event['id'], all(identity in events for identity in event['caused_by']))


def _coverage_bindings(scenario, contract, observation, context):
    streams, barriers = context['streams'], context['barriers']
    named_barrier, snapshots = context['named_barrier'], context['snapshots']
    yield _binding('scenario -> collection', scenario['id'], observation['collection_complete']
                   and scenario['host_session_id'] is not None and scenario['session_id'] is not None)
    yield _binding('milestone -> binding policy', 'schema', set(EVENT_BINDINGS) == set(PAYLOADS))
    roles = {'injection', 'broker', 'contract', 'state'}
    if scenario['health_class'] != 'degraded':
        roles.add('queue')
    if contract['milestones']['fetch' if scenario['transport'] == 'fetch' else 'live']:
        roles.add('host')
    if scenario['transport'] == 'fetch':
        roles.add('transport')
    if ('recoverable' in scenario['final_dispositions'] and scenario['health_class'] == 'degraded'
            or scenario['resume_requirement'] == 'attachment_recovery'):
        roles.add('recovery')
    required_stream_ids = {event['position']['stream_id'] for event in context['relevant']}
    required_stream_ids.update(stream['id'] for stream in streams.values()
                               if stream['role'] in roles and _scope_matches(stream['scope'], scenario))
    for role in sorted(roles):
        yield _binding('required role -> scoped stream', role, any(
            stream['role'] == role and _scope_matches(stream['scope'], scenario) for stream in streams.values()))
    final = barriers.get(scenario['final_barrier_id'])
    for identity in sorted(required_stream_ids):
        stream = streams.get(identity)
        yield _binding('stream -> final coverage', identity, bool(stream and _scope_matches(stream['scope'], scenario)
            and stream['state'] == 'complete' and stream['through_barrier_id'] == scenario['final_barrier_id']
            and stream['first_seq'] is not None and stream['last_seq'] is not None and stream['first_seq'] <= stream['last_seq']
            and stream['artifact_ids'] and all(artifact in context['artifacts'] for artifact in stream['artifact_ids'])
            and final and final['stream_cutoffs'].get(identity) == stream['last_seq']))
    for event in context['events'].values():
        yield _binding('event -> role/stream/artifact/owner', event['id'], _event_bound(event, context))
    for barrier in barriers.values():
        # The artifact records the cut, which can cover other role streams.
        yield _binding('barrier -> artifact stream', barrier['id'], barrier['collection_complete']
                       and _references_bound(barrier, context))
    required_snapshots = [] if scenario['health_class'] == 'degraded' else ['pre_injection', 'final']
    if scenario['readiness_barrier_id']:
        required_snapshots.append('before_ready')
    if scenario['transport'] == 'fetch' and contract['axes']['fetch_policy'] == 'receipt':
        required_snapshots.append('fetch_result_held')
    for name in required_snapshots:
        yield _binding('required snapshot -> anchored queue evidence', name, name in snapshots)
    for snapshot in observation['queue_snapshots']:
        if _scope_matches(snapshot['scope'], scenario):
            yield _binding('snapshot -> scenario cut/queue stream', snapshot['id'], _snapshot_bound(snapshot, context))
    for name in snapshots:
        yield _binding('snapshot name -> unique snapshot', name, sum(
            _scope_matches(snapshot['scope'], scenario) and snapshot['barrier_id'] == snapshots[name]['barrier_id']
            for snapshot in observation['queue_snapshots']) == 1)
    required_barriers = {scenario['injection_barrier_id'], scenario['final_barrier_id']}
    if scenario['readiness_barrier_id']:
        required_barriers.add(scenario['readiness_barrier_id'])
    required_barriers.update(snapshot['barrier_id'] for snapshot in snapshots.values())
    for item in observation['contract_observations']:
        if _scope_matches(item['scope'], scenario):
            for name in contract['contract_barrier_names']:
                barrier = named_barrier(name, item['scope']['connection_epoch_id'])
                if barrier:
                    required_barriers.add(barrier['id'])
    for identity in sorted(required_barriers):
        barrier = barriers.get(identity)
        yield _binding('required cut -> reached scenario barrier', identity, bool(
            barrier and barrier['state'] == 'reached' and _scope_matches(barrier['scope'], scenario)))
        for stream_id in sorted(required_stream_ids):
            yield _binding('required cut -> stream coverage', (identity, stream_id), _cut_bound(barrier, streams.get(stream_id)))
    # Optional cuts prove only their participating streams; required cuts above
    # enumerate every dependent stream, including otherwise empty role streams.
    for barrier in barriers.values():
        if not _scope_matches(barrier['scope'], scenario):
            continue
        yield _binding('cut -> reached nonempty boundary', barrier['id'], bool(barrier['stream_cutoffs']) and barrier['state'] == 'reached')
        for stream_id in barrier['stream_cutoffs']:
            stream = streams.get(stream_id)
            yield _binding('cut -> participating stream', (barrier['id'], stream_id), bool(
                stream and _scope_matches(stream['scope'], scenario) and stream['state'] == 'complete' and _cut_bound(barrier, stream)))
    injection, ready = barriers.get(scenario['injection_barrier_id']), barriers.get(scenario['readiness_barrier_id'])
    ordered = [named_barrier('pre_injection'), injection]
    if scenario['health_class'] == 'degraded' and ordered[0] is None:
        ordered.pop(0)
    if scenario['readiness_barrier_id']:
        ordered += [named_barrier('before_ready'), ready]
    ordered.append(final)
    for index, (left, right) in enumerate(zip(ordered, ordered[1:])):
        yield _binding('scenario cuts -> chronology', index, _cut_before(left, right))
    for name, anchor in (('pre_injection', injection), ('before_ready', ready)):
        snapshot = snapshots.get(name)
        if snapshot:
            yield _binding('snapshot -> anchor owner', name, bool(anchor and snapshot['scope'] == anchor['scope']))
    for name, snapshot in snapshots.items():
        if name not in ('pre_injection', 'final'):
            barrier = barriers[snapshot['barrier_id']]
            yield _binding('intermediate snapshot -> observation window', name, _cut_before(injection, barrier) and _cut_before(barrier, final))


def _cut_bound(barrier, stream):
    if not barrier or not stream:
        return False
    cutoff = barrier['stream_cutoffs'].get(stream['id'])
    return (cutoff is not None and stream['first_seq'] is not None and stream['last_seq'] is not None
            and stream['first_seq'] - 1 <= cutoff <= stream['last_seq'])


def _source_bindings(scenario, contract, observation, context):
    sources = sorted(observation['sources'], key=lambda source: source['slot'])
    source_ids = [source['source_id'] for source in sources]
    yield _binding('requested slots <-> source assignments', 'sources', _assignments_bound(sources, scenario))
    for source in sources:
        yield _binding('source -> admission', source['slot'], _admission_bound(source, scenario, contract, context))
    admissions = [event for event in context['relevant'] if event['milestone'] == 'injection_completed']
    for admission in admissions:
        yield _binding('admission -> source slots', admission['id'], _admitted_sources_bound(admission, sources, contract, context))
    pre = context['snapshots'].get('pre_injection')
    origins = ([pre['rows']] if pre else []) + [event['members'] for event in context['relevant']
                                               if event['milestone'] in ('row_persisted', 'injection_completed')]
    for index, members in enumerate(origins):
        yield _binding('row/revision -> unique source set', index, all(
            context['known_members'].get(_member_key(member)) == set(member['source_ids']) for member in members), 'identity_mismatch')
    if pre:
        injected_rows = {member['row_id'] for event in context['relevant'] if event['milestone'] in ('row_persisted', 'injection_completed')
                         for member in event['members'] if set(member['source_ids']) & set(source_ids)}
        yield _binding('injected identities != pre-existing rows', pre['id'], not any(
            member['row_id'] in injected_rows or set(member['source_ids']) & set(source_ids) for member in pre['rows']), 'identity_mismatch')


def _checkpoint_bindings(scenario, contract, observation, context):
    injection = context['barriers'].get(scenario['injection_barrier_id'])
    if scenario['readiness_barrier_id']:
        ready = context['barriers'].get(scenario['readiness_barrier_id'])
        yield _binding('readiness -> contract boundary/injection owner', scenario['readiness_barrier_id'], bool(
            ready and ready['name'] == contract['readiness_boundary'] and injection and ready['scope'] == injection['scope']), 'contract_matches')
    for name in ('state_proof', 'session_proof'):
        yield _binding('proof -> injection/events/role', name, _proof_bound(observation[name], scenario, context))
    epochs = {event['scope']['connection_epoch_id'] for event in context['relevant'] if _event_policy(event)[1]}
    if injection:
        epochs.add(injection['scope']['connection_epoch_id'])
    yield _binding('contract -> required epochs and checkpoints', 'contract', bool(injection and epochs)
                   and None not in epochs and bool(contract['contract_barrier_names']), 'contract_matches')
    observations = [item for item in observation['contract_observations'] if _scope_matches(item['scope'], scenario)]
    for index, observed in enumerate(observations):
        yield _binding('contract observation -> barrier/role/artifact', index, _contract_bound(observed, context), 'contract_matches')
    for epoch in sorted(epochs, key=lambda value: (value is not None, value or '')):
        for name in contract['contract_barrier_names']:
            anchor = context['named_barrier'](name, epoch)
            yield _binding('required epoch/checkpoint -> contract observation', (epoch, name), any(
                item['scope']['connection_epoch_id'] == epoch and anchor and item['barrier_id'] == anchor['id']
                and _contract_bound(item, context) for item in observations), 'contract_matches')


def _window_bindings(scenario, contract, observation, context):
    injection = context['barriers'].get(scenario['injection_barrier_id'])
    final = context['barriers'].get(scenario['final_barrier_id'])
    starts = [event for event in context['relevant'] if injection and _at_cut(event, injection) is False]
    ends = [event for event in context['relevant'] if final and _at_cut(event, final) is True]
    yield _binding('observation duration -> comparable evidence', 'window', scenario['observation_duration_ms'] == 0 or any(
        start['position']['clock_id'] is not None and start['position']['clock_id'] == end['position']['clock_id']
        and start['position']['time_ms'] is not None and end['position']['time_ms'] is not None
        and end['position']['time_ms'] - start['position']['time_ms'] >= scenario['observation_duration_ms']
        for start in starts for end in ends))
    ready = context['barriers'].get(scenario['readiness_barrier_id'])
    for event in context['relevant']:
        if scenario['readiness_barrier_id'] and event['milestone'] in ('delivery_offered', 'attempt_reserved') and ready and event['scope'] == ready['scope']:
            yield _binding('offer -> injection/readiness cuts', event['id'], bool(
                injection and _at_cut(event, injection) is not None and _at_cut(event, ready) is not None))
        if event['milestone'] == 'fault_observed':
            yield _binding('fault -> injection cut', event['id'], bool(injection and _at_cut(event, injection) is not None))
    held = context['snapshots'].get('fetch_result_held')
    if held:
        cut = context['barriers'][held['barrier_id']]
        for events, included in ((context['broker_results'], True), (context['recorded'], False)):
            for event in events:
                yield _binding('fetch result -> held cut order', event['id'], _at_cut(event, cut) is included)
    if scenario['health_class'] != 'degraded':
        yield _binding('Held notices -> contemporaneous queue and exclusions', 'notices', context['notice_complete'])


def _queue_bindings(scenario, contract, observation, context):
    if any(key not in context['injected_members'] for key in context['known_members']):
        yield _binding('backlog -> final queue', 'final', 'final' in context['snapshots'])
    complete, lost, mismatch = _reconcile_queue(context, context['persisted'], context['authorized_retirements'])
    yield _binding('complete rows -> survivor or authorized retirement', 'queue', complete)
    yield _binding('disappearing rows -> authorized retirement', 'queue', not lost, 'unauthorized_retirement')
    yield _binding('surviving rows -> current revision/source set', 'queue', not mismatch, 'identity_mismatch')
    yield _binding('current revision -> ordered persistence', 'queue', context['current_revisions_ordered'])


def _upstream_recovery_bound(event, source, scenario, context):
    injection = context['barriers'].get(scenario['injection_barrier_id'])
    final = context['barriers'].get(scenario['final_barrier_id'])
    return bool(event['milestone'] == 'source_recoverable' and source in event['payload']['source_ids']
                and event['payload']['storage'] == 'upstream' and event['payload']['checkpoint_id'] == scenario['final_barrier_id']
                and final and final['state'] == 'reached' and event['scope'] == final['scope']
                and _event_bound(event, context) and _at_cut(event, final) is True
                and injection and _at_cut(event, injection) is False)


def _attachment_bound(event, scenario, context):
    injection = context['barriers'].get(scenario['injection_barrier_id'])
    return bool(event['payload']['automatic'] and injection and event['scope'] == injection['scope']
                and _event_bound(event, context) and _at_cut(event, injection) is True
                and event['payload']['previous_session_id'] != event['scope']['session_id'])


def _recovery_bindings(scenario, contract, observation, context):
    if scenario['health_class'] == 'degraded':
        sources = {source['slot']: source['source_id'] for source in observation['sources']}
        for slot, disposition in enumerate(scenario['final_dispositions']):
            if disposition == 'recoverable':
                yield _binding('recoverable source -> final upstream checkpoint', slot, any(
                    _upstream_recovery_bound(event, sources.get(slot), scenario, context) for event in context['relevant']))
    if scenario['resume_requirement'] == 'attachment_recovery':
        recoveries = [event for event in context['relevant'] if event['milestone'] == 'attachment_recovered']
        if recoveries:
            yield _binding('claimed attachment recovery -> injection owner/cut', 'recovery', any(
                _attachment_bound(event, scenario, context) for event in recoveries))


def _claim_bindings(scenario, contract, observation, context):
    reservations, receipts = context['reservations'], context['receipts']
    known_members, broker_results = context['known_members'], context['broker_results']
    injection = context['barriers'].get(scenario['injection_barrier_id'])
    for event in context['events'].values():
        stream = context['streams'].get(event['position']['stream_id'])
        roles, claimed, origin = _event_policy(event)
        if not claimed or (not _scope_matches(event['scope'], scenario)
                and not (stream and _scope_matches(stream['scope'], scenario))):
            continue
        if event['milestone'] in ('fetch_result_produced', 'fetch_result_recorded') and not event['payload']['success']:
            continue
        scope, identity = event['scope'], event['id']
        required = ('run_id', 'route_id', 'host_session_id', 'session_id', 'connection_epoch_id')
        yield _binding('claim -> known owner', identity, all(scope[key] is not None for key in required))
        wrong_owner = any(scope[key] is not None and scope[key] != scenario[key] for key in required[:-1])
        yield _binding('claim -> scenario owner', identity, all(
            scope[key] is not None and scope[key] == scenario[key] for key in required[:-1]),
            *(['identity_mismatch'] if wrong_owner else []))
        yield _binding('claim -> observed epoch', identity, scope['connection_epoch_id'] is not None
                       and scope['connection_epoch_id'] in context['observed_by_epoch'],
                       *(['identity_mismatch'] if scope['connection_epoch_id'] is not None else []))
        if injection and scope['connection_epoch_id'] == injection['scope']['connection_epoch_id']:
            yield _binding('claim -> injection generation', identity,
                           scope['claim_generation'] == injection['scope']['claim_generation'],
                           *(['identity_mismatch'] if scope['claim_generation'] is not None else []))
        if contract['axes']['negotiation'] == 'v1' and event['milestone'] != 'fetch_requested':
            keys = ('attempt_id', 'group_id', 'token')
            yield _binding('negotiated claim -> identities', identity,
                           scope['claim_generation'] is not None and all(event[key] is not None for key in keys))
            matches = [reservation for reservation in reservations if _same_attempt(event, reservation)]
            related = [reservation for reservation in reservations if any(
                event[key] is not None and event[key] == reservation[key] for key in keys)]
            conflict = (related and not matches and all(event[key] is not None for key in keys)
                        and any(all(reservation[key] is not None for key in keys) for reservation in related))
            yield _binding('attempt/group/token equality and inequality', identity, not conflict, 'identity_mismatch')
            yield _binding('claim revision -> reserved revision', identity, bool(matches) and any(
                _members(event['members']) <= _members(reservation['members']) for reservation in matches),
                *(['identity_mismatch'] if matches else []))
            if event['milestone'] != 'attempt_reserved':
                yield _binding('claim -> prior matching reservation', identity, _reservation_bound(event, context, reservations))
        else:
            yield _binding('claim -> operation', identity, event['operation_id'] is not None)
        if contract['axes']['negotiation'] == 'none' and event['milestone'] in HOST_MILESTONES and event['transport'] != 'fetch':
            yield _binding('legacy host -> broker acceptance', identity, any(
                _same_operation(event, receipt) and receipt['origin'] == 'broker-ack' and context['source_bound'](receipt)
                and _member_identities(event['members']) <= _member_identities(receipt['members']) for receipt in receipts))
        yield _binding('claim -> prior row/revision/source', identity,
                       event['transport'] is not None and context['source_bound'](event))
        if event['milestone'] == 'attempt_terminal' and event['payload']['retired_members']:
            origin = 'retirement'
        if origin is not None:
            yield _binding('claim -> evidence origin', identity, event['origin'] == origin, 'identity_mismatch')
        if event['milestone'] in HOST_MILESTONES:
            yield _binding('host -> delivery occurrence/members', identity, event['delivery_id'] is not None and bool(event['members']))
        for member in event['members']:
            known = (member['revision']['kind'] != 'unknown' and bool(member['source_ids'])
                     and (contract['axes']['negotiation'] != 'v1' or member['revision']['kind'] == 'exact'))
            yield _binding('member -> known revision/source', (identity, member['row_id']), known)
            if known:
                yield _binding('member revision equality and inequality', (identity, member['row_id']),
                               set(member['source_ids']) == known_members.get(_member_key(member)), 'identity_mismatch')
        if event['transport'] == 'fetch':
            yield _binding('fetch -> operation', identity, event['operation_id'] is not None)
            if event['milestone'] == 'fetch_result_produced':
                yield _binding('broker result -> prior acknowledged request', identity, event in broker_results)
            if event['milestone'] == 'fetch_result_recorded':
                matches = [broker for broker in broker_results if _same_operation(event, broker)]
                yield _binding('host result -> broker operation', identity, bool(matches),
                               *(['identity_mismatch'] if broker_results else []))
                yield _binding('host result -> prior broker members', identity, any(
                    _member_identities(event['members']) <= _member_identities(broker['members'])
                    and _before(broker, event, context['events'], context['barriers'].values()) is True for broker in matches))
                if contract['axes']['fetch_policy'] == 'receipt' and event['payload']['trailer']['state'] == 'complete' and matches:
                    yield _binding('host trailer -> broker/reservation token and members', identity, any(
                        _trailer_matches(event, broker, reservations, context['injected_members']) for broker in matches), 'identity_mismatch')
    for reservation in reservations:
        yield _binding('reservation -> identities/members', reservation['id'], all(
            reservation[key] is not None for key in ('attempt_id', 'group_id', 'token', 'transport')) and bool(reservation['members']))
        yield _binding('reservation -> known membership', reservation['id'], all(
            member['revision']['kind'] != 'unknown' and bool(member['source_ids']) for member in reservation['members']))
        yield _binding('reservation -> exact source binding', reservation['id'], all(
            set(member['source_ids']) == known_members.get(_member_key(member)) for member in reservation['members']), 'identity_mismatch')


def _retirement_facts(scenario, contract, context):
    facts = []
    reservations, receipts = context['reservations'], context['receipts']
    known_members = context['known_members']
    for terminal in context['relevant']:
        if terminal['milestone'] != 'attempt_terminal' or not terminal['payload']['retired_members']:
            continue
        members = terminal['payload']['retired_members']
        retired = _member_identities(members)
        matches = _reservation_matches(terminal, reservations)
        bound = (terminal['origin'] == 'retirement' and context['source_bound'](terminal)
                 and _known_members(members, known_members, contract['axes']['negotiation'] == 'v1')
                 and _current_members(terminal, members, context['persisted'], context['events'], context['barriers'].values())
                 and retired <= _member_identities(terminal['members']))
        if contract['axes']['negotiation'] == 'v1':
            bound = bound and any(retired <= _member_identities(reservation['members']) for reservation in matches)
        if scenario['transport'] == 'fetch' and contract['axes']['fetch_policy'] == 'consume':
            evidence = [broker for broker in context['broker_results'] if _same_operation(terminal, broker)
                        and retired <= _member_identities(broker['members'])]
        elif contract['axes']['negotiation'] == 'v1':
            evidence = [receipt for receipt in receipts if _same_attempt(terminal, receipt) and receipt['origin'] == 'broker-ack'
                        and _reservation_matches(receipt, reservations) and context['source_bound'](receipt)
                        and retired <= _member_identities(receipt['members'])]
        else:
            evidence = [receipt for receipt in receipts if _same_operation(terminal, receipt) and receipt['origin'] == 'broker-ack'
                        and context['source_bound'](receipt) and retired <= _member_identities(receipt['members'])]
        facts.append(dict(terminal=terminal, bound=bound,
                          order=[_before(item, terminal, context['events'], context['barriers'].values()) for item in evidence]))
    return facts


def _retirement_bindings(scenario, contract, observation, context):
    for item in context['retirements']:
        terminal, ordered = item['terminal'], item['order']
        yield _binding('retirement -> authorized current members', terminal['id'], item['bound'], 'unauthorized_retirement')
        yield _binding('retirement -> source equality', terminal['id'], not any(
            _member_key(member) in context['known_members']
            and set(member['source_ids']) != context['known_members'][_member_key(member)]
            for member in terminal['payload']['retired_members']), 'unauthorized_retirement', 'identity_mismatch')
        yield _binding('retirement -> prior contract evidence', terminal['id'], any(order is True for order in ordered),
                       *(['premature_retirement'] if ordered and all(order is False for order in ordered) else []))


def _validate_bindings(scenario, contract, observation, context):
    """Enumerate and positively validate all required cross-bindings once."""
    # Required-binding manifest (including both directions and inequality):
    # identity/causality; role/stream/artifact; scenario cuts/snapshots/order;
    # source <-> admission; member/revision/source; event/reservation/token;
    # contract epochs/checkpoints; state/session proofs; observation window;
    # retirement/authorization/order; complete queue reconciliation; recovery.
    manifest = (
        _integrity_bindings, _coverage_bindings, _source_bindings, _checkpoint_bindings,
        _window_bindings, _claim_bindings, _retirement_bindings, _queue_bindings, _recovery_bindings,
    )
    failures = set()
    for enumerate_bindings in manifest:
        for relation, subject, satisfied, consequences in enumerate_bindings(scenario, contract, observation, context):
            if satisfied is not True:
                failures.update(consequences)
    return failures


def derive(scenario, contract, observation):
    """Compute report projections from canonical events, never legacy counts."""
    _validate_inputs(scenario, contract, observation)
    if observation['setup_errors']:
        return {}
    context = _context(scenario, contract, observation)
    events, relevant = context['events'], context['relevant']
    injection = context['barriers'].get(scenario['injection_barrier_id'])
    final_barrier = context['barriers'].get(scenario['final_barrier_id'])
    known_members = {}
    pre = context['snapshots'].get('pre_injection')
    if pre:
        for member in pre['rows']:
            known_members[_member_key(member)] = set(member['source_ids'])
    for event in relevant:
        if event['milestone'] in ('row_persisted', 'injection_completed'):
            for member in event['members']:
                key, identities = _member_key(member), set(member['source_ids'])
                known_members.setdefault(key, identities)
    context['known_members'] = known_members
    def source_bound(event):
        if not _known_members(event['members'], known_members, contract['axes']['negotiation'] == 'v1'):
            return False
        for member in event['members']:
            identity = (_member_key(member), frozenset(member['source_ids']))
            if pre and identity in _member_identities(pre['rows']) and _at_cut(event, context['barriers'][pre['barrier_id']]) is False:
                continue
            if not any(item['milestone'] in ('row_persisted', 'injection_completed')
                       and identity in _member_identities(item['members'])
                       and _before(item, event, events, context['barriers'].values()) is True for item in relevant):
                return False
        return True
    context['source_bound'] = source_bound
    sources = sorted(observation['sources'], key=lambda source: source['slot'])
    source_ids = [source['source_id'] for source in sources]
    injected_members = {key: identities for key, identities in known_members.items() if identities and identities <= set(source_ids)}
    context['injected_members'] = injected_members
    admissions = [event for event in relevant if event['milestone'] == 'injection_completed']
    is_injected = (_assignments_bound(sources, scenario)
                   and all(_admitted_sources_bound(event, sources, contract, context) for event in admissions)
                   and all(_admission_bound(source, scenario, contract, context)
                           and events[source['admission_event_id']]['payload']['accepted'] for source in sources))
    def rows(name):
        snapshot = context['snapshots'].get(name)
        if snapshot is None:
            return None
        injected_rows = {member['row_id'] for event in relevant if event['milestone'] in ('row_persisted', 'injection_completed')
                         for member in event['members'] if set(member['source_ids']) & set(source_ids)}
        return len({member['row_id'] for member in snapshot['rows'] if member['row_id'] in injected_rows or set(member['source_ids']) & set(source_ids)})
    reservations = [event for event in relevant if event['milestone'] == 'attempt_reserved']
    confirmed = [event for event in relevant if event['milestone'] == 'attempt_terminal' and event['payload']['outcome'] == 'confirmed']
    receipts = [event for event in relevant if event['milestone'] == 'receipt_accepted']
    attempts = []
    for event in relevant:
        if event['milestone'] not in ('attempt_reserved', 'receipt_accepted', 'receipt_rejected', 'attempt_terminal'):
            continue
        group = next((group for group in attempts if _same_attempt(group['events'][0], event)), None)
        if group is None:
            group = dict(scope=event['scope'], attempt_id=event['attempt_id'], group_id=event['group_id'], token=event['token'], events=[])
            attempts.append(group)
        group['events'].append(event)
    selected = contract['milestones']['fetch' if scenario['transport'] == 'fetch' else 'live']
    target_scope = injection['scope'] if injection else None
    delivery_epochs = {item['scope']['connection_epoch_id'] for item in observation['contract_observations'] if _scope_matches(item['scope'], scenario)}
    def eligible(event, host=True):
        scope = event['scope']
        if (tuple(scope[key] for key in SCOPE), event['milestone'], event['delivery_id']) in context['invalid_deliveries']:
            return False
        if not injection or _at_cut(event, injection) is not False:
            return False
        if not final_barrier or _at_cut(event, final_barrier) is not True:
            return False
        if event not in relevant or (host and event['origin'] != 'host-evidence') or not _event_bound(event, context):
            return False
        if any(scope[key] is None for key in ('route_id', 'host_session_id', 'session_id', 'connection_epoch_id')):
            return False
        if scope['connection_epoch_id'] not in delivery_epochs:
            return False
        if not event['members'] or any(not member['source_ids'] for member in event['members']):
            return False
        if target_scope and scope['connection_epoch_id'] == target_scope['connection_epoch_id'] and scope['claim_generation'] != target_scope['claim_generation']:
            return False
        if contract['axes']['negotiation'] == 'v1':
            if not _reservation_bound(event, context, reservations):
                return False
        elif event['operation_id'] is None:
            return False
        elif host and scenario['transport'] != 'fetch' and not any(_same_operation(event, receipt)
                and receipt['origin'] == 'broker-ack' and source_bound(receipt)
                and _member_identities(event['members']) <= _member_identities(receipt['members']) for receipt in receipts):
            return False
        if not source_bound(event):
            return False
        return event['delivery_id'] is not None and event['transport'] == scenario['transport'] and all(member['revision']['kind'] != 'unknown' for member in event['members'])
    host_events = [event for event in relevant if event['milestone'] == selected and eligible(event)]
    requests = [event for event in relevant if event['milestone'] == 'fetch_requested' and event['operation_id'] is not None
                and event['transport'] == 'fetch' and event['payload']['ack'] and source_bound(event)]
    broker_results = [event for event in relevant if event['milestone'] == 'fetch_result_produced' and event['payload']['success']
                      and eligible(event, host=False) and any(_same_operation(event, request)
                          and _member_identities(event['members']) <= _member_identities(request['members'])
                          and _before(request, event, events, context['barriers'].values()) is True for request in requests)]
    recorded = [event for event in host_events if event['milestone'] == 'fetch_result_recorded' and event['payload']['success']
                and any(_same_operation(event, broker) and _member_identities(event['members']) <= _member_identities(broker['members'])
                        and _before(broker, event, events, context['barriers'].values()) is True for broker in broker_results)]
    if scenario['transport'] == 'fetch':
        host_events = recorded
    def occurrence_counts(items):
        occurrences = {source: set() for source in source_ids}
        for event in items:
            for source in _source_set(event['members']):
                occurrences.setdefault(source, set()).add((event['delivery_id'], event['milestone']))
        return {source: len(identities) for source, identities in occurrences.items()}
    received = occurrence_counts(host_events)
    returned = recorded if contract['milestones']['fetch'] else broker_results
    fetch_counts = occurrence_counts(returned)
    early = []
    ready = context['barriers'].get(scenario['readiness_barrier_id'])
    if scenario['readiness_barrier_id'] and ready and injection and ready['state'] == 'reached':
        for event in relevant:
            if event['milestone'] not in ('delivery_offered', 'attempt_reserved') or event['scope'] != ready['scope']:
                continue
            if _at_cut(event, ready) is True and _at_cut(event, injection) is False:
                description = event['payload'].get('description', 'broker reserved a delivery attempt')
                if description not in early:
                    early.append(description)
    broker_streams = [stream for stream in context['streams'].values() if stream['role'] == 'broker' and _scope_matches(stream['scope'], scenario)]
    final_barrier = context['barriers'].get(scenario['final_barrier_id'])
    notice_complete = bool(broker_streams) and bool(final_barrier) and final_barrier['state'] == 'reached' and all(
        stream['state'] == 'complete' and stream['first_seq'] is not None and stream['last_seq'] is not None
        and stream['through_barrier_id'] == scenario['final_barrier_id']
        and final_barrier['stream_cutoffs'].get(stream['id']) == stream['last_seq'] for stream in broker_streams)
    route_lines = 0 if notice_complete else None
    is_false_held = False
    for notice in relevant:
        if notice['milestone'] != 'notice_emitted':
            continue
        if not notice['collection_complete']:
            route_lines = None
        if route_lines is not None:
            route_lines += len(notice['payload']['route_diagnostic_lines'])
        if notice['payload']['kind'] != 'held':
            continue
        snapshot = context['snapshots'].get('notice/' + notice['id'])
        if (snapshot is None or snapshot['id'] != notice['payload']['queue_snapshot_id'] or snapshot['scope'] != notice['scope']
                or notice['payload']['held_count'] is None or (snapshot['rows'] and not _known_members(snapshot['rows'], known_members))):
            notice_complete = False
            continue
        notice_barrier = context['barriers'][snapshot['barrier_id']]
        if (notice_barrier['state'] != 'reached' or not notice_barrier['collection_complete']
                or notice_barrier['stream_cutoffs'].get(notice['position']['stream_id']) != notice['position']['seq']):
            notice_complete = False
            continue
        excluded = set()
        for reservation in reservations:
            if not _same_owner(reservation, notice):
                continue
            order = _before(reservation, notice, events, context['barriers'].values())
            if order is None:
                notice_complete = False
            if order is not True:
                continue
            terminals = [event for event in relevant if event['milestone'] == 'attempt_terminal' and _same_attempt(event, reservation)]
            is_open = True
            for terminal in terminals:
                order = _before(terminal, notice, events, context['barriers'].values())
                if order is None:
                    notice_complete = False
                if order and (contract['axes']['negotiation'] == 'v1' or terminal['payload']['outcome'] in ('confirmed', 'failed', 'released')):
                    is_open = False
            if is_open:
                excluded |= _members(reservation['members'])
        for receipt in receipts:
            if not _same_owner(receipt, notice):
                continue
            order = _before(receipt, notice, events, context['barriers'].values())
            if order is None:
                notice_complete = False
            if order and (contract['axes']['negotiation'] == 'none' or any(_same_attempt(receipt, reservation) and _members(receipt['members']) <= _members(reservation['members']) for reservation in reservations)):
                excluded |= _members(receipt['members'])
        visible = _members(snapshot['rows']) - excluded
        if notice['payload']['held_count'] > len(visible):
            is_false_held = True
    context.update(notice_complete=notice_complete, recorded=recorded)
    if scenario['transport'] == 'fetch':
        relevant_reservations = [event for event in reservations if event['transport'] == 'fetch']
        relevant_confirmed = [event for event in confirmed if event['transport'] == 'fetch']
    else:
        relevant_reservations, relevant_confirmed = reservations, confirmed
    held_snapshot = context['snapshots'].get('fetch_result_held')
    retained_members = bool(held_snapshot) and bool(relevant_reservations) and all(
        _member_identities(event['members']) <= _member_identities(held_snapshot['rows']) for event in relevant_reservations)
    if held_snapshot:
        held_barrier = context['barriers'][held_snapshot['barrier_id']]
        if any(_at_cut(event, held_barrier) is not True for event in broker_results) or any(
                _at_cut(event, held_barrier) is not False for event in recorded):
            retained_members = False
    result = dict(fetch_members_retained=retained_members, injected=bool(is_injected), source_ids=source_ids, received=received, attempts=attempts,
                  attempted_member_count=sum(len(event['members']) for event in relevant_reservations),
                  retirement_count=sum(len(event['payload']['retired_members']) for event in relevant_confirmed),
                  rows_before_ready=rows('before_ready'), rows_while_fetch_result_held=rows('fetch_result_held'), rows_final=rows('final'),
                  fetch_tool_result=bool(recorded), successful_broker_fetch_result=bool(broker_results),
                  fetch_token=any(_trailer_valid(event['payload']['trailer']) for event in broker_results),
                  fetch_trailer_complete=any(_trailer_matches(host, broker, reservations, injected_members)
                                            for host in recorded for broker in broker_results),
                  fetch_sources_returned=fetch_counts, fetch_source_occurrences=sum(fetch_counts.values()),
                  offered_before_ready=early, route_line_count=route_lines, false_held=is_false_held,
                  no_false_held=notice_complete and not is_false_held)
    result.update(_policy_projections(scenario, contract, observation, context, result, reservations, receipts, host_events, broker_results))
    failures = _validate_bindings(scenario, contract, observation, context)
    result.update(collection_complete='collection_complete' not in failures,
                  identity_mismatch='identity_mismatch' in failures,
                  unauthorized_retirement='unauthorized_retirement' in failures,
                  premature_retirement='premature_retirement' in failures)
    result['contract_matches'] = result['contract_matches'] and 'contract_matches' not in failures
    return deepcopy(result)


def _policy_projections(scenario, contract, observation, context, projections, reservations, receipts, host_events, broker_results):
    events, relevant = context['events'], context['relevant']
    injection = context['barriers'].get(scenario['injection_barrier_id'])
    contract_matches = True
    def axes_equal(left, right):
        return left is not None and len(left['accepted_modes']) == len(set(left['accepted_modes'])) and all(left[key] == right[key] for key in right if key != 'accepted_modes') and set(left['accepted_modes']) == set(right['accepted_modes'])
    observed_by_epoch = {}
    for observed in observation['contract_observations']:
        if not _scope_matches(observed['scope'], scenario):
            continue
        barrier = context['barriers'].get(observed['barrier_id'])
        if not axes_equal(observed['axes'], contract['axes']) or (barrier and observed['scope'] != barrier['scope']):
            contract_matches = False
        epoch = observed['scope']['connection_epoch_id']
        if observed['axes'] is not None:
            modes = set(observed['axes']['accepted_modes'])
            if epoch in observed_by_epoch and observed_by_epoch[epoch] != modes:
                contract_matches = False
            observed_by_epoch[epoch] = modes
    state = observation['state_proof']
    session = observation['session_proof']
    state_valid = _proof_bound(state, scenario, context) and state['proven'] and state['state'] == scenario['state'] and any(events[identity]['milestone'] == 'state_observed' and events[identity]['payload']['state'] == scenario['state'] for identity in state['event_ids'])
    session_valid = _proof_bound(session, scenario, context) and session['proven'] and session['posture'] == scenario['session'] and session['freshness'] == scenario['freshness']
    requested_sources = set(projections['source_ids'])
    persisted = [event for event in relevant if event['milestone'] in ('row_persisted', 'injection_completed')]
    context.update(reservations=reservations, receipts=receipts, broker_results=broker_results,
                   observed_by_epoch=observed_by_epoch, persisted=persisted)
    retirements = _retirement_facts(scenario, contract, context)
    context['retirements'] = retirements
    context['authorized_retirements'] = [item['terminal'] for item in retirements
                                         if item['bound'] and any(order is True for order in item['order'])]
    authorized_sources = {source for terminal in context['authorized_retirements']
                          for source in _source_set(terminal['payload']['retired_members'])}
    fault_events = [event for event in relevant if event['milestone'] == 'fault_observed']
    faults = [event for event in fault_events if injection and _at_cut(event, injection) is False]
    observed_faults = {event['payload']['kind'] for event in faults}
    expected_fault = scenario['health_class']
    fault_valid = not observed_faults if expected_fault == 'healthy' else observed_faults == {expected_fault}
    disposition_valid = True
    final = context['snapshots'].get('final')
    final_sources = set()
    context['current_revisions_ordered'] = True
    if final:
        current = {}
        for event in persisted:
            for member in event['members']:
                previous = current.get(member['row_id'])
                if previous is None or _before(previous[1], event, events, context['barriers'].values()) is True:
                    current[member['row_id']] = (member, event)
                elif previous[0]['revision'] != member['revision'] and _before(event, previous[1], events, context['barriers'].values()) is None:
                    context['current_revisions_ordered'] = False
        final_sources = {source for member in final['rows'] if member['row_id'] in current
                         and member == current[member['row_id']][0] for source in member['source_ids']}
    selected_counts = projections['fetch_sources_returned'] if scenario['transport'] == 'fetch' else projections['received']
    for source, disposition in zip(projections['source_ids'], scenario['final_dispositions']):
        if disposition == 'delivered':
            if selected_counts.get(source, 0) < 1 or source not in authorized_sources:
                disposition_valid = False
        elif expected_fault == 'degraded':
            if not any(_upstream_recovery_bound(event, source, scenario, context) for event in relevant):
                disposition_valid = False
        elif source not in final_sources:
            disposition_valid = False
    if len(requested_sources) != scenario['count']:
        disposition_valid = False
    if expected_fault != 'healthy':
        # A fault permits only subsequent repeat occurrences.
        for source in requested_sources:
            occurrences = {}
            for event in host_events:
                if source in _source_set(event['members']):
                    occurrences.setdefault(event['delivery_id'], event)
            before_fault = [event for event in occurrences.values() if not any(_before(fault, event, events, context['barriers'].values()) is True for fault in faults)]
            if len(before_fault) > 1:
                disposition_valid = False
    recoveries = [event for event in relevant if event['milestone'] == 'attachment_recovered']
    recovered = [event for event in recoveries if _attachment_bound(event, scenario, context)]
    explicit = [event for event in relevant if event['milestone'] == 'explicit_attach']
    recovery_valid = any(not any(_same_owner(attach, event) and _before(attach, event, events, context['barriers'].values()) is not False for attach in explicit) for event in recovered)
    return dict(contract_matches=contract_matches, state_proven=bool(state_valid),
                session_proven=bool(session_valid), fault_matches=fault_valid, disposition_preserved=disposition_valid,
                attachment_recovered=recovery_valid)


def evaluate(scenario, contract, observation):
    """Return exactly status, reasons, and not_evaluated, without mutating inputs."""
    try:
        _validate_inputs(scenario, contract, observation)
    except InvalidInput as error:
        return dict(status='FAIL', reasons=['invalid verdict input: ' + str(error)], not_evaluated=[])
    if observation['setup_errors']:
        return dict(status='FAIL', reasons=observation['setup_errors'][:1], not_evaluated=delivery_assertions(scenario, contract))
    projections = derive(scenario, contract, observation)
    reasons = list(observation['run_errors'])
    def require(condition, message):
        if not condition:
            reasons.append(message)
    count = scenario['count']
    is_healthy = scenario['health_class'] == 'healthy'
    is_fetch = scenario['transport'] == 'fetch'
    events = {event['id']: event for event in observation['events']}
    relevant = [event for event in events.values() if _scope_matches(event['scope'], scenario)]
    reservations = [event for event in relevant if event['milestone'] == 'attempt_reserved']
    confirmed = [event for event in relevant if event['milestone'] == 'attempt_terminal' and event['payload']['outcome'] == 'confirmed']
    require(projections['injected'], 'injection was not completed')
    if is_healthy:
        require(projections['rows_final'] == 0, 'durable rows remain')
    if scenario['readiness_barrier_id'] and projections['offered_before_ready']:
        reasons.append('delivery offered before host initialized (' + '; '.join(projections['offered_before_ready']) + ')')
    if scenario['health_class'] != 'degraded':
        require(projections['no_false_held'], 'no false Held assertion failed or missing')
    require(projections['route_line_count'] == ROUTE_LINE_LIMIT, 'route line count assertion failed or missing')
    def exactly_once(counts):
        return set(counts) == set(projections['source_ids']) and len(counts) == count and all(value == 1 for value in counts.values())
    def timely(reserved, terminals):
        timing = contract['timing']['fetch' if is_fetch else 'live']
        basis = terminals if timing['basis'] == 'terminal_confirmation' else [event for event in relevant if event['milestone'] == 'receipt_accepted' and (not is_fetch or event['transport'] == 'fetch')]
        if is_healthy and {event['token'] for event in reserved if event['token']} != {event['token'] for event in terminals if event['token']}:
            return False
        if is_healthy and (not reserved or not terminals):
            return False
        if any(not any(_same_attempt(terminal, reservation) for reservation in reserved) for terminal in terminals):
            return False
        if any(not any(_same_attempt(event, reservation) for reservation in reserved) for event in basis):
            return False
        if timing['basis'] == 'broker_receipt' and any(not any(_same_attempt(terminal, receipt) for receipt in basis) for terminal in terminals):
            return False
        return all(event['payload']['elapsed_ms'] is not None and 0 <= event['payload']['elapsed_ms'] < timing['limit_ms'] for event in basis) and (timing['basis'] != 'broker_receipt' or not terminals or bool(basis))
    live_offers = [event for event in relevant if event['milestone'] in ('attempt_reserved', 'delivery_offered') and event['transport'] in ('channel', 'inbox')]
    if is_fetch:
        require(not live_offers, 'live offer in fetch-only cell')
        if contract['axes']['fetch_policy'] == 'receipt':
            if is_healthy:
                require(projections['rows_while_fetch_result_held'] == count and projections['fetch_members_retained'], 'rows retired before host tool-result receipt (baseline consume-on-fetch)')
                require(projections['fetch_tool_result'], 'no successful fetch tool-result record')
            require(timely([event for event in reservations if event['transport'] == 'fetch'], [event for event in confirmed if event['transport'] == 'fetch']), _window(contract, True)[1])
            if is_healthy:
                require(projections['retirement_count'] == count, 'fetch group retirement count differs from injected sources')
            if is_healthy:
                require(projections['fetch_trailer_complete'], 'fetch tool result lacks the complete matching receipt trailer')
                require(projections['fetch_token'], 'fetch result has no broker receipt token')
        else:
            require(projections['successful_broker_fetch_result'], 'no successful broker fetch result')
            if contract['milestones']['fetch']:
                require(projections['fetch_tool_result'], 'no successful fetch tool-result record')
            if is_healthy:
                require(projections['retirement_count'] == count, 'fetch group retirement count differs from injected sources')
        if is_healthy:
            require(exactly_once(projections['fetch_sources_returned']), 'fetch did not return each injected source exactly once')
    elif contract['axes']['negotiation'] == 'v1':
        require(bool(reservations), 'no negotiated attempt observed')
        require(not any(event['transport'] != scenario['transport'] and (is_healthy or not contract['axes']['live_eligibility'].get(event['transport'], False)) for event in reservations), 'fallback/wrong transport attempted')
        if is_healthy:
            source_counts = Counter(source for event in reservations for member in event['members'] for source in member['source_ids'])
            require(projections['attempted_member_count'] == count and exactly_once(source_counts), 'source attempted more or less than once')
        require(timely(reservations, confirmed), _window(contract, False)[1])
        if is_healthy:
            require(projections['retirement_count'] == count, 'retirement count differs from injected source count')
            require(exactly_once(projections['received']), 'host did not receive each source exactly once')
    else:
        hosts = [event for event in relevant if event['milestone'] == contract['milestones']['live'] and event['origin'] == 'host-evidence']
        acknowledgements = [event for event in relevant if event['milestone'] == 'receipt_accepted' and event['origin'] == 'broker-ack']
        require(any(host['members'] and _same_operation(host, receipt)
                    and _member_identities(host['members']) <= _member_identities(receipt['members'])
                    for host in hosts for receipt in acknowledgements), 'no contract live acceptance observed')
        require(not any(event['transport'] != scenario['transport'] and (is_healthy or not contract['axes']['live_eligibility'][event['transport']]) for event in live_offers), 'fallback/wrong transport attempted')
        if is_healthy:
            require(exactly_once(projections['received']), 'host did not receive each source exactly once')
            require(projections['retirement_count'] == count, 'retirement count differs from injected source count')
    checks = [projections['contract_matches'], projections['state_proven'], projections['session_proven'],
              not projections['identity_mismatch'], not projections['unauthorized_retirement'], not projections['premature_retirement'],
              projections['fault_matches'], projections['disposition_preserved'], projections['attachment_recovered'], projections['collection_complete']]
    for (label, message), condition in zip(ADDITIONAL, checks):
        if label == 'automatic attachment recovered' and scenario['resume_requirement'] != 'attachment_recovery':
            continue
        require(condition, message)
    status = 'FAIL' if observation['run_errors'] or not projections['collection_complete'] else (
        'COLLECTED' if scenario['evaluation_mode'] == 'collect_only' else ('FAIL' if reasons else 'PASS'))
    return dict(status=status, reasons=reasons, not_evaluated=[])
