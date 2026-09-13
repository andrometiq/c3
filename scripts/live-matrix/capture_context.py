"""Checked raw observer tables for the production capture assembler.

This is an observer input format, not the verdict schema. See the replay corpus
README for its versioned grammar and the limits of today's live instrumentation.
"""
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
import json
import re

from host import checked_read, read_problem
from matrix import Cell

# Declared public symbol for descriptor diagnostics, never a delivery witness.
CAPTURE_ARTIFACT = 'driver-observations'
SCOPE = ('run_id', 'route_id', 'host_session_id', 'session_id', 'connection_epoch_id', 'claim_generation')
ROLES = {'injection', 'broker', 'host', 'transport', 'queue', 'contract', 'state', 'recovery'}
TABLES = {'ownership', 'rows', 'attempts', 'receipts', 'queue-samples', 'contracts', 'driver',
          'injection', 'broker', 'adapter', 'host', 'held'}
REQUIRED = TABLES - {'held'}
ID = lambda x: type(x) is str and bool(x)
INT = lambda x: type(x) is int and x >= 0
IDS = lambda x: type(x) is list and all(ID(v) for v in x)
REV = lambda x: type(x) is str and re.fullmatch('[0-9a-f]{64}', x) is not None
PAIR = lambda x: type(x) is dict and set(x) == {'row_id', 'revision'} and ID(x['row_id']) and REV(x['revision'])
PAIRS = lambda x: type(x) is list and all(PAIR(v) for v in x)
ENUM = lambda *values: lambda x: type(x) is str and x in values
BOOL = lambda x: type(x) is bool
SCHEMAS = {
    'rows': dict(row_id=ID, revision=REV, source_ids=IDS),
    'attempts': dict(artifact_id=ID, line=INT, block=INT, attempt_id=ID, group_id=ID, rows=PAIRS, retired_rows=PAIRS),
    'receipts': dict(action=ENUM('accepted', 'rejected'), token=ID, attempt_id=ID, group_id=ID,
                     transport=ENUM('channel', 'inbox', 'fetch'), operation_id=lambda x: x is None or ID(x),
                     rows=PAIRS, elapsed_ms=INT),
    'queue-samples': dict(barrier_id=ID, snapshot_id=ID, read_state=ENUM('complete', 'unavailable', 'malformed'), rows=PAIRS),
    'contracts': dict(barrier_id=ID, negotiation=ENUM('none', 'v1'), channel=BOOL, inbox=BOOL,
                      receipt_type=ENUM('none', 'transcript', 'queue_acceptance', 'input_echo'),
                      fetch_policy=ENUM('receipt', 'consume', 'unavailable'),
                      accepted_modes=lambda x: type(x) is list and all(v in ('channel', 'inbox', 'fetch_receipt') for v in x)),
    'injection': dict(message_ids=lambda x: type(x) is list and all(type(v) is int for v in x),
                      kind=ENUM('text', 'voice', 'photo'), broker_lines=lambda x: type(x) is list and all(INT(v) and v > 0 for v in x)),
}
DRIVER = {
    'state_sample': dict(observed_state=ENUM('idle', 'foreground', 'background', 'startup', 'reconnect')),
    'session_sample': dict(operation=ENUM('resume', 'new'), host_session_ref=ID, transcript_records=INT),
    'cut': dict(barrier_id=ID, cuts=lambda x: type(x) is list and all(
        type(v) is dict and set(v) == {'stream_id', 'seq'} and ID(v['stream_id']) and INT(v['seq']) for v in x)),
    'window_end': dict(observed_state=ENUM('idle', 'foreground', 'background', 'startup', 'reconnect')),
    'fetch_request': dict(frame=lambda x: type(x) is dict, host_record_id=ID, operation_id=ID),
    'fetch_release': dict(artifact_id=ID, request_id=lambda x: type(x) in (int, str), operation_id=ID),
}


def valid(record, schema):
    return type(record) is dict and set(record) == set(schema) and all(check(record[k]) for k, check in schema.items())


@dataclass
class CaptureContext:
    descriptor: dict
    reads: dict
    inventory: list
    diagnostics: list = field(default_factory=list)
    decisions: list = field(default_factory=list)

    def problem(self, artifact, locator, code, *, malformed=False):
        detail = f'{locator}: {code}'
        item = dict(artifact_id=artifact, locator=locator, code=code)
        if item not in self.diagnostics:
            self.diagnostics.append(item)
        if artifact in self.reads:
            if malformed:
                read_problem(self.reads[artifact], 'malformed', detail)
            elif detail not in self.reads[artifact]['detail']:
                self.reads[artifact]['detail'] = '; '.join(filter(None, (self.reads[artifact]['detail'], detail)))

    def entries(self, table):
        return [entry for entry in self.inventory if entry['table'] == table]

    def records(self, table):
        for entry in self.entries(table):
            read = self.reads[entry['artifact_id']]
            for line, record in zip(read['record_lines'], read['records']):
                yield entry['artifact_id'], line, record


def contained_file(root, name):
    try:
        return (root / name).resolve().is_relative_to(root.resolve())
    except (OSError, RuntimeError, ValueError):
        return False


def _load_context(root, *, live=False, reader=None):
    read_artifact = reader or checked_read
    descriptor_read = read_artifact(root / 'capture.json', format='json')
    descriptor = next(iter(descriptor_read['records']), {})
    context = CaptureContext(descriptor, {}, [])
    descriptor_schema = dict(schema_version=lambda v: type(v) is int and v == 1,
        context_version=lambda v: type(v) is int and v == 1, cell=lambda v: valid(v, dict(
            transport=ENUM('channel', 'inbox', 'fetch'), state=ENUM('idle', 'foreground', 'background', 'startup', 'reconnect'),
            session=ENUM('fresh', 'resumed'), kind=ENUM('text', 'voice', 'photo'), burst=ENUM('single', 'double'))),
        evidence=lambda v: valid(v, {key: ID for key in SCOPE[:4]}),
        provenance=lambda v: valid(v, dict(kind=ENUM('raw_replay', 'live_capture'), capture_id=ID,
            broker_build=ID, adapter_build=ID, host_version=ID, platform=ID)), artifacts=lambda v: type(v) is list)
    if descriptor_read['state'] != 'complete' or not valid(descriptor, descriptor_schema):
        context.problem(CAPTURE_ARTIFACT, 'capture.json', 'invalid or unavailable capture descriptor')
        context.descriptor = {}
        return context
    if live and descriptor['provenance']['kind'] == 'raw_replay':
        context.problem(CAPTURE_ARTIFACT, 'capture.json', 'synthetic context forbidden for live collection')
        context.descriptor = {}
        return context
    entry_schema = dict(artifact_id=ID, file=ID, table=ENUM(*TABLES), format=ENUM('json', 'jsonl', 'log'),
        seal=lambda v: valid(v, dict(bytes=INT, sha256=lambda x: type(x) is str and re.fullmatch('[0-9a-f]{64}', x) is not None,
                                    lines=INT, read_state=ENUM('complete', 'missing', 'unreadable', 'malformed', 'partial', 'truncated'))))
    files = set()
    for index, entry in enumerate(descriptor['artifacts']):
        if (not valid(entry, entry_schema) or entry['artifact_id'] in context.reads or entry['file'] in files
                or not contained_file(root, entry['file'])):
            context.problem(CAPTURE_ARTIFACT, f'artifacts/{index}', 'invalid artifact inventory entry')
            continue
        files.add(entry['file'])
        context.inventory.append(entry)
        identity = entry['artifact_id']
        read = read_artifact(root / entry['file'], format=entry['format'])
        context.reads[identity] = read
        seal = entry['seal']
        if read['bytes'] is not None and read['bytes'] < seal['bytes']:
            read_problem(read, 'truncated', 'extent: artifact shorter than captured seal')
        if read['bytes'] is not None and (read['bytes'] != seal['bytes'] or read['sha256'] != seal['sha256'] or read['lines'] != seal['lines']):
            read_problem(read, 'truncated', 'extent: captured seal mismatch')
        if seal['read_state'] != 'complete':
            read_problem(read, seal['read_state'], 'seal: non-complete captured read')
        for problem in read['problems']:
            context.problem(identity, 'read', problem)
    for table in sorted(REQUIRED | ({'held'} if descriptor['cell']['transport'] == 'fetch' else set())):
        entries = context.entries(table)
        if len(entries) != 1:
            context.problem(CAPTURE_ARTIFACT, 'inventory', 'missing or ambiguous required table: ' + table)
    return context


def load_capture(root, *, live=False):
    """Load final artifacts, assemble using collect.py, and pin the expected inputs."""
    from collect import capture_observation, build_verdict_inputs, count_receives, notice_evidence, TOKEN, strings
    context = _load_context(Path(root), live=live)
    cell = Cell(**context.descriptor.get('cell', dict(transport='channel', state='idle', session='resumed', kind='text', burst='single')))
    evidence = deepcopy(context.descriptor.get('evidence', {}))
    def read(table):
        entries = context.entries(table)
        return context.reads[entries[0]['artifact_id']] if entries else dict(state='missing', text='', records=[], detail='artifact unavailable')
    broker, adapter, host = read('broker'), read('adapter'), read('host')
    observation = capture_observation(cell, evidence, broker, adapter, host['records'], host, capture_context=context)
    message_ids = [source for _, _, row in context.records('injection')
                   if valid(row, SCHEMAS['injection']) for source in row['message_ids']]
    safe_records = [record for record in host['records'] if host_shape_valid(record)]
    tokens = {match[1] for record in safe_records for text in strings(record) for match in TOKEN.finditer(text)}
    evidence.update(message_ids=message_ids, received=count_receives(safe_records, tokens, message_ids))
    evidence.update(notice_evidence(broker['text'], cell.count))
    return dict(inputs=build_verdict_inputs(cell, evidence, raw_observation=observation), context=context,
                evidence=evidence, classifications=deepcopy(context.decisions), diagnostics=deepcopy(context.diagnostics))


def broker_records(text):
    """Recognized broker evidence cannot disappear when its grammar is damaged."""
    from collect import attempt_events
    for line, text_line in enumerate(text.splitlines(keepends=True), 1):
        if not text_line.endswith('\n'):
            continue
        kind = next((kind for kind in ('INJECT', 'ATTEMPT', 'SINK') if 'TEST ' + kind in text_line), None)
        if kind is None:
            continue
        value = None
        if kind == 'ATTEMPT':
            parsed = attempt_events(text_line)
            if parsed:
                p = parsed[0]
                if (set(p) == {'token', 'topic', 'transport', 'phase', 'members', 'retired', 'elapsed_ms'}
                        and p['transport'] in ('channel', 'inbox', 'fetch') and p['phase'] in ('reserved', 'confirmed', 'failed', 'expired', 'released')
                        and all(re.fullmatch(r'\d+', p[k]) for k in ('members', 'retired', 'elapsed_ms'))
                        and re.fullmatch(r'-?\d+', p['topic'])):
                    try:
                        if len(re.findall(r'(\w+)=([^\s]+)', text_line)) == len(p):
                            value = {**p, **{key: int(p[key]) for key in ('topic', 'members', 'retired', 'elapsed_ms')}}
                    except ValueError:
                        pass
        elif kind == 'INJECT':
            m = re.search(r'TEST INJECT accepted topic=(-?\d+) message_id=(-?\d+) kind=(text|voice|photo) source=test-inject\s*$', text_line)
            if m:
                try:
                    value = dict(topic=int(m[1]), message_id=int(m[2]), kind=m[3])
                except ValueError:
                    pass
        else:
            m = re.search(r'TEST SINK (reply topic=(-?\d+)|edit) text=("(?:[^"\\]|\\.)*")\s*$', text_line)
            if m:
                try:
                    value = dict(text=json.loads(m[3]), topic=int(m[2]) if m[2] else None)
                except ValueError:
                    pass
        yield line, kind, value


def host_shape_valid(record):
    """Validate containers before calling the historical shape classifiers."""
    if type(record) is not dict:
        return False
    for key in ('message', 'attachment', 'origin'):
        if key in record and type(record[key]) is not dict:
            return False
    attachment = record.get('attachment', {})
    if 'origin' in attachment and type(attachment['origin']) is not dict:
        return False
    content = record.get('message', {}).get('content')
    if content is not None and type(content) not in (str, list):
        return False
    if type(content) is list:
        for block in content:
            if type(block) is not dict:
                return False
            if block.get('type') == 'text' and type(block.get('text')) is not str:
                return False
            if block.get('type') == 'tool_use' and (not ID(block.get('id')) or not ID(block.get('name')) or ('input' in block and type(block['input']) is not dict)):
                return False
            if block.get('type') == 'tool_result' and (not ID(block.get('tool_use_id')) or ('is_error' in block and type(block['is_error']) is not bool)):
                return False
    return True


def assemble_context(cell, evidence, broker_read, adapter_read, records, host_read, context):
    from collect import build_verdict_inputs
    _, _, observation = build_verdict_inputs(cell, evidence)
    if not isinstance(context, CaptureContext):
        observation['artifacts'] = [dict(id=CAPTURE_ARTIFACT, kind='context', content_digest=None)]
        scope = {key: evidence.get(key) for key in SCOPE}
        scope['run_id'] = evidence.get('run_id', 'unrecorded-run')
        observation['streams'] = [dict(id='state', role='state', scope=scope, state='malformed', first_seq=None,
            last_seq=None, through_barrier_id=None, artifact_ids=[CAPTURE_ARTIFACT], detail='capture context must be checked raw tables; canonical input forbidden')]
        return observation
    if not context.descriptor:
        # Retain the legacy incomplete facts on descriptor failure. Missing v1
        # identities cannot erase a raw attempt or authorize a delivery witness.
        from collect import capture_observation
        safe_records = [record for record in records if host_shape_valid(record)]
        observation = capture_observation(cell, evidence, broker_read, adapter_read, safe_records, host_read)
        observation['collection_complete'] = False
        return observation
    # The seam's principal arguments remain authoritative. A context may bind
    # their occurrences, but cannot substitute an older copy of the raw evidence.
    for table, supplied in (('broker', broker_read), ('adapter', adapter_read), ('host', host_read)):
        entries = context.entries(table)
        if not entries:
            continue
        artifact = entries[0]['artifact_id']
        required = {'state', 'text', 'records', 'record_lines', 'lines', 'bytes', 'sha256', 'detail', 'problems'}
        if type(supplied) is not dict or not required <= supplied.keys():
            context.problem(artifact, 'read', 'principal final read unavailable or unchecked')
            context.reads[artifact] = dict(state='missing' if supplied is None else 'malformed', text='', records=[],
                record_lines=[], lines=0, bytes=None, sha256=None, detail='principal final read unavailable or unchecked',
                problems=['principal final read unavailable or unchecked'])
            continue
        if supplied != context.reads[artifact]:
            context.problem(artifact, 'read', 'principal read differs from checked context extent')
            context.reads[artifact] = dict(supplied)
        if table == 'host' and records != supplied['records']:
            context.problem(artifact, 'read', 'host records differ from checked final read')
            context.reads[artifact] = dict(supplied, records=records)
            read_problem(context.reads[artifact], 'malformed', 'host records differ from checked final read')
    assembler = Assembler(cell, evidence, observation, context)
    return assembler.assemble()


class Assembler:
    def __init__(self, cell, evidence, observation, context):
        self.cell, self.evidence, self.o, self.c = cell, evidence, observation, context
        self.owners, self.occurrences, self.bindings, self.row_members = {}, {}, {}, {}
        self.tables, self.streams, self.host = {}, {}, []
        self.broker = {}
        self.fetch_call = None
        self.release = None
        self.failed_correlations = set()

    def problem(self, artifact, line, code, malformed=False):
        self.c.problem(artifact, f'line:{line}', code, malformed=malformed)

    def identities(self, artifact, line, supplied, observed, *, block=0):
        """Only fill absent facts. Observed identities always survive a failed join.

        Callers translate real extractor fields into identity domains before
        joining. Never infer an identity from an ordinal, sample text or count.
        Member/source comparisons share the extractors' unordered semantics.
        Diagnostics identify the occurrence and field, never private values.
        """
        def comparable(key, value):
            if key == 'members':
                return {(v['row_id'], v['revision']['value'], frozenset(v['source_ids'])) for v in value}
            if key == 'source_ids':
                return set(value)
            return value
        for key, value in observed.items():
            if key in supplied and (type(supplied[key]) is not type(value)
                                    or comparable(key, supplied[key]) != comparable(key, value)):
                self.c.problem(artifact, f'line:{line}/block:{block}',
                               'delivery evidence identity mismatch: ' + key + ' correlation conflict')
                self.failed_correlations.add((artifact, line, block))
        return deepcopy({**supplied, **observed})

    def prepare(self):
        for table in SCHEMAS.keys() | {'driver'}:
            self.tables[table] = []
            for artifact, line, record in self.c.records(table):
                schema = SCHEMAS.get(table)
                if table == 'queue-samples' and 'event_id' in record:
                    schema = dict(schema, event_id=ID)
                if table == 'driver':
                    action = record.get('action')
                    schema = dict(action=ENUM(action), **DRIVER[action]) if type(action) is str and action in DRIVER else {}
                if not schema or not valid(record, schema):
                    self.problem(artifact, line, 'malformed recognized ' + table + ' record', True)
                    continue
                self.tables[table].append((artifact, line, record))
        owner_schema = dict(artifact_id=ID, stream_id=ID, role=ENUM(*ROLES), **{k: ID for k in SCOPE[:-1]}, claim_generation=INT, pid=INT)
        meta_schema = dict(artifact_id=ID, line=lambda x: INT(x) and x > 0, block=INT, stream_id=ID, event_id=ID,
                           delivery_id=lambda x: x is None or ID(x), clock_id=ID, time_ms=INT)
        for artifact, line, record in self.c.records('ownership'):
            if set(record) != {'schema_version', 'owners', 'occurrences'} or type(record.get('schema_version')) is not int or record['schema_version'] != 1 or type(record.get('owners')) is not list or type(record.get('occurrences')) is not list:
                self.problem(artifact, line, 'malformed ownership table', True)
                continue
            for owner in record['owners']:
                key = (owner.get('artifact_id'), owner.get('stream_id')) if type(owner) is dict else None
                if not valid(owner, owner_schema) or key in self.owners or owner['artifact_id'] not in self.c.reads:
                    self.problem(artifact, line, 'invalid or duplicate artifact ownership', True)
                    continue
                self.owners[key] = owner
            for meta in record['occurrences']:
                overrides = {key: meta[key] for key in SCOPE if key in meta} if type(meta) is dict else {}
                base = {key: value for key, value in meta.items() if key not in SCOPE} if type(meta) is dict else {}
                key = (base.get('artifact_id'), base.get('line'), base.get('block'))
                if (not valid(base, meta_schema) or key in self.occurrences
                        or any(not (INT(value) if name == 'claim_generation' else ID(value)) for name, value in overrides.items())):
                    self.problem(artifact, line, 'invalid or duplicate occurrence ownership', True)
                    continue
                self.occurrences[key] = meta
        for artifact, line, record in self.tables['attempts']:
            key = (record['artifact_id'], record['line'], record['block'])
            if key in self.bindings or key not in self.occurrences:
                self.problem(artifact, line, 'attempt occurrence missing or ambiguous', True)
                continue
            self.bindings[key] = record
        for artifact, line, row in self.tables['rows']:
            key = (row['row_id'], row['revision'])
            member = dict(row_id=row['row_id'], revision=dict(kind='exact', value=row['revision']), source_ids=[str(v) for v in row['source_ids']])
            if key in self.row_members and self.row_members[key] != member:
                self.problem(artifact, line, 'conflicting row revision source membership', True)
            self.row_members.setdefault(key, member)
        for entry in self.c.entries('broker'):
            artifact = entry['artifact_id']
            for line, kind, value in broker_records(self.c.reads[artifact]['text']):
                if value is None:
                    self.problem(artifact, line, 'malformed recognized TEST ' + kind + ' record', True)
                else:
                    self.broker[(artifact, line)] = (kind, value)
        seen = {}
        for artifact, line, record in self.c.records('host'):
            if not host_shape_valid(record):
                self.problem(artifact, line, 'malformed recognized host record', True)
                continue
            uuid = record.get('uuid')
            if uuid is not None and not ID(uuid):
                self.problem(artifact, line, 'invalid host UUID', True)
                continue
            if uuid in seen:
                if seen[uuid][0] != record:
                    self.problem(artifact, line, f'host UUID content conflict with line:{seen[uuid][1]}', True)
                continue
            if uuid:
                seen[uuid] = (record, line)
            self.host.append((artifact, line, record))

    def scope(self, artifact, line, block=0):
        meta = self.occurrences.get((artifact, line, block))
        owner = self.owners.get((artifact, meta['stream_id'])) if meta else None
        if not owner:
            self.problem(artifact, line, 'occurrence ownership unavailable')
            return None
        return self.identities(artifact, line, {key: owner[key] for key in SCOPE},
                               {key: meta[key] for key in SCOPE if key in meta}, block=block)

    def members(self, pairs, artifact, line):
        result = []
        for pair in pairs:
            member = self.row_members.get((pair['row_id'], pair['revision']))
            if member is None:
                self.problem(artifact, line, 'row revision source binding unavailable')
                member = dict(row_id=pair['row_id'], revision=dict(kind='exact', value=pair['revision']), source_ids=[])
            result.append(self.identities(artifact, line, member,
                dict(row_id=pair['row_id'], revision=dict(kind='exact', value=pair['revision']),
                     **({'source_ids': pair['source_ids']} if 'source_ids' in pair else {}))))
        return result

    def event(self, artifact, line, milestone, payload, *, block=0, origin='rig-control', transport=None,
              members=(), token=None, operation_id=None, binding=True, scope=None, observed=None):
        meta = self.occurrences.get((artifact, line, block))
        scope = scope or self.scope(artifact, line, block)
        if not meta or not scope:
            return None
        bound = self.bindings.get((artifact, line, block))
        if binding and bound is None:
            self.problem(artifact, line, 'attempt binding unavailable')
        supplied = dict(attempt_id=bound['attempt_id'], group_id=bound['group_id'],
                        members=self.members(bound['rows'], artifact, line)) if bound else {}
        identity = self.identities(artifact, line, supplied,
            dict(transport=transport, token=token, operation_id=operation_id, members=list(members), **(observed or {})), block=block)
        event = dict(id=meta['event_id'], scope=scope,
            position=dict(stream_id=meta['stream_id'], seq=line, clock_id=meta['clock_id'], time_ms=meta['time_ms']),
            artifact_ref=dict(artifact_id=artifact, locator=f'line:{line}/block:{block}'), collection_complete=True,
            caused_by=[], origin=origin, milestone=milestone,
            delivery_id=meta['delivery_id'], payload=payload,
            **{'attempt_id': None, 'group_id': None, **identity})
        self.o['events'].append(event)
        return event

    def assemble(self):
        self.prepare()
        if self.c.descriptor:
            self.o['provenance'].update(self.c.descriptor['provenance'], extractor_version='capture-context-v1')
        self.assemble_driver()
        self.assemble_rows_injection()
        self.assemble_fetch()
        self.assemble_broker()
        self.assemble_host()
        self.assemble_receipts()
        self.assemble_samples()
        self.finish()
        return self.o

    def assemble_driver(self):
        for artifact, line, row in self.tables['driver']:
            action = row['action']
            if action in ('cut', 'fetch_request', 'fetch_release'):
                continue
            state = row.get('observed_state', 'resumed' if row.get('operation') == 'resume' else 'fresh')
            event = self.event(artifact, line, 'state_observed', dict(state=state, detail=action), binding=False)
            if not event:
                continue
            selected_session_matches = action != 'session_sample' or row['host_session_ref'] == event['scope']['host_session_id']
            if action == 'session_sample':
                event['scope'] = self.identities(artifact, line, event['scope'], dict(host_session_id=row['host_session_ref']))
            proof = dict(barrier_id='injection', scope=deepcopy(event['scope']), proven=True, event_ids=[event['id']], detail=action)
            if action == 'state_sample':
                self.o['state_proof'] = dict(proof, state=state)
            elif action == 'session_sample':
                proof['proven'] = selected_session_matches and (
                    row['transcript_records'] > 0 if row['operation'] == 'resume' else row['transcript_records'] == 0)
                self.o['session_proof'] = dict(proof, posture=state,
                    freshness='not_applicable' if state == 'resumed' else 'transcript_free_at_injection')

    def assemble_rows_injection(self):
        for artifact, line, row in self.tables['rows']:
            self.event(artifact, line, 'row_persisted', {}, origin='persistence', binding=False,
                       members=self.members([row], artifact, line))
        for artifact, line, row in self.tables['injection']:
            broker_entries = self.c.entries('broker')
            broker_id = broker_entries[0]['artifact_id'] if broker_entries else None
            admissions = [self.broker.get((broker_id, number)) for number in row['broker_lines']]
            for number, admission, source in zip(row['broker_lines'], admissions, row['message_ids']):
                if admission and admission[0] == 'INJECT':
                    self.identities(broker_id, number, dict(source_ids=[str(source)]),
                                    dict(source_ids=[str(admission[1]['message_id'])]))
                    scope = self.scope(broker_id, number)
                    if scope:
                        route = scope['route_id'].split('/')
                        self.identities(broker_id, number, scope, dict(route_id=(route[0] + '/' if len(route) == 2 else '') + str(admission[1]['topic'])))
            accepted = len(admissions) == len(row['message_ids']) and all(
                entry and entry[0] == 'INJECT' and entry[1]['message_id'] == source and entry[1]['kind'] == row['kind']
                for entry, source in zip(admissions, row['message_ids']))
            if not accepted:
                self.problem(artifact, line, 'injection result disagrees with broker acceptance')
            members = [deepcopy(member) for member in self.row_members.values() if set(member['source_ids']) <= {str(v) for v in row['message_ids']}]
            # Injection binds the first observed revision of each row; later persistence is separate evidence.
            members = list({member['row_id']: member for member in reversed(members)}.values())[::-1]
            event = self.event(artifact, line, 'injection_completed', dict(accepted=accepted), members=members, binding=False)
            if event:
                self.o['sources'].extend(dict(slot=slot, source_id=str(source), kind=row['kind'], admission_event_id=event['id'])
                                         for slot, source in enumerate(row['message_ids']))

    def assemble_fetch(self):
        from collect import fetch_trailer, result_text
        calls = []
        for artifact, line, record in self.host:
            if record.get('type') != 'assistant' or record.get('message', {}).get('role') != 'assistant':
                continue
            content = record.get('message', {}).get('content', [])
            for block_index, block in enumerate(content if type(content) is list else []):
                if block.get('type') == 'tool_use' and block['name'].endswith('__fetch_queue'):
                    calls.append((artifact, line, block_index, record, block))
        requests = []
        for artifact, line, row in self.tables['driver']:
            if row['action'] != 'fetch_request':
                continue
            frame = row['frame']
            related = [call for call in calls if call[3].get('uuid') == row['host_record_id'] or call[4]['id'] == row['operation_id']]
            for source, number, index, record, block in related:
                self.identities(source, number, {key: row[key] for key in ('host_record_id', 'operation_id')},
                                dict(host_record_id=record.get('uuid'), operation_id=block['id']), block=index)
            matches = [call for call in related if call[3].get('uuid') == row['host_record_id'] and call[4]['id'] == row['operation_id']]
            params = frame.get('params')
            if (len(matches) != 1 or frame.get('method') != 'tools/call' or type(frame.get('id')) not in (int, str)
                    or type(params) is not dict or params.get('name') != 'fetch_queue' or type(params.get('arguments')) is not dict
                    or params['arguments'] != matches[0][4].get('input')):
                self.problem(artifact, line, 'fetch request and recorded tool call association rejected', True)
                continue
            source, number, index, record, block = matches[0]
            raw_session = record.get('sessionId', record.get('session_id'))
            self.identities(source, number, self.scope(source, number, index) or {},
                            dict(host_session_id=raw_session), block=index)
            self.identities(source, number, self.scope(artifact, line) or {},
                            dict(host_session_id=raw_session), block=index)
            if 'session_id' in record:
                self.identities(source, number, dict(host_session_id=raw_session),
                                dict(host_session_id=record['session_id']), block=index)
            if record.get('sessionId', record.get('session_id')) != (self.scope(artifact, line) or {}).get('host_session_id'):
                self.problem(artifact, line, 'fetch call ownership conflict')
                continue
            binding = self.bindings.get((artifact, line, 0), {})
            event = self.event(artifact, line, 'fetch_requested', dict(ack=params['arguments'].get('ack') is True),
                transport='fetch', operation_id=block['id'], members=self.members(binding.get('rows', []), artifact, line))
            if event:
                requests.append((frame['id'], block['id'], event))
        if len(requests) == 1:
            self.fetch_call = requests[0]
        elif self.cell.transport == 'fetch':
            self.c.problem(CAPTURE_ARTIFACT, 'driver', 'fetch operation association unavailable or ambiguous')
        for entry in self.c.entries('held'):
            artifact = entry['artifact_id']
            for line, response in zip(self.c.reads[artifact]['record_lines'], self.c.reads[artifact]['records']):
                result = response.get('result')
                trailer = fetch_trailer(result_text(result.get('content'))) if type(result) is dict else None
                if not trailer or result.get('isError', False) is not False:
                    self.problem(artifact, line, 'held fetch response trailer rejected')
                    continue
                operation = None
                if self.fetch_call:
                    self.identities(artifact, line, dict(request_id=self.fetch_call[0]), dict(request_id=response.get('id')))
                if self.fetch_call and response.get('id') == self.fetch_call[0]:
                    operation = self.fetch_call[1]
                else:
                    self.problem(artifact, line, 'held response request correlation rejected')
                members = self.members([dict(row_id=v['record_id'], revision=v['revision']) for v in trailer['members']], artifact, line)
                self.event(artifact, line, 'fetch_result_produced', dict(success=True,
                    trailer=dict(state='complete', token=trailer['token'], members=deepcopy(members))),
                    transport='fetch', token=trailer['token'], operation_id=operation, members=members)
                bound = self.bindings.get((artifact, line, 0), {})
                reservations = [value for (broker, number), (kind, value) in self.broker.items()
                    if kind == 'ATTEMPT' and value['phase'] == 'reserved'
                    and all(self.bindings.get((broker, number, 0), {}).get(key) == bound.get(key)
                            for key in ('attempt_id', 'group_id'))]
                if not reservations or not any(row['token'] == trailer['token'] for row in reservations):
                    self.problem(artifact, line, 'held response reservation correlation rejected')
                self.held = (artifact, response, trailer)
        for artifact, line, row in self.tables['driver']:
            if row['action'] != 'fetch_release':
                continue
            held = getattr(self, 'held', None)
            if held and self.fetch_call:
                self.identities(artifact, line, dict(artifact_id=held[0], request_id=held[1].get('id'), operation_id=self.fetch_call[1]),
                                {key: row[key] for key in ('artifact_id', 'request_id', 'operation_id')})
            if (not held or not self.fetch_call or row['artifact_id'] != held[0] or row['request_id'] != held[1].get('id')
                    or row['operation_id'] != self.fetch_call[1]):
                self.problem(artifact, line, 'fetch release correlation rejected')
                continue
            self.release = self.event(artifact, line, 'state_observed', dict(state='fetch_response_released', detail='fetch_release'), binding=False)

    def bound_operation(self, binding, artifact, line):
        if self.fetch_call and all(binding.get(key) == self.fetch_call[2][key] for key in ('attempt_id', 'group_id')):
            return self.fetch_call[1]
        self.problem(artifact, line, 'fetch attempt request correlation rejected')
        return None

    def assemble_broker(self):
        for (artifact, line), (kind, row) in self.broker.items():
            if kind == 'INJECT':
                continue
            scope = self.scope(artifact, line)
            if scope and row.get('topic') is not None:
                route = scope['route_id'].split('/')
                if len(route) == 2 and route[1] != str(row['topic']):
                    self.problem(artifact, line, 'broker route ownership conflict')
                scope = self.identities(artifact, line, scope,
                    dict(route_id=route[0] + '/' + str(row['topic']) if len(route) == 2 else str(row['topic'])))
            if kind == 'ATTEMPT':
                binding = self.bindings.get((artifact, line, 0), {})
                members = self.members(binding.get('rows', []), artifact, line)
                retired = self.members(binding.get('retired_rows', []), artifact, line)
                if len(members) != row['members'] or len(retired) != row['retired']:
                    self.problem(artifact, line, 'attempt counts disagree with observed row membership')
                reserved = row['phase'] == 'reserved'
                if not reserved and any(event['milestone'] == 'attempt_reserved'
                        and event['attempt_id'] == binding.get('attempt_id') and event['group_id'] == binding.get('group_id')
                        and event['token'] != row['token'] for event in self.o['events']):
                    self.problem(artifact, line, 'terminal token conflicts with reservation')
                payload = dict(deadline_ms=None) if reserved else dict(
                    outcome=row['phase'], elapsed_ms=row['elapsed_ms'], retired_members=retired, reason='')
                self.event(artifact, line, 'attempt_reserved' if reserved else 'attempt_terminal', payload,
                    origin='persistence' if reserved else 'retirement', transport=row['transport'], token=row['token'],
                    operation_id=self.bound_operation(binding, artifact, line) if row['transport'] == 'fetch' else None, members=members, scope=scope)
            else:
                text = row['text']
                held = re.search(r'(\d+) messages? queued', text) if 'Held' in text else None
                if 'Held' in text and not held:
                    self.problem(artifact, line, 'malformed recognized Held notice', True)
                meta = self.occurrences.get((artifact, line, 0), {})
                snapshot = next((row['snapshot_id'] for _, _, row in self.tables['queue-samples']
                                 if row.get('event_id') == meta.get('event_id')), None)
                self.event(artifact, line, 'notice_emitted', dict(kind='held' if held else 'route' if 'Live route:' in text else 'other',
                    held_count=int(held[1]) if held else None, route_diagnostic_lines=[part for part in text.splitlines() if 'Live route:' in part],
                    queue_snapshot_id=snapshot), binding=False, scope=scope)

    def assemble_host(self):
        from collect import classify, classify_fetch, intake_text, TOKEN, ATTEMPT, fetch_trailer, result_text
        held = getattr(self, 'held', None)
        expected = held[2] if held else None
        calls = {self.fetch_call[1]} if self.fetch_call else set()
        for artifact, line, record in self.host:
            if self.cell.transport == 'fetch':
                content = record.get('message', {}).get('content', [])
                candidates = [(index, block) for index, block in enumerate(content if type(content) is list else []) if block.get('type') == 'tool_result']
            else:
                candidates = [(0, None)]
            for block_index, block in candidates:
                candidate = deepcopy(record)
                if block is not None:
                    candidate['message']['content'] = [block]
                classification = classify_fetch(candidate, expected, calls) if block is not None else classify(candidate)
                if block is not None:
                    # Rejected candidates still carry raw identities. Compare
                    # them to the occurrence binding before withholding a milestone.
                    trailer = fetch_trailer(result_text(block.get('content')))
                    binding = self.bindings.get((artifact, line, block_index))
                    if trailer and binding:
                        self.identities(artifact, line, dict(members=self.members(binding['rows'], artifact, line)),
                            dict(members=self.members([dict(row_id=v['record_id'], revision=v['revision'])
                                                       for v in trailer['members']], artifact, line)), block=block_index)
                    if trailer and expected:
                        self.identities(artifact, line, dict(token=expected['token']),
                                        dict(token=trailer['token']), block=block_index)
                    if self.fetch_call:
                        self.identities(artifact, line, dict(operation_id=self.fetch_call[1]),
                                        dict(operation_id=block['tool_use_id']), block=block_index)
                self.c.decisions.append(dict(artifact_id=artifact, locator=f'line:{line}/block:{block_index}', accept=classification['accept']))
                if classification['accept'] is not True:
                    self.problem(artifact, line, 'fetch candidate rejected' if block is not None else 'host candidate rejected')
                    continue
                if record.get('type') not in ('user', 'attachment'):
                    continue
                if not ID(record.get('uuid')):
                    self.problem(artifact, line, 'host occurrence UUID unavailable', True)
                    continue
                scope = self.scope(artifact, line, block_index)
                if not scope:
                    continue
                session = record.get('sessionId', record.get('session_id'))
                if not ID(session):
                    self.problem(artifact, line, 'transcript session identity unavailable', True)
                    scope['host_session_id'] = None
                else:
                    if session != scope['host_session_id'] or ('session_id' in record and record['session_id'] != session):
                        self.problem(artifact, line, 'transcript ownership conflict')
                    scope = self.identities(artifact, line, scope, dict(host_session_id=session), block=block_index)
                    if 'session_id' in record:
                        self.identities(artifact, line, dict(host_session_id=session),
                                        dict(host_session_id=record['session_id']), block=block_index)
                binding = self.bindings.get((artifact, line, block_index), {})
                members = self.members(binding.get('rows', []), artifact, line)
                if block is None:
                    text = intake_text(record)
                    route = scope['route_id'].split('/')
                    chat = re.search(r'\bchat_id=["\']([^"\']*)["\']', text)
                    topic = re.search(r'\bmessage_thread_id=["\']([^"\']*)["\']', text)
                    if any(value and not re.fullmatch(r'-?\d+', value[1]) for value in (chat, topic)):
                        self.problem(artifact, line, 'host route attributes invalid', True)
                    if chat or topic:
                        raw_route = (chat[1] if chat else route[0]) + '/' + (topic[1] if topic else route[1] if len(route) == 2 else 'dm')
                        scope = self.identities(artifact, line, scope, dict(route_id=raw_route), block=block_index)
                    sources = re.search(r'\bmerged_message_ids=["\']([^"\']*)["\']', text) or re.search(r'\bmessage_id=["\']([^"\']*)["\']', text)
                    ids = sources[1].split(',') if sources else []
                    if not ids or any(not re.fullmatch(r'-?\d+', v) for v in ids):
                        self.problem(artifact, line, 'host source attributes unavailable or invalid', True)
                        ids = []
                    if set(ids) != {v for member in members for v in member['source_ids']}:
                        self.problem(artifact, line, 'host source membership conflict')
                        self.identities(artifact, line, dict(source_ids=[v for m in members for v in m['source_ids']]),
                                        dict(source_ids=ids), block=block_index)
                        # Retain the observed claim on its independently bound row.
                        for member in members:
                            member['source_ids'] = ids[:]
                    attempt = ATTEMPT.search(text)
                    if not attempt or not re.fullmatch(r'(channel|inbox|cross-session):[1-9]\d*', attempt[1]):
                        self.problem(artifact, line, 'host attempt label invalid', True)
                    self.event(artifact, line, 'transcript_recorded', dict(host_record_id=record['uuid']), origin='host-evidence',
                        transport=classification['transport'], token=classification['token'], members=members, scope=scope,
                        observed=dict(attempt_id=classification['attempt']))
                else:
                    trailer = fetch_trailer(result_text(block.get('content')))
                    members = self.members([dict(row_id=v['record_id'], revision=v['revision']) for v in trailer['members']], artifact, line)
                    # Canonical set order is the independent row-observation order.
                    members.sort(key=lambda v: list(self.row_members).index((v['row_id'], v['revision']['value'])) if (v['row_id'], v['revision']['value']) in self.row_members else len(self.row_members))
                    if not self.release:
                        self.problem(artifact, line, 'host fetch recording lacks observed release')
                    self.event(artifact, line, 'fetch_result_recorded', dict(host_record_id=record['uuid'], success=True,
                        trailer=dict(state='complete', token=trailer['token'], members=deepcopy(members))), block=block_index,
                        origin='host-evidence', transport='fetch', token=trailer['token'], operation_id=block['tool_use_id'], members=members, scope=scope)

    def assemble_receipts(self):
        for artifact, line, row in self.tables['receipts']:
            event = self.event(artifact, line, 'receipt_accepted' if row['action'] == 'accepted' else 'receipt_rejected',
                dict(elapsed_ms=row['elapsed_ms']) if row['action'] == 'accepted' else dict(reason='broker rejected receipt'),
                origin='broker-ack', transport=row['transport'], token=row['token'], operation_id=row['operation_id'],
                members=self.members(row['rows'], artifact, line), binding=False,
                observed=dict(attempt_id=row['attempt_id'], group_id=row['group_id']))

    def assemble_samples(self):
        for artifact, line, row in self.tables['driver']:
            if row['action'] != 'cut':
                continue
            scope = self.scope(artifact, line)
            if scope:
                cutoffs = {cut['stream_id']: cut['seq'] for cut in row['cuts']}
                complete = True
                if len(cutoffs) != len(row['cuts']):
                    self.problem(artifact, line, 'duplicate stream cut', True)
                    complete = False
                extents = {}
                for (source, stream), owner in self.owners.items():
                    extents.setdefault(stream, []).append(self.c.reads[source])
                if set(extents) - cutoffs.keys():
                    self.problem(artifact, line, 'required stream cutoff unavailable')
                    complete = False
                for stream, cutoff in cutoffs.items():
                    reads = extents.get(stream, [])
                    if len(reads) != 1:
                        self.problem(artifact, line, 'stream cutoff ownership unavailable or ambiguous')
                        complete = False
                    elif cutoff > reads[0]['lines']:
                        self.problem(artifact, line, 'stream cutoff outside observed extent')
                        complete = False
                    elif row['barrier_id'] == 'final' and cutoff != reads[0]['lines']:
                        self.problem(artifact, line, 'final cut does not cover observed stream extent')
                        complete = False
                name = next(('notice/' + sample['event_id'] for _, _, sample in self.tables['queue-samples']
                             if sample['barrier_id'] == row['barrier_id'] and 'event_id' in sample), row['barrier_id'])
                self.o['barriers'].append(dict(id=row['barrier_id'], name=name, scope=scope, state='reached',
                    stream_cutoffs=cutoffs, artifact_refs=[dict(artifact_id=artifact, locator=f'line:{line}')], collection_complete=complete))
        for artifact, line, row in self.tables['queue-samples']:
            scope = self.scope(artifact, line)
            if scope:
                self.o['queue_snapshots'].append(dict(id=row['snapshot_id'], barrier_id=row['barrier_id'], scope=scope,
                    state=row['read_state'], rows=self.members(row['rows'], artifact, line),
                    artifact_refs=[dict(artifact_id=artifact, locator=f'line:{line}')]))
        for artifact, line, row in self.tables['contracts']:
            scope = self.scope(artifact, line)
            if scope:
                axes = dict(negotiation=row['negotiation'], live_eligibility=dict(channel=row['channel'], inbox=row['inbox']),
                            receipt_type=row['receipt_type'], fetch_policy=row['fetch_policy'], accepted_modes=row['accepted_modes'])
                self.o['contract_observations'].append(dict(barrier_id=row['barrier_id'], scope=scope, axes=axes,
                    artifact_refs=[dict(artifact_id=artifact, locator=f'line:{line}')], collection_complete=True))

    def finish(self):
        final = next((barrier for barrier in self.o['barriers'] if barrier['id'] == 'final'), None)
        if not final or not any(row['action'] == 'window_end' for _, _, row in self.tables['driver']):
            self.c.problem(CAPTURE_ARTIFACT, 'driver', 'missing final cut or observation window boundary')
        # Bindings also associate occurrences with independent reservations.
        # Compare all carried identities, preserving the occurrence on conflict.
        reservations = [e for e in self.o['events'] if e['milestone'] == 'attempt_reserved']
        for event in self.o['events']:
            if event['milestone'] not in ('transcript_recorded', 'fetch_result_produced', 'fetch_result_recorded',
                                           'receipt_accepted', 'receipt_rejected', 'attempt_terminal'):
                continue
            related = [r for r in reservations if any(event[key] is not None and event[key] == r[key]
                                                      for key in ('attempt_id', 'group_id', 'token'))]
            artifact = event['artifact_ref']['artifact_id']
            line = event['position']['seq']
            block = int(event['artifact_ref']['locator'].rsplit(':', 1)[1])
            if len(related) == 1:
                keys = ('attempt_id', 'group_id', 'token', 'transport', 'operation_id', 'members')
                self.identities(artifact, line, {key: related[0][key] for key in keys},
                                {key: event[key] for key in keys}, block=block)
                self.identities(artifact, line, related[0]['scope'], event['scope'], block=block)
        for entry in self.c.inventory:
            artifact = entry['artifact_id']
            read = self.c.reads[artifact]
            self.o['artifacts'].append(dict(id=artifact, kind=entry['table'], content_digest=read['sha256']))
        for (artifact, identity), owner in self.owners.items():
            read = self.c.reads[artifact]
            if identity in self.streams:
                self.c.problem(artifact, 'ownership', 'stream ownership is ambiguous')
                continue
            self.streams[identity] = dict(id=identity, role=owner['role'], scope={key: owner[key] for key in SCOPE},
                state=read['state'], first_seq=1 if read['lines'] else 0, last_seq=read['lines'],
                through_barrier_id='final' if final and final['stream_cutoffs'].get(identity) == read['lines'] else None,
                artifact_ids=[artifact], detail=read['detail'])
        self.o['streams'] = list(self.streams.values())
        for event in self.o['events']:
            stream = self.streams.get(event['position']['stream_id'])
            key = (event['artifact_ref']['artifact_id'], event['position']['seq'],
                   int(event['artifact_ref']['locator'].rsplit(':', 1)[1]))
            event['collection_complete'] = bool(stream and stream['state'] == 'complete' and key not in self.failed_correlations)
        for barrier in self.o['barriers']:
            barrier['collection_complete'] &= all(identity in self.streams and self.streams[identity]['state'] == 'complete'
                                                   and cutoff <= self.streams[identity]['last_seq'] for identity, cutoff in barrier['stream_cutoffs'].items())
        # Classifier rejections are diagnostics, not read corruption. They cannot
        # authorize a milestone, even when all bytes were successfully collected.
        harmless = {'host candidate rejected', 'fetch candidate rejected'}
        self.o['collection_complete'] = bool(final and self.c.inventory
            and all(item['code'] in harmless for item in self.c.diagnostics)
            and all(read['state'] == 'complete' for read in self.c.reads.values())
            and all(barrier['collection_complete'] for barrier in self.o['barriers']))
