"""Native Claude evidence classifiers; policy retained verbatim."""
import re

TOKEN = re.compile(r'c3_delivery_id=["\']([^"\']+)["\']')
ATTEMPT = re.compile(r'c3_attempt=["\']([^"\']+)["\']')
PEER_PREFIX = "Another Claude session sent a message:\n"


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from strings(item)


def intake_text(record):
    from capture_context import host_shape_valid
    if not host_shape_valid(record):
        return ""
    if record.get("type") == "user" and record.get("message", {}).get("role") == "user":
        content = record["message"].get("content", "")
        if isinstance(content, str):
            return content
        return "\n".join(b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text") if isinstance(content, list) else ""
    if record.get("type") == "attachment" and record.get("attachment", {}).get("type") == "queued_command":
        prompt = record["attachment"].get("prompt", "")
        return prompt if isinstance(prompt, str) else ""
    if record.get("type") == "queue-operation":
        content = record.get("content", "")
        return content if isinstance(content, str) else ""
    return ""


def classify(record):
    """Only observed, checked-in shapes become positives; new shapes are TODO."""
    from capture_context import host_shape_valid
    if not host_shape_valid(record):
        return dict(transport="channel", token="TOKEN", attempt="channel:1", accept=False, reason="malformed host record")
    text = intake_text(record)
    token, attempt = TOKEN.search(text), ATTEMPT.search(text)
    transport = "inbox" if attempt and attempt[1].startswith(("inbox:", "cross-session:")) else "channel"
    result = dict(transport=transport, token=token[1] if token else "TOKEN", attempt=attempt[1] if attempt else "channel:1", accept=None,
                  reason="TODO: review captured host shape before adding positive coverage")
    if record.get("type") == "queue-operation" and record.get("operation") == "remove":
        result.update(accept=False, reason="queue removal is not receipt evidence")
    elif text and not token:
        result.update(accept=False, reason="captured record predates delivery token metadata")
    elif token and attempt:
        if transport == "inbox":
            if record.get("type") == "user":
                result.update(accept=bool(record.get("isMeta") and record.get("origin", {}).get("kind") == "peer"
                                          and record.get("origin", {}).get("from") == "c3" and text.startswith(PEER_PREFIX)),
                              reason="verified 2.1.263 peer user shape and prefix")
            elif record.get("type") in ("queue-operation", "attachment"):
                source = re.match(r'<channel\s[^>]*\bsource=["\']plugin:c3:c3["\']', text)
                if record.get("type") == "queue-operation":
                    provenance = record.get("operation") == "enqueue"
                else:
                    attachment = record.get("attachment", {})
                    provenance = (attachment.get("isMeta") and attachment.get("origin", {}).get("kind") == "peer"
                                  and attachment.get("origin", {}).get("from") == "c3")
                result.update(accept=bool(source and provenance), reason="verified 2.1.266 bare peer intake envelope")
        elif text.lstrip().startswith("<channel"):
            known = record.get("type") == "user" or (record.get("type") == "queue-operation" and record.get("operation") == "enqueue") or (
                record.get("type") == "attachment" and record.get("attachment", {}).get("origin", {}).get("kind") == "channel")
            if known:
                result.update(accept=True, reason="verified channel intake envelope")
    return result


def result_text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list) and all(isinstance(p, dict) and p.get("type") == "text" and isinstance(p.get("text"), str) for p in content):
        return "\n".join(p["text"] for p in content)
    return ""


def fetch_trailer(text):
    """The phase-4 grammar: final delimiter, complete unique member set."""
    marker = "[C3_FETCH_RECEIPT_V1]\n"
    start = text.rfind(marker)
    if start < 0 or (start and text[start - 1] != "\n"):
        return None
    lines = text[start:].split("\n")
    if len(lines) < 4 or lines[-1] != "[/C3_FETCH_RECEIPT_V1]":
        return None
    group = re.fullmatch(r"group ([A-Za-z0-9_-]+)", lines[1])
    if not group:
        return None
    members = []
    for line in lines[2:-1]:
        member = re.fullmatch(r"member ([A-Za-z0-9_-]+) ([0-9a-f]{64})", line)
        if not member or any(m["record_id"] == member[1] for m in members):
            return None
        members.append(dict(record_id=member[1], revision=member[2]))
    return dict(token=group[1], members=members)


def classify_fetch(record, expected, call_ids):
    """Expected identities come from the held MCP response, not the transcript."""
    result = dict(transport="fetch", token=expected["token"] if expected else "TOKEN",
                  members=expected["members"] if expected else [], tool_use_id="ID", accept=False,
                  reason="not a matching successful complete fetch tool result")
    if not expected:
        result.update(accept=None, reason="TODO: capture a complete broker fetch trailer")
        return result
    from capture_context import host_shape_valid
    if not host_shape_valid(record):
        result.update(reason="malformed host record")
        return result
    content = record.get("message", {}).get("content", [])
    if record.get("type") != "user" or not isinstance(content, list):
        return result
    for block in content:
        if not isinstance(block, dict) or block.get("type") != "tool_result" or block.get("tool_use_id") not in call_ids:
            continue
        result["tool_use_id"] = block["tool_use_id"]
        if block.get("is_error", False) is not False:
            continue
        trailer = fetch_trailer(result_text(block.get("content")))
        if trailer and trailer["token"] == expected["token"] and sorted(trailer["members"], key=lambda m: m["record_id"]) == sorted(expected["members"], key=lambda m: m["record_id"]):
            result.update(accept=True, reason="matching tool result with complete phase-4 receipt trailer")
            return result
    return result


def _same_fetch_trailer(left, right):
    return bool(left and right and left['token'] == right['token'] and
                sorted(left['members'], key=lambda member: member['record_id']) ==
                sorted(right['members'], key=lambda member: member['record_id']))




class ClaudeObservation:
    """Native evidence reader; broker observer tables retain their own authority."""
    def __init__(self, scratch, broker, adapter, profile, instance_id):
        from capture_store import ArtifactSource, CaptureStore
        self.source = ArtifactSource(profile.artifact_source or scratch.root)
        if broker.observer_store and not isinstance(broker.observer_store, ArtifactSource):
            raise ValueError('observer store must be a read-only ArtifactSource')
        self.shared_source = broker.observer_store or self.source
        self.scratch, self.broker, self.adapter, self.profile = scratch, broker, adapter, profile
        self.store = CaptureStore(instance_id, scratch.run_id)
        self.window_finished = False
        self.observed_state = None
        self.operation_lines = set()

    def observe(self, cursor=None):
        from copy import deepcopy
        from capture_store import encoded
        from capture_context import _load_context, REQUIRED
        from evidence_io import checked_bytes, read_problem
        from hostdriver import ArtifactRef
        descriptor_read, descriptor_bytes = self.source.read('capture.json', 'json')
        frozen = self.profile.execution == 'replay' and descriptor_bytes is not None
        reads, raw, controls, diagnostics = {}, {}, [], []
        for identity, name, format in (('pane', 'control/pane.txt', 'log'),
                ('sessions', 'control/sessions.jsonl', 'jsonl'), ('proxy', 'control/events.jsonl', 'jsonl'),
                ('controls', 'control/operations.jsonl', 'jsonl'), ('injection-request', 'control/injection.json', 'json')):
            read, data = self.source.read(name, format)
            if data is not None:
                controls.append((ArtifactRef(identity, name), data))
            if identity == 'sessions':
                sessions = read
            if identity == 'proxy':
                proxy = read
            if identity == 'controls':
                operations = read
            if identity == 'injection-request':
                injection = read
        if frozen:
            # The frozen observer store is raw input, including independent broker
            # observers. The existing loader checks its closed descriptor and seals.
            context = _load_context(self.source.root)
            if not context.descriptor:
                diagnostics.append('invalid frozen capture descriptor')
            descriptor = deepcopy(context.descriptor) if context.descriptor else None
            diagnostics.extend(item['code'] for item in context.diagnostics)
        else:
            descriptor = None
        if descriptor is None:
            selected = sessions['records'][-1] if sessions['records'] else {}
            transcript = selected.get('transcript_path')
            host_session = selected.get('session_id')
            if type(transcript) is not str or not transcript:
                transcript = 'unavailable/transcript.jsonl'
            descriptor = dict(schema_version=1, context_version=1,
                cell={key: self.profile.case[key] for key in ('transport', 'state', 'session', 'kind', 'burst')},
                evidence=dict(run_id=self.scratch.run_id, route_id=self.broker.route_id,
                              host_session_id=host_session, session_id=None),
                provenance=dict(kind='raw_replay' if self.profile.execution == 'replay' else 'live_capture',
                    capture_id=self.scratch.run_id, broker_build='unknown', adapter_build=self.adapter.build_id,
                    host_version=self.profile.host_version, platform='unknown'), artifacts=[])
            paths = dict(broker=('broker/broker.log', 'log'), adapter=('control/adapter.log', 'log'),
                         host=(transcript, 'jsonl'), ownership=('context/ownership.json', 'json'),
                         injection=('context/injection.json', 'json'), held=('context/held.json', 'json'))
            for table in sorted(REQUIRED | ({'held'} if self.profile.case['transport'] == 'fetch' else set())):
                name, format = paths.get(table, ('context/' + table + '.jsonl', 'jsonl'))
                # Export names are public locators, never private transcript paths.
                output_name = 'records.jsonl' if table == 'host' else name
                descriptor['artifacts'].append(dict(artifact_id=table, table=table, file=output_name, format=format))
                source = self.source if table in ('host', 'adapter', 'held', 'driver') else self.shared_source
                read, data = source.read(name, format)
                reads[table] = read
                if data is not None:
                    raw[table] = data
            from capture_store import injection_table
            if reads['injection']['state'] == 'missing':
                row = injection_table(injection, reads['broker'])
                if row is not None:
                    raw['injection'] = encoded(row)
                    reads['injection'] = checked_bytes(raw['injection'], format='json')
                    if reads['broker']['state'] != 'complete':
                        read_problem(reads['injection'], reads['broker']['state'], 'admission read incomplete')
            if sessions['state'] != 'complete':
                read_problem(reads['host'], sessions['state'], 'selected SessionStart read incomplete')
            # The held frame is the actual proxy response, never reconstructed
            # from a transcript or the aggregate fetch_result projection.
            held = [row for row in proxy['records'] if row.get('event') == 'fetch_result_waiting']
            if 'held' in reads and held and type(held[-1].get('frame')) is dict:
                raw['held'] = encoded(held[-1]['frame'])
                reads['held'] = checked_bytes(raw['held'], format='json')
                if proxy['state'] != 'complete':
                    read_problem(reads['held'], proxy['state'], 'held proxy read incomplete')
            from capture_context import DRIVER, valid, ENUM
            for line, operation in zip(operations['record_lines'], operations['records']):
                if line in self.operation_lines or self.store.finalized:
                    continue
                facts = operation.get('facts', {})
                boundary = facts.get('observation_window_end') if type(facts) is dict else None
                if (type(boundary) is dict and boundary.get('observed_state') in
                        (None, 'idle', 'foreground', 'background', 'startup', 'reconnect')
                        and type(boundary.get('time_ms')) is int and boundary['time_ms'] >= 0
                        and type(boundary.get('clock_id')) is str and boundary['clock_id']):
                    self.window_finished = True
                    self.observed_state = boundary['observed_state']
                rows = facts.get('observer_rows', []) if type(facts) is dict else []
                for row in rows if type(rows) is list else []:
                    action = row.get('action') if type(row) is dict else None
                    if action in DRIVER and valid(row, dict(action=ENUM(action), **DRIVER[action])):
                        self.store.append(row)
                    else:
                        diagnostics.append('malformed driver control sample')
                self.operation_lines.add(line)
            ownership = reads['ownership']['records']
            owners = ownership[0].get('owners', []) if len(ownership) == 1 else []
            owners = [owner for owner in owners if type(owner) is dict] if type(owners) is list else []
            sessions_observed = {owner.get('session_id') for owner in owners
                if owner.get('run_id') == self.scratch.run_id and owner.get('route_id') == self.broker.route_id
                and owner.get('host_session_id') == host_session and type(owner.get('session_id')) is str}
            if reads['ownership']['state'] == 'complete' and len(sessions_observed) == 1:
                descriptor['evidence']['session_id'] = sessions_observed.pop()
            if self.window_finished:
                identities = dict(host='host-records', ownership='ownership-observers', rows='queue',
                    attempts='attempt-observers', receipts='receipt-observers',
                    **{'queue-samples': 'sample-observers'}, contracts='contract', driver='state', held='held-fetch-response')
                final_reads = {identities.get(key, key): value for key, value in reads.items()}
                final_reads['state'] = dict(reads['driver'], lines=len(self.store.journal) + 1 + (self.observed_state is not None))
                self.store.finalize(final_reads, owners, self.observed_state)
            if self.store.journal:
                raw['driver'] = b''.join(encoded(row) for row in self.store.journal)
                reads['driver'] = checked_bytes(raw['driver'], format='jsonl')
                if operations['state'] != 'complete':
                    read_problem(reads['driver'], operations['state'], 'driver control read incomplete')
            for table in ('ownership', 'rows', 'attempts', 'receipts', 'queue-samples', 'contracts', 'injection'):
                if reads[table]['state'] == 'missing':
                    diagnostics.append(table + ': independent observer unavailable')
        else:
            for entry in descriptor['artifacts']:
                identity = entry['artifact_id']
                read, data = self.source.read(entry['file'], entry['format'])
                seal = entry['seal']
                if data is not None and any(read[key] != seal[key] for key in ('bytes', 'sha256', 'lines')):
                    read_problem(read, 'truncated', 'extent: captured seal mismatch')
                if seal['read_state'] != 'complete':
                    read_problem(read, seal['read_state'], 'seal: non-complete captured read')
                reads[identity] = read
                if data is not None:
                    raw[identity] = data
        if not frozen:
            identities = dict(host='host-records', ownership='ownership-observers', rows='queue',
                attempts='attempt-observers', receipts='receipt-observers',
                **{'queue-samples': 'sample-observers'}, contracts='contract', driver='state', held='held-fetch-response')
            for entry in descriptor['artifacts']:
                entry['artifact_id'] = identities.get(entry['artifact_id'], entry['artifact_id'])
            reads = {identities.get(key, key): value for key, value in reads.items()}
            raw = {identities.get(key, key): value for key, value in raw.items()}
        # Classification remains owned by the native evidence implementation;
        # the v1 assembler invokes these compatibility exports without filtering
        # the principal bytes supplied here.
        return self.store.snapshot(descriptor, reads, raw, cursor=cursor,
                                   diagnostics=diagnostics, controls=controls)
