"""Evidence selection, redaction, receipt-shape classification and report verdicts."""
from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re
import shutil

from host import read_jsonl, checked_read, read_problem
from redaction import RedactionContext, RedactionError, sanitize
from capture_export import export_capture
from verdict_core import evaluate, derive, ROUTE_LINE_LIMIT
from verdict_core import delivery_assertions as core_delivery_assertions

from hostdrivers.claude_evidence import (TOKEN, ATTEMPT, PEER_PREFIX, strings, intake_text, classify,
    result_text, fetch_trailer, classify_fetch, _same_fetch_trailer)


def sanitize_legacy_fixture(value, tokens=(), key="", receipt_ids=()):
    """Keep envelope keys and types; replace identifiers and local paths.

    Numeric identity fields use 1, the numeric stand-in for ID. Host prose from
    these scratch-only transcripts is retained so peer prefix checks stay real.
    """
    identity = key.lower().replace("_", "")
    if isinstance(value, dict):
        return {k: sanitize_legacy_fixture(v, tokens, k, receipt_ids) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_legacy_fixture(v, tokens, key, receipt_ids) for v in value]
    if identity == "recordid" and value in receipt_ids:
        return "ROW" + str(receipt_ids.index(value) + 1)
    if identity in {"uuid", "parentuuid", "promptid", "sessionid", "recordid", "tooluseid", "verifiedpeerpid", "verifiedpeerprocstart", "pid", "chatid", "topicid", "messageid", "userid", "id", "requestid"}:
        return 1 if isinstance(value, (int, float)) else "ID"
    if identity in {"user", "username"}:
        return "USER"
    if isinstance(value, str):
        for i, record_id in enumerate(receipt_ids):
            value = value.replace(record_id, "ROW" + str(i + 1))
        for token in sorted(set(tokens), key=len, reverse=True):
            value = value.replace(token, "TOKEN")
        value = TOKEN.sub('c3_delivery_id="TOKEN"', value)
        value = re.sub(r'\b((?:chat|message|user|attachment_file|reply_to_message|message_thread)_id|merged_message_ids|reply_to_user|user|ts)=["\'][^"\']*["\']',
                       lambda m: m[1] + '="' + ("USER" if m[1] in ("user", "reply_to_user") else "ID") + '"', value)
        value = re.sub(r'\b[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\b', "ID", value)
        value = re.sub(r'(?:/home/|/Users/|/tmp/|/private/|/run/)[^\s"<>\']+', "/work/ID", value)
        value = re.sub(r'\b(token|message_id|topic|pid|conn|record_id|file_id)=(?!["\'])[^\s]+',
                       lambda m: m[1] + "=" + ("TOKEN" if m[1] == "token" else "ID"), value)
        if identity in {"cwd", "transcriptpath", "path"}:
            return "/work/ID"
        if identity in {"token", "leasetoken", "deliverytoken"}:
            return "TOKEN"
    return value


def count_receives(records, tokens, message_ids):
    """Enqueue/remove are receipts/housekeeping, not extra user deliveries."""
    counts = Counter({str(i): 0 for i in message_ids})
    seen = set()
    for index, record in enumerate(records):
        if record.get("type") not in ("user", "attachment"):
            continue
        text = intake_text(record)
        if not any(m[1] in tokens for m in TOKEN.finditer(text)):
            continue
        identity = record.get("uuid") or f"record-{index}"
        if identity in seen:
            continue
        seen.add(identity)
        ids = re.search(r'merged_message_ids=["\']([^"\']+)', text) or re.search(r'message_id=["\']([^"\']+)', text)
        if ids:
            for message_id in set(ids[1].split(",")):
                if message_id in counts:
                    counts[message_id] += 1
    return dict(counts)


def attempt_events(log):
    events = []
    for line in log.splitlines():
        if "TEST ATTEMPT " in line:
            events.append(dict(re.findall(r'(\w+)=([^\s]+)', line)))
    return events


def notice_evidence(log, count):
    active, observed = {}, {}
    retired = 0
    false_held = False
    route_lines = 0
    injected = "TEST INJECT accepted" not in log
    for line in log.splitlines():
        if "TEST INJECT accepted" in line:
            injected = True
        for event in attempt_events(line):
            try:
                if not event.get("token") or event.get("phase") not in ("reserved", "confirmed", "failed", "expired", "released"):
                    continue
                int(event.get("members", "invalid") if event["phase"] == "reserved" else 0)
                int(event.get("retired", 0))
            except (TypeError, ValueError):
                continue
            token = event["token"]
            if event["phase"] == "reserved":
                active[token] = int(event["members"])
            else:
                active.pop(token, None)
                retired += int(event.get("retired", 0))
                if int(event.get("retired", 0)):
                    observed.pop(token, None)
        if "attempt confirmed token=" in line:
            token = re.search(r"token=([^ ]+)", line)
            if token:
                observed[token[1]] = active.get(token[1], 0)
        if "TEST SINK " in line and injected:
            if 'text="Live route:' in line or 'text="📨 Held —' in line:
                route_lines += line.count("Live route:")
            held = re.search(r'(\d+) messages? queued', line) if "Held" in line else None
            if held and int(held[1]) > max(0, count - retired - sum(active.values()) - sum(n for token, n in observed.items() if token not in active)):
                false_held = True
    return dict(no_false_held=not false_held, false_held=false_held, route_line_count=route_lines)


def route_line_limit(cell):
    return ROUTE_LINE_LIMIT


def build_verdict_inputs(cell, evidence, *, raw_observation=None, collect_only=False,
                         pinned_scenario=None, pinned_contract=None):
    """Pin the harness contract; aggregate evidence is never a complete capture."""
    is_fetch = cell.transport == 'fetch'
    scenario = dict(schema_version=1, id=cell.name, transport=cell.transport, kind=cell.kind,
                    burst=cell.burst, count=cell.count, state=cell.state, session=cell.session,
                    freshness='transcript_free_at_injection' if cell.session == 'fresh' else 'not_applicable',
                    resume_requirement='delivery_only', health_class='healthy', final_dispositions=['delivered'] * cell.count,
                    evaluation_mode='collect_only' if collect_only else 'verify',
                    run_id=evidence.get('run_id', 'unrecorded-run'), route_id=evidence.get('route_id', 'unrecorded-route'),
                    host_session_id=evidence.get('host_session_id'), session_id=evidence.get('session_id'),
                    injection_barrier_id=evidence.get('injection_barrier_id', 'injection'),
                    readiness_barrier_id='ready' if cell.state in ('startup', 'reconnect') or evidence.get('attempt_before_ready') else None,
                    final_barrier_id=evidence.get('final_barrier_id', 'final'), observation_duration_ms=60000 if is_fetch else 15000)
    contract = dict(schema_version=1, id='matrix-negotiated-v1', capability_id='matrix-v1',
                    axes=dict(negotiation='v1', live_eligibility=dict(channel=cell.transport == 'channel', inbox=cell.transport == 'inbox'),
                              receipt_type='transcript', fetch_policy='receipt',
                              accepted_modes=['fetch_receipt'] if is_fetch else [cell.transport, 'fetch_receipt']),
                    milestones=dict(live='transcript_recorded', fetch='fetch_result_recorded'),
                    timing=dict(live=dict(limit_ms=15000, basis='terminal_confirmation'), fetch=dict(limit_ms=60000, basis='terminal_confirmation')),
                    readiness_boundary='ready' if scenario['readiness_barrier_id'] else None, contract_barrier_names=['injection', 'final'])
    if (pinned_scenario is None) != (pinned_contract is None):
        raise ValueError('scenario and contract must be pinned together')
    if pinned_scenario is not None:
        scenario, contract = deepcopy(pinned_scenario), deepcopy(pinned_contract)
    if raw_observation is not None:
        observation = deepcopy(raw_observation)
    else:
        observation = dict(schema_version=1,
            provenance=dict(kind='live_capture', capture_id=cell.name, schema_version=1, extractor_version='verdict-core-v1',
                            contract_version=contract['id'], broker_build='unknown', adapter_build='unknown', host_version='unknown',
                            platform='unknown', mode='matrix', known_defect_baselines=[]),
            setup_errors=[], run_errors=[], collection_complete=False, streams=[], artifacts=[], sources=[], events=[],
            barriers=[], queue_snapshots=[], contract_observations=[], state_proof=None, session_proof=None)
        # These are explicit driver observations, not inferred delivery identities.
        descriptions = evidence.get('attempt_before_ready_observations', [])
        if evidence.get('attempt_before_ready') and descriptions:
            scope = dict(run_id=scenario['run_id'], route_id=scenario['route_id'], host_session_id=scenario['host_session_id'],
                         session_id=scenario['session_id'], connection_epoch_id=None, claim_generation=None)
            observation['artifacts'].append(dict(id='driver-observations', kind='driver', content_digest=None))
            for index, description in enumerate(descriptions, 1):
                observation['events'].append(dict(id='driver-offer-' + str(index), scope=deepcopy(scope),
                    position=dict(stream_id='driver', seq=index, clock_id=None, time_ms=None),
                    artifact_ref=dict(artifact_id='driver-observations', locator='attempt_before_ready_observations/' + str(index - 1)),
                    collection_complete=False, caused_by=[], origin='rig-control', milestone='delivery_offered', transport=cell.transport,
                    attempt_id=None, group_id=None, token=None, operation_id=None, delivery_id=None, members=[], payload=dict(description=description)))
            for identity, cutoff in ((scenario['injection_barrier_id'], 0), ('ready', len(descriptions))):
                observation['barriers'].append(dict(id=identity, name=identity, scope=deepcopy(scope), state='reached',
                    stream_cutoffs={'driver': cutoff}, artifact_refs=[dict(artifact_id='driver-observations', locator=identity)], collection_complete=False))
    observation['setup_errors'] = list(evidence.get('setup_errors', []))
    observation['run_errors'] = list(evidence.get('run_errors', []))
    return scenario, contract, observation


def verdict(cell, evidence, *, raw_observation=None, collect_only=False, full_result=False):
    result = evaluate(*build_verdict_inputs(cell, evidence, raw_observation=raw_observation, collect_only=collect_only))
    return result if full_result else result['reasons']


def delivery_assertions(cell):
    scenario, contract, _ = build_verdict_inputs(cell, {})
    return core_delivery_assertions(scenario, contract)


def capture_observation(cell, evidence, broker_read, adapter_read, records, host_read=None, *, capture_context=None):
    """Assemble checked occurrences; missing observer facts never imply completeness."""
    if capture_context is not None:
        from capture_context import assemble_context
        return assemble_context(cell, evidence, broker_read, adapter_read, records, host_read, capture_context)
    scenario, _, observation = build_verdict_inputs(cell, evidence)
    scope = dict(run_id=scenario['run_id'], route_id=scenario['route_id'], host_session_id=scenario['host_session_id'],
                 session_id=scenario['session_id'], connection_epoch_id=None, claim_generation=None)
    for identity, role, captured in (('broker', 'broker', broker_read), ('adapter', 'transport', adapter_read)):
        observation['artifacts'].append(dict(id=identity, kind=role, content_digest=hashlib.sha256(captured['text'].encode()).hexdigest()))
        observation['streams'].append(dict(id=identity, role=role, scope=deepcopy(scope), state=captured['state'],
            first_seq=1 if captured['text'] else None, last_seq=len(captured['text'].splitlines()) if captured['text'] else None,
            through_barrier_id=None, artifact_ids=[identity], detail=captured['detail'] or 'run/epoch end boundary unavailable'))
    for index, line in enumerate(broker_read['text'].splitlines(), 1):
        for attempt in attempt_events(line):
            phase = attempt.get('phase')
            if phase not in ('reserved', 'confirmed', 'failed', 'expired', 'released'):
                continue
            try:
                elapsed = int(attempt['elapsed_ms']) if 'elapsed_ms' in attempt else None
            except ValueError:
                elapsed = None
            observation['events'].append(dict(id=f'broker-line-{index}', scope=deepcopy(scope),
                position=dict(stream_id='broker', seq=index, clock_id=None, time_ms=None), artifact_ref=dict(artifact_id='broker', locator=f'line:{index}'),
                collection_complete=False, caused_by=[], origin='persistence' if phase == 'reserved' else 'retirement',
                milestone='attempt_reserved' if phase == 'reserved' else 'attempt_terminal', transport=attempt.get('transport'),
                attempt_id=None, group_id=None, token=attempt.get('token'), operation_id=None, delivery_id=None, members=[],
                payload=dict(deadline_ms=None) if phase == 'reserved' else dict(outcome=phase, elapsed_ms=elapsed, retired_members=[], reason='')))
    admission_line = next((index for index, line in enumerate(broker_read['text'].splitlines(), 1) if 'TEST INJECT accepted' in line), None)
    if admission_line is not None:
        admission_id = 'injection-line-' + str(admission_line)
        observation['events'].append(dict(id=admission_id, scope=deepcopy(scope),
            position=dict(stream_id='broker', seq=admission_line, clock_id=None, time_ms=None),
            artifact_ref=dict(artifact_id='broker', locator=f'line:{admission_line}'), collection_complete=False,
            caused_by=[], origin='rig-control', milestone='injection_completed', transport=cell.transport,
            attempt_id=None, group_id=None, token=None, operation_id=None, delivery_id=None, members=[], payload=dict(accepted=True)))
        observation['sources'] = [dict(slot=index, source_id=str(identity), kind=cell.kind, admission_event_id=admission_id)
                                  for index, identity in enumerate(evidence.get('message_ids', []))]
    if host_read is not None:
        observation['streams'].append(dict(id='host', role='host', scope=deepcopy(scope), state=host_read['state'],
            first_seq=1 if host_read['records'] else None, last_seq=len(host_read['records']) or None,
            through_barrier_id=None, artifact_ids=['host-records'], detail=host_read['detail'] or 'host end boundary unavailable'))
    seen_host = {}
    for index, record in enumerate(records, 1):
        classification = classify(record)
        if cell.transport == 'fetch' or record.get('type') not in ('user', 'attachment') or classification['accept'] is not True:
            continue
        token = TOKEN.search(intake_text(record))
        attempt = ATTEMPT.search(intake_text(record))
        record_id = record.get('uuid')
        if not isinstance(record_id, str) or not record_id:
            continue
        if record_id in seen_host:
            if seen_host[record_id] != record and host_read is not None:
                observation['streams'][-1]['state'] = 'malformed'
                observation['streams'][-1]['detail'] += f'; record:{index}: host UUID content conflict'
            continue
        seen_host[record_id] = record
        observed_scope = dict(scope, host_session_id=record.get('sessionId', record.get('session_id')))
        if not isinstance(observed_scope['host_session_id'], str):
            observed_scope['host_session_id'] = None
        observation['events'].append(dict(id='host-record-' + str(index), scope=observed_scope,
            position=dict(stream_id='host', seq=index, clock_id=None, time_ms=None),
            artifact_ref=dict(artifact_id='host-records', locator=f'record:{index}'), collection_complete=False,
            caused_by=[], origin='host-evidence', milestone='transcript_recorded', transport=classification['transport'],
            attempt_id=attempt[1] if attempt else None, group_id=None, token=token[1] if token else None,
            operation_id=None, delivery_id=record_id, members=[], payload=dict(host_record_id=record_id)))
    if cell.transport == 'fetch':
        expected = fetch_trailer(result_text((evidence.get('fetch_result') or {}).get('content')))
        if expected:
            members = [dict(row_id=member['record_id'], revision=dict(kind='exact', value=member['revision']), source_ids=[])
                       for member in expected['members']]
            trailer = dict(state='complete', token=expected['token'], members=members)
            observation['artifacts'].append(dict(id='held-fetch-response', kind='transport', content_digest=None))
            observation['events'].append(dict(id='held-fetch-response', scope=deepcopy(scope),
                position=dict(stream_id='adapter', seq=0, clock_id=None, time_ms=None),
                artifact_ref=dict(artifact_id='held-fetch-response', locator='driver/fetch_result'), collection_complete=False,
                caused_by=[], origin='rig-control', milestone='fetch_result_produced', transport='fetch',
                attempt_id=None, group_id=None, token=expected['token'], operation_id=None, delivery_id=None,
                members=deepcopy(members), payload=dict(success=True, trailer=deepcopy(trailer))))
            calls = {block.get('id') for record in records
                     for block in (record.get('message', {}).get('content', []) if isinstance(record.get('message', {}).get('content'), list) else [])
                     if isinstance(block, dict) and block.get('type') == 'tool_use' and block.get('name', '').endswith('__fetch_queue')}
            for index, record in enumerate(records, 1):
                if classify_fetch(record, expected, calls)['accept'] is not True:
                    continue
                record_id = record.get('uuid')
                if not isinstance(record_id, str) or not record_id:
                    continue
                for block_index, block in enumerate(record['message']['content']):
                    if not isinstance(block, dict) or block.get('type') != 'tool_result' or block.get('tool_use_id') not in calls:
                        continue
                    if block.get('is_error', False) is not False or not _same_fetch_trailer(fetch_trailer(result_text(block.get('content'))), expected):
                        continue
                    observation['events'].append(dict(id=f'host-result-{index}-{block_index}', scope=dict(scope, host_session_id=record.get('sessionId', record.get('session_id')) if isinstance(record.get('sessionId', record.get('session_id')), str) else None),
                        position=dict(stream_id='host', seq=index, clock_id=None, time_ms=None),
                        artifact_ref=dict(artifact_id='host-records', locator=f'record:{index}/block:{block_index}'), collection_complete=False,
                        caused_by=[], origin='host-evidence', milestone='fetch_result_recorded', transport='fetch', attempt_id=None,
                        group_id=None, token=expected['token'], operation_id=block['tool_use_id'], delivery_id=json.dumps([record_id, block_index], separators=(',', ':')),
                        members=deepcopy(members), payload=dict(host_record_id=record_id, success=True, trailer=deepcopy(trailer))))
    # Rejected or incomplete raw shapes remain extractor artifacts.
    observation['artifacts'].append(dict(id='host-records', kind='host', content_digest=hashlib.sha256(json.dumps(records, sort_keys=True).encode()).hexdigest()))
    return observation


def assemble_driver_capture(cell, host, evidence, *, profile, cursor=None):
    """Thin checked-table adapter; policy and expected inputs stay caller-owned."""
    from capture_store import checked_context
    from evidence_io import checked_bytes
    from capture_context import CaptureContext
    bundle = None
    tables = None
    try:
        tables, bundle = host.observe(cursor)
        context = checked_context(tables, bundle, live=profile.execution == 'live')
    except Exception as error:
        context = CaptureContext({}, {}, [])
        context.problem('driver-observations', 'observe', type(error).__name__ + ': ' + str(error))
    def read(table):
        entries = context.entries(table)
        if entries:
            return context.reads[entries[0]['artifact_id']]
        if bundle and tables:
            entries = [entry for entry in bundle.inventory if entry['table'] == table]
            if len(entries) == 1:
                return dict(tables.reads)[entries[0]['artifact_id']]
        result = checked_bytes(b'')
        result.update(state='missing', bytes=None, sha256=None, detail='observer artifact unavailable')
        return result
    broker, adapter, host_read = read('broker'), read('adapter'), read('host')
    observed_evidence = dict(context.descriptor.get('evidence', {}), **evidence)
    observation = capture_observation(cell, observed_evidence, broker, adapter, host_read['records'], host_read,
                                      capture_context=context)
    inputs = build_verdict_inputs(cell, observed_evidence, raw_observation=observation,
                                 pinned_scenario=profile.scenario, pinned_contract=profile.contract)
    parity = None
    if profile.execution == 'live':
        from capture_context import host_shape_valid
        safe_records = [record for record in host_read['records'] if host_shape_valid(record)]
        baseline = capture_observation(cell, observed_evidence, broker, adapter, safe_records, host_read)
        baseline_inputs = build_verdict_inputs(cell, observed_evidence, raw_observation=baseline,
                                              pinned_scenario=profile.scenario, pinned_contract=profile.contract)
        legacy, current = evaluate(*baseline_inputs), evaluate(*inputs)
        parity = dict(matches=legacy == current, legacy=legacy, driver=current)
    return dict(parity=parity, inputs=inputs, context=context, bundle=bundle, evidence=observed_evidence,
                classifications=deepcopy(context.decisions), diagnostics=deepcopy(context.diagnostics),
                reads=dict(tables.reads) if tables else {}, verdict=evaluate(*inputs), broker_read=broker, adapter_read=adapter, host_read=host_read)


def collect(cell, host, root, output, evidence, collect_only=False, capture_profile=None):
    from capture_store import DriverCapture
    if isinstance(host, DriverCapture):
        host, capture_profile = host.driver, host.profile
    output.mkdir(parents=True, exist_ok=True)
    capture = assemble_driver_capture(cell, host, evidence, profile=capture_profile) if capture_profile is not None else None
    broker_read = capture['broker_read'] if capture else checked_read(root / "broker/broker.log")
    adapter_read = capture["adapter_read"] if capture else checked_read(root / "control/adapter.log")
    broker_log, adapter_log = broker_read["text"], adapter_read["text"]
    attempts = attempt_events(broker_log)
    tokens = {e["token"] for e in attempts if e.get("token")}
    try:
        records = capture["host_read"]["records"] if capture else host.records() if host else []
    except Exception as exc:
        records = []
        evidence.setdefault("setup_errors", []).append(f"collection: {type(exc).__name__}: {exc}")
    host_read = capture["host_read"] if capture else None
    if not capture and host and not evidence.get('setup_errors'):
        try:
            transcript = host.transcript()
            host_read = checked_read(transcript, jsonl=True) if transcript is not None else dict(state='missing', records=[], text='', detail='transcript unavailable')
            records = host_read['records']
        except Exception as exc:
            evidence.setdefault('setup_errors', []).append(f"collection: {type(exc).__name__}: {exc}")
    from capture_context import host_shape_valid, broker_records
    for line, kind, record in broker_records(broker_log):
        if record is None:
            read_problem(broker_read, 'malformed', f'line:{line}: malformed recognized TEST {kind} record')
    valid_records = []
    for index, record in enumerate(records, 1):
        if host_shape_valid(record):
            valid_records.append(record)
        elif host_read is not None:
            read_problem(host_read, 'malformed', f'line:{index}: malformed recognized host record')
    records = valid_records
    tokens.update(match[1] for r in records for text in strings(r) for match in TOKEN.finditer(text))
    selected = [r for r in records if any(token in text for text in strings(r) for token in tokens)
                or (cell.transport == "fetch" and ("MATRIX_SAMPLE" in json.dumps(r) or "__fetch_queue" in json.dumps(r)))]
    fetch_calls = {b.get("id") for r in records for b in (r.get("message", {}).get("content", []) if isinstance(r.get("message", {}).get("content"), list) else [])
                   if isinstance(b, dict) and b.get("type") == "tool_use" and b.get("name", "").endswith("__fetch_queue")}
    expected = fetch_trailer(result_text((evidence.get("fetch_result") or {}).get("content")))
    receipt_ids = [m["record_id"] for m in expected["members"]] if expected else []
    if expected:
        tokens.add(expected["token"])
    expectations = [classify_fetch(record, expected, fetch_calls) if cell.transport == "fetch" else classify(record)
                    for record in selected]
    events = [] if capture else host.events() if host else []
    if capture and capture['bundle']:
        from evidence_io import checked_bytes
        events = [row for ref, data in capture['bundle'].artifacts if ref.artifact_id == 'proxy'
                  for row in checked_bytes(data, format='jsonl')['records']]
    evidence.update(notice_evidence(broker_log, cell.count))
    evidence["route_line_limit"] = route_line_limit(cell)
    evidence["attempts"] = attempts
    evidence["received"] = count_receives(records, tokens, evidence.get("message_ids", []))
    fetch_results = [b for r in records if r.get("type") == "user"
                     for b in (r.get("message", {}).get("content", []) if isinstance(r.get("message", {}).get("content"), list) else [])
                     if isinstance(b, dict) and b.get("type") == "tool_result" and not b.get("is_error")
                     and b.get("tool_use_id") in fetch_calls and "MATRIX_SAMPLE" in json.dumps(b.get("content"))]
    evidence["fetch_tool_result"] = bool(fetch_results)
    evidence["fetch_source_occurrences"] = sum(json.dumps(b.get("content")).count("MATRIX_SAMPLE") for b in fetch_results)
    evidence["fetch_token"] = bool(expected)
    evidence["fetch_trailer_complete"] = any(classify_fetch(r, expected, fetch_calls)["accept"] for r in records) if expected else False
    raw_observation = capture['inputs'][2] if capture else capture_observation(cell, evidence, broker_read, adapter_read, records, host_read)
    inputs = capture['inputs'] if capture else build_verdict_inputs(cell, evidence, raw_observation=raw_observation, collect_only=collect_only)
    if evidence.get("setup_errors"):
        evaluation = evaluate(*inputs)
    else:
        evaluation = evaluate(*inputs)
        projections = derive(*inputs)
        for key in ("injected", "received", "rows_final", "fetch_tool_result", "fetch_source_occurrences", "fetch_token",
                    "fetch_trailer_complete", "no_false_held", "false_held", "route_line_count"):
            evidence[key] = projections[key]
    result = {"cell": cell.name, "status": evaluation["status"], "reasons": evaluation["reasons"],
              "evidence": evidence,
              "todo_records": sum(e["accept"] is None for e in expectations), "not_evaluated": evaluation["not_evaluated"]}
    if capture:
        result['collection_diagnostics'] = capture['diagnostics']
    if capture and capture['parity'] is not None:
        result['capture_parity'] = capture['parity']
    # A setup failure may still have a complete or damaged transcript to export.
    # This extra final read is for the artifact; it does not change the assembler.
    export_host_read = host_read
    if host and export_host_read is None:
        try:
            transcript = host.transcript()
            if isinstance(transcript, Path):
                export_host_read = checked_read(transcript, jsonl=True)
        except Exception:
            export_host_read = dict(state='unreadable', text='', records=[], detail='transcript unavailable during export')
    result, context = export_capture(output, root, checked_read, broker_read, adapter_read, records, export_host_read, events,
                                     inputs, result, tokens=tokens, receipt_ids=receipt_ids, driver_capture=capture)
    # Compatibility output is deliberately separate from the complete capture.
    # Rejected trailer candidates also contain private tokens/rows. Add their
    # hints only after legacy selection, preserving the old selection semantics.
    legacy_tokens, legacy_rows = set(tokens), list(receipt_ids)
    for record in selected:
        for text in strings(record):
            legacy_tokens.update(re.findall(r'^group ([^\n]+)', text, re.M))
            for row in re.findall(r'^member (\S+)', text, re.M):
                if row not in legacy_rows:
                    legacy_rows.append(row)
    legacy_records = [sanitize_legacy_fixture(record, legacy_tokens, receipt_ids=legacy_rows) for record in selected]
    legacy_expectations = [sanitize_legacy_fixture(expectation, legacy_tokens, receipt_ids=legacy_rows) for expectation in expectations]
    collapsed_negative = cell.transport == 'fetch' and any(
        expectation['accept'] is False and classify_fetch(record, expectation, {'ID'})['accept'] is True
        for record, expectation in zip(legacy_records, legacy_expectations))
    refusal = 'collapse changes a rejected fetch correlation' if collapsed_negative else ''
    try:
        context.check_legacy_fixture(legacy_records, schema='host')
        context.check_legacy_fixture(legacy_expectations)
        legacy_record_text = ''.join(json.dumps(record, ensure_ascii=False) + '\n' for record in legacy_records)
        legacy_expectation_text = json.dumps(legacy_expectations, indent=2) + '\n'
        context.check_export_text(legacy_record_text, format='jsonl', schema='host', legacy_fixture=True)
        context.check_export_text(legacy_expectation_text, format='json', legacy_fixture=True)
    except RedactionError:
        refusal = 'private identity unsupported by legacy renderer'
    if refusal:
        (output / 'legacy-fixture-refused.txt').write_text('legacy fixture export refused: ' + refusal + '\n')
    else:
        (output / "records.jsonl").write_text(legacy_record_text)
        (output / "records.expect.json").write_text(legacy_expectation_text)
    return result


def export_fixtures(output, repo, version, cell):
    if (output / 'legacy-fixture-refused.txt').exists():
        raise ValueError('legacy fixture export refused: unsafe redaction or collapse changes a rejected fetch correlation; replay bundle retained')
    target = repo / "cmd/c3-claude-adapter/testdata" / ("claude-" + version)
    pair = [(output / source, target / (cell.name + suffix))
            for source, suffix in (("records.jsonl", ".jsonl"), ("records.expect.json", ".expect.json"))]
    for source, dest in pair:
        if dest.exists():
            raise FileExistsError(f"refusing to overwrite fixture {dest.name}")
        if not source.is_file():
            raise FileNotFoundError('legacy fixture pair is incomplete')
    target.mkdir(exist_ok=True)
    for source, dest in pair:
        shutil.copyfile(source, dest)
