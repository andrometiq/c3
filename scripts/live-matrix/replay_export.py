"""Identity-preserving export of checked observer captures.

Uses the Phase 2A sanitizer unchanged. Extents describe the new representation;
failed reads stay failed. The returned canonical input file is for path B, while
all other files are independently loadable raw observer evidence for path C.
"""
from copy import deepcopy
import hashlib
import json
import re

from capture_export import Artifact, json_text
from redaction import ATTEMPT_LABEL, RedactionContext, sanitize


def attempt_label_keys(value, *, restore=False):
    """Render extracted attempt labels in their existing literal grammar.

    The Phase 2A sanitizer already preserves c3_attempt/attempt labels. The
    assembler now uses that same identity in canonical attempt_id positions.
    Apply this alias only to canonical inputs and closed observer tables;
    opaque fetch attempt IDs still use the ordinary attempt pseudonym domain.
    """
    if type(value) is list:
        return [attempt_label_keys(v, restore=restore) for v in value]
    if type(value) is not dict:
        return value
    result = {k: attempt_label_keys(v, restore=restore) for k, v in value.items()}
    source, target = ('attempt', 'attempt_id') if restore else ('attempt_id', 'attempt')
    label = result.get(source)
    if type(label) is str and ATTEMPT_LABEL.fullmatch(label) and target not in result:
        result[target] = result.pop(source)
    return result


def export_replay_capture(replay, destination, *, redaction_context=None, write=True):
    capture = replay['context']
    descriptor = deepcopy(replay.get('descriptor', capture.descriptor))
    scenario, contract, observation = replay['inputs']
    inputs = attempt_label_keys(dict(scenario=scenario, contract=contract, observation=observation))
    context = redaction_context if redaction_context is not None else RedactionContext()
    # Filenames and extent hashes are representation metadata, not private IDs.
    identity_descriptor = {key:value for key,value in descriptor.items() if key != 'artifacts'}
    identity_inventory = [{key:value for key,value in entry.items() if key not in ('file','seal')} for entry in descriptor['artifacts']]
    context.discover(identity_descriptor)
    context.discover(identity_inventory)
    artifacts = []
    order = {'ownership':0, 'rows':1, 'attempts':2, 'receipts':3, 'queue-samples':4, 'contracts':5, 'driver':6,
             'injection':7, 'broker':8, 'adapter':9, 'host':10, 'held':11}
    for entry in sorted(capture.inventory, key=lambda item: order[item['table']]):
        read = capture.reads[entry['artifact_id']]
        schema = {'host':'host', 'broker':'broker', 'adapter':'adapter', 'held':'ipc'}.get(entry['table'], 'generic')
        if entry['format'] == 'json':
            value = read['records'][0] if read['records'] else None
            artifact = None
        else:
            artifact = Artifact(entry['file'], read, schema, jsonl=entry['format']=='jsonl')
            if entry['table'] in ('attempts', 'receipts'):
                artifact.value = attempt_label_keys(artifact.value)
            value = artifact.value
        artifacts.append((entry, read, schema, artifact, value))
        context.discover(value, schema=schema)
    context.discover(inputs, schema='canonical')
    context.discover(replay['evidence'])
    context.discover(replay['diagnostics'])
    context.discover(replay['classifications'])
    context.seal()
    clean_inputs = attempt_label_keys(sanitize(inputs, context=context, schema='canonical'), restore=True)
    clean_descriptor = sanitize(identity_descriptor, context=context)
    inventory = sanitize(identity_inventory, context=context)
    clean_descriptor['artifacts'] = [dict(entry, file=original['file'], seal=deepcopy(original['seal']))
                                     for entry, original in zip(inventory, descriptor['artifacts'])]
    rendered = {}
    invalid_bytes = set()
    for entry, read, schema, artifact, value in artifacts:
        if read['state'] == 'missing':
            continue
        invalid_utf8 = next((re.fullmatch(r'byte:(\d+): invalid UTF-8', problem) for problem in read['problems']
                             if re.fullmatch(r'byte:(\d+): invalid UTF-8', problem)), None)
        if invalid_utf8:
            data = b' ' * int(invalid_utf8[1]) + b'\xff\n'  # Preserve the failure locator without original bytes.
            invalid_bytes.add(entry['file'])
        elif entry['format'] == 'json':
            data = (json_text(sanitize(value, context=context, schema=schema)) if value is not None else 'REDACTED malformed record\n').encode()
        else:
            data = artifact.render(context).encode()
            if entry['table'] in ('attempts', 'receipts'):
                lines = []
                for raw_line in data.decode().splitlines(keepends=True):
                    try:
                        value = attempt_label_keys(json.loads(raw_line), restore=True)
                        raw_line = json.dumps(value, ensure_ascii=False, sort_keys=True) + ('\n' if raw_line.endswith('\n') else '')
                    except ValueError:
                        pass  # Artifact.render's invalid stub remains invalid.
                    lines.append(raw_line)
                data = ''.join(lines).encode()
        rendered[entry['file']] = data
        clean_entry = next(item for item in clean_descriptor['artifacts'] if item['file'] == entry['file'])
        try:
            lines = 1 if entry['format']=='json' else len(data.decode('utf-8').splitlines())
        except UnicodeDecodeError:
            lines = 0
        seal = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest(), lines=lines, read_state=entry['seal']['read_state'])
        if 'extent: artifact shorter than captured seal' in read['problems']:
            seal['bytes'] += 1
        if 'extent: captured seal mismatch' in read['problems']:
            seal['sha256'] = '0' * 64
        clean_entry['seal'] = seal
    # Notice names encode a relation ("notice/" + event ID) in the core schema.
    # Rebuild that derived spelling using the mapped event ID, retaining the
    # independently mapped barrier and snapshot identities.
    for notice in observation['events']:
        if notice['milestone'] != 'notice_emitted':
            continue
        raw_name = 'notice/' + notice['id']
        for old, clean in zip(observation['barriers'], clean_inputs['observation']['barriers']):
            if old['name'] == raw_name:
                clean['name'] = 'notice/' + context.pseudonym('event', notice['id'])
    rendered['summary.json'] = json_text(sanitize(replay['evidence'], context=context)).encode()
    rendered['capture.json'] = json_text(clean_descriptor).encode()
    rendered['verdict-inputs.json'] = json_text(clean_inputs).encode()
    for name, data in rendered.items():
        if name not in invalid_bytes:
            entry = next((entry for entry in descriptor['artifacts'] if entry['file']==name), None)
            if entry and capture.reads[entry['artifact_id']]['state'] not in ('complete',):
                continue  # Artifact.render audits valid records; invalid stubs carry no raw bytes.
            format = entry['format'] if entry else 'json'
            schema = {'host':'host','broker':'broker','adapter':'adapter','held':'ipc'}.get(entry['table'], 'generic') if entry else 'canonical'
            context.check_export_text(data.decode(), format='text' if format=='log' else format, schema=schema)
    if write:
        destination.mkdir(parents=True, exist_ok=True)
        for name, data in rendered.items():
            path = destination / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
    return dict(rendered=rendered, inputs=clean_inputs, diagnostics=sanitize(replay['diagnostics'], context=context),
                classifications=sanitize(replay['classifications'], context=context))
