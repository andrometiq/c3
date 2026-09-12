"""Export-only capture bundle. All public bytes are prepared before any writes."""
import hashlib
import json
from pathlib import Path

from redaction import RedactionContext, sanitize


CONTROL_FILES = ('pane.txt', 'events.jsonl', 'sessions.jsonl', 'adapter.log')


def json_text(value):
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + '\n'


class Artifact:
    def __init__(self, name, captured, schema, *, jsonl=False):
        self.name, self.schema, self.jsonl = name, schema, jsonl
        self.state, self.detail = captured['state'], captured['detail']
        if schema in ('broker', 'adapter') and self.state == 'complete' and captured['text'] and not captured['text'].endswith('\n'):
            self.state, self.detail = 'partial', 'unterminated log tail'
        self.present = self.state != 'missing'
        self.entries = []
        if jsonl:
            # Keep physical order and final-newline status, including rejected
            # records and duplicates. Unsafe lines become invalid JSON stubs.
            for line in captured['text'].splitlines(keepends=True):
                newline = '\n' if line.endswith('\n') else ''
                try:
                    record = json.loads(line)
                    if type(record) is not dict:
                        raise ValueError()
                except ValueError:
                    record = None
                self.entries.append((record, newline))
            self.value = [record for record, _ in self.entries if record is not None]
        else:
            self.value = captured['text']
        if self.state in ('unreadable', 'malformed') and not captured['text']:
            self.value = ''

    def render(self, context):
        if not self.present:
            return None
        if self.jsonl:
            clean = iter(sanitize(self.value, context=context, schema=self.schema))
            text = ''.join((json.dumps(next(clean), ensure_ascii=False, sort_keys=True) if record is not None
                            else 'REDACTED malformed record') + newline for record, newline in self.entries)
        else:
            text = sanitize(self.value, context=context, schema=self.schema)
        if not text and self.state in ('unreadable', 'malformed'):
            text = 'REDACTED unavailable artifact\n'
        return text


def capture_controls(root, checked_read):
    """Read only the allowlisted scratch controls and contained JSONL transcripts."""
    artifacts = []
    for name in CONTROL_FILES:
        if name == 'adapter.log':
            continue  # Already captured as a principal root.
        schema = 'session_hook' if name == 'sessions.jsonl' else 'generic'
        artifacts.append(Artifact('control/' + name, checked_read(root / 'control' / name, jsonl=name.endswith('.jsonl')),
                                  schema, jsonl=name.endswith('.jsonl')))
    sessions = next(artifact for artifact in artifacts if artifact.name == 'control/sessions.jsonl')
    for index, session in enumerate(sessions.value):
        raw_path = session.get('transcript_path')
        if type(raw_path) is not str or not raw_path:
            continue
        path = Path(raw_path)
        # Resolve symlinks as well as '..'; never read outside this scratch root.
        if path.suffix != '.jsonl' or not path.resolve().is_relative_to(root.resolve()):
            continue
        artifacts.append(Artifact(f'control/transcript-{index}.jsonl', checked_read(path, jsonl=True), 'host', jsonl=True))
    return sorted(artifacts, key=lambda artifact: artifact.name)


def export_capture(output, root, checked_read, broker_read, adapter_read, records, host_read, events,
                   inputs, report, *, tokens=(), receipt_ids=()):
    scenario, contract, observation = inputs
    controls = capture_controls(root, checked_read)
    broker = Artifact('broker.log', broker_read, 'broker')
    adapter = Artifact('adapter.log', adapter_read, 'adapter')
    if host_read is None:
        host_read = dict(state='missing' if not records else 'complete', detail='transcript unavailable' if not records else '',
                         text=''.join(json.dumps(record) + '\n' for record in records))
    host = Artifact('records.jsonl', host_read, 'host', jsonl=True)
    held_responses = [event['frame'] for event in events if event.get('event') == 'fetch_result_waiting'
                      and type(event.get('frame')) is dict]
    # Older driver evidence only retained result.content. Preserve that original
    # shape when a complete proxy frame is unavailable; do not invent RPC fields.
    if not held_responses and report['evidence'].get('fetch_result') is not None:
        held_responses = [report['evidence']['fetch_result']]
    verdict_inputs = dict(scenario=scenario, contract=contract, observation=observation)
    artifacts = [broker, adapter, host, *controls]
    descriptor = dict(schema_version=1, redaction_version='identity-v1', capture_id=scenario['id'],
                      scenario=scenario, context=[],
                      artifacts=[dict(path=artifact.name, state=artifact.state, detail=artifact.detail) for artifact in artifacts],
                      held_fetch=[f'held-fetch/response-{index}.json' for index in range(1, len(held_responses) + 1)])
    # Descriptor artifact filenames are public locators, not local path identities.
    # The scenario is discovered separately; descriptor metadata is constructed by
    # the exporter and contains no raw identity except capture_id and scenario.
    identity_descriptor = dict(capture_id=descriptor['capture_id'], scenario=scenario)
    context = RedactionContext(tokens=tokens, receipt_ids=receipt_ids)
    context.discover(identity_descriptor)
    context.discover(broker.value, schema=broker.schema)
    context.discover(adapter.value, schema=adapter.schema)
    context.discover(host.value, schema=host.schema)
    for held in held_responses:
        context.discover(held, schema='ipc')
    for artifact in controls:
        context.discover(artifact.value, schema=artifact.schema)
    context.discover(events)
    context.discover(verdict_inputs, schema='canonical')
    context.discover(observation, schema='canonical')
    context.discover(report)
    # Include every detail in discovery; diagnostics from checked_read are safe,
    # but callers must never be able to bypass redaction with an error string.
    context.discover([artifact.detail for artifact in artifacts])
    context.seal()
    descriptor['redaction_diagnostics'] = context.diagnostics
    clean_inputs = sanitize(verdict_inputs, context=context, schema='canonical')
    clean_report = sanitize(report, context=context)
    descriptor.update(sanitize(identity_descriptor, context=context))
    rendered = {}
    for artifact, entry in zip(artifacts, descriptor['artifacts']):
        text = artifact.render(context)
        entry['detail'] = sanitize(artifact.detail, context=context)
        entry['sha256'] = hashlib.sha256(text.encode()).hexdigest() if text is not None else None
        entry['bytes'] = len(text.encode()) if text is not None else None
        if text is not None:
            rendered['replay/' + artifact.name] = text
    for index, held in enumerate(held_responses, 1):
        rendered[f'replay/held-fetch/response-{index}.json'] = json_text(sanitize(held, context=context, schema='ipc'))
    rendered['replay/capture.json'] = json_text(descriptor)
    rendered['replay/verdict-inputs.json'] = json_text(clean_inputs)
    rendered['observation.json'] = json_text(sanitize(observation, context=context, schema='canonical'))
    rendered['summary.json'] = json_text(clean_report)
    for name in ('broker.log', 'adapter.log'):
        if 'replay/' + name in rendered:
            rendered[name] = rendered['replay/' + name]
    # Preserve the top-level events interface even when a host only offers its
    # in-memory event stream. Raw failure cleanup can never replace this file.
    rendered['events.jsonl'] = rendered.get('replay/control/events.jsonl', ''.join(
        json.dumps(event, sort_keys=True) + '\n' for event in sanitize(events, context=context)))
    schemas = {'replay/' + artifact.name: artifact.schema for artifact in artifacts}
    schemas.update({'broker.log': 'broker', 'adapter.log': 'adapter'})
    # Gate the actual serialized files, including exporter-built metadata, as
    # one transaction. A failed audit must leave no partially published capture.
    for name, text in rendered.items():
        format = 'jsonl' if name.endswith('.jsonl') else 'json' if name.endswith('.json') else 'text'
        context.check_export_text(text, format=format, schema=schemas.get(name, 'generic'))
    output.mkdir(parents=True, exist_ok=True)
    for directory in ('replay/context', 'replay/held-fetch', 'replay/control'):
        (output / directory).mkdir(parents=True, exist_ok=True)
    for name, text in rendered.items():
        (output / name).write_text(text)
    return clean_report, context


def save_sanitized_failure_evidence(destination):
    """Project already-rendered controls; never reopen private scratch artifacts."""
    source = destination / 'replay/control'
    if not source.is_dir():
        return
    names = [name for name in CONTROL_FILES if name != 'adapter.log']
    names += sorted(path.name for path in source.glob('transcript-*.jsonl'))
    for name in names:
        candidate = source / name
        if candidate.is_file() and not (destination / name).exists():
            # Exclusive creation also guards against an existing sanitized file.
            with (destination / name).open('x') as target:
                target.write(candidate.read_text())
