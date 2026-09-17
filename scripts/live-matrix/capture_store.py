"""Common artifact snapshots, append integrity, and final capture packaging."""
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path

from evidence_io import checked_bytes, checked_read, read_problem
from hostdriver import ArtifactBundle, ArtifactRef, Extent, ObservationCursor, ObserverTables
from capture_context import _load_context, contained_file


def encoded(value):
    return (json.dumps(value, sort_keys=True) + '\n').encode()


@dataclass(frozen=True)
class DriverCapture:
    """Trusted collection binding, kept separate from aggregate evidence."""
    driver: object
    profile: object


class ArtifactSource:
    """Read-only source. Each successful read owns the exact bytes it parsed."""
    def __init__(self, root):
        self.root = Path(root)

    def read(self, name, format='log'):
        if not contained_file(self.root, name):
            read = checked_bytes(b'', format=format)
            read_problem(read, 'unreadable', 'artifact path escapes capture root')
            return read, None
        path = self.root / name
        try:
            data = path.read_bytes()
        except OSError:
            return checked_read(path, format=format), None
        return checked_bytes(data, format=format), data


class CaptureStore:
    """One writer per generated table; snapshots never renumber earlier rows."""
    def __init__(self, instance_id, run_id):
        self.instance_id, self.run_id = instance_id, run_id
        self.previous = {}
        self.cursors = {}
        self.failures = {}
        self.history = []
        self.supporting = {}
        self.supporting_failures = set()
        self.journal = []
        self.finalized = False

    def append(self, row):
        if self.finalized:
            raise ValueError('capture journal is finalized')
        self.journal.append(deepcopy(row))

    def finalize(self, reads, ownership, observed_state=None):
        if self.finalized:
            return
        # Only streams with actual ownership can participate in a cut.
        cuts = [dict(stream_id=owner['stream_id'], seq=reads[owner['artifact_id']]['lines'])
                for owner in ownership if owner.get('artifact_id') in reads and owner.get('stream_id')]
        self.append(dict(action='cut', barrier_id='final', cuts=cuts))
        if observed_state is not None:
            self.append(dict(action='window_end', observed_state=observed_state))
        self.finalized = True

    def snapshot(self, descriptor, reads, raw, *, cursor=None, diagnostics=(), controls=()):
        if cursor is not None and (not isinstance(cursor, ObservationCursor) or
                type(cursor.schema_version) is not int or cursor.schema_version != 1 or cursor.driver_instance_id != self.instance_id or
                cursor.run_id != self.run_id or self.cursors.get(cursor.snapshot_id) != cursor):
            raise ValueError('foreign or altered observation cursor')
        reads, descriptor = deepcopy(reads), deepcopy(descriptor)
        for identity, data in raw.items():
            previous = self.previous.get(identity)
            if previous is not None and not data.startswith(previous):
                self.failures[identity] = 'extent: previously observed bytes changed or shrank'
                entry = next(entry for entry in descriptor['artifacts'] if entry['artifact_id'] == identity)
                suffix = entry['format']
                self.history.append((ArtifactRef('prior-' + identity, f'control/prior-{len(self.history) + 1}.' + suffix), previous))
            self.previous[identity] = data
        for identity in self.previous.keys() - raw.keys():
            self.failures[identity] = 'extent: previously observed artifact disappeared'
        for identity, detail in self.failures.items():
            if identity in reads:
                read_problem(reads[identity], 'truncated', detail)
        for entry in descriptor['artifacts']:
            read = reads[entry['artifact_id']]
            entry['seal'] = dict(bytes=read['bytes'] or 0, sha256=read['sha256'] or hashlib.sha256(b'').hexdigest(),
                                 lines=read['lines'], read_state=read['state'])
        present_controls = {ref.locator for ref, _ in controls if ref.artifact_id != 'pane'}
        for name in self.supporting.keys() - present_controls:
            self.supporting_failures.add(name + ': previously observed supporting artifact disappeared')
        for ref, data in controls:
            if ref.artifact_id == 'pane':
                continue  # A pane is a replaceable screenshot, not an append log.
            previous = self.supporting.get(ref.locator)
            if previous is not None and not data.startswith(previous):
                self.supporting_failures.add(ref.locator + ': previously observed supporting bytes changed or shrank')
                suffix = Path(ref.locator).suffix
                self.history.append((ArtifactRef('prior-' + ref.artifact_id, f'control/prior-{len(self.history) + 1}' + suffix), previous))
            self.supporting[ref.locator] = data
        diagnostics = (*diagnostics, *sorted(self.supporting_failures))
        controls = (*controls, *self.history)
        control_digests = [(ref.artifact_id, ref.locator, hashlib.sha256(data).hexdigest()) for ref, data in controls]
        digest = hashlib.sha256(encoded(descriptor) + encoded(reads) + encoded(control_digests)).hexdigest()
        extents = tuple(Extent(identity, read['bytes'], read['lines'], read['sha256'])
                        for identity, read in sorted(reads.items()) if read['bytes'] is not None)
        next_cursor = ObservationCursor(driver_instance_id=self.instance_id, run_id=self.run_id,
                                        snapshot_id=digest, extents=extents)
        self.cursors[digest] = next_cursor
        artifacts = list(controls)
        artifacts.append((ArtifactRef('capture-descriptor', 'capture.json'), encoded(descriptor)))
        artifacts.extend((ArtifactRef(entry['artifact_id'], entry['file']), raw[entry['artifact_id']])
                         for entry in descriptor['artifacts'] if entry['artifact_id'] in raw)
        problems = list(diagnostics)
        problems.extend(f'{identity}: {read["state"]}: {read["detail"]}'
                        for identity, read in reads.items() if read['state'] != 'complete')
        return ObserverTables(reads=tuple(reads.items())), ArtifactBundle(self.run_id, digest, next_cursor,
            provenance=tuple(descriptor['provenance'].items()), inventory=tuple(descriptor['artifacts']),
            artifacts=tuple(artifacts), control_refs=tuple(ref for ref, _ in controls), diagnostics=tuple(problems))


def checked_context(tables, bundle, *, live=False):
    """Validate the sealed snapshot with the existing v1 loader, without rereads."""
    root = Path('/capture')
    artifacts = {}
    for reference, data in bundle.artifacts:
        if not contained_file(root, reference.locator) or reference.locator in artifacts:
            raise ValueError('invalid or duplicate snapshot artifact path')
        artifacts[reference.locator] = data
    def read_snapshot(path, *, format):
        data = artifacts.get(str(path.relative_to(root)))
        if data is not None:
            return checked_bytes(data, format=format)
        read = checked_bytes(b'')
        read.update(bytes=None, sha256=None)
        read_problem(read, 'missing', 'file missing')
        return read
    context = _load_context(root, live=live, reader=read_snapshot)
    supplied = dict(tables.reads)
    for identity, read in context.reads.items():
        original = supplied.get(identity)
        if original is None:
            context.problem(identity, 'read', 'snapshot checked read unavailable')
        elif any(read[key] != original[key] for key in ('bytes', 'sha256', 'records', 'record_lines')):
            context.problem(identity, 'read', 'snapshot bytes disagree with producer read', malformed=True)
        elif original['state'] != 'complete':
            context.reads[identity] = deepcopy(original)
    for detail in bundle.diagnostics:
        context.problem('driver-observations', 'producer', detail)
    return context


def record_injection(root, response, kind):
    """The injector owns the request/result; final reads join actual admissions."""
    path = root / 'control/injection.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as output:
        output.write(encoded(dict(response=response, kind=kind)))


def injection_table(request, broker):
    from capture_context import broker_records
    if request['state'] != 'complete' or len(request['records']) != 1:
        return None
    request = request['records'][0]
    response = request.get('response')
    if (type(response) is not dict or response.get('accepted') is not True or
            type(response.get('message_ids')) is not list or
            any(type(value) is not int for value in response['message_ids']) or
            request.get('kind') not in ('text', 'voice', 'photo')):
        return None
    admissions = list(broker_records(broker['text']))
    lines = []
    for source in response['message_ids']:
        matches = [line for line, kind, row in admissions if kind == 'INJECT' and row is not None
                   and row['message_id'] == source and row['kind'] == request['kind']]
        if len(matches) != 1:
            return None
        lines.extend(matches)
    return dict(message_ids=response['message_ids'], kind=request['kind'], broker_lines=lines)
