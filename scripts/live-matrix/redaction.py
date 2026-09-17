"""Two-pass, per-capture identity redaction for the rig's registered grammars.

Discover roots in capture order, then seal once and render. Numeric candidates
are 1, 2, ... (negative originals use -1, -2, ...); zero is an identity, not a
sentinel. Integral finite floats up to 2**53 are supported and keep their type.
Decimal strings alias integers only in the numeric wire domains below. RPC IDs
retain the JSON distinction between strings and numbers.
"""
from dataclasses import dataclass
import ast
import json
import math
import re
import warnings

from matrix import cells


class RedactionError(ValueError):
    """Messages contain only generated locators and fixed diagnostic text."""


@dataclass(frozen=True)
class Domain:
    prefix: str
    aliases: tuple
    numeric: bool = False


REGISTRY = {
    'token': Domain('TOKEN', ('token', 'lease_token', 'delivery_token', 'c3_delivery_id')),
    'row': Domain('ROW', ('row_id', 'record_id', '_c3_queue_id', '_c3_drained_record_id', 'row_ref', 'row_ids')),
    'source': Domain('SOURCE', ('source_id', 'source_ids', 'message_ids', 'MessageID', 'message_id',
                              'merged_message_ids', 'reply_to_message_id', 'SourceMessageID', 'msg'), True),
    'revision': Domain('', ('revision', 'row_version', 'revision_ref')),
    'host_session': Domain('HOSTSESSION', ('sessionId', 'host_session_id', 'host_session_ref')),
    'session': Domain('SESSION', ('session_id', 'previous_session_id', 'session', 'SessionID')),
    'epoch': Domain('EPOCH', ('connection_epoch_id', 'connection_lifetime_id', 'connection_ref', 'conn', 'old-conn', 'new-conn'), True),
    'attempt': Domain('ATTEMPT', ('attempt_id', 'attempt_ref')),
    'group': Domain('GROUP', ('group_id', 'group_ref')),
    'call': Domain('CALL', ('operation_id', 'tool_use_id', 'call_id', 'request_id', 'response_id')),
    'host_record': Domain('ID', ('uuid', 'parentUuid', 'source_uuid', 'promptId', 'host_record_id')),
    'delivery': Domain('DELIVERY', ('delivery_id', 'delivery_ref')),
    'run': Domain('RUN', ('run_id', 'run_ref')),
    'capture': Domain('CAPTURE', ('capture_id', 'capture_ref')),
    'route': Domain('ROUTE', ('route_id', 'route', 'route_ref')),
    'chat': Domain('CHAT', ('chat_id', 'ChatID', 'chat'), True),
    'topic': Domain('TOPIC', ('topic_id', 'TopicID', 'topic', 'message_thread_id'), True),
    'user_id': Domain('USERID', ('user_id', 'UserID', 'sender_id'), True),
    'user': Domain('USER', ('user', 'username', 'Username', 'reply_to_user')),
    'process': Domain('PID', ('pid', 'PID', 'verifiedPeerPid', 'process_id', 'process_ref'), True),
    'process_start': Domain('PROCSTART', ('verifiedPeerProcStart', 'process_start', 'process_start_ref'), True),
    'file': Domain('FILE', ('file_id', 'attachment_file_id', 'FileID', 'file_ref')),
    'path': Domain('/work/PATH', ('cwd', 'CWD', 'path', 'transcript_path', 'transcriptPath', 'local_path')),
    'event': Domain('EVENT', ('event_id', 'event_ids', 'caused_by', 'admission_event_id')),
    'artifact': Domain('ARTIFACT', ('artifact_id', 'artifact_ids')),
    'stream': Domain('STREAM', ('stream_id', 'stream_ids')),
    'barrier': Domain('BARRIER', ('barrier_id', 'injection_barrier_id', 'readiness_barrier_id', 'final_barrier_id',
                                  'through_barrier_id', 'readiness_boundary', 'contract_barrier_names')),
    'snapshot': Domain('SNAPSHOT', ('queue_snapshot_id', 'snapshot_id')),
    'checkpoint': Domain('CHECKPOINT', ('checkpoint_id', 'checkpoint_ref')),
    'clock': Domain('CLOCK', ('clock_id',)),
    'contract': Domain('CONTRACT', ('contract_id', 'contract_version')),
    'capability': Domain('CAPABILITY', ('capability_id',)),
    'scenario': Domain('SCENARIO', ('scenario_id',)),
    'other': Domain('ID', ('opaque_id',)),
}
ALIASES = {alias: name for name, domain in REGISTRY.items() for alias in domain.aliases}
# These are declared rig symbols, not a pattern that trusts pseudonym-like input.
PUBLIC = {
    'barrier': {'injection', 'ready', 'final', 'pre_injection', 'fetch_result_held'},
    'stream': {'broker', 'adapter', 'host', 'driver', 'injection', 'transport', 'queue', 'contract', 'state', 'recovery'},
    'artifact': {'broker', 'adapter', 'host', 'host-records', 'driver-observations', 'held-fetch-response',
                 'injection', 'transport', 'queue', 'contract', 'state', 'recovery'},
    'event': {'held-fetch-response'},
    'contract': {'matrix-negotiated-v1'}, 'capability': {'matrix-v1'},
    'scenario': {cell.name for cell in cells()},
    'capture': {cell.name for cell in cells()},
}
COLLECTIONS = {'events': 'event', 'artifacts': 'artifact', 'streams': 'stream', 'barriers': 'barrier',
               'queue_snapshots': 'snapshot', 'checkpoints': 'checkpoint', 'scenario': 'scenario', 'contract': 'contract'}
MAP_KEYS = {'received': 'source', 'stream_cutoffs': 'stream'}
# Values in these fields describe evidence, rather than naming a private entity.
LITERALS = {'type', 'role', 'kind', 'origin', 'from', 'server', 'source', 'operation', 'name', 'method',
            'transport', 'phase', 'state', 'milestone', 'outcome', 'reason_code', 'promptSource', 'userType',
            'entrypoint', 'permissionMode', 'schema_version', 'redaction_version', 'version', 'timestamp',
            'content_digest', 'seq', 'slot', 'count', 'members', 'retired', 'elapsed_ms', 'time', 'time_ms',
            'claim_generation', 'byte_offset', 'duration_ms', 'session', 'cell'}
UUID = re.compile(r'\b[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\b')
LOCAL_PATH = re.compile(r'(?<!\w)/(?:home|Users|tmp|private|run|work|root|var|etc|opt|mnt|srv|Volumes)'
                        r'(?:/[^\s"<>\'\],;)}]*)?(?![\w-])')
# Conservative secret shapes, not an attempt to anonymize arbitrary prose.
# Non-identity, non-secret-shaped natural language still requires explicit
# registry/grammar coverage before export (spec 3.7).
SECRET_RUN = re.compile(r'(?<![\w-])(?:[A-Za-z0-9_+/-]{24,}={0,2}|[A-Za-z0-9]+(?:[-_][A-Za-z0-9]+)+)(?![\w-])')
PEER_PREFIX = 'Another Claude session sent a message:\n'
NUMERIC_LITERAL = re.compile(
    r'\b(?:counts?|versions?|sequences?|seq|slots?|lines?|offsets?|members|retired|elapsed_ms|claim_generation|byte_offset|duration_ms)'
    r'\s*(?:[=:#]\s*)?["\']?(-?\d+(?:\.\d+)*)\b', re.I)
QUANTITY = re.compile(r'(?<![\w.+/-])(-?\d+(?:\.\d+)*)\s+'
                      r'(?:messages|sources|rows|records|attempts|retries|bytes|seconds|minutes|ms)\b', re.I)
# Punctuation delimits numeric identities, except a dot joined to another
# digit: sentence tails and path components match; decimals/versions do not.
NUMERIC_START = r'(?<!\w)(?<!\d\.)'
NUMERIC_END = r'(?!\w)(?!\.\d)'
NUMERIC_REFERENCE = re.compile(r'\b(source|message|pid|user_id|chat|topic|epoch)\s*(?:[:#]\s*|\s+)'
                               + NUMERIC_START + r'(-?(?:0|[1-9][0-9]*))' + NUMERIC_END)
REFERENCE_DOMAINS = {'source': 'source', 'message': 'source', 'pid': 'process', 'user_id': 'user_id',
                     'chat': 'chat', 'topic': 'topic', 'epoch': 'epoch'}
ENVELOPE_FIELDS = set(ALIASES) | LITERALS | set(COLLECTIONS) | set(MAP_KEYS) | {
    'id', 'value', 'message', 'content', 'text', 'attachment', 'isMeta', 'is_error',
    'jsonrpc', 'result', 'params', 'frame', 'scope', 'observation', 'records', 'host_records',
    'sessions', 'attempt', 'c3_attempt', 'setup_errors', 'run_errors', 'reason', 'reasons',
    'detail', 'details', 'evidence', 'diagnostics', 'diagnostic_map', 'sha256',
}
LITERAL_VALUES = {
    'type': {'user', 'assistant', 'attachment', 'queue-operation', 'tool_use', 'tool_result', 'text', 'image', 'queued_command'},
    'role': {'user', 'assistant', 'system', 'tool'}, 'kind': {'peer', 'channel'},
    'from': {'c3'}, 'source': {'plugin:c3:c3'}, 'operation': {'enqueue', 'remove'},
    'transport': {'channel', 'inbox', 'fetch', 'fetch_receipt'},
    'phase': {'reserved', 'confirmed', 'failed', 'expired', 'released'},
    'state': {'complete', 'missing', 'unreadable', 'malformed', 'partial', 'truncated', 'unavailable',
              'idle', 'foreground', 'background', 'startup', 'reconnect', 'reached', 'not_reached', 'unknown'},
    'session': {'fresh', 'resumed'},
}
NUMERIC_FIELDS = {'schema_version', 'version', 'seq', 'slot', 'count', 'members', 'retired',
                  'elapsed_ms', 'time', 'time_ms', 'claim_generation', 'byte_offset', 'duration_ms'}
DECIMAL = re.compile(r'(?:0|-?[1-9][0-9]*)\Z')
ROUTE = re.compile(r'(-?(?:0|[1-9][0-9]*))/(dm|-?(?:0|[1-9][0-9]*))\Z')
ATTEMPT_LABEL = re.compile(r'(?:inbox|channel|cross-session):[1-9][0-9]*\Z')
LEXICAL_ID = re.compile(r'[A-Za-z0-9_-]+\Z')
REVISION = re.compile(r'[0-9a-f]{64}\Z')
ASSIGNMENT = re.compile(r'(?<![\w-])([A-Za-z_][\w-]*)=(?:"((?:\\.|[^"\\])*)"|\'([^\']*)\'|([^\s<>"\'{}(),;]+))')
TRAILER_GROUP = re.compile(r'^group ([^\n]*)', re.M)
TRAILER_MEMBER = re.compile(r'^member ([^\n]*)', re.M)


def _domain(key, schema, parent):
    if key == 'session_id' and schema in ('host', 'session_hook'):
        return 'host_session'
    if key == 'session' and schema not in ('broker', 'ipc'):
        return None
    if key == 'id':
        if parent.get('type') in ('tool_use', 'tool_result') or 'jsonrpc' in parent or schema == 'ipc':
            return 'call'
        return schema if schema in REGISTRY else 'other'
    if key == 'name' and schema == 'barrier':
        return 'barrier'
    if key == 'value' and schema == 'revision':
        return 'revision'
    if key.startswith('attachment_file_id_'):
        return 'file'
    return ALIASES.get(key)


def _child_schema(key, schema, parent):
    if key in ('diagnostics', 'diagnostic_map') and type(parent.get(key)) is dict:
        return 'map'
    if key in COLLECTIONS and schema != 'host':
        return COLLECTIONS[key]
    if key == 'revision':
        return 'revision'
    if key in ('scope', 'observation'):
        return 'canonical'
    if key == 'frame':
        return 'ipc'
    if key in ('records', 'host_records'):
        return 'host'
    if key == 'sessions':
        return 'session_hook'
    if key == 'message':
        return 'message'
    return schema


def _secret_shaped(raw):
    return len(raw) >= 24 and SECRET_RUN.fullmatch(raw) is not None


def _literal_value(field, raw):
    return (raw in LITERAL_VALUES.get(field, ())
            or (field in NUMERIC_FIELDS and re.fullmatch(r'-?\d+(?:\.\d+)*', raw))
            or (field in ('attempt', 'c3_attempt') and ATTEMPT_LABEL.fullmatch(raw)))


def _occurrences(raw, text):
    # Long secret-shaped canaries must be scrubbed even inside a larger run.
    # Short words and decimal IDs need lexical boundaries: session != sessions,
    # 987 != 1987, v987, or 2.987, but punctuation alone allows a numeric match.
    pattern = re.escape(raw)
    if DECIMAL.fullmatch(raw):
        pattern = NUMERIC_START + pattern + NUMERIC_END
    elif not _secret_shaped(raw):
        pattern = r'(?<![\w-])' + pattern + r'(?![\w-])'
    return re.finditer(pattern, text)


class RedactionContext:
    """One capture only: discover all roots, seal, then render known identities.

    Discovery order is the caller's root order, sorted dictionary fields, list
    order, and left-to-right text spans. Hints recognize occurrences but never
    allocate IDs in set iteration order. Sealing resolves untyped text against
    the complete inventory; late identities and ambiguous aliases fail closed.
    """

    def __init__(self, *, tokens=(), receipt_ids=()):
        self._roots = []
        self._hints = {'token': set(tokens), 'row': set(receipt_ids)}
        self._seen = {name: {} for name in REGISTRY}
        self._maps = {name: {} for name in REGISTRY}
        self._texts = []
        self._position = 0
        self._sealed = False
        self._scanning = False
        self._checking_legacy = False
        self.diagnostics = []

    def __repr__(self):
        return f'<RedactionContext sealed={self._sealed} roots={len(self._roots)}>'

    @staticmethod
    def _error(locator, problem):
        raise RedactionError(f'redaction {locator}: {problem}') from None

    def discover(self, value, *, schema='generic', key='', tokens=(), receipt_ids=()):
        if self._sealed:
            self._error('capture', 'context already sealed; use a new context for another capture')
        self._hints['token'].update(tokens)
        self._hints['row'].update(receipt_ids)
        self._roots.append((value, schema, key))
        return self

    def _logical(self, domain, value, locator):
        if value is None or value == '' or type(value) is bool:
            return None
        if type(value) in (int, float):
            if type(value) is float and (not math.isfinite(value) or not value.is_integer() or abs(value) > 2**53):
                self._error(locator, 'unsupported numeric identity encoding')
            if domain in ('token', 'row', 'revision', 'path'):
                self._error(locator, 'invalid identity type')
            return ('number', int(value))
        if type(value) is not str:
            self._error(locator, 'invalid identity type')
        if REGISTRY[domain].numeric and DECIMAL.fullmatch(value):
            # No whitespace, leading zero or exponent coercion.
            return ('number', int(value))
        return ('text', value)

    def _remember(self, domain, value, position, locator):
        if domain is None:
            return value
        logical = self._logical(domain, value, locator)
        if logical is not None:
            previous = self._seen[domain].get(logical)
            if previous is None or position < previous:
                self._seen[domain][logical] = position
            if domain == 'route' and type(value) is str and (match := ROUTE.fullmatch(value)):
                self._remember('chat', match[1], position, locator)
                if match[2] != 'dm':
                    self._remember('topic', match[2], (position[0], position[1] + len(match[1]) + 1), locator)
        return value

    def _identity(self, domain, value, position, locator, *, lexical=None):
        if domain is None:
            return value
        logical = self._logical(domain, value, locator)
        if type(value) is bool and self._scanning:
            self.diagnostics.append(f'{locator}: invalid boolean identity retained')
        if self._scanning:
            return self._remember(domain, value, position, locator)
        if logical is None:
            return value
        if self._checking_legacy:
            if domain != 'revision' and logical in self._seen[domain] and value not in PUBLIC.get(domain, ()):
                self._error(locator, 'private identity remains in legacy fixture')
            return value
        if logical not in self._maps[domain]:
            self._error(locator, 'identity was not discovered before sealing')
        replacement = self._maps[domain][logical]
        if domain == 'route' and type(value) is str and (match := ROUTE.fullmatch(value)):
            chat = self._identity('chat', match[1], position, locator)
            topic = 'dm' if match[2] == 'dm' else self._identity('topic', match[2], position, locator)
            replacement = f'{chat}/{topic}'
        if lexical and value and not lexical.fullmatch(str(value)):
            # Keep invalid lexical slots invalid, even when their logical identity
            # also occurs in a perfectly valid canonical string field.
            replacement = '!' + str(replacement)
            if lexical is ATTEMPT_LABEL and str(value).startswith(('inbox:', 'channel:', 'cross-session:')):
                replacement = str(value).split(':', 1)[0] + ':' + replacement
        if type(value) is int:
            return int(replacement)
        if type(value) is float:
            return float(replacement)
        return str(replacement)

    def seal(self):
        if self._sealed:
            return self
        self._scanning = True
        for index, (value, schema, key) in enumerate(self._roots):
            self._walk(value, schema, key, {}, f'root[{index}]')
        # Hints enter the inventory only after actual occurrences establish order.
        for domain, values in self._hints.items():
            for index, value in enumerate(sorted(values)):
                self._remember(domain, value, (self._position + 1, index), 'hints')
        # Unknown UUIDs use their own opaque namespace; known UUIDs resolve to
        # their typed domain. Typed paths were already discovered with grammar.
        for text, schema, position, locator in self._texts:
            if schema == 'map_key' and self._domains_for(text):
                continue  # Resolve the entire key after the typed inventory is complete.
            typed = self._structured_spans(text, schema, locator)
            for start, end, domain, raw, lexical, _ in typed:
                self._remember(domain, raw, (position, start), locator)
            for match in sorted([*UUID.finditer(text), *(m for m in SECRET_RUN.finditer(text) if _secret_shaped(m[0]))],
                                key=lambda m: (m.start(), -len(m[0]))):
                if any(start < match.end() and end > match.start() for start, end, *_ in typed):
                    continue
                # A registered secret inside a longer canary already has a
                # domain; do not invent a second identity for its surrounding run.
                if not any(next(_occurrences(raw, match[0]), None) for raw in self._inventory()):
                    self._remember('other', match[0], (position, match.start()), locator)
        for text, schema, position, locator in self._texts:
            for start, end, domain, value, lexical, _ in self._spans(text, schema, locator):
                self._remember(domain, value, (position, start), locator)
        self._scanning = False
        self._allocate()
        self._sealed = True
        # Validate every root before a caller can publish any part of the capture.
        for value, schema, key in self._roots:
            self.render(value, schema=schema, key=key)
        return self

    def _allocate(self):
        originals = {str(value) for values in self._seen.values() for kind, value in values}
        for domain, identities in self._seen.items():
            used = set()
            for logical in sorted(identities, key=lambda item: (identities[item], item)):
                kind, raw = logical
                if kind == 'text' and raw in PUBLIC.get(domain, ()):
                    candidate = raw
                else:
                    n = 1
                    while True:
                        if kind == 'number':
                            candidate = -n if raw < 0 else n
                        elif domain == 'revision':
                            candidate = format(n, '064x')
                        else:
                            candidate = REGISTRY[domain].prefix + str(n)
                        if str(candidate) not in originals and candidate not in used:
                            break
                        n += 1
                used.add(candidate)
                self._maps[domain][logical] = candidate

    def pseudonym(self, domain, value):
        """Look up a known identity without exposing the forward/reverse maps."""
        if not self._sealed:
            self._error('capture', 'context must be sealed before lookup')
        return self._identity(domain, value, (0, 0), 'lookup')

    def render(self, value, *, schema='generic', key=''):
        if not self._sealed:
            self._error('capture', 'context must be sealed before rendering')
        rendered = self._walk(value, schema, key, {}, 'render')
        self.check_private_text(rendered, schema=schema, key=key)
        return rendered

    def check_private_text(self, rendered, *, legacy_fixture=False, schema='generic', key=''):
        # Audit the serialized representation too, including keys and escaped
        # strings. A substring ban on e.g. "1" or "session" would reject public
        # counts and the peer prefix: those have explicit grammar exemptions.
        encoded = json.dumps(rendered, ensure_ascii=False, sort_keys=True)
        self._audit(json.loads(encoded), schema, key, {}, 'audit', legacy_fixture)

    def check_export_text(self, text, *, format='text', schema='generic', legacy_fixture=False):
        """Audit final file bytes before publication; never repair at this gate."""
        text = text.encode('utf-8').decode('utf-8')
        if format == 'json':
            try:
                values = [json.loads(text)]
            except ValueError:
                self._error('export', 'invalid serialized JSON')
        elif format == 'jsonl':
            values = []
            for line in text.splitlines():
                try:
                    values.append(json.loads(line))
                except ValueError:
                    values.append(line)  # Safe malformed stubs still get audited.
        else:
            values = [text]
        for index, value in enumerate(values):
            self._audit(value, schema, '', {}, f'export[{index}]', legacy_fixture)

    def _audit(self, value, schema, key, parent, locator, legacy):
        if type(value) is dict:
            for index, (field, child) in enumerate(sorted(value.items())):
                child_locator = f'{locator}.field[{index}]'
                if key in MAP_KEYS or schema == 'map' or field not in ENVELOPE_FIELDS:
                    self._audit_text(field, 'map_key', '', child_locator, legacy)
                if key in MAP_KEYS or schema == 'map':
                    self._audit(child, 'generic', '', {}, child_locator, legacy)
                else:
                    self._audit(child, _child_schema(field, schema, value), field, value, child_locator, legacy)
        elif type(value) is list:
            for index, child in enumerate(value):
                self._audit(child, schema, key, parent, f'{locator}[{index}]', legacy)
        elif type(value) is str:
            domain = _domain(key, schema, parent)
            if (domain and not (legacy and domain == 'revision') and value not in PUBLIC.get(domain, ())
                    and self._logical(domain, value, locator) in self._seen[domain]):
                self._error(locator, 'private identity remains in output')
            self._audit_text(value, schema, key, locator, legacy)
        elif type(value) in (int, float):
            domain = _domain(key, schema, parent)
            if domain and self._logical(domain, value, locator) in self._seen[domain]:
                self._error(locator, 'private numeric identity remains in output')

    def _audit_text(self, text, schema, key, locator, legacy):
        if _literal_value(key, text):
            return
        protected = [] if schema == 'map_key' else [
            (a, b) for a, b, domain, *_ in self._structured_spans(text, schema, locator) if domain is None]
        inventory = self._inventory()
        generated = {str(value) for values in self._maps.values() for value in values.values()
                     if not any(str(value) not in PUBLIC.get(domain, ()) for domain in inventory.get(str(value), ()))}
        if legacy:
            generated.add('/work/ID')
        # Generated revisions/paths are secret-shaped too. Trust only values
        # allocated by this context, never a pseudonym-looking input pattern.
        safe = [(m.start(), m.end()) for raw in generated for m in _occurrences(raw, text)]
        for raw, domains in inventory.items():
            if all(raw in PUBLIC.get(domain, ()) or (legacy and domain == 'revision') for domain in domains):
                continue
            for match in _occurrences(raw, text):
                if any(a <= match.start() and b >= match.end() for a, b in protected + safe):
                    continue
                self._error(locator, 'private identity remains in output')
        for match in [*LOCAL_PATH.finditer(text), *UUID.finditer(text),
                      *(m for m in SECRET_RUN.finditer(text) if _secret_shaped(m[0]))]:
            if any(a <= match.start() and b >= match.end() for a, b in protected + safe):
                continue
            if key in ('sha256', 'content_digest') and REVISION.fullmatch(text):
                continue  # Representation digests, after the registered-identity check.
            if legacy and ('text', match[0]) in self._seen['revision']:
                continue
            self._error(locator, 'secret-shaped content remains in output')

    def check_legacy_fixture(self, value, *, schema='generic'):
        """Audit compatibility output with the same registry, without changing it."""
        self._checking_legacy = True
        try:
            self._walk(value, schema, '', {}, 'legacy')
            self.check_private_text(value, legacy_fixture=True, schema=schema)
        finally:
            self._checking_legacy = False

    def _walk(self, value, schema, key, parent, locator):
        if (not self._scanning and key and not _domain(key, schema, parent) and type(value) not in (bool, type(None))
                and re.search(r'_(?:id|ids|token|tokens|uuid|path)$', key)):
            self._error(locator, 'unregistered identity field')
        if type(value) is dict:
            if schema == 'generic' and value.get('type') in ('user', 'assistant', 'attachment', 'queue-operation'):
                schema = 'host'
            if schema == 'generic' and 'transcript_path' in value and 'session_id' in value:
                schema = 'session_hook'
            if _domain(key, schema, parent) and key != 'revision':
                self._error(locator, 'invalid identity type')
            if any(type(field) is not str for field in value):
                self._error(locator, 'object fields must be strings')
            result = {}
            for index, field in enumerate(sorted(value)):
                child_locator = f'{locator}.field[{index}]'
                rendered_field = field
                if key in MAP_KEYS:
                    self._position += 1
                    rendered_field = self._identity(MAP_KEYS[key], field, (self._position, 0), child_locator)
                    child = self._walk(value[field], 'generic', '', {}, child_locator)
                else:
                    data_key = schema == 'map' or field not in ENVELOPE_FIELDS
                    if data_key:
                        rendered_field = self._walk(field, 'map_key', '', {}, child_locator + '.key')
                    identity_key = data_key and (schema == 'map' or bool(self._domains_for(field)))
                    child = self._walk(value[field], 'generic' if identity_key else _child_schema(field, schema, value),
                                       '' if identity_key else field, value, child_locator)
                if rendered_field in result:
                    self._error(child_locator, 'identity key collision')
                result[rendered_field] = child
            return result
        if type(value) is list:
            return [self._walk(child, schema, key, parent, f'{locator}[{index}]') for index, child in enumerate(value)]
        self._position += 1
        position = self._position
        if schema == 'literal':
            return value
        domain = _domain(key, schema, parent)
        if key == 'merged_message_ids' and type(value) is str:
            return ','.join(self._identity('source', part, (position, index), locator) for index, part in enumerate(value.split(',')))
        if domain:
            return self._identity(domain, value, (position, 0), locator)
        if type(value) is str:
            if key in ('attempt', 'c3_attempt') and ATTEMPT_LABEL.fullmatch(value):
                return value
            if key in ('attempt', 'c3_attempt'):
                return self._identity('attempt', value, (position, 0), locator, lexical=ATTEMPT_LABEL)
            # Literal enums cannot accidentally alias short private identities.
            if key in LITERALS:
                return value
            if self._scanning:
                self._texts.append((value, schema, position, locator))
                if schema != 'map_key':
                    for start, end, domain, raw, lexical, _ in self._structured_spans(value, schema, locator, paths=False):
                        self._remember(domain, raw, (position, start), locator)
                return value
            spans = self._spans(value, schema, locator)
            pieces, cursor = [], 0
            for start, end, domain, raw, lexical, quote in spans:
                pieces.append(value[cursor:start])
                replacement = str(self._identity(domain, raw, (position, start), locator, lexical=lexical))
                if quote == 'json':
                    replacement = json.dumps(replacement, ensure_ascii=False)[1:-1]
                pieces.append(replacement)
                cursor = end
            pieces.append(value[cursor:])
            return ''.join(pieces)
        return value

    def _structured_spans(self, text, schema, locator, *, paths=True):
        spans = []
        if text.startswith(PEER_PREFIX):
            spans.append((0, len(PEER_PREFIX), None, PEER_PREFIX, None, None))
        for match in ASSIGNMENT.finditer(text):
            field = match[1]
            domain = _domain(field, schema, {})
            if field == 'key' and schema == 'broker':
                domain = 'route'
            if field in ('c3_attempt', 'attempt'):
                raw = next(group for group in match.groups()[1:] if group is not None)
                domain = None if ATTEMPT_LABEL.fullmatch(raw) else 'attempt'
            if not domain and field not in LITERALS and field not in ('c3_attempt', 'attempt'):
                continue
            spans.append((match.start(1), match.end(1), None, field, None, None))
            group = next(index for index in (2, 3, 4) if match[index] is not None)
            raw, quote = match[group], None
            if group == 2 and schema in ('broker', 'adapter'):
                try:
                    raw = json.loads('"' + raw + '"')
                except ValueError:
                    try:
                        # Go's %q additionally emits \x, \a and \v escapes.
                        with warnings.catch_warnings():
                            warnings.simplefilter('error')
                            raw = ast.literal_eval('"' + raw + '"')
                    except (SyntaxError, ValueError, Warning):
                        self._error(locator, 'unsupported quoted identity encoding')
                quote = 'json'
            # '-' is the broker's explicit absent-topic spelling.
            if domain == 'topic' and raw == '-' and schema == 'broker':
                continue
            if domain is None and not _literal_value(field, raw):
                # A public field spelling does not make an arbitrary value safe.
                # Let fallback discovery/sanitization handle private diagnostics.
                continue
            lexical = ATTEMPT_LABEL if field in ('c3_attempt', 'attempt') and domain == 'attempt' else None
            spans.append((match.start(group), match.end(group), domain, raw, lexical, quote))
        # Scan candidates even with a missing marker, bad member, or final tail.
        for match in TRAILER_GROUP.finditer(text):
            spans.append((match.start(1), match.end(1), 'token', match[1], LEXICAL_ID, None))
        for match in TRAILER_MEMBER.finditer(text):
            raw = match[1]
            if ' ' in raw:
                row, revision = raw.split(' ', 1)
                split = match.start(1) + len(row)
                spans.append((match.start(1), split, 'row', row, LEXICAL_ID, None))
                spans.append((split + 1, match.end(1), 'revision', revision, REVISION, None))
            else:
                spans.append((match.start(1), match.end(1), 'row', raw, LEXICAL_ID, None))
        # Prose references and quantities are weaker than assignments/trailers:
        # token="source 987" names one token, not an embedded source identity.
        prose = [(m.start(), m.end(), None, m[0], None, None)
                 for m in [*NUMERIC_LITERAL.finditer(text), *QUANTITY.finditer(text)]]
        for match in NUMERIC_REFERENCE.finditer(text):
            domain = REFERENCE_DOMAINS[match[1]]
            prose.append((match.start(1), match.end(1), None, match[1], None, None))
            if self._logical(domain, match[2], locator) in self._seen[domain]:
                prose.append((match.start(2), match.end(2), domain, match[2], None, None))
        for span in prose:
            if not any(span[0] < end and span[1] > start for start, end, *_ in spans):
                spans.append(span)
        # A merged list is one assignment, but each element is its own identity.
        expanded = []
        for span in spans:
            start, end, domain, raw, lexical, quote = span
            if domain == 'source' and ',' in raw:
                offset = start
                for part in raw.split(','):
                    expanded.append((offset, offset + len(part), domain, part, lexical, quote))
                    offset += len(part) + 1
            else:
                expanded.append(span)
        if paths:
            known_paths = [raw for kind, raw in self._seen['path'] if kind == 'text']
            for match in LOCAL_PATH.finditer(text):
                raw = max([match[0]] + [raw for raw in known_paths if text.startswith(raw, match.start())], key=len)
                end = match.start() + len(raw)
                if not any(start <= match.start() and stop >= end for start, stop, *_ in expanded):
                    expanded.append((match.start(), end, 'path', raw, None, None))
        return self._nonoverlapping(expanded, locator, structured=True)

    def _domains_for(self, raw):
        domains = []
        for domain, identities in self._seen.items():
            logical = self._logical(domain, raw, 'text')
            if logical in identities or (DECIMAL.fullmatch(raw) and ('number', int(raw)) in identities):
                domains.append(domain)
        return domains

    def _inventory(self):
        inventory = {}
        for domain, identities in self._seen.items():
            for kind, raw in identities:
                inventory.setdefault(str(raw), []).append(domain)
        return inventory

    def _embedded_value(self, domain, raw, locator):
        logical = self._logical(domain, raw, locator)
        number = ('number', int(raw)) if DECIMAL.fullmatch(raw) else None
        if number in self._seen[domain] and number != logical:
            if logical in self._seen[domain]:
                self._error(locator, 'ambiguous embedded identity representations')
            return number[1]
        return raw

    def _spans(self, text, schema, locator):
        if schema == 'map_key' and (domains := self._domains_for(text)):
            if len(domains) != 1:
                self._error(locator, 'ambiguous embedded identity domains')
            return [(0, len(text), domains[0], self._embedded_value(domains[0], text, locator), None, None)]
        structured = self._structured_spans(text, schema, locator)
        generic = []
        inventory = self._inventory()
        for raw in sorted(inventory, key=lambda item: (-len(item), item)):
            for match in _occurrences(raw, text):
                start, end = match.span()
                if any(start < b and end > a for a, b, *_ in structured):
                    continue
                domains = self._domains_for(raw)
                if all(raw in PUBLIC.get(domain, ()) for domain in domains):
                    continue
                generic.append((start, end, 'embedded', raw, None, None))
        resolved = []
        for start, end, _, raw, lexical, quote in self._nonoverlapping(generic, locator):
            domains = self._domains_for(raw)
            if len(domains) != 1:
                self._error(locator, 'ambiguous embedded identity domains')
            resolved.append((start, end, domains[0], self._embedded_value(domains[0], raw, locator), lexical, quote))
        return sorted(structured + resolved, key=lambda span: span[:2])

    def _nonoverlapping(self, spans, locator, *, structured=False):
        chosen = []
        for span in sorted(spans, key=lambda item: (-(item[1] - item[0]), item[0], item[2] or '')):
            if span in chosen:
                continue
            overlaps = [other for other in chosen if span[0] < other[1] and span[1] > other[0]]
            if overlaps:
                if structured or any(span[0] < other[0] or span[1] > other[1] for other in overlaps):
                    self._error(locator, 'incompatible overlapping identity spans')
                continue
            if span[0] != span[1]:
                chosen.append(span)
        return sorted(chosen, key=lambda span: span[:2])


def sanitize(value, tokens=(), key='', receipt_ids=(), *, context=None, schema='generic'):
    if context is None:
        context = RedactionContext(tokens=tokens, receipt_ids=receipt_ids)
        context.discover(value, schema=schema, key=key).seal()
    elif not context._sealed:
        context._error('capture', 'discover all roots and seal the shared context first')
    else:
        for domain, hints in (('token', tokens), ('row', receipt_ids)):
            for hint in sorted(set(hints)):
                context.pseudonym(domain, hint)
    return context.render(value, schema=schema, key=key)
