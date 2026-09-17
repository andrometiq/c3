"""Format-neutral checked evidence reads and tolerant polling."""
import hashlib
import json


def read_jsonl(path):
    if not path.exists():
        return []
    records = []
    for line in path.read_text(errors="replace").splitlines(keepends=True):
        if not line.endswith("\n"):
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def read_problem(captured, state, detail):
    """Accumulate final-read failures without letting a tail hide corruption."""
    severity = ('complete', 'partial', 'truncated', 'malformed', 'unreadable', 'missing')
    if severity.index(state) > severity.index(captured['state']):
        captured['state'] = state
    if detail not in captured['problems']:
        captured['problems'].append(detail)
    captured['detail'] = '; '.join(captured['problems'])


def _invalid_json_constant(value):
    raise ValueError('non-finite JSON number')


def _unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON field')
        result[key] = value
    return result


def checked_read(path, *, jsonl=False, format=None):
    """Authoritative final read; polling deliberately continues using read_jsonl."""
    format = format or ('jsonl' if jsonl else 'log')
    try:
        data = path.read_bytes()
    except OSError as error:
        result = dict(state='complete', records=[], record_lines=[], text='', detail='',
                      problems=[], bytes=None, sha256=None, lines=0)
        read_problem(result, 'missing' if isinstance(error, FileNotFoundError) else 'unreadable',
                     'file missing' if isinstance(error, FileNotFoundError) else type(error).__name__)
        return result
    return checked_bytes(data, format=format)


def checked_bytes(data, *, format='log'):
    """Parse one immutable byte snapshot; never reopen its source for sealing."""
    result = dict(state='complete', records=[], record_lines=[], text='', detail='',
                  problems=[], bytes=None, sha256=None, lines=0)
    result.update(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
    try:
        result['text'] = data.decode('utf-8')
    except UnicodeDecodeError as error:
        read_problem(result, 'malformed', f'byte:{error.start}: invalid UTF-8')
        return result
    lines = result['text'].splitlines(keepends=True)
    result['lines'] = len(lines)
    if format == 'json':
        lines = [result['text']]
        result['lines'] = 1
    for index, line in enumerate(lines, 1):
        if format != 'json' and not line.endswith('\n'):
            read_problem(result, 'partial', f'line:{index}: unterminated tail')
            continue
        if format not in ('json', 'jsonl'):
            continue
        try:
            record = json.loads(line, parse_constant=_invalid_json_constant, object_pairs_hook=_unique_json_object)
            if type(record) is not dict:
                raise ValueError()
        except (ValueError, RecursionError):
            read_problem(result, 'malformed', f'line:{index}: malformed JSON object')
            continue
        result['records'].append(record)
        result['record_lines'].append(index)
    return result


