"""Explicit raw-corpus authoring utilities. Never decides expected verdicts.

Run with --write-raw DIRECTORY to author the synthetic controls. Ordinary test
execution only reads the frozen corpus; expectation freezing is a separate step.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil


def json_text(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + '\n'


def jsonl(records, *, terminated=True):
    text = ''.join(json.dumps(record, ensure_ascii=False, sort_keys=True) + '\n' for record in records)
    return text if terminated else text.removesuffix('\n')


def attempt_line(token, transport, phase, members, retired=0, elapsed_ms=0):
    return f'TEST ATTEMPT token={token} topic=881 transport={transport} phase={phase} members={members} retired={retired} elapsed_ms={elapsed_ms}\n'


def injection_line(source):
    return f'TEST INJECT accepted topic=881 message_id={source} kind=text source=test-inject\n'


def trailer(token, rows):
    return '\n'.join(['[C3_FETCH_RECEIPT_V1]', 'group ' + token,
                      *['member ' + row['row_id'] + ' ' + row['revision'] for row in rows], '[/C3_FETCH_RECEIPT_V1]'])


def host_record(source, token, ordinal, *, peer=False):
    text = f'<channel source="plugin:c3:c3" message_id="{source}" c3_delivery_id="{token}" c3_attempt="{"inbox" if peer else "channel"}:{ordinal}">MATRIX_SAMPLE</channel>'
    record = dict(type='user', uuid=f'11111111-2222-4333-8444-{ordinal:012d}', sessionId='session-host-original',
                  message=dict(role='user', content=('Another Claude session sent a message:\n' if peer else '') + text))
    if peer:
        record.update(isMeta=True, origin=dict(kind='peer', **{'from': 'c3'}), verifiedPeerPid=45671)
    return record


def reseal(root):
    path = root / 'capture.json'
    descriptor = json.loads(path.read_text())
    for entry in descriptor['artifacts']:
        artifact = root / entry['file']
        if not artifact.is_file():
            continue
        data = artifact.read_bytes()
        try:
            text = data.decode('utf-8')
            lines = 1 if entry['format'] == 'json' else len(text.splitlines())
        except UnicodeDecodeError:
            lines = 0
        entry['seal'] = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest(), lines=lines, read_state='complete')
    path.write_text(json_text(descriptor))


def replace_exact(root, file, old, new, *, occurrences=1):
    path = root / file
    original = path.read_bytes()
    old, new = (v.encode() if type(v) is str else v for v in (old, new))
    if original.count(old) != occurrences or old == new:
        raise ValueError('raw mutation occurrence count differs or replacement is unchanged')
    path.write_bytes(original.replace(old, new))
    return dict(file=file, before=hashlib.sha256(original).hexdigest(), after=hashlib.sha256(path.read_bytes()).hexdigest())


def copy_capture(source, destination):
    shutil.copytree(source, destination, ignore=shutil.ignore_patterns('expected-*', 'mutation.json'))


def control(root, transport):
    root.mkdir(parents=True, exist_ok=True)
    (root / 'context').mkdir(exist_ok=True)
    scope = dict(run_id='run-synthetic-original', route_id='-771/881', host_session_id='session-host-original',
                 session_id='session-broker-original', connection_epoch_id='epoch-original', claim_generation=3)
    sources = [41001] if transport == 'inbox' else [41001, 41002]
    rows = [dict(row_id='row-original-' + str(i), revision=c * 64) for i, c in enumerate('ab'[:len(sources)], 1)]
    tokens = ['token-original-one', 'token-original-two']
    artifacts, owners, occurrences, bindings = [], [], [], []
    data = {}
    def artifact(identity, file, table, format, stream, role, value):
        artifacts.append(dict(artifact_id=identity, file=file, table=table, format=format, seal={}))
        owners.append(dict(artifact_id=identity, stream_id=stream, role=role, **scope, pid=45671))
        data[identity] = value
    def occurrence(artifact, line, event, time, *, stream=None, delivery=None, block=0):
        owner = next(owner for owner in owners if owner['artifact_id'] == artifact)
        occurrences.append(dict(artifact_id=artifact, line=line, block=block, stream_id=stream or owner['stream_id'],
                                event_id=event, delivery_id=delivery, clock_id='observer-clock', time_ms=time))
    def binding(artifact, line, group, members, retired=(), block=0):
        bindings.append(dict(artifact_id=artifact, line=line, block=block,
                             attempt_id=('attempt-original-' if transport == 'fetch' else transport + ':') + str(group),
                             group_id='group-original-' + str(group), rows=deepcopy(members), retired_rows=deepcopy(list(retired))))
    broker = ''.join(injection_line(source) for source in sources)
    if transport == 'fetch':
        broker += attempt_line(tokens[0], transport, 'reserved', 2) + attempt_line(tokens[0], transport, 'confirmed', 2, 2, 40)
    else:
        for i in range(len(sources)):
            broker += attempt_line(tokens[i], transport, 'reserved', 1) + attempt_line(tokens[i], transport, 'confirmed', 1, 1, 40)
    artifact('broker', 'broker.log', 'broker', 'log', 'broker', 'broker', broker)
    artifact('adapter', 'adapter.log', 'adapter', 'log', 'adapter', 'transport', 'adapter initialized\n')
    records = [dict(type='assistant', uuid='11111111-2222-4333-8444-000000000090', sessionId=scope['host_session_id'],
                    message=dict(role='assistant', content='seed conversation'))]
    if transport != 'fetch':
        records += [host_record(source, tokens[i], i + 1, peer=transport == 'inbox') for i, source in enumerate(sources)]
    else:
        content = 'MATRIX_SAMPLE\nMATRIX_SAMPLE\n' + trailer(tokens[0], rows)
        records += [dict(type='assistant', uuid='11111111-2222-4333-8444-000000000091', sessionId=scope['host_session_id'],
                         message=dict(role='assistant', content=[dict(type='tool_use', id='call-original', name='mcp__c3__fetch_queue', input=dict(ack=True))])),
                    dict(type='user', uuid='11111111-2222-4333-8444-000000000092', sessionId=scope['host_session_id'],
                         message=dict(role='user', content=[dict(type='tool_result', tool_use_id='call-original', content=content)]))]
    artifact('host-records', 'records.jsonl', 'host', 'jsonl', 'host', 'host', records)
    artifact('injection', 'context/injection.json', 'injection', 'json', 'injection', 'injection',
             dict(message_ids=sources, kind='text', broker_lines=list(range(1, len(sources) + 1))))
    artifact('queue', 'context/rows.jsonl', 'rows', 'jsonl', 'queue', 'queue',
             [dict(**row, source_ids=[str(source)]) for row, source in zip(rows, sources)])
    artifact('attempt-observers', 'context/attempts.jsonl', 'attempts', 'jsonl', 'attempt-observer-stream', 'state', bindings)
    receipts = []
    for i, row_set in enumerate([rows] if transport == 'fetch' else [[row] for row in rows]):
        receipts.append(dict(action='accepted', token=tokens[i],
            attempt_id=('attempt-original-' if transport == 'fetch' else transport + ':') + str(i+1),
            group_id='group-original-' + str(i+1), transport=transport, operation_id='call-original' if transport == 'fetch' else None,
            rows=deepcopy(row_set), elapsed_ms=30))
    artifact('receipt-observers', 'context/receipts.jsonl', 'receipts', 'jsonl', 'receipt-stream', 'broker', receipts)
    samples = [dict(barrier_id='pre_injection', snapshot_id='sample-pre', read_state='complete', rows=[])]
    if transport == 'fetch':
        samples.append(dict(barrier_id='fetch_result_held', snapshot_id='sample-held', read_state='complete', rows=deepcopy(rows)))
    samples.append(dict(barrier_id='final', snapshot_id='sample-final', read_state='complete', rows=[]))
    artifact('sample-observers', 'context/queue-samples.jsonl', 'queue-samples', 'jsonl', 'sample-stream', 'queue', samples)
    artifact('contract', 'context/contracts.jsonl', 'contracts', 'jsonl', 'contract', 'contract', [dict(
        barrier_id=barrier, negotiation='v1', channel=transport=='channel', inbox=transport=='inbox', receipt_type='transcript',
        fetch_policy='receipt', accepted_modes=['fetch_receipt'] if transport=='fetch' else [transport, 'fetch_receipt']) for barrier in ('injection', 'final')])
    journal = [dict(action='cut', barrier_id='pre_injection', cuts=[]),
               dict(action='state_sample', observed_state='idle'),
               dict(action='session_sample', operation='resume', host_session_ref=scope['host_session_id'], transcript_records=1),
               dict(action='cut', barrier_id='injection', cuts=[])]
    if transport == 'fetch':
        journal.extend([dict(action='fetch_request', frame=dict(jsonrpc='2.0', id='rpc-original', method='tools/call',
                                  params=dict(name='fetch_queue', arguments=dict(ack=True))), host_record_id=records[1]['uuid'], operation_id='call-original'),
                        dict(action='cut', barrier_id='fetch_result_held', cuts=[]),
                        dict(action='fetch_release', artifact_id='held-fetch-response', request_id='rpc-original', operation_id='call-original')])
    journal += [dict(action='window_end', observed_state='idle'), dict(action='cut', barrier_id='final', cuts=[])]
    artifact('state', 'context/driver.jsonl', 'driver', 'jsonl', 'driver', 'state', journal)
    if transport == 'fetch':
        owners.append(dict(artifact_id='state', stream_id='transport', role='transport', **scope, pid=45671))
        artifact('held-fetch-response', 'held-fetch/response-1.json', 'held', 'json', 'held-stream', 'transport',
                 dict(jsonrpc='2.0', id='rpc-original', result=dict(content=[dict(type='text', text=content)])))
    artifact('ownership-observers', 'context/ownership.json', 'ownership', 'json', 'ownership-stream', 'state', {})
    # Independent observer locators and times. They are never inferred from sample prose.
    for i in range(len(sources)):
        occurrence('broker', i+1, 'admission-log-' + str(i+1), 100+i)
        occurrence('queue', i+1, 'persist-' + str(i+1), 110+i)
    occurrence('injection', 1, 'admission', 102)
    occurrence('adapter', 1, 'adapter-start', 0)
    occurrence('host-records', 1, 'seed-record', 0)
    for i, row_set in enumerate([rows] if transport == 'fetch' else [[row] for row in rows]):
        reserve = len(sources) + 1 + i*2
        terminal = reserve+1
        occurrence('broker', reserve, 'reserve-' + str(i+1), 200+i*100)
        occurrence('broker', terminal, 'terminal-' + str(i+1), 280+i*100)
        binding('broker', reserve, i+1, row_set)
        binding('broker', terminal, i+1, row_set, row_set)
        occurrence('receipt-observers', i+1, 'receipt-' + str(i+1), 270+i*100)
        if transport != 'fetch':
            occurrence('host-records', i+2, 'host-' + str(i+1), 250+i*100, delivery='delivery-original-' + str(i+1))
            binding('host-records', i+2, i+1, row_set)
    if transport == 'fetch':
        occurrence('host-records', 2, 'recorded-call', 180)
        occurrence('host-records', 3, 'host-fetch', 250, delivery='delivery-original-fetch')
        binding('host-records', 3, 1, rows)
        occurrence('held-fetch-response', 1, 'held-fetch-response', 220, delivery='produced-original-fetch')
        binding('held-fetch-response', 1, 1, rows)
        binding('state', 5, 1, rows)
    for i, row in enumerate(journal, 1):
        time = {1:0, 2:20, 3:30, 4:50, 5:180, 6:230, 7:240}.get(i, 70000)
        if row['action'] in ('window_end',) or row.get('barrier_id') == 'final':
            time = 70000
        occurrence('state', i, 'journal-' + str(i), time, stream='transport' if row['action']=='fetch_request' else 'driver')
    for identity in ('attempt-observers', 'sample-observers', 'contract', 'ownership-observers'):
        values = data[identity] if type(data[identity]) is list else [data[identity]]
        for i, _ in enumerate(values, 1):
            occurrence(identity, i, identity + '-observation-' + str(i), 0)
    data['ownership-observers'] = dict(schema_version=1, owners=owners, occurrences=occurrences)
    lengths = {owner['stream_id']: (len(data[owner['artifact_id']].splitlines()) if type(data[owner['artifact_id']]) is str else
                                   len(data[owner['artifact_id']]) if type(data[owner['artifact_id']]) is list else 1) for owner in owners}
    for i, row in enumerate(journal, 1):
        if row['action'] != 'cut':
            continue
        cut = {stream:0 for stream in lengths}
        if row['barrier_id'] == 'pre_injection':
            cut.update(driver=1, host=1, **{'sample-stream':1, 'ownership-stream':1})
        elif row['barrier_id'] == 'injection':
            cut.update(driver=4, host=1, contract=1, **{'sample-stream':1, 'ownership-stream':1})
        elif row['barrier_id'] == 'fetch_result_held':
            cut.update(broker=3, adapter=1, host=2, injection=1, queue=2, contract=1, driver=6, transport=6,
                       **{'sample-stream':2, 'ownership-stream':1, 'attempt-observer-stream':len(bindings), 'held-stream':1})
        else:
            cut = lengths.copy()
        row['cuts'] = [dict(stream_id=key, seq=value) for key, value in cut.items()]
    descriptor = dict(schema_version=1, context_version=1, cell=dict(transport=transport, state='idle', session='resumed', kind='text',
        burst='single' if len(sources)==1 else 'double'), evidence={key:scope[key] for key in ('run_id','route_id','host_session_id','session_id')},
        provenance=dict(kind='raw_replay', capture_id='synthetic-' + transport, broker_build='synthetic', adapter_build='synthetic', host_version='synthetic', platform='synthetic'),
        artifacts=artifacts)
    for entry in artifacts:
        path = root / entry['file']
        path.parent.mkdir(parents=True, exist_ok=True)
        value = data[entry['artifact_id']]
        path.write_text(value if entry['format']=='log' else jsonl(value) if entry['format']=='jsonl' else json_text(value))
    (root / 'capture.json').write_text(json_text(descriptor))
    reseal(root)



def edit_json(root, file, edit, *, line=None):
    path = root / file
    before = path.read_bytes()
    records = [json.loads(value) for value in before.decode().splitlines()] if line is not None else json.loads(before)
    target = records[line-1] if line is not None else records
    edit(target)
    after = (jsonl(records) if line is not None else json_text(records)).encode()
    if after == before:
        raise ValueError('raw mutation did not change its target')
    path.write_bytes(after)


def set_final_extents(root):
    descriptor = json.loads((root/'capture.json').read_text())
    owner = json.loads((root/'context/ownership.json').read_text())
    lengths = {}
    for entry in descriptor['artifacts']:
        path = root/entry['file']
        if path.exists():
            lengths[entry['artifact_id']] = 1 if entry['format']=='json' else len(path.read_text().splitlines())
    journal = [json.loads(line) for line in (root/'context/driver.jsonl').read_text().splitlines()]
    for row in journal:
        if row.get('barrier_id') == 'final':
            row['cuts'] = [dict(stream_id=o['stream_id'], seq=lengths[o['artifact_id']]) for o in owner['owners']]
    (root/'context/driver.jsonl').write_text(jsonl(journal))


def add_host_copy(root, *, identical=False):
    path = root/'records.jsonl'
    records = [json.loads(line) for line in path.read_text().splitlines()]
    record = deepcopy(records[1])
    if not identical:
        record['uuid'] = '11111111-2222-4333-8444-000000000099'
    records.append(record)
    path.write_text(jsonl(records))
    def meta(owner):
        row = deepcopy(next(row for row in owner['occurrences'] if row['artifact_id']=='host-records' and row['line']==2))
        row.update(line=len(records), event_id='host-extra', delivery_id='delivery-original-extra', time_ms=500)
        owner['occurrences'].append(row)
    edit_json(root, 'context/ownership.json', meta)
    binding = next(json.loads(line) for line in (root/'context/attempts.jsonl').read_text().splitlines()
                   if json.loads(line)['artifact_id']=='host-records' and json.loads(line)['line']==2)
    binding['line'] = len(records)
    with (root/'context/attempts.jsonl').open('a') as stream:
        stream.write(jsonl([binding]))
    set_final_extents(root)


def add_persistence(root, *, other_row=False):
    row = dict(row_id='row-other' if other_row else 'row-original-1', revision='c'*64,
               source_ids=['49999' if other_row else '41001'])
    path = root/'context/rows.jsonl'
    line = len(path.read_text().splitlines())+1
    with path.open('a') as out:
        out.write(jsonl([row]))
    def meta(owner):
        template = deepcopy(next(row for row in owner['occurrences'] if row['artifact_id']=='queue' and row['line']==1))
        template.update(line=line, event_id='persist-new-revision', time_ms=120 if other_row else 275)
        owner['occurrences'].append(template)
    edit_json(root, 'context/ownership.json', meta)
    if other_row:
        edit_json(root, 'context/attempts.jsonl', lambda record: record.update(retired_rows=[{k:row[k] for k in ('row_id','revision')}]), line=2)
    set_final_extents(root)


def add_notice(root, *, held):
    broker_path = root/'broker.log'
    line = len(broker_path.read_text().splitlines())+1
    text = '📨 Held — 2 messages queued' if held else 'Live route: inbox'
    with broker_path.open('a') as out:
        out.write('TEST SINK reply topic=881 text=' + json.dumps(text, ensure_ascii=False) + '\n')
    def meta(owner):
        template = deepcopy(next(row for row in owner['occurrences'] if row['artifact_id']=='broker'))
        template.update(line=line, event_id='notice-original', time_ms=600)
        owner['occurrences'].append(template)
    edit_json(root, 'context/ownership.json', meta)
    if held:
        path = root/'context/queue-samples.jsonl'
        samples = [json.loads(line) for line in path.read_text().splitlines()]
        samples.append(dict(barrier_id='notice-cut', event_id='notice-original', snapshot_id='notice-sample', read_state='complete', rows=[]))
        path.write_text(jsonl(samples))
        journal_path = root/'context/driver.jsonl'
        journal = [json.loads(line) for line in journal_path.read_text().splitlines()]
        cut = deepcopy(journal[-1])
        cut.update(barrier_id='notice-cut')
        for value in cut['cuts']:
            if value['stream_id']=='broker': value['seq']=line
            if value['stream_id']=='driver': value['seq']=5
            if value['stream_id']=='sample-stream': value['seq']=3
        journal.insert(4,cut)
        journal_path.write_text(jsonl(journal))
        def notice_meta(owner):
            for record in owner['occurrences']:
                if record['artifact_id']=='state' and record['line']>=5:
                    record['line']+=1
            for artifact, line_number, event in [('state',5,'notice-cut-observation'), ('sample-observers',3,'notice-sample-observation')]:
                template=deepcopy(next(row for row in owner['occurrences'] if row['artifact_id']==artifact))
                template.update(line=line_number,event_id=event,time_ms=600)
                owner['occurrences'].append(template)
        edit_json(root, 'context/ownership.json', notice_meta)
    set_final_extents(root)


# Named required discriminators are authored independently of the verdict core.
INCOMPLETE = 'evidence collection incomplete'
IDENTITY = 'delivery evidence identity mismatch'
TRAILER = 'fetch tool result lacks the complete matching receipt trailer'
NO_FETCH = 'no successful fetch tool-result record'
ONCE = 'host did not receive each source exactly once'
RETIRED = 'retired rows do not match authorized delivery members'


def author_variants(corpus):
    """Explicit authoring operation; no expected observation/result generation."""
    def variant(name, base, operation, reasons, *, reseal_artifacts=True, healthy=False, identity=None):
        source = corpus/base
        target = corpus/name if healthy else corpus/'mutations'/name
        copy_capture(source,target)
        before = {entry['file']:(target/entry['file']).read_bytes() for entry in json.loads((target/'capture.json').read_text())['artifacts']}
        operation(target)
        if reseal_artifacts:
            reseal(target)
        changes=[]
        for file,data in before.items():
            path=target/file
            after=path.read_bytes() if path.exists() else None
            if after!=data:
                changes.append(dict(file=file,before=hashlib.sha256(data).hexdigest(),after=hashlib.sha256(after).hexdigest() if after is not None else None))
        if not changes:
            raise ValueError('mutation changed no raw artifact')
        manifest = dict(base=base, changes=changes, required_reasons=reasons)
        if identity:
            manifest['identity'] = identity
        (target/'mutation.json').write_text(json_text(manifest))
    channel='healthy-channel-double'; fetch='healthy-fetch-double'; inbox='healthy-inbox-single'
    variant('healthy-channel-repeated',channel,lambda p:add_host_copy(p,identical=True),[],healthy=True)
    def permutation(p):
        old='member row-original-1 '+'a'*64+'\\nmember row-original-2 '+'b'*64
        new='member row-original-2 '+'b'*64+'\\nmember row-original-1 '+'a'*64
        replace_exact(p,'records.jsonl',old,new)
    variant('healthy-fetch-permuted',fetch,permutation,[],healthy=True)
    variant('wrong-terminal-token',channel,lambda p:replace_exact(p,'broker.log',
        'token=token-original-one topic=881 transport=channel phase=confirmed','token=token-terminal-wrong topic=881 transport=channel phase=confirmed'),
        ['receipt missing or outside 15-second window',IDENTITY])
    variant('wrong-recorded-fetch-token',fetch,lambda p:replace_exact(p,'records.jsonl','group token-original-one','group token-recorded-wrong'),[NO_FETCH,TRAILER])
    variant('wrong-held-response-token',fetch,lambda p:replace_exact(p,'held-fetch/response-1.json','group token-original-one','group token-held-wrong'),[TRAILER,IDENTITY])
    variant('wrong-host-session',channel,lambda p:edit_json(p,'records.jsonl',lambda r:r.update(sessionId='session-host-wrong'),line=2),[IDENTITY,ONCE])
    def owner_change(p, artifact, field, value, line=None):
        def edit(owner):
            table='owners' if line is None else 'occurrences'
            row=next(row for row in owner[table] if row['artifact_id']==artifact and (line is None or row['line']==line))
            row[field]=value
        edit_json(p,'context/ownership.json',edit)
    variant('misattributed-transcript',channel,lambda p:owner_change(p,'host-records','host_session_id','session-host-wrong'),[INCOMPLETE])
    variant('wrong-broker-session',channel,lambda p:owner_change(p,'receipt-observers','session_id','session-broker-wrong',1),[IDENTITY])
    variant('wrong-terminal-epoch',channel,lambda p:owner_change(p,'broker','connection_epoch_id','epoch-other',4),[IDENTITY])
    variant('reused-pid-different-epoch',channel,lambda p:owner_change(p,'receipt-observers','connection_epoch_id','epoch-other',1),[IDENTITY])
    variant('duplicate-host-delivery',channel,add_host_copy,[ONCE])
    variant('duplicate-source-replacing-another',channel,lambda p:replace_exact(p,'records.jsonl','message_id=\\"41002\\"','message_id=\\"41001\\"'),[ONCE,IDENTITY])
    variant('duplicate-fetch-row',fetch,lambda p:replace_exact(p,'records.jsonl','member row-original-2','member row-original-1'),[TRAILER,NO_FETCH])
    variant('wrong-recorded-fetch-row',fetch,lambda p:replace_exact(p,'records.jsonl','member row-original-1','member row-recorded-wrong'),[TRAILER,NO_FETCH])
    variant('stale-host-revision',fetch,lambda p:replace_exact(p,'records.jsonl','a'*64,'c'*64),[NO_FETCH,TRAILER])
    variant('stale-retired-revision',channel,add_persistence,[RETIRED])
    variant('wrong-retired-row',channel,lambda p:add_persistence(p,other_row=True),[RETIRED])
    for name,file in [('broker','broker.log'),('host','records.jsonl'),('held','held-fetch/response-1.json'),
                      ('ownership','context/ownership.json'),('rows','context/rows.jsonl'),('attempts','context/attempts.jsonl'),
                      ('receipts','context/receipts.jsonl'),('queue-samples','context/queue-samples.jsonl'),('contracts','context/contracts.jsonl'),
                      ('driver','context/driver.jsonl'),('injection','context/injection.json'),('adapter','adapter.log')]:
        variant('missing-'+name,fetch,lambda p,file=file:(p/file).unlink(),[INCOMPLETE])
    variant('truncated-broker-line',channel,lambda p:(p/'broker.log').write_bytes((p/'broker.log').read_bytes()[:-10]),[INCOMPLETE],reseal_artifacts=False)
    variant('truncated-broker-suffix',channel,lambda p:(p/'broker.log').write_bytes(b'\n'.join((p/'broker.log').read_bytes().split(b'\n')[:-2])+b'\n'),[INCOMPLETE],reseal_artifacts=False)
    variant('malformed-broker-field',channel,lambda p:replace_exact(p,'broker.log','phase=confirmed members=1 retired=1 elapsed_ms=40','phase=confirmed retired=1 elapsed_ms=40',occurrences=2),[INCOMPLETE])
    variant('malformed-broker-integer',channel,lambda p:replace_exact(p,'broker.log','phase=reserved members=1','phase=reserved members=bad',occurrences=2),[INCOMPLETE])
    for name,data in [('json',b'{broken\n'),('nonobject',b'[]\n'),('utf8',b'\xff\n'),('tail',b'{"type":"user"}'),('shape',b'{"type":"user","message":null}\n')]:
        variant('malformed-host-'+name,channel,lambda p,data=data:(p/'records.jsonl').write_bytes(data),[INCOMPLETE])
    variant('conflicting-host-uuid',channel,lambda p:replace_exact(p,'records.jsonl','11111111-2222-4333-8444-000000000002','11111111-2222-4333-8444-000000000001'),[INCOMPLETE,ONCE])
    variant('missing-final-boundary',channel,lambda p:(p/'context/driver.jsonl').write_text('\n'.join((p/'context/driver.jsonl').read_text().splitlines()[:-1])+'\n'),[INCOMPLETE])
    def short_cut(p):
        edit_json(p,'context/driver.jsonl',lambda row:next(v for v in row['cuts'] if v['stream_id']=='host').update(seq=4),line=6)
    variant('short-final-cut',channel,short_cut,[INCOMPLETE])
    variant('premature-fetch-consumption',fetch,lambda p:edit_json(p,'context/queue-samples.jsonl',lambda r:r.update(rows=[]),line=2),
            ['rows retired before host tool-result receipt (baseline consume-on-fetch)'])
    variant('premature-terminal-retirement',channel,lambda p:owner_change(p,'broker','time_ms',260,4),['rows retired before contract evidence'])
    variant('rejected-peer-provenance',inbox,lambda p:edit_json(p,'records.jsonl',lambda r:r.pop('origin'),line=2),[ONCE])
    variant('rejected-peer-prefix',inbox,lambda p:replace_exact(p,'records.jsonl','Another Claude session sent a message:\\n',''),[ONCE])
    variant('false-held',channel,lambda p:add_notice(p,held=True),['no false Held assertion failed or missing'])
    variant('automatic-route-line',channel,lambda p:add_notice(p,held=False),['route line count assertion failed or missing'])

    # Each contradiction names its raw bad claim, the canonical claim (when a
    # successful shape exists), and the identity domain diagnosed by the join.
    def contradiction(name, base, operation, file, line, path, bad, field, *,
                      milestone=None, canonical_path=None, contains=False, absent=None):
        identity = dict(file=file, line=line, path=path, bad=bad, field=field, contains=contains)
        if milestone:
            identity.update(milestone=milestone, canonical_path=canonical_path)
        if absent:
            identity['absent'] = absent
        variant('occurrence-' + name, base, operation, [INCOMPLETE], identity=identity)

    for transport, base in (('channel', channel), ('inbox', inbox)):
        old, bad = transport + ':1', transport + ':999'
        contradiction(transport+'-attempt', base,
            lambda p,old=old,bad=bad:replace_exact(p,'records.jsonl',old,bad),
            'records.jsonl',2,['message','content'],bad,'attempt_id', contains=True,
            milestone='transcript_recorded',canonical_path=['attempt_id'])
    contradiction('host-token', channel,
        lambda p:replace_exact(p,'records.jsonl','token-original-one','token-host-wrong'),
        'records.jsonl',2,['message','content'],'token-host-wrong','token',contains=True,
        milestone='transcript_recorded',canonical_path=['token'])
    def host_transport(p):
        edit_json(p,'records.jsonl',lambda r:r['message'].update(content=r['message']['content'].replace(
            'Another Claude session sent a message:\n','').replace('inbox:1','channel:1')),line=2)
    contradiction('host-transport',inbox,host_transport,'records.jsonl',2,['message','content'],
        'channel:1','transport',contains=True, milestone='transcript_recorded',canonical_path=['attempt_id'])
    contradiction('host-session',channel,
        lambda p:edit_json(p,'records.jsonl',lambda r:r.update(sessionId='session-occurrence-wrong'),line=2),
        'records.jsonl',2,['sessionId'],'session-occurrence-wrong','host_session_id',
        milestone='transcript_recorded',canonical_path=['scope','host_session_id'])
    contradiction('host-session-alias',channel,
        lambda p:edit_json(p,'records.jsonl',lambda r:r.update(session_id='session-alias-wrong'),line=2),
        'records.jsonl',2,['session_id'],'session-alias-wrong','host_session_id')
    contradiction('host-source',channel,
        lambda p:replace_exact(p,'records.jsonl','message_id=\\"41001\\"','message_id=\\"49999\\"'),
        'records.jsonl',2,['message','content'],'49999','source_ids',contains=True,
        milestone='transcript_recorded',canonical_path=['members',0,'source_ids',0])
    for name, attribute, value, route in (('chat','chat_id','-772','-772/881'),('topic','message_thread_id','882','-771/882')):
        contradiction('host-'+name,channel,
            lambda p,attribute=attribute,value=value:edit_json(p,'records.jsonl',lambda r:r['message'].update(
                content=r['message']['content'].replace('<channel ',f'<channel {attribute}="{value}" ')),line=2),
            'records.jsonl',2,['message','content'],value,'route_id',contains=True)
    for field, before, bad in (('token','token-original-one','token-broker-wrong'),('transport','channel','inbox'),('topic','881','882')):
        old=f'token=token-original-one topic=881 transport=channel phase=confirmed'
        new=old.replace(field+'='+before,field+'='+bad)
        contradiction('broker-'+field,channel,lambda p,old=old,new=new:replace_exact(p,'broker.log',old,new),
            'broker.log',4,[],field+'='+bad,'route_id' if field=='topic' else field,contains=True)
    contradiction('broker-source',channel,
        lambda p:replace_exact(p,'broker.log','message_id=41001','message_id=49999'),
        'broker.log',1,[],'message_id=49999','source_ids',contains=True)
    for name, old, bad, field in (('row','row-original-1','row-fetch-wrong','row_id'),('revision','a'*64,'c'*64,'revision')):
        for target in ('held','recorded'):
            def change(p,old=old,bad=bad,target=target):
                replace_exact(p,'held-fetch/response-1.json',old,bad)
                if target=='recorded':
                    replace_exact(p,'records.jsonl',old,bad)
            path = ['result','content',0,'text'] if target=='held' else ['message','content',0,'content']
            member_index = 1 if target == 'recorded' else 0  # Unknown pairs follow independently observed rows.
            canonical_path = ['members',member_index,'row_id'] if name=='row' else ['members',member_index,'revision','value']
            contradiction(target+'-'+name,fetch,change,
                'held-fetch/response-1.json' if target=='held' else 'records.jsonl', None if target=='held' else 3,
                path,bad,'members',contains=True,
                milestone='fetch_result_produced' if target=='held' else 'fetch_result_recorded',canonical_path=canonical_path)
    contradiction('recorded-token',fetch,
        lambda p:replace_exact(p,'records.jsonl','group token-original-one','group token-recorded-wrong'),
        'records.jsonl',3,['message','content',0,'content'],'token-recorded-wrong','token',contains=True,absent='fetch_result_recorded')
    contradiction('recorded-operation',fetch,
        lambda p:edit_json(p,'records.jsonl',lambda r:r['message']['content'][0].update(tool_use_id='call-recorded-wrong'),line=3),
        'records.jsonl',3,['message','content',0,'tool_use_id'],'call-recorded-wrong','operation_id',absent='fetch_result_recorded')
    for name, path, bad, field in (('operation',['message','content',0,'id'],'call-occurrence-wrong','operation_id'),
                                    ('uuid',['uuid'],'11111111-2222-4333-8444-000000000098','host_record_id')):
        def change(p,name=name,bad=bad):
            edit_json(p,'records.jsonl',lambda r:(r['message']['content'][0].update(id=bad) if name=='operation' else r.update(uuid=bad)),line=2)
        contradiction('call-'+name,fetch,change,'records.jsonl',2,path,bad,field,absent='fetch_requested')
    contradiction('held-request',fetch,
        lambda p:edit_json(p,'held-fetch/response-1.json',lambda r:r.update(id='rpc-occurrence-wrong')),
        'held-fetch/response-1.json',None,['id'],'rpc-occurrence-wrong','request_id')
    for field, bad in (('operation_id','call-release-wrong'),('request_id','rpc-release-wrong'),('artifact_id','response-release-wrong')):
        contradiction('release-'+field,fetch,
            lambda p,field=field,bad=bad:edit_json(p,'context/driver.jsonl',lambda r:r.update({field:bad}),line=7),
            'context/driver.jsonl',7,[field],bad,field)
    for field, bad in (('attempt_id','attempt-receipt-wrong'),('group_id','group-receipt-wrong'),('operation_id','call-receipt-wrong')):
        contradiction('receipt-'+field,fetch,
            lambda p,field=field,bad=bad:edit_json(p,'context/receipts.jsonl',lambda r:r.update({field:bad}),line=1),
            'context/receipts.jsonl',1,[field],bad,field,milestone='receipt_accepted',canonical_path=[field])
    for field, bad in (('run_id','run-occurrence-wrong'),('route_id','-772/881'),('host_session_id','host-occurrence-wrong'),
                       ('session_id','broker-occurrence-wrong'),('connection_epoch_id','epoch-occurrence-wrong'),('claim_generation',999)):
        # Ownership occurrence facts are the only version-1 observations of
        # broker sessions, epochs and generations; TEST ATTEMPT has no such fields.
        base_owner=json.loads((corpus/channel/'context/ownership.json').read_text())
        index=next(i for i,r in enumerate(base_owner['occurrences']) if r['artifact_id']=='broker' and r['line']==4)
        contradiction('scope-'+field,channel,
            lambda p,field=field,bad=bad:owner_change(p,'broker',field,bad,4),
            'context/ownership.json',None,['occurrences',index,field],bad,field,
            milestone='attempt_terminal',canonical_path=['scope',field])
    contradiction('persisted-source',channel,
        lambda p:edit_json(p,'context/rows.jsonl',lambda r:r.update(row_id='row-original-1',revision='a'*64),line=2),
        'context/rows.jsonl',2,['source_ids',0],'41002','source_ids')
    contradiction('selected-session',channel,
        lambda p:edit_json(p,'context/driver.jsonl',lambda r:r.update(host_session_ref='selected-session-wrong'),line=3),
        'context/driver.jsonl',3,['host_session_ref'],'selected-session-wrong','host_session_id')
    def cut_change(p, line, stream, value):
        edit_json(p,'context/driver.jsonl',lambda row:next(v for v in row['cuts'] if v['stream_id']==stream).update(seq=value),line=line)
    variant('short-injection-cut',channel,lambda p:cut_change(p,4,'host',4),[INCOMPLETE])
    variant('short-held-cut',fetch,lambda p:cut_change(p,6,'host',4),[INCOMPLETE])
    variant('early-final-cut',channel,lambda p:cut_change(p,6,'host',2),[INCOMPLETE])


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write-raw', type=Path, required=True)
    args = parser.parse_args()
    if args.write_raw.exists():
        parser.error('the authoring destination must not exist; frozen expectations are never overwritten')
    for transport, burst in (('channel', 'double'), ('inbox', 'single'), ('fetch', 'double')):
        control(args.write_raw / f'healthy-{transport}-{burst}', transport)
    author_variants(args.write_raw)
