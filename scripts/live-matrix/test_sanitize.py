"""Identity and security tests; no live hosts or replay loader."""
import copy
import hashlib
from itertools import product
import json
from pathlib import Path
import tempfile
import traceback
import unittest
from unittest.mock import Mock, patch

from collect import (RedactionContext, RedactionError, sanitize, sanitize_legacy_fixture,
                     collect, classify, classify_fetch, fetch_trailer, export_fixtures)
from redaction import REGISTRY
from matrix import Cell


class IdentityTests(unittest.TestCase):
    def test_registry_equality_inequality_and_text_aliases(self):
        for domain, definition in REGISTRY.items():
            with self.subTest(domain=domain):
                a, b = 'private-canary-' + domain + '-alpha', 'private-canary-' + domain + '-beta'
                if domain == 'revision':
                    a, b = 'a' * 64, 'b' * 64
                if domain == 'path':
                    a, b = '/home/synthetic/private-alpha', '/tmp/private-beta'
                aliases = [alias for alias in definition.aliases if alias not in ('session', 'merged_message_ids')]
                raw = [{alias: [a, b, a]} for alias in aliases] + [{'text': a + ' ' + b + ' ' + a}]
                clean = sanitize(raw)
                expected = clean[0][aliases[0]]
                self.assertEqual(expected[0], expected[2])
                self.assertNotEqual(expected[0], expected[1])
                for alias, record in zip(aliases, clean):
                    self.assertEqual(record[alias], expected)
                self.assertEqual(clean[-1]['text'], ' '.join(expected))
                self.assertNotIn(a, json.dumps(clean))
                self.assertNotIn(b, json.dumps(clean))
                if domain == 'revision':
                    for value in expected:
                        self.assertRegex(value, r'^[0-9a-f]{64}$')

    def test_identity_spelling_is_not_normalized(self):
        originals = ['Case-Canary', 'case-canary', ' Case-Canary', 'Case-Canary ', 'Case-Canary']
        clean = sanitize({'token': originals})['token']
        self.assertEqual(len(set(clean)), 4)
        self.assertEqual(clean[0], clean[-1])
        self.assertFalse(set(clean) & set(originals))
        sources = sanitize({'source_ids': ['0', '-0', '00', '+0', ' 0']})['source_ids']
        self.assertEqual(len(set(sources)), 5)
        ctx = RedactionContext().discover({'token': 'known-canary'}).seal()
        with self.assertRaisesRegex(RedactionError, 'not discovered'):
            sanitize('unknown-private-canary', tokens={'unknown-private-canary'}, context=ctx)

    def test_raw_canonical_host_namespaces(self):
        host = {'uuid': 'shared-canary', 'parentUuid': 'other-canary', 'sessionId': 'session-canary',
                'session_id': 'session-canary', 'message': {'content': [
                    {'type': 'tool_use', 'id': 'call-canary'}, {'type': 'tool_result', 'tool_use_id': 'call-canary'}]}}
        canonical = {'session_id': 'session-canary', 'host_session_id': 'session-canary',
                     'host_record_id': 'shared-canary', 'operation_id': 'call-canary',
                     'c3_delivery_id': 'delivery-canary', 'delivery_id': 'delivery-canary',
                     'record_id': 'shared-canary', 'opaque_id': 'shared-canary'}
        ctx = RedactionContext().discover(host, schema='host').discover(canonical, schema='canonical').seal()
        h = sanitize(host, context=ctx, schema='host')
        c = sanitize(canonical, context=ctx, schema='canonical')
        self.assertEqual(h['session_id'], h['sessionId'])
        self.assertEqual(h['sessionId'], c['host_session_id'])
        self.assertNotEqual(c['host_session_id'], c['session_id'])
        self.assertEqual(h['uuid'], c['host_record_id'])
        self.assertNotEqual(h['uuid'], c['record_id'])
        self.assertEqual(h['message']['content'][0]['id'], c['operation_id'])
        self.assertEqual(h['message']['content'][1]['tool_use_id'], c['operation_id'])
        self.assertTrue(c['c3_delivery_id'].startswith('TOKEN'))
        self.assertTrue(c['delivery_id'].startswith('DELIVERY'))
        self.assertNotEqual(c['c3_delivery_id'], c['delivery_id'])
        self.assertEqual(ctx.pseudonym('session', 'session-canary'), c['session_id'])

    def test_numeric_source_keys_lists_and_attributes(self):
        raw = {'message_ids': [1, 2, 0, 1], 'received': {'1': 2, '2': 1, '0': 0},
               'source_ids': ['1', '2', '0', '1'], 'merged_message_ids': '1,2,1,0',
               'text': '<channel message_id="1" merged_message_ids="1,2,1,0" reply_to_message_id="2">x</channel>',
               'MessageID': 1.0, 'count': 1, 'seq': 2, 'claim_generation': 2, 'elapsed_ms': 1}
        clean = sanitize(raw)
        ids = clean['message_ids']
        self.assertEqual(ids[0], ids[3])
        self.assertEqual(len(set(ids)), 3)
        for original, replacement in zip(raw['message_ids'], ids):
            self.assertNotEqual(original, replacement)
            self.assertIs(type(replacement), int)
        self.assertEqual(clean['source_ids'], list(map(str, ids)))
        self.assertEqual(clean['received'], {str(ids[0]): 2, str(ids[1]): 1, str(ids[2]): 0})
        merged = ','.join(str(ids[i]) for i in (0, 1, 0, 2))
        self.assertEqual(clean['merged_message_ids'], merged)
        self.assertIn(f'merged_message_ids="{merged}"', clean['text'])
        self.assertIn(f'message_id="{ids[0]}"', clean['text'])
        self.assertEqual(clean['MessageID'], float(ids[0]))
        self.assertIs(type(clean['MessageID']), float)
        for field in ('count', 'seq', 'claim_generation', 'elapsed_ms'):
            self.assertEqual(clean[field], raw[field])

    def test_numeric_types_and_invalid_encodings(self):
        for field in ('source_id', 'chat_id', 'topic_id', 'user_id', 'pid', 'verifiedPeerProcStart'):
            with self.subTest(field=field):
                raw = {field: [None, '', False, True, 0, 1, 1.0, -1, '1', '01', '+1', ' 1']}
                clean = sanitize(raw)[field]
                self.assertEqual(clean[:4], raw[field][:4])
                self.assertEqual(clean[5], clean[6])
                self.assertEqual(str(clean[5]), clean[8])
                self.assertLess(clean[7], 0)
                self.assertNotEqual(clean[7], -1)
                self.assertEqual(len(set(clean[8:])), 4)
                for bad in (1.5, float('nan'), float('inf'), float(2**54)):
                    with self.assertRaisesRegex(RedactionError, 'unsupported numeric identity encoding'):
                        sanitize({field: bad})
        rpc = sanitize([{'jsonrpc': '2.0', 'id': 1}, {'jsonrpc': '2.0', 'id': '1'}])
        self.assertIs(type(rpc[0]['id']), int)
        self.assertIs(type(rpc[1]['id']), str)
        self.assertNotEqual(str(rpc[0]['id']), rpc[1]['id'])

    def test_public_attempts_provenance_and_scalars(self):
        raw = {'attempt': 'inbox:3', 'c3_attempt': 'cross-session:3', 'attempt_id': 'inbox:3',
               'claim_generation': [1, 2], 'seq': 3, 'slot': 0, 'count': 2, 'byte_offset': 987,
               'duration_ms': 60000, 'elapsed_ms': 15.5, 'schema_version': 1, 'origin': {'kind': 'peer', 'from': 'c3'},
               'isMeta': True, 'operation': 'enqueue', 'text': 'Another Claude session sent a message:\n<channel source="plugin:c3:c3" c3_attempt="inbox:3">sample</channel>'}
        clean = sanitize(raw)
        self.assertEqual(clean['attempt'], raw['attempt'])
        self.assertNotEqual(clean['attempt_id'], raw['attempt_id'])
        self.assertEqual({k: v for k, v in clean.items() if k != 'attempt_id'},
                         {k: v for k, v in raw.items() if k != 'attempt_id'})

    def test_structural_definitions_and_references(self):
        for collection, reference in (('events', 'event_ids'), ('artifacts', 'artifact_ids'), ('streams', 'stream_ids'),
                                      ('barriers', 'barrier_id'), ('queue_snapshots', 'queue_snapshot_id'), ('checkpoints', 'checkpoint_id')):
            with self.subTest(collection=collection):
                raw = {collection: [{'id': 'private-structural-a'}, {'id': 'private-structural-b'}],
                       reference: ['private-structural-a', 'private-structural-b', 'private-structural-a']}
                clean = sanitize(raw, schema='canonical')
                self.assertEqual(clean[reference], [clean[collection][i]['id'] for i in (0, 1, 0)])
                self.assertNotEqual(clean[reference][0], clean[reference][1])
        raw = {'barriers': [{'id': 'final', 'name': 'final'}, {'id': 'private-barrier', 'name': 'private-barrier'}],
               'final_barrier_id': 'final', 'streams': [{'id': 'private-stream'}], 'stream_cutoffs': {'private-stream': 9}}
        clean = sanitize(raw, schema='canonical')
        self.assertEqual(clean['barriers'][0], raw['barriers'][0])
        self.assertEqual(clean['barriers'][1]['name'], clean['barriers'][1]['id'])
        self.assertEqual(clean['stream_cutoffs'], {clean['streams'][0]['id']: 9})

    def test_overlap_collision_and_nonoverlapping_rendering(self):
        raw = {'token': ['secret-a', 'secret-ab', 'TOKEN1'], 'row_id': ['row-a', 'row-ab', 'ROW1'],
               'uuid': ['ID1', 'private-record'], 'path': ['/work/PATH1', '/home/private/other'],
               'text': 'secret-ab secret-a TOKEN1 row-ab row-a ROW1 ID1 /work/PATH1'}
        ctx = RedactionContext(tokens=set(raw['token']), receipt_ids=raw['row_id']).discover(raw).seal()
        clean = sanitize(raw, context=ctx)
        expected = [clean['token'][1], clean['token'][0], clean['token'][2], clean['row_id'][1],
                    clean['row_id'][0], clean['row_id'][2], clean['uuid'][0], clean['path'][0]]
        self.assertEqual(clean['text'], ' '.join(expected))
        for field in ('token', 'row_id', 'uuid', 'path'):
            self.assertEqual(len(set(clean[field])), len(raw[field]))
            self.assertFalse(set(clean[field]) & set(raw[field]))
        for secret in raw['token'][:2] + raw['row_id'][:2]:
            self.assertNotIn(secret, json.dumps(clean))

    def test_revision_candidates_skip_all_originals(self):
        raw = {'revision': ['a' * 64, format(1, '064x'), format(2, '064x'), 'a' * 64]}
        values = sanitize(raw)['revision']
        self.assertEqual(values, [format(n, '064x') for n in (3, 4, 5, 3)])

    def test_discovery_order_and_determinism(self):
        raw = {'z': 'late-canary early-canary', 'token': ['early-canary', 'late-canary'],
               'a': 'late-canary early-canary'}
        original = copy.deepcopy(raw)
        encoded = []
        for fields in (list(raw), list(reversed(raw))):
            for hints in (['early-canary', 'late-canary'], ['late-canary', 'early-canary']):
                ctx = RedactionContext(tokens=hints).discover({field: raw[field] for field in fields}).seal()
                clean = sanitize(raw, context=ctx)
                self.assertEqual(clean['a'], 'TOKEN1 TOKEN2')
                encoded.append(json.dumps(clean))
        self.assertEqual(len(set(encoded)), 1)
        self.assertEqual(raw, original)
        with self.assertRaisesRegex(RedactionError, 'already sealed'):
            ctx.discover({'token': 'new-capture-secret'})
        with self.assertRaisesRegex(RedactionError, 'not discovered'):
            sanitize({'token': 'new-capture-secret'}, context=ctx)

    def test_paths_uuids_rejected_candidates_and_composite_routes(self):
        a, b = 'a3cdbbbb-aaaa-bbbb-cccc-123456789abc', 'b3cdbbbb-aaaa-bbbb-cccc-123456789abc'
        raw = {'records': [{'uuid': a, 'sessionId': 'rejected-session-canary', 'accept': False,
                            'content': 'c3_delivery_id="rejected-token-canary"'}, {'uuid': b}],
               'path': '/opt/private-canary/with space', 'text': a + ' ' + b + ' /opt/private-canary/with space',
               'routes': [{'route_id': '-123/987', 'ChatID': -123, 'TopicID': 987},
                          {'route_id': '-123/988'}, {'route_id': '-123/dm'}]}
        clean = sanitize(raw)
        self.assertEqual(clean['text'], clean['records'][0]['uuid'] + ' ' + clean['records'][1]['uuid'] + ' ' + clean['path'])
        self.assertNotEqual(clean['records'][0]['uuid'], clean['records'][1]['uuid'])
        self.assertNotIn('canary', json.dumps(clean))
        chat, topic = clean['routes'][0]['route_id'].split('/')
        self.assertEqual(chat, str(clean['routes'][0]['ChatID']))
        self.assertEqual(topic, str(clean['routes'][0]['TopicID']))
        self.assertNotEqual(clean['routes'][0]['route_id'], clean['routes'][1]['route_id'])
        self.assertEqual(clean['routes'][2]['route_id'], chat + '/dm')
        unknown = sanitize({'text': a + ' ' + b + ' ' + a})['text'].split()
        self.assertEqual(unknown[0], unknown[2])
        self.assertNotEqual(unknown[0], unknown[1])

    def test_path_prefixes_spaces_and_embedded_registered_values(self):
        raw = {'content': '/home/private folder/project /home/private folder/project-two',
               'path': ['/home/private folder/project', '/home/private folder/project-two'],
               'token': 'long-private-canary-token', 'detail': 'prefixlong-private-canary-tokensuffix'}
        clean = sanitize(raw)
        self.assertEqual(clean['content'], ' '.join(clean['path']))
        self.assertNotEqual(clean['path'][0], clean['path'][1])
        self.assertEqual(clean['detail'], 'prefix' + clean['token'] + 'suffix')
        simple = sanitize({'path': '/tmp/path', 'text': '/tmp/path-extra /tmp/path'})
        self.assertNotEqual(*simple['text'].split())
        self.assertEqual(simple['text'].split()[1], simple['path'])

    def test_pid_reuse_does_not_merge_epochs(self):
        raw = [{'pid': 42, 'connection_epoch_id': 97}, {'pid': 42, 'connection_epoch_id': 98},
               {'text': 'pid=42 conn=97'}, {'text': 'pid=42 conn=98'}]
        clean = sanitize(raw, schema='broker')
        self.assertEqual(clean[0]['pid'], clean[1]['pid'])
        self.assertNotEqual(clean[0]['connection_epoch_id'], clean[1]['connection_epoch_id'])
        for index in (0, 1):
            self.assertEqual(clean[index + 2]['text'], f"pid={clean[index]['pid']} conn={clean[index]['connection_epoch_id']}")

    def test_invalid_identities_and_public_exports_fail_safely(self):
        ctx = RedactionContext().discover({'pid': False, 'uuid': None, 'source_ids': ['', '1']}).seal()
        self.assertEqual(len(ctx.diagnostics), 1)
        self.assertIn('invalid boolean identity retained', ctx.diagnostics[0])
        for raw in ({'unregistered_private_id': 'long-private-canary'},
                    {'token': 'long-private-canary', 'type': 'long-private-canary'}):
            with self.assertRaises(RedactionError) as error:
                sanitize(raw)
            self.assertNotIn('long-private-canary', str(error.exception))
            self.assertNotIn('unregistered_private_id', str(error.exception))
        # Registered map keys are now supported; prove the secret is removed
        # and its relationship to the typed value survives.
        clean = sanitize({'token': 'long-private-canary', 'long-private-canary': 1})
        self.assertEqual(clean[clean['token']], 1)
        self.assertNotIn('long-private-canary', json.dumps(clean))

    def test_invalid_attempt_label_does_not_change_peer_classification(self):
        raw = {'type': 'user', 'message': {'role': 'user', 'content':
               '<channel c3_attempt="inbox:invalid" c3_delivery_id="secret-canary">sample</channel>'}}
        self.assertFalse(classify(raw)['accept'])
        clean = sanitize(raw, schema='host')
        self.assertFalse(classify(clean)['accept'])
        self.assertIn('c3_attempt="inbox:!', clean['message']['content'])
        self.assertNotIn('secret-canary', json.dumps(clean))

    def test_quoted_broker_assignments(self):
        raw = {'token': 'private-token', 'record_id': 'private-row', 'session_id': 'private-session',
               'text': 'token="private-token" record_id=private-row session=private-session topic=987 pid=123 route=-123/987'}
        clean = sanitize(raw, schema='broker')
        self.assertIn('token="' + clean['token'] + '"', clean['text'])
        self.assertIn('record_id=' + clean['record_id'], clean['text'])
        self.assertIn('session=' + clean['session_id'], clean['text'])
        self.assertNotIn('private', json.dumps(clean))

    def test_diagnostics_never_include_secret_values(self):
        secret = 'long-sensitive-canary-for-error-coverage'
        for raw in ({'token': secret, 'row_id': secret, 'detail': secret},
                    {'token': {'private-field': [secret]}, 'path': secret}):
            with self.subTest(raw_type=type(raw)), self.assertRaises(RedactionError) as error:
                sanitize(raw)
            self.assertNotIn(secret, str(error.exception))
            self.assertNotIn('private-field', str(error.exception))
        ctx = RedactionContext(tokens=[secret])
        self.assertNotIn(secret, repr(ctx))
        malformed = 'token="' + secret + '\\xZZ"'
        try:
            sanitize(malformed, schema='broker')
        except RedactionError as error:
            self.assertNotIn(secret, ''.join(traceback.format_exception(type(error), error, error.__traceback__)))
        else:
            self.fail('malformed quoted identity was accepted')

    def test_malformed_trailers_stay_malformed(self):
        trailer = '[C3_FETCH_RECEIPT_V1]\ngroup secret-token\nmember secret-row ' + 'a' * 64 + '\n[/C3_FETCH_RECEIPT_V1]'
        variants = [trailer + '\n', trailer + '\nextra', trailer[:-1],
                    trailer.replace('secret-token', 'invalid/token-canary'),
                    trailer.replace('secret-row', 'invalid/row-canary'),
                    trailer.replace('a' * 64, 'invalid-revision-canary'),
                    trailer.replace('secret-token', ''), trailer.replace('secret-row', ''),
                    trailer.replace('[/C3', 'member secret-row ' + 'a' * 64 + '\n[/C3')]
        for raw in variants:
            with self.subTest(variant=variants.index(raw)):
                self.assertIsNone(fetch_trailer(raw))
                clean = sanitize(raw)
                self.assertIsNone(fetch_trailer(clean))
                for secret in ('secret-token', 'secret-row', 'invalid/token-canary', 'invalid/row-canary', 'invalid-revision-canary', 'a' * 64):
                    self.assertNotIn(secret, clean)
        self.assertIsNotNone(fetch_trailer(sanitize(trailer)))


class SecurityRegressionTests(unittest.TestCase):
    def export(self, evidence, *, records=(), broker='', adapter=''):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'broker').mkdir()
            (root / 'control').mkdir()
            (root / 'broker/broker.log').write_text(broker)
            (root / 'control/adapter.log').write_text(adapter)
            host = Mock()
            host.records.return_value = list(records)
            host.events.return_value = []
            host.transcript.return_value = None
            output = root / 'output'
            result = collect(Cell('inbox', 'idle', 'resumed', 'text', 'single'), host, root, output, evidence)
            return result, {str(path.relative_to(output)): path.read_text() for path in output.rglob('*') if path.is_file()}

    def test_numeric_identity_in_diagnostics_is_redacted(self):
        diagnostic = 'source 987 rejected'
        result, files = self.export(dict(message_ids=[987], setup_errors=[diagnostic], run_errors=[diagnostic]),
                                    broker=diagnostic + '\n', adapter=diagnostic + '\n')
        mapped = result['evidence']['message_ids'][0]
        self.assertNotEqual(mapped, 987)
        self.assertEqual(result['reasons'], [f'source {mapped} rejected'])
        for name in ('summary.json', 'observation.json', 'replay/verdict-inputs.json',
                     'broker.log', 'adapter.log', 'replay/broker.log', 'replay/adapter.log'):
            self.assertIn(f'source {mapped} rejected', files[name], name)
        for name, text in files.items():
            self.assertNotIn('987', text, name)
        raw = dict(source_ids=[987, 1], token='count', uuid='source',
                   setup_errors=[diagnostic], run_errors=[diagnostic], reasons=[diagnostic],
                   streams=[dict(detail=diagnostic)], count=987, seq=1, version='2.987.1',
                   text='source 987; source 1; count 987; seq=1; line 1; 987 messages; version 2.987.1; 1987 v987 x987 9870')
        clean = sanitize(raw)
        self.assertEqual(clean['streams'][0]['detail'], clean['setup_errors'][0])
        self.assertEqual(clean['reasons'], clean['run_errors'])
        self.assertEqual(clean['text'], f"source {clean['source_ids'][0]}; source {clean['source_ids'][1]}; "
                         'count 987; seq=1; line 1; 987 messages; version 2.987.1; 1987 v987 x987 9870')
        for field in ('count', 'seq', 'version'):
            self.assertEqual(clean[field], raw[field])
        # Decimal spellings of numeric identities also work outside the source domain.
        for field in ('pid', 'user_id', 'chat_id', 'connection_epoch_id', 'request_id'):
            with self.subTest(field=field):
                clean = sanitize({field: -987, 'detail': 'rejected -987'})
                self.assertEqual(clean['detail'], 'rejected ' + str(clean[field]))
        clean = sanitize({'source_id': 987, 'pid': 987, 'detail': 'source 987 rejected by pid 987'})
        self.assertEqual(clean['detail'], f"source {clean['source_id']} rejected by pid {clean['pid']}")

    def test_numeric_identity_punctuation_boundaries(self):
        delimiters = ('', '.', '/', ':', ',', ';', '(', ')', ' ', '\n', '[', ']', '+', '-')
        for before in delimiters:
            for after in delimiters:
                with self.subTest(before=before, after=after):
                    raw = {'source_id': 987, 'detail': before + '987' + after}
                    clean = sanitize(raw)
                    self.assertNotEqual(clean['source_id'], 987)
                    self.assertEqual(clean['detail'], before + str(clean['source_id']) + after)
        # Typed prose references must still disambiguate domains at punctuation.
        raw = {'source_ids': [986, 987], 'pid': 987,
               'detail': 'source 987. pid 987/ source:987: pid:987.'}
        ctx = RedactionContext().discover({'source_id': 986}).discover(raw).seal()
        clean = sanitize(raw, context=ctx)
        source, pid = clean['source_ids'][1], clean['pid']
        self.assertNotEqual(source, pid)
        self.assertEqual(clean['detail'], f'source {source}. pid {pid}/ source:{source}: pid:{pid}.')

    def test_numeric_identity_versions_longer_numbers_and_counts_unchanged(self):
        protected = ('2.987.1', '2.987', '987.1', '-2.987', '2.987.', '987.1.',
                     '19870', '9876', '0987', '9870', '1987')
        fields = ('count', 'seq', 'rows_final', 'elapsed_ms', 'schema_version', 'claim_generation')
        raw = {'source_id': 987, 'detail': list(protected) + ['source ' + value for value in protected],
               **{field: 987 for field in fields}}
        before = copy.deepcopy(raw)
        ctx = RedactionContext().discover(raw).seal()
        clean = sanitize(raw, context=ctx)
        self.assertNotEqual(clean['source_id'], 987)
        self.assertEqual(clean['detail'], raw['detail'])
        for field in fields:
            self.assertEqual(clean[field], raw[field])
            self.assertIs(type(clean[field]), int)
        ctx.check_export_text(json.dumps(clean), format='json')
        self.assertEqual(raw, before)

    def test_numeric_identity_punctuation_in_production_exports(self):
        diagnostics = ['source 987.', 'source 987. next', 'record:987/block:0',
                       'record:987:block:0', 'path:/x/987/y']
        result, files = self.export(dict(message_ids=[987], setup_errors=diagnostics, run_errors=diagnostics,
                                        diagnostics={value: {'detail': value} for value in diagnostics}),
                                    broker='\n'.join(diagnostics) + '\n', adapter='\n'.join(diagnostics) + '\n')
        mapped = result['evidence']['message_ids'][0]
        self.assertNotEqual(mapped, 987)
        expected = [value.replace('987', str(mapped)) for value in diagnostics]
        self.assertEqual(result['reasons'], expected[:1])
        self.assertEqual(result['evidence']['setup_errors'], expected)
        self.assertEqual(result['evidence']['run_errors'], expected)
        self.assertEqual(result['evidence']['diagnostics'], {value: {'detail': value} for value in expected})
        for name in ('summary.json', 'observation.json', 'replay/verdict-inputs.json',
                     'broker.log', 'adapter.log', 'replay/broker.log', 'replay/adapter.log'):
            for value in expected:
                self.assertIn(value, files[name], name)
        for name, text in files.items():
            for value in diagnostics:
                self.assertNotIn(value, text, name)

    def test_numeric_identity_punctuation_audit_fails_closed(self):
        ctx = RedactionContext().discover({'source_id': 987}).seal()
        for survivor in ('987.', '987/', '987:', 'source 987.', 'record:987/block:0', 'path:/x/987/y'):
            with self.subTest(survivor=survivor):
                with self.assertRaisesRegex(RedactionError, 'private identity remains'):
                    ctx.check_export_text(survivor, schema='broker')
                for leaked in ({'detail': survivor}, {'diagnostics': {survivor: 'rejected'}}):
                    with self.assertRaisesRegex(RedactionError, 'private identity remains'):
                        ctx.check_private_text(leaked)
                    for format in ('json', 'jsonl'):
                        with self.assertRaisesRegex(RedactionError, 'private identity remains'):
                            ctx.check_export_text(json.dumps(leaked) + '\n', format=format)
        for suffix in ('.', '/'):
            with self.assertRaisesRegex(RedactionError, 'private identity remains'):
                ctx.check_export_text('{"detail":"\\u0039\\u0038\\u0037' + suffix + '"}', format='json')

    def test_identity_map_keys_are_redacted_consistently(self):
        result, files = self.export(dict(token='tokA', message_ids=[987], setup_errors=['staging incomplete'],
                                        diagnostics={'tokA': {'detail': 'tokA rejected'}},
                                        arbitrary_map={'987': 'source 987 rejected'}), broker='token=tokA\n')
        token = result['evidence']['token']
        source = result['evidence']['message_ids'][0]
        self.assertEqual(result['evidence']['diagnostics'], {token: {'detail': token + ' rejected'}})
        self.assertEqual(result['evidence']['arbitrary_map'], {str(source): f'source {source} rejected'})
        for name, text in files.items():
            self.assertNotIn('tokA', text, name)
        clean = sanitize({'token': 'message', 'diagnostics': {'message': 'message rejected'}})
        self.assertEqual(clean['diagnostics'], {clean['token']: clean['token'] + ' rejected'})
        for raw_key in ('count 987', 'source 987', 'Another Claude session sent a message:\n'):
            clean = sanitize({'token': raw_key, 'source_id': 987, 'diagnostics': {raw_key: 2},
                              'detail': 'token="' + raw_key + '"'})
            self.assertEqual(clean['diagnostics'], {clean['token']: 2})
            self.assertEqual(clean['detail'], 'token="' + clean['token'] + '"')
            self.assertNotIn(raw_key, clean['diagnostics'])
        # Map ordering, even when keys precede their typed definitions, is stable.
        raw = {'arbitrary_map': {'tokA': 2, 'tokB': 3}, 'token': ['tokB', 'tokA']}
        self.assertEqual(sanitize(raw), sanitize(dict(reversed(list(raw.items())))))
        with self.assertRaisesRegex(RedactionError, 'ambiguous embedded identity domains'):
            sanitize({'token': 'x', 'row_id': 'x', 'diagnostics': {'x': 1}})

    def test_secret_shaped_diagnostic_canaries_are_redacted(self):
        canaries = ['LONG-DIAGNOSTIC-CANARY-UNREGISTERED', 'a7' * 24,
                    'Ab9cD4eF7gH2jK5mN8pQ3rS6tU1vW0xY',
                    'a3cdbbbb-aaaa-bbbb-cccc-123456789abc',
                    *(prefix + '/synthetic-canary' for prefix in
                      ('/home', '/Users', '/tmp', '/private', '/run', '/root', '/var', '/opt'))]
        for canary in canaries:
            with self.subTest(canary=canaries.index(canary)):
                diagnostic = 'backend emitted ' + canary
                result, files = self.export(dict(setup_errors=[diagnostic], run_errors=[diagnostic]))
                self.assertNotEqual(result['reasons'][0], diagnostic)
                for name, text in files.items():
                    self.assertNotIn(canary, text, name)
                raw = {'setup_errors': [diagnostic], 'run_errors': [diagnostic], 'reasons': [diagnostic],
                       'detail': 'state=' + canary,
                       'streams': [{'detail': diagnostic}], 'diagnostics': {canary: diagnostic}}
                clean = sanitize(raw)
                self.assertEqual(clean['streams'][0]['detail'], clean['setup_errors'][0])
                self.assertEqual(clean['setup_errors'], clean['run_errors'])
                self.assertEqual(clean['reasons'], clean['run_errors'])
                self.assertNotIn(canary, json.dumps(clean))

    def test_peer_prefix_word_identities_preserve_classification(self):
        for token, user, uuid in (('session', 'message', 'Another'), ('sent', 'Claude', 'a')):
            with self.subTest(token=token):
                record = ExportTests.peer(token=token, uuid=uuid)
                record['user'] = user
                record['message']['content'] = record['message']['content'].replace('private-user-label-alpha', user)
                raw = {'records': [record], 'token': token, 'user': user, 'uuid': uuid,
                       'detail': f'token={token} user={user} uuid={uuid} sessions messages Anotherwise'}
                before = copy.deepcopy(raw)
                self.assertTrue(classify(record)['accept'])
                clean = sanitize(raw)
                self.assertEqual(raw, before)
                peer = clean['records'][0]
                self.assertTrue(peer['message']['content'].startswith('Another Claude session sent a message:\n'))
                self.assertTrue(classify(peer)['accept'])
                self.assertEqual(classify(peer)['token'], clean['token'])
                self.assertEqual(peer['uuid'], clean['uuid'])
                self.assertIn('user="' + clean['user'] + '"', peer['message']['content'])
                self.assertEqual(clean['detail'], f"token={clean['token']} user={clean['user']} uuid={clean['uuid']} "
                                 'sessions messages Anotherwise')
                for field in ('token', 'user', 'uuid'):
                    self.assertNotEqual(clean[field], raw[field])
                _, files = self.export(dict(setup_errors=['staging incomplete']), records=[record])
                saved = json.loads(files['replay/records.jsonl'])
                self.assertTrue(classify(saved)['accept'])
                self.assertNotEqual(classify(saved)['token'], token)

    def test_post_render_audit_rejects_every_registered_identity_length(self):
        for field, raw in (('token', 'x'), ('user', 'ab'), ('uuid', 'xyz'), ('source_id', 987),
                           ('source_id', 1), ('pid', -7), ('request_id', 42), ('token', 'tiny-secret')):
            with self.subTest(field=field, raw=raw):
                ctx = RedactionContext().discover({field: raw}).seal()
                for leaked in ({'detail': f'rejected {raw}'}, {'diagnostics': {str(raw): 'rejected'}}, {field: raw}):
                    for format in ('json', 'jsonl'):
                        with self.assertRaises(RedactionError) as error:
                            ctx.check_export_text(json.dumps(leaked) + '\n', format=format)
                        self.assertIn('private', str(error.exception))
                    with self.assertRaises(RedactionError):
                        ctx.check_private_text(leaked)
                with self.assertRaises(RedactionError):
                    ctx.check_export_text('rejected ' + str(raw), schema='broker')
        ctx = RedactionContext().discover({'source_id': 987}).seal()
        with self.assertRaises(RedactionError):
            ctx.check_export_text('{"detail":"source \\u0039\\u0038\\u0037 rejected"}', format='json')
        ctx.check_export_text('{"count":987,"version":"2.987","seq":987}', format='json')
        # Detection is independent of discovery, including short values hidden
        # in literal fields and unregistered shapes introduced after rendering.
        ctx = RedactionContext().discover({'token': 'x'}).seal()
        with self.assertRaises(RedactionError):
            ctx.check_private_text({'type': 'x'})
        for raw in ('LONG-DIAGNOSTIC-CANARY-UNREGISTERED', '/tmp/synthetic-secret',
                    'a3cdbbbb-aaaa-bbbb-cccc-123456789abc', 'deadbeef' * 8):
            with self.assertRaisesRegex(RedactionError, 'secret-shaped'):
                ctx.check_export_text(json.dumps({'detail': raw}), format='json')

    def test_final_serialized_file_audit_prevents_all_writes(self):
        from capture_export import Artifact, json_text
        original_render = Artifact.render
        targets = ('summary', 'observation', 'verdict-inputs', 'capture', 'broker.log', 'adapter.log', 'records.jsonl')
        for target, survivor in product(targets, ('source 987 rejected', '987.', '987/')):
            with self.subTest(target=target, survivor=survivor), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                output = root / 'output'
                def corrupt_json(value):
                    matches = {'summary': 'evidence' in value, 'observation': 'sources' in value,
                               'verdict-inputs': 'observation' in value, 'capture': 'redaction_version' in value}
                    if matches.get(target):
                        value = dict(value, detail=survivor)
                    return json_text(value)
                def corrupt_artifact(artifact, context):
                    if artifact.name == target:
                        return json.dumps({'detail': survivor}) + '\n'
                    return original_render(artifact, context)
                with patch('capture_export.json_text', side_effect=corrupt_json), \
                     patch.object(Artifact, 'render', corrupt_artifact), self.assertRaises(RedactionError) as error:
                    collect(Cell('inbox', 'idle', 'resumed', 'text', 'single'), None, root, output,
                            dict(message_ids=[987], setup_errors=['staging incomplete']))
                self.assertNotIn('987', str(error.exception))
                self.assertFalse(any(path.is_file() for path in output.rglob('*')))


class ExportTests(unittest.TestCase):
    @staticmethod
    def peer(token='private-delivery-token-alpha', uuid='private-host-record-alpha', message=987):
        return {'type': 'user', 'isMeta': True, 'uuid': uuid, 'sessionId': 'private-host-session-alpha',
                'origin': {'kind': 'peer', 'from': 'c3', 'verifiedPeerPid': 12345},
                'message': {'role': 'user', 'content': 'Another Claude session sent a message:\n'
                    '<channel source="plugin:c3:c3" c3_attempt="inbox:3" c3_delivery_id="' + token +
                    '" message_id="' + str(message) + '" user="private-user-label-alpha">MATRIX_SAMPLE</channel>'}}

    def test_capture_wide_consistency_and_failure_cleanup(self):
        from driver import save_failure_evidence
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'broker').mkdir()
            (root / 'control').mkdir()
            output = root / 'output'
            a, b = self.peer(), self.peer('private-delivery-token-beta', 'private-host-record-beta', 988)
            rejected = self.peer('private-rejected-token-canary', 'private-rejected-record-canary', 989)
            rejected['isMeta'] = False
            records = [a, b, a, rejected, {'type': 'assistant', 'message': {'content': [
                {'type': 'tool_use', 'id': 'private-call-identity-canary', 'name': 'mcp__c3__fetch_queue'}]}}]
            transcript = root / 'host.jsonl'
            transcript.write_text(''.join(json.dumps(record) + '\n' for record in records))
            broker = ''.join('TEST ATTEMPT token=' + token + ' topic=42 transport=inbox phase=reserved members=1 retired=0 elapsed_ms=0\n'
                             for token in (a['message']['content'].split('c3_delivery_id="')[1].split('"')[0],
                                           b['message']['content'].split('c3_delivery_id="')[1].split('"')[0]))
            (root / 'broker/broker.log').write_text(broker)
            (root / 'control/adapter.log').write_text('token=private-delivery-token-alpha pid=12345\n')
            events = [{'event': 'channel_notify', 'pid': 12345, 'frame': {'token': 'private-delivery-token-alpha'}}]
            (root / 'control/events.jsonl').write_text(''.join(json.dumps(event) + '\n' for event in events))
            (root / 'control/sessions.jsonl').write_text(json.dumps({'session_id': 'private-host-session-alpha',
                'transcript_path': str(transcript)}) + '\n')
            (root / 'control/pane.txt').write_text('token=private-delivery-token-alpha ' + str(transcript))
            evidence = dict(message_ids=[987, 988], setup_errors=['staging token=private-delivery-token-alpha ' + str(transcript)],
                            run_id='private-run-identity-canary', session_id='private-broker-session-canary',
                            host_session_id='private-host-session-alpha', route_id='-123/42')
            host = Mock()
            host.records.return_value = records
            host.events.return_value = events
            host.transcript.return_value = transcript
            result = collect(Cell('inbox', 'idle', 'resumed', 'text', 'double'), host, root, output, evidence)
            saved = {path.relative_to(output): path.read_bytes() for path in output.rglob('*') if path.is_file()}
            save_failure_evidence(root, output)
            # Subsequent raw changes never get a second, independent redaction or
            # overwrite an artifact that was already sealed and exported.
            (root / 'control/adapter.log').write_text('unsanitized-later-secret-canary')
            (root / 'control/events.jsonl').write_text('unsanitized-later-secret-canary')
            save_failure_evidence(root, output)
            for name, content in saved.items():
                self.assertEqual((output / name).read_bytes(), content)
            self.assertEqual(result, json.loads((output / 'summary.json').read_text()))
            inputs = json.loads((output / 'replay/verdict-inputs.json').read_text())
            observation = json.loads((output / 'observation.json').read_text())
            self.assertEqual(observation, inputs['observation'])
            self.assertEqual(result['evidence']['run_id'], inputs['scenario']['run_id'])
            self.assertEqual(result['evidence']['host_session_id'], inputs['scenario']['host_session_id'])
            self.assertNotEqual(result['evidence']['session_id'], result['evidence']['host_session_id'])
            clean_records = [json.loads(line) for line in (output / 'replay/records.jsonl').read_text().splitlines()]
            self.assertEqual(len(clean_records), len(records))
            self.assertEqual(clean_records[0], clean_records[2])
            self.assertNotEqual(clean_records[0]['uuid'], clean_records[1]['uuid'])
            self.assertFalse(classify(clean_records[3])['accept'])
            ids = result['evidence']['message_ids']
            self.assertEqual(result['evidence']['received'], {str(ids[0]): 1, str(ids[1]): 1})
            for index in (0, 1):
                token = classify(clean_records[index])['token']
                self.assertEqual(token, result['evidence']['attempts'][index]['token'])
                self.assertIn('token=' + token + ' ', (output / 'replay/broker.log').read_text())
                self.assertIn('message_id="' + str(ids[index]) + '"', clean_records[index]['message']['content'])
                self.assertEqual(token, observation['events'][index]['token'])
            sessions = json.loads((output / 'sessions.jsonl').read_text())
            self.assertEqual(sessions['session_id'], clean_records[0]['sessionId'])
            self.assertEqual((output / 'transcript-0.jsonl').read_bytes(), (output / 'replay/control/transcript-0.jsonl').read_bytes())
            descriptor = json.loads((output / 'replay/capture.json').read_text())
            self.assertEqual(descriptor['redaction_version'], 'identity-v1')
            self.assertEqual(descriptor['context'], [])
            for entry in descriptor['artifacts']:
                path = output / 'replay' / entry['path']
                if path.exists():
                    self.assertEqual(entry['sha256'], hashlib.sha256(path.read_bytes()).hexdigest())
                    self.assertEqual(entry['bytes'], len(path.read_bytes()))
            # Original short numerics are tested in their actual identity slots.
            self.assertNotIn(987, ids)
            self.assertNotIn(988, ids)
            for path in output.rglob('*'):
                if path.is_file():
                    self.assertNotIn(b'private-', path.read_bytes(), str(path.relative_to(output)))
                    self.assertNotIn(b'unsanitized-later-secret', path.read_bytes())
                    self.assertNotIn(str(root).encode(), path.read_bytes())

    def test_malformed_failure_artifacts_keep_their_state(self):
        from driver import save_failure_evidence
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'control').mkdir()
            (root / 'control/events.jsonl').write_text('{"token":"private-partial-token-canary"}')
            (root / 'control/sessions.jsonl').write_text('{broken private-malformed-canary\n')
            (root / 'control/pane.txt').write_bytes(b'private-invalid-utf8-canary\xff')
            (root / 'control/adapter.log').write_text('token=private-partial-log-canary')
            output = root / 'output'
            collect(Cell('channel', 'idle', 'fresh', 'text', 'single'), None, root, output, {})
            save_failure_evidence(root, output)
            descriptor = json.loads((output / 'replay/capture.json').read_text())
            states = {entry['path']: entry['state'] for entry in descriptor['artifacts']}
            self.assertEqual(states['control/events.jsonl'], 'partial')
            self.assertEqual(states['control/sessions.jsonl'], 'malformed')
            self.assertEqual(states['control/pane.txt'], 'malformed')
            self.assertEqual(states['adapter.log'], 'partial')
            self.assertFalse((output / 'adapter.log').read_text().endswith('\n'))
            self.assertFalse((output / 'events.jsonl').read_text().endswith('\n'))
            with self.assertRaises(ValueError):
                json.loads((output / 'sessions.jsonl').read_text())
            for path in output.rglob('*'):
                if path.is_file():
                    self.assertNotIn(b'private-', path.read_bytes())

    def test_security_failure_stops_all_public_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'broker').mkdir()
            (root / 'control').mkdir()
            (root / 'broker/broker.log').write_text('token=ambiguous-private-canary record_id=ambiguous-private-canary\n')
            (root / 'control/pane.txt').write_text('ambiguous-private-canary')
            output = root / 'output'
            with self.assertRaises(RedactionError) as error:
                collect(Cell('channel', 'idle', 'fresh', 'text', 'single'), None, root, output, {})
            self.assertNotIn('ambiguous-private-canary', str(error.exception))
            self.assertFalse(any(path.is_file() for path in output.rglob('*')))

    def test_legacy_bytes_and_export_selection(self):
        # Frozen prior-renderer bytes, including original spacing and JSONL newline.
        samples = [
            ({'type': 'user', 'uuid': 'private', 'content': '<channel c3_delivery_id="secret-token" c3_attempt="channel:1" user="someone" message_id="987">sample</channel>'},
             '{"type": "user", "uuid": "ID", "content": "<channel c3_delivery_id=\\"TOKEN\\" c3_attempt=\\"channel:1\\" user=\\"USER\\" message_id=\\"ID\\">sample</channel>"}\n'),
            ({'type': 'user', 'uuid': 'private', 'origin': {'kind': 'peer', 'from': 'c3', 'verifiedPeerPid': 123}, 'cwd': '/home/example/private'},
             '{"type": "user", "uuid": "ID", "origin": {"kind": "peer", "from": "c3", "verifiedPeerPid": 1}, "cwd": "/work/ID"}\n'),
            ({'type': 'tool_result', 'tool_use_id': 'private-call', 'content': '[C3_FETCH_RECEIPT_V1]\ngroup secret-token\nmember private-row ' + 'a' * 64 + '\n[/C3_FETCH_RECEIPT_V1]'},
             '{"type": "tool_result", "tool_use_id": "ID", "content": "[C3_FETCH_RECEIPT_V1]\\ngroup TOKEN\\nmember ROW1 ' + 'a' * 64 + '\\n[/C3_FETCH_RECEIPT_V1]"}\n')]
        for raw, expected in samples:
            actual = json.dumps(sanitize_legacy_fixture(raw, {'secret-token'}, receipt_ids=['private-row']), ensure_ascii=False) + '\n'
            self.assertEqual(actual, expected)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'output'
            (output / 'replay').mkdir(parents=True)
            (root / 'cmd/c3-claude-adapter/testdata').mkdir(parents=True)
            records = ''.join(expected for _, expected in samples)
            sidecars = json.dumps([{'accept': True}] * len(samples), indent=2) + '\n'
            (output / 'records.jsonl').write_text(records)
            (output / 'records.expect.json').write_text(sidecars)
            (output / 'replay/records.jsonl').write_text('must never be selected')
            cell = Cell('channel', 'idle', 'fresh', 'text', 'single')
            export_fixtures(output, root, 'synthetic', cell)
            target = root / 'cmd/c3-claude-adapter/testdata/claude-synthetic'
            self.assertEqual((target / (cell.name + '.jsonl')).read_text(), records)
            self.assertEqual((target / (cell.name + '.expect.json')).read_text(), sidecars)
            self.assertEqual(len(records.splitlines()), len(json.loads(sidecars)))
            with self.assertRaises(FileExistsError):
                export_fixtures(output, root, 'synthetic', cell)

    def test_held_frames_share_call_token_row_and_revision_maps(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'output'
            frames, records = [], []
            for index, revision in enumerate(('a' * 64, 'b' * 64), 1):
                call, token, row = (f'private-{domain}-canary-{index}' for domain in ('call', 'token', 'row'))
                trailer = f'[C3_FETCH_RECEIPT_V1]\ngroup {token}\nmember {row} {revision}\n[/C3_FETCH_RECEIPT_V1]'
                frames.append({'jsonrpc': '2.0', 'id': call, 'result': {'content': [{'type': 'text', 'text': trailer}]}})
                records.extend([{'type': 'assistant', 'message': {'content': [{'type': 'tool_use', 'id': call, 'name': 'mcp__c3__fetch_queue'}]}},
                                {'type': 'user', 'uuid': f'private-record-canary-{index}', 'message': {'content': [
                                    {'type': 'tool_result', 'tool_use_id': call, 'content': trailer}]}}])
            host = Mock()
            host.records.return_value = records
            host.events.return_value = [{'event': 'fetch_result_waiting', 'frame': frame} for frame in frames]
            result = collect(Cell('fetch', 'idle', 'resumed', 'text', 'single'), host, root, output,
                             dict(setup_errors=['staging incomplete'], fetch_result=frames[-1]['result']))
            clean_frames = [json.loads((output / f'replay/held-fetch/response-{index}.json').read_text()) for index in (1, 2)]
            clean_records = [json.loads(line) for line in (output / 'replay/records.jsonl').read_text().splitlines()]
            for index, frame in enumerate(clean_frames):
                expected = fetch_trailer(frame['result']['content'][0]['text'])
                call = clean_records[2 * index]['message']['content'][0]['id']
                self.assertEqual(frame['id'], call)
                self.assertTrue(classify_fetch(clean_records[2 * index + 1], expected, {call})['accept'])
            first, second = [fetch_trailer(frame['result']['content'][0]['text']) for frame in clean_frames]
            self.assertNotEqual(first['token'], second['token'])
            self.assertNotEqual(first['members'][0]['record_id'], second['members'][0]['record_id'])
            self.assertNotEqual(first['members'][0]['revision'], second['members'][0]['revision'])
            self.assertEqual(result['evidence']['fetch_result'], clean_frames[-1]['result'])
            for path in (output / 'replay').rglob('*'):
                if path.is_file():
                    self.assertNotIn(b'private-', path.read_bytes())

    def test_legacy_fetch_collapse_refuses_contradictory_pair(self):
        trailer = '[C3_FETCH_RECEIPT_V1]\ngroup correct-token\nmember row-first ' + 'a' * 64 + '\n[/C3_FETCH_RECEIPT_V1]'
        for bad_call, bad_trailer in (('wrong-call', trailer), ('correct-call', trailer.replace('correct-token', 'wrong-token-canary'))):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                output = root / 'output'
                records = [{'type': 'assistant', 'message': {'content': [{'type': 'tool_use', 'name': 'mcp__c3__fetch_queue', 'id': 'correct-call'}]}},
                           {'type': 'user', 'message': {'content': [{'type': 'tool_result', 'tool_use_id': bad_call, 'content': 'MATRIX_SAMPLE\n' + bad_trailer}]}}]
                host = Mock()
                host.records.return_value = records
                host.events.return_value = []
                # A setup failure still retains the raw shapes without forcing a host read.
                collect(Cell('fetch', 'idle', 'resumed', 'text', 'single'), host, root, output,
                        dict(setup_errors=['staging incomplete'], fetch_result={'content': [{'type': 'text', 'text': trailer}]}))
                self.assertTrue((output / 'replay/verdict-inputs.json').is_file())
                for path in (output / 'replay').rglob('*'):
                    if path.is_file():
                        self.assertNotIn(b'wrong-token-canary', path.read_bytes())
                        self.assertNotIn(b'wrong-call', path.read_bytes())
                self.assertTrue((output / 'legacy-fixture-refused.txt').is_file())
                self.assertFalse((output / 'records.jsonl').exists())
                with self.assertRaisesRegex(ValueError, 'collapse changes a rejected fetch correlation'):
                    export_fixtures(output, root, 'synthetic', Cell('fetch', 'idle', 'resumed', 'text', 'single'))

    def test_legacy_pair_cannot_leak_new_private_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record = self.peer()
            record['source_uuid'] = 'private-opaque-host-record-canary'
            host = Mock()
            host.records.return_value = [record]
            host.events.return_value = []
            output = root / 'output'
            collect(Cell('inbox', 'idle', 'resumed', 'text', 'single'), host, root, output,
                    dict(setup_errors=['staging incomplete']))
            self.assertFalse((output / 'records.jsonl').exists())
            self.assertTrue((output / 'replay/records.jsonl').exists())
            self.assertIn('private identity unsupported', (output / 'legacy-fixture-refused.txt').read_text())
            for path in output.rglob('*'):
                if path.is_file():
                    self.assertNotIn(b'private-opaque-host-record-canary', path.read_bytes())


if __name__ == '__main__':
    unittest.main()
