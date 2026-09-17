"""Explicit test-only golden authoring. Never run automatically from tests.

Historical outputs come exclusively from the frozen oracle. Expected corrections
below are authored assertions, not outputs obtained from the new evaluator.
"""
from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path

from matrix import Cell
from test_verdict_legacy_oracle import verdict as old_verdict, old_report
from verdict_fixture_support import healthy_evidence, author_witness

ROOT = Path(__file__).parent / 'testdata/verdict-core'
EXTRA_LABELS = ['expected contract observed', 'state proven at injection', 'session posture proven',
                'delivery identities correlated', 'retired rows match authorized rows', 'retirement follows contract evidence',
                'fault expectation observed', 'source disposition preserved']
DISPOSITION = 'injected source lacks its required final disposition'
INCOMPLETE = 'evidence collection incomplete'
IDENTITY = 'delivery evidence identity mismatch'
UNAUTHORIZED = 'retired rows do not match authorized delivery members'
HOST_MISSING = 'host did not receive each source exactly once'
FETCH_MISSING = 'no successful fetch tool-result record'
FETCH_SOURCES = 'fetch did not return each injected source exactly once'
FETCH_TOKEN = 'fetch result has no broker receipt token'
FETCH_RETAINED = 'rows retired before host tool-result receipt (baseline consume-on-fetch)'

# Reviewed refinements under the orchestrator's ruling. These add assertions;
# they neither replace historical reasons nor obtain expectations from the core.
ABSENCE_REFINEMENTS = {
    'aggregate-only-incomplete': ['receipt missing or outside 15-second window'],
    'attempt-correct-total-wrong-identities': [INCOMPLETE],
    'attempt-repeated-source-correct-total': [HOST_MISSING, UNAUTHORIZED, DISPOSITION, INCOMPLETE],
    'channel-extra-confirmation': [INCOMPLETE],
    'channel-missing-attempt-token': ['receipt missing or outside 15-second window', HOST_MISSING, UNAUTHORIZED],
    'channel-missing-confirmation': [UNAUTHORIZED, INCOMPLETE],
    'channel-retired-0': [UNAUTHORIZED, INCOMPLETE],
    'channel-retired-2': [DISPOSITION, INCOMPLETE],
    'channel-unequal-token-sets': [UNAUTHORIZED, DISPOSITION, INCOMPLETE],
    'double-only-one-admitted': [INCOMPLETE],
    'fetch-extra-confirmation': [INCOMPLETE],
    'fetch-held-0': [UNAUTHORIZED, INCOMPLETE],
    'fetch-held-1': [UNAUTHORIZED, INCOMPLETE],
    'fetch-held-3': [UNAUTHORIZED, INCOMPLETE],
    'fetch-held-correct-count-stale-revision': [IDENTITY, UNAUTHORIZED, INCOMPLETE],
    'fetch-missing-attempt-token': [FETCH_MISSING, 'fetch group confirmation missing or outside 60-second window', FETCH_TOKEN, FETCH_SOURCES, UNAUTHORIZED],
    'fetch-missing-confirmation': [UNAUTHORIZED, INCOMPLETE],
    'fetch-no-reservations-or-confirmations': [FETCH_RETAINED, FETCH_MISSING, FETCH_TOKEN, FETCH_SOURCES, UNAUTHORIZED, INCOMPLETE],
    'fetch-result-wrong-call': [INCOMPLETE],
    'fetch-result-wrong-session': [INCOMPLETE],
    'fetch-retired-0': [UNAUTHORIZED, INCOMPLETE],
    'fetch-retired-2': [DISPOSITION, INCOMPLETE],
    'fetch-trailer-duplicate-row': [INCOMPLETE],
    'fetch-trailer-extra-member': [INCOMPLETE],
    'fetch-trailer-missing-member': [INCOMPLETE],
    'fetch-trailer-stale-revision': [INCOMPLETE],
    'fetch-trailer-wrong-group': [INCOMPLETE],
    'fetch-unequal-token-sets': [UNAUTHORIZED, DISPOSITION, INCOMPLETE],
    'host-extra-source-key': [INCOMPLETE],
    'live-reserved-2': [UNAUTHORIZED, INCOMPLETE],
    'no-reservations-or-confirmations': ['receipt missing or outside 15-second window', HOST_MISSING, UNAUTHORIZED, INCOMPLETE],
    'ordering-multiple-failures': ['receipt missing or outside 15-second window', UNAUTHORIZED, INCOMPLETE],
}
for field in ('attempt_id', 'claim_generation', 'group_id', 'host_session_id', 'revision', 'route_id',
              'row', 'run_id', 'session_id', 'token'):
    ABSENCE_REFINEMENTS['identity-' + field] = [INCOMPLETE]


def refine_failure(cell, expected, additions):
    live = ['no negotiated attempt observed', 'fallback/wrong transport attempted', 'source attempted more or less than once',
            'receipt missing or outside 15-second window', 'retirement count differs from injected source count', HOST_MISSING]
    fetch = ['live offer in fetch-only cell', FETCH_RETAINED, FETCH_MISSING,
             'fetch group confirmation missing or outside 60-second window', 'fetch group retirement count differs from injected sources',
             'fetch tool result lacks the complete matching receipt trailer', FETCH_TOKEN, FETCH_SOURCES]
    ordered = (fetch if cell.transport == 'fetch' else live) + [
        'observed delivery contract does not match expected contract', 'requested host state was not proven at injection',
        'requested session posture was not proven at injection', IDENTITY, UNAUTHORIZED, 'rows retired before contract evidence',
        'observed fault does not match scenario health class', DISPOSITION, 'automatic attachment recovery was not proven', INCOMPLETE]
    reasons = expected['reasons'] + additions
    return dict(status='FAIL', reasons=[reason for reason in reasons if reason not in ordered]
                + [reason for reason in ordered if reason in reasons], not_evaluated=[])


def save(identity, cell, evidence, *, extras=(), witness=None, collect_only=False, classification=None, defect_id=None, raw=None, replacement=None):
    witness = witness or author_witness(cell, evidence)
    report = old_report(cell, evidence, collect_only)
    expected = deepcopy(report)
    if evidence.get('setup_errors'):
        # Preserve the frozen setup prefix even when no readiness gate was tested.
        expected['not_evaluated'] += EXTRA_LABELS
    if extras:
        expected['reasons'] += list(extras)
    if replacement is not None:
        expected = replacement
    if INCOMPLETE in expected['reasons']:
        expected['status'] = 'FAIL'
    elif expected['reasons'] and expected['status'] == 'PASS':
        expected['status'] = 'FAIL'
    fixture = dict(id=identity, baseline_source='test_verdict_legacy_oracle.py; original collect.py SHA-256 in oracle header',
                   cell=asdict(cell), collect_only=collect_only, legacy_evidence=evidence, synthetic_witness=witness,
                   old_verdict_reasons=old_verdict(cell, evidence), old_report=report,
                   classification=classification or ('intentional-correction' if extras or replacement else 'characterization'),
                   defect_id=defect_id, expected_core=expected)
    if identity in ABSENCE_REFINEMENTS:
        if fixture['classification'] == 'characterization':
            assert report['status'] == expected['status'] == 'FAIL', 'a historical status change requires a separate ruling'
        fixture['classification'] = 'intentional-correction'
        fixture['defect_id'] = defect_id or 'accept-on-absence'
        fixture['correction_note'] = 'Positive binding and full queue reconciliation refine an already failing trace; the frozen oracle report is unchanged.'
        fixture['expected_core'] = refine_failure(cell, expected, ABSENCE_REFINEMENTS[identity])
    if raw is not None:
        fixture['raw_synthetic_records'] = raw
    ROOT.mkdir(parents=True, exist_ok=True)
    (ROOT / (identity + '.json')).write_text(json.dumps(fixture, indent=2) + '\n')


def generate():
    for transport in ('channel', 'inbox', 'fetch'):
        for collect_only in (False, True):
            cell = Cell(transport, 'idle', 'resumed', 'text', 'single')
            evidence = dict(healthy_evidence(cell), setup_errors=['setup failed'])
            save(f'setup-idle-{transport}-{collect_only}', cell, evidence, collect_only=collect_only)
    for transport in ('channel', 'inbox', 'fetch'):
        for burst in ('single', 'double'):
            for kind in ('text', 'voice', 'photo'):
                cell = Cell(transport, 'idle', 'resumed', kind, burst)
                save(f'healthy-{transport}-{burst}-{kind}', cell, healthy_evidence(cell))
    live = Cell('channel', 'reconnect', 'resumed', 'text', 'single')
    fetch = Cell('fetch', 'reconnect', 'resumed', 'text', 'single')
    double = Cell('channel', 'reconnect', 'resumed', 'text', 'double')
    fetch_double = Cell('fetch', 'reconnect', 'resumed', 'text', 'double')
    for transport in ('channel', 'inbox'):
        cell = Cell(transport, 'reconnect', 'resumed', 'text', 'double')
        evidence = healthy_evidence(cell)
        evidence['attempts'] = [dict(phase='reserved', token=token, transport=transport, members='1') for token in ('one', 'two')] + [
            dict(phase='confirmed', token=token, transport=transport, retired='1', elapsed_ms='100') for token in ('one', 'two')]
        witness = author_witness(cell, evidence)
        row_two = deepcopy(next(event for event in witness['observation']['events'] if event['id'] == 'persisted')['members'][1])
        for event in witness['observation']['events']:
            if event['token'] == 'two':
                event['members'] = [deepcopy(row_two)]
                if event['milestone'] == 'attempt_terminal':
                    event['payload']['retired_members'] = [deepcopy(row_two)]
            if event['id'] == 'host-2-0':
                event.update(token='two', attempt_id='attempt-two', group_id='group-two')
        save(f'healthy-{transport}-separate-attempts', cell, evidence, witness=witness)
    evidence = healthy_evidence(double)
    witness = author_witness(double, evidence)
    hosts = [event for event in witness['observation']['events'] if event['milestone'] == 'transcript_recorded']
    hosts[0]['members'] += hosts[1]['members']
    witness['observation']['events'].remove(hosts[1])
    save('healthy-merged-source-envelope', double, evidence, witness=witness)
    for key, value, name, extra in (
        ('injected', False, 'injection-false', ()), ('injected', None, 'injection-absent', ()),
        ('rows_final', 1, 'final-positive', ()), ('rows_final', None, 'final-unavailable', (INCOMPLETE,)),
        ('received', {'1': 0}, 'host-zero', (DISPOSITION,)), ('received', {'1': 2}, 'host-two', ()),
        ('received', {}, 'host-missing-source', (DISPOSITION,)),
        ('attempts', [], 'no-reservations-or-confirmations', (DISPOSITION,)),
        ('route_line_count', 1, 'route-one', ()), ('route_line_count', 3, 'route-multiple', ()),
        ('no_false_held', False, 'false-held', ()),
    ):
        evidence = healthy_evidence(live)
        if value is None and key == 'injected':
            del evidence[key]
        else:
            evidence[key] = value
        save(name, live, evidence, extras=extra)
    for name in ('broker reserved a delivery attempt', 'broker delivered to the channel', 'gated proxy forwarded a channel notification'):
        evidence = healthy_evidence(live)
        evidence.update(attempt_before_ready=True, attempt_before_ready_observations=[name])
        save('early-' + name.replace(' ', '-'), live, evidence)
    for names, name in (([], 'early-fallback'), (['broker delivered to the channel', 'gated proxy forwarded a channel notification'], 'early-ordered')):
        evidence = healthy_evidence(live)
        evidence.update(attempt_before_ready=True, attempt_before_ready_observations=names)
        save(name, live, evidence)
    for name, epoch, seq in (('predecessor-offer-excluded', 'predecessor', 15), ('after-ready-excluded', 'epoch', 21)):
        evidence = healthy_evidence(live)
        witness = author_witness(live, dict(evidence, attempt_before_ready=True, attempt_before_ready_observations=['broker delivered to the channel']))
        offer = next(event for event in witness['observation']['events'] if event['milestone'] == 'delivery_offered')
        offer['scope']['connection_epoch_id'] = epoch
        offer['position'].update(seq=seq, time_ms=seq * 1000)
        save(name, live, evidence, witness=witness)
    for transport, wrong in (('channel', 'inbox'), ('inbox', 'channel'), ('fetch', 'inbox')):
        cell = Cell(transport, 'reconnect', 'resumed', 'text', 'single')
        evidence = healthy_evidence(cell)
        evidence['attempts'].append(dict(phase='reserved', token='wrong', transport=wrong, members='1'))
        save('wrong-transport-' + transport, cell, evidence)
    for cell in (live, fetch):
        prefix = cell.transport
        limit = 60000 if prefix == 'fetch' else 15000
        for elapsed in (limit - 1, limit, limit + 1, None):
            evidence = healthy_evidence(cell)
            if elapsed is None:
                del evidence['attempts'][1]['elapsed_ms']
            else:
                evidence['attempts'][1]['elapsed_ms'] = str(elapsed)
            save(f'{prefix}-elapsed-{elapsed}', cell, evidence)
        for amount in (0, 2):
            evidence = healthy_evidence(cell)
            evidence['attempts'][1]['retired'] = str(amount)
            save(f'{prefix}-retired-{amount}', cell, evidence,
                 extras=(DISPOSITION,) if amount == 0 else (IDENTITY, UNAUTHORIZED))
        for mode in ('missing-confirmation', 'extra-confirmation', 'unequal-token-sets'):
            evidence = healthy_evidence(cell)
            if mode == 'missing-confirmation':
                evidence['attempts'].pop()
            elif mode == 'extra-confirmation':
                evidence['attempts'].append(dict(phase='confirmed', token='two', transport=prefix, retired='0', elapsed_ms='100'))
            else:
                evidence['attempts'][1]['token'] = 'two'
            save(prefix + '-' + mode, cell, evidence, extras=(DISPOSITION,) if mode == 'missing-confirmation' else ())
    for amount in (0, 2):
        evidence = healthy_evidence(live)
        evidence['attempts'][0]['members'] = str(amount)
        save(f'live-reserved-{amount}', live, evidence, extras=() if amount else (IDENTITY, UNAUTHORIZED, DISPOSITION, INCOMPLETE),
             replacement=dict(status='FAIL', reasons=old_verdict(live, evidence) + ['host did not receive each source exactly once', IDENTITY, UNAUTHORIZED, DISPOSITION, INCOMPLETE], not_evaluated=[]) if amount == 0 else None)
    for amount in (0, 1, 3, None):
        evidence = healthy_evidence(fetch_double)
        evidence['rows_while_fetch_result_held'] = amount
        save(f'fetch-held-{amount}', fetch_double, evidence, extras=(INCOMPLETE,) if amount is None else ())
    for field, value, name, extras in (
        ('fetch_tool_result', False, 'fetch-result-absent', (DISPOSITION,)),
        ('fetch_trailer_complete', False, 'fetch-trailer-absent', ()),
        ('fetch_token', False, 'fetch-token-absent', ()),
        ('fetch_source_occurrences', 1, 'fetch-sources-too-few', (DISPOSITION,)),
        ('fetch_source_occurrences', 3, 'fetch-sources-too-many', ()),
    ):
        evidence = healthy_evidence(fetch_double)
        evidence[field] = value
        if field == 'fetch_tool_result':
            evidence['fetch_source_occurrences'] = 0
            evidence['fetch_trailer_complete'] = False
        if field == 'fetch_token':
            evidence['fetch_trailer_complete'] = False
        save(name, fetch_double, evidence, extras=extras)
    for transport in ('channel', 'inbox', 'fetch'):
        cell = Cell(transport, 'reconnect', 'resumed', 'text', 'single')
        for collect_only in (False, True):
            for errors in (['setup failed'], ['setup failed', 'ignored second error']):
                evidence = healthy_evidence(cell)
                evidence.update(setup_errors=errors, run_errors=['ignored run error'], injected=False, rows_final=2)
                save(f'setup-{transport}-{collect_only}-{len(errors)}', cell, evidence, collect_only=collect_only)
            evidence = healthy_evidence(cell)
            evidence.update(run_errors=['run failure', 'run failure'], rows_final=1)
            save(f'run-{transport}-{collect_only}', cell, evidence, collect_only=collect_only)
    for failing in (False, True):
        evidence = healthy_evidence(live)
        if failing:
            evidence.update(injected=False, rows_final=1, false_held=True, route_line_count=2)
        save('collect-only-' + str(failing), live, evidence, collect_only=True)
    evidence = healthy_evidence(live)
    evidence.update(injected=False, rows_final=1, false_held=True, route_line_count=2, attempts=[], received={'1': 0})
    save('ordering-multiple-failures', live, evidence, extras=(DISPOSITION,))
    # Baselines characterize what the old extractor claimed, not the raw truth.
    for defect, cell, raw in (
        ('shared-sample-fetch-oracle', fetch_double, [dict(type='user', message=dict(content=[dict(type='tool_result', tool_use_id='call', content='source 1 MATRIX_SAMPLE MATRIX_SAMPLE\n[C3_FETCH_RECEIPT_V1]\ngroup one\nmember row-1 ' + 'a' * 64 + '\nmember row-2 ' + 'b' * 64 + '\n[/C3_FETCH_RECEIPT_V1]')]))]),
        ('unaccepted-live-intake-counted', live, [dict(type='user', uuid='record', message=dict(role='user', content='<channel c3_delivery_id="one" c3_attempt="inbox:1" message_id="1">sample</channel>'))]),
        ('explicit-attach-masks-resume', live, [dict(event='explicit_attach', session_id='session')]),
    ):
        evidence = healthy_evidence(cell)
        witness = author_witness(cell, evidence)
        witness['observation']['provenance']['known_defect_baselines'] = [defect]
        if defect == 'explicit-attach-masks-resume':
            event = deepcopy(witness['observation']['events'][0])
            event.update(id='explicit-attach', milestone='explicit_attach', payload={})
            event['position'].update(seq=2, time_ms=2000)
            witness['observation']['events'].append(event)
        save('known-defect-' + defect, cell, evidence, witness=witness, classification='known-defect baseline', defect_id=defect, raw=raw)
    evidence = healthy_evidence(live)
    evidence['received']['extra'] = 1
    save('host-extra-source-key', live, evidence, replacement=dict(status='FAIL', reasons=[IDENTITY], not_evaluated=[]))
    for cell in (live, fetch):
        evidence = healthy_evidence(cell)
        for attempt in evidence['attempts']:
            attempt['token'] = None
        extras = (['fetch tool result lacks the complete matching receipt trailer'] if cell.transport == 'fetch' else [])
        save(cell.transport + '-missing-attempt-token', cell, evidence, extras=extras + [DISPOSITION, INCOMPLETE])
    # Aggregate-only production call is explicitly incomplete, never synthesized.
    evidence = healthy_evidence(live)
    from collect import build_verdict_inputs
    scenario, contract, observation = build_verdict_inputs(live, evidence)
    replacement = dict(status='FAIL', reasons=[
        'injection was not completed', 'durable rows remain', 'no false Held assertion failed or missing',
        'route line count assertion failed or missing', 'no negotiated attempt observed', 'source attempted more or less than once',
        'retirement count differs from injected source count', 'host did not receive each source exactly once',
        'observed delivery contract does not match expected contract', 'requested host state was not proven at injection',
        'requested session posture was not proven at injection', DISPOSITION, INCOMPLETE], not_evaluated=[])
    save('aggregate-only-incomplete', live, evidence, witness=dict(scenario=scenario, contract=contract, observation=observation), replacement=replacement)


def generate_additional():
    live = Cell('channel', 'reconnect', 'resumed', 'text', 'single')
    fetch = Cell('fetch', 'reconnect', 'resumed', 'text', 'double')
    host_failure = 'host did not receive each source exactly once'
    fetch_failure = ['no successful fetch tool-result record', 'fetch tool result lacks the complete matching receipt trailer',
                     'fetch did not return each injected source exactly once']
    def canonical(name, cell, mutate, reasons, *, defect=None):
        evidence = healthy_evidence(cell)
        witness = author_witness(cell, evidence)
        mutate(witness)
        save(name, cell, evidence, witness=witness, classification='intentional-correction', defect_id=defect,
             replacement=dict(status='FAIL' if reasons else 'PASS', reasons=reasons, not_evaluated=[]))
    def host(witness):
        return next(event for event in witness['observation']['events'] if event['milestone'] in ('transcript_recorded', 'fetch_result_recorded'))
    for field in ('run_id', 'route_id', 'host_session_id', 'session_id', 'connection_epoch_id', 'claim_generation'):
        canonical('identity-' + field, live, lambda witness, field=field: host(witness)['scope'].update({field: 2 if field == 'claim_generation' else 'other'}),
                  [host_failure, 'observed delivery contract does not match expected contract', IDENTITY, DISPOSITION, INCOMPLETE] if field == 'connection_epoch_id' else [host_failure, IDENTITY, DISPOSITION])
    for field in ('token', 'attempt_id', 'group_id'):
        canonical('identity-' + field, live, lambda witness, field=field: host(witness).update({field: 'other'}), [host_failure, IDENTITY, DISPOSITION])
    for field in ('row', 'revision'):
        def change(witness, field=field):
            member = host(witness)['members'][0]
            if field == 'row':
                member['row_id'] = 'other'
            else:
                member['revision']['value'] = 'stale'
        canonical('identity-' + field, live, change, [host_failure, IDENTITY, DISPOSITION])
    for mutation in ('wrong-call', 'wrong-session', 'unsuccessful', 'broker-only'):
        def change(witness, mutation=mutation):
            hosts = [event for event in witness['observation']['events'] if event['milestone'] == 'fetch_result_recorded']
            for event in hosts:
                if mutation == 'wrong-call':
                    event['operation_id'] = 'other'
                elif mutation == 'wrong-session':
                    event['scope']['session_id'] = 'other'
                elif mutation == 'unsuccessful':
                    event['payload']['success'] = False
                else:
                    witness['observation']['events'].remove(event)
        canonical('fetch-result-' + mutation, fetch, change, fetch_failure + ([IDENTITY] if mutation.startswith('wrong') else []) + [DISPOSITION])
    for mutation in ('malformed', 'wrong-group', 'missing-member', 'extra-member', 'duplicate-row', 'stale-revision', 'trailing-newline', 'trailing-text'):
        def change(witness, mutation=mutation):
            for event in witness['observation']['events']:
                if event['milestone'] != 'fetch_result_recorded':
                    continue
                trailer = event['payload']['trailer']
                if mutation in ('malformed', 'trailing-newline', 'trailing-text'):
                    trailer.update(state='malformed', token=None, members=[])
                elif mutation == 'wrong-group':
                    trailer['token'] = 'other'
                elif mutation == 'missing-member':
                    trailer['members'].pop()
                elif mutation == 'extra-member':
                    member = deepcopy(trailer['members'][0]); member['row_id'] = 'extra'; trailer['members'].append(member)
                elif mutation == 'duplicate-row':
                    trailer['members'].append(deepcopy(trailer['members'][0]))
                else:
                    trailer['members'][0]['revision']['value'] = 'stale'
        canonical('fetch-trailer-' + mutation, fetch, change, ['fetch tool result lacks the complete matching receipt trailer'] +
                  ([] if mutation in ('malformed', 'trailing-newline', 'trailing-text') else [IDENTITY]))
    def wrong_retirement(witness):
        terminal = next(event for event in witness['observation']['events'] if event['milestone'] == 'attempt_terminal')
        terminal['payload']['retired_members'][0]['row_id'] = 'unrelated'
    for cell in (live, fetch):
        canonical(cell.transport + '-wrong-retired-row', cell, wrong_retirement, [UNAUTHORIZED, DISPOSITION, INCOMPLETE])
    def wrong_retired_revision(witness):
        terminal = next(event for event in witness['observation']['events'] if event['milestone'] == 'attempt_terminal')
        terminal['payload']['retired_members'][0]['revision']['value'] = 'stale'
    canonical('fetch-wrong-retired-revision', fetch, wrong_retired_revision, [UNAUTHORIZED, DISPOSITION, INCOMPLETE])
    def duplicate_source(witness):
        hosts = [event for event in witness['observation']['events'] if event['milestone'] == 'fetch_result_recorded']
        hosts[1]['members'] = deepcopy(hosts[0]['members'])
    canonical('fetch-duplicate-source-correct-total', fetch, duplicate_source,
              ['fetch did not return each injected source exactly once', DISPOSITION], defect='shared-sample-fetch-oracle')
    def unaccepted(witness):
        witness['observation']['events'].remove(host(witness))
    canonical('identity-correct-unaccepted-intake', live, unaccepted, [host_failure, DISPOSITION], defect='unaccepted-live-intake-counted')
    def attach(witness):
        scenario, observation = witness['scenario'], witness['observation']
        scenario['resume_requirement'] = 'attachment_recovery'
        template = deepcopy(observation['events'][0])
        template.update(id='explicit-attach', milestone='explicit_attach', payload={})
        template['position'].update(seq=2, time_ms=2000)
        observation['events'].append(template)
    canonical('automatic-recovery-explicit-attach', live, attach, ['automatic attachment recovery was not proven'], defect='explicit-attach-masks-resume')
    def admitted_one(witness):
        witness['observation']['sources'].pop()
    double = Cell('channel', 'reconnect', 'resumed', 'text', 'double')
    canonical('double-only-one-admitted', double, admitted_one,
              ['injection was not completed', 'source attempted more or less than once', host_failure, DISPOSITION])
    def repeated_attempt(witness):
        reservation = next(event for event in witness['observation']['events'] if event['milestone'] == 'attempt_reserved')
        reservation['members'][1]['source_ids'] = ['1']
    canonical('attempt-repeated-source-correct-total', double, repeated_attempt, ['source attempted more or less than once', IDENTITY])
    def wrong_attempt_rows(witness):
        reservation = next(event for event in witness['observation']['events'] if event['milestone'] == 'attempt_reserved')
        reservation['members'][1]['row_id'] = 'other'
    canonical('attempt-correct-total-wrong-identities', double, wrong_attempt_rows, [host_failure, IDENTITY, UNAUTHORIZED, DISPOSITION])
    for kind in ('same-record', 'distinct-record'):
        def duplicate(witness, kind=kind):
            event = deepcopy(host(witness))
            if kind == 'distinct-record':
                event['id'] += '-duplicate'; event['delivery_id'] += '-duplicate'; event['payload']['host_record_id'] += '-duplicate'
            witness['observation']['events'].append(event)
        canonical('host-duplicate-' + kind, live, duplicate, [] if kind == 'same-record' else [host_failure])
    for field in ('no_false_held', 'route_line_count'):
        evidence = healthy_evidence(live); del evidence[field]
        extra = [INCOMPLETE]
        report = old_report(live, evidence)
        reasons = report['reasons'] + extra
        save('missing-' + field, live, evidence, replacement=dict(status='FAIL', reasons=reasons, not_evaluated=[]))
    def incomplete(witness):
        witness['observation']['collection_complete'] = False
    evidence = healthy_evidence(live)
    witness = author_witness(live, evidence); incomplete(witness)
    save('collect-only-incomplete', live, evidence, witness=witness, collect_only=True, extras=[INCOMPLETE])


def generate_notice_and_policy_fixtures():
    cell = Cell('channel', 'reconnect', 'resumed', 'text', 'single')
    evidence = healthy_evidence(cell)
    for name, moment, count, backlog in (('held-visible-backlog', 81, 1, True), ('held-during-reservation', 35, 1, False),
                                        ('held-after-receipt', 65, 1, False)):
        witness = author_witness(cell, evidence)
        observation = witness['observation']
        member = deepcopy(next(event for event in observation['events'] if event['milestone'] == 'row_persisted')['members'][0])
        if backlog:
            member.update(row_id='backlog', source_ids=['backlog-source'])
            # Complete the synthetic witness: this unrelated row survives the run.
            # Injected-row projections and the frozen legacy judgment are unchanged.
            for item in observation['queue_snapshots']:
                item['rows'].append(deepcopy(member))
        barrier = deepcopy(observation['barriers'][0]); barrier.update(id='notice/held', name='notice/held')
        barrier['stream_cutoffs'] = {key: moment for key in barrier['stream_cutoffs']}
        observation['barriers'].append(barrier)
        snapshot = deepcopy(observation['queue_snapshots'][0]); snapshot.update(id='held-queue', barrier_id='notice/held', rows=[member])
        observation['queue_snapshots'].append(snapshot)
        notice = deepcopy(observation['events'][0]); notice.update(id='held', milestone='notice_emitted',
            payload=dict(kind='held', held_count=count, route_diagnostic_lines=[], queue_snapshot_id='held-queue'))
        notice['position'].update(stream_id='broker', seq=moment, time_ms=moment * 1000)
        notice['artifact_ref']['artifact_id'] = 'broker'
        observation['events'].append(notice)
        legacy = dict(evidence, no_false_held=backlog, false_held=not backlog)
        save(name, cell, legacy, witness=witness)
    # Reuse test-only canonical policy constructors, never their evaluator output.
    from test_verdict_core import PolicyTests
    policies = PolicyTests()
    for health in ('expiry', 'restart', 'degraded', 'delivery_failure'):
        scenario, contract, observation = policies.fault_trace(health)
        save('health-' + health, cell, evidence, witness=dict(scenario=scenario, contract=contract, observation=observation),
             classification='intentional-correction', replacement=dict(status='PASS', reasons=[], not_evaluated=[]))
    for milestone in ('queue_accepted', 'input_landed'):
        scenario, contract, observation = policies.legacy(milestone)
        save('legacy-' + milestone, cell, evidence, witness=dict(scenario=scenario, contract=contract, observation=observation),
             classification='intentional-correction', replacement=dict(status='PASS', reasons=[], not_evaluated=[]))
    for held_count in (0, 1):
        scenario, contract, observation = policies.fault_trace('delivery_failure')
        snapshot = deepcopy(next(item for item in observation['queue_snapshots'] if item['barrier_id'] == 'final'))
        snapshot.update(id='held-queue', barrier_id='notice/held')
        observation['queue_snapshots'].append(snapshot)
        barrier = deepcopy(observation['barriers'][0]); barrier.update(id='notice/held', name='notice/held')
        barrier['stream_cutoffs'] = {key: 81 for key in barrier['stream_cutoffs']}; observation['barriers'].append(barrier)
        notice = deepcopy(observation['events'][0]); notice.update(id='held', milestone='notice_emitted',
            payload=dict(kind='held', held_count=held_count, route_diagnostic_lines=[], queue_snapshot_id='held-queue'))
        notice['position'].update(stream_id='broker', seq=81, time_ms=81000)
        notice['artifact_ref']['artifact_id'] = 'broker'
        observation['events'].append(notice)
        reasons = ['no false Held assertion failed or missing'] if held_count else []
        save('held-receipt-retirement-failure-' + str(held_count), cell, evidence,
             witness=dict(scenario=scenario, contract=contract, observation=observation), classification='intentional-correction',
             replacement=dict(status='FAIL' if reasons else 'PASS', reasons=reasons, not_evaluated=[]))
    scenario, contract, observation = policies.fault_trace('expiry')
    scenario['final_dispositions'] = ['recoverable']
    observation['events'] = [event for event in observation['events'] if event['milestone'] not in ('receipt_accepted', 'transcript_recorded')]
    terminal = next(event for event in observation['events'] if event['milestone'] == 'attempt_terminal')
    terminal['payload'].update(outcome='expired', retired_members=[])
    terminal['position'].update(seq=40, time_ms=40000)
    member = deepcopy(terminal['members'][0])
    next(item for item in observation['queue_snapshots'] if item['barrier_id'] == 'final')['rows'] = [member]
    barrier = deepcopy(observation['barriers'][0]); barrier.update(id='notice/held', name='notice/held')
    barrier['stream_cutoffs'] = {key: 81 for key in barrier['stream_cutoffs']}; observation['barriers'].append(barrier)
    snapshot = deepcopy(observation['queue_snapshots'][0]); snapshot.update(id='held-queue', barrier_id='notice/held', rows=[member])
    observation['queue_snapshots'].append(snapshot)
    notice = deepcopy(observation['events'][0]); notice.update(id='held', milestone='notice_emitted',
        payload=dict(kind='held', held_count=1, route_diagnostic_lines=[], queue_snapshot_id='held-queue'))
    notice['position'].update(stream_id='broker', seq=81, time_ms=81000)
    notice['artifact_ref']['artifact_id'] = 'broker'
    observation['events'].append(notice)
    save('held-expired-attempt-exposes-row', cell, evidence, witness=dict(scenario=scenario, contract=contract, observation=observation),
         classification='intentional-correction', replacement=dict(status='PASS', reasons=[], not_evaluated=[]))
    fetch = Cell('fetch', 'reconnect', 'resumed', 'text', 'single')
    missing = healthy_evidence(fetch); missing['attempts'] = []
    save('fetch-no-reservations-or-confirmations', fetch, missing,
         extras=['fetch tool result lacks the complete matching receipt trailer', DISPOSITION])
    witness = author_witness(fetch, healthy_evidence(fetch))
    held = next(snapshot for snapshot in witness['observation']['queue_snapshots'] if snapshot['barrier_id'] == 'fetch_result_held')
    held['rows'][0]['revision']['value'] = 'stale'
    save('fetch-held-correct-count-stale-revision', fetch, healthy_evidence(fetch), witness=witness, classification='intentional-correction',
         replacement=dict(status='FAIL', reasons=['rows retired before host tool-result receipt (baseline consume-on-fetch)'], not_evaluated=[]))


if __name__ == '__main__':
    generate()
    generate_additional()
    generate_notice_and_policy_fixtures()
