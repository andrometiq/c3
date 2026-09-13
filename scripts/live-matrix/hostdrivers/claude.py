"""Claude profile data and the version 1 wrapper around the retained mechanics."""
from copy import deepcopy
from dataclasses import asdict, replace
from functools import wraps
from itertools import product
from pathlib import Path
import hashlib
import math
import subprocess
import time
import uuid

from hostdriver import (ArtifactBundle, ArtifactRef, BarrierPoint, ControlEvidence, Deadline, DriverError,
                        ObserverTables, ReconnectEvidence, Release, ResumeHandle, Scope, Session,
                        StateEvidence, SubmissionProof, resolve_contract)
from hostdrivers.claude_host import Host, HostSetupError, read_jsonl, wait_for
from matrix import Cell, DIMENSIONS

BARRIERS = {
    'readiness': {'ready': 'mcp-initialized'},
    'fetch_result': {'fetch': 'mcp-fetch-response'},
    'checkpoint': {'attachment': 'adapter-attached', 'session': 'session-at-arrival',
                   'injection': 'injection-state', 'before_ready': 'offers-before-ready', 'final': 'observation-end'},
    'workload_finished': {'workload': 'requested-workload-finished'},
}
VERIFIED_VERSIONS = ('2.1.266', '2.1.267', '2.1.268')


def describe(version, mode):
    if type(version) is not str or not version or type(mode) is not str or not mode:
        raise DriverError('describe', 'profile', 'invalid_request', 'host version and mode must be nonempty identifiers')
    is_known = version in VERIFIED_VERSIONS and mode == 'matrix'
    dimensions = (('channel', 'inbox', 'fetch'), ('idle', 'foreground', 'background', 'startup', 'reconnect'),
                  ('fresh', 'resumed'), ('text', 'voice', 'photo'), ('single', 'double'))
    contracts = {}
    cases = []
    for values in product(*dimensions):
        cell = Cell(*values)
        is_gated = cell.state in ('startup', 'reconnect')
        contract_id = f'matrix-{cell.transport}-' + ('ready' if is_gated else 'ungated')
        if contract_id not in contracts:
            contracts[contract_id] = dict(schema_version=1, id=contract_id, capability_id='matrix-v1',
                axes=dict(negotiation='v1', live_eligibility=dict(channel=cell.transport == 'channel', inbox=cell.transport == 'inbox'),
                          receipt_type='transcript', fetch_policy='receipt',
                          accepted_modes=['fetch_receipt'] if cell.transport == 'fetch' else [cell.transport, 'fetch_receipt']),
                milestones=dict(live='transcript_recorded', fetch='fetch_result_recorded'),
                timing=dict(live=dict(limit_ms=15000, basis='terminal_confirmation'),
                            fetch=dict(limit_ms=60000, basis='terminal_confirmation')),
                readiness_boundary='ready' if is_gated else None, contract_barrier_names=['injection', 'final'])
        is_infeasible = cell.session == 'fresh' and cell.state in ('foreground', 'background', 'reconnect')
        feasibility = 'infeasible' if is_infeasible else 'feasible' if is_known else 'unknown'
        reason = ('a tool call or reconnect requires an existing transcript' if is_infeasible else
                  '' if is_known else 'host version or mode has no verified control recipe')
        cases.append(dict(zip(DIMENSIONS, values), id=cell.name, feasibility=feasibility, reason=reason,
                          freshness='transcript_free_at_injection' if cell.session == 'fresh' else 'not_applicable',
                          resume_requirement='delivery_only', contract_ids=[contract_id]))
    support = 'supported' if is_known else 'unknown'
    return dict(hostdriver_version=1, driver_id='claude', host_version=version, mode=mode,
        capabilities=dict(schema_version=1, id='matrix-v1', cases=cases,
            **dict(zip(('transports', 'states', 'sessions', 'kinds', 'bursts'), map(list, dimensions)))),
        contracts=list(contracts.values()), operations=dict(launch='automatic' if is_known else 'unknown',
            resume=support, reconnect=support, states=dict.fromkeys(dimensions[1], support), barriers=deepcopy(BARRIERS)),
        prerequisites=[dict(id=kind, case_ids=[case['id'] for case in cases], kind=kind, description=description, knownness='known')
            for kind, description in (
                ('executable', 'Selected Claude executable and built adapter'),
                ('authentication', 'Maintainer-authorized authentication and provider environment'),
                ('terminal facility', 'tmux with a private socket'),
                ('endpoint', 'Explicit run-owned scratch broker socket'),
                ('evidence reader', 'Claude SessionStart, proxy records and native transcripts'))],
        observer_context_versions=[1], artifact_grammars=['claude-transcript-jsonl-v1'])


def controlled(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        previous = getattr(self, '_method', None), getattr(self, '_operation_id', None)
        operation_id = self._identity(method.__name__)
        self._operation_id, self._method = operation_id, method.__name__
        try:
            with Deadline(self._budget(method.__name__, args, kwargs), self._clock):
                return method(self, *args, **kwargs)
        except DriverError:
            raise
        except ValueError as error:
            raise DriverError(method.__name__, operation_id, 'invalid_request', f'{type(error).__name__}: {error}') from error
        except (TimeoutError, OSError, subprocess.SubprocessError, RuntimeError) as error:
            kind = ('timeout' if isinstance(error, (TimeoutError, subprocess.TimeoutExpired)) else
                    'io_error' if isinstance(error, (OSError, subprocess.SubprocessError)) else
                    'host_exited' if 'exited' in str(error) or 'pane is unavailable' in str(error) else 'unrecognized_state')
            detail = str(error) if isinstance(error, HostSetupError) else f'{type(error).__name__}: {error}'
            raise DriverError(method.__name__, operation_id, kind, detail,
                              (ArtifactRef('pane', 'control/pane.txt'),),
                              submission_uncertain=self._submission_pending or 'submission uncertain' in str(error)) from error
        finally:
            self._method, self._operation_id = previous
    return call


class ClaudeHostDriver:
    def __init__(self, *, backend_factory=Host, reconnect_keys=None, scripts=None):
        self._backend_factory = backend_factory
        self._reconnect_keys = reconnect_keys
        self._scripts = scripts
        self._backend = None
        self._scratch = self._profile = self._broker = None
        self._clock = time
        self._instance_id = None
        self._sequence = 0
        self._session = None
        self._issued_sessions = set()
        self._resume = None
        self._launch_time = 0
        self._session_line = 0
        self._gates = {}
        self._workload = None
        self._observed_state = None
        self._submission_pending = False
        self._is_disposed = False
        self._is_prepared = False

    def _identity(self, prefix):
        self._sequence += 1
        return f'{self._instance_id or "unprepared"}/{prefix}-{self._sequence}'

    def _budget(self, method, args, kwargs):
        profile = self._profile or (args[3] if method == 'prepare' and len(args) > 3 else kwargs.get('profile'))
        if profile is None:
            return 15
        timeouts = profile.timeouts
        if method == 'enter_state':
            return timeouts.state_seconds
        if method == 'send_prompt':
            return timeouts.submission_seconds
        if method == 'barrier' and args and isinstance(args[0], BarrierPoint) and args[0].kind == 'workload_finished':
            return profile.workload.duration_ms / 1000 + 5
        return getattr(timeouts, method + '_seconds', timeouts.command_seconds)

    def _error(self, kind, detail, *, uncertain=False):
        raise DriverError(self._method, self._operation_id, kind, detail, submission_uncertain=uncertain)

    def _require_live(self):
        if not self._is_prepared:
            self._error('invalid_request', 'driver is not prepared')
        if self._profile.execution == 'replay':
            self._error('unsupported', 'replay control execution is unavailable')
        if self._is_disposed:
            self._error('invalid_request', 'driver has been disposed')

    def _require_session(self):
        self._require_live()
        if self._session is None:
            self._error('invalid_request', 'no active selected conversation')

    def _control(self, facts):
        position, reference = self._scratch.journal.append(self._operation_id, self._method, facts, self._clock)
        session = self._session
        return dict(operation_id=self._operation_id, run_id=self._scratch.run_id,
            session_handle_id=session.handle_id if session else None,
            scope=Scope(self._scratch.run_id, self._broker.route_id,
                        session.host_session_id if session else None, session.session_id if session else None,
                        session.connection_epoch_id if session else None, session.claim_generation if session else None),
            positions=(position,), artifact_refs=(reference,))

    def describe(self, version, mode):
        return describe(version, mode)

    @controlled
    def prepare(self, scratch, adapter, broker, profile):
        if self._profile is not None:
            if self._is_prepared and (scratch, adapter, broker, profile) == self._binding:
                return
            self._error('invalid_request', 'preparation binding cannot change')
        paths = (scratch.root, scratch.cwd, scratch.artifact_store, scratch.journal.root, broker.state_directory, broker.endpoint)
        if any(not path.is_absolute() or not path.resolve().is_relative_to(scratch.root.resolve()) for path in paths):
            self._error('invalid_request', 'resource paths must remain inside the run-owned scratch')
        if not scratch.run_id or not scratch.scratch_id or broker.run_id != scratch.run_id or not broker.route_id:
            self._error('invalid_request', 'scratch and broker run/route binding disagree')
        if scratch.cwd != scratch.root / 'cwd' or broker.state_directory != scratch.root / 'broker' or broker.endpoint != broker.state_directory / 'c3.sock':
            self._error('invalid_request', 'Claude profile requires the isolated cwd and private broker endpoint')
        if not all(path.is_absolute() for path in (adapter.binary, broker.binary, profile.executable)):
            self._error('invalid_request', 'executable paths must be absolute')
        if profile.authentication_reference is not None and (not isinstance(profile.authentication_reference, Path) or not profile.authentication_reference.is_absolute()):
            self._error('invalid_request', 'authentication reference must be an absolute configuration directory')
        if profile.execution not in ('live', 'replay'):
            self._error('invalid_request', 'unknown execution mode')
        for value in asdict(profile.timeouts).values():
            if type(value) is not int or value <= 0 or not math.isfinite(value):
                self._error('invalid_request', 'timeout settings must be positive finite integers')
        if profile.description != describe(profile.host_version, profile.mode):
            self._error('invalid_request', 'profile description does not match the selected driver recipe')
        from verdict_core import CONTRACT, SCENARIO, _validate
        _validate(profile.scenario, SCENARIO, 'scenario')
        _validate(profile.contract, CONTRACT, 'contract')
        if profile.case not in profile.description['capabilities']['cases'] or profile.contract != resolve_contract(profile.description, profile.case, profile.contract['id']):
            self._error('invalid_request', 'profile must pin a declared case and expected contract')
        if (any(profile.scenario[name] != profile.case[name] for name in (*DIMENSIONS, 'freshness', 'resume_requirement'))
                or profile.scenario['count'] != (2 if profile.case['burst'] == 'double' else 1)
                or profile.scenario['run_id'] != scratch.run_id or profile.scenario['route_id'] != broker.route_id):
            self._error('invalid_request', 'scenario and resource bindings disagree')
        self._scratch, self._broker = scratch, broker
        self._profile = replace(profile, description=deepcopy(profile.description), case=deepcopy(profile.case),
                                scenario=deepcopy(profile.scenario), contract=deepcopy(profile.contract))
        self._binding = (scratch, adapter, broker, self._profile)
        self._clock = profile.clock
        self._instance_id = uuid.uuid4().hex
        from hostdrivers.claude_evidence import ClaudeObservation
        self._observation = ClaudeObservation(scratch, broker, adapter, self._profile, self._instance_id)
        if profile.case['feasibility'] != 'feasible':
            self._error('missing_prerequisite' if profile.case['feasibility'] == 'unknown' else 'unsupported', profile.case['reason'])
        if profile.execution == 'replay':
            if profile.artifact_source is None:
                self._error('missing_prerequisite', 'replay requires a frozen artifact source')
            self._is_prepared = True
            return
        self._control({'preparation': 'started'})
        # Allocate before initialization so stop(False) can clean up partial setup.
        self._backend = self._backend_factory.__new__(self._backend_factory) if isinstance(self._backend_factory, type) else None
        arguments = (scratch.root, profile.executable, broker.binary, adapter.binary,
                     self._scripts or Path(__file__).resolve().parents[1], Cell(**{key: profile.case[key] for key in DIMENSIONS}),
                     profile.host_version, profile.timeouts.checkpoint_seconds)
        configuration = {'authentication_reference': profile.authentication_reference} if profile.authentication_reference is not None else {}
        if self._backend is None:
            self._backend = self._backend_factory(*arguments, **configuration)
        else:
            self._backend.__init__(*arguments, **configuration)
        self._send = self._backend.send
        self._backend.send = self.send_prompt
        self._backend.command_timeout = profile.timeouts.command_seconds
        self._backend.exit_timeout = profile.timeouts.exit_seconds
        self._backend.menu_timeout = profile.timeouts.menu_seconds
        self._backend.server_menu_timeout = profile.timeouts.server_menu_seconds
        self._is_prepared = True

    def _bind_session(self):
        records = read_jsonl(self._scratch.root / 'control/sessions.jsonl')[self._session_line:]
        if not records:
            return
        selected = records[-1].get('session_id')
        if type(selected) is not str or not selected:
            self._error('evidence_unavailable', 'SessionStart did not identify the selected conversation')
        if self._session.host_session_id is not None and self._session.host_session_id != selected:
            self._error('identity_conflict', 'SessionStart selected a different conversation')
        if self._resume and self._session.posture == 'resumed' and selected != self._resume.host_session_id:
            self._error('identity_conflict', 'resumed SessionStart does not match the preserved conversation')
        self._session = replace(self._session, host_session_id=selected,
                                artifact_refs=(ArtifactRef('sessions', f'line:{self._session_line + len(records)}'),))

    @controlled
    def launch(self, resume_handle=None):
        self._require_live()
        if self._session is not None:
            self._error('invalid_request', 'a conversation is already active')
        if resume_handle is not None and (not isinstance(resume_handle, ResumeHandle) or
                                          type(resume_handle.schema_version) is not int or resume_handle.schema_version != 1 or
                                          self._resume is None or resume_handle != self._resume):
            self._error('invalid_request', 'foreign or stale resume handle')
        self._launch_time = self._clock.time()
        self._session_line = len(read_jsonl(self._scratch.root / 'control/sessions.jsonl'))
        self._backend.launch(continued=resume_handle is not None)
        self._session = Session(handle_id=self._identity('session'), run_id=self._scratch.run_id,
            scratch_id=self._scratch.scratch_id, host_session_id=None, session_id=None, connection_epoch_id=None,
            claim_generation=None, posture='resumed' if resume_handle else 'fresh', artifact_refs=())
        self._bind_session()
        self._issued_sessions.add(self._session)
        return self._session

    @controlled
    def send_prompt(self, text):
        self._require_session()
        if type(text) is not str or not text.strip():
            self._error('invalid_request', 'prompt must be nonempty text')
        if self._submission_pending:
            self._error('invalid_request', 'previous submission is unresolved', uncertain=True)
        self._submission_pending = True
        self._send(text)
        digest = hashlib.sha256(text.encode()).hexdigest()
        evidence = self._control({'text_digest': digest, 'mechanism': 'composer_transition',
                                  'composer_before': self._backend.submission_composer_before,
                                  'composer': self._backend.submission_composer})
        self._submission_pending = False
        return SubmissionProof(**evidence, submission_id=self._operation_id, text_digest=digest, mechanism='composer_transition')

    @controlled
    def enter_state(self, state, workload):
        self._require_session()
        from hostdriver import Workload
        if type(state) is not str or not isinstance(workload, Workload):
            self._error('invalid_request', 'state and workload must be common control values')
        if self._profile.description['operations']['states'].get(state) != 'supported':
            self._error('unsupported', 'state recipe is not supported')
        if workload.kind not in ('none', 'warmup', 'sleep') or type(workload.duration_ms) is not int or workload.duration_ms < 0:
            self._error('invalid_request', 'invalid workload')
        facts = {'state': state}
        workload_id = None
        if state in ('foreground', 'background'):
            if workload.kind != 'sleep' or workload.duration_ms <= 0 or workload.duration_ms % 1000:
                self._error('invalid_request', 'Claude workload requires whole positive sleep seconds')
            observed = self._backend.tool_state(state == 'background', workload.duration_ms // 1000)
            self._workload = (state, observed)
            workload_id = self._identity('workload')
            facts['legacy_evidence'] = {'tool_state': observed}
        elif state == 'idle':
            if workload.kind == 'warmup':
                self._backend.ready_turn()
            elif workload.kind != 'none':
                self._error('invalid_request', 'idle requires none or warmup workload')
            elif self._backend.composer() is None:
                self._error('unrecognized_state', 'idle input region is unrecognized')
        else:
            gate = self._gates.get('readiness')
            if workload.kind != 'none' or not gate or gate['status'] != 'held':
                self._error('invalid_request', 'requested state requires a held readiness gate')
        self._observed_state = state
        evidence = self._control(facts)
        return StateEvidence(**evidence, state=state, workload_id=workload_id,
                             valid_from=evidence['positions'][0], valid_until=None)

    def _checkpoint(self, boundary):
        facts = {}
        if boundary == 'observation-end':
            # Persist the boundary so frozen observation has the same authority
            # as live observation; read time is never an event timestamp.
            facts['observation_window_end'] = dict(
                observed_state='idle' if self._backend.composer() is not None else None,
                clock_id='control-monotonic', time_ms=int(self._clock.monotonic() * 1000))
        elif boundary == 'adapter-attached':
            gate = self._gates.get('readiness')
            after = gate['event']['time'] if gate and gate['status'] == 'released' else self._launch_time
            self._backend.wait_event('attached', after)
            self._bind_session()
            starts = self._backend.events('proxy_started', self._launch_time)
            if starts and self._session.connection_epoch_id is None:
                self._session = replace(self._session, connection_epoch_id=self._identity('connection'))
            if self._session.posture == 'resumed' and self._session.host_session_id is None:
                self._error('evidence_unavailable', 'resumed SessionStart identity is unavailable')
        elif boundary == 'session-at-arrival':
            self._bind_session()
            if self._session.posture == 'resumed' and self._session.host_session_id is None:
                self._error('evidence_unavailable', 'resumed SessionStart identity is unavailable')
            transcript = self._backend.transcript()
            if self._profile.case['session'] == 'fresh' and transcript and transcript.exists() and transcript.stat().st_size:
                self._error('unrecognized_state', 'RuntimeError: fresh cell already has a transcript before injection')
            from evidence_io import checked_read
            selected_read = checked_read(transcript, jsonl=True) if transcript is not None else None
            if self._session.host_session_id and selected_read is not None and selected_read['state'] == 'complete':
                facts['observer_rows'] = [dict(action='session_sample',
                    operation='resume' if self._session.posture == 'resumed' else 'new',
                    host_session_ref=self._session.host_session_id, transcript_records=len(selected_read['records']))]
            if self._profile.case['transport'] == 'inbox':
                started = self._backend.events('proxy_started', self._launch_time)
                if not started or not started[-1].get('inbox_env'):
                    self._error('missing_prerequisite', 'RuntimeError: host did not supply an owning-session inbox endpoint')
        elif boundary == 'injection-state':
            if self._workload:
                state, observed = self._workload
                calls = [block for record in self._backend.records()
                         for block in record.get('message', {}).get('content', [])
                         if isinstance(block, dict) and block.get('type') == 'tool_use' and block.get('name') == 'Bash']
                if not (self._backend.cwd / 'tool-running').exists() or not any(
                        block.get('input', {}).get('command') == observed['command'] and
                        bool(block.get('input', {}).get('run_in_background', False)) == (state == 'background') for block in calls):
                    self._error('unrecognized_state', 'requested workload is no longer running at injection')
            gate = self._gates.get('readiness')
            if gate and gate['status'] == 'held' and any(
                    event.get('pid') == gate['event']['pid'] for event in self._backend.events('initialized_released', gate['event']['time'])):
                self._error('unrecognized_state', 'readiness gate was released before injection')
            observed_state = self._observed_state
            if self._gates.get('readiness', {}).get('status') == 'held':
                observed_state = self._profile.case['state']
            if observed_state is not None:
                facts['observer_rows'] = [dict(action='state_sample', observed_state=observed_state)]
        elif boundary == 'offers-before-ready':
            gate = self._gates.get('readiness')
            if not gate or gate['status'] != 'held':
                self._error('invalid_request', 'before-ready checkpoint requires a held gate')
            event = gate['event']
            releases = [item['time'] for item in self._backend.events('initialized_released', gate['time']) if item.get('pid') == event['pid']]
            before = releases[0] if releases else float('inf')
            observations = ['gated proxy forwarded a channel notification'] if any(
                item.get('pid') == event['pid'] and item['time'] < before for item in self._backend.events('channel_notify', gate['time'])) else []
            facts['legacy_evidence'] = {'attempt_before_ready_observations': observations}
        return facts

    def _held(self, kind, gate, event):
        if kind == 'readiness':
            recent = self._backend.events()[gate['event_count']:]
            starts = [item for item in recent if item['event'] == 'proxy_started' and item.get('pid') == event.get('pid')]
            if not starts or event not in recent or event['time'] < starts[-1]['time']:
                self._error('identity_conflict', 'readiness event does not identify the replacement connection')
            self._session = replace(self._session, connection_epoch_id=self._identity('connection'))
            self._bind_session()
        if gate['session_handle_id'] is not None and gate['session_handle_id'] != self._session.handle_id:
            self._error('invalid_request', 'gate belongs to another conversation lifetime')
        gate.update(status='held', event=event, session_handle_id=self._session.handle_id)

    @controlled
    def barrier(self, point):
        self._require_live()
        if not isinstance(point, BarrierPoint):
            self._error('invalid_request', 'barrier point must be a common control record')
        declared = self._profile.description['operations']['barriers']
        if point.kind not in declared or point.boundary_id not in declared[point.kind].values():
            self._error('unsupported', 'barrier boundary is not declared')
        if not point.id or (point.session_handle_id is not None and
                            (self._session is None or point.session_handle_id != self._session.handle_id)):
            self._error('invalid_request', 'foreign barrier session handle')
        facts = {}
        release_id = None
        if point.kind == 'checkpoint':
            self._require_session()
            if point.action != 'sample' or point.release_id is not None:
                self._error('invalid_request', 'checkpoint requires sample without a release handle')
            facts = self._checkpoint(point.boundary_id)
            status = 'reached'
        elif point.kind == 'workload_finished':
            self._require_session()
            if point.action != 'wait' or not self._workload:
                self._error('invalid_request', 'workload completion requires an active requested workload')
            wait_for(lambda: not (self._backend.cwd / 'tool-running').exists(),
                     self._profile.workload.duration_ms / 1000 + 5, 'foreground tool did not finish')
            status = 'reached'
        else:
            gate = self._gates.get(point.kind)
            if point.action == 'arm':
                if point.release_id is not None or gate and gate['status'] != 'released':
                    self._error('invalid_request', 'gate is already armed or release handle is unexpected')
                filename = 'gate-initialized' if point.kind == 'readiness' else 'gate-fetch'
                # Release files are owned by this scratch and may outlive a PID.
                pattern = 'release-initialized-*' if point.kind == 'readiness' else 'release-fetch'
                for path in self._backend.control.glob(pattern):
                    path.unlink()
                (self._backend.control / filename).touch()
                gate = dict(id=point.id, boundary_id=point.boundary_id, status='armed',
                            release_id=self._identity('release'), event_count=len(self._backend.events()),
                            time=self._clock.time(), event=None, session_handle_id=self._session.handle_id if self._session else None)
                self._gates[point.kind] = gate
            else:
                self._require_session()
                if not gate or point.id != gate['id'] or point.boundary_id != gate['boundary_id'] or point.release_id != gate['release_id']:
                    self._error('invalid_request', 'foreign or stale gate release handle')
                if point.action == 'wait':
                    if gate['status'] == 'armed':
                        name = 'initialized_waiting' if point.kind == 'readiness' else 'fetch_result_waiting'
                        event = self._backend.wait_event(name, gate['time'])
                        self._held(point.kind, gate, event)
                    elif gate['status'] != 'held':
                        self._error('invalid_request', 'released gate cannot be waited again')
                    if point.kind == 'fetch_result':
                        facts['legacy_evidence'] = {'fetch_result': gate['event']['frame'].get('result')}
                elif point.action == 'release':
                    if gate['session_handle_id'] != self._session.handle_id:
                        self._error('invalid_request', 'gate belongs to another conversation lifetime')
                    if gate['status'] == 'held':
                        filename = f"release-initialized-{gate['event']['pid']}" if point.kind == 'readiness' else 'release-fetch'
                        (self._backend.control / filename).touch()
                        gate['status'] = 'released'
                    elif gate['status'] != 'released':
                        self._error('invalid_request', 'gate has not reached the held boundary')
                else:
                    self._error('invalid_request', 'invalid gate action')
            status, release_id = gate['status'], gate['release_id']
        barrier_record = asdict(point)
        barrier_record['barrier_id'] = barrier_record.pop('id')
        evidence = self._control(dict(facts, barrier=barrier_record, status=status))
        return Release(**evidence, release_id=release_id, barrier_id=point.id, status=status, cuts=())

    @controlled
    def reconnect(self, session):
        self._require_session()
        if not isinstance(session, Session) or session not in self._issued_sessions or session.handle_id != self._session.handle_id:
            self._error('invalid_request', 'foreign or stale session handle')
        gate = self._gates.get('readiness')
        if not gate or gate['status'] != 'armed':
            self._error('invalid_request', 'reconnect requires a replacement readiness gate')
        previous = self._session
        event = self._backend.reconnect(self._reconnect_keys)
        self._held('readiness', gate, event)
        self._issued_sessions.add(self._session)
        evidence = self._control({'previous_session': asdict(previous), 'current_session': asdict(self._session)})
        return ReconnectEvidence(**evidence, previous_session=previous, current_session=self._session)

    @controlled
    def observe(self, cursor=None):
        if self._profile is None:
            self._error('invalid_request', 'driver is not prepared')
        if self._profile.execution == 'live' and self._session is not None and not self._is_disposed:
            self._backend.pane()
        return self._observation.observe(cursor)

    @controlled
    def stop(self, preserve_session):
        if type(preserve_session) is not bool:
            self._error('invalid_request', 'preserve_session must be a boolean')
        if preserve_session:
            if self._session is None and self._resume is not None:
                return self._resume
            self._require_session()
            self._bind_session()
            if self._session.host_session_id is None:
                self._error('evidence_unavailable', 'cannot preserve an unidentified conversation')
            self._backend.stop_session()
            self._resume = ResumeHandle(handle_id=self._identity('resume'), driver_id=self._profile.description['driver_id'],
                host_version=self._profile.host_version, mode=self._profile.mode, run_id=self._scratch.run_id,
                scratch_id=self._scratch.scratch_id, host_session_id=self._session.host_session_id,
                artifact_refs=self._session.artifact_refs)
            self._session = None
            return self._resume
        if self._is_disposed:
            return None
        if self._backend is not None:
            try:
                self._backend.close()
            finally:
                for kind, gate in self._gates.items():
                    filename = 'gate-initialized' if kind == 'readiness' else 'gate-fetch'
                    (self._backend.control / filename).unlink(missing_ok=True)
                    gate['status'] = 'released'
        self._is_disposed = True
        self._session = None
        return None

    # Compatibility readers for legacy fixture callers; live assembly uses observe().
    def records(self):
        return self._backend.records() if self._backend is not None else []

    def transcript(self):
        return self._backend.transcript() if self._backend is not None else None

    def events(self, name=None, after=0):
        return self._backend.events(name, after) if self._backend is not None else []
