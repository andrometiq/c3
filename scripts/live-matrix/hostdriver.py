"""Version 1 host lifecycle records; control evidence is never delivery proof."""
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol, TypedDict
import json
import math
import time

Id = str
State = Literal['idle', 'foreground', 'background', 'startup', 'reconnect']
Scenario = dict
Contract = dict


class CapabilityCase(TypedDict):
    id: Id
    transport: str
    state: State
    session: str
    kind: str
    burst: str
    freshness: str
    resume_requirement: str
    feasibility: Literal['feasible', 'infeasible', 'unknown']
    reason: str
    contract_ids: list[Id]


class Capabilities(TypedDict):
    schema_version: int
    id: Id
    transports: list[str]
    states: list[State]
    sessions: list[str]
    kinds: list[str]
    bursts: list[str]
    cases: list[CapabilityCase]


class OperationSupport(TypedDict):
    launch: str
    resume: str
    reconnect: str
    states: dict[str, str]
    barriers: dict[str, dict[str, Id]]


class Prerequisite(TypedDict):
    id: Id
    case_ids: list[Id]
    kind: str
    description: str
    knownness: str


class DriverDescription(TypedDict):
    hostdriver_version: int
    driver_id: Id
    host_version: str
    mode: Id
    capabilities: Capabilities
    contracts: list[Contract]
    operations: OperationSupport
    prerequisites: list[Prerequisite]
    observer_context_versions: list[int]
    artifact_grammars: list[Id]


@dataclass(frozen=True)
class ArtifactRef:
    artifact_id: Id
    locator: str


@dataclass(frozen=True)
class Scope:
    run_id: Id
    route_id: Id | None
    host_session_id: Id | None
    session_id: Id | None
    connection_epoch_id: Id | None
    claim_generation: int | None


@dataclass(frozen=True)
class Position:
    stream_id: Id
    seq: int
    clock_id: Id | None
    time_ms: int | None


@dataclass(frozen=True, kw_only=True)
class ControlEvidence:
    schema_version: int = 1
    operation_id: Id
    run_id: Id
    session_handle_id: Id | None
    scope: Scope
    positions: tuple[Position, ...]
    artifact_refs: tuple[ArtifactRef, ...]


@dataclass(frozen=True, kw_only=True)
class Session:
    schema_version: int = 1
    handle_id: Id
    run_id: Id
    scratch_id: Id
    host_session_id: Id | None
    session_id: Id | None
    connection_epoch_id: Id | None
    claim_generation: int | None
    posture: Literal['fresh', 'resumed']
    artifact_refs: tuple[ArtifactRef, ...]


@dataclass(frozen=True, kw_only=True)
class ResumeHandle:
    schema_version: int = 1
    handle_id: Id
    driver_id: Id
    host_version: str
    mode: Id
    run_id: Id
    scratch_id: Id
    host_session_id: Id
    artifact_refs: tuple[ArtifactRef, ...]


@dataclass(frozen=True, kw_only=True)
class SubmissionProof(ControlEvidence):
    submission_id: Id
    text_digest: str
    mechanism: Literal['composer_transition', 'host_record', 'input_ack']


@dataclass(frozen=True)
class Workload:
    kind: Literal['none', 'warmup', 'sleep'] = 'none'
    duration_ms: int = 0
    marker: str | None = None


@dataclass(frozen=True, kw_only=True)
class StateEvidence(ControlEvidence):
    state: State
    workload_id: Id | None
    valid_from: Position
    valid_until: Position | None


@dataclass(frozen=True)
class BarrierPoint:
    id: Id
    kind: Literal['readiness', 'fetch_result', 'checkpoint', 'workload_finished']
    action: Literal['arm', 'wait', 'release', 'sample']
    boundary_id: Id
    session_handle_id: Id | None = None
    release_id: Id | None = None


@dataclass(frozen=True)
class Cut:
    stream_id: Id
    seq: int


@dataclass(frozen=True, kw_only=True)
class Release(ControlEvidence):
    release_id: Id | None
    barrier_id: Id
    status: Literal['armed', 'held', 'released', 'reached']
    cuts: tuple[Cut, ...]


@dataclass(frozen=True, kw_only=True)
class ReconnectEvidence(ControlEvidence):
    previous_session: Session
    current_session: Session


@dataclass(frozen=True)
class Extent:
    artifact_id: Id
    bytes: int
    lines: int
    sha256: str


@dataclass(frozen=True, kw_only=True)
class ObservationCursor:
    schema_version: int = 1
    driver_instance_id: Id
    run_id: Id
    snapshot_id: Id
    extents: tuple[Extent, ...]


@dataclass(frozen=True)
class ObserverTables:
    context_version: int = 1
    reads: tuple = ()


@dataclass(frozen=True)
class ArtifactBundle:
    run_id: Id
    snapshot_id: Id
    next_cursor: ObservationCursor | None
    provenance: tuple = ()
    inventory: tuple = ()
    artifacts: tuple[tuple[ArtifactRef, bytes], ...] = ()
    control_refs: tuple[ArtifactRef, ...] = ()
    diagnostics: tuple[str, ...] = ()


class DriverError(RuntimeError):
    def __init__(self, method, operation_id, kind, detail, artifact_refs=(), submission_uncertain=False):
        super().__init__(detail)
        self.method, self.operation_id, self.kind, self.detail = method, operation_id, kind, detail
        self.artifact_refs = tuple(artifact_refs)
        self.submission_uncertain = submission_uncertain


class Clock(Protocol):
    def monotonic(self) -> float: ...
    def time(self) -> float: ...
    def sleep(self, seconds: float) -> None: ...


_active_deadline = ContextVar('hostdriver_deadline', default=None)


def remaining_timeout(seconds):
    deadline = _active_deadline.get()
    if deadline is None:
        return seconds
    remaining = deadline.end - deadline.clock.monotonic()
    if remaining <= 0:
        raise TimeoutError('operation deadline expired')
    return min(seconds, remaining)


class Deadline:
    def __init__(self, seconds, clock=time):
        if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0:
            raise ValueError('deadline must be finite and positive')
        self.clock = clock
        self.end = clock.monotonic() + seconds
        parent = _active_deadline.get()
        if parent:
            self.end = min(self.end, parent.end)

    def __enter__(self):
        self.token = _active_deadline.set(self)
        return self

    def __exit__(self, *error):
        _active_deadline.reset(self.token)


@dataclass(frozen=True)
class Timeouts:
    checkpoint_seconds: int = 90
    command_seconds: int = 15
    submission_seconds: int = 8
    exit_seconds: int = 15
    menu_seconds: int = 15
    server_menu_seconds: int = 10
    prepare_seconds: int = 180
    launch_seconds: int = 15
    state_seconds: int = 98
    barrier_seconds: int = 90
    reconnect_seconds: int = 160
    stop_seconds: int = 23
    observe_seconds: int = 80


class ControlJournal:
    """Supporting controls, separate from the closed capture-context driver table."""
    def __init__(self, root: Path):
        self.root = root
        self.sequence = 0
        self.entries = {}

    def append(self, operation_id, method, facts, clock=time):
        self.root.mkdir(parents=True, exist_ok=True)
        self.sequence += 1
        entry = dict(operation_id=operation_id, method=method, facts=facts)
        with (self.root / 'operations.jsonl').open('a') as output:
            output.write(json.dumps(entry) + '\n')
        self.entries[self.sequence] = entry
        position = Position('controls', self.sequence, 'control-monotonic', int(clock.monotonic() * 1000))
        return position, ArtifactRef('controls', f'line:{self.sequence}')

    def legacy_evidence(self, result: ControlEvidence):
        evidence = {}
        for reference in result.artifact_refs:
            if reference.artifact_id == 'controls':
                sequence = int(reference.locator.removeprefix('line:'))
                evidence.update(self.entries[sequence]['facts'].get('legacy_evidence', {}))
        return json.loads(json.dumps(evidence))


@dataclass(frozen=True)
class Scratch:
    root: Path
    scratch_id: Id
    run_id: Id
    cwd: Path
    artifact_store: Path
    journal: ControlJournal
    capture_journals: tuple = ()


@dataclass(frozen=True)
class Adapter:
    binary: Path
    build_id: Id


@dataclass(frozen=True)
class Broker:
    binary: Path
    endpoint: Path
    state_directory: Path
    run_id: Id
    route_id: Id
    observer_store: tuple = ()


@dataclass(frozen=True)
class RunProfile:
    description: DriverDescription
    case: CapabilityCase
    scenario: Scenario
    contract: Contract
    executable: Path
    host_version: str
    mode: Id
    authentication_reference: Path | None
    execution: Literal['live', 'replay'] = 'live'
    workload: Workload = Workload('sleep', 35000, 'workload')
    timeouts: Timeouts = Timeouts()
    profile_reference: Id | None = None
    artifact_source: Path | None = None
    clock: Clock = field(default=time, compare=False)


class HostDriver(Protocol):
    def describe(self, version: str, mode: Id) -> DriverDescription: ...
    def prepare(self, scratch: Scratch, adapter: Adapter, broker: Broker, profile: RunProfile) -> None: ...
    def launch(self, resume_handle: ResumeHandle | None = None) -> Session: ...
    def send_prompt(self, text: str) -> SubmissionProof: ...
    def enter_state(self, state: State, workload: Workload) -> StateEvidence: ...
    def barrier(self, point: BarrierPoint) -> Release: ...
    def reconnect(self, session: Session) -> ReconnectEvidence: ...
    def observe(self, cursor: ObservationCursor | None = None) -> tuple[ObserverTables, ArtifactBundle]: ...
    def stop(self, preserve_session: bool) -> ResumeHandle | None: ...


def resolve_contract(description, case, contract_id=None):
    from copy import deepcopy
    from verdict_core import ID, InvalidInput, _validate
    try:
        validate_description(description)
        _validate(case, _description_schema()['capabilities']['cases'][0], 'case')
        if case not in description['capabilities']['cases']:
            raise InvalidInput('selected case is not declared')
        choices = case['contract_ids']
        if contract_id is None:
            if len(choices) != 1:
                raise InvalidInput('select exactly one expected contract before injection')
            contract_id = choices[0]
        _validate(contract_id, ID, 'contract_id')
        if contract_id not in choices:
            raise InvalidInput('selected contract is not bound to case')
        return deepcopy(next(contract for contract in description['contracts'] if contract['id'] == contract_id))
    except (InvalidInput, DriverError) as error:
        raise DriverError('prepare', 'profile', 'invalid_request', str(error)) from error


def _description_schema():
    """Closed records for every required profile field; maps are checked below."""
    from verdict_core import CONTRACT, ID, SCENARIO
    dimensions = ('transport', 'state', 'session', 'kind', 'burst')
    summaries = ('transports', 'states', 'sessions', 'kinds', 'bursts')
    support = ('supported', 'unsupported', 'unknown')
    case = dict(id=ID, **{name: SCENARIO[name] for name in dimensions}, freshness=SCENARIO['freshness'],
                resume_requirement=SCENARIO['resume_requirement'], feasibility=('feasible', 'infeasible', 'unknown'),
                reason=str, contract_ids=[ID])
    return dict(hostdriver_version=int, driver_id=ID, host_version=ID, mode=ID,
        capabilities=dict(schema_version=int, id=ID,
            **{plural: [SCENARIO[name]] for name, plural in zip(dimensions, summaries)}, cases=[case]),
        contracts=[CONTRACT], operations=dict(launch=('automatic', 'externally_prepared', 'unknown'),
            resume=support, reconnect=support, states=dict, barriers=dict),
        prerequisites=[dict(id=ID, case_ids=[ID], kind=('executable', 'authentication', 'terminal facility',
            'endpoint', 'launch recipe', 'state recipe', 'reconnect recipe', 'evidence reader'),
            description=ID, knownness=('known', 'unknown'))],
        observer_context_versions=[int], artifact_grammars=[ID])


def validate_description(description):
    """Validate the complete record, then all identifier joins in one boundary."""
    from verdict_core import ID, InvalidInput, _validate, _validate_inputs
    def unique(values, path):
        if len(set(values)) != len(values):
            raise InvalidInput(path + ' must be duplicate-free')
    def bound(values, declared, path):
        unique(values, path)
        if any(value not in declared for value in values):
            raise InvalidInput(path + ' contains an undeclared reference')
    try:
        _validate(description, _description_schema(), 'description')
        capabilities = description['capabilities']
        if description['hostdriver_version'] != 1 or capabilities['schema_version'] != 1:
            raise InvalidInput('description and capabilities versions must equal 1')
        for name in ('observer_context_versions', 'artifact_grammars'):
            unique(description[name], name)
        if any(version <= 0 for version in description['observer_context_versions']):
            raise InvalidInput('observer context versions must be positive')
        operations = description['operations']
        _validate(operations['states'], {state: ('supported', 'unsupported', 'unknown')
                  for state in capabilities['states']}, 'operations.states')
        for kind, boundaries in operations['barriers'].items():
            _validate(kind, ('readiness', 'fetch_result', 'checkpoint', 'workload_finished'), 'barrier kind')
            _validate(boundaries, dict, 'barrier boundaries')
            for name, boundary in boundaries.items():
                _validate(name, ID, 'barrier name')
                _validate(boundary, ID, 'barrier boundary')
        contracts = description['contracts']
        contract_ids = [contract['id'] for contract in contracts]
        unique(contract_ids, 'contract identifiers')
        for contract in contracts:
            if contract['schema_version'] != 1 or contract['capability_id'] != capabilities['id']:
                raise InvalidInput('contract version or capability binding is invalid')
            bound(contract['contract_barrier_names'], operations['barriers'].get('checkpoint', {}), 'contract barriers')
            if contract['readiness_boundary'] is not None:
                bound([contract['readiness_boundary']], operations['barriers'].get('readiness', {}), 'readiness boundary')
        dimensions = ('transport', 'state', 'session', 'kind', 'burst')
        summaries = ('transports', 'states', 'sessions', 'kinds', 'bursts')
        for plural in summaries:
            unique(capabilities[plural], plural)
        cases = capabilities['cases']
        case_ids = [case['id'] for case in cases]
        unique(case_ids, 'case identifiers')
        unique([tuple(case[name] for name in dimensions) for case in cases], 'case dimensions')
        for case in cases:
            for name, plural in zip(dimensions, summaries):
                bound([case[name]], capabilities[plural], 'case.' + name)
            if case['feasibility'] != 'feasible' and not case['reason']:
                raise InvalidInput('unknown/infeasible cases require an explanation')
            bound(case['contract_ids'], contract_ids, 'case.contract_ids')
            # Reuse the core's semantic scenario/contract checks. A setup-error
            # envelope ends validation before any observation/evidence checks.
            count = 1 if case['burst'] == 'single' else 2
            scenario = dict(schema_version=1, id=case['id'],
                **{name: case[name] for name in dimensions}, count=count,
                freshness=case['freshness'], resume_requirement=case['resume_requirement'],
                health_class='healthy', final_dispositions=['delivered'] * count, evaluation_mode='verify',
                run_id='profile-validation', route_id='profile-validation', host_session_id=None, session_id=None,
                injection_barrier_id='injection', readiness_barrier_id=None, final_barrier_id='final',
                observation_duration_ms=0)
            for contract in contracts:
                if contract['id'] in case['contract_ids']:
                    _validate_inputs(scenario, contract, dict(schema_version=1,
                        setup_errors=['profile validation only'], run_errors=[], collection_complete=False))
        unique([item['id'] for item in description['prerequisites']], 'prerequisite identifiers')
        for item in description['prerequisites']:
            bound(item['case_ids'], case_ids, 'prerequisite.case_ids')
    except InvalidInput as error:
        raise DriverError('describe', 'profile', 'invalid_request', str(error)) from error
