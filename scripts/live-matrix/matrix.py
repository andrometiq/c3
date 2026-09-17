"""Capability-case selection; listing never needs an installed host."""
from dataclasses import InitVar, dataclass
from itertools import product
from fnmatch import fnmatchcase

DIMENSIONS = ('transport', 'state', 'session', 'kind', 'burst')
SUMMARIES = ('transports', 'states', 'sessions', 'kinds', 'bursts')


@dataclass(frozen=True)
class Cell:
    transport: str
    state: str
    session: str
    kind: str
    burst: str
    capability_case: InitVar[dict | None] = None

    def __post_init__(self, capability_case):
        # Keep the five-field serialized Cell shape used by existing fixtures.
        object.__setattr__(self, 'capability_case', capability_case)

    @property
    def name(self):
        return '-'.join(getattr(self, name) for name in DIMENSIONS)

    @property
    def count(self):
        return 2 if self.burst == 'double' else 1

    @property
    def infeasible(self):
        case = self.capability_case or {}
        return case.get('reason', '') if case.get('feasibility') == 'infeasible' else ''

    @property
    def feasibility(self):
        return (self.capability_case or {}).get('feasibility', 'unknown')


def default_description():
    from hostdrivers.claude import describe
    return describe('2.1.267', 'matrix')


def cells(description=None):
    from copy import deepcopy
    from hostdriver import validate_description
    description = description if description is not None else default_description()
    validate_description(description)
    capabilities = description['capabilities']
    declared = {tuple(case[name] for name in DIMENSIONS): case for case in capabilities['cases']}
    result = []
    # Missing combinations stay visible as unknown, never as runnable recipes.
    for values in product(*(capabilities[name] for name in SUMMARIES)):
        case = declared.get(values)
        result.append(Cell(*values, capability_case=deepcopy(case)))
    return result


def selection(pattern='*', description=None):
    """Matched cases, selected non-infeasible cases, and conversational launches."""
    matched = [cell for cell in cells(description) if fnmatchcase(cell.name, pattern)]
    selected = [cell for cell in matched if not cell.infeasible]
    launches = sum(2 if cell.session == 'resumed' else 1 for cell in selected)
    return matched, selected, launches


def selection_summary(pattern='*', description=None):
    matched, selected, launches = selection(pattern, description)
    unknown = sum(cell.feasibility == 'unknown' for cell in matched)
    label = 'Claude' if description is None else description['driver_id'].capitalize()
    counts = f'{len(selected) - unknown} feasible, {len(matched) - len(selected)} N/A'
    if unknown:
        counts += f', {unknown} unknown'
    return (f'Selected {len(matched)} cells: {counts}; launches {launches} {label} sessions '
            '(including resume seeds).')
