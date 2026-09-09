"""The complete Cartesian product; no host processes are needed to list it."""
from dataclasses import dataclass
from itertools import product
from fnmatch import fnmatchcase


@dataclass(frozen=True)
class Cell:
    transport: str
    state: str
    session: str
    kind: str
    burst: str

    @property
    def name(self):
        return "-".join((self.transport, self.state, self.session, self.kind, self.burst))

    @property
    def count(self):
        return 2 if self.burst == "double" else 1

    @property
    def infeasible(self):
        if self.session == "fresh" and self.state in ("foreground", "background", "reconnect"):
            return "a tool call or reconnect requires an existing transcript"
        return ""


def cells():
    return [Cell(*args) for args in product(
        ("channel", "inbox", "fetch"),
        ("idle", "foreground", "background", "startup", "reconnect"),
        ("fresh", "resumed"), ("text", "voice", "photo"), ("single", "double"))]


def selection(pattern="*"):
    """Matched cells, runnable cells, and actual conversational launches."""
    matched = [c for c in cells() if fnmatchcase(c.name, pattern)]
    feasible = [c for c in matched if not c.infeasible]
    launches = sum(2 if c.session == "resumed" else 1 for c in feasible)
    return matched, feasible, launches


def selection_summary(pattern="*"):
    matched, feasible, launches = selection(pattern)
    return (f"Selected {len(matched)} cells: {len(feasible)} feasible, "
            f"{len(matched) - len(feasible)} N/A; launches {launches} Claude sessions "
            "(including resume seeds).")
