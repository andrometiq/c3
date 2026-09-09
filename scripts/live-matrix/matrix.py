"""The complete Cartesian product; no host processes are needed to list it."""
from dataclasses import dataclass
from itertools import product


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
