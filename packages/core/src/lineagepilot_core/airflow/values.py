"""Abstract values produced by the static evaluator.

Nothing here executes user code. Each value records how much we trust it:

- ``declared``: a literal in the file.
- ``inferred``: computed from literals (f-strings, path joins, imports of constants).
- ``heuristic``: depends on a guess, such as an environment variable's default.
"""

from __future__ import annotations

import posixpath
from dataclasses import dataclass, field
from typing import Any

from lineagepilot_core.airflow.safe_io import is_absolute

CONFIDENCE_RANK = {"declared": 3, "inferred": 2, "heuristic": 1}


def weakest(*confs: str) -> str:
    """Return the weakest confidence of the given ones."""
    present = [c for c in confs if c]
    if not present:
        return "declared"
    return min(present, key=lambda c: CONFIDENCE_RANK.get(c, 0))


@dataclass
class Val:
    conf: str = field(default="declared", kw_only=True)
    notes: tuple[str, ...] = field(default=(), kw_only=True)

    def with_meta(self, conf: str, notes: tuple[str, ...] = ()) -> Val:
        self.conf = weakest(self.conf, conf)
        self.notes = tuple(dict.fromkeys(self.notes + tuple(notes)))
        return self


@dataclass
class Const(Val):
    value: Any


@dataclass
class PathV(Val):
    path: str

    def join(self, other: str) -> PathV:
        if is_absolute(other):
            return PathV(posixpath.normpath(other), conf=self.conf, notes=self.notes)
        return PathV(
            posixpath.normpath(posixpath.join(self.path, other)), conf=self.conf, notes=self.notes
        )


@dataclass
class ListV(Val):
    items: list[Val]


@dataclass
class DictV(Val):
    items: dict[str, Val]


@dataclass
class Sym(Val):
    """A dotted name bound to no value, such as ``TestBehavior.BUILD`` or ``cosmos.DbtDag``."""

    name: str


@dataclass
class CallV(Val):
    """A call to something we model as a plain constructor (ProjectConfig, RenderConfig, ...)."""

    func: str
    args: list[Val]
    kwargs: dict[str, Val]
    line: int = 0
    col: int = 0
    has_star: bool = False

    def arg(self, name: str, pos: int | None = None) -> Val | None:
        if name in self.kwargs:
            return self.kwargs[name]
        if pos is not None and pos < len(self.args):
            return self.args[pos]
        return None


@dataclass
class TaskV(Val):
    """A handle to an Airflow task or task group created by an operator call."""

    handle: Any


@dataclass
class PartialV(Val):
    """``Operator.partial(...)``: kwargs bound, waiting for ``.expand(...)``."""

    func: str
    kwargs: dict[str, Val]
    line: int = 0
    col: int = 0


@dataclass
class FuncV(Val):
    """A function defined in the scanned module (helper, ``@task`` or ``@dag``)."""

    name: str
    node: Any
    kind: str  # "plain", "task", "task_bash", "dag", "task_group"
    decorator_kwargs: dict[str, Val] = field(default_factory=dict)
    defining_scope: Any = None
    imported_from: str | None = None


@dataclass
class DagV(Val):
    dag_id: str | None
    reason: str | None = None
    owner: str | None = None


@dataclass
class Unknown(Val):
    reason: str
    line: int = 0
    col: int = 0


def is_unknown(v: Val | None) -> bool:
    return v is None or isinstance(v, Unknown)


def as_str(v: Val | None) -> str | None:
    """Return a plain string for Const/PathV string-like values, else None."""
    if isinstance(v, Const) and isinstance(v.value, str):
        return v.value
    if isinstance(v, PathV):
        return v.path
    return None


def as_str_list(v: Val | None) -> list[str] | None:
    """Return a list of strings from a str, list or tuple value; None if any part is unknown."""
    if v is None:
        return []
    if isinstance(v, Const) and v.value is None:
        return []
    s = as_str(v)
    if s is not None:
        return [s]
    if isinstance(v, ListV):
        out: list[str] = []
        for item in v.items:
            si = as_str(item)
            if si is None:
                return None
            out.append(si)
        return out
    return None


def collect_meta(*vals: Val | None) -> tuple[str, tuple[str, ...]]:
    """Weakest confidence and merged notes over several values (including list items)."""
    confs: list[str] = []
    notes: list[str] = []

    def visit(v: Val | None) -> None:
        if v is None or isinstance(v, Unknown):
            return
        confs.append(v.conf)
        notes.extend(v.notes)
        if isinstance(v, ListV):
            for i in v.items:
                visit(i)
        elif isinstance(v, DictV):
            for i in v.items.values():
                visit(i)
        elif isinstance(v, CallV):
            for i in v.args:
                visit(i)
            for i in v.kwargs.values():
                visit(i)

    for v in vals:
        visit(v)
    return weakest(*confs), tuple(dict.fromkeys(notes))


def same_value(a: Val, b: Val) -> bool:
    """Structural equality used when merging the two arms of a conditional."""
    if type(a) is not type(b):
        return False
    if isinstance(a, Const):
        return a.value == b.value  # type: ignore[attr-defined]
    if isinstance(a, PathV):
        return a.path == b.path  # type: ignore[attr-defined]
    if isinstance(a, Sym):
        return a.name == b.name  # type: ignore[attr-defined]
    if isinstance(a, ListV):
        return len(a.items) == len(b.items) and all(  # type: ignore[attr-defined]
            same_value(x, y)
            for x, y in zip(a.items, b.items, strict=False)  # type: ignore[attr-defined]
        )
    if isinstance(a, DictV):
        return a.items.keys() == b.items.keys() and all(  # type: ignore[attr-defined]
            same_value(a.items[k], b.items[k])
            for k in a.items  # type: ignore[attr-defined]
        )
    if isinstance(a, (TaskV, FuncV, CallV, DagV, PartialV)):
        return a is b
    return False


def merge_values(a: Val, b: Val, reason: str) -> Val:
    """Merge the values a name has after two alternative branches."""
    if a is b or same_value(a, b):
        return a
    if isinstance(a, DictV) and isinstance(b, DictV):
        merged: dict[str, Val] = {}
        for k in set(a.items) | set(b.items):
            if k in a.items and k in b.items:
                merged[k] = merge_values(a.items[k], b.items[k], reason)
            else:
                merged[k] = Unknown(f"key {k!r} set only in one branch ({reason})")
        return DictV(merged, conf=weakest(a.conf, b.conf), notes=a.notes + b.notes)
    return Unknown(reason)
