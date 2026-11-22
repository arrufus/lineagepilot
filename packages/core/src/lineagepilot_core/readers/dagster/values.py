"""The values the Dagster reader's evaluator makes of Dagster's calls (tasks 8.4 and 8.6).

Each is a ``CallV``, so a value imported from another workspace module keeps what it is: the
shared evaluator follows imports of values of that kind. A definition, a job or a schedule has a
``key``, its file and line, by which a ``Definitions`` call in another file is matched to it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lineagepilot_core.readers.common.values import CallV, PathV


@dataclass
class AssetsV(CallV):
    """A dbt asset definition, with ``key``, where it is defined."""

    key: str = ""


@dataclass
class ModulesV(CallV):
    """``load_assets_from_modules(…)`` and its kin: every definition in these modules, and in the
    packages, each relative to the workspace."""

    modules: tuple[str, ...] = ()
    packages: tuple[str, ...] = ()


@dataclass
class ManifestPath(PathV):
    """``DbtProject(…).manifest_path``: a manifest whose dbt project is known, as ``project``."""

    project: str = ""


@dataclass
class SelectionV(CallV):
    """An ``AssetSelection``, as a tree of JSON (``select_syntax``), resolved once the code
    location's assets are known (``jobs``)."""

    tree: dict[str, Any] = field(default_factory=dict)


@dataclass
class JobV(CallV):
    """An asset job, by its ``key`` and its name."""

    key: str = ""
    name: str = ""


@dataclass
class ScheduleV(CallV):
    """A schedule, by its ``key`` and its name."""

    key: str = ""
    name: str = ""


@dataclass
class SpecV(CallV):
    """``AssetSpec(key, deps=…, group_name=…)``: an asset by its key, what it depends on, and its
    group, with ``key``, where it is written (task 8.7)."""

    key: str = ""
    asset_key: list[str] = field(default_factory=list)
    deps: list[dict[str, Any]] = field(default_factory=list)
    deps_unknown: str | None = None
    group: str | None = None
