"""Which code location a Dagster definition is in, and the dg project around it (task 8.3).

A code location's name comes, in this order, from (design_next_phase section 7.1, mapping note):

1. a dg project's ``pyproject.toml``: ``[tool.dg.project] code_location_name``, or else
   ``[project] name``, as dagster-dg-core 1.13.29 names it;
2. the ``workspace.yaml`` or ``dagster_cloud.yaml`` entry that loads it (``locations``, task 8.4);
3. ``[tool.dagster]``: ``code_location_name``, or else ``module_name`` (task 8.4);
4. LineagePilot's own fallback (D-d).

Only the few keys these need are read from ``pyproject.toml``, each a plain string on its own line.
Python 3.10, which the language server still runs on, has no ``tomllib``, and a TOML parser would
be a dependency for four keys. A value written any other way is not read, and the next rule
applies.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from lineagepilot_core.readers.common.safe_io import read_capped, resolve_inside, to_posix

FILE_SIZE_CAP = 2 * 1024 * 1024
# What a dg project calls its definitions folder when its pyproject does not say
DEFAULT_DEFS = "defs"
# Code location and module names are short: a longer one is not a name
MAX_NAME = 200

_TABLE = re.compile(r"^\s*\[\s*([A-Za-z0-9_.\-\"' ]+?)\s*\]\s*(?:#.*)?$")
_STRING = re.compile(
    r"""^\s*([A-Za-z0-9_\-]+)\s*=\s*(?:"((?:[^"\\\n]|\\.)*)"|'([^'\n]*)')\s*(?:#.*)?$"""
)
_WANTED = {
    "project": ("name",),
    "tool.dg": ("directory_type",),
    "tool.dg.project": ("root_module", "code_location_name", "defs_module"),
    "tool.dagster": ("module_name", "code_location_name"),
}


def read_tables(text: str) -> dict[str, dict[str, str]]:
    """The plain string values of the keys LineagePilot needs, by table. Anything else, such as an
    array, an inline table or a multi-line string, is passed over."""
    found: dict[str, dict[str, str]] = {}
    table: str | None = None
    for line in text.splitlines():
        header = _TABLE.match(line)
        if header:
            table = header.group(1).replace('"', "").replace("'", "").replace(" ", "")
            if table in _WANTED:
                found.setdefault(table, {})
            continue
        if table not in _WANTED:
            continue
        pair = _STRING.match(line)
        if pair and pair.group(1) in _WANTED[table]:
            value = pair.group(2) if pair.group(2) is not None else pair.group(3)
            if value and len(value) <= MAX_NAME and "\\" not in value:
                found[table][pair.group(1)] = value
    return found


@dataclass
class DgProject:
    """A dg project: a folder whose ``pyproject.toml`` has ``[tool.dg]``."""

    folder: str  # real path
    pyproject: str  # real path
    name: str | None
    code_location_name: str | None
    root_module: str | None
    defs_module: str | None

    @property
    def location(self) -> str | None:
        """The code location's name, as dg gives it; None if the pyproject names neither."""
        return self.code_location_name or self.name

    def location_in(self, space: str) -> str:
        """The code location's name, or, when the pyproject gives none, the root module or the
        project's folder."""
        return self.location or self.root_module or to_posix(os.path.relpath(self.folder, space))

    def defs_folder(self, space: str) -> str | None:
        """The folder that holds the project's definitions: its defs module's folder, under
        ``src/`` or not, if it is in the workspace."""
        module = self.defs_module or (
            f"{self.root_module}.{DEFAULT_DEFS}" if self.root_module else None
        )
        if not module or not all(part.isidentifier() for part in module.split(".")):
            return None
        relative = os.path.join(*module.split("."))
        for base in (os.path.join(self.folder, "src"), self.folder):
            folder = resolve_inside(os.path.join(base, relative), space)
            if folder is not None and os.path.isdir(folder):
                return folder
        return None


@dataclass
class ToolDagster:
    """``[tool.dagster]`` in a ``pyproject.toml``: the module ``dagster dev`` loads from there,
    and the code location's name."""

    folder: str  # real path
    module_name: str | None
    code_location_name: str | None


def read_dg_project(pyproject: str, space: str) -> DgProject | None:
    """The dg project a ``pyproject.toml`` describes, or None if it does not describe one."""
    return read_pyproject(pyproject, space)[0]


def read_pyproject(pyproject: str, space: str) -> tuple[DgProject | None, ToolDagster | None]:
    """The dg project and the ``[tool.dagster]`` table a ``pyproject.toml`` has, each or None."""
    data = read_capped(pyproject, FILE_SIZE_CAP, space)
    if data is None:
        return None, None
    tables = read_tables(data.decode("utf-8", "replace"))
    real = os.path.realpath(pyproject)
    tool = tables.get("tool.dagster")
    dagster = (
        ToolDagster(os.path.dirname(real), tool.get("module_name"), tool.get("code_location_name"))
        if tool
        else None
    )
    if "tool.dg" not in tables and "tool.dg.project" not in tables:
        return None, dagster
    dg = tables.get("tool.dg.project", {})
    project = DgProject(
        folder=os.path.dirname(real),
        pyproject=real,
        name=tables.get("project", {}).get("name"),
        code_location_name=dg.get("code_location_name"),
        root_module=dg.get("root_module"),
        defs_module=dg.get("defs_module"),
    )
    return project, dagster


def project_of(path: str, projects: list[DgProject]) -> DgProject | None:
    """The innermost dg project whose folder holds ``path``, or is it."""
    real = os.path.realpath(path)
    holding = [p for p in projects if real == p.folder or real.startswith(p.folder + os.sep)]
    return max(holding, key=lambda p: len(p.folder), default=None)
