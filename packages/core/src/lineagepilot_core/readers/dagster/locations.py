"""Which code location each Python definition is in (task 8.4; mapping note, design_next_phase 7.1).

A definition in Python is in the code location of what loads it. That is its dg project, when one
holds its file. Otherwise it is the ``Definitions(…)`` call whose assets list it, by name or
through ``load_assets_from_modules``. The location is named after what loads that call's file,
by the first of these that applies:

1. the dg project that holds it (``projects``);
2. an entry of a ``workspace.yaml`` (``load_from``) or of a ``dagster_cloud.yaml``
   (``locations``) that loads the file, or the package it is in. The name is the entry's
   ``location_name``. When an entry gives none, it is the name Dagster 1.13.29 gives it: the
   package or the module as written, or the file's base name, with ``:attribute`` after it when
   the entry names one;
3. ``[tool.dagster]`` in a ``pyproject.toml`` whose ``module_name`` is the file or its package:
   ``code_location_name``, or else ``module_name``;
4. the module the ``Definitions(…)`` call is in, as a dotted path from the workspace (D-d).

A definition that no ``Definitions(…)`` call is found to load, nor any entry, is put in a location
named after its own file, and the reader's summary says how many were.

The YAML files are untrusted: each is read with a size cap and parsed by ``safe_yaml`` within a
budget. A module name is found as a file inside the workspace only, never imported.
"""

from __future__ import annotations

import os
import posixpath
from dataclasses import dataclass
from typing import Any

import yaml

from lineagepilot_core.parsers import safe_yaml
from lineagepilot_core.readers.common.safe_io import (
    read_capped,
    regular_file_inside,
    resolve_inside,
    to_posix,
)
from lineagepilot_core.readers.dagster.projects import (
    MAX_NAME,
    DgProject,
    ToolDagster,
    project_of,
)

FILE_SIZE_CAP = 2 * 1024 * 1024
# A workspace.yaml entry's kinds, and the key each names its target by
_KEYS = {
    "python_file": "relative_path",
    "python_module": "module_name",
    "python_package": "package_name",
}
# A dagster_cloud.yaml code_source's keys, and the kind of entry each is
_CODE_SOURCE = {
    "python_file": "python_file",
    "module_name": "python_module",
    "package_name": "python_package",
}
# A workspace file with more entries than this is not one anybody wrote
MAX_ENTRIES = 1_000
NO_LOADER = "no Definitions call the reader found loads this definition"


@dataclass
class Entry:
    """What a workspace file's entry, or a ``[tool.dagster]`` table, loads."""

    location: str
    target: str  # the real path of the module's file; a package's is its __init__.py
    source: str  # where the entry is, relative to the workspace

    @property
    def package(self) -> str | None:
        """The folder of the package it loads, if it loads one."""
        if os.path.basename(self.target) == "__init__.py":
            return os.path.dirname(self.target)
        return None

    def loads(self, real: str) -> bool:
        """Whether this entry loads the file ``real``: it is the file, or in the package."""
        package = self.package
        return real == self.target or (package is not None and real.startswith(package + os.sep))


def _name(value: Any) -> str | None:
    if isinstance(value, str) and value.strip() and len(value.strip()) <= MAX_NAME:
        return value.strip()
    return None


def _module_file(module: str, bases: list[str], space: str) -> str | None:
    """The file of a dotted module name, found below one of ``bases`` (and their ``src/``)."""
    parts = module.split(".")
    if not all(part.isidentifier() for part in parts):
        return None
    for base in bases:
        for root in (base, os.path.join(base, "src")):
            stem = os.path.join(root, *parts)
            for path in (stem + ".py", os.path.join(stem, "__init__.py")):
                real = regular_file_inside(path, space)
                if real is not None:
                    return real
    return None


def _folder(base: str, value: Any, space: str) -> str | None:
    """A folder written in a workspace file, relative to that file's folder, if it is inside the
    workspace."""
    if not isinstance(value, str) or not value.strip():
        return None
    return resolve_inside(os.path.normpath(os.path.join(base, value.strip())), space)


def _entry(
    kind: str, config: Any, base: str, space: str, source: str, location: str | None = None
) -> Entry | None:
    """One ``python_file``, ``python_module`` or ``python_package`` entry, or a ``code_source``."""
    if isinstance(config, str):
        config = {_KEYS[kind]: config}
    if not isinstance(config, dict):
        return None
    location = location or _name(config.get("location_name"))
    attribute = _name(config.get("attribute"))
    working = _folder(base, config.get("working_directory"), space) or base
    if kind == "python_file":
        written = _name(config.get("relative_path"))
        if written is None:
            return None
        target = regular_file_inside(os.path.normpath(os.path.join(base, written)), space)
        default = posixpath.basename(written.replace("\\", "/"))
    else:
        key = "module_name" if kind == "python_module" else "package_name"
        written = _name(config.get(key))
        if written is None:
            return None
        target = _module_file(written, [working], space)
        default = written
    if target is None:
        return None
    if location is None:
        location = f"{default}:{attribute}" if attribute else default
    return Entry(location[:MAX_NAME], target, source)


def read_entries(
    space: str, files: list[str], tools: list[ToolDagster]
) -> tuple[list[Entry], list[str]]:
    """The entries of the workspace files (relative to ``space``), then those of the
    ``[tool.dagster]`` tables, in that order; and problems reading them."""
    entries: list[Entry] = []
    problems: list[str] = []
    for relative in files:
        full = os.path.join(space, relative)
        data = read_capped(full, FILE_SIZE_CAP, space)
        if data is None:
            problems.append(f"{relative} is over {FILE_SIZE_CAP // 2**20} MB, or not a file")
            continue
        try:
            document = safe_yaml.load(data)
        except yaml.YAMLError as exc:
            first = str(exc).splitlines()[0][:200] if str(exc) else type(exc).__name__
            problems.append(f"{relative} could not be read as YAML: {first}")
            continue
        if not isinstance(document, dict):
            continue
        base = os.path.dirname(os.path.realpath(full))
        if os.path.basename(relative) == "dagster_cloud.yaml":
            entries += _cloud_entries(document, base, space, relative)
            continue
        items = document.get("load_from")
        for item in items[:MAX_ENTRIES] if isinstance(items, list) else []:
            if isinstance(item, dict) and len(item) == 1:
                ((kind, config),) = item.items()
                if kind in _KEYS:
                    found = _entry(kind, config, base, space, relative)
                    if found is not None:
                        entries.append(found)
    for tool in tools:
        module = _name(tool.module_name)
        target = _module_file(module, [tool.folder], space) if module else None
        if module and target is not None:
            location = _name(tool.code_location_name) or module
            source = to_posix(os.path.relpath(os.path.join(tool.folder, "pyproject.toml"), space))
            entries.append(Entry(location[:MAX_NAME], target, source))
    return entries, problems


def _cloud_entries(document: dict[str, Any], base: str, space: str, source: str) -> list[Entry]:
    """``dagster_cloud.yaml``: ``locations``, each with a ``location_name`` and a
    ``code_source`` that names a package, a module or a file."""
    entries: list[Entry] = []
    items = document.get("locations")
    for item in items[:MAX_ENTRIES] if isinstance(items, list) else []:
        if not isinstance(item, dict) or not isinstance(item.get("code_source"), dict):
            continue
        name = _name(item.get("location_name"))
        working = _folder(base, item.get("working_directory"), space) or base
        for key, value in item["code_source"].items():
            kind = _CODE_SOURCE.get(key)
            if kind is None:
                continue
            config = {_KEYS[kind]: value, "working_directory": working}
            # A file in code_source is relative to the working directory
            where = working if kind == "python_file" else base
            found = _entry(kind, config, where, space, source, name)
            if found is not None:
                entries.append(found)
                break
    return entries


def dotted(relative: str) -> str:
    """A file as a dotted module path from the workspace (D-d)."""
    stem = relative[:-3] if relative.endswith(".py") else relative
    if stem == "__init__" or stem.endswith("/__init__"):
        stem = stem[: -len("__init__")].rstrip("/")
    return stem.replace("/", ".") or "__init__"


def assign(
    records: list[dict[str, Any]],
    loads: list[dict[str, Any]],
    projects: list[DgProject],
    entries: list[Entry],
    space: str,
) -> int:
    """Give each record from the Python files its ``location``, and a note on how it was found.
    Returns how many were put in a location named after their own file."""
    by_file = sorted(loads, key=lambda load: (load["file"], load["line"]))
    unloaded = 0
    for record in records:
        module = record["module"]
        dg = project_of(os.path.join(space, module), projects)
        if dg is not None:
            record["location"] = dg.location_in(space)
            continue
        loaders = [load for load in by_file if _includes(load, record)]
        if loaders:
            first = loaders[0]
            record["location"] = location_of(first["file"], projects, entries, space)
            if len({load["file"] for load in loaders}) > 1:
                record["notes"].append(
                    f"loaded by Definitions calls in {len({x['file'] for x in loaders})} files; "
                    f"its code location is that of the first, in {first['file']}"
                )
            continue
        entry = _entry_for(module, entries, space)
        if entry is not None:
            record["location"] = entry.location
            continue
        record["location"] = dotted(module)[:MAX_NAME]
        record["notes"].append(f"{NO_LOADER}: its code location is named after its file")
        unloaded += 1
    return unloaded


def _includes(load: dict[str, Any], record: dict[str, Any]) -> bool:
    module = record["module"]
    return (
        record["key"] in load["keys"]
        or module in load["modules"]
        or any(module.startswith(package + "/") for package in load["packages"])
    )


def _entry_for(relative: str, entries: list[Entry], space: str) -> Entry | None:
    real = os.path.realpath(os.path.join(space, relative))
    return next((entry for entry in entries if entry.loads(real)), None)


def location_of(relative: str, projects: list[DgProject], entries: list[Entry], space: str) -> str:
    """The name of the code location that loads the file ``relative``: rules 1 to 4 above."""
    dg = project_of(os.path.join(space, relative), projects)
    if dg is not None:
        return dg.location_in(space)
    entry = _entry_for(relative, entries, space)
    if entry is not None:
        return entry.location
    return dotted(relative)[:MAX_NAME]
