"""A dg project's dbt components: ``DbtProjectComponent`` in ``defs.yaml`` (task 8.3).

A ``defs.yaml`` holds one component per YAML document. For ``type: dagster_dbt.DbtProjectComponent``
this reads, as dagster-dbt 0.29.29's component has them:

- ``attributes.project``: the dbt project, as a path relative to the component's folder, with
  ``{{ project_root }}`` for the dg project's folder; or a mapping with ``project_dir``. The git
  form, which fetches the project, is not read: it is ``remote_manifest``.
- ``attributes.select``, ``exclude`` and ``selector``: dbt's selection, by default ``fqn:*``, run
  over the project's manifest by the shared selector engine (``selection``, as for a Python
  definition). The component makes an asset of each model, seed and snapshot it selects.
- ``attributes.translation``: its ``key`` template and its ``key_prefix``, which set the asset
  keys (``keys``, task 8.5).

Any other template is filled in only when Dagster loads the component, so a value that holds one
is an unknown, at the component's line, and so is a project path that matches no dbt project.
The file is untrusted: it is read with a size cap, and parsed by ``safe_yaml`` within a budget.
"""

from __future__ import annotations

import hashlib
import os
import posixpath
import re
from dataclasses import dataclass
from typing import Any

import yaml

from lineagepilot_core.parsers import safe_yaml
from lineagepilot_core.readers.common.categories import UnknownCategory
from lineagepilot_core.readers.common.projects import ProjectIndex
from lineagepilot_core.readers.common.safe_io import read_capped, to_posix
from lineagepilot_core.readers.dagster.keys import (
    DEFAULT,
    KeyRule,
    check_template,
    prefix_of,
)
from lineagepilot_core.readers.dagster.projects import MAX_NAME, DgProject, project_of
from lineagepilot_core.readers.dagster.selection import (
    DEFAULT_SELECT,
    MAX_SELECTION,
    match_project,
    select_nodes,
)
from lineagepilot_core.readers.dagster.selection import unknown as _unknown

COMPONENT_TYPE = "dagster_dbt.DbtProjectComponent"
API = "DbtProjectComponent"
FILE_SIZE_CAP = 2 * 1024 * 1024
# A key template this long is not one anybody wrote
MAX_KEY_TEMPLATE = 1000
# {{ project_root }}, the only template variable this fills in, with Dagster's spacing rules
_PROJECT_ROOT = re.compile(r"\{\{\s*(?:context\.)?project_root\s*\}\}")
_TEMPLATE = "{{"


@dataclass
class Component:
    """Where a component is: its file, relative to the workspace, the line its document starts
    on, its code location, and its name there."""

    file: str
    line: int
    location: str
    name: str
    dg: DgProject | None


def read_components(
    space: str,
    root: str,
    files: list[str],
    projects: list[DgProject],
    index: ProjectIndex,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str], list[str]]:
    """The dbt components in ``files`` (``defs.yaml``, relative to ``space``) that are for the
    dbt project at ``root``: each a definition record, or an unknown record. Also each file's
    SHA-256, for the evidence, and problems that are about no one component."""
    definitions: list[dict[str, Any]] = []
    unknowns: list[dict[str, Any]] = []
    hashes: dict[str, str] = {}
    problems: list[str] = []
    for relative in files:
        full = os.path.join(space, relative)
        data = read_capped(full, FILE_SIZE_CAP, space)
        if data is None:
            problems.append(
                f"{relative} is over {FILE_SIZE_CAP // 2**20} MB, or not a regular file"
            )
            continue
        hashes[relative] = hashlib.sha256(data).hexdigest()
        try:
            documents = safe_yaml.load_documents(data)
        except yaml.YAMLError as exc:
            problems.append(f"{relative} could not be read as YAML: {_first_line(exc)}")
            continue
        for position, (document, line) in enumerate(documents):
            if not isinstance(document, dict) or document.get("type") != COMPONENT_TYPE:
                continue
            component = _where(space, relative, line, projects)
            if len(documents) > 1:
                # Several components in one file: Dagster tells them apart by their place in it
                component.name = f"{component.name}[{position}]"
            record = _read(component, document.get("attributes"), space, root, index)
            if record is None:
                continue
            (definitions if record["status"] == "resolved" else unknowns).append(record)
    return definitions, unknowns, hashes, problems


def _first_line(exc: Exception) -> str:
    return str(exc).splitlines()[0][:200] if str(exc) else type(exc).__name__


def _where(space: str, relative: str, line: int, projects: list[DgProject]) -> Component:
    """The component's code location and name: its folder under the dg project's defs folder.
    A component in no dg project is named after its folder (D-d)."""
    folder = os.path.dirname(os.path.join(space, relative))
    dg = project_of(folder, projects)
    if dg is not None:
        defs = dg.defs_folder(space)
        base = defs if defs is not None and _within(folder, defs) else dg.folder
        name = to_posix(os.path.relpath(folder, base))
        name = name if name != "." else DEFAULT_NAME
        return Component(relative, line, dg.location_in(space), name, dg)
    where = to_posix(os.path.relpath(folder, space))
    return Component(relative, line, where, posixpath.basename(where) or DEFAULT_NAME, None)


DEFAULT_NAME = "defs"


def _within(path: str, folder: str) -> bool:
    real, base = os.path.realpath(path), os.path.realpath(folder)
    return real == base or real.startswith(base + os.sep)


def _read(
    component: Component, attributes: Any, space: str, root: str, index: ProjectIndex
) -> dict[str, Any] | None:
    """One component's record, or None if it is for another dbt project in the workspace."""
    record: dict[str, Any] = {
        "api": API,
        "file": component.file,
        "line": component.line,
        "location": component.location,
        "name": component.name,
        "in_dg_project": component.dg is not None,
        "notes": [],
    }
    if len(component.location) > MAX_NAME or len(component.name) > MAX_NAME:
        return _unknown(record, UnknownCategory.LIMIT, "the component's name is too long to show")
    if not isinstance(attributes, dict):
        return _unknown(record, UnknownCategory.NOT_LITERAL, "the component has no attributes")
    project = attributes.get("project")
    raw = _project_path(project)
    if raw is None:
        if isinstance(project, dict) and any(k in project for k in ("repo_url", "url", "git")):
            return _unknown(
                record,
                UnknownCategory.REMOTE_MANIFEST,
                "the dbt project is fetched from a git repository when Dagster loads it",
            )
        return _unknown(record, UnknownCategory.NOT_LITERAL, "the component names no dbt project")
    if _PROJECT_ROOT.search(raw):
        if component.dg is None:
            return _unknown(
                record,
                UnknownCategory.JINJA,
                f"{{{{ project_root }}}} in {raw!r} needs a dg project, and none holds it",
            )
        root_folder = component.dg.folder.replace("\\", "/")
        raw = _PROJECT_ROOT.sub(lambda _: root_folder, raw)
    if _TEMPLATE in raw:
        return _unknown(record, UnknownCategory.JINJA, f"the project path {raw!r} holds a template")
    folder = to_posix(os.path.dirname(os.path.join(space, component.file)))
    match = match_project(record, raw, folder, index, root)
    if match is None:
        # another project's, which its own language server links, or an unknown
        return record if record["status"] == "unknown" else None
    selection: dict[str, str] = {}
    for key, default in (("select", DEFAULT_SELECT), ("exclude", ""), ("selector", "")):
        value = attributes.get(key, default)
        if value is None:
            value = default
        if not isinstance(value, str) or len(value) > MAX_SELECTION:
            return _unknown(record, UnknownCategory.NOT_LITERAL, f"{key} is not a string")
        if _TEMPLATE in value:
            return _unknown(record, UnknownCategory.JINJA, f"{key} {value!r} holds a template")
        selection[key] = value.strip()
    record.update(selection)
    rule = key_rule(attributes.get("translation"))
    return select_nodes(record, match, selection, index, rule=rule)


def _project_path(project: Any) -> str | None:
    if isinstance(project, str) and project.strip():
        return project.strip()
    if isinstance(project, dict) and isinstance(project.get("project_dir"), str):
        return project["project_dir"].strip() or None
    return None


def key_rule(translation: Any) -> KeyRule:
    """What sets the component's asset keys: its ``translation``'s ``key`` and ``key_prefix``,
    which apply to dagster-dbt's default key, or something that cannot be read."""
    if translation is None:
        return DEFAULT
    if not isinstance(translation, dict):
        return KeyRule(unknown="the translation is set by code Dagster runs when it loads")
    template = translation.get("key")
    group = translation.get("group_name")
    if group is not None:
        if not isinstance(group, str) or len(group) > MAX_KEY_TEMPLATE or check_template(group):
            return KeyRule(
                unknown="the translation's group_name is not a template the reader can read"
            )
    prefix: tuple[str, ...] = ()
    if "key_prefix" in translation:
        found = prefix_of(translation["key_prefix"])
        if found is None or any(_TEMPLATE in part for part in found):
            return KeyRule(unknown="the translation's key_prefix is not a literal string or list")
        prefix = found
    if template is None:
        return KeyRule(prefix=prefix, group=group)
    if not isinstance(template, str) or len(template) > MAX_KEY_TEMPLATE:
        return KeyRule(unknown="the translation's key is not a template the reader can read")
    why = check_template(template)
    if why is not None:
        return KeyRule(unknown=why)
    return KeyRule(template=template, prefix=prefix, group=group)
