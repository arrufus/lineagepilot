"""One read of a workspace's Dagster definitions, for one dbt project, as JSON (tasks 8.2 to 8.7).

It runs in the reader's child process, with its time and memory limits (``readers.contract``).
Discovery is task 8.2, a dg project's dbt components task 8.3, and dbt asset definitions in Python
task 8.4, with the code location each is in. The asset jobs and schedules that run them are task
8.6, and the non-dbt assets that read them task 8.7. Paths in the result are relative to the
workspace; the linker makes them relative to the project.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from lineagepilot_core.graph.model import DAGSTER
from lineagepilot_core.parsers.data_flow_annotations import DataFlowAnnotationLoader
from lineagepilot_core.readers.common.evaluator import scrub
from lineagepilot_core.readers.common.projects import ProjectIndex
from lineagepilot_core.readers.common.safe_io import to_posix
from lineagepilot_core.readers.common.workspace import load_annotations, project_and_workspace
from lineagepilot_core.readers.dagster.components import read_components
from lineagepilot_core.readers.dagster.discovery import DEFAULT_PATHS, discover
from lineagepilot_core.readers.dagster.downstream import resolve as resolve_downstream
from lineagepilot_core.readers.dagster.jobs import resolve as resolve_jobs
from lineagepilot_core.readers.dagster.locations import (
    NO_LOADER,
    Entry,
    assign,
    location_of,
    read_entries,
)
from lineagepilot_core.readers.dagster.projects import DgProject, ToolDagster, read_pyproject
from lineagepilot_core.readers.dagster.python import read_python

# What a record carries only for the reader's own use
_WORKING_KEYS = (
    "module",
    "key",
    "dynamic",
    "selection",
    "job_key",
    "kind",
    "deps",
    "groups_unknown",
)


def scan_dagster(
    project_root: str | Path,
    workspace: str | Path | None = None,
    paths: Iterable[Any] = DEFAULT_PATHS,
    manifest: str | Path | None = None,
) -> dict[str, Any]:
    """The Dagster definitions for this project, as JSON: read them with ``link_dagster``.
    Nothing in them is imported or run."""
    root, space = project_and_workspace(project_root, workspace)
    found = discover(space, paths)
    definitions: list[dict[str, Any]] = []
    unknowns: list[dict[str, Any]] = []
    jobs: list[dict[str, Any]] = []
    schedules: list[dict[str, Any]] = []
    downstream: list[dict[str, Any]] = []
    located: list[dict[str, Any]] = []
    hashes: dict[str, str] = {}
    problems = list(found.problems)
    if found.components or found.python:
        dg_projects: list[DgProject] = []
        tools: list[ToolDagster] = []
        for relative in found.projects:
            dg, tool = read_pyproject(os.path.join(space, relative), space)
            dg_projects += [dg] if dg is not None else []
            tools += [tool] if tool is not None else []
        index = ProjectIndex(
            space, manifests={root: str(manifest)} if manifest else None, focus=root
        )
        records: list[dict[str, Any]] = []
        read: list[str] = []
        if found.components:
            done, open_, more_hashes, more = read_components(
                space, root, found.components, dg_projects, index
            )
            records += done + open_
            hashes.update(more_hashes)
            read += more
        for record in records:
            # A component is named in no Python selection, but jobs select its assets by key
            record.setdefault("key", f"{record['file']}:{record['line']}:component")
        if found.python:
            python = read_python(space, root, found.python, index)
            entries, unread = read_entries(space, found.workspaces, tools)
            assign(
                python.definitions + python.jobs + python.schedules + python.assets,
                python.loads,
                dg_projects,
                entries,
                space,
            )
            records += python.definitions
            choice = index.manifest_for(root) if python.jobs or python.schedules else None
            manifest_of = choice.manifest if choice else None
            jobs, schedules = resolve_jobs(python.jobs, python.schedules, records, manifest_of)
            if python.assets and choice is None:
                manifest_of = index.manifest_for(root).manifest
            downstream = resolve_downstream(python.assets, records, manifest_of)
            located = _locate(python.located, python.assets, dg_projects, entries, space)
            unloaded = sum(
                any(note.startswith(NO_LOADER) for note in r["notes"])
                for r in python.definitions + jobs + schedules + downstream
            )
            if unloaded:
                read.append(
                    f"{unloaded} Dagster definition{'s are' if unloaded > 1 else ' is'} loaded "
                    "by no Definitions call the reader found; each is in a code location named "
                    "after its file"
                )
            hashes.update(python.hashes)
            read += python.problems + unread
        for record in records + jobs + schedules + downstream + located:
            for key in _WORKING_KEYS:
                record.pop(key, None)
        for record in records:
            (definitions if record["status"] == "resolved" else unknowns).append(record)
        problems += index.problems + read
    result = {
        "tool": DAGSTER,
        "project": to_posix(os.path.relpath(root, space)),
        "paths": found.paths,
        "files": found.to_json(),
        "hashes": hashes,
        "definitions": definitions,
        "unknowns": unknowns,
        "jobs": jobs,
        "schedules": schedules,
        "assets": downstream,
        "located": located,
        "annotations": _annotations(root, space),
        "problems": problems,
    }
    # Where the workspace is on this machine is no part of the result: Path(__file__) values
    # reach the notes and reasons
    return scrub(result, to_posix(space))


def _locate(
    located: list[dict[str, Any]],
    assets: list[dict[str, Any]],
    projects: list[DgProject],
    entries: list[Entry],
    space: str,
) -> list[dict[str, Any]]:
    """The code location of each unknown about a location or an asset (task 8.8): the asset's,
    or else that of the file it is in, as a Definitions call in it would be named."""
    of_asset = {(a["key"], a["name"]): a.get("location") for a in assets}
    for record in located:
        found = (
            of_asset.get((record["key"], record["name"])) if record.get("of") == "asset" else None
        )
        record["location"] = found or location_of(record["module"], projects, entries, space)
    return located


def _annotations(root: str, space: str) -> dict[str, Any]:
    """The annotation file's Dagster tasks, and what it acknowledges (task 8.8). The file is read
    with the dbt project's, as the DAG scan reads it; its problems are the DAG scan's to report."""
    loader = DataFlowAnnotationLoader()
    file = load_annotations(loader, root, space)
    return {
        "file": file,
        "tasks": [a.to_json() for a in loader.for_tool(DAGSTER)],
        "acknowledged": loader.acknowledged,
    }
