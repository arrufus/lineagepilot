"""Helpers for the Airflow scanner's tests. The fixtures are in fixtures/airflow: DAG files, one per
pattern, and dbt projects written for spike S3 (see that spike's README)."""

from __future__ import annotations

import os
from pathlib import Path

from lineagepilot_core.airflow.dag_static import scan_dags

FIXTURES = Path(__file__).parent / "fixtures" / "airflow"
SHOP = FIXTURES / "dbt" / "shop"
MINI = FIXTURES / "dbt" / "mini"


def scan(paths: list[str], workspace: str | None = None, safe_mode: bool = True, **options):
    """Scan these files or folders, by absolute path."""
    first = paths[0]
    root = workspace or (first if os.path.isdir(first) else os.path.dirname(first))
    root = os.path.realpath(root)
    folders = [Path(os.path.relpath(os.path.realpath(p), root)).as_posix() for p in paths]
    return scan_dags(root, folders, safe_mode=safe_mode, **options)


def find_link(result, dag_id, name=None, command_index=None, file=None):
    """The link for a DAG and a task id or (prefixed) group id; None means the DbtDag itself."""
    out = []
    for link in result["links"]:
        if link["dag_id"] != dag_id:
            continue
        if file and not link["location"]["file"].endswith(file):
            continue
        ident = link.get("group_id") or link.get("task_id")
        if ident != name:
            continue
        if command_index is not None and link.get("command_index", 0) != command_index:
            continue
        out.append(link)
    assert len(out) == 1, f"expected one link for {dag_id}/{name}, got {len(out)}"
    return out[0]


def short(ids):
    return sorted(i.split(".", 2)[2] for i in ids)
