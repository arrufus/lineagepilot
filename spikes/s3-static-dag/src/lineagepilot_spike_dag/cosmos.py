"""Astronomer Cosmos: read DbtDag/DbtTaskGroup configuration and predict rendered task ids.

The task-id rules come from reading Cosmos source as text (``cosmos/airflow/graph.py`` at
astronomer-cosmos c92d0b23): ``_get_task_id_and_args``, ``create_task_metadata``,
``generate_task_or_group``, ``_add_test_tasks`` and ``create_task_groups_based_on_folder``.
Defaults come from ``cosmos/config.py`` (``RenderConfig``) and ``cosmos/airflow/task_group.py``.
"""

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass, field
from typing import Any

from .manifest import Manifest, Node
from .values import CallV, Const, DictV, ListV, PathV, Sym, Unknown, Val, as_str

COSMOS_OPERATOR_RE = re.compile(
    r"^Dbt(?P<cmd>RunOperation|SemanticMetricView|ProducerWatcher|Build|Run|Test|Seed|Snapshot|Source|Clone|"
    r"Compile|Deps|Docs|LS|Warning)(?P<rest>\w*)Operator$"
)
OPERATOR_COMMANDS = {
    "Build": "build", "Run": "run", "Test": "test", "Seed": "seed", "Snapshot": "snapshot",
    "Source": "source freshness", "RunOperation": "run-operation", "Clone": "clone", "Compile": "compile",
    "Deps": "deps", "Docs": "docs generate", "LS": "ls", "SemanticMetricView": "run",
    "ProducerWatcher": "build", "Warning": "warning",
}
DEFAULT_TASK_GROUP_ID = "dbt_task_group"  # cosmos/airflow/task_group.py
TESTABLE = {"model", "seed", "snapshot"}
SUFFIX = {"model": "run", "seed": "seed", "snapshot": "snapshot"}
WATCHER_MODES = {"watcher", "watcher_kubernetes", "watcher_gcp_gke"}


def enum_value(v: Val | None) -> str | None:
    """``TestBehavior.BUILD`` / ``"build"`` -> ``"build"``."""
    if v is None:
        return None
    if isinstance(v, Sym):
        return v.name.rsplit(".", 1)[-1].lower()
    s = as_str(v)
    return s.lower() if s is not None else None


def call_named(v: Val | None, *names: str) -> CallV | None:
    if isinstance(v, CallV) and v.func.rsplit(".", 1)[-1] in names:
        return v
    return None


@dataclass
class CosmosSpec:
    kind: str  # "dag" | "task_group"
    dag_id: Val | None = None
    group_id: Val | None = None
    render_project: Val | None = None
    render_project_source: str | None = None
    execution_project: Val | None = None
    manifest_path: Val | None = None
    project_name: Val | None = None
    select: Val | None = None
    exclude: Val | None = None
    selector: Val | None = None
    load_method: str | None = None
    test_behavior: str = "after_each"
    dbt_ls_path: Val | None = None
    group_nodes_by_folder: bool = False
    normalize_task_id: bool = False
    source_rendering: str = "none"
    seed_rendering: str = "always"
    detach_tests: bool = False
    ephemeral_empty: bool = True
    execution_mode: str = "local"
    legacy: bool = False
    unknown: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _flag(v: Val | None, default: bool, spec: CosmosSpec, name: str) -> bool:
    if v is None:
        return default
    if isinstance(v, Const) and isinstance(v.value, bool):
        return v.value
    if isinstance(v, Const) and v.value is None:
        return default
    spec.unknown.append(f"{name} is not a literal boolean")
    return default


def parse_cosmos(kind: str, call: CallV) -> CosmosSpec:
    spec = CosmosSpec(kind=kind)
    if kind == "dag":
        spec.dag_id = call.arg("dag_id", 0)
    else:
        spec.group_id = call.arg("group_id", 0) or Const(DEFAULT_TASK_GROUP_ID, conf="inferred",
                                                      notes=("no group_id: Cosmos default 'dbt_task_group'",))
    pc = call.kwargs.get("project_config")
    rc = call.kwargs.get("render_config")
    ec = call.kwargs.get("execution_config")
    for label, v, cls in (("project_config", pc, "ProjectConfig"), ("render_config", rc, "RenderConfig"),
                          ("execution_config", ec, "ExecutionConfig")):
        if v is not None and call_named(v, cls) is None:
            spec.unknown.append(f"{label} is not a literal {cls}(...) call")
    pcc = call_named(pc, "ProjectConfig")
    rcc = call_named(rc, "RenderConfig")
    ecc = call_named(ec, "ExecutionConfig")
    if pcc is not None:
        if pcc.has_star:
            spec.unknown.append("ProjectConfig uses **kwargs")
        spec.manifest_path = pcc.arg("manifest_path")
        spec.project_name = pcc.arg("project_name")
        pp = pcc.arg("dbt_project_path", 0)
        if pp is not None and not (isinstance(pp, Const) and pp.value is None):
            spec.render_project, spec.render_project_source = pp, "ProjectConfig.dbt_project_path"
    if rcc is not None:
        if rcc.has_star:
            spec.unknown.append("RenderConfig uses **kwargs")
        rp = rcc.arg("dbt_project_path")
        if rp is not None:
            spec.render_project, spec.render_project_source = rp, "RenderConfig.dbt_project_path"
        spec.select = rcc.arg("select")
        spec.exclude = rcc.arg("exclude")
        spec.selector = rcc.arg("selector")
        spec.load_method = enum_value(rcc.arg("load_method"))
        tb = rcc.arg("test_behavior")
        if tb is not None:
            spec.test_behavior = enum_value(tb) or "unknown"
        spec.dbt_ls_path = rcc.arg("dbt_ls_path")
        spec.group_nodes_by_folder = _flag(rcc.arg("group_nodes_by_folder"), False, spec, "group_nodes_by_folder")
        ntid = rcc.arg("normalize_task_id")
        spec.normalize_task_id = ntid is not None and not (isinstance(ntid, Const) and ntid.value is None)
        sr = rcc.arg("source_rendering_behavior")
        if sr is not None:
            spec.source_rendering = enum_value(sr) or "unknown"
        sd = rcc.arg("seed_rendering_behavior")
        if sd is not None:
            spec.seed_rendering = enum_value(sd) or "unknown"
        spec.detach_tests = _flag(rcc.arg("should_detach_multiple_parents_tests"), False, spec, "should_detach_multiple_parents_tests")
        spec.ephemeral_empty = _flag(rcc.arg("ephemeral_models_as_empty_operator"), True, spec, "ephemeral_models_as_empty_operator")
    if ecc is not None:
        spec.execution_project = ecc.arg("dbt_project_path")
        em = ecc.arg("execution_mode")
        if em is not None:
            spec.execution_mode = enum_value(em) or "unknown"
    # Cosmos 0.x interface: DbtDag(dbt_project_name=..., dbt_root_path=..., select={"tags": [...]})
    if pcc is None and ("dbt_project_name" in call.kwargs or "dbt_root_path" in call.kwargs):
        spec.legacy = True
        root = call.kwargs.get("dbt_root_path") or Const("/usr/local/airflow/dags/dbt", conf="heuristic",
                                                         notes=("Cosmos 0.x default dbt_root_path",))
        name = call.kwargs.get("dbt_project_name")
        if as_str(root) is not None and as_str(name) is not None:
            spec.render_project = PathV(posixpath.join(as_str(root), as_str(name)), conf="heuristic")  # type: ignore[arg-type]
            spec.render_project_source = "legacy dbt_root_path/dbt_project_name"
        spec.select = _legacy_selection(call.kwargs.get("select"), spec)
        spec.exclude = _legacy_selection(call.kwargs.get("exclude"), spec)
        spec.notes.append("Cosmos 0.x keyword interface (dbt_project_name/dbt_root_path)")
    if spec.render_project is None and spec.manifest_path is None and spec.execution_project is not None:
        spec.render_project, spec.render_project_source = spec.execution_project, "ExecutionConfig.dbt_project_path"
    return spec


def _legacy_selection(v: Val | None, spec: CosmosSpec) -> Val | None:
    if v is None or not isinstance(v, DictV):
        return v
    out: list[Val] = []
    for key, val in v.items.items():
        items = val.items if isinstance(val, ListV) else [val]
        for item in items:
            s = as_str(item)
            if s is None:
                spec.unknown.append(f"legacy select/exclude {key!r} is not literal")
                continue
            if key == "tags":
                out.append(Const(f"tag:{s}"))
            elif key == "paths":
                out.append(Const(f"path:{s}"))
            elif key == "configs":
                out.append(Const(f"config.{s}" if not s.startswith("tags:") else f"tag:{s[5:]}"))
            else:
                spec.unknown.append(f"legacy select key {key!r} not supported")
    return ListV(out, conf="heuristic")


def _resource_name(uid: str) -> str:
    return uid.split(".", 2)[2] if uid.count(".") >= 2 else uid


def node_task_name(node: Node) -> str:
    """Cosmos ``DbtNode.name``: resource name from the unique_id with '.' replaced by '_'."""
    return _resource_name(node.unique_id).replace(".", "_")


def tests_by_parent(manifest: Manifest, excluded: set[str]) -> dict[str, list[Node]]:
    out: dict[str, list[Node]] = {}
    for n in manifest.nodes.values():
        if n.resource_type != "test" or n.unique_id in excluded:
            continue
        for p in n.depends_on:
            out.setdefault(p, []).append(n)
    return out


def expand_tasks(
    manifest: Manifest,
    selected: list[str],
    spec: CosmosSpec,
    prefix: str,
    excluded_tests: set[str],
    project_name: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
    """Predict Airflow task ids for each rendered node; returns (per-node tasks, extra tasks, unknown reason)."""
    if spec.normalize_task_id:
        return [], [], "RenderConfig.normalize_task_id is a callable: task ids cannot be predicted statically"
    if spec.test_behavior not in ("after_each", "after_all", "build", "none"):
        return [], [], f"unknown test_behavior {spec.test_behavior!r}"
    tests = tests_by_parent(manifest, excluded_tests)
    selected_set = set(selected)
    rows: list[dict[str, Any]] = []
    extra: list[dict[str, Any]] = []
    detached: dict[str, Node] = {}
    for uid in selected:
        node = manifest.nodes[uid]
        if node.resource_type not in TESTABLE:
            continue
        if node.resource_type == "seed" and spec.seed_rendering == "none":
            continue
        name = node_task_name(node)
        folder = ""
        if spec.group_nodes_by_folder:
            parts = [p for p in posixpath.dirname(node.original_file_path).split("/") if p]
            folder = "".join(f"{p}." for p in parts)
        p = prefix + folder
        node_tests = [t for t in tests.get(uid, []) if any(d in selected_set for d in t.depends_on)]
        non_detached = [t for t in node_tests if not (spec.detach_tests and len(t.depends_on) > 1)]
        for t in node_tests:
            if spec.detach_tests and len(t.depends_on) > 1 and spec.test_behavior in ("after_each", "build"):
                detached[t.unique_id] = t
        suffix = SUFFIX[node.resource_type]
        operator = "dbt"
        if node.resource_type == "model" and node.materialized == "ephemeral" and spec.ephemeral_empty:
            operator = "EmptyOperator (ephemeral model)"
        if node.resource_type == "seed" and spec.seed_rendering == "render_only":
            operator = "EmptyOperator (seed render_only)"
        if spec.test_behavior == "build":
            ids = [f"{p}{name}_{node.resource_type}_build"]
        elif spec.test_behavior == "after_each" and non_detached:
            ids = [f"{p}{name}.{suffix}", f"{p}{name}.test"]
        else:
            ids = [f"{p}{name}_{suffix}"]
        rows.append({"unique_id": uid, "resource_type": node.resource_type, "task_ids": ids, "operator": operator})
    for t in detached.values():
        extra.append({"task_id": f"{prefix}{_resource_name(t.unique_id).split('.')[0]}_test", "runs": "detached multi-parent test", "unique_id": t.unique_id})
    if spec.test_behavior == "after_all" and rows:
        extra.append({"task_id": f"{prefix}{project_name}_test", "runs": "dbt test over the whole selection"})
    if spec.execution_mode in WATCHER_MODES:
        extra.append({"task_id": f"{prefix}dbt_producer_watcher", "runs": "dbt build over the whole selection (watcher producer)"})
    return rows, extra, None


def pattern_for(spec: CosmosSpec, prefix: str) -> str:
    p = prefix + ("<folder>." if spec.group_nodes_by_folder else "")
    if spec.normalize_task_id:
        return "unknown (normalize_task_id callable)"
    if spec.test_behavior == "build":
        return f"{p}<name>_<resource_type>_build"
    if spec.test_behavior == "after_each":
        return f"{p}<name>.run + {p}<name>.test (node has tests) | {p}<name>_run (no tests)"
    if spec.test_behavior == "after_all":
        return f"{p}<name>_run, then {prefix}<project>_test"
    return f"{p}<name>_run"


def unknown_for_dynamic(v: Val | None) -> str | None:
    if isinstance(v, Unknown):
        return v.reason
    return None
