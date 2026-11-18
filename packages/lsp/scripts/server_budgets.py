"""Measure the language server against the design's budgets, end to end over stdio, with every
reader turned on (design section 11; design_next_phase section 8.2, task 7.10).

Usage, from packages/lsp:

    uv run python scripts/server_budgets.py ARTIFACTS [--readers airflow,dbt_semantic] [--check]

ARTIFACTS is a folder with a manifest.json and a catalog.json. The 5,000-model synthetic project
comes from spike S2 (spikes/s2-parse-perf, scripts/gen_project.py and build_manifests.py).

A throwaway dbt project points at the artifacts, with every node's files. Each reader in READERS
then gives the project a heavy workload of its own, and says what to measure about it:

- airflow: 51 DAG files: a Cosmos DAG over every model, and 2,000 shell tasks, each with its own
  selector. Measured: when the scan is linked, from process start, and what it found.
- dbt_semantic: a semantic layer in the manifest, in the shape dbt writes it: a semantic model on
  every mart, with an entity, dimensions and measures from its columns (one dimension an
  expression, for sqlglot to read); a metric for each measure, the first filtered on that
  dimension; a ratio for each pair of marts; and a saved query, with an export, for each ten.
  Measured: what it adds to the graph, and how long reading and linking it take, in this process.

The server is started cold (empty cache) and then warm, and timed from process start to each
stage. On each, CodeLens and impact requests are timed; Change Impact Reviews of 1, 10 and 100
changed models (staging models first, which have the most downstream), and one against a
baseline manifest, the same manifest here, which costs a read in a child process the first time;
and the server's peak memory is read from the OS (current, not peak, on macOS).

The budgets checked (BUDGETS): warm start within 1 s; CodeLens within 100 ms at p95; impact, and a
review of 100 changed models with every reader on, within 300 ms; the server within 500 MB. With
--check, the script exits 1 if one is missed. A phase that adds a reader adds it to READERS.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import shutil
import statistics
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from lsprotocol import types
from pygls.lsp.client import BaseLanguageClient
from pygls.protocol.json_rpc import JsonRPCResponseMessage

from lineagepilot_lsp.server import converter


def client_converter():
    c = converter()
    c.register_structure_hook(JsonRPCResponseMessage, lambda obj, cls: cls(**obj))
    return c


def peak_rss_mb(pid: int) -> float | None:
    """The process's peak resident memory (Linux and macOS), read before it exits."""
    status = Path(f"/proc/{pid}/status")
    if status.exists():
        for line in status.read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) / 1024
    try:
        import subprocess

        out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
        return int(out.stdout.strip()) / 1024  # current, not peak, on macOS
    except (ValueError, OSError):
        return None


# The readers, each with a workload and what to measure


def prepare_airflow(project: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    """DAG files, as a project with Airflow has: a Cosmos DAG over every model, and shell tasks."""
    dags = project / "dags"
    dags.mkdir()
    (dags / "everything.py").write_text(
        "from cosmos import DbtDag, ProjectConfig\n"
        "# airflow dag\n"
        'DbtDag(dag_id="everything", '
        'project_config=ProjectConfig("/usr/local/airflow/project"))\n'
    )
    names = sorted(n["name"] for n in manifest["nodes"].values() if n["resource_type"] == "model")
    for number in range(50):
        (dags / f"shell_{number}.py").write_text(
            "from airflow import DAG\nfrom airflow.operators.bash import BashOperator\n"
            f'with DAG("shell_{number}") as dag:\n'
            + "".join(
                f'    BashOperator(task_id="t{i}", bash_command="dbt run --select '
                f'{names[(number * 40 + i) % len(names)]}+")\n'
                for i in range(40)
            )
        )
    return {"dag_files": 51, "shell_tasks": 2000, "cosmos_dags": 1}


_NUMERIC = ("INT", "DOUBLE", "DECIMAL", "FLOAT", "NUMERIC", "REAL")
_TIME = ("DATE", "TIMESTAMP")


def prepare_semantic(project: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    """A semantic layer on every mart, added to the manifest as dbt writes one."""
    catalog = json.loads((project / "target" / "catalog.json").read_text())["nodes"]
    package = manifest["metadata"].get("project_name") or "synthetic"
    semantic_models: dict[str, Any] = {}
    metrics: dict[str, Any] = {}
    queries: dict[str, Any] = {}
    marts = sorted(
        uid
        for uid, n in manifest["nodes"].items()
        if n["resource_type"] == "model" and "/marts/" in f"/{n.get('original_file_path', '')}"
    )
    measures_of: list[tuple[str, str]] = []  # (metric uid, semantic model uid), one per mart
    for uid in marts:
        node = manifest["nodes"][uid]
        types_of = {
            c.lower(): str(info.get("type") or "").upper()
            for c, info in catalog.get(uid, {}).get("columns", {}).items()
        }
        columns = [c for c in node.get("columns", {}) if c.lower() in types_of]
        keys = [c for c in columns if c.endswith(("_id", "_key"))]
        numbers = [c for c in columns if types_of[c.lower()].startswith(_NUMERIC) and c not in keys]
        times = [c for c in columns if types_of[c.lower()].startswith(_TIME)]
        texts = [c for c in columns if types_of[c.lower()].startswith("VARCHAR")]
        if not keys or not numbers:
            continue
        # MetricFlow's names have no double underscore: it joins an entity to a dimension. One
        # file for each folder of marts, as a project keeps its semantic YAML beside its models
        name = node["name"].replace("__", "_")
        path = f"{os.path.dirname(node['original_file_path'])}/_semantic.yml"
        entity = f"{name}_entity"
        dimensions = [
            {"name": t, "type": "time", "type_params": {"time_granularity": "day"}, "expr": None}
            for t in times[:1]
        ] + [
            {"name": t, "type": "categorical", "type_params": None, "expr": None} for t in texts[:2]
        ]
        # One dimension from an expression, as real projects bucket a number
        dimensions.append(
            {
                "name": f"{numbers[0]}_band",
                "type": "categorical",
                "type_params": None,
                "expr": f"case when {numbers[0]} >= 100 then 'high' else 'low' end",
            }
        )
        # A measure's name is unique in the project, so it names its column in expr. The
        # dimensions above read the columns of their own names.
        measures = [{"name": f"{name}_{c}", "agg": "sum", "expr": c} for c in numbers[:3]]
        sm_id = f"semantic_model.{package}.{name}"
        semantic_models[sm_id] = {
            "name": name,
            "resource_type": "semantic_model",
            "package_name": package,
            "original_file_path": path,
            "unique_id": sm_id,
            "model": f"ref('{node['name']}')",
            "description": f"{name}, for the semantic layer.",
            "entities": [{"name": entity, "type": "primary", "expr": keys[0]}],
            "dimensions": dimensions,
            "measures": measures,
            "depends_on": {"macros": [], "nodes": [uid]},
            "config": {"enabled": True, "group": None, "meta": {"owner": "semantic-team"}},
            "group": None,
        }
        for i, measure in enumerate(measures):
            metric_id = f"metric.{package}.{measure['name']}"
            where = None
            if i == 0:  # filtered on the expression dimension
                band = f"{entity}__{numbers[0]}_band"
                where = {
                    "where_filters": [
                        {"where_sql_template": f"{{{{ Dimension('{band}') }}}} = 'high'"}
                    ]
                }
            metrics[metric_id] = {
                "name": measure["name"],
                "resource_type": "metric",
                "package_name": package,
                "original_file_path": path,
                "unique_id": metric_id,
                "label": measure["name"],
                "type": "simple",
                "type_params": {"measure": {"name": measure["name"], "filter": None}},
                "filter": where,
                "depends_on": {"macros": [], "nodes": [sm_id]},
                "config": {"enabled": True, "group": None, "meta": {}},
                "meta": {},
                "tags": [],
            }
        measures_of.append((f"metric.{package}.{measures[0]['name']}", sm_id))
    for i in range(0, len(measures_of) - 1, 2):
        (first, _), (second, _) = measures_of[i], measures_of[i + 1]
        ratio = f"metric.{package}.ratio_{i}"
        metrics[ratio] = {
            "name": f"ratio_{i}",
            "resource_type": "metric",
            "package_name": package,
            "original_file_path": SHARED_METRICS,
            "unique_id": ratio,
            "label": f"ratio {i}",
            "type": "ratio",
            "type_params": {
                "numerator": {"name": metrics[first]["name"]},
                "denominator": {"name": metrics[second]["name"]},
            },
            "filter": None,
            "depends_on": {"macros": [], "nodes": [first, second]},
            "config": {"enabled": True, "group": None, "meta": {}},
            "meta": {},
            "tags": [],
        }
    for i in range(0, len(measures_of), 10):
        chosen = measures_of[i : i + 10]
        query = f"saved_query.{package}.query_{i}"
        entity = semantic_models[chosen[0][1]]["entities"][0]["name"]
        band = semantic_models[chosen[0][1]]["dimensions"][-1]["name"]
        queries[query] = {
            "name": f"query_{i}",
            "resource_type": "saved_query",
            "package_name": package,
            "original_file_path": SHARED_METRICS,
            "unique_id": query,
            "query_params": {
                "metrics": [metrics[m]["name"] for m, _ in chosen],
                "group_by": [
                    "TimeDimension('metric_time', 'week')",
                    f"Dimension('{entity}__{band}')",
                ],
                "where": None,
            },
            "exports": [
                {
                    "name": f"query_{i}",
                    "config": {
                        "export_as": "table",
                        "schema_name": "exports",
                        "alias": f"query_{i}",
                    },
                }
            ],
            "depends_on": {"macros": [], "nodes": [m for m, _ in chosen]},
            "config": {"enabled": True, "group": "operations", "meta": {}},
            "group": "operations",
        }
    manifest["semantic_models"] = semantic_models
    manifest["metrics"] = metrics
    manifest["saved_queries"] = queries
    files = write_semantic_yaml(project, [semantic_models, metrics, queries])
    return {
        "semantic_models": len(semantic_models),
        "metrics": len(metrics),
        "saved_queries": len(queries),
        "yaml_files": len(files),
        "yaml_mb": round(sum(files.values()) / 1e6, 2),
    }


# Where the ratios and saved queries are defined, apart from the marts
SHARED_METRICS = "models/marts/_metrics.yml"
# What a YAML entry holds, by section: the manifest's fields a person writes
_WRITTEN = {
    "semantic_models": ("name", "description", "model", "entities", "dimensions", "measures"),
    "metrics": ("name", "label", "type", "type_params", "filter"),
    "saved_queries": ("name", "query_params", "exports", "config"),
}


def write_semantic_yaml(project: Path, sections: list[dict[str, Any]]) -> dict[str, int]:
    """The YAML the semantic layer is written in, as a person would write it, so that finding
    where each node is costs what it would in a real project. The sizes of the files, by path."""
    import yaml

    by_file: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for section, entries in zip(_WRITTEN, sections, strict=True):
        for entry in entries.values():
            written = {k: entry[k] for k in _WRITTEN[section] if entry.get(k) is not None}
            by_file.setdefault(entry["original_file_path"], {}).setdefault(section, []).append(
                written
            )
    sizes = {}
    for path, body in sorted(by_file.items()):
        text = yaml.safe_dump({"version": 2, **body}, sort_keys=False)
        (project / path).parent.mkdir(parents=True, exist_ok=True)
        (project / path).write_text(text)
        sizes[path] = len(text)
    return sizes


# Each reader's workload: it adds to the project and its manifest, and says what it added. A
# reader with a scan of its own is in the server's Status.readers, and is timed there too.
READERS: dict[str, Callable[[Path, dict[str, Any]], dict[str, Any]]] = {
    "airflow": prepare_airflow,
    "dbt_semantic": prepare_semantic,
}


def link_semantic(project: Path) -> dict[str, Any]:
    """How long the semantic layer takes to read from the manifest and to link, in this process:
    the part of the index it adds, measured apart from the rest."""
    from lineagepilot_core.graph.build import build_dbt_graph
    from lineagepilot_core.graph.model import EdgeKind
    from lineagepilot_core.graph.semantic import add_semantic_layer
    from lineagepilot_core.parsers import DbtParser
    from lineagepilot_core.parsers.base import ParseResult
    from lineagepilot_core.parsers.dbt_semantic import SEMANTIC_KEYS

    manifest = json.loads((project / "target" / "manifest.json").read_bytes())
    t = time.perf_counter()
    DbtParser()._process_semantic_layer(manifest, ParseResult())
    read_s = time.perf_counter() - t
    del manifest
    result = DbtParser().parse(
        {
            "manifest_path": str(project / "target" / "manifest.json"),
            "catalog_path": str(project / "target" / "catalog.json"),
        }
    )
    layer = (result.semantic_models, result.metrics, result.saved_queries)
    result.semantic_models, result.metrics, result.saved_queries = [], [], []
    graph = build_dbt_graph(result)  # its nodes, and what they depend on, without their reads
    edges = sum(1 for e in graph.edges if e.kind is EdgeKind.CONSUMES)
    columns = len(graph.columns)
    result.semantic_models, result.metrics, result.saved_queries = layer
    t = time.perf_counter()
    add_semantic_layer(graph, result)
    link_s = time.perf_counter() - t
    return {
        "read_ms": round(read_s * 1000, 1),
        "link_ms": round(link_s * 1000, 1),
        "fields": len(graph.columns) - columns,
        "column_reads": sum(1 for e in graph.edges if e.kind is EdgeKind.CONSUMES) - edges,
        "unresolved": sum(1 for u in graph.unresolved if u.subject.split(".")[0] in SEMANTIC_KEYS),
    }


# The server, end to end


async def run(project: Path, storage: Path, readers: list[str]) -> dict:
    client = BaseLanguageClient("budgets", "0", converter_factory=client_converter)
    client.feature(types.WINDOW_LOG_MESSAGE)(lambda params: None)
    updates: list[tuple[float, dict]] = []
    started = time.perf_counter()

    @client.feature("lineage/indexUpdated")
    def on_update(params):
        updates.append((time.perf_counter() - started, params))

    @client.feature(types.CLIENT_REGISTER_CAPABILITY)
    def on_register(params):
        return None

    diagnostics = {"files": 0, "items": 0, "last": 0.0}

    @client.feature(types.TEXT_DOCUMENT_PUBLISH_DIAGNOSTICS)
    def on_diagnostics(params):
        diagnostics["files"] += 1
        diagnostics["items"] += len(params.diagnostics)
        diagnostics["last"] = time.perf_counter() - started

    await client.start_io(sys.executable, "-I", "-m", "lineagepilot_lsp")
    await client.initialize_async(
        types.InitializeParams(
            capabilities=types.ClientCapabilities(),
            root_uri=project.as_uri(),
            initialization_options={
                "protocolVersions": [1, 2],
                "projectRoot": str(project),
                "trusted": True,
                "storagePath": str(storage),
            },
        )
    )
    client.initialized(types.InitializedParams())
    while not any(u["columnLineage"] == "done" and not u["indexing"] for _, u in updates):
        if any(u["state"] in ("unknown", "failed") for _, u in updates):
            raise SystemExit(f"index failed: {updates[-1][1]['message']}")
        await asyncio.sleep(0.05)
    model_stage = next((t for t, u in updates if u["state"] == "ready"), None)
    done = next(t for t, u in updates if u["columnLineage"] == "done" and not u["indexing"])

    def finished(update: dict) -> bool:
        return all(r["stage"] in ("done", "failed") for r in update["readers"])

    while not any(finished(u) for _, u in updates):
        await asyncio.sleep(0.05)
    await asyncio.sleep(2)  # let the diagnostics drain

    last = updates[-1][1]
    result: dict[str, Any] = {
        "first_graph_s": round(model_stage, 2) if model_stage else None,
        "column_lineage_done_s": round(done, 2),
        "from_cache": last["fromCache"],
        "nodes": last["counts"]["nodes"],
        "node_kinds": last["counts"]["nodeKinds"],
        "diagnostics": diagnostics["items"],
        "diagnostic_files": diagnostics["files"],
        "diagnostics_done_s": round(diagnostics["last"], 2),
        "readers": {},
    }
    for status in last["readers"]:
        if status["tool"] not in readers:
            continue
        linked = next(
            t
            for t, u in updates
            if any(
                r["tool"] == status["tool"] and r["stage"] in ("done", "failed")
                for r in u["readers"]
            )
        )
        summary = status["summary"] or {}
        result["readers"][status["tool"]] = {
            "linked_s": round(linked, 2),
            "stage": status["stage"],
            **{k: summary.get(k) for k in ("files", "pipelines", "tasks", "consumers", "links")},
        }
    graph = await client.protocol.send_request_async("lineage/export", {"format": "json"})
    nodes = json.loads(graph["content"])["nodes"]
    models = [n for n in nodes if n["kind"] == "model" and n.get("file_path")]
    sources = [n["id"] for n in nodes if n["kind"] == "source"]

    # Before the CodeLens loop below rewrites files, so the manifest still matches them
    async def review_ms(picked: list[dict], manifest: str | None = None) -> float:
        files = [
            {"uri": (project / n["file_path"]).as_uri(), "status": "modified", "previousUri": None}
            for n in picked
        ]
        t = time.perf_counter()
        await client.protocol.send_request_async(
            "lineage/reviewChanges",
            {
                "changedFiles": files,
                "baseline": {"ref": "main", "commit": "0" * 40, "manifestPath": manifest},
            },
        )
        return round((time.perf_counter() - t) * 1000, 1)

    # Staging models first: they have the most downstream
    ordered = sorted(models, key=lambda n: ("stg_" not in n["id"], n["id"]))
    result["review_1_model_ms"] = await review_ms(ordered[:1])
    result["review_10_models_ms"] = await review_ms(ordered[:10])
    result["review_100_models_ms"] = await review_ms(ordered[:100])
    # Marts: what the semantic layer reads directly
    marts = [n for n in models if "/marts/" in f"/{n['file_path']}"]
    result["review_100_marts_ms"] = await review_ms(marts[:100])
    state = str(project / "target" / "manifest.json")
    result["review_baseline_first_ms"] = await review_ms(ordered[:10], state)
    result["review_baseline_again_ms"] = await review_ms(ordered[:10], state)

    lens_times = []
    for node in models[::50]:
        path = project / node["file_path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("select 1\n")
        uri = path.as_uri()
        t = time.perf_counter()
        await client.protocol.send_request_async(
            types.TEXT_DOCUMENT_CODE_LENS,
            types.CodeLensParams(text_document=types.TextDocumentIdentifier(uri=uri)),
        )
        lens_times.append(time.perf_counter() - t)
    result["codelens_p95_ms"] = round(sorted(lens_times)[int(len(lens_times) * 0.95)] * 1000, 2)

    # The semantic layer's YAML: a lens on every entry, each counting what is downstream of it
    yaml_files = sorted(project.glob("models/**/_semantic.yml"))
    if (project / SHARED_METRICS).is_file():
        yaml_files.append(project / SHARED_METRICS)
    yaml_times, yaml_lenses = [], 0
    for path in yaml_files:
        t = time.perf_counter()
        found = await client.protocol.send_request_async(
            types.TEXT_DOCUMENT_CODE_LENS,
            types.CodeLensParams(text_document=types.TextDocumentIdentifier(uri=path.as_uri())),
        )
        yaml_times.append(time.perf_counter() - t)
        yaml_lenses += len(found or [])
    if yaml_times:
        ordered_times = sorted(yaml_times)
        result["codelens_yaml_files"] = len(yaml_times)
        result["codelens_yaml_lenses"] = yaml_lenses
        result["codelens_yaml_p95_ms"] = round(
            ordered_times[int(len(ordered_times) * 0.95)] * 1000, 1
        )
        result["codelens_yaml_max_ms"] = round(ordered_times[-1] * 1000, 1)

    impact_times = []
    for source in sources[:20]:
        t = time.perf_counter()
        await client.protocol.send_request_async("lineage/impact", {"targets": [source]})
        impact_times.append(time.perf_counter() - t)
    result["impact_max_ms"] = round(max(impact_times) * 1000, 1)
    result["impact_median_ms"] = round(statistics.median(impact_times) * 1000, 1)
    result["server_rss_mb"] = peak_rss_mb(client._server.pid)

    await client.shutdown_async(None)
    client.exit(None)
    await client.stop()
    return result


# The budgets: (run, measure, budget, unit, what it is). Design section 11 and design_next_phase
# section 8.2; "every reader" means those in READERS that were turned on.
BUDGETS = [
    ("warm", "first_graph_s", 1.0, "s", "warm start: lineage ready from process start"),
    ("cold", "codelens_p95_ms", 100, "ms", "CodeLens at p95"),
    ("warm", "codelens_p95_ms", 100, "ms", "CodeLens at p95"),
    ("cold", "impact_max_ms", 300, "ms", "impact, worst of 20 sources"),
    ("warm", "impact_max_ms", 300, "ms", "impact, worst of 20 sources"),
    ("cold", "review_100_models_ms", 300, "ms", "a review of 100 changed models, every reader on"),
    ("warm", "review_100_models_ms", 300, "ms", "a review of 100 changed models, every reader on"),
    ("cold", "server_rss_mb", 500, "MB", "server memory"),
    ("warm", "server_rss_mb", 500, "MB", "server memory"),
]


def checked(results: dict[str, Any]) -> list[dict[str, Any]]:
    """Each budget, with what was measured and whether it held."""
    found = []
    for run_name, measure, budget, unit, what in BUDGETS:
        value = results[run_name].get(measure)
        found.append(
            {
                "run": run_name,
                "measure": measure,
                "what": what,
                "value": value,
                "budget": budget,
                "unit": unit,
                "held": value is not None and value <= budget,
            }
        )
    return found


def make_project(artifacts: Path, project: Path, readers: list[str]) -> dict[str, Any]:
    """The throwaway project, with every node's files and each reader's workload."""
    (project / "target").mkdir(parents=True)
    os.symlink(artifacts / "catalog.json", project / "target" / "catalog.json")
    manifest = json.loads((artifacts / "manifest.json").read_text())
    # Named as the manifest's project is, so its nodes are this project's, not a package's
    name = manifest["metadata"].get("project_name") or "synthetic"
    (project / "dbt_project.yml").write_text(f"name: {name}\n")
    # Every node's files, so diagnostics are published as for a real project
    for node in [*manifest["nodes"].values(), *manifest["sources"].values()]:
        for relative in (
            node.get("original_file_path"),
            (node.get("patch_path") or "").split("://")[-1],
        ):
            if relative:
                path = project / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                if not path.exists():
                    # A model's own text, so a review finds the manifest has seen the file
                    path.write_text(node.get("raw_code") or "select 1\n")
    workloads = {name: READERS[name](project, manifest) for name in readers}
    if "dbt_semantic" in readers:
        (project / "target" / "manifest.json").write_text(json.dumps(manifest))
    else:
        os.symlink(artifacts / "manifest.json", project / "target" / "manifest.json")
    return workloads


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("artifacts", type=Path, help="a folder with manifest.json and catalog.json")
    parser.add_argument(
        "--readers",
        default=",".join(READERS),
        help=f"the readers to turn on, comma-separated, or 'none' (default: {','.join(READERS)})",
    )
    parser.add_argument("--check", action="store_true", help="exit 1 if a budget is missed")
    args = parser.parse_args()
    readers = [] if args.readers == "none" else [r for r in args.readers.split(",") if r]
    for name in readers:
        if name not in READERS:
            parser.error(f"no reader {name!r}; there are: {', '.join(READERS)}")
    artifacts = args.artifacts.resolve()
    with tempfile.TemporaryDirectory(prefix="lp-budgets-") as tmp:
        project = Path(tmp) / "project"
        results: dict[str, Any] = {
            "python": sys.version.split()[0],
            "platform": f"{platform.system()} {platform.machine()}",
            "readers": make_project(artifacts, project, readers),
        }
        if "dbt_semantic" in readers:
            results["readers"]["dbt_semantic"].update(link_semantic(project))
        storage = Path(tmp) / "storage"
        results["cold"] = asyncio.run(run(project, storage, readers))
        results["warm"] = asyncio.run(run(project, storage, readers))
        shutil.rmtree(storage, ignore_errors=True)
    results["budgets"] = checked(results)
    sys.stdout.write(json.dumps(results, indent=2) + "\n")
    missed = [b for b in results["budgets"] if not b["held"]]
    for budget in missed:
        sys.stderr.write(
            f"missed: {budget['run']} {budget['what']}: {budget['value']} {budget['unit']}, "
            f"budget {budget['budget']} {budget['unit']}\n"
        )
    if args.check and missed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
