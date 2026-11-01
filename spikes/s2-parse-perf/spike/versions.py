"""Manifest schema versions and per-manifest correctness checks.

For every real manifest under work/manifests/<project>/<dbt minor>/<step>/:

- metadata (dbt_version, dbt_schema_version, adapter_type);
- the key sets the parser touches (top level, model, column, test, source,
  exposure) and how they differ from dbt 1.8;
- the as-is DbtParser run: counts against the manifest, edges against the
  depends_on totals, ParseErrors, has_tests against the real test nodes,
  URN collisions, and the file-index coverage.

    uv run python -m spike.versions            # writes results/manifest_versions.json
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent.parent
MANIFESTS = HERE / "work" / "manifests"
RESULTS = HERE / "results"

SCHEMA_RE = re.compile(r"/manifest/v(\d+)\.json$")


def schema_number(url: str | None) -> int | None:
    m = SCHEMA_RE.search(url or "")
    return int(m.group(1)) if m else None


def key_profile(m: dict[str, Any]) -> dict[str, list[str]]:
    nodes = m.get("nodes", {}).values()
    def union(items: Any) -> list[str]:
        s: set[str] = set()
        for it in items:
            s.update(it.keys())
        return sorted(s)
    models = [n for n in nodes if n.get("resource_type") == "model"]
    tests = [n for n in nodes if n.get("resource_type") == "test"]
    return {
        "top_level": sorted(m.keys()),
        "metadata": sorted(m.get("metadata", {}).keys()),
        "model": union(models),
        "model_config": union(n.get("config", {}) for n in models),
        "column": union(c for n in models for c in n.get("columns", {}).values()),
        "test": union(tests),
        "source": union(m.get("sources", {}).values()),
        "exposure": union(m.get("exposures", {}).values()),
    }


def manifest_facts(m: dict[str, Any]) -> dict[str, Any]:
    nodes = m.get("nodes", {})
    rtypes: dict[str, int] = {}
    for n in nodes.values():
        rtypes[n.get("resource_type", "?")] = rtypes.get(n.get("resource_type", "?"), 0) + 1
    models = [n for n in nodes.values() if n.get("resource_type") == "model"]
    tests = [n for n in nodes.values() if n.get("resource_type") == "test"]
    owner_cfg = sum(1 for n in models if (n.get("config", {}).get("meta") or {}).get("owner"))
    owner_top = sum(1 for n in models if (n.get("meta") or {}).get("owner"))
    versioned = sum(1 for n in models if n.get("version") is not None)
    return {
        "resource_types": rtypes,
        "sources": len(m.get("sources", {})),
        "exposures": len(m.get("exposures", {})),
        "disabled": len(m.get("disabled", {}) or {}),
        "models_with_compiled_code": sum(1 for n in models if n.get("compiled_code")),
        "models_with_node_data_tests_key": sum(1 for n in models if "data_tests" in n),
        "columns_with_data_tests_key": sum(1 for n in models for c in n.get("columns", {}).values() if "data_tests" in c),
        "test_nodes": len(tests),
        "test_nodes_with_attached_node": sum(1 for t in tests if t.get("attached_node")),
        "test_nodes_with_column_name": sum(1 for t in tests if t.get("column_name")),
        "generic_test_names": sorted({(t.get("test_metadata") or {}).get("name") for t in tests} - {None}),
        "models_owner_in_config_meta": owner_cfg,
        "models_owner_in_top_meta": owner_top,
        "versioned_model_nodes": versioned,
        "models_with_patch_path": sum(1 for n in models if n.get("patch_path")),
    }


def parser_checks(manifest_path: Path, catalog_path: Path | None) -> dict[str, Any]:
    from spike.graph import build_graph, expected_edges
    from spike.loader import load_parsers
    mods = load_parsers()
    cfg: dict[str, Any] = {"manifest_path": str(manifest_path)}
    if catalog_path:
        cfg["catalog_path"] = str(catalog_path)
    result = asyncio.run(mods["dbt_parser"].DbtParser().parse(cfg))
    m = json.loads(manifest_path.read_bytes())
    g = build_graph(result)
    by_type: dict[str, int] = {}
    for c in result.capsules:
        by_type[c.capsule_type] = by_type.get(c.capsule_type, 0) + 1
    exp = expected_edges(m, [c.unique_id for c in result.capsules])
    attached: dict[str, int] = {}
    for n in m["nodes"].values():
        if n.get("resource_type") == "test" and n.get("attached_node"):
            attached[n["attached_node"]] = attached.get(n["attached_node"], 0) + 1
    models = [c for c in result.capsules if c.capsule_type == "model"]
    return {
        "capsules_by_type": by_type,
        "columns": len(result.columns),
        "column_edges": len(result.column_edges),
        "edges": len(result.edges),
        "graph_edges_unique": g.edge_count,
        "expected": exp,
        "edges_match_depends_on": len(result.edges) == exp["depends_on_to_capsules"],
        "parse_errors": [str(e) for e in result.errors][:20],
        "parse_error_count": len(result.errors),
        "source_version": result.source_version,
        "models_has_tests_true": sum(1 for c in models if c.has_tests),
        "models_test_count_sum": sum(c.test_count for c in models),
        "models_with_attached_tests_truth": sum(1 for c in models if attached.get(c.unique_id)),
        "attached_tests_truth": sum(attached.get(c.unique_id, 0) for c in models),
        "columns_has_tests_true": sum(1 for c in result.columns if c.has_tests),
        "urn_collisions": {u: ids for u, ids in g.urn_collisions.items()},
        "capsules_without_file": sum(1 for c in result.capsules if not c.original_file_path),
        "sources_without_file": sum(1 for c in result.capsules if c.capsule_type == "source" and not c.original_file_path),
        "pii_columns": sum(1 for c in result.columns if c.pii_type),
        "columns_with_type": sum(1 for c in result.columns if c.data_type),
        "exposures_in_parse_result": 0,  # DbtParser has no exposure handling at all
    }


def diff(a: dict[str, list[str]], b: dict[str, list[str]]) -> dict[str, dict[str, list[str]]]:
    out = {}
    for k in a:
        added = sorted(set(b.get(k, [])) - set(a[k]))
        removed = sorted(set(a[k]) - set(b.get(k, [])))
        if added or removed:
            out[k] = {"added": added, "removed": removed}
    return out


def analyse(root: Path = MANIFESTS) -> dict[str, Any]:
    rows = []
    profiles: dict[str, dict[str, list[str]]] = {}
    for mpath in sorted(root.glob("*/*/*/manifest.json")):
        project, minor, step = mpath.parts[-4:-1]
        m = json.loads(mpath.read_bytes())
        meta = m.get("metadata", {})
        cat = mpath.parent / "catalog.json"
        row = {
            "project": project, "dbt_minor": minor, "step": step,
            "manifest_mb": round(mpath.stat().st_size / 1e6, 2),
            "dbt_version": meta.get("dbt_version"),
            "dbt_schema_version": meta.get("dbt_schema_version"),
            "schema_number": schema_number(meta.get("dbt_schema_version")),
            "adapter_type": meta.get("adapter_type"),
            "facts": manifest_facts(m),
            "parser": parser_checks(mpath, cat if cat.exists() else None),
        }
        if cat.exists():
            c = json.loads(cat.read_bytes())
            row["catalog_schema_version"] = c.get("metadata", {}).get("dbt_schema_version")
        rows.append(row)
        profiles.setdefault(project, {})
        profiles[f"{project}/{minor}/{step}"] = key_profile(m)  # type: ignore[assignment]
        print(f"{project:12} {minor:5} {step:8} {row['dbt_version']:8} v{row['schema_number']} "
              f"edges_ok={row['parser']['edges_match_depends_on']} has_tests={row['parser']['models_has_tests_true']}"
              f"/{row['parser']['models_with_attached_tests_truth']} errors={row['parser']['parse_error_count']}",
              flush=True)
    diffs = {}
    for project in sorted({r["project"] for r in rows}):
        base = next((k for k in profiles if k.startswith(f"{project}/1.8/")), None)
        if not base:
            continue
        step = base.split("/")[2]
        for k in sorted(profiles):
            if k.startswith(f"{project}/") and k.endswith(f"/{step}") and k != base:
                diffs[f"{base} -> {k}"] = diff(profiles[base], profiles[k])  # type: ignore[arg-type]
    versions = sorted({(r["dbt_version"], r["dbt_schema_version"]) for r in rows})
    return {"manifests": rows, "dbt_version_to_schema": [{"dbt_version": v, "dbt_schema_version": s} for v, s in versions],
            "key_diffs_vs_1_8": diffs, "key_profiles": profiles}


def main() -> None:
    res = analyse()
    RESULTS.mkdir(exist_ok=True)
    extra = RESULTS / "fusion_attempt.json"
    if extra.exists():
        res["fusion"] = json.loads(extra.read_text())
    out = RESULTS / "manifest_versions.json"
    out.write_text(json.dumps(res, indent=1, default=str))
    print(out)


if __name__ == "__main__":
    sys.exit(main())
