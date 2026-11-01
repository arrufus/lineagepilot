"""Render the README tables from results/*.json, so numbers are never hand-copied.

    uv run python -m spike.report > work/report.md
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent.parent
RESULTS = HERE / "results"


def f(x: float | None, unit: str = "s", nd: int = 2) -> str:
    if x is None:
        return "–"
    if unit == "ms":
        return f"{x:.{nd}f} ms"
    if unit == "MB":
        return f"{x:.0f} MB"
    return f"{x:.{nd}f} s"


def status(value: float | None, budget: float) -> str:
    if value is None:
        return "not measured"
    return "met" if value <= budget else "**not met**"


def size_table(tag: str, size: str, s: dict[str, Any], budgets: dict[str, float]) -> list[str]:
    cold = s["cold"]
    rows = []
    a, o = cold["as-is"]["total_s"], cold["optimised"]["total_s"]
    budget = budgets["cold_index_2000_s"]
    label = "Cold index (JSON load + parse + graph), with catalog"
    st = status(a["max"], budget) if size == "2000" else ("reference" if size != "jaffle" else "reference")
    rows.append((label, "≤ 5 s (2,000)", f"{f(a['p50'])} / {f(a['max'])}", f"{f(o['p50'])} / {f(o['max'])}", st))
    nc = cold["as-is-no-catalog"]["total_s"]
    rows.append(("Cold index, manifest only", "", f"{f(nc['p50'])} / {f(nc['max'])}", "", ""))
    sj, osj = cold["as-is-stdlib-json"]["total_s"], cold["optimised-stdlib-json"]["total_s"]
    rows.append(("Cold index, stdlib json (labelled shim), with catalog", "", f"{f(sj['p50'])}",
                 f"{f(osj['p50'])} / {f(osj['max'])}", ""))
    bl_a = [r["phases_s"].get("_build_lineage", 0) for r in cold["as-is"]["runs"]]
    bl_o = [r["phases_s"].get("_build_lineage", 0) for r in cold["optimised"]["runs"]]
    rows.append(("  of which `_build_lineage` (median)", "", f(sorted(bl_a)[len(bl_a) // 2]),
                 f(sorted(bl_o)[len(bl_o) // 2], nd=3), ""))
    warm = s["warm"]
    best = min(warm, key=lambda k: warm[k]["total_s"]["max"])
    worst_w = max(v["total_s"]["max"] for v in warm.values())
    for k in ("json-stdlib", "json-orjson", "sqlite"):
        w = warm[k]
        rows.append((f"Warm start from snapshot ({k}), in-process / incl. interpreter start", "≤ 1 s",
                     f"{f(w['total_s']['p50'], nd=3)} / {f(w['wall_s_incl_interpreter']['p50'], nd=3)}", "",
                     status(w["wall_s_incl_interpreter"]["max"], budgets["warm_start_s"])))
    q = s["queries"]
    cl = q["codelens"]["ms"]
    rows.append(("CodeLens/hover, 1,000 random files (direct downstream)", "≤ 100 ms p95",
                 f"p50 {f(cl['p50'], 'ms', 4)}, p95 {f(cl['p95'], 'ms', 4)}, max {f(cl['max'], 'ms', 3)}", "",
                 status(cl["p95"], budgets["codelens_p95_ms"])))
    ct = q["codelens_transitive"]["ms"]
    rows.append(("  reference: with full downstream count", "", f"p95 {f(ct['p95'], 'ms', 3)}, max {f(ct['max'], 'ms', 3)}", "", ""))
    for k, lbl in (("high_fanout_source", "Impact, high-fan-out source"), ("median_model", "Impact, median model"),
                   ("worst_node", "Impact, worst node")):
        if k in q["impact"]:
            r = q["impact"][k]
            rows.append((f"{lbl} (closure {r['closure']:,})", "≤ 300 ms",
                         f"p95 {f(r['ms']['p95'], 'ms', 3)}, max {f(r['ms']['max'], 'ms', 3)}", "",
                         status(r["ms"]["max"], budgets["impact_ms"])))
    c = s["column_lineage"]["column_lineage"]
    star = sum(1 for m in c["models"] if m["star_only"])
    found = sum(m["output_columns_found"] for m in c["models"])
    total = sum(m["output_columns"] for m in c["models"])
    rows.append((f"Column lineage, {len(c['models'])} most complex models", "≤ 2 s",
                 f"p50 {f(c['s']['p50'], nd=3)}, max {f(c['s']['max'], nd=3)}", "",
                 status(c["s"]["max"], budgets["column_lineage_s"]) + " (time only)"))
    rows.append(("  output quality: models giving only `*` / output columns found", "",
                 f"{star}/{len(c['models'])} / {found} of {total}", "", "unusable" if star else ""))
    alt = [m["alt_sqlglot_lineage"] for m in c["models"] if "output_columns" in m.get("alt_sqlglot_lineage", {})]
    if alt:
        rows.append(("  labelled alternative: sqlglot.lineage + catalog schema", "",
                     f"max {f(max(x['s'] for x in alt), nd=3)}, {sum(x['resolved_to_table'] for x in alt)} of "
                     f"{sum(x['output_columns'] for x in alt)} columns reach a table", "", ""))
    rss = cold["as-is"]["peak_rss_mb"]
    rss_o = cold["optimised"]["peak_rss_mb"]
    after = [r.get("rss_after_parse_mb") or 0 for r in cold["as-is"]["runs"]]
    rows.append(("Peak RSS, indexing process (with catalog)", "≤ 500 MB (5,000)",
                 f"{f(rss['max'], 'MB')}", f"{f(rss_o['max'], 'MB')}",
                 status(rss["max"], budgets["peak_rss_5000_mb"]) if size == "5000" else "reference"))
    rows.append(("  RSS after parse, manifest freed", "", f(max(after), "MB"), "", ""))
    wr = max(r["peak_rss_mb"] for v in warm.values() for r in v["runs"])
    rows.append(("  server RSS if indexing runs in a child and the server loads the snapshot", "",
                 f(wr, "MB"), "", status(wr, budgets["peak_rss_5000_mb"]) if size == "5000" else ""))
    js = s["json"]["json_loads_s"]
    rows.append(("JSON loads (manifest + catalog), orjson / stdlib", "", f"{f(js['orjson']['p50'], nd=3)} / "
                 f"{f(js['stdlib']['p50'], nd=3)}", "", f"stdlib {js['stdlib']['p50'] / js['orjson']['p50']:.1f}× slower"))
    ev = s["event_loop"]["event_loop"]
    rows.append(("asyncio loop blocked by `await parse()` (max gap)", "", f(ev["await_in_loop"]["max_loop_gap_ms"], "ms", 0),
                 f"to_thread: {f(ev['to_thread']['max_loop_gap_ms'], 'ms', 0)}", ""))
    out = [f"#### Python {tag[2:]}, {size if size == 'jaffle' else f'{int(size):,} models'} "
           f"(manifest {s['manifest_mb']} MB, catalog {s['catalog_mb']} MB)", "",
           "| Measure | Budget | As-is (p50 / max) | Optimised variant | Status |", "|---|---|---|---|---|"]
    out += [f"| {a} | {b} | {c} | {d} | {e} |" for a, b, c, d, e in rows]
    return out


def main() -> None:
    br = json.loads((RESULTS / "bench_results.json").read_text())
    for tag in sorted(br):
        env = br[tag]["env"]
        print(f"### {tag}: {env['implementation']} {env['python']} on {env['machine']} ({env.get('cpu')}), "
              f"sqlglot {env.get('sqlglot')}, orjson {env.get('orjson')}\n")
        for size, s in br[tag]["sizes"].items():
            print("\n".join(size_table(tag, size, s, br[tag]["budgets"])))
            print()
        if "per_version" in br[tag]:
            print(f"#### Python {tag[2:]}: cold index per dbt version (compile manifests, no catalog, 1 run each)\n")
            print("| Project | dbt | Manifest | Capsules | Edges | Errors | As-is | Optimised | `_build_lineage` as-is | Peak RSS as-is | Same graph |")
            print("|---|---|---|---|---|---|---|---|---|---|---|")
            for r in br[tag]["per_version"]:
                a, o = r["as-is"], r["optimised"]
                print(f"| {r['project']} | {r['dbt_minor']} | {r['manifest_mb']} MB | {a['capsules']:,} | {a['edges']:,} | "
                      f"{a['parse_errors']} | {f(a['total_s'])} | {f(o['total_s'])} | {f(a['build_lineage_s'])} | "
                      f"{f(a['peak_rss_mb'], 'MB')} | {'yes' if r['identical'] else 'NO'} |")
            print()
    mv = RESULTS / "manifest_versions.json"
    if mv.exists():
        v = json.loads(mv.read_text())
        print("### dbt version to manifest schema\n")
        print("| dbt version | `metadata.dbt_schema_version` |\n|---|---|")
        for r in v["dbt_version_to_schema"]:
            print(f"| {r['dbt_version']} | `{r['dbt_schema_version']}` |")
        print("\n### Correctness per manifest (as-is DbtParser)\n")
        print("| Project | dbt | Step | Models | Sources | Capsules | Edges = depends_on | ParseErrors | "
              "`has_tests` true / models with tests | Column edges | URN collisions |")
        print("|---|---|---|---|---|---|---|---|---|---|---|")
        for r in v["manifests"]:
            p, fa = r["parser"], r["facts"]
            print(f"| {r['project']} | {r['dbt_version']} | {r['step']} | {fa['resource_types'].get('model', 0):,} | "
                  f"{fa['sources']:,} | {sum(p['capsules_by_type'].values()):,} | "
                  f"{p['edges']:,} = {p['expected']['depends_on_to_capsules']:,} {'✓' if p['edges_match_depends_on'] else '✗'} | "
                  f"{p['parse_error_count']} | {p['models_has_tests_true']} / {p['models_with_attached_tests_truth']:,} | "
                  f"{p['column_edges']} | {len(p['urn_collisions'])} |")


if __name__ == "__main__":
    main()
