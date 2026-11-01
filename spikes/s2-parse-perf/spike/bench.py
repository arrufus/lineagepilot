"""Benchmarks for design section 11, run against the as-is DbtParser.

Library functions (used by the tests on the small fixture) plus a CLI:

    python -m spike.bench worker <task> [options]   # one measurement, prints JSON
    python -m spike.bench run [--quick]              # orchestrates workers, writes results

Every measurement that says "cold" or "warm" runs in a fresh subprocess, so
module import, allocation and GC state are those of a new language server.
The page cache is warm after the first run on a file (no root to purge it);
on this machine a cold SSD read of the largest manifest adds well under 0.1 s.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import gc
import hashlib
import io
import json
import os
import platform
import random
import re
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent.parent
WORK = HERE / "work"
RESULTS = HERE / "results"

BUDGETS = {
    "cold_index_2000_s": 5.0,
    "warm_start_s": 1.0,
    "codelens_p95_ms": 100.0,
    "impact_ms": 300.0,
    "column_lineage_s": 2.0,
    "peak_rss_5000_mb": 500.0,
}


# --------------------------------------------------------------------------- utils
def env_info() -> dict[str, Any]:
    info: dict[str, Any] = {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "machine": platform.machine(),
        "system": platform.system(),
        "executable": sys.executable,
    }
    try:
        info["cpu"] = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                                     capture_output=True, text=True).stdout.strip() or None
    except OSError:
        info["cpu"] = None
    from importlib.metadata import PackageNotFoundError, version
    for dist in ("sqlglot", "orjson"):  # metadata only: importing orjson here would defeat the stdlib shim
        try:
            info[dist] = version(dist)
        except PackageNotFoundError:
            info[dist] = None
    return info


def pct(xs: list[float]) -> dict[str, float]:
    s = sorted(xs)
    if not s:
        return {}
    def q(p: float) -> float:
        k = (len(s) - 1) * p
        lo, hi = int(k), min(int(k) + 1, len(s) - 1)
        return s[lo] + (s[hi] - s[lo]) * (k - lo)
    return {"n": len(s), "p50": q(0.5), "p95": q(0.95), "max": s[-1], "min": s[0],
            "mean": statistics.fmean(s)}


def max_rss_mb() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux kilobytes.
    return r / (1024 * 1024) if sys.platform == "darwin" else r / 1024


def current_rss_mb() -> float | None:
    try:
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True).stdout
        return int(out.strip()) / 1024
    except (OSError, ValueError):
        return None


def instrument(parser: Any) -> dict[str, float]:
    """Wrap parser methods on the instance to time phases. Does not edit code."""
    timings: dict[str, float] = {}
    sync_names = ["_process_models", "_process_sources", "_process_seeds", "_process_snapshots",
                  "_build_lineage", "_build_column_lineage", "_extract_domains", "_extract_tags"]
    for name in sync_names:
        fn = getattr(parser, name)
        def wrap(*a: Any, __fn: Callable = fn, __name: str = name, **k: Any) -> Any:
            t = time.perf_counter()
            try:
                return __fn(*a, **k)
            finally:
                timings[__name] = timings.get(__name, 0.0) + time.perf_counter() - t
        setattr(parser, name, wrap)
    load = parser._load_json
    async def wrap_load(path: Any) -> Any:
        t = time.perf_counter()
        try:
            return await load(path)
        finally:
            timings["_load_json"] = timings.get("_load_json", 0.0) + time.perf_counter() - t
    parser._load_json = wrap_load
    return timings


def parser_class(mods: dict[str, Any], variant: str) -> type:
    if variant == "as-is":
        return mods["dbt_parser"].DbtParser
    if variant == "optimised":
        from spike.optimised import make_optimised_parser
        return make_optimised_parser(mods["dbt_parser"])
    raise ValueError(variant)


def parse_config(manifest: Path, catalog: Path | None) -> dict[str, Any]:
    cfg: dict[str, Any] = {"manifest_path": str(manifest)}
    if catalog:
        cfg["catalog_path"] = str(catalog)
    return cfg


def graph_fingerprint(g: Any) -> str:
    h = hashlib.sha256()
    for uid in sorted(g.nodes):
        h.update(uid.encode())
        for d in sorted(g.down[uid]):
            h.update(b">" + d.encode())
    return h.hexdigest()[:16]


def cold_index(manifest: Path, catalog: Path | None = None, variant: str = "as-is",
               json_backend: str = "orjson") -> tuple[dict[str, Any], Any, Any]:
    """JSON load + DbtParser.parse + graph build. Returns (timings, result, graph)."""
    t0 = time.perf_counter()
    from spike.loader import load_parsers
    mods = load_parsers(json_backend=json_backend)  # type: ignore[arg-type]
    from spike.graph import build_graph
    t_import = time.perf_counter()
    parser = parser_class(mods, variant)()
    phases = instrument(parser)
    result = asyncio.run(parser.parse(parse_config(manifest, catalog)))
    t_parse = time.perf_counter()
    graph = build_graph(result)
    t_graph = time.perf_counter()
    timings = {
        "import_s": t_import - t0,
        "parse_s": t_parse - t_import,
        "graph_s": t_graph - t_parse,
        "total_s": t_graph - t0,
        "phases_s": {k: round(v, 4) for k, v in phases.items()},
    }
    return timings, result, graph


# --------------------------------------------------------------------------- queries
def bench_codelens(graph: Any, n: int = 1000, seed: int = 11) -> dict[str, Any]:
    files = sorted(f for f, ids in graph.by_file.items()
                   if any(graph.nodes[i]["kind"] == "model" for i in ids))
    rng = random.Random(seed)
    queries = [rng.choice(files) for _ in range(n)]
    times = []
    sample = None
    for f in queries:
        t = time.perf_counter_ns()
        lens = graph.codelens(f)
        times.append((time.perf_counter_ns() - t) / 1e6)
        sample = sample or (f, lens)
    return {"queries": n, "distinct_files": len(files), "ms": pct(times), "sample": sample}


def bench_codelens_transitive(graph: Any, n: int = 1000, seed: int = 11) -> dict[str, Any]:
    """Reference only: the design's CodeLens text may mean the full downstream count."""
    files = sorted(f for f, ids in graph.by_file.items()
                   if any(graph.nodes[i]["kind"] == "model" for i in ids))
    rng = random.Random(seed)
    times = []
    for _ in range(n):
        f = rng.choice(files)
        t = time.perf_counter_ns()
        for uid in graph.lookup_file(f):
            len(graph.downstream_closure(uid))
        times.append((time.perf_counter_ns() - t) / 1e6)
    return {"queries": n, "ms": pct(times)}


def closure_sizes(graph: Any) -> dict[str, int]:
    """Exact closure size per node via reverse topological bitsets (fast, exact)."""
    order: list[str] = []
    indeg = {u: len(graph.up[u]) for u in graph.nodes}
    q = [u for u, d in indeg.items() if d == 0]
    while q:
        u = q.pop()
        order.append(u)
        for v in graph.down[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)
    bit = {u: 1 << i for i, u in enumerate(graph.nodes)}
    reach: dict[str, int] = {}
    for u in reversed(order):
        acc = 0
        for v in graph.down[u]:
            acc |= bit[v] | reach[v]
        reach[u] = acc
    return {u: bin(r).count("1") for u, r in reach.items()}


def pick_impact_roots(graph: Any) -> dict[str, dict[str, Any]]:
    sizes = closure_sizes(graph)
    sources = [u for u in graph.nodes if graph.nodes[u]["kind"] == "source"]
    models = [u for u in graph.nodes if graph.nodes[u]["kind"] == "model"]
    hi_src = max(sources, key=lambda u: (sizes[u], len(graph.down[u]), u)) if sources else None
    ranked = sorted(models, key=lambda u: (sizes[u], u))
    median = ranked[len(ranked) // 2] if ranked else None
    worst = max(graph.nodes, key=lambda u: (sizes[u], u))
    hi_fanout_src = max(sources, key=lambda u: (len(graph.down[u]), sizes[u], u)) if sources else None
    roots = {}
    for label, u in (("high_fanout_source", hi_src), ("highest_direct_fanout_source", hi_fanout_src),
                     ("median_model", median), ("worst_node", worst)):
        if u:
            roots[label] = {"uid": u, "closure": sizes[u], "direct_children": len(graph.down[u])}
    return roots


def bench_impact(graph: Any, repeats: int = 30) -> dict[str, Any]:
    out = {}
    for label, r in pick_impact_roots(graph).items():
        times = []
        res = None
        for _ in range(repeats):
            t = time.perf_counter_ns()
            res = graph.impact(r["uid"])
            times.append((time.perf_counter_ns() - t) / 1e6)
        assert res is not None and res["total"] == r["closure"], (res, r)
        out[label] = {**r, "ms": pct(times), "by_kind": res["by_kind"], "owners": len(res["owners"])}
    return out


# --------------------------------------------------------------------------- column lineage
_CTE = re.compile(r"\b(\w+)\s+as\s*\(", re.I)
_JOIN = re.compile(r"\bjoin\b", re.I)


def sql_complexity(sql: str) -> tuple[int, int, int]:
    return (len(_CTE.findall(sql)) + 2 * len(_JOIN.findall(sql)), len(sql), sql.count("\n"))


def column_lineage_sample(manifest: dict[str, Any], n: int = 10) -> list[tuple[str, str]]:
    models = [(uid, node.get("compiled_code") or "") for uid, node in manifest["nodes"].items()
              if node.get("resource_type") == "model" and node.get("compiled_code")]
    models.sort(key=lambda x: (sql_complexity(x[1]), x[0]), reverse=True)
    return models[:n]


def bench_column_lineage(manifest: dict[str, Any], n: int = 10, repeats: int = 3,
                         with_alternative: bool = True, catalog: dict[str, Any] | None = None) -> dict[str, Any]:
    from spike.loader import load_parsers
    mods = load_parsers()
    dialect = manifest.get("metadata", {}).get("adapter_type") or "postgres"
    parser = mods["sql_column_parser"].SQLColumnParser(dialect=dialect)
    sample = column_lineage_sample(manifest, n)
    schema = catalog_schema(catalog) if with_alternative else {}
    rows = []
    stdout_bytes = 0
    for uid, sql in sample:
        times = []
        mappings: list[Any] = []
        for _ in range(repeats):
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                t = time.perf_counter()
                mappings = parser.parse_select_statement(sql)
                times.append(time.perf_counter() - t)
            stdout_bytes += len(buf.getvalue())
        manifest_cols = list(manifest["nodes"][uid].get("columns", {}))
        cat_cols = list(((catalog or {}).get("nodes", {}).get(uid) or {}).get("columns", {}))
        out_cols = cat_cols or manifest_cols
        targets = [m.target_column for m in mappings]
        row = {
            "uid": uid,
            "sql_chars": len(sql),
            "complexity": sql_complexity(sql)[0],
            "s": statistics.median(times),
            "mappings": len(mappings),
            "star_only": targets == ["*"],
            "documented_columns": len(manifest_cols),
            "documented_columns_found": len(set(c.lower() for c in targets) & set(c.lower() for c in manifest_cols)),
            "output_columns": len(out_cols),
            "output_columns_source": "catalog" if cat_cols else "manifest",
            "output_columns_found": len(set(c.lower() for c in targets) & set(c.lower() for c in out_cols)),
            "printed_to_stdout": bool(stdout_bytes),
        }
        if with_alternative:
            row["alt_sqlglot_lineage"] = _alt_lineage(sql, out_cols, dialect, schema)
            if not schema:
                row["alt_sqlglot_lineage"]["note"] = "no catalog: select * cannot be expanded"
        rows.append(row)
    return {"dialect": dialect, "models": rows, "s": pct([r["s"] for r in rows]),
            "stdout_bytes_from_parser": stdout_bytes}


def catalog_schema(catalog: dict[str, Any] | None) -> dict[str, Any]:
    """sqlglot MappingSchema input {db: {schema: {table: {col: type}}}} from catalog.json."""
    schema: dict[str, Any] = {}
    for sec in ("nodes", "sources"):
        for n in ((catalog or {}).get(sec) or {}).values():
            md = n.get("metadata", {})
            if not md.get("name"):
                continue
            schema.setdefault(md.get("database") or "", {}).setdefault(md.get("schema") or "", {})[md["name"]] = {
                c: (v.get("type") or "unknown") for c, v in n.get("columns", {}).items()}
    return schema


def _alt_lineage(sql: str, columns: list[str], dialect: str, schema: dict[str, Any] | None = None) -> dict[str, Any]:
    """LABELLED ALTERNATIVE, not as-is: sqlglot.lineage for all output columns,
    with the catalog as schema so ``select *`` through CTEs can be expanded.
    Counts output columns whose lineage reaches a physical table."""
    from sqlglot import exp
    from sqlglot.lineage import lineage
    t0 = time.perf_counter()
    try:
        res = lineage(None, sql, schema or {}, dialect=dialect)
    except Exception as e:  # noqa: BLE001
        return {"s": round(time.perf_counter() - t0, 4), "error": f"{e.__class__.__name__}: {str(e)[:120]}",
                "schema_tables": sum(len(t) for d in (schema or {}).values() for t in d.values())}
    dt = time.perf_counter() - t0
    nodes = res if isinstance(res, dict) else {res.name: res}
    reached = sum(1 for node in nodes.values()
                  if any(isinstance(x.source, exp.Table) for x in node.walk() if not x.downstream))
    return {"s": round(dt, 4), "output_columns": len(nodes), "resolved_to_table": reached,
            "expected_columns": len(columns),
            "schema_tables": sum(len(t) for d in (schema or {}).values() for t in d.values())}


def column_lineage_quality(manifest: dict[str, Any]) -> dict[str, Any]:
    """Run the as-is SQLColumnParser on every compiled model and measure whether
    its output could link a column to an upstream relation at all."""
    from spike.loader import load_parsers
    mods = load_parsers()
    dialect = manifest.get("metadata", {}).get("adapter_type") or "postgres"
    parser = mods["sql_column_parser"].SQLColumnParser(dialect=dialect)
    rel = {n.get("alias") or n["name"] for n in manifest["nodes"].values()
           if n.get("resource_type") in ("model", "seed", "snapshot")}
    rel |= {s.get("identifier") or s["name"] for s in manifest.get("sources", {}).values()}
    star = explicit = empty = src_cols = to_relation = 0
    times = []
    for node in manifest["nodes"].values():
        if node.get("resource_type") != "model" or not node.get("compiled_code"):
            continue
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            t = time.perf_counter()
            maps = parser.parse_select_statement(node["compiled_code"])
            times.append(time.perf_counter() - t)
        if not maps:
            empty += 1
        elif [m.target_column for m in maps] == ["*"]:
            star += 1
        else:
            explicit += 1
            for m in maps:
                for c in m.source_columns:
                    src_cols += 1
                    if "." in c and c.rsplit(".", 1)[0] in rel:
                        to_relation += 1
    return {"models": len(times), "star_only": star, "explicit_select": explicit, "no_output": empty,
            "source_columns": src_cols, "source_columns_naming_a_relation": to_relation,
            "per_model_s": pct(times)}


# --------------------------------------------------------------------------- event loop
def event_loop_block(manifest: Path, catalog: Path | None, variant: str = "optimised") -> dict[str, Any]:
    """How long DbtParser.parse() blocks an asyncio loop (pygls runs on one)."""
    from spike.loader import load_parsers
    mods = load_parsers()
    cls = parser_class(mods, variant)
    cfg = parse_config(manifest, catalog)

    async def heartbeat(gaps: list[float], stop: asyncio.Event) -> None:
        last = time.perf_counter()
        while not stop.is_set():
            await asyncio.sleep(0.005)
            now = time.perf_counter()
            gaps.append(now - last)
            last = now

    async def run(mode: str) -> dict[str, float]:
        gaps: list[float] = []
        stop = asyncio.Event()
        hb = asyncio.create_task(heartbeat(gaps, stop))
        await asyncio.sleep(0.02)
        t = time.perf_counter()
        if mode == "await":
            await cls().parse(cfg)
        else:
            await asyncio.to_thread(lambda: asyncio.run(cls().parse(cfg)))
        dur = time.perf_counter() - t
        stop.set()
        await hb
        return {"parse_s": round(dur, 3), "max_loop_gap_ms": round(max(gaps) * 1000, 1),
                "p95_loop_gap_ms": round(pct(gaps)["p95"] * 1000, 1)}

    return {"variant": variant, "await_in_loop": asyncio.run(run("await")),
            "to_thread": asyncio.run(run("thread"))}


# --------------------------------------------------------------------------- workers
def _manifest_info(manifest: Path, catalog: Path | None) -> dict[str, Any]:
    return {"manifest": str(manifest.relative_to(HERE)) if manifest.is_relative_to(HERE) else str(manifest),
            "manifest_mb": round(manifest.stat().st_size / 1e6, 2),
            "catalog_mb": round(catalog.stat().st_size / 1e6, 2) if catalog else None}


def worker(task: str, a: argparse.Namespace) -> dict[str, Any]:
    manifest = Path(a.manifest)
    catalog = Path(a.catalog) if a.catalog else None
    out: dict[str, Any] = {"task": task, "env": env_info(), **_manifest_info(manifest, catalog)}
    if task == "cold":
        timings, result, graph = cold_index(manifest, catalog, a.variant, a.json_backend)
        from spike.loader import forbidden_loaded, json_backend_in_use
        out.update(variant=a.variant, json_backend=json_backend_in_use(), **timings,
                   capsules=len(result.capsules), columns=len(result.columns), edges=len(result.edges),
                   parse_errors=len(result.errors), graph_edges=graph.edge_count,
                   fingerprint=graph_fingerprint(graph), forbidden_modules=forbidden_loaded(),
                   peak_rss_mb=round(max_rss_mb(), 1))
        del result
        gc.collect()
        out["rss_after_parse_mb"] = current_rss_mb()
    elif task == "snapshot-write":
        from spike.graph import (add_manifest_extras, file_sha256, write_json_snapshot,
                                 write_sqlite_snapshot)
        _, result, graph = cold_index(manifest, catalog, "optimised")
        add_manifest_extras(graph, json.loads(manifest.read_bytes()))
        t = time.perf_counter(); sha = file_sha256(manifest); t_hash = time.perf_counter() - t
        snap = graph.to_snapshot(sha)
        d = Path(a.snapshot_dir); d.mkdir(parents=True, exist_ok=True)
        t = time.perf_counter(); write_json_snapshot(snap, d / "snapshot.json"); t_json = time.perf_counter() - t
        t = time.perf_counter(); write_sqlite_snapshot(snap, d / "snapshot.sqlite"); t_sql = time.perf_counter() - t
        out.update(hash_s=t_hash, json_write_s=t_json, sqlite_write_s=t_sql,
                   json_mb=round((d / "snapshot.json").stat().st_size / 1e6, 2),
                   sqlite_mb=round((d / "snapshot.sqlite").stat().st_size / 1e6, 2),
                   nodes=len(snap["nodes"]), edges=len(snap["edges"]), columns=len(snap["columns"]),
                   fingerprint=graph_fingerprint(graph))
    elif task == "warm":
        t0 = time.perf_counter()
        from spike import graph as G
        sha = G.file_sha256(manifest)
        t_hash = time.perf_counter() - t0
        p = Path(a.snapshot_dir) / ("snapshot.sqlite" if a.store == "sqlite" else "snapshot.json")
        snap = G.read_sqlite_snapshot(p) if a.store == "sqlite" else G.read_json_snapshot(p, a.json_backend)
        if snap["manifest_sha256"] != sha:
            raise SystemExit("snapshot is stale")
        t_read = time.perf_counter() - t0 - t_hash
        g = G.Graph.from_snapshot(snap)
        total = time.perf_counter() - t0
        from spike.bench import graph_fingerprint as fp
        out.update(store=a.store, json_backend=a.json_backend if a.store == "json" else None,
                   hash_s=t_hash, read_s=t_read, build_s=total - t_hash - t_read, total_s=total,
                   fingerprint=fp(g), peak_rss_mb=round(max_rss_mb(), 1))
    elif task == "queries":
        from spike.graph import add_manifest_extras, expected_edges
        _, result, graph = cold_index(manifest, catalog, "optimised")
        man = json.loads(manifest.read_bytes())
        exp_edges = expected_edges(man, [c.unique_id for c in result.capsules])
        add_manifest_extras(graph, man)
        out.update(codelens=bench_codelens(graph), codelens_transitive=bench_codelens_transitive(graph),
                   impact=bench_impact(graph), expected_edges=exp_edges, extras=graph.extras)
    elif task == "column-lineage":
        man = json.loads(manifest.read_bytes())
        cat = json.loads(catalog.read_bytes()) if catalog else None
        out.update(column_lineage=bench_column_lineage(man, n=a.n, catalog=cat))
    elif task == "json":
        import orjson
        data = manifest.read_bytes()
        cdata = catalog.read_bytes() if catalog else b""
        res: dict[str, list[float]] = {"orjson": [], "stdlib": []}
        for _ in range(a.repeats):
            for name, fn in (("orjson", orjson.loads), ("stdlib", json.loads)):
                gc.collect()
                t = time.perf_counter()
                fn(data)
                if cdata:
                    fn(cdata)
                res[name].append(time.perf_counter() - t)
        out.update(json_loads_s={k: pct(v) for k, v in res.items()})
    elif task == "event-loop":
        out.update(event_loop=event_loop_block(manifest, catalog))
    else:
        raise SystemExit(f"unknown task {task}")
    return out


def run_worker(task: str, **kw: Any) -> dict[str, Any]:
    cmd = [sys.executable, "-m", "spike.bench", "worker", task]
    for k, v in kw.items():
        if v is None:
            continue
        cmd += [f"--{k.replace('_', '-')}", str(v)]
    t = time.perf_counter()
    p = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True)
    wall = time.perf_counter() - t
    if p.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed:\n{p.stderr[-3000:]}")
    res = json.loads(p.stdout.strip().splitlines()[-1])
    res["wall_s_incl_interpreter"] = round(wall, 4)
    return res


# --------------------------------------------------------------------------- orchestration
def manifests_for(size: str) -> tuple[Path, Path | None]:
    if size == "jaffle":
        d = WORK / "manifests" / "jaffle" / "1.12" / "docs"
    else:
        d = WORK / "manifests" / f"synth-{size}" / "1.12" / "docs"
    cat = d / "catalog.json"
    return d / "manifest.json", (cat if cat.exists() else None)


def summarise(runs: list[dict[str, Any]], key: str = "total_s") -> dict[str, Any]:
    return pct([r[key] for r in runs])


def orchestrate(sizes: list[str], reps: int, as_is_reps: int, per_version: bool = True) -> dict[str, Any]:
    info = env_info()
    if info["system"] == "Darwin" and info["machine"] != "arm64":
        raise SystemExit(f"refusing to time on {info['machine']}: use a native arm64 interpreter")
    tag = f"py{sys.version_info.major}.{sys.version_info.minor}"
    out: dict[str, Any] = {"env": info, "budgets": BUDGETS, "sizes": {}}
    for size in sizes:
        manifest, catalog = manifests_for(size)
        if not manifest.exists():
            print(f"skip {size}: {manifest} missing", flush=True)
            continue
        print(f"[{tag}] {size}: {manifest.stat().st_size / 1e6:.1f} MB manifest", flush=True)
        s: dict[str, Any] = {**_manifest_info(manifest, catalog)}
        snap_dir = WORK / "snapshots" / tag / size
        cold: dict[str, list[dict[str, Any]]] = {}
        plan = [("as-is", "orjson", catalog, as_is_reps), ("optimised", "orjson", catalog, reps),
                ("as-is-no-catalog", "orjson", None, max(1, as_is_reps - 2)),
                ("optimised-stdlib-json", "stdlib", catalog, max(1, reps - 2)),
                ("as-is-stdlib-json", "stdlib", catalog, 1)]
        for label, backend, cat, n in plan:
            variant = "as-is" if label.startswith("as-is") else "optimised"
            cold[label] = []
            for i in range(n):
                r = run_worker("cold", manifest=manifest, catalog=cat, variant=variant, json_backend=backend)
                cold[label].append(r)
                print(f"  cold {label} #{i + 1}: total {r['total_s']:.2f}s "
                      f"(build_lineage {r['phases_s'].get('_build_lineage', 0):.2f}s) rss {r['peak_rss_mb']} MB",
                      flush=True)
        s["cold"] = {k: {"total_s": summarise(v), "wall_s_incl_interpreter": summarise(v, "wall_s_incl_interpreter"),
                         "peak_rss_mb": summarise(v, "peak_rss_mb"), "runs": v} for k, v in cold.items()}
        fps = {r["fingerprint"] for v in cold.values() for r in v}
        s["cold_graph_identical_across_variants"] = len(fps) == 1
        s["snapshot_write"] = run_worker("snapshot-write", manifest=manifest, catalog=catalog, snapshot_dir=snap_dir)
        warm: dict[str, list[dict[str, Any]]] = {}
        for store, backend in (("json", "stdlib"), ("json", "orjson"), ("sqlite", None)):
            key = f"{store}-{backend}" if backend else store
            warm[key] = [run_worker("warm", manifest=manifest, snapshot_dir=snap_dir, store=store,
                                    json_backend=backend or "stdlib") for _ in range(reps)]
            print(f"  warm {key}: p50 {summarise(warm[key])['p50']:.3f}s", flush=True)
        s["warm"] = {k: {"total_s": summarise(v), "wall_s_incl_interpreter": summarise(v, "wall_s_incl_interpreter"),
                         "runs": v} for k, v in warm.items()}
        s["warm_graph_matches_snapshot"] = all(r["fingerprint"] == s["snapshot_write"]["fingerprint"]
                                               for v in warm.values() for r in v)
        s["queries"] = run_worker("queries", manifest=manifest, catalog=catalog)
        print(f"  codelens p95 {s['queries']['codelens']['ms']['p95']:.4f} ms", flush=True)
        s["column_lineage"] = run_worker("column-lineage", manifest=manifest, catalog=catalog, n=10)
        print(f"  column lineage max {s['column_lineage']['column_lineage']['s']['max']:.3f}s", flush=True)
        s["json"] = run_worker("json", manifest=manifest, catalog=catalog, repeats=5)
        s["event_loop"] = run_worker("event-loop", manifest=manifest, catalog=catalog)
        out["sizes"][size] = s
    if per_version:
        out["per_version"] = per_version_cold()
    return out


def per_version_cold() -> list[dict[str, Any]]:
    """One as-is and one optimised cold index per compile manifest (no catalog),
    for every dbt Core minor compiled and for Fusion."""
    rows = []
    for mpath in sorted((WORK / "manifests").glob("*/*/compile/manifest.json")):
        project, minor = mpath.parts[-4], mpath.parts[-3]
        row: dict[str, Any] = {"project": project, "dbt_minor": minor, **_manifest_info(mpath, None)}
        for variant in ("as-is", "optimised"):
            r = run_worker("cold", manifest=mpath, variant=variant, json_backend="orjson")
            row[variant] = {k: r[k] for k in ("total_s", "parse_s", "peak_rss_mb", "capsules", "columns",
                                              "edges", "parse_errors", "fingerprint")}
            row[variant]["build_lineage_s"] = r["phases_s"].get("_build_lineage")
        row["identical"] = row["as-is"]["fingerprint"] == row["optimised"]["fingerprint"]
        print(f"  per-version {project} {minor}: as-is {row['as-is']['total_s']:.2f}s "
              f"optimised {row['optimised']['total_s']:.2f}s", flush=True)
        rows.append(row)
    return rows


def memory_variants(sizes: list[str], reps: int = 3) -> dict[str, Any]:
    """Peak RSS of the indexing process per JSON loader (optimised parser).
    stdlib-trim is a LABELLED variant that drops unused objects while decoding."""
    out: dict[str, Any] = {}
    for size in sizes:
        manifest, catalog = manifests_for(size)
        rows = {}
        for backend in ("orjson", "stdlib", "stdlib-trim"):
            runs = [run_worker("cold", manifest=manifest, catalog=catalog, variant="optimised", json_backend=backend)
                    for _ in range(reps)]
            rows[backend] = {"total_s": summarise(runs), "peak_rss_mb": summarise(runs, "peak_rss_mb"),
                             "rss_after_parse_mb": max(r.get("rss_after_parse_mb") or 0 for r in runs),
                             "fingerprints": sorted({r["fingerprint"] for r in runs}),
                             "summary": {k: runs[0][k] for k in ("capsules", "columns", "edges")}}
            print(f"  memory {size} {backend}: peak {rows[backend]['peak_rss_mb']['max']:.0f} MB, "
                  f"total p50 {rows[backend]['total_s']['p50']:.2f}s", flush=True)
        rows["identical_graphs"] = len({fp for r in rows.values() for fp in r["fingerprints"]}) == 1
        out[size] = rows
    return out


def merge_results(tag: str, data: dict[str, Any]) -> Path:
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / "bench_results.json"
    allres = json.loads(path.read_text()) if path.exists() else {}
    allres[tag] = data
    path.write_text(json.dumps(allres, indent=1, default=str))
    return path


def main() -> None:
    ap = argparse.ArgumentParser(prog="spike.bench")
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker")
    w.add_argument("task")
    w.add_argument("--manifest", required=True)
    w.add_argument("--catalog")
    w.add_argument("--variant", default="as-is")
    w.add_argument("--json-backend", default="orjson")
    w.add_argument("--snapshot-dir")
    w.add_argument("--store", default="json")
    w.add_argument("--n", type=int, default=10)
    w.add_argument("--repeats", type=int, default=5)
    r = sub.add_parser("run")
    r.add_argument("--sizes", nargs="+", default=["jaffle", "2000", "5000"])
    r.add_argument("--reps", type=int, default=5)
    r.add_argument("--as-is-reps", type=int, default=5)
    r.add_argument("--skip-per-version", action="store_true")
    qual = sub.add_parser("quality")
    qual.add_argument("--sizes", nargs="+", default=["jaffle", "2000", "5000"])
    mem = sub.add_parser("memory")
    mem.add_argument("--sizes", nargs="+", default=["2000", "5000"])
    mem.add_argument("--reps", type=int, default=3)
    a = ap.parse_args()
    if a.cmd == "worker":
        res = worker(a.task, a)
        sys.stdout.write("\n" + json.dumps(res, default=str) + "\n")
    elif a.cmd == "quality":
        tag = f"py{sys.version_info.major}.{sys.version_info.minor}"
        path = RESULTS / "bench_results.json"
        allres = json.loads(path.read_text())
        allres[tag]["column_lineage_quality"] = {
            size: column_lineage_quality(json.loads(manifests_for(size)[0].read_bytes())) for size in a.sizes}
        path.write_text(json.dumps(allres, indent=1, default=str))
        print(json.dumps(allres[tag]["column_lineage_quality"], indent=1))
    elif a.cmd == "memory":
        tag = f"py{sys.version_info.major}.{sys.version_info.minor}"
        path = RESULTS / "bench_results.json"
        allres = json.loads(path.read_text())
        allres[tag]["memory_variants"] = memory_variants(a.sizes, a.reps)
        path.write_text(json.dumps(allres, indent=1, default=str))
        print(path)
    else:
        tag = f"py{sys.version_info.major}.{sys.version_info.minor}"
        data = orchestrate(a.sizes, a.reps, a.as_is_reps, not a.skip_per_version)
        print(merge_results(tag, data))


if __name__ == "__main__":
    main()
