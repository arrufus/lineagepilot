"""Measure the design section 11 budgets on a dbt project (Phase 1 exit evidence).

Usage, from packages/core:

    uv run python scripts/budgets.py <dir with manifest.json and catalog.json>

Each measurement runs in a fresh interpreter, with orjson hidden: the VSIX bundle ships
without it. The 5,000-model synthetic manifest comes from spike S2
(``spikes/s2-parse-perf``, ``scripts/build_manifests.py``; it lives under ``work/``).
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

PRELUDE = "import sys, time, gc, resource; sys.modules['orjson'] = None\n"

COLD = """
t = time.perf_counter()
from lineagepilot_core.graph import build_dbt_graph
from lineagepilot_core.parsers import DbtParser
r = DbtParser().parse({"manifest_path": f"{root}/manifest.json", "catalog_path": f"{root}/catalog.json"})
g = build_dbt_graph(r)
out = {"cold_index_s": time.perf_counter() - t, "models": sum(n.resource_type == "model" for n in r.nodes)}
from lineagepilot_core.graph.column_lineage import add_column_lineage, compiled_sql
t = time.perf_counter()
statuses = add_column_lineage(g, compiled_sql(r))
out["column_lineage_all_s"] = time.perf_counter() - t
from lineagepilot_core.snapshot import save_snapshot
save_snapshot(g, f"{cache}/graph.json")
out["indexer_peak_rss_mb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (2**20 if sys.platform == "darwin" else 2**10)
print(json.dumps(out))
"""

WARM = """
t = time.perf_counter()
from lineagepilot_core.snapshot import load_snapshot, file_sha256
file_sha256(f"{root}/manifest.json"); file_sha256(f"{root}/catalog.json")
g = load_snapshot(f"{cache}/graph.json")
gc.collect(); gc.freeze()  # what the server does after loading (design section 11)
out = {"warm_start_s": time.perf_counter() - t}
out["server_rss_mb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (2**20 if sys.platform == "darwin" else 2**10)

from lineagepilot_core.analysis import impact, traverse, Direction
from lineagepilot_core.graph import NodeKind
from lineagepilot_core.graph.column_lineage import catalog_schema
models = sorted(n.id for n in g.nodes.values() if n.kind is NodeKind.MODEL)
sources = [n.id for n in g.nodes.values() if n.kind is NodeKind.SOURCE]
worst = max(sources, key=lambda s: len(traverse(g, [s], Direction.DOWNSTREAM).found()))

def p95(samples):
    samples = sorted(samples)
    return samples[int(len(samples) * 0.95)]

times = []
for m in models[::5]:  # CodeLens: count everything downstream of a model
    t = time.perf_counter(); impact(g, [m]).counts(); times.append(time.perf_counter() - t)
out["codelens_p95_ms"] = p95(times) * 1000
times = []
for _ in range(30):
    t = time.perf_counter(); impact(g, [worst]); times.append(time.perf_counter() - t)
out["model_impact_worst_ms"] = max(times) * 1000
out["model_impact_worst_nodes"] = len(impact(g, [worst]).nodes)
print(json.dumps(out))
"""

ONE_MODEL = """
from lineagepilot_core.graph import build_dbt_graph, NodeKind
from lineagepilot_core.graph.column_lineage import add_column_lineage, catalog_schema, compiled_sql
from lineagepilot_core.parsers import DbtParser
r = DbtParser().parse({"manifest_path": f"{root}/manifest.json", "catalog_path": f"{root}/catalog.json"})
g = build_dbt_graph(r); sql = compiled_sql(r); schema = catalog_schema(g)
longest = sorted(sql, key=lambda k: len(sql[k]), reverse=True)[:10]
times = []
for node_id in longest:
    t = time.perf_counter(); add_column_lineage(g, sql, [node_id], schema=schema); times.append(time.perf_counter() - t)
print(json.dumps({"column_lineage_one_model_max_s": max(times)}))
"""


def run(code: str, root: Path, cache: Path) -> dict:
    script = PRELUDE + f"import json\nroot, cache = {str(root)!r}, {str(cache)!r}\n" + code
    done = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )
    return json.loads(done.stdout.strip().splitlines()[-1])


def main() -> None:
    root = Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory() as cache:
        results = {"python": sys.version.split()[0]}
        results |= run(COLD, root, Path(cache))
        results |= run(WARM, root, Path(cache))
        results |= run(ONE_MODEL, root, Path(cache))
    sys.stdout.write(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
