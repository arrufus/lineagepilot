"""Check the Dagster reader against dagster and dagster-dbt themselves: asset keys and groups
(task 8.5) and what asset jobs select (task 8.6).

CI runs it in a job of its own, as ``selector_conformance.py``, because dagster-dbt 0.29 needs
Python 3.11 to 3.13 and is not one of LineagePilot's dependencies:

    uv run --no-config --no-project --python 3.13 --with dagster==1.13.29 \\
        --with dagster-dbt==0.29.29 --with dbt-core==1.12.6 --with-editable . \\
        python scripts/dagster_key_conformance.py

**Keys and groups.** The manifests are ``fixtures/impact-shop``'s, whose seeds set a schema and
whose models have meta; the Airflow test project's, which has versioned models; and a copy of
impact-shop's, changed in memory to set ``meta.dagster.asset_key`` and groups in each way
dagster-dbt reads them. For each, dagster-dbt gives the keys twice,
``DagsterDbtTranslator().get_asset_key`` for every model, seed, snapshot and source, and
``@dbt_assets(manifest=…)`` for the assets it makes, and the group once, by
``get_group_name``. Each must equal the reader's.

**Jobs and downstream assets.** A definitions file, written once, holds two dbt asset definitions
over the changed copy, an asset job for each selection in ``JOBS``, and an asset that is not dbt's
for each form of dependency in ``DOWNSTREAM``. dagster loads the file and resolves each job's
selection and each asset's dependencies; the reader reads the same file, as syntax, and must find
the same dbt nodes.

**The acceptance fixture** (task 8.9). ``fixtures/impact-shop/dagster`` holds two code locations:
a dg project whose ``DbtProjectComponent`` makes the marts into assets, loaded through the
project's own ``definitions.py``, and a definitions file with a dbt asset definition, a job, its
schedule and assets downstream. dagster loads both, from a copy of the shop. Each dbt asset
definition, job, schedule and asset that is not dbt's must select the dbt nodes the reader finds,
and each schedule must name the same job and cron.

dbt-core evaluates the definitions' dbt selections, as dagster-dbt needs it to; nothing here runs
dbt. With ``--record FILE``, the answers are written to that file, with the
fields of each node that the key and the group are made from:
tests/fixtures/dagster/asset_keys.json is what the offline tests read.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import shutil
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Any

from lineagepilot_core.readers.common.manifest import manifest_from_dict
from lineagepilot_core.readers.dagster.keys import default_group, default_key

HERE = Path(__file__).resolve().parent
IMPACT_SHOP = HERE.parents[2] / "fixtures" / "impact-shop"
AIRFLOW_SHOP = (
    HERE.parent / "tests" / "fixtures" / "airflow" / "dbt" / "shop" / "target" / "manifest.json"
)
KINDS = ("model", "seed", "snapshot")
# What dagster-dbt's key and group are made from, and so what the record keeps of each node
FIELDS = ("resource_type", "name", "source_name", "version", "alias", "meta")
CONFIG_FIELDS = ("meta", "schema", "group")


def variants(raw: dict[str, Any]) -> dict[str, Any]:
    """impact-shop's manifest, with ``meta.dagster.asset_key`` and groups set in each way
    dagster-dbt reads them, a schema in a model's config, and a versioned model."""
    changed = copy.deepcopy(raw)
    nodes = changed["nodes"]
    p = "model.impact_shop."
    # A list in config.meta, and a group in meta, which the key does not read here
    nodes[p + "orders"]["config"]["meta"] = {"dagster": {"asset_key": ["mart", "orders"]}}
    nodes[p + "orders"]["meta"] = {"dagster": {"group": "finance"}}
    # A string in meta, with config.meta empty
    nodes[p + "stg_orders"]["config"]["meta"] = {}
    nodes[p + "stg_orders"]["meta"] = {"dagster": {"asset_key": "staging_orders"}}
    # config.meta is not empty, so meta is not read for the key; and a dbt group
    nodes[p + "customer_revenue"]["config"]["meta"] = {"owner": "finance"}
    nodes[p + "customer_revenue"]["meta"] = {"dagster": {"asset_key": ["not", "read"]}}
    nodes[p + "customer_revenue"]["config"]["group"] = "finance"
    # A schema in the config, and a versioned model
    nodes[p + "order_counts"]["config"]["schema"] = "analytics"
    nodes[p + "revenue_tiers"].update(version=2, alias="revenue_tiers_v2")
    return changed


# What dagster-dbt must make of the variants, so that each rule is reached and none passes by
# being missed: a key in config.meta, a string in meta, meta not read, a schema, a version
CONTROLS = {
    "model.impact_shop.orders": ["mart", "orders"],
    "model.impact_shop.stg_orders": ["staging_orders"],
    "model.impact_shop.customer_revenue": ["customer_revenue"],
    "model.impact_shop.order_counts": ["analytics", "order_counts"],
    "model.impact_shop.revenue_tiers": ["revenue_tiers_v2"],
}
GROUP_CONTROLS = {
    "model.impact_shop.orders": "finance",
    "model.impact_shop.customer_revenue": "finance",
    "model.impact_shop.stg_customers": "default",
}

# (label, the job's selection in Python; None for none). ``staging`` and ``rest`` are the two
# definitions in the file the jobs are written into.
JOBS: list[tuple[str, str | None]] = [
    ("all", "AssetSelection.all()"),
    ("no selection", None),
    ("a key", 'AssetSelection.keys("stg_customers")'),
    ("an AssetKey", 'AssetSelection.keys(AssetKey(["raw", "raw_orders"]))'),
    ("a key from config.meta", 'AssetSelection.keys("mart/orders")'),
    ("upstream", 'AssetSelection.keys("mart/orders").upstream()'),
    ("upstream to a depth", 'AssetSelection.keys("customer_revenue").upstream(depth=1)'),
    ("upstream without itself", 'AssetSelection.keys("mart/orders").upstream(include_self=False)'),
    ("downstream", 'AssetSelection.keys("staging_orders").downstream()'),
    ("a group", 'AssetSelection.groups("finance")'),
    ("the default group", 'AssetSelection.groups("default")'),
    ("union", 'AssetSelection.groups("finance") | AssetSelection.keys("stg_customers")'),
    ("difference", 'AssetSelection.all() - AssetSelection.groups("finance")'),
    ("intersection", 'AssetSelection.groups("finance") & AssetSelection.keys("mart/orders")'),
    ("a definition's assets", "AssetSelection.assets(staging)"),
    ("a list of definitions", "[staging]"),
    ("a dbt selection", 'build_dbt_asset_selection([rest], dbt_select="+customer_revenue")'),
    ("string key", '"key:mart/orders"'),
    ("string all upstream", '"+key:mart/orders"'),
    ("string one upstream", '"1+key:mart/orders"'),
    ("string downstream", '"key:staging_orders+"'),
    ("string pattern", '"key:stg_*"'),
    ("string group", '"group:finance"'),
    ("string not", '"not group:finance"'),
    ("string precedence", '"key:mart/orders or key:staging_orders and group:finance"'),
    ("string parentheses", '"(key:mart/orders or key:staging_orders) and group:finance"'),
    ("bare name", '"stg_customers"'),
    ("bare one upstream", '"+customer_revenue"'),
    ("bare all upstream", '"*customer_revenue"'),
    ("bare two downstream", '"stg_customers++"'),
    ("star", '"*"'),
    ("a list of strings", '["stg_customers", "key:raw/*"]'),
]


# Assets that are not dbt's, each depending on dbt's in one way (task 8.7): (its key, its source)
DOWNSTREAM: list[tuple[str, str]] = [
    ("d_string", '@asset(deps=["customer_revenue"])\ndef d_string(): ...'),
    ("d_key", '@asset(deps=[AssetKey(["mart", "orders"])])\ndef d_key(): ...'),
    ("d_unmatched", '@asset(deps=["orders"])\ndef d_unmatched(): ...'),
    ("d_group", '@asset(deps=["orders"], group_name="finance")\ndef d_group(): ...'),
    ("d_model", '@asset(deps=[get_asset_key_for_model([rest], "orders")])\ndef d_model(): ...'),
    ("d_definition", "@asset(deps=[staging])\ndef d_definition(): ..."),
    ("d_dep", '@asset(deps=[AssetDep("customer_revenue")])\ndef d_dep(): ...'),
    ("d_in", '@asset(ins={"x": AssetIn(key="staging_orders")})\ndef d_in(x): ...'),
    ("d_param", "@asset\ndef d_param(context, stg_customers): ..."),
    ("d_mixed", '@asset(deps=["stg_customers", "not_dbt"])\ndef d_mixed(): ...'),
    ("m1", ""),
    ("m2", ""),
    ("ext", ""),
]
_MULTI = (
    '@multi_asset(specs=[AssetSpec("m1", deps=["stg_customers"]), '
    'AssetSpec("m2", deps=[AssetKey(["raw", "raw_orders"])])])\ndef d_multi(): ...'
)
_EXTERNAL = 'ext = AssetSpec("ext", deps=["revenue_tiers_v2"])'


def manifests() -> dict[str, dict[str, Any]]:
    impact = json.loads((IMPACT_SHOP / "target" / "manifest.json").read_text(encoding="utf-8"))
    return {
        "impact-shop": impact,
        "airflow-shop": json.loads(AIRFLOW_SHOP.read_text(encoding="utf-8")),
        "impact-shop-variants": variants(impact),
    }


def job_name(position: int) -> str:
    return f"job_{position}"


def write_workspace(folder: Path) -> Path:
    """impact-shop in ``folder/dbt``, with the changed manifest, and the definitions file in
    ``folder/orch/defs.py``. Returns the dbt project."""
    project = folder / "dbt"
    ignore = shutil.ignore_patterns("changes", "variants", "dagster")
    shutil.copytree(IMPACT_SHOP, project, ignore=ignore)
    raw = json.loads((project / "target" / "manifest.json").read_text(encoding="utf-8"))
    (project / "target" / "manifest.json").write_text(json.dumps(variants(raw)), encoding="utf-8")
    jobs = "\n".join(
        f"{job_name(i)} = define_asset_job({job_name(i)!r}"
        + (f", selection={selection})" if selection is not None else ")")
        for i, (_, selection) in enumerate(JOBS)
    )
    names = ", ".join(job_name(i) for i in range(len(JOBS)))
    text = textwrap.dedent(
        """
        from pathlib import Path

        from dagster import (
            AssetDep, AssetIn, AssetKey, AssetSelection, AssetSpec, Definitions, asset,
            define_asset_job, multi_asset,
        )
        from dagster_dbt import build_dbt_asset_selection, dbt_assets, get_asset_key_for_model

        MANIFEST = Path(__file__).parent.parent / "dbt" / "target" / "manifest.json"


        @dbt_assets(manifest=MANIFEST, select="stg_orders stg_customers", name="staging")
        def staging(context):
            yield from ()


        @dbt_assets(manifest=MANIFEST, exclude="stg_orders stg_customers", name="rest")
        def rest(context):
            yield from ()

        """
    )
    downstream = "\n\n\n".join(source for _, source in DOWNSTREAM if source)
    text += f"{downstream}\n\n\n{_MULTI}\n\n\n{_EXTERNAL}\n\n\n{jobs}\n\n"
    made = ", ".join(key for key, source in DOWNSTREAM if source)
    text += f"defs = Definitions(assets=[staging, rest, {made}, d_multi, ext], jobs=[{names}])\n"
    (folder / "orch").mkdir()
    (folder / "orch" / "defs.py").write_text(text, encoding="utf-8")
    return project


def dagster_jobs(folder: Path) -> dict[str, list[str]]:
    """What dagster selects for each job in the definitions file: the dbt nodes, by unique ID."""
    from dagster import AssetKey
    from dagster_dbt.asset_utils import DAGSTER_DBT_UNIQUE_ID_METADATA_KEY

    spec = importlib.util.spec_from_file_location("conformance_defs", folder / "orch" / "defs.py")
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    defs = module.defs
    graph = defs.resolve_asset_graph()
    uid_of = {
        spec.key: spec.metadata[DAGSTER_DBT_UNIQUE_ID_METADATA_KEY]
        for assets in (module.staging, module.rest)
        for spec in assets.specs
    }
    found = {}
    for position in range(len(JOBS)):
        job = getattr(module, job_name(position))
        keys = job.selection.resolve(graph)
        found[job_name(position)] = sorted(uid_of[key] for key in keys if key in uid_of)
    for key, _ in DOWNSTREAM:
        parents = graph.get(AssetKey(key)).parent_keys
        found[key] = sorted(uid_of[parent] for parent in parents if parent in uid_of)
    return found


def reader_jobs(folder: Path, project: Path) -> dict[str, Any]:
    """What the reader finds each job runs: the dbt nodes, or why it cannot say."""
    from lineagepilot_core.readers.dagster.scan import scan_dagster

    scan = scan_dagster(project, folder, ["orch"])
    found: dict[str, Any] = {job_name(i): [] for i in range(len(JOBS))}
    found.update({key: [] for key, _ in DOWNSTREAM})
    for record in scan["jobs"] + scan["assets"]:
        found[record["name"]] = (
            record["nodes"] if record["status"] == "resolved" else f"unknown: {record['reason']}"
        )
    return found


def fixture_copy(folder: Path) -> Path:
    """A copy of impact-shop with its Dagster code locations, for dagster to load: importing them
    writes nothing into the fixture."""
    shop = folder / "impact_shop"
    ignore = shutil.ignore_patterns("changes", "variants", "dags", "__pycache__")
    shutil.copytree(IMPACT_SHOP, shop, ignore=ignore)
    return shop


def dagster_fixture(shop: Path) -> dict[str, Any]:
    """What dagster loads from the fixture's code locations: for each dbt asset definition, job,
    schedule and asset that is not dbt's, the dbt nodes, by unique ID; for a schedule, its job and
    cron too. The component's definition is ``component``, as dagster names it after the project.
    The locations are named as Dagster names them: the dg project by its ``[project] name``, and
    the definitions file by its ``workspace.yaml`` entry's ``location_name``."""
    from dagster import AssetKey
    from dagster_dbt.asset_utils import DAGSTER_DBT_UNIQUE_ID_METADATA_KEY

    sys.dont_write_bytecode = True
    source = shop / "dagster" / "analytics" / "src"
    sys.path.insert(0, str(source))
    try:
        project = importlib.import_module("shop_analytics.definitions")
        analytics = project.defs.load_fn()
    finally:
        sys.path.remove(str(source))
    path = shop / "dagster" / "operations" / "definitions.py"
    spec = importlib.util.spec_from_file_location("operations_definitions", path)
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    operations = module.defs

    def dbt_nodes(definition: Any) -> list[str]:
        return sorted(
            spec.metadata[DAGSTER_DBT_UNIQUE_ID_METADATA_KEY]
            for spec in definition.specs
            if DAGSTER_DBT_UNIQUE_ID_METADATA_KEY in spec.metadata
        )

    # Keys are global: an asset in one location can read a dbt asset of the other
    uid_of = {
        spec.key: spec.metadata[DAGSTER_DBT_UNIQUE_ID_METADATA_KEY]
        for defs in (analytics, operations)
        for definition in defs.assets or ()
        for spec in getattr(definition, "specs", ())
        if DAGSTER_DBT_UNIQUE_ID_METADATA_KEY in spec.metadata
    }
    found: dict[str, Any] = {}
    [component] = analytics.assets
    found["shop-analytics: definition component"] = dbt_nodes(component)
    graph = operations.resolve_asset_graph()
    for definition in operations.assets:
        nodes = dbt_nodes(definition)
        if nodes:
            found[f"operations: definition {definition.node_def.name}"] = nodes
            continue
        for key in definition.keys:
            parents = graph.get(key).parent_keys
            read = sorted(uid_of[parent] for parent in parents if parent in uid_of)
            if read:  # an asset that reads no dbt node is left out, by dagster's lights too
                found[f"operations: asset {key.to_user_string()}"] = read
    jobs = {}
    for job in operations.jobs:
        keys = job.selection.resolve(graph)
        jobs[job.name] = sorted(uid_of[key] for key in keys if key in uid_of)
        found[f"operations: job {job.name}"] = jobs[job.name]
    for schedule in operations.schedules:
        found[f"operations: schedule {schedule.name}"] = {
            "nodes": jobs[schedule.job_name],
            "job": schedule.job_name,
            "cron": schedule.cron_schedule,
        }
    assert AssetKey("customer_revenue") in uid_of, "the shop's marts are not dagster's assets"
    return found


def reader_fixture(shop: Path) -> dict[str, Any]:
    """What the reader finds in the fixture's code locations, labelled as ``dagster_fixture``."""
    from lineagepilot_core.readers.dagster.scan import scan_dagster

    scan = scan_dagster(shop, shop, ["dagster"])
    found: dict[str, Any] = {}
    for kind in ("definitions", "jobs", "schedules", "assets"):
        for record in scan[kind]:
            name = "component" if record.get("api") == "DbtProjectComponent" else record["name"]
            label = f"{record['location']}: {kind[:-1]} {name}"
            nodes = record["nodes"] if record["status"] == "resolved" else record["reason"]
            if kind == "schedules":
                nodes = {"nodes": nodes, "job": record.get("job_name"), "cron": record.get("cron")}
            found[label] = nodes
    return found


def compare_fixture(theirs: dict[str, Any], ours: dict[str, Any]) -> list[str]:
    problems = [
        f"{label}: dagster {theirs.get(label)}, the reader {ours.get(label)}"
        for label in sorted(set(theirs) | set(ours))
        if theirs.get(label) != ours.get(label)
    ]
    kinds = {label.split(": ")[1].split(" ")[0] for label in theirs}
    if kinds != {"definition", "job", "schedule", "asset"}:
        problems.append(f"the fixture should have each kind of row, and has {sorted(kinds)}")
    return problems


def dagster_answers(label: str, raw: dict[str, Any]) -> list[dict[str, Any]]:
    """dagster-dbt's key for each node, by the translator and by @dbt_assets, and its group."""
    from dagster_dbt import DagsterDbtTranslator, dbt_assets
    from dagster_dbt.asset_utils import DAGSTER_DBT_UNIQUE_ID_METADATA_KEY

    @dbt_assets(manifest=raw)
    def everything(context):  # pragma: no cover - never run
        yield from ()

    made = {
        spec.metadata[DAGSTER_DBT_UNIQUE_ID_METADATA_KEY]: list(spec.key.path)
        for spec in everything.specs
    }
    translator = DagsterDbtTranslator()
    found = []
    for section in ("nodes", "sources"):
        for uid, props in sorted(raw[section].items()):
            if props["resource_type"] not in (*KINDS, "source"):
                continue
            node = {key: props.get(key) for key in FIELDS}
            config = props.get("config") or {}
            node["config"] = {key: config[key] for key in CONFIG_FIELDS if key in config}
            found.append(
                {
                    "manifest": label,
                    "unique_id": uid,
                    "node": node,
                    "translator": list(translator.get_asset_key(props).path),
                    "dbt_assets": made.get(uid),
                    "group": translator.get_group_name(props) or "default",
                }
            )
    return found


def compare(cases: list[dict[str, Any]]) -> list[str]:
    """Where the reader's key or group differs from dagster-dbt's, from the recorded node fields
    alone."""
    problems = []
    for case in cases:
        uid = case["unique_id"]
        variant = case["manifest"].endswith("variants")
        control = CONTROLS.get(uid) if variant else None
        if control is not None and case["translator"] != control:
            problems.append(f"control {uid}: dagster-dbt gave {case['translator']}")
        group_control = GROUP_CONTROLS.get(uid) if variant else None
        if group_control is not None and case["group"] != group_control:
            problems.append(f"group control {uid}: dagster-dbt gave {case['group']}")
        manifest = manifest_from_dict({"nodes": {uid: case["node"]}, "metadata": {}})
        node = manifest.nodes[uid]
        ours = default_key(node)
        for how in ("translator", "dbt_assets"):
            theirs = case[how]
            if theirs is not None and ours != theirs:
                problems.append(f"{case['manifest']} {uid} {how}: {theirs}, the reader {ours}")
        if case["group"] != default_group(node):
            problems.append(
                f"{case['manifest']} {uid} group: {case['group']}, the reader {default_group(node)}"
            )
    return problems


def compare_jobs(theirs: dict[str, list[str]], ours: dict[str, Any]) -> list[str]:
    problems = []
    for position, (label, selection) in enumerate(JOBS):
        name = job_name(position)
        if theirs[name] != ours[name]:
            problems.append(
                f"job {label!r} ({selection}): dagster {theirs[name]}, the reader {ours[name]}"
            )
    for key, source in DOWNSTREAM:
        if theirs[key] != ours[key]:
            shown = source.split("\n")[0] or key
            problems.append(
                f"asset {key!r} ({shown}): dagster {theirs[key]}, the reader {ours[key]}"
            )
    if len({tuple(nodes) for nodes in theirs.values()}) < len(JOBS) // 3:
        problems.append("the jobs select too few different sets of nodes to test anything")
    return problems


def main() -> int:
    import dagster
    import dagster_dbt

    cases = [case for label, raw in manifests().items() for case in dagster_answers(label, raw)]
    problems = compare(cases)
    made = sum(1 for case in cases if case["dbt_assets"] is not None)
    with tempfile.TemporaryDirectory() as work:
        folder = Path(work)
        project = write_workspace(folder)
        theirs = dagster_jobs(folder)
        job_problems = compare_jobs(theirs, reader_jobs(folder, project))
    with tempfile.TemporaryDirectory() as work:
        shop = fixture_copy(Path(work))
        fixture = dagster_fixture(shop)
        fixture_problems = compare_fixture(fixture, reader_fixture(shop))
    sys.stdout.write(
        f"dagster {dagster.__version__}, dagster-dbt {dagster_dbt.__version__}: "
        f"{len(cases) - len(problems)} of {len(cases)} keys and groups agree "
        f"({made} made by @dbt_assets); "
        f"{len(JOBS) + len(DOWNSTREAM) - len(job_problems)} of {len(JOBS) + len(DOWNSTREAM)} "
        "jobs and downstream assets select the same nodes; "
        f"the acceptance fixture's {len(fixture)} definitions, jobs, schedules and assets "
        + ("agree\n" if not fixture_problems else "do not all agree\n")
    )
    problems += fixture_problems
    for problem in problems + job_problems:
        sys.stdout.write(f"  {problem}\n")
    if "--record" in sys.argv:
        record = Path(sys.argv[sys.argv.index("--record") + 1])
        payload = {
            "dagster": dagster.__version__,
            "dagster_dbt": dagster_dbt.__version__,
            "cases": cases,
            "jobs": [
                {"label": label, "selection": selection, "nodes": theirs[job_name(i)]}
                for i, (label, selection) in enumerate(JOBS)
            ],
            "assets": [{"key": key, "nodes": theirs[key]} for key, _ in DOWNSTREAM],
            "fixture": fixture,
        }
        record.parent.mkdir(parents=True, exist_ok=True)
        record.write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8", newline="\n")
    return 1 if problems or job_problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
