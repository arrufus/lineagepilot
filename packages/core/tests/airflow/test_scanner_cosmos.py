"""Cosmos DbtDag / DbtTaskGroup / single operators, and the predicted Airflow task ids."""

from __future__ import annotations

from airflow_support import find_link, short


def task_ids(link):
    return sorted(t for row in link["tasks"] for t in row["task_ids"])


def test_dbtdag_positional_dag_id_and_project(fixture_scan):
    link = find_link(fixture_scan, "cosmos_positional")
    assert (
        link["kind"] == "cosmos_dag"
        and link["origin"] == "dag_static"
        and link["status"] == "resolved"
    )
    assert link["project_path"] == "dbt/shop" and link["manifest"]["kind"] == "target"
    assert short(link["resolved_models"]) == [
        "fct_orders",
        "fin_daily_summary",
    ]  # tag:finance minus fct_revenue
    assert link["select"] == ["tag:finance"] and link["exclude"] == ["fct_revenue"]
    assert link["confidence"] == "inferred"
    assert link["location"] == {"file": "dags/cosmos_patterns.py", "line": 15, "col": 13}
    # after_each: models with tests become <name>.run/<name>.test groups
    assert task_ids(link) == ["fct_orders.run", "fct_orders.test", "fin_daily_summary_run"]
    assert link["task_id_pattern"].startswith("<name>.run")


def test_env_default_is_heuristic_and_build_ids(fixture_scan):
    link = find_link(fixture_scan, "cosmos_build")
    assert link["confidence"] == "heuristic"
    assert any("SHOP_DIR" in n for n in link["notes"])
    assert short(link["resolved_models"]) == [
        "customer_ltv",
        "fct_orders",
        "fct_revenue.v1",
        "fct_revenue.v2",
        "fin_daily_summary",
        "rpt_weekly",
    ]
    assert "fct_revenue_v1_model_build" in task_ids(link)  # versioned name: '.' -> '_'
    assert link["selector"] == "finance_and_children"


def test_manifest_path_after_all_and_watcher(fixture_scan):
    link = find_link(fixture_scan, "cosmos_manifest")
    assert link["manifest"]["kind"] == "dag_declared" and link["project_path"] == "dbt/shop"
    assert short(link["resolved_models"]) == [
        "stg_customers",
        "stg_orders",
        "stg_payments",
        "stg_products",
    ]
    extra = [x["task_id"] for x in link["extra_tasks"]]
    assert extra == ["shop_test", "dbt_producer_watcher"]
    assert "stg_orders_run" in task_ids(link)


def test_task_group_prefixes_and_defaults(fixture_scan):
    inner = find_link(fixture_scan, "cosmos_groups", "outer.inner")
    assert task_ids(inner) == [
        "outer.inner.stg_customers.run",
        "outer.inner.stg_customers.test",
        "outer.inner.stg_orders.run",
        "outer.inner.stg_orders.test",
    ]
    dflt = find_link(fixture_scan, "cosmos_groups", "dbt_task_group")
    assert any("dbt_task_group" in n for n in dflt["notes"])
    ids = task_ids(dflt)
    assert "dbt_task_group.models.marts.core.dim_products_run" in ids  # group_nodes_by_folder
    assert not any("seed" in i for i in ids)  # seed rendering NONE
    ephemeral = [r for r in dflt["tasks"] if r["unique_id"].endswith("int_order_items")]
    assert ephemeral[0]["operator"].startswith("EmptyOperator")
    none_tests = find_link(fixture_scan, "cosmos_groups", "none_tests")
    assert task_ids(none_tests) == ["none_tests.int_order_items_run"]


def test_detached_multi_parent_tests(fixture_scan):
    link = find_link(fixture_scan, "cosmos_groups", "detached")
    extra = [x["task_id"] for x in link["extra_tasks"]]
    assert (
        len(extra) == 1
        and extra[0].startswith("detached.relationships_fct_orders_customer_id")
        and extra[0].endswith("_test")
    )
    # fct_orders keeps its single-parent not_null test, so it is still a group
    assert "detached.fct_orders.test" in task_ids(link)


def test_normalize_task_id_makes_task_ids_unknown_not_models(fixture_scan):
    link = find_link(fixture_scan, "cosmos_groups", "normalized")
    assert link["status"] == "resolved" and link["tasks"] is None
    assert any("normalize_task_id" in n for n in link["notes"])


def test_unknown_cosmos_cases(fixture_scan):
    remote = find_link(fixture_scan, "cosmos_groups", "remote")
    assert remote["status"] == "unknown" and "remote" in remote["unknown_reasons"][0]
    unsupported = find_link(fixture_scan, "cosmos_groups", "unsupported")
    assert unsupported["status"] == "unknown" and unsupported["resolved_models"] is None
    assert "state" in unsupported["unknown_reasons"][0]
    templated = find_link(fixture_scan, "cosmos_groups", "templated")
    assert templated["status"] == "unknown" and templated["resolved_models"] is None


def test_legacy_container_and_standin(fixture_scan):
    legacy = find_link(fixture_scan, "cosmos_legacy")
    assert short(legacy["resolved_models"]) == [
        "customer_ltv",
        "dim_customers",
        "dim_products",
        "py_scores",
    ]
    assert legacy["confidence"] == "heuristic"
    container = find_link(fixture_scan, "cosmos_container")
    assert container["project_path"] == "dbt/shop" and any(
        "last 2 path component" in n for n in container["notes"]
    )
    mini = find_link(fixture_scan, "cosmos_mini")
    assert mini["manifest"]["kind"] == "stand_in" and mini["manifest"]["approximate"] is True
    assert short(mini["resolved_models"]) == [
        "base",
        "dyn",
        "report.v1",
        "report.v2",
        "scored",
        "wide",
    ]


def test_cosmos_single_operators(fixture_scan):
    run = find_link(fixture_scan, "cosmos_ops", "run")
    assert run["operator"] == "DbtRunLocalOperator" and run["dbt_command"] == "run"
    assert short(run["resolved_models"]) == [
        "fct_orders",
        "int_order_items",
        "stg_orders",
        "stg_products",
    ]
    test = find_link(fixture_scan, "cosmos_ops", "test")
    assert short(test["resolved_models"]) == ["dim_customers", "fct_orders"]
    seed = find_link(fixture_scan, "cosmos_ops", "seed")
    assert seed["resolved_models"] == [] and seed["resolved_nodes"] == ["seed.shop.products"]
    build = find_link(fixture_scan, "cosmos_ops", "build")
    assert "model.shop.rpt_weekly" in build["resolved_models"]
    docs = find_link(fixture_scan, "cosmos_ops", "docs")
    assert docs["in_scope"] is False and docs["dbt_command"] == "docs generate"
    snap = find_link(fixture_scan, "cosmos_ops", "snap")
    assert snap["resolved_nodes"] == ["snapshot.shop.snap_customers"]
    mapped = find_link(fixture_scan, "cosmos_ops", "mapped")
    assert short(mapped["resolved_models"]) == ["stg_customers", "stg_orders"]
    assert any("mapping" in n for n in mapped["notes"])


def test_cross_module_constants(fixture_scan):
    link = find_link(fixture_scan, "cross_module")
    assert link["project_path"] == "dbt/shop"
    assert short(link["resolved_models"]) == [
        "customer_ltv",
        "dim_customers",
        "dim_products",
        "py_scores",
        "stg_orders",
    ]
    assert any("include/constants.py" in n for n in link["notes"])
    attr = find_link(fixture_scan, "cross_module_attr")
    assert short(attr["resolved_models"]) == ["dim_products"]  # relative import + module attribute


def test_wrappers_are_flagged(fixture_scan):
    reasons = [
        u["reason"] for u in fixture_scan["unresolved"] if u["file"].endswith("uses_constants.py")
    ]
    assert any("DbtWrapper" in r and "include/dbt_wrapper.py" in r for r in reasons)
    assert any("some_lib_call" in r for r in reasons)


def test_folder_groups_collapse_like_cosmos_tests():
    """Mirrors cosmos tests/airflow/test_graph.py::test_build_airflow_graph_with_after_each (read as text)."""
    from lineagepilot_core.airflow.cosmos import CosmosSpec, expand_tasks
    from lineagepilot_core.airflow.manifest import manifest_from_dict

    def node(uid, path, deps=()):
        return {
            "resource_type": uid.split(".")[0],
            "original_file_path": path,
            "depends_on": {"nodes": list(deps)},
            "fqn": ["astro_shop", uid.split(".")[-1]],
            "config": {"materialized": "view"},
        }

    m = manifest_from_dict(
        {
            "metadata": {"project_name": "astro_shop"},
            "nodes": {
                "seed.astro_shop.seed_parent": node("seed.astro_shop.seed_parent", "."),
                "model.astro_shop.parent": node(
                    "model.astro_shop.parent",
                    "gen2/models/parent.sql",
                    ["seed.astro_shop.seed_parent"],
                ),
                "test.astro_shop.test_parent": node(
                    "test.astro_shop.test_parent", ".", ["model.astro_shop.parent"]
                ),
                "model.astro_shop.child": node(
                    "model.astro_shop.child", "gen3/models/child.sql", ["model.astro_shop.parent"]
                ),
                "model.astro_shop.child2.v2": node(
                    "model.astro_shop.child2.v2",
                    "gen3/models/child2_v2.sql",
                    ["model.astro_shop.parent"],
                ),
            },
        }
    )
    selected = sorted(u for u in m.nodes if not u.startswith("test."))
    rows, extra, why = expand_tasks(
        m, selected, CosmosSpec(kind="dag", group_nodes_by_folder=True), "", set(), "astro_shop"
    )
    got = sorted(t for r in rows for t in r["task_ids"])
    # Cosmos' own expected ids: same-named "models" folders collapse into the first group (gen2.models)
    assert got == sorted(
        [
            "seed_parent_seed",
            "gen2.models.parent.run",
            "gen2.models.parent.test",
            "gen2.models.child_run",
            "gen2.models.child2_v2_run",
        ]
    )
    rows, extra, _ = expand_tasks(
        m, selected, CosmosSpec(kind="dag", test_behavior="after_all"), "", set(), "astro_shop"
    )
    assert extra[-1]["task_id"] == "astro_shop_test"
    rows, _, _ = expand_tasks(
        m,
        ["model.astro_shop.parent"],
        CosmosSpec(kind="dag", test_behavior="build"),
        "",
        set(),
        "astro_shop",
    )
    assert rows[0]["task_ids"] == ["parent_model_build"]
    assert expand_tasks(m, [], CosmosSpec(kind="dag", test_behavior="weird"), "", set(), "x")[2]
