"""BashOperator / KubernetesPodOperator / DockerOperator / @task.bash dbt calls and task dependencies."""

from __future__ import annotations

from airflow_support import find_link, short


def test_cd_select_exclude_and_env_target(fixture_scan):
    link = find_link(fixture_scan, "shell_dag", "cd_run")
    assert link["kind"] == "bash" and link["dbt_command"] == "run"
    assert (
        link["select"] == ["tag:daily"]
        and link["exclude"] == ["fct_revenue"]
        and link["target"] == "dev"
    )
    assert short(link["resolved_models"]) == ["fct_orders", "fin_daily_summary"]
    assert link["confidence"] == "heuristic"  # DBT_TARGET default


def test_multiple_commands_in_one_string(fixture_scan):
    seed = find_link(fixture_scan, "shell_dag", "multi", 0)
    build = find_link(fixture_scan, "shell_dag", "multi", 1)
    test = find_link(fixture_scan, "shell_dag", "multi", 2)
    assert (seed["dbt_command"], build["dbt_command"], test["dbt_command"]) == (
        "seed",
        "build",
        "test",
    )
    assert short(build["resolved_models"]) == ["int_customer_orders", "stg_customers"]
    assert test["select"] == ["dim_customers"]
    assert short(test["resolved_models"]) == ["dim_customers"]


def test_selector_jinja_relative_env_and_cwd(fixture_scan):
    sel = find_link(fixture_scan, "shell_dag", "selector")
    assert sel["selector"] == "core_not_python"
    assert short(sel["resolved_models"]) == [
        "customer_ltv",
        "dim_customers",
        "dim_products",
        "py_scores",
    ]
    jinja = find_link(fixture_scan, "shell_dag", "jinja")
    assert (
        jinja["status"] == "unknown"
        and jinja["resolved_models"] is None
        and jinja["partial_models"]
    )
    rel = find_link(fixture_scan, "shell_dag", "relative")
    assert rel["project_path"] == "dbt/shop" and short(rel["resolved_models"]) == ["stg_payments"]
    env = find_link(fixture_scan, "shell_dag", "env_dir")
    assert env["dbt_command"] == "snapshot" and any("DBT_PROJECT_DIR" in n for n in env["notes"])
    cwd = find_link(fixture_scan, "shell_dag", "with_cwd")
    assert cwd["project_path"] == "dbt/shop" and "model.shop.fct_orders" in cwd["resolved_models"]
    deps = find_link(fixture_scan, "shell_dag", "deps")
    assert deps["in_scope"] is False


def test_pod_and_docker(fixture_scan):
    pod = find_link(fixture_scan, "shell_dag", "pod")
    assert (
        pod["kind"] == "pod"
        and pod["dbt_command"] == "build"
        and short(pod["resolved_models"]) == ["fct_orders"]
    )
    pod_sh = find_link(fixture_scan, "shell_dag", "pod_sh")
    assert short(pod_sh["resolved_models"]) == ["stg_orders"]
    entry = find_link(fixture_scan, "shell_dag", "pod_entry")
    assert entry["confidence"] == "heuristic" and any("entrypoint" in n for n in entry["notes"])
    docker = find_link(fixture_scan, "shell_dag", "docker")
    assert short(docker["resolved_models"]) == ["stg_products"]
    docker_list = find_link(fixture_scan, "shell_dag", "docker_list")
    assert docker_list["dbt_command"] == "seed"


def test_task_bash_decorator_and_python_dbt(fixture_scan):
    link = find_link(fixture_scan, "decorated_dag", "bash_task")
    assert link["kind"] == "task_bash" and short(link["resolved_models"]) == ["stg_orders"]
    assert any(u.get("category") == "python_dbt_call" for u in fixture_scan["unresolved"])
    assert any(d["dag_id"] == "decorated_dag" and d["how"] == "@dag" for d in fixture_scan["dags"])


def test_positional_dag_and_dag_kwarg(fixture_scan):
    link = find_link(fixture_scan, "positional_dag", "attached")
    assert short(link["resolved_models"]) == ["fct_orders"]


def test_unresolvable_shell_commands_are_reported(fixture_scan):
    reasons = {
        u["reason"] for u in fixture_scan["unresolved"] if u["file"].endswith("shell_patterns.py")
    }
    assert any("script" in r for r in reasons)
    assert any("CMD" in r for r in reasons)
    assert not any(l.get("task_id") == "not_dbt" for l in fixture_scan["links"])


def edges(result, dag_id):
    return {
        (e["upstream"].split(".", 1)[1], e["downstream"].split(".", 1)[1])
        for e in result["dependencies"]
        if e["dag_id"] == dag_id and e["upstream"] and e["downstream"]
    }


def test_dependencies_shift_lists_chain_and_set_methods(fixture_scan):
    got = edges(fixture_scan, "shell_dag")
    assert {("start", "cd_run"), ("cd_run", "multi"), ("selector", "start")} <= got
    # chain(start, [jinja, relative], [env_dir, with_cwd], pod): pairwise between equal-length lists
    assert {
        ("start", "jinja"),
        ("start", "relative"),
        ("jinja", "env_dir"),
        ("relative", "with_cwd"),
        ("env_dir", "pod"),
        ("with_cwd", "pod"),
    } <= got
    assert ("jinja", "with_cwd") not in got
    assert {("pod", "pod_sh"), ("docker", "pod_sh")} <= got
    ops = edges(fixture_scan, "cosmos_ops")
    assert {
        ("seed", "run"),
        ("run", "test"),
        ("run", "build"),
        ("snap", "docs"),
        ("seed", "mapped"),
    } <= ops
    groups = edges(fixture_scan, "cosmos_groups")
    assert {
        ("outer.inner", "dbt_task_group"),
        ("none_tests", "remote"),
        ("normalized", "remote"),
    } <= groups
    assert all(
        e["edge"] == "task_depends_on" and e["origin"] == "dag_static"
        for e in fixture_scan["dependencies"]
    )
    assert edges(fixture_scan, "decorated_dag") == {("bash_task", "py_task")}
