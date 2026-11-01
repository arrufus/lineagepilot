"""The CLI and the ground-truth scorer."""
from __future__ import annotations

import json

from lineagepilot_spike_dag.cli import _git_root, main
from lineagepilot_spike_dag.evaluate import evaluate, expected_models, review_unresolved

from .conftest import FIXTURES, SHOP


def test_cli_scan_and_select(tmp_path, capsys):
    out = tmp_path / "scan.json"
    assert main(["scan", str(FIXTURES / "dags" / "cosmos_operators.py"), "--workspace", str(FIXTURES), "-o", str(out),
                 "--manifest", str(SHOP / "target" / "manifest.json")]) == 0
    data = json.loads(out.read_text())
    assert data["stats"]["links"] == 7 and data["links"][0]["origin"] == "dag_static"
    assert data["links"][0]["manifest"]["kind"] == "cli"
    assert main(["select", str(SHOP / "target" / "manifest.json"), "-s", "tag:core", "--exclude", "py_scores"]) == 0
    got = json.loads(capsys.readouterr().out)
    assert got["ids"] == ["model.shop.customer_ltv", "model.shop.dim_customers", "model.shop.dim_products"]
    assert main(["select", str(SHOP / "target" / "manifest.json"), "--selector", "nope"]) == 0
    assert json.loads(capsys.readouterr().out)["ids"] is None
    assert main(["select", str(tmp_path / "missing.json")]) == 2


def test_cli_default_workspace_and_stdout(tmp_path, capsys):
    dag = tmp_path / "d.py"
    dag.write_text("from airflow import DAG\nfrom airflow.operators.bash import BashOperator\n"
                   "with DAG('x') as dag:\n    BashOperator(task_id='t', bash_command='dbt run')\n")
    assert main(["scan", str(tmp_path)]) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["links"][0]["status"] == "unknown"  # no dbt project anywhere in the workspace
    assert _git_root(str(dag)) is None or isinstance(_git_root(str(dag)), str)


def test_cli_manifest_fallback_without_project(tmp_path, capsys):
    dag = tmp_path / "d.py"
    dag.write_text("from airflow import DAG\nfrom airflow.operators.bash import BashOperator\n"
                   "with DAG('x') as dag:\n    BashOperator(task_id='t', bash_command='dbt run --select stg_orders')\n")
    assert main(["scan", str(dag), "--workspace", str(tmp_path), "--manifest", str(SHOP / "target" / "manifest.json")]) == 0
    link = json.loads(capsys.readouterr().out)["links"][0]
    assert link["status"] == "resolved" and link["resolved_models"] == ["model.shop.stg_orders"]
    assert any("--manifest" in n for n in link["notes"])


def _link(**kw):
    base = {"origin": "dag_static", "kind": "bash", "location": {"file": "d.py", "line": 1, "col": 0}, "dag_id": "d",
            "task_id": "t", "group_id": None, "dbt_command": "run", "project_path": "p", "status": "resolved",
            "resolved_models": ["model.x.a"], "unknown_reasons": [], "in_scope": True, "confidence": "declared"}
    base.update(kw)
    return base


def _gt(**kw):
    base = {"id": "e", "file": "d.py", "line": 1, "dag_id": "d", "unit": "task", "task_id": "t", "dbt_command": "run",
            "project_path": "p", "expect": "resolved", "package": "x", "models": ["a"]}
    base.update(kw)
    return base


def _score(gt_links, links, unresolved=(), reviews=()):
    scan = {"links": list(links), "unresolved": list(unresolved), "dependencies": [], "stats": {"files_scanned": 1}}
    gt = {"links": gt_links, "unresolved_review": list(reviews)}
    return evaluate(gt, scan), review_unresolved(gt, scan)


def test_evaluate_outcomes():
    ev, _ = _score([_gt()], [_link()])
    assert ev["rows"][0]["outcome"] == "correct" and ev["metrics"]["miss_rate"] == 0
    ev, _ = _score([_gt()], [_link(resolved_models=["model.x.b"])])
    assert ev["rows"][0]["reason"] == "wrong model set" and ev["rows"][0]["missing"] == ["model.x.a"]
    ev, _ = _score([_gt()], [_link(project_path="q")])
    assert ev["rows"][0]["reason"].startswith("wrong project")
    ev, _ = _score([_gt()], [_link(status="unknown", unknown_reasons=["why"])])
    assert ev["rows"][0]["reason"] == "unknown: why"
    ev, _ = _score([_gt()], [_link(task_id="other")])
    assert ev["rows"][0]["reason"] == "found with the wrong identity" and ev["metrics"]["false_positives"] == 0
    ev, _ = _score([_gt()], [], unresolved=[{"file": "d.py", "line": 1, "reason": "flagged"}])
    assert ev["rows"][0]["reason"].startswith("only an unresolved record")
    ev, _ = _score([_gt()], [])
    assert ev["rows"][0]["reason"].startswith("not found") and ev["metrics"]["miss_rate"] == 1.0
    ev, _ = _score([_gt()], [_link(), _link(task_id="extra", location={"file": "d.py", "line": 9, "col": 0})])
    assert ev["metrics"]["false_positives"] == 1


def test_evaluate_expected_unknowns_and_units():
    unk = _gt(expect="unknown", legit=True, unit="dynamic", lines=[5])
    ev, _ = _score([unk], [], unresolved=[{"file": "d.py", "line": 5, "reason": "loop"}])
    assert ev["rows"][0]["outcome"] == "unknown_legit"
    ev, _ = _score([dict(unk, legit=False)], [_link(location={"file": "d.py", "line": 5, "col": 0}, status="unknown",
                                                    unknown_reasons=["x"])])
    assert ev["rows"][0]["outcome"] == "unknown_not_legit"
    ev, _ = _score([unk], [_link(location={"file": "d.py", "line": 5, "col": 0})])
    assert ev["rows"][0]["outcome"] == "miss" and "guess" in ev["rows"][0]["reason"]
    ev, _ = _score([unk], [])
    assert "silently dropped" in ev["rows"][0]["reason"]
    dag = _gt(unit="cosmos_dag", task_ids=["a_run"])
    ev, _ = _score([dag], [_link(kind="cosmos_dag", task_id=None, tasks=[{"task_ids": ["a_run"]}], extra_tasks=[])])
    assert ev["rows"][0]["outcome"] == "correct" and ev["rows"][0]["task_ids_match"]
    grp = _gt(unit="cosmos_task_group", group_id="g", task_ids=["g.a_run"])
    ev, _ = _score([grp], [_link(kind="cosmos_task_group", group_id="g", tasks=None)])
    assert ev["rows"][0]["task_ids_match"] is False
    idx = _gt(command_index=1)
    ev, _ = _score([idx], [_link(command_index=0), _link(command_index=1)])
    assert ev["rows"][0]["outcome"] == "correct"
    assert _score([_gt(unit="weird")], [_link()])[0]["rows"][0]["outcome"] == "miss"
    assert expected_models({"package": "p", "models": ["a", "model.q.b"]}) == ["model.p.a", "model.q.b"]
    _, rv = _score([], [], unresolved=[{"file": "d.py", "line": 2, "reason": "r"}],
                   reviews=[{"file": "d.py", "line": 2, "legit": True, "why": "w"}])
    assert rv[0]["legit"] is True and rv[0]["review"] == "w"
    ev, _ = _score([], [_link(in_scope=False)])
    assert ev["metrics"]["out_of_scope_dbt_links"] == 1 and ev["metrics"]["miss_rate"] is None
