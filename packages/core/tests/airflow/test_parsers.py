"""The shell-command parser, manifest loading and the stand-in manifest."""

from __future__ import annotations

import json

import pytest
from airflow_support import FIXTURES, MINI, SHOP

from lineagepilot_core.airflow.manifest import (
    ManifestError,
    load_dbt_ls_file,
    load_manifest,
    manifest_from_dict,
)
from lineagepilot_core.airflow.projects import ProjectIndex, _drift
from lineagepilot_core.airflow.shell import (
    find_dbt_in_argv,
    find_dbt_in_shell,
    mentions_dbt,
    parse_dbt_argv,
    tokenize_shell,
)
from lineagepilot_core.airflow.standin import build_standin


def test_shell_cd_and_multiple_commands():
    invs, err = find_dbt_in_shell(
        "cd /dbt && dbt run -s a b --exclude c; FOO=1 dbt test --select=x --target prod"
    )
    assert err is None
    assert [i.command for i in invs] == ["run", "test"]
    assert invs[0].select == ["a", "b"] and invs[0].exclude == ["c"] and invs[0].cwd == "/dbt"
    assert invs[1].select == ["x"] and invs[1].target == "prod" and invs[1].cwd == "/dbt"


def test_shell_global_flags_and_wrappers():
    invs, _ = find_dbt_in_shell(
        "source venv/bin/activate && uv run dbt --log-level debug --no-write-json build --selector nightly "
        "--vars '{\"a\": 1}' --profiles-dir /p --project-dir=/proj -t dev"
    )
    (inv,) = invs
    assert (
        inv.command == "build"
        and inv.selector == "nightly"
        and inv.profiles_dir == "/p"
        and inv.project_dir == "/proj"
    )
    assert inv.target == "dev" and inv.to_dict()["dbt_command"] == "build"
    invs, _ = find_dbt_in_shell(
        "python -m dbt seed\n/opt/venv/bin/dbt docs generate --project-dir x"
    )
    assert [i.command for i in invs] == ["seed", "docs generate"]
    invs, _ = find_dbt_in_shell("bash -c 'dbt run -m my_model'")
    assert invs[0].select == ["my_model"] and invs[0].used_models_flag


def test_shell_jinja_makes_fields_unknown():
    invs, _ = find_dbt_in_shell(
        "cd {{ params.dir }} && dbt run --select {{ params.sel }} --target {{ var.value.t }}"
    )
    (inv,) = invs
    assert inv.cwd is None and "select" in inv.unknown_fields and "target" in inv.unknown_fields
    inv = parse_dbt_argv(["{{ cmd }}"], jinja=[])
    assert inv.command == "unknown"


def test_shell_relative_cd_and_untokenisable():
    invs, _ = find_dbt_in_shell("cd /a && cd b && dbt ls")
    assert invs[0].cwd == "/a/b"
    assert tokenize_shell("echo 'unterminated") is None
    assert find_dbt_in_shell("dbt run 'unterminated") == (
        [],
        "shell command mentions dbt but could not be tokenised",
    )
    assert find_dbt_in_shell("echo 'unterminated") == ([], None)
    assert find_dbt_in_shell("echo hi") == ([], None)


def test_argv_forms():
    assert find_dbt_in_argv([]) == []
    assert find_dbt_in_argv(["dbt", "run", "--select", "a"])[0].select == ["a"]
    assert find_dbt_in_argv(["/bin/sh", "-c", "dbt test"])[0].command == "test"
    assert find_dbt_in_argv(["dbt run --select a"])[0].select == ["a"]
    assert find_dbt_in_argv(["python", "script.py"]) == []
    inv = parse_dbt_argv(["run", "--select"])
    assert inv.select == []
    inv = parse_dbt_argv(["run", "--target"])
    assert inv.target is None
    assert mentions_dbt("cd x && dbt run") and not mentions_dbt("dbtx run")


# --- manifest ----------------------------------------------------------------------------------
def test_manifest_errors(tmp_path):
    with pytest.raises(ManifestError):
        load_manifest(str(tmp_path / "missing.json"))
    p = tmp_path / "bad.json"
    p.write_text("{not json")
    with pytest.raises(ManifestError):
        load_manifest(str(p))
    p.write_text(json.dumps({"nope": 1}))
    with pytest.raises(ManifestError):
        load_manifest(str(p))
    p.write_text(json.dumps({"nodes": {}}))
    with pytest.raises(ManifestError):
        load_manifest(str(p), size_cap=1)


def test_old_manifest_without_project_name():
    m = manifest_from_dict(
        {
            "metadata": {},
            "nodes": {
                "model.jaffle.a": {
                    "resource_type": "model",
                    "tags": "single",
                    "fqn": ["jaffle", "a"],
                    "original_file_path": "models/a.sql",
                },
                "seed.jaffle.s": {"original_file_path": "data/s.csv"},
            },
        }
    )
    assert m.project_name == "jaffle"
    assert m.nodes["model.jaffle.a"].tags == ["single"]
    assert m.nodes["seed.jaffle.s"].resource_type == "seed"


def test_dbt_ls_file(tmp_path):
    p = tmp_path / "ls.txt"
    p.write_text(
        'log line\n{"unique_id": "model.p.a", "resource_type": "model"}\n{bad json\n{"no_id": 1}\n'
    )
    assert [r["unique_id"] for r in load_dbt_ls_file(str(p))] == ["model.p.a"]
    with pytest.raises(ManifestError):
        load_dbt_ls_file(str(p), size_cap=1)


# --- stand-in -----------------------------------------------------------------------------------
def test_standin_matches_real_manifest_on_shop(shop_manifest):
    st = build_standin(str(SHOP))
    kinds = ("model", "seed", "snapshot", "source")
    real = {u: n for u, n in shop_manifest.nodes.items() if n.resource_type in kinds}
    fake = {u: n for u, n in st.nodes.items() if n.resource_type in kinds}
    assert real.keys() == fake.keys()
    for uid, n in real.items():
        assert sorted(n.depends_on) == sorted(fake[uid].depends_on), uid
        assert sorted(n.tags) == sorted(fake[uid].tags), uid
        assert n.config.get("materialized") == fake[uid].config.get("materialized"), uid


def test_standin_mini_project_features():
    st = build_standin(str(MINI))
    models = {u for u, n in st.nodes.items() if n.resource_type == "model"}
    assert models == {
        "model.mini.base",
        "model.mini.wide",
        "model.mini.report.v1",
        "model.mini.report.v2",
        "model.mini.dyn",
        "model.mini.scored",
    }  # legacy/old.sql is disabled by folder config
    wide = st.nodes["model.mini.wide"]
    assert wide.materialized == "incremental" and set(wide.tags) == {
        "core",
        "hourly",
        "pii",
        "wide_cfg",
    }
    assert sorted(wide.depends_on) == ["model.mini.base", "seed.mini.lookup"]
    assert (
        st.nodes["model.mini.report.v2"].original_file_path == "models/core/report.sql"
    )  # latest in unsuffixed file
    assert st.nodes["model.mini.scored"].depends_on == ["model.mini.report.v2"]
    assert st.nodes["snapshot.mini.snap_base"].depends_on == ["model.mini.base"]
    assert st.nodes["source.mini.raw.events"].tags == ["raw_events"]
    tests = [n for n in st.nodes.values() if n.resource_type == "test"]
    assert any(
        set(t.depends_on) == {"model.mini.wide", "model.mini.base"} for t in tests
    )  # relationships test
    notes = " ".join(st.notes)
    assert (
        "YAML not parsed" in notes
        and "non-literal ref()" in notes
        and "other_pkg.shared" in notes
        and "packages" in notes
    )


def test_project_index_and_drift(tmp_path, shop_manifest):
    idx = ProjectIndex(str(FIXTURES))
    assert {p.rsplit("/", 2)[-1] for p in idx.projects} == {"shop", "mini"}
    m = idx.match("/usr/local/airflow/dbt/shop")
    assert m.path and m.path.endswith("dbt/shop") and m.conf == "heuristic" and "last 2" in m.note
    assert idx.match("/nowhere/at/all").path is None
    assert idx.only_project() is None
    (tmp_path / "a" / "x").mkdir(parents=True)
    (tmp_path / "b" / "x").mkdir(parents=True)
    (tmp_path / "a" / "x" / "dbt_project.yml").write_text("name: x\n")
    (tmp_path / "b" / "x" / "dbt_project.yml").write_text("name: x\n")
    amb = ProjectIndex(str(tmp_path)).match("/image/x")
    assert amb.path is None and "ambiguous" in amb.reason
    proj = tmp_path / "p"
    (proj / "models").mkdir(parents=True)
    (proj / "models" / "new_model.sql").write_text("select 1")
    notes = _drift("p", str(proj), shop_manifest)
    assert any("not in it" in n for n in notes) and any("no file" in n for n in notes)


def test_project_index_manifest_choices(tmp_path):
    idx = ProjectIndex(str(FIXTURES))
    assert idx.manifest_for(str(SHOP.resolve())).kind == "target"
    assert idx.manifest_for(str(MINI.resolve())).kind == "stand_in"
    assert idx.load_declared("/etc/passwd").manifest is None
    bad = tmp_path / "ws"
    (bad / "proj" / "target").mkdir(parents=True)
    (bad / "proj" / "dbt_project.yml").write_text("name: proj\n")
    (bad / "proj" / "target" / "manifest.json").write_text("{broken")
    (bad / "other").mkdir()
    (bad / "other" / "manifest.json").write_text("{broken")
    bidx = ProjectIndex(str(bad), {str(bad / "other"): str(bad / "other" / "manifest.json")})
    assert bidx.manifest_for(str((bad / "proj").resolve())).manifest is None
    assert bidx.load_declared(str(bad / "proj" / "target" / "manifest.json")).manifest is None
    assert bidx.manifest_for(str((bad / "other").resolve())).manifest is None
