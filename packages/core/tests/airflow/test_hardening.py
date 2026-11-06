"""What a crafted workspace can do to the scanner: nothing but get a wrong answer (design section 10).

These go beyond the spike, whose inputs were real repositories rather than hostile ones.
"""

from __future__ import annotations

import os
import sys
import textwrap
import time
from pathlib import Path

import pytest
from airflow_support import find_link

from lineagepilot_core.airflow import dag_static
from lineagepilot_core.airflow.dag_static import (
    clean_dag_folder,
    discover_dag_files,
    in_dag_folder,
    resolve_dag_folders,
    scan_dags,
)
from lineagepilot_core.airflow.safe_io import is_absolute, read_capped

HEADER = """
from airflow import DAG
from airflow.operators.bash import BashOperator
"""


def write_dag(root: Path, body: str, name: str = "t.py") -> Path:
    (root / "dags").mkdir(exist_ok=True)
    path = root / "dags" / name
    path.write_text(HEADER + textwrap.dedent(body), encoding="utf-8")
    return path


def scan_one(root: Path, body: str):
    write_dag(root, body)
    return scan_dags(str(root), ["dags"])


class TestResourceLimits:
    def test_doubling_a_string_cannot_exhaust_memory(self, tmp_path):
        lines = "\n".join(f"s{i + 1} = s{i} + s{i}" for i in range(60))
        result = scan_one(
            tmp_path,
            f"""
            s0 = "aaaaaaaaaaaaaaaa"
{textwrap.indent(lines, " " * 12)}
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command=s60)
            """,
        )
        assert result["stats"]["files_scanned"] == 1
        assert any("not statically resolvable" in u["reason"] for u in result["unresolved"])

    def test_doubling_a_list_cannot_exhaust_memory(self, tmp_path):
        lines = "\n".join(f"s{i + 1} = s{i} + s{i}" for i in range(60))
        result = scan_one(
            tmp_path,
            f"""
            s0 = ["a", "b"]
{textwrap.indent(lines, " " * 12)}
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="echo")
            """,
        )
        assert result["stats"]["files_scanned"] == 1

    @pytest.mark.parametrize(
        "expression",
        [
            '"%999999999s" % "a"',
            '"%.999999999f" % "1"',
            '"{:>999999999}".format("a")',
            '"{0:>999999999}".format("a")',
            '"a".replace("a", "b" * 2).join(["x"] * 3)',
        ],
    )
    def test_a_format_string_cannot_allocate_a_gigabyte(self, tmp_path, expression):
        started = time.perf_counter()
        result = scan_one(
            tmp_path,
            f"""
            cmd = {expression}
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command=cmd)
            """,
        )
        assert time.perf_counter() - started < 5
        assert result["stats"]["files_scanned"] == 1

    @pytest.mark.parametrize(
        "template",
        ["{0.__class__.__mro__}", "{0.__init__}", "{0[0]}", "{!r}", "{a.b}"],
    )
    def test_a_format_string_cannot_reach_attributes(self, tmp_path, template):
        result = scan_one(
            tmp_path,
            f"""
            cmd = "{template}".format("x", a="y")
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command=cmd)
            """,
        )
        assert not [link for link in result["links"] if "class" in str(link)]
        assert any("not statically resolvable" in u["reason"] for u in result["unresolved"])

    def test_plain_format_strings_still_work(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text("name: p\n")
        (tmp_path / "models").mkdir()
        (tmp_path / "models" / "a.sql").write_text("select 1")
        result = scan_one(
            tmp_path,
            """
            cmd = "dbt {} --select {name}".format("run", name="a")
            old = "dbt %s --select %s" % ("run", "a")
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command=cmd)
                BashOperator(task_id="u", bash_command=old)
            """,
        )
        assert find_link(result, "d", "t")["resolved_models"] == ["model.p.a"]
        assert find_link(result, "d", "u")["resolved_models"] == ["model.p.a"]

    def test_fan_out_stops_at_the_step_budget_and_says_so(self, tmp_path, monkeypatch):
        monkeypatch.setattr(dag_static, "MAX_STEPS", 2000)
        calls = "\n".join("    next_level()" for _ in range(20))
        result = scan_one(
            tmp_path,
            f"""
            def next_level():
                x = "a"
            def level_two():
{textwrap.indent(calls, " " * 12)}
            def level_one():
{textwrap.indent(calls.replace("next_level", "level_two"), " " * 12)}
            def level_zero():
{textwrap.indent(calls.replace("next_level", "level_one"), " " * 12)}
            level_zero()
            """,
        )
        limit = [u for u in result["unresolved"] if u["category"] == "limit"]
        assert limit and "analysis stopped after 2000 steps" in limit[0]["reason"]

    def test_nesting_too_deep_for_the_parser_is_not_a_crash(self, tmp_path):
        result = scan_one(tmp_path, "x = " + "(" * 5000 + "1" + ")" * 5000 + "\n")
        assert result["stats"]["files_scanned"] == 1
        assert result["unresolved"][0]["category"] in ("dag_not_loadable", "limit")

    def test_too_many_dag_files_are_reported(self, tmp_path, monkeypatch):
        monkeypatch.setattr(dag_static, "MAX_DAG_FILES", 2)
        for name in ("a.py", "b.py", "c.py"):
            write_dag(tmp_path, "with DAG('d'): pass", name)
        result = scan_dags(str(tmp_path), ["dags"])
        assert len(result["files"]) == 2
        assert "only the first 2" in result["problems"][0]

    def test_a_bug_in_the_scanner_costs_one_file_and_says_so(self, tmp_path, monkeypatch):
        write_dag(tmp_path, "with DAG('d'): pass")

        def broken(self):
            raise TypeError("a bug")

        monkeypatch.setattr(dag_static.FileScanner, "run", broken)
        result = scan_dags(str(tmp_path), ["dags"])
        assert "the scanner failed on this file: TypeError" in result["unresolved"][0]["reason"]
        assert result["unresolved"][0]["mentions_dbt"] is True


class TestWhatIsRead:
    @pytest.mark.skipif(sys.platform == "win32", reason="needs symlinks and FIFOs")
    def test_a_symlink_to_a_device_or_a_fifo_never_blocks(self, tmp_path):
        (tmp_path / "dags").mkdir()
        os.symlink("/dev/zero", tmp_path / "dags" / "zero_dag.py")
        os.mkfifo(tmp_path / "dags" / "fifo_dag.py")
        (tmp_path / "dags" / "imports.py").write_text(
            "# airflow dag\nfrom zero_dag import x\nfrom fifo_dag import y\nprint(x, y)\n"
        )
        result = scan_dags(str(tmp_path), ["dags"])
        reasons = {u["file"]: u["category"] for u in result["unresolved"]}
        assert reasons["dags/fifo_dag.py"] == "limit" and reasons["dags/zero_dag.py"] == "limit"
        assert all(file.startswith("dags/") for file in result["files"])

    @pytest.mark.skipif(sys.platform == "win32", reason="needs symlinks")
    def test_a_symlink_out_of_the_workspace_is_not_read(self, tmp_path):
        outside = tmp_path / "outside"
        outside.mkdir()
        secret = outside / "secrets_dag.py"
        secret.write_text("# airflow dag\nDAG_ID = 'stolen'\n")
        space = tmp_path / "space"
        (space / "dags").mkdir(parents=True)
        os.symlink(secret, space / "dags" / "linked_dag.py")
        (space / "dags" / "importer.py").write_text(
            "from airflow import DAG\nfrom secrets_dag import DAG_ID\nwith DAG(DAG_ID): pass\n"
        )
        result = scan_dags(str(space), ["dags"])
        assert "stolen" not in str(result)

    def test_read_capped_refuses_what_it_should(self, tmp_path):
        file = tmp_path / "a.txt"
        file.write_text("hello")
        assert read_capped(str(file), 100) == b"hello"
        assert read_capped(str(file), 1) is None
        assert read_capped(str(tmp_path), 100) is None
        assert read_capped(str(tmp_path / "missing"), 100) is None
        assert read_capped(str(file), 100, str(tmp_path / "elsewhere")) is None

    def test_a_folder_setting_cannot_leave_the_workspace(self, tmp_path):
        (tmp_path / "dags").mkdir()
        assert clean_dag_folder("../dags") is None
        assert clean_dag_folder("/etc") is None
        assert clean_dag_folder("C:/dags") is None
        assert clean_dag_folder("") is None
        assert clean_dag_folder(3) is None
        assert clean_dag_folder(".") == "."
        assert clean_dag_folder(r"a\b") == "a/b"
        assert resolve_dag_folders(str(tmp_path), ["../..", "/", "dags"]) == [
            os.path.realpath(tmp_path / "dags")
        ]


class TestDiscovery:
    def test_dags_are_found_at_any_depth_by_default(self, tmp_path):
        for folder in ("dags", "airflow/dags", "a/b/c/dags"):
            (tmp_path / folder).mkdir(parents=True)
        (tmp_path / "models").mkdir()
        found = resolve_dag_folders(str(tmp_path), ["**/dags"])
        assert sorted(Path(f).relative_to(tmp_path.resolve()).as_posix() for f in found) == [
            "a/b/c/dags",
            "airflow/dags",
            "dags",
        ]

    def test_a_dags_folder_in_a_dbt_project_or_a_hidden_folder_does_not_count(self, tmp_path):
        (tmp_path / "project" / "dags").mkdir(parents=True)
        (tmp_path / "project" / "dbt_project.yml").write_text("name: p\n")
        (tmp_path / ".venv" / "dags").mkdir(parents=True)
        (tmp_path / "node_modules" / "dags").mkdir(parents=True)
        assert resolve_dag_folders(str(tmp_path), ["**/dags"]) == []

    @pytest.mark.parametrize(
        ("path", "folders", "expected"),
        [
            ("dags/a.py", ["dags"], True),
            ("dags/sub/a.py", ["dags"], True),
            ("other/a.py", ["dags"], False),
            ("dagsx/a.py", ["dags"], False),
            ("dags/a.py", ["**/dags"], True),
            ("airflow/dags/a.py", ["**/dags"], True),
            ("airflow/dags/sub/a.py", ["**/dags"], True),
            ("airflow/a.py", ["**/dags"], False),
        ],
    )
    def test_in_dag_folder(self, path, folders, expected):
        assert in_dag_folder(path, folders) is expected

    def test_a_single_file_can_be_named(self, tmp_path):
        path = write_dag(tmp_path, "with DAG('d'): pass")
        assert discover_dag_files(str(path))[0] == [str(path)]

    def test_airflowignore_is_read_both_ways(self, tmp_path):
        (tmp_path / "dags").mkdir()
        (tmp_path / "dags" / ".airflowignore").write_text(
            "# a comment\n"
            "^regexp_.*\n"  # regexp syntax, Airflow 2
            "*_glob.py\n"  # glob syntax, Airflow 3
            "!negated\n"
            "nested/*.py\n"
        )
        for name in ("regexp_a.py", "b_glob.py", "kept_dag.py"):
            (tmp_path / "dags" / name).write_text("# airflow dag\n")
        (tmp_path / "dags" / "nested").mkdir()
        (tmp_path / "dags" / "nested" / "c.py").write_text("# airflow dag\n")
        files, skipped, _ = discover_dag_files(str(tmp_path / "dags"))
        assert [Path(f).name for f in files] == ["kept_dag.py"]
        assert {s["path"] for s in skipped} == {"regexp_a.py", "b_glob.py", "nested/c.py"}

    def test_an_exploding_pattern_in_airflowignore_is_refused_and_said(self, tmp_path):
        (tmp_path / "dags").mkdir()
        (tmp_path / "dags" / ".airflowignore").write_text("(a+)+$\n")
        (tmp_path / "dags" / ("a" * 200 + "!_dag.py")).write_text("# airflow dag\n")
        started = time.perf_counter()
        files, _, problems = discover_dag_files(str(tmp_path / "dags"))
        assert time.perf_counter() - started < 2
        assert len(files) == 1 and "only read as a glob" in problems[0]

    def test_is_absolute(self):
        assert is_absolute("/usr/local/airflow")
        assert is_absolute("C:/dags/x.py")
        assert not is_absolute("dags/x.py")
        assert not is_absolute("C:dags")


class TestOutput:
    def test_nothing_about_the_machine_reaches_the_result(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text("name: p\n")
        (tmp_path / "models").mkdir()
        (tmp_path / "models" / "a.sql").write_text("select 1")
        write_dag(
            tmp_path,
            """
            from pathlib import Path
            import os
            HERE = Path(__file__).parent
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command=f"cd {HERE} && dbt run", cwd=str(HERE))
                BashOperator(task_id="u", bash_command=f"dbt run --project-dir {HERE}/nowhere")
            """,
        )
        assert str(tmp_path.resolve()) not in str(scan_dags(str(tmp_path), ["dags"]))

    def test_the_same_workspace_gives_the_same_result_wherever_it_is(self, tmp_path):
        for name in ("one", "two"):
            root = tmp_path / name / "deeper" / "here"
            root.mkdir(parents=True)
            (root / "dbt_project.yml").write_text("name: p\n")
            (root / "models").mkdir()
            (root / "models" / "a.sql").write_text("select 1")
            write_dag(
                root, 'with DAG("d") as d:\n    BashOperator(task_id="t", bash_command="dbt run")\n'
            )
        first = scan_dags(str(tmp_path / "one" / "deeper" / "here"), ["dags"])
        second = scan_dags(str(tmp_path / "two" / "deeper" / "here"), ["dags"])
        assert first == second


class TestUnknownCategories:
    """Task 5.4: each of the categories spike S3 saw in the wild is named at the call site."""

    @pytest.fixture
    def project(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text("name: p\n")
        (tmp_path / "models").mkdir()
        (tmp_path / "models" / "a.sql").write_text("select 1")
        return tmp_path

    def categories(self, project, body):
        result = scan_one(project, body)
        return {u["category"] for u in result["unresolved"]}, result

    def test_remote_manifest(self, project):
        found, _ = self.categories(
            project,
            """
            from cosmos import DbtDag, ProjectConfig
            DbtDag(dag_id="d", project_config=ProjectConfig(manifest_path="s3://b/manifest.json"))
            """,
        )
        assert found == {"remote_manifest"}

    def test_tasks_generated_from_a_manifest(self, project):
        found, _ = self.categories(
            project,
            """
            import json
            with open("target/manifest.json") as f:
                manifest = json.load(f)
            with DAG("d") as d:
                for node in manifest["nodes"]:
                    BashOperator(task_id=node, bash_command=f"dbt run --select {node}")
            """,
        )
        assert "manifest_generated" in found and "loop_or_factory" not in found

    def test_wrapper_class(self, project):
        (project / "dags").mkdir()
        (project / "dags" / "wrap.py").write_text(
            "from airflow.operators.bash import BashOperator\n"
            "class Tasks:\n"
            "    def __init__(self, dag):\n"
            "        BashOperator(task_id='x', bash_command='dbt run', dag=dag)\n"
        )
        found, _ = self.categories(
            project,
            """
            from wrap import Tasks
            with DAG("d") as d:
                Tasks(d)
            """,
        )
        assert "wrapper" in found

    def test_loop_or_factory(self, project):
        found, _ = self.categories(
            project,
            """
            with DAG("d") as d:
                for name in names():
                    BashOperator(task_id=name, bash_command=f"dbt run --select {name}")
            """,
        )
        assert "loop_or_factory" in found

    def test_generated_project(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text("name: p\n")
        found = scan_one(
            tmp_path,
            """
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="dbt run")
            """,
        )["unresolved"]
        assert found[0]["category"] == "generated_project"

    def test_unsupported_selector(self, project):
        found, _ = self.categories(
            project,
            """
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="dbt run --select state:modified+")
            """,
        )
        assert found == {"unsupported_selector"}

    def test_jinja(self, project):
        found, _ = self.categories(
            project,
            """
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="dbt run --select {{ params.model }}")
            """,
        )
        assert found == {"jinja"}

    def test_a_shell_variable_is_as_unknown_as_jinja(self, project):
        found, result = self.categories(
            project,
            """
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="dbt run --select $MODEL")
            """,
        )
        assert found == {"not_literal"}
        assert find_link(result, "d", "t")["resolved_models"] is None

    def test_a_dag_that_cannot_load(self, project):
        found, _ = self.categories(project, "def broken(:\n")
        assert found == {"dag_not_loadable"}

    def test_a_shell_script(self, project):
        found, _ = self.categories(
            project,
            """
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="run_dbt.sh ")
            """,
        )
        assert found == {"shell_script"}

    def test_an_operator_that_mentions_dbt(self, project):
        found, _ = self.categories(
            project,
            """
            from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
            with DAG("d") as d:
                DbtCloudRunJobOperator(task_id="t", job_id=1)
            """,
        )
        assert found == {"other_operator"}

    def test_every_category_has_a_label_and_a_way_out(self):
        from lineagepilot_core.airflow.categories import (
            ANNOTATE,
            UnknownCategory,
            explanation,
            label,
        )

        for category in UnknownCategory:
            assert label(category.value)
            assert explanation(category.value).endswith(ANNOTATE)


class TestShellForms:
    @pytest.fixture
    def project(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text("name: p\n")
        (tmp_path / "models").mkdir()
        (tmp_path / "models" / "a.sql").write_text("select 1")
        (tmp_path / "models" / "b.sql").write_text("select 1")
        return tmp_path

    @pytest.mark.parametrize(
        "command",
        [
            "docker exec runner dbt run --select a",
            "docker compose run --rm dbt dbt run --select a",
            "docker-compose exec -T dbt dbt run --select a",
            "kubectl exec pod -- dbt run --select a",
            "ssh host dbt run --select a",
        ],
    )
    def test_dbt_inside_a_container_command(self, project, command):
        result = scan_one(
            project,
            f"""
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="{command}")
            """,
        )
        link = find_link(result, "d", "t")
        assert link["resolved_models"] == ["model.p.a"]
        assert any("another container" in note for note in link["notes"])

    def test_cd_to_a_variable_is_noted(self, project):
        result = scan_one(
            project,
            """
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="cd $HOME/app && dbt run --select a")
            """,
        )
        link = find_link(result, "d", "t")
        assert link["resolved_models"] == ["model.p.a"]
        assert any("not known" in note for note in link["notes"])

    def test_docker_working_dir_and_environment(self, tmp_path):
        for name in ("one", "two"):
            (tmp_path / name / "models").mkdir(parents=True)
            (tmp_path / name / "dbt_project.yml").write_text(f"name: {name}\n")
            (tmp_path / name / "models" / f"{name}.sql").write_text("select 1")
        result = scan_one(
            tmp_path,
            """
            from airflow.providers.docker.operators.docker import DockerOperator
            with DAG("d") as d:
                DockerOperator(task_id="t", image="x", command="dbt run", working_dir="/app/one")
                DockerOperator(
                    task_id="u",
                    image="x",
                    command="dbt run",
                    environment={"DBT_PROJECT_DIR": "/app/two"},
                )
            """,
        )
        assert find_link(result, "d", "t")["resolved_models"] == ["model.one.one"]
        assert find_link(result, "d", "u")["resolved_models"] == ["model.two.two"]


class TestSilenceIsNeverTheAnswer:
    """What the held-out corpus found: tasks that run dbt, and left no record at all. The scanner
    may not be able to read them, but it must say so (the plan's definition of done)."""

    POD = "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator\n"
    DOCKER = "from airflow.providers.docker.operators.docker import DockerOperator\n"

    @pytest.fixture
    def project(self, tmp_path):
        (tmp_path / "dbt_project.yml").write_text("name: p\n")
        (tmp_path / "models").mkdir()
        (tmp_path / "models" / "a.sql").write_text("select 1")
        return tmp_path

    def unknowns(self, project, body):
        result = scan_one(project, body)
        return [u for u in result["unresolved"] if u["category"] == "other_operator"], result

    @pytest.mark.parametrize(
        "call",
        [
            'KubernetesPodOperator(task_id="t", image="acme/dbt-duckdb", name="x")',
            'KubernetesPodOperator(task_id="dbt_run", image="x", cmds=["run"])',
            'KubernetesPodOperator(task_id="t", image="x", name="run-dbt", arguments=["go"])',
        ],
    )
    def test_a_pod_whose_command_is_in_its_image(self, project, call):
        found, result = self.unknowns(project, f"{self.POD}with DAG('d') as d:\n    {call}\n")
        assert found and found[0]["task_id"] in ("t", "dbt_run") and found[0]["mentions_dbt"]
        assert result["links"] == []

    def test_a_shell_flag_the_parser_does_not_know_leaves_a_record(self, project):
        found, _ = self.unknowns(
            project,
            f"{self.POD}with DAG('d') as d:\n"
            '    KubernetesPodOperator(task_id="t", image="x", cmds=["/bin/bash", "-cx"],\n'
            '                          arguments=["dbt run --select a"])\n',
        )
        assert found

    def test_a_docker_task_whose_command_is_in_its_image(self, project):
        found, _ = self.unknowns(
            project,
            f"{self.DOCKER}with DAG('d') as d:\n"
            '    DockerOperator(task_id="t", image="acme/dbt:1")\n',
        )
        assert found

    def test_a_shell_command_that_mentions_dbt_in_a_form_not_known(self, project):
        found, _ = self.unknowns(
            project,
            """
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="python -m run_dbt --all")
            """,
        )
        assert found

    def test_a_factory_from_a_library(self, project):
        result = scan_one(
            project,
            """
            from some_library import compile_dbt_dags
            dags = compile_dbt_dags(manifest_path="/x/manifest.json")
            """,
        )
        assert [u["category"] for u in result["unresolved"]] == ["wrapper"]

    @pytest.mark.parametrize(
        "line",
        [
            'BashOperator(task_id="t", bash_command="echo dbt is done")',
            'BashOperator(task_id="t", bash_command="python load.py")',
            'BashOperator(task_id="dbt_ready", bash_command="python load.py")',
        ],
    )
    def test_a_task_that_only_looks_near_dbt_is_not_flagged(self, project, line):
        found, _ = self.unknowns(project, f"with DAG('d') as d:\n    {line}\n")
        assert found == []

    def test_a_pod_with_nothing_to_do_with_dbt_is_not_flagged(self, project):
        found, _ = self.unknowns(
            project,
            f"{self.POD}with DAG('d') as d:\n"
            '    KubernetesPodOperator(task_id="t", image="x", cmds=["python", "-c", "print(1)"])\n',
        )
        assert found == []

    def test_a_config_helper_with_dbt_in_its_name_is_not_flagged(self, project):
        result = scan_one(
            project,
            """
            from some_library import get_dbt_profile
            profile = get_dbt_profile("x")
            """,
        )
        assert result["unresolved"] == []
