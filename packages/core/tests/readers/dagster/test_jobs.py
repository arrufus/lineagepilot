"""Dagster asset jobs and schedules (task 8.6): what each selects, read from Python and resolved
over the code location's dbt assets, and linked as pipelines."""

from __future__ import annotations

import importlib.util
import json
import textwrap
from pathlib import Path

import pytest
from support import copy_impact_shop

from lineagepilot_core.analysis.impact import impact
from lineagepilot_core.graph.model import DAGSTER, EdgeKind, NodeKind, Origin
from lineagepilot_core.indexer import index_project
from lineagepilot_core.readers.dagster.reader import DagsterReader
from lineagepilot_core.readers.dagster.select_syntax import NotRead, parse
from lineagepilot_core.readers.views import pipelines_view

READER = DagsterReader()
S = "model.impact_shop"
CORE = Path(__file__).resolve().parents[3]
RECORD = CORE / "tests" / "fixtures" / "dagster" / "asset_keys.json"


def script():
    path = CORE / "scripts" / "dagster_key_conformance.py"
    spec = importlib.util.spec_from_file_location("dagster_key_conformance", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestAgainstDagster:
    """The answers are dagster's own, recorded by scripts/dagster_key_conformance.py, which CI
    runs with dagster and dagster-dbt: each job's dbt nodes, from the same definitions file."""

    def test_the_reader_selects_what_dagster_selects(self, tmp_path):
        module = script()
        record = json.loads(RECORD.read_text(encoding="utf-8"))
        recorded = record["jobs"]
        assert [job["label"] for job in recorded] == [label for label, _ in module.JOBS]
        theirs = {module.job_name(i): job["nodes"] for i, job in enumerate(recorded)}
        theirs.update({asset["key"]: asset["nodes"] for asset in record["assets"]})
        project = module.write_workspace(tmp_path)
        assert module.compare_jobs(theirs, module.reader_jobs(tmp_path, project)) == []

    def test_the_jobs_select_enough_different_nodes_to_test_something(self):
        recorded = json.loads(RECORD.read_text(encoding="utf-8"))["jobs"]
        assert len({tuple(job["nodes"]) for job in recorded}) >= 12
        # The source named for the seed is joined to it, so upstream reaches the seed
        upstream = next(job for job in recorded if job["label"] == "upstream")
        assert "seed.impact_shop.raw_orders" in upstream["nodes"]


class TestSelectionStrings:
    @pytest.mark.parametrize(
        ("text", "tree"),
        [
            ("key:a/b", {"op": "keys", "keys": [["a", "b"]], "strict": False}),
            ('key:"a/b"', {"op": "keys", "keys": [["a", "b"]], "strict": False}),
            ("key:a*", {"op": "key_pattern", "pattern": "a*"}),
            ("group:marts", {"op": "groups", "names": ["marts"]}),
            ("*", {"op": "all"}),
            ("a", {"op": "keys", "keys": [["a"]], "strict": True}),
        ],
    )
    def test_terms(self, text, tree):
        assert parse(text) == tree

    @pytest.mark.parametrize(
        ("text", "op", "depth"),
        [
            ("+key:a", "up", None),
            ("2+key:a", "up", 2),
            ("key:a+", "down", None),
            ("key:a+1", "down", 1),
            ("+a", "up", 1),
            ("++a", "up", 2),
            ("*a", "up", None),
            ("a++", "down", 2),
            ("a*", "down", None),
        ],
    )
    def test_traversal_in_each_syntax(self, text, op, depth):
        tree = parse(text)
        assert (tree["op"], tree["depth"], tree["self"]) == (op, depth, True)

    def test_both_ways_is_the_union(self):
        assert parse("+key:a+")["op"] == "or"

    def test_not_binds_tightest_then_and(self):
        tree = parse("key:a or not key:b and group:c")
        assert tree["op"] == "or"
        assert tree["of"][1] == {
            "op": "and",
            "of": [{"op": "not", "of": parse("key:b")}, {"op": "groups", "names": ["c"]}],
        }
        assert parse("(key:a or key:b) and group:c")["op"] == "and"

    @pytest.mark.parametrize(
        ("text", "words"),
        [
            ("tag:nightly", "tag:"),
            ("owner:team", "owner:"),
            ("kind:dbt", "kind:"),
            ("key:*", "Dagster accepts"),
            ("key:a or", "ends too soon"),
            ("(key:a", "not closed"),
            ("key:a key:b", "not a selection"),
            ("", "empty"),
            ('key:"a', "not a selection"),
        ],
    )
    def test_what_is_not_read_says_why(self, text, words):
        with pytest.raises(NotRead, match=words):
            parse(text)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    copy_impact_shop(root / "dbt")
    return root


def write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip("\n"), encoding="utf-8")


ASSETS = """
from pathlib import Path
from dagster import (
    AssetKey, AssetSelection, Definitions, ScheduleDefinition, define_asset_job, job, schedule,
)
from dagster_dbt import build_dbt_asset_selection, build_schedule_from_dbt_selection, dbt_assets

M = Path(__file__).parent.parent / "dbt" / "target" / "manifest.json"

@dbt_assets(manifest=M, select="+orders")
def shop(context, dbt):
    ...
"""


def read(repo: Path, body: str, definitions: str | None = None) -> dict:
    loaded = definitions or "defs = Definitions(assets=[shop], jobs=JOBS, schedules=SCHEDULES)"
    write(repo, "orch/defs.py", ASSETS + textwrap.dedent(body) + "\n" + loaded + "\n")
    return READER.read(repo / "dbt", repo, ["."], None)


def by_name(records: list[dict]) -> dict:
    return {r["name"]: r for r in records}


class TestJobs:
    def test_a_job_s_selection_resolved_over_the_location_s_assets(self, repo):
        scan = read(
            repo,
            """
            a = define_asset_job("a", selection="key:orders")
            up = AssetSelection.keys("orders").upstream(depth=1)
            b = define_asset_job(name="b", selection=up)
            c = define_asset_job("c")
            dbt = build_dbt_asset_selection([shop], dbt_select="stg_orders")
            d = define_asset_job("d", selection=dbt)
            JOBS, SCHEDULES = [a, b, c, d], []
            """,
        )
        jobs = by_name(scan["jobs"])
        assert jobs["a"]["nodes"] == [f"{S}.orders"]
        assert jobs["b"]["nodes"] == [f"{S}.orders", f"{S}.stg_orders"]
        assert jobs["c"]["nodes"] == [f"{S}.orders", f"{S}.stg_orders"]
        assert jobs["d"]["nodes"] == [f"{S}.stg_orders"]
        assert {j["location"] for j in jobs.values()} == {"orch.defs"}
        assert jobs["a"]["api"] == "define_asset_job" and jobs["a"]["confidence"] == "inferred"

    def test_a_job_that_runs_no_dbt_is_no_dbt_job(self, repo):
        scan = read(
            repo,
            """
            other = define_asset_job("other", selection=AssetSelection.keys("not_dbt"))
            JOBS, SCHEDULES = [other], [ScheduleDefinition(job=other, cron_schedule="@daily")]
            """,
        )
        assert scan["jobs"] == [] and scan["schedules"] == []

    @pytest.mark.parametrize(
        ("job", "words"),
        [
            ('define_asset_job("t", selection="tag:nightly")', "selection by tag: is not read"),
            (
                'define_asset_job("t", selection=AssetSelection.tag("a", "b"))',
                "AssetSelection.tag()",
            ),
            ('define_asset_job("t", selection=pick())', "not one the reader reads"),
            ('define_asset_job("t", selection=AssetSelection.all().sinks())', ".sinks()"),
            ("define_asset_job(NAME)", "name is not known"),
        ],
    )
    def test_a_job_that_cannot_be_read_is_an_unknown_at_its_line(self, repo, job, words):
        scan = read(repo, f"t = {job}\nJOBS, SCHEDULES = [t], []\n")
        [found] = scan["jobs"]
        assert found["status"] == "unknown" and words in found["reason"], found["reason"]
        assert found["line"] == 12

    def test_a_job_in_a_loop_is_dynamic(self, repo):
        scan = read(
            repo,
            """
            JOBS = []
            for name in ["a", "b"]:
                JOBS.append(define_asset_job(name, selection="key:orders"))
            SCHEDULES = []
            """,
            "defs = Definitions(assets=[shop])",
        )
        [found] = scan["jobs"]
        assert found["category"] == "loop_or_factory"

    def test_a_custom_translator_makes_a_selection_by_key_say_it_may_miss(self, repo):
        write(
            repo,
            "orch/defs.py",
            ASSETS.replace('select="+orders")', 'select="+orders", dagster_dbt_translator=T())')
            + 'a = define_asset_job("a", selection="key:orders")\n'
            + 'b = define_asset_job("b", selection=AssetSelection.assets(shop))\n'
            + "defs = Definitions(assets=[shop], jobs=[a, b])\n",
        )
        jobs = by_name(READER.read(repo / "dbt", repo, ["."], None)["jobs"])
        # The keys are not known, so a selection by key may select them: never dropped
        assert jobs["a"]["status"] == "unknown" and jobs["a"]["category"] == "custom_translator"
        # By definition, nothing is lost
        assert jobs["b"]["nodes"] == [f"{S}.orders", f"{S}.stg_orders"]
        assert "keys_unknown" not in jobs["b"]

    def test_a_job_selecting_an_unreadable_definition_is_an_unknown(self, repo):
        write(
            repo,
            "orch/defs.py",
            ASSETS.replace("manifest=M,", "manifest=load(),")
            + 'a = define_asset_job("a", selection=AssetSelection.assets(shop))\n'
            + "defs = Definitions(assets=[shop], jobs=[a])\n",
        )
        [found] = READER.read(repo / "dbt", repo, ["."], None)["jobs"]
        assert found["status"] == "unknown" and "shop, a dbt asset definition" in found["reason"]


class TestSchedules:
    def test_each_form_runs_its_job_s_nodes(self, repo):
        scan = read(
            repo,
            """
            a = define_asset_job("a", selection="key:orders")
            daily = ScheduleDefinition(job=a, cron_schedule="0 6 * * *")
            named = ScheduleDefinition(name="by_name", job_name="a", cron_schedule="@hourly")

            @schedule(cron_schedule="0 7 * * *", job=a)
            def morning(context):
                return {}

            nightly = build_schedule_from_dbt_selection(
                [shop], job_name="nightly", cron_schedule="0 0 * * *", dbt_select="stg_orders"
            )
            JOBS, SCHEDULES = [a], [daily, named, morning, nightly]
            """,
        )
        schedules = by_name(scan["schedules"])
        assert set(schedules) == {"a_schedule", "by_name", "morning", "nightly_schedule"}
        assert schedules["a_schedule"]["nodes"] == [f"{S}.orders"]
        assert (schedules["a_schedule"]["job_name"], schedules["a_schedule"]["cron"]) == (
            "a",
            "0 6 * * *",
        )
        assert schedules["by_name"]["nodes"] == [f"{S}.orders"]
        assert schedules["morning"]["api"] == "@schedule"
        assert schedules["nightly_schedule"]["nodes"] == [f"{S}.stg_orders"]
        assert by_name(scan["jobs"])["nightly"]["nodes"] == [f"{S}.stg_orders"]

    def test_a_job_only_its_schedule_lists_is_in_the_schedule_s_location(self, repo):
        write(
            repo,
            "orch/jobs.py",
            'from dagster import define_asset_job\na = define_asset_job("a")\n',
        )
        scan = read(
            repo,
            """
            from .jobs import a
            JOBS, SCHEDULES = [], [ScheduleDefinition(job=a, cron_schedule="@daily")]
            """,
        )
        [job] = scan["jobs"]
        assert job["location"] == "orch.defs" and not job["notes"]
        assert not any("loaded by no Definitions" in p for p in scan["problems"])

    def test_a_schedule_of_an_op_job_is_none_of_dbt_s(self, repo):
        scan = read(
            repo,
            """
            @job
            def ops():
                ...
            JOBS, SCHEDULES = [], [ScheduleDefinition(job=ops, cron_schedule="@daily")]
            """,
        )
        assert scan["schedules"] == []

    def test_a_schedule_of_a_job_that_cannot_be_read_is_an_unknown(self, repo):
        scan = read(
            repo,
            """
            t = define_asset_job("t", selection="tag:x")
            JOBS, SCHEDULES = [t], [ScheduleDefinition(job=t, cron_schedule="@daily")]
            """,
        )
        [found] = scan["schedules"]
        assert found["status"] == "unknown" and "its job, t, cannot be read" in found["reason"]


class TestLinked:
    @pytest.fixture
    def graph(self, repo):
        body = """
            a = define_asset_job("a", selection="key:orders")
            t = define_asset_job("t", selection="tag:x")
            JOBS, SCHEDULES = [a, t], [ScheduleDefinition(job=a, cron_schedule="0 6 * * *")]
            """
        scan = read(repo, body)
        graph = index_project(repo / "dbt").graph
        READER.link(graph, scan)
        return graph

    def test_a_job_and_its_schedule_are_pipelines_that_run_its_nodes(self, graph):
        job = graph.nodes["dagster:orch.defs/job/a"]
        schedule = graph.nodes["dagster:orch.defs/schedule/a_schedule"]
        assert (job.kind, job.subkind, job.tool) == (NodeKind.PIPELINE, "job", DAGSTER)
        assert schedule.subkind == "schedule"
        assert (schedule.properties["job"], schedule.properties["cron"]) == ("a", "0 6 * * *")
        for node in (job, schedule):
            [edge] = graph.out_edges(node.id, (EdgeKind.ORCHESTRATES,))
            assert edge.downstream == f"{S}.orders"
            assert edge.evidence[0].origin is Origin.DAGSTER_DEFS

    def test_a_change_reaches_the_job_and_the_schedule(self, graph):
        found = impact(graph, [f"{S}.orders"])
        assert {"dagster:orch.defs/job/a", "dagster:orch.defs/schedule/a_schedule"} <= set(
            found.nodes
        )

    def test_the_pipelines_view_lists_them(self, graph):
        ids = {p["id"] for p in pipelines_view(graph, [f"{S}.orders"], [DAGSTER])["pipelines"]}
        assert {"dagster:orch.defs/job/a", "dagster:orch.defs/schedule/a_schedule"} <= ids

    def test_an_unreadable_job_is_an_unknown_about_it(self, graph):
        [unknown] = [u for u in graph.unresolved if u.subject == "dagster:orch.defs/job/t"]
        assert "selection by tag: is not read" in unknown.reason
        assert graph.nodes["dagster:orch.defs/job/t"].subkind == "job"

    def test_the_review_groups_them_in_their_code_location_after_its_definitions(self, graph, repo):
        """The schedule's row names its job and cron (task 8.9)."""
        from lineagepilot_core.analysis.review import ChangedFile, FileStatus, review_changes
        from lineagepilot_core.project import read_project

        review = review_changes(
            graph,
            [ChangedFile("models/marts/orders.sql", FileStatus.MODIFIED)],
            read_project(repo / "dbt"),
        )
        rows = {n["id"]: n for n in review["impacted"] if n.get("tool") == DAGSTER}
        assert {r["pipeline"]["pipeline"] for r in rows.values()} == {"orch.defs"}
        schedule = rows["dagster:orch.defs/schedule/a_schedule"]["pipeline"]
        assert (schedule["job"], schedule["cron"]) == ("a", "0 6 * * *")
        assert "job" not in rows["dagster:orch.defs/job/a"]["pipeline"]
        part = review["markdown"].split("#### Dagster")[1].split("### Owners")[0]
        assert part.startswith(" (1 dbt asset definition, 1 job and 1 schedule in 1 code location)")
        rows = [line.split(" | ")[:3] for line in part.splitlines() if line.startswith("| `")]
        assert rows == [
            ["| `orch.defs`", "dbt asset definition `shop`", "dbt, through Dagster: `orders`"],
            ["| `orch.defs`", "job `a`", "dbt, through Dagster: `orders`"],
            [
                "| `orch.defs`",
                "schedule `a_schedule`",
                "dbt, through Dagster: `orders`; job `a`, `0 6 * * *`",
            ],
        ]
