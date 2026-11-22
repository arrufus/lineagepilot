"""The review's Markdown: golden file, determinism, and what workspace text cannot do to it."""

from __future__ import annotations

import copy
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from support import apply_rename, copy_impact_shop

from lineagepilot_core.analysis.review import BaseRef, ChangedFile, FileStatus, review_changes
from lineagepilot_core.export.review_markdown import MAX_CHARS, render_markdown
from lineagepilot_core.indexer import index_project
from lineagepilot_core.project import read_project
from lineagepilot_core.readers.airflow.link import link_airflow
from lineagepilot_core.readers.airflow.scan import scan_airflow
from lineagepilot_core.readers.dagster.link import link_dagster
from lineagepilot_core.readers.dagster.scan import scan_dagster

GOLDEN = Path(__file__).parents[1] / "fixtures" / "review" / "rename-order-total.md"
BASE = BaseRef("origin/main", "0123456789abcdef0123456789abcdef01234567")
FILES = [
    ChangedFile("models/marts/orders.sql", FileStatus.MODIFIED),
    ChangedFile("models/marts/orders.yml", FileStatus.MODIFIED),
    ChangedFile("macros/cents_to_dollars.sql", FileStatus.MODIFIED),
    ChangedFile("models/marts/brand_new.sql", FileStatus.UNTRACKED),
    ChangedFile("README.md", FileStatus.ADDED),
]


@pytest.fixture
def root(tmp_path):
    root = copy_impact_shop(tmp_path / "impact_shop", dagster=True)
    apply_rename(root)
    return root


def render(root, files=FILES, **kwargs):
    graph = index_project(root).graph
    link_airflow(graph, scan_airflow(root, root))
    link_dagster(graph, scan_dagster(root, root, ["."]))
    return review_changes(graph, files, read_project(root), base=BASE, outside_project=2, **kwargs)[
        "markdown"
    ]


def test_the_review_matches_the_golden_file(root):
    assert render(root) == GOLDEN.read_text(encoding="utf-8"), (
        "the output changed: if that is intended, regenerate "
        "tests/fixtures/review/rename-order-total.md and read the diff"
    )


def test_the_order_of_the_input_does_not_matter(root):
    assert render(root, list(reversed(FILES))) == render(root)


_RENDER_SCRIPT = """
import sys
from pathlib import Path
from lineagepilot_core.readers.airflow.link import link_airflow
from lineagepilot_core.readers.airflow.scan import scan_airflow
from lineagepilot_core.readers.dagster.link import link_dagster
from lineagepilot_core.readers.dagster.scan import scan_dagster
from lineagepilot_core.analysis.review import BaseRef, ChangedFile, FileStatus, review_changes
from lineagepilot_core.indexer import index_project
from lineagepilot_core.project import read_project
root = Path(sys.argv[1])
files = [ChangedFile(p, FileStatus.MODIFIED) for p in sys.argv[2:]]
graph = index_project(root).graph
link_airflow(graph, scan_airflow(root, root))
link_dagster(graph, scan_dagster(root, root, ["."]))
sys.stdout.buffer.write(review_changes(graph, files, read_project(root))["markdown"].encode())
"""


def test_separate_processes_agree_whatever_the_hash_seed(root):
    # Sets and dicts of strings iterate differently under another seed; nothing may show it
    outputs = {
        subprocess.run(
            [sys.executable, "-c", _RENDER_SCRIPT, str(root), *[f.path for f in FILES]],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            check=True,
        ).stdout
        for seed in ("0", "1", "4242")
    }
    assert len(outputs) == 1


def test_no_machine_detail_is_in_the_text(root):
    text = render(root)
    assert str(root) not in text and str(Path.home()) not in text


def reader(tool="airflow", **fields):
    """What a reader read, as the review has it: analysed, one file, nothing linked."""
    return {
        "tool": tool,
        "analysed": True,
        "reason": None,
        "paths": ["dags"],
        "files": 1,
        "pipelines": 0,
        "tasks": 0,
        "consumers": 0,
        "links": 0,
        "unknowns": 0,
        "opaqueTasks": 0,
        "problems": [],
        **fields,
    }


def minimal(**overrides):
    """A review with one of everything, for changing a field at a time."""
    review = {
        "project": "shop",
        "base": {"ref": "main", "commit": "abc"},
        "artifacts": {"manifestGeneratedAt": None, "dbtVersion": None, "baselineManifest": None},
        "columnLineage": "done",
        "summary": {
            "changedNodes": 1,
            "changedColumns": 1,
            "impacted": {"model": 1},
            "unknowns": 1,
        },
        "changedNodes": [
            {
                "id": "model.s.a",
                "kind": "model",
                "name": "a",
                "owner": "o",
                "status": "modified",
                "files": [{"path": "a.sql", "status": "modified", "role": "definition"}],
                "columns": {
                    "state": "known",
                    "baseline": "catalog",
                    "added": [],
                    "removed": ["x"],
                    "typeChanged": [],
                    "reason": None,
                    "stale": False,
                },
            }
        ],
        "impacted": [
            {
                "id": "model.s.b",
                "kind": "model",
                "name": "b",
                "owner": "o",
                "file": "b.sql",
                "depth": 1,
                "reach": "column",
                "confidence": "inferred",
                "origins": ["compiled_sql"],
                "columns": ["y"],
                "reason": None,
                "via": [
                    {
                        "source": "model.s.a#x",
                        "target": "model.s.b#y",
                        "confidence": "inferred",
                        "origins": ["compiled_sql"],
                    }
                ],
                "viaMore": 0,
            }
        ],
        "owners": [{"owner": "o", "nodes": ["model.s.b"]}],
        "airflow": {
            "analysed": True,
            "reason": None,
            "dagFiles": 1,
            "dags": 0,
            "tasks": 0,
            "links": 0,
            "unknowns": 0,
            "opaqueTasks": 0,
            "folders": ["dags"],
            "problems": [],
        },
        "pipelines": [reader()],
        "consumers": [],
        "unknowns": [{"subject": "model.s.b", "reference": "r", "reason": "why"}],
        "otherFiles": [{"path": "README.md", "status": "modified"}],
        "outsideProject": 0,
        "notes": [],
    }
    review.update(overrides)
    return review


def spans_removed(line: str) -> str:
    """The line with its code spans taken out: what GitHub renders as Markdown."""
    return re.sub(r"(?<!`)(`+)(?!`).*?(?<!`)\1(?!`)", "", line)


def split_cells(row: str) -> list[str]:
    return re.split(r"(?<!\\)\|", row.strip())[1:-1]


HOSTILE = [
    "pipe|name",
    "back`tick",
    "``double``",
    "`edge`",
    "@org/team",
    "<img src=x onerror=alert(1)>",
    "</details>",
    "line\nbreak\r\n### Heading",
    "[link](https://example.com)",
    "**bold** _em_ ~~gone~~",
    "esc\x1b[31m\x07aped",
    "‮right-to-left",
    "#123",
    "| a | b |",
]


@pytest.mark.parametrize("text", HOSTILE)
def test_hostile_names_cannot_escape_their_code_span_or_cell(text):
    review = minimal()
    node = review["impacted"][0]
    node["name"], node["owner"], node["columns"] = text, text, [text]
    review["changedNodes"][0].update(name=text, owner=text)
    review["changedNodes"][0]["columns"]["removed"] = [text]
    review["owners"] = [{"owner": text, "nodes": [f"model.s.{text}"]}]
    review["project"] = text
    review["unknowns"] = [{"subject": text, "reference": text, "reason": text}]
    review["otherFiles"] = [{"path": text, "status": "modified"}]
    review["artifacts"]["baselineManifest"] = {
        "name": text,
        "generatedAt": text,
        "dbtVersion": None,
        "sha256": None,
    }
    markdown = render_markdown(review)
    assert_safe(markdown)


def assert_safe(markdown: str) -> None:
    assert "\x1b" not in markdown and "\x07" not in markdown and "‮" not in markdown
    # Inside a code span these are text; only our own pair may be real HTML
    rendered = [spans_removed(line) for line in markdown.splitlines()]
    assert sum("<details>" in line.replace("\\<", "") for line in rendered) <= 1
    assert sum("</details>" in line.replace("\\<", "") for line in rendered) <= 1
    headings = re.findall(r"^#+ .*$", markdown, flags=re.M)
    allowed = (
        "## Change impact review",
        "### Changed",
        "### Downstream impact",
        "### Pipelines",
        "### Consumers",
        "### Owners",
        "### Unknowns",
        "#### ",
    )
    assert all(h.startswith(allowed) for h in headings), headings
    for line in markdown.splitlines():
        outside = spans_removed(line)
        if line.strip() in ("</details>",) or line.startswith("<details>"):
            continue
        assert "<" not in outside.replace("\\<", ""), line
        assert not re.search(r"(?<![\w`​\\])@\w", outside), line
        assert "](" not in outside.replace("\\]", ""), line
        if line.startswith("|"):
            header = line.startswith("|---")
            # Owners, impact, and pipeline tables
            assert header or len(split_cells(line)) in (2, 5, 6), line


@given(st.text(min_size=1, max_size=40))
@settings(max_examples=120, suppress_health_check=[HealthCheck.too_slow], deadline=None)
def test_any_name_keeps_the_table_and_the_structure_intact(text):
    review = minimal()
    review["impacted"][0].update(name=text, owner=text, columns=[text], reach="column")
    review["unknowns"] = [{"subject": "s", "reference": "r", "reason": text}]
    markdown = render_markdown(review)
    rows = [line for line in markdown.splitlines() if line.startswith("| ")]
    # The impact table's header and row, and the owners table's header and row
    assert [len(split_cells(r)) for r in rows] == [5, 5, 2, 2], rows


def test_reasons_are_escaped_and_cannot_mention_anyone():
    review = minimal(unknowns=[{"subject": "s", "reference": "r", "reason": "ask @team *now* <b>"}])
    markdown = render_markdown(review)
    assert "@​team" in markdown and "\\*now\\*" in markdown and "\\<b\\>" in markdown


def test_a_huge_review_is_cut_to_fit_a_github_comment_and_says_so():
    review = minimal()
    row = review["impacted"][0]
    review["impacted"] = [
        {**copy.deepcopy(row), "id": f"model.s.m{i}", "name": f"m{i}" * 40, "reason": "r" * 500}
        for i in range(3000)
    ]
    review["unknowns"] = [
        {"subject": f"s{i}" * 20, "reference": "r" * 100, "reason": "why " * 100}
        for i in range(3000)
    ]
    review["summary"]["impacted"] = {"model": 3000}
    markdown = render_markdown(review)
    assert len(markdown) <= MAX_CHARS
    assert "more models." in markdown and "more unknowns." in markdown
    assert "#### Models (3000)" in markdown and "### Unknowns (3000)" in markdown


def test_long_notes_are_cut_like_any_list():
    review = minimal(notes=["n" * 500] * 400)
    markdown = render_markdown(review)
    assert len(markdown) <= MAX_CHARS
    assert re.search(r"\.\.\.and \d+ more notes\.", markdown)
    # Everything after the notes is still there
    assert "### Unknowns (1)" in markdown and "</details>" in markdown


def test_an_impossibly_long_review_is_still_cut_to_the_limit():
    # Thousands of tools are thousands of parts, each a heading at least. Between lines, so no
    # code span is cut open (LP-SEC-02), and it says so
    review = minimal()
    row = review["impacted"][0]
    review["impacted"] = [
        {**row, "id": f"t{i}:c", "kind": "consumer", "tool": f"t{i}", "subkind": None}
        for i in range(5000)
    ]
    markdown = render_markdown(review)
    assert MAX_CHARS - 600 < len(markdown) <= MAX_CHARS
    assert markdown.endswith(
        "\n\n...and the rest of the review, which is too long for a GitHub comment.\n"
    )


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"base": None}, "with no base commit"),
        ({"base": {"ref": None, "commit": "abc"}}, "Compared with `abc`."),
        ({"impacted": [], "owners": [], "summary": {
            "changedNodes": 0, "changedColumns": 0, "impacted": {}, "unknowns": 0,
        }}, "nothing downstream"),
        ({"changedNodes": []}, "No changed file maps to a node."),
        ({"unknowns": []}, "### Unknowns (0)\n\nNone."),
        ({"otherFiles": [], "outsideProject": 1}, "1 file outside this dbt project"),
        ({"pipelines": [reader(analysed=False, reason="no")]}, "Airflow\n\nNot analysed: no."),
        ({"pipelines": [reader(files=3)]}, "No task in the 3 DAG files read runs a changed"),
        ({"project": None}, "`dbt project`"),
    ],
)  # fmt: skip
def test_each_section_says_something_when_empty_or_missing(change, expected):
    assert expected in render_markdown(minimal(**change))


def test_every_origin_has_words():
    from lineagepilot_core.export import review_markdown
    from lineagepilot_core.graph.model import Origin

    assert set(review_markdown._ORIGINS) == {o.value for o in Origin}


def test_a_reader_that_found_none_of_its_files_says_nothing():
    """A review of a project with no Dagster is as it was before task 8.2: a reader with nothing
    to report has no part, while one that was not analysed still says why."""
    nothing = reader("dagster", analysed=True, files=0)
    assert "Dagster" not in render_markdown(minimal(pipelines=[reader(files=3), nothing]))
    some = render_markdown(minimal(pipelines=[reader("dagster", files=2)]))
    assert "#### Dagster\n\nNo task in the 2 pipeline files read" in some
    not_read = render_markdown(minimal(pipelines=[reader("dagster", analysed=False, reason="x")]))
    assert "Dagster\n\nNot analysed: x." in not_read


def test_a_column_change_is_spelled_out():
    review = minimal()
    columns = review["changedNodes"][0]["columns"]
    columns.update(
        added=["n"],
        typeChanged=[{"name": "t", "before": "int", "after": "text"}],
        reason="An extra word.",
        baseline="baseline_manifest",
    )
    markdown = render_markdown(review)
    assert "Removed: `x`" in markdown and "Added: `n`" in markdown
    assert "Type of `t`: `int` to `text`" in markdown
    assert "Compared with the baseline manifest. An extra word." in markdown


def test_no_column_change_and_unknown_columns_are_said():
    review = minimal()
    columns = review["changedNodes"][0]["columns"]
    columns["removed"] = []
    assert "No column added, removed or retyped, compared with `catalog.json`." in render_markdown(
        review
    )
    columns.update(state="unknown", reason="run dbt compile")
    assert "Columns unknown: run dbt compile" in render_markdown(review)


def test_many_columns_and_sources_are_counted_not_listed():
    review = minimal()
    node = review["impacted"][0]
    node["columns"] = [f"c{i}" for i in range(8)]
    node["via"] = [{**node["via"][0], "source": f"model.s.m#c{i}"} for i in range(3)]
    node["viaMore"] = 4
    review["owners"][0]["nodes"] = [f"model.s.m{i}" for i in range(12)]
    markdown = render_markdown(review)
    assert "Columns `c0`" in markdown and "and 3 more" in markdown
    assert "from `m.c0`, `m.c1` and 5 more" in markdown
    assert "and 2 more |" in markdown


def test_a_model_level_hit_with_columns_shows_both():
    review = minimal()
    review["impacted"][0].update(reach="model", reason="why")
    assert "Model level: why. Column `y`" in render_markdown(review)
    review["impacted"][0].update(via=[], reason=None, columns=[])
    assert "| Model level | inferred | compiled SQL |" in render_markdown(review)


def test_the_text_is_stable_json_serialisable_input():
    # The server sends the review over JSON; the text it renders must not depend on that
    review = minimal()
    assert render_markdown(json.loads(json.dumps(review))) == render_markdown(review)


# Sections by tool (task 7.7, design_next_phase section 7.5)

EVERY_SECTION = Path(__file__).parents[1] / "fixtures" / "review" / "every-section.md"


def hit(node_id, kind, name, tool=None, subkind=None, **fields):
    """An impacted row, of any kind and tool."""
    return {
        "id": node_id,
        "kind": kind,
        "name": name,
        "owner": None,
        "file": None,
        "depth": 2,
        "reach": "model",
        "confidence": "declared",
        "origins": ["annotation"],
        "columns": [],
        "reason": None,
        "via": [],
        "viaMore": 0,
        "tool": tool,
        "subkind": subkind,
        "pipeline": None,
        **fields,
    }


def task(node_id, name, tool, pipeline, **fields):
    detail = {
        "tool": tool,
        "pipeline": pipeline,
        "operator": None,
        "how": ["dbt run"],
        "runs": ["b"],
        "runsMore": 0,
        "tasks": [],
        "tasksMore": 0,
        "after": [],
        "notes": [],
    }
    return hit(
        node_id, "task", name, tool, "task", confidence="heuristic", pipeline=detail, **fields
    )


def every_section():
    """A review with something in each section, from several tools."""
    review = minimal()
    review["impacted"] += [
        hit("exposure.s.dash", "exposure", "dash", owner="bi"),
        hit("metric.shop.revenue", "consumer", "revenue", "dbt_semantic", "metric"),
        task("airflow:daily.run_b", "run_b", "airflow", "daily", owner="etl"),
        task("dagster:nightly.load_b", "load_b", "dagster", "nightly"),
        hit(
            "looker:view.b",
            "consumer",
            "b",
            "looker",
            "view",
            reach="column",
            columns=["total", "id"],
            confidence="inferred",
            owner="bi",
        ),
        hit("tableau:finance/gl", "consumer", "gl", "tableau"),
    ]
    review["pipelines"] = [reader(), reader("dagster", paths=["defs.yaml"], files=2)]
    review["consumers"] = [
        reader("dbt_semantic"),
        reader("looker", paths=["**/*.lkml"], files=3),
        reader("powerbi", analysed=False, reason="no .SemanticModel folder was found"),
    ]
    review["owners"] += [
        {"owner": "bi", "nodes": ["exposure.s.dash", "looker:view.b"]},
        {"owner": "etl", "nodes": ["airflow:daily.run_b"]},
    ]
    review["unknowns"] += [
        {"subject": "airflow:backfill", "reference": "loop or factory", "reason": "a loop"},
        {"subject": "looker:view.b", "reference": "sql_table_name", "reason": "a Liquid template"},
        {"subject": "macros/m.sql", "reference": "changed file", "reason": "a macro"},
    ]
    review["summary"]["unknowns"] = len(review["unknowns"])
    return review


def test_every_section_in_the_design_s_order():
    markdown = render_markdown(every_section())
    assert markdown == EVERY_SECTION.read_text(encoding="utf-8"), (
        "the output changed: if that is intended, regenerate "
        "tests/fixtures/review/every-section.md and read the diff"
    )
    headings = re.findall(r"^#{2,4} .*$", markdown, flags=re.M)
    assert headings == [
        "## Change impact review: `shop`",
        "### Changed",
        "### Downstream impact",
        "#### Models (1)",
        "#### Exposures (1)",
        "#### Semantic layer (1)",
        "### Pipelines",
        "#### Airflow (1 task in 1 DAG)",
        "#### Dagster (1 task in 1 code location)",
        "### Consumers",
        "#### Looker (1)",
        "#### Power BI",
        "#### Tableau (1)",
        "### Owners",
        "### Unknowns (4)",
        "#### dbt (2)",
        "#### Airflow (1)",
        "#### Looker (1)",
    ]
    # The semantic layer is counted as dbt's own nodes are, by what each is
    assert (
        "7 impacted (1 model, 1 exposure, 1 metric, 2 tasks (Airflow 1, Dagster 1), 2 consumers"
        in (markdown)
    )


def test_one_tool_s_long_list_is_cut_and_every_other_section_stays_whole():
    review = every_section()
    looker = next(n for n in review["impacted"] if n.get("tool") == "looker")
    review["impacted"] += [
        {**looker, "id": f"looker:view.v{i}", "name": f"view_{i}_" + "v" * 100} for i in range(3000)
    ]
    review["unknowns"] += [
        {"subject": f"looker:view.v{i}", "reference": "r" * 50, "reason": "why " * 50}
        for i in range(3000)
    ]
    whole = render_markdown(every_section())
    markdown = render_markdown(review)
    assert len(markdown) <= MAX_CHARS
    # Looker's rows and unknowns are cut, each with a count
    assert re.search(r"\n\.\.\.and \d+ more Looker consumers\.\n", markdown)
    assert re.search(r"\n\.\.\.and \d+ more unknowns\.\n", markdown)
    # Everything else is as it was without them, each section and each other tool's rows
    for section in ("#### Airflow (1 task in 1 DAG)", "#### Dagster", "#### Power BI"):
        start = whole.index(section)
        end = whole.index("\n\n#### ", start + 1)
        assert whole[start:end] in markdown, section
    for unknown in ("`airflow:backfill`", "`macros/m.sql`", "`model.s.b`"):
        assert unknown in markdown
    assert "### Owners" in markdown and "<details><summary>Other changed files" in markdown


def test_the_longest_part_is_cut_first():
    review = minimal()
    row = review["impacted"][0]
    review["impacted"] = [
        {**row, "id": f"model.s.m{i}", "name": f"m{i}" + "m" * 100} for i in range(800)
    ] + [{**row, "id": f"exposure.s.e{i}", "kind": "exposure", "name": f"e{i}"} for i in range(80)]
    markdown = render_markdown(review)
    assert len(markdown) <= MAX_CHARS
    # The models are cut until the review fits; the shorter exposures table keeps every row
    assert re.search(r"\.\.\.and \d+ more models\.", markdown)
    assert "more exposures" not in markdown and "`e79`" in markdown


def dagster(subkind, name, kind="task", location="loc", **detail):
    """A Dagster row of the reader's: a definition, job, schedule or asset in a code location."""
    row = task(f"dagster:{location}.{name}", name, "dagster", location)
    row["pipeline"].update({"how": ["dbt, through Dagster"], **detail})
    fields = {"kind": kind, "subkind": subkind, "confidence": "inferred"}
    return {**row, **fields, "origins": ["dagster_defs"]}


def dagster_part(markdown: str) -> str:
    return markdown.split("#### Dagster")[1].split("\n\n### ")[0]


def cells(line: str) -> list[str]:
    return [cell.strip() for cell in split_cells(line)]


def test_dagster_s_rows_by_code_location_definitions_then_each_job_with_its_schedules():
    """Then the assets downstream, each row saying what it is (task 8.9)."""
    review = minimal()
    review["impacted"] += [
        dagster("asset", "features", how=["reads, as a Dagster asset"]),
        dagster("schedule", "early", "pipeline", job="b_job"),
        dagster("schedule", "daily", "pipeline", job="b_job", cron="0 6 * * *"),
        dagster("job", "b_job", "pipeline"),
        dagster("job", "a_job", "pipeline"),
        dagster("schedule", "a_job_schedule", "pipeline", job="a_job", cron="@hourly"),
        dagster("dbt_assets", "shop"),
        dagster("job", "other", "pipeline", location="another"),
    ]
    review["pipelines"] = [reader("dagster", paths=["."], files=2)]
    part = dagster_part(render_markdown(review))
    heading = " (1 dbt asset definition, 3 jobs, 3 schedules and 1 asset in 2 code locations)\n"
    assert part.startswith(heading)
    lines = part.splitlines()
    assert lines[2] == (
        "| Code location | Definition | What it has to do with this | Confidence | Evidence "
        "| Owner |"
    )
    rows = [cells(line)[:2] for line in lines[4:]]
    assert rows == [
        ["`another`", "job `other`"],
        ["`loc`", "dbt asset definition `shop`"],
        ["`loc`", "job `a_job`"],
        ["`loc`", "schedule `a_job_schedule`"],
        ["`loc`", "job `b_job`"],
        ["`loc`", "schedule `daily`"],
        ["`loc`", "schedule `early`"],
        ["`loc`", "asset `features`"],
    ]
    what = {cells(line)[1]: cells(line)[2] for line in lines[4:]}
    # A schedule names its job, and its cron when it has one
    assert what["schedule `daily`"] == "dbt, through Dagster: `b`; job `b_job`, `0 6 * * *`"
    assert what["schedule `early`"] == "dbt, through Dagster: `b`; job `b_job`"
    assert what["job `b_job`"] == "dbt, through Dagster: `b`"


@pytest.mark.parametrize("text", HOSTILE)
def test_a_schedule_s_job_and_cron_are_text_like_any_name(text):
    """They come from the workspace's definitions files, as names do (task 8.9)."""
    review = minimal()
    review["impacted"].append(
        dagster("schedule", text, "pipeline", location=text, job=text, cron=text)
    )
    review["pipelines"] = [reader("dagster", paths=["."], files=1)]
    markdown = render_markdown(review)
    assert_safe(markdown)
    rows = [line for line in dagster_part(markdown).splitlines()[4:] if line.strip()]
    assert len(rows) == 1 and len(split_cells(rows[0])) == 6, rows


def test_dagster_rows_that_are_not_all_the_reader_s_are_tasks_as_before():
    """An annotation's task has no subkind of the reader's, so the part counts tasks."""
    review = minimal()
    review["impacted"] += [
        dagster("job", "a_job", "pipeline"),
        task("dagster:loc.load_b", "load_b", "dagster", "loc"),
    ]
    review["pipelines"] = [reader("dagster", paths=["."], files=2)]
    part = dagster_part(render_markdown(review))
    assert part.startswith(" (2 tasks in 1 code location)\n")
    lines = part.splitlines()
    assert lines[2].startswith("| Code location | Task |")
    # The reader's row still says what it is, after the annotation's in its code location
    assert [cells(line)[1] for line in lines[4:]] == ["`load_b`", "job `a_job`"]


def test_a_path_with_a_colon_is_dbt_s_unknown_and_an_unnamed_tool_is_its_own():
    review = minimal()
    review["impacted"].append(hit("acme_bi:report", "consumer", "report", "acme_bi"))
    review["unknowns"] = [
        {"subject": "odd:name.sql", "reference": "changed file", "reason": "a file"},
        {"subject": "acme_bi:report", "reference": "x", "reason": "a tool nobody named"},
        {"subject": "zzz:thing", "reference": "x", "reason": "no reader or row has zzz"},
    ]
    markdown = render_markdown(review)
    assert "#### dbt (2)" in markdown and "#### acme\\_bi (1)" in markdown
    # A tool this version does not name is shown as it is written, its consumers and unknowns too
    assert "### Consumers\n\n#### acme\\_bi (1)\n\n| Name |" in markdown
