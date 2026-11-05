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
    root = copy_impact_shop(tmp_path / "impact_shop")
    apply_rename(root)
    return root


def render(root, files=FILES, **kwargs):
    graph = index_project(root).graph
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
from lineagepilot_core.analysis.review import BaseRef, ChangedFile, FileStatus, review_changes
from lineagepilot_core.indexer import index_project
from lineagepilot_core.project import read_project
root = Path(sys.argv[1])
files = [ChangedFile(p, FileStatus.MODIFIED) for p in sys.argv[2:]]
graph = index_project(root).graph
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
        "airflow": {"analysed": True},
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
            assert header or len(split_cells(line)) in (2, 5), line


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


def test_an_impossibly_long_review_is_still_cut_to_the_limit():
    review = minimal(notes=["n" * 500] * 400)
    assert len(render_markdown(review)) == MAX_CHARS


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
        ({"airflow": {"analysed": False}}, "Airflow DAGs were not analysed"),
        ({"project": None}, "`dbt project`"),
    ],
)  # fmt: skip
def test_each_section_says_something_when_empty_or_missing(change, expected):
    assert expected in render_markdown(minimal(**change))


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
