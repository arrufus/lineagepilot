"""LP-SEC-02: no name in the review becomes Markdown, HTML, a link or a mention on GitHub.

The text is parsed back as GitHub reads it: CommonMark, with raw HTML, plus tables. Every name
must come back as a whole code span, and nothing outside the code spans may be HTML, an image,
a link, a mention or an issue reference.
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from markdown_it import MarkdownIt

# _flatten is how the review folds a name onto one line; the hostile-name tests check it
from lineagepilot_core.export.review_markdown import (
    MAX_CHARS,
    NAME_LIMIT,
    _flatten,
    render_markdown,
)

GFM = MarkdownIt("commonmark").enable(["table", "strikethrough"])
# In text, what GitHub makes a mention, an issue reference or a link (a bare URL, or www.)
LINKING = re.compile(r"@\w|#\d|://|www\.", re.IGNORECASE)
# The only HTML the review writes itself
OWN_HTML = re.compile(r"<details><summary>Other changed files \(\d+\)</summary>\n|</details>\n")
CUT_NOTE = "...and the rest of the review, which is too long for a GitHub comment.\n"


def parse(markdown: str) -> tuple[list[str], list[str]]:
    """The code spans' contents, and anything outside them that GitHub would act on."""
    codes: list[str] = []
    problems: list[str] = []
    for token in GFM.parse(markdown):
        if token.type == "html_block" and not OWN_HTML.fullmatch(token.content):
            problems.append(f"HTML block {token.content!r}")
        for child in token.children or []:
            if child.type == "code_inline":
                codes.append(child.content)
            elif child.type in ("html_inline", "image", "link_open"):
                problems.append(f"{child.type} {child.content!r}")
            elif child.type == "text" and LINKING.search(child.content):
                problems.append(f"text {child.content!r}")
    return codes, problems


def shown(name: str) -> str:
    """A name as its code span holds it. One that folds to nothing is a single space."""
    return _flatten(name, NAME_LIMIT) or " "


def base() -> dict[str, Any]:
    """A review with one of everything, an Airflow task too, for changing a field at a time."""
    return {
        "project": "shop",
        "base": {"ref": "main", "commit": "abc"},
        "artifacts": {"manifestGeneratedAt": None, "dbtVersion": None, "baselineManifest": None},
        "columnLineage": "done",
        "summary": {
            "changedNodes": 1,
            "changedColumns": 1,
            "impacted": {"model": 1, "airflow_task": 1},
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
            },
            {
                "id": "airflow:d.t",
                "kind": "airflow_task",
                "name": "t",
                "owner": None,
                "file": "dags/d.py",
                "depth": 2,
                "reach": "model",
                "confidence": "heuristic",
                "origins": ["dag_static"],
                "columns": [],
                "reason": None,
                "via": [],
                "viaMore": 0,
                "airflow": {
                    "dag": "d",
                    "operator": "BashOperator",
                    "how": ["dbt run"],
                    "runs": ["a"],
                    "runsMore": 0,
                    "tasks": [],
                    "tasksMore": 0,
                    "after": [],
                    "notes": [],
                },
            },
        ],
        "owners": [{"owner": "o", "nodes": ["model.s.b"]}],
        "airflow": {"analysed": True, "reason": None, "dagFiles": 1, "opaqueTasks": 0},
        "unknowns": [{"subject": "model.s.b", "reference": "r", "reason": "why"}],
        "otherFiles": [{"path": "README.md", "status": "modified"}],
        "outsideProject": 0,
        "notes": [],
    }


HOSTILE = [
    "",
    " ",
    "\t \r\n",
    "\x00\x07",
    "\x1b[31m",
    "\u202e\u2066",
    "`",
    "``",
    "` `",
    "a`b",
    "```fence```",
    "|",
    "a|b",
    "\\|",
    "a\\",
    "<img src=x>",
    "<a href=//x.example>y</a>",
    "![a](http://x.example/a.png)",
    "[a](http://x.example)",
    "<http://x.example>",
    "@user",
    "@org/team",
    "#123",
    "é@user",
    "`@user",
    "a@b.example",
    "https://x.example",
    "www.x.example",
    "</details>",
    "**b** _e_ ~~s~~",
    "&#64;user &lt;img&gt;",
    "@user <img src=x> " + "y" * 400,
]
NAMES = st.one_of(
    st.sampled_from(HOSTILE),
    st.text(max_size=30),
    st.text(st.sampled_from("`|\\@#<>![]()&:/w.é1 \t\x00"), max_size=16),
)


@st.composite
def reviews(draw) -> tuple[dict[str, Any], list[str]]:
    """A review with a drawn name in every place one is printed, and the names it must show."""
    must_show: list[str] = []

    def name() -> str:
        must_show.append(draw(NAMES))
        return must_show[-1]

    def names(most: int = 3) -> list[str]:
        return [name() for _ in range(draw(st.integers(0, most)))]

    def maybe() -> str | None:
        """A name that is printed only when there is one, like an owner."""
        value = draw(st.none() | NAMES)
        if value:
            must_show.append(value)
        return value

    def text() -> str:
        return draw(NAMES)

    review = base()
    project, ref, commit = draw(NAMES), draw(NAMES), draw(NAMES)
    must_show.append(project or "dbt project")
    must_show += [ref, commit[:12]] if ref and commit else [commit or ref or "the base"]
    generated = maybe()
    review.update(
        project=project,
        base={"ref": ref, "commit": commit},
        notes=[text() for _ in range(draw(st.integers(0, 2)))],
        artifacts={
            "manifestGeneratedAt": generated,
            "dbtVersion": maybe() if generated else None,
            "baselineManifest": {
                "name": name(),
                "generatedAt": maybe(),
                "dbtVersion": None,
                "sha256": None,
            },
        },
    )

    changed = review["changedNodes"][0]
    changed.update(name=name(), owner=maybe())
    changed["files"] = [
        {"path": name(), "status": "modified", "role": "definition"}
        for _ in range(draw(st.integers(1, 3)))
    ]
    changed["columns"].update(
        removed=names(),
        added=names(),
        typeChanged=[
            {"name": name(), "before": name(), "after": name()}
            for _ in range(draw(st.integers(0, 2)))
        ],
        reason=text(),
    )

    model, task = review["impacted"]
    model.update(name=name(), owner=maybe(), columns=names(5), reason=text())
    model["via"] = [{**model["via"][0], "source": f"model.s.a#{text()}"}]
    task.update(name=name(), owner=maybe(), file=maybe())
    dag = draw(st.none() | NAMES)
    must_show.append(dag or "")
    task["airflow"].update(
        dag=dag,
        how=[text() for _ in range(draw(st.integers(0, 2)))],
        runs=names(),
        tasks=names(),
        after=names(),
        notes=[text()],
    )

    review["owners"] = [{"owner": maybe(), "nodes": [f"model.s.{text()}"]}]
    review["unknowns"] = [{"subject": name(), "reference": name(), "reason": text()}]
    review["otherFiles"] = [
        {"path": name(), "status": "modified"} for _ in range(draw(st.integers(1, 3)))
    ]
    return review, must_show


@given(reviews())
@settings(max_examples=250, suppress_health_check=[HealthCheck.too_slow], deadline=None)
def test_every_name_is_a_whole_code_span_and_nothing_else_is_html_a_link_or_a_mention(drawn):
    review, names = drawn
    codes, problems = parse(render_markdown(review))
    assert not problems
    # A span that swallowed its neighbours, or a name left outside one, does not count
    missing = Counter(shown(n) for n in names) - Counter(codes)
    assert not missing


# (a) A name that folds to nothing


@pytest.mark.parametrize("empty", ["", " ", "\t\r\n", "\x00\x1b", "\u202e\u2066"])
def test_a_name_that_folds_to_nothing_is_a_code_span_of_its_own(empty):
    review = base()
    removed = [empty, "a`b", "<img src=x>", "@user", "![i](http://x.example/i.png)"]
    review["changedNodes"][0]["columns"]["removed"] = removed
    review["impacted"][1]["airflow"]["dag"] = empty or None
    markdown = render_markdown(review)
    codes, problems = parse(markdown)
    assert not problems
    assert not Counter(shown(n) for n in removed) - Counter(codes)
    assert "Removed: ` `, ``a`b``, `<img src=x>`, `@user`, `![i](http://x.example/i.png)`" in (
        markdown
    )
    assert "\n| ` ` | `t` |" in markdown


# (b) A review over the limit


def over_the_limit(padding: int) -> tuple[dict[str, Any], list[str]]:
    """A review still too long with the shortest lists, all of it hostile long names."""
    names = [f"@user{i} <img src=x> ![i](http://x.example/{i}.png) {'y' * 70}" for i in range(30)]
    review = base()
    review["project"] = "p" * padding  # moves where the limit falls
    node = review["changedNodes"][0]
    review["changedNodes"] = [
        {
            **node,
            "id": f"model.s.n{i}",
            "name": names[i],
            "files": [{"path": n, "status": "modified", "role": "definition"} for n in names],
            "columns": {
                **node["columns"],
                "removed": names,
                "added": names,
                "typeChanged": [{"name": n, "before": n, "after": n} for n in names],
            },
        }
        for i in range(12)
    ]
    return review, [review["project"] or "dbt project", *names]


@pytest.mark.parametrize("padding", range(0, 121, 15))
def test_a_review_over_the_limit_is_cut_between_lines_with_every_span_closed(padding):
    review, names = over_the_limit(padding)
    markdown = render_markdown(review)
    codes, problems = parse(markdown)
    assert not problems
    texts = [
        child.content
        for token in GFM.parse(markdown)
        for child in token.children or []
        if child.type == "text"
    ]
    assert not [t for t in texts if "`" in t], "a backtick outside a code span"
    # Every span is a whole name: none was cut, and none swallowed what followed it
    assert set(codes) <= {shown(n) for n in names} | {"HEAD", "main", "abc", "catalog.json", "o"}
    assert len(markdown) <= MAX_CHARS
    assert markdown.endswith(CUT_NOTE)


def test_the_other_files_are_closed_when_the_cut_falls_among_them():
    review = base()
    review["otherFiles"] = [{"path": f"{i}/" + "f" * 110, "status": "modified"} for i in range(10)]
    fits = render_markdown(review)
    note = "n" * 500  # each note is "> note" and a blank line: 504 characters
    review["notes"] = [note] * ((MAX_CHARS - len(fits)) // 504 + 1)
    markdown = render_markdown(review)
    assert len(markdown) <= MAX_CHARS
    assert "\n<details><summary>" in markdown
    assert markdown.endswith("\n\n</details>\n\n" + CUT_NOTE)
    _, problems = parse(markdown)
    assert not problems


def test_a_nodes_column_and_file_lists_are_cut_like_the_rows():
    review = base()
    node = review["changedNodes"][0]
    many = [f"c{i}" for i in range(150)]
    node["files"] = [
        {"path": f"f{i}.sql", "status": "modified", "role": "definition"} for i in range(150)
    ]
    node["columns"].update(
        removed=many,
        added=many,
        typeChanged=[{"name": c, "before": "int", "after": "text"} for c in many],
    )
    review["impacted"][1]["airflow"]["how"] = [f"how {i}" for i in range(8)]
    markdown = render_markdown(review)
    assert "`f99.sql` and 50 more" in markdown and "`f100.sql`" not in markdown
    assert "Removed: `c0`" in markdown and "`c99` and 50 more" in markdown
    assert "  - Type of `c99`: `int` to `text`" in markdown
    assert "Type of `c100`" not in markdown
    assert "  - ...and 50 more type changes." in markdown
    assert "how 4 and 3 more: `a`" in markdown and "how 5" not in markdown


# (c) Mentions, issue references and links in text, whatever comes before them


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("`@user", "\\`@\u200buser"),
        ("é@user", "é@\u200buser"),
        ("x@org/team", "x@\u200borg/team"),
        ("mail a@b.example", "a@\u200bb.example"),
        ("see x#123", "x#\u200b123"),
        ("see https://x.example", "https:\u200b//x.example"),
        ("see WWW.x.example", "WWW\u200b.x.example"),
    ],
)
def test_text_cannot_mention_or_link_whatever_comes_before(text, expected):
    review = base()
    review["unknowns"][0]["reason"] = text
    markdown = render_markdown(review)
    assert expected in markdown
    _, problems = parse(markdown)
    assert not problems


def test_values_the_review_does_not_know_are_escaped_too():
    # Each of these is an enum value today; the text must not depend on that staying true
    review = base()
    hostile = "<img src=x> @user"
    review["changedNodes"][0].update(status=hostile, kind=hostile)
    review["changedNodes"][0]["columns"]["baseline"] = hostile
    review["impacted"][0].update(confidence=hostile, origins=[hostile])
    review["impacted"][1].update(confidence=hostile, origins=[hostile])
    review["otherFiles"][0]["status"] = hostile
    _, problems = parse(render_markdown(review))
    assert not problems
