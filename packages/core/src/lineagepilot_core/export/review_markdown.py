"""The Change Impact Review as Markdown, for pasting into a pull request (tasks 4.4 and 7.7).

This one function produces the text for both the panel's "Copy as Markdown" and ``lineagepilot
review``, so they cannot differ. It reads only the review, whose order is already stable, and
adds nothing about the machine or the time, so the same inputs give the same bytes.

Its sections are in the order of design_next_phase section 7.5: what changed; dbt's nodes, with
the semantic layer; the pipelines that run them, by tool and then by pipeline; the consumers that
read them, by tool; the owners, across tools; the unknowns, by tool; then the other files.

Almost everything here comes from the workspace, and the text is pasted where other people read
it and where GitHub renders it. So a name never reaches the page as Markdown: names and paths are
code spans, in which nothing is rendered and ``@team`` mentions nobody, and other text has its
punctuation escaped.

GitHub refuses a comment over 65,536 characters. Each part of the review (each list, each table,
and each tool's) is cut on its own, with a count of what it leaves out: the longest is cut first,
a step at a time, until the whole fits. So a long list from one tool cannot push another tool's
rows, or the unknowns, out of the review.
"""

from __future__ import annotations

import heapq
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from lineagepilot_core.readers.tools import (
    KIND_WORDS,
    ROW_WORDS,
    TOOL_LABELS,
    count_key,
    counted,
    group_word,
    pipeline_word,
    tool_label,
    tool_of_id,
)

# GitHub refuses a comment over 65,536 characters; leave room for what a person adds
MAX_CHARS = 60_000
# How many rows a list or table shows, in the steps the longest part is cut by
ROW_LIMITS = (100, 50, 25, 10, 5, 2, 1, 0)
NAME_LIMIT = 120
TEXT_LIMIT = 300
CELL_TEXT_LIMIT = 120  # the Unknowns section has the whole reason
SHORT_COMMIT = 12

_KIND_WORDS = {kind: (one, many) for kind, one, many in KIND_WORDS}
# dbt's own nodes, each kind a table of its own
_DBT_KINDS = ("model", "snapshot", "seed", "source", "exposure")
_PIPELINE_KINDS = ("pipeline", "task")
# Consumers that are dbt's own, listed with dbt's nodes rather than with the other tools'
_DBT_TOOLS = frozenset({"dbt_semantic"})
_ORIGINS = {
    "manifest": "manifest",
    "compiled_sql": "compiled SQL",
    "catalog": "catalog",
    "dag_static": "DAG file",
    "annotation": "annotation",
    "dagster_defs": "Dagster definitions",
}
_BASELINES = {
    "baseline_manifest": "the baseline manifest",
    "catalog": "`catalog.json`",
    "none": "no baseline",
}
_STATUSES = {
    "added": "added",
    "modified": "modified",
    "deleted": "deleted",
    "renamed": "renamed",
    "untracked": "new",
    "properties": "properties changed",
}
# Enough to stop text mid-line becoming markup, a link, HTML or a new table cell
_ESCAPED = re.compile(r"([\\`*_\[\]<>|~&])")
# Where GitHub starts a mention, an issue reference, an email link or a bare-URL link. Whatever
# comes before: GitHub's word characters are ASCII only, and an escaped backtick is just text.
_LINKING = re.compile(r"[@#](?=\w)|:(?=//)|(?i:www)(?=\.)")
_ZERO_WIDTH_SPACE = "\u200b"
# The last line when even the shortest parts do not fit
_CUT = "...and the rest of the review, which is too long for a GitHub comment."
# Control characters, keeping the whitespace that _flatten then folds. Also the characters that
# reorder text when displayed (bidirectional controls), which could disguise a name.
_CONTROL = re.compile(r"[\x00-\x08\x0e-\x1f\x7f-\x9f\u200e\u200f\u202a-\u202e\u2066-\u2069]")


@dataclass(frozen=True)
class _Part:
    """A part of the review: its lines for a number of rows. A heading is never cut."""

    lines: Callable[[int], list[str]]
    cut: bool = True


def render_markdown(review: dict[str, Any]) -> str:
    parts = _parts(review)
    steps = [0] * len(parts)
    rendered = [part.lines(ROW_LIMITS[0]) for part in parts]
    sizes = [_size(lines) for lines in rendered]
    total = sum(sizes)
    # The longest part that can still be cut loses a step, and the others stay as they are
    longest = [(-size, i) for i, size in enumerate(sizes) if parts[i].cut]
    heapq.heapify(longest)
    while total > MAX_CHARS and longest:
        _, i = heapq.heappop(longest)
        steps[i] += 1
        rendered[i] = parts[i].lines(ROW_LIMITS[steps[i]])
        total += _size(rendered[i]) - sizes[i]
        sizes[i] = _size(rendered[i])
        if steps[i] < len(ROW_LIMITS) - 1:
            heapq.heappush(longest, (-sizes[i], i))
    lines = [line for part in rendered for line in part]
    while lines and not lines[-1]:
        lines.pop()
    text = "\n".join(lines) + "\n"
    return text if len(text) <= MAX_CHARS else _cut(text)


def _size(lines: list[str]) -> int:
    return sum(len(line) + 1 for line in lines)


def _cut(text: str) -> str:
    """The whole lines that fit, then a note that the rest is missing: only when even every
    part at its shortest is too long, such as with thousands of tools.

    Each line is complete, with every code span closed, so a cut between lines cannot leave one
    open; a cut inside a code span would turn the rest of its name into Markdown. The other
    files' ``<details>`` comes last, so if it was started, it is closed again here.
    """
    close, end = "\n</details>\n", f"\n{_CUT}\n"
    kept = text[: text.rfind("\n", 0, MAX_CHARS - len(close) - len(end)) + 1]
    return kept + (close if "\n<details>" in kept else "") + end


def _parts(review: dict[str, Any]) -> list[_Part]:
    """The review's parts, in order. Each list, table and tool is a part that is cut on its own."""
    head = [f"## Change impact review: {_code(review['project'] or 'dbt project')}", ""]
    head += [_compared_with(review), "", _headline(review), ""]
    parts = [_Part(lambda _: head, cut=False)]
    if review["notes"]:
        parts.append(_Part(lambda limit: _notes(review["notes"], limit)))
    parts.append(_Part(lambda limit: _changed(review, limit)))
    parts += _downstream(review)
    parts += _pipelines(review)
    parts += _consumers(review)
    if review["owners"]:
        parts.append(_Part(lambda limit: _owners(review["owners"], limit)))
    parts += _unknowns(review)
    parts.append(_Part(lambda limit: _other_files(review, limit)))
    return parts


def _heading(*lines: str) -> _Part:
    return _Part(lambda _: [*lines, ""], cut=False)


def _notes(notes: list[str], limit: int) -> list[str]:
    lines = []
    for note in notes[:limit]:
        lines += [f"> {_plain(note, 600)}", ""]
    if len(notes) > limit:
        lines += [f"...and {len(notes) - limit} more notes.", ""]
    return lines


def _compared_with(review: dict[str, Any]) -> str:
    parts = []
    base = review["base"]
    if base is None:
        parts.append("Changes as listed, with no base commit.")
    elif base["ref"] and base["commit"]:
        commit = base["commit"][:SHORT_COMMIT]
        parts.append(
            f"Compared with the merge-base of `HEAD` and {_code(base['ref'])} ({_code(commit)})."
        )
    else:
        parts.append(f"Compared with {_code(base['commit'] or base['ref'] or 'the base')}.")
    artifacts = review["artifacts"]
    if artifacts["manifestGeneratedAt"]:
        version = f" by dbt {_code(artifacts['dbtVersion'])}" if artifacts["dbtVersion"] else ""
        parts.append(f"Manifest written {_code(artifacts['manifestGeneratedAt'])}{version}.")
    baseline = artifacts["baselineManifest"]
    if baseline:
        generated = (
            f" (written {_code(baseline['generatedAt'])})" if baseline["generatedAt"] else ""
        )
        parts.append(f"Baseline manifest {_code(baseline['name'])}{generated}.")
    return " ".join(parts)


def _headline(review: dict[str, Any]) -> str:
    summary = review["summary"]
    parts = [
        _count(summary["changedNodes"], "changed node"),
        _count(summary["changedColumns"], "changed column"),
    ]
    impacted = review["impacted"]
    if impacted:
        kinds: dict[tuple[str, str | None], int] = {}
        for node in impacted:
            key = count_key(node["kind"], node.get("tool"), node.get("subkind"))
            kinds[key] = kinds.get(key, 0) + 1
        # A tool's name comes from the workspace when no reader knows it
        names = ", ".join(_plain(part, NAME_LIMIT) for part in counted(kinds))
        parts.append(f"{len(impacted)} impacted ({names})")
    else:
        parts.append("nothing downstream")
    parts.append(_count(summary["unknowns"], "unknown"))
    return "**" + " · ".join(parts) + "**"


def _changed(review: dict[str, Any], row_limit: int) -> list[str]:
    nodes = review["changedNodes"]
    lines = ["### Changed", ""]
    if not nodes:
        return [*lines, "No changed file maps to a node.", ""]
    for node in nodes[:row_limit]:
        files = _codes([f["path"] for f in node["files"]], row_limit)
        owner = f"; owner {_code(node['owner'])}" if node["owner"] else ""
        status = _label(_STATUSES, node["status"])
        lines.append(f"- {_code(node['name'])} ({_title(node['kind'])}, {status}): {files}{owner}")
        lines += [f"  - {line}" for line in _column_lines(node["columns"], row_limit)]
    lines += _more(len(nodes), row_limit, "changed nodes")
    lines.append("")
    return lines


def _column_lines(diff: dict[str, Any], row_limit: int) -> list[str]:
    """What happened to a node's columns. A wide table's lists are cut like the rows are."""
    if diff["state"] == "unknown":
        return [f"Columns unknown: {_plain(diff['reason'])}"]
    if diff["state"] == "not_compared":
        return []
    lines = []
    if diff["removed"]:
        lines.append(f"Removed: {_codes(diff['removed'], row_limit)}")
    if diff["added"]:
        lines.append(f"Added: {_codes(diff['added'], row_limit)}")
    changes = diff["typeChanged"]
    for change in changes[:row_limit]:
        before, after = _code(change["before"]), _code(change["after"])
        lines.append(f"Type of {_code(change['name'])}: {before} to {after}")
    if len(changes) > row_limit:
        lines.append(f"...and {len(changes) - row_limit} more type changes.")
    against = _label(_BASELINES, diff["baseline"])
    if lines:
        lines.append(f"Compared with {against}.")
    else:
        lines.append(f"No column added, removed or retyped, compared with {against}.")
    if diff["reason"]:
        lines[-1] += f" {_plain(diff['reason'])}"
    return lines


def _downstream(review: dict[str, Any]) -> list[_Part]:
    """dbt's nodes, a table for each kind, then the semantic layer."""
    impacted = review["impacted"]
    parts = [_heading("### Downstream impact")]
    if not impacted:
        parts.append(_heading("Nothing is known to be downstream of the changed nodes."))
    for kind in _DBT_KINDS:
        nodes = [n for n in impacted if n["kind"] == kind]
        if nodes:
            plural = _KIND_WORDS[kind][1]
            parts.append(_table_part(f"{plural[0].upper()}{plural[1:]}", plural, nodes))
    semantic = [n for n in impacted if n["kind"] == "consumer" and n.get("tool") in _DBT_TOOLS]
    if semantic:
        parts.append(_table_part("Semantic layer", "semantic layer nodes", semantic))
    return parts


def _table_part(title: str, what: str, nodes: list[dict[str, Any]]) -> _Part:
    """A table of impacted nodes: how each is reached, how sure that is, and whose it is."""

    def lines(limit: int) -> list[str]:
        rows = [f"#### {title} ({len(nodes)})", ""]
        rows += ["| Name | Reached | Confidence | Evidence | Owner |", "|---|---|---|---|---|"]
        rows += [_impact_row(node) for node in nodes[:limit]]
        return [*rows, *_more(len(nodes), limit, what), ""]

    return _Part(lines)


def _tools(summaries: list[dict[str, Any]], nodes: list[dict[str, Any]]) -> list[str]:
    """The tools a section is about, by name: each reader's, and any other a row has. A reader that
    read and found none of its tool's files has nothing to say here (task 8.2)."""
    readers = {s["tool"] for s in summaries if not (s["analysed"] and not s["files"])}
    tools = readers | {n["tool"] for n in nodes if n.get("tool")}
    return sorted(tools, key=lambda tool: (tool_label(tool).casefold(), tool))


def _summary(summaries: list[dict[str, Any]], tool: str) -> dict[str, Any] | None:
    return next((s for s in summaries if s["tool"] == tool), None)


def _not_analysed(name: str, summary: dict[str, Any] | None, files: str) -> list[str] | None:
    if summary is None or summary["analysed"]:
        return None
    reason = _sentence(_plain(summary["reason"] or f"the {files}s were not read"))
    return [f"#### {name}", "", f"Not analysed: {reason}", ""]


def _pipelines(review: dict[str, Any]) -> list[_Part]:
    """Each tool's tasks, by pipeline, each with what it runs. A heuristic link is shown, with
    why it is one."""
    summaries = review.get("pipelines") or []
    nodes = [n for n in review["impacted"] if n["kind"] in _PIPELINE_KINDS]
    tools = _tools(summaries, nodes)
    if not tools:
        return []
    parts = [_heading("### Pipelines")]
    for tool in tools:
        found = [n for n in nodes if n.get("tool") == tool]
        parts.append(_pipeline_part(tool, _summary(summaries, tool), found))
    return parts


def _pipeline_part(tool: str, summary: dict[str, Any] | None, nodes: list[dict[str, Any]]) -> _Part:
    name = _plain(tool_label(tool), NAME_LIMIT)
    word = pipeline_word(tool, 1)
    nodes = sorted(nodes, key=_row_order)

    def lines(limit: int) -> list[str]:
        not_analysed = _not_analysed(name, summary, f"{word} file")
        if not_analysed is not None:
            return not_analysed
        title = f"#### {name}"
        rows: list[str] = []
        if nodes:
            pipelines = len({_detail(n)["pipeline"] for n in nodes})
            in_pipelines = f"{pipelines} {group_word(tool, pipelines)}"
            title += f" ({_what(nodes)} in {in_pipelines})"
            group = group_word(tool, 1)
            column = group[0].upper() + group[1:]
            labelled = all(n.get("subkind") in _ROWS for n in nodes)
            task = "Definition" if labelled else "Task"
            header = f"{task} | What it has to do with this | Confidence | Evidence | Owner"
            rows += ["", f"| {column} | {header} |", "|---|---|---|---|---|---|"]
            rows += [_task_row(node, word) for node in nodes[:limit]]
            rows += _more(len(nodes), limit, f"{name} tasks")
        elif summary is not None:
            files = _count(summary["files"], f"{word} file")
            rows += ["", f"No task in the {files} read runs a changed or downstream node."]
        if summary is not None and summary["opaqueTasks"]:
            tasks = _count(summary["opaqueTasks"], "task")
            rows += [
                "",
                f"{tasks} could not be read and do not mention dbt, so they are not listed.",
            ]
        return [title, *rows, ""]

    return _Part(lines)


_NO_DETAIL: dict[str, Any] = {
    "pipeline": None,
    "how": [],
    "runs": [],
    "runsMore": 0,
    "tasks": [],
    "tasksMore": 0,
    "after": [],
    "notes": [],
}


def _detail(node: dict[str, Any]) -> dict[str, Any]:
    return node.get("pipeline") or _NO_DETAIL


_ROWS = {subkind: (rank, one, many) for rank, (subkind, one, many) in enumerate(ROW_WORDS)}


def _row_order(node: dict[str, Any]) -> tuple[Any, ...]:
    """By pipeline; Dagster's by definitions, then each job with its schedules, then assets."""
    detail = _detail(node)
    subkind = node.get("subkind")
    if subkind not in _ROWS:
        return (detail["pipeline"] or "", 0, node["kind"], "", node["name"])
    if subkind in ("job", "schedule"):
        job = node["name"] if subkind == "job" else detail.get("job") or node["name"]
        return (detail["pipeline"] or "", 1, job, _ROWS[subkind][0], node["name"])
    return (detail["pipeline"] or "", 0 if subkind == "dbt_assets" else 2, "", 0, node["name"])


def _what(nodes: list[dict[str, Any]]) -> str:
    """How many rows there are, by what each is when every one is labelled, as Dagster's are."""
    if not all(n.get("subkind") in _ROWS for n in nodes):
        return _count(len(nodes), "task")
    counts = []
    for subkind, (_, one, many) in sorted(_ROWS.items(), key=lambda kv: kv[1][0]):
        found = sum(n.get("subkind") == subkind for n in nodes)
        if found:
            counts.append(f"{found} {one if found == 1 else many}")
    return counts[0] if len(counts) == 1 else f"{', '.join(counts[:-1])} and {counts[-1]}"


def _task_row(node: dict[str, Any], word: str) -> str:
    detail = _detail(node)
    subkind = node.get("subkind")
    if subkind in _ROWS:
        task = f"{_ROWS[subkind][1]} {_cell_code(node['name'])}"
    elif node["kind"] == "pipeline":
        task = f"The whole {word}"
    else:
        task = _cell_code(node["name"])
    if detail["tasks"]:
        shown = ", ".join(_cell_code(t) for t in detail["tasks"][:5])
        more = len(detail["tasks"]) - 5 + detail["tasksMore"]
        task += f". Derived: {shown}" + (f" and {more} more" if more > 0 else "")
    parts = []
    if detail["runs"]:
        names = ", ".join(_cell_code(n) for n in detail["runs"][:5])
        more = len(detail["runs"]) - 5 + detail["runsMore"]
        how = ", ".join(_plain(h, 40) for h in detail["how"][:5]) or "runs"
        if len(detail["how"]) > 5:
            how += f" and {len(detail['how']) - 5} more"
        parts.append(f"{how}: {names}" + (f" and {more} more" if more > 0 else ""))
    if detail["after"]:
        parts.append("runs after " + ", ".join(_cell_code(n) for n in detail["after"][:5]))
    if detail.get("job"):
        cron = f", {_cell_code(detail['cron'])}" if detail.get("cron") else ""
        parts.append(f"job {_cell_code(detail['job'])}{cron}")
    origins = ", ".join(_label(_ORIGINS, o) for o in node["origins"])
    evidence = origins + (f" {_cell_code(node['file'])}" if node["file"] else "")
    if detail["notes"]:
        evidence += f": {_plain(detail['notes'][0], CELL_TEXT_LIMIT)}"
    owner = _cell_code(node["owner"]) if node["owner"] else "No owner"
    return (
        f"| {_cell_code(detail['pipeline'] or '')} | {task} | {'; '.join(parts)} | "
        f"{_plain(node['confidence'])} | {evidence} | {owner} |"
    )


def _consumers(review: dict[str, Any]) -> list[_Part]:
    """What reads the changed nodes from outside dbt, by tool, with the fields of each that
    break. Only when a reader of consumers is turned on, or something declares a consumer."""
    summaries = [s for s in review.get("consumers") or [] if s["tool"] not in _DBT_TOOLS]
    nodes = [
        n for n in review["impacted"] if n["kind"] == "consumer" and n.get("tool") not in _DBT_TOOLS
    ]
    tools = _tools(summaries, nodes)
    if not tools:
        return []
    parts = [_heading("### Consumers")]
    for tool in tools:
        found = [n for n in nodes if n.get("tool") == tool]
        parts.append(_consumer_part(tool, _summary(summaries, tool), found))
    return parts


def _consumer_part(tool: str, summary: dict[str, Any] | None, nodes: list[dict[str, Any]]) -> _Part:
    name = _plain(tool_label(tool), NAME_LIMIT)
    table = _table_part(name, f"{name} consumers", nodes)

    def lines(limit: int) -> list[str]:
        not_analysed = _not_analysed(name, summary, "file")
        if not_analysed is not None:
            return not_analysed
        if nodes:
            return table.lines(limit)
        files = _count(summary["files"] if summary else 0, "file")
        return [
            f"#### {name}",
            "",
            f"No consumer in the {files} read uses a changed or downstream node.",
            "",
        ]

    return _Part(lines)


def _owners(owners: list[dict[str, Any]], limit: int) -> list[str]:
    lines = ["### Owners", "", "| Owner | Impacted |", "|---|---|"]
    for entry in owners[:limit]:
        nodes = entry["nodes"]
        names = ", ".join(_cell_code(_short_id(n)) for n in nodes[:10])
        more = f" and {len(nodes) - 10} more" if len(nodes) > 10 else ""
        who = _cell_code(entry["owner"]) if entry["owner"] else "No owner"
        lines.append(f"| {who} | {names}{more} |")
    return [*lines, *_more(len(owners), limit, "owners"), ""]


def _impact_row(node: dict[str, Any]) -> str:
    reached = "Model level"
    if node["reason"]:
        reached += f": {_plain(node['reason'], CELL_TEXT_LIMIT)}"
    if node["columns"]:
        shown = ", ".join(_cell_code(c) for c in node["columns"][:5])
        more = f" and {len(node['columns']) - 5} more" if len(node["columns"]) > 5 else ""
        # A consumer's columns are its fields: a metric's measures, a view's dimensions
        word = "Field" if node["kind"] == "consumer" else "Column"
        label = f"{word}s" if len(node["columns"]) > 1 else word
        columns = f"{label} {shown}{more}"
        reached = columns if node["reach"] == "column" else f"{reached}. {columns}"
    owner = _cell_code(node["owner"]) if node["owner"] else "No owner"
    name = _cell_code(node["name"])
    about = []
    if node.get("tool") and node.get("subkind"):
        about.append(_plain(node["subkind"].replace("_", " "), NAME_LIMIT))
    if node.get("exports"):  # a saved query's: the tables it writes change too
        about.append(f"exports {', '.join(_cell_code(e) for e in node['exports'][:3])}")
        if len(node["exports"]) > 3:
            about[-1] += f" and {len(node['exports']) - 3} more"
    if about:
        name += f" ({'; '.join(about)})"
    return f"| {name} | {reached} | {_plain(node['confidence'])} | {_evidence(node)} | {owner} |"


def _evidence(node: dict[str, Any]) -> str:
    """Where the claim comes from: the kinds of evidence, and what it was reached from."""
    origins = ", ".join(_label(_ORIGINS, o) for o in node["origins"])
    via = node["via"]
    if not via:
        return origins
    sources = list(dict.fromkeys(_short_ref(v["source"]) for v in via))
    shown = ", ".join(_cell_code(s) for s in sources[:2])
    more = len(sources) - 2 + node["viaMore"]
    return f"{origins}, from {shown}" + (f" and {more} more" if more > 0 else "")


def _unknowns(review: dict[str, Any]) -> list[_Part]:
    """The unknowns, a list for each tool, dbt's first."""
    unknowns = review["unknowns"]
    parts = [_heading(f"### Unknowns ({len(unknowns)})")]
    if not unknowns:
        return [*parts, _heading("None.")]
    groups = _unknown_groups(review)
    for name, found in groups:
        title = f"#### {name} ({len(found)})" if len(groups) > 1 else None
        parts.append(
            _Part(lambda limit, title=title, found=found: _unknown_list(title, found, limit))
        )
    return parts


def _unknown_groups(review: dict[str, Any]) -> list[tuple[str, list[dict[str, Any]]]]:
    """A review's unknowns are about a node, a column or a changed file. A pipeline's, task's or
    consumer's ID starts with its tool; anything else is dbt's."""
    known = set(TOOL_LABELS) | {n["tool"] for n in review["impacted"] if n.get("tool")}
    known |= {s["tool"] for s in (review.get("pipelines") or []) + (review.get("consumers") or [])}
    groups: dict[str, list[dict[str, Any]]] = {}
    for unknown in review["unknowns"]:
        tool = tool_of_id(unknown["subject"])
        name = _plain(tool_label(tool), NAME_LIMIT) if tool in known else "dbt"
        groups.setdefault(name, []).append(unknown)
    return sorted(groups.items(), key=lambda group: (group[0] != "dbt", group[0].casefold()))


def _unknown_list(title: str | None, unknowns: list[dict[str, Any]], limit: int) -> list[str]:
    lines = [title, ""] if title else []
    for unknown in unknowns[:limit]:
        lines.append(
            f"- {_code(unknown['subject'])}, {_code(unknown['reference'])}: "
            f"{_plain(unknown['reason'])}"
        )
    return [*lines, *_more(len(unknowns), limit, "unknowns"), ""]


def _other_files(review: dict[str, Any], row_limit: int) -> list[str]:
    files = review["otherFiles"]
    outside = review["outsideProject"]
    if not files and not outside:
        return []
    lines = [f"<details><summary>Other changed files ({len(files) + outside})</summary>", ""]
    for file in files[:row_limit]:
        lines.append(f"- {_code(file['path'])} ({_label(_STATUSES, file['status'])})")
    lines += _more(len(files), row_limit, "files")
    if outside:
        lines.append(f"- {_count(outside, 'file')} outside this dbt project")
    lines += ["", "</details>", ""]
    return lines


# Text


def _more(total: int, limit: int, what: str) -> list[str]:
    """How many more there are, after a blank line so that a table or a list ends first."""
    return [f"\n...and {total - limit} more {what}."] if total > limit else []


def _count(count: int, singular: str) -> str:
    return f"{count} {singular}{'' if count == 1 else 's'}"


def _title(kind: str) -> str:
    return _KIND_WORDS[kind][0] if kind in _KIND_WORDS else _plain(kind)


def _sentence(text: str) -> str:
    return text if text.endswith((".", "!", "?")) else f"{text}."


def _label(labels: dict[str, str], value: str) -> str:
    """Our words for a value we know. Any other value is not ours, so it is escaped like text."""
    return labels[value] if value in labels else _plain(value)


def _short_id(node_id: str) -> str:
    """``orders`` for ``model.shop.orders``; ``daily.marts`` for ``airflow:daily.marts``."""
    tool = tool_of_id(node_id)
    if tool is not None:
        return node_id[len(tool) + 1 :]
    return node_id.rsplit(".", 1)[-1] or node_id


def _short_ref(ref: str) -> str:
    """``model.shop.orders#total`` as ``orders.total``."""
    node_id, sep, column = ref.partition("#")
    return f"{_short_id(node_id)}.{column}" if sep else _short_id(node_id)


def _codes(names: list[str], limit: int) -> str:
    """The first names of a list as code spans, and how many more there are."""
    shown = ", ".join(_code(name) for name in names[:limit])
    return shown + (f" and {len(names) - limit} more" if len(names) > limit else "")


def _flatten(text: str, limit: int) -> str:
    """One line, with nothing a terminal or an editor would act on, and no longer than a limit."""
    text = " ".join(_CONTROL.sub("", text).split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _code(text: str, limit: int = NAME_LIMIT) -> str:
    """A code span: its content is shown exactly, and nothing in it is rendered or linked."""
    flat = _flatten(text, limit)
    if not flat:
        # Two backticks with nothing between them are not a span: they wait for the next two,
        # which can be the opening of a later name's span, and that name is then Markdown
        return "` `"
    run = max((len(r) for r in re.findall(r"`+", flat)), default=0)
    fence = "`" * (run + 1)
    pad = " " if flat.startswith("`") or flat.endswith("`") else ""
    return f"{fence}{pad}{flat}{pad}{fence}"


def _cell_code(text: str, limit: int = NAME_LIMIT) -> str:
    """A code span in a table, where a pipe ends the cell unless it is escaped, even here."""
    return _code(text, limit).replace("|", "\\|")


def _plain(text: str | None, limit: int = TEXT_LIMIT) -> str:
    """Text with its Markdown punctuation escaped, and nothing GitHub would link or mention."""
    flat = _flatten(text or "", limit)
    flat = _LINKING.sub(lambda m: m.group() + _ZERO_WIDTH_SPACE, flat)
    return _ESCAPED.sub(r"\\\1", flat)
