"""The Change Impact Review as Markdown, for pasting into a pull request (task 4.4).

This one function produces the text for both the panel's "Copy as Markdown" and ``lineagepilot
review``, so they cannot differ. It reads only the review, whose order is already stable, and
adds nothing about the machine or the time, so the same inputs give the same bytes.

Almost everything here comes from the workspace, and the text is pasted where other people read
it and where GitHub renders it. So a name never reaches the page as Markdown: names and paths are
code spans, in which nothing is rendered and ``@team`` mentions nobody, and other text has its
punctuation escaped. A long list is cut, with a count, to stay under GitHub's 65,536-character
limit on a comment.
"""

from __future__ import annotations

import re
from typing import Any

# GitHub refuses a comment over 65,536 characters; leave room for what a person adds
MAX_CHARS = 60_000
ROW_LIMITS = (100, 50, 25, 10)
NAME_LIMIT = 120
TEXT_LIMIT = 300
CELL_TEXT_LIMIT = 120  # the Unknowns section has the whole reason
SHORT_COMMIT = 12

KIND_TITLES = {
    "model": ("model", "models"),
    "snapshot": ("snapshot", "snapshots"),
    "seed": ("seed", "seeds"),
    "source": ("source", "sources"),
    "exposure": ("exposure", "exposures"),
    "airflow_task": ("Airflow task", "Airflow tasks"),
    "airflow_dag": ("Airflow DAG", "Airflow DAGs"),
}
_ORIGINS = {
    "manifest": "manifest",
    "compiled_sql": "compiled SQL",
    "catalog": "catalog",
    "dag_static": "DAG file",
    "annotation": "annotation",
}
_AIRFLOW_KINDS = ("airflow_task", "airflow_dag")
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
# The last line when even the shortest lists do not fit
_CUT = "...and the rest of the review, which is too long for a GitHub comment."
# Control characters, keeping the whitespace that _flatten then folds. Also the characters that
# reorder text when displayed (bidirectional controls), which could disguise a name.
_CONTROL = re.compile(r"[\x00-\x08\x0e-\x1f\x7f-\x9f\u200e\u200f\u202a-\u202e\u2066-\u2069]")


def render_markdown(review: dict[str, Any]) -> str:
    for limit in ROW_LIMITS:
        text = _render(review, limit)
        if len(text) <= MAX_CHARS:
            return text
    return _cut(text)


def _cut(text: str) -> str:
    """The whole lines that fit, then a note that the rest is missing.

    Each line is complete, with every code span closed, so a cut between lines cannot leave one
    open; a cut inside a code span would turn the rest of its name into Markdown. The other
    files' ``<details>`` comes last, so if it was started, it is closed again here.
    """
    close, end = "\n</details>\n", f"\n{_CUT}\n"
    kept = text[: text.rfind("\n", 0, MAX_CHARS - len(close) - len(end)) + 1]
    return kept + (close if "\n<details>" in kept else "") + end


def _render(review: dict[str, Any], row_limit: int) -> str:
    lines: list[str] = [f"## Change impact review: {_code(review['project'] or 'dbt project')}", ""]
    lines += [_compared_with(review), "", _headline(review), ""]
    for note in review["notes"]:
        lines += [f"> {_plain(note, 600)}", ""]
    lines += _changed(review, row_limit)
    lines += _downstream(review, row_limit)
    lines += _unknowns(review, row_limit)
    lines += _other_files(review, row_limit)
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines) + "\n"


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
    kinds = summary["impacted"]
    if kinds:
        names = ", ".join(_of_kind(kinds[k], k) for k in KIND_TITLES if k in kinds)
        parts.append(f"{sum(kinds.values())} impacted ({names})")
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


def _downstream(review: dict[str, Any], row_limit: int) -> list[str]:
    lines = ["### Downstream impact", ""]
    impacted = review["impacted"]
    if not impacted:
        lines += ["Nothing is known to be downstream of the changed nodes.", ""]
    for kind, (_, plural) in KIND_TITLES.items():
        nodes = [n for n in impacted if n["kind"] == kind]
        if not nodes or kind in _AIRFLOW_KINDS:
            continue
        lines += [f"#### {plural[0].upper()}{plural[1:]} ({len(nodes)})", ""]
        lines += ["| Name | Reached | Confidence | Evidence | Owner |", "|---|---|---|---|---|"]
        lines += [_impact_row(node) for node in nodes[:row_limit]]
        lines += _more(len(nodes), row_limit, plural)
        lines.append("")
    lines += _airflow(review, row_limit)
    owners = review["owners"]
    if owners:
        lines += ["#### Owners", "", "| Owner | Impacted |", "|---|---|"]
        for entry in owners[:row_limit]:
            nodes = entry["nodes"]
            names = ", ".join(_cell_code(_short_id(n)) for n in nodes[:10])
            more = f" and {len(nodes) - 10} more" if len(nodes) > 10 else ""
            who = _cell_code(entry["owner"]) if entry["owner"] else "No owner"
            lines.append(f"| {who} | {names}{more} |")
        lines += _more(len(owners), row_limit, "owners")
        lines.append("")
    return lines


def _airflow(review: dict[str, Any], row_limit: int) -> list[str]:
    """The tasks by DAG, each with what it runs. A heuristic link is shown, with why it is one."""
    summary = review["airflow"]
    lines = ["#### Airflow tasks"]
    if not summary["analysed"]:
        reason = _plain(summary["reason"] or "they were not read")
        return [*lines, "", f"Airflow DAGs were not analysed: {reason}", ""]
    nodes = sorted(
        (n for n in review["impacted"] if n["kind"] in _AIRFLOW_KINDS),
        key=lambda n: (n["airflow"]["dag"] or "", n["kind"], n["name"]),
    )
    if nodes:
        dags = {n["airflow"]["dag"] for n in nodes}
        lines[0] += f" ({_count(len(nodes), 'task')} in {_count(len(dags), 'DAG')})"
        lines += [
            "",
            "| DAG | Task | What it has to do with this | Confidence | Evidence | Owner |",
        ]
        lines += ["|---|---|---|---|---|---|"]
        lines += [_airflow_row(node) for node in nodes[:row_limit]]
        lines += _more(len(nodes), row_limit, "Airflow tasks")
    else:
        files = _count(summary["dagFiles"], "DAG file")
        lines += ["", f"No task in the {files} read runs a changed or downstream node."]
    if summary["opaqueTasks"]:
        tasks = _count(summary["opaqueTasks"], "task")
        lines += ["", f"{tasks} could not be read and do not mention dbt, so they are not listed."]
    lines.append("")
    return lines


def _airflow_row(node: dict[str, Any]) -> str:
    detail = node["airflow"]
    is_whole_dag = node["kind"] == "airflow_dag"
    task = "The whole DAG" if is_whole_dag else _cell_code(node["name"])
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
    origins = ", ".join(_label(_ORIGINS, o) for o in node["origins"])
    evidence = origins + (f" {_cell_code(node['file'])}" if node["file"] else "")
    if detail["notes"]:
        evidence += f": {_plain(detail['notes'][0], CELL_TEXT_LIMIT)}"
    owner = _cell_code(node["owner"]) if node["owner"] else "No owner"
    return (
        f"| {_cell_code(detail['dag'] or '')} | {task} | {'; '.join(parts)} | "
        f"{_plain(node['confidence'])} | {evidence} | {owner} |"
    )


def _impact_row(node: dict[str, Any]) -> str:
    reached = "Model level"
    if node["reason"]:
        reached += f": {_plain(node['reason'], CELL_TEXT_LIMIT)}"
    if node["columns"]:
        shown = ", ".join(_cell_code(c) for c in node["columns"][:5])
        more = f" and {len(node['columns']) - 5} more" if len(node["columns"]) > 5 else ""
        label = "Columns" if len(node["columns"]) > 1 else "Column"
        columns = f"{label} {shown}{more}"
        reached = columns if node["reach"] == "column" else f"{reached}. {columns}"
    owner = _cell_code(node["owner"]) if node["owner"] else "No owner"
    return (
        f"| {_cell_code(node['name'])} | {reached} | {_plain(node['confidence'])} | "
        f"{_evidence(node)} | {owner} |"
    )


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


def _unknowns(review: dict[str, Any], row_limit: int) -> list[str]:
    unknowns = review["unknowns"]
    lines = [f"### Unknowns ({len(unknowns)})", ""]
    if not unknowns:
        return [*lines, "None.", ""]
    for unknown in unknowns[:row_limit]:
        lines.append(
            f"- {_code(unknown['subject'])}, {_code(unknown['reference'])}: "
            f"{_plain(unknown['reason'])}"
        )
    lines += _more(len(unknowns), row_limit, "unknowns")
    lines.append("")
    return lines


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
    return [f"\n...and {total - limit} more {what}."] if total > limit else []


def _count(count: int, singular: str) -> str:
    return f"{count} {singular}{'' if count == 1 else 's'}"


def _of_kind(count: int, kind: str) -> str:
    singular, plural = KIND_TITLES.get(kind) or (_plain(kind), _plain(f"{kind}s"))
    return f"{count} {singular if count == 1 else plural}"


def _title(kind: str) -> str:
    return KIND_TITLES[kind][0] if kind in KIND_TITLES else _plain(kind)


def _label(labels: dict[str, str], value: str) -> str:
    """Our words for a value we know. Any other value is not ours, so it is escaped like text."""
    return labels[value] if value in labels else _plain(value)


def _short_id(node_id: str) -> str:
    if node_id.startswith("airflow:"):
        return node_id.removeprefix("airflow:")
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
