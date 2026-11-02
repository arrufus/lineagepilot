"""CodeLens, hover and diagnostics (task 2.2, design section 7).

- **CodeLens** on a model's SQL file, and on each entry in a YAML properties file:
  ``↓ 14 models · 3 Airflow tasks · 1 exposure · PII``, counted at model level over
  everything downstream. Clicking it runs ``lineagePilot.showImpact`` with the node ID.
- **Hover** on ``ref()`` and ``source()`` calls, and on the current model's columns: the
  direct upstream and downstream, each edge with its evidence (confidence and origin).
- **Diagnostics** from the rules (design section 5.3): failures with the rule's
  severity, ``unknown`` results as information ("not checked"), ``unsupported`` ones as
  hints, each with its rule ID and a link to the rule's documentation. PII rules are
  labelled as indicators. Problems in the rule pack show on the pack file.

Everything from the workspace that reaches Markdown is escaped, so a crafted manifest
cannot add links, images or HTML to a hover.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from lineagepilot_core.analysis.impact import weakest
from lineagepilot_core.analysis.traversal import Direction, Granularity, Step, steps
from lineagepilot_core.graph.ids import split_column_id
from lineagepilot_core.graph.model import Edge, LineageState, Node, NodeKind
from lineagepilot_core.rules import Category, Evaluation, Result, RulePack, Severity

from lineagepilot_lsp.context import GraphContext
from lineagepilot_lsp.locate import Span, entry_key, yaml_entries

SHOW_IMPACT_COMMAND = "lineagePilot.showImpact"
RULES_DOCS_URL = "https://github.com/arrufus/lineagepilot/blob/main/docs/rules.md"
SOURCE = "LineagePilot"
YAML_SUFFIXES = (".yml", ".yaml")
LENS_KINDS = frozenset(
    {NodeKind.MODEL, NodeKind.SEED, NodeKind.SNAPSHOT, NodeKind.SOURCE, NodeKind.EXPOSURE}
)
HOVER_LIST_LIMIT = 10
DESCRIPTION_LIMIT = 300

# Downstream kinds in CodeLens order: (kind, one, many)
_LENS_WORDS = (
    ("model", "model", "models"),
    ("snapshot", "snapshot", "snapshots"),
    ("seed", "seed", "seeds"),
    ("source", "source", "sources"),
    ("exposure", "exposure", "exposures"),
    ("airflow_task", "Airflow task", "Airflow tasks"),
    ("airflow_dag", "Airflow DAG", "Airflow DAGs"),
)

# LSP DiagnosticSeverity
ERROR, WARNING, INFORMATION, HINT = 1, 2, 3, 4
_SEVERITY = {
    Severity.CRITICAL: ERROR,
    Severity.ERROR: ERROR,
    Severity.WARNING: WARNING,
    Severity.INFO: INFORMATION,
}

_MD_SPECIAL = re.compile(r"([\\`*_{}\[\]()#+\-.!|<>~&])")
_REF = re.compile(
    r"""\bref\s*\(\s*(['"])(?P<a>[^'"]+)\1\s*"""
    r"""(?:,\s*(['"])(?P<b>[^'"]+)\3\s*)?"""
    r"""(?:,\s*(?:v|version)\s*=\s*(['"]?)(?P<v>[\w.]+)\5\s*)?\)"""
)
_SOURCE = re.compile(r"""\bsource\s*\(\s*(['"])(?P<s>[^'"]+)\1\s*,\s*(['"])(?P<t>[^'"]+)\3\s*\)""")
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")


def md(text: str | None) -> str:
    """Markdown-escape untrusted text, on one line."""
    return _MD_SPECIAL.sub(r"\\\1", " ".join((text or "").split()))


def _range(span: Span | None, line: int = 0) -> dict[str, Any]:
    if span is not None:
        return span.to_json()
    return {"start": {"line": line, "character": 0}, "end": {"line": line + 1, "character": 0}}


# CodeLens


def lens_title(counts: dict[str, int], pii: bool) -> str:
    parts = [
        f"{n} {one if n == 1 else many}"
        for kind, one, many in _LENS_WORDS
        if (n := counts.get(kind))
    ]
    title = "↓ " + " · ".join(parts) if parts else "↓ nothing downstream"
    return f"{title} · PII" if pii else title


def code_lenses(ctx: GraphContext, path: Path, text: str) -> list[dict[str, Any]]:
    nodes = [n for n in ctx.nodes_for_file(path) if n.kind in LENS_KINDS]
    if not nodes:
        return []
    entries = yaml_entries(text) if path.suffix in YAML_SUFFIXES else None
    lenses = []
    for node in nodes:
        line = 0
        if entries is not None:
            key = entry_key(node)
            span = entries.get(*key) if key else None
            if span is None:
                continue
            line = span.start_line
        position = {"line": line, "character": 0}
        lenses.append(
            {
                "range": {"start": position, "end": position},
                "command": {
                    "title": lens_title(ctx.downstream_counts(node.id), ctx.has_pii(node.id)),
                    "command": SHOW_IMPACT_COMMAND,
                    "arguments": [node.id],
                },
            }
        )
    return lenses


# Hover


def hover(
    ctx: GraphContext, path: Path, text: str, line: int, character: int
) -> dict[str, Any] | None:
    if path.suffix != ".sql":
        return None
    lines = text.splitlines()
    if line >= len(lines):
        return None
    current = lines[line]
    here = [n for n in ctx.nodes_for_file(path) if n.kind in (NodeKind.MODEL, NodeKind.SNAPSHOT)]
    for match in _REF.finditer(current):
        if match.start() <= character <= match.end():
            node = resolve_ref(ctx, match["a"], match["b"], match["v"])
            return _hover(
                node_markdown(ctx, node, here) if node else _missing("ref", match), line, match
            )
    for match in _SOURCE.finditer(current):
        if match.start() <= character <= match.end():
            node = resolve_source(ctx, match["s"], match["t"])
            return _hover(
                node_markdown(ctx, node, here) if node else _missing("source", match), line, match
            )
    for match in _WORD.finditer(current):
        if match.start() <= character <= match.end():
            for model in here:
                column = _column_named(ctx, model, match.group())
                if column is not None:
                    return _hover(column_markdown(ctx, column), line, match)
            return None
    return None


def _hover(markdown: str, line: int, match: re.Match[str]) -> dict[str, Any]:
    return {
        "contents": {"kind": "markdown", "value": markdown},
        "range": {
            "start": {"line": line, "character": match.start()},
            "end": {"line": line, "character": match.end()},
        },
    }


def _missing(kind: str, match: re.Match[str]) -> str:
    return (
        f"**Not in the lineage graph:** {md(match.group())}\n\n"
        "Run dbt parse to refresh the manifest."
    )


def resolve_ref(
    ctx: GraphContext, first: str, second: str | None, version: str | None
) -> Node | None:
    package, name = (first, second) if second else (None, first)
    candidates = [
        n
        for n in ctx.graph.nodes.values()
        if n.kind in (NodeKind.MODEL, NodeKind.SEED, NodeKind.SNAPSHOT)
        and n.name == name
        and (package is None or n.package == package)
        and (version is None or str(n.version) == version)
    ]
    if not candidates:
        return None
    own = [n for n in candidates if n.package == (ctx.graph.project_name or ctx.project.name)]
    pool = own or candidates
    # An unpinned ref to a versioned model means its latest version
    return max(pool, key=lambda n: (_version_key(n.version), n.id))


def _version_key(version: str | None) -> tuple[int, ...]:
    if version is None:
        return ()
    return tuple(int(p) if p.isdigit() else -1 for p in str(version).split("."))


def resolve_source(ctx: GraphContext, source: str, table: str) -> Node | None:
    suffix = f".{source}.{table}"
    candidates = [
        n for n in ctx.graph.nodes.values() if n.kind is NodeKind.SOURCE and n.id.endswith(suffix)
    ]
    own = [n for n in candidates if n.package == (ctx.graph.project_name or ctx.project.name)]
    return (own or candidates or [None])[0]


def _column_named(ctx: GraphContext, node: Node, word: str) -> str | None:
    lowered = word.lower()
    for column in ctx.graph.columns_of(node.id):
        if column.name.lower() == lowered:
            return column.id
    return None


def _evidence(edges: Iterable[Edge]) -> str:
    edges = list(edges)
    confidence = weakest(*(e.confidence for e in edges)).value
    origins = sorted({ev.origin.value.replace("_", " ") for e in edges for ev in e.evidence})
    return f"{confidence}, {', '.join(origins)}"


def _step_line(ctx: GraphContext, step: Step, label: str) -> str:
    edges = step.edges
    how = edges[0].transformation if len(edges) == 1 else None
    via = ""
    if len(edges) > 1:
        shared = {edges[0].upstream, edges[0].downstream} & {edges[1].upstream, edges[1].downstream}
        via = f" · via table {md(next(iter(shared), ''))}"
    detail = " · ".join(p for p in (md(how) if how else "", _evidence(edges)) if p)
    return f"- {label} · {detail}{via}"


def _node_label(ctx: GraphContext, node_id: str) -> str:
    node = ctx.graph.nodes[node_id]
    return f"**{md(node.name)}** ({node.kind.value})"


def node_markdown(ctx: GraphContext, node: Node, here: list[Node]) -> str:
    lines = [f"### {md(node.name)}", f"{node.kind.value} · {md(node.id)}"]
    if node.description:
        lines += ["", md(node.description[:DESCRIPTION_LIMIT])]
    upstream = sorted(
        steps(ctx.graph, node.id, Direction.UPSTREAM, Granularity.MODEL), key=lambda s: s.ref
    )
    downstream = sorted(
        steps(ctx.graph, node.id, Direction.DOWNSTREAM, Granularity.MODEL), key=lambda s: s.ref
    )
    for title, found in (("Upstream", upstream), ("Downstream", downstream)):
        lines += ["", f"**{title}** ({len(found)} direct)"]
        lines += [_step_line(ctx, s, _node_label(ctx, s.ref)) for s in found[:HOVER_LIST_LIMIT]]
        if len(found) > HOVER_LIST_LIMIT:
            lines.append(f"- and {len(found) - HOVER_LIST_LIMIT} more")
    counts = ctx.downstream_counts(node.id)
    if counts:
        lines += ["", "In all downstream: " + lens_title(counts, False).removeprefix("↓ ")]
    if ctx.has_pii(node.id):
        lines += ["", "PII indicators on its columns (not verified exposure)."]
    status = ctx.graph.column_lineage.get(node.id)
    if status is not None:
        reason = f": {md(status.reason)}" if status.reason else ""
        lines += ["", f"Column lineage: {status.state.value}{reason}"]
    return "\n".join(lines)


def column_markdown(ctx: GraphContext, column_id: str) -> str:
    column = ctx.graph.columns[column_id]
    node = ctx.graph.nodes[column.node_id]
    if ctx.columns_computing():
        ctx.on_demand.start_request()
        ctx.on_demand.status(node.id)
        for step in steps(ctx.graph, node.id, Direction.DOWNSTREAM, Granularity.MODEL):
            ctx.on_demand.status(step.ref)
    kind = f"column · {md(column.data_type)}" if column.data_type else "column"
    lines = [f"### {md(node.name)}.{md(column.name)}", kind]
    if column.description:
        lines += ["", md(column.description[:DESCRIPTION_LIMIT])]
    for title, direction in (("Derived from", Direction.UPSTREAM), ("Feeds", Direction.DOWNSTREAM)):
        found = sorted(
            steps(ctx.graph, column_id, direction, Granularity.COLUMN), key=lambda s: s.ref
        )
        lines += ["", f"**{title}** ({len(found)})"]
        for step in found[:HOVER_LIST_LIMIT]:
            other, name = split_column_id(step.ref)
            label = f"**{md(ctx.graph.nodes[other].name)}.{md(name)}**"
            lines.append(_step_line(ctx, step, label))
        if len(found) > HOVER_LIST_LIMIT:
            lines.append(f"- and {len(found) - HOVER_LIST_LIMIT} more")
    status = ctx.graph.column_lineage.get(node.id)
    if status is None:
        lines += ["", "Column lineage: not computed yet"]
    elif status.state is not LineageState.RESOLVED:
        reason = f": {md(status.reason)}" if status.reason else ""
        lines += ["", f"Column lineage of {md(node.name)}: {status.state.value}{reason}"]
    indicator = ctx.pii.indicators.get(column_id)
    if indicator is not None:
        masked = " (masked)" if indicator.masked else ""
        lines += [
            "",
            f"PII indicator{masked}: {md(', '.join(indicator.pii_types))}, "
            f"from {indicator.basis.value} ({indicator.confidence.value}); not verified exposure",
        ]
    return "\n".join(lines)


# Diagnostics


def diagnostics(
    ctx: GraphContext,
    evaluation: Evaluation,
    pack: RulePack | None,
    pack_path: Path | None,
    restricted: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """Diagnostics by file URI. ``restricted``: the workspace is not trusted, so a rule
    pack at ``pack_path`` was not loaded, and says so on the file."""
    by_file: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for finding in evaluation.findings:
        location = finding.evidence.location
        node_id, _ = (
            split_column_id(finding.subject) if "#" in finding.subject else (finding.subject, "")
        )
        node = ctx.graph.nodes.get(node_id)
        if location is None or node is None:
            continue
        path = ctx.locator.file(node, location.path)
        if path is None:
            continue
        span = None
        if path.suffix in YAML_SUFFIXES:
            key = entry_key(node)
            if key is not None:
                entries = ctx.locator.entries(path)
                if "#" in finding.subject:
                    span = entries.get(*key, "columns", finding.subject.partition("#")[2].lower())
                span = span or entries.get(*key)
        rule = evaluation.rules.get(finding.rule_id)
        message = finding.message
        if rule is not None and rule.category is Category.PII:
            message = f"PII indicator: {message}"
        if finding.result is Result.FAIL:
            severity = _SEVERITY[finding.severity]
            if finding.remediation:
                message = f"{message}\n{finding.remediation}"
        elif finding.result is Result.UNKNOWN:
            severity, message = INFORMATION, f"Not checked: {message}"
        else:
            severity, message = HINT, f"Cannot be checked: {message}"
        diagnostic = {
            "range": _range(span),
            "severity": severity,
            "code": finding.rule_id,
            "source": SOURCE,
            "message": message,
        }
        if rule is not None and rule.source == "builtin":
            diagnostic["codeDescription"] = {"href": f"{RULES_DOCS_URL}#{finding.rule_id.lower()}"}
        by_file[path.as_uri()].append(diagnostic)
    if pack is not None and pack_path is not None:
        for problem in pack.problems + [p for p in evaluation.problems if p not in pack.problems]:
            by_file[pack_path.as_uri()].append(
                {
                    "range": _range(None),
                    "severity": WARNING,
                    "code": "rule-pack",
                    "source": SOURCE,
                    "message": problem,
                }
            )
    if restricted and pack_path is not None and pack_path.is_file():
        by_file[pack_path.as_uri()].append(
            {
                "range": _range(None),
                "severity": INFORMATION,
                "code": "restricted-mode",
                "source": SOURCE,
                "message": (
                    "Custom rules are not loaded in Restricted Mode; "
                    "trust the workspace to use them."
                ),
            }
        )
    for found in by_file.values():
        found.sort(key=lambda d: (d["range"]["start"]["line"], d["code"], d["message"]))
    return dict(by_file)
