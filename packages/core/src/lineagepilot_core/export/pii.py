"""PII inventory exports: Markdown and CSV (design section 7, task 1.7).

Replaces data-capsule's HTML and CSV reports (``api/routers/reports.py``), which put
names into HTML unescaped and left spreadsheet formulas live (LP-aml). There is no HTML
report now. Markdown cells are escaped and CSV cells neutralised, so a column named
``=HYPERLINK(...)`` or ``<img onerror=...>`` is inert text.
"""

from __future__ import annotations

import csv
import io
from collections import Counter

from lineagepilot_core.analysis.pii import PIIIndex
from lineagepilot_core.export.escape import csv_cell, markdown_cell
from lineagepilot_core.graph.ids import split_column_id
from lineagepilot_core.graph.model import MetadataGraph

CSV_COLUMNS = (
    "node",
    "column",
    "pii_types",
    "basis",
    "confidence",
    "severity",
    "masked",
    "layer",
    "origins",
)


def _rows(graph: MetadataGraph, index: PIIIndex) -> list[dict[str, str]]:
    rows = []
    for indicator in index.indicators.values():
        node = graph.nodes[indicator.node_id]
        rows.append(
            {
                "node": indicator.node_id,
                "column": split_column_id(indicator.column)[1],
                "pii_types": ", ".join(indicator.pii_types),
                "basis": indicator.basis.value,
                "confidence": indicator.confidence.value,
                "severity": indicator.severity.value,
                "masked": "yes" if indicator.masked else "no",
                "layer": node.layer or "",
                "origins": ", ".join(indicator.origins),
            }
        )
    return rows


def pii_inventory_csv(graph: MetadataGraph, index: PIIIndex) -> str:
    out = io.StringIO()
    writer = csv.writer(out, quoting=csv.QUOTE_ALL, lineterminator="\n")
    writer.writerow(CSV_COLUMNS)
    for row in _rows(graph, index):
        writer.writerow([csv_cell(row[key]) for key in CSV_COLUMNS])
    return out.getvalue()


def pii_inventory_markdown(graph: MetadataGraph, index: PIIIndex) -> str:
    rows = _rows(graph, index)
    lines = [
        "# PII inventory",
        "",
        "These are **indicators**, found from dbt tags, column names and column lineage. "
        "They are not verified exposure.",
        "",
        f"{len(rows)} columns in {len({r['node'] for r in rows})} nodes.",
        "",
    ]
    by_type = Counter(t for r in rows for t in r["pii_types"].split(", ") if t)
    if by_type:
        lines += ["| PII type | Columns |", "|---|---|"]
        lines += [f"| {markdown_cell(t)} | {n} |" for t, n in sorted(by_type.items())]
        lines.append("")
    if index.unknown:
        lines += [
            f"{len(index.unknown)} models downstream of PII have unknown column lineage, so "
            "PII may reach them unseen:",
            "",
        ]
        lines += [
            f"- {markdown_cell(node_id)}: {markdown_cell(reason)}"
            for node_id, reason in index.unknown.items()
        ]
        lines.append("")
    if rows:
        header = ("Node", "Column", "PII types", "Basis", "Confidence", "Severity", "Masked")
        keys = ("node", "column", "pii_types", "basis", "confidence", "severity", "masked")
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|" + "---|" * len(header))
        for row in rows:
            lines.append("| " + " | ".join(markdown_cell(row[k]) for k in keys) + " |")
    return "\n".join(lines) + "\n"
