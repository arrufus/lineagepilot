"""Consumers declared in the annotation file (task 7.4, decision D-14).

``.lineagepilot/lineage.yml``'s ``consumers`` say what reads dbt's models when no file in the
repository says so: a Tableau workbook, a notebook, a spreadsheet. Each becomes a consumer node with
its tool, from origin ``annotation`` with confidence ``declared``:

- it ``consumes`` each node it names: a dbt ID, a table's name, or a model's name if only one model
  has it (``References``);
- for a column (``model.p.orders#amount``) it consumes that column too, so a renamed column reaches
  it and a change to another column of the same model does not, as for the semantic layer.

A name that matches no node, or a column its node does not list, is an unknown, at the file. A
consumer whose every name is a column it found reads by column (``graph.consumer_lineage``); any
other is impacted whenever a node it names is.

The annotation file is read with the DAG files, in the scan's child process, and the Airflow linker
links its consumers with its tasks; the CLI's ``--no-airflow`` reads the file alone
(``read_declared``). Like everything a reader adds, they are never part of a snapshot.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from lineagepilot_core.graph.ids import column_id
from lineagepilot_core.graph.model import (
    ArtifactVersion,
    ColumnLineageStatus,
    Confidence,
    EdgeKind,
    Evidence,
    LineageState,
    Location,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    Unresolved,
)
from lineagepilot_core.parsers.data_flow_annotations import DataFlowAnnotationLoader
from lineagepilot_core.readers.common.references import References
from lineagepilot_core.readers.common.safe_io import to_posix
from lineagepilot_core.readers.common.workspace import load_annotations, project_and_workspace

# Marks a node as one the annotation file declares, so it can be taken out again
DECLARED = "declared"


def read_declared(project_root: str | Path, workspace: str | Path | None = None) -> dict[str, Any]:
    """The project's declared consumers, without the DAG files: what ``--no-airflow`` reads."""
    root, space = project_and_workspace(project_root, workspace)
    loader = DataFlowAnnotationLoader()
    file = load_annotations(loader, root, space)
    relative = to_posix(os.path.relpath(os.path.join(space, file), root)) if file else None
    return {
        "file": relative,
        "consumers": [c.to_json() for c in loader.get_consumers().values()],
        "problems": loader.problems,
    }


def is_declared(node: Node) -> bool:
    return node.kind is NodeKind.CONSUMER and bool(node.properties.get(DECLARED))


def unlink_declared(graph: MetadataGraph) -> None:
    """Take out every declared consumer, with its edges and its unknowns."""
    gone = {n.id for n in graph.nodes.values() if is_declared(n)}
    for node_id in gone:
        graph.remove_node(node_id)
        graph.consumer_lineage.pop(node_id, None)
    graph.remove_unresolved(gone, Origin.ANNOTATION)


def link_declared(
    graph: MetadataGraph,
    consumers: list[dict[str, Any]],
    file: str | None,
    references: References | None = None,
) -> int:
    """Add the declared consumers, in place of any added before; ``file`` is the annotation
    file's path, relative to the project. Returns how many were added."""
    unlink_declared(graph)
    references = references or References(graph)
    added = 0
    for entry in consumers:
        if entry["id"] in graph.nodes:
            continue  # the loader keeps one of each ID; nothing else has a tool's prefix
        graph.add_node(
            Node(
                id=entry["id"],
                kind=NodeKind.CONSUMER,
                name=entry["name"],
                owner=entry.get("owner"),
                tool=entry["tool"],
                file_path=file,
                properties={DECLARED: True},
            )
        )
        added += 1
        if _link(graph, entry, file, references):
            graph.consumer_lineage[entry["id"]] = ColumnLineageStatus(LineageState.RESOLVED)
    return added


def _link(
    graph: MetadataGraph, entry: dict[str, Any], file: str | None, references: References
) -> bool:
    """One consumer's edges and unknowns; whether it reads only columns, every one found."""
    consumer = entry["id"]
    by_column = True

    def evidence(confidence: Confidence) -> Evidence:
        return Evidence(
            Origin.ANNOTATION,
            confidence,
            ArtifactVersion(file) if file else None,
            Location(file) if file else None,
        )

    def unknown(reference: str, reason: str) -> None:
        graph.add_unresolved(Unresolved(consumer, reference, reason, evidence(Confidence.DECLARED)))

    for consumed in entry["consumes"]:
        name, column = consumed["node"], consumed.get("column")
        found, partial = references.resolve(name)
        shown = f"{name}#{column}" if column else name
        if not found:
            unknown(
                shown,
                f"{file or 'the annotation file'} says it reads this, and no dbt node matches",
            )
            by_column = False
            continue
        # A table's name that only part of a relation's name matches is a guess
        confidence = Confidence.HEURISTIC if partial else Confidence.DECLARED
        for target in found:
            graph.add_edge(EdgeKind.CONSUMES, consumer, target, evidence(confidence))
            if column is None:
                by_column = False
                continue
            read = column_id(target, column, graph.adapter_type)
            if read in graph.columns:
                graph.add_edge(EdgeKind.CONSUMES, consumer, read, evidence(confidence))
            else:
                by_column = False
                unknown(
                    shown,
                    f"{target} has no column {column!r} in the catalog or the docs; it is read "
                    "as a whole",
                )
    return by_column
