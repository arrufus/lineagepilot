"""What a reader's file names, as dbt nodes: a dbt ID, a table's name, or a model's name.

Shared by the Airflow linker (its SQL tasks and its annotated tasks) and the declared consumers of
the annotation file (task 7.4), so each name is read the same way wherever it is written.
"""

from __future__ import annotations

from lineagepilot_core.graph.ids import normalise_identifier
from lineagepilot_core.graph.model import DBT_KINDS, MetadataGraph, NodeKind


class References:
    """Names to dbt nodes, in one graph. The index of tables by partial name is built once."""

    def __init__(self, graph: MetadataGraph) -> None:
        self.graph = graph
        self._suffixes: dict[str, set[str]] | None = None

    def resolve(self, reference: str) -> tuple[list[str], bool]:
        """What an annotation names: a dbt ID, a table name, or a model's name if it is unique."""
        node = self.graph.nodes.get(reference)
        if node is not None and node.kind in DBT_KINDS:
            return [reference], False
        if "." in reference:
            return self.table(reference.split("."))
        named = [
            n.id
            for n in self.graph.nodes.values()
            if n.kind in DBT_KINDS and n.name.lower() == reference.lower()
        ]
        if len(named) == 1:
            return named, False
        return ([], False) if named else self.table([reference])

    def table(self, parts: list[str | None]) -> tuple[list[str], bool]:
        """The dbt nodes for ``[database, schema, table]``, and whether that was by a partial name.

        A partial name counts only if exactly one relation in the graph ends with it.
        """
        adapter = self.graph.adapter_type
        names = [normalise_identifier(p, adapter) for p in parts if p]
        if not names:
            return [], False
        key = ".".join(names)
        full = f"relation:{key}"
        if full in self.graph.nodes:
            return self._builders(full), False
        matches = self._by_suffix().get(key, set())
        if len(matches) != 1:
            return [], False
        return self._builders(next(iter(matches))), True

    def _builders(self, relation: str) -> list[str]:
        """Who the table belongs to: what builds it, or else the sources that read it."""
        nodes = self.graph.logical_nodes_for(relation)
        built = [n.id for n in nodes if n.kind is not NodeKind.SOURCE]
        return sorted(built or [n.id for n in nodes])

    def _by_suffix(self) -> dict[str, set[str]]:
        if self._suffixes is None:
            self._suffixes = {}
            for node in self.graph.nodes.values():
                if node.kind is NodeKind.RELATION:
                    parts = node.id.removeprefix("relation:").split(".")
                    for start in range(1, len(parts)):
                        key = ".".join(parts[start:])
                        self._suffixes.setdefault(key, set()).add(node.id)
        return self._suffixes
