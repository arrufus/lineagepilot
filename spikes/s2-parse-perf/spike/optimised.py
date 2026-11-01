"""LABELLED VARIANT, NOT THE PARSER UNDER TEST.

``OptimisedDbtParser`` subclasses the as-is ``DbtParser`` and overrides one
method, ``_build_lineage``, to show how much headroom the obvious fix gives.

The as-is method (dbt_parser.py lines 711-720) builds the column lookup by
scanning every capsule for every column: O(columns x capsules). The override
builds a ``capsule_urn -> capsule`` dict once. ``setdefault`` keeps the first
capsule per URN, which matches the as-is ``break`` on first match, so the
output is identical even when URNs collide (tests/test_optimised.py checks).
Everything else, including the edge loop, is the same logic as the original.
"""

from __future__ import annotations

from typing import Any


def make_optimised_parser(dbt_parser_module: Any) -> type:
    DbtParser = dbt_parser_module.DbtParser
    RawEdge = dbt_parser_module.RawEdge

    class OptimisedDbtParser(DbtParser):  # type: ignore[misc, valid-type]
        variant = "optimised: dict lookup in _build_lineage"

        def _build_lineage(self, manifest: dict[str, Any], result: Any) -> None:
            urn_lookup: dict[str, str] = {c.unique_id: c.urn for c in result.capsules}
            capsule_by_urn: dict[str, Any] = {}
            for capsule in result.capsules:
                capsule_by_urn.setdefault(capsule.urn, capsule)
            column_urn_lookup: dict[str, str] = {}
            for column in result.columns:
                capsule = capsule_by_urn.get(column.capsule_urn)
                if capsule is not None:
                    column_urn_lookup[f"{capsule.unique_id}.{column.name.lower()}"] = column.urn
            for capsule in result.capsules:
                for dep_id in capsule.depends_on:
                    source_urn = urn_lookup.get(dep_id)
                    if source_urn:
                        if dep_id.startswith("source."):
                            transformation = "source"
                        elif dep_id.startswith("model.") or dep_id.startswith("seed."):
                            transformation = "ref"
                        else:
                            transformation = "dependency"
                        result.edges.append(RawEdge(source_urn=source_urn, target_urn=capsule.urn,
                                                    edge_type="flows_to", transformation=transformation))
            self._build_column_lineage(manifest, result, urn_lookup, column_urn_lookup)

    return OptimisedDbtParser
