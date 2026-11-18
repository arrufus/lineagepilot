"""The dbt semantic layer in the manifest: semantic models, metrics and saved queries (task 7.8).

The manifest already holds them (``semantic_models``, ``metrics``, ``saved_queries``), so this
reads them with the rest of the manifest. Each becomes a ``RawNode`` for the node itself, and a
record of what it reads, which ``graph/semantic.py`` links once the models' columns are in the
graph:

- a semantic model reads its model, and each of its entities, dimensions and measures reads the
  model's columns its ``expr`` names, or the column of its own name;
- a metric reads its measures (the older YAML), or aggregates an expression over a semantic
  model's model directly (the newer YAML, dbt 1.12); its filters read dimensions;
- a saved query reads its metrics, and the dimensions it groups and filters by. Its exports write
  tables, which are kept with the node.

Both YAML forms write the same manifest objects. The metrics a metric or a saved query is built
from are its ``depends_on``, read as for any other node.

Everything here is untrusted input (design section 10): a wrong type reads as absent, and the
number of references read from one node is capped.
"""

from __future__ import annotations

from typing import Any

from lineagepilot_core.parsers.base import RawField, RawMetric, RawSavedQuery, RawSemanticModel

# Resource type -> the manifest's key for it
SEMANTIC_KEYS = {
    "semantic_model": "semantic_models",
    "metric": "metrics",
    "saved_query": "saved_queries",
}
SEMANTIC_TOOL = "dbt_semantic"
# Nodes a semantic model can be defined on
_MODEL_PREFIXES = ("model.", "snapshot.", "seed.")
# Field lists of a semantic model, and the role of each
_FIELD_KEYS = (("entities", "entity"), ("dimensions", "dimension"), ("measures", "measure"))
_TYPE_KEYS = {"entity": "type", "dimension": "type", "measure": "agg"}
# Filters are found by walking a metric's type parameters; a crafted manifest cannot make the
# walk deep or long
MAX_REFERENCES = 200
_MAX_DEPTH = 8


def semantic_model(node_id: str, entry: dict[str, Any], depends_on: list[str]) -> RawSemanticModel:
    model = next((d for d in depends_on if d.startswith(_MODEL_PREFIXES)), None)
    fields = []
    for key, role in _FIELD_KEYS:
        items = entry.get(key)
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, dict) or not _text(item.get("name")):
                continue  # a field with no name cannot be referred to
            fields.append(
                RawField(
                    name=item["name"],
                    role=role,
                    expr=_expr(item.get("expr")),
                    type=_text(item.get(_TYPE_KEYS[role])),
                    description=_text(item.get("description")),
                )
            )
    return RawSemanticModel(node_id=node_id, model_id=model, fields=fields)


def metric(node_id: str, entry: dict[str, Any]) -> RawMetric:
    params = _dict(entry.get("type_params"))
    conversion = _dict(params.get("conversion_type_params"))
    measures = [
        name
        for name in (
            _input_name(params.get("measure")),
            _input_name(conversion.get("base_measure")),
            _input_name(conversion.get("conversion_measure")),
        )
        if name
    ]
    references = _templates(entry.get("filter"))
    references += _templates({k: v for k, v in params.items() if k != "input_measures"})
    entity = _text(conversion.get("entity"))
    if entity:
        references.append(f"Entity('{entity}')")
    aggregation = params.get("metric_aggregation_params")
    expr = semantic = None
    if isinstance(aggregation, dict):
        # The newer YAML: a simple metric aggregates over its semantic model's model, and with
        # no expr it reads the column of its own name, as a measure does
        semantic = _text(aggregation.get("semantic_model"))
        expr = _expr(params.get("expr")) or _text(entry.get("name"))
    return RawMetric(
        node_id=node_id,
        metric_type=_text(entry.get("type")),
        measures=list(dict.fromkeys(measures)),
        references=_capped(references),
        expr=expr,
        semantic_model=semantic,
    )


def saved_query(node_id: str, entry: dict[str, Any]) -> RawSavedQuery:
    params = _dict(entry.get("query_params"))
    group_by = params.get("group_by")
    references = [g for g in group_by if _text(g)] if isinstance(group_by, list) else []
    references += _templates(params.get("where"))
    exports = []
    items = entry.get("exports")
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict) or not _text(item.get("name")):
            continue
        config = _dict(item.get("config"))
        schema, alias = _text(config.get("schema_name")), _text(config.get("alias"))
        exports.append(
            {
                "name": item["name"],
                "as": _text(config.get("export_as")),
                "relation": f"{schema}.{alias}" if schema and alias else alias,
            }
        )
    return RawSavedQuery(node_id=node_id, references=_capped(references), exports=exports)


def _templates(value: Any, depth: int = 0) -> list[str]:
    """Every ``where_sql_template`` in a filter or in type parameters, in order."""
    if depth > _MAX_DEPTH:
        return []
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "where_sql_template" and _text(item):
                found.append(item)
            else:
                found += _templates(item, depth + 1)
    elif isinstance(value, list):
        for item in value[:MAX_REFERENCES]:
            found += _templates(item, depth + 1)
    return found[:MAX_REFERENCES]


def _capped(references: list[str]) -> list[str]:
    return list(dict.fromkeys(references))[:MAX_REFERENCES]


def _input_name(value: Any) -> str | None:
    """A metric input's name: ``{"name": "order_total", "filter": ...}``, or just the name."""
    if isinstance(value, dict):
        return _text(value.get("name"))
    return _text(value)


def _expr(value: Any) -> str | None:
    """An ``expr`` as text: dbt writes ``1`` as a number when the YAML did."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return str(value)
    return value if _text(value) else None


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
