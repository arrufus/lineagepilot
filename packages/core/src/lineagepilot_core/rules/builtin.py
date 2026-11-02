"""The built-in rules (design section 12).

Carried from data-capsule's ``services/conformance.py`` (commit 5e57fcc), with the same
IDs, names, severities and remediations: MED_001-005, DBT_001-004 and PII_001-003.
Remapped to dbt equivalents, with new IDs and ``replaces`` naming the server rule:
DBT_005-010.

Changes in behaviour from the server:

- MED_001/002 report ``unknown`` when an upstream node has no layer or a dependency
  did not resolve. The server counted that as a pass (review finding F11).
- DBT_003 is case-sensitive. The server matched every pattern case-insensitively, so
  ``Customers`` passed a snake_case check.
- DBT_002 and the test-based rules use the tests in the manifest's test nodes, which the
  server never saw (spike S2).
- PII_002 uses the parser's PII name patterns, the same ones that set ``pii_type``.
- Findings that rest on a pattern guess (a layer inferred from a name or path, PII
  detected from a column name) carry ``heuristic`` confidence.
"""

from __future__ import annotations

import re

from lineagepilot_core.graph.model import (
    Column,
    Confidence,
    EdgeKind,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
)
from lineagepilot_core.rules.model import (
    NOT_APPLICABLE,
    PASS,
    Category,
    Outcome,
    Result,
    Rule,
    Scope,
    Severity,
)

GOLD = frozenset({"gold", "marts", "mart"})
SILVER = frozenset({"silver", "intermediate", "int"})
BRONZE = frozenset({"bronze", "raw", "staging"})

# Where PII enters (sources, seeds) and where it flows
PII_KINDS = frozenset({NodeKind.MODEL, NodeKind.SNAPSHOT, NodeKind.SEED, NodeKind.SOURCE})

_MASKING_INDICATORS = (
    "_hash", "_hashed", "_masked", "_encrypted", "_redacted",
    "_anonymized", "_tokenized", "_obfuscated",
    "hash_", "masked_", "encrypted_",
)  # fmt: skip


def _layer(node: Node) -> str:
    return (node.layer or "").lower()


def _layer_confidence(*nodes: Node) -> Confidence:
    if any(n.layer_source == "pattern" for n in nodes):
        return Confidence.HEURISTIC
    return Confidence.DECLARED


# Medallion


def _upstream_layers(allowed: frozenset[str]):
    def check(graph: MetadataGraph, node: Node, _: Column | None) -> Outcome:
        upstream = [
            graph.nodes[e.upstream] for e in graph.in_edges(node.id, (EdgeKind.DEPENDS_ON,))
        ]
        wrong = sorted(
            f"{u.name} ({_layer(u)})" for u in upstream if u.layer and _layer(u) not in allowed
        )
        confidence = _layer_confidence(node, *upstream)
        if wrong:
            return Outcome(
                Result.FAIL,
                f"{node.name} ({_layer(node)}) sources from {', '.join(wrong)}, which violates "
                "the layer hierarchy",
                confidence,
            )
        no_layer = sorted(u.name for u in upstream if not u.layer)
        unresolved = sorted(
            u.reference
            for u in graph.unresolved_for(node.id)
            if u.evidence.origin is Origin.MANIFEST
        )
        if no_layer or unresolved:
            return Outcome(
                Result.UNKNOWN,
                "the layer of these upstream nodes is not known: "
                + ", ".join(no_layer + unresolved),
                confidence,
            )
        return Outcome(Result.PASS, confidence=confidence)

    return check


def _when_layer(layers: frozenset[str], check):
    def guarded(graph: MetadataGraph, node: Node, column: Column | None) -> Outcome:
        if _layer(node) not in layers:
            return NOT_APPLICABLE
        return check(graph, node, column)

    return guarded


def _name_matches(pattern: str, flags: int = re.IGNORECASE):
    compiled = re.compile(pattern, flags)

    def check(graph: MetadataGraph, node: Node, _: Column | None) -> Outcome:
        if compiled.match(node.name):
            return Outcome(Result.PASS, confidence=_layer_confidence(node))
        return Outcome(
            Result.FAIL,
            f"name '{node.name}' does not match pattern '{pattern}'",
            _layer_confidence(node),
        )

    return check


# Documentation and tests


def _has_description(graph: MetadataGraph, node: Node, column: Column | None) -> Outcome:
    subject = column or node
    if (subject.description or "").strip():
        return PASS
    what = f"column '{column.name}' in '{node.name}'" if column else f"model '{node.name}'"
    return Outcome(Result.FAIL, f"{what} is missing a description")


def _has_tests(graph: MetadataGraph, node: Node, _: Column | None) -> Outcome:
    if node.has_tests:
        return PASS
    return Outcome(Result.FAIL, f"model '{node.name}' has no tests", _layer_confidence(node))


# PII


def _is_masked(column: Column) -> bool:
    name = column.name.lower()
    if any(indicator in name for indicator in _MASKING_INDICATORS):
        return True
    meta = column.meta or {}
    if meta.get("masked") or meta.get("hashed") or meta.get("encrypted"):
        return True
    return meta.get("transformation") in ("hash", "mask", "encrypt", "redact")


def _pii_confidence(node: Node, column: Column) -> Confidence:
    if column.pii_detected_by == "pattern" or node.layer_source == "pattern":
        return Confidence.HEURISTIC
    return Confidence.DECLARED


def _pii_masked_in_gold(graph: MetadataGraph, node: Node, column: Column | None) -> Outcome:
    assert column is not None
    if _layer(node) not in GOLD or not column.pii_type:
        return NOT_APPLICABLE
    if _is_masked(column):
        return Outcome(Result.PASS, confidence=_pii_confidence(node, column))
    return Outcome(
        Result.FAIL,
        f"PII column '{column.name}' ({column.pii_type}) in gold model '{node.name}' is not masked",
        _pii_confidence(node, column),
    )


def _pii_tagged(graph: MetadataGraph, node: Node, column: Column | None) -> Outcome:
    assert column is not None
    if column.pii_detected_by == "tag":
        return PASS
    if column.pii_detected_by == "pattern":
        return Outcome(
            Result.FAIL,
            f"column '{column.name}' looks like {column.pii_type} PII but is not tagged",
            Confidence.HEURISTIC,
        )
    return NOT_APPLICABLE


def _pii_typed(graph: MetadataGraph, node: Node, column: Column | None) -> Outcome:
    assert column is not None
    if not column.pii_type:
        return NOT_APPLICABLE
    if column.pii_type in ("unknown", "pii"):
        return Outcome(
            Result.FAIL,
            f"PII column '{column.name}' has no specific PII type",
            _pii_confidence(node, column),
        )
    return Outcome(Result.PASS, confidence=_pii_confidence(node, column))


# Remapped from the server's structural, quality and semantic rules


def _tested_with(column: Column, *kinds: str) -> bool:
    return all(kind in column.test_types for kind in kinds)


def _has_primary_key(graph: MetadataGraph, node: Node, _: Column | None) -> Outcome:
    if node.primary_key:
        return PASS
    if any(_tested_with(c, "unique", "not_null") for c in graph.columns_of(node.id)):
        return PASS
    return Outcome(
        Result.FAIL,
        f"model '{node.name}' has no primary key: no column has both unique and not_null "
        "tests, and its contract declares no primary-key constraint",
    )


def _primary_key_columns(graph: MetadataGraph, node: Node) -> tuple[list[Column], Confidence]:
    columns = graph.columns_of(node.id)
    if node.primary_key:
        declared = {c.lower() for c in node.primary_key}
        return [c for c in columns if c.name.lower() in declared], Confidence.DECLARED
    return [c for c in columns if c.semantic_type == "surrogate_key"], Confidence.HEURISTIC


def _primary_key_unique(graph: MetadataGraph, node: Node, _: Column | None) -> Outcome:
    keys, confidence = _primary_key_columns(graph, node)
    if not keys:
        return NOT_APPLICABLE
    if node.primary_key and len(keys) > 1:  # a declared composite key
        if "unique_combination_of_columns" in node.test_types:
            return Outcome(Result.PASS, confidence=confidence)
        return Outcome(
            Result.FAIL,
            f"the composite primary key of '{node.name}' "
            f"({', '.join(c.name for c in keys)}) has no unique_combination_of_columns test",
            confidence,
        )
    untested = sorted(c.name for c in keys if "unique" not in c.test_types)
    if not untested:
        return Outcome(Result.PASS, confidence=confidence)
    return Outcome(
        Result.FAIL,
        f"primary-key columns of '{node.name}' have no unique test: {', '.join(untested)}",
        confidence,
    )


def _foreign_keys_related(graph: MetadataGraph, node: Node, _: Column | None) -> Outcome:
    declared_keys = {c.lower() for c in node.primary_key}
    foreign = [
        c
        for c in graph.columns_of(node.id)
        if c.semantic_type == "foreign_key"
        and c.name.lower() not in declared_keys
        and "unique" not in c.test_types  # a unique column is this model's own key
    ]
    if not foreign:
        return NOT_APPLICABLE
    missing = sorted(c.name for c in foreign if "relationships" not in c.test_types)
    if missing:
        return Outcome(
            Result.FAIL,
            f"foreign-key columns of '{node.name}' have no relationships test: "
            + ", ".join(missing),
            Confidence.HEURISTIC,
        )
    return Outcome(Result.PASS, confidence=Confidence.HEURISTIC)


def _has_owner(graph: MetadataGraph, node: Node, _: Column | None) -> Outcome:
    if node.owner or node.group:
        return PASS
    return Outcome(Result.FAIL, f"model '{node.name}' has no meta.owner and no group")


def _has_domain(graph: MetadataGraph, node: Node, _: Column | None) -> Outcome:
    domain = (node.meta or {}).get("domain")
    if isinstance(domain, str) and domain.strip():
        return PASS
    return Outcome(Result.FAIL, f"model '{node.name}' has no meta.domain")


def builtin_rules() -> list[Rule]:
    model = frozenset({NodeKind.MODEL})
    return [
        Rule(
            "MED_001",
            "Gold sources Silver only",
            "Gold/Mart layer models should only source from Silver/Intermediate layers",
            Severity.ERROR,
            Category.LINEAGE,
            Scope.NODE,
            _when_layer(GOLD, _upstream_layers(SILVER)),
            model,
            "medallion",
            "Refactor to source from Silver/Intermediate layer instead of Bronze/Raw",
        ),
        Rule(
            "MED_002",
            "Silver sources Bronze only",
            "Silver/Intermediate models should only source from Bronze/Raw or other Silver",
            Severity.WARNING,
            Category.LINEAGE,
            Scope.NODE,
            _when_layer(
                frozenset({"silver", "intermediate", "int"}),
                _upstream_layers(
                    frozenset({"bronze", "raw", "silver", "intermediate", "int", "staging", "stg"})
                ),
            ),
            model,
            "medallion",
            "Ensure Silver models source from Bronze/Raw or other Silver models",
        ),
        Rule(
            "MED_003",
            "Bronze naming convention",
            "Bronze/Raw layer models should follow naming convention",
            Severity.WARNING,
            Category.NAMING,
            Scope.NODE,
            _when_layer(BRONZE, _name_matches(r"^(raw_|bronze_|stg_|src_)")),
            model,
            "medallion",
            "Rename model to start with raw_, bronze_, stg_, or src_",
        ),
        Rule(
            "MED_004",
            "Silver naming convention",
            "Silver/Intermediate models should follow naming convention",
            Severity.WARNING,
            Category.NAMING,
            Scope.NODE,
            _when_layer(
                frozenset({"silver", "intermediate"}), _name_matches(r"^(int_|silver_|stg_)")
            ),
            model,
            "medallion",
            "Rename model to start with int_, silver_, or stg_",
        ),
        Rule(
            "MED_005",
            "Gold naming convention",
            "Gold/Mart models should follow naming convention",
            Severity.WARNING,
            Category.NAMING,
            Scope.NODE,
            _when_layer(GOLD, _name_matches(r"^(dim_|fct_|fact_|mart_|rpt_|gold_)")),
            model,
            "medallion",
            "Rename model to start with dim_, fct_, fact_, mart_, rpt_, or gold_",
        ),
        Rule(
            "DBT_001",
            "Model has description",
            "All models should have a description",
            Severity.WARNING,
            Category.DOCUMENTATION,
            Scope.NODE,
            _has_description,
            model,
            "dbt_best_practices",
            "Add a description in the schema.yml file",
            prefer_yaml=True,
        ),
        Rule(
            "DBT_002",
            "Model has tests",
            "All models should have at least one test",
            Severity.WARNING,
            Category.TESTING,
            Scope.NODE,
            _has_tests,
            model,
            "dbt_best_practices",
            "Add at least one test (unique, not_null, etc.) in schema.yml",
            prefer_yaml=True,
        ),
        Rule(
            "DBT_003",
            "Model naming snake_case",
            "Model names should be in snake_case",
            Severity.INFO,
            Category.NAMING,
            Scope.NODE,
            _name_matches(r"^[a-z][a-z0-9_]*$", flags=0),
            model,
            "dbt_best_practices",
            "Rename model to use snake_case (lowercase with underscores)",
        ),
        Rule(
            "DBT_004",
            "Column has description",
            "Columns should have descriptions",
            Severity.INFO,
            Category.DOCUMENTATION,
            Scope.COLUMN,
            _has_description,
            model,
            "dbt_best_practices",
            "Add a description for this column in schema.yml",
            prefer_yaml=True,
        ),
        Rule(
            "PII_001",
            "PII in Gold must be masked",
            "PII columns in Gold/Mart layer should be masked, hashed, or encrypted",
            Severity.CRITICAL,
            Category.PII,
            Scope.COLUMN,
            _pii_masked_in_gold,
            PII_KINDS,
            "pii_compliance",
            "Apply masking, hashing, or encryption to this PII column",
            prefer_yaml=True,
        ),
        Rule(
            "PII_002",
            "PII columns must be tagged",
            "Columns containing PII should have explicit PII tags",
            Severity.ERROR,
            Category.PII,
            Scope.COLUMN,
            _pii_tagged,
            PII_KINDS,
            "pii_compliance",
            "Add PII tag via meta: {pii: true, pii_type: ...} in schema.yml",
            prefer_yaml=True,
        ),
        Rule(
            "PII_003",
            "PII type should be specified",
            "PII columns should have specific PII type classification",
            Severity.WARNING,
            Category.PII,
            Scope.COLUMN,
            _pii_typed,
            PII_KINDS,
            "pii_compliance",
            "Specify PII type (email, phone, ssn, etc.) in meta",
            prefer_yaml=True,
        ),
        Rule(
            "DBT_005",
            "Model has a primary key",
            "Every model should have a primary key: a column with unique and not_null tests, "
            "or a contract primary-key constraint",
            Severity.ERROR,
            Category.STRUCTURAL,
            Scope.NODE,
            _has_primary_key,
            model,
            "dbt_best_practices",
            "Add unique and not_null tests to the key column, or a primary_key constraint",
            replaces="DIM2_006",
            prefer_yaml=True,
        ),
        Rule(
            "DBT_006",
            "Primary key is tested for uniqueness",
            "Primary-key columns should have a unique test",
            Severity.ERROR,
            Category.QUALITY,
            Scope.NODE,
            _primary_key_unique,
            model,
            "dbt_best_practices",
            "Add a unique test to the primary-key column (unique_combination_of_columns for "
            "a composite key)",
            replaces="DIM4_002",
            prefer_yaml=True,
        ),
        Rule(
            "DBT_007",
            "Foreign keys have relationships tests",
            "Foreign-key columns should have a relationships test",
            Severity.WARNING,
            Category.QUALITY,
            Scope.NODE,
            _foreign_keys_related,
            model,
            "dbt_best_practices",
            "Add a relationships test for each foreign-key column",
            replaces="DIM4_004",
            prefer_yaml=True,
        ),
        Rule(
            "DBT_008",
            "Gold models have tests",
            "Gold/Mart models should have dbt tests",
            Severity.WARNING,
            Category.QUALITY,
            Scope.NODE,
            _when_layer(GOLD, _has_tests),
            model,
            "dbt_best_practices",
            "Add tests to validate the data in this gold model",
            replaces="DIM4_001",
            prefer_yaml=True,
        ),
        Rule(
            "DBT_009",
            "Model has an owner",
            "Every model should have an owner: meta.owner or a group",
            Severity.WARNING,
            Category.SEMANTIC,
            Scope.NODE,
            _has_owner,
            model,
            "dbt_best_practices",
            "Set meta.owner, or put the model in a group",
            replaces="DIM3_005",
            prefer_yaml=True,
        ),
        Rule(
            "DBT_010",
            "Model belongs to a domain",
            "Every model should declare its business domain in meta.domain",
            Severity.INFO,
            Category.SEMANTIC,
            Scope.NODE,
            _has_domain,
            model,
            "dbt_best_practices",
            "Set meta.domain",
            replaces="DIM3_006",
            prefer_yaml=True,
        ),
    ]
