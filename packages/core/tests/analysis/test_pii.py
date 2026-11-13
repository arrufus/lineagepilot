"""PII indicators along every lineage path (review finding F12)."""

from __future__ import annotations

import pytest
from support import project_config

from lineagepilot_core.analysis.pii import (
    PIIBasis,
    PIISeverity,
    is_masked,
    pii_exposure,
    pii_index,
    severity_of,
    trace_pii,
)
from lineagepilot_core.graph import (
    Column,
    Confidence,
    EdgeKind,
    Evidence,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    build_dbt_graph,
)
from lineagepilot_core.graph.column_lineage import add_column_lineage, compiled_sql
from lineagepilot_core.graph.model import ColumnLineageStatus, LineageState
from lineagepilot_core.parsers import DbtParser

DECLARED = Evidence(Origin.MANIFEST, Confidence.DECLARED)
INFERRED = Evidence(Origin.COMPILED_SQL, Confidence.INFERRED)
RESOLVED = ColumnLineageStatus(LineageState.RESOLVED)


@pytest.fixture
def graph() -> MetadataGraph:
    """A diamond: a#email (tagged) and a#phone (pattern) both reach d#contact.

    a -> b -> d and a -> c -> d, with b#email_hash masking one branch.
    """
    g = MetadataGraph()
    for node_id, layer in (("a", "silver"), ("b", "silver"), ("c", "silver"), ("d", "gold")):
        g.add_node(Node(node_id, NodeKind.MODEL, node_id, layer=layer))
    g.add_node(Node("e", NodeKind.EXPOSURE, "dashboard"))
    g.add_node(Node("airflow:dag.t", NodeKind.AIRFLOW_TASK, "t"))
    for up, down in (("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("d", "e")):
        g.add_edge(EdgeKind.DEPENDS_ON, up, down, DECLARED)
    g.add_edge(EdgeKind.ORCHESTRATES, "airflow:dag.t", "d", DECLARED)
    for cid, fields in {
        "a#email": {"pii_type": "email", "pii_detected_by": "tag"},
        "a#phone": {"pii_type": "phone", "pii_detected_by": "pattern"},
        "a#ok": {},
        "b#x": {},
        "b#email_hash": {},
        "c#y": {},
        "d#contact": {},
        "d#hashed": {},
    }.items():
        node_id, name = cid.split("#")
        g.add_column(Column(cid, node_id, name, **fields))
    for up, down in (
        ("a#email", "b#x"),
        ("a#email", "b#email_hash"),
        ("a#phone", "c#y"),
        ("b#x", "d#contact"),
        ("c#y", "d#contact"),
        ("b#email_hash", "d#hashed"),
    ):
        g.add_edge(EdgeKind.DERIVES, up, down, INFERRED)
    for node_id in ("b", "c", "d"):
        g.column_lineage[node_id] = RESOLVED
    return g


def test_direct_indicators(graph):
    index = pii_index(graph)
    email, phone = index.indicators["a#email"], index.indicators["a#phone"]
    assert (email.basis, email.confidence, email.origins) == (PIIBasis.TAG, Confidence.DECLARED, ())
    assert (phone.basis, phone.confidence) == (PIIBasis.PATTERN, Confidence.HEURISTIC)
    assert "a#ok" not in index.indicators


def test_pii_is_inherited_along_every_path(graph):
    """The server followed only the first upstream edge (F12)."""
    contact = pii_index(graph).indicators["d#contact"]
    assert contact.basis is PIIBasis.LINEAGE
    assert contact.origins == ("a#email", "a#phone")
    assert contact.pii_types == ("email", "phone")
    assert contact.confidence is Confidence.INFERRED  # one origin is declared
    assert contact.severity is PIISeverity.HIGH


def test_pattern_only_origins_are_heuristic(graph):
    assert pii_index(graph).indicators["c#y"].confidence is Confidence.HEURISTIC


def test_masking_stops_the_flow(graph):
    index = pii_index(graph)
    assert "b#email_hash" not in index.indicators
    assert "d#hashed" not in index.indicators


def test_a_masked_direct_column_is_an_indicator_but_not_a_pii_node(graph):
    graph.columns["d#hashed"].pii_type = "email"
    graph.columns["d#hashed"].pii_detected_by = "tag"
    graph.columns["d#hashed"].meta = {"masked": True}
    index = pii_index(graph)
    hashed = index.indicators["d#hashed"]
    assert hashed.masked and hashed.basis is PIIBasis.TAG
    assert index.nodes() == {"a", "b", "c", "d"}
    assert [i.column for i in index.for_node("d")] == ["d#contact", "d#hashed"]


def test_a_direct_column_also_names_its_upstream_origins(graph):
    graph.columns["d#contact"].pii_type = "email"
    graph.columns["d#contact"].pii_detected_by = "pattern"
    contact = pii_index(graph).indicators["d#contact"]
    assert contact.basis is PIIBasis.PATTERN
    assert contact.origins == ("a#email", "a#phone")
    assert contact.pii_types == ("email", "phone")


def test_unknown_lineage_downstream_is_never_clean(graph):
    del graph.column_lineage["c"]
    graph.column_lineage["d"] = ColumnLineageStatus(LineageState.UNKNOWN, "no catalog.json")
    assert pii_index(graph).unknown == {
        "c": "column lineage not computed",
        "d": "column lineage unknown: no catalog.json",
    }


def test_masking_indicators():
    assert is_masked(Column("x#a", "x", "email_hashed"))
    assert is_masked(Column("x#a", "x", "Masked_Email"))
    assert is_masked(Column("x#a", "x", "email", meta={"encrypted": True}))
    assert is_masked(Column("x#a", "x", "email", meta={"transformation": "redact"}))
    assert not is_masked(Column("x#a", "x", "email"))


def test_severity():
    assert severity_of(["location", "ssn"]) is PIISeverity.CRITICAL
    assert severity_of(["Email"]) is PIISeverity.HIGH
    assert severity_of(["shoe_size"]) is PIISeverity.MEDIUM
    assert severity_of([]) is PIISeverity.MEDIUM


def test_exposure_report(graph):
    report = pii_exposure(graph)
    (exposure,) = report.exposures
    assert exposure.indicator.column == "d#contact"
    assert exposure.layer == "gold"
    assert exposure.reason == (
        "unmasked email, phone PII indicator in the gold layer (derived from a#email, a#phone)"
    )
    assert exposure.recommendation.startswith("Mask, hash or encrypt contact")
    assert report.severity_breakdown() == {"critical": 0, "high": 1, "medium": 0, "low": 0}
    assert report.consumers == {"d": ["airflow:dag.t", "e"]}


def test_exposure_in_chosen_layers_with_each_basis(graph):
    report = pii_exposure(graph, layers=["Silver"])
    reasons = {e.indicator.column: e.reason for e in report.exposures}
    assert reasons["a#email"].endswith("(declared)")
    assert reasons["a#phone"].endswith("(suggested by the column name)")
    assert set(reasons) == {"a#email", "a#phone", "b#x", "c#y"}


def test_trace_follows_every_path(graph):
    trace = trace_pii(graph, "d#contact")
    assert trace.origins == ["a#email", "a#phone"]
    assert set(trace.upstream.found()) == {"b#x", "c#y", "a#email", "a#phone"}
    assert trace.downstream == [] and trace.terminals == []

    trace = trace_pii(graph, "a#email")
    assert trace.downstream == ["b#x", "d#contact"]
    assert trace.masked_at == ["b#email_hash"]
    assert trace.terminals == ["d#contact"]
    assert trace.unknown == {}


def test_trace_reports_unknown_downstream(graph):
    del graph.column_lineage["d"]
    assert trace_pii(graph, "a#email").unknown == {"d": "column lineage not computed"}
    with pytest.raises(KeyError, match="unknown column"):
        trace_pii(graph, "nope#x")


def test_pilot_seed_pii_reaches_the_mart_through_the_source(write_json, manifest_of):
    """A tag on the seed's column flows across the physical table to the source."""
    manifest = manifest_of("pilot-1.12")
    seed = manifest["nodes"]["seed.pilot_shop.raw_customers"]
    seed["columns"]["name"] = {"name": "name", "meta": {"pii": "name"}, "tags": []}
    config = project_config("pilot-1.12")
    config["manifest_path"] = str(write_json(manifest))
    result = DbtParser().parse(config)
    graph = build_dbt_graph(result)
    add_column_lineage(graph, compiled_sql(result))
    index = pii_index(graph)
    origin = "seed.pilot_shop.raw_customers#name"
    source = index.indicators["source.pilot_shop.ecom.raw_customers#name"]
    assert (source.basis, source.origins) == (PIIBasis.LINEAGE, (origin,))
    mart = index.indicators["model.pilot_shop.customers#customer_name"]
    assert origin in mart.origins and mart.basis is PIIBasis.PATTERN
    assert trace_pii(graph, "model.pilot_shop.customers#customer_name").origins == [
        "model.pilot_shop.stg_customers#customer_name",
        origin,
    ]
