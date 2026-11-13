"""Each built-in rule, on small graphs, then all of them on pilot-shop."""

from __future__ import annotations

import pytest
from support import project_config

from lineagepilot_core.graph import (
    Column,
    Confidence,
    EdgeKind,
    Evidence,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    Unresolved,
    build_dbt_graph,
)
from lineagepilot_core.parsers import DbtParser
from lineagepilot_core.rules import Result, builtin_rules, evaluate

RULES = {rule.id: rule for rule in builtin_rules()}
DECLARED = Evidence(Origin.MANIFEST, Confidence.DECLARED)


def model(graph: MetadataGraph, node_id: str, **fields) -> Node:
    fields.setdefault("kind", NodeKind.MODEL)
    return graph.add_node(Node(node_id, name=node_id.split(".")[-1], **fields))


def column(graph: MetadataGraph, node_id: str, name: str, **fields) -> Column:
    return graph.add_column(Column(f"{node_id}#{name}", node_id, name, **fields))


def check(rule_id: str, graph: MetadataGraph, node_id: str, column_name: str | None = None):
    node = graph.node(node_id)
    col = graph.columns[f"{node_id}#{column_name}"] if column_name else None
    return RULES[rule_id].check(graph, node, col)


def test_eighteen_rules_with_the_design_ids():
    assert sorted(RULES) == sorted(
        [f"MED_00{i}" for i in range(1, 6)]
        + [f"DBT_00{i}" for i in range(1, 10)]
        + ["DBT_010"]
        + [f"PII_00{i}" for i in range(1, 4)]
    )
    assert {r.replaces for r in RULES.values() if r.replaces} == {
        "DIM2_006",
        "DIM4_002",
        "DIM4_004",
        "DIM4_001",
        "DIM3_005",
        "DIM3_006",
    }


class TestMedallion:
    @pytest.fixture
    def graph(self):
        g = MetadataGraph()
        model(g, "m.fct", layer="gold", layer_source="meta")
        model(g, "m.int", layer="silver", layer_source="meta")
        model(g, "m.raw", layer="bronze", layer_source="pattern")
        model(g, "m.free")
        return g

    def test_gold_from_silver_passes(self, graph):
        graph.add_edge(EdgeKind.DEPENDS_ON, "m.int", "m.fct", DECLARED)
        outcome = check("MED_001", graph, "m.fct")
        assert (outcome.result, outcome.confidence) == (Result.PASS, Confidence.DECLARED)

    def test_gold_from_bronze_fails_heuristically_when_a_layer_was_guessed(self, graph):
        graph.add_edge(EdgeKind.DEPENDS_ON, "m.raw", "m.fct", DECLARED)
        outcome = check("MED_001", graph, "m.fct")
        assert outcome.result is Result.FAIL
        assert outcome.message == (
            "fct (gold) sources from raw (bronze), which violates the layer hierarchy"
        )
        assert outcome.confidence is Confidence.HEURISTIC

    def test_an_upstream_without_a_layer_is_unknown_not_a_pass(self, graph):
        """Review finding F11: the server counted this as a pass."""
        graph.add_edge(EdgeKind.DEPENDS_ON, "m.free", "m.fct", DECLARED)
        outcome = check("MED_001", graph, "m.fct")
        assert outcome.result is Result.UNKNOWN
        assert "free" in outcome.message

    def test_an_unresolved_dependency_is_unknown(self, graph):
        graph.add_unresolved(Unresolved("m.fct", "model.p.ghost", "not found", DECLARED))
        outcome = check("MED_001", graph, "m.fct")
        assert outcome.result is Result.UNKNOWN and "model.p.ghost" in outcome.message

    def test_a_failure_beats_an_unknown(self, graph):
        graph.add_edge(EdgeKind.DEPENDS_ON, "m.raw", "m.fct", DECLARED)
        graph.add_edge(EdgeKind.DEPENDS_ON, "m.free", "m.fct", DECLARED)
        assert check("MED_001", graph, "m.fct").result is Result.FAIL

    def test_silver_may_source_bronze_and_silver(self, graph):
        graph.add_edge(EdgeKind.DEPENDS_ON, "m.raw", "m.int", DECLARED)
        assert check("MED_002", graph, "m.int").result is Result.PASS
        graph.add_edge(EdgeKind.DEPENDS_ON, "m.fct", "m.int", DECLARED)
        assert check("MED_002", graph, "m.int").result is Result.FAIL

    def test_layer_rules_do_not_apply_to_other_layers(self, graph):
        assert check("MED_001", graph, "m.int").result is Result.NOT_APPLICABLE
        assert check("MED_002", graph, "m.fct").result is Result.NOT_APPLICABLE
        assert check("MED_005", graph, "m.free").result is Result.NOT_APPLICABLE

    @pytest.mark.parametrize(
        ("rule_id", "layer", "good", "bad"),
        [
            ("MED_003", "bronze", "raw_orders", "orders"),
            ("MED_004", "silver", "int_orders", "orders"),
            ("MED_005", "gold", "FCT_orders", "orders"),
        ],
    )
    def test_naming_conventions(self, rule_id, layer, good, bad):
        g = MetadataGraph()
        g.add_node(Node("a", NodeKind.MODEL, good, layer=layer))
        g.add_node(Node("b", NodeKind.MODEL, bad, layer=layer))
        assert RULES[rule_id].check(g, g.node("a"), None).result is Result.PASS
        failed = RULES[rule_id].check(g, g.node("b"), None)
        assert failed.result is Result.FAIL and f"'{bad}' does not match" in failed.message


class TestDbtBestPractices:
    def test_description(self):
        g = MetadataGraph()
        model(g, "m.a", description="Orders, one row each")
        model(g, "m.b", description="   ")
        assert check("DBT_001", g, "m.a").result is Result.PASS
        assert check("DBT_001", g, "m.b").message == "model 'b' is missing a description"

    def test_tests(self):
        g = MetadataGraph()
        model(g, "m.a", test_count=1)
        model(g, "m.b")
        assert check("DBT_002", g, "m.a").result is Result.PASS
        assert check("DBT_002", g, "m.b").result is Result.FAIL

    def test_snake_case_is_case_sensitive(self):
        """The server matched case-insensitively, so this passed there."""
        g = MetadataGraph()
        g.add_node(Node("a", NodeKind.MODEL, "Customers"))
        g.add_node(Node("b", NodeKind.MODEL, "customers_v2"))
        assert RULES["DBT_003"].check(g, g.node("a"), None).result is Result.FAIL
        assert RULES["DBT_003"].check(g, g.node("b"), None).result is Result.PASS

    def test_column_description(self):
        g = MetadataGraph()
        model(g, "m.a")
        column(g, "m.a", "id", description="Key")
        column(g, "m.a", "x")
        assert check("DBT_004", g, "m.a", "id").result is Result.PASS
        assert check("DBT_004", g, "m.a", "x").message == (
            "column 'x' in 'a' is missing a description"
        )


class TestPII:
    @pytest.fixture
    def graph(self):
        g = MetadataGraph()
        model(g, "m.gold", layer="gold", layer_source="meta")
        model(g, "m.silver", layer="silver")
        column(g, "m.gold", "email", pii_type="email", pii_detected_by="pattern")
        column(g, "m.gold", "email_hash", pii_type="email", pii_detected_by="tag")
        column(g, "m.gold", "phone", pii_type="phone", pii_detected_by="tag", meta={"masked": True})
        column(g, "m.gold", "dob", pii_type="unknown", pii_detected_by="tag")
        column(
            g,
            "m.gold",
            "ssn",
            pii_type="ssn",
            pii_detected_by="tag",
            meta={"transformation": "hash"},
        )
        column(g, "m.gold", "amount")
        column(g, "m.silver", "email", pii_type="email", pii_detected_by="tag")
        return g

    def test_pii_in_gold_must_be_masked(self, graph):
        unmasked = check("PII_001", graph, "m.gold", "email")
        assert unmasked.result is Result.FAIL
        assert unmasked.confidence is Confidence.HEURISTIC  # detected from the column name
        for masked in ("email_hash", "phone", "ssn"):
            outcome = check("PII_001", graph, "m.gold", masked)
            assert outcome.result is Result.PASS, masked
        assert check("PII_001", graph, "m.gold", "amount").result is Result.NOT_APPLICABLE
        assert check("PII_001", graph, "m.silver", "email").result is Result.NOT_APPLICABLE

    def test_pii_must_be_tagged(self, graph):
        assert check("PII_002", graph, "m.gold", "email").result is Result.FAIL
        assert check("PII_002", graph, "m.gold", "phone").result is Result.PASS
        assert check("PII_002", graph, "m.gold", "amount").result is Result.NOT_APPLICABLE

    def test_pii_type_must_be_specific(self, graph):
        assert check("PII_003", graph, "m.gold", "dob").result is Result.FAIL
        assert check("PII_003", graph, "m.gold", "phone").result is Result.PASS
        assert check("PII_003", graph, "m.gold", "amount").result is Result.NOT_APPLICABLE

    def test_pii_rules_cover_sources_and_seeds(self):
        assert NodeKind.SOURCE in RULES["PII_002"].resource_types
        assert NodeKind.SEED in RULES["PII_002"].resource_types
        assert RULES["DBT_004"].resource_types == {NodeKind.MODEL}


class TestRemapped:
    def test_primary_key_from_tests_or_contract(self):
        g = MetadataGraph()
        model(g, "m.tested")
        column(g, "m.tested", "id", test_types=["not_null", "unique"], test_count=2)
        model(g, "m.contract", primary_key=["id"])
        model(g, "m.none")
        column(g, "m.none", "id", test_types=["unique"], test_count=1)
        assert check("DBT_005", g, "m.tested").result is Result.PASS
        assert check("DBT_005", g, "m.contract").result is Result.PASS
        assert check("DBT_005", g, "m.none").result is Result.FAIL

    def test_primary_key_unique_test(self):
        g = MetadataGraph()
        model(g, "m.declared", primary_key=["ID"])
        column(g, "m.declared", "id")
        model(g, "m.guessed")
        column(g, "m.guessed", "id", semantic_type="surrogate_key", test_types=["unique"])
        model(g, "m.composite", primary_key=["a", "b"])
        column(g, "m.composite", "a")
        column(g, "m.composite", "b")
        model(g, "m.combined", primary_key=["a", "b"], test_types=["unique_combination_of_columns"])
        column(g, "m.combined", "a")
        column(g, "m.combined", "b")
        model(g, "m.keyless")

        declared = check("DBT_006", g, "m.declared")
        assert (declared.result, declared.confidence) == (Result.FAIL, Confidence.DECLARED)
        guessed = check("DBT_006", g, "m.guessed")
        assert (guessed.result, guessed.confidence) == (Result.PASS, Confidence.HEURISTIC)
        assert "unique_combination_of_columns" in check("DBT_006", g, "m.composite").message
        assert check("DBT_006", g, "m.combined").result is Result.PASS
        assert check("DBT_006", g, "m.keyless").result is Result.NOT_APPLICABLE

    def test_foreign_keys_need_relationships_tests(self):
        g = MetadataGraph()
        model(g, "m.orders", primary_key=["order_id"])
        column(g, "m.orders", "order_id", semantic_type="foreign_key")  # its own key
        column(g, "m.orders", "store_id", semantic_type="foreign_key", test_types=["unique"])
        column(g, "m.orders", "customer_id", semantic_type="foreign_key")
        outcome = check("DBT_007", g, "m.orders")
        assert outcome.result is Result.FAIL
        assert outcome.message.endswith(": customer_id")
        assert outcome.confidence is Confidence.HEURISTIC
        g.columns["m.orders#customer_id"].test_types = ["relationships"]
        assert check("DBT_007", g, "m.orders").result is Result.PASS
        model(g, "m.plain")
        assert check("DBT_007", g, "m.plain").result is Result.NOT_APPLICABLE

    def test_gold_models_need_tests(self):
        g = MetadataGraph()
        model(g, "m.gold", layer="gold")
        model(g, "m.silver", layer="silver")
        assert check("DBT_008", g, "m.gold").result is Result.FAIL
        assert check("DBT_008", g, "m.silver").result is Result.NOT_APPLICABLE

    def test_owner_and_domain(self):
        g = MetadataGraph()
        model(g, "m.owned", owner="data-team", meta={"domain": "finance"})
        model(g, "m.grouped", group="finance")
        model(g, "m.orphan", meta={"domain": ""}, domain="from-path")
        assert check("DBT_009", g, "m.owned").result is Result.PASS
        assert check("DBT_009", g, "m.grouped").result is Result.PASS
        assert check("DBT_009", g, "m.orphan").result is Result.FAIL
        assert check("DBT_010", g, "m.owned").result is Result.PASS
        # A domain inferred from the folder is not a declared meta.domain
        assert check("DBT_010", g, "m.orphan").result is Result.FAIL


def test_pilot_shop_results():
    graph = build_dbt_graph(DbtParser().parse(project_config("pilot-1.12")))
    evaluation = evaluate(graph)
    counts = {
        rule_id: {result.value: n for result, n in sorted(c.items(), key=lambda i: i[0].value)}
        for rule_id, c in evaluation.counts.items()
    }
    # Spot checks against the project, verified by hand
    assert counts["DBT_002"] == {"fail": 3, "pass": 8}  # 8 of 11 models are tested
    assert counts["DBT_009"] == {"fail": 11}  # pilot-shop sets no owners
    assert counts["MED_001"] == {"fail": 2, "not_applicable": 5, "pass": 4}
    failing = {f.subject for f in evaluation.findings if f.rule_id == "MED_001"}
    assert failing == {"model.pilot_shop.customers", "model.pilot_shop.orders"}
    score = evaluation.score()
    assert (score.passed, score.failed, score.coverage) == (64, 98, 100.0)


def test_the_rules_documentation_is_up_to_date():
    """Diagnostics link to docs/rules.md#<rule id>; scripts/rules_doc.py writes it."""
    import importlib.util
    from pathlib import Path

    script = Path(__file__).resolve().parents[2] / "scripts" / "rules_doc.py"
    spec = importlib.util.spec_from_file_location("rules_doc", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.main(["--check"]) == 0
    text = module.DOC.read_text()
    assert all(f"### {rule.id}\n" in text for rule in builtin_rules())
