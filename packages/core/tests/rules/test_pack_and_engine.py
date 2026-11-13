"""Rule packs (.lineagepilot/rules.yml) and the evaluation engine."""

from __future__ import annotations

import textwrap

import pytest
from support import project_config

from lineagepilot_core.graph import (
    Column,
    Confidence,
    MetadataGraph,
    Node,
    NodeKind,
    build_dbt_graph,
)
from lineagepilot_core.parsers import DbtParser
from lineagepilot_core.rules import (
    Category,
    Outcome,
    Result,
    Rule,
    Scope,
    Severity,
    builtin_rules,
    evaluate,
    load_rule_pack,
    parse_rule_pack,
)
from lineagepilot_core.rules.pack import MAX_PACK_BYTES, MAX_RULES

BUILTIN = {rule.id for rule in builtin_rules()}


def pack_from(tmp_path, text: str):
    path = tmp_path / "rules.yml"
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return load_rule_pack(path, BUILTIN)


@pytest.fixture
def graph() -> MetadataGraph:
    g = MetadataGraph()
    g.add_node(
        Node(
            "model.p.fin_fct_orders",
            NodeKind.MODEL,
            "fin_fct_orders",
            layer="gold",
            tags=["finance"],
            materialization="table",
            file_path="models/finance/fin_fct_orders.sql",
            patch_path="models/finance/_models.yml",
            description="Orders",
            owner="fin",
            meta={"domain": "finance", "sla": "daily"},
            test_count=1,
            test_types=["unique"],
        )
    )
    g.add_node(
        Node(
            "model.p.orders",
            NodeKind.MODEL,
            "orders",
            layer="gold",
            tags=["finance"],
            file_path="models/finance/orders.sql",
        )
    )
    g.add_node(Node("model.p.stg", NodeKind.MODEL, "stg", layer="silver"))
    g.add_column(Column("model.p.orders#amount", "model.p.orders", "amount", tags=["certified"]))
    g.add_column(Column("model.p.orders#raw_amount", "model.p.orders", "raw_amount"))
    return g


class TestLoading:
    def test_a_full_pack(self, tmp_path):
        pack = pack_from(
            tmp_path,
            """
            version: 1
            disable: [DBT_003]
            severity: {DBT_001: error}
            rules:
              - id: ACME_001
                name: Finance marts are prefixed
                severity: error
                when: {layer: [gold], tags: [finance]}
                require: {name_matches: "^fin_(dim|fct)_", owner: true}
                remediation: Rename it
            """,
        )
        assert pack.problems == []
        assert pack.disabled == {"DBT_003"}
        assert pack.severities == {"DBT_001": Severity.ERROR}
        (rule,) = pack.rules
        assert (rule.id, rule.severity, rule.scope, rule.category) == (
            "ACME_001",
            Severity.ERROR,
            Scope.NODE,
            Category.CUSTOM,
        )
        assert rule.source == str(tmp_path / "rules.yml")

    def test_a_missing_file_is_an_empty_pack(self, tmp_path):
        pack = load_rule_pack(tmp_path / "nope.yml")
        assert (pack.rules, pack.problems) == ([], [])

    @pytest.mark.parametrize(
        ("text", "problem"),
        [
            ("rules: [unclosed", "cannot be read"),
            ("- a list", "the top level must be a mapping"),
            ("version: 2", "version 2 is not supported"),
            ("disable: DBT_003", "'disable' must be a list"),
            ("severity: [x]", "'severity' must be a mapping"),
            ("severity: {DBT_001: loud}", "'loud' is not valid"),
            ("rules: {}", "'rules' must be a list"),
            ("extra: 1", "unknown key 'extra' ignored"),
        ],
    )
    def test_file_level_problems(self, tmp_path, text, problem):
        pack = pack_from(tmp_path, text)
        assert any(problem in p for p in pack.problems), pack.problems

    def test_an_empty_file_is_an_empty_pack(self, tmp_path):
        assert pack_from(tmp_path, "").problems == []

    def test_oversized_and_unreadable_files(self, tmp_path):
        big = tmp_path / "big.yml"
        big.write_bytes(b"#" * (MAX_PACK_BYTES + 1))
        assert "larger than" in load_rule_pack(big).problems[0]
        binary = tmp_path / "bin.yml"
        binary.write_bytes(b"\xff\xfe\x00")
        assert "cannot be read" in load_rule_pack(binary).problems[0]

    @pytest.mark.parametrize(
        ("rule", "problem"),
        [
            ("- not a mapping", "each rule must be a mapping"),
            ("- {id: acme, name: x, require: {owner: true}}", "'id' must be upper case"),
            ("- {id: ACME_1, require: {owner: true}}", "'name' is required"),
            ("- {id: ACME_1, name: x, severity: loud, require: {owner: true}}", "is not a valid"),
            (
                "- {id: ACME_1, name: x, resource_types: [view], require: {owner: true}}",
                "resource_types",
            ),
            (
                "- {id: ACME_1, name: x, scope: column, resource_types: [exposure],"
                " require: {tags: [a]}}",
                "no columns",
            ),
            ("- {id: ACME_1, name: x}", "at least one check"),
            ("- {id: ACME_1, name: x, require: [owner]}", "must be mappings"),
            ("- {id: ACME_1, name: x, require: {owner: yes please}}", "true or false"),
            ("- {id: ACME_1, name: x, require: {tags: certified}}", "list of text"),
            (
                "- {id: ACME_1, name: x, when: {layer: gold}, require: {owner: true}}",
                "list of text",
            ),
            ("- {id: ACME_1, name: x, require: {name_matches: '(a+)+'}}", "backtrack"),
            ("- {id: ACME_1, name: x, description: [1], require: {owner: true}}", "must be text"),
            ("- {id: DBT_001, name: x, require: {owner: true}}", "already used"),
        ],
    )
    def test_invalid_rules_are_skipped_with_a_reason(self, tmp_path, rule, problem):
        pack = pack_from(tmp_path, f"rules:\n{rule}\n")
        assert pack.rules == []
        assert any(problem in p for p in pack.problems), pack.problems

    def test_duplicate_custom_ids(self, tmp_path):
        pack = pack_from(
            tmp_path,
            """
            rules:
              - {id: ACME_1, name: a, require: {owner: true}}
              - {id: ACME_1, name: b, require: {owner: true}}
            """,
        )
        assert [r.name for r in pack.rules] == ["a"]
        assert "ID ACME_1 is already used" in pack.problems[0]

    def test_rule_count_is_capped(self):
        rules = [
            {"id": f"R_{i}", "name": "x", "require": {"owner": True}} for i in range(MAX_RULES + 5)
        ]
        pack = parse_rule_pack({"rules": rules}, "rules.yml", set())
        assert len(pack.rules) == MAX_RULES
        assert "only the first" in pack.problems[0]

    def test_unknown_checks_make_a_rule_unsupported_not_invalid(self, tmp_path):
        pack = pack_from(
            tmp_path,
            """
            rules:
              - {id: ACME_1, name: x, require: {owner: true, freshness: 1h}}
              - {id: ACME_2, name: y, future_key: 1}
            """,
        )
        assert pack.problems == []
        assert [r.id for r in pack.rules] == ["ACME_1", "ACME_2"]


class TestCustomRules:
    def evaluate(self, graph, tmp_path, text):
        return evaluate(graph, pack_from(tmp_path, text))

    def test_when_and_require(self, graph, tmp_path):
        evaluation = self.evaluate(
            graph,
            tmp_path,
            """
            rules:
              - id: ACME_1
                name: Finance marts
                when:
                  layer: [gold]
                  tags: [Finance]
                  materialization: [table, view]
                  path_matches: "^models/finance/"
                require:
                  name_matches: "^fin_(dim|fct)_"
                  description: true
                  tests: true
                  test_types: [unique]
                  owner: true
                  domain: true
                  meta_keys: [sla]
            """,
        )
        assert evaluation.counts["ACME_1"] == {Result.PASS: 1, Result.NOT_APPLICABLE: 2}

        graph.nodes["model.p.orders"].materialization = "view"
        evaluation = self.evaluate(
            graph,
            tmp_path,
            """
            rules:
              - id: ACME_1
                name: Finance marts
                when: {tags: [finance]}
                require:
                  name_matches: "^fin_"
                  description: true
                  tests: true
                  test_types: [unique]
                  owner: true
                  domain: true
                  meta_keys: [sla]
                  tags: [certified]
            """,
        )
        findings = {f.subject: f for f in evaluation.findings if f.rule_id == "ACME_1"}
        # The prefixed model only lacks the tag
        assert findings["model.p.fin_fct_orders"].message == "ACME_1: no tag certified"
        finding = findings["model.p.orders"]
        assert finding.message == (
            "ACME_1: name 'orders' does not match '^fin_'; no description; no tests; "
            "no meta.owner or group; no meta.domain; no unique test; no meta.sla; no tag certified"
        )

    def test_column_scope(self, graph, tmp_path):
        evaluation = self.evaluate(
            graph,
            tmp_path,
            """
            rules:
              - {id: COL_1, name: Certified columns, scope: column, when: {name_matches: "amount$"},
                 require: {tags: [certified]}}
            """,
        )
        assert evaluation.counts["COL_1"] == {Result.PASS: 1, Result.FAIL: 1}
        assert [f.subject for f in evaluation.findings if f.rule_id == "COL_1"] == [
            "model.p.orders#raw_amount"
        ]

    def test_unsupported_rules_count_as_unsupported(self, graph, tmp_path):
        evaluation = self.evaluate(
            graph, tmp_path, "rules:\n- {id: ACME_1, name: x, require: {freshness: 1h}}\n"
        )
        assert evaluation.counts["ACME_1"] == {Result.UNSUPPORTED: 3}
        assert "require.freshness" in evaluation.findings[0].message
        assert evaluation.score({"ACME_1"}).coverage == 0.0

    def test_a_regex_that_cannot_run_is_unknown(self, graph, tmp_path):
        graph.nodes["model.p.stg"].name = "s" * 600
        evaluation = self.evaluate(
            graph,
            tmp_path,
            """
            rules:
              - {id: N_1, name: a, when: {name_matches: "^s"}, require: {owner: true}}
              - {id: N_2, name: b, require: {name_matches: "^s"}}
            """,
        )
        assert evaluation.counts["N_1"][Result.UNKNOWN] == 1
        assert evaluation.counts["N_2"][Result.UNKNOWN] == 1


class TestEngine:
    def test_disable_and_severity_overrides(self, graph, tmp_path):
        pack = pack_from(tmp_path, "disable: [DBT_003]\nseverity: {DBT_001: error, NOPE_1: info}\n")
        evaluation = evaluate(graph, pack)
        assert "DBT_003" not in evaluation.rules
        assert evaluation.rules["DBT_001"].severity is Severity.ERROR
        assert evaluation.problems == [
            f"{tmp_path / 'rules.yml'}: severity for unknown rule NOPE_1"
        ]

    def test_score_excludes_unknown_and_unsupported(self):
        def outcome(result):
            return lambda graph, node, column: Outcome(result)

        rules = [
            Rule(
                f"R_{r.name}",
                r.value,
                "",
                Severity.WARNING,
                Category.CUSTOM,
                Scope.NODE,
                outcome(r),
            )
            for r in Result
        ]
        g = MetadataGraph()
        g.add_node(Node("m", NodeKind.MODEL, "m"))
        score = evaluate(g, rules=rules).score()
        assert (
            score.passed,
            score.failed,
            score.unknown,
            score.unsupported,
            score.not_applicable,
        ) == (
            1,
            1,
            1,
            1,
            1,
        )
        assert (score.score, score.weighted_score, score.coverage) == (50.0, 50.0, 50.0)

    def test_weighted_score(self):
        rules = [
            Rule(
                "A",
                "a",
                "",
                Severity.CRITICAL,
                Category.CUSTOM,
                Scope.NODE,
                lambda *_: Outcome(Result.FAIL),
            ),
            Rule(
                "B",
                "b",
                "",
                Severity.INFO,
                Category.CUSTOM,
                Scope.NODE,
                lambda *_: Outcome(Result.PASS),
            ),
        ]
        g = MetadataGraph()
        g.add_node(Node("m", NodeKind.MODEL, "m"))
        score = evaluate(g, rules=rules).score()
        assert (score.score, score.weighted_score) == (50.0, round(1 / 11 * 100, 2))

    def test_nothing_decided_has_no_score(self):
        score = evaluate(MetadataGraph()).score()
        assert (score.score, score.weighted_score, score.coverage) == (None, None, None)

    def test_a_failing_check_is_unknown_not_a_crash(self, graph):
        def explode(graph, node, column):
            raise RuntimeError("bug")

        rule = Rule("BOOM", "boom", "", Severity.ERROR, Category.CUSTOM, Scope.NODE, explode)
        evaluation = evaluate(graph, rules=[rule])
        assert evaluation.counts["BOOM"] == {Result.UNKNOWN: 3}
        assert evaluation.findings[0].message == "the rule could not be evaluated: RuntimeError"

    def test_findings_point_at_yaml_or_sql(self, graph):
        evaluation = evaluate(graph)
        yaml_rule = next(f for f in evaluation.findings if f.rule_id == "DBT_010")
        assert yaml_rule.evidence.location.path == "models/finance/orders.sql"  # no YAML file
        graph.nodes["model.p.orders"].patch_path = "models/finance/_models.yml"
        evaluation = evaluate(graph)
        finding = next(
            f
            for f in evaluation.findings
            if f.rule_id == "DBT_010" and f.subject == "model.p.orders"
        )
        assert finding.evidence.location.path == "models/finance/_models.yml"
        naming = next(
            f
            for f in evaluation.findings
            if f.rule_id == "MED_005" and f.subject == "model.p.orders"
        )
        assert naming.evidence.location.path == "models/finance/orders.sql"
        assert evaluation.findings_for("model.p.stg") == [
            f for f in evaluation.findings if f.subject == "model.p.stg"
        ]
        stg = [f for f in evaluation.findings if f.subject == "model.p.stg"]
        assert all(f.evidence.location is None for f in stg)  # no file at all

    def test_findings_carry_manifest_evidence(self):
        graph = build_dbt_graph(DbtParser().parse(project_config("pilot-1.12")))
        evaluation = evaluate(graph)
        assert evaluation.findings
        for finding in evaluation.findings:
            assert finding.evidence.artifact is graph.manifest
            assert finding.evidence.confidence in Confidence
            assert finding.result in (Result.FAIL, Result.UNKNOWN, Result.UNSUPPORTED)
        assert evaluation.findings == sorted(
            evaluation.findings, key=lambda f: (f.subject, f.rule_id)
        )
