"""Data flow annotation loading.

Ported from data-capsule ``backend/tests/unit/parsers/test_data_flow_annotations.py``.
Task IDs follow design section 5.1 (``airflow:<dag_id>.<task_id>``), and references are
dbt unique_ids or relation names rather than server URNs.
"""

from __future__ import annotations

import json
import textwrap

import pytest
import yaml

from lineagepilot_core.graph.ids import airflow_task_id
from lineagepilot_core.parsers.data_flow_annotations import (
    AIRFLOW_FILE,
    LINEAGE_FILE,
    DataFlowAnnotationLoader,
    choose_file,
)

STG_ACCOUNTS = "model.finance.stg_chart_of_accounts"
RAW_TRANSACTIONS = "analytics.staging.raw_transactions"
GL_TRANSACTIONS = "analytics.facts.gl_transactions"
REVENUE = "model.finance.revenue_by_month"

LOAD_GL = "airflow:finance_gl_pipeline.load_gl_transactions"
REPORT = "airflow:finance_gl_pipeline.generate_revenue_report"


@pytest.fixture
def yaml_file(tmp_path):
    path = tmp_path / "annotations.yaml"
    path.write_text(
        f"""
pipelines:
  - pipeline_id: finance_gl_pipeline
    instance: prod
    tasks:
      - task_id: validate_chart_of_accounts
        consumes:
          - {STG_ACCOUNTS}

      - task_id: load_gl_transactions
        consumes:
          - {STG_ACCOUNTS}
          - urn: {RAW_TRANSACTIONS}
            access_pattern: incremental
        produces:
          - urn: {GL_TRANSACTIONS}
            operation: insert

      - task_id: generate_revenue_report
        consumes:
          - {GL_TRANSACTIONS}
        produces:
          - urn: {REVENUE}
            operation: merge
        transforms:
          - urn: {REVENUE}
        validates:
          - {GL_TRANSACTIONS}
""",
        encoding="utf-8",
    )
    return str(path)


@pytest.fixture
def json_file(tmp_path):
    data = {
        "pipelines": [
            {
                "pipeline_id": "customer_pipeline",
                "tasks": [
                    {
                        "task_id": "extract_customers",
                        "produces": [{"urn": "staging.raw.customers", "operation": "insert"}],
                    }
                ],
            }
        ]
    }
    path = tmp_path / "annotations.json"
    path.write_text(json.dumps(data))
    return str(path)


def load(path: str) -> DataFlowAnnotationLoader:
    loader = DataFlowAnnotationLoader(path)
    assert loader.load_annotations()
    return loader


class TestDataFlowAnnotationLoader:
    def test_load_yaml(self, yaml_file):
        assert len(load(yaml_file).get_all_annotations()) == 3

    def test_load_json(self, json_file):
        loader = load(json_file)
        assert loader.has_annotation("airflow:customer_pipeline.extract_customers")

    def test_missing_file(self):
        assert not DataFlowAnnotationLoader("/nonexistent/file.yaml").load_annotations()

    def test_no_file_given(self):
        assert not DataFlowAnnotationLoader().load_annotations()

    def test_unsupported_extension(self, tmp_path):
        path = tmp_path / "annotations.txt"
        path.write_text("pipelines: []")
        assert not DataFlowAnnotationLoader(str(path)).load_annotations()

    def test_argument_overrides_constructor(self, yaml_file, json_file):
        loader = DataFlowAnnotationLoader(json_file)
        assert loader.load_annotations(yaml_file)
        assert loader.has_annotation(LOAD_GL)

    def test_task_id_follows_the_design(self, yaml_file):
        """Design section 5.1; the server's instance segment is ignored."""
        assert airflow_task_id("dag", "task") == "airflow:dag.task"
        assert load(yaml_file).has_annotation(
            "airflow:finance_gl_pipeline.validate_chart_of_accounts"
        )

    def test_get_and_has(self, yaml_file):
        loader = load(yaml_file)
        annotation = loader.get_annotation(LOAD_GL)
        assert annotation is not None and annotation.task_id == LOAD_GL
        assert not loader.has_annotation("airflow:finance_gl_pipeline.nonexistent")
        assert loader.get_annotation("airflow:nope.nope") is None

    def test_consumes_with_access_pattern(self, yaml_file):
        annotation = load(yaml_file).get_annotation(LOAD_GL)
        assert annotation.consumes == [STG_ACCOUNTS, RAW_TRANSACTIONS]
        assert annotation.access_patterns == {RAW_TRANSACTIONS: "incremental"}

    def test_produces_with_operation(self, yaml_file):
        annotation = load(yaml_file).get_annotation(LOAD_GL)
        assert annotation.produces == [GL_TRANSACTIONS]
        assert annotation.operations == {GL_TRANSACTIONS: "insert"}

    def test_transforms_and_validates(self, yaml_file):
        annotation = load(yaml_file).get_annotation(REPORT)
        assert annotation.transforms == [REVENUE]
        assert annotation.validates == [GL_TRANSACTIONS]

    def test_get_all_returns_a_copy(self, yaml_file):
        loader = load(yaml_file)
        loader.get_all_annotations().clear()
        assert len(loader.get_all_annotations()) == 3

    def test_validate_references(self, yaml_file):
        loader = load(yaml_file)
        known = {STG_ACCOUNTS, RAW_TRANSACTIONS, GL_TRANSACTIONS, REVENUE}
        assert loader.validate_references(known) == []

        warnings = loader.validate_references({STG_ACCOUNTS})
        assert warnings
        assert all("references unknown node" in w for w in warnings)
        assert f"Task {LOAD_GL} references unknown node: {RAW_TRANSACTIONS}" in warnings

    def test_multiple_pipelines(self, tmp_path):
        data = {
            "pipelines": [
                {"pipeline_id": "p1", "tasks": [{"task_id": "t1", "produces": ["a"]}]},
                {"pipeline_id": "p2", "tasks": [{"task_id": "t2", "produces": ["b"]}]},
            ]
        }
        path = tmp_path / "multi.yaml"
        path.write_text(yaml.dump(data))
        loader = load(str(path))
        assert set(loader.get_all_annotations()) == {"airflow:p1.t1", "airflow:p2.t2"}

    @pytest.mark.parametrize("content", ["{}", "", "- just\n- a list\n", "null"])
    def test_files_without_annotations_load_empty(self, tmp_path, content):
        path = tmp_path / "empty.yaml"
        path.write_text(content)
        assert load(str(path)).get_all_annotations() == {}

    def test_malformed_yaml(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text("bad: [yaml: content")
        assert not DataFlowAnnotationLoader(str(path)).load_annotations()

    def test_malformed_json(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("{not json")
        assert not DataFlowAnnotationLoader(str(path)).load_annotations()

    def test_invalid_entries_are_skipped(self, tmp_path):
        data = {
            "pipelines": [
                "not a mapping",
                {"tasks": [{"task_id": "orphan"}]},
                {"pipeline_id": 7, "tasks": []},
                {
                    "pipeline_id": "p",
                    "tasks": [
                        "not a mapping",
                        {"consumes": ["x"]},
                        {
                            "task_id": "t",
                            "consumes": ["ok", 5, {"no_urn": 1}, {"urn": 9}],
                            "produces": "not a list",
                            "validates": [{"urn": "v", "operation": ["not", "a", "string"]}],
                        },
                    ],
                },
            ]
        }
        path = tmp_path / "messy.yaml"
        path.write_text(yaml.dump(data))
        annotations = load(str(path)).get_all_annotations()
        assert set(annotations) == {"airflow:p.t"}
        annotation = annotations["airflow:p.t"]
        assert (annotation.consumes, annotation.produces, annotation.validates) == (
            ["ok"],
            [],
            ["v"],
        )


def loaded(tmp_path, text: str) -> DataFlowAnnotationLoader:
    path = tmp_path / "lineage.yml"
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return load(str(path))


class TestTools:
    """``tool`` on a pipeline (task 7.4, D-14)."""

    def test_the_tool_defaults_to_airflow(self, yaml_file):
        annotation = load(yaml_file).get_annotation(LOAD_GL)
        assert annotation.tool == "airflow" and annotation.to_json()["tool"] == "airflow"

    def test_a_task_of_another_tool_is_named_by_it(self, tmp_path):
        loader = loaded(
            tmp_path,
            """
            pipelines:
              - pipeline_id: nightly
                tool: Dagster
                tasks: [{task_id: orders_asset, consumes: [model.shop.orders]}]
              - pipeline_id: nightly
                tasks: [{task_id: orders_asset, consumes: [model.shop.orders]}]
            """,
        )
        assert set(loader.get_all_annotations()) == {
            "dagster:nightly.orders_asset",
            "airflow:nightly.orders_asset",
        }
        assert [a.task_id for a in loader.for_tool("dagster")] == ["dagster:nightly.orders_asset"]
        assert [a.task_id for a in loader.for_tool("airflow")] == ["airflow:nightly.orders_asset"]

    @pytest.mark.parametrize("tool", ["7", "''", "two words", "[airflow]", "dag-ster"])
    def test_a_tool_that_is_not_a_name_drops_its_pipeline(self, tmp_path, tool):
        loader = loaded(
            tmp_path,
            f"""
            pipelines:
              - pipeline_id: p
                tool: {tool}
                tasks: [{{task_id: t, consumes: [x]}}]
            """,
        )
        assert loader.get_all_annotations() == {}
        assert any("is not a tool's name" in p for p in loader.problems)


class TestConsumers:
    """The ``consumers`` section (task 7.4, design_next_phase section 7.3)."""

    def test_the_designs_example(self, tmp_path):
        loader = loaded(
            tmp_path,
            """
            consumers:
              - id: tableau:finance/gl_overview
                tool: tableau
                owner: finance-analytics
                consumes:
                  - model.finance.fct_gl
                  - model.finance.fct_gl#amount   # a column, optional
            """,
        )
        assert loader.problems == []
        [consumer] = loader.get_consumers().values()
        assert consumer.to_json() == {
            "id": "tableau:finance/gl_overview",
            "tool": "tableau",
            "name": "finance/gl_overview",
            "owner": "finance-analytics",
            "consumes": [
                {"node": "model.finance.fct_gl", "column": None},
                {"node": "model.finance.fct_gl", "column": "amount"},
            ],
        }

    @pytest.mark.parametrize(
        ("entry", "expected"),
        [
            ("{id: tableau:gl, consumes: [m]}", "tableau:gl"),
            ("{id: gl, tool: tableau, consumes: [m]}", "tableau:gl"),
            ("{id: gl/2024:q1, tool: Tableau, consumes: [m]}", "tableau:gl/2024:q1"),
            ("{id: ' notebook:a b ', consumes: [m]}", "notebook:a b"),
        ],
    )
    def test_the_tool_comes_from_the_id_or_the_tool_key(self, tmp_path, entry, expected):
        loader = loaded(tmp_path, f"consumers:\n  - {entry}\n")
        assert list(loader.get_consumers()) == [expected] and loader.problems == []

    def test_a_reference_may_be_a_mapping_with_urn(self, tmp_path):
        loader = loaded(
            tmp_path,
            "consumers:\n  - {id: tableau:gl, consumes: [{urn: analytics.facts.gl#amount}]}\n",
        )
        [consumer] = loader.get_consumers().values()
        assert consumer.consumes[0].to_json() == {"node": "analytics.facts.gl", "column": "amount"}

    def test_a_reference_given_twice_is_kept_once(self, tmp_path):
        loader = loaded(tmp_path, "consumers:\n  - {id: tableau:gl, consumes: [m#a, m#a, m]}\n")
        assert len(loader.get_consumers()["tableau:gl"].consumes) == 2

    @pytest.mark.parametrize(
        ("text", "problem"),
        [
            ("consumers: 3\n", "consumers is not a list"),
            ("consumers:\n  - nope\n", "is not a mapping"),
            ("consumers:\n  - {consumes: [m]}\n", "has no id"),
            ("consumers:\n  - {id: '  ', consumes: [m]}\n", "has no id"),
            ("consumers:\n  - {id: gl, consumes: [m]}\n", "names no tool"),
            ("consumers:\n  - {id: gl, tool: 7, consumes: [m]}\n", "is not a tool's name"),
            ("consumers:\n  - {id: 'tableau:', consumes: [m]}\n", "no name after the tool"),
            (
                "consumers:\n  - {id: looker:gl, tool: tableau, consumes: [m]}\n",
                "starts with looker: but its tool is tableau",
            ),
            ("consumers:\n  - {id: airflow:d.t, consumes: [m]}\n", "declare it under pipelines"),
            ("consumers:\n  - {id: gl, tool: dagster, consumes: [m]}\n", "runs pipelines"),
            ("consumers:\n  - {id: tableau:gl}\n", "consumes nothing"),
            ("consumers:\n  - {id: tableau:gl, consumes: m}\n", "consumes nothing"),
            ("consumers:\n  - {id: tableau:gl, consumes: [7]}\n", "names no node"),
            ("consumers:\n  - {id: tableau:gl, consumes: ['m#']}\n", "names no node"),
            ("consumers:\n  - {id: tableau:gl, consumes: ['#a']}\n", "names no node"),
            ("consumers:\n  - {id: tableau:gl, consumes: [{node: m}]}\n", "names no node"),
            (f"consumers:\n  - {{id: 'tableau:{'x' * 1001}', consumes: [m]}}\n", "longer than"),
            (f"consumers:\n  - {{id: tableau:gl, consumes: ['{'x' * 1001}']}}\n", "names no"),
        ],
    )
    def test_what_cannot_be_used_is_reported(self, tmp_path, text, problem):
        loader = loaded(tmp_path, text)
        assert any(problem in p for p in loader.problems), loader.problems

    def test_a_bad_reference_does_not_drop_the_good_ones(self, tmp_path):
        loader = loaded(tmp_path, "consumers:\n  - {id: tableau:gl, consumes: [7, m#a]}\n")
        assert [c.column for c in loader.get_consumers()["tableau:gl"].consumes] == ["a"]
        assert len(loader.problems) == 1

    def test_the_first_of_two_with_one_id_is_kept(self, tmp_path):
        loader = loaded(
            tmp_path,
            """
            consumers:
              - {id: tableau:gl, owner: first, consumes: [m]}
              - {id: gl, tool: tableau, owner: second, consumes: [m]}
            """,
        )
        assert loader.get_consumers()["tableau:gl"].owner == "first"
        assert any("declared twice" in p for p in loader.problems)

    def test_a_messages_value_is_cut_short(self, tmp_path):
        loader = loaded(tmp_path, f"consumers:\n  - {{id: gl, tool: '{'y' * 5000}'}}\n")
        assert all(len(p) < 500 for p in loader.problems)


class TestChooseFile:
    """Which file is read (D-14): ``lineage.yml`` wins, and the other is reported."""

    def test_neither(self):
        assert choose_file([]) == (None, [])

    @pytest.mark.parametrize("name", [LINEAGE_FILE, AIRFLOW_FILE])
    def test_either_alone(self, name):
        assert choose_file([name]) == (name, [])

    def test_both(self):
        name, problems = choose_file([AIRFLOW_FILE, LINEAGE_FILE])
        assert name == LINEAGE_FILE
        assert problems == [
            ".lineagepilot/airflow.yml was ignored: .lineagepilot/lineage.yml is read instead"
        ]

    def test_anything_else_is_not_an_annotation_file(self):
        assert choose_file([".lineagepilot/rules.yml"]) == (None, [])
