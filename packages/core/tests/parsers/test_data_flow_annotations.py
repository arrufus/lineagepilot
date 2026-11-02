"""Data flow annotation loading.

Ported from data-capsule ``backend/tests/unit/parsers/test_data_flow_annotations.py``.
Task IDs follow design section 5.1 (``airflow:<dag_id>.<task_id>``), and references are
dbt unique_ids or relation names rather than server URNs.
"""

from __future__ import annotations

import json

import pytest
import yaml

from lineagepilot_core.parsers.data_flow_annotations import (
    DataFlowAnnotationLoader,
    task_id_for,
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
        assert task_id_for("dag", "task") == "airflow:dag.task"
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
