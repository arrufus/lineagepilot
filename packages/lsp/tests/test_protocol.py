"""Task 2.3: the shared schema, its generator, and the Python validators it produces."""

from __future__ import annotations

import importlib.util
import json
import sys

import pytest
from conftest import REPO

from lineagepilot_lsp import protocol

EXAMPLES = json.loads((REPO / "protocol" / "examples.json").read_text())


def load_generator():
    spec = importlib.util.spec_from_file_location("generate", REPO / "protocol" / "generate.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["generate"] = module
    spec.loader.exec_module(module)
    return module


generate = load_generator()


@pytest.mark.parametrize("example", EXAMPLES["valid"], ids=lambda e: e["type"])
def test_valid_examples_are_accepted(example):
    protocol.validate(example["type"], example["value"])
    record = protocol.RECORDS.get(example["type"])
    if record is not None:
        built = record.from_json(example["value"])
        assert built.to_json() == example["value"]


@pytest.mark.parametrize("example", EXAMPLES["invalid"], ids=lambda e: e["why"])
def test_invalid_examples_are_rejected(example):
    with pytest.raises(protocol.ValidationError):
        protocol.validate(example["type"], example["value"])


def test_records_build_nested_dataclasses():
    result = protocol.ImpactResult.from_json(EXAMPLES["valid"][11]["value"])
    assert isinstance(result.nodes[0], protocol.ImpactedNode)
    assert result.nodes[0].granularity == "model" and result.counts == {"model": 1}
    location = protocol.Location.from_json(EXAMPLES["valid"][8]["value"])
    assert isinstance(location.range, protocol.Range) and location.range.start.character == 10
    params = protocol.GraphParams.from_json(EXAMPLES["valid"][2]["value"])
    assert params.max_nodes == 5000 and params.granularity == "column"


def test_errors_name_the_path():
    with pytest.raises(protocol.ValidationError) as e:
        protocol.validate("ImpactResult", EXAMPLES["invalid"][-2]["value"])
    assert e.value.path == "ImpactResult.counts.model"
    with pytest.raises(protocol.ValidationError, match="unexpected 'extra'"):
        protocol.GraphParams.from_json({**EXAMPLES["valid"][1]["value"], "extra": 1})
    with pytest.raises(protocol.ValidationError, match="must be number"):
        protocol._check({"type": "number"}, float("nan"), "x")
    protocol._check({"type": "number", "maximum": 2}, 1.5, "x")


def test_generated_files_are_up_to_date():
    assert generate.main(["--check"]) == 0


def test_the_generator_rewrites_stale_files(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(generate, "TS_OUT", tmp_path / "protocol.ts")
    monkeypatch.setattr(generate, "PY_OUT", tmp_path / "protocol.py")
    monkeypatch.setattr(generate, "ROOT", tmp_path)
    assert generate.main(["--check"]) == 1
    assert "out of date" in capsys.readouterr().err
    assert generate.main([]) == 0
    assert generate.main(["--check"]) == 0
    assert (tmp_path / "protocol.ts").read_text().startswith("// Generated from")


def schema_with(defs) -> dict:
    return {"$defs": defs, "x-requests": {}, "x-notifications": {}}


@pytest.mark.parametrize(
    "defs, message",
    [
        ({"A": {"type": "string", "pattern": "x"}}, "unsupported keywords ['pattern']"),
        ({"A": {"type": "tuple"}}, "type must be one of"),
        ({"A": {"enum": [1, 2]}}, "only string enums"),
        ({"A": {"$ref": "#/$defs/B"}}, "bad $ref"),
        ({"A": {"$ref": "#/$defs/A", "maxLength": 3}}, "$ref cannot have siblings"),
        (
            {"A": {"type": "object", "properties": {"x": {"type": "string"}}}},
            "additionalProperties to false",
        ),
        (
            {
                "A": {
                    "type": "object",
                    "properties": {},
                    "required": ["x"],
                    "additionalProperties": False,
                }
            },
            "required names unknown properties",
        ),
        ({"A": {"type": "object"}}, "a map needs an additionalProperties schema"),
        (
            {
                "A": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"x": {"type": "string"}},
                        "additionalProperties": False,
                    },
                }
            },
            "must be named in $defs",
        ),
        ({"A": "string"}, "not a schema object"),
    ],
)
def test_unsupported_schemas_are_refused(tmp_path, monkeypatch, defs, message):
    path = tmp_path / "schema.json"
    path.write_text(json.dumps(schema_with(defs)))
    monkeypatch.setattr(generate, "SCHEMA", path)
    with pytest.raises(generate.SchemaError, match=message.replace("$", r"\$").replace("[", r"\[")):
        generate.load()


def test_methods_must_name_definitions(tmp_path, monkeypatch, capsys):
    for schema, message in [
        (
            {
                "$defs": {},
                "x-requests": {"a/b": {"params": "P", "result": "R"}},
                "x-notifications": {},
            },
            "a/b params",
        ),
        ({"$defs": {}, "x-requests": {}, "x-notifications": {"a/c": "S"}}, "a/c: no $defs entry S"),
    ]:
        path = tmp_path / "schema.json"
        path.write_text(json.dumps(schema))
        monkeypatch.setattr(generate, "SCHEMA", path)
        assert generate.main(["--check"]) == 2
        assert message in capsys.readouterr().err


def test_recursive_definitions_are_refused():
    defs = {
        "A": {"type": "array", "items": {"$ref": "#/$defs/B"}},
        "B": {"type": "array", "items": {"$ref": "#/$defs/A"}},
    }
    with pytest.raises(generate.SchemaError, match="recursive definition"):
        generate.ordered(defs)


def test_zod_for_general_unions():
    assert (
        generate.zod({"anyOf": [{"type": "string"}, {"type": "integer"}]})
        == "z.union([z.string(), z.int()])"
    )
    assert generate.zod({"type": "number", "minimum": 0}) == "z.number().min(0)"
    assert generate.zod({"type": "null"}) == "z.null()"
    assert generate.py_type({"type": "number"}, {}) == "float"


def test_every_method_names_a_record():
    for method, (params, result) in protocol.REQUESTS.items():
        assert method.startswith("lineage/")
        assert params in protocol.RECORDS and result in protocol.RECORDS
    assert protocol.NOTIFICATIONS == {"lineage/indexUpdated": "Status"}
