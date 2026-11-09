"""YAML from the workspace with nested aliases or deep nesting (audit LP-SEC-05).

A 411-byte ``dbt_project.yml`` of nested aliases used to become a 312 MB string when one of its
values was put in a message, and deep nesting raised ``RecursionError`` where callers expected a
YAML error, so the server failed to start.
"""

from __future__ import annotations

import time

import pytest
import yaml

from lineagepilot_core.parsers import safe_yaml
from lineagepilot_core.parsers.data_flow_annotations import DataFlowAnnotationLoader
from lineagepilot_core.project import read_project
from lineagepilot_core.rules.pack import load_rule_pack

BOMB = "a: &a [x, x, x, x, x, x, x, x, x]\n" + "".join(
    f"{c}: &{c} [*{p}, *{p}, *{p}, *{p}, *{p}, *{p}, *{p}, *{p}, *{p}]\n"
    for p, c in zip("abcdefgh", "bcdefghi", strict=True)
)
DEEP = "x: " + "[" * 5_000 + "]" * 5_000 + "\n"  # far past the recursion limit


def test_the_loader_refuses_what_safe_load_lets_through():
    with pytest.raises(safe_yaml.YAMLTooLarge):
        safe_yaml.load(BOMB)
    with pytest.raises(safe_yaml.YAMLTooDeep):
        safe_yaml.load(DEEP)
    with pytest.raises(safe_yaml.YAMLTooLarge):
        safe_yaml.load("&a [*a]")
    assert issubclass(safe_yaml.YAMLTooLarge, yaml.YAMLError)


def test_ordinary_yaml_loads_as_safe_load_does():
    text = "base: &b {x: 1}\nm:\n  <<: *b\n  y: [1, 2, *b]\nempty:\n"
    assert safe_yaml.load(text) == yaml.safe_load(text)
    assert safe_yaml.load("") is None
    assert safe_yaml.compose("a: 1").value[0][1].value == "1"


def test_shown_is_short_whatever_the_value():
    assert len(safe_yaml.shown({"k" * 500: list(range(10_000))})) <= safe_yaml.MAX_SHOWN
    assert safe_yaml.shown("models") == "'models'"


@pytest.mark.parametrize("text", [BOMB.replace("a:", "target-path:", 1) + "\n", DEEP])
def test_a_hostile_dbt_project_file_is_reported_not_expanded(tmp_path, text):
    (tmp_path / "dbt_project.yml").write_text(f"name: p\n{text}")
    started = time.perf_counter()
    project = read_project(tmp_path)
    assert time.perf_counter() - started < 10  # a hang, not a speed test
    assert project.target_path == "target"
    assert len(" ".join(project.problems)) < 1000
    assert "cannot be read" in project.problems[0]


def test_a_structured_value_is_shown_cut_short(tmp_path):
    (tmp_path / "dbt_project.yml").write_text(
        "name: p\ntarget-path: {" + ", ".join(f"k{i}: {'v' * 100}" for i in range(500)) + "}\n"
    )
    (problem,) = read_project(tmp_path).problems
    assert len(problem) < 200 and "is not a plain relative path" in problem


@pytest.mark.parametrize("text", [BOMB, DEEP])
def test_a_hostile_rule_pack_is_a_problem_not_an_error(tmp_path, text):
    path = tmp_path / "rules.yml"
    path.write_text(text)
    pack = load_rule_pack(path)
    assert pack.rules == [] and "cannot be read" in pack.problems[0]


@pytest.mark.parametrize("text", [BOMB, DEEP])
def test_a_hostile_annotation_file_is_a_problem(tmp_path, text):
    path = tmp_path / "airflow.yml"
    path.write_text(text)
    loader = DataFlowAnnotationLoader()
    assert loader.load_annotations(str(path)) is False
    assert "cannot be read" in loader.problems[0]
