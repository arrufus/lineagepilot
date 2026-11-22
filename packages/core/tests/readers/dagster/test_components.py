"""A dg project's dbt components, read and linked (task 8.3): DbtProjectComponent in defs.yaml."""

from __future__ import annotations

from pathlib import Path

import pytest
from support import copy_impact_shop

from lineagepilot_core.analysis.impact import impact
from lineagepilot_core.graph.model import (
    DAGSTER,
    Confidence,
    EdgeKind,
    MetadataGraph,
    NodeKind,
    Origin,
)
from lineagepilot_core.indexer import index_project
from lineagepilot_core.parsers import safe_yaml
from lineagepilot_core.readers.dagster import components
from lineagepilot_core.readers.dagster.keys import KeyRule
from lineagepilot_core.readers.dagster.projects import read_tables
from lineagepilot_core.readers.dagster.reader import DagsterReader
from lineagepilot_core.readers.views import pipelines_view

READER = DagsterReader()
S = "model.impact_shop"
PYPROJECT = (
    '[project]\nname = "orch"\n\n[tool.dg]\ndirectory_type = "project"\n\n'
    '[tool.dg.project]\nroot_module = "orch"\n'
)
DEFS = "orchestration/src/orch/defs"


def write(root: Path, relative: str, text: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def component(project: str = "{{ project_root }}/../dbt", **attributes: str) -> str:
    lines = ["type: dagster_dbt.DbtProjectComponent", "attributes:", f'  project: "{project}"']
    lines += [f'  {key}: "{value}"' for key, value in attributes.items()]
    return "\n".join(lines) + "\n"


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A repository with the impact shop in dbt/ and a dg project beside it, in orchestration/."""
    root = tmp_path / "repo"
    copy_impact_shop(root / "dbt")
    write(root, "orchestration/pyproject.toml", PYPROJECT)
    return root


def read(repo: Path) -> dict:
    return READER.read(repo / "dbt", repo, ["."], None)


def only(scan: dict, status: str = "resolved") -> dict:
    found = scan["definitions"] if status == "resolved" else scan["unknowns"]
    assert len(found) == 1, (scan["definitions"], scan["unknowns"], scan["problems"])
    return found[0]


class TestTheComponent:
    def test_its_project_and_selection(self, repo):
        write(repo, f"{DEFS}/analytics/defs.yaml", component(select="+orders"))
        found = only(read(repo))
        assert found["nodes"] == [f"{S}.orders", f"{S}.stg_orders"]
        assert (found["location"], found["name"], found["api"]) == (
            "orch",
            "analytics",
            "DbtProjectComponent",
        )
        assert (found["file"], found["line"]) == (f"{DEFS}/analytics/defs.yaml", 1)
        assert found["confidence"] == "inferred" and found["select"] == "+orders"

    def test_by_default_it_makes_an_asset_of_every_model_seed_and_snapshot(self, repo):
        write(repo, f"{DEFS}/dbt/defs.yaml", component())
        nodes = only(read(repo))["nodes"]
        assert f"{S}.orders" in nodes and "seed.impact_shop.raw_orders" in nodes
        assert not any(n.startswith(("source.", "test.", "semantic_model.")) for n in nodes)

    def test_exclude_takes_away_from_the_selection(self, repo):
        write(repo, f"{DEFS}/dbt/defs.yaml", component(select="+orders", exclude="stg_orders"))
        assert only(read(repo))["nodes"] == [f"{S}.orders"]

    def test_several_selection_methods_are_a_union(self, repo):
        write(repo, f"{DEFS}/dbt/defs.yaml", component(select="orders customer_revenue"))
        assert only(read(repo))["nodes"] == [f"{S}.customer_revenue", f"{S}.orders"]

    @pytest.mark.parametrize(
        "project",
        ["../../../../../dbt", "{{ project_root }}/../dbt", "{{project_root}}/../dbt/"],
    )
    def test_the_project_path_relative_to_the_component(self, repo, project):
        write(repo, f"{DEFS}/dbt/defs.yaml", component(project, select="orders"))
        assert only(read(repo))["nodes"] == [f"{S}.orders"]

    def test_the_project_as_a_mapping(self, repo):
        text = (
            "type: dagster_dbt.DbtProjectComponent\nattributes:\n"
            "  project:\n    project_dir: ../../../../../dbt\n  select: orders\n"
        )
        write(repo, f"{DEFS}/dbt/defs.yaml", text)
        assert only(read(repo))["nodes"] == [f"{S}.orders"]

    def test_the_asset_keys_are_dagster_dbt_s_by_default(self, repo):
        write(repo, f"{DEFS}/dbt/defs.yaml", component(select="orders raw_orders"))
        assert only(read(repo))["keys"] == {
            f"{S}.orders": ["orders"],
            # The seed's config sets a schema
            "seed.impact_shop.raw_orders": ["raw", "raw_orders"],
        }

    def test_the_translation_s_key_template_and_prefix_set_the_keys(self, repo):
        text = component(select="orders") + (
            '  translation:\n    key: "dbt/{{ node.name }}"\n    key_prefix: [warehouse]\n'
        )
        write(repo, f"{DEFS}/dbt/defs.yaml", text)
        assert only(read(repo))["keys"] == {f"{S}.orders": ["warehouse", "dbt", "orders"]}

    def test_several_components_in_one_file_are_named_by_their_place(self, repo):
        text = (
            component(select="orders")
            + "---\ntype: dagster.SomeOtherComponent\n---\n"
            + component(select="customer_revenue")
        )
        write(repo, f"{DEFS}/dbt/defs.yaml", text)
        found = read(repo)["definitions"]
        assert [(d["name"], d["line"], d["nodes"]) for d in found] == [
            ("dbt[0]", 1, [f"{S}.orders"]),
            ("dbt[2]", 8, [f"{S}.customer_revenue"]),
        ]

    def test_other_components_are_not_dbt_s(self, repo):
        write(repo, f"{DEFS}/x/defs.yaml", "type: dagster.DefsFolderComponent\n")
        scan = read(repo)
        assert scan["definitions"] == scan["unknowns"] == [] and scan["problems"] == []


class TestCodeLocations:
    def test_the_code_location_name_wins_over_the_project_name(self, repo):
        write(
            repo, "orchestration/pyproject.toml", PYPROJECT + 'code_location_name = "analytics"\n'
        )
        write(repo, f"{DEFS}/dbt/defs.yaml", component(select="orders"))
        assert only(read(repo))["location"] == "analytics"

    def test_a_custom_defs_module(self, repo):
        write(repo, "orchestration/pyproject.toml", PYPROJECT + 'defs_module = "orch.pipelines"\n')
        write(repo, "orchestration/src/orch/pipelines/dbt/defs.yaml", component(select="orders"))
        assert only(read(repo))["name"] == "dbt"

    def test_a_project_without_src(self, repo):
        write(
            repo,
            "orchestration/orch/defs/dbt/defs.yaml",
            component("../../../../dbt", select="orders"),
        )
        assert only(read(repo))["name"] == "dbt"

    def test_a_component_in_no_dg_project_is_named_after_its_folder(self, repo):
        write(repo, "elsewhere/dbt_assets/defs.yaml", component("../../dbt", select="orders"))
        found = only(read(repo))
        assert (found["location"], found["name"]) == ("elsewhere/dbt_assets", "dbt_assets")
        assert found["in_dg_project"] is False

    def test_project_root_needs_a_dg_project(self, repo):
        write(repo, "elsewhere/x/defs.yaml", component())
        found = only(read(repo), "unknown")
        assert found["category"] == "jinja" and "needs a dg project" in found["reason"]


class TestUnknowns:
    @pytest.mark.parametrize(
        ("text", "category", "words"),
        [
            (component("{{ env.DBT_DIR }}"), "jinja", "holds a template"),
            (component(select="tag:{{ env.TAG }}"), "jinja", "select"),
            (component("../nowhere"), "no_project", "matches no dbt project"),
            ("type: dagster_dbt.DbtProjectComponent\n", "not_literal", "no attributes"),
            (
                "type: dagster_dbt.DbtProjectComponent\nattributes:\n  select: orders\n",
                "not_literal",
                "names no dbt project",
            ),
            (
                "type: dagster_dbt.DbtProjectComponent\nattributes:\n"
                "  project:\n    repo_url: https://example.com/x.git\n",
                "remote_manifest",
                "git repository",
            ),
            (
                component() + "  exclude: [a, b]\n",
                "not_literal",
                "exclude is not a string",
            ),
            (component(selector="nightly"), "unsupported_selector", "nightly"),
        ],
    )
    def test_what_cannot_be_read_is_an_unknown_at_the_component(self, repo, text, category, words):
        write(repo, f"{DEFS}/dbt/defs.yaml", text)
        found = only(read(repo), "unknown")
        assert found["category"] == category and words in found["reason"]
        assert (found["file"], found["line"]) == (f"{DEFS}/dbt/defs.yaml", 1)

    def test_a_component_for_another_dbt_project_is_left_to_it(self, repo):
        copy_impact_shop(repo / "other")
        write(repo, f"{DEFS}/dbt/defs.yaml", component("{{ project_root }}/../other"))
        scan = read(repo)
        assert scan["definitions"] == scan["unknowns"] == []


class TestHostileFiles:
    @pytest.mark.parametrize(
        ("text", "words"),
        [
            ("type: [unclosed\n", "could not be read as YAML"),
            (
                "a: &a [x, x, x, x, x, x, x, x, x, x]\n"
                + "".join(
                    f"{chr(98 + i)}: &{chr(98 + i)} [{', '.join(['*' + chr(97 + i)] * 10)}]\n"
                    for i in range(6)
                ),
                "could not be read as YAML",
            ),
            ("[" * 5000 + "]" * 5000 + "\n", "could not be read as YAML"),
        ],
    )
    def test_a_file_that_cannot_be_read_is_a_problem_not_a_failure(self, repo, text, words):
        write(repo, f"{DEFS}/dbt/defs.yaml", text)
        scan = read(repo)
        assert scan["definitions"] == scan["unknowns"] == []
        assert any(words in p for p in scan["problems"]), scan["problems"]

    def test_a_file_over_the_size_limit_is_not_read(self, repo, monkeypatch):
        monkeypatch.setattr(components, "FILE_SIZE_CAP", 50)
        write(repo, f"{DEFS}/dbt/defs.yaml", component(select="orders"))
        scan = read(repo)
        assert scan["definitions"] == [] and any("over" in p for p in scan["problems"])


class TestLinked:
    def linked(self, repo: Path, text: str) -> MetadataGraph:
        write(repo, f"{DEFS}/analytics/defs.yaml", text)
        graph = index_project(repo / "dbt").graph
        READER.link(graph, read(repo))
        return graph

    def test_a_code_location_and_its_definition_run_the_selected_nodes(self, repo):
        graph = self.linked(repo, component(select="+orders"))
        location, task = graph.nodes["dagster:orch"], graph.nodes["dagster:orch.analytics"]
        assert (location.kind, location.tool, location.subkind) == (
            NodeKind.PIPELINE,
            DAGSTER,
            "code_location",
        )
        assert (task.kind, task.tool, task.subkind) == (NodeKind.TASK, DAGSTER, "dbt_assets")
        assert (
            task.properties["pipeline"] == "orch"
            and task.properties["operator"] == "DbtProjectComponent"
        )
        assert task.file_path == f"../{DEFS}/analytics/defs.yaml"
        edges = graph.out_edges(task.id, (EdgeKind.ORCHESTRATES,))
        assert sorted(e.downstream for e in edges) == [f"{S}.orders", f"{S}.stg_orders"]
        [evidence] = edges[0].evidence
        assert evidence.origin is Origin.DAGSTER_DEFS and evidence.confidence is Confidence.INFERRED
        # The file's SHA-256 is its version, as for DAG files
        assert evidence.location.path == f"../{DEFS}/analytics/defs.yaml"
        assert evidence.location.start_line == 0 and len(evidence.artifact.sha256) == 64
        assert edges[0].transformation == "dbt, through Dagster"
        summary = graph.readers[DAGSTER]
        assert (summary.pipelines, summary.tasks, summary.links, summary.unknowns) == (1, 1, 2, 0)

    def test_a_change_to_a_model_reaches_the_definition(self, repo):
        graph = self.linked(repo, component(select="+orders"))
        found = impact(graph, [f"{S}.stg_orders"])
        assert "dagster:orch.analytics" in found.nodes

    def test_the_pipelines_view_lists_it_under_its_code_location(self, repo):
        graph = self.linked(repo, component(select="orders"))
        view = pipelines_view(graph, [f"{S}.orders"], [DAGSTER])
        [location] = view["pipelines"]
        assert (location["id"], location["subkind"]) == ("dagster:orch", "code_location")
        [task] = location["tasks"]
        assert task["id"] == "dagster:orch.analytics" and task["nodes"] == [f"{S}.orders"]
        assert task["how"] == ["dbt, through Dagster"] and task["origins"] == ["dagster_defs"]

    def test_an_unknown_is_about_the_definition_so_the_review_files_it_under_dagster(self, repo):
        graph = self.linked(repo, component("{{ env.X }}"))
        [unknown] = [u for u in graph.unresolved if u.evidence.origin is Origin.DAGSTER_DEFS]
        assert unknown.subject == "dagster:orch.analytics" and unknown.reference == "Jinja"
        assert unknown.reason.startswith(f"../{DEFS}/analytics/defs.yaml:1: the project path")
        assert graph.readers[DAGSTER].unknowns == 1 and graph.readers[DAGSTER].links == 0

    def test_a_selection_that_matches_nothing_says_so(self, repo):
        graph = self.linked(repo, component(select="no_such_model"))
        [unknown] = [u for u in graph.unresolved if u.evidence.origin is Origin.DAGSTER_DEFS]
        assert unknown.reference == "selects nothing" and "no_such_model" in unknown.reason

    def test_a_node_the_graph_does_not_have_says_the_manifest_may_be_stale(self):
        graph = MetadataGraph()
        READER.link(graph, scan_of({"nodes": ["model.x.gone"]}))
        [unknown] = graph.unresolved
        assert unknown.reference == "model.x.gone" and "may be stale" in unknown.reason

    @pytest.mark.parametrize(
        "broken",
        [
            {"file": None},
            {"location": ""},
            {"name": "n" * 1001},
            {"line": "1"},
            {"line": True},
            {"confidence": "certain"},
        ],
    )
    def test_a_malformed_record_is_not_trusted(self, broken):
        graph = MetadataGraph()
        READER.link(graph, scan_of(broken))
        if "confidence" in broken:  # still linked, but no surer than a guess
            assert graph.nodes and not list(graph.edges)
            assert graph.unresolved[0].evidence.confidence is Confidence.HEURISTIC
        else:
            assert not graph.nodes


def scan_of(change: dict) -> dict:
    record = {
        "api": "DbtProjectComponent",
        "file": "defs.yaml",
        "line": 1,
        "location": "loc",
        "name": "dbt",
        "status": "resolved",
        "nodes": ["model.x.gone"],
        "confidence": "inferred",
        **change,
    }
    return {"project": ".", "files": {"components": ["defs.yaml"]}, "definitions": [record]}


class TestSafeYamlDocuments:
    def test_each_document_with_its_first_line(self):
        found = safe_yaml.load_documents("a: 1\n---\n\n---\nb: 2\n")
        # An empty document is null, and PyYAML places it at the separator after it
        assert [d for d in found if d[0] is not None] == [({"a": 1}, 1), ({"b": 2}, 5)]
        assert len(found) == 3

    def test_the_budget_is_for_the_whole_stream(self):
        document = "x: [" + ", ".join(["1"] * 40) + "]\n"
        assert len(safe_yaml.load_documents(document, max_nodes=100)) == 1
        with pytest.raises(safe_yaml.YAMLTooLarge):
            safe_yaml.load_documents(
                document + "---\n" + document + "---\n" + document, max_nodes=100
            )

    def test_the_number_of_documents_is_bounded(self, monkeypatch):
        monkeypatch.setattr(safe_yaml, "MAX_DOCUMENTS", 2)
        with pytest.raises(safe_yaml.YAMLTooLarge):
            safe_yaml.load_documents("a: 1\n---\nb: 2\n---\nc: 3\n")


class TestPyprojectKeys:
    def test_only_the_keys_it_needs_as_plain_strings(self):
        text = (
            '[project]\nname = "orch"  # a comment\nversion = "1"\n\n'
            '[tool.dg]\ndirectory_type = "project"\n\n'
            "[tool.dg.project]\nroot_module = 'orch'\ncode_location_name = \"a\\\\b\"\n"
            'defs_module = ["not", "a", "string"]\n\n'
            '[tool."dagster"]\nmodule_name = "orch.definitions"\n'
        )
        assert read_tables(text) == {
            "project": {"name": "orch"},
            "tool.dg": {"directory_type": "project"},
            "tool.dg.project": {"root_module": "orch"},
            "tool.dagster": {"module_name": "orch.definitions"},
        }

    def test_a_name_too_long_to_be_a_name_is_not_read(self):
        assert read_tables(f'[project]\nname = "{"n" * 201}"\n') == {"project": {}}


class TestEdges:
    """What the main tests do not reach: other forms of translation, names, and dg projects."""

    @pytest.mark.parametrize(
        ("translation", "rule"),
        [
            (None, KeyRule()),
            ({"group_name": "dbt"}, KeyRule(group="dbt")),
            ({"group_name": "{{ node.schema }}"}, KeyRule(group="{{ node.schema }}")),
            ({"key": "dbt/{{ node.name }}"}, KeyRule(template="dbt/{{ node.name }}")),
            ({"key_prefix": "dbt"}, KeyRule(prefix=("dbt",))),
            (
                {"key_prefix": ["a", "b"], "key": "{{node.schema}}"},
                KeyRule("{{node.schema}}", ("a", "b")),
            ),
        ],
    )
    def test_what_sets_the_asset_keys(self, translation, rule):
        assert components.key_rule(translation) == rule

    @pytest.mark.parametrize(
        ("translation", "words"),
        [
            ("{{ some_function }}", "code Dagster runs"),
            ({"key": ["not", "a", "template"]}, "not a template"),
            ({"key": "{{ node.name | upper }}"}, "more than node fields"),
            ({"key": "{{ context.x }}/{{ node.name }}"}, "more than node fields"),
            ({"key_prefix": "{{ env.X }}"}, "key_prefix"),
            ({"key_prefix": 3}, "key_prefix"),
            ({"group_name": "{{ node.name | upper }}"}, "group_name"),
        ],
    )
    def test_what_cannot_be_read_says_why(self, translation, words):
        rule = components.key_rule(translation)
        assert rule.unknown is not None and words in rule.unknown

    def test_a_name_too_long_to_show_is_an_unknown(self, repo, monkeypatch):
        monkeypatch.setattr(components, "MAX_NAME", 3)
        write(repo, f"{DEFS}/analytics/defs.yaml", component(select="orders"))
        found = only(read(repo), "unknown")
        assert found["category"] == "limit"

    def test_a_pyproject_that_is_not_a_dg_project_is_not_one(self, tmp_path):
        from lineagepilot_core.readers.dagster.projects import read_dg_project

        write(tmp_path, "a/pyproject.toml", '[tool.dagster]\nmodule_name = "a"\n')
        assert read_dg_project(str(tmp_path / "a" / "pyproject.toml"), str(tmp_path)) is None
        assert read_dg_project(str(tmp_path / "missing.toml"), str(tmp_path)) is None

    @pytest.mark.parametrize("defs_module", ["orch.not-a-module", "orch.missing"])
    def test_a_defs_module_that_is_not_there_names_from_the_project_folder(self, repo, defs_module):
        write(repo, "orchestration/pyproject.toml", PYPROJECT + f'defs_module = "{defs_module}"\n')
        write(repo, f"{DEFS}/dbt/defs.yaml", component(select="orders"))
        assert only(read(repo))["name"] == "src/orch/defs/dbt"

    def test_a_dg_project_with_no_names_is_named_after_its_folder(self, repo):
        write(repo, "orchestration/pyproject.toml", "[tool.dg]\n")
        write(repo, "orchestration/defs.yaml", component("../dbt", select="orders"))
        found = only(read(repo))
        assert (found["location"], found["name"]) == ("orchestration", "defs")
