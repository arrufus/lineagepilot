"""dbt asset definitions in Python, read with the shared safe evaluator (task 8.4): @dbt_assets,
DbtProject, the legacy loaders, and the code location each is in."""

from __future__ import annotations

import builtins
import importlib
import runpy
import sys
import textwrap
from pathlib import Path

import pytest
from support import copy_impact_shop

from lineagepilot_core.analysis.impact import impact
from lineagepilot_core.graph.model import DAGSTER, Confidence, EdgeKind, NodeKind, Origin
from lineagepilot_core.indexer import index_project
from lineagepilot_core.readers.common import evaluator
from lineagepilot_core.readers.dagster import locations
from lineagepilot_core.readers.dagster.reader import DagsterReader
from lineagepilot_core.readers.views import pipelines_view

READER = DagsterReader()
S = "model.impact_shop"
PROJECT = """
from pathlib import Path
from dagster_dbt import DbtProject

shop = DbtProject(project_dir=Path(__file__).joinpath("..", "..", "..", "dbt").resolve())
shop.prepare_if_dev()
"""


def write(root: Path, relative: str, text: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip("\n"), encoding="utf-8")
    return path


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """The impact shop in dbt/, and a Python package beside it, orch/shop/, with a DbtProject."""
    root = tmp_path / "repo"
    copy_impact_shop(root / "dbt")
    write(root, "orch/shop/__init__.py", "")
    write(root, "orch/shop/project.py", PROJECT)
    return root


def assets(select: str = "+orders", extra: str = "", **decorator: str) -> str:
    arguments = ", ".join(
        ["manifest=shop.manifest_path", f"select={select!r}"]
        + [f"{key}={value}" for key, value in decorator.items()]
    )
    return textwrap.dedent(f"""
    from dagster_dbt import DbtCliResource, dbt_assets
    from .project import shop
    {extra}

    @dbt_assets({arguments})
    def shop_assets(context, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()
    """).lstrip("\n")


def read(repo: Path) -> dict:
    return READER.read(repo / "dbt", repo, ["."], None)


def only(scan: dict, status: str = "resolved") -> dict:
    found = scan["definitions"] if status == "resolved" else scan["unknowns"]
    assert len(found) == 1, (scan["definitions"], scan["unknowns"], scan["problems"])
    return found[0]


class TestDbtAssets:
    def test_its_project_and_selection(self, repo):
        write(repo, "orch/shop/assets.py", assets())
        found = only(read(repo))
        assert found["nodes"] == [f"{S}.orders", f"{S}.stg_orders"]
        assert (found["api"], found["name"], found["file"], found["line"]) == (
            "@dbt_assets",
            "shop_assets",
            "orch/shop/assets.py",
            6,
        )
        assert found["confidence"] == "inferred" and found["select"] == "+orders"

    def test_by_default_it_makes_an_asset_of_every_model_seed_and_snapshot(self, repo):
        text = assets().replace(", select='+orders'", "")
        write(repo, "orch/shop/assets.py", text)
        nodes = only(read(repo))["nodes"]
        assert f"{S}.orders" in nodes and "seed.impact_shop.raw_orders" in nodes
        assert not any(n.startswith(("source.", "test.", "semantic_model.")) for n in nodes)

    def test_exclude_and_a_list_of_selections(self, repo):
        write(repo, "orch/shop/assets.py", assets(select="+orders", exclude="'stg_orders'"))
        assert only(read(repo))["nodes"] == [f"{S}.orders"]
        text = assets().replace("select='+orders'", "select=['orders', 'customer_revenue']")
        write(repo, "orch/shop/assets.py", text)
        assert only(read(repo))["nodes"] == [f"{S}.customer_revenue", f"{S}.orders"]

    def test_name_sets_the_definition_s_name(self, repo):
        write(repo, "orch/shop/assets.py", assets(name="'shop_dbt'"))
        assert only(read(repo))["name"] == "shop_dbt"

    @pytest.mark.parametrize(
        "manifest",
        [
            'Path(__file__).parent.parent.parent / "dbt" / "target" / "manifest.json"',
            'file_relative_path(__file__, "../../dbt/target/manifest.json")',
            'DBT.joinpath("target", "manifest.json")',
            'os.path.join(os.fspath(DBT), "target", "manifest.json")',
        ],
    )
    def test_a_manifest_path_names_its_project(self, repo, manifest):
        write(
            repo,
            "orch/shop/assets.py",
            f"""
            import os
            from pathlib import Path
            from dagster import file_relative_path
            from dagster_dbt import dbt_assets

            DBT = Path(__file__).joinpath("..", "..", "..", "dbt").resolve()

            @dbt_assets(manifest={manifest}, select="orders")
            def shop_assets(context, dbt):
                ...
            """,
        )
        assert only(read(repo))["nodes"] == [f"{S}.orders"]

    def test_project_names_it_too(self, repo):
        text = assets().replace("manifest=shop.manifest_path", "manifest=M, project=shop")
        write(repo, "orch/shop/assets.py", text)
        assert only(read(repo))["nodes"] == [f"{S}.orders", f"{S}.stg_orders"]

    def test_a_custom_target_path(self, repo):
        project = PROJECT.replace(".resolve())", '.resolve(), target_path="build")')
        write(repo, "orch/shop/project.py", project)
        write(repo, "orch/shop/assets.py", assets())
        assert only(read(repo))["nodes"] == [f"{S}.orders", f"{S}.stg_orders"]

    def test_a_definition_made_by_a_factory_called_with_literals(self, repo):
        write(
            repo,
            "orch/shop/assets.py",
            """
            from dagster_dbt import dbt_assets
            from .project import shop

            def make(select, name):
                @dbt_assets(manifest=shop.manifest_path, select=select, name=name)
                def _assets(context, dbt):
                    ...
                return _assets

            orders_assets = make("orders", "orders_dbt")
            """,
        )
        found = only(read(repo))
        assert (found["name"], found["nodes"], found["line"]) == ("orders_dbt", [f"{S}.orders"], 6)

    def test_a_definition_in_an_imported_factory_is_read_at_its_call(self, repo):
        write(
            repo,
            "orch/shop/factory.py",
            """
            from dagster_dbt import dbt_assets
            from .project import shop

            def make(select):
                @dbt_assets(manifest=shop.manifest_path, select=select, name="made")
                def _assets(context, dbt):
                    ...
                return _assets
            """,
        )
        write(
            repo,
            "orch/shop/assets.py",
            """
            from dagster import Definitions
            from shop.factory import make
            made = make("orders")
            defs = Definitions(assets=[made])
            """,
        )
        found = only(read(repo))
        assert (found["file"], found["line"], found["nodes"]) == (
            "orch/shop/assets.py",
            3,
            [f"{S}.orders"],
        )

    def test_a_team_package_named_like_an_integration_is_followed(self, repo):
        write(repo, "orch/dagster_shop/__init__.py", "")
        write(repo, "orch/dagster_shop/project.py", PROJECT)
        text = assets().replace("from .project", "from dagster_shop.project")
        write(repo, "orch/dagster_shop/assets.py", text)
        assert only(read(repo))["nodes"] == [f"{S}.orders", f"{S}.stg_orders"]

    def test_other_decorated_functions_are_not_dbt_s(self, repo):
        write(
            repo,
            "orch/shop/assets.py",
            """
            from dagster import asset

            @asset(deps=["orders"])
            def export(context):
                subprocess.run(["dbt", "run"])
            """,
        )
        scan = read(repo)
        assert scan["definitions"] == [] and scan["unknowns"] == []


class TestLegacyLoaders:
    def test_load_assets_from_dbt_project_with_its_name(self, repo):
        write(
            repo,
            "orch/shop/assets.py",
            """
            from dagster_dbt import load_assets_from_dbt_project
            from dagster import file_relative_path

            DBT = file_relative_path(__file__, "../../dbt")
            dbt_assets = load_assets_from_dbt_project(DBT, DBT, select="orders")
            """,
        )
        found = only(read(repo))
        assert (found["api"], found["name"], found["nodes"]) == (
            "load_assets_from_dbt_project",
            "dbt_assets",
            [f"{S}.orders"],
        )

    def test_load_assets_from_dbt_manifest(self, repo):
        write(
            repo,
            "orch/shop/assets.py",
            """
            from pathlib import Path
            from dagster_dbt import load_assets_from_dbt_manifest

            MANIFEST = Path(__file__).parent / "../../dbt/target/manifest.json"
            shop = load_assets_from_dbt_manifest(
                manifest=MANIFEST, select="orders", op_name="shop_op"
            )
            """,
        )
        found = only(read(repo))
        assert (found["name"], found["nodes"]) == ("shop_op", [f"{S}.orders"])

    def test_an_unnamed_call_is_named_by_its_line(self, repo):
        write(
            repo,
            "orch/shop/definitions.py",
            """
            from dagster import Definitions, file_relative_path
            from dagster_dbt import load_assets_from_dbt_project
            defs = Definitions(
                assets=load_assets_from_dbt_project(file_relative_path(__file__, "../../dbt")),
            )
            """,
        )
        found = only(read(repo))
        assert found["name"] == "load_assets_from_dbt_project_4"
        assert found["location"] == "orch.shop.definitions"


class TestEnvironment:
    SCAFFOLD = """
    import os
    from pathlib import Path
    from dagster_dbt import DbtCliResource, dbt_assets

    dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "dbt").resolve()
    dbt = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

    if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
        dbt_manifest_path = (
            dbt.cli(["--quiet", "parse"], target_path=Path("target"))
            .wait()
            .target_path.joinpath("manifest.json")
        )
    else:
        dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")

    @dbt_assets(manifest=dbt_manifest_path, select="orders")
    def shop_assets(context, dbt):
        ...
    """

    def test_the_scaffold_s_manifest_is_the_one_used_when_the_variable_is_unset(self, repo):
        write(repo, "orch/shop/assets.py", self.SCAFFOLD)
        found = only(read(repo))
        assert found["nodes"] == [f"{S}.orders"] and found["confidence"] == "heuristic"
        assert any("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD" in note for note in found["notes"])

    @pytest.mark.parametrize(
        "test",
        ['not os.environ.get("ON_LOAD")', '"ON_LOAD" not in os.environ'],
    )
    def test_a_negated_test_runs_its_body(self, repo, test):
        write(
            repo,
            "orch/shop/assets.py",
            f"""
            import os
            from pathlib import Path
            from dagster_dbt import dbt_assets
            DBT = Path(__file__).joinpath("..", "..", "..", "dbt").resolve()
            if {test}:
                manifest = DBT / "target" / "manifest.json"
            else:
                manifest = make_it()

            @dbt_assets(manifest=manifest, select="orders")
            def shop_assets(context, dbt):
                ...
            """,
        )
        assert only(read(repo))["nodes"] == [f"{S}.orders"]

    def test_a_definition_made_only_when_the_variable_is_set_is_an_unknown(self, repo):
        write(
            repo,
            "orch/shop/assets.py",
            """
            import os
            from dagster_dbt import dbt_assets
            from .project import shop
            if os.getenv("WITH_DBT"):
                @dbt_assets(manifest=shop.manifest_path)
                def shop_assets(context, dbt):
                    ...
            """,
        )
        found = only(read(repo), "unknown")
        assert (found["name"], found["category"], found["line"]) == (
            "shop_assets",
            "not_literal",
            6,
        )
        assert "'WITH_DBT' is set" in found["reason"]


class TestUnknowns:
    @pytest.mark.parametrize(
        ("decorator", "category", "words"),
        [
            ("manifest=load_manifest()", "not_literal", "load_manifest() returns"),
            ("manifest=json.loads(M.read_text())", "not_literal", "not known"),
            ("manifest={'nodes': {}}", "not_literal", "a mapping built in Python"),
            ("manifest=shop.manifest_path, select=SELECT", "not_literal", "select is not known"),
            ("manifest=shop.manifest_path, **options", "not_literal", "** mapping"),
            ("select='orders'", "not_literal", "names no manifest"),
            ("manifest='/elsewhere/target/manifest.json'", "no_project", "matches no dbt project"),
            (
                "manifest=shop.manifest_path, select='state:modified'",
                "unsupported_selector",
                "state",
            ),
        ],
    )
    def test_what_cannot_be_read_is_an_unknown_at_the_definition(
        self, repo, decorator, category, words
    ):
        write(
            repo,
            "orch/shop/assets.py",
            f"""
            import json
            from dagster_dbt import dbt_assets
            from .project import shop

            @dbt_assets({decorator})
            def shop_assets(context, dbt):
                ...
            """,
        )
        found = only(read(repo), "unknown")
        assert (found["category"], found["name"], found["line"]) == (category, "shop_assets", 6)
        assert words in found["reason"], found["reason"]

    def test_a_definition_in_a_loop_is_dynamic(self, repo):
        write(
            repo,
            "orch/shop/assets.py",
            """
            from dagster_dbt import dbt_assets
            from .project import shop
            for name in ["a", "b"]:
                @dbt_assets(manifest=shop.manifest_path, name=name)
                def shop_assets(context, dbt):
                    ...
            """,
        )
        found = only(read(repo), "unknown")
        assert found["category"] == "loop_or_factory" and "for-loop" in found["reason"]
        assert "definitions made a dynamic number of times" in found["reason"]

    def test_a_factory_never_called_is_dynamic(self, repo):
        write(
            repo,
            "orch/shop/assets.py",
            """
            from dagster_dbt import dbt_assets
            from .project import shop
            def make(select):
                @dbt_assets(manifest=shop.manifest_path, select=select)
                def made(context, dbt):
                    ...
                return made
            """,
        )
        found = only(read(repo), "unknown")
        assert found["category"] == "loop_or_factory" and "never called" in found["reason"]

    def test_a_definition_for_another_dbt_project_is_left_to_it(self, repo):
        copy_impact_shop(repo / "other")
        text = PROJECT.replace('"dbt"', '"other"')
        write(repo, "orch/shop/project.py", text)
        write(repo, "orch/shop/assets.py", assets())
        scan = read(repo)
        assert scan["definitions"] == [] and scan["unknowns"] == []

    def test_a_file_that_cannot_be_parsed_is_a_problem_not_a_failure(self, repo):
        write(repo, "orch/shop/assets.py", "import dagster\ndef broken(:\n")
        scan = read(repo)
        assert any("orch/shop/assets.py:2: SyntaxError" in p for p in scan["problems"])

    def test_a_bug_in_the_reader_costs_one_file_and_says_so(self, repo, monkeypatch):
        write(repo, "orch/shop/assets.py", assets())

        def broken(self):
            raise TypeError("a bug")

        from lineagepilot_core.readers.dagster import python

        monkeypatch.setattr(python.DagsterScanner, "run", broken)
        scan = read(repo)
        assert "the reader failed on this file: TypeError" in " ".join(scan["problems"])

    def test_the_evaluator_s_budget_holds(self, repo, monkeypatch):
        monkeypatch.setattr(evaluator, "MAX_STEPS", 50)
        write(repo, "orch/shop/assets.py", assets() + "\n" + "x = 1\n" * 100)
        assert any("analysis stopped after 50 steps" in p for p in read(repo)["problems"])

    def test_nothing_about_the_machine_is_in_the_result(self, repo):
        write(repo, "orch/shop/assets.py", assets(select="+orders", exclude="UNKNOWN"))
        scan = read(repo)
        assert str(repo.resolve()) not in str(scan)


class TestCodeLocations:
    def test_a_dg_project_names_its_location(self, repo):
        write(
            repo,
            "orch/pyproject.toml",
            '[project]\nname = "shop-orch"\n\n[tool.dg]\ndirectory_type = "project"\n\n'
            '[tool.dg.project]\nroot_module = "shop"\n',
        )
        write(repo, "orch/shop/assets.py", assets())
        assert only(read(repo))["location"] == "shop-orch"

    def test_the_definitions_call_that_lists_it_in_another_file(self, repo):
        write(repo, "orch/shop/assets.py", assets())
        write(
            repo,
            "orch/shop/definitions.py",
            """
            from dagster import Definitions
            from .assets import shop_assets
            defs = Definitions(assets=[shop_assets])
            """,
        )
        found = only(read(repo))
        assert found["location"] == "orch.shop.definitions" and not found["notes"]

    @pytest.mark.parametrize(
        "call",
        [
            "load_assets_from_modules([assets])",
            "load_assets_from_package_module(shop)",
            "[*load_assets_from_modules(modules=[assets]), other]",
        ],
    )
    def test_definitions_loaded_from_modules(self, repo, call):
        write(repo, "orch/shop/assets.py", assets())
        write(
            repo,
            "orch/definitions.py",
            f"""
            from dagster import Definitions
            from dagster import load_assets_from_modules, load_assets_from_package_module
            import shop
            from shop import assets
            defs = Definitions(assets={call})
            """,
        )
        # D-d: the module the Definitions call is in, as a dotted path from the workspace
        assert only(read(repo))["location"] == "orch.definitions"

    def test_load_assets_from_current_module(self, repo):
        write(
            repo,
            "orch/shop/assets.py",
            assets()
            + "\nfrom dagster import Definitions, load_assets_from_current_module\n"
            + "defs = Definitions(assets=load_assets_from_current_module())\n",
        )
        assert only(read(repo))["location"] == "orch.shop.assets"

    @pytest.mark.parametrize(
        ("workspace", "location"),
        [
            (
                "load_from:\n  - python_module:\n      module_name: shop.definitions\n"
                "      working_directory: orch\n      location_name: shop_location\n",
                "shop_location",
            ),
            (
                "load_from:\n  - python_module:\n      module_name: shop.definitions\n"
                "      working_directory: orch\n",
                "shop.definitions",
            ),
            (
                "load_from:\n  - python_package:\n      package_name: shop\n"
                "      working_directory: orch\n      attribute: defs\n",
                "shop:defs",
            ),
            ("load_from:\n  - python_file: orch/shop/definitions.py\n", "definitions.py"),
        ],
    )
    def test_a_workspace_yaml_entry_names_it(self, repo, workspace, location):
        write(repo, "orch/shop/assets.py", assets())
        write(
            repo,
            "orch/shop/definitions.py",
            "from dagster import Definitions\nfrom .assets import shop_assets\n"
            "defs = Definitions(assets=[shop_assets])\n",
        )
        write(repo, "workspace.yaml", workspace)
        assert only(read(repo))["location"] == location

    def test_a_dagster_cloud_yaml_location(self, repo):
        write(repo, "orch/shop/assets.py", assets())
        write(
            repo,
            "orch/shop/definitions.py",
            "from dagster import Definitions\nfrom .assets import shop_assets\n"
            "defs = Definitions(assets=[shop_assets])\n",
        )
        write(
            repo,
            "dagster_cloud.yaml",
            "locations:\n  - location_name: prod_shop\n    code_source:\n"
            "      module_name: shop.definitions\n    working_directory: ./orch\n",
        )
        assert only(read(repo))["location"] == "prod_shop"

    @pytest.mark.parametrize(
        ("table", "location"),
        [
            ('module_name = "shop.definitions"\ncode_location_name = "shop_loc"\n', "shop_loc"),
            ('module_name = "shop"\n', "shop"),
        ],
    )
    def test_tool_dagster_names_it(self, repo, table, location):
        write(repo, "orch/shop/assets.py", assets())
        write(
            repo,
            "orch/shop/definitions.py",
            "from dagster import Definitions\nfrom .assets import shop_assets\n"
            "defs = Definitions(assets=[shop_assets])\n",
        )
        write(repo, "orch/pyproject.toml", f'[project]\nname = "x"\n\n[tool.dagster]\n{table}')
        assert only(read(repo))["location"] == location

    def test_a_definition_no_definitions_call_loads_is_named_after_its_file(self, repo):
        write(repo, "orch/shop/assets.py", assets())
        scan = read(repo)
        found = only(scan)
        assert found["location"] == "orch.shop.assets"
        assert locations.NO_LOADER in found["notes"][0]
        assert any("loaded by no Definitions call" in p for p in scan["problems"])

    def test_a_yaml_file_that_cannot_be_read_is_a_problem(self, repo):
        write(repo, "orch/shop/assets.py", assets())
        write(repo, "workspace.yaml", "load_from: [")
        assert any("workspace.yaml could not be read" in p for p in read(repo)["problems"])

    def test_dotted_names(self):
        assert locations.dotted("orch/shop/definitions.py") == "orch.shop.definitions"
        assert locations.dotted("orch/shop/__init__.py") == "orch.shop"
        assert locations.dotted("__init__.py") == "__init__"


class TestNeverRun:
    def test_definitions_files_are_never_imported_or_executed(self, repo, tmp_path, monkeypatch):
        marker = tmp_path / "marker"
        monkeypatch.setenv("LP_SIDE_EFFECT_MARKER", str(marker))
        side_effect = (
            "import os\n"
            'open(os.environ["LP_SIDE_EFFECT_MARKER"], "w").write("run")\n'
            'raise SystemExit("must never run")\n'
        )
        write(repo, "orch/shop/project.py", side_effect + PROJECT)
        write(repo, "orch/shop/assets.py", side_effect + assets())
        imported: list[str] = []

        class Spy:
            def find_spec(self, name, path=None, target=None):
                imported.append(name)
                return None

        def forbidden(*a, **k):
            raise AssertionError("the reader must not exec/eval/run user code")

        spy = Spy()
        sys.meta_path.insert(0, spy)
        monkeypatch.setattr(builtins, "exec", forbidden)
        monkeypatch.setattr(builtins, "eval", forbidden)
        monkeypatch.setattr(runpy, "run_path", forbidden)
        monkeypatch.setattr(importlib, "import_module", forbidden)
        try:
            scan = read(repo)
        finally:
            sys.meta_path.remove(spy)
        assert not marker.exists()
        assert not any(name.startswith(("shop", "orch")) for name in imported)
        # and it still read the definition, past the side effect
        assert only(scan)["nodes"] == [f"{S}.orders", f"{S}.stg_orders"]


class TestLinked:
    @pytest.fixture
    def graph(self, repo):
        write(repo, "orch/shop/assets.py", assets())
        write(
            repo,
            "orch/shop/definitions.py",
            "from dagster import Definitions\nfrom .assets import shop_assets\n"
            "defs = Definitions(assets=[shop_assets])\n",
        )
        graph = index_project(repo / "dbt").graph
        READER.link(graph, read(repo))
        return graph

    def test_a_code_location_and_its_definition_run_the_selected_nodes(self, graph):
        location = graph.nodes["dagster:orch.shop.definitions"]
        assert (location.kind, location.subkind, location.tool) == (
            NodeKind.PIPELINE,
            "code_location",
            DAGSTER,
        )
        task = graph.nodes["dagster:orch.shop.definitions.shop_assets"]
        assert (task.kind, task.subkind, task.properties["operator"]) == (
            NodeKind.TASK,
            "dbt_assets",
            "@dbt_assets",
        )
        assert task.file_path == "../orch/shop/assets.py"
        edges = graph.out_edges(task.id, (EdgeKind.ORCHESTRATES,))
        assert sorted(e.downstream for e in edges) == [f"{S}.orders", f"{S}.stg_orders"]
        [evidence] = edges[0].evidence
        assert evidence.origin is Origin.DAGSTER_DEFS and evidence.confidence is Confidence.INFERRED
        assert evidence.location.path == "../orch/shop/assets.py"
        assert evidence.location.start_line == 5 and len(evidence.artifact.sha256) == 64
        summary = graph.readers[DAGSTER]
        assert (summary.pipelines, summary.tasks, summary.links, summary.unknowns) == (1, 1, 2, 0)

    def test_a_change_to_a_model_reaches_the_definition(self, graph):
        found = impact(graph, [f"{S}.stg_orders"])
        assert "dagster:orch.shop.definitions.shop_assets" in found.nodes

    def test_the_pipelines_view_lists_it_under_its_code_location(self, graph):
        view = pipelines_view(graph, [f"{S}.orders"], [DAGSTER])
        [location] = view["pipelines"]
        assert location["id"] == "dagster:orch.shop.definitions"
        [task] = location["tasks"]
        assert task["id"] == "dagster:orch.shop.definitions.shop_assets"
        assert task["nodes"] == [f"{S}.orders"]

    def test_an_unknown_is_filed_under_its_definition(self, repo):
        write(repo, "orch/shop/assets.py", assets().replace("select='+orders'", "select=SELECT"))
        graph = index_project(repo / "dbt").graph
        READER.link(graph, read(repo))
        [unknown] = [u for u in graph.unresolved if u.evidence.origin is Origin.DAGSTER_DEFS]
        assert unknown.subject == "dagster:orch.shop.assets.shop_assets"
        assert unknown.reason.startswith("../orch/shop/assets.py:6: select is not known")
