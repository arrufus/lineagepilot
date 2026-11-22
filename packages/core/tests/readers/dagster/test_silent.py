"""What the held-out corpus found the Dagster reader silent on (task 8.9). The definition of done
asks for every unknown at the line that caused it, so each of these is now read, or an unknown
there: a module importing from its own package (LP-odu), a ``@repository`` function (LP-xpx), a
factory imported from another file (LP-9zx), and what is built on a dbt asset definition the
reader cannot read (LP-ios)."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from support import copy_impact_shop

from lineagepilot_core.readers.airflow.scan import scan_airflow
from lineagepilot_core.readers.dagster.reader import DagsterReader

READER = DagsterReader()
S = "model.impact_shop"
DBT = """
from dagster import file_relative_path

DBT_DIR = file_relative_path(__file__, "../dbt")
"""


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    copy_impact_shop(root / "dbt")
    return root


def write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip("\n"), encoding="utf-8")


def read(repo: Path, files: dict[str, str]) -> dict:
    for relative, text in files.items():
        write(repo, relative, text)
    return READER.read(repo / "dbt", repo, ["."], None)


def named(records: list[dict], name: str) -> dict:
    found = [r for r in records if r["name"] == name]
    assert len(found) == 1, [(r["name"], r["status"], r.get("reason")) for r in records]
    return found[0]


def no_limits(scan: dict) -> None:
    assert not [p for p in scan["problems"] if "nesting too deep" in p], scan["problems"]


# --- LP-odu: a module that imports from its own package --------------------------------------


PACKAGE = {
    "orch/pipeline/__init__.py": """
        from dagster import Definitions
        from .assets import bronze_assets, dbt_assets

        defs = Definitions(assets=[*bronze_assets, *dbt_assets])
        """,
    "orch/pipeline/assets/__init__.py": """
        from dagster import file_relative_path, load_assets_from_modules
        from dagster_dbt import load_assets_from_dbt_project

        from . import bronze

        bronze_assets = load_assets_from_modules([bronze])

        DBT_PROJECT_PATH = file_relative_path(__file__, "../../../dbt")

        dbt_assets = load_assets_from_dbt_project(project_dir=DBT_PROJECT_PATH, select="orders")
        """,
    "orch/pipeline/assets/bronze.py": """
        from dagster import asset

        @asset
        def raw_books(): ...
        """,
}


class TestOwnPackage:
    def test_from_dot_import_in_a_package_is_its_submodule_not_a_loop(self, repo):
        scan = read(repo, PACKAGE)
        no_limits(scan)
        found = named(scan["definitions"], "dbt_assets")
        assert found["nodes"] == [f"{S}.orders"]
        assert found["file"] == "orch/pipeline/assets/__init__.py" and found["line"] == 10

    def test_a_name_imported_round_a_cycle_is_not_followed_for_ever(self, repo):
        scan = read(
            repo,
            {
                "orch/a.py": "from orch.b import SHARED\n",
                "orch/b.py": "from orch.a import SHARED\n",
                "orch/defs.py": DBT
                + textwrap.dedent("""
                    from dagster_dbt import load_assets_from_dbt_project
                    from orch.a import SHARED

                    shop_assets = load_assets_from_dbt_project(project_dir=DBT_DIR, select="orders")
                    """),
            },
        )
        no_limits(scan)
        assert named(scan["definitions"], "shop_assets")["nodes"] == [f"{S}.orders"]

    def test_the_dag_scan_reads_its_own_package_the_same_way(self, repo):
        write(repo, "dags/__init__.py", "from . import settings\n")
        write(repo, "dags/settings.py", 'SELECT = "orders"\n')
        write(
            repo,
            "dags/shop.py",
            """
            from airflow import DAG
            from airflow.operators.bash import BashOperator
            from dags import settings

            with DAG("shop") as dag:
                BashOperator(task_id="run", bash_command=f"dbt run --select {settings.SELECT}")
            """,
        )
        found = scan_airflow(repo / "dbt", repo, ["dags"])
        assert not [u for u in found["unresolved"] if "nesting too deep" in u["reason"]]
        (link,) = found["links"]
        assert link["resolved_models"] == [f"{S}.orders"]


# --- LP-xpx: a @repository function ----------------------------------------------------------


REPOSITORY = (
    DBT
    + """
from dagster import AssetSelection, ScheduleDefinition, define_asset_job, repository, with_resources
from dagster_dbt import load_assets_from_dbt_project


@repository
def shop_repository():
    dbt_assets = with_resources(
        load_assets_from_dbt_project(project_dir=DBT_DIR, select="orders"),
        resource_defs={},
    )
    daily = define_asset_job("daily", selection=AssetSelection.all())
    return [*dbt_assets, daily, ScheduleDefinition(job=daily, cron_schedule="0 6 * * *")]
"""
)


class TestRepository:
    def test_a_repository_function_is_read_as_dagster_runs_it(self, repo):
        scan = read(repo, {"orch/repository.py": REPOSITORY})
        definition = named(scan["definitions"], "dbt_assets")
        assert (definition["api"], definition["line"]) == ("load_assets_from_dbt_project", 12)
        assert definition["nodes"] == [f"{S}.orders"]
        job = named(scan["jobs"], "daily")
        schedule = named(scan["schedules"], "daily_schedule")
        assert job["nodes"] == schedule["nodes"] == [f"{S}.orders"]
        assert schedule["cron"] == "0 6 * * *"
        # What it returns loads them: they are in one code location, and none is unloaded
        assert definition["location"] == job["location"] == schedule["location"]
        for record in (definition, job, schedule):
            assert not [n for n in record["notes"] if "no Definitions call" in n], record
        assert scan["unknowns"] == []

    def test_a_loader_wrapped_in_with_resources_takes_its_variable_s_name(self, repo):
        scan = read(
            repo,
            {
                "orch/defs.py": DBT
                + textwrap.dedent("""
                    from dagster import Definitions, with_resources
                    from dagster_dbt import load_assets_from_dbt_project

                    shop_assets = with_resources(
                        load_assets_from_dbt_project(project_dir=DBT_DIR, select="orders"), {}
                    )
                    defs = Definitions(assets=shop_assets)
                    """)
            },
        )
        assert named(scan["definitions"], "shop_assets")["nodes"] == [f"{S}.orders"]

    def test_a_job_by_group_over_a_legacy_loader_is_an_unknown(self, repo):
        """Its default groups changed from one dagster-dbt version to the next, so a selection by
        group that matches none of the reader's is not taken to select nothing."""
        scan = read(
            repo,
            {
                "orch/repository.py": REPOSITORY.replace(
                    "AssetSelection.all()", 'AssetSelection.groups("marts").upstream()'
                )
            },
        )
        job = named(scan["jobs"], "daily")
        assert (job["status"], job["category"]) == ("unknown", "not_literal")
        assert "groups of dbt_assets are not known here" in job["reason"]
        assert "groups_unknown" not in named(scan["definitions"], "dbt_assets")


# --- LP-9zx: a factory imported from another workspace file ----------------------------------


FACTORY = """
from dagster import AssetKey, asset


def build_export(*, model):
    @asset(name=f"{model}_export", deps=[AssetKey([model])])
    def export(context): ...

    return export


def build_spec_export(*, spec):
    @asset(name=spec.name, deps=[AssetKey(m) for m in spec.models])
    def spec_export(context): ...

    return spec_export
"""
CALLS = (
    DBT
    + """
from dagster import Definitions
from dagster_dbt import load_assets_from_dbt_project

from orch.factory import build_export, build_spec_export
from orch.specs import COUNTS, REVENUE

shop_assets = load_assets_from_dbt_project(project_dir=DBT_DIR, select="orders+")

orders_export = build_export(model="orders")
customer_export = build_export(model="customer_revenue")
counts_export = build_spec_export(spec=COUNTS)
revenue_export = build_spec_export(spec=REVENUE)

defs = Definitions(
    assets=[shop_assets, orders_export, customer_export, counts_export, revenue_export]
)
"""
)
SPECS = """
from orch.spec import ExportSpec

COUNTS = ExportSpec(name="counts_export", models=[["order_counts"]])
REVENUE = ExportSpec(name="revenue_export", models=[["customer_revenue"]])
"""


class TestFactoryInAnotherFile:
    @pytest.fixture
    def scan(self, repo) -> dict:
        return read(
            repo,
            {"orch/factory.py": FACTORY, "orch/specs.py": SPECS, "orch/defs.py": CALLS},
        )

    def test_each_call_is_an_asset_of_its_own_at_the_call(self, scan):
        by_line = {r["line"]: r for r in scan["assets"]}
        assert by_line[13]["name"] == "orders_export"
        assert by_line[13]["nodes"] == [f"{S}.orders"]
        assert by_line[14]["name"] == "customer_revenue_export"
        assert by_line[14]["nodes"] == [f"{S}.customer_revenue"]
        assert {r["file"] for r in scan["assets"]} == {"orch/defs.py"}

    def test_a_call_whose_values_cannot_be_read_is_an_unknown_at_the_call(self, scan):
        unknowns = sorted(
            (r["line"], r["category"]) for r in scan["assets"] if r["status"] == "unknown"
        )
        assert unknowns == [(15, "not_literal"), (16, "not_literal")]

    def test_what_a_call_makes_is_in_the_calling_file_s_code_location(self, scan):
        shop = named(scan["definitions"], "shop_assets")
        assert {r["location"] for r in scan["assets"]} == {shop["location"]}
        for record in scan["assets"]:
            assert not [n for n in record["notes"] if "no Definitions call" in n], record

    def test_the_factory_is_not_also_an_unknown_where_it_is_defined(self, scan):
        assert not [r for r in scan["assets"] if r["file"] == "orch/factory.py"]


# --- LP-ios: what is built on a dbt asset definition that cannot be read ---------------------


WRAPPED = """
from dagster import (
    AssetKey,
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DbtCliResource, build_dbt_asset_selection, build_dbt_specs


def dbt_layer(*, select, name):
    specs, checks = build_dbt_specs(select=select, manifest=MANIFEST, translator=None)
    return multi_asset(name=name, specs=specs, check_specs=checks)


@dbt_layer(select="orders", name="marts")
def marts(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


marts_job = define_asset_job("marts_job", selection=build_dbt_asset_selection([marts], "orders"))
everything = define_asset_job("everything", selection=AssetSelection.all())
nightly = ScheduleDefinition(job=marts_job, cron_schedule="0 2 * * *")


@asset(deps=[AssetKey(["orders"])])
def orders_export(): ...


@asset
def unrelated(): ...


defs = Definitions(
    assets=[marts, orders_export, unrelated], jobs=[marts_job, everything], schedules=[nightly]
)
"""


class TestOverAnUnreadDefinition:
    @pytest.fixture
    def scan(self, repo) -> dict:
        return read(repo, {"orch/defs.py": WRAPPED})

    def test_a_function_a_custom_decorator_makes_dbt_assets_of_is_a_named_unknown(self, scan):
        found = named(scan["unknowns"], "marts")
        assert (found["api"], found["line"], found["category"]) == ("dbt_layer", 19, "wrapper")
        assert "dbt_layer(), a decorator the reader does not read" in found["reason"]
        assert "dbt.cli" in found["reason"]
        # It is that, not also Python that runs dbt
        assert not [r for r in scan["located"] if r["name"] == "marts"]

    def test_a_job_and_its_schedule_over_it_are_unknowns(self, scan):
        job = named(scan["jobs"], "marts_job")
        assert (job["status"], job["line"]) == ("unknown", 23)
        assert "marts, a dbt asset definition that cannot be read" in job["reason"]
        schedule = named(scan["schedules"], "marts_job_schedule")
        assert schedule["status"] == "unknown" and "marts_job" in schedule["reason"]

    def test_a_job_of_every_asset_may_run_its_nodes(self, scan):
        job = named(scan["jobs"], "everything")
        assert job["status"] == "unknown"
        assert "marts" in job["reason"] and "cannot be read" in job["reason"]

    def test_an_asset_that_reads_a_key_no_asset_is_known_to_make_is_an_unknown(self, scan):
        found = named(scan["assets"], "orders_export")
        assert (found["status"], found["category"]) == ("unknown", "not_literal")
        assert "marts" in found["reason"] and "orders" in found["reason"]

    def test_an_asset_that_reads_nothing_is_still_left_out(self, scan):
        assert not [r for r in scan["assets"] if r["name"] == "unrelated"]

    def test_assets_a_loop_makes_whose_deps_cannot_be_read_are_one_unknown(self, repo):
        scan = read(
            repo,
            {
                "orch/defs.py": WRAPPED.replace(
                    "defs = Definitions(",
                    """
def make_export(table):
    @asset(name=f"export_{table}", deps=[AssetKey(["silver", table])])
    def export(): ...

    return export


EXPORTS = [make_export(t) for t in ["orders", "customers"]]

defs = Definitions(""",
                )
            },
        )
        found = [r for r in scan["assets"] if r["line"] == 39]
        assert [(r["status"], r["category"]) for r in found] == [("unknown", "loop_or_factory")]

    def test_a_decorated_function_that_does_not_run_dbt_is_nothing(self, repo):
        scan = read(
            repo,
            {
                "orch/defs.py": """
                    from dagster import define_asset_job

                    def layer(**kwargs):
                        return lambda fn: fn

                    @layer(name="raw")
                    def raw(context): ...

                    raw_job = define_asset_job("raw_job")
                    """
            },
        )
        assert scan["unknowns"] == scan["jobs"] == scan["located"] == []
