"""The shim loader runs the backend parsers without src/parsers/__init__.py
and without any server dependency."""

from __future__ import annotations

import json

import pytest

from tests.conftest import BACKEND_FIXTURE, FIX, run_python

FULL_RUN = f"""
import asyncio, io, contextlib, json, sys
from spike.loader import load_parsers, forbidden_loaded
m = load_parsers()
r = asyncio.run(m['dbt_parser'].DbtParser().parse({{
    'manifest_path': {str(FIX / 'jaffle-1.12' / 'manifest.json')!r},
    'catalog_path': {str(FIX / 'jaffle-1.12' / 'catalog.json')!r}}}))
with contextlib.redirect_stdout(io.StringIO()):
    m['sql_column_parser'].SQLColumnParser('duckdb').parse_select_statement('select a, b from t')
    m['sql_parser'].parse_sql('insert into x select * from y', dialect='duckdb')
print(json.dumps({{
    'forbidden': forbidden_loaded(),
    'capsules': len(r.capsules),
    'package_init_ran': any(k in sys.modules for k in (
        'src.parsers.airflow_parser', 'src.parsers.snowflake_parser', 'src.parsers.airflow_config')),
    'stub_path': list(sys.modules['src.parsers'].__path__),
    'loaded': sorted(k for k in sys.modules if k.startswith('src.')),
}}))
"""


def test_full_parse_pulls_in_no_forbidden_module():
    p = run_python(FULL_RUN)
    assert p.returncode == 0, p.stderr
    out = json.loads(p.stdout.strip().splitlines()[-1])
    assert out["forbidden"] == []  # httpx, snowflake, sqlalchemy, fastapi, pydantic
    assert out["capsules"] > 0
    assert out["package_init_ran"] is False
    assert out["stub_path"] == []
    assert out["loaded"] == ["src.parsers", "src.parsers.base", "src.parsers.dbt_config",
                             "src.parsers.dbt_parser", "src.parsers.sql_column_parser",
                             "src.parsers.sql_parser"]


def test_forbidden_list_is_the_design_list():
    from spike.loader import FORBIDDEN_MODULES
    assert set(FORBIDDEN_MODULES) == {"httpx", "snowflake", "sqlalchemy", "fastapi", "pydantic"}


def test_other_parser_files_cannot_be_imported_through_the_stub(mods):
    import importlib
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("src.parsers.airflow_parser")


def test_loader_is_idempotent(mods):
    from spike.loader import load_parsers
    again = load_parsers()
    assert again["dbt_parser"] is mods["dbt_parser"]


def test_as_is_parser_hard_requires_orjson():
    """Finding: dbt_parser.py has no stdlib json fallback."""
    code = """
import sys
class Block:
    def find_spec(self, name, path=None, target=None):
        if name == 'orjson':
            raise ModuleNotFoundError("No module named 'orjson'", name='orjson')
sys.meta_path.insert(0, Block())
from spike.loader import load_parsers
try:
    load_parsers()
    print('LOADED')
except ModuleNotFoundError as e:
    print('MISSING', e.name)
"""
    p = run_python(code)
    assert p.returncode == 0, p.stderr
    assert p.stdout.strip().endswith("MISSING orjson")


def test_stdlib_shim_gives_identical_result_without_orjson():
    """The labelled stdlib shim (models the Phase 1 fallback) parses identically."""
    code = f"""
import sys, asyncio, json
class Block:
    def find_spec(self, name, path=None, target=None):
        if name == 'orjson':
            raise ModuleNotFoundError("No module named 'orjson'")
sys.meta_path.insert(0, Block())
from spike.loader import load_parsers, json_backend_in_use
m = load_parsers(json_backend='stdlib')
r = asyncio.run(m['dbt_parser'].DbtParser().parse({{'manifest_path': {str(FIX / 'synth-24' / 'manifest.json')!r},
    'catalog_path': {str(FIX / 'synth-24' / 'catalog.json')!r}}}))
print(json.dumps({{'backend': json_backend_in_use(), 'summary': r.summary()}}))
"""
    p = run_python(code)
    assert p.returncode == 0, p.stderr
    out = json.loads(p.stdout.strip().splitlines()[-1])
    assert out["backend"] == "stdlib-shim"
    from tests.conftest import parse
    from spike.loader import load_parsers
    ref = parse(load_parsers(), FIX / "synth-24" / "manifest.json", FIX / "synth-24" / "catalog.json")
    assert out["summary"] == ref.summary()


def test_backend_fixture_still_parses(mods):
    from tests.conftest import parse
    r = parse(mods, BACKEND_FIXTURE / "manifest.json", BACKEND_FIXTURE / "catalog.json")
    assert r.summary()["capsules"] == 8 and not r.errors


def test_column_parser_prints_to_stdout_on_failure(mods, capsys):
    """Finding: SQLColumnParser reports failures with print(). On a stdio
    language server, stdout is the JSON-RPC channel, so this corrupts it."""
    out = mods["sql_column_parser"].SQLColumnParser("duckdb").parse_select_statement("select from where (")
    assert out == []
    assert "Failed to parse SQL" in capsys.readouterr().out
