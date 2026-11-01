"""In-process unit tests for the server's helpers.

The end-to-end tests run the server in a subprocess under -I, which coverage cannot follow, so
the helpers are also imported here directly. sqlglot and PyYAML come from bundled/libs.
"""

from __future__ import annotations

import sys

from conftest import LIBS, ROOT, WORKSPACE

sys.path[0:0] = [str(LIBS), str(ROOT / "bundled" / "tool")]

from lineagepilot_spike import server  # noqa: E402


def test_upstream_refs_finds_refs_and_sources_once():
    sql = "{{ ref('a') }} {{ref(\"b\")}} {{ ref('a') }} {{ source('raw', 'orders') }}"
    assert server.upstream_refs(sql) == ["a", "b", "raw.orders"]


def test_strip_jinja_keeps_table_names():
    sql = "{% set x = 1 %}select * from {{ ref('a') }} join {{ source('raw','t') }} {# c #}"
    assert server.strip_jinja(sql) == "select * from a join raw__t "


def test_table_count_counts_distinct_tables():
    assert server.table_count("select * from {{ ref('a') }} join {{ ref('a') }} x on 1=1") == 1


def test_table_count_returns_none_when_unparseable():
    assert server.table_count("select from where (") is None
    assert server.table_count("") is None


def test_project_name_reads_nearest_dbt_project():
    assert server.project_name(str(WORKSPACE / "models" / "orders.sql")) == "spike_shop"


def test_project_name_none_outside_a_project(tmp_path):
    (tmp_path / "x.sql").write_text("select 1")
    assert server.project_name(str(tmp_path / "x.sql")) is None


def test_project_name_ignores_non_mapping_yaml(tmp_path):
    (tmp_path / "dbt_project.yml").write_text("- just\n- a list\n")
    assert server.project_name(str(tmp_path / "m.sql")) is None


def test_project_name_skips_oversized_project_files(tmp_path, monkeypatch):
    (tmp_path / "dbt_project.yml").write_text("name: big\n")
    monkeypatch.setattr(server, "MAX_PROJECT_FILE_BYTES", 4)
    assert server.project_name(str(tmp_path / "m.sql")) is None


def test_lens_title_singular_and_plural():
    path = str(WORKSPACE / "models" / "x.sql")
    assert "1 upstream ref, 1 table |" in server.lens_title("select * from {{ ref('a') }}", path)
    assert "0 upstream refs, unparsed" in server.lens_title("select from", path)


def test_runtime_status_shape():
    status = server.runtime_status()
    assert status["protocolVersion"] == server.PROTOCOL_VERSION
    assert set(status["libs"]) == {"pygls", "sqlglot", "yaml"}
    assert status["libs"]["pygls"]["version"]
