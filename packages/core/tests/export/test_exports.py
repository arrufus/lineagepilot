"""Exports: one regression test per bug, escaping helpers, scope and determinism."""

from __future__ import annotations

import json
from xml.etree import ElementTree as ET

import pydot
import pytest
from support import project_config

from lineagepilot_core.analysis.pii import pii_index
from lineagepilot_core.export import (
    ExportFormat,
    export_graph,
    graph_view,
    pii_inventory_csv,
    pii_inventory_markdown,
    safe_filename,
)
from lineagepilot_core.export.escape import (
    csv_cell,
    cypher_string,
    dot_string,
    markdown_cell,
    mermaid_label,
    xml_text,
)
from lineagepilot_core.export.graph import ExportNode, GraphView
from lineagepilot_core.graph import build_dbt_graph
from lineagepilot_core.graph.column_lineage import add_column_lineage, compiled_sql
from lineagepilot_core.parsers import DbtParser

WIPE = 'x"}) RETURN 1; MATCH (n) DETACH DELETE n; MATCH (a {urn: "y'


def hostile(name: str, node_id: str = "model.p.x", layer: str | None = None) -> GraphView:
    return GraphView(nodes=[ExportNode(id=node_id, kind="model", name=name, layer=layer)])


def test_lp_auw_cypher_cannot_be_broken_out_of():
    output = export_graph(hostile(WIPE, node_id=WIPE), "cypher")
    constraint, merge = output.splitlines()  # two statements, nothing injected
    assert constraint.startswith("CREATE CONSTRAINT")
    literal = merge.removeprefix("MERGE (n:LineagePilot {id: ").split("}) SET")[0]
    assert json.loads(literal) == WIPE  # the payload is one string value
    assert cypher_string('a"b\\\n\u00e9') == '"a\\"b\\\\\\n\\u00e9"'


def test_lp_aml_csv_formulas_are_inert():
    for formula in ('=HYPERLINK("http://evil")', "+1", "-1+1", "@SUM(A1)", "\tx", "\rx"):
        assert csv_cell(formula) == "'" + formula
    assert csv_cell("customer_email") == "customer_email"
    assert csv_cell("\x00=1") == "'=1"  # NUL dropped first, then the formula neutralised


def test_lp_aml_markdown_has_no_live_html_or_links():
    cell = markdown_cell("<img src=x onerror=alert(1)> [click](http://evil) a|b\nc")
    assert "<" not in cell and ">" not in cell and "\n" not in cell
    assert "\\[click\\]\\(http://evil\\)" in cell
    assert "a\\|b c" in cell


def test_lp_l1s_graphml_cannot_inject_elements():
    payload = '</data></node><node id="evil"/>'
    root = ET.fromstring(export_graph(hostile(payload, layer=payload), "graphml"))
    nodes = root.findall("{http://graphml.graphdrawing.org/xmlns}graph/{*}node")
    assert len(nodes) == 1


def test_lp_l1s_dot_ids_are_generated_and_strings_escaped():
    payload = '"]; evil [label="x"]; n0 -> evil;'
    output = export_graph(hostile(payload, layer="a\\Nb"), "dot")
    (graph,) = pydot.graph_from_dot_data(output)
    (cluster,) = graph.get_subgraphs()
    assert [n.get_name() for n in cluster.get_nodes()] == ["n0"]  # no "evil" node
    assert graph.get_edges() == []
    assert dot_string('a"b\\c\nd') == '"a\\"b\\\\c\\nd"'
    assert 'label="a\\\\Nb"' in output  # no \N escString


def test_lp_l1s_mermaid_cannot_add_lines_or_directives():
    output = export_graph(hostile("a\nclick n0 call alert(1)\n%%{init: {}}%%"), "mermaid")
    assert "\nclick" not in output and "%%" not in output
    assert mermaid_label('a"b') == '"a#34;b"'


def test_lp_l1s_file_names():
    assert safe_filename('../../etc/passwd";x', "dot") == "etc_passwd_x.dot"
    assert safe_filename("Model: Orders (v2)", ".json") == "Model_Orders_v2.json"
    assert safe_filename("", "mmd") == "export.mmd"
    assert safe_filename("x" * 300, "dot") == "x" * 100 + ".dot"
    assert safe_filename("x", "") == "x"


def test_xml_text_drops_what_xml_cannot_carry():
    assert xml_text("a\x00b\x1fc￾d\r\ne") == "abcd\ne"


@pytest.fixture(scope="module")
def pilot():
    result = DbtParser().parse(project_config("pilot-1.12"))
    graph = build_dbt_graph(result)
    add_column_lineage(graph, compiled_sql(result))
    return graph


def test_graph_view_of_the_whole_project(pilot):
    view = graph_view(pilot)
    assert {n.kind for n in view.nodes} == {"model", "seed", "source"}
    assert {e.kind for e in view.edges} == {"depends_on"}
    assert len(view.edges) == 14


def test_graph_view_scope_columns_and_relations(pilot):
    scope = ["model.pilot_shop.customers", "model.pilot_shop.stg_customers"]
    view = graph_view(pilot, scope, columns=True, relations=True)
    kinds = {e.kind for e in view.edges}
    assert kinds == {"depends_on", "derives", "has_column", "materialises"}
    assert {n.kind for n in view.nodes} == {"model", "column", "relation"}
    derives = [e for e in view.edges if e.kind == "derives"]
    assert (
        "model.pilot_shop.stg_customers#customer_id",
        "model.pilot_shop.customers#customer_id",
    ) in {(e.upstream, e.downstream) for e in derives}
    with pytest.raises(KeyError):
        graph_view(pilot, ["model.pilot_shop.nope"])


@pytest.mark.parametrize("fmt", list(ExportFormat))
def test_every_format_is_deterministic(pilot, fmt):
    view = graph_view(pilot)
    first = export_graph(view, fmt)
    assert first == export_graph(graph_view(pilot), fmt.value)
    assert first.endswith("\n")
    assert fmt.extension


def test_json_export_shape(pilot):
    data = json.loads(export_graph(graph_view(pilot), ExportFormat.JSON))
    assert (data["format"], data["version"]) == ("lineagepilot-graph", 1)
    assert data["nodes"][0].keys() == {
        "id",
        "kind",
        "name",
        "layer",
        "domain",
        "owner",
        "file_path",
    }


def test_cypher_skips_unknown_kinds():
    view = GraphView(nodes=[ExportNode(id="x", kind="mystery", name="x")])
    assert export_graph(view, "cypher").count("MERGE") == 0


def test_pii_inventory_exports(write_json, manifest_of):
    manifest = manifest_of("pilot-1.12")
    staging = manifest["nodes"]["model.pilot_shop.stg_customers"]["columns"]
    staging["customer_name"] = {"name": "customer_name", "meta": {"pii": "name"}}
    graph = build_dbt_graph(DbtParser().parse({"manifest_path": str(write_json(manifest))}))
    index = pii_index(graph)
    csv_text = pii_inventory_csv(graph, index)
    assert csv_text.splitlines()[0] == (
        '"node","column","pii_types","basis","confidence","severity","masked","layer","origins"'
    )
    assert (
        '"model.pilot_shop.stg_customers","customer_name","name","tag","declared","high","no",'
        '"silver",""' in csv_text
    )
    markdown = pii_inventory_markdown(graph, index)
    assert markdown.startswith("# PII inventory\n")
    assert "not verified exposure" in markdown
    assert "| name |" in markdown
    # customers is downstream of the PII and its column lineage was never computed
    assert "model.pilot\\_shop.customers: column lineage not computed" in markdown


def test_an_empty_inventory():
    from lineagepilot_core.analysis.pii import PIIIndex
    from lineagepilot_core.graph import MetadataGraph

    markdown = pii_inventory_markdown(MetadataGraph(), PIIIndex())
    assert "0 columns in 0 nodes." in markdown and "|" not in markdown
