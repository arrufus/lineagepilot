"""The labelled optimised variant must give output identical to the as-is parser."""

from __future__ import annotations

import pytest

from spike.optimised import make_optimised_parser
from tests.conftest import BACKEND_FIXTURE, REAL_FIXTURES, parse


def _edges(r):
    return [(e.source_urn, e.target_urn, e.edge_type, e.transformation) for e in r.edges]


CASES = {**REAL_FIXTURES, "backend": (BACKEND_FIXTURE / "manifest.json", BACKEND_FIXTURE / "catalog.json")}


@pytest.mark.parametrize("name", sorted(CASES))
def test_identical_output(mods, name):
    m, c = CASES[name]
    as_is = parse(mods, m, c)
    opt = parse(mods, m, c, cls=make_optimised_parser(mods["dbt_parser"]))
    assert _edges(opt) == _edges(as_is)
    assert opt.summary() == as_is.summary()
    assert [(x.source_column_urn, x.target_column_urn) for x in opt.column_edges] == \
           [(x.source_column_urn, x.target_column_urn) for x in as_is.column_edges]


def test_column_lookup_identical_with_collisions(mods):
    """Both variants build the same column_urn_lookup, including on the URN collision."""
    m, c = REAL_FIXTURES["synth-24"]
    seen = {}

    def spy(label, cls):
        class Spy(cls):  # type: ignore[misc, valid-type]
            def _build_column_lineage(self, manifest, result, urn_lookup, column_urn_lookup):
                seen[label] = dict(column_urn_lookup)
                return super()._build_column_lineage(manifest, result, urn_lookup, column_urn_lookup)
        return Spy

    parse(mods, m, c, cls=spy("as-is", mods["dbt_parser"].DbtParser))
    parse(mods, m, c, cls=spy("opt", make_optimised_parser(mods["dbt_parser"])))
    assert seen["as-is"] == seen["opt"] and seen["opt"]
