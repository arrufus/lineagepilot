"""The JSON loader (orjson or the standard library) and the parse result types."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from lineagepilot_core.parsers import _json
from lineagepilot_core.parsers.base import ParseError, ParseErrorSeverity, ParseResult


@pytest.fixture(params=["orjson", "stdlib"])
def loader(request, monkeypatch):
    if request.param == "stdlib":
        monkeypatch.setattr(_json, "orjson", None)
    else:
        pytest.importorskip("orjson")
    return _json


def test_loads(loader):
    assert loader.loads(b'{"a": [1, 2]}') == {"a": [1, 2]}
    assert loader.loads('{"a": null}') == {"a": None}


@pytest.mark.parametrize("bad", [b"{not json", b"\xff\xfe", b""])
def test_bad_input_raises_value_error(loader, bad):
    with pytest.raises(ValueError):
        loader.loads(bad)


def test_load_file(loader, tmp_path):
    path = tmp_path / "x.json"
    path.write_text(json.dumps({"k": "v"}))
    assert loader.load_file(path) == {"k": "v"}
    with pytest.raises(OSError):
        loader.load_file(tmp_path / "missing.json")


def test_stdlib_is_used_when_orjson_cannot_be_imported():
    """S2 finding 4f: the old parser could not even be imported without orjson."""
    code = (
        "import sys; sys.modules['orjson'] = None\n"  # makes `import orjson` fail
        "from lineagepilot_core.parsers import _json, DbtParser\n"
        "assert _json.orjson is None\n"
        "print(_json.loads(b'[1]'))"
    )
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert p.returncode == 0, p.stderr
    assert p.stdout.strip() == "[1]"


def test_parse_error_str():
    assert str(ParseError("bad", ParseErrorSeverity.ERROR, "x.json")) == "[ERROR] at x.json: bad"
    assert str(ParseError("meh")) == "[WARNING]: meh"


def test_parse_result_counts():
    result = ParseResult()
    assert not result.has_errors
    result.add_error("w1")
    result.add_error("e1", ParseErrorSeverity.ERROR, location="loc", context={"k": 1})
    result.add_error("w2")
    assert result.has_errors
    assert (result.warning_count, result.error_count) == (2, 1)
    assert result.errors[1].context == {"k": 1}
    assert result.summary()["warnings"] == 2 and result.summary()["errors"] == 1


def test_a_full_parse_is_identical_on_both_json_paths(monkeypatch):
    from dataclasses import asdict

    from support import project_config

    from lineagepilot_core.parsers import DbtParser

    pytest.importorskip("orjson")
    with_orjson = DbtParser().parse(project_config("synth-24"))
    monkeypatch.setattr(_json, "orjson", None)
    with_stdlib = DbtParser().parse(project_config("synth-24"))
    assert [asdict(n) for n in with_stdlib.nodes] == [asdict(n) for n in with_orjson.nodes]
    assert [asdict(c) for c in with_stdlib.columns] == [asdict(c) for c in with_orjson.columns]
    assert with_stdlib.edges == with_orjson.edges


@pytest.mark.parametrize("value", [{"a": "é"}, {"a": "lone \ud800 surrogate"}, [1, None, True]])
def test_dumps_round_trips_on_both_paths(loader, value):
    """orjson refuses lone surrogates both ways; both paths fall back to the stdlib."""
    assert loader.loads(loader.dumps(value)) == value


def test_loads_accepts_what_the_stdlib_accepts(loader):
    assert loader.loads(b'{"a": "\\ud800"}') == {"a": "\ud800"}


class TestSourceHash:
    def test_line_endings_and_surrounding_space_are_not_the_text(self):
        from lineagepilot_core.parsers.base import source_sha256

        same = source_sha256("select 1\nfrom t")
        assert source_sha256("select 1\r\nfrom t") == same
        assert source_sha256("\n select 1\nfrom t\n\n") == same
        assert source_sha256("select 1\r\nfrom t\r\n") == same

    def test_anything_else_is(self):
        from lineagepilot_core.parsers.base import source_sha256

        assert source_sha256("select 1\nfrom t") != source_sha256("select 2\nfrom t")
        assert source_sha256("select 1\nfrom t") != source_sha256("select 1 from t")
