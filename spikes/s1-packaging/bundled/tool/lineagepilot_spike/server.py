"""A minimal pygls server for spike S1.

It returns one CodeLens per dbt model file. Building the lens uses all three bundled runtime
libraries: pygls for LSP, PyYAML to read dbt_project.yml, and sqlglot to parse the model's SQL.
The custom `lineage/status` request reports where each library was loaded from, so tests can
prove the server ran from bundled/libs and not from the interpreter's site-packages.
"""

from __future__ import annotations

import os
import platform
import re
import sys
from pathlib import Path

import pygls
import sqlglot
import yaml
from lsprotocol import types
from pygls.lsp.server import LanguageServer
from sqlglot import exp

PROTOCOL_VERSION = 0
MAX_PROJECT_FILE_BYTES = 1_000_000
MAX_PARENT_LEVELS = 20

REF = re.compile(r"""\{\{\s*ref\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}""")
SOURCE = re.compile(r"""\{\{\s*source\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}""")
JINJA = re.compile(r"\{\{.*?\}\}|\{%.*?%\}|\{#.*?#\}", re.S)

server = LanguageServer("lineagepilot-spike", "0.0.1")


def upstream_refs(sql: str) -> list[str]:
    refs = REF.findall(sql)
    refs += [f"{source}.{table}" for source, table in SOURCE.findall(sql)]
    return sorted(set(refs))


def strip_jinja(sql: str) -> str:
    """Replace ref() and source() with plain table names and drop other Jinja."""
    sql = REF.sub(lambda m: m.group(1), sql)
    sql = SOURCE.sub(lambda m: f"{m.group(1)}__{m.group(2)}", sql)
    return JINJA.sub("", sql)


def table_count(sql: str) -> int | None:
    """Distinct tables sqlglot finds in the model, or None if it cannot parse it."""
    try:
        tree = sqlglot.parse_one(strip_jinja(sql))
    except sqlglot.errors.ParseError:
        return None
    if tree is None:
        return None
    return len({table.name for table in tree.find_all(exp.Table)})


def project_name(model_path: str) -> str | None:
    """Name from the nearest dbt_project.yml above the model, read with yaml.safe_load."""
    directory = Path(model_path).parent
    for _ in range(MAX_PARENT_LEVELS):
        candidate = directory / "dbt_project.yml"
        if candidate.is_file() and candidate.stat().st_size <= MAX_PROJECT_FILE_BYTES:
            data = yaml.safe_load(candidate.read_text(encoding="utf-8"))
            return data.get("name") if isinstance(data, dict) else None
        if directory.parent == directory:
            return None
        directory = directory.parent
    return None


def lens_title(sql: str, model_path: str) -> str:
    refs = upstream_refs(sql)
    tables = table_count(sql)
    parsed = "unparsed" if tables is None else f"{tables} table{'s' if tables != 1 else ''}"
    project = project_name(model_path) or "no dbt project"
    noun = "ref" if len(refs) == 1 else "refs"
    return (f"LineagePilot spike: {len(refs)} upstream {noun}, {parsed}"
            f" | {project} | Python {platform.python_version()}")


def runtime_status() -> dict:
    return {
        "protocolVersion": PROTOCOL_VERSION,
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "executable": sys.executable,
            "isolated": bool(sys.flags.isolated),
        },
        "platform": {"system": platform.system(), "machine": platform.machine()},
        "libs": {
            "pygls": {"version": pygls_version(), "file": pygls.__file__},
            "sqlglot": {"version": sqlglot.__version__, "file": sqlglot.__file__},
            "yaml": {"version": yaml.__version__, "file": yaml.__file__,
                     "withLibyaml": bool(getattr(yaml, "__with_libyaml__", False))},
        },
        "pid": os.getpid(),
    }


def pygls_version() -> str:
    from importlib.metadata import version

    return version("pygls")


@server.feature(types.TEXT_DOCUMENT_CODE_LENS)
def code_lens(ls: LanguageServer, params: types.CodeLensParams) -> list[types.CodeLens]:
    document = ls.workspace.get_text_document(params.text_document.uri)
    if not document.path.endswith(".sql"):
        return []
    start = types.Position(line=0, character=0)
    return [types.CodeLens(
        range=types.Range(start=start, end=start),
        command=types.Command(title=lens_title(document.source, document.path),
                              command="lineagePilot.showStatus"),
    )]


@server.feature("lineage/status")
def status(ls: LanguageServer, params: object) -> dict:
    return runtime_status()


def main() -> None:
    server.start_io()
