"""Escaping for every export format (design section 10).

Every value in an export comes from the workspace, so any of them can be hostile: a dbt
model, tag or column name can hold quotes, newlines, markup or code. Each function
here makes a value inert in one format. No exporter interpolates a raw value.

Closes LP-auw (Cypher), LP-aml (CSV formula injection) and LP-l1s (GraphML, DOT,
Mermaid and file names).
"""

from __future__ import annotations

import json
import re

# Characters XML 1.0 cannot carry, even escaped
_XML_INVALID = re.compile("[^\u0009\u000a\u000d -퟿-�\U00010000-\U0010ffff]")

# Characters left as they are in Mermaid labels; everything else becomes an entity
_MERMAID_PLAIN = re.compile(r"[A-Za-z0-9 _.,:/()\-]")

_FORMULA_START = ("=", "+", "-", "@", "\t", "\r")

_MARKDOWN_SPECIAL = re.compile(r"([\\`*_{}\[\]()#+!|~>-])")


def xml_text(value: str) -> str:
    """Drop characters XML 1.0 cannot represent; ElementTree escapes the rest. Line breaks
    become ``\n``: XML parsers turn a carriage return in text into one anyway."""
    return _XML_INVALID.sub("", value).replace("\r\n", "\n").replace("\r", "\n")


def dot_string(value: str) -> str:
    """A double-quoted DOT string. Backslashes are escaped so none acts as an escString."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    escaped = escaped.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    return f'"{escaped}"'


def mermaid_label(value: str) -> str:
    """A double-quoted Mermaid label. Anything but plain text is an entity (``#34;``), so
    quotes, newlines, brackets, ``%%`` directives and ``click`` lines cannot appear."""
    return '"' + "".join(c if _MERMAID_PLAIN.match(c) else f"#{ord(c)};" for c in value) + '"'


def cypher_string(value: str) -> str:
    """A Cypher string literal. JSON's escapes (``\\"``, ``\\\\``, ``\\n``, ``\\uXXXX``)
    are all valid in Cypher, and ASCII-only output leaves nothing unescaped."""
    return json.dumps(value, ensure_ascii=True)


def csv_cell(value: str) -> str:
    """Neutralise a spreadsheet formula: a cell that starts with ``= + - @``, a tab or a
    carriage return gets a leading apostrophe. NUL characters are dropped: they mean
    nothing in a cell, and Python 3.10's csv module cannot write them. The csv module
    does the quoting."""
    value = value.replace("\x00", "")
    return "'" + value if value.startswith(_FORMULA_START) else value


def markdown_cell(value: str) -> str:
    """Text for one Markdown table cell: no raw HTML, no markup, no line breaks."""
    text = " ".join(value.split())
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return _MARKDOWN_SPECIAL.sub(r"\\\1", text)


def safe_filename(stem: str, extension: str, fallback: str = "export") -> str:
    """A file name of ``[A-Za-z0-9_-]`` characters, at most 100, plus the extension."""
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", stem).strip("_")[:100] or fallback
    suffix = re.sub(r"[^A-Za-z0-9]", "", extension)[:10]
    return f"{cleaned}.{suffix}" if suffix else cleaned
