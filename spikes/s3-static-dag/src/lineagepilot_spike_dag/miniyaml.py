"""A deliberately small YAML subset reader (stdlib only).

Supports what dbt ``dbt_project.yml`` and ``schema.yml`` files normally use: block mappings,
block sequences, flow lists/maps, quoted and plain scalars, comments and ``|``/``>`` blocks.
Anchors, tags and multi-document streams are not supported; :class:`YamlError` is raised and
the caller records the file as unparsed. Used only to build approximate stand-in manifests.
"""

from __future__ import annotations

import re
from typing import Any


class YamlError(Exception):
    pass


_KEY_RE = re.compile(r"""^(?P<key>"(?:[^"\\]|\\.)*"|'(?:[^']|'')*'|[^\s"'#\[\]{},][^:#]*?)\s*:(?:\s+(?P<val>.*)|\s*)$""")


def _strip_comment(line: str) -> str:
    out = []
    quote: str | None = None
    prev = " "
    for ch in line:
        if quote:
            out.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
            out.append(ch)
        elif ch == "#" and prev in (" ", "\t"):
            break
        else:
            out.append(ch)
        prev = ch
    return "".join(out).rstrip()


def _scalar(text: str) -> Any:
    t = text.strip()
    if t == "":
        return None
    if t[0] in ("[", "{"):
        val, rest = _flow(t, 0)
        if t[rest:].strip():
            raise YamlError(f"trailing text after flow value: {t!r}")
        return val
    if t[0] == '"':
        if not t.endswith('"') or len(t) < 2:
            raise YamlError(f"unterminated string {t!r}")
        return bytes(t[1:-1], "utf-8").decode("unicode_escape") if "\\" in t else t[1:-1]
    if t[0] == "'":
        if not t.endswith("'") or len(t) < 2:
            raise YamlError(f"unterminated string {t!r}")
        return t[1:-1].replace("''", "'")
    if t in ("&", "*", "!") or t[0] in ("&", "*", "!"):
        raise YamlError(f"anchors/tags not supported: {t!r}")
    low = t.lower()
    if low in ("true", "yes", "on"):
        return True
    if low in ("false", "no", "off"):
        return False
    if low in ("null", "~"):
        return None
    if re.fullmatch(r"[-+]?\d+", t):
        return int(t)
    if re.fullmatch(r"[-+]?\d*\.\d+(e[-+]?\d+)?", low):
        return float(t)
    return t


def _skip_ws(t: str, i: int) -> int:
    while i < len(t) and t[i] in " \t":
        i += 1
    return i


def _flow_value(t: str, i: int, stop: str) -> tuple[Any, int]:
    i = _skip_ws(t, i)
    if i >= len(t):
        raise YamlError("unterminated flow collection")
    if t[i] in "[{":
        return _flow(t, i)
    if t[i] in "\"'":
        q = t[i]
        j = i + 1
        while j < len(t):
            if t[j] == q:
                if q == "'" and j + 1 < len(t) and t[j + 1] == "'":
                    j += 2
                    continue
                break
            if q == '"' and t[j] == "\\":
                j += 1
            j += 1
        if j >= len(t):
            raise YamlError("unterminated quoted string in flow collection")
        return _scalar(t[i : j + 1]), j + 1
    j = i
    while j < len(t) and t[j] not in stop:
        j += 1
    return _scalar(t[i:j]), j


def _flow(t: str, i: int) -> tuple[Any, int]:
    """Parse a flow collection starting at t[i]; return (value, index after it)."""
    opener = t[i]
    closer = "]" if opener == "[" else "}"
    items: list[Any] = []
    mapping: dict[Any, Any] = {}
    i += 1
    while True:
        i = _skip_ws(t, i)
        if i >= len(t):
            raise YamlError("unterminated flow collection")
        if t[i] == closer:
            return (items if opener == "[" else mapping), i + 1
        if opener == "[":
            val, i = _flow_value(t, i, ",]")
            items.append(val)
        else:
            key, i = _flow_value(t, i, ",:}")
            i = _skip_ws(t, i)
            if i < len(t) and t[i] == ":":
                val, i = _flow_value(t, i + 1, ",}")
            else:
                val = None
            mapping[key] = val
        i = _skip_ws(t, i)
        if i < len(t) and t[i] == ",":
            i += 1
            continue
        if i < len(t) and t[i] == closer:
            return (items if opener == "[" else mapping), i + 1
        raise YamlError(f"bad flow collection near {t[i:i + 10]!r}")


class _Parser:
    def __init__(self, text: str):
        self.lines: list[tuple[int, str]] = []
        raw_lines = text.splitlines()
        self.raw = raw_lines
        for idx, raw in enumerate(raw_lines):
            if raw.strip() in ("---", "...") and not raw.startswith(" "):
                if self.lines:
                    break
                continue
            if "\t" in raw[: len(raw) - len(raw.lstrip())]:
                raise YamlError(f"tab indentation on line {idx + 1}")
            stripped = _strip_comment(raw)
            if not stripped.strip():
                self.lines.append((-1, str(idx)))  # blank marker keeps block scalars intact
                continue
            self.lines.append((len(stripped) - len(stripped.lstrip(" ")), stripped.strip()))
        self.pos = 0

    def _skip_blank(self) -> None:
        while self.pos < len(self.lines) and self.lines[self.pos][0] == -1:
            self.pos += 1

    def parse(self) -> Any:
        self._skip_blank()
        if self.pos >= len(self.lines):
            return None
        val = self._block(self.lines[self.pos][0])
        self._skip_blank()
        if self.pos < len(self.lines):
            raise YamlError(f"unexpected content: {self.lines[self.pos][1]!r}")
        return val

    def _block(self, indent: int) -> Any:
        self._skip_blank()
        _, text = self.lines[self.pos]
        if text == "-" or text.startswith("- "):
            return self._seq(indent)
        return self._map(indent)

    def _value_after_key(self, rest: str | None, indent: int) -> Any:
        if rest is not None and rest not in ("|", ">", "|-", ">-", "|+", ">+"):
            return _scalar(rest)
        if rest is not None:
            return self._block_scalar(indent, folded=rest.startswith(">"))
        self._skip_blank()
        if self.pos >= len(self.lines):
            return None
        nind, ntext = self.lines[self.pos]
        if nind > indent:
            return self._block(nind)
        if nind == indent and (ntext == "-" or ntext.startswith("- ")):
            return self._seq(indent)
        return None

    def _block_scalar(self, indent: int, folded: bool) -> str:
        parts = []
        while self.pos < len(self.lines):
            ind, text = self.lines[self.pos]
            if ind != -1 and ind <= indent:
                break
            parts.append("" if ind == -1 else text)
            self.pos += 1
        return (" " if folded else "\n").join(parts).strip("\n")

    def _map(self, indent: int) -> dict[Any, Any]:
        out: dict[Any, Any] = {}
        while True:
            self._skip_blank()
            if self.pos >= len(self.lines):
                return out
            ind, text = self.lines[self.pos]
            if ind < indent:
                return out
            if ind > indent:
                raise YamlError(f"bad indentation: {text!r}")
            if text == "-" or text.startswith("- "):
                return out
            m = _KEY_RE.match(text)
            if not m:
                raise YamlError(f"expected 'key: value', got {text!r}")
            key = _scalar(m.group("key"))
            self.pos += 1
            out[key] = self._value_after_key(m.group("val"), indent)

    def _seq(self, indent: int) -> list[Any]:
        out: list[Any] = []
        while True:
            self._skip_blank()
            if self.pos >= len(self.lines):
                return out
            ind, text = self.lines[self.pos]
            if ind != indent or not (text == "-" or text.startswith("- ")):
                return out
            rest = text[1:].strip()
            if not rest:
                self.pos += 1
                self._skip_blank()
                if self.pos < len(self.lines) and self.lines[self.pos][0] > indent:
                    out.append(self._block(self.lines[self.pos][0]))
                else:
                    out.append(None)
                continue
            item_indent = indent + (len(text) - len(text[1:].lstrip()))
            if _KEY_RE.match(rest) and not rest.startswith(("[", "{", '"', "'")) or (
                rest[0] in ('"', "'") and _KEY_RE.match(rest)
            ):
                # "- key: value" starts a mapping whose keys sit at item_indent
                self.lines[self.pos] = (item_indent, rest)
                out.append(self._map(item_indent))
            elif rest.startswith("- "):
                self.lines[self.pos] = (item_indent, rest)
                out.append(self._seq(item_indent))
            else:
                self.pos += 1
                out.append(_scalar(rest))


def loads(text: str) -> Any:
    return _Parser(text).parse()
