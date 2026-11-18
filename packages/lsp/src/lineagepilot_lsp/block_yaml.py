"""A quick reader for the plain block YAML that dbt's properties files are written in (task 7.10).

Finding where each ``name:`` sits (``locate.yaml_entries``) needs no more than the file's keys,
sequences and scalars, with their positions. PyYAML's composer, which is pure Python here,
reads about 1.5 MB a second: 1.3 s for the YAML of a semantic layer of 1,200 semantic models,
and a CodeLens request reads its file each time. This reads the same files a line at a time.

It reads a subset of YAML: block mappings and sequences, an indentless sequence under a key,
plain and quoted scalars (over several lines too, as tools wrap them), flow collections on one
line, comments, and block scalars, which it skips. Anything else raises ``NotPlain``, and the
caller composes the file with PyYAML instead, so the two always agree: an anchor, an alias or a
tag; a quoted key; a flow collection over several lines; a tab in an indent; a directive, or more
than one document. A scalar whose value would take YAML's escapes or folding to work out (over
several lines, or with a backslash escape) has no value here (``Scalar.value`` is None): where a
value matters, as a name does, the caller steps aside for PyYAML too.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# An unquoted key and the separator after it: "name: x", "columns:"
_KEY = re.compile(r"([A-Za-z0-9_][A-Za-z0-9_.\-/ ]*?)[ ]*:(?: +|$)")
_BLOCK_HEADER = re.compile(r"[|>][+-]?(?:[ ]+#.*)?$")
_RESERVED = frozenset("&*!%@`?:,")


class NotPlain(ValueError):
    """The text uses YAML this reader does not read: compose it instead."""


@dataclass(frozen=True, slots=True)
class Scalar:
    value: str | None  # None when only YAML's escapes or folding would give it
    line: int
    start: int
    end: int


class Mapping(list):
    """``(key, value)`` pairs, in order: a key may repeat, as PyYAML's node tree keeps it."""


class Sequence(list):
    pass


class _Other:
    """A value that is no scalar this reader needs: a flow collection, or a block scalar."""


OTHER = _Other()


def read(text: str) -> Mapping | None:
    """The top-level mapping of ``text``, None if it is empty, or ``NotPlain``."""
    reader = _Reader(text)
    if not reader.tokens:
        return None
    if reader.tokens[0][1] != 0:
        raise NotPlain("the top level is indented")
    found, end = reader.mapping(0, 0)
    if end != len(reader.tokens):
        raise NotPlain(f"line {reader.tokens[end][0] + 1} is not part of the top level")
    return found


def _is_dash(content: str) -> bool:
    return content == "-" or content.startswith("- ")


class _Reader:
    def __init__(self, text: str) -> None:
        self.raw = [line[:-1] if line.endswith("\r") else line for line in text.split("\n")]
        # (line, indent, content) for each line with something on it but a comment
        self.tokens: list[tuple[int, int, str]] = []
        for number, line in enumerate(self.raw):
            content = line.lstrip(" ")
            if not content or content.startswith("#"):
                continue
            if content.startswith("\t"):
                raise NotPlain(f"a tab indents line {number + 1}")
            indent = len(line) - len(content)
            if indent == 0 and content.startswith(("---", "...", "%")):
                if content.rstrip() == "---" and not self.tokens:
                    continue  # the one document's start
                raise NotPlain(f"line {number + 1} starts or ends a document")
            self.tokens.append((number, indent, content.rstrip()))

    def node(self, i: int) -> tuple[Mapping | Sequence, int]:
        _, indent, content = self.tokens[i]
        if _is_dash(content):
            return self.sequence(i, indent)
        if _KEY.match(content):
            return self.mapping(i, indent)
        raise NotPlain(f"line {self.tokens[i][0] + 1} is neither a key nor an item")

    def mapping(self, i: int, indent: int) -> tuple[Mapping, int]:
        found, tokens = Mapping(), self.tokens
        while i < len(tokens):
            line, at, content = tokens[i]
            if at < indent:
                break
            if at > indent:
                raise NotPlain(f"line {line + 1} is deeper than its mapping")
            key = _KEY.match(content)
            if key is None:
                raise NotPlain(f"line {line + 1} is not a key")
            value, i = self.value(content[key.end() :], i, at, at + key.end())
            if value is None:
                if i < len(tokens) and tokens[i][1] > at:
                    value, i = self.node(i)
                elif i < len(tokens) and tokens[i][1] == at and _is_dash(tokens[i][2]):
                    value, i = self.sequence(i, at)  # an indentless sequence
            found.append((key.group(1).rstrip(), value))
        return found, i

    def sequence(self, i: int, indent: int) -> tuple[Sequence, int]:
        found, tokens = Sequence(), self.tokens
        while i < len(tokens):
            line, at, content = tokens[i]
            if at != indent or not _is_dash(content):
                break
            after = content[1:]
            item = after.lstrip(" ")
            column = at + 1 + len(after) - len(item)
            if not item or _is_dash(item):
                raise NotPlain(f"line {line + 1} starts an item this reader does not read")
            if _KEY.match(item):
                # An item that is a mapping: its keys sit where its first one does
                tokens[i] = (line, column, item)
                value, i = self.mapping(i, column)
            else:
                value, i = self.value(item, i, at, column)
            found.append(value)
        if i < len(tokens) and tokens[i][1] > indent:
            raise NotPlain(f"line {tokens[i][0] + 1} is deeper than its sequence")
        return found, i

    # Values

    def value(self, rest: str, i: int, parent: int, column: int) -> tuple[object, int]:
        """The value that starts on token ``i`` after its key or dash, at ``column``, under a
        node at indent ``parent``: a scalar, a flow collection, a skipped block scalar
        (``OTHER``), or None for nothing; and the next token to read."""
        line = self.tokens[i][0]
        if not rest or rest.startswith("#"):
            return None, i + 1
        first = rest[0]
        if first in "|>":
            if not _BLOCK_HEADER.match(rest):
                raise NotPlain(
                    f"line {line + 1} has a block scalar header this reader does not read"
                )
            return OTHER, self._past(i + 1, lambda t: t[1] > parent)
        if first in _RESERVED or (first == "-" and rest[1:2] in ("", " ")):
            raise NotPlain(f"line {line + 1} has an anchor, an alias, a tag or a reserved mark")
        if first in "\"'":
            return self._quoted(rest, i, parent, column)
        if first in "[{":
            _flow(rest, line)
            return OTHER, self._deeper(i + 1, parent)
        return self._plain(rest, i, parent, column)

    def _quoted(self, rest: str, i: int, parent: int, column: int) -> tuple[Scalar, int]:
        line, quote = self.tokens[i][0], rest[0]
        close = _closing(rest, 1, quote)
        if close is not None:
            inner = rest[1:close]
            if quote == "'":
                value: str | None = inner.replace("''", "'")
            else:
                value = None if "\\" in inner else inner
            _nothing_after(rest[close + 1 :], line)
            return Scalar(value, line, column, column + close + 1), self._deeper(i + 1, parent)
        # Over several lines, as tools wrap long strings: found in the raw lines, so a line
        # inside it that looks like a comment cannot end it
        for number in range(line + 1, len(self.raw)):
            text = self.raw[number]
            if text.strip() and len(text) - len(text.lstrip(" ")) <= parent:
                raise NotPlain(f"line {number + 1} is inside a quoted scalar, and not indented")
            close = _closing(text, 0, quote)
            if close is not None:
                _nothing_after(text[close + 1 :], number)
                end = self._past(i + 1, lambda t, last=number: t[0] <= last)
                return Scalar(None, line, column, close + 1), self._deeper(end, parent)
        raise NotPlain(f"line {line + 1} opens a quoted scalar that does not close")

    def _plain(self, rest: str, i: int, parent: int, column: int) -> tuple[Scalar, int]:
        line = self.tokens[i][0]
        cut = rest.find(" #")
        value = (rest[:cut] if cut >= 0 else rest).rstrip()
        if ": " in value or value.endswith(":") or "\t" in value:
            raise NotPlain(f"line {line + 1} has a plain scalar this reader does not read")
        end = self._past(i + 1, lambda t: t[1] > parent)
        if end == i + 1:
            return Scalar(value, line, column, column + len(value)), end
        # Wrapped onto the lines below: no comment may end it, and no line may be a mapping
        if cut >= 0:
            raise NotPlain(f"line {line + 1} has a comment inside a plain scalar")
        last = self.tokens[end - 1][0]
        for number in range(line + 1, last + 1):
            text = self.raw[number].strip()
            if text.startswith("#") or ": " in text or " #" in text or text.endswith(":"):
                raise NotPlain(f"line {number + 1} cannot continue a plain scalar")
        return Scalar(None, line, column, len(self.raw[last].rstrip())), end

    def _past(self, i: int, inside) -> int:
        while i < len(self.tokens) and inside(self.tokens[i]):
            i += 1
        return i

    def _deeper(self, i: int, parent: int) -> int:
        """After a value that ends on its own line, nothing may sit deeper."""
        if i < len(self.tokens) and self.tokens[i][1] > parent:
            raise NotPlain(f"line {self.tokens[i][0] + 1} continues a value")
        return i


def _closing(text: str, start: int, quote: str) -> int | None:
    """Where the quoted scalar that is open at ``start`` closes in ``text``, or None."""
    at = start
    while True:
        at = text.find(quote, at)
        if at < 0:
            return None
        if quote == "'":
            if text[at + 1 : at + 2] == "'":  # a quote, escaped by doubling
                at += 2
                continue
            return at
        backslashes = len(text[:at]) - len(text[:at].rstrip("\\"))
        if backslashes % 2 == 0:
            return at
        at += 1


def _nothing_after(tail: str, line: int) -> None:
    tail = tail.strip()
    if tail and not tail.startswith("#"):
        raise NotPlain(f"line {line + 1} has text after a quoted scalar")


def _flow(rest: str, line: int) -> None:
    """A flow collection, whole on its line, with nothing in it this reader must parse."""
    code = rest.split(" #", 1)[0].rstrip()
    depth = 0
    for at, char in enumerate(code):
        if char in "\"'#&*!":
            raise NotPlain(f"line {line + 1} has a flow collection this reader does not read")
        depth += char in "[{"
        depth -= char in "]}"
        if depth == 0 and at != len(code) - 1:
            raise NotPlain(f"line {line + 1} has text after a flow collection")
        if depth < 0:
            raise NotPlain(f"line {line + 1} closes a flow collection it did not open")
    if depth:
        raise NotPlain(f"line {line + 1} leaves a flow collection open")
