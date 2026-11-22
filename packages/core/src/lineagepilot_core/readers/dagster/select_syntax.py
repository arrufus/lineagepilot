"""Dagster's asset selection strings, as far as D-e reads them (task 8.6).

dagster 1.13.29 parses a selection string with its grammar first, and falls back to an older
syntax. What is read here, and what each means, was checked against dagster itself
(``scripts/dagster_key_conformance.py``):

- ``key:<key>`` and ``group:<name>``. A key is written with ``/`` between its parts, quoted or
  not, and may hold ``*``, which matches anything. ``key:`` alone, or ``key:*``, is not accepted.
- Traversal around them: ``+key:a`` is everything upstream, ``2+key:a`` two levels, and ``key:a+``
  and ``key:a+2`` the same downstream.
- ``and``, ``or`` and ``not``, with ``not`` binding tightest and then ``and``, and parentheses.
- The older syntax, for a bare name: ``a`` is exactly that key; ``+a`` one level upstream, ``++a``
  two, ``*a`` all of them, and ``a+``, ``a++`` and ``a*`` the same downstream. ``*`` alone is
  every asset.

Anything else, such as ``tag:``, ``owner:`` or ``kind:``, is not read: :func:`parse` says why.
The result is a tree of plain JSON, resolved once the code location's assets are known
(``jobs``).
"""

from __future__ import annotations

import re
from typing import Any

MAX_SELECTION = 10_000
MAX_DEPTH = 100
_TOKEN = re.compile(r'\s*(\(|\)|(?:[^\s()"]|"[^"]*")+)')
_ATTRIBUTE = re.compile(
    r'^(?:(?P<up>\d*)\+)?(?P<attr>key|group):(?P<value>"[^"]*"|[^\s"+]+)(?:\+(?P<down>\d*))?$'
)
_LEGACY = re.compile(r"^(?P<up>\*|\++)?(?P<name>[A-Za-z0-9_\-./]+?)(?P<down>\*|\++)?$")
_OTHER_ATTRIBUTE = re.compile(r"^\d*\+?([a-z_]+):")
_KEYWORDS = ("and", "or", "not")


class NotRead(Exception):
    """A selection string the reader does not read, with why."""


def key_parts(text: str) -> list[str]:
    """A key written as a string, as Dagster splits one (``AssetKey.from_user_string``)."""
    return text.split("/")


def parse(text: str) -> dict[str, Any]:
    """The tree a selection string means. Raises :class:`NotRead` when it is not read."""
    if len(text) > MAX_SELECTION:
        raise NotRead("the selection string is too long")
    tokens = _tokens(text)
    if not tokens:
        raise NotRead("the selection string is empty")
    if tokens == ["*"]:
        return {"op": "all"}
    tree, rest = _or(tokens)
    if rest:
        raise NotRead(f"{text[:200]!r} is not a selection the reader reads")
    return tree


def _tokens(text: str) -> list[str]:
    found: list[str] = []
    position = 0
    while position < len(text):
        match = _TOKEN.match(text, position)
        if match is None:
            if text[position:].strip():
                raise NotRead(f"{text[:200]!r} is not a selection the reader reads")
            break
        found.append(match.group(1))
        position = match.end()
    return found


def _or(tokens: list[str]) -> tuple[dict[str, Any], list[str]]:
    left, rest = _and(tokens)
    parts = [left]
    while rest[:1] == ["or"]:
        right, rest = _and(rest[1:])
        parts.append(right)
    return (parts[0] if len(parts) == 1 else {"op": "or", "of": parts}), rest


def _and(tokens: list[str]) -> tuple[dict[str, Any], list[str]]:
    left, rest = _not(tokens)
    parts = [left]
    while rest[:1] == ["and"]:
        right, rest = _not(rest[1:])
        parts.append(right)
    return (parts[0] if len(parts) == 1 else {"op": "and", "of": parts}), rest


def _not(tokens: list[str]) -> tuple[dict[str, Any], list[str]]:
    if tokens[:1] == ["not"]:
        inner, rest = _not(tokens[1:])
        return {"op": "not", "of": inner}, rest
    return _primary(tokens)


def _primary(tokens: list[str]) -> tuple[dict[str, Any], list[str]]:
    if not tokens:
        raise NotRead("the selection string ends too soon")
    first, rest = tokens[0], tokens[1:]
    if first == "(":
        inner, rest = _or(rest)
        if rest[:1] != [")"]:
            raise NotRead("a parenthesis in the selection string is not closed")
        return inner, rest[1:]
    if first in (")", *_KEYWORDS):
        raise NotRead(f"{first!r} is out of place in the selection string")
    return _term(first), rest


def _depth(written: str | None, unlimited: bool) -> int | None:
    """``+`` alone is every level in the grammar's syntax; digits are that many."""
    if written is None:
        return 0
    if written == "":
        return None if unlimited else 1
    depth = int(written)
    if depth > MAX_DEPTH:
        return None
    return depth


def _traverse(tree: dict[str, Any], up: int | None, down: int | None) -> dict[str, Any]:
    """``tree``, with ``up`` levels upstream and ``down`` downstream added (0 for none, None for
    all), as Dagster joins the two: the union of each."""
    if up == 0 and down == 0:
        return tree
    parts = []
    if up != 0:
        parts.append({"op": "up", "of": tree, "depth": up, "self": True})
    if down != 0:
        parts.append({"op": "down", "of": tree, "depth": down, "self": True})
    return parts[0] if len(parts) == 1 else {"op": "or", "of": parts}


def _term(word: str) -> dict[str, Any]:
    found = _ATTRIBUTE.match(word)
    if found:
        value = found.group("value").strip('"')
        if not value or value == "*":
            raise NotRead(f"{word!r} is not a selection Dagster accepts")
        if found.group("attr") == "group":
            base: dict[str, Any] = {"op": "groups", "names": [value]}
        elif "*" in value:
            base = {"op": "key_pattern", "pattern": value}
        else:
            base = {"op": "keys", "keys": [key_parts(value)], "strict": False}
        up = _depth(found.group("up"), unlimited=True) if "+" in word.split(":", 1)[0] else 0
        down = _depth(found.group("down"), unlimited=True)
        return _traverse(base, up, down)
    other = _OTHER_ATTRIBUTE.match(word)
    if other:
        raise NotRead(f"selection by {other.group(1)}: is not read")
    legacy = _LEGACY.match(word)
    if legacy:
        name = legacy.group("name")
        base = {"op": "keys", "keys": [key_parts(name)], "strict": True}
        return _traverse(
            base, _legacy_depth(legacy.group("up")), _legacy_depth(legacy.group("down"))
        )
    raise NotRead(f"{word!r} is not a selection the reader reads")


def _legacy_depth(written: str | None) -> int | None:
    """The older syntax: each ``+`` one level, and ``*`` all of them."""
    if not written:
        return 0
    if written == "*":
        return None
    return min(len(written), MAX_DEPTH)
