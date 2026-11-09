"""YAML from the workspace, which is untrusted (design section 10).

``yaml.safe_load`` builds nothing but plain data, but two things still get through it (audit
LP-SEC-05):

- An alias is a reference, so a few hundred bytes of nested aliases (a "billion laughs") load as a
  small structure that is enormous to walk or print: 411 bytes became a 312 MB string when one
  value was put in a message. Here a document whose aliases would expand past a budget is refused.
- The composer recurses once per level of nesting, so a deeply nested document raises
  ``RecursionError``, which callers did not expect. Here it is a ``YAMLError``, which they do.

``shown`` is for putting a value from such a file in a message: never more than a short line.
"""

from __future__ import annotations

import reprlib
from typing import Any

import yaml

# Nodes, counting every use of an alias in full. A 2 MB file with no aliases has fewer.
MAX_EXPANDED_NODES = 1_000_000
MAX_SHOWN = 120


class YAMLTooLarge(yaml.YAMLError):
    """The document's aliases expand past the budget, or refer to themselves."""


class YAMLTooDeep(yaml.YAMLError):
    """The document is nested deeper than the parser can follow."""


def compose(text: str | bytes, max_nodes: int = MAX_EXPANDED_NODES) -> yaml.Node | None:
    """The node tree of a one-document YAML text, like ``yaml.compose`` with ``SafeLoader``."""
    loader = yaml.SafeLoader(text)
    try:
        node = loader.get_single_node()
    except RecursionError:
        raise YAMLTooDeep("the YAML is nested too deeply") from None
    finally:
        loader.dispose()
    if node is not None:
        _check_expansion(node, max_nodes)
    return node


def load(text: str | bytes, max_nodes: int = MAX_EXPANDED_NODES) -> Any:
    """Like ``yaml.safe_load``, within the budget."""
    node = compose(text, max_nodes)
    if node is None:
        return None
    loader = yaml.SafeLoader("")
    try:
        return loader.construct_document(node)
    except RecursionError:
        raise YAMLTooDeep("the YAML is nested too deeply") from None
    finally:
        loader.dispose()


def _children(node: yaml.Node) -> list[yaml.Node]:
    if isinstance(node, yaml.MappingNode):
        return [n for pair in node.value for n in pair]
    if isinstance(node, yaml.SequenceNode):
        return list(node.value)
    return []


def _check_expansion(root: yaml.Node, max_nodes: int) -> None:
    """Count the nodes a full walk of the document would visit, without making that walk: an
    alias is the same node object each time it is used, so each node's size is computed once."""
    size: dict[int, int] = {}
    open_: set[int] = set()  # being counted: met again below itself, it is a cycle
    stack: list[tuple[yaml.Node, bool]] = [(root, False)]
    while stack:
        node, finished = stack.pop()
        key = id(node)
        if finished:
            open_.discard(key)
            total = 1 + sum(size[id(c)] for c in _children(node))
            if total > max_nodes:
                raise YAMLTooLarge(f"the YAML's aliases expand past {max_nodes} values")
            size[key] = total
            continue
        if key in size:
            continue
        if key in open_:
            raise YAMLTooLarge("the YAML has an alias inside itself")
        open_.add(key)
        stack.append((node, True))
        stack.extend((c, False) for c in _children(node) if id(c) not in size)


_REPR = reprlib.Repr()
_REPR.maxlevel = 2
_REPR.maxstring = _REPR.maxother = 80
_REPR.maxlist = _REPR.maxtuple = _REPR.maxdict = _REPR.maxset = 5


def shown(value: Any) -> str:
    """``repr(value)`` cut short, for a message about a value from an untrusted file."""
    text = _REPR.repr(value)
    return text if len(text) <= MAX_SHOWN else text[: MAX_SHOWN - 1] + "…"
