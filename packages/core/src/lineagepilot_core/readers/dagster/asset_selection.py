"""``AssetSelection`` in Python, as far as task 8.6 reads it, made into the trees that
``select_syntax`` makes of a string.

Read, as dagster 1.13.29 and dagster-dbt 0.29.29 have them:

- ``AssetSelection.all()``, ``.keys(…)`` and ``.assets(…)`` (keys, ``AssetKey(…)`` or asset
  definitions), and ``.groups(…)``;
- ``.upstream(depth=None, include_self=True)`` and ``.downstream(…)``;
- ``|``, ``&`` and ``-``;
- ``build_dbt_asset_selection([definition], dbt_select=…, dbt_exclude=…, dbt_selector=…)``: what
  the definition selects, and of that what the dbt selection selects;
- a job's ``selection`` given as a string, a list of strings, of keys or of definitions, or nothing,
  which is every asset.

Anything else is an ``Unknown`` that says what it is, and the job is an unknown at its line.
"""

from __future__ import annotations

import ast
from typing import Any

from lineagepilot_core.readers.common.values import CallV, Const, ListV, Unknown, Val, as_str
from lineagepilot_core.readers.dagster.select_syntax import NotRead, key_parts, parse
from lineagepilot_core.readers.dagster.selection import DEFAULT_SELECT
from lineagepilot_core.readers.dagster.values import AssetsV, ModulesV, SelectionV

ALL: dict[str, Any] = {"op": "all"}
# A tree this deep is not one anybody wrote
MAX_TREE = 50


def selection(tree: dict[str, Any]) -> SelectionV:
    return SelectionV("dagster.AssetSelection", [], {}, tree=tree)


def _last(func: str) -> str:
    return func.rsplit(".", 1)[-1]


def key_of(value: Val) -> list[str] | None:
    """An asset key as ``AssetSelection.keys`` takes one: a string written with ``/``, an
    ``AssetKey(…)``, or a list of its parts."""
    text = as_str(value)
    if text is not None:
        return key_parts(text)
    if isinstance(value, ListV):
        parts = [as_str(item) for item in value.items]
        return parts if parts and all(p is not None for p in parts) else None  # type: ignore[return-value]
    if isinstance(value, CallV) and value.func.startswith("dagster"):
        if _last(value.func) == "AssetKey" and value.args:
            given = value.args[0]
            text = as_str(given)
            return [text] if text is not None else key_of(given)
        if value.func.endswith("AssetKey.from_user_string") and value.args:
            text = as_str(value.args[0])
            return key_parts(text) if text is not None else None
    return None


def _definitions(value: Val) -> list[str] | None:
    """The keys of the asset definitions in a value: one, or a list of them, as the legacy loaders
    return."""
    if isinstance(value, AssetsV):
        return [value.key]
    if isinstance(value, ListV) and value.items:
        found: list[str] = []
        for item in value.items:
            inner = _definitions(item)
            if inner is None:
                return None
            found += inner
        return found
    return None


def _assets(items: list[Val], line: int) -> Val:
    """``AssetSelection.assets`` and ``.keys``: definitions, or keys."""
    keys: list[list[str]] = []
    definitions: list[str] = []
    for item in items:
        found = _definitions(item)
        if found is not None:
            definitions += found
            continue
        key = key_of(item)
        if key is None:
            return Unknown(f"an asset selected is not known here: {_why(item)}", line=line)
        keys.append(key)
    parts = []
    if keys:
        parts.append({"op": "keys", "keys": keys, "strict": True})
    if definitions:
        parts.append({"op": "assets", "definitions": definitions})
    if not parts:
        return Unknown("an asset selection of nothing", line=line)
    return selection(parts[0] if len(parts) == 1 else {"op": "or", "of": parts})


def _why(value: Val | None) -> str:
    if isinstance(value, Unknown):
        return value.reason
    if isinstance(value, CallV):
        return f"it is what {value.func or 'a call'}() returns"
    return f"a {type(value).__name__} value"


def selection_of(value: Val | None, line: int) -> Val:
    """A job's ``selection``, as Dagster coerces one (``AssetSelection.from_coercible``)."""
    if value is None or (isinstance(value, Const) and value.value is None):
        return selection(ALL)
    if isinstance(value, SelectionV):
        return value
    text = as_str(value)
    if text is not None:
        return _parsed(text, line)
    if isinstance(value, ListV) and value.items:
        texts = [as_str(item) for item in value.items]
        if all(t is not None for t in texts):
            parts = [_parsed(t, line) for t in texts]  # type: ignore[arg-type]
            bad = next((p for p in parts if isinstance(p, Unknown)), None)
            if bad is not None:
                return bad
            trees = [p.tree for p in parts]  # type: ignore[attr-defined]
            return selection(trees[0] if len(trees) == 1 else {"op": "or", "of": trees})
        return _assets(value.items, line)
    if isinstance(value, AssetsV):
        return _assets([value], line)
    if isinstance(value, ModulesV):
        return Unknown("a selection of modules' assets is not read", line=line)
    return Unknown(f"the selection is not one the reader reads: {_why(value)}", line=line)


def _parsed(text: str, line: int) -> Val:
    try:
        return selection(parse(text))
    except NotRead as exc:
        return Unknown(str(exc), line=line)


def constructor(meth: str, args: list[Val], kwargs: dict[str, Val], line: int) -> Val:
    """``AssetSelection.<meth>(…)``."""
    if meth == "all":
        return selection(ALL)
    if meth in ("keys", "assets"):
        return _assets(args, line)
    if meth == "groups":
        names = [as_str(arg) for arg in args]
        if not names or any(n is None for n in names):
            return Unknown("AssetSelection.groups() of names not known here", line=line)
        return selection({"op": "groups", "names": names})
    return Unknown(f"AssetSelection.{meth}() is not read", line=line)


def method(base: SelectionV, meth: str, args: list[Val], kwargs: dict[str, Val], line: int) -> Val:
    """``.upstream(…)`` and ``.downstream(…)`` of a selection."""
    if meth not in ("upstream", "downstream"):
        return Unknown(f".{meth}() of an asset selection is not read", line=line)
    depth_v = args[0] if args else kwargs.get("depth")
    depth = None
    if depth_v is not None and not (isinstance(depth_v, Const) and depth_v.value is None):
        if not (isinstance(depth_v, Const) and type(depth_v.value) is int and depth_v.value >= 0):
            return Unknown(f".{meth}() to a depth not known here", line=line)
        depth = depth_v.value
    self_v = args[1] if len(args) > 1 else kwargs.get("include_self")
    include = True
    if self_v is not None:
        if not (isinstance(self_v, Const) and isinstance(self_v.value, bool)):
            return Unknown(f".{meth}() with include_self not known here", line=line)
        include = self_v.value
    if _height(base.tree) >= MAX_TREE:
        return Unknown("the asset selection is too deep to read", line=line)
    op = "up" if meth == "upstream" else "down"
    return selection({"op": op, "of": base.tree, "depth": depth, "self": include})


def operator(op: ast.operator, left: Val, right: Val, line: int) -> Val | None:
    """``|``, ``&`` and ``-`` between selections; None when neither side is one."""
    if not isinstance(left, SelectionV) and not isinstance(right, SelectionV):
        return None
    if not (isinstance(left, SelectionV) and isinstance(right, SelectionV)):
        other = right if isinstance(left, SelectionV) else left
        return Unknown(f"an asset selection combined with {_why(other)}", line=line)
    if max(_height(left.tree), _height(right.tree)) >= MAX_TREE:
        return Unknown("the asset selection is too deep to read", line=line)
    names = {ast.BitOr: "or", ast.BitAnd: "and", ast.Sub: "sub"}
    name = names.get(type(op))
    if name is None:
        return Unknown(f"{type(op).__name__} of asset selections is not read", line=line)
    return selection({"op": name, "of": [left.tree, right.tree]})


def dbt_selection(args: list[Val], kwargs: dict[str, Val], line: int) -> Val:
    """``build_dbt_asset_selection([definition], dbt_select=…, …)``: one definition, as
    dagster-dbt requires, and a dbt selection over its manifest."""
    given = args[0] if args else kwargs.get("dbt_assets")
    definitions = _definitions(given) if given is not None else None
    if not definitions or len(definitions) != 1:
        return Unknown("build_dbt_asset_selection() of definitions not known here", line=line)
    texts: dict[str, str] = {}
    names = ("dbt_select", "dbt_exclude", "dbt_selector")
    for position, (name, default) in enumerate(zip(names, (DEFAULT_SELECT, "", ""), strict=True)):
        value = args[position + 1] if len(args) > position + 1 else kwargs.get(name)
        if value is None or (isinstance(value, Const) and value.value is None):
            texts[name] = default
            continue
        text = as_str(value)
        if text is None:
            return Unknown(f"{name} is not known here: {_why(value)}", line=line)
        texts[name] = text.strip()
    return selection(
        {
            "op": "dbt",
            "definition": definitions[0],
            "select": texts["dbt_select"],
            "exclude": texts["dbt_exclude"],
            "selector": texts["dbt_selector"],
        }
    )


def _height(tree: dict[str, Any]) -> int:
    of = tree.get("of")
    if isinstance(of, dict):
        return 1 + _height(of)
    if isinstance(of, list) and of:
        return 1 + max(_height(t) for t in of)
    return 0


def uses_keys(tree: dict[str, Any]) -> bool:
    """Whether a selection picks assets by key or group, which a custom translator hides."""
    if tree.get("op") in ("keys", "key_pattern", "groups"):
        return True
    of = tree.get("of")
    if isinstance(of, dict):
        return uses_keys(of)
    return isinstance(of, list) and any(uses_keys(t) for t in of)
