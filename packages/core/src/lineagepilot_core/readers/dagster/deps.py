"""What a non-dbt asset depends on, as Dagster 1.13.29 reads it (task 8.7).

These forms were checked against dagster itself (``scripts/dagster_key_conformance.py``):

- ``@asset(deps=[…])`` and ``AssetSpec(…, deps=[…])``: a string is a key of one part (it is not
  split on ``/``), an ``AssetKey(…)`` is its parts, ``AssetDep(…)`` is what it wraps, and an asset
  definition, such as a ``@dbt_assets`` function, is every asset it makes;
- ``ins={"x": AssetIn(key=…, key_prefix=…)}``: a key as above, or the parameter's name under the
  prefix; and every other parameter of an ``@asset`` function, except ``context`` and resources, is
  an input keyed by its name;
- ``get_asset_key_for_model([dbt_assets], "orders")``: the key of the dbt model, seed or snapshot of
  that name.

Each dependency is a small dict: ``{"key": [...]}``, ``{"definition": <key>}`` or
``{"model": <name>, "definition": <key>}``. What cannot be read is a reason instead.
"""

from __future__ import annotations

from typing import Any

from lineagepilot_core.readers.common.values import CallV, Const, ListV, Unknown, Val, as_str
from lineagepilot_core.readers.dagster.select_syntax import key_parts
from lineagepilot_core.readers.dagster.values import AssetsV, SpecV

# A list of dependencies this long is not one anybody wrote
MAX_DEPS = 1_000


def _last(func: str) -> str:
    return func.rsplit(".", 1)[-1]


def asset_key(value: Val | None) -> list[str] | None:
    """A key as Dagster coerces one (``AssetKey.from_coercible``): a string is one part."""
    text = as_str(value)
    if text is not None:
        return [text]
    if isinstance(value, ListV):
        parts = [as_str(item) for item in value.items]
        return parts if parts and all(p is not None for p in parts) else None  # type: ignore[return-value]
    if isinstance(value, CallV) and value.func.startswith("dagster"):
        if _last(value.func) == "AssetKey" and value.args:
            return asset_key(value.args[0])
        if value.func.endswith("AssetKey.from_user_string") and value.args:
            text = as_str(value.args[0])
            return key_parts(text) if text is not None else None
    return None


def _why(value: Val | None) -> str:
    if isinstance(value, Unknown):
        return value.reason
    if isinstance(value, CallV):
        return f"it is what {value.func or 'a call'}() returns"
    return f"a {type(value).__name__} value"


def dependency(value: Val) -> list[dict[str, Any]] | str:
    """One item of ``deps``: what it names, or why it cannot be read."""
    if isinstance(value, AssetsV):
        return [{"definition": value.key}]
    if isinstance(value, SpecV):
        return [{"key": list(value.asset_key)}]
    if isinstance(value, ListV):  # the legacy loaders return a list of definitions
        found: list[dict[str, Any]] = []
        for item in value.items:
            inner = dependency(item)
            if isinstance(inner, str):
                return inner
            found += inner
        return found
    if isinstance(value, CallV) and value.func.startswith("dagster"):
        if _last(value.func) == "AssetDep" and value.args:
            return dependency(value.args[0])
        if _last(value.func) == "get_asset_key_for_model" and len(value.args) == 2:
            definitions = value.args[0]
            name = as_str(value.args[1])
            items = definitions.items if isinstance(definitions, ListV) else []
            if name is not None and len(items) == 1 and isinstance(items[0], AssetsV):
                return [{"model": name, "definition": items[0].key}]
            return "get_asset_key_for_model() of a definition or model not known here"
    key = asset_key(value)
    if key is None:
        return f"a dependency is not known here: {_why(value)}"
    return [{"key": key}]


def dependencies(value: Val | None) -> tuple[list[dict[str, Any]], str | None]:
    """``deps=[…]``: what each item names, and why some cannot be read."""
    if value is None or (isinstance(value, Const) and value.value is None):
        return [], None
    if not isinstance(value, ListV):
        return [], f"deps is not a list known here: {_why(value)}"
    if len(value.items) > MAX_DEPS:
        return [], "deps is too long to read"
    found: list[dict[str, Any]] = []
    for item in value.items:
        inner = dependency(item)
        if isinstance(inner, str):
            return found, inner
        found += inner
    return found, None


def asset_in(name: str, value: Val) -> list[dict[str, Any]] | str:
    """``AssetIn(key=…, key_prefix=…)`` for the parameter ``name``."""
    if not (isinstance(value, CallV) and _last(value.func) == "AssetIn"):
        return f"the input {name} is not an AssetIn known here"
    key_v = value.arg("key", 0)
    if key_v is not None and not (isinstance(key_v, Const) and key_v.value is None):
        key = asset_key(key_v)
        return [{"key": key}] if key is not None else f"the key of input {name} is not known here"
    prefix_v = value.arg("key_prefix", 2)
    if prefix_v is None or (isinstance(prefix_v, Const) and prefix_v.value is None):
        return [{"key": [name]}]
    prefix = asset_key(prefix_v)
    if prefix is None:
        return f"the key_prefix of input {name} is not known here"
    return [{"key": [*prefix, name]}]
