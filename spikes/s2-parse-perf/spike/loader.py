"""Load the backend parser modules by file path, without src/parsers/__init__.py.

The package __init__ imports airflow_parser (httpx) and snowflake_parser
(snowflake-connector), so it must never run. Instead we:

1. register empty stub packages ``src`` and ``src.parsers`` in sys.modules,
   with an empty ``__path__`` so no other file in that directory can be
   imported by accident; then
2. load the five modules we need, in dependency order, under the dotted
   names they use to import each other (``from src.parsers.base import ...``).

The backend files are executed as-is. Nothing here edits or copies them.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path
from typing import Literal

REPO_ROOT = Path(__file__).resolve().parents[4]
PARSERS_DIR = REPO_ROOT / "backend" / "src" / "parsers"

# Order matters: each module may import only modules loaded before it.
MODULES: list[tuple[str, str]] = [
    ("src.parsers.base", "base.py"),
    ("src.parsers.dbt_config", "dbt_config.py"),
    ("src.parsers.sql_parser", "sql_parser.py"),
    ("src.parsers.sql_column_parser", "sql_column_parser.py"),
    ("src.parsers.dbt_parser", "dbt_parser.py"),
]

# Modules the language server must never pull in (design section 4.4).
FORBIDDEN_MODULES = ("httpx", "snowflake", "sqlalchemy", "fastapi", "pydantic")

JsonBackend = Literal["orjson", "stdlib", "stdlib-trim"]


class _StdlibOrjson(types.ModuleType):
    """Stand-in for ``orjson`` backed by stdlib ``json``.

    LABELLED VARIANT, not as-is: dbt_parser.py does ``import orjson`` with no
    fallback. This shim models the Phase 1 fix ("stdlib json becomes the
    fallback for orjson") so we can time the parser when orjson is absent.
    """

    JSONDecodeError = json.JSONDecodeError

    def __init__(self) -> None:
        super().__init__("orjson")
        self.__dict__["__spec__"] = None
        self.__dict__["_lineagepilot_shim"] = True

    @staticmethod
    def loads(data: bytes | str):  # noqa: D401 - mirrors orjson.loads
        return json.loads(data)

    @staticmethod
    def dumps(obj) -> bytes:
        return json.dumps(obj).encode()


_UNUSED_RESOURCES = {"test", "macro", "doc", "unit_test"}
_UNUSED_KEYS = ("raw_code", "compiled_code")


def _trim_hook(d: dict) -> dict:
    """Drop, while decoding, what DbtParser never reads, so it is never held."""
    rt = d.get("resource_type")
    if rt in _UNUSED_RESOURCES:
        return {"resource_type": rt}
    for k in _UNUSED_KEYS:
        if k in d:
            del d[k]
    return d


class _StdlibTrimOrjson(_StdlibOrjson):
    """LABELLED VARIANT, not as-is: stdlib json with an object_hook that drops
    test/macro/doc bodies and raw/compiled SQL during decoding. The ParseResult is
    unchanged (tests/test_loader.py checks); it exists to show the memory headroom.
    Compiled SQL for column lineage would then be read per model, on demand."""

    def __init__(self) -> None:
        super().__init__()
        self.__dict__["_lineagepilot_trim"] = True

    @staticmethod
    def loads(data: bytes | str):  # noqa: D401
        return json.loads(data, object_hook=_trim_hook)


def _stub_package(name: str) -> types.ModuleType:
    mod = types.ModuleType(name)
    mod.__path__ = []  # a package, but nothing can be found inside it
    mod.__dict__["_lineagepilot_stub"] = True
    return mod


def load_parsers(json_backend: JsonBackend = "orjson") -> dict[str, types.ModuleType]:
    """Load the backend parser modules and return them keyed by short name.

    Idempotent: a second call returns the modules already loaded.
    ``json_backend="stdlib"`` installs the labelled ``orjson`` shim first and
    must be chosen before the first load in a process.
    """
    if "src.parsers.dbt_parser" in sys.modules:
        mods = {n.rsplit(".", 1)[1]: sys.modules[n] for n, _ in MODULES}
        return mods

    if json_backend in ("stdlib", "stdlib-trim"):
        if "orjson" in sys.modules and not getattr(sys.modules["orjson"], "_lineagepilot_shim", False):
            raise RuntimeError("real orjson already imported; cannot install stdlib shim")
        sys.modules["orjson"] = _StdlibTrimOrjson() if json_backend == "stdlib-trim" else _StdlibOrjson()

    for pkg in ("src", "src.parsers"):
        if pkg not in sys.modules:
            sys.modules[pkg] = _stub_package(pkg)
    sys.modules["src"].parsers = sys.modules["src.parsers"]  # type: ignore[attr-defined]

    loaded: dict[str, types.ModuleType] = {}
    for dotted, filename in MODULES:
        path = PARSERS_DIR / filename
        spec = importlib.util.spec_from_file_location(dotted, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[dotted] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            del sys.modules[dotted]
            raise
        setattr(sys.modules["src.parsers"], dotted.rsplit(".", 1)[1], module)
        loaded[dotted.rsplit(".", 1)[1]] = module
    return loaded


def forbidden_loaded() -> list[str]:
    """Return any forbidden top-level module currently in sys.modules."""
    return sorted(
        name for name in sys.modules
        if name.split(".", 1)[0] in FORBIDDEN_MODULES
    )


def json_backend_in_use() -> str:
    mod = sys.modules.get("orjson")
    if mod is None:
        return "none"
    if getattr(mod, "_lineagepilot_trim", False):
        return "stdlib-trim-shim"
    return "stdlib-shim" if getattr(mod, "_lineagepilot_shim", False) else "orjson"
