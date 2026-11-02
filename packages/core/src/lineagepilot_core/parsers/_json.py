"""JSON loading: orjson if it is already importable, the standard library otherwise.

orjson is never a dependency (design section 4.4). Spike S2 found stdlib ``json`` adds
0.1-0.4 s to a 5,000-model cold index and lowers peak memory, so it is a safe default.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import orjson
except ImportError:  # pragma: no cover - depends on the environment
    orjson = None  # type: ignore[assignment]


def loads(data: bytes | str) -> Any:
    """Decode JSON. Raises ``ValueError`` (``json.JSONDecodeError``) on bad input."""
    if orjson is not None:
        return orjson.loads(data)
    return json.loads(data)


def load_file(path: str | Path) -> Any:
    """Read and decode a JSON file. Raises ``OSError`` or ``ValueError``."""
    with open(path, "rb") as f:
        return loads(f.read())
