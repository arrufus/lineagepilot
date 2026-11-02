"""Shared fixtures."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from support import raw_manifest


@pytest.fixture
def manifest_of() -> Callable[[str], dict[str, Any]]:
    """A fresh, mutable copy of a fixture project's manifest."""
    return lambda name: copy.deepcopy(raw_manifest(name))


@pytest.fixture
def write_json(tmp_path: Path) -> Callable[..., Path]:
    """Write a value as JSON under tmp_path and return the path."""

    def write(value: Any, name: str = "manifest.json") -> Path:
        path = tmp_path / name
        path.write_text(json.dumps(value))
        return path

    return write
