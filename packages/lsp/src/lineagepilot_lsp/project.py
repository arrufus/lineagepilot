"""The dbt project a server serves: where its artifacts and source files are.

Read from ``dbt_project.yml``, which is untrusted input (design section 10): the file is
size-capped and parsed with ``yaml.safe_load``, and every path in it must stay inside
the project. A value that cannot be used falls back to dbt's default and is reported.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

PROJECT_FILE = "dbt_project.yml"
MAX_PROJECT_FILE_BYTES = 1_000_000

# dbt's defaults. Each names directories whose files feed the manifest, so a change to
# one of them makes the manifest stale.
SOURCE_PATH_KEYS = {
    "model-paths": ["models"],
    "seed-paths": ["seeds"],
    "snapshot-paths": ["snapshots"],
    "macro-paths": ["macros"],
    "analysis-paths": ["analyses"],
    "test-paths": ["tests"],
}
DEFAULT_TARGET_PATH = "target"


@dataclass(frozen=True)
class DbtProject:
    root: Path
    name: str | None
    target_path: str  # relative to the root, with forward slashes
    source_paths: tuple[str, ...]  # likewise
    problems: tuple[str, ...] = ()

    @property
    def manifest_path(self) -> Path:
        return self.root / self.target_path / "manifest.json"

    @property
    def catalog_path(self) -> Path:
        return self.root / self.target_path / "catalog.json"

    def relative(self, path: Path) -> str | None:
        """The path relative to the root with forward slashes, or None if outside it."""
        return relative_to(path, self.root)

    def is_source_file(self, relative: str) -> bool:
        """Whether a file (relative to the root) is one dbt reads to build the manifest."""
        if relative == PROJECT_FILE:
            return True
        return any(p == "." or relative.startswith(f"{p}/") for p in self.source_paths)


def relative_to(path: Path, root: Path) -> str | None:
    """``path`` relative to ``root`` with forward slashes, or None if it is outside.

    Compared with the platform's case rules: on Windows a URI from the editor may spell
    the drive letter differently, and a deleted file does not resolve to canonical case.
    """
    full, base = str(path.resolve()), str(root.resolve())
    if os.path.normcase(full) == os.path.normcase(base):
        return "."
    prefix = base.rstrip("\\/") + os.sep
    if not os.path.normcase(full).startswith(os.path.normcase(prefix)):
        return None
    return Path(full[len(prefix) :]).as_posix()


def read_project(root: Path) -> DbtProject:
    root = root.resolve()
    problems: list[str] = []
    data: dict[str, Any] = {}
    path = root / PROJECT_FILE
    try:
        if path.stat().st_size > MAX_PROJECT_FILE_BYTES:
            problems.append(f"{PROJECT_FILE} is larger than 1 MB; using dbt's defaults")
        else:
            loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                data = loaded
            else:
                problems.append(f"{PROJECT_FILE} is not a mapping; using dbt's defaults")
    except FileNotFoundError:
        problems.append(f"no {PROJECT_FILE} in {root}")
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as e:
        problems.append(f"{PROJECT_FILE} cannot be read ({' '.join(str(e).split())[:200]})")

    name = data.get("name") if isinstance(data.get("name"), str) else None
    target = _inside(root, data.get("target-path", DEFAULT_TARGET_PATH), "target-path", problems)
    sources: list[str] = []
    for key, default in SOURCE_PATH_KEYS.items():
        value = data.get(key, default)
        if not isinstance(value, list):
            problems.append(f"{key} is not a list; using {default}")
            value = default
        for item in value:
            relative = _inside(root, item, key, problems, fallback=None)
            if relative is not None and relative not in sources:
                sources.append(relative)
    return DbtProject(root, name, target or DEFAULT_TARGET_PATH, tuple(sources), tuple(problems))


def _inside(
    root: Path,
    value: Any,
    key: str,
    problems: list[str],
    fallback: str | None = DEFAULT_TARGET_PATH,
) -> str | None:
    """``value`` as a relative path inside ``root``, or ``fallback`` with a problem noted."""
    if not isinstance(value, str) or not value.strip() or "{{" in value or "{%" in value:
        problems.append(f"{key} {value!r} is not a plain relative path; ignored")
        return fallback
    relative = PurePosixPath(value.strip().replace("\\", "/"))
    if relative.is_absolute() or (relative.parts and ":" in relative.parts[0]):
        problems.append(f"{key} {value!r} is absolute; ignored")
        return fallback
    resolved = (root / relative).resolve()
    if not resolved.is_relative_to(root):
        problems.append(f"{key} {value!r} leaves the project; ignored")
        return fallback
    return resolved.relative_to(root).as_posix() or "."
