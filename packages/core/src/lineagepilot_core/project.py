"""A dbt project: where its artifacts and source files are.

Read from ``dbt_project.yml``, which is untrusted input (design section 10): the file is
size-capped and parsed with ``yaml.safe_load``, and every path in it must stay inside
the project. A value that cannot be used falls back to dbt's default and is reported.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
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
DEFAULT_PACKAGES_PATH = "dbt_packages"


@dataclass(frozen=True)
class DbtProject:
    root: Path
    name: str | None
    target_path: str  # relative to the root, with forward slashes
    source_paths: tuple[str, ...]  # likewise
    problems: tuple[str, ...] = ()
    packages_path: str = DEFAULT_PACKAGES_PATH  # where dbt deps installs packages
    # The folders of each SOURCE_PATH_KEYS key, relative to the root
    paths: dict[str, tuple[str, ...]] = field(default_factory=dict, hash=False)

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
        return any(_under(relative, p) for p in self.source_paths)

    def is_under(self, relative: str, key: str) -> bool:
        """Whether a file is in the folders of a ``dbt_project.yml`` key, say ``macro-paths``."""
        return any(_under(relative, p) for p in self.paths.get(key, ()))


def _under(relative: str, folder: str) -> bool:
    return folder == "." or relative.startswith(f"{folder}/")


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
    # Absolute, but spelled as the client gave it: paths the server reports (diagnostics,
    # navigation) must match the editor's, and on macOS /var is a symlink to /private/var.
    # Containment checks resolve symlinks on both sides.
    root = Path(os.path.abspath(root))
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
    paths: dict[str, tuple[str, ...]] = {}
    for key, default in SOURCE_PATH_KEYS.items():
        value = data.get(key, default)
        if not isinstance(value, list):
            problems.append(f"{key} is not a list; using {default}")
            value = default
        inside = [_inside(root, item, key, problems, fallback=None) for item in value]
        paths[key] = tuple(relative for relative in inside if relative is not None)
        for relative in paths[key]:
            if relative not in sources:
                sources.append(relative)
    packages = _inside(
        root,
        data.get("packages-install-path", DEFAULT_PACKAGES_PATH),
        "packages-install-path",
        problems,
        fallback=DEFAULT_PACKAGES_PATH,
    )
    return DbtProject(
        root,
        name,
        target or DEFAULT_TARGET_PATH,
        tuple(sources),
        tuple(problems),
        packages or DEFAULT_PACKAGES_PATH,
        paths,
    )


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
    base = root.resolve()
    resolved = (root / relative).resolve()
    if not resolved.is_relative_to(base):
        problems.append(f"{key} {value!r} leaves the project; ignored")
        return fallback
    return resolved.relative_to(base).as_posix() or "."
