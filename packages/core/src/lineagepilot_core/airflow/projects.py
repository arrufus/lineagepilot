"""dbt project discovery, container-path remapping and manifest choice (tasks 5.1 and 5.2).

A DAG file names a dbt project by a path that is rarely this machine's:
``/usr/local/airflow/dbt/x`` in an Astro image, ``dags/dbt/x`` relative to something unknown. So a
path is resolved in this order:

1. As written, when it is a dbt project inside the workspace (``inferred``).
2. Through the annotation file's explicit path map (``inferred``, a person declared it).
3. By its trailing path components against the projects in the workspace (``heuristic``).

Nothing outside the workspace is ever read.
"""

from __future__ import annotations

import os
import posixpath
from dataclasses import dataclass, field

from lineagepilot_core.airflow.manifest import Manifest, ManifestError, load_manifest
from lineagepilot_core.airflow.safe_io import is_absolute, is_inside, to_posix
from lineagepilot_core.airflow.standin import build_standin

SKIP_DIRS = {
    ".git",
    "node_modules",
    "dbt_packages",
    "dbt_modules",
    "target",
    "logs",
    "__pycache__",
    ".venv",
    "venv",
    "site-packages",
    ".tox",
}
# Directories looked at when finding dbt projects: enough for any repository a person works in
MAX_DIRECTORIES = 200_000


@dataclass
class ProjectMatch:
    path: str | None
    conf: str
    note: str | None = None
    reason: str | None = None


@dataclass
class ManifestChoice:
    manifest: Manifest | None
    kind: str | None  # target | dag_declared | stand_in
    conf: str
    notes: list[str] = field(default_factory=list)
    reason: str | None = None


def _parts(p: str) -> list[str]:
    return [x for x in p.replace("\\", "/").split("/") if x not in ("", ".")]


class ProjectIndex:
    """The dbt projects in a workspace.

    Args:
        manifests: a project's root to its manifest, for one not at ``target/manifest.json``
        path_map: container path to a folder, already absolute (see ``annotations``)
        focus: the one project wanted. Links to others are not resolved: the language server of
            each project scans for itself, and reading every manifest would multiply the cost.
    """

    def __init__(
        self,
        workspace: str,
        manifests: dict[str, str] | None = None,
        path_map: dict[str, str] | None = None,
        focus: str | None = None,
    ) -> None:
        self.workspace = os.path.realpath(workspace)
        self.manifests = {os.path.realpath(k): v for k, v in (manifests or {}).items()}
        self.path_map = sorted((path_map or {}).items(), key=lambda kv: -len(kv[0]))
        self.focus = os.path.realpath(focus) if focus else None
        self.problems: list[str] = []
        self.projects = self._discover()
        self._manifests: dict[str, ManifestChoice] = {}

    def _discover(self) -> list[str]:
        found: list[str] = []
        seen = 0
        for dirpath, dirnames, filenames in os.walk(self.workspace):
            seen += 1
            if seen > MAX_DIRECTORIES:
                self.problems.append(
                    f"looked at {MAX_DIRECTORIES} directories for dbt projects, and stopped"
                )
                break
            dirnames[:] = sorted(
                d for d in dirnames if d not in SKIP_DIRS and not d.startswith(".")
            )
            if "dbt_project.yml" in filenames:
                found.append(os.path.realpath(dirpath))
        return sorted(found)

    def relative(self, path: str) -> str:
        """A path inside the workspace, relative to it with forward slashes."""
        return to_posix(os.path.relpath(path, self.workspace))

    def is_project(self, path: str) -> bool:
        return os.path.isfile(os.path.join(path, "dbt_project.yml"))

    def is_other_project(self, path: str) -> bool:
        return self.focus is not None and path != self.focus

    def _as_project(self, path: str) -> str | None:
        real = os.path.realpath(path)
        return real if is_inside(real, self.workspace) and self.is_project(real) else None

    def _mapped(self, path: str) -> tuple[str, str] | None:
        """The longest path-map prefix that applies: (the path in the workspace, the prefix)."""
        for prefix, target in self.path_map:
            clean = prefix.rstrip("/")
            if path == clean or path.startswith(f"{clean}/"):
                return os.path.join(target, path[len(clean) :].lstrip("/")), prefix
        return None

    def match(self, raw: str, base_dir: str | None = None) -> ProjectMatch:
        """Map a project path written in a DAG to a dbt project in the workspace."""
        candidate = raw
        if not is_absolute(raw) and base_dir:
            candidate = posixpath.normpath(posixpath.join(base_dir, raw))
        if is_absolute(candidate):
            found = self._as_project(candidate)
            if found:
                return ProjectMatch(found, "inferred")
        mapped = self._mapped(candidate)
        if mapped is not None:
            found = self._as_project(mapped[0])
            if found:
                note = f"project path {raw!r} mapped by the path map entry {mapped[1]!r}"
                return ProjectMatch(found, "inferred", note=note)
        # Not a project on this machine: container paths such as /usr/local/airflow/dbt/x,
        # or paths relative to an unknown working directory. Match by trailing components.
        want = _parts(candidate)
        best: list[tuple[int, str]] = []
        for project in self.projects:
            have = _parts(self.relative(project))
            n = 0
            while n < len(want) and n < len(have) and want[-1 - n] == have[-1 - n]:
                n += 1
            if n:
                best.append((n, project))
        if not best:
            return ProjectMatch(
                None, "heuristic", reason=f"project path {raw!r} matches no dbt project here"
            )
        top = max(n for n, _ in best)
        winners = sorted(p for n, p in best if n == top)
        if len(winners) > 1:
            names = [self.relative(p) for p in winners]
            return ProjectMatch(
                None, "heuristic", reason=f"project path {raw!r} is ambiguous: {names}"
            )
        note = (
            f"project path {raw!r} does not exist here; mapped to {self.relative(winners[0])} "
            f"by its last {top} path component(s)"
        )
        return ProjectMatch(winners[0], "heuristic", note=note)

    def only_project(self) -> str | None:
        return self.projects[0] if len(self.projects) == 1 else None

    def manifest_for(self, project_dir: str) -> ManifestChoice:
        if project_dir not in self._manifests:
            self._manifests[project_dir] = self._choose(project_dir)
        return self._manifests[project_dir]

    def load_declared(self, manifest_path: str) -> ManifestChoice:
        real = os.path.realpath(manifest_path)
        if not is_inside(real, self.workspace):
            reason = f"manifest {manifest_path} is outside the workspace"
            return ManifestChoice(None, None, "heuristic", reason=reason)
        try:
            manifest = load_manifest(real)
        except ManifestError as exc:
            reason = f"manifest {manifest_path}: {exc}"
            return ManifestChoice(None, None, "heuristic", reason=reason)
        return ManifestChoice(manifest, "dag_declared", "inferred")

    def _choose(self, project_dir: str) -> ManifestChoice:
        path = self.manifests.get(project_dir) or os.path.join(
            project_dir, "target", "manifest.json"
        )
        if os.path.isfile(path):
            try:
                manifest = load_manifest(path)
            except ManifestError as exc:
                return ManifestChoice(None, None, "heuristic", reason=str(exc))
            self.problems.extend(_drift(self.relative(project_dir), project_dir, manifest))
            return ManifestChoice(manifest, "target", "inferred")
        manifest = build_standin(project_dir)
        notes = ["no manifest: an approximate stand-in was built from the project's files"]
        return ManifestChoice(manifest, "stand_in", "heuristic", notes=notes + manifest.notes)


def _drift(name: str, project_dir: str, manifest: Manifest) -> list[str]:
    """A cheap staleness check: model files on disk and in the manifest that do not agree."""
    on_disk: set[str] = set()
    for dirpath, dirnames, filenames in os.walk(os.path.join(project_dir, "models")):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for file in filenames:
            if file.endswith((".sql", ".py")):
                path = os.path.relpath(os.path.join(dirpath, file), project_dir)
                on_disk.add(to_posix(path))
    in_manifest = {n.original_file_path for n in manifest.of_type("model") if not n.external}
    notes = []
    if on_disk - in_manifest:
        notes.append(
            f"{name}: the manifest may be stale: "
            f"{len(on_disk - in_manifest)} model file(s) not in it"
        )
    if in_manifest - on_disk:
        notes.append(
            f"{name}: the manifest may be stale: {len(in_manifest - on_disk)} model(s) with no file"
        )
    return notes
