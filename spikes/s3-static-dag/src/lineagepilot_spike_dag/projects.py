"""dbt project discovery, container-path remapping and manifest choice."""

from __future__ import annotations

import os
import posixpath
from dataclasses import dataclass, field

from .manifest import Manifest, ManifestError, load_manifest
from .standin import build_standin

SKIP_DIRS = {".git", "node_modules", "dbt_packages", "dbt_modules", "target", "logs", "__pycache__", ".venv", "venv", "site-packages", ".tox"}


@dataclass
class ProjectMatch:
    path: str | None
    conf: str
    note: str | None = None
    reason: str | None = None


@dataclass
class ManifestChoice:
    manifest: Manifest | None
    kind: str | None  # committed | dag_declared | cli | stand_in
    conf: str
    notes: list[str] = field(default_factory=list)
    reason: str | None = None


def _parts(p: str) -> list[str]:
    return [x for x in p.replace("\\", "/").split("/") if x not in ("", ".")]


class ProjectIndex:
    def __init__(self, workspace: str, extra_manifests: list[str] | None = None, remap: bool = True):
        self.workspace = os.path.realpath(workspace)
        self.remap = remap
        self.projects = self._discover()
        self._manifests: dict[str, ManifestChoice] = {}
        self.cli_manifests: dict[str, str] = {}
        for mp in extra_manifests or []:
            mp = os.path.realpath(mp)
            parent = os.path.dirname(mp)
            proj = os.path.dirname(parent) if os.path.basename(parent) == "target" else parent
            self.cli_manifests[proj] = mp

    def _discover(self) -> list[str]:
        found: list[str] = []
        for dirpath, dirnames, filenames in os.walk(self.workspace):
            dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
            if "dbt_project.yml" in filenames:
                found.append(os.path.realpath(dirpath))
        return found

    def is_project(self, path: str) -> bool:
        return os.path.isfile(os.path.join(path, "dbt_project.yml"))

    def match(self, raw: str, base_dir: str | None = None) -> ProjectMatch:
        """Map a project path written in a DAG to a dbt project in the workspace."""
        candidate = raw
        if not posixpath.isabs(raw) and base_dir:
            candidate = posixpath.normpath(posixpath.join(base_dir, raw))
        if posixpath.isabs(candidate):
            real = os.path.realpath(candidate)
            if self._inside(real) and self.is_project(real):
                return ProjectMatch(real, "inferred")
        if not self.remap:
            return ProjectMatch(None, "heuristic", reason=f"project path {raw!r} is not a dbt project in the workspace (remapping disabled)")
        # Not a project on this machine: container paths such as /usr/local/airflow/dbt/x,
        # or paths relative to an unknown working directory. Match by trailing components.
        want = _parts(raw)
        best: list[tuple[int, str]] = []
        for proj in self.projects:
            have = _parts(os.path.relpath(proj, self.workspace))
            n = 0
            while n < len(want) and n < len(have) and want[-1 - n] == have[-1 - n]:
                n += 1
            if n:
                best.append((n, proj))
        if not best:
            return ProjectMatch(None, "heuristic", reason=f"project path {raw!r} matches no dbt project in the workspace")
        best.sort(reverse=True)
        top = best[0][0]
        winners = [p for n, p in best if n == top]
        if len(winners) > 1:
            rels = [os.path.relpath(p, self.workspace) for p in winners]
            return ProjectMatch(None, "heuristic", reason=f"project path {raw!r} is ambiguous: {rels}")
        rel = os.path.relpath(winners[0], self.workspace)
        return ProjectMatch(
            winners[0], "heuristic",
            note=f"project path {raw!r} does not exist here; mapped to {rel} by its last {top} path component(s)",
        )

    def only_project(self) -> str | None:
        return self.projects[0] if len(self.projects) == 1 else None

    def _inside(self, path: str) -> bool:
        return path == self.workspace or path.startswith(self.workspace + os.sep)

    def manifest_for(self, project_dir: str) -> ManifestChoice:
        if project_dir in self._manifests:
            return self._manifests[project_dir]
        choice = self._choose(project_dir)
        self._manifests[project_dir] = choice
        return choice

    def load_declared(self, manifest_path: str) -> ManifestChoice:
        real = os.path.realpath(manifest_path)
        if not self._inside(real):
            return ManifestChoice(None, None, "heuristic", reason=f"manifest {manifest_path} is outside the workspace")
        try:
            m = load_manifest(real)
        except ManifestError as exc:
            return ManifestChoice(None, None, "heuristic", reason=f"manifest {manifest_path}: {exc}")
        return ManifestChoice(m, "dag_declared", "inferred")

    def _choose(self, project_dir: str) -> ManifestChoice:
        if project_dir in self.cli_manifests:
            try:
                return ManifestChoice(load_manifest(self.cli_manifests[project_dir]), "cli", "inferred")
            except ManifestError as exc:
                return ManifestChoice(None, None, "heuristic", reason=str(exc))
        committed = os.path.join(project_dir, "target", "manifest.json")
        if os.path.isfile(committed):
            try:
                m = load_manifest(committed)
            except ManifestError as exc:
                return ManifestChoice(None, None, "heuristic", reason=str(exc))
            notes = drift_notes(project_dir, m)
            return ManifestChoice(m, "committed", "inferred", notes=notes)
        m = build_standin(project_dir)
        return ManifestChoice(m, "stand_in", "heuristic", notes=["no target/manifest.json: approximate stand-in manifest built from project files"] + m.notes)


def drift_notes(project_dir: str, manifest: Manifest) -> list[str]:
    """Cheap staleness check: model files present on disk but absent from the manifest, and vice versa."""
    on_disk: set[str] = set()
    for mp in ("models",):
        base = os.path.join(project_dir, mp)
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            for fn in filenames:
                if fn.endswith((".sql", ".py")):
                    on_disk.add(os.path.relpath(os.path.join(dirpath, fn), project_dir).replace(os.sep, "/"))
    in_manifest = {n.original_file_path for n in manifest.of_type("model") if not n.external}
    notes = []
    missing = sorted(on_disk - in_manifest)
    extra = sorted(in_manifest - on_disk)
    if missing:
        notes.append(f"committed manifest may be stale: model files not in it: {missing}")
    if extra:
        notes.append(f"committed manifest may be stale: manifest models with no file: {extra}")
    return notes
