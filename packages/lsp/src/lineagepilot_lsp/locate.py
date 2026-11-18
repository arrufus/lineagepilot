"""Where things are in the project's files: navigation targets, CodeLens and diagnostic
positions (tasks 2.2 and 2.3).

A dbt node lives in its SQL (or CSV) file and is documented in a YAML properties file.
Positions inside YAML come from ``block_yaml``, a quick reader for the plain block YAML dbt's
files are written in, and otherwise from ``yaml.compose``, which yields the node tree with line
and column marks and never constructs Python objects. Both give the same positions. YAML files
are untrusted, so each is size-capped, its aliases are bounded (``safe_yaml``), and a file that
does not parse simply has no positions.

File paths come from the manifest, which is untrusted too (audit LP-SEC-04): a path is
used only if it stays inside the project (a DAG file: the workspace), checked as written
before anything is looked at, then with its links resolved.
"""

from __future__ import annotations

import logging
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from lineagepilot_core.graph.ids import split_column_id
from lineagepilot_core.graph.model import TOOL_KINDS, Node, NodeKind
from lineagepilot_core.parsers import safe_yaml
from lineagepilot_core.parsers.dbt_semantic import SEMANTIC_KEYS, SEMANTIC_TOOL
from lineagepilot_core.readers.common.safe_io import regular_file_inside, resolve_inside

from lineagepilot_lsp import block_yaml
from lineagepilot_lsp.project import DbtProject

logger = logging.getLogger(__name__)

MAX_YAML_BYTES = 2_000_000

# Top-level YAML keys, by the kind of node they declare
_SECTIONS = {
    NodeKind.MODEL: "models",
    NodeKind.SEED: "seeds",
    NodeKind.SNAPSHOT: "snapshots",
    NodeKind.EXPOSURE: "exposures",
}
# The semantic layer's sections, by subkind (task 7.8)
_SEMANTIC_SECTIONS = SEMANTIC_KEYS
_NAMED_SECTIONS = frozenset({*_SECTIONS.values(), *_SEMANTIC_SECTIONS.values()})


@dataclass(frozen=True)
class Span:
    """A 0-based LSP range."""

    start_line: int
    start_character: int
    end_line: int
    end_character: int

    def to_json(self) -> dict[str, Any]:
        return {
            "start": {"line": self.start_line, "character": self.start_character},
            "end": {"line": self.end_line, "character": self.end_character},
        }


@dataclass
class YamlEntries:
    """The ``name:`` values of one YAML file, keyed by where they sit.

    Keys: ``("models", name)``, ``("models", name, "columns", column)``, ``("sources",
    source, "tables", table)``, and so on. Column names are lower-cased, because YAML
    and the warehouse often spell them differently.
    """

    spans: dict[tuple[str, ...], Span] = field(default_factory=dict)

    def get(self, *key: str) -> Span | None:
        return self.spans.get(key)


def yaml_entries(text: str) -> YamlEntries:
    if len(text) > MAX_YAML_BYTES:
        return YamlEntries()
    try:
        return _plain_entries(block_yaml.read(text))
    except block_yaml.NotPlain:
        return _composed_entries(text)


def _plain_entries(root: block_yaml.Mapping | None) -> YamlEntries:
    """``yaml_entries`` from ``block_yaml``'s tree: the same walk as ``_composed_entries``."""
    entries = YamlEntries()
    for section, value in root or ():
        if section == "sources":
            for source, source_item in _plain_named(value):
                for table, table_item in _plain_named(_plain_child(source_item, "tables")):
                    key = ("sources", source.value, "tables", table.value)
                    entries.spans[key] = _plain_span(table)
                    _plain_columns(entries, key, table_item)
        elif section in _NAMED_SECTIONS:
            for name, item in _plain_named(value):
                entries.spans[(section, name.value)] = _plain_span(name)
                _plain_columns(entries, (section, name.value), item)
    return entries


_MISSING = object()


def _plain_child(node: Any, key: str) -> Any:
    if isinstance(node, block_yaml.Mapping):
        for found, value in node:
            if found == key:
                return value
    return _MISSING


def _plain_named(node: Any) -> list[tuple[block_yaml.Scalar, block_yaml.Mapping]]:
    if not isinstance(node, block_yaml.Sequence):
        return []
    found = []
    for item in node:
        name = _plain_child(item, "name")
        if name is None:
            # "name:" with nothing after it: PyYAML reads an empty name, at a place of its own
            raise block_yaml.NotPlain("an item's name is empty")
        if isinstance(name, block_yaml.Scalar):
            if name.value is None:  # only YAML's escapes or folding would say what it is
                raise block_yaml.NotPlain("a name this reader cannot spell")
            found.append((name, item))
    return found


def _plain_span(name: block_yaml.Scalar) -> Span:
    return Span(name.line, name.start, name.line, name.end)


def _plain_columns(entries: YamlEntries, prefix: tuple[str, ...], item: Any) -> None:
    for column, _ in _plain_named(_plain_child(item, "columns")):
        entries.spans[(*prefix, "columns", column.value.lower())] = _plain_span(column)


def _composed_entries(text: str) -> YamlEntries:
    entries = YamlEntries()
    try:
        root = safe_yaml.compose(text)
    except yaml.YAMLError:
        return entries
    if not isinstance(root, yaml.MappingNode):
        return entries
    # An alias is the same node each time it is used: each is read once, so a list of aliases
    # costs its length, not its length times what each alias holds
    seen: set[int] = set()
    for key_node, value in root.value:
        section = key_node.value if isinstance(key_node, yaml.ScalarNode) else None
        if section == "sources":
            for source, source_node in _named(value, seen):
                for table, table_node in _named(_child(source_node, "tables"), seen):
                    entries.spans[("sources", source, "tables", table)] = _name_span(table_node)
                    _columns(entries, ("sources", source, "tables", table), table_node, seen)
        elif section in _NAMED_SECTIONS:
            for name, item in _named(value, seen):
                entries.spans[(section, name)] = _name_span(item)
                _columns(entries, (section, name), item, seen)
    return entries


def _named(node: Any, seen: set[int]) -> list[tuple[str, yaml.MappingNode]]:
    """The ``- name: x`` items of a sequence, each node once."""
    if not isinstance(node, yaml.SequenceNode) or id(node) in seen:
        return []
    seen.add(id(node))
    found = []
    for item in node.value:
        if id(item) in seen:
            continue
        seen.add(id(item))
        name = _child(item, "name")
        if isinstance(name, yaml.ScalarNode) and isinstance(item, yaml.MappingNode):
            found.append((name.value, item))
    return found


def _child(node: Any, key: str) -> Any:
    if not isinstance(node, yaml.MappingNode):
        return None
    for key_node, value in node.value:
        if isinstance(key_node, yaml.ScalarNode) and key_node.value == key:
            return value
    return None


def _name_span(item: yaml.MappingNode) -> Span:
    name = _child(item, "name")
    return Span(
        name.start_mark.line, name.start_mark.column, name.end_mark.line, name.end_mark.column
    )


def _columns(
    entries: YamlEntries, prefix: tuple[str, ...], item: yaml.MappingNode, seen: set[int]
) -> None:
    for column, column_node in _named(_child(item, "columns"), seen):
        entries.spans[(*prefix, "columns", column.lower())] = _name_span(column_node)


def entry_key(node: Node) -> tuple[str, ...] | None:
    """Where a node is declared in its YAML file."""
    if node.kind is NodeKind.SOURCE:
        prefix = f"source.{node.package}."
        suffix = f".{node.name}"
        if node.id.startswith(prefix) and node.id.endswith(suffix):
            source = node.id[len(prefix) : -len(suffix)]
            return ("sources", source, "tables", node.name)
        return None
    if node.tool == SEMANTIC_TOOL:
        section = _SEMANTIC_SECTIONS.get(node.subkind or "")
    else:
        section = _SECTIONS.get(node.kind)
    return (section, node.name) if section else None


def _written_inside(path: Path, root: Path) -> bool:
    """Whether a path is ``root`` or below it as written, with no filesystem call."""
    full = os.path.normcase(os.path.normpath(path))
    base = os.path.normcase(os.path.normpath(root))
    return full == base or full.startswith(base.rstrip("\\/") + os.sep)


class Locator:
    """Files and positions for the nodes of one project, with YAML files read once.

    ``workspace``: the folder DAG files must be in, and the one a dbt file's links must stay
    in (``dbt deps`` links a local package into the packages folder). By default the
    project's root.
    """

    def __init__(self, project: DbtProject, workspace: Path | None = None) -> None:
        self.project = project
        # As for the DAG scan: a project outside the workspace folder is its own workspace
        inside = workspace is not None and _written_inside(project.root, workspace)
        self.workspace = workspace if inside else project.root
        self._real = {root: os.path.realpath(root) for root in (self.project.root, self.workspace)}
        self._yaml: dict[Path, YamlEntries] = {}
        # For one request: directories with no link between them and the root, and files found
        self._plain_dirs: dict[str, bool] | None = None
        self._files: dict[str, bool] | None = None

    @contextmanager
    def batch(self) -> Iterator[None]:
        """Where to open many nodes at once, as one request does: a review of 100 models places
        thousands of rows, many in the same folders and files (task 7.10). Each folder is checked
        once, then each file with one ``lstat``, rather than every path from the root each time.
        Nothing is kept after the request, so a file that becomes a link is checked afresh."""
        if self._files is not None:  # already in one
            yield
            return
        self._plain_dirs, self._files = {}, {}
        try:
            yield
        finally:
            self._plain_dirs = self._files = None

    def _regular_file(self, real: str, root: str) -> bool:
        """Whether ``real`` is a regular file inside ``root``, its links followed only inside it:
        ``regular_file_inside``, quicker for many files in a batch."""
        files, plain_dirs = self._files, self._plain_dirs
        if files is None or plain_dirs is None:
            return regular_file_inside(real, root) is not None
        if real not in files:
            directory = os.path.dirname(real)
            if directory not in plain_dirs:
                # resolve_inside leaves a path as written only if no part of it is a link
                plain_dirs[directory] = resolve_inside(directory, root) == directory
            try:
                found = os.lstat(real)
            except (OSError, ValueError):
                found = None
            # On Windows any reparse point may lead elsewhere, as safe_io treats a junction
            plain = found is not None and not stat.S_ISLNK(found.st_mode)
            if plain_dirs[directory] and plain and not getattr(found, "st_reparse_tag", 0):
                files[real] = stat.S_ISREG(found.st_mode)  # type: ignore[union-attr]
            else:  # a link, or something that is not there: the full check
                files[real] = regular_file_inside(real, root) is not None
        return files[real]

    def file(self, node: Node, relative: str | None) -> Path | None:
        """A node's file (relative to its package), if it is a file inside the project; or,
        for a DAG file, inside the workspace."""
        if not relative:
            return None
        root = self.project.root
        if node.package and self.project.name and node.package != self.project.name:
            root = root / self.project.packages_path / node.package
        # A DAG file may be outside the project: its path is relative, with .. in it
        path = Path(os.path.normpath(root / relative))
        written = self.workspace if node.kind in TOOL_KINDS else self.project.root
        if not _written_inside(path, written):
            return None
        # The path keeps the editor's spelling (macOS: /var, not /private/var); its links are
        # followed from the real root, one at a time, and only while they stay in the workspace
        real = os.path.join(self._real[written], os.path.relpath(path, written))
        return path if self._regular_file(real, self._real[self.workspace]) else None

    def entries(self, path: Path) -> YamlEntries:
        if path not in self._yaml:
            try:
                text = (
                    path.read_text(encoding="utf-8")
                    if path.stat().st_size <= MAX_YAML_BYTES
                    else ""
                )
            except (OSError, UnicodeDecodeError):
                text = ""
            self._yaml[path] = yaml_entries(text)
        return self._yaml[path]

    def forget(self, path: Path) -> None:
        self._yaml.pop(path, None)

    def node(self, node: Node) -> tuple[Path, Span | None] | None:
        """Where to open a node: its SQL or CSV file, or its entry in YAML."""
        path = self.file(node, node.file_path)
        if path is None:
            return None
        if node.kind in TOOL_KINDS and "line" in node.properties:
            line, column = node.properties.get("line", 0), node.properties.get("col", 0)
            return path, Span(line, column, line, column)
        if path.suffix in (".yml", ".yaml"):
            key = entry_key(node)
            return path, self.entries(path).get(*key) if key else None
        return path, None

    def documentation(self, node: Node) -> tuple[Path, Span | None] | None:
        """A node's entry in the YAML that documents it, if it has one."""
        relative = node.patch_path or (node.file_path if node.kind is NodeKind.SOURCE else None)
        path = self.file(node, relative)
        if path is None or path.suffix not in (".yml", ".yaml"):
            return None
        key = entry_key(node)
        return path, self.entries(path).get(*key) if key else None

    def column(self, node: Node, column_id: str) -> tuple[Path, Span | None] | None:
        """A column's entry in YAML, or else its node's file."""
        _, column = split_column_id(column_id)
        documented = self.documentation(node)
        key = entry_key(node)
        if documented is not None and key is not None:
            span = self.entries(documented[0]).get(*key, "columns", column.lower())
            if span is not None:
                return documented[0], span
        return self.node(node)


def location_json(found: tuple[Path, Span | None] | None) -> dict[str, Any] | None:
    if found is None:
        return None
    path, span = found
    return {"uri": path.as_uri(), "range": span.to_json() if span else None}
