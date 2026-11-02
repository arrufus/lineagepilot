"""Where things are in the project's files: navigation targets, CodeLens and diagnostic
positions (tasks 2.2 and 2.3).

A dbt node lives in its SQL (or CSV) file and is documented in a YAML properties file.
Positions inside YAML come from ``yaml.compose``, which yields the node tree with line
and column marks and never constructs Python objects. YAML files are untrusted, so each
is size-capped, and a file that does not parse simply has no positions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from lineagepilot_core.graph.ids import split_column_id
from lineagepilot_core.graph.model import Node, NodeKind

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
    entries = YamlEntries()
    if len(text) > MAX_YAML_BYTES:
        return entries
    try:
        root = yaml.compose(text, Loader=yaml.SafeLoader)
    except yaml.YAMLError:
        return entries
    if not isinstance(root, yaml.MappingNode):
        return entries
    for key_node, value in root.value:
        section = key_node.value if isinstance(key_node, yaml.ScalarNode) else None
        if section == "sources":
            for source, source_node in _named(value):
                for table, table_node in _named(_child(source_node, "tables")):
                    entries.spans[("sources", source, "tables", table)] = _name_span(table_node)
                    _columns(entries, ("sources", source, "tables", table), table_node)
        elif section in _SECTIONS.values():
            for name, item in _named(value):
                entries.spans[(section, name)] = _name_span(item)
                _columns(entries, (section, name), item)
    return entries


def _named(node: Any) -> list[tuple[str, yaml.MappingNode]]:
    """The ``- name: x`` items of a sequence."""
    if not isinstance(node, yaml.SequenceNode):
        return []
    found = []
    for item in node.value:
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


def _columns(entries: YamlEntries, prefix: tuple[str, ...], item: yaml.MappingNode) -> None:
    for column, column_node in _named(_child(item, "columns")):
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
    section = _SECTIONS.get(node.kind)
    return (section, node.name) if section else None


class Locator:
    """Files and positions for the nodes of one project, with YAML files read once."""

    def __init__(self, project: DbtProject) -> None:
        self.project = project
        self._yaml: dict[Path, YamlEntries] = {}

    def file(self, node: Node, relative: str | None) -> Path | None:
        """A node's file (relative to its package), if it exists in the workspace."""
        if not relative:
            return None
        root = self.project.root
        if node.package and self.project.name and node.package != self.project.name:
            root = root / self.project.packages_path / node.package
        path = root / relative
        return path if path.is_file() else None

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
