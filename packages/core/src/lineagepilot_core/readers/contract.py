"""The reader contract (design_next_phase §7.2, task 7.3).

Each tool beside dbt is read by a reader under ``lineagepilot_core.readers.<tool>``, which declares
the five parts below, and ``registry.READERS`` lists them. The language server runs every reader
the same way: it reads in a child process, with a time limit and a memory limit; it links what was
read on top of the loaded graph, never into a snapshot; and it lets what was read go once linked.

- **Discovery**: a setting of paths or globs relative to the workspace, bounded like
  ``dagFolders``. From it, the globs the editor watches, and whether a changed file is the
  reader's. A change to one of its annotation files also means reading again.
- **Reading**: workspace files as text and syntax only, through ``common.safe_io``, into JSON.
  Nothing is imported or run, so a reader works in Restricted Mode.
- **Linking**: that JSON onto the graph, by dbt ID, by selector (``common.selector``), by
  relation name, or by a key of the tool's own.
- **Output**: nodes and edges with evidence, named unknowns at the line that caused them, each
  with a category from ``common.categories``, and a summary of what was read, in
  ``graph.summaries[tool]``.
- **Lifecycle**: ``unlink`` takes it all out again. ``not_analysed`` says why there is nothing,
  so nobody reads an empty part of the graph as "nothing is affected".
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol, runtime_checkable

from lineagepilot_core.graph.model import MetadataGraph


@runtime_checkable
class Reader(Protocol):
    tool: str  # Node.tool of what it adds, and the key of its summary
    files: str  # what it reads, as messages name it: "the DAG files"
    option: str  # its setting in the server's initializationOptions: "dagFolders"
    default_paths: tuple[str, ...]
    annotation_files: tuple[str, ...]  # relative to the project

    # Discovery

    def clean_paths(self, paths: Iterable[str]) -> list[str]:
        """The setting's entries that are relative and stay in the workspace; the rest dropped."""
        ...

    def watch_globs(self, paths: list[str]) -> list[str]:
        """Globs relative to the workspace, for the editor to watch."""
        ...

    def watches(self, relative: str, paths: list[str]) -> bool:
        """Whether a changed file, relative to the workspace, is one this reader reads."""
        ...

    # Reading, in the child process

    def read(
        self, project_dir: str, workspace: str, paths: list[str], manifest: str | None
    ) -> dict[str, Any]:
        """What the files say about this project, as JSON."""
        ...

    # Linking and output, in the server

    def link(self, graph: MetadataGraph, found: dict[str, Any]) -> Any:
        """Replace what this reader put on the graph with what ``read`` found."""
        ...

    def not_analysed(self, graph: MetadataGraph, reason: str) -> None:
        """Take this reader's part out, and say why there is none."""
        ...

    def unlink(self, graph: MetadataGraph) -> None:
        """Take this reader's part out, leaving the graph as it was indexed."""
        ...
