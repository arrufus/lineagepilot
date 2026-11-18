"""The reader contract (design_next_phase section 7.2, task 7.3).

A reader reads one tool's files as data and links what it finds to the dbt graph. It declares:

- **Discovery**: which files are its own. A setting of relative folders or patterns, bounded,
  with defaults that find the tool's marker files (``default_paths``, ``clean_paths``); the globs
  the editor watches for it (``watch_globs``); and whether a changed file is one (``owns``).
  When nothing is found, the reader's summary says where it looked.
- **Reading**: ``read`` runs in a child process with a time limit and a memory limit, through
  ``readers.common.safe_io``. It reads only regular files inside the workspace, never runs
  anything, and so works in Restricted Mode. What it returns is plain JSON.
- **Linking**: ``link`` puts what was read into a graph, against that graph's manifest, and
  ``unlink`` takes it out again, leaving the dbt graph as it was indexed.
- **Output**: nodes of its ``kinds``, edges with evidence, and named unknowns at the line that
  caused them, with the annotation that answers each, under its ``origins``; and a summary of what
  was read, in ``graph.readers[tool]``, which ``summary_json`` writes as the protocol's
  ``ReaderSummary``.
  ``mark_not_analysed`` says why a graph has nothing from the reader, so nobody reads its absence
  as "nothing is affected".
- **Lifecycle**: what a reader adds is composed on top of the graph and never stored in a
  snapshot. The caller reads again when the reader's files or the annotation file change, or a
  new manifest arrives, and lets the result go once it is linked.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from lineagepilot_core.graph.model import MetadataGraph, NodeKind, Origin


@runtime_checkable
class Reader(Protocol):
    """One tool's reader. Implementations hold no state: the caller holds what was read."""

    # The tool, as in a node's ``tool`` and an ID's prefix: ``airflow``
    tool: str
    # The client's option for the reader's paths: ``dagFolders``, from the setting
    # lineagePilot.airflow.dagFolders
    option: str
    # Where it looks when the setting is not given
    default_paths: tuple[str, ...]
    # The files, in a sentence: "the DAG files are still being read"
    files: str
    # What it adds to the graph: the kinds of node, and the origins of its evidence and unknowns
    kinds: frozenset[NodeKind]
    origins: frozenset[Origin]

    # Discovery

    def clean_paths(self, paths: Iterable[Any]) -> list[str]:
        """The setting's relative folders or patterns that are safe to use; the rest dropped."""
        ...

    def watch_globs(self, paths: Sequence[str]) -> list[str]:
        """Globs, relative to the workspace, for the editor to watch."""
        ...

    def owns(self, relative: str, paths: Sequence[str]) -> bool:
        """Whether a file, relative to the workspace, is one the reader reads."""
        ...

    # Reading, in a child process

    def read(
        self,
        project_root: str | Path,
        workspace: str | Path,
        paths: Sequence[str],
        manifest: str | Path | None,
    ) -> dict[str, Any]:
        """Read the files for this project, as JSON. Nothing in them is ever run."""
        ...

    # Linking and output

    def link(self, graph: MetadataGraph, scan: dict[str, Any]) -> Any:
        """Replace what an earlier read added to ``graph`` with this one's, and return its summary,
        which is also in ``graph.readers[tool]``."""
        ...

    def unlink(self, graph: MetadataGraph) -> None:
        """Take out everything the reader added."""
        ...

    def mark_not_analysed(self, graph: MetadataGraph, reason: str) -> None:
        """Take out what the reader added, and say why there is nothing from it."""
        ...

    def summary_json(self, graph: MetadataGraph | None) -> dict[str, Any]:
        """What its last read found in ``graph``, as the protocol's ``ReaderSummary``; not
        analysed, with the reason, when nothing has been read into it."""
        ...
