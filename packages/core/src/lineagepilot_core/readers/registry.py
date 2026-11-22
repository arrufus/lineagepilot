"""Every reader, by tool (task 7.3). A reader is added here in the phase that builds it.

The set is closed: a reader is never named by a workspace, a setting or a request, only looked up
here by the tool's name.
"""

from __future__ import annotations

from lineagepilot_core.readers.airflow.reader import AirflowReader
from lineagepilot_core.readers.contract import Reader
from lineagepilot_core.readers.dagster.reader import DagsterReader

READERS: dict[str, Reader] = {reader.tool: reader for reader in (AirflowReader(), DagsterReader())}


def reader(tool: str) -> Reader:
    """The reader for a tool; ``KeyError`` for a tool with none."""
    return READERS[tool]
