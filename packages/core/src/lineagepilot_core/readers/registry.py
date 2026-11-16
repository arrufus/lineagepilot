"""The readers the language server runs, in order (task 7.3). Each later phase adds its own."""

from __future__ import annotations

from lineagepilot_core.readers.airflow.reader import READER as AIRFLOW_READER
from lineagepilot_core.readers.contract import Reader

READERS: tuple[Reader, ...] = (AIRFLOW_READER,)


def reader_for(tool: str) -> Reader:
    for reader in READERS:
        if reader.tool == tool:
            return reader
    raise KeyError(f"no reader for {tool!r}")
