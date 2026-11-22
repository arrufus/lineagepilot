"""The analytics project's definitions, as `dg scaffold project` writes them: everything in defs/.

Read by LineagePilot as syntax only. Nothing here is ever imported, so dagster need not be
installed.
"""

from pathlib import Path

from dagster import definitions, load_from_defs_folder


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
