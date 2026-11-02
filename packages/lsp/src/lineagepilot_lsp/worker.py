"""The indexer, run in a child process (design section 11).

Parsing 5,000 models stalls an event loop for about 2 s, a thread still leaves stalls of
100-520 ms, and the parse peaks near 1 GB (spike S2). So the server never indexes in its
own process: it starts this module, which indexes the project with lineagepilot_core,
writes snapshots to the cache directory, and reports on stdout, one JSON object a line:

    {"event": "model", ...}   the model-level graph is in "snapshot"; column lineage next
    {"event": "done", ...}    finished: "status" is "ok" (the full graph is in
                              "snapshot") or "unknown" (see "errors")
    {"event": "failed", ...}  an unexpected error; details on stderr

stdout carries nothing else. Logging goes to stderr, which the server forwards.
"""

from __future__ import annotations

import json
import logging
import sys
import traceback
from typing import Any, TextIO

from lineagepilot_core.indexer import DEFAULT_MAX_ARTIFACT_BYTES, IndexResult, index_project
from lineagepilot_core.snapshot import SnapshotCache

# Started as: python -I -c BOOTSTRAP <server's sys.path as JSON> <request as JSON>.
# -I keeps the workspace, the user site-packages and PYTHON* variables off the path, as
# for the server; the server's own path (bundled/libs first) is then restored exactly.
BOOTSTRAP = (
    "import json, sys; sys.path[:] = json.loads(sys.argv[1]); "
    "from lineagepilot_lsp.worker import main; sys.exit(main(json.loads(sys.argv[2])))"
)

# A model-level and a full snapshot per artifact version, and the previous version's
# pair as a baseline.
CACHE_KEEP = 4


def command(python: str, request: dict[str, Any]) -> list[str]:
    return [python, "-I", "-c", BOOTSTRAP, json.dumps(sys.path), json.dumps(request)]


def describe(result: IndexResult) -> dict[str, Any]:
    return {
        "status": result.status.value,
        "snapshot": str(result.snapshot) if result.snapshot else None,
        "fromCache": result.from_cache,
        "projectName": result.project_name,
        "dbtVersion": result.dbt_version,
        "schemaVersion": result.schema_version,
        "errors": [
            {"severity": e.severity.value, "message": e.message, "location": e.location}
            for e in result.errors
        ],
        "seconds": {k: round(v, 3) for k, v in result.seconds.items()},
    }


def main(request: dict[str, Any], out: TextIO | None = None) -> int:
    """Index one project. ``request`` keys: projectDir, cacheDir, manifest, catalog,
    columnLineage, maxArtifactBytes."""
    out = out or sys.stdout
    logging.basicConfig(
        stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    def emit(event: dict[str, Any]) -> None:
        out.write(json.dumps(event) + "\n")
        out.flush()

    try:
        result = index_project(
            request["projectDir"],
            manifest=request.get("manifest"),
            catalog=request.get("catalog"),
            cache=SnapshotCache(request["cacheDir"], keep=CACHE_KEEP),
            column_lineage=request.get("columnLineage", True),
            on_model_graph=lambda stage: emit({"event": "model", **describe(stage)}),
            max_artifact_bytes=request.get("maxArtifactBytes", DEFAULT_MAX_ARTIFACT_BYTES),
        )
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        emit({"event": "failed", "message": f"{type(e).__name__}: {e}"})
        return 1
    emit({"event": "done", **describe(result)})
    return 0
