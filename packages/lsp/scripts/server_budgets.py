"""Measure the language server against the design section 11 budgets, end to end over stdio.

Usage, from packages/lsp:

    uv run python scripts/server_budgets.py <dir with manifest.json and catalog.json>

A throwaway dbt project points at the artifacts. The server is started cold (empty cache)
and then warm, and timed from initialize to each index stage; CodeLens and impact requests
are timed on the warm server, and its peak memory is read from the OS. The 5,000-model
synthetic manifest comes from spike S2 (spikes/s2-parse-perf, scripts/build_manifests.py).
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

from lsprotocol import types
from pygls.lsp.client import BaseLanguageClient
from pygls.protocol.json_rpc import JsonRPCResponseMessage

from lineagepilot_lsp.server import converter


def client_converter():
    c = converter()
    c.register_structure_hook(JsonRPCResponseMessage, lambda obj, cls: cls(**obj))
    return c


def peak_rss_mb(pid: int) -> float | None:
    """The process's peak resident memory (Linux and macOS), read before it exits."""
    status = Path(f"/proc/{pid}/status")
    if status.exists():
        for line in status.read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) / 1024
    try:
        import subprocess

        out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
        return int(out.stdout.strip()) / 1024  # current, not peak, on macOS
    except (ValueError, OSError):
        return None


async def run(project: Path, storage: Path) -> dict:
    client = BaseLanguageClient("budgets", "0", converter_factory=client_converter)
    client.feature(types.WINDOW_LOG_MESSAGE)(lambda params: None)
    updates: list[tuple[float, dict]] = []
    started = time.perf_counter()

    @client.feature("lineage/indexUpdated")
    def on_update(params):
        updates.append((time.perf_counter() - started, params))

    @client.feature(types.CLIENT_REGISTER_CAPABILITY)
    def on_register(params):
        return None

    diagnostics = {"files": 0, "items": 0, "last": 0.0}

    @client.feature(types.TEXT_DOCUMENT_PUBLISH_DIAGNOSTICS)
    def on_diagnostics(params):
        diagnostics["files"] += 1
        diagnostics["items"] += len(params.diagnostics)
        diagnostics["last"] = time.perf_counter() - started

    await client.start_io(sys.executable, "-I", "-m", "lineagepilot_lsp")
    await client.initialize_async(
        types.InitializeParams(
            capabilities=types.ClientCapabilities(),
            root_uri=project.as_uri(),
            initialization_options={
                "protocolVersions": [1],
                "projectRoot": str(project),
                "trusted": True,
                "storagePath": str(storage),
            },
        )
    )
    client.initialized(types.InitializedParams())
    while not any(u["columnLineage"] == "done" and not u["indexing"] for _, u in updates):
        if any(u["state"] in ("unknown", "failed") for _, u in updates):
            raise SystemExit(f"index failed: {updates[-1][1]['message']}")
        await asyncio.sleep(0.05)
    model_stage = next((t for t, u in updates if u["state"] == "ready"), None)
    done = next(t for t, u in updates if u["columnLineage"] == "done" and not u["indexing"])
    await asyncio.sleep(2)  # let the diagnostics drain

    result = {
        "first_graph_s": round(model_stage, 2) if model_stage else None,
        "column_lineage_done_s": round(done, 2),
        "from_cache": updates[-1][1]["fromCache"],
        "nodes": updates[-1][1]["counts"]["nodes"],
        "diagnostics": diagnostics["items"],
        "diagnostic_files": diagnostics["files"],
        "diagnostics_done_s": round(diagnostics["last"], 2),
    }
    graph = await client.protocol.send_request_async("lineage/export", {"format": "json"})
    nodes = json.loads(graph["content"])["nodes"]
    models = [n for n in nodes if n["kind"] == "model" and n.get("file_path")]
    sources = [n["id"] for n in nodes if n["kind"] == "source"]

    lens_times = []
    for node in models[::50]:
        path = project / node["file_path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("select 1\n")
        uri = path.as_uri()
        t = time.perf_counter()
        await client.protocol.send_request_async(
            types.TEXT_DOCUMENT_CODE_LENS,
            types.CodeLensParams(text_document=types.TextDocumentIdentifier(uri=uri)),
        )
        lens_times.append(time.perf_counter() - t)
    result["codelens_p95_ms"] = round(sorted(lens_times)[int(len(lens_times) * 0.95)] * 1000, 2)

    impact_times = []
    for source in sources[:20]:
        t = time.perf_counter()
        await client.protocol.send_request_async("lineage/impact", {"targets": [source]})
        impact_times.append(time.perf_counter() - t)
    result["impact_max_ms"] = round(max(impact_times) * 1000, 1)
    result["impact_median_ms"] = round(statistics.median(impact_times) * 1000, 1)
    result["server_rss_mb"] = peak_rss_mb(client._server.pid)

    await client.shutdown_async(None)
    client.exit(None)
    await client.stop()
    return result


def main() -> None:
    artifacts = Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory(prefix="lp-budgets-") as tmp:
        project = Path(tmp) / "project"
        (project / "target").mkdir(parents=True)
        (project / "dbt_project.yml").write_text("name: synthetic\n")
        for name in ("manifest.json", "catalog.json"):
            os.symlink(artifacts / name, project / "target" / name)
        # Every node's files, so diagnostics are published as for a real project
        manifest = json.loads((artifacts / "manifest.json").read_text())
        for node in [*manifest["nodes"].values(), *manifest["sources"].values()]:
            for relative in (
                node.get("original_file_path"),
                (node.get("patch_path") or "").split("://")[-1],
            ):
                if relative:
                    path = project / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    if not path.exists():
                        path.write_text("select 1\n")
        storage = Path(tmp) / "storage"
        results = {"python": sys.version.split()[0]}
        results["cold"] = asyncio.run(run(project, storage))
        results["warm"] = asyncio.run(run(project, storage))
        shutil.rmtree(storage, ignore_errors=True)
    sys.stdout.write(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
