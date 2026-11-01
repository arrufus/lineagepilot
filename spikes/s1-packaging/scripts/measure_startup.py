"""Measure language-server startup: spawn to `initialize` result, and to the first CodeLens.

Usage: python scripts/measure_startup.py --python <exe> [--python <exe> ...] [--runs 5]

"cold" deletes every __pycache__ under bundled/ first, as on a first start or a read-only
extension directory; "warm" reuses the bytecode the previous start wrote. Prints JSON.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path

from lsprotocol import types
from pytest_lsp import ClientServerConfig

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from bundle_libs import clean_pycache  # noqa: E402

SERVER = ROOT / "bundled" / "tool" / "lsp_server.py"
MODEL = ROOT / "fixture-workspace" / "models" / "orders.sql"


async def one_start(python: str) -> tuple[float, float]:
    started = time.perf_counter()
    client = await ClientServerConfig(server_command=[python, "-I", str(SERVER)]).start()
    await client.initialize_session(types.InitializeParams(capabilities=types.ClientCapabilities()))
    initialised = time.perf_counter() - started
    client.text_document_did_open(types.DidOpenTextDocumentParams(text_document=types.TextDocumentItem(
        uri=MODEL.as_uri(), language_id="sql", version=1, text=MODEL.read_text())))
    await client.text_document_code_lens_async(
        types.CodeLensParams(text_document=types.TextDocumentIdentifier(uri=MODEL.as_uri())))
    first_lens = time.perf_counter() - started
    await client.shutdown_session()
    await client.stop()
    return initialised, first_lens


async def measure(python: str, runs: int) -> dict:
    version = subprocess.run([python, "-I", "-c", "import platform; print(platform.python_version())"],
                             capture_output=True, text=True, check=True).stdout.strip()
    results = {}
    for mode in ("cold", "warm"):
        samples = []
        if mode == "warm":
            await one_start(python)  # make sure bytecode exists
        for _ in range(runs):
            if mode == "cold":
                clean_pycache(ROOT / "bundled")
            samples.append(await one_start(python))
        results[mode] = {
            "initialize_ms_median": round(statistics.median(s[0] for s in samples) * 1000),
            "first_codelens_ms_median": round(statistics.median(s[1] for s in samples) * 1000),
            "first_codelens_ms_max": round(max(s[1] for s in samples) * 1000),
        }
    clean_pycache(ROOT / "bundled")
    return {"python": version, "machine": platform.machine(), "system": platform.system(), "runs": runs, **results}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--python", action="append", required=True)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()
    print(json.dumps([asyncio.run(measure(p, args.runs)) for p in args.python], indent=2))


if __name__ == "__main__":
    main()
