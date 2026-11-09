"""The child processes' memory limit, and the cap on reading a DAG scan (audit LP-SEC-05)."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from lineagepilot_lsp import airflow_scan, indexing, limits, worker


def test_the_limit_scales_with_the_artifact_limit_within_the_machine():
    assert limits.memory_limit(1) >= min(limits.MIN_LIMIT, limits.physical_memory() or 1 << 62)
    total = limits.physical_memory()
    huge = limits.memory_limit(1 << 40)
    assert huge <= total * limits.MAX_SHARE if total else huge == 16 << 40


def test_the_command_carries_the_limit_for_the_server_to_watch():
    cmd = worker.command(sys.executable, {"task": "airflow", "maxArtifactBytes": 1 << 30})
    assert worker.memory_limit_of(cmd) == limits.memory_limit(1 << 30)
    assert json.loads(cmd[5])["task"] == "airflow"
    assert worker.memory_limit_of([sys.executable, "-c", "pass"]) is None


@pytest.mark.skipif(
    limits.resident_bytes(__import__("os").getpid()) is None,
    reason="resident memory is not measured on this platform",
)
async def test_the_server_stops_a_child_that_uses_too_much_memory(monkeypatch):
    monkeypatch.setattr(worker, "memory_limit_of", lambda cmd: 300 * 2**20)
    grow = (
        "import time\n"
        "blocks = []\n"
        "for _ in range(40):\n"
        "    blocks.append(bytearray(50 * 2**20))\n"  # zero-filled, so resident
        "    time.sleep(0.05)\n"
        'print(\'{"event": "done"}\')\n'
    )
    events = []

    async def on_event(event):
        events.append(event)

    exit_ = await indexing.run_worker([sys.executable, "-c", grow], on_event)
    assert exit_.over_memory == 300 * 2**20
    assert events == []
    assert "used more than 0.3 GB of memory" in exit_.stopped("the indexer")


@pytest.mark.skipif(
    not (sys.platform.startswith("linux") or sys.platform == "win32"),
    reason="macOS does not enforce a memory limit on a process; the server's watch applies",
)
def test_the_operating_system_stops_an_allocation_past_the_limit():
    code = (
        "from lineagepilot_lsp import limits\n"
        "print(limits.apply_in_child(512 * 2**20))\n"
        "try:\n"
        "    bytearray(2 * 2**30)\n"
        "    print('allocated')\n"
        "except MemoryError:\n"
        "    print('MemoryError')\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=60, check=True
    ).stdout.split()
    assert out[-1] == "MemoryError", out


def test_a_scan_over_the_cap_is_not_read(tmp_path, monkeypatch):
    path = tmp_path / "scan.json"
    path.write_text(json.dumps({"links": ["x" * 100]}))
    monkeypatch.setattr(airflow_scan, "MAX_SCAN_BYTES", 50)
    with pytest.raises(airflow_scan.ScanTooLarge):
        airflow_scan._read(path)
    monkeypatch.setattr(airflow_scan, "MAX_SCAN_BYTES", 1000)
    assert airflow_scan._read(path) == {"links": ["x" * 100]}
