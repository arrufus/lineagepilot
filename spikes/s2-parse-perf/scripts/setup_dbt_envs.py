"""Create one isolated venv per dbt Core minor under work/dbt-envs/.

dbt only ever runs from these venvs. They use a uv-managed CPython 3.11
(dbt 1.8 does not support 3.13), never the Homebrew x86_64 builds.

    uv run python scripts/setup_dbt_envs.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
ENVS = HERE / "work" / "dbt-envs"

# dbt-core version -> dbt-duckdb version. Adapters are decoupled from core since
# 1.8, so 1.12 pairs with the newest dbt-duckdb (1.11.1; there is no 1.12).
PAIRS = {
    "1.8": ("1.8.10", "1.8.4"),
    "1.9": ("1.9.11", "1.9.6"),
    "1.10": ("1.10.23", "1.10.1"),
    "1.11": ("1.11.16", "1.11.1"),
    "1.12": ("1.12.6", "1.11.1"),
}


def run(cmd: list[str], **kw) -> str:
    env = {**os.environ, "UV_PYTHON_PREFERENCE": "only-managed"}
    return subprocess.run(cmd, check=True, text=True, capture_output=True, env=env, **kw).stdout


def main(minors: list[str]) -> None:
    ENVS.mkdir(parents=True, exist_ok=True)
    run(["uv", "python", "install", "3.11"])
    for minor in minors or list(PAIRS):
        core, duck = PAIRS[minor]
        d = ENVS / f"dbt-{minor}"
        py = d / "bin" / "python"
        if not (d / "bin" / "dbt").exists():
            run(["uv", "venv", "--quiet", "--python", "3.11", str(d)])
            run(["uv", "pip", "install", "--quiet", "--python", str(py),
                 f"dbt-core=={core}", f"dbt-duckdb=={duck}"])
        (d / "freeze.txt").write_text(run(["uv", "pip", "freeze", "--python", str(py)]))
        ver = run([str(d / "bin" / "dbt"), "--version"]).splitlines()
        arch = run([str(py), "-c", "import platform;print(platform.machine())"]).strip()
        print(f"{d.name}: core={core} duckdb={duck} arch={arch} | {' '.join(v.strip() for v in ver[:6])}")


if __name__ == "__main__":
    main(sys.argv[1:])
