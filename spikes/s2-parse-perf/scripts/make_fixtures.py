"""Make the small checked-in test fixtures from real dbt output.

- tests/fixtures/synth-24/: a 24-model synthetic project built with dbt Core
  1.12 (parse, run, docs generate), so it has real test nodes, a versioned
  model, exposures, compiled SQL and a real catalog.json.
- tests/fixtures/jaffle-1.12/: jaffle-shop docs-generate output from dbt 1.12.
- tests/fixtures/jaffle-fusion-2.0/: jaffle-shop compile output from dbt v2.0.9.

The ``macros`` key is emptied to keep each fixture under 1 MB. DbtParser
never reads macros, so this does not change what it sees.

Needs scripts/setup_dbt_envs.py, and build_manifests.py jaffle /
fusion_attempt.py to have run first.

    uv run python scripts/make_fixtures.py
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE / "scripts"))
import build_manifests as B  # noqa: E402

FIX = HERE / "tests" / "fixtures"


def strip(src: Path, dest: Path) -> int:
    m = json.loads(src.read_bytes())
    if "macros" in m:
        m["macros"] = {}
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(m, separators=(",", ":"), sort_keys=True))
    return dest.stat().st_size


def main() -> None:
    prov: dict[str, dict] = {}
    proj = B.PROJECTS / "fixture-synth-24"
    subprocess.run([sys.executable, str(HERE / "scripts" / "gen_project.py"), "--models", "24",
                    "--out", str(proj), "--seed", "3"], check=True, capture_output=True)
    B.PROFILES.mkdir(parents=True, exist_ok=True)
    (B.PROFILES / "profiles.yml").write_text(B.PROFILES_YML)
    db = proj / "fixture.duckdb"
    py = B.ENVS / "dbt-1.12" / "bin" / "python"
    subprocess.run([str(py), "-I", "-c",
                    "import duckdb,sys; c=duckdb.connect(sys.argv[1]); c.execute(sys.stdin.read()); c.close()",
                    str(db)], input=(proj / "raw_tables.sql").read_text(), text=True, check=True)
    for args in (["seed"], ["run"], ["docs", "generate"]):
        if B.dbt("1.12", proj, args, "target", db)["rc"] != 0:
            raise SystemExit(f"dbt {args} failed")
    out = FIX / "synth-24"
    size = strip(proj / "target" / "manifest.json", out / "manifest.json")
    (out / "catalog.json").write_text((proj / "target" / "catalog.json").read_text())
    prov["synth-24"] = {"generator": "scripts/gen_project.py --models 24 --seed 3", "dbt": "1.12.6 + dbt-duckdb 1.11.1",
                        "steps": "seed, run, docs generate", "manifest_bytes": size}
    jd = B.MANIFESTS / "jaffle" / "1.12" / "docs"
    size = strip(jd / "manifest.json", FIX / "jaffle-1.12" / "manifest.json")
    (FIX / "jaffle-1.12" / "catalog.json").write_text((jd / "catalog.json").read_text())
    prov["jaffle-1.12"] = {"repo": B.JAFFLE_URL, "sha": B.JAFFLE_SHA, "dbt": "1.12.6 + dbt-duckdb 1.11.1",
                           "steps": "deps, seed, run, docs generate", "manifest_bytes": size}
    fd = HERE / "work" / "manifests" / "fusion-jaffle" / "2.0" / "compile"
    size = strip(fd / "manifest.json", FIX / "jaffle-fusion-2.0" / "manifest.json")
    prov["jaffle-fusion-2.0"] = {"repo": B.JAFFLE_URL, "sha": "5beb145b00f5465ec759cfcdd9745e858818cf95",
                                 "dbt": "2.0.9 (Fusion), in Docker", "steps": "deps, seed, compile",
                                 "manifest_bytes": size}
    for k, v in prov.items():
        v["macros"] = "emptied"
    (FIX / "PROVENANCE.json").write_text(json.dumps(prov, indent=1))
    print(json.dumps(prov, indent=1))


if __name__ == "__main__":
    main()
