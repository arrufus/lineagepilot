"""Run dbt v2 (Fusion engine) ONLY inside Docker and collect its manifests.

    docker build -t s2-fusion:latest scripts/fusion
    uv run python scripts/fusion_attempt.py

Projects (prepared under work/fusion/, mounted at /work):
- jaffle-head: dbt-labs/jaffle-shop at JAFFLE_V2_SHA (requires dbt >= 2.0.0);
- synth-2000, synth-5000: the synthetic generator with --yaml-style v2
  (Fusion requires generic-test ``arguments:``).

Profiles are dummies: DuckDB files, plus a Snowflake profile with fake
credentials to check whether ``parse`` works without a connection.
Manifests are copied to work/manifests/fusion-*/2.0/<step>/ so spike.versions
picks them up. A summary goes to results/fusion_attempt.json.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
W = HERE / "work" / "fusion"
JAFFLE_REPO = HERE / "work" / "src" / "jaffle-shop"
JAFFLE_V2_SHA = "5beb145b00f5465ec759cfcdd9745e858818cf95"
IMAGE = "s2-fusion:latest"
SYNTH_SIZES = (2000, 5000)
PROFILES = """\
# Dummy profiles for the spike S2 Fusion attempt. No real credentials.
default:
  target: dev
  outputs:
    dev: {type: duckdb, path: /work/jaffle.duckdb, threads: 8}
    sf:
      type: snowflake
      account: dummy-account
      user: dummy
      password: dummy
      role: dummy
      database: dummy_db
      warehouse: dummy_wh
      schema: dummy_schema
      threads: 8
synthetic:
  target: dev
  outputs:
    dev: {type: duckdb, path: /work/synth.duckdb, threads: 8}
    sf:
      type: snowflake
      account: dummy-account
      user: dummy
      password: dummy
      role: dummy
      database: dummy_db
      warehouse: dummy_wh
      schema: dummy_schema
      threads: 8
"""


def docker(project: str, cmd: str) -> dict:
    stale = W / project / "target" / "manifest.json"
    if stale.exists():
        stale.unlink()  # never keep a manifest from an earlier, different run
    t = time.perf_counter()
    p = subprocess.run(["docker", "run", "--rm", "-v", f"{W}:/work", "-w", f"/work/{project}", IMAGE,
                        "bash", "-lc", cmd], capture_output=True, text=True)
    out = (p.stdout + p.stderr).strip().splitlines()
    rec = {"project": project, "cmd": cmd, "rc": p.returncode, "wall_s": round(time.perf_counter() - t, 2),
           "tail": [l for l in out if l.strip()][-8:]}
    print(f"{project}: {cmd} -> rc={p.returncode} {rec['wall_s']}s", flush=True)
    LAST_RC[project] = p.returncode
    return rec


LAST_RC: dict[str, int] = {}


def keep(project: str, label: str, step: str) -> str | None:
    src = W / project / "target" / "manifest.json"
    if LAST_RC.get(project) != 0 or not src.exists():
        return None
    dest = HERE / "work" / "manifests" / label / "2.0" / step
    dest.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest / "manifest.json")
    m = json.loads(src.read_bytes())
    return m["metadata"].get("dbt_schema_version")


def main() -> None:
    if W.exists():
        shutil.rmtree(W)
    (W / "profiles").mkdir(parents=True)
    (W / "profiles" / "profiles.yml").write_text(PROFILES)
    (W / "jaffle-head").mkdir()
    arch = subprocess.run(["git", "-C", str(JAFFLE_REPO), "archive", JAFFLE_V2_SHA], check=True,
                          capture_output=True).stdout
    subprocess.run(["tar", "-x", "-C", str(W / "jaffle-head")], input=arch, check=True)
    for n in SYNTH_SIZES:
        subprocess.run([sys.executable, str(HERE / "scripts" / "gen_project.py"), "--models", str(n),
                        "--out", str(W / f"synth-{n}"), "--yaml-style", "v2"], check=True, capture_output=True)
    version = subprocess.run(["docker", "run", "--rm", IMAGE, "dbt", "--version"],
                             capture_output=True, text=True).stdout.strip()
    arch_ = subprocess.run(["docker", "run", "--rm", IMAGE, "uname", "-m"], capture_output=True, text=True).stdout.strip()
    runs = []
    schema: dict[str, str | None] = {}
    P = "--profiles-dir /work/profiles"
    runs.append(docker("jaffle-head", f"dbt deps {P}"))
    runs.append(docker("jaffle-head", f"dbt parse {P}"))
    schema["jaffle parse (duckdb)"] = keep("jaffle-head", "fusion-jaffle", "parse")
    runs.append(docker("jaffle-head", f"dbt parse {P} --target sf"))
    schema["jaffle parse (dummy snowflake creds, no connection)"] = keep("jaffle-head", "fusion-jaffle", "parse-sf")
    vars_ = "--vars '{\"load_source_data\": true}'"
    runs.append(docker("jaffle-head", f"dbt seed {P} {vars_}"))
    runs.append(docker("jaffle-head", f"dbt compile {P} {vars_}"))
    schema["jaffle compile (duckdb, seeded)"] = keep("jaffle-head", "fusion-jaffle", "compile")
    for n in SYNTH_SIZES:
        s = f"synth-{n}"
        runs.append(docker(s, f"dbt parse {P}"))
        schema[f"{s} parse (duckdb)"] = keep(s, f"fusion-{s}", "parse")
        runs.append(docker(s, f"dbt parse {P} --target sf"))
        schema[f"{s} parse (dummy snowflake creds, no connection)"] = keep(s, f"fusion-{s}", "parse-sf")
        runs.append(docker(s, f"dbt compile {P} --static-analysis off"))
        schema[f"{s} compile (duckdb, static analysis off)"] = keep(s, f"fusion-{s}", "compile")
    out = {"image": IMAGE, "dbt_version_output": version, "container_arch": arch_,
           "jaffle_sha": JAFFLE_V2_SHA, "runs": runs, "dbt_schema_version": schema}
    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "results" / "fusion_attempt.json").write_text(json.dumps(out, indent=1))
    print(json.dumps(schema, indent=1))


if __name__ == "__main__":
    main()
