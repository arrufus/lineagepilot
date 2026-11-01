"""Produce real dbt Core manifests for the spike. Everything lands under work/.

Steps (each is skipped if its output already exists, unless --force):

1. jaffle-shop at a pinned SHA: dbt deps, parse, compile; on every Core minor.
   Then seed + run + docs generate for a real catalog.json.
2. Synthetic 2,000- and 5,000-model projects from scripts/gen_project.py:
   dbt parse on every minor; dbt compile on 1.8, 1.10 and 1.12;
   on 1.12 also create the raw source tables in DuckDB, then seed + run +
   docs generate for a real catalog.json and compiled SQL.

dbt runs only from the venvs made by setup_dbt_envs.py, with a dummy
profiles.yml under work/profiles (DuckDB files, no credentials).

    uv run python scripts/build_manifests.py            # everything
    uv run python scripts/build_manifests.py jaffle     # one target
    uv run python scripts/build_manifests.py synth-2000 synth-5000
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
WORK = HERE / "work"
ENVS = WORK / "dbt-envs"
PROFILES = WORK / "profiles"
PROJECTS = WORK / "projects"
MANIFESTS = WORK / "manifests"
JAFFLE_REPO = WORK / "src" / "jaffle-shop"
JAFFLE_URL = "https://github.com/dbt-labs/jaffle-shop.git"
# Last commit whose dbt_project.yml accepts dbt Core 1.8 (require-dbt-version
# ">=1.5.0") and uses flat generic-test arguments. Later commits require
# >=1.12 (2026-07-23) and then >=2.0.0, i.e. dbt v2 / Fusion (2026-09-15).
JAFFLE_SHA = "7be2c5838dbdeca8e915d4e46db70e910753d7f6"
MINORS = ["1.8", "1.9", "1.10", "1.11", "1.12"]
COMPILE_MINORS = ["1.8", "1.10", "1.12"]
CATALOG_MINOR = "1.12"

PROFILES_YML = """\
# Dummy profiles for spike S2. Local DuckDB files only; no credentials.
default:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('S2_DUCKDB_PATH') }}"
      threads: 8
synthetic:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('S2_DUCKDB_PATH') }}"
      threads: 8
"""

LOG: list[dict] = []


def dbt(minor: str, project: Path, args: list[str], target: str, duckdb: Path) -> dict:
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(PROFILES),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "DO_NOT_TRACK": "1",
        "S2_DUCKDB_PATH": str(duckdb),
    }
    env.pop("VIRTUAL_ENV", None)
    cmd = [str(ENVS / f"dbt-{minor}" / "bin" / "dbt"), *args, "--project-dir", str(project),
           "--log-path", f"logs-{minor}", "--no-version-check"]
    if args[0] != "deps":  # deps has no --target-path
        cmd += ["--target-path", target]
    if args[0] in ("parse", "compile", "run", "seed", "docs"):
        cmd.append("--no-partial-parse")
    t0 = time.perf_counter()
    p = subprocess.run(cmd, cwd=project, env=env, text=True, capture_output=True)
    dt = time.perf_counter() - t0
    out = p.stdout + p.stderr
    rec = {"minor": minor, "project": project.name, "cmd": " ".join(args), "rc": p.returncode,
           "seconds": round(dt, 2),
           "warn_lines": sum(1 for l in out.splitlines() if "[WARNING]" in l or " Warning" in l or "Deprecat" in l),
           "tail": out.strip().splitlines()[-4:]}
    LOG.append(rec)
    print(f"  dbt-{minor} {' '.join(args)} -> rc={p.returncode} {dt:.1f}s", flush=True)
    (project / f"logs-{minor}").mkdir(exist_ok=True)
    (project / f"logs-{minor}" / f"{args[0]}-{args[1] if len(args) > 1 and not args[1].startswith('-') else 'x'}.out").write_text(out)
    if p.returncode != 0:
        print("\n".join(out.strip().splitlines()[-25:]))
    return rec


def save(project: Path, target: str, dest: Path, with_catalog: bool = False) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    shutil.copy2(project / target / "manifest.json", dest / "manifest.json")
    if with_catalog and (project / target / "catalog.json").exists():
        shutil.copy2(project / target / "catalog.json", dest / "catalog.json")


def build_jaffle(force: bool) -> None:
    if not JAFFLE_REPO.exists():
        JAFFLE_REPO.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "clone", "--quiet", JAFFLE_URL, str(JAFFLE_REPO)], check=True)
    for minor in MINORS:
        dest = MANIFESTS / "jaffle" / minor
        if (dest / "compile" / "manifest.json").exists() and not force:
            continue
        proj = PROJECTS / f"jaffle-{minor}"
        if proj.exists():
            shutil.rmtree(proj)
        proj.mkdir(parents=True)
        arch = subprocess.run(["git", "-C", str(JAFFLE_REPO), "archive", JAFFLE_SHA], check=True, capture_output=True).stdout
        subprocess.run(["tar", "-x", "-C", str(proj)], input=arch, check=True)
        db = proj / "jaffle.duckdb"
        print(f"jaffle-shop @ {JAFFLE_SHA[:10]} with dbt {minor}")
        dbt(minor, proj, ["deps"], "target", db)
        if dbt(minor, proj, ["parse"], "target", db)["rc"] == 0:
            save(proj, "target", dest / "parse")
        if dbt(minor, proj, ["compile"], "target", db)["rc"] == 0:
            save(proj, "target", dest / "compile")
        vars_ = ["--vars", '{"load_source_data": true}']
        dbt(minor, proj, ["seed", *vars_], "target", db)
        dbt(minor, proj, ["run", *vars_], "target", db)
        if dbt(minor, proj, ["docs", "generate", *vars_], "target", db)["rc"] == 0:
            save(proj, "target", dest / "docs", with_catalog=True)


def build_synth(n: int, force: bool) -> None:
    proj = PROJECTS / f"synth-{n}"
    if not proj.exists() or force:
        subprocess.run([sys.executable, str(HERE / "scripts" / "gen_project.py"),
                        "--models", str(n), "--out", str(proj)], check=True)
    for minor in MINORS:
        dest = MANIFESTS / f"synth-{n}" / minor
        db = proj / f"synth_{minor.replace('.', '_')}.duckdb"
        target = f"target-{minor}"
        print(f"synth-{n} with dbt {minor}")
        if not (dest / "parse" / "manifest.json").exists() or force:
            if dbt(minor, proj, ["parse"], target, db)["rc"] == 0:
                save(proj, target, dest / "parse")
        if minor in COMPILE_MINORS and (not (dest / "compile" / "manifest.json").exists() or force):
            if dbt(minor, proj, ["compile"], target, db)["rc"] == 0:
                save(proj, target, dest / "compile")
        if minor == CATALOG_MINOR and (not (dest / "docs" / "catalog.json").exists() or force):
            ddl = (proj / "raw_tables.sql").read_text()
            py = ENVS / f"dbt-{minor}" / "bin" / "python"
            subprocess.run([str(py), "-I", "-c",
                            "import duckdb,sys; c=duckdb.connect(sys.argv[1]); c.execute(sys.stdin.read()); c.close()",
                            str(db)], input=ddl, text=True, check=True)
            dbt(minor, proj, ["seed"], target, db)
            dbt(minor, proj, ["run"], target, db)
            if dbt(minor, proj, ["docs", "generate"], target, db)["rc"] == 0:
                save(proj, target, dest / "docs", with_catalog=True)


def main(targets: list[str]) -> None:
    force = "--force" in targets
    targets = [t for t in targets if t != "--force"] or ["jaffle", "synth-2000", "synth-5000"]
    PROFILES.mkdir(parents=True, exist_ok=True)
    (PROFILES / "profiles.yml").write_text(PROFILES_YML)
    for t in targets:
        if t == "jaffle":
            build_jaffle(force)
        elif t.startswith("synth-"):
            build_synth(int(t.split("-")[1]), force)
        else:
            raise SystemExit(f"unknown target {t}")
    log_path = MANIFESTS / "dbt_runs.json"
    prior = json.loads(log_path.read_text()) if log_path.exists() else []
    log_path.write_text(json.dumps(prior + LOG, indent=1))


if __name__ == "__main__":
    main(sys.argv[1:])
