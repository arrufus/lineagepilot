"""Build bundled/libs from requirements.lock as pure Python, and fail if any part of it is not.

Usage: python scripts/bundle_libs.py [--out bundled/libs]

1. All work happens in a throwaway venv whose pip and setuptools come from the hash-pinned
   build-requirements.lock, so the user's environment is never touched.
2. Every package except PyYAML is installed with
       pip install --target OUT --implementation py --only-binary=:all: --no-deps --require-hashes
   --only-binary=:all: matters: without it pip silently builds sdists, which is how PyYAML first
   produced a macOS-only _yaml.so in a "universal" bundle during spike S1.
3. PyYAML has no pure wheel. It is built from its hash-pinned sdist with PYYAML_FORCE_LIBYAML=0
   and no build isolation, then the wheel's tag is checked before it is installed.
4. Environment markers are dropped, so the bundle is identical whichever Python builds it, and it
   includes the Python 3.10 backports (exceptiongroup) that a 3.13 build would otherwise skip.
5. The result is checked (no compiled files, every WHEEL tag pure, every locked package present)
   and BUNDLE.json records each package's version and locked hashes.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import venv
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SDIST_BUILT = {"pyyaml": {"PYYAML_FORCE_LIBYAML": "0"}}
NATIVE_SUFFIXES = (".so", ".pyd", ".dylib", ".dll")
PURE_TAG = re.compile(r"^(py2\.)?py3-none-any$")


@dataclass
class Locked:
    name: str
    version: str
    hashes: list[str]

    def requirement(self) -> str:
        hashes = " ".join(f"--hash={h}" for h in self.hashes)
        return f"{self.name}=={self.version} {hashes}"


def parse_lock(text: str) -> list[Locked]:
    """Parse a `uv pip compile --generate-hashes` lock, ignoring environment markers."""
    joined = re.sub(r"\\\n\s*", " ", text)
    packages = []
    for line in joined.splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        match = re.match(r"^([A-Za-z0-9._-]+)==([^\s;]+)", line)
        if not match:
            raise ValueError(f"unexpected lock line: {line[:80]}")
        hashes = re.findall(r"--hash=(sha256:[0-9a-f]{64})", line)
        if not hashes:
            raise ValueError(f"{match.group(1)} has no hashes; regenerate with --generate-hashes")
        packages.append(Locked(normalise(match.group(1)), match.group(2), hashes))
    return packages


def normalise(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def check_pure(libs: Path, expected: list[Locked] | None = None) -> list[str]:
    """Return every reason the directory is not a pure-Python bundle. Empty means pure."""
    problems = []
    for path in sorted(libs.rglob("*")):
        if path.is_file() and path.suffix.lower() in NATIVE_SUFFIXES:
            problems.append(f"compiled file: {path.relative_to(libs)}")
    installed = {}
    for wheel_file in sorted(libs.glob("*.dist-info/WHEEL")):
        tags = [l.split(":", 1)[1].strip() for l in wheel_file.read_text().splitlines()
                if l.startswith("Tag:")]
        impure = [t for t in tags if not PURE_TAG.match(t)]
        if impure:
            problems.append(f"impure wheel tag in {wheel_file.parent.name}: {', '.join(impure)}")
        meta = (wheel_file.parent / "METADATA").read_text(encoding="utf-8")
        name = re.search(r"^Name: (.+)$", meta, re.M).group(1).strip()
        version = re.search(r"^Version: (.+)$", meta, re.M).group(1).strip()
        installed[normalise(name)] = version
    for package in expected or []:
        got = installed.get(package.name)
        if got is None:
            problems.append(f"missing locked package: {package.name}")
        elif got != package.version:
            problems.append(f"{package.name} is {got}, lock says {package.version}")
    return problems


def run(cmd: list[str], env: dict[str, str] | None = None) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True, env={**os.environ, **(env or {})})


def venv_python(env_dir: Path) -> Path:
    scripts = "Scripts" if os.name == "nt" else "bin"
    return env_dir / scripts / ("python.exe" if os.name == "nt" else "python")


def build(out: Path) -> None:
    if sys.version_info < (3, 10):
        sys.exit("bundle_libs.py needs Python 3.10 or later")
    packages = parse_lock((ROOT / "requirements.lock").read_text())
    build_lock = ROOT / "build-requirements.lock"

    with tempfile.TemporaryDirectory(prefix="lp-bundle-") as tmp_name:
        tmp = Path(tmp_name)
        # Symlink on POSIX like `python -m venv` does: a copied python-build-standalone
        # interpreter (uv-managed Pythons) aborts on macOS.
        venv.EnvBuilder(with_pip=True, clear=True, symlinks=os.name != "nt").create(tmp / "venv")
        python = str(venv_python(tmp / "venv"))
        pip = [python, "-m", "pip", "--disable-pip-version-check", "--no-input"]
        run(pip + ["install", "--require-hashes", "--no-deps", "-r", str(build_lock)])

        pure = [p for p in packages if p.name not in SDIST_BUILT]
        pure_lock = tmp / "pure.lock"
        pure_lock.write_text("\n".join(p.requirement() for p in pure) + "\n")

        if out.exists():
            shutil.rmtree(out)
        run(pip + ["install", "--target", str(out), "--implementation", "py",
                   "--only-binary=:all:", "--no-deps", "--require-hashes", "--no-cache-dir",
                   "-r", str(pure_lock)])

        for package in (p for p in packages if p.name in SDIST_BUILT):
            one_lock = tmp / f"{package.name}.lock"
            one_lock.write_text(package.requirement() + "\n")
            wheels = tmp / "wheels" / package.name
            run(pip + ["wheel", "--no-deps", "--no-binary", package.name, "--no-build-isolation",
                       "--require-hashes", "--no-cache-dir", "-w", str(wheels), "-r", str(one_lock)],
                env=SDIST_BUILT[package.name])
            built = list(wheels.glob("*.whl"))
            if len(built) != 1 or not built[0].name.endswith("-py3-none-any.whl"):
                sys.exit(f"{package.name} did not build as a pure wheel: {[w.name for w in built]}")
            run(pip + ["install", "--target", str(out), "--no-deps", "--no-index",
                       "--no-cache-dir", str(built[0])])

    if (out / "bin").exists():
        shutil.rmtree(out / "bin")
    clean_pycache(out)

    problems = check_pure(out, packages)
    if problems:
        sys.exit("bundle is not pure Python:\n  " + "\n  ".join(problems))

    (out / "BUNDLE.json").write_text(json.dumps(
        {"packages": [{"name": p.name, "version": p.version, "hashes": p.hashes} for p in packages],
         "built_with": f"{sys.implementation.name} {sys.version.split()[0]}"},
        indent=2) + "\n")
    print(f"bundled {len(packages)} pure-Python packages into {out}")


def clean_pycache(directory: Path) -> int:
    """Remove bytecode caches that local runs leave behind, so they are never packaged."""
    caches = list(directory.rglob("__pycache__"))
    for cache in caches:
        shutil.rmtree(cache)
    return len(caches)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--out", type=Path, default=ROOT / "bundled" / "libs")
    parser.add_argument("--clean-only", action="store_true",
                        help="only remove __pycache__ under bundled/ (run before packaging)")
    args = parser.parse_args()
    if args.clean_only:
        print(f"removed {clean_pycache(ROOT / 'bundled')} __pycache__ directories")
        return
    build(args.out.resolve())


if __name__ == "__main__":
    main()
