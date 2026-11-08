"""Write THIRD_PARTY_NOTICES.md: the licence of every package the VSIX carries code from (task 6.2).

Usage: python scripts/third_party_notices.py [--check]

esbuild bundles npm packages into dist/extension.js and the webview scripts. That keeps their code
but drops their licence files, and the MIT, ISC, BSD and Blue Oak licences ask for the notice to go
with the code. esbuild.mjs writes dist/bundled-inputs.json, the files each bundle read, which says
which packages those are. The Python libraries in bundled/libs keep their licence files in their
dist-info folders; they are listed here as well, so that one file covers everything that ships.

A licence that is not on the ALLOWED list fails the build: shipping one is a decision to make, not
a side effect of updating a dependency. --check reports whether the file is up to date.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from email.parser import Parser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INPUTS = ROOT / "dist" / "bundled-inputs.json"
LIBS = ROOT / "bundled" / "libs"
OUT = ROOT / "THIRD_PARTY_NOTICES.md"

# Permissive licences whose terms a notice file meets. Anything else needs a decision first.
ALLOWED = {"MIT", "ISC", "BSD-2-Clause", "BSD-3-Clause", "0BSD", "Apache-2.0", "BlueOak-1.0.0", "PSF-2.0"}
# Trove classifiers, for packages whose metadata has no licence expression
CLASSIFIERS = {
    "License :: OSI Approved :: MIT License": "MIT",
    "License :: OSI Approved :: Apache Software License": "Apache-2.0",
    "License :: OSI Approved :: ISC License (ISCL)": "ISC",
    "License :: OSI Approved :: Python Software Foundation License": "PSF-2.0",
}
LICENCE_FILE = re.compile(r"^(licen[cs]e|copying|notice)([.-].*)?$", re.IGNORECASE)
PACKAGE_DIR = re.compile(r"^(.*node_modules/(?:@[^/]+/)?[^/]+)/")
# Our own packages, under LICENSE
OURS = {"lineagepilot-core", "lineagepilot-lsp"}


@dataclass
class Package:
    ecosystem: str
    name: str
    version: str
    licence: str
    texts: list[tuple[str, str]] = field(default_factory=list)


def _licence_texts(directory: Path) -> list[tuple[str, str]]:
    return [(p.name, p.read_text(encoding="utf-8", errors="replace").strip())
            for p in sorted(directory.iterdir()) if p.is_file() and LICENCE_FILE.match(p.name)]


def npm_packages(inputs: list[str], root: Path = ROOT) -> list[Package]:
    """The npm packages that any bundle read a file from, with their licences."""
    directories = sorted({m.group(1) for i in inputs if (m := PACKAGE_DIR.match(i.replace("\\", "/")))})
    packages = []
    for relative in directories:
        directory = root / relative
        meta = json.loads((directory / "package.json").read_text(encoding="utf-8"))
        licence = meta.get("license")
        if isinstance(licence, dict):
            licence = licence.get("type")
        packages.append(Package("npm", meta["name"], meta["version"], str(licence or ""), _licence_texts(directory)))
    return packages


def _python_licence(meta) -> str:
    if meta.get("License-Expression"):
        return meta["License-Expression"].strip()
    short = (meta.get("License") or "").strip()
    if short in ALLOWED:
        return short
    for classifier in meta.get_all("Classifier") or []:
        if classifier in CLASSIFIERS:
            return CLASSIFIERS[classifier]
    return short


def python_packages(libs: Path = LIBS) -> list[Package]:
    """The Python packages in the bundle, ours excepted, with their licences."""
    packages = []
    for info in sorted(libs.glob("*.dist-info")):
        meta = Parser().parsestr((info / "METADATA").read_text(encoding="utf-8"), headersonly=True)
        name = meta["Name"].strip()
        if re.sub(r"[-_.]+", "-", name).lower() in OURS:
            continue
        texts = _licence_texts(info)
        if (info / "licenses").is_dir():
            texts += [(p.relative_to(info / "licenses").as_posix(), p.read_text(encoding="utf-8", errors="replace").strip())
                      for p in sorted((info / "licenses").rglob("*")) if p.is_file()]
        packages.append(Package("PyPI", name, meta["Version"].strip(), _python_licence(meta), texts))
    return packages


def problems(packages: list[Package]) -> list[str]:
    found = []
    for package in packages:
        if package.licence not in ALLOWED:
            found.append(f"{package.ecosystem} {package.name} {package.version}: licence {package.licence or 'unknown'!r} "
                         "is not on the allowed list in scripts/third_party_notices.py")
        if not package.texts:
            found.append(f"{package.ecosystem} {package.name} {package.version}: no licence file to include")
    return found


def _fence(text: str) -> str:
    longest = max((len(run) for run in re.findall(r"`+", text)), default=0)
    return "`" * max(3, longest + 1)


def render(npm: list[Package], python: list[Package]) -> str:
    lines = [
        "# Third-party notices",
        "",
        "LineagePilot is released under the MIT licence, in `LICENSE`. It includes code from the packages below, "
        "each under its own licence, whose text follows. This file is generated by `scripts/third_party_notices.py` "
        "when the VSIX is built.",
        "",
    ]
    sections = [
        ("npm packages bundled into the extension and its views", npm),
        ("Python packages in `bundled/libs`, which run the language server", python),
    ]
    for title, packages in sections:
        lines += [f"## {title}", "", "| Package | Version | Licence |", "|---|---|---|"]
        lines += [f"| `{p.name}` | {p.version} | {p.licence} |" for p in packages]
        lines.append("")
    lines += ["## Licence texts", ""]
    for package in [*npm, *python]:
        lines += [f"### {package.name} {package.version} ({package.licence})", ""]
        for filename, text in package.texts:
            fence = _fence(text)
            lines += [f"`{filename}`:", "", f"{fence}text", text, fence, ""]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="only report whether the notices are up to date")
    args = parser.parse_args()
    if not INPUTS.is_file():
        sys.exit(f"{INPUTS.relative_to(ROOT)} is missing: run node esbuild.mjs first")
    if not LIBS.is_dir():
        sys.exit(f"{LIBS.relative_to(ROOT)} is missing: run python scripts/bundle_libs.py first")
    npm = npm_packages(json.loads(INPUTS.read_text(encoding="utf-8")))
    python = python_packages()
    found = problems([*npm, *python])
    if found:
        sys.exit("third-party licence problems:\n  " + "\n  ".join(found))
    text = render(npm, python)
    if args.check:
        if not OUT.is_file() or OUT.read_text(encoding="utf-8") != text:
            sys.exit("THIRD_PARTY_NOTICES.md is out of date: run python scripts/third_party_notices.py")
        print("THIRD_PARTY_NOTICES.md is up to date")
        return
    OUT.write_text(text, encoding="utf-8", newline="\n")
    print(f"Wrote {OUT.name}: {len(npm)} npm and {len(python)} Python packages")


if __name__ == "__main__":
    main()
