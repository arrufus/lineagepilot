"""Command line: ``python -m lineagepilot_spike_dag scan <dir> [--manifest PATH] [--workspace DIR]``.

Prints JSON. Never imports or executes the scanned files.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Sequence

from .manifest import ManifestError, load_manifest
from .scanner import scan
from .selector import select


def _git_root(path: str) -> str | None:
    d = os.path.realpath(path if os.path.isdir(path) else os.path.dirname(path))
    while True:
        if os.path.exists(os.path.join(d, ".git")):
            return d
        nd = os.path.dirname(d)
        if nd == d:
            return None
        d = nd


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="lineagepilot_spike_dag", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("scan", help="scan a DAG folder (or file) and print task-to-dbt-model links as JSON")
    sp.add_argument("paths", nargs="+")
    sp.add_argument("--manifest", action="append", default=[], help="dbt manifest.json to use for its project (repeatable)")
    sp.add_argument("--workspace", help="workspace root used for imports and dbt project discovery (default: git root or the scan dir)")
    sp.add_argument("--no-safe-mode", action="store_true", help="scan every .py file, not only those mentioning 'dag' and 'airflow'")
    sp.add_argument("--output", "-o", help="write JSON here instead of stdout")
    sl = sub.add_parser("select", help="evaluate a dbt selection over a manifest (models only)")
    sl.add_argument("manifest")
    sl.add_argument("--select", "-s", nargs="*", default=[])
    sl.add_argument("--exclude", nargs="*", default=[])
    sl.add_argument("--selector")
    sl.add_argument("--resource-type", action="append", default=None)
    args = ap.parse_args(argv)

    if args.cmd == "scan":
        ws = args.workspace or _git_root(args.paths[0]) or (args.paths[0] if os.path.isdir(args.paths[0]) else os.path.dirname(args.paths[0]))
        result = scan(args.paths, workspace=ws, manifests=args.manifest, safe_mode=not args.no_safe_mode)
        text = json.dumps(result, indent=1, sort_keys=False, default=str)
    else:
        try:
            m = load_manifest(args.manifest)
        except ManifestError as exc:
            print(json.dumps({"error": str(exc)}), file=sys.stderr)
            return 2
        res = select(m, args.select, args.exclude, args.selector, resource_types=args.resource_type or ("model",))
        text = json.dumps({"ids": None if res.unknown else sorted(res.ids), "unknown_reason": res.reason, "notes": res.notes}, indent=1)  # type: ignore[arg-type]
    if getattr(args, "output", None):
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
    else:
        print(text)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
