"""Approximate stand-in manifest for a dbt project that has no ``target/manifest.json``.

Built from text only, without running dbt or its Jinja:

- models from ``<model-paths>/**/*.sql|py``; seeds from ``<seed-paths>/**/*.csv``;
  snapshots from ``{% snapshot name %}`` blocks;
- edges from literal ``ref('...')`` / ``source('...', '...')`` calls;
- tags and materializations from literal ``config(...)`` calls, ``schema.yml`` and ``dbt_project.yml``;
- tests from ``tests``/``data_tests`` entries in ``schema.yml`` (for Cosmos task-id mapping).

Packages under ``dbt_packages/`` are not installed, so their models are missing. Everything this
module produces is labelled ``approximate``.
"""

from __future__ import annotations

import os
import re
from typing import Any

from . import miniyaml
from .manifest import Manifest, manifest_from_dict

FILE_CAP = 2 * 1024 * 1024
REF_RE = re.compile(r"""\bref\(\s*(['"])([^'"]+)\1\s*(?:,\s*(['"])([^'"]+)\3\s*)?(?:,\s*v(?:ersion)?\s*=\s*['"]?(\w+)['"]?\s*)?\)""")
SOURCE_RE = re.compile(r"""\bsource\(\s*(['"])([^'"]+)\1\s*,\s*(['"])([^'"]+)\3\s*\)""")
DYN_REF_RE = re.compile(r"""\bref\(\s*[^'"\s)]""")
CONFIG_RE = re.compile(r"""\bconfig\((?P<body>[^)]*)\)""", re.S)
MAT_RE = re.compile(r"""materialized\s*=\s*['"](\w+)['"]""")
TAGS_RE = re.compile(r"""tags\s*=\s*(\[[^\]]*\]|['"][^'"]+['"])""")
ENABLED_RE = re.compile(r"""enabled\s*=\s*(False|false)""")
SNAPSHOT_RE = re.compile(r"""\{%-?\s*snapshot\s+(\w+)\s*-?%\}""")
JINJA_COMMENT_RE = re.compile(r"\{#.*?#\}", re.S)
CONFIG_KEYS = {
    "materialized", "tags", "schema", "database", "alias", "enabled", "persist_docs", "full_refresh",
    "meta", "grants", "contract", "docs", "group", "access", "pre-hook", "post-hook", "on_schema_change",
    "unique_key", "incremental_strategy", "sql_header", "quoting", "column_types", "event_time",
}
SKIP_DIRS = {"target", "dbt_packages", "dbt_modules", "logs", ".git", "node_modules", "__pycache__", ".venv", "venv"}


def _read(path: str) -> str:
    if os.path.getsize(path) > FILE_CAP:
        return ""
    with open(path, encoding="utf-8", errors="replace") as fh:
        return fh.read()


def _load_yaml(path: str, notes: list[str], root: str) -> Any:
    try:
        return miniyaml.loads(_read(path))
    except miniyaml.YamlError as exc:
        notes.append(f"YAML not parsed ({os.path.relpath(path, root)}): {exc}")
        return None


def _as_list(v: Any) -> list[Any]:
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


def _literal_tags(text: str) -> list[str]:
    raw = text.strip()
    if raw.startswith("["):
        return re.findall(r"""['"]([^'"]+)['"]""", raw)
    return [raw.strip("'\"")]


def _walk(base: str, exts: tuple[str, ...]):
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS and not d.startswith("."))
        for fn in sorted(filenames):
            if fn.lower().endswith(exts):
                yield os.path.join(dirpath, fn)


def _folder_config(tree: Any, parts: list[str]) -> dict[str, Any]:
    """Resolve dbt_project.yml folder-level config for a node at ``parts`` (project, dirs...)."""
    cfg: dict[str, Any] = {"tags": []}
    node = tree
    for depth in range(len(parts) + 1):
        if not isinstance(node, dict):
            break
        for k, v in node.items():
            if not isinstance(k, str):
                continue
            key = k[1:] if k.startswith("+") else k
            if k.startswith("+") or (key in CONFIG_KEYS and not isinstance(v, dict)):
                if key == "tags":
                    cfg["tags"] = cfg["tags"] + [t for t in _as_list(v) if isinstance(t, str)]
                else:
                    cfg[key] = v
        if depth < len(parts):
            node = node.get(parts[depth]) if isinstance(node, dict) else None
    return cfg


def build_standin(project_dir: str) -> Manifest:
    notes: list[str] = []
    proj_file = os.path.join(project_dir, "dbt_project.yml")
    proj = _load_yaml(proj_file, notes, project_dir) if os.path.exists(proj_file) else None
    proj = proj if isinstance(proj, dict) else {}
    name = str(proj.get("name") or os.path.basename(os.path.normpath(project_dir)))
    model_paths = _as_list(proj.get("model-paths") or proj.get("source-paths") or ["models"])
    seed_paths = _as_list(proj.get("seed-paths") or proj.get("data-paths") or ["seeds", "data"])
    snapshot_paths = _as_list(proj.get("snapshot-paths") or ["snapshots"])
    models_tree = (proj.get("models") or {})
    seeds_tree = (proj.get("seeds") or {})

    nodes: dict[str, dict[str, Any]] = {}
    by_name: dict[str, str] = {}
    pending_refs: dict[str, list[tuple[str, str | None]]] = {}
    pending_sources: dict[str, list[tuple[str, str]]] = {}
    yml_files: list[str] = []
    dynamic_refs = 0

    def add_node(uid: str, rtype: str, nm: str, rel: str, fqn: list[str], cfg: dict[str, Any]) -> None:
        nodes[uid] = {
            "unique_id": uid, "resource_type": rtype, "name": nm, "package_name": name, "fqn": fqn,
            "original_file_path": rel, "tags": list(dict.fromkeys(cfg.get("tags") or [])), "config": cfg,
            "depends_on": {"nodes": []},
        }
        by_name.setdefault(nm, uid)

    for mp in model_paths:
        base = os.path.join(project_dir, str(mp))
        for path in _walk(base, (".sql", ".py", ".yml", ".yaml")):
            rel = os.path.relpath(path, project_dir).replace(os.sep, "/")
            if path.endswith((".yml", ".yaml")):
                yml_files.append(path)
                continue
            text = JINJA_COMMENT_RE.sub("", _read(path))
            stem = os.path.splitext(os.path.basename(path))[0]
            dirs = os.path.relpath(os.path.dirname(path), base).replace(os.sep, "/")
            dir_parts = [] if dirs == "." else dirs.split("/")
            cfg = _folder_config(models_tree, [name] + dir_parts)
            cfg.setdefault("materialized", "view")
            for m in CONFIG_RE.finditer(text):
                body = m.group("body")
                mm = MAT_RE.search(body)
                if mm:
                    cfg["materialized"] = mm.group(1)
                tm = TAGS_RE.search(body)
                if tm:
                    cfg["tags"] = cfg["tags"] + _literal_tags(tm.group(1))
                if ENABLED_RE.search(body):
                    cfg["enabled"] = False
            if cfg.get("enabled") is False:
                continue
            uid = f"model.{name}.{stem}"
            add_node(uid, "model", stem, rel, [name] + dir_parts + [stem], cfg)
            pending_refs[uid] = [(m.group(4) or m.group(2), m.group(2) if m.group(4) else None, m.group(5)) for m in REF_RE.finditer(text)]
            pending_sources[uid] = [(m.group(2), m.group(4)) for m in SOURCE_RE.finditer(text)]
            if DYN_REF_RE.search(text):
                dynamic_refs += 1

    for sp in seed_paths:
        base = os.path.join(project_dir, str(sp))
        if not os.path.isdir(base):
            continue
        for path in _walk(base, (".csv",)):
            rel = os.path.relpath(path, project_dir).replace(os.sep, "/")
            stem = os.path.splitext(os.path.basename(path))[0]
            dirs = os.path.relpath(os.path.dirname(path), base).replace(os.sep, "/")
            dir_parts = [] if dirs == "." else dirs.split("/")
            cfg = _folder_config(seeds_tree, [name] + dir_parts)
            cfg["materialized"] = "seed"
            add_node(f"seed.{name}.{stem}", "seed", stem, rel, [name] + dir_parts + [stem], cfg)

    for sp in snapshot_paths:
        base = os.path.join(project_dir, str(sp))
        if not os.path.isdir(base):
            continue
        for path in _walk(base, (".sql",)):
            text = _read(path)
            rel = os.path.relpath(path, project_dir).replace(os.sep, "/")
            for m in SNAPSHOT_RE.finditer(text):
                uid = f"snapshot.{name}.{m.group(1)}"
                add_node(uid, "snapshot", m.group(1), rel, [name, m.group(1)], {"materialized": "snapshot", "tags": []})
                pending_refs[uid] = [(r.group(4) or r.group(2), None, r.group(5)) for r in REF_RE.finditer(text)]
                pending_sources[uid] = [(r.group(2), r.group(4)) for r in SOURCE_RE.finditer(text)]

    # schema.yml: model configs, versions, sources and tests
    tests: list[tuple[str, list[str], list[str]]] = []  # (test name, depends on model names, ref targets)
    for path in yml_files:
        doc = _load_yaml(path, notes, project_dir)
        if not isinstance(doc, dict):
            continue
        rel = os.path.relpath(path, project_dir).replace(os.sep, "/")
        for src in _as_list(doc.get("sources")):
            if not isinstance(src, dict) or not src.get("name"):
                continue
            stags = [t for t in _as_list(src.get("tags")) if isinstance(t, str)]
            for tbl in _as_list(src.get("tables")):
                if not isinstance(tbl, dict) or not tbl.get("name"):
                    continue
                uid = f"source.{name}.{src['name']}.{tbl['name']}"
                nodes[uid] = {
                    "unique_id": uid, "resource_type": "source", "name": tbl["name"], "source_name": src["name"],
                    "package_name": name, "fqn": [name, src["name"], tbl["name"]], "original_file_path": rel,
                    "tags": stags + [t for t in _as_list(tbl.get("tags")) if isinstance(t, str)], "config": {},
                    "depends_on": {"nodes": []},
                }
        for mdl in _as_list(doc.get("models")) + _as_list(doc.get("seeds")) + _as_list(doc.get("snapshots")):
            if not isinstance(mdl, dict) or not mdl.get("name"):
                continue
            mname = str(mdl["name"])
            uid = by_name.get(mname)
            versions = [v for v in _as_list(mdl.get("versions")) if isinstance(v, dict) and "v" in v]
            if versions:
                latest = mdl.get("latest_version")
                _apply_versions(nodes, by_name, pending_refs, pending_sources, name, mname, versions, latest)
                uid = None
            if uid and uid in nodes:
                node = nodes[uid]
                node["patch_path"] = f"{name}://{rel}"
                cfg = mdl.get("config") if isinstance(mdl.get("config"), dict) else {}
                if "materialized" in cfg:
                    node["config"]["materialized"] = cfg["materialized"]
                extra = _as_list(cfg.get("tags")) + _as_list(mdl.get("tags"))
                node["tags"] = list(dict.fromkeys(node["tags"] + [t for t in extra if isinstance(t, str)]))
                node["config"]["tags"] = node["tags"]
            for t in _as_list(mdl.get("tests")) + _as_list(mdl.get("data_tests")):
                tests.append((_test_name(t), [mname], _test_refs(t)))
            for col in _as_list(mdl.get("columns")):
                if not isinstance(col, dict):
                    continue
                for t in _as_list(col.get("tests")) + _as_list(col.get("data_tests")):
                    tests.append((f"{_test_name(t)}_{col.get('name')}", [mname], _test_refs(t)))

    # resolve edges
    unresolved_refs: set[str] = set()
    for uid, refs in pending_refs.items():
        deps = nodes[uid]["depends_on"]["nodes"]
        for target, package, version in refs:
            tuid = (by_name.get(f"{target}_v{version}") if version else None) or by_name.get(target)
            if package and package != name:
                unresolved_refs.add(f"{package}.{target}")
                continue
            if tuid:
                deps.append(tuid)
            else:
                unresolved_refs.add(target)
        for sname, tname in pending_sources.get(uid, []):
            suid = f"source.{name}.{sname}.{tname}"
            if suid in nodes:
                deps.append(suid)
            else:
                unresolved_refs.add(f"source:{sname}.{tname}")
    for i, (tname, models, refs) in enumerate(tests):
        deps = [by_name[m] for m in models + refs if m in by_name]
        if not deps:
            continue
        uid = f"test.{name}.{tname}_{models[0]}.{i}"
        nodes[uid] = {
            "unique_id": uid, "resource_type": "test", "name": f"{tname}_{models[0]}", "package_name": name,
            "fqn": [name, f"{tname}_{models[0]}"], "original_file_path": "", "tags": [], "config": {},
            "depends_on": {"nodes": list(dict.fromkeys(deps))},
        }
    if unresolved_refs:
        notes.append(f"refs not resolved in the stand-in: {sorted(unresolved_refs)}")
    if dynamic_refs:
        notes.append(f"{dynamic_refs} model file(s) contain non-literal ref() calls; edges may be missing")
    if os.path.exists(os.path.join(project_dir, "packages.yml")) or os.path.exists(os.path.join(project_dir, "dependencies.yml")):
        notes.append("project declares dbt packages; package models are not in the stand-in")
    data = {
        "metadata": {"project_name": name, "dbt_version": None, "generated_at": None, "approximate": True},
        "nodes": {k: v for k, v in nodes.items() if v["resource_type"] != "source"},
        "sources": {k: v for k, v in nodes.items() if v["resource_type"] == "source"},
    }
    manifest = manifest_from_dict(data, path=None)
    manifest.approximate = True
    manifest.notes = notes
    return manifest


def _apply_versions(nodes, by_name, pending_refs, pending_sources, project, mname, versions, latest) -> None:
    """dbt's rule: version v lives in ``defined_in`` or ``<name>_v<v>``; the latest may use ``<name>``."""
    def _key(x: Any) -> Any:
        try:
            return float(x)
        except (TypeError, ValueError):
            return str(x)

    latest_v = latest if latest is not None else max((v["v"] for v in versions), key=_key)
    for ver in versions:
        v = ver["v"]
        fname = ver.get("defined_in") or f"{mname}_v{v}"
        old = by_name.get(fname)
        if (not old or old not in nodes) and str(v) == str(latest_v):
            fname = mname
            old = by_name.get(mname)
        if not old or old not in nodes or nodes[old].get("version") is not None:
            continue
        node = nodes.pop(old)
        uid = f"model.{project}.{mname}.v{v}"
        node.update({"unique_id": uid, "name": mname, "version": v, "fqn": node["fqn"][:-1] + [mname, f"v{v}"]})
        nodes[uid] = node
        by_name[fname] = uid
        if str(v) == str(latest_v):
            by_name[mname] = uid
        pending_refs[uid] = pending_refs.pop(old, [])
        pending_sources[uid] = pending_sources.pop(old, [])


def _test_name(t: Any) -> str:
    if isinstance(t, str):
        return t
    if isinstance(t, dict) and t:
        return str(next(iter(t)))
    return "test"


def _test_refs(t: Any) -> list[str]:
    out: list[str] = []
    if isinstance(t, dict):
        for body in t.values():
            if isinstance(body, dict):
                for scope in (body, body.get("arguments") if isinstance(body.get("arguments"), dict) else {}):
                    to = scope.get("to") if isinstance(scope, dict) else None
                    if isinstance(to, str):
                        m = REF_RE.search(to)
                        if m:
                            out.append(m.group(4) or m.group(2))
    return out
