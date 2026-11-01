"""Throwaway in-memory graph built from a ParseResult. Phase 1 builds the real
MetadataGraph; this exists only to measure the section 11 budgets.

- ``nodes``: unique_id -> small summary dict
- ``by_file``: original_file_path -> [unique_id]
- ``down`` / ``up``: forward and reverse adjacency over unique_ids

Edges in a ParseResult carry URNs, not unique_ids, so the build maps them back
through ``urn -> unique_id``. When two capsules share a URN (for example dbt
model versions v1 and v2, which have the same name and schema) the mapping is
ambiguous; ``urn_collisions`` records every such case instead of hiding it.

``add_manifest_extras`` adds what DbtParser does not emit at all (exposures and
generic-test attachment). It is labelled separately so the as-is gap is
visible in the results.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


@dataclass
class Graph:
    nodes: dict[str, dict[str, Any]] = field(default_factory=dict)
    by_file: dict[str, list[str]] = field(default_factory=dict)
    down: dict[str, list[str]] = field(default_factory=dict)
    up: dict[str, list[str]] = field(default_factory=dict)
    columns: dict[str, list[tuple[str, str | None, str | None]]] = field(default_factory=dict)
    urn_collisions: dict[str, list[str]] = field(default_factory=dict)
    unresolved_edges: int = 0
    duplicate_edges: int = 0
    extras: dict[str, int] = field(default_factory=dict)

    # ------------------------------------------------------------- building
    def add_node(self, uid: str, info: dict[str, Any]) -> None:
        self.nodes[uid] = info
        self.down.setdefault(uid, [])
        self.up.setdefault(uid, [])
        f = info.get("file")
        if f:
            self.by_file.setdefault(f, []).append(uid)

    def add_edge(self, src: str, dst: str) -> bool:
        if dst in self.down[src]:
            self.duplicate_edges += 1
            return False
        self.down[src].append(dst)
        self.up[dst].append(src)
        return True

    @property
    def edge_count(self) -> int:
        return sum(len(v) for v in self.down.values())

    # ------------------------------------------------------------- queries
    def lookup_file(self, path: str) -> list[str]:
        return self.by_file.get(path, [])

    def direct_downstream(self, uid: str) -> list[str]:
        return self.down.get(uid, [])

    def downstream_closure(self, uid: str) -> set[str]:
        seen: set[str] = set()
        q = deque(self.down.get(uid, ()))
        down = self.down
        while q:
            n = q.popleft()
            if n in seen:
                continue
            seen.add(n)
            q.extend(down.get(n, ()))
        return seen

    def codelens(self, path: str) -> list[str]:
        """One CodeLens title per node defined in ``path`` (direct downstream)."""
        out = []
        for uid in self.lookup_file(path):
            kids = self.down.get(uid, ())
            kinds: dict[str, int] = {}
            for k in kids:
                kind = self.nodes[k]["kind"]
                kinds[kind] = kinds.get(kind, 0) + 1
            info = self.nodes[uid]
            parts = [f"↓ {kinds.get('model', 0)} models"]
            if kinds.get("exposure"):
                parts.append(f"{kinds['exposure']} exposure{'s' if kinds['exposure'] > 1 else ''}")
            if info.get("pii"):
                parts.append("PII")
            if info.get("owner"):
                parts.append(f"owner {info['owner']}")
            out.append(" · ".join(parts))
        return out

    def impact(self, uid: str) -> dict[str, Any]:
        """Model-level impact: full downstream closure grouped by kind and owner."""
        closure = self.downstream_closure(uid)
        by_kind: dict[str, int] = {}
        owners: dict[str, int] = {}
        for n in closure:
            info = self.nodes[n]
            by_kind[info["kind"]] = by_kind.get(info["kind"], 0) + 1
            o = info.get("owner") or "(none)"
            owners[o] = owners.get(o, 0) + 1
        return {"root": uid, "total": len(closure), "by_kind": by_kind, "owners": owners}

    # ------------------------------------------------------------- snapshots
    def to_snapshot(self, manifest_sha256: str) -> dict[str, Any]:
        ids = list(self.nodes)
        index = {u: i for i, u in enumerate(ids)}
        return {
            "format": 1,
            "manifest_sha256": manifest_sha256,
            "nodes": [[u, *(self.nodes[u].get(k) for k in SNAP_FIELDS)] for u in ids],
            "edges": [[index[s], index[d]] for s, ds in self.down.items() for d in ds],
            "columns": [[index[u], c[0], c[1], c[2]] for u, cs in self.columns.items() for c in cs],
        }

    @classmethod
    def from_snapshot(cls, snap: dict[str, Any]) -> "Graph":
        g = cls()
        ids = []
        for row in snap["nodes"]:
            uid = row[0]
            ids.append(uid)
            g.add_node(uid, dict(zip(SNAP_FIELDS, row[1:])))
        for s, d in snap["edges"]:
            g.down[ids[s]].append(ids[d])
            g.up[ids[d]].append(ids[s])
        cols = g.columns
        for i, name, dtype, pii in snap["columns"]:
            cols.setdefault(ids[i], []).append((name, dtype, pii))
        return g


SNAP_FIELDS = ("name", "kind", "file", "layer", "owner", "materialization", "pii", "has_tests", "n_tests")


def build_graph(result: Any) -> Graph:
    """Build from a DbtParser ParseResult only (what the as-is parser gives)."""
    g = Graph()
    urn_to_ids: dict[str, list[str]] = {}
    for c in result.capsules:
        urn_to_ids.setdefault(c.urn, []).append(c.unique_id)
    g.urn_collisions = {u: ids for u, ids in urn_to_ids.items() if len(ids) > 1}

    cols_by_urn: dict[str, list[Any]] = {}
    for col in result.columns:
        cols_by_urn.setdefault(col.capsule_urn, []).append(col)

    for c in result.capsules:
        meta = (c.config or {}).get("meta") or c.meta.get("config", {}).get("meta") or {}
        cols = cols_by_urn.get(c.urn, [])
        g.add_node(c.unique_id, {
            "name": c.name,
            "kind": c.capsule_type,
            "file": c.original_file_path,
            "layer": c.layer,
            "owner": meta.get("owner") if isinstance(meta, dict) else None,
            "materialization": c.materialization,
            "pii": any(x.pii_type for x in cols),
            "has_tests": c.has_tests,
            "n_tests": c.test_count,
        })
        g.columns[c.unique_id] = [(x.name, x.data_type, x.pii_type) for x in cols]

    for e in result.edges:
        src = urn_to_ids.get(e.source_urn)
        dst = urn_to_ids.get(e.target_urn)
        if not src or not dst:
            g.unresolved_edges += 1
            continue
        # Ambiguous on collision: take the last capsule registered under the URN,
        # which is what a plain urn -> unique_id dict would do.
        g.add_edge(src[-1], dst[-1])
    return g


def add_manifest_extras(g: Graph, manifest: dict[str, Any]) -> None:
    """NOT from the parser: exposures as nodes, and test counts from test nodes."""
    n_exp = 0
    for uid, exp in manifest.get("exposures", {}).items():
        g.add_node(uid, {"name": exp.get("name"), "kind": "exposure", "file": exp.get("original_file_path"),
                         "layer": None, "owner": (exp.get("owner") or {}).get("name"),
                         "materialization": None, "pii": False, "has_tests": False, "n_tests": 0})
        n_exp += 1
        for dep in exp.get("depends_on", {}).get("nodes", []):
            if dep in g.nodes:
                g.add_edge(dep, uid)
    tests_by_node: dict[str, int] = {}
    for n in manifest.get("nodes", {}).values():
        if n.get("resource_type") != "test":
            continue
        att = n.get("attached_node")
        if att:
            tests_by_node[att] = tests_by_node.get(att, 0) + 1
    for uid, k in tests_by_node.items():
        if uid in g.nodes:
            g.nodes[uid]["has_tests_manifest"] = True
            g.nodes[uid]["n_tests_manifest"] = k
    g.extras = {"exposures": n_exp, "nodes_with_attached_tests": len(tests_by_node),
                "attached_tests": sum(tests_by_node.values())}


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ----------------------------------------------------------------- snapshot IO
def write_json_snapshot(snap: dict[str, Any], path: Path) -> None:
    path.write_text(json.dumps(snap, separators=(",", ":")))


def read_json_snapshot(path: Path, backend: str = "stdlib") -> dict[str, Any]:
    if backend == "orjson":
        import orjson
        return orjson.loads(path.read_bytes())
    return json.loads(path.read_bytes())


def write_sqlite_snapshot(snap: dict[str, Any], path: Path) -> None:
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    cols = ", ".join(f"{f}" for f in SNAP_FIELDS)
    con.executescript(
        f"""
        create table meta(key text primary key, value text);
        create table nodes(idx integer primary key, uid text not null, {cols});
        create table edges(src integer not null, dst integer not null);
        create table columns(node integer not null, name text, data_type text, pii text);
        """
    )
    con.execute("insert into meta values ('manifest_sha256', ?)", (snap["manifest_sha256"],))
    con.executemany(
        f"insert into nodes values (?, ?, {', '.join('?' for _ in SNAP_FIELDS)})",
        ([i, *row] for i, row in enumerate(snap["nodes"])),
    )
    con.executemany("insert into edges values (?, ?)", snap["edges"])
    con.executemany("insert into columns values (?, ?, ?, ?)", snap["columns"])
    con.commit()
    con.close()


def read_sqlite_snapshot(path: Path) -> dict[str, Any]:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    sha = con.execute("select value from meta where key='manifest_sha256'").fetchone()[0]
    nodes = [list(r[1:]) for r in con.execute("select * from nodes order by idx")]
    edges = con.execute("select src, dst from edges").fetchall()
    columns = con.execute("select node, name, data_type, pii from columns").fetchall()
    con.close()
    return {"format": 1, "manifest_sha256": sha, "nodes": nodes, "edges": edges, "columns": columns}


def expected_edges(manifest: dict[str, Any], capsule_ids: Iterable[str]) -> dict[str, int]:
    """Ground truth from the manifest, for the correctness checks."""
    ids = set(capsule_ids)
    total_deps = 0
    deps_to_capsules = 0
    parent_map_total = 0
    pm = manifest.get("parent_map", {})
    for uid in ids:
        node = manifest["nodes"].get(uid)
        if node is None:
            continue  # sources have no depends_on
        deps = node.get("depends_on", {}).get("nodes", [])
        total_deps += len(deps)
        deps_to_capsules += sum(1 for d in deps if d in ids)
        parent_map_total += sum(1 for d in pm.get(uid, []) if d in ids)
    return {"depends_on_total": total_deps, "depends_on_to_capsules": deps_to_capsules,
            "parent_map_to_capsules": parent_map_total}
