"""A subset of dbt node selection, evaluated over a :class:`Manifest`.

Modelled on dbt-core's ``graph/selector_methods.py``, ``selector_spec.py`` and ``cli.py``
(read as the reference). Supported:

- methods: ``fqn`` (default), ``path``, ``file``, ``tag``, ``config.<key>``, ``resource_type``,
  ``source``, ``package``, ``selector`` (YAML selectors stored in the manifest);
- graph operators ``+``, ``n+``, ``+n``, ``@``;
- union (space), intersection (comma), ``--exclude``;
- YAML selector definitions with ``union``/``intersection``/``exclude`` and graph keys.

Anything else raises :class:`UnsupportedSelector`, which callers report as ``unknown``.
It never quietly returns an empty set for something it does not understand.
"""

from __future__ import annotations

import posixpath
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from typing import Any

from lineagepilot_core.airflow.manifest import Manifest, Node

RAW_SELECTOR_PATTERN = re.compile(
    r"\A"
    r"(?P<childrens_parents>(\@))?"
    r"(?P<parents>((?P<parents_depth>(\d*))\+))?"
    r"((?P<method>([\w.]+)):)?(?P<value>(.*?))"
    r"(?P<children>(\+(?P<children_depth>(\d*))))?"
    r"\Z"
)
SUPPORTED_METHODS = {
    "fqn",
    "path",
    "file",
    "tag",
    "config",
    "resource_type",
    "source",
    "package",
    "selector",
}
KNOWN_DBT_METHODS = SUPPORTED_METHODS | {
    "group",
    "access",
    "test_name",
    "test_type",
    "state",
    "exposure",
    "metric",
    "result",
    "source_status",
    "version",
    "semantic_model",
    "saved_query",
    "unit_test",
}
NODE_TYPES = {
    "model",
    "analysis",
    "test",
    "snapshot",
    "operation",
    "seed",
    "rpc",
    "sql_operation",
    "doc",
    "source",
    "macro",
    "exposure",
    "metric",
    "group",
    "saved_query",
    "semantic_model",
    "unit_test",
    "fixture",
    "function",
}
NON_SOURCE_TYPES_EXCLUDED = {"source"}
WILDCARDS = ("*", "?", "[", "]")
# A selector comes from a workspace file and becomes a regular expression
MAX_SELECTOR = 1000
_VERSION = re.compile(r"^(\d+)\.(\d+)")


class UnsupportedSelector(Exception):
    """The selector uses syntax or a method this engine does not implement."""


@dataclass
class Selection:
    ids: set[str] | None
    reason: str | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def unknown(self) -> bool:
        return self.ids is None


@dataclass
class Criteria:
    raw: str
    method: str
    args: list[str]
    value: str
    childrens_parents: bool = False
    parents: bool = False
    parents_depth: int | None = None
    children: bool = False
    children_depth: int | None = None


def _probably_path(value: str) -> bool:
    return "/" in value or "\\" in value


def _default_method(value: str) -> str:
    if _probably_path(value):
        return "path"
    if value.lower().endswith((".sql", ".py", ".csv")):
        return "file"
    return "fqn"


def _depth(raw: str | None) -> int | None:
    if not raw:
        return None
    if len(raw) > 6:
        raise UnsupportedSelector(f"graph depth {raw} is too large")
    return int(raw)


def leaf_wildcards(dbt_version: str | None) -> bool:
    """Whether a bare wildcard such as ``stg_*`` also matches a node's leaf name: since dbt 1.10.

    Fusion reports 2.x and behaves the same. A manifest that does not say gets current behaviour.
    """
    found = _VERSION.match(dbt_version or "")
    return found is None or (int(found[1]), int(found[2])) >= (1, 10)


def parse_spec(raw: str) -> Criteria:
    if len(raw) > MAX_SELECTOR:
        raise UnsupportedSelector(f"a selector longer than {MAX_SELECTOR} characters")
    m = RAW_SELECTOR_PATTERN.match(raw)
    if m is None:
        raise UnsupportedSelector(f"invalid selector spec {raw!r}")
    d = m.groupdict()
    if d.get("childrens_parents") and d.get("parents"):
        raise UnsupportedSelector(f"'@' cannot be combined with a '+' prefix in {raw!r}")
    value = d["value"]
    if d.get("method") is None:
        method, args = _default_method(value), []
    else:
        parts = d["method"].split(".")
        method, args = parts[0], parts[1:]
    if method not in KNOWN_DBT_METHODS:
        raise UnsupportedSelector(f"{method!r} is not a dbt selector method ({raw!r})")
    if method not in SUPPORTED_METHODS:
        raise UnsupportedSelector(
            f"selector method {method!r} is not supported by the static engine ({raw!r})"
        )
    if method == "config" and not args:
        raise UnsupportedSelector(f"config selector without a key ({raw!r})")
    return Criteria(
        raw=raw,
        method=method,
        args=args,
        value=value,
        childrens_parents=bool(d.get("childrens_parents")),
        parents=bool(d.get("parents")),
        parents_depth=_depth(d.get("parents_depth")),
        children=bool(d.get("children")),
        children_depth=_depth(d.get("children_depth")),
    )


def is_selected_node(
    fqn: list[str], node_selector: str, is_versioned: bool, leaf_rule: bool = True
) -> bool:
    """dbt-core's fqn matching, with the bare-wildcard-matches-leaf rule from dbt 1.10."""
    if not fqn:
        return False
    if is_versioned and len(fqn) >= 2:
        flat_node_selector = node_selector.split(".")
        if fqn[-2] == node_selector:
            return True
        if "_".join(fqn[-2:]) == "_".join(flat_node_selector[-2:]):
            return True
    elif fqn[-1] == node_selector:
        return True
    flat_fqn = [item for segment in fqn for item in segment.split(".")]
    parts = node_selector.split(".")
    if len(flat_fqn) < len(parts):
        return False
    slurp_from_ix: int | None = None
    for i, part in enumerate(parts):
        if any(w in part for w in WILDCARDS):
            slurp_from_ix = i
            break
        if flat_fqn[i] == part:
            continue
        return False
    if slurp_from_ix is not None:
        if fnmatchcase(".".join(flat_fqn[slurp_from_ix:]), ".".join(parts[slurp_from_ix:])):
            return True
        if leaf_rule and "." not in node_selector:
            leaf = fqn[-2] if (is_versioned and len(fqn) >= 2) else fqn[-1]
            return fnmatchcase(leaf, node_selector)
        return False
    return True


class _PathGlob:
    """A ``path:`` selector's glob, matched one path component at a time: ``*``, ``?`` and
    ``[...]`` within a component, ``**`` for any number of whole components, as pathlib's glob,
    which dbt uses. A regular expression made from the whole pattern backtracked (``*a*a*a*b``
    against one long path took seconds), and an invalid character class in it raised
    ``re.error``, which lost the whole DAG file (audit LP-SEC-05). ``fnmatch`` does neither."""

    def __init__(self, pattern: str) -> None:
        self.parts = [p for p in pattern.split("/") if p not in ("", ".")]

    def match(self, path: str) -> bool:
        names = [n for n in path.split("/") if n]
        reach = {0}  # how many of the path's components the pattern so far can have matched
        for part in self.parts:
            if part == "**":
                reach = set(range(min(reach), len(names) + 1))
            else:
                reach = {i + 1 for i in reach if i < len(names) and fnmatchcase(names[i], part)}
            if not reach:
                return False
        return len(names) in reach


def _norm_rel(p: str) -> str:
    p = p.replace("\\", "/")
    while p.startswith("./"):
        p = p[2:]
    p = posixpath.normpath(p) if p else p
    return "" if p == "." else p


class SelectorEngine:
    """Node selection over one manifest. A scan asks it thousands of times, so what it works out
    is kept: walking every node for each selector took a minute at 5,000 models and 2,000 tasks."""

    def __init__(self, manifest: Manifest):
        self.m = manifest
        self._path_candidates: set[str] | None = None
        self._leaf_rule = leaf_wildcards(manifest.dbt_version)
        self._names: dict[str, set[str]] | None = None
        self._joined: dict[str, set[str]] = {}
        self._methods: dict[tuple[str, tuple[str, ...], str], set[str]] = {}

    # --- node sets -------------------------------------------------------------------------
    def _non_source(self) -> Iterable[Node]:
        return (
            n for n in self.m.nodes.values() if n.resource_type not in NON_SOURCE_TYPES_EXCLUDED
        )

    def _all(self) -> Iterable[Node]:
        return self.m.nodes.values()

    def _configurable(self) -> Iterable[Node]:
        return (
            n
            for n in self.m.nodes.values()
            if n.resource_type
            not in ("exposure", "metric", "semantic_model", "saved_query", "unit_test")
        )

    def _candidates(self) -> set[str]:
        if self._path_candidates is None:
            cands: set[str] = set()
            for n in self.m.nodes.values():
                for p in (n.original_file_path, (n.patch_path or "").split("://")[-1]):
                    p = _norm_rel(p)
                    while p:
                        cands.add(p)
                        p = posixpath.dirname(p)
            self._path_candidates = cands
        return self._path_candidates

    # --- methods ---------------------------------------------------------------------------
    def _fqn_names(self) -> dict[str, set[str]]:
        """Every name without a wildcard that selects a node: what ``is_selected_node`` accepts
        for one, computed once. Prefixes of the dotted fqn, the leaf name, and for a versioned
        node the name before its version."""
        if self._names is None:
            self._names = {}
            for node in self._non_source():
                for fqn in (node.fqn, node.fqn[1:]):
                    if not fqn:
                        continue
                    if node.version is not None and len(fqn) >= 2:
                        self._names.setdefault(fqn[-2], set()).add(node.unique_id)
                        self._joined.setdefault("_".join(fqn[-2:]), set()).add(node.unique_id)
                    else:
                        self._names.setdefault(fqn[-1], set()).add(node.unique_id)
                    flat = [item for segment in fqn for item in segment.split(".")]
                    for end in range(1, len(flat) + 1):
                        prefix = ".".join(flat[:end])
                        self._names.setdefault(prefix, set()).add(node.unique_id)
        return self._names

    def _method(self, c: Criteria) -> set[str]:
        key = (c.method, tuple(c.args), c.value)
        if key not in self._methods:
            self._methods[key] = self._select_by(c)
        return self._methods[key]

    def _select_by(self, c: Criteria) -> set[str]:
        v = c.value
        if c.method == "fqn":
            if not any(w in v for w in WILDCARDS):
                found = set(self._fqn_names().get(v, ()))
                return found | self._joined.get("_".join(v.split(".")[-2:]), set())
            return {
                n.unique_id
                for n in self._non_source()
                if n.fqn
                and (
                    is_selected_node(n.fqn, v, n.version is not None, self._leaf_rule)
                    or is_selected_node(n.fqn[1:], v, n.version is not None, self._leaf_rule)
                )
            }
        if c.method == "tag":
            return {n.unique_id for n in self._all() if any(fnmatchcase(t, v) for t in n.tags)}
        if c.method == "path":
            sel = _norm_rel(v)
            if sel.startswith("/") or sel.startswith(".."):
                raise UnsupportedSelector(f"path selector outside the project ({v!r})")
            glob = _PathGlob(sel)
            paths = {p for p in self._candidates() if glob.match(p)}
            out = set()
            for n in self._all():
                ofp = _norm_rel(n.original_file_path)
                pfp = _norm_rel((n.patch_path or "").split("://")[-1]) if n.patch_path else ""
                if ofp in paths or (pfp and pfp in paths):
                    out.add(n.unique_id)
                    continue
                parent = posixpath.dirname(ofp)
                while parent:
                    if parent in paths:
                        out.add(n.unique_id)
                        break
                    parent = posixpath.dirname(parent)
            return out
        if c.method == "file":
            out = set()
            for n in self._all():
                base = posixpath.basename(n.original_file_path.replace("\\", "/"))
                stem = base.rsplit(".", 1)[0] if "." in base else base
                if base and (fnmatchcase(base, v) or fnmatchcase(stem, v)):
                    out.add(n.unique_id)
            return out
        if c.method == "package":
            target = self.m.project_name if (v == "this" and self.m.project_name) else v
            return {n.unique_id for n in self._all() if fnmatchcase(n.package_name, target)}
        if c.method == "resource_type":
            if v not in NODE_TYPES:
                raise UnsupportedSelector(f"invalid resource_type {v!r}")
            return {n.unique_id for n in self._all() if n.resource_type == v}
        if c.method == "source":
            parts = v.split(".")
            pkg = "*"
            if len(parts) == 1:
                src, tbl = parts[0], "*"
            elif len(parts) == 2:
                src, tbl = parts
            elif len(parts) == 3:
                pkg, src, tbl = parts
            else:
                raise UnsupportedSelector(f"invalid source selector {v!r}")
            return {
                n.unique_id
                for n in self._all()
                if n.resource_type == "source"
                and fnmatchcase(n.package_name, pkg)
                and fnmatchcase(n.source_name or "", src)
                and fnmatchcase(n.name, tbl)
            }
        if c.method == "config":
            out = set()
            for n in self._configurable():
                value: Any = n.config
                ok = True
                for key in c.args:
                    if isinstance(value, dict) and key in value:
                        value = value[key]
                    else:
                        ok = False
                        break
                if not ok:
                    continue
                if self._config_match(value, v, c.args):
                    out.add(n.unique_id)
            return out
        if c.method == "selector":
            return self.select_yaml_selector(v)
        raise UnsupportedSelector(f"method {c.method!r} not supported")  # pragma: no cover

    @staticmethod
    def _config_match(value: Any, sel: str, args: list[str]) -> bool:
        if args == ["severity"] and isinstance(value, str):
            return value.upper() == sel.upper()
        is_true = sel.upper() == "TRUE"
        is_false = sel.upper() == "FALSE"
        if isinstance(value, list):
            return sel in value or (is_true and True in value) or (is_false and False in value)
        return (
            (isinstance(value, str) and value == sel)
            or (is_true and value is True)
            or (is_false and value is False)
        )

    # --- graph -----------------------------------------------------------------------------
    def _parents(self, selected: set[str], depth: int | None = None) -> set[str]:
        found: set[str] = set()
        layer = set(selected)
        i = 0
        while layer and (depth is None or i < depth):
            nxt = {p for uid in layer for p in self.m.parents.get(uid, ()) if p not in found}
            found |= nxt
            layer = nxt
            i += 1
        return found

    def _children(self, selected: set[str], depth: int | None = None) -> set[str]:
        found: set[str] = set()
        layer = set(selected)
        i = 0
        while layer and (depth is None or i < depth):
            nxt = {c for uid in layer for c in self.m.children.get(uid, ()) if c not in found}
            found |= nxt
            layer = nxt
            i += 1
        return found

    def criteria(self, c: Criteria) -> set[str]:
        base = self._method(c)
        extra: set[str] = set()
        if c.childrens_parents:
            anc_for = self._children(base) | base
            extra |= self._parents(anc_for) | anc_for
        if c.parents:
            extra |= self._parents(base, c.parents_depth)
        if c.children:
            extra |= self._children(base, c.children_depth)
        return base | extra

    # --- CLI-style expressions ---------------------------------------------------------------
    def union(self, components: list[str]) -> set[str]:
        out: set[str] = set()
        for raw in (part for comp in components for part in comp.split(" ")):
            if not raw:
                continue
            inter: set[str] | None = None
            for piece in raw.split(","):
                ids = self.criteria(parse_spec(piece))
                inter = ids if inter is None else inter & ids
            out |= inter or set()
        return out

    def everything(self) -> set[str]:
        """dbt's DEFAULT_INCLUDES (fqn:* source:* exposure:* ...): every node in the manifest."""
        return set(self.m.nodes)

    # --- YAML selectors ----------------------------------------------------------------------
    def select_yaml_selector(self, name: str, _depth: int = 0) -> set[str]:
        if _depth > 20:
            raise UnsupportedSelector("selector recursion too deep")
        entry = self.m.selectors.get(name)
        if entry is None:
            raise UnsupportedSelector(f"YAML selector {name!r} not found in the manifest")
        definition = entry.get("definition") if isinstance(entry, dict) else None
        if definition is None:
            raise UnsupportedSelector(f"YAML selector {name!r} has no definition")
        return self._definition(definition, _depth + 1)

    def _definition(self, d: Any, depth: int) -> set[str]:
        if isinstance(d, str):
            return self.criteria(parse_spec(d))
        if not isinstance(d, dict):
            raise UnsupportedSelector(f"unsupported selector definition {d!r}")
        if "union" in d or "intersection" in d:
            key = "union" if "union" in d else "intersection"
            items = d[key]
            if not isinstance(items, list):
                raise UnsupportedSelector(f"{key} must be a list")
            include: list[set[str]] = []
            exclude: set[str] | None = None
            for item in items:
                if isinstance(item, dict) and "exclude" in item:
                    exclude = self._exclusions(item["exclude"], depth)
                else:
                    include.append(self._definition(item, depth))
            if key == "union":
                result = set().union(*include) if include else set()
            else:
                result = set.intersection(*include) if include else set()
            return result - exclude if exclude is not None else result
        if len(d) == 1 and "method" not in d:
            ((k, v),) = d.items()
            d = {"method": k, "value": v}
        if "method" not in d or "value" not in d:
            raise UnsupportedSelector(f"selector definition needs method and value: {d!r}")
        method = str(d["method"])
        value = str(d["value"])
        unknown_keys = set(d) - {
            "method",
            "value",
            "parents",
            "children",
            "parents_depth",
            "children_depth",
            "childrens_parents",
            "indirect_selection",
            "exclude",
            "description",
        }
        if unknown_keys:
            raise UnsupportedSelector(f"unsupported selector keys {sorted(unknown_keys)}")
        if method == "selector":
            base = self.select_yaml_selector(value, depth)
        else:
            parts = method.split(".")
            if parts[0] not in KNOWN_DBT_METHODS:
                raise UnsupportedSelector(f"{parts[0]!r} is not a dbt selector method")
            if parts[0] not in SUPPORTED_METHODS:
                raise UnsupportedSelector(
                    f"selector method {parts[0]!r} is not supported by the static engine"
                )
            c = Criteria(
                raw=f"{method}:{value}",
                method=parts[0],
                args=parts[1:],
                value=value,
                childrens_parents=bool(d.get("childrens_parents")),
                parents=bool(d.get("parents")),
                parents_depth=_depth(str(d["parents_depth"]))
                if d.get("parents_depth") is not None
                else None,
                children=bool(d.get("children")),
                children_depth=_depth(str(d["children_depth"]))
                if d.get("children_depth") is not None
                else None,
            )
            base = self.criteria(c)
        if "exclude" in d:
            base = base - self._exclusions(d["exclude"], depth)
        return base

    def _exclusions(self, items: Any, depth: int) -> set[str]:
        if not isinstance(items, list):
            raise UnsupportedSelector("exclude must be a list")
        out: set[str] = set()
        for item in items:
            out |= self._definition(item, depth)
        return out


def select(
    manifest: Manifest,
    select: list[str] | None = None,
    exclude: list[str] | None = None,
    selector: str | None = None,
    resource_types: Iterable[str] | None = ("model",),
    include_external: bool = False,
) -> Selection:
    """Evaluate a dbt selection. Returns ``Selection(ids=None, reason=...)`` when unsupported."""
    eng = manifest.selector_engine(SelectorEngine)
    notes: list[str] = []
    try:
        if selector:
            # dbt: a YAML selector replaces --select/--exclude entirely
            if select or exclude:
                notes.append("--selector given: --select/--exclude are ignored, as dbt does")
            included = eng.select_yaml_selector(selector)
        else:
            included = eng.union(list(select)) if select else eng.everything()
            if exclude:
                included -= eng.union(list(exclude))
    except UnsupportedSelector as exc:
        return Selection(ids=None, reason=str(exc))
    types = set(resource_types) if resource_types else None
    out = set()
    for uid in included:
        node = manifest.nodes.get(uid)
        if node is None:
            continue
        if types is not None and node.resource_type not in types:
            continue
        if node.external and not include_external:
            continue
        out.add(uid)
    if manifest.approximate:
        notes.append("model set computed over an approximate stand-in manifest")
    return Selection(ids=out, notes=notes)
