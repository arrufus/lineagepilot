"""AST-only scanner for Airflow DAG files.

DAG files are untrusted workspace input. They are read as bytes (with a size cap) and parsed
with :func:`ast.parse`. Nothing is imported, executed or ``eval``-ed. Expressions are
evaluated by a small abstract interpreter over the AST (see :mod:`values`); whatever it cannot
evaluate becomes an ``Unknown`` value, and links that depend on it become ``unknown`` records.
"""

from __future__ import annotations

import ast
import os
import posixpath
import re
from dataclasses import dataclass, field, replace
from typing import Any, Iterable

from . import cosmos as cz
from .manifest import Manifest, ManifestError, load_dbt_ls_file, manifest_from_dict
from .projects import ManifestChoice, ProjectIndex
from .selector import select as run_select
from .shell import BUILDING_COMMANDS, DbtInvocation, find_dbt_in_argv, find_dbt_in_shell, mentions_dbt, parse_dbt_argv
from .values import (
    CallV, Const, DagV, DictV, FuncV, ListV, PartialV, PathV, Sym, TaskV, Unknown, Val,
    as_str, as_str_list, collect_meta, merge_values, weakest,
)

ORIGIN = "dag_static"
FILE_SIZE_CAP = 2 * 1024 * 1024
MAX_CALL_DEPTH = 4
MAX_IMPORT_DEPTH = 3
NO_IMPORT_TOP = {
    "airflow", "cosmos", "pendulum", "datetime", "os", "pathlib", "typing", "json", "sys", "logging", "re",
    "collections", "functools", "itertools", "dataclasses", "enum", "subprocess", "airflow_dbt",
    "airflow_dbt_python", "dbt", "kubernetes", "docker", "yaml", "__future__",
}
KNOWN_LIBRARY_PREFIXES = ("cosmos.", "airflow.", "pendulum.", "datetime.", "pathlib.", "os.", "typing.", "json.")
BUILTIN_CALLS = {"str", "dict", "list", "tuple", "int", "bool", "set", "frozenset", "sorted", "len", "float"}
SHELL_OPS = {"BashOperator": "bash", "SSHOperator": "ssh"}
POD_OPS = {"KubernetesPodOperator", "GKEStartPodOperator", "EksPodOperator", "AKSPodOperator"}
DOCKER_OPS = {"DockerOperator", "DockerSwarmOperator"}
SUBPROCESS_FUNCS = ("subprocess.run", "subprocess.call", "subprocess.check_call", "subprocess.check_output",
                    "subprocess.Popen", "os.system", "os.popen")
RESOURCE_TYPES_FOR_COMMAND = {
    "run": ("model",), "test": ("model", "seed", "snapshot"), "build": ("model", "seed", "snapshot"),
    "seed": ("seed",), "snapshot": ("snapshot",),
}


# ---------------------------------------------------------------------------------------------
# context and handles
# ---------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class DagInfo:
    dag_id: str | None
    reason: str | None
    conf: str
    line: int
    col: int


@dataclass(frozen=True)
class Ctx:
    dag: DagInfo | None = None
    group_prefix: tuple[str, ...] = ()
    dynamic: str | None = None  # reason the code may run 0..n times (loop, comprehension, uncalled function)
    branch: bool = False
    funcs: tuple[str, ...] = ()
    call_sites: tuple[tuple[int, int], ...] = ()
    emit: bool = True

    @property
    def prefix(self) -> str:
        return "".join(f"{g}." for g in self.group_prefix)


@dataclass
class TaskHandle:
    dag_id: str | None
    task_id: str | None
    kind: str
    line: int
    col: int


class Scope:
    def __init__(self, parent: "Scope | None" = None):
        self.vars: dict[str, Val] = {}
        self.parent = parent

    def lookup(self, name: str) -> Val | None:
        s: Scope | None = self
        while s is not None:
            if name in s.vars:
                return s.vars[name]
            s = s.parent
        return None

    def copy(self) -> "Scope":
        c = Scope(self.parent)
        c.vars = dict(self.vars)
        return c


class _Return(Exception):
    pass


# ---------------------------------------------------------------------------------------------
# workspace module loader (cross-module constants)
# ---------------------------------------------------------------------------------------------
class ModuleLoader:
    """Resolve ``from include.constants import X`` to another workspace file, read as AST only."""

    def __init__(self, workspace: str, index: ProjectIndex, enabled: bool = True, inline_calls: bool = True):
        self.workspace = os.path.realpath(workspace)
        self.index = index
        self.enabled = enabled
        self.inline_calls = inline_calls
        self.cache: dict[str, "FileScanner | None"] = {}

    def find(self, module: str, from_file: str, level: int = 0) -> str | None:
        if not self.enabled or (not module and not level):
            return None
        top = module.split(".", 1)[0] if module else ""
        if level == 0 and top in NO_IMPORT_TOP:
            return None
        rel_parts = module.split(".") if module else []
        bases: list[str] = []
        d = os.path.dirname(os.path.realpath(from_file))
        if level:
            for _ in range(level - 1):
                d = os.path.dirname(d)
            bases = [d]
        else:
            while True:
                bases.append(d)
                if d == self.workspace or not d.startswith(self.workspace):
                    break
                nd = os.path.dirname(d)
                if nd == d:
                    break
                d = nd
        for base in bases:
            cand = os.path.join(base, *rel_parts)
            for path in (cand + ".py", os.path.join(cand, "__init__.py")):
                real = os.path.realpath(path)
                if os.path.isfile(real) and (real == self.workspace or real.startswith(self.workspace + os.sep)):
                    return real
        return None

    def module(self, path: str, depth: int) -> "FileScanner | None":
        if path in self.cache:
            return self.cache[path]
        self.cache[path] = None  # cycle guard
        if depth > MAX_IMPORT_DEPTH:
            return None
        fs = FileScanner(path, self.index, self, emit=False, import_depth=depth)
        ok = fs.run()
        self.cache[path] = fs if ok else None
        return self.cache[path]


# ---------------------------------------------------------------------------------------------
# the scanner
# ---------------------------------------------------------------------------------------------
class FileScanner:
    def __init__(self, path: str, index: ProjectIndex, loader: ModuleLoader, emit: bool = True,
                 import_depth: int = 0, cli_manifest_fallback: str | None = None):
        self.path = os.path.realpath(path)
        self.rel = os.path.relpath(self.path, index.workspace)
        self.index = index
        self.loader = loader
        self.emit = emit
        self.import_depth = import_depth
        self.cli_manifest_fallback = cli_manifest_fallback
        self.links: list[dict[str, Any]] = []
        self.unresolved: list[dict[str, Any]] = []
        self.dependencies: list[dict[str, Any]] = []
        self.dags: list[dict[str, Any]] = []
        self.tasks: list[dict[str, Any]] = []
        self.imports: dict[str, tuple[str, str | None, int]] = {}  # local name -> (module, attr, level)
        self.scope = Scope()
        self.source = b""
        self.tree: ast.Module | None = None
        self.functions: dict[str, FuncV] = {}
        self.called: set[int] = set()
        self._seen: set[tuple] = set()
        self._call_depth = 0

    # --- entry ----------------------------------------------------------------------------
    def run(self) -> bool:
        try:
            size = os.path.getsize(self.path)
        except OSError as exc:
            self._unresolved(0, 0, f"cannot stat file: {exc}", Ctx(), force=True)
            return False
        if size > FILE_SIZE_CAP:
            self._unresolved(0, 0, f"file is {size} bytes, over the {FILE_SIZE_CAP}-byte cap; not parsed", Ctx(), force=True)
            return False
        with open(self.path, "rb") as fh:
            self.source = fh.read()
        try:
            self.tree = ast.parse(self.source, filename=self.rel)
        except SyntaxError as exc:
            self._unresolved(exc.lineno or 0, (exc.offset or 1) - 1, f"SyntaxError: {exc.msg}", Ctx(), force=True)
            return False
        except (ValueError, RecursionError, MemoryError) as exc:
            self._unresolved(0, 0, f"cannot parse: {type(exc).__name__}", Ctx(), force=True)
            return False
        self.scope.vars["__file__"] = Const(self.path, conf="inferred", notes=("__file__ resolved to the file's real path",))
        self.scope.vars["__name__"] = Const("__lineagepilot_static__", conf="inferred")
        try:
            self.exec_block(self.tree.body, self.scope, Ctx(emit=self.emit))
            if self.emit:
                self._scan_uncalled()
        except RecursionError:
            self._unresolved(0, 0, "expression nesting too deep for the static evaluator", Ctx(), force=True)
        return True

    def _scan_uncalled(self) -> None:
        for fv in list(self.functions.values()):
            if id(fv.node) in self.called or fv.kind in ("task", "dag"):
                continue
            scope = Scope(self.scope)
            for a in _all_args(fv.node.args):
                scope.vars[a] = Unknown(f"parameter {a!r} of function {fv.name!r}")
            ctx = Ctx(dynamic=f"inside function {fv.name!r}, which is never called at module level (factory or runtime callable)",
                      funcs=(fv.name,))
            try:
                self.exec_block(fv.node.body, scope, ctx)
            except _Return:
                pass

    # --- records --------------------------------------------------------------------------
    def _loc(self, line: int, col: int) -> dict[str, Any]:
        return {"file": self.rel, "line": line, "col": col}

    def _unresolved(self, line: int, col: int, reason: str, ctx: Ctx, force: bool = False, **extra: Any) -> None:
        if not (ctx.emit or force) or not self.emit and not force:
            return
        if ctx.dynamic and ctx.dynamic not in reason:
            reason = f"{ctx.dynamic}; {reason}"
            extra.setdefault("dynamic", True)
        key = ("u", line, col, reason, ctx.call_sites)
        if key in self._seen:
            return
        self._seen.add(key)
        rec = {"file": self.rel, "line": line, "col": col, "reason": reason}
        rec.update({k: v for k, v in extra.items() if v is not None})
        if ctx.call_sites:
            rec["via_call_at"] = [list(cs) for cs in ctx.call_sites]
        self.unresolved.append(rec)

    # --- statements -----------------------------------------------------------------------
    def exec_block(self, stmts: Iterable[ast.stmt], scope: Scope, ctx: Ctx) -> None:
        for st in stmts:
            self.exec_stmt(st, scope, ctx)

    def exec_stmt(self, st: ast.stmt, scope: Scope, ctx: Ctx) -> None:
        if isinstance(st, (ast.Import, ast.ImportFrom)):
            self._import(st, scope)
        elif isinstance(st, ast.Assign):
            val = self.eval(st.value, scope, ctx)
            for t in st.targets:
                self._bind(t, val, scope, ctx)
        elif isinstance(st, ast.AnnAssign):
            if st.value is not None:
                self._bind(st.target, self.eval(st.value, scope, ctx), scope, ctx)
        elif isinstance(st, ast.AugAssign):
            if isinstance(st.target, ast.Name):
                cur = self.eval(st.target, scope, ctx)
                rhs = self.eval(st.value, scope, ctx)
                self._bind(st.target, self._binop(st.op, cur, rhs, st, ctx), scope, ctx)
            else:
                self.eval(st.value, scope, ctx)
        elif isinstance(st, ast.Expr):
            self.eval(st.value, scope, ctx)
        elif isinstance(st, (ast.With, ast.AsyncWith)):
            self._with(st, scope, ctx)
        elif isinstance(st, ast.If):
            self._if(st, scope, ctx)
        elif isinstance(st, ast.Try) or type(st).__name__ == "TryStar":
            self._try(st, scope, ctx)
        elif isinstance(st, (ast.For, ast.AsyncFor, ast.While)):
            self._loop(st, scope, ctx)
        elif isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef)):
            self._funcdef(st, scope, ctx)
        elif isinstance(st, ast.ClassDef):
            scope.vars[st.name] = Sym(st.name)
            if ctx.emit and not ctx.funcs:
                for item in st.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        self._subprocess_heuristic(item, f"{st.name}.{item.name}", ctx)
                        fv = FuncV(f"{st.name}.{item.name}", item, "plain")
                        self.functions[f"{st.name}.{item.name}"] = fv
        elif isinstance(st, ast.Return):
            raise _Return(self.eval(st.value, scope, ctx) if st.value is not None else Const(None))
        elif type(st).__name__ == "Match":
            subject = self.eval(st.subject, scope, ctx)  # type: ignore[attr-defined]
            del subject
            branches = [c.body for c in st.cases]  # type: ignore[attr-defined]
            self._branches(branches, scope, ctx, "match statement")
        # pass, break, continue, raise, global, assert, delete: no effect on what we track

    def _import(self, st: ast.Import | ast.ImportFrom, scope: Scope) -> None:
        if isinstance(st, ast.Import):
            for a in st.names:
                local = a.asname or a.name.split(".")[0]
                canon = a.name if a.asname else a.name.split(".")[0]
                scope.vars[local] = Sym(canon)
            return
        mod = st.module or ""
        for a in st.names:
            if a.name == "*":
                continue
            local = a.asname or a.name
            dotted = f"{mod}.{a.name}" if mod else a.name
            scope.vars[local] = Sym(dotted)
            self.imports[local] = (mod, a.name, st.level or 0)

    def _bind(self, target: ast.expr, val: Val, scope: Scope, ctx: Ctx) -> None:
        if isinstance(target, ast.Name):
            scope.vars[target.id] = val
        elif isinstance(target, (ast.Tuple, ast.List)):
            if isinstance(val, ListV) and len(val.items) == len(target.elts):
                for t, v in zip(target.elts, val.items):
                    self._bind(t, v, scope, ctx)
            else:
                for t in target.elts:
                    self._bind(t, Unknown("tuple unpacking of a non-literal value", line=target.lineno), scope, ctx)
        elif isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name):
            base = scope.lookup(target.value.id)
            key = self.eval(target.slice, scope, ctx)
            if isinstance(base, DictV) and isinstance(key, Const) and isinstance(key.value, str):
                items = dict(base.items)
                items[key.value] = val
                scope.vars[target.value.id] = DictV(items, conf=base.conf, notes=base.notes)
            elif isinstance(base, DictV):
                scope.vars[target.value.id] = Unknown("dict mutated with a non-literal key", line=target.lineno)
        elif isinstance(target, ast.Starred):
            self._bind(target.value, Unknown("starred assignment"), scope, ctx)

    def _merge_into(self, scope: Scope, variants: list[Scope], reason: str) -> None:
        names = set().union(*(v.vars.keys() for v in variants))
        for n in names:
            vals = [v.vars.get(n) for v in variants]
            if any(x is None for x in vals):
                base = scope.vars.get(n)
                vals = [x if x is not None else base for x in vals]
            if any(x is None for x in vals):
                scope.vars[n] = Unknown(f"{n!r} is bound only on some branches of a {reason}")
                continue
            merged = vals[0]
            for x in vals[1:]:
                merged = merge_values(merged, x, f"{n!r} differs between branches of a {reason}")  # type: ignore[arg-type]
            scope.vars[n] = merged  # type: ignore[assignment]

    def _branches(self, bodies: list[list[ast.stmt]], scope: Scope, ctx: Ctx, reason: str) -> None:
        variants = []
        bctx = replace(ctx, branch=True)
        for body in bodies:
            s = scope.copy()
            try:
                self.exec_block(body, s, bctx)
            except _Return:
                pass
            variants.append(s)
        self._merge_into(scope, variants, reason)

    def _if(self, st: ast.If, scope: Scope, ctx: Ctx) -> None:
        test = self._truth(st.test, scope, ctx)
        if test is True:
            self.exec_block(st.body, scope, ctx)
        elif test is False:
            self.exec_block(st.orelse, scope, ctx)
        else:
            self._branches([st.body, st.orelse], scope, ctx, "conditional")

    def _truth(self, test: ast.expr, scope: Scope, ctx: Ctx) -> bool | None:
        # `if __name__ == "__main__":` never runs when Airflow parses a DAG file
        if (isinstance(test, ast.Compare) and isinstance(test.left, ast.Name) and test.left.id == "__name__"
                and len(test.comparators) == 1 and isinstance(test.comparators[0], ast.Constant)
                and test.comparators[0].value == "__main__"):
            return isinstance(test.ops[0], ast.NotEq)
        v = self.eval(test, scope, Ctx(emit=False))
        if isinstance(v, Const) and isinstance(v.value, (bool, int, str, type(None))):
            return bool(v.value)
        return None

    def _try(self, st: Any, scope: Scope, ctx: Ctx) -> None:
        bodies = [st.body + st.orelse] + [h.body for h in st.handlers]
        self._branches(bodies, scope, ctx, "try/except")
        self.exec_block(st.finalbody, scope, ctx)

    def _loop(self, st: ast.For | ast.AsyncFor | ast.While, scope: Scope, ctx: Ctx) -> None:
        kind = "while-loop" if isinstance(st, ast.While) else "for-loop"
        lctx = replace(ctx, dynamic=ctx.dynamic or f"inside a {kind} (line {st.lineno}): tasks created a dynamic number of times")
        inner = scope.copy()
        if isinstance(st, (ast.For, ast.AsyncFor)):
            self.eval(st.iter, scope, ctx)
            self._bind(st.target, Unknown(f"loop variable of the {kind} at line {st.lineno}"), inner, lctx)
        else:
            self.eval(st.test, scope, Ctx(emit=False))
        try:
            self.exec_block(st.body, inner, lctx)
            self.exec_block(st.orelse, inner, lctx)
        except _Return:
            pass
        for n, v in inner.vars.items():
            if scope.vars.get(n) is not v:
                scope.vars[n] = Unknown(f"{n!r} is assigned inside a {kind} (line {st.lineno})")

    def _with(self, st: ast.With | ast.AsyncWith, scope: Scope, ctx: Ctx) -> None:
        new_ctx = ctx
        for item in st.items:
            v = self.eval(item.context_expr, scope, new_ctx)
            if isinstance(v, DagV):
                new_ctx = replace(new_ctx, dag=_dag_info(v, item.context_expr), group_prefix=())
            elif isinstance(v, TaskV) and getattr(v.handle, "kind", "") in ("task_group", "cosmos_task_group"):
                gid = v.handle.task_id
                if v.handle.kind == "task_group" and gid is not None:
                    pref = getattr(v.handle, "prefix_parts", None)
                    new_ctx = replace(new_ctx, group_prefix=tuple(pref) if pref is not None else new_ctx.group_prefix)
                elif gid is None:
                    new_ctx = replace(new_ctx, dynamic=new_ctx.dynamic or "inside a TaskGroup whose group_id is not resolvable")
            if item.optional_vars is not None:
                self._bind(item.optional_vars, v, scope, new_ctx)
        self.exec_block(st.body, scope, new_ctx)

    def _funcdef(self, st: ast.FunctionDef | ast.AsyncFunctionDef, scope: Scope, ctx: Ctx) -> None:
        kind, dkw = self._decorator_kind(st, scope)
        fv = FuncV(st.name, st, kind, decorator_kwargs=dkw)
        fv.defining_scope = scope  # type: ignore[attr-defined]
        scope.vars[st.name] = fv
        if ctx.funcs:
            self.called.add(id(st))  # nested functions are covered by their parent
        else:
            self.functions[st.name] = fv
        if kind in ("task", "task_bash"):
            self._subprocess_heuristic(st, st.name, ctx)
        if kind == "dag":
            dag_id_v = dkw.get("dag_id")
            dag_id = as_str(dag_id_v) if dag_id_v is not None else st.name
            reason = None if dag_id is not None else "dag_id in @dag(...) is not resolvable"
            conf = dag_id_v.conf if dag_id_v is not None else "declared"
            info = DagInfo(dag_id, reason, conf, st.lineno, st.col_offset)
            self._record_dag(info, "@dag", ctx)
            inner = Scope(scope)
            for a in _all_args(st.args):
                inner.vars[a] = Unknown(f"parameter {a!r} of @dag function {st.name!r} (DAG params are runtime values)")
            try:
                self.exec_block(st.body, inner, replace(ctx, dag=info, group_prefix=(), funcs=ctx.funcs + (st.name,)))
            except _Return:
                pass
        elif kind == "plain" and not ctx.funcs:
            self._subprocess_heuristic(st, st.name, ctx)

    def _decorator_kind(self, st: ast.FunctionDef | ast.AsyncFunctionDef, scope: Scope) -> tuple[str, dict[str, Val]]:
        for dec in st.decorator_list:
            call_kwargs: dict[str, Val] = {}
            target = dec
            if isinstance(dec, ast.Call):
                target = dec.func
                for kw in dec.keywords:
                    if kw.arg:
                        call_kwargs[kw.arg] = self.eval(kw.value, scope, Ctx(emit=False))
                if dec.args and isinstance(dec.func, (ast.Name, ast.Attribute)):
                    call_kwargs.setdefault("_pos0", self.eval(dec.args[0], scope, Ctx(emit=False)))
            name = self._dotted_name(target, scope)
            if not name:
                continue
            last = name.rsplit(".", 1)[-1]
            parts = name.split(".")
            if last == "dag":
                return "dag", call_kwargs
            if last == "task_group":
                return "task_group", call_kwargs
            if last == "bash" and len(parts) >= 2 and parts[-2] == "task":
                return "task_bash", call_kwargs
            if last == "task" or (len(parts) >= 2 and parts[-2] == "task"):
                return "task", call_kwargs
        return "plain", {}

    def _dotted_name(self, node: ast.expr, scope: Scope) -> str | None:
        if isinstance(node, ast.Name):
            v = scope.lookup(node.id)
            if isinstance(v, Sym):
                return v.name
            return node.id if v is None else None
        if isinstance(node, ast.Attribute):
            base = self._dotted_name(node.value, scope)
            return f"{base}.{node.attr}" if base else None
        return None

    def _subprocess_heuristic(self, fn: ast.AST, name: str, ctx: Ctx) -> None:
        if not ctx.emit:
            return
        for node in ast.walk(fn):
            if not isinstance(node, ast.Call):
                continue
            fname = self._dotted_name(node.func, self.scope) or ""
            is_runner = fname.endswith(SUBPROCESS_FUNCS) or (
                isinstance(node.func, ast.Attribute) and node.func.attr == "invoke"
                and "dbtRunner" in (ast.get_source_segment(self.source.decode("utf-8", "replace"), node.func) or "")
            )
            if not is_runner:
                continue
            seg = ast.get_source_segment(self.source.decode("utf-8", "replace"), node) or ""
            if mentions_dbt(seg) or "'dbt'" in seg or '"dbt"' in seg:
                self._unresolved(node.lineno, node.col_offset,
                                 f"dbt invoked from Python code in {name!r} ({fname or 'dbtRunner.invoke'}); not analysed",
                                 ctx, category="python_dbt_call")

    # --- expressions ----------------------------------------------------------------------
    def eval(self, node: ast.expr | None, scope: Scope, ctx: Ctx) -> Val:
        if node is None:
            return Const(None)
        meth = getattr(self, f"_e_{type(node).__name__}", None)
        if meth is None:
            self._scan_nested_operators(node, scope, ctx, f"{type(node).__name__} expression")
            return Unknown(f"{type(node).__name__} expressions are not evaluated", line=getattr(node, "lineno", 0))
        return meth(node, scope, ctx)

    def _e_Constant(self, node: ast.Constant, scope: Scope, ctx: Ctx) -> Val:
        return Const(node.value)

    def _e_JoinedStr(self, node: ast.JoinedStr, scope: Scope, ctx: Ctx) -> Val:
        out: list[str] = []
        confs: list[str] = ["inferred"]
        notes: list[str] = []
        for part in node.values:
            if isinstance(part, ast.Constant):
                out.append(str(part.value))
                continue
            if isinstance(part, ast.FormattedValue):
                if part.format_spec is not None:
                    return Unknown("f-string with a format spec", line=node.lineno)
                v = self.eval(part.value, scope, ctx)
                s = _to_str(v)
                if s is None:
                    seg = ast.get_source_segment(self.source.decode("utf-8", "replace"), part.value) or "?"
                    return Unknown(f"f-string part {{{seg}}} is not resolvable" + (f" ({v.reason})" if isinstance(v, Unknown) else ""),
                                   line=node.lineno)
                if part.conversion == ord("r"):
                    s = repr(s)
                out.append(s)
                confs.append(v.conf)
                notes.extend(v.notes)
        return Const("".join(out), conf=weakest(*confs), notes=tuple(dict.fromkeys(notes)))

    def _e_Name(self, node: ast.Name, scope: Scope, ctx: Ctx) -> Val:
        v = scope.lookup(node.id)
        if v is None:
            if node.id in ("True", "False", "None"):
                return Const({"True": True, "False": False, "None": None}[node.id])
            return Unknown(f"name {node.id!r} is not bound", line=node.lineno)
        if isinstance(v, Sym) and node.id in self.imports:
            resolved = self._resolve_import(node.id)
            if resolved is not None:
                return resolved
        return v

    def _resolve_import(self, local: str) -> Val | None:
        mod, attr, level = self.imports[local]
        path = self.loader.find(mod, self.path, level)
        if path is None:
            if attr:
                sub = self.loader.find(f"{mod}.{attr}" if mod else attr, self.path, level)
                if sub is not None:
                    return Sym(f"{mod}.{attr}")
            return None
        fs = self.loader.module(path, self.import_depth + 1)
        if fs is None or attr is None:
            return None
        v = fs.scope.vars.get(attr)
        if v is None:
            return None
        if isinstance(v, Sym) and attr in fs.imports:
            v = fs._resolve_import(attr) or v
        if isinstance(v, (Const, PathV, ListV, DictV, CallV)):
            rel = os.path.relpath(path, self.index.workspace)
            v = _clone(v)
            v.with_meta("inferred", (f"{local} resolved via import from {rel}",))
            return v
        if isinstance(v, FuncV):
            v.imported_from = path  # type: ignore[attr-defined]
            return v
        if isinstance(v, Sym):
            return v
        return None

    def _e_Attribute(self, node: ast.Attribute, scope: Scope, ctx: Ctx) -> Val:
        base = self.eval(node.value, scope, ctx)
        a = node.attr
        if isinstance(base, Sym):
            return Sym(f"{base.name}.{a}")
        if isinstance(base, PathV) or (isinstance(base, Const) and isinstance(base.value, str) and a in ("parent", "name", "stem")):
            p = base.path if isinstance(base, PathV) else base.value  # type: ignore[union-attr]
            if a == "parent":
                return PathV(posixpath.dirname(p.rstrip("/")) or "/", conf=weakest(base.conf, "inferred"), notes=base.notes)
            if a == "name":
                return Const(posixpath.basename(p.rstrip("/")), conf=base.conf, notes=base.notes)
            if a == "stem":
                return Const(posixpath.splitext(posixpath.basename(p.rstrip("/")))[0], conf=base.conf, notes=base.notes)
            if a == "parents":
                parts = []
                cur = p.rstrip("/")
                while cur not in ("", "/"):
                    cur = posixpath.dirname(cur)
                    parts.append(PathV(cur or "/", conf=base.conf, notes=base.notes))
                return ListV(parts, conf=base.conf, notes=base.notes)
            if a == "suffix":
                return Const(posixpath.splitext(p)[1], conf=base.conf)
        if isinstance(base, TaskV) and a == "output":
            return base
        if isinstance(base, Unknown):
            return base
        return Unknown(f"attribute .{a} of a {type(base).__name__} value", line=node.lineno)

    def _e_Subscript(self, node: ast.Subscript, scope: Scope, ctx: Ctx) -> Val:
        base = self.eval(node.value, scope, ctx)
        key = self.eval(node.slice, scope, ctx)
        if isinstance(base, Sym) and base.name in ("os.environ",):
            k = as_str(key)
            return Unknown(f"environment variable {k!r} read with os.environ[...] (no default)", line=node.lineno)
        if isinstance(base, DictV) and isinstance(key, Const) and isinstance(key.value, str):
            v = base.items.get(key.value)
            return v if v is not None else Unknown(f"key {key.value!r} not in dict", line=node.lineno)
        if isinstance(base, ListV) and isinstance(key, Const) and isinstance(key.value, int):
            try:
                return base.items[key.value]
            except IndexError:
                return Unknown("list index out of range", line=node.lineno)
        if isinstance(base, Unknown):
            return base
        return Unknown("subscript of a non-literal value", line=node.lineno)

    def _e_List(self, node: ast.List, scope: Scope, ctx: Ctx) -> Val:
        items: list[Val] = []
        for e in node.elts:
            if isinstance(e, ast.Starred):
                v = self.eval(e.value, scope, ctx)
                if isinstance(v, ListV):
                    items.extend(v.items)
                else:
                    items.append(Unknown("starred element", line=node.lineno))
            else:
                items.append(self.eval(e, scope, ctx))
        return ListV(items)

    _e_Tuple = _e_List
    _e_Set = _e_List

    def _e_Dict(self, node: ast.Dict, scope: Scope, ctx: Ctx) -> Val:
        items: dict[str, Val] = {}
        conf = "declared"
        for k, v in zip(node.keys, node.values):
            if k is None:
                spread = self.eval(v, scope, ctx)
                if isinstance(spread, DictV):
                    items.update(spread.items)
                    conf = weakest(conf, spread.conf)
                else:
                    return Unknown("dict built from a non-literal **spread", line=node.lineno)
                continue
            kv = self.eval(k, scope, ctx)
            vv = self.eval(v, scope, ctx)
            if isinstance(kv, Const) and isinstance(kv.value, (str, int, bool)):
                items[str(kv.value)] = vv
            else:
                items[f"<unknown key {len(items)}>"] = vv
        return DictV(items, conf=conf)

    def _e_IfExp(self, node: ast.IfExp, scope: Scope, ctx: Ctx) -> Val:
        t = self._truth(node.test, scope, ctx)
        if t is True:
            return self.eval(node.body, scope, ctx)
        if t is False:
            return self.eval(node.orelse, scope, ctx)
        a = self.eval(node.body, scope, replace(ctx, branch=True))
        b = self.eval(node.orelse, scope, replace(ctx, branch=True))
        return merge_values(a, b, "conditional expression with a non-literal test")

    def _e_BoolOp(self, node: ast.BoolOp, scope: Scope, ctx: Ctx) -> Val:
        vals = [self.eval(v, scope, ctx) for v in node.values]
        if isinstance(node.op, ast.Or):
            for i, v in enumerate(vals):
                if isinstance(v, Unknown) and v.reason.startswith("environment variable") and i + 1 < len(vals):
                    nxt = _clone(vals[i + 1])
                    return nxt.with_meta("heuristic", (f"{v.reason}; used the `or` fallback value",))
                if isinstance(v, Const):
                    if v.value:
                        return v
                    continue
                if isinstance(v, (PathV, ListV, DictV)):
                    return v
                return Unknown("boolean expression over non-literal values", line=node.lineno)
            return vals[-1]
        if all(isinstance(v, Const) for v in vals):
            result = vals[0]
            for v in vals:
                result = v
                if not v.value:  # type: ignore[union-attr]
                    break
            return result
        return Unknown("boolean expression over non-literal values", line=node.lineno)

    def _e_Compare(self, node: ast.Compare, scope: Scope, ctx: Ctx) -> Val:
        left = self.eval(node.left, scope, ctx)
        rights = [self.eval(c, scope, ctx) for c in node.comparators]
        if len(rights) == 1 and isinstance(left, Const) and isinstance(rights[0], Const):
            op = node.ops[0]
            a, b = left.value, rights[0].value
            try:
                if isinstance(op, ast.Eq):
                    return Const(a == b)
                if isinstance(op, ast.NotEq):
                    return Const(a != b)
                if isinstance(op, ast.In):
                    return Const(a in b)
                if isinstance(op, ast.NotIn):
                    return Const(a not in b)
            except TypeError:
                pass
        return Unknown("comparison over non-literal values", line=node.lineno)

    def _e_UnaryOp(self, node: ast.UnaryOp, scope: Scope, ctx: Ctx) -> Val:
        v = self.eval(node.operand, scope, ctx)
        if isinstance(v, Const):
            try:
                if isinstance(node.op, ast.Not):
                    return Const(not v.value)
                if isinstance(node.op, ast.USub):
                    return Const(-v.value)
            except TypeError:
                pass
        return Unknown("unary operation over a non-literal value", line=node.lineno)

    def _e_NamedExpr(self, node: ast.NamedExpr, scope: Scope, ctx: Ctx) -> Val:
        v = self.eval(node.value, scope, ctx)
        self._bind(node.target, v, scope, ctx)
        return v

    def _e_Lambda(self, node: ast.Lambda, scope: Scope, ctx: Ctx) -> Val:
        return Unknown("lambda", line=node.lineno)

    def _comprehension(self, node: ast.expr, scope: Scope, ctx: Ctx) -> Val:
        self._scan_nested_operators(node, scope, ctx, "comprehension")
        return Unknown("comprehension (dynamic)", line=node.lineno)

    _e_ListComp = _comprehension
    _e_SetComp = _comprehension
    _e_DictComp = _comprehension
    _e_GeneratorExp = _comprehension

    def _scan_nested_operators(self, node: ast.AST, scope: Scope, ctx: Ctx, where: str) -> None:
        """Operators created inside expressions we do not evaluate (comprehensions) become unknown."""
        if not ctx.emit:
            return
        for sub in ast.walk(node):
            if isinstance(sub, ast.Call) and sub is not node:
                name = self._dotted_name(sub.func, scope) or ""
                kind = _classify(name)
                if kind and kind not in ("dag", "task_group", "chain", "generic"):
                    self._unresolved(sub.lineno, sub.col_offset,
                                     f"{name.rsplit('.', 1)[-1]} created inside a {where}; tasks are dynamic",
                                     ctx, dag_id=ctx.dag.dag_id if ctx.dag else None, category="dynamic")

    def _e_BinOp(self, node: ast.BinOp, scope: Scope, ctx: Ctx) -> Val:
        left = self.eval(node.left, scope, ctx)
        right = self.eval(node.right, scope, ctx)
        return self._binop(node.op, left, right, node, ctx)

    def _binop(self, op: ast.operator, left: Val, right: Val, node: ast.AST, ctx: Ctx) -> Val:
        line = getattr(node, "lineno", 0)
        col = getattr(node, "col_offset", 0)
        if isinstance(op, (ast.RShift, ast.LShift)):
            lt, rt = _task_list(left), _task_list(right)
            if lt is None and rt is None:
                return Unknown("bit shift over non-task values", line=line)
            if lt is None or rt is None:
                self._unresolved(line, col, "dependency operand is not a known task (>>/<<)", ctx,
                                 dag_id=ctx.dag.dag_id if ctx.dag else None, category="dependency")
                return right
            ups, downs = (lt, rt) if isinstance(op, ast.RShift) else (rt, lt)
            for u in ups:
                for d in downs:
                    self._edge(u, d, line, col, ctx, ">>" if isinstance(op, ast.RShift) else "<<")
            return right
        if isinstance(op, ast.Div) and isinstance(left, PathV):
            s = _to_str(right)
            if s is None:
                return Unknown("path joined with a non-literal part", line=line)
            j = left.join(s)
            conf, notes = collect_meta(left, right)
            j.conf, j.notes = weakest(conf, "inferred"), notes
            return j
        if isinstance(op, ast.Add):
            if isinstance(left, Const) and isinstance(right, Const) and isinstance(left.value, str) and isinstance(right.value, str):
                conf, notes = collect_meta(left, right)
                return Const(left.value + right.value, conf=weakest(conf, "inferred"), notes=notes)
            if isinstance(left, ListV) and isinstance(right, ListV):
                return ListV(left.items + right.items, conf=weakest(left.conf, right.conf))
        if isinstance(op, ast.Mod) and isinstance(left, Const) and isinstance(left.value, str):
            args: Any
            if isinstance(right, ListV):
                args = tuple(_to_str(i) for i in right.items)
                if any(a is None for a in args):
                    return Unknown("%-format with non-literal arguments", line=line)
            elif isinstance(right, DictV):
                return Unknown("%-format with a mapping", line=line)
            else:
                args = _to_str(right)
                if args is None:
                    return Unknown("%-format with a non-literal argument", line=line)
            try:
                return Const(left.value % args, conf="inferred")
            except (TypeError, ValueError):
                return Unknown("%-format failed", line=line)
        if isinstance(left, Unknown):
            return left
        if isinstance(right, Unknown):
            return right
        return Unknown(f"{type(op).__name__} over these values is not evaluated", line=line)

    # --- calls ----------------------------------------------------------------------------
    def _e_Call(self, node: ast.Call, scope: Scope, ctx: Ctx) -> Val:
        func = node.func
        # method calls on evaluated values
        if isinstance(func, ast.Attribute):
            base = self.eval(func.value, scope, ctx)
            if not isinstance(base, Sym):
                return self._method_call(base, func.attr, node, scope, ctx)
            fname = f"{base.name}.{func.attr}"
            fval: Val = Sym(fname)
        else:
            fval = self.eval(func, scope, ctx) if not isinstance(func, ast.Name) else (scope.lookup(func.id) or Sym(func.id))
            fname = fval.name if isinstance(fval, Sym) else (func.id if isinstance(func, ast.Name) else "")
        args, kwargs, has_star = self._call_args(node, scope, ctx)
        if isinstance(fval, FuncV):
            return self._call_function(fval, node, args, kwargs, has_star, scope, ctx)
        return self._call_named(fname, node, args, kwargs, has_star, scope, ctx)

    def _call_args(self, node: ast.Call, scope: Scope, ctx: Ctx) -> tuple[list[Val], dict[str, Val], bool]:
        args: list[Val] = []
        kwargs: dict[str, Val] = {}
        has_star = False
        for a in node.args:
            if isinstance(a, ast.Starred):
                v = self.eval(a.value, scope, ctx)
                if isinstance(v, ListV):
                    args.extend(v.items)
                else:
                    has_star = True
            else:
                args.append(self.eval(a, scope, ctx))
        for kw in node.keywords:
            v = self.eval(kw.value, scope, ctx)
            if kw.arg is None:
                if isinstance(v, DictV):
                    kwargs.update(v.items)
                else:
                    has_star = True
            else:
                kwargs[kw.arg] = v
        return args, kwargs, has_star

    def _method_call(self, base: Val, meth: str, node: ast.Call, scope: Scope, ctx: Ctx) -> Val:
        args, kwargs, _ = self._call_args(node, scope, ctx)
        line = node.lineno
        if isinstance(base, PathV) or (isinstance(base, Const) and isinstance(base.value, str) and meth in ("joinpath",)):
            p = base.path if isinstance(base, PathV) else base.value  # type: ignore[union-attr]
            pv = base if isinstance(base, PathV) else PathV(p, conf=base.conf)
            if meth in ("resolve", "absolute"):
                return PathV(posixpath.normpath(p), conf=weakest(base.conf, "inferred"), notes=base.notes)
            if meth in ("as_posix", "__str__", "__fspath__"):
                return Const(p, conf=base.conf, notes=base.notes)
            if meth == "joinpath":
                out = pv
                for a in args:
                    s = _to_str(a)
                    if s is None:
                        return Unknown("joinpath with a non-literal part", line=line)
                    out = out.join(s)
                conf, notes = collect_meta(base, *args)
                out.conf, out.notes = weakest(conf, "inferred"), notes
                return out
            if meth in ("exists", "is_dir", "is_file"):
                return Unknown(f"filesystem check .{meth}() at parse time", line=line)
            if meth == "with_name" and args and _to_str(args[0]) is not None:
                return PathV(posixpath.join(posixpath.dirname(p), _to_str(args[0])), conf="inferred")  # type: ignore[arg-type]
        if isinstance(base, Const) and isinstance(base.value, str):
            s = base.value
            try:
                if meth == "format":
                    sargs = [_to_str(a) for a in args]
                    skw = {k: _to_str(v) for k, v in kwargs.items()}
                    if any(a is None for a in sargs) or any(v is None for v in skw.values()):
                        return Unknown(".format() with non-literal arguments", line=line)
                    return Const(s.format(*sargs, **skw), conf=weakest(base.conf, "inferred"))
                if meth == "replace" and len(args) >= 2 and all(_to_str(a) is not None for a in args[:2]):
                    return Const(s.replace(_to_str(args[0]), _to_str(args[1])), conf=weakest(base.conf, "inferred"))  # type: ignore[arg-type]
                if meth in ("strip", "lstrip", "rstrip", "lower", "upper") and not args:
                    return Const(getattr(s, meth)(), conf=base.conf)
                if meth == "join" and args and isinstance(args[0], ListV):
                    parts = [_to_str(i) for i in args[0].items]
                    if any(p is None for p in parts):
                        return Unknown("str.join over non-literal items", line=line)
                    return Const(s.join(parts), conf=weakest(base.conf, "inferred"))  # type: ignore[arg-type]
                if meth == "split":
                    sep = _to_str(args[0]) if args else None
                    return ListV([Const(x) for x in s.split(sep)], conf=base.conf)
            except (IndexError, KeyError, ValueError):
                return Unknown(f"str.{meth}() failed on literal arguments", line=line)
        if isinstance(base, DictV):
            if meth == "get" and args and isinstance(args[0], Const):
                v = base.items.get(str(args[0].value))
                if v is not None:
                    return v
                return args[1] if len(args) > 1 else Const(None)
            if meth == "copy":
                return DictV(dict(base.items), conf=base.conf, notes=base.notes)
        if isinstance(base, TaskV) and meth in ("set_upstream", "set_downstream"):
            others = _task_list(args[0]) if args else None
            if others is None:
                self._unresolved(line, node.col_offset, f".{meth}() argument is not a known task", ctx,
                                 dag_id=ctx.dag.dag_id if ctx.dag else None, category="dependency")
            else:
                for o in others:
                    if meth == "set_downstream":
                        self._edge(base, o, line, node.col_offset, ctx, meth)
                    else:
                        self._edge(o, base, line, node.col_offset, ctx, meth)
            return Const(None)
        if isinstance(base, PartialV) and meth in ("expand", "expand_kwargs"):
            if meth == "expand_kwargs":
                merged = dict(base.kwargs)
                return self._create_task(base.func, node, [], merged, False, scope,
                                         replace(ctx, dynamic=ctx.dynamic or ".expand_kwargs() mapping over non-literal input"))
            merged = dict(base.kwargs)
            dyn = None
            for k, v in kwargs.items():
                if not isinstance(v, ListV):
                    dyn = f".expand({k}=...) over a non-literal value"
                merged[k] = v
            mctx = replace(ctx, dynamic=ctx.dynamic or dyn)
            return self._create_task(base.func, node, [], merged, False, scope, mctx, mapped=set(kwargs))
        if isinstance(base, (TaskV, DagV, CallV, FuncV)) or isinstance(base, Unknown):
            if isinstance(base, Unknown):
                return base
            return Unknown(f".{meth}() on a {type(base).__name__}", line=line)
        return Unknown(f".{meth}() on a {type(base).__name__} value", line=line)

    def _call_named(self, fname: str, node: ast.Call, args: list[Val], kwargs: dict[str, Val], has_star: bool,
                    scope: Scope, ctx: Ctx) -> Val:
        line, col = node.lineno, node.col_offset
        last = fname.rsplit(".", 1)[-1]
        # environment and Airflow Variables: use the default, flagged heuristic
        if fname in ("os.getenv", "os.environ.get", "getenv", "environ.get") or fname.endswith(".environ.get"):
            key = as_str(args[0]) if args else None
            default = args[1] if len(args) > 1 else kwargs.get("default")
            if default is None or (isinstance(default, Const) and default.value is None):
                return Unknown(f"environment variable {key!r} has no default", line=line)
            d = _clone(default)
            return d.with_meta("heuristic", (f"environment variable {key!r} assumed unset; used its default",))
        if fname.endswith("Variable.get"):
            key = as_str(args[0]) if args else None
            default = kwargs.get("default_var") or kwargs.get("default") or (args[1] if len(args) > 1 else None)
            if default is None or (isinstance(default, Const) and default.value is None):
                return Unknown(f"Airflow Variable {key!r} has no default", line=line)
            return _clone(default).with_meta("heuristic", (f"Airflow Variable {key!r} assumed unset; used default_var",))
        if fname in ("os.path.join", "posixpath.join", "path.join") or (fname.endswith("path.join") and fname.startswith("os")):
            parts = [_to_str(a) for a in args]
            if any(p is None for p in parts) or not parts:
                return Unknown("os.path.join with non-literal parts", line=line)
            conf, notes = collect_meta(*args)
            return Const(posixpath.join(*parts), conf=weakest(conf, "inferred"), notes=notes)  # type: ignore[arg-type]
        if fname in ("os.path.dirname", "os.path.abspath", "os.path.realpath", "os.path.normpath", "os.path.basename"):
            s = _to_str(args[0]) if args else None
            if s is None:
                return Unknown(f"{fname} of a non-literal value", line=line)
            conf, notes = collect_meta(*args)
            fn = {"os.path.dirname": posixpath.dirname, "os.path.basename": posixpath.basename}.get(fname, posixpath.normpath)
            if fname in ("os.path.abspath", "os.path.realpath") and not s.startswith("/"):
                return Unknown(f"{fname} of a relative path depends on the working directory", line=line)
            return Const(fn(s), conf=weakest(conf, "inferred"), notes=notes)
        if fname in ("os.getcwd", "pathlib.Path.cwd", "Path.cwd", "pathlib.Path.home", "os.path.expanduser"):
            return Unknown(f"{fname}() depends on the runtime environment", line=line)
        if fname in ("pathlib.Path", "pathlib.PurePath", "pathlib.PosixPath", "pathlib.PurePosixPath"):
            if not args:
                return Unknown("Path() with no arguments is the working directory", line=line)
            parts = [_to_str(a) for a in args]
            if any(p is None for p in parts):
                bad = next(a for a, p in zip(args, parts) if p is None)
                return Unknown("Path() of a non-literal value" + (f": {bad.reason}" if isinstance(bad, Unknown) else ""), line=line)
            conf, notes = collect_meta(*args)
            out = PathV(posixpath.normpath(posixpath.join(*parts)), conf=conf, notes=notes)  # type: ignore[arg-type]
            return out
        if last in BUILTIN_CALLS and fname == last and scope.lookup(last) is None:
            if last == "str" and args:
                s = _to_str(args[0])
                return Const(s, conf=args[0].conf, notes=args[0].notes) if s is not None else Unknown("str() of a non-literal", line=line)
            if last == "dict":
                return DictV(dict(kwargs)) if not args else Unknown("dict() with positional args", line=line)
            if last in ("list", "tuple") and args and isinstance(args[0], ListV):
                return args[0]
            return Unknown(f"builtin {last}() is not evaluated", line=line)
        if last == "partial" and fname.count("."):
            owner = fname.rsplit(".", 1)[0]
            if _classify(owner) is not None:
                return PartialV(owner, kwargs, line=line, col=col)
        kind = _classify(fname)
        if kind is not None:
            return self._create_task(fname, node, args, kwargs, has_star, scope, ctx)
        if last in ("open", "load", "loads", "read_text", "load_manifest") and any(
            "manifest.json" in (_to_str(a) or "") for a in args
        ):
            self._unresolved(line, col, "reads a dbt manifest.json at DAG-parse time; tasks generated from it are dynamic",
                             ctx, dag_id=ctx.dag.dag_id if ctx.dag else None, category="dynamic")
            return Unknown("contents of manifest.json read at parse time", line=line)
        self._maybe_wrapper(fname, _root_name(node.func), node, kwargs, ctx)
        return CallV(fname, args, kwargs, line=line, col=col, has_star=has_star)

    def _maybe_wrapper(self, fname: str, local: str | None, node: ast.Call, kwargs: dict[str, Val], ctx: Ctx) -> None:
        """Flag calls that may create dbt tasks we cannot see: workspace wrappers and dbt-flavoured calls."""
        if not ctx.emit or fname.startswith(KNOWN_LIBRARY_PREFIXES):
            return
        dbt_kw = [k for k in kwargs if "dbt" in k.lower()]
        info = None
        if local and local in self.imports:
            mod, attr, level = self.imports[local]
            path = self.loader.find(mod, self.path, level)
            if path is not None and attr:
                info = _operators_defined_in(path, attr)
                if info:
                    rel = os.path.relpath(path, self.index.workspace)
                    self._unresolved(node.lineno, node.col_offset,
                                     f"{fname}(...) is defined in {rel} and creates {', '.join(sorted(info))} at parse time "
                                     f"from non-literal inputs; tasks not resolved statically", ctx,
                                     dag_id=ctx.dag.dag_id if ctx.dag else None, category="wrapper")
                    return
        if dbt_kw:
            self._unresolved(node.lineno, node.col_offset,
                             f"unrecognised call {fname}(...) with dbt-related arguments {dbt_kw}; may create dbt tasks",
                             ctx, dag_id=ctx.dag.dag_id if ctx.dag else None, category="wrapper")

    def _call_function(self, fv: FuncV, node: ast.Call, args: list[Val], kwargs: dict[str, Val], has_star: bool,
                       scope: Scope, ctx: Ctx) -> Val:
        if fv.kind == "task":
            return self._decorated_task(fv, node, ctx, None)
        if fv.kind == "task_bash":
            self.called.add(id(fv.node))
            cmd = self._exec_function(fv, node, args, kwargs, has_star, replace(ctx, emit=False))
            return self._decorated_task(fv, node, ctx, cmd)
        if fv.kind == "dag":
            return DagV(as_str(fv.decorator_kwargs.get("dag_id")) or fv.name)
        if getattr(fv, "imported_from", None):
            self._unresolved(node.lineno, node.col_offset, f"call to {fv.name}() imported from another module is not followed", ctx,
                             category="wrapper")
            return Unknown(f"call to imported function {fv.name}", line=node.lineno)
        if not self.loader.inline_calls and fv.kind == "plain":
            return Unknown(f"call to {fv.name}() not followed (call inlining disabled)", line=node.lineno)
        self.called.add(id(fv.node))
        gctx = ctx
        handle = None
        if fv.kind == "task_group":
            gid = as_str(fv.decorator_kwargs.get("group_id") or fv.decorator_kwargs.get("_pos0")) or fv.name
            handle = TaskHandle(ctx.dag.dag_id if ctx.dag else None, ctx.prefix + gid, "task_group", node.lineno, node.col_offset)
            handle.prefix_parts = ctx.group_prefix + (gid,)  # type: ignore[attr-defined]
            gctx = replace(ctx, group_prefix=ctx.group_prefix + (gid,))
        ret = self._exec_function(fv, node, args, kwargs, has_star, gctx)
        return TaskV(handle) if handle is not None else ret

    def _exec_function(self, fv: FuncV, node: ast.Call, args: list[Val], kwargs: dict[str, Val], has_star: bool,
                       ctx: Ctx) -> Val:
        if self._call_depth >= MAX_CALL_DEPTH or fv.name in ctx.funcs:
            return Unknown(f"call to {fv.name}() not followed (depth/recursion limit)", line=node.lineno)
        fn = fv.node
        inner = Scope(getattr(fv, "defining_scope", self.scope))
        params = fn.args
        positional = [a.arg for a in params.posonlyargs + params.args]
        defaults = params.defaults
        dscope = getattr(fv, "defining_scope", self.scope)
        for i, name in enumerate(positional):
            di = i - (len(positional) - len(defaults))
            if i < len(args):
                inner.vars[name] = args[i]
            elif name in kwargs:
                inner.vars[name] = kwargs[name]
            elif di >= 0:
                inner.vars[name] = self.eval(defaults[di], dscope, Ctx(emit=False))
            else:
                inner.vars[name] = Unknown(f"parameter {name!r} of {fv.name}() not passed")
        for a, d in zip(params.kwonlyargs, params.kw_defaults):
            if a.arg in kwargs:
                inner.vars[a.arg] = kwargs[a.arg]
            elif d is not None:
                inner.vars[a.arg] = self.eval(d, dscope, Ctx(emit=False))
            else:
                inner.vars[a.arg] = Unknown(f"parameter {a.arg!r} not passed")
        if params.vararg:
            inner.vars[params.vararg.arg] = ListV(args[len(positional):]) if not has_star else Unknown("*args")
        if params.kwarg:
            inner.vars[params.kwarg.arg] = DictV({k: v for k, v in kwargs.items() if k not in positional}) if not has_star else Unknown("**kwargs")
        if has_star:
            for name in positional:
                if name not in kwargs and positional.index(name) >= len(args):
                    inner.vars[name] = Unknown(f"parameter {name!r} may come from *args/**kwargs")
        if any(isinstance(n, (ast.Yield, ast.YieldFrom)) for n in ast.walk(fn)):
            return Unknown(f"generator function {fv.name}()", line=node.lineno)
        cctx = replace(ctx, funcs=ctx.funcs + (fv.name,), call_sites=ctx.call_sites + ((node.lineno, node.col_offset),))
        self._call_depth += 1
        try:
            self.exec_block(fn.body, inner, cctx)
        except _Return as r:
            return r.args[0]
        finally:
            self._call_depth -= 1
        return Const(None)

    def _decorated_task(self, fv: FuncV, node: ast.Call, ctx: Ctx, bash_cmd: Val | None) -> Val:
        tid_v = fv.decorator_kwargs.get("task_id")
        tid = as_str(tid_v) if tid_v is not None else fv.name
        dag = ctx.dag
        handle = TaskHandle(dag.dag_id if dag else None, (ctx.prefix + tid) if tid else None, "decorated_task", node.lineno, node.col_offset)
        self._record_task(handle, "@task.bash" if fv.kind == "task_bash" else "@task", ctx)
        if fv.kind == "task_bash":
            self._shell_task("task_bash", handle, bash_cmd, node, ctx, {}, Const(tid) if tid else Unknown("task_id"))
        return TaskV(handle)

    # --- task creation --------------------------------------------------------------------
    def _create_task(self, fname: str, node: ast.Call, args: list[Val], kwargs: dict[str, Val], has_star: bool,
                     scope: Scope, ctx: Ctx, mapped: set[str] | None = None) -> Val:
        kind = _classify(fname)
        line, col = node.lineno, node.col_offset
        if kind == "dag":
            dag_id_v = kwargs.get("dag_id") or (args[0] if args else None)
            dag_id = as_str(dag_id_v)
            reason = None
            if dag_id is None:
                reason = "dag_id is not statically resolvable" + (f": {dag_id_v.reason}" if isinstance(dag_id_v, Unknown) else "")
            dv = DagV(dag_id, reason, conf=dag_id_v.conf if dag_id_v is not None and dag_id is not None else "declared",
                      notes=dag_id_v.notes if dag_id_v is not None else ())
            self._record_dag(_dag_info(dv, node), "DAG()", ctx)
            return dv
        if kind == "chain":
            items = [_task_list(a) for a in args]
            if any(i is None for i in items):
                self._unresolved(line, col, f"{fname}() has an argument that is not a known task", ctx, category="dependency")
            elif fname.rsplit(".", 1)[-1] == "cross_downstream" and len(items) == 2:
                for u in items[0]:  # type: ignore[union-attr]
                    for d in items[1]:  # type: ignore[union-attr]
                        self._edge(u, d, line, col, ctx, "cross_downstream")
            else:
                for a, b in zip(items, items[1:]):
                    if len(a) > 1 and len(b) > 1:  # type: ignore[arg-type]
                        if len(a) != len(b):  # type: ignore[arg-type]
                            self._unresolved(line, col, "chain() with lists of different lengths", ctx, category="dependency")
                            continue
                        pairs = list(zip(a, b))  # type: ignore[arg-type]
                    else:
                        pairs = [(u, d) for u in a for d in b]  # type: ignore[union-attr]
                    for u, d in pairs:
                        self._edge(u, d, line, col, ctx, "chain")
            return Const(None)
        dag_kw = kwargs.get("dag")
        dag = ctx.dag
        if isinstance(dag_kw, DagV):
            dag = _dag_info(dag_kw, node)
        dag_id = dag.dag_id if dag else None
        if kind == "task_group":
            gid_v = kwargs.get("group_id") or (args[0] if args else None)
            gid = as_str(gid_v)
            prefix_on = kwargs.get("prefix_group_id")
            no_prefix = isinstance(prefix_on, Const) and prefix_on.value is False
            handle = TaskHandle(dag_id, (ctx.prefix + gid) if gid else None, "task_group", line, col)
            handle.prefix_parts = ctx.group_prefix if no_prefix else (ctx.group_prefix + (gid,) if gid else None)  # type: ignore[attr-defined]
            self._record_task(handle, fname, ctx)
            return TaskV(handle)
        if kind in ("cosmos_dag", "cosmos_task_group"):
            call = CallV(fname, args, kwargs, line=line, col=col, has_star=has_star)
            return self._cosmos(kind, call, node, ctx, dag)
        tid_v = kwargs.get("task_id")
        tid = as_str(tid_v)
        handle = TaskHandle(dag_id, (ctx.prefix + tid) if tid else None, "operator", line, col)
        self._record_task(handle, fname, ctx)
        if kind == "cosmos_operator":
            self._cosmos_operator(fname, handle, node, kwargs, ctx, dag, tid_v, mapped or set())
        elif kind == "airflow_dbt_operator":
            self._airflow_dbt_operator(fname, handle, node, kwargs, ctx, tid_v)
        elif kind in ("bash", "ssh"):
            cmd_key = "bash_command" if kind == "bash" else "command"
            self._shell_task(kind, handle, kwargs.get(cmd_key), node, ctx, kwargs, tid_v, mapped=mapped)
        elif kind == "pod":
            self._argv_task(kind, handle, node, ctx, kwargs, tid_v)
        elif kind == "docker":
            cmd = kwargs.get("command")
            if isinstance(cmd, ListV):
                self._argv_task(kind, handle, node, ctx, {"cmds": cmd}, tid_v)
            else:
                self._shell_task(kind, handle, cmd, node, ctx, kwargs, tid_v)
        return TaskV(handle)

    # --- records for DAGs, tasks and edges ---------------------------------------------------
    def _record_dag(self, info: DagInfo, how: str, ctx: Ctx) -> None:
        if not ctx.emit:
            return
        key = ("dag", info.line, info.col, ctx.call_sites)
        if key in self._seen:
            return
        self._seen.add(key)
        self.dags.append({"dag_id": info.dag_id, "how": how, "location": self._loc(info.line, info.col), "confidence": info.conf})
        if info.dag_id is None:
            self._unresolved(info.line, info.col, info.reason or "dag_id not resolvable", ctx, category="dag_id")

    def _record_task(self, handle: TaskHandle, fname: str, ctx: Ctx) -> None:
        if not ctx.emit:
            return
        key = ("task", handle.line, handle.col, ctx.call_sites)
        if key in self._seen:
            return
        self._seen.add(key)
        self.tasks.append({"dag_id": handle.dag_id, "task_id": handle.task_id, "operator": fname.rsplit(".", 1)[-1],
                           "kind": handle.kind, "location": self._loc(handle.line, handle.col),
                           "dynamic": ctx.dynamic})

    def _edge(self, up: TaskHandle, down: TaskHandle, line: int, col: int, ctx: Ctx, how: str) -> None:
        if not ctx.emit:
            return
        key = ("edge", up.dag_id or down.dag_id, up.task_id, down.task_id, id(up) if not up.task_id else 0, id(down) if not down.task_id else 0)
        if key in self._seen:
            return
        self._seen.add(key)
        dag_id = up.dag_id or down.dag_id
        rec = {
            "edge": "task_depends_on", "origin": ORIGIN, "dag_id": dag_id,
            "upstream": f"airflow:{dag_id}.{up.task_id}" if up.task_id else None,
            "downstream": f"airflow:{dag_id}.{down.task_id}" if down.task_id else None,
            "how": how, "confidence": "declared" if not ctx.dynamic else "heuristic",
            "location": self._loc(line, col),
        }
        if ctx.dynamic or not up.task_id or not down.task_id:
            rec["status"] = "unknown"
            rec["reason"] = ctx.dynamic or "task id not resolvable"
        self.dependencies.append(rec)

    # --- dbt links ------------------------------------------------------------------------
    def _base_link(self, kind: str, node: ast.AST, ctx: Ctx, dag: DagInfo | None, task_id: str | None,
                   group_id: str | None = None) -> dict[str, Any]:
        rec: dict[str, Any] = {
            "origin": ORIGIN, "kind": kind, "confidence": "declared",
            "location": self._loc(node.lineno, node.col_offset),  # type: ignore[attr-defined]
            "dag_id": dag.dag_id if dag else None,
            "task_id": task_id, "group_id": group_id,
            "dbt_command": None, "project_path": None, "manifest": None,
            "select": [], "exclude": [], "selector": None,
            "resolved_models": None, "resolved_nodes": None,
            "status": "unknown", "unknown_reasons": [], "notes": [],
        }
        if ctx.call_sites:
            # report the outermost call site (what a user navigates to); keep where the call was built
            rec["constructed_at"] = rec["location"]
            rec["location"] = self._loc(*ctx.call_sites[0])
            rec["via_call_at"] = [list(cs) for cs in ctx.call_sites]
        if dag is None:
            rec["unknown_reasons"].append("task is not inside a resolvable DAG")
        elif dag.dag_id is None:
            rec["unknown_reasons"].append(dag.reason or "dag_id not resolvable")
        else:
            rec["confidence"] = weakest(rec["confidence"], dag.conf)
        if ctx.dynamic:
            rec["unknown_reasons"].append(ctx.dynamic)
        if ctx.branch:
            rec["notes"].append("defined inside a conditional branch")
        return rec

    def _finish(self, rec: dict[str, Any], ctx: Ctx) -> None:
        if not ctx.emit:
            return
        key = ("link", rec["location"]["line"], rec["location"]["col"], rec.get("command_index"), ctx.call_sites)
        if key in self._seen:
            return
        self._seen.add(key)
        rec["notes"] = list(dict.fromkeys(rec["notes"]))
        rec["unknown_reasons"] = list(dict.fromkeys(rec["unknown_reasons"]))
        rec["in_scope"] = (rec.get("dbt_command") in BUILDING_COMMANDS) or rec["kind"].startswith("cosmos_dag") or rec["kind"] == "cosmos_task_group"
        if rec["resolved_models"] is not None and not rec["unknown_reasons"]:
            rec["status"] = "resolved"
        else:
            rec["status"] = "unknown"
            if rec["resolved_models"] is None and not rec["unknown_reasons"]:
                rec["unknown_reasons"].append("model set not resolvable")
        self.links.append(rec)
        if rec["status"] == "unknown":
            self._unresolved(rec["location"]["line"], rec["location"]["col"],
                             "; ".join(rec["unknown_reasons"]), ctx, dag_id=rec["dag_id"],
                             task_id=rec.get("task_id") or rec.get("group_id"), category="link")

    def _resolve_project(self, rec: dict[str, Any], raw: Val | None, base_dir: str | None, ctx: Ctx,
                         label: str = "project_dir") -> str | None:
        """Turn a project path value into a workspace dbt project directory."""
        if raw is None:
            if base_dir:
                m = self.index.match(base_dir)
                if m.path:
                    rec["notes"].append(f"project taken from `cd {base_dir}`" + (f"; {m.note}" if m.note else ""))
                    rec["confidence"] = weakest(rec["confidence"], m.conf)
                    return m.path
            only = self.index.only_project()
            if only:
                rec["notes"].append(f"no {label}: dbt uses its working directory; assumed the only dbt project in the workspace")
                rec["confidence"] = weakest(rec["confidence"], "heuristic")
                return only
            if self.cli_manifest_fallback:
                rec["notes"].append("no project dir: fell back to the --manifest given on the command line")
                rec["confidence"] = weakest(rec["confidence"], "heuristic")
                return self.cli_manifest_fallback
            rec["unknown_reasons"].append(f"no {label} and the working directory is unknown")
            return None
        s = as_str(raw)
        if s is None:
            rec["unknown_reasons"].append(f"{label} not resolvable" + (f": {raw.reason}" if isinstance(raw, Unknown) else ""))
            return None
        rec["project_path_raw"] = s
        rec["confidence"] = weakest(rec["confidence"], raw.conf)
        rec["notes"].extend(raw.notes)
        if "{{" in s:
            rec["unknown_reasons"].append(f"{label} contains a Jinja template: {s}")
            return None
        m = self.index.match(s, base_dir)
        if m.path is None:
            rec["unknown_reasons"].append(m.reason or f"{label} {s!r} not found")
            return None
        if m.note:
            rec["notes"].append(m.note)
        rec["confidence"] = weakest(rec["confidence"], m.conf)
        return m.path

    def _apply_selection(self, rec: dict[str, Any], choice: ManifestChoice, command: str | None,
                         select: list[str], exclude: list[str], selector: str | None) -> Manifest | None:
        if choice.manifest is None:
            rec["unknown_reasons"].append(choice.reason or "no manifest available")
            return None
        m = choice.manifest
        if choice.kind == "stand_in" and not m.of_type("model"):
            rec["unknown_reasons"].append("dbt project has no model files in the workspace (generated at build time?)")
        rec["manifest"] = {"kind": choice.kind, "path": os.path.relpath(m.path, self.index.workspace) if m.path else None,
                           **m.artifact_version()}
        rec["notes"].extend(choice.notes)
        rec["confidence"] = weakest(rec["confidence"], choice.conf)
        types = RESOURCE_TYPES_FOR_COMMAND.get(command or "", None)
        if types is None:
            rec["resolved_models"], rec["resolved_nodes"] = [], []
            return m
        sel = run_select(m, select or None, exclude or None, selector, resource_types=("model", "seed", "snapshot"))
        if sel.unknown:
            rec["unknown_reasons"].append(f"selector not supported: {sel.reason}")
            return m
        rec["notes"].extend(sel.notes)
        ids = sorted(sel.ids)  # type: ignore[arg-type]
        nodes = [i for i in ids if m.nodes[i].resource_type in types]
        if command == "test":
            tested = {p for n in m.nodes.values() if n.resource_type == "test" for p in n.depends_on}
            nodes = [i for i in nodes if i in tested]
            rec["notes"].append("dbt test: linked to selected nodes that have at least one test")
        rec["resolved_nodes"] = nodes
        rec["resolved_models"] = [i for i in nodes if m.nodes[i].resource_type == "model"]
        return m

    def _link_from_invocation(self, kind: str, handle: TaskHandle, inv: DbtInvocation, idx: int, total: int,
                              node: ast.AST, ctx: Ctx, base_conf: str, base_notes: tuple[str, ...],
                              tid_v: Val | None, env: Val | None = None, mapped: bool = False) -> None:
        rec = self._base_link(kind, node, ctx, _dag_of(handle, ctx), handle.task_id)
        if handle.task_id is None:
            rec["unknown_reasons"].append("task_id not resolvable" + (f": {tid_v.reason}" if isinstance(tid_v, Unknown) else ""))
        if total > 1:
            rec["command_index"] = idx
        rec["confidence"] = weakest(rec["confidence"], base_conf)
        rec["notes"].extend(base_notes)
        rec["dbt_command"] = inv.command
        rec["select"], rec["exclude"], rec["selector"] = inv.select, inv.exclude, inv.selector
        rec["target"] = inv.target
        rec["command_text"] = inv.raw[:500]
        if mapped:
            rec["notes"].append("dynamic task mapping over a literal list: union of all mapped values")
        for k, reason in inv.unknown_fields.items():
            rec["unknown_reasons"].append(reason)
        project_raw: Val | None = Const(inv.project_dir) if inv.project_dir else None
        if project_raw is None and isinstance(env, DictV) and "DBT_PROJECT_DIR" in env.items:
            project_raw = env.items["DBT_PROJECT_DIR"]
            rec["notes"].append("project dir from env DBT_PROJECT_DIR")
        proj = self._resolve_project(rec, project_raw, inv.cwd, ctx)
        if proj:
            rec["project_path"] = os.path.relpath(proj, self.index.workspace)
            if "dbt_command" not in inv.unknown_fields:
                self._apply_selection(rec, self.index.manifest_for(proj), inv.command, inv.select, inv.exclude, inv.selector)
        self._finish(rec, ctx)

    def _shell_task(self, kind: str, handle: TaskHandle, cmd: Val | None, node: ast.AST, ctx: Ctx,
                    kwargs: dict[str, Val], tid_v: Val | None, mapped: set[str] | None = None) -> None:
        if not ctx.emit:
            return
        commands: list[Val] = []
        if isinstance(cmd, ListV) and mapped and ("bash_command" in mapped or "command" in mapped):
            commands = list(cmd.items)
        elif cmd is not None:
            commands = [cmd]
        for c in commands:
            text = as_str(c)
            if text is None:
                seg = ast.get_source_segment(self.source.decode("utf-8", "replace"), node) or ""
                reason = "shell command not statically resolvable" + (f": {c.reason}" if isinstance(c, Unknown) else "")
                self._unresolved(node.lineno, node.col_offset, reason, ctx, dag_id=handle.dag_id, task_id=handle.task_id,  # type: ignore[attr-defined]
                                 category="shell", mentions_dbt="dbt" in seg.lower())
                continue
            invs, err = find_dbt_in_shell(text)
            if err:
                self._unresolved(node.lineno, node.col_offset, err, ctx, dag_id=handle.dag_id, task_id=handle.task_id,  # type: ignore[attr-defined]
                                 category="shell")
            if not invs:
                if re.search(r"\.(sh|bash)(\s|$)", text) and not text.strip().startswith(("echo", "#")):
                    self._unresolved(node.lineno, node.col_offset, f"shell command runs a script; its contents are not analysed: {text[:120]}",  # type: ignore[attr-defined]
                                     ctx, dag_id=handle.dag_id, task_id=handle.task_id, category="shell_script")
                continue
            for i, inv in enumerate(invs):
                self._link_from_invocation(kind, handle, inv, i, len(invs), node, ctx, weakest(c.conf, "declared"), c.notes,
                                           tid_v, env=kwargs.get("env"), mapped=bool(mapped))

    def _argv_task(self, kind: str, handle: TaskHandle, node: ast.AST, ctx: Ctx, kwargs: dict[str, Val], tid_v: Val | None) -> None:
        if not ctx.emit:
            return
        cmds = kwargs.get("cmds")
        arguments = kwargs.get("arguments")
        parts: list[str] = []
        conf_vals = []
        for v in (cmds, arguments):
            if v is None:
                continue
            lst = as_str_list(v)
            if lst is None:
                seg = ast.get_source_segment(self.source.decode("utf-8", "replace"), node) or ""
                self._unresolved(node.lineno, node.col_offset, "container command/arguments not statically resolvable",  # type: ignore[attr-defined]
                                 ctx, dag_id=handle.dag_id, task_id=handle.task_id, category="shell", mentions_dbt="dbt" in seg.lower())
                return
            parts.extend(lst)
            conf_vals.append(v)
        if not parts:
            return
        if cmds is None:
            parts_note = ("no cmds: assumed the image entrypoint is dbt-compatible with arguments",)
        else:
            parts_note = ()
        invs = find_dbt_in_argv(parts)
        if not invs and cmds is None and parts and parts[0] in BUILDING_COMMANDS:
            invs = [parse_dbt_argv(parts)]
            parts_note = ("no cmds and arguments start with a dbt sub-command: assumed the image entrypoint is dbt",)
        conf, notes = collect_meta(*conf_vals)
        for i, inv in enumerate(invs):
            self._link_from_invocation(kind, handle, inv, i, len(invs), node, ctx,
                                       weakest(conf, "heuristic" if parts_note else "declared"), notes + parts_note, tid_v,
                                       env=kwargs.get("env_vars"))

    def _cosmos_operator(self, fname: str, handle: TaskHandle, node: ast.AST, kwargs: dict[str, Val], ctx: Ctx,
                         dag: DagInfo | None, tid_v: Val | None, mapped: set[str]) -> None:
        if not ctx.emit:
            return
        m = cz.COSMOS_OPERATOR_RE.match(fname.rsplit(".", 1)[-1])
        command = cz.OPERATOR_COMMANDS[m.group("cmd")] if m else None
        rec = self._base_link("cosmos_operator", node, ctx, dag, handle.task_id)
        if handle.task_id is None:
            rec["unknown_reasons"].append("task_id not resolvable" + (f": {tid_v.reason}" if isinstance(tid_v, Unknown) else ""))
        rec["dbt_command"] = command
        rec["operator"] = fname.rsplit(".", 1)[-1]
        select: list[str] = []
        exclude: list[str] = []
        selector: str | None = None
        for key in ("select", "models"):
            v = kwargs.get(key)
            if v is None:
                continue
            lst = as_str_list(v)
            if lst is None:
                rec["unknown_reasons"].append(f"{key}= is not literal" + (f": {v.reason}" if isinstance(v, Unknown) else ""))
            else:
                select.extend(x for s in lst for x in s.split())
                rec["confidence"] = weakest(rec["confidence"], v.conf)
        ev = kwargs.get("exclude")
        if ev is not None:
            lst = as_str_list(ev)
            if lst is None:
                rec["unknown_reasons"].append("exclude= is not literal")
            else:
                exclude.extend(x for s in lst for x in s.split())
        sv = kwargs.get("selector")
        if sv is not None:
            selector = as_str(sv)
            if selector is None:
                rec["unknown_reasons"].append("selector= is not literal")
        flags = kwargs.get("dbt_cmd_flags")
        if flags is not None:
            fl = as_str_list(flags)
            if fl is None:
                rec["unknown_reasons"].append("dbt_cmd_flags is not literal")
            else:
                inv = parse_dbt_argv([command.split()[0] if command else "run"] + fl)
                select.extend(inv.select)
                exclude.extend(inv.exclude)
                selector = selector or inv.selector
                for reason in inv.unknown_fields.values():
                    rec["unknown_reasons"].append(reason)
        if any("{{" in s for s in select + exclude):
            rec["unknown_reasons"].append("Jinja template in select/exclude")
        if mapped & {"select", "models", "exclude"}:
            rec["notes"].append("dynamic task mapping over a literal list: union of all mapped values")
        rec["select"], rec["exclude"], rec["selector"] = select, exclude, selector
        proj = self._resolve_project(rec, kwargs.get("project_dir"), None, ctx)
        if proj:
            rec["project_path"] = os.path.relpath(proj, self.index.workspace)
            self._apply_selection(rec, self.index.manifest_for(proj), command, select, exclude, selector)
        self._finish(rec, ctx)

    def _airflow_dbt_operator(self, fname: str, handle: TaskHandle, node: ast.AST, kwargs: dict[str, Val], ctx: Ctx,
                              tid_v: Val | None) -> None:
        if not ctx.emit:
            return
        last = fname.rsplit(".", 1)[-1]
        m = re.match(r"^Dbt(Run|Test|Build|Seed|Snapshot|Deps|Compile|Ls|Docs\w*|Source\w*|Clean|Debug|Parse)\w*Operator$", last)
        command = m.group(1).lower() if m else None
        args: list[str] = [command or "run"]
        for key, flag in (("select", "--select"), ("models", "--select"), ("exclude", "--exclude"), ("selector", "--selector")):
            v = kwargs.get(key)
            if v is None:
                continue
            lst = as_str_list(v)
            if lst is None:
                args += [flag, "{{ unresolved }}"]
            else:
                args += [flag, *lst]
        inv = parse_dbt_argv(args)
        inv.command = command
        pd = kwargs.get("dir") or kwargs.get("project_dir")
        if pd is not None and as_str(pd) is not None:
            inv.project_dir = as_str(pd)
        self._link_from_invocation("airflow_dbt_operator", handle, inv, 0, 1, node, ctx, "declared", (), tid_v)

    def _cosmos(self, kind: str, call: CallV, node: ast.Call, ctx: Ctx, dag: DagInfo | None) -> Val:
        spec = cz.parse_cosmos("dag" if kind == "cosmos_dag" else "task_group", call)
        dag_v: Val = DagV(None)
        if kind == "cosmos_dag":
            dag_id = as_str(spec.dag_id)
            reason = None if dag_id else "DbtDag dag_id is not resolvable"
            dag_v = DagV(dag_id, reason, conf=spec.dag_id.conf if spec.dag_id is not None and dag_id else "declared")
            info = _dag_info(dag_v, node)  # type: ignore[arg-type]
            self._record_dag(info, "DbtDag()", ctx)
            dag = info
            group_id = None
            prefix = ""
            ret: Val = dag_v
        else:
            group_id = as_str(spec.group_id)
            prefix = ctx.prefix + (f"{group_id}." if group_id else "")
            handle = TaskHandle(dag.dag_id if dag else None, (ctx.prefix + group_id) if group_id else None, "cosmos_task_group",
                                node.lineno, node.col_offset)
            self._record_task(handle, "DbtTaskGroup", ctx)
            ret = TaskV(handle)
        if not ctx.emit:
            return ret
        rec = self._base_link(kind, node, ctx, dag, None, (ctx.prefix + group_id) if group_id else None)
        rec["dbt_command"] = "cosmos_render"
        rec["task_id_pattern"] = cz.pattern_for(spec, prefix)
        rec["cosmos"] = {
            "test_behavior": spec.test_behavior, "load_method": spec.load_method or "automatic",
            "execution_mode": spec.execution_mode, "source_rendering_behavior": spec.source_rendering,
            "seed_rendering_behavior": spec.seed_rendering, "group_nodes_by_folder": spec.group_nodes_by_folder,
            "project_path_from": spec.render_project_source,
        }
        rec["notes"].extend(spec.notes)
        rec["unknown_reasons"].extend(spec.unknown)
        if kind == "cosmos_task_group" and group_id is None:
            rec["unknown_reasons"].append("DbtTaskGroup group_id not resolvable")
        if call.has_star:
            rec["unknown_reasons"].append("DbtDag/DbtTaskGroup called with **kwargs")
        if spec.group_id is not None and spec.group_id.notes:
            rec["notes"].extend(spec.group_id.notes)
        sel = as_str_list(spec.select)
        exc = as_str_list(spec.exclude)
        selector = as_str(spec.selector) if spec.selector is not None else None
        if sel is None:
            rec["unknown_reasons"].append("RenderConfig.select is not literal" + (f": {spec.select.reason}" if isinstance(spec.select, Unknown) else ""))
        if exc is None:
            rec["unknown_reasons"].append("RenderConfig.exclude is not literal")
        if spec.selector is not None and selector is None:
            rec["unknown_reasons"].append("RenderConfig.selector is not literal")
        rec["select"], rec["exclude"], rec["selector"] = sel or [], exc or [], selector
        for v in (spec.select, spec.exclude, spec.selector):
            if v is not None and not isinstance(v, Unknown):
                rec["confidence"] = weakest(rec["confidence"], v.conf)
                rec["notes"].extend(collect_meta(v)[1])
        if any("{{" in s for s in (sel or []) + (exc or [])):
            rec["unknown_reasons"].append("Jinja template in RenderConfig select/exclude")
        manifest, project_dir = self._cosmos_manifest(rec, spec, ctx)
        if manifest is not None and sel is not None and exc is not None:
            ids = self._cosmos_select(rec, manifest, spec, sel, exc, selector)
            if ids is not None:
                rec["resolved_nodes"] = ids
                rec["resolved_models"] = [i for i in ids if manifest.nodes[i].resource_type == "model"]
                excluded_tests: set[str] = set()
                if exc:
                    es = run_select(manifest, exc, None, None, resource_types=("test",))
                    excluded_tests = es.ids or set()
                pname = as_str(spec.project_name) or (manifest.project_name or os.path.basename(project_dir or "") or "project")
                rows, extra, why = cz.expand_tasks(manifest, ids, spec, prefix, excluded_tests, pname)
                if why:
                    rec["notes"].append(why)
                    rec["tasks"] = None
                else:
                    rec["tasks"] = rows
                    rec["extra_tasks"] = extra
        self._finish(rec, ctx)
        return ret

    def _cosmos_manifest(self, rec: dict[str, Any], spec: cz.CosmosSpec, ctx: Ctx) -> tuple[Manifest | None, str | None]:
        """Which node set Cosmos renders from: dbt ls file, declared manifest, or the project's own."""
        project_dir = None
        if spec.render_project is not None:
            project_dir = self._resolve_project(rec, spec.render_project, None, ctx, label=spec.render_project_source or "project")
        if project_dir:
            rec["project_path"] = os.path.relpath(project_dir, self.index.workspace)
        lm = spec.load_method or "automatic"
        if lm == "dbt_ls_file":
            p = as_str(spec.dbt_ls_path)
            if p is None:
                rec["unknown_reasons"].append("LoadMode.DBT_LS_FILE with a non-literal dbt_ls_path")
                return None, project_dir
            real = os.path.realpath(p)
            if not os.path.isfile(real) or not real.startswith(self.index.workspace + os.sep):
                rec["unknown_reasons"].append(f"dbt ls file {p} not found in the workspace")
                return None, project_dir
            try:
                rows = load_dbt_ls_file(real)
            except (OSError, ManifestError) as exc:
                rec["unknown_reasons"].append(f"dbt ls file unreadable: {exc}")
                return None, project_dir
            data = {"metadata": {"project_name": None}, "nodes": {r["unique_id"]: r for r in rows}}
            m = manifest_from_dict(data, path=real)
            rec["manifest"] = {"kind": "dbt_ls_file", "path": os.path.relpath(real, self.index.workspace)}
            rec["notes"].append("LoadMode.DBT_LS_FILE: Cosmos renders exactly the nodes listed in the dbt ls file")
            rec["confidence"] = weakest(rec["confidence"], "inferred")
            rec["_ls_file"] = True
            return m, project_dir
        if spec.manifest_path is not None and lm in ("automatic", "dbt_manifest"):
            mp = as_str(spec.manifest_path)
            if mp is None:
                rec["unknown_reasons"].append("ProjectConfig.manifest_path is not literal")
                return None, project_dir
            rec["confidence"] = weakest(rec["confidence"], spec.manifest_path.conf)
            rec["notes"].extend(spec.manifest_path.notes)
            if re.match(r"^[a-z][a-z0-9+.-]*://", mp):
                rec["unknown_reasons"].append(f"manifest is remote ({mp}); not readable offline")
                if not project_dir and spec.execution_project is not None:
                    pd = self._resolve_project(rec, spec.execution_project, None, ctx, label="ExecutionConfig.dbt_project_path")
                    if pd:
                        rec["project_path"] = os.path.relpath(pd, self.index.workspace)
                return None, project_dir
            choice = self.index.load_declared(mp) if os.path.isfile(mp) else None
            if choice is None or choice.manifest is None:
                # remap a container path to the workspace project that owns it
                parent = posixpath.dirname(mp)
                pdir = posixpath.dirname(parent) if posixpath.basename(parent) == "target" else parent
                pm = self.index.match(pdir)
                if pm.path and os.path.isfile(os.path.join(pm.path, posixpath.basename(parent), posixpath.basename(mp))):
                    choice = self.index.load_declared(os.path.join(pm.path, posixpath.basename(parent), posixpath.basename(mp)))
                    rec["notes"].append(f"manifest path {mp!r} remapped: {pm.note}")
                    rec["confidence"] = weakest(rec["confidence"], "heuristic")
                    project_dir = project_dir or pm.path
                else:
                    rec["unknown_reasons"].append(f"declared manifest {mp} not found in the workspace")
                    return None, project_dir
            if not project_dir:
                parent = os.path.dirname(choice.manifest.path or "")  # type: ignore[union-attr]
                project_dir = os.path.dirname(parent) if os.path.basename(parent) == "target" else parent
                rec["project_path"] = os.path.relpath(project_dir, self.index.workspace)
            rec["manifest"] = {"kind": "dag_declared", "path": os.path.relpath(choice.manifest.path, self.index.workspace),  # type: ignore[union-attr,arg-type]
                               **choice.manifest.artifact_version()}  # type: ignore[union-attr]
            return choice.manifest, project_dir
        if project_dir is None:
            if not rec["unknown_reasons"]:
                rec["unknown_reasons"].append("no dbt project path or manifest path found in the Cosmos config")
            return None, None
        choice = self.index.manifest_for(project_dir)
        if choice.manifest is None:
            rec["unknown_reasons"].append(choice.reason or "no manifest")
            return None, project_dir
        if choice.kind == "stand_in" and not choice.manifest.of_type("model"):
            rec["unknown_reasons"].append("dbt project has no model files in the workspace (generated at build time?)")
        rec["manifest"] = {"kind": choice.kind, "path": os.path.relpath(choice.manifest.path, self.index.workspace) if choice.manifest.path else None,
                           **choice.manifest.artifact_version()}
        rec["notes"].extend(choice.notes)
        rec["confidence"] = weakest(rec["confidence"], choice.conf)
        if lm == "automatic":
            rec["notes"].append("LoadMode.AUTOMATIC without manifest_path: Cosmos runs dbt ls at parse time; resolved over the project's manifest instead")
        return choice.manifest, project_dir

    def _cosmos_select(self, rec: dict[str, Any], m: Manifest, spec: cz.CosmosSpec, sel: list[str], exc: list[str],
                       selector: str | None) -> list[str] | None:
        types = ["model", "snapshot"] + ([] if spec.seed_rendering == "none" else ["seed"])
        if rec.pop("_ls_file", False):
            ids = sorted(uid for uid, n in m.nodes.items() if n.resource_type in types)
            return ids
        s = run_select(m, sel or None, exc or None, selector, resource_types=types)
        if s.unknown:
            rec["unknown_reasons"].append(f"selector not supported: {s.reason}")
            return None
        rec["notes"].extend(s.notes)
        externals = [n.unique_id for n in m.nodes.values() if n.external]
        if externals:
            rec["notes"].append(f"{len(externals)} external (e.g. dbt-loom) nodes in the manifest are not rendered by Cosmos")
        return sorted(s.ids)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------
def _all_args(args: ast.arguments) -> list[str]:
    names = [a.arg for a in args.posonlyargs + args.args + args.kwonlyargs]
    if args.vararg:
        names.append(args.vararg.arg)
    if args.kwarg:
        names.append(args.kwarg.arg)
    return names


def _to_str(v: Val | None) -> str | None:
    if isinstance(v, PathV):
        return v.path
    if isinstance(v, Const) and isinstance(v.value, (str, int, float, bool)) and not isinstance(v.value, bool):
        return str(v.value)
    if isinstance(v, Const) and isinstance(v.value, bool):
        return str(v.value)
    return None


def _root_name(func: ast.expr) -> str | None:
    while isinstance(func, ast.Attribute):
        func = func.value
    return func.id if isinstance(func, ast.Name) else None


def _clone(v: Val) -> Val:
    import copy
    return copy.copy(v)


def _task_list(v: Val) -> list[TaskHandle] | None:
    if isinstance(v, TaskV):
        return [v.handle] if v.handle is not None else None
    if isinstance(v, ListV):
        out = []
        for i in v.items:
            if not isinstance(i, TaskV) or i.handle is None:
                return None
            out.append(i.handle)
        return out
    return None


def _dag_info(v: DagV, node: ast.AST) -> DagInfo:
    return DagInfo(v.dag_id, v.reason, v.conf, getattr(node, "lineno", 0), getattr(node, "col_offset", 0))


def _dag_of(handle: TaskHandle, ctx: Ctx) -> DagInfo | None:
    if ctx.dag and ctx.dag.dag_id == handle.dag_id:
        return ctx.dag
    if handle.dag_id is not None:
        return DagInfo(handle.dag_id, None, "declared", handle.line, handle.col)
    return ctx.dag


def _classify(fname: str) -> str | None:
    if not fname:
        return None
    last = fname.rsplit(".", 1)[-1]
    if last == "DAG":
        return "dag"
    if last in ("chain", "cross_downstream", "chain_linear"):
        return "chain"
    if last == "TaskGroup":
        return "task_group"
    if fname.startswith(("airflow_dbt.", "airflow_dbt_python.")) and re.match(r"^Dbt\w+Operator$", last):
        return "airflow_dbt_operator"
    if last == "DbtDag":
        return "cosmos_dag"
    if last == "DbtTaskGroup":
        return "cosmos_task_group"
    if cz.COSMOS_OPERATOR_RE.match(last):
        return "cosmos_operator"
    if last in SHELL_OPS:
        return SHELL_OPS[last]
    if last in POD_OPS:
        return "pod"
    if last in DOCKER_OPS:
        return "docker"
    if last.endswith(("Operator", "Sensor")):
        return "generic"
    return None


_DEF_CACHE: dict[tuple[str, str], set[str]] = {}


def _operators_defined_in(path: str, name: str) -> set[str]:
    """Names of operator classes constructed inside definition ``name`` of a workspace module (AST only)."""
    key = (path, name)
    if key in _DEF_CACHE:
        return _DEF_CACHE[key]
    found: set[str] = set()
    try:
        if os.path.getsize(path) <= FILE_SIZE_CAP:
            with open(path, "rb") as fh:
                tree = ast.parse(fh.read())
            for st in tree.body:
                if isinstance(st, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and st.name == name:
                    for sub in ast.walk(st):
                        if isinstance(sub, ast.Call):
                            n = sub.func.attr if isinstance(sub.func, ast.Attribute) else getattr(sub.func, "id", "")
                            k = _classify(n)
                            if k and k not in ("generic", "chain"):
                                found.add(n)
    except (OSError, SyntaxError, ValueError, RecursionError):
        pass
    _DEF_CACHE[key] = found
    return found


# ---------------------------------------------------------------------------------------------
# directory scan
# ---------------------------------------------------------------------------------------------
SKIP_SCAN_DIRS = {".git", "__pycache__", "node_modules", ".venv", "venv", "dbt_packages", "target", "logs", ".tox"}


def _airflowignore_patterns(d: str) -> list[re.Pattern[str]]:
    p = os.path.join(d, ".airflowignore")
    out: list[re.Pattern[str]] = []
    if os.path.isfile(p):
        with open(p, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.split("#", 1)[0].strip()
                if line:
                    try:
                        out.append(re.compile(line))
                    except re.error:
                        continue
    return out


def discover_dag_files(root: str, safe_mode: bool = True) -> tuple[list[str], list[dict[str, str]]]:
    """Python files Airflow would consider, honouring .airflowignore (regexp) and the safe-mode heuristic."""
    files: list[str] = []
    skipped: list[dict[str, str]] = []
    root = os.path.realpath(root)
    if os.path.isfile(root):
        return [root], []
    ignore: list[tuple[str, re.Pattern[str]]] = []
    for dirpath, dirnames, filenames in os.walk(root):
        for pat in _airflowignore_patterns(dirpath):
            ignore.append((dirpath, pat))
        keep = []
        for d in sorted(dirnames):
            full = os.path.join(dirpath, d)
            if d in SKIP_SCAN_DIRS or d.startswith("."):
                continue
            if os.path.isfile(os.path.join(full, "dbt_project.yml")):
                skipped.append({"path": os.path.relpath(full, root), "why": "dbt project directory"})
                continue
            if any(full.startswith(base) and pat.search(os.path.relpath(full, base)) for base, pat in ignore):
                skipped.append({"path": os.path.relpath(full, root), "why": ".airflowignore"})
                continue
            keep.append(d)
        dirnames[:] = keep
        for fn in sorted(filenames):
            if not fn.endswith(".py"):
                continue
            full = os.path.join(dirpath, fn)
            if any(full.startswith(base) and pat.search(os.path.relpath(full, base)) for base, pat in ignore):
                skipped.append({"path": os.path.relpath(full, root), "why": ".airflowignore"})
                continue
            if safe_mode:
                try:
                    with open(full, "rb") as fh:
                        head = fh.read(FILE_SIZE_CAP + 1).lower()
                except OSError:
                    continue
                if b"dag" not in head or b"airflow" not in head:
                    skipped.append({"path": os.path.relpath(full, root), "why": "Airflow safe-mode heuristic (no 'dag' and 'airflow')"})
                    continue
            files.append(full)
    return files, skipped


def scan(paths: list[str], workspace: str | None = None, manifests: list[str] | None = None,
         safe_mode: bool = True, cross_module: bool = True, path_remap: bool = True,
         inline_calls: bool = True) -> dict[str, Any]:
    """Scan DAG files. The three feature switches exist so the spike can measure what each one buys."""
    ws = os.path.realpath(workspace or (paths[0] if os.path.isdir(paths[0]) else os.path.dirname(paths[0])))
    index = ProjectIndex(ws, manifests, remap=path_remap)
    loader = ModuleLoader(ws, index, enabled=cross_module, inline_calls=inline_calls)
    fallback = None
    if manifests and len(manifests) == 1:
        mp = os.path.realpath(manifests[0])
        parent = os.path.dirname(mp)
        fallback = os.path.dirname(parent) if os.path.basename(parent) == "target" else parent
    files: list[str] = []
    skipped: list[dict[str, str]] = []
    for p in paths:
        f, s = discover_dag_files(p, safe_mode)
        files.extend(f)
        skipped.extend(s)
    out: dict[str, Any] = {
        "scanner": {"origin": ORIGIN, "version": "0.0.1", "executes_user_code": False,
                    "options": {"cross_module": cross_module, "path_remap": path_remap, "inline_calls": inline_calls,
                                "safe_mode": safe_mode}},
        "workspace": ws,
        "dbt_projects": [os.path.relpath(p, ws) for p in index.projects],
        "files": [], "skipped": skipped, "dags": [], "tasks": [], "links": [], "dependencies": [], "unresolved": [],
    }
    for f in sorted(set(files)):
        fs = FileScanner(f, index, loader, emit=True, cli_manifest_fallback=fallback)
        fs.run()
        out["files"].append(os.path.relpath(f, ws))
        out["dags"].extend(fs.dags)
        out["tasks"].extend(fs.tasks)
        out["links"].extend(fs.links)
        out["dependencies"].extend(fs.dependencies)
        out["unresolved"].extend(fs.unresolved)
    out["stats"] = {
        "files_scanned": len(out["files"]), "files_skipped": sum(1 for x in skipped if x["why"] != "dbt project directory"),
        "dbt_project_dirs_skipped": sum(1 for x in skipped if x["why"] == "dbt project directory"), "dags": len(out["dags"]),
        "links": len(out["links"]), "links_resolved": sum(1 for l in out["links"] if l["status"] == "resolved"),
        "links_unknown": sum(1 for l in out["links"] if l["status"] == "unknown"),
        "dependencies": len(out["dependencies"]), "unresolved": len(out["unresolved"]),
    }
    return out
