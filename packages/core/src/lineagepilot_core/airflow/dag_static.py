"""AST-only scanner for Airflow DAG files (task 5.1, design section 6.2).

DAG files are untrusted workspace input. They are read as bytes (with a size cap) and parsed
with :func:`ast.parse`. Nothing is imported, executed or ``eval``-ed. Expressions are
evaluated by a small abstract interpreter over the AST (see ``values``); whatever it cannot
evaluate becomes an ``Unknown`` value, and links that depend on it become ``unknown`` records.

A crafted file gets no more out of this than a wrong answer: strings and lists it builds are
capped, each file has a budget of evaluation steps, and format strings cannot reach attributes.

The result is plain JSON, with nothing about the machine in it, so the language server can read it
from a child process and reviews stay comparable.
"""

from __future__ import annotations

import ast
import copy
import fnmatch
import hashlib
import os
import posixpath
import re
import string
from collections.abc import Iterable
from dataclasses import dataclass, replace
from pathlib import PurePosixPath
from typing import Any

from sqlglot import exp, parse
from sqlglot.errors import SqlglotError

from lineagepilot_core.airflow import cosmos as cz
from lineagepilot_core.airflow.categories import UnknownCategory, most_specific
from lineagepilot_core.airflow.manifest import (
    Manifest,
    ManifestError,
    load_dbt_ls_file,
    manifest_from_dict,
)
from lineagepilot_core.airflow.projects import ManifestChoice, ProjectIndex
from lineagepilot_core.airflow.safe_io import (
    is_absolute,
    is_inside,
    is_regular_file,
    read_capped,
    regular_file_inside,
    resolve_inside,
    to_posix,
    walk_inside,
)
from lineagepilot_core.airflow.selector import select as run_select
from lineagepilot_core.airflow.shell import (
    BUILDING_COMMANDS,
    DbtInvocation,
    find_dbt_in_argv,
    find_dbt_in_shell,
    mentions_dbt,
    parse_dbt_argv,
)
from lineagepilot_core.airflow.values import (
    CallV,
    Const,
    DagV,
    DictV,
    FuncV,
    ListV,
    PartialV,
    PathV,
    Sym,
    TaskV,
    Unknown,
    Val,
    as_str,
    as_str_list,
    collect_meta,
    merge_values,
    weakest,
)
from lineagepilot_core.parsers.sql_parser import parse_sql
from lineagepilot_core.rules.safe_regex import SafeRegex, UnsafePattern

ORIGIN = "dag_static"
FILE_SIZE_CAP = 2 * 1024 * 1024
MAX_CALL_DEPTH = 4
MAX_IMPORT_DEPTH = 3
# What one file can make the evaluator do. Real DAG files stay in the thousands of steps.
MAX_STEPS = 500_000
MAX_STRING = 100_000
MAX_ITEMS = 10_000
# Beyond this, a width in a format string is an attempt to allocate memory
MAX_FORMAT_WIDTH = 1000
MAX_DAG_FILES = 5_000
MAX_SQL_BYTES = 200_000
NO_IMPORT_TOP = {
    "airflow",
    "cosmos",
    "pendulum",
    "datetime",
    "os",
    "pathlib",
    "typing",
    "json",
    "sys",
    "logging",
    "re",
    "collections",
    "functools",
    "itertools",
    "dataclasses",
    "enum",
    "subprocess",
    "airflow_dbt",
    "airflow_dbt_python",
    "dbt",
    "kubernetes",
    "docker",
    "yaml",
    "__future__",
}
KNOWN_LIBRARY_PREFIXES = (
    "cosmos.",
    "airflow.",
    "pendulum.",
    "datetime.",
    "pathlib.",
    "os.",
    "typing.",
    "json.",
)
BUILTIN_CALLS = {
    "str",
    "dict",
    "list",
    "tuple",
    "int",
    "bool",
    "set",
    "frozenset",
    "sorted",
    "len",
    "float",
}
SHELL_OPS = {"BashOperator": "bash", "SSHOperator": "ssh"}
# Operators whose SQL says which tables they touch: (the keyword arguments that can hold SQL)
SQL_ARGUMENTS = ("sql", "hql", "query", "statement")
SQL_OPS = {
    "SQLExecuteQueryOperator",
    "PostgresOperator",
    "SnowflakeOperator",
    "MySqlOperator",
    "MsSqlOperator",
    "OracleOperator",
    "SqliteOperator",
    "TrinoOperator",
    "VerticaOperator",
    "ExasolOperator",
    "JdbcOperator",
    "HiveOperator",
    "AthenaOperator",
    "RedshiftSQLOperator",
    "RedshiftDataOperator",
    "BigQueryExecuteQueryOperator",
    "BigQueryInsertJobOperator",
    "BigQueryCheckOperator",
    "BigQueryValueCheckOperator",
    "DatabricksSqlOperator",
    "SnowflakeSqlApiOperator",
    "SQLCheckOperator",
    "SQLValueCheckOperator",
    "SQLThresholdCheckOperator",
    "BranchSQLOperator",
    "SQLColumnCheckOperator",
    "SQLTableCheckOperator",
    "SQLIntervalCheckOperator",
}
# These name a table rather than give a statement
SQL_TABLE_OPS = {"SQLColumnCheckOperator", "SQLTableCheckOperator", "SQLIntervalCheckOperator"}
_JINJA_EXPRESSION = re.compile(r"\{\{.*?\}\}", re.S)
_JINJA_BLOCK = re.compile(r"\{%.*?%\}", re.S)
_JINJA_PLACEHOLDER = "__lp_jinja__"
# dbt as a word or part of a name, as in `dbt_run` or `acme/dbt-duckdb`, but not in `xdbtx`
_DBT_NAME = re.compile(r"(?<![a-z])dbt(?![a-z])", re.I)
_CREATES_TASKS = re.compile(r"dag|task|operator|group", re.I)
_FORMAT_WIDTH = re.compile(r"%[-#0 +]*(\d+)?(?:\.(\d+))?")
POD_OPS = {"KubernetesPodOperator", "GKEStartPodOperator", "EksPodOperator", "AKSPodOperator"}
DOCKER_OPS = {"DockerOperator", "DockerSwarmOperator"}
SUBPROCESS_FUNCS = (
    "subprocess.run",
    "subprocess.call",
    "subprocess.check_call",
    "subprocess.check_output",
    "subprocess.Popen",
    "os.system",
    "os.popen",
)
RESOURCE_TYPES_FOR_COMMAND = {
    "run": ("model",),
    "test": ("model", "seed", "snapshot"),
    "build": ("model", "seed", "snapshot"),
    "seed": ("seed",),
    "snapshot": ("snapshot",),
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
    owner: str | None = None


@dataclass(frozen=True)
class Ctx:
    dag: DagInfo | None = None
    group_prefix: tuple[str, ...] = ()
    dynamic: str | None = (
        None  # reason the code may run 0..n times (loop, comprehension, uncalled function)
    )
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
    prefix_parts: tuple[str, ...] | None = None
    owner: str | None = None


class Scope:
    def __init__(self, parent: Scope | None = None):
        self.vars: dict[str, Val] = {}
        self.parent = parent

    def lookup(self, name: str) -> Val | None:
        s: Scope | None = self
        while s is not None:
            if name in s.vars:
                return s.vars[name]
            s = s.parent
        return None

    def copy(self) -> Scope:
        c = Scope(self.parent)
        c.vars = dict(self.vars)
        return c


class _Return(Exception):
    pass


class _OverBudget(Exception):
    """This file has used its evaluation steps."""


# ---------------------------------------------------------------------------------------------
# workspace module loader (cross-module constants)
# ---------------------------------------------------------------------------------------------
class ModuleLoader:
    """Resolve ``from include.constants import X`` to another workspace file, read as AST only."""

    def __init__(self, workspace: str, index: ProjectIndex):
        self.workspace = os.path.realpath(workspace)
        self.index = index
        self.cache: dict[str, FileScanner | None] = {}
        self._find_cache: dict[tuple[str, str, int], str | None] = {}
        self._operators: dict[tuple[str, str], set[str]] = {}

    def operators_in(self, path: str, name: str) -> set[str]:
        key = (path, name)
        if key not in self._operators:
            self._operators[key] = _operators_defined_in(path, name, self.workspace)
        return self._operators[key]

    def find(self, module: str, from_file: str, level: int = 0) -> str | None:
        if not module and not level:
            return None
        # ``from_file`` is a FileScanner's path, already resolved inside the workspace
        key = (module, os.path.dirname(from_file), level)
        if key not in self._find_cache:
            self._find_cache[key] = self._find(module, from_file, level)
        return self._find_cache[key]

    def _find(self, module: str, from_file: str, level: int) -> str | None:
        top = module.split(".", 1)[0] if module else ""
        if level == 0 and top in NO_IMPORT_TOP:
            return None
        rel_parts = module.split(".") if module else []
        bases: list[str] = []
        d = os.path.dirname(from_file)
        if level:
            for _ in range(level - 1):
                d = os.path.dirname(d)
            bases = [d]
        else:
            while True:
                bases.append(d)
                if d == self.workspace or not is_inside(d, self.workspace):
                    break
                nd = os.path.dirname(d)
                if nd == d:
                    break
                d = nd
        for base in bases:
            cand = os.path.join(base, *rel_parts)
            for path in (cand + ".py", os.path.join(cand, "__init__.py")):
                real = regular_file_inside(path, self.workspace)
                if real is not None:
                    return real
        return None

    def module(self, path: str, depth: int) -> FileScanner | None:
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
    def __init__(
        self,
        path: str,
        index: ProjectIndex,
        loader: ModuleLoader,
        emit: bool = True,
        import_depth: int = 0,
    ):
        # A symlink out of the workspace is reported where it is, not where it points, and is
        # never followed: reading it fails, and says so
        real = resolve_inside(os.path.abspath(path), index.workspace)
        self.path = real or os.path.abspath(path)
        self.rel = index.relative(self.path)
        self.index = index
        self.loader = loader
        self.emit = emit
        self.import_depth = import_depth
        self.links: list[dict[str, Any]] = []
        self.sql: list[dict[str, Any]] = []
        self.sha256: str | None = None
        self.other_project_links = 0
        self.steps = 0
        self.reads_manifest = False  # the file reads manifest.json at parse time
        self.unresolved: list[dict[str, Any]] = []
        self.dependencies: list[dict[str, Any]] = []
        self.dags: list[dict[str, Any]] = []
        self.tasks: list[dict[str, Any]] = []
        self.imports: dict[
            str, tuple[str, str | None, int]
        ] = {}  # local name -> (module, attr, level)
        self.scope = Scope()
        self.source = b""
        self.tree: ast.Module | None = None
        self.functions: dict[str, FuncV] = {}
        self.called: set[int] = set()
        self._seen: set[tuple] = set()
        self._call_depth = 0

    # --- entry ----------------------------------------------------------------------------
    def run(self) -> bool:
        data = read_capped(self.path, FILE_SIZE_CAP, self.index.workspace)
        if data is None:
            self._unresolved(
                0,
                0,
                f"not a regular file in the workspace under {FILE_SIZE_CAP} bytes; not parsed",
                Ctx(),
                force=True,
                category=UnknownCategory.LIMIT,
            )
            return False
        self.source = data
        self.sha256 = hashlib.sha256(data).hexdigest()
        self.text = data.decode("utf-8", "replace")
        self.lines = self.text.splitlines(keepends=True)
        try:
            self.tree = ast.parse(self.source, filename=self.rel)
        except SyntaxError as exc:
            self._unresolved(
                exc.lineno or 0,
                (exc.offset or 1) - 1,
                f"SyntaxError: {exc.msg}",
                Ctx(),
                force=True,
                category=UnknownCategory.DAG_NOT_LOADABLE,
            )
            return False
        except (ValueError, RecursionError, MemoryError) as exc:
            self._unresolved(
                0,
                0,
                f"cannot parse: {type(exc).__name__}",
                Ctx(),
                force=True,
                category=UnknownCategory.DAG_NOT_LOADABLE,
            )
            return False
        self.scope.vars["__file__"] = Const(to_posix(self.path), conf="inferred")
        self.scope.vars["__name__"] = Const("__lineagepilot_static__", conf="inferred")
        try:
            self.exec_block(self.tree.body, self.scope, Ctx(emit=self.emit))
            if self.emit:
                self._scan_uncalled()
        except RecursionError:
            self._unresolved(
                0,
                0,
                "expression nesting too deep for the static evaluator",
                Ctx(),
                force=True,
                category=UnknownCategory.LIMIT,
            )
        except _OverBudget:
            self._unresolved(
                0,
                0,
                f"analysis stopped after {MAX_STEPS} steps; what came before is kept",
                Ctx(),
                force=True,
                category=UnknownCategory.LIMIT,
            )
        return True

    def _step(self) -> None:
        self.steps += 1
        if self.steps > MAX_STEPS:
            raise _OverBudget

    def _segment(self, node: ast.AST) -> str:
        """The whole lines a node spans: enough to see what it mentions, and cheap to get."""
        start = getattr(node, "lineno", 1) - 1
        return "".join(self.lines[start : getattr(node, "end_lineno", start + 1)])

    def _scan_uncalled(self) -> None:
        for fv in list(self.functions.values()):
            if id(fv.node) in self.called or fv.kind in ("task", "dag"):
                continue
            scope = Scope(self.scope)
            for a in _all_args(fv.node.args):
                scope.vars[a] = Unknown(f"parameter {a!r} of function {fv.name!r}")
            ctx = Ctx(
                dynamic=f"inside function {fv.name!r}, which is never called at module level "
                "(factory or runtime callable)",
                funcs=(fv.name,),
            )
            try:
                self.exec_block(fv.node.body, scope, ctx)
            except _Return:
                pass

    # --- records --------------------------------------------------------------------------
    def _loc(self, line: int, col: int) -> dict[str, Any]:
        return {"file": self.rel, "line": line, "col": col}

    def _dynamic_category(self) -> UnknownCategory:
        if self.reads_manifest:
            return UnknownCategory.MANIFEST_GENERATED
        return UnknownCategory.LOOP_OR_FACTORY

    def _why(self, rec: dict[str, Any], category: UnknownCategory, reason: str) -> None:
        rec["unknown_reasons"].append(reason)
        rec["categories"].append(category.value)

    def _unresolved(
        self,
        line: int,
        col: int,
        reason: str,
        ctx: Ctx,
        force: bool = False,
        category: UnknownCategory = UnknownCategory.NOT_LITERAL,
        **extra: Any,
    ) -> None:
        if not (ctx.emit or force) or not self.emit and not force:
            return
        if ctx.dynamic and ctx.dynamic not in reason:
            reason = f"{ctx.dynamic}; {reason}"
            extra.setdefault("dynamic", True)
            if category is UnknownCategory.NOT_LITERAL:
                category = self._dynamic_category()
        key = ("u", line, col, reason, ctx.call_sites)
        if key in self._seen:
            return
        self._seen.add(key)
        rec = {
            "file": self.rel,
            "line": line,
            "col": col,
            "reason": reason,
            "category": category.value,
        }
        rec.update({k: v for k, v in extra.items() if v is not None})
        if ctx.call_sites:
            rec["via_call_at"] = [list(cs) for cs in ctx.call_sites]
        self.unresolved.append(rec)

    # --- statements -----------------------------------------------------------------------
    def exec_block(self, stmts: Iterable[ast.stmt], scope: Scope, ctx: Ctx) -> None:
        for st in stmts:
            self.exec_stmt(st, scope, ctx)

    def exec_stmt(self, st: ast.stmt, scope: Scope, ctx: Ctx) -> None:
        self._step()
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
                for t, v in zip(target.elts, val.items, strict=False):
                    self._bind(t, v, scope, ctx)
            else:
                for t in target.elts:
                    self._bind(
                        t,
                        Unknown("tuple unpacking of a non-literal value", line=target.lineno),
                        scope,
                        ctx,
                    )
        elif isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name):
            base = scope.lookup(target.value.id)
            key = self.eval(target.slice, scope, ctx)
            if isinstance(base, DictV) and isinstance(key, Const) and isinstance(key.value, str):
                items = dict(base.items)
                items[key.value] = val
                scope.vars[target.value.id] = DictV(items, conf=base.conf, notes=base.notes)
            elif isinstance(base, DictV):
                scope.vars[target.value.id] = Unknown(
                    "dict mutated with a non-literal key", line=target.lineno
                )
        elif isinstance(target, ast.Starred):
            self._bind(target.value, Unknown("starred assignment"), scope, ctx)

    def _merge_into(self, scope: Scope, variants: list[Scope], reason: str) -> None:
        names = sorted(set().union(*(v.vars.keys() for v in variants)))
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
        if (
            isinstance(test, ast.Compare)
            and isinstance(test.left, ast.Name)
            and test.left.id == "__name__"
            and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Constant)
            and test.comparators[0].value == "__main__"
        ):
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
        lctx = replace(
            ctx,
            dynamic=ctx.dynamic
            or f"inside a {kind} (line {st.lineno}): tasks created a dynamic number of times",
        )
        inner = scope.copy()
        if isinstance(st, (ast.For, ast.AsyncFor)):
            self.eval(st.iter, scope, ctx)
            self._bind(
                st.target, Unknown(f"loop variable of the {kind} at line {st.lineno}"), inner, lctx
            )
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
            elif isinstance(v, TaskV) and getattr(v.handle, "kind", "") in (
                "task_group",
                "cosmos_task_group",
            ):
                gid = v.handle.task_id
                if v.handle.kind == "task_group" and gid is not None:
                    pref = getattr(v.handle, "prefix_parts", None)
                    new_ctx = replace(
                        new_ctx,
                        group_prefix=tuple(pref) if pref is not None else new_ctx.group_prefix,
                    )
                elif gid is None:
                    new_ctx = replace(
                        new_ctx,
                        dynamic=new_ctx.dynamic
                        or "inside a TaskGroup whose group_id is not resolvable",
                    )
            if item.optional_vars is not None:
                self._bind(item.optional_vars, v, scope, new_ctx)
        self.exec_block(st.body, scope, new_ctx)

    def _funcdef(self, st: ast.FunctionDef | ast.AsyncFunctionDef, scope: Scope, ctx: Ctx) -> None:
        kind, dkw = self._decorator_kind(st, scope)
        fv = FuncV(st.name, st, kind, decorator_kwargs=dkw, defining_scope=scope)
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
            info = DagInfo(dag_id, reason, conf, st.lineno, st.col_offset, _owner_of(dkw))
            self._record_dag(info, "@dag", ctx)
            inner = Scope(scope)
            for a in _all_args(st.args):
                inner.vars[a] = Unknown(
                    f"parameter {a!r} of @dag function {st.name!r} (DAG params are runtime values)"
                )
            try:
                self.exec_block(
                    st.body,
                    inner,
                    replace(ctx, dag=info, group_prefix=(), funcs=ctx.funcs + (st.name,)),
                )
            except _Return:
                pass
        elif kind == "plain" and not ctx.funcs:
            self._subprocess_heuristic(st, st.name, ctx)

    def _decorator_kind(
        self, st: ast.FunctionDef | ast.AsyncFunctionDef, scope: Scope
    ) -> tuple[str, dict[str, Val]]:
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
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "invoke"
                and "dbtRunner" in self._segment(node.func)
            )
            if not is_runner:
                continue
            seg = self._segment(node)
            if mentions_dbt(seg) or "'dbt'" in seg or '"dbt"' in seg:
                self._unresolved(
                    node.lineno,
                    node.col_offset,
                    f"dbt invoked from Python code in {name!r} ({fname or 'dbtRunner.invoke'}); "
                    "not analysed",
                    ctx,
                    category=UnknownCategory.PYTHON_DBT,
                    mentions_dbt=True,
                )

    # --- expressions ----------------------------------------------------------------------
    def eval(self, node: ast.expr | None, scope: Scope, ctx: Ctx) -> Val:
        self._step()
        if node is None:
            return Const(None)
        meth = getattr(self, f"_e_{type(node).__name__}", None)
        if meth is None:
            self._scan_nested_operators(node, scope, ctx, f"{type(node).__name__} expression")
            return Unknown(
                f"{type(node).__name__} expressions are not evaluated",
                line=getattr(node, "lineno", 0),
            )
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
                    return Unknown(
                        "an f-string part is not resolvable"
                        + (f" ({v.reason})" if isinstance(v, Unknown) else ""),
                        line=node.lineno,
                    )
                if part.conversion == ord("r"):
                    s = repr(s)
                out.append(s)
                confs.append(v.conf)
                notes.extend(v.notes)
        return _bounded_str("".join(out), weakest(*confs), tuple(dict.fromkeys(notes)), node.lineno)

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

    def _module_attr(self, module: str, attr: str) -> Val | None:
        """``constants.SHOP_PATH`` where ``constants`` is a workspace module (AST only)."""
        path = self.loader.find(module, self.path)
        if path is None:
            return None
        fs = self.loader.module(path, self.import_depth + 1)
        v = fs.scope.vars.get(attr) if fs is not None else None
        if isinstance(v, (Const, PathV, ListV, DictV, CallV)):
            v = _clone(v)
            v.with_meta(
                "inferred",
                (f"{module}.{attr} resolved via import from {self.index.relative(path)}",),
            )
            return v
        return None

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
            v = _clone(v)
            v.with_meta(
                "inferred", (f"{local} resolved via import from {self.index.relative(path)}",)
            )
            return v
        if isinstance(v, FuncV):
            v.imported_from = path
            return v
        if isinstance(v, Sym):
            return v
        return None

    def _e_Attribute(self, node: ast.Attribute, scope: Scope, ctx: Ctx) -> Val:
        base = self.eval(node.value, scope, ctx)
        a = node.attr
        if isinstance(base, Sym):
            resolved = self._module_attr(base.name, a)
            return resolved if resolved is not None else Sym(f"{base.name}.{a}")
        if isinstance(base, PathV) or (
            isinstance(base, Const)
            and isinstance(base.value, str)
            and a in ("parent", "name", "stem")
        ):
            p = base.path if isinstance(base, PathV) else base.value  # type: ignore[union-attr]
            if a == "parent":
                return PathV(
                    posixpath.dirname(p.rstrip("/")) or "/",
                    conf=weakest(base.conf, "inferred"),
                    notes=base.notes,
                )
            if a == "name":
                return Const(posixpath.basename(p.rstrip("/")), conf=base.conf, notes=base.notes)
            if a == "stem":
                return Const(
                    posixpath.splitext(posixpath.basename(p.rstrip("/")))[0],
                    conf=base.conf,
                    notes=base.notes,
                )
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
            return Unknown(
                f"environment variable {k!r} read with os.environ[...] (no default)",
                line=node.lineno,
            )
        if isinstance(base, DictV) and isinstance(key, Const) and isinstance(key.value, str):
            v = base.items.get(key.value)
            return (
                v if v is not None else Unknown(f"key {key.value!r} not in dict", line=node.lineno)
            )
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
        if len(items) > MAX_ITEMS:
            return Unknown(f"a list of more than {MAX_ITEMS} items", line=node.lineno)
        return ListV(items)

    _e_Tuple = _e_List
    _e_Set = _e_List

    def _e_Dict(self, node: ast.Dict, scope: Scope, ctx: Ctx) -> Val:
        items: dict[str, Val] = {}
        conf = "declared"
        for k, v in zip(node.keys, node.values, strict=False):
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
                if (
                    isinstance(v, Unknown)
                    and v.reason.startswith("environment variable")
                    and i + 1 < len(vals)
                ):
                    nxt = _clone(vals[i + 1])
                    return nxt.with_meta(
                        "heuristic", (f"{v.reason}; used the `or` fallback value",)
                    )
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
        """Operators created inside expressions not evaluated (comprehensions) become unknown."""
        if not ctx.emit:
            return
        for sub in ast.walk(node):
            if isinstance(sub, ast.Call) and sub is not node:
                name = self._dotted_name(sub.func, scope) or ""
                kind = _classify(name)
                if kind and kind not in ("dag", "task_group", "chain", "generic"):
                    self._unresolved(
                        sub.lineno,
                        sub.col_offset,
                        f"{name.rsplit('.', 1)[-1]} created inside a {where}; tasks are dynamic",
                        ctx,
                        dag_id=ctx.dag.dag_id if ctx.dag else None,
                        category=self._dynamic_category(),
                        mentions_dbt=kind in ("bash", "pod", "docker", "ssh")
                        and "dbt" in self._segment(sub).lower()
                        or kind not in ("bash", "pod", "docker", "ssh", "sql"),
                    )

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
                self._unresolved(
                    line,
                    col,
                    "dependency operand is not a known task (>>/<<)",
                    ctx,
                    dag_id=ctx.dag.dag_id if ctx.dag else None,
                    category=UnknownCategory.DEPENDENCY,
                )
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
            if (
                isinstance(left, Const)
                and isinstance(right, Const)
                and isinstance(left.value, str)
                and isinstance(right.value, str)
            ):
                conf, notes = collect_meta(left, right)
                return _bounded_str(
                    left.value + right.value, weakest(conf, "inferred"), notes, line
                )
            if isinstance(left, ListV) and isinstance(right, ListV):
                if len(left.items) + len(right.items) > MAX_ITEMS:
                    return Unknown(f"a list of more than {MAX_ITEMS} items", line=line)
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
            if not _safe_percent(left.value):
                return Unknown("%-format with a very large width", line=line)
            try:
                return _bounded_str(left.value % args, "inferred", (), line)
            except (TypeError, ValueError, MemoryError, OverflowError):
                return Unknown("%-format failed", line=line)
        if isinstance(left, Unknown):
            return left
        if isinstance(right, Unknown):
            return right
        for side in (left, right):
            if isinstance(side, CallV):
                return Unknown(
                    f"result of {side.func or 'a call'}() is not known statically", line=line
                )
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
            fval = (
                self.eval(func, scope, ctx)
                if not isinstance(func, ast.Name)
                else (scope.lookup(func.id) or Sym(func.id))
            )
            fname = (
                fval.name
                if isinstance(fval, Sym)
                else (func.id if isinstance(func, ast.Name) else "")
            )
        args, kwargs, has_star = self._call_args(node, scope, ctx)
        if isinstance(fval, FuncV):
            return self._call_function(fval, node, args, kwargs, has_star, scope, ctx)
        return self._call_named(fname, node, args, kwargs, has_star, scope, ctx)

    def _call_args(
        self, node: ast.Call, scope: Scope, ctx: Ctx
    ) -> tuple[list[Val], dict[str, Val], bool]:
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
        if isinstance(base, PathV) or (
            isinstance(base, Const) and isinstance(base.value, str) and meth in ("joinpath",)
        ):
            p = base.path if isinstance(base, PathV) else base.value  # type: ignore[union-attr]
            pv = base if isinstance(base, PathV) else PathV(p, conf=base.conf)
            if meth in ("resolve", "absolute"):
                return PathV(
                    posixpath.normpath(p), conf=weakest(base.conf, "inferred"), notes=base.notes
                )
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
                return PathV(
                    posixpath.join(posixpath.dirname(p), _to_str(args[0])), conf="inferred"
                )  # type: ignore[arg-type]
        if isinstance(base, Const) and isinstance(base.value, str):
            s = base.value
            try:
                if meth == "format":
                    sargs = [_to_str(a) for a in args]
                    skw = {k: _to_str(v) for k, v in kwargs.items()}
                    if any(a is None for a in sargs) or any(v is None for v in skw.values()):
                        return Unknown(".format() with non-literal arguments", line=line)
                    if not _safe_format(s):
                        return Unknown(
                            ".format() with attribute access or a format spec", line=line
                        )
                    return _bounded_str(
                        s.format(*sargs, **skw), weakest(base.conf, "inferred"), (), line
                    )
                if (
                    meth == "replace"
                    and len(args) >= 2
                    and all(_to_str(a) is not None for a in args[:2])
                ):
                    old, new = _to_str(args[0]), _to_str(args[1])
                    if len(s) + (s.count(old) + 1) * len(new) > MAX_STRING:  # type: ignore[arg-type]
                        return Unknown(f"a string of more than {MAX_STRING} characters", line=line)
                    return Const(s.replace(old, new), conf=weakest(base.conf, "inferred"))  # type: ignore[arg-type]
                if meth in ("strip", "lstrip", "rstrip", "lower", "upper") and not args:
                    return Const(getattr(s, meth)(), conf=base.conf)
                if meth == "join" and args and isinstance(args[0], ListV):
                    parts = [_to_str(i) for i in args[0].items]
                    if any(p is None for p in parts):
                        return Unknown("str.join over non-literal items", line=line)
                    return _bounded_str(s.join(parts), weakest(base.conf, "inferred"), (), line)  # type: ignore[arg-type]
                if meth == "split":
                    sep = _to_str(args[0]) if args else None
                    return ListV([Const(x) for x in s.split(sep)], conf=base.conf)
            except (IndexError, KeyError, ValueError, AttributeError, TypeError, MemoryError):
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
                self._unresolved(
                    line,
                    node.col_offset,
                    f".{meth}() argument is not a known task",
                    ctx,
                    dag_id=ctx.dag.dag_id if ctx.dag else None,
                    category=UnknownCategory.DEPENDENCY,
                )
            else:
                for o in others:
                    if meth == "set_downstream":
                        self._edge(base.handle, o, line, node.col_offset, ctx, meth)
                    else:
                        self._edge(o, base.handle, line, node.col_offset, ctx, meth)
            return Const(None)
        if isinstance(base, PartialV) and meth in ("expand", "expand_kwargs"):
            if meth == "expand_kwargs":
                merged = dict(base.kwargs)
                return self._create_task(
                    base.func,
                    node,
                    [],
                    merged,
                    False,
                    scope,
                    replace(
                        ctx,
                        dynamic=ctx.dynamic or ".expand_kwargs() mapping over non-literal input",
                    ),
                )
            merged = dict(base.kwargs)
            dyn = None
            for k, v in kwargs.items():
                if not isinstance(v, ListV):
                    dyn = f".expand({k}=...) over a non-literal value"
                merged[k] = v
            mctx = replace(ctx, dynamic=ctx.dynamic or dyn)
            return self._create_task(
                base.func, node, [], merged, False, scope, mctx, mapped=set(kwargs)
            )
        if isinstance(base, Unknown):
            return base
        for a in list(args) + list(kwargs.values()):
            if isinstance(a, Unknown):
                return Unknown(f".{meth}() argument not resolvable: {a.reason}", line=line)
        return Unknown(f".{meth}() on a {type(base).__name__} value is not evaluated", line=line)

    def _call_named(
        self,
        fname: str,
        node: ast.Call,
        args: list[Val],
        kwargs: dict[str, Val],
        has_star: bool,
        scope: Scope,
        ctx: Ctx,
    ) -> Val:
        line, col = node.lineno, node.col_offset
        last = fname.rsplit(".", 1)[-1]
        # environment and Airflow Variables: use the default, flagged heuristic
        if fname in ("os.getenv", "os.environ.get", "getenv", "environ.get") or fname.endswith(
            ".environ.get"
        ):
            key = as_str(args[0]) if args else None
            default = args[1] if len(args) > 1 else kwargs.get("default")
            if default is None or (isinstance(default, Const) and default.value is None):
                return Unknown(f"environment variable {key!r} has no default", line=line)
            d = _clone(default)
            return d.with_meta(
                "heuristic", (f"environment variable {key!r} assumed unset; used its default",)
            )
        if fname.endswith("Variable.get"):
            key = as_str(args[0]) if args else None
            default = (
                kwargs.get("default_var")
                or kwargs.get("default")
                or (args[1] if len(args) > 1 else None)
            )
            if default is None or (isinstance(default, Const) and default.value is None):
                return Unknown(f"Airflow Variable {key!r} has no default", line=line)
            return _clone(default).with_meta(
                "heuristic", (f"Airflow Variable {key!r} assumed unset; used default_var",)
            )
        if fname in ("os.path.join", "posixpath.join", "path.join") or (
            fname.endswith("path.join") and fname.startswith("os")
        ):
            parts = [_to_str(a) for a in args]
            if any(p is None for p in parts) or not parts:
                return Unknown("os.path.join with non-literal parts", line=line)
            conf, notes = collect_meta(*args)
            return Const(posixpath.join(*parts), conf=weakest(conf, "inferred"), notes=notes)  # type: ignore[arg-type]
        if fname in (
            "os.path.dirname",
            "os.path.abspath",
            "os.path.realpath",
            "os.path.normpath",
            "os.path.basename",
        ):
            s = _to_str(args[0]) if args else None
            if s is None:
                return Unknown(f"{fname} of a non-literal value", line=line)
            conf, notes = collect_meta(*args)
            fn = {"os.path.dirname": posixpath.dirname, "os.path.basename": posixpath.basename}.get(
                fname, posixpath.normpath
            )
            if fname in ("os.path.abspath", "os.path.realpath") and not is_absolute(s):
                return Unknown(
                    f"{fname} of a relative path depends on the working directory", line=line
                )
            return Const(fn(s), conf=weakest(conf, "inferred"), notes=notes)
        if fname in (
            "os.getcwd",
            "pathlib.Path.cwd",
            "Path.cwd",
            "pathlib.Path.home",
            "os.path.expanduser",
        ):
            return Unknown(f"{fname}() depends on the runtime environment", line=line)
        if fname in (
            "pathlib.Path",
            "pathlib.PurePath",
            "pathlib.PosixPath",
            "pathlib.PurePosixPath",
        ):
            if not args:
                return Unknown("Path() with no arguments is the working directory", line=line)
            parts = [_to_str(a) for a in args]
            if any(p is None for p in parts):
                bad = next(a for a, p in zip(args, parts, strict=False) if p is None)
                return Unknown(
                    "Path() of a non-literal value"
                    + (f": {bad.reason}" if isinstance(bad, Unknown) else ""),
                    line=line,
                )
            conf, notes = collect_meta(*args)
            out = PathV(posixpath.normpath(posixpath.join(*parts)), conf=conf, notes=notes)  # type: ignore[arg-type]
            return out
        if last in BUILTIN_CALLS and fname == last and scope.lookup(last) is None:
            if last == "str" and args:
                s = _to_str(args[0])
                if s is not None:
                    return Const(s, conf=args[0].conf, notes=args[0].notes)
                inner = (
                    args[0].reason
                    if isinstance(args[0], Unknown)
                    else f"a {type(args[0]).__name__} value"
                )
                return Unknown(f"str() of a non-literal: {inner}", line=line)
            if last == "dict":
                return (
                    DictV(dict(kwargs))
                    if not args
                    else Unknown("dict() with positional args", line=line)
                )
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
            self.reads_manifest = True
            self._unresolved(
                line,
                col,
                "reads a dbt manifest.json at DAG-parse time; tasks generated from it are dynamic",
                ctx,
                dag_id=ctx.dag.dag_id if ctx.dag else None,
                category=UnknownCategory.MANIFEST_GENERATED,
                mentions_dbt=True,
            )
            return Unknown("contents of manifest.json read at parse time", line=line)
        self._maybe_wrapper(fname, _root_name(node.func), node, kwargs, ctx)
        return CallV(fname, args, kwargs, line=line, col=col, has_star=has_star)

    def _maybe_wrapper(
        self, fname: str, local: str | None, node: ast.Call, kwargs: dict[str, Val], ctx: Ctx
    ) -> None:
        """Flag calls that may create dbt tasks: workspace wrappers, dbt-flavoured calls."""
        if not ctx.emit or fname.startswith(KNOWN_LIBRARY_PREFIXES):
            return
        dbt_kw = [k for k in kwargs if "dbt" in k.lower()]
        last = fname.rsplit(".", 1)[-1]
        info = None
        if local and local in self.imports:
            mod, attr, level = self.imports[local]
            path = self.loader.find(mod, self.path, level)
            if path is not None and attr:
                info = self.loader.operators_in(path, attr)
                if info:
                    self._unresolved(
                        node.lineno,
                        node.col_offset,
                        f"{fname}(...) is defined in {self.index.relative(path)} and creates "
                        f"{', '.join(sorted(info))} at parse time "
                        f"from non-literal inputs; tasks not resolved statically",
                        ctx,
                        dag_id=ctx.dag.dag_id if ctx.dag else None,
                        category=UnknownCategory.WRAPPER,
                        mentions_dbt=True,
                    )
                    return
        if dbt_kw:
            self._unresolved(
                node.lineno,
                node.col_offset,
                f"unrecognised call {fname}(...) with dbt-related arguments {dbt_kw}; "
                "may create dbt tasks",
                ctx,
                dag_id=ctx.dag.dag_id if ctx.dag else None,
                category=UnknownCategory.WRAPPER,
                mentions_dbt=True,
            )
        elif _DBT_NAME.search(last) and _CREATES_TASKS.search(last):
            self._unresolved(
                node.lineno,
                node.col_offset,
                f"{fname}(...) looks like it creates dbt tasks; what it creates cannot be read",
                ctx,
                dag_id=ctx.dag.dag_id if ctx.dag else None,
                category=UnknownCategory.WRAPPER,
                mentions_dbt=True,
            )

    def _no_dbt_command(self, handle: TaskHandle, node: ast.AST, ctx: Ctx, seen_in: str) -> None:
        """A task that mentions dbt in which no dbt command could be read: never nothing."""
        if not _DBT_NAME.search(seen_in):
            return
        self._unresolved(
            node.lineno,  # type: ignore[attr-defined]
            node.col_offset,  # type: ignore[attr-defined]
            "this task mentions dbt, and no dbt command could be read in it; it may be in the "
            "image, or in a form the scanner does not know",
            ctx,
            dag_id=handle.dag_id,
            task_id=handle.task_id,
            category=UnknownCategory.OTHER_OPERATOR,
            mentions_dbt=True,
        )

    def _call_function(
        self,
        fv: FuncV,
        node: ast.Call,
        args: list[Val],
        kwargs: dict[str, Val],
        has_star: bool,
        scope: Scope,
        ctx: Ctx,
    ) -> Val:
        if fv.kind == "task":
            return self._decorated_task(fv, node, ctx, None)
        if fv.kind == "task_bash":
            self.called.add(id(fv.node))
            cmd = self._exec_function(fv, node, args, kwargs, has_star, replace(ctx, emit=False))
            return self._decorated_task(fv, node, ctx, cmd)
        if fv.kind == "dag":
            return DagV(as_str(fv.decorator_kwargs.get("dag_id")) or fv.name)
        if fv.imported_from:
            ops = sorted(
                {
                    n
                    for n in (_call_name(c) for c in ast.walk(fv.node) if isinstance(c, ast.Call))
                    if _classify(n) not in (None, "generic", "chain")
                }
            )
            self._unresolved(
                node.lineno,
                node.col_offset,
                f"call to {fv.name}() imported from another module is not followed",
                ctx,
                category=UnknownCategory.WRAPPER,
                dag_id=ctx.dag.dag_id if ctx.dag else None,
                mentions_dbt=bool(ops) or "dbt" in self._segment(node).lower(),
            )
            return Unknown(f"call to imported function {fv.name}", line=node.lineno)
        self.called.add(id(fv.node))
        gctx = ctx
        handle = None
        if fv.kind == "task_group":
            gid = (
                as_str(fv.decorator_kwargs.get("group_id") or fv.decorator_kwargs.get("_pos0"))
                or fv.name
            )
            handle = TaskHandle(
                ctx.dag.dag_id if ctx.dag else None,
                ctx.prefix + gid,
                "task_group",
                node.lineno,
                node.col_offset,
                prefix_parts=ctx.group_prefix + (gid,),
            )
            self._record_task(handle, "@task_group", ctx)
            gctx = replace(ctx, group_prefix=ctx.group_prefix + (gid,))
        ret = self._exec_function(fv, node, args, kwargs, has_star, gctx)
        return TaskV(handle) if handle is not None else ret

    def _not_followed(self, fv: FuncV, node: ast.Call, ctx: Ctx, why: str) -> Val:
        """A call we do not follow; if the function builds operators, say so at the call site."""
        ops = sorted(
            {
                n
                for n in (_call_name(c) for c in ast.walk(fv.node) if isinstance(c, ast.Call))
                if _classify(n) not in (None, "generic", "chain")
            }
        )
        if ops:
            self._unresolved(
                node.lineno,
                node.col_offset,
                f"call to {fv.name}() not followed ({why}); it creates {', '.join(ops)}",
                ctx,
                dag_id=ctx.dag.dag_id if ctx.dag else None,
                category=UnknownCategory.NOT_FOLLOWED,
                mentions_dbt=True,
            )
        return Unknown(f"call to {fv.name}() not followed ({why})", line=node.lineno)

    def _exec_function(
        self,
        fv: FuncV,
        node: ast.Call,
        args: list[Val],
        kwargs: dict[str, Val],
        has_star: bool,
        ctx: Ctx,
    ) -> Val:
        if self._call_depth >= MAX_CALL_DEPTH or fv.name in ctx.funcs:
            return self._not_followed(fv, node, ctx, "depth/recursion limit")
        fn = fv.node
        inner = Scope(fv.defining_scope or self.scope)
        params = fn.args
        positional = [a.arg for a in params.posonlyargs + params.args]
        defaults = params.defaults
        dscope = fv.defining_scope or self.scope
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
        for a, d in zip(params.kwonlyargs, params.kw_defaults, strict=False):
            if a.arg in kwargs:
                inner.vars[a.arg] = kwargs[a.arg]
            elif d is not None:
                inner.vars[a.arg] = self.eval(d, dscope, Ctx(emit=False))
            else:
                inner.vars[a.arg] = Unknown(f"parameter {a.arg!r} not passed")
        if params.vararg:
            inner.vars[params.vararg.arg] = (
                ListV(args[len(positional) :]) if not has_star else Unknown("*args")
            )
        if params.kwarg:
            inner.vars[params.kwarg.arg] = (
                DictV({k: v for k, v in kwargs.items() if k not in positional})
                if not has_star
                else Unknown("**kwargs")
            )
        if has_star:
            for name in positional:
                if name not in kwargs and positional.index(name) >= len(args):
                    inner.vars[name] = Unknown(f"parameter {name!r} may come from *args/**kwargs")
        if any(isinstance(n, (ast.Yield, ast.YieldFrom)) for n in ast.walk(fn)):
            return Unknown(f"generator function {fv.name}()", line=node.lineno)
        cctx = replace(
            ctx,
            funcs=ctx.funcs + (fv.name,),
            call_sites=ctx.call_sites + ((node.lineno, node.col_offset),),
        )
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
        owner = _owner_of(fv.decorator_kwargs) or (dag.owner if dag else None)
        handle = TaskHandle(
            dag.dag_id if dag else None,
            (ctx.prefix + tid) if tid else None,
            "decorated_task",
            node.lineno,
            node.col_offset,
            owner=owner,
        )
        self._record_task(handle, "@task.bash" if fv.kind == "task_bash" else "@task", ctx)
        if fv.kind == "task_bash":
            self._shell_task(
                "task_bash",
                handle,
                bash_cmd,
                node,
                ctx,
                {},
                Const(tid) if tid else Unknown("task_id"),
            )
        return TaskV(handle)

    # --- task creation --------------------------------------------------------------------
    def _create_task(
        self,
        fname: str,
        node: ast.Call,
        args: list[Val],
        kwargs: dict[str, Val],
        has_star: bool,
        scope: Scope,
        ctx: Ctx,
        mapped: set[str] | None = None,
    ) -> Val:
        kind = _classify(fname)
        line, col = node.lineno, node.col_offset
        if kind == "dag":
            dag_id_v = kwargs.get("dag_id") or (args[0] if args else None)
            dag_id = as_str(dag_id_v)
            reason = None
            if dag_id is None:
                reason = "dag_id is not statically resolvable" + (
                    f": {dag_id_v.reason}" if isinstance(dag_id_v, Unknown) else ""
                )
            dv = DagV(
                dag_id,
                reason,
                conf=dag_id_v.conf if dag_id_v is not None and dag_id is not None else "declared",
                notes=dag_id_v.notes if dag_id_v is not None else (),
                owner=_owner_of(kwargs),
            )
            self._record_dag(_dag_info(dv, node), "DAG()", ctx)
            return dv
        if kind == "chain":
            items = [_task_list(a) for a in args]
            if any(i is None for i in items):
                self._unresolved(
                    line,
                    col,
                    f"{fname}() has an argument that is not a known task",
                    ctx,
                    category=UnknownCategory.DEPENDENCY,
                )
            elif fname.rsplit(".", 1)[-1] == "cross_downstream" and len(items) == 2:
                for u in items[0]:  # type: ignore[union-attr]
                    for d in items[1]:  # type: ignore[union-attr]
                        self._edge(u, d, line, col, ctx, "cross_downstream")
            else:
                for a, b in zip(items, items[1:], strict=False):
                    if len(a) > 1 and len(b) > 1:  # type: ignore[arg-type]
                        if len(a) != len(b):  # type: ignore[arg-type]
                            self._unresolved(
                                line,
                                col,
                                "chain() with lists of different lengths",
                                ctx,
                                category=UnknownCategory.DEPENDENCY,
                            )
                            continue
                        pairs = list(zip(a, b, strict=False))  # type: ignore[arg-type]
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
            parts = ctx.group_prefix if no_prefix else (ctx.group_prefix + (gid,) if gid else None)
            handle = TaskHandle(
                dag_id,
                (ctx.prefix + gid) if gid else None,
                "task_group",
                line,
                col,
                prefix_parts=parts,
            )
            self._record_task(handle, fname, ctx)
            return TaskV(handle)
        if kind in ("cosmos_dag", "cosmos_task_group"):
            call = CallV(fname, args, kwargs, line=line, col=col, has_star=has_star)
            return self._cosmos(kind, call, node, ctx, dag)
        tid_v = kwargs.get("task_id")
        tid = as_str(tid_v)
        owner = _owner_of(kwargs) or (dag.owner if dag else None)
        handle = TaskHandle(
            dag_id, (ctx.prefix + tid) if tid else None, "operator", line, col, owner=owner
        )
        self._record_task(handle, fname, ctx)
        if kind == "cosmos_operator":
            self._cosmos_operator(fname, handle, node, kwargs, ctx, dag, tid_v, mapped or set())
        elif kind == "airflow_dbt_operator":
            self._airflow_dbt_operator(fname, handle, node, kwargs, ctx, tid_v)
        elif kind in ("bash", "ssh"):
            cmd_key = "bash_command" if kind == "bash" else "command"
            self._shell_task(
                kind, handle, kwargs.get(cmd_key), node, ctx, kwargs, tid_v, mapped=mapped
            )
        elif kind == "pod":
            self._argv_task(kind, handle, node, ctx, kwargs, tid_v)
        elif kind == "docker":
            cmd = kwargs.get("command")
            if isinstance(cmd, ListV):
                self._argv_task(kind, handle, node, ctx, {"cmds": cmd}, tid_v)
            else:
                self._shell_task(kind, handle, cmd, node, ctx, kwargs, tid_v)
        elif kind == "sql":
            self._sql_task(fname, handle, node, kwargs, ctx)
        elif kind == "generic" and (
            re.search(r"\bdbt\b", self._segment(node), re.I)
            or "dbt" in fname.rsplit(".", 1)[-1].lower()
        ):
            self._unresolved(
                line,
                col,
                f"{fname.rsplit('.', 1)[-1]} mentions dbt, but what it runs cannot be read",
                ctx,
                dag_id=dag_id,
                task_id=handle.task_id,
                category=UnknownCategory.OTHER_OPERATOR,
                mentions_dbt=True,
            )
        return TaskV(handle)

    # --- records for DAGs, tasks and edges ---------------------------------------------------
    def _record_dag(self, info: DagInfo, how: str, ctx: Ctx) -> None:
        if not ctx.emit:
            return
        key = ("dag", info.line, info.col, ctx.call_sites)
        if key in self._seen:
            return
        self._seen.add(key)
        self.dags.append(
            {
                "dag_id": info.dag_id,
                "how": how,
                "location": self._loc(info.line, info.col),
                "confidence": info.conf,
                "owner": info.owner,
            }
        )
        if info.dag_id is None:
            self._unresolved(
                info.line,
                info.col,
                info.reason or "dag_id not resolvable",
                ctx,
                category=UnknownCategory.NOT_LITERAL,
                mentions_dbt=False,
            )

    def _record_task(self, handle: TaskHandle, fname: str, ctx: Ctx) -> None:
        if not ctx.emit:
            return
        key = ("task", handle.line, handle.col, ctx.call_sites)
        if key in self._seen:
            return
        self._seen.add(key)
        self.tasks.append(
            {
                "dag_id": handle.dag_id,
                "task_id": handle.task_id,
                "operator": fname.rsplit(".", 1)[-1],
                "kind": handle.kind,
                "location": self._loc(handle.line, handle.col),
                "dynamic": ctx.dynamic,
                "owner": handle.owner,
            }
        )

    def _edge(
        self, up: TaskHandle, down: TaskHandle, line: int, col: int, ctx: Ctx, how: str
    ) -> None:
        if not ctx.emit:
            return
        key = (
            "edge",
            up.dag_id or down.dag_id,
            up.task_id,
            down.task_id,
            id(up) if not up.task_id else 0,
            id(down) if not down.task_id else 0,
        )
        if key in self._seen:
            return
        self._seen.add(key)
        dag_id = up.dag_id or down.dag_id
        rec = {
            "edge": "task_depends_on",
            "origin": ORIGIN,
            "dag_id": dag_id,
            "upstream": f"airflow:{dag_id}.{up.task_id}" if up.task_id else None,
            "downstream": f"airflow:{dag_id}.{down.task_id}" if down.task_id else None,
            "how": how,
            "confidence": "declared" if not ctx.dynamic else "heuristic",
            "location": self._loc(line, col),
        }
        if ctx.dynamic or not up.task_id or not down.task_id:
            rec["status"] = "unknown"
            rec["reason"] = ctx.dynamic or "task id not resolvable"
        self.dependencies.append(rec)

    # --- dbt links ------------------------------------------------------------------------
    def _base_link(
        self,
        kind: str,
        node: ast.AST,
        ctx: Ctx,
        dag: DagInfo | None,
        task_id: str | None,
        group_id: str | None = None,
    ) -> dict[str, Any]:
        rec: dict[str, Any] = {
            "origin": ORIGIN,
            "kind": kind,
            "confidence": "declared",
            "location": self._loc(node.lineno, node.col_offset),  # type: ignore[attr-defined]
            "dag_id": dag.dag_id if dag else None,
            "task_id": task_id,
            "group_id": group_id,
            "dbt_command": None,
            "project_path": None,
            "manifest": None,
            "select": [],
            "exclude": [],
            "selector": None,
            "resolved_models": None,
            "resolved_nodes": None,
            "status": "unknown",
            "unknown_reasons": [],
            "categories": [],
            "notes": [],
            "owner": dag.owner if dag else None,
        }
        if ctx.call_sites:
            # report the outermost call site (where a user looks); keep where the call was built
            rec["constructed_at"] = rec["location"]
            rec["location"] = self._loc(*ctx.call_sites[0])
            rec["via_call_at"] = [list(cs) for cs in ctx.call_sites]
        if dag is None:
            self._why(rec, UnknownCategory.NOT_LITERAL, "task is not inside a resolvable DAG")
        elif dag.dag_id is None:
            self._why(rec, UnknownCategory.NOT_LITERAL, dag.reason or "dag_id not resolvable")
        else:
            rec["confidence"] = weakest(rec["confidence"], dag.conf)
        if ctx.dynamic:
            self._why(rec, self._dynamic_category(), ctx.dynamic)
        if ctx.branch:
            rec["notes"].append("defined inside a conditional branch")
        return rec

    def _finish(self, rec: dict[str, Any], ctx: Ctx) -> None:
        if not ctx.emit:
            return
        if rec.get("other_project"):
            self.other_project_links += 1
            return
        key = (
            "link",
            rec["location"]["line"],
            rec["location"]["col"],
            rec.get("command_index"),
            ctx.call_sites,
        )
        if key in self._seen:
            return
        self._seen.add(key)
        rec["notes"] = list(dict.fromkeys(rec["notes"]))
        rec["unknown_reasons"] = list(dict.fromkeys(rec["unknown_reasons"]))
        rec["in_scope"] = (
            (rec.get("dbt_command") in BUILDING_COMMANDS)
            or rec["kind"].startswith("cosmos_dag")
            or rec["kind"] == "cosmos_task_group"
        )
        if rec["resolved_models"] is not None and not rec["unknown_reasons"]:
            rec["status"] = "resolved"
        else:
            rec["status"] = "unknown"
            if rec["resolved_models"] is None and not rec["unknown_reasons"]:
                self._why(rec, UnknownCategory.NOT_LITERAL, "model set not resolvable")
            # never hand out a model set for an unknown link; keep it only as a diagnostic
            if rec["resolved_models"] is not None:
                rec["partial_models"] = rec["resolved_models"]
            rec["resolved_models"] = None
            rec["resolved_nodes"] = None
        self.links.append(rec)
        if rec["status"] == "unknown":
            rec["category"] = most_specific(rec["categories"])
            self._unresolved(
                rec["location"]["line"],
                rec["location"]["col"],
                "; ".join(rec["unknown_reasons"]),
                ctx,
                dag_id=rec["dag_id"],
                task_id=rec.get("task_id") or rec.get("group_id"),
                category=UnknownCategory(rec["category"]),
                mentions_dbt=True,
            )

    def _resolve_project(
        self,
        rec: dict[str, Any],
        raw: Val | None,
        base_dir: str | None,
        ctx: Ctx,
        label: str = "project_dir",
    ) -> str | None:
        """Turn a project path value into a workspace dbt project directory."""
        if raw is None:
            if base_dir:
                m = self.index.match(base_dir)
                if m.path:
                    rec["notes"].append(
                        f"project taken from the working directory {base_dir}"
                        + (f"; {m.note}" if m.note else "")
                    )
                    rec["confidence"] = weakest(rec["confidence"], m.conf)
                    return m.path
            only = self.index.only_project()
            if only:
                rec["notes"].append(
                    f"no {label}: dbt uses its working directory; "
                    "assumed the only dbt project in the workspace"
                )
                rec["confidence"] = weakest(rec["confidence"], "heuristic")
                return only
            self._why(
                rec, UnknownCategory.NO_PROJECT, f"no {label} and the working directory is unknown"
            )
            return None
        s = as_str(raw)
        if s is None:
            self._why(
                rec,
                UnknownCategory.NOT_LITERAL,
                f"{label} not resolvable" + (f": {raw.reason}" if isinstance(raw, Unknown) else ""),
            )
            return None
        rec["project_path_raw"] = s
        rec["confidence"] = weakest(rec["confidence"], raw.conf)
        rec["notes"].extend(raw.notes)
        if "{{" in s:
            self._why(rec, UnknownCategory.JINJA, f"{label} contains a Jinja template: {s}")
            return None
        m = self.index.match(s, base_dir)
        if m.path is None:
            self._why(rec, UnknownCategory.NO_PROJECT, m.reason or f"{label} {s!r} not found")
            return None
        if m.note:
            rec["notes"].append(m.note)
        rec["confidence"] = weakest(rec["confidence"], m.conf)
        return m.path

    def _apply_selection(
        self,
        rec: dict[str, Any],
        choice: ManifestChoice,
        command: str | None,
        select: list[str],
        exclude: list[str],
        selector: str | None,
    ) -> Manifest | None:
        if choice.manifest is None:
            self._why(rec, UnknownCategory.NO_PROJECT, choice.reason or "no manifest available")
            return None
        m = choice.manifest
        if choice.kind == "stand_in" and not m.of_type("model"):
            self._why(
                rec,
                UnknownCategory.GENERATED_PROJECT,
                "dbt project has no model files in the workspace (generated at build time?)",
            )
        rec["manifest"] = {
            "kind": choice.kind,
            "path": self.index.relative(m.path) if m.path else None,
            **m.artifact_version(),
        }
        rec["notes"].extend(choice.notes)
        rec["confidence"] = weakest(rec["confidence"], choice.conf)
        types = RESOURCE_TYPES_FOR_COMMAND.get(command or "", None)
        if types is None:
            rec["resolved_models"], rec["resolved_nodes"] = [], []
            return m
        sel = run_select(
            m,
            select or None,
            exclude or None,
            selector,
            resource_types=("model", "seed", "snapshot"),
        )
        if sel.unknown:
            self._why(
                rec, UnknownCategory.UNSUPPORTED_SELECTOR, f"selector not supported: {sel.reason}"
            )
            return m
        rec["notes"].extend(sel.notes)
        ids = sorted(sel.ids)  # type: ignore[arg-type]
        nodes = [i for i in ids if m.nodes[i].resource_type in types]
        if command == "test":
            nodes = [i for i in nodes if i in m.tested]
            rec["notes"].append("dbt test: linked to selected nodes that have at least one test")
        rec["resolved_nodes"] = nodes
        rec["resolved_models"] = [i for i in nodes if m.nodes[i].resource_type == "model"]
        return m

    def _link_from_invocation(
        self,
        kind: str,
        handle: TaskHandle,
        inv: DbtInvocation,
        idx: int,
        total: int,
        node: ast.AST,
        ctx: Ctx,
        base_conf: str,
        base_notes: tuple[str, ...],
        tid_v: Val | None,
        env: Val | None = None,
        mapped: bool = False,
        task_cwd: Val | None = None,
    ) -> None:
        rec = self._base_link(kind, node, ctx, _dag_of(handle, ctx), handle.task_id)
        rec["owner"] = handle.owner or rec["owner"]
        if handle.task_id is None:
            self._why(
                rec,
                UnknownCategory.NOT_LITERAL,
                "task_id not resolvable"
                + (f": {tid_v.reason}" if isinstance(tid_v, Unknown) else ""),
            )
        if total > 1:
            rec["command_index"] = idx
        rec["confidence"] = weakest(rec["confidence"], base_conf)
        rec["notes"].extend(base_notes)
        rec["notes"].extend(inv.notes)
        rec["dbt_command"] = inv.command
        rec["select"], rec["exclude"], rec["selector"] = inv.select, inv.exclude, inv.selector
        rec["target"] = inv.target
        rec["command_text"] = inv.raw[:500]
        if mapped:
            rec["notes"].append(
                "dynamic task mapping over a literal list: union of all mapped values"
            )
        for _, reason in inv.unknown_fields.items():
            self._why(rec, _category_of_template(reason), reason)
        project_raw: Val | None = Const(inv.project_dir) if inv.project_dir else None
        if project_raw is None and isinstance(env, DictV) and "DBT_PROJECT_DIR" in env.items:
            project_raw = env.items["DBT_PROJECT_DIR"]
            rec["notes"].append("project dir from env DBT_PROJECT_DIR")
        if inv.remote:
            rec["notes"].append(
                "dbt runs in another container or machine; its working directory is not known"
            )
        base_dir = inv.cwd
        if task_cwd is not None and not inv.remote:
            cwd_s = as_str(task_cwd)
            if cwd_s is None:
                self._why(
                    rec,
                    UnknownCategory.NOT_LITERAL,
                    "the task's working directory is not resolvable",
                )
            else:
                rec["confidence"] = weakest(rec["confidence"], task_cwd.conf)
                rec["notes"].extend(task_cwd.notes)
                if base_dir is None:
                    base_dir = cwd_s
                elif not is_absolute(base_dir):
                    base_dir = posixpath.normpath(posixpath.join(cwd_s, base_dir))
        proj = self._resolve_project(rec, project_raw, base_dir, ctx)
        if proj:
            rec["project_path"] = self.index.relative(proj)
            if self.index.is_other_project(proj):
                rec["other_project"] = True
            elif "dbt_command" not in inv.unknown_fields:
                self._apply_selection(
                    rec,
                    self.index.manifest_for(proj),
                    inv.command,
                    inv.select,
                    inv.exclude,
                    inv.selector,
                )
        self._finish(rec, ctx)

    def _shell_task(
        self,
        kind: str,
        handle: TaskHandle,
        cmd: Val | None,
        node: ast.AST,
        ctx: Ctx,
        kwargs: dict[str, Val],
        tid_v: Val | None,
        mapped: set[str] | None = None,
    ) -> None:
        if not ctx.emit:
            return
        if cmd is None and kind == "docker":  # what it runs is in the image
            self._no_dbt_command(handle, node, ctx, self._segment(node))
        commands: list[Val] = []
        if isinstance(cmd, ListV) and mapped and ("bash_command" in mapped or "command" in mapped):
            commands = list(cmd.items)
        elif cmd is not None:
            commands = [cmd]
        for c in commands:
            text = as_str(c)
            if text is None:
                reason = "shell command not statically resolvable" + (
                    f": {c.reason}" if isinstance(c, Unknown) else ""
                )
                self._unresolved(
                    node.lineno,
                    node.col_offset,
                    reason,
                    ctx,
                    dag_id=handle.dag_id,
                    task_id=handle.task_id,  # type: ignore[attr-defined]
                    category=UnknownCategory.NOT_LITERAL,
                    mentions_dbt="dbt" in self._segment(node).lower(),
                )
                continue
            invs, err = find_dbt_in_shell(text)
            if err:
                self._unresolved(
                    node.lineno,
                    node.col_offset,
                    err,
                    ctx,
                    dag_id=handle.dag_id,
                    task_id=handle.task_id,  # type: ignore[attr-defined]
                    category=UnknownCategory.NOT_LITERAL,
                    mentions_dbt=True,
                )
            if not invs:
                if re.search(r"\.(sh|bash)(\s|$)", text) and not text.strip().startswith(
                    ("echo", "#")
                ):
                    self._unresolved(
                        node.lineno,
                        node.col_offset,
                        f"shell command runs a script; its contents are not analysed: {text[:120]}",  # type: ignore[attr-defined]
                        ctx,
                        dag_id=handle.dag_id,
                        task_id=handle.task_id,
                        category=UnknownCategory.SHELL_SCRIPT,
                        mentions_dbt="dbt" in (text + self._segment(node)).lower(),
                    )
                elif not text.strip().startswith(("echo", "#")):
                    self._no_dbt_command(handle, node, ctx, text)
                continue
            for i, inv in enumerate(invs):
                self._link_from_invocation(
                    kind,
                    handle,
                    inv,
                    i,
                    len(invs),
                    node,
                    ctx,
                    weakest(c.conf, "declared"),
                    c.notes,
                    tid_v,
                    env=kwargs.get("env") or kwargs.get("environment"),
                    mapped=bool(mapped),
                    task_cwd=kwargs.get("cwd") or kwargs.get("working_dir"),
                )

    def _argv_task(
        self,
        kind: str,
        handle: TaskHandle,
        node: ast.AST,
        ctx: Ctx,
        kwargs: dict[str, Val],
        tid_v: Val | None,
    ) -> None:
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
                self._unresolved(
                    node.lineno,
                    node.col_offset,
                    "container command/arguments not statically resolvable",  # type: ignore[attr-defined]
                    ctx,
                    dag_id=handle.dag_id,
                    task_id=handle.task_id,
                    category=UnknownCategory.NOT_LITERAL,
                    mentions_dbt="dbt" in self._segment(node).lower(),
                )
                return
            parts.extend(lst)
            conf_vals.append(v)
        if not parts:  # what it runs is in the image
            self._no_dbt_command(handle, node, ctx, self._segment(node))
            return
        if cmds is None:
            parts_note = ("no cmds: assumed the image entrypoint is dbt-compatible with arguments",)
        else:
            parts_note = ()
        invs = find_dbt_in_argv(parts)
        if not invs and cmds is None and parts and parts[0] in BUILDING_COMMANDS:
            invs = [parse_dbt_argv(parts)]
            parts_note = (
                "no cmds and arguments start with a dbt sub-command: "
                "assumed the image entrypoint is dbt",
            )
        if not invs:
            self._no_dbt_command(handle, node, ctx, self._segment(node) + " ".join(parts))
        conf, notes = collect_meta(*conf_vals)
        for i, inv in enumerate(invs):
            self._link_from_invocation(
                kind,
                handle,
                inv,
                i,
                len(invs),
                node,
                ctx,
                weakest(conf, "heuristic" if parts_note else "declared"),
                notes + parts_note,
                tid_v,
                env=kwargs.get("env_vars"),
            )

    def _sql_task(
        self, fname: str, handle: TaskHandle, node: ast.Call, kwargs: dict[str, Val], ctx: Ctx
    ) -> None:
        """Keep the literal SQL of a SQL operator; ``_read_sql`` parses it once the file is read."""
        if not ctx.emit:
            return
        last = fname.rsplit(".", 1)[-1]
        rec: dict[str, Any] = {
            "origin": ORIGIN,
            "kind": "sql",
            "operator": last,
            "dag_id": handle.dag_id,
            "task_id": handle.task_id,
            "owner": handle.owner,
            "location": self._loc(node.lineno, node.col_offset),
            "confidence": "inferred",
            "statements": [],
            "reads": [],
            "writes": [],
            "status": "unknown",
            "notes": [],
        }
        value = kwargs.get("table") if last in SQL_TABLE_OPS else self._sql_argument(kwargs)
        if value is None:
            return
        rec["confidence"] = weakest(rec["confidence"], value.conf)
        rec["notes"].extend(collect_meta(value)[1])
        if ctx.dynamic or handle.dag_id is None or handle.task_id is None:
            why = ctx.dynamic or "the task's DAG or id is not resolvable"
            self._unresolved(
                node.lineno,
                node.col_offset,
                f"a SQL task: {why}",
                ctx,
                dag_id=handle.dag_id,
                task_id=handle.task_id,
                category=UnknownCategory.SQL,
                mentions_dbt=False,
            )
            return
        if last in SQL_TABLE_OPS:
            name = as_str(value)
            if name is None or "{{" in name:
                self._unresolved(
                    node.lineno,
                    node.col_offset,
                    "the table a SQL check reads is only known at run time",
                    ctx,
                    dag_id=handle.dag_id,
                    task_id=handle.task_id,
                    category=UnknownCategory.SQL,
                    mentions_dbt=False,
                )
                return
            rec["statements"] = [f"SELECT * FROM {name}"]
            self.sql.append(rec)
            return
        items = as_str_list(value)
        if items is None:
            reason = "its SQL is not literal" + (
                f": {value.reason}" if isinstance(value, Unknown) else ""
            )
            self._unresolved(
                node.lineno,
                node.col_offset,
                reason,
                ctx,
                dag_id=handle.dag_id,
                task_id=handle.task_id,
                category=UnknownCategory.SQL,
                mentions_dbt=False,
            )
            return
        statements = []
        for item in items:
            text = self._sql_text(item, node, ctx, handle)
            if text is None:
                return
            statements.append(text)
        rec["statements"] = statements
        self.sql.append(rec)

    @staticmethod
    def _sql_argument(kwargs: dict[str, Val]) -> Val | None:
        for key in SQL_ARGUMENTS:
            if key in kwargs:
                return kwargs[key]
        configuration = kwargs.get("configuration")  # BigQueryInsertJobOperator
        query = configuration.items.get("query") if isinstance(configuration, DictV) else None
        return query.items.get("query") if isinstance(query, DictV) else None

    def _sql_text(self, item: str, node: ast.Call, ctx: Ctx, handle: TaskHandle) -> str | None:
        """A statement, or the file a ``.sql`` value names, which Airflow reads as a template."""
        if len(item) > MAX_SQL_BYTES:
            self._unresolved(
                node.lineno,
                node.col_offset,
                f"its SQL is over {MAX_SQL_BYTES} characters",
                ctx,
                dag_id=handle.dag_id,
                task_id=handle.task_id,
                category=UnknownCategory.SQL,
                mentions_dbt=False,
            )
            return None
        if not item.strip().lower().endswith(".sql"):
            return item
        path = os.path.join(os.path.dirname(self.path), item.strip())
        data = read_capped(path, MAX_SQL_BYTES, self.index.workspace)
        if data is None:
            self._unresolved(
                node.lineno,
                node.col_offset,
                f"its SQL file {item.strip()} is not next to the DAG file",
                ctx,
                dag_id=handle.dag_id,
                task_id=handle.task_id,
                category=UnknownCategory.SQL,
                mentions_dbt=False,
            )
            return None
        return data.decode("utf-8", "replace")

    def _cosmos_operator(
        self,
        fname: str,
        handle: TaskHandle,
        node: ast.AST,
        kwargs: dict[str, Val],
        ctx: Ctx,
        dag: DagInfo | None,
        tid_v: Val | None,
        mapped: set[str],
    ) -> None:
        if not ctx.emit:
            return
        m = cz.COSMOS_OPERATOR_RE.match(fname.rsplit(".", 1)[-1])
        command = cz.OPERATOR_COMMANDS[m.group("cmd")] if m else None
        rec = self._base_link("cosmos_operator", node, ctx, dag, handle.task_id)
        rec["owner"] = handle.owner or rec["owner"]
        if handle.task_id is None:
            self._why(
                rec,
                UnknownCategory.NOT_LITERAL,
                "task_id not resolvable"
                + (f": {tid_v.reason}" if isinstance(tid_v, Unknown) else ""),
            )
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
                self._why(
                    rec,
                    UnknownCategory.NOT_LITERAL,
                    f"{key}= is not literal" + (f": {v.reason}" if isinstance(v, Unknown) else ""),
                )
            else:
                select.extend(x for s in lst for x in s.split())
                rec["confidence"] = weakest(rec["confidence"], v.conf)
        ev = kwargs.get("exclude")
        if ev is not None:
            lst = as_str_list(ev)
            if lst is None:
                self._why(rec, UnknownCategory.NOT_LITERAL, "exclude= is not literal")
            else:
                exclude.extend(x for s in lst for x in s.split())
        sv = kwargs.get("selector")
        if sv is not None:
            selector = as_str(sv)
            if selector is None:
                self._why(rec, UnknownCategory.NOT_LITERAL, "selector= is not literal")
        flags = kwargs.get("dbt_cmd_flags")
        if flags is not None:
            fl = as_str_list(flags)
            if fl is None:
                self._why(rec, UnknownCategory.NOT_LITERAL, "dbt_cmd_flags is not literal")
            else:
                inv = parse_dbt_argv([command.split()[0] if command else "run"] + fl)
                select.extend(inv.select)
                exclude.extend(inv.exclude)
                selector = selector or inv.selector
                for reason in inv.unknown_fields.values():
                    self._why(rec, _category_of_template(reason), reason)
        if any("{{" in s for s in select + exclude):
            self._why(rec, UnknownCategory.JINJA, "Jinja template in select/exclude")
        if mapped & {"select", "models", "exclude"}:
            rec["notes"].append(
                "dynamic task mapping over a literal list: union of all mapped values"
            )
        rec["select"], rec["exclude"], rec["selector"] = select, exclude, selector
        proj = self._resolve_project(rec, kwargs.get("project_dir"), None, ctx)
        if proj:
            rec["project_path"] = self.index.relative(proj)
            if self.index.is_other_project(proj):
                rec["other_project"] = True
            else:
                self._apply_selection(
                    rec, self.index.manifest_for(proj), command, select, exclude, selector
                )
        self._finish(rec, ctx)

    def _airflow_dbt_operator(
        self,
        fname: str,
        handle: TaskHandle,
        node: ast.AST,
        kwargs: dict[str, Val],
        ctx: Ctx,
        tid_v: Val | None,
    ) -> None:
        if not ctx.emit:
            return
        last = fname.rsplit(".", 1)[-1]
        m = re.match(
            r"^Dbt(Run|Test|Build|Seed|Snapshot|Deps|Compile|Ls|Docs\w*|Source\w*|Clean|Debug|Parse)\w*Operator$",
            last,
        )
        command = m.group(1).lower() if m else None
        args: list[str] = [command or "run"]
        for key, flag in (
            ("select", "--select"),
            ("models", "--select"),
            ("exclude", "--exclude"),
            ("selector", "--selector"),
        ):
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
        self._link_from_invocation(
            "airflow_dbt_operator", handle, inv, 0, 1, node, ctx, "declared", (), tid_v
        )

    def _cosmos(self, kind: str, call: CallV, node: ast.Call, ctx: Ctx, dag: DagInfo | None) -> Val:
        spec = cz.parse_cosmos("dag" if kind == "cosmos_dag" else "task_group", call)
        dag_v: Val = DagV(None)
        if kind == "cosmos_dag":
            dag_id = as_str(spec.dag_id)
            reason = None if dag_id else "DbtDag dag_id is not resolvable"
            dag_v = DagV(
                dag_id,
                reason,
                conf=spec.dag_id.conf if spec.dag_id is not None and dag_id else "declared",
                owner=_owner_of(call.kwargs),
            )
            info = _dag_info(dag_v, node)  # type: ignore[arg-type]
            self._record_dag(info, "DbtDag()", ctx)
            dag = info
            group_id = None
            prefix = ""
            ret: Val = dag_v
        else:
            group_id = as_str(spec.group_id)
            prefix = ctx.prefix + (f"{group_id}." if group_id else "")
            handle = TaskHandle(
                dag.dag_id if dag else None,
                (ctx.prefix + group_id) if group_id else None,
                "cosmos_task_group",
                node.lineno,
                node.col_offset,
                owner=_owner_of(call.kwargs) or (dag.owner if dag else None),
            )
            self._record_task(handle, "DbtTaskGroup", ctx)
            ret = TaskV(handle)
        if not ctx.emit:
            return ret
        rec = self._base_link(
            kind, node, ctx, dag, None, (ctx.prefix + group_id) if group_id else None
        )
        rec["dbt_command"] = "cosmos_render"
        rec["task_id_pattern"] = cz.pattern_for(spec, prefix)
        rec["cosmos"] = {
            "test_behavior": spec.test_behavior,
            "load_method": spec.load_method or "automatic",
            "execution_mode": spec.execution_mode,
            "source_rendering_behavior": spec.source_rendering,
            "seed_rendering_behavior": spec.seed_rendering,
            "group_nodes_by_folder": spec.group_nodes_by_folder,
            "project_path_from": spec.render_project_source,
        }
        rec["notes"].extend(spec.notes)
        if kind == "cosmos_task_group":
            rec["owner"] = ret.handle.owner  # type: ignore[attr-defined]
        for reason in spec.unknown:
            self._why(rec, UnknownCategory.NOT_LITERAL, reason)
        if kind == "cosmos_task_group" and group_id is None:
            self._why(rec, UnknownCategory.NOT_LITERAL, "DbtTaskGroup group_id not resolvable")
        if call.has_star:
            self._why(rec, UnknownCategory.NOT_LITERAL, "DbtDag/DbtTaskGroup called with **kwargs")
        if spec.group_id is not None and spec.group_id.notes:
            rec["notes"].extend(spec.group_id.notes)
        sel = as_str_list(spec.select)
        exc = as_str_list(spec.exclude)
        selector = as_str(spec.selector) if spec.selector is not None else None
        if sel is None:
            self._why(
                rec,
                UnknownCategory.NOT_LITERAL,
                "RenderConfig.select is not literal"
                + (f": {spec.select.reason}" if isinstance(spec.select, Unknown) else ""),
            )
        if exc is None:
            self._why(rec, UnknownCategory.NOT_LITERAL, "RenderConfig.exclude is not literal")
        if spec.selector is not None and selector is None:
            self._why(rec, UnknownCategory.NOT_LITERAL, "RenderConfig.selector is not literal")
        rec["select"], rec["exclude"], rec["selector"] = sel or [], exc or [], selector
        for v in (spec.select, spec.exclude, spec.selector):
            if v is not None and not isinstance(v, Unknown):
                rec["confidence"] = weakest(rec["confidence"], v.conf)
                rec["notes"].extend(collect_meta(v)[1])
        if any("{{" in s for s in (sel or []) + (exc or [])):
            self._why(rec, UnknownCategory.JINJA, "Jinja template in RenderConfig select/exclude")
        manifest, project_dir = self._cosmos_manifest(rec, spec, ctx)
        if manifest is not None and sel is not None and exc is not None:
            ids = self._cosmos_select(rec, manifest, spec, sel, exc, selector)
            if ids is not None:
                rec["resolved_nodes"] = ids
                rec["resolved_models"] = [
                    i for i in ids if manifest.nodes[i].resource_type == "model"
                ]
                excluded_tests: set[str] = set()
                if exc:
                    es = run_select(manifest, exc, None, None, resource_types=("test",))
                    excluded_tests = es.ids or set()
                pname = as_str(spec.project_name) or (
                    manifest.project_name or os.path.basename(project_dir or "") or "project"
                )
                rec["cosmos"]["versions"] = cz.SUPPORTED_COSMOS
                rows, extra, why = cz.expand_tasks(
                    manifest, ids, spec, prefix, excluded_tests, pname
                )
                if spec.group_nodes_by_folder:
                    rec["notes"].append(
                        "group_nodes_by_folder: task ids assume Cosmos' default "
                        "non-hierarchical naming "
                        "(same-named folders collapse, order-dependent); "
                        "enable_hierarchical_naming_for_group_nodes_by_folder changes them"
                    )
                if why:
                    rec["notes"].append(why)
                    rec["tasks"] = None
                else:
                    rec["tasks"] = rows
                    rec["extra_tasks"] = extra
        self._finish(rec, ctx)
        return ret

    def _cosmos_manifest(
        self, rec: dict[str, Any], spec: cz.CosmosSpec, ctx: Ctx
    ) -> tuple[Manifest | None, str | None]:
        """Which nodes Cosmos renders from: dbt ls file, declared manifest, or the project's own."""
        project_dir = None
        if spec.render_project is not None:
            project_dir = self._resolve_project(
                rec, spec.render_project, None, ctx, label=spec.render_project_source or "project"
            )
        if project_dir:
            rec["project_path"] = self.index.relative(project_dir)
            if self.index.is_other_project(project_dir):
                rec["other_project"] = True
                return None, project_dir
        lm = spec.load_method or "automatic"
        if lm == "dbt_ls_file":
            p = as_str(spec.dbt_ls_path)
            if p is None:
                self._why(
                    rec,
                    UnknownCategory.NOT_LITERAL,
                    "LoadMode.DBT_LS_FILE with a non-literal dbt_ls_path",
                )
                return None, project_dir
            real = regular_file_inside(p, self.index.workspace)
            if real is None:
                self._why(
                    rec, UnknownCategory.NO_PROJECT, f"dbt ls file {p} not found in the workspace"
                )
                return None, project_dir
            try:
                rows = load_dbt_ls_file(real, self.index.workspace)
            except (OSError, ManifestError) as exc:
                self._why(rec, UnknownCategory.NO_PROJECT, f"dbt ls file unreadable: {exc}")
                return None, project_dir
            data = {"metadata": {"project_name": None}, "nodes": {r["unique_id"]: r for r in rows}}
            m = manifest_from_dict(data, path=real)
            rec["manifest"] = {"kind": "dbt_ls_file", "path": self.index.relative(real)}
            rec["notes"].append(
                "LoadMode.DBT_LS_FILE: Cosmos renders exactly the nodes listed in the dbt ls file"
            )
            rec["confidence"] = weakest(rec["confidence"], "inferred")
            rec["_ls_file"] = True
            return m, project_dir
        if spec.manifest_path is not None and lm in ("automatic", "dbt_manifest"):
            mp = as_str(spec.manifest_path)
            if mp is None:
                self._why(
                    rec, UnknownCategory.NOT_LITERAL, "ProjectConfig.manifest_path is not literal"
                )
                return None, project_dir
            rec["confidence"] = weakest(rec["confidence"], spec.manifest_path.conf)
            rec["notes"].extend(spec.manifest_path.notes)
            if re.match(r"^[a-z][a-z0-9+.-]*://", mp):
                self._why(
                    rec,
                    UnknownCategory.REMOTE_MANIFEST,
                    f"manifest is remote ({mp}); not readable offline",
                )
                if not project_dir and spec.execution_project is not None:
                    pd = self._resolve_project(
                        rec,
                        spec.execution_project,
                        None,
                        ctx,
                        label="ExecutionConfig.dbt_project_path",
                    )
                    if pd:
                        rec["project_path"] = self.index.relative(pd)
                return None, project_dir
            declared = regular_file_inside(mp, self.index.workspace)
            choice = self.index.load_declared(declared) if declared else None
            if choice is None or choice.manifest is None:
                # remap a container path to the workspace project that owns it
                parent = posixpath.dirname(mp)
                pdir = (
                    posixpath.dirname(parent) if posixpath.basename(parent) == "target" else parent
                )
                pm = self.index.match(pdir)
                remapped = (
                    regular_file_inside(
                        os.path.join(pm.path, posixpath.basename(parent), posixpath.basename(mp)),
                        self.index.workspace,
                    )
                    if pm.path
                    else None
                )
                if remapped:
                    choice = self.index.load_declared(remapped)
                    rec["notes"].append(f"manifest path {mp!r} remapped: {pm.note}")
                    rec["confidence"] = weakest(rec["confidence"], "heuristic")
                    project_dir = project_dir or pm.path
                else:
                    self._why(
                        rec,
                        UnknownCategory.NO_PROJECT,
                        f"declared manifest {mp} not found in the workspace",
                    )
                    return None, project_dir
            if not project_dir:
                parent = os.path.dirname(choice.manifest.path or "")  # type: ignore[union-attr]
                project_dir = (
                    os.path.dirname(parent) if os.path.basename(parent) == "target" else parent
                )
                rec["project_path"] = self.index.relative(project_dir)
            if self.index.is_other_project(project_dir):
                rec["other_project"] = True
                return None, project_dir
            rec["manifest"] = {
                "kind": "dag_declared",
                "path": self.index.relative(choice.manifest.path),  # type: ignore[union-attr,arg-type]
                **choice.manifest.artifact_version(),
            }  # type: ignore[union-attr]
            return choice.manifest, project_dir
        if project_dir is None:
            if not rec["unknown_reasons"]:
                self._why(
                    rec,
                    UnknownCategory.NO_PROJECT,
                    "no dbt project path or manifest path found in the Cosmos config",
                )
            return None, None
        choice = self.index.manifest_for(project_dir)
        if choice.manifest is None:
            self._why(rec, UnknownCategory.NO_PROJECT, choice.reason or "no manifest")
            return None, project_dir
        if choice.kind == "stand_in" and not choice.manifest.of_type("model"):
            self._why(
                rec,
                UnknownCategory.GENERATED_PROJECT,
                "dbt project has no model files in the workspace (generated at build time?)",
            )
        rec["manifest"] = {
            "kind": choice.kind,
            "path": self.index.relative(choice.manifest.path) if choice.manifest.path else None,
            **choice.manifest.artifact_version(),
        }
        rec["notes"].extend(choice.notes)
        rec["confidence"] = weakest(rec["confidence"], choice.conf)
        if lm == "automatic":
            rec["notes"].append(
                "LoadMode.AUTOMATIC without manifest_path: Cosmos runs dbt ls at parse time; "
                "resolved over the project's manifest instead"
            )
        return choice.manifest, project_dir

    def _cosmos_select(
        self,
        rec: dict[str, Any],
        m: Manifest,
        spec: cz.CosmosSpec,
        sel: list[str],
        exc: list[str],
        selector: str | None,
    ) -> list[str] | None:
        types = ["model", "snapshot"] + ([] if spec.seed_rendering == "none" else ["seed"])
        if rec.pop("_ls_file", False):
            ids = sorted(uid for uid, n in m.nodes.items() if n.resource_type in types)
            return ids
        s = run_select(m, sel or None, exc or None, selector, resource_types=types)
        if s.unknown:
            self._why(
                rec, UnknownCategory.UNSUPPORTED_SELECTOR, f"selector not supported: {s.reason}"
            )
            return None
        rec["notes"].extend(s.notes)
        externals = [n.unique_id for n in m.nodes.values() if n.external]
        if externals:
            rec["notes"].append(
                f"{len(externals)} external (e.g. dbt-loom) nodes in the manifest "
                "are not rendered by Cosmos"
            )
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
    if (
        isinstance(v, Const)
        and isinstance(v.value, (str, int, float, bool))
        and not isinstance(v.value, bool)
    ):
        return str(v.value)
    if isinstance(v, Const) and isinstance(v.value, bool):
        return str(v.value)
    return None


def _call_name(call: ast.Call) -> str:
    f = call.func
    return f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", "")


def _root_name(func: ast.expr) -> str | None:
    while isinstance(func, ast.Attribute):
        func = func.value
    return func.id if isinstance(func, ast.Name) else None


def _clone(v: Val) -> Val:
    return copy.copy(v)


def _owner_of(kwargs: dict[str, Val]) -> str | None:
    """``owner=``, or ``default_args={"owner": ...}``: who Airflow says the task belongs to."""
    owner = as_str(kwargs.get("owner"))
    if owner:
        return owner
    default_args = kwargs.get("default_args")
    return as_str(default_args.items.get("owner")) if isinstance(default_args, DictV) else None


def _bounded_str(text: str, conf: str, notes: tuple[str, ...], line: int) -> Val:
    if len(text) > MAX_STRING:
        return Unknown(f"a string of more than {MAX_STRING} characters", line=line)
    return Const(text, conf=conf, notes=notes)


def _safe_percent(template: str) -> bool:
    """No width or precision in a %-format that would allocate memory."""
    for found in _FORMAT_WIDTH.finditer(template):
        for digits in found.groups():
            if digits and (len(digits) > 6 or int(digits) > MAX_FORMAT_WIDTH):
                return False
    return True


def _safe_format(template: str) -> bool:
    """A format string that only fills in plain fields: ``{0.__class__}`` reaches attributes, and
    ``{:>999999999}`` allocates memory."""
    try:
        fields = list(string.Formatter().parse(template))
    except ValueError:
        return False
    for _, name, spec, conversion in fields:
        if spec or conversion:
            return False
        if name and not (name.isidentifier() or name.isdigit()):
            return False
    return True


def _category_of_template(reason: str) -> UnknownCategory:
    return UnknownCategory.JINJA if "Jinja" in reason else UnknownCategory.NOT_LITERAL


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
    line, col = getattr(node, "lineno", 0), getattr(node, "col_offset", 0)
    return DagInfo(v.dag_id, v.reason, v.conf, line, col, v.owner)


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
    if fname.startswith(("airflow_dbt.", "airflow_dbt_python.")) and re.match(
        r"^Dbt\w+Operator$", last
    ):
        return "airflow_dbt_operator"
    if last == "DbtDag":
        return "cosmos_dag"
    if last == "DbtTaskGroup":
        return "cosmos_task_group"
    if cz.COSMOS_OPERATOR_RE.match(last):
        return "cosmos_operator"
    if last in SHELL_OPS:
        return SHELL_OPS[last]
    if last in SQL_OPS:
        return "sql"
    if last in POD_OPS:
        return "pod"
    if last in DOCKER_OPS:
        return "docker"
    if last.endswith(("Operator", "Sensor")):
        return "generic"
    return None


def _operators_defined_in(path: str, name: str, workspace: str) -> set[str]:
    """Operator classes constructed inside definition ``name`` of a workspace module (AST only)."""
    found: set[str] = set()
    data = read_capped(path, FILE_SIZE_CAP, workspace)
    if data is None:
        return found
    try:
        tree = ast.parse(data)
    except (SyntaxError, ValueError, RecursionError, MemoryError):
        return found
    for st in tree.body:
        if (
            isinstance(st, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            and st.name == name
        ):
            for sub in ast.walk(st):
                if isinstance(sub, ast.Call):
                    n = _call_name(sub)
                    k = _classify(n)
                    if k and k not in ("generic", "chain"):
                        found.add(n)
    return found


# ---------------------------------------------------------------------------------------------
# directory scan
# ---------------------------------------------------------------------------------------------
SKIP_SCAN_DIRS = {
    ".git",
    "__pycache__",
    "node_modules",
    ".venv",
    "venv",
    "dbt_packages",
    "target",
    "logs",
    ".tox",
}
# Folders that hold DAG files, relative to the workspace. Most repositories call theirs "dags",
# at the top or below something like "airflow".
DEFAULT_DAG_FOLDERS = ("**/dags",)
MAX_FOLDER_DEPTH = 6
MAX_IGNORE_FILE = 100_000
# A DAG folder pattern is a setting a workspace can choose, matched on every file event: enough
# for any real layout, and a bound on the work (audit LP-SEC-05)
MAX_FOLDER_PATTERN = 200
MAX_FOLDER_WILDCARDS = 6


def glob_match(relative: str, pattern: str) -> bool:
    """``PurePosixPath(relative).match(pattern)``, in linear time: one ``fnmatch`` per component,
    from the right. On Python 3.13 ``PurePath.match`` compiles a pattern such as ``*a*a*a*b`` to a
    regular expression that backtracks, taking seconds on one long name; ``fnmatch`` does not."""
    parts, wanted = PurePosixPath(relative).parts, PurePosixPath(pattern).parts
    if not wanted:
        return False
    if PurePosixPath(pattern).is_absolute():
        if len(wanted) != len(parts):
            return False
    elif len(wanted) > len(parts):
        return False
    pairs = zip(reversed(parts), reversed(wanted), strict=False)  # the path may be longer
    return all(fnmatch.fnmatchcase(p, w) for p, w in pairs)


@dataclass
class _Ignore:
    """One line of an ``.airflowignore``, which applies below its own folder. Airflow reads these
    as regular expressions before version 3 and as globs from it, and a repository does not say
    which it runs on, so a path is skipped if either reading matches. What is skipped is
    reported."""

    base: str
    regex: SafeRegex | None
    glob: str

    def matches(self, path: str) -> bool:
        if not is_inside(path, self.base) or path == self.base:
            return False
        relative = to_posix(os.path.relpath(path, self.base))
        if self.regex is not None and self.regex.search(relative):
            return True
        glob = self.glob.rstrip("/")
        if "/" in glob:
            return glob_match(relative, glob.lstrip("/"))
        return any(fnmatch.fnmatchcase(part, glob) for part in relative.split("/"))


def _airflowignore(folder: str, workspace: str, problems: list[str]) -> list[_Ignore]:
    """The patterns of the folder's ``.airflowignore``, if it is a file inside the workspace. A
    pattern is never echoed into ``problems``: the file may be a link to something private, and
    what is reported reaches logs and pasted output (audit LP-SEC-01)."""
    path = os.path.join(folder, ".airflowignore")
    data = read_capped(path, MAX_IGNORE_FILE, workspace)
    if data is None:
        return []
    where = to_posix(os.path.relpath(path, workspace))
    found: list[_Ignore] = []
    for number, line in enumerate(data.decode("utf-8", "replace").splitlines(), 1):
        line = line.split("#", 1)[0].strip()
        if not line or line.startswith("!"):
            continue
        try:
            regex: SafeRegex | None = SafeRegex(line)
        except UnsafePattern:
            regex = None
            problems.append(
                f"{where} line {number} is not a safe regular expression; it is only read as a glob"
            )
        found.append(_Ignore(folder, regex, line))
    return found


def resolve_dag_folders(workspace: str, patterns: Iterable[str]) -> list[str]:
    """The folders that exist for these patterns: a plain path (a single file will do), or a
    pattern with ``*``, which is looked for up to a few levels down. Each is a real path inside
    the workspace: a folder that is a link out of it is not followed."""
    workspace = os.path.realpath(workspace)
    found: list[str] = []
    wanted = [p for p in (clean_dag_folder(p) for p in patterns) if p]
    for pattern in wanted:
        if "*" not in pattern:
            path = resolve_inside(os.path.join(workspace, pattern), workspace)
            if path is not None and (os.path.isdir(path) or is_regular_file(path)):
                found.append(path)
            continue
        target = pattern[3:] if pattern.startswith("**/") else pattern
        for dirpath, dirnames, _ in walk_inside(workspace, workspace):
            depth = (
                0
                if dirpath == workspace
                else to_posix(os.path.relpath(dirpath, workspace)).count("/") + 1
            )
            dirnames[:] = (
                sorted(
                    d
                    for d in dirnames
                    if d not in SKIP_SCAN_DIRS
                    and not d.startswith(".")
                    and not _is_dbt_project(os.path.join(dirpath, d), workspace)
                )
                if depth < MAX_FOLDER_DEPTH
                else []
            )
            for d in list(dirnames):
                relative = to_posix(os.path.relpath(os.path.join(dirpath, d), workspace))
                if glob_match(relative, target):
                    # A link to a folder is resolved here, and left out if it leads elsewhere
                    real = resolve_inside(os.path.join(dirpath, d), workspace)
                    if real is not None:
                        found.append(real)
                    dirnames.remove(d)  # what is below it is scanned as part of it
    return sorted(set(found))


def _is_dbt_project(folder: str, workspace: str) -> bool:
    marker = os.path.join(folder, "dbt_project.yml")
    return os.path.lexists(marker) and regular_file_inside(marker, workspace) is not None


def clean_dag_folder(folder: Any) -> str | None:
    """A relative folder or pattern, or None: a setting cannot point outside the workspace, and
    a pattern is bounded in length and wildcards."""
    if not isinstance(folder, str):
        return None
    if folder.strip() in (".", "./"):
        return "."
    if len(folder) > MAX_FOLDER_PATTERN or (
        folder.count("*") + folder.count("?") > MAX_FOLDER_WILDCARDS
    ):
        return None
    posix = PurePosixPath(folder.strip().replace("\\", "/"))
    if not posix.parts or posix.is_absolute() or ".." in posix.parts or ":" in posix.parts[0]:
        return None
    return posix.as_posix().rstrip("/")


def in_dag_folder(relative: str, folders: Iterable[str]) -> bool:
    """Whether a file (relative to the workspace) is below one of these folders or patterns."""
    parents = PurePosixPath(relative).parents
    for folder in folders:
        if "*" not in folder:
            if relative.startswith(f"{folder}/"):
                return True
            continue
        target = folder[3:] if folder.startswith("**/") else folder
        if any(p.parts and glob_match(p.as_posix(), target) for p in parents):
            return True
    return False


def discover_dag_files(
    root: str, safe_mode: bool = True, workspace: str | None = None
) -> tuple[list[str], list[dict[str, str]], list[str]]:
    """Python files Airflow would consider: its safe-mode heuristic, ``.airflowignore``, and no
    dbt project folders, whose Python models are not DAGs. Nothing outside ``workspace`` (by
    default ``root``) is read or listed."""
    files: list[str] = []
    skipped: list[dict[str, str]] = []
    problems: list[str] = []
    root = os.path.realpath(root)
    space = os.path.realpath(workspace) if workspace else root
    if os.path.isfile(root):
        return [root], [], []
    ignore: list[_Ignore] = []
    for dirpath, dirnames, filenames in walk_inside(root, space):
        ignore.extend(_airflowignore(dirpath, space, problems))
        keep = []
        for d in sorted(dirnames):
            full = os.path.join(dirpath, d)
            if d in SKIP_SCAN_DIRS or d.startswith("."):
                continue
            if _is_dbt_project(full, space):
                skipped.append(
                    {"path": to_posix(os.path.relpath(full, root)), "why": "dbt project directory"}
                )
                continue
            if any(i.matches(full) for i in ignore):
                skipped.append(
                    {"path": to_posix(os.path.relpath(full, root)), "why": ".airflowignore"}
                )
                continue
            keep.append(d)
        dirnames[:] = keep
        for fn in sorted(filenames):
            if not fn.endswith(".py"):
                continue
            full = os.path.join(dirpath, fn)
            relative = to_posix(os.path.relpath(full, root))
            if any(i.matches(full) for i in ignore):
                skipped.append({"path": relative, "why": ".airflowignore"})
                continue
            if safe_mode:
                head = read_capped(full, FILE_SIZE_CAP, space)
                if head is None:
                    # Over the cap, or not a regular file: FileScanner says which
                    files.append(full)
                    continue
                head = head.lower()
                if b"dag" not in head or b"airflow" not in head:
                    skipped.append(
                        {
                            "path": relative,
                            "why": "Airflow safe-mode heuristic (no 'dag' and 'airflow')",
                        }
                    )
                    continue
            files.append(full)
    return files, skipped, problems


def scan_dags(
    workspace: str,
    dag_folders: Iterable[str] = DEFAULT_DAG_FOLDERS,
    manifests: dict[str, str] | None = None,
    path_map: dict[str, str] | None = None,
    focus_project: str | None = None,
    safe_mode: bool = True,
) -> dict[str, Any]:
    """Read the DAG files under ``dag_folders``, as JSON with nothing about the machine in it.

    Args:
        workspace: the folder DAG files and imported modules must be inside
        dag_folders: folders or patterns relative to the workspace (``resolve_dag_folders``)
        manifests: a project's root to its manifest, when it is not ``target/manifest.json``
        path_map: an absolute container path to an absolute folder in the workspace
        focus_project: the one project wanted; links to others are counted, not resolved
    """
    ws = os.path.realpath(workspace)
    patterns = list(dag_folders)
    index = ProjectIndex(ws, manifests, path_map, focus_project)
    loader = ModuleLoader(ws, index)
    folders = resolve_dag_folders(ws, patterns)
    files: list[str] = []
    skipped: list[dict[str, str]] = []
    problems: list[str] = []
    for folder in folders:
        found, skips, ignored = discover_dag_files(folder, safe_mode, ws)
        files.extend(found)
        prefix = "" if folder == ws else f"{index.relative(folder)}/"
        skipped.extend({**s, "path": f"{prefix}{s['path']}"} for s in skips)
        problems.extend(ignored)
    ordered = sorted(set(files))
    if len(ordered) > MAX_DAG_FILES:
        problems.append(f"{len(ordered)} DAG files found; only the first {MAX_DAG_FILES} were read")
        ordered = ordered[:MAX_DAG_FILES]
    out: dict[str, Any] = {
        "scanner": {"origin": ORIGIN, "executes_user_code": False},
        "patterns": patterns,
        "folders": [index.relative(f) for f in folders],
        "dbt_projects": [index.relative(p) for p in index.projects],
        "files": {},
        "skipped": sorted(skipped, key=lambda s: s["path"]),
        "problems": problems,
        "dags": [],
        "tasks": [],
        "links": [],
        "sql": [],
        "dependencies": [],
        "unresolved": [],
        "other_project_links": 0,
    }
    for f in ordered:
        fs = FileScanner(f, index, loader, emit=True)
        try:
            fs.run()
            _read_sql(fs, index)
        except Exception as exc:  # a bug here must cost one file, and say so
            fs._unresolved(
                0,
                0,
                f"the scanner failed on this file: {type(exc).__name__}",
                Ctx(),
                force=True,
                category=UnknownCategory.DAG_NOT_LOADABLE,
                mentions_dbt=True,
            )
        out["files"][fs.rel] = fs.sha256
        out["dags"].extend(fs.dags)
        out["tasks"].extend(fs.tasks)
        out["links"].extend(fs.links)
        out["sql"].extend(fs.sql)
        out["dependencies"].extend(fs.dependencies)
        out["unresolved"].extend(fs.unresolved)
        out["other_project_links"] += fs.other_project_links
    # Choosing a manifest happens as files need one, and can add to these
    out["problems"] = sorted(index.problems) + problems
    out["stats"] = {
        "files_scanned": len(out["files"]),
        "files_skipped": sum(1 for x in skipped if x["why"] != "dbt project directory"),
        "dbt_project_dirs_skipped": sum(1 for x in skipped if x["why"] == "dbt project directory"),
        "dags": len(out["dags"]),
        "links": len(out["links"]),
        "links_resolved": sum(1 for x in out["links"] if x["status"] == "resolved"),
        "links_unknown": sum(1 for x in out["links"] if x["status"] == "unknown"),
        "sql_tasks": len(out["sql"]),
        "dependencies": len(out["dependencies"]),
        "unresolved": len(out["unresolved"]),
    }
    return _scrub(out, to_posix(ws))


def _scrub(value: Any, workspace: str) -> Any:
    """Where the workspace is on this machine is no part of the result: ``Path(__file__)`` values
    reach the notes and reasons."""
    if isinstance(value, str):
        return value.replace(workspace, "<workspace>") if workspace in value else value
    if isinstance(value, list):
        return [_scrub(v, workspace) for v in value]
    if isinstance(value, dict):
        return {k: _scrub(v, workspace) for k, v in value.items()}
    return value


def _read_sql(fs: FileScanner, index: ProjectIndex) -> None:
    """Parse the literal SQL of the file's SQL operators with sqlglot, in the project's dialect."""
    for rec in fs.sql:
        statements = rec.pop("statements", [])
        reads: list[list[str | None]] = []
        writes: list[list[str | None]] = []
        dialect = _dialect(index)
        for sql in statements:
            found = _tables_of(sql, dialect)
            if isinstance(found, str):
                rec["status"], rec["reason"] = "unknown", found
                break
            reads.extend(found[0])
            writes.extend(found[1])
        else:
            rec["status"] = "resolved"
        rec["reads"] = sorted({tuple(r): r for r in reads}.values(), key=str)
        rec["writes"] = sorted({tuple(r): r for r in writes}.values(), key=str)
        if rec["status"] == "unknown":
            fs._unresolved(
                rec["location"]["line"],
                rec["location"]["col"],
                rec["reason"],
                Ctx(emit=True),
                dag_id=rec["dag_id"],
                task_id=rec["task_id"],
                category=UnknownCategory.SQL,
                mentions_dbt=False,
            )


def _dialect(index: ProjectIndex) -> str | None:
    project = index.focus or index.only_project()
    if project is None:
        return None
    manifest = index.manifest_for(project).manifest
    adapter = manifest.metadata.get("adapter_type") if manifest is not None else None
    return adapter if isinstance(adapter, str) else None


def _tables_of(
    sql: str, dialect: str | None
) -> tuple[list[list[str | None]], list[list[str | None]]] | str:
    """The tables a statement reads and writes, each ``[database, schema, table]``; or why not."""
    if _JINJA_BLOCK.search(sql):
        return "the SQL has a Jinja block, so its statements are only known at run time"
    text = _JINJA_EXPRESSION.sub(_JINJA_PLACEHOLDER, sql)
    reads: list[list[str | None]] = []
    writes: list[list[str | None]] = []
    try:
        trees = [t for t in parse(text, dialect=dialect) if t is not None]
    except (SqlglotError, ValueError, RecursionError, MemoryError) as e:
        return f"the SQL cannot be parsed: {type(e).__name__}"
    if not trees:
        return "the SQL is empty"
    for tree in trees:
        if not isinstance(
            tree,
            (
                exp.Select,
                exp.Union,
                exp.Insert,
                exp.Update,
                exp.Merge,
                exp.Delete,
                exp.Create,
                exp.Drop,
            ),
        ):
            tree_tables = list(tree.find_all(exp.Table))
            if not tree_tables:
                continue
        parsed = parse_sql(tree.sql(dialect=dialect), dialect=dialect or "")
        if not parsed.is_valid:
            return "the SQL cannot be parsed"
        for side, out in ((parsed.reads, reads), (parsed.writes, writes)):
            for ref in side:
                if _JINJA_PLACEHOLDER in ref.qualified_name:
                    shown = ref.qualified_name.replace(_JINJA_PLACEHOLDER, "{{ ... }}")
                    return f"a table name is a template: {shown}"
                out.append([ref.database, ref.schema, ref.table])
    return reads, writes
