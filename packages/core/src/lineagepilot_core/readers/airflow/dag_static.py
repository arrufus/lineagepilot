"""AST-only scanner for Airflow DAG files (task 5.1, design section 6.2).

DAG files are untrusted workspace input. They are read with the shared safe evaluator
(``readers.common.evaluator``, task 8.4): as bytes, with a size cap, parsed with
:func:`ast.parse`, and never imported, executed or ``eval``-ed. Whatever it cannot evaluate becomes
an ``Unknown`` value, and links that depend on it become ``unknown`` records. This module says what
Airflow's calls mean: DAGs, operators, task groups, dependencies and the dbt they run.

The result is plain JSON, with nothing about the machine in it, so the language server can read it
from a child process and reviews stay comparable.
"""

from __future__ import annotations

import ast
import fnmatch
import os
import posixpath
import re
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Any

from sqlglot import exp, parse
from sqlglot.errors import SqlglotError

from lineagepilot_core.parsers.sql_parser import parse_sql
from lineagepilot_core.readers.airflow import cosmos as cz
from lineagepilot_core.readers.common import evaluator
from lineagepilot_core.readers.common.categories import UnknownCategory, most_specific
from lineagepilot_core.readers.common.evaluator import (
    FILE_SIZE_CAP,
    MAX_STEPS,
    Evaluator,
    Return,
    Scope,
    all_args,
    call_name,
    clone,
    scrub,
    to_str,
)
from lineagepilot_core.readers.common.folders import (
    SKIP_SCAN_DIRS,
    glob_match,
)

# The DAG scan's names for the shared folder helpers (task 8.2), which the reader and the
# hardening tests import from here
from lineagepilot_core.readers.common.folders import clean_folder as clean_dag_folder  # noqa: F401
from lineagepilot_core.readers.common.folders import in_folders as in_dag_folder  # noqa: F401
from lineagepilot_core.readers.common.folders import is_dbt_project as _is_dbt_project
from lineagepilot_core.readers.common.folders import resolve_folders as resolve_dag_folders
from lineagepilot_core.readers.common.manifest import (
    Manifest,
    ManifestError,
    load_dbt_ls_file,
    manifest_from_dict,
)
from lineagepilot_core.readers.common.projects import ManifestChoice, ProjectIndex
from lineagepilot_core.readers.common.safe_io import (
    env_tail,
    is_absolute,
    is_inside,
    read_capped,
    regular_file_inside,
    to_posix,
    walk_inside,
)
from lineagepilot_core.readers.common.selector import select as run_select
from lineagepilot_core.readers.common.shell import (
    BUILDING_COMMANDS,
    DbtInvocation,
    find_dbt_in_argv,
    find_dbt_in_shell,
    parse_dbt_argv,
    runs_a_variable,
)
from lineagepilot_core.readers.common.values import (
    CallV,
    Const,
    DagV,
    DictV,
    EnvPath,
    FuncV,
    ListV,
    PartialV,
    TaskV,
    Unknown,
    Val,
    as_str,
    as_str_list,
    collect_meta,
    text_of,
    weakest,
)
from lineagepilot_core.rules.safe_regex import SafeRegex, UnsafePattern

ORIGIN = "dag_static"
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
POD_OPS = {"KubernetesPodOperator", "GKEStartPodOperator", "EksPodOperator", "AKSPodOperator"}
DOCKER_OPS = {"DockerOperator", "DockerSwarmOperator"}
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
class Ctx(evaluator.Ctx):
    """Where the code being evaluated is, with the DAG and the task groups it is in."""

    dag: DagInfo | None = None
    group_prefix: tuple[str, ...] = ()

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


@dataclass(frozen=True)
class Container:
    """dbt runs in a container, so the paths it is given are the container's (LP-c05).
    ``mounts`` are its bind mounts that could be read: (the path in the container, the host
    folder)."""

    mounts: tuple[tuple[str, str], ...] = ()


# ---------------------------------------------------------------------------------------------
# workspace module loader (cross-module constants)
# ---------------------------------------------------------------------------------------------
class ModuleLoader(evaluator.ModuleLoader):
    """The shared loader, reading modules with the DAG scan, and the operators a helper makes."""

    def __init__(self, workspace: str, index: ProjectIndex):
        super().__init__(workspace, index, FileScanner)
        self._operators: dict[tuple[str, str], set[str]] = {}

    def operators_in(self, path: str, name: str) -> set[str]:
        key = (path, name)
        if key not in self._operators:
            self._operators[key] = _operators_defined_in(path, name, self.workspace)
        return self._operators[key]


# ---------------------------------------------------------------------------------------------
# the scanner
# ---------------------------------------------------------------------------------------------
class FileScanner(Evaluator):
    """One DAG file: the shared evaluator (task 8.4), and what Airflow's calls mean."""

    no_import = NO_IMPORT_TOP
    library_prefixes = KNOWN_LIBRARY_PREFIXES
    self_running = ("task", "dag")
    not_parsed = UnknownCategory.DAG_NOT_LOADABLE
    made_in_a_loop = "tasks created a dynamic number of times"
    ctx_type = Ctx
    loader: ModuleLoader

    def __init__(
        self,
        path: str,
        index: ProjectIndex,
        loader: ModuleLoader,
        emit: bool = True,
        import_depth: int = 0,
    ):
        super().__init__(path, index, loader, emit, import_depth)
        self.links: list[dict[str, Any]] = []
        self.sql: list[dict[str, Any]] = []
        self.other_project_links = 0
        self.reads_manifest = False  # the file reads manifest.json at parse time
        self.dependencies: list[dict[str, Any]] = []
        self.dags: list[dict[str, Any]] = []
        self.tasks: list[dict[str, Any]] = []

    # This module's budgets, which the hardening tests set
    @property
    def max_steps(self) -> int:
        return MAX_STEPS

    @property
    def file_size_cap(self) -> int:
        return FILE_SIZE_CAP

    def _dynamic_category(self) -> UnknownCategory:
        if self.reads_manifest:
            return UnknownCategory.MANIFEST_GENERATED
        return UnknownCategory.LOOP_OR_FACTORY

    def _why(self, rec: dict[str, Any], category: UnknownCategory, reason: str) -> None:
        rec["unknown_reasons"].append(reason)
        rec["categories"].append(category.value)

    # --- the shared evaluator's hooks ---------------------------------------------------------
    def _defined(self, fv: FuncV, scope: Scope, ctx: Ctx) -> None:  # type: ignore[override]
        st, kind, dkw = fv.node, fv.kind, fv.decorator_kwargs
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
            for a in all_args(st.args):
                inner.vars[a] = Unknown(
                    f"parameter {a!r} of @dag function {st.name!r} (DAG params are runtime values)"
                )
            try:
                self.exec_block(
                    st.body,
                    inner,
                    replace(ctx, dag=info, group_prefix=(), funcs=ctx.funcs + (st.name,)),
                )
            except Return:
                pass
        elif kind == "plain" and not ctx.funcs:
            self._subprocess_heuristic(st, st.name, ctx)

    def _enter(self, v: Val, node: ast.expr, ctx: Ctx) -> Ctx:  # type: ignore[override]
        new_ctx = ctx
        if isinstance(v, DagV):
            new_ctx = replace(new_ctx, dag=_dag_info(v, node), group_prefix=())
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
        return new_ctx

    def _operator(  # type: ignore[override]
        self, op: ast.operator, left: Val, right: Val, node: ast.AST, ctx: Ctx
    ) -> Val | None:
        if not isinstance(op, (ast.RShift, ast.LShift)):
            return None
        line = getattr(node, "lineno", 0)
        col = getattr(node, "col_offset", 0)
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

    def _tool_attribute(self, base: Val, attr: str, node: ast.Attribute) -> Val | None:
        if isinstance(base, TaskV) and attr == "output":
            return base
        return None

    def _tool_method(  # type: ignore[override]
        self,
        base: Val,
        meth: str,
        args: list[Val],
        kwargs: dict[str, Val],
        node: ast.Call,
        scope: Scope,
        ctx: Ctx,
    ) -> Val | None:
        line = node.lineno
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
        return None

    def _tool_call(  # type: ignore[override]
        self,
        fname: str,
        node: ast.Call,
        args: list[Val],
        kwargs: dict[str, Val],
        has_star: bool,
        scope: Scope,
        ctx: Ctx,
    ) -> Val | None:
        line, col = node.lineno, node.col_offset
        last = fname.rsplit(".", 1)[-1]
        # Airflow Variables: use the default, flagged heuristic
        if fname.endswith("Variable.get"):
            key = as_str(args[0]) if args else None
            default = (
                kwargs.get("default_var")
                or kwargs.get("default")
                or (args[1] if len(args) > 1 else None)
            )
            if default is None or (isinstance(default, Const) and default.value is None):
                return Unknown(f"Airflow Variable {key!r} has no default", line=line)
            return clone(default).with_meta(
                "heuristic", (f"Airflow Variable {key!r} assumed unset; used default_var",)
            )
        if last == "partial" and fname.count("."):
            owner = fname.rsplit(".", 1)[0]
            if _classify(owner) is not None:
                return PartialV(owner, kwargs, line=line, col=col)
        kind = _classify(fname)
        if kind is not None:
            return self._create_task(fname, node, args, kwargs, has_star, scope, ctx)
        if last in ("open", "load", "loads", "read_text", "load_manifest") and any(
            "manifest.json" in (to_str(a) or "") for a in args
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
        return None

    def _skipped(self, body: list[ast.stmt], test: ast.expr, scope: Scope, ctx: Ctx) -> None:
        """A branch not scanned because a guess decided its test: an operator in it that
        mentions dbt is reported at its line, never left out in silence. It may well run."""
        if not ctx.emit:
            return
        for stmt in body:
            for sub in ast.walk(stmt):
                if not isinstance(sub, ast.Call):
                    continue
                name = self._dotted_name(sub.func, scope) or ""
                kind = _classify(name)
                if kind is None or kind in ("dag", "task_group", "chain"):
                    continue
                runs_dbt = kind.startswith("cosmos") or kind == "airflow_dbt_operator"
                if not runs_dbt and not _DBT_NAME.search(self._segment(sub)):
                    continue
                self._unresolved(
                    sub.lineno,
                    sub.col_offset,
                    f"{name.rsplit('.', 1)[-1]} is created only if the test at line "
                    f"{test.lineno} goes the other way; that test was decided by an environment "
                    "variable's default, so this was not scanned",
                    ctx,
                    dag_id=ctx.dag.dag_id if ctx.dag else None,
                    category=UnknownCategory.NOT_LITERAL,
                    mentions_dbt=True,
                )

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
                    for n in (call_name(c) for c in ast.walk(fv.node) if isinstance(c, ast.Call))
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
                for n in (call_name(c) for c in ast.walk(fv.node) if isinstance(c, ast.Call))
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
            self._argv_task(kind, handle, node, ctx, kwargs, tid_v, container=Container())
        elif kind == "docker":
            cmd = kwargs.get("command")
            box = _docker_container(kwargs)
            if isinstance(cmd, ListV):
                self._argv_task(kind, handle, node, ctx, {"cmds": cmd}, tid_v, container=box)
            else:
                self._shell_task(kind, handle, cmd, node, ctx, kwargs, tid_v, container=box)
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
            rec["constructed_at"] = self._here(node.lineno, node.col_offset)  # type: ignore[attr-defined]
            rec["location"] = self._site_loc(*ctx.call_sites[0])
            rec["via_call_at"] = [list(cs) for cs in ctx.call_sites]
        if self._foreign is not None:
            rec["notes"].append(
                f"built by {self._foreign.name}() in {self._foreign.rel}, a helper imported from "
                "another workspace file and followed one level"
            )
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
                at_site=True,
                mentions_dbt=True,
            )

    def _resolve_project(
        self,
        rec: dict[str, Any],
        raw: Val | None,
        base_dir: str | None,
        ctx: Ctx,
        label: str = "project_dir",
        container: Container | None = None,
    ) -> str | None:
        """Turn a project path value into a workspace dbt project directory.

        ``container`` says dbt runs in a container, so a path is the container's: one that
        matches no project goes through the container's bind mounts, and then, with exactly one
        dbt project in the workspace, to that project (LP-c05). Both are ``heuristic``.
        """
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
                if container is not None:
                    mounted = self._through_mount(rec, base_dir, container)
                    if mounted:
                        return mounted
                rec["notes"].append(f"the working directory {base_dir} is not known: {m.reason}")
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
        if s is None and isinstance(raw, EnvPath) and env_tail(raw.text) is not None:
            s = raw.text
            rec["notes"].append(
                f"{label} is built on the environment variable(s) {', '.join(raw.names)}, "
                "which have no default in the DAG file: the project is matched by the path after "
                "the variable"
            )
            raw = Const(s, conf="heuristic")
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
        if m.path is None and container is not None:
            path = s
            if base_dir and not is_absolute(s) and env_tail(s) is None:
                path = posixpath.normpath(posixpath.join(base_dir, s))
            found = self._through_mount(rec, path, container) or self._only_in_container(
                rec, s, label
            )
            if found:
                return found
        if m.path is None:
            self._why(rec, UnknownCategory.NO_PROJECT, m.reason or f"{label} {s!r} not found")
            return None
        if m.note:
            rec["notes"].append(m.note)
        rec["confidence"] = weakest(rec["confidence"], m.conf)
        return m.path

    def _through_mount(self, rec: dict[str, Any], path: str, container: Container) -> str | None:
        """A path in the container, through the bind mount whose target holds it: the longest
        one decides. The host folder is matched as any project path is (LP-c05)."""
        for target, source in sorted(container.mounts, key=lambda m: -len(m[0])):
            root = target.rstrip("/")
            if path != root and not path.startswith(f"{root}/"):
                continue
            rest = path[len(root) :].strip("/")
            host = posixpath.join(source, rest) if rest else source
            m = self.index.match(host)
            if m.path is None:
                return None
            rec["notes"].append(
                f"container path {path!r} is mounted from the host folder {host!r}"
                + (f"; {m.note}" if m.note else "")
            )
            rec["confidence"] = weakest(rec["confidence"], "heuristic")
            return m.path
        return None

    def _only_in_container(self, rec: dict[str, Any], raw: str, label: str) -> str | None:
        """A container path that names no project, and no mount: the workspace's only project,
        if it has exactly one (LP-c05). With two or more, it stays unknown."""
        only = self.index.only_project()
        if only is None:
            return None
        rec["notes"].append(
            f"{label} {raw!r} is a path in the container, and neither it nor a mount names a dbt "
            "project here; assumed the only dbt project in the workspace"
        )
        rec["confidence"] = weakest(rec["confidence"], "heuristic")
        return only

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
        container: Container | None = None,
    ) -> None:
        if inv.remote and container is None:
            container = Container()  # `docker run ... dbt run`: its paths are the container's
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
            if cwd_s is None and isinstance(task_cwd, EnvPath) and env_tail(task_cwd.text):
                cwd_s = task_cwd.text  # matched by the path after the variable (LP-0o1)
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
        proj = self._resolve_project(rec, project_raw, base_dir, ctx, container=container)
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
        container: Container | None = None,
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
            env_src = (
                c
                if text is None and isinstance(c, EnvPath) and not runs_a_variable(c.text)
                else None
            )
            if env_src is not None:
                # Parsed as a shell would see it: ${NAME} is unknown in a selector, and a path
                # after it can still name the project (LP-0o1)
                text = env_src.text
                c = Const(
                    text,
                    notes=(
                        f"the command is built on the environment variable(s) "
                        f"{', '.join(c.names)}, which have no default in the DAG file; "
                        "read as ${NAME}",
                    ),
                )
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
            _explain_env(invs, env_src)
            if env_src is not None and not invs:
                # No dbt in what is known, and the variable may hold it: as unknown as before
                self._unresolved(
                    node.lineno,
                    node.col_offset,
                    f"shell command not statically resolvable: {env_src.reason}",
                    ctx,
                    dag_id=handle.dag_id,
                    task_id=handle.task_id,  # type: ignore[attr-defined]
                    category=UnknownCategory.NOT_LITERAL,
                    mentions_dbt="dbt" in self._segment(node).lower(),
                )
                continue
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
                    # A container runs its image's entrypoint, which may be dbt (`run --target
                    # dev`): the whole call says whether it is about dbt, as for an argv
                    self._no_dbt_command(
                        handle,
                        node,
                        ctx,
                        f"{self._segment(node)} {text}" if kind == "docker" else text,
                    )
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
                    container=container,
                )

    def _argv_task(
        self,
        kind: str,
        handle: TaskHandle,
        node: ast.AST,
        ctx: Ctx,
        kwargs: dict[str, Val],
        tid_v: Val | None,
        container: Container | None = None,
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
            lst = _argv_texts(v)
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
        _explain_env(
            invs,
            *(
                item
                for v in (cmds, arguments)
                for item in (v.items if isinstance(v, ListV) else [v])
            ),
        )
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
                container=container,
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
        # Airflow finds a template next to the DAG file, wherever the operator was built
        path = os.path.join(os.path.dirname(self.home), item.strip())
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
        elif isinstance(pd, EnvPath) and env_tail(pd.text) is not None:
            inv.project_dir = pd.text  # matched by the path after the variable (LP-0o1)
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
            from_env = (
                mp is None
                and isinstance(spec.manifest_path, EnvPath)
                and env_tail(spec.manifest_path.text) is not None
            )
            if from_env:
                # ${AIRFLOW_HOME}/dags/dbt/x/target/manifest.json: only the owning project can be
                # matched, by the path after the variable (LP-0o1)
                mp = spec.manifest_path.text  # type: ignore[union-attr]
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
            declared = None if from_env else regular_file_inside(mp, self.index.workspace)
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
                    if not project_dir:
                        project_dir = pm.path
                        rec["project_path"] = self.index.relative(pm.path)  # type: ignore[arg-type]
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


_HOST_PATH = ("/", "./", "../", "~", "$")


def _docker_container(kwargs: dict[str, Val]) -> Container:
    """A DockerOperator's bind mounts: ``mounts=[Mount(target=..., source=..., type="bind")]``,
    and the older ``volumes=["/host/path:/container/path:ro"]``. A volume, a mount whose type is
    not ``"bind"``, and anything not literal are left out: they name no host folder."""
    found: list[tuple[str, str]] = []
    mounts = kwargs.get("mounts")
    for item in mounts.items if isinstance(mounts, ListV) else []:
        if not (isinstance(item, CallV) and item.func.rsplit(".", 1)[-1] == "Mount"):
            continue
        target = as_str(item.arg("target", 0))
        source = text_of(item.arg("source", 1))
        if target and source and as_str(item.arg("type", 2)) == "bind" and is_absolute(target):
            found.append((target, source))
    volumes = kwargs.get("volumes")
    for item in volumes.items if isinstance(volumes, ListV) else []:
        spec = text_of(item)
        if spec is None or spec.count(":") not in (1, 2):
            continue
        source, target = spec.split(":")[:2]
        if source.startswith(_HOST_PATH) and is_absolute(target):
            found.append((target, source))
    return Container(tuple(found))


def _explain_env(invs: list[DbtInvocation], *sources: Val | None) -> None:
    """A field left unknown by ``${NAME}`` says where the variable came from in the DAG file."""
    env = [s for s in sources if isinstance(s, EnvPath)]
    if not env:
        return
    for inv in invs:
        for key, reason in list(inv.unknown_fields.items()):
            if "$" in reason:
                inv.unknown_fields[key] = f"{reason}: {env[0].reason}"


def _argv_texts(v: Val | None) -> list[str] | None:
    """A container's ``cmds`` or ``arguments`` as strings, an environment variable with no
    default written as ``${NAME}``; None when a part is unknown in any other way."""
    if isinstance(v, EnvPath):
        return [v.text]
    if isinstance(v, ListV):
        out: list[str] = []
        for item in v.items:
            text = item.text if isinstance(item, EnvPath) else as_str(item)
            if text is None:
                return None
            out.append(text)
        return out
    return as_str_list(v)


def _owner_of(kwargs: dict[str, Val]) -> str | None:
    """``owner=``, or ``default_args={"owner": ...}``: who Airflow says the task belongs to."""
    owner = as_str(kwargs.get("owner"))
    if owner:
        return owner
    default_args = kwargs.get("default_args")
    return as_str(default_args.items.get("owner")) if isinstance(default_args, DictV) else None


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
                    n = call_name(sub)
                    k = _classify(n)
                    if k and k not in ("generic", "chain"):
                        found.add(n)
    return found


# ---------------------------------------------------------------------------------------------
# directory scan
# ---------------------------------------------------------------------------------------------
# Folders that hold DAG files, relative to the workspace. Most repositories call theirs "dags",
# at the top or below something like "airflow".
DEFAULT_DAG_FOLDERS = ("**/dags",)
MAX_IGNORE_FILE = 100_000


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
    return scrub(out, to_posix(ws))


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
