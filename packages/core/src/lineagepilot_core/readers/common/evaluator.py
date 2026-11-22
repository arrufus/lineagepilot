"""A safe evaluator for Python files, shared by every reader of Python (design_next_phase §8.1).

It began as the Airflow DAG scan's (task 5.1) and moved here in task 8.4, unchanged, so the Dagster
reader reads its definitions the same way. Python files are untrusted workspace input. They are
read as bytes, with a size cap, and parsed with :func:`ast.parse`. Nothing is imported, executed or
``eval``-ed. Expressions are evaluated by a small abstract interpreter over the AST (see
``values``). Whatever it cannot evaluate becomes an ``Unknown`` value, and what a reader builds on
it becomes an ``unknown`` record.

A crafted file gets no more out of this than a wrong answer: strings and lists it builds are
capped, each file has a budget of evaluation steps, and format strings cannot reach attributes.

A reader subclasses :class:`Evaluator` and fills in its hooks: what a call, a method, an attribute,
a decorator or a ``with`` block means for its tool. The hooks do nothing here.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import os
import posixpath
import re
import string
from collections.abc import Container, Iterable
from dataclasses import dataclass, replace
from typing import Any, ClassVar

from lineagepilot_core.readers.common.categories import UnknownCategory
from lineagepilot_core.readers.common.projects import ProjectIndex
from lineagepilot_core.readers.common.safe_io import (
    env_tail,
    is_absolute,
    is_inside,
    read_capped,
    regular_file_inside,
    resolve_inside,
    to_posix,
)
from lineagepilot_core.readers.common.shell import mentions_dbt
from lineagepilot_core.readers.common.values import (
    CallV,
    Const,
    DictV,
    EnvPath,
    FuncV,
    ListV,
    PathV,
    Sym,
    Unknown,
    Val,
    as_str,
    collect_meta,
    merge_values,
    plain,
    text_of,
    weakest,
)

FILE_SIZE_CAP = 2 * 1024 * 1024
MAX_CALL_DEPTH = 4
MAX_IMPORT_DEPTH = 3
# What one file can make the evaluator do. Real DAG files stay in the thousands of steps.
MAX_STEPS = 500_000
MAX_STRING = 100_000
MAX_ITEMS = 10_000
# Beyond this, a width in a format string is an attempt to allocate memory
MAX_FORMAT_WIDTH = 1000
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
# Python that runs dbt from its own code, which no reader follows
SUBPROCESS_FUNCS = (
    "subprocess.run",
    "subprocess.call",
    "subprocess.check_call",
    "subprocess.check_output",
    "subprocess.Popen",
    "os.system",
    "os.popen",
)
_FORMAT_WIDTH = re.compile(r"%[-#0 +]*(\d+)?(?:\.(\d+))?")
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass(frozen=True)
class Ctx:
    """Where the code being evaluated is. A reader adds what its tool needs, such as the DAG."""

    dynamic: str | None = (
        None  # reason the code may run 0..n times (loop, comprehension, uncalled function)
    )
    branch: bool = False
    funcs: tuple[str, ...] = ()
    call_sites: tuple[tuple[int, int], ...] = ()
    emit: bool = True


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


class Return(Exception):
    pass


class OverBudget(Exception):
    """This file has used its evaluation steps."""


@dataclass(frozen=True)
class Foreign:
    """A helper from another workspace file, whose body is running (LP-z2x)."""

    rel: str  # its file, relative to the workspace
    name: str  # the function
    site: tuple[int, int]  # the call in the file scanned


class ModuleLoader:
    """Resolve ``from include.constants import X`` to another workspace file, read as AST only,
    by the reader's own evaluator, ``scanner``."""

    def __init__(self, workspace: str, index: ProjectIndex, scanner: type[Evaluator]):
        self.workspace = os.path.realpath(workspace)
        self.index = index
        self.scanner = scanner
        self.cache: dict[str, Evaluator | None] = {}
        self._find_cache: dict[tuple[str, str, int], str | None] = {}
        # The imported names being looked up now, by module file: one met again is a cycle
        self.resolving: set[tuple[str, str | None]] = set()

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
        if level == 0 and top in self.scanner.no_import:
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

    def dotted(self, path: str) -> str:
        """A workspace file's module name from the workspace's root, as ``find`` takes it."""
        rel = to_posix(os.path.relpath(path, self.workspace))
        for suffix in ("/__init__.py", ".py"):
            if rel.endswith(suffix):
                rel = rel[: -len(suffix)]
                break
        return rel.replace("/", ".")

    def module(self, path: str, depth: int) -> Evaluator | None:
        if path in self.cache:
            return self.cache[path]
        self.cache[path] = None  # cycle guard
        if depth > MAX_IMPORT_DEPTH:
            return None
        fs = self.scanner(path, self.index, self, emit=False, import_depth=depth)
        ok = fs.run()
        self.cache[path] = fs if ok else None
        return self.cache[path]


class Evaluator:
    """One Python file, evaluated. A reader's subclass sets the class attributes and the hooks."""

    # Top-level packages that are libraries, never workspace modules
    no_import: ClassVar[Container[str]] = frozenset()
    # Calls into these are the libraries', never a workspace helper's
    library_prefixes: ClassVar[tuple[str, ...]] = ()
    # Decorated functions that the tool runs, so their bodies are not read as uncalled helpers
    self_running: ClassVar[tuple[str, ...]] = ()
    # What a file that cannot be parsed is
    not_parsed: ClassVar[UnknownCategory] = UnknownCategory.NOT_LITERAL
    # What a loop does to what is made inside it
    made_in_a_loop: ClassVar[str] = "definitions made a dynamic number of times"
    # The context the reader's hooks expect
    ctx_type: ClassVar[type[Ctx]] = Ctx

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
        # The file scanned. ``path`` is the module whose code runs, which is another one while a
        # helper imported from it is followed (LP-z2x)
        self.home = self.path
        self.rel = index.relative(self.path)
        self._foreign: Foreign | None = None
        self.index = index
        self.loader = loader
        self.emit = emit
        self.import_depth = import_depth
        self.sha256: str | None = None
        self.steps = 0
        self.unresolved: list[dict[str, Any]] = []
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

    # Budgets, read when used, so a reader's module can set its own
    @property
    def max_steps(self) -> int:
        return MAX_STEPS

    @property
    def file_size_cap(self) -> int:
        return FILE_SIZE_CAP

    # --- entry ----------------------------------------------------------------------------
    def run(self) -> bool:
        data = read_capped(self.path, self.file_size_cap, self.index.workspace)
        if data is None:
            self._unresolved(
                0,
                0,
                f"not a regular file in the workspace under {self.file_size_cap} bytes; not parsed",
                self.ctx_type(),
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
                self.ctx_type(),
                force=True,
                category=self.not_parsed,
            )
            return False
        except (ValueError, RecursionError, MemoryError) as exc:
            self._unresolved(
                0,
                0,
                f"cannot parse: {type(exc).__name__}",
                self.ctx_type(),
                force=True,
                category=self.not_parsed,
            )
            return False
        self.scope.vars["__file__"] = Const(to_posix(self.path), conf="inferred")
        self.scope.vars["__name__"] = Const("__lineagepilot_static__", conf="inferred")
        try:
            self.exec_block(self.tree.body, self.scope, self.ctx_type(emit=self.emit))
            if self.emit:
                self._scan_uncalled()
        except RecursionError:
            self._unresolved(
                0,
                0,
                "expression nesting too deep for the static evaluator",
                self.ctx_type(),
                force=True,
                category=UnknownCategory.LIMIT,
            )
        except OverBudget:
            self._unresolved(
                0,
                0,
                f"analysis stopped after {self.max_steps} steps; what came before is kept",
                self.ctx_type(),
                force=True,
                category=UnknownCategory.LIMIT,
            )
        return True

    def _step(self) -> None:
        self.steps += 1
        if self.steps > self.max_steps:
            raise OverBudget

    def _segment(self, node: ast.AST) -> str:
        """The whole lines a node spans: enough to see what it mentions, and cheap to get."""
        start = getattr(node, "lineno", 1) - 1
        return "".join(self.lines[start : getattr(node, "end_lineno", start + 1)])

    def _scan_uncalled(self) -> None:
        for fv in list(self.functions.values()):
            if id(fv.node) in self.called or fv.kind in self.self_running:
                continue
            scope = Scope(self.scope)
            for a in all_args(fv.node.args):
                scope.vars[a] = Unknown(f"parameter {a!r} of function {fv.name!r}")
            ctx = self.ctx_type(
                dynamic=f"inside function {fv.name!r}, which is never called at module level "
                "(factory or runtime callable)",
                funcs=(fv.name,),
            )
            try:
                self.exec_block(fv.node.body, scope, ctx)
            except Return:
                pass

    # --- records --------------------------------------------------------------------------
    def _loc(self, line: int, col: int) -> dict[str, Any]:
        """Where a record is reported. In a helper followed into another file, a line there means
        nothing in this one, so it is the call site in this file (LP-z2x)."""
        if self._foreign is not None:
            line, col = self._foreign.site
        return {"file": self.rel, "line": line, "col": col}

    def _site_loc(self, line: int, col: int) -> dict[str, Any]:
        """A call site: always in this file, even inside a followed helper."""
        return {"file": self.rel, "line": line, "col": col}

    def _here(self, line: int, col: int) -> dict[str, Any]:
        """Where a node is, in the file it is in."""
        rel = self._foreign.rel if self._foreign is not None else self.rel
        return {"file": rel, "line": line, "col": col}

    def _dynamic_category(self) -> UnknownCategory:
        return UnknownCategory.LOOP_OR_FACTORY

    def _unresolved(
        self,
        line: int,
        col: int,
        reason: str,
        ctx: Ctx,
        force: bool = False,
        category: UnknownCategory = UnknownCategory.NOT_LITERAL,
        at_site: bool = False,
        **extra: Any,
    ) -> None:
        if not (ctx.emit or force) or not self.emit and not force:
            return
        if self._foreign is not None and not at_site:
            # A line in the helper's file: say so, and report it at the call in this file
            reason = f"{reason} (in {self._foreign.name}() in {self._foreign.rel}, line {line})"
            line, col = self._foreign.site
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
            raise Return(self.eval(st.value, scope, ctx) if st.value is not None else Const(None))
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
            except Return:
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
        if test is not None and self._guessed_test(st.test, scope):
            self._skipped(st.orelse if test else st.body, st.test, scope, ctx)

    def _guessed_test(self, test: ast.expr, scope: Scope) -> bool:
        """Whether a test was decided by a guess, such as an environment variable's default."""
        v = self.eval(test, scope, self.ctx_type(emit=False))
        return isinstance(v, Const) and v.conf == "heuristic"

    def _truth(self, test: ast.expr, scope: Scope, ctx: Ctx) -> bool | None:
        # `if __name__ == "__main__":` never runs when a tool loads the file
        if (
            isinstance(test, ast.Compare)
            and isinstance(test.left, ast.Name)
            and test.left.id == "__name__"
            and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Constant)
            and test.comparators[0].value == "__main__"
        ):
            return isinstance(test.ops[0], ast.NotEq)
        v = self.eval(test, scope, self.ctx_type(emit=False))
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
            dynamic=ctx.dynamic or f"inside a {kind} (line {st.lineno}): {self.made_in_a_loop}",
        )
        inner = scope.copy()
        if isinstance(st, (ast.For, ast.AsyncFor)):
            self.eval(st.iter, scope, ctx)
            self._bind(
                st.target, Unknown(f"loop variable of the {kind} at line {st.lineno}"), inner, lctx
            )
        else:
            self.eval(st.test, scope, self.ctx_type(emit=False))
        try:
            self.exec_block(st.body, inner, lctx)
            self.exec_block(st.orelse, inner, lctx)
        except Return:
            pass
        for n, v in inner.vars.items():
            if scope.vars.get(n) is not v:
                scope.vars[n] = Unknown(f"{n!r} is assigned inside a {kind} (line {st.lineno})")

    def _with(self, st: ast.With | ast.AsyncWith, scope: Scope, ctx: Ctx) -> None:
        new_ctx = ctx
        for item in st.items:
            v = self.eval(item.context_expr, scope, new_ctx)
            new_ctx = self._enter(v, item.context_expr, new_ctx)
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
        self._defined(fv, scope, ctx)

    # --- hooks ----------------------------------------------------------------------------
    # A reader fills these in for its tool. Each is called where the DAG scan, which they were
    # cut from, did that part of its work, so moving them here changed no result.
    def _decorator_kind(
        self, st: ast.FunctionDef | ast.AsyncFunctionDef, scope: Scope
    ) -> tuple[str, dict[str, Val]]:
        """What a function's decorators make it, and their keyword arguments."""
        return "plain", {}

    def _defined(self, fv: FuncV, scope: Scope, ctx: Ctx) -> None:
        """A function was defined in ``scope``, and bound to ``fv``."""
        if fv.kind == "plain" and not ctx.funcs:
            self._subprocess_heuristic(fv.node, fv.name, ctx)

    def _enter(self, v: Val, node: ast.expr, ctx: Ctx) -> Ctx:
        """The context inside ``with v:``."""
        return ctx

    def _skipped(self, body: list[ast.stmt], test: ast.expr, scope: Scope, ctx: Ctx) -> None:
        """A branch not run because a guess decided its test."""

    def _scan_nested_operators(self, node: ast.AST, scope: Scope, ctx: Ctx, where: str) -> None:
        """Code in an expression that is not evaluated, such as a comprehension."""

    def _operator(
        self, op: ast.operator, left: Val, right: Val, node: ast.AST, ctx: Ctx
    ) -> Val | None:
        """A binary operator that means something in the tool, such as ``>>``; None if not."""
        return None

    def _tool_attribute(self, base: Val, attr: str, node: ast.Attribute) -> Val | None:
        """An attribute of one of the tool's values; None if it is not one."""
        return None

    def _tool_method(
        self,
        base: Val,
        meth: str,
        args: list[Val],
        kwargs: dict[str, Val],
        node: ast.Call,
        scope: Scope,
        ctx: Ctx,
    ) -> Val | None:
        """A method of one of the tool's values; None if it is not one."""
        return None

    def _tool_call(
        self,
        fname: str,
        node: ast.Call,
        args: list[Val],
        kwargs: dict[str, Val],
        has_star: bool,
        scope: Scope,
        ctx: Ctx,
    ) -> Val | None:
        """A call to something the tool provides, by its dotted name; None if it is not one."""
        return None

    def _maybe_wrapper(
        self, fname: str, local: str | None, node: ast.Call, kwargs: dict[str, Val], ctx: Ctx
    ) -> None:
        """A call to something unknown, which may build what the reader looks for."""

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
        """A call to a function defined in the workspace."""
        if fv.imported_from:
            return Unknown(f"call to imported function {fv.name}", line=node.lineno)
        self.called.add(id(fv.node))
        return self._exec_function(fv, node, args, kwargs, has_star, ctx)

    def _not_followed(self, fv: FuncV, node: ast.Call, ctx: Ctx, why: str) -> Val:
        """A call that is not followed."""
        return Unknown(f"call to {fv.name}() not followed ({why})", line=node.lineno)

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
        env: list[EnvPath] = []
        for part in node.values:
            if isinstance(part, ast.Constant):
                out.append(str(part.value))
                continue
            if isinstance(part, ast.FormattedValue):
                if part.format_spec is not None:
                    return Unknown("f-string with a format spec", line=node.lineno)
                v = self.eval(part.value, scope, ctx)
                if isinstance(v, EnvPath) and part.conversion in (-1, ord("s")):
                    env.append(v)
                    out.append(v.text)
                    continue
                s = to_str(v)
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
        if env:
            reason = f"an f-string part is not resolvable ({env[0].reason})"
            return env_value(env, "".join(out), reason, node.lineno)
        return bounded_str("".join(out), weakest(*confs), tuple(dict.fromkeys(notes)), node.lineno)

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
            v = clone(v)
            v.with_meta(
                "inferred",
                (f"{module}.{attr} resolved via import from {self.index.relative(path)}",),
            )
            return v
        return None

    def _resolve_import(self, local: str) -> Val | None:
        """What an imported name is bound to in its module. ``from . import bronze`` in a
        package's ``__init__.py`` names the package itself, where ``bronze`` is bound only by
        that import: it is the submodule, as Python finds it. A name that leads back to itself
        through other modules is not followed again (LP-odu)."""
        mod, attr, level = self.imports[local]
        path = self.loader.find(mod, self.path, level)
        if attr and path is not None and path == self.path:
            sub = self.loader.find(f"{mod}.{attr}" if mod else attr, self.path, level)
            if sub is not None:
                return Sym(self.loader.dotted(sub))
        if path is None:
            if attr:
                sub = self.loader.find(f"{mod}.{attr}" if mod else attr, self.path, level)
                if sub is not None:
                    return Sym(f"{mod}.{attr}")
            return None
        mark = (path, attr)
        if mark in self.loader.resolving:
            return None
        self.loader.resolving.add(mark)
        try:
            return self._import_from(local, path, attr)
        finally:
            self.loader.resolving.discard(mark)

    def _import_from(self, local: str, path: str, attr: str | None) -> Val | None:
        fs = self.loader.module(path, self.import_depth + 1)
        if fs is None or attr is None:
            return None
        v = fs.scope.vars.get(attr)
        if v is None:
            return None
        if isinstance(v, Sym) and attr in fs.imports:
            v = fs._resolve_import(attr) or v
        if isinstance(v, (Const, PathV, ListV, DictV, CallV)):
            v = clone(v)
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
        tool = self._tool_attribute(base, a, node)
        if tool is not None:
            return tool
        if isinstance(base, EnvPath) and a == "parent" and env_tail(base.text) is not None:
            # ${X}/a/b -> ${X}/a; the variable alone has no parent that can be known
            return replace(base, text=posixpath.dirname(base.text.rstrip("/")))
        if isinstance(base, Unknown):
            return plain(base)
        return Unknown(f"attribute .{a} of a {type(base).__name__} value", line=node.lineno)

    def _e_Subscript(self, node: ast.Subscript, scope: Scope, ctx: Ctx) -> Val:
        base = self.eval(node.value, scope, ctx)
        key = self.eval(node.slice, scope, ctx)
        if isinstance(base, Sym) and base.name in ("os.environ",):
            k = as_str(key)
            return no_default(
                k, f"environment variable {k!r} read with os.environ[...] (no default)", node.lineno
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
            return plain(base)
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
                    nxt = clone(vals[i + 1])
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
            # An environment variable's default makes the answer a guess, and a branch it
            # decides is reported (see _if)
            conf = weakest(left.conf, rights[0].conf)
            try:
                if isinstance(op, ast.Eq):
                    return Const(a == b, conf=conf)
                if isinstance(op, ast.NotEq):
                    return Const(a != b, conf=conf)
                if isinstance(op, ast.In):
                    return Const(a in b, conf=conf)
                if isinstance(op, ast.NotIn):
                    return Const(a not in b, conf=conf)
            except TypeError:
                pass
        return Unknown("comparison over non-literal values", line=node.lineno)

    def _e_UnaryOp(self, node: ast.UnaryOp, scope: Scope, ctx: Ctx) -> Val:
        v = self.eval(node.operand, scope, ctx)
        if isinstance(v, Const):
            try:
                if isinstance(node.op, ast.Not):
                    return Const(not v.value, conf=v.conf)
                if isinstance(node.op, ast.USub):
                    return Const(-v.value, conf=v.conf)
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

    def _e_BinOp(self, node: ast.BinOp, scope: Scope, ctx: Ctx) -> Val:
        left = self.eval(node.left, scope, ctx)
        right = self.eval(node.right, scope, ctx)
        return self._binop(node.op, left, right, node, ctx)

    def _binop(self, op: ast.operator, left: Val, right: Val, node: ast.AST, ctx: Ctx) -> Val:
        line = getattr(node, "lineno", 0)
        tool = self._operator(op, left, right, node, ctx)
        if tool is not None:
            return tool
        if isinstance(op, ast.Div) and isinstance(left, PathV) and not isinstance(right, EnvPath):
            s = to_str(right)
            if s is None:
                return Unknown("path joined with a non-literal part", line=line)
            j = left.join(s)
            conf, notes = collect_meta(left, right)
            j.conf, j.notes = weakest(conf, "inferred"), notes
            return j
        if isinstance(op, (ast.Div, ast.Add)) and (
            isinstance(left, EnvPath) or isinstance(right, EnvPath)
        ):
            # Path(os.environ["X"]) / "dbt", os.environ["X"] + "/dbt" (LP-0o1)
            joined = (
                join_parts([left, right], line)
                if isinstance(op, ast.Div)
                else concat_env(left, right, line)
            )
            if joined is not None:
                return joined
        if isinstance(op, ast.Add):
            if (
                isinstance(left, Const)
                and isinstance(right, Const)
                and isinstance(left.value, str)
                and isinstance(right.value, str)
            ):
                conf, notes = collect_meta(left, right)
                return bounded_str(left.value + right.value, weakest(conf, "inferred"), notes, line)
            if isinstance(left, ListV) and isinstance(right, ListV):
                if len(left.items) + len(right.items) > MAX_ITEMS:
                    return Unknown(f"a list of more than {MAX_ITEMS} items", line=line)
                return ListV(left.items + right.items, conf=weakest(left.conf, right.conf))
        if isinstance(op, ast.Mod) and isinstance(left, Const) and isinstance(left.value, str):
            args: Any
            items = right.items if isinstance(right, ListV) else [right]
            env = [i for i in items if isinstance(i, EnvPath)]
            if isinstance(right, ListV):
                args = tuple(fill(i) for i in right.items)
                if any(a is None for a in args):
                    return Unknown("%-format with non-literal arguments", line=line)
            elif isinstance(right, DictV):
                return Unknown("%-format with a mapping", line=line)
            else:
                args = fill(right)
                if args is None:
                    return Unknown("%-format with a non-literal argument", line=line)
            if not safe_percent(left.value):
                return Unknown("%-format with a very large width", line=line)
            if env and re.search(r"%[^s%]", left.value.replace("%%", "")):
                return Unknown(f"%-format of {env[0].reason}, not with %s", line=line)
            try:
                text = left.value % args
            except (TypeError, ValueError, MemoryError, OverflowError):
                return Unknown("%-format failed", line=line)
            if env:
                return env_value(env, text, env[0].reason, line)
            return bounded_str(text, "inferred", (), line)
        if isinstance(left, Unknown):
            return plain(left)
        if isinstance(right, Unknown):
            return plain(right)
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
        if isinstance(base, EnvPath):
            # Path(os.environ["X"]).joinpath("dbt") and the like (LP-0o1); anything else on it
            # is unknown, below
            if meth == "joinpath":
                joined = join_parts([base, *args], line)
                if joined is not None:
                    return joined
            if meth in ("resolve", "absolute", "as_posix", "__str__", "__fspath__") and not args:
                return base
        if (
            meth == "format"
            and isinstance(base, Const)
            and isinstance(base.value, str)
            and any(isinstance(a, EnvPath) for a in [*args, *kwargs.values()])
        ):
            return self._format_env(base.value, args, kwargs, line)
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
                    s = to_str(a)
                    if s is None:
                        return Unknown("joinpath with a non-literal part", line=line)
                    out = out.join(s)
                conf, notes = collect_meta(base, *args)
                out.conf, out.notes = weakest(conf, "inferred"), notes
                return out
            if meth in ("exists", "is_dir", "is_file"):
                return Unknown(f"filesystem check .{meth}() at parse time", line=line)
            if meth == "with_name" and args and to_str(args[0]) is not None:
                return PathV(posixpath.join(posixpath.dirname(p), to_str(args[0])), conf="inferred")  # type: ignore[arg-type]
        if isinstance(base, Const) and isinstance(base.value, str):
            s = base.value
            try:
                if meth == "format":
                    sargs = [to_str(a) for a in args]
                    skw = {k: to_str(v) for k, v in kwargs.items()}
                    if any(a is None for a in sargs) or any(v is None for v in skw.values()):
                        return Unknown(".format() with non-literal arguments", line=line)
                    if not safe_format(s):
                        return Unknown(
                            ".format() with attribute access or a format spec", line=line
                        )
                    return bounded_str(
                        s.format(*sargs, **skw), weakest(base.conf, "inferred"), (), line
                    )
                if (
                    meth == "replace"
                    and len(args) >= 2
                    and all(to_str(a) is not None for a in args[:2])
                ):
                    old, new = to_str(args[0]), to_str(args[1])
                    if len(s) + (s.count(old) + 1) * len(new) > MAX_STRING:  # type: ignore[arg-type]
                        return Unknown(f"a string of more than {MAX_STRING} characters", line=line)
                    return Const(s.replace(old, new), conf=weakest(base.conf, "inferred"))  # type: ignore[arg-type]
                if meth in ("strip", "lstrip", "rstrip", "lower", "upper") and not args:
                    return Const(getattr(s, meth)(), conf=base.conf)
                if meth == "join" and args and isinstance(args[0], ListV):
                    parts = [to_str(i) for i in args[0].items]
                    if any(p is None for p in parts):
                        return Unknown("str.join over non-literal items", line=line)
                    return bounded_str(s.join(parts), weakest(base.conf, "inferred"), (), line)  # type: ignore[arg-type]
                if meth == "split":
                    sep = to_str(args[0]) if args else None
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
        tool = self._tool_method(base, meth, args, kwargs, node, scope, ctx)
        if tool is not None:
            return tool
        if isinstance(base, Unknown):
            return plain(base)
        for a in list(args) + list(kwargs.values()):
            if isinstance(a, Unknown):
                return Unknown(f".{meth}() argument not resolvable: {a.reason}", line=line)
        return Unknown(f".{meth}() on a {type(base).__name__} value is not evaluated", line=line)

    @staticmethod
    def _format_env(template: str, args: list[Val], kwargs: dict[str, Val], line: int) -> Val:
        """``"{}/dbt".format(os.environ["X"])``: plain fields only, as for any ``.format()``."""
        sargs = [fill(a) for a in args]
        skw = {k: fill(v) for k, v in kwargs.items()}
        if any(a is None for a in sargs) or any(v is None for v in skw.values()):
            return Unknown(".format() with non-literal arguments", line=line)
        if not safe_format(template):
            return Unknown(".format() with attribute access or a format spec", line=line)
        try:
            text = template.format(*sargs, **skw)
        except (IndexError, KeyError, ValueError):
            return Unknown("str.format() failed on literal arguments", line=line)
        env = [a for a in [*args, *kwargs.values()] if isinstance(a, EnvPath)]
        return env_value(env, text, env[0].reason, line)

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
        # environment variables: use the default, flagged heuristic
        if fname in ("os.getenv", "os.environ.get", "getenv", "environ.get") or fname.endswith(
            ".environ.get"
        ):
            key = as_str(args[0]) if args else None
            default = args[1] if len(args) > 1 else kwargs.get("default")
            if default is None or (isinstance(default, Const) and default.value is None):
                return no_default(key, f"environment variable {key!r} has no default", line)
            d = clone(default)
            return d.with_meta(
                "heuristic", (f"environment variable {key!r} assumed unset; used its default",)
            )
        if fname in ("os.path.join", "posixpath.join", "path.join") or (
            fname.endswith("path.join") and fname.startswith("os")
        ):
            if any(isinstance(a, EnvPath) for a in args):
                joined = join_parts(args, line)  # os.path.join(os.environ["X"], "dbt") (LP-0o1)
                if joined is not None:
                    return joined
            parts = [to_str(a) for a in args]
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
            if args and isinstance(args[0], EnvPath):
                # The variable holds a whole path at run time: these keep it as it is, and the
                # parent of ${X}/a/b is ${X}/a. The parent of ${X} alone cannot be known.
                if fname in ("os.path.abspath", "os.path.realpath", "os.path.normpath"):
                    return args[0]
                if fname == "os.path.dirname" and env_tail(args[0].text) is not None:
                    return replace(args[0], text=posixpath.dirname(args[0].text.rstrip("/")))
            s = to_str(args[0]) if args else None
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
            if any(isinstance(a, EnvPath) for a in args):
                joined = join_parts(args, line)  # Path(os.environ["X"], "dbt") (LP-0o1)
                if joined is not None:
                    return joined
            parts = [to_str(a) for a in args]
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
                if isinstance(args[0], EnvPath):
                    return args[0]
                s = to_str(args[0])
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
        tool = self._tool_call(fname, node, args, kwargs, has_star, scope, ctx)
        if tool is not None:
            return tool
        helper = self._imported_helper(fname, node.func)
        if helper is not None:
            return self._follow(helper[0], helper[1], node, args, kwargs, has_star, ctx)
        self._maybe_wrapper(fname, root_name(node.func), node, kwargs, ctx)
        return CallV(fname, args, kwargs, line=line, col=col, has_star=has_star)

    def _imported_helper(self, fname: str, func: ast.expr) -> tuple[FuncV, Evaluator] | None:
        """A plain function defined at the top of another workspace module, called from the file
        scanned: ``f()`` after ``from helpers import f``, or ``helpers.f()`` after
        ``import helpers`` or ``from pkg import helpers`` (LP-z2x).

        One level only: not from a module read for its constants, and not from inside a helper
        already followed, where the call stays a named unknown. A module is found inside the
        workspace only, and read as syntax, as for constants.
        """
        if self.import_depth or self._foreign is not None:
            return None
        if fname.startswith(self.library_prefixes) or "." not in fname:
            return None
        local = root_name(func)
        if local is None:
            return None
        level = self.imports[local][2] if local in self.imports else 0
        if isinstance(func, ast.Name):
            if local not in self.imports:
                return None
            module, name = self.imports[local][0], self.imports[local][1] or ""
        else:
            module, name = fname.rsplit(".", 1)
        path = self.loader.find(module, self.path, level)
        if path is None or path == self.path:
            return None
        fs = self.loader.module(path, self.import_depth + 1)
        fv = fs.scope.vars.get(name) if fs is not None else None
        if not isinstance(fv, FuncV) or fv.kind != "plain" or fs is None:
            return None
        return fv, fs

    def _follow(
        self,
        fv: FuncV,
        fs: Evaluator,
        node: ast.Call,
        args: list[Val],
        kwargs: dict[str, Val],
        has_star: bool,
        ctx: Ctx,
    ) -> Val:
        """Run an imported helper's body here, as for a local helper, with its own module's names
        and imports. What it creates is reported at the call in this file."""
        if self._call_depth >= MAX_CALL_DEPTH or fv.name in ctx.funcs:
            return self._not_followed(fv, node, ctx, "depth/recursion limit")
        saved = (self.path, self.imports, self.lines, self.scope, self.import_depth, self._foreign)
        self.path, self.imports, self.lines = fs.path, fs.imports, fs.lines
        self.scope, self.import_depth = fs.scope, fs.import_depth
        self._foreign = Foreign(fs.rel, fv.name, (node.lineno, node.col_offset))
        try:
            return self._exec_function(fv, node, args, kwargs, has_star, ctx)
        finally:
            (
                self.path,
                self.imports,
                self.lines,
                self.scope,
                self.import_depth,
                self._foreign,
            ) = saved

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
                inner.vars[name] = self.eval(defaults[di], dscope, self.ctx_type(emit=False))
            else:
                inner.vars[name] = Unknown(f"parameter {name!r} of {fv.name}() not passed")
        for a, d in zip(params.kwonlyargs, params.kw_defaults, strict=False):
            if a.arg in kwargs:
                inner.vars[a.arg] = kwargs[a.arg]
            elif d is not None:
                inner.vars[a.arg] = self.eval(d, dscope, self.ctx_type(emit=False))
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
        except Return as r:
            return r.args[0]
        finally:
            self._call_depth -= 1
        return Const(None)


def all_args(args: ast.arguments) -> list[str]:
    names = [a.arg for a in args.posonlyargs + args.args + args.kwonlyargs]
    if args.vararg:
        names.append(args.vararg.arg)
    if args.kwarg:
        names.append(args.kwarg.arg)
    return names


def to_str(v: Val | None) -> str | None:
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


def fill(v: Val | None) -> str | None:
    """What a value puts into a string it is formatted into: an environment variable with no
    default goes in as ``${NAME}``."""
    return v.text if isinstance(v, EnvPath) else to_str(v)


def no_default(key: str | None, reason: str, line: int) -> Unknown:
    """An environment variable read with no default: unknown, but it remembers its name, so a
    path built on it can still be matched by what comes after it (LP-0o1)."""
    if key is not None and _ENV_NAME.fullmatch(key):
        return EnvPath(reason, line=line, text="${" + key + "}", names=(key,))
    return Unknown(reason, line=line)


def env_value(parts: list[EnvPath], text: str, reason: str, line: int) -> Unknown:
    """A string built on the environment variables in ``parts``, written out as ``text``."""
    if len(text) > MAX_STRING:
        return Unknown(f"a string of more than {MAX_STRING} characters", line=line)
    names = tuple(dict.fromkeys(name for part in parts for name in part.names))
    return EnvPath(reason, line=line, text=text, names=names)


def join_parts(vals: list[Val], line: int) -> Val | None:
    """``posixpath.join`` over strings, paths and environment variables with no default, or None
    when a part is none of these. A part that is absolute drops those before it, as in Python."""
    texts = [text_of(v) for v in vals]
    if not texts or any(t is None for t in texts):
        return None
    start = max((i for i, t in enumerate(texts) if is_absolute(t)), default=0)  # type: ignore[arg-type]
    joined = posixpath.join(*texts)  # type: ignore[arg-type]
    env = [v for v in vals[start:] if isinstance(v, EnvPath)]
    if env:
        return env_value(env, joined, env[0].reason, line)
    conf, notes = collect_meta(*vals)
    return PathV(posixpath.normpath(joined), conf=weakest(conf, "inferred"), notes=notes)


def concat_env(left: Val, right: Val, line: int) -> Val | None:
    """``os.environ["X"] + "/dbt"``: string concatenation that keeps the variable."""
    a, b = fill(left), fill(right)
    if a is None or b is None or isinstance(left, PathV) or isinstance(right, PathV):
        return None
    env = [v for v in (left, right) if isinstance(v, EnvPath)]
    return env_value(env, a + b, env[0].reason, line)


def call_name(call: ast.Call) -> str:
    f = call.func
    return f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", "")


def root_name(func: ast.expr) -> str | None:
    while isinstance(func, ast.Attribute):
        func = func.value
    return func.id if isinstance(func, ast.Name) else None


def clone(v: Val) -> Val:
    return copy.copy(v)


def bounded_str(text: str, conf: str, notes: tuple[str, ...], line: int) -> Val:
    if len(text) > MAX_STRING:
        return Unknown(f"a string of more than {MAX_STRING} characters", line=line)
    return Const(text, conf=conf, notes=notes)


def safe_percent(template: str) -> bool:
    """No width or precision in a %-format that would allocate memory."""
    for found in _FORMAT_WIDTH.finditer(template):
        for digits in found.groups():
            if digits and (len(digits) > 6 or int(digits) > MAX_FORMAT_WIDTH):
                return False
    return True


def safe_format(template: str) -> bool:
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


def scrub(value: Any, workspace: str) -> Any:
    """Where the workspace is on this machine is no part of the result: ``Path(__file__)`` values
    reach the notes and reasons."""
    if isinstance(value, str):
        return value.replace(workspace, "<workspace>") if workspace in value else value
    if isinstance(value, list):
        return [scrub(v, workspace) for v in value]
    if isinstance(value, dict):
        return {k: scrub(v, workspace) for k, v in value.items()}
    return value
