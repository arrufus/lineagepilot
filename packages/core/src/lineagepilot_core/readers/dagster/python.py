"""A workspace's dbt asset definitions in Python (task 8.4), read with the shared safe evaluator.

Each Python file that imports Dagster is read as syntax by ``readers.common.evaluator``, never
imported or run, with its budgets. What this module adds is what dagster 1.13.29's and
dagster-dbt 0.29.29's calls mean:

- ``@dbt_assets(manifest=…, select=…, exclude=…, selector=…, name=…, project=…)`` on a function: a
  dbt asset definition, named by ``name`` or else by the function;
- ``DbtProject(project_dir, target_path=…)``, whose ``manifest_path`` is
  ``<project_dir>/<target_path>/manifest.json``; and ``file_relative_path``;
- ``load_assets_from_dbt_project`` and ``load_assets_from_dbt_manifest``, which dagster-dbt no
  longer exports but older projects call (D-c): dbt asset definitions too, named by ``op_name``,
  or else by the name they are assigned to;
- ``Definitions(assets=…)``, with ``load_assets_from_modules`` and its kin, which say which code
  location loads each definition (``locations``).

The dbt project is the one ``project`` or the manifest path names: a manifest is at
``<project>/<target>/manifest.json``. The selection runs over that project's manifest through the
shared selector engine (``selection``). Whatever cannot be read is an unknown at the definition's
line: a value that is not literal, a definition made in a loop or a factory, a path that matches no
dbt project, or a selector that cannot be run.

An environment variable that is read with no default, and only tested, is taken to be unset, as
the evaluator takes any variable with a default to be: so ``if os.getenv("X"):`` runs its ``else``,
and what that branch sets is ``heuristic``. A dbt asset definition in the branch not read is an
unknown, never left out in silence.
"""

from __future__ import annotations

import ast
import os
import posixpath
from dataclasses import dataclass, field, replace
from typing import Any

from lineagepilot_core.readers.common.categories import UnknownCategory
from lineagepilot_core.readers.common.evaluator import (
    SUBPROCESS_FUNCS,
    Ctx,
    Evaluator,
    ModuleLoader,
    Return,
    Scope,
    all_args,
    clone,
)
from lineagepilot_core.readers.common.projects import ProjectIndex
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
    text_of,
    weakest,
)
from lineagepilot_core.readers.dagster import asset_selection as selections
from lineagepilot_core.readers.dagster import deps as dependencies
from lineagepilot_core.readers.dagster.keys import DEFAULT, LEGACY_GROUPS, KeyRule, prefix_of
from lineagepilot_core.readers.dagster.projects import MAX_NAME
from lineagepilot_core.readers.dagster.selection import (
    DEFAULT_SELECT,
    MAX_SELECTION,
    SELECTION_KEYS,
    match_project,
    select_nodes,
    unknown,
)
from lineagepilot_core.readers.dagster.values import (
    AssetsV,
    JobV,
    ManifestPath,
    ModulesV,
    ScheduleV,
    SelectionV,
    SpecV,
)

DBT_ASSETS = "dbt_assets"
API = "@dbt_assets"
# The loaders dagster-dbt no longer exports (D-c), and the parameter each takes first
LEGACY = {
    "load_assets_from_dbt_project": "project_dir",
    "load_assets_from_dbt_manifest": "manifest",
}
# Dagster's decorators whose functions Dagster runs, so their bodies are not read as helpers
DAGSTER_DECORATORS = {
    "asset",
    "multi_asset",
    "graph_asset",
    "graph_multi_asset",
    "observable_source_asset",
    "multi_observable_source_asset",
    "asset_check",
    "multi_asset_check",
    "op",
    "graph",
    "job",
    "schedule",
    "sensor",
    "asset_sensor",
    "multi_asset_sensor",
    "run_status_sensor",
    "run_failure_sensor",
    "repository",
}
# Calls that say which modules' definitions a Definitions call loads
FROM_MODULES = {
    "load_assets_from_modules",
    "load_assets_from_current_module",
    "load_assets_from_package_module",
    "load_assets_from_package_name",
}
SCHEDULE = "schedule"
ASSET = "asset"
MULTI_ASSET = "multi_asset"
# A function Dagster calls once, with no arguments, when it loads the code location
REPOSITORY = "repository"
DEFAULT_TARGET = "target"
MANIFEST = "manifest.json"
_ENV_READS = ("os.getenv", "os.environ.get", "getenv", "environ.get")


# Top-level packages that are never workspace modules: Dagster's own, and the standard ones a
# definitions file uses. Any other name is looked for in the workspace, where an integration such
# as dagster_snowflake is not, and a team's own package, often named dagster_<something>, is.
LIBRARIES = frozenset(
    {"dagster", "dagster_dbt", "dagster_pipes", "dagster_shared", "dagster_dg", "dagster_cloud"}
    | {"os", "sys", "pathlib", "json", "typing", "datetime", "re", "logging", "functools"}
    | {"itertools", "collections", "dataclasses", "enum", "subprocess", "yaml", "__future__"}
    | {"dbt", "pandas", "pendulum"}
)


class DagsterScanner(Evaluator):
    """One Python file: the shared evaluator, and what Dagster's and dagster-dbt's calls mean."""

    no_import = LIBRARIES
    library_prefixes = ("dagster.", "dagster_dbt.", "os.", "pathlib.", "json.", "typing.")
    self_running = (DBT_ASSETS, SCHEDULE, ASSET, MULTI_ASSET, REPOSITORY, "dagster")

    def __init__(
        self,
        path: str,
        index: ProjectIndex,
        loader: ModuleLoader,
        emit: bool = True,
        import_depth: int = 0,
    ):
        super().__init__(path, index, loader, emit, import_depth)
        # The dbt project whose links are wanted; set for a file that is read for its own sake
        self.root: str | None = None
        self.records: list[dict[str, Any]] = []
        self.loads: list[dict[str, Any]] = []
        # Asset jobs and schedules (task 8.6), resolved once code locations are known (jobs)
        self.jobs: list[dict[str, Any]] = []
        self.schedules: list[dict[str, Any]] = []
        # Assets that are not dbt's, which may read dbt's (task 8.7)
        self.assets: list[dict[str, Any]] = []
        # Unknowns about a code location or an asset, not a dbt asset definition (task 8.8)
        self.located: list[dict[str, Any]] = []
        # The calls being followed, outermost first, each by the file scanned and its line
        self._calls: list[tuple[str, int]] = []
        # The name the statement being run assigns its value to, and the nodes that make it:
        # the value, or the definitions ``with_resources(…)`` wraps
        self._target: tuple[str, frozenset[int]] | None = None

    def _module(self) -> str:
        """The module what is being read belongs to: the file scanned, relative to the workspace.
        In a helper followed from another file, what the helper makes is bound in the file that
        called it, and loaded with that file, so it is that file's too (LP-9zx)."""
        return self.rel

    def _key(self, line: int) -> str:
        """What identifies a definition: the file and line it is written at, and the calls that
        made it, so that each call of one factory is a definition of its own (LP-9zx)."""
        here = f"{self.index.relative(self.path)}:{line}"
        if not self._calls:
            return here
        return here + "@" + ",".join(f"{rel}:{at}" for rel, at in self._calls)

    def _exec_function(
        self,
        fv: FuncV,
        node: ast.Call,
        args: list[Val],
        kwargs: dict[str, Val],
        has_star: bool,
        ctx: Ctx,
    ) -> Val:
        self._calls.append((self.rel, node.lineno))
        try:
            return super()._exec_function(fv, node, args, kwargs, has_star, ctx)
        finally:
            self._calls.pop()

    # --- statements ---------------------------------------------------------------------------
    def exec_stmt(self, st: ast.stmt, scope: Scope, ctx: Ctx) -> None:
        target = None
        if isinstance(st, ast.Assign) and len(st.targets) == 1:
            if isinstance(st.targets[0], ast.Name):
                target = (st.targets[0].id, _made_by(st.value))
        elif isinstance(st, ast.AnnAssign) and isinstance(st.target, ast.Name) and st.value:
            target = (st.target.id, _made_by(st.value))
        saved, self._target = self._target, target
        try:
            super().exec_stmt(st, scope, ctx)
        finally:
            self._target = saved

    def _if(self, st: ast.If, scope: Scope, ctx: Ctx) -> None:
        env = _env_test(st.test, self, scope)
        if env is None:
            super()._if(st, scope, ctx)
            return
        name, is_set = env
        taken, skipped = (st.body, st.orelse) if not is_set else (st.orelse, st.body)
        before = dict(scope.vars)
        self.exec_block(taken, scope, ctx)
        note = f"the environment variable {name!r} was taken to be unset, as it has no default"
        for var, value in list(scope.vars.items()):
            if before.get(var) is not value:
                scope.vars[var] = clone(value).with_meta("heuristic", (note,))
        self._skipped_definitions(skipped, name, scope, ctx)

    def _e_IfExp(self, node: ast.IfExp, scope: Scope, ctx: Ctx) -> Val:
        env = _env_test(node.test, self, scope)
        if env is None:
            return super()._e_IfExp(node, scope, ctx)
        name, is_set = env
        value = self.eval(node.orelse if is_set else node.body, scope, ctx)
        note = f"the environment variable {name!r} was taken to be unset, as it has no default"
        return clone(value).with_meta("heuristic", (note,))

    def _truth(self, test: ast.expr, scope: Scope, ctx: Ctx) -> bool | None:
        env = _env_test(test, self, scope)
        if env is not None:
            return not env[1]
        return super()._truth(test, scope, ctx)

    def _skipped_definitions(self, body: list[ast.stmt], name: str, scope: Scope, ctx: Ctx) -> None:
        """A dbt asset definition made only if the variable is set: an unknown at its line."""
        reason = (
            f"made only if the environment variable {name!r} is set; it was taken to be unset, "
            "so this branch was not read"
        )
        for stmt in body:
            for sub in ast.walk(stmt):
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if any(
                        (
                            self._dotted_name(d.func if isinstance(d, ast.Call) else d, scope) or ""
                        ).rsplit(".", 1)[-1]
                        == DBT_ASSETS
                        for d in sub.decorator_list
                    ):
                        self._unknown_at(
                            API, sub.name, sub, ctx, UnknownCategory.NOT_LITERAL, reason
                        )
                elif isinstance(sub, ast.Call):
                    called = (self._dotted_name(sub.func, scope) or "").rsplit(".", 1)[-1]
                    if called in LEGACY:
                        self._unknown_at(
                            called,
                            f"{called}_{sub.lineno}",
                            sub,
                            ctx,
                            UnknownCategory.NOT_LITERAL,
                            reason,
                        )

    # --- decorated functions ------------------------------------------------------------------
    def _decorator_kind(
        self, st: ast.FunctionDef | ast.AsyncFunctionDef, scope: Scope
    ) -> tuple[str, dict[str, Val]]:
        for dec in st.decorator_list:
            target = dec.func if isinstance(dec, ast.Call) else dec
            name = self._dotted_name(target, scope) or ""
            if not name.startswith("dagster"):
                continue
            last = name.rsplit(".", 1)[-1]
            if last in (DBT_ASSETS, SCHEDULE, ASSET, MULTI_ASSET):
                kwargs: dict[str, Val] = {}
                for kw in dec.keywords if isinstance(dec, ast.Call) else []:
                    if kw.arg:
                        kwargs[kw.arg] = self.eval(kw.value, scope, self.ctx_type(emit=False))
                    else:
                        kwargs["**"] = Unknown("keyword arguments from a ** mapping")
                if isinstance(dec, ast.Call) and dec.args:
                    # @schedule("0 0 * * *", job=...): the cron schedule comes first
                    kwargs.setdefault(
                        "cron_schedule", self.eval(dec.args[0], scope, self.ctx_type(emit=False))
                    )
                return last, kwargs
            if last == REPOSITORY:
                return REPOSITORY, {}
            if last in DAGSTER_DECORATORS:
                return "dagster", {}
        return "plain", {}

    def _defined(self, fv: FuncV, scope: Scope, ctx: Ctx) -> None:
        if fv.kind == REPOSITORY:
            self._repository(fv, scope, ctx)
            return
        if fv.kind == "plain" and not ctx.funcs and self._wrapped(fv, scope, ctx):
            return
        if fv.kind not in (DBT_ASSETS, SCHEDULE) and not ctx.funcs:
            self._dbt_from_python(fv, ctx)
        if fv.kind in (ASSET, MULTI_ASSET):
            st = fv.node
            key = self._key(st.lineno)
            scope.vars[st.name] = AssetsV(f"dagster.{fv.kind}", [], {}, line=st.lineno, key=key)
            if fv.kind == ASSET:
                self._asset(fv, ctx, key)
            else:
                self._multi_asset(fv, ctx)
            return
        if fv.kind == SCHEDULE:
            st = fv.node
            name = as_str(fv.decorator_kwargs.get("name")) or st.name
            scope.vars[st.name] = self._schedule(
                "@schedule", name, st, fv.decorator_kwargs, ctx, self._key(st.lineno)
            )
            return
        if fv.kind != DBT_ASSETS:
            return
        st = fv.node
        key = self._key(st.lineno)
        scope.vars[st.name] = AssetsV(
            "dagster_dbt.dbt_assets",
            [],
            dict(fv.decorator_kwargs),
            line=st.lineno,
            col=st.col_offset,
            key=key,
        )
        name_v = fv.decorator_kwargs.get("name")
        name = as_str(name_v) if name_v is not None else st.name
        notes = []
        if name is None:
            name = st.name
            notes.append("its name= is not known here, so it is named after its function")
        self._definition(API, name, st, fv.decorator_kwargs, ctx, key, notes)

    def _wrapped(self, fv: FuncV, scope: Scope, ctx: Ctx) -> bool:
        """A function under a decorator of the workspace's own, or a library's the reader does not
        know, whose body runs dbt through a dbt resource's ``cli``: the decorator makes dbt assets
        of it, as ``@dbt_assets`` would, but which ones is not read (LP-ios). It is a dbt asset
        definition that cannot be read, so jobs and assets built on it are unknowns, not left out.
        """
        st = fv.node
        if not st.decorator_list:
            return False
        how = next(
            (
                found
                for node in ast.walk(st)
                if isinstance(node, ast.Call)
                for found in [self._runs_dbt(node)]
                if found is not None and found.endswith(".cli")
            ),
            None,
        )
        if how is None:
            return False
        decorator = st.decorator_list[0]
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        wrapper = self._dotted_name(target, scope) or ast.unparse(target)
        name = st.name
        for kw in decorator.keywords if isinstance(decorator, ast.Call) else []:
            if kw.arg == "name":
                name = as_str(self.eval(kw.value, scope, self.ctx_type(emit=False))) or name
        key = self._key(st.lineno)
        scope.vars[st.name] = AssetsV(
            f"wrapper.{wrapper}", [], {}, line=st.lineno, col=st.col_offset, key=key
        )
        record = self._record(wrapper[:MAX_NAME], name, st, ctx, key)
        if record is not None:
            reason = (
                f"made by {wrapper}(), a decorator the reader does not read; its body runs dbt "
                f"({how}), so it makes dbt assets, but which ones is not known"
            )
            self.records.append(unknown(record, UnknownCategory.WRAPPER, reason))
        return True

    def _repository(self, fv: FuncV, scope: Scope, ctx: Ctx) -> None:
        """A ``@repository`` function (LP-xpx). Dagster calls it once, with no arguments, when it
        loads the code location, so its body is read as if it ran where it is defined; and what it
        returns, definitions or a mapping of them, is loaded as a ``Definitions(…)`` call's are."""
        st = fv.node
        inner = Scope(scope)
        for name in all_args(st.args):
            inner.vars[name] = Unknown(f"parameter {name!r} of the repository {fv.name!r}")
        returned: Val = Const(None)
        try:
            self.exec_block(st.body, inner, ctx)
        except Return as done:
            returned = done.args[0]
        if isinstance(returned, DictV):
            # {"jobs": {...}, "schedules": {...}}: each value, a mapping or a list of them
            values: list[Val] = []
            for value in returned.items.values():
                values += list(value.items.values()) if isinstance(value, DictV) else [value]
            returned = ListV(values)
        self._load(st, [], {"assets": returned}, ctx)

    def _subprocess_heuristic(self, fn: ast.AST, name: str, ctx: Ctx) -> None:
        """The DAG scan's check, which Dagster's own replaces (``_dbt_from_python``)."""

    def _dbt_from_python(self, fv: FuncV, ctx: Ctx) -> None:
        """An asset, an op or a helper that runs dbt from its own code: ``subprocess``, dbt's
        Python API, ``DbtCliResource.cli`` outside ``@dbt_assets``, or Dagster Pipes. What it runs
        is not read, so it is an unknown, about the asset or else the code location."""
        st = fv.node
        how = None
        for node in ast.walk(st):
            if isinstance(node, ast.Call):
                how = self._runs_dbt(node)
                if how is not None:
                    break
        if how is None:
            return
        if fv.kind == ASSET:
            prefix_v = fv.decorator_kwargs.get("key_prefix")
            prefix = dependencies.asset_key(prefix_v) if prefix_v is not None else []
            name = "/".join([*(prefix or []), as_str(fv.decorator_kwargs.get("name")) or st.name])
            of = ASSET
        else:
            name, of = st.name, "location"
        key = self._key(st.lineno)
        record = self._record("python", name, st, ctx, key, f"python_dbt:{name}")
        if record is None:
            return
        record.update(kind="python_dbt", of=of)
        reason = f"{st.name}() runs dbt from Python ({how}); what it runs is not read"
        self.located.append(unknown(record, UnknownCategory.PYTHON_DBT, reason))

    def _runs_dbt(self, node: ast.Call) -> str | None:
        """How a call runs dbt, or None if it does not."""
        segment = self._segment(node)
        named = '"dbt"' in segment or "'dbt'" in segment or mentions_dbt(segment)
        dotted = self._dotted_name(node.func, self.scope) or ""
        attr = node.func.attr if isinstance(node.func, ast.Attribute) else ""
        receiver = ast.unparse(node.func.value) if isinstance(node.func, ast.Attribute) else ""
        if dotted.endswith(SUBPROCESS_FUNCS) and named:
            return dotted
        if attr == "invoke" and "dbtRunner" in segment:
            return "dbtRunner.invoke"
        if attr == "cli" and "dbt" in receiver.lower():
            return f"{receiver}.cli"
        if attr == "run" and "pipes" in (receiver + dotted).lower() and named:
            return "Dagster Pipes"
        return None

    # --- Dagster's values ---------------------------------------------------------------------
    def _tool_attribute(self, base: Val, attr: str, node: ast.Attribute) -> Val | None:
        if not (isinstance(base, CallV) and base.func.rsplit(".", 1)[-1] == "DbtProject"):
            return None
        project = base.arg("project_dir", 0)
        if attr == "project_dir":
            return project if isinstance(project, (PathV, Const, EnvPath)) else _not_known(project)
        if attr != "manifest_path":
            return None
        target = as_str(base.arg("target_path")) or DEFAULT_TARGET
        if isinstance(project, EnvPath):
            return replace(project, text=posixpath.join(project.text, target, MANIFEST))
        text = text_of(project)
        if text is None:
            return _not_known(project)
        return ManifestPath(
            posixpath.normpath(posixpath.join(text, target, MANIFEST)),
            project=text,
            conf=weakest(project.conf if project else "inferred", "inferred"),
            notes=project.notes if project else (),
        )

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
        last = fname.rsplit(".", 1)[-1]
        if fname == "os.fspath" and len(args) == 1:
            return args[0] if isinstance(args[0], (PathV, Const, EnvPath)) else None
        if not fname.startswith("dagster"):
            return None
        if last == "file_relative_path" and len(args) == 2:
            base, relative = text_of(args[0]), as_str(args[1])
            if base is None or relative is None or isinstance(args[0], EnvPath):
                return Unknown("file_relative_path() of a value not known", line=node.lineno)
            joined = posixpath.normpath(posixpath.join(posixpath.dirname(base), relative))
            return Const(joined, conf=weakest(args[0].conf, args[1].conf, "inferred"))
        if last in LEGACY:
            return self._legacy(last, node, args, kwargs, has_star, ctx)
        parts = fname.split(".")
        if len(parts) >= 2 and parts[-2] == "AssetSelection":
            return selections.constructor(last, args, kwargs, node.lineno)
        if last == "build_dbt_asset_selection":
            return selections.dbt_selection(args, kwargs, node.lineno)
        if last == "define_asset_job":
            return self._job(node, args, kwargs, has_star, ctx)
        if last == "AssetSpec":
            return self._spec(node, args, kwargs)
        if last == "ScheduleDefinition":
            return self._schedule_definition(node, kwargs, ctx)
        if last == "build_schedule_from_dbt_selection":
            return self._dbt_schedule(node, args, kwargs, ctx)
        if last == "Definitions":
            self._load(node, args, kwargs, ctx)
            return None
        if last in FROM_MODULES:
            return ListV([self._from_modules(last, node, scope)])
        if last == "with_resources" and args:
            return args[0]
        return None

    def _legacy(
        self,
        api: str,
        node: ast.Call,
        args: list[Val],
        kwargs: dict[str, Val],
        has_star: bool,
        ctx: Ctx,
    ) -> Val:
        """``load_assets_from_dbt_project`` and ``load_assets_from_dbt_manifest`` (D-c)."""
        params = dict(kwargs)
        if args:
            params.setdefault(LEGACY[api], args[0])
        if has_star:
            params["**"] = Unknown("arguments from a * or ** value")
        name = as_str(kwargs.get("op_name"))
        if name is None and self._target is not None and id(node) in self._target[1]:
            name = self._target[0]
        key = self._key(node.lineno)
        self._definition(api, name or f"{api}_{node.lineno}", node, params, ctx, key, [])
        # They return a list of asset definitions
        return ListV([AssetsV(f"dagster_dbt.{api}", args, kwargs, line=node.lineno, key=key)])

    def _load(self, node: ast.AST, args: list[Val], kwargs: dict[str, Val], ctx: Ctx) -> None:
        """A ``Definitions(…)`` call, or a ``@repository`` function: the definitions its assets
        list, by key or by module."""
        if not (self.emit and ctx.emit):
            return
        keys: set[str] = set()
        modules: set[str] = set()
        packages: set[str] = set()

        def collect(value: Val | None, depth: int = 0) -> None:
            if isinstance(value, (AssetsV, JobV, ScheduleV)):
                keys.add(value.key)
            elif isinstance(value, ModulesV):
                modules.update(value.modules)
                packages.update(value.packages)
            elif isinstance(value, ListV) and depth < 3:
                for item in value.items:
                    collect(item, depth + 1)

        given = kwargs.get("assets") or (args[0] if args else None)
        collect(given)
        for value in [kwargs.get(k) for k in ("assets", "jobs", "schedules")] + args[:1]:
            for item in value.items if isinstance(value, ListV) else [value]:
                if isinstance(item, Sym):
                    self._outside(item.name, node, ctx, "Definitions loads")
        for item in given.items if isinstance(given, ListV) else []:
            if isinstance(item, SpecV):
                # An asset declared by its spec alone, such as a dashboard downstream
                keys.add(item.key)
                self._spec_record("AssetSpec", item, node, ctx, item.key)
        collect(kwargs.get("jobs"))
        collect(kwargs.get("schedules"))
        self.loads.append(
            {
                "file": self.rel,
                "line": node.lineno,
                "keys": sorted(keys),
                "modules": sorted(modules),
                "packages": sorted(packages),
            }
        )

    def _from_modules(self, last: str, node: ast.Call, scope: Scope) -> ModulesV:
        """The modules and packages a ``load_assets_from_*`` call names, found in the workspace."""
        modules: list[str] = []
        packages: list[str] = []
        if last == "load_assets_from_current_module":
            # The module the call is written in, even in a helper followed from another file
            modules.append(self.index.relative(self.path))
        elif last == "load_assets_from_package_name":
            name = (
                as_str(self.eval(node.args[0], scope, self.ctx_type(emit=False)))
                if node.args
                else None
            )
            found = self.loader.find(name, self.path) if name else None
            if found is not None and os.path.basename(found) == "__init__.py":
                packages.append(self.index.relative(os.path.dirname(found)))
        else:
            given = (
                node.args[0]
                if node.args
                else next(
                    (kw.value for kw in node.keywords if kw.arg in ("modules", "package_module")),
                    None,
                )
            )
            exprs = (
                given.elts if isinstance(given, (ast.List, ast.Tuple)) else [given] if given else []
            )
            for expr in exprs:
                found = self._module_file(expr, scope)
                if found is None:
                    named = self._dotted_name(expr, scope) if expr is not None else None
                    if named:
                        self._outside(named, node, self.ctx_type(emit=self.emit), last)
                    continue
                if last == "load_assets_from_package_module" and found.endswith("__init__.py"):
                    packages.append(self.index.relative(os.path.dirname(found)))
                else:
                    modules.append(self.index.relative(found))
        return ModulesV(
            f"dagster.{last}",
            [],
            {},
            line=node.lineno,
            modules=tuple(modules),
            packages=tuple(packages),
        )

    def _outside(self, dotted: str, node: ast.AST, ctx: Ctx, how: str) -> None:
        """Something loaded from a module the workspace does not have, such as an installed
        package: what it holds is not read (``call_not_followed``)."""
        top = dotted.split(".", 1)[0]
        if "." not in dotted or top.startswith("dagster") or top in LIBRARIES:
            return  # a name of this file's own, such as a class, or a library's
        parts = dotted.split(".")
        for size in range(len(parts), 0, -1):
            if self.loader.find(".".join(parts[:size]), self.path) is not None:
                return  # in the workspace: read as it is
        key = self._key(getattr(node, "lineno", 0))
        record = self._record("Definitions", dotted, node, ctx, key, f"outside:{dotted}")
        if record is None:
            return
        record.update(kind="outside", of="location")
        reason = (
            f"{how} {dotted}, from a module that is not in the workspace; what it holds is not read"
        )
        self.located.append(unknown(record, UnknownCategory.NOT_FOLLOWED, reason))

    def _module_file(self, expr: ast.expr, scope: Scope) -> str | None:
        """The workspace file of a module named in code: ``assets`` after ``from . import
        assets``, or ``pkg.assets`` after ``import pkg.assets``."""
        if isinstance(expr, ast.Name) and expr.id in self.imports:
            module, attr, level = self.imports[expr.id]
            if attr:
                found = self.loader.find(f"{module}.{attr}" if module else attr, self.path, level)
                if found is not None:
                    return found
            return self.loader.find(module, self.path, level)
        dotted = self._dotted_name(expr, scope)
        return self.loader.find(dotted, self.path) if dotted else None

    # --- assets downstream (task 8.7) ---------------------------------------------------------
    def _asset(self, fv: FuncV, ctx: Ctx, key: str) -> None:
        """``@asset``: its key, its group, and what it depends on."""
        st, params = fv.node, fv.decorator_kwargs
        name = as_str(params.get("name")) or st.name
        given = params.get("key")
        if given is not None and not (isinstance(given, Const) and given.value is None):
            asset = dependencies.asset_key(given)
        else:
            prefix_v = params.get("key_prefix")
            prefix = (
                []
                if prefix_v is None or (isinstance(prefix_v, Const) and prefix_v.value is None)
                else dependencies.asset_key(prefix_v)
            )
            asset = None if prefix is None else [*prefix, name]
        deps, why = dependencies.dependencies(params.get("deps"))
        ins = params.get("ins")
        named: set[str] = set()
        if isinstance(ins, DictV):
            for input_name, value in ins.items.items():
                named.add(input_name)
                found = dependencies.asset_in(input_name, value)
                if isinstance(found, str):
                    why = why or found
                else:
                    deps += found
        elif ins is not None and not (isinstance(ins, Const) and ins.value is None):
            why = why or "ins is not a mapping known here"
        for input_name in _inputs(st, named):
            deps.append({"key": [input_name]})
        spec = SpecV(
            "dagster.asset",
            [],
            {},
            line=st.lineno,
            key=key,
            asset_key=asset or [],
            deps=deps,
            deps_unknown=why if asset is not None else "the asset's key is not known here",
            group=as_str(params.get("group_name")),
        )
        self._spec_record("@asset", spec, st, ctx, key)

    def _multi_asset(self, fv: FuncV, ctx: Ctx) -> None:
        """``@multi_asset(specs=[AssetSpec(…), …])``: each spec is an asset. Specs that are worked
        out, in a loop or a comprehension, cannot be read."""
        st, params = fv.node, fv.decorator_kwargs
        specs = params.get("specs")
        shared, why = dependencies.dependencies(params.get("deps"))
        items = specs.items if isinstance(specs, ListV) else None
        if items is None or not all(isinstance(item, SpecV) for item in items):
            if specs is None:
                return  # outs, not specs: what they name is their own
            record = self._record("@multi_asset", st.name, st, ctx, self._key(st.lineno), ASSET)
            if record is not None:
                record["kind"] = ASSET
                reason = specs.reason if isinstance(specs, Unknown) else "they are worked out"
                self.assets.append(
                    unknown(
                        record,
                        UnknownCategory.LOOP_OR_FACTORY,
                        f"the multi_asset's specs: {reason}",
                    )
                )
            return
        # Each is loaded where the function is: by its key, which a Definitions call lists
        key = self._key(st.lineno)
        for item in items:
            spec = replace(item, deps=[*item.deps, *shared], deps_unknown=item.deps_unknown or why)
            self._spec_record("@multi_asset", spec, st, ctx, key)

    def _spec(self, node: ast.Call, args: list[Val], kwargs: dict[str, Val]) -> Val:
        given = args[0] if args else kwargs.get("key")
        asset = dependencies.asset_key(given) if given is not None else None
        deps, why = dependencies.dependencies(kwargs.get("deps"))
        return SpecV(
            "dagster.AssetSpec",
            args,
            kwargs,
            line=node.lineno,
            key=self._key(node.lineno),
            asset_key=asset or [],
            deps=deps,
            deps_unknown=why if asset is not None else "the asset's key is not known here",
            group=as_str(kwargs.get("group_name")),
        )

    def _spec_record(self, api: str, spec: SpecV, node: ast.AST, ctx: Ctx, key: str) -> None:
        line = self._loc(getattr(node, "lineno", 0), getattr(node, "col_offset", 0))["line"]
        name = "/".join(spec.asset_key) or f"asset_{line}"
        record = self._record(api, name, node, ctx, key, f"{ASSET}:{name}")
        if record is None:
            return
        record.update(kind=ASSET, asset_key=list(spec.asset_key), group=spec.group, deps=spec.deps)
        if ctx.dynamic:
            record["dynamic"] = True
            unknown(record, UnknownCategory.LOOP_OR_FACTORY, ctx.dynamic)
            if spec.deps_unknown:
                # What it reads is not known either: it may read dbt (LP-ios)
                record["deps_unknown"] = spec.deps_unknown
        elif len(name) > MAX_NAME:
            unknown(record, UnknownCategory.LIMIT, "the asset's key is too long to show")
        elif spec.deps_unknown:
            record["deps_unknown"] = spec.deps_unknown
        self.assets.append(record)

    # --- jobs and schedules (task 8.6) ---------------------------------------------------------
    def _operator(
        self, op: ast.operator, left: Val, right: Val, node: ast.AST, ctx: Ctx
    ) -> Val | None:
        return selections.operator(op, left, right, getattr(node, "lineno", 0))

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
        if isinstance(base, SelectionV):
            return selections.method(base, meth, args, kwargs, node.lineno)
        return None

    def _job(
        self, node: ast.Call, args: list[Val], kwargs: dict[str, Val], has_star: bool, ctx: Ctx
    ) -> Val:
        """``define_asset_job(name, selection=…)``: a job, and its selection as a tree."""
        name_v = args[0] if args else kwargs.get("name")
        name = as_str(name_v)
        given = args[1] if len(args) > 1 else kwargs.get("selection")
        chosen = selections.selection_of(given, node.lineno)
        key = self._key(node.lineno)
        record = self._record(
            "define_asset_job", name or f"job_{node.lineno}", node, ctx, key, "job"
        )
        if record is not None:
            self._job_record(record, name is None, has_star, chosen, ctx)
        return JobV("dagster.define_asset_job", [], {}, line=node.lineno, key=key, name=name or "")

    def _job_record(
        self, record: dict[str, Any], unnamed: bool, has_star: bool, chosen: Val, ctx: Ctx
    ) -> None:
        record["kind"] = "job"
        if ctx.dynamic:
            record["dynamic"] = True
            unknown(record, UnknownCategory.LOOP_OR_FACTORY, ctx.dynamic)
        elif unnamed or has_star:
            unknown(record, UnknownCategory.NOT_LITERAL, "the job's name is not known here")
        elif len(record["name"]) > MAX_NAME:
            unknown(record, UnknownCategory.LIMIT, "the job's name is too long to show")
        elif isinstance(chosen, SelectionV):
            record["selection"] = chosen.tree
        else:
            reason = chosen.reason if isinstance(chosen, Unknown) else "it is not a selection"
            unknown(record, UnknownCategory.NOT_LITERAL, f"the job's selection: {reason}")
        self.jobs.append(record)

    def _schedule(
        self, api: str, name: str | None, node: ast.AST, params: dict[str, Val], ctx: Ctx, key: str
    ) -> ScheduleV:
        """A schedule of a job: ``ScheduleDefinition`` or ``@schedule``. Its job is a job value,
        or a name; a schedule of a job that is not an asset job the reader found is not one."""
        job = params.get("job")
        if isinstance(job, FuncV):
            # An op job, @job: it runs no asset, so this is no schedule of dbt assets
            return ScheduleV("dagster.ScheduleDefinition", [], {}, key=key, name=name or "")
        job_key = job.key if isinstance(job, JobV) else None
        job_name = (job.name or None) if isinstance(job, JobV) else as_str(params.get("job_name"))
        name = name or (f"{job_name}_schedule" if job_name else None)
        line = getattr(node, "lineno", 0)
        value = ScheduleV("dagster.ScheduleDefinition", [], {}, line=line, key=key, name=name or "")
        record = self._record(api, name or f"schedule_{line}", node, ctx, key, "schedule")
        if record is None:
            return value
        record.update(kind="schedule", job_key=job_key, job_name=job_name)
        cron = params.get("cron_schedule")
        record["cron"] = as_str(cron) if cron is not None else None
        if ctx.dynamic:
            record["dynamic"] = True
            unknown(record, UnknownCategory.LOOP_OR_FACTORY, ctx.dynamic)
        elif isinstance(job, Unknown) or (job is not None and not isinstance(job, JobV)):
            unknown(record, UnknownCategory.NOT_LITERAL, f"the schedule's job: {_why(job)}")
        elif name is None:
            unknown(record, UnknownCategory.NOT_LITERAL, "the schedule's name is not known here")
        self.schedules.append(record)
        return value

    def _schedule_definition(self, node: ast.Call, kwargs: dict[str, Val], ctx: Ctx) -> Val:
        name = as_str(kwargs.get("name"))
        key = self._key(node.lineno)
        return self._schedule("ScheduleDefinition", name, node, kwargs, ctx, key)

    def _dbt_schedule(
        self, node: ast.Call, args: list[Val], kwargs: dict[str, Val], ctx: Ctx
    ) -> Val:
        """``build_schedule_from_dbt_selection``: a job of a dbt selection, and its schedule."""
        names = ("dbt_assets", "job_name", "cron_schedule", "dbt_select", "dbt_exclude")
        params = {name: args[i] for i, name in enumerate(names) if i < len(args)}
        params.update(kwargs)
        chosen = selections.dbt_selection(
            [params["dbt_assets"]] if "dbt_assets" in params else [],
            {k: v for k, v in params.items() if k in ("dbt_select", "dbt_exclude", "dbt_selector")},
            node.lineno,
        )
        job_name = as_str(params.get("job_name"))
        job_key = f"{self._key(node.lineno)}:job"
        api = "build_schedule_from_dbt_selection"
        record = self._record(api, job_name or f"job_{node.lineno}", node, ctx, job_key, "job")
        if record is not None:
            self._job_record(record, job_name is None, False, chosen, ctx)
        job = JobV(
            "dagster.define_asset_job", [], {}, line=node.lineno, key=job_key, name=job_name or ""
        )
        name = as_str(params.get("schedule_name"))
        return self._schedule(api, name, node, {**params, "job": job}, ctx, self._key(node.lineno))

    # --- records ------------------------------------------------------------------------------
    def _record(
        self, api: str, name: str, node: ast.AST, ctx: Ctx, key: str, kind: str = "definition"
    ) -> dict[str, Any] | None:
        """A new record for a definition, a job or a schedule at ``node``, or None when there is
        none to make."""
        if not (self.emit and ctx.emit) or self.root is None:
            return None
        where = self._loc(node.lineno, node.col_offset)  # type: ignore[attr-defined]
        seen = (kind, where["line"], where["col"], ctx.call_sites)
        if seen in self._seen:
            return None
        self._seen.add(seen)
        return {
            "api": api,
            "file": where["file"],
            "line": where["line"],
            "name": name,
            "module": self._module(),
            "key": key,
            "notes": [],
        }

    def _unknown_at(
        self, api: str, name: str, node: ast.AST, ctx: Ctx, category: UnknownCategory, reason: str
    ) -> None:
        key = self._key(node.lineno)  # type: ignore[attr-defined]
        record = self._record(api, name, node, ctx, key)
        if record is not None:
            self.records.append(unknown(record, category, reason))

    def _definition(
        self,
        api: str,
        name: str,
        node: ast.AST,
        params: dict[str, Val],
        ctx: Ctx,
        key: str,
        notes: list[str],
    ) -> None:
        record = self._record(api, name, node, ctx, key)
        if record is None:
            return
        record["notes"] += notes
        if ctx.dynamic:
            record["dynamic"] = True
            self.records.append(unknown(record, UnknownCategory.LOOP_OR_FACTORY, ctx.dynamic))
        elif len(name) > MAX_NAME:
            self.records.append(
                unknown(record, UnknownCategory.LIMIT, "the definition's name is too long to show")
            )
        elif self._resolve(record, api, params) is not None:
            self.records.append(record)

    def _resolve(
        self, record: dict[str, Any], api: str, params: dict[str, Val]
    ) -> dict[str, Any] | None:
        """The record, resolved or unknown; None if it is another dbt project's."""
        if "**" in params:
            return unknown(record, UnknownCategory.NOT_LITERAL, params["**"].reason)  # type: ignore[attr-defined]
        raw, used, why = _project_path(api, params)
        if raw is None:
            return unknown(record, UnknownCategory.NOT_LITERAL, why)
        match = match_project(record, raw, None, self.index, self.root or "")
        if match is None:
            return record if record.get("status") == "unknown" else None
        selection: dict[str, str] = {}
        for key in SELECTION_KEYS:
            text, reason = _selection(params.get(key), DEFAULT_SELECT if key == "select" else "")
            if text is None:
                return unknown(record, UnknownCategory.NOT_LITERAL, f"{key} is not known: {reason}")
            selection[key] = text
        # How sure the values are, and what they say about themselves, such as a guess
        conf, notes = collect_meta(used, *(params.get(key) for key in SELECTION_KEYS))
        record["notes"] += [note for note in notes if note not in record["notes"]]
        rule = _key_rule(api, params)
        found = select_nodes(record, match, selection, self.index, conf, rule)
        if found is not None and api in LEGACY and found.get("status") == "resolved":
            # dagster-dbt 0.29.29 no longer has these loaders, and the groups they gave changed
            # from version to version: a selection by group cannot rely on them (LP-xpx)
            found["groups_unknown"] = LEGACY_GROUPS
        return found


def _key_rule(api: str, params: dict[str, Val]) -> KeyRule:
    """What sets a definition's asset keys (task 8.5): dagster-dbt's default, a legacy loader's
    literal ``key_prefix``, or code that runs only when Dagster loads, which cannot be read."""
    translator = params.get("dagster_dbt_translator")
    if translator is not None and not (isinstance(translator, Const) and translator.value is None):
        if not (
            isinstance(translator, CallV)
            and translator.func.startswith("dagster_dbt")
            and translator.func.rsplit(".", 1)[-1] == "DagsterDbtTranslator"
        ):
            return KeyRule(unknown=f"the asset keys come from {_described(translator)}")
    if api not in LEGACY:
        return DEFAULT
    if params.get("node_info_to_asset_key") is not None:
        return KeyRule(unknown="the asset keys come from node_info_to_asset_key, a function")
    prefix_v = params.get("key_prefix")
    if prefix_v is None or (isinstance(prefix_v, Const) and prefix_v.value is None):
        return DEFAULT
    literal: Any = as_str(prefix_v)
    if isinstance(prefix_v, ListV):
        literal = [as_str(item) for item in prefix_v.items]
    prefix = prefix_of(literal)
    if prefix is None:
        return KeyRule(unknown="key_prefix is not a literal string or list of strings")
    return KeyRule(prefix=prefix)


def _described(value: Val) -> str:
    if isinstance(value, CallV):
        return f"{value.func.rsplit('.', 1)[-1] or 'a call'}(), a translator of the workspace's own"
    if isinstance(value, Sym):
        return f"{value.name.rsplit('.', 1)[-1]}, a translator of the workspace's own"
    return "a translator that is not known here"


# Parameters of an @asset function that are not inputs
_NOT_INPUTS = ("context", "config")


def _inputs(st: ast.FunctionDef | ast.AsyncFunctionDef, named: set[str]) -> list[str]:
    """The parameters Dagster makes inputs of, keyed by their names: all but the context, the
    config, those given in ``ins``, and resources, which are annotated with a resource's type."""
    found = []
    for position, arg in enumerate([*st.args.posonlyargs, *st.args.args, *st.args.kwonlyargs]):
        if arg.arg in named or arg.arg in _NOT_INPUTS:
            continue
        annotation = ast.unparse(arg.annotation) if arg.annotation is not None else ""
        if position == 0 and "Context" in annotation:
            continue
        if annotation.rsplit(".", 1)[-1].endswith("Resource"):
            continue
        found.append(arg.arg)
    return found


def _not_known(value: Val | None) -> Unknown:
    reason = value.reason if isinstance(value, Unknown) else "it is not a path"
    return Unknown(f"DbtProject's project_dir is not known: {reason}")


def _made_by(value: ast.expr) -> frozenset[int]:
    """The nodes whose definitions an assignment names: the value, and what a
    ``with_resources(…)`` call wraps, as Dagster 0.x projects wrap a legacy loader's assets."""
    found = {id(value)}
    if isinstance(value, ast.Call) and value.args:
        func = value.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if name == "with_resources":
            found.add(id(value.args[0]))
    return frozenset(found)


def _env_test(test: ast.expr, scanner: DagsterScanner, scope: Scope) -> tuple[str, bool] | None:
    """A test of whether an environment variable is set, with no default given: (its name,
    whether the test is true when the variable is set). None for any other test."""
    negated = False
    if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
        test, negated = test.operand, True
    name = None
    if isinstance(test, ast.Call) and len(test.args) == 1 and not test.keywords:
        called = scanner._dotted_name(test.func, scope) or ""
        if called in _ENV_READS or called.endswith(".environ.get"):
            name = _literal(test.args[0])
    elif (
        isinstance(test, ast.Compare)
        and len(test.ops) == 1
        and isinstance(test.ops[0], (ast.In, ast.NotIn))
        and (scanner._dotted_name(test.comparators[0], scope) or "").endswith("os.environ")
    ):
        name = _literal(test.left)
        negated = negated != isinstance(test.ops[0], ast.NotIn)
    return (name, not negated) if name is not None else None


def _literal(node: ast.expr) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _project_path(api: str, params: dict[str, Val]) -> tuple[str | None, Val | None, str]:
    """The dbt project a definition is for, as a path to match: (the path, the value it was read
    from, why there is none)."""
    project = params.get("project") if api == API else params.get("project_dir")
    if project is not None and not (isinstance(project, Const) and project.value is None):
        if isinstance(project, CallV) and project.func.rsplit(".", 1)[-1] == "DbtProject":
            project = project.arg("project_dir", 0)
        text = text_of(project)
        if text is None:
            return None, None, f"the dbt project is not known: {_why(project)}"
        return text, project, ""
    manifest = params.get("manifest")
    if manifest is None:
        return None, None, "it names no manifest and no dbt project"
    if isinstance(manifest, ManifestPath):
        return manifest.project, manifest, ""
    text = text_of(manifest)
    if text is None:
        if isinstance(manifest, DictV):
            return None, None, "the manifest is a mapping built in Python"
        return None, None, f"the manifest is not known: {_why(manifest)}"
    # <project>/<target>/manifest.json
    return posixpath.dirname(posixpath.dirname(text)) or ".", manifest, ""


def _why(value: Val | None) -> str:
    if isinstance(value, Unknown):
        return value.reason
    if isinstance(value, CallV):
        return f"it is what {value.func or 'a call'}() returns"
    return "it is not a path"


def _selection(value: Val | None, default: str) -> tuple[str | None, str]:
    """A selection argument as one string: dbt reads a list's items as a union, as it does the
    words of a string. (None, why) when it is not literal."""
    if value is None or (isinstance(value, Const) and value.value is None):
        return default, ""
    if isinstance(value, ListV):
        items = [as_str(item) for item in value.items]
        if any(item is None for item in items):
            return None, "an item of the list is not a string"
        text = " ".join(items)  # type: ignore[arg-type]
    else:
        text = as_str(value)  # type: ignore[assignment]
        if text is None:
            return None, value.reason if isinstance(value, Unknown) else "it is not a string"
    if len(text) > MAX_SELECTION:
        return None, "it is too long"
    return text.strip(), ""


@dataclass
class PythonRead:
    """What the Python files hold, each record without its code location yet."""

    definitions: list[dict[str, Any]] = field(default_factory=list)
    jobs: list[dict[str, Any]] = field(default_factory=list)
    schedules: list[dict[str, Any]] = field(default_factory=list)
    assets: list[dict[str, Any]] = field(default_factory=list)  # not dbt's (task 8.7)
    located: list[dict[str, Any]] = field(default_factory=list)  # unknowns of 8.8
    loads: list[dict[str, Any]] = field(default_factory=list)  # the Definitions calls
    hashes: dict[str, str] = field(default_factory=dict)  # each file's SHA-256
    problems: list[str] = field(default_factory=list)


def _called(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A factory read where it is called is not also an unknown where it is defined: its
    definitions' keys start with where they are written (``_key``)."""
    called = {_written_at(r["key"]) for r in records if not r.get("dynamic")}
    return [r for r in records if not (r.get("dynamic") and _written_at(r["key"]) in called)]


def _written_at(key: str) -> str:
    return key.split("@", 1)[0]


def read_python(space: str, root: str, files: list[str], index: ProjectIndex) -> PythonRead:
    """The dbt asset definitions, asset jobs and schedules in ``files`` (relative to ``space``)
    that are for the dbt project at ``root``."""
    loader = ModuleLoader(space, index, DagsterScanner)
    read = PythonRead()
    for relative in files:
        scanner = DagsterScanner(os.path.join(space, relative), index, loader)
        scanner.root = root
        try:
            scanner.run()
        except Exception as exc:  # a bug here must cost one file, and say so
            read.problems.append(
                f"{relative}: the reader failed on this file: {type(exc).__name__}"
            )
            continue
        if scanner.sha256:
            read.hashes[scanner.rel] = scanner.sha256
        read.definitions += scanner.records
        read.jobs += scanner.jobs
        read.schedules += scanner.schedules
        read.assets += scanner.assets
        read.located += scanner.located
        read.loads += scanner.loads
        read.problems += [f"{u['file']}:{u['line']}: {u['reason']}" for u in scanner.unresolved]
    read.definitions = _called(read.definitions)
    read.jobs = _called(read.jobs)
    read.schedules = _called(read.schedules)
    # A spec that a multi_asset and a Definitions call both list is one asset
    seen: set[tuple[str, str]] = set()
    unique = []
    for record in _called(read.assets):
        mark = (record["key"], record["name"])
        if mark not in seen:
            seen.add(mark)
            unique.append(record)
    read.assets = unique
    return read
