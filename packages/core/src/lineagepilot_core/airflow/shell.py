"""Find ``dbt`` invocations in shell command strings and argv lists.

Handles ``cd x && dbt ...``, several dbt commands in one string, global flags before the
sub-command (``dbt --no-write-json run``), env-var prefixes and absolute dbt executables.
Jinja (``{{ ... }}``) inside a selector-bearing flag makes that flag unknown.
"""

from __future__ import annotations

import posixpath
import re
import shlex
from dataclasses import dataclass, field

from lineagepilot_core.airflow.safe_io import is_absolute

JINJA_RE = re.compile(r"\{\{.*?\}\}|\{%.*?%\}", re.S)
JINJA_PLACEHOLDER = "__LP_JINJA_{}__"

# Sub-commands whose output builds or tests dbt nodes. Others are still reported.
BUILDING_COMMANDS = {"run", "build", "test", "seed", "snapshot"}
KNOWN_COMMANDS = BUILDING_COMMANDS | {
    "compile",
    "deps",
    "docs",
    "source",
    "run-operation",
    "ls",
    "list",
    "parse",
    "debug",
    "clean",
    "clone",
    "show",
    "retry",
    "init",
    "freshness",
    "snapshot-freshness",
}

# Flags that take exactly one value (we skip their value).
ONE_VALUE_FLAGS = {
    "--vars",
    "--state",
    "--defer-state",
    "--threads",
    "--target-path",
    "--log-path",
    "--log-level",
    "--log-format",
    "--profile",
    "--macro",
    "--args",
    "--output",
    "-o",
    "--output-keys",
    "--resource-type",
    "--resource-types",
    "--indirect-selection",
    "--favor-state",
    "--event-time-start",
    "--event-time-end",
    "--sample",
    "--printer-width",
    "--log-level-file",
    "--log-format-file",
    "--empty-catalog",
    "--warn-error-options",
    "--exclude-resource-type",
    "--exclude-resource-types",
    "--inline",
    "--limit",
    "--project-dir-override",
}
SELECT_FLAGS = {
    "--select": "select",
    "-s": "select",
    "--models": "select",
    "-m": "select",
    "--model": "select",
}
EXCLUDE_FLAGS = {"--exclude": "exclude"}
SINGLE_FLAGS = {
    "--selector": "selector",
    "--project-dir": "project_dir",
    "--profiles-dir": "profiles_dir",
    "--target": "target",
    "-t": "target",
}
SHELL_OPERATORS = {"&&", "||", ";", "|", "&", "\n", "(", ")"}
SHELL_VARIABLE = re.compile(r"\$\{?\w+")
# Commands that run another command elsewhere: dbt is in their arguments, not their first word
CONTAINER_COMMANDS = {"docker", "docker-compose", "podman", "kubectl", "ssh"}


@dataclass
class DbtInvocation:
    command: str | None
    select: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    selector: str | None = None
    project_dir: str | None = None
    profiles_dir: str | None = None
    target: str | None = None
    cwd: str | None = None
    used_models_flag: bool = False
    unknown_fields: dict[str, str] = field(default_factory=dict)
    raw: str = ""
    notes: list[str] = field(default_factory=list)
    # In another machine's or container's file system, so the working directory says nothing
    remote: bool = False

    def to_dict(self) -> dict:
        return {
            "dbt_command": self.command,
            "select": self.select,
            "exclude": self.exclude,
            "selector": self.selector,
            "project_dir": self.project_dir,
            "profiles_dir": self.profiles_dir,
            "target": self.target,
            "cwd": self.cwd,
            "unknown_fields": self.unknown_fields,
        }


def _protect_jinja(text: str) -> tuple[str, list[str]]:
    found: list[str] = []

    def repl(m: re.Match[str]) -> str:
        found.append(m.group(0))
        return JINJA_PLACEHOLDER.format(len(found) - 1)

    return JINJA_RE.sub(repl, text), found


def _restore(token: str, jinja: list[str]) -> str:
    for i, j in enumerate(jinja):
        token = token.replace(JINJA_PLACEHOLDER.format(i), j)
    return token


def _has_jinja(token: str) -> bool:
    return "__LP_JINJA_" in token or "{{" in token or "{%" in token


def tokenize_shell(text: str) -> list[str] | None:
    """Split a shell string into words and operators. None when it cannot be tokenised."""
    protected, _ = _protect_jinja(text)
    protected = protected.replace("\\\n", " ")
    lex = shlex.shlex(protected, posix=True, punctuation_chars=";&|()")
    lex.whitespace = " \t\r"
    lex.whitespace_split = True
    lex.commenters = ""
    tokens: list[str] = []
    try:
        for tok in lex:
            tokens.append(tok)
    except ValueError:
        return None
    # shlex does not split on newlines once whitespace excludes them; split manually.
    out: list[str] = []
    for tok in tokens:
        parts = tok.split("\n")
        for i, p in enumerate(parts):
            if i:
                out.append("\n")
            if p:
                out.append(p)
    return out


def _is_dbt_executable(token: str) -> bool:
    base = posixpath.basename(token)
    return base == "dbt" or base == "dbt.exe"


def _segments(tokens: list[str]) -> list[list[str]]:
    segs: list[list[str]] = [[]]
    for tok in tokens:
        if tok in SHELL_OPERATORS or set(tok) <= set(";&|()"):
            segs.append([])
        else:
            segs[-1].append(tok)
    return [s for s in segs if s]


def parse_dbt_argv(
    argv: list[str], jinja: list[str] | None = None, cwd: str | None = None
) -> DbtInvocation:
    """Parse the words after the ``dbt`` executable."""
    jinja = jinja or []
    inv = DbtInvocation(command=None, cwd=cwd, raw=" ".join(_restore(a, jinja) for a in argv))
    i = 0
    n = len(argv)

    def take_values(start: int) -> tuple[list[str], int]:
        vals: list[str] = []
        j = start
        while j < n and not argv[j].startswith("-"):
            vals.append(argv[j])
            j += 1
        return vals, j

    while i < n:
        tok = argv[i]
        flag, eq, inline_val = tok.partition("=") if tok.startswith("--") else (tok, "", "")
        if flag in SELECT_FLAGS or flag in EXCLUDE_FLAGS:
            key = SELECT_FLAGS.get(flag) or EXCLUDE_FLAGS[flag]
            if flag in ("--models", "-m", "--model"):
                inv.used_models_flag = True
            if eq:
                vals, i = [inline_val], i + 1
            else:
                vals, i = take_values(i + 1)
            for v in vals:
                real = _restore(v, jinja)
                if _has_jinja(v) or _has_jinja(real):
                    inv.unknown_fields[key] = f"Jinja template in --{key}: {real}"
                    continue
                if SHELL_VARIABLE.search(real):
                    inv.unknown_fields[key] = f"shell variable in --{key}: {real}"
                    continue
                # a single quoted argument may hold several space-separated specs
                getattr(inv, key).extend(real.split())
            continue
        if flag in SINGLE_FLAGS:
            key = SINGLE_FLAGS[flag]
            if eq:
                val: str | None = inline_val
                i += 1
            elif i + 1 < n:
                val = argv[i + 1]
                i += 2
            else:
                val = None
                i += 1
            if val is not None:
                real = _restore(val, jinja)
                if _has_jinja(val) or _has_jinja(real):
                    inv.unknown_fields[key] = f"Jinja template in {flag}: {real}"
                elif SHELL_VARIABLE.search(real):
                    if key in ("selector", "project_dir"):
                        inv.unknown_fields[key] = f"shell variable in {flag}: {real}"
                else:
                    setattr(inv, key, real)
            continue
        if flag in ONE_VALUE_FLAGS:
            i += 1 if eq else 2
            continue
        if tok.startswith("-"):
            i += 1
            continue
        if inv.command is None:
            word = _restore(tok, jinja)
            if _has_jinja(word):
                inv.unknown_fields["dbt_command"] = f"Jinja template as dbt sub-command: {word}"
                inv.command = "unknown"
            else:
                inv.command = word
                # `dbt docs generate`, `dbt source freshness`: keep the second word
                if word in ("docs", "source") and i + 1 < n and not argv[i + 1].startswith("-"):
                    inv.command = f"{word} {argv[i + 1]}"
                    i += 1
        i += 1
    return inv


def find_dbt_in_shell(text: str) -> tuple[list[DbtInvocation], str | None]:
    """Return dbt invocations in a shell string, plus an error reason if tokenising failed."""
    tokens = tokenize_shell(text)
    if tokens is None:
        if mentions_dbt(text):
            return [], "shell command mentions dbt but could not be tokenised"
        return [], None
    # tokenize_shell protects Jinja the same deterministic way; rebuild the placeholder list
    _, jinja = _protect_jinja(text)
    invocations: list[DbtInvocation] = []
    cwd: str | None = None
    unknown_cd: str | None = None
    for seg in _segments(tokens):
        words = list(seg)
        # env assignments and wrappers before the executable
        while words and (
            re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", words[0])
            or words[0] in ("env", "exec", "time", "sudo", "uv", "poetry", "pipenv")
        ):
            if words[0] in ("uv", "poetry", "pipenv") and len(words) > 1 and words[1] == "run":
                words = words[2:]
                continue
            words = words[1:]
        if not words:
            continue
        if words[0] == "cd" and len(words) >= 2:
            target = _restore(words[1], jinja)
            if _has_jinja(target) or SHELL_VARIABLE.search(target):
                cwd = None
                unknown_cd = target
            elif cwd and not is_absolute(target):
                cwd = posixpath.normpath(posixpath.join(cwd, target))
            else:
                cwd = target
            continue
        if (
            words[0] in ("bash", "sh", "zsh")
            and len(words) >= 3
            and words[1] in ("-c", "-lc", "-ec")
        ):
            inner, _ = find_dbt_in_shell(_restore(words[2], jinja))
            invocations.extend(inner)
            continue
        if (
            words[0] in ("python", "python3")
            and len(words) >= 3
            and words[1] == "-m"
            and words[2] == "dbt"
        ):
            words = ["dbt"] + words[3:]
        remote = words[0] in CONTAINER_COMMANDS
        if remote:
            # `docker compose run dbt dbt run`: the first may be the service's name
            at = next(
                (
                    i
                    for i, w in enumerate(words)
                    if i
                    and _is_dbt_executable(_restore(w, jinja))
                    and not (
                        i + 1 < len(words) and _is_dbt_executable(_restore(words[i + 1], jinja))
                    )
                ),
                None,
            )
            if at is None:
                continue
            words = words[at:]
        if _is_dbt_executable(_restore(words[0], jinja)):
            invocation = parse_dbt_argv(words[1:], jinja, None if remote else cwd)
            invocation.remote = remote
            if unknown_cd is not None:
                invocation.notes.append(f"working directory comes from {unknown_cd}, not known")
            invocations.append(invocation)
    return invocations, None


def find_dbt_in_argv(argv: list[str]) -> list[DbtInvocation]:
    """As :func:`find_dbt_in_shell`, for an exec-style argv (Kubernetes ``cmds + arguments``)."""
    if not argv:
        return []
    if (
        posixpath.basename(argv[0]) in ("bash", "sh", "zsh")
        and len(argv) >= 3
        and argv[1] in ("-c", "-lc", "-ec")
    ):
        found, _ = find_dbt_in_shell(" ".join(argv[2:]))
        return found
    if len(argv) == 1:
        found, _ = find_dbt_in_shell(argv[0])
        return found
    found, _ = find_dbt_in_shell(shlex.join(argv))
    return found


def mentions_dbt(text: str) -> bool:
    return bool(re.search(r"(^|[\s/;&|(\"'])dbt(\.exe)?(\s|$)", text))
