"""The named reasons a DAG file cannot be fully read (design section 6.2, task 5.4).

A dynamic DAG produces an ``unknown`` record at the call site rather than a guess. Each record
carries one of these categories, so the review can say in one line what happened and what to do
about it. Every category can be answered in the annotation file (task 5.2).
"""

from __future__ import annotations

from enum import Enum

ANNOTATE = "Declare what it runs in .lineagepilot/airflow.yml."


class UnknownCategory(str, Enum):
    REMOTE_MANIFEST = "remote_manifest"
    MANIFEST_GENERATED = "manifest_generated"
    WRAPPER = "wrapper"
    LOOP_OR_FACTORY = "loop_or_factory"
    GENERATED_PROJECT = "generated_project"
    UNSUPPORTED_SELECTOR = "unsupported_selector"
    JINJA = "jinja"
    DAG_NOT_LOADABLE = "dag_not_loadable"
    NOT_FOLLOWED = "call_not_followed"
    SHELL_SCRIPT = "shell_script"
    PYTHON_DBT = "python_dbt_call"
    OTHER_OPERATOR = "other_operator"
    NOT_LITERAL = "not_literal"
    NO_PROJECT = "no_project"
    DEPENDENCY = "dependency"
    SQL = "sql"
    LIMIT = "limit"


# (a short label, one line saying why it cannot be read)
_EXPLANATIONS = {
    UnknownCategory.REMOTE_MANIFEST: (
        "remote manifest",
        "Cosmos reads this manifest from remote storage, which is not read offline.",
    ),
    UnknownCategory.MANIFEST_GENERATED: (
        "tasks generated from a manifest",
        "The DAG builds its tasks from manifest.json when Airflow parses it.",
    ),
    UnknownCategory.WRAPPER: (
        "wrapper",
        "A class or helper in the workspace builds operators from values that cannot be read.",
    ),
    UnknownCategory.LOOP_OR_FACTORY: (
        "loop or factory",
        "Tasks are created a number of times that is only known when the file runs.",
    ),
    UnknownCategory.GENERATED_PROJECT: (
        "generated project",
        "The dbt project has no model files in the workspace; they are generated at build time.",
    ),
    UnknownCategory.UNSUPPORTED_SELECTOR: (
        "unsupported selector",
        "This selector needs state dbt has at run time, or a method the scanner does not support.",
    ),
    UnknownCategory.JINJA: (
        "Jinja",
        "A template in the command is only filled in when the task runs.",
    ),
    UnknownCategory.DAG_NOT_LOADABLE: (
        "DAG cannot load",
        "The file cannot be parsed, so Airflow could not load it either.",
    ),
    UnknownCategory.NOT_FOLLOWED: (
        "call not followed",
        "A function that creates operators is called, and the scanner does not follow that call.",
    ),
    UnknownCategory.SHELL_SCRIPT: (
        "shell script",
        "The task runs a script, and scripts are not analysed.",
    ),
    UnknownCategory.PYTHON_DBT: (
        "dbt from Python",
        "dbt is called from Python code inside a task, which is not analysed.",
    ),
    UnknownCategory.OTHER_OPERATOR: (
        "other operator",
        "This operator mentions dbt, but what it runs cannot be read from the DAG file.",
    ),
    UnknownCategory.NOT_LITERAL: (
        "value not known",
        "A value this link needs cannot be worked out from the source.",
    ),
    UnknownCategory.NO_PROJECT: (
        "no dbt project",
        "The path in the DAG file matches no dbt project in the workspace; map it if it moved.",
    ),
    UnknownCategory.DEPENDENCY: (
        "task order",
        "An operand of a task dependency is not a task the scanner knows.",
    ),
    UnknownCategory.SQL: (
        "SQL not read",
        "The SQL cannot be parsed, or a table name in it is only known at run time.",
    ),
    UnknownCategory.LIMIT: (
        "limit reached",
        "The file or the workspace is over a limit that keeps the analysis safe.",
    ),
}

# Most specific first: which category a link with several reasons is filed under
PRIORITY = tuple(_EXPLANATIONS)


def label(category: str) -> str:
    """A short name for a category, for the Unknowns section."""
    return _EXPLANATIONS[UnknownCategory(category)][0]


def explanation(category: str) -> str:
    """One line for the UI: what happened, and the way out."""
    return f"{_EXPLANATIONS[UnknownCategory(category)][1]} {ANNOTATE}"


def most_specific(categories: list[str]) -> str:
    return min(categories, key=lambda c: PRIORITY.index(UnknownCategory(c)))
