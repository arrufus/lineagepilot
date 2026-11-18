"""Readers: each reads one tool's files as data and links what it finds to the dbt graph.

``common`` holds what every reader shares: reading untrusted files inside the workspace
(``safe_io``), a safe evaluator for literal values (``values``), dbt in shell commands (``shell``),
dbt's node selection (``selector``) over a light view of a manifest (``manifest``, ``standin``),
the dbt projects in the workspace (``projects``), the named reasons a file cannot be read
(``categories``), and the workspace and its annotation file (``workspace``).

One package per tool: ``airflow`` is the first (design_next_phase section 7.2).
"""
