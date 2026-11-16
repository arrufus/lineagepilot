"""Readers of the tools beside dbt, each in its own package, on shared reading code (design §7.2).

``common`` holds what any reader uses: bounded file access inside the workspace, the safe
evaluator's values, the dbt command parser, the selector engine with its small manifest and
stand-in manifest, project discovery, and the named categories of what could not be read.
"""
