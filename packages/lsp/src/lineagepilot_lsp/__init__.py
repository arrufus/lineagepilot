"""LineagePilot language server: lineagepilot_core behind LSP, on stdio.

One server runs per dbt project (design section 4). It indexes the project in a child
process, keeps the graph in memory, and answers CodeLens, hover, diagnostics and the
``lineage/*`` requests.
"""

__version__ = "0.1.0.dev0"
