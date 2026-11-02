# lineagepilot_core

The editor-agnostic core of LineagePilot: parsers, the metadata graph, analysis (lineage, impact, rules, PII) and exporters. The language server (`packages/lsp`) and the headless CLI both run on it. See [design section 4.1](../../docs/design.md#41-components).

## Develop

```sh
cd packages/core
export UV_PYTHON_PREFERENCE=only-managed   # pyproject.toml also sets this
uv sync --locked
uv run pytest                              # tests, the import contract and the 80% coverage gate
uv run ruff check . && uv run ruff format --check .
```

To run the tests on Python 3.10 as well:

```sh
UV_PROJECT_ENVIRONMENT=work/venv-py310 uv run --python 3.10 pytest
```

CI runs the same commands on Linux, macOS and Windows with Python 3.10 and 3.13 ([workflow](../../.github/workflows/lineagepilot-core.yml)).

## Rules this package keeps

- **No server stack.** An `import-linter` contract in `pyproject.toml` forbids `sqlalchemy`, `fastapi` and `pydantic` (design section 13). `tests/test_package.py` runs the contract. It also imports every module in a fresh interpreter and checks that nothing from design section 4.4's dropped list gets loaded.
- **Locked with hashes.** `uv.lock` pins every package by sha256. CI runs `uv sync --locked`, which fails when the lock is stale, and audits the locked set with `pip-audit --require-hashes`.
- **Releases at least two weeks old.** `exclude-newer` in `pyproject.toml` stops the resolver picking anything newer than a fixed date. To take a newer release, move that date forward in its own commit, then re-run `uv lock` and the audit.
- **One sqlglot major.** sqlglot is pinned to `>=30.23,<31`, because its lineage API changes between majors (S2 finding 9).
- **No `print()`.** The language server talks over stdio, so any stray output corrupts the protocol. Ruff rule T20 enforces this.
