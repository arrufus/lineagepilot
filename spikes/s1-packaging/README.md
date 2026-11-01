# Spike S1: packaging

**Question.** Can one universal VSIX carry the Python language server's dependencies as pure Python, and start a pygls server that returns a CodeLens on macOS, Windows and Linux, with Python 3.10 and 3.13?

**Answer.** Yes, on all three operating systems.

- **Locally:** the VSIX, built once on macOS, passed on macOS arm64 and Linux arm64 inside VS Code 1.144.0.
- **In CI:** a VSIX built once on Linux passed on Windows, macOS and Ubuntu with Python 3.10 and 3.13. That was GitHub Actions run 41317287230, on commit 04cfa69: 54 of 54 tests.

Decision D-2 (a Python language server) holds.

## Done-when criteria

| Criterion | Status | Evidence |
|---|---|---|
| Every runtime dependency installs as pure Python with `--implementation py` | **Met, with one change** | 8 packages, 0 compiled files. PyYAML needs a pure build from its sdist (finding 2). |
| Universal VSIX | **Met** | 1.36 MB packed, 384 files, no `TargetPlatform`. `scripts/check_vsix.py` enforces it. |
| Interpreter found via `@vscode/python-extension` | **Met** | `python-extension` runs pass on macOS and Linux, on 3.10 and 3.13 |
| CodeLens on macOS, Python 3.10 and 3.13 | **Met** | 6 VS Code runs, 18/18 tests |
| CodeLens on Linux, Python 3.10 and 3.13 | **Met** | 6 VS Code runs on arm64, 18/18 tests. In CI on x86_64: 18/18. |
| CodeLens on Windows, Python 3.10 and 3.13 | **Met** | CI on windows-latest: 6 VS Code runs, 18/18 tests |

## Results

The **source** column says how the extension found the interpreter: the machine-scoped setting, the Python extension's active environment, or `python3` on `PATH`.

| OS | Source | Python | VS Code tests |
|---|---|---|---|
| macOS arm64 | setting | 3.10.17, 3.13.3 | 3/3, 3/3 |
| macOS arm64 | python-extension | 3.10.17, 3.13.3 | 3/3, 3/3 |
| macOS arm64 | path | 3.11.4 (pyenv, from the login shell) | 3/3, 3/3 |
| Linux arm64 | setting | 3.10.17, 3.13.3 | 3/3, 3/3 |
| Linux arm64 | python-extension | 3.10.17, 3.13.3 | 3/3, 3/3 |
| Linux arm64 | path | 3.11.2 (`/usr/bin/python3`) | 3/3, 3/3 |
| Linux x86_64 | stdio tests only | 3.10.22, 3.13.18 | 28 passed each |
| Windows (CI) | setting, python-extension | 3.10.11, 3.13.16 | 3/3 each |
| Windows (CI) | path (`py -3`) | 3.15.x, the runner's default | 3/3, 3/3 |
| macOS arm64 (CI) | setting, python-extension | 3.10.11, 3.13.17 | 3/3 each |
| macOS arm64 (CI) | path | 3.15.1 (Homebrew) | 3/3, 3/3 |
| Ubuntu x86_64 (CI) | all three | 3.10.22, 3.13.17 | 9/9, 9/9 |

The first attempt of the Ubuntu 3.13 CI job timed out downloading VS Code, before any test ran; the re-run passed. The workflow now pins VS Code 1.144.0, and retries the download and nothing else.

The three VS Code tests check three things:
- The packaged server returns the CodeLens `2 upstream refs, 2 tables | spike_shop | Python x.y.z`. Building that title uses the bundled pygls, sqlglot and PyYAML.
- `pygls`, `sqlglot` and `yaml` were imported from the VSIX's `bundled/libs`, under isolated mode, with no libyaml.
- A workspace `.vscode/settings.json` that sets the interpreter is ignored.

**Startup**, from spawn to the first CodeLens, median of 5 runs on an M4 Max (`results/startup_macos_arm64.json`):

| Python | Cold (no bytecode yet) | Warm |
|---|---|---|
| 3.10.17 | 431 ms | 237 ms |
| 3.13.3 | 478 ms | 228 ms |

**Tests and coverage**:
- Python: 48 tests, 90% line coverage. This counts the network-marked bundling test.
- Extension unit tests (`node:test`): 15 tests. `interpreter.ts` has 95.7% line and 87.3% branch coverage.

## Findings that change the design or plan

1. **Design section 4.4's bundle command is not enough.** `--implementation py --no-deps` still lets pip build sdists. PyYAML came out as `cp310-cp310-macosx_11_0_arm64`, with a macOS-only `_yaml.cpython-310-darwin.so` inside a "universal" bundle. The command must add `--only-binary=:all:`, and the bundle must be checked after install. `scripts/bundle_libs.py` does both.
2. **PyYAML has no pure wheel.** The bundler builds one from the hash-pinned sdist, with `PYYAML_FORCE_LIBYAML=0` and no build isolation. The only build dependency is a hash-pinned setuptools; Cython is not needed. The result is `py3-none-any`. If this proves fragile, ruamel.yaml without its optional C extension is the fallback.
3. **Environment markers must be ignored when bundling.** `exceptiongroup` is locked as `python < 3.11`. A bundle built on 3.13 would leave it out and then break on 3.10. The bundler installs every locked package, so the bundle is the same whichever Python builds it.
4. **pygls 2.1.1 cannot decode a request with no `params` field.** vscode-languageclient omits `params` when none are given. pygls then fails inside its decoder and never replies, so the request hangs. For Phase 2.3, the `lineage/*` JSON Schema should require a params object on every request. The pygls client sends `"params": null` instead, which is why server-only tests missed this.
5. **Restricted mode must never run a workspace interpreter, not even to probe it.** A cloned repository can commit a `.venv`, and the Python extension may select it. The resolver skips any candidate inside a workspace folder while the workspace is untrusted, both before probing it and after resolving `sys.executable`. Design section 10 should say so.
6. **Machine scope works.** VS Code drops a workspace value for `lineagePilot.python.interpreterPath` (`workspaceValue` is `undefined`). As defence in depth, the extension reads only the user value.
7. **`python -I` and bundled libraries first on `sys.path`** stop a workspace `yaml.py` and a planted `PYTHONPATH` package from shadowing the bundle. Tests cover both. The server's working directory is the extension's, never the workspace.
8. **"`python3` on `PATH`" means the login-shell `PATH` on macOS.** VS Code rebuilds `PATH` from the user's shell, so the resolver found pyenv's 3.11.4 here. That is what users expect, but it cannot be pinned in tests; the test compares the result with a lookup made inside the extension host.
9. **Startup is not a risk.** A cold start costs under 0.5 s even when the extension directory is read-only. The VSIX should not ship `.pyc` files: they are version-specific. `prepackage` deletes the ones that local runs leave behind.
10. **Tooling for later phases:**
    - npm 10.9.3 crashes on vitest 4.1's peer dependencies, and vitest 3.2.x has two critical advisories. The extension's unit tests therefore use `node:test`, with no extra dependencies.
    - The Phase 3 webview needs vitest with React Testing Library. Revisit with vitest ≥ 4.1.12 and a newer npm.
11. **Supply chain status:**
    - Python runtime and build dependencies are hash-pinned, and the build venv's pip is replaced from the lock.
    - `npm audit`: 0 critical, 0 high. Two moderate production advisories come from `minimatch` via vscode-languageclient 10.1.2; it only matches our own globs against file paths. Re-check with languageclient 11 in Phase 2.
    - GitHub Actions are pinned by commit SHA.
12. **Harness gotchas for Phase 2:**
    - When tests are launched from inside VS Code, clear `ELECTRON_RUN_AS_NODE` and `VSCODE_*`.
    - Keep `--user-data-dir` short, because macOS limits socket paths to 103 characters.
    - Create venvs with symlinks, because a copied uv-managed interpreter aborts on macOS.

## Reproduce

Prerequisites: Node 22, uv 0.7.6, Python 3.10 and 3.13 (`uv python install 3.10 3.13`), and Docker for the Linux run.

```bash
cd lineagepilot/spikes/s1-packaging
npm ci
python3 scripts/bundle_libs.py               # pure bundle into bundled/libs, checked
npm run package                              # dist/lineagepilot-spike.vsix
python3 scripts/check_vsix.py dist/lineagepilot-spike.vsix
npm run test:unit                            # resolver unit tests with coverage

P10=$(UV_PYTHON_PREFERENCE=only-managed uv python find 3.10)
P13=$(UV_PYTHON_PREFERENCE=only-managed uv python find 3.13)
LP_TEST_PYTHONS="$P10:$P13" LP_NETWORK_TESTS=1 uv run --python 3.13 pytest --cov

npm run compile-tests
for s in setting path python-extension; do for p in $P10 $P13; do
  node scripts/run_integration.mjs --python $p --source $s --vsix dist/lineagepilot-spike.vsix
done; done

docker build -t lineagepilot-s1-linux -f docker/Dockerfile.linux docker
docker run --rm -v "$PWD":/src:ro lineagepilot-s1-linux bash /src/docker/run_linux_tests.sh

uv run --python 3.13 python scripts/measure_startup.py --python $P10 --python $P13
```

To regenerate the lock files, use the commands at the top of `requirements.in` and `build-requirements.in`.

## Layout

| Path | What it is |
|---|---|
| `requirements.in`, `requirements.lock` | Runtime dependencies, hash-pinned, at least two weeks old (`--exclude-newer`) |
| `build-requirements.in`, `build-requirements.lock` | pip and setuptools for the bundler's throwaway venv |
| `scripts/bundle_libs.py` | Builds and checks the pure-Python bundle, and writes `BUNDLE.json` |
| `scripts/check_vsix.py` | VSIX gate: universal, no compiled files, expected contents, size budget |
| `scripts/run_integration.mjs` | Runs the suite in real VS Code against the source folder or an unpacked VSIX |
| `scripts/measure_startup.py` | Cold and warm startup timing |
| `bundled/tool/` | Server entry point (`lsp_server.py`) and the spike server |
| `src/interpreter.ts` | Interpreter resolution: setting → Python extension → `PATH`, with the restricted-mode rule |
| `src/extension.ts` | Activation, language client, status command |
| `src/test/` | `node:test` unit tests and the in-VS Code mocha suite |
| `tests/` | pytest: the bundler, the VSIX gate, and the server over stdio (pytest-lsp) |
| `fixture-workspace/` | A tiny dbt project, plus a hostile `.vscode/settings.json` and `yaml.py` |
| `docker/` | Linux test image and runner |
| `../../../.github/workflows/lineagepilot-s1.yml` | CI: build once on Linux, then test on 3 operating systems × 2 Pythons |

## Not done

- **The uv-managed interpreter fallback**, step 4 of the resolution order. It is out of scope for the spike and moves to Phase 2.4.
- **A restricted-mode run in real VS Code.** The trust rule is unit-tested only. Phase 2.5 adds the integration test.
