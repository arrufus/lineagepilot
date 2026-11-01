#!/usr/bin/env bash
# Runs inside the Linux test image. /src is the spike folder, mounted read-only, with
# bundled/libs and dist/lineagepilot-spike.vsix already built (on any OS).
set -euo pipefail

mkdir -p "$HOME/work"
tar -C /src --exclude=.venv --exclude=.vscode-test -cf - . | tar -C "$HOME/work" -xf -
cd "$HOME/work"

P10=$(uv python find 3.10)
P13=$(uv python find 3.13)
echo "[linux] $(uname -sm), Python 3.10 at $P10, Python 3.13 at $P13"

echo "[linux] server tests over stdio, bundle built elsewhere"
UV_PROJECT_ENVIRONMENT="$HOME/venv" LP_TEST_PYTHONS="$P10:$P13" uv run --python 3.13 pytest -q -p no:cacheprovider

echo "[linux] VS Code integration tests on the packaged VSIX"
status=0
for source in setting path python-extension; do
  for py in "$P10" "$P13"; do
    if ! xvfb-run -a node scripts/run_integration.mjs --python "$py" --source "$source" \
         --vsix dist/lineagepilot-spike.vsix; then
      echo "[linux] FAILED: source=$source python=$py"
      status=1
    fi
  done
done
exit $status
