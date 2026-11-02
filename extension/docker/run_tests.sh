#!/usr/bin/env bash
# Runs inside the Linux test image. /src is the repository, mounted read-only, with
# extension/bundled/libs already built (it is pure Python, so any OS can build it).
#
#   LP_LABELS    integration configurations to run (default: trusted restricted path)
#   LP_PYTHONS   interpreters, by version (default: 3.13)
#   LP_COVERAGE  1 to also run the coverage configuration and its 75% gate
set -euo pipefail

mkdir -p "$HOME/work"
tar -C /src --exclude=node_modules --exclude=.vscode-test --exclude='.venv*' --exclude=out \
    --exclude=dist --exclude=coverage --exclude=work --exclude=.git -cf - . | tar -C "$HOME/work" -xf -
cd "$HOME/work/extension"
# The VS Code download is kept in a volume mounted at ~/cache, when there is one
mkdir -p "$HOME/cache/vscode-test" && ln -sfn "$HOME/cache/vscode-test" .vscode-test

npm ci --no-audit --no-fund --loglevel=error
npm run compile-tests >/dev/null
npm run compile >/dev/null
npm run test:unit
npm run test:webview-coverage

status=0
for version in ${LP_PYTHONS:-3.13}; do
  python=$(uv python find "$version")
  for label in ${LP_LABELS:-trusted restricted path}; do
    echo "[linux] $label, Python $version"
    if ! LP_TEST_PYTHON="$python" LP_TEST_LOG="$HOME/$label-$version.log" xvfb-run -a npx vscode-test --label "$label"; then
      echo "[linux] FAILED: $label, Python $version"
      cat "$HOME/$label-$version.log" 2>/dev/null || true
      status=1
    fi
  done
done
if [ "${LP_COVERAGE:-0}" = 1 ]; then
  LP_TEST_PYTHON=$(uv python find 3.13) LP_TEST_LOG="$HOME/coverage.log" xvfb-run -a npm run test:coverage \
    || { status=1; cat "$HOME/coverage.log" 2>/dev/null || true; }
  mkdir -p /out && cp -r coverage /out/ 2>/dev/null || true
fi
exit $status
