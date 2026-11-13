#!/usr/bin/env bash
# Removes what docs/demo.md and docs/demo_phase5.md make: the demo folder (the workspace copies, the VS Code profile and
# extensions, and the snapshot caches, rule packs and exports inside them), the build from Prepare
# step 1, and the folders that the tests in Prepare step 4 leave in $TMPDIR. Without --yes it only
# lists what it would remove.
#
#   --yes               remove them
#   --keep-build        keep the extension build and the packages' .venvs
#   --uninstall-python  also uninstall uv's Python 3.13, which Prepare installs. Other projects may
#                       use it, and so do this repository's tests (LP_PYTHONS defaults to 3.13)
#
#   DEMO   the demo folder (default: ~/lp-demo). The scripts set it without exporting it.
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
DEMO=${DEMO:-$HOME/lp-demo}
DEMO=${DEMO%/}
TMP=${TMPDIR:-/tmp}
TMP=${TMP%/}

yes=0 keep_build=0 python=0
for arg in "$@"; do
  case $arg in
    --yes) yes=1 ;;
    --keep-build) keep_build=1 ;;
    --uninstall-python) python=1 ;;
    -h | --help) sed -n '2,/^set /p' "$0" | sed '$d' | cut -c3-; exit 0 ;;
    *) echo "unknown option: $arg (see --help)" >&2; exit 2 ;;
  esac
done

die() { echo "demo_cleanup: $*" >&2; exit 1; }
show() { case $1 in "$HOME"/*) echo "~/${1#"$HOME"/}" ;; *) echo "$1" ;; esac; }

removals=()
plan() {
  [ -e "$1" ] || return 0
  case $1 in
    "$REPO"/*) [ -z "$(git -C "$REPO" ls-files -- "$1")" ] || die "$(show "$1") holds files git tracks" ;;
  esac
  removals+=("$1")
  printf '  %-60s %6s\n' "$(show "$1")" "$(du -sh "$1" 2>/dev/null | cut -f1)"
}
# A heading, then each of the paths that is there
group() {
  local heading=$1 before=${#removals[@]} path
  shift
  printf '\n%s\n' "$heading"
  for path in "$@"; do plan "$path"; done
  [ ${#removals[@]} -gt "$before" ] || echo "  nothing to remove"
}

case $DEMO in
  /*) ;;
  *) die "DEMO must be an absolute path, not '$DEMO'" ;;
esac
[ "$DEMO" != "$HOME" ] || die "refusing to remove your home folder"
case $REPO/ in "$DEMO"/*) die "refusing to remove $DEMO: the repository is inside it" ;; esac
case $DEMO/ in "$REPO"/*) die "refusing to remove $DEMO: it is inside the repository" ;; esac
# The demo makes only these. Anything else means DEMO points somewhere it shouldn't.
if [ -d "$DEMO" ]; then
  for entry in "$DEMO"/* "$DEMO"/.[!.]*; do
    [ -e "$entry" ] || continue
    case ${entry##*/} in
      pilot-shop | untrusted-clone | impact-shop | airflow-shop | airflow-clone | profile | extensions | PLANTED-RAN.txt | PLANTED-DAG-RAN.txt | pilot_shop-* | .DS_Store) ;;
      *) die "$(show "$entry") is not something the demo makes; move it out of $(show "$DEMO") first" ;;
    esac
  done
fi

# VS Code windows on the demo profile, and the language servers they started
if pids=$(pgrep -f -- "$DEMO/"); then
  echo "Still running from $(show "$DEMO"); close the demo's VS Code windows first:"
  ps -o pid=,command= -p "$(echo "$pids" | paste -sd, -)" | cut -c1-140 | sed 's/^/  /' || true
  [ $yes = 0 ] || die "nothing removed"
fi
if command -v lsof >/dev/null && lsof -nP -iTCP:8123 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Something still listens on port 8123, probably Part 4's esbuild server: Ctrl+C in its terminal stops it."
fi

group "Demo folder: workspace copies, VS Code profile and extensions, caches, rule packs, exports" "$DEMO"

if [ $keep_build = 0 ]; then
  build=()
  for path in extension/node_modules extension/bundled/libs extension/dist extension/out extension/.vscode-test \
              packages/core/.venv packages/lsp/.venv; do
    build+=("$REPO/$path")
  done
  while IFS= read -r path; do build+=("$path"); done < <(
    find "$REPO/extension" "$REPO/packages" \( -name node_modules -o -name .venv -o -path '*/bundled/libs' \) -prune \
      -o -type d -name __pycache__ -print)
  group "Build outputs (npm ci and uv sync --locked make them again; --keep-build keeps them)" "${build[@]}"
fi

tests=()
# pytest's own temp root is shared with other projects' tests; pytest makes it again when it needs it
for path in "$TMP"/lp-?????? "$TMP"/lp-export-?????? "$TMP"/lp-outside-?????? "$TMP/pytest-of-$(id -un)"; do
  case ${path##*/} in
    # The integration tests' work folder (extension/.vscode-test.mjs) has a markers folder
    lp-??????) [ -d "$path/markers" ] || continue ;;
  esac
  if [ -O "$path" ]; then tests+=("$path"); fi
done
group "Test folders in \$TMPDIR" ${tests[@]+"${tests[@]}"}

if [ $python = 1 ]; then
  command -v uv >/dev/null || die "uv is not on PATH"
  printf '\n%s\n' "uv's Python 3.13 (uv python uninstall 3.13)"
  uv python list 3.13 --only-installed --managed-python | sed 's/^/  /'
else
  printf '\n%s\n' "Kept: uv's Python 3.13 (--uninstall-python removes it)"
fi

echo
if [ $yes = 0 ]; then
  echo "Nothing was removed. Run again with --yes to remove the above."
  exit 0
fi
if [ ${#removals[@]} -gt 0 ]; then
  for path in "${removals[@]}"; do
    rm -rf -- "$path"
    echo "removed $(show "$path")"
  done
fi
[ $python = 0 ] || uv python uninstall 3.13
