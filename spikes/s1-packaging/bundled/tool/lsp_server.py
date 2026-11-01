"""Entry point for the S1 spike language server.

The extension starts this file as `python -I lsp_server.py`. Isolated mode (-I) keeps the
script's directory, the user site-packages and every PYTHON* variable off the import path, so a
workspace file such as `yaml.py` or a user-installed pygls cannot shadow a bundled module. The
bundled libraries are then put first on sys.path, ahead of the interpreter's own site-packages.
"""

import os
import sys

MIN_PYTHON = (3, 10)
TOOL_DIR = os.path.dirname(os.path.abspath(__file__))
LIBS_DIR = os.path.normpath(os.path.join(TOOL_DIR, "..", "libs"))


def update_sys_path() -> None:
    sys.path[0:0] = [LIBS_DIR, TOOL_DIR]


if __name__ == "__main__":
    if sys.version_info < MIN_PYTHON:
        sys.stderr.write(
            "LineagePilot needs Python %d.%d or later; this is %s\n"
            % (MIN_PYTHON + (sys.version.split()[0],))
        )
        sys.exit(3)
    update_sys_path()
    from lineagepilot_spike.server import main

    main()
