"""Entry point for the LineagePilot language server in the VSIX (design section 4.4).

The extension starts this file as ``python -I lsp_server.py``, with the extension's
directory as the working directory. Isolated mode (-I) keeps the script's directory, the
user site-packages and every PYTHON* variable off the import path, so a workspace file
such as ``yaml.py`` or a user-installed pygls cannot shadow a bundled module. The bundled
libraries are then put first on sys.path, ahead of the interpreter's own site-packages.
The indexer child process inherits this path (lineagepilot_lsp.worker).
"""

import os
import sys

MIN_PYTHON = (3, 10)
LIBS_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "libs"))


if __name__ == "__main__":
    if sys.version_info < MIN_PYTHON:
        sys.stderr.write(
            "LineagePilot needs Python %d.%d or later; this is %s\n"
            % (MIN_PYTHON + (sys.version.split()[0],))
        )
        sys.exit(3)
    sys.path[0:0] = [LIBS_DIR]
    from lineagepilot_lsp.server import main

    main()
