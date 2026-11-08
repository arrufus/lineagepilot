# Check that LineagePilot is running

LineagePilot starts when you open a folder that holds a `dbt_project.yml`. Its language server is written in Python and ships inside the extension, so it needs **Python 3.10 or later** and nothing installed into it.

It looks for Python in this order:

1. the `lineagePilot.python.interpreterPath` setting;
2. the interpreter selected in the Python extension;
3. `python3` on `PATH` (`py -3` on Windows);
4. a Python installed by [uv](https://docs.astral.sh/uv/). If there is none and uv is installed, LineagePilot offers to have uv download one. Nothing is downloaded unless you agree.

When it is ready, the status bar shows the number of nodes and the age of the manifest. **Lineage: no Python** means it found none: **LineagePilot: Show Output** lists every interpreter it tried and why each was passed over.

## Restricted Mode

In a workspace you have not trusted, LineagePilot only reads the project's artifacts. It never runs dbt or a Python interpreter from inside the workspace, uses only its built-in rules, and cannot review changes, because VS Code turns Git off.
