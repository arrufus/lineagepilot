# Planted module: if the server ever imports from the workspace, this shadows PyYAML and fails loudly.
raise ImportError("LineagePilot imported yaml.py from the workspace")
