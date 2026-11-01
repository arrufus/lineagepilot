# CLAUDE.md

## Issue tracking: use beads

This project uses **beads** (`bd`) for all issue tracking. Issue IDs use the prefix `LP`. A `DCS-…` ID refers to the data-capsule repository's tracker, where LineagePilot started.

### Required workflow

Before starting any work:

1. Check for ready work:
   bd ready

2. Pick a task and claim it:
   bd update <issue-id> --status=in_progress

3. Work on the task (code, tests, docs)

4. When done, close it:
   bd close <issue-id>

### Creating new issues

If you discover new work while implementing:

bd create --title="Issue title" --type=task|bug|feature --priority=2

### Rules

- ALWAYS check `bd ready` before asking "what should I work on?"
- ALWAYS update issue status to `in_progress` when you start working
- ALWAYS close issues when you complete them
- NEVER use markdown TODO lists for tracking work

## Project pointers

- Design: `docs/design.md`. Plan: `docs/implementation_plan.md`. Gate G0 evidence: `spikes/G0_REPORT.md`.
- Each spike under `spikes/` is self-contained, with its own `pyproject.toml`, `uv.lock` and offline tests. Use uv-managed CPython (`UV_PYTHON_PREFERENCE=only-managed`); the Homebrew Pythons on this machine are x86_64 builds running under Rosetta.
