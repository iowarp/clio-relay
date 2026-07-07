# agent instructions

Read this file before changing the repository.

## start here

- Use `uv` for dependency management and execution.
- Run commands with `uv run`.
- Keep public docs human-centered. Do not move implementation dumps into the README.
- Use lowercase sentence-style headings in docs.
- Keep cluster names, agent binaries, and transport choices configurable.
- Do not hardcode `ares`, `codex`, `frps.jcernuda.com`, or any specific workload into core logic.

## detailed context

Agent-oriented context lives in `docs/ai/`:

- `docs/ai/README.md`: map of the agent context files.
- `docs/ai/system-context.md`: full architecture and behavior notes.
- `docs/ai/testing-context.md`: verification expectations and live acceptance notes.

Read those files before making non-trivial implementation changes.

## local checks

```powershell
uv run ruff check --fix
uv run ruff format
uv run pyright
uv run pytest
```

Failed or skipped tests are failures unless the user explicitly narrows the task.
