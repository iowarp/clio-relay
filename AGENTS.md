# Agent Instructions

Read this file before changing the repository.

## Start Here

- Use `uv` for dependency management and execution.
- Run commands with `uv run`.
- Keep public docs human-centered. Do not move implementation dumps into the README.
- Use lowercase sentence-style headings in docs.
- Keep cluster names, agent binaries, and transport choices configurable.
- Do not hardcode `ares`, `codex`, `frps.jcernuda.com`, or any specific workload into core logic.

## Detailed Context

Read these before making non-trivial implementation changes:

- `CONTRIBUTING.md`: the layout, read top-down, and the checks.
- `docs/architecture.md`: how a call moves through relay.
- `src/clio_relay/adapters/base.py` and `docs/adding-an-adapter.md`: the adapter contract.
- `docs/files.md`: the files semantics every adapter follows.
- `docs/design/relay-rework-ledger.md`: decisions and their history.

## Local Checks

```powershell
uv run ruff check --fix
uv run ruff format
uv run pyright
uv run pytest
```

Failed or skipped tests are failures unless the user explicitly narrows the task.
