# clio-relay 0.9.22 validation

Date: 2026-07-10

## Scope

This release changes the agent-facing MCP contract:

- JARVIS virtual tools are now the compact clio-kit user surface:
  `jarvis_create_pipeline`, `jarvis_describe`, `jarvis_add_step`,
  `jarvis_edit_step`, `jarvis_remove_step`, and `jarvis_run`.
- The default relay MCP profile now exposes only:
  `relay_submit_agent`, `relay_status`, `relay_cancel`, `relay_observe`,
  `relay_wait`, `relay_remote_mcp_context`, and the compact JARVIS tools.
- Operational queue, gateway, monitor-rule, low-level log, and raw MCP tools
  remain available through `clio-relay mcp-server --profile admin`.
- The remote JARVIS MCP command now uses the released clio-kit artifact:
  `uvx --from clio-kit==2.2.6 clio-kit mcp-server jarvis`.
- The JARVIS MCP-call package now drives stdio MCP sequentially instead of
  batching initialize and tools/call in one write.
- `relay_observe` now matches regexes against job events and current stdout /
  stderr log snapshots.

## Local verification

Commands run from `D:\Libraries\Documents\projects\clio-relay`:

```powershell
uv run ruff check --force-exclude src tests jarvis-packages/clio_relay/clio_relay/mcp_call/runner.py
uv run ruff format --check src tests jarvis-packages/clio_relay/clio_relay/mcp_call/runner.py
uv run pyright
uv run pytest -q
```

Results:

- Ruff check: passed.
- Ruff format check: passed.
- Pyright: passed with 0 errors.
- Pytest: passed all tests.

## Live Ares verification

The committed tree `c0b784b` was bootstrapped to Ares with:

```powershell
uv run clio-relay cluster bootstrap --cluster ares
```

Then a JSON-RPC client drove the Ares-hosted relay MCP server over SSH. The
default `tools/list` output was verified to contain only:

```text
jarvis_add_step
jarvis_create_pipeline
jarvis_describe
jarvis_edit_step
jarvis_remove_step
jarvis_run
relay_cancel
relay_observe
relay_remote_mcp_context
relay_status
relay_submit_agent
relay_wait
```

The live run created and executed a cluster-local JARVIS pipeline through the
virtual MCP tools:

- Pipeline: `clio_relay_mcp_1783674848`
- `jarvis_create_pipeline` relay job: `job_03f8315afb7b4fecb1e8fa4b50d78853`
- `jarvis_add_step` relay job: `job_45b6d801c58846289b354982baa12cfe`
- `jarvis_run` relay job: `job_49f29e52c3514540bdf9a7266d8f0f03`
- JARVIS package: `builtin.echo`
- Scheduler: SLURM
- Scheduler job id: `21813`
- Scheduler phase observed by relay: `allocated`
- Allocated node/reason field: `ares-comp-29`

`relay_status` returned the terminal relay job state `succeeded` and scheduler
metadata for SLURM job `21813`.

`relay_observe` matched the live stderr log text:

```text
Submitted batch job 21813
```

with regex:

```text
Submitted batch job\s+(?P<slurm_id>\d+)
```

and returned `groupdict={"slurm_id": "21813"}`.

## Remaining caveats

Issue 3 remains open. This release does not claim that all scheduler/provider
hardening is complete. In particular, LAMMPS bootstrap behavior, the LAMMPS
progress adapter location, and the generic scheduler provider boundary still
need follow-up before a 1.0 claim.
