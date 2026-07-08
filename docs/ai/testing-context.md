# testing context

Local tests are necessary but not sufficient for transport or cluster behavior.

## local gates

Run:

```powershell
uv run ruff check --fix
uv run ruff format
uv run pyright
uv run pytest
uv build
```

For CI parity, also verify the built artifacts:

```powershell
uvx twine check dist/*
```

## live acceptance

A feature that depends on a cluster, transport, scheduler, agent process, or MCP call is not done until that path has been live-tested.

The expected Ares acceptance includes:

- bootstrap or update the cluster deployment.
- start or restart the worker service.
- submit a real JARVIS pipeline.
- verify terminal state, events, task records, stdout, stderr, artifacts, and provenance.
- verify package-aware progress when the pipeline uses a supported package.
- verify agent MCP submission when agent behavior is in scope.
- verify transport through the configured path when transport behavior is in scope.
- verify detach and teardown behavior when lifecycle behavior is in scope.

## recent live evidence

The SSH lifecycle implementation was live-tested on Ares after deployment:

- default SSH-forward probe reached `/healthz` and tore down the remote API.
- detached SSH-forward probe reached `/healthz`, left the remote API alive, then `session teardown` stopped it.
- standalone `session start` supported a separately opened `ssh -L` reattach path.
- `session teardown --stop-worker` stopped the Ares user worker service, and the worker was restarted successfully.
- builtin JARVIS LAMMPS live acceptance passed with stdout, stderr, artifacts, provenance, progress, and monitor checks.

Task timeline and gateway session surfaces were live-tested on Ares after deployment:

- task `task_0b2bec1345e041dc965322a0c9c5eccf` recorded and replayed three events:
  - `seq=1 paraview_dataset_found` through desktop CLI passthrough.
  - `seq=2 paraview_script_planned` through HTTP over an SSH-forwarded Ares API.
  - `seq=3 paraview_command_planned` through MCP stdio on Ares.
- the same task events were read through CLI, HTTP JSON, SSE, WebSocket, and MCP.
- gateway sessions were created, updated, read, and closed through CLI, HTTP, and MCP:
  - `gateway_f0292af89638476eae8c82cbed575242` through CLI.
  - `gateway_bfbe7f5bd15841fcbcbfc772ddb20298` through HTTP.
  - `gateway_e406b8c309eb47b7856e47b015239e60` through MCP.
  - `gateway_3418bce505ed4d4e81255266e509df99` through CLI JSON-file metadata.
- `gateway close` was validated as durable record closure. It does not yet scancel or terminate the underlying scheduler-backed service by itself.

Refresh this evidence when changing the relevant code. Do not present old live evidence as current proof after transport, lifecycle, worker, or acceptance code changes.
