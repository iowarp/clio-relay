# clio-relay 0.9.17 validation record

Date: 2026-07-08

This record captures validation performed from the local wheel with
`uvx --python 3.12 --from .\dist\clio_relay-0.9.17-py3-none-any.whl`.

## Local release gate

Passed:

- `uv lock`
- `uv run ruff check`
- `uv run ruff format --check`
- `uv run pyright`
- `uv run pytest -q`
- `uv build`
- `uvx twine check dist\*`

The local test suite reported 205 collected tests, and the full release gate
passed. Package artifacts:

- `dist\clio_relay-0.9.17-py3-none-any.whl`
- `dist\clio_relay-0.9.17.tar.gz`

## Packaged bootstrap

The installed wheel was run from a temporary directory with no `.git` directory
and only `.clio-relay/clusters.json`. The command included the local wheel in
the bootstrap archive for prerelease validation:

```powershell
uvx --python 3.12 --from $wheel clio-relay cluster bootstrap --cluster ares --relay-wheel $wheel
```

Observed:

```json
{
  "git_exists": false,
  "version": "0.9.17"
}
```

The Ares worker service was then installed and restarted from the 0.9.17 wheel:

```text
clio-relay-worker-ares.service active (running)
clio_relay.__version__ == 0.9.17
```

## Homelab transport

Commands:

```powershell
uvx --python 3.12 --from .\dist\clio_relay-0.9.17-py3-none-any.whl clio-relay relay-host test-http-transport --cluster homelab --local-bind-port 18851 --remote-api-port 18852 --proxy-name uvx-homelab-stcp-017 --timeout-seconds 45
uvx --python 3.12 --from .\dist\clio_relay-0.9.17-py3-none-any.whl clio-relay relay-host test-direct-transport --cluster homelab --local-bind-port 18853 --remote-api-port 18854 --proxy-name uvx-homelab-xtcp-017 --timeout-seconds 45 --no-allow-stcp-fallback
uvx --python 3.12 --from .\dist\clio_relay-0.9.17-py3-none-any.whl clio-relay relay-host test-ssh-transport --cluster homelab --local-bind-port 18855 --remote-api-port 18856 --session-id uvx-homelab-ssh-017 --timeout-seconds 45 --teardown-remote
```

Observed:

```text
transport.protocol=wss
transport.healthz=ok
direct_transport.result=xtcp
transport.protocol=ssh_forward
```

Post-probe cleanup verification found no matching owned probe API/frpc processes
and no `transport-probes/*/metadata.json` files. The SSH session record reported
`running=false`.

## Ares builtin LAMMPS acceptance

Command:

```powershell
uvx --python 3.12 --from .\dist\clio_relay-0.9.17-py3-none-any.whl clio-relay live-test --cluster ares --jarvis-yaml examples\ares-lammps\pipeline.yaml --verify-transport --verify-direct-transport --no-allow-direct-transport-fallback --timeout-seconds 900
```

Observed:

```text
acceptance.jarvis_package=builtin.lammps:/mnt/common/jcernudagarcia/.local/src/jarvis-cd/builtin/builtin/lammps/pkg.py
transport.protocol=wss
transport.http_job_id=job_f2a76a7e21974b5ebb6d7bb231bc46f1
transport.http_wait=succeeded
direct_transport.result=xtcp
transport.http_job_id=job_ddeee9099c484dfebe159e2f109de529
acceptance.job_id=job_7f615f97e2ac4b3494d29c796b509632
acceptance.job_state=succeeded
acceptance.artifacts=jarvis_pipeline,provenance,stderr,stdout
acceptance.progress_adapter=lammps
live acceptance passed
```

Ares cleanup verification after the live test found no matching owned probe
processes and no leftover transport probe metadata.

## Remote agent child job

Command:

```powershell
uvx --python 3.12 --from .\dist\clio_relay-0.9.17-py3-none-any.whl clio-relay live-test --cluster ares --jarvis-yaml examples\ares-lammps\pipeline.yaml --agent-mcp-config /home/jcernudagarcia/.local/share/clio-relay/live-tests/agent-mcp.toml --agent-child-jarvis-yaml examples\ares-lammps\pipeline.yaml --timeout-seconds 1200
```

Observed:

```text
acceptance.job_id=job_845a5605d9eb4d0d8e988223c5897ba9
acceptance.agent_job_id=job_f5d57cd9f7fa47b09602b6fe20aacfed
acceptance.agent_state=succeeded
acceptance.agent_child_job_id=job_d454d82d3bb84eaf93bba0ed11de3c05
acceptance.agent_child.events=ok
acceptance.agent_child.artifacts=jarvis_pipeline,provenance,stderr,stdout
acceptance.agent_child.progress_adapter=lammps
live acceptance passed
```

## Scheduler pending and cancel

An oversized exclusive LAMMPS job was submitted to force SLURM pending state.

Observed:

```json
{
  "job_id": "job_450e4d96abe542e1b81d7f38d62615e5",
  "relay_state_before": "running",
  "scheduler_phase_before": "pending",
  "scheduler_job_id": "21667",
  "reason": "(PartitionNodeLimit)",
  "queue_position": 13,
  "jobs_ahead": 12,
  "relay_state_after": "canceled",
  "scheduler_phase_after": "canceled"
}
```

## Gateway sessions

A ParaView-style gateway session was created, updated, read, and closed through
the Ares remote CLI using file-backed JSON metadata.

Observed:

```json
{
  "session_id": "gateway_85e646884779434686edda71705bf943",
  "created_state": "starting",
  "updated_state": "ready",
  "read_state": "ready",
  "queue_state": "RUNNING",
  "local_port": 5901,
  "stdout_uri": "file:///tmp/pvserver.out",
  "closed_state": "closed",
  "service": "pvserver",
  "validation": "uvx-0.9.17"
}
```

This validates durable gateway record lifecycle. Service process launch and
shutdown remain owned by the package or operator process that created the
gateway.

## Task timeline

The remote-agent task was:

```text
task_85d55251ccba497d97559ef3ace2d428
```

Durable CLI record/read passed:

```json
{
  "created_seq": 1,
  "events_count": 1,
  "next_cursor": 2,
  "first_type": "agent.observation",
  "first_status": "succeeded",
  "validation": "uvx-0.9.17"
}
```

HTTP, SSE, and WebSocket timeline streaming were then live-tested through an
owned Ares API session and SSH-forward transport from the 0.9.17 wheel:

```text
transport.protocol=ssh_forward
transport.healthz=ok
session_started=uvx-live-timeline-017c
timeline.task_id=task_85d55251ccba497d97559ef3ace2d428
timeline.created_seq=2
timeline.http_events=1
timeline.sse_contains_task_events=True
timeline.websocket_contains_task_events=True
timeline.validation=uvx-0.9.17
```

The owned API session was torn down and later reported:

```json
{
  "session_id": "uvx-live-timeline-017c",
  "running": false,
  "api_pid": null
}
```

## Release decision notes

Earlier candidates were rejected during validation:

- 0.9.12 and 0.9.13 exposed transport-probe cleanup defects.
- 0.9.14 fixed transport cleanup but did not prove packaged bootstrap or live
  timeline SSE/WebSocket.
- 0.9.15 and 0.9.16 were intermediate bootstrap-validation candidates and were
  not accepted.

0.9.17 is the first candidate in this sequence with local gates passing,
packaged bootstrap proven from a non-git directory, transport cleanup verified,
live Ares LAMMPS and remote-agent paths passing, scheduler pending/cancel
verified, gateway lifecycle verified, and task timeline HTTP/SSE/WebSocket
verified live.
