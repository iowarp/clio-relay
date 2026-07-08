# clio-relay 0.9.5 live validation evidence

Date: 2026-07-08

Package under test: `clio-relay==0.9.5` from PyPI, invoked with `uvx --python 3.12 --from clio-relay==0.9.5 clio-relay ...`.

Status: prerelease validation build. This version passed the live checks below, but it is not ready for internal rollout because generic bootstrap still includes workload-specific LAMMPS setup, scheduler control is SLURM-specific in core paths, and LAMMPS progress parsing is not package-owned. These are tracked in issue #3.

Validation root: `C:\Users\jaime\AppData\Local\Temp\clio-relay-uvx-live-20260708110057`

Release state:

- PyPI reported `pypi_version=0.9.5`.
- GitHub release `v0.9.5` exists at `https://github.com/iowarp/clio-relay/releases/tag/v0.9.5`.
- Release assets:
  - `clio_relay-0.9.5-py3-none-any.whl`
  - `clio_relay-0.9.5.tar.gz`

Local package checks before tagging:

- `uv run ruff format --check`
- `uv run ruff check`
- `uv run pyright`
- `uv run pytest -q`
- `uv build`
- `uvx twine check dist\*`

## Bootstrap and service lifecycle

Homelab bootstrap from released wheel passed:

```powershell
uvx --python 3.12 --from clio-relay==0.9.5 clio-relay cluster bootstrap --cluster homelab
```

Homelab doctor passed:

```text
cluster: homelab
ssh_host: homelab
frpc=0.69.1
frps=0.69.1
jarvis=Usage: [command] [options]
```

Homelab user service install/start passed:

```text
user_systemd=running
linger=yes
Active: active (running)
```

Ares bootstrap from released wheel passed:

```powershell
uvx --python 3.12 --from clio-relay==0.9.5 clio-relay cluster bootstrap --cluster ares
```

Ares doctor passed:

```text
cluster: ares
ssh_host: ares
frpc=0.69.1
frps=0.69.1
jarvis=Usage: [command] [options]
agent=codex-cli 0.142.5
```

Ares user service install/start passed:

```text
user_systemd=running
linger=no
Active: active (running)
```

## LAMMPS and transport acceptance

Command:

```powershell
uvx --python 3.12 --from clio-relay==0.9.5 clio-relay live-test --cluster ares --jarvis-yaml examples\ares-lammps\pipeline.yaml --verify-transport --verify-direct-transport --no-allow-direct-transport-fallback --timeout-seconds 900
```

Observed:

```text
acceptance.jarvis_package=builtin.lammps:/mnt/common/jcernudagarcia/.local/src/jarvis-cd/builtin/builtin/lammps/pkg.py
transport.protocol=wss
transport.healthz=ok
transport.http_job_id=job_59e9d3888b9d4652ab7e39b90762ae90
transport.http_wait=succeeded
transport.http_stdout_bytes=3529
transport.http_progress_adapter=lammps
direct_transport.mode=xtcp
direct_transport.result=xtcp
transport.http_job_id=job_3048a73bf4e54575a2cc951538b49deb
acceptance.job_id=job_3903d7969af947d7a9fedb6b7de7da72
acceptance.live_progress_adapter=lammps
acceptance.job_state=succeeded
acceptance.stdout_bytes=3528
acceptance.stderr_bytes=77
acceptance.artifacts=jarvis_pipeline,provenance,stderr,stdout
acceptance.progress_adapter=lammps
live acceptance passed
```

## Remote agent MCP child job

The Ares MCP profile was generated on the remote host and pointed at the authoritative Ares core and spool:

```toml
[mcp_servers.clio-relay]
command = "clio-relay"
args = ["mcp-server"]

[mcp_servers.clio-relay.env]
CLIO_RELAY_CORE_DIR = "/mnt/common/jcernudagarcia/.local/share/clio-relay/core"
CLIO_RELAY_SPOOL_DIR = "/mnt/common/jcernudagarcia/.local/share/clio-relay/spool"
```

Command:

```powershell
uvx --python 3.12 --from clio-relay==0.9.5 clio-relay live-test --cluster ares --jarvis-yaml examples\ares-lammps\pipeline.yaml --agent-mcp-config /home/jcernudagarcia/.local/share/clio-relay/live-tests/agent-mcp.toml --agent-child-jarvis-yaml examples\ares-lammps\pipeline.yaml --timeout-seconds 1200
```

Observed:

```text
acceptance.job_id=job_b460147b45c347d6a05c156ad336208e
acceptance.agent_job_id=job_199e8030d9464364bfe305bafa93eb85
acceptance.agent_state=succeeded
acceptance.agent_child_job_id=job_94c830311028495cbbdd367ea8fd495d
acceptance.agent_child.events=ok
acceptance.agent_child.tasks=1
acceptance.agent_child.stdout_bytes=3528
acceptance.agent_child.stderr_bytes=77
acceptance.agent_child.artifacts=jarvis_pipeline,provenance,stderr,stdout
acceptance.agent_child.provenance=ok
acceptance.agent_child.progress_adapter=lammps
live acceptance passed
```

## Scheduler pending and cancel

Submitted a 10-node exclusive LAMMPS job to force SLURM pending on Ares:

```text
job_id=job_e096e88408364cd2bcb9c1cbdf3cf08f
scheduler_job_id=21635
phase=pending
queue_position=13
jobs_ahead=12
```

Canceled through relay:

```text
job_e096e88408364cd2bcb9c1cbdf3cf08f canceled
phase=canceled
raw_state=CANCELLED
reason=relay cancellation requested
```

## Gateway sessions

Created, read, updated, listed, and closed a ParaView-style gateway session on Ares using the installed remote CLI.

Session:

```text
gateway_1e1681daae78450e9cad7d25528050b8
```

Closed record:

```json
{
  "session_id": "gateway_1e1681daae78450e9cad7d25528050b8",
  "cluster": "ares",
  "name": "paraview-live-20260708115552",
  "state": "closed",
  "scheduler": "slurm",
  "scheduler_job_id": "21635",
  "queue_state": "running",
  "node": "ares-test-node",
  "gateway": {
    "kind": "paraview",
    "host": "ares-test-node",
    "port": 11111,
    "scheme": "ssh-forward"
  },
  "metadata": {
    "validation": "uvx-0.9.5",
    "updated_by": "uvx-live"
  }
}
```

## Task timeline streaming

Started a token-protected remote API session on Ares:

```text
session_started=uvx-live-timeline
api_pid=2729417
remote_api_port=8766
running=true
```

Forwarded it over SSH and recorded a task timeline event against task `task_004ed8cde0fd43c88dcdde813155ae75`.

Observed:

```text
health=ok
created_seq=1
http_events=1
sse_contains_task_events=True
```

WebSocket payload:

```json
{
  "event": "task_events",
  "data": {
    "task_id": "task_004ed8cde0fd43c88dcdde813155ae75",
    "events": [
      {
        "seq": 1,
        "event_type": "agent.observation",
        "label": "Agent monitor",
        "status": "succeeded",
        "summary": "remote agent timeline event recorded over SSH-forwarded HTTP",
        "metadata": {
          "transport": "ssh_forward",
          "validation": "uvx-0.9.5"
        }
      }
    ],
    "next_cursor": 2
  }
}
```

## SSH forwarding and teardown

Command:

```powershell
uvx --python 3.12 --from clio-relay==0.9.5 clio-relay relay-host test-ssh-transport --cluster ares --local-bind-port 19067 --remote-api-port 8767 --session-id uvx-live-ssh-probe --timeout-seconds 30 --teardown-remote
```

Observed:

```text
transport.protocol=ssh_forward
transport.healthz=ok
session_started=uvx-live-ssh-probe
api_pid=2743249
remote_api_port=8767
```

Teardown with worker stop:

```text
api_stopped=2729417
worker_stopped=clio-relay-worker-ares.service
session_teardown=uvx-live-timeline
running=false
```

The Ares worker service was then restarted and reported active.

## Homelab transport

Commands:

```powershell
uvx --python 3.12 --from clio-relay==0.9.5 clio-relay relay-host test-http-transport --cluster homelab --local-bind-port 19068 --remote-api-port 8768 --proxy-name uvx-homelab-stcp --timeout-seconds 45
uvx --python 3.12 --from clio-relay==0.9.5 clio-relay relay-host test-direct-transport --cluster homelab --local-bind-port 19069 --remote-api-port 8769 --proxy-name uvx-homelab-xtcp --timeout-seconds 45 --no-allow-stcp-fallback
```

Observed:

```text
transport.cluster=homelab
transport.server=frps.jcernuda.com:443
transport.protocol=wss
transport.healthz=ok
direct_transport.cluster=homelab
direct_transport.mode=xtcp
direct_transport.result=xtcp
transport.proxy_type=xtcp
transport.healthz=ok
```

## Known design issues found by adversarial review

- The generic Linux bootstrap still installs an `lmp` wrapper and can install LAMMPS through Spack on first use. This is workload-specific behavior in a generic bootstrap path and should move to package or cluster-profile semantics.
- Scheduler support is SLURM-specific in production code. That is acceptable for the Ares 0.9.x validation target, but it should become a provider boundary before claiming generic scheduler support.
- LAMMPS progress parsing is in relay core and bound to `builtin.lammps`. It should eventually move closer to the JARVIS package/application boundary.
- The agent task itself did not emit timeline events during the child-job run. Timeline REST/SSE/WebSocket were live-tested against that agent task by recording an explicit observer event through HTTP.
