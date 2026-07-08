# clio-relay 0.9.14 validation record

Date: 2026-07-08

This record captures the release validation performed from the local wheel with
`uvx --python 3.12 --from .\dist\clio_relay-0.9.14-py3-none-any.whl`.

## Local release gate

Passed:

- `uv lock`
- `uv run ruff check`
- `uv run ruff format --check`
- `uv run pyright`
- `uv run pytest -q`
- `uv build`
- `uvx twine check dist\*`

Observed package artifacts:

- `dist\clio_relay-0.9.14-py3-none-any.whl`
- `dist\clio_relay-0.9.14.tar.gz`

The local test suite reported 201 passed tests.

## Ares deployment

Commands:

```powershell
uvx --python 3.12 --from .\dist\clio_relay-0.9.14-py3-none-any.whl clio-relay cluster bootstrap --cluster ares
uvx --python 3.12 --from .\dist\clio_relay-0.9.14-py3-none-any.whl clio-relay cluster install-endpoint-service --cluster ares --start --enable
ssh ares "~/.local/share/clio-relay/relay-venv312/bin/python -c 'import clio_relay; print(clio_relay.__version__)'"
```

Observed:

```text
user_systemd=running
clio-relay-worker-ares.service active (running)
0.9.14
```

## Homelab transport

Commands:

```powershell
uvx --python 3.12 --from .\dist\clio_relay-0.9.14-py3-none-any.whl clio-relay relay-host test-http-transport --cluster homelab --local-bind-port 18841 --remote-api-port 18842 --proxy-name uvx-homelab-stcp-014 --timeout-seconds 45
uvx --python 3.12 --from .\dist\clio_relay-0.9.14-py3-none-any.whl clio-relay relay-host test-direct-transport --cluster homelab --local-bind-port 18843 --remote-api-port 18844 --proxy-name uvx-homelab-xtcp-014 --timeout-seconds 45 --no-allow-stcp-fallback
uvx --python 3.12 --from .\dist\clio_relay-0.9.14-py3-none-any.whl clio-relay relay-host test-ssh-transport --cluster homelab --local-bind-port 18845 --remote-api-port 18846 --session-id uvx-homelab-ssh-014 --timeout-seconds 45 --teardown-remote
```

Observed:

```text
transport.protocol=wss
transport.healthz=ok
direct_transport.result=xtcp
transport.protocol=ssh_forward
session_started=uvx-homelab-ssh-014
```

Cleanup verification after the probes found no matching owned probe processes and
no `transport-probes/*/metadata.json` files:

```bash
ps -u "$u" -o pid=,args= | grep -E 'clio-relay api start|/\.local/bin/frpc -c /tmp/tmp\.' | grep -v grep || true
find "$HOME/.local/share/clio-relay/transport-probes" -maxdepth 2 -type f -name metadata.json -print 2>/dev/null || true
```

The SSH session record reported:

```json
{"session_id":"uvx-homelab-ssh-014","running":false,"api_pid":null}
```

## Ares builtin LAMMPS acceptance

Command:

```powershell
uvx --python 3.12 --from .\dist\clio_relay-0.9.14-py3-none-any.whl clio-relay live-test --cluster ares --jarvis-yaml examples\ares-lammps\pipeline.yaml --verify-transport --verify-direct-transport --no-allow-direct-transport-fallback --timeout-seconds 900
```

Observed:

```text
acceptance.jarvis_package=builtin.lammps:/mnt/common/jcernudagarcia/.local/src/jarvis-cd/builtin/builtin/lammps/pkg.py
transport.protocol=wss
transport.http_wait=succeeded
transport.http_progress_adapter=lammps
direct_transport.result=xtcp
acceptance.job_id=job_28a7e8818ecd4364b7d34ed368b67b65
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
uvx --python 3.12 --from .\dist\clio_relay-0.9.14-py3-none-any.whl clio-relay live-test --cluster ares --jarvis-yaml examples\ares-lammps\pipeline.yaml --agent-mcp-config /home/jcernudagarcia/.local/share/clio-relay/live-tests/agent-mcp.toml --agent-child-jarvis-yaml examples\ares-lammps\pipeline.yaml --timeout-seconds 1200
```

Observed:

```text
acceptance.agent_job_id=job_d670823b76cd40a3bba2ec7539e7c78e
acceptance.agent_state=succeeded
acceptance.agent_child_job_id=job_eeb12afa2b814df7902c8fe29e8f4db9
acceptance.agent_child.events=ok
acceptance.agent_child.artifacts=jarvis_pipeline,provenance,stderr,stdout
acceptance.agent_child.progress_adapter=lammps
live acceptance passed
```

## Scheduler pending and cancel

An oversized exclusive LAMMPS job was submitted to force scheduler pending state.

Observed:

```json
{
  "job_id": "job_cf91c79f512c423cbb3619f57e515a76",
  "relay_state_before": "running",
  "scheduler_phase_before": "pending",
  "scheduler_job_id": "21658",
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
  "session_id": "gateway_d4c292500738476dab7d6587650d0993",
  "created_state": "starting",
  "updated_state": "ready",
  "read_state": "ready",
  "queue_state": "RUNNING",
  "local_port": 5901,
  "stdout_uri": "file:///tmp/pvserver.out",
  "closed_state": "closed",
  "service": "pvserver"
}
```

This validates durable gateway record lifecycle. It intentionally does not mean
the relay should kill `pvserver`; service launch and shutdown are owned by the
package/operator process that created the gateway.

## Task timeline

The current remote-agent task was read from the Ares job:

```text
task_f2533375c32f461cb8a904199e6a4550
```

A durable task timeline event was recorded and replayed through the Ares remote
CLI.

Observed:

```json
{
  "task_id": "task_f2533375c32f461cb8a904199e6a4550",
  "created_seq": 1,
  "events_count": 1,
  "next_cursor": 2,
  "first_type": "agent.observation",
  "first_status": "succeeded",
  "validation": "uvx-0.9.14"
}
```

HTTP/SSE/WebSocket task timeline surfaces are covered by local tests in this
release gate. They were not freshly live-exercised on Ares during the 0.9.14
validation pass.

## Release decision notes

The 0.9.12 and 0.9.13 candidates exposed real cleanup defects and were not
accepted:

- 0.9.12 fixed `/proc/{pid}` path construction but still leaked probes because
  cleanup ran after the remote controller was terminated.
- 0.9.13 changed cleanup ordering but still failed on homelab because the remote
  embedded cleanup script used Python 3.10 union type syntax with an older
  default `python3`.

0.9.14 is the first candidate in this sequence with both transport functionality
and post-probe cleanup verified live on homelab and Ares.
