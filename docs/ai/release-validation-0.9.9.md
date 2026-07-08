# clio-relay 0.9.9 validation evidence

Date: 2026-07-08

Package under test: local wheel `dist/clio_relay-0.9.9-py3-none-any.whl`,
invoked with `uvx --python 3.12 --from .\dist\clio_relay-0.9.9-py3-none-any.whl
clio-relay ...`.

Status: corrective validation build. This version is not tagged or published
yet.

## Local gates

- `uv run ruff format --check`
- `uv run ruff check`
- `uv run pyright`
- `uv run pytest -q`
- `uv build`
- `uvx twine check dist\*`

Observed: `197 passed`; wheel and sdist passed Twine checks.

## Corrected blocker checks

- Generic bootstrap no longer writes `$HOME/.local/bin/lmp` or installs LAMMPS.
- LAMMPS install is explicit: `clio-relay cluster install-app --cluster ares --app lammps`.
- Ares `~/.local/bin/lmp` now sources `~/.local/share/clio-relay/apps/lammps/env.sh`
  and executes the Spack-installed LAMMPS binary.
- Remote worker service was restarted after bootstrap and reports `clio_relay.__version__ == 0.9.9`.

## Ares app install

Command:

```powershell
uvx --python 3.12 --from .\dist\clio_relay-0.9.9-py3-none-any.whl clio-relay cluster install-app --cluster ares --app lammps
```

Observed:

```text
lammps_prefix=/mnt/common/jcernudagarcia/spack/opt/spack/linux-ubuntu22.04-skylake_avx512/gcc-11.4.0/lammps-20240829.1-p5gjmq4rseitqanua7mdd2zdnag4v3u2
lammps_bin=/mnt/common/jcernudagarcia/spack/opt/spack/linux-ubuntu22.04-skylake_avx512/gcc-11.4.0/lammps-20240829.1-p5gjmq4rseitqanua7mdd2zdnag4v3u2/bin/lmp
lmp=/home/jcernudagarcia/.local/bin/lmp
```

`~/.local/bin/lmp -help` reports LAMMPS `29 Aug 2024 - Update 1`.

## Ares LAMMPS and transport acceptance

Command:

```powershell
uvx --python 3.12 --from .\dist\clio_relay-0.9.9-py3-none-any.whl clio-relay live-test --cluster ares --jarvis-yaml examples\ares-lammps\pipeline.yaml --verify-transport --verify-direct-transport --no-allow-direct-transport-fallback --timeout-seconds 900
```

Observed:

```text
transport.protocol=wss
transport.healthz=ok
transport.http_wait=succeeded
transport.http_progress_adapter=lammps
direct_transport.mode=xtcp
direct_transport.result=xtcp
acceptance.job_id=job_2df360eb872142aaa1061c3eff4e529b
acceptance.job_state=succeeded
acceptance.stdout_bytes=3528
acceptance.stderr_bytes=77
acceptance.artifacts=jarvis_pipeline,provenance,stderr,stdout
acceptance.progress_adapter=lammps
live acceptance passed
```

## Remote agent MCP child job

Command:

```powershell
uvx --python 3.12 --from .\dist\clio_relay-0.9.9-py3-none-any.whl clio-relay live-test --cluster ares --jarvis-yaml examples\ares-lammps\pipeline.yaml --agent-mcp-config /home/jcernudagarcia/.local/share/clio-relay/live-tests/agent-mcp.toml --agent-child-jarvis-yaml examples\ares-lammps\pipeline.yaml --timeout-seconds 1200
```

Observed:

```text
acceptance.agent_state=succeeded
acceptance.agent_child_job_id=job_f8be1598b9344932b475ae6771aae241
acceptance.agent_child.events=ok
acceptance.agent_child.tasks=1
acceptance.agent_child.stdout_bytes=3529
acceptance.agent_child.stderr_bytes=77
acceptance.agent_child.artifacts=jarvis_pipeline,provenance,stderr,stdout
acceptance.agent_child.provenance=ok
acceptance.agent_child.progress_adapter=lammps
live acceptance passed
```

## Scheduler pending and cancel

Submitted a temporary 10-node exclusive LAMMPS pipeline through `job submit`.

Observed before cancel:

```text
job_id=job_46acc73f7f774a92a1e439563be85de5
scheduler_job_id=21647
phase=pending
reason=(Priority)
nodes=10
queue_position=13
jobs_ahead=12
```

Canceled through relay:

```text
job_46acc73f7f774a92a1e439563be85de5 canceled
scheduler_job_id=21647
phase=canceled
raw_state=CANCELLED
queue_position_note=scheduler cancellation was requested by relay; the scheduler provider did not return a terminal record yet
```

Events included `scheduler.pending`, `scheduler.cancel_requested`, and
`scheduler.canceled`.

## Remaining before release

## Homelab transport

STCP over WSS:

```powershell
uvx --python 3.12 --from .\dist\clio_relay-0.9.9-py3-none-any.whl clio-relay relay-host test-http-transport --cluster homelab --local-bind-port 19168 --remote-api-port 8868 --proxy-name uvx-homelab-stcp-099b --timeout-seconds 45
```

Observed:

```text
transport.cluster=homelab
transport.server=frps.jcernuda.com:443
transport.protocol=wss
transport.local_url=http://127.0.0.1:19168
transport.healthz=ok
```

XTCP direct transport:

```powershell
uvx --python 3.12 --from .\dist\clio_relay-0.9.9-py3-none-any.whl clio-relay relay-host test-direct-transport --cluster homelab --local-bind-port 19169 --remote-api-port 8869 --proxy-name uvx-homelab-xtcp-099 --timeout-seconds 45 --no-allow-stcp-fallback
```

Observed:

```text
direct_transport.cluster=homelab
direct_transport.mode=xtcp
direct_transport.result=xtcp
transport.cluster=homelab
transport.server=frps.jcernuda.com:443
transport.protocol=wss
transport.proxy_type=xtcp
transport.local_url=http://127.0.0.1:19169
transport.healthz=ok
```

SSH forwarding:

```powershell
uvx --python 3.12 --from .\dist\clio_relay-0.9.9-py3-none-any.whl clio-relay relay-host test-ssh-transport --cluster homelab --local-bind-port 19170 --remote-api-port 8870 --session-id uvx-homelab-ssh-099 --timeout-seconds 45 --teardown-remote
```

Observed:

```text
transport.cluster=homelab
transport.protocol=ssh_forward
transport.ssh_host=homelab
transport.session_id=uvx-homelab-ssh-099
transport.remote_api_port=8870
transport.local_url=http://127.0.0.1:19170
transport.healthz=ok
```

## Remaining before release

- Publish only after tagging a corrected version, not from 0.9.5.
