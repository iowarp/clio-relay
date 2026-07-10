# Remote MCP federation

`clio-relay` exposes one local MCP server to the desktop agent. That server is the control plane. Remote tools run on the target cluster through queued relay jobs, so the agent does not need a separate `jarvis_ares`, `jarvis_chameleon`, or `paraview_cluster` MCP registration for every machine.

The agent-facing surface should still be concrete. Agents are more reliable when tool names and argument contracts are visible in the system tool list, so clio-relay virtualizes selected remote MCP tools as local tools with an added `cluster` argument.

The pattern is:

1. The desktop agent calls the local `clio-relay` MCP server.
2. The request names a `cluster`.
3. The relay queues a remote MCP call for that cluster.
4. A worker on that cluster launches the requested MCP server inside the cluster environment.
5. The worker records stdout, stderr, the MCP result artifact, timeline events, and terminal state in the relay core.

This keeps machine selection in data, not in duplicated server registrations. A client can use one local interface and route calls to the right cluster at call time.

## Built-in JARVIS path

The built-in JARVIS MCP command is:

```bash
uvx --from clio-kit==2.2.6 clio-kit mcp-server jarvis
```

That command runs on the remote cluster, not on the desktop. During cluster bootstrap, clio-relay installs JARVIS-CD for execution and warms the released clio-kit MCP entry point through `uvx`. The user JARVIS MCP is intentionally compact: create a pipeline, describe packages or pipelines, add/edit/remove a step, and run.

The bootstrap install source can be overridden for prerelease or site-local deployments:

```bash
export CLIO_RELAY_JARVIS_MCP_INSTALL_SPEC='clio-kit==2.2.6'
```

Use `CLIO_RELAY_JARVIS_MCP_COMMAND` only when a site intentionally wants to replace the launched command.

## Relay MCP profiles

The default local MCP profile is the user profile. It exposes:

- `relay_submit_agent`
- `relay_status`
- `relay_cancel`
- `relay_observe`
- `relay_wait`
- `jarvis_create_pipeline`
- `jarvis_describe`
- `jarvis_add_step`
- `jarvis_edit_step`
- `jarvis_remove_step`
- `jarvis_run`

Operational tools for queues, gateway sessions, low-level log reads, monitor rules, and raw MCP calls remain available through the admin/operator profile:

```bash
clio-relay mcp-server --profile admin
```

## Agent workflow

An agent should use the virtual JARVIS tools exposed by the local clio-relay MCP server. Each tool maps to the JARVIS MCP running on the selected cluster.

Typical flow:

```text
jarvis_create_pipeline(cluster="ares", pipeline_id="demo_lammps")
jarvis_describe(cluster="ares", target="package", package_name="builtin.lammps")
jarvis_add_step(cluster="ares", pipeline_id="demo_lammps", package_name="builtin.lammps", step_id="lammps", config={...})
jarvis_run(cluster="ares", pipeline_id="demo_lammps", execution={"mode":"cluster"}, submit=true, wait=false)
relay_observe(job_id="<jarvis-run-relay-job>", pattern="Loop time")
relay_wait(job_id="<jarvis-run-relay-job>")
```

The relay submission does not require the agent to copy YAML back to the desktop. The named pipeline lives in cluster-local JARVIS state, and `jarvis_run` is itself routed as a durable relay job.

## Generic remote MCP calls

For MCP servers other than JARVIS, the admin profile exposes the lower-level raw call contract:

- `cluster`: target cluster name.
- `server`: executable to launch on the cluster.
- `server_args`: argument list for the server.
- `tool`: remote MCP tool name.
- `arguments`: tool arguments.

The virtual JARVIS tools are typed conveniences over this generic mechanism. They are not Ares-specific paths.

## Discovery model

The local relay MCP should stay small and stable while still exposing concrete agent-facing tools for common remote capabilities. Remote discovery is cluster-scoped:

- JARVIS pipeline inspection uses `jarvis_describe(cluster=..., target="pipeline", pipeline_id=...)`.
- Generic remote MCP discovery should be exposed as a relay-level remote `tools/list` operation before adding more built-in remote servers.
- Site-specific MCP servers should be registered in cluster configuration, then called through the same `cluster`, `server`, `server_args`, `tool`, `arguments` shape.
- Virtualized tools can be generated from a remote MCP catalog and exposed locally with a `cluster` parameter when a tool family becomes common enough for agent-facing use.

This avoids giving an agent N copies of the same tool surface for N clusters while still preserving the real remote execution environment.
