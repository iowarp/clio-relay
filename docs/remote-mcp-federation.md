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
jarvis-mcp --profile user
```

That command runs on the remote cluster, not on the desktop. During cluster bootstrap, clio-relay installs `jarvis-mcp` into the same user virtualenv that contains JARVIS-CD, then links it into `~/.local/bin`. The `user` profile is meant for normal pipeline construction and inspection. Admin operations such as repository management are separated in `clio-kit` so a normal agent does not receive unnecessary privileged tools.

The bootstrap install source can be overridden for prerelease or site-local deployments:

```bash
export CLIO_RELAY_JARVIS_MCP_INSTALL_SPEC='git+https://github.com/iowarp/clio-kit.git@develop/jarvis-mcp-user-surface#subdirectory=clio-kit-mcp-servers/jarvis'
```

Use `CLIO_RELAY_JARVIS_MCP_COMMAND` only when a site intentionally wants to replace the launched command.

## Agent workflow

An agent should use the virtual JARVIS tools exposed by the local clio-relay MCP server. Each tool maps to the JARVIS MCP running on the selected cluster.

Typical flow:

```text
jarvis_create_pipeline(cluster="ares", pipeline_id="demo_lammps")
jarvis_append_pkg(cluster="ares", pipeline_id="demo_lammps", pkg_type="builtin.lammps", ...)
jarvis_configure_pkg(cluster="ares", pipeline_id="demo_lammps", pkg_id="lammps", extra_args={...})
jarvis_export_pipeline(cluster="ares", pipeline_id="demo_lammps")
relay_submit_jarvis_job(cluster="ares", pipeline_name="demo_lammps")
```

The relay submission does not require the agent to copy YAML back to the desktop. The named pipeline already lives in the cluster-local JARVIS state.

`relay_call_jarvis_mcp` remains available as a lower-level escape hatch for tools that have not yet been virtualized.

## Generic remote MCP calls

For MCP servers other than JARVIS, use `relay_submit_mcp_call` with:

- `cluster`: target cluster name.
- `server`: executable to launch on the cluster.
- `server_args`: argument list for the server.
- `tool`: remote MCP tool name.
- `arguments`: tool arguments.

The virtual JARVIS tools and the JARVIS helper are typed conveniences over this generic mechanism. They are not Ares-specific paths.

## Discovery model

The local relay MCP should stay small and stable while still exposing concrete agent-facing tools for common remote capabilities. Remote discovery is cluster-scoped:

- JARVIS pipeline inspection uses `relay_call_jarvis_mcp(..., tool="export_pipeline", ...)`.
- Generic remote MCP discovery should be exposed as a relay-level remote `tools/list` operation before adding more built-in remote servers.
- Site-specific MCP servers should be registered in cluster configuration, then called through the same `cluster`, `server`, `server_args`, `tool`, `arguments` shape.
- Virtualized tools can be generated from a remote MCP catalog and exposed locally with a `cluster` parameter when a tool family becomes common enough for agent-facing use.

This avoids giving an agent N copies of the same tool surface for N clusters while still preserving the real remote execution environment.
