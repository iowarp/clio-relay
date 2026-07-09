# Architecture

`clio-relay` connects a local tool to a remote execution environment without making the network tunnel responsible for job state.

The system has three roles:

- `desktop`: accepts local requests through CLI, HTTP, or MCP and submits work to the durable queue.
- `worker`: runs near the cluster, leases work, calls JARVIS-CD, and records events, logs, artifacts, progress, and provenance.
- `relay-host`: carries bytes for frp deployments. It has no queue and no application state.

## State

The queue boundary is `clio-core`. The file-backed queue in this repository is a development backend for the same record model.

The durable records are jobs, tasks, leases, events, cursors, artifacts, progress, checkpoints, endpoint registrations, and idempotency records. Per-job spool directories hold backing files such as `stdout.log`, `stderr.log`, `pipeline.yaml`, and `provenance.json`, but those files are not the queue.

Tasks can also have structured timeline events. A remote agent can record discovery, planning, warnings, commands, scheduler decisions, and completion as resumable task-scoped records. These events are separate from raw stdout so a UI can show meaningful work before the final answer exists.

Gateway sessions are durable records for scheduler-backed services such as interactive visualization servers, data streams, remote MCP servers, or long-running agent services. A session records the scheduler job, queue state, allocated node, logs, forwarded endpoint metadata, health hints, and close state. This lets the desktop detach, reconnect, and mark the session closed without treating it as an anonymous process. The package or scheduler integration remains responsible for stopping the actual remote service.

## Execution

JARVIS-CD owns cluster execution. A relay job describes the desired work. The worker materializes that intent into JARVIS inputs, runs JARVIS, streams output while the job is active, and writes provenance when the run ends.

Application behavior belongs in JARVIS packages. For example, LAMMPS progress comes from the upstream JARVIS `builtin.lammps` package and the relay-side parser that is enabled only for that package. The generic bounded-command package stays generic.

Interactive services should be launched through scheduler-backed package or pipeline behavior as well. The relay records the gateway session and transport metadata, while the package owns how the service starts on the allocated node, how stdout and stderr are interpreted, and which structured progress or runtime events are reported.

## Transport

Transport is replaceable:

- frp over WebSocket/TLS for Cloudflare or other HTTPS edges.
- frp over TCP for public hosts or institutional relay hosts.
- SSH local port forwarding for closed environments that already have SSH or VPN access.
- frp XTCP probing as an optional direct path optimization.

Every transport carries local HTTP between endpoints. No job submission, cursor, artifact, cancellation, progress, or provenance record depends on a particular tunnel.

## Detach and Teardown

Remote sessions are owned by a session id. The cluster stores session metadata and a PID file under `$HOME/.local/share/clio-relay/sessions/<session-id>`.

Closing a desktop client can mean two different things:

- detach locally and leave the remote session alive for reconnect.
- tear down the owned remote session, and optionally stop the persistent worker service.

`session teardown` stops only the PID recorded for that session. `session teardown --stop-worker` is the explicit cleanup path for the persistent worker.
