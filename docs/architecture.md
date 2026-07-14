# Architecture

`clio-relay` connects a local tool to a remote execution environment without making the network tunnel responsible for job state.

The system has three roles:

- `desktop`: accepts local requests through CLI, HTTP, or MCP and submits work to the durable queue.
- `worker`: runs near the cluster, leases work, calls JARVIS-CD, and records events, logs, artifacts, progress, and provenance.
- `relay-host`: carries bytes for frp deployments. It has no queue and no application state.

## State

The queue boundary is `clio-core`. The file-backed queue in this repository is a development backend for the same record model.

The durable records are jobs, tasks, leases, events, cursors, artifacts, progress, checkpoints, endpoint registrations, and idempotency records. Per-job spool directories hold backing files such as `stdout.log`, `stderr.log`, `pipeline.yaml`, and `provenance.json`, but those files are not the queue. Durable stdout and stderr capture has independent per-stream and whole-job byte quotas. The worker preserves only complete UTF-8 prefixes, emits one explicit truncation event per affected stream, and records observed, persisted, and dropped byte counts in `log-capture.json` and execution provenance. Queue output events are also split into bounded records, so a single write cannot bypass the durable limit.

Worker slots define total process capacity. Optional per-job-kind limits are
durable queue-admission policy, not thread-local semaphores: active same-cluster
leases are counted and the next eligible job is selected under the same queue
lock that creates its lease. This prevents separate slots or worker processes
from racing past a cap and avoids head-of-line blocking when one kind is full.
The executable lease scan shares the 10,000-record active-job bound. Canonical
job, task, lease, and gateway changes first create a bounded transition intent;
startup replay then converges exact indexes and gateway backlinks after a hard
exit. Lease deletion is not complete until both the canonical record and its
per-job reference are absent.

Tasks can also have structured timeline events. A remote agent can record discovery, planning, warnings, commands, scheduler decisions, and completion as resumable task-scoped records. These events are separate from raw stdout so a UI can show meaningful work before the final answer exists.

Gateway sessions are durable records for scheduler-backed services such as interactive visualization servers, data streams, remote MCP servers, or long-running agent services. A session records the scheduler job, queue state, allocated node, logs, forwarded endpoint metadata, health hints, and close state. This lets the desktop detach, reconnect, and mark the session closed without treating it as an anonymous process. The package or scheduler integration remains responsible for stopping the actual remote service.

Before a gateway launches scheduler or connector work, it records an exact
ownership intent. Scheduler output is captured into a private bounded file and
an identity-and-digest sidecar before the canonical submission record is
published. Restart recovery accepts that output only when the session,
submission, provider, and unforgeable marker all match. Connector discovery is
tri-state: an observation error is unresolved ownership, never proof that the
process is absent.

## Execution

JARVIS-CD owns cluster execution. A relay job describes the desired work. The worker materializes that intent into JARVIS inputs, runs JARVIS, streams output while the job is active, and writes provenance when the run ends.

Application behavior belongs in JARVIS packages or operator-selected application profiles. JARVIS core owns execution identity, durable progress events, and aggregate progress snapshots; an individual package owns only the application-specific interpretation used to produce those events. Generic bootstrap and worker code do not import application modules or infer application log paths. The generic bounded-command package stays generic.

The production boundary is the native JARVIS contract. Every run returns exact
`jarvis.execution.handle.v1`, `jarvis.execution.record.v1`, and
`jarvis.execution.progress.v1` documents. The query API returns the same
identity-bound documents after submission, and MCP progress notifications carry
native progress snapshots rather than relay-defined application payloads. The
installation receipt and live worker evidence independently probe the JARVIS-CD
API in its execution interpreter and clio-kit's locked MCP schemas through its
receipt-bound runtime command.

`clio_relay.package_progress_adapters` remains a compatibility mechanism for
older deployments. Compatibility observations are labeled as such and may help
diagnose an older package, but an adapter entry point, parsed log, or stdout
pattern cannot satisfy a 1.0 release claim.
Providers treat logs as host-visible only when the pipeline explicitly marks
them shared; a non-container path is not sufficient evidence because it may be
node-local.

The virtual JARVIS MCP path preserves that boundary across the stdio hop. The
packaged client uses a per-call MCP progress token unrelated to relay
credentials, validates each exact `jarvis.execution.progress.v1` snapshot and
its transport sequence, and binds it to the discovered server artifact. Live
records may be persisted before the final execution identity is confirmed, but
only the final native handle, record, and progress envelope can complete that
binding. MCP transport sequence is never treated as workload percentage.

When JARVIS owns execution, the exact native handle and record are the primary
source of execution and scheduler identity, and the exact progress snapshot is
the primary source of package progress. A scheduler-native ID also requires
matching `jarvis.scheduler.submission.v1` proof inside the native record. The
worker projects these documents into the durable
`clio-relay.jarvis-runtime.v1` job/task view and indexes
`runtime-metadata.json` as an artifact without discarding the originals. The
older `jarvis.runtime.v1`, flexible missing-schema payloads, and log-derived
identities remain visible only as compatibility evidence; none can establish a
1.0 claim, ownership, polling, or cancellation eligibility.

Interactive services should be launched through scheduler-backed package or pipeline behavior as well. The relay records the gateway session and transport metadata, while the package owns how the service starts on the allocated node, how stdout and stderr are interpreted, and which structured progress or runtime events are reported.

## Remote MCP catalogs

Remote MCP servers are registered per cluster with direct-execution commands,
tool allowlists, local profile permissions, and optional child-to-worker
environment references. References contain names only; the packaged runner
constructs a minimal child environment and resolves explicitly allowed values
at execution time. Relay-owned credentials are never eligible references.
Schema discovery is a durable
`mcp_call` job using the `tools/list` operation. Its validated result and
provenance are cached on the operator desktop; local MCP `tools/list` never
starts a cluster process.

Fresh equivalent schemas across clusters become one deterministic local alias
with a `cluster` routing property. The property is added to a copy of the
remote input schema and removed before submission, so it never changes the
remote contract. Stale, command-mismatched, unsafe, or non-allowlisted schemas
are excluded. Alias collisions receive stable content-derived suffixes.
Paginated discovery is bounded by page, unique-tool, and response-byte limits;
cursor cycles and conflicting duplicate definitions fail the durable job.

## Transport

Transport is replaceable:

- frp over WebSocket/TLS for Cloudflare or other HTTPS edges.
- frp over TCP for public hosts or institutional relay hosts.
- SSH local port forwarding for closed environments that already have SSH or VPN access.
- frp XTCP probing as an optional direct path optimization.

Every transport carries local HTTP between endpoints. No job submission, cursor, artifact, cancellation, progress, or provenance record depends on a particular tunnel.

## Detach and Teardown

Remote sessions are owned by a session id. The cluster stores session metadata and a PID file under `$HOME/.local/share/clio-relay/sessions/<session-id>`.

Closing a desktop client has two explicit modes:

- Detach stops owned desktop connectors and leaves the remote relay API,
  cluster connector, gateway record, scheduler job, and worker available for reconnect.
- Teardown stops only processes whose session metadata, process identity, command,
  and ownership token all match. It also closes owned gateway records. Unverified
  processes are not signaled and are reported as residual resources.

The owned API stamps every submitted relay job and created gateway record with its
server-side session identity. `session teardown` keeps those owned relay jobs and
scheduler jobs by default. `--cancel-jobs` applies only to active jobs carrying the
exact owning session metadata; unrelated jobs on the same cluster are never part of
the cancellation set. Teardown first quiesces that session generation's intake and
waits boundedly for each leased or running worker to acknowledge its durable relay
cancellation after process cleanup. If this initial acknowledgment times out, the
generation stays quiesced and gateway/API teardown does not begin. After the API is
stopped, teardown rescans and acknowledges any submission already in flight before
quiescence, then starts gateway cleanup. `--cancel-scheduler-jobs` is separately
required to cancel
the scheduler ids durably attached to that owned set, and is rejected without
`--cancel-jobs`. `--stop-worker` is the explicit cleanup path for the persistent
worker service. Every detach and teardown returns a JSON ownership and
residual-resource report with exact relay and scheduler resource ids.

Before destructive work, teardown atomically persists an operation id and the
exact `stop_worker`, relay-cancel, and scheduler-cancel policy. Retries must use
that same policy; attach and detach are refused once teardown is committed. Each
cleanup report binds exactly one desktop-connector and one remote-connector
disposition to every owned gateway record. A retry after closure re-observes the
closed records and emits fresh evidence instead of treating their absence from an
active-session scan as proof. If a scheduler job completes naturally during a
cancel race, teardown may close after recording the terminal disposition, but the
validation report does not claim that the job was canceled.

## Scheduler providers

Scheduler observation and cancellation use an explicit provider selected in each
cluster definition. `external` means the deployment driver owns scheduler state;
`slurm` selects the SLURM provider. Generic relay records and commands do not infer
SLURM from a missing value. A site uses SLURM because its operator selects that
provider, not because its cluster name has special behavior.
Additional sites can register another provider factory without changing relay queue,
worker, gateway, or lifecycle semantics.

Gateway runtimes preserve the same boundary. A named provider is invoked in the
cluster process context for normalized scheduler phase, queue metadata, target
identity, retained-job verification, and exact-job cancellation. Separately, the
deployment driver's structured runtime status supplies application events,
readiness, and the allocated service host. Only the `external` provider delegates
scheduler observation and cancellation to those driver commands.

Scheduler providers do not submit JARVIS application work. JARVIS materializes the
pipeline execution plan and emits its native execution documents. The SLURM
provider remains limited to queue/status normalization, deterministic provider
validation, and exact-job cancellation.

JARVIS runtime metadata is the preferred source for provider name, job id, phase,
paths, and allocation. The configured worker provider must match structured runtime
metadata before relay-owned polling or cancellation is allowed.
