# System Context

`clio-relay` is a relay layer for submitting and observing remote work. It is part of the `clio-agent` federation story, but it must remain usable by non-CLIO clients through CLI, HTTP, and MCP.

## Core Boundaries

- `clio-core` is the authoritative queue and state boundary.
- The file-backed queue in this repository is a development backend for the same record contract.
- frp and SSH forwarding are byte transports only. A connection has exactly one
  such link, established at bring-up and held; every control-plane and
  data-plane operation rides it. See `docs/one-link-control-plane.md` for
  transport, the ssh budget and reconnect, and `docs/adapters/worker.md` for the
  worker's link.
- JARVIS-CD owns cluster execution, scheduler integration, package behavior, output collection, and provenance.
- Application-specific behavior belongs in JARVIS packages or package-aware adapters, not in generic relay core code.

## Roles

- `desktop`: accepts local user, agent, HTTP, and MCP requests and submits durable jobs.
- `worker`: runs on a configured cluster, leases jobs, invokes JARVIS-CD, streams events, and writes artifacts.
- `relay-host`: renders or runs frp host configuration. It must not own queue state or job state.

## Records

The durable record families are:

- endpoints
- jobs
- tasks
- leases
- lease-capacity aggregate and checkpoint records
- events
- cursors
- artifacts
- artifact-use edges and reverse indexes
- per-job transform references
- progress
- checkpoints
- idempotency records

Each job has monotonic events and cursor-based replay. Logs are readable by byte offset. Artifacts are indexed records that can point to backing files.

Steady lease admission reads one self-validating aggregate/checkpoint pair for
global and cluster-kind counts. Lease transitions journal the exact before and
after generations, including a distinct rollback generation for an interrupted
acquisition. Exact canonical and operational-index scans remain mandatory for
migration, explicit repair, and `queue audit-lease-capacity`.

## Execution Semantics

Job submission is asynchronous by default. Submit returns a `job_id`, state, kind, and terminal flag. Clients can choose synchronous waiting only when they are not blocking the only worker that can execute child work.

Remote agent jobs should usually submit child cluster work asynchronously and return the child `job_id`. A cluster worker running a parent agent job cannot also execute a child job if it is waiting synchronously inside the parent.

Cancellation is durable and cooperative. A cancel request records queue events and the worker terminates the running process group. For scheduler-backed packages, package code should capture scheduler job ids and cancel through the scheduler when needed.

The native FastMCP server implements SEP-2663 with a custom
`RelayTasksExtension`. Its task ID equals the existing relay job ID. The
projection record is protocol state only; clio-core/relay remains the sole
execution, scheduling, recovery, artifact, and cancellation authority.
FastMCP's stock Docket-backed `TasksExtension` is not enabled and
`FastMCP.docket` remains `None`.
Session teardown quiesces the exact owner-session generation before discovery. With
`--cancel-jobs`, it waits boundedly for worker cleanup acknowledgment of leased and
running jobs before gateway or API cleanup. It stops the API to seal intake, rescans
for pre-quiescence in-flight submissions, and acknowledges those before gateway
cleanup. A timeout leaves intake quiesced and the remaining resources explicit.
Default scheduler retention remains evidence-bearing without a per-job remote
round trip: exact scheduler identities are de-duplicated, grouped by provider,
and queried in bounded batches of at most 256 over the connection's established
link. Every job still emits its own validated cleanup resource and status;
batching changes transport cost, not ownership or preservation semantics. Per-job
dialing is not the baseline this improves on — no relay operation opens
transport of its own.

JARVIS-owned execution is authoritative only when it supplies the exact `jarvis.execution.handle.v1`, `jarvis.execution.record.v1`, and `jarvis.execution.progress.v1` documents. The relay preserves those documents and projects them into `clio-relay.jarvis-runtime.v1` for job/task metadata, events, artifacts, and provenance. The older `jarvis.runtime.v1`, flexible structured payloads, and stdout scheduler patterns are compatibility evidence only and cannot authorize polling or cancellation or satisfy the 1.0 gate.

## Progress

JARVIS core owns execution IDs, durable `jarvis.progress.v1` events, and aggregate execution progress. Package-local code owns only application-specific interpretation. Native MCP notifications carry exact execution progress snapshots and remain distinct from MCP transport sequence numbers.

`clio_relay.package_progress_adapters` and generic regex progress remain explicitly labeled compatibility paths. They cannot satisfy a 1.0 release claim, even when their provider entry point or parsed output is otherwise valid.

ETA is based on observed iterative progress after warmup. It should include confidence or sample metadata when exposed to clients.

## provenance

`ArtifactUse.provenance` is an additive, bounded explanation of a content-pinned
dependency. Missing provenance preserves legacy serialization and digests. When
present, the full object is immutable and copied to every forward/reverse used
edge; changed evidence is a conflict, not an update.

`TransformRef` is a separate one-per-job activity record. It contains a bounded
activity/mechanism, fixed non-secret environment identity, replay contract, and
used evidence. Internal evidence must exactly cover the job's immutable
dependencies, while authority-only and zero-input activities are valid. Only
the authenticated exact-generation HTTP/session API records a transform;
agent-facing MCP has no transform mutation tool. Existing job/status reads may
return the nullable record.

## remote MCP inputs

Transparent local-file staging is enabled only for an immutable registered
`clio-kit-jarvis-user-v3.7.2` route and a package setting whose description carries
the exact closed `jarvis.configuration-input-binding.v1` local-regular-file
declaration. A successful package description on the same connection precedes
add-step configuration; both description and add-step use their bounded
same-call terminal observation when staging/lineage is needed. Relay snapshots
only a stable regular file under
`CLIO_RELAY_INPUT_WORKSPACE_ROOT`, applies the configured per-file, aggregate,
and count bounds, ingests through the authenticated owner-generation API,
rewrites the package argument, and records schema-argument provenance. Each
accepted binding is durable and exact to route, server artifact, pipeline,
step, canonical setting, and session generation. Every genuinely new
`jarvis_run` snapshots the stable logical paths again, reuses unchanged
artifacts, ingests changed bytes, and admits one immutable per-run input
manifest before the worker materializes those settings and calls JARVIS.
Idempotent retries reuse that manifest without rescanning Host state. Other
schemas and path-looking arguments are pass-through.

This is the only way a run input reaches the cluster. The declared value is a
workspace-relative path on the client machine and the bytes ride the
connection's one link; relay never copies a run input with `scp` or `rsync`, and
a cluster-absolute path in a binding-declared setting disables staging rather
than performing it. Non-empty `used_artifact_refs` with a digest matching the
client's bytes is the proof that staging engaged. An empty list on a declared
file-typed setting means the input arrived out of band: that run is not
reproducible and cannot serve as evidence, whatever its terminal state says.

Input file, aggregate-byte, and count limits are part of the immutable owned
session start plan, status selector, retry selector, attempt journal, and ready
session metadata. The cluster-local launcher projects that exact policy into
the owned API child process. A running pre-policy session or a changed policy
requires explicit replacement; reconnect and retry never reinterpret limits
from whichever environment happens to observe the session later.

The canonical FastMCP JARVIS add-step result exposes `step_id` directly in
`structured_result`. Relay accepts the historical nested `result.step_id`
envelope only to read existing evidence and rejects an ambiguous response that
contains both forms.

## Transports

One connection is one local relay and one remote relay joined by exactly one
persistent link. The mode below decides how that link is built; it never gives
an individual operation its own transport. Modes, in order of preference:

- frp STCP over WebSocket/TLS for Cloudflare-backed or HTTPS-edge relay hosts,
  and over TCP for public or institutional relay hosts. Both relays dial
  outbound and the relay point brokers the handshake that joins them; neither
  side accepts inbound. This is the primary path.
- frp XTCP probing as the UDP NAT-bypass variant, yielding a direct peer link
  and falling back to the relay-point path.
- SSH local port forwarding as the fallback for infrastructure that permits no
  other path. One forward is established at bring-up and held for the lifetime
  of the connection.

The ssh budget is per connection lifetime: at most one connection in the frp
modes (deploying the cluster relay, skipped when already deployed) and at most
two in the ssh fallback (deploy plus the held forward). After bring-up, zero.
The operating assumption is that a protected cluster requires interactive 2FA
and the user is present only for the first connection, so no path may dial ssh
unattended for status, polling, recovery, reconnect, or staging. SSH
multiplexing and forward pooling are not fixes for exceeding the budget.

Reconnect is first class: durable state lives in the core and the link is only
transport, so the link is re-establishable without loss and a reconnect is never
a new deployment. One local relay manages connections to many remote relays
concurrently, behind one stable client-facing MCP endpoint.

Transport failure must not corrupt queue state. Direct transport and NAT punching are optimizations, not reliability requirements.

`docs/one-link-control-plane.md` describes the implemented transport and lists
the paths that still dial ssh per operation; they are tracked on #182.

## Sessions

Remote sessions are owned by a session id. Session metadata and PID files live under `$HOME/.local/share/clio-relay/sessions/<session-id>`.

Session start is a durable transition with persistable status/retry selectors.
Only `ready` is usable. `starting` and `ambiguous` remain nonterminal handles;
bounded status/watch deadlines detach observation and do not fail, replace, or
cancel the transition. A generation id on a non-ready result is lifecycle-only
and must not authorize attach or submission. Compatibility text marks these
results handle-only and exits nonzero. Pre-metadata failure teardown produces a
verified `failed_cleaned` receipt and preserves jobs by default.

The desktop shutdown choices are separate:

- close local UI, stop owned desktop connectors, and detach from the remote session.
- close local UI, stop verified owned local/remote connectors and relay API processes,
  and close gateway records owned by the session.
- close local UI and also stop the persistent worker service.

Only the last option should call `session teardown --stop-worker`.
Relay jobs are retained unless `--cancel-jobs` is explicit. Scheduler work is
retained unless `--cancel-scheduler-jobs` is also explicit. Cleanup reports must
include ownership verification, action, outcome, and residual-resource fields.
Teardown persists one immutable cleanup operation and policy before side effects;
same-policy retries are idempotent, policy drift is rejected, and attach/detach is
forbidden after that intent exists. Connector evidence must map one desktop and
one remote disposition to each owned gateway. A natural scheduler terminal state
during a cancel race permits safe closure but must not be reported as canceled.
An owner-session gateway without the exact active generation is ambiguous: detach
or teardown reports it as a residual resource and does not use the session id alone
to authorize connector or gateway cleanup.

The persistent endpoint user service is independent of desktop session lifetime.
With user lingering and its restart policy it recovers after process exits and
survives logout; an explicit systemd stop remains intentional and is not treated
as a crash. The status probe is read-only and reports ready, recovering,
intentional-stop, failed, inactive-unexpected, not-installed, or degraded
evidence.

Live services go through the `serve` verb. The old JARVIS runtime binding, the
allocation-scoped connector and the browser gateway are retired.

## Scheduler provider boundary

Every cluster has an explicit `scheduler_provider`. `external` is the generic
default for package-owned or nonscheduler runtimes; a SLURM site selects `slurm`
in its cluster definition. Missing provider metadata must never silently become
SLURM. Structured JARVIS runtime metadata is preferred for provider and job ids,
and must agree with the worker's configured provider before polling or canceling.
Default detach and stop paths retain scheduler work.

## Configurability

Do not hardcode:

- `ares`
- `codex`
- `frps.jcernuda.com`
- application names, package log formats, or installer commands
- Cloudflare
- local filesystem roots

Those are configured examples or live acceptance targets. The implementation must support a second configured machine such as `homelab` without changing core semantics.
