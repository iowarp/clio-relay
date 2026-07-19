# Operations

This page covers the common operator paths. Use the README for the short overview and `docs/ai/` for the full implementation context.

For a complete first-connection walkthrough from a local desktop through a
homelab relay to a cluster worker and remote agent, see
`docs/connect-desktop-homelab-cluster.md`.

## Add a Cluster

```powershell
uv tool install --python 3.12 --no-config clio-relay
clio-relay cluster add --name my-cluster --ssh-host my-cluster-login --scheduler-provider slurm --agent-adapter exec --agent-bin agent
clio-relay cluster bootstrap --cluster my-cluster
clio-relay cluster install-endpoint-service --cluster my-cluster --start --enable
```

An enabled endpoint worker is expected to survive every desktop disconnect and
the operator's final cluster logout. Before installing it, verify the cluster's
systemd user manager is persistent:

```bash
loginctl show-user "$USER" -p Linger --value
loginctl enable-linger "$USER"  # one-time; site authorization may be required
```

`install-endpoint-service` requires the first command to return exactly `yes`
and fails before writing the unit otherwise. If site policy intentionally
forbids lingering, `--allow-login-scoped` installs a diagnostic, login-scoped
worker. It may stop after the last login and is not eligible for live release
claims. Desktop detach and default teardown never stop either service; only an
explicit `session teardown --stop-worker` does. The persistent unit restarts
after both clean and failed worker-process exits; an explicit systemd stop is
still respected and remains stopped until an operator starts it again.

### Upgrade durable queue indexes

The worker service runs this preflight before it accepts work:

```powershell
clio-relay queue migrate-indexes --all --batch-size 500
```

Inspect the durable checkpoint without advancing it:

```powershell
clio-relay queue migration-status
```

Migration holds the queue lock for one bounded batch at a time and resumes from
durable checkpoints after interruption. Jobs, endpoints, gateway sessions, and
monitor rules receive stable global sequence indexes; task, artifact, progress,
active-resource, and terminal-retention indexes are migrated in the same gated
upgrade. Existing queues also receive a crash-replayed lease-capacity aggregate
only after canonical leases and every exact operational index have been rebuilt
and reconciled. While any checkpoint is incomplete, submission, endpoint
registration, gateway creation, monitor creation, worker acquisition, paged
reads, and terminal collection fail with an actionable `queue migrate-indexes`
error. They never fall back to an unbounded scan or read a partial index. The
generated systemd user unit uses `ExecStartPre` with `--all`, so a migration
error prevents the worker from starting and remains visible in the unit journal.

### Upgrade v0.9 output records

Some v0.9 workers stored a complete stdout or stderr callback twice in one
event. Those records can be larger than the current event limit. A normal
`clio-relay init` audits this state but does not change it; when migration is
required, it stops with the exact operator command:

```powershell
clio-relay init --migrate-legacy-output
```

Run that command only after every process writing the same queue has stopped.
This includes endpoint workers, `clio-relay api start`, and any retained
`clio-relay mcp-server` process.
It preserves the original bytes in an owner-private archive, replaces the
oversized event with a bounded compatibility record, and writes durable
receipts so an interrupted migration resumes safely. Managed `cluster
bootstrap` performs this sequence itself: it stops the configured worker before
replacing packages, refuses to continue while any installed `clio-relay`
process still owns the same physical queue, runs a final bounded source-to-index
reconciliation under the queue lock, and restarts the service only if it was
previously active. New managed queues hold shared core ownership for their full
lifetime, so later migrations cannot overlap an API, MCP server, worker, or new
queue-backed writer. Explicit migration returns a closed inspection object,
never an unfenced writable queue. The migration does not delete jobs, logs, or
scheduler work.

Cluster names are local labels. `ares`, `homelab`, or a later institutional target are registry entries, not hardcoded behavior.
Use `--scheduler-provider external` when JARVIS or another deployment driver owns
all scheduler observation and cancellation.

Pin the physical identity of an existing entry without replacing its transport,
remote MCP registrations, paths, live-test inputs, or agent settings:

```powershell
clio-relay cluster pin-target --cluster my-cluster --target-hostname login-1.example.edu --ssh-host-key-sha256 SHA256:REPLACE_WITH_PIN --scheduler-cluster-name my-cluster
```

Repeat either identity option during hostname aliases or SSH host-key rotations.
`cluster pin-target --cluster my-cluster --clear` removes only the target identity
pin and cannot be combined with identity values. Target pinning first reads the
effective `UserKnownHostsFile` entries from `ssh -G` (including hashed entries
and nondefault ports), then uses a bounded `ssh-keyscan` only when no configured
key is available.

### manage system software and external application plugins

The generic `linux-user` bootstrap installs relay, transport, JARVIS, and
configured agent dependencies only. It does not install, load, or shim
scientific application binaries, Spack environments, MPI launchers, or site
modules. No application installer or progress parser ships inside
`clio-relay`.

For agent-driven system-software operations, register a cluster-side generic
Spack MCP server through the normal `remote_mcp_servers` cluster configuration.
The audited clio-kit user contract is `spack_find`, `spack_locate`, and
`spack_install`. There is deliberately no stateful `spack_load` tool: an MCP
child's process environment cannot survive into a later JARVIS process.
Declare `contract: clio-kit-spack-user-v2.1` on new registrations so the live
gate checks the exact upstream schemas; the operator's server name has no
semantic effect.
Existing `clio-kit-spack-user-v2` registrations remain accepted and are
verified against the preserved v2 digest; changing the declared identifier
requires a fresh discovery of the matching server surface.
`jarvis_run(spack_specs=[...])` resolves the requested specs immediately before
execution and persists the filtered environment into the JARVIS pipeline so
direct execution and scheduler reload see the same values. Use the server's
discovered `tools/list` schema as the authority for arguments, and allowlist
only the operations permitted by site policy.

Sites that require a non-agent installation workflow can install an external
Python distribution that contributes a profile through the
`clio_relay.application_profiles` Python entry-point group without changing
generic bootstrap code. After that distribution is installed, operators may
invoke `cluster install-app --app <external-profile-name>`. The separate
`clio_relay.package_progress_adapters` entry-point group remains a compatibility
surface for older packages that only expose application log parsers. Those
adapters are inspectable, but they cannot satisfy the 1.0 execution/progress
gate. Production acceptance requires the JARVIS-owned execution handle, durable
record, and application-independent progress snapshot described below.
The cluster bootstrap downloads one exact JARVIS-CD GitHub release wheel,
verifies its pinned SHA-256, and installs those same bytes normally in the
JARVIS execution environment and with `--no-deps` in the relay provider
environment. The latter exposes only the provider entry point to the worker; it
does not move application semantics into relay core. The install receipt records
the release URL, wheel digest, distribution name/version, both interpreters, the
JARVIS executable, and exported entry points. The running worker verifies that
both environments still resolve to the receipt-bound wheel before release
acceptance.

An older compatibility pipeline may declare its provider explicitly under the
package, for example `progress.adapter: lammps`. The worker replaces all
plugin-supplied provenance with the bound entry-point and distribution identity.
Every provider-produced, structurally valid observation is persisted with
`provider_validated=true`; warm-up observations remain visible with
`acceptance_validated=false`, while observations satisfying the package's live
acceptance predicate carry `acceptance_validated=true`. Desktop acceptance
therefore needs no local copy of JARVIS-CD: it verifies the explicit YAML
contract against the durable worker attestation. Providers must also flush
buffered fragments from
`finalize_jarvis_stdout()` and `finalize_stdout()` at their respective EOFs.
The two inputs are mutually exclusive for one execution. If the provider
declares one `progress_log_paths()` entry, that package log is authoritative and
JARVIS stdout is not parsed. Scoped JARVIS stdout is used only when the provider
declares no host-visible log, including container-private log configurations.
Filesystem location alone does not prove host visibility: node-local paths such
as `/tmp` also use stdout fallback. A provider may expose a host-tail path only
when the package explicitly declares `progress.log_visibility: shared`; the
Ares acceptance pipelines opt into that contract.
Only the selected source's EOF finalizer runs. A provider may expose at most one
log path in 1.0. Relative paths are resolved against the exact JARVIS child
working directory. Provider logs must be regular files; symlinks, devices, and
FIFOs are rejected. The worker opens and identifies one descriptor before use,
reads it in bounded chunks, and checkpoints its size, file identity, and trailing
bytes before launch. Historical bytes are skipped, while replacement,
truncation, or rewrite resets the tail and provider log parser before new bytes
are consumed.

## Run a Job

```powershell
clio-relay job submit --cluster my-cluster --jarvis-yaml .\pipeline.yaml
clio-relay job watch <job-id> --cluster my-cluster
clio-relay job read-log <job-id> --cluster my-cluster --stream stdout
clio-relay job list-artifacts <job-id> --cluster my-cluster
```

Submissions are asynchronous by default. CLI, HTTP, and MCP callers get a `job_id` and can monitor events and logs by cursor or byte offset.

### Record consumed artifact lineage

Every submission surface accepts a bounded list of content-pinned dependencies.
On the CLI, repeat `--used-artifact ARTIFACT_ID=SHA256`:

```powershell
clio-relay job submit --cluster my-cluster --jarvis-yaml .\pipeline.yaml `
  --used-artifact artifact_input=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
clio-relay job used-artifacts <consumer-job-id> --cluster my-cluster
clio-relay job used-by artifact_input --cluster my-cluster
```

The relay verifies each digest before reserving the submission, stores one
immutable `used` edge, and maintains the reverse `used_by` index. A retained
consumer protects its producer artifact from garbage collection. Owned-session
APIs require producer and consumer to belong to the exact same session
generation. HTTP exposes `/jobs/{job_id}/used-artifacts` and
`/artifacts/{artifact_id}/used-by`; MCP exposes both directions through
`relay_artifact_lineage` by accepting exactly one of `job_id` or `artifact_id`.

For inline YAML, path, and command JARVIS runs, the endpoint captures and hashes
the exact materialized pipeline bytes before launching that same spool file.
Named JARVIS pipelines currently retain a name reference rather than a resolved,
content-hashed pipeline snapshot; a name alone must not be claimed as
artifact-backed replay evidence.

### inspect structured runtime metadata

For JARVIS-owned execution, the authoritative source is the exact native
document set returned by JARVIS and polled by the relay-owned broker:

- `jarvis.execution.handle.v1` is the stable execution identity and contains
  the JARVIS execution id, pipeline id, execution mode, and native scheduler
  identity when applicable;
- `jarvis.execution.record.v1` is the durable lifecycle record;
- `jarvis.execution.progress.v1` is the queryable, package-independent progress
  snapshot whose latest events use `jarvis.progress.v1`.

The broker requires all three documents to agree on execution, pipeline,
provider, native scheduler identity, lifecycle, and reconciliation marker. It
then writes the bounded document set to the authenticated relay sidecar. The
normalized `clio-relay.jarvis-runtime.v1` record can include:

- execution and pipeline ids;
- scheduler provider, type, job id, and phase;
- scheduler script, hostfile, stdout, and stderr paths;
- allocated nodes;
- package names, versions, ids, sources, and paths;
- terminal state, return code, reason, and timestamps.

The current record is stored in both job and task `metadata.runtime_metadata`,
and `runtime-metadata.json` is indexed as a job artifact. A claimed scheduler id
is authoritative only when the mutually bound native documents prove the
scheduler provider and native id and the authenticated sidecar belongs to the
durable relay job and task. Missing, wrong, stale, or internally inconsistent
documents fail closed: they cannot create scheduler ownership, polling, or
cancellation eligibility. `field_sources` preserves that field-level trust
decision. Older `jarvis.runtime.v1`, package-adapter, and log-only paths remain
visible as compatibility evidence but cannot satisfy the 1.0 gate. Scheduler
cancellation additionally requires an exact `scheduler_job_ownership` record;
it is opt-in, and detach/exit does not cancel scheduler jobs by default.

An MCP result is treated as owned JARVIS metadata only when its command matches
the operator-configured JARVIS MCP command and its successful result envelope
matches the durable relay job's server, arguments, operation, and tool. A
different server cannot gain scheduler ownership merely by naming a tool
`jarvis_run`.

For the legacy clio-kit 2.2.6 compatibility path, a successful synchronous
`jarvis_run` MCP return is normalized to a terminal `completed` record even
though that release labels the result `status=running`; the original status and
completion basis remain in `details.completion_normalization` for auditability.
The pinned clio-kit 2.5.17 production path removes that ambiguity upstream and
returns a structured durable execution handle immediately. The legacy normalization is
diagnostic compatibility evidence and cannot satisfy the 1.0 gate. Scheduler
submissions remain non-terminal and are observed through `jarvis_get_execution`.

Exercise the agent-facing virtual `jarvis_run` tool, its cluster routing, durable
result, and non-legacy runtime artifact in one canonical acceptance run:

```powershell
clio-relay jarvis-mcp-validate `
  --cluster my-cluster `
  --package-search-query lammps `
  --arguments-json-file .\jarvis-run.json `
  --report .\validation\jarvis-mcp.json
```

The package-search query must identify at least one installed application; its
bounded summary page is retained in the same machine-readable report.
`jarvis-run.json` must contain the remote `pipeline_id` and any `execution` or
`submit` fields accepted by the cluster-local JARVIS MCP. Internal `wait` is not
part of the public handle-first contract. The local
`cluster` selector is injected by clio-relay and is never forwarded to JARVIS.

Runtime observations are emitted only by the relay-owned JARVIS execution
broker or returned through an owned `jarvis_run` MCP result. Application and
package code does not receive the runtime sidecar path or HMAC key and must not
write the sidecar directly. The following older `jarvis.runtime.v1` payload is
accepted for compatibility inspection only; it is not native 1.0 evidence:

```json
{
  "schema_version": "jarvis.runtime.v1",
  "execution_id": "run-42",
  "scheduler_provider": "site-scheduler",
  "scheduler_type": "batch",
  "scheduler_job_id": "12345",
  "scheduler_phase": "allocated",
  "package_provenance": [
    {"package_name": "example.package", "package_version": "1.2.3"}
  ],
  "details": {
    "scheduler_submission": {
      "schema_version": "jarvis.scheduler.submission.v1",
      "provider": "site-scheduler",
      "scheduler_job_id": "12345",
      "identity_source": "scheduler_submit_api",
      "submitted": true
    }
  }
}
```

The private adapter wraps each observation with a monotonic `sequence` and a
canonical HMAC by calling `runtime_sidecar_record`; the HMAC key is never
serialized. The scheduled wrapper receives that key once through the
containment broker after dump and trace access have been disabled. Any invalid,
out-of-order, oversized, replaced, or unreadable sidecar record durably latches
the channel failed closed. Later signed records cannot restore authority; only
an exact one-match scheduler-marker reconciliation can resolve the latch, and
the quarantined sidecar remains with the job spool as evidence.

## Manage the Relay Queue

Relay queue state is separate from cluster scheduler state. A queued relay job
has not been leased by a worker yet. A running relay job may already have
submitted scheduler work through its JARVIS package.

```powershell
clio-relay queue list --cluster my-cluster --kind remote_agent
clio-relay queue diagnose <job-id> --cluster my-cluster
clio-relay queue stale --cluster my-cluster --older-than 2h
clio-relay queue cleanup-stale --cluster my-cluster --older-than 2h
clio-relay worker status --cluster my-cluster
```

Queue reads and cleanup scans are bounded. Their JSON includes
`scan_truncated` and `result_truncated`; increase `--scan-limit` or `--limit`
explicitly when an operator needs a larger window. Diagnosis is job-specific
and reports raw submission order separately from effective admission blockers,
plus the durable lease and owner heartbeat, worker capacity, scheduler
observations, current tasks, and the most recent event and progress record. The
admission snapshot applies the same fresh worker slots, per-kind limits, active
leases, pending-cleanup exclusions, and global lease bound as worker admission.
If any bounded input is truncated or inconsistent, diagnosis returns
`admission_analysis_incomplete` instead of presenting a raw earlier job as a
proven blocker. Every job-specific mutation verifies the requested cluster.
The same payloads expose `active_job_capacity` with `limit`, `used`,
`remaining`, and `over_capacity`. Admission rejects the next job with the
stable `active_job_capacity_reached` error before the durable active population
can exceed 10,000 records. A queue inherited above that bound remains readable
and drainable, but accepts no additional work until it is below the limit.
Live lease admission uses the durable two-record capacity pair for a constant
number of filesystem reads, then selects the requested cluster-kind count in
memory. The pair is self-digested and independently checkpointed with one epoch,
monotonic generation, and checkpoint identity; missing, torn, duplicated,
hard-linked, oversized, or mismatched records fail closed. It is updated under
the same cross-process lock and transition journal as canonical leases.

Run the full exact audit whenever diagnosing storage damage, after offline
maintenance, or before and after a repair:

```powershell
clio-relay queue audit-lease-capacity --cluster my-cluster
clio-relay queue repair-lease-indexes --cluster my-cluster
clio-relay queue audit-lease-capacity --cluster my-cluster
```

The audit returns `clio-relay.lease-capacity-audit.v1` JSON and exits nonzero
unless canonical leases, manifests, identity/expiry references, endpoint and
cluster-kind scopes, and aggregate totals all agree within the explicit
10,000-record bound. Repair is separately explicit: it rebuilds exact indexes
from canonical leases, creates a new aggregate epoch, and leaves jobs and
scheduler work untouched.

Stale cleanup is a dry run by default. Executing it can recover an expired
lease or stop a stale owned relay task, but it never requests scheduler
cancellation. Queued jobs are retained unless both queued cancellation and
execution are explicit:

An expired lease is requeued only when no scheduler observation exists. If the
job has any durable scheduler identity or status, automatic recovery leaves it
stale and explicit cleanup cancels only the relay side; this prevents an
unattended lease recovery from submitting duplicate scheduler work.

```powershell
clio-relay queue cleanup-stale --cluster my-cluster --older-than 2h --cancel-queued --no-dry-run
```

Cancel queued jobs without touching any scheduler:

```powershell
clio-relay queue cancel <job-id> --cluster my-cluster
```

For leased or running jobs, scheduler cancellation is explicit:

```powershell
clio-relay queue cancel <job-id> --cluster my-cluster --cancel-scheduler-job
```

Use `--keep-scheduler-job` or omit the flag when the user only wants the relay
job/session to stop observing or driving the work. Use `--cancel-scheduler-job`
only when the user explicitly wants the package or scheduler adapter to stop the
remote scheduler job too.

Every cancellation persists a versioned `metadata.cancellation_request` on the
job before the terminal state transition. Workers use that durable policy after
restart and reconcile a cluster-routable scheduler-cancellation index rather
than reconstructing retry state from event history. A successful cancellation
command remains pending until provider polling proves the exact scheduler id is
canceled, otherwise terminal, or explicitly not found. Query failures remain
retryable. Scheduler identifiers still require durable ownership proof before
any provider cancellation is attempted.

Each owner-session generation has a separate durable membership index, with a
10,000-job capacity and separate legacy-generation coverage. The stable
`owner_session_job_capacity_reached` error prevents an unbounded desktop
generation from making teardown enumeration unsafe. Terminal jobs remain in
that membership through verified generation closure.

The user MCP profile exposes the read-only `relay_queue_list`,
`relay_queue_diagnose`, and `relay_queue_stale` tools. The admin profile also
exposes `relay_queue_cleanup_stale`, `relay_cancel_job`, and worker status.
When `cluster` names a configured SSH target, these tools operate on that
cluster's queue and return a `route_revision`; exact follow-up operations can
echo that revision so a changed cluster route is rejected instead of silently
acting on a different queue.

For release evidence, configure at least three live worker slots and the exact
per-kind cap `jarvis=2`, then run during a quiet window with an otherwise empty
relay queue. The validator creates only bounded harmless commands and runs the
canonical acceptance command from the exact artifact under test:

```powershell
clio-relay queue validate --cluster my-cluster --older-than 1m --report .\validation\queue-management.json --validation-launcher uv-tool --validation-install-source pypi
```

The validator requires two generated commands to be concurrently running with
real leases on distinct registered worker-slot endpoints. While an otherwise
idle third slot continues to heartbeat, a third JARVIS command must remain
queued with no lease. It then executes an exact stale cleanup target and cancels
one real running worker process. Passing evidence includes the worker's
`execution.canceled` acknowledgment, task cancellation, lease release, and the
absence of both the outer JARVIS PID and embedded command PID. A bounded held
scheduler fixture must remain pending through that relay-only cancellation; it
is then released and allowed to complete naturally. Scheduler cancellation is
used only as failure cleanup. The report is subsequently bound to the remote
worker's exact released relay artifact, source identity, released clio-kit
component, scheduler provider, and operator-pinned physical target. An optional
positional job id is treated only as an expendable queued compatibility anchor:
it is canceled before the live fixtures and never executed or copied.

For targeted operator cleanup, pass `--job-id` to `queue stale` or
`queue cleanup-stale`. The exact-job form still computes bounded queue context,
but it cannot recover or cancel a neighboring stale record.

Worker capacity is configured when the user-level worker service is installed:

```powershell
clio-relay cluster install-endpoint-service --cluster my-cluster --concurrency 4 --kind-concurrency jarvis=2 --kind-concurrency remote_agent=2 --kind-concurrency mcp_call=1 --start --enable
```

This keeps one sudo-less user service per cluster and runs multiple in-process
worker slots inside that service. `--concurrency` is the total process capacity.
Repeat `--kind-concurrency KIND=LIMIT` to reserve an independent admission cap
for `jarvis`, `remote_agent`, or `mcp_call`; omitted kinds remain governed only
by total capacity. The queue checks these limits atomically with durable lease
creation across slots and processes. A saturated kind is skipped so it cannot
block an eligible job of another kind. `clio-relay worker status`, HTTP
`/workers`, and the equivalent MCP operation report the configured policy,
whether fresh worker registrations agree, and active leases by kind.

## Expose Tools to an Agent

```powershell
clio-relay agent render-mcp-config --output .\clio-relay-agent.config.toml
clio-relay agent run --cluster my-cluster --prompt /path/on/cluster/prompt.md --mcp-config /path/on/cluster/clio-relay-agent.config.toml
```

Agents should submit child cluster work asynchronously and return the child `job_id`. A single cluster worker cannot execute a child job while it is blocked inside a parent agent job waiting for that child to finish.

Agents can also record structured task timeline events:

```powershell
clio-relay job record-task-event <task-id> --cluster my-cluster --event-type dataset_found --label dataset --summary "found staged dataset" --path-ref /mnt/common/datasets/example_001
clio-relay job task-events <task-id> --cluster my-cluster --cursor 1
```

Use timeline events for UI-visible agent work such as repository scans, dataset discovery, generated scripts, planned commands, scheduler submissions, warnings, and completion. Use normal job logs for stdout and stderr.

## Use Remote JARVIS MCP

Install the cluster-side server once, then launch its persistent executable:

```bash
uv tool install --python 3.12 --no-config \
  https://github.com/iowarp/clio-kit/releases/download/v2.5.17/clio_kit-2.5.17-py3-none-any.whl
clio-kit mcp-server jarvis
```

Release bootstrap is stricter: it downloads and hashes the exact released
wheel first, installs that wheel with `uv tool install`, and records the uv
binary/version, tool and environment directories, provider interpreter,
`pyvenv.cfg` uv marker, installed RECORD closure, and retained wheel digest.
Repeated worker calls execute that receipt-bound tool directly; they do not
create a new `uvx` environment per call.

The default agent MCP profile exposes compact relay tools and the compact JARVIS tools:

- `relay_remote_mcp_context`
- `relay_submit_agent`
- `relay_status`
- `relay_cancel`
- `relay_observe`
- `relay_wait`
- `relay_queue_list`
- `relay_queue_diagnose`
- `relay_queue_stale`
- `relay_storage_status`
- `relay_bind_jarvis_runtime`
- `relay_artifact_lineage`
- `jarvis_create_pipeline`
- `jarvis_describe`
- `jarvis_add_step`
- `jarvis_edit_step`
- `jarvis_get_execution`
- `jarvis_run`

Operator-registered remote MCP aliases are added to this user surface from the
profile-filtered durable schema cache. Administrative mutation, worker, raw
dispatch, and direct gateway tools remain hidden and are rejected at dispatch.

The pre-1.0 user contract merges removal into
`jarvis_edit_step(operation="remove")`; `operation="edit"` requires `config`.
Removal unlinks pipeline membership and intentionally does not clean package
files. There is no `jarvis_remove_step` alias, including in admin/all profiles;
admin retains the lower-level `unlink_pkg` compatibility tool.
`jarvis_run(spack_specs=[...])` applies the runtime Spack
environment inside JARVIS rather than relying on a process-local load command.

The expected workflow is to create or load a pipeline through those JARVIS
tools, use `jarvis_describe` for package and pipeline inspection, and call
`jarvis_run` to submit the configured pipeline through the cluster-local JARVIS
environment. Discover an application with
`jarvis_describe(target="package_search", query="visualization", page_size=10)`.
The query is required and limited to 256 characters; `page_size` defaults to 10
and is bounded from 1 through 25. Results contain lightweight package summaries
rather than package settings and never exceed 64 KiB. When `next_cursor` is
present, repeat the same query with that opaque cursor to read the next page.
The cursor is limited to 1,024 characters and is bound to both the query and the
current package inventory, so a different query or a changed inventory fails
closed. After selecting a result, use `target="package"` with its canonical
`name` as `package_name` to retrieve the package-owned settings. The exhaustive
`target="packages"` inventory remains available for explicit legacy use, but it
is not the normal discovery path. `relay_observe` and `relay_wait` are the
agent-facing monitor loop for progress and terminal output. `relay_wait` omits
stdout and stderr unless `include_logs=true`; its inline MCP result prioritizes
the authoritative `structured_result`, while the complete protocol response
remains available in the SHA-verified immutable result artifact.
After submission, `jarvis_get_execution` is the single durable query. It always
returns the execution handle, lifecycle record, and runtime metadata; progress
is included by default and can be disabled with `include_progress=false`.
Artifacts remain opt-in: pass `artifacts={}` or bounded filters for one page,
then pass the returned opaque cursor with the same filters for the next page.
The call requires the local `cluster` selector plus the returned `pipeline_id`
and `execution_id`. The relay forwards the query as a durable remote MCP job and
validates the returned identities, lifecycle, progress, artifact filters,
counts, and cursor bounds before marking its result valid.

Virtual `jarvis_run` returns a durable execution handle without waiting for the
workload. The short relay MCP job may be waited only long enough to obtain that
handle; its completion is not workload completion and its progress
notifications are not live-workload evidence. `jarvis-mcp-validate` repeatedly
calls `jarvis_get_execution`, requiring an identity-coherent running package
progress snapshot followed by a coherent terminal snapshot. Package progress
adapters, stdout scraping, and `clio-kit.jarvis-package-progress.v1`
notifications remain legacy compatibility paths and are invalid as 1.0
acceptance evidence.

Cluster bootstrap stores the exact clio-kit wheel path, digest, persistent-tool
environment, and JARVIS MCP command in the installation receipt. The worker
service reads that receipt and invokes the installed executable directly; it
does not resolve a package version or create another cache environment at call
time. Desktop CLI and MCP submissions defer JARVIS command selection to the
target, so a desktop package default cannot replace the cluster receipt. For
prerelease diagnostics, `CLIO_RELAY_JARVIS_MCP_COMMAND` can still
override the command with a JSON string array. The override is interpreted on
the cluster, but it cannot satisfy released evidence unless it exactly matches
the receipt-bound command and wheel. A Git
branch is diagnostic only and cannot satisfy the 1.0 released-artifact gate.

Operational queue, gateway-session, raw MCP-call, and low-level log tools are available through:

```bash
clio-relay mcp-server --profile admin
```

## Bound Worker Output Storage

Worker stdout and stderr are durable artifacts, so their write limits are set at
the worker rather than only at the read API. The defaults retain at most 64 MiB
per stream and 128 MiB across both streams for one job. Override them with
positive byte counts when launching a worker:

```bash
export CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_STREAM=67108864
export CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_JOB=134217728
```

When either limit is reached, execution continues and live structured sidecars
continue to be ingested. The relay persists only a complete UTF-8 prefix, emits
one `<stream>.truncated` event, and indexes `log-capture.json`. Execution
provenance records the configured limits and exact observed, persisted, and
dropped byte counts. Changing these limits while retrying an existing job is
rejected because it would make the durable accounting ambiguous.

See `docs/remote-mcp-federation.md` for the full agent-facing model.

## Federate a registered remote MCP server

Register only the tools an agent should see, then refresh their schemas through
a durable cluster-side `tools/list` job:

```powershell
uv tool install --python 3.12 --no-config C:\artifacts\science_mcp_kit-1.4.0-py3-none-any.whl
clio-relay remote-mcp register --cluster my-cluster --name science --command science-mcp --env-from SCIENCE_API_TOKEN=SITE_SCIENCE_API_TOKEN --allow-tool inspect_dataset --profile user --call-timeout-seconds 300
clio-relay remote-mcp refresh --cluster my-cluster --name science
clio-relay remote-mcp reload --profile user
clio-relay remote-mcp validate --cluster my-cluster --name science --tool inspect_dataset --arguments-json-file .\inspect-arguments.json --output-json .\validation\remote-mcp.json
```

Release validation can additionally require contract-specific
`structuredContent` semantics with `--result-expectation-json` or
`--result-expectation-json-file`. The expectation is validated against the
registered semantic contract and selected tool before relay dispatch. For the
Spack user contract, it can bind an exact package name and DAG hash, the unique
locate prefix and canonical `/hash` load spec, or installed status with the
requested reuse setting. The resulting `remote-mcp.structured-result` check and
`structured_result_assertion` job metadata are suitable for release-policy
constraints; successful text content cannot substitute for structured data.

For clio-kit's scientific catalog, register the current
`clio-kit-scientific-catalog-user-v1.1` contract. Its describe result exposes an
exact top-level `dataset_descriptor` for direct handoff to
`jarvis_add_step(config.dataset_descriptor=...)`; do not pass the surrounding
catalog `dataset` record to JARVIS. Historical
`clio-kit-scientific-catalog-user-v1` registrations remain valid against their
own preserved digest, but do not claim this explicit handoff shape.

The wheel path is an operator-supplied immutable artifact. A PyPI requirement
string is insufficient for user-profile evidence. A direct console script is
accepted only when it belongs to one non-editable installed distribution and
every file in that distribution's `RECORD` closure verifies.

The generated alias is normally `remote_science_inspect_dataset`. Its local
schema contains a `cluster` selector; the remote call receives only the
discovered server arguments. Stale schemas and command-changed cache entries
are hidden until the operator refreshes them. `reload` is local and performs no
remote execution.

The alias returns a durable relay job handle, so its local `outputSchema`
describes that handle rather than copying the remote tool's synchronous result
schema. The worker enforces the registration's call timeout and bounded MCP
stdout, stderr, and response sizes. Read the completed `mcp_result` artifact for
the upstream tool result.

`--env-from` stores variable-name references only. The worker gives the MCP
child a minimal runtime environment plus those references; it does not inherit
the worker's remaining environment. Relay API, frp, progress, runtime-metadata,
and STCP credentials cannot be referenced. Schema discovery follows bounded
MCP pagination and records its page, tool, and response-byte limits in the
durable result.

## Manage Streaming Service Runtimes

Use gateway sessions for scheduler-backed services that need to survive long enough for a desktop to connect, such as a visualization service, Jupyter-like service, remote MCP server, or long-running agent service.

For a managed runtime, describe the application generically with `ServiceRuntimeSpec`. In production, `submit_command` should invoke a JARVIS package or pipeline. That package owns the application launch, scheduler script, readiness behavior, logs, provenance, and any application-specific stream protocol. The relay waits for the allocated node and service health, starts the cluster-side frp connector, starts the desktop visitor, records both owned PIDs/configs/logs, and returns desktop-local URLs.

```json
{
  "kind": "streaming-http-service",
  "deployment_driver": "jarvis",
  "submit_command": [
    "jarvis",
    "run",
    "/remote/service-runtime.yaml",
    "--set",
    "RELAY_APPLICATION_PORT=18777"
  ],
  "status_command": [
    "jarvis",
    "runtime",
    "status",
    "{scheduler_job_id}"
  ],
  "cancel_command": [
    "jarvis",
    "runtime",
    "cancel",
    "{scheduler_job_id}"
  ],
  "service_port": 18777,
  "health_path": "/healthz",
  "stream_mode": "push",
  "stream_path": "/live-data",
  "event_stream_path": "/events",
  "state_path": "/state",
  "compatibility_paths": {
    "snapshot": "/debug/snapshot"
  },
  "desktop_bind_addr": "127.0.0.1",
  "desktop_bind_port": 28777,
  "proxy_name": "my-service-session",
  "transport_mode": "frp-stcp-wss",
  "readiness_timeout_seconds": 900,
  "poll_seconds": 5,
  "scheduler": "external",
  "connect_url_template": "http://{bind_addr}:{bind_port}"
}
```

```powershell
clio-relay gateway start-runtime --cluster my-cluster --name my-live-service --runtime-json-file .\runtime.json --owner-session-id desktop-session --owner-session-generation-id <generation-id>
clio-relay gateway get <session-id>
clio-relay gateway detach-runtime <session-id> --cluster my-cluster
clio-relay gateway attach-runtime <session-id> --cluster my-cluster
clio-relay gateway stop-runtime <session-id> --cluster my-cluster --keep-scheduler-job
```

When JARVIS already owns the application execution, an agent must not construct a
second runtime specification or submit another scheduler job. The user MCP profile
exposes `relay_bind_jarvis_runtime` for this case. Call
`jarvis_get_execution` with `include_service_runtimes=true` and
`wait_for_terminal=true`, then pass one returned `service_runtime_bindings` entry
unchanged as `binding`:

```json
{
  "binding": {
    "cluster": "my-cluster",
    "source_job_id": "<relay-job-id>",
    "source_artifact_id": "<mcp-result-artifact-id>",
    "package_id": "paraview-1",
    "package_name": "builtin.paraview",
    "service_instance_id": "paraview-live-1"
  },
  "name": "paraview-live"
}
```

The compact binding contains selectors only. The bind operation accepts no host,
port, endpoint path, dataset descriptor,
scheduler identity, or lifecycle command from the caller. It verifies the exact
completed relay job, artifact SHA-256, installed MCP-server artifact digest,
structured protocol result, native JARVIS execution identity, scheduler-provider
boundary, ready `jarvis.service-runtime.v1` report, and dataset fingerprint. It
accepts only `clio-kit.jarvis-execution.v2` from the configured artifact-bound
clio-kit JARVIS MCP; `jarvis_run` and historical execution v1 results are not
reinterpreted as containing service runtimes. It then starts only the remote frp
connector and desktop visitor and persists an
immutable `clio-relay.jarvis-service-runtime-binding.v1` provenance record in the
gateway session. The result returns the gateway session plus `connect_url`,
`health_url`, `stream_url`, `events_url`, `state_url`, and `command_url`.

Detach stops only the owned desktop connector. Stop removes the owned relay
connectors while retaining the JARVIS scheduler job by default. An explicit
scheduler cancellation is fail-closed: the relay re-reads and re-verifies the
source artifact and exact provider/native identity immediately before requesting
provider cancellation. Generic gateway create/update/close calls cannot modify or
erase the binding.

JARVIS service endpoints remain loopback-only inside the allocation. For SLURM,
relay resolves the exact job with the configured scheduler provider, requires
`NumNodes=1`, verifies that `BatchHost` is the sole expansion of `NodeList`, and
launches a detached `srun --jobid ... --overlap --exact --nodes=1 --ntasks=1
--nodelist=<verified-BatchHost>` with private launcher output, then boundedly resolves
its exact `job.step` by a unique step marker and host. That step identity is durably
stored with the placement evidence; the login-node `srun` launcher is never recorded
as the connector PID. Failed registration terminates the launcher. Missing,
ambiguous, or multi-node placement and ambiguous marker recovery are rejected.
Detach checks that exact step with `squeue --steps=job.step`. Stop cancels only
`scancel job.step`, waits for provider-confirmed step absence, and retains the
parent scheduler job by default. Relay readiness also requires the exact
`jarvis.paraview.health.v1` service identity and a matching initial
`jarvis.paraview.service-state.v1` execution, state revision, and canonical dataset
descriptor digest; an arbitrary HTTP 2xx cannot satisfy binding.

Sandboxed MCP-UI iframes have the browser origin `null` and cannot attach headers
to `EventSource`. Do not add broad CORS to the remote application and do not put a
browser capability in the normal bind or gateway record. After verifying the exact
ready gateway and `gateway.jarvis_runtime_binding`, the trusted desktop viewer
issues an explicit short-lived attachment:

```powershell
clio-relay gateway browser-attach <session-id> --cluster my-cluster --ttl-seconds 1800
clio-relay gateway browser-detach <session-id> --cluster my-cluster --attachment-id <attachment-id>
```

`browser-attach` returns one `clio-relay.browser-attachment.v1` document containing
the attachment id, expiry, and capability-bearing connect, health, stream, events,
state, and command URLs. This is the only output containing the raw 256-bit
capability. The gateway record stores only the digest, expiry, local proxy identity,
and revocation metadata. The loopback proxy requires the exact capability and
`Origin: null` on every request, emits only `Access-Control-Allow-Origin: null`,
never emits a wildcard, accepts only the advertised paths and narrow
GET/POST/OPTIONS contract, bounds request bodies and upstream idle time, and strips
the capability before forwarding. Detach durably writes a revocation marker before
stopping the owned proxy; same-id retries return an idempotent revoked result, while
a different attachment id is refused.

Use `transport_mode: "frp-stcp-wss"` for the relay path and `transport_mode: "frp-xtcp-wss"` for direct NAT-bypass attempts. The application stream still flows through the relay/bypass transport to the desktop-local bind port, not through an SSH port forward.

The default service contract is push-based. A desktop client subscribes once to `stream_url`, and the remote application pushes data, images, frames, or domain records as they are emitted. The relay does not assume any application-specific endpoint shape. Pull-style endpoints are represented only as named `compatibility_paths`, such as `snapshot`, `render_once`, or `state_dump`.

The JARVIS package must emit structured JSON records for the runtime supervisor. The submit command must eventually print a JSON object with `scheduler_job_id` and may include `service_host` when allocation is already known:

```json
{"scheduler_job_id":"12345","service_host":"compute-01"}
```

If `service_host` is not known at submission time, provide `status_command`. The status command must print JSON such as:

```json
{"state":"allocated","service_host":"compute-01","reason":null,"events":[{"type":"progress","source":"jarvis_package","package":"example_stream","message":"runtime allocated"}]}
```

The JARVIS package is the source of application monitoring. It can watch stdout, stderr, readiness files, or application-specific control channels, then report generic structured events and the allocated `service_host` through its status output or through the service's push stream. The service runtime supervisor does not parse scheduler command output, scheduler environment variables, or application logs.

`scheduler` is an explicit ownership boundary. Use `external` only when the deployment driver owns scheduler observation and cancellation; in that mode `status_command` and `cancel_command` provide those lifecycle operations. Use a registered provider name such as `slurm` when relay owns scheduler observation and cancellation. The relay then invokes that provider on the cluster for normalized status, queue metadata, target identity, and exact-job cancellation, while the deployment-driver status remains responsible for application events and `service_host`. A site profile for a SLURM cluster must therefore declare `"scheduler": "slurm"`; the generic gateway supervisor contains no site or scheduler branch.

`stop-runtime` stops owned relay connector processes. It keeps the scheduler job by default; pass `--cancel-scheduler-job` only when the user explicitly wants to stop the remote application.
Both `detach-runtime` and `stop-runtime` return JSON resource reports and write a
canonical validation report by default. Supplying `--validation-report` changes
only the destination path; it does not weaken remote worker, target identity, or
artifact provenance checks. A requested stop that cannot prove ownership is
refused and appears under `residual_resources`. If the retained scheduler state
cannot be observed, the gateway stays `degraded` with `cleanup_retryable: true`
instead of being closed.

Gateway submission writes a private pre-submit intent and a bounded, fsynced
output record before publishing the canonical scheduler sidecar. Restart may
recover the exact scheduler id from that output only when its session id,
submission id, scheduler provider, marker, size, and digest all match. Missing
or unreadable connector process evidence is likewise unresolved: cleanup stays
retryable and never treats an observation error as verified absence.

Manual gateway record updates remain available for ordinary endpoint metadata:

```powershell
clio-relay gateway create --cluster my-cluster --name live-service-example --gateway-json-file .\gateway.json
clio-relay gateway update <session-id> --cluster my-cluster --state ready --node compute-01 --gateway-json-file .\gateway-ready.json
clio-relay gateway get <session-id> --cluster my-cluster
clio-relay gateway close <session-id> --cluster my-cluster
```

Generic CLI, HTTP, and MCP gateway writes reject scheduler provider/native ids,
relay runtime specifications and ownership intents, connector ownership, and
relay owner metadata. Use `start-runtime` for a relay-owned scheduler-backed
service and `detach-runtime` or `stop-runtime` for its lifecycle. `close` only
marks an ordinary durable record closed; the core refuses it for an owned
runtime until `stop-runtime` has proven connector cleanup.

## Use FRP Transport

Use frp when the desktop and cluster cannot directly SSH to each other but can both reach a relay host.

```powershell
$env:CLIO_RELAY_FRP_TOKEN = "<shared-frp-token>"
$env:CLIO_RELAY_STCP_SECRET = "<shared-stcp-secret>"
clio-relay cluster add --name my-cluster --ssh-host my-cluster-login --frp-server-addr relay.example.edu
clio-relay relay-host render-frpc-config --cluster my-cluster --local-port 8848
clio-relay relay-host render-frpc-visitor-config --cluster my-cluster --bind-port 8765
clio-relay relay-host test-http-transport --cluster my-cluster --local-bind-port 18765
```

For Cloudflare-backed homelab deployments, configure the cluster transport as `wss` over port `443`. For a raw public relay host, configure `tcp`.

## Use SSH Forwarding

Use SSH forwarding when the desktop already has SSH or VPN access to the cluster.

```powershell
clio-relay relay-host test-ssh-transport --cluster my-cluster --local-bind-port 18766 --remote-api-port 8766 --session-id relay-ssh-test
```

For detach and reattach workflows:

```powershell
clio-relay session start --cluster my-cluster --session-id desktop-session --remote-api-port 8766 --replace
clio-relay session status --cluster my-cluster --session-id desktop-session
clio-relay session detach --cluster my-cluster --session-id desktop-session
clio-relay session teardown --cluster my-cluster --session-id desktop-session
```

Detach succeeds only after the exact owned remote API generation is observed
running after the desktop resources are removed. Teardown closes the owner
generation only after the API, connectors, gateway records, scheduler
dispositions, and any requested worker service stop are verified. Worker stop
evidence must report the exact service as `inactive` or `not-found`; transitional
or unknown systemd states remain retryable. The default canonical report has the
same checks and remote installation provenance as an explicitly named report.
Gateway records must carry that same owner-session generation. A legacy or damaged
record with only the session id is reported as an ambiguous residual and is not
stopped or closed automatically.

Teardown writes a durable cleanup intent before its first side effect. Its
operation id and job/worker policy are returned in `cleanup_evidence`; retry with
the same flags only. A different policy is rejected, and a committed teardown
cannot be changed into attach or detach. Evidence must contain exactly one local
and remote connector result for each owned gateway record. Closed-gateway retries
recreate that evidence idempotently. Remote session commands are wall-clock
bounded, so an unavailable host produces an explicit failed report instead of an
unbounded shutdown.

`session start` requires `CLIO_RELAY_API_TOKEN` by default and fails before opening
the remote API when it is absent. An unauthenticated API requires the explicit
`--no-require-token` operator choice and must not be used for release acceptance.
Owner-bound desktop clients set `CLIO_RELAY_OWNER_SESSION_CLUSTER` to the selected
cluster. `CLIO_RELAY_REMOTE_CLUSTER` identifies the cluster where the current
process is running and must not be set on the desktop merely to select an owned
session route. When both variables are present, they must identify the same
cluster; a mismatch is rejected before admission.

To clean up the persistent worker too:

```powershell
clio-relay session teardown --cluster my-cluster --session-id desktop-session --stop-worker
```

The safe teardown default is `--keep-jobs --keep-scheduler-jobs`. Canceling both
relay and scheduler work requires both explicit flags:

```powershell
clio-relay session teardown --cluster my-cluster --session-id desktop-session --cancel-jobs --cancel-scheduler-jobs --preserve-scheduler-job-id unrelated-validation-job
```

Jobs submitted through the owned session API are stamped with the server-side
`owner_session_id` and `owner_session_generation_id`; clients cannot supply those
metadata fields. Every owned job submission must authenticate with the relay API
token and send `X-Clio-Relay-Owner-Session-Id` and
`X-Clio-Relay-Session-Generation-Id` matching the API process exactly. Missing,
stale, or mismatched bindings are rejected before queue admission. Teardown
discovers active jobs with that exact ownership record, plus terminal relay
submission jobs that still carry an owned scheduler identity. It never treats all
jobs on a cluster as session-owned. A scheduler id is cancelable only when an
authenticated JARVIS ownership record binds it to the exact relay job and task and
agrees with the cluster's configured provider. Raw runtime fields and legacy stdout
ids are reported as refused residuals. The scheduler flag is rejected unless
`--cancel-jobs` is also present; relay jobs are canceled relay-only, followed by one
exact scheduler cancellation and verification path so duplicate provider
cancellation requests cannot race. Release acceptance
also supplies one or more repeatable `--preserve-scheduler-job-id` values. Each
sentinel must be active and absent from both owned and unowned scheduler identities
for the target session generation before cancellation. After owned cancellation it
must still be active or have completed naturally; canceled, failed, unknown, or
unqueryable sentinels fail the cleanup report. The option is rejected unless both
cancellation flags are explicit.

If the scheduler reports `completed` or `failed` while cancellation is being
requested, cleanup records a verified terminal outcome and can close the owned
session. The explicit scheduler-canceled validation check still fails, because a
natural terminal race is not cancellation evidence.

Queued relay cancellation is acknowledged immediately. Leased and running jobs are
cooperative: teardown waits for the worker to terminate its owned execution and
write durable cleanup acknowledgment before stopping gateways or the session API.
The wait defaults to 30 seconds and can be tuned with
`--relay-cancel-timeout-seconds` and `--relay-cancel-poll-seconds`. A timeout leaves
the session generation intake quiesced for a safe retry. Teardown does not start
gateway/API cleanup when an initially discovered cancellation is still pending; it
also rescans for in-flight submissions after the API stops and before gateway
cleanup.

Inspect provider-normalized scheduler state independently:

```powershell
clio-relay scheduler status 12345 --cluster my-cluster
```

For release acceptance, use the deterministic held-job orchestrator instead of
hoping a normal job remains pending long enough to sample:

```powershell
clio-relay scheduler validate-lifecycle --cluster my-cluster --report .clio-relay/validation-reports/scheduler-lifecycle.json
```

It owns one bounded validation job, observes pending before release, proves allocation
from assigned nodes, observes a fresh running state, and waits for completion. A failed
run cancels only the exact job id it submitted and records cleanup in the JSON report.

## Live Acceptance

```powershell
clio-relay live-test --cluster my-cluster --jarvis-yaml .\path\to\site-acceptance.yaml --report .clio-relay\validation-reports\validation-my-cluster-live-test.json
```

A complete live acceptance should verify the cluster bootstrap, worker service, transport, JARVIS package execution, logs, artifacts, provenance, progress, and agent tool submission path.

When a package adapter or runtime metadata artifact is present, the text and machine-readable reports also record `application_boundary`, `application_profile`, `package_adapter`, `package_owner`, `runtime_metadata_source`, `runtime_scheduler_provider`, `runtime_scheduler_job_id`, its field-level source, and the runtime artifact id. `structured_runtime_metadata=legacy_fallback` is evidence of a compatibility path, not proof that structured runtime ingestion passed the 1.0 gate.
