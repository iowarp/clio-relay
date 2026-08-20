# Remote MCP federation

`clio-relay` exposes one local MCP server to a desktop agent. Operators can
register stdio MCP servers that exist in a cluster environment, discover their
real schemas through durable relay jobs, and expose selected remote tools as
normal local tools with a `cluster` argument.

This focalization is the point of the layer, not a convenience. Servers on any
number of clusters collapse into one MCP surface: the agent calls one endpoint
and the local relay fans out behind it over each connection's single held link.
Adding a cluster adds a value to the `cluster` enum, never a second MCP
registration, endpoint, tunnel, or shell.

The desktop agent does not need one MCP registration per cluster. A virtual
call follows the normal relay path:

1. The agent calls a concrete local alias and selects a configured cluster.
2. The relay removes the local-only `cluster` selector.
3. The relay submits a durable `mcp_call` job with the registered command,
   arguments, remote tool name, and untouched remote tool arguments.
4. The persistent endpoint worker launches the MCP server directly in
   relay-owned process containment.
5. The worker records stdout, stderr, the MCP result, execution provenance, and
   terminal state.

The low-level `relay_submit_mcp_call` admin tool remains available as an escape
hatch. Registration is the safer agent-facing path because commands, schemas,
profiles, and tools are operator-controlled.

## keep transport execution separate from application execution

Relay does not create an outer JARVIS pipeline, request a scheduler allocation,
or consume a compute node merely to carry a generic `tools/list` or
`tools/call`. The endpoint writes one bounded `mcp-request.json`, starts the
packaged stdio client and registered server under relay-owned process
containment, and applies the relay job's lease, timeout, cancellation, log,
artifact, and recovery rules directly. Any domain side effect requested by the
remote tool still belongs to that server's contract. Relay provenance identifies
the transport provider as `clio-relay-endpoint-mcp` and records
`outer_jarvis_pipeline=false`.

JARVIS remains authoritative when the remote tool itself creates or queries a
JARVIS execution. In that case the outer MCP transport is still endpoint-owned,
while `jarvis_run` returns JARVIS's durable handle and JARVIS owns package,
scheduler, progress, and output semantics. An arbitrary server or similarly
named tool cannot acquire those semantics. Relay enables its specialized JARVIS
validation, recovery, and input behavior only for an immutable registered
server whose discovered surface matches the exact declared JARVIS contract.

## Register a server

Registrations live under the selected cluster in
`.clio-relay/clusters.json`. Commands are direct argument arrays, not shell
strings. Cluster names, executables, package versions, and server arguments are
configuration.

The registry and schema cache are executable-control state. Relay accepts only
bounded regular files and rejects links/reparse points and unstable reads. On
POSIX, the files must be owned by the current user and not writable by group or
other users; atomic replacements are created with mode `0600`. On Windows,
relay creates new state directories and atomic files with a protected ACL before
exposing them, granting full control only to Owner Rights, Local System, and
built-in Administrators. Existing state is accepted only for the current owner,
while no data writer is open, and only after exact native ACL readback. Legacy
inherited ACLs are repaired in place; perform that migration with other local
accounts logged out, and inspect/trust the legacy registry and cache contents or
delete them for recreation before starting relay. The first registry/cache access
performs the repair, and Windows cannot revoke security-control handles that were
opened before hardening. If the ACL cannot be applied, registry/cache access fails
closed. Do not place the state directory on a filesystem or parent path that cannot
preserve these ownership, replacement, and ACL guarantees. The existing parent
and its ancestors are a trust boundary: they must not be reparse points and must
prevent other principals from replacing descendants. Relay retains no-delete-share
handles for every directory it creates until the complete new directory chain is
hardened and verified.

The package and executable names in this example are placeholders for a
site-approved server:

```powershell
uv tool install --python 3.12 --no-config `
  /absolute/path/to/science_mcp_kit-1.4.0-py3-none-any.whl
clio-relay remote-mcp register `
  --cluster my-cluster `
  --name science `
  --command science-mcp `
  --env-from SCIENCE_API_TOKEN=SITE_SCIENCE_API_TOKEN `
  --allow-tool inspect_dataset `
  --allow-tool summarize_run `
  --profile user `
  --call-timeout-seconds 300
```

For a user profile, install from the exact immutable wheel file and retain its
digest. Index requirements such as `science-mcp-kit==1.4.0` remain
resolver-mutable and therefore cannot produce released-artifact evidence. A
direct console script is valid only for a unique non-editable distribution with
a complete, hash-valid `RECORD` closure.

Remote registrations are deny-by-default:

- `allow_tools` is empty unless the operator names tools. `--allow-tool '*'`
  is the explicit opt-in for the entire discovered surface.
- the default profile is `admin`; pass `--profile user` to expose a tool to the
  normal agent profile.
- repeat `--profile` to authorize more than one local profile.
- `--disabled` retains configuration without exposing or refreshing it.
- replacing an existing registration requires `--replace`.
- every virtual call has a bounded duration; the default is 300 seconds and
  `--call-timeout-seconds` may be raised explicitly for long-running tools.

`--env-from CHILD=SOURCE` declares an environment reference, not a value. The
registry stores only the two variable names. At execution time, the endpoint
worker resolves `SOURCE` from its own environment and exposes the value to the
MCP child as `CHILD`. Undeclared endpoint variables are not inherited. Relay
credentials such as `CLIO_RELAY_API_TOKEN`, progress/runtime sidecar tokens,
and frp secrets are forbidden as either side of a reference. Put site secrets
in the worker service environment or site secret manager; never place values in
command arguments or `clusters.json`.

Changing only an allowlist, profile, or namespace takes effect on the next
local `tools/list`. Changing the executable or arguments invalidates the cached
schema until it is refreshed from that command. Changing `env_from` also
invalidates the cache because the execution identity changed.

## Refresh schemas

Discovery is explicit and uses the same durable remote execution path as a
normal tool call:

```powershell
clio-relay remote-mcp refresh --cluster my-cluster --name science
```

The command submits an `mcp_call` job whose typed operation is `tools/list`,
waits for terminal success, reads its indexed `mcp_result` artifact, validates
the command and protocol result against the registration, and atomically
updates `.clio-relay/remote-mcp-cache.json`. Set
`CLIO_RELAY_REMOTE_MCP_CACHE` when the operator cache belongs elsewhere.

The packaged MCP client follows `nextCursor` until discovery is complete. It
deduplicates identical tool definitions across pages and fails closed on a
repeated cursor, conflicting definitions for one name, more than 64 pages,
more than 10,000 distinct tools, or more than 16 MiB of list responses. The
`mcp_result` artifact records page/tool/byte counts and all three limits.

Each cache entry records:

- cluster and server registration name;
- the direct-execution command fingerprint;
- discovery and expiry timestamps;
- a deterministic schema digest;
- the discovery relay job and result artifact identifiers;
- artifact checksum, negotiated MCP protocol version, and remote server info;
- validated input, output, description, title, and annotation fields for every
  discovered tool.

Expired entries and entries produced by a different command are not exposed.
No remote process is launched implicitly from an agent's `tools/list` request.
This prevents a slow or unavailable cluster from blocking local MCP startup and
makes every accepted schema traceable to a relay job.

The local virtual tool is asynchronous. Its advertised `outputSchema` is the
relay job handle (`job_id`, `state`, `kind`, and `terminal`), not the remote
tool's synchronous output schema. The discovered remote output schema remains
in the provenance cache and is validated at the remote server boundary. Agents
obtain a complete bounded public projection through `relay_wait`; the immutable
raw `mcp_result` artifact is private protocol evidence and is not model-readable.
A remote call response is limited to 16 MiB, session stdout to 32 MiB, and
stderr to 4 MiB; exceeding one of those execution limits fails the durable job.
The public result has a separate 64 KiB inline limit. Because arbitrary MCP
output has no generic paging contract, an oversized public result fails delivery
explicitly as `clio-relay.mcp-result-delivery.v1` instead of returning a partial
success. The durable job and private artifact still record what actually happened,
and the error warns that remote side effects may already have occurred so an agent
does not blindly retry the operation.

## Reload the local catalog

The stdio MCP server reads cluster configuration and the schema cache on every
`tools/list` and virtual call. It has no hidden in-memory catalog. Inspect the
exact next catalog revision without contacting a cluster:

```powershell
clio-relay remote-mcp reload --profile user
```

The JSON response contains the catalog revision, generated definitions, and
reasons that registrations are unavailable. A relay server restart is not
required. MCP clients that cache tool lists must request `tools/list` again or
reconnect after a refresh.

Use this command for an operator overview with freshness and provenance:

```powershell
clio-relay remote-mcp list --cluster my-cluster
```

`reload` never performs discovery. `refresh` is the only command that replaces
a cached remote schema.

## Understand generated aliases

A tool is normally exposed as:

```text
remote_<server-namespace>_<remote-tool>
```

For example, `inspect_dataset` from the `science` registration becomes
`remote_science_inspect_dataset`. Registrations on multiple clusters share one
alias when the namespace, remote tool name, schema, and optional declared
semantic contract are equivalent. Each cluster route retains its own
operator-chosen registration name; that name is not cross-cluster identity.
The local `cluster` schema is an enum of the eligible targets.

Each MCP stdio connection binds virtual calls to the profile-specific catalog
revision rendered by its most recent `tools/list`. The revision is returned in
the list result's `_meta`, without adding relay bookkeeping fields to any tool's
input schema. If a refresh, registration edit, profile change, or alias
collision changes that catalog, the server rejects the stale call before route
resolution; the client must run `tools/list` again. Successful virtual
submissions return the bound `catalog_revision` alongside the durable job
handle.

Names are normalized deterministically. If normalized names collide, schemas
differ between clusters, or a name conflicts with a built-in relay tool, the
relay appends a stable digest. Generated aliases are capped at 64 characters,
with a digest-preserving suffix when an operator namespace or remote tool name
is longer, so the local surface remains interoperable with MCP clients. Alias
generation is independent of registry and cache file ordering.

The generated input schema preserves the discovered contract. Simple object
schemas stay flat and receive a local-only `cluster` property. Composed,
recursive, property-constrained, or remote-`cluster` schemas are exposed as
`{cluster, arguments: <remote schema>}` so routing cannot alter their JSON
Schema semantics; relay unwraps `arguments` before the remote call. Invalid
JSON Schema and explicitly non-object tool inputs fail closed with reload
diagnostics rather than reaching MCP clients.

## Call a virtual tool

An agent sees and calls the generated definition directly:

```text
remote_science_inspect_dataset(cluster="my-cluster", path="/data/run-001")
```

The immediate result is a durable relay job record. Use `relay_observe` and
`relay_wait`, or the equivalent CLI, to follow it and obtain its bounded public
result:

```powershell
clio-relay job wait <job-id> --cluster my-cluster
clio-relay job list-artifacts <job-id> --cluster my-cluster
```

The indexed `mcp_result`, `stdout`, `stderr`, and `provenance` artifacts provide
operator acceptance evidence for the actual cluster-side execution. The raw
`mcp_result` remains model-private; its public artifact binding and SHA-256 prove
which immutable evidence backs the `relay_wait` projection.

## stage declared local package inputs

This is the only way a run input reaches the cluster. Registered JARVIS contract
v3.6 moves a small caller-local file without adding an upload tool to the agent
surface: the bytes travel through the owned session over the connection's one
link, exactly like every other relay operation. Nothing is copied around the
transport — `scp` and `rsync` of a run input are always wrong, and the value
supplied for a binding-declared setting is a path on the **caller's** machine,
never a cluster-absolute path.

Configure the desktop MCP process with a workspace it is allowed to read:

```powershell
$env:CLIO_RELAY_INPUT_WORKSPACE_ROOT = "$PWD\science-workspace"
$env:CLIO_RELAY_INPUT_FILE_MAX_BYTES = "1048576"
$env:CLIO_RELAY_INPUT_TOTAL_MAX_BYTES = "4194304"
$env:CLIO_RELAY_INPUT_FILE_MAX_COUNT = "16"
```

The values shown are the defaults: 1 MiB per file, 4 MiB across one
`jarvis_add_step` call, and 16 files. Operators may choose different positive
bounds; the file-count setting is capped at 1,000 and the aggregate byte bound
must be at least the per-file bound. Relative paths are resolved under the
workspace. Absolute paths are accepted only when the verified file remains
inside it.

Both JARVIS doors stage through the same plane. A registered route reaches it
through its `clio-kit-jarvis-user-v3.7.1` registration; the built-in `jarvis_*`
tools reach it through the relay's own pinned clio-kit release, whose contract
digest and JARVIS-CD lock take the place of a registration revision in the
staged route identity. The built-in door engages staging only when the JARVIS
MCP runs on another machine: when it runs on this host the configured path is
already the path the package reads, so nothing is transferred.

Staging is schema-driven, not filename-driven. It is enabled only when all of
these statements are true:

- the door is the built-in JARVIS route, or a registration declares exactly
  `contract: clio-kit-jarvis-user-v3.7.1` and its immutable server artifact and
  discovered schemas match that contract;
- the same route first completes
  `jarvis_describe(target="package", package_name=...,
  wait_for_terminal=true)`, in this MCP connection or in an earlier one that
  recorded the durable contract;
- the selected package setting contains exactly
  `jarvis.configuration-input-binding.v1` with `kind="local_file"` and
  `structure="regular_file"`;
- the caller uses an active relay-owned session generation and supplies one
  canonical setting name or declared alias.

On `jarvis_add_step`, relay snapshots only the declared, owner-readable regular
file under the configured workspace, enforcing stable identity and the count
and byte bounds before any remote mutation. It hashes the bytes, ingests them
through the authenticated owned-session API, verifies the returned durable
input job and content-addressed artifact, and replaces the local argument with
the cluster-local artifact path. Set `wait_for_terminal=true` on this bounded
configuration call as well so relay can verify success before committing the
pipeline lineage. The resulting job dependency has
`clio-relay.artifact-use-provenance.v1` evidence `schema-arg` naming the exact
package setting.

After the add-step call succeeds, relay records each stable workspace-relative
logical path against the exact cluster route revision, MCP registration
revision, immutable server artifact, pipeline id, step id, canonical setting,
and owner-session generation. Tracked `jarvis_edit_step` calls replace or remove
those exact bindings only after JARVIS accepts the edit.

On the first admission of every genuinely new `jarvis_run`, relay securely
snapshots every tracked path again. Unchanged content reuses its immutable
artifact; changed content is ingested as a new artifact. Relay persists a
checksum-bound per-run manifest and the cluster worker materializes its exact
step settings before invoking `jarvis_run`, so failure during reconciliation
cannot reach scheduler submission. The relay job's artifact dependencies and
private MCP result retain machine-readable reused/updated evidence, while
earlier executions keep their original hashes. Retrying the same run
idempotency key reuses the already admitted manifest without rescanning the
mutable workspace. A missing, unsafe, oversized, or concurrently changing file
fails before run submission.

The cluster-side materialization of a per-run manifest is accepted only for the
registered contract. The built-in route therefore compares every tracked path
against its staged digest before it ingests anything and refuses the run with a
typed error naming each changed `step.setting`; call `jarvis_edit_step` on that
setting to stage the new content, then run again.

A changed route, registration, artifact, pipeline, session generation,
checksum, or provenance fails closed rather than reusing stale bytes.

Settings without the exact declaration are passed through unchanged, so a
package that means a cluster-side absolute path keeps working; a path-looking
name is never sufficient authority to read a local file. Legacy or other remote
MCP contracts also remain pass-through and cannot opt into staging.
Large collections should use a separately managed data-staging service or
shared storage rather than raising these control-plane limits without review.
That is a data-management decision made by the site with its own transfer
service; it is not permission for a client, harness, or agent to `scp` run
inputs to the cluster beside the relay.

### verify that staging engaged

A job whose package declared a file-typed setting must return a non-empty
`used_artifact_refs`, each entry pinning an artifact id and its SHA-256:

```powershell
clio-relay job used-artifacts <job-id> --cluster my-cluster
```

Non-empty, with a digest matching the caller's file, is the proof of engagement:
the bytes travelled through relay, the configuration was rewritten to the staged
reference, and the run is reproducible from its lineage. Empty means staging did
not happen. Whatever the job read on the cluster arrived some other way, the run
has no input lineage, and it is not evidence of anything even when it reports
success and the output looks correct. Treat an empty list as a failed run and
fix the staging path; do not work around it by placing the file on the cluster.

One configuration produces an empty list without any error: a cluster-absolute
path in the setting is forwarded verbatim, because a path-looking value carries
no authority to read a local file. Pass a workspace-relative client path
instead.

The built-in JARVIS door used to produce an empty list for a declared binding
too, because it never entered the staging plane
([#176](https://github.com/iowarp/clio-relay/issues/176)). As of this release it
shares the plane with the registered route, so the check above means the same
thing on both doors. One deliberate, typed difference remains: the built-in door
engages staging only when the JARVIS MCP runs on another machine, and a built-in
run whose staged content changed is refused by name (`jarvis_run_input_drift`)
rather than run against bytes the configuration never received. The end-to-end
proof through both doors is still outstanding and tracked as
[#177](https://github.com/iowarp/clio-relay/issues/177).

## Keep the compact JARVIS surface

The compact built-in JARVIS aliases remain compatible:

- `jarvis_create_pipeline`
- `jarvis_describe`
- `jarvis_add_step`
- `jarvis_edit_step`
- `jarvis_get_execution`
- `jarvis_run`

`jarvis_edit_step` uses an explicit `edit` or `remove` operation. The remove
operation unlinks pipeline membership without deleting package files. There is
no `jarvis_remove_step` alias, including in admin/all profiles; admin retains the
lower-level `unlink_pkg` compatibility tool. `jarvis_run` can accept `spack_specs`, whose environment is resolved
and persisted by JARVIS immediately before execution.
`jarvis_get_execution` is the unified durable query for the JARVIS handle,
lifecycle record, runtime metadata, optional progress, and an optional bounded
artifact page. It takes `cluster`, `pipeline_id`, and `execution_id`, plus
`include_progress`, `include_service_runtimes`, and an `artifacts` filter object
when needed. The relay
removes only local routing controls before submitting the durable remote call;
all JARVIS query and cursor fields pass through unchanged.

Application discovery stays inside the same compact tool. Call
`jarvis_describe(target="package_search", query="visualization")` for a ranked,
summary-only page, then call `target="package"` with the selected canonical
`name` as `package_name` for its settings. `query` is required and bounded to 256 characters;
`page_size` defaults to 10 and is limited to 25. Each page is capped at 64 KiB
and reports `total_matches`, `returned_count`, and an opaque `next_cursor`.
Continue with the identical query and cursor. Cursors are limited to 1,024
characters and bind both the normalized query and package-inventory revision,
so using one with another query or after the inventory changes fails closed.
The legacy `target="packages"` response remains exhaustive and potentially
large; agents should not use it for ordinary discovery.

Virtual JARVIS mutations and runs receive a fresh relay job by default. `jarvis_run`
is handle-first: outer `wait_for_terminal` waits only for the brief MCP dispatch
that returns the handle, never for workload completion. Use
`jarvis_get_execution` for lifecycle, progress, artifacts, and services. Supply
an explicit `idempotency_key` only when retry de-duplication is intentional; an
identical second `jarvis_run` is otherwise a new execution.

The released clio-kit 2.7.2 artifact carries the pinned six-tool JARVIS v3.7.1
contract.
Bootstrap
downloads and hashes the exact coordinated wheel, installs it once with
`uv tool install`, and persists the wheel plus the direct JARVIS command in the
worker receipt. The receipt also binds the exact uv executable/version and
tool directories, provider `sys.prefix`, `pyvenv.cfg` uv marker, console-script
ownership, and complete installed RECORD closure. At call time the worker uses
that persistent executable directly. clio-kit's child launcher still uses its
wheel-owned server source and lock with `uv run --frozen --no-editable`, so the
live MCP response binds both the installed outer tool and the locked child
server rather than trusting an unobserved nested resolution. For the built-in
JARVIS route, bootstrap and every process launch additionally require the
embedded lock's unique unconditional `jarvis-mcp` to `jarvis-cd` resolved
dependency edge, exact unconditional direct-URL metadata requirement, unique
`jarvis-cd` package record, source wheel URL, and wheel SHA-256 to match the
relay's exact JARVIS-CD release pin. That dependency edge is recorded in the
install receipt and call result. Operator-registered MCP servers remain bound
by their own discovery artifact and are not constrained to the relay's
JARVIS-CD version.

For a verified `clio-kit.locked-server.v4` launcher, the worker withholds the
MCP `initialize` message until clio-kit's typed post-build cache event proves
that the locked environment sync has finished. This prevents a preparatory
subprocess from consuming protocol input. The request-scoped launch sets
`CLIO_KIT_UV_CACHE_PRUNE=0`: cache-wide pruning can wait behind another live
clio-kit server and is not part of an individual MCP call's bounded startup.
The locked sync and safe superseded-environment eviction still run; operators
can perform explicit clio-kit cache GC outside a served MCP session.

The release gate requires that exact 2.6.6 artifact to be rerun on every target
selected by the release policy. Other servers use the operator registry and
generated `remote_...` aliases.

The exact release wheel bootstrap installs by default is
`clio_kit-2.7.2-py3-none-any.whl` with SHA-256
`8ebe41bf366e475a7da703a52c968231780d5d9013fc5fc913fe0f0539c6b6b5`.
Its canonical contract is `clio-kit-jarvis-user-v3.7.1`. The relay's own
vendored certification snapshot of that contract (used to cross-check the
bundled `_contracts/jarvis-user-v3.7.1.json` copy against a known-good
clio-kit release, independent of which wheel bootstrap installs) is
re-certified against the same clio-kit 2.7.2 build: contract SHA-256
`ede2e48f7201d3e072bd24713ea15f5e4a714a8d52974d884d956fc400174849`,
canonical tools-wire SHA-256
`b17ab5c0f19afbac934e464641668d250beac176b6cfde06c1ce9b50f50d0b6c`, and
bundled contract artifact SHA-256
`3c22d89d1bbc4acda49dc6e324566224de38e01a1c80c3879c66305678adfdbe`.
(clio-kit 2.7.2 added a `title` key to every user-profile tool, which shifts
all three digests from the pre-2.7.2 values; clio-relay#199.)
The nested runtime lock is bound to the public
[`jarvis_cd-1.8.0-py3-none-any.whl`](https://github.com/grc-iit/jarvis-cd/releases/download/v1.8.0/jarvis_cd-1.8.0-py3-none-any.whl)
release artifact with SHA-256
`2c2e2042d0256bd3d9c117d75aaf00d26d9e814fcbcca9a904abf06399fc1067`;
bootstrap and call-time validation reject any other URL, version, or bytes.

## Register the Spack MCP

The audited clio-kit user surface contains `spack_find`, `spack_locate`, and
`spack_install`. Operators expose only those selected tools through the generic
cluster registry. `spack_load` is intentionally absent because environment
changes in an MCP child process would not affect a later JARVIS run. Runtime
environment application belongs to `jarvis_run(spack_specs=[...])`.
The semantic check is enabled explicitly with the current
`clio-kit-spack-user-v2.1` contract identifier; registration names remain
operator-defined and do not select behavior. Existing
`clio-kit-spack-user-v2` registrations remain supported and are verified
against their distinct preserved contract digest.
The current v2.1 contract SHA-256 is
`4a065d2c67c0dd34e2cc18bca9dc53ed87ce35aa4ac524ef3e5c954a875c19db`,
its canonical tools-wire SHA-256 is
`c7f1d1a4ce35b58664b46d2994863257a1e5a30e5c4ab7501b0a96a4becc08b7`,
and its bundled artifact SHA-256 is
`b8da9a3cad05ad734ac3a20adb635f11fa45a8870afe08a9f4e261fdc713b57d`
(re-certified against clio-kit 2.7.2; clio-relay#199).

## Register the scientific catalog MCP

clio-kit also ships the two-tool
`clio-kit-scientific-catalog-user-v1.1` contract. It separates dataset discovery
from visualization control: `scientific_dataset_search` finds operator catalog
records and `scientific_dataset_describe` returns the complete catalog record
plus one exact top-level `dataset_descriptor` with schema
`jarvis.dataset-descriptor.v1`. Pass only that top-level descriptor unchanged as
`jarvis_add_step`'s `config.dataset_descriptor`; the surrounding `dataset`
record is human discovery metadata and is not a JARVIS package argument. Register
the server through the same generic federation layer; the relay does not add
dataset names, scene recipes, or site-specific semantics:

```powershell
clio-relay remote-mcp register `
  --cluster my-cluster `
  --name scientific-catalog `
  --command clio-kit `
  --arg mcp-server `
  --arg scientific-catalog `
  --allow-tool scientific_dataset_search `
  --allow-tool scientific_dataset_describe `
  --contract clio-kit-scientific-catalog-user-v1.1 `
  --profile user
clio-relay remote-mcp refresh --cluster my-cluster --name scientific-catalog
```

The relay checks the current contract SHA-256
`fd9fd4ba76617f1fd13560420cd650f78adc55d0957bd950d10d09c72ebe1889`,
canonical tools-wire SHA-256
`6a8fc61e31515880c722db3447d2f01584e4b297cb02b70b5618bff081840380`,
and exact contract artifact SHA-256
`8548aa8f0d1993ec644bb2fea778a4759b27d34bea3ed93ff92254b6fbf3052e`
(re-certified against clio-kit 2.7.2; clio-relay#199).
Historical `clio-kit-scientific-catalog-user-v1` registrations remain accepted
against their separately preserved contract, wire, and artifact digests; they
do not claim the explicit top-level descriptor handoff added in v1.1. At
runtime, the operator registration and refreshed schema cache remain the
authority, so adding a different catalog or cluster requires no relay code
change.

For an unreleased candidate, use an exact wheel path for the remote command and
record its digest in the validation report. Replace the placeholder only after
building the coordinated clio-kit artifact:

```powershell
clio-relay remote-mcp register `
  --cluster my-cluster `
  --name spack `
  --command /home/operator/.local/bin/clio-kit `
  --arg=mcp-server `
  --arg=spack `
  --contract clio-kit-spack-user-v2.1 `
  --allow-tool spack_find `
  --allow-tool spack_locate `
  --allow-tool spack_install `
  --profile user `
  --call-timeout-seconds 14400
```

## Run live acceptance

Before claiming a registered server path as released:

1. install the exact candidate or released wheel once with `uv tool install`
   on the desktop and target cluster, retaining its digest;
2. register a non-JARVIS MCP server with an exact user-profile allowlist;
3. run `remote-mcp refresh` and retain its JSON output;
4. request `tools/list` from `clio-relay mcp-server` and record the generated
   alias and schema;
5. call that alias with the configured cluster;
6. wait for success and verify `stdout`, `stderr`, `mcp_result`, and
   `provenance` artifacts from the discovery and tool-call jobs;
7. run `remote-mcp reload` and retain the machine-readable catalog revision and
   cache provenance in the live validation report.

Each refresh profile reports both `virtual_tools`, the complete rendered profile
catalog, and `registration_virtual_tools`, the aliases contributed by the exact
`cluster` and `server_name` refreshed in that operation. Use the latter when
attributing live evidence to one registered server; aliases from another cached
registration must not be credited to it.

The validation helper performs steps 4 through 7 against one allowlisted tool
and writes report-ready JSON. It requires a fresh explicit discovery cache and
starts the installed `clio-relay mcp-server` executable over stdio. The
initialize, `tools/list`, and `tools/call` responses, executable command,
return code, and transcript digests are retained as machine evidence; the
helper does not call the in-process request handler:

```powershell
clio-relay remote-mcp validate `
  --cluster my-cluster `
  --name science `
  --tool inspect_dataset `
  --arguments-json-file .\inspect-arguments.json `
  --profile user `
  --output-json .\validation\remote-mcp.json
```

The report contains the canonical checks `remote-mcp.register`,
`remote-mcp.discover`, `remote-mcp.tools-list`, `remote-mcp.call`, and
`remote-mcp.durable-result`. The final check requires a successful durable job
plus indexed `stdout`, `stderr`, `mcp_result`, and `provenance` artifacts whose
job and route metadata match the call.

Local fake-server tests prove protocol and virtualization behavior, but they do
not replace this released-artifact cluster acceptance.
