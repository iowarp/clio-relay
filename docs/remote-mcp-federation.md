# Remote MCP federation

`clio-relay` exposes one local MCP server to a desktop agent. Operators can
register stdio MCP servers that exist in a cluster environment, discover their
real schemas through durable relay jobs, and expose selected remote tools as
normal local tools with a `cluster` argument.

The desktop agent does not need one MCP registration per cluster. A virtual
call follows the normal relay path:

1. The agent calls a concrete local alias and selects a configured cluster.
2. The relay removes the local-only `cluster` selector.
3. The relay submits a durable `mcp_call` job with the registered command,
   arguments, remote tool name, and untouched remote tool arguments.
4. A worker launches the server in the cluster environment through JARVIS-CD.
5. The worker records stdout, stderr, the MCP result, execution provenance, and
   terminal state.

The low-level `relay_submit_mcp_call` admin tool remains available as an escape
hatch. Registration is the safer agent-facing path because commands, schemas,
profiles, and tools are operator-controlled.

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
retrieve the actual remote result from the completed job's `mcp_result`
artifact. A call response is limited to 16 MiB, session stdout to 32 MiB, and
stderr to 4 MiB; exceeding a limit fails the durable job instead of allowing an
unbounded worker process.

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
`relay_wait`, or the equivalent CLI, to follow it and read its result:

```powershell
clio-relay job wait <job-id> --cluster my-cluster
clio-relay job list-artifacts <job-id> --cluster my-cluster
```

The `mcp_result`, `stdout`, `stderr`, and `provenance` artifacts provide the
acceptance evidence for the actual cluster-side execution.

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

Virtual JARVIS mutations and runs receive a fresh relay job by default. Supply
an explicit `idempotency_key` only when retry de-duplication is intentional; an
identical second `jarvis_run` is otherwise a new execution.

The released clio-kit 2.5.0 artifact is the pinned six-tool JARVIS v3.2 contract.
Bootstrap
downloads and hashes the exact coordinated wheel, installs it once with
`uv tool install`, and persists the wheel plus the direct JARVIS command in the
worker receipt. The receipt also binds the exact uv executable/version and
tool directories, provider `sys.prefix`, `pyvenv.cfg` uv marker, console-script
ownership, and complete installed RECORD closure. At call time the worker uses
that persistent executable directly. clio-kit's child launcher still uses its
wheel-owned server source and lock with `uv run --frozen --no-editable`, so the
live MCP response binds both the installed outer tool and the locked child
server rather than trusting an unobserved nested resolution. The release gate
requires that exact 2.5.0 artifact to be rerun on every target selected by the
release policy. Other servers use the operator registry and generated
`remote_...` aliases.

The exact release wheel is
`clio_kit-2.5.0-py3-none-any.whl` with SHA-256
`acc13d7924045f2b636a8ceededf4816cfb3b936512b7e5d3dd0d50055540f5f`.
Its canonical contract is `clio-kit-jarvis-user-v3.2`, with contract SHA-256
`12f6d349c9d44d8ce3594943dcd4018ec9b6e01ebb0e59d468bb1bb783a1ad5d`
and canonical tools-wire SHA-256
`bda0abe2b57d5e52ef639bf530e967c3b65072ebc4761d25cd9cbbcf0cd934e9`.

## Register the Spack MCP

The audited clio-kit user surface contains `spack_find`, `spack_locate`, and
`spack_install`. Operators expose only those selected tools through the generic
cluster registry. `spack_load` is intentionally absent because environment
changes in an MCP child process would not affect a later JARVIS run. Runtime
environment application belongs to `jarvis_run(spack_specs=[...])`.
The semantic check is enabled explicitly with the
`clio-kit-spack-user-v2` contract identifier; registration names remain
operator-defined and do not select behavior.

## Register the scientific catalog MCP

clio-kit 2.5.0 also ships the two-tool
`clio-kit-scientific-catalog-user-v1` contract. It separates dataset discovery
from visualization control: `scientific_dataset_search` finds operator catalog
records and `scientific_dataset_describe` returns one exact
`jarvis.dataset-descriptor.v1`. Register it through the same generic federation
layer; the relay does not add dataset names, scene recipes, or site-specific
semantics:

```powershell
clio-relay remote-mcp register `
  --cluster my-cluster `
  --name scientific-catalog `
  --command clio-kit `
  --arg mcp-server `
  --arg scientific-catalog `
  --allow-tool scientific_dataset_search `
  --allow-tool scientific_dataset_describe `
  --profile user
clio-relay remote-mcp refresh --cluster my-cluster --name scientific-catalog
```

The released wheel contract and its hashes are checked by the relay release
gate. At runtime, the operator registration and refreshed schema cache remain
the authority, so adding a different catalog or cluster requires no relay code
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
  --contract clio-kit-spack-user-v2 `
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
