# Connection model

This page is normative. It states how a `clio-relay` connection is designed to
behave, and every other document in this repository is subordinate to it. Where
shipped code currently deviates, the deviation is named in
[Known deviations](#known-deviations) with its tracking issue. A deviation is a
defect to be fixed, never a description of how the system works and never a
pattern to copy.

Read this before designing any integration, harness, benchmark, or agent
instruction that uses relay.

## What a connection is

A connection is one local relay process and one remote relay process joined by
exactly one persistent relay-protocol link.

- The **local relay** runs next to the client. It manages every remote
  connection, holds the durable client-facing surfaces (CLI, HTTP, MCP), and
  projects the remote interfaces into one local MCP endpoint.
- The **remote relay** runs on the cluster. It owns the cluster-side endpoint
  worker, JARVIS execution, and the cluster's local state.
- A cluster entry in `.clio-relay/clusters.json` names one such connection.

The relay point (`frps`) in the modes below is not a third party to the
connection. It joins two outbound connections and stores nothing.

## The one-link rule

All traffic for a connection rides that one channel. Control plane and data
plane are the same channel:

owned-session status, owned-session identity challenge, job submission, job
state, events and cursors, log reads by offset, artifact listing, artifact
content, input-artifact ingest, progress, watch, cancellation, gateway session
operations, queue inspection, detach, and teardown.

There is no second channel, no side channel, and no per-operation dial. Every
one of those operations is plain local HTTP over the mapped port.

The link is established once, at connection bring-up, and is held for the
lifetime of the connection. **An operation never establishes transport. It uses
transport that already exists.** Anything that would open a new connection to
perform a status check, a poll, a recovery, or a transfer is a design violation
rather than an optimization opportunity, and lowering its cost does not make it
conforming.

## Transport modes

Three modes, in order of preference. The mode decides how the single link is
built. It never changes the one-link rule, and no mode gives an operation its
own transport.

### (a) TCP through an internet-accessible relay point — primary

1. One ssh connection deploys the relay on the cluster. Skipped when the cluster
   relay is already deployed.
2. The cluster relay connects **outbound** to the internet-accessible relay
   point over TCP.
3. The local relay is started on the client machine.
4. The local relay connects outbound to the same relay point.
5. The relay point brokers a handshake that joins the two outbound connections
   into one relay-to-relay link.

Neither side accepts an inbound connection. The relay point is a dumb joiner and
holds no job, session, or application state. `CLIO_RELAY_STCP_SECRET` is this
mode's pairing secret; `CLIO_RELAY_FRP_TOKEN` authenticates to the relay point.
WebSocket/TLS on port 443 serves Cloudflare-style HTTPS edges; raw TCP serves a
public or institutional host.

### (b) UDP NAT-bypass rendezvous

The same rendezvous, with the handshake attempting NAT traversal over UDP. The
two relays end with a direct peer link and the relay point is used only for the
handshake. When hole punching fails, the connection falls back to (a)'s
relay-point-carried TCP. This is a latency and throughput optimization; it is
never a reliability requirement, and a failed bypass is not a failed connection.

### (c) SSH port forward — fallback

For infrastructure that permits no other path, such as DOE-class clusters:

1. One ssh connection deploys the cluster relay. Skipped when it is already
   deployed.
2. The local relay is started on the client machine.
3. A **second** ssh connection establishes one port forward. The user is present
   for its interactive authorization.
4. The local relay connects to the cluster relay through that mapped port, for
   the lifetime of the connection.

Every subsequent message rides the mapped port. The forward is the connection.

## The SSH budget

| mode | ssh connections for the whole connection lifetime | what they are |
|---|---|---|
| (a) TCP through relay point | at most 1 | deploy the cluster relay; skipped when already deployed |
| (b) UDP NAT bypass | at most 1 | deploy the cluster relay; skipped when already deployed |
| (c) SSH port forward | at most 2 | deploy (skippable) plus the one held forward |

The budget is per connection lifetime. It is not per operation, per session, per
job, per client context, per poll, or per hour. After bring-up the count of new
ssh connections is **zero**, across any number of operations, watches,
recoveries, cancellations, and staging transfers.

A build that opens a third ssh connection in mode (c), or a second in (a)/(b),
is broken even when every dial succeeds.

SSH multiplexing (`ControlMaster`/`ControlPersist`), forward caching, and
connection pooling are explicitly **not** fixes for exceeding the budget. They
preserve per-operation ssh semantics at lower cost, which reinforces the wrong
model. The design has one link, and everything rides it.

## The 2FA operating assumption

Establishing an ssh connection to a protected cluster requires interactive
two-factor authorization. This is the operating constraint the whole design is
built on:

**The user is present to approve the first connection at bring-up, and is
present for none after.**

Any code path that could dial ssh unattended — a retry, a reconnect loop, a
per-context probe, an ephemeral forward, a status poll, a recovery scan — is a
violation even when it happens to succeed. Key-based access on a test machine is
a property of that machine, not permission to design around a prompt the real
user will never see. A path that works only because the tester had passwordless
ssh is unshippable.

This is why the ssh budget is a hard number and not a performance target.

## Reconnect

Reconnect is a first-class client behavior, not an error path.

The local relay can disconnect and connect again, and the link is
re-establishable without loss: durable state lives in the core, and the link is
only transport. No mode may treat a reconnect as a new deployment, re-run
bootstrap, or demand authorization beyond what its own handshake requires.

- In modes (a) and (b), a reconnect costs zero ssh connections. Both relays dial
  out again and the relay point rejoins them.
- In mode (c), the held forward *is* the connection, so re-establishing it
  re-enters bring-up with the user present. That is precisely why unattended
  reconnect machinery is forbidden in this mode: an automatic retry loop spends
  a budget only a present human can authorize.

Losing the link never loses queue state, job state, session state, or lineage. A
transport failure must not corrupt or invent durable state, and an observation
timeout is never proof of a terminal outcome.

## One local relay, many remote relays

A single local relay process manages connections to multiple remote relays
concurrently. It is the manager of all connections, not a per-connection
sidecar.

- One held channel per connected cluster, all owned by that one local process.
- The client-facing MCP endpoint stays single and stable across the connect,
  disconnect, and reconnect cycles of any individual remote.
- Adding a second cluster adds a connection inside the same local relay. It does
  not add a second local relay, a second MCP server, or a second client
  configuration.

## The virtual MCP layer

Focalization is the point of the product, not a convenience feature.

Multiple remote servers, across multiple clusters, focalize into **one** virtual
MCP endpoint on the client. The agent speaks to a single MCP surface and the
local relay fans out behind it. That is exactly why the projection layer exists:
virtual tools carry per-cluster identity, and the local-only `cluster` selector
chooses the route.

Consequences a client or agent can rely on:

- One MCP registration serves every cluster. There is never one registration per
  cluster.
- The way to reach a second cluster is to name it in the `cluster` argument. It
  is never to open a second connection, a second tunnel, or a shell.
- A tool alias is stable across clusters when the namespace, tool name, schema,
  and declared contract are equivalent; the cluster route is an argument, not a
  different tool.

See [remote MCP federation](remote-mcp-federation.md) for alias generation,
schema cache, profile, and freshness rules.

## Input staging is relay-owned

Run inputs that live on the client machine reach the cluster **through the
relay, over the one link**. Nothing copies files around the transport.

The contract for a file-typed package setting:

1. The package description declares the binding — exactly
   `jarvis.configuration-input-binding.v1` with `kind="local_file"`. Staging is
   schema-driven, never filename-driven; a path-looking argument is never
   sufficient authority to read a local file.
2. The caller passes a path **relative to the client workspace root**
   (`CLIO_RELAY_INPUT_WORKSPACE_ROOT`), naming a file on the client machine.
3. The local door snapshots those bytes from the workspace and hashes them,
   enforcing the per-file, aggregate, and count bounds before any remote
   mutation.
4. The bytes ingest through the authenticated owned session over the one link
   (`POST /input-artifacts/ingest`).
5. The cluster materializes a content-addressed copy in the run's staging area.
6. Relay rewrites the package configuration to that cluster-local staged
   reference.
7. The job records immutable `ArtifactUse` lineage for the staged artifact.

Every genuinely new run re-snapshots the tracked logical paths: unchanged
content reuses its immutable artifact, changed content is ingested as a new one,
and an idempotent retry reuses the already admitted manifest without rescanning
the workspace.

### used_artifact_refs is the proof of engagement

A job whose package declared a file-typed setting must come back with a
**non-empty `used_artifact_refs`**, each entry pinning an artifact id and its
SHA-256.

- Non-empty, with a digest matching the client's bytes: staging engaged. The run
  is reproducible and its inputs are attributable.
- Empty: staging did **not** engage. Whatever the job read on the cluster got
  there some other way, the run is not reproducible, and its result is not
  evidence of anything. Treat that run as failed even when the job reports
  success and the output looks right.

This is the single check that distinguishes a real relay run from a run that was
set up out of band. Any harness, benchmark, or acceptance procedure that reports
success without asserting it is measuring the wrong system.

## Bootstrap-time file placement

One bounded exception exists, and it is not a channel.

Before a connection exists there is nothing to carry bytes, so the deploy step
of each mode places what the cluster needs to become a relay peer: the relay
artifact itself, and operator-owned material such as the remote agent's prompt
file, its MCP profile, and site fixtures. This happens **once, at bring-up, over
the deploy connection the mode already budgets**.

Rules that make it bounded:

- It is bootstrap only. It never repeats per run, per job, or per operation.
- It never carries run inputs. Run inputs use the staging contract above, for
  the whole lifetime of the deployment.
- It is not a template. A step marked bootstrap-only in a runbook must not be
  generalized into a way of getting files to the cluster.

There is currently no product channel for placing the remote agent's prompt and
MCP profile on the cluster: `clio-relay agent run --prompt` and `--mcp-config`
take cluster-side paths, and `agent render-mcp-config` writes locally or to
stdout. That is a limitation of that surface at bring-up time, not permission to
move run inputs the same way.

## Never do this

Each item below is a concrete way the model has been misread. They are listed so
no reader can rationally construct the wrong one.

- **Never copy files around the transport.** `scp`, `rsync`, or `sftp` of a run
  input is always wrong. If a file is an input to a run, it reaches the cluster
  through the input-staging contract or it does not reach it at all.
- **Never pass a cluster-absolute path for a binding-declared setting.** The
  correct value is a workspace-relative client path. A cluster path in that slot
  silently disables staging, empties `used_artifact_refs`, and destroys lineage,
  and the job may still appear to succeed.
- **Never re-dial ssh for status, polling, recovery, watch, cancellation, or
  staging.** All of those are HTTP over the established link. If an
  implementation seems to require a new dial, the implementation is wrong, not
  the requirement.
- **Never treat the relay as an ssh convenience wrapper.** Relay is not a nicer
  way to run remote commands. It is a durable queue, a lineage boundary, and one
  held link. A design that reduces to "shell out to the cluster, but through
  relay" has removed everything relay provides.
- **Never build a substitute path around relay.** Driving the cluster with ssh
  or scp beside relay does not extend relay; it replaces it with an unrecorded
  system that produces no lineage, no provenance, no cancellation, and no
  reconnect. Results collected that way describe the substitute, not the
  product.
- **Never host the client-facing door on the cluster and reach it through a
  tunnel.** The door is local, next to the client, because the workspace it
  snapshots is the client's. A cluster-hosted door has no path to the client's
  bytes.
- **Never assume the local relay can read cluster storage.** It cannot. It reads
  the client workspace and speaks HTTP to the remote relay. Reading a cluster
  file means asking the remote side over the link, never `ssh <host> cat`.
- **Never add a second local relay, MCP endpoint, or client configuration per
  cluster.** One local relay manages many remotes behind one virtual MCP
  surface.
- **Never dial unattended because the test machine has passwordless ssh.** See
  the 2FA assumption.

## Known deviations

These are tracked defects. The sections above state the design; this section
records where the shipped implementation does not yet meet it, so no reader
mistakes current behavior for the contract.

- **Owned-session control plane dials ssh per operation**
  ([#179](https://github.com/iowarp/clio-relay/issues/179)). On 1.5.x, owned
  session status, identity challenge, and watch open fresh ssh connections per
  client context, and a per-context `-L` forward is created and torn down around
  each call. A two-sided measurement of one `jarvis_describe` recorded nine
  fresh ssh connections. The fix is to ride the established link; multiplexing
  and forward pooling are explicitly out of scope as fixes.
- **Cluster-targeted CLI dispatch dials ssh per invocation.** With
  `CLIO_RELAY_CLI_MODE=auto`, a cluster-targeted CLI command whose cluster has a
  non-local `ssh_host` is executed by opening an ssh connection for that
  invocation. This is the same shape as #179 and the same rule applies to it.
  `CLIO_RELAY_CLI_MODE=local` keeps the command in-process.
- **The built-in JARVIS door skips input staging**
  ([#176](https://github.com/iowarp/clio-relay/issues/176)). Virtual `jarvis_*`
  tools reached through the built-in door forward a declared file-typed setting
  verbatim and return an empty `used_artifact_refs`, so the binding's promise is
  silently skipped. Until it lands, an empty `used_artifact_refs` on that route
  is expected, and it still means the run is not evidence.
- **End-to-end client-local staging to a remote cluster is not yet proven**
  ([#177](https://github.com/iowarp/clio-relay/issues/177)). The capability
  exists on the registered JARVIS route; the release-gating proof is outstanding
  because harness-side `scp` had been masking the seam. Acceptance requires no
  out-of-band copying anywhere in the proven path.

## Related pages

- [architecture](architecture.md) — roles, durable records, execution boundary.
- [connect a desktop, homelab relay, and cluster](connect-desktop-homelab-cluster.md)
  — the first-connection walkthrough.
- [remote MCP federation](remote-mcp-federation.md) — the virtual layer and the
  staging contract in operational detail.
- [operations](operations.md) — operator paths for each mode.
