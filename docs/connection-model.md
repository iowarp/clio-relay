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

### (c) SSH port forward — the configured pathway for secure environments

"Fallback" here is a statement about which mode is the default, not about
runtime behavior. The transport mode is a per-connection **configuration
choice**: TCP through a relay point is what a user configures by default, and
the ssh pathway is what a user configures for secure environments that permit
no other path, such as DOE-class clusters. The relay never switches modes on
its own — a connection whose configured link fails reports a typed link
failure and re-establishes the **same** configured mode; it does not try
another transport. (The one sanctioned in-mode degradation is (b)'s
hole-punch handshake falling back to (a)'s relay-point-carried TCP, which
stays within the same configured rendezvous.)

For a connection configured on this pathway:

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

## The frp dial budget

Modes (a)/(b) pay their per-connection budget in held frp visitor pairs
(one local `frpc` process holding an stcp/xtcp visitor tunnel) instead of ssh
connections. The same "operations never open new transport" rule applies, just
counted in a different unit:

| mode | bring-up | any number of later operations |
|---|---|---|
| (a) `brokered_tcp` | 1 frp visitor pair + 0 ssh (beyond the skippable deploy step above) | 0 new pairs; a dropped tunnel never respawns on its own -- the next operation surfaces a typed `dropped`/`ChannelDropped` failure; `reconnect()` costs exactly +1 pair |
| (b) `udp_rendezvous` | same as (a), over an xtcp visitor | same as (a); a failed hole punch is a typed refusal (`TransportPunchFailed`) in this release -- see Known deviations -- never a second, silently-rendered stcp visitor |

A build that opens a second frp visitor pair for the same held connection, or
renders an stcp config for a udp_rendezvous connection that never asked for
one, is broken even when the pair it opens succeeds.

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
mistakes current behavior for the contract. The campaign that restores the
implementation to this page is
[#182](https://github.com/iowarp/clio-relay/issues/182), which carries the
residual checklist.

### Closed by this release

Both entries describe deployments older than this release, which still behave
this way. They are kept because a reader meeting a 1.5.x deployment needs to
recognize the shape.

- **Owned-session control plane dialed ssh per operation**
  ([#179](https://github.com/iowarp/clio-relay/issues/179)). On 1.5.x, owned
  session status, identity challenge, and watch opened fresh ssh connections per
  client context, and a per-context `-L` forward was created and torn down around
  each call. A two-sided measurement of one `jarvis_describe` recorded nine
  fresh ssh connections. The owned-session control plane now rides one channel
  held per remote connection — see
  [the one-link control plane](one-link-control-plane.md) for the implemented
  shape and the two-sided measurement the deployment gate re-runs. Multiplexing
  and forward pooling were never in scope as fixes and still are not. #179 stays
  open for the residuals listed below.
- **The built-in JARVIS door skipped input staging**
  ([#176](https://github.com/iowarp/clio-relay/issues/176)). Virtual `jarvis_*`
  tools reached through the built-in door forwarded a declared file-typed setting
  verbatim and returned an empty `used_artifact_refs`, so the binding's promise
  was silently skipped. Both doors now stage through one plane
  (`src/clio_relay/jarvis_input_plane.py`), so an empty `used_artifact_refs` on
  either route means what this page says it means everywhere else: staging did
  not engage, and the run is not evidence.

### Still deviating

- **`relay_bind_jarvis_runtime` admission dials ssh per call.**
  `owner_session_admission.py` reads its status through
  `remote_cli.run_remote_clio`, which opens one ssh connection per call; the bind
  flow was measured at four. It sits outside the owned-session control plane
  restored above and needs a server-side admission endpoint.
- **The `jarvis_service_runtime` scheduler bridge dials ssh per operation** at
  roughly twenty sites. Same shape, separate subsystem, restored separately.
- **Cluster-targeted CLI dispatch dials ssh per invocation.** With
  `CLIO_RELAY_CLI_MODE=auto`, a cluster-targeted CLI command whose cluster has a
  non-local `ssh_host` is executed by opening an ssh connection for that
  invocation. This is the same shape as the entries above and the same rule
  applies to it. `CLIO_RELAY_CLI_MODE=local` keeps the command in-process.
- **No production surface re-establishes a dropped channel.**
  `RemoteConnection.reconnect()`, `RemoteConnectionRegistry.reconnect()`,
  `event_report()`, and `close_all()` exist and are covered by tests, but no
  door calls them, so a dropped channel surfaces as a typed failure rather than
  a recovery a user can authorize.
- **`brokered_tcp`/`udp_rendezvous`'s identity anchor is weaker than an
  ssh-authenticated bring-up.** Mode (c) carries its bring-up identity document
  over the ssh-authenticated act that establishes the channel; modes (a)/(b)
  have no such act, so they fetch it as plain HTTP over the tunnel itself and
  anchor it to the preshared stcp/xtcp pairing secret instead
  (`ChannelLink.identity_anchor="preshared_link_secret"`,
  [relay-architecture-2026-08.md](design/relay-architecture-2026-08.md) §8.3).
  A cluster must opt into this explicitly (`frp_transport.identity_anchor`);
  it is never a silent default -- an unconfigured cluster refuses the mode
  rather than falling through to the weaker anchor unannounced. **The anchor
  does not cover the local bind end (the loopback port):** it authenticates
  the two relays to each other, not what is already listening on the local
  machine's loopback port before `frpc` connects there. Bring-up is
  identity-first (the unauthenticated `/session-identity` challenge is fetched
  and verified against this connection's pinned identity BEFORE the
  bearer-authenticated `/session-status` request) precisely to bound this: a
  process with no prior knowledge of this connection's pinned identity learns
  nothing, though one that already knows it could still pass that specific
  check.
  [#232](https://github.com/iowarp/clio-relay/issues/232) tracks the
  client-verifiable (asymmetric-signature) bring-up proof that supersedes
  this anchor for all three modes.
- **`udp_rendezvous`'s hole-punch failure is a typed refusal, not yet the
  automatic in-mode fallback to `brokered_tcp`'s stcp visitor this page
  describes above.** A failed punch raises a typed `TransportPunchFailed`; it
  never renders or spawns an stcp visitor to simulate the fallback
  automatically. Reconfigure the cluster's `remote_transport_mode` to
  `brokered_tcp` directly, or reconnect to retry the same xtcp handshake,
  until that automatic degradation is built.
- **Live service streams still ride a compute-node-side `frpc`** that dials the
  relay host directly, instead of reaching the cluster relay over
  cluster-internal connectivity and riding the connection's link. It also
  requires outbound internet reachability from compute nodes, which most sites do
  not grant.
- **The connection-lifetime identity nonce is weaker than a per-operation
  proof.** Streams are re-proven against the same out-of-band bring-up identity
  document; a client-verifiable per-operation challenge is still owed.
- **`--wait-seconds` is a wire-compatibility break** against cluster relays older
  than this release. It fails loudly, but without naming the version mismatch as
  the cause.
- **End-to-end client-local staging to a remote cluster is not yet proven**
  ([#177](https://github.com/iowarp/clio-relay/issues/177)). The capability now
  exists on both JARVIS routes; the release-gating proof is outstanding because
  harness-side `scp` had been masking the seam. Acceptance requires no
  out-of-band copying anywhere in the proven path, through both doors.

## Related pages

- [the one-link control plane](one-link-control-plane.md) — the implemented
  transport, its configuration, and the two-sided ssh measurement.
- [architecture](architecture.md) — roles, durable records, execution boundary.
- [connect a desktop, homelab relay, and cluster](connect-desktop-homelab-cluster.md)
  — the first-connection walkthrough.
- [remote MCP federation](remote-mcp-federation.md) — the virtual layer and the
  staging contract in operational detail.
- [operations](operations.md) — operator paths for each mode.
- [relay architecture — 2026-08 decomposition design](design/relay-architecture-2026-08.md) — the owner-module map this document's transport modes feed into (§8), including the identity-anchor ruling for modes (a)/(b).
