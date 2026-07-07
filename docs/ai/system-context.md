# system context

`clio-relay` is a relay layer for submitting and observing remote work. It is part of the `clio-agent` federation story, but it must remain usable by non-CLIO clients through CLI, HTTP, and MCP.

## core boundaries

- `clio-core` is the authoritative queue and state boundary.
- The file-backed queue in this repository is a development backend for the same record contract.
- frp and SSH forwarding are byte transports only.
- JARVIS-CD owns cluster execution, scheduler integration, package behavior, output collection, and provenance.
- Application-specific behavior belongs in JARVIS packages or package-aware adapters, not in generic relay core code.

## roles

- `desktop`: accepts local user, agent, HTTP, and MCP requests and submits durable jobs.
- `worker`: runs on a configured cluster, leases jobs, invokes JARVIS-CD, streams events, and writes artifacts.
- `relay-host`: renders or runs frp host configuration. It must not own queue state or job state.

## records

The durable record families are:

- endpoints
- jobs
- tasks
- leases
- events
- cursors
- artifacts
- progress
- checkpoints
- idempotency records

Each job has monotonic events and cursor-based replay. Logs are readable by byte offset. Artifacts are indexed records that can point to backing files.

## execution semantics

Job submission is asynchronous by default. Submit returns a `job_id`, state, kind, and terminal flag. Clients can choose synchronous waiting only when they are not blocking the only worker that can execute child work.

Remote agent jobs should usually submit child cluster work asynchronously and return the child `job_id`. A cluster worker running a parent agent job cannot also execute a child job if it is waiting synchronously inside the parent.

Cancellation is durable and cooperative. A cancel request records queue events and the worker terminates the running process group. For scheduler-backed packages, package code should capture scheduler job ids and cancel through the scheduler when needed.

## progress

Progress can come from generic regex rules or package-aware adapters.

Trusted package progress must be distinguishable from raw workload stdout. Raw stdout text is not proof that a package adapter ran. LAMMPS acceptance uses the upstream JARVIS `builtin.lammps` package and package-aware relay parsing enabled for that package.

ETA is based on observed iterative progress after warmup. It should include confidence or sample metadata when exposed to clients.

## transports

Supported paths:

- frp STCP over WebSocket/TLS for Cloudflare-backed or HTTPS-edge relay hosts.
- frp STCP over TCP for public or institutional relay hosts.
- SSH local port forwarding for closed environments that already have SSH or VPN access.
- frp XTCP probing as an optional optimization with fallback.

Transport failure must not corrupt queue state. Direct transport and NAT punching are optimizations, not reliability requirements.

## sessions

Remote sessions are owned by a session id. Session metadata and PID files live under `$HOME/.local/share/clio-relay/sessions/<session-id>`.

The desktop shutdown choices are separate:

- close local UI and detach from remote session.
- close local UI and tear down the owned remote session.
- close local UI and also stop the persistent worker service.

Only the last option should call `session teardown --stop-worker`.

## configurability

Do not hardcode:

- `ares`
- `codex`
- `frps.jcernuda.com`
- LAMMPS
- Cloudflare
- local filesystem roots

Those are configured examples or live acceptance targets. The implementation must support a second configured machine such as `homelab` without changing core semantics.
