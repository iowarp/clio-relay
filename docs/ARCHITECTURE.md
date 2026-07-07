# Architecture

`clio-relay` has three roles and one source of truth.

The relay host is only `frps` configuration. It has no queue, no job records, and no application logic. The frpc-to-frps transport protocol is deploy-time configuration. For the current Cloudflare-backed homelab path, use WebSocket/TLS (`wss`) over port 443. For a later VPS or institutional relay host with raw TCP, use `tcp` without changing endpoint semantics.

In the Cloudflare-backed homelab deployment, Cloudflare terminates public HTTPS for `frps.jcernuda.com` and forwards to a local HTTP origin. A small nginx edge container owns that HTTP origin, forwards the frp WebSocket path `/~!frp` to `frps` on loopback, and exposes only a simple health endpoint otherwise. `frps` still remains a dumb byte relay: it has no CLIO queue state, no job state, and no application routing logic.

The desktop endpoint submits configured-cluster work into the durable queue and exposes job, event, artifact, cancellation, remote-agent, and MCP-call surfaces for CLIO consumers.

The worker endpoint leases queued jobs for one configured cluster, materializes relay intents into JARVIS-CD YAML, runs JARVIS-CD, and records progress, stdout, stderr, artifacts, provenance, and terminal state back into the queue.

The queue boundary is `ClioCoreQueue`. It owns endpoint, job, task, lease, event, cursor, artifact, progress, checkpoint, and idempotency record families. The filesystem implementation in this repository is a development backend for the same record contract; production deployment can bind the same API to clio-core CTE storage.

Each leased job gets a durable execution task record. Task records move through queued, running, and terminal states independently of the parent job and emit `task.*` events so clients can distinguish job-level state from execution-step state during replay.

Per-job spool directories live on cluster-accessible storage and are backing files only:

- `metadata.json`
- `events.jsonl`
- `stdout.log`
- `stderr.log`
- `artifacts.jsonl`
- `pipeline.yaml`
- `provenance.json`

Spool files are never the authoritative queue. Reconnects and cursor replay come from clio-core records.
