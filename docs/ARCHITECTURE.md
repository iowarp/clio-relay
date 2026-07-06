# Architecture

`clio-relay` has three roles and one source of truth.

The relay host is only `frps` configuration. It has no queue, no job records, and no application logic. The frpc-to-frps transport protocol is deploy-time configuration. For the current Cloudflare-backed homelab path, use WebSocket/TLS (`wss`) over port 443. For a later VPS or institutional relay host with raw TCP, use `tcp` without changing endpoint semantics.

The desktop endpoint submits configured-cluster work into the durable queue and exposes job, event, artifact, cancellation, remote-agent, and MCP-call surfaces for CLIO consumers.

The worker endpoint leases queued jobs for one configured cluster, materializes relay intents into JARVIS-CD YAML, runs JARVIS-CD, and records progress, stdout, stderr, artifacts, and terminal state back into the queue.

The queue boundary is `ClioCoreQueue`. It owns endpoint, job, task, lease, event, cursor, artifact, checkpoint, and idempotency record families. The filesystem implementation in this repository is a development backend for the same record contract; production deployment can bind the same API to clio-core CTE storage.

Per-job spool directories live on cluster-accessible storage and are backing files only:

- `metadata.json`
- `events.jsonl`
- `stdout.log`
- `stderr.log`
- `artifacts.jsonl`
- `pipeline.yaml`

Spool files are never the authoritative queue. Reconnects and cursor replay come from clio-core records.
