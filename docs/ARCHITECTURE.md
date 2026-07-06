# Architecture

`clio-relay` has three roles and one source of truth.

The relay host is only `frps` configuration. It has no queue, no job records, and no application logic.

The desktop endpoint submits Ares work into the durable queue and exposes job, event, artifact, cancellation, Codex-task, and MCP-call surfaces for CLIO consumers.

The Ares endpoint leases queued Ares jobs, materializes relay intents into JARVIS-CD YAML, runs JARVIS-CD, and records progress, stdout, stderr, artifacts, and terminal state back into the queue.

The queue boundary is `ClioCoreQueue`. It owns endpoint, job, task, lease, event, cursor, artifact, checkpoint, and idempotency record families. The filesystem implementation in this repository is a development backend for the same record contract; production deployment can bind the same API to clio-core CTE storage.

Per-job spool directories live on Ares shared storage and are backing files only:

- `metadata.json`
- `events.jsonl`
- `stdout.log`
- `stderr.log`
- `artifacts.jsonl`
- `pipeline.yaml`

Spool files are never the authoritative queue. Reconnects and cursor replay come from clio-core records.
