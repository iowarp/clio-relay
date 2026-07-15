# JARVIS service-runtime binding validation, 2026-07-15

## Scope

This evidence covers the local implementation of the connector-only binding from
a durable relay-routed JARVIS MCP result to a local agent-facing gateway. It is not
a live cluster acceptance report and makes no Ares, homelab, released-wheel, or
network claim.

## Contract checks

- The source relay job must be succeeded, `mcp_call`, `tools/call`, and exactly
  `jarvis_get_execution` through the configured clio-kit JARVIS MCP with
  `include_service_runtimes=true`.
- The source artifact must be the exact job-owned `mcp_result`; its content must
  match the durable SHA-256 and the job's expected/observed MCP-server artifact
  digest, route, arguments, environment references, and structured protocol result.
- Native execution handle, record, progress, scheduler identity, service snapshot,
  ready package identity, dataset descriptor, canonical descriptor fingerprint,
  and all exact-schema boundaries are validated before connectors start.
- The agent-facing bind accepts no host, port, service path, dataset descriptor,
  scheduler identity, or submit/status/cancel command.
- The durable gateway binding stores the source job/artifact identity and digest,
  execution/scheduler identity, package/service identity and revision, service
  report digest, and exact dataset descriptor and digest.
- Bind starts connector resources only. Detach removes the desktop connector only;
  stop retains scheduler work by default. Explicit cancellation re-verifies the
  original binding and exact provider/native identity before provider invocation.
- SLURM placement requires one exact provider-verified BatchHost in a single-node
  allocation; the connector step is pinned to that host and persists the evidence.
- Readiness verifies the exact ParaView health identity and matching initial state,
  execution, state revision, and canonical dataset descriptor digest.
- Browser access is a separate one-time, expiring loopback capability. Every request
  requires exact `Origin: null`; no wildcard CORS is emitted. Revocation is durable
  before proxy cleanup, and no raw capability enters normal gateway state.

## Local focused evidence

Run from the repository checkout with `uv`:

```text
uv run pytest tests/test_jarvis_service_runtime.py -q
17 passed

uv run pytest tests/test_browser_gateway.py -q
4 passed

uv run pytest tests/test_browser_attachment_queue.py -q
6 passed

uv run pytest tests/test_scheduler_status.py -k connector_placement -q
2 passed

uv run pytest tests/test_mcp_server.py -q
68 passed

uv run pytest tests/test_service_runtime.py -q
59 passed
```

The focused tests cover valid provenance binding, historical schema refusal,
ambiguous and non-ready service refusal, source revision drift, injected
lifecycle-command rejection, all six returned and persisted operator URLs,
detach/default-stop retention, fail-closed explicit cancellation after binding
tamper, exact single-node placement, exact health/state/dataset admission, browser
capability and null-origin enforcement, preflight/method narrowing, streaming,
expiry, single-slot atomic attach, teardown/attach race refusal, exact-identity
revocation transitions, and idempotent concurrent revocation. Release and live
validation must add a
machine-readable acceptance report from the installed wheel on each configured
target before any live claim is made.
