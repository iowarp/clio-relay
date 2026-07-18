# Release-gate fixtures

These files support the operator procedure in
[`docs/release-acceptance-1.0.md`](../../docs/release-acceptance-1.0.md). They
contain no credentials, cluster registry state, or operator home-directory
paths.

The JSON and YAML templates use the following literal replacement tokens:

- `__RUN_ID__`: a fresh identifier unique to the candidate or released stage.
- `__REMOTE_ROOT__`: an absolute per-run remote output directory.
- `__REMOTE_FIXTURE_ROOT__`: the absolute remote directory containing this
  staged fixture tree.
- `__REMOTE_STATE_ROOT__`: an absolute private remote runtime-state directory.
- `__SERVICE_PORT__`: a unique remote service port.
- `__DESKTOP_PORT__`: a unique local visitor port.
- `__HEALTH_NONCE__`: a fresh 256-bit runtime identity returned exactly by the
  health endpoint.

Render into an ignored per-stage directory. Never edit a tracked template with
live values, and never write token or shared-secret values into rendered files.
`report-matrix-1.0.json` is the authoritative ordered inventory of the 18
non-local reports required for each evidence stage.

The nonscheduler gateway driver is Linux-specific and stores private state with
the service leader PID, process-group and session IDs, `/proc` start ticks,
exact argv, and a fresh 256-bit owner token. Status fails closed when a numeric
PID has been reused or any identity field changes. Cancellation signals a group
only after exact leader verification, signals it once, and then polls the
kernel for exact process-group absence until a bounded deadline.
